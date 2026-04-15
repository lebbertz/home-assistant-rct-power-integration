"""Continuous central reader for RCT Power with its own request cycles."""
from __future__ import annotations

import asyncio
from asyncio import StreamReader, StreamWriter, open_connection
from dataclasses import dataclass
from datetime import datetime

from homeassistant.helpers.update_coordinator import UpdateFailed
from rctclient.frame import ReceiveFrame, SendFrame
from rctclient.registry import REGISTRY
from rctclient.types import Command, EventEntry
from rctclient.utils import decode_value

from ..const import LOGGER

CONNECTION_TIMEOUT = 20
CACHE_MAX_AGE_SECONDS = 10
RECONNECT_DELAY = 3

# Request pacing
FREQUENT_INTERVAL_SECONDS = 15
INFREQUENT_INTERVAL_SECONDS = 120
INTER_REQUEST_DELAY_SECONDS = 0.15
IDLE_SLEEP_SECONDS = 0.05

type ReaderValue = (
    bool
    | bytes
    | float
    | int
    | str
    | tuple[datetime, dict[datetime, int]]
    | tuple[datetime, dict[datetime, EventEntry]]
)


@dataclass
class ReaderCacheEntry:
    object_id: int
    value: ReaderValue
    time: datetime


class RctReader:
    """
    Central active reader:
    - keeps one TCP connection open
    - continuously reads everything that comes in
    - actively sends requests in its own cycles
    - stores all decoded values in a cache
    """

    # Frequent objects (15s)
    FREQUENT_OBJECT_IDS: tuple[int, ...] = (
        0x1AC87AA0,  # g_sync.p_ac_load_sum_lp
        0x91617C58,  # g_sync.p_ac_grid_sum_lp
        0xAA9AA253,  # dc_conv.dc_conv_struct[1].p_dc
        0xB5317B78,  # dc_conv.dc_conv_struct[0].p_dc
        0x400F015B,  # g_sync.p_acc_lp
        0x959930BF,  # battery.soc
    )

    # Infrequent objects (120s)
    INFREQUENT_OBJECT_IDS: tuple[int, ...] = (
        0xA9033880,  # battery.used_energy
        0x44D4C533,  # energy.e_grid_feed_total
        0xEFF4B537,  # energy.e_load_total
        0xC0DF2978,  # battery.cycles
        0x5570401B,  # battery.stored_energy
        0x62FBE7DC,  # energy.e_grid_load_total
        0x68EEFD3D,  # energy.e_dc_total[1]
        0xFC724A9E,  # energy.e_dc_total[0]
    )

    def __init__(self, hostname: str, port: int) -> None:
        self._hostname = hostname
        self._port = port

        self._reader: StreamReader | None = None
        self._writer: StreamWriter | None = None

        self._cache: dict[int, ReaderCacheEntry] = {}

        self._main_task: asyncio.Task | None = None
        self._read_task: asyncio.Task | None = None
        self._request_task: asyncio.Task | None = None

        self._running = False
        self._connection_lock = asyncio.Lock()

    # ===============================
    # PUBLIC API
    # ===============================

    async def start(self) -> None:
        if self._running:
            return

        self._running = True
        self._main_task = asyncio.create_task(self._run())
        LOGGER.warning("RCT central active reader started")

    async def stop(self) -> None:
        self._running = False

        tasks = [self._main_task, self._read_task, self._request_task]
        for task in tasks:
            if task is not None:
                task.cancel()

        await self._close_connection()

    def get(self, object_id: int) -> ReaderCacheEntry | None:
        entry = self._cache.get(object_id)
        if entry is None:
            return None

        age = (datetime.now() - entry.time).total_seconds()
        if age > CACHE_MAX_AGE_SECONDS:
            return None

        return entry

    # ===============================
    # MAIN LOOP
    # ===============================

    async def _run(self) -> None:
        while self._running:
            try:
                await self._connect()

                self._read_task = asyncio.create_task(self._read_loop())
                self._request_task = asyncio.create_task(self._request_loop())

                done, pending = await asyncio.wait(
                    [self._read_task, self._request_task],
                    return_when=asyncio.FIRST_EXCEPTION,
                )

                for task in pending:
                    task.cancel()

                for task in done:
                    exc = task.exception()
                    if exc is not None:
                        raise exc

            except asyncio.CancelledError:
                break
            except Exception as exc:
                LOGGER.error("RCT reader main loop crashed: %s", str(exc))
            finally:
                await self._close_connection()

            if self._running:
                LOGGER.warning(
                    "RCT reader reconnecting in %s seconds",
                    RECONNECT_DELAY,
                )
                await asyncio.sleep(RECONNECT_DELAY)

    async def _connect(self) -> None:
        async with self._connection_lock:
            await self._close_connection()

            LOGGER.debug("RCT reader opening TCP connection...")

            async with asyncio.timeout(CONNECTION_TIMEOUT):
                self._reader, self._writer = await open_connection(
                    host=self._hostname,
                    port=self._port,
                )

            if self._reader.at_eof():
                raise UpdateFailed("Reader stream closed immediately after connect")

            LOGGER.debug("RCT reader connected")

    async def _close_connection(self) -> None:
        if self._writer:
            self._writer.close()
            try:
                await self._writer.wait_closed()
            except Exception:
                pass

        self._reader = None
        self._writer = None

    # ===============================
    # READ LOOP
    # ===============================

    async def _read_loop(self) -> None:
        assert self._reader is not None

        while self._running and not self._reader.at_eof():
            frame = ReceiveFrame()

            while not frame.complete() and not self._reader.at_eof():
                data = await self._reader.read(1)
                if data:
                    frame.consume(data)

            if not frame.complete():
                await asyncio.sleep(IDLE_SLEEP_SECONDS)
                continue

            try:
                info = REGISTRY.get_by_id(frame.id)
                data_type = info.response_data_type
                name = info.name

                value: ReaderValue = decode_value(
                    data_type,  # type: ignore[arg-type]
                    frame.data,
                )  # type: ignore[arg-type]

                self._cache[frame.id] = ReaderCacheEntry(
                    object_id=frame.id,
                    value=value,
                    time=datetime.now(),
                )

                LOGGER.debug(
                    "Reader stream update %x (%s): %s",
                    frame.id,
                    name,
                    value,
                )

            except Exception as exc:
                LOGGER.debug(
                    "Reader decode error for object %x: %s",
                    frame.id,
                    str(exc),
                )

        raise UpdateFailed("RCT read loop ended")

    # ===============================
    # REQUEST LOOP
    # ===============================

    async def _request_loop(self) -> None:
        assert self._writer is not None

        last_frequent = 0.0
        last_infrequent = 0.0
        loop = asyncio.get_running_loop()

        while self._running:
            now = loop.time()

            did_work = False

            if now - last_frequent >= FREQUENT_INTERVAL_SECONDS:
                await self._request_objects(self.FREQUENT_OBJECT_IDS, "frequent")
                last_frequent = loop.time()
                did_work = True

            if now - last_infrequent >= INFREQUENT_INTERVAL_SECONDS:
                await self._request_objects(self.INFREQUENT_OBJECT_IDS, "infrequent")
                last_infrequent = loop.time()
                did_work = True

            if not did_work:
                await asyncio.sleep(IDLE_SLEEP_SECONDS)

    async def _request_objects(
        self,
        object_ids: tuple[int, ...],
        label: str,
    ) -> None:
        assert self._writer is not None

        LOGGER.debug("Reader starting %s request cycle", label)

        for object_id in object_ids:
            info = REGISTRY.get_by_id(object_id)
            name = info.name

            try:
                frame = SendFrame(command=Command.READ, id=object_id)
                self._writer.write(frame.data)
                await self._writer.drain()

                LOGGER.debug(
                    "Reader requested object %x (%s) in %s cycle",
                    object_id,
                    name,
                    label,
                )

            except Exception as exc:
                LOGGER.error(
                    "Reader failed to request object %x (%s): %s",
                    object_id,
                    name,
                    str(exc),
                )
                raise UpdateFailed(
                    f"Failed to request object {object_id:x} ({name})"
                ) from exc

            await asyncio.sleep(INTER_REQUEST_DELAY_SECONDS)