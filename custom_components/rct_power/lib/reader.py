"""Central reader for RCT Power communication."""
from __future__ import annotations

import asyncio
import struct
from asyncio import StreamReader, StreamWriter, open_connection
from dataclasses import dataclass
from datetime import datetime
from time import monotonic

from homeassistant.helpers.update_coordinator import UpdateFailed
from rctclient.frame import ReceiveFrame, SendFrame
from rctclient.registry import REGISTRY
from rctclient.types import Command, EventEntry
from rctclient.utils import decode_value

from ..const import LOGGER

READ_TIMEOUT = 1
CONNECTION_TIMEOUT = 20
CACHE_MAX_AGE_SECONDS = 10
RECOVERY_COOLDOWN_SECONDS = 3

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
    """Single communication endpoint for the inverter."""

    def __init__(self, hostname: str, port: int) -> None:
        self._hostname = hostname
        self._port = port

        self._reader: StreamReader | None = None
        self._writer: StreamWriter | None = None

        self._cache: dict[int, ReaderCacheEntry] = {}
        self._lock = asyncio.Lock()
        self._cooldown_until: float = 0.0

    async def _respect_cooldown(self) -> None:
        remaining = self._cooldown_until - monotonic()
        if remaining > 0:
            LOGGER.warning(
                "RCT reader is in cooldown for %.1f more seconds",
                remaining,
            )
            await asyncio.sleep(remaining)

    def _enter_cooldown(self) -> None:
        self._cooldown_until = monotonic() + RECOVERY_COOLDOWN_SECONDS
        LOGGER.warning(
            "Entering RCT reader cooldown for %s seconds",
            RECOVERY_COOLDOWN_SECONDS,
        )

    async def _ensure_connection(self) -> None:
        if self._reader is not None and self._writer is not None:
            if not self._reader.at_eof():
                return

        await self._close_connection()
        await self._respect_cooldown()

        async with asyncio.timeout(CONNECTION_TIMEOUT):
            self._reader, self._writer = await open_connection(
                host=self._hostname,
                port=self._port,
            )

        if self._reader.at_eof():
            self._enter_cooldown()
            raise UpdateFailed("Read stream closed immediately after connect")

        LOGGER.debug("RCT reader opened new TCP connection")

    async def _close_connection(self) -> None:
        if self._writer is not None:
            self._writer.close()
            try:
                await self._writer.wait_closed()
            except Exception:
                pass

        self._reader = None
        self._writer = None

    def _get_cached(self, object_id: int) -> ReaderCacheEntry | None:
        cached = self._cache.get(object_id)
        if cached is None:
            return None

        age_seconds = (datetime.now() - cached.time).total_seconds()
        if age_seconds > CACHE_MAX_AGE_SECONDS:
            self._cache.pop(object_id, None)
            return None

        return cached

    def _store_cached(self, object_id: int, value: ReaderValue) -> ReaderCacheEntry:
        entry = ReaderCacheEntry(
            object_id=object_id,
            value=value,
            time=datetime.now(),
        )
        self._cache[object_id] = entry
        return entry

    async def read_object(self, object_id: int) -> ReaderCacheEntry | None:
        """Read one object while caching every valid frame that arrives."""

        async with self._lock:
            cached = self._get_cached(object_id)
            if cached is not None:
                object_name = REGISTRY.get_by_id(object_id).name
                LOGGER.debug(
                    "Reader using cached data for object %x (%s): %s",
                    object_id,
                    object_name,
                    cached.value,
                )
                return cached

            await self._ensure_connection()

            assert self._reader is not None
            assert self._writer is not None

            object_name = REGISTRY.get_by_id(object_id).name
            request_time = datetime.now()
            frame = SendFrame(command=Command.READ, id=object_id)

            LOGGER.debug(
                "Reader requesting object %x (%s)...",
                object_id,
                object_name,
            )

            try:
                async with asyncio.timeout(READ_TIMEOUT):
                    await self._writer.drain()
                    self._writer.write(frame.data)

                    while True:
                        response_frame = ReceiveFrame()

                        while not response_frame.complete() and not self._reader.at_eof():
                            raw = await self._reader.read(1)
                            if len(raw) > 0:
                                response_frame.consume(raw)

                        if not response_frame.complete():
                            LOGGER.debug(
                                "Reader got incomplete frame for object %x (%s)",
                                object_id,
                                object_name,
                            )
                            return None

                        response_info = REGISTRY.get_by_id(response_frame.id)
                        received_name = response_info.name
                        data_type = response_info.response_data_type

                        decoded_value: ReaderValue = decode_value(
                            data_type,  # type: ignore[arg-type]
                            response_frame.data,
                        )  # type: ignore[arg-type]

                        LOGGER.debug(
                            "Reader decoded object %x (%s): %s",
                            response_frame.id,
                            received_name,
                            decoded_value,
                        )

                        self._store_cached(response_frame.id, decoded_value)

                        if response_frame.id == object_id:
                            return self._cache[object_id]

                        LOGGER.debug(
                            "Reader mismatch: requested %x (%s), received %x (%s) - cached for later use",
                            object_id,
                            object_name,
                            response_frame.id,
                            received_name,
                        )

                        cached = self._get_cached(object_id)
                        if cached is not None:
                            LOGGER.debug(
                                "Reader resolved requested object %x (%s) from cache after mismatch: %s",
                                object_id,
                                object_name,
                                cached.value,
                            )
                            return cached

            except TimeoutError:
                LOGGER.debug(
                    "Reader timeout for object %x (%s)",
                    object_id,
                    object_name,
                )
                return None
            except Exception as exc:
                LOGGER.error(
                    "Reader hard failure for object %x (%s): %s",
                    object_id,
                    object_name,
                    str(exc),
                )
                self._enter_cooldown()
                await self._close_connection()
                raise UpdateFailed(f"Reader hard failure: {str(exc)}") from exc