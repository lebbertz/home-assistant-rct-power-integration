"""RCT Power API client backed by a continuous central reader."""
from __future__ import annotations

import asyncio
from asyncio.locks import Lock
from dataclasses import dataclass
from datetime import datetime

from rctclient.types import EventEntry

from .reader import RctReader

type ApiResponseValue = (
    bool
    | bytes
    | float
    | int
    | str
    | tuple[datetime, dict[datetime, int]]
    | tuple[datetime, dict[datetime, EventEntry]]
)


@dataclass
class BaseApiResponse:
    object_id: int
    time: datetime


@dataclass
class ValidApiResponse(BaseApiResponse):
    value: ApiResponseValue


@dataclass
class InvalidApiResponse(BaseApiResponse):
    cause: str


type ApiResponse = ValidApiResponse | InvalidApiResponse
type RctPowerData = dict[int, ApiResponse]

INVERTER_SN_OID = 0x7924ABD9

# How long async_get_data should wait for initial reader warmup / cache population
INITIAL_CACHE_WAIT_SECONDS = 2.0
INITIAL_CACHE_POLL_INTERVAL = 0.1


def get_valid_response_value_or[_R](
    response: ApiResponse | None, defaultValue: _R
) -> ApiResponseValue | _R:
    if isinstance(response, ValidApiResponse):
        return response.value
    return defaultValue


class RctPowerApiClient:
    def __init__(self, hostname: str, port: int) -> None:
        self._hostname = hostname
        self._port = port

        self._connection_lock = Lock()
        self._reader = RctReader(hostname, port)
        self._reader_started = False

    async def _ensure_reader_started(self) -> None:
        if self._reader_started:
            return

        async with self._connection_lock:
            if self._reader_started:
                return

            await self._reader.start()
            self._reader_started = True

    async def _wait_for_any_requested_data(self, object_ids: list[int]) -> None:
        """
        On startup, give the continuous reader a short chance to populate cache.
        This avoids returning all entities as unavailable immediately after HA start.
        """
        deadline = asyncio.get_running_loop().time() + INITIAL_CACHE_WAIT_SECONDS

        while asyncio.get_running_loop().time() < deadline:
            for object_id in object_ids:
                if self._reader.get(object_id) is not None:
                    return

            await asyncio.sleep(INITIAL_CACHE_POLL_INTERVAL)

    async def get_serial_number(self) -> str | None:
        await self._ensure_reader_started()

        # Give the reader a short chance to see inverter_sn
        deadline = asyncio.get_running_loop().time() + INITIAL_CACHE_WAIT_SECONDS
        while asyncio.get_running_loop().time() < deadline:
            entry = self._reader.get(INVERTER_SN_OID)
            if entry is not None and isinstance(entry.value, str):
                return entry.value
            await asyncio.sleep(INITIAL_CACHE_POLL_INTERVAL)

        return None

    async def async_get_data(self, object_ids: list[int]) -> RctPowerData:
        await self._ensure_reader_started()

        # Especially after HA start, give the reader a moment to fill cache
        await self._wait_for_any_requested_data(object_ids)

        results: RctPowerData = {}
        now = datetime.now()

        for object_id in object_ids:
            entry = self._reader.get(object_id)

            if entry is None:
                results[object_id] = InvalidApiResponse(
                    object_id=object_id,
                    time=now,
                    cause="CACHE_MISS_OR_STALE",
                )
            else:
                results[object_id] = ValidApiResponse(
                    object_id=object_id,
                    time=entry.time,
                    value=entry.value,
                )

        return results