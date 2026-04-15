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

INITIAL_CACHE_WAIT_SECONDS = 4.0
INITIAL_CACHE_POLL_INTERVAL = 0.1

# Keep last valid values at API level for a short time so HA does not
# immediately flip to unavailable on every brief reader hiccup.
API_FALLBACK_MAX_AGE_SECONDS = 30

# Infrequent values may miss one full cycle before becoming unavailable
INFREQUENT_MISS_THRESHOLD = 2

# Frequent core objects that should ideally exist before first HA update
FREQUENT_CORE_OBJECT_IDS: tuple[int, ...] = (
    0x1AC87AA0,  # g_sync.p_ac_load_sum_lp
    0x91617C58,  # g_sync.p_ac_grid_sum_lp
    0xAA9AA253,  # dc_conv.dc_conv_struct[1].p_dc
    0xB5317B78,  # dc_conv.dc_conv_struct[0].p_dc
    0x400F015B,  # g_sync.p_acc_lp
    0x959930BF,  # battery.soc
)

# Infrequent objects that may miss one refresh cycle before becoming unavailable
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

        # Last valid values seen by the API layer
        self._last_valid_api_values: dict[int, ValidApiResponse] = {}

        # Consecutive miss counter for infrequent objects
        self._infrequent_miss_counts: dict[int, int] = {}

    async def _ensure_reader_started(self) -> None:
        if self._reader_started:
            return

        async with self._connection_lock:
            if self._reader_started:
                return

            await self._reader.start()
            self._reader_started = True

    async def _wait_for_initial_frequent_cache(self, object_ids: list[int]) -> None:
        """
        Wait briefly until the frequent core values are actually present.
        This prevents HA from getting an early half-empty cache right after startup.
        """
        requested_frequent = [
            object_id for object_id in object_ids if object_id in FREQUENT_CORE_OBJECT_IDS
        ]

        if not requested_frequent:
            return

        deadline = asyncio.get_running_loop().time() + INITIAL_CACHE_WAIT_SECONDS

        while asyncio.get_running_loop().time() < deadline:
            ready_count = 0
            for object_id in requested_frequent:
                if self._reader.get(object_id) is not None:
                    ready_count += 1

            if ready_count == len(requested_frequent):
                return

            await asyncio.sleep(INITIAL_CACHE_POLL_INTERVAL)

    def _get_recent_api_fallback(self, object_id: int) -> ValidApiResponse | None:
        last_valid = self._last_valid_api_values.get(object_id)
        if last_valid is None:
            return None

        age_seconds = (datetime.now() - last_valid.time).total_seconds()
        if age_seconds > API_FALLBACK_MAX_AGE_SECONDS:
            return None

        return last_valid

    def _is_infrequent_object(self, object_id: int) -> bool:
        return object_id in INFREQUENT_OBJECT_IDS

    def _reset_infrequent_miss(self, object_id: int) -> None:
        if object_id in self._infrequent_miss_counts:
            self._infrequent_miss_counts[object_id] = 0

    def _increment_infrequent_miss(self, object_id: int) -> int:
        current = self._infrequent_miss_counts.get(object_id, 0) + 1
        self._infrequent_miss_counts[object_id] = current
        return current

    async def get_serial_number(self) -> str | None:
        await self._ensure_reader_started()

        deadline = asyncio.get_running_loop().time() + INITIAL_CACHE_WAIT_SECONDS
        while asyncio.get_running_loop().time() < deadline:
            entry = self._reader.get(INVERTER_SN_OID)
            if entry is not None and isinstance(entry.value, str):
                return entry.value
            await asyncio.sleep(INITIAL_CACHE_POLL_INTERVAL)

        fallback = self._get_recent_api_fallback(INVERTER_SN_OID)
        if fallback is not None and isinstance(fallback.value, str):
            return fallback.value

        return None

    async def async_get_data(self, object_ids: list[int]) -> RctPowerData:
        await self._ensure_reader_started()
        await self._wait_for_initial_frequent_cache(object_ids)

        results: RctPowerData = {}
        now = datetime.now()

        for object_id in object_ids:
            entry = self._reader.get(object_id)

            # Fresh reader value available
            if entry is not None:
                valid_response = ValidApiResponse(
                    object_id=object_id,
                    time=entry.time,
                    value=entry.value,
                )
                self._last_valid_api_values[object_id] = valid_response
                self._reset_infrequent_miss(object_id)
                results[object_id] = valid_response
                continue

            # Infrequent objects: allow one missed cycle before invalid
            if self._is_infrequent_object(object_id):
                miss_count = self._increment_infrequent_miss(object_id)
                fallback = self._get_recent_api_fallback(object_id)

                if fallback is not None and miss_count < INFREQUENT_MISS_THRESHOLD:
                    results[object_id] = fallback
                    continue

            # Frequent or infrequent after threshold: short API-level fallback
            fallback = self._get_recent_api_fallback(object_id)
            if fallback is not None:
                results[object_id] = fallback
                continue

            results[object_id] = InvalidApiResponse(
                object_id=object_id,
                time=now,
                cause="CACHE_MISS_OR_STALE",
            )

        return results