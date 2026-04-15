"""RCT Power API client."""
from __future__ import annotations

from asyncio.locks import Lock
from dataclasses import dataclass
from datetime import datetime
from time import monotonic

from homeassistant.helpers.update_coordinator import UpdateFailed
from rctclient.registry import REGISTRY
from rctclient.types import EventEntry

from ..const import LOGGER
from .reader import RctReader

CACHE_MAX_AGE_SECONDS = 6

# Recovery behavior at API level
POLL_CYCLE_MAX_SECONDS = 6
RECOVERY_COOLDOWN_SECONDS = 3
MAX_INVALID_RESPONSES_PER_CYCLE = 5

INVERTER_SN_OID = 0x7924ABD9

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

        # one API cycle at a time
        self._connection_lock = Lock()

        # central reader
        self._reader = RctReader(hostname, port)

        # API-level cooldown
        self._cooldown_until: float = 0.0

    async def _respect_cooldown(self) -> None:
        remaining = self._cooldown_until - monotonic()
        if remaining > 0:
            LOGGER.warning(
                "RCT API is in recovery cooldown for %.1f more seconds before next polling cycle",
                remaining,
            )
            import asyncio

            await asyncio.sleep(remaining)

    def _enter_cooldown(self) -> None:
        self._cooldown_until = monotonic() + RECOVERY_COOLDOWN_SECONDS
        LOGGER.warning(
            "Entering RCT API recovery cooldown for %s seconds",
            RECOVERY_COOLDOWN_SECONDS,
        )

    def _check_cycle_limits(
        self,
        cycle_start: float,
        invalid_count: int,
        current_object_id: int | None = None,
    ) -> None:
        elapsed = monotonic() - cycle_start

        if elapsed >= POLL_CYCLE_MAX_SECONDS:
            if current_object_id is not None:
                object_name = REGISTRY.get_by_id(current_object_id).name
                LOGGER.error(
                    "RCT FAIL-FAST TRIGGERED: aborting poll cycle after %.3f seconds at object %x (%s), invalid_count=%s",
                    elapsed,
                    current_object_id,
                    object_name,
                    invalid_count,
                )
            else:
                LOGGER.error(
                    "RCT FAIL-FAST TRIGGERED: aborting poll cycle after %.3f seconds, invalid_count=%s",
                    elapsed,
                    invalid_count,
                )

            self._enter_cooldown()
            raise UpdateFailed(
                f"Polling cycle exceeded {POLL_CYCLE_MAX_SECONDS} seconds"
            )

        if invalid_count >= MAX_INVALID_RESPONSES_PER_CYCLE:
            if current_object_id is not None:
                object_name = REGISTRY.get_by_id(current_object_id).name
                LOGGER.error(
                    "RCT FAIL-FAST TRIGGERED: aborting poll cycle after %s invalid responses at object %x (%s)",
                    invalid_count,
                    current_object_id,
                    object_name,
                )
            else:
                LOGGER.error(
                    "RCT FAIL-FAST TRIGGERED: aborting poll cycle after %s invalid responses",
                    invalid_count,
                )

            self._enter_cooldown()
            raise UpdateFailed(
                f"Polling cycle exceeded {MAX_INVALID_RESPONSES_PER_CYCLE} invalid responses"
            )

    async def get_serial_number(self) -> str | None:
        inverter_data = await self.async_get_data([INVERTER_SN_OID])
        inverter_sn_response = inverter_data.get(INVERTER_SN_OID)
        if isinstance(inverter_sn_response, ValidApiResponse) and isinstance(
            inverter_sn_response.value, str
        ):
            return inverter_sn_response.value
        return None

    async def async_get_data(self, object_ids: list[int]) -> RctPowerData:
        async with self._connection_lock:
            await self._respect_cooldown()

            cycle_start = monotonic()
            invalid_count = 0
            results: RctPowerData = {}

            try:
                for object_id in object_ids:
                    # Check before starting next read
                    self._check_cycle_limits(
                        cycle_start=cycle_start,
                        invalid_count=invalid_count,
                        current_object_id=object_id,
                    )

                    response = await self._read_object(object_id)
                    results[object_id] = response

                    if isinstance(response, InvalidApiResponse):
                        invalid_count += 1
                        object_name = REGISTRY.get_by_id(object_id).name
                        LOGGER.debug(
                            "RCT invalid response %s/%s for object %x (%s), cause=%s",
                            invalid_count,
                            MAX_INVALID_RESPONSES_PER_CYCLE,
                            object_id,
                            object_name,
                            response.cause,
                        )
                    else:
                        # a valid response resets the streak severity
                        invalid_count = 0

                    # Check again after the read completed
                    self._check_cycle_limits(
                        cycle_start=cycle_start,
                        invalid_count=invalid_count,
                        current_object_id=object_id,
                    )

                return results

            except UpdateFailed:
                raise
            except Exception as exc:
                LOGGER.error(
                    "RCT API hard failure, forcing cooldown and reconnect: %s",
                    str(exc),
                )
                self._enter_cooldown()
                raise UpdateFailed(f"Hard RCT API failure: {str(exc)}") from exc

    async def _read_object(self, object_id: int) -> ApiResponse:
        object_name = REGISTRY.get_by_id(object_id).name

        try:
            result = await self._reader.read_object(object_id)

            if result is None:
                LOGGER.debug(
                    "Reader returned no data for object %x (%s)",
                    object_id,
                    object_name,
                )
                return InvalidApiResponse(
                    object_id=object_id,
                    time=datetime.now(),
                    cause="READER_NO_DATA",
                )

            return ValidApiResponse(
                object_id=object_id,
                time=result.time,
                value=result.value,
            )

        except Exception as exc:
            LOGGER.debug(
                "Reader error for object %x (%s): %s",
                object_id,
                object_name,
                str(exc),
            )
            return InvalidApiResponse(
                object_id=object_id,
                time=datetime.now(),
                cause="READER_ERROR",
            )