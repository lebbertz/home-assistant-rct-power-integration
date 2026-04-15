"""RCT Power API client."""
from __future__ import annotations

import asyncio
import struct
from asyncio import StreamReader, StreamWriter, open_connection
from asyncio.locks import Lock
from dataclasses import dataclass
from datetime import datetime
from time import monotonic

from homeassistant.helpers.update_coordinator import UpdateFailed
from rctclient.exceptions import FrameCRCMismatch, FrameLengthExceeded, InvalidCommand
from rctclient.frame import ReceiveFrame, SendFrame
from rctclient.registry import REGISTRY
from rctclient.types import Command, EventEntry
from rctclient.utils import decode_value

from ..const import LOGGER

CONNECTION_TIMEOUT = 20
READ_TIMEOUT = 1
CACHE_MAX_AGE_SECONDS = 6

# Recovery behavior
POLL_CYCLE_MAX_SECONDS = 6
RECOVERY_COOLDOWN_SECONDS = 3
MAX_INVALID_RESPONSES_PER_CYCLE = 8

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

        self._connection_lock = Lock()
        self._response_cache: dict[int, ValidApiResponse] = {}
        self._cooldown_until: float = 0.0

    async def _respect_cooldown(self) -> None:
        remaining = self._cooldown_until - monotonic()
        if remaining > 0:
            LOGGER.warning(
                "RCT client is in recovery cooldown for %.1f more seconds before reconnect",
                remaining,
            )
            await asyncio.sleep(remaining)

    def _enter_cooldown(self) -> None:
        self._cooldown_until = monotonic() + RECOVERY_COOLDOWN_SECONDS
        LOGGER.warning(
            "Entering RCT recovery cooldown for %s seconds",
            RECOVERY_COOLDOWN_SECONDS,
        )

    def _get_cached_response(self, object_id: int) -> ValidApiResponse | None:
        cached = self._response_cache.get(object_id)
        if cached is None:
            return None

        age_seconds = (datetime.now() - cached.time).total_seconds()
        if age_seconds > CACHE_MAX_AGE_SECONDS:
            self._response_cache.pop(object_id, None)
            return None

        return cached

    def _store_cached_response(
        self, object_id: int, value: ApiResponseValue, response_time: datetime
    ) -> ValidApiResponse:
        response = ValidApiResponse(
            object_id=object_id,
            time=response_time,
            value=value,
        )
        self._response_cache[object_id] = response
        return response

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

            async with asyncio.timeout(CONNECTION_TIMEOUT):
                reader: StreamReader | None = None
                writer: StreamWriter | None = None

                try:
                    reader, writer = await open_connection(
                        host=self._hostname, port=self._port
                    )

                    if reader.at_eof():
                        self._enter_cooldown()
                        raise UpdateFailed("Read stream closed immediately after connect")

                    for object_id in object_ids:
                        # Check before starting next read
                        self._check_cycle_limits(
                            cycle_start=cycle_start,
                            invalid_count=invalid_count,
                            current_object_id=object_id,
                        )

                        response = await self._read_object(
                            reader=reader,
                            writer=writer,
                            object_id=object_id,
                        )
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
                            # valid responses reduce severity of the current cycle
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
                        "RCT hard communication failure, forcing cooldown and reconnect: %s",
                        str(exc),
                    )
                    self._enter_cooldown()
                    raise UpdateFailed(
                        f"Hard RCT communication failure: {str(exc)}"
                    ) from exc
                finally:
                    if writer is not None:
                        writer.close()
                        try:
                            await writer.wait_closed()
                        except Exception:
                            pass

    async def _read_object(
        self, reader: StreamReader, writer: StreamWriter, object_id: int
    ) -> ApiResponse:
        object_name = REGISTRY.get_by_id(object_id).name

        cached_response = self._get_cached_response(object_id)
        if cached_response is not None:
            LOGGER.debug(
                "Using cached RCT Power data for object %x (%s): %s",
                object_id,
                object_name,
                cached_response.value,
            )
            return cached_response

        read_command_frame = SendFrame(command=Command.READ, id=object_id)

        LOGGER.debug(
            "Requesting RCT Power data for object %x (%s)...",
            object_id,
            object_name,
        )

        request_time = datetime.now()

        try:
            async with asyncio.timeout(READ_TIMEOUT):
                await writer.drain()
                writer.write(read_command_frame.data)

                while True:
                    response_frame = ReceiveFrame()

                    while not response_frame.complete() and not reader.at_eof():
                        raw_response = await reader.read(1)
                        if len(raw_response) > 0:
                            response_frame.consume(raw_response)

                    if response_frame.complete():
                        response_object_info = REGISTRY.get_by_id(response_frame.id)
                        data_type = response_object_info.response_data_type
                        received_object_name = response_object_info.name

                        decoded_value: ApiResponseValue = decode_value(
                            data_type,  # type: ignore[arg-type]
                            response_frame.data,
                        )  # type: ignore[arg-type]

                        LOGGER.debug(
                            "Decoded data for object %x (%s): %s",
                            response_frame.id,
                            received_object_name,
                            decoded_value,
                        )

                        response_time = datetime.now()

                        self._store_cached_response(
                            object_id=response_frame.id,
                            value=decoded_value,
                            response_time=response_time,
                        )

                        if object_id != response_frame.id:
                            LOGGER.debug(
                                "Mismatch of requested and received object ids: requested %x (%s), but received %x (%s) - cached for later use",
                                object_id,
                                object_name,
                                response_frame.id,
                                received_object_name,
                            )

                            cached_response = self._get_cached_response(object_id)
                            if cached_response is not None:
                                LOGGER.debug(
                                    "Resolved requested object %x (%s) from cache after mismatch: %s",
                                    object_id,
                                    object_name,
                                    cached_response.value,
                                )
                                return cached_response

                            continue

                        return ValidApiResponse(
                            object_id=object_id,
                            time=response_time,
                            value=decoded_value,
                        )

                    LOGGER.debug(
                        "Error decoding object %x (%s): %s",
                        object_id,
                        object_name,
                        response_frame.data,
                    )
                    return InvalidApiResponse(
                        object_id=object_id,
                        time=request_time,
                        cause="INCOMPLETE",
                    )

        except TimeoutError as exc:
            LOGGER.debug(
                "Error reading object %x (%s): %s",
                object_id,
                object_name,
                str(exc),
            )
            return InvalidApiResponse(
                object_id=object_id,
                time=request_time,
                cause="OBJECT_READ_TIMEOUT",
            )
        except FrameCRCMismatch as exc:
            LOGGER.debug(
                "Error reading object %x (%s): %s",
                object_id,
                object_name,
                str(exc),
            )
            return InvalidApiResponse(
                object_id=object_id,
                time=request_time,
                cause="CRC_ERROR",
            )
        except FrameLengthExceeded as exc:
            LOGGER.debug(
                "Error reading object %x (%s): %s",
                object_id,
                object_name,
                str(exc),
            )
            return InvalidApiResponse(
                object_id=object_id,
                time=request_time,
                cause="FRAME_LENGTH_EXCEEDED",
            )
        except InvalidCommand as exc:
            LOGGER.debug(
                "Error reading object %x (%s): %s",
                object_id,
                object_name,
                str(exc),
            )
            return InvalidApiResponse(
                object_id=object_id,
                time=request_time,
                cause="INVALID_COMMAND",
            )
        except ConnectionResetError as exc:
            LOGGER.debug(
                "Error reading object %x (%s): %s",
                object_id,
                object_name,
                str(exc),
            )
            return InvalidApiResponse(
                object_id=object_id,
                time=request_time,
                cause="CONNECTION_RESET",
            )
        except struct.error as exc:
            LOGGER.debug(
                "Error reading object %x (%s): %s",
                object_id,
                object_name,
                str(exc),
            )
            return InvalidApiResponse(
                object_id=object_id,
                time=request_time,
                cause="PARSING_ERROR",
            )
        except Exception as exc:
            LOGGER.debug(
                "Error reading object %x (%s): %s",
                object_id,
                object_name,
                str(exc),
            )
            return InvalidApiResponse(
                object_id=object_id,
                time=request_time,
                cause="UNKNOWN_ERROR",
            )