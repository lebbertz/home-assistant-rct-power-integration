from __future__ import annotations

import re

from homeassistant.components.sensor import SensorDeviceClass, SensorStateClass
from rctclient.registry import REGISTRY

from ..const import EntityUpdatePriority
from .device_info_helpers import get_battery_device_info, get_inverter_device_info
from .entity import (
    RctPowerBitfieldSensorEntityDescription,
    RctPowerSensorEntityDescription,
)
from .state_helpers import (
    available_battery_status,
    get_first_api_response_value_as_absolute_state,
    get_first_api_response_value_as_battery_status,
    get_first_api_response_value_as_timestamp,
    sum_api_response_values_as_state,
)


def get_matching_names(expression: str) -> list[str]:
    compiled_expression = re.compile(expression)
    return [
        object_info.name
        for object_info in REGISTRY.all()
        if compiled_expression.match(object_info.name) is not None
    ]


battery_sensor_entity_descriptions: list[RctPowerSensorEntityDescription] = [
    
    RctPowerSensorEntityDescription(
        get_device_info=get_battery_device_info,
        key="battery.temperature",
        name="Battery Temperature",
        update_priority=EntityUpdatePriority.FREQUENT,
        state_class=SensorStateClass.MEASUREMENT,
    ),
    RctPowerSensorEntityDescription(
        get_device_info=get_battery_device_info,
        key="battery.stored_energy",
        name="Battery Stored Energy",
        update_priority=EntityUpdatePriority.FREQUENT,
        state_class=SensorStateClass.TOTAL_INCREASING,
    ),
    RctPowerSensorEntityDescription(
        get_device_info=get_battery_device_info,
        key="battery.used_energy",
        name="Battery Used Energy",
        update_priority=EntityUpdatePriority.FREQUENT,
        state_class=SensorStateClass.TOTAL_INCREASING,
    ),
    RctPowerSensorEntityDescription(
        get_device_info=get_battery_device_info,
        key="battery.soc",
        name="Battery State of Charge",
        update_priority=EntityUpdatePriority.FREQUENT,
        state_class=SensorStateClass.MEASUREMENT,
        device_class=SensorDeviceClass.BATTERY,
    ),
    RctPowerSensorEntityDescription(
        get_device_info=get_battery_device_info,
        key="battery.cycles",
        name="Battery Cycles",
        update_priority=EntityUpdatePriority.INFREQUENT,
        state_class=SensorStateClass.TOTAL_INCREASING,
    ),
]

inverter_sensor_entity_descriptions: list[RctPowerSensorEntityDescription] = [

    RctPowerSensorEntityDescription(
        get_device_info=get_inverter_device_info,
        key="dc_conv.dc_conv_struct[0].p_dc",
        name="Generator A Power",
        state_class=SensorStateClass.MEASUREMENT,
    ),
    RctPowerSensorEntityDescription(
        get_device_info=get_inverter_device_info,
        key="dc_conv.dc_conv_struct[1].p_dc",
        name="Generator B Power",
        state_class=SensorStateClass.MEASUREMENT,
    ),
    RctPowerSensorEntityDescription(
        get_device_info=get_inverter_device_info,
        key="dc_conv.dc_conv_struct.p_dc",
        object_names=[
            "dc_conv.dc_conv_struct[0].p_dc",
            "dc_conv.dc_conv_struct[1].p_dc",
        ],
        name="All Generators Power",
        state_class=SensorStateClass.MEASUREMENT,
        get_native_value=sum_api_response_values_as_state,
    ),
    RctPowerSensorEntityDescription(
        get_device_info=get_inverter_device_info,
        key="g_sync.p_ac_load_sum_lp",
        name="Consumer Power",
        state_class=SensorStateClass.MEASUREMENT,
    ),
    RctPowerSensorEntityDescription(
        get_device_info=get_inverter_device_info,
        key="g_sync.p_acc_lp",
        name="Battery Power",
        state_class=SensorStateClass.MEASUREMENT,
    ),
    RctPowerSensorEntityDescription(
        get_device_info=get_inverter_device_info,
        key="g_sync.p_ac_grid_sum_lp",
        name="Grid Power",
        state_class=SensorStateClass.MEASUREMENT,
    ),
    RctPowerSensorEntityDescription(
        get_device_info=get_inverter_device_info,
        key="energy.e_load_total",
        name="Consumer Energy Consumption Total",
        update_priority=EntityUpdatePriority.INFREQUENT,
        state_class=SensorStateClass.TOTAL_INCREASING,
    ),
    RctPowerSensorEntityDescription(
        get_device_info=get_inverter_device_info,
        key="energy.e_grid_load_total",
        name="Grid Energy Consumption Total",
        update_priority=EntityUpdatePriority.INFREQUENT,
        state_class=SensorStateClass.TOTAL_INCREASING,
    ),
    RctPowerSensorEntityDescription(
        get_device_info=get_inverter_device_info,
        key="energy.e_grid_feed_absolute_total",
        unique_id="energy.e_grid_feed_absolute_total",
        object_names=["energy.e_grid_feed_total"],
        name="Grid Energy Production Absolute Total",
        update_priority=EntityUpdatePriority.INFREQUENT,
        state_class=SensorStateClass.TOTAL_INCREASING,
        get_native_value=get_first_api_response_value_as_absolute_state,
    ),
]

bitfield_sensor_entity_descriptions: list[RctPowerBitfieldSensorEntityDescription] = [

]

sensor_entity_descriptions = [
    *battery_sensor_entity_descriptions,
    *inverter_sensor_entity_descriptions,
    *bitfield_sensor_entity_descriptions,
]

all_entity_descriptions = [
    *sensor_entity_descriptions,
]
