from __future__ import annotations

import json
import re
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from deckr.hardware.capabilities import (
    button_activation_value_schema,
    button_momentary_value_schema,
)
from deckr.hardware.descriptors import (
    DECKR_INPUT_BUTTON,
    CapabilityDescriptor,
    ControlDescriptor,
    ControlGeometry,
)

DEFAULT_BASE_TOPIC = "zigbee2mqtt"
DEFAULT_DEDUPE_MS = 250
QOS = 2

BUTTON_PRESS_CAPABILITY_ID = "button.press"
BUTTON_MOMENTARY_CAPABILITY_ID = "button.momentary"
SIMPLE_PRESS_ACTIONS = frozenset({"on", "off", "toggle", "store", "recall"})
_TOKEN_RE = re.compile(r"[^a-z0-9_]+")
_STORE_RECALL_RE = re.compile(r"^(store|recall)_\d+$")
_DIRECTIONAL_AXIS_ACTION_RE = re.compile(
    r"^(?P<axis>brightness|color_temperature)_(?P<kind>step|move)_(?P<direction>up|down)$"
)


@dataclass(frozen=True, slots=True)
class Zigbee2MqttDevice:
    friendly_name: str
    topic: str
    ieee_address: str | None = None
    vendor: str | None = None
    model: str | None = None
    model_id: str | None = None
    description: str | None = None
    installed_description: str | None = None
    device_type: str | None = None
    power_source: str | None = None
    interview_state: str | None = None
    supported: bool | None = None
    disabled: bool | None = None
    exposes: tuple[str, ...] = ()
    actions: tuple[str, ...] = ()

    @property
    def display_name(self) -> str:
        return (
            self.installed_description
            or self.description
            or self.model
            or self.friendly_name
        )

    @property
    def device_id(self) -> str:
        if self.ieee_address:
            return f"z2m.{self.ieee_address}"
        return f"z2m.{contract_token(self.friendly_name)}"

    @property
    def fingerprint(self) -> str:
        if self.ieee_address:
            return f"zigbee2mqtt:{self.ieee_address}"
        return f"zigbee2mqtt:{self.topic}"

    @property
    def is_action_device(self) -> bool:
        return self.disabled is not True and self.supported is not False and bool(self.actions)

    def to_dict(self) -> dict[str, Any]:
        return {
            "friendly_name": self.friendly_name,
            "topic": self.topic,
            "ieee_address": self.ieee_address,
            "vendor": self.vendor,
            "model": self.model,
            "model_id": self.model_id,
            "description": self.description,
            "installed_description": self.installed_description,
            "type": self.device_type,
            "power_source": self.power_source,
            "interview_state": self.interview_state,
            "supported": self.supported,
            "disabled": self.disabled,
            "exposes": list(self.exposes),
            "actions": list(self.actions),
        }


@dataclass(frozen=True, slots=True)
class InferredAction:
    action: str
    control_id: str | None
    capability_id: str
    event_type: str
    axis: str | None = None
    stop: bool = False


@dataclass(frozen=True, slots=True)
class InferredControl:
    control_id: str
    capability_ids: tuple[str, ...]
    actions: tuple[InferredAction, ...]


def zigbee2mqtt_device_topic(base_topic: str, friendly_name: str) -> str:
    return f"{base_topic.rstrip('/')}/{friendly_name.strip('/')}"


def contract_token(value: str) -> str:
    token = _TOKEN_RE.sub("_", value.lower()).strip("_")
    if not token:
        return "unknown"
    if not token[0].isalpha():
        return f"device_{token}"
    return token


def payload_text(payload: bytes | bytearray | str | object) -> str:
    if isinstance(payload, bytes | bytearray):
        return bytes(payload).decode("utf-8", errors="replace").strip()
    if isinstance(payload, str):
        return payload.strip()
    return str(payload).strip()


def topic_text(topic: object) -> str:
    value = getattr(topic, "value", None)
    return str(value if value is not None else topic)


def unique(values: Sequence[str]) -> tuple[str, ...]:
    return tuple(dict.fromkeys(value for value in values if value))


def optional_string(value: object) -> str | None:
    return value if isinstance(value, str) and value else None


def _optional_bool(value: object) -> bool | None:
    return value if isinstance(value, bool) else None


def _definition(device: Mapping[str, Any]) -> Mapping[str, Any]:
    definition = device.get("definition")
    return definition if isinstance(definition, Mapping) else {}


def _expose_name(expose: Mapping[str, Any]) -> str | None:
    for key in ("property", "name", "type"):
        value = expose.get(key)
        if isinstance(value, str) and value:
            return value
    return None


def collect_expose_details(exposes: object) -> tuple[tuple[str, ...], tuple[str, ...]]:
    names: list[str] = []
    actions: list[str] = []
    if not isinstance(exposes, list):
        return (), ()
    for expose in exposes:
        if not isinstance(expose, Mapping):
            continue
        name = _expose_name(expose)
        if name is not None:
            names.append(name)
        values = expose.get("values")
        if name == "action" and isinstance(values, list):
            actions.extend(value for value in values if isinstance(value, str) and value)
        for nested_key in ("features", "exposes"):
            nested_names, nested_actions = collect_expose_details(expose.get(nested_key))
            names.extend(nested_names)
            actions.extend(nested_actions)
    return unique(names), unique(actions)


def parse_bridge_devices(
    payload: bytes | str,
    *,
    base_topic: str = DEFAULT_BASE_TOPIC,
) -> tuple[Zigbee2MqttDevice, ...]:
    raw = payload_text(payload)
    data = json.loads(raw)
    if not isinstance(data, list):
        raise ValueError("zigbee2mqtt bridge/devices payload must be a JSON list")

    devices: list[Zigbee2MqttDevice] = []
    for item in data:
        if not isinstance(item, Mapping):
            continue
        friendly_name = item.get("friendly_name")
        if not isinstance(friendly_name, str) or not friendly_name:
            continue
        definition = _definition(item)
        exposes, actions = collect_expose_details(definition.get("exposes"))
        devices.append(
            Zigbee2MqttDevice(
                friendly_name=friendly_name,
                topic=zigbee2mqtt_device_topic(base_topic, friendly_name),
                ieee_address=optional_string(item.get("ieee_address")),
                vendor=optional_string(definition.get("vendor")),
                model=optional_string(definition.get("model")),
                model_id=optional_string(item.get("model_id")),
                description=optional_string(definition.get("description")),
                installed_description=optional_string(item.get("description")),
                device_type=optional_string(item.get("type")),
                power_source=optional_string(item.get("power_source")),
                interview_state=optional_string(item.get("interview_state")),
                supported=_optional_bool(item.get("supported")),
                disabled=_optional_bool(item.get("disabled")),
                exposes=exposes,
                actions=actions,
            )
        )
    return tuple(devices)


def json_safe(value: object) -> object:
    if isinstance(value, Mapping):
        return {
            str(key): json_safe(item)
            for key, item in value.items()
            if item is not None
        }
    if isinstance(value, list | tuple):
        return [json_safe(item) for item in value]
    if isinstance(value, str | int | float | bool) or value is None:
        return value
    return str(value)


def _top_level_metadata(device: Mapping[str, Any]) -> dict[str, Any]:
    keys = (
        "ieee_address",
        "type",
        "network_address",
        "supported",
        "disabled",
        "description",
        "power_source",
        "date_code",
        "model_id",
        "interview_state",
    )
    return {
        key: json_safe(value)
        for key in keys
        if (value := device.get(key)) is not None
    }


def _expose_metadata(exposes: object) -> list[object]:
    value = json_safe(exposes)
    return value if isinstance(value, list) else []


def normalize_bridge_device_metadata(
    device: Mapping[str, Any],
    *,
    base_topic: str = DEFAULT_BASE_TOPIC,
) -> dict[str, Any] | None:
    friendly_name = device.get("friendly_name")
    if not isinstance(friendly_name, str) or not friendly_name:
        return None

    definition = _definition(device)
    exposes = definition.get("exposes")
    options = definition.get("options")
    expose_names, actions = collect_expose_details(exposes)
    option_names, _option_actions = collect_expose_details(options)

    metadata: dict[str, Any] = {
        "friendly_name": friendly_name,
        "topic": zigbee2mqtt_device_topic(base_topic, friendly_name),
        **_top_level_metadata(device),
    }
    if definition:
        metadata["definition"] = {
            key: json_safe(value)
            for key, value in {
                "source": definition.get("source"),
                "vendor": definition.get("vendor"),
                "model": definition.get("model"),
                "description": definition.get("description"),
            }.items()
            if value is not None
        }
        metadata["definition"]["exposes"] = _expose_metadata(exposes)
        metadata["definition"]["expose_names"] = list(expose_names)
        metadata["definition"]["actions"] = list(actions)
        metadata["definition"]["options"] = _expose_metadata(options)
        metadata["definition"]["option_names"] = list(option_names)

    endpoints = device.get("endpoints")
    if isinstance(endpoints, Mapping):
        metadata["endpoints"] = json_safe(endpoints)

    return metadata


def parse_bridge_device_metadata(
    payload: bytes | str,
    *,
    base_topic: str = DEFAULT_BASE_TOPIC,
) -> tuple[dict[str, Any], ...]:
    raw = payload_text(payload)
    data = json.loads(raw)
    if not isinstance(data, list):
        raise ValueError("zigbee2mqtt bridge/devices payload must be a JSON list")
    return tuple(
        metadata
        for item in data
        if isinstance(item, Mapping)
        and (
            metadata := normalize_bridge_device_metadata(
                item,
                base_topic=base_topic,
            )
        )
        is not None
    )


def extract_action_values(payload: bytes | str, *, include_empty: bool = False) -> tuple[str, ...]:
    raw = payload_text(payload)
    if not raw:
        return ("",) if include_empty else ()
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return (raw,) if raw or include_empty else ()

    if isinstance(data, Mapping) and "action" in data:
        action = data.get("action")
        if isinstance(action, str) and (action or include_empty):
            return (action,)
        return ()
    if isinstance(data, str) and (data or include_empty):
        return (data,)
    return ()


def infer_actions(actions: Sequence[str]) -> tuple[InferredAction, ...]:
    known = set(actions)
    inferred: list[InferredAction] = []
    for action in actions:
        mapping = _infer_action(action, known)
        if mapping is not None:
            inferred.append(mapping)
    return tuple(inferred)


def infer_controls(actions: Sequence[str]) -> tuple[InferredControl, ...]:
    by_control: dict[str, list[InferredAction]] = defaultdict(list)
    for mapping in infer_actions(actions):
        if mapping.control_id is not None:
            by_control[mapping.control_id].append(mapping)

    controls: list[InferredControl] = []
    for control_id, mappings in by_control.items():
        capability_ids = unique(tuple(mapping.capability_id for mapping in mappings))
        controls.append(
            InferredControl(
                control_id=control_id,
                capability_ids=capability_ids,
                actions=tuple(mappings),
            )
        )
    return tuple(controls)


def build_controls(actions: Sequence[str]) -> tuple[ControlDescriptor, ...]:
    controls: list[ControlDescriptor] = []
    for index, control in enumerate(infer_controls(actions)):
        capabilities: list[CapabilityDescriptor] = []
        if BUTTON_PRESS_CAPABILITY_ID in control.capability_ids:
            capabilities.append(activation_button_capability())
        if BUTTON_MOMENTARY_CAPABILITY_ID in control.capability_ids:
            capabilities.append(momentary_button_capability())
        controls.append(
            ControlDescriptor(
                controlId=control.control_id,
                kind="button",
                label=control.control_id,
                geometry=ControlGeometry(x=index, y=0, width=1, height=1, unit="grid"),
                inputCapabilities=tuple(capabilities),
                sources=(),
            )
        )
    return tuple(controls)


def render_inferred_controls(device: Zigbee2MqttDevice) -> str:
    controls = [
        {
            "control_id": control.control_id,
            "capabilities": list(control.capability_ids),
            "actions": [
                {
                    "action": action.action,
                    "capability_id": action.capability_id,
                    "event_type": action.event_type,
                }
                for action in control.actions
            ],
        }
        for control in infer_controls(device.actions)
    ]
    return json.dumps(
        {
            "friendly_name": device.friendly_name,
            "topic": device.topic,
            "device_id": device.device_id,
            "fingerprint": device.fingerprint,
            "controls": controls,
        },
        indent=2,
    )


def activation_button_capability() -> CapabilityDescriptor:
    return CapabilityDescriptor(
        capabilityId=BUTTON_PRESS_CAPABILITY_ID,
        family=DECKR_INPUT_BUTTON,
        type="activation",
        direction="input",
        access=("emits",),
        valueSchema=button_activation_value_schema(),
        eventTypes=("press",),
    )


def momentary_button_capability() -> CapabilityDescriptor:
    return CapabilityDescriptor(
        capabilityId=BUTTON_MOMENTARY_CAPABILITY_ID,
        family=DECKR_INPUT_BUTTON,
        type="momentary",
        direction="input",
        access=("emits",),
        valueSchema=button_momentary_value_schema(),
        eventTypes=("down", "up"),
    )


def _infer_action(action: str, known: set[str]) -> InferredAction | None:
    directional = _infer_directional_action(action)
    if directional is not None:
        return directional
    simple = _infer_simple_action(action)
    if simple is not None:
        return simple
    return _infer_slot_action(action, known)


def _infer_simple_action(action: str) -> InferredAction | None:
    if action in SIMPLE_PRESS_ACTIONS or _STORE_RECALL_RE.fullmatch(action):
        return InferredAction(
            action=action,
            control_id=contract_token(action),
            capability_id=BUTTON_PRESS_CAPABILITY_ID,
            event_type="press",
        )
    return None


def _infer_directional_action(action: str) -> InferredAction | None:
    match = _DIRECTIONAL_AXIS_ACTION_RE.fullmatch(action)
    if match is not None:
        axis = match.group("axis")
        direction = match.group("direction")
        kind = match.group("kind")
        return InferredAction(
            action=action,
            control_id=f"{axis}_{direction}",
            capability_id=(
                BUTTON_PRESS_CAPABILITY_ID
                if kind == "step"
                else BUTTON_MOMENTARY_CAPABILITY_ID
            ),
            event_type="press" if kind == "step" else "down",
            axis=axis if kind == "move" else None,
        )
    axis = _stop_axis(action)
    if axis is not None:
        return InferredAction(
            action=action,
            control_id=None,
            capability_id=BUTTON_MOMENTARY_CAPABILITY_ID,
            event_type="up",
            axis=axis,
            stop=True,
        )
    return None


def _stop_axis(action: str) -> str | None:
    if action == "brightness_stop":
        return "brightness"
    if action == "color_temperature_stop" or action == "color_temperature_move_stop":
        return "color_temperature"
    if action.endswith("_move_stop"):
        return action.removesuffix("_move_stop")
    if action.endswith("_stop"):
        return action.removesuffix("_stop")
    return None


def _infer_slot_action(action: str, known: set[str]) -> InferredAction | None:
    if action.endswith("_press_release"):
        return _slot_mapping(
            action,
            action.removesuffix("_press_release"),
            BUTTON_MOMENTARY_CAPABILITY_ID,
            "up",
        )
    if action.endswith("_hold_release"):
        return _slot_mapping(
            action,
            action.removesuffix("_hold_release"),
            BUTTON_MOMENTARY_CAPABILITY_ID,
            "up",
        )
    if action.endswith("_release"):
        return _slot_mapping(
            action,
            action.removesuffix("_release"),
            BUTTON_MOMENTARY_CAPABILITY_ID,
            "up",
        )
    if action.endswith("_click"):
        return _slot_mapping(
            action,
            action.removesuffix("_click"),
            BUTTON_PRESS_CAPABILITY_ID,
            "press",
        )
    if action.endswith("_hold"):
        return _slot_mapping(
            action,
            action.removesuffix("_hold"),
            BUTTON_MOMENTARY_CAPABILITY_ID,
            "down",
        )
    if action.endswith("_press"):
        slot = action.removesuffix("_press")
        has_release = f"{slot}_press_release" in known or f"{slot}_release" in known
        return _slot_mapping(
            action,
            slot,
            BUTTON_MOMENTARY_CAPABILITY_ID if has_release else BUTTON_PRESS_CAPABILITY_ID,
            "down" if has_release else "press",
        )
    return None


def _slot_mapping(
    action: str,
    slot: str,
    capability_id: str,
    event_type: str,
) -> InferredAction:
    return InferredAction(
        action=action,
        control_id=contract_token(slot),
        capability_id=capability_id,
        event_type=event_type,
    )
