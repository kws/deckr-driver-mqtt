from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, TextIO

import aiomqtt
import anyio
import yaml

from ._zigbee2mqtt import (
    DEFAULT_BASE_TOPIC,
    QOS,
    Zigbee2MqttDevice,
    extract_action_values,
    parse_bridge_device_metadata,
    parse_bridge_devices,
    payload_text,
    render_inferred_controls,
    topic_text,
    zigbee2mqtt_device_topic,
)

DEFAULT_HA_DISCOVERY_TOPIC = "homeassistant"
DEFAULT_TIMEOUT_SECONDS = 5.0


@dataclass(frozen=True, slots=True)
class BrokerOptions:
    hostname: str
    port: int = 1883
    username: str | None = None
    password: str | None = None


@dataclass(frozen=True, slots=True)
class HaDiscoveryTrigger:
    discovery_topic: str
    topic: str
    trigger_type: str
    subtype: str
    payload: str | None = None

    def key(self) -> tuple[str, str, str, str | None]:
        return self.topic, self.trigger_type, self.subtype, self.payload


@dataclass(slots=True)
class InspectState:
    seen_actions: set[str] = field(default_factory=set)
    seen_ha_triggers: set[tuple[str, str, str, str | None]] = field(default_factory=set)


def parse_ha_discovery_trigger(
    discovery_topic: str,
    payload: bytes | str,
) -> HaDiscoveryTrigger | None:
    raw = payload_text(payload)
    if not raw:
        return None
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return None
    if not isinstance(data, Mapping):
        return None
    if data.get("automation_type") != "trigger":
        return None
    topic = data.get("topic")
    trigger_type = data.get("type")
    subtype = data.get("subtype")
    if not (
        isinstance(topic, str)
        and topic
        and isinstance(trigger_type, str)
        and trigger_type
        and isinstance(subtype, str)
        and subtype
    ):
        return None
    payload_value = data.get("payload")
    return HaDiscoveryTrigger(
        discovery_topic=discovery_topic,
        topic=topic,
        trigger_type=trigger_type,
        subtype=subtype,
        payload=payload_value if isinstance(payload_value, str) else None,
    )


def action_values_for_observation(
    payload: bytes | str,
    *,
    include_empty: bool = False,
) -> tuple[str, ...]:
    return extract_action_values(payload, include_empty=include_empty)


def process_inspect_message(
    *,
    topic: str,
    payload: bytes | str,
    device_topic: str,
    ha_discovery_topic: str | None,
    unique: bool,
    include_empty: bool,
    state: InspectState,
    out: TextIO,
    timestamp: datetime | None = None,
) -> None:
    if topic == device_topic:
        now = timestamp or datetime.now()
        for action in action_values_for_observation(
            payload,
            include_empty=include_empty,
        ):
            if unique and action in state.seen_actions:
                continue
            state.seen_actions.add(action)
            print(
                f"{now:%H:%M:%S} action={action} topic={topic} "
                f"payload={payload_text(payload)}",
                file=out,
            )
        return

    if ha_discovery_topic is None:
        return
    prefix = f"{ha_discovery_topic.rstrip('/')}/device_automation/"
    if not topic.startswith(prefix):
        return
    trigger = parse_ha_discovery_trigger(topic, payload)
    if trigger is None or trigger.topic != device_topic:
        return
    if trigger.key() in state.seen_ha_triggers:
        return
    state.seen_ha_triggers.add(trigger.key())
    payload_part = f" payload={trigger.payload}" if trigger.payload is not None else ""
    print(
        "ha-trigger "
        f"type={trigger.trigger_type} subtype={trigger.subtype}"
        f"{payload_part} topic={trigger.topic}",
        file=out,
    )


def render_device_list(
    devices: Sequence[Zigbee2MqttDevice],
    *,
    as_json: bool = False,
) -> str:
    if as_json:
        return json.dumps([device.to_dict() for device in devices], indent=2)
    lines: list[str] = []
    for device in devices:
        lines.append(device.friendly_name)
        if device.ieee_address:
            lines.append(f"  ieee: {device.ieee_address}")
        if device.vendor or device.model:
            model = " ".join(
                value for value in (device.vendor, device.model) if value
            )
            lines.append(f"  model: {model}")
        if device.description:
            lines.append(f"  description: {device.description}")
        if device.exposes:
            lines.append(f"  exposes: {', '.join(device.exposes)}")
        if device.actions:
            lines.append(f"  actions: {', '.join(device.actions)}")
        lines.append(f"  topic: {device.topic}")
    return "\n".join(lines)


async def _read_bridge_devices(
    broker: BrokerOptions,
    *,
    base_topic: str,
    timeout: float,
) -> tuple[Zigbee2MqttDevice, ...]:
    topic = f"{base_topic.rstrip('/')}/bridge/devices"
    async with _mqtt_client(broker) as client:
        await client.subscribe(topic, qos=QOS)
        with anyio.fail_after(timeout):
            async for message in client.messages:
                if topic_text(message.topic) == topic:
                    return parse_bridge_devices(
                        message.payload,
                        base_topic=base_topic,
                    )
    return ()


async def _read_bridge_device_metadata(
    broker: BrokerOptions,
    *,
    base_topic: str,
    timeout: float,
) -> tuple[dict[str, Any], ...]:
    topic = f"{base_topic.rstrip('/')}/bridge/devices"
    async with _mqtt_client(broker) as client:
        await client.subscribe(topic, qos=QOS)
        with anyio.fail_after(timeout):
            async for message in client.messages:
                if topic_text(message.topic) == topic:
                    return parse_bridge_device_metadata(
                        message.payload,
                        base_topic=base_topic,
                    )
    return ()


def _mqtt_client(broker: BrokerOptions) -> aiomqtt.Client:
    return aiomqtt.Client(
        broker.hostname,
        port=broker.port,
        username=broker.username,
        password=broker.password,
    )


async def _list_devices(args: argparse.Namespace) -> int:
    devices = await _read_bridge_devices(
        _broker_options(args),
        base_topic=args.base_topic,
        timeout=args.timeout,
    )
    print(render_device_list(devices, as_json=args.json))
    return 0


async def _describe_device(args: argparse.Namespace) -> int:
    metadata = await _read_bridge_device_metadata(
        _broker_options(args),
        base_topic=args.base_topic,
        timeout=args.timeout,
    )
    device = _find_device_metadata(
        metadata,
        friendly_name=args.friendly_name,
        topic=_device_topic_from_args(args),
    )
    if device is None:
        print("Device not found in Zigbee2MQTT bridge/devices", file=sys.stderr)
        return 1
    if args.json:
        print(json.dumps(device, indent=2))
    else:
        print(yaml.safe_dump(device, sort_keys=False).strip())
    return 0


async def _infer_controls(args: argparse.Namespace) -> int:
    devices = await _read_bridge_devices(
        _broker_options(args),
        base_topic=args.base_topic,
        timeout=args.timeout,
    )
    device = _find_device_summary(
        devices,
        friendly_name=args.friendly_name,
        topic=_device_topic_from_args(args),
    )
    if device is None:
        print("Device not found in Zigbee2MQTT bridge/devices", file=sys.stderr)
        return 1
    print(render_inferred_controls(device))
    return 0


async def _inspect(args: argparse.Namespace) -> int:
    broker = _broker_options(args)
    device_topic = _device_topic_from_args(args)
    ha_topic = None if args.no_ha_discovery else args.ha_discovery_topic
    state = InspectState()
    async with _mqtt_client(broker) as client:
        await client.subscribe(device_topic, qos=QOS)
        if ha_topic is not None:
            await client.subscribe(f"{ha_topic.rstrip('/')}/device_automation/#", qos=QOS)
        cancel_scope = anyio.move_on_after(args.timeout) if args.timeout else None
        if cancel_scope is None:
            async for message in client.messages:
                process_inspect_message(
                    topic=topic_text(message.topic),
                    payload=message.payload,
                    device_topic=device_topic,
                    ha_discovery_topic=ha_topic,
                    unique=args.unique,
                    include_empty=args.include_empty,
                    state=state,
                    out=sys.stdout,
                )
        else:
            with cancel_scope:
                async for message in client.messages:
                    process_inspect_message(
                        topic=topic_text(message.topic),
                        payload=message.payload,
                        device_topic=device_topic,
                        ha_discovery_topic=ha_topic,
                        unique=args.unique,
                        include_empty=args.include_empty,
                        state=state,
                        out=sys.stdout,
                    )
    return 0


def _find_device_summary(
    devices: Sequence[Zigbee2MqttDevice],
    *,
    friendly_name: str | None,
    topic: str,
) -> Zigbee2MqttDevice | None:
    for device in devices:
        if friendly_name is not None and device.friendly_name == friendly_name:
            return device
        if device.topic == topic:
            return device
    return None


def _find_device_metadata(
    devices: Sequence[Mapping[str, Any]],
    *,
    friendly_name: str | None,
    topic: str,
) -> Mapping[str, Any] | None:
    for device in devices:
        if friendly_name is not None and device.get("friendly_name") == friendly_name:
            return device
        if device.get("topic") == topic:
            return device
    return None


def _broker_options(args: argparse.Namespace) -> BrokerOptions:
    return BrokerOptions(
        hostname=args.hostname,
        port=args.port,
        username=args.username,
        password=args.password,
    )


def _device_topic_from_args(args: argparse.Namespace) -> str:
    if args.topic:
        return args.topic
    return zigbee2mqtt_device_topic(args.base_topic, args.friendly_name)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="deckr-mqtt-actions",
        description="Inspect Zigbee2MQTT action devices and Deckr inference.",
    )
    subcommands = parser.add_subparsers(dest="command", required=True)

    list_devices = subcommands.add_parser("list-devices")
    _add_broker_args(list_devices)
    list_devices.add_argument("--json", action="store_true")
    list_devices.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT_SECONDS)
    list_devices.set_defaults(handler=_list_devices)

    describe_device = subcommands.add_parser("describe-device")
    _add_broker_args(describe_device)
    _add_device_topic_args(describe_device)
    describe_device.add_argument("--json", action="store_true")
    describe_device.add_argument(
        "--timeout",
        type=float,
        default=DEFAULT_TIMEOUT_SECONDS,
    )
    describe_device.set_defaults(handler=_describe_device)

    infer_controls = subcommands.add_parser("infer-controls")
    _add_broker_args(infer_controls)
    _add_device_topic_args(infer_controls)
    infer_controls.add_argument(
        "--timeout",
        type=float,
        default=DEFAULT_TIMEOUT_SECONDS,
    )
    infer_controls.set_defaults(handler=_infer_controls)

    inspect = subcommands.add_parser("inspect")
    _add_broker_args(inspect)
    _add_device_topic_args(inspect)
    inspect.add_argument(
        "--ha-discovery-topic",
        default=DEFAULT_HA_DISCOVERY_TOPIC,
    )
    inspect.add_argument("--no-ha-discovery", action="store_true")
    inspect.add_argument("--unique", action="store_true")
    inspect.add_argument("--include-empty", action="store_true")
    inspect.add_argument("--timeout", type=float)
    inspect.set_defaults(handler=_inspect)

    return parser


def _add_broker_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--hostname", required=True)
    parser.add_argument("--port", type=int, default=1883)
    parser.add_argument("--username")
    parser.add_argument("--password")
    parser.add_argument("--base-topic", default=DEFAULT_BASE_TOPIC)


def _add_device_topic_args(parser: argparse.ArgumentParser) -> None:
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--friendly-name")
    group.add_argument("--topic")


async def _main_async(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        return await args.handler(args)
    except TimeoutError as exc:
        print(f"Timed out waiting for MQTT message: {exc}", file=sys.stderr)
        return 1


def main(argv: Sequence[str] | None = None) -> None:
    raise SystemExit(anyio.run(_main_async, argv))


if __name__ == "__main__":
    main()
