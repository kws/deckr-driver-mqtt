# deckr-driver-mqtt

MQTT-backed remote hardware manager for Deckr.

The manager keeps the existing `deckr.drivers.mqtt` runtime surface and is
loaded through the normal `deckr.components` entry point group.

Configure one MQTT hardware manager instance per broker. Broker connection
settings and selection labels live on the component instance:

```toml
[deckr.components.instances.mqtt_openhabian]
component = "dev.deckr.hardware.mqtt"
instance_id = "mqtt-openhabian"

[deckr.components.instances.mqtt_openhabian.endpoints]
hardware_manager = "mqtt-openhabian"

[deckr.components.instances.mqtt_openhabian.config]
base_topic = "zigbee2mqtt"
dedupe_ms = 250

[deckr.components.instances.mqtt_openhabian.config.broker]
hostname = "openhabian"
port = 1883

[deckr.components.instances.mqtt_openhabian.config.labels]
mqtt-host = "openhabian"
```

The manager discovers Zigbee2MQTT action devices from the retained
`<base_topic>/bridge/devices` payload, subscribes to modern device state topics
such as `zigbee2mqtt/remote/0x0330`, and infers Deckr core button controls from
the Zigbee2MQTT action enum. Remote device YAML, reusable templates,
`devices_path`, and `templates_path` are no longer supported.

## Zigbee2MQTT action inspection

The `deckr-mqtt-actions` helper can inspect Zigbee2MQTT metadata and live
remote action payloads:

```bash
deckr-mqtt-actions list-devices --hostname openhabian
deckr-mqtt-actions describe-device --hostname openhabian --friendly-name remote/0x0330
deckr-mqtt-actions infer-controls --hostname openhabian --friendly-name remote/0x0330
deckr-mqtt-actions inspect --hostname openhabian --friendly-name remote/0x0330 --unique
```

See [Zigbee2MQTT Discovery And Button Mapping](docs/zigbee2mqtt-discovery.md)
for the discovery-driven mapping from Zigbee2MQTT actions to Deckr core button
capabilities.

## Development

Build a local `deckr` wheel first:

```bash
cd ../deckr && uv build --wheel
cd ../deckr-driver-mqtt
uv sync --dev --find-links ../deckr/dist
uv run --find-links ../deckr/dist pytest
```
