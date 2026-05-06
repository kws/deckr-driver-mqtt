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
config_path = "../mqtt/openhabian"

[deckr.components.instances.mqtt_openhabian.config.broker]
hostname = "openhabian"
port = 1883

[deckr.components.instances.mqtt_openhabian.config.labels]
mqtt-host = "openhabian"
```

Remote device YAML under `config_path` describes the device topic and event
mapping only. Per-device broker overrides are not supported.

## Development

Build a local `deckr` wheel first:

```bash
cd ../deckr && uv build --wheel
cd ../deckr-driver-mqtt
uv sync --dev --find-links ../deckr/dist
uv run --find-links ../deckr/dist pytest
```
