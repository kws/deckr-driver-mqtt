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
devices_path = "../hardware/mqtt/openhabian/devices"
templates_path = "../hardware/mqtt/templates"

[deckr.components.instances.mqtt_openhabian.config.broker]
hostname = "openhabian"
port = 1883

[deckr.components.instances.mqtt_openhabian.config.labels]
mqtt-host = "openhabian"
```

Remote device YAML under `devices_path` describes the physical or virtual remote
instance and MQTT topic. It references a reusable template from
`templates_path`, which owns the control layout and MQTT action-to-Deckr event
mapping. Per-device broker overrides are not supported.

```yaml
# devices_path/remote-bedroom.yml
id: remote-0x0330
name: Bedroom remote
template: zigbee2mqtt-5-button-remote
remote:
  mqtt:
    topic: zigbee2mqtt/remote/0x0330/action
    dedupe_ms: 250
```

```yaml
# templates_path/zigbee2mqtt-5-button-remote.yml
id: zigbee2mqtt-5-button-remote
name: Zigbee2MQTT 5-button remote
events:
  - match: off
    control_id: volume-power
    event_type: press
```

## Development

Build a local `deckr` wheel first:

```bash
cd ../deckr && uv build --wheel
cd ../deckr-driver-mqtt
uv sync --dev --find-links ../deckr/dist
uv run --find-links ../deckr/dist pytest
```
