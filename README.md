# deckr-driver-mqtt

MQTT-backed remote hardware manager for Deckr.

The package exposes the hosted Deckr component entry point as
`deckr.drivers.mqtt:component` and is loaded through the normal
`deckr.components` entry point group. The runtime uses
`ComponentContext.open_endpoint("hardware_manager")`,
`ComponentContext.require_beacon()`, and
`ComponentContext.require_concord()`; there is no legacy `driver_factory`
surface.

Beacon advertises inferred Zigbee2MQTT devices as candidates for controller
claims. The manager attaches its participant token to matching
`dev.deckr.profile.hardware_claim.v1` Concord contracts, and live claim
authority is Concord-only after negotiation. Input events are routed only while
a Concord claim is valid. MQTT action devices are input-only, so authorized
controller commands are rejected as unsupported rather than forwarded to MQTT.

The manager no longer publishes or consumes legacy discovery inventory,
endpoint-presence, or unilateral device-claim current-state records.

If `config.broker.hostname` is omitted, the component stays running and reports
itself unready instead of attempting an MQTT connection.

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

The `deckr` Python distribution now comes from the sibling
[`deckr-core-python`](https://github.com/kws/deckr-core-python) repository.
Check it out next to this repository at the revision in `.deckr-core-revision`. In the parent workspace, run
`uv sync --all-packages --all-groups`, then the repository-local checks:

```bash
uv run --no-sync ruff check .
uv run --no-sync pytest
```

For standalone development, build a wheel from the separated core and install
it without the parent workspace source override:

```bash
(cd ../deckr-core-python && uv build --wheel)
uv sync --no-sources --dev --find-links ../deckr-core-python/dist
uv run --no-sync pytest
```

Runtime imports remain under `deckr`; the reference-only `deckr` repository is
not a Python package dependency.
