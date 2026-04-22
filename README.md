# deckr-driver-mqtt

MQTT-backed remote hardware driver for Deckr.

The driver keeps the existing `deckr.drivers.mqtt` runtime surface and is intended to
be loaded by the controller through the normal `deckr.drivers` entry point group.

## Development

Build a local `deckr` wheel first:

```bash
cd ../deckr && uv build --wheel
cd ../deckr-driver-mqtt
uv sync --dev --find-links ../deckr/dist
uv run --find-links ../deckr/dist pytest
```
