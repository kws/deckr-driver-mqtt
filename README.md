# deckr-driver-mqtt

MQTT-backed remote hardware manager for Deckr.

The manager keeps the existing `deckr.drivers.mqtt` runtime surface and is
loaded through the normal `deckr.components` entry point group.

## Development

Build a local `deckr` wheel first:

```bash
cd ../deckr && uv build --wheel
cd ../deckr-driver-mqtt
uv sync --dev --find-links ../deckr/dist
uv run --find-links ../deckr/dist pytest
```
