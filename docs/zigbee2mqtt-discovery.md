# Zigbee2MQTT Discovery And Button Mapping

This note captures the runtime contract for Zigbee2MQTT-backed remotes. The
driver no longer supports explicit remote device YAML or reusable templates;
standard Zigbee2MQTT remotes are discovered from broker metadata and mapped by
convention.

## Goal

For standard Zigbee2MQTT action devices, the MQTT hardware manager should be
able to discover devices from `zigbee2mqtt/bridge/devices`, infer Deckr controls
from the published action enum, and emit ordinary Deckr core button events where
the mapping is honest.

Per-device hardware config is intentionally gone. Future convention overrides
may cover presentation and physical-layout facts Zigbee2MQTT cannot know, such
as grouping `on` and `off` actions into one physical `power` control.

The target runtime shape is:

```text
MQTT manager config
  broker settings, manager labels, base topic, dedupe window

Zigbee2MQTT bridge/devices
  discovered devices, topics, action enums, metadata, status hints

Convention mapper
  inferred Deckr controls and core button capabilities

Controller device config
  fingerprint/label match plus user-facing bindings
```

## Source Metadata

The primary source of truth is the retained `bridge/devices` payload:

- `friendly_name`: derives the state topic as `<base_topic>/<friendly_name>`.
- `ieee_address`, `vendor`, `model`, `model_id`, and `description`: identify the
  device and improve fingerprints/display names.
- `definition.exposes`: contains the device capabilities.
- `definition.exposes[]` where `property: action`: contains the known action
  enum values.
- `definition.options`: exposes useful Zigbee2MQTT behavior such as
  `simulated_brightness`.
- Other exposes such as `battery`, `linkquality`, `action_duration`, and
  `identify`: inform diagnostics, state, or configuration, but are not control
  inputs.
- `endpoints[].clusters`: provides confidence and diagnostics, but should not
  override the action grammar.
- `power_source`, `type`, `interview_state`, `supported`, and `disabled`: inform
  inventory status and filtering.

Runtime MQTT messages should be read from the modern device state topic,
`<base_topic>/<friendly_name>`. Payloads are usually JSON objects with an
`action` property. Older/plain action payloads should remain accepted at the MQTT
protocol boundary, but empty actions should be ignored unless explicitly
debugging legacy behavior. The driver should not default to the older
`<base_topic>/<friendly_name>/action` topic shape for discovered devices.

Availability and bridge events are useful runtime signals:

- `<base_topic>/<friendly_name>/availability`: retained online/offline state when
  Zigbee2MQTT availability is enabled.
- `<base_topic>/bridge/event`: joins, leaves, announces, and interview status.

Other useful metadata remains advisory:

- Home Assistant `device_automation` discovery can help compare inferred
  triggers, but is not the source of truth.
- `bridge/info` and `bridge/health` can support manager diagnostics.
- `bridge/groups`, endpoint bindings, and clusters can explain Zigbee topology,
  but should not create Deckr controls by themselves.

## Design Principles

- Treat these remotes as buttons first. Prefer `dev.deckr.input.button` core
  capabilities over invented axis or encoder semantics.
- The raw Zigbee2MQTT action string is an input fact, not the durable Deckr
  control id.
- Zigbee2MQTT action names are converter conventions, not a universal formal
  grammar. Known patterns should be mapped conservatively; unknown patterns
  should remain visible through diagnostics or future raw extension capability.
- Emit one normal core input event for one raw action. Do not emit synonym events
  for the same physical interaction.
- Preserve raw Zigbee2MQTT actions through logs, diagnostics, and an optional
  future package-owned extension capability, not by adding extra fields to core
  button values.
- Core button mapping intentionally loses Zigbee2MQTT-specific gesture
  classification. For example, `*_press_release` and `*_hold_release` both become
  `button.momentary` `up`. Consumers that need that distinction should use
  diagnostics or future native Zigbee2MQTT extension input.
- Reconstructed `down` events are best effort. Some devices only publish after a
  hold threshold or after a semantic command decision, so `down` means "earliest
  available start signal from Zigbee2MQTT", not always electrical key-down time.
- Use endpoint clusters and options as confidence and operator metadata. Do not
  infer a different physical control shape solely because a cluster exists.
- Convention overrides should fix physical grouping or naming, not duplicate the
  complete Zigbee2MQTT action list. They are not implemented in the first
  discovery rewrite.

## Core Button Mapping

Deckr core button value schemas are intentionally small:

```json
{"eventType": "press"}
```

```json
{"eventType": "down"}
```

```json
{"eventType": "up"}
```

Raw Zigbee2MQTT metadata such as the original action name must not be added to
these core values. Consumers that need the exact raw action should use logs or
inspection tooling for now; native Zigbee2MQTT extension input is future work.

Actions that only require ordinary activation can bind to `button.momentary`
`up`, `button.press` `press`, or touch `tap` through the normal Deckr activation
semantics. A control that already exposes `button.momentary` should not emit an
extra projected `button.press` for the same release just to satisfy activation
bindings.

### Simple Actions

Actions without lifecycle information map to activation buttons:

| Zigbee2MQTT action | Control id | Core capability | Event |
| --- | --- | --- | --- |
| `on` | `on` | `button.press` | `press` |
| `off` | `off` | `button.press` | `press` |
| `toggle` | `toggle` | `button.press` | `press` |
| `store` | `store` | `button.press` | `press` |
| `recall` | `recall` | `button.press` | `press` |
| `store_1` | `store_1` | `button.press` | `press` |
| `recall_1` | `recall_1` | `button.press` | `press` |

If a device toggles one physical button between `on` and `off`, the default
mapping should still expose logical `on` and `off` controls. A convention
override may group both actions into one `power` control when the physical
layout is known.

### Gesture Slot Actions

Actions with a slot prefix map to one control per slot:

| Pattern | Control id | Core capability | Event |
| --- | --- | --- | --- |
| `<slot>_click` | `<slot>` | `button.press` | `press` |
| `<slot>_press` with matching release | `<slot>` | `button.momentary` | `down` |
| `<slot>_press` without matching release | `<slot>` | `button.press` | `press` |
| `<slot>_press_release` | `<slot>` | `button.momentary` | `up` |
| `<slot>_hold` | `<slot>` | `button.momentary` | `down` |
| `<slot>_hold_release` | `<slot>` | `button.momentary` | `up` |
| `<slot>_release` | `<slot>` | `button.momentary` | `up` |

Examples:

- `arrow_left_click` emits `controlId=arrow_left`, `button.press`, `press`.
- `arrow_left_hold` emits `controlId=arrow_left`, `button.momentary`, `down`.
- `arrow_left_release` emits `controlId=arrow_left`, `button.momentary`, `up`.
- `up_press` emits `controlId=up`, `button.momentary`, `down` when
  `up_press_release` exists.
- `up_press_release` emits `controlId=up`, `button.momentary`, `up`.

When a control supports both click and hold lifecycle actions, it may expose both
`button.press` and `button.momentary`. The driver still emits only the capability
that corresponds to the raw action.

If a control only has `press` and `release` lifecycle actions, expose
`button.momentary`, not a duplicate `button.press` projection. A normal
activation action can fire on the resulting `up` event.

### Directional Step Actions

Step actions are directional button presses, not encoders:

| Pattern | Control id | Core capability | Event |
| --- | --- | --- | --- |
| `brightness_step_up` | `brightness_up` | `button.press` | `press` |
| `brightness_step_down` | `brightness_down` | `button.press` | `press` |
| `color_temperature_step_up` | `color_temperature_up` | `button.press` | `press` |
| `color_temperature_step_down` | `color_temperature_down` | `button.press` | `press` |

The `genLevelCtrl` or `lightingColorCtrl` clusters can confirm the meaning, but
the Deckr-facing shape remains a button press. Zigbee2MQTT
`simulated_brightness` state can be useful diagnostics or future state input, but
it should not make a remote button look like a Deckr encoder.

### Directional Move And Stop Actions

Move actions are directional button lifecycle events:

| Pattern | Control id | Core capability | Event |
| --- | --- | --- | --- |
| `brightness_move_up` | `brightness_up` | `button.momentary` | `down` |
| `brightness_move_down` | `brightness_down` | `button.momentary` | `down` |
| `color_temperature_move_up` | `color_temperature_up` | `button.momentary` | `down` |
| `color_temperature_move_down` | `color_temperature_down` | `button.momentary` | `down` |

Stop actions require small runtime state:

```text
brightness_move_up
  active["brightness"] = "brightness_up"
  emit controlId=brightness_up eventType=down

brightness_stop
  emit controlId=active["brightness"] eventType=up
  clear active["brightness"]
```

If a stop arrives without a known active direction, the driver should drop it and
log at debug level rather than inventing an `up` event for both directions.

The same rule applies to `color_temperature_move_stop` and other
`<axis>_move_stop` or `<axis>_stop` patterns.

Runtime state must be scoped per discovered device and axis. A
`brightness_stop` from one remote must only release a direction previously
started by that same remote.

## Native Zigbee2MQTT Extension Capability

Some automations may eventually need the exact raw Zigbee2MQTT action as a
runtime input. The driver does not expose that capability in the first discovery
rewrite. If added later, it must use a package-owned namespace and must not use
`dev.deckr.*`; that prefix is reserved for Deckr core capabilities.

A provisional family could be:

```text
org.deckr.driver_mqtt.zigbee2mqtt.action
```

With event:

```text
action
```

And value shape:

```json
{
  "eventType": "action",
  "action": "brightness_move_up",
  "topic": "zigbee2mqtt/remote/0x0330"
}
```

This raw capability should be opt-in or carefully filtered if implemented. If
the driver emits both raw and core events by default, a broad control binding may
receive duplicates.

## Convention Overrides

Convention overrides are not implemented in the first discovery rewrite. When
added, the convention file should be small. It should adjust inferred output
rather than redefine full devices:

- group several raw actions into one physical control, such as `on` and `off`
  into `power`;
- rename inferred controls for presentation or binding stability;
- hide controls that should not be exposed;
- choose model-specific behavior when a converter naming convention is
  ambiguous.

Overrides may match by vendor/model, model id, IEEE address, or friendly name.
Vendor/model overrides are reusable; IEEE/friendly-name overrides are local to
one installed device.

## Example Inference

### Paulmann 501.41 Basic Remote

Actions:

```text
on, off,
brightness_move_up, brightness_move_down, brightness_stop,
brightness_step_up, brightness_step_down,
color_temperature_move_up, color_temperature_move_down,
color_temperature_move_stop,
color_temperature_step_up, color_temperature_step_down,
store, recall
```

Default inferred controls:

- `on`: `button.press`
- `off`: `button.press`
- `brightness_up`: `button.press` and `button.momentary`
- `brightness_down`: `button.press` and `button.momentary`
- `color_temperature_up`: `button.press` and `button.momentary`
- `color_temperature_down`: `button.press` and `button.momentary`
- `store`: `button.press`
- `recall`: `button.press`

Optional physical-layout override:

```yaml
models:
  - vendor: Paulmann
    model: "501.41"
    actions:
      on:
        control_id: power
        event: press
      off:
        control_id: power
        event: press
```

### Philips Hue Dimmer

Actions:

```text
on_press, on_press_release, on_hold, on_hold_release,
up_press, up_press_release, up_hold, up_hold_release,
down_press, down_press_release, down_hold, down_hold_release,
off_press, off_press_release, off_hold, off_hold_release
```

Default inferred controls:

- `on`: `button.momentary`
- `up`: `button.momentary`
- `down`: `button.momentary`
- `off`: `button.momentary`

The `*_press` and `*_hold` actions emit `down`. The matching release actions
emit `up`. The raw action remains available through diagnostics and inspection
tooling.

### IKEA STYRBAR

Actions:

```text
on, off,
brightness_move_up, brightness_move_down, brightness_stop,
arrow_left_click, arrow_left_hold, arrow_left_release,
arrow_right_click, arrow_right_hold, arrow_right_release
```

Default inferred controls:

- `on`: `button.press`
- `off`: `button.press`
- `brightness_up`: `button.momentary`
- `brightness_down`: `button.momentary`
- `arrow_left`: `button.press` and `button.momentary`
- `arrow_right`: `button.press` and `button.momentary`

## Open Questions

- Exact convention override schema.
- Whether discovered device fingerprints should use only IEEE address or include
  vendor/model for clearer diagnostics.
- Final extension capability namespace and schema id if raw Zigbee2MQTT action
  input becomes a runtime feature.
- How much controller selector behavior should help avoid duplicate handling when
  a control exposes both core and native extension capabilities.
