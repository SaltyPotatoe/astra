# Scheduling Syntax

```{image} ../_static/scheduling-banner.svg
:class: responsive-banner
:align: center
:alt: banner
```

_Astra_ uses a scheduling system to automate observatory operations. Schedules are defined using JSONL files (JSON Lines format), where each JSON line represents a scheduled action with these fields:

- `device_name`: Name of the camera device (the primary instrument that coordinates all operations)
- `action_type`: Type of action to perform
- `action_value`: Parameters for the action
- `start_time`: Earliest time the action is valid to start (UTC ISO format: YYYY-MM-DD HH:MM:SS.sss)
- `end_time`: Latest time the action is valid (UTC ISO format: YYYY-MM-DD HH:MM:SS.sss)

```{admonition} Instrument-Centric Design
All scheduled actions specify a camera as the `device_name`. The camera acts as the primary instrument that coordinates operations with its paired devices (telescope, dome, filter wheel, focuser, etc.). This design ensures all devices work together as a cohesive system.
```

```{admonition} Timing and Execution Flow
The `start_time` and `end_time` fields define a validity window, not a strict duration block.

* **Early Completion**: If an action (e.g., observatory open) completes successfully before its `end_time`, _Astra_ does **not** wait. It moves immediately to the next action (idling only if the next action's `start_time` has not yet been reached).
* _Astra_ actions are completed sequentially, ordered by start times, so the next action will not start until the current one finishes, even if the next action's `start_time` has already passed. This is only invalidated if `execute_parallel` variable is set true.
```

## Example Schedule

```json
// open observatory
{
   "device_name":"camera_main",
   "action_type":"open",
   "action_value":{},
   "start_time":"2025-08-23 22:38:25.210",
   "end_time":"2025-08-24 10:49:15.363"
}
// dusk sky flats
{
   "device_name":"camera_main",
   "action_type":"flats",
   "action_value":{"filter":["r'", "g'"],"n":[20, 20]},
   "start_time":"2025-08-23 22:39:25.210",
   "end_time":"2025-08-23 23:16:00.018"
}
// science observations
{
   "device_name":"camera_main",
   "action_type":"object",
   "action_value":{"object":"Kepler-1","filter":"r'","ra":286.808542,"dec":49.316422,"exptime":8,"guiding":true,"pointing":true},
   "start_time":"2025-08-23 23:17:00.018",
   "end_time":"2025-08-24 04:43:40.018"
}
// dawn sky flats
{
   "device_name":"camera_main",
   "action_type":"flats",
   "action_value":{"filter":["r'", "g'"],"n":[20, 20]},
   "start_time":"2025-08-24 10:24:40.018",
   "end_time":"2025-08-24 10:49:15.363"
}
// close observatory
{
   "device_name":"camera_main",
   "action_type":"close",
   "action_value":{},
   "start_time":"2025-08-24 10:49:15.363",
   "end_time":"2025-08-24 11:49:15.363"
}
// calibration frames, biases and darks
{
   "device_name":"camera_main",
   "action_type":"calibration",
   "action_value":{"exptime":[0,10,15,30,38,60,120],"n":[10,10,10,10,10,10,10]},
   "start_time":"2025-08-24 10:55:15.363",
   "end_time":"2025-08-24 11:49:15.363"
}
```

```{admonition} JSONL Comments
_Astra_'s JSONL files support comments using lines that start with `//`:
```

## Schedule File Location

Place your schedule file in the observatory schedules directory with a `.jsonl` extension. For example:

- `~/Documents/Astra/schedules/{observatory_name}.jsonl`

_Astra_ will automatically detect and load the JSONL schedule file, with the specified name pattern, if modified.

## Supported Action Types

_Astra_ supports the following action types for observatory automation, organized by function:

- `open`: Open observatory
- `close`: Close observatory
- `cool_camera`: Activate camera cooling
- `object`: Capture light frames with optional pointing correction & autoguiding
- `calibration`: Capture dark and bias frames
- `flats`: Capture sky flat field frames
- `autofocus`: Autofocus
- `calibrate_guiding`: Calibrate guiding parameters
- `pointing_model`: Help build a telescope pointing model
- `complete_headers`: Complete FITS headers of all images captured

```{note}
The `complete_headers` action automatically runs at the end of every schedule execution to ensure complete metadata in all FITS files.
```

```{note}
All actions run `cool_camera` as a prerequisite to ensure the camera is at the correct operating temperature before any exposures are taken. Only `open` and `close` run `cool_camera` after their execution.
```

## Tracking Moving Targets

Solar system bodies and artificial satellites move appreciably against the background stars over the course of a single exposure. Astra compensates for this motion by commanding differential tracking rates in right ascension and declination.

To observe such a target, supply a `lookup_name` in the `object` action instead of
a fixed `ra` and `dec`.

Astra resolves `lookup_name` as the
schedule is loaded, and the source it resolves against determines the tracking:

| Source of the position | Example | Tracking |
| --- | --- | --- |
| Astropy's built-in ephemeris | `"mars"`, `"moon"` | Non-sidereal |
| JPL Horizons small-body search | `"C/2023 A3"`, `"Ceres"` | Non-sidereal |
| A two-line element set you supply, with `lookup_name` set to `"TLE"` | The ISS | Non-sidereal |
| SIMBAD (stars and deep-sky objects) | `"M31"`, `"Vega"` | Sidereal |

### Sequence of operations

1. **Pointing.** The mount slews to the target’s predicted position at `start_time`. Planets and comets move less than one arcsecond during a typical slew, so the mount will reach the target directly. Satellites move much faster. For example, the ISS can travel about 20 degrees in 30 seconds, so it cannot be slewed to directly.

   For satellites, set `nonsidereal_start_lead_time_seconds` to at least the mount’s slew and settling time. The mount will then slew to the position where the target is expected to be that many seconds after `start_time`, and wait there until the target arrives.

   If imaging starts later than that and the target has moved more than one arcminute from the waiting position, Astra first re-centres the mount on the target’s current position. This one-arcminute limit is based on angular distance, not elapsed time, so it depends on how fast the target is moving. For example, Mars would need to be about 45 minutes late to trigger re-centering, while the ISS would trigger it after only a few milliseconds.

2. **Tracking.** The differential rates are applied and refreshed throughout the
   sequence, including during exposures and while each frame is written to disk.
3. **Re-centring.** At intervals of `nonsidereal_recenter_interval` seconds the
   mount slews to the target's current ephemeris position. Setting the interval to
   zero suppresses these slews and leaves the rates to work alone; it does not
   disable non-sidereal tracking.
4. **Reset.** The rates are returned to zero when the sequence ends, including on
   error.

### Why periodic re-centring is necessary

Differential rates are open-loop velocity control: they specify how fast the mount
should move, but never where it should be pointing. They therefore cancel the
target's apparent motion without correcting any positional error that has already
accumulated.

Because autoguiding is disabled during non-sidereal tracking, re-centring supplies
the only positional feedback in the system. In its absence three errors grow
unchecked.

The initial pointing error persists. Whatever offset the pre-pointing slew leaves
behind, whether from residuals in the pointing model, a stale ephemeris, or the
target's motion during the slew, is preserved exactly by the rates.

Rate errors integrate into positional errors. No mount applies a commanded rate
perfectly, and the true rate itself evolves between updates.

Mechanical imperfections remain uncorrected. Periodic error, flexure and polar
misalignment act during non-sidereal tracking exactly as they do during sidereal
tracking.

The interval trades drift against dead time, since each re-centring costs two slews
and their settling. A few minutes is a reasonable starting point for a comet or
asteroid; fast-moving targets require shorter intervals.

### Requirements and limitations

- **The mount must support differential rates**, reporting the ASCOM capabilities
  `CanSetRightAscensionRate` and `CanSetDeclinationRate`. If no telescope in the
  observatory reports both, Astra rejects the schedule as it is loaded.
- **Autoguiding is disabled.** The guide field drifts relative to a moving target,
  so a guider would work against the tracking rates. Setting `guiding` to true for
  a moving target produces a warning and is otherwise ignored.
- **Rate commands are issued sparingly.** Some mounts stutter when a new tracking
  rate is applied, so Astra sends one only when retaining the current rate would
  trail the target appreciably, and never more often than
  `nonsidereal_rate_update_interval` seconds (10 s by default). Shorten it for
  targets whose rate changes rapidly; lengthen it for a mount sensitive to rate
  commands.
- **Minor bodies and TLEs require network access at load time.** Astra queries JPL
  Horizons once when the schedule is read, computing an ephemeris that is then
  interpolated throughout the night. A network failure while observing is therefore
  harmless, but one at load time causes the schedule to be rejected. Planets and the
  Moon are computed from Astropy's built-in ephemeris and need no network.

### Examples

The following action tracks Saturn, re-centring every five minutes:

```json
{
   "device_name":"camera_main",
   "action_type":"object",
   "action_value":{"object":"Saturn","lookup_name":"saturn","filter":"r'","exptime":30,"nonsidereal_recenter_interval":300},
   "start_time":"2025-08-23 23:17:00.018",
   "end_time":"2025-08-24 00:17:00.018"
}
```

This action tracks a satellite from its two-line element set. Set `lookup_name` to
`"TLE"` and give the two element lines in `tle`, separated by `\n`. The lead time
gives the mount 45 s to reach the position the satellite will occupy, and the short
rate update interval suits the rapidly changing rates of a low orbit:

```json
{
   "device_name":"camera_main",
   "action_type":"object",
   "action_value":{"object":"ISS","lookup_name":"TLE","tle":"1 25544U 98067A   26084.45430866  .00012951  00000-0  24673-3 0  9999\n2 25544  51.6344 354.4276 0006215 231.1671 128.8763 15.48531543558777","filter":"Clear","exptime":2,"nonsidereal_recenter_interval":60,"nonsidereal_rate_update_interval":1,"nonsidereal_start_lead_time_seconds":45},
   "start_time":"2025-08-23 23:17:00.018",
   "end_time":"2025-08-23 23:27:00.018"
}
```

```{warning}
Two-line element sets degrade quickly. A set more than a few days old will not place
a satellite in low Earth orbit accurately enough to land it on the detector.
```

## Action Value Parameters

Each action type requires specific parameters in the `action_value` field.
The sections below are generated automatically from the action configuration
dataclasses to ensure the documentation always matches the implementation.

```{eval-rst}
.. autoscheduleactions::
   :format: literal
```
