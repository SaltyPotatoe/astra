"""Non-sidereal (solar system body) tracking for observatory imaging sequences.

This module provides the machinery for tracking solar system objects (comets,
asteroids, planets, Earth-orbiting objects) whose celestial coordinates change
significantly over short timescales. It uses high-precision cubic interpolation
of pre-computed ephemerides to provide smooth, differential tracking rates to
the telescope mount via ASCOM.

Key Capabilities:
    - Ephemeris Pre-computation: Uses Astropy or JPL Horizons to generate a sequence of
      positions for a given object over the duration of an observation.
    - Cubic Interpolation: Provides sub-second precision for RA/Dec coordinates
      without requiring repeated, expensive lookups.
    - Differential Tracking: Calculates and applies the exact RA/Dec rates
      required for the mount to follow the target (blind tracking).
    - Periodic Re-centering: Automatically re-slews the telescope to the latest
      ephemeris position at user-defined intervals to correct for long-term drift.

Integration Notes:
    - Non-sidereal tracking is generally incompatible with standard autoguiding.
      The system is designed to disable guiding when active.
    - Requires ASCOM drivers that support `RightAscensionRate` and
      `DeclinationRate` properties.
"""

import logging
import math
import time
from dataclasses import dataclass, field
from typing import Any, Callable

import astropy.units as u
from astropy.coordinates import SkyCoord
from astropy.time import Time

from astra.utils import ephemeris
from astra.alpaca_device_process import AlpacaDevice
from astra.paired_devices import PairedDevices
from astra.scheduler import Action

__all__ = ["NonSiderealManager"]

# Issuing a rate command interrupts tracking on some mounts, so the two settings
# below govern how rarely we can get away with it.
#
# Never send rates more often than this, however fast the caller asks. apply_rates
# is driven from the exposure and image-save loops at up to 10 Hz, which is far
# beyond what any ephemeris justifies: outside low Earth orbit the required rate
# changes over minutes, not milliseconds. A schedule overrides this default with
# the action value "nonsidereal_rate_update_interval".
_MIN_RATE_UPDATE_INTERVAL_S = 10.0

# Beyond the interval floor, only send a rate once keeping the current one would
# trail the target by more than this much. Judged as angle on the sky rather than
# as a percentage of the rate: a percentage is undefined as a rate passes through
# zero -- which happens routinely, at a target's maximum declination -- and fires
# continuously there over changes far too small to see.
_RATE_UPDATE_TOLERANCE_ARCSEC = 0.5

# How far the target may drift past the intended start of imaging before the mount
# is re-pointed. Expressed as an angle rather than as a grace period, because no
# single period suits every target: a comet takes most of an hour to move this far,
# while a satellite in low orbit covers it in a fraction of a second.
_MAX_START_DRIFT_ARCSEC = 60.0

# Seconds to wait after issuing an asynchronous slew before the caller starts
# polling ``Slewing``; some drivers do not raise the flag immediately. Mirrors
# ``observatory.SLEW_POLL_START_DELAY``, which cannot be imported here because
# observatory imports this module. The post-slew settle is applied by the
# ``wait_for_slew_fn`` passed into ``recenter``.
_SLEW_POLL_START_DELAY = 1.0

# How far the sequence start may sit from the epoch the ephemeris interpolators
# were computed for. These are the same timestamp in a correctly ordered load, so
# any real drift is a re-timed schedule, not rounding.
_MAX_EPHEMERIS_EPOCH_DRIFT_S = 1.0


@dataclass
class _NonSiderealState:
    """
    Holds pre-computed ephemeris state for a single imaging sequence.

    Attributes:
        body_name (str): Name of the solar system body being tracked.
        ra_interp (Any): Scipy interpolator for Right Ascension (degrees).
        dec_interp (Any): Scipy interpolator for Declination (degrees).
        sequence_start_time (Time): The exact Astropy Time the sequence began.
        recenter_interval (int): Seconds between re-centering slews.
        last_recenter_time (float): Unix timestamp of the last successful re-center.
    """

    body_name: str
    ra_interp: Any
    dec_interp: Any
    ra_rate_interp: Any
    dec_rate_interp: Any
    sequence_start_time: Time
    recenter_interval: int
    last_recenter_time: float = field(default_factory=time.time)
    last_applied_ra_rate: float | None = None
    last_applied_dec_rate: float | None = None
    last_rate_update_time: float | None = None


class NonSiderealManager:
    """Manages non-sidereal tracking operations for a single imaging sequence.

    This class encapsulates the mathematical and operational complexity of
    differential tracking, allowing the main observatory loop to remain
    focused on hardware orchestration.

    Usage::

        manager = NonSiderealManager(action, logger)
        if manager.is_active:
            manager.apply_rates(telescope)

        try:
            # ... exposure loop ...
            if manager.should_recenter():
                manager.recenter(paired_devices, wait_for_slew_fn)
        finally:
            manager.reset_rates(telescope)
    """

    def __init__(
        self,
        action: Action,
        logger: logging.Logger,
        telescope: AlpacaDevice | None = None,
    ) -> None:
        """
        Args:
            action: The scheduled action for this sequence.
            logger: Observatory logger.
            telescope: Mount this sequence will run on. When given, its ASCOM
                ``CanSetRightAscensionRate`` / ``CanSetDeclinationRate`` flags decide
                whether non-sidereal tracking can run at all. This is the authoritative
                check: schedule validation only knows whether *some* mount in the
                observatory supports differential rates, not which one the action is
                paired with.
        """
        self.logger = logger

        # The schedule sets the shortest time between rate commands. Increase it for
        # a mount that moves incorrectly when it receives a rate command. Decrease it
        # for a target whose rate changes over seconds.
        requested = action.action_value.get("nonsidereal_rate_update_interval")
        self.min_rate_update_interval = (
            _MIN_RATE_UPDATE_INTERVAL_S if requested is None else float(requested)
        )
        self._state: _NonSiderealState | None = self._setup(action, telescope)

    @property
    def is_active(self) -> bool:
        return self._state is not None

    def apply_rates(self, telescope: AlpacaDevice) -> None:
        """Push current differential RA/Dec rates to the mount."""
        if self._state is None:
            return
        self._apply_rates(telescope, self._state)

    def prepoint_coordinates(
        self, lead_time_seconds: float = 0.0
    ) -> tuple[float, float] | None:
        """Return RA/Dec for an initial lead-pointing slew.

        Args:
            lead_time_seconds: Seconds after sequence start used as the pre-pointing
                target. Defaults to 0, which points at the target's position at the
                start of the sequence.

        Returns:
            Tuple of (ra_deg, dec_deg) or None when non-sidereal tracking is inactive.
        """
        if self._state is None:
            return None

        state = self._state
        ra_deg = float(state.ra_interp(lead_time_seconds)) % 360.0
        dec_deg = float(state.dec_interp(lead_time_seconds))
        return (ra_deg, dec_deg)

    def tracking_activation_time(self, lead_time_seconds: float = 0.0) -> Time | None:
        """Return the time when non-sidereal rates and imaging should begin."""
        if self._state is None:
            return None
        return self._state.sequence_start_time + lead_time_seconds * u.s

    def should_recenter(self) -> bool:
        """Return True if the recenter interval has elapsed."""
        if self._state is None or self._state.recenter_interval <= 0:
            return False
        return (
            time.time() - self._state.last_recenter_time > self._state.recenter_interval
        )

    def drift_between(self, start: Time, end: Time) -> float | None:
        """Angular distance the target moves between two times, in arcsec.

        Returns None when non-sidereal tracking is inactive.
        """
        if self._state is None:
            return None

        state = self._state

        def position(when: Time) -> SkyCoord:
            t = (when - state.sequence_start_time).to_value(u.s)
            return SkyCoord(
                ra=(float(state.ra_interp(t)) % 360.0) * u.deg,
                dec=float(state.dec_interp(t)) * u.deg,
            )

        return float(position(start).separation(position(end)).arcsec)

    def recenter_if_late(
        self,
        activation_time: Time,
        paired_devices: PairedDevices,
        wait_for_slew_fn: Callable[[PairedDevices], None],
    ) -> bool:
        """Re-point the mount if the target has drifted since ``activation_time``.

        The mount is pre-pointed at where the target would be at ``activation_time``.
        If the sequence reaches that point late, the target has moved on, and how
        much that matters depends entirely on the target: the same delay is
        imperceptible for a comet and puts a satellite degrees outside the field. The
        decision is therefore made on the distance the target has actually travelled,
        not on how late the sequence is.

        Returns True if a re-centre was performed.
        """
        if self._state is None:
            return False

        now = Time.now()
        late_s = (now - activation_time).to_value(u.s)
        if late_s <= 0:
            return False

        drift = self.drift_between(activation_time, now)
        if drift is None or drift < _MAX_START_DRIFT_ARCSEC:
            return False

        self.logger.warning(
            f"Imaging of {self._state.body_name} starts {late_s:.0f}s after the "
            f'intended moment, by which time it has moved {drift:.0f}". Re-centering.'
        )
        return self.recenter(paired_devices, wait_for_slew_fn)

    def recenter(
        self,
        paired_devices: PairedDevices,
        wait_for_slew_fn: Callable[[PairedDevices], None],
    ) -> bool:
        """Slew to the updated ephemeris position and refresh tracking rates.

        Args:
            paired_devices: PairedDevices for the sequence.
            wait_for_slew_fn: Callable(paired_devices) that blocks until slew completes.

        Returns:
            True if re-centering was performed (caller should reset guiding flag).
        """
        if self._state is None:
            return False

        state = self._state
        try:
            telescope = paired_devices.telescope
            # TODO: Check dome open

            # Coarse slew to the satellite's current position.
            t_seconds = (Time.now() - state.sequence_start_time).to(u.s).value
            ra_deg = float(state.ra_interp(t_seconds)) % 360.0  # unwrap → [0, 360)
            dec_deg = float(state.dec_interp(t_seconds))
            self.logger.info(
                f"Re-centering on {state.body_name} at RA={ra_deg:.3f}°, Dec={dec_deg:.3f}°"
            )
            telescope.get(
                "SlewToCoordinatesAsync",
                RightAscension=ra_deg / 15.0,  # ASCOM expects RA in hours [0, 24)
                Declination=dec_deg,
            )
            time.sleep(_SLEW_POLL_START_DELAY)
            wait_for_slew_fn(paired_devices)

            # Fine correction: the satellite moved during the coarse slew. Re-target
            # the current position so the offset isn't locked in by the tracking rates.
            t_seconds = (Time.now() - state.sequence_start_time).to(u.s).value
            ra_deg = float(state.ra_interp(t_seconds)) % 360.0
            dec_deg = float(state.dec_interp(t_seconds))
            self.logger.debug(
                f"Re-centering correction on {state.body_name} at RA={ra_deg:.3f}°, Dec={dec_deg:.3f}°"
            )
            telescope.get(
                "SlewToCoordinatesAsync",
                RightAscension=ra_deg / 15.0,
                Declination=dec_deg,
            )
            time.sleep(_SLEW_POLL_START_DELAY)
            wait_for_slew_fn(paired_devices)

            # Force the post-slew rate push regardless of delta or interval — the
            # mount has just moved and we want a known-good rate applied immediately.
            state.last_applied_ra_rate = None
            state.last_applied_dec_rate = None
            state.last_rate_update_time = None
            self._apply_rates(telescope, state)
            state.last_recenter_time = time.time()
            return True
        except Exception as e:
            self.logger.warning(f"Non-sidereal re-centering failed: {e}")
            return False

    def reset_rates(self, telescope: AlpacaDevice) -> None:
        """Reset differential tracking rates to zero.

        Safe to call even when not active (no-op if non-sidereal tracking was
        never started, so the telescope is never touched); always call this in
        a finally block.
        """
        if self._state is None:
            return
        try:
            telescope.set("RightAscensionRate", 0.0)
            telescope.set("DeclinationRate", 0.0)
            self._state.last_applied_ra_rate = None
            self._state.last_applied_dec_rate = None
            self._state.last_rate_update_time = None
            self.logger.info("Non-sidereal tracking rates reset to zero")
        except Exception as e:
            self.logger.warning(f"Could not reset non-sidereal tracking rates: {e}")

    def _setup(
        self, action: Action, telescope: AlpacaDevice | None = None
    ) -> _NonSiderealState | None:
        """Build state from pre-computed ephemeris in the action config.

        Returns None if non-sidereal tracking is not active (``_nonsidereal`` is False),
        telescope movement is disabled, or the mount cannot accept differential tracking
        rates.  The ephemeris interpolators are computed once at schedule load time (in
        ``ObjectActionConfig.validate_visibility``) and read here at sequence start — no
        repeated network or ephemeris calls at runtime.
        """
        if (
            not action.action_value.get("_nonsidereal", False)
            or action.action_type == "calibration"
            or action.action_value.get("disable_telescope_movement", False)
        ):
            return None

        lname = action.action_value.get("lookup_name")

        if telescope is not None and not self._telescope_supports_rates(telescope):
            # Reaching here means the schedule resolved this target as a moving body,
            # so running the sequence sidereally would trail it. logger.error clears
            # error_free, stopping the sequence via the usual check_conditions() path.
            # Schedule validation catches this for the observatory as a whole; it can
            # still fire here when the action is paired with a different mount than
            # the one validation happened to sample.
            self.logger.error(
                f"'{lname}' needs non-sidereal tracking, but mount "
                f"{getattr(telescope, 'device_name', '')} does not support "
                "differential tracking rates (CanSetRightAscensionRate / "
                "CanSetDeclinationRate are False)."
            )
            return None

        ra_interp = action.action_value.get("_ra_interp")
        dec_interp = action.action_value.get("_dec_interp")
        ra_rate_interp = action.action_value.get("_ra_rate_interp")
        dec_rate_interp = action.action_value.get("_dec_rate_interp")
        recenter_interval = int(
            action.action_value.get("nonsidereal_recenter_interval", 0)
        )

        if ra_interp is None or dec_interp is None:
            self.logger.error(
                f"Non-sidereal tracking requested for '{lname}' but ephemeris "
                "interpolators are missing. This should never happen if the schedule "
                "was validated correctly at load time."
            )
            return None

        # The interpolators are keyed to the epoch they were computed for. If the
        # sequence is starting at a different time, every position and rate read out
        # of them is for the wrong moment, and far enough out the cubic fit is
        # extrapolating. Refuse rather than slew somewhere confidently wrong.
        sequence_start_time = Time(action.start_time)
        epoch = action.action_value.get("_ephemeris_epoch")
        if epoch is not None:
            drift_s = abs((sequence_start_time - epoch).to_value(u.s))
            if drift_s > _MAX_EPHEMERIS_EPOCH_DRIFT_S:
                self.logger.error(
                    f"Ephemeris for '{lname}' was computed for {epoch.isot} but the "
                    f"sequence starts at {sequence_start_time.isot} ({drift_s:.0f}s "
                    "later). The schedule was re-timed after validation; refusing to "
                    "track from a stale ephemeris."
                )
                return None
            sequence_start_time = epoch

        recenter_msg = (
            f"re-centering every {recenter_interval}s"
            if recenter_interval > 0
            else "re-centering disabled"
        )
        self.logger.info(f"Non-sidereal tracking active for '{lname}', {recenter_msg}")
        return _NonSiderealState(
            body_name=lname,
            ra_interp=ra_interp,
            dec_interp=dec_interp,
            ra_rate_interp=ra_rate_interp,
            dec_rate_interp=dec_rate_interp,
            sequence_start_time=sequence_start_time,
            recenter_interval=recenter_interval,
        )

    def _apply_rates(self, telescope: AlpacaDevice, state: _NonSiderealState) -> None:
        """Set ASCOM RightAscensionRate / DeclinationRate from the interpolated ephemeris.

        Called at up to 10 Hz from the exposure and image-save loops, but most of
        those calls must not reach the mount: rate commands interrupt tracking on
        some mounts, and the required rate changes far more slowly than that. Two
        gates decide whether a call gets through -- a minimum interval since the
        last command, then whether keeping the current rate would actually trail
        the target. See ``_rate_update_worthwhile``.
        """
        now = time.time()
        if (
            state.last_rate_update_time is not None
            and now - state.last_rate_update_time < self.min_rate_update_interval
        ):
            # Cheap early exit before touching the interpolators, since this is the
            # branch the high-frequency callers take almost every time.
            return

        try:
            t_seconds = (Time.now() - state.sequence_start_time).to(u.s).value
            if state.ra_rate_interp is not None and state.dec_rate_interp is not None:
                ra_rate = float(state.ra_rate_interp(t_seconds))
                dec_rate = float(state.dec_rate_interp(t_seconds))
            else:
                ra_rate, dec_rate = ephemeris.compute_nonsidereal_rates_from_interp(
                    state.ra_interp, state.dec_interp, t_seconds
                )

            drift = self._projected_drift_arcsec(
                state, ra_rate, dec_rate, t_seconds, now
            )
            if drift is not None and drift < _RATE_UPDATE_TOLERANCE_ARCSEC:
                self.logger.debug(
                    f"Keeping current non-sidereal rates; updating them would recover "
                    f'only {drift:.4f}" of trailing '
                    f"(dRA={ra_rate:.6f} s/s, dDec={dec_rate:.6f} as/s)"
                )
                return

            telescope.set("RightAscensionRate", ra_rate)
            telescope.set("DeclinationRate", dec_rate)
            state.last_applied_ra_rate = ra_rate
            state.last_applied_dec_rate = dec_rate
            state.last_rate_update_time = now
            self.logger.info(
                f"Non-sidereal tracking rates: dRA={ra_rate:.6f} s/s, dDec={dec_rate:.6f} as/s"
            )
        except Exception as e:
            # Rate support was verified in _setup, so this is a transient device
            # or ephemeris failure. Do not re-query the mount here: if the link is
            # down that raises a second exception out of the handler.
            self.logger.warning(f"Could not set non-sidereal tracking rates: {e}")

    def _telescope_supports_rates(self, telescope: AlpacaDevice) -> bool:
        """Return True if the mount can accept differential RA and Dec tracking rates.

        Both ASCOM capability flags are required: a mount that can offset only one
        axis cannot follow a moving target. If the flags cannot be read the mount is
        assumed capable, so a transient query failure degrades to the previous
        behaviour (attempt the rates, warn if they are rejected) rather than
        silently disabling tracking.
        """
        try:
            can_ra = bool(telescope.get("CanSetRightAscensionRate"))
            can_dec = bool(telescope.get("CanSetDeclinationRate"))
        except Exception as e:
            self.logger.warning(
                f"Could not query differential tracking rate support: {e}. "
                "Assuming the mount supports it."
            )
            return True
        return can_ra and can_dec

    def _projected_drift_arcsec(
        self,
        state: _NonSiderealState,
        ra_rate: float,
        dec_rate: float,
        t_seconds: float,
        now: float,
    ) -> float | None:
        """Trailing, in arcsec, that keeping the current rates would cause.

        Measured over the horizon until rates could next be sent, so it answers the
        question that matters -- how far off target the mount drifts by not being
        updated -- rather than how much the number changed.

        Returns None when no rates have been applied yet, meaning the caller should
        send them unconditionally.
        """
        if state.last_applied_ra_rate is None or state.last_applied_dec_rate is None:
            return None

        # RightAscensionRate is in seconds of RA, DeclinationRate in arcsec. Put both
        # on the sky in arcsec so they are comparable and the tolerance means one
        # thing on each axis.
        dec_deg = float(state.dec_interp(t_seconds))
        cos_dec = max(abs(math.cos(math.radians(dec_deg))), 1e-6)
        d_ra_arcsec_s = abs(ra_rate - state.last_applied_ra_rate) * 15.0 * cos_dec
        d_dec_arcsec_s = abs(dec_rate - state.last_applied_dec_rate)

        elapsed = now - (state.last_rate_update_time or now)
        horizon_s = max(elapsed, self.min_rate_update_interval)
        return math.hypot(d_ra_arcsec_s, d_dec_arcsec_s) * horizon_s
