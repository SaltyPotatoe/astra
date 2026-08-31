"""Unit tests for NonSiderealManager.

Covers the logic in nonsidereal.py that is not exercised by the integration test
in test_observatory_running_schedule.py:

  - _setup guard conditions (inactive by default, calibration, disable_telescope_movement,
    missing interpolators)
  - is_active property
  - apply_rates / reset_rates telescope interactions and error handling
  - should_recenter interval logic
  - recenter slew + rate refresh + timestamp update + error path
"""

import math
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import numpy as np
import pytest
import astropy.units as u
from scipy.interpolate import interp1d
from astropy.time import Time

from astropy.coordinates import EarthLocation

from astropy.coordinates import SkyCoord

from astra.nonsidereal import NonSiderealManager
from astra.action_configs import ObjectActionConfig
from astra.observatory import Observatory
from astra.utils.ephemeris import (
    _SOLAR_TO_SIDEREAL,
    compute_nonsidereal_rates_from_interp,
    precompute_ephemeris,
)
from astra.scheduler import Action


def _make_interp(slope=0.0, intercept=0.0):
    """Return a trivial linear interp1d (no real ephemeris needed)."""
    ts = np.array([0.0, 3600.0])
    vals = intercept + slope * ts
    return interp1d(ts, vals, kind="linear", fill_value="extrapolate")


def _make_action(
    action_type="object",
    nonsidereal=True,
    disable_telescope_movement=False,
    ra_interp=None,
    dec_interp=None,
    ra_rate_interp=None,
    dec_rate_interp=None,
    recenter_interval=0,
    lookup_name="mars",
    start_time=None,
    tle=None,
):
    """Build a minimal Action suitable for NonSiderealManager._setup."""
    if start_time is None:
        start_time = datetime(2025, 6, 1, 0, 0, 0, tzinfo=UTC)
    end_time = datetime(2025, 6, 1, 1, 0, 0, tzinfo=UTC)

    action_value = MagicMock()
    action_value.get = lambda key, default=None: {
        "_nonsidereal": nonsidereal,
        "disable_telescope_movement": disable_telescope_movement,
        "_ra_interp": ra_interp,
        "_dec_interp": dec_interp,
        "_ra_rate_interp": ra_rate_interp,
        "_dec_rate_interp": dec_rate_interp,
        "nonsidereal_recenter_interval": recenter_interval,
        "lookup_name": lookup_name,
        "tle": tle,
    }.get(key, default)

    return Action(
        device_name="cam1",
        action_type=action_type,
        action_value=action_value,
        start_time=start_time,
        end_time=end_time,
    )


def _make_active_manager(recenter_interval=0, ra_slope=1e-4, dec_slope=0.0):
    ra_interp = _make_interp(slope=ra_slope, intercept=100.0)
    dec_interp = _make_interp(slope=dec_slope, intercept=20.0)
    action = _make_action(
        ra_interp=ra_interp,
        dec_interp=dec_interp,
        recenter_interval=recenter_interval,
    )
    return NonSiderealManager(action, MagicMock())


class TestSetup:
    def test_inactive_when_nonsidereal_false(self):
        action = _make_action(nonsidereal=False)
        mgr = NonSiderealManager(action, MagicMock())
        assert not mgr.is_active

    def test_inactive_for_calibration_action(self):
        action = _make_action(action_type="calibration")
        mgr = NonSiderealManager(action, MagicMock())
        assert not mgr.is_active

    def test_inactive_when_telescope_movement_disabled(self):
        action = _make_action(disable_telescope_movement=True)
        mgr = NonSiderealManager(action, MagicMock())
        assert not mgr.is_active

    def test_inactive_and_logs_error_when_interps_missing(self):
        logger = MagicMock()
        action = _make_action(ra_interp=None, dec_interp=None)
        mgr = NonSiderealManager(action, logger)
        assert not mgr.is_active
        logger.error.assert_called_once()

    def test_active_with_valid_config(self):
        mgr = _make_active_manager()
        assert mgr.is_active

    def test_active_logs_recenter_disabled(self):
        logger = MagicMock()
        action = _make_action(
            ra_interp=_make_interp(),
            dec_interp=_make_interp(),
            recenter_interval=0,
        )
        NonSiderealManager(action, logger)
        logger.info.assert_called_once()
        assert "re-centering disabled" in logger.info.call_args[0][0]

    def test_active_logs_recenter_interval(self):
        logger = MagicMock()
        action = _make_action(
            ra_interp=_make_interp(),
            dec_interp=_make_interp(),
            recenter_interval=300,
        )
        NonSiderealManager(action, logger)
        assert "300s" in logger.info.call_args[0][0]


class TestApplyRates:
    def test_sets_rates_on_telescope(self):
        mgr = _make_active_manager()
        telescope = MagicMock()
        mgr.apply_rates(telescope)
        assert telescope.set.call_count == 2
        keys_set = {c.args[0] for c in telescope.set.call_args_list}
        assert keys_set == {"RightAscensionRate", "DeclinationRate"}

    def test_noop_when_inactive(self):
        action = _make_action(nonsidereal=False)
        mgr = NonSiderealManager(action, MagicMock())
        telescope = MagicMock()
        mgr.apply_rates(telescope)
        telescope.set.assert_not_called()

    def test_warns_and_does_not_raise_on_telescope_error(self):
        mgr = _make_active_manager()
        telescope = MagicMock()
        telescope.set.side_effect = RuntimeError("device offline")
        # Should not propagate
        mgr.apply_rates(telescope)
        mgr.logger.warning.assert_called_once()

    @patch("astra.nonsidereal.ephemeris.compute_nonsidereal_rates_from_interp")
    def test_uses_precomputed_rate_interpolators_when_available(self, rate_fn):
        action = _make_action(
            ra_interp=_make_interp(slope=1e-4, intercept=100.0),
            dec_interp=_make_interp(slope=0.0, intercept=20.0),
            ra_rate_interp=_make_interp(slope=0.0, intercept=0.123),
            dec_rate_interp=_make_interp(slope=0.0, intercept=-4.56),
        )
        mgr = NonSiderealManager(action, MagicMock())
        telescope = MagicMock()

        mgr.apply_rates(telescope)

        telescope.set.assert_any_call("RightAscensionRate", 0.123)
        telescope.set.assert_any_call("DeclinationRate", -4.56)
        rate_fn.assert_not_called()


def _make_telescope(can_ra=True, can_dec=True):
    """Mount mock reporting the given ASCOM differential-rate capabilities."""
    telescope = MagicMock()
    telescope.get.side_effect = lambda key, **kwargs: {
        "CanSetRightAscensionRate": can_ra,
        "CanSetDeclinationRate": can_dec,
    }.get(key, MagicMock())
    return telescope


class TestTelescopeCapabilityGate:
    """Non-sidereal tracking requires ASCOM differential rates on both axes."""

    def test_active_when_mount_supports_both_axes(self):
        mgr = NonSiderealManager(
            _make_action(ra_interp=_make_interp(), dec_interp=_make_interp()),
            MagicMock(),
            telescope=_make_telescope(can_ra=True, can_dec=True),
        )
        assert mgr.is_active

    @pytest.mark.parametrize(
        "can_ra,can_dec",
        [(False, True), (True, False), (False, False)],
        ids=["no_ra_rate", "no_dec_rate", "neither"],
    )
    def test_inactive_when_mount_lacks_a_rate_axis(self, can_ra, can_dec):
        # Following a moving target needs both axes; one is not enough.
        mgr = NonSiderealManager(
            _make_action(ra_interp=_make_interp(), dec_interp=_make_interp()),
            MagicMock(),
            telescope=_make_telescope(can_ra=can_ra, can_dec=can_dec),
        )
        assert not mgr.is_active

    @pytest.mark.parametrize(
        "kwargs",
        [{}, {"tle": "1 25544U ...\n2 25544 ..."}, {"recenter_interval": 300}],
        ids=["bare_lookup_name", "tle_supplied", "recenter_interval_set"],
    )
    def test_unsupported_mount_is_an_error_regardless_of_other_fields(self, kwargs):
        """Reaching _setup means the target already resolved as moving.

        Sidereal tracking would trail it, so this is an error however the action was
        written -- recenter_interval is a cadence knob, not an opt-in. logger.error
        clears error_free, which stops the sequence.
        """
        logger = MagicMock()
        mgr = NonSiderealManager(
            _make_action(ra_interp=_make_interp(), dec_interp=_make_interp(), **kwargs),
            logger,
            telescope=_make_telescope(can_ra=False, can_dec=False),
        )
        assert not mgr.is_active
        logger.error.assert_called_once()

    def test_unreadable_capabilities_assume_supported(self):
        """A transient query failure must not silently disable tracking."""
        logger = MagicMock()
        telescope = MagicMock()
        telescope.get.side_effect = ConnectionError("mount unreachable")

        mgr = NonSiderealManager(
            _make_action(ra_interp=_make_interp(), dec_interp=_make_interp()),
            logger,
            telescope=telescope,
        )
        assert mgr.is_active
        logger.warning.assert_called_once()

    def test_no_telescope_skips_the_check(self):
        """Callers without a paired mount keep the pre-capability behaviour."""
        mgr = NonSiderealManager(
            _make_action(ra_interp=_make_interp(), dec_interp=_make_interp()),
            MagicMock(),
            telescope=None,
        )
        assert mgr.is_active


def _make_rate_manager(ra_rate=0.01, dec_rate=0.0, rate_update_interval=None):
    """Manager whose rate interpolators return fixed values, so the gating logic
    can be exercised without any dependence on wall-clock time."""
    action = _make_action(
        ra_interp=_make_interp(intercept=100.0),
        dec_interp=_make_interp(intercept=20.0),
        ra_rate_interp=_make_interp(intercept=ra_rate),
        dec_rate_interp=_make_interp(intercept=dec_rate),
    )
    if rate_update_interval is not None:
        inner = action.action_value.get
        action.action_value.get = lambda key, default=None: (
            rate_update_interval
            if key == "nonsidereal_rate_update_interval"
            else inner(key, default)
        )
    return NonSiderealManager(action, MagicMock())


def _rate_calls(telescope):
    return [
        c
        for c in telescope.set.call_args_list
        if c.args and c.args[0] in ("RightAscensionRate", "DeclinationRate")
    ]


class TestRateUpdateGating:
    """Rate commands interrupt tracking on some mounts, so they must be rare."""

    def test_first_call_always_reaches_the_mount(self):
        mgr = _make_rate_manager()
        telescope = MagicMock()
        mgr.apply_rates(telescope)
        assert len(_rate_calls(telescope)) == 2  # RA and Dec

    def test_rapid_repeat_calls_do_not_reach_the_mount(self):
        """apply_rates is driven at up to 10 Hz from the exposure and save loops."""
        mgr = _make_rate_manager()
        telescope = MagicMock()

        for _ in range(50):
            mgr.apply_rates(telescope)

        assert len(_rate_calls(telescope)) == 2  # only the first call got through

    def test_unchanged_rate_is_not_resent_after_the_interval(self):
        mgr = _make_rate_manager()
        telescope = MagicMock()
        mgr.apply_rates(telescope)
        telescope.reset_mock()

        # Interval has elapsed, but the ephemeris still wants the same rate.
        mgr._state.last_rate_update_time -= 3600
        mgr.apply_rates(telescope)

        assert _rate_calls(telescope) == []

    def test_meaningful_change_is_sent_after_the_interval(self):
        mgr = _make_rate_manager(ra_rate=0.01)
        telescope = MagicMock()
        mgr.apply_rates(telescope)
        telescope.reset_mock()

        mgr._state.last_rate_update_time -= 3600
        mgr._state.last_applied_ra_rate = 0.0  # mount is holding something far off
        mgr.apply_rates(telescope)

        assert len(_rate_calls(telescope)) == 2

    def test_rate_crossing_zero_does_not_cause_a_command_storm(self):
        """Regression: a percentage-of-rate test is undefined near zero.

        Dec rate passes through zero whenever a target reaches maximum declination,
        and the old relative threshold fired on every call throughout.
        """
        mgr = _make_rate_manager(ra_rate=0.001, dec_rate=0.0)
        telescope = MagicMock()

        for _ in range(200):
            mgr._state.last_applied_dec_rate = 1e-9  # crawling through zero
            mgr.apply_rates(telescope)

        assert len(_rate_calls(telescope)) == 2

    def test_schedule_interval_is_honoured(self):
        mgr = _make_rate_manager(rate_update_interval=120.0)
        assert mgr.min_rate_update_interval == 120.0

        telescope = MagicMock()
        mgr.apply_rates(telescope)
        telescope.reset_mock()

        mgr._state.last_rate_update_time -= 60  # past the default, inside this mount's
        mgr._state.last_applied_ra_rate = 0.0
        mgr.apply_rates(telescope)
        assert _rate_calls(telescope) == []

        mgr._state.last_rate_update_time -= 120
        mgr.apply_rates(telescope)
        assert len(_rate_calls(telescope)) == 2

    def test_recenter_forces_a_push_despite_the_interval(self):
        """The mount just moved, so it needs a known-good rate immediately."""
        mgr = _make_rate_manager()
        paired_devices = MagicMock()
        paired_devices.telescope = MagicMock()

        mgr.apply_rates(paired_devices.telescope)
        paired_devices.telescope.reset_mock()

        with patch("time.sleep"):
            mgr.recenter(paired_devices, MagicMock())

        assert len(_rate_calls(paired_devices.telescope)) == 2

    def test_drift_converts_ra_seconds_to_arcsec_on_sky(self):
        """RA rate is seconds of time; Dec rate is arcsec. They must be comparable."""
        mgr = _make_rate_manager()
        state = mgr._state
        state.last_applied_ra_rate = 0.0
        state.last_applied_dec_rate = 0.0
        state.last_rate_update_time = None

        # 1 second of RA per sidereal second is 15 arcsec/s of coordinate, which
        # foreshortens to 15*cos(dec) arcsec/s on the sky. The fixture sits at dec=20.
        expected_ra = 15.0 * math.cos(math.radians(20.0)) * mgr.min_rate_update_interval
        drift_ra = mgr._projected_drift_arcsec(state, 1.0, 0.0, 0.0, now=0.0)
        assert drift_ra == pytest.approx(expected_ra, rel=1e-6)

        # The same numeric value in Dec is 1 arcsec/s, needing no cos(dec) factor.
        drift_dec = mgr._projected_drift_arcsec(state, 0.0, 1.0, 0.0, now=0.0)
        assert drift_dec == pytest.approx(mgr.min_rate_update_interval, rel=1e-6)

        # So an identical number on the RA axis counts for ~14x more drift.
        assert drift_ra / drift_dec == pytest.approx(
            15.0 * math.cos(math.radians(20.0)), rel=1e-6
        )


class TestEphemerisEpochGuard:
    """The interpolators are only meaningful at the epoch they were computed for."""

    def _action_with_epoch(self, epoch, start_time):
        action = _make_action(
            ra_interp=_make_interp(intercept=100.0),
            dec_interp=_make_interp(intercept=20.0),
            start_time=start_time,
        )
        inner = action.action_value.get
        action.action_value.get = lambda key, default=None: (
            epoch if key == "_ephemeris_epoch" else inner(key, default)
        )
        return action

    def test_matching_epoch_is_active(self):
        start = datetime(2025, 6, 1, 0, 0, 0, tzinfo=UTC)
        mgr = NonSiderealManager(
            self._action_with_epoch(Time(start), start), MagicMock()
        )
        assert mgr.is_active

    def test_stale_epoch_is_refused(self):
        """A schedule re-timed after validation must not track from a stale ephemeris.

        Reading positions for the wrong epoch points the mount confidently at the
        wrong patch of sky, so this has to be loud rather than silent.
        """
        logger = MagicMock()
        start = datetime(2025, 6, 1, 0, 0, 0, tzinfo=UTC)
        stale = Time(start) - 30 * u.day

        mgr = NonSiderealManager(self._action_with_epoch(stale, start), logger)

        assert not mgr.is_active
        logger.error.assert_called_once()

    def test_state_is_keyed_to_the_epoch_not_the_action(self):
        start = datetime(2025, 6, 1, 0, 0, 0, tzinfo=UTC)
        epoch = Time(start)
        mgr = NonSiderealManager(self._action_with_epoch(epoch, start), MagicMock())
        assert mgr._state.sequence_start_time.isot == epoch.isot

    def test_missing_epoch_falls_back_to_action_start(self):
        """Actions validated before the epoch was recorded still work."""
        mgr = _make_active_manager()
        assert mgr.is_active


class TestNonsiderealRateHelper:
    def test_compute_nonsidereal_rates_uses_sidereal_second_conversion(self):
        # Target advances 1 deg of RA coordinate per 60 solar seconds. One degree
        # is 240 seconds of RA, so the rate is 4 RA-seconds per solar second.
        # ASCOM wants it per *sidereal* second, and an interval of N solar seconds
        # spans N * _SOLAR_TO_SIDEREAL sidereal seconds, so the rate per sidereal
        # second is 4 / _SOLAR_TO_SIDEREAL (~3.98908, i.e. slightly less than 4).
        ra_interp = _make_interp(slope=1.0 / 60.0, intercept=0.0)
        dec_interp = _make_interp(slope=0.0, intercept=0.0)

        ra_rate, dec_rate = compute_nonsidereal_rates_from_interp(
            ra_interp,
            dec_interp,
            t_seconds=0.0,
            dt=60.0,
        )

        assert ra_rate == pytest.approx(4.0 / _SOLAR_TO_SIDEREAL, rel=1e-9)
        assert dec_rate == pytest.approx(0.0, abs=1e-12)

    def test_rate_helper_agrees_with_precomputed_rate_interpolators(self):
        """The finite-difference fallback must match precompute_ephemeris's rates.

        Both express ASCOM units per sidereal second; a mismatch here means one of
        the two solar/sidereal conversions has been inverted.
        """
        obs_time = Time("2025-06-01T00:00:00", format="isot", scale="utc")
        location = EarthLocation(lat=28.3 * u.deg, lon=-16.5 * u.deg, height=2390 * u.m)

        ra_interp, dec_interp, ra_rate_interp, dec_rate_interp = precompute_ephemeris(
            "mars", obs_time, 2.0, location, return_rates=True
        )

        t = 1800.0
        ra_rate_fd, dec_rate_fd = compute_nonsidereal_rates_from_interp(
            ra_interp, dec_interp, t
        )

        assert ra_rate_fd == pytest.approx(float(ra_rate_interp(t)), rel=1e-3)
        assert dec_rate_fd == pytest.approx(float(dec_rate_interp(t)), rel=1e-3)


class TestPrepointAndActivation:
    def test_prepoint_coordinates_return_offset_position(self):
        mgr = _make_active_manager(ra_slope=1e-3, dec_slope=2e-3)

        ra_deg, dec_deg = mgr.prepoint_coordinates(lead_time_seconds=60.0)

        assert ra_deg == pytest.approx((100.0 + 0.001 * 60.0) % 360.0)
        assert dec_deg == pytest.approx(20.0 + 0.002 * 60.0)

    def test_prepoint_coordinates_none_when_inactive(self):
        action = _make_action(nonsidereal=False)
        mgr = NonSiderealManager(action, MagicMock())
        assert mgr.prepoint_coordinates() is None

    def test_tracking_activation_time_is_start_plus_offset(self):
        start = datetime(2025, 6, 1, 0, 0, 0, tzinfo=UTC)
        action = _make_action(
            start_time=start,
            ra_interp=_make_interp(slope=1e-4, intercept=100.0),
            dec_interp=_make_interp(slope=0.0, intercept=20.0),
        )
        mgr = NonSiderealManager(action, MagicMock())

        activation_time = mgr.tracking_activation_time(lead_time_seconds=60.0)

        assert activation_time is not None
        assert activation_time.unix == pytest.approx((Time(start) + 60.0 * u.s).unix)

    def test_tracking_activation_time_none_when_inactive(self):
        action = _make_action(nonsidereal=False)
        mgr = NonSiderealManager(action, MagicMock())
        assert mgr.tracking_activation_time() is None


class TestResetRates:
    def test_zeros_both_rates(self):
        mgr = _make_active_manager()
        telescope = MagicMock()
        mgr.reset_rates(telescope)
        telescope.set.assert_any_call("RightAscensionRate", 0.0)
        telescope.set.assert_any_call("DeclinationRate", 0.0)

    def test_noop_when_inactive(self):
        action = _make_action(nonsidereal=False)
        mgr = NonSiderealManager(action, MagicMock())
        telescope = MagicMock()
        mgr.reset_rates(telescope)
        telescope.set.assert_not_called()

    def test_warns_and_does_not_raise_on_telescope_error(self):
        mgr = _make_active_manager()
        telescope = MagicMock()
        telescope.set.side_effect = RuntimeError("device offline")
        mgr.reset_rates(telescope)
        mgr.logger.warning.assert_called_once()


class TestShouldRecenter:
    def test_false_when_inactive(self):
        action = _make_action(nonsidereal=False)
        mgr = NonSiderealManager(action, MagicMock())
        assert not mgr.should_recenter()

    def test_false_when_interval_is_zero(self):
        mgr = _make_active_manager(recenter_interval=0)
        assert not mgr.should_recenter()

    def test_false_before_interval_elapses(self):
        mgr = _make_active_manager(recenter_interval=300)
        # last_recenter_time was just set — well within 300 s
        assert not mgr.should_recenter()

    def test_true_after_interval_elapses(self):
        mgr = _make_active_manager(recenter_interval=300)
        # Wind the clock back so the interval appears to have passed
        mgr._state.last_recenter_time -= 301
        assert mgr.should_recenter()


class TestRecenter:
    def _make_paired_devices(self):
        telescope = MagicMock()
        pd = MagicMock()
        pd.telescope = telescope
        return pd

    def test_returns_false_when_inactive(self):
        action = _make_action(nonsidereal=False)
        mgr = NonSiderealManager(action, MagicMock())
        assert mgr.recenter(MagicMock(), MagicMock()) is False

    def test_slews_to_current_position(self):
        mgr = _make_active_manager()
        paired_devices = self._make_paired_devices()
        wait_fn = MagicMock()

        with patch("time.sleep"):
            mgr.recenter(paired_devices, wait_fn)

        # recenter() does a coarse slew followed by a fine correction, because the
        # target keeps moving while the coarse slew is in progress.
        slew_calls = [
            c
            for c in paired_devices.telescope.get.call_args_list
            if c.args and c.args[0] == "SlewToCoordinatesAsync"
        ]
        assert len(slew_calls) == 2

        for call in slew_calls:
            ra_hours = call.kwargs["RightAscension"]
            dec_deg = call.kwargs["Declination"]
            assert 0.0 <= ra_hours < 24.0
            assert -90.0 <= dec_deg <= 90.0

    def test_fine_correction_retargets_after_coarse_slew(self):
        """The second slew must be re-evaluated, not a replay of the first."""
        mgr = _make_active_manager(ra_slope=1e-2)
        paired_devices = self._make_paired_devices()

        # Advance the clock between the two ephemeris evaluations so the target
        # has measurably moved by the time the fine correction is computed.
        base = Time(mgr._state.sequence_start_time)
        calls = {"n": 0}

        def fake_now():
            # First evaluation is the coarse slew; everything after it happens
            # 30 s later, once the coarse slew has completed.
            calls["n"] += 1
            return base if calls["n"] == 1 else base + 30 * u.s

        with patch("time.sleep"), patch("astra.nonsidereal.Time.now", fake_now):
            mgr.recenter(paired_devices, MagicMock())

        slew_calls = [
            c
            for c in paired_devices.telescope.get.call_args_list
            if c.args and c.args[0] == "SlewToCoordinatesAsync"
        ]
        assert len(slew_calls) == 2
        assert (
            slew_calls[0].kwargs["RightAscension"]
            != slew_calls[1].kwargs["RightAscension"]
        )

    def test_calls_wait_fn_and_reapplies_rates(self):
        mgr = _make_active_manager()
        paired_devices = self._make_paired_devices()
        wait_fn = MagicMock()

        with patch("time.sleep"):
            result = mgr.recenter(paired_devices, wait_fn)

        assert result is True
        # Once after the coarse slew, once after the fine correction.
        assert wait_fn.call_args_list == [
            ((paired_devices,), {}),
            ((paired_devices,), {}),
        ]
        # apply_rates sets RightAscensionRate and DeclinationRate
        keys_set = {c.args[0] for c in paired_devices.telescope.set.call_args_list}
        assert "RightAscensionRate" in keys_set
        assert "DeclinationRate" in keys_set

    def test_updates_last_recenter_time(self):
        mgr = _make_active_manager(recenter_interval=300)
        mgr._state.last_recenter_time -= 400  # pretend it's been a while
        paired_devices = self._make_paired_devices()

        with patch("time.sleep"):
            mgr.recenter(paired_devices, MagicMock())

        assert not mgr.should_recenter()  # timestamp was refreshed

    def test_returns_false_and_warns_on_error(self):
        mgr = _make_active_manager()
        paired_devices = self._make_paired_devices()
        paired_devices.telescope.get.side_effect = RuntimeError("slew failed")

        result = mgr.recenter(paired_devices, MagicMock())

        assert result is False
        mgr.logger.warning.assert_called_once()


class TestResolveSlewTargetForTLE:
    """A TLE target has no fixed position to fall back to."""

    def _observatory(self):
        obs = Observatory.__new__(Observatory)
        obs.logger = MagicMock()
        return obs

    def _paired_devices(self):
        paired = MagicMock()
        paired.__contains__ = lambda self, key: key == "Telescope"
        paired.__getitem__ = lambda self, key: "tel1"
        return paired

    def test_tle_target_without_active_tracking_is_an_error(self):
        """Without this, SkyCoord.from_name("TLE") raises out of pre_sequence.

        It has to be an error, not a warning: there is no fixed position to fall
        back on, so the sequence would otherwise expose on whatever the mount
        happens to be pointing at. logger.error clears error_free, stopping it.
        """
        obs = self._observatory()
        action_value = {
            "object": "ISS",
            "lookup_name": "TLE",
            "tle": "1 25544U ...\n2 25544 ...",
        }

        with patch("astra.observatory.ephemeris.get_body_coordinates") as get_coords:
            ra, dec = obs._resolve_slew_target(
                self._paired_devices(), action_value, nonsidereal=None
            )

        assert (ra, dec) == (None, None)
        get_coords.assert_not_called()
        obs.logger.error.assert_called_once()

    def test_tle_target_is_silent_when_movement_is_disabled(self):
        """disable_telescope_movement means no slew was expected; not a problem."""
        obs = self._observatory()
        action_value = {
            "object": "ISS",
            "lookup_name": "TLE",
            "tle": "1 25544U ...\n2 25544 ...",
            "disable_telescope_movement": True,
        }

        ra, dec = obs._resolve_slew_target(
            self._paired_devices(), action_value, nonsidereal=None
        )

        assert (ra, dec) == (None, None)
        obs.logger.error.assert_not_called()
        obs.logger.warning.assert_not_called()

    def test_ordinary_lookup_name_still_resolves(self):
        obs = self._observatory()
        obs.get_observatory_location = MagicMock(return_value=None)
        coord = SkyCoord(ra=10.0 * u.deg, dec=20.0 * u.deg)

        with patch(
            "astra.observatory.ephemeris.get_body_coordinates", return_value=coord
        ):
            ra, dec = obs._resolve_slew_target(
                self._paired_devices(),
                {"object": "M31", "lookup_name": "M31"},
                nonsidereal=None,
            )

        assert ra == pytest.approx(10.0)
        assert dec == pytest.approx(20.0)


class TestRateUpdateIntervalSource:
    """The schedule is the only source of the rate update interval."""

    def test_default_when_the_schedule_says_nothing(self):
        from astra.nonsidereal import _MIN_RATE_UPDATE_INTERVAL_S

        mgr = _make_rate_manager()
        assert mgr.min_rate_update_interval == _MIN_RATE_UPDATE_INTERVAL_S

    def test_schedule_can_ask_for_a_shorter_interval(self):
        """A fast target needs commands more often than the default allows."""
        mgr = _make_rate_manager(rate_update_interval=1.0)
        assert mgr.min_rate_update_interval == 1.0

    def test_schedule_can_ask_for_a_longer_interval(self):
        mgr = _make_rate_manager(rate_update_interval=120.0)
        assert mgr.min_rate_update_interval == 120.0

    def test_zero_removes_the_interval_gate(self):
        """Only the drift tolerance then decides when to send a rate."""
        mgr = _make_rate_manager(rate_update_interval=0)
        assert mgr.min_rate_update_interval == 0

    def test_negative_interval_is_rejected_by_the_config(self):
        with pytest.raises(ValueError, match="nonsidereal_rate_update_interval"):
            ObjectActionConfig(
                object="M42", exptime=1.0, nonsidereal_rate_update_interval=-1.0
            )


class TestRecenterIfLate:
    """A late start matters in proportion to how far the target has moved."""

    def _paired_devices(self):
        paired = MagicMock()
        paired.telescope = MagicMock()
        return paired

    def _run(self, ra_slope, late_s):
        """Start `late_s` after the activation time with a target of `ra_slope` deg/s."""
        mgr = _make_active_manager(ra_slope=ra_slope)
        activation = Time(mgr._state.sequence_start_time)
        now = activation + late_s * u.s
        paired = self._paired_devices()

        with patch("time.sleep"), patch("astra.nonsidereal.Time.now", lambda: now):
            recentred = mgr.recenter_if_late(activation, paired, MagicMock())
        return mgr, recentred, paired

    def test_no_recentre_when_not_late(self):
        mgr = _make_active_manager(ra_slope=1e-2)
        activation = Time(mgr._state.sequence_start_time) + 300 * u.s
        with patch(
            "astra.nonsidereal.Time.now",
            lambda: Time(mgr._state.sequence_start_time),
        ):
            assert (
                mgr.recenter_if_late(activation, self._paired_devices(), MagicMock())
                is False
            )

    def test_slow_target_tolerates_a_long_delay(self):
        """A comet-like rate: 60 s late is a couple of arcsec, not worth two slews."""
        _, recentred, paired = self._run(ra_slope=1e-5, late_s=60.0)
        assert recentred is False
        paired.telescope.get.assert_not_called()

    def test_fast_target_is_recentred_after_a_short_delay(self):
        """A satellite-like rate: the same 60 s puts it far outside the field."""
        _, recentred, paired = self._run(ra_slope=1e-2, late_s=60.0)
        assert recentred is True
        slews = [
            c
            for c in paired.telescope.get.call_args_list
            if c.args and c.args[0] == "SlewToCoordinatesAsync"
        ]
        assert slews

    def test_threshold_is_an_angle_not_a_grace_period(self):
        """The old fixed 30 s cut-off could not separate these two cases.

        Both start 60 s late. Only the drift distinguishes them.
        """
        _, slow, _ = self._run(ra_slope=1e-5, late_s=60.0)
        _, fast, _ = self._run(ra_slope=1e-2, late_s=60.0)
        assert (slow, fast) == (False, True)

    def test_inactive_manager_does_nothing(self):
        action = _make_action(nonsidereal=False)
        mgr = NonSiderealManager(action, MagicMock())
        assert mgr.recenter_if_late(Time.now(), MagicMock(), MagicMock()) is False

    def test_drift_between_measures_sky_separation(self):
        """RA drift foreshortens by cos(dec); the fixture sits at dec=20."""
        mgr = _make_active_manager(ra_slope=1e-3, dec_slope=0.0)
        start = Time(mgr._state.sequence_start_time)
        drift = mgr.drift_between(start, start + 100 * u.s)

        expected = 1e-3 * 100 * 3600 * math.cos(math.radians(20.0))
        assert drift == pytest.approx(expected, rel=1e-3)
