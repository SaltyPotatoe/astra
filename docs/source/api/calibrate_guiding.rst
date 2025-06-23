Calibrate Guiding Module
=====================

.. automodule:: astra.calibrate_guiding
   :members:
   :undoc-members:
   :show-inheritance:

Overview
--------

The ``calibrate_guiding`` module provides functionality for calibrating the autoguiding system. It determines how the telescope responds to guide commands in different directions.

Key Components
-------------

- ``GuidingCalibrator``: Main class for calibrating the guiding system
- ``slew_telescope_one_hour_east_of_sidereal_meridian``: Position telescope for calibration
- ``perform_calibration_cycles``: Execute the calibration procedure
- ``complete_calibration_config``: Finalize calibration settings
- ``save_calibration_config``: Save calibration results
- ``update_observatory_config``: Update observatory configuration with calibration data