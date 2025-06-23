Autoguiding
==========

Astra provides an integrated autoguiding system to maintain precise telescope tracking during long exposures. This section explains how to set up and use the autoguiding functionality.

Overview
--------

Autoguiding works by analyzing the position of stars in images and making small corrections to the telescope position to keep stars precisely centered. Astra uses the same main camera for both science imaging and guiding (self-guiding), eliminating the need for a separate guide camera.

The guiding system:

1. Takes an initial reference image when guiding starts
2. Monitors subsequent images for star movement
3. Sends correction commands to the telescope to maintain position
4. Logs guiding performance for analysis

Configuration
------------

Autoguiding configuration is defined in the telescope section of your observatory configuration file:

.. code-block:: yaml

    Telescope:
      - device_name: "Main Telescope"
        device_type: "Telescope"
        device_number: 0
        address: "localhost:11111"
        pointing_threshold: 30
        
        # Guider configuration
        guider:
          guiding_interval: 10          # Check for drift every N seconds
          guiding_max_correction: 5     # Maximum correction in arcseconds
          kernel_size: 48               # Size of tracking kernel in pixels
          update_ref_frames: 10         # Update reference frame every N frames
          log_to_db: true               # Save guiding logs to database
          min_stars: 5                  # Minimum stars required for guiding
          calibration_file: "guiding_calibration.json"

Calibration
----------

Before using autoguiding, you need to calibrate the system to determine how the telescope responds to guide commands:

1. Add a ``calibrate_guiding`` action to your schedule before observation sessions:

   .. code-block:: text

       device_type,device_name,action_type,action_value,start_time,end_time
       Camera,Main Camera,calibrate_guiding,"{'exptime': 2}",2025-04-14T20:00:00,2025-04-14T20:15:00

2. The calibration process:
   - Slews the telescope to an appropriate position 
   - Takes a series of exposures while moving the telescope
   - Analyzes the movement to determine guide rates
   - Saves the calibration data for future use

Using Autoguiding
----------------

To enable autoguiding during an observation:

1. Set ``guiding: true`` in your object action parameters:

   .. code-block:: python

       {
           'object': 'M51',
           'ra': 202.48,
           'dec': 47.195,
           'exptime': 120,
           'n': 10,
           'filter': 'R',
           'guiding': true,    # Enable autoguiding
           'pointing': true
       }

2. Astra will:
   - Take an initial reference frame
   - Begin monitoring subsequent images
   - Apply corrections as needed
   - Log guiding performance

Guiding Logs
-----------

Guiding logs are stored in the observatory database and can be analyzed to measure guiding performance. The logs record:

- Timestamp for each measurement
- X and Y offsets detected
- Corrections applied
- Reference stars used

The logs can be queried from the database:

.. code-block:: sql

    SELECT * FROM autoguider_log_new WHERE datetime > datetime('now', '-24 hours')

Troubleshooting
--------------

Common autoguiding issues and solutions:

- **No stars detected**: Check exposure time, focus, and ensure telescope is pointed at a star field
- **Erratic corrections**: Recalibrate guiding, check for mechanical issues
- **Drift in one direction**: Check polar alignment and balance
- **Guiding stops**: Check logs for error messages, may need to adjust minimum stars setting