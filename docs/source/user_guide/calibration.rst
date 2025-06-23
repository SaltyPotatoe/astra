Calibration Frames
================

Astra provides automated acquisition of calibration frames (bias and dark frames) that are essential for scientific image processing.

Overview
--------

Calibration frames are used to remove systematic noise from your astronomical images:

- **Bias frames**: Zero-second exposures that capture the electronic readout pattern of the camera
- **Dark frames**: Long exposures with the shutter closed that capture thermal noise

Astra can automatically acquire these calibration frames as part of your observation schedule.

Configuration
------------

No specific configuration is needed for calibration frames beyond the basic configuration of the camera. The camera should be properly cooled and stabilized at its operating temperature.

Running Calibration Sequences
---------------------------

To add calibration frame acquisition to your observation schedule:

.. code-block:: text

    device_type,device_name,action_type,action_value,start_time,end_time
    Camera,Main Camera,calibration,"{'exptime': [0, 60, 120, 300], 'n': [10, 5, 5, 3]}",2025-04-15T00:30:00,2025-04-15T01:30:00

Parameters:
- ``exptime``: List of exposure times in seconds (0 for bias frames)
- ``n``: List of how many frames to take for each exposure time

For example, the configuration above will take:
- 10 bias frames (0-second exposures)
- 5 dark frames at 60 seconds each
- 5 dark frames at 120 seconds each
- 3 dark frames at 300 seconds each

Best Practices
------------

For optimal calibration frames:

1. **Temperature**:
   - Keep the camera at a stable temperature
   - Take calibration frames at the same temperature as light frames
   - Allow at least 20-30 minutes for the camera to stabilize after cooling

2. **Bias Frames**:
   - Take at least 10-20 bias frames
   - Bias frames should be taken with the shutter closed (0-second exposure)
   - They can be taken at any time (not weather-dependent)

3. **Dark Frames**:
   - Match dark frame exposure times to your light frame exposures
   - Take at least 5-10 dark frames for each exposure time
   - Consider taking darks at the end of the night when not using the telescope

4. **Timing**:
   - Schedule calibration frames when you don't need the telescope for imaging
   - Bias and dark frames can be taken regardless of weather conditions
   - They're ideal to schedule during periods of poor weather

Matching Dark Frames to Light Frames
----------------------------------

For proper calibration, your dark frame exposure times should match your light frame exposure times. If you plan to take 120-second exposures of your target, you should include 120-second dark frames in your calibration sequence.

Example Full Schedule with Calibration
-----------------------------------

Here's an example of a complete schedule including calibration:

.. code-block:: text

    device_type,device_name,action_type,action_value,start_time,end_time
    Camera,Main Camera,open,"{}",2025-04-14T19:00:00,2025-04-14T19:05:00
    Camera,Main Camera,cool_camera,"{}",2025-04-14T19:05:00,2025-04-14T19:30:00
    Camera,Main Camera,flats,"{'filter': ['R', 'G', 'B'], 'n': [5, 5, 5]}",2025-04-14T19:30:00,2025-04-14T20:00:00
    Camera,Main Camera,autofocus,"{'exptime': 3, 'filter': 'R'}",2025-04-14T20:05:00,2025-04-14T20:20:00
    Camera,Main Camera,object,"{'object': 'M51', 'ra': 202.48, 'dec': 47.195, 'exptime': 120, 'n': 10, 'filter': 'R', 'guiding': true, 'pointing': true}",2025-04-14T20:30:00,2025-04-14T22:30:00
    Camera,Main Camera,object,"{'object': 'M101', 'ra': 210.80, 'dec': 54.35, 'exptime': 180, 'n': 8, 'filter': 'R', 'guiding': true, 'pointing': true}",2025-04-14T22:30:00,2025-04-15T00:30:00
    Camera,Main Camera,calibration,"{'exptime': [0, 120, 180], 'n': [10, 5, 5]}",2025-04-15T00:30:00,2025-04-15T01:30:00
    Camera,Main Camera,complete_headers,"{}",2025-04-15T01:30:00,2025-04-15T01:35:00
    Camera,Main Camera,close,"{}",2025-04-15T01:35:00,2025-04-15T01:40:00

Note that the calibration frames in this example match the exposure times used for the science targets (120s and 180s).