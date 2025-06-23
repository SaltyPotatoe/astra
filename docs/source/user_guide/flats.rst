Flat Field Acquisition
====================

Astra provides automated flat field acquisition to facilitate proper calibration of your astronomical images.

Overview
--------

Flat fields are calibration frames used to correct for uneven illumination and dust shadows in your optical system. Astra can automatically take flat fields during twilight when the sky has optimal brightness.

The flat field system:

1. Waits for the right twilight conditions
2. Points the telescope to the optimal sky position
3. Calculates appropriate exposure times
4. Acquires the specified number of flat frames

Configuration
------------

Flat field settings are defined in the camera section of your observatory configuration:

.. code-block:: yaml

    Camera:
      - device_name: "Main Camera"
        device_type: "Camera"
        device_number: 0
        address: "localhost:11112"
        
        # Flats configuration
        flats:
          target_adu: 30000        # Target ADU level for flats
          bias_offset: 1000        # Camera bias level to subtract
          lower_exptime_limit: 0.1 # Minimum exposure time
          upper_exptime_limit: 15  # Maximum exposure time

Parameters:
- ``target_adu``: The desired brightness level (in ADU) for your flat frames
- ``bias_offset``: The approximate bias level of your camera
- ``lower_exptime_limit``: The shortest exposure time to use
- ``upper_exptime_limit``: The longest exposure time to use

Running Flat Field Acquisition
----------------------------

To add flat field acquisition to your observation schedule:

.. code-block:: text

    device_type,device_name,action_type,action_value,start_time,end_time
    Camera,Main Camera,flats,"{'filter': ['R', 'G', 'B'], 'n': [10, 10, 10]}",2025-04-14T19:30:00,2025-04-14T20:00:00

Parameters:
- ``filter``: List of filters for which to take flats
- ``n``: List of how many flats to take for each filter

The time window should overlap with twilight (dawn or dusk) when the sky is at the right brightness.

How It Works
----------

When a flat field sequence runs:

1. **Wait for optimal conditions**: Astra checks if the sun is at the right elevation (-1° to -10°)
2. **Position telescope**: Points to a blank sky area opposite the sun
3. **Calculate exposure**: Takes test exposures to determine the optimal exposure time
4. **Take flats**: Captures the requested number of flat frames for each filter
5. **Adjust exposure**: Continuously adjusts exposure time as sky brightness changes

Best Practices
------------

For optimal flat fields:

- Schedule flat acquisition during twilight (dawn or dusk)
- Allow enough time in your schedule (15-30 minutes)
- Take flats for all filters you plan to use
- Take at least 10 flats per filter for good statistical averaging
- Check the resulting flat fields for proper exposure level (30-50% of full well capacity)

Troubleshooting
-------------

Common flat field issues and solutions:

- **Too bright/dark**: Adjust the target_adu setting
- **Uneven illumination**: Check for vignetting or improper pointing
- **Gradient in flats**: Point to a different area of the sky
- **Stars in flats**: Take flats when the sky is brighter (closer to sunset/sunrise)
- **Time window issues**: Adjust the schedule timing to better match twilight