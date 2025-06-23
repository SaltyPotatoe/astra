Autofocus
=========

Astra includes an automated focusing system that can determine the optimal focus position for your telescope using image analysis techniques.

Overview
--------

The autofocus system works by taking a series of images at different focuser positions and analyzing the sharpness of stars in each image. It then determines the optimal focus position by fitting a curve to the measured sharpness values.

Astra uses the `astrafocus` library to perform the focus analysis, providing reliable results across a wide range of conditions.

Configuration
------------

No specific configuration is needed for the autofocus function beyond the basic configuration of the camera and focuser. The focuser should be properly connected and configured in your observatory configuration file:

.. code-block:: yaml

    Focuser:
      - device_name: "Main Focuser"
        device_type: "Focuser"
        device_number: 0
        address: "localhost:11114"
        temperature_compensation: false

Running Autofocus
---------------

To perform an autofocus operation, add an `autofocus` action to your schedule:

.. code-block:: text

    device_type,device_name,action_type,action_value,start_time,end_time
    Camera,Main Camera,autofocus,"{'exptime': 3, 'filter': 'R'}",2025-04-14T20:05:00,2025-04-14T20:30:00

Parameters
~~~~~~~~~

The autofocus action accepts these parameters:

- ``exptime``: Exposure time in seconds for focus images
- ``filter``: Which filter to use during focusing

Focus Process
------------

When the autofocus sequence runs, it goes through these steps:

1. **Field Selection**: Identifies a suitable field with enough stars
2. **Telescope Slew**: Moves the telescope to the focus calibration position
3. **Initial Focus Run**: Takes images at different focus positions
4. **Analysis**: Measures star sharpness in each image
5. **Curve Fitting**: Fits a curve to determine optimal focus
6. **Final Position**: Moves the focuser to the optimal position
7. **Verification**: Takes a final image to verify focus quality
8. **Results**: Generates plots and saves focus results

Focus Results
-----------

After an autofocus run, Astra generates:

1. A focus curve plot showing the measure of sharpness vs. focuser position
2. A results file detailing the focus run
3. Log entries with the optimal focus position

The focus results are saved in the `autofocus` directory within your data storage location.

Best Practices
------------

For best autofocus results:

- Run autofocus at the beginning of each observation night
- Repeat autofocus when temperature changes significantly (more than 2°C)
- Run autofocus when changing filters with significant focus shifts
- Use exposure times that show at least 15-20 stars of moderate brightness
- Allow adequate time for the autofocus routine (typically 10-20 minutes)

Troubleshooting
-------------

Common autofocus issues and solutions:

- **Not enough stars**: Increase exposure time or choose a field with more stars
- **Poor curve fit**: Check for mechanical issues or vibrations
- **Inconsistent results**: Verify the focuser has no backlash or slippage
- **Failed to reach focus**: Ensure the focus range encompasses the optimal position