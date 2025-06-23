Scheduling
==========

Astra uses a scheduling system to automate observatory operations. The schedule is defined as a CSV file that specifies what actions to perform and when to perform them.

Schedule Format
-------------

The schedule file is a CSV file with the following columns:

- ``device_type``: Type of device to perform the action (e.g., "Camera")
- ``device_name``: Name of the specific device (must match a device_name in the configuration)
- ``action_type``: Type of action to perform
- ``action_value``: Parameters for the action (in Python dictionary format)
- ``start_time``: When the action should start (ISO format: YYYY-MM-DDThh:mm:ss)
- ``end_time``: When the action should end (ISO format: YYYY-MM-DDThh:mm:ss)

Supported Action Types
--------------------

Astra supports the following action types:

- ``open``: Open the observatory (dome, unpark telescope)
- ``close``: Close the observatory (park telescope, close dome)
- ``cool_camera``: Turn on camera cooling and set target temperature
- ``object``: Take science images of a target object
- ``calibration``: Take calibration frames (darks or bias)
- ``flats``: Take flat field calibration frames
- ``autofocus``: Perform autofocus operation
- ``calibrate_guiding``: Calibrate the autoguiding system
- ``pointing_model``: Build or refine a pointing model
- ``complete_headers``: Finalize FITS headers with additional metadata

Action Parameters
---------------

Each action type requires specific parameters in the ``action_value`` field:

Object Imaging
~~~~~~~~~~~~

Parameters for ``object`` action type:

.. code-block:: python

    {
        'object': 'M51',                # Target name
        'ra': 202.48,                   # Right Ascension in degrees
        'dec': 47.195,                  # Declination in degrees
        'exptime': 120,                 # Exposure time in seconds
        'n': 10,                        # Number of exposures
        'filter': 'R',                  # Filter name
        'guiding': True,                # Enable autoguiding
        'pointing': True,               # Enable pointing correction
        'bin': 1                        # Binning factor (optional)
    }

Calibration Frames
~~~~~~~~~~~~~~~~

Parameters for ``calibration`` action type:

.. code-block:: python

    {
        'exptime': [0, 10, 30],         # List of exposure times
        'n': [10, 5, 5]                 # List of number of exposures for each time
    }

Flat Fields
~~~~~~~~~~

Parameters for ``flats`` action type:

.. code-block:: python

    {
        'filter': ['R', 'G', 'B'],      # List of filters
        'n': [10, 10, 10]               # Number of flats per filter
    }

Autofocus
~~~~~~~~

Parameters for ``autofocus`` action type:

.. code-block:: python

    {
        'exptime': 3,                   # Exposure time for focus frames
        'filter': 'R'                   # Filter to use for focusing
    }

Opening and Closing
~~~~~~~~~~~~~~~~~

The ``open`` and ``close`` action types typically use an empty dictionary:

.. code-block:: python

    {}

Typical Schedule Structure
------------------------

A typical night's schedule might follow this pattern:

1. Open observatory
2. Cool camera
3. Take evening flat fields
4. Perform autofocus
5. Observe scientific targets
6. Take calibration frames
7. Complete headers
8. Close observatory

Example Schedule
--------------

Here's a example of a complete schedule for a night's observation:

.. code-block:: text

    device_type,device_name,action_type,action_value,start_time,end_time
    Camera,Main Camera,open,"{}",2025-04-14T19:00:00,2025-04-14T19:05:00
    Camera,Main Camera,cool_camera,"{}",2025-04-14T19:05:00,2025-04-14T19:30:00
    Camera,Main Camera,flats,"{'filter': ['R', 'G', 'B'], 'n': [5, 5, 5]}",2025-04-14T19:30:00,2025-04-14T20:00:00
    Camera,Main Camera,autofocus,"{'exptime': 3, 'filter': 'R'}",2025-04-14T20:05:00,2025-04-14T20:20:00
    Camera,Main Camera,object,"{'object': 'M51', 'ra': 202.48, 'dec': 47.195, 'exptime': 120, 'n': 10, 'filter': 'R', 'guiding': true, 'pointing': true}",2025-04-14T20:30:00,2025-04-14T22:30:00
    Camera,Main Camera,object,"{'object': 'M101', 'ra': 210.80, 'dec': 54.35, 'exptime': 180, 'n': 8, 'filter': 'R', 'guiding': true, 'pointing': true}",2025-04-14T22:30:00,2025-04-15T00:30:00
    Camera,Main Camera,calibration,"{'exptime': [0, 120, 180], 'n': [10, 3, 3]}",2025-04-15T00:30:00,2025-04-15T01:30:00
    Camera,Main Camera,complete_headers,"{}",2025-04-15T01:30:00,2025-04-15T01:35:00
    Camera,Main Camera,close,"{}",2025-04-15T01:35:00,2025-04-15T01:40:00

Weather Conditions
----------------

Astra continuously monitors weather conditions using the SafetyMonitor device. Weather-sensitive actions (like opening the observatory or taking object frames) will only execute if weather conditions are safe. Calibration frames and other non-weather-dependent actions will still run in unsafe weather.

Schedule Control
---------------------

The schedule is loaded automatically when Astra starts. Currently, schedule control is handled through the Astra interface and not through command-line arguments. The schedule automatically runs according to the times specified.

Schedule File Location
-------------------

The schedule file should be located in the ``schedule`` directory and named after the observatory (e.g., ``schedule/my_observatory.csv``). Astra will automatically detect and load this file when it starts.