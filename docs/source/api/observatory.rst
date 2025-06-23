Observatory Module
=================

.. automodule:: astra.observatory
   :members:
   :undoc-members:
   :show-inheritance:

Overview
--------

The ``observatory`` module contains the main ``Observatory`` class that manages all aspects of an astronomical observatory's automation using ASCOM Alpaca-compatible devices.

Key Components
-------------

Observatory Management
~~~~~~~~~~~~~~~~~~~~

- ``Observatory``: Main class for managing the observatory operation
- ``create_db``: Create database for the observatory
- ``backup``: Backup database tables
- ``read_config``: Read observatory configuration files

Device Interactions
~~~~~~~~~~~~~~~~~ 

- ``load_devices``: Load and initialize ASCOM Alpaca devices
- ``connect_all``: Connect to all devices and start polling
- ``pause_polls``: Pause polling on devices
- ``resume_polls``: Resume polling on devices

Watchdog and Safety
~~~~~~~~~~~~~~~~~ 

- ``start_watchdog``: Start the watchdog thread
- ``watchdog``: Monitor safety and system status
- ``internal_safety_weather_monitor``: Monitor internal safety conditions
- ``check_devices_alive``: Check if all devices are responding
- ``update_heartbeat``: Update the system heartbeat status

Observatory Operations
~~~~~~~~~~~~~~~~~~~~ 

- ``open_observatory``: Open the observatory (dome, telescope etc.)
- ``close_observatory``: Close the observatory safely
- ``flats_position``: Position for taking flat-field calibrations
- ``flats_exptime``: Calculate optimal exposure time for flats

Scheduling and Observation
~~~~~~~~~~~~~~~~~~~~~~~

- ``read_schedule``: Read and parse observation schedule
- ``start_schedule``: Start executing the schedule
- ``run_schedule``: Execute scheduled observations and actions
- ``run_action``: Execute specific action from schedule
- ``image_sequence``: Execute image acquisition sequence
- ``pointing_correction``: Perform telescope pointing correction
- ``pointing_model_sequence``: Build a pointing model for the telescope
- ``guiding_calibration_sequence``: Calibrate the autoguider
- ``autofocus_sequence``: Perform autofocus operation
- ``flats_sequence``: Acquire flat-field calibration frames

FITS Header Management
~~~~~~~~~~~~~~~~~~~~ 

- ``base_header``: Create base FITS headers for images
- ``final_headers``: Complete FITS headers with metadata
- ``perform_exposure``: Perform camera exposure and handle image data