Safety Monitoring
===============

Astra includes comprehensive safety monitoring features to protect your observatory equipment from weather hazards and other potentially harmful conditions.

Overview
--------

The safety monitoring system continuously checks:

1. Weather conditions through an ASCOM SafetyMonitor device
2. Observatory status parameters
3. Communication with critical devices

When unsafe conditions are detected, Astra will automatically close the observatory and park all equipment to prevent damage.

Configuration
------------

Safety monitoring is configured in two sections of your observatory configuration:

SafetyMonitor Configuration
~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: yaml

    SafetyMonitor:
      - device_name: "Weather Monitor"
        device_type: "SafetyMonitor"
        device_number: 0
        address: "localhost:11116"
        max_safe_duration: 30  # Minutes before stale data is considered unsafe

The ``max_safe_duration`` parameter specifies how long (in minutes) Astra will consider safety data valid. If the safety monitor hasn't provided updated data within this timeframe, Astra will assume conditions are unsafe.

ObservingConditions Configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For more granular weather monitoring:

.. code-block:: yaml

    ObservingConditions:
      - device_name: "Weather Station"
        device_type: "ObservingConditions"
        device_number: 0
        address: "localhost:11117"
        closing_limits:
          Humidity:
            - limit: 85               # Close if humidity exceeds 85%
              duration: 10            # For at least 10 minutes
          WindSpeed:
            - limit: 40               # Close if wind speed exceeds 40 km/h
              duration: 5             # For at least 5 minutes
          CloudCover:
            - limit: 90               # Close if cloud cover exceeds 90%
              duration: 15            # For at least 15 minutes

Each ``closing_limits`` entry specifies:
- The measurement to monitor
- The threshold value
- How long the threshold must be exceeded before taking action

How It Works
----------

The safety monitoring system works as follows:

1. **Continuous Monitoring**: Astra polls the SafetyMonitor device and ObservingConditions device at regular intervals
2. **Data Analysis**: Weather data is analyzed against configured thresholds
3. **Decision Making**: If unsafe conditions are detected, Astra initiates closure procedures
4. **Closure Sequence**:
   - Stop any ongoing observations
   - Park the telescope
   - Close the dome (if present)
   - Continue monitoring conditions

Safety-related actions are executed regardless of the current schedule status and have the highest priority.

Weather-Sensitive Actions
----------------------

Not all actions in the schedule are weather-sensitive:

- **Weather-sensitive actions** (only run when conditions are safe):
  - Opening the observatory
  - Taking science images
  - Pointing model creation
  - Flat field acquisition

- **Non-weather-sensitive actions** (run regardless of weather):
  - Taking darks and bias frames
  - Closing the observatory
  - Image header completion
  - Database backups

Recovery From Unsafe Conditions
----------------------------

When conditions return to safe:

1. Astra waits for the duration specified in the configuration (to avoid rapid cycling)
2. If conditions remain safe for that duration, weather status is marked as safe
3. Weather-sensitive operations in the schedule can resume
4. The observatory is NOT automatically reopened - this requires a scheduled "open" action

Safety System Controls
------------

Currently, there is no built-in manual override for the safety system. All safety features are controlled through the configuration files and operate automatically during runtime. This is an intentional design choice to prevent accidental overrides that could put equipment at risk.

Best Practices
------------

To ensure proper safety monitoring:

- Test your safety monitoring systems regularly
- Configure conservative thresholds for closure conditions
- Use reliable weather data sources
- Include redundant safety systems when possible
- Check logs regularly for any safety-related warnings
- Allow adequate time in your schedule between weather-sensitive actions