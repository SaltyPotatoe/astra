Pointing Correction
=================

Astra includes a pointing correction system that improves telescope pointing accuracy by analyzing star patterns in images and comparing them with star catalogs.

Overview
--------

The pointing correction system uses astrometric plate solving to:

1. Determine the exact coordinates where the telescope is pointing
2. Compare with the requested target coordinates
3. Calculate the offset between actual and requested position
4. Apply corrections to center the target accurately

This process helps compensate for mechanical imprecisions and improves target acquisition.

Configuration
------------

Pointing correction settings are defined in the telescope section of your observatory configuration:

.. code-block:: yaml

    Telescope:
      - device_name: "Main Telescope"
        device_type: "Telescope"
        device_number: 0
        address: "localhost:11111"
        pointing_threshold: 30  # Threshold in arcseconds

The ``pointing_threshold`` parameter determines when pointing corrections are applied:

- If the offset between actual and requested position is greater than this threshold, a correction is applied
- If the offset is less than this threshold, no correction is needed

Using Pointing Correction
-----------------------

To enable pointing correction during observation, set ``pointing: true`` in your object action parameters:

.. code-block:: python

    {
        'object': 'M51',
        'ra': 202.48,
        'dec': 47.195,
        'exptime': 120,
        'n': 10,
        'filter': 'R',
        'guiding': true,
        'pointing': true     # Enable pointing correction
    }

How It Works
----------

When pointing correction is enabled:

1. Astra takes an initial image after slewing to the target
2. The image is analyzed to identify star patterns
3. Star patterns are matched with catalog data to determine precise coordinates
4. If the offset exceeds the threshold:
   - The telescope position is updated
   - A new slew is performed to center the target
5. This process repeats until the pointing is within the threshold

Pointing Model
------------

Astra can build and maintain a pointing model to improve pointing accuracy over time:

1. Add a ``pointing_model`` action to your schedule:

   .. code-block:: text

       device_type,device_name,action_type,action_value,start_time,end_time
       Camera,Main Camera,pointing_model,"{'n': 20, 'exptime': 2}",2025-04-14T19:00:00,2025-04-14T19:45:00

2. The pointing model process:
   - Takes images at different points across the sky
   - Calculates pointing errors at each position
   - Builds a model of systematic pointing errors
   - Saves the model for future use

Parameters:
   - ``n``: Number of points to measure (recommended: 15-30)
   - ``exptime``: Exposure time for each image in seconds

Best Practices
------------

For optimal pointing accuracy:

- Build a pointing model before important observation sessions
- Verify your mount's polar alignment
- Use longer exposures for pointing model images to detect more stars
- Build your model across the sky area where you'll be observing
- Allow adequate time for the pointing model procedure (typically 30-60 minutes)