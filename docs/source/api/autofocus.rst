Autofocus Module
===============

.. automodule:: astra.autofocus
   :members:
   :undoc-members:
   :show-inheritance:

Overview
--------

The ``autofocus`` module provides functionality for automatically focusing telescopes. It analyzes a series of images taken at different focus positions to determine the optimal focus setting.

Key Components
-------------

- ``Autofocuser``: Main class that handles the autofocus process
- ``determine_autofocus_calibration_field``: Selects a suitable field for focusing
- ``slew_to_calibration_field``: Moves the telescope to the focus calibration position
- ``setup``: Sets up the autofocus sequence
- ``run``: Executes the autofocus sequence
- ``make_summary_plot``: Creates plots showing the focus run results
- ``create_result_file``: Saves the autofocus results