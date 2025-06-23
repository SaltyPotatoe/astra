Guiding Module
=============

.. automodule:: astra.guiding
   :members:
   :undoc-members:
   :show-inheritance:

Overview
--------

The ``guiding`` module provides functionality for autoguiding during astronomical observations. Autoguiding is the process of making small corrections to the telescope's position during long exposures to maintain precise pointing.

Key Components
-------------

- ``Guider``: Main class that handles the autoguiding process
- ``guider_loop``: Main loop for the autoguiding process that monitors images and makes corrections
- ``initialize_reference_frame``: Create a reference frame for guiding
- ``compute_offsets``: Calculate the offset between current and reference images