Alpaca Device Process Module
=======================

.. automodule:: astra.alpaca_device_process
   :members:
   :undoc-members:
   :show-inheritance:

Overview
--------

The ``alpaca_device_process`` module provides functionality for communicating with astronomical devices using the ASCOM Alpaca protocol. It handles the creation and management of device processes.

Key Components
-------------

- ``AlpacaDevice``: Main class for interacting with Alpaca devices
- ``start_poll``: Start polling a device for status information
- ``pause_poll``: Pause polling a device
- ``resume_poll``: Resume polling a device
- ``get``: Get a property or execute a method on an Alpaca device
- ``set``: Set a property on an Alpaca device