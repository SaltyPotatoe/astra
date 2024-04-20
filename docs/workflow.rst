Workflow
========

For each observatory, an :code:`Astra` object is instantiated and triggers the run of parallel threads. These threads monitor and set multiple flags, that can in-turn be used to trigger actions.

 
Main `Astra` flags
--------------------

.. autoattribute:: astra.Astra.weather_safe

.. autoattribute:: astra.Astra.error_free

.. autoattribute:: astra.Astra.schedule_running

.. autoattribute:: astra.Astra.watchdog_running

.. autoattribute:: astra.Astra.interrupt

.. autoattribute:: astra.Astra.queue_running

Main `Astra` threads
--------------------

.. automethod:: astra.Astra.watchdog

.. automethod:: astra.Astra.run_schedule

.. automethod:: astra.Astra.run_action

.. automethod:: astra.Astra.toggle_interrupt