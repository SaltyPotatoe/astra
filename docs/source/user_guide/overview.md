# Overview

This User Guide assists users in configuring and operating observatories with _Astra_.
It covers the main topics required to get started:

- **[Observatory Configuration](observatory_configuration)**:

  Configure your observatory's hardware and safety limits.

- **[FITS Header Configuration](fits_header_configuration)**:

  Manage FITS headers, linking device methods to FIT keywords.

- **[Scheduling](scheduling)**:

  Develop automated observing plans using JSON Lines (`.jsonl`) files.

- **[Operation](operation)**:

  Explore the operational lifecycle: from CLI startup options and the multi-process
  architecture to the web interface.

- **[Customising Observatories by Subclassing](custom_observatories)**:

  Learn how to create and load `Observatory` subclasses to adapt site-specific
  behaviour — for example custom startup/shutdown sequences — without modifying the
  core source.
