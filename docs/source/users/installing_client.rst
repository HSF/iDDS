Installing iDDS Clients
=======================

Prerequisites
~~~~~~~~~~~~~~

iDDS clients run on Python 2.7, 3.6 on any Unix-like platform.


Python Dependencies
~~~~~~~~~~~~~~~~~~~~

All Dependencies are automatically installed with pip.

Install via pip
~~~~~~~~~~~~~~~

When ``pip`` is available, the distribution can be downloaded from the iDDS PyPI server and installed in one step::

   $> pip install idds-common idds-client

This command will download the latest version of Rucio and install it to your system.


Upgrade via pip
~~~~~~~~~~~~~~~~

To upgrade via pip::

   $> pip install --upgrade idds-common idds-client


config client
~~~~~~~~~~~~~

To use iDDS client to access the iDDS server, a config file is needed. Below is an example of the config file.

.. code-block:: python

    [common]
    loglevel = INFO

    [rest]
    host = https://<hostname>:<port>/idds

iDDS will look for this config file in order of:

.. code-block:: python

    ${IDDS_CONFIG}
    ${IDDS_HOME}/etc/idds/idds.cfg
    /etc/idds/idds.cfg
    ${VIRTUAL_ENV}/etc/idds/idds.cfg
