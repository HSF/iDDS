iDDS OIDC authorization
=============================

Here are the commands how to setup oidc tokens. For other client examples, please check normal user documents.

iDDS OIDC authorization
~~~~~~~~~~~~~~~~~~~~~~~~

1. Setup the client. It's for users to setup the client for the first time or update the client configurations.
By default it will create a file in ~/.idds/idds_local.cfg to remember these configurations.

.. code-block:: python

    from idds.client.clientmanager import ClientManager
    cm = ClientManager()
    cm.setup_local_configuration(local_config_root=<local_config_root>,  # default ~/.idds/
                                 host=<host>,                            # default host for different authorization methods. https://<hostname or ip>:443/idds
                                 auth_type=<auth_type>,                  # authorization type: x509_proxy, oidc
                                 auth_type_host=<auth_type_host>,        # for different authorization methods, users can define different idds servers.
                                 x509_proxy=<x509_proxy path>,
                                 vo=<vo name>,

2. setup oidc token

.. code-block:: python

    from idds.client.clientmanager import ClientManager
    cm = ClientManager()
    cm.setup_oidc_token()

3. refresh oidc token

.. code-block:: python

    from idds.client.clientmanager import ClientManager
    cm = ClientManager()
    cm.refresh_oidc_token()

4. get token info

.. code-block:: python

    from idds.client.clientmanager import ClientManager
    cm = ClientManager()
    cm.check_oidc_token_status()

5. clean oidc token

.. code-block:: python

    from idds.client.clientmanager import ClientManager
    cm = ClientManager()
    cm.clean_oidc_token()


iDDS OIDC Command Line Interface (CLI)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. Setup the client. It's for users to setup the client for the first time or update the client configurations.
By default it will create a file in ~/.idds/idds_local.cfg to remember these configurations.

.. code-block:: python

   idds setup --auth_type oidc --host https://<hostname or ip>:443/idds --vo Rubin

2. setup oidc token

.. code-block:: python

    idds setup_oidc_token

3. refresh oidc token

.. code-block:: python

    idds refresh_oidc_token

4. get token info

.. code-block:: python

    idds get_oidc_token_info

5. clean oidc token

.. code-block:: python

    idds clean_oidc_token
