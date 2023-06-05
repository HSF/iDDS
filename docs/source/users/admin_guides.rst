Administrator guides
=============================

Here is a quick tutorial for setup an iDDS server.

Environment setup on CENTOS 7
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. setup environment for the first time.

.. code-block:: text

    yum install -y httpd.x86_64 conda gridsite mod_ssl.x86_64 httpd-devel.x86_64 gcc.x86_64 supervisor.noarch
    mkdir /opt/idds
    mkdir /opt/idds_source
    mkdir /opt/idds
    mkdir /var/log/idds
    mkdir /var/log/idds/wsgisocks
    chown atlpilo1 -R /opt/idds
    chown atlpilo1 -R /opt/idds_source
    chown atlpilo1 /var/log/idds
    chown apache -R /var/log/idds/wsgisocks

    source /etc/profile.d/conda.sh
    conda create --prefix=/opt/idds python=3.6.2
    conda activate /opt/idds
    pip install idds-server idds-doma idds-atlas idds-monitor idds-website

    pip install rucio-clients-atlas rucio-clients panda-client-light
    # # add "auth_type = x509_proxy" to /opt/idds/etc/rucio.cfg

2. setup environment after installed.

.. code-block:: text

    source /etc/profile.d/conda.sh
    conda activate /opt/idds

    pip install --upgrade idds-server idds-doma idds-atlas idds-monitor idds-website


3. Configure REST service

The Rest service is based http server. By default it's using the port 443. If the port 443 is used, you need to comment out the 443 port in ssl.conf.

.. code-block:: text

    # configure httpd service
    cp /opt/idds/etc/idds/rest/httpd-idds-443-py36-cc7.conf.install_template /etc/httpd/conf.d/httpd-idds-443-py36-cc7.conf
    # comment /etc/httpd/conf.d/ssl.conf "Listen 443 https"
    systemctl restart httpd.service
    systemctl enable httpd.service

4. Configure iDDS agents

"supervisord" is employed to manage iDDS agents. Here is the configuration.

.. code-block:: text

    # configure iDDS agents
    cp /opt/idds/etc/idds/supervisord.d/idds.ini /etc/supervisord.d/idds.ini
    cp /opt/idds_source/main/etc/idds/supervisord.d/idds.ini /etc/supervisord.d/idds.ini
    systemctl start supervisord
    systemctl status supervisord
    systemctl enable supervisord

Normally, these agents are required for idds.
a) Clerk, to manage requests and workflow.
b) Transformer, to manage transforms, collections, contents and create processings.
c) Carrier, to submit processings to workload manager and poll processings.
d) Conductor, to send messages to ActiveMQ for workload managers to consume.

.. code-block:: text

    cp /opt/idds/etc/idds/idds.cfg.template /opt/idds/etc/idds/idds.cfg
    # configure the database

5. Logs locations (httpd REST logs and idds agents logs)

.. code-block:: text

    ls /var/log/idds
    ls /var/log/idds/httpd_error_log
    ls /var/log/idds/idds-server-std*
    # Normally grep 'Traceback' can find the errors.

6. Restart service

.. code-block:: text

    systemctl stop httpd
    systemctl start httpd

    # restart agents
    supervisorctl stop all
    supervisorctl start all
