#!/bin/bash

# as root
yum install -y httpd.x86_64 conda gridsite mod_ssl.x86_64 httpd-devel.x86_64 gcc.x86_64 supervisor.noarch
mkdir /opt/idds
mkdir /opt/idds_source
mkdir /var/log/idds
mkdir /var/log/idds/wsgisocks
chown atlpilo1 -R /opt/idds
chown atlpilo1 -R /opt/idds_source
chown atlpilo1 /var/log/idds
chown apache -R /var/log/idds/wsgisocks

git clone @github_idds@ /opt/idds_source
conda env create --prefix=/opt/idds -f main/tools/env/environment.yml
conda activate /opt/idds
pip install rucio-clients-atlas rucio-clients
# root ca.crt to  /opt/idds/etc/ca.crt

# add "auth_type = x509_proxy" to /opt/idds/etc/rucio.cfg

# python setup.py install --old-and-unmanageable
# cp /opt/idds/etc/idds/rest/httpd-idds-443-py36-cc7.conf.install_template /etc/httpd/conf.d/httpd-idds-443-py36-cc7.conf

chown atlpilo1 -R /opt/idds
chown atlpilo1 -R /opt/idds_source

cp /opt/idds/etc/idds/idds.cfg.template /opt/idds/etc/idds/idds.cfg

# comment /etc/httpd/conf.d/ssl.conf "Listen 443 https"
systemctl restart httpd.service
systemctl enable httpd.service


cp /opt/idds/etc/idds/supervisord.d/idds.ini /etc/supervisord.d/idds.ini

systemctl start supervisord
systemctl status supervisord
systemctl enable supervisord
#supervisorctl status
#supervisorctl start all
#supervisorctl stop all

