#!/bin/bash

# as root
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

cd /opt/idds_source
#  rm -fr *; cp -r /afs/cern.ch/user/w/wguan/workdisk/iDDS/* .;python setup.py install --old-and-unmanageable
git clone @github_idds@ /opt/idds_source
conda env create --prefix=/opt/idds -f main/tools/env/environment.yml
# source /etc/profile.d/conda.sh
conda activate /opt/idds
pip install rucio-clients-atlas rucio-clients panda-client
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
cp /opt/idds_source/main/etc/idds/supervisord.d/idds.ini /etc/supervisord.d/idds.ini

systemctl start supervisord
systemctl status supervisord
systemctl enable supervisord
#supervisorctl status
#supervisorctl start all
#supervisorctl stop all


#condor
yum install -y condor.x86_64 condor-python.x86_64
#firewall-cmd --zone=public --add-port=9618/tcp --permanent
firewall-cmd --zone=public --add-port=9618/udp --permanent
firewall-cmd --zone=public --add-port=9600-9700/tcp --permanent
firewall-cmd --reload
cp /opt/idds_source/main/etc/condor/submitter/00personal_condor.config /etc/condor/config.d/
systemctl enable condor
systemctl start condor
systemctl status condor


#docker https://docs.docker.com/engine/install/linux-postinstall/
groupadd docker
yum install docker
systemctl start docker
systemctl status docker
systemctl enable docker
usermod -aG docker $(whoami)
usermod -aG docker nobody  # for condor jobs which are running in this account

# shpinx https://sphinx-rtd-tutorial.readthedocs.io/en/latest/sphinx-config.html
#[wguan@lxplus723 docs]$ make html
pip install --upgrade sphinx
pip install --upgrade sphinx-rtd-theme
sphinx-quickstart
make clean
make html
sphinx-apidoc -o ./source/codes/main/ ../main/lib/idds
sphinx-apidoc -o ./source/codes/common/ ../common/lib/idds
sphinx-apidoc -o ./source/codes/client/ ../client/lib/idds
sphinx-apidoc -o ./source/codes/atlas/ ../atlas/lib/idds

