#!/bin/bash

# as root
yum install -y httpd.x86_64 conda gridsite mod_ssl.x86_64 httpd-devel.x86_64 gcc.x86_64 supervisor.noarch
# yum install -y gfal2-plugin-gridftp gfal2-plugin-file.x86_64  gfal2-plugin-http.x86_64   gfal2-plugin-xrootd.x86_64  gfal2-python.x86_64 gfal2-python3.x86_64 gfal2-all.x86_64
# conda install -c conda-forge python-gfal2
# pip install requests SQLAlchemy urllib3 retrying mod_wsgi flask futures stomp.py cx-Oracle  unittest2 pep8 flake8 pytest nose sphinx recommonmark sphinx-rtd-theme nevergrad

mkdir /opt/idds
mkdir /opt/idds_source
mkdir /opt/idds
mkdir /var/log/idds
mkdir /var/log/idds/wsgisocks
mkdir /tmp/idds/wsgisocks
chown atlpilo1 -R /opt/idds
chown atlpilo1 -R /opt/idds_source
chown atlpilo1 /var/log/idds
chown apache -R /var/log/idds/wsgisocks
chown apache -R /tmp/idds/wsgisocks

cd /opt/idds_source
#  rm -fr *; cp -r /afs/cern.ch/user/w/wguan/workdisk/iDDS/* .;python setup.py install --old-and-unmanageable
git clone @github_idds@ /opt/idds_source
conda env create --prefix=/opt/idds -f main/tools/env/environment.yml
# source /etc/profile.d/conda.sh
conda activate /opt/idds
conda install -c conda-forge python-gfal2

pip install rucio-clients-atlas rucio-clients panda-client-light panda-client
# root ca.crt to  /opt/idds/etc/ca.crt

pip install requests SQLAlchemy urllib3 retrying mod_wsgi flask futures stomp.py cx-Oracle  unittest2 pep8 flake8 pytest nose sphinx recommonmark sphinx-rtd-theme nevergrad
 pip install psycopg2-binary

# add "auth_type = x509_proxy" to /opt/idds/etc/rucio.cfg

# python setup.py install --old-and-unmanageable
# cp /opt/idds/etc/idds/rest/httpd-idds-443-py36-cc7.conf.install_template /etc/httpd/conf.d/httpd-idds-443-py36-cc7.conf

# scp wguan@aipanda102:/opt/idds/etc/rucio.cfg /opt/idds/etc/rucio.cfg
# scp wguan@aipanda102:/etc/httpd/conf.d/httpd-idds-443-py310-al.conf /etc/httpd/conf.d/httpd-idds-443-py310-al.conf
# scp wguan@aipanda102:/etc/httpd/conf.d/httpd-idds-443-py39-cc7.conf /etc/httpd/conf.d/httpd-idds-443-py39-cc7.conf
# mv /etc/httpd/conf.d/httpd-idds-443-py310-al.conf /etc/httpd/conf.d/httpd-idds-443-py310-al.conf.bac
# scp wguan@aipanda102:/etc/supervisord.d/idds.ini /etc/supervisord.d/idds.ini
# scp wguan@aipanda102:/opt/idds/etc/idds/idds.cfg /opt/idds/etc/idds/idds.cfg
# scp wguan@aipanda102:/opt/idds/etc/idds/rest/gacl /opt/idds/etc/idds/rest/gacl
# scp wguan@aipanda102:/opt/idds/etc/idds/auth/auth.cfg /opt/idds/etc/idds/auth/auth.cfg

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
yum install https://research.cs.wisc.edu/htcondor/repo/current/htcondor-release-current.el9.noarch.rpm
# cp /etc/yum.repos.d/htcondor* /etc/yum-puppet.repos.d/
# yum install -y condor.x86_64 condor-python.x86_64
yum install -y condor.x86_64 python3-condor.x86_64
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
sphinx-apidoc -f -o ./source/codes/main/ ../main/lib/idds
sphinx-apidoc -f -o ./source/codes/common/ ../common/lib/idds
sphinx-apidoc -f -o ./source/codes/client/ ../client/lib/idds
sphinx-apidoc -f -o ./source/codes/workflow/ ../workflow/lib/idds
sphinx-apidoc -f -o ./source/codes/atlas/ ../atlas/lib/idds
sphinx-apidoc -f -o ./source/codes/doma/ ../doma/lib/idds


yum install fetch-crl.noarch
yum install lcg-CA


yum install redis
systemctl start redis
systemctl enable redis
