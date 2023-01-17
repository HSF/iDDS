#!/bin/sh
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2022 - 2023

IDDS_SERVICE=$1

source /etc/profile.d/conda.sh
conda activate /opt/idds;

export IDDS_HOME=/opt/idds

if [ -f /etc/grid-security/hostkey.pem ]; then
    echo "host certificate is already created."
    chmod 600 /etc/grid-security/hostkey.pem
elif [ -f /opt/idds/certs/hostkey.pem ]; then
    echo "mount /opt/idds/certs/hostkey.pem to /etc/grid-security/hostkey.pem"
    ln -fs /opt/idds/certs/hostkey.pem /etc/grid-security/hostkey.pem
    ln -fs /opt/idds/certs/hostcert.pem /etc/grid-security/hostcert.pem
    chmod 600 /etc/grid-security/hostkey.pem
fi

if [ -f /opt/idds/config/idds/idds.cfg ]; then
    echo "idds.cfg already mounted."
else
    echo "idds.cfg not found. will generate one."
    python3 /opt/idds/tools/env/merge_idds_configs.py \
        -s /opt/idds/config_default/idds.cfg $IDDS_OVERRIDE_IDDS_CONFIGS \
        --use-env \
        --prefix IDDS_CFG_IDDS \
        -d /opt/idds/config/idds/idds.cfg
    python3 /opt/idds/tools/env/merge_configmap.py \
        -s /opt/idds/configmap/idds_configmap.json \
        -d /opt/idds/config/idds/idds.cfg
fi

if [ -f /opt/idds/config/idds/auth.cfg ]; then
    echo "auth.cfg already mounted."
else
    echo "auth.cfg not found. will generate one."
    python3 /opt/idds/tools/env/merge_idds_configs.py \
        -s /opt/idds/config_default/auth.cfg $IDDS_OVERRIDE_AUTH_CONFIGS \
        --use-env \
        --prefix IDDS_CFG_AUTH \
        -d /opt/idds/config/idds/auth.cfg
    
    if [ -f /opt/idds/configmap/auth.cfg.json ]; then
        python3 /opt/idds/tools/env/merge_configmap.py \
            -s /opt/idds/configmap/auth.cfg.json \
	    -d /opt/idds/config/idds/auth.cfg
    fi

    python3 /opt/idds/tools/env/merge_configmap.py \
        -s /opt/idds/configmap/idds_configmap.json \
        -d /opt/idds/config/idds/auth.cfg
fi

if [ -f /opt/idds/config/idds/gacl ]; then
    echo "gacl already mounted."
else
    echo "gacl not found. will generate one."
    ln -s /opt/idds/config_default/gacl /opt/idds/config/idds/gacl
fi

if [ -f /opt/idds/config/panda.cfg ]; then
    echo "panda.cfg already mounted."
else
    echo "panda.cfg not found. will generate one."
    python3 /opt/idds/tools/env/merge_idds_configs.py \
        -s /opt/idds/config_default/panda.cfg $IDDS_OVERRIDE_PANDA_CONFIGS \
        --use-env \
        --prefix IDDS_CFG_PANDA \
        -d /opt/idds/config/panda.cfg
    python3 /opt/idds/tools/env/merge_configmap.py \
        -s /opt/idds/configmap/idds_configmap.json \
        -d /opt/idds/config/panda.cfg
fi

if [ -f /opt/idds/config/rucio.cfg ]; then
    echo "rucio.cfg already mounted."
else 
    echo "rucio.cfg not found. will generate one."
    python3 /opt/idds/tools/env/merge_idds_configs.py \
        -s /opt/idds/config_default/rucio.cfg $IDDS_OVERRIDE_RUCIO_CONFIGS \
        --use-env \
        --prefix IDDS_CFG_RUCIO \
        -d /opt/idds/config/rucio.cfg
    python3 /opt/idds/tools/env/merge_configmap.py \
        -s /opt/idds/configmap/idds_configmap.json \
        -d /opt/idds/config/rucio.cfg
fi

# generate oidc token from environment
echo "generate oidc token from environment PANDA_AUTH_ID_TOKEN if it exists."
python3 /opt/idds/tools/env/merge_configmap.py --create_oidc_token

if [ -f /opt/idds/config/idds/httpd-idds-443-py39-cc7.conf ]; then
    echo "httpd conf already mounted."
else
    echo "httpd conf not found. will use the default one."
    cp /opt/idds/config_default/httpd-idds-443-py39-cc7.conf /opt/idds/config/idds/httpd-idds-443-py39-cc7.conf
fi

if [ -f /opt/idds/config/idds/supervisord_idds.ini ]; then
    echo "supervisord conf already mounted."
else
    echo "supervisord conf not found. will use the default one."
    cp /opt/idds/config_default/supervisord_idds.ini /opt/idds/config/idds/supervisord_idds.ini
    cp /opt/idds/config_default/supervisord_iddsfake.ini /opt/idds/config/idds/supervisord_iddsfake.ini
    cp /opt/idds/config_default/supervisord_httpd.ini /opt/idds/config/idds/supervisord_httpd.ini
    cp /opt/idds/config_default/supervisord_syslog-ng.ini /opt/idds/config/idds/supervisord_syslog-ng.ini
fi

if [ -f /etc/grid-security/hostkey.pem ]; then
    echo "Host certificate already mounted."
else
    echo "Host certificate not found. will generate a self-signed one."
    openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
        -subj "/C=US/DC=IDDS/OU=computers/CN=$(hostname -f)" \
        -keyout /opt/idds/config/hostkey.pem \
        -out /opt/idds/config/hostcert.pem
    ln -fs /opt/idds/config/hostcert.pem /etc/grid-security/hostcert.pem
    ln -fs /opt/idds/config/hostkey.pem /etc/grid-security/hostkey.pem
    chmod 600 /etc/grid-security/hostkey.pem
fi

mkdir -p /opt/idds/config/.panda/

if [ ! -z "$IDDS_PRINT_CFG" ]; then
    echo "=================== /opt/idds/etc/idds.cfg ============================"
    cat /opt/idds/etc/idds.cfg
    echo ""
    echo "=================== /opt/idds/etc/idds/auth/auth.cfg ============================"
    cat /opt/idds/etc/idds/auth/auth.cfg
    echo ""
    echo "=================== /opt/idds/etc/idds/rest/gacl ============================"
    cat /opt/idds/etc/idds/rest/gacl
    echo ""
    echo "=================== /etc/httpd/conf.d/httpd-idds-443-py39-cc7.conf ============================"
    cat /etc/httpd/conf.d/httpd-idds-443-py39-cc7.conf
    echo ""
    echo "=================== /opt/idds/config/idds/supervisord_idds.ini ============================"
    cat /opt/idds/config/idds/supervisord_idds.ini
    echo ""
    echo "=================== /opt/idds/etc/panda/panda.cfg ============================"
    cat /opt/idds/etc/panda/panda.cfg
    echo ""
    echo "=================== /opt/idds/etc/rucio.cfg ============================"
    cat /opt/idds/etc/rucio.cfg
    echo ""
fi

# create database if not exists
python /opt/idds/tools/env/create_database.py
python /opt/idds/tools/env/config_monitor.py -s ${IDDS_HOME}/monitor/data/conf.js.template -d ${IDDS_HOME}/monitor/data/conf.js  --host ${IDDS_SERVER}

if ! [ -f /opt/idds/config/.token ]; then
    echo "/opt/idds/config/.token does not exist."
    if [ -f /opt/idds/configmap/idds2panda_token ]; then
        ln -s /opt/idds/configmap/idds2panda_token /opt/idds/config/.token
    fi
fi

# start redis
mkdir /var/log/idds/redis
if [ ! -f /var/log/redis ]; then
    ln -s /var/log/idds/redis /var/log/redis
fi
if [ ! -f /var/lib/redis ]; then
    ln -s /var/log/idds/redis /var/lib/redis
fi
/usr/bin/redis-server /etc/redis.conf --supervised systemd &

if [ "${IDDS_SERVICE}" == "rest" ]; then
  echo "starting iDDS ${IDDS_SERVICE} service"
  # systemctl restart httpd.service
  # systemctl enable httpd.service
  # systemctl status httpd.service
  /usr/sbin/httpd
elif [ "${IDDS_SERVICE}" == "daemon" ]; then
  echo "starting iDDS ${IDDS_SERVICE} service"
  # systemctl enable supervisord
  # systemctl start supervisord
  # systemctl status supervisord
  /usr/bin/supervisord -c /etc/supervisord.conf
elif [ "${IDDS_SERVICE}" == "all" ]; then
  echo "starting iDDS rest service"
  # /usr/sbin/httpd

  echo "starting iDDS daemon service"
  /usr/bin/supervisord -c /etc/supervisord.conf
else
  exec "$@"
fi

# echo "start syslog-ng"
# /usr/sbin/syslog-ng -F --no-caps --persist-file=/var/log/idds/syslog-ng.persist -p /var/log/idds/syslog-ng.pid
tail -f -F /var/log/idds/syslog-ng-stdout.log &
tail -f -F /var/log/idds/syslog-ng-stderr.log &

trap : TERM INT; sleep infinity & wait
