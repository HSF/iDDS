#!/bin/sh

source /etc/profile.d/conda.sh
conda activate /opt/idds;

if [ -f /opt/idds/config/idds/idds.cfg ]; then
    echo "idds.cfg already mounted."
else
    echo "idds.cfg not found. will generate one."
    python3 /opt/idds/tools/env/merge_idds_configs.py \
        -s /opt/idds/config_default/idds.cfg $IDDS_OVERRIDE_IDDS_CONFIGS \
        --use-env \
        --prefix IDDS_CFG_IDDS
        -d /opt/idds/config/idds/idds.cfg
fi

if [ -f /opt/idds/config/idds/auth.cfg ]; then
    echo "auth.cfg already mounted."
else
    echo "auth.cfg not found. will generate one."
    python3 /opt/idds/tools/env/merge_idds_configs.py \
        -s /opt/idds/config_default/auth.cfg $IDDS_OVERRIDE_AUTH_CONFIGS \
        --use-env \
        --prefix IDDS_CFG_AUTH
        -d /opt/idds/config/idds/auth.cfg
fi

if [ -f /opt/idds/config/idds/gacl ]; then
    echo "gacl already mounted."
else
    echo "gacl not found. will generate one."
    python3 /opt/idds/tools/env/merge_idds_configs.py \
        -s /opt/idds/config_default/gacl $IDDS_OVERRIDE_GACL_CONFIGS \
        --use-env \
        --env-string IDDS_CFG_GACL
        --replace-whole-file
        -d /opt/idds/config/idds/gacl
fi

if [ -f /opt/idds/config/panda.cfg ]; then
    echo "panda.cfg already mounted."
else
    echo "panda.cfg not found. will generate one."
    python3 /opt/idds/tools/env/merge_idds_configs.py \
        -s /opt/idds/config_default/panda.cfg $IDDS_OVERRIDE_PANDA_CONFIGS \
        --use-env \
        --prefix IDDS_CFG_PANDA
        -d /opt/idds/config/panda.cfg
fi

if [ -f /opt/idds/config/rucio.cfg ]; then
    echo "rucio.cfg already mounted."
else 
    echo "rucio.cfg not found. will generate one."
    python3 /opt/idds/tools/env/merge_idds_configs.py \
        -s /opt/idds/config_default/rucio.cfg $IDDS_OVERRIDE_RUCIO_CONFIGS \
        --use-env \
        --prefix IDDS_CFG_RUCIO
        -d /opt/idds/config/rucio.cfg
fi

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
    echo "=================== /opt/idds/etc/panda/panda.cfg ============================"
    cat /opt/idds/etc/panda/panda.cfg
    echo ""
    echo "=================== /opt/idds/etc/rucio.cfg ============================"
    cat /opt/idds/etc/rucio.cfg
    echo ""
fi

if [ "${IDDS_SERVICE}" == "rest" ]; then
  echo "starting iDDS ${IDDS_SERVICE} service"
  systemctl restart httpd.service
  systemctl enable httpd.service
  systemctl status httpd.service
elif [ "${IDDS_SERVICE}" == "daemon" ];then
  echo "starting iDDS ${IDDS_SERVICE} service"
  systemctl enable supervisord
  systemctl start supervisord
  systemctl status supervisord
else
  echo "starting iDDS rest service"
  systemctl restart httpd.service
  systemctl enable httpd.service
  systemctl status httpd.service

  echo "starting iDDS daemon service"
  systemctl enable supervisord
  systemctl start supervisord
  systemctl status supervisord
fi

trap : TERM INT; sleep infinity & wait
