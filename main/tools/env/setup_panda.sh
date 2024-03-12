#!/bin/bash

instance=doma
if [ "$#" -eq 1 ]; then
  instance=$1
fi

export X509_USER_PROXY=/afs/cern.ch/user/w/wguan/workdisk/iDDS/test/x509up
export RUCIO_ACCOUNT=pilot

#export IDDS_HOST=https://panda-idds-dev.cern.ch:443/idds

export PANDA_BEHIND_REAL_LB=true
#  export PANDA_SYS=/opt/idds/

if [ "$instance" == "k8s" ]; then
    export IDDS_HOST=https://panda-idds-dev.cern.ch:443/idds
    export PANDA_AUTH=oidc
    export PANDA_BEHIND_REAL_LB=true
    export PANDA_VERIFY_HOST=off
    export PANDA_URL_SSL=https://panda-server-dev.cern.ch:443/server/panda
    export PANDA_URL=http://panda-server-dev.cern.ch:80/server/panda
    export PANDAMON_URL=https://panda-server-dev.cern.ch
    export PANDA_AUTH_VO=panda_dev

    export PANDACACHE_URL=$PANDA_URL_SSL
    export PANDA_SYS=/afs/cern.ch/user/w/wguan/workdisk/iDDS/.conda/iDDS/
    # export PANDA_CONFIG_ROOT=/afs/cern.ch/user/w/wguan/workdisk/iDDS/main/etc/panda/
    export PANDA_CONFIG_ROOT=~/.panda/
elif [ "$instance" == "slac" ]; then
    export PANDA_AUTH=oidc
    export PANDA_BEHIND_REAL_LB=true
    export PANDA_VERIFY_HOST=off
    export PANDA_URL_SSL=https://rubin-panda-server-dev.slac.stanford.edu:8443/server/panda
    export PANDA_URL=http://rubin-panda-server-dev.slac.stanford.edu:80/server/panda
    export PANDAMON_URL=https://rubin-panda-bigmon-dev.slac.stanford.edu
    export PANDA_AUTH_VO=Rubin

    export PANDACACHE_URL=$PANDA_URL_SSL
    export PANDA_SYS=/afs/cern.ch/user/w/wguan/workdisk/iDDS/.conda/iDDS/

    # export PANDA_CONFIG_ROOT=/afs/cern.ch/user/w/wguan/workdisk/iDDS/main/etc/panda/
    export PANDA_CONFIG_ROOT=~/.panda/
elif [ "$instance" == "usdf" ]; then
    export PANDA_AUTH=oidc
    export PANDA_BEHIND_REAL_LB=true
    export PANDA_VERIFY_HOST=off
    export PANDA_URL_SSL=https://usdf-panda-server.slac.stanford.edu:8443/server/panda
    export PANDA_URL=https://usdf-panda-server.slac.stanford.edu:8443/server/panda
    export PANDACACHE_URL=$PANDA_URL_SSL
    export PANDAMON_URL=https://usdf-panda-bigmon.slac.stanford.edu:8443/
    export PANDA_AUTH_VO=Rubin:production

    export PANDACACHE_URL=$PANDA_URL_SSL
    export PANDA_SYS=/afs/cern.ch/user/w/wguan/workdisk/iDDS/.conda/iDDS/

    # export PANDA_CONFIG_ROOT=/afs/cern.ch/user/w/wguan/workdisk/iDDS/main/etc/panda/
    export PANDA_CONFIG_ROOT=~/.panda/
else
    export PANDA_AUTH=oidc
    export PANDA_URL_SSL=https://pandaserver-doma.cern.ch:25443/server/panda
    export PANDA_URL=http://pandaserver-doma.cern.ch:25080/server/panda
    export PANDAMON_URL=https://panda-doma.cern.ch
    # export PANDA_AUTH_VO=panda_dev
    export PANDA_AUTH_VO=Rubin:production

    export PANDACACHE_URL=$PANDA_URL_SSL

    export PANDA_SYS=/afs/cern.ch/user/w/wguan/workdisk/iDDS/.conda/iDDS/
    # export PANDA_CONFIG_ROOT=/afs/cern.ch/user/w/wguan/workdisk/iDDS/main/etc/panda/
    export PANDA_CONFIG_ROOT=~/.panda/

    # export IDDS_HOST=https://aipanda015.cern.ch:443/idds

    # dev
    # export IDDS_HOST=https://aipanda104.cern.ch:443/idds

    # doma
    export IDDS_HOST=https://aipanda105.cern.ch:443/idds

    export IDDS_BROKERS=atlas-test-mb.cern.ch:61013
    export IDDS_BROKER_DESTINATION=/topic/doma.idds
    export IDDS_BROKER_USERNAME=domaidds
    export IDDS_BROKER_PASSWORD=1d25yeft6krJ1HFH
    export IDDS_BROKER_TIMEOUT=360

    PANDA_QUEUE=BNL_OSG_2
    PANDA_WORKING_GROUP=EIC
    PANDA_VO=wlcg
fi
