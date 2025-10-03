#!/bin/bash

# min number of workers
if [[ -z "${IDDS_SERVER_CONF_MIN_WORKERS}" ]]; then
  export IDDS_SERVER_CONF_MIN_WORKERS=32
fi

# max number of workers
if [[ -z "${IDDS_SERVER_CONF_MAX_WORKERS}" ]]; then
  export IDDS_SERVER_CONF_MAX_WORKERS=512
fi

# max number of WSGI daemons
if [[ -z "${IDDS_SERVER_CONF_NUM_WSGI}" ]]; then
  export IDDS_SERVER_CONF_NUM_WSGI=32
fi

# max number of WSGI daemons
if [[ -z "${IDDS_SERVER_CONF_MAX_BACKLOG}" ]]; then
  export IDDS_SERVER_CONF_MAX_BACKLOG=511
fi

# max number of WSGI threads
if [[ -z "${IDDS_SERVER_CONF_NUM_WSGI_THREAD}" ]]; then
  export IDDS_SERVER_CONF_NUM_WSGI_THREAD=32
fi

/usr/sbin/httpd -DFOREGROUND
