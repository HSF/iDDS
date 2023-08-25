#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2022


FROM docker.io/centos:7

ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8

ARG TAG

WORKDIR /tmp

RUN yum install -y epel-release.noarch && \
     yum clean all && \
     rm -rf /var/cache/yum
RUN yum upgrade -y && \
    yum clean all && \
    rm -rf /var/cache/yum

# RUN yum install -y httpd.x86_64 conda gridsite mod_ssl.x86_64 httpd-devel.x86_64 gcc.x86_64 supervisor.noarch fetch-crl.noarch lcg-CA postgresql postgresql-contrib postgresql-static postgresql-libs postgresql-devel && \
#     yum clean all && \
#     rm -rf /var/cache/yum
RUN yum install -y httpd.x86_64 which conda gridsite mod_ssl.x86_64 httpd-devel.x86_64 gcc.x86_64 supervisor.noarch fetch-crl.noarch lcg-CA redis syslog-ng && \
yum clean all && \
rm -rf /var/cache/yum

RUN yum install -y https://download.postgresql.org/pub/repos/yum/reporpms/EL-7-x86_64/pgdg-redhat-repo-latest.noarch.rpm
RUN yum install -y postgresql14

# RUN curl http://repository.egi.eu/sw/production/cas/1/current/repo-files/EGI-trustanchors.repo -o /etc/yum.repos.d/EGI-trustanchors.repo/
RUN curl https://repository.egi.eu/sw/production/cas/1/current/repo-files/egi-trustanchors.repo -o /etc/yum.repos.d/EGI-trustanchors.repo

RUN yum install -y fetch-crl.noarch lcg-CA ca-policy-egi-core && \
    yum clean all && \
    rm -rf /var/cache/yum

# setup env
RUN adduser atlpan
RUN groupadd zp
RUN usermod -a -G zp atlpan

RUN mkdir /opt/idds
RUN mkdir /var/log/idds
RUN mkdir /var/log/idds/wsgisocks/
RUN mkdir /var/idds
RUN mkdir /var/idds/wsgisocks
RUN chown atlpan -R /opt/idds
# RUN chown atlpan -R /opt/idds_source
RUN chown atlpan -R /var/log/idds
RUN chown apache -R /var/idds/wsgisocks/

# redis
RUN chmod a+rx /etc/redis*
# RUN chmod a+rwx /var/log/redis
# RUN chmod a+rwx /var/lib/redis
RUN rm -fr /var/log/redis
RUN rm -fr /var/lib/redis
RUN mkdir /var/log/idds/redis
RUN chmod a+rwx /var/log/idds/redis
RUN ln -s /var/log/idds/redis /var/log/redis
RUN ln -s /var/log/idds/redis /var/lib/redis

# setup conda virtual env
ADD requirements.yaml /opt/idds/
# ADD start-daemon.sh /opt/idds/
RUN conda env create --prefix=/opt/idds -f /opt/idds/requirements.yaml
RUN source /etc/profile.d/conda.sh; conda activate /opt/idds
# RUN conda activate /opt/idds

# Make RUN commands use the new environment:
# SHELL ["conda", "run", "-p", "/opt/idds", "/bin/bash", "-c"]

# install required packages
RUN source /etc/profile.d/conda.sh; conda activate /opt/idds; python3 -m pip install --no-cache-dir --upgrade pip
RUN source /etc/profile.d/conda.sh; conda activate /opt/idds; python3 -m pip install --no-cache-dir --upgrade setuptools

RUN source /etc/profile.d/conda.sh; conda activate /opt/idds; python3 -m pip install --no-cache-dir --upgrade requests SQLAlchemy urllib3 retrying mod_wsgi flask futures stomp.py cx-Oracle  unittest2 pep8 flake8 pytest nose sphinx recommonmark sphinx-rtd-theme nevergrad
RUN source /etc/profile.d/conda.sh; conda activate /opt/idds; python3 -m pip install --no-cache-dir --upgrade psycopg2-binary
RUN source /etc/profile.d/conda.sh; conda activate /opt/idds; python3 -m pip install --no-cache-dir --upgrade rucio-clients-atlas rucio-clients panda-client-light


WORKDIR /tmp/src
COPY . .

RUN source /etc/profile.d/conda.sh; conda activate /opt/idds; \
  if [[ -z "$TAG" ]] ; then \
  python3 setup.py sdist bdist_wheel && main/tools/env/install_packages.sh ; \
  else \
  python3 -m pip install --no-cache-dir --upgrade idds-common==$TAG idds-workflow==$TAG idds-server==$TAG idds-client==$TAG idds-doma==$TAG idds-atlas==$TAG idds-website==$TAG idds-monitor==$TAG ; \
  fi

WORKDIR /tmp
RUN rm -rf /tmp/src

RUN chmod 777 /opt/idds/monitor/data
RUN chmod 777 /opt/idds/monitor/data/conf.js
RUN mkdir /opt/idds/config
RUN mkdir /opt/idds/config/idds
# RUN mkdir /opt/idds/config_default

# ADD idds.cfg.default /opt/idds/config

# RUN ls /opt/idds/etc; ls /opt/idds/etc/idds; ls /opt/idds/etc/panda;
# RUN ls /opt/idds/config; ls /opt/idds/config/idds;

# for rest service
# RUN ln -fs /opt/idds/config/hostkey.pem /etc/grid-security/hostkey.pem
# RUN ln -fs /opt/idds/config/hostcert.pem /etc/grid-security/hostcert.pem

# to authenticate to rucio
RUN ln -fs /opt/idds/config/ca.crt /opt/idds/etc/ca.crt
RUN ln -fs /opt/idds/config/rucio.cfg /opt/idds/etc/rucio.cfg

# for panda client to access panda
RuN mkdir -p /opt/idds/etc/panda/
RUN ln -fs /opt/idds/config/panda.cfg /opt/idds/etc/panda/panda.cfg

# for idds rest service
RUN ln -fs /opt/idds/config/idds/idds.cfg /opt/idds/etc/idds/idds.cfg
RUN ln -fs /opt/idds/config/idds/auth.cfg /opt/idds/etc/idds/auth/auth.cfg
RUN ln -fs /opt/idds/config/idds/gacl /opt/idds/etc/idds/rest/gacl
RUN ln -fs /opt/idds/config/idds/httpd-idds-443-py39-cc7.conf /etc/httpd/conf.d/httpd-idds-443-py39-cc7.conf

# update http config
RUN sed -i 's/Listen\ 443/#\ Listen\ 443/g' /etc/httpd/conf.d/ssl.conf
RUN sed -i 's/Listen\ 80/#\ Listen\ 80/g' /etc/httpd/conf/httpd.conf
RUN sed -i "s/WSGISocketPrefix\ \/var\/log\/idds\/wsgisocks\/wsgi/WSGISocketPrefix\ \/var\/idds\/wsgisocks\/wsgi/g" /opt/idds/config_default/httpd-idds-443-py39-cc7.conf

# for idds daemons
RUN ln -fs /opt/idds/config/idds/supervisord_idds.ini /etc/supervisord.d/idds.ini
# RUN ln -fs /opt/idds/config/idds/supervisord_iddsfake.ini /etc/supervisord.d/iddsfake.ini
RUN ln -fs /opt/idds/config/idds/supervisord_httpd.ini /etc/supervisord.d/httpd.ini
# RUN ln -fs /opt/idds/config/idds/supervisord_syslog-ng.ini /etc/supervisord.d/syslog-ng.ini
RUN ln -fs /opt/idds/config/idds/supervisord_logrotate.ini /etc/supervisord.d/logrotate.ini
RUN ln -fs /opt/idds/config/idds/logrotate_idds /etc/logrotate.d/idds

# for syslog-ng
RUN mv /etc/syslog-ng/syslog-ng.conf /etc/syslog-ng/syslog-ng.conf.back
ADD main/tools/syslog-ng/syslog-ng.conf /etc/syslog-ng/
ADD main/tools/syslog-ng/idds.conf /etc/syslog-ng/conf.d/
ADD main/tools/syslog-ng/http.conf /etc/syslog-ng/conf.d/

RUN chown atlpan -R /etc/grid-security/certificates

RUN chmod -R 777 /opt/idds/config
RUN chmod -R 777 /var/log/idds
RUN chmod -R 777 /var/idds
RUN chmod 777 /etc/grid-security
RUN chmod 777 /etc/httpd/conf.d
RUN chmod 777 /etc/httpd/conf/httpd.conf
RUN chmod 777 /etc/httpd/conf
RUN chmod 777 /run/httpd
RUN chmod 777 /var/log/supervisor/
RUN chmod 777 /var/run/supervisor
RUN chmod 777 /var/run
RUN chmod 777 /etc/httpd/logs

ENV PATH /opt/idds/bin/:$PATH

ADD start-daemon.sh /opt/idds/bin/
RUN mv /etc/httpd/conf.d/ssl.conf /etc/httpd/conf.d/ssl.conf.back
# ADD ssl.conf /etc/httpd/conf.d/ssl.conf
RUN ln -s /opt/idds/etc/idds/rest/ssl.conf /etc/httpd/conf.d/ssl.conf

VOLUME /var/log/idds
VOLUME /opt/idds/config

ENTRYPOINT ["start-daemon.sh"]

STOPSIGNAL SIGINT

EXPOSE 443
CMD ["all"]
