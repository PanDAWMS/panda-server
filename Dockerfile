FROM docker.io/almalinux:9

ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8

RUN yum update -y
RUN yum install -y epel-release
RUN yum install -y python3 python3-devel httpd httpd-devel gcc gridsite less git psmisc wget logrotate
RUN yum install -y https://download.postgresql.org/pub/repos/yum/reporpms/EL-9-x86_64/pgdg-redhat-repo-latest.noarch.rpm
# temp until offifical PGP key is fixed
RUN sed -i 's/repo_gpgcheck = 1/repo_gpgcheck = 0/g' /etc/yum.repos.d/pgdg-redhat-all.repo
RUN yum install -y postgresql15
RUN yum clean all && rm -rf /var/cache/yum

RUN python3 -m venv /opt/panda
RUN /opt/panda/bin/pip install --no-cache-dir -U pip
RUN /opt/panda/bin/pip install --no-cache-dir -U setuptools
RUN adduser atlpan
RUN groupadd zp
RUN usermod -a -G zp atlpan
RUN mkdir /tmp/src
WORKDIR /tmp/src
COPY . .
RUN /opt/panda/bin/python setup.py sdist; /opt/panda/bin/pip install --no-cache-dir `ls dist/p*.tar.gz`[postgres]
RUN /opt/panda/bin/pip install --no-cache-dir rucio-clients
RUN /opt/panda/bin/pip install --no-cache-dir "git+https://github.com/PanDAWMS/panda-cacheschedconfig.git"
RUN ln -s /opt/panda/lib/python*/site-packages/mod_wsgi/server/mod_wsgi*.so /etc/httpd/modules/mod_wsgi.so
WORKDIR /
RUN rm -rf /tmp/src

RUN mkdir -p /etc/panda
RUN mkdir -p /etc/idds
RUN mv /opt/panda/etc/panda/panda_common.cfg.rpmnew /etc/panda/panda_common.cfg
RUN mv /opt/panda/etc/panda/panda_server.cfg.rpmnew /etc/panda/panda_server.cfg
RUN mv /opt/panda/etc/panda/panda_server.sysconfig.rpmnew /etc/sysconfig/panda_server
RUN mv /opt/panda/etc/panda/panda_server-httpd-FastCGI.conf.rpmnew /opt/panda/etc/panda/panda_server-httpd.conf

# make a wrapper script to launch services and periodic jobs in non-root container
RUN echo $'#!/bin/bash \n\
set -m \n\
/data/panda/init-panda \n\
/data/panda/run-panda-crons & \n\
/etc/rc.d/init.d/httpd-pandasrv start \n ' > /etc/rc.d/init.d/run-panda-services

RUN chmod +x /etc/rc.d/init.d/run-panda-services

RUN mkdir -p /data/panda
RUN mkdir -p /data/atlpan
RUN mkdir -p /var/log/panda/wsgisocks
RUN mkdir -p /var/log/panda/pandacache
RUN mkdir -p /run/httpd/wsgisocks
RUN mkdir -p /var/cache/pandaserver/jedilog
RUN mkdir -p /var/run/panda
RUN mkdir -p /var/cric

RUN ln -fs /opt/panda/etc/cert/hostkey.pem /etc/grid-security/hostkey.pem
RUN ln -fs /opt/panda/etc/cert/hostcert.pem /etc/grid-security/hostcert.pem
RUN ln -fs /opt/panda/etc/cert/chain.pem /etc/grid-security/chain.pem
RUN ln -s /opt/panda/etc/rc.d/init.d/panda_server /etc/rc.d/init.d/httpd-pandasrv
RUN ln -fs /data/panda/idds.cfg /opt/panda/etc/idds/idds.cfg
RUN ln -fs /data/panda/rucio.cfg /opt/panda/etc/rucio.cfg
RUN ln -fs /data/panda/panda_mbproxy_config.json /opt/panda/etc/panda/panda_mbproxy_config.json
RUN ln -s /etc/sysconfig/panda_server /opt/panda/etc/panda/panda_server.sysconfig

RUN chown -R atlpan:zp /var/log/panda

# to run with non-root PID
RUN mkdir -p /etc/grid-security/certificates
RUN chmod -R 777 /etc/grid-security/certificates
RUN chmod -R 777 /data/panda
RUN chmod -R 777 /data/atlpan
RUN chmod -R 777 /var/log/panda
RUN chmod -R 777 /run/httpd
RUN chmod -R 777 /home/atlpan
RUN chmod -R 777 /var/lock
RUN chmod -R 777 /var/cache/pandaserver
RUN chmod -R 777 /var/log/panda/pandacache
RUN chmod -R 777 /var/run/panda
RUN chmod -R 777 /var/lib/logrotate
RUN chmod -R 777 /var/cric

ENV PANDA_LOCK_DIR /var/run/panda
RUN mkdir -p ${PANDA_LOCK_DIR} && chmod 777 ${PANDA_LOCK_DIR}

CMD exec /bin/bash -c "trap : TERM INT; sleep infinity & wait"

EXPOSE 25080 25443
