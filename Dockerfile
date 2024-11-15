ARG PYTHON_VERSION=3.11.6

FROM docker.io/almalinux:9

ARG PYTHON_VERSION

RUN yum update -y
RUN yum install -y epel-release

RUN yum install -y httpd httpd-devel gcc gridsite git psmisc less wget logrotate procps which \
    openssl-devel readline-devel bzip2-devel libffi-devel zlib-devel systemd-udev

# install python
RUN mkdir /tmp/python && cd /tmp/python && \
    wget https://www.python.org/ftp/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tgz && \
    tar -xzf Python-*.tgz && rm -f Python-*.tgz && \
    cd Python-* && \
    ./configure --enable-shared --enable-optimizations --with-lto && \
    make altinstall && \
    echo /usr/local/lib > /etc/ld.so.conf.d/local.conf && ldconfig && \
    cd / && rm -rf /tmp/pyton

# install postgres
RUN yum install -y https://download.postgresql.org/pub/repos/yum/reporpms/EL-9-x86_64/pgdg-redhat-repo-latest.noarch.rpm
RUN yum install --nogpgcheck -y postgresql16
RUN  yum clean all && rm -rf /var/cache/yum

# update network limitations
# RUN echo 4096 > /proc/sys/net/core/somaxconn
# RUN sysctl -w net.core.somaxconn=4096
RUN echo 'net.core.somaxconn=4096' >> /etc/sysctl.d/999-net.somax.conf

# setup venv with pythonX.Y
RUN python$(echo ${PYTHON_VERSION} | sed -E 's/\.[0-9]+$//') -m venv /opt/panda
RUN /opt/panda/bin/pip install --no-cache-dir -U pip
RUN /opt/panda/bin/pip install --no-cache-dir -U setuptools
RUN /opt/panda/bin/pip install --no-cache-dir -U gnureadline
RUN adduser atlpan
RUN groupadd zp
RUN usermod -a -G zp atlpan
RUN mkdir /tmp/src
WORKDIR /tmp/src
COPY . .

# install panda-common first to prevent panda-client from installing redundant files
RUN /opt/panda/bin/pip install --no-cache-dir panda-common
RUN /opt/panda/bin/pip install --no-cache-dir .[postgres,oracle]
RUN /opt/panda/bin/pip install --no-cache-dir rucio-clients
RUN /opt/panda/bin/pip install --no-cache-dir "git+https://github.com/PanDAWMS/panda-cacheschedconfig.git"
RUN ln -s /opt/panda/lib/python*/site-packages/mod_wsgi/server/mod_wsgi*.so /etc/httpd/modules/mod_wsgi.so

# install Oracle Instant Client and tnsnames.ora
RUN wget https://download.oracle.com/otn_software/linux/instantclient/oracle-instantclient-basic-linuxx64.rpm -P /tmp/ && \
    yum install /tmp/oracle-instantclient-basic-linuxx64.rpm -y && \
    wget https://download.oracle.com/otn_software/linux/instantclient/oracle-instantclient-sqlplus-linuxx64.rpm -P /tmp/ && \
    yum install /tmp/oracle-instantclient-sqlplus-linuxx64.rpm -y

# Grab the latest version of the Oracle tnsnames.ora file
RUN ln -fs /data/panda/tnsnames.ora /etc/tnsnames.ora

WORKDIR /
RUN rm -rf /tmp/src

RUN mkdir -p /etc/panda
RUN mkdir -p /etc/idds
RUN mkdir -p /etc/rc.d/init.d
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
RUN mkdir -p /var/log/panda/pandacache/jedilog
RUN mkdir -p /var/cache/pandaserver/schedconfig
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
RUN chmod -R 777 /var/log/panda/pandacache
RUN chmod -R 777 /var/run/panda
RUN chmod -R 777 /var/lib/logrotate
RUN chmod -R 777 /var/cric
RUN chmod -R 777 /var/cache/pandaserver

# to have trf files under /var/trf/user
RUN mkdir -p /var/trf/user

RUN mkdir /tmp/panda-wnscript && cd /tmp/panda-wnscript && \
    git clone https://github.com/PanDAWMS/panda-wnscript.git && \
    cp -R panda-wnscript/dist/* /var/trf/user/ && \
    cd / && rm -rf /tmp/panda-wnscript

ENV PANDA_LOCK_DIR /var/run/panda
RUN mkdir -p ${PANDA_LOCK_DIR} && chmod 777 ${PANDA_LOCK_DIR}

CMD exec /bin/bash -c "trap : TERM INT; sleep infinity & wait"

EXPOSE 25080 25443
