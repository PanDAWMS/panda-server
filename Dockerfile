# To build the unified JEDI and PanDA server image, run the following command from the root of the repository:
# docker build .

FROM ghcr.io/pandawms/panda-base-platform:latest

RUN mkdir /tmp/src
WORKDIR /tmp/src
COPY . .

RUN /opt/panda/bin/pip install --no-cache-dir .[postgres,oracle]
RUN /opt/panda/bin/pip install --no-cache-dir rucio-clients
RUN /opt/panda/bin/pip install --no-cache-dir "git+https://github.com/PanDAWMS/panda-cacheschedconfig.git"
RUN ln -s /opt/panda/lib/python*/site-packages/mod_wsgi/server/mod_wsgi*.so /etc/httpd/modules/mod_wsgi.so

WORKDIR /
RUN rm -rf /tmp/src

RUN mkdir -p /etc/panda
RUN mkdir -p /etc/idds
RUN mv /opt/panda/etc/panda/panda_common.cfg.rpmnew /etc/panda/panda_common.cfg
RUN mv /opt/panda/etc/panda/panda_server.cfg.rpmnew /etc/panda/panda_server.cfg
RUN mv /opt/panda/etc/panda/sysconfig/panda_server.sysconfig.rpmnew /etc/sysconfig/panda_server
RUN mv /opt/panda/etc/panda/panda_server-httpd.conf.rpmnew /opt/panda/etc/panda/panda_server-httpd.conf
RUN mv /opt/panda/etc/panda/panda_jedi.cfg.rpmnew /etc/panda/panda_jedi.cfg
RUN mv /opt/panda/etc/panda/sysconfig/panda_jedi /etc/sysconfig/panda_jedi

RUN mkdir -p /etc/rc.d/init.d
RUN ln -s /opt/panda/etc/rc.d/init.d/panda_jedi /etc/rc.d/init.d/panda-jedi

RUN mkdir -p /data/atlpan
RUN mkdir -p /data/panda
RUN mkdir -p /var/log/panda/wsgisocks
RUN mkdir -p /var/log/panda/pandacache
RUN mkdir -p /var/log/panda/pandacache/jedilog
RUN mkdir -p /var/cache/pandaserver/schedconfig
RUN mkdir -p /var/run/panda
RUN mkdir -p /var/cric
RUN mkdir -p /run/httpd/wsgisocks
RUN chown -R atlpan:zp /var/log/panda

RUN ln -fs /opt/panda/etc/cert/hostkey.pem /etc/grid-security/hostkey.pem
RUN ln -fs /opt/panda/etc/cert/hostcert.pem /etc/grid-security/hostcert.pem
RUN ln -fs /opt/panda/etc/cert/chain.pem /etc/grid-security/chain.pem
RUN ln -s /opt/panda/etc/rc.d/init.d/panda_server /etc/rc.d/init.d/httpd-pandasrv
RUN ln -fs /data/panda/idds.cfg /opt/panda/etc/idds/idds.cfg
RUN ln -fs /data/panda/rucio.cfg /opt/panda/etc/rucio.cfg
RUN ln -fs /data/panda/panda_mbproxy_config.json /opt/panda/etc/panda/panda_mbproxy_config.json
RUN ln -s /etc/sysconfig/panda_server /opt/panda/etc/panda/panda_server.sysconfig
RUN ln -fs /data/panda/jedi_mq_config.json /opt/panda/etc/panda/jedi_mq_config.json
RUN ln -fs /data/panda/jedi_msg_proc_config.json /opt/panda/etc/panda/jedi_msg_proc_config.json
RUN ln -fs /data/panda/panda_mbproxy_config.json /opt/panda/etc/panda/panda_mbproxy_config.json

# to run with non-root PID
RUN mkdir -p /etc/grid-security/certificates
RUN chmod -R 777 /etc/grid-security/certificates

RUN chmod -R 777 /data/panda
RUN chmod -R 777 /data/atlpan
RUN chmod -R 777 /home/atlpan
RUN chmod -R 777 /run/httpd
RUN chmod -R 777 /var/log/panda
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

ENV PANDA_LOCK_DIR=/var/run/panda
RUN mkdir -p ${PANDA_LOCK_DIR} && chmod 777 ${PANDA_LOCK_DIR}

# make a wrapper script to launch services and periodic jobs in non-root container
RUN echo $'#!/bin/bash \n\
set -m \n\
/data/panda/init-panda \n\
/data/panda/run-panda-crons & \n\
/etc/rc.d/init.d/httpd-pandasrv start \n ' > /etc/rc.d/init.d/run-panda-services

RUN chmod +x /etc/rc.d/init.d/run-panda-services

# make a wrapper script to launch services and periodic jobs in non-root container
RUN echo $'#!/bin/bash \n\
set -m \n\
/data/panda/init-jedi \n\
/data/panda/run-jedi-crons & \n\
/etc/rc.d/init.d/panda-jedi start \n ' > /etc/rc.d/init.d/run-jedi-services

RUN chmod +x /etc/rc.d/init.d/run-jedi-services

CMD ["sleep", "infinity"]

EXPOSE 25080 25443
