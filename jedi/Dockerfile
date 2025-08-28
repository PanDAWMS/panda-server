# To build the JEDI image, run the following command from the root of the repository:
# docker build -f jedi/Dockerfile .

FROM ghcr.io/pandawms/panda-base-platform:latest


RUN /opt/panda/bin/pip install "git+https://github.com/PanDAWMS/panda-server.git#egg=panda-server[postgres]&subdirectory=server"

RUN mkdir /tmp/src
WORKDIR /tmp/src
COPY . .
RUN cd jedi && /opt/panda/bin/pip install --no-cache-dir .[postgres] && cd ..
RUN /opt/panda/bin/pip install --no-cache-dir rucio-clients

RUN mkdir -p /etc/panda
RUN mkdir -p /etc/idds
RUN mv /opt/panda/etc/panda/panda_common.cfg.rpmnew /etc/panda/panda_common.cfg
RUN mv /opt/panda/etc/panda/panda_server.cfg.rpmnew /etc/panda/panda_server.cfg
RUN mv /opt/panda/etc/panda/panda_jedi.cfg.rpmnew /etc/panda/panda_jedi.cfg
RUN mv /opt/panda/etc/panda/sysconfig/panda_server.sysconfig.rpmnew /etc/sysconfig/panda_server
RUN mv /opt/panda/etc/panda/sysconfig/panda_jedi /etc/sysconfig/panda_jedi

RUN mkdir -p /etc/rc.d/init.d
RUN ln -s /opt/panda/etc/rc.d/init.d/panda_jedi /etc/rc.d/init.d/panda-jedi
RUN ln -s /etc/sysconfig/panda_server /opt/panda/etc/panda/panda_server.sysconfig

RUN mkdir -p /data/atlpan
RUN mkdir -p /data/panda
RUN mkdir -p /var/log/panda/wsgisocks
RUN mkdir -p /run/httpd/wsgisocks
RUN chown -R atlpan:zp /var/log/panda

RUN ln -fs /data/panda/idds.cfg /opt/panda/etc/idds/idds.cfg
RUN ln -fs /data/panda/rucio.cfg /opt/panda/etc/rucio.cfg
RUN ln -fs /data/panda/jedi_mq_config.json /opt/panda/etc/panda/jedi_mq_config.json
RUN ln -fs /data/panda/jedi_msg_proc_config.json /opt/panda/etc/panda/jedi_msg_proc_config.json
RUN ln -fs /data/panda/panda_mbproxy_config.json /opt/panda/etc/panda/panda_mbproxy_config.json

# to run with non-root PID
RUN chmod -R 777 /var/log/panda
RUN chmod -R 777 /home/atlpan
RUN chmod -R 777 /var/lock
RUN chmod -R 777 /data/panda
RUN mkdir -p /etc/grid-security/certificates
RUN chmod -R 777 /etc/grid-security/certificates
RUN chmod -R 777 /var/lib/logrotate

ENV PANDA_LOCK_DIR=/var/run/panda
RUN mkdir -p ${PANDA_LOCK_DIR} && chmod 777 ${PANDA_LOCK_DIR}

# make a wrapper script to launch services and periodic jobs in non-root container
RUN echo $'#!/bin/bash \n\
set -m \n\
/data/panda/init-jedi \n\
/data/panda/run-jedi-crons & \n\
/etc/rc.d/init.d/panda-jedi start \n ' > /etc/rc.d/init.d/run-jedi-services

RUN chmod +x /etc/rc.d/init.d/run-jedi-services

CMD ["sleep", "infinity"]
