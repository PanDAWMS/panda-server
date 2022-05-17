FROM docker.io/centos:7

RUN yum update -y
RUN yum install -y epel-release
RUN yum install -y python3 python3-devel httpd httpd-devel gcc gridsite less git postgresql
RUN python3 -m venv /opt/panda
RUN /opt/panda/bin/pip install -U pip
RUN /opt/panda/bin/pip install -U setuptools
RUN adduser atlpan
RUN groupadd zp
RUN usermod -a -G zp atlpan
RUN /opt/panda/bin/pip install "git+https://github.com/PanDAWMS/panda-server.git#egg=panda-server[postgres]"
RUN /opt/panda/bin/pip install rucio-clients
RUN ln -s /opt/panda/lib/python*/site-packages/mod_wsgi/server/mod_wsgi*.so /etc/httpd/modules/mod_wsgi.so

RUN mkdir -p /etc/panda
RUN mkdir -p /etc/idds
RUN mv /opt/panda/etc/panda/panda_common.cfg.rpmnew /etc/panda/panda_common.cfg
RUN mv /opt/panda/etc/idds/idds.cfg.client.template /opt/panda/etc/idds/idds.cfg
RUN mv /opt/panda/etc/panda/panda_server.cfg.rpmnew /etc/panda/panda_server.cfg
RUN mv /opt/panda/etc/panda/panda_server.sysconfig.rpmnew /etc/sysconfig/panda_server
RUN mv /opt/panda/etc/panda/panda_server-httpd-FastCGI.conf.rpmnew /opt/panda/etc/panda/panda_server-httpd.conf

RUN ln -fs /opt/panda/etc/cert/hostkey.pem /etc/grid-security/hostkey.pem
RUN ln -fs /opt/panda/etc/cert/hostcert.pem /etc/grid-security/hostcert.pem
RUN ln -s /opt/panda/etc/rc.d/init.d/panda_server /etc/rc.d/init.d/httpd-pandasrv

RUN mkdir -p /data/atlpan
RUN mkdir -p /var/log/panda/wsgisocks
RUN mkdir -p /run/httpd/wsgisocks
RUN chown -R atlpan:zp /var/log/panda

# to run with non-root PID
RUN chmod -R 777 /var/log/panda
RUN chmod -R 777 /run/httpd
RUN chmod -R 777 /home/atlpan

CMD exec /bin/bash -c "trap : TERM INT; sleep infinity & wait"

EXPOSE 25080 25443
