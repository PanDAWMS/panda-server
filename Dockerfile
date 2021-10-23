FROM docker.io/centos:7

RUN yum update -y
RUN yum install -y epel-release
RUN yum install -y python3 python3-devel httpd httpd-devel gcc gridsite less
RUN python3 -m venv /opt/panda
RUN /opt/panda/bin/pip install -U pip
RUN /opt/panda/bin/pip install -U setuptools
RUN adduser atlpan
RUN groupadd zp
RUN usermod -a -G zp atlpan
RUN /opt/panda/bin/pip install panda-server[postgres]
RUN /opt/panda/bin/pip install rucio-clients
RUN ln -s /opt/panda/lib/python*/site-packages/mod_wsgi/server/mod_wsgi*.so /etc/httpd/modules/mod_wsgi.so

RUN mkdir -p /etc/panda
RUN mkdir -p /etc/idds
RUN mv /opt/panda/etc/panda/panda_common.cfg.rpmnew /etc/panda/panda_common.cfg
RUN mv /opt/panda/etc/idds/idds.cfg.client.template /opt/panda/etc/idds/idds.cfg
RUN ln -fs /mnt/config/panda_server.cfg /etc/panda/panda_server.cfg
RUN ln -fs /mnt/config/panda_server /etc/sysconfig/panda_server
RUN ln -fs /mnt/config/panda_server-httpd.conf /opt/panda/etc/panda/panda_server-httpd.conf
RUN ln -s /opt/panda/etc/rc.d/init.d/panda_server /etc/rc.d/init.d/httpd-pandasrv

RUN mkdir -p /var/log/panda/wsgisocks
RUN chown -R atlpan:zp /var/log/panda

RUN chkconfig --add httpd-pandasrv

EXPOSE 25080 25443
