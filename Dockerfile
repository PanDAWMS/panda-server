# Base Platform Image for PanDA services
#
# NOTE: Modify the image tag in ./github/workflows/base-platform-docker-image.yml when OS or Python version is changed

ARG PYTHON_VERSION=3.11.6

FROM docker.io/almalinux:9

ARG PYTHON_VERSION

RUN yum update -y
RUN yum install -y epel-release

RUN yum install -y httpd httpd-devel gcc gridsite git psmisc less wget logrotate procps which \
    openssl-devel readline-devel bzip2-devel libffi-devel zlib-devel systemd-udev voms-clients

# install python
RUN mkdir /tmp/python && cd /tmp/python && \
    wget https://www.python.org/ftp/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tgz && \
    tar -xzf Python-*.tgz && rm -f Python-*.tgz && \
    cd Python-* && \
    ./configure --enable-shared --enable-optimizations --with-lto && \
    make altinstall && \
    echo /usr/local/lib > /etc/ld.so.conf.d/local.conf && ldconfig && \
    cd / && rm -rf /tmp/pyton

# install Oracle Instant Client and tnsnames.ora
RUN wget https://download.oracle.com/otn_software/linux/instantclient/oracle-instantclient-basic-linuxx64.rpm -P /tmp/ && \
    yum install /tmp/oracle-instantclient-basic-linuxx64.rpm -y && \
    wget https://download.oracle.com/otn_software/linux/instantclient/oracle-instantclient-sqlplus-linuxx64.rpm -P /tmp/ && \
    yum install /tmp/oracle-instantclient-sqlplus-linuxx64.rpm -y

# Grab the latest version of the Oracle tnsnames.ora file
RUN ln -fs /data/panda/tnsnames.ora /etc/tnsnames.ora

# install postgres
RUN yum install -y https://download.postgresql.org/pub/repos/yum/reporpms/EL-9-x86_64/pgdg-redhat-repo-latest.noarch.rpm
RUN yum install --nogpgcheck -y postgresql16
RUN yum clean all && rm -rf /var/cache/yum

# update network limitations
RUN echo 'net.core.somaxconn=4096' >> /etc/sysctl.d/999-net.somax.conf

# setup venv with pythonX.Y
RUN python$(echo ${PYTHON_VERSION} | sed -E 's/\.[0-9]+$//') -m venv /opt/panda
RUN /opt/panda/bin/pip install --no-cache-dir -U pip
RUN /opt/panda/bin/pip install --no-cache-dir -U setuptools
RUN /opt/panda/bin/pip install --no-cache-dir -U gnureadline
RUN adduser atlpan
RUN groupadd zp
RUN usermod -a -G zp atlpan
