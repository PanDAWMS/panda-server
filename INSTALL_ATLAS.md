Installation for ATLAS on CC7 + virtualenv + python3 + atlpan
--------------------

1. Preparation.
   ```
   # setup virtualenv unser /opt/pandaserver, and install host/user certificates and voms-clients
   ...
   # install httpd stuff
   yum install httpd, httpd-devel, mod_ssl, gridsite
   # make home dir for the service account
   mkdir /home/atlpan
   chown atlpan:zp /home/atlpan
   # make log dirs
   mkdir -p /var/log/panda
   mkdir -p /var/log/panda/wsgisocks
   mkdir -p /var/cache/pandaserver
   chown atlpan:zp /var/log/panda
   chown atlpan:zp /var/log/panda/wsgisocks
   chown atlpan:zp /var/cache/pandaserver
   ```
1. Install panda-server and oracledb.
   ```
   source /opt/pandaserver/bin/activate
   pip install panda-server
   pip install oracledb
   ``` 
   
1. Modify config files.
   ```
   cd /opt/pandaserver/etc/panda
   mv panda_common.cfg.rpmnew panda_common.cfg
   mv panda_server.cfg.rpmnew panda_server.cfg       
   mv panda_server-httpd-FastCGI.conf.rpmnew panda_server-httpd.conf        
   vi panda_server.cfg
   vi panda_common.cfg panda_server-httpd.conf # (if needed)
   ```
1. Make symlinks and add the service.
   ```
   ln -fs /opt/pandaserver/etc/panda/panda_server.sysconfig /etc/sysconfig/panda_server
   ln -fs /opt/pandaserver/etc/rc.d/init.d/panda_server /etc/rc.d/init.d/httpd-pandasrv
   chkconfig --add httpd-pandasrv
   ```
1. Add cron and logrotate.
   ```
   cp /opt/pandaserver/etc/panda/pandasrv.cron /etc/cron.d/
   vi /etc/cron.d/pandasrv.cron # (if needed)
   cp /opt/pandaserver/etc/panda/panda_server.logrotate /etc/logrotate.d/
   ``` 
