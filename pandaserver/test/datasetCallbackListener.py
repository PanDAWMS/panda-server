import os
import re
import sys
import ssl
import time
import signal
import socket
import optparse
import datetime
import stomp

from pandaserver.config import panda_config
from pandaserver.brokerage.SiteMapper import SiteMapper
from pandaserver.dataservice import DataServiceUtils
from pandaserver.dataservice.DDMHandler import DDMHandler
from pandaserver.srvcore.CoreUtils import commands_get_status_output

import yaml
import logging
logging.basicConfig(level = logging.DEBUG)

# logger
from pandacommon.pandalogger.PandaLogger import PandaLogger
_logger = PandaLogger().getLogger('datasetCallbackListener')

# keep PID
pidFile = '%s/dataset_callback_listener.pid' % panda_config.logdir

# overall timeout value
overallTimeout = 60 * 59

# expiration time
expirationTime = datetime.datetime.utcnow() + datetime.timedelta(minutes=overallTimeout)


# kill whole process
def catch_sig(sig, frame):
    try:
        os.remove(pidFile)
    except Exception:
        pass
    # kill
    _logger.debug('terminating ...')
    commands_get_status_output('kill -9 -- -%s' % os.getpgrp())
    # exit
    sys.exit(0)
                                        

# callback listener
class DatasetCallbackListener(stomp.ConnectionListener):

    def __init__(self,conn,tb,sm,subscription_id):
        # connection
        self.conn = conn
        # task buffer
        self.taskBuffer = tb
        # site mapper
        self.siteMapper = sm
        # subscription ID
        self.subscription_id = subscription_id

        
    def on_error(self,headers,body):
        _logger.error("on_error : %s" % headers['message'])


    def on_disconnected(self,headers,body):
        _logger.error("on_disconnected : %s" % headers['message'])
                        

    def on_message(self, headers, message):
        try:
            dsn = 'UNKNOWN'
            # send ack
            id = headers['message-id']
            #self.conn.ack(id,self.subscription_id)
            # convert message form str to dict
            messageDict = yaml.load(message)
            # check event type
            if not messageDict['event_type'] in ['datasetlock_ok']:
                _logger.debug('%s skip' % messageDict['event_type'])
                return
            _logger.debug('%s start' % messageDict['event_type'])
            messageObj = messageDict['payload']
            # only for _dis or _sub
            dsn = messageObj['name']
            if (re.search('_dis\d+$',dsn) is None) and (re.search('_sub\d+$',dsn) is None):
                _logger.debug('%s is not _dis or _sub dataset, skip' % dsn)
                return
            # take action
            scope = messageObj['scope']
            site  = messageObj['rse']
            _logger.debug('%s site=%s type=%s' % (dsn, site, messageDict['event_type']))
            thr = DDMHandler(self.taskBuffer,None,site,dsn,scope)
            thr.start()
            thr.join()
            _logger.debug('done %s' % dsn)
        except Exception:
            errtype,errvalue = sys.exc_info()[:2]
            _logger.error("on_message : %s %s" % (errtype,errvalue))
        

# main
def main(backGround=False): 
    _logger.debug('starting ...')
    # register signal handler
    signal.signal(signal.SIGINT, catch_sig)
    signal.signal(signal.SIGHUP, catch_sig)
    signal.signal(signal.SIGTERM,catch_sig)
    signal.signal(signal.SIGALRM,catch_sig)
    signal.alarm(overallTimeout)
    # forking    
    pid = os.fork()
    if pid != 0:
        # watch child process
        os.wait()
        time.sleep(1)
    else:    
        # main loop
        from pandaserver.taskbuffer.TaskBuffer import taskBuffer
        # check certificate
        certName = '%s/pandasv1_usercert.pem' %panda_config.certdir
        keyName = '%s/pandasv1_userkey.pem' %panda_config.certdir
        _logger.debug('checking certificate {0}'.format(certName))
        certOK,certMsg = DataServiceUtils.checkCertificate(certName)
        if not certOK:
            _logger.error('bad certificate : {0}'.format(certMsg))
        # initialize cx_Oracle using dummy connection
        from pandaserver.taskbuffer.Initializer import initializer
        initializer.init()
        # instantiate TB
        taskBuffer.init(panda_config.dbhost,panda_config.dbpasswd,nDBConnection=1)
        # instantiate sitemapper
        siteMapper = SiteMapper(taskBuffer)
        # ActiveMQ params
        queue = '/queue/Consumer.panda.rucio.events'
        ssl_opts = {'use_ssl' : True,
                    'ssl_version' : ssl.PROTOCOL_TLSv1,
                    'ssl_cert_file' : certName,
                    'ssl_key_file'  : keyName}
        # resolve multiple brokers
        brokerList = socket.gethostbyname_ex('atlas-mb.cern.ch')[-1]
        # set listener
        connList = []
        for tmpBroker in brokerList:
            try:
                clientid = 'PANDA-' + socket.getfqdn() + '-' + tmpBroker
                subscription_id = 'panda-server-consumer'
                _logger.debug('setting listener %s to broker %s' % (clientid, tmpBroker))
                conn = stomp.Connection(host_and_ports = [(tmpBroker, 61023)], **ssl_opts)
                connList.append(conn)
            except Exception:
                errtype,errvalue = sys.exc_info()[:2]
                _logger.error("failed to connect to %s : %s %s" % (tmpBroker,errtype,errvalue))
                catch_sig(None,None)
        while True:
            for conn in connList:
                try:
                    if not conn.is_connected():
                        conn.set_listener('DatasetCallbackListener', DatasetCallbackListener(conn,taskBuffer,siteMapper,
                                                                                             subscription_id))
                        conn.start()
                        conn.connect(headers = {'client-id': clientid})
                        conn.subscribe(destination=queue, id=subscription_id, ack='auto')
                        _logger.debug('listener %s is up and running' % clientid)
                except Exception:
                    errtype,errvalue = sys.exc_info()[:2]
                    _logger.error("failed to set listener on %s : %s %s" % (tmpBroker,errtype,errvalue))
                    catch_sig(None,None)
            time.sleep(5)
            
# entry
if __name__ == "__main__":
    optP = optparse.OptionParser(conflict_handler="resolve")
    options,args = optP.parse_args()
    try:
        # time limit
        timeLimit = datetime.datetime.utcnow() - datetime.timedelta(seconds=overallTimeout-180)
        # get process list
        scriptName = sys.argv[0]
        out = commands_get_status_output('env TZ=UTC ps axo user,pid,lstart,args | grep %s' % scriptName)[-1]
        for line in out.split('\n'):
            items = line.split()
            # owned process
            if not items[0] in ['sm','atlpan','pansrv','root']: # ['os.getlogin()']: doesn't work in cron
                continue
            # look for python
            if re.search('python',line) is None:
                continue
            # PID
            pid = items[1]
            # start time
            timeM = re.search('(\S+\s+\d+ \d+:\d+:\d+ \d+)',line)
            startTime = datetime.datetime(*time.strptime(timeM.group(1),'%b %d %H:%M:%S %Y')[:6])
            # kill old process
            if startTime < timeLimit:
                _logger.debug("old process : %s %s" % (pid,startTime))
                _logger.debug(line)            
                commands_get_status_output('kill -9 %s' % pid)
    except Exception:
        errtype,errvalue = sys.exc_info()[:2]
        _logger.error("kill process : %s %s" % (errtype,errvalue))
    # main loop    
    main()
