import os
import re
import sys
import json
import time
import signal
import socket
import commands
import optparse
import datetime
import cPickle as pickle

from dq2.common import log as logging
from dq2.common import stomp
from config import panda_config
from brokerage.SiteMapper import SiteMapper
from dataservice.Finisher import Finisher

# logger
from pandalogger.PandaLogger import PandaLogger
_logger = PandaLogger().getLogger('genCallbackConsumer')

# keep PID
pidFile = '%s/gen_callback_consumer.pid' % panda_config.logdir

# overall timeout value
overallTimeout = 60 * 59

# expiration time
expirationTime = datetime.datetime.utcnow() + datetime.timedelta(minutes=overallTimeout)


# kill whole process
def catch_sig(sig, frame):
    try:
        os.remove(pidFile)
    except:
        pass
    # kill
    _logger.debug('terminating ...')
    commands.getoutput('kill -9 -- -%s' % os.getpgrp())
    # exit
    sys.exit(0)
                                        

# callback listener
class GenCallbackConsumer(stomp.ConnectionListener):

    def __init__(self,conn,tb,sm):
        # connection
        self.conn = conn
        # task buffer
        self.taskBuffer = tb
        # site mapper
        self.siteMapper = sm

        
    def on_error(self,headers,body):
        _logger.error("on_error : %s" % headers['message'])


    def on_disconnected(self,headers,body):
        _logger.error("on_disconnected : %s" % headers['message'])
                        

    def on_message(self, headers, message):
        try:
            # send ack
            id = headers['message-id']
            _logger.debug('%s start' % id)
            # re-construct message
            _logger.debug(str(message))
            messageObj =  json.loads(message)
            _logger.debug(str(messageObj))
            pandaID    = messageObj['PandaID']
            fileStatus = messageObj['transferStatus']
            transferError = None
            if messageObj.has_key('transferError'):
                transferError = messageObj['transferError']
            transferErrorCode = None
            try:
                transferErrorCode = int(messageObj['transferErrorCode'])
            except:
                pass
            _logger.debug('%s id=%s status=%s err=%s code=%s' % \
                              (pandaID,id,fileStatus,transferError,transferErrorCode))
            # get job
            job = self.taskBuffer.peekJobs([pandaID],
                                           fromDefined=False,
                                           fromArchived=False,
                                           fromWaiting=False)[0]
            # check job status
            if job == None:
                _logger.debug('%s : not found' % pandaID)
            elif not job.jobStatus in ['transferring']:
                _logger.error('%s : invalid state -> %s' % (pandaID,self.job.jobStatus))
            else:
                # set transfer status
                isFailed = False
                for tmpFile in job.Files:
                    # only output or log
                    if not tmpFile.type in ['log','output']:
                        continue
                    if fileStatus.has_key(tmpFile.lfn) and fileStatus[tmpFile.lfn] in ['done','skip']:
                        tmpFile.status = 'ready'
                    else:
                        tmpFile.status = 'failed'
                        isFailed = True
                    _logger.debug('%s set %s to %s' % (pandaID,tmpFile.status,tmpFile.lfn))
                if isFailed:
                    updateAttr = {}
                    if transferError != None:
                        updateAttr['ddmErrorDiag'] = str(transferError)
                    if transferErrorCode != None:
                        updateAttr['ddmErrorCode'] = transferErrorCode
                    if updateAttr != {}:    
                        self.taskBuffer.updateJobStatus(job.PandaID,job.jobStatus,updateAttr)
                # call finisher
                _logger.debug('%s call finisher' % pandaID)
                fThr = Finisher(self.taskBuffer,None,job)
                fThr.start()
                fThr.join()
            self.conn.ack({'message-id':id})
            _logger.debug('%s done' % pandaID)
        except:
            errtype,errvalue = sys.exc_info()[:2]
            _logger.error("on_message : %s %s %s" % (id,errtype,errvalue))
        

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
        from taskbuffer.TaskBuffer import taskBuffer
        # initialize cx_Oracle using dummy connection
        from taskbuffer.Initializer import initializer
        initializer.init()
        # instantiate TB
        taskBuffer.init(panda_config.dbhost,panda_config.dbpasswd,nDBConnection=1)
        # instantiate sitemapper
        siteMapper = SiteMapper(taskBuffer)
        # ActiveMQ params
        clientid = 'PANDA-' + socket.getfqdn()
        queue = '/queue/Consumer.test1.poc.pocMSG'
        ssl_opts = {'use_ssl' : True,
                    'ssl_cert_file' : '/data/atlpan/hostcert.pem',
                    'ssl_key_file'  : '/data/atlpan/hostkey.pem'}
        # resolve multiple brokers
        brokerList = socket.gethostbyname_ex('gridmsg007.cern.ch')[-1]
        # set listener
        for tmpBroker in brokerList:
            try:
                _logger.debug('setting listener on %s' % tmpBroker)                
                conn = stomp.Connection(host_and_ports = [(tmpBroker, 6162)], **ssl_opts)
                conn.set_listener('GenCallbackConsumer', GenCallbackConsumer(conn,taskBuffer,siteMapper))
                conn.start()
                conn.connect(headers = {'client-id': clientid})
                conn.subscribe(destination=queue, ack='client-individual')
                               #,headers = {'selector':"cbtype='FileDoneMessage'"})
                if not conn.is_connected():
                    _logger.error("connection failure to %s" % tmpBroker)
            except:     
                errtype,errvalue = sys.exc_info()[:2]
                _logger.error("failed to set listener on %s : %s %s" % (tmpBroker,errtype,errvalue))
                catch_sig(None,None)
            
# entry
if __name__ == "__main__":
    optP = optparse.OptionParser(conflict_handler="resolve")
    options,args = optP.parse_args()
    try:
        # time limit
        timeLimit = datetime.datetime.utcnow() - datetime.timedelta(seconds=overallTimeout-180)
        # get process list
        scriptName = sys.argv[0]
        out = commands.getoutput('env TZ=UTC ps axo user,pid,lstart,args | grep %s' % scriptName)
        for line in out.split('\n'):
            items = line.split()
            # owned process
            if not items[0] in ['sm','atlpan','root']: # ['os.getlogin()']: doesn't work in cron
                continue
            # look for python
            if re.search('python',line) == None:
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
                commands.getoutput('kill -9 %s' % pid)
    except:
        errtype,errvalue = sys.exc_info()[:2]
        _logger.error("kill process : %s %s" % (errtype,errvalue))
    # main loop    
    main()
