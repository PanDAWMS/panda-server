import os
import re
import sys
import ssl
import time
import signal
import socket
import optparse
import datetime
try:
    import cPickle as pickle
except ImportError:
    import pickle

import stomp

from pandaserver.config import panda_config
from pandaserver.brokerage.SiteMapper import SiteMapper
from pandaserver.dataservice.Finisher import Finisher
from pandaserver.dataservice import DataServiceUtils
from pandaserver.srvcore.CoreUtils import commands_get_status_output

import logging
logging.basicConfig(level = logging.DEBUG)

# logger
from pandacommon.pandalogger.PandaLogger import PandaLogger
_logger = PandaLogger().getLogger('fileCallbackListener')

# keep PID
pidFile = '%s/file_callback_listener.pid' % panda_config.logdir

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
class FileCallbackListener(stomp.ConnectionListener):

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
            lfn = 'UNKNOWN'
            # send ack
            id = headers['message-id']
            self.conn.ack(id,self.subscription_id)
            # check message type
            messageType = headers['cbtype']
            if not messageType in ['FileDoneMessage']:
                _logger.debug('%s skip' % messageType)
                return
            _logger.debug('%s start' % messageType)            
            # re-construct message
            messageObj = pickle.loads(message)
            evtTime = datetime.datetime.utcfromtimestamp(messageObj.getItem('eventTime'))
            lfn     = messageObj.getItem('lfn')
            guid    = messageObj.getItem('guid')
            ddmSite = messageObj.getItem('site')
            _logger.debug('%s site=%s type=%s time=%s' % \
                          (lfn,ddmSite,messageType,evtTime.strftime('%Y-%m-%d %H:%M:%S')))
            # ignore non production files
            flagNgPrefix = False
            for ngPrefix in ['user','step']:
                if lfn.startswith(ngPrefix):
                    flagNgPrefix = True
                    break
            if flagNgPrefix:
                _logger.debug('%s skip' % lfn)                
                return
            # get datasets associated with the file only for high priority jobs
            dsNameMap = self.taskBuffer.getDatasetWithFile(lfn,800)
            _logger.debug('%s ds=%s' % (lfn,str(dsNameMap)))
            # loop over all datasets
            for dsName in dsNameMap:
                dsData = dsNameMap[dsName]
                pandaSite,dsToken = dsData
                # skip multiple destination since each file doesn't have
                # transferStatus
                if not dsToken in ['',None] and ',' in dsToken:
                    _logger.debug('%s ignore ds=%s token=%s' % (lfn,dsName,dsToken))
                    continue
                # check site
                tmpSiteSpec = self.siteMapper.getSite(pandaSite)
                if re.search('_dis\d+$',dsName) is not None:
                    setokens = tmpSiteSpec.setokens_input
                    ddm = tmpSiteSpec.ddm_input
                elif re.search('_sub\d+$',dsName) is not None:
                    setokens = tmpSiteSpec.setokens_output
                    ddm = tmpSiteSpec.ddm_output
                if dsToken in setokens:
                    pandaSiteDdmID = setokens[dsToken]
                else:
                    pandaSiteDdmID = ddm
                if  pandaSiteDdmID != ddmSite:
                    _logger.debug('%s ignore ds=%s site=%s:%s <> %s' % \
                                  (lfn,dsName,pandaSite,pandaSiteDdmID,ddmSite))
                    continue
                # update file
                forInput = None
                if re.search('_dis\d+$',dsName) is not None:
                    # dispatch datasets
                    forInput = True
                    ids = self.taskBuffer.updateInFilesReturnPandaIDs(dsName,'ready',lfn)
                elif re.search('_sub\d+$',dsName) is not None:
                    # sub datasets
                    forInput = False
                    ids = self.taskBuffer.updateOutFilesReturnPandaIDs(dsName,lfn)
                _logger.debug('%s ds=%s ids=%s' % (lfn,dsName,str(ids)))
                # loop over all PandaIDs
                if forInput is not None and len(ids) != 0:
                    # remove None and unknown
                    targetIDs = []
                    for tmpID in ids:
                        # count the number of pending files
                        nPending = self.taskBuffer.countPendingFiles(tmpID,forInput)
                        _logger.debug('%s PandaID=%s nPen=%s' % (lfn,tmpID,nPending))
                        if nPending != 0:
                            continue
                        targetIDs.append(tmpID)
                    # get jobs
                    targetJobs = []
                    if targetIDs != []:
                        if forInput:
                            jobs = self.taskBuffer.peekJobs(targetIDs,fromActive=False,fromArchived=False,
                                                            fromWaiting=False)
                        else:
                            jobs = self.taskBuffer.peekJobs(targetIDs,fromDefined=False,fromArchived=False,
                                                            fromWaiting=False)
                        for tmpJob in jobs:
                            if tmpJob is None or tmpJob.jobStatus == 'unknown':
                                continue
                            targetJobs.append(tmpJob)
                    # trigger subsequent processe
                    if targetJobs == []:
                        _logger.debug('%s no jobs to be triggerd for subsequent processe' % lfn)
                    else:
                        if forInput:
                            # activate
                            _logger.debug('%s activate %s' % (lfn,str(targetIDs)))
                            self.taskBuffer.activateJobs(targetJobs)
                        else:
                            # finish
                            _logger.debug('%s finish %s' % (lfn,str(targetIDs)))                        
                            for tmpJob in targetJobs:
                                fThr = Finisher(self.taskBuffer,None,tmpJob)
                                fThr.start()
                                fThr.join()
            _logger.debug('%s done' % lfn)
        except Exception:
            errtype,errvalue = sys.exc_info()[:2]
            _logger.error("on_message : %s %s %s" % (lfn,errtype,errvalue))
        

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
        queue = '/queue/Consumer.PANDA.atlas.ddm.siteservices'
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
                subscription_id = 'panda-server-consumer-' +  socket.getfqdn()
                _logger.debug('setting listener %s' % clientid)
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
                        conn.set_listener('FileCallbackListener',
                                          FileCallbackListener(conn,taskBuffer,siteMapper,subscription_id))
                        conn.start()
                        conn.connect(headers = {'client-id': clientid})
                        conn.subscribe(destination=queue, id=subscription_id, ack='client-individual')
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
