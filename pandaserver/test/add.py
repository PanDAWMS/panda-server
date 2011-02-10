import os
import re
import sys
import time
import glob
import fcntl
import random
import datetime
import commands
import threading
from taskbuffer.TaskBuffer import taskBuffer
from pandalogger.PandaLogger import PandaLogger
from dataservice.Adder2 import Adder
from brokerage.SiteMapper import SiteMapper

# password
from config import panda_config
passwd = panda_config.dbpasswd

# logger
_logger = PandaLogger().getLogger('add')

_logger.debug("===================== start =====================")

# overall timeout value
overallTimeout = 60

# kill old process
try:
    # time limit
    timeLimit = datetime.datetime.utcnow() - datetime.timedelta(minutes=overallTimeout)
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
    type, value, traceBack = sys.exc_info()
    _logger.error("kill process : %s %s" % (type,value))
    
# instantiate TB
taskBuffer.init(panda_config.dbhost,panda_config.dbpasswd,nDBConnection=1)

# instantiate sitemapper
aSiteMapper = SiteMapper(taskBuffer)

# delete
_logger.debug("Del session")
status,retSel = taskBuffer.querySQLS("SELECT MAX(PandaID) FROM ATLAS_PANDA.jobsDefined4",{})
if retSel != None:
    try:
        maxID = retSel[0][0]
        _logger.debug("maxID : %s" % maxID)
        if maxID != None:
            varMap = {}
            varMap[':maxID'] = maxID
            varMap[':jobStatus1'] = 'activated'
            varMap[':jobStatus2'] = 'waiting'
            varMap[':jobStatus3'] = 'failed'
            varMap[':jobStatus4'] = 'cancelled'            
            status,retDel = taskBuffer.querySQLS("DELETE FROM ATLAS_PANDA.jobsDefined4 WHERE PandaID<:maxID AND jobStatus IN (:jobStatus1,:jobStatus2,:jobStatus3,:jobStatus4)",varMap)
    except:
        pass

# count # of getJob/updateJob in dispatcher's log
try:
    # don't update when logrotate is running
    timeNow = datetime.datetime.utcnow()
    logRotateTime = timeNow.replace(hour=3,minute=2,second=0,microsecond=0)
    if (timeNow > logRotateTime and (timeNow-logRotateTime) < datetime.timedelta(minutes=5)) or \
           (logRotateTime > timeNow and (logRotateTime-timeNow) < datetime.timedelta(minutes=5)):
        _logger.debug("skip pilotCounts session for logrotate")
    else:
        # log filename
        dispLogName = '%s/panda-PilotRequests.log' % panda_config.logdir
        # time limit
        timeLimit  = datetime.datetime.utcnow() - datetime.timedelta(hours=3)
        # check if tgz is required
        com = 'head -1 %s' % dispLogName
        lostat,loout = commands.getstatusoutput(com)
        useLogTgz = True
        if lostat == 0:
            match = re.search('^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}',loout)
            if match != None:
                startTime = datetime.datetime(*time.strptime(match.group(0),'%Y-%m-%d %H:%M:%S')[:6])
                # current log contains all info
                if startTime<timeLimit:
                    useLogTgz = False
        # log files
        dispLogNameList = [dispLogName]
        if useLogTgz:
            dispLogNameList.append('%s.1.gz' % dispLogName)
        # tmp name
        tmpLogName = '%s.tmp' % dispLogName
        # loop over all files
        pilotCounts = {}
        for tmpDispLogName in dispLogNameList:
            # expand or copy
            if tmpDispLogName.endswith('.gz'):
                com = 'gunzip -c %s | tac > %s' % (tmpDispLogName,tmpLogName)
            else:
                com = 'tac %s > %s' % (tmpDispLogName,tmpLogName)            
            lostat,loout = commands.getstatusoutput(com)
            if lostat != 0:
                errMsg = 'failed to expand/copy %s with : %s' % (tmpDispLogName,loout)
                raise RuntimeError,errMsg
            # search string
            sStr  = '^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}).* '
            sStr += 'INFO .* method=(.+),site=(.+),node=(.+),type=(.+)'        
            # read
            logFH = open(tmpLogName)
            for line in logFH:
                # check format
                match = re.search(sStr,line)
                if match != None: 
                    # check timerange
                    timeStamp = datetime.datetime(*time.strptime(match.group(1),'%Y-%m-%d %H:%M:%S')[:6])
                    if timeStamp<timeLimit:
                        break
                    tmpMethod = match.group(2)
                    tmpSite   = match.group(3)
                    tmpNode   = match.group(4)
                    tmpType   = match.group(5)
                    # sum
                    if not pilotCounts.has_key(tmpSite):
                        pilotCounts[tmpSite] = {}
                    if not pilotCounts[tmpSite].has_key(tmpMethod):
                        pilotCounts[tmpSite][tmpMethod] = {}
                    if not pilotCounts[tmpSite][tmpMethod].has_key(tmpNode):
                        pilotCounts[tmpSite][tmpMethod][tmpNode] = 0
                    pilotCounts[tmpSite][tmpMethod][tmpNode] += 1
            # close            
            logFH.close()
        # delete tmp
        commands.getoutput('rm %s' % tmpLogName)
        # update
        hostID = panda_config.pserverhost.split('.')[0]
        _logger.debug("pilotCounts session")    
        _logger.debug(pilotCounts)
        retPC = taskBuffer.updateSiteData(hostID,pilotCounts)
        _logger.debug(retPC)
except:
    errType,errValue = sys.exc_info()[:2]
    _logger.error("updateJob/getJob : %s %s" % (errType,errValue))


# mail sender
class MailSender (threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        _logger.debug("mail : start")
        tmpFileList = glob.glob('%s/mail_*' % panda_config.logdir)
        for tmpFile in tmpFileList:
            # check timestamp to avoid too new files
            timeStamp = os.path.getmtime(tmpFile)
            if datetime.datetime.utcnow() - datetime.datetime.fromtimestamp(timeStamp) < datetime.timedelta(minutes=1):
                continue
            # lock
            mailFile = open(tmpFile)
            try:
                fcntl.flock(mailFile.fileno(), fcntl.LOCK_EX|fcntl.LOCK_NB)
            except:
                _logger.debug("mail : failed to lock %s" % tmpFile.split('/')[-1])
                mailFile.close()
                continue
            # start notifier
            from dataservice.Notifier import Notifier
            nThr = Notifier(None,None,None,None,mailFile,tmpFile)
            nThr.start()
            nThr.join()
            # remove
            try:
                os.remove(tmpFile)
            except:
                pass
            # unlock
            try:
                fcntl.flock(self.lockXML.fileno(), fcntl.LOCK_UN)
                mailFile.close()
            except:
                pass
            
# start sender
mailSender =  MailSender()
mailSender.start()
            
    
# thread pool
class ThreadPool:
    def __init__(self):
        self.lock = threading.Lock()
        self.list = []

    def add(self,obj):
        self.lock.acquire()
        self.list.append(obj)
        self.lock.release()

    def remove(self,obj):
        self.lock.acquire()
        self.list.remove(obj)
        self.lock.release()

    def join(self):
        self.lock.acquire()
        thrlist = tuple(self.list)
        self.lock.release()
        for thr in thrlist:
            thr.join()

# thread to adder
class AdderThr (threading.Thread):
    def __init__(self,lock,pool,taskBuffer,aSiteMapper,pandaID,jobStatus,fileName,ignoreError=True):
        threading.Thread.__init__(self)
        self.lock       = lock
        self.pool       = pool
        self.pool.add(self)
        self.adder = Adder(taskBuffer,pandaID,"",jobStatus,xmlFile=fileName,
                           ignoreDDMError=ignoreError,joinCloser=True,addOutput=True,
                           siteMapper=aSiteMapper)
                                        
    def run(self):
        self.lock.acquire()
        self.adder.start()
        self.adder.join()
        self.pool.remove(self)
        self.lock.release()


# get buildJobs in the holding state
holdingAna = []
varMap = {}
varMap[':prodSourceLabel'] = 'panda'
varMap[':jobStatus'] = 'holding'
status,res = taskBuffer.querySQLS("SELECT PandaID from ATLAS_PANDA.jobsActive4 WHERE prodSourceLabel=:prodSourceLabel AND jobStatus=:jobStatus",varMap)
if res != None:
    for id, in res:
        holdingAna.append(id)
_logger.debug("holding Ana %s " % holdingAna)
    
# add files
_logger.debug("Adder session")
timeNow = datetime.datetime.utcnow()
timeInt = datetime.datetime.utcnow()
dirName = panda_config.logdir
fileList = os.listdir(dirName)
fileList.sort() 
# remove duplicated files
tmpList = []
uMap = {}
for file in fileList:
    match = re.search('^(\d+)_([^_]+)_.{36}$',file)
    if match != None:
        fileName = '%s/%s' % (dirName,file)
        id = match.group(1)
        if uMap.has_key(id):
            try:
                os.remove(fileName)
            except:
                pass
        else:
            uMap[id] = fileName
            if long(id) in holdingAna:
                # give a priority to buildJobs
                tmpList.insert(0,file)
            else:
                tmpList.append(file)
fileList = tmpList            

# create thread pool and semaphore
adderLock = threading.Semaphore(3)
adderThreadPool = ThreadPool()

# add
while len(fileList) != 0:
    # time limit to aviod too many copyArchve running at the sametime
    if (datetime.datetime.utcnow() - timeNow) > datetime.timedelta(minutes=overallTimeout):
        _logger.debug("time over in Adder session")
        break
    # try to get Semaphore
    adderLock.acquire()
    # get fileList
    if (datetime.datetime.utcnow() - timeInt) > datetime.timedelta(minutes=15):
        timeInt = datetime.datetime.utcnow()
        # get file
        fileList = os.listdir(dirName)
        fileList.sort() 
        # remove duplicated files
        tmpList = []
        uMap = {}
        for file in fileList:
            match = re.search('^(\d+)_([^_]+)_.{36}$',file)
            if match != None:
                fileName = '%s/%s' % (dirName,file)
                id = match.group(1)
                if uMap.has_key(id):
                    try:
                        os.remove(fileName)
                    except:
                        pass
                else:
                    uMap[id] = fileName
                    if long(id) in holdingAna:
                        # give a priority to buildJob
                        tmpList.insert(0,file)
                    else:
                        tmpList.append(file)
        fileList = tmpList
    # choose a file
    file = fileList.pop(0)
    # release lock
    adderLock.release()
    # check format
    match = re.search('^(\d+)_([^_]+)_.{36}$',file)
    if match != None:
        fileName = '%s/%s' % (dirName,file)
        if not os.path.exists(fileName):
            continue
        try:
            modTime = datetime.datetime(*(time.gmtime(os.path.getmtime(fileName))[:7]))
            if (timeNow - modTime) > datetime.timedelta(hours=24):
                # last chance
                _logger.debug("Last Add File : %s" % fileName)
                thr = AdderThr(adderLock,adderThreadPool,taskBuffer,aSiteMapper,match.group(1),
                               match.group(2),fileName,False)
                thr.start()
            elif (timeInt - modTime) > datetime.timedelta(minutes=3):
                # add
                _logger.debug("Add File : %s" % fileName)            
                thr = AdderThr(adderLock,adderThreadPool,taskBuffer,aSiteMapper,match.group(1),
                               match.group(2),fileName)
                thr.start()
        except:
            type, value, traceBack = sys.exc_info()
            _logger.error("%s %s" % (type,value))

# join all threads
adderThreadPool.join()

# join sender
mailSender.join()

_logger.debug("===================== end =====================")
