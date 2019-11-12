import re
import sys
import glob
import time
import os.path
import datetime
import threading
from pandaserver.config import panda_config
from pandaserver.taskbuffer.TaskBuffer import taskBuffer
from pandaserver.brokerage import SiteMapper
from pandaserver.dataservice.EventPicker import EventPicker
from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandaserver.srvcore.CoreUtils import commands_get_status_output

# logger
_logger = PandaLogger().getLogger('evpPD2P')

_logger.debug("===================== start =====================")

# overall timeout value
overallTimeout = 300
# prefix of evp files
prefixEVP = 'evp.'
# file pattern of evp files
evpFilePatt = panda_config.cache_dir + '/' + prefixEVP + '*'

# kill old process
try:
    # time limit
    timeLimit = datetime.datetime.utcnow() - datetime.timedelta(minutes=overallTimeout)
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
    type, value, traceBack = sys.exc_info()
    _logger.error("kill process : %s %s" % (type,value))

# instantiate PD2P
taskBuffer.init(panda_config.dbhost,panda_config.dbpasswd,nDBConnection=1)
siteMapper = SiteMapper.SiteMapper(taskBuffer)


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


# thread to ev-pd2p
class EvpThr (threading.Thread):
    def __init__(self,lock,pool,aTaskBuffer,aSiteMapper,fileName,ignoreError):
        threading.Thread.__init__(self)
        self.lock       = lock
        self.pool       = pool
        self.fileName   = fileName
        self.evp        = EventPicker(aTaskBuffer,aSiteMapper,fileName,ignoreError)
        self.pool.add(self)
                                        
    def run(self):
        self.lock.acquire()
        retRun = self.evp.run()
        _logger.debug("%s : %s" % (retRun,self.fileName))
        self.pool.remove(self)
        self.lock.release()


# get files
_logger.debug("EVP session")
timeNow = datetime.datetime.utcnow()
timeInt = datetime.datetime.utcnow()
fileList = glob.glob(evpFilePatt)
fileList.sort() 

# create thread pool and semaphore
adderLock = threading.Semaphore(1)
adderThreadPool = ThreadPool()

# add
while len(fileList) != 0:
    # time limit to aviod too many copyArchve running at the sametime
    if (datetime.datetime.utcnow() - timeNow) > datetime.timedelta(minutes=overallTimeout):
        _logger.debug("time over in EVP session")
        break
    # try to get Semaphore
    adderLock.acquire()
    # get fileList
    if (datetime.datetime.utcnow() - timeInt) > datetime.timedelta(minutes=15):
        timeInt = datetime.datetime.utcnow()
        # get file
        fileList = glob.glob(evpFilePatt)
        fileList.sort() 
    # choose a file
    fileName = fileList.pop(0)
    # release lock
    adderLock.release()
    if not os.path.exists(fileName):
        continue
    try:
        modTime = datetime.datetime(*(time.gmtime(os.path.getmtime(fileName))[:7]))
        if (timeNow - modTime) > datetime.timedelta(hours=24):
            # last chance
            _logger.debug("Last event picking : %s" % fileName)
            thr = EvpThr(adderLock,adderThreadPool,taskBuffer,siteMapper,fileName,False)
            thr.start()
        elif (timeInt - modTime) > datetime.timedelta(minutes=1):
            # try
            _logger.debug("event picking : %s" % fileName)            
            thr = EvpThr(adderLock,adderThreadPool,taskBuffer,siteMapper,fileName,True)
            thr.start()
        else:
            _logger.debug("%s : %s" % ((timeInt - modTime),fileName))
    except Exception:
        errType,errValue = sys.exc_info()[:2]
        _logger.error("%s %s" % (errType,errValue))

# join all threads
adderThreadPool.join()

_logger.debug("===================== end =====================")
