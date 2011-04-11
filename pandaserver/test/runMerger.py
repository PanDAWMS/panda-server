import os
import re
import sys
import time
import datetime
import commands
import threading

from config import panda_config

# initialize cx_Oracle using dummy connection
from taskbuffer.Initializer import initializer
initializer.init()

from dataservice.Merger import Merger
from taskbuffer.TaskBuffer import taskBuffer
from pandalogger.PandaLogger import PandaLogger


# logger
_logger = PandaLogger().getLogger('runMerger')

_logger.debug("================= start ==================")

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

# time limit
timeLimitU = datetime.datetime.utcnow() - datetime.timedelta(minutes=5)
timeLimitL = datetime.datetime.utcnow() - datetime.timedelta(hours=12)
timeLimitX = datetime.datetime.utcnow() - datetime.timedelta(hours=6)

# instantiate TB
taskBuffer.init(panda_config.dbhost,panda_config.dbpasswd,nDBConnection=1)

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


# thread to merge dataset
class MergerThr (threading.Thread):
    def __init__(self,lock,proxyLock,datasets,pool):
        threading.Thread.__init__(self)
        self.datasets   = datasets
        self.lock       = lock
        self.proxyLock  = proxyLock
        self.pool       = pool
        self.pool.add(self)
                                        
    def run(self):
        self.lock.acquire()
        try:
            # loop over all datasets
            for vuid,name,modDate in self.datasets:
                _logger.debug("Merge %s %s" % (modDate,name))
                toBeClosed = False
                # close old datasets anyway
                if modDate < timeLimitX:
                    toBeClosed = True
                # get PandaID
                self.proxyLock.acquire()                
                proxyS = taskBuffer.proxyPool.getProxy()
                pandaID = proxyS.getPandaIDwithDestDBlock(name)
                taskBuffer.proxyPool.putProxy(proxyS)
                self.proxyLock.release()                
                if pandaID == None:
                    _logger.error("failed to find PandaID for %s" % name)
                    toBeClosed = True
                else:
                    # get job
                    self.proxyLock.acquire()
                    pandaJob = taskBuffer.peekJobs([pandaID])[0]
                    self.proxyLock.release()
                    if pandaJob == None:
                        _logger.error("failed to get job for PandaID=" % pandaID)
                        toBeClosed = True
                    else:
                        # run merger
                        _logger.debug("run merger for %s" % name)
                        merger = Merger(taskBuffer,pandaJob)
                        mRet = merger.run()
                        if mRet == None:
                            _logger.debug("got unrecoverable for %s" % name)
                            toBeClosed = True
                        elif mRet == True:
                            _logger.debug("succeeded for %s" % name)
                            toBeClosed = True
                        else:
                            _logger.debug("failed for %s" % name)                            
                # close dataset
                if toBeClosed:
                    self.proxyLock.acquire()
                    varMap = {}
                    varMap[':vuid'] = vuid
                    varMap[':status'] = 'tobeclosed'
                    taskBuffer.querySQLS("UPDATE ATLAS_PANDA.Datasets SET status=:status,modificationdate=CURRENT_DATE WHERE vuid=:vuid",
                                     varMap)
                    self.proxyLock.release()                    
        except:
            errType,errValue = sys.exc_info()[:2]
            _logger.error("MergerThr failed with %s:%s" % (errType,errValue))
        self.pool.remove(self)
        self.lock.release()


# start merger
mergeLock = threading.Semaphore(1)
mergeProxyLock = threading.Lock()
mergeThreadPool = ThreadPool()
sqlQuery = "type=:type AND status=:status AND (modificationdate BETWEEN :modificationdateL AND :modificationdateU) AND rownum <= 100" 
while True:
    # lock
    mergeLock.acquire()
    # get datasets
    mergeProxyLock.acquire()
    varMap = {}
    varMap[':modificationdateU'] = timeLimitU
    varMap[':modificationdateL'] = timeLimitL    
    varMap[':type']   = 'output'
    varMap[':status'] = 'tobemerged'
    proxyS = taskBuffer.proxyPool.getProxy()
    res = proxyS.getLockDatasets(sqlQuery,varMap)
    taskBuffer.proxyPool.putProxy(proxyS)
    if res == None:
        _logger.debug("# of datasets to be merged: %s" % res)
    else:
        _logger.debug("# of datasets to be merged: %s" % len(res))
    if res==None or len(res)==0:
        mergeProxyLock.release()
        mergeLock.release()
        break
    # release
    mergeProxyLock.release()
    mergeLock.release()
    # run thread
    mergerThr = MergerThr(mergeLock,mergeProxyLock,res,mergeThreadPool)
    mergerThr.start()

mergeThreadPool.join()

_logger.debug("================= end ==================")
