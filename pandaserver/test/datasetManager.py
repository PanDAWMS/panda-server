import os
import re
import sys
import time
import fcntl
import types
import shelve
import random
import datetime
import commands
import threading
import userinterface.Client as Client
from dataservice.DDM import ddm
from dataservice.DDM import dashBorad
from taskbuffer.OraDBProxy import DBProxy
from taskbuffer.TaskBuffer import taskBuffer
from pandalogger.PandaLogger import PandaLogger
from jobdispatcher.Watcher import Watcher
from brokerage.SiteMapper import SiteMapper
from dataservice.Adder import Adder
from dataservice.Finisher import Finisher
from dataservice.MailUtils import MailUtils
from taskbuffer import ProcessGroups
import brokerage.broker_util
import brokerage.broker
import taskbuffer.ErrorCode
import dataservice.DDM

# password
from config import panda_config
passwd = panda_config.dbpasswd

# logger
_logger = PandaLogger().getLogger('datasetManager')

_logger.debug("===================== start =====================")

# use native DQ2
ddm.useDirectDQ2()

# memory checker
def _memoryCheck(str):
    try:
        proc_status = '/proc/%d/status' % os.getpid()
        procfile = open(proc_status)
        name   = ""
        vmSize = ""
        vmRSS  = ""
        # extract Name,VmSize,VmRSS
        for line in procfile:
            if line.startswith("Name:"):
                name = line.split()[-1]
                continue
            if line.startswith("VmSize:"):
                vmSize = ""
                for item in line.split()[1:]:
                    vmSize += item
                continue
            if line.startswith("VmRSS:"):
                vmRSS = ""
                for item in line.split()[1:]:
                    vmRSS += item
                continue
        procfile.close()
        _logger.debug('MemCheck - %s Name=%s VSZ=%s RSS=%s : %s' % (os.getpid(),name,vmSize,vmRSS,str))
    except:
        type, value, traceBack = sys.exc_info()
        _logger.error("memoryCheck() : %s %s" % (type,value))
        _logger.debug('MemCheck - %s unknown : %s' % (os.getpid(),str))
    return

_memoryCheck("start")

# kill old dq2 process
try:
    # time limit
    timeLimit = datetime.datetime.utcnow() - datetime.timedelta(hours=2)
    # get process list
    scriptName = sys.argv[0]
    out = commands.getoutput('ps axo user,pid,lstart,args | grep dq2.clientapi | grep -v PYTHONPATH | grep -v grep')
    for line in out.split('\n'):
        if line == '':
            continue
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
            _logger.debug("old dq2 process : %s %s" % (pid,startTime))
            _logger.debug(line)            
            commands.getoutput('kill -9 %s' % pid)
except:
    type, value, traceBack = sys.exc_info()
    _logger.error("kill dq2 process : %s %s" % (type,value))


# kill old process
try:
    # time limit
    timeLimit = datetime.datetime.utcnow() - datetime.timedelta(hours=7)
    # get process list
    scriptName = sys.argv[0]
    out = commands.getoutput('ps axo user,pid,lstart,args | grep %s' % scriptName)
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
siteMapper = SiteMapper(taskBuffer)


# list with lock
class ListWithLock:
    def __init__(self):
        self.lock = threading.Lock()
        self.list = []

    def __contains__(self,item):
        self.lock.acquire()
        ret = self.list.__contains__(item)
        self.lock.release()
        return ret

    def append(self,item):
        appended = False
        self.lock.acquire()
        if not item in self.list:
            self.list.append(item)
            appended = True
        self.lock.release()
        return appended
    

# list of dis datasets to be deleted
deletedDisList = ListWithLock()


# set tobedeleted to dis dataset 
def setTobeDeletedToDis(subDsName):
    try:
        # only production sub datasets
        if subDsName.startswith('user') or subDsName.startswith('group') or \
               subDsName.startswith('pandaddm_') or re.search('_sub\d+$',subDsName)==None:
            return
        # get _dis names with _sub
        disNameList = taskBuffer.getAssociatedDisDatasets(subDsName)
        _logger.debug("setTobeDeletedToDis : sub:%s has dis:%s" % (subDsName,str(disNameList)))
        # loop over all _dis datasets
        for tmpDisName in disNameList:
            # try to append to locked list
            if not deletedDisList.append(tmpDisName):
                # another thread already took care of the _dis
                continue
            # get dataset
            _logger.debug("setTobeDeletedToDis : try to get %s in DB" % tmpDisName)            
            tmpDS = taskBuffer.queryDatasetWithMap({'name':tmpDisName})
            if tmpDS == None:
                _logger.error("setTobeDeletedToDis : cannot get %s in DB" % tmpDisName)
                continue
            # check status
            if tmpDS.status in ['tobedeleted','deleted']:
                _logger.debug("setTobeDeletedToDis : skip %s since status=%s" % (tmpDisName,tmpDS.status))
                continue
            # check the number of failed jobs associated to the _dis
            if tmpDS.currentfiles != 0:
                _logger.debug("setTobeDeletedToDis : skip %s since nFailed=%s" % (tmpDisName,tmpDS.currentfiles))
                continue
            # update dataset
            tmpDS.status = 'deleting'
            retU = taskBuffer.updateDatasets([tmpDS],withLock=True,withCriteria="status<>:crStatus",
                                             criteriaMap={':crStatus':'deleted'})
            _logger.debug("setTobeDeletedToDis : set deleting to %s with %s" % (tmpDisName,str(retU)))
    except:
        errType,errValue = sys.exc_info()[:2]
        _logger.error("setTobeDeletedToDis : %s %s %s" % (subDsName,errType,errValue))
            

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


# thread to close dataset
class CloserThr (threading.Thread):
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
                _logger.debug("Close %s %s" % (modDate,name))
                if not name.startswith('pandaddm_'):
                    status,out = ddm.DQ2.main('freezeDataset',name)
                else:
                    status,out = 0,''
                if status != 0 and out.find('DQFrozenDatasetException') == -1 and \
                       out.find("DQUnknownDatasetException") == -1 and out.find("DQSecurityException") == -1 and \
                       out.find("DQDeletedDatasetException") == -1 and out.find("DQUnknownDatasetException") == -1:
                    _logger.error(out)
                else:
                    self.proxyLock.acquire()
                    varMap = {}
                    varMap[':vuid'] = vuid
                    varMap[':status'] = 'completed'
                    taskBuffer.querySQLS("UPDATE ATLAS_PANDA.Datasets SET status=:status,modificationdate=CURRENT_DATE WHERE vuid=:vuid",
                                     varMap)
                    self.proxyLock.release()                    
                    if name.startswith('pandaddm_'):
                        continue
                    # set tobedeleted to dis
                    setTobeDeletedToDis(name)
                    # count # of files
                    status,out = ddm.DQ2.main('getNumberOfFiles',name)
                    _logger.debug(out)                                            
                    if status != 0:
                        _logger.error(out)                            
                    else:
                        try:
                            nFile = int(out)
                            _logger.debug(nFile)
                            if nFile == 0:
                                # erase dataset
                                _logger.debug('erase %s' % name)
                                status,out = ddm.DQ2.main('eraseDataset',name)
                                _logger.debug('OK with %s' % name)
                        except:
                            pass
        except:
            pass
        self.pool.remove(self)
        self.lock.release()

# close datasets
_logger.debug("==== close datasets ====")
timeLimitU = datetime.datetime.utcnow() - datetime.timedelta(minutes=30)
timeLimitL = datetime.datetime.utcnow() - datetime.timedelta(days=3)
closeLock = threading.Semaphore(5)
closeProxyLock = threading.Lock()
closeThreadPool = ThreadPool()
maxRows = 100000
while True:
    # lock
    closeLock.acquire()
    # get datasets
    closeProxyLock.acquire()
    varMap = {}
    varMap[':modificationdateU'] = timeLimitU
    varMap[':modificationdateL'] = timeLimitL    
    varMap[':type']   = 'output'
    varMap[':status'] = 'tobeclosed'
    sqlQuery = "type=:type AND status=:status AND (modificationdate BETWEEN :modificationdateL AND :modificationdateU) AND rownum <= %s" % maxRows   
    proxyS = taskBuffer.proxyPool.getProxy()
    res = proxyS.getLockDatasets(sqlQuery,varMap,modTimeOffset='90/24/60')
    taskBuffer.proxyPool.putProxy(proxyS)
    if res == None:
        _logger.debug("# of datasets to be closed: %s" % res)
    else:
        _logger.debug("# of datasets to be closed: %s" % len(res))
    if res==None or len(res)==0:
        closeProxyLock.release()
        closeLock.release()
        break
    # release
    closeProxyLock.release()
    closeLock.release()
    # run thread
    iRows = 0
    nRows = 500
    while iRows < len(res):    
        closerThr = CloserThr(closeLock,closeProxyLock,res[iRows:iRows+nRows],closeThreadPool)
        closerThr.start()
        iRows += nRows
    closeThreadPool.join()
    if len(res) < maxRows:
        break


# thread to freeze dataset
class Freezer (threading.Thread):
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
            for vuid,name,modDate in self.datasets:
                _logger.debug("start %s %s" % (modDate,name))
                self.proxyLock.acquire()
                retF,resF = taskBuffer.querySQLS("SELECT /*+ index(tab FILESTABLE4_DESTDBLOCK_IDX) */ lfn FROM ATLAS_PANDA.filesTable4 tab WHERE destinationDBlock=:destinationDBlock AND NOT status IN (:status1,:status2,:status3)",
                                             {':destinationDBlock':name,':status1':'ready',':status2':'failed',':status3':'skipped'})
                self.proxyLock.release()
                if retF<0:
                    _logger.error("SQL error")
                else:
                    # no files in filesTable
                    if len(resF) == 0:
                        _logger.debug("freeze %s " % name)
                        if not name.startswith('pandaddm_'):
                            status,out = ddm.DQ2.main('freezeDataset',name)
                        else:
                            status,out = 0,''
                        if status != 0 and out.find('DQFrozenDatasetException') == -1 and \
                               out.find("DQUnknownDatasetException") == -1 and out.find("DQSecurityException") == -1 and \
                               out.find("DQDeletedDatasetException") == -1 and out.find("DQUnknownDatasetException") == -1:
                            _logger.error(out)
                        else:
                            self.proxyLock.acquire()
                            varMap = {}
                            varMap[':vuid'] = vuid
                            varMap[':status'] = 'completed' 
                            taskBuffer.querySQLS("UPDATE ATLAS_PANDA.Datasets SET status=:status,modificationdate=CURRENT_DATE WHERE vuid=:vuid",
                                             varMap)
                            self.proxyLock.release()                            
                            if name.startswith('pandaddm_'):
                                continue
                            # set tobedeleted to dis
                            setTobeDeletedToDis(name)
                            # count # of files
                            status,out = ddm.DQ2.main('getNumberOfFiles',name)
                            _logger.debug(out)                                            
                            if status != 0:
                                _logger.error(out)                            
                            else:
                                try:
                                    nFile = int(out)
                                    _logger.debug(nFile)
                                    if nFile == 0:
                                        # erase dataset
                                        _logger.debug('erase %s' % name)                                
                                        status,out = ddm.DQ2.main('eraseDataset',name)
                                        _logger.debug('OK with %s' % name)
                                except:
                                    pass
                    else:
                        _logger.debug("wait %s " % name)
                        self.proxyLock.acquire()                        
                        taskBuffer.querySQLS("UPDATE ATLAS_PANDA.Datasets SET modificationdate=CURRENT_DATE WHERE vuid=:vuid", {':vuid':vuid})
                        self.proxyLock.release()                                                    
                _logger.debug("end %s " % name)
        except:
            pass
        self.pool.remove(self)
        self.lock.release()
                            
# freeze dataset
_logger.debug("==== freeze datasets ====")
timeLimitU = datetime.datetime.utcnow() - datetime.timedelta(days=4)
timeLimitL = datetime.datetime.utcnow() - datetime.timedelta(days=14)
freezeLock = threading.Semaphore(5)
freezeProxyLock = threading.Lock()
freezeThreadPool = ThreadPool()
maxRows = 100000
while True:
    # lock
    freezeLock.acquire()
    # get datasets
    sqlQuery = "type=:type AND status IN (:status1,:status2,:status3,:status4) " + \
               "AND (modificationdate BETWEEN :modificationdateL AND :modificationdateU) AND subType=:subType AND rownum <= %s" % maxRows
    varMap = {}
    varMap[':modificationdateU'] = timeLimitU
    varMap[':modificationdateL'] = timeLimitL    
    varMap[':type'] = 'output'
    varMap[':status1'] = 'running'
    varMap[':status2'] = 'created'
    varMap[':status3'] = 'defined'
    varMap[':status4'] = 'locked'    
    varMap[':subType'] = 'sub'
    freezeProxyLock.acquire()
    proxyS = taskBuffer.proxyPool.getProxy()
    res = proxyS.getLockDatasets(sqlQuery,varMap,modTimeOffset='90/24/60')
    taskBuffer.proxyPool.putProxy(proxyS)
    if res == None:
        _logger.debug("# of datasets to be frozen: %s" % res)
    else:
        _logger.debug("# of datasets to be frozen: %s" % len(res))
    if res==None or len(res)==0:
        freezeProxyLock.release()
        freezeLock.release()
        break
    freezeProxyLock.release()            
    # release
    freezeLock.release()
    # run freezer
    iRows = 0
    nRows = 500
    while iRows < len(res):    
        freezer = Freezer(freezeLock,freezeProxyLock,res[iRows:iRows+nRows],freezeThreadPool)
        freezer.start()
        iRows += nRows
    freezeThreadPool.join()
    if len(res) < maxRows:
        break


# thread to delete dataset replica from T2
class T2Cleaner (threading.Thread):
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
            for vuid,name,modDate in self.datasets:
                _logger.debug("cleanT2 %s" % name)
                # get list of replicas
                status,out = ddm.DQ2.main('listDatasetReplicas',name,0,None,False)
                if status != 0 and out.find('DQFrozenDatasetException')  == -1 and \
                       out.find("DQUnknownDatasetException") == -1 and out.find("DQSecurityException") == -1 and \
                       out.find("DQDeletedDatasetException") == -1 and out.find("DQUnknownDatasetException") == -1:
                    _logger.error(out)
                    continue
                else:
                    if out.find("DQUnknownDatasetException") == -1 and out.find("DQDeletedDatasetException") == -1:
                        listOut = out
                        try:
                            # convert res to map
                            exec "tmpRepSites = %s" % out
                        except:
                            tmpRepSites = {}
                            _logger.error("cannot convert to replica map")
                            _logger.error(out)
                            continue
                        # check cloud
                        cloudName = None
                        for tmpCloudName in siteMapper.getCloudList():
                            t1SiteName = siteMapper.getCloud(tmpCloudName)['source']
                            t1SiteDDMs  = siteMapper.getSite(t1SiteName).setokens.values()
                            for tmpDDM in t1SiteDDMs:
                                # ignore _PRODDISK
                                if tmpDDM.endswith('_PRODDISK'):
                                    continue
                                if tmpRepSites.has_key(tmpDDM):
                                    cloudName = tmpCloudName
                                    break
                            if cloudName != None:
                                break
                        # cloud is not found
                        if cloudName == None:        
                            _logger.error("cannot find cloud for %s : %s" % (name,str(tmpRepSites)))
                        else:
                            # look for T2 IDs
                            t2DDMs = []
                            for tmpDDM in tmpRepSites.keys():
                                if not tmpDDM in t1SiteDDMs and tmpDDM.endswith('_PRODDISK'):
                                    # check home cloud
                                    notDeleteFlag = False
                                    for tmpT2siteID,tmpT2siteSpec in siteMapper.siteSpecList.iteritems():
                                        if tmpT2siteSpec.ddm == tmpDDM:
                                            # not delete if src and dest are in US. OSG is regarded as US due to tier1
                                            if tmpT2siteSpec.cloud in ['US'] and cloudName in ['US','OSG']:
                                                notDeleteFlag = True
                                    if not notDeleteFlag:            
                                        t2DDMs.append(tmpDDM)
                            # delete replica for sub
                            if re.search('_sub\d+$',name) != None and t2DDMs != []:
                                setMetaFlag = True
                                for tmpT2DDM in t2DDMs:
                                    _logger.debug('setReplicaMetaDataAttribute %s %s' % (name,tmpT2DDM))
                                    status,out = ddm.DQ2.main('setReplicaMetaDataAttribute',name,tmpT2DDM,'pin_lifetime','')
                                    if status != 0:
                                        _logger.error(out)
                                        if out.find('DQFrozenDatasetException')  == -1 and \
                                               out.find("DQUnknownDatasetException") == -1 and out.find("DQSecurityException") == -1 and \
                                               out.find("DQDeletedDatasetException") == -1 and out.find("DQUnknownDatasetException") == -1 and \
                                               out.find("No replica found") == -1:
                                            setMetaFlag = False
                                if not setMetaFlag:            
                                    continue
                                _logger.debug(('deleteDatasetReplicas',name,t2DDMs))
                                status,out = ddm.DQ2.main('deleteDatasetReplicas',name,t2DDMs,0,False,False,False,False,False,'00:00:00')
                                if status != 0:
                                    _logger.error(out)
                                    if out.find('DQFrozenDatasetException')  == -1 and \
                                           out.find("DQUnknownDatasetException") == -1 and out.find("DQSecurityException") == -1 and \
                                           out.find("DQDeletedDatasetException") == -1 and out.find("DQUnknownDatasetException") == -1 and \
                                           out.find("No replica found") == -1:
                                        continue
                            else:
                                _logger.debug('no delete for %s due to empty target in %s' % (name,listOut))
                    # update        
                    self.proxyLock.acquire()
                    varMap = {}
                    varMap[':vuid'] = vuid
                    varMap[':status'] = 'completed' 
                    taskBuffer.querySQLS("UPDATE ATLAS_PANDA.Datasets SET status=:status,modificationdate=CURRENT_DATE WHERE vuid=:vuid",
                                         varMap)
                    self.proxyLock.release()                            
                _logger.debug("end %s " % name)
        except:
            pass
        self.pool.remove(self)
        self.lock.release()
                            
# delete dataset replica from T2
_logger.debug("==== delete datasets from T2 ====")
timeLimitU = datetime.datetime.utcnow() - datetime.timedelta(minutes=30)
timeLimitL = datetime.datetime.utcnow() - datetime.timedelta(days=3)
t2cleanLock = threading.Semaphore(5)
t2cleanProxyLock = threading.Lock()
t2cleanThreadPool = ThreadPool()
maxRows = 100000
while True:
    # lock
    t2cleanLock.acquire()
    # get datasets
    varMap = {}
    varMap[':modificationdateU'] = timeLimitU
    varMap[':modificationdateL'] = timeLimitL    
    varMap[':type']   = 'output'
    varMap[':status'] = 'cleanup'
    sqlQuery = "type=:type AND status=:status AND (modificationdate BETWEEN :modificationdateL AND :modificationdateU) AND rownum <= %s" % maxRows   
    t2cleanProxyLock.acquire()
    proxyS = taskBuffer.proxyPool.getProxy()
    res = proxyS.getLockDatasets(sqlQuery,varMap,modTimeOffset='90/24/60')
    taskBuffer.proxyPool.putProxy(proxyS)
    if res == None:
        _logger.debug("# of datasets to be deleted from T2: %s" % res)
    else:
        _logger.debug("# of datasets to be deleted from T2: %s" % len(res))
    if res==None or len(res)==0:
        t2cleanProxyLock.release()
        t2cleanLock.release()
        break
    t2cleanProxyLock.release()            
    # release
    t2cleanLock.release()
    # run t2cleanr
    iRows = 0
    nRows = 500
    while iRows < len(res):
        t2cleanr = T2Cleaner(t2cleanLock,t2cleanProxyLock,res[iRows:iRows+nRows],t2cleanThreadPool)
        t2cleanr.start()
        iRows += nRows
    t2cleanThreadPool.join()
    if len(res) < maxRows:
        break


# delete dis datasets
class EraserThr (threading.Thread):
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
                # only dis datasets
                if re.search('_dis\d+$',name) == None:
                    _logger.error("Eraser : non disDS %s" % name)
                    continue
                # delete
                _logger.debug("Eraser delete dis %s %s" % (modDate,name))
                status,out = ddm.DQ2.main('eraseDataset',name)
                if status != 0 and out.find('DQFrozenDatasetException') == -1 and \
                       out.find("DQUnknownDatasetException") == -1 and out.find("DQSecurityException") == -1 and \
                       out.find("DQDeletedDatasetException") == -1 and out.find("DQUnknownDatasetException") == -1:
                    _logger.error(out)
                    continue
                _logger.debug('OK with %s' % name)
                # update
                self.proxyLock.acquire()
                varMap = {}
                varMap[':vuid'] = vuid
                varMap[':status'] = 'deleted'
                taskBuffer.querySQLS("UPDATE ATLAS_PANDA.Datasets SET status=:status,modificationdate=CURRENT_DATE WHERE vuid=:vuid",
                                     varMap)
                self.proxyLock.release()
        except:
            pass
        self.pool.remove(self)
        self.lock.release()

# delete dis datasets
_logger.debug("==== delete dis datasets ====")
timeLimitU = datetime.datetime.utcnow() - datetime.timedelta(minutes=30)
timeLimitL = datetime.datetime.utcnow() - datetime.timedelta(days=3)
disEraseLock = threading.Semaphore(5)
disEraseProxyLock = threading.Lock()
disEraseThreadPool = ThreadPool()
maxRows = 100000
while True:
    # lock
    disEraseLock.acquire()
    # get datasets
    varMap = {}
    varMap[':modificationdateU'] = timeLimitU
    varMap[':modificationdateL'] = timeLimitL    
    varMap[':type']   = 'dispatch'
    varMap[':status'] = 'deleting'
    sqlQuery = "type=:type AND status=:status AND (modificationdate BETWEEN :modificationdateL AND :modificationdateU) AND rownum <= %s" % maxRows     
    disEraseProxyLock.acquire()
    proxyS = taskBuffer.proxyPool.getProxy()
    res = proxyS.getLockDatasets(sqlQuery,varMap,modTimeOffset='90/24/60')
    taskBuffer.proxyPool.putProxy(proxyS)
    if res == None:
        _logger.debug("# of dis datasets to be deleted: %s" % res)
    else:
        _logger.debug("# of dis datasets to be deleted: %s" % len(res))
    if res==None or len(res)==0:
        disEraseProxyLock.release()
        disEraseLock.release()
        break
    disEraseProxyLock.release()            
    # release
    disEraseLock.release()
    # run disEraser
    iRows = 0
    nRows = 500
    while iRows < len(res):        
        disEraser = EraserThr(disEraseLock,disEraseProxyLock,res[iRows:iRows+nRows],disEraseThreadPool)
        disEraser.start()
        iRows += nRows
    disEraseThreadPool.join()
    if len(res) < maxRows:
        break

_memoryCheck("finisher")

# finisher thread
class FinisherThr (threading.Thread):
    def __init__(self,lock,proxyLock,ids,pool,timeNow):
        threading.Thread.__init__(self)
        self.ids        = ids
        self.lock       = lock
        self.proxyLock  = proxyLock
        self.pool       = pool
        self.timeNow    = timeNow
        self.pool.add(self)
                                        
    def run(self):
        self.lock.acquire()
        try:
            # get jobs from DB
            ids = self.ids
            self.proxyLock.acquire()
            jobs = taskBuffer.peekJobs(ids,fromDefined=False,fromArchived=False,fromWaiting=False)
            self.proxyLock.release()
            upJobs = []
            finJobs = []
            for job in jobs:
                if job == None or job.jobStatus == 'unknown':
                    continue
                # use BNL by default
                dq2URL = siteMapper.getSite('BNL_ATLAS_1').dq2url
                dq2SE  = []
                # get LFC and SEs
                if job.prodSourceLabel == 'user' and not siteMapper.siteSpecList.has_key(job.destinationSE):
                    # using --destSE for analysis job to transfer output
                    try:
                        dq2URL = dataservice.DDM.toa.getLocalCatalog(job.destinationSE)[-1]
                        match = re.search('.+://([^:/]+):*\d*/*',dataservice.DDM.toa.getSiteProperty(job.destinationSE,'srm')[-1])
                        if match != None:
                            dq2SE.append(match.group(1))
                    except:
                        type, value, traceBack = sys.exc_info()
                        _logger.error("%s Failed to get DQ2/SE with %s %s" % (job.PandaID,type,value))
                        continue
                elif siteMapper.checkCloud(job.cloud):
                    # normal production jobs
                    tmpDstID   = siteMapper.getCloud(job.cloud)['dest']
                    tmpDstSite = siteMapper.getSite(tmpDstID)
                    if not tmpDstSite.lfchost in [None,'']:
                        # LFC
                        dq2URL = 'lfc://'+tmpDstSite.lfchost+':/grid/atlas/'
                        if tmpDstSite.se != None:
                            for tmpDstSiteSE in tmpDstSite.se.split(','):
                                match = re.search('.+://([^:/]+):*\d*/*',tmpDstSiteSE)
                                if match != None:
                                    dq2SE.append(match.group(1))
                    else:
                        # LRC
                        dq2URL = tmpDstSite.dq2url
                        dq2SE  = []
                # get LFN list
                lfns  = []
                guids = []
                nTokens = 0
                for file in job.Files:
                    # only output files are checked
                    if file.type == 'output' or file.type == 'log':
                        lfns.append(file.lfn)
                        guids.append(file.GUID)
                        nTokens += len(file.destinationDBlockToken.split(','))
                # get files in LRC
                _logger.debug("%s Cloud:%s DQ2URL:%s" % (job.PandaID,job.cloud,dq2URL))
                okFiles = brokerage.broker_util.getFilesFromLRC(lfns,dq2URL,guids,dq2SE,getPFN=True)
                # count files
                nOkTokens = 0
                for okLFN,okPFNs in okFiles.iteritems():
                    nOkTokens += len(okPFNs)
                # check all files are ready    
                _logger.debug("%s nToken:%s nOkToken:%s" % (job.PandaID,nTokens,nOkTokens))
                if nTokens <= nOkTokens:
                    _logger.debug("%s Finisher : Finish" % job.PandaID)
                    for file in job.Files:
                        if file.type == 'output' or file.type == 'log':
                            file.status = 'ready'
                    # append to run Finisher
                    finJobs.append(job)                        
                else:
                    endTime = job.endTime
                    if endTime == 'NULL':
                        endTime = job.startTime
                    # priority-dependent timeout
                    tmpCloudSpec = siteMapper.getCloud(job.cloud)
                    if job.currentPriority >= 800 and (not job.prodSourceLabel in ['user']):
                        if tmpCloudSpec.has_key('transtimehi'):
                            timeOutValue = tmpCloudSpec['transtimehi']
                        else:
                            timeOutValue = 1
                    else:
                        if tmpCloudSpec.has_key('transtimelo'):                    
                            timeOutValue = tmpCloudSpec['transtimelo']
                        else:
                            timeOutValue = 2                        
                    # protection
                    if timeOutValue < 1:
                        timeOutValue  = 1
                    timeOut = self.timeNow - datetime.timedelta(days=timeOutValue)
                    _logger.debug("%s  Priority:%s Limit:%s End:%s" % (job.PandaID,job.currentPriority,str(timeOut),str(endTime)))
                    if endTime < timeOut:
                        # timeout
                        _logger.debug("%s Finisher : Kill" % job.PandaID)
                        strMiss = ''
                        for lfn in lfns:
                            if not lfn in okFiles:
                                strMiss += ' %s' % lfn
                        job.jobStatus = 'failed'
                        job.taskBufferErrorCode = taskbuffer.ErrorCode.EC_Transfer
                        job.taskBufferErrorDiag = 'transfer timeout for '+strMiss
                        guidMap = {}
                        for file in job.Files:
                            # set file status
                            if file.status == 'transferring':
                                file.status = 'failed'
                            # collect GUIDs to delete files from _tid datasets
                            if file.type == 'output' or file.type == 'log':
                                if not guidMap.has_key(file.destinationDBlock):
                                    guidMap[file.destinationDBlock] = []
                                guidMap[file.destinationDBlock].append(file.GUID)
                    else:
                        # wait
                        _logger.debug("%s Finisher : Wait" % job.PandaID)
                        for lfn in lfns:
                            if not lfn in okFiles:
                                _logger.debug("%s    -> %s" % (job.PandaID,lfn))
                upJobs.append(job)
            # update
            _logger.debug("updating ...")
            self.proxyLock.acquire()
            taskBuffer.updateJobs(upJobs,False)
            self.proxyLock.release()
            # run Finisher
            for job in finJobs:
                fThr = Finisher(taskBuffer,None,job)
                fThr.start()
                fThr.join()
            _logger.debug("done")
            time.sleep(1)
        except:
            pass
        self.pool.remove(self)
        self.lock.release()

# finish transferring jobs
_logger.debug("==== finish transferring jobs ====")
finisherLock = threading.Semaphore(3)
finisherProxyLock = threading.Lock()
finisherThreadPool = ThreadPool()
for loopIdx in ['low','high']:
    timeNow = datetime.datetime.utcnow()
    if loopIdx == 'high':
        highPrioFlag = True
    else:
        highPrioFlag = False
    # get jobs
    for ii in range(1000):
        # lock
        finisherLock.acquire()
        finisherProxyLock.acquire()
        ret,res = taskBuffer.lockJobsForFinisher(timeNow,200,highPrioFlag)
        finisherProxyLock.release()
        finisherLock.release()
        if res == None:
            _logger.debug("# of jobs to be finished for %s : %s" % (loopIdx,res))
        else:
            _logger.debug("# of jobs to be finished for %s : %s" % (loopIdx,len(res)))
        if res == None or len(res) == 0:
            break
        # run thread
        finThr = FinisherThr(finisherLock,finisherProxyLock,res,finisherThreadPool,timeNow)
        finThr.start()
    # wait
    finisherThreadPool.join()


_memoryCheck("end")

_logger.debug("===================== end =====================")
