import os
import re
import sys
import time
import datetime
import traceback
import threading

import pandaserver.taskbuffer.ErrorCode
import pandaserver.taskbuffer.ErrorCode
from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandaserver.config import panda_config
from pandaserver.dataservice.DDM import rucioAPI
from pandaserver.taskbuffer import EventServiceUtils
from pandaserver.brokerage.SiteMapper import SiteMapper
from pandaserver.dataservice.Finisher import Finisher
from pandaserver.dataservice import DataServiceUtils
from pandaserver.dataservice.DataServiceUtils import select_scope
from pandaserver.dataservice.Closer import Closer
# from pandaserver.srvcore.CoreUtils import commands_get_status_output


# logger
_logger = PandaLogger().getLogger('datasetManager')


# main
def main(tbuf=None, **kwargs):

    _logger.debug("===================== start =====================")

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
        except Exception:
            type, value, traceBack = sys.exc_info()
            _logger.error("memoryCheck() : %s %s" % (type,value))
            _logger.debug('MemCheck - %s unknown : %s' % (os.getpid(),str))
        return

    _memoryCheck("start")

    # # kill old dq2 process
    # try:
    #     # time limit
    #     timeLimit = datetime.datetime.utcnow() - datetime.timedelta(hours=2)
    #     # get process list
    #     scriptName = sys.argv[0]
    #     out = commands_get_status_output(
    #         'ps axo user,pid,lstart,args | grep dq2.clientapi | grep -v PYTHONPATH | grep -v grep')[-1]
    #     for line in out.split('\n'):
    #         if line == '':
    #             continue
    #         items = line.split()
    #         # owned process
    #         if items[0] not in ['sm','atlpan','pansrv','root']: # ['os.getlogin()']: doesn't work in cron
    #             continue
    #         # look for python
    #         if re.search('python',line) is None:
    #             continue
    #         # PID
    #         pid = items[1]
    #         # start time
    #         timeM = re.search('(\S+\s+\d+ \d+:\d+:\d+ \d+)',line)
    #         startTime = datetime.datetime(*time.strptime(timeM.group(1),'%b %d %H:%M:%S %Y')[:6])
    #         # kill old process
    #         if startTime < timeLimit:
    #             _logger.debug("old dq2 process : %s %s" % (pid,startTime))
    #             _logger.debug(line)
    #             commands_get_status_output('kill -9 %s' % pid)
    # except Exception:
    #     type, value, traceBack = sys.exc_info()
    #     _logger.error("kill dq2 process : %s %s" % (type,value))
    #
    #
    # # kill old process
    # try:
    #     # time limit
    #     timeLimit = datetime.datetime.utcnow() - datetime.timedelta(hours=7)
    #     # get process list
    #     scriptName = sys.argv[0]
    #     out = commands_get_status_output('ps axo user,pid,lstart,args | grep %s' % scriptName)[-1]
    #     for line in out.split('\n'):
    #         items = line.split()
    #         # owned process
    #         if items[0] not in ['sm','atlpan','pansrv','root']: # ['os.getlogin()']: doesn't work in cron
    #             continue
    #         # look for python
    #         if re.search('python',line) is None:
    #             continue
    #         # PID
    #         pid = items[1]
    #         # start time
    #         timeM = re.search('(\S+\s+\d+ \d+:\d+:\d+ \d+)',line)
    #         startTime = datetime.datetime(*time.strptime(timeM.group(1),'%b %d %H:%M:%S %Y')[:6])
    #         # kill old process
    #         if startTime < timeLimit:
    #             _logger.debug("old process : %s %s" % (pid,startTime))
    #             _logger.debug(line)
    #             commands_get_status_output('kill -9 %s' % pid)
    # except Exception:
    #     type, value, traceBack = sys.exc_info()
    #     _logger.error("kill process : %s %s" % (type,value))


    # instantiate TB
    # if tbuf is None:
    from pandaserver.taskbuffer.TaskBuffer import taskBuffer
    taskBuffer.init(panda_config.dbhost,panda_config.dbpasswd,nDBConnection=1, useTimeout=True)
    # else:
    #     taskBuffer = tbuf

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
            if item not in self.list:
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
                   subDsName.startswith('pandaddm_') or re.search('_sub\d+$',subDsName) is None:
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
                # skip non _dis
                if re.search('_dis\d+$',tmpDisName) is None:
                    continue
                # get dataset
                _logger.debug("setTobeDeletedToDis : try to get %s in DB" % tmpDisName)
                tmpDS = taskBuffer.queryDatasetWithMap({'name':tmpDisName})
                if tmpDS is None:
                    _logger.error("setTobeDeletedToDis : cannot get %s in DB" % tmpDisName)
                    continue
                # check status
                if tmpDS.status in ['tobedeleted','deleted']:
                    _logger.debug("setTobeDeletedToDis : skip %s since status=%s" % (tmpDisName,tmpDS.status))
                    continue
                # check the number of failed jobs associated to the _dis
                if tmpDS.currentfiles == 0:
                    # all succeeded
                    tmpDS.status = 'deleting'
                    excStatus    = 'deleted'
                else:
                    # some failed, to reduce the lifetime
                    tmpDS.status = 'shortening'
                    excStatus    = 'shortened'
                # update dataset
                retU = taskBuffer.updateDatasets([tmpDS],withLock=True,withCriteria="status<>:crStatus",
                                                 criteriaMap={':crStatus':excStatus})
                _logger.debug("setTobeDeletedToDis : set %s to %s with %s" % (tmpDS.status,tmpDisName,str(retU)))
        except Exception:
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
                    dsExists = True
                    if name.startswith('pandaddm_') or name.startswith('user.') or name.startswith('group.') \
                            or name.startswith('hc_test.') or name.startswith('panda.um.'):
                        dsExists = False
                    if dsExists:
                        # check if dataset exists
                        status,out = rucioAPI.getMetaData(name)
                        if status is True:
                            if out is not None:
                                try:
                                    rucioAPI.closeDataset(name)
                                    status = True
                                except Exception:
                                    errtype,errvalue = sys.exc_info()[:2]
                                    out = 'failed to freeze : {0} {1}'.format(errtype,errvalue)
                                    status = False
                            else:
                                # dataset not exist
                                status,out = True,''
                                dsExists = False
                    else:
                        status,out = True,''
                    if not status:
                        _logger.error('{0} failed to close with {1}'.format(name,out))
                    else:
                        self.proxyLock.acquire()
                        varMap = {}
                        varMap[':vuid'] = vuid
                        varMap[':newstatus'] = 'completed'
                        varMap[':oldstatus'] = 'tobeclosed'
                        taskBuffer.querySQLS("UPDATE ATLAS_PANDA.Datasets SET status=:newstatus,modificationdate=CURRENT_DATE WHERE vuid=:vuid AND status=:oldstatus",
                                         varMap)
                        self.proxyLock.release()
                        # set tobedeleted to dis
                        setTobeDeletedToDis(name)
                        # skip if dataset is not real
                        if not dsExists:
                            continue
                        # count # of files
                        status,out = rucioAPI.getNumberOfFiles(name)
                        if status is not True:
                            if status is False:
                                _logger.error(out)
                        else:
                            _logger.debug(out)
                            try:
                                nFile = int(out)
                                if nFile == 0:
                                    # erase dataset
                                    _logger.debug('erase %s' % name)
                                    status,out = rucioAPI.eraseDataset(name)
                                    _logger.debug('OK with %s' % name)
                            except Exception:
                                pass
            except Exception:
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
        res = taskBuffer.getLockDatasets(sqlQuery,varMap,modTimeOffset='90/24/60')
        if res is None:
            _logger.debug("# of datasets to be closed: %s" % res)
        else:
            _logger.debug("# of datasets to be closed: %s" % len(res))
        if res is None or len(res)==0:
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
                    _logger.debug("Freezer start %s %s" % (modDate,name))
                    self.proxyLock.acquire()
                    retF,resF = taskBuffer.querySQLS("SELECT /*+ index(tab FILESTABLE4_DESTDBLOCK_IDX) */ PandaID,status FROM ATLAS_PANDA.filesTable4 tab WHERE destinationDBlock=:destinationDBlock ",
                                                 {':destinationDBlock':name})
                    self.proxyLock.release()
                    if isinstance(retF, int) and retF < 0:
                        _logger.error("SQL error")
                    else:
                        allFinished = True
                        onePandaID = None
                        for tmpPandaID,tmpFileStatus in resF:
                            onePandaID = tmpPandaID
                            if tmpFileStatus not in ['ready', 'failed', 'skipped', 'merging', 'finished']:
                                allFinished = False
                                break
                        # check sub datasets in the jobset for event service job
                        if allFinished:
                            self.proxyLock.acquire()
                            tmpJobs = taskBuffer.getFullJobStatus([onePandaID])
                            self.proxyLock.release()
                            if len(tmpJobs) > 0 and tmpJobs[0] is not None:
                                if EventServiceUtils.isEventServiceMerge(tmpJobs[0]):
                                    self.proxyLock.acquire()
                                    cThr = Closer(taskBuffer, [], tmpJobs[0])
                                    allFinished = cThr.checkSubDatasetsInJobset()
                                    self.proxyLock.release()
                                    _logger.debug("closer checked sub datasets in the jobset for %s : %s" % (name, allFinished))
                        # no files in filesTable
                        if allFinished:
                            _logger.debug("freeze %s " % name)
                            dsExists = True
                            if name.startswith('pandaddm_') or name.startswith('user.') or name.startswith('group.') \
                                    or name.startswith('hc_test.') or name.startswith('panda.um.'):
                                dsExists = False
                            if name.startswith('panda.um.'):
                                self.proxyLock.acquire()
                                retMer,resMer = taskBuffer.querySQLS("SELECT /*+ index(tab FILESTABLE4_DESTDBLOCK_IDX) */ PandaID FROM ATLAS_PANDA.filesTable4 tab WHERE destinationDBlock=:destinationDBlock AND status IN (:statusM,:statusF) ",
                                                                     {':destinationDBlock':name,
                                                                      ':statusM':'merging',
                                                                      ':statusF':'failed'})
                                self.proxyLock.release()
                                if resMer is not None and len(resMer)>0:
                                    mergeID = resMer[0][0]
                                    # get merging jobs
                                    self.proxyLock.acquire()
                                    mergingJobs = taskBuffer.peekJobs([mergeID],fromDefined=False,fromArchived=False,fromWaiting=False)
                                    self.proxyLock.release()
                                    mergeJob = mergingJobs[0]
                                    if mergeJob is not None:
                                        tmpDestDBlocks = []
                                        # get destDBlock
                                        for tmpFile in mergeJob.Files:
                                            if tmpFile.type in ['output','log']:
                                                if tmpFile.destinationDBlock not in tmpDestDBlocks:
                                                    tmpDestDBlocks.append(tmpFile.destinationDBlock)
                                        # run
                                        _logger.debug("start JEDI closer for %s " % name)
                                        self.proxyLock.acquire()
                                        cThr = Closer(taskBuffer,tmpDestDBlocks,mergeJob)
                                        cThr.start()
                                        cThr.join()
                                        self.proxyLock.release()
                                        _logger.debug("end JEDI closer for %s " % name)
                                        continue
                                    else:
                                        _logger.debug("failed to get merging job for %s " % name)
                                else:
                                    _logger.debug("failed to get merging file for %s " % name)
                                status,out = True,''
                            elif dsExists:
                                # check if dataset exists
                                status,out = rucioAPI.getMetaData(name)
                                if status is True:
                                    if out is not None:
                                        try:
                                            rucioAPI.closeDataset(name)
                                            status = True
                                        except Exception:
                                            errtype,errvalue = sys.exc_info()[:2]
                                            out = 'failed to freeze : {0} {1}'.format(errtype,errvalue)
                                            status = False
                                    else:
                                        # dataset not exist
                                        status,out = True,''
                                        dsExists = False
                            else:
                                status,out = True,''
                            if not status:
                                _logger.error('{0} failed to freeze with {1}'.format(name,out))
                            else:
                                self.proxyLock.acquire()
                                varMap = {}
                                varMap[':vuid'] = vuid
                                varMap[':status'] = 'completed'
                                taskBuffer.querySQLS("UPDATE ATLAS_PANDA.Datasets SET status=:status,modificationdate=CURRENT_DATE WHERE vuid=:vuid",
                                                 varMap)
                                self.proxyLock.release()
                                if name.startswith('pandaddm_') or name.startswith('panda.um.') or not dsExists:
                                    continue
                                # set tobedeleted to dis
                                setTobeDeletedToDis(name)
                                # count # of files
                                status,out = rucioAPI.getNumberOfFiles(name)
                                if status is not True:
                                    if status is False:
                                        _logger.error(out)
                                else:
                                    _logger.debug(out)
                                    try:
                                        nFile = int(out)
                                        _logger.debug(nFile)
                                        if nFile == 0:
                                            # erase dataset
                                            _logger.debug('erase %s' % name)
                                            status,out = rucioAPI.eraseDataset(name)
                                            _logger.debug('OK with %s' % name)
                                    except Exception:
                                        pass
                        else:
                            _logger.debug("wait %s " % name)
                            self.proxyLock.acquire()
                            taskBuffer.querySQLS("UPDATE ATLAS_PANDA.Datasets SET modificationdate=CURRENT_DATE WHERE vuid=:vuid", {':vuid':vuid})
                            self.proxyLock.release()
                    _logger.debug("end %s " % name)
            except Exception:
                errStr = traceback.format_exc()
                _logger.error(errStr)
            self.pool.remove(self)
            self.lock.release()

    # freeze dataset
    _logger.debug("==== freeze datasets ====")
    timeLimitRU = datetime.datetime.utcnow() - datetime.timedelta(hours=3)
    timeLimitRL = datetime.datetime.utcnow() - datetime.timedelta(hours=12)
    timeLimitU = datetime.datetime.utcnow() - datetime.timedelta(hours=6)
    timeLimitL = datetime.datetime.utcnow() - datetime.timedelta(days=14)
    # reset doing so that Closer can update unmerged datasets
    sql  = "SELECT name FROM ATLAS_PANDA.Datasets "
    sql += "WHERE type=:type AND (modificationdate BETWEEN :modificationdateRL AND :modificationdateRU) AND subType=:subType AND status=:oldStatus "
    varMap = {}
    varMap[':modificationdateRU'] = timeLimitRU
    varMap[':modificationdateRL'] = timeLimitRL
    varMap[':type'] = 'output'
    varMap[':subType'] = 'sub'
    varMap[':oldStatus'] = 'doing'
    retReset,resReset = taskBuffer.querySQLS(sql,varMap)
    sql = "UPDATE ATLAS_PANDA.Datasets SET status=:newStatus,modificationdate=:modificationdateU WHERE name=:name AND status=:oldStatus "
    if resReset is not None:
        for name, in resReset:
            varMap = {}
            varMap[':name'] = name
            varMap[':oldStatus'] = 'doing'
            varMap[':newStatus'] = 'running'
            varMap[':modificationdateU'] = timeLimitU
            _logger.debug("reset {0} to freeze".format(name))
            taskBuffer.querySQLS(sql,varMap)
    # loop for freezer
    freezeLock = threading.Semaphore(5)
    freezeProxyLock = threading.Lock()
    freezeThreadPool = ThreadPool()
    maxRows = 100000
    while True:
        # lock
        freezeLock.acquire()
        # get datasets
        sqlQuery = "type=:type AND status IN (:status1,:status2,:status3,:status4,:status5) " + \
                   "AND (modificationdate BETWEEN :modificationdateL AND :modificationdateU) AND subType=:subType AND rownum <= %s" % maxRows
        varMap = {}
        varMap[':modificationdateU'] = timeLimitU
        varMap[':modificationdateL'] = timeLimitL
        varMap[':type'] = 'output'
        varMap[':status1'] = 'running'
        varMap[':status2'] = 'created'
        varMap[':status3'] = 'defined'
        varMap[':status4'] = 'locked'
        varMap[':status5'] = 'doing'
        varMap[':subType'] = 'sub'
        freezeProxyLock.acquire()
        res = taskBuffer.getLockDatasets(sqlQuery,varMap,modTimeOffset='90/24/60')
        if res is None:
            _logger.debug("# of datasets to be frozen: %s" % res)
        else:
            _logger.debug("# of datasets to be frozen: %s" % len(res))
        if res is None or len(res)==0:
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

    # delete dis datasets
    class EraserThr (threading.Thread):
        def __init__(self,lock,proxyLock,datasets,pool,operationType):
            threading.Thread.__init__(self)
            self.datasets   = datasets
            self.lock       = lock
            self.proxyLock  = proxyLock
            self.pool       = pool
            self.pool.add(self)
            self.operationType = operationType

        def run(self):
            self.lock.acquire()
            try:
                # loop over all datasets
                for vuid,name,modDate in self.datasets:
                    # only dis datasets
                    if re.search('_dis\d+$',name) is None:
                        _logger.error("Eraser : non disDS %s" % name)
                        continue
                    # delete
                    _logger.debug("Eraser %s dis %s %s" % (self.operationType,modDate,name))
                    # delete or shorten
                    endStatus = 'deleted'
                    status,out = rucioAPI.eraseDataset(name)
                    if not status:
                        _logger.error(out)
                        continue
                    _logger.debug('OK with %s' % name)
                    # update
                    self.proxyLock.acquire()
                    varMap = {}
                    varMap[':vuid'] = vuid
                    varMap[':status'] = endStatus
                    taskBuffer.querySQLS("UPDATE ATLAS_PANDA.Datasets SET status=:status,modificationdate=CURRENT_DATE WHERE vuid=:vuid",
                                         varMap)
                    self.proxyLock.release()
            except Exception:
                errStr = traceback.format_exc()
                _logger.error(errStr)
            self.pool.remove(self)
            self.lock.release()

    # delete dis datasets
    _logger.debug("==== delete dis datasets ====")
    timeLimitU = datetime.datetime.utcnow() - datetime.timedelta(minutes=30)
    timeLimitL = datetime.datetime.utcnow() - datetime.timedelta(days=3)
    disEraseLock = threading.Semaphore(5)
    disEraseProxyLock = threading.Lock()
    disEraseThreadPool = ThreadPool()
    #maxRows = 100000
    maxRows = 5000
    for targetStatus in ['deleting','shortening']:
        for i in range(10):
            # lock
            disEraseLock.acquire()
            # get datasets
            varMap = {}
            varMap[':modificationdateU'] = timeLimitU
            varMap[':modificationdateL'] = timeLimitL
            varMap[':type']   = 'dispatch'
            varMap[':status'] = targetStatus
            sqlQuery = "type=:type AND status=:status AND (modificationdate BETWEEN :modificationdateL AND :modificationdateU) AND rownum <= %s" % maxRows
            disEraseProxyLock.acquire()
            res = taskBuffer.getLockDatasets(sqlQuery,varMap,modTimeOffset='90/24/60')
            if res is None:
                _logger.debug("# of dis datasets for %s: None" % targetStatus)
            else:
                _logger.debug("# of dis datasets for %s: %s" % (targetStatus,len(res)))
            if res is None or len(res)==0:
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
                disEraser = EraserThr(disEraseLock,disEraseProxyLock,res[iRows:iRows+nRows],
                                      disEraseThreadPool,targetStatus)
                disEraser.start()
                iRows += nRows
            disEraseThreadPool.join()
            if len(res) < 100:
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
                    if job is None or job.jobStatus == 'unknown':
                        continue
                    seList = ['dummy']
                    tmpNucleus = siteMapper.getNucleus(job.nucleus)
                    # get SEs
                    if job.prodSourceLabel == 'user' and job.destinationSE not in siteMapper.siteSpecList:
                        # using --destSE for analysis job to transfer output
                        seList = [job.destinationSE]
                    elif tmpNucleus is not None:
                        seList = list(tmpNucleus.allDdmEndPoints)
                    elif siteMapper.checkCloud(job.cloud):
                        # normal production jobs
                        if DataServiceUtils.checkJobDestinationSE(job) is None:
                            tmpDstID = siteMapper.getCloud(job.cloud)['dest']
                        else:
                            tmpDstID = job.destinationSE
                        tmpDstSite = siteMapper.getSite(tmpDstID)
                        scope_input, scope_output = select_scope(tmpDstSite, job.prodSourceLabel)
                        seList = tmpDstSite.ddm_endpoints_output[scope_output].getLocalEndPoints()
                    # get LFN list
                    lfns   = []
                    guids  = []
                    scopes = []
                    nTokens = 0
                    for file in job.Files:
                        # only output files are checked
                        if file.type == 'output' or file.type == 'log':
                            if file.status == 'nooutput':
                                continue
                            if DataServiceUtils.getDistributedDestination(file.destinationDBlockToken) is not None:
                                continue
                            lfns.append(file.lfn)
                            guids.append(file.GUID)
                            scopes.append(file.scope)
                            nTokens += len(file.destinationDBlockToken.split(','))
                    # get files in LRC
                    _logger.debug("%s Cloud:%s" % (job.PandaID,job.cloud))
                    tmpStat,okFiles = rucioAPI.listFileReplicas(scopes,lfns,seList)
                    if not tmpStat:
                        _logger.error("%s failed to get file replicas" % job.PandaID)
                        okFiles = {}
                    # count files
                    nOkTokens = 0
                    for okLFN in okFiles:
                        okSEs = okFiles[okLFN]
                        nOkTokens += len(okSEs)
                    # check all files are ready
                    _logger.debug("%s nToken:%s nOkToken:%s" % (job.PandaID,nTokens,nOkTokens))
                    if nTokens <= nOkTokens:
                        _logger.debug("%s Finisher : Finish" % job.PandaID)
                        for file in job.Files:
                            if file.type == 'output' or file.type == 'log':
                                if file.status != 'nooutput':
                                    file.status = 'ready'
                        # append to run Finisher
                        finJobs.append(job)
                    else:
                        endTime = job.endTime
                        if endTime == 'NULL':
                            endTime = job.startTime
                        # priority-dependent timeout
                        tmpCloudSpec = siteMapper.getCloud(job.cloud)
                        if job.currentPriority >= 800 and (job.prodSourceLabel not in ['user']):
                            if 'transtimehi' in tmpCloudSpec:
                                timeOutValue = tmpCloudSpec['transtimehi']
                            else:
                                timeOutValue = 1
                        else:
                            if 'transtimelo' in tmpCloudSpec:
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
                                if lfn not in okFiles:
                                    strMiss += ' %s' % lfn
                            job.jobStatus = 'failed'
                            job.taskBufferErrorCode = pandaserver.taskbuffer.ErrorCode.EC_Transfer
                            job.taskBufferErrorDiag = 'transfer timeout for '+strMiss
                            guidMap = {}
                            for file in job.Files:
                                # set file status
                                if file.status == 'transferring' or file.type in ['log','output']:
                                    file.status = 'failed'
                                # collect GUIDs to delete files from _tid datasets
                                if file.type == 'output' or file.type == 'log':
                                    if file.destinationDBlock not in guidMap:
                                        guidMap[file.destinationDBlock] = []
                                    guidMap[file.destinationDBlock].append(file.GUID)
                        else:
                            # wait
                            _logger.debug("%s Finisher : Wait" % job.PandaID)
                            for lfn in lfns:
                                if lfn not in okFiles:
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
            except Exception:
                errtype,errvalue = sys.exc_info()[:2]
                errStr  = "FinisherThr failed with %s %s" % (errtype,errvalue)
                errStr += traceback.format_exc()
                _logger.error(errStr)
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
            if res is None:
                _logger.debug("# of jobs to be finished for %s : %s" % (loopIdx,res))
            else:
                _logger.debug("# of jobs to be finished for %s : %s" % (loopIdx,len(res)))
            if res is None or len(res) == 0:
                break
            # run thread
            finThr = FinisherThr(finisherLock,finisherProxyLock,res,finisherThreadPool,timeNow)
            finThr.start()
        # wait
        finisherThreadPool.join()


    # activator thread
    class ActivatorThr (threading.Thread):
        def __init__(self,lock,proxyLock,ids,pool):
            threading.Thread.__init__(self)
            self.ids        = ids
            self.lock       = lock
            self.proxyLock  = proxyLock
            self.pool       = pool
            self.pool.add(self)

        def run(self):
            self.lock.acquire()
            try:
                # get jobs from DB
                ids = self.ids
                self.proxyLock.acquire()
                jobs = taskBuffer.peekJobs(ids,fromActive=False,fromArchived=False,fromWaiting=False)
                self.proxyLock.release()
                actJobs = []
                for tmpJob in jobs:
                    if tmpJob is None or tmpJob.jobStatus == 'unknown':
                        continue
                    # get LFN list
                    lfns   = []
                    guids  = []
                    scopes = []
                    for tmpFile in tmpJob.Files:
                        # only input files are checked
                        if tmpFile.type == 'input' and tmpFile.status != 'ready':
                            lfns.append(tmpFile.lfn)
                            scopes.append(tmpFile.scope)
                    # get file replicas
                    _logger.debug("%s check input files at %s" % (tmpJob.PandaID, tmpJob.computingSite))
                    tmpStat,okFiles = rucioAPI.listFileReplicas(scopes,lfns)
                    if not tmpStat:
                        pass
                    else:
                        # check if locally available
                        siteSpec = siteMapper.getSite(tmpJob.computingSite)
                        scope_input, scope_output = select_scope(siteSpec, tmpJob.prodSourceLabel, tmpJob.job_label)
                        allOK = True
                        for tmpFile in tmpJob.Files:
                            # only input
                            if tmpFile.type == 'input' and tmpFile.status != 'ready':
                                # check RSEs
                                if tmpFile.lfn in okFiles:
                                    for rse in okFiles[tmpFile.lfn]:
                                        if siteSpec.ddm_endpoints_input[scope_input].isAssociated(rse) and \
                                                siteSpec.ddm_endpoints_input[scope_input].getEndPoint(rse)['is_tape'] == 'N':
                                            tmpFile.status = 'ready'
                                            break
                                # missing
                                if tmpFile.status != 'ready':
                                    allOK = False
                                    _logger.debug("%s skip since %s:%s is missing" % (tmpJob.PandaID,tmpFile.scope,tmpFile.lfn))
                                    break
                        if not allOK:
                            continue
                        # append to run activator
                        _logger.debug("%s to activate" % tmpJob.PandaID)
                        actJobs.append(tmpJob)
                # update
                _logger.debug("activating ...")
                self.proxyLock.acquire()
                taskBuffer.activateJobs(actJobs)
                self.proxyLock.release()
                _logger.debug("done")
                time.sleep(1)
            except Exception:
                errtype,errvalue = sys.exc_info()[:2]
                _logger.error("ActivatorThr failed with %s %s" % (errtype,errvalue))
            self.pool.remove(self)
            self.lock.release()


    _memoryCheck("activator")

    # activate assigned jobs
    _logger.debug("==== activate assigned jobs ====")
    activatorLock = threading.Semaphore(3)
    activatorProxyLock = threading.Lock()
    activatorThreadPool = ThreadPool()
    timeLimit = datetime.datetime.utcnow() - datetime.timedelta(hours=1)
    # get jobs
    for ii in range(1000):
        # lock
        activatorLock.acquire()
        activatorProxyLock.acquire()
        ret,res = taskBuffer.lockJobsForActivator(timeLimit,100,800)
        activatorProxyLock.release()
        activatorLock.release()
        if res is None:
            _logger.debug("# of jobs to be activated for %s " % res)
        else:
            _logger.debug("# of jobs to be activated for %s " % len(res))
        if res is None or len(res) == 0:
            break
        # run thread
        actThr = ActivatorThr(activatorLock,activatorProxyLock,res,activatorThreadPool)
        actThr.start()
    # wait
    activatorThreadPool.join()


    # activator thread with rule
    class ActivatorWithRuleThr (threading.Thread):
        def __init__(self,lock,proxyLock,ids,pool):
            threading.Thread.__init__(self)
            self.ids        = ids
            self.lock       = lock
            self.proxyLock  = proxyLock
            self.pool       = pool
            self.pool.add(self)

        def run(self):
            self.lock.acquire()
            try:
                # get jobs from DB
                ids = self.ids
                self.proxyLock.acquire()
                jobs = taskBuffer.peekJobs(ids,fromActive=False,fromArchived=False,fromWaiting=False)
                self.proxyLock.release()
                actJobs = []
                replicaMap = dict()
                for tmpJob in jobs:
                    if tmpJob is None or tmpJob.jobStatus == 'unknown':
                        continue
                    # check if locally available
                    siteSpec = siteMapper.getSite(tmpJob.computingSite)
                    scope_input, scope_output = select_scope(siteSpec, tmpJob.prodSourceLabel, tmpJob.job_label)
                    allOK = True
                    for tmpFile in tmpJob.Files:
                        # only input files are checked
                        if tmpFile.type == 'input' and tmpFile.status != 'ready':
                            # get replicas
                            if tmpFile.dispatchDBlock not in replicaMap:
                                tmpStat, repMap = rucioAPI.listDatasetReplicas(tmpFile.dispatchDBlock)
                                if tmpStat != 0:
                                    repMap = {}
                                replicaMap[tmpFile.dispatchDBlock] = repMap
                            # check RSEs
                            for rse in replicaMap[tmpFile.dispatchDBlock]:
                                repInfo = replicaMap[tmpFile.dispatchDBlock][rse]
                                if siteSpec.ddm_endpoints_input[scope_input].isAssociated(rse) and \
                                        siteSpec.ddm_endpoints_input[scope_input].getEndPoint(rse)['is_tape'] == 'N' and \
                                        repInfo[0]['total'] == repInfo[0]['found'] and repInfo[0]['total'] is not None:
                                    tmpFile.status = 'ready'
                                    break
                            # missing
                            if tmpFile.status != 'ready':
                                allOK = False
                                _logger.debug("%s skip since %s:%s is missing with rule" % (tmpJob.PandaID,tmpFile.scope,tmpFile.lfn))
                                break
                    if not allOK:
                        continue
                    # append to run activator
                    _logger.debug("{} to activate with rule {}".format(tmpJob.PandaID, str(replicaMap)))
                    actJobs.append(tmpJob)
                # update
                _logger.debug("activating ...")
                self.proxyLock.acquire()
                taskBuffer.activateJobs(actJobs)
                self.proxyLock.release()
                _logger.debug("done")
                time.sleep(1)
            except Exception:
                errtype,errvalue = sys.exc_info()[:2]
                _logger.error("ActivatorThr failed with %s %s" % (errtype,errvalue))
            self.pool.remove(self)
            self.lock.release()


    _memoryCheck("activator")

    # activate assigned jobs
    _logger.debug("==== activate assigned jobs with rule ====")
    activatorLock = threading.Semaphore(3)
    activatorProxyLock = threading.Lock()
    activatorThreadPool = ThreadPool()
    timeLimit = datetime.datetime.utcnow() - datetime.timedelta(hours=1)
    # get jobs
    for ii in range(1000):
        # lock
        activatorLock.acquire()
        activatorProxyLock.acquire()
        ret,res = taskBuffer.lockJobsForActivator(timeLimit,100,0)
        activatorProxyLock.release()
        activatorLock.release()
        if res is None:
            _logger.debug("# of jobs to be activated with rule for %s " % res)
        else:
            _logger.debug("# of jobs to be activated with rule for %s " % len(res))
        if res is None or len(res) == 0:
            break
        # run thread
        actThr = ActivatorWithRuleThr(activatorLock,activatorProxyLock,res,activatorThreadPool)
        actThr.start()
    # wait
    activatorThreadPool.join()


    # thread to delete sub datasets
    class SubDeleter (threading.Thread):
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
                    # check just in case
                    if re.search('_sub\d+$',name) is None:
                        _logger.debug("skip non sub %s" % name)
                        continue
                    _logger.debug("delete sub %s" % name)
                    if name.startswith('pandaddm_') or name.startswith('user.') or name.startswith('group.') \
                            or name.startswith('hc_test.') or name.startswith('panda.um.'):
                        dsExists = False
                    else:
                        dsExists = True
                        # get PandaIDs
                        self.proxyLock.acquire()
                        retF,resF = taskBuffer.querySQLS("SELECT /*+ index(tab FILESTABLE4_DESTDBLOCK_IDX) */ DISTINCT PandaID FROM ATLAS_PANDA.filesTable4 tab WHERE destinationDBlock=:destinationDBlock ",
                                                         {':destinationDBlock':name})
                        self.proxyLock.release()
                        if retF is None:
                            _logger.error("SQL error for sub {0}".format(name))
                            continue
                        else:
                            _logger.debug("sub {0} has {1} jobs".format(name,len(resF)))
                            self.proxyLock.acquire()
                            # check jobs
                            sqlP  = "SELECT jobStatus FROM ATLAS_PANDA.jobsArchived4 WHERE PandaID=:PandaID "
                            sqlP += "UNION "
                            sqlP += "SELECT jobStatus FROM ATLAS_PANDAARCH.jobsArchived WHERE PandaID=:PandaID AND modificationTime>CURRENT_DATE-30 "
                            allDone = True
                            for pandaID, in resF:
                                retP,resP = taskBuffer.querySQLS(sqlP, {':PandaID':pandaID})
                                if len(resP) == 0:
                                    _logger.debug("skip delete sub {0} PandaID={1} not found".format(name,pandaID))
                                    allDone = False
                                    break
                                jobStatus = resP[0][0]
                                if jobStatus not in ['finished','failed','cancelled','closed']:
                                    _logger.debug("skip delete sub {0} PandaID={1} is active {2}".format(name,pandaID,jobStatus))
                                    allDone = False
                                    break
                            self.proxyLock.release()
                            if allDone:
                                _logger.debug("deleting sub %s" % name)
                                try:
                                    rucioAPI.eraseDataset(name, grace_period=4)
                                    status = True
                                except Exception:
                                    errtype,errvalue = sys.exc_info()[:2]
                                    out = '{0} {1}'.format(errtype,errvalue)
                                    _logger.error('{0} failed to erase with {1}'.format(name,out))
                            else:
                                _logger.debug("wait sub %s" % name)
                                continue
                    # update dataset
                    self.proxyLock.acquire()
                    varMap = {}
                    varMap[':vuid'] = vuid
                    varMap[':ost1'] = 'completed'
                    varMap[':ost2'] = 'cleanup'
                    varMap[':newStatus'] = 'deleted'
                    taskBuffer.querySQLS("UPDATE ATLAS_PANDA.Datasets SET status=:newStatus,modificationdate=CURRENT_DATE WHERE vuid=:vuid AND status IN (:ost1,:ost2) ",
                                         varMap)
                    self.proxyLock.release()
                    _logger.debug("end %s " % name)
            except Exception:
                errStr = traceback.format_exc()
                _logger.error(errStr)
            self.pool.remove(self)
            self.lock.release()


    # delete sub datasets
    _logger.debug("==== delete sub datasets ====")
    timeLimitU = datetime.datetime.utcnow() - datetime.timedelta(minutes=30)
    timeLimitL = datetime.datetime.utcnow() - datetime.timedelta(days=14)
    subdeleteLock = threading.Semaphore(5)
    subdeleteProxyLock = threading.Lock()
    subdeleteThreadPool = ThreadPool()
    maxRows = 5000
    while True:
        # lock
        subdeleteLock.acquire()
        # get datasets
        varMap = {}
        varMap[':limitU'] = timeLimitU
        varMap[':limitL'] = timeLimitL
        varMap[':type']    = 'output'
        varMap[':subtype'] = 'sub'
        varMap[':st1']  = 'completed'
        varMap[':st2']  = 'cleanup'
        sqlQuery = "type=:type AND subType=:subtype AND status IN (:st1,:st2) AND (creationdate BETWEEN :limitL AND :limitU) AND (modificationdate BETWEEN :limitL AND :limitU) AND rownum <= %s" % maxRows
        subdeleteProxyLock.acquire()
        res = taskBuffer.getLockDatasets(sqlQuery,varMap,modTimeOffset='90/24/60')
        if res is None:
            _logger.debug("# of sub datasets to be deleted %s" % res)
        else:
            _logger.debug("# of sub datasets to be deleted %s" % len(res))
        if res is None or len(res)==0:
            subdeleteProxyLock.release()
            subdeleteLock.release()
            break
        subdeleteProxyLock.release()
        # release
        subdeleteLock.release()
        # run subdeleter
        iRows = 0
        nRows = 500
        while iRows < len(res):
            subdeleter = SubDeleter(subdeleteLock,subdeleteProxyLock,res[iRows:iRows+nRows],subdeleteThreadPool)
            subdeleter.start()
            iRows += nRows
        subdeleteThreadPool.join()
        if len(res) < 100:
            break

    # release memory
    del siteMapper
    del deletedDisList

    _memoryCheck("end")

    # stop taskBuffer if created inside this script
    taskBuffer.cleanup()

    _logger.debug("===================== end =====================")


# run
if __name__ == '__main__':
    main()
