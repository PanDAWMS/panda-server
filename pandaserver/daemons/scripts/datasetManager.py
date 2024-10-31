import datetime
import os
import re
import sys
import threading
import time
import traceback

from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandautils.thread_utils import GenericThread

import pandaserver.taskbuffer.ErrorCode
from pandaserver.brokerage.SiteMapper import SiteMapper
from pandaserver.config import panda_config
from pandaserver.dataservice import DataServiceUtils
from pandaserver.dataservice.closer import Closer
from pandaserver.dataservice.DataServiceUtils import select_scope
from pandaserver.dataservice.ddm import rucioAPI
from pandaserver.dataservice.finisher import Finisher
from pandaserver.taskbuffer import EventServiceUtils

_logger = PandaLogger().getLogger("datasetManager")

TRANSFER_TIMEOUT_HI_PRIORITY = 2
TRANSFER_TIMEOUT_LO_PRIORITY = 6


def main(tbuf=None, **kwargs):
    _logger.debug("===================== start =====================")

    # memory checker
    def _memoryCheck(str):
        try:
            proc_status = f"/proc/{os.getpid()}/status"
            procfile = open(proc_status)
            name = ""
            vmSize = ""
            vmRSS = ""
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
            _logger.debug(f"MemCheck - {os.getpid()} Name={name} VSZ={vmSize} RSS={vmRSS} : {str}")
        except Exception:
            type, value, traceBack = sys.exc_info()
            _logger.error(f"memoryCheck() : {type} {value}")
            _logger.debug(f"MemCheck - {os.getpid()} unknown : {str}")
        return

    _memoryCheck("start")

    from pandaserver.taskbuffer.TaskBuffer import taskBuffer

    requester_id = GenericThread().get_full_id(__name__, sys.modules[__name__].__file__)
    taskBuffer.init(
        panda_config.dbhost,
        panda_config.dbpasswd,
        nDBConnection=1,
        useTimeout=True,
        requester=requester_id,
    )
    # else:
    #     taskBuffer = tbuf

    # instantiate sitemapper
    siteMapper = SiteMapper(taskBuffer)

    # list with lock
    class ListWithLock:
        def __init__(self):
            self.lock = threading.Lock()
            self.list = []

        def __contains__(self, item):
            self.lock.acquire()
            ret = self.list.__contains__(item)
            self.lock.release()
            return ret

        def append(self, item):
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
            if subDsName.startswith("user") or subDsName.startswith("group") or re.search("_sub\d+$", subDsName) is None:
                return
            # get _dis names with _sub
            disNameList = taskBuffer.getAssociatedDisDatasets(subDsName)
            _logger.debug(f"setTobeDeletedToDis : sub:{subDsName} has dis:{str(disNameList)}")
            # loop over all _dis datasets
            for tmpDisName in disNameList:
                # try to append to locked list
                if not deletedDisList.append(tmpDisName):
                    # another thread already took care of the _dis
                    continue
                # skip non _dis
                if re.search("_dis\d+$", tmpDisName) is None:
                    continue
                # get dataset
                _logger.debug(f"setTobeDeletedToDis : try to get {tmpDisName} in DB")
                tmpDS = taskBuffer.queryDatasetWithMap({"name": tmpDisName})
                if tmpDS is None:
                    _logger.error(f"setTobeDeletedToDis : cannot get {tmpDisName} in DB")
                    continue
                # check status
                if tmpDS.status in ["tobedeleted", "deleted"]:
                    _logger.debug(f"setTobeDeletedToDis : skip {tmpDisName} since status={tmpDS.status}")
                    continue
                # check the number of failed jobs associated to the _dis
                if tmpDS.currentfiles == 0:
                    # all succeeded
                    tmpDS.status = "deleting"
                    excStatus = "deleted"
                else:
                    # some failed, to reduce the lifetime
                    tmpDS.status = "shortening"
                    excStatus = "shortened"
                # update dataset
                retU = taskBuffer.updateDatasets(
                    [tmpDS],
                    withLock=True,
                    withCriteria="status<>:crStatus",
                    criteriaMap={":crStatus": excStatus},
                )
                _logger.debug(f"setTobeDeletedToDis : set {tmpDS.status} to {tmpDisName} with {str(retU)}")
        except Exception:
            errType, errValue = sys.exc_info()[:2]
            _logger.error(f"setTobeDeletedToDis : {subDsName} {errType} {errValue}")

    # thread pool
    class ThreadPool:
        def __init__(self):
            self.lock = threading.Lock()
            self.list = []

        def add(self, obj):
            self.lock.acquire()
            self.list.append(obj)
            self.lock.release()

        def remove(self, obj):
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
    class CloserThr(threading.Thread):
        def __init__(self, lock, proxyLock, datasets, pool):
            threading.Thread.__init__(self)
            self.datasets = datasets
            self.lock = lock
            self.proxyLock = proxyLock
            self.pool = pool
            self.pool.add(self)

        def run(self):
            self.lock.acquire()
            try:
                # loop over all datasets
                for vuid, name, modDate in self.datasets:
                    _logger.debug(f"Close {modDate} {name}")
                    dsExists = True
                    if name.startswith("user.") or name.startswith("group.") or name.startswith("hc_test.") or name.startswith("panda.um."):
                        dsExists = False
                    if dsExists:
                        # check if dataset exists
                        status, out = rucioAPI.get_metadata(name)
                        if status is True:
                            if out is not None:
                                try:
                                    rucioAPI.close_dataset(name)
                                    status = True
                                except Exception:
                                    errtype, errvalue = sys.exc_info()[:2]
                                    out = f"failed to freeze : {errtype} {errvalue}"
                                    status = False
                            else:
                                # dataset not exist
                                status, out = True, ""
                                dsExists = False
                    else:
                        status, out = True, ""
                    if not status:
                        _logger.error(f"{name} failed to close with {out}")
                    else:
                        self.proxyLock.acquire()
                        varMap = {}
                        varMap[":vuid"] = vuid
                        varMap[":newstatus"] = "completed"
                        varMap[":oldstatus"] = "tobeclosed"
                        taskBuffer.querySQLS(
                            "UPDATE ATLAS_PANDA.Datasets SET status=:newstatus,modificationdate=CURRENT_DATE WHERE vuid=:vuid AND status=:oldstatus",
                            varMap,
                        )
                        self.proxyLock.release()
                        # set tobedeleted to dis
                        setTobeDeletedToDis(name)
                        # skip if dataset is not real
                        if not dsExists:
                            continue
                        # count # of files
                        status, out = rucioAPI.get_number_of_files(name)
                        if status is not True:
                            if status is False:
                                _logger.error(out)
                        else:
                            _logger.debug(out)
                            try:
                                nFile = int(out)
                                if nFile == 0:
                                    # erase dataset
                                    _logger.debug(f"erase {name}")
                                    status, out = rucioAPI.erase_dataset(name)
                                    _logger.debug(f"OK with {name}")
                            except Exception:
                                pass
            except Exception:
                pass
            self.pool.remove(self)
            self.lock.release()

    # close datasets
    _logger.debug("==== close datasets ====")
    timeLimitU = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(minutes=1)
    timeLimitL = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(days=3)
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
        varMap[":modificationdateU"] = timeLimitU
        varMap[":modificationdateL"] = timeLimitL
        varMap[":type"] = "output"
        varMap[":status"] = "tobeclosed"
        sqlQuery = f"type=:type AND status=:status AND (modificationdate BETWEEN :modificationdateL AND :modificationdateU) AND rownum <= {maxRows}"
        res = taskBuffer.getLockDatasets(sqlQuery, varMap, modTimeOffset="90/24/60")
        if res is None:
            _logger.debug(f"# of datasets to be closed: {res}")
        else:
            _logger.debug(f"# of datasets to be closed: {len(res)}")
        if res is None or len(res) == 0:
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
            closerThr = CloserThr(closeLock, closeProxyLock, res[iRows : iRows + nRows], closeThreadPool)
            closerThr.start()
            iRows += nRows
        closeThreadPool.join()
        if len(res) < maxRows:
            break

    # thread to freeze dataset
    class Freezer(threading.Thread):
        def __init__(self, lock, proxyLock, datasets, pool):
            threading.Thread.__init__(self)
            self.datasets = datasets
            self.lock = lock
            self.proxyLock = proxyLock
            self.pool = pool
            self.pool.add(self)

        def run(self):
            self.lock.acquire()
            try:
                for vuid, name, modDate in self.datasets:
                    _logger.debug(f"Freezer start {modDate} {name}")
                    self.proxyLock.acquire()
                    retF, resF = taskBuffer.querySQLS(
                        "SELECT /*+ index(tab FILESTABLE4_DESTDBLOCK_IDX) */ PandaID,status FROM ATLAS_PANDA.filesTable4 tab WHERE destinationDBlock=:destinationDBlock ",
                        {":destinationDBlock": name},
                    )
                    self.proxyLock.release()
                    if isinstance(retF, int) and retF < 0:
                        _logger.error("SQL error")
                    else:
                        allFinished = True
                        onePandaID = None
                        for tmpPandaID, tmpFileStatus in resF:
                            onePandaID = tmpPandaID
                            if tmpFileStatus not in [
                                "ready",
                                "failed",
                                "skipped",
                                "merging",
                                "finished",
                            ]:
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
                                    _logger.debug(f"closer checked sub datasets in the jobset for {name} : {allFinished}")
                        # no files in filesTable
                        if allFinished:
                            _logger.debug(f"freeze {name} ")
                            dsExists = True
                            if name.startswith("user.") or name.startswith("group.") or name.startswith("hc_test.") or name.startswith("panda.um."):
                                dsExists = False
                            if name.startswith("panda.um."):
                                self.proxyLock.acquire()
                                retMer, resMer = taskBuffer.querySQLS(
                                    "SELECT /*+ index(tab FILESTABLE4_DESTDBLOCK_IDX) */ PandaID FROM ATLAS_PANDA.filesTable4 tab WHERE destinationDBlock=:destinationDBlock AND status IN (:statusM,:statusF) ",
                                    {
                                        ":destinationDBlock": name,
                                        ":statusM": "merging",
                                        ":statusF": "failed",
                                    },
                                )
                                self.proxyLock.release()
                                if resMer is not None and len(resMer) > 0:
                                    mergeID = resMer[0][0]
                                    # get merging jobs
                                    self.proxyLock.acquire()
                                    mergingJobs = taskBuffer.peekJobs(
                                        [mergeID],
                                        fromDefined=False,
                                        fromArchived=False,
                                        fromWaiting=False,
                                    )
                                    self.proxyLock.release()
                                    mergeJob = mergingJobs[0]
                                    if mergeJob is not None:
                                        tmpDestDBlocks = []
                                        # get destDBlock
                                        for tmpFile in mergeJob.Files:
                                            if tmpFile.type in ["output", "log"]:
                                                if tmpFile.destinationDBlock not in tmpDestDBlocks:
                                                    tmpDestDBlocks.append(tmpFile.destinationDBlock)
                                        # run
                                        _logger.debug(f"start JEDI closer for {name} ")
                                        self.proxyLock.acquire()
                                        cThr = Closer(taskBuffer, tmpDestDBlocks, mergeJob)
                                        cThr.run()
                                        self.proxyLock.release()
                                        _logger.debug(f"end JEDI closer for {name} ")
                                        continue
                                    else:
                                        _logger.debug(f"failed to get merging job for {name} ")
                                else:
                                    _logger.debug(f"failed to get merging file for {name} ")
                                status, out = True, ""
                            elif dsExists:
                                # check if dataset exists
                                status, out = rucioAPI.get_metadata(name)
                                if status is True:
                                    if out is not None:
                                        try:
                                            rucioAPI.close_dataset(name)
                                            status = True
                                        except Exception:
                                            errtype, errvalue = sys.exc_info()[:2]
                                            out = f"failed to freeze : {errtype} {errvalue}"
                                            status = False
                                    else:
                                        # dataset not exist
                                        status, out = True, ""
                                        dsExists = False
                            else:
                                status, out = True, ""
                            if not status:
                                _logger.error(f"{name} failed to freeze with {out}")
                            else:
                                self.proxyLock.acquire()
                                varMap = {}
                                varMap[":vuid"] = vuid
                                varMap[":status"] = "completed"
                                taskBuffer.querySQLS(
                                    "UPDATE ATLAS_PANDA.Datasets SET status=:status,modificationdate=CURRENT_DATE WHERE vuid=:vuid",
                                    varMap,
                                )
                                self.proxyLock.release()
                                if name.startswith("panda.um.") or not dsExists:
                                    continue
                                # set tobedeleted to dis
                                setTobeDeletedToDis(name)
                                # count # of files
                                status, out = rucioAPI.get_number_of_files(name)
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
                                            _logger.debug(f"erase {name}")
                                            status, out = rucioAPI.erase_dataset(name)
                                            _logger.debug(f"OK with {name}")
                                    except Exception:
                                        pass
                        else:
                            _logger.debug(f"wait {name} ")
                            self.proxyLock.acquire()
                            taskBuffer.querySQLS(
                                "UPDATE ATLAS_PANDA.Datasets SET modificationdate=CURRENT_DATE WHERE vuid=:vuid",
                                {":vuid": vuid},
                            )
                            self.proxyLock.release()
                    _logger.debug(f"end {name} ")
            except Exception:
                errStr = traceback.format_exc()
                _logger.error(errStr)
            self.pool.remove(self)
            self.lock.release()

    # freeze dataset
    _logger.debug("==== freeze datasets ====")
    timeLimitRU = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(hours=3)
    timeLimitRL = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(hours=12)
    timeLimitU = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(hours=6)
    timeLimitL = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(days=14)
    # reset doing so that Closer can update unmerged datasets
    sql = "SELECT name FROM ATLAS_PANDA.Datasets "
    sql += "WHERE type=:type AND (modificationdate BETWEEN :modificationdateRL AND :modificationdateRU) AND subType=:subType AND status=:oldStatus "
    varMap = {}
    varMap[":modificationdateRU"] = timeLimitRU
    varMap[":modificationdateRL"] = timeLimitRL
    varMap[":type"] = "output"
    varMap[":subType"] = "sub"
    varMap[":oldStatus"] = "doing"
    retReset, resReset = taskBuffer.querySQLS(sql, varMap)
    sql = "UPDATE ATLAS_PANDA.Datasets SET status=:newStatus,modificationdate=:modificationdateU WHERE name=:name AND status=:oldStatus "
    if resReset is not None:
        for (name,) in resReset:
            varMap = {}
            varMap[":name"] = name
            varMap[":oldStatus"] = "doing"
            varMap[":newStatus"] = "running"
            varMap[":modificationdateU"] = timeLimitU
            _logger.debug(f"reset {name} to freeze")
            taskBuffer.querySQLS(sql, varMap)
    # loop for freezer
    freezeLock = threading.Semaphore(5)
    freezeProxyLock = threading.Lock()
    freezeThreadPool = ThreadPool()
    maxRows = 100000
    while True:
        # lock
        freezeLock.acquire()
        # get datasets
        sqlQuery = (
            "type=:type AND status IN (:status1,:status2,:status3,:status4,:status5) "
            + f"AND (modificationdate BETWEEN :modificationdateL AND :modificationdateU) AND subType=:subType AND rownum <= {maxRows}"
        )
        varMap = {}
        varMap[":modificationdateU"] = timeLimitU
        varMap[":modificationdateL"] = timeLimitL
        varMap[":type"] = "output"
        varMap[":status1"] = "running"
        varMap[":status2"] = "created"
        varMap[":status3"] = "defined"
        varMap[":status4"] = "locked"
        varMap[":status5"] = "doing"
        varMap[":subType"] = "sub"
        freezeProxyLock.acquire()
        res = taskBuffer.getLockDatasets(sqlQuery, varMap, modTimeOffset="90/24/60")
        if res is None:
            _logger.debug(f"# of datasets to be frozen: {res}")
        else:
            _logger.debug(f"# of datasets to be frozen: {len(res)}")
        if res is None or len(res) == 0:
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
            freezer = Freezer(
                freezeLock,
                freezeProxyLock,
                res[iRows : iRows + nRows],
                freezeThreadPool,
            )
            freezer.start()
            iRows += nRows
        freezeThreadPool.join()
        if len(res) < maxRows:
            break

    # delete dis datasets
    class EraserThr(threading.Thread):
        def __init__(self, lock, proxyLock, datasets, pool, operationType):
            threading.Thread.__init__(self)
            self.datasets = datasets
            self.lock = lock
            self.proxyLock = proxyLock
            self.pool = pool
            self.pool.add(self)
            self.operationType = operationType

        def run(self):
            self.lock.acquire()
            try:
                # loop over all datasets
                for vuid, name, modDate in self.datasets:
                    # only dis datasets
                    if re.search("_dis\d+$", name) is None:
                        _logger.error(f"Eraser : non disDS {name}")
                        continue
                    # delete
                    _logger.debug(f"Eraser {self.operationType} dis {modDate} {name}")
                    # delete or shorten
                    endStatus = "deleted"
                    status, out = rucioAPI.erase_dataset(name)
                    if not status:
                        _logger.error(out)
                        continue
                    _logger.debug(f"OK with {name}")
                    # update
                    self.proxyLock.acquire()
                    varMap = {}
                    varMap[":vuid"] = vuid
                    varMap[":status"] = endStatus
                    taskBuffer.querySQLS(
                        "UPDATE ATLAS_PANDA.Datasets SET status=:status,modificationdate=CURRENT_DATE WHERE vuid=:vuid",
                        varMap,
                    )
                    self.proxyLock.release()
            except Exception:
                errStr = traceback.format_exc()
                _logger.error(errStr)
            self.pool.remove(self)
            self.lock.release()

    # delete dis datasets
    _logger.debug("==== delete dis datasets ====")
    timeLimitU = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(minutes=30)
    timeLimitL = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(days=3)
    disEraseLock = threading.Semaphore(5)
    disEraseProxyLock = threading.Lock()
    disEraseThreadPool = ThreadPool()
    # maxRows = 100000
    maxRows = 5000
    for targetStatus in ["deleting", "shortening"]:
        for i in range(10):
            # lock
            disEraseLock.acquire()
            # get datasets
            varMap = {}
            varMap[":modificationdateU"] = timeLimitU
            varMap[":modificationdateL"] = timeLimitL
            varMap[":type"] = "dispatch"
            varMap[":status"] = targetStatus
            sqlQuery = f"type=:type AND status=:status AND (modificationdate BETWEEN :modificationdateL AND :modificationdateU) AND rownum <= {maxRows}"
            disEraseProxyLock.acquire()
            res = taskBuffer.getLockDatasets(sqlQuery, varMap, modTimeOffset="90/24/60")
            if res is None:
                _logger.debug(f"# of dis datasets for {targetStatus}: None")
            else:
                _logger.debug(f"# of dis datasets for {targetStatus}: {len(res)}")
            if res is None or len(res) == 0:
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
                disEraser = EraserThr(
                    disEraseLock,
                    disEraseProxyLock,
                    res[iRows : iRows + nRows],
                    disEraseThreadPool,
                    targetStatus,
                )
                disEraser.start()
                iRows += nRows
            disEraseThreadPool.join()
            if len(res) < 100:
                break

    _memoryCheck("finisher")

    # finisher thread
    class FinisherThr(threading.Thread):
        def __init__(self, lock, proxyLock, ids, pool, timeNow):
            threading.Thread.__init__(self)
            self.ids = ids
            self.lock = lock
            self.proxyLock = proxyLock
            self.pool = pool
            self.timeNow = timeNow
            self.pool.add(self)

        def run(self):
            self.lock.acquire()
            try:
                # get jobs from DB
                ids = self.ids
                self.proxyLock.acquire()
                jobs = taskBuffer.peekJobs(ids, fromDefined=False, fromArchived=False, fromWaiting=False)
                self.proxyLock.release()
                upJobs = []
                finJobs = []
                for job in jobs:
                    if job is None or job.jobStatus == "unknown":
                        continue
                    seList = ["dummy"]
                    tmpNucleus = siteMapper.getNucleus(job.nucleus)
                    # get SEs
                    if job.prodSourceLabel == "user" and job.destinationSE not in siteMapper.siteSpecList:
                        # using --destSE for analysis job to transfer output
                        seList = [job.destinationSE]
                    elif tmpNucleus is not None:
                        seList = list(tmpNucleus.allDdmEndPoints)

                    # get LFN list
                    lfns = []
                    guids = []
                    scopes = []
                    nTokens = 0
                    for file in job.Files:
                        # only output files are checked
                        if file.type == "output" or file.type == "log":
                            if file.status == "nooutput":
                                continue
                            if DataServiceUtils.getDistributedDestination(file.destinationDBlockToken) is not None:
                                continue
                            lfns.append(file.lfn)
                            guids.append(file.GUID)
                            scopes.append(file.scope)
                            nTokens += len(file.destinationDBlockToken.split(","))
                    # get files
                    _logger.debug(f"{job.PandaID} Cloud:{job.cloud}")
                    tmpStat, okFiles = rucioAPI.list_file_replicas(scopes, lfns, seList)
                    if not tmpStat:
                        _logger.error(f"{job.PandaID} failed to get file replicas")
                        okFiles = {}
                    # count files
                    nOkTokens = 0
                    for okLFN in okFiles:
                        okSEs = okFiles[okLFN]
                        nOkTokens += len(okSEs)
                    # check all files are ready
                    _logger.debug(f"{job.PandaID} nToken:{nTokens} nOkToken:{nOkTokens}")
                    if nTokens <= nOkTokens:
                        _logger.debug(f"{job.PandaID} Finisher : Finish")
                        for file in job.Files:
                            if file.type == "output" or file.type == "log":
                                if file.status != "nooutput":
                                    file.status = "ready"
                        # append to run Finisher
                        finJobs.append(job)
                    else:
                        endTime = job.endTime
                        if endTime == "NULL":
                            endTime = job.startTime
                        # priority-dependent timeout
                        if job.currentPriority >= 800 and (job.prodSourceLabel not in ["user"]):
                            timeOutValue = TRANSFER_TIMEOUT_HI_PRIORITY
                        else:
                            timeOutValue = TRANSFER_TIMEOUT_LO_PRIORITY

                        timeOut = self.timeNow - datetime.timedelta(days=timeOutValue)
                        _logger.debug(f"{job.PandaID}  Priority:{job.currentPriority} Limit:{str(timeOut)} End:{str(endTime)}")
                        if endTime < timeOut:
                            # timeout
                            _logger.debug(f"{job.PandaID} Finisher : Kill")
                            strMiss = ""
                            for lfn in lfns:
                                if lfn not in okFiles:
                                    strMiss += f" {lfn}"
                            job.jobStatus = "failed"
                            job.taskBufferErrorCode = pandaserver.taskbuffer.ErrorCode.EC_Transfer
                            job.taskBufferErrorDiag = f"transfer timeout for {strMiss}"
                            guidMap = {}
                            for file in job.Files:
                                # set file status
                                if file.status == "transferring" or file.type in [
                                    "log",
                                    "output",
                                ]:
                                    file.status = "failed"
                                # collect GUIDs to delete files from _tid datasets
                                if file.type == "output" or file.type == "log":
                                    if file.destinationDBlock not in guidMap:
                                        guidMap[file.destinationDBlock] = []
                                    guidMap[file.destinationDBlock].append(file.GUID)
                        else:
                            # wait
                            _logger.debug(f"{job.PandaID} Finisher : Wait")
                            for lfn in lfns:
                                if lfn not in okFiles:
                                    _logger.debug(f"{job.PandaID}    -> {lfn}")
                    upJobs.append(job)
                # update
                _logger.debug("updating ...")
                self.proxyLock.acquire()
                taskBuffer.updateJobs(upJobs, False)
                self.proxyLock.release()
                # run Finisher
                for job in finJobs:
                    fThr = Finisher(taskBuffer, None, job)
                    fThr.run()
                _logger.debug("done")
                time.sleep(1)
            except Exception:
                errtype, errvalue = sys.exc_info()[:2]
                errStr = f"FinisherThr failed with {errtype} {errvalue}"
                errStr += traceback.format_exc()
                _logger.error(errStr)
            self.pool.remove(self)
            self.lock.release()

    # finish transferring jobs
    _logger.debug("==== finish transferring jobs ====")
    finisherLock = threading.Semaphore(3)
    finisherProxyLock = threading.Lock()
    finisherThreadPool = ThreadPool()
    for loopIdx in ["low", "high"]:
        timeNow = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
        if loopIdx == "high":
            highPrioFlag = True
        else:
            highPrioFlag = False
        # get jobs
        for ii in range(1000):
            # lock
            finisherLock.acquire()
            finisherProxyLock.acquire()
            ret, res = taskBuffer.lockJobsForFinisher(timeNow, 200, highPrioFlag)
            finisherProxyLock.release()
            finisherLock.release()
            if res is None:
                _logger.debug(f"# of jobs to be finished for {loopIdx} : {res}")
            else:
                _logger.debug(f"# of jobs to be finished for {loopIdx} : {len(res)}")
            if res is None or len(res) == 0:
                break
            # run thread
            finThr = FinisherThr(finisherLock, finisherProxyLock, res, finisherThreadPool, timeNow)
            finThr.start()
        # wait
        finisherThreadPool.join()

    # activator thread
    class ActivatorThr(threading.Thread):
        def __init__(self, lock, proxyLock, ids, pool):
            threading.Thread.__init__(self)
            self.ids = ids
            self.lock = lock
            self.proxyLock = proxyLock
            self.pool = pool
            self.pool.add(self)

        def run(self):
            self.lock.acquire()
            try:
                # get jobs from DB
                ids = self.ids
                self.proxyLock.acquire()
                jobs = taskBuffer.peekJobs(ids, fromActive=False, fromArchived=False, fromWaiting=False)
                self.proxyLock.release()
                actJobs = []
                for tmpJob in jobs:
                    if tmpJob is None or tmpJob.jobStatus == "unknown":
                        continue
                    # get LFN list
                    lfns = []
                    guids = []
                    scopes = []
                    for tmpFile in tmpJob.Files:
                        # only input files are checked
                        if tmpFile.type == "input" and tmpFile.status != "ready":
                            lfns.append(tmpFile.lfn)
                            scopes.append(tmpFile.scope)
                    # get file replicas
                    _logger.debug(f"{tmpJob.PandaID} check input files at {tmpJob.computingSite}")
                    tmpStat, okFiles = rucioAPI.list_file_replicas(scopes, lfns)
                    if not tmpStat:
                        pass
                    else:
                        # check if locally available
                        siteSpec = siteMapper.getSite(tmpJob.computingSite)
                        scope_input, scope_output = select_scope(siteSpec, tmpJob.prodSourceLabel, tmpJob.job_label)
                        allOK = True
                        for tmpFile in tmpJob.Files:
                            # only input
                            if tmpFile.type == "input" and tmpFile.status != "ready":
                                # check RSEs
                                if tmpFile.lfn in okFiles:
                                    for rse in okFiles[tmpFile.lfn]:
                                        if (
                                            siteSpec.ddm_endpoints_input[scope_input].isAssociated(rse)
                                            and siteSpec.ddm_endpoints_input[scope_input].getEndPoint(rse)["is_tape"] == "N"
                                        ):
                                            tmpFile.status = "ready"
                                            break
                                # missing
                                if tmpFile.status != "ready":
                                    allOK = False
                                    _logger.debug(f"{tmpJob.PandaID} skip since {tmpFile.scope}:{tmpFile.lfn} is missing")
                                    break
                        if not allOK:
                            continue
                        # append to run activator
                        _logger.debug(f"{tmpJob.PandaID} to activate")
                        actJobs.append(tmpJob)
                # update
                _logger.debug("activating ...")
                self.proxyLock.acquire()
                taskBuffer.activateJobs(actJobs)
                self.proxyLock.release()
                _logger.debug("done")
                time.sleep(1)
            except Exception:
                errtype, errvalue = sys.exc_info()[:2]
                _logger.error(f"ActivatorThr failed with {errtype} {errvalue}")
            self.pool.remove(self)
            self.lock.release()

    _memoryCheck("activator")

    # activate assigned jobs
    _logger.debug("==== activate assigned jobs ====")
    activatorLock = threading.Semaphore(3)
    activatorProxyLock = threading.Lock()
    activatorThreadPool = ThreadPool()
    timeLimit = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(hours=1)
    # get jobs
    for ii in range(1000):
        # lock
        activatorLock.acquire()
        activatorProxyLock.acquire()
        ret, res = taskBuffer.lockJobsForActivator(timeLimit, 100, 800)
        activatorProxyLock.release()
        activatorLock.release()
        if res is None:
            _logger.debug(f"# of jobs to be activated for {res} ")
        else:
            _logger.debug(f"# of jobs to be activated for {len(res)} ")
        if res is None or len(res) == 0:
            break
        # run thread
        actThr = ActivatorThr(activatorLock, activatorProxyLock, res, activatorThreadPool)
        actThr.start()
    # wait
    activatorThreadPool.join()

    # activator thread with rule
    class ActivatorWithRuleThr(threading.Thread):
        def __init__(self, lock, proxyLock, ids, pool):
            threading.Thread.__init__(self)
            self.ids = ids
            self.lock = lock
            self.proxyLock = proxyLock
            self.pool = pool
            self.pool.add(self)

        def run(self):
            self.lock.acquire()
            try:
                # get jobs from DB
                ids = self.ids
                self.proxyLock.acquire()
                jobs = taskBuffer.peekJobs(ids, fromActive=False, fromArchived=False, fromWaiting=False)
                self.proxyLock.release()
                actJobs = []
                replicaMap = dict()
                for tmpJob in jobs:
                    if tmpJob is None or tmpJob.jobStatus == "unknown":
                        continue
                    # check if locally available
                    siteSpec = siteMapper.getSite(tmpJob.computingSite)
                    scope_input, scope_output = select_scope(siteSpec, tmpJob.prodSourceLabel, tmpJob.job_label)
                    allOK = True
                    for tmpFile in tmpJob.Files:
                        # only input files are checked
                        if tmpFile.type == "input" and tmpFile.status != "ready":
                            # get replicas
                            if tmpFile.dispatchDBlock not in replicaMap:
                                tmpStat, repMap = rucioAPI.list_dataset_replicas(tmpFile.dispatchDBlock)
                                if tmpStat != 0:
                                    repMap = {}
                                replicaMap[tmpFile.dispatchDBlock] = repMap
                            # check RSEs
                            for rse in replicaMap[tmpFile.dispatchDBlock]:
                                repInfo = replicaMap[tmpFile.dispatchDBlock][rse]
                                if (
                                    siteSpec.ddm_endpoints_input[scope_input].isAssociated(rse)
                                    and siteSpec.ddm_endpoints_input[scope_input].getEndPoint(rse)["is_tape"] == "N"
                                    and repInfo[0]["total"] == repInfo[0]["found"]
                                    and repInfo[0]["total"] is not None
                                ):
                                    tmpFile.status = "ready"
                                    break
                            # missing
                            if tmpFile.status != "ready":
                                allOK = False
                                _logger.debug(f"{tmpJob.PandaID} skip since {tmpFile.scope}:{tmpFile.lfn} is missing with rule")
                                break
                    if not allOK:
                        continue
                    # append to run activator
                    _logger.debug(f"{tmpJob.PandaID} to activate with rule {str(replicaMap)}")
                    actJobs.append(tmpJob)
                # update
                _logger.debug("activating ...")
                self.proxyLock.acquire()
                taskBuffer.activateJobs(actJobs)
                self.proxyLock.release()
                _logger.debug("done")
                time.sleep(1)
            except Exception:
                errtype, errvalue = sys.exc_info()[:2]
                _logger.error(f"ActivatorThr failed with {errtype} {errvalue}")
            self.pool.remove(self)
            self.lock.release()

    _memoryCheck("activator")

    # activate assigned jobs
    _logger.debug("==== activate assigned jobs with rule ====")
    activatorLock = threading.Semaphore(3)
    activatorProxyLock = threading.Lock()
    activatorThreadPool = ThreadPool()
    timeLimit = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(hours=1)
    # get jobs
    for ii in range(1000):
        # lock
        activatorLock.acquire()
        activatorProxyLock.acquire()
        ret, res = taskBuffer.lockJobsForActivator(timeLimit, 100, 0)
        activatorProxyLock.release()
        activatorLock.release()
        if res is None:
            _logger.debug(f"# of jobs to be activated with rule for {res} ")
        else:
            _logger.debug(f"# of jobs to be activated with rule for {len(res)} ")
        if res is None or len(res) == 0:
            break
        # run thread
        actThr = ActivatorWithRuleThr(activatorLock, activatorProxyLock, res, activatorThreadPool)
        actThr.start()
    # wait
    activatorThreadPool.join()

    # thread to delete sub datasets
    class SubDeleter(threading.Thread):
        def __init__(self, lock, proxyLock, datasets, pool):
            threading.Thread.__init__(self)
            self.datasets = datasets
            self.lock = lock
            self.proxyLock = proxyLock
            self.pool = pool
            self.pool.add(self)

        def run(self):
            self.lock.acquire()
            try:
                for vuid, name, modDate in self.datasets:
                    # check just in case
                    if re.search("_sub\d+$", name) is None:
                        _logger.debug(f"skip non sub {name}")
                        continue
                    _logger.debug(f"delete sub {name}")
                    if name.startswith("user.") or name.startswith("group.") or name.startswith("hc_test.") or name.startswith("panda.um."):
                        dsExists = False
                    else:
                        dsExists = True
                        # get PandaIDs
                        self.proxyLock.acquire()
                        retF, resF = taskBuffer.querySQLS(
                            "SELECT /*+ index(tab FILESTABLE4_DESTDBLOCK_IDX) */ DISTINCT PandaID FROM ATLAS_PANDA.filesTable4 tab WHERE destinationDBlock=:destinationDBlock ",
                            {":destinationDBlock": name},
                        )
                        self.proxyLock.release()
                        if retF is None:
                            _logger.error(f"SQL error for sub {name}")
                            continue
                        else:
                            _logger.debug(f"sub {name} has {len(resF)} jobs")
                            self.proxyLock.acquire()
                            # check jobs
                            sqlP = "SELECT jobStatus FROM ATLAS_PANDA.jobsArchived4 WHERE PandaID=:PandaID "
                            sqlP += "UNION "
                            sqlP += "SELECT jobStatus FROM ATLAS_PANDAARCH.jobsArchived WHERE PandaID=:PandaID AND modificationTime>CURRENT_DATE-30 "
                            allDone = True
                            for (pandaID,) in resF:
                                retP, resP = taskBuffer.querySQLS(sqlP, {":PandaID": pandaID})
                                if len(resP) == 0:
                                    _logger.debug(f"skip delete sub {name} PandaID={pandaID} not found")
                                    allDone = False
                                    break
                                jobStatus = resP[0][0]
                                if jobStatus not in [
                                    "finished",
                                    "failed",
                                    "cancelled",
                                    "closed",
                                ]:
                                    _logger.debug(f"skip delete sub {name} PandaID={pandaID} is active {jobStatus}")
                                    allDone = False
                                    break
                            self.proxyLock.release()
                            if allDone:
                                _logger.debug(f"deleting sub {name}")
                                try:
                                    rucioAPI.erase_dataset(name, grace_period=4)
                                    status = True
                                except Exception:
                                    errtype, errvalue = sys.exc_info()[:2]
                                    out = f"{errtype} {errvalue}"
                                    _logger.error(f"{name} failed to erase with {out}")
                            else:
                                _logger.debug(f"wait sub {name}")
                                continue
                    # update dataset
                    self.proxyLock.acquire()
                    varMap = {}
                    varMap[":vuid"] = vuid
                    varMap[":ost1"] = "completed"
                    varMap[":ost2"] = "cleanup"
                    varMap[":newStatus"] = "deleted"
                    taskBuffer.querySQLS(
                        "UPDATE ATLAS_PANDA.Datasets SET status=:newStatus,modificationdate=CURRENT_DATE WHERE vuid=:vuid AND status IN (:ost1,:ost2) ",
                        varMap,
                    )
                    self.proxyLock.release()
                    _logger.debug(f"end {name} ")
            except Exception:
                errStr = traceback.format_exc()
                _logger.error(errStr)
            self.pool.remove(self)
            self.lock.release()

    # delete sub datasets
    _logger.debug("==== delete sub datasets ====")
    timeLimitU = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(minutes=30)
    timeLimitL = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(days=14)
    subdeleteLock = threading.Semaphore(5)
    subdeleteProxyLock = threading.Lock()
    subdeleteThreadPool = ThreadPool()
    maxRows = 5000
    while True:
        # lock
        subdeleteLock.acquire()
        # get datasets
        varMap = {}
        varMap[":limitU"] = timeLimitU
        varMap[":limitL"] = timeLimitL
        varMap[":type"] = "output"
        varMap[":subtype"] = "sub"
        varMap[":st1"] = "completed"
        varMap[":st2"] = "cleanup"
        sqlQuery = (
            "type=:type AND subType=:subtype AND status IN (:st1,:st2) AND (creationdate BETWEEN :limitL AND :limitU) AND (modificationdate BETWEEN :limitL AND :limitU) AND rownum <= %s"
            % maxRows
        )
        subdeleteProxyLock.acquire()
        res = taskBuffer.getLockDatasets(sqlQuery, varMap, modTimeOffset="90/24/60")
        if res is None:
            _logger.debug(f"# of sub datasets to be deleted {res}")
        else:
            _logger.debug(f"# of sub datasets to be deleted {len(res)}")
        if res is None or len(res) == 0:
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
            subdeleter = SubDeleter(
                subdeleteLock,
                subdeleteProxyLock,
                res[iRows : iRows + nRows],
                subdeleteThreadPool,
            )
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
    taskBuffer.cleanup(requester=requester_id)

    _logger.debug("===================== end =====================")


# run
if __name__ == "__main__":
    main()
