import datetime
import math
import os
import re
import socket
import sys
import time
import traceback
import uuid

from pandajedi.jediconfig import jedi_config
from pandajedi.jedicore import Interaction
from pandajedi.jedicore.MsgWrapper import MsgWrapper
from pandajedi.jedicore.ThreadUtils import ListWithLock, ThreadPool, WorkerThread
from pandajedi.jedirefine import RefinerUtils

from .JediKnight import JediKnight

try:
    import idds.common.constants
    import idds.common.utils
    from idds.client.client import Client as iDDS_Client
except ImportError:
    pass

from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandautils.PandaUtils import naive_utcnow

logger = PandaLogger().getLogger(__name__.split(".")[-1])


# worker class to take care of DatasetContents table
class ContentsFeeder(JediKnight):
    # constructor
    def __init__(self, commuChannel, taskBufferIF, ddmIF, vos, prodSourceLabels):
        self.vos = self.parseInit(vos)
        self.prodSourceLabels = self.parseInit(prodSourceLabels)
        self.pid = f"{socket.getfqdn().split('.')[0]}-{os.getpid()}_{os.getpgrp()}-con"
        JediKnight.__init__(self, commuChannel, taskBufferIF, ddmIF, logger)

    # main
    def start(self):
        # start base class
        JediKnight.start(self)
        # go into main loop
        while True:
            startTime = naive_utcnow()
            try:
                # loop over all vos
                for vo in self.vos:
                    # loop over all sourceLabels
                    for prodSourceLabel in self.prodSourceLabels:
                        # get the list of datasets to feed contents to DB
                        tmpList = self.taskBufferIF.getDatasetsToFeedContents_JEDI(vo, prodSourceLabel)
                        if tmpList is None:
                            # failed
                            logger.error("failed to get the list of datasets to feed contents")
                        else:
                            logger.debug(f"got {len(tmpList)} datasets")
                            # put to a locked list
                            dsList = ListWithLock(tmpList)
                            # make thread pool
                            threadPool = ThreadPool()
                            # make workers
                            nWorker = jedi_config.confeeder.nWorkers
                            for iWorker in range(nWorker):
                                thr = ContentsFeederThread(dsList, threadPool, self.taskBufferIF, self.ddmIF, self.pid)
                                thr.start()
                            # join
                            threadPool.join()
            except Exception:
                errtype, errvalue = sys.exc_info()[:2]
                logger.error(f"failed in {self.__class__.__name__}.start() with {errtype.__name__} {errvalue}")
            # sleep if needed
            loopCycle = jedi_config.confeeder.loopCycle
            timeDelta = naive_utcnow() - startTime
            sleepPeriod = loopCycle - timeDelta.seconds
            if sleepPeriod > 0:
                time.sleep(sleepPeriod)
            # randomize cycle
            self.randomSleep(max_val=loopCycle)


# thread for real worker
class ContentsFeederThread(WorkerThread):
    # constructor
    def __init__(self, taskDsList, threadPool, taskbufferIF, ddmIF, pid):
        # initialize woker with no semaphore
        WorkerThread.__init__(self, None, threadPool, logger)
        # attributres
        self.taskDsList = taskDsList
        self.taskBufferIF = taskbufferIF
        self.ddmIF = ddmIF
        self.msgType = "contentsfeeder"
        self.pid = pid

    # main
    def runImpl(self):
        while True:
            try:
                # get a part of list
                nTasks = 10
                taskDsList = self.taskDsList.get(nTasks)
                # no more datasets
                if len(taskDsList) == 0:
                    self.logger.debug(f"{self.__class__.__name__} terminating since no more items")
                    return
                # feed to tasks
                self.feed_contents_to_tasks(taskDsList)
            except Exception as e:
                logger.error(f"{self.__class__.__name__} failed in runImpl() with {str(e)}: {traceback.format_exc()}")

    # feed contents to tasks
    def feed_contents_to_tasks(self, task_ds_list, real_run=True):
        # max number of file records per dataset
        maxFileRecords = 200000
        # loop over all tasks
        for jediTaskID, dsList in task_ds_list:
            allUpdated = True
            taskBroken = False
            taskOnHold = False
            runningTask = False
            taskToFinish = False
            missingMap = {}
            datasetsIdxConsistency = []

            # get task
            tmpStat, taskSpec = self.taskBufferIF.getTaskWithID_JEDI(jediTaskID, False, real_run, self.pid, 10, clearError=True)
            if not tmpStat or taskSpec is None:
                self.logger.debug(f"failed to get taskSpec for jediTaskID={jediTaskID}")
                continue

            # make logger
            try:
                gshare = "_".join(taskSpec.gshare.split(" "))
            except Exception:
                gshare = "Undefined"
            tmpLog = MsgWrapper(self.logger, f"<jediTaskID={jediTaskID} gshare={gshare}>")

            try:
                # get task parameters
                taskParam = self.taskBufferIF.getTaskParamsWithID_JEDI(jediTaskID)
                taskParamMap = RefinerUtils.decodeJSON(taskParam)
            except Exception as e:
                tmpLog.error(f"task param conversion from json failed with {str(e)}")
                # unlock
                tmpStat = self.taskBufferIF.unlockSingleTask_JEDI(jediTaskID, self.pid)
                tmpLog.debug(f"unlocked with {tmpStat}")
                continue
            # renaming of parameters
            if "nEventsPerInputFile" in taskParamMap:
                taskParamMap["nEventsPerFile"] = taskParamMap["nEventsPerInputFile"]
            # the number of files per job
            nFilesPerJob = taskSpec.getNumFilesPerJob()
            # the number of chunks used by scout
            nChunksForScout = 10
            # load XML
            if taskSpec.useLoadXML():
                xmlConfig = taskParamMap["loadXML"]
            else:
                xmlConfig = None
            # skip files used by another task
            if "skipFilesUsedBy" in taskParamMap:
                skipFilesUsedBy = taskParamMap["skipFilesUsedBy"]
            else:
                skipFilesUsedBy = None
            # check no wait
            noWaitParent = False
            parentOutDatasets = set()
            if taskSpec.noWaitParent() and taskSpec.parent_tid not in [None, taskSpec.jediTaskID]:
                tmpStat = self.taskBufferIF.checkParentTask_JEDI(taskSpec.parent_tid, taskSpec.jediTaskID)
                if tmpStat is None or tmpStat == "running":
                    noWaitParent = True
                    # get output datasets from parent task
                    tmpParentStat, tmpParentOutDatasets = self.taskBufferIF.getDatasetsWithJediTaskID_JEDI(taskSpec.parent_tid, ["output", "log"])
                    # collect dataset names
                    for tmpParentOutDataset in tmpParentOutDatasets:
                        parentOutDatasets.add(tmpParentOutDataset.datasetName)
                        if tmpParentOutDataset.containerName:
                            if tmpParentOutDataset.containerName.endswith("/"):
                                parentOutDatasets.add(tmpParentOutDataset.containerName)
                                parentOutDatasets.add(tmpParentOutDataset.containerName[:-1])
                            else:
                                parentOutDatasets.add(tmpParentOutDataset.containerName)
                                parentOutDatasets.add(tmpParentOutDataset.containerName + "/")
            # loop over all datasets
            nFilesMaster = 0
            checkedMaster = False
            setFrozenTime = True
            master_offset = None
            master_is_open = False
            if not taskBroken:
                ddmIF = self.ddmIF.getInterface(taskSpec.vo, taskSpec.cloud)
                origNumFiles = None
                if "nFiles" in taskParamMap:
                    origNumFiles = taskParamMap["nFiles"]
                id_to_container = {}
                [id_to_container.update({datasetSpec.datasetID: datasetSpec.containerName}) for datasetSpec in dsList]
                skip_secondaries = False
                for datasetSpec in dsList:
                    tmpLog.debug(f"start loop for {datasetSpec.datasetName}(id={datasetSpec.datasetID})")
                    if skip_secondaries and not datasetSpec.isMaster():
                        tmpLog.debug(f"skip {datasetSpec.datasetName} since it is secondary")
                        continue
                    # index consistency
                    if datasetSpec.indexConsistent():
                        datasetsIdxConsistency.append(datasetSpec.datasetID)
                    # prestaging
                    if taskSpec.inputPreStaging() and (datasetSpec.isMaster() or datasetSpec.isSeqNumber()):
                        if datasetSpec.is_no_staging():
                            inputPreStaging = False
                        elif (
                            (nStaging := self.taskBufferIF.getNumStagingFiles_JEDI(taskSpec.jediTaskID)) is not None
                            and nStaging == 0
                            and datasetSpec.nFiles > 0
                        ):
                            inputPreStaging = False
                        else:
                            inputPreStaging = True
                    else:
                        inputPreStaging = False
                    # get dataset metadata
                    tmpLog.debug("get metadata")
                    stateUpdateTime = naive_utcnow()
                    try:
                        if not datasetSpec.isPseudo():
                            tmpMetadata = ddmIF.getDatasetMetaData(datasetSpec.datasetName, ignore_missing=True)
                        else:
                            # dummy metadata for pseudo dataset
                            tmpMetadata = {"state": "closed"}
                        # check if master is open
                        if datasetSpec.isMaster() and tmpMetadata["state"] == "open":
                            master_is_open = True
                        # set mutable when the dataset is open and parent is running or task is configured to run until the dataset is closed
                        if (noWaitParent or taskSpec.runUntilClosed() or inputPreStaging) and (
                            tmpMetadata["state"] == "open"
                            or datasetSpec.datasetName in parentOutDatasets
                            or datasetSpec.datasetName.split(":")[-1] in parentOutDatasets
                            or inputPreStaging
                        ):
                            # dummy metadata when parent is running
                            tmpMetadata = {"state": "mutable"}
                    except Exception:
                        errtype, errvalue = sys.exc_info()[:2]
                        tmpLog.error(f"{self.__class__.__name__} failed to get metadata to {errtype.__name__}:{errvalue}")
                        if errtype == Interaction.JEDIFatalError:
                            # fatal error
                            datasetStatus = "broken"
                            taskBroken = True
                            # update dataset status
                            self.updateDatasetStatus(datasetSpec, datasetStatus, tmpLog)
                        else:
                            if not taskSpec.ignoreMissingInDS():
                                # temporary error
                                taskOnHold = True
                            else:
                                # ignore missing
                                datasetStatus = "failed"
                                # update dataset status
                                self.updateDatasetStatus(datasetSpec, datasetStatus, tmpLog)
                        taskSpec.setErrDiag(f"failed to get metadata for {datasetSpec.datasetName}")
                        if not taskSpec.ignoreMissingInDS():
                            allUpdated = False
                    else:
                        # to skip missing dataset
                        if tmpMetadata["state"] == "missing":
                            # ignore missing
                            datasetStatus = "finished"
                            # update dataset status
                            self.updateDatasetStatus(datasetSpec, datasetStatus, tmpLog, "closed")
                            tmpLog.debug(f"disabled missing {datasetSpec.datasetName}")
                            continue
                        # get file list specified in task parameters
                        if taskSpec.is_work_segmented() and not datasetSpec.isPseudo() and not datasetSpec.isMaster():
                            fileList = []
                            includePatt = []
                            excludePatt = []
                            try:
                                segment_id = int(id_to_container[datasetSpec.masterID].split("/")[-1])
                                for item in taskParamMap["segmentSpecs"]:
                                    if item["id"] == segment_id:
                                        if "files" in item:
                                            fileList = item["files"]
                                        elif "datasets" in item:
                                            for tmpDatasetName in item["datasets"]:
                                                tmpRet = ddmIF.getFilesInDataset(tmpDatasetName)
                                                fileList += [tmpAttr["lfn"] for tmpAttr in tmpRet.values()]
                            except Exception:
                                pass
                        else:
                            fileList, includePatt, excludePatt = RefinerUtils.extractFileList(taskParamMap, datasetSpec.datasetName)
                        # get the number of events in metadata
                        if "getNumEventsInMetadata" in taskParamMap:
                            getNumEvents = True
                        else:
                            getNumEvents = False
                        # get file list from DDM
                        tmpLog.debug("get files")
                        try:
                            useInFilesWithNewAttemptNr = False
                            skipDuplicate = not datasetSpec.useDuplicatedFiles()
                            if not datasetSpec.isPseudo():
                                if fileList != [] and "useInFilesInContainer" in taskParamMap and datasetSpec.containerName not in ["", None]:
                                    # read files from container if file list is specified in task parameters
                                    tmpDatasetName = datasetSpec.containerName
                                else:
                                    tmpDatasetName = datasetSpec.datasetName
                                # use long format for LB
                                longFormat = False
                                if taskSpec.respectLumiblock() or taskSpec.orderByLB():
                                    longFormat = True
                                tmpRet = ddmIF.getFilesInDataset(tmpDatasetName, getNumEvents=getNumEvents, skipDuplicate=skipDuplicate, longFormat=longFormat)
                                tmpLog.debug(f"got {len(tmpRet)} files in {tmpDatasetName}")
                            else:
                                if datasetSpec.isSeqNumber():
                                    # make dummy files for seq_number
                                    if datasetSpec.getNumRecords() is not None:
                                        nPFN = datasetSpec.getNumRecords()
                                    elif origNumFiles is not None:
                                        nPFN = origNumFiles
                                        if "nEventsPerFile" in taskParamMap and taskSpec.get_min_granularity():
                                            nPFN = nPFN * taskParamMap["nEventsPerFile"] // taskSpec.get_min_granularity()
                                        elif (
                                            "nEventsPerJob" in taskParamMap
                                            and "nEventsPerFile" in taskParamMap
                                            and taskParamMap["nEventsPerFile"] > taskParamMap["nEventsPerJob"]
                                        ):
                                            nPFN = nPFN * math.ceil(taskParamMap["nEventsPerFile"] / taskParamMap["nEventsPerJob"])
                                        elif "nEventsPerJob" in taskParamMap and "nEventsPerFile" not in taskParamMap:
                                            max_events_in_file = self.taskBufferIF.get_max_events_in_dataset(jediTaskID, datasetSpec.masterID)
                                            if max_events_in_file and max_events_in_file > taskParamMap["nEventsPerJob"]:
                                                nPFN = nPFN * math.ceil(max_events_in_file / taskParamMap["nEventsPerJob"])
                                    elif "nEvents" in taskParamMap and "nEventsPerJob" in taskParamMap:
                                        nPFN = math.ceil(taskParamMap["nEvents"] / taskParamMap["nEventsPerJob"])
                                    elif "nEvents" in taskParamMap and "nEventsPerFile" in taskParamMap and taskSpec.getNumFilesPerJob() is not None:
                                        nPFN = math.ceil(taskParamMap["nEvents"] / taskParamMap["nEventsPerFile"] / taskSpec.getNumFilesPerJob())
                                    else:
                                        # the default number of records for seq_number
                                        seqDefNumRecords = 10000
                                        # get nFiles of the master
                                        tmpMasterAtt = self.taskBufferIF.getDatasetAttributes_JEDI(datasetSpec.jediTaskID, datasetSpec.masterID, ["nFiles"])
                                        # use nFiles of the master as the number of records if it is larger than the default
                                        if "nFiles" in tmpMasterAtt and tmpMasterAtt["nFiles"] > seqDefNumRecords:
                                            nPFN = tmpMasterAtt["nFiles"]
                                        else:
                                            nPFN = seqDefNumRecords
                                        # check usedBy
                                        if skipFilesUsedBy is not None:
                                            for tmpJediTaskID in str(skipFilesUsedBy).split(","):
                                                tmpParentAtt = self.taskBufferIF.getDatasetAttributesWithMap_JEDI(
                                                    tmpJediTaskID, {"datasetName": datasetSpec.datasetName}, ["nFiles"]
                                                )
                                                if "nFiles" in tmpParentAtt and tmpParentAtt["nFiles"]:
                                                    nPFN += tmpParentAtt["nFiles"]
                                    if nPFN > maxFileRecords:
                                        raise Interaction.JEDIFatalError(f"Too many file records for seq_number >{maxFileRecords}")
                                    tmpRet = {}
                                    # get offset
                                    tmpOffset = datasetSpec.getOffset()
                                    tmpOffset += 1
                                    for iPFN in range(nPFN):
                                        tmpRet[str(uuid.uuid4())] = {
                                            "lfn": iPFN + tmpOffset,
                                            "scope": None,
                                            "filesize": 0,
                                            "checksum": None,
                                        }
                                elif not taskSpec.useListPFN():
                                    # dummy file list for pseudo dataset
                                    tmpRet = {
                                        str(uuid.uuid4()): {
                                            "lfn": "pseudo_lfn",
                                            "scope": None,
                                            "filesize": 0,
                                            "checksum": None,
                                        }
                                    }
                                else:
                                    # make dummy file list for PFN list
                                    if "nFiles" in taskParamMap:
                                        nPFN = taskParamMap["nFiles"]
                                    else:
                                        nPFN = 1
                                    tmpRet = {}
                                    for iPFN in range(nPFN):
                                        base_name = taskParamMap["pfnList"][iPFN].split("/")[-1]
                                        n_events = None
                                        if "^" in base_name:
                                            base_name, n_events = base_name.split("^")

                                        tmpRet[str(uuid.uuid4())] = {
                                            "lfn": f"{iPFN:06d}:{base_name}",
                                            "scope": None,
                                            "filesize": 0,
                                            "checksum": None,
                                            "events": n_events,
                                        }
                        except Exception:
                            errtype, errvalue = sys.exc_info()[:2]
                            reason = str(errvalue)
                            tmpLog.error(f"failed to get files in {self.__class__.__name__}:{errtype.__name__} due to {reason}")
                            if errtype == Interaction.JEDIFatalError:
                                # fatal error
                                datasetStatus = "broken"
                                taskBroken = True
                                # update dataset status
                                self.updateDatasetStatus(datasetSpec, datasetStatus, tmpLog)
                            else:
                                # temporary error
                                taskOnHold = True
                            taskSpec.setErrDiag(f"failed to get files for {datasetSpec.datasetName} due to {reason}")
                            allUpdated = False
                        else:
                            # parameters for master input
                            respectLB = False
                            useRealNumEvents = False
                            if datasetSpec.isMaster():
                                # respect LB boundaries
                                respectLB = taskSpec.respectLumiblock()
                                # use real number of events
                                useRealNumEvents = taskSpec.useRealNumEvents()
                            # the number of events per file
                            nEventsPerFile = None
                            nEventsPerJob = None
                            nEventsPerRange = None
                            tgtNumEventsPerJob = None
                            if (datasetSpec.isMaster() and ("nEventsPerFile" in taskParamMap or useRealNumEvents)) or (
                                datasetSpec.isPseudo() and "nEvents" in taskParamMap and not datasetSpec.isSeqNumber()
                            ):
                                if "nEventsPerFile" in taskParamMap:
                                    nEventsPerFile = taskParamMap["nEventsPerFile"]
                                elif datasetSpec.isMaster() and datasetSpec.isPseudo() and "nEvents" in taskParamMap:
                                    # use nEvents as nEventsPerFile for pseudo input
                                    nEventsPerFile = taskParamMap["nEvents"]
                                if taskSpec.get_min_granularity():
                                    nEventsPerRange = taskSpec.get_min_granularity()
                                elif "nEventsPerJob" in taskParamMap:
                                    nEventsPerJob = taskParamMap["nEventsPerJob"]
                                if "tgtNumEventsPerJob" in taskParamMap:
                                    tgtNumEventsPerJob = taskParamMap["tgtNumEventsPerJob"]
                                    # reset nEventsPerJob
                                    nEventsPerJob = None
                            # max attempts
                            maxAttempt = None
                            maxFailure = None
                            if datasetSpec.isMaster() or datasetSpec.toKeepTrack():
                                # max attempts
                                if taskSpec.disableAutoRetry():
                                    # disable auto retry
                                    maxAttempt = 1
                                elif "maxAttempt" in taskParamMap:
                                    maxAttempt = taskParamMap["maxAttempt"]
                                else:
                                    # use default value
                                    maxAttempt = 3
                                # max failure
                                if "maxFailure" in taskParamMap:
                                    maxFailure = taskParamMap["maxFailure"]
                            # first event number
                            firstEventNumber = None
                            if datasetSpec.isMaster():
                                # first event number
                                firstEventNumber = 1 + taskSpec.getFirstEventOffset()
                            # nMaxEvents
                            nMaxEvents = None
                            if datasetSpec.isMaster() and "nEvents" in taskParamMap:
                                nMaxEvents = taskParamMap["nEvents"]
                            # nMaxFiles
                            nMaxFiles = None
                            if "nFiles" in taskParamMap:
                                if datasetSpec.isMaster():
                                    nMaxFiles = taskParamMap["nFiles"]
                                else:
                                    # calculate for secondary
                                    if not datasetSpec.isPseudo():
                                        # check nFilesPerJob
                                        nFilesPerJobSec = datasetSpec.getNumFilesPerJob()
                                        if nFilesPerJobSec is not None:
                                            nMaxFiles = origNumFiles * nFilesPerJobSec
                                    # check ratio
                                    if nMaxFiles is None:
                                        nMaxFiles = datasetSpec.getNumMultByRatio(origNumFiles)
                                    # multiplied by the number of jobs per file for event-level splitting
                                    if nMaxFiles is not None:
                                        if "nEventsPerFile" in taskParamMap:
                                            if taskSpec.get_min_granularity():
                                                if taskParamMap["nEventsPerFile"] > taskSpec.get_min_granularity():
                                                    nMaxFiles *= float(taskParamMap["nEventsPerFile"]) / float(taskSpec.get_min_granularity())
                                                    nMaxFiles = int(math.ceil(nMaxFiles))
                                            elif "nEventsPerJob" in taskParamMap:
                                                if taskParamMap["nEventsPerFile"] > taskParamMap["nEventsPerJob"]:
                                                    nMaxFiles *= float(taskParamMap["nEventsPerFile"]) / float(taskParamMap["nEventsPerJob"])
                                                    nMaxFiles = int(math.ceil(nMaxFiles))
                                        elif "useRealNumEvents" in taskParamMap:
                                            # reset nMaxFiles since it is unknown
                                            nMaxFiles = None
                            # use scout
                            useScout = False
                            if datasetSpec.isMaster() and taskSpec.useScout() and (datasetSpec.status != "toupdate" or not taskSpec.isPostScout()):
                                useScout = True
                            # use files with new attempt numbers
                            useFilesWithNewAttemptNr = False
                            if not datasetSpec.isPseudo() and fileList != [] and "useInFilesWithNewAttemptNr" in taskParamMap:
                                useFilesWithNewAttemptNr = True
                            # ramCount
                            ramCount = 0
                            # skip short input
                            if (
                                datasetSpec.isMaster()
                                and not datasetSpec.isPseudo()
                                and nEventsPerFile is not None
                                and nEventsPerJob is not None
                                and nEventsPerFile >= nEventsPerJob
                                and "skipShortInput" in taskParamMap
                                and taskParamMap["skipShortInput"] is True
                            ):
                                skipShortInput = True
                            else:
                                skipShortInput = False
                            # skip short output
                            if datasetSpec.isMaster() and not datasetSpec.isPseudo() and taskParamMap.get("skipShortOutput"):
                                skip_short_output = True
                            else:
                                skip_short_output = False
                            # order by
                            if taskSpec.order_input_by() and datasetSpec.isMaster() and not datasetSpec.isPseudo():
                                orderBy = taskSpec.order_input_by()
                            else:
                                orderBy = None
                            # feed files to the contents table
                            tmpLog.debug("update contents")
                            retDB, missingFileList, nFilesUnique, diagMap = self.taskBufferIF.insertFilesForDataset_JEDI(
                                datasetSpec,
                                tmpRet,
                                tmpMetadata["state"],
                                stateUpdateTime,
                                nEventsPerFile,
                                nEventsPerJob,
                                maxAttempt,
                                firstEventNumber,
                                nMaxFiles,
                                nMaxEvents,
                                useScout,
                                fileList,
                                useFilesWithNewAttemptNr,
                                nFilesPerJob,
                                nEventsPerRange,
                                nChunksForScout,
                                includePatt,
                                excludePatt,
                                xmlConfig,
                                noWaitParent,
                                taskSpec.parent_tid,
                                self.pid,
                                maxFailure,
                                useRealNumEvents,
                                respectLB,
                                tgtNumEventsPerJob,
                                skipFilesUsedBy,
                                ramCount,
                                taskSpec,
                                skipShortInput,
                                inputPreStaging,
                                orderBy,
                                maxFileRecords,
                                skip_short_output,
                            )
                            if retDB is False:
                                taskSpec.setErrDiag(f"failed to insert files for {datasetSpec.datasetName}. {diagMap['errMsg']}")
                                allUpdated = False
                                taskBroken = True
                                break
                            elif retDB is None:
                                # the dataset is locked by another or status is not applicable
                                allUpdated = False
                                tmpLog.debug("escape since task or dataset is locked")
                                break
                            elif missingFileList != []:
                                # files are missing
                                tmpErrStr = f"{len(missingFileList)} files missing in {datasetSpec.datasetName}"
                                tmpLog.debug(tmpErrStr)
                                taskSpec.setErrDiag(tmpErrStr)
                                allUpdated = False
                                taskOnHold = True
                                missingMap[datasetSpec.datasetName] = {"datasetSpec": datasetSpec, "missingFiles": missingFileList}
                            else:
                                # reduce the number of files to be read
                                if "nFiles" in taskParamMap:
                                    if datasetSpec.isMaster():
                                        taskParamMap["nFiles"] -= nFilesUnique
                                # reduce the number of files for scout
                                if useScout:
                                    nChunksForScout = diagMap["nChunksForScout"]
                                # number of master input files
                                if datasetSpec.isMaster():
                                    checkedMaster = True
                                    nFilesMaster += nFilesUnique
                                    master_offset = datasetSpec.getOffset()
                            # running task
                            if diagMap["isRunningTask"]:
                                runningTask = True
                            # no activated pending input for noWait
                            if (
                                (noWaitParent or inputPreStaging)
                                and diagMap["nActivatedPending"] == 0
                                and not (useScout and nChunksForScout <= 0)
                                and tmpMetadata["state"] != "closed"
                                and datasetSpec.isMaster()
                            ):
                                tmpErrStr = "insufficient inputs are ready. "
                                if inputPreStaging:
                                    # message for pending for staging
                                    tmpErrStr += "inputs are staging from a tape. "
                                tmpErrStr += diagMap["errMsg"]
                                tmpLog.debug(tmpErrStr + ". skipping secondary datasets from now")
                                taskSpec.setErrDiag(tmpErrStr)
                                taskOnHold = True
                                setFrozenTime = False
                                skip_secondaries = True
                    tmpLog.debug("end loop")
            # no master input
            if not taskOnHold and not taskBroken and allUpdated and nFilesMaster == 0 and checkedMaster:
                tmpErrStr = "no master input files. input dataset is empty"
                if master_offset:
                    tmpErrStr += f" with offset={master_offset}"
                tmpLog.error(tmpErrStr)
                taskSpec.setErrDiag(tmpErrStr, None)
                if noWaitParent:
                    # parent is running
                    taskOnHold = True
                else:
                    # the task has no parent or parent is finished
                    if master_is_open and taskSpec.runUntilClosed():
                        # wait until the input dataset is closed
                        taskOnHold = True
                    elif not taskSpec.allowEmptyInput():
                        # empty input is not allowed
                        taskBroken = True
                    else:
                        # finish the task with empty input
                        taskToFinish = True
            # index consistency
            if not taskOnHold and not taskBroken and len(datasetsIdxConsistency) > 0:
                self.taskBufferIF.removeFilesIndexInconsistent_JEDI(jediTaskID, datasetsIdxConsistency)
            # update task status
            if taskBroken or taskToFinish:
                if taskBroken:
                    # task is broken
                    taskSpec.status = "tobroken"
                else:
                    taskSpec.status = "finishing"
                tmpMsg = f"set task_status={taskSpec.status}"
                tmpLog.info(tmpMsg)
                tmpLog.sendMsg(tmpMsg, self.msgType)
                allRet = self.taskBufferIF.updateTaskStatusByContFeeder_JEDI(jediTaskID, taskSpec, pid=self.pid)
            # change task status unless the task is running
            if not runningTask:
                # send prestaging request
                if taskSpec.inputPreStaging() and taskSpec.is_first_contents_feed():
                    tmpStat, tmpErrStr = self.send_prestaging_request(taskSpec, taskParamMap, dsList, tmpLog)
                    if tmpStat:
                        taskSpec.set_first_contents_feed(False)
                    else:
                        tmpLog.debug(tmpErrStr)
                        taskSpec.setErrDiag(tmpErrStr)
                        taskOnHold = True
                if taskOnHold:
                    # go to pending state
                    if taskSpec.status not in ["broken", "tobroken", "finishing"]:
                        taskSpec.setOnHold()
                    tmpMsg = f"set task_status={taskSpec.status}"
                    tmpLog.info(tmpMsg)
                    tmpLog.sendMsg(tmpMsg, self.msgType)
                    allRet = self.taskBufferIF.updateTaskStatusByContFeeder_JEDI(jediTaskID, taskSpec, pid=self.pid, setFrozenTime=setFrozenTime)
                elif allUpdated:
                    # all OK
                    allRet, newTaskStatus = self.taskBufferIF.updateTaskStatusByContFeeder_JEDI(
                        jediTaskID, getTaskStatus=True, pid=self.pid, useWorldCloud=taskSpec.useWorldCloud()
                    )
                    tmpMsg = f"set task_status={newTaskStatus}"
                    tmpLog.info(tmpMsg)
                    tmpLog.sendMsg(tmpMsg, self.msgType)
                # just unlock
                retUnlock = self.taskBufferIF.unlockSingleTask_JEDI(jediTaskID, self.pid)
                tmpLog.debug(f"unlock not-running task with {retUnlock}")
            else:
                # just unlock
                retUnlock = self.taskBufferIF.unlockSingleTask_JEDI(jediTaskID, self.pid)
                tmpLog.debug(f"unlock task with {retUnlock}")
            # send message to job generator if new inputs are ready
            if not taskOnHold and not taskBroken and allUpdated and taskSpec.is_msg_driven():
                push_ret = self.taskBufferIF.push_task_trigger_message("jedi_job_generator", jediTaskID, task_spec=taskSpec)
                if push_ret:
                    tmpLog.debug("pushed trigger message to jedi_job_generator")
                else:
                    tmpLog.warning("failed to push trigger message to jedi_job_generator")
            tmpLog.debug("done")

    # update dataset
    def updateDatasetStatus(self, datasetSpec, datasetStatus, tmpLog, datasetState=None):
        # update dataset status
        datasetSpec.status = datasetStatus
        datasetSpec.lockedBy = None
        if datasetState:
            datasetSpec.state = datasetState
        tmpLog.info(f"update dataset status to {datasetSpec.status} state to {datasetSpec.state}")
        self.taskBufferIF.updateDataset_JEDI(datasetSpec, {"datasetID": datasetSpec.datasetID, "jediTaskID": datasetSpec.jediTaskID}, lockTask=True)

    # send prestaging request
    def send_prestaging_request(self, task_spec, task_params_map, ds_list, tmp_log):
        try:
            c = iDDS_Client(idds.common.utils.get_rest_host())
            for datasetSpec in ds_list:
                if datasetSpec.is_no_staging():
                    # skip no_staging
                    continue
                # get rule
                try:
                    tmp_scope, tmp_name = datasetSpec.datasetName.split(":")
                    tmp_name = re.sub("/$", "", tmp_name)
                except Exception:
                    continue
                try:
                    if "prestagingRuleID" in task_params_map:
                        if tmp_name not in task_params_map["prestagingRuleID"]:
                            continue
                        rule_id = task_params_map["prestagingRuleID"][tmp_name]
                    elif "selfPrestagingRule" in task_params_map:
                        if not datasetSpec.isMaster() or datasetSpec.isPseudo():
                            continue
                        rule_id = self.ddmIF.getInterface(task_spec.vo, task_spec.cloud).make_staging_rule(
                            tmp_scope + ":" + tmp_name, task_params_map["selfPrestagingRule"]
                        )
                        if not rule_id:
                            continue
                    else:
                        rule_id = self.ddmIF.getInterface(task_spec.vo, task_spec.cloud).getActiveStagingRule(tmp_scope + ":" + tmp_name)
                        if rule_id is None:
                            continue
                except Exception as e:
                    return False, f"DDM error : {str(e)}"
                # request
                tmp_log.debug(f"sending request to iDDS for {datasetSpec.datasetName}")
                req = {
                    "scope": tmp_scope,
                    "name": tmp_name,
                    "requester": "panda",
                    "request_type": idds.common.constants.RequestType.StageIn,
                    "transform_tag": idds.common.constants.RequestType.StageIn.value,
                    "status": idds.common.constants.RequestStatus.New,
                    "priority": 0,
                    "lifetime": 30,
                    "request_metadata": {
                        "workload_id": task_spec.jediTaskID,
                        "rule_id": rule_id,
                    },
                }
                tmp_log.debug(f"req {str(req)}")
                ret = c.add_request(**req)
                tmp_log.debug(f"got requestID={str(ret)}")
        except Exception as e:
            return False, f"iDDS error : {str(e)}"
        return True, None


# launch


def launcher(commuChannel, taskBufferIF, ddmIF, vos=None, prodSourceLabels=None):
    p = ContentsFeeder(commuChannel, taskBufferIF, ddmIF, vos, prodSourceLabels)
    p.start()
