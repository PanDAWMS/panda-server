import copy
import datetime
import math
import os
import random
import re
import sys
import traceback

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandautils.PandaUtils import (
    batched,
    get_sql_IN_bind_variables,
    naive_utcnow,
)

from pandaserver.config import panda_config
from pandaserver.srvcore import CoreUtils
from pandaserver.taskbuffer import EventServiceUtils, JobUtils, ParseJobXML
from pandaserver.taskbuffer.db_proxy_mods.base_module import BaseModule, varNUMBER
from pandaserver.taskbuffer.db_proxy_mods.job_complex_module import (
    get_job_complex_module,
)
from pandaserver.taskbuffer.db_proxy_mods.metrics_module import get_metrics_module
from pandaserver.taskbuffer.db_proxy_mods.task_event_module import get_task_event_module
from pandaserver.taskbuffer.db_proxy_mods.task_utils_module import get_task_utils_module
from pandaserver.taskbuffer.InputChunk import InputChunk
from pandaserver.taskbuffer.JediDatasetSpec import (
    INPUT_TYPES_var_map,
    INPUT_TYPES_var_str,
    JediDatasetSpec,
    MERGE_TYPES_var_map,
    MERGE_TYPES_var_str,
    PROCESS_TYPES_var_map,
    PROCESS_TYPES_var_str,
)
from pandaserver.taskbuffer.JediFileSpec import JediFileSpec
from pandaserver.taskbuffer.JediTaskSpec import JediTaskSpec, is_msg_driven
from pandaserver.taskbuffer.WorkQueue import WorkQueue


# Module class to define task related methods that use other modules' methods
class TaskComplexModule(BaseModule):
    # constructor
    def __init__(self, log_stream: LogWrapper):
        super().__init__(log_stream)

    # get the list of datasets to feed contents to DB
    def getDatasetsToFeedContents_JEDI(self, vo, prodSourceLabel, task_id=None, force_read=False):
        """Get the list of datasets to feed contents to DB

        :param vo: VO
        :param prodSourceLabel: production source label
        :param task_id: task ID (optional)
        :param force_read: force read from DB regardless of task status when task_id is specified (default: False)

        :return: list of (jediTaskID, [JediDatasetSpec, ...]) or None in case of error
        """
        comment = " /* JediDBProxy.getDatasetsToFeedContents_JEDI */"
        if task_id is not None:
            tmpLog = self.create_tagged_logger(comment, f"vo={vo} label={prodSourceLabel} taskid={task_id}")
        else:
            tmpLog = self.create_tagged_logger(comment, f"vo={vo} label={prodSourceLabel}")
        tmpLog.debug("start")
        try:
            # SQL
            varMap = {}
            if not force_read:
                varMap[":ts_running"] = "running"
                varMap[":ts_scouting"] = "scouting"
                varMap[":ts_ready"] = "ready"
                varMap[":ts_defined"] = "defined"
                varMap[":dsStatus_pending"] = "pending"
                varMap[":dsState_mutable"] = "mutable"
                if task_id is None:
                    try:
                        checkInterval = self.jedi_config.confeeder.checkInterval
                    except Exception:
                        checkInterval = 60
                else:
                    checkInterval = 0
                varMap[":checkTimeLimit"] = naive_utcnow() - datetime.timedelta(minutes=checkInterval)
                varMap[":lockTimeLimit"] = naive_utcnow() - datetime.timedelta(minutes=10)
            sql = f"SELECT {JediDatasetSpec.columnNames('tabD')} "
            if task_id is None:
                sql += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_Datasets tabD,{0}.JEDI_AUX_Status_MinTaskID tabA ".format(panda_config.schemaJEDI)
                sql += "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID "
            else:
                varMap[":task_id"] = task_id
                sql += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_Datasets tabD ".format(panda_config.schemaJEDI)
                sql += "WHERE tabT.jediTaskID=:task_id "
            if not force_read:
                sql += "AND (tabT.lockedTime IS NULL OR tabT.lockedTime<:lockTimeLimit) "
            if vo not in [None, "any"]:
                varMap[":vo"] = vo
                sql += "AND tabT.vo=:vo "
            if prodSourceLabel not in [None, "any"]:
                varMap[":prodSourceLabel"] = prodSourceLabel
                sql += "AND tabT.prodSourceLabel=:prodSourceLabel "
            sql += "AND tabT.jediTaskID=tabD.jediTaskID "
            sql += f"AND type IN ({INPUT_TYPES_var_str}) "
            varMap.update(INPUT_TYPES_var_map)
            if not force_read:
                ds_status_var_names_str, ds_status_var_map = get_sql_IN_bind_variables(
                    JediDatasetSpec.statusToUpdateContents(), prefix=":dsStatus_", value_as_suffix=True
                )
                sql += f" AND ((tabT.status=:ts_defined AND tabD.status IN ({ds_status_var_names_str})) "
                varMap.update(ds_status_var_map)
                sql += "OR (tabT.status IN (:ts_running,:ts_scouting,:ts_ready,:ts_defined) "
                sql += "AND tabD.state=:dsState_mutable AND tabD.stateCheckTime<=:checkTimeLimit)) "
                sql += "AND tabT.lockedBy IS NULL AND tabD.lockedBy IS NULL "
                sql += "AND NOT EXISTS "
                sql += f"(SELECT 1 FROM {panda_config.schemaJEDI}.JEDI_Datasets "
                sql += f"WHERE {panda_config.schemaJEDI}.JEDI_Datasets.jediTaskID=tabT.jediTaskID "
                sql += f"AND type IN ({INPUT_TYPES_var_str}) "
                sql += "AND status=:dsStatus_pending) "
            # begin transaction
            self.conn.begin()
            self.cur.arraysize = 10000
            tmpLog.debug(sql + comment + str(varMap))
            self.cur.execute(sql + comment, varMap)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            resList = self.cur.fetchall()
            returnMap = {}
            taskDatasetMap = {}
            nDS = 0
            for res in resList:
                datasetSpec = JediDatasetSpec()
                datasetSpec.pack(res)
                if datasetSpec.jediTaskID not in returnMap:
                    returnMap[datasetSpec.jediTaskID] = []
                returnMap[datasetSpec.jediTaskID].append(datasetSpec)
                nDS += 1
                if datasetSpec.jediTaskID not in taskDatasetMap:
                    taskDatasetMap[datasetSpec.jediTaskID] = []
                taskDatasetMap[datasetSpec.jediTaskID].append(datasetSpec.datasetID)
            jediTaskIDs = sorted(returnMap.keys())
            # get seq_number
            sqlSEQ = f"SELECT {JediDatasetSpec.columnNames()} "
            sqlSEQ += f"FROM {panda_config.schemaJEDI}.JEDI_Datasets "
            sqlSEQ += "WHERE jediTaskID=:jediTaskID AND datasetName=:datasetName "
            for jediTaskID in jediTaskIDs:
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                varMap[":datasetName"] = "seq_number"
                self.conn.begin()
                self.cur.execute(sqlSEQ + comment, varMap)
                # commit
                if not self._commit():
                    raise RuntimeError("Commit error")
                resSeqList = self.cur.fetchall()
                for resSeq in resSeqList:
                    datasetSpec = JediDatasetSpec()
                    datasetSpec.pack(resSeq)
                    # append if missing
                    if datasetSpec.datasetID not in taskDatasetMap[datasetSpec.jediTaskID]:
                        taskDatasetMap[datasetSpec.jediTaskID].append(datasetSpec.datasetID)
                        returnMap[datasetSpec.jediTaskID].append(datasetSpec)
            returnList = []
            for jediTaskID in jediTaskIDs:
                returnList.append((jediTaskID, returnMap[jediTaskID]))
            tmpLog.debug(f"got {nDS} datasets for {len(jediTaskIDs)} tasks")
            return returnList
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return None

    # feed files to the JEDI contents table
    def insertFilesForDataset_JEDI(
        self,
        datasetSpec,
        fileMap,
        datasetState,
        stateUpdateTime,
        nEventsPerFile,
        nEventsPerJob,
        maxAttempt,
        firstEventNumber,
        nMaxFiles,
        nMaxEvents,
        useScout,
        givenFileList,
        useFilesWithNewAttemptNr,
        nFilesPerJob,
        nEventsPerRange,
        nChunksForScout,
        includePatt,
        excludePatt,
        xmlConfig,
        noWaitParent,
        parent_tid,
        pid,
        maxFailure,
        useRealNumEvents,
        respectLB,
        tgtNumEventsPerJob,
        skipFilesUsedBy,
        ramCount,
        taskSpec,
        skipShortInput,
        inputPreStaging,
        order_by,
        maxFileRecords,
        skip_short_output,
    ):
        comment = " /* JediDBProxy.insertFilesForDataset_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"jediTaskID={datasetSpec.jediTaskID} datasetID={datasetSpec.datasetID}")
        tmpLog.debug(f"start nEventsPerFile={nEventsPerFile} nEventsPerJob={nEventsPerJob} maxAttempt={maxAttempt} maxFailure={maxFailure}")
        tmpLog.debug(f"firstEventNumber={firstEventNumber} nMaxFiles={nMaxFiles} nMaxEvents={nMaxEvents}")
        tmpLog.debug(f"useFilesWithNewAttemptNr={useFilesWithNewAttemptNr} nFilesPerJob={nFilesPerJob} nEventsPerRange={nEventsPerRange}")
        tmpLog.debug(f"useScout={useScout} nChunksForScout={nChunksForScout} userRealEventNumber={useRealNumEvents}")
        tmpLog.debug(f"includePatt={str(includePatt)} excludePatt={str(excludePatt)}")
        tmpLog.debug(f"xmlConfig={type(xmlConfig)} noWaitParent={noWaitParent} parent_tid={parent_tid}")
        tmpLog.debug(f"len(fileMap)={len(fileMap)} pid={pid}")
        tmpLog.debug(f"datasetState={datasetState} dataset.state={datasetSpec.state}")
        tmpLog.debug(f"respectLB={respectLB} tgtNumEventsPerJob={tgtNumEventsPerJob} skipFilesUsedBy={skipFilesUsedBy} ramCount={ramCount}")
        tmpLog.debug(f"skipShortInput={skipShortInput} skipShortOutput={skip_short_output} inputPreStaging={inputPreStaging} order_by={order_by}")
        # return value for failure
        diagMap = {"errMsg": "", "nChunksForScout": nChunksForScout, "nActivatedPending": 0, "isRunningTask": False}
        failedRet = False, 0, None, diagMap
        harmlessRet = None, 0, None, diagMap
        regStart = naive_utcnow()
        # mutable
        fake_mutable_for_skip_short_output = False
        if (noWaitParent or inputPreStaging) and datasetState == "mutable":
            isMutableDataset = True
        elif skip_short_output:
            # treat as mutable to skip short output by using the SR mechanism
            isMutableDataset = True
            fake_mutable_for_skip_short_output = True
        else:
            isMutableDataset = False
        tmpLog.debug(f"isMutableDataset={isMutableDataset} (fake={fake_mutable_for_skip_short_output}) respectSplitRule={taskSpec.respectSplitRule()}")
        # event level splitting
        if nEventsPerJob is not None and nFilesPerJob is None:
            isEventSplit = True
        else:
            isEventSplit = False
        try:
            # current date
            timeNow = naive_utcnow()
            # get list of files produced by parent
            if datasetSpec.checkConsistency():
                # sql to get the list
                sqlPPC = "SELECT lfn FROM {0}.JEDI_Datasets tabD,{0}.JEDI_Dataset_Contents tabC ".format(panda_config.schemaJEDI)
                sqlPPC += "WHERE tabD.jediTaskID=tabC.jediTaskID AND tabD.datasetID=tabC.datasetID "
                sqlPPC += "AND tabD.jediTaskID=:jediTaskID AND tabD.type IN (:type1,:type2) "
                sqlPPC += "AND tabD.datasetName IN (:dsName,:didName) AND tabC.status=:fileStatus "
                varMap = {}
                varMap[":type1"] = "output"
                varMap[":type2"] = "log"
                varMap[":jediTaskID"] = parent_tid
                varMap[":fileStatus"] = "finished"
                varMap[":didName"] = datasetSpec.datasetName
                varMap[":dsName"] = datasetSpec.datasetName.split(":")[-1]
                # begin transaction
                self.conn.begin()
                self.cur.execute(sqlPPC + comment, varMap)
                tmpPPC = self.cur.fetchall()
                producedFileList = set()
                for (tmpLFN,) in tmpPPC:
                    producedFileList.add(tmpLFN)
                # commit
                if not self._commit():
                    raise RuntimeError("Commit error")
                # check if files are 'finished' in JEDI table
                newFileMap = {}
                for guid, fileVal in fileMap.items():
                    if fileVal["lfn"] in producedFileList:
                        newFileMap[guid] = fileVal
                    else:
                        tmpLog.debug(f"{fileVal['lfn']} skipped since was not properly produced by the parent according to JEDI table")
                fileMap = newFileMap
            # get files used by another task
            usedFilesToSkip = set()
            if skipFilesUsedBy is not None:
                # sql to get the list
                sqlSFU = "SELECT lfn,startEvent,endEvent FROM {0}.JEDI_Datasets tabD,{0}.JEDI_Dataset_Contents tabC ".format(panda_config.schemaJEDI)
                sqlSFU += "WHERE tabD.jediTaskID=tabC.jediTaskID AND tabD.datasetID=tabC.datasetID "
                sqlSFU += "AND tabD.jediTaskID=:jediTaskID AND tabD.type IN (:type1,:type2) "
                sqlSFU += "AND tabD.datasetName IN (:dsName,:didName) AND tabC.status=:fileStatus "
                for tmpTaskID in str(skipFilesUsedBy).split(","):
                    varMap = {}
                    varMap[":type1"] = "input"
                    varMap[":type2"] = "pseudo_input"
                    varMap[":jediTaskID"] = tmpTaskID
                    varMap[":fileStatus"] = "finished"
                    varMap[":didName"] = datasetSpec.datasetName
                    varMap[":dsName"] = datasetSpec.datasetName.split(":")[-1]
                    try:
                        # begin transaction
                        self.conn.begin()
                        self.cur.execute(sqlSFU + comment, varMap)
                        tmpSFU = self.cur.fetchall()
                        for tmpLFN, tmpStartEvent, tmpEndEvent in tmpSFU:
                            tmpID = f"{tmpLFN}.{tmpStartEvent}.{tmpEndEvent}"
                            usedFilesToSkip.add(tmpID)
                            # add the file itself if the number of events was not specified in the old task
                            if tmpStartEvent is None:
                                usedFilesToSkip.add(tmpLFN)
                        # commit
                        if not self._commit():
                            raise RuntimeError("Commit error")
                    except Exception:
                        # roll back
                        self._rollback()
                        # error
                        self.dump_error_message(tmpLog)
                        return failedRet
            # include files
            if includePatt != []:
                newFileMap = {}
                for guid, fileVal in fileMap.items():
                    if get_task_utils_module(self).isMatched(fileVal["lfn"], includePatt):
                        newFileMap[guid] = fileVal
                fileMap = newFileMap
            # exclude files
            if excludePatt != []:
                newFileMap = {}
                for guid, fileVal in fileMap.items():
                    if not get_task_utils_module(self).isMatched(fileVal["lfn"], excludePatt):
                        newFileMap[guid] = fileVal
                fileMap = newFileMap
            # file list is given
            givenFileMap = {}
            if givenFileList != []:
                for tmpFileItem in givenFileList:
                    if isinstance(tmpFileItem, dict):
                        tmpLFN = tmpFileItem["lfn"]
                        fileItem = tmpFileItem
                    else:
                        tmpLFN = tmpFileItem
                        fileItem = {"lfn": tmpFileItem}
                    givenFileMap[tmpLFN] = fileItem
                newFileMap = {}
                for guid, fileVal in fileMap.items():
                    if fileVal["lfn"] in givenFileMap:
                        newFileMap[guid] = fileVal
                fileMap = newFileMap
            # XML config
            if xmlConfig is not None:
                try:
                    xmlConfig = ParseJobXML.dom_parser(xmlStr=xmlConfig)
                except Exception:
                    errtype, errvalue = sys.exc_info()[:2]
                    tmpErrStr = f"failed to load XML config with {errtype.__name__}:{errvalue}"
                    raise RuntimeError(tmpErrStr)
                newFileMap = {}
                for guid, fileVal in fileMap.items():
                    if fileVal["lfn"] in xmlConfig.files_in_DS(datasetSpec.datasetName):
                        newFileMap[guid] = fileVal
                fileMap = newFileMap
            # make map with LFN as key
            filelValMap = {}
            for guid, fileVal in fileMap.items():
                filelValMap[fileVal["lfn"]] = (guid, fileVal)
            # make LFN list
            listBoundaryID = []
            if order_by == "eventsAlignment" and nEventsPerJob:
                aligned = []
                unaligned = dict()
                for tmpLFN, (tmpGUID, tmpFileVar) in filelValMap.items():
                    if "events" in tmpFileVar and int(tmpFileVar["events"]) % nEventsPerJob == 0:
                        aligned.append(tmpLFN)
                    else:
                        unaligned[tmpLFN] = int(tmpFileVar["events"])
                aligned.sort()
                unaligned = sorted(unaligned, key=lambda i: unaligned[i], reverse=True)
                lfnList = aligned + unaligned
            elif xmlConfig is None:
                # sort by LFN
                lfnList = sorted(filelValMap.keys())
            else:
                # sort as described in XML
                tmpBoundaryID = 0
                lfnList = []
                for tmpJobXML in xmlConfig.jobs:
                    for tmpLFN in tmpJobXML.files_in_DS(datasetSpec.datasetName):
                        # check if the file is available
                        if tmpLFN not in filelValMap:
                            diagMap["errMsg"] = f"{tmpLFN} is not found in {datasetSpec.datasetName}"
                            tmpLog.error(diagMap["errMsg"])
                            return failedRet
                        lfnList.append(tmpLFN)
                        listBoundaryID.append(tmpBoundaryID)
                    # increment boundaryID
                    tmpBoundaryID += 1
            # truncate if necessary
            if datasetSpec.isSeqNumber():
                offsetVal = 0
            else:
                offsetVal = datasetSpec.getOffset()
            if offsetVal > 0:
                lfnList = lfnList[offsetVal:]
            tmpLog.debug(f"offset={offsetVal}")
            # randomize
            if datasetSpec.isRandom():
                random.shuffle(lfnList)
            # use perRange as perJob
            if nEventsPerJob is None and nEventsPerRange is not None:
                nEventsPerJob = nEventsPerRange
            # make file specs
            fileSpecMap = {}
            uniqueFileKeyList = []
            nRemEvents = nEventsPerJob
            totalEventNumber = firstEventNumber
            uniqueLfnList = {}
            totalNumEventsF = 0
            lumiBlockNr = None
            for tmpIdx, tmpLFN in enumerate(lfnList):
                # check if the file should be skipped
                if tmpLFN in usedFilesToSkip:
                    continue
                # collect unique LFN list
                if tmpLFN not in uniqueLfnList:
                    uniqueLfnList[tmpLFN] = None
                # check if enough files
                if nMaxFiles is not None and len(uniqueLfnList) > nMaxFiles:
                    break
                guid, fileVal = filelValMap[tmpLFN]
                fileSpec = JediFileSpec()
                fileSpec.jediTaskID = datasetSpec.jediTaskID
                fileSpec.datasetID = datasetSpec.datasetID
                fileSpec.GUID = guid
                fileSpec.type = datasetSpec.type
                fileSpec.status = "ready"
                fileSpec.proc_status = "ready"
                fileSpec.lfn = fileVal["lfn"]
                fileSpec.scope = fileVal["scope"]
                fileSpec.fsize = fileVal["filesize"]
                fileSpec.checksum = fileVal["checksum"]
                fileSpec.creationDate = timeNow
                fileSpec.attemptNr = 0
                fileSpec.failedAttempt = 0
                fileSpec.maxAttempt = maxAttempt
                fileSpec.maxFailure = maxFailure
                fileSpec.ramCount = ramCount
                tmpNumEvents = None
                if "events" in fileVal:
                    try:
                        tmpNumEvents = int(fileVal["events"])
                    except Exception:
                        pass
                if skipShortInput and tmpNumEvents is not None:
                    # set multiples of nEventsPerJob if actual nevents is small
                    if tmpNumEvents >= nEventsPerFile:
                        fileSpec.nEvents = nEventsPerFile
                    else:
                        fileSpec.nEvents = int(tmpNumEvents // nEventsPerJob) * nEventsPerJob
                        if fileSpec.nEvents == 0:
                            tmpLog.debug(f"skip {fileSpec.lfn} due to nEvents {tmpNumEvents} < nEventsPerJob {nEventsPerJob}")
                            continue
                        else:
                            tmpLog.debug(f"set nEvents to {fileSpec.nEvents} from {tmpNumEvents} for {fileSpec.lfn} to skip short input")
                elif nEventsPerFile is not None:
                    fileSpec.nEvents = nEventsPerFile
                elif "events" in fileVal and fileVal["events"] not in ["None", None]:
                    try:
                        fileSpec.nEvents = int(fileVal["events"])
                    except Exception:
                        fileSpec.nEvents = None
                if "lumiblocknr" in fileVal:
                    try:
                        fileSpec.lumiBlockNr = int(fileVal["lumiblocknr"])
                    except Exception:
                        pass
                # keep track
                if datasetSpec.toKeepTrack():
                    fileSpec.keepTrack = 1
                tmpFileSpecList = []
                if xmlConfig is not None:
                    # splitting with XML
                    fileSpec.boundaryID = listBoundaryID[tmpIdx]
                    tmpFileSpecList.append(fileSpec)
                elif (
                    ((nEventsPerJob is None or nEventsPerJob <= 0) and (tgtNumEventsPerJob is None or tgtNumEventsPerJob <= 0))
                    or fileSpec.nEvents is None
                    or fileSpec.nEvents <= 0
                    or ((nEventsPerFile is None or nEventsPerFile <= 0) and not useRealNumEvents)
                ):
                    if firstEventNumber is not None and nEventsPerFile is not None:
                        fileSpec.firstEvent = totalEventNumber
                        totalEventNumber += fileSpec.nEvents
                    # file-level splitting
                    tmpFileSpecList.append(fileSpec)
                else:
                    # event-level splitting
                    tmpStartEvent = 0
                    # change nEventsPerJob if target number is specified
                    if tgtNumEventsPerJob is not None and tgtNumEventsPerJob > 0:
                        # calcurate to how many chunks the file is split
                        tmpItem = divmod(fileSpec.nEvents, tgtNumEventsPerJob)
                        nSubChunk = tmpItem[0]
                        if tmpItem[1] > 0:
                            nSubChunk += 1
                        if nSubChunk <= 0:
                            nSubChunk = 1
                        # get nEventsPerJob
                        tmpItem = divmod(fileSpec.nEvents, nSubChunk)
                        nEventsPerJob = tmpItem[0]
                        if tmpItem[1] > 0:
                            nEventsPerJob += 1
                        if nEventsPerJob <= 0:
                            nEventsPerJob = 1
                        nRemEvents = nEventsPerJob
                    # LB boundaries
                    if respectLB:
                        if lumiBlockNr is None or lumiBlockNr != fileSpec.lumiBlockNr:
                            lumiBlockNr = fileSpec.lumiBlockNr
                            nRemEvents = nEventsPerJob
                    # make file specs
                    while nRemEvents > 0:
                        splitFileSpec = copy.copy(fileSpec)
                        if tmpStartEvent + nRemEvents >= splitFileSpec.nEvents:
                            splitFileSpec.startEvent = tmpStartEvent
                            splitFileSpec.endEvent = splitFileSpec.nEvents - 1
                            nRemEvents -= splitFileSpec.nEvents - tmpStartEvent
                            if nRemEvents == 0:
                                nRemEvents = nEventsPerJob
                            if firstEventNumber is not None and (nEventsPerFile is not None or useRealNumEvents):
                                splitFileSpec.firstEvent = totalEventNumber
                                totalEventNumber += splitFileSpec.endEvent - splitFileSpec.startEvent + 1
                            tmpFileSpecList.append(splitFileSpec)
                            break
                        else:
                            splitFileSpec.startEvent = tmpStartEvent
                            splitFileSpec.endEvent = tmpStartEvent + nRemEvents - 1
                            tmpStartEvent += nRemEvents
                            nRemEvents = nEventsPerJob
                            if firstEventNumber is not None and (nEventsPerFile is not None or useRealNumEvents):
                                splitFileSpec.firstEvent = totalEventNumber
                                totalEventNumber += splitFileSpec.endEvent - splitFileSpec.startEvent + 1
                            tmpFileSpecList.append(splitFileSpec)
                        if len(tmpFileSpecList) >= maxFileRecords:
                            break
                # append
                for fileSpec in tmpFileSpecList:
                    # check if to skip
                    tmpID = f"{fileSpec.lfn}.{fileSpec.startEvent}.{fileSpec.endEvent}"
                    # check file with event range when the number of events is identical between old and new tasks
                    if tmpID in usedFilesToSkip:
                        continue
                    # check the file when old task didn't specify the number of events
                    if fileSpec.lfn in usedFilesToSkip:
                        continue
                    # append
                    uniqueFileKey = f"{fileSpec.lfn}.{fileSpec.startEvent}.{fileSpec.endEvent}.{fileSpec.boundaryID}"
                    uniqueFileKeyList.append(uniqueFileKey)
                    fileSpecMap[uniqueFileKey] = fileSpec
                # check if number of events is enough
                if fileSpec.nEvents is not None:
                    totalNumEventsF += fileSpec.nEvents
                if nMaxEvents is not None and totalNumEventsF >= nMaxEvents:
                    break
                # too long list
                if len(uniqueFileKeyList) > maxFileRecords:
                    if len(fileMap) > maxFileRecords and nMaxFiles is None:
                        diagMap["errMsg"] = f"Input dataset contains too many files >{maxFileRecords}. Split the dataset or set nFiles properly"
                    elif nEventsPerJob is not None:
                        diagMap["errMsg"] = (
                            f"SUM(nEventsInEachFile/nEventsPerJob) >{maxFileRecords}. Split the dataset, set nFiles properly, or increase nEventsPerJob"
                        )
                    else:
                        diagMap["errMsg"] = f"Too many file record >{maxFileRecords}"
                    tmpLog.error(diagMap["errMsg"])
                    return failedRet
            missingFileList = []
            tmpLog.debug(f"{len(missingFileList)} files missing while {len(uniqueFileKeyList)} unique files")
            # sql to check if task is locked
            sqlTL = f"SELECT status,lockedBy FROM {panda_config.schemaJEDI}.JEDI_Tasks WHERE jediTaskID=:jediTaskID FOR UPDATE NOWAIT "
            # sql to check dataset status
            sqlDs = f"SELECT status,nFilesToBeUsed-nFilesUsed,state,nFilesToBeUsed,nFilesUsed FROM {panda_config.schemaJEDI}.JEDI_Datasets "
            sqlDs += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID FOR UPDATE "
            # sql to get existing files
            sqlCh = "SELECT fileID,lfn,status,startEvent,endEvent,boundaryID,nEvents,lumiBlockNr,attemptNr,maxAttempt,failedAttempt,maxFailure FROM {0}.JEDI_Dataset_Contents ".format(
                panda_config.schemaJEDI
            )
            sqlCh += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID FOR UPDATE "
            # sql to count existing files
            sqlCo = f"SELECT count(*) FROM {panda_config.schemaJEDI}.JEDI_Dataset_Contents "
            sqlCo += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            # sql for insert
            sqlIn = f"INSERT INTO {panda_config.schemaJEDI}.JEDI_Dataset_Contents ({JediFileSpec.columnNames()}) "
            sqlIn += JediFileSpec.bindValuesExpression(useSeq=False)
            # sql to get fileID
            sqlFID = f"SELECT {panda_config.schemaJEDI}.JEDI_DATASET_CONT_FILEID_SEQ.nextval FROM "
            sqlFID += "(SELECT level FROM dual CONNECT BY level<=:nIDs) "
            # sql to update file counts
            sqlFU = f"UPDATE {panda_config.schemaJEDI}.JEDI_Dataset_Contents SET status=:status "
            sqlFU += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
            # sql to get master status
            sqlMS = f"SELECT status FROM {panda_config.schemaJEDI}.JEDI_Datasets "
            sqlMS += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            # sql to update dataset
            sqlDU = f"UPDATE {panda_config.schemaJEDI}.JEDI_Datasets "
            sqlDU += "SET status=:status,state=:state,stateCheckTime=:stateUpdateTime,"
            sqlDU += "nFiles=:nFiles,nFilesTobeUsed=:nFilesTobeUsed,nEvents=:nEvents," "nFilesMissing=:nFilesMissing "
            sqlDU += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            # sql to update dataset including nFilesUsed
            sqlDUx = f"UPDATE {panda_config.schemaJEDI}.JEDI_Datasets "
            sqlDUx += "SET status=:status,state=:state,stateCheckTime=:stateUpdateTime,"
            sqlDUx += "nFiles=:nFiles,nFilesTobeUsed=:nFilesTobeUsed,nEvents=:nEvents," "nFilesUsed=:nFilesUsed,nFilesMissing=:nFilesMissing "
            sqlDUx += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            # sql to propagate number of input events to DEFT
            sqlCE = f"UPDATE {panda_config.schemaDEFT}.T_TASK "
            sqlCE += "SET total_input_events=LEAST(9999999999,("
            sqlCE += f"SELECT SUM(nEvents) FROM {panda_config.schemaJEDI}.JEDI_Datasets "
            sqlCE += "WHERE jediTaskID=:jediTaskID "
            sqlCE += f"AND type IN ({INPUT_TYPES_var_str}) "
            sqlCE += "AND masterID IS NULL)) "
            sqlCE += "WHERE taskID=:jediTaskID "
            nInsert = 0
            nReady = 0
            nPending = 0
            nUsed = 0
            nLost = 0
            nStaging = 0
            nFailed = 0
            pendingFID = []
            oldDsStatus = None
            newDsStatus = None
            nActivatedPending = 0
            nEventsToUseEventSplit = 0
            nFilesToUseEventSplit = 0
            nFilesUnprocessed = 0
            nEventsInsert = 0
            nEventsLost = 0
            nEventsExist = 0
            stagingLB = set()
            retVal = None, missingFileList, None, diagMap
            # begin transaction
            self.conn.begin()
            # check task
            try:
                varMap = {}
                varMap[":jediTaskID"] = datasetSpec.jediTaskID
                self.cur.execute(sqlTL + comment, varMap)
                resTask = self.cur.fetchone()
            except Exception:
                errType, errValue = sys.exc_info()[:2]
                if self.isNoWaitException(errValue):
                    # resource busy and acquire with NOWAIT specified
                    tmpLog.debug(f"skip locked jediTaskID={datasetSpec.jediTaskID}")
                    if not self._commit():
                        raise RuntimeError("Commit error")
                    return retVal
                else:
                    # failed with something else
                    raise errType(errValue)
            if resTask is None:
                tmpLog.debug("task not found in Task table")
            else:
                taskStatus, taskLockedBy = resTask
                if taskLockedBy != pid:
                    # task is locked
                    tmpLog.debug(f"task is locked by {taskLockedBy}")
                elif not (
                    taskStatus in JediTaskSpec.statusToUpdateContents()
                    or (
                        taskStatus in ["running", "ready", "scouting", "assigning", "pending"]
                        and taskSpec.oldStatus not in ["defined"]
                        and (datasetState == "mutable" or datasetSpec.state == "mutable" or datasetSpec.isSeqNumber())
                    )
                ):
                    # task status is irrelevant
                    tmpLog.debug(f"task.status={taskStatus} taskSpec.oldStatus={taskSpec.oldStatus} is not for contents update")
                else:
                    tmpLog.debug(f"task.status={taskStatus} task.oldStatus={taskSpec.oldStatus}")
                    # running task
                    if taskStatus in ["running", "assigning", "ready", "scouting", "pending"]:
                        diagMap["isRunningTask"] = True
                    # size of pending input chunk to be activated
                    sizePendingEventChunk = None
                    strSizePendingEventChunk = ""
                    if (set([taskStatus, taskSpec.oldStatus]) & set(["defined", "ready", "scouting", "assigning"])) and useScout:
                        nChunks = nChunksForScout
                        # number of files for scout
                        sizePendingFileChunk = nChunksForScout
                        strSizePendingFileChunk = f"{sizePendingFileChunk}"
                        # number of files per job is specified
                        if nFilesPerJob not in [None, 0]:
                            sizePendingFileChunk *= nFilesPerJob
                            strSizePendingFileChunk = f"{nFilesPerJob}*" + strSizePendingFileChunk
                        strSizePendingFileChunk += " files required for scout"
                        # number of events for scout
                        if isEventSplit:
                            sizePendingEventChunk = nChunksForScout * nEventsPerJob
                            strSizePendingEventChunk = f"{nEventsPerJob}*{nChunksForScout} events required for scout"
                    else:
                        # the number of chunks in one bunch
                        if taskSpec.nChunksToWait() is not None:
                            nChunkInBunch = taskSpec.nChunksToWait()
                        elif taskSpec.noInputPooling():
                            nChunkInBunch = 1
                        else:
                            nChunkInBunch = 20
                        nChunks = nChunkInBunch
                        # number of files to be activated
                        sizePendingFileChunk = nChunkInBunch
                        strSizePendingFileChunk = f"{sizePendingFileChunk}"
                        # number of files per job is specified
                        if nFilesPerJob not in [None, 0]:
                            sizePendingFileChunk *= nFilesPerJob
                            strSizePendingFileChunk = f"{nFilesPerJob}*" + strSizePendingFileChunk
                        strSizePendingFileChunk += " files required"
                        # number of events to be activated
                        if isEventSplit:
                            sizePendingEventChunk = nChunkInBunch * nEventsPerJob
                            strSizePendingEventChunk = f"{nEventsPerJob}*{nChunkInBunch} events required"
                    # check dataset status
                    varMap = {}
                    varMap[":jediTaskID"] = datasetSpec.jediTaskID
                    varMap[":datasetID"] = datasetSpec.datasetID
                    self.cur.execute(sqlDs + comment, varMap)
                    resDs = self.cur.fetchone()
                    if resDs is None:
                        tmpLog.debug("dataset not found in Datasets table")
                    elif resDs[2] != datasetSpec.state:
                        tmpLog.debug(f"dataset.state changed from {datasetSpec.state} to {resDs[2]} in DB")
                    elif not (
                        resDs[0] in JediDatasetSpec.statusToUpdateContents()
                        or (
                            taskStatus in ["running", "assigning", "ready", "scouting", "pending"]
                            and (datasetState == "mutable" or datasetSpec.state == "mutable")
                            or (taskStatus in ["running", "defined", "ready", "scouting", "assigning", "pending"] and datasetSpec.isSeqNumber())
                        )
                    ):
                        tmpLog.debug(f"ds.status={resDs[0]} is not for contents update")
                        oldDsStatus = resDs[0]
                        nFilesUnprocessed = resDs[1]
                        # count existing files
                        if resDs[0] == "ready":
                            varMap = {}
                            varMap[":jediTaskID"] = datasetSpec.jediTaskID
                            varMap[":datasetID"] = datasetSpec.datasetID
                            self.cur.execute(sqlCo + comment, varMap)
                            resCo = self.cur.fetchone()
                            numUniqueLfn = resCo[0]
                            retVal = True, missingFileList, numUniqueLfn, diagMap
                    else:
                        oldDsStatus, nFilesUnprocessed, dsStateInDB, nFilesToUseDS, nFilesUsedInDS = resDs
                        tmpLog.debug(f"ds.state={dsStateInDB} in DB")
                        if not nFilesUsedInDS:
                            nFilesUsedInDS = 0
                        # get existing file list
                        varMap = {}
                        varMap[":jediTaskID"] = datasetSpec.jediTaskID
                        varMap[":datasetID"] = datasetSpec.datasetID
                        self.cur.execute(sqlCh + comment, varMap)
                        tmpRes = self.cur.fetchall()
                        tmpLog.debug(f"{len(tmpRes)} file records in DB")
                        existingFiles = {}
                        statusMap = {}
                        for (
                            fileID,
                            lfn,
                            status,
                            startEvent,
                            endEvent,
                            boundaryID,
                            nEventsInDS,
                            lumiBlockNr,
                            attemptNr,
                            maxAttempt,
                            failedAttempt,
                            maxFailure,
                        ) in tmpRes:
                            statusMap.setdefault(status, 0)
                            statusMap[status] += 1
                            uniqueFileKey = f"{lfn}.{startEvent}.{endEvent}.{boundaryID}"
                            existingFiles[uniqueFileKey] = {"fileID": fileID, "status": status}
                            if startEvent is not None and endEvent is not None:
                                existingFiles[uniqueFileKey]["nevents"] = endEvent - startEvent + 1
                            elif nEventsInDS is not None:
                                existingFiles[uniqueFileKey]["nevents"] = nEventsInDS
                            else:
                                existingFiles[uniqueFileKey]["nevents"] = None
                            existingFiles[uniqueFileKey]["is_failed"] = False
                            lostFlag = False
                            if status == "ready":
                                if (maxAttempt is not None and attemptNr is not None and attemptNr >= maxAttempt) or (
                                    failedAttempt is not None and maxFailure is not None and failedAttempt >= maxFailure
                                ):
                                    nUsed += 1
                                    existingFiles[uniqueFileKey]["is_failed"] = True
                                    nFailed += 1
                                else:
                                    nReady += 1
                            elif status == "pending":
                                nPending += 1
                                pendingFID.append(fileID)
                                # count number of events for scouts with event-level splitting
                                if isEventSplit:
                                    try:
                                        if nEventsToUseEventSplit < sizePendingEventChunk:
                                            nEventsToUseEventSplit += endEvent - startEvent + 1
                                            nFilesToUseEventSplit += 1
                                    except Exception:
                                        pass
                            elif status == "staging":
                                nStaging += 1
                                stagingLB.add(lumiBlockNr)
                            elif status not in ["lost", "missing"]:
                                nUsed += 1
                            elif status in ["lost", "missing"]:
                                nLost += 1
                                lostFlag = True
                            if existingFiles[uniqueFileKey]["nevents"] is not None:
                                if lostFlag:
                                    nEventsLost += existingFiles[uniqueFileKey]["nevents"]
                                else:
                                    nEventsExist += existingFiles[uniqueFileKey]["nevents"]
                        tmStr = "inDB nReady={} nPending={} nUsed={} nUsedInDB={} nLost={} nStaging={} nFailed={}"
                        tmpLog.debug(tmStr.format(nReady, nPending, nUsed, nFilesUsedInDS, nLost, nStaging, nFailed))
                        tmpLog.debug(f"inDB {str(statusMap)}")
                        # insert files
                        uniqueLfnList = {}
                        totalNumEventsF = 0
                        totalNumEventsE = 0
                        escapeNextFile = False
                        numUniqueLfn = 0
                        fileSpecsForInsert = []
                        for uniqueFileKey in uniqueFileKeyList:
                            fileSpec = fileSpecMap[uniqueFileKey]
                            # count number of files
                            if fileSpec.lfn not in uniqueLfnList:
                                # the limit is reached at the previous file
                                if escapeNextFile:
                                    break
                                uniqueLfnList[fileSpec.lfn] = None
                                # maximum number of files to be processed
                                if nMaxFiles is not None and len(uniqueLfnList) > nMaxFiles:
                                    break
                                # counts number of events for non event-level splitting
                                if fileSpec.nEvents is not None:
                                    totalNumEventsF += fileSpec.nEvents
                                    # maximum number of events to be processed
                                    if nMaxEvents is not None and totalNumEventsF >= nMaxEvents:
                                        escapeNextFile = True
                                # count number of unique LFNs
                                numUniqueLfn += 1
                            # count number of events for event-level splitting
                            if fileSpec.startEvent is not None and fileSpec.endEvent is not None:
                                totalNumEventsE += fileSpec.endEvent - fileSpec.startEvent + 1
                                if nMaxEvents is not None and totalNumEventsE > nMaxEvents:
                                    break
                            # avoid duplication
                            if uniqueFileKey in existingFiles:
                                continue
                            if inputPreStaging:
                                # go to staging
                                fileSpec.status = "staging"
                                nStaging += 1
                                stagingLB.add(fileSpec.lumiBlockNr)
                            elif isMutableDataset:
                                # go pending if no wait
                                fileSpec.status = "pending"
                                nPending += 1
                            nInsert += 1
                            if fileSpec.startEvent is not None and fileSpec.endEvent is not None:
                                nEventsInsert += fileSpec.endEvent - fileSpec.startEvent + 1
                            elif fileSpec.nEvents is not None:
                                nEventsInsert += fileSpec.nEvents
                            # count number of events for scouts with event-level splitting
                            if isEventSplit:
                                try:
                                    if nEventsToUseEventSplit < sizePendingEventChunk:
                                        nEventsToUseEventSplit += fileSpec.endEvent - fileSpec.startEvent + 1
                                        nFilesToUseEventSplit += 1
                                except Exception:
                                    pass
                            fileSpecsForInsert.append(fileSpec)
                        # get fileID
                        tmpLog.debug(f"get fileIDs for {nInsert} inputs")
                        newFileIDs = []
                        if nInsert > 0:
                            varMap = {}
                            varMap[":nIDs"] = nInsert
                            self.cur.execute(sqlFID, varMap)
                            resFID = self.cur.fetchall()
                            for (fileID,) in resFID:
                                newFileIDs.append(fileID)
                        if not inputPreStaging and isMutableDataset:
                            pendingFID += newFileIDs
                        # sort fileID
                        tmpLog.debug("sort fileIDs")
                        newFileIDs.sort()
                        # set fileID
                        tmpLog.debug("set fileIDs")
                        varMaps = []
                        for fileID, fileSpec in zip(newFileIDs, fileSpecsForInsert):
                            fileSpec.fileID = fileID
                            # make vars
                            varMap = fileSpec.valuesMap()
                            varMaps.append(varMap)
                        # bulk insert
                        tmpLog.debug(f"bulk insert {len(varMaps)} files")
                        self.cur.executemany(sqlIn + comment, varMaps)
                        # keep original pendingFID
                        orig_pendingFID = set(pendingFID)
                        # respect split rule
                        enough_pending_files_to_activate = False
                        total_pending_files_to_activate = 0
                        total_pending_chunks = 0
                        num_pending_files_in_first_bunch = None
                        num_available_files_in_an_input = 0
                        if datasetSpec.isMaster() and taskSpec.respectSplitRule() and (useScout or isMutableDataset or datasetSpec.state == "mutable"):
                            tmpDatasetSpecMap = {}
                            # read files
                            sqlFR = f"SELECT {JediFileSpec.columnNames()} "
                            sqlFR += f"FROM {panda_config.schemaJEDI}.JEDI_Dataset_Contents WHERE "
                            sqlFR += "jediTaskID=:jediTaskID AND datasetID=:datasetID AND status=:status "
                            sqlFR += "ORDER BY lfn, startEvent "
                            varMap = {}
                            varMap[":datasetID"] = datasetSpec.datasetID
                            varMap[":jediTaskID"] = datasetSpec.jediTaskID
                            if isMutableDataset or datasetSpec.state == "mutable":
                                varMap[":status"] = "pending"
                            else:
                                varMap[":status"] = "ready"
                            self.cur.execute(sqlFR + comment, varMap)
                            resFileList = self.cur.fetchall()
                            for resFile in resFileList:
                                # make FileSpec
                                tmpFileSpec = JediFileSpec()
                                tmpFileSpec.pack(resFile)
                                # make a list per LB
                                if taskSpec.releasePerLumiblock():
                                    tmpLumiBlockNr = tmpFileSpec.lumiBlockNr
                                else:
                                    tmpLumiBlockNr = None
                                tmpDatasetSpecMap.setdefault(tmpLumiBlockNr, {"datasetSpec": copy.deepcopy(datasetSpec), "newPandingFID": []})
                                tmpDatasetSpecMap[tmpLumiBlockNr]["newPandingFID"].append(tmpFileSpec.fileID)
                                tmpDatasetSpecMap[tmpLumiBlockNr]["datasetSpec"].addFile(tmpFileSpec)
                            if not isMutableDataset and datasetSpec.state == "mutable":
                                for tmpFileSpec in fileSpecsForInsert:
                                    # make a list per LB
                                    if taskSpec.releasePerLumiblock():
                                        tmpLumiBlockNr = tmpFileSpec.lumiBlockNr
                                    else:
                                        tmpLumiBlockNr = None
                                    tmpDatasetSpecMap.setdefault(tmpLumiBlockNr, {"datasetSpec": copy.deepcopy(datasetSpec), "newPandingFID": []})
                                    tmpDatasetSpecMap[tmpLumiBlockNr]["newPandingFID"].append(tmpFileSpec.fileID)
                                    tmpDatasetSpecMap[tmpLumiBlockNr]["datasetSpec"].addFile(tmpFileSpec)
                            # make bunches
                            if fake_mutable_for_skip_short_output:
                                # use # of files as max # of bunches for skip_short_output to activate all files in closed datasets
                                max_num_bunches = max(len(uniqueFileKeyList), 100)
                            elif taskSpec.status == "running":
                                max_num_bunches = 100
                            else:
                                max_num_bunches = 1
                            if taskSpec.useHS06():
                                walltimeGradient = taskSpec.getCpuTime()
                            else:
                                walltimeGradient = None
                            maxWalltime = taskSpec.getMaxWalltime()
                            if maxWalltime is None:
                                maxWalltime = 345600
                            corePower = 10
                            maxSizePerJob = None
                            tmpInputChunk = None
                            newPendingFID = []
                            tmpDatasetSpecMapIdxList = list(tmpDatasetSpecMap.keys())
                            for i_bunch in range(max_num_bunches):
                                # making a single bunch with multiple chunks
                                i_chunks_in_a_bunch = 0
                                files_available_for_a_chunk = False
                                while i_chunks_in_a_bunch < nChunks:
                                    # make a new input with another lumiblock
                                    if tmpInputChunk is None:
                                        if not tmpDatasetSpecMapIdxList:
                                            break
                                        tmpLumiBlockNr = tmpDatasetSpecMapIdxList.pop()
                                        tmpInputChunk = InputChunk(taskSpec)
                                        tmpInputChunk.addMasterDS(tmpDatasetSpecMap[tmpLumiBlockNr]["datasetSpec"])
                                        maxSizePerJob = taskSpec.getMaxSizePerJob()
                                        if maxSizePerJob is not None:
                                            maxSizePerJob += InputChunk.defaultOutputSize
                                            maxSizePerJob += taskSpec.getWorkDiskSize()
                                        else:
                                            if useScout:
                                                maxSizePerJob = InputChunk.maxInputSizeScouts * 1024 * 1024
                                            else:
                                                maxSizePerJob = InputChunk.maxInputSizeAvalanche * 1024 * 1024
                                        i_chunks_with_a_lumiblock = 0
                                    # get a chunk
                                    tmp_sub_chunk, is_short = tmpInputChunk.getSubChunk(
                                        None,
                                        maxNumFiles=taskSpec.getMaxNumFilesPerJob(),
                                        nFilesPerJob=taskSpec.getNumFilesPerJob(),
                                        walltimeGradient=walltimeGradient,
                                        maxWalltime=maxWalltime,
                                        sizeGradients=taskSpec.getOutDiskSize(),
                                        sizeIntercepts=taskSpec.getWorkDiskSize(),
                                        maxSize=maxSizePerJob,
                                        nEventsPerJob=taskSpec.getNumEventsPerJob(),
                                        coreCount=taskSpec.coreCount,
                                        corePower=corePower,
                                        respectLB=taskSpec.respectLumiblock(),
                                        skip_short_output=skip_short_output,
                                    )
                                    files_available_for_a_chunk = tmpInputChunk.checkUnused()
                                    if not files_available_for_a_chunk:
                                        if (
                                            (not isMutableDataset)
                                            or (taskSpec.releasePerLumiblock() and tmpLumiBlockNr not in stagingLB)
                                            or (skip_short_output and tmp_sub_chunk)
                                            or (tmp_sub_chunk and not is_short)
                                        ):
                                            i_chunks_with_a_lumiblock += 1
                                            i_chunks_in_a_bunch += 1
                                            total_pending_chunks += 1
                                            num_available_files_in_an_input = tmpInputChunk.getMasterUsedIndex()
                                        if i_chunks_with_a_lumiblock > 0:
                                            total_pending_files_to_activate += num_available_files_in_an_input
                                            newPendingFID += tmpDatasetSpecMap[tmpLumiBlockNr]["newPandingFID"][:num_available_files_in_an_input]
                                        tmpInputChunk = None
                                    else:
                                        i_chunks_with_a_lumiblock += 1
                                        i_chunks_in_a_bunch += 1
                                        total_pending_chunks += 1
                                        num_available_files_in_an_input = tmpInputChunk.getMasterUsedIndex()
                                # end of a single bunch creation
                                if num_pending_files_in_first_bunch is None:
                                    num_pending_files_in_first_bunch = num_available_files_in_an_input
                                if files_available_for_a_chunk:
                                    enough_pending_files_to_activate = True
                                else:
                                    # one bunch is at least available
                                    if i_bunch > 0 or i_chunks_in_a_bunch >= nChunks:
                                        enough_pending_files_to_activate = True
                                    # terminate lookup for skip short output
                                    if skip_short_output and datasetState == "closed":
                                        enough_pending_files_to_activate = True
                                    break
                            if tmpInputChunk:
                                total_pending_files_to_activate += tmpInputChunk.getMasterUsedIndex()
                                newPendingFID += tmpDatasetSpecMap[tmpLumiBlockNr]["newPandingFID"][: tmpInputChunk.getMasterUsedIndex()]
                            pendingFID = newPendingFID
                            tmpLog.debug(
                                f"respecting SR nFilesToActivate={total_pending_files_to_activate} nChunksToActivate={total_pending_chunks} minChunks={nChunks} "
                                f"isEnough={enough_pending_files_to_activate} nFilesPerJob={taskSpec.getNumFilesPerJob()} "
                                f"maxSizePerJob={int(maxSizePerJob/1024/1024) if maxSizePerJob else None} "
                            )
                        if num_pending_files_in_first_bunch is None:
                            num_pending_files_in_first_bunch = 0
                        # activate pending
                        tmpLog.debug("activate pending")
                        toActivateFID = []
                        if isMutableDataset:
                            if not datasetSpec.isMaster():
                                # activate all files except master dataset
                                toActivateFID = pendingFID
                            elif inputPreStaging and nStaging == 0:
                                # all files are staged
                                toActivateFID = pendingFID
                            else:
                                if datasetSpec.isMaster() and taskSpec.respectSplitRule() and (useScout or isMutableDataset):
                                    # enough pending
                                    if enough_pending_files_to_activate:
                                        toActivateFID = pendingFID[:total_pending_files_to_activate]
                                    else:
                                        diagMap["errMsg"] = "not enough files"
                                elif isEventSplit:
                                    # enough events are pending
                                    if nEventsToUseEventSplit >= sizePendingEventChunk and nFilesToUseEventSplit > 0:
                                        toActivateFID = pendingFID[: (int(nPending / nFilesToUseEventSplit) * nFilesToUseEventSplit)]
                                    else:
                                        diagMap["errMsg"] = f"{nEventsToUseEventSplit} events ({nPending} files) available, {strSizePendingEventChunk}"
                                else:
                                    # enough files are pending
                                    if nPending >= sizePendingFileChunk and sizePendingFileChunk > 0:
                                        toActivateFID = pendingFID[: (int(nPending / sizePendingFileChunk) * sizePendingFileChunk)]
                                    else:
                                        diagMap["errMsg"] = f"{nPending} files available, {strSizePendingFileChunk}"
                        else:
                            nReady += nInsert
                            toActivateFID = orig_pendingFID
                        tmpLog.debug(f"length of pendingFID {len(orig_pendingFID)} -> {len(toActivateFID)}")
                        for tmpFileID in toActivateFID:
                            if tmpFileID in orig_pendingFID:
                                varMap = {}
                                varMap[":status"] = "ready"
                                varMap[":jediTaskID"] = datasetSpec.jediTaskID
                                varMap[":datasetID"] = datasetSpec.datasetID
                                varMap[":fileID"] = tmpFileID
                                self.cur.execute(sqlFU + comment, varMap)
                                nActivatedPending += 1
                                nReady += 1
                        tmpLog.debug(f"nReady={nReady} nPending={nPending} nActivatedPending={nActivatedPending} after activation")
                        # lost or recovered files
                        if datasetSpec.isSeqNumber():
                            tmpLog.debug("skip lost or recovered file check for SEQ")
                        else:
                            tmpLog.debug("lost or recovered files")
                            uniqueFileKeySet = set(uniqueFileKeyList)
                            for uniqueFileKey, fileVarMap in existingFiles.items():
                                varMap = {}
                                varMap[":jediTaskID"] = datasetSpec.jediTaskID
                                varMap[":datasetID"] = datasetSpec.datasetID
                                varMap[":fileID"] = fileVarMap["fileID"]
                                lostInPending = False
                                if uniqueFileKey not in uniqueFileKeySet:
                                    if fileVarMap["status"] == "lost":
                                        continue
                                    if fileVarMap["status"] not in ["ready", "pending", "staging"]:
                                        continue
                                    elif fileVarMap["status"] != "ready":
                                        lostInPending = True
                                    varMap["status"] = "lost"
                                    tmpLog.debug(f"{uniqueFileKey} was lost")
                                else:
                                    continue
                                if varMap["status"] == "ready":
                                    nLost -= 1
                                    nReady += 1
                                    if fileVarMap["nevents"] is not None:
                                        nEventsExist += fileVarMap["nevents"]
                                if varMap["status"] in ["lost", "missing"]:
                                    nLost += 1
                                    if not lostInPending:
                                        nReady -= 1
                                    if fileVarMap["nevents"] is not None:
                                        nEventsExist -= fileVarMap["nevents"]
                                    if fileVarMap["is_failed"]:
                                        nUsed -= 1
                                self.cur.execute(sqlFU + comment, varMap)
                            tmpLog.debug(
                                "nReady={} nLost={} nUsed={} nUsedInDB={} nUsedConsistent={} after lost/recovery check".format(
                                    nReady, nLost, nUsed, nFilesUsedInDS, nUsed == nFilesUsedInDS
                                )
                            )
                        # get master status
                        masterStatus = None
                        if not datasetSpec.isMaster():
                            varMap = {}
                            varMap[":jediTaskID"] = datasetSpec.jediTaskID
                            varMap[":datasetID"] = datasetSpec.masterID
                            self.cur.execute(sqlMS + comment, varMap)
                            resMS = self.cur.fetchone()
                            (masterStatus,) = resMS
                        tmpLog.debug(f"masterStatus={masterStatus}")
                        tmpLog.debug(f"nFilesToUseDS={nFilesToUseDS}")
                        if nFilesToUseDS is None:
                            nFilesToUseDS = 0
                        # updata dataset
                        varMap = {}
                        varMap[":jediTaskID"] = datasetSpec.jediTaskID
                        varMap[":datasetID"] = datasetSpec.datasetID
                        varMap[":nFiles"] = nInsert + len(existingFiles) - nLost
                        if skip_short_output:
                            # remove pending files to avoid wrong task transition due to nFiles>nFilesTobeUsed
                            varMap[":nFiles"] -= nPending - nActivatedPending
                        varMap[":nEvents"] = nEventsInsert + nEventsExist
                        varMap[":nFilesMissing"] = nLost
                        if datasetSpec.isMaster() and taskSpec.respectSplitRule() and useScout:
                            if set([taskStatus, taskSpec.oldStatus]) & set(["scouting", "ready", "assigning"]):
                                varMap[":nFilesTobeUsed"] = nFilesToUseDS
                            else:
                                if fake_mutable_for_skip_short_output:
                                    # use num_files_with_sl in the first bunch since numFilesWithSL is too big for scouts
                                    varMap[":nFilesTobeUsed"] = num_pending_files_in_first_bunch + nUsed
                                elif isMutableDataset:
                                    varMap[":nFilesTobeUsed"] = nReady + nUsed
                                else:
                                    varMap[":nFilesTobeUsed"] = total_pending_files_to_activate + nUsed
                        elif datasetSpec.isMaster() and useScout and (set([taskStatus, taskSpec.oldStatus]) & set(["scouting", "ready", "assigning"])):
                            varMap[":nFilesTobeUsed"] = nFilesToUseDS
                        elif xmlConfig is not None:
                            # disable scout for --loadXML
                            varMap[":nFilesTobeUsed"] = nReady + nUsed
                        elif (
                            (set([taskStatus, taskSpec.oldStatus]) & set(["defined", "ready", "scouting", "assigning"]))
                            and useScout
                            and not isEventSplit
                            and nChunksForScout is not None
                            and nReady > sizePendingFileChunk
                        ):
                            # set a fewer number for scout for file level splitting
                            varMap[":nFilesTobeUsed"] = sizePendingFileChunk
                        elif (
                            [1 for tmpStat in [taskStatus, taskSpec.oldStatus] if tmpStat in ["defined", "ready", "scouting", "assigning"]]
                            and useScout
                            and isEventSplit
                            and nReady > max(nFilesToUseEventSplit, nFilesToUseDS)
                        ):
                            # set a fewer number for scout for event level splitting
                            varMap[":nFilesTobeUsed"] = max(nFilesToUseEventSplit, nFilesToUseDS)
                        else:
                            varMap[":nFilesTobeUsed"] = nReady + nUsed
                        if useScout:
                            if not isEventSplit:
                                # file level splitting
                                if nFilesPerJob in [None, 0]:
                                    # number of files per job is not specified
                                    diagMap["nChunksForScout"] = nChunksForScout - varMap[":nFilesTobeUsed"]
                                else:
                                    tmpQ, tmpR = divmod(varMap[":nFilesTobeUsed"], nFilesPerJob)
                                    diagMap["nChunksForScout"] = nChunksForScout - tmpQ
                                    if tmpR > 0:
                                        diagMap["nChunksForScout"] -= 1
                            else:
                                # event level splitting
                                if varMap[":nFilesTobeUsed"] > 0:
                                    tmpQ, tmpR = divmod(nEventsToUseEventSplit, nEventsPerJob)
                                    diagMap["nChunksForScout"] = nChunksForScout - tmpQ
                                    if tmpR > 0:
                                        diagMap["nChunksForScout"] -= 1
                        if missingFileList != [] or (isMutableDataset and nActivatedPending == 0 and nFilesUnprocessed in [0, None]):
                            if datasetSpec.isMaster() or masterStatus is None:
                                # don't change status when some files are missing or no pending inputs are activated
                                tmpLog.debug(f"using datasetSpec.status={datasetSpec.status}")
                                varMap[":status"] = datasetSpec.status
                            else:
                                # use master status
                                tmpLog.debug(f"using masterStatus={masterStatus}")
                                varMap[":status"] = masterStatus
                        else:
                            varMap[":status"] = "ready"
                        # no more inputs are required even if parent is still running
                        numReqFileRecords = nMaxFiles
                        try:
                            if nEventsPerFile > nEventsPerJob:
                                numReqFileRecords = numReqFileRecords * nEventsPerFile // nEventsPerJob
                        except Exception:
                            pass
                        tmpLog.debug(f"the number of requested file records : {numReqFileRecords}")
                        if isMutableDataset and numReqFileRecords is not None and varMap[":nFilesTobeUsed"] >= numReqFileRecords:
                            varMap[":state"] = "open"
                        elif inputPreStaging and nStaging == 0 and datasetSpec.isMaster() and nPending == nActivatedPending:
                            varMap[":state"] = "closed"
                        else:
                            varMap[":state"] = datasetState
                        varMap[":stateUpdateTime"] = stateUpdateTime
                        newDsStatus = varMap[":status"]
                        if nUsed != nFilesUsedInDS:
                            varMap[":nFilesUsed"] = nUsed
                            tmpLog.debug(sqlDUx + comment + str(varMap))
                            self.cur.execute(sqlDUx + comment, varMap)
                        else:
                            tmpLog.debug(sqlDU + comment + str(varMap))
                            self.cur.execute(sqlDU + comment, varMap)
                        # propagate number of input events to DEFT
                        if datasetSpec.isMaster():
                            varMap = {}
                            varMap[":jediTaskID"] = datasetSpec.jediTaskID
                            varMap.update(INPUT_TYPES_var_map)
                            tmpLog.debug(sqlCE + comment + str(varMap))
                            self.cur.execute(sqlCE + comment, varMap)
                        # return number of activated pending inputs
                        diagMap["nActivatedPending"] = nActivatedPending
                        if nFilesUnprocessed not in [0, None]:
                            diagMap["nActivatedPending"] += nFilesUnprocessed
                        # set return value
                        retVal = True, missingFileList, numUniqueLfn, diagMap
            # fix secondary files in staging
            if inputPreStaging and datasetSpec.isSeqNumber():
                get_task_utils_module(self).fix_associated_files_in_staging(datasetSpec.jediTaskID, secondary_id=datasetSpec.datasetID)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmpLog.debug(
                ("inserted rows={0} with activated={1}, pending={2}, ready={3}, " "unprocessed={4}, staging={5} status={6}->{7}").format(
                    nInsert, nActivatedPending, nPending - nActivatedPending, nReady, nStaging, nFilesUnprocessed, oldDsStatus, newDsStatus
                )
            )
            regTime = naive_utcnow() - regStart
            tmpLog.debug("took %s.%03d sec" % (regTime.seconds, regTime.microseconds / 1000))
            return retVal
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            regTime = naive_utcnow() - regStart
            tmpLog.debug("took %s.%03d sec" % (regTime.seconds, regTime.microseconds / 1000))
            return harmlessRet

    # update JEDI task status by ContentsFeeder
    def updateTaskStatusByContFeeder_JEDI(self, jediTaskID, taskSpec=None, getTaskStatus=False, pid=None, setFrozenTime=True, useWorldCloud=False):
        comment = " /* JediDBProxy.updateTaskStatusByContFeeder_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID}")
        tmpLog.debug("start")
        try:
            # sql to check status
            sqlS = f"SELECT status,lockedBy,cloud,prodSourceLabel,frozenTime,nucleus FROM {panda_config.schemaJEDI}.JEDI_Tasks "
            sqlS += "WHERE jediTaskID=:jediTaskID FOR UPDATE "
            # sql to get number of unassigned datasets
            sqlD = f"SELECT COUNT(*) FROM {panda_config.schemaJEDI}.JEDI_Datasets "
            sqlD += "WHERE jediTaskID=:jediTaskID AND destination IS NULL AND type IN (:type1,:type2) "
            # sql to update task
            sqlU = f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks "
            sqlU += "SET status=:status,modificationTime=:updateTime,stateChangeTime=CURRENT_DATE,"
            sqlU += "lockedBy=NULL,lockedTime=NULL,frozenTime=:frozenTime"
            if taskSpec is not None:
                sqlU += ",oldStatus=:oldStatus,errorDialog=:errorDialog,splitRule=:splitRule"
            sqlU += " WHERE jediTaskID=:jediTaskID "
            # sql to unlock task
            sqlL = f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks "
            sqlL += "SET lockedBy=NULL,lockedTime=NULL "
            sqlL += "WHERE jediTaskID=:jediTaskID AND status=:status "
            if pid is not None:
                sqlL += "AND lockedBy=:pid "
            # begin transaction
            self.conn.begin()
            # check status
            taskStatus = None
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            tmpLog.debug(sqlS + comment + str(varMap))
            self.cur.execute(sqlS + comment, varMap)
            res = self.cur.fetchone()
            if res is None:
                tmpLog.debug("task is not found in Tasks table")
            else:
                taskStatus, lockedBy, cloudName, prodSourceLabel, frozenTime, nucleus = res
                if lockedBy != pid:
                    # task is locked
                    tmpLog.debug(f"task is locked by {lockedBy}")
                elif (taskSpec is None or taskSpec.status != "tobroken") and taskStatus not in JediTaskSpec.statusToUpdateContents():
                    # task status is irrelevant
                    tmpLog.debug(f"task.status={taskStatus} is not for contents update")
                    # unlock
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    varMap[":status"] = taskStatus
                    if pid is not None:
                        varMap[":pid"] = pid
                    self.cur.execute(sqlL + comment, varMap)
                    tmpLog.debug("unlocked")
                else:
                    # get number of unassigned datasets
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    varMap[":type1"] = "output"
                    varMap[":type2"] = "log"
                    self.cur.execute(sqlD + comment, varMap)
                    (nUnassignedDSs,) = self.cur.fetchone()
                    # update task
                    timeNow = naive_utcnow()
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    varMap[":updateTime"] = timeNow
                    if taskSpec is not None:
                        # new task status is specified
                        varMap[":status"] = taskSpec.status
                        varMap[":oldStatus"] = taskSpec.oldStatus
                        varMap[":errorDialog"] = taskSpec.errorDialog
                        varMap[":splitRule"] = taskSpec.splitRule
                        # set/unset frozen time
                        if taskSpec.status == "pending" and setFrozenTime:
                            if frozenTime is None:
                                varMap[":frozenTime"] = timeNow
                            else:
                                varMap[":frozenTime"] = frozenTime
                        else:
                            varMap[":frozenTime"] = None
                    elif (cloudName is None or (useWorldCloud and (nUnassignedDSs > 0 or nucleus in ["", None]))) and prodSourceLabel in ["managed", "test"]:
                        # set assigning for TaskBrokerage
                        varMap[":status"] = "assigning"
                        varMap[":frozenTime"] = timeNow
                        # set old update time to trigger TaskBrokerage immediately
                        varMap[":updateTime"] = naive_utcnow() - datetime.timedelta(hours=6)
                    else:
                        # skip task brokerage since cloud is preassigned
                        varMap[":status"] = "ready"
                        varMap[":frozenTime"] = None
                        # set old update time to trigger JG immediately
                        varMap[":updateTime"] = naive_utcnow() - datetime.timedelta(hours=6)
                    tmpLog.debug(sqlU + comment + str(varMap))
                    self.cur.execute(sqlU + comment, varMap)
                    # update DEFT task status
                    taskStatus = varMap[":status"]
                    if taskStatus in ["broken", "assigning"]:
                        self.setDeftStatus_JEDI(jediTaskID, taskStatus)
                        self.setSuperStatus_JEDI(jediTaskID, taskStatus)
                    # task status logging
                    self.record_task_status_change(jediTaskID)
                    self.push_task_status_message(taskSpec, jediTaskID, taskStatus)
                    tmpLog.debug(f"set to {taskStatus}")
                    # update queued and activated times
                    get_metrics_module(self).update_task_queued_activated_times(jediTaskID)
                    get_metrics_module(self).unset_task_activated_time(jediTaskID, taskStatus)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            if not getTaskStatus:
                return True
            else:
                return True, taskStatus
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            if not getTaskStatus:
                return False
            else:
                return False, None

    # update JEDI task
    def updateTask_JEDI(self, taskSpec, criteria, oldStatus=None, updateDEFT=True, insertUnknown=None, setFrozenTime=True, setOldModTime=False):
        comment = " /* JediDBProxy.updateTask_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"jediTaskID={taskSpec.jediTaskID}")
        tmpLog.debug("start")
        # return value for failure
        failedRet = False, 0
        # no criteria
        if criteria == {}:
            tmpLog.error("no selection criteria")
            return failedRet
        # check criteria
        for tmpKey in criteria.keys():
            if not hasattr(taskSpec, tmpKey):
                tmpLog.error(f"unknown attribute {tmpKey} is used in criteria")
                return failedRet
        try:
            # set attributes
            timeNow = naive_utcnow()
            taskSpec.resetChangedAttr("jediTaskID")
            if setOldModTime:
                taskSpec.modificationTime = timeNow - datetime.timedelta(hours=1)
            else:
                taskSpec.modificationTime = timeNow
            # sql to get old status
            sqlS = f"SELECT status,frozenTime FROM {panda_config.schemaJEDI}.JEDI_Tasks "
            sql = "WHERE "
            varMap = {}
            for tmpKey, tmpVal in criteria.items():
                crKey = f":cr_{tmpKey}"
                sql += f"{tmpKey}={crKey} AND "
                varMap[crKey] = tmpVal
            if oldStatus is not None:
                old_status_var_names_str, old_status_var_map = get_sql_IN_bind_variables(oldStatus, prefix=":old_", value_as_suffix=True)
                sql += f"status IN ({old_status_var_names_str}) AND "
                varMap.update(old_status_var_map)
            sql = sql[:-4]
            # begin transaction
            self.conn.begin()
            # get old status
            frozenTime = None
            statusUpdated = False
            self.cur.execute(sqlS + sql + comment, varMap)
            res = self.cur.fetchone()
            if res is not None:
                statusInDB, frozenTime = res
                if statusInDB != taskSpec.status:
                    taskSpec.stateChangeTime = timeNow
                    statusUpdated = True
            # set/unset frozen time
            if taskSpec.status == "pending" and setFrozenTime:
                if frozenTime is None:
                    taskSpec.frozenTime = timeNow
            elif taskSpec.status == "assigning":
                # keep original frozen time for assigning tasks
                pass
            else:
                if frozenTime is not None:
                    taskSpec.frozenTime = None
            # update task
            sqlU = f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks SET {taskSpec.bindUpdateChangesExpression()} "
            for tmpKey, tmpVal in taskSpec.valuesMap(useSeq=False, onlyChanged=True).items():
                varMap[tmpKey] = tmpVal
            tmpLog.debug(sqlU + sql + comment + str(varMap))
            self.cur.execute(sqlU + sql + comment, varMap)
            # the number of updated rows
            nRows = self.cur.rowcount
            # insert unknown datasets
            if nRows > 0 and insertUnknown is not None:
                # sql to check
                sqlUC = f"SELECT datasetID FROM {panda_config.schemaJEDI}.JEDI_Datasets "
                sqlUC += "WHERE jediTaskID=:jediTaskID AND type=:type AND datasetName=:datasetName "
                # sql to insert dataset
                sqlUI = f"INSERT INTO {panda_config.schemaJEDI}.JEDI_Datasets ({JediDatasetSpec.columnNames()}) "
                sqlUI += JediDatasetSpec.bindValuesExpression()
                # loop over all datasets
                for tmpUnknownDataset in insertUnknown:
                    # check if already in DB
                    varMap = {}
                    varMap[":type"] = JediDatasetSpec.getUnknownInputType()
                    varMap[":jediTaskID"] = taskSpec.jediTaskID
                    varMap[":datasetName"] = tmpUnknownDataset
                    self.cur.execute(sqlUC + comment, varMap)
                    resUC = self.cur.fetchone()
                    if resUC is None:
                        # insert dataset
                        datasetSpec = JediDatasetSpec()
                        datasetSpec.jediTaskID = taskSpec.jediTaskID
                        datasetSpec.datasetName = tmpUnknownDataset
                        datasetSpec.creationTime = naive_utcnow()
                        datasetSpec.modificationTime = datasetSpec.creationTime
                        datasetSpec.type = JediDatasetSpec.getUnknownInputType()
                        varMap = datasetSpec.valuesMap(useSeq=True)
                        self.cur.execute(sqlUI + comment, varMap)
            # update DEFT
            if nRows > 0:
                if updateDEFT:
                    # count number of finished jobs
                    sqlC = "SELECT count(distinct pandaID) "
                    sqlC += "FROM {0}.JEDI_Datasets tabD,{0}.JEDI_Dataset_Contents tabC ".format(panda_config.schemaJEDI)
                    sqlC += "WHERE tabD.jediTaskID=tabC.jediTaskID AND tabD.jediTaskID=:jediTaskID "
                    sqlC += "AND tabC.datasetID=tabD.datasetID "
                    sqlC += "AND tabC.status=:status "
                    sqlC += "AND masterID IS NULL AND pandaID IS NOT NULL "
                    varMap = {}
                    varMap[":jediTaskID"] = taskSpec.jediTaskID
                    varMap[":status"] = "finished"
                    self.cur.execute(sqlC + comment, varMap)
                    res = self.cur.fetchone()
                    if res is None:
                        tmpLog.debug("failed to count # of finished jobs when updating DEFT table")
                    else:
                        (nDone,) = res
                        sqlD = f"UPDATE {panda_config.schemaDEFT}.T_TASK "
                        sqlD += "SET status=:status,total_done_jobs=:nDone,timeStamp=CURRENT_DATE "
                        sqlD += "WHERE taskID=:jediTaskID "
                        varMap = {}
                        varMap[":status"] = taskSpec.status
                        varMap[":jediTaskID"] = taskSpec.jediTaskID
                        varMap[":nDone"] = nDone
                        tmpLog.debug(sqlD + comment + str(varMap))
                        self.cur.execute(sqlD + comment, varMap)
                        self.setSuperStatus_JEDI(taskSpec.jediTaskID, taskSpec.status)
                elif taskSpec.status in ["running", "broken", "assigning", "scouting", "aborted", "aborting", "exhausted", "staging"]:
                    # update DEFT task status
                    if taskSpec.status == "scouting":
                        deftStatus = "submitting"
                    else:
                        deftStatus = taskSpec.status
                    sqlD = f"UPDATE {panda_config.schemaDEFT}.T_TASK "
                    sqlD += "SET status=:status,timeStamp=CURRENT_DATE"
                    if taskSpec.status == "scouting":
                        sqlD += ",start_time=CURRENT_DATE"
                    sqlD += " WHERE taskID=:jediTaskID "
                    varMap = {}
                    varMap[":status"] = deftStatus
                    varMap[":jediTaskID"] = taskSpec.jediTaskID
                    tmpLog.debug(sqlD + comment + str(varMap))
                    self.cur.execute(sqlD + comment, varMap)
                    self.setSuperStatus_JEDI(taskSpec.jediTaskID, deftStatus)
                    if taskSpec.status == "running":
                        varMap = {}
                        varMap[":jediTaskID"] = taskSpec.jediTaskID
                        sqlDS = f"UPDATE {panda_config.schemaDEFT}.T_TASK "
                        sqlDS += "SET start_time=timeStamp "
                        sqlDS += "WHERE taskID=:jediTaskID AND start_time IS NULL "
                        tmpLog.debug(sqlDS + comment + str(varMap))
                        self.cur.execute(sqlDS + comment, varMap)
                # status change logging
                if statusUpdated:
                    self.record_task_status_change(taskSpec.jediTaskID)
                    self.push_task_status_message(taskSpec, taskSpec.jediTaskID, taskSpec.status)
                    # task attempt end log
                    if taskSpec.status in ["done", "finished", "failed", "broken", "aborted", "exhausted"]:
                        get_task_utils_module(self).log_task_attempt_end(taskSpec.jediTaskID)
                # update queued and activated time
                get_metrics_module(self).update_task_queued_activated_times(taskSpec.jediTaskID)
                get_metrics_module(self).unset_task_activated_time(taskSpec.jediTaskID, taskSpec.status)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmpLog.debug(f"updated {nRows} rows")
            return True, nRows
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return failedRet

    # get JEDI tasks to be finished
    def getTasksToBeFinished_JEDI(self, vo, prodSourceLabel, pid, nTasks=50, target_tasks=None):
        comment = " /* JediDBProxy.getTasksToBeFinished_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"vo={vo} label={prodSourceLabel} pid={pid}")
        tmpLog.debug("start")
        # return value for failure
        failedRet = None
        try:
            # sql
            varMap = {}
            varMap[":status1"] = "prepared"
            varMap[":status2"] = "scouted"
            varMap[":status3"] = "tobroken"
            varMap[":status4"] = "toabort"
            varMap[":status5"] = "passed"
            sqlRT = "SELECT tabT.jediTaskID,tabT.status,tabT.eventService,tabT.site,tabT.useJumbo,tabT.splitRule "
            sqlRT += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_AUX_Status_MinTaskID tabA ".format(panda_config.schemaJEDI)
            sqlRT += "WHERE tabT.status=tabA.status "
            or_taskids_sql = ""
            if target_tasks:
                taskids_var_name_key_str, taskids_var_map = get_sql_IN_bind_variables(target_tasks, prefix=":jediTaskID")
                or_taskids_sql = f"OR tabT.jediTaskID IN ({taskids_var_name_key_str}) "
                varMap.update(taskids_var_map)
            sqlRT += f"AND (tabT.jediTaskID>=tabA.min_jediTaskID {or_taskids_sql}) "
            sqlRT += "AND tabT.status IN (:status1,:status2,:status3,:status4,:status5) "
            if vo not in [None, "any"]:
                varMap[":vo"] = vo
                sqlRT += "AND tabT.vo=:vo "
            if prodSourceLabel not in [None, "any"]:
                varMap[":prodSourceLabel"] = prodSourceLabel
                sqlRT += "AND tabT.prodSourceLabel=:prodSourceLabel "
            sqlRT += "AND (lockedBy IS NULL OR lockedTime<:timeLimit) "
            sqlRT += f"AND rownum<{nTasks} "
            sqlNW = f"SELECT jediTaskID FROM {panda_config.schemaJEDI}.JEDI_Tasks "
            sqlNW += "WHERE jediTaskID=:jediTaskID FOR UPDATE NOWAIT"
            sqlLK = f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks SET lockedBy=:lockedBy,lockedTime=CURRENT_DATE "
            sqlLK += "WHERE jediTaskID=:jediTaskID AND (lockedBy IS NULL OR lockedTime<:timeLimit) AND status=:status "
            sqlTS = f"SELECT {JediTaskSpec.columnNames()} "
            sqlTS += f"FROM {panda_config.schemaJEDI}.JEDI_Tasks "
            sqlTS += "WHERE jediTaskID=:jediTaskID "
            sqlDS = f"SELECT {JediDatasetSpec.columnNames()} "
            sqlDS += f"FROM {panda_config.schemaJEDI}.JEDI_Datasets WHERE jediTaskID=:jediTaskID "
            sqlSC = f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks SET status=:newStatus,modificationTime=:updateTime,stateChangeTime=CURRENT_DATE "
            sqlSC += "WHERE jediTaskID=:jediTaskID AND status=:oldStatus "
            # begin transaction
            self.conn.begin()
            self.cur.arraysize = 10000
            # get tasks
            timeLimit = naive_utcnow() - datetime.timedelta(minutes=10)
            varMap[":timeLimit"] = timeLimit
            tmpLog.debug(sqlRT + comment + str(varMap))
            self.cur.execute(sqlRT + comment, varMap)
            resList = self.cur.fetchall()
            retTasks = []
            allTasks = []
            taskStatList = []
            for jediTaskID, taskStatus, eventService, site, useJumbo, splitRule in resList:
                taskStatList.append((jediTaskID, taskStatus, eventService, site, useJumbo, splitRule))
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # get tasks and datasets
            for jediTaskID, taskStatus, eventService, site, useJumbo, splitRule in taskStatList:
                # begin transaction
                self.conn.begin()
                # check task
                try:
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    self.cur.execute(sqlNW + comment, varMap)
                except Exception:
                    tmpLog.debug(f"skip locked jediTaskID={jediTaskID}")
                    # commit
                    if not self._commit():
                        raise RuntimeError("Commit error")
                    continue
                # special action for scouted
                if taskStatus == "scouted":
                    # make avalanche
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    varMap[":newStatus"] = "running"
                    varMap[":oldStatus"] = taskStatus
                    # set old update time to trigger JG immediately
                    varMap[":updateTime"] = naive_utcnow() - datetime.timedelta(hours=6)
                    self.cur.execute(sqlSC + comment, varMap)
                    nRows = self.cur.rowcount
                    tmpLog.debug(f"changed status to {varMap[':newStatus']} for jediTaskID={jediTaskID} with {nRows}")
                    if nRows > 0:
                        self.setSuperStatus_JEDI(jediTaskID, "running")
                        self.record_task_status_change(jediTaskID)
                        self.push_task_status_message(None, jediTaskID, varMap[":newStatus"], splitRule)
                        # enable jumbo
                        get_task_utils_module(self).enableJumboInTask_JEDI(jediTaskID, eventService, site, useJumbo, splitRule)
                else:
                    # lock task
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    varMap[":lockedBy"] = pid
                    varMap[":status"] = taskStatus
                    varMap[":timeLimit"] = timeLimit
                    self.cur.execute(sqlLK + comment, varMap)
                    nRows = self.cur.rowcount
                    if nRows == 1:
                        # read task
                        varMap = {}
                        varMap[":jediTaskID"] = jediTaskID
                        self.cur.execute(sqlTS + comment, varMap)
                        resTS = self.cur.fetchone()
                        if resTS is not None:
                            taskSpec = JediTaskSpec()
                            taskSpec.pack(resTS)
                            retTasks.append(taskSpec)
                            # read datasets
                            varMap = {}
                            varMap[":jediTaskID"] = taskSpec.jediTaskID
                            self.cur.execute(sqlDS + comment, varMap)
                            resList = self.cur.fetchall()
                            for resDS in resList:
                                datasetSpec = JediDatasetSpec()
                                datasetSpec.pack(resDS)
                                taskSpec.datasetSpecList.append(datasetSpec)
                # commit
                if not self._commit():
                    raise RuntimeError("Commit error")
            tmpLog.debug(f"got {len(retTasks)} tasks")
            return retTasks
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return failedRet

    # get tasks and datasets with unprocessed inputs
    def _get_tasks_datasets_with_unprocessed_inputs(
        self,
        comment: str,
        tmp_log: LogWrapper,
        vo: str,
        work_queue: WorkQueue,
        prod_source_label: str,
        cloud_name: str,
        attr_name_for_group_by: str | None,
        time_limit: datetime.datetime,
        min_priority: int | None,
        simulation_with_file_stat: bool,
        target_datasets: list | None,
        merge_un_throttled: bool | None,
        resource_name: str,
        target_tasks: list | None,
    ) -> list:
        """
        Get tasks and datasets with unprocessed inputs.

        :param comment: Comment for SQL queries.
        :param tmp_log: Logger object.
        :param vo: The VO name.
        :param work_queue: Work queue object.
        :param prod_source_label: Production source label.
        :param cloud_name: Cloud name.
        :param attr_name_for_group_by: Attribute name for aggregation.
        :param time_limit: Time window for task selection.
        :param min_priority: Minimum task priority.
        :param simulation_with_file_stat: Whether to read files by ignoring file counts of the dataset for dry run.
        :param target_datasets: Targeted dataset IDs.
        :param merge_un_throttled: Whether to read tasks with unprocessed unmerged inputs even if enough tasks have been already read.
        :param resource_name: Resource name.
        :param target_tasks: Target task IDs in actual run.
        :return: A list of task and dataset attributes.
        """
        # get tasks/datasets
        if not target_tasks:
            var_map = {}
            var_map[":vo"] = vo
            if prod_source_label not in [None, "", "any"]:
                var_map[":prodSourceLabel"] = prod_source_label
            if cloud_name not in [None, "", "any"]:
                var_map[":cloud"] = cloud_name
            var_map[":dsStatus1"] = "ready"
            var_map[":dsStatus2"] = "done"
            var_map[":dsOKStatus1"] = "ready"
            var_map[":dsOKStatus2"] = "done"
            var_map[":dsOKStatus3"] = "defined"
            var_map[":dsOKStatus4"] = "registered"
            var_map[":dsOKStatus5"] = "failed"
            var_map[":dsOKStatus6"] = "finished"
            var_map[":dsOKStatus7"] = "removed"
            var_map[":dsStatusRemoved"] = "removed"
            var_map[":timeLimit"] = time_limit
            var_map[":useJumboLack"] = JediTaskSpec.enum_useJumbo["lack"]
            sql = "SELECT tabT.jediTaskID,datasetID,currentPriority,nFilesToBeUsed-nFilesUsed,tabD.type,tabT.status,"
            sql += f"tabT.{attr_name_for_group_by},nFiles,nEvents,nFilesWaiting,tabT.useJumbo "
            sql += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_Datasets tabD,{0}.JEDI_AUX_Status_MinTaskID tabA ".format(panda_config.schemaJEDI)
            sql += "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID AND tabT.jediTaskID=tabD.jediTaskID "
            sql += "AND tabT.vo=:vo "
            if work_queue.is_global_share:
                sql += "AND gshare=:wq_name "
                sql += f"AND workqueue_id NOT IN (SELECT queue_id FROM {panda_config.schemaJEDI}.jedi_work_queue WHERE queue_function = 'Resource') "
                var_map[":wq_name"] = work_queue.queue_name
            else:
                sql += "AND workQueue_ID=:wq_id "
                var_map[":wq_id"] = work_queue.queue_id
            if resource_name:
                sql += "AND resource_type=:resource_name "
                var_map[":resource_name"] = resource_name
            if prod_source_label not in [None, "", "any"]:
                sql += "AND prodSourceLabel=:prodSourceLabel "
            if cloud_name not in [None, "", "any"]:
                sql += "AND tabT.cloud=:cloud "
            tstat_var_names_str, tstat_var_map = get_sql_IN_bind_variables(JediTaskSpec.statusForJobGenerator(), prefix=":tstat_", value_as_suffix=True)
            sql += f"AND tabT.status IN ({tstat_var_names_str}) "
            var_map.update(tstat_var_map)
            sql += "AND tabT.lockedBy IS NULL "
            sql += "AND tabT.modificationTime<:timeLimit "
            sql += "AND "
            sql += "(tabT.useJumbo=:useJumboLack "
            if merge_un_throttled is True:
                tmp_var_names_str = MERGE_TYPES_var_str
                tmp_var_map = MERGE_TYPES_var_map
            else:
                tmp_var_names_str = PROCESS_TYPES_var_str
                tmp_var_map = PROCESS_TYPES_var_map
            var_map.update(tmp_var_map)
            sql += f"OR (nFilesToBeUsed > nFilesUsed AND tabD.status<>:dsStatusRemoved AND type IN ({tmp_var_names_str})) "
            if merge_un_throttled is True:
                sql += f"OR (tabT.useJumbo IS NOT NULL AND nFilesWaiting IS NOT NULL AND nFilesToBeUsed>(nFilesUsed+nFilesWaiting) AND type IN ({INPUT_TYPES_var_str})) "
                var_map.update(INPUT_TYPES_var_map)
            sql += ") "
            sql += "AND tabD.status IN (:dsStatus1,:dsStatus2) "
            sql += "AND masterID IS NULL "
            if min_priority is not None:
                var_map[":minPriority"] = min_priority
                sql += "AND currentPriority>=:minPriority "
            sql += "AND NOT EXISTS "
            sql += f"(SELECT 1 FROM {panda_config.schemaJEDI}.JEDI_Datasets "
            sql += f"WHERE {panda_config.schemaJEDI}.JEDI_Datasets.jediTaskID=tabT.jediTaskID "
            if merge_un_throttled is True:
                tmp_var_names_str = MERGE_TYPES_var_str
            else:
                tmp_var_names_str = PROCESS_TYPES_var_str
            sql += f"AND type IN ({tmp_var_names_str}) "
            sql += "AND NOT status IN (:dsOKStatus1,:dsOKStatus2,:dsOKStatus3,:dsOKStatus4,:dsOKStatus5,:dsOKStatus6,:dsOKStatus7)) "
            sql += "ORDER BY currentPriority DESC,jediTaskID "
        else:
            var_map = {}
            if not simulation_with_file_stat:
                sql = "SELECT tabT.jediTaskID,datasetID,currentPriority,nFilesToBeUsed-nFilesUsed,tabD.type,tabT.status,"
                sql += f"tabT.{attr_name_for_group_by},nFiles,nEvents,nFilesWaiting,tabT.useJumbo "
            else:
                sql = "SELECT tabT.jediTaskID,datasetID,currentPriority,nFilesToBeUsed,tabD.type,tabT.status,"
                sql += f"tabT.{attr_name_for_group_by},nFiles,nEvents,nFilesWaiting,tabT.useJumbo "
            sql += f"FROM {panda_config.schemaJEDI}.JEDI_Tasks tabT,{panda_config.schemaJEDI}.JEDI_Datasets tabD "
            tasks_to_loop = target_tasks
            sql += "WHERE tabT.jediTaskID=tabD.jediTaskID "
            taskid_var_names_str, taskid_var_map = get_sql_IN_bind_variables(tasks_to_loop, prefix=":jediTaskID")
            sql += f"AND tabT.jediTaskID IN ({taskid_var_names_str}) "
            var_map.update(taskid_var_map)
            sql += f"AND type IN ({PROCESS_TYPES_var_str}) "
            var_map.update(PROCESS_TYPES_var_map)
            sql += "AND masterID IS NULL "
            if target_datasets is not None:
                dsid_var_names_str, dsid_var_map = get_sql_IN_bind_variables(target_datasets, prefix=":datasetID")
                sql += f"AND tabD.datasetID IN ({dsid_var_names_str}) "
                var_map.update(dsid_var_map)
            if not simulation_with_file_stat:
                var_map[":dsStatusRemoved"] = "removed"
                sql += "AND nFilesToBeUsed > nFilesUsed AND tabD.status<>:dsStatusRemoved "
        # begin transaction
        self.conn.begin()
        self.cur.arraysize = 100000
        # select
        tmp_log.debug(sql + comment + str(var_map))
        self.cur.execute(sql + comment, var_map)
        res_list = self.cur.fetchall()
        # commit
        if not self._commit():
            raise RuntimeError("Commit error")
        return res_list

    # make dictionaries for tasks and datasets with unprocessed inputs
    def _make_dicts_tasks_datasets_with_unprocessed_inputs(
        self, res_list: list, tmp_log: LogWrapper, work_queue: WorkQueue, is_peeking: bool, super_high_prio_task_ratio: int | None, set_group_by_attr: bool
    ) -> tuple[dict, dict, list, dict, dict, dict] | int:
        """
        Make dictionaries for tasks and datasets with unprocessed inputs

        :param res_list: List of tasks and datasets with unprocessed inputs.
        :param tmp_log: Logger object.
        :param work_queue: Work queue object.
        :param is_peeking: Whether to peek at the highest priority among waiting tasks without any interventions.
        :param super_high_prio_task_ratio: Ratio for superhigh priority tasks.
        :param set_group_by_attr: Whether to aggregate tasks by an attribute.
        :return: Various dictionaries of tasks and datasets for later processing.
        """
        # make return
        task_dataset_map = {}
        task_status_map = {}
        jedi_task_id_list = []
        task_user_prio_map = {}
        task_prio_map = {}
        task_with_jumbo_map = {}
        task_group_by_attr_map = {}
        express_attr = "express_group_by"
        task_merge_map = {}
        for (
            jedi_task_id,
            dataset_id,
            current_priority,
            n_files_unprocessed,
            dataset_type,
            task_status,
            group_by_name,
            total_input_files,
            total_input_events,
            n_files_waiting,
            use_jumbo,
        ) in res_list:
            tmp_log.debug(
                "jediTaskID={0} datasetID={1} tmpNumFiles={2} type={3} prio={4} useJumbo={5} nFilesWaiting={6}".format(
                    jedi_task_id, dataset_id, n_files_unprocessed, dataset_type, current_priority, use_jumbo, n_files_waiting
                )
            )

            # return the max priority for peeking mode
            if is_peeking:
                return current_priority
            # make task-status mapping
            task_status_map[jedi_task_id] = task_status
            # make task-useJumbo mapping
            task_with_jumbo_map[jedi_task_id] = use_jumbo
            # make task and group-by attribute mapping
            task_group_by_attr_map[jedi_task_id] = group_by_name
            # make task-dataset mapping
            if jedi_task_id not in task_dataset_map:
                task_dataset_map[jedi_task_id] = []
            data = (dataset_id, n_files_unprocessed, dataset_type, total_input_files, total_input_events, n_files_waiting, use_jumbo)
            if dataset_type in JediDatasetSpec.getMergeProcessTypes():
                task_dataset_map[jedi_task_id].insert(0, data)
            else:
                task_dataset_map[jedi_task_id].append(data)
            # overwrite group-by name to be a single value if WQ has a share
            if work_queue is not None and work_queue.queue_share is not None and not set_group_by_attr:
                group_by_name = ""
            elif current_priority >= JobUtils.priorityTasksToJumpOver:
                # use a special value to collect superhigh prio tasks into one group
                group_by_name = express_attr
            # increase priority so that scouts do not wait behind the bulk
            if task_status in ["scouting"]:
                current_priority += 1
            # make task-prio mapping
            task_prio_map[jedi_task_id] = current_priority
            if group_by_name not in task_user_prio_map:
                task_user_prio_map[group_by_name] = {}
            if current_priority not in task_user_prio_map[group_by_name]:
                task_user_prio_map[group_by_name][current_priority] = []
            if jedi_task_id not in task_user_prio_map[group_by_name][current_priority]:
                task_user_prio_map[group_by_name][current_priority].append(jedi_task_id)
            task_merge_map.setdefault(jedi_task_id, True)
            if dataset_type not in JediDatasetSpec.getMergeProcessTypes():
                task_merge_map[jedi_task_id] = False
        # sort tasks by priority per group
        sorted_tasks_per_group = {}
        for group_by_name in task_user_prio_map.keys():
            # use high priority tasks first
            priority_list = sorted(task_user_prio_map[group_by_name].keys())
            priority_list.reverse()
            for current_priority in priority_list:
                tmp_merge_tasks = []
                sorted_tasks_per_group.setdefault(group_by_name, [])
                # randomize superhigh prio tasks to avoid that multiple threads try to get the same tasks
                if group_by_name == express_attr:
                    random.shuffle(task_user_prio_map[group_by_name][current_priority])
                for jedi_task_id in task_user_prio_map[group_by_name][current_priority]:
                    if task_merge_map[jedi_task_id]:
                        tmp_merge_tasks.append(jedi_task_id)
                    else:
                        sorted_tasks_per_group[group_by_name].append(jedi_task_id)
                sorted_tasks_per_group[group_by_name] = tmp_merge_tasks + sorted_tasks_per_group[group_by_name]
        # shuffle tasks to avoid too many tasks coming from a few groups
        group_by_name_list = list(sorted_tasks_per_group.keys())
        random.shuffle(group_by_name_list)
        tmp_log.debug(f"{len(group_by_name_list)} groupBy values for {len(task_dataset_map)} tasks")
        if express_attr in sorted_tasks_per_group:
            use_super_high = True
        else:
            use_super_high = False
        n_pick_up = 10
        while group_by_name_list:
            # pickup one task from each group
            for group_by_name in group_by_name_list:
                if not sorted_tasks_per_group[group_by_name]:
                    group_by_name_list.remove(group_by_name)
                else:
                    # add high prio tasks first
                    if use_super_high and express_attr in sorted_tasks_per_group and random.randint(1, 100) <= super_high_prio_task_ratio:
                        tmp_group_by_attr_list = [express_attr]
                    else:
                        tmp_group_by_attr_list = []
                    # add normal tasks
                    tmp_group_by_attr_list.append(group_by_name)
                    for tmp_group_by_attr in tmp_group_by_attr_list:
                        for _ in range(n_pick_up):
                            if len(sorted_tasks_per_group[tmp_group_by_attr]) > 0:
                                jedi_task_id = sorted_tasks_per_group[tmp_group_by_attr].pop(0)
                                jedi_task_id_list.append(jedi_task_id)
                                # add next task if contains only unmerged inputs
                                if not task_merge_map[jedi_task_id]:
                                    break
                            else:
                                break
        return task_dataset_map, task_status_map, jedi_task_id_list, task_prio_map, task_with_jumbo_map, task_group_by_attr_map

    # read a task with unprocessed inputs
    def _read_task_with_unprocessed_inputs(
        self,
        jedi_task_id: int,
        task_status_map: dict,
        locked_tasks: list,
        tmp_log: LogWrapper,
        comment: str,
        pid: str,
        locked_by_another: list,
        is_dry_run: bool,
        ignore_lock: bool,
        task_dataset_map: dict,
        contain_merging: bool,
        ds_with_fake_co_jumbo: set,
        time_limit: datetime.datetime,
        target_tasks: list | None,
    ) -> tuple[bool, JediTaskSpec | None, list, list]:
        """
        Read a task with unprocessed inputs

        :param jedi_task_id: JEDI task ID.
        :param task_status_map: Mapping of task status.
        :param locked_tasks: List of locked tasks.
        :param tmp_log: Logger object.
        :param comment: Comment for SQL queries.
        :param pid: Process ID.
        :param locked_by_another: List of tasks locked by another process.
        :param is_dry_run: Whether it is a dry run.
        :param ignore_lock: Whether to ignore the lock.
        :param task_dataset_map: Mapping of tasks and datasets.
        :param contain_merging: Whether the task contains unmerged inputs.
        :param ds_with_fake_co_jumbo: Set of datasets with fake co-jumbo.
        :param time_limit: Time limit for locking tasks.
        :param target_tasks: List of targeted tasks.
        :return Tuple of (flag to skip, task specification, updated locked task list, updated locked by another list, updated task list).
        """

        # sql to read a task
        sql_read_task = f"SELECT {JediTaskSpec.columnNames()} "
        sql_read_task += f"FROM {panda_config.schemaJEDI}.JEDI_Tasks "
        sql_read_task += "WHERE jediTaskID=:jediTaskID AND status=:statusInDB "
        if not ignore_lock:
            sql_read_task += "AND lockedBy IS NULL "
        if not is_dry_run:
            sql_read_task += "FOR UPDATE NOWAIT "
        # sql to read a locked task
        sql_read_locked_task = f"SELECT {JediTaskSpec.columnNames()} "
        sql_read_locked_task += f"FROM {panda_config.schemaJEDI}.JEDI_Tasks "
        sql_read_locked_task += "WHERE jediTaskID=:jediTaskID AND status=:statusInDB AND lockedBy=:newLockedBy "
        if not is_dry_run:
            sql_read_locked_task += "FOR UPDATE NOWAIT "
        # sql to lock task
        sql_to_lock_task = f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks  "
        sql_to_lock_task += "SET lockedBy=:newLockedBy,lockedTime=CURRENT_DATE,modificationTime=CURRENT_DATE "
        sql_to_lock_task += "WHERE jediTaskID=:jediTaskID AND status=:status AND lockedBy IS NULL AND modificationTime<:timeLimit "
        # sql to check files
        sel_check_files = (
            f"SELECT nFilesToBeUsed-nFilesUsed FROM {panda_config.schemaJEDI}.JEDI_Datasets WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
        )
        # read task
        to_skip = False
        orig_task_spec = None
        try:
            # read task
            var_map = {}
            var_map[":jediTaskID"] = jedi_task_id
            var_map[":statusInDB"] = task_status_map[jedi_task_id]
            if jedi_task_id not in locked_tasks:
                tmp_log.debug(sql_read_task + comment + str(var_map))
                self.cur.execute(sql_read_task + comment, var_map)
            else:
                var_map[":newLockedBy"] = pid
                tmp_log.debug(sql_read_locked_task + comment + str(var_map))
                self.cur.execute(sql_read_locked_task + comment, var_map)
            tmp_res = self.cur.fetchone()
            # locked by another
            if tmp_res is None:
                to_skip = True
                tmp_log.debug(f"skip locked jediTaskID={jedi_task_id}")
                locked_by_another.append(jedi_task_id)
                if not self._commit():
                    raise RuntimeError("Commit error")
                return to_skip, orig_task_spec, locked_tasks, locked_by_another
            else:
                orig_task_spec = JediTaskSpec()
                orig_task_spec.pack(tmp_res)
            # check nFiles in datasets
            if not is_dry_run and not ignore_lock and not target_tasks:
                to_skip = False
                for tmp_item in task_dataset_map[jedi_task_id]:
                    tmp_dataset_id, n_files_unprocessed = tmp_item[:2]
                    var_map = {}
                    var_map[":jediTaskID"] = jedi_task_id
                    var_map[":datasetID"] = tmp_dataset_id
                    self.cur.execute(sel_check_files + comment, var_map)
                    (newNumFiles,) = self.cur.fetchone()
                    tmp_log.debug(f"jediTaskID={jedi_task_id} datasetID={tmp_dataset_id} nFilesToBeUsed-nFilesUsed old:{n_files_unprocessed} new:{newNumFiles}")
                    if n_files_unprocessed > newNumFiles:
                        tmp_log.debug(f"skip jediTaskID={jedi_task_id} since nFilesToBeUsed-nFilesUsed decreased")
                        locked_by_another.append(jedi_task_id)
                        to_skip = True
                        break
                if to_skip:
                    if not self._commit():
                        raise RuntimeError("Commit error")
                    return to_skip, orig_task_spec, locked_tasks, locked_by_another
            # skip fake co-jumbo for scouting
            if not contain_merging and len(ds_with_fake_co_jumbo) > 0 and orig_task_spec.useScout() and not orig_task_spec.isPostScout():
                to_skip = True
                tmp_log.debug(f"skip scouting jumbo jediTaskID={jedi_task_id}")
                if not self._commit():
                    raise RuntimeError("Commit error")
                return to_skip, orig_task_spec, locked_tasks, locked_by_another
            # lock task
            if not is_dry_run and jedi_task_id not in locked_tasks:
                var_map = {}
                var_map[":jediTaskID"] = jedi_task_id
                var_map[":newLockedBy"] = pid
                var_map[":status"] = task_status_map[jedi_task_id]
                var_map[":timeLimit"] = time_limit
                tmp_log.debug(sql_to_lock_task + comment + str(var_map))
                self.cur.execute(sql_to_lock_task + comment, var_map)
                n_row = self.cur.rowcount
                if n_row != 1:
                    tmp_log.debug(f"failed to lock jediTaskID={jedi_task_id}")
                    locked_by_another.append(jedi_task_id)
                    to_skip = True
                    if not self._commit():
                        raise RuntimeError("Commit error")
                    return to_skip, orig_task_spec, locked_tasks, locked_by_another
                # list of locked tasks
                if jedi_task_id not in locked_tasks:
                    locked_tasks.append(jedi_task_id)
            return to_skip, orig_task_spec, locked_tasks, locked_by_another
        except Exception:
            err_type, err_value = sys.exc_info()[:2]
            if self.isNoWaitException(err_value):
                # resource busy and acquire with NOWAIT specified
                to_skip = True
                tmp_log.debug(f"skip locked with NOWAIT jediTaskID={jedi_task_id}")
                if not self._commit():
                    raise RuntimeError("Commit error")
                return to_skip, orig_task_spec, locked_tasks, locked_by_another
            else:
                # failed with something else
                raise err_type(err_value)

    # check a task with unprocessed inputs
    def _check_task_with_unprocessed_inputs(
        self, jedi_task_id: int, comment: str, tmp_log: LogWrapper, original_task_spec: JediTaskSpec, is_dry_run: bool
    ) -> tuple[bool, int | None, dict | None]:
        """
        Check a task with unprocessed inputs. Count the number of available files when the task avalanches. Change userName for user tasks. Get the number of HPO workers and finish the task if enough workers have been done.

        :param jedi_task_id: JEDI task ID.
        :param comment: Comment for SQL queries.
        :param tmp_log: Logger object.
        :param original_task_spec: Original task specification.
        :param is_dry_run: Whether it is a dry run.
        :return: Tuple of (flag to skip, the number of files for avalanche, dict for the number of HPO samples)
        """
        # sql to count the number of files for avalanche
        sql_avalanche = f"SELECT SUM(nFiles-nFilesToBeUsed) FROM {panda_config.schemaJEDI}.JEDI_Datasets "
        sql_avalanche += f"WHERE jediTaskID=:jediTaskID AND type IN ({INPUT_TYPES_var_str}) "
        sql_avalanche += "AND masterID IS NULL "
        # sql to read DN
        sql_dn = f"SELECT dn FROM {panda_config.schemaMETA}.users WHERE name=:name "
        # sql to update task status
        sql_update_task_status = (
            "UPDATE {0}.JEDI_Tasks " "SET lockedBy=NULL,lockedTime=NULL,status=:status,errorDialog=:err " "WHERE jediTaskID=:jediTaskID "
        ).format(panda_config.schemaJEDI)
        # sql to get number of events
        sql_get_n_events = (
            "SELECT COUNT(*),datasetID FROM {0}.JEDI_Events " "WHERE jediTaskID=:jediTaskID AND status=:eventStatus " "GROUP BY datasetID "
        ).format(panda_config.schemaJEDI)
        # sql to get number of ready HPO workers
        sql_get_n_hpo_workers = (
            "SELECT COUNT(*),datasetID FROM ("
            "(SELECT j.PandaID,f.datasetID FROM {0}.jobsDefined4 j, {0}.filesTable4 f "
            "WHERE j.jediTaskID=:jediTaskID AND f.PandaID=j.PandaID AND f.type=:f_type "
            "UNION "
            "SELECT j.PandaID,f.datasetID FROM {0}.jobsActive4 j, {0}.filesTable4 f "
            "WHERE j.jediTaskID=:jediTaskID AND f.PandaID=j.PandaID AND f.type=:f_type) "
            "MINUS "
            "SELECT PandaID,datasetID FROM {1}.JEDI_Events "
            "WHERE  jediTaskID=:jediTaskID AND "
            "status IN (:esSent,:esRunning)"
            ") GROUP BY datasetID"
        ).format(panda_config.schemaPANDA, panda_config.schemaJEDI)
        # sql to set frozenTime
        sql_update_frozen_time = f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks SET frozenTime=:frozenTime WHERE jediTaskID=:jediTaskID "

        to_skip = False
        num_avalanche = None
        num_hpo_workers = None
        # count the number of files for avalanche
        if not to_skip:
            var_map = {}
            var_map[":jediTaskID"] = jedi_task_id
            var_map.update(INPUT_TYPES_var_map)
            tmp_log.debug(sql_avalanche + comment + str(var_map))
            self.cur.execute(sql_avalanche + comment, var_map)
            tmp_res = self.cur.fetchone()
            tmp_log.debug(str(tmp_res))
            if tmp_res is None:
                # no file info
                to_skip = True
                tmp_log.error("skipped since failed to get number of files for avalanche")
                return to_skip, num_avalanche, num_hpo_workers
            else:
                (num_avalanche,) = tmp_res
        # change userName for analysis
        if not to_skip:
            # for analysis use DN as userName
            if original_task_spec.prodSourceLabel in ["user"]:
                var_map = {}
                var_map[":name"] = original_task_spec.userName
                tmp_log.debug(sql_dn + comment + str(var_map))
                self.cur.execute(sql_dn + comment, var_map)
                tmp_res = self.cur.fetchone()
                tmp_log.debug(tmp_res)
                if tmp_res is None:
                    # no user info
                    to_skip = True
                    tmp_log.error(f"skipped since failed to get DN for {original_task_spec.userName} jediTaskID={jedi_task_id}")
                else:
                    original_task_spec.origUserName = original_task_spec.userName
                    (original_task_spec.userName,) = tmp_res
                    if original_task_spec.userName in ["", None]:
                        # DN is empty
                        to_skip = True
                        err_msg = f"{original_task_spec.origUserName} has an empty DN"
                        tmp_log.error(f"{err_msg} for jediTaskID={jedi_task_id}")
                        var_map = {}
                        var_map[":jediTaskID"] = jedi_task_id
                        var_map[":status"] = "tobroken"
                        var_map[":err"] = err_msg
                        self.cur.execute(sql_update_task_status + comment, var_map)
                    else:
                        # reset the change to preserve userName
                        original_task_spec.resetChangedAttr("userName")
                if to_skip:
                    if not self._commit():
                        raise RuntimeError("Commit error")
                    return to_skip, num_avalanche, num_hpo_workers
        # checks for HPO
        if not to_skip and not is_dry_run:
            if original_task_spec.is_hpo_workflow():
                # number of HPO jobs
                num_max_hpo_jobs = original_task_spec.get_max_num_jobs()
                if num_max_hpo_jobs is not None:
                    sql_n_hpo_jobs = f"SELECT total_req_jobs FROM {panda_config.schemaDEFT}.T_TASK "
                    sql_n_hpo_jobs += "WHERE taskid=:taskid "
                    var_map = {}
                    var_map[":taskID"] = jedi_task_id
                    self.cur.execute(sql_n_hpo_jobs + comment, var_map)
                    (tmp_num_hpo_jobs,) = self.cur.fetchone()
                    if tmp_num_hpo_jobs >= num_max_hpo_jobs:
                        var_map = {}
                        var_map[":jediTaskID"] = jedi_task_id
                        var_map[":status"] = original_task_spec.status
                        var_map[":err"] = "skipped max number of HPO jobs reached"
                        self.cur.execute(sql_update_task_status + comment, var_map)
                        tmp_log.debug(f"jediTaskID={jedi_task_id} to finish due to maxNumHpoJobs={num_max_hpo_jobs} numHpoJobs={tmp_num_hpo_jobs}")
                        if not self._commit():
                            raise RuntimeError("Commit error")
                        # send finish command
                        get_task_event_module(self).sendCommandTaskPanda(
                            jedi_task_id, "HPO task finished since max_num_jobs reached", True, "finish", comQualifier="soft"
                        )
                        to_skip = True
                        return to_skip, num_avalanche, num_hpo_workers
                # get number of active samples
                var_map = {}
                var_map[":jediTaskID"] = jedi_task_id
                var_map[":eventStatus"] = EventServiceUtils.ST_ready
                self.cur.execute(sql_get_n_events + comment, var_map)
                tmp_res = self.cur.fetchall()
                num_hpo_workers = {}
                total_num_events_hpo = 0
                for tmp_num_events_hpo, dataset_id_hpo in tmp_res:
                    num_hpo_workers[dataset_id_hpo] = tmp_num_events_hpo
                    total_num_events_hpo += tmp_num_events_hpo
                # subtract ready workers
                var_map = {}
                var_map[":jediTaskID"] = jedi_task_id
                var_map[":esSent"] = EventServiceUtils.ST_sent
                var_map[":esRunning"] = EventServiceUtils.ST_running
                var_map[":f_type"] = "pseudo_input"
                self.cur.execute(sql_get_n_hpo_workers + comment, var_map)
                tmp_res = self.cur.fetchall()
                total_num_workers_hpo = 0
                for tmp_num_workers_hpo, dataset_id_hpo in tmp_res:
                    total_num_workers_hpo += tmp_num_workers_hpo
                    if dataset_id_hpo in num_hpo_workers:
                        num_hpo_workers[dataset_id_hpo] -= tmp_num_workers_hpo
                # go to pending if no events (samples)
                if not [i for i in num_hpo_workers.values() if i > 0]:
                    var_map = {}
                    var_map[":jediTaskID"] = jedi_task_id
                    var_map[":status"] = original_task_spec.status
                    if not num_hpo_workers:
                        var_map[":err"] = "skipped since no HP points to evaluate"
                    else:
                        var_map[":err"] = "skipped since enough HPO jobs are running or scheduled"
                    self.cur.execute(sql_update_task_status + comment, var_map)
                    # set frozenTime
                    if total_num_events_hpo + total_num_workers_hpo == 0 and original_task_spec.frozenTime is None:
                        var_map = {}
                        var_map[":jediTaskID"] = jedi_task_id
                        var_map[":frozenTime"] = naive_utcnow()
                        self.cur.execute(sql_update_frozen_time + comment, var_map)
                    elif total_num_events_hpo + total_num_workers_hpo > 0 and original_task_spec.frozenTime is not None:
                        var_map = {}
                        var_map[":jediTaskID"] = jedi_task_id
                        var_map[":frozenTime"] = None
                        self.cur.execute(sql_update_frozen_time + comment, var_map)
                    tmp_log.debug(
                        f"HPO jediTaskID={jedi_task_id} skipped due to nSamplesToEvaluate={total_num_events_hpo} nReadyWorkers={total_num_workers_hpo}"
                    )
                    if not self._commit():
                        raise RuntimeError("Commit error")
                    # terminate if inactive for long time
                    wait_interval = 24
                    if (
                        total_num_events_hpo + total_num_workers_hpo == 0
                        and original_task_spec.frozenTime is not None
                        and naive_utcnow() - original_task_spec.frozenTime > datetime.timedelta(hours=wait_interval)
                    ):
                        # send finish command
                        get_task_event_module(self).sendCommandTaskPanda(
                            jedi_task_id, "HPO task finished since inactive for one day", True, "finish", comQualifier="soft"
                        )
                    to_skip = True
                    return to_skip, num_avalanche, num_hpo_workers
        return to_skip, num_avalanche, num_hpo_workers

    # get memory requirements of unprocessed inputs
    def _get_memory_requirements_unprocessed_inputs(
        self,
        comment: str,
        tmp_log: LogWrapper,
        jedi_task_id: int,
        dataset_id: int,
        dataset_type: str,
        primary_dataset_id: int,
        simulation_with_file_stat: bool,
        orig_n_files_unprocessed: int,
        use_jumbo: bool,
        datasets_with_fake_co_jumbo: set,
    ) -> tuple[bool, list]:
        """
        Get memory requirements of unprocessed inputs and fix file counts of the dataset if necessary

        :param comment: Comment for SQL queries.
        :param tmp_log: Logger object.
        :param jedi_task_id: JEDI task ID.
        :param dataset_id: Dataset ID.
        :param dataset_type: Dataset type.
        :param primary_dataset_id: Primary dataset ID.
        :param simulation_with_file_stat: Whether to read files by ignoring file counts of the dataset.
        :param orig_n_files_unprocessed: The number of files to be read.
        :param use_jumbo: The task uses jumbo jobs.
        :param datasets_with_fake_co_jumbo: Set of datasets with fake co-jumbo.

        :return: Tuple of (flag to skip, memory requirements).
        """
        # sql to read memory requirements of files in dataset
        sql_read_memory = f"""SELECT ramCount FROM {panda_config.schemaJEDI}.JEDI_Dataset_Contents
                   WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID """
        if not simulation_with_file_stat:
            sql_read_memory += """AND status=:status AND (maxAttempt IS NULL OR attemptNr<maxAttempt)
                        AND (maxFailure IS NULL OR failedAttempt<maxFailure) """
        sql_read_memory += "GROUP BY ramCount "
        # sql to read memory requirements for fake co-jumbo
        sql_read_memory_for_co_jumbo = re.sub(
            "jediTaskID=:jediTaskID AND datasetID=:datasetID ", "jediTaskID=:jediTaskID AND datasetID=:datasetID AND is_waiting IS NULL ", sql_read_memory
        )
        # sql to check datasets and files with empty requirements
        sql_check_files = f"SELECT status,attemptNr,maxAttempt,failedAttempt,maxFailure FROM {panda_config.schemaJEDI}.JEDI_Dataset_Contents "
        sql_check_files += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
        sql_check_dataset = f"SELECT nFilesUsed,nFilesToBeUsed,nFilesFinished,nFilesFailed FROM {panda_config.schemaJEDI}.JEDI_Datasets "
        sql_check_dataset += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
        # sql to update file counts of datasets with empty requirements
        sql_update_dataset_n_used_files = f"UPDATE {panda_config.schemaJEDI}.JEDI_Datasets SET nFilesUsed=:nFilesUsed "
        sql_update_dataset_n_used_files += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
        sql_update_dataset_n_unprocessed_files = f"UPDATE {panda_config.schemaJEDI}.JEDI_Datasets SET nFilesToBeUsed=:nFilesToBeUsed "
        sql_update_dataset_n_unprocessed_files += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
        # sql to update datasets with empty requirements
        sql_update_dataset_status = f"UPDATE {panda_config.schemaJEDI}.JEDI_Datasets SET status=:status "
        sql_update_dataset_status += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
        to_skip = False
        # See if there are different memory requirements that need to be mapped to different chunks
        var_map = {}
        var_map[":jediTaskID"] = jedi_task_id
        var_map[":datasetID"] = dataset_id
        if not simulation_with_file_stat:
            if use_jumbo == JediTaskSpec.enum_useJumbo["lack"] and orig_n_files_unprocessed == 0:
                var_map[":status"] = "running"
            else:
                var_map[":status"] = "ready"
        self.cur.arraysize = 100000
        # figure out if there are different memory requirements in the dataset
        if dataset_id not in datasets_with_fake_co_jumbo or use_jumbo == JediTaskSpec.enum_useJumbo["lack"]:
            self.cur.execute(sql_read_memory + comment, var_map)
        else:
            self.cur.execute(sql_read_memory_for_co_jumbo + comment, var_map)
        memory_requirements = [req[0] for req in self.cur.fetchall()]  # Unpack resultset

        # Group 0 and NULL memReqs
        if 0 in memory_requirements and None in memory_requirements:
            memory_requirements.remove(None)

        tmp_log.debug(f"memory requirements for files in jediTaskID={jedi_task_id} datasetID={dataset_id} type={dataset_type} are: {memory_requirements}")
        if not memory_requirements:
            to_skip = True
            tmp_log.debug(f"skip jediTaskID={jedi_task_id} datasetID={primary_dataset_id} since memory requirements are empty")
            var_map = dict()
            var_map[":jediTaskID"] = jedi_task_id
            var_map[":datasetID"] = primary_dataset_id
            self.cur.execute(sql_check_dataset + comment, var_map)
            tmp_n_files_used, tmp_n_files_to_be_used, tmp_n_files_finished, tmp_n_files_failed = self.cur.fetchone()
            var_map = dict()
            var_map[":jediTaskID"] = jedi_task_id
            var_map[":datasetID"] = primary_dataset_id
            self.cur.execute(sql_check_files + comment, var_map)
            tmp_res = self.cur.fetchall()
            n_done = 0
            n_finished = 0
            n_failed = 0
            n_active = 0
            n_running = 0
            n_ready = 0
            n_unknown = 0
            n_lost = 0
            for tmp_file_status, tmp_file_attempt_nr, tmp_file_max_attempt, tmp_file_failed_attempt, tmp_file_max_failure in tmp_res:
                if tmp_file_status in ["missing", "lost"]:
                    n_lost += 1
                elif tmp_file_status in ["finished", "failed", "cancelled"] or (
                    tmp_file_status == "ready"
                    and (tmp_file_attempt_nr >= tmp_file_max_attempt or (tmp_file_max_failure and tmp_file_failed_attempt >= tmp_file_max_failure))
                ):
                    n_done += 1
                    if tmp_file_status == "finished":
                        n_finished += 1
                    else:
                        n_failed += 1
                else:
                    n_active += 1
                    if tmp_file_status in ["running", "merging", "picked"]:
                        n_running += 1
                    elif tmp_file_status == "ready":
                        n_ready += 1
                    else:
                        n_unknown += 1
            tmp_msg = "jediTaskID={} datasetID={} to check due to empty memory requirements :" " nDone={} nActive={} nReady={} ".format(
                jedi_task_id, primary_dataset_id, n_done, n_active, n_ready
            )
            tmp_msg += f"nRunning={n_running} nFinished={n_finished} nFailed={n_failed} nUnknown={n_unknown} nLost={n_lost} "
            tmp_msg += "ds.nFilesUsed={} nFilesToBeUsed={} ds.nFilesFinished={} " "ds.nFilesFailed={}".format(
                tmp_n_files_used, tmp_n_files_to_be_used, tmp_n_files_finished, tmp_n_files_failed
            )
            tmp_log.debug(tmp_msg)
            if tmp_n_files_used < tmp_n_files_to_be_used and tmp_n_files_to_be_used > 0 and n_unknown == 0:
                var_map = dict()
                var_map[":jediTaskID"] = jedi_task_id
                var_map[":datasetID"] = primary_dataset_id
                var_map[":nFilesUsed"] = n_done + n_active
                self.cur.execute(sql_update_dataset_n_used_files + comment, var_map)
                tmp_log.debug(
                    "jediTaskID={} datasetID={} set nFilesUsed={} from {} "
                    "to fix empty memory req".format(jedi_task_id, primary_dataset_id, var_map[":nFilesUsed"], tmp_n_files_used)
                )
            if tmp_n_files_to_be_used > n_done + n_running + n_ready:
                var_map = dict()
                var_map[":jediTaskID"] = jedi_task_id
                var_map[":datasetID"] = primary_dataset_id
                var_map[":nFilesToBeUsed"] = n_done + n_running + n_ready
                self.cur.execute(sql_update_dataset_n_unprocessed_files + comment, var_map)
                tmp_log.debug(
                    "jediTaskID={} datasetID={} set nFilesToBeUsed={} from {} "
                    "to fix empty memory req ".format(jedi_task_id, primary_dataset_id, var_map[":nFilesToBeUsed"], tmp_n_files_to_be_used)
                )
            if n_active == 0:
                var_map = dict()
                var_map[":jediTaskID"] = jedi_task_id
                var_map[":datasetID"] = primary_dataset_id
                var_map[":status"] = "finished"
                self.cur.execute(sql_update_dataset_status + comment, var_map)
                tmp_log.debug(f"jediTaskID={jedi_task_id} datasetID={primary_dataset_id} set status=finished to fix empty memory requirements")
        return to_skip, memory_requirements

    # get datasets with unprocessed inputs
    def _get_datasets_with_unprocessed_inputs(
        self,
        comment: str,
        tmp_log: LogWrapper,
        input_chunk_list: list,
        task_spec: JediTaskSpec,
        jedi_task_id: int,
        dataset_id: int,
        dataset_type: str,
        dataset_id_list: list,
        is_dry_run: bool,
        simulation_with_file_stat: bool,
        num_avalanche: int,
        read_min_files: bool,
    ) -> tuple[bool, list, list]:
        """
        Add datasets to input chunks, append secondary dataset IDs, and return updated input chunks and dataset IDs.

        :param comment: Comment for SQL queries.
        :param tmp_log: Logger object.
        :param input_chunk_list: List of input chunks.
        :param task_spec: Task specification.
        :param jedi_task_id: JEDI task ID.
        :param dataset_id: Dataset ID.
        :param dataset_type: Dataset type.
        :param dataset_id_list: List of dataset IDs.
        :param is_dry_run: Whether it is a dry run.
        :param simulation_with_file_stat: Whether to read files by ignoring file counts of the dataset.
        :param num_avalanche: Number of files for avalanche.
        :param read_min_files: Whether to read minimum files.
        :return: Tuple of (flag to skip, dataset IDs, input chunks).
        """
        to_skip = False
        # sql to read secondary dataset IDs
        sql_read_secondary_dataset_ids = f"SELECT datasetID FROM {panda_config.schemaJEDI}.JEDI_Datasets WHERE jediTaskID=:jediTaskID "
        # sql to read datasets
        sql_read_datasets = f"SELECT {JediDatasetSpec.columnNames()} "
        sql_read_datasets += f"FROM {panda_config.schemaJEDI}.JEDI_Datasets "
        sql_read_datasets += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
        if not is_dry_run:
            sql_read_datasets += "FOR UPDATE NOWAIT "
        # append secondary dataset IDs
        var_map = {}
        if dataset_type not in JediDatasetSpec.getMergeProcessTypes():
            # for normal process
            tmp_var_names_str = INPUT_TYPES_var_str
            tmp_var_map = INPUT_TYPES_var_map
        else:
            # for merge process
            tmp_var_names_str = MERGE_TYPES_var_str
            tmp_var_map = MERGE_TYPES_var_map
        var_map.update(tmp_var_map)
        if not simulation_with_file_stat:
            sql_read_secondary_dataset_ids += f"AND nFilesToBeUsed >= nFilesUsed "
        sql_read_secondary_dataset_ids += f"AND type IN ({tmp_var_names_str}) "
        if not is_dry_run:
            sql_read_secondary_dataset_ids += "AND status=:dsStatus "
            var_map[":dsStatus"] = "ready"
        sql_read_secondary_dataset_ids += "AND masterID=:masterID "
        var_map[":jediTaskID"] = jedi_task_id
        var_map[":masterID"] = dataset_id
        self.cur.execute(sql_read_secondary_dataset_ids + comment, var_map)
        tmp_res = self.cur.fetchall()
        for (tmp_dataset_id,) in tmp_res:
            dataset_id_list.append(tmp_dataset_id)
        # read datasets
        for dataset_id in dataset_id_list:
            var_map = {}
            var_map[":jediTaskID"] = jedi_task_id
            var_map[":datasetID"] = dataset_id
            try:
                for input_chunk in input_chunk_list:
                    # select
                    self.cur.execute(sql_read_datasets + comment, var_map)
                    tmp_res = self.cur.fetchone()
                    dataset_spec = JediDatasetSpec()
                    dataset_spec.pack(tmp_res)
                    # change stream name for merging
                    if dataset_spec.type in JediDatasetSpec.getMergeProcessTypes():
                        # change OUTPUT to IN
                        dataset_spec.streamName = re.sub("^OUTPUT", "TRN_OUTPUT", dataset_spec.streamName)
                        # change LOG to INLOG
                        dataset_spec.streamName = re.sub("^LOG", "TRN_LOG", dataset_spec.streamName)
                    # add to InputChunk
                    if dataset_spec.isMaster():
                        input_chunk.addMasterDS(dataset_spec)
                    else:
                        input_chunk.addSecondaryDS(dataset_spec)
            except Exception:
                err_type, err_value = sys.exc_info()[:2]
                if self.isNoWaitException(err_value):
                    # resource busy and acquire with NOWAIT specified
                    to_skip = True
                    tmp_log.debug(f"skip locked jediTaskID={jedi_task_id} datasetID={dataset_id}")
                else:
                    # failed with something else
                    raise err_type(err_value)
        # flag input chunks to use scout
        if (num_avalanche == 0 and not input_chunk_list[0].isMutableMaster()) or not task_spec.useScout() or read_min_files:
            for input_chunk in input_chunk_list:
                input_chunk.setUseScout(False)
        else:
            for input_chunk in input_chunk_list:
                input_chunk.setUseScout(True)
        return to_skip, dataset_id_list, input_chunk_list

    # read unprocessed input files
    def _read_unprocessed_inputs(
        self,
        comment: str,
        tmp_log: LogWrapper,
        input_chunk_list: list,
        task_spec: JediTaskSpec,
        jedi_task_id: int,
        dataset_id_list: list,
        total_input_files: int,
        total_input_events: int,
        typical_num_files_map: dict | None,
        max_num_jobs: int | None,
        is_dry_run: bool,
        read_min_files: bool,
        max_files_per_task: int,
        simulation_with_file_stat: bool,
        n_files_unprocessed: int,
        primary_dataset_id: int,
        use_jumbo: bool,
        orig_n_files_unprocessed: int,
        ds_with_fake_co_jumbo: set,
    ) -> tuple[list, int]:
        """
        Read unprocessed input files, duplicate secondary files if necessary, and update file counts in the dataset.

        :param comment: Comment for SQL queries.
        :param tmp_log: Logger object.
        :param input_chunk_list: List of input chunks.
        :param task_spec: Task specification.
        :param jedi_task_id: JEDI task ID.
        :param dataset_id_list: List of dataset IDs.
        :param total_input_files: Total number of input files.
        :param total_input_events: Total number of input events.
        :param typical_num_files_map: Map of typical number of files.
        :param max_num_jobs: Maximum number of jobs.
        :param is_dry_run: Whether it is a dry run.
        :param read_min_files: Whether to read minimum files.
        :param max_files_per_task: Maximum number of files per task.
        :param simulation_with_file_stat: Whether to read files by ignoring file counts of the dataset.
        :param n_files_unprocessed: The number of files to be read.
        :param primary_dataset_id: Primary dataset ID.
        :param use_jumbo: The task uses jumbo jobs.
        :param orig_n_files_unprocessed: The number of files to be read, which could be different from n_files_unprocessed for (co) jumbo jobs
        :param ds_with_fake_co_jumbo: Set of datasets with fake co-jumbo.
        :return: Tuple of (input chunks, the typical number of files per job).
        """
        # sql to read files
        sql_read_files = f"SELECT * FROM (SELECT {JediFileSpec.columnNames()} "
        sql_read_files += f"FROM {panda_config.schemaJEDI}.JEDI_Dataset_Contents WHERE "
        sql_read_files += "jediTaskID=:jediTaskID AND datasetID=:datasetID "
        if not simulation_with_file_stat:
            sql_read_files += "AND status=:status AND (maxAttempt IS NULL OR attemptNr<maxAttempt) "
            sql_read_files += "AND (maxFailure IS NULL OR failedAttempt<maxFailure) "
            sql_read_files += "AND ramCount=:ramCount "
        sql_read_files += "ORDER BY {0}) "
        sql_read_files += "WHERE rownum <= {1}"

        # sql to read files for fake co-jumbo
        sql_read_files_co_jumbo = re.sub(
            "jediTaskID=:jediTaskID AND datasetID=:datasetID ", "jediTaskID=:jediTaskID AND datasetID=:datasetID AND is_waiting IS NULL ", sql_read_files
        )
        # For the cases where the ram count is not set
        sql_read_files_empty_ram = f"SELECT * FROM (SELECT {JediFileSpec.columnNames()} "
        sql_read_files_empty_ram += f"FROM {panda_config.schemaJEDI}.JEDI_Dataset_Contents WHERE "
        sql_read_files_empty_ram += "jediTaskID=:jediTaskID AND datasetID=:datasetID "
        if not simulation_with_file_stat:
            sql_read_files_empty_ram += "AND status=:status AND (maxAttempt IS NULL OR attemptNr<maxAttempt) "
            sql_read_files_empty_ram += "AND (maxFailure IS NULL OR failedAttempt<maxFailure) "
            sql_read_files_empty_ram += "AND (ramCount IS NULL OR ramCount=0) "
        sql_read_files_empty_ram += "ORDER BY {0}) "
        sql_read_files_empty_ram += "WHERE rownum <= {1}"

        # sql to read files for fake co-jumbo for the cases where the ram count is not set
        sql_read_files_co_jumbo_empty_ram = re.sub(
            "jediTaskID=:jediTaskID AND datasetID=:datasetID ",
            "jediTaskID=:jediTaskID AND datasetID=:datasetID AND is_waiting IS NULL ",
            sql_read_files_empty_ram,
        )

        # sql to read files without ramcount
        sql_read_files_ignore_ram = f"SELECT * FROM (SELECT {JediFileSpec.columnNames()} "
        sql_read_files_ignore_ram += f"FROM {panda_config.schemaJEDI}.JEDI_Dataset_Contents WHERE "
        sql_read_files_ignore_ram += "jediTaskID=:jediTaskID AND datasetID=:datasetID "
        if not simulation_with_file_stat:
            sql_read_files_ignore_ram += "AND status=:status AND (maxAttempt IS NULL OR attemptNr<maxAttempt) "
            sql_read_files_ignore_ram += "AND (maxFailure IS NULL OR failedAttempt<maxFailure) "
        sql_read_files_ignore_ram += "ORDER BY {0}) "
        sql_read_files_ignore_ram += "WHERE rownum <= {1}"

        # sql to read files for fake co-jumbo without ramcount
        sql_read_files_co_jumbo_ignore_ram = re.sub(
            "jediTaskID=:jediTaskID AND datasetID=:datasetID ",
            "jediTaskID=:jediTaskID AND datasetID=:datasetID AND is_waiting IS NULL ",
            sql_read_files_ignore_ram,
        )
        # sql to update file counts
        sql_update_file_status = "UPDATE /*+ INDEX_RS_ASC(JEDI_DATASET_CONTENTS (JEDI_DATASET_CONTENTS.JEDITASKID JEDI_DATASET_CONTENTS.DATASETID JEDI_DATASET_CONTENTS.FILEID)) */ {0}.JEDI_Dataset_Contents SET status=:nStatus ".format(
            panda_config.schemaJEDI
        )
        sql_update_file_status += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID AND status=:oStatus "
        # sql to update file usage info in dataset
        sql_update_file_stat = f"UPDATE {panda_config.schemaJEDI}.JEDI_Datasets SET nFilesUsed=:nFilesUsed "

        sql_update_file_stat += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "

        # determine how many files to read

        # set the typical number of files per job first
        typical_num_files_per_job = 5
        if task_spec.getNumFilesPerJob() is not None:
            # the number of files is specified
            typical_num_files_per_job = task_spec.getNumFilesPerJob()
        elif task_spec.getNumEventsPerJob() is not None:
            typical_num_files_per_job = 1
            try:
                if task_spec.getNumEventsPerJob() > (total_input_events // total_input_files):
                    typical_num_files_per_job = task_spec.getNumEventsPerJob() * total_input_files // total_input_events
            except Exception:
                pass
            if typical_num_files_per_job < 1:
                typical_num_files_per_job = 1
        elif typical_num_files_map is not None and task_spec.processingType in typical_num_files_map and typical_num_files_map[task_spec.processingType] > 0:
            # based on typical usage
            typical_num_files_per_job = typical_num_files_map[task_spec.processingType]
        tmp_log.debug(f"jediTaskID={jedi_task_id} typicalNumFilesPerJob={typical_num_files_per_job}")
        # define the upper limit on the number of files to read in this cycle
        if max_num_jobs is not None and not input_chunk_list[0].isMerging and not input_chunk_list[0].useScout():
            max_files_to_read_this_cycle = min(max_files_per_task, typical_num_files_per_job * max_num_jobs + 10)
        else:
            max_files_to_read_this_cycle = max_files_per_task
        # set a lower limit to avoid too many small chunks
        if not is_dry_run:
            max_files_to_read_this_cycle = max(max_files_to_read_this_cycle, 100)
        # define the number of master files to read
        n_master_files_to_read = min(max_files_to_read_this_cycle, n_files_unprocessed)
        if max_files_to_read_this_cycle > n_files_unprocessed:
            # reading with a fix size of block
            read_block = False
        else:
            read_block = True

        # read files
        total_already_read_files_map = {}
        total_events_map = {}
        max_secondary_files_to_read_with_event_ratio = 10000
        # loop over all input chunks
        for input_chunk in input_chunk_list:
            # check if sequence numbers need to be consistent with masters
            to_be_used_with_same_master = False
            if (task_spec.getNumFilesPerJob() or task_spec.getNumEventsPerJob()) and not task_spec.dynamicNumEvents():
                for dataset_id in dataset_id_list:
                    tmp_dataset_spec = input_chunk.getDatasetWithID(dataset_id)
                    if tmp_dataset_spec.isSeqNumber():
                        to_be_used_with_same_master = True
                        break
            # loop over all dataset IDs
            panda_ids_used_by_master = set()
            panda_ids_used_by_master_list = []
            for dataset_id in dataset_id_list:
                total_already_read_files_map.setdefault(dataset_id, 0)
                total_events_map.setdefault(dataset_id, [])
                tmp_dataset_spec = input_chunk.getDatasetWithID(dataset_id)
                # determine the number of files to read for this dataset
                if tmp_dataset_spec.isMaster():
                    max_files_to_read_for_this_dataset = n_master_files_to_read
                else:
                    # for secondaries
                    if task_spec.useLoadXML() or tmp_dataset_spec.isNoSplit() or tmp_dataset_spec.getEventRatio() is not None:
                        max_files_to_read_for_this_dataset = max_secondary_files_to_read_with_event_ratio
                    elif tmp_dataset_spec.getNumFilesPerJob() is not None:
                        max_files_to_read_for_this_dataset = n_master_files_to_read * tmp_dataset_spec.getNumFilesPerJob()
                    else:
                        max_files_to_read_for_this_dataset = tmp_dataset_spec.getNumMultByRatio(n_master_files_to_read)
                # minimum read
                if read_min_files:
                    max_files_to_read_for_this_dataset = min(max_files_to_read_for_this_dataset, 10)
                # number of files to read for the chunk
                if tmp_dataset_spec.isMaster():
                    num_files_to_read_for_the_chunk = max_files_to_read_for_this_dataset - total_already_read_files_map[dataset_id]
                elif input_chunk.isEmpty:
                    num_files_to_read_for_the_chunk = 0
                else:
                    num_files_to_read_for_the_chunk = max_files_to_read_for_this_dataset
                # determine file sorting policy
                if tmp_dataset_spec.isSeqNumber():
                    order_by_policy = "fileID"
                elif not tmp_dataset_spec.isMaster() and task_spec.reuseSecOnDemand() and not input_chunk.isMerging:
                    order_by_policy = "fileID"
                elif task_spec.respectLumiblock() or task_spec.orderByLB():
                    order_by_policy = "lumiBlockNr,lfn"
                elif not task_spec.useLoadXML():
                    order_by_policy = "fileID"
                else:
                    order_by_policy = "boundaryID"
                # repeat file reading to duplicate secondly file records if necessary
                tmp_num_already_read_files = 0
                tmp_num_waiting_files = 0
                for i_duplication_cycle in range(5000):  # avoid infinite loop just in case
                    tmp_log.debug(
                        f"jediTaskID={jedi_task_id} to read {num_files_to_read_for_the_chunk} files from datasetID={dataset_id} in attmpt={i_duplication_cycle + 1} "
                        f"with ramCount={input_chunk.ramCount} orderBy={order_by_policy} isSEQ={tmp_dataset_spec.isSeqNumber()} "
                        f"same_master={to_be_used_with_same_master}"
                    )
                    var_map = {}
                    var_map[":datasetID"] = dataset_id
                    var_map[":jediTaskID"] = jedi_task_id
                    if not tmp_dataset_spec.toKeepTrack():
                        if not simulation_with_file_stat:
                            var_map[":status"] = "ready"
                        if primary_dataset_id not in ds_with_fake_co_jumbo:
                            self.cur.execute(
                                sql_read_files_ignore_ram.format(order_by_policy, num_files_to_read_for_the_chunk - tmp_num_already_read_files) + comment,
                                var_map,
                            )
                        else:
                            self.cur.execute(
                                sql_read_files_co_jumbo_ignore_ram.format(order_by_policy, num_files_to_read_for_the_chunk - tmp_num_already_read_files)
                                + comment,
                                var_map,
                            )
                    else:
                        if not simulation_with_file_stat:
                            if use_jumbo == JediTaskSpec.enum_useJumbo["lack"] and orig_n_files_unprocessed == 0:
                                var_map[":status"] = "running"
                            else:
                                var_map[":status"] = "ready"
                            if input_chunk.ramCount not in (None, 0):
                                var_map[":ramCount"] = input_chunk.ramCount
                        # safety margin to read enough sequential numbers, which is required since sequential numbers can become
                        # ready after the cycle reading master files is done
                        if tmp_dataset_spec.isSeqNumber() and to_be_used_with_same_master and num_files_to_read_for_the_chunk > tmp_num_already_read_files:
                            if task_spec.inputPreStaging():
                                # use larger margin for data carousel since all sequence numbers are ready even if master files are not yet staged-in
                                safety_margin = 200000
                            else:
                                safety_margin = 100
                        else:
                            safety_margin = 0
                        if input_chunk.ramCount not in (None, 0):
                            if primary_dataset_id not in ds_with_fake_co_jumbo or use_jumbo == JediTaskSpec.enum_useJumbo["lack"]:
                                self.cur.execute(
                                    sql_read_files.format(order_by_policy, num_files_to_read_for_the_chunk - tmp_num_already_read_files + safety_margin)
                                    + comment,
                                    var_map,
                                )
                            else:
                                self.cur.execute(
                                    sql_read_files_co_jumbo.format(
                                        order_by_policy, num_files_to_read_for_the_chunk - tmp_num_already_read_files + safety_margin
                                    )
                                    + comment,
                                    var_map,
                                )
                        else:  # We group inputChunk.ramCount None and 0 together
                            if primary_dataset_id not in ds_with_fake_co_jumbo or use_jumbo == JediTaskSpec.enum_useJumbo["lack"]:
                                self.cur.execute(
                                    sql_read_files_empty_ram.format(
                                        order_by_policy, num_files_to_read_for_the_chunk - tmp_num_already_read_files + safety_margin
                                    )
                                    + comment,
                                    var_map,
                                )
                            else:
                                self.cur.execute(
                                    sql_read_files_co_jumbo_empty_ram.format(
                                        order_by_policy, num_files_to_read_for_the_chunk - tmp_num_already_read_files + safety_margin
                                    )
                                    + comment,
                                    var_map,
                                )

                    # read files
                    tmp_res_list = self.cur.fetchall()
                    file_spec_list = []
                    file_spec_map_with_panda_id = {}
                    file_spec_list_with_no_panda_id = []
                    file_spec_list_reserved = []
                    n_files_proper_panda_id = 0
                    n_files_null_panda_id = 0
                    n_files_inconsistent_panda_id = 0
                    for res_file in tmp_res_list:
                        # make FileSpec
                        tmp_file_spec = JediFileSpec()
                        tmp_file_spec.pack(res_file)
                        # sort sequential numbers based on old PandaIDs
                        if tmp_dataset_spec.isSeqNumber() and to_be_used_with_same_master:
                            if tmp_file_spec.PandaID is not None:
                                if tmp_file_spec.PandaID in panda_ids_used_by_master:
                                    file_spec_map_with_panda_id[tmp_file_spec.PandaID] = tmp_file_spec
                                else:
                                    # reserve the sequential number
                                    # as it may be used
                                    # when primary files don't have enough sequential numbers
                                    file_spec_list_reserved.append(tmp_file_spec)
                            else:
                                file_spec_list_with_no_panda_id.append(tmp_file_spec)
                        else:
                            file_spec_list.append(tmp_file_spec)
                            n_files_proper_panda_id += 1
                    # sort sequential numbers consistently with master's PandaIDs
                    if tmp_dataset_spec.isSeqNumber() and to_be_used_with_same_master:
                        used_panda_ids = set()
                        for tmp_panda_id in panda_ids_used_by_master_list:
                            if tmp_panda_id is not None and tmp_panda_id in used_panda_ids:
                                continue
                            if tmp_panda_id is not None and tmp_panda_id in file_spec_map_with_panda_id:
                                file_spec_list.append(file_spec_map_with_panda_id[tmp_panda_id])
                                n_files_proper_panda_id += 1
                            else:
                                # take sequential numbers which are not used by master
                                if file_spec_list_with_no_panda_id:
                                    file_spec_list.append(file_spec_list_with_no_panda_id.pop(0))
                                    n_files_null_panda_id += 1
                                elif file_spec_list_reserved:
                                    file_spec_list.append(file_spec_list_reserved.pop(0))
                                    n_files_inconsistent_panda_id += 1
                            # to ignore duplicated master's PandaIDs
                            if tmp_panda_id is not None:
                                used_panda_ids.add(tmp_panda_id)

                    tmp_log.debug(
                        f"jediTaskID={jedi_task_id} datasetID={dataset_id} old PandaID: proper={n_files_proper_panda_id} "
                        f"null={n_files_null_panda_id} inconsistent={n_files_inconsistent_panda_id}"
                    )

                    # update file counts
                    for tmp_file_spec in file_spec_list:
                        # lock files
                        if not is_dry_run and tmp_dataset_spec.toKeepTrack():
                            var_map = {}
                            var_map[":jediTaskID"] = tmp_file_spec.jediTaskID
                            var_map[":datasetID"] = tmp_file_spec.datasetID
                            var_map[":fileID"] = tmp_file_spec.fileID
                            var_map[":nStatus"] = "picked"
                            var_map[":oStatus"] = "ready"
                            self.cur.execute(sql_update_file_status + comment, var_map)
                            n_file_row = self.cur.rowcount
                            if n_file_row != 1 and not (use_jumbo == JediTaskSpec.enum_useJumbo["lack"] and orig_n_files_unprocessed == 0):
                                tmp_log.debug(f"skip fileID={tmp_file_spec.fileID} already used by another")
                                continue
                        # add to InputChunk
                        tmp_dataset_spec.addFile(tmp_file_spec)
                        total_already_read_files_map[dataset_id] += 1
                        tmp_num_already_read_files += 1
                        total_events_map[dataset_id].append(tmp_file_spec.getEffectiveNumEvents())
                        if tmp_file_spec.is_waiting == "Y":
                            tmp_num_waiting_files += 1

                    # escape the duplication look since it is not requested
                    if (
                        not task_spec.reuseSecOnDemand()
                        or tmp_dataset_spec.isMaster()
                        or task_spec.useLoadXML()
                        or tmp_dataset_spec.isNoSplit()
                        or tmp_dataset_spec.toMerge()
                        or input_chunk.ramCount not in (None, 0)
                    ):
                        break
                    # escape since enough files were read
                    if tmp_num_already_read_files >= num_files_to_read_for_the_chunk and tmp_dataset_spec.getEventRatio() is None:
                        break
                    # check if enough events were read
                    total_secondary_events = 0
                    index_secondary_files = 0
                    secondary_has_enough_events = False
                    if tmp_dataset_spec.getEventRatio() is not None:
                        secondary_has_enough_events = True
                        for n_events_in_a_master_file in total_events_map[input_chunk.masterDataset.datasetID]:
                            target_n_events = n_events_in_a_master_file * tmp_dataset_spec.getEventRatio()
                            target_n_events = int(math.ceil(target_n_events))
                            if target_n_events <= 0:
                                target_n_events = 1
                            # count number of secondary events per master file
                            tmp_n_secondary_events = 0
                            for tmp_n in total_events_map[dataset_id][index_secondary_files:]:
                                tmp_n_secondary_events += tmp_n
                                index_secondary_files += 1
                                if tmp_n_secondary_events >= target_n_events:
                                    total_secondary_events += target_n_events
                                    break
                            if tmp_n_secondary_events < target_n_events:
                                secondary_has_enough_events = False
                                break
                        if not secondary_has_enough_events:
                            # read more files without making duplication
                            if tmp_num_already_read_files >= num_files_to_read_for_the_chunk:
                                num_files_to_read_for_the_chunk += max_secondary_files_to_read_with_event_ratio
                                continue
                    if secondary_has_enough_events:
                        break

                    # duplicate files on demand
                    tmp_str = f"jediTaskID={jedi_task_id} try to increase files for datasetID={tmp_dataset_spec.datasetID} "
                    tmp_str += f"since only {tmp_num_already_read_files}/{num_files_to_read_for_the_chunk} files were read "
                    if tmp_dataset_spec.getEventRatio() is not None:
                        tmp_str += "or {0} events is less than {1}*{2} ".format(
                            total_secondary_events, tmp_dataset_spec.getEventRatio(), sum(total_events_map[input_chunk.masterDataset.datasetID])
                        )
                    tmp_log.debug(tmp_str)
                    if not tmp_dataset_spec.isSeqNumber():
                        n_new_rec = get_task_utils_module(self).duplicateFilesForReuse_JEDI(tmp_dataset_spec)
                        tmp_log.debug(f"jediTaskID={jedi_task_id} {n_new_rec} files were duplicated")
                    else:
                        n_new_rec = get_task_utils_module(self).increaseSeqNumber_JEDI(
                            tmp_dataset_spec, num_files_to_read_for_the_chunk - tmp_num_already_read_files
                        )
                        tmp_log.debug(f"jediTaskID={jedi_task_id} {n_new_rec} seq nums were added")
                    if n_new_rec == 0:
                        break

                # collect old PandaIDs of master files to check with PandaIDs of sequential numbers later
                if tmp_dataset_spec.isMaster() and to_be_used_with_same_master:
                    # sort by PandaID and move PandaID=None to the end, to avoid
                    #  * master: 1 1 None None 2 2 -> seq: 1 None None
                    #  * master: 1 2 1 2 -> seq: 1 1
                    #  when getting corresponding sequence numbers for nFilesPeroJob=2
                    tmp_dataset_spec.sort_files_by_panda_ids()
                    panda_ids_used_by_master_list = [f.PandaID for f in tmp_dataset_spec.Files]
                    panda_ids_used_by_master = set(panda_ids_used_by_master_list)

                if tmp_dataset_spec.isMaster() and tmp_num_already_read_files == 0:
                    input_chunk.isEmpty = True

                if total_already_read_files_map[dataset_id] == 0:
                    # no input files
                    if not read_min_files or not tmp_dataset_spec.isPseudo():
                        tmp_log.debug(f"jediTaskID={jedi_task_id} datasetID={dataset_id} has no files to be processed")
                        break
                elif (
                    not is_dry_run
                    and tmp_dataset_spec.toKeepTrack()
                    and tmp_num_already_read_files != 0
                    and not (use_jumbo == JediTaskSpec.enum_useJumbo["lack"] and orig_n_files_unprocessed == 0)
                ):
                    # update file counts in dataset
                    n_files_used = tmp_dataset_spec.nFilesUsed + total_already_read_files_map[dataset_id]
                    tmp_dataset_spec.nFilesUsed = n_files_used
                    var_map = {}
                    var_map[":jediTaskID"] = jedi_task_id
                    var_map[":datasetID"] = dataset_id
                    var_map[":nFilesUsed"] = n_files_used
                    self.cur.execute(sql_update_file_stat + comment, var_map)
                tmp_log.debug(
                    "jediTaskID={2} datasetID={0} has {1} files to be processed for ramCount={3}".format(
                        dataset_id, tmp_num_already_read_files, jedi_task_id, input_chunk.ramCount
                    )
                )
                # set flag if it is a block read
                if tmp_dataset_spec.isMaster():
                    if read_block and total_already_read_files_map[dataset_id] == max_files_to_read_for_this_dataset:
                        input_chunk.readBlock = True
                    else:
                        input_chunk.readBlock = False
        return input_chunk_list, typical_num_files_per_job

    # get tasks to be processed
    def getTasksToBeProcessed_JEDI(
        self,
        pid: str,
        vo: str,
        workQueue: WorkQueue,
        prodSourceLabel: str,
        cloudName: str,
        nTasks: int = 50,
        nFiles: int = 100,
        isPeeking: bool = False,
        simTasks: list | None = None,
        minPriority: int | None = None,
        maxNumJobs: int | None = None,
        typicalNumFilesMap: dict | None = None,
        fullSimulation: bool | None = False,
        simDatasets: list | None = None,
        mergeUnThrottled: bool | None = None,
        readMinFiles: bool = False,
        numNewTaskWithJumbo: int = 0,
        resource_name: str | None = None,
        ignore_lock: bool = False,
        target_tasks: list | None = None,
        is_dry_run: bool = False,
    ) -> list | int | None:
        """
        Get tasks to generate jobs.
        This method is also used for task brokerage and job throttler.

        :param pid: Process ID.
        :param vo: Virtual organization.
        :param workQueue: Work queue object.
        :param prodSourceLabel: Production source label.
        :param cloudName: Cloud name.
        :param nTasks: Max number of tasks to read.
        :param nFiles: Max number of files per task.
        :param isPeeking: Whether for job throttler to peek at the highest priority among waiting tasks without any interventions.
        :param simTasks: The list of tasks to read for dry run without locking them.
        :param minPriority: Minimum priority of tasks to read.
        :param maxNumJobs: Maximum number of jobs to generate.
        :param typicalNumFilesMap: Map of the typical number of input files per work type.
        :param fullSimulation: Whether to read files by ignoring file counts of the dataset for dry run.
        :param simDatasets: The list of datasets to read for dry run without locking them.
        :param mergeUnThrottled: Whether to read tasks with unprocessed unmerged inputs even if enough tasks have been already read.
        :param readMinFiles: Whether to read minimum files for task brokerage.
        :param numNewTaskWithJumbo: The Number of new tasks with jumbo jobs while the total number of running tasks with jumbo jobs is limited.
        :param resource_name: Resource name.
        :param ignore_lock: Whether to ignore lock when reading tasks.
        :param target_tasks: The list of tasks to read for message-driven processing or dry run.
        :param is_dry_run: Whether to read tasks for dry run.

        :return: List of tasks to generate jobs, the highest priority among waiting tasks if isPeeking is True, or None in case of failure.
        """
        comment = " /* JediDBProxy.getTasksToBeProcessed_JEDI */"
        time_now = naive_utcnow().strftime("%Y/%m/%d %H:%M:%S")
        if simTasks is not None:
            target_tasks = simTasks
            is_dry_run = True
        if target_tasks:
            tmp_log = self.create_tagged_logger(comment, f"jediTasks={str(target_tasks)}")
        elif workQueue is None:
            tmp_log = self.create_tagged_logger(comment, f"vo={vo} queue={None} cloud={cloudName} pid={pid} {time_now}")
        else:
            tmp_log = self.create_tagged_logger(comment, f"vo={vo} queue={workQueue.queue_name} cloud={cloudName} pid={pid} {time_now}")
        tmp_log.debug(f"start label={prodSourceLabel} nTasks={nTasks} nFiles={nFiles} minPriority={minPriority}")
        tmp_log.debug(f"max_num_jobs={maxNumJobs} typicalNumFilesMap={str(typicalNumFilesMap)}")
        tmp_log.debug(f"is_dry_run={is_dry_run} mergeUnThrottled={str(mergeUnThrottled)} readMinFiles={readMinFiles}")
        tmp_log.debug(f"numNewTaskWithJumbo={numNewTaskWithJumbo}")

        mem_start = CoreUtils.getMemoryUsage()
        tmp_log.debug(f"memUsage start {mem_start} MB pid={os.getpid()}")
        # return value for failure
        failed_return = None
        # set max number of jobs if undefined
        if maxNumJobs is None:
            tmp_log.debug(f"set max_num_jobs={maxNumJobs} since undefined ")
        super_high_prio_task_ratio = self.getConfigValue("dbproxy", "SUPER_HIGH_PRIO_TASK_RATIO", "jedi")
        if super_high_prio_task_ratio is None:
            super_high_prio_task_ratio = 30
        # time limit to avoid duplication
        if hasattr(self.jedi_config.jobgen, "lockInterval"):
            lock_interval = self.jedi_config.jobgen.lockInterval
        else:
            lock_interval = 10
        time_limit = naive_utcnow() - datetime.timedelta(minutes=lock_interval)
        try:
            # attribute for GROUP BY
            if workQueue is not None:
                attr_name_for_group_by = self.getConfigValue("jobgen", f"GROUPBYATTR_{workQueue.queue_name}", "jedi")
            else:
                attr_name_for_group_by = None
            if attr_name_for_group_by is None or attr_name_for_group_by not in JediTaskSpec.attributes:
                attr_name_for_group_by = "userName"
                set_group_by_attr = False
            else:
                set_group_by_attr = True
            # get tasks and datasets with unprocessed inputs
            res_list = self._get_tasks_datasets_with_unprocessed_inputs(
                comment,
                tmp_log,
                vo,
                workQueue,
                prodSourceLabel,
                cloudName,
                attr_name_for_group_by,
                time_limit,
                minPriority,
                fullSimulation,
                simDatasets,
                mergeUnThrottled,
                resource_name,
                target_tasks,
            )

            # no tasks
            if res_list == [] and isPeeking:
                return 0

            # make dictionaries with tasks and datasets
            tmp_ret = self._make_dicts_tasks_datasets_with_unprocessed_inputs(
                res_list, tmp_log, workQueue, isPeeking, super_high_prio_task_ratio, set_group_by_attr
            )
            if isPeeking:
                return tmp_ret

            task_dataset_map, task_status_map, jedi_task_id_list, task_prio_map, task_with_jumbo_map, task_group_by_attr_map = tmp_ret

            # loop over all tasks to make return
            i_tasks = 0
            locked_tasks_list = []
            locked_tasks_by_another_list = []
            memory_exceed = False
            return_map = {}
            for tmpIdxTask, jediTaskID in enumerate(jedi_task_id_list):
                # process only merging if enough jobs are already generated
                dataset_with_fake_co_jumbo = set()
                contain_merging = False
                if (maxNumJobs is not None and maxNumJobs <= 0) or task_with_jumbo_map[jediTaskID] == JediTaskSpec.enum_useJumbo["pending"] or mergeUnThrottled:
                    for dataset_id, n_files_unprocessed, dataset_type, total_input_files, total_input_events, n_files_waiting, use_jumbo in task_dataset_map[
                        jediTaskID
                    ]:
                        if dataset_type in JediDatasetSpec.getMergeProcessTypes():
                            # internal merging
                            contain_merging = True
                            if use_jumbo is None or use_jumbo == JediTaskSpec.enum_useJumbo["disabled"]:
                                break
                        elif (
                            use_jumbo is None
                            or use_jumbo == JediTaskSpec.enum_useJumbo["disabled"]
                            or (n_files_unprocessed - n_files_waiting <= 0 and use_jumbo != JediTaskSpec.enum_useJumbo["lack"])
                        ):
                            # no jumbo or no more co-jumbo
                            pass
                        elif use_jumbo in [
                            JediTaskSpec.enum_useJumbo["running"],
                            JediTaskSpec.enum_useJumbo["pending"],
                            JediTaskSpec.enum_useJumbo["lack"],
                        ] or (use_jumbo == JediTaskSpec.enum_useJumbo["waiting"] and numNewTaskWithJumbo > 0):
                            # jumbo with fake co-jumbo
                            dataset_with_fake_co_jumbo.add(dataset_id)
                    if not contain_merging and len(dataset_with_fake_co_jumbo) == 0:
                        tmp_log.debug(
                            f"skipping no pmerge or jumbo jediTaskID={jediTaskID} {tmpIdxTask}/{len(jedi_task_id_list)}/{i_tasks} prio={task_prio_map[jediTaskID]}"
                        )

                        continue
                tmp_log.debug(
                    f"getting jediTaskID={jediTaskID} {tmpIdxTask}/{len(jedi_task_id_list)}/{i_tasks} prio={task_prio_map[jediTaskID]} by={task_group_by_attr_map[jediTaskID]}"
                )
                # locked by another
                if jediTaskID in locked_tasks_by_another_list:
                    tmp_log.debug(f"skip locked by another jediTaskID={jediTaskID}")
                    continue
                # begin transaction
                self.conn.begin()
                # read task
                to_skip_task, orig_task_spec, locked_tasks_list, locked_tasks_by_another_list = self._read_task_with_unprocessed_inputs(
                    jediTaskID,
                    task_status_map,
                    locked_tasks_list,
                    tmp_log,
                    comment,
                    pid,
                    locked_tasks_by_another_list,
                    is_dry_run,
                    ignore_lock,
                    task_dataset_map,
                    contain_merging,
                    dataset_with_fake_co_jumbo,
                    time_limit,
                    target_tasks,
                )
                if to_skip_task:
                    continue
                # count the number of files for avalanche, check username, and get the number of samples for HPO tasks
                to_skip_task, num_avalanche, num_hpo_samples = self._check_task_with_unprocessed_inputs(
                    jediTaskID, comment, tmp_log, orig_task_spec, is_dry_run
                )
                if to_skip_task:
                    continue
                # read datasets
                if not to_skip_task:
                    i_ds_per_task = 0
                    n_ds_per_task = 10
                    task_with_new_jumbo = False
                    for dataset_id, n_files_unprocessed, dataset_type, total_input_files, total_input_events, n_files_waiting, use_jumbo in task_dataset_map[
                        jediTaskID
                    ]:
                        primary_dataset_id = dataset_id
                        dataset_id_list = [dataset_id]
                        task_spec = copy.copy(orig_task_spec)
                        orig_n_files_unprocessed = n_files_unprocessed
                        # reduce NumInputFiles for HPO to avoid redundant workers
                        if num_hpo_samples is not None:
                            if dataset_id not in num_hpo_samples or num_hpo_samples[dataset_id] <= 0:
                                continue
                            if n_files_unprocessed > num_hpo_samples[dataset_id]:
                                n_files_unprocessed = num_hpo_samples[dataset_id]
                        # See if there are different memory requirements that need to be mapped to different chuncks
                        to_skip_dataset, memory_requirements = self._get_memory_requirements_unprocessed_inputs(
                            comment,
                            tmp_log,
                            jediTaskID,
                            dataset_id,
                            dataset_type,
                            primary_dataset_id,
                            fullSimulation,
                            orig_n_files_unprocessed,
                            use_jumbo,
                            dataset_with_fake_co_jumbo,
                        )

                        # make InputChunks by ram count
                        input_chunk_list = []
                        if not to_skip_dataset:
                            if not to_skip_dataset:
                                for tmp_mem in memory_requirements:
                                    input_chunk_list.append(InputChunk(task_spec, ramCount=tmp_mem))
                                # merging
                                if dataset_type in JediDatasetSpec.getMergeProcessTypes():
                                    for input_chunk in input_chunk_list:
                                        input_chunk.isMerging = True
                                elif (
                                    use_jumbo in [JediTaskSpec.enum_useJumbo["running"], JediTaskSpec.enum_useJumbo["pending"]]
                                    or (use_jumbo == JediTaskSpec.enum_useJumbo["waiting"] and numNewTaskWithJumbo > 0)
                                ) and n_files_unprocessed > n_files_waiting:
                                    # set jumbo flag only to the first chunk
                                    if dataset_id in dataset_with_fake_co_jumbo:
                                        if orig_task_spec.useScout() and not orig_task_spec.isPostScout():
                                            tmp_log.debug(f"skip jediTaskID={jediTaskID} datasetID={primary_dataset_id} due to jumbo for scouting")
                                            continue
                                        input_chunk_list[0].useJumbo = "fake"
                                    else:
                                        input_chunk_list[0].useJumbo = "full"
                                    # overwrite tmpNumFiles
                                    n_files_unprocessed -= n_files_waiting
                                    if use_jumbo == JediTaskSpec.enum_useJumbo["waiting"]:
                                        task_with_new_jumbo = True
                                elif use_jumbo == JediTaskSpec.enum_useJumbo["lack"]:
                                    input_chunk_list[0].useJumbo = "only"
                                    n_files_unprocessed = 1
                                else:
                                    # only process merging or jumbo if enough jobs are already generated
                                    if maxNumJobs is not None and maxNumJobs <= 0:
                                        tmp_log.debug(f"skip jediTaskID={jediTaskID} datasetID={primary_dataset_id} due to non-merge + enough jobs")
                                        continue

                        # read datasets
                        if not to_skip_dataset:
                            to_skip_dataset, dataset_id_list, input_chunk_list = self._get_datasets_with_unprocessed_inputs(
                                comment,
                                tmp_log,
                                input_chunk_list,
                                task_spec,
                                jediTaskID,
                                dataset_id,
                                dataset_type,
                                dataset_id_list,
                                is_dry_run,
                                fullSimulation,
                                num_avalanche,
                                readMinFiles,
                            )

                        # read job params and files
                        if not to_skip_dataset:
                            # sql to read template
                            sql_read_job_param_template = (
                                f"SELECT jobParamsTemplate FROM {panda_config.schemaJEDI}.JEDI_JobParams_Template WHERE jediTaskID=:jediTaskID "
                            )
                            # read template to generate job parameters
                            var_map = {":jediTaskID": jediTaskID}
                            self.cur.execute(sql_read_job_param_template + comment, var_map)
                            for (clobJobP,) in self.cur:
                                if clobJobP is not None:
                                    task_spec.jobParamsTemplate = clobJobP
                                break
                            # read files
                            input_chunk_list, typical_num_files_per_job = self._read_unprocessed_inputs(
                                comment,
                                tmp_log,
                                input_chunk_list,
                                task_spec,
                                jediTaskID,
                                dataset_id_list,
                                total_input_files,
                                total_input_events,
                                typicalNumFilesMap,
                                maxNumJobs,
                                is_dry_run,
                                readMinFiles,
                                nFiles,
                                fullSimulation,
                                n_files_unprocessed,
                                primary_dataset_id,
                                use_jumbo,
                                orig_n_files_unprocessed,
                                dataset_with_fake_co_jumbo,
                            )

                            # add to return
                            if jediTaskID not in return_map:
                                return_map[jediTaskID] = []
                                i_tasks += 1
                            for input_chunk in input_chunk_list:
                                if not input_chunk.isEmpty:
                                    return_map[jediTaskID].append((task_spec, cloudName, input_chunk))
                                    i_ds_per_task += 1
                                # reduce the number of jobs
                                if maxNumJobs is not None and not input_chunk.isMerging:
                                    maxNumJobs -= int(math.ceil(float(len(input_chunk.masterDataset.Files)) / float(typical_num_files_per_job)))
                            if i_ds_per_task > n_ds_per_task:
                                tmp_log.debug(f"escape due to too many datasets to process")
                                break

                        if maxNumJobs is not None and maxNumJobs <= 0:
                            pass
                        # memory check
                        try:
                            mem_limit = 1 * 1024
                            mem_now = CoreUtils.getMemoryUsage()
                            tmp_log.debug(f"memUsage now {mem_now} MB pid={os.getpid()}")
                            if mem_now - mem_start > mem_limit:
                                tmp_log.warning(f"memory limit exceeds {mem_now}-{mem_start} > {mem_limit} MB : jediTaskID={jediTaskID}")
                                memory_exceed = True
                                break
                        except Exception:
                            pass
                    if task_with_new_jumbo:
                        numNewTaskWithJumbo -= 1
                if not to_skip_task:
                    # commit
                    if not self._commit():
                        raise RuntimeError("Commit error")
                else:
                    tmp_log.debug(f"rollback for jediTaskID={jediTaskID}")
                    # roll back
                    self._rollback()
                # enough tasks
                if i_tasks >= nTasks:
                    break
                # already read enough files to generate jobs
                if maxNumJobs is not None and maxNumJobs <= 0:
                    pass
                # memory limit exceeds
                if memory_exceed:
                    break
            tmp_log.debug(f"returning {i_tasks} tasks")
            # change map to list
            return_list = []
            for tmpJediTaskID, tmpTaskDsList in return_map.items():
                return_list.append((tmpJediTaskID, tmpTaskDsList))
            tmp_log.debug(f"memUsage end {CoreUtils.getMemoryUsage()} MB pid={os.getpid()}")
            return return_list
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return failed_return

    # set scout job data to tasks
    def setScoutJobDataToTasks_JEDI(self, vo, prodSourceLabel, site_mapper):
        comment = " /* JediDBProxy.setScoutJobDataToTasks_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"vo={vo} label={prodSourceLabel}")
        tmpLog.debug("start")
        try:
            # sql to get tasks to set scout job data
            varMap = {}
            varMap[":status"] = "running"
            varMap[":minJobs"] = 5
            varMap[":timeLimit"] = naive_utcnow() - datetime.timedelta(hours=24)
            sqlSCF = "SELECT tabT.jediTaskID "
            sqlSCF += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_AUX_Status_MinTaskID tabA,{1}.T_TASK tabD ".format(panda_config.schemaJEDI, panda_config.schemaDEFT)
            sqlSCF += "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID "
            sqlSCF += "AND tabT.jediTaskID=tabD.taskID AND tabT.modificationTime>:timeLimit "
            sqlSCF += "AND tabT.status=:status AND tabT.walltimeUnit IS NULL "
            sqlSCF += "AND tabD.total_done_jobs>=:minJobs "
            if vo not in [None, "any"]:
                varMap[":vo"] = vo
                sqlSCF += "AND tabT.vo=:vo "
            if prodSourceLabel not in [None, "any"]:
                varMap[":prodSourceLabel"] = prodSourceLabel
                sqlSCF += "AND tabT.prodSourceLabel=:prodSourceLabel "
            # sql to update task status
            sqlTU = f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks "
            sqlTU += "SET status=:newStatus,modificationTime=CURRENT_DATE,"
            sqlTU += "errorDialog=:errorDialog,stateChangeTime=CURRENT_DATE "
            sqlTU += "WHERE jediTaskID=:jediTaskID AND status=:oldStatus "
            # begin transaction
            self.conn.begin()
            # get tasks
            tmpLog.debug(sqlSCF + comment + str(varMap))
            self.cur.execute(sqlSCF + comment, varMap)
            resList = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            nTasks = 0
            for (jediTaskID,) in resList:
                # get task
                tmpStat, taskSpec = get_task_utils_module(self).getTaskWithID_JEDI(jediTaskID, False)
                if tmpStat:
                    tmpLog.debug(f"set jediTaskID={jediTaskID}")
                    get_task_utils_module(self).setScoutJobData_JEDI(taskSpec, True, True, site_mapper)
                    # update exhausted task status
                    if taskSpec.status == "exhausted":
                        # begin transaction
                        self.conn.begin()
                        # update task status
                        varMap = {}
                        varMap[":jediTaskID"] = taskSpec.jediTaskID
                        varMap[":newStatus"] = taskSpec.status
                        varMap[":oldStatus"] = "running"
                        varMap[":errorDialog"] = taskSpec.errorDialog
                        self.cur.execute(sqlTU + comment, varMap)
                        nRow = self.cur.rowcount
                        # update DEFT task
                        if nRow > 0:
                            self.setDeftStatus_JEDI(taskSpec.jediTaskID, taskSpec.status)
                            self.setSuperStatus_JEDI(taskSpec.jediTaskID, taskSpec.status)
                            self.record_task_status_change(taskSpec.jediTaskID)
                            self.push_task_status_message(taskSpec, taskSpec.jediTaskID, taskSpec.status)
                        # commit
                        if not self._commit():
                            raise RuntimeError("Commit error")
                        tmpLog.debug(f"set status={taskSpec.status} to jediTaskID={taskSpec.jediTaskID} with {nRow} since {taskSpec.errorDialog}")
                    nTasks += 1
            # return
            tmpLog.debug(f"done with {nTasks} tasks")
            return True
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return None

    # prepare tasks to be finished
    def prepareTasksToBeFinished_JEDI(self, vo, prodSourceLabel, nTasks=50, simTasks=None, pid="lock", noBroken=False, site_mapper=None):
        comment = " /* JediDBProxy.prepareTasksToBeFinished_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"vo={vo} label={prodSourceLabel}")
        tmpLog.debug("start")
        # return value for failure
        failedRet = None
        # return list of taskids
        ret_list = []
        try:
            # sql to get tasks/datasets
            if simTasks is None:
                varMap = {}
                varMap[":taskstatus1"] = "running"
                varMap[":taskstatus2"] = "scouting"
                varMap[":taskstatus3"] = "merging"
                varMap[":taskstatus4"] = "preprocessing"
                varMap[":taskstatus5"] = "ready"
                varMap[":taskstatus6"] = "throttled"
                varMap[":dsEndStatus1"] = "finished"
                varMap[":dsEndStatus2"] = "done"
                varMap[":dsEndStatus3"] = "failed"
                varMap[":dsEndStatus4"] = "removed"
                if vo is not None:
                    varMap[":vo"] = vo
                if prodSourceLabel is not None:
                    varMap[":prodSourceLabel"] = prodSourceLabel
                sql = "SELECT tabT.jediTaskID,tabT.status "
                sql += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_AUX_Status_MinTaskID tabA ".format(panda_config.schemaJEDI)
                sql += "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID "
                sql += "AND tabT.status IN (:taskstatus1,:taskstatus2,:taskstatus3,:taskstatus4,:taskstatus5,:taskstatus6) "
                if vo is not None:
                    sql += "AND tabT.vo=:vo "
                if prodSourceLabel is not None:
                    sql += "AND prodSourceLabel=:prodSourceLabel "
                sql += "AND tabT.lockedBy IS NULL AND NOT EXISTS "
                sql += f"(SELECT 1 FROM {panda_config.schemaJEDI}.JEDI_Datasets tabD "
                sql += "WHERE tabD.jediTaskID=tabT.jediTaskID AND masterID IS NULL "
                sql += f"AND type IN ({PROCESS_TYPES_var_str}) "
                varMap.update(PROCESS_TYPES_var_map)
                sql += "AND NOT status IN (:dsEndStatus1,:dsEndStatus2,:dsEndStatus3,:dsEndStatus4) AND ("
                sql += "nFilesToBeUsed>nFilesFinished+nFilesFailed "
                sql += "OR (nFilesUsed=0 AND nFilesToBeUsed IS NOT NULL AND nFilesToBeUsed>0) "
                sql += "OR (nFilesToBeUsed IS NOT NULL AND nFilesToBeUsed>nFilesFinished+nFilesFailed)) "
                sql += f") AND rownum<={nTasks}"
            else:
                varMap = {}
                sql = "SELECT tabT.jediTaskID,tabT.status "
                sql += f"FROM {panda_config.schemaJEDI}.JEDI_Tasks tabT "
                taskid_var_names_str, taskid_var_map = get_sql_IN_bind_variables(simTasks, prefix=":jediTaskID")
                sql += f"WHERE jediTaskID IN ({taskid_var_names_str}) "
                varMap.update(taskid_var_map)
            # begin transaction
            self.conn.begin()
            self.cur.arraysize = 10000
            # get tasks
            tmpLog.debug(sql + comment + str(varMap))
            self.cur.execute(sql + comment, varMap)
            resList = self.cur.fetchall()
            # make list
            jediTaskIDstatusMap = {}
            set_scout_data_only = set()
            for jediTaskID, taskStatus in resList:
                jediTaskIDstatusMap[jediTaskID] = taskStatus
            # tasks to force avalanche
            toAvalancheTasks = set()
            # get tasks for early avalanche
            if simTasks is None:
                minSuccessScouts = 5
                timeToCheck = naive_utcnow() - datetime.timedelta(minutes=10)
                varMap = {}
                varMap[":scouting"] = "scouting"
                varMap[":running"] = "running"
                if prodSourceLabel:
                    varMap[":prodSourceLabel"] = prodSourceLabel
                else:
                    varMap[":prodSourceLabel"] = "managed"
                varMap[":timeLimit"] = timeToCheck
                if vo is not None:
                    varMap[":vo"] = vo
                sqlEA = "SELECT jediTaskID,t_status,walltimeUnit, COUNT(*),SUM(CASE WHEN f_status='finished' THEN 1 ELSE 0 END) FROM "
                sqlEA += "(SELECT DISTINCT tabT.jediTaskID,tabT.status as t_status,tabT.walltimeUnit,tabF.PandaID,tabF.status as f_status "
                sqlEA += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_AUX_Status_MinTaskID tabA,{0}.JEDI_Datasets tabD,{0}.JEDI_Dataset_Contents tabF ".format(
                    panda_config.schemaJEDI
                )
                sqlEA += "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID "
                sqlEA += "AND tabT.jediTaskID=tabD.jediTaskID "
                sqlEA += "AND tabD.jediTaskID=tabF.jediTaskID AND tabD.datasetID=tabF.datasetID "
                sqlEA += "AND (tabT.status=:scouting OR (tabT.status=:running AND tabT.walltimeUnit IS NULL)) "
                sqlEA += "AND tabT.prodSourceLabel=:prodSourceLabel "
                sqlEA += "AND (tabT.assessmentTime IS NULL OR tabT.assessmentTime<:timeLimit) "
                if vo is not None:
                    sqlEA += "AND tabT.vo=:vo "
                sqlEA += "AND tabT.lockedBy IS NULL "
                sqlEA += "AND tabD.masterID IS NULL AND tabD.nFilesToBeUsed>0 "
                sqlEA += f"AND tabD.type IN ({INPUT_TYPES_var_str}) "
                varMap.update(INPUT_TYPES_var_map)
                sqlEA += "AND tabF.PandaID IS NOT NULL "
                sqlEA += ") "
                sqlEA += "GROUP BY jediTaskID,t_status,walltimeUnit "
                # get tasks
                tmpLog.debug(sqlEA + comment + str(varMap))
                self.cur.execute(sqlEA + comment, varMap)
                resList = self.cur.fetchall()
                # update assessmentTime
                sqlLK = f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks SET assessmentTime=CURRENT_DATE "
                sqlLK += "WHERE jediTaskID=:jediTaskID AND (assessmentTime IS NULL OR assessmentTime<:timeLimit) "
                sqlLK += "AND (status=:scouting OR (status=:running AND walltimeUnit IS NULL)) "
                # append to list
                for jediTaskID, taskstatus, walltimeUnit, totJobs, totFinished in resList:
                    # update assessmentTime to avoid frequent check
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    varMap[":timeLimit"] = timeToCheck
                    varMap[":scouting"] = "scouting"
                    varMap[":running"] = "running"
                    self.cur.execute(sqlLK + comment, varMap)
                    nRow = self.cur.rowcount
                    if nRow and totFinished and jediTaskID not in jediTaskIDstatusMap:
                        to_trigger = False
                        if taskstatus == "scouting" and totFinished >= totJobs * minSuccessScouts / 10:
                            msg_piece = "early avalanche"
                            to_trigger = True
                        elif totFinished >= 1 and walltimeUnit is None:
                            set_scout_data_only.add(jediTaskID)
                            msg_piece = f"reset in {taskstatus}"
                            to_trigger = True
                        if to_trigger:
                            jediTaskIDstatusMap[jediTaskID] = taskstatus
                            tmpLog.debug(f"got jediTaskID={jediTaskID} {totFinished}/{totJobs} finished for {msg_piece}")

            # get tasks to force avalanche
            if simTasks is None:
                taskstatus = "scouting"
                varMap = {}
                varMap[":taskstatus"] = taskstatus
                varMap[":walltimeUnit"] = "ava"
                sqlFA = "SELECT jediTaskID "
                sqlFA += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_AUX_Status_MinTaskID tabA ".format(panda_config.schemaJEDI)
                sqlFA += "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID "
                sqlFA += "AND tabT.status=:taskstatus "
                if prodSourceLabel is not None:
                    sqlFA += "AND prodSourceLabel=:prodSourceLabel "
                    varMap[":prodSourceLabel"] = prodSourceLabel
                if vo is not None:
                    sqlFA += "AND tabT.vo=:vo "
                    varMap[":vo"] = vo
                sqlFA += "AND tabT.walltimeUnit=:walltimeUnit "
                # get tasks
                tmpLog.debug(sqlFA + comment + str(varMap))
                self.cur.execute(sqlFA + comment, varMap)
                resList = self.cur.fetchall()
                # append to list
                for (jediTaskID,) in resList:
                    if jediTaskID not in jediTaskIDstatusMap:
                        jediTaskIDstatusMap[jediTaskID] = taskstatus
                        toAvalancheTasks.add(jediTaskID)
                        tmpLog.debug(f"got jediTaskID={jediTaskID} to force avalanche")
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            jediTaskIDList = list(jediTaskIDstatusMap.keys())
            random.shuffle(jediTaskIDList)
            tmpLog.debug(f"got {len(jediTaskIDList)} tasks")
            # sql to read task
            sqlRT = f"SELECT {JediTaskSpec.columnNames()} "
            sqlRT += f"FROM {panda_config.schemaJEDI}.JEDI_Tasks "
            sqlRT += "WHERE jediTaskID=:jediTaskID AND status=:statusInDB AND lockedBy IS NULL FOR UPDATE NOWAIT "
            # sql to lock task
            sqlLK = f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks SET lockedBy=:newLockedBy "
            sqlLK += "WHERE jediTaskID=:jediTaskID AND status=:status AND lockedBy IS NULL "
            # sql to read dataset status
            sqlRD = "SELECT datasetID,status,nFiles,nFilesFinished,nFilesFailed,masterID,state "
            sqlRD += f"FROM {panda_config.schemaJEDI}.JEDI_Datasets "
            sqlRD += f"WHERE jediTaskID=:jediTaskID AND status=:status AND type IN ({PROCESS_TYPES_var_str}) "
            # sql to check if there is mutable dataset
            sqlMTC = f"SELECT COUNT(*) FROM {panda_config.schemaJEDI}.JEDI_Datasets "
            sqlMTC += "WHERE jediTaskID=:jediTaskID AND state=:state AND masterID IS NULL "
            sqlMTC += f"AND type IN ({INPUT_TYPES_var_str}) "
            # sql to update input dataset status
            sqlDIU = f"UPDATE {panda_config.schemaJEDI}.JEDI_Datasets SET status=:status,modificationTime=CURRENT_DATE "
            sqlDIU += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            # sql to update output/log dataset status
            sqlDOU = f"UPDATE {panda_config.schemaJEDI}.JEDI_Datasets SET status=:status,modificationTime=CURRENT_DATE "
            sqlDOU += "WHERE jediTaskID=:jediTaskID AND type IN (:type1,:type2) "
            # sql to update status of mutable dataset
            sqlMUT = f"UPDATE {panda_config.schemaJEDI}.JEDI_Datasets SET status=:status,modificationTime=CURRENT_DATE "
            sqlMUT += "WHERE jediTaskID=:jediTaskID AND state=:state "
            # sql to get nFilesToBeUsed of dataset
            sqlFUD = "SELECT tabD.datasetID,COUNT(*) FROM {0}.JEDI_Datasets tabD,{0}.JEDI_Dataset_Contents tabC ".format(panda_config.schemaJEDI)
            sqlFUD += "WHERE tabD.jediTaskID=tabC.jediTaskID AND tabD.datasetID=tabC.datasetID "
            sqlFUD += f"AND tabD.type IN ({INPUT_TYPES_var_str}) "
            sqlFUD += "AND tabD.jediTaskID=:jediTaskID AND tabD.masterID IS NULL "
            sqlFUD += "AND NOT tabC.status IN (:status1,:status2,:status3,:status4) "
            sqlFUD += "GROUP BY tabD.datasetID "
            # sql to update nFiles of dataset
            sqlFUU = f"UPDATE {panda_config.schemaJEDI}.JEDI_Datasets SET nFilesToBeUsed=:nFilesToBeUsed,modificationTime=CURRENT_DATE "
            sqlFUU += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            # sql to update task status
            sqlTU = f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks "
            sqlTU += "SET status=:status,modificationTime=CURRENT_DATE,lockedBy=NULL,lockedTime=NULL,"
            sqlTU += "errorDialog=:errorDialog,splitRule=:splitRule,stateChangeTime=CURRENT_DATE,oldStatus=:oldStatus "
            sqlTU += "WHERE jediTaskID=:jediTaskID "
            # sql to unlock task
            sqlTUU = f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks "
            sqlTUU += "SET lockedBy=NULL,lockedTime=NULL "
            sqlTUU += "WHERE jediTaskID=:jediTaskID AND status=:status "
            # sql to update split rule
            sqlUSL = f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks "
            sqlUSL += "SET splitRule=:splitRule WHERE jediTaskID=:jediTaskID "
            # sql to reset walltimeUnit
            sqlRWU = f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks SET walltimeUnit=NULL "
            sqlRWU += "WHERE jediTaskID=:jediTaskID AND status=:status AND walltimeUnit IS NOT NULL "
            # loop over all tasks
            iTasks = 1
            for jediTaskID in jediTaskIDList:
                taskStatus = jediTaskIDstatusMap[jediTaskID]
                tmpLog.debug(f"start {iTasks}/{len(jediTaskIDList)} jediTaskID={jediTaskID} status={taskStatus}")
                iTasks += 1
                # begin transaction
                self.conn.begin()
                # read task
                toSkip = False
                errorDialog = None
                oldStatus = None
                try:
                    # read task
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    varMap[":statusInDB"] = taskStatus
                    self.cur.execute(sqlRT + comment, varMap)
                    resRT = self.cur.fetchone()
                    # locked by another
                    if resRT is None:
                        tmpLog.debug(f"skip jediTaskID={jediTaskID} since status has changed")
                        toSkip = True
                        if not self._commit():
                            raise RuntimeError("Commit error")
                        continue
                    else:
                        taskSpec = JediTaskSpec()
                        taskSpec.pack(resRT)
                        taskSpec.lockedBy = None
                        taskSpec.lockedTime = None
                    # lock
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    varMap[":newLockedBy"] = pid
                    varMap[":status"] = taskStatus
                    self.cur.execute(sqlLK + comment, varMap)
                    nRow = self.cur.rowcount
                    if nRow != 1:
                        tmpLog.debug(f"failed to lock jediTaskID={jediTaskID}")
                        toSkip = True
                        if not self._commit():
                            raise RuntimeError("Commit error")
                        continue
                except Exception:
                    errType, errValue = sys.exc_info()[:2]
                    if self.isNoWaitException(errValue):
                        # resource busy and acquire with NOWAIT specified
                        toSkip = True
                        tmpLog.debug(f"skip locked jediTaskID={jediTaskID}")
                        if not self._commit():
                            raise RuntimeError("Commit error")
                        continue
                    else:
                        # failed with something else
                        raise errType(errValue)
                # update dataset
                if not toSkip:
                    tmpLog.debug(
                        "jediTaskID={} status={} useScout={} isPostScout={} onlyData={}".format(
                            jediTaskID, taskSpec.status, taskSpec.useScout(), taskSpec.isPostScout(), jediTaskID in set_scout_data_only
                        )
                    )
                    if (
                        taskSpec.status == "scouting"
                        or jediTaskID in set_scout_data_only
                        or (taskSpec.status == "ready" and taskSpec.useScout() and not taskSpec.isPostScout())
                    ):
                        # reset walltimeUnit
                        if jediTaskID in toAvalancheTasks:
                            varMap = {}
                            varMap[":jediTaskID"] = jediTaskID
                            varMap[":status"] = taskSpec.status
                            self.cur.execute(sqlRWU + comment, varMap)
                        # set average job data
                        if jediTaskID in set_scout_data_only:
                            use_exhausted = False
                        else:
                            use_exhausted = True
                        scoutSucceeded, mergeScoutSucceeded = get_task_utils_module(self).setScoutJobData_JEDI(taskSpec, False, use_exhausted, site_mapper)
                        if jediTaskID in set_scout_data_only:
                            toSkip = True
                            tmpLog.debug(f"done set only scout data for jediTaskID={jediTaskID} in status={taskSpec.status}")
                        else:
                            # get nFiles to be used
                            varMap = {}
                            varMap[":jediTaskID"] = jediTaskID
                            varMap[":status1"] = "pending"
                            varMap[":status2"] = "lost"
                            varMap[":status3"] = "missing"
                            varMap[":status4"] = "staging"
                            varMap.update(INPUT_TYPES_var_map)
                            self.cur.execute(sqlFUD + comment, varMap)
                            resFUD = self.cur.fetchall()
                            # update nFiles to be used
                            for datasetID, nReadyFiles in resFUD:
                                varMap = {}
                                varMap[":jediTaskID"] = jediTaskID
                                varMap[":datasetID"] = datasetID
                                varMap[":nFilesToBeUsed"] = nReadyFiles
                                tmpLog.debug(f"jediTaskID={jediTaskID} datasetID={datasetID} set nFilesToBeUsed={nReadyFiles}")
                                self.cur.execute(sqlFUU + comment, varMap)
                            # new task status
                            if scoutSucceeded or noBroken or jediTaskID in toAvalancheTasks:
                                if taskSpec.status == "exhausted":
                                    # went to exhausted since real cpuTime etc is too large
                                    newTaskStatus = "exhausted"
                                    errorDialog = taskSpec.errorDialog
                                    oldStatus = taskStatus
                                else:
                                    newTaskStatus = "scouted"
                                taskSpec.setPostScout()
                            else:
                                newTaskStatus = "tobroken"
                                if taskSpec.getScoutSuccessRate() is None:
                                    errorDialog = "no scout jobs succeeded"
                                else:
                                    errorDialog = "not enough scout jobs succeeded"
                    elif taskSpec.status in ["running", "merging", "preprocessing", "ready", "throttled"]:
                        # get input datasets
                        varMap = {}
                        varMap[":jediTaskID"] = jediTaskID
                        varMap[":status"] = "ready"
                        varMap.update(PROCESS_TYPES_var_map)
                        self.cur.execute(sqlRD + comment, varMap)
                        resRD = self.cur.fetchall()
                        varMapList = []
                        mutableFlag = False
                        preprocessedFlag = False
                        for datasetID, dsStatus, nFiles, nFilesFinished, nFilesFailed, masterID, dsState in resRD:
                            # parent could be still running
                            if dsState == "mutable" and masterID is None:
                                mutableFlag = True
                                break
                            # check if there are unprocessed files
                            if masterID is None and nFiles and nFiles > nFilesFinished + nFilesFailed:
                                tmpLog.debug(f"skip jediTaskID={jediTaskID} datasetID={datasetID} has unprocessed files")
                                toSkip = True
                                break
                            # set status for input datasets
                            varMap = {}
                            varMap[":datasetID"] = datasetID
                            varMap[":jediTaskID"] = jediTaskID
                            if masterID is not None:
                                # seconday dataset, this will be reset in post-processor
                                varMap[":status"] = "done"
                            else:
                                # master dataset
                                if nFiles == nFilesFinished:
                                    # all succeeded
                                    varMap[":status"] = "done"
                                    preprocessedFlag = True
                                elif nFilesFinished == 0:
                                    # all failed
                                    varMap[":status"] = "failed"
                                else:
                                    # partially succeeded
                                    varMap[":status"] = "finished"
                            varMapList.append(varMap)
                        if not toSkip:
                            # check just in case if there is mutable dataset
                            if not mutableFlag:
                                varMap = {}
                                varMap[":jediTaskID"] = jediTaskID
                                varMap[":state"] = "mutable"
                                varMap.update(INPUT_TYPES_var_map)
                                self.cur.execute(sqlMTC + comment, varMap)
                                resMTC = self.cur.fetchone()
                                (numMutable,) = resMTC
                                tmpLog.debug(f"jediTaskID={jediTaskID} has {numMutable} mutable datasets")
                                if numMutable > 0:
                                    mutableFlag = True
                            if mutableFlag:
                                # go to defined to trigger CF
                                newTaskStatus = "defined"
                                # change status of mutable datasets to trigger CF
                                varMap = {}
                                varMap[":jediTaskID"] = jediTaskID
                                varMap[":state"] = "mutable"
                                varMap[":status"] = "toupdate"
                                self.cur.execute(sqlMUT + comment, varMap)
                                nRow = self.cur.rowcount
                                tmpLog.debug(f"jediTaskID={jediTaskID} updated {nRow} mutable datasets")
                            else:
                                # update input datasets
                                for varMap in varMapList:
                                    self.cur.execute(sqlDIU + comment, varMap)
                                # update output datasets
                                varMap = {}
                                varMap[":jediTaskID"] = jediTaskID
                                varMap[":type1"] = "log"
                                varMap[":type2"] = "output"
                                varMap[":status"] = "prepared"
                                self.cur.execute(sqlDOU + comment, varMap)
                                # new task status
                                if taskSpec.status == "preprocessing" and preprocessedFlag:
                                    # failed preprocess goes to prepared to terminate the task
                                    newTaskStatus = "registered"
                                    # update split rule
                                    taskSpec.setPreProcessed()
                                    varMap = {}
                                    varMap[":jediTaskID"] = jediTaskID
                                    varMap[":splitRule"] = taskSpec.splitRule
                                    self.cur.execute(sqlUSL + comment, varMap)
                                else:
                                    newTaskStatus = "prepared"
                    else:
                        toSkip = True
                        tmpLog.debug(f"skip jediTaskID={jediTaskID} due to status={taskSpec.status}")
                    # update tasks
                    if not toSkip:
                        varMap = {}
                        varMap[":jediTaskID"] = jediTaskID
                        varMap[":status"] = newTaskStatus
                        varMap[":oldStatus"] = oldStatus
                        varMap[":errorDialog"] = errorDialog
                        varMap[":splitRule"] = taskSpec.splitRule
                        self.cur.execute(sqlTU + comment, varMap)
                        tmpLog.debug(f"done new status={newTaskStatus} for jediTaskID={jediTaskID}{f' since {errorDialog}' if errorDialog else ''}")
                        if newTaskStatus == "exhausted":
                            self.setDeftStatus_JEDI(jediTaskID, newTaskStatus)
                            self.setSuperStatus_JEDI(jediTaskID, newTaskStatus)
                        self.record_task_status_change(jediTaskID)
                        self.push_task_status_message(taskSpec, jediTaskID, newTaskStatus)
                        get_metrics_module(self).update_task_queued_activated_times(jediTaskID)
                        get_metrics_module(self).unset_task_activated_time(jediTaskID, newTaskStatus)
                        ret_list.append(jediTaskID)
                    else:
                        # unlock
                        varMap = {}
                        varMap[":jediTaskID"] = jediTaskID
                        varMap[":status"] = taskSpec.status
                        self.cur.execute(sqlTUU + comment, varMap)
                        nRow = self.cur.rowcount
                        tmpLog.debug(f"unlock jediTaskID={jediTaskID} in status={taskSpec.status} with {nRow}")
                # commit
                if not self._commit():
                    raise RuntimeError("Commit error")
            tmpLog.debug("done")
            return ret_list
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return failedRet

    # get tasks to be assigned
    def getTasksToAssign_JEDI(self, vo, prodSourceLabel, workQueue, resource_name):
        comment = " /* JediDBProxy.getTasksToAssign_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"vo={vo} label={prodSourceLabel} queue={workQueue.queue_name} resource_name={resource_name}")
        tmpLog.debug("start")
        retJediTaskIDs = []
        try:
            # sql to get tasks to assign
            varMap = {}
            varMap[":status"] = "assigning"
            varMap[":worldCloud"] = JediTaskSpec.worldCloudName
            varMap[":timeLimit"] = naive_utcnow() - datetime.timedelta(minutes=30)
            sqlSCF = "SELECT jediTaskID "
            sqlSCF += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_AUX_Status_MinTaskID tabA ".format(panda_config.schemaJEDI)
            sqlSCF += "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID "
            sqlSCF += "AND tabT.status=:status AND tabT.modificationTime<:timeLimit "
            if vo not in [None, "any"]:
                varMap[":vo"] = vo
                sqlSCF += "AND vo=:vo "
            if prodSourceLabel not in [None, "any"]:
                varMap[":prodSourceLabel"] = prodSourceLabel
                sqlSCF += "AND prodSourceLabel=:prodSourceLabel "
            sqlSCF += "AND (cloud IS NULL OR "
            sqlSCF += "(cloud=:worldCloud AND (nucleus IS NULL OR EXISTS "
            sqlSCF += f"(SELECT 1 FROM {panda_config.schemaJEDI}.JEDI_Datasets "
            sqlSCF += f"WHERE {panda_config.schemaJEDI}.JEDI_Datasets.jediTaskID=tabT.jediTaskID "
            sqlSCF += "AND type IN (:dsType1,:dsType2) AND destination IS NULL) "
            sqlSCF += "))) "
            varMap[":dsType1"] = "output"
            varMap[":dsType2"] = "log"
            if workQueue.is_global_share:
                sqlSCF += "AND gshare=:wq_name AND resource_type=:resource_name "
                sqlSCF += f"AND tabT.workqueue_id NOT IN (SELECT queue_id FROM {panda_config.schemaJEDI}.jedi_work_queue WHERE queue_function = 'Resource') "
                varMap[":wq_name"] = workQueue.queue_name
                varMap[":resource_name"] = resource_name
            else:
                sqlSCF += "AND workQueue_ID=:wq_id "
                varMap[":wq_id"] = workQueue.queue_id
            sqlSCF += "ORDER BY currentPriority DESC,jediTaskID FOR UPDATE"
            sqlSPC = f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks SET modificationTime=CURRENT_DATE,errorDialog=NULL "
            sqlSPC += "WHERE jediTaskID=:jediTaskID "
            # begin transaction
            self.conn.begin()
            # get tasks
            tmpLog.debug(sqlSCF + comment + str(varMap))
            self.cur.execute(sqlSCF + comment, varMap)
            resList = self.cur.fetchall()
            for (jediTaskID,) in resList:
                # update modificationTime
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                self.cur.execute(sqlSPC + comment, varMap)
                nRow = self.cur.rowcount
                if nRow > 0:
                    retJediTaskIDs.append(jediTaskID)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            tmpLog.debug(f"got {len(retJediTaskIDs)} tasks")
            return retJediTaskIDs
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return None

    # get tasks to check task assignment
    def getTasksToCheckAssignment_JEDI(self, vo, prodSourceLabel, workQueue, resource_name):
        comment = " /* JediDBProxy.getTasksToCheckAssignment_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"vo={vo} label={prodSourceLabel} queue={workQueue.queue_name}")
        tmpLog.debug("start")
        retJediTaskIDs = []
        try:
            # sql to get tasks to assign
            varMap = {}
            varMap[":status"] = "assigning"
            varMap[":worldCloud"] = JediTaskSpec.worldCloudName
            sqlSCF = "SELECT jediTaskID "
            sqlSCF += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_AUX_Status_MinTaskID tabA ".format(panda_config.schemaJEDI)
            sqlSCF += "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID "
            sqlSCF += "AND tabT.status=:status "
            if vo not in [None, "any"]:
                varMap[":vo"] = vo
                sqlSCF += "AND vo=:vo "
            if prodSourceLabel not in [None, "any"]:
                varMap[":prodSourceLabel"] = prodSourceLabel
                sqlSCF += "AND prodSourceLabel=:prodSourceLabel "
            sqlSCF += "AND (cloud IS NULL OR "
            sqlSCF += "(cloud=:worldCloud AND EXISTS "
            sqlSCF += f"(SELECT 1 FROM {panda_config.schemaJEDI}.JEDI_Datasets "
            sqlSCF += f"WHERE {panda_config.schemaJEDI}.JEDI_Datasets.jediTaskID=tabT.jediTaskID "
            sqlSCF += "AND type IN (:dsType1,:dsType2) AND destination IS NULL) "
            sqlSCF += ")) "
            varMap[":dsType1"] = "output"
            varMap[":dsType2"] = "log"
            if workQueue.is_global_share:
                sqlSCF += "AND gshare=:wq_name AND resource_type=:resource_name "
                sqlSCF += f"AND tabT.workqueue_id NOT IN (SELECT queue_id FROM {panda_config.schemaJEDI}.jedi_work_queue WHERE queue_function = 'Resource') "
                varMap[":wq_name"] = workQueue.queue_name
                varMap[":resource_name"] = resource_name
            else:
                sqlSCF += "AND workQueue_ID=:wq_id "
                varMap[":wq_id"] = workQueue.queue_id

            # begin transaction
            self.conn.begin()
            # get tasks
            tmpLog.debug(sqlSCF + comment + str(varMap))
            self.cur.execute(sqlSCF + comment, varMap)
            resList = self.cur.fetchall()
            for (jediTaskID,) in resList:
                retJediTaskIDs.append(jediTaskID)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            tmpLog.debug(f"got {len(retJediTaskIDs)} tasks")
            return retJediTaskIDs
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return None

    # set cloud to tasks
    def setCloudToTasks_JEDI(self, taskCloudMap):
        comment = " /* JediDBProxy.setCloudToTasks_JEDI */"
        tmpLog = self.create_tagged_logger(comment)
        tmpLog.debug("start")
        try:
            if taskCloudMap != {}:
                for jediTaskID, tmpVal in taskCloudMap.items():
                    # begin transaction
                    self.conn.begin()
                    if isinstance(tmpVal, str):
                        # sql to set cloud
                        sql = f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks "
                        sql += "SET cloud=:cloud,status=:status,oldStatus=NULL,stateChangeTime=CURRENT_DATE "
                        sql += "WHERE jediTaskID=:jediTaskID AND cloud IS NULL "
                        varMap = {}
                        varMap[":jediTaskID"] = jediTaskID
                        varMap[":status"] = "ready"
                        varMap[":cloud"] = tmpVal
                        # set cloud
                        self.cur.execute(sql + comment, varMap)
                        nRow = self.cur.rowcount
                        tmpLog.debug(f"set cloud={tmpVal} for jediTaskID={jediTaskID} with {nRow}")
                    else:
                        # sql to set destinations for WORLD cloud
                        sql = f"UPDATE {panda_config.schemaJEDI}.JEDI_Datasets "
                        sql += "SET storageToken=:token,destination=:destination "
                        sql += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
                        for tmpItem in tmpVal["datasets"]:
                            varMap = {}
                            varMap[":jediTaskID"] = jediTaskID
                            varMap[":datasetID"] = tmpItem["datasetID"]
                            varMap[":token"] = tmpItem["token"]
                            varMap[":destination"] = tmpItem["destination"]
                            self.cur.execute(sql + comment, varMap)
                            tmpLog.debug(f"set token={tmpItem['token']} for jediTaskID={jediTaskID} datasetID={tmpItem['datasetID']}")
                        # sql to set ready
                        sql = f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks "
                        sql += "SET nucleus=:nucleus,status=:newStatus,oldStatus=NULL,stateChangeTime=CURRENT_DATE,modificationTime=CURRENT_DATE-1/24 "
                        sql += "WHERE jediTaskID=:jediTaskID AND status=:oldStatus "
                        varMap = {}
                        varMap[":jediTaskID"] = jediTaskID
                        varMap[":nucleus"] = tmpVal["nucleus"]
                        varMap[":newStatus"] = "ready"
                        varMap[":oldStatus"] = "assigning"
                        self.cur.execute(sql + comment, varMap)
                        nRow = self.cur.rowcount
                        tmpLog.debug(f"set nucleus={tmpVal['nucleus']} for jediTaskID={jediTaskID} with {nRow}")
                        newStatus = varMap[":newStatus"]
                    # update DEFT
                    if nRow > 0:
                        deftStatus = "ready"
                        self.setDeftStatus_JEDI(jediTaskID, deftStatus)
                        self.setSuperStatus_JEDI(jediTaskID, deftStatus)
                        # get parameters to enable jumbo
                        sqlRT = f"SELECT eventService,site,useJumbo,splitRule FROM {panda_config.schemaJEDI}.JEDI_Tasks "
                        sqlRT += "WHERE jediTaskID=:jediTaskID "
                        varMap = {}
                        varMap[":jediTaskID"] = jediTaskID
                        self.cur.execute(sqlRT + comment, varMap)
                        resRT = self.cur.fetchone()
                        if resRT is not None:
                            eventService, site, useJumbo, splitRule = resRT
                            # enable jumbo
                            get_task_utils_module(self).enableJumboInTask_JEDI(jediTaskID, eventService, site, useJumbo, splitRule)
                        # task status logging
                        self.record_task_status_change(jediTaskID)
                        try:
                            (newStatus, splitRule)
                        except NameError:
                            pass
                        else:
                            self.push_task_status_message(None, jediTaskID, newStatus, splitRule)
                    # set queued time
                    get_metrics_module(self).update_task_queued_activated_times(jediTaskID)
                    # commit
                    if not self._commit():
                        raise RuntimeError("Commit error")
            # return
            tmpLog.debug("done")
            return True
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return False

    # get the list of tasks to exec command
    def getTasksToExecCommand_JEDI(self, vo, prodSourceLabel):
        comment = " /* JediDBProxy.getTasksToExecCommand_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"vo={vo} label={prodSourceLabel}")
        tmpLog.debug("start")
        retTaskIDs = {}
        commandStatusMap = JediTaskSpec.commandStatusMap()
        try:
            # sql to get jediTaskIDs to exec a command from the command table
            varMap = {}
            varMap[":comm_owner"] = "DEFT"
            sqlC = f"SELECT comm_task,comm_cmd,comm_comment FROM {panda_config.schemaDEFT}.PRODSYS_COMM "
            comm_var_names_str, comm_var_map = get_sql_IN_bind_variables(commandStatusMap.keys(), prefix=":comm_cmd_", value_as_suffix=True)
            sqlC += f"WHERE comm_owner=:comm_owner AND comm_cmd IN ({comm_var_names_str}) "
            varMap.update(comm_var_map)
            if vo not in [None, "any"]:
                varMap[":comm_vo"] = vo
                sqlC += "AND comm_vo=:comm_vo "
            if prodSourceLabel not in [None, "any"]:
                varMap[":comm_prodSourceLabel"] = prodSourceLabel
                sqlC += "AND comm_prodSourceLabel=:comm_prodSourceLabel "
            sqlC += "ORDER BY comm_ts "
            # start transaction
            self.conn.begin()
            self.cur.arraysize = 10000
            tmpLog.debug(sqlC + comment + str(varMap))
            self.cur.execute(sqlC + comment, varMap)
            resList = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmpLog.debug(f"got {len(resList)} tasks")
            for jediTaskID, commandStr, comComment in resList:
                tmpLog.debug(f"start jediTaskID={jediTaskID} command={commandStr}")
                # start transaction
                self.conn.begin()
                # lock
                varMap = {}
                varMap[":comm_task"] = jediTaskID
                sqlLock = f"SELECT comm_cmd FROM {panda_config.schemaDEFT}.PRODSYS_COMM WHERE comm_task=:comm_task "
                sqlLock += "FOR UPDATE "
                toSkip = False
                sync_action_only = False
                resetFrozenTime = False
                try:
                    tmpLog.debug(sqlLock + comment + str(varMap))
                    self.cur.execute(sqlLock + comment, varMap)
                except Exception:
                    errType, errValue = sys.exc_info()[:2]
                    if self.isNoWaitException(errValue):
                        # resource busy and acquire with NOWAIT specified
                        toSkip = True
                        tmpLog.debug(f"skip locked+nowauit jediTaskID={jediTaskID}")
                    else:
                        # failed with something else
                        raise errType(errValue)
                isOK = True
                update_task = True
                if not toSkip:
                    if isOK:
                        # check task status
                        varMap = {}
                        varMap[":jediTaskID"] = jediTaskID
                        sqlTC = f"SELECT status,oldStatus,wallTimeUnit FROM {panda_config.schemaJEDI}.JEDI_Tasks "
                        sqlTC += "WHERE jediTaskID=:jediTaskID FOR UPDATE "
                        self.cur.execute(sqlTC + comment, varMap)
                        resTC = self.cur.fetchone()
                        if resTC is None or resTC[0] is None:
                            tmpLog.error(f"jediTaskID={jediTaskID} is not found in JEDI_Tasks")
                            isOK = False
                        else:
                            taskStatus, taskOldStatus, wallTimeUnit = resTC
                            tmpLog.debug(f"jediTaskID={jediTaskID} in status:{taskStatus} old:{taskOldStatus} com:{commandStr}")
                            if commandStr == "retry":
                                if taskStatus not in JediTaskSpec.statusToRetry():
                                    # task is in a status which rejects retry
                                    tmpLog.error(f"jediTaskID={jediTaskID} rejected command={commandStr}. status={taskStatus} is not for retry")
                                    isOK = False
                            elif commandStr == "incexec":
                                if taskStatus not in JediTaskSpec.statusToIncexec():
                                    # task is in a status which rejects retry
                                    tmpLog.error(f"jediTaskID={jediTaskID} rejected command={commandStr}. status={taskStatus} is not for incexec")
                                    isOK = False
                            elif commandStr == "pause":
                                if taskStatus in JediTaskSpec.statusNotToPause():
                                    # task is in a status which rejects pause
                                    tmpLog.error(f"jediTaskID={jediTaskID} rejected command={commandStr}. status={taskStatus} is not for pause")
                                    isOK = False
                            elif commandStr == "resume":
                                if taskStatus not in ["paused", "throttled", "staging"]:
                                    # task is in a status which rejects resume
                                    tmpLog.error(f"jediTaskID={jediTaskID} rejected command={commandStr}. status={taskStatus} is not for resume")
                                    isOK = False
                            elif commandStr == "avalanche":
                                if taskStatus not in ["scouting"]:
                                    # task is in a status which rejects avalanche
                                    tmpLog.error(f"jediTaskID={jediTaskID} rejected command={commandStr}. status={taskStatus} is not for avalanche")
                                    isOK = False
                            elif commandStr == "release":
                                if taskStatus not in ["scouting", "pending", "running", "ready", "assigning", "defined"]:
                                    # task is in a status which rejects avalanche
                                    tmpLog.error(f"jediTaskID={jediTaskID} rejected command={commandStr}. status={taskStatus} is not applicable")
                                    isOK = False
                                update_task = False
                                sync_action_only = True
                            elif taskStatus in JediTaskSpec.statusToRejectExtChange():
                                # task is in a status which rejects external changes
                                tmpLog.error(f"jediTaskID={jediTaskID} rejected command={commandStr} (due to status={taskStatus})")
                                isOK = False
                            if isOK:
                                # set new task status
                                if commandStr == "retry" and taskStatus == "exhausted" and taskOldStatus in ["running", "scouting"]:
                                    # change task status only since retryTask increments attemptNrs for existing jobs
                                    if taskOldStatus == "scouting" and wallTimeUnit:
                                        # go to running since scouting passed, to avoid being prepared again
                                        newTaskStatus = "running"
                                    else:
                                        newTaskStatus = taskOldStatus
                                    sync_action_only = True
                                    resetFrozenTime = True
                                elif commandStr in ["avalanche"]:
                                    newTaskStatus = "scouting"
                                    sync_action_only = True
                                elif commandStr == "resume" and taskStatus == "staging":
                                    newTaskStatus = "staged"
                                    sync_action_only = True
                                elif commandStr in commandStatusMap:
                                    newTaskStatus = commandStatusMap[commandStr]["doing"]
                                else:
                                    tmpLog.error(f"jediTaskID={jediTaskID} new status is undefined for command={commandStr}")
                                    isOK = False
                    if isOK:
                        # actions in transaction
                        if commandStr == "release":
                            get_task_utils_module(self).updateInputDatasetsStaged_JEDI(jediTaskID, None, use_commit=False, by="release")
                    if isOK and update_task:
                        # update task status
                        varMap = {}
                        varMap[":jediTaskID"] = jediTaskID
                        varMap[":taskStatus"] = taskStatus
                        if newTaskStatus != "dummy":
                            varMap[":status"] = newTaskStatus
                        varMap[":errDiag"] = comComment
                        sqlTU = f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks "
                        if newTaskStatus != "dummy":
                            sqlTU += "SET status=:status,"
                        else:
                            if taskOldStatus is None:
                                tmpLog.error("jediTaskID={0} has oldStatus=None and status={1} for ".format(jediTaskID, taskStatus, commandStr))
                                isOK = False
                            sqlTU += "SET status=oldStatus,"
                        if taskStatus in ["paused"] or sync_action_only:
                            sqlTU += "oldStatus=NULL,"
                        elif taskStatus in ["throttled"] and commandStr in ["pause", "reassign"]:
                            # unchange oldStatus when throttled->paused/toreassign
                            pass
                        elif taskStatus not in ["pending"]:
                            sqlTU += "oldStatus=status,"
                        if commandStr in ["avalanche"]:
                            # set dummy wallTimeUnit to trigger avalanche
                            sqlTU += "wallTimeUnit=:wallTimeUnit,"
                            varMap[":wallTimeUnit"] = "ava"
                        if resetFrozenTime:
                            sqlTU += "frozenTime=NULL,"
                        sqlTU += "modificationTime=CURRENT_DATE,errorDialog=:errDiag,stateChangeTime=CURRENT_DATE "
                        sqlTU += "WHERE jediTaskID=:jediTaskID AND status=:taskStatus "
                        if isOK:
                            tmpLog.debug(sqlTU + comment + str(varMap))
                            self.cur.execute(sqlTU + comment, varMap)
                            nRow = self.cur.rowcount
                        else:
                            nRow = 0
                        if nRow != 1:
                            tmpLog.debug(f"skip updated jediTaskID={jediTaskID}")
                            toSkip = True
                        else:
                            # update T_TASK
                            if (
                                newTaskStatus in ["paused"]
                                or (newTaskStatus in ["running", "ready", "scouting"] and taskStatus in ["paused", "exhausted"])
                                or newTaskStatus in ["staged"]
                            ):
                                if newTaskStatus == "scouting":
                                    deftStatus = "submitting"
                                elif newTaskStatus == "staged":
                                    deftStatus = "registered"
                                else:
                                    deftStatus = newTaskStatus
                                self.setDeftStatus_JEDI(jediTaskID, deftStatus)
                                self.setSuperStatus_JEDI(jediTaskID, deftStatus)
                                # add missing record_task_status_change and push_task_status_message updates
                                self.record_task_status_change(jediTaskID)
                                self.push_task_status_message(None, jediTaskID, newTaskStatus)
                    # update command table
                    if not toSkip:
                        varMap = {}
                        varMap[":comm_task"] = jediTaskID
                        if isOK:
                            varMap[":comm_cmd"] = commandStr + "ing"
                        else:
                            varMap[":comm_cmd"] = commandStr + " failed"
                        sqlUC = f"UPDATE {panda_config.schemaDEFT}.PRODSYS_COMM SET comm_cmd=:comm_cmd WHERE comm_task=:comm_task "
                        self.cur.execute(sqlUC + comment, varMap)
                        # append
                        if isOK:
                            if commandStr not in ["pause", "resume"] and not sync_action_only:
                                retTaskIDs[jediTaskID] = {"command": commandStr, "comment": comComment, "oldStatus": taskStatus}
                                # use old status if pending or throttled
                                if taskStatus in ["pending", "throttled"]:
                                    retTaskIDs[jediTaskID]["oldStatus"] = taskOldStatus
                            # update job table
                            if commandStr in ["pause", "resume"]:
                                sqlJT = f"UPDATE {panda_config.schemaPANDA}.jobsActive4 "
                                sqlJT += "SET jobStatus=:newJobStatus "
                                sqlJT += "WHERE jediTaskID=:jediTaskID AND jobStatus=:oldJobStatus "
                                varMap = {}
                                varMap[":jediTaskID"] = jediTaskID
                                if commandStr == "resume":
                                    varMap[":newJobStatus"] = "activated"
                                    varMap[":oldJobStatus"] = "throttled"
                                else:
                                    varMap[":newJobStatus"] = "throttled"
                                    varMap[":oldJobStatus"] = "activated"
                                self.cur.execute(sqlJT + comment, varMap)
                # commit
                if not self._commit():
                    raise RuntimeError("Commit error")
            # find orphaned tasks to rescue
            for commandStr, taskStatusMap in commandStatusMap.items():
                varMap = {}
                varMap[":status"] = taskStatusMap["doing"]
                # skip dummy status
                if varMap[":status"] in ["dummy", "paused"]:
                    continue
                self.conn.begin()
                # FIXME
                # varMap[':timeLimit'] = naive_utcnow() - datetime.timedelta(hours=1)
                varMap[":timeLimit"] = naive_utcnow() - datetime.timedelta(minutes=5)
                sqlOrpS = "SELECT jediTaskID,errorDialog,oldStatus "
                sqlOrpS += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_AUX_Status_MinTaskID tabA ".format(panda_config.schemaJEDI)
                sqlOrpS += "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID "
                sqlOrpS += "AND tabT.status=:status AND tabT.modificationtime<:timeLimit "
                if vo not in [None, "any"]:
                    sqlOrpS += "AND vo=:vo "
                    varMap[":vo"] = vo
                if prodSourceLabel not in [None, "any"]:
                    sqlOrpS += "AND prodSourceLabel=:prodSourceLabel "
                    varMap[":prodSourceLabel"] = prodSourceLabel
                sqlOrpS += "FOR UPDATE "
                tmpLog.debug(sqlOrpS + comment + str(varMap))
                self.cur.execute(sqlOrpS + comment, varMap)
                resList = self.cur.fetchall()
                # update modtime to avoid immediate reattempts
                sqlOrpU = f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks SET modificationtime=CURRENT_DATE "
                sqlOrpU += "WHERE jediTaskID=:jediTaskID "
                for jediTaskID, comComment, oldStatus in resList:
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    tmpLog.debug(sqlOrpU + comment + str(varMap))
                    self.cur.execute(sqlOrpU + comment, varMap)
                    nRow = self.cur.rowcount
                    if nRow == 1 and jediTaskID not in retTaskIDs:
                        retTaskIDs[jediTaskID] = {"command": commandStr, "comment": comComment, "oldStatus": oldStatus}
                # commit
                if not self._commit():
                    raise RuntimeError("Commit error")
            # read clob
            sqlCC = f"SELECT comm_parameters FROM {panda_config.schemaDEFT}.PRODSYS_COMM WHERE comm_task=:comm_task "
            for jediTaskID in retTaskIDs.keys():
                if retTaskIDs[jediTaskID]["command"] in ["incexec"]:
                    # start transaction
                    self.conn.begin()
                    varMap = {}
                    varMap[":comm_task"] = jediTaskID
                    self.cur.execute(sqlCC + comment, varMap)
                    tmpComComment = None
                    for (clobCC,) in self.cur:
                        if clobCC is not None:
                            tmpComComment = clobCC
                        break
                    if tmpComComment not in ["", None]:
                        retTaskIDs[jediTaskID]["comment"] = tmpComComment
                    # commit
                    if not self._commit():
                        raise RuntimeError("Commit error")
            # convert to list
            retTaskList = []
            for jediTaskID, varMap in retTaskIDs.items():
                retTaskList.append((jediTaskID, varMap))
            # return
            tmpLog.debug(f"return {len(retTaskList)} tasks")
            return retTaskList
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return None

    # reactivate pending tasks
    def reactivatePendingTasks_JEDI(self, vo, prodSourceLabel, timeLimit, timeoutLimit=None, minPriority=None):
        comment = " /* JediDBProxy.reactivatePendingTasks_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"vo={vo} label={prodSourceLabel} limit={timeLimit} min timeout={timeoutLimit}hours minPrio={minPriority}")
        tmpLog.debug("start")
        try:
            timeoutDate = None
            if timeoutLimit is not None:
                timeoutDate = naive_utcnow() - datetime.timedelta(hours=timeoutLimit)
            # sql to get pending tasks
            varMap = {}
            varMap[":status"] = "pending"
            varMap[":timeLimit"] = naive_utcnow() - datetime.timedelta(minutes=timeLimit)
            sqlTL = "SELECT jediTaskID,frozenTime,errorDialog,parent_tid,splitRule,startTime "
            sqlTL += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_AUX_Status_MinTaskID tabA ".format(panda_config.schemaJEDI)
            sqlTL += "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID "
            sqlTL += "AND tabT.status=:status AND tabT.modificationTime<:timeLimit AND tabT.oldStatus IS NOT NULL "
            if vo not in [None, "any"]:
                varMap[":vo"] = vo
                sqlTL += "AND vo=:vo "
            if prodSourceLabel not in [None, "any"]:
                varMap[":prodSourceLabel"] = prodSourceLabel
                sqlTL += "AND prodSourceLabel=:prodSourceLabel "
            if minPriority is not None:
                varMap[":minPriority"] = minPriority
                sqlTL += "AND currentPriority>=:minPriority "
            # sql to update tasks
            sqlTU = f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks "
            sqlTU += "SET status=oldStatus,oldStatus=NULL "
            sqlTU += "WHERE jediTaskID=:jediTaskID AND oldStatus IS NOT NULL AND status=:oldStatus "
            # sql to timeout tasks
            sqlTO = f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks "
            sqlTO += "SET status=:newStatus,errorDialog=:errorDialog,modificationtime=CURRENT_DATE,stateChangeTime=CURRENT_DATE "
            sqlTO += "WHERE jediTaskID=:jediTaskID AND status=:oldStatus "
            # sql to keep pending
            sqlTK = f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks "
            sqlTK += "SET modificationtime=CURRENT_DATE,frozenTime=CURRENT_DATE "
            sqlTK += "WHERE jediTaskID=:jediTaskID AND status=:oldStatus "
            # sql to check the number of finished files
            sqlND = f"SELECT SUM(nFilesFinished) FROM {panda_config.schemaJEDI}.JEDI_Datasets "
            sqlND += f"WHERE jediTaskID=:jediTaskID AND type IN ({INPUT_TYPES_var_str}) "
            sqlND += "AND masterID IS NULL "
            # start transaction
            self.conn.begin()
            self.cur.execute(sqlTL + comment, varMap)
            resTL = self.cur.fetchall()
            # loop over all tasks
            nRow = 0
            msg_driven_taskid_set = set()
            for jediTaskID, frozenTime, errorDialog, parent_tid, splitRule, startTime in resTL:
                timeoutFlag = False
                keepFlag = False
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                varMap[":oldStatus"] = "pending"
                # check parent
                parentRunning = False
                if parent_tid not in [None, jediTaskID]:
                    tmpStat = get_task_utils_module(self).checkParentTask_JEDI(parent_tid, jediTaskID, use_commit=False)
                    # if parent is running
                    if tmpStat == "running":
                        parentRunning = True
                if not keepFlag:
                    # if timeout
                    if not parentRunning and timeoutDate is not None and frozenTime is not None and frozenTime < timeoutDate:
                        timeoutFlag = True
                        # check the number of finished files
                        varMap = {}
                        varMap[":jediTaskID"] = jediTaskID
                        varMap.update(INPUT_TYPES_var_map)
                        self.cur.execute(sqlND + comment, varMap)
                        tmpND = self.cur.fetchone()
                        if tmpND is not None and tmpND[0] is not None and tmpND[0] > 0:
                            abortingFlag = False
                        else:
                            abortingFlag = True
                        # go to exhausted
                        varMap = {}
                        varMap[":jediTaskID"] = jediTaskID
                        varMap[":newStatus"] = "exhausted"
                        varMap[":oldStatus"] = "pending"
                        if errorDialog is None:
                            errorDialog = ""
                        else:
                            errorDialog += ". "
                        errorDialog += f"timeout while in pending since {frozenTime.strftime('%Y/%m/%d %H:%M:%S')}"
                        varMap[":errorDialog"] = errorDialog[: JediTaskSpec._limitLength["errorDialog"]]
                        sql = sqlTO
                    else:
                        varMap = {}
                        varMap[":jediTaskID"] = jediTaskID
                        varMap[":oldStatus"] = "pending"
                        sql = sqlTU
                self.cur.execute(sql + comment, varMap)
                tmpRow = self.cur.rowcount
                if tmpRow > 0:
                    if timeoutFlag:
                        tmpLog.info(f"#ATM #KV jediTaskID={jediTaskID} timeout")
                    elif keepFlag:
                        tmpLog.info(f"#ATM #KV jediTaskID={jediTaskID} action=keep_pending")
                    else:
                        tmpLog.info(f"#ATM #KV jediTaskID={jediTaskID} action=reactivate")
                        if is_msg_driven(splitRule):
                            # added msg driven tasks
                            msg_driven_taskid_set.add(jediTaskID)
                nRow += tmpRow
                if tmpRow > 0 and not keepFlag:
                    self.record_task_status_change(jediTaskID)
                # update DEFT for timeout
                if timeoutFlag:
                    self.push_task_status_message(None, jediTaskID, varMap[":newStatus"], splitRule)
                    deftStatus = varMap[":newStatus"]
                    self.setDeftStatus_JEDI(jediTaskID, deftStatus)
                    self.setSuperStatus_JEDI(jediTaskID, deftStatus)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            tmpLog.debug(f"updated {nRow} rows")
            return nRow, msg_driven_taskid_set
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return None, None

    # insert lib dataset and files
    def insertBuildFileSpec_JEDI(self, jobSpec, reusedDatasetID, simul):
        comment = " /* JediDBProxy.insertBuildFileSpec_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"jediTaskID={jobSpec.jediTaskID}")
        tmpLog.debug("start")
        try:
            # sql to insert dataset
            sqlDS = f"INSERT INTO {panda_config.schemaJEDI}.JEDI_Datasets ({JediDatasetSpec.columnNames()}) "
            sqlDS += JediDatasetSpec.bindValuesExpression()
            sqlDS += " RETURNING datasetID INTO :newDatasetID"
            # sql to insert file
            sqlFI = f"INSERT INTO {panda_config.schemaJEDI}.JEDI_Dataset_Contents ({JediFileSpec.columnNames()}) "
            sqlFI += JediFileSpec.bindValuesExpression()
            sqlFI += " RETURNING fileID INTO :newFileID"
            # sql to update LFN
            sqlFU = f"UPDATE {panda_config.schemaJEDI}.JEDI_Dataset_Contents "
            sqlFU += "SET lfn=:newLFN "
            sqlFU += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
            # make datasetSpec
            pandaFileSpec = jobSpec.Files[0]
            timeNow = naive_utcnow()
            datasetSpec = JediDatasetSpec()
            datasetSpec.jediTaskID = jobSpec.jediTaskID
            datasetSpec.creationTime = timeNow
            datasetSpec.modificationTime = timeNow
            datasetSpec.datasetName = pandaFileSpec.dataset
            datasetSpec.status = "defined"
            datasetSpec.type = "lib"
            datasetSpec.vo = jobSpec.VO
            datasetSpec.cloud = jobSpec.cloud
            datasetSpec.site = jobSpec.computingSite
            # make fileSpec
            fileSpecList = []
            for pandaFileSpec in jobSpec.Files:
                fileSpec = JediFileSpec()
                fileSpec.convertFromJobFileSpec(pandaFileSpec)
                fileSpec.status = "defined"
                fileSpec.creationDate = timeNow
                fileSpec.keepTrack = 1
                # change type to lib
                if fileSpec.type == "output":
                    fileSpec.type = "lib"
                # scope
                if datasetSpec.vo in self.jedi_config.ddm.voWithScope.split(","):
                    fileSpec.scope = get_job_complex_module(self).extractScope(datasetSpec.datasetName)
                # append
                fileSpecList.append((fileSpec, pandaFileSpec))
            # start transaction
            self.conn.begin()
            varMap = datasetSpec.valuesMap(useSeq=True)
            varMap[":newDatasetID"] = self.cur.var(varNUMBER)
            # insert dataset
            if reusedDatasetID is not None:
                datasetID = reusedDatasetID
            elif not simul:
                self.cur.execute(sqlDS + comment, varMap)
                val = self.getvalue_corrector(self.cur.getvalue(varMap[":newDatasetID"]))
                datasetID = int(val)
            else:
                datasetID = 0
            # insert files
            fileIdMap = {}
            for fileSpec, pandaFileSpec in fileSpecList:
                fileSpec.datasetID = datasetID
                varMap = fileSpec.valuesMap(useSeq=True)
                varMap[":newFileID"] = self.cur.var(varNUMBER)
                if not simul:
                    self.cur.execute(sqlFI + comment, varMap)
                    val = self.getvalue_corrector(self.cur.getvalue(varMap[":newFileID"]))
                    fileID = int(val)
                else:
                    fileID = 0
                # change placeholder in filename
                newLFN = fileSpec.lfn.replace("$JEDIFILEID", str(fileID))
                varMap = {}
                varMap[":jediTaskID"] = fileSpec.jediTaskID
                varMap[":datasetID"] = datasetID
                varMap[":fileID"] = fileID
                varMap[":newLFN"] = newLFN
                if not simul:
                    self.cur.execute(sqlFU + comment, varMap)
                # return IDs in a map since changes to jobSpec are not effective
                # since invoked in separate processes
                fileIdMap[fileSpec.lfn] = {"datasetID": datasetID, "fileID": fileID, "newLFN": newLFN, "scope": fileSpec.scope}
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            tmpLog.debug("done")
            return True, fileIdMap
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return False, None

    # retry or incrementally execute a task
    def retryTask_JEDI(
        self,
        jediTaskID,
        commStr,
        maxAttempt=5,
        useCommit=True,
        statusCheck=True,
        retryChildTasks=True,
        discardEvents=False,
        release_unstaged=False,
        keep_share_priority=False,
        ignore_hard_exhausted=False,
    ):
        comment = " /* JediDBProxy.retryTask_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID}")
        tmpLog.debug(
            f"start command={commStr} retry_child={retryChildTasks} discard_events={discardEvents} release_unstaged={release_unstaged} keep_share_priority={keep_share_priority} ignore_hard_exhausted={ignore_hard_exhausted}"
        )
        newTaskStatus = None
        retried_tasks = []
        # check command
        if commStr not in ["retry", "incexec"]:
            tmpLog.debug(f"unknown command={commStr}")
            return False, None, retried_tasks
        try:
            # sql to retry files without maxFailure
            sqlRFO = f"UPDATE {panda_config.schemaJEDI}.JEDI_Dataset_Contents "
            sqlRFO += "SET maxAttempt=maxAttempt+:maxAttempt,proc_status=:proc_status "
            sqlRFO += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND status=:status "
            sqlRFO += "AND keepTrack=:keepTrack AND maxAttempt IS NOT NULL AND maxAttempt<=attemptNr AND maxFailure IS NULL "
            # sql to retry files with maxFailure
            sqlRFF = f"UPDATE {panda_config.schemaJEDI}.JEDI_Dataset_Contents "
            sqlRFF += "SET maxAttempt=maxAttempt+:maxAttempt,maxFailure=maxFailure+:maxAttempt,proc_status=:proc_status "
            sqlRFF += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND status=:status "
            sqlRFF += "AND keepTrack=:keepTrack AND maxAttempt IS NOT NULL AND maxFailure IS NOT NULL AND (maxAttempt<=attemptNr OR maxFailure<=failedAttempt) "
            # sql to reset ramCount
            sqlRRC = f"UPDATE {panda_config.schemaJEDI}.JEDI_Dataset_Contents "
            sqlRRC += "SET ramCount=0 "
            sqlRRC += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND status=:status "
            sqlRRC += "AND keepTrack=:keepTrack "
            # sql to count unprocessed files
            sqlCU = f"SELECT COUNT(*) FROM {panda_config.schemaJEDI}.JEDI_Dataset_Contents "
            sqlCU += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND status=:status "
            sqlCU += "AND keepTrack=:keepTrack AND maxAttempt IS NOT NULL AND maxAttempt>attemptNr "
            sqlCU += "AND (maxFailure IS NULL OR maxFailure>failedAttempt) "
            # sql to count failed files
            sqlCF = f"SELECT COUNT(*) FROM {panda_config.schemaJEDI}.JEDI_Dataset_Contents "
            sqlCF += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND status=:status "
            sqlCF += "AND keepTrack=:keepTrack AND ((maxAttempt IS NOT NULL AND maxAttempt<=attemptNr) "
            sqlCF += "OR (maxFailure IS NOT NULL AND maxFailure<=failedAttempt)) "
            # sql to retry/incexecute datasets
            sqlRD = f"UPDATE {panda_config.schemaJEDI}.JEDI_Datasets "
            sqlRD += (
                "SET status=:status,"
                "nFilesUsed=(CASE WHEN nFilesUsed-:nDiff-:nRun > 0 THEN nFilesUsed-:nDiff-:nRun ELSE 0 END),"
                "nFilesFailed=(CASE WHEN nFilesFailed-:nDiff > 0 THEN nFilesFailed-:nDiff ELSE 0 END) "
            )
            sqlRD += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            # sql to reset lost files in datasets
            sqlRL = f"UPDATE {panda_config.schemaJEDI}.JEDI_Datasets "
            sqlRL += "SET nFiles=nFiles+nFilesMissing,nFilesToBeUsed=nFilesToBeUsed+nFilesMissing,nFilesMissing=0 "
            sqlRL += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            # sql to update task status
            sqlUTB = f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks "
            sqlUTB += "SET status=:status,oldStatus=NULL,modificationtime=:updateTime,errorDialog=:errorDialog,stateChangeTime=CURRENT_DATE "
            sqlUTB += "WHERE jediTaskID=:jediTaskID "
            sqlUTN = f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks "
            sqlUTN += "SET status=:status,oldStatus=NULL,modificationtime=:updateTime,errorDialog=:errorDialog,"
            sqlUTN += "stateChangeTime=CURRENT_DATE,startTime=NULL,attemptNr=attemptNr+1,frozenTime=NULL "
            if not keep_share_priority:
                sqlUTN += ",currentPriority=taskPriority "
            sqlUTN += "WHERE jediTaskID=:jediTaskID "
            # sql to update DEFT task status
            sqlTT = f"UPDATE {panda_config.schemaDEFT}.T_TASK "
            sqlTT += "SET status=:status,timeStamp=CURRENT_DATE,start_time=NULL "
            sqlTT += "WHERE taskID=:jediTaskID AND start_time IS NOT NULL "
            # sql to discard events
            sqlDE = f"UPDATE {panda_config.schemaJEDI}.JEDI_Events "
            sqlDE += "SET status=:newStatus "
            sqlDE += "WHERE jediTaskID=:jediTaskID "
            sqlDE += "AND status IN (:esFinished,:esDone) "
            # sql to reset running files
            sqlRR = f"UPDATE {panda_config.schemaJEDI}.JEDI_Dataset_Contents "
            sqlRR += "SET status=:newStatus,attemptNr=attemptNr+1,maxAttempt=maxAttempt+:maxAttempt,proc_status=:proc_status "
            sqlRR += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND status IN (:oldStatus1,:oldStatus2) "
            sqlRR += "AND keepTrack=:keepTrack AND maxAttempt IS NOT NULL "
            # sql to update output/lib/log datasets
            sqlUO = f"UPDATE {panda_config.schemaJEDI}.JEDI_Datasets "
            sqlUO += "SET status=:status "
            sqlUO += "WHERE jediTaskID=:jediTaskID AND type IN (:type1,:type2,:type3) "
            # start transaction
            if useCommit:
                self.conn.begin()
            self.cur.arraysize = 100000
            # check task status
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            sqlTK = f"SELECT status,oldStatus,attemptNr,prodSourceLabel FROM {panda_config.schemaJEDI}.JEDI_Tasks WHERE jediTaskID=:jediTaskID FOR UPDATE "
            self.cur.execute(sqlTK + comment, varMap)
            resTK = self.cur.fetchone()
            if resTK is None:
                # task not found
                msgStr = "task not found"
                tmpLog.debug(msgStr)
            else:
                # check task status
                taskStatus, taskOldStatus, task_attempt_number, prod_source_label = resTK
                # limits for attempt
                task_max_attempt = self.getConfigValue("retry_task", f"TASK_MAX_ATTEMPT_{prod_source_label}", "jedi")
                job_max_attempt = self.getConfigValue("retry_task", f"JOB_MAX_ATTEMPT_{prod_source_label}", "jedi")
                max_job_failure_rate = self.getConfigValue("retry_task", f"MAX_JOB_FAILURE_RATE_{prod_source_label}", "jedi")
                max_failed_hep_score_rate = self.getConfigValue("retry_task", f"MAX_FAILED_HEP_SCORE_RATE_{prod_source_label}", "jedi")
                max_failed_hep_score_hours = self.getConfigValue("retry_task", f"MAX_FAILED_HEP_SCORE_HOURS_{prod_source_label}", "jedi")
                min_cpu_efficiency = self.getConfigValue("retry_task", f"MIN_CPU_EFFICIENCY_{prod_source_label}", "jedi")
                newTaskStatus = None
                newErrorDialog = None
                if taskOldStatus == "done" and commStr == "retry" and statusCheck:
                    # no retry for finished task
                    msgStr = f"no {commStr} for task in {taskOldStatus} status"
                    tmpLog.debug(msgStr)
                    newTaskStatus = taskOldStatus
                    newErrorDialog = msgStr
                elif taskOldStatus not in JediTaskSpec.statusToIncexec() and statusCheck:
                    # only tasks in a relevant final status
                    msgStr = f"no {commStr} since not in relevant final status ({taskOldStatus})"
                    tmpLog.debug(msgStr)
                    newTaskStatus = taskOldStatus
                    newErrorDialog = msgStr
                else:
                    # get failure metrics
                    failure_metrics = get_metrics_module(self).get_task_failure_metrics(jediTaskID, False)
                    # get scout metrics
                    try:
                        scout_metics_ok, scout_metrics, scout_metrics_extra = get_task_utils_module(self).getScoutJobData_JEDI(jediTaskID)
                    except Exception as e:
                        tmpLog.debug(f"Failed to get scout metrics: {str(e)} {traceback.format_exc()}")
                        scout_metics_ok = False
                        scout_metrics_extra = {}
                    # check max attempts
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    sqlMAX = "SELECT MAX(c.maxAttempt) "
                    sqlMAX += "FROM {0}.JEDI_Datasets d, {0}.JEDI_Dataset_Contents c ".format(panda_config.schemaJEDI)
                    sqlMAX += "WHERE c.jediTaskID=d.jediTaskID AND c.datasetID=d.datasetID "
                    sqlMAX += f"AND d.jediTaskID=:jediTaskID AND d.type IN ({INPUT_TYPES_var_str}) "
                    varMap.update(INPUT_TYPES_var_map)
                    self.cur.execute(sqlMAX + comment, varMap)
                    resMAX = self.cur.fetchone()
                    if (
                        not ignore_hard_exhausted
                        and task_max_attempt is not None
                        and task_attempt_number is not None
                        and task_attempt_number >= task_max_attempt > 0
                    ):
                        # too many attempts
                        msg_str = f"exhausted upon retry since too many task attempts more than {task_max_attempt} are forbidden"
                        tmpLog.debug(msg_str)
                        newTaskStatus = "exhausted"
                        newErrorDialog = msg_str
                    elif (
                        not ignore_hard_exhausted
                        and max_failed_hep_score_hours is not None
                        and failure_metrics
                        and failure_metrics["failed_hep_score_hour"] is not None
                        and failure_metrics["failed_hep_score_hour"] >= max_failed_hep_score_hours > 0
                    ):
                        # failed HEP score hours are too large
                        msg_val = str(failure_metrics["failed_hep_score_hour"])
                        msg_str = f"exhausted upon retry since HEP score hours used by failed jobs ({msg_val} hours) exceed {max_failed_hep_score_hours} hours"
                        tmpLog.debug(msg_str)
                        newTaskStatus = "exhausted"
                        newErrorDialog = msg_str
                    elif (
                        not ignore_hard_exhausted
                        and max_failed_hep_score_rate is not None
                        and failure_metrics
                        and failure_metrics["failed_hep_score_ratio"] is not None
                        and failure_metrics["failed_hep_score_ratio"] >= max_failed_hep_score_rate > 0
                    ):
                        # failed HEP score hours are too large
                        msg_val = str(failure_metrics["failed_hep_score_ratio"])
                        msg_str = f"exhausted upon retry since failed/total HEP score rate ({msg_val}) exceeds {max_failed_hep_score_rate}"
                        tmpLog.debug(msg_str)
                        newTaskStatus = "exhausted"
                        newErrorDialog = msg_str
                    elif (
                        not ignore_hard_exhausted
                        and max_job_failure_rate is not None
                        and failure_metrics
                        and failure_metrics["single_failure_rate"] is not None
                        and failure_metrics["single_failure_rate"] >= max_job_failure_rate > 0
                    ):
                        # high failure rate
                        msg_val = str(failure_metrics["single_failure_rate"])
                        msg_str = f"exhausted upon retry since single job failure rate ({msg_val}) is higher than {max_job_failure_rate}"
                        tmpLog.debug(msg_str)
                        newTaskStatus = "exhausted"
                        newErrorDialog = msg_str
                    elif (
                        not ignore_hard_exhausted
                        and job_max_attempt is not None
                        and resMAX is not None
                        and resMAX[0] is not None
                        and resMAX[0] >= job_max_attempt
                    ):
                        # too many job attempts
                        msgStr = f"{commStr} was rejected due to too many attempts ({resMAX[0]} >= {job_max_attempt}) for some jobs"
                        tmpLog.debug(msgStr)
                        newTaskStatus = taskOldStatus
                        newErrorDialog = msgStr
                    elif (
                        not ignore_hard_exhausted
                        and min_cpu_efficiency is not None
                        and scout_metics_ok
                        and scout_metrics_extra.get("minCpuEfficiency") is not None
                        and min_cpu_efficiency > scout_metrics_extra.get("minCpuEfficiency")
                    ):
                        # inefficient CPU usage
                        msg_val = scout_metrics_extra.get("minCpuEfficiency")
                        msg_str = f"exhausted upon retry since average CPU efficiency across finished jobs ({msg_val}%) is less than {min_cpu_efficiency}%"
                        tmpLog.debug(msg_str)
                        newTaskStatus = "exhausted"
                        newErrorDialog = msg_str
                    else:
                        # get input datasets
                        varMap = {}
                        varMap[":jediTaskID"] = jediTaskID
                        sqlDS = "SELECT datasetID,masterID,nFiles,nFilesFinished,nFilesFailed,nFilesUsed,status,state,type,datasetName,nFilesMissing "
                        sqlDS += f"FROM {panda_config.schemaJEDI}.JEDI_Datasets "
                        sqlDS += "WHERE jediTaskID=:jediTaskID "
                        sqlDS += f"AND type IN ({INPUT_TYPES_var_str}) "
                        varMap.update(INPUT_TYPES_var_map)
                        self.cur.execute(sqlDS + comment, varMap)
                        resDS = self.cur.fetchall()
                        changedMasterList = []
                        secMap = {}
                        for (
                            datasetID,
                            masterID,
                            nFiles,
                            nFilesFinished,
                            nFilesFailed,
                            nFilesUsed,
                            status,
                            state,
                            datasetType,
                            datasetName,
                            nFilesMissing,
                        ) in resDS:
                            if masterID is not None:
                                if state not in [None, ""]:
                                    # keep secondary dataset info
                                    if masterID not in secMap:
                                        secMap[masterID] = []
                                    secMap[masterID].append((datasetID, nFilesFinished, status, state, datasetType))
                                    # update dataset
                                    varMap = {}
                                    varMap[":jediTaskID"] = jediTaskID
                                    varMap[":datasetID"] = datasetID
                                    varMap[":nDiff"] = 0
                                    varMap[":nRun"] = 0
                                    varMap[":status"] = "ready"
                                    tmpLog.debug(f"set status={varMap[':status']} for 2nd datasetID={datasetID}")
                                    self.cur.execute(sqlRD + comment, varMap)
                                else:
                                    # set dataset status to defined to trigger file lookup when state is not set
                                    varMap = {}
                                    varMap[":jediTaskID"] = jediTaskID
                                    varMap[":datasetID"] = datasetID
                                    varMap[":nDiff"] = 0
                                    varMap[":nRun"] = 0
                                    varMap[":status"] = "defined"
                                    tmpLog.debug(f"set status={varMap[':status']} for 2nd datasetID={datasetID}")
                                    self.cur.execute(sqlRD + comment, varMap)
                            else:
                                # set done if no more try is needed
                                if nFiles == nFilesFinished and status == "failed":
                                    # update dataset
                                    varMap = {}
                                    varMap[":jediTaskID"] = jediTaskID
                                    varMap[":datasetID"] = datasetID
                                    varMap[":nDiff"] = 0
                                    varMap[":nRun"] = 0
                                    varMap[":status"] = "done"
                                    tmpLog.debug(f"set status={varMap[':status']} for datasetID={datasetID}")
                                    self.cur.execute(sqlRD + comment, varMap)
                                # no retry if master dataset successfully finished
                                if commStr == "retry" and nFiles == nFilesFinished:
                                    tmpLog.debug(f"no {commStr} for datasetID={datasetID} : nFiles==nFilesFinished")
                                    continue
                                # count unprocessed files
                                varMap = {}
                                varMap[":jediTaskID"] = jediTaskID
                                varMap[":datasetID"] = datasetID
                                varMap[":status"] = "ready"
                                varMap[":keepTrack"] = 1
                                self.cur.execute(sqlCU + comment, varMap)
                                (nUnp,) = self.cur.fetchone()
                                # update files
                                varMap = {}
                                varMap[":jediTaskID"] = jediTaskID
                                varMap[":datasetID"] = datasetID
                                varMap[":status"] = "ready"
                                varMap[":proc_status"] = "ready"
                                varMap[":maxAttempt"] = maxAttempt
                                varMap[":keepTrack"] = 1
                                nDiff = 0
                                self.cur.execute(sqlRFO + comment, varMap)
                                nDiff += self.cur.rowcount
                                self.cur.execute(sqlRFF + comment, varMap)
                                nDiff += self.cur.rowcount
                                # reset running files
                                varMap = {}
                                varMap[":jediTaskID"] = jediTaskID
                                varMap[":datasetID"] = datasetID
                                varMap[":oldStatus1"] = "picked"
                                if taskOldStatus == "exhausted":
                                    varMap[":oldStatus2"] = "dummy"
                                else:
                                    varMap[":oldStatus2"] = "running"
                                varMap[":newStatus"] = "ready"
                                varMap[":proc_status"] = "ready"
                                varMap[":keepTrack"] = 1
                                varMap[":maxAttempt"] = maxAttempt
                                self.cur.execute(sqlRR + comment, varMap)
                                nRun = self.cur.rowcount
                                # reset ramCount
                                if commStr == "incexec":
                                    varMap = {}
                                    varMap[":jediTaskID"] = jediTaskID
                                    varMap[":datasetID"] = datasetID
                                    varMap[":status"] = "ready"
                                    varMap[":keepTrack"] = 1
                                    self.cur.execute(sqlRRC + comment, varMap)
                                    # reset lost files
                                    varMap = {}
                                    varMap[":jediTaskID"] = jediTaskID
                                    varMap[":datasetID"] = datasetID
                                    varMap[":oldStatus1"] = "lost"
                                    varMap[":oldStatus2"] = "missing"
                                    varMap[":newStatus"] = "ready"
                                    varMap[":proc_status"] = "ready"
                                    varMap[":keepTrack"] = 1
                                    varMap[":maxAttempt"] = maxAttempt
                                    self.cur.execute(sqlRR + comment, varMap)
                                    nLost = self.cur.rowcount
                                    if nLost > 0 and nFilesMissing:
                                        varMap = {}
                                        varMap[":jediTaskID"] = jediTaskID
                                        varMap[":datasetID"] = datasetID
                                        self.cur.execute(sqlRL + comment, varMap)
                                        tmpLog.debug(f"reset nFilesMissing for datasetID={datasetID}")
                                # no retry if no failed files
                                if commStr == "retry" and nDiff == 0 and nUnp == 0 and nRun == 0 and state != "mutable":
                                    tmpLog.debug(f"no {commStr} for datasetID={datasetID} : nDiff/nReady/nRun=0")
                                    continue
                                # count failed files which could be screwed up when files are lost
                                if nDiff == 0 and nRun == 0 and nFilesUsed <= (nFilesFinished + nFilesFailed):
                                    varMap = {}
                                    varMap[":jediTaskID"] = jediTaskID
                                    varMap[":datasetID"] = datasetID
                                    varMap[":status"] = "ready"
                                    varMap[":keepTrack"] = 1
                                    self.cur.execute(sqlCF + comment, varMap)
                                    (newNumFailed,) = self.cur.fetchone()
                                    nDiff = nFilesFailed - newNumFailed
                                    tmpLog.debug(f"got nFilesFailed={newNumFailed} while {nFilesFailed} in DB for datasetID={datasetID}")
                                # update dataset
                                varMap = {}
                                varMap[":jediTaskID"] = jediTaskID
                                varMap[":datasetID"] = datasetID
                                varMap[":nDiff"] = nDiff
                                varMap[":nRun"] = nRun
                                if commStr == "retry":
                                    varMap[":status"] = "ready"
                                    tmpLog.debug(f"set status={varMap[':status']} for datasetID={datasetID} diff={nDiff}")
                                elif commStr == "incexec":
                                    varMap[":status"] = "toupdate"
                                self.cur.execute(sqlRD + comment, varMap)
                                # collect masterIDs
                                changedMasterList.append(datasetID)
                                # release unstaged
                                if release_unstaged:
                                    get_task_utils_module(self).updateInputDatasetsStaged_JEDI(
                                        jediTaskID, datasetName.split(":")[0], [datasetName.split(":")[-1]], use_commit=False, by="retry"
                                    )
                        # update secondary
                        for masterID in changedMasterList:
                            # no seconday
                            if masterID not in secMap:
                                continue
                            # loop over all datasets
                            for datasetID, nFilesFinished, status, state, datasetType in secMap[masterID]:
                                # update files
                                varMap = {}
                                varMap[":jediTaskID"] = jediTaskID
                                varMap[":datasetID"] = datasetID
                                varMap[":status"] = "ready"
                                varMap[":proc_status"] = "ready"
                                varMap[":maxAttempt"] = maxAttempt
                                varMap[":keepTrack"] = 1
                                nDiff = 0
                                self.cur.execute(sqlRFO + comment, varMap)
                                nDiff += self.cur.rowcount
                                self.cur.execute(sqlRFF + comment, varMap)
                                nDiff += self.cur.rowcount
                                # reset running files
                                varMap = {}
                                varMap[":jediTaskID"] = jediTaskID
                                varMap[":datasetID"] = datasetID
                                varMap[":oldStatus1"] = "picked"
                                if taskOldStatus == "exhausted":
                                    varMap[":oldStatus2"] = "dummy"
                                else:
                                    varMap[":oldStatus2"] = "running"
                                varMap[":newStatus"] = "ready"
                                varMap[":proc_status"] = "ready"
                                varMap[":keepTrack"] = 1
                                varMap[":maxAttempt"] = maxAttempt
                                self.cur.execute(sqlRR + comment, varMap)
                                nRun = self.cur.rowcount
                                # reset ramCount
                                if commStr == "incexec":
                                    varMap = {}
                                    varMap[":jediTaskID"] = jediTaskID
                                    varMap[":datasetID"] = datasetID
                                    varMap[":status"] = "ready"
                                    varMap[":keepTrack"] = 1
                                    self.cur.execute(sqlRRC + comment, varMap)
                                    # reset lost files
                                    varMap = {}
                                    varMap[":jediTaskID"] = jediTaskID
                                    varMap[":datasetID"] = datasetID
                                    varMap[":oldStatus1"] = "lost"
                                    varMap[":oldStatus2"] = "missing"
                                    varMap[":newStatus"] = "ready"
                                    varMap[":proc_status"] = "ready"
                                    varMap[":keepTrack"] = 1
                                    varMap[":maxAttempt"] = maxAttempt
                                    self.cur.execute(sqlRR + comment, varMap)
                                    nLost = self.cur.rowcount
                                    if nLost > 0 and nFilesMissing:
                                        varMap = {}
                                        varMap[":jediTaskID"] = jediTaskID
                                        varMap[":datasetID"] = datasetID
                                        self.cur.execute(sqlRL + comment, varMap)
                                        tmpLog.debug(f"reset nFilesMissing for datasetID={datasetID}")
                                # update dataset
                                varMap = {}
                                varMap[":jediTaskID"] = jediTaskID
                                varMap[":datasetID"] = datasetID
                                varMap[":nDiff"] = nDiff
                                varMap[":nRun"] = nRun
                                if commStr == "incexec" and datasetType == "input":
                                    varMap[":status"] = "toupdate"
                                else:
                                    varMap[":status"] = "ready"
                                tmpLog.debug(f"set status={varMap[':status']} for associated 2nd datasetID={datasetID}")
                                self.cur.execute(sqlRD + comment, varMap)
                        # discard events
                        if discardEvents:
                            varMap = {}
                            varMap[":jediTaskID"] = jediTaskID
                            varMap[":newStatus"] = EventServiceUtils.ST_discarded
                            varMap[":esDone"] = EventServiceUtils.ST_done
                            varMap[":esFinished"] = EventServiceUtils.ST_finished
                            self.cur.execute(sqlDE + comment, varMap)
                            nDE = self.cur.rowcount
                            tmpLog.debug(f"discarded {nDE} events")
                        # update task
                        if commStr == "retry":
                            if changedMasterList != [] or taskOldStatus == "exhausted":
                                newTaskStatus = JediTaskSpec.commandStatusMap()[commStr]["done"]
                            else:
                                # to finalization since no files left in ready status
                                msgStr = f"no {commStr} since no new/unprocessed files available"
                                tmpLog.debug(msgStr)
                                newTaskStatus = taskOldStatus
                                newErrorDialog = msgStr
                        else:
                            # for incremental execution
                            newTaskStatus = JediTaskSpec.commandStatusMap()[commStr]["done"]
                # update task
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                varMap[":status"] = newTaskStatus
                varMap[":errorDialog"] = newErrorDialog
                if newTaskStatus != taskOldStatus and newTaskStatus != "exhausted":
                    tmpLog.debug(f"set taskStatus={newTaskStatus} from {taskStatus} for command={commStr}")
                    # set old update time to trigger subsequent process
                    varMap[":updateTime"] = naive_utcnow() - datetime.timedelta(hours=6)
                    self.cur.execute(sqlUTN + comment, varMap)
                    deftStatus = "ready"
                    self.setSuperStatus_JEDI(jediTaskID, deftStatus)
                    # update DEFT
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    varMap[":status"] = deftStatus
                    self.cur.execute(sqlTT + comment, varMap)
                    # task status log
                    self.record_task_status_change(jediTaskID)
                    self.push_task_status_message(None, jediTaskID, newTaskStatus)
                    # task attempt start log
                    get_task_utils_module(self).log_task_attempt_start(jediTaskID)
                    retried_tasks.append(jediTaskID)
                else:
                    tmpLog.debug(f"back to taskStatus={newTaskStatus} for command={commStr}")
                    varMap[":updateTime"] = naive_utcnow()
                    self.cur.execute(sqlUTB + comment, varMap)
                    if newTaskStatus == "exhausted":
                        self.setDeftStatus_JEDI(jediTaskID, newTaskStatus)
                        self.setSuperStatus_JEDI(jediTaskID, newTaskStatus)
                        self.record_task_status_change(jediTaskID)
                # update output/lib/log
                if newTaskStatus != taskOldStatus and taskStatus != "exhausted" and newTaskStatus != "exhausted":
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    varMap[":type1"] = "output"
                    varMap[":type2"] = "lib"
                    varMap[":type3"] = "log"
                    varMap[":status"] = "done"
                    self.cur.execute(sqlUO + comment, varMap)
                # retry or reactivate child tasks
                if retryChildTasks and newTaskStatus != taskOldStatus and taskStatus != "exhausted" and newTaskStatus != "exhausted":
                    _, tmp_retried_tasks = self.retryChildTasks_JEDI(jediTaskID, keep_share_priority=keep_share_priority, useCommit=False)
                    retried_tasks += tmp_retried_tasks
            if useCommit:
                # commit
                if not self._commit():
                    raise RuntimeError("Commit error")
            # return
            tmpLog.debug("done")
            return True, newTaskStatus, retried_tasks
        except Exception:
            if useCommit:
                # roll back
                self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return None, None, retried_tasks

    # retry child tasks
    def retryChildTasks_JEDI(self, jediTaskID, keep_share_priority=False, ignore_hard_exhausted=False, useCommit=True):
        comment = " /* JediDBProxy.retryChildTasks_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID}")
        tmpLog.debug("start")
        retried_tasks = []
        try:
            # sql to get output datasets of parent task
            sqlPD = f"SELECT datasetName,containerName FROM {panda_config.schemaJEDI}.JEDI_Datasets "
            sqlPD += "WHERE jediTaskID=:jediTaskID AND type IN (:type1,:type2) "
            # sql to get child tasks
            sqlGT = f"SELECT jediTaskID,status FROM {panda_config.schemaJEDI}.JEDI_Tasks "
            sqlGT += "WHERE parent_tid=:jediTaskID AND parent_tid<>jediTaskID "
            # sql to get input datasets of child task
            sqlRD = f"SELECT datasetID,datasetName FROM {panda_config.schemaJEDI}.JEDI_Datasets "
            sqlRD += f"WHERE jediTaskID=:jediTaskID AND type IN ({PROCESS_TYPES_var_str}) "
            # sql to change task status
            sqlCT = f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks "
            sqlCT += "SET status=:status,errorDialog=NULL,stateChangeTime=CURRENT_DATE "
            sqlCT += "WHERE jediTaskID=:jediTaskID "
            # sql to set mutable to dataset status
            sqlMD = f"UPDATE {panda_config.schemaJEDI}.JEDI_Datasets "
            sqlMD += "SET state=:state,stateCheckTime=CURRENT_DATE "
            sqlMD += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            # sql to set dataset status
            sqlCD = f"UPDATE {panda_config.schemaJEDI}.JEDI_Datasets "
            sqlCD += "SET status=:status "
            sqlCD += "WHERE jediTaskID=:jediTaskID AND type=:type "
            # begin transaction
            if useCommit:
                self.conn.begin()
            # get output datasets of parent task
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            varMap[":type1"] = "output"
            varMap[":type2"] = "log"
            self.cur.execute(sqlPD + comment, varMap)
            resList = self.cur.fetchall()
            parentDatasets = set()
            for tmpDS, tmpDC in resList:
                parentDatasets.add(tmpDS)
                parentDatasets.add(tmpDC)
            # get child tasks
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            self.cur.execute(sqlGT + comment, varMap)
            resList = self.cur.fetchall()
            for cJediTaskID, cTaskStatus in resList:
                # not to retry if child task is aborted/broken
                if cTaskStatus in ["aborted", "toabort", "aborting", "broken", "tobroken", "failed"]:
                    tmpLog.debug(f"not to retry child jediTaskID={cJediTaskID} in {cTaskStatus}")
                    continue
                # get input datasets of child task
                varMap = {}
                varMap[":jediTaskID"] = cJediTaskID
                varMap.update(PROCESS_TYPES_var_map)
                self.cur.execute(sqlRD + comment, varMap)
                dsList = self.cur.fetchall()
                inputReady = False
                for datasetID, datasetName in dsList:
                    # set dataset status to mutable
                    if datasetName in parentDatasets or datasetName.split(":")[-1] in parentDatasets:
                        varMap = {}
                        varMap[":jediTaskID"] = cJediTaskID
                        varMap[":datasetID"] = datasetID
                        varMap[":state"] = "mutable"
                        self.cur.execute(sqlMD + comment, varMap)
                        inputReady = True
                # set task status
                if not inputReady:
                    # set task status to registered since dataset is not ready
                    varMap = {}
                    varMap[":jediTaskID"] = cJediTaskID
                    varMap[":status"] = "registered"
                    self.cur.execute(sqlCT + comment, varMap)
                    # add missing record_task_status_change and push_task_status_message updates
                    self.record_task_status_change(cJediTaskID)
                    self.push_task_status_message(None, cJediTaskID, varMap[":status"])
                    tmpLog.debug(f"set status of child jediTaskID={cJediTaskID} to {varMap[':status']}")
                elif cTaskStatus not in ["ready", "running", "scouting", "scouted"]:
                    # incexec child task
                    tmpLog.debug(f"incremental execution for child jediTaskID={cJediTaskID}")
                    _, _, tmp_retried_tasks = self.retryTask_JEDI(
                        cJediTaskID,
                        "incexec",
                        useCommit=False,
                        statusCheck=False,
                        keep_share_priority=keep_share_priority,
                        ignore_hard_exhausted=ignore_hard_exhausted,
                    )
                    retried_tasks += tmp_retried_tasks
            # commit
            if useCommit:
                if not self._commit():
                    raise RuntimeError("Commit error")
            # return
            tmpLog.debug("done")
            return True, retried_tasks
        except Exception:
            # roll back
            if useCommit:
                self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return False, retried_tasks

    # record retry history
    def recordRetryHistory_JEDI(self, jediTaskID, oldNewPandaIDs, relationType):
        comment = " /* JediDBProxy.recordRetryHistory_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID}")
        tmpLog.debug("start")
        try:
            sqlIN = f"INSERT INTO {panda_config.schemaJEDI}.JEDI_Job_Retry_History "
            if relationType is None:
                sqlIN += "(jediTaskID,oldPandaID,newPandaID,originPandaID) "
                sqlIN += "VALUES(:jediTaskID,:oldPandaID,:newPandaID,:originPandaID) "
            else:
                sqlIN += "(jediTaskID,oldPandaID,newPandaID,originPandaID,relationType) "
                sqlIN += "VALUES(:jediTaskID,:oldPandaID,:newPandaID,:originPandaID,:relationType) "
            # start transaction
            self.conn.begin()
            for newPandaID, oldPandaIDs in oldNewPandaIDs.items():
                for oldPandaID in oldPandaIDs:
                    # get origin
                    originIDs = get_job_complex_module(self).getOriginPandaIDsJEDI(oldPandaID, jediTaskID, self.cur)
                    for originID in originIDs:
                        varMap = {}
                        varMap[":jediTaskID"] = jediTaskID
                        varMap[":oldPandaID"] = oldPandaID
                        varMap[":newPandaID"] = newPandaID
                        varMap[":originPandaID"] = originID
                        if relationType is not None:
                            varMap[":relationType"] = relationType
                        self.cur.execute(sqlIN + comment, varMap)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            tmpLog.debug("done")
            return True
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return False

    # update input files stage-in done (according to message from iDDS, called by other methods, etc.)
    def updateInputFilesStaged_JEDI(self, jeditaskid, scope, filenames_dict, chunk_size=500, by=None, check_scope=True):
        comment = " /* JediDBProxy.updateInputFilesStaged_JEDI */"
        tmp_tag = f"jediTaskID={jeditaskid}"
        if by:
            tmp_tag += f" by={by}"
        tmpLog = self.create_tagged_logger(comment, tmp_tag)
        tmpLog.debug("start")
        try:
            to_update_files = True
            retVal = 0
            # varMap
            varMap = dict()
            varMap[":jediTaskID"] = jeditaskid
            varMap[":type1"] = "input"
            varMap[":type2"] = "pseudo_input"
            # sql to get datasetIDs
            sqlGD = f"SELECT datasetID,masterID FROM {panda_config.schemaJEDI}.JEDI_Datasets WHERE jediTaskID=:jediTaskID AND type IN (:type1,:type2) "
            # sql to update file counts
            if scope != "pseudo_dataset":
                sqlUF = (
                    f"UPDATE {panda_config.schemaJEDI}.JEDI_Dataset_Contents "
                    f"SET status=:new_status "
                    f"WHERE jediTaskID=:jediTaskID "
                    f"AND status=:old_status "
                )
                sqlUF_with_lfn = sqlUF + "AND lfn=:lfn "
                if check_scope:
                    sqlUF_with_lfn += "AND scope=:scope "
                sqlUF_with_fileID = sqlUF + "AND fileID=:fileID "
            else:
                sqlUF = (
                    f"UPDATE {panda_config.schemaJEDI}.JEDI_Dataset_Contents "
                    f"SET status=:new_status "
                    f"WHERE jediTaskID=:jediTaskID "
                    f"AND status=:old_status "
                    f"AND scope IS NULL "
                )
                sqlUF_with_lfn = sqlUF + "AND lfn like :lfn "
                sqlUF_with_fileID = sqlUF + "AND fileID=:fileID "
            sqlUF_with_datasetID = sqlUF_with_lfn + "AND datasetID=:datasetID "
            # begin transaction
            self.conn.begin()
            # get datasetIDs from DB if no fileID nor datasetID provided by the message
            tmpLog.debug(f"running sql: {sqlGD} {varMap}")
            self.cur.execute(sqlGD + comment, varMap)
            varMap = dict()
            varMap[":jediTaskID"] = jeditaskid
            if scope != "pseudo_dataset" and check_scope:
                varMap[":scope"] = scope
            varMap[":old_status"] = "staging"
            varMap[":new_status"] = "pending"
            resGD = self.cur.fetchall()
            primaryID = None
            params_key_list = []
            var_map_datasetids = {}
            dsid_var_names_str = ""
            if len(resGD) > 0:
                dsid_var_names_str, dsid_var_map = get_sql_IN_bind_variables([dsid for (dsid, masterID) in resGD], prefix=":datasetID_")
                var_map_datasetids.update(dsid_var_map)
            else:
                to_update_files = False
            # set sqls to update file counts
            datesetid_list_str = f"AND datasetID IN ({dsid_var_names_str}) "
            sqlUF_without_ID = sqlUF_with_lfn + datesetid_list_str
            # update files
            if to_update_files:
                # split into groups according to whether with ids
                filenames_dict_with_fileID = {}
                filenames_dict_with_datasetID = {}
                filenames_dict_without_ID = {}
                for filename, (datasetid, fileid) in filenames_dict.items():
                    if fileid is not None:
                        # with fileID from message
                        filenames_dict_with_fileID[filename] = (datasetid, fileid)
                    elif datasetid is not None:
                        # with datasetID from message
                        filenames_dict_with_datasetID[filename] = (datasetid, fileid)
                    else:
                        # without datasetID from message
                        filenames_dict_without_ID[filename] = (datasetid, fileid)
                # loop over files with fileID
                if filenames_dict_with_fileID:
                    for one_batch in batched(filenames_dict_with_fileID.items(), chunk_size):
                        # loop batches of executemany
                        varMaps = []
                        for filename, (datasetid, fileid) in one_batch:
                            tmp_varMap = varMap.copy()
                            tmp_varMap[":fileID"] = fileid
                            if ":scope" in tmp_varMap:
                                del tmp_varMap[":scope"]
                            varMaps.append(tmp_varMap)
                            tmpLog.debug(f"tmp_varMap: {tmp_varMap}")
                        tmpLog.debug(f"running sql executemany: {sqlUF_with_fileID} for {len(varMaps)} items")
                        self.cur.executemany(sqlUF_with_fileID + comment, varMaps)
                        retVal += self.cur.rowcount
                # loop over files with datasetID
                if filenames_dict_with_datasetID:
                    for one_batch in batched(filenames_dict_with_datasetID.items(), chunk_size):
                        # loop batches of executemany
                        varMaps = []
                        for filename, (datasetid, fileid) in one_batch:
                            tmp_varMap = varMap.copy()
                            if scope != "pseudo_dataset":
                                tmp_varMap[":lfn"] = filename
                            else:
                                tmp_varMap[":lfn"] = "%" + filename
                            tmp_varMap[":datasetID"] = datasetid
                            varMaps.append(tmp_varMap)
                            tmpLog.debug(f"tmp_varMap: {tmp_varMap}")
                        tmpLog.debug(f"running sql executemany: {sqlUF_with_datasetID} for {len(varMaps)} items")
                        self.cur.executemany(sqlUF_with_datasetID + comment, varMaps)
                        retVal += self.cur.rowcount
                # loop over files without ID
                if filenames_dict_without_ID:
                    for one_batch in batched(filenames_dict_without_ID.items(), chunk_size):
                        # loop batches of executemany
                        varMaps = []
                        for filename, (datasetid, fileid) in one_batch:
                            tmp_varMap = varMap.copy()
                            if scope != "pseudo_dataset":
                                tmp_varMap[":lfn"] = filename
                            else:
                                tmp_varMap[":lfn"] = "%" + filename
                            tmp_varMap.update(var_map_datasetids)
                            varMaps.append(tmp_varMap)
                            tmpLog.debug(f"tmp_varMap: {tmp_varMap}")
                        tmpLog.debug(f"running sql executemany: {sqlUF_without_ID} for {len(varMaps)} items")
                        self.cur.executemany(sqlUF_without_ID + comment, varMaps)
                        retVal += self.cur.rowcount
            # update associated files
            if primaryID is not None:
                get_task_utils_module(self).fix_associated_files_in_staging(jeditaskid, primary_id=primaryID)
            # update task to trigger CF immediately
            if retVal:
                sqlUT = f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks SET modificationTime=CURRENT_DATE-1 WHERE jediTaskID=:jediTaskID AND lockedBy IS NULL "
                varMap = dict()
                varMap[":jediTaskID"] = jeditaskid
                self.cur.execute(sqlUT + comment, varMap)
                tmpLog.debug(f"unlocked task with {self.cur.rowcount}")
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmpLog.debug(f"updated {retVal} files")
            return retVal
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return None

    # close and reassign N jobs of a preassigned task
    def reassignJobsInPreassignedTask_JEDI(self, jedi_taskid, site, n_jobs_to_close):
        comment = " /* JediDBProxy.reassignJobsInPreassignedTask_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"jediTaskID={jedi_taskid} to {site} to close {n_jobs_to_close} jobs")
        tmpLog.debug("start")
        try:
            self.conn.begin()
            # check if task is still running and brokered to the site
            sqlT = (
                "SELECT jediTaskID " "FROM {0}.JEDI_Tasks t " "WHERE t.jediTaskID=:jediTaskID " "AND t.site =:site " "AND t.status IN ('ready','running') "
            ).format(panda_config.schemaJEDI)
            varMap = {}
            varMap[":jediTaskID"] = jedi_taskid
            varMap[":site"] = site
            self.cur.execute(sqlT + comment, varMap)
            resT = self.cur.fetchall()
            if not resT:
                # skip as preassigned task not running and brokered
                tmpLog.debug("no longer brokered to site or not ready/running ; skipped")
                return None
            # close jobs
            sqlJC = (
                "SELECT pandaID " "FROM {0}.jobsActive4 " "WHERE jediTaskID=:jediTaskID " "AND jobStatus='activated' " "AND computingSite!=:computingSite "
            ).format(panda_config.schemaPANDA)
            varMap = {}
            varMap[":jediTaskID"] = jedi_taskid
            varMap[":computingSite"] = site
            self.cur.execute(sqlJC + comment, varMap)
            pandaIDs = self.cur.fetchall()
            n_jobs_closed = 0
            for (pandaID,) in pandaIDs:
                res_close = get_job_complex_module(self).killJob(pandaID, "reassign", "51", True)
                if res_close:
                    n_jobs_closed += 1
                if n_jobs_closed >= n_jobs_to_close:
                    break
            tmpLog.debug(f"closed {n_jobs_closed} jobs")
            return n_jobs_closed
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)

    # register task/dataset/templ/param in a single transaction
    def registerTaskInOneShot_JEDI(
        self,
        jediTaskID,
        taskSpec,
        inMasterDatasetSpecList,
        inSecDatasetSpecList,
        outDatasetSpecList,
        outputTemplateMap,
        jobParamsTemplate,
        taskParams,
        unmergeMasterDatasetSpec,
        unmergeDatasetSpecMap,
        uniqueTaskName,
        oldTaskStatus,
    ):
        comment = " /* JediDBProxy.registerTaskInOneShot_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID}")
        tmpLog.debug("start")
        try:
            timeNow = naive_utcnow()
            # set attributes
            if taskSpec.status not in ["topreprocess"]:
                taskSpec.status = "defined"
            tmpLog.debug(f"taskStatus={taskSpec.status}")
            taskSpec.modificationTime = timeNow
            taskSpec.resetChangedAttr("jediTaskID")
            # begin transaction
            self.conn.begin()
            # check duplication
            duplicatedFlag = False
            if uniqueTaskName is True:
                sqlDup = f"SELECT jediTaskID FROM {panda_config.schemaJEDI}.JEDI_Tasks "
                sqlDup += "WHERE userName=:userName AND taskName=:taskName AND jediTaskID<>:jediTaskID FOR UPDATE "
                varMap = {}
                varMap[":userName"] = taskSpec.userName
                varMap[":taskName"] = taskSpec.taskName
                varMap[":jediTaskID"] = jediTaskID
                self.cur.execute(sqlDup + comment, varMap)
                resDupList = self.cur.fetchall()
                tmpErrStr = ""
                for (tmpJediTaskID,) in resDupList:
                    duplicatedFlag = True
                    tmpErrStr += f"{tmpJediTaskID},"
                if duplicatedFlag:
                    taskSpec.status = "toabort"
                    tmpErrStr = tmpErrStr[:-1]
                    tmpErrStr = f"{taskSpec.status} since there is duplicated task -> jediTaskID={tmpErrStr}"
                    taskSpec.setErrDiag(tmpErrStr)
                    # reset task name
                    taskSpec.taskName = None
                    tmpLog.debug(tmpErrStr)
            # update task
            varMap = taskSpec.valuesMap(useSeq=False, onlyChanged=True)
            varMap[":jediTaskID"] = jediTaskID
            varMap[":preStatus"] = oldTaskStatus
            sql = f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks SET {taskSpec.bindUpdateChangesExpression()} WHERE "
            sql += "jediTaskID=:jediTaskID AND status=:preStatus "
            self.cur.execute(sql + comment, varMap)
            nRow = self.cur.rowcount
            tmpLog.debug(f"update {nRow} row in task table")
            if nRow != 1:
                tmpLog.error("the task not found in task table or already registered")
            elif duplicatedFlag:
                pass
            else:
                # delete unknown datasets
                tmpLog.debug("deleting unknown datasets")
                sql = f"DELETE FROM {panda_config.schemaJEDI}.JEDI_Datasets "
                sql += "WHERE jediTaskID=:jediTaskID AND type=:type "
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                varMap[":type"] = JediDatasetSpec.getUnknownInputType()
                self.cur.execute(sql + comment, varMap)
                tmpLog.debug("inserting datasets")
                # sql to insert datasets
                sql = f"INSERT INTO {panda_config.schemaJEDI}.JEDI_Datasets ({JediDatasetSpec.columnNames()}) "
                sql += JediDatasetSpec.bindValuesExpression()
                sql += " RETURNING datasetID INTO :newDatasetID"
                # sql to insert files
                sqlI = f"INSERT INTO {panda_config.schemaJEDI}.JEDI_Dataset_Contents ({JediFileSpec.columnNames()}) "
                sqlI += JediFileSpec.bindValuesExpression()
                # insert master dataset
                masterID = -1
                datasetIdMap = {}
                for datasetSpec in inMasterDatasetSpecList:
                    if datasetSpec is not None:
                        datasetSpec.creationTime = timeNow
                        datasetSpec.modificationTime = timeNow
                        varMap = datasetSpec.valuesMap(useSeq=True)
                        varMap[":newDatasetID"] = self.cur.var(varNUMBER)
                        # insert dataset
                        self.cur.execute(sql + comment, varMap)
                        val = self.getvalue_corrector(self.cur.getvalue(varMap[":newDatasetID"]))
                        datasetID = int(val)
                        masterID = datasetID
                        datasetIdMap[datasetSpec.uniqueMapKey()] = datasetID
                        datasetSpec.datasetID = datasetID
                        # insert files
                        for fileSpec in datasetSpec.Files:
                            fileSpec.datasetID = datasetID
                            fileSpec.creationDate = timeNow
                            varMap = fileSpec.valuesMap(useSeq=True)
                            self.cur.execute(sqlI + comment, varMap)
                    # insert secondary datasets
                    for datasetSpec in inSecDatasetSpecList:
                        datasetSpec.creationTime = timeNow
                        datasetSpec.modificationTime = timeNow
                        datasetSpec.masterID = masterID
                        varMap = datasetSpec.valuesMap(useSeq=True)
                        varMap[":newDatasetID"] = self.cur.var(varNUMBER)
                        # insert dataset
                        self.cur.execute(sql + comment, varMap)
                        val = self.getvalue_corrector(self.cur.getvalue(varMap[":newDatasetID"]))
                        datasetID = int(val)
                        datasetIdMap[datasetSpec.uniqueMapKey()] = datasetID
                        datasetSpec.datasetID = datasetID
                        # insert files
                        for fileSpec in datasetSpec.Files:
                            fileSpec.datasetID = datasetID
                            fileSpec.creationDate = timeNow
                            varMap = fileSpec.valuesMap(useSeq=True)
                            self.cur.execute(sqlI + comment, varMap)
                # insert unmerged master dataset
                unmergeMasterID = -1
                for datasetSpec in unmergeMasterDatasetSpec.values():
                    datasetSpec.creationTime = timeNow
                    datasetSpec.modificationTime = timeNow
                    varMap = datasetSpec.valuesMap(useSeq=True)
                    varMap[":newDatasetID"] = self.cur.var(varNUMBER)
                    # insert dataset
                    self.cur.execute(sql + comment, varMap)
                    val = self.getvalue_corrector(self.cur.getvalue(varMap[":newDatasetID"]))
                    datasetID = int(val)
                    datasetIdMap[datasetSpec.outputMapKey()] = datasetID
                    datasetSpec.datasetID = datasetID
                    unmergeMasterID = datasetID
                # insert unmerged output datasets
                for datasetSpec in unmergeDatasetSpecMap.values():
                    datasetSpec.creationTime = timeNow
                    datasetSpec.modificationTime = timeNow
                    datasetSpec.masterID = unmergeMasterID
                    varMap = datasetSpec.valuesMap(useSeq=True)
                    varMap[":newDatasetID"] = self.cur.var(varNUMBER)
                    # insert dataset
                    self.cur.execute(sql + comment, varMap)
                    val = self.getvalue_corrector(self.cur.getvalue(varMap[":newDatasetID"]))
                    datasetID = int(val)
                    datasetIdMap[datasetSpec.outputMapKey()] = datasetID
                    datasetSpec.datasetID = datasetID
                # insert output datasets
                for datasetSpec in outDatasetSpecList:
                    datasetSpec.creationTime = timeNow
                    datasetSpec.modificationTime = timeNow
                    # keep original outputMapKey since provenanceID may change
                    outputMapKey = datasetSpec.outputMapKey()
                    # associate to unmerged dataset
                    if datasetSpec.outputMapKey() in unmergeMasterDatasetSpec:
                        datasetSpec.provenanceID = unmergeMasterDatasetSpec[datasetSpec.outputMapKey()].datasetID
                    elif datasetSpec.outputMapKey() in unmergeDatasetSpecMap:
                        datasetSpec.provenanceID = unmergeDatasetSpecMap[datasetSpec.outputMapKey()].datasetID
                    varMap = datasetSpec.valuesMap(useSeq=True)
                    varMap[":newDatasetID"] = self.cur.var(varNUMBER)
                    # insert dataset
                    self.cur.execute(sql + comment, varMap)
                    val = self.getvalue_corrector(self.cur.getvalue(varMap[":newDatasetID"]))
                    datasetID = int(val)
                    datasetIdMap[outputMapKey] = datasetID
                    datasetSpec.datasetID = datasetID
                # insert outputTemplates
                tmpLog.debug("inserting outTmpl")
                for outputMapKey, outputTemplateList in outputTemplateMap.items():
                    if outputMapKey not in datasetIdMap:
                        raise RuntimeError(f"datasetID is not defined for {outputMapKey}")
                    for outputTemplate in outputTemplateList:
                        sqlH = f"INSERT INTO {panda_config.schemaJEDI}.JEDI_Output_Template (outTempID,datasetID,"
                        sqlL = f"VALUES({panda_config.schemaJEDI}.JEDI_OUTPUT_TEMPLATE_ID_SEQ.nextval,:datasetID,"
                        varMap = {}
                        varMap[":datasetID"] = datasetIdMap[outputMapKey]
                        for tmpAttr, tmpVal in outputTemplate.items():
                            tmpKey = ":" + tmpAttr
                            sqlH += f"{tmpAttr},"
                            sqlL += f"{tmpKey},"
                            varMap[tmpKey] = tmpVal
                        sqlH = sqlH[:-1] + ") "
                        sqlL = sqlL[:-1] + ") "
                        sql = sqlH + sqlL
                        self.cur.execute(sql + comment, varMap)
                # check if jobParams is already there
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                sql = f"SELECT jediTaskID FROM {panda_config.schemaJEDI}.JEDI_JobParams_Template "
                sql += "WHERE jediTaskID=:jediTaskID "
                self.cur.execute(sql + comment, varMap)
                resPar = self.cur.fetchone()
                if resPar is None:
                    # insert job parameters
                    tmpLog.debug("inserting jobParamsTmpl")
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    varMap[":templ"] = jobParamsTemplate
                    sql = f"INSERT INTO {panda_config.schemaJEDI}.JEDI_JobParams_Template "
                    sql += "(jediTaskID,jobParamsTemplate) VALUES (:jediTaskID,:templ) "
                else:
                    tmpLog.debug("replacing jobParamsTmpl")
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    varMap[":templ"] = jobParamsTemplate
                    sql = f"UPDATE {panda_config.schemaJEDI}.JEDI_JobParams_Template "
                    sql += "SET jobParamsTemplate=:templ WHERE jediTaskID=:jediTaskID"
                self.cur.execute(sql + comment, varMap)
                # update task parameters
                if taskParams is not None:
                    tmpLog.debug("updating taskParams")
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    varMap[":taskParams"] = taskParams
                    sql = f"UPDATE {panda_config.schemaJEDI}.JEDI_TaskParams SET taskParams=:taskParams "
                    sql += "WHERE jediTaskID=:jediTaskID "
                    self.cur.execute(sql + comment, varMap)
            # task status logging
            self.record_task_status_change(taskSpec.jediTaskID)
            self.push_task_status_message(taskSpec, taskSpec.jediTaskID, taskSpec.status)
            # task attempt start log
            get_task_utils_module(self).log_task_attempt_start(taskSpec.jediTaskID)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmpLog.debug("done")
            return True, taskSpec.status
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return False, "tobroken"

    # generate output files for task, and instantiate template datasets if necessary
    def getOutputFiles_JEDI(
        self,
        jediTaskID,
        provenanceID,
        simul,
        instantiateTmpl,
        instantiatedSites,
        isUnMerging,
        isPrePro,
        xmlConfigJob,
        siteDsMap,
        middleName,
        registerDatasets,
        parallelOutMap,
        fileIDPool,
        n_files_per_chunk=1,
        bulk_fetch_for_multiple_jobs=False,
        master_dataset_id=None,
    ):
        comment = " /* JediDBProxy.getOutputFiles_JEDI */"
        if master_dataset_id:
            tmpLog = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID} datasetID={master_dataset_id}")
        else:
            tmpLog = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID}")
        tmpLog.debug(f"start with simul={simul} instantiateTmpl={instantiateTmpl} instantiatedSites={instantiatedSites}")
        tmpLog.debug(f"isUnMerging={isUnMerging} isPrePro={isPrePro} provenanceID={provenanceID} xmlConfigJob={type(xmlConfigJob)}")
        tmpLog.debug(f"middleName={middleName} registerDatasets={registerDatasets} idPool={len(fileIDPool)}")
        tmpLog.debug(f"n_files_per_chunk={n_files_per_chunk} bulk_fetch={bulk_fetch_for_multiple_jobs}")
        try:
            if instantiatedSites is None:
                instantiatedSites = ""
            if siteDsMap is None:
                siteDsMap = {}
            if parallelOutMap is None:
                parallelOutMap = {}
            outMap = {}
            datasetToRegister = []
            indexFileID = 0
            fetched_serial_ids = 0
            maxSerialNr = None
            output_map_for_bulk_fetch = [{} for _ in range(n_files_per_chunk)]
            parallel_out_map_for_bulk_fetch = [{} for _ in range(n_files_per_chunk)]
            max_serial_numbers_for_bulk_fetch = [None] * n_files_per_chunk
            # sql to get dataset
            sqlD = "SELECT "
            sqlD += f"datasetID,datasetName,vo,masterID,status,type FROM {panda_config.schemaJEDI}.JEDI_Datasets "
            sqlD += "WHERE jediTaskID=:jediTaskID AND type IN (:type1,:type2) "
            if provenanceID is not None:
                sqlD += "AND (provenanceID IS NULL OR provenanceID=:provenanceID) "
            # sql to read template
            sqlR = "SELECT outTempID,datasetID,fileNameTemplate,serialNr,outType,streamName "
            sqlR += f"FROM {panda_config.schemaJEDI}.JEDI_Output_Template "
            sqlR += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            if not simul:
                sqlR += "FOR UPDATE "
            # sql to insert files
            sqlI = f"INSERT INTO {panda_config.schemaJEDI}.JEDI_Dataset_Contents ({JediFileSpec.columnNames()}) "
            sqlI += JediFileSpec.bindValuesExpression()
            sqlI += " RETURNING fileID INTO :newFileID"
            # sql to insert files without fileID
            sqlII = f"INSERT INTO {panda_config.schemaJEDI}.JEDI_Dataset_Contents ({JediFileSpec.columnNames()}) "
            sqlII += JediFileSpec.bindValuesExpression(useSeq=False)
            # sql to increment SN
            sqlU = f"UPDATE {panda_config.schemaJEDI}.JEDI_Output_Template SET serialNr=serialNr+:diff "
            sqlU += "WHERE jediTaskID=:jediTaskID AND outTempID=:outTempID "
            # sql to instantiate template dataset
            sqlT1 = f"SELECT {JediDatasetSpec.columnNames()} FROM {panda_config.schemaJEDI}.JEDI_Datasets "
            sqlT1 += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            sqlT2 = f"INSERT INTO {panda_config.schemaJEDI}.JEDI_Datasets ({JediDatasetSpec.columnNames()}) "
            sqlT2 += JediDatasetSpec.bindValuesExpression()
            sqlT2 += "RETURNING datasetID INTO :newDatasetID "
            # sql to change concrete dataset name
            sqlCN = f"UPDATE {panda_config.schemaJEDI}.JEDI_Datasets "
            sqlCN += "SET site=:site,datasetName=:datasetName,destination=:destination "
            sqlCN += " WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            # sql to set masterID to concrete datasets
            sqlMC = f"UPDATE {panda_config.schemaJEDI}.JEDI_Datasets "
            sqlMC += "SET masterID=:masterID "
            sqlMC += " WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            # current current date
            timeNow = naive_utcnow()
            # begin transaction
            self.conn.begin()
            self.cur.arraysize = 100
            # get datasets
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            varMap[":type1"] = "output"
            varMap[":type2"] = "log"
            # unmerged datasets
            if isUnMerging:
                varMap[":type1"] = "trn_" + varMap[":type1"]
                varMap[":type2"] = "trn_" + varMap[":type2"]
            elif isPrePro:
                varMap[":type1"] = "pp_" + varMap[":type1"]
                varMap[":type2"] = "pp_" + varMap[":type2"]
            # template datasets
            if instantiateTmpl:
                varMap[":type1"] = "tmpl_" + varMap[":type1"]
                varMap[":type2"] = "tmpl_" + varMap[":type2"]
            # keep dataset types
            tmpl_VarMap = {}
            tmpl_VarMap[":type1"] = varMap[":type1"]
            tmpl_VarMap[":type2"] = varMap[":type2"]
            if provenanceID is not None:
                varMap[":provenanceID"] = provenanceID
            self.cur.execute(sqlD + comment, varMap)
            resList = self.cur.fetchall()
            tmpl_RelationMap = {}
            mstr_RelationMap = {}
            varMapsForInsert = []
            varMapsForSN = []
            for datasetID, datasetName, vo, masterID, datsetStatus, datasetType in resList:
                fileDatasetIDs = []
                for instantiatedSite in instantiatedSites.split(","):
                    fileDatasetID = datasetID
                    if registerDatasets and datasetType in ["output", "log"] and fileDatasetID not in datasetToRegister:
                        datasetToRegister.append(fileDatasetID)
                    # instantiate template datasets
                    if instantiateTmpl:
                        doInstantiate = False
                        if isUnMerging:
                            # instantiate new datasets in each submission for premerged
                            if datasetID in siteDsMap and instantiatedSite in siteDsMap[datasetID]:
                                fileDatasetID = siteDsMap[datasetID][instantiatedSite]
                                tmpLog.debug(f"found concrete premerged datasetID={fileDatasetID}")
                            else:
                                doInstantiate = True
                        else:
                            # check if concrete dataset is already there
                            varMap = {}
                            varMap[":jediTaskID"] = jediTaskID
                            varMap[":type1"] = re.sub("^tmpl_", "", tmpl_VarMap[":type1"])
                            varMap[":type2"] = re.sub("^tmpl_", "", tmpl_VarMap[":type2"])
                            varMap[":templateID"] = datasetID
                            varMap[":closedState"] = "closed"
                            if provenanceID is not None:
                                varMap[":provenanceID"] = provenanceID
                            if instantiatedSite is not None:
                                sqlDT = sqlD + "AND site=:site "
                                varMap[":site"] = instantiatedSite
                            else:
                                sqlDT = sqlD
                            sqlDT += "AND (state IS NULL OR state<>:closedState) "
                            sqlDT += "AND templateID=:templateID "
                            self.cur.execute(sqlDT + comment, varMap)
                            resDT = self.cur.fetchone()
                            if resDT is not None:
                                fileDatasetID = resDT[0]
                                # collect ID of dataset to be registered
                                if resDT[-1] == "defined":
                                    datasetToRegister.append(fileDatasetID)
                                tmpLog.debug(f"found concrete datasetID={fileDatasetID}")
                            else:
                                doInstantiate = True
                        if doInstantiate:
                            # read dataset template
                            varMap = {}
                            varMap[":jediTaskID"] = jediTaskID
                            varMap[":datasetID"] = datasetID
                            self.cur.execute(sqlT1 + comment, varMap)
                            resT1 = self.cur.fetchone()
                            cDatasetSpec = JediDatasetSpec()
                            cDatasetSpec.pack(resT1)
                            # instantiate template dataset
                            cDatasetSpec.type = re.sub("^tmpl_", "", cDatasetSpec.type)
                            cDatasetSpec.templateID = datasetID
                            cDatasetSpec.creationTime = timeNow
                            cDatasetSpec.modificationTime = timeNow
                            varMap = cDatasetSpec.valuesMap(useSeq=True)
                            varMap[":newDatasetID"] = self.cur.var(varNUMBER)
                            self.cur.execute(sqlT2 + comment, varMap)
                            val = self.getvalue_corrector(self.cur.getvalue(varMap[":newDatasetID"]))
                            fileDatasetID = int(val)
                            if instantiatedSite is not None:
                                # set concreate name
                                cDatasetSpec.site = instantiatedSite
                                cDatasetSpec.datasetName = re.sub("/*$", f".{fileDatasetID}", datasetName)
                                # set destination
                                if cDatasetSpec.destination in [None, ""]:
                                    cDatasetSpec.destination = cDatasetSpec.site
                                varMap = {}
                                varMap[":datasetName"] = cDatasetSpec.datasetName
                                varMap[":jediTaskID"] = jediTaskID
                                varMap[":datasetID"] = fileDatasetID
                                varMap[":site"] = cDatasetSpec.site
                                varMap[":destination"] = cDatasetSpec.destination
                                self.cur.execute(sqlCN + comment, varMap)
                            tmpLog.debug(f"instantiated {cDatasetSpec.datasetName} datasetID={fileDatasetID}")
                            if masterID is not None:
                                mstr_RelationMap[fileDatasetID] = (masterID, instantiatedSite)
                            # collect ID of dataset to be registered
                            if fileDatasetID not in datasetToRegister:
                                datasetToRegister.append(fileDatasetID)
                            # collect IDs for pre-merging
                            if isUnMerging:
                                if datasetID not in siteDsMap:
                                    siteDsMap[datasetID] = {}
                                if instantiatedSite not in siteDsMap[datasetID]:
                                    siteDsMap[datasetID][instantiatedSite] = fileDatasetID
                        # keep relation between template and concrete
                        if datasetID not in tmpl_RelationMap:
                            tmpl_RelationMap[datasetID] = {}
                        tmpl_RelationMap[datasetID][instantiatedSite] = fileDatasetID
                    fileDatasetIDs.append(fileDatasetID)
                # get output templates
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                varMap[":datasetID"] = datasetID
                self.cur.execute(sqlR + comment, varMap)
                resTmpList = self.cur.fetchall()
                maxSerialNr = None
                for resR in resTmpList:
                    # make FileSpec
                    outTempID, datasetID, fileNameTemplate, serialNr, outType, streamName = resR
                    if xmlConfigJob is None or outType.endswith("log"):
                        fileNameTemplateList = [(fileNameTemplate, streamName)]
                    else:
                        fileNameTemplateList = []
                        # get output filenames from XML config
                        for tmpFileName in xmlConfigJob.outputs().split(","):
                            # ignore empty
                            if tmpFileName == "":
                                continue
                            newStreamName = tmpFileName
                            newFileNameTemplate = fileNameTemplate + "." + xmlConfigJob.prepend_string() + "." + newStreamName
                            fileNameTemplateList.append((newFileNameTemplate, newStreamName))
                    if bulk_fetch_for_multiple_jobs:
                        nFileLoop = n_files_per_chunk
                    else:
                        if outType.endswith("log"):
                            nFileLoop = 1
                        else:
                            nFileLoop = n_files_per_chunk
                    # loop over all filename templates
                    for fileNameTemplate, streamName in fileNameTemplateList:
                        firstFileID = None
                        first_file_id_for_bulk_fetch = {}
                        for fileDatasetID in fileDatasetIDs:
                            for iFileLoop in range(nFileLoop):
                                fileSpec = JediFileSpec()
                                fileSpec.jediTaskID = jediTaskID
                                fileSpec.datasetID = fileDatasetID
                                nameTemplate = fileNameTemplate.replace("${SN}", "{SN:06d}")
                                nameTemplate = nameTemplate.replace("${SN/P}", "{SN:06d}")
                                nameTemplate = nameTemplate.replace("${SN", "{SN")
                                nameTemplate = nameTemplate.replace("${MIDDLENAME}", middleName)
                                fileSpec.lfn = nameTemplate.format(SN=serialNr)
                                fileSpec.status = "defined"
                                fileSpec.creationDate = timeNow
                                fileSpec.type = outType
                                fileSpec.keepTrack = 1
                                if bulk_fetch_for_multiple_jobs:
                                    if max_serial_numbers_for_bulk_fetch[iFileLoop] is None or max_serial_numbers_for_bulk_fetch[iFileLoop] < serialNr:
                                        max_serial_numbers_for_bulk_fetch[iFileLoop] = serialNr
                                else:
                                    if maxSerialNr is None or maxSerialNr < serialNr:
                                        maxSerialNr = serialNr
                                serialNr += 1
                                # scope
                                if vo in self.jedi_config.ddm.voWithScope.split(","):
                                    fileSpec.scope = get_job_complex_module(self).extractScope(datasetName)
                                # insert
                                if indexFileID < len(fileIDPool):
                                    fileSpec.fileID = fileIDPool[indexFileID]
                                    varMap = fileSpec.valuesMap()
                                    varMapsForInsert.append(varMap)
                                    indexFileID += 1
                                else:
                                    if not simul:
                                        varMap = fileSpec.valuesMap(useSeq=True)
                                        varMap[":newFileID"] = self.cur.var(varNUMBER)
                                        self.cur.execute(sqlI + comment, varMap)
                                        val = self.getvalue_corrector(self.cur.getvalue(varMap[":newFileID"]))
                                        fileSpec.fileID = int(val)
                                        fetched_serial_ids += 1
                                    else:
                                        # set dummy for simulation
                                        fileSpec.fileID = indexFileID
                                        indexFileID += 1
                                # append
                                if bulk_fetch_for_multiple_jobs:
                                    if first_file_id_for_bulk_fetch.get(iFileLoop) is None:
                                        output_map_for_bulk_fetch[iFileLoop][streamName] = fileSpec
                                        first_file_id_for_bulk_fetch[iFileLoop] = fileSpec.fileID
                                        parallel_out_map_for_bulk_fetch[iFileLoop][fileSpec.fileID] = []
                                    parallel_out_map_for_bulk_fetch[iFileLoop][first_file_id_for_bulk_fetch[iFileLoop]].append(fileSpec)
                                else:
                                    if firstFileID is None:
                                        outMap[streamName] = fileSpec
                                        firstFileID = fileSpec.fileID
                                        parallelOutMap[firstFileID] = []
                                    if iFileLoop > 0:
                                        outMap[streamName + f"|{iFileLoop}"] = fileSpec
                                        continue
                                    parallelOutMap[firstFileID].append(fileSpec)
                            # increment SN
                            varMap = {}
                            varMap[":jediTaskID"] = jediTaskID
                            varMap[":outTempID"] = outTempID
                            varMap[":diff"] = nFileLoop
                            varMapsForSN.append(varMap)
            # bulk increment
            if len(varMapsForSN) > 0 and not simul:
                tmpLog.debug(f"bulk increment {len(varMapsForSN)} SNs")
                self.cur.executemany(sqlU + comment, varMapsForSN)
            # bulk insert
            if len(varMapsForInsert) > 0 and not simul:
                tmpLog.debug(f"bulk insert {len(varMapsForInsert)} files")
                self.cur.executemany(sqlII + comment, varMapsForInsert)
            # set masterID to concrete datasets
            for fileDatasetID, (masterID, instantiatedSite) in mstr_RelationMap.items():
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                varMap[":datasetID"] = fileDatasetID
                if masterID in tmpl_RelationMap and instantiatedSite in tmpl_RelationMap[masterID]:
                    varMap[":masterID"] = tmpl_RelationMap[masterID][instantiatedSite]
                else:
                    varMap[":masterID"] = masterID
                self.cur.execute(sqlMC + comment, varMap)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmpLog.debug(f"done indexFileID={indexFileID} fetched_serial_ids={fetched_serial_ids}")
            if bulk_fetch_for_multiple_jobs:
                return output_map_for_bulk_fetch, max_serial_numbers_for_bulk_fetch, datasetToRegister, siteDsMap, parallel_out_map_for_bulk_fetch
            else:
                return outMap, maxSerialNr, datasetToRegister, siteDsMap, parallelOutMap
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return None, None, None, siteDsMap, parallelOutMap
