import copy
import datetime
import json
import math
import operator
import re
import traceback
import uuid

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandautils.PandaUtils import get_sql_IN_bind_variables, naive_utcnow

from pandaserver.config import panda_config
from pandaserver.srvcore import CoreUtils, srv_msg_utils
from pandaserver.taskbuffer import (
    ErrorCode,
    EventServiceUtils,
    JobUtils,
    PrioUtil,
    task_split_rules,
)
from pandaserver.taskbuffer.db_proxy_mods.base_module import BaseModule, varNUMBER
from pandaserver.taskbuffer.db_proxy_mods.entity_module import get_entity_module
from pandaserver.taskbuffer.FileSpec import FileSpec
from pandaserver.taskbuffer.JediDatasetSpec import (
    INPUT_TYPES_var_map,
    INPUT_TYPES_var_str,
)
from pandaserver.taskbuffer.JediTaskSpec import JediTaskSpec
from pandaserver.taskbuffer.JobSpec import JobSpec

try:
    import idds.common.constants
    import idds.common.utils
    from idds.client.client import Client as iDDS_Client
except ImportError:
    pass


# Module class to define methods related to tasks and events, being merged into a single module due to their cross-references
class TaskEventModule(BaseModule):
    # constructor
    def __init__(self, log_stream: LogWrapper):
        super().__init__(log_stream)

    # make event range ID for event service
    def makeEventRangeID(self, jediTaskID, pandaID, fileID, job_processID, attemptNr):
        return f"{jediTaskID}-{pandaID}-{fileID}-{job_processID}-{attemptNr}"

    # get a list of even ranges for a PandaID
    def getEventRanges(self, pandaID, jobsetID, jediTaskID, nRanges, acceptJson, scattered, segment_id):
        comment = " /* DBProxy.getEventRanges */"
        tmp_log = self.create_tagged_logger(comment, f"<PandaID={pandaID} jobsetID={jobsetID} jediTaskID={jediTaskID}")
        tmp_log.debug(f"start nRanges={nRanges} scattered={scattered} segment={segment_id}")
        try:
            regStart = naive_utcnow()
            # convert to int
            try:
                nRanges = int(nRanges)
            except Exception:
                nRanges = 8
            try:
                pandaID = int(pandaID)
            except Exception:
                pass
            try:
                jobsetID = int(jobsetID)
            except Exception:
                pass
            try:
                jediTaskID = int(jediTaskID)
            except Exception:
                jediTaskID = None
            iRanges = 0
            # sql to get job
            sqlJ = f"SELECT jobStatus,commandToPilot,eventService,jediTaskID FROM {panda_config.schemaPANDA}.jobsActive4 "
            sqlJ += "WHERE PandaID=:pandaID FOR UPDATE "
            # sql to find a file to lock
            sqlFF = f"SELECT jediTaskID,datasetID,fileID FROM {panda_config.schemaPANDA}.filesTable4 "
            sqlFF += "WHERE PandaID=:pandaID AND type IN (:type1,:type2) "
            sqlFF += "ORDER BY fileID "
            # sql to use a dataset as lock
            sqlLD = f"SELECT status FROM {panda_config.schemaJEDI}.JEDI_Datasets "
            sqlLD += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            sqlLD += "FOR UPDATE "
            # sql to use a file as lock
            sqlLK = f"SELECT status FROM {panda_config.schemaJEDI}.JEDI_Dataset_Contents "
            sqlLK += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
            sqlLK += "FOR UPDATE "
            # sql to get ranges with jediTaskID
            sqlW = f"UPDATE {panda_config.schemaJEDI}.JEDI_Events tab "
            sqlW += "SET PandaID=:pandaID,status=:newEventStatus "
            sqlW += "WHERE (jediTaskID,PandaID,fileID,job_processID,attemptNr) IN ("
            sqlW += "SELECT jediTaskID,PandaID,fileID,job_processID,attemptNr FROM ("
            sqlW += "SELECT jediTaskID,PandaID,fileID,job_processID,attemptNr FROM "
            sqlW += "/* sorted by JEDITASKID, PANDAID, FILEID to take advantage of the IOT table structure*/ "
            sqlW += f"{panda_config.schemaJEDI}.JEDI_Events tab "
            sqlW += "WHERE jediTaskID=:jediTaskID AND PandaID=:jobsetID AND status=:eventStatus AND attemptNr>:minAttemptNr "
            if segment_id is not None:
                sqlW += "AND datasetID=:datasetID "
            sqlW += "ORDER BY jediTaskID,PandaID,fileID "
            sqlW += f") WHERE rownum<={nRanges + 1}) "
            # sql to get ranges for jumbo
            sqlJM = f"UPDATE {panda_config.schemaJEDI}.JEDI_Events tab "
            sqlJM += "SET PandaID=:pandaID,status=:newEventStatus "
            sqlJM += "WHERE (jediTaskID,PandaID,fileID,job_processID,attemptNr) IN ("
            sqlJM += "SELECT jediTaskID,PandaID,fileID,job_processID,attemptNr FROM ("
            sqlJM += "SELECT jediTaskID,PandaID,fileID,job_processID,attemptNr FROM "
            sqlJM += "/* sorted by JEDITASKID, PANDAID, FILEID to take advantage of the IOT table structure*/ "
            sqlJM += f"{panda_config.schemaJEDI}.JEDI_Events tab "
            sqlJM += "WHERE jediTaskID=:jediTaskID AND status=:eventStatus AND attemptNr>:minAttemptNr "
            if scattered:
                pass
            else:
                sqlJM += "ORDER BY jediTaskID,PandaID,fileID "
            sqlJM += f") WHERE rownum<={nRanges + 1}) "
            # sql to get ranges
            sqlRR = "SELECT jediTaskID,datasetID,fileID,attemptNr,job_processID,def_min_eventID,def_max_eventID,event_offset "
            sqlRR += f"FROM {panda_config.schemaJEDI}.JEDI_Events tab "
            sqlRR += "WHERE jediTaskID=:jediTaskID AND PandaID=:PandaID AND status=:eventStatus "
            # sql to get datasets
            sqlGD = f"SELECT datasetID FROM {panda_config.schemaJEDI}.JEDI_Datasets "
            sqlGD += "WHERE jediTaskID=:jediTaskID AND type IN (:type1,:type2) "
            # sql to update files in the jobset
            sqlJS = f"UPDATE {panda_config.schemaJEDI}.JEDI_Dataset_Contents "
            sqlJS += "SET status=:newStatus,is_waiting=NULL "
            sqlJS += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            sqlJS += "AND status=:oldStatus AND keepTrack=:keepTrack AND PandaID IN ("
            # sql to update dataset
            sqlUD = f"UPDATE {panda_config.schemaJEDI}.JEDI_Datasets "
            sqlUD += "SET nFilesUsed=nFilesUsed+:nDiff,nFilesWaiting=nFilesWaiting-:nDiff "
            sqlUD += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            # sql to get file info
            sqlF = f"SELECT lfn,GUID,scope FROM {panda_config.schemaJEDI}.JEDI_Dataset_Contents "
            sqlF += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
            # sql to lock range
            sqlU = f"UPDATE {panda_config.schemaJEDI}.JEDI_Events "
            sqlU += "SET status=:eventStatus,is_jumbo=:isJumbo "
            sqlU += "WHERE jediTaskID=:jediTaskID AND PandaID=:pandaID "
            sqlU += "AND status=:oldEventStatus "
            # sql to release range
            sqlRS = f"UPDATE {panda_config.schemaJEDI}.JEDI_Events "
            sqlRS += "SET PandaID=event_offset,status=:eventStatus "
            sqlRS += "WHERE jediTaskID=:jediTaskID AND fileID=:fileID AND PandaID=:pandaID "
            sqlRS += "AND job_processID=:job_processID AND attemptNr=:attemptNr "
            sqlRS += "AND status=:oldEventStatus "
            # start transaction
            self.conn.begin()
            self.cur.arraysize = 100000
            # get job
            varMap = {}
            varMap[":pandaID"] = pandaID
            self.cur.execute(sqlJ + comment, varMap)
            resJ = self.cur.fetchone()
            toSkip = True
            retRanges = []
            noMoreEvents = False
            if resJ is None:
                # job not found
                tmp_log.debug("skip job not found")
            elif resJ[0] not in ["sent", "running", "starting"]:
                # wrong job status
                tmp_log.debug(f"skip wrong job status in {resJ[0]}")
            elif resJ[1] == "tobekilled":
                # job is being killed
                tmp_log.debug("skip job is being killed")
            else:
                toSkip = False
                # jumbo
                if resJ[2] == EventServiceUtils.jumboJobFlagNumber:
                    isJumbo = True
                else:
                    isJumbo = False
                # get jediTaskID
                if jediTaskID is None:
                    jediTaskID = resJ[3]
                # find a file to lock
                varMap = dict()
                varMap[":pandaID"] = pandaID
                varMap[":type1"] = "input"
                varMap[":type2"] = "pseudo_input"
                self.cur.execute(sqlFF + comment, varMap)
                resFF = self.cur.fetchone()
                if resFF is not None:
                    ffJediTask, ffDatasetID, ffFileID = resFF
                    varMap = dict()
                    varMap[":jediTaskID"] = ffJediTask
                    varMap[":datasetID"] = ffDatasetID
                    if isJumbo:
                        self.cur.execute(sqlLD + comment, varMap)
                        tmp_log.debug(f"locked datasetID={ffDatasetID}")
                # prelock event ranges
                varMap = {}
                varMap[":eventStatus"] = EventServiceUtils.ST_ready
                varMap[":minAttemptNr"] = 0
                varMap[":jediTaskID"] = jediTaskID
                varMap[":pandaID"] = pandaID
                varMap[":eventStatus"] = EventServiceUtils.ST_ready
                varMap[":newEventStatus"] = EventServiceUtils.ST_reserved_get
                if segment_id is not None:
                    varMap[":datasetID"] = segment_id
                if not isJumbo:
                    varMap[":jobsetID"] = jobsetID
                if isJumbo:
                    tmp_log.debug(sqlJM + comment + str(varMap))
                    self.cur.execute(sqlJM + comment, varMap)
                else:
                    self.cur.execute(sqlW + comment, varMap)
                nRow = self.cur.rowcount
                tmp_log.debug(f"pre-locked {nRow} events")
                # get event ranges
                varMap = dict()
                varMap[":jediTaskID"] = jediTaskID
                varMap[":PandaID"] = pandaID
                varMap[":eventStatus"] = EventServiceUtils.ST_reserved_get
                tmp_log.debug(sqlRR + comment + str(varMap))
                self.cur.execute(sqlRR + comment, varMap)
                resList = self.cur.fetchall()
                if len(resList) > nRanges:
                    # release the last event range
                    (
                        tmpJediTaskID,
                        datasetID,
                        fileID,
                        attemptNr,
                        job_processID,
                        startEvent,
                        lastEvent,
                        tmpJobsetID,
                    ) = resList[-1]
                    varMap = {}
                    varMap[":jediTaskID"] = tmpJediTaskID
                    varMap[":fileID"] = fileID
                    varMap[":job_processID"] = job_processID
                    varMap[":pandaID"] = pandaID
                    varMap[":attemptNr"] = attemptNr
                    varMap[":eventStatus"] = EventServiceUtils.ST_ready
                    varMap[":oldEventStatus"] = EventServiceUtils.ST_reserved_get
                    self.cur.execute(sqlRS + comment, varMap)
                    resList = resList[:nRanges]
                else:
                    noMoreEvents = True
                # make dict
                fileInfo = {}
                jobsetList = {}
                for (
                    tmpJediTaskID,
                    datasetID,
                    fileID,
                    attemptNr,
                    job_processID,
                    startEvent,
                    lastEvent,
                    tmpJobsetID,
                ) in resList:
                    # get file info
                    if fileID not in fileInfo:
                        varMap = {}
                        varMap[":jediTaskID"] = tmpJediTaskID
                        varMap[":datasetID"] = datasetID
                        varMap[":fileID"] = fileID
                        self.cur.execute(sqlF + comment, varMap)
                        resF = self.cur.fetchone()
                        # not found
                        if resF is None:
                            resF = (None, None, None)
                            tmp_log.warning(f"file info is not found for fileID={fileID}")
                        fileInfo[fileID] = resF
                    # get LFN and GUID
                    tmpLFN, tmpGUID, tmpScope = fileInfo[fileID]
                    # make dict
                    tmpDict = {
                        "eventRangeID": self.makeEventRangeID(tmpJediTaskID, pandaID, fileID, job_processID, attemptNr),
                        "startEvent": startEvent,
                        "lastEvent": lastEvent,
                        "LFN": tmpLFN,
                        "GUID": tmpGUID,
                        "scope": tmpScope,
                    }
                    # append
                    retRanges.append(tmpDict)
                    iRanges += 1
                    if tmpJediTaskID not in jobsetList:
                        jobsetList[tmpJediTaskID] = []
                    jobsetList[tmpJediTaskID].append(tmpJobsetID)
                tmp_log.debug(f"got {len(retRanges)} events")
                # lock events
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                varMap[":pandaID"] = pandaID
                varMap[":eventStatus"] = EventServiceUtils.ST_sent
                varMap[":oldEventStatus"] = EventServiceUtils.ST_reserved_get
                if isJumbo:
                    varMap[":isJumbo"] = EventServiceUtils.eventTableIsJumbo
                else:
                    varMap[":isJumbo"] = None
                self.cur.execute(sqlU + comment, varMap)
                nRow = self.cur.rowcount
                tmp_log.debug(f"locked {nRow} events")
                # kill unused consumers
                if not isJumbo and not toSkip and (retRanges == [] or noMoreEvents) and jediTaskID is not None and segment_id is None:
                    tmp_log.debug("kill unused consumers")
                    tmpJobSpec = JobSpec()
                    tmpJobSpec.PandaID = pandaID
                    tmpJobSpec.jobsetID = jobsetID
                    tmpJobSpec.jediTaskID = jediTaskID
                    self.killUnusedEventServiceConsumers(tmpJobSpec, False, checkAttemptNr=True)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            regTime = naive_utcnow() - regStart
            tmp_log.debug(f"done with {iRanges} event ranges. took {regTime.seconds} sec")
            if not acceptJson:
                return json.dumps(retRanges)
            return retRanges
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return None

    # update even ranges
    def updateEventRanges(self, eventDictParam, version=0):
        # version 0: normal event service
        # version 1: jumbo jobs with zip file support
        # version 2: fine-grained processing where events can be updated before being dispatched
        comment = " /* DBProxy.updateEventRanges */"
        tmp_log = self.create_tagged_logger(comment)
        commandMap = {}
        retList = []
        try:
            regStart = naive_utcnow()
            jobAttrs = {}
            # sql to update status
            sqlU = f"UPDATE {panda_config.schemaJEDI}.JEDI_Events "
            sqlU += "SET status=:eventStatus,objstore_ID=:objstoreID,error_code=:errorCode," "path_convention=:pathConvention,error_diag=:errorDiag"
            if version != 0:
                sqlU += ",zipRow_ID=:zipRow_ID"
            sqlU += " WHERE jediTaskID=:jediTaskID AND pandaID=:pandaID AND fileID=:fileID "
            sqlU += "AND job_processID=:job_processID AND attemptNr=:attemptNr "
            if version == 2:
                sqlU += "AND status IN (:esSent, :esRunning, :esReady) "
            else:
                sqlU += "AND status IN (:esSent, :esRunning) "
            # sql to get event range
            sqlC = f"SELECT splitRule FROM {panda_config.schemaJEDI}.JEDI_Tasks "
            sqlC += "WHERE jediTaskID=:jediTaskID "
            # sql to get nEvents
            sqlE = "SELECT jobStatus,nEvents,commandToPilot,supErrorCode,specialHandling FROM ATLAS_PANDA.jobsActive4 "
            sqlE += "WHERE PandaID=:pandaID "
            if version == 2:
                sqlE += "OR jobsetID=:pandaID "
            # sql to set nEvents
            sqlS = "UPDATE ATLAS_PANDA.jobsActive4 "
            sqlS += f"SET nEvents=(SELECT COUNT(1) FROM {panda_config.schemaJEDI}.JEDI_Events "
            sqlS += "WHERE jediTaskID=:jediTaskID AND PandaID=:PandaID AND status IN (:esFinished,:esDone,:esMerged))*:nEvents "
            sqlS += "WHERE PandaID=:pandaID "
            if version == 2:
                sqlS += "OR jobsetID=:pandaID "
            # sql to check zip file
            sqlFC = "SELECT row_ID FROM ATLAS_PANDA.filesTable4 "
            sqlFC += "WHERE PandaID=:pandaID AND lfn=:lfn "
            # sql to insert zip file
            sqlF = f"INSERT INTO ATLAS_PANDA.filesTable4 ({FileSpec.columnNames()}) "
            sqlF += FileSpec.bindValuesExpression(useSeq=True)
            sqlF += " RETURNING row_ID INTO :newRowID"
            # sql for fatal events
            sqlFA = f"UPDATE {panda_config.schemaJEDI}.JEDI_Events "
            sqlFA += "SET attemptNr=:newAttemptNr "
            sqlFA += " WHERE jediTaskID=:jediTaskID AND pandaID=:pandaID AND fileID=:fileID "
            sqlFA += "AND job_processID=:job_processID AND attemptNr=:oldAttemptNr "
            sqlFA += "AND status=:eventStatus "
            # params formatting with version
            if version == 0:
                # format without zip
                eventDictList = eventDictParam
            else:
                # format with zip
                eventDictList = []
                for eventDictChunk in eventDictParam:
                    # get zip file if any
                    if "zipFile" in eventDictChunk:
                        zipFile = eventDictChunk["zipFile"]
                    else:
                        zipFile = None
                    # collect all dicts
                    if "eventRanges" in eventDictChunk:
                        for eventDict in eventDictChunk["eventRanges"]:
                            # add zip file
                            eventDict["zipFile"] = zipFile
                            # append
                            eventDictList.append(eventDict)
                    else:
                        eventDictList.append(eventDictChunk)
            # update events
            tmp_log.debug(f"total {len(eventDictList)} events")
            zipRowIdMap = {}
            nEventsMap = dict()
            iEvents = 0
            maxEvents = 100000
            iSkipped = 0
            ok_job_status = ["sent", "running", "starting", "transferring"]
            if version == 2:
                ok_job_status += ["activated"]
            # start transaction
            self.conn.begin()
            # loop over all events
            varMapListU = []
            varMapListFA = []
            for eventDict in eventDictList:
                # avoid too many events
                iEvents += 1
                if iEvents > maxEvents:
                    retList.append(None)
                    iSkipped += 1
                    continue
                # get event range ID
                if "eventRangeID" not in eventDict:
                    tmp_log.error(f"eventRangeID is missing in {str(eventDict)}")
                    retList.append(False)
                    continue
                eventRangeID = eventDict["eventRangeID"]
                # decompose eventRangeID
                try:
                    tmpItems = eventRangeID.split("-")
                    jediTaskID, pandaID, fileID, job_processID, attemptNr = tmpItems
                    jediTaskID = int(jediTaskID)
                    pandaID = int(pandaID)
                    fileID = int(fileID)
                    job_processID = int(job_processID)
                    attemptNr = int(attemptNr)
                except Exception:
                    tmp_log.error(f"wrongly formatted eventRangeID")
                    retList.append(False)
                    continue
                # get event status
                if "eventStatus" not in eventDict:
                    tmp_log.error(f"<eventRangeID={eventRangeID}> eventStatus is missing in {str(eventDict)}")
                    retList.append(False)
                    continue
                eventStatus = eventDict["eventStatus"]
                # map string status to int
                isFatal = False
                if eventStatus == "running":
                    intEventStatus = EventServiceUtils.ST_running
                elif eventStatus == "transferring":
                    intEventStatus = EventServiceUtils.ST_running
                elif eventStatus == "finished":
                    intEventStatus = EventServiceUtils.ST_finished
                elif eventStatus == "failed":
                    intEventStatus = EventServiceUtils.ST_failed
                elif eventStatus == "fatal":
                    intEventStatus = EventServiceUtils.ST_failed
                    isFatal = True
                else:
                    tmp_log.error(f"<eventRangeID={eventRangeID}> unknown status {eventStatus}")
                    retList.append(False)
                    continue
                # only final status
                if eventStatus not in ["finished", "failed", "fatal"]:
                    retList.append(None)
                    iSkipped += 1
                    tmp_log.debug(f"<eventRangeID={eventRangeID}> eventStatus={eventStatus} skipped")
                    continue
                # core count
                coreCount = eventDict.get("coreCount")
                # CPU consumption
                cpuConsumptionTime = eventDict.get("cpuConsumptionTime")
                # objectstore ID
                objstoreID = eventDict.get("objstoreID")
                # error code
                errorCode = eventDict.get("errorCode")
                # path convention
                pathConvention = eventDict.get("pathConvention")
                # error diag
                errorDiag = eventDict.get("errorDiag")
                isOK = True
                # get job attributes
                if pandaID not in jobAttrs:
                    varMap = {}
                    varMap[":pandaID"] = pandaID
                    self.cur.execute(sqlE + comment, varMap)
                    resE = self.cur.fetchone()
                    jobAttrs[pandaID] = resE
                    tmp_log.debug(f"PandaID={pandaID}")
                resE = jobAttrs[pandaID]
                if resE is None:
                    tmp_log.error(f"<eventRangeID={eventRangeID}> unknown PandaID")
                    retList.append(False)
                    isOK = False
                    commandToPilot = "tobekilled"
                else:
                    # check job status
                    (
                        jobStatus,
                        nEventsOld,
                        commandToPilot,
                        supErrorCode,
                        specialHandling,
                    ) = resE
                    if jobStatus not in ok_job_status:
                        tmp_log.error(f"<eventRangeID={eventRangeID}> wrong jobStatus={jobStatus}")
                        retList.append(False)
                        isOK = False
                    else:
                        # insert zip
                        zipRow_ID = None
                        if "zipFile" in eventDict and eventDict["zipFile"] is not None:
                            if eventDict["zipFile"]["lfn"] in zipRowIdMap:
                                zipRow_ID = zipRowIdMap[eventDict["zipFile"]["lfn"]]
                            else:
                                # check zip
                                varMap = dict()
                                varMap[":pandaID"] = pandaID
                                varMap[":lfn"] = eventDict["zipFile"]["lfn"]
                                self.cur.execute(sqlFC + comment, varMap)
                                resFC = self.cur.fetchone()
                                if resFC is not None:
                                    (zipRow_ID,) = resFC
                                else:
                                    # insert a new file
                                    zipJobSpec = JobSpec()
                                    zipJobSpec.PandaID = pandaID
                                    zipJobSpec.specialHandling = specialHandling
                                    zipFileSpec = FileSpec()
                                    zipFileSpec.jediTaskID = jediTaskID
                                    zipFileSpec.lfn = eventDict["zipFile"]["lfn"]
                                    zipFileSpec.GUID = str(uuid.uuid4())
                                    if "fsize" in eventDict["zipFile"]:
                                        zipFileSpec.fsize = int(eventDict["zipFile"]["fsize"])
                                    else:
                                        zipFileSpec.fsize = 0
                                    if "adler32" in eventDict["zipFile"]:
                                        zipFileSpec.checksum = f"ad:{eventDict['zipFile']['adler32']}"
                                    if "numEvents" in eventDict["zipFile"]:
                                        zipFileSpec.dispatchDBlockToken = eventDict["zipFile"]["numEvents"]
                                    zipFileSpec.type = "zipoutput"
                                    zipFileSpec.status = "ready"
                                    zipFileSpec.destinationSE = eventDict["zipFile"]["objstoreID"]
                                    if "pathConvention" in eventDict["zipFile"]:
                                        zipFileSpec.destinationSE = f"{zipFileSpec.destinationSE}/{eventDict['zipFile']['pathConvention']}"
                                    zipJobSpec.addFile(zipFileSpec)
                                    varMap = zipFileSpec.valuesMap(useSeq=True)
                                    varMap[":newRowID"] = self.cur.var(varNUMBER)
                                    self.cur.execute(sqlF + comment, varMap)
                                    val = self.getvalue_corrector(self.cur.getvalue(varMap[":newRowID"]))
                                    zipRow_ID = int(val)
                                    zipRowIdMap[eventDict["zipFile"]["lfn"]] = zipRow_ID
                                    # make an empty file to trigger registration for zip files in Adder
                                    if zipJobSpec.registerEsFiles():
                                        # tmpFileName = '{0}_{1}_{2}'.format(pandaID, EventServiceUtils.esRegStatus,
                                        #                                    uuid.uuid3(uuid.NAMESPACE_DNS,''))
                                        # tmpFileName = os.path.join(panda_config.logdir, tmpFileName)
                                        # try:
                                        #     open(tmpFileName, 'w').close()
                                        # except Exception:
                                        #     pass
                                        # sql to insert
                                        sqlI = (
                                            "INSERT INTO {0}.Job_Output_Report "
                                            "(PandaID, prodSourceLabel, jobStatus, attemptNr, data, timeStamp) "
                                            "VALUES(:PandaID, :prodSourceLabel, :jobStatus, :attemptNr, :data, :timeStamp) "
                                        ).format(panda_config.schemaPANDA)
                                        # insert
                                        varMap = {}
                                        varMap[":PandaID"] = pandaID
                                        varMap[":prodSourceLabel"] = zipJobSpec.prodSourceLabel
                                        varMap[":jobStatus"] = zipJobSpec.jobStatus
                                        varMap[":attemptNr"] = 0 if zipJobSpec.attemptNr in [None, "NULL", ""] else zipJobSpec.attemptNr
                                        varMap[":data"] = None
                                        varMap[":timeStamp"] = naive_utcnow()
                                        try:
                                            self.cur.execute(sqlI + comment, varMap)
                                        except Exception:
                                            pass
                                        else:
                                            tmp_log.debug(f"successfully inserted job output report {pandaID}.{varMap[':attemptNr']}")
                        # update event
                        varMap = {}
                        varMap[":jediTaskID"] = jediTaskID
                        varMap[":pandaID"] = pandaID
                        varMap[":fileID"] = fileID
                        varMap[":job_processID"] = job_processID
                        varMap[":attemptNr"] = attemptNr
                        varMap[":eventStatus"] = intEventStatus
                        varMap[":objstoreID"] = objstoreID
                        varMap[":errorCode"] = errorCode
                        varMap[":pathConvention"] = pathConvention
                        varMap[":errorDiag"] = errorDiag
                        varMap[":esSent"] = EventServiceUtils.ST_sent
                        varMap[":esRunning"] = EventServiceUtils.ST_running
                        if version == 2:
                            varMap[":esReady"] = EventServiceUtils.ST_ready
                        if version != 0:
                            varMap[":zipRow_ID"] = zipRow_ID
                        varMapListU.append(varMap)
                        # fatal event
                        if isFatal:
                            varMap = {}
                            varMap[":jediTaskID"] = jediTaskID
                            varMap[":pandaID"] = pandaID
                            varMap[":fileID"] = fileID
                            varMap[":job_processID"] = job_processID
                            varMap[":oldAttemptNr"] = attemptNr
                            varMap[":newAttemptNr"] = 1
                            varMap[":eventStatus"] = EventServiceUtils.ST_failed
                            varMapListFA.append(varMap)
                        # nEvents of finished
                        if eventStatus in ["finished"]:
                            # get nEvents
                            if pandaID not in nEventsMap:
                                nEventsDef = 1
                                varMap = {}
                                varMap[":jediTaskID"] = jediTaskID
                                self.cur.execute(sqlC + comment, varMap)
                                resC = self.cur.fetchone()
                                if resC is not None:
                                    (splitRule,) = resC
                                    tmpM = re.search("ES=(\d+)", splitRule)
                                    if tmpM is not None:
                                        nEventsDef = int(tmpM.group(1))
                                nEventsMap[pandaID] = {
                                    "jediTaskID": jediTaskID,
                                    "nEvents": nEventsDef,
                                }
                    # soft kill
                    if commandToPilot not in [None, ""] and supErrorCode in [ErrorCode.EC_EventServicePreemption]:
                        commandToPilot = "softkill"
                if isOK:
                    retList.append(True)
                if pandaID not in commandMap:
                    commandMap[pandaID] = commandToPilot
            tmp_log.debug(f"update {len(varMapListU)} events")
            if len(varMapListU) > 0:
                self.cur.executemany(sqlU + comment, varMapListU)
            tmp_log.debug(f"fatal {len(varMapListFA)} events")
            if len(varMapListFA) > 0:
                self.cur.executemany(sqlFA + comment, varMapListFA)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # update nevents
            for pandaID in nEventsMap:
                data = nEventsMap[pandaID]
                self.conn.begin()
                varMap = {}
                varMap[":pandaID"] = pandaID
                varMap[":jediTaskID"] = data["jediTaskID"]
                varMap[":nEvents"] = data["nEvents"]
                varMap[":esFinished"] = EventServiceUtils.ST_finished
                varMap[":esDone"] = EventServiceUtils.ST_done
                varMap[":esMerged"] = EventServiceUtils.ST_merged
                self.cur.execute(sqlS + comment, varMap)
                if not self._commit():
                    raise RuntimeError("Commit error")
            regTime = naive_utcnow() - regStart
            tmp_log.debug(f"done. {iSkipped} events out of {len(eventDictList)} events skipped. took {regTime.seconds} sec")
            return retList, commandMap
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            retList.append(False)
            return retList, commandMap

    # get events status
    def get_events_status(self, ids):
        comment = " /* DBProxy.get_events_status */"
        tmp_log = self.create_tagged_logger(comment)
        tmp_log.debug("start")
        try:
            ids = json.loads(ids)
            # sql to get event stats
            sql = f"SELECT jediTaskID,fileID,attemptNr,job_processID,status,error_code,error_diag FROM {panda_config.schemaJEDI}.JEDI_Events "
            sql += "WHERE jediTaskID=:jediTaskID AND PandaID=:PandaID "
            ret_val = {}
            for tmp_id in ids:
                varMap = {
                    ":jediTaskID": tmp_id["task_id"],
                    ":PandaID": tmp_id["panda_id"],
                }
                # start transaction
                self.conn.begin()
                self.cur.arraysize = 10000
                # get stats
                self.cur.execute(sql + comment, varMap)
                resM = self.cur.fetchall()
                tmp_map = {}
                for jediTaskID, fileID, attemptNr, job_processID, eventStatus, error_code, error_diag in resM:
                    eventRangeID = self.makeEventRangeID(jediTaskID, tmp_id["panda_id"], fileID, job_processID, attemptNr)
                    tmp_map[eventRangeID] = {"status": EventServiceUtils.ES_status_map[eventStatus], "error": error_code, "dialog": error_diag}
                ret_val[tmp_id["panda_id"]] = tmp_map
                # commit
                if not self._commit():
                    raise RuntimeError("Commit error")
            tmp_log.debug("done")
            return ret_val
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return None

    # kill active consumers related to an ES job
    def killEventServiceConsumers(self, job, killedFlag, useCommit=True):
        comment = " /* DBProxy.killEventServiceConsumers */"
        tmp_log = self.create_tagged_logger(comment, f"PandaID={job.PandaID}")
        tmp_log.debug(f"start")
        try:
            # begin transaction
            if useCommit:
                self.conn.begin()
            # sql to get consumers
            sqlCP = "SELECT /*+ INDEX_RS_ASC(tab JEDI_EVENTS_FILEID_IDX) NO_INDEX_FFS(tab JEDI_EVENTS_PK) NO_INDEX_SS(tab JEDI_EVENTS_PK) */ "
            sqlCP += f"distinct PandaID FROM {panda_config.schemaJEDI}.JEDI_Events tab "
            sqlCP += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
            sqlCP += "AND NOT status IN (:esDiscarded,:esCancelled) "
            # sql to discard or cancel event ranges
            sqlDE = "UPDATE /*+ INDEX_RS_ASC(tab JEDI_EVENTS_FILEID_IDX) NO_INDEX_FFS(tab JEDI_EVENTS_PK) NO_INDEX_SS(tab JEDI_EVENTS_PK) */ "
            sqlDE += f"{panda_config.schemaJEDI}.JEDI_Events tab "
            sqlDE += "SET status=:status "
            sqlDE += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID AND PandaID=:PandaID "
            sqlDE += "AND status IN (:esFinished,:esDone) "
            sqlCE = "UPDATE /*+ INDEX_RS_ASC(tab JEDI_EVENTS_FILEID_IDX) NO_INDEX_FFS(tab JEDI_EVENTS_PK) NO_INDEX_SS(tab JEDI_EVENTS_PK) */ "
            sqlCE += f"{panda_config.schemaJEDI}.JEDI_Events tab "
            sqlCE += "SET status=:status "
            sqlCE += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID AND PandaID=:PandaID "
            sqlCE += "AND NOT status IN (:esFinished,:esDone,:esDiscarded,:esCancelled,:esFailed,:esFatal,:esCorrupted) "
            # look for consumers for each input
            killPandaIDs = {}
            for fileSpec in job.Files:
                if fileSpec.type not in ["input", "pseudo_input"]:
                    continue
                if fileSpec.fileID in ["NULL", None]:
                    continue
                # get PandaIDs
                varMap = {}
                varMap[":jediTaskID"] = fileSpec.jediTaskID
                varMap[":datasetID"] = fileSpec.datasetID
                varMap[":fileID"] = fileSpec.fileID
                varMap[":esDiscarded"] = EventServiceUtils.ST_discarded
                varMap[":esCancelled"] = EventServiceUtils.ST_cancelled
                self.cur.arraysize = 100000
                self.cur.execute(sqlCP + comment, varMap)
                resPs = self.cur.fetchall()
                for (esPandaID,) in resPs:
                    if esPandaID not in killPandaIDs:
                        killPandaIDs[esPandaID] = set()
                    killPandaIDs[esPandaID].add((fileSpec.jediTaskID, fileSpec.datasetID, fileSpec.fileID))
            # kill consumers
            sqlDJS = f"SELECT {JobSpec.columnNames()} "
            sqlDJS += "FROM ATLAS_PANDA.jobsActive4 WHERE PandaID=:PandaID "
            sqlDJS += "FOR UPDATE NOWAIT "
            sqlDJD = "DELETE FROM ATLAS_PANDA.jobsActive4 WHERE PandaID=:PandaID"
            sqlDJI = f"INSERT INTO ATLAS_PANDA.jobsArchived4 ({JobSpec.columnNames()}) "
            sqlDJI += JobSpec.bindValuesExpression()
            sqlFSF = "UPDATE ATLAS_PANDA.filesTable4 SET status=:newStatus "
            sqlFSF += "WHERE PandaID=:PandaID AND type IN (:type1,:type2) "
            sqlFMod = "UPDATE ATLAS_PANDA.filesTable4 SET modificationTime=:modificationTime WHERE PandaID=:PandaID"
            sqlMMod = "UPDATE ATLAS_PANDA.metaTable SET modificationTime=:modificationTime WHERE PandaID=:PandaID"
            sqlPMod = "UPDATE ATLAS_PANDA.jobParamsTable SET modificationTime=:modificationTime WHERE PandaID=:PandaID"
            nKilled = 0
            killPandaIDsList = sorted(killPandaIDs)
            for pandaID in killPandaIDsList:
                # ignore original PandaID since it will be killed by caller
                if pandaID == job.PandaID:
                    continue
                # skip jobsetID
                if pandaID == job.jobsetID:
                    continue
                # read job
                varMap = {}
                varMap[":PandaID"] = pandaID
                self.cur.arraysize = 10
                self.cur.execute(sqlDJS + comment, varMap)
                resJob = self.cur.fetchall()
                if len(resJob) == 0:
                    continue
                # instantiate JobSpec
                dJob = JobSpec()
                dJob.pack(resJob[0])
                # skip if jobset different
                if dJob.jobsetID != job.jobsetID:
                    tmp_log.debug(f"skip consumer {pandaID} since jobsetID is different")
                    continue
                # skip jumbo
                if EventServiceUtils.isJumboJob(dJob):
                    tmp_log.debug(f"skip jumbo {pandaID}")
                    continue
                tmp_log.debug(f"kill associated consumer {pandaID}")
                # delete
                varMap = {}
                varMap[":PandaID"] = pandaID
                self.cur.execute(sqlDJD + comment, varMap)
                retD = self.cur.rowcount
                if retD == 0:
                    continue
                # set error code
                dJob.endTime = naive_utcnow()
                if EventServiceUtils.isJobCloningJob(dJob):
                    dJob.jobStatus = "closed"
                    dJob.jobSubStatus = "jc_unlock"
                    dJob.taskBufferErrorCode = ErrorCode.EC_JobCloningUnlock
                    dJob.taskBufferErrorDiag = f"closed since another clone PandaID={job.PandaID} got semaphore"
                elif killedFlag:
                    dJob.jobStatus = "cancelled"
                    dJob.jobSubStatus = "es_killed"
                    dJob.taskBufferErrorCode = ErrorCode.EC_EventServiceKillOK
                    dJob.taskBufferErrorDiag = f"killed since an associated consumer PandaID={job.PandaID} was killed"
                else:
                    dJob.jobStatus = "failed"
                    dJob.jobSubStatus = "es_aborted"
                    dJob.taskBufferErrorCode = ErrorCode.EC_EventServiceKillNG
                    dJob.taskBufferErrorDiag = f"killed since an associated consumer PandaID={job.PandaID} failed"
                dJob.modificationTime = dJob.endTime
                dJob.stateChangeTime = dJob.endTime
                # insert
                self.cur.execute(sqlDJI + comment, dJob.valuesMap())
                # set file status
                varMap = {}
                varMap[":PandaID"] = pandaID
                varMap[":type1"] = "output"
                varMap[":type2"] = "log"
                varMap[":newStatus"] = "failed"
                self.cur.execute(sqlFSF + comment, varMap)
                # update files,metadata,parametes
                varMap = {}
                varMap[":PandaID"] = pandaID
                varMap[":modificationTime"] = dJob.modificationTime
                self.cur.execute(sqlFMod + comment, varMap)
                self.cur.execute(sqlMMod + comment, varMap)
                self.cur.execute(sqlPMod + comment, varMap)
                nKilled += 1
                # discard event ranges
                nRowsDis = 0
                nRowsCan = 0
                for jediTaskID, datasetID, fileID in killPandaIDs[pandaID]:
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    varMap[":datasetID"] = datasetID
                    varMap[":fileID"] = fileID
                    varMap[":PandaID"] = pandaID
                    varMap[":status"] = EventServiceUtils.ST_discarded
                    varMap[":esFinished"] = EventServiceUtils.ST_finished
                    varMap[":esDone"] = EventServiceUtils.ST_done
                    if not job.notDiscardEvents():
                        self.cur.execute(sqlDE + comment, varMap)
                        nRowsDis += self.cur.rowcount
                    varMap[":status"] = EventServiceUtils.ST_cancelled
                    varMap[":esDiscarded"] = EventServiceUtils.ST_discarded
                    varMap[":esCancelled"] = EventServiceUtils.ST_cancelled
                    varMap[":esCorrupted"] = EventServiceUtils.ST_corrupted
                    varMap[":esFatal"] = EventServiceUtils.ST_fatal
                    varMap[":esFailed"] = EventServiceUtils.ST_failed
                    self.cur.execute(sqlCE + comment, varMap)
                    nRowsCan += self.cur.rowcount
                tmp_log.debug(f"{pandaID} discarded {nRowsDis} events")
                tmp_log.debug(f"{pandaID} cancelled {nRowsCan} events")
            # commit
            if useCommit:
                if not self._commit():
                    raise RuntimeError("Commit error")
            tmp_log.debug(f"killed {nKilled} jobs")
            return True
        except Exception:
            # roll back
            if useCommit:
                self._rollback()
            # error
            self.dump_error_message(tmp_log)
            if not useCommit:
                raise
            return False

    # kill unused consumers related to an ES job
    def killUnusedEventServiceConsumers(self, job, useCommit=True, killAll=False, checkAttemptNr=False):
        comment = " /* DBProxy.killUnusedEventServiceConsumers */"
        tmp_log = self.create_tagged_logger(comment, f"PandaID={job.PandaID}")
        tmp_log.debug(f"start")
        try:
            # begin transaction
            if useCommit:
                self.conn.begin()
            self.cur.arraysize = 100000
            # get dataset
            sqlPD = "SELECT f.datasetID,f.fileID FROM ATLAS_PANDA.JEDI_Datasets d,ATLAS_PANDA.filesTable4 f "
            sqlPD += "WHERE d.jediTaskID=:jediTaskID AND d.type IN (:type1,:type2) AND d.masterID IS NULL "
            sqlPD += "AND f.PandaID=:PandaID AND f.jeditaskID=f.jediTaskID AND f.datasetID=d.datasetID "
            varMap = {}
            varMap[":jediTaskID"] = job.jediTaskID
            varMap[":PandaID"] = job.PandaID
            varMap[":type1"] = "input"
            varMap[":type2"] = "pseudo_input"
            self.cur.execute(sqlPD + comment, varMap)
            resPD = self.cur.fetchall()
            # get PandaIDs
            killPandaIDs = set()
            myAttemptNr = None
            sqlCP = "SELECT PandaID,attemptNr FROM ATLAS_PANDA.filesTable4 WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
            for datasetID, fileID in resPD:
                if fileID is None:
                    continue
                varMap = {}
                varMap[":jediTaskID"] = job.jediTaskID
                varMap[":datasetID"] = datasetID
                varMap[":fileID"] = fileID
                self.cur.execute(sqlCP + comment, varMap)
                resCP = self.cur.fetchall()
                for esPandaID, esAttemptNr in resCP:
                    if esPandaID == job.PandaID:
                        myAttemptNr = esAttemptNr
                        continue
                    killPandaIDs.add((esPandaID, esAttemptNr))
            # kill consumers
            nKilled = 0
            sqlDJS = f"SELECT {JobSpec.columnNames()} "
            sqlDJS += "FROM ATLAS_PANDA.{0} WHERE PandaID=:PandaID"
            sqlDJD = "DELETE FROM ATLAS_PANDA.{0} WHERE PandaID=:PandaID"
            sqlDJI = f"INSERT INTO ATLAS_PANDA.jobsArchived4 ({JobSpec.columnNames()}) "
            sqlDJI += JobSpec.bindValuesExpression()
            sqlFSF = "UPDATE ATLAS_PANDA.filesTable4 SET status=:newStatus "
            sqlFSF += "WHERE PandaID=:PandaID AND type IN (:type1,:type2) "
            sqlFMod = "UPDATE ATLAS_PANDA.filesTable4 SET modificationTime=:modificationTime WHERE PandaID=:PandaID"
            sqlMMod = "UPDATE ATLAS_PANDA.metaTable SET modificationTime=:modificationTime WHERE PandaID=:PandaID"
            sqlPMod = "UPDATE ATLAS_PANDA.jobParamsTable SET modificationTime=:modificationTime WHERE PandaID=:PandaID"
            for pandaID, attemptNr in killPandaIDs:
                # read job
                varMap = {}
                varMap[":PandaID"] = pandaID
                self.cur.arraysize = 10
                deletedFlag = False
                notToDelete = False
                for tableName in ["jobsActive4", "jobsDefined4"]:
                    # check attemptNr
                    if checkAttemptNr and attemptNr != myAttemptNr:
                        tmp_log.debug(f"skip to kill {pandaID} since attemptNr:{attemptNr} is different from mine={myAttemptNr}")
                        notToDelete = True
                        break
                    self.cur.execute(sqlDJS.format(tableName) + comment, varMap)
                    resJob = self.cur.fetchall()
                    if len(resJob) == 0:
                        continue
                    # instantiate JobSpec
                    dJob = JobSpec()
                    dJob.pack(resJob[0])
                    # not kill all status
                    if not killAll:
                        if dJob.jobStatus not in ["activated", "assigned", "throttled"]:
                            tmp_log.debug(f"skip to kill unused consumer {pandaID} since status={dJob.jobStatus}")
                            notToDelete = True
                            break
                    # skip merge
                    if EventServiceUtils.isEventServiceMerge(dJob):
                        tmp_log.debug(f"skip to kill merge {pandaID}")
                        notToDelete = True
                        break
                    # skip jumbo
                    if EventServiceUtils.isJumboJob(dJob):
                        tmp_log.debug(f"skip to kill jumbo {pandaID}")
                        notToDelete = True
                        break
                    # delete
                    varMap = {}
                    varMap[":PandaID"] = pandaID
                    self.cur.execute(sqlDJD.format(tableName) + comment, varMap)
                    retD = self.cur.rowcount
                    if retD != 0:
                        deletedFlag = True
                        break
                # not to be deleted
                if notToDelete:
                    continue
                # not found
                if not deletedFlag:
                    tmp_log.debug(f"skip to kill {pandaID} as already deleted")
                    continue
                tmp_log.debug(f"kill unused consumer {pandaID}")
                # set error code
                dJob.jobStatus = "closed"
                dJob.endTime = naive_utcnow()
                if EventServiceUtils.isJobCloningJob(dJob):
                    dJob.jobSubStatus = "jc_unlock"
                    dJob.taskBufferErrorCode = ErrorCode.EC_JobCloningUnlock
                    dJob.taskBufferErrorDiag = f"closed since another clone PandaID={job.PandaID} got semaphore while waiting in the queue"
                else:
                    dJob.jobSubStatus = "es_unused"
                    dJob.taskBufferErrorCode = ErrorCode.EC_EventServiceUnused
                    dJob.taskBufferErrorDiag = "killed since all event ranges were processed by other consumers while waiting in the queue"
                dJob.modificationTime = dJob.endTime
                dJob.stateChangeTime = dJob.endTime
                # insert
                self.cur.execute(sqlDJI + comment, dJob.valuesMap())
                # set file status
                varMap = {}
                varMap[":PandaID"] = pandaID
                varMap[":type1"] = "output"
                varMap[":type2"] = "log"
                varMap[":newStatus"] = "failed"
                self.cur.execute(sqlFSF + comment, varMap)
                # update files,metadata,parametes
                varMap = {}
                varMap[":PandaID"] = pandaID
                varMap[":modificationTime"] = dJob.modificationTime
                self.cur.execute(sqlFMod + comment, varMap)
                self.cur.execute(sqlMMod + comment, varMap)
                self.cur.execute(sqlPMod + comment, varMap)
                nKilled += 1
                # record status change
                self.recordStatusChange(dJob.PandaID, dJob.jobStatus, jobInfo=dJob, useCommit=False)
            # commit
            if useCommit:
                if not self._commit():
                    raise RuntimeError("Commit error")
            tmp_log.debug(f"killed {nKilled} jobs")
            return True
        except Exception:
            # roll back
            if useCommit:
                self._rollback()
            # error
            self.dump_error_message(tmp_log)
            if not useCommit:
                raise
            return False

    # kill unused event ranges
    def killUnusedEventRanges(self, jediTaskID, jobsetID):
        comment = " /* DBProxy.killUnusedEventRanges */"
        tmp_log = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID} jobsetID={jobsetID}")
        # sql to kill event ranges
        varMap = {}
        varMap[":jediTaskID"] = jediTaskID
        varMap[":jobsetID"] = jobsetID
        varMap[":esReady"] = EventServiceUtils.ST_ready
        varMap[":esCancelled"] = EventServiceUtils.ST_cancelled
        sqlCE = f"UPDATE {panda_config.schemaJEDI}.JEDI_Events "
        sqlCE += "SET status=:esCancelled "
        sqlCE += "WHERE jediTaskID=:jediTaskID AND pandaID=:jobsetID "
        sqlCE += "AND status=:esReady "
        self.cur.execute(sqlCE, varMap)
        nRowsCan = self.cur.rowcount
        tmp_log.debug(f"cancelled {nRowsCan} events")

    # release unprocessed events
    def release_unprocessed_events(self, jedi_task_id, panda_id):
        comment = " /* DBProxy.release_unprocessed_events */"
        tmp_log = self.create_tagged_logger(comment, f"jediTaskID={jedi_task_id} PandaID={panda_id}")
        # look for hopeless events
        varMap = {}
        varMap[":jediTaskID"] = jedi_task_id
        varMap[":PandaID"] = panda_id
        varMap[":esReady"] = EventServiceUtils.ST_ready
        varMap[":esFinished"] = EventServiceUtils.ST_finished
        varMap[":esFailed"] = EventServiceUtils.ST_failed
        sqlBE = (
            "SELECT job_processID FROM {0}.JEDI_Events "
            "WHERE jediTaskID=:jediTaskID AND pandaID=:PandaID "
            "AND status NOT IN (:esReady,:esFinished,:esFailed) "
            "AND attemptNr=1 "
        ).format(panda_config.schemaJEDI)
        self.cur.execute(sqlBE, varMap)
        resBD = self.cur.fetchall()
        if len(resBD) > 0:
            # report very large loss
            c = iDDS_Client(idds.common.utils.get_rest_host())
            for (sample_id,) in resBD:
                tmp_log.debug(f"reporting large loss for id={sample_id}")
                c.update_hyperparameter(workload_id=jedi_task_id, request_id=None, id=sample_id, loss=1e5)
        # release
        sqlCE = f"UPDATE {panda_config.schemaJEDI}.JEDI_Events "
        sqlCE += (
            "SET status=(CASE WHEN attemptNr>1 THEN :esReady ELSE :esFailed END),"
            "pandaID=(CASE WHEN attemptNr>1 THEN 0 ELSE pandaID END),"
            "attemptNr=attemptNr-1 "
            "WHERE jediTaskID=:jediTaskID AND pandaID=:PandaID "
            "AND status NOT IN (:esReady,:esFinished,:esFailed) "
        )
        self.cur.execute(sqlCE, varMap)
        nRowsCan = self.cur.rowcount
        tmp_log.debug(f"released {nRowsCan} events")

    # kill used event ranges
    def killUsedEventRanges(self, jediTaskID, pandaID, notDiscardEvents=False):
        comment = " /* DBProxy.killUsedEventRanges */"
        tmp_log = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID} pandaID={pandaID}")
        # sql to discard or cancel event ranges
        sqlDE = "UPDATE "
        sqlDE += f"{panda_config.schemaJEDI}.JEDI_Events tab "
        sqlDE += "SET status=:status "
        sqlDE += "WHERE jediTaskID=:jediTaskID AND PandaID=:PandaID "
        sqlDE += "AND status IN (:esFinished,:esDone) "
        sqlCE = "UPDATE "
        sqlCE += f"{panda_config.schemaJEDI}.JEDI_Events tab "
        sqlCE += "SET status=:status "
        sqlCE += "WHERE jediTaskID=:jediTaskID AND PandaID=:PandaID "
        sqlCE += "AND NOT status IN (:esFinished,:esDone,:esDiscarded,:esCancelled,:esFailed,:esFatal,:esCorrupted) "
        varMap = {}
        varMap[":jediTaskID"] = jediTaskID
        varMap[":PandaID"] = pandaID
        varMap[":status"] = EventServiceUtils.ST_discarded
        varMap[":esFinished"] = EventServiceUtils.ST_finished
        varMap[":esDone"] = EventServiceUtils.ST_done
        if not notDiscardEvents:
            self.cur.execute(sqlDE + comment, varMap)
            nRowsDis = self.cur.rowcount
        else:
            nRowsDis = 0
        varMap[":status"] = EventServiceUtils.ST_cancelled
        varMap[":esDiscarded"] = EventServiceUtils.ST_discarded
        varMap[":esCancelled"] = EventServiceUtils.ST_cancelled
        varMap[":esCorrupted"] = EventServiceUtils.ST_corrupted
        varMap[":esFatal"] = EventServiceUtils.ST_fatal
        varMap[":esFailed"] = EventServiceUtils.ST_failed
        self.cur.execute(sqlCE + comment, varMap)
        nRowsCan = self.cur.rowcount
        tmp_log.debug(f"discarded {nRowsDis} events")
        tmp_log.debug(f"cancelled {nRowsCan} events")

    # set corrupted events
    def setCorruptedEventRanges(self, jediTaskID, pandaID):
        comment = " /* DBProxy.setCorruptedEventRanges */"
        tmp_log = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID} pandaID={pandaID}")
        # sql to get bad files
        sqlBD = "SELECT lfn FROM ATLAS_PANDA.filesTable4 WHERE PandaID=:PandaID AND type=:type AND status=:status "
        # sql to get PandaID produced the bad file
        sqlPP = "SELECT row_ID,PandaID FROM ATLAS_PANDA.filesTable4 WHERE lfn=:lfn AND type=:type "
        # sql to get PandaIDs with jobMetrics
        sqlJJ = "SELECT /*+ INDEX_RS_ASC(tab JEDI_EVENTS_FILEID_IDX) NO_INDEX_FFS(tab JEDI_EVENTS_PK) NO_INDEX_SS(tab JEDI_EVENTS_PK) */ "
        sqlJJ += "DISTINCT e.PandaID FROM ATLAS_PANDA.filesTable4 f,ATLAS_PANDA.JEDI_Events e "
        sqlJJ += "WHERE f.PandaID=:PandaID AND f.type IN (:type1,:type2) "
        sqlJJ += "AND e.jediTaskID=f.jediTaskID AND e.datasetID=f.datasetID AND e.fileID=f.fileID "
        # sql to get jobMetrics
        sqlJM = "SELECT jobMetrics FROM ATLAS_PANDA.jobsArchived4 WHERE PandaID=:PandaID "
        sqlJM += "UNION "
        sqlJM += "SELECT jobMetrics FROM ATLAS_PANDAARCH.jobsArchived WHERE PandaID=:PandaID AND modificationTime=CURRENT_DATE-90 "
        # sql to get dataset and file IDs
        sqlGI = "SELECT datasetID,fileID FROM ATLAS_PANDA.filesTable4 "
        sqlGI += "WHERE PandaID=:PandaID AND type IN (:t1,:t2) "
        # sql to update event ranges
        sqlCE = "UPDATE "
        sqlCE += f"{panda_config.schemaJEDI}.JEDI_Events tab "
        sqlCE += "SET status=:esCorrupted "
        sqlCE += "WHERE jediTaskID=:jediTaskID AND PandaID=:PandaID AND zipRow_ID=:row_ID "
        sqlCE += "AND datasetID=:datasetID AND fileID=:fileID AND status=:esDone "
        # sql to update event ranges with jobMetrics
        sqlJE = "UPDATE "
        sqlJE += f"{panda_config.schemaJEDI}.JEDI_Events tab "
        sqlJE += "SET status=:esCorrupted "
        sqlJE += "WHERE jediTaskID=:jediTaskID AND PandaID=:PandaID "
        sqlJE += "AND datasetID=:datasetID AND fileID=:fileID AND status=:esDone "
        # get bad files
        varMap = {}
        varMap[":PandaID"] = pandaID
        varMap[":status"] = "corrupted"
        varMap[":type"] = "zipinput"
        self.cur.execute(sqlBD + comment, varMap)
        resBD = self.cur.fetchall()
        for (lfn,) in resBD:
            # get origon PandaID
            nCor = 0
            varMap = {}
            varMap[":lfn"] = lfn
            varMap[":type"] = "zipoutput"
            self.cur.execute(sqlPP + comment, varMap)
            resPP = self.cur.fetchall()
            if len(resPP) > 0:
                # with zipoutput
                for zipRow_ID, oPandaID in resPP:
                    # get dataset and file IDs
                    varMap = {}
                    varMap[":PandaID"] = oPandaID
                    varMap[":t1"] = "input"
                    varMap[":t2"] = "pseudo_input"
                    self.cur.execute(sqlGI + comment, varMap)
                    resGI = self.cur.fetchall()
                    # set corrupted
                    for datasetID, fileID in resGI:
                        varMap = {}
                        varMap[":PandaID"] = oPandaID
                        varMap[":row_ID"] = zipRow_ID
                        varMap[":jediTaskID"] = jediTaskID
                        varMap[":datasetID"] = datasetID
                        varMap[":fileID"] = fileID
                        varMap[":esDone"] = EventServiceUtils.ST_done
                        varMap[":esCorrupted"] = EventServiceUtils.ST_corrupted
                        self.cur.execute(sqlCE + comment, varMap)
                        nCor += self.cur.rowcount
            else:
                # check jobMetrics
                varMap = dict()
                varMap[":PandaID"] = pandaID
                varMap[":type1"] = "input"
                varMap[":type2"] = "pseudo_input"
                self.cur.execute(sqlJJ + comment, varMap)
                resJJ = self.cur.fetchall()
                # get jobMetrics
                for (oPandaID,) in resJJ:
                    varMap = dict()
                    varMap[":PandaID"] = oPandaID
                    self.cur.execute(sqlJM + comment, varMap)
                    resJM = self.cur.fetchone()
                    if resJM is not None:
                        (jobMetrics,) = resJM
                        if jobMetrics is not None and f"outputZipName={lfn}" in jobMetrics:
                            # get dataset and file IDs
                            varMap = {}
                            varMap[":PandaID"] = oPandaID
                            varMap[":t1"] = "input"
                            varMap[":t2"] = "pseudo_input"
                            self.cur.execute(sqlGI + comment, varMap)
                            resGI = self.cur.fetchall()
                            # set corrupted
                            for datasetID, fileID in resGI:
                                varMap = {}
                                varMap[":PandaID"] = oPandaID
                                varMap[":jediTaskID"] = jediTaskID
                                varMap[":datasetID"] = datasetID
                                varMap[":fileID"] = fileID
                                varMap[":esDone"] = EventServiceUtils.ST_done
                                varMap[":esCorrupted"] = EventServiceUtils.ST_corrupted
                                self.cur.execute(sqlJE + comment, varMap)
                                nCor += self.cur.rowcount
                            break
            tmp_log.debug(f"{nCor} corrupted events in {lfn}")

    # check if all events are done
    def checkAllEventsDone(self, job, pandaID, useCommit=False, dumpLog=True, getProcStatus=False):
        comment = " /* DBProxy.checkAllEventsDone */"
        if job is not None:
            pandaID = job.PandaID
        tmp_log = self.create_tagged_logger(comment, f"PandaID={pandaID}")
        if dumpLog:
            tmp_log.debug("start")
        try:
            # get files
            sqlF = f"SELECT type,jediTaskID,datasetID,fileID FROM {panda_config.schemaPANDA}.filesTable4 "
            sqlF += "WHERE PandaID=:PandaID AND type=:type "
            # check if all events are done
            sqlEOC = "SELECT /*+ INDEX_RS_ASC(tab JEDI_EVENTS_FILEID_IDX) NO_INDEX_FFS(tab JEDI_EVENTS_PK) NO_INDEX_SS(tab JEDI_EVENTS_PK) */ "
            sqlEOC += f"distinct PandaID,status FROM {panda_config.schemaJEDI}.JEDI_Events tab "
            sqlEOC += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
            sqlEOC += "AND NOT status IN (:esDone,:esDiscarded,:esCancelled,:esFatal,:esCorrupted,:esFailed,:esFinished) "
            sqlEOC += "AND NOT (status=:esReady AND attemptNr=0) "
            # get jumbo jobs
            sqlGJ = "SELECT /*+ INDEX_RS_ASC(tab JEDI_EVENTS_FILEID_IDX) NO_INDEX_FFS(tab JEDI_EVENTS_PK) NO_INDEX_SS(tab JEDI_EVENTS_PK) */ "
            sqlGJ += f"distinct PandaID FROM {panda_config.schemaJEDI}.JEDI_Events tab "
            sqlGJ += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
            sqlGJ += "AND status IN (:esRunning,:esSent,:esFinished,:esDone) "
            # check if job is still alive
            sqlJAL = f"SELECT jobStatus,eventService FROM {panda_config.schemaPANDA}.jobsActive4 "
            sqlJAL += "WHERE PandaID=:PandaID "
            # begin transaction
            if useCommit:
                self.conn.begin()
            self.cur.arraysize = 1000000
            # get files if needed
            if job is not None:
                fileList = job.Files
            else:
                varMap = {}
                varMap[":PandaID"] = pandaID
                varMap[":type"] = "input"
                self.cur.execute(sqlF + comment, varMap)
                resF = self.cur.fetchall()
                fileList = []
                for tmpType, tmpJediTaskID, tmpDatasetID, tmpFileID in resF:
                    fileSpec = FileSpec()
                    fileSpec.type = tmpType
                    fileSpec.jediTaskID = tmpJediTaskID
                    fileSpec.datasetID = tmpDatasetID
                    fileSpec.fileID = tmpFileID
                    fileList.append(fileSpec)
            # check all inputs
            allDone = True
            proc_status = None
            checkedPandaIDs = set()
            jobStatusMap = dict()
            for fileSpec in fileList:
                if fileSpec.type == "input":
                    varMap = {}
                    varMap[":jediTaskID"] = fileSpec.jediTaskID
                    varMap[":datasetID"] = fileSpec.datasetID
                    varMap[":fileID"] = fileSpec.fileID
                    varMap[":esDone"] = EventServiceUtils.ST_done
                    varMap[":esFinished"] = EventServiceUtils.ST_finished
                    varMap[":esDiscarded"] = EventServiceUtils.ST_discarded
                    varMap[":esCancelled"] = EventServiceUtils.ST_cancelled
                    varMap[":esCorrupted"] = EventServiceUtils.ST_corrupted
                    varMap[":esFatal"] = EventServiceUtils.ST_fatal
                    varMap[":esFailed"] = EventServiceUtils.ST_failed
                    varMap[":esReady"] = EventServiceUtils.ST_ready
                    self.cur.execute(sqlEOC + comment, varMap)
                    resEOC = self.cur.fetchall()
                    for pandaID, esStatus in resEOC:
                        # skip redundant lookup
                        if pandaID in checkedPandaIDs:
                            continue
                        checkedPandaIDs.add(pandaID)
                        # not yet dispatched
                        if esStatus == EventServiceUtils.ST_ready:
                            tmpStr = "some events are not yet dispatched "
                            tmpStr += f"for jediTaskID={fileSpec.jediTaskID} datasetID={fileSpec.datasetID} fileID={fileSpec.fileID}"
                            if dumpLog:
                                tmp_log.debug(tmpStr)
                            allDone = False
                            break
                        # check job
                        varMap = {}
                        varMap[":PandaID"] = pandaID
                        self.cur.execute(sqlJAL + comment, varMap)
                        resJAL = self.cur.fetchone()
                        if resJAL is None:
                            # no active job
                            tmpStr = "no associated job is in active "
                            tmpStr += f"for jediTaskID={fileSpec.jediTaskID} datasetID={fileSpec.datasetID} fileID={fileSpec.fileID}"
                            if dumpLog:
                                tmp_log.debug(tmpStr)
                            jobStatusMap[pandaID] = None
                        else:
                            # still active
                            tmpStr = f"PandaID={pandaID} is associated in {resJAL[0]} "
                            tmpStr += f"for jediTaskID={fileSpec.jediTaskID} datasetID={fileSpec.datasetID} fileID={fileSpec.fileID}"
                            if dumpLog:
                                tmp_log.debug(tmpStr)
                            allDone = False
                            if resJAL[1] == EventServiceUtils.jumboJobFlagNumber:
                                jobStatusMap[pandaID] = resJAL[0]
                            else:
                                jobStatusMap[pandaID] = None
                            break
                        # escape
                        if not allDone:
                            break
                # escape
                if not allDone:
                    break
            # get proc_status
            if not allDone and getProcStatus:
                proc_status = "queued"
                to_escape = False
                is_starting = False
                for fileSpec in fileList:
                    if fileSpec.type == "input":
                        varMap = {}
                        varMap[":jediTaskID"] = fileSpec.jediTaskID
                        varMap[":datasetID"] = fileSpec.datasetID
                        varMap[":fileID"] = fileSpec.fileID
                        varMap[":esDone"] = EventServiceUtils.ST_done
                        varMap[":esFinished"] = EventServiceUtils.ST_finished
                        varMap[":esRunning"] = EventServiceUtils.ST_running
                        varMap[":esSent"] = EventServiceUtils.ST_sent
                        self.cur.execute(sqlGJ + comment, varMap)
                        resGJ = self.cur.fetchall()
                        for (pandaID,) in resGJ:
                            if pandaID not in jobStatusMap:
                                # get job
                                varMap = {}
                                varMap[":PandaID"] = pandaID
                                self.cur.execute(sqlJAL + comment, varMap)
                                resJAL = self.cur.fetchone()
                                if resJAL is None:
                                    jobStatusMap[pandaID] = None
                                else:
                                    if resJAL[1] == EventServiceUtils.jumboJobFlagNumber:
                                        jobStatusMap[pandaID] = resJAL[0]
                                    else:
                                        jobStatusMap[pandaID] = None
                            # check status
                            if jobStatusMap[pandaID] == "running":
                                proc_status = "running"
                                to_escape = True
                                break
                            elif jobStatusMap[pandaID] == "starting":
                                is_starting = True
                        if to_escape:
                            break
                if proc_status == "queued" and is_starting:
                    proc_status = "starting"
            # commit
            if useCommit:
                if not self._commit():
                    raise RuntimeError("Commit error")
            if dumpLog:
                tmp_log.debug(f"done with {allDone} {proc_status}")
            if getProcStatus:
                return (allDone, proc_status)
            return allDone
        except Exception:
            # roll back
            if useCommit:
                self._rollback()
            # error
            self.dump_error_message(tmp_log)
            if getProcStatus:
                return (None, None)
            return None

    # get co-jumbo jobs to be finished
    def getCoJumboJobsToBeFinished(self, timeLimit, minPriority, maxJobs):
        comment = " /* DBProxy.getCoJumboJobsToBeFinished */"
        tmp_log = self.create_tagged_logger(comment)
        tmp_log.debug(f"start for minPriority={minPriority} timeLimit={timeLimit}")
        try:
            # get co-jumbo jobs
            sqlEOD = "SELECT PandaID,jediTaskID,jobStatus,computingSite,creationTime FROM ATLAS_PANDA.{0} "
            sqlEOD += "WHERE eventService=:eventService "
            sqlEOD += "AND (prodDBUpdateTime IS NULL OR prodDBUpdateTime<:timeLimit) "
            sqlEOD += "AND currentPriority>=:minPriority "
            # lock job
            sqlPL = "SELECT 1 FROM ATLAS_PANDA.{0} "
            sqlPL += "WHERE PandaID=:PandaID "
            sqlPL += "AND (prodDBUpdateTime IS NULL OR prodDBUpdateTime<:timeLimit) "
            sqlPL += "FOR UPDATE NOWAIT "
            sqlLK = "UPDATE ATLAS_PANDA.{0} "
            sqlLK += "SET prodDBUpdateTime=CURRENT_DATE "
            sqlLK += "WHERE PandaID=:PandaID "
            sqlLK += "AND (prodDBUpdateTime IS NULL OR prodDBUpdateTime<:timeLimit) "
            # get useJumbo
            sqlJM = f"SELECT useJumbo FROM {panda_config.schemaJEDI}.JEDI_Tasks "
            sqlJM += "WHERE jediTaskID=:jediTaskID "
            # get datasetID and fileID of the primary input
            sqlID = "SELECT f.datasetID,f.fileID,c.status,c.proc_status FROM {0}.JEDI_Datasets d,{0}.JEDI_Dataset_Contents c,{1}.filesTable4 f ".format(
                panda_config.schemaJEDI, panda_config.schemaPANDA
            )
            sqlID += "WHERE d.jediTaskID=:jediTaskID AND d.type IN (:t1,:t2) AND d.masterID IS NULL "
            sqlID += "AND f.jediTaskID=d.jediTaskID AND f.datasetID=d.datasetID AND f.PandaID=:PandaID "
            sqlID += "AND c.jediTaskID=d.jediTaskID AND c.datasetID=d.datasetID AND c.fileID=f.fileID "
            # get PandaIDs
            sqlCP = "SELECT PandaID FROM ATLAS_PANDA.filesTable4 "
            sqlCP += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
            # check jobs
            sqlWP = "SELECT 1 FROM ATLAS_PANDA.jobsDefined4 WHERE PandaID=:PandaID "
            sqlWP += "UNION "
            sqlWP += "SELECT 1 FROM ATLAS_PANDA.jobsActive4 WHERE PandaID=:PandaID "
            self.cur.arraysize = 1000000
            timeLimit = naive_utcnow() - datetime.timedelta(minutes=timeLimit)
            timeLimitWaiting = naive_utcnow() - datetime.timedelta(hours=6)
            retList = []
            # get jobs
            coJumboTobeKilled = set()
            useJumbos = dict()
            for tableName in ["jobsActive4", "jobsDefined4"]:
                self.conn.begin()
                varMap = {}
                varMap[":eventService"] = EventServiceUtils.coJumboJobFlagNumber
                varMap[":timeLimit"] = timeLimit
                varMap[":minPriority"] = minPriority
                self.cur.execute(sqlEOD.format(tableName) + comment, varMap)
                tmpRes = self.cur.fetchall()
                if not self._commit():
                    raise RuntimeError("Commit error")
                tmp_log.debug(f"checking {len(tmpRes)} co-jumbo jobs in {tableName}")
                checkedPandaIDs = set()
                iJobs = 0
                # scan all jobs
                for (
                    pandaID,
                    jediTaskID,
                    jobStatus,
                    computingSite,
                    creationTime,
                ) in tmpRes:
                    # lock job
                    self.conn.begin()
                    varMap = {}
                    varMap[":PandaID"] = pandaID
                    varMap[":timeLimit"] = timeLimit
                    toSkip = False
                    resPL = None
                    try:
                        # lock with NOWAIT
                        self.cur.execute(sqlPL.format(tableName) + comment, varMap)
                        resPL = self.cur.fetchone()
                    except Exception:
                        toSkip = True
                    if resPL is None:
                        toSkip = True
                    if toSkip:
                        tmp_log.debug(f"skipped PandaID={pandaID} jediTaskID={jediTaskID} in {tableName} since locked by another")
                    else:
                        # lock
                        self.cur.execute(sqlLK.format(tableName) + comment, varMap)
                        nRow = self.cur.rowcount
                        if nRow > 0:
                            iJobs += 1
                            # check if all events are done
                            allDone, proc_status = self.checkAllEventsDone(None, pandaID, False, True, True)
                            if allDone is True:
                                tmp_log.debug(f"locked co-jumbo PandaID={pandaID} jediTaskID={jediTaskID} to finish in {tableName}")
                                checkedPandaIDs.add(pandaID)
                            elif jobStatus == "waiting" and computingSite == EventServiceUtils.siteIdForWaitingCoJumboJobs and proc_status == "queued":
                                # check if jumbo is disabled
                                if jediTaskID not in useJumbos:
                                    varMap = {}
                                    varMap[":jediTaskID"] = jediTaskID
                                    self.cur.execute(sqlJM + comment, varMap)
                                    resJM = self.cur.fetchone()
                                    (useJumbos[jediTaskID],) = resJM
                                if useJumbos[jediTaskID] == "D" or creationTime < timeLimitWaiting:
                                    # get info of the primary input
                                    varMap = {}
                                    varMap[":jediTaskID"] = jediTaskID
                                    varMap[":PandaID"] = pandaID
                                    varMap[":t1"] = "input"
                                    varMap[":t2"] = "pseudo_input"
                                    self.cur.execute(sqlID + comment, varMap)
                                    resID = self.cur.fetchone()
                                    (
                                        datasetID,
                                        fileID,
                                        fileStatus,
                                        fileProcStatus,
                                    ) = resID
                                    if fileStatus == "running" and fileProcStatus == "queued":
                                        # count # of active consumers
                                        nAct = 0
                                        varMap = {}
                                        varMap[":jediTaskID"] = jediTaskID
                                        varMap[":datasetID"] = datasetID
                                        varMap[":fileID"] = fileID
                                        self.cur.execute(sqlCP + comment, varMap)
                                        resCP = self.cur.fetchall()
                                        for (tmpPandaID,) in resCP:
                                            varMap = {}
                                            varMap[":PandaID"] = tmpPandaID
                                            self.cur.execute(sqlWP + comment, varMap)
                                            resWP = self.cur.fetchone()
                                            if resWP is not None:
                                                nAct += 1
                                        if nAct > 0:
                                            tmp_log.debug(f"skip to kill PandaID={pandaID} jediTaskID={jediTaskID} due to {nAct} active consumers")
                                        else:
                                            tmp_log.debug(f"locked co-jumbo PandaID={pandaID} jediTaskID={jediTaskID} to kill")
                                            coJumboTobeKilled.add(pandaID)
                            if proc_status is not None:
                                self.updateInputStatusJedi(jediTaskID, pandaID, "queued", checkOthers=True)
                    if not self._commit():
                        raise RuntimeError("Commit error")
                    if iJobs >= maxJobs:
                        break
                retList.append(checkedPandaIDs)
            totJobs = 0
            for tmpList in retList:
                totJobs += len(tmpList)
            tmp_log.debug(f"got {totJobs} jobs to finish and {len(coJumboTobeKilled)} co-jumbo jobs to kill")
            retList.append(coJumboTobeKilled)
            return retList
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return None

    # check if there are done events
    def hasDoneEvents(self, jediTaskID, pandaID, jobSpec, useCommit=True):
        comment = " /* DBProxy.hasDoneEvents */"
        tmp_log = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID} PandaID={pandaID}")
        tmp_log.debug("start")
        retVal = False
        try:
            # sql to release events
            sqlR = f"UPDATE {panda_config.schemaJEDI}.JEDI_Events "
            if jobSpec.decAttOnFailedES():
                sqlR += "SET status=:newStatus,pandaID=event_offset,is_jumbo=NULL "
            else:
                sqlR += "SET status=:newStatus,attemptNr=attemptNr-1,pandaID=event_offset,is_jumbo=NULL "
            sqlR += "WHERE jediTaskID=:jediTaskID AND pandaID=:pandaID AND status IN (:esSent,:esRunning) "
            # sql to check event
            sqlF = f"SELECT COUNT(*) FROM {panda_config.schemaJEDI}.JEDI_Events "
            sqlF += "WHERE jediTaskID=:jediTaskID AND PandaID=:pandaID AND status IN (:esDone,:esFinished) "
            # begin transaction
            if useCommit:
                self.conn.begin()
            # release events
            varMap = {}
            varMap[":pandaID"] = pandaID
            varMap[":jediTaskID"] = jediTaskID
            varMap[":esSent"] = EventServiceUtils.ST_sent
            varMap[":esRunning"] = EventServiceUtils.ST_running
            varMap[":newStatus"] = EventServiceUtils.ST_ready
            self.cur.execute(sqlR + comment, varMap)
            resR = self.cur.rowcount
            tmp_log.debug(f"released {resR} event ranges")
            # check event
            varMap = {}
            varMap[":pandaID"] = pandaID
            varMap[":jediTaskID"] = jediTaskID
            varMap[":esDone"] = EventServiceUtils.ST_done
            varMap[":esFinished"] = EventServiceUtils.ST_finished
            self.cur.execute(sqlF + comment, varMap)
            resF = self.cur.fetchone()
            # commit
            if useCommit:
                if not self._commit():
                    raise RuntimeError("Commit error")
            nFinished = 0
            if resF is not None:
                (nFinished,) = resF
            if nFinished > 0:
                retVal = True
            else:
                retVal = False
            tmp_log.debug(f"finished {nFinished} event ranges. ret={retVal}")
            return retVal
        except Exception:
            # roll back
            if useCommit:
                self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return retVal

    # check if there are events to be processed
    def hasReadyEvents(self, jediTaskID):
        comment = " /* DBProxy.hasReadyEvents */"
        tmp_log = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID}")
        tmp_log.debug("start")
        retVal = None
        try:
            # sql to check event
            sqlF = f"SELECT COUNT(*) FROM {panda_config.schemaJEDI}.JEDI_Events "
            sqlF += "WHERE jediTaskID=:jediTaskID AND status=:esReady AND attemptNr>:minAttemptNr "
            # check event
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            varMap[":esReady"] = EventServiceUtils.ST_ready
            varMap[":minAttemptNr"] = 0
            # begin transaction
            self.conn.begin()
            self.cur.execute(sqlF + comment, varMap)
            resF = self.cur.fetchone()
            nReady = None
            if resF is not None:
                (nReady,) = resF
                retVal = nReady > 0
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmp_log.debug(f"{nReady} ready events. ret={retVal}")
            return retVal
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return None

    # get number of events to be processed
    def getNumReadyEvents(self, jediTaskID):
        comment = " /* DBProxy.getNumReadyEvents */"
        tmp_log = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID}")
        tmp_log.debug("start")
        nReady = None
        try:
            # sql to count event
            sqlF = f"SELECT COUNT(*) FROM {panda_config.schemaJEDI}.JEDI_Events "
            sqlF += "WHERE jediTaskID=:jediTaskID AND status=:esReady AND attemptNr>:minAttemptNr "
            # count event
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            varMap[":esReady"] = EventServiceUtils.ST_ready
            varMap[":minAttemptNr"] = 0
            # begin transaction
            self.conn.begin()
            self.cur.execute(sqlF + comment, varMap)
            resF = self.cur.fetchone()
            nReady = None
            if resF is not None:
                (nReady,) = resF
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmp_log.debug(f"{nReady} ready events")
            return nReady
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return None

    # update related ES jobs when ES-merge job is done
    def updateRelatedEventServiceJobs(self, job, killEvents=False, forceFailed=False):
        comment = " /* DBProxy.updateRelatedEventServiceJobs */"
        tmp_log = self.create_tagged_logger(comment, f"PandaID={job.PandaID}")
        if forceFailed:
            jobStatus = "failed"
        else:
            jobStatus = job.jobStatus
        if not forceFailed and jobStatus not in ["finished"] and not (killEvents and not job.notDiscardEvents()):
            tmp_log.debug(f"skip jobStatus={jobStatus} killEvents={killEvents} discard={job.notDiscardEvents()}")
            return True
        tmp_log.debug(f"start jobStatus={jobStatus} killEvents={killEvents} discard={job.notDiscardEvents()}")
        try:
            # sql to read range
            sqlRR = "SELECT /*+ INDEX_RS_ASC(tab JEDI_EVENTS_FILEID_IDX) NO_INDEX_FFS(tab JEDI_EVENTS_PK) NO_INDEX_SS(tab JEDI_EVENTS_PK) */ "
            sqlRR += "distinct PandaID "
            sqlRR += f"FROM {panda_config.schemaJEDI}.JEDI_Events tab "
            sqlRR += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID AND status IN (:es_done,:es_finished,:es_merged) "
            # loop over all files
            esPandaIDs = set()
            for tmpFile in job.Files:
                # only for input
                if tmpFile.type in ["input", "pseudo_input"]:
                    # get ranges
                    if tmpFile.fileID is [None, "NULL"]:
                        continue
                    varMap = {}
                    varMap[":jediTaskID"] = tmpFile.jediTaskID
                    varMap[":datasetID"] = tmpFile.datasetID
                    varMap[":fileID"] = tmpFile.fileID
                    varMap[":es_done"] = EventServiceUtils.ST_done
                    varMap[":es_finished"] = EventServiceUtils.ST_finished
                    varMap[":es_merged"] = EventServiceUtils.ST_merged
                    self.cur.execute(sqlRR + comment, varMap)
                    resRR = self.cur.fetchall()
                    for (tmpPandaID,) in resRR:
                        esPandaIDs.add(tmpPandaID)
            # sql to update ES job
            sqlUE = "UPDATE {0} SET jobStatus=:newStatus,stateChangeTime=CURRENT_DATE,taskBufferErrorDiag=:errDiag "
            if jobStatus in ["failed"]:
                updateSubStatus = True
                sqlUE += ",jobSubStatus=:jobSubStatus "
            else:
                updateSubStatus = False
            sqlUE += "WHERE PandaID=:PandaID AND jobStatus in (:oldStatus1,:oldStatus2,:oldStatus3) AND modificationTime>(CURRENT_DATE-90) "
            sqlUE += "AND NOT eventService IN (:esJumbo) "
            for tmpPandaID in esPandaIDs:
                varMap = {}
                varMap[":PandaID"] = tmpPandaID
                varMap[":newStatus"] = jobStatus
                varMap[":oldStatus1"] = "closed"
                varMap[":oldStatus2"] = "merging"
                varMap[":oldStatus3"] = "failed"
                varMap[":esJumbo"] = EventServiceUtils.jumboJobFlagNumber
                if updateSubStatus is True:
                    if forceFailed:
                        varMap[":jobSubStatus"] = "es_discard"
                    elif EventServiceUtils.isEventServiceMerge(job):
                        varMap[":jobSubStatus"] = f"es_merge_{jobStatus}"
                    else:
                        varMap[":jobSubStatus"] = f"es_ass_{jobStatus}"
                if forceFailed:
                    varMap[":errDiag"] = f"{jobStatus} to discard old events to retry in PandaID={job.PandaID}"
                else:
                    varMap[":errDiag"] = f"{jobStatus} since an associated ES or merge job PandaID={job.PandaID} {jobStatus}"
                isUpdated = False
                for tableName in [
                    "ATLAS_PANDA.jobsArchived4",
                    "ATLAS_PANDAARCH.jobsArchived",
                ]:
                    self.cur.execute(sqlUE.format(tableName) + comment, varMap)
                    nRow = self.cur.rowcount
                    if nRow > 0:
                        tmp_log.debug(f"change PandaID={tmpPandaID} to {jobStatus}")
                        isUpdated = True
                # kill processed events if necessary
                if killEvents and isUpdated:
                    self.killUsedEventRanges(job.jediTaskID, tmpPandaID, job.notDiscardEvents())
            tmp_log.debug("done")
            return True
        except Exception:
            # error
            self.dump_error_message(tmp_log)
            return False

    # disable further reattempt for pmerge
    def disableFurtherReattempt(self, jobSpec):
        comment = " /* JediDBProxy.disableFurtherReattempt */"
        tmp_log = self.create_tagged_logger(comment, f"PandaID={jobSpec.PandaID}")
        # sql to update file
        sqlFJ = f"UPDATE {panda_config.schemaJEDI}.JEDI_Dataset_Contents "
        sqlFJ += "SET maxAttempt=attemptNr-1 "
        sqlFJ += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
        sqlFJ += "AND attemptNr=:attemptNr AND keepTrack=:keepTrack "
        nRow = 0
        for tmpFile in jobSpec.Files:
            # skip if no JEDI
            if tmpFile.fileID == "NULL":
                continue
            # only input
            if tmpFile.type not in ["input", "pseudo_input"]:
                continue
            # update JEDI contents
            varMap = {}
            varMap[":jediTaskID"] = tmpFile.jediTaskID
            varMap[":datasetID"] = tmpFile.datasetID
            varMap[":fileID"] = tmpFile.fileID
            varMap[":attemptNr"] = tmpFile.attemptNr
            varMap[":keepTrack"] = 1
            self.cur.execute(sqlFJ + comment, varMap)
            nRow += self.cur.rowcount
        # finish
        tmp_log.debug(f"done with nRows={nRow}")
        return

    # get active consumers
    def getActiveConsumers(self, jediTaskID, jobsetID, myPandaID):
        comment = " /* DBProxy.getActiveConsumers */"
        tmp_log = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID} jobsetID={jobsetID} PandaID={myPandaID}")
        tmp_log.debug("start")
        try:
            # sql to get sites where consumers are active
            sqlA = "SELECT PandaID FROM ATLAS_PANDA.jobsActive4 WHERE jediTaskID=:jediTaskID AND jobsetID=:jobsetID "
            sqlA += "UNION "
            sqlA += "SELECT PandaID FROM ATLAS_PANDA.jobsDefined4 WHERE jediTaskID=:jediTaskID AND jobsetID=:jobsetID "
            # get IDs
            ids = set()
            varMap = dict()
            varMap[":jediTaskID"] = jediTaskID
            varMap[":jobsetID"] = jobsetID
            self.cur.execute(sqlA + comment, varMap)
            resA = self.cur.fetchall()
            for (pandaID,) in resA:
                if pandaID != myPandaID:
                    ids.add(pandaID)
            nIDs = len(ids)
            if nIDs == 0:
                # get dataset
                sqlPD = "SELECT f.datasetID,f.fileID FROM ATLAS_PANDA.JEDI_Datasets d,ATLAS_PANDA.filesTable4 f "
                sqlPD += "WHERE d.jediTaskID=:jediTaskID AND d.type IN (:type1,:type2) AND d.masterID IS NULL "
                sqlPD += "AND f.PandaID=:PandaID AND f.jeditaskID=f.jediTaskID AND f.datasetID=d.datasetID "
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                varMap[":PandaID"] = myPandaID
                varMap[":type1"] = "input"
                varMap[":type2"] = "pseudo_input"
                self.cur.execute(sqlPD + comment, varMap)
                resPD = self.cur.fetchall()
                # get PandaIDs
                idAttrMap = dict()
                sqlCP = "SELECT PandaID,attemptNr FROM ATLAS_PANDA.filesTable4 "
                sqlCP += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
                sqlWP = "SELECT 1 FROM ATLAS_PANDA.jobsDefined4 WHERE PandaID=:PandaID AND computingSite=:computingSite "
                for datasetID, fileID in resPD:
                    if fileID is None:
                        continue
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    varMap[":datasetID"] = datasetID
                    varMap[":fileID"] = fileID
                    self.cur.execute(sqlCP + comment, varMap)
                    resCP = self.cur.fetchall()
                    for pandaID, attemptNr in resCP:
                        idAttrMap[pandaID] = attemptNr
                # look for my attemptNr
                if myPandaID in idAttrMap:
                    myAttemptNr = idAttrMap[myPandaID]
                    for pandaID in idAttrMap:
                        attemptNr = idAttrMap[pandaID]
                        if attemptNr == myAttemptNr and pandaID != myPandaID and pandaID not in ids:
                            varMap = {}
                            varMap[":PandaID"] = pandaID
                            varMap[":computingSite"] = EventServiceUtils.siteIdForWaitingCoJumboJobs
                            self.cur.execute(sqlWP + comment, varMap)
                            resWP = self.cur.fetchone()
                            if resWP is not None:
                                nIDs += 1
            tmp_log.debug(f"got {nIDs} ids")
            return nIDs
        except Exception:
            # error
            self.dump_error_message(tmp_log)
            return 0

    # check event availability
    def checkEventsAvailability(self, pandaID, jobsetID, jediTaskID):
        comment = " /* DBProxy.checkEventsAvailability */"
        tmp_log = self.create_tagged_logger(comment, f"PandaID={pandaID} jobsetID={jobsetID} jediTaskID={jediTaskID}")
        tmp_log.debug("start")
        try:
            sqlJ = f"SELECT eventService FROM {panda_config.schemaJEDI}.jobsActive4 WHERE PandaID=:PandaID "
            # start transaction
            self.conn.begin()
            # get job to check if a jumbo job
            isJumbo = False
            varMap = {}
            varMap[":PandaID"] = pandaID
            self.cur.execute(sqlJ + comment, varMap)
            res = self.cur.fetchone()
            if res is not None:
                (eventService,) = res
                if eventService == EventServiceUtils.jumboJobFlagNumber:
                    isJumbo = True
            # get number of event ranges
            sqlE = "SELECT COUNT(*) "
            sqlE += f"FROM {panda_config.schemaJEDI}.JEDI_Events "
            sqlE += "WHERE jediTaskID=:jediTaskID AND status=:eventStatus AND attemptNr>:minAttemptNr "
            varMap = {}
            varMap[":eventStatus"] = EventServiceUtils.ST_ready
            varMap[":minAttemptNr"] = 0
            varMap[":jediTaskID"] = jediTaskID
            if not isJumbo:
                varMap[":jobsetID"] = jobsetID
                sqlE += "AND PandaID=:jobsetID "
            self.cur.execute(sqlE + comment, varMap)
            res = self.cur.fetchone()
            if res is not None:
                (nEvents,) = res
            else:
                nEvents = 0
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmp_log.debug(f"has {nEvents} event ranges")
            return nEvents
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return None

    # enable job cloning
    def enable_job_cloning(self, jedi_task_id: int, mode: str = None, multiplicity: int = None, num_sites: int = None) -> tuple[bool, str]:
        """
        Enable job cloning for a task

        :param jedi_task_id: jediTaskID
        :param mode: mode of cloning, runonce or storeonce
        :param multiplicity: number of jobs to be created for each target
        :param num_sites: number of sites to be used for each target
        :return: (True, None) if success otherwise (False, error message)
        """
        comment = " /* DBProxy.enable_job_cloning */"
        tmp_log = self.create_tagged_logger(comment, f"jediTaskID={jedi_task_id}")
        tmp_log.debug("start")
        try:
            ret_value = (True, None)
            # start transaction
            self.conn.begin()
            # get current split rule
            sql_check = f"SELECT splitRule FROM {panda_config.schemaJEDI}.JEDI_Tasks WHERE jediTaskID=:jediTaskID "
            var_map = {":jediTaskID": jedi_task_id}
            self.cur.execute(sql_check + comment, var_map)
            res = self.cur.fetchone()
            if not res:
                # not found
                ret_value = (False, "task not found")
            else:
                (split_rule,) = res
                # set default values
                if mode is None:
                    mode = "runonce"
                if multiplicity is None:
                    multiplicity = 2
                if num_sites is None:
                    num_sites = 2
                # ID of job cloning mode
                mode_id = EventServiceUtils.getJobCloningValue(mode)
                if mode_id == "":
                    ret_value = (False, f"invalid job cloning mode: {mode}")
                else:
                    # set mode
                    split_rule = task_split_rules.replace_rule(split_rule, "useJobCloning", mode_id)
                    # set semaphore size
                    split_rule = task_split_rules.replace_rule(split_rule, "nEventsPerWorker", 1)
                    # set job multiplicity
                    split_rule = task_split_rules.replace_rule(split_rule, "nEsConsumers", multiplicity)
                    # set number of sites
                    split_rule = task_split_rules.replace_rule(split_rule, "nSitesPerJob", num_sites)
                    # update split rule and event service flag
                    sql_update = (
                        f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks SET splitRule=:splitRule,eventService=:eventService WHERE jediTaskID=:jediTaskID "
                    )
                    var_map = {":jediTaskID": jedi_task_id, ":splitRule": split_rule, ":eventService": EventServiceUtils.TASK_JOB_CLONING}
                    self.cur.execute(sql_update + comment, var_map)
                    if not self.cur.rowcount:
                        ret_value = (False, "failed to update task")
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmp_log.debug("done")
            return ret_value
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return False, "failed to enable job cloning"

    # disable job cloning
    def disable_job_cloning(self, jedi_task_id: int) -> tuple[bool, str]:
        """
        Disable job cloning for a task

        :param jedi_task_id: jediTaskID
        :return: (True, None) if success otherwise (False, error message)
        """
        comment = " /* DBProxy.disable_job_cloning */"
        tmp_log = self.create_tagged_logger(comment, f"jediTaskID={jedi_task_id}")
        tmp_log.debug("start")
        try:
            ret_value = (True, None)
            # start transaction
            self.conn.begin()
            # get current split rule
            sql_check = f"SELECT splitRule FROM {panda_config.schemaJEDI}.JEDI_Tasks WHERE jediTaskID=:jediTaskID "
            var_map = {":jediTaskID": jedi_task_id}
            self.cur.execute(sql_check + comment, var_map)
            res = self.cur.fetchone()
            if not res:
                # not found
                ret_value = (False, "task not found")
            else:
                (split_rule,) = res
                # remove job cloning related rules
                split_rule = task_split_rules.remove_rule_with_name(split_rule, "useJobCloning")
                split_rule = task_split_rules.remove_rule_with_name(split_rule, "nEventsPerWorker")
                split_rule = task_split_rules.remove_rule_with_name(split_rule, "nEsConsumers")
                split_rule = task_split_rules.remove_rule_with_name(split_rule, "nSitesPerJob")
                # update split rule and event service flag
                sql_update = f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks SET splitRule=:splitRule,eventService=:eventService WHERE jediTaskID=:jediTaskID "
                var_map = {":jediTaskID": jedi_task_id, ":splitRule": split_rule, ":eventService": EventServiceUtils.TASK_NORMAL}
                self.cur.execute(sql_update + comment, var_map)
                if not self.cur.rowcount:
                    ret_value = (False, "failed to update task")
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmp_log.debug("done")
            return ret_value
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return False, "failed to disable job cloning"

    # check if task is active
    def checkTaskStatusJEDI(self, jediTaskID, cur):
        comment = " /* DBProxy.checkTaskStatusJEDI */"
        tmp_log = self.create_tagged_logger(comment, f" < jediTaskID={jediTaskID} >")
        retVal = False
        curStat = None
        if jediTaskID not in ["NULL", None]:
            sql = "SELECT status FROM ATLAS_PANDA.JEDI_Tasks WHERE jediTaskID=:jediTaskID "
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            cur.execute(sql + comment, varMap)
            res = cur.fetchone()
            if res is not None:
                curStat = res[0]
                if curStat not in [
                    "done",
                    "finished",
                    "failed",
                    "broken",
                    "aborted",
                    "prepared",
                ]:
                    retVal = True
        tmp_log.debug(f"in {curStat} with {retVal}")
        return retVal

    # update tasks's input status in JEDI
    def updateInputStatusJedi(self, jediTaskID, pandaID, newStatus, checkOthers=False, no_late_bulk_exec=True, extracted_sqls=None):
        comment = " /* DBProxy.updateInputStatusJedi */"
        tmp_log = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID} PandaID={pandaID}")
        tmp_log.debug(f"start newStatus={newStatus}")
        statusMap = {
            "ready": ["queued", "starting", "running", "merging", "transferring"],
            "queued": ["ready", "starting", "running"],
            "starting": ["queued", "running", "ready"],
            "running": ["starting", "queued", "ready"],
            "merging": ["queued", "running"],
            "transferring": ["running", "merging"],
            "finished": ["running", "transferring", "merging"],
            "failed": ["running", "transferring", "merging", "queued", "starting"],
        }
        try:
            # change canceled/closed to failed
            if newStatus in ["cancelled", "closed"]:
                newStatus = "failed"
            # check status
            if newStatus not in statusMap:
                tmp_log.error(f"unknown status : {newStatus}")
                return False
            # get datasetID and fileID
            sqlF = f"SELECT f.datasetID,f.fileID,f.attemptNr FROM {panda_config.schemaJEDI}.JEDI_Datasets d,{panda_config.schemaPANDA}.filesTable4 f "
            sqlF += "WHERE d.jediTaskID=:jediTaskID AND d.type IN (:type1,:type2) AND d.masterID IS NULL "
            sqlF += "AND f.datasetID=d.datasetID AND f.PandaID=:PandaID "
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            varMap[":PandaID"] = pandaID
            varMap[":type1"] = "input"
            varMap[":type2"] = "pseudo_input"
            self.cur.execute(sqlF + comment, varMap)
            resF = self.cur.fetchall()
            # get status and attemptNr in JEDI
            sqlJ = "SELECT status,proc_status,attemptNr,maxAttempt,failedAttempt,maxFailure "
            sqlJ += f"FROM {panda_config.schemaJEDI}.JEDI_Dataset_Contents "
            sqlJ += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
            sqlU = f"UPDATE {panda_config.schemaJEDI}.JEDI_Dataset_Contents "
            sqlU += "SET proc_status=:newStatus "
            sqlU += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
            sqlU += "AND attemptNr=:attemptNr "
            sqlC = "SELECT j.PandaID FROM {0}.jobsActive4 j,{0}.filesTable4 f ".format(panda_config.schemaPANDA)
            sqlC += "WHERE j.PandaID=f.PandaID AND j.jobStatus=:jobStatus "
            sqlC += "AND f.jediTaskID=:jediTaskID AND f.datasetID=:datasetID AND f.fileID=:fileID "
            sqlC += "AND f.attemptNr=:attemptNr "
            for datasetID, fileID, f_attemptNr in resF:
                # increment attemptNr for final status
                if newStatus in ["finished", "failed"]:
                    f_attemptNr += 1
                # check others
                if checkOthers and newStatus == "queued":
                    otherStatus = "running"
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    varMap[":datasetID"] = datasetID
                    varMap[":fileID"] = fileID
                    varMap[":attemptNr"] = f_attemptNr
                    varMap[":jobStatus"] = otherStatus
                    self.cur.execute(sqlC + comment, varMap)
                    resC = self.cur.fetchall()
                    if len(resC) > 0:
                        tmp_log.debug(f"skip to update fileID={fileID} to {newStatus} since others like PandaID={resC[0][0]} is {otherStatus}")
                        continue
                # get data in JEDI
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                varMap[":datasetID"] = datasetID
                varMap[":fileID"] = fileID
                self.cur.execute(sqlJ + comment, varMap)
                (
                    fileStatus,
                    oldStatus,
                    j_attemptNr,
                    maxAttempt,
                    failedAttempt,
                    maxFailure,
                ) = self.cur.fetchone()
                # check attemptNr
                if j_attemptNr != f_attemptNr:
                    tmp_log.error(f"inconsistent attempt number : JEDI:{j_attemptNr} Panda:{f_attemptNr} for fileID={fileID} newStatus={newStatus}")
                    continue
                # check status
                if oldStatus is not None and oldStatus not in statusMap[newStatus] and oldStatus != newStatus:
                    tmp_log.error(f"{oldStatus} -> {newStatus} is forbidden for fileID={fileID}")
                    continue
                # conversion for failed
                tmpNewStatus = newStatus
                if newStatus == "failed" and j_attemptNr < maxAttempt and (maxFailure is None or failedAttempt < maxFailure):
                    tmpNewStatus = "ready"
                # no change
                if tmpNewStatus == oldStatus:
                    tmp_log.debug(f"skip to update fileID={fileID} due to no status change already in {tmpNewStatus}")
                    continue
                # ready
                if tmpNewStatus in ["ready", "failed"] and fileStatus != "ready":
                    tmp_log.debug(f"skip to update fileID={fileID} to {tmpNewStatus} since the file status is {fileStatus}")
                    continue
                # update
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                varMap[":datasetID"] = datasetID
                varMap[":fileID"] = fileID
                varMap[":attemptNr"] = f_attemptNr
                varMap[":newStatus"] = tmpNewStatus
                if no_late_bulk_exec:
                    self.cur.execute(sqlU + comment, varMap)
                    nRow = self.cur.rowcount
                    tmp_log.debug(f"{oldStatus} -> {tmpNewStatus} for fileID={fileID} with {nRow}")
                else:
                    extracted_sqls.setdefault("jedi_input", {"sql": sqlU + comment, "vars": []})
                    extracted_sqls["jedi_input"]["vars"].append(varMap)
            # return
            tmp_log.debug("done")
            return True
        except Exception:
            # error
            self.dump_error_message(tmp_log)
            return False

    # change split rule for task
    def changeTaskSplitRulePanda(self, jediTaskID, attrName, attrValue, useCommit=True, sendLog=True):
        comment = " /* DBProxy.changeTaskSplitRulePanda */"
        tmp_log = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID}")
        tmp_log.debug(f"changing {attrName}={attrValue}")
        try:
            # sql to get split rule
            sqlS = f"SELECT splitRule FROM {panda_config.schemaJEDI}.JEDI_Tasks "
            sqlS += "WHERE jediTaskID=:jediTaskID "
            # sql to update JEDI task table
            sqlT = f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks SET "
            sqlT += "splitRule=:splitRule WHERE jediTaskID=:jediTaskID "
            # start transaction
            if useCommit:
                self.conn.begin()
            # select
            self.cur.arraysize = 10
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            # get split rule
            self.cur.execute(sqlS + comment, varMap)
            resS = self.cur.fetchone()
            if resS is None:
                retVal = 0
            else:
                splitRule = resS[0]
                if splitRule is None:
                    items = []
                else:
                    items = splitRule.split(",")
                # remove old
                newItems = []
                for tmpItem in items:
                    if tmpItem.startswith(f"{attrName}="):
                        continue
                    newItems.append(tmpItem)
                # add new
                if attrValue not in [None, "", "None"]:
                    newItems.append(f"{attrName}={attrValue}")
                # update
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                varMap[":splitRule"] = ",".join(newItems)
                self.cur.execute(sqlT + comment, varMap)
                retVal = 1
            # commit
            if useCommit:
                if not self._commit():
                    raise RuntimeError("Commit error")
            tmp_log.debug(f"done with {retVal}")
            if sendLog:
                tmp_log.sendMsg(
                    f"set {attrName}={attrValue} to splitRule",
                    "jedi",
                    "pandasrv",
                )
            return retVal
        except Exception:
            # roll back
            if useCommit:
                self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return None

    # enable jumbo jobs
    def enableJumboJobs(self, jediTaskID, nJumboJobs, nJumboPerSite, useCommit=True, sendLog=True):
        comment = " /* DBProxy.enableJumboJobs */"
        tmp_log = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID}")
        tmp_log.debug("start")
        try:
            # sql to set flag
            sqlJumboF = f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks "
            sqlJumboF += "SET useJumbo=:newJumbo WHERE jediTaskID=:jediTaskID "
            # start transaction
            if useCommit:
                self.conn.begin()
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            if nJumboJobs == 0:
                varMap[":newJumbo"] = "D"
            else:
                varMap[":newJumbo"] = "W"
            self.cur.execute(sqlJumboF, varMap)
            nRow = self.cur.rowcount
            if nRow > 0:
                self.changeTaskSplitRulePanda(jediTaskID, "NJ", nJumboJobs, useCommit=False, sendLog=sendLog)
                self.changeTaskSplitRulePanda(jediTaskID, "MJ", nJumboPerSite, useCommit=False, sendLog=sendLog)
                retVal = (0, "done")
                tmp_log.debug(f"set nJumboJobs={nJumboJobs} nJumboPerSite={nJumboPerSite} useJumbo={varMap[':newJumbo']}")
            else:
                retVal = (2, "task not found")
                tmp_log.debug("task not found")
            # commit
            if useCommit:
                if not self._commit():
                    raise RuntimeError("Commit error")
            # return
            return retVal
        except Exception:
            # roll back
            if useCommit:
                self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return (1, "database error in the panda server")

    # enable event service
    def enableEventService(self, jediTaskID):
        comment = " /* DBProxy.enableEventService */"
        tmp_log = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID}")
        tmp_log.debug("start")
        try:
            # get default values
            nEsConsumers = self.getConfigValue("taskrefiner", "AES_NESCONSUMERS", "jedi", "atlas")
            if nEsConsumers is None:
                nEsConsumers = 1
            nSitesPerJob = self.getConfigValue("taskrefiner", "AES_NSITESPERJOB", "jedi", "atlas")
            # get task params
            sqlTP = f"SELECT taskParams FROM {panda_config.schemaJEDI}.JEDI_TaskParams WHERE jediTaskID=:jediTaskID "
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            tmpV, taskParams = self.getClobObj(sqlTP, varMap)
            if taskParams is None:
                errStr = "task parameter is not found"
                tmp_log.error(errStr)
                return (3, errStr)
            try:
                taskParamMap = json.loads(taskParams[0][0])
            except Exception:
                errStr = "cannot load task parameter"
                tmp_log.error(errStr)
                return (4, errStr)
            # extract parameters
            transPath = "UnDefined"
            jobParameters = "UnDefined"
            if "esmergeSpec" in taskParamMap:
                if "transPath" in taskParamMap["esmergeSpec"]:
                    transPath = taskParamMap["esmergeSpec"]["transPath"]
                if "jobParameters" in taskParamMap["esmergeSpec"]:
                    jobParameters = taskParamMap["esmergeSpec"]["jobParameters"]
            esJobParameters = "<PANDA_ESMERGE_TRF>" + transPath + "</PANDA_ESMERGE_TRF>" + "<PANDA_ESMERGE_JOBP>" + jobParameters + "</PANDA_ESMERGE_JOBP>"
            esJobParameters = str(esJobParameters)
            # get job params template
            sqlJT = f"SELECT jobParamsTemplate FROM {panda_config.schemaJEDI}.JEDI_JobParams_Template WHERE jediTaskID=:jediTaskID "
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            tmpV, jobParamsTemplate = self.getClobObj(sqlJT, varMap)
            if jobParamsTemplate is None:
                errStr = "job params template is not found"
                tmp_log.error(errStr)
                return (5, errStr)
            jobParamsTemplate = jobParamsTemplate[0][0]
            # sql to set flag
            sqlES = f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks "
            sqlES += "SET eventService=:newEventService,coreCount=0,"
            sqlES += f"workqueue_id=(SELECT queue_id FROM {panda_config.schemaJEDI}.JEDI_Work_Queue WHERE queue_name=:queueName) "
            sqlES += "WHERE jediTaskID=:jediTaskID AND lockedBy IS NULL "
            # start transaction
            self.conn.begin()
            # update ES flag
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            varMap[":newEventService"] = 1
            varMap[":queueName"] = "eventservice"
            self.cur.execute(sqlES, varMap)
            nRow = self.cur.rowcount
            if nRow > 0:
                # update splitrule
                self.changeTaskSplitRulePanda(jediTaskID, "EC", nEsConsumers, useCommit=False, sendLog=True)
                if nSitesPerJob is not None:
                    self.changeTaskSplitRulePanda(jediTaskID, "NS", nSitesPerJob, useCommit=False, sendLog=True)
                self.changeTaskSplitRulePanda(jediTaskID, "ES", 1, useCommit=False, sendLog=True)
                self.changeTaskSplitRulePanda(jediTaskID, "RE", 1, useCommit=False, sendLog=True)
                self.changeTaskSplitRulePanda(jediTaskID, "ME", 1, useCommit=False, sendLog=True)
                self.changeTaskSplitRulePanda(jediTaskID, "XA", 1, useCommit=False, sendLog=True)
                self.changeTaskSplitRulePanda(jediTaskID, "XJ", 0, useCommit=False, sendLog=True)
                self.changeTaskSplitRulePanda(jediTaskID, "ND", 1, useCommit=False, sendLog=True)
                self.changeTaskSplitRulePanda(jediTaskID, "XF", 1, useCommit=False, sendLog=True)
                self.changeTaskSplitRulePanda(jediTaskID, "SC", None, useCommit=False, sendLog=True)
                if esJobParameters not in jobParamsTemplate:
                    # update job params template
                    sqlUJ = f"UPDATE {panda_config.schemaJEDI}.JEDI_JobParams_Template SET jobParamsTemplate=:new WHERE jediTaskID=:jediTaskID "
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    varMap[":new"] = jobParamsTemplate + esJobParameters
                    self.cur.execute(sqlUJ, varMap)
                retVal = (0, "done")
                tmp_log.debug("done")
            else:
                retVal = (2, "task not found or locked")
                tmp_log.debug("failed to update the flag")
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            return retVal
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return (1, "database error in the panda server")

    # check if task is applicable for jumbo jobs
    def isApplicableTaskForJumbo(self, jediTaskID):
        comment = " /* DBProxy.isApplicableTaskForJumbo */"
        tmp_log = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID}")
        tmp_log.debug("start")
        retVal = True
        try:
            # sql to check event
            sqlF = "SELECT SUM(nFiles),SUM(nFilesFinished),SUM(nFilesFailed) "
            sqlF += f"FROM {panda_config.schemaJEDI}.JEDI_Datasets "
            sqlF += "WHERE jediTaskID=:jediTaskID AND type IN (:type1,:type2) "
            sqlF += "AND masterID IS NULL "
            # begin transaction
            self.conn.begin()
            # check task status
            if not self.checkTaskStatusJEDI(jediTaskID, self.cur):
                # task is in a final status
                retVal = False
            else:
                # threshold in % to stop jumbo jobs
                threshold = 100
                # check percentage
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                varMap[":type1"] = "input"
                varMap[":type2"] = "pseudo_input"
                self.cur.execute(sqlF + comment, varMap)
                resF = self.cur.fetchone()
                nFiles, nFilesFinished, nFilesFailed = resF
                if (nFilesFinished + nFilesFailed) * 100 >= nFiles * threshold:
                    retVal = False
                    tmp_log.debug(f"nFilesFinished({nFilesFinished}) + nFilesFailed({nFilesFailed}) >= nFiles({nFiles})*{threshold}%")
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmp_log.debug(f"done with {retVal}")
            return retVal
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return retVal

    # increase memory limit
    def increaseRamLimitJEDI(self, jediTaskID, jobRamCount, noLimits=False):
        comment = " /* DBProxy.increaseRamLimitJEDI */"
        tmp_log = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID}")
        tmp_log.debug(f"start")
        try:
            # RAM limit
            limitList = [1000, 2000, 3000, 4000, 6000, 8000]
            # begin transaction
            self.conn.begin()
            # get current limit
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            sqlUE = f"SELECT ramCount FROM {panda_config.schemaJEDI}.JEDI_Tasks "
            sqlUE += "WHERE jediTaskID=:jediTaskID "
            self.cur.execute(sqlUE + comment, varMap)
            (taskRamCount,) = self.cur.fetchone()
            tmp_log.debug(f"RAM limit task={taskRamCount} job={jobRamCount}")

            increased = False

            # skip if already increased or largest limit
            if taskRamCount > jobRamCount:
                dbgStr = f"no change since task RAM limit ({taskRamCount}) is larger than job limit ({jobRamCount})"
                tmp_log.debug(f"{dbgStr}")
            elif taskRamCount >= limitList[-1] and not noLimits:
                dbgStr = "no change "
                dbgStr += f"since task RAM limit ({taskRamCount}) is larger than or equal to the highest limit ({limitList[-1]})"
                tmp_log.debug(f"{dbgStr}")
            else:
                increased = True
                limit = max(taskRamCount, jobRamCount)
                for nextLimit in limitList:
                    if limit < nextLimit:
                        break
                # if there are no limits
                if limit > nextLimit and noLimits:
                    nextLimit = limit

                # update RAM limit
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                varMap[":ramCount"] = nextLimit
                sqlRL = f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks "
                sqlRL += "SET ramCount=:ramCount "
                sqlRL += "WHERE jediTaskID=:jediTaskID "
                self.cur.execute(sqlRL + comment, varMap)
                tmp_log.debug(f"increased RAM limit to {nextLimit} from {taskRamCount}")
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")

            # reset the tasks resource type, since it could have jumped to HIMEM
            if increased:
                try:
                    get_entity_module(self).reset_resource_type_task(jediTaskID)
                except Exception:
                    tmp_log.error(f"reset_resource_type excepted with {traceback.format_exc()}")

            tmp_log.debug(f"done")
            return True
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return False

    # increase memory limit
    def increaseRamLimitJobJEDI(self, job, job_ram_count, jedi_task_id):
        """Note that this function only increases the min RAM count for the job,
        not for the entire task (for the latter use increaseRamLimitJEDI)
        """
        comment = " /* DBProxy.increaseRamLimitJobJEDI */"
        tmp_log = self.create_tagged_logger(comment, f"PandaID={job.PandaID}")
        tmp_log.debug("start")

        # RAM limit
        limit_list = [1000, 2000, 3000, 4000, 6000, 8000]
        # Files defined as input types
        input_types = ("input", "pseudo_input", "pp_input", "trn_log", "trn_output")

        try:
            # if there is no task associated to the job, don't take any action
            if job.jediTaskID in [None, 0, "NULL"]:
                tmp_log.debug(f"Done. No task({job.jediTaskID}) associated to job({job.PandaID}). Skipping")
                return True

            # get current task ram info
            var_map = {":jediTaskID": jedi_task_id}
            sql_get_ram_task = f"SELECT ramCount, ramUnit, baseRamCount FROM {panda_config.schemaJEDI}.JEDI_Tasks "
            sql_get_ram_task += "WHERE jediTaskID=:jediTaskID "
            self.cur.execute(sql_get_ram_task + comment, var_map)
            task_ram_count, task_ram_unit, task_base_ram_count = self.cur.fetchone()

            if task_base_ram_count in [0, None, "NULL"]:
                task_base_ram_count = 0

            core_count = job.coreCount

            if core_count in [0, None, "NULL"]:
                core_count = 1

            # roll back the memory compensation of the job
            job_ram_count = JobUtils.decompensate_ram_count(job_ram_count)

            tmp_log.debug(
                f"RAM limit task={task_ram_count}{task_ram_unit} cores={core_count} baseRamCount={task_base_ram_count} job={job_ram_count}{job.minRamUnit}"
            )

            # If more than x% of the task's jobs needed a memory increase, increase the task's memory instead
            var_map = {":jediTaskID": jedi_task_id}

            input_type_var_names_str, input_type_var_map = get_sql_IN_bind_variables(input_types, prefix=":type")
            var_map.update(input_type_var_map)

            sql_get_memory_stats = (
                f"SELECT ramCount, count(*) "
                f"FROM {panda_config.schemaJEDI}.JEDI_Datasets tabD, {panda_config.schemaJEDI}.JEDI_Dataset_Contents tabC "
                f"WHERE tabD.jediTaskID=tabC.jediTaskID AND tabD.datasetID=tabC.datasetID AND tabD.jediTaskID=:jediTaskID "
                f"AND tabD.type IN ({input_type_var_names_str}) AND tabD.masterID IS NULL GROUP BY ramCount"
            )

            self.cur.execute(sql_get_memory_stats + comment, var_map)
            memory_stats = self.cur.fetchall()
            total = sum([entry[1] for entry in memory_stats])
            above_task = sum(tuple[1] for tuple in filter(lambda entry: entry[0] > task_ram_count, memory_stats))
            max_task = max([entry[0] for entry in memory_stats])
            tmp_log.debug(f"Current task statistics: #increased_files: {above_task}; #total_files: {total}")

            # normalize the job ram-count by base ram count and number of cores
            try:
                normalized_job_ram_count = (job_ram_count - task_base_ram_count) * 1.0
                if task_ram_unit in [
                    "MBPerCore",
                    "MBPerCoreFixed",
                ] and job.minRamUnit in ("MB", None, "NULL"):
                    normalized_job_ram_count = normalized_job_ram_count / core_count
            except TypeError:
                normalized_job_ram_count = 0

            # increase task limit in case >30% of the jobs were increased and the task is not fixed
            if task_ram_unit != "MBPerCoreFixed" and (1.0 * above_task) / total > 0.3:
                minimum_ram = 0
                if normalized_job_ram_count and normalized_job_ram_count > minimum_ram:
                    minimum_ram = normalized_job_ram_count
                if max_task > minimum_ram:
                    minimum_ram = max_task - 1  # otherwise we go over the max_task step

                if minimum_ram:
                    tmp_log.debug(f"calling increaseRamLimitJEDI with minimum_ram {minimum_ram}")
                    return self.increaseRamLimitJEDI(jedi_task_id, minimum_ram)

            # skip if already at largest limit
            if normalized_job_ram_count >= limit_list[-1]:
                tmp_log.debug(
                    f"Done. No change since job RAM limit ({normalized_job_ram_count}) " f"is larger than or equal to the highest limit ({limit_list[-1]})"
                )
                return True

            # look for the next limit in the list above the current RAM count
            for next_limit in limit_list:
                if normalized_job_ram_count < next_limit:
                    break

            # task ram-count could already have been increased higher than the next limit. in this case don't do anything
            if task_ram_count > next_limit:
                tmp_log.debug(f"Done. Task ram count ({task_ram_count}) has been increased and is larger than the next limit ({next_limit})")
                return True

            # update RAM limit
            var_map = {":jediTaskID": job.jediTaskID, ":ramCount": next_limit}
            input_files = filter(lambda panda_file: panda_file.type in input_types, job.Files)
            input_tuples = [(input_file.datasetID, input_file.fileID, input_file.attemptNr) for input_file in input_files]

            for entry in input_tuples:
                dataset_id, file_id, attempt_nr = entry
                var_map[":datasetID"] = dataset_id
                var_map[":fileID"] = file_id

                sql_get_update_ram_job = (
                    f"UPDATE {panda_config.schemaJEDI}.JEDI_Dataset_Contents SET ramCount=:ramCount "
                    f"WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID AND ramCount<:ramCount "
                )

                self.cur.execute(sql_get_update_ram_job + comment, var_map)
                tmp_log.debug(
                    f"increased RAM limit to {next_limit} from {normalized_job_ram_count} for PandaID {job.PandaID} "
                    f"fileID {file_id} attemptNr {attempt_nr} jediTaskID {job.jediTaskID} datasetID {dataset_id}"
                )

            if not self._commit():
                raise RuntimeError("Commit error")

            tmp_log.debug("Done")
            return True
        except Exception:
            self._rollback()
            self.dump_error_message(tmp_log)
            return False

    # increase memory limit xtimes
    def increaseRamLimitJobJEDI_xtimes(self, job, jobRamCount, jediTaskID, attemptNr):
        """Note that this function only increases the min RAM count for the job,
        not for the entire task (for the latter use increaseRamLimitJEDI)
        """
        comment = " /* DBProxy.increaseRamLimitJobJEDI_xtimes */"
        tmp_log = self.create_tagged_logger(comment, f"PandaID={job.PandaID}")
        tmp_log.debug(f"start")

        # Files defined as input types
        input_types = ("input", "pseudo_input", "pp_input", "trn_log", "trn_output")

        try:
            # If no task associated to job don't take any action
            if job.jediTaskID in [None, 0, "NULL"]:
                tmp_log.debug(f"No task({job.jediTaskID}) associated to job({job.PandaID}). Skipping increase of RAM limit xtimes")
            else:
                # get current task Ram info
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                sqlUE = f"SELECT ramCount, ramUnit, baseRamCount, splitRule FROM {panda_config.schemaJEDI}.JEDI_Tasks "
                sqlUE += "WHERE jediTaskID=:jediTaskID "
                self.cur.execute(sqlUE + comment, varMap)
                taskRamCount, taskRamUnit, taskBaseRamCount, splitRule = self.cur.fetchone()

                if taskBaseRamCount in [0, None, "NULL"]:
                    taskBaseRamCount = 0

                coreCount = job.coreCount

                if coreCount in [0, None, "NULL"]:
                    coreCount = 1

                if splitRule is None:
                    items = []
                else:
                    items = splitRule.split(",")

                # set default value
                retryRamOffset = 0
                retryRamStep = 1.0
                # set values from task
                for tmpItem in items:
                    if tmpItem.startswith("RX="):
                        retryRamOffset = int(tmpItem.replace("RX=", ""))
                    if tmpItem.startswith("RY="):
                        retryRamStep = float(tmpItem.replace("RY=", ""))

                tmp_log.debug(
                    f"RAM limit task={taskRamCount}{taskRamUnit} cores={coreCount} baseRamCount={taskBaseRamCount} job={jobRamCount}{job.minRamUnit} jobPSS={job.maxPSS}kB retryRamOffset={retryRamOffset} retryRamStep={retryRamStep} attemptNr={attemptNr}"
                )

                # normalize the job ram-count by base ram count and number of cores
                multiplier = retryRamStep * 1.0 / taskRamCount
                minimumRam = retryRamOffset + taskRamCount * (multiplier**attemptNr)

                if taskRamUnit != "MBPerCoreFixed":
                    # If more than x% of the task's jobs needed a memory increase, increase the task's memory instead
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID

                    input_type_var_names_str, input_type_var_map = get_sql_IN_bind_variables(input_types, prefix=":type")
                    varMap.update(input_type_var_map)

                    sqlMS = """
                             SELECT ramCount, count(*)
                             FROM {0}.JEDI_Datasets tabD,{0}.JEDI_Dataset_Contents tabC
                             WHERE tabD.jediTaskID=tabC.jediTaskID
                             AND tabD.datasetID=tabC.datasetID
                             AND tabD.jediTaskID=:jediTaskID
                             AND tabD.type IN ({1})
                             AND tabD.masterID IS NULL
                             GROUP BY ramCount
                             """.format(
                        panda_config.schemaJEDI, input_type_var_names_str
                    )

                    self.cur.execute(sqlMS + comment, varMap)
                    memory_stats = self.cur.fetchall()
                    total = sum([entry[1] for entry in memory_stats])
                    above_task = sum(tuple[1] for tuple in filter(lambda entry: entry[0] > taskRamCount, memory_stats))
                    # max_task = max([entry[0] for entry in memory_stats])
                    tmp_log.debug(f"#increased_files: {above_task}; #total_files: {total}")

                    # increase task limit in case >30% of the jobs were increased and the task is not fixed
                    if taskRamUnit != "MBPerCoreFixed" and (1.0 * above_task) / total > 0.3:
                        if minimumRam and minimumRam > taskRamCount:
                            tmp_log.debug(f"calling increaseRamLimitJEDI with minimumRam {minimumRam}")
                            return self.increaseRamLimitJEDI(jediTaskID, minimumRam, noLimits=True)

                # Ops could have increased task RamCount through direct DB access. In this case don't do anything
                if taskRamCount > minimumRam:
                    tmp_log.debug(f"task ramcount has already been increased and is higher than minimumRam. Skipping")
                    return True

                # skip if already at largest limit
                if jobRamCount >= minimumRam:
                    tmp_log.debug(f"job ramcount is larger than minimumRam. Skipping")
                    return True
                else:
                    nextLimit = minimumRam

                    # update RAM limit
                    varMap = {}
                    varMap[":jediTaskID"] = job.jediTaskID
                    varMap[":ramCount"] = nextLimit
                    input_files = filter(lambda pandafile: pandafile.type in input_types, job.Files)
                    input_tuples = [(input_file.datasetID, input_file.fileID, input_file.attemptNr) for input_file in input_files]

                    for entry in input_tuples:
                        datasetID, fileId, attemptNr = entry
                        varMap[":datasetID"] = datasetID
                        varMap[":fileID"] = fileId

                        sqlRL = f"UPDATE {panda_config.schemaJEDI}.JEDI_Dataset_Contents "
                        sqlRL += "SET ramCount=:ramCount "
                        sqlRL += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
                        sqlRL += "AND ramCount<:ramCount "

                        self.cur.execute(sqlRL + comment, varMap)
                        tmp_log.debug(
                            f"increased RAM limit to {nextLimit} from {jobRamCount} for PandaID {job.PandaID} fileID {fileId} attemptNr {attemptNr} jediTaskID {job.jediTaskID} datasetID {datasetID}"
                        )
                # commit
                if not self._commit():
                    raise RuntimeError("Commit error")

            tmp_log.debug(f"done")
            return True
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return False

    # reduce input per job
    def reduce_input_per_job(self, panda_id, jedi_task_id, attempt_nr, excluded_rules, steps, dry_mode):
        comment = " /* DBProxy.reduce_input_per_job */"
        tmp_log = self.create_tagged_logger(comment, f"PandaID={panda_id} jediTaskID={jedi_task_id} attemptNr={attempt_nr}")
        tmp_log.debug("start")
        try:
            # rules to skip action when they are set
            if not excluded_rules:
                excluded_rules = ["nEventsPerJob", "nFilesPerJob"]
            else:
                excluded_rules = excluded_rules.split(",")

            # thresholds with attempt numbers to trigger actions
            if not steps:
                threshold_low = 2
                threshold_middle = 4
                threshold_high = 7
            else:
                threshold_low, threshold_middle, threshold_high = [int(s) for s in steps.split(",")]

            # if no task associated to job don't take any action
            if jedi_task_id in [None, 0, "NULL"]:
                msg_str = "skipping since no task associated to job"
                tmp_log.debug(msg_str)
                return False, msg_str

            # check attempt number
            if attempt_nr < threshold_low:
                msg_str = f"skipping since not enough attempts ({attempt_nr} < {threshold_low}) have been made"
                tmp_log.debug(msg_str)
                return False, msg_str

            # get current split rules
            var_map = {":jediTaskID": jedi_task_id}
            sql_gr = f"SELECT splitRule FROM {panda_config.schemaJEDI}.JEDI_Tasks "
            sql_gr += "WHERE jediTaskID=:jediTaskID "
            self.cur.execute(sql_gr + comment, var_map)
            (split_rule,) = self.cur.fetchone()

            # extract split rule values
            rule_values = task_split_rules.extract_rule_values(
                split_rule, ["nEventsPerJob", "nFilesPerJob", "nGBPerJob", "nMaxFilesPerJob", "retryModuleRules"]
            )

            # no action if num events or files per job is specified
            for rule_name in excluded_rules:
                if rule_values[rule_name]:
                    msg_str = f"skipping since task uses {rule_name}"
                    tmp_log.debug(msg_str)
                    return False, msg_str

            # current max number of files or gigabytes per job
            current_max_files_per_job = rule_values["nMaxFilesPerJob"]
            if current_max_files_per_job:
                current_max_files_per_job = int(current_max_files_per_job)
            current_gigabytes_per_job = rule_values["nGBPerJob"]
            if current_gigabytes_per_job:
                current_gigabytes_per_job = int(current_gigabytes_per_job)

            # initial max number of files or gigabytes per job for retry module
            rules_for_retry_module = rule_values["retryModuleRules"]
            rule_values_for_retry_module = task_split_rules.extract_rule_values(rules_for_retry_module, ["nGBPerJob", "nMaxFilesPerJob"], is_sub_rule=True)
            init_gigabytes_per_job = rule_values_for_retry_module["nGBPerJob"]
            init_max_files_per_job = rule_values_for_retry_module["nMaxFilesPerJob"]

            # set initial values for the first action
            set_init_rules = False
            if not init_gigabytes_per_job:
                set_init_rules = True
                if current_gigabytes_per_job:
                    init_gigabytes_per_job = current_gigabytes_per_job
                else:
                    # use current job size as initial gigabytes per job for retry module
                    var_map = {":PandaID": panda_id}
                    sql_fz = f"SELECT SUM(fsize) FROM {panda_config.schemaPANDA}.filesTable4 "
                    sql_fz += "WHERE PandaID=:PandaID "
                    self.cur.execute(sql_fz + comment, var_map)
                    (init_gigabytes_per_job,) = self.cur.fetchone()
                    init_gigabytes_per_job = math.ceil(init_gigabytes_per_job / 1024 / 1024 / 1024)
            if not init_max_files_per_job:
                set_init_rules = True
                if current_max_files_per_job:
                    init_max_files_per_job = current_max_files_per_job
                else:
                    # use current job size as initial max number of files per job for retry module
                    var_map = {":PandaID": panda_id, ":jediTaskID": jedi_task_id, ":type1": "input", ":type2": "pseudo_input"}
                    sql_fc = f"SELECT COUNT(*) FROM {panda_config.schemaPANDA}.filesTable4 tabF, {panda_config.schemaJEDI}.JEDI_Datasets tabD "
                    sql_fc += (
                        "WHERE tabD.jediTaskID=:jediTaskID AND tabD.type IN (:type1, :type2) AND tabD.masterID IS NULL "
                        "AND tabF.PandaID=:PandaID AND tabF.datasetID=tabD.datasetID "
                    )
                    self.cur.execute(sql_fc + comment, var_map)
                    (init_max_files_per_job,) = self.cur.fetchone()

            # set target based on attempt number
            if attempt_nr < threshold_middle:
                target_gigabytes_per_job = math.floor(init_gigabytes_per_job / 2)
                target_max_files_per_job = math.floor(init_max_files_per_job / 2)
            elif attempt_nr < threshold_high:
                target_gigabytes_per_job = math.floor(init_gigabytes_per_job / 4)
                target_max_files_per_job = math.floor(init_max_files_per_job / 4)
            else:
                target_gigabytes_per_job = 1
                target_max_files_per_job = 1
            target_gigabytes_per_job = max(1, target_gigabytes_per_job)
            target_max_files_per_job = max(1, target_max_files_per_job)

            # update rules when initial values were unset or new values need to be set
            if set_init_rules or current_gigabytes_per_job != target_gigabytes_per_job or current_max_files_per_job != target_max_files_per_job:
                msg_str = "update splitRule: "
                if set_init_rules:
                    msg_str += f"initial nGBPerJob={init_gigabytes_per_job} nMaxFilesPerJob={init_max_files_per_job}. "
                    rules_for_retry_module = task_split_rules.replace_rule(rules_for_retry_module, "nGBPerJob", init_gigabytes_per_job, is_sub_rule=True)
                    rules_for_retry_module = task_split_rules.replace_rule(rules_for_retry_module, "nMaxFilesPerJob", init_max_files_per_job, is_sub_rule=True)
                    if not dry_mode:
                        self.changeTaskSplitRulePanda(
                            jedi_task_id, task_split_rules.split_rule_dict["retryModuleRules"], rules_for_retry_module, useCommit=False, sendLog=True
                        )
                if current_gigabytes_per_job != target_gigabytes_per_job:
                    msg_str += f"new nGBPerJob {current_gigabytes_per_job} -> {target_gigabytes_per_job}. "
                    if not dry_mode:
                        self.changeTaskSplitRulePanda(
                            jedi_task_id, task_split_rules.split_rule_dict["nGBPerJob"], target_gigabytes_per_job, useCommit=False, sendLog=True
                        )
                if current_max_files_per_job != target_max_files_per_job:
                    msg_str += f"new nMaxFilesPerJob {current_max_files_per_job} -> {target_max_files_per_job}. "
                    if not dry_mode:
                        self.changeTaskSplitRulePanda(
                            jedi_task_id, task_split_rules.split_rule_dict["nMaxFilesPerJob"], target_max_files_per_job, useCommit=False, sendLog=True
                        )
                tmp_log.debug(msg_str)
                # commit
                if not dry_mode and not self._commit():
                    raise RuntimeError("Commit error")
                return True, msg_str

            msg_str = "not applicable"
            tmp_log.debug(msg_str)
            return False, msg_str
        except Exception:
            # roll back
            if not dry_mode:
                self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return None, "failed"

    def create_pseudo_files_for_dyn_num_events(self, job_spec, tmp_log):
        """
        create pseudo files for dynamic number of events
        param job_spec: JobSpec
        param tmp_log: logger
        """
        comment = " /* DBProxy.create_pseudo_files_for_dyn_num_events */"
        # make row_ID and fileSpec map
        row_id_spec_map = {}
        for fileSpec in job_spec.Files:
            row_id_spec_map[fileSpec.row_ID] = fileSpec
        # get pseudo files
        pseudo_files = []
        var_map = {":PandaID": job_spec.PandaID, ":jediTaskID": job_spec.jediTaskID, ":eventID": -1}
        sql = (
            "SELECT fileID,attemptNr,job_processID "
            f"FROM {panda_config.schemaJEDI}.JEDI_Events "
            "WHERE jediTaskID=:jediTaskID AND PandaID=:PandaID AND processed_upto_eventID=:eventID "
        )
        self.cur.execute(sql + comment, var_map)
        res = self.cur.fetchall()
        for tmpFileID, tmpAttemptNr, tmpRow_ID in res:
            tmpFileSpec = copy.copy(row_id_spec_map[tmpRow_ID])
            tmpFileSpec.fileID = tmpFileID
            tmpFileSpec.attemptNr = tmpAttemptNr - 1
            pseudo_files.append(tmpFileSpec)
        tmp_log.debug(f"{len(pseudo_files)} pseudo files")
        return pseudo_files

    # check input file status
    def checkInputFileStatusInJEDI(self, jobSpec, useCommit=True, withLock=False):
        comment = " /* DBProxy.checkInputFileStatusInJEDI */"
        tmp_log = self.create_tagged_logger(comment, f"PandaID={jobSpec.PandaID}")
        tmp_log.debug("start")
        try:
            # only JEDI
            if jobSpec.lockedby != "jedi":
                return True
            # sql to check file status
            sqlFileStat = "SELECT PandaID,status,attemptNr,keepTrack,is_waiting FROM ATLAS_PANDA.JEDI_Dataset_Contents "
            sqlFileStat += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
            if withLock:
                sqlFileStat += "FOR UPDATE NOWAIT "
            # begin transaction
            if useCommit:
                self.conn.begin()
            # get dataset
            sqlPD = "SELECT datasetID FROM ATLAS_PANDA.JEDI_Datasets "
            sqlPD += "WHERE jediTaskID=:jediTaskID AND type IN (:type1,:type2) AND masterID IS NULL "
            varMap = {}
            varMap[":jediTaskID"] = jobSpec.jediTaskID
            varMap[":type1"] = "input"
            varMap[":type2"] = "pseudo_input"
            self.cur.execute(sqlPD + comment, varMap)
            resPD = self.cur.fetchone()
            if resPD is not None:
                (datasetID,) = resPD
            else:
                datasetID = None
            # make pseudo files for dynamic number of events
            if EventServiceUtils.isDynNumEventsSH(jobSpec.specialHandling):
                pseudoFiles = self.create_pseudo_files_for_dyn_num_events(jobSpec, tmp_log)
            else:
                pseudoFiles = []
            is_job_cloning = EventServiceUtils.isJobCloningJob(jobSpec)
            # loop over all input files
            allOK = True
            for fileSpec in jobSpec.Files + pseudoFiles:
                if datasetID is None:
                    continue
                # only input file
                if jobSpec.processingType != "pmerge":
                    if fileSpec.datasetID != datasetID:
                        continue
                else:
                    if fileSpec.type != "input":
                        continue
                # skip if not normal JEDI files
                if fileSpec.fileID == "NULL":
                    continue
                varMap = {}
                varMap[":jediTaskID"] = fileSpec.jediTaskID
                varMap[":datasetID"] = fileSpec.datasetID
                varMap[":fileID"] = fileSpec.fileID
                self.cur.execute(sqlFileStat + comment, varMap)
                resFileStat = self.cur.fetchone()
                if resFileStat is None:
                    tmp_log.debug(f"jediTaskID={fileSpec.jediTaskID} datasetID={fileSpec.datasetID} fileID={fileSpec.fileID} is not found")
                    allOK = False
                    break
                else:
                    input_panda_id, fileStatus, attemptNr, keepTrack, is_waiting = resFileStat
                    if attemptNr is None:
                        continue
                    if keepTrack != 1:
                        continue
                    if attemptNr != fileSpec.attemptNr:
                        tmp_log.debug(
                            "jediTaskID={0} datasetID={1} fileID={2} attemptNr={3} is inconsitent with attemptNr={4} in JEDI".format(
                                fileSpec.jediTaskID,
                                fileSpec.datasetID,
                                fileSpec.fileID,
                                fileSpec.attemptNr,
                                attemptNr,
                            )
                        )
                        allOK = False
                        break
                    if fileStatus in ["finished"] or (
                        fileStatus not in ["running"] and jobSpec.computingSite != EventServiceUtils.siteIdForWaitingCoJumboJobs and is_waiting is None
                    ):
                        tmp_log.debug(
                            "jediTaskID={0} datasetID={1} fileID={2} attemptNr={3} is in wrong status ({4}) in JEDI".format(
                                fileSpec.jediTaskID,
                                fileSpec.datasetID,
                                fileSpec.fileID,
                                fileSpec.attemptNr,
                                fileStatus,
                            )
                        )
                        allOK = False
                        break
                    if not is_job_cloning and input_panda_id != jobSpec.PandaID:
                        tmp_log.debug(
                            f"jediTaskID={fileSpec.jediTaskID} datasetID={fileSpec.datasetID} fileID={fileSpec.fileID} attemptNr={fileSpec.attemptNr} has different PandaID={input_panda_id}"
                        )
                        allOK = False
                        break
            # commit
            if useCommit:
                if not self._commit():
                    raise RuntimeError("Commit error")
            tmp_log.debug(f"done with {allOK} for processingType={jobSpec.processingType}")
            return allOK
        except Exception:
            if useCommit:
                # roll back
                self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return None

    # set site for ES merge
    def setSiteForEsMerge(self, jobSpec, isFakeCJ, methodName, comment):
        comment = " /* DBProxy.setSiteForEsMerge */"
        tmp_log = self.create_tagged_logger(comment, f"PandaID={jobSpec.PandaID}")
        tmp_log.debug(f"looking for ES merge site")
        # merge on OS
        isMergeAtOS = EventServiceUtils.isMergeAtOS(jobSpec.specialHandling)
        # check where merge is done
        lookForMergeSite = True
        sqlWM = "SELECT /* use_json_type */ scj.data.catchall, scj.data.objectstores " "FROM ATLAS_PANDA.schedconfig_json scj " "WHERE scj.panda_queue=:siteid "

        varMap = {}
        varMap[":siteid"] = jobSpec.computingSite
        self.cur.execute(sqlWM + comment, varMap)
        resWM = self.cur.fetchone()
        resSN = []
        resSN_back = []
        catchAll, objectstores = None, None
        if resWM is not None:
            catchAll, objectstores = resWM
        if catchAll is None:
            catchAll = ""
        try:
            if isFakeCJ:
                objectstores = []
            else:
                objectstores = json.loads(objectstores)
        except Exception:
            objectstores = []
        # get objstoreIDs
        sqlZIP = "SELECT /*+ INDEX_RS_ASC(tab JEDI_EVENTS_FILEID_IDX) NO_INDEX_FFS(tab JEDI_EVENTS_PK) NO_INDEX_SS(tab JEDI_EVENTS_PK) */ "
        sqlZIP += f"DISTINCT zipRow_ID FROM {panda_config.schemaJEDI}.JEDI_Events "
        sqlZIP += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
        sqlZIP += "AND status=:esDone "
        sqlOST = f"SELECT fsize,destinationSE FROM {panda_config.schemaPANDA}.filesTable4 "
        sqlOST += "WHERE row_ID=:row_ID "
        sqlOST += "UNION "
        sqlOST += f"SELECT fsize,destinationSE FROM {panda_config.schemaPANDAARCH}.filesTable_ARCH "
        sqlOST += "WHERE row_ID=:row_ID "
        objStoreZipMap = dict()
        storageZipMap = dict()
        zipRowIDs = set()
        totalZipSize = 0
        for tmpFileSpec in jobSpec.Files:
            if tmpFileSpec.type in ["input", "pseudo_input"]:
                varMap = dict()
                varMap[":jediTaskID"] = tmpFileSpec.jediTaskID
                varMap[":datasetID"] = tmpFileSpec.datasetID
                varMap[":fileID"] = tmpFileSpec.fileID
                varMap[":esDone"] = EventServiceUtils.ST_done
                self.cur.execute(sqlZIP + comment, varMap)
                resZIP = self.cur.fetchall()
                for (zipRowID,) in resZIP:
                    if zipRowID is None:
                        continue
                    if zipRowID in zipRowIDs:
                        continue
                    zipRowIDs.add(zipRowID)
                    # get file info
                    varMap = dict()
                    varMap[":row_ID"] = zipRowID
                    self.cur.execute(sqlOST + comment, varMap)
                    resOST = self.cur.fetchone()
                    tmpFsize, tmpDestSE = resOST
                    totalZipSize += tmpFsize
                    tmpRSE = get_entity_module(self).convertObjIDtoEndPoint(panda_config.endpoint_mapfile, int(tmpDestSE.split("/")[0]))
                    if tmpRSE is not None:
                        objStoreZipMap.setdefault(tmpRSE["name"], 0)
                        objStoreZipMap[tmpRSE["name"]] += tmpFsize
                        if tmpRSE["type"].endswith("DISK"):
                            storageZipMap.setdefault(tmpRSE["name"], 0)
                            storageZipMap[tmpRSE["name"]] += tmpFsize
        if len(storageZipMap) > 0:
            sortedOST = sorted(storageZipMap.items(), key=operator.itemgetter(1))
        else:
            sortedOST = sorted(objStoreZipMap.items(), key=operator.itemgetter(1))
        sortedOST.reverse()
        if len(sortedOST) > 0:
            tmp_log.debug(f"old objectstores {str(objectstores)}")
            objectstores = [{"ddmendpoint": sortedOST[0][0]}]
            tmp_log.debug(f"{methodName} new objectstores {str(objectstores)}")
        if isFakeCJ:
            # use nucleus for fake co-jumbo since they don't have sub datasets
            pass
        elif "localEsMergeNC" in catchAll:
            # no site change
            lookForMergeSite = False
        else:
            # get sites in the nucleus associated to the site to run merge jobs in the same nucleus
            sqlSN = "SELECT /* use_json_type */ dr.panda_site_name, dr.ddm_endpoint_name "
            sqlSN += "FROM ATLAS_PANDA.panda_site ps1, ATLAS_PANDA.panda_site ps2, ATLAS_PANDA.schedconfig_json sc, ATLAS_PANDA.panda_ddm_relation dr "
            sqlSN += "WHERE ps1.panda_site_name=:site AND ps1.site_name=ps2.site_name AND sc.panda_queue=ps2.panda_site_name "
            sqlSN += "AND dr.panda_site_name=ps2.panda_site_name "
            sqlSN += "AND (sc.data.corecount IS NULL OR sc.data.corecount=1 OR sc.data.capability=:capability) "
            sqlSN += "AND (sc.data.maxtime=0 OR sc.data.maxtime>=86400) "
            sqlSN += "AND (sc.data.maxrss IS NULL OR sc.data.minrss=0) "
            sqlSN += "AND (sc.data.jobseed IS NULL OR sc.data.jobseed<>'es') "
            sqlSN += "AND sc.data.type != 'analysis' "

            if "localEsMerge" in catchAll and "useBrokerOff" in catchAll:
                sqlSN += "AND sc.data.status IN (:siteStatus1,:siteStatus2) "
            else:
                sqlSN += "AND sc.data.status=:siteStatus "

            sqlSN += "AND dr.default_write ='Y' "
            sqlSN += "AND (scope = 'default' OR scope IS NULL) "  # skip endpoints with analysis roles
            sqlSN += "AND (sc.data.wnconnectivity IS NULL OR sc.data.wnconnectivity LIKE :wc1) "

            varMap = {}
            varMap[":site"] = jobSpec.computingSite
            if "localEsMerge" in catchAll and "useBrokerOff" in catchAll:
                varMap[":siteStatus1"] = "online"
                varMap[":siteStatus2"] = "brokeroff"
            else:
                varMap[":siteStatus"] = "online"
            varMap[":wc1"] = "full%"
            varMap[":capability"] = "ucore"
            # get sites
            self.cur.execute(sqlSN + comment, varMap)
            if "localEsMerge" in catchAll:
                resSN = self.cur.fetchall()
            else:
                resSN_back = self.cur.fetchall()
        if len(resSN) == 0 and lookForMergeSite:
            # run merge jobs at destination
            if not jobSpec.destinationSE.startswith("nucleus:"):
                jobSpec.computingSite = jobSpec.destinationSE
                lookForMergeSite = False
            else:
                # use nucleus close to OS
                tmpNucleus = None
                if isMergeAtOS and len(objectstores) > 0:
                    osEndpoint = objectstores[0]["ddmendpoint"]
                    sqlCO = "SELECT site_name FROM ATLAS_PANDA.ddm_endpoint WHERE ddm_endpoint_name=:osEndpoint "
                    varMap = dict()
                    varMap[":osEndpoint"] = osEndpoint
                    self.cur.execute(sqlCO + comment, varMap)
                    resCO = self.cur.fetchone()
                    if resCO is not None:
                        (tmpNucleus,) = resCO
                        tmp_log.info(f"look for merge sites in nucleus:{tmpNucleus} close to pre-merged files")
                # use nucleus
                if tmpNucleus is None:
                    tmpNucleus = jobSpec.destinationSE.split(":")[-1]
                    tmp_log.info(f"look for merge sites in destination nucleus:{tmpNucleus}")
                # get sites in a nucleus
                sqlSN = "SELECT /* use_json_type */ dr.panda_site_name, dr.ddm_endpoint_name "
                sqlSN += "FROM ATLAS_PANDA.panda_site ps, ATLAS_PANDA.schedconfig_json sc, ATLAS_PANDA.panda_ddm_relation dr "
                sqlSN += "WHERE site_name=:nucleus AND sc.panda_queue=ps.panda_site_name "
                sqlSN += "AND dr.panda_site_name=ps.panda_site_name "
                sqlSN += "AND (sc.data.corecount IS NULL OR sc.data.corecount=1 OR sc.data.capability=:capability) "
                sqlSN += "AND (sc.maxtime=0 OR sc.maxtime>=86400) "
                sqlSN += "AND (sc.maxrss IS NULL OR sc.minrss=0) "
                sqlSN += "AND (sc.jobseed IS NULL OR sc.jobseed<>'es') "
                sqlSN += "AND sc.data.type != 'analysis' "
                sqlSN += "AND sc.data.status=:siteStatus "
                sqlSN += "AND dr.default_write='Y' "
                sqlSN += "AND (dr.scope = 'default' OR dr.scope IS NULL) "  # skip endpoints with analysis roles
                sqlSN += "AND (sc.data.wnconnectivity IS NULL OR sc.data.wnconnectivity LIKE :wc1) "

                varMap = {}
                varMap[":nucleus"] = tmpNucleus
                varMap[":siteStatus"] = "online"
                varMap[":wc1"] = "full%"
                varMap[":capability"] = "ucore"
                # get sites
                self.cur.execute(sqlSN + comment, varMap)
                resSN = self.cur.fetchall()

        # last resort for jumbo
        resSN_all = []
        if lookForMergeSite and (isFakeCJ or "useJumboJobs" in catchAll or len(resSN + resSN_back) == 0):
            sqlSN = "SELECT /* use_json_type */ dr.panda_site_name, dr.ddm_endpoint_name "
            sqlSN += "FROM ATLAS_PANDA.panda_site ps, ATLAS_PANDA.schedconfig_json sc, ATLAS_PANDA.panda_ddm_relation dr "
            sqlSN += "WHERE sc.panda_queue=ps.panda_site_name "
            sqlSN += "AND dr.panda_site_name=ps.panda_site_name "
            sqlSN += "AND (sc.data.corecount IS NULL OR sc.data.corecount=1 OR sc.data.capability=:capability) "
            sqlSN += "AND (sc.data.maxtime=0 OR sc.data.maxtime>=86400) "
            sqlSN += "AND (sc.data.maxrss IS NULL OR sc.data.minrss=0) "
            sqlSN += "AND (sc.data.jobseed IS NULL OR sc.data.jobseed<>'es') "
            sqlSN += "AND sc.data.type != 'analysis' "
            sqlSN += "AND sc.data.status=:siteStatus "
            sqlSN += "AND dr.default_write='Y' "
            sqlSN += "AND (dr.scope = 'default' OR dr.scope IS NULL) "  # skip endpoints with analysis roles
            sqlSN += "AND (sc.data.wnconnectivity IS NULL OR sc.data.wnconnectivity LIKE :wc1) "

            varMap = {}
            varMap[":siteStatus"] = "online"
            varMap[":wc1"] = "full%"
            varMap[":capability"] = "ucore"

            # get sites
            self.cur.execute(sqlSN + comment, varMap)
            resSN_all = self.cur.fetchall()

        # look for a site for merging
        if lookForMergeSite:
            # compare number of pilot requests
            maxNumPilot = 0
            sqlUG = "SELECT updateJob+getJob FROM ATLAS_PANDAMETA.sitedata "
            sqlUG += "WHERE site=:panda_site AND HOURS=:hours AND FLAG=:flag "

            sqlRJ = "SELECT SUM(num_of_jobs) FROM ATLAS_PANDA.MV_JOBSACTIVE4_STATS "
            sqlRJ += "WHERE computingSite=:panda_site AND jobStatus=:jobStatus "

            newSiteName = None
            for resItem in [resSN, resSN_back, resSN_all]:
                for tmp_panda_site_name, tmp_ddm_endpoint in resItem:
                    # get nPilot
                    varMap = {}
                    varMap[":panda_site"] = tmp_panda_site_name
                    varMap[":hours"] = 3
                    varMap[":flag"] = "production"
                    self.cur.execute(sqlUG + comment, varMap)
                    resUG = self.cur.fetchone()
                    if resUG is None:
                        nPilots = 0
                    else:
                        (nPilots,) = resUG
                    # get nRunning
                    varMap = {}
                    varMap[":panda_site"] = tmp_panda_site_name
                    varMap[":jobStatus"] = "running"
                    self.cur.execute(sqlRJ + comment, varMap)
                    resRJ = self.cur.fetchone()
                    if resRJ is None:
                        nRunning = 0
                    else:
                        (nRunning,) = resRJ
                    tmpStr = f"site={tmp_panda_site_name} nPilot={nPilots} nRunning={nRunning}"
                    tmp_log.info(f"{tmpStr}")
                    # use larger
                    if maxNumPilot < nPilots:
                        maxNumPilot = nPilots
                        jobSpec.computingSite = tmp_panda_site_name
                        newSiteName = jobSpec.computingSite
                        for tmpFileSpec in jobSpec.Files:
                            if tmpFileSpec.destinationDBlockToken.startswith("ddd:"):
                                tmpFileSpec.destinationDBlockToken = f"ddd:{tmp_ddm_endpoint}"
                                tmpFileSpec.destinationSE = jobSpec.computingSite
                if newSiteName is not None:
                    tmp_log.info(f"set merge site to {newSiteName}")
                    break
        # return
        return

    # set score site to ES job
    def setScoreSiteToEs(self, jobSpec, methodName, comment):
        comment = " /* DBProxy.setScoreSiteToEs */"
        tmp_log = self.create_tagged_logger(comment, f"PandaID={jobSpec.PandaID}")
        tmp_log.debug(f"looking for single-core site")
        # get score PQ in the nucleus associated to the site to run the small ES job
        sqlSN = "SELECT /* use_json_type */ ps2.panda_site_name "
        sqlSN += "FROM ATLAS_PANDA.panda_site ps1, ATLAS_PANDA.panda_site ps2, ATLAS_PANDA.schedconfig_json sc "
        sqlSN += "WHERE ps1.panda_site_name=:site AND ps1.site_name=ps2.site_name AND sc.panda_queue=ps2.panda_site_name "
        sqlSN += "AND (sc.data.corecount IS NULL OR sc.data.corecount=1 OR sc.data.capability=:capability) "
        sqlSN += "AND (sc.data.jobseed IS NULL OR sc.data.jobseed<>'std') "
        sqlSN += "AND sc.data.status=:siteStatus "

        varMap = {}
        varMap[":site"] = jobSpec.computingSite
        varMap[":siteStatus"] = "online"
        varMap[":capability"] = "ucore"

        # get sites
        self.cur.execute(sqlSN + comment, varMap)
        resSN = self.cur.fetchall()
        # compare number of pilot requests
        maxNumPilot = 0
        sqlUG = "SELECT updateJob+getJob FROM ATLAS_PANDAMETA.sitedata "
        sqlUG += "WHERE site=:panda_site AND HOURS=:hours AND FLAG=:flag "
        sqlRJ = "SELECT SUM(num_of_jobs) FROM ATLAS_PANDA.MV_JOBSACTIVE4_STATS "
        sqlRJ += "WHERE computingSite=:panda_site AND jobStatus=:jobStatus "
        newSiteName = None
        for (tmp_panda_site_name,) in resSN:
            # get nPilot
            varMap = {}
            varMap[":panda_site"] = tmp_panda_site_name
            varMap[":hours"] = 3
            varMap[":flag"] = "production"
            self.cur.execute(sqlUG + comment, varMap)
            resUG = self.cur.fetchone()
            if resUG is None:
                nPilots = 0
            else:
                (nPilots,) = resUG
            # get nRunning
            varMap = {}
            varMap[":panda_site"] = tmp_panda_site_name
            varMap[":jobStatus"] = "running"
            self.cur.execute(sqlRJ + comment, varMap)
            resRJ = self.cur.fetchone()
            if resRJ is None:
                nRunning = 0
            else:
                (nRunning,) = resRJ
            tmpStr = f"site={tmp_panda_site_name} nPilot={nPilots} nRunning={nRunning}"
            tmp_log.info(f"{methodName} {tmpStr}")
            # use larger
            if maxNumPilot < nPilots:
                maxNumPilot = nPilots
                jobSpec.computingSite = tmp_panda_site_name
                jobSpec.coreCount = 1
                jobSpec.minRamCount = 0
                jobSpec.resource_type = get_entity_module(self).get_resource_type_job(jobSpec)
                newSiteName = jobSpec.computingSite
        if newSiteName is not None:
            tmp_log.info(f"{methodName} set single-core site to {newSiteName}")
        else:
            tmp_log.info(f"{methodName} no single-core site for {jobSpec.computingSite}")
        # return
        return

    # get parent task id
    def get_parent_task_id_with_name(self, user_name, parent_name):
        comment = " /* DBProxy.get_task_id_with_dataset */"
        tmp_log = self.create_tagged_logger(comment, f"userName={user_name}")
        try:
            tmp_log.debug(f"try to find parent={parent_name}")
            # sql to get workers
            sqlC = "SELECT jediTaskID FROM ATLAS_PANDA.JEDI_Tasks " "WHERE userName=:userName AND taskName=:taskName " "ORDER BY jediTaskID DESC "
            # start transaction
            self.conn.begin()
            varMap = {}
            varMap[":userName"] = user_name
            varMap[":taskName"] = parent_name
            self.cur.execute(sqlC + comment, varMap)
            tid = self.cur.fetchone()
            if tid:
                (tid,) = tid
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmp_log.debug(f"got {tid}")
            return tid
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return None

    # insert TaskParams
    def insertTaskParamsPanda(self, taskParams, dn, prodRole, fqans, parent_tid, properErrorCode=False, allowActiveTask=False, decode=True):
        comment = " /* JediDBProxy.insertTaskParamsPanda */"
        try:
            # get compact DN
            compactDN = CoreUtils.clean_user_id(dn)
            if compactDN in ["", "NULL", None]:
                compactDN = dn
            tmp_log = self.create_tagged_logger(comment, f"userName={compactDN}")
            tmp_log.debug(f"start")

            # decode json
            if decode:
                taskParamsJson = PrioUtil.decodeJSON(taskParams)
            else:
                taskParamsJson = taskParams

            # set user name
            if not prodRole or "userName" not in taskParamsJson:
                taskParamsJson["userName"] = compactDN
            # identify parent
            if "parentTaskName" in taskParamsJson:
                parent_tid = self.get_parent_task_id_with_name(taskParamsJson["userName"], taskParamsJson["parentTaskName"])
                if not parent_tid:
                    tmpMsg = f"failed to find parent with user=\"{taskParamsJson['userName']}\" name={taskParamsJson['parentTaskName']}"
                    tmp_log.debug(f"{tmpMsg}")
                    return 11, tmpMsg
                else:
                    tmp_log.debug(f"found parent {parent_tid} with user=\"{taskParamsJson['userName']}\" name={taskParamsJson['parentTaskName']}")
            # set task type
            if not prodRole or "taskType" not in taskParamsJson:
                taskParamsJson["taskType"] = "anal"
                taskParamsJson["taskPriority"] = 1000
                # extract working group
                if "official" in taskParamsJson and taskParamsJson["official"] is True:
                    workingGroup = get_entity_module(self).getWorkingGroup(fqans)
                    if workingGroup is not None:
                        taskParamsJson["workingGroup"] = workingGroup

            tmp_log.debug(f"taskName={taskParamsJson['taskName']}")
            schemaDEFT = panda_config.schemaDEFT
            # sql to check task duplication for user
            sqlTDU = f"SELECT jediTaskID,status FROM {panda_config.schemaJEDI}.JEDI_Tasks "
            sqlTDU += "WHERE vo=:vo AND prodSourceLabel=:prodSourceLabel AND userName=:userName AND taskName=:taskName "
            sqlTDU += "ORDER BY jediTaskID DESC FOR UPDATE "
            # sql to check task duplication for group
            sqlTDW = f"SELECT jediTaskID,status FROM {panda_config.schemaJEDI}.JEDI_Tasks "
            sqlTDW += "WHERE vo=:vo AND prodSourceLabel=:prodSourceLabel AND taskName=:taskName "
            sqlTDW += "ORDER BY jediTaskID DESC FOR UPDATE "
            # sql to check DEFT table for user
            sqlCDU = f"SELECT taskid FROM {schemaDEFT}.T_TASK "
            sqlCDU += "WHERE vo=:vo AND prodSourceLabel=:prodSourceLabel AND userName=:userName AND taskName=:taskName "
            sqlCDU += "ORDER BY taskid DESC FOR UPDATE "
            # sql to check DEFT table for group
            sqlCDW = f"SELECT taskid FROM {schemaDEFT}.T_TASK "
            sqlCDW += "WHERE vo=:vo AND prodSourceLabel=:prodSourceLabel AND taskName=:taskName "
            sqlCDW += "ORDER BY taskid DESC FOR UPDATE "
            # sql to insert task parameters
            sqlT = f"INSERT INTO {schemaDEFT}.T_TASK "
            sqlT += "(taskid,status,submit_time,vo,prodSourceLabel,userName,taskName,jedi_task_parameters,priority,current_priority,parent_tid) VALUES "
            varMap = {}
            if self.backend in ["oracle", "postgres"]:
                sqlT += f"({schemaDEFT}.PRODSYS2_TASK_ID_SEQ.nextval,"
            else:
                # panda_config.backend == 'mysql':
                # fake sequence
                sql = " INSERT INTO PRODSYS2_TASK_ID_SEQ (col) VALUES (NULL) "
                self.cur.arraysize = 100
                self.cur.execute(sql + comment, {})
                sql2 = """ SELECT LAST_INSERT_ID() """
                self.cur.execute(sql2 + comment, {})
                (nextval,) = self.cur.fetchone()
                sqlT += "( :nextval ,".format(schemaDEFT)
                varMap[":nextval"] = nextval
            sqlT += ":status,CURRENT_DATE,:vo,:prodSourceLabel,:userName,:taskName,:param,:priority,:current_priority,"
            if parent_tid is None:
                if self.backend in ["oracle", "postgres"]:
                    sqlT += f"{schemaDEFT}.PRODSYS2_TASK_ID_SEQ.currval) "
                else:
                    # panda_config.backend == 'mysql':
                    # fake sequence
                    sql = " SELECT MAX(COL) FROM PRODSYS2_TASK_ID_SEQ "
                    self.cur.arraysize = 100
                    self.cur.execute(sql + comment, {})
                    (currval,) = self.cur.fetchone()
                    sqlT += " :currval ) "
                    varMap[":currval"] = currval
            else:
                sqlT += ":parent_tid) "
            sqlT += "RETURNING TASKID INTO :jediTaskID"
            # sql to delete command
            sqlDC = f"DELETE FROM {schemaDEFT}.PRODSYS_COMM "
            sqlDC += "WHERE COMM_TASK=:jediTaskID "
            # sql to insert command
            sqlIC = f"INSERT INTO {schemaDEFT}.PRODSYS_COMM (COMM_TASK,COMM_OWNER,COMM_CMD,COMM_PARAMETERS) "
            sqlIC += "VALUES (:jediTaskID,:comm_owner,:comm_cmd,:comm_parameters) "
            max_n_tasks = self.getConfigValue(
                "dbproxy",
                f"MAX_ACTIVE_TASKS_PER_USER_{taskParamsJson['prodSourceLabel']}",
            )
            # begin transaction
            self.conn.begin()
            # check max
            if max_n_tasks is not None:
                sqlTOT = (
                    "SELECT COUNT(*) "
                    "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_AUX_Status_MinTaskID tabA "
                    "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID "
                    "AND tabT.prodSourceLabel=:prodSourceLabel AND tabT.userName=:userName "
                ).format(panda_config.schemaJEDI)
                varMapTot = {}
                varMapTot[":prodSourceLabel"] = taskParamsJson["prodSourceLabel"]
                varMapTot[":userName"] = taskParamsJson["userName"]
                st_var_names_str, st_var_map = get_sql_IN_bind_variables(
                    [
                        "registered",
                        "defined",
                        "ready",
                        "scouting",
                        "running",
                        "paused",
                        "throttled",
                    ],
                    prefix=":",
                    value_as_suffix=True,
                )
                sqlTOT += f"AND tabT.status IN ({st_var_names_str}) "
                varMapTot.update(st_var_map)
                self.cur.execute(sqlTOT + comment, varMapTot)
                resTOT = self.cur.fetchone()
                if resTOT is not None and resTOT[0] > max_n_tasks:
                    # commit
                    if not self._commit():
                        raise RuntimeError("Commit error")
                    tmpMsg = f"Too many active tasks for {taskParamsJson['userName']} {resTOT[0]}>{max_n_tasks}"
                    tmp_log.debug(f"{tmpMsg}")
                    return 10, tmpMsg
            # check duplication
            goForward = True
            retFlag = False
            retVal = None
            errorCode = 0
            if taskParamsJson["taskType"] == "anal" and (("uniqueTaskName" in taskParamsJson and taskParamsJson["uniqueTaskName"] is True) or allowActiveTask):
                if "official" in taskParamsJson and taskParamsJson["official"] is True:
                    isOfficial = True
                else:
                    isOfficial = False
                # check JEDI
                varMap[":vo"] = taskParamsJson["vo"]
                if isOfficial:
                    pass
                else:
                    varMap[":userName"] = taskParamsJson["userName"]
                varMap[":taskName"] = taskParamsJson["taskName"]
                varMap[":prodSourceLabel"] = taskParamsJson["prodSourceLabel"]
                if isOfficial:
                    self.cur.execute(sqlTDW + comment, varMap)
                else:
                    self.cur.execute(sqlTDU + comment, varMap)
                resDT = self.cur.fetchone()
                if resDT is None:
                    # check DEFT table
                    varMap = {}
                    varMap[":vo"] = taskParamsJson["vo"]
                    if isOfficial:
                        pass
                    else:
                        varMap[":userName"] = taskParamsJson["userName"]
                    varMap[":taskName"] = taskParamsJson["taskName"]
                    varMap[":prodSourceLabel"] = taskParamsJson["prodSourceLabel"]
                    if isOfficial:
                        self.cur.execute(sqlCDW + comment, varMap)
                    else:
                        self.cur.execute(sqlCDU + comment, varMap)
                    resCD = self.cur.fetchone()
                    if resCD is not None:
                        # task is already in DEFT
                        (jediTaskID,) = resCD
                        tmp_log.debug(f"old jediTaskID={jediTaskID} with taskName={varMap[':taskName']} in DEFT table")
                        goForward = False
                        retVal = f"jediTaskID={jediTaskID} is already queued for outDS={taskParamsJson['taskName']}. "
                        retVal += "You cannot submit duplicated tasks. "
                        tmp_log.debug(f"skip since old task is already queued in DEFT")
                        errorCode = 1
                else:
                    # task is already in JEDI table
                    jediTaskID, taskStatus = resDT
                    tmp_log.debug(f"old jediTaskID={jediTaskID} with taskName={varMap[':taskName']} in status={taskStatus}")
                    # check task status
                    if taskStatus not in [
                        "finished",
                        "failed",
                        "aborted",
                        "done",
                        "exhausted",
                    ] and not (allowActiveTask and taskStatus in ["running", "scouting", "pending"] and taskParamsJson["prodSourceLabel"] in ["user"]):
                        # still active
                        goForward = False
                        retVal = f"jediTaskID={jediTaskID} is in the {taskStatus} state for outDS={taskParamsJson['taskName']}. "
                        retVal += "You can re-submit the task with new parameters for the same or another input "
                        retVal += "once it goes into finished/failed/done. "
                        retVal += "Or you can retry the task once it goes into running/finished/failed/done. "
                        retVal += "Note that retry != resubmission according to "
                        retVal += "https://twiki.cern.ch/twiki/bin/view/PanDA/PandaJEDI#Task_retry_and_resubmission "
                        tmp_log.debug(f"skip since old task is not yet finalized")
                        errorCode = 2
                    else:
                        # extract several params for incremental execution
                        newTaskParams = {}
                        newRamCount = None
                        for tmpKey in taskParamsJson:
                            tmpVal = taskParamsJson[tmpKey]
                            # dataset names
                            # site limitation
                            # command line parameters
                            # splitting hints
                            # fixed source code
                            if (
                                tmpKey.startswith("dsFor")
                                or tmpKey
                                in [
                                    "site",
                                    "cloud",
                                    "includedSite",
                                    "excludedSite",
                                    "cliParams",
                                    "nFiles",
                                    "nEvents",
                                    "fixedSandbox",
                                    "currentPriority",
                                    "priority",
                                    "ramCount",
                                    "loopingCheck",
                                    "forceStaged",
                                ]
                                + task_split_rules.changeable_split_rule_names
                            ):
                                if tmpKey == "priority":
                                    tmpKey = "currentPriority"
                                if tmpKey == "loopingCheck":
                                    tmpKey = "noLoopingCheck"
                                    if tmpVal:
                                        tmpVal = False
                                    else:
                                        tmpVal = True
                                newTaskParams[tmpKey] = tmpVal
                                if tmpKey == "fixedSandbox" and "sourceURL" in taskParamsJson:
                                    newTaskParams["sourceURL"] = taskParamsJson["sourceURL"]
                                elif tmpKey == "ramCount":
                                    newRamCount = tmpVal
                        # send command to reactivate the task
                        if not allowActiveTask or taskStatus in [
                            "finished",
                            "failed",
                            "aborted",
                            "done",
                            "exhausted",
                        ]:
                            # set new RAM count
                            if newRamCount is not None:
                                sqlRAM = f"UPDATE {panda_config.schemaJEDI}.JEDI_Dataset_Contents SET ramCount=:ramCount "
                                sqlRAM += "WHERE jediTaskID=:jediTaskID AND (ramCount IS NOT NULL AND ramCount>:ramCount) "
                                sqlRAM += f"AND datasetID IN (SELECT datasetID FROM {panda_config.schemaJEDI}.JEDI_Datasets "
                                sqlRAM += "WHERE jediTaskID=:jediTaskID AND type IN (:type1,:type2)) "
                                varMap = {}
                                varMap[":jediTaskID"] = jediTaskID
                                varMap[":type1"] = "input"
                                varMap[":type2"] = "pseudo_input"
                                varMap[":ramCount"] = newRamCount
                                self.cur.execute(sqlRAM + comment, varMap)
                                sqlRAMT = f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks SET ramCount=:ramCount WHERE jediTaskID=:jediTaskID "
                                varMap = {}
                                varMap[":jediTaskID"] = jediTaskID
                                varMap[":ramCount"] = newRamCount
                                self.cur.execute(sqlRAMT + comment, varMap)
                            # delete command just in case
                            varMap = {}
                            varMap[":jediTaskID"] = jediTaskID
                            self.cur.execute(sqlDC + comment, varMap)
                            # insert command
                            varMap = {}
                            varMap[":jediTaskID"] = jediTaskID
                            varMap[":comm_cmd"] = "incexec"
                            varMap[":comm_owner"] = "DEFT"
                            varMap[":comm_parameters"] = json.dumps(newTaskParams)
                            self.cur.execute(sqlIC + comment, varMap)
                            tmp_log.info(f"{varMap[':comm_cmd']} jediTaskID={jediTaskID} with {str(newTaskParams)}")
                            retVal = "reactivation accepted. "
                            retVal += f"jediTaskID={jediTaskID} (currently in {taskStatus} state) will be re-executed with old and/or new input"
                            errorCode = 3
                        else:
                            # sql to read task params
                            sqlTP = f"SELECT taskParams FROM {panda_config.schemaJEDI}.JEDI_TaskParams WHERE jediTaskID=:jediTaskID "
                            varMap = {}
                            varMap[":jediTaskID"] = jediTaskID
                            self.cur.execute(sqlTP + comment, varMap)
                            tmpStr = ""
                            for (tmpItem,) in self.cur:
                                try:
                                    tmpStr = tmpItem.read()
                                except AttributeError:
                                    tmpStr = str(tmpItem)
                                break
                            # decode json
                            taskParamsJson = json.loads(tmpStr)
                            # just change some params for active task
                            for tmpKey in newTaskParams:
                                tmpVal = newTaskParams[tmpKey]
                                taskParamsJson[tmpKey] = tmpVal
                            # update params
                            sqlTU = f"UPDATE {panda_config.schemaJEDI}.JEDI_TaskParams SET taskParams=:taskParams "
                            sqlTU += "WHERE jediTaskID=:jediTaskID "
                            varMap = {}
                            varMap[":jediTaskID"] = jediTaskID
                            varMap[":taskParams"] = json.dumps(taskParamsJson)
                            self.cur.execute(sqlTU + comment, varMap)
                            tmp_log.debug(f"add new params for jediTaskID={jediTaskID} with {str(newTaskParams)}")
                            retVal = f"{taskStatus}. new tasks params have been set to jediTaskID={jediTaskID}. "
                            errorCode = 5
                        goForward = False
                        retFlag = True
            if goForward:
                # insert task parameters
                taskParams = json.dumps(taskParamsJson)
                varMap = {}
                varMap[":param"] = taskParams
                varMap[":status"] = "waiting"
                varMap[":vo"] = taskParamsJson["vo"]
                varMap[":userName"] = taskParamsJson["userName"]
                varMap[":taskName"] = taskParamsJson["taskName"]
                if parent_tid is not None:
                    varMap[":parent_tid"] = parent_tid
                varMap[":prodSourceLabel"] = taskParamsJson["prodSourceLabel"]
                varMap[":jediTaskID"] = self.cur.var(varNUMBER)
                if "taskPriority" in taskParamsJson:
                    varMap[":priority"] = taskParamsJson["taskPriority"]
                else:
                    varMap[":priority"] = 100
                varMap[":current_priority"] = varMap[":priority"]
                self.cur.execute(sqlT + comment, varMap)
                val = self.getvalue_corrector(self.cur.getvalue(varMap[":jediTaskID"]))
                jediTaskID = int(val)
                if properErrorCode:
                    retVal = f"succeeded. new jediTaskID={jediTaskID}"
                else:
                    retVal = jediTaskID
                tmp_log.debug(f"inserted new jediTaskID={jediTaskID}")
                retFlag = True
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmp_log.debug(f"done")
            if properErrorCode:
                return errorCode, retVal
            return retFlag, retVal
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            errorCode = 4
            retVal = "failed to register task"
            if properErrorCode:
                return errorCode, retVal
            return False, retVal

    # send command to task through DEFT
    def sendCommandTaskPanda(
        self,
        jediTaskID,
        dn,
        prodRole,
        comStr,
        comComment=None,
        useCommit=True,
        properErrorCode=False,
        comQualifier=None,
        broadcast=False,
    ):
        comment = " /* JediDBProxy.sendCommandTaskPanda */"
        tmp_log = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID}")
        try:
            # get compact DN
            compactDN = CoreUtils.clean_user_id(dn)
            if compactDN in ["", "NULL", None]:
                compactDN = dn
            tmp_log.debug(f"start com={comStr} DN={compactDN} prod={prodRole} comment={comComment} qualifier={comQualifier} broadcast={broadcast}")
            # sql to check status and owner
            sqlTC = f"SELECT status,userName,prodSourceLabel FROM {panda_config.schemaJEDI}.JEDI_Tasks "
            sqlTC += "WHERE jediTaskID=:jediTaskID FOR UPDATE "
            # sql to delete command
            schemaDEFT = panda_config.schemaDEFT
            sqlT = f"DELETE FROM {schemaDEFT}.PRODSYS_COMM "
            sqlT += "WHERE COMM_TASK=:jediTaskID "
            # sql to insert command
            sqlC = f"INSERT INTO {schemaDEFT}.PRODSYS_COMM (COMM_TASK,COMM_OWNER,COMM_CMD,COMM_COMMENT) "
            sqlC += "VALUES (:jediTaskID,:comm_owner,:comm_cmd,:comm_comment) "
            goForward = True
            retStr = ""
            retCode = 0
            sendMsgToPilot = False
            # begin transaction
            if useCommit:
                self.conn.begin()
            # get task status and owner
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            self.cur.execute(sqlTC + comment, varMap)
            resTC = self.cur.fetchone()
            if resTC is None:
                # task not found
                retStr = f"jediTaskID={jediTaskID} not found"
                tmp_log.debug(retStr)
                goForward = False
                retCode = 2
            else:
                taskStatus, userName, prodSourceLabel = resTC
                tmp_log.debug(f"status={taskStatus}")
            # check owner
            if goForward:
                if not prodRole and compactDN != userName:
                    retStr = "Permission denied: not the task owner or no production role"
                    tmp_log.debug(retStr)
                    goForward = False
                    retCode = 3
            # check task status
            if goForward:
                add_msg = ""
                if comStr in ["kill", "finish"]:
                    sendMsgToPilot = broadcast
                    if taskStatus in [
                        "finished",
                        "done",
                        "prepared",
                        "broken",
                        "aborted",
                        "aborted",
                        "toabort",
                        "aborting",
                        "failed",
                    ]:
                        goForward = False
                if comStr == "retry":
                    if taskStatus not in ["finished", "failed", "exhausted"]:
                        goForward = False
                    elif taskStatus == "exhausted" and not prodRole:
                        goForward = False
                        add_msg = "and production role is missing"
                if comStr == "incexec":
                    if taskStatus not in [
                        "finished",
                        "failed",
                        "aborted",
                        "done",
                        "exhausted",
                    ]:
                        goForward = False
                if comStr == "reassign":
                    if taskStatus not in [
                        "registered",
                        "defined",
                        "ready",
                        "running",
                        "scouting",
                        "scouted",
                        "pending",
                        "assigning",
                        "exhausted",
                    ]:
                        goForward = False
                if comStr == "pause":
                    if taskStatus in [
                        "finished",
                        "failed",
                        "done",
                        "aborted",
                        "broken",
                        "paused",
                    ]:
                        goForward = False
                if comStr == "resume":
                    if taskStatus not in ["paused", "throttled", "staging"]:
                        goForward = False
                if comStr == "avalanche":
                    if taskStatus not in ["scouting"]:
                        goForward = False
                if comStr == "release":
                    if taskStatus not in ["scouting", "pending", "running", "ready", "assigning", "defined"]:
                        goForward = False
                if not goForward:
                    retStr = f"Command rejected: the {comStr} command is not accepted " f"if the task is in {taskStatus} status {add_msg}"
                    tmp_log.debug(f"{retStr}")
                    retCode = 4
                    # retry for failed analysis jobs
                    if comStr == "retry" and properErrorCode and taskStatus in ["running", "scouting", "pending"] and prodSourceLabel in ["user"]:
                        retCode = 5
                        retStr = taskStatus
            if goForward:
                # delete command just in case
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                self.cur.execute(sqlT + comment, varMap)
                # insert command
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                varMap[":comm_cmd"] = comStr
                varMap[":comm_owner"] = "DEFT"
                if comComment is None:
                    tmpStr = ""
                    if comQualifier not in ["", None]:
                        tmpStr += f"{comQualifier} "
                    tmpStr += f"{comStr} by {compactDN}"
                    varMap[":comm_comment"] = tmpStr
                else:
                    varMap[":comm_comment"] = comComment
                self.cur.execute(sqlC + comment, varMap)
                retStr = f"command={comStr} is registered. will be executed in a few minutes"
                tmp_log.info(f"{retStr}")
            # commit
            if useCommit:
                if not self._commit():
                    raise RuntimeError("Commit error")
            # send command to the pilot
            if sendMsgToPilot:
                mb_proxy_topic = self.get_mb_proxy("panda_pilot_topic")
                if mb_proxy_topic:
                    tmp_log.debug(f"push {comStr}")
                    srv_msg_utils.send_task_message(mb_proxy_topic, comStr, jediTaskID)
                else:
                    tmp_log.debug("message topic not configured")
            if properErrorCode:
                return retCode, retStr
            else:
                if retCode == 0:
                    return True, retStr
                else:
                    return False, retStr
        except Exception:
            # roll back
            if useCommit:
                self._rollback()
            # error
            self.dump_error_message(tmp_log)
            if properErrorCode:
                return 1, "failed to register command"
            else:
                return False, "failed to register command"

    # get active JediTasks in a time range
    def getJediTasksInTimeRange(self, dn, timeRange, fullFlag=False, minTaskID=None, task_type="user"):
        comment = " /* DBProxy.getJediTasksInTimeRange */"
        tmp_log = self.create_tagged_logger(comment)
        tmp_log.debug(f"DN={dn} range={timeRange.strftime('%Y-%m-%d %H:%M:%S')} full={fullFlag}")
        try:
            # get compact DN
            compactDN = CoreUtils.clean_user_id(dn)
            if compactDN in ["", "NULL", None]:
                compactDN = dn
            # make sql
            attrList = [
                "jediTaskID",
                "modificationTime",
                "status",
                "processingType",
                "transUses",
                "transHome",
                "architecture",
                "reqID",
                "creationDate",
                "site",
                "cloud",
                "taskName",
            ]
            sql = "SELECT "
            if fullFlag:
                sql += "* FROM (SELECT "
            for tmpAttr in attrList:
                sql += f"{tmpAttr},"
            sql = sql[:-1]
            sql += f" FROM {panda_config.schemaJEDI}.JEDI_Tasks "
            sql += "WHERE userName=:userName AND modificationTime>=:modificationTime AND prodSourceLabel=:prodSourceLabel "
            varMap = {}
            varMap[":userName"] = compactDN
            varMap[":prodSourceLabel"] = task_type
            varMap[":modificationTime"] = timeRange
            if minTaskID is not None:
                sql += "AND jediTaskID>:minTaskID "
                varMap[":minTaskID"] = minTaskID
            if fullFlag:
                sql += "ORDER BY jediTaskID) WHERE rownum<=500 "
            # start transaction
            self.conn.begin()
            # select
            self.cur.arraysize = 10000
            tmp_log.debug(sql + comment + str(varMap))
            self.cur.execute(sql + comment, varMap)
            resList = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # append
            retTasks = {}
            for tmpRes in resList:
                tmpDict = {}
                for tmpIdx, tmpAttr in enumerate(attrList):
                    tmpDict[tmpAttr] = tmpRes[tmpIdx]
                if fullFlag:
                    # additional info
                    addInfo = self.getJediTaskDigest(tmpDict["jediTaskID"])
                    for k in addInfo:
                        v = addInfo[k]
                        tmpDict[k] = v
                retTasks[tmpDict["reqID"]] = tmpDict
            tmp_log.debug(f"{str(retTasks)}")
            return retTasks
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return {}

    # get details of JediTask
    def getJediTaskDetails(self, jediTaskID, fullFlag, withTaskInfo):
        comment = " /* DBProxy.getJediTaskDetails */"
        tmp_log = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID}")
        tmp_log.debug(f"full={fullFlag}")
        try:
            retDict = {
                "inDS": "",
                "outDS": "",
                "statistics": "",
                "PandaID": set(),
                "mergeStatus": None,
                "mergePandaID": set(),
            }
            # sql to get task status
            sqlT = f"SELECT status FROM {panda_config.schemaJEDI}.JEDI_Tasks WHERE jediTaskID=:jediTaskID "
            # sql to get datasets
            sqlD = "SELECT datasetID,datasetName,containerName,type,nFiles,nFilesTobeUsed,nFilesFinished,nFilesFailed,masterID,nFilesUsed,nFilesOnHold "
            sqlD += f"FROM {panda_config.schemaJEDI}.JEDI_Datasets "
            sqlD += "WHERE jediTaskID=:jediTaskID "
            # sql to get PandaIDs
            sqlP = f"SELECT PandaID,COUNT(*) FROM {panda_config.schemaJEDI}.JEDI_Dataset_Contents "
            sqlP += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND PandaID IS NOT NULL "
            sqlP += "GROUP BY PandaID "
            # sql to get job status
            sqlJS = "SELECT PandaID,jobStatus,processingType FROM ATLAS_PANDA.jobsDefined4 "
            sqlJS += "WHERE jediTaskID=:jediTaskID AND prodSourceLabel=:prodSourceLabel "
            sqlJS += "UNION "
            sqlJS = "SELECT PandaID,jobStatus,processingType FROM ATLAS_PANDA.jobsActive4 "
            sqlJS += "WHERE jediTaskID=:jediTaskID AND prodSourceLabel=:prodSourceLabel "
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            # start transaction
            self.conn.begin()
            # select
            self.cur.arraysize = 100000
            # get task status
            if withTaskInfo:
                self.cur.execute(sqlT + comment, varMap)
                resT = self.cur.fetchone()
                if resT is None:
                    raise RuntimeError("No task info")
                retDict["status"] = resT[0]
            # get datasets
            self.cur.execute(sqlD + comment, varMap)
            resList = self.cur.fetchall()
            # get job status
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            varMap[":prodSourceLabel"] = "user"
            self.cur.execute(sqlJS + comment, varMap)
            resJS = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # make jobstatus map
            jobStatPandaIDs = {}
            for tmpPandaID, tmpJobStatus, tmpProcessingType in resJS:
                # ignore merge jobs
                if tmpProcessingType == "pmerge":
                    continue
                jobStatPandaIDs[tmpPandaID] = tmpJobStatus
            # append
            inDSs = []
            outDSs = []
            totalNumFiles = 0
            totalTobeDone = 0
            totalFinished = 0
            totalFailed = 0
            totalStatMap = {}
            for (
                datasetID,
                datasetName,
                containerName,
                datasetType,
                nFiles,
                nFilesTobeUsed,
                nFilesFinished,
                nFilesFailed,
                masterID,
                nFilesUsed,
                nFilesOnHold,
            ) in resList:
                # primay input
                if datasetType in ["input", "pseudo_input", "trn_log"] and masterID is None:
                    # unmerge dataset
                    if datasetType == "trn_log":
                        unmergeFlag = True
                    else:
                        unmergeFlag = False
                    # collect input dataset names
                    if datasetType == "input":
                        # use container name if not empty
                        if containerName not in [None, ""]:
                            targetName = containerName
                        else:
                            targetName = datasetName
                        if targetName not in inDSs:
                            inDSs.append(targetName)
                            retDict["inDS"] += f"{targetName},"
                    # statistics
                    if datasetType in ["input", "pseudo_input"]:
                        totalNumFiles += nFiles
                        totalFinished += nFilesFinished
                        totalFailed += nFilesFailed
                        totalTobeDone += nFiles - nFilesUsed
                    # collect PandaIDs
                    self.conn.begin()
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    varMap[":datasetID"] = datasetID
                    self.cur.execute(sqlP + comment, varMap)
                    resP = self.cur.fetchall()
                    # commit
                    if not self._commit():
                        raise RuntimeError("Commit error")
                    for tmpPandaID, tmpNumFiles in resP:
                        if not unmergeFlag:
                            retDict["PandaID"].add(tmpPandaID)
                        else:
                            retDict["mergePandaID"].add(tmpPandaID)
                        # map to job status
                        if datasetType in ["input", "pseudo_input"]:
                            if tmpPandaID in jobStatPandaIDs:
                                tmpJobStatus = jobStatPandaIDs[tmpPandaID]
                                if tmpJobStatus not in totalStatMap:
                                    totalStatMap[tmpJobStatus] = 0
                                totalStatMap[tmpJobStatus] += tmpNumFiles
                # output
                if datasetType.endswith("output") or datasetType.endswith("log"):
                    # ignore transient datasets
                    if "trn_" in datasetType:
                        continue
                    # use container name if not empty
                    if containerName not in [None, ""]:
                        targetName = containerName
                    else:
                        targetName = datasetName
                    if targetName not in outDSs:
                        outDSs.append(targetName)
                        retDict["outDS"] += f"{targetName},"
            retDict["inDS"] = retDict["inDS"][:-1]
            retDict["outDS"] = retDict["outDS"][:-1]
            # statistics
            statStr = ""
            nPicked = totalNumFiles
            if totalTobeDone > 0:
                statStr += f"tobedone*{totalTobeDone},"
                nPicked -= totalTobeDone
            if totalFinished > 0:
                statStr += f"finished*{totalFinished},"
                nPicked -= totalFinished
            if totalFailed > 0:
                statStr += f"failed*{totalFailed},"
                nPicked -= totalFailed
            for tmpJobStatus in totalStatMap:
                tmpNumFiles = totalStatMap[tmpJobStatus]
                # skip active failed
                if tmpJobStatus == "failed":
                    continue
                statStr += f"{tmpJobStatus}*{tmpNumFiles},"
                nPicked -= tmpNumFiles
            if nPicked > 0:
                statStr += f"picked*{nPicked},"
            retDict["statistics"] = statStr[:-1]
            # command line parameters
            if fullFlag:
                # sql to read task params
                sql = f"SELECT taskParams FROM {panda_config.schemaJEDI}.JEDI_TaskParams WHERE jediTaskID=:jediTaskID "
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                # begin transaction
                self.conn.begin()
                self.cur.execute(sql + comment, varMap)
                retStr = ""
                for (tmpItem,) in self.cur:
                    try:
                        retStr = tmpItem.read()
                    except AttributeError:
                        retStr = str(tmpItem)
                    break
                # commit
                if not self._commit():
                    raise RuntimeError("Commit error")
                # decode json
                taskParamsJson = json.loads(retStr)
                if "cliParams" in taskParamsJson:
                    retDict["cliParams"] = taskParamsJson["cliParams"]
                else:
                    retDict["cliParams"] = ""
            retDict["PandaID"] = list(retDict["PandaID"])
            retDict["mergePandaID"] = list(retDict["mergePandaID"])
            tmp_log.debug(f"{str(retDict)}")
            return retDict
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return {}

    # get JediTask digest
    def getJediTaskDigest(self, jediTaskID):
        comment = " /* DBProxy.getJediTaskDigest */"
        tmp_log = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID}")
        try:
            retDict = {
                "inDS": "",
                "outDS": "",
                "statistics": "",
                "PandaID": [],
                "mergeStatus": None,
                "mergePandaID": [],
            }
            # sql to get datasets
            sqlD = "SELECT datasetName,containerName,type "
            sqlD += f"FROM {panda_config.schemaJEDI}.JEDI_Datasets "
            sqlD += "WHERE jediTaskID=:jediTaskID AND ((type IN (:in1,:in2) AND masterID IS NULL) OR type IN (:out1,:out2)) "
            sqlD += "GROUP BY datasetName,containerName,type "
            # sql to get job status
            sqlJS = "SELECT proc_status,COUNT(*) FROM {0}.JEDI_Datasets d,{0}.JEDI_Dataset_Contents c ".format(panda_config.schemaJEDI)
            sqlJS += "WHERE c.jediTaskID=d.jediTaskID AND c.datasetID=d.datasetID AND d.jediTaskID=:jediTaskID "
            sqlJS += "AND d.type IN (:in1,:in2) AND d.masterID IS NULL "
            sqlJS += "GROUP BY proc_status "
            # sql to read task params
            sqlTP = f"SELECT taskParams FROM {panda_config.schemaJEDI}.JEDI_TaskParams WHERE jediTaskID=:jediTaskID "
            # start transaction
            self.conn.begin()
            self.cur.arraysize = 100000
            # get datasets
            inDSs = set()
            outDSs = set()
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            varMap[":in1"] = "input"
            varMap[":in2"] = "pseudo_input"
            varMap[":out1"] = "output"
            varMap[":out2"] = "tmpl_output"
            self.cur.execute(sqlD + comment, varMap)
            resList = self.cur.fetchall()
            for datasetName, containerName, datasetType in resList:
                # use container name if not empty
                if containerName not in [None, ""]:
                    targetName = containerName
                else:
                    targetName = datasetName
                if "output" in datasetType:
                    outDSs.add(targetName)
                else:
                    inDSs.add(targetName)
            inDSs = sorted(inDSs)
            retDict["inDS"] = ",".join(inDSs)
            outDSs = sorted(outDSs)
            retDict["outDS"] = ",".join(outDSs)
            # get job status
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            varMap[":in1"] = "input"
            varMap[":in2"] = "pseudo_input"
            self.cur.execute(sqlJS + comment, varMap)
            resJS = self.cur.fetchall()
            jobStatMap = dict()
            for proc_status, ninputs in resJS:
                jobStatMap[proc_status] = ninputs
            psList = sorted(jobStatMap)
            retDict["statistics"] = ",".join([f"{j}*{jobStatMap[j]}" for j in psList])
            # command line parameters
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            self.cur.execute(sqlTP + comment, varMap)
            retStr = ""
            for (tmpItem,) in self.cur:
                try:
                    retStr = tmpItem.read()
                except AttributeError:
                    retStr = str(tmpItem)
                break
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # decode json
            taskParamsJson = json.loads(retStr)
            if "cliParams" in taskParamsJson:
                retDict["cliParams"] = taskParamsJson["cliParams"]
            else:
                retDict["cliParams"] = ""
            tmp_log.debug(f"{str(retDict)}")
            return retDict
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return {}

    # change task attribute
    def changeTaskAttributePanda(self, jediTaskID, attrName, attrValue):
        comment = " /* DBProxy.changeTaskAttributePanda */"
        tmp_log = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID}")
        tmp_log.debug(f"name={attrName} value={attrValue}")
        try:
            # sql to update JEDI task table
            sqlT = f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks SET "
            sqlT += "{0}=:{0} WHERE jediTaskID=:jediTaskID ".format(attrName)
            # start transaction
            self.conn.begin()
            # select
            self.cur.arraysize = 10
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            keyName = f":{attrName}"
            varMap[keyName] = attrValue
            # update JEDI
            self.cur.execute(sqlT + comment, varMap)
            nRow = self.cur.rowcount
            if nRow:
                get_entity_module(self).reset_resource_type_task(jediTaskID, use_commit=False)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmp_log.debug(f"done with {nRow}")
            return nRow
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return None

    # make fake co-jumbo
    def makeFakeCoJumbo(self, oldJobSpec):
        comment = " /* DBProxy.self.makeFakeCoJumbo */"
        tmp_log = self.create_tagged_logger(comment, f"PandaID={oldJobSpec.PandaID}")
        tmp_log.debug("start")
        try:
            # make a new job
            jobSpec = copy.copy(oldJobSpec)
            jobSpec.Files = []
            # reset job attributes
            jobSpec.startTime = None
            jobSpec.creationTime = naive_utcnow()
            jobSpec.modificationTime = jobSpec.creationTime
            jobSpec.stateChangeTime = jobSpec.creationTime
            jobSpec.batchID = None
            jobSpec.schedulerID = None
            jobSpec.pilotID = None
            jobSpec.endTime = None
            jobSpec.transExitCode = None
            jobSpec.jobMetrics = None
            jobSpec.jobSubStatus = None
            jobSpec.actualCoreCount = None
            jobSpec.hs06sec = None
            jobSpec.nEvents = None
            jobSpec.cpuConsumptionTime = None
            jobSpec.computingSite = EventServiceUtils.siteIdForWaitingCoJumboJobs
            jobSpec.jobExecutionID = 0
            jobSpec.jobStatus = "waiting"
            jobSpec.jobSubStatus = None
            for attr in jobSpec._attributes:
                for patt in [
                    "ErrorCode",
                    "ErrorDiag",
                    "CHAR",
                    "BYTES",
                    "RSS",
                    "PSS",
                    "VMEM",
                    "SWAP",
                ]:
                    if attr.endswith(patt):
                        setattr(jobSpec, attr, None)
                        break
            # read files
            varMap = {}
            varMap[":PandaID"] = oldJobSpec.PandaID
            sqlFile = f"SELECT {FileSpec.columnNames()} FROM ATLAS_PANDA.filesTable4 "
            sqlFile += "WHERE PandaID=:PandaID "
            self.cur.arraysize = 100000
            self.cur.execute(sqlFile + comment, varMap)
            resFs = self.cur.fetchall()
            # loop over all files
            for resF in resFs:
                # add
                fileSpec = FileSpec()
                fileSpec.pack(resF)
                # skip zip
                if fileSpec.type.startswith("zip"):
                    continue
                jobSpec.addFile(fileSpec)
                # reset file status
                if fileSpec.type in ["output", "log"]:
                    fileSpec.status = "unknown"
            # read job parameters
            sqlJobP = "SELECT jobParameters FROM ATLAS_PANDA.jobParamsTable WHERE PandaID=:PandaID "
            varMap = {}
            varMap[":PandaID"] = oldJobSpec.PandaID
            self.cur.execute(sqlJobP + comment, varMap)
            for (clobJobP,) in self.cur:
                try:
                    jobSpec.jobParameters = clobJobP.read()
                except AttributeError:
                    jobSpec.jobParameters = str(clobJobP)
                break
            # insert job with new PandaID
            sql1 = f"INSERT INTO ATLAS_PANDA.jobsDefined4 ({JobSpec.columnNames()}) "
            sql1 += JobSpec.bindValuesExpression(useSeq=True)
            sql1 += " RETURNING PandaID INTO :newPandaID"
            varMap = jobSpec.valuesMap(useSeq=True)
            varMap[":newPandaID"] = self.cur.var(varNUMBER)
            # insert
            retI = self.cur.execute(sql1 + comment, varMap)
            # set PandaID
            val = self.getvalue_corrector(self.cur.getvalue(varMap[":newPandaID"]))
            jobSpec.PandaID = int(val)
            msgStr = f"Generate a fake co-jumbo new PandaID={jobSpec.PandaID} at {jobSpec.computingSite} "
            tmp_log.debug(msgStr)
            # insert files
            sqlFile = f"INSERT INTO ATLAS_PANDA.filesTable4 ({FileSpec.columnNames()}) "
            sqlFile += FileSpec.bindValuesExpression(useSeq=True)
            sqlFile += " RETURNING row_ID INTO :newRowID"
            for fileSpec in jobSpec.Files:
                # reset rowID
                fileSpec.row_ID = None
                # change GUID and LFN for log
                if fileSpec.type == "log":
                    fileSpec.GUID = str(uuid.uuid4())
                    fileSpec.lfn = re.sub(f"\\.{oldJobSpec.PandaID}$", "", fileSpec.lfn)
                # insert
                varMap = fileSpec.valuesMap(useSeq=True)
                varMap[":newRowID"] = self.cur.var(varNUMBER)
                self.cur.execute(sqlFile + comment, varMap)
                val = self.getvalue_corrector(self.cur.getvalue(varMap[":newRowID"]))
                fileSpec.row_ID = int(val)
            # insert job parameters
            sqlJob = "INSERT INTO ATLAS_PANDA.jobParamsTable (PandaID,jobParameters) VALUES (:PandaID,:param) "
            varMap = {}
            varMap[":PandaID"] = jobSpec.PandaID
            varMap[":param"] = jobSpec.jobParameters
            self.cur.execute(sqlJob + comment, varMap)
            self.recordStatusChange(jobSpec.PandaID, jobSpec.jobStatus, jobInfo=jobSpec, useCommit=False)
            self.push_job_status_message(jobSpec, jobSpec.PandaID, jobSpec.jobStatus)
            # return
            tmp_log.debug("done")
            return 1
        except Exception:
            # error
            self.dump_error_message(tmp_log)
            return 0

    # get active jumbo jobs for a task
    def getActiveJumboJobs_JEDI(self, jediTaskID):
        comment = " /* JediDBProxy.getActiveJumboJobs_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID}")
        tmpLog.debug("start")
        try:
            # sql
            sql = "SELECT PandaID,jobStatus,computingSite "
            sql += f"FROM {panda_config.schemaPANDA}.jobsDefined4 "
            sql += "WHERE jediTaskID=:jediTaskID AND eventService=:jumboJob "
            sql += "UNION "
            sql += "SELECT PandaID,jobStatus,computingSite "
            sql += f"FROM {panda_config.schemaPANDA}.jobsActive4 "
            sql += "WHERE jediTaskID=:jediTaskID AND eventService=:jumboJob "
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            varMap[":jumboJob"] = EventServiceUtils.jumboJobFlagNumber
            # start transaction
            self.conn.begin()
            self.cur.execute(sql + comment, varMap)
            resList = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            retMap = {}
            for pandaID, jobStatus, computingSite in resList:
                if jobStatus in ["transferring", "holding"]:
                    continue
                retMap[pandaID] = {"status": jobStatus, "site": computingSite}
            tmpLog.debug(str(retMap))
            return retMap
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return {}

    # set useJumbo flag
    def setUseJumboFlag_JEDI(self, jediTaskID, statusStr):
        comment = " /* JediDBProxy.setUseJumboFlag_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID} status={statusStr}")
        tmpLog.debug("start")
        try:
            # check current flag
            sqlCF = f"SELECT useJumbo FROM {panda_config.schemaJEDI}.JEDI_Tasks "
            sqlCF += "WHERE jediTaskID=:jediTaskID "
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            # start transaction
            self.conn.begin()
            self.cur.execute(sqlCF + comment, varMap)
            (curStr,) = self.cur.fetchone()
            # check files
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            sqlFF = f"SELECT nFilesToBeUsed-nFilesUsed-nFilesWaiting FROM {panda_config.schemaJEDI}.JEDI_Datasets "
            sqlFF += "WHERE jediTaskID=:jediTaskID "
            sqlFF += f"AND type IN ({INPUT_TYPES_var_str}) "
            varMap.update(INPUT_TYPES_var_map)
            sqlFF += "AND masterID IS NULL "
            self.cur.execute(sqlFF + comment, varMap)
            (nFiles,) = self.cur.fetchone()
            # disallow some transition
            retVal = True
            if statusStr == "pending" and curStr == JediTaskSpec.enum_useJumbo["lack"]:
                # to prevent from changing lack to pending
                statusStr = "lack"
                tmpLog.debug(f"changed to {statusStr} since to pending is not allowed")
                retVal = False
            elif statusStr == "running" and curStr == JediTaskSpec.enum_useJumbo["pending"]:
                # to running from pending only when all files are used
                if nFiles != 0:
                    statusStr = "pending"
                    tmpLog.debug(f"changed to {statusStr} since nFiles={nFiles}")
                    retVal = False
            elif statusStr == "pending" and curStr == JediTaskSpec.enum_useJumbo["running"]:
                # to pending from running only when some files are available
                if nFiles == 0:
                    statusStr = "running"
                    tmpLog.debug(f"changed to {statusStr} since nFiles == 0")
                    retVal = False
            # set jumbo
            sqlDJ = f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks SET useJumbo=:status "
            sqlDJ += "WHERE jediTaskID=:jediTaskID "
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            varMap[":status"] = JediTaskSpec.enum_useJumbo[statusStr]
            self.cur.execute(sqlDJ + comment, varMap)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            tmpLog.debug(f"set {curStr} -> {varMap[':status']}")
            return retVal
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return False

    # get number of tasks with running jumbo jobs
    def getNumTasksWithRunningJumbo_JEDI(self, vo, prodSourceLabel, cloudName, workqueue):
        comment = " /* JediDBProxy.getNumTasksWithRunningJumbo_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"vo={vo} label={prodSourceLabel} cloud={cloudName} queue={workqueue.queue_name}")
        tmpLog.debug("start")
        try:
            # get tasks
            sqlDJ = f"SELECT task_count FROM {panda_config.schemaJEDI}.MV_RUNNING_JUMBO_TASK_COUNT "
            sqlDJ += "WHERE vo=:vo AND prodSourceLabel=:label AND cloud=:cloud "
            sqlDJ += "AND useJumbo in (:useJumbo1,:useJumbo2) AND status IN (:st1,:st2,:st3) "
            varMap = {}
            varMap[":vo"] = vo
            varMap[":label"] = prodSourceLabel
            varMap[":cloud"] = cloudName
            if workqueue.is_global_share:
                sqlDJ += "AND gshare =:gshare "
                sqlDJ += f"AND workqueue_id NOT IN (SELECT queue_id FROM {panda_config.schemaJEDI}.jedi_work_queue WHERE queue_function = 'Resource') "
                varMap[":gshare"] = workqueue.queue_name
            else:
                sqlDJ += "AND workQueue_ID =:queue_id "
                varMap[":queue_id"] = workqueue.queue_id
            varMap[":st1"] = "running"
            varMap[":st2"] = "pending"
            varMap[":st3"] = "ready"
            varMap[":useJumbo1"] = JediTaskSpec.enum_useJumbo["running"]
            varMap[":useJumbo2"] = JediTaskSpec.enum_useJumbo["pending"]
            # start transaction
            self.conn.begin()
            self.cur.execute(sqlDJ + comment, varMap)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            res = self.cur.fetchone()
            if res is None:
                nTasks = 0
            else:
                nTasks = res[0]
            # return
            tmpLog.debug(f"got {nTasks} tasks")
            return nTasks
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return 0

    # get number of unprocessed events
    def getNumUnprocessedEvents_JEDI(self, vo, prodSourceLabel, criteria, neg_criteria):
        comment = " /* JediDBProxy.getNumUnprocessedEvents_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"vo={vo} label={prodSourceLabel}")
        tmpLog.debug(f"start with criteria={str(criteria)} neg={str(neg_criteria)}")
        try:
            # get num events
            varMap = {}
            varMap[":vo"] = vo
            varMap[":label"] = prodSourceLabel
            varMap[":type"] = "input"
            sqlDJ = "SELECT SUM(nEvents),MAX(creationDate) FROM ("
            sqlDJ += "SELECT CASE tabD.nFiles WHEN 0 THEN 0 ELSE tabD.nEvents*(tabD.nFiles-tabD.nFilesUsed)/tabD.nFiles END nEvents,"
            sqlDJ += "tabT.creationDate creationDate "
            sqlDJ += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_Datasets tabD,{0}.JEDI_AUX_Status_MinTaskID tabA ".format(panda_config.schemaJEDI)
            sqlDJ += "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID "
            sqlDJ += "AND tabT.jediTaskID=tabD.jediTaskID "
            sqlDJ += "AND tabT.vo=:vo AND tabT.prodSourceLabel=:label "
            sqlDJ += "AND tabT.status IN (:st1,:st2,:st3,:st4,:st5,:st6,:st7) AND tabD.type=:type AND tabD.masterID IS NULL "
            for key, val in criteria.items():
                sqlDJ += "AND tabT.{0}=:{0} ".format(key)
                varMap[f":{key}"] = val
            for key, val in neg_criteria.items():
                sqlDJ += "AND tabT.{0}<>:neg_{0} ".format(key)
                varMap[f":neg_{key}"] = val
            sqlDJ += ") "
            varMap[":st1"] = "running"
            varMap[":st2"] = "pending"
            varMap[":st3"] = "ready"
            varMap[":st4"] = "scouting"
            varMap[":st5"] = "registered"
            varMap[":st6"] = "defined"
            varMap[":st7"] = "assigning"
            # sql to get pending tasks
            sqlPD = "SELECT COUNT(1) "
            sqlPD += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_AUX_Status_MinTaskID tabA ".format(panda_config.schemaJEDI)
            sqlPD += "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID "
            sqlPD += "AND tabT.vo=:vo AND tabT.prodSourceLabel=:label "
            sqlPD += "AND tabT.status IN (:st1,:st2) "
            for key, val in criteria.items():
                sqlPD += "AND tabT.{0}=:{0} ".format(key)
            # get num events
            self.conn.begin()
            self.cur.execute(sqlDJ + comment, varMap)
            nEvents, lastTaskTime = self.cur.fetchone()
            if nEvents is None:
                nEvents = 0
            # get num of pending tasks
            varMap = dict()
            varMap[":vo"] = vo
            varMap[":label"] = prodSourceLabel
            varMap[":st1"] = "pending"
            varMap[":st2"] = "registered"
            for key, val in criteria.items():
                varMap[f":{key}"] = val
            self.cur.execute(sqlPD + comment, varMap)
            (nPending,) = self.cur.fetchone()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            tmpLog.debug(f"got nEvents={nEvents} lastTaskTime={lastTaskTime} nPendingTasks={nPending}")
            return nEvents, lastTaskTime, nPending
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return None, None, None

    # get tasks with jumbo jobs
    def getTaskWithJumbo_JEDI(self, vo, prodSourceLabel):
        comment = " /* JediDBProxy.getTaskWithJumbo_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"vo={vo} label={prodSourceLabel}")
        tmpLog.debug("start")
        try:
            # sql to get tasks
            sqlAV = "SELECT t.jediTaskID,t.status,t.splitRule,t.useJumbo,d.nEvents,t.currentPriority,"
            sqlAV += "d.nFiles,d.nFilesFinished,d.nFilesFailed,t.site,d.nEventsUsed "
            sqlAV += "FROM {0}.JEDI_Tasks t,{0}.JEDI_Datasets d ".format(panda_config.schemaJEDI)
            sqlAV += "WHERE t.prodSourceLabel=:prodSourceLabel AND t.vo=:vo AND t.useJumbo IS NOT NULL "
            sqlAV += "AND t.status IN (:s1,:s2,:s3,:s4,:s5) "
            sqlAV += "AND d.jediTaskID=t.jediTaskID "
            sqlAV += f"AND d.type IN ({INPUT_TYPES_var_str}) "
            sqlAV += "AND d.masterID IS NULL "
            # sql to get event stat info
            sqlFR = "SELECT /*+ INDEX_RS_ASC(c (JEDI_DATASET_CONTENTS.JEDITASKID JEDI_DATASET_CONTENTS.DATASETID JEDI_DATASET_CONTENTS.FILEID)) NO_INDEX_FFS(tab JEDI_EVENTS_PK) NO_INDEX_SS(tab JEDI_EVENTS_PK) NO_INDEX(tab JEDI_EVENTS_PANDAID_STATUS_IDX)*/ "
            sqlFR += "tab.status,COUNT(*) "
            sqlFR += "FROM {0}.JEDI_Events tab,{0}.JEDI_Dataset_Contents c ".format(panda_config.schemaJEDI)
            sqlFR += "WHERE tab.jediTaskID=:jediTaskID AND c.jediTaskID=tab.jediTaskID AND c.datasetid=tab.datasetID "
            sqlFR += "AND c.fileID=tab.fileID AND c.status<>:status "
            sqlFR += "GROUP BY tab.status "
            # sql to get jumbo jobs
            sqlUO = f"SELECT computingSite,jobStatus FROM {panda_config.schemaPANDA}.jobsDefined4 "
            sqlUO += "WHERE jediTaskID=:jediTaskID AND eventService=:eventService "
            sqlUO += "UNION "
            sqlUO += f"SELECT computingSite,jobStatus FROM {panda_config.schemaPANDA}.jobsActive4 "
            sqlUO += "WHERE jediTaskID=:jediTaskID AND eventService=:eventService "
            sqlUO += "UNION "
            sqlUO += f"SELECT computingSite,jobStatus FROM {panda_config.schemaPANDA}.jobsArchived4 "
            sqlUO += "WHERE jediTaskID=:jediTaskID AND eventService=:eventService "
            sqlUO += "AND modificationTime>CURRENT_DATE-1 "
            self.conn.begin()
            # get tasks
            varMap = dict()
            varMap[":vo"] = vo
            varMap[":prodSourceLabel"] = prodSourceLabel
            varMap[":s1"] = "running"
            varMap[":s2"] = "pending"
            varMap[":s3"] = "scouting"
            varMap[":s4"] = "ready"
            varMap[":s5"] = "scouted"
            varMap.update(INPUT_TYPES_var_map)
            self.cur.execute(sqlAV + comment, varMap)
            resAV = self.cur.fetchall()
            tmpLog.debug("got tasks")
            tasksWithJumbo = dict()
            for jediTaskID, taskStatus, splitRule, useJumbo, nEvents, currentPriority, nFiles, nFilesFinished, nFilesFailed, taskSite, nEventsUsed in resAV:
                tasksWithJumbo[jediTaskID] = dict()
                taskData = tasksWithJumbo[jediTaskID]
                taskData["taskStatus"] = taskStatus
                taskData["nEvents"] = nEvents
                taskData["useJumbo"] = useJumbo
                taskData["currentPriority"] = currentPriority
                taskData["site"] = taskSite
                taskSpec = JediTaskSpec()
                taskSpec.useJumbo = useJumbo
                taskSpec.splitRule = splitRule
                taskData["nJumboJobs"] = taskSpec.getNumJumboJobs()
                taskData["maxJumboPerSite"] = taskSpec.getMaxJumboPerSite()
                taskData["nFiles"] = nFiles
                taskData["nFilesDone"] = nFilesFinished + nFilesFailed
                # get event stat info
                varMap = dict()
                varMap[":jediTaskID"] = jediTaskID
                varMap[":status"] = "finished"
                self.cur.execute(sqlFR + comment, varMap)
                resFR = self.cur.fetchall()
                tmpLog.debug(f"got event stat info for jediTaskID={jediTaskID}")
                nEventsDone = nEventsUsed
                nEventsRunning = 0
                for eventStatus, eventCount in resFR:
                    if eventStatus in [EventServiceUtils.ST_done, EventServiceUtils.ST_finished, EventServiceUtils.ST_merged]:
                        nEventsDone += eventCount
                    elif eventStatus in [EventServiceUtils.ST_sent, EventServiceUtils.ST_running]:
                        nEventsRunning += eventCount
                taskData["nEventsDone"] = nEventsDone
                taskData["nEventsRunning"] = nEventsRunning
                # get jumbo jobs
                varMap = dict()
                varMap[":jediTaskID"] = jediTaskID
                varMap[":eventService"] = EventServiceUtils.jumboJobFlagNumber
                self.cur.execute(sqlUO + comment, varMap)
                resUO = self.cur.fetchall()
                tmpLog.debug(f"got jumbo jobs for jediTaskID={jediTaskID}")
                taskData["jumboJobs"] = dict()
                for computingSite, jobStatus in resUO:
                    taskData["jumboJobs"].setdefault(computingSite, dict())
                    taskData["jumboJobs"][computingSite].setdefault(jobStatus, 0)
                    taskData["jumboJobs"][computingSite][jobStatus] += 1
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            tmpLog.debug(f"done with {str(tasksWithJumbo)}")
            return tasksWithJumbo
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return dict()

    # kick pending tasks with jumbo jobs
    def kickPendingTasksWithJumbo_JEDI(self, jediTaskID):
        comment = " /* JediDBProxy.kickPendingTasksWithJumbo_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID}")
        tmpLog.debug("start")
        try:
            # sql to kick
            sqlAV = f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks "
            sqlAV += "SET useJumbo=:useJumboL "
            sqlAV += "WHERE jediTaskID=:jediTaskID AND useJumbo IN (:useJumboP,:useJumboR) "
            sqlAV += "AND status IN (:statusR,:statusP) AND lockedBy IS NULL "
            self.conn.begin()
            # get tasks
            varMap = dict()
            varMap[":jediTaskID"] = jediTaskID
            varMap[":statusP"] = "pending"
            varMap[":statusR"] = "running"
            varMap[":useJumboL"] = JediTaskSpec.enum_useJumbo["lack"]
            varMap[":useJumboP"] = JediTaskSpec.enum_useJumbo["pending"]
            varMap[":useJumboR"] = JediTaskSpec.enum_useJumbo["running"]
            self.cur.execute(sqlAV + comment, varMap)
            nDone = self.cur.rowcount
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            tmpLog.debug(f"kicked with {nDone}")
            return nDone
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return None

    # reset input to re-generate co-jumbo jobs
    def resetInputToReGenCoJumbo_JEDI(self, jediTaskID):
        comment = " /* JediDBProxy.resetInputToReGenCoJumbo_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID}")
        tmpLog.debug("start")
        try:
            nReset = 0
            # sql to get JDI files
            sqlF = "SELECT c.datasetID,c.fileID FROM {0}.JEDI_Datasets d, {0}.JEDI_Dataset_Contents c ".format(panda_config.schemaJEDI)
            sqlF += "WHERE d.jediTaskID=:jediTaskID "
            sqlF += f"AND d.type IN ({INPUT_TYPES_var_str}) "
            sqlF += "AND d.masterID IS NULL "
            sqlF += "AND c.jediTaskID=d.jediTaskID AND c.datasetID=d.datasetID AND c.status=:status "
            # sql to get PandaIDs
            sqlP = f"SELECT PandaID FROM {panda_config.schemaPANDA}.filesTable4 "
            sqlP += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileid=:fileID "
            sqlP += "ORDER BY PandaID DESC "
            # sql to check jobs
            sqlJ = f"SELECT 1 FROM {panda_config.schemaPANDA}.jobsDefined4 WHERE PandaID=:PandaID "
            sqlJ += "UNION "
            sqlJ += f"SELECT 1 FROM {panda_config.schemaPANDA}.jobsActive4 WHERE PandaID=:PandaID "
            # sql to get files
            sqlFL = f"SELECT datasetID,fileID FROM {panda_config.schemaPANDA}.filesTable4 "
            sqlFL += "WHERE PandaID=:PandaID "
            sqlFL += f"AND type IN ({INPUT_TYPES_var_str}) "
            # sql to update files
            sqlUF = f"UPDATE {panda_config.schemaJEDI}.JEDI_Dataset_Contents "
            sqlUF += "SET status=:newStatus,proc_status=:proc_status,attemptNr=attemptNr+1,maxAttempt=maxAttempt+1,"
            sqlUF += "maxFailure=(CASE WHEN maxFailure IS NULL THEN NULL ELSE maxFailure+1 END) "
            sqlUF += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
            sqlUF += "AND status=:oldStatus AND keepTrack=:keepTrack "
            # sql to update datasets
            sqlUD = f"UPDATE {panda_config.schemaJEDI}.JEDI_Datasets "
            sqlUD += "SET nFilesUsed=nFilesUsed-1 WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            self.conn.begin()
            # get JEDI files
            varMap = dict()
            varMap[":jediTaskID"] = jediTaskID
            varMap[":status"] = "running"
            varMap.update(INPUT_TYPES_var_map)
            self.cur.execute(sqlF + comment, varMap)
            resF = self.cur.fetchall()
            # get PandaIDs
            for datasetID, fileID in resF:
                varMap = dict()
                varMap[":jediTaskID"] = jediTaskID
                varMap[":datasetID"] = datasetID
                varMap[":fileID"] = fileID
                self.cur.execute(sqlP + comment, varMap)
                resP = self.cur.fetchall()
                # check jobs
                hasJob = False
                for (PandaID,) in resP:
                    varMap = dict()
                    varMap[":PandaID"] = PandaID
                    self.cur.execute(sqlJ + comment, varMap)
                    resJ = self.cur.fetchone()
                    if resJ is not None:
                        hasJob = True
                        break
                # get files
                if not hasJob:
                    varMap = dict()
                    varMap[":PandaID"] = PandaID
                    varMap.update(INPUT_TYPES_var_map)
                    self.cur.execute(sqlFL + comment, varMap)
                    resFL = self.cur.fetchall()
                    # update file
                    for f_datasetID, f_fileID in resFL:
                        varMap = dict()
                        varMap[":jediTaskID"] = jediTaskID
                        varMap[":datasetID"] = f_datasetID
                        varMap[":fileID"] = f_fileID
                        varMap[":oldStatus"] = "running"
                        varMap[":newStatus"] = "ready"
                        varMap[":proc_status"] = "ready"
                        varMap[":keepTrack"] = 1
                        self.cur.execute(sqlUF + comment, varMap)
                        nRow = self.cur.rowcount
                        tmpLog.debug(f"reset datasetID={f_datasetID} fileID={f_fileID} with {nRow}")
                        if nRow > 0:
                            varMap = dict()
                            varMap[":jediTaskID"] = jediTaskID
                            varMap[":datasetID"] = f_datasetID
                            self.cur.execute(sqlUD + comment, varMap)
                            nReset += 1
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            tmpLog.debug(f"done with {nReset}")
            return nReset
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return None


# get task event module
def get_task_event_module(base_mod) -> TaskEventModule:
    return base_mod.get_composite_module("task_event")
