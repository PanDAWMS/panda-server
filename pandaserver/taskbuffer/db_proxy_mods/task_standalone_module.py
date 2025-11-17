import datetime
import math
import re
import socket
import sys

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandautils.PandaUtils import get_sql_IN_bind_variables, naive_utcnow

from pandaserver.config import panda_config
from pandaserver.taskbuffer import EventServiceUtils, JobUtils
from pandaserver.taskbuffer.db_proxy_mods.base_module import BaseModule, varNUMBER
from pandaserver.taskbuffer.JediCacheSpec import JediCacheSpec
from pandaserver.taskbuffer.JediDatasetSpec import (
    INPUT_TYPES_var_map,
    INPUT_TYPES_var_str,
    JediDatasetSpec,
    PROCESS_TYPES_var_map,
    PROCESS_TYPES_var_str,
)
from pandaserver.taskbuffer.JediFileSpec import JediFileSpec
from pandaserver.taskbuffer.JediTaskSpec import JediTaskSpec, is_msg_driven


# Module class to define isolated task related methods
class TaskStandaloneModule(BaseModule):
    # constructor
    def __init__(self, log_stream: LogWrapper):
        super().__init__(log_stream)

    # get files from the JEDI contents table with jediTaskID and/or datasetID
    def getFilesInDatasetWithID_JEDI(self, jediTaskID, datasetID, nFiles, status):
        comment = " /* JediDBProxy.getFilesInDataset_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID} datasetID={datasetID}")
        tmpLog.debug(f"start nFiles={nFiles} status={status}")
        # return value for failure
        failedRet = False, 0
        if jediTaskID is None and datasetID is None:
            tmpLog.error("either jediTaskID or datasetID is not defined")
            return failedRet
        try:
            # sql
            varMap = {}
            sql = f"SELECT * FROM (SELECT {JediFileSpec.columnNames()} "
            sql += f"FROM {panda_config.schemaJEDI}.JEDI_Dataset_Contents WHERE "
            useAND = False
            if jediTaskID is not None:
                sql += "jediTaskID=:jediTaskID "
                varMap[":jediTaskID"] = jediTaskID
                useAND = True
            if datasetID is not None:
                if useAND:
                    sql += "AND "
                sql += "datasetID=:datasetID "
                varMap[":datasetID"] = datasetID
                useAND = True
            if status is not None:
                if useAND:
                    sql += "AND "
                sql += "status=:status "
                varMap[":status"] = status
                useAND = True
            sql += " ORDER BY fileID) "
            if nFiles is not None:
                sql += f"WHERE rownum <= {nFiles}"
            # begin transaction
            self.conn.begin()
            self.cur.arraysize = 100000
            # get existing file list
            self.cur.execute(sql + comment, varMap)
            tmpResList = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # make file specs
            fileSpecList = []
            for tmpRes in tmpResList:
                fileSpec = JediFileSpec()
                fileSpec.pack(tmpRes)
                fileSpecList.append(fileSpec)
            tmpLog.debug(f"got {len(fileSpecList)} files")
            return True, fileSpecList
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return failedRet

    # insert task to the JEDI task table
    def insertTask_JEDI(self, taskSpec):
        comment = " /* JediDBProxy.insertTask_JEDI */"
        tmpLog = self.create_tagged_logger(comment)
        tmpLog.debug("start")
        try:
            # set attributes
            timeNow = naive_utcnow()
            taskSpec.creationDate = timeNow
            taskSpec.modificationTime = timeNow
            # sql
            sql = f"INSERT INTO {panda_config.schemaJEDI}.JEDI_Tasks ({JediTaskSpec.columnNames()}) "
            sql += JediTaskSpec.bindValuesExpression()
            varMap = taskSpec.valuesMap()
            # begin transaction
            self.conn.begin()
            # insert dataset
            self.cur.execute(sql + comment, varMap)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmpLog.debug("done")
            return True
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return False

    # update JEDI task lock
    def updateTaskLock_JEDI(self, jediTaskID):
        comment = " /* JediDBProxy.updateTaskLock_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID}")
        tmpLog.debug("start")
        # return value for failure
        failedRet = False
        try:
            # sql to update lock
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            sqlS = f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks "
            sqlS += "SET lockedTime=CURRENT_DATE "
            sqlS += "WHERE jediTaskID=:jediTaskID "
            # begin transaction
            self.conn.begin()
            # get old status
            self.cur.execute(sqlS + comment, varMap)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmpLog.debug("done")
            return True
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return failedRet

    # get JEDI task and datasets with ID and lock it
    def getTaskDatasetsWithID_JEDI(self, jediTaskID, pid, lockTask=True):
        comment = " /* JediDBProxy.getTaskDatasetsWithID_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID}")
        tmpLog.debug(f"start pid={pid}")
        # return value for failure
        failedRet = False, None
        try:
            # sql
            sql = f"SELECT {JediTaskSpec.columnNames()} "
            sql += f"FROM {panda_config.schemaJEDI}.JEDI_Tasks WHERE jediTaskID=:jediTaskID "
            if lockTask:
                sql += "AND lockedBy IS NULL FOR UPDATE NOWAIT"
            sqlLK = f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks SET lockedBy=:lockedBy,lockedTime=CURRENT_DATE "
            sqlLK += "WHERE jediTaskID=:jediTaskID "
            sqlDS = f"SELECT {JediDatasetSpec.columnNames()} "
            sqlDS += f"FROM {panda_config.schemaJEDI}.JEDI_Datasets WHERE jediTaskID=:jediTaskID "
            # begin transaction
            self.conn.begin()
            self.cur.arraysize = 10000
            # select
            res = None
            try:
                # read task
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                self.cur.execute(sql + comment, varMap)
                res = self.cur.fetchone()
                if res is None:
                    taskSpec = None
                else:
                    taskSpec = JediTaskSpec()
                    taskSpec.pack(res)
                    # lock task
                    if lockTask:
                        varMap = {}
                        varMap[":jediTaskID"] = jediTaskID
                        self.cur.execute(sqlLK + comment, varMap)
                    # read datasets
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    self.cur.execute(sqlDS + comment, varMap)
                    resList = self.cur.fetchall()
                    for res in resList:
                        datasetSpec = JediDatasetSpec()
                        datasetSpec.pack(res)
                        taskSpec.datasetSpecList.append(datasetSpec)
            except Exception:
                errType, errValue = sys.exc_info()[:2]
                if self.isNoWaitException(errValue):
                    # resource busy and acquire with NOWAIT specified
                    tmpLog.debug("skip locked")
                else:
                    # failed with something else
                    raise errType(errValue)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            if taskSpec is None:
                tmpLog.debug("done with None")
            else:
                tmpLog.debug("done with OK")
            return True, taskSpec
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return failedRet

    # get JEDI tasks with selection criteria
    def getTaskIDsWithCriteria_JEDI(self, criteria, nTasks=50):
        comment = " /* JediDBProxy.getTaskIDsWithCriteria_JEDI */"
        tmpLog = self.create_tagged_logger(comment)
        tmpLog.debug("start")
        # return value for failure
        failedRet = None
        # no criteria
        if criteria == {}:
            tmpLog.error("no selection criteria")
            return failedRet
        # check criteria
        for tmpKey in criteria.keys():
            if tmpKey not in JediTaskSpec.attributes:
                tmpLog.error(f"unknown attribute {tmpKey} is used in criteria")
                return failedRet
        varMap = {}
        try:
            # sql
            sql = "SELECT jediTaskID "
            sql += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_AUX_Status_MinTaskID tabA ".format(panda_config.schemaJEDI)
            sql += "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID "
            isFirst = True
            for tmpKey, tmpVal in criteria.items():
                if not isFirst:
                    sql += "AND "
                else:
                    isFirst = False
                if tmpVal in ["NULL", "NOT NULL"]:
                    sql += f"{tmpKey} IS {tmpVal} "
                elif tmpVal is None:
                    sql += f"{tmpKey} IS NULL "
                else:
                    crKey = f":cr_{tmpKey}"
                    sql += f"{tmpKey}={crKey} "
                    varMap[crKey] = tmpVal
            sql += f"AND rownum<={nTasks}"
            # begin transaction
            self.conn.begin()
            # select
            self.cur.arraysize = 10000
            tmpLog.debug(sql + comment + str(varMap))
            self.cur.execute(sql + comment, varMap)
            resList = self.cur.fetchall()
            # collect jediTaskIDs
            retTaskIDs = []
            for (jediTaskID,) in resList:
                retTaskIDs.append(jediTaskID)
            retTaskIDs.sort()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmpLog.debug(f"got {len(retTaskIDs)} tasks")
            return retTaskIDs
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return failedRet

    # insert output file templates
    def insertOutputTemplate_JEDI(self, templates):
        comment = " /* JediDBProxy.insertOutputTemplate_JEDI */"
        tmpLog = self.create_tagged_logger(comment)
        tmpLog.debug("start")
        try:
            # begin transaction
            self.conn.begin()
            # loop over all templates
            for template in templates:
                # make sql
                varMap = {}
                sqlH = f"INSERT INTO {panda_config.schemaJEDI}.JEDI_Output_Template (outTempID,"
                sqlL = f"VALUES({panda_config.schemaJEDI}.JEDI_OUTPUT_TEMPLATE_ID_SEQ.nextval,"
                for tmpAttr, tmpVal in template.items():
                    tmpKey = ":" + tmpAttr
                    sqlH += f"{tmpAttr},"
                    sqlL += f"{tmpKey},"
                    varMap[tmpKey] = tmpVal
                sqlH = sqlH[:-1] + ") "
                sqlL = sqlL[:-1] + ") "
                sql = sqlH + sqlL
                self.cur.execute(sql + comment, varMap)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmpLog.debug("done")
            return True
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return False

    # insert JobParamsTemplate
    def insertJobParamsTemplate_JEDI(self, jediTaskID, templ):
        comment = " /* JediDBProxy.insertJobParamsTemplate_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID}")
        tmpLog.debug("start")
        try:
            # SQL
            sql = f"INSERT INTO {panda_config.schemaJEDI}.JEDI_JobParams_Template (jediTaskID,jobParamsTemplate) VALUES (:jediTaskID,:templ) "
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            varMap[":templ"] = templ
            # begin transaction
            self.conn.begin()
            # insert
            self.cur.execute(sql + comment, varMap)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmpLog.debug("done")
            return True
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return False

    # insert TaskParams
    def insertTaskParams_JEDI(self, vo, prodSourceLabel, userName, taskName, taskParams, parent_tid=None):
        comment = " /* JediDBProxy.insertTaskParams_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"userName={userName} taskName={taskName}")
        tmpLog.debug("start")
        try:
            # sql to insert task parameters
            sqlT = f"INSERT INTO {panda_config.schemaDEFT}.T_TASK "
            sqlT += "(taskid,status,submit_time,vo,prodSourceLabel,userName,taskName,jedi_task_parameters,parent_tid) VALUES "
            sqlT += f"({panda_config.schemaDEFT}.PRODSYS2_TASK_ID_SEQ.nextval,"
            sqlT += ":status,CURRENT_DATE,:vo,:prodSourceLabel,:userName,:taskName,:param,"
            if parent_tid is None:
                sqlT += f"{panda_config.schemaDEFT}.PRODSYS2_TASK_ID_SEQ.currval) "
            else:
                sqlT += ":parent_tid) "
            sqlT += "RETURNING taskid INTO :jediTaskID"
            # begin transaction
            self.conn.begin()
            # insert task parameters
            varMap = {}
            varMap[":vo"] = vo
            varMap[":param"] = taskParams
            varMap[":status"] = "waiting"
            varMap[":userName"] = userName
            varMap[":taskName"] = taskName
            if parent_tid is not None:
                varMap[":parent_tid"] = parent_tid
            varMap[":prodSourceLabel"] = prodSourceLabel
            varMap[":jediTaskID"] = self.cur.var(varNUMBER)
            self.cur.execute(sqlT + comment, varMap)
            val = self.getvalue_corrector(self.cur.getvalue(varMap[":jediTaskID"]))
            jediTaskID = int(val)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")

            tmpLog.debug(f"done new jediTaskID={jediTaskID}")
            return True, jediTaskID
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return False, None

    # insert new TaskParams and update parent TaskParams. mainly used by TaskGenerator
    def insertUpdateTaskParams_JEDI(self, jediTaskID, vo, prodSourceLabel, updateTaskParams, insertTaskParamsList):
        comment = " /* JediDBProxy.insertUpdateTaskParams_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID}")
        tmpLog.debug("start")
        try:
            # sql to insert new task parameters
            sqlIT = f"INSERT INTO {panda_config.schemaDEFT}.T_TASK "
            sqlIT += "(taskid,status,submit_time,vo,prodSourceLabel,jedi_task_parameters,parent_tid) VALUES "
            sqlIT += f"({panda_config.schemaDEFT}.PRODSYS2_TASK_ID_SEQ.nextval,"
            sqlIT += ":status,CURRENT_DATE,:vo,:prodSourceLabel,:param,:parent_tid) "
            sqlIT += "RETURNING taskid INTO :jediTaskID"
            # sql to update parent task parameters
            sqlUT = f"UPDATE {panda_config.schemaJEDI}.JEDI_TaskParams SET taskParams=:taskParams "
            sqlUT += "WHERE jediTaskID=:jediTaskID "
            # begin transaction
            self.conn.begin()
            # insert task parameters
            newJediTaskIDs = []
            for taskParams in insertTaskParamsList:
                varMap = {}
                varMap[":vo"] = vo
                varMap[":param"] = taskParams
                varMap[":status"] = "waiting"
                varMap[":parent_tid"] = jediTaskID
                varMap[":prodSourceLabel"] = prodSourceLabel
                varMap[":jediTaskID"] = self.cur.var(varNUMBER)
                self.cur.execute(sqlIT + comment, varMap)
                val = self.getvalue_corrector(self.cur.getvalue(varMap[":jediTaskID"]))
                newJediTaskID = int(val)
                newJediTaskIDs.append(newJediTaskID)
            # update task parameters
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            varMap[":taskParams"] = updateTaskParams
            self.cur.execute(sqlUT + comment, varMap)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmpLog.debug(f"done new jediTaskIDs={str(newJediTaskIDs)}")
            return True, newJediTaskIDs
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return False, None

    # reset unused files
    def resetUnusedFiles_JEDI(self, jediTaskID, inputChunk):
        comment = " /* JediDBProxy.resetUnusedFiles_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID}")
        tmpLog.debug("start")
        try:
            nFileRowMaster = 0
            # sql to rollback files
            sql = f"UPDATE {panda_config.schemaJEDI}.JEDI_Dataset_Contents SET status=:nStatus "
            sql += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND status=:oStatus "
            if inputChunk.ramCount in (None, 0):
                sql += "AND (ramCount IS NULL OR ramCount=:ramCount) "
            else:
                sql += "AND ramCount=:ramCount "
            # sql to reset nFilesUsed
            sqlD = f"UPDATE {panda_config.schemaJEDI}.JEDI_Datasets SET nFilesUsed=nFilesUsed-:nFileRow "
            sqlD += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            # begin transaction
            self.conn.begin()
            for datasetSpec in inputChunk.getDatasets(includePseudo=True):
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                varMap[":datasetID"] = datasetSpec.datasetID
                varMap[":nStatus"] = "ready"
                varMap[":oStatus"] = "picked"
                varMap[":ramCount"] = inputChunk.ramCount
                # update contents
                self.cur.execute(sql + comment, varMap)
                nFileRow = self.cur.rowcount
                tmpLog.debug(f"reset {nFileRow} rows for datasetID={datasetSpec.datasetID}")
                if nFileRow > 0:
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    varMap[":datasetID"] = datasetSpec.datasetID
                    varMap[":nFileRow"] = nFileRow
                    # update dataset
                    self.cur.execute(sqlD + comment, varMap)
                    if datasetSpec.isMaster():
                        nFileRowMaster = nFileRow
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmpLog.debug("done")
            return nFileRowMaster
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return 0

    # set missing files
    def setMissingFiles_JEDI(self, jediTaskID, datasetID, fileIDs):
        comment = " /* JediDBProxy.setMissingFiles_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID} datasetID={datasetID}")
        tmpLog.debug("start")
        try:
            # sql to set missing files
            sqlF = f"UPDATE {panda_config.schemaJEDI}.JEDI_Dataset_Contents SET status=:nStatus "
            sqlF += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID and status<>:nStatus"
            # sql to set nFilesFailed
            sqlD = f"UPDATE {panda_config.schemaJEDI}.JEDI_Datasets SET nFilesFailed=nFilesFailed+:nFileRow "
            sqlD += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            # begin transaction
            self.conn.begin()
            nFileRow = 0
            # update contents
            for fileID in fileIDs:
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                varMap[":datasetID"] = datasetID
                varMap[":fileID"] = fileID
                varMap[":nStatus"] = "missing"
                self.cur.execute(sqlF + comment, varMap)
                nRow = self.cur.rowcount
                nFileRow += nRow
            # update dataset
            if nFileRow > 0:
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                varMap[":datasetID"] = datasetID
                varMap[":nFileRow"] = nFileRow
                self.cur.execute(sqlD + comment, varMap)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmpLog.debug(f"done set {nFileRow} missing files")
            return True
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return False

    # rescue picked files
    def rescuePickedFiles_JEDI(self, vo, prodSourceLabel, waitTime):
        comment = " /* JediDBProxy.rescuePickedFiles_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"vo={vo} label={prodSourceLabel}")
        tmpLog.debug("start")
        try:
            # sql to get orphaned tasks
            sqlTR = "SELECT jediTaskID,lockedBy "
            sqlTR += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_AUX_Status_MinTaskID tabA ".format(panda_config.schemaJEDI)
            sqlTR += "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID "
            sqlTR += "AND tabT.status IN (:status1,:status2,:status3,:status4) AND lockedBy IS NOT NULL AND lockedTime<:timeLimit "
            if vo not in [None, "any"]:
                sqlTR += "AND vo=:vo "
            if prodSourceLabel not in [None, "any"]:
                sqlTR += "AND prodSourceLabel=:prodSourceLabel "
            # sql to get picked datasets
            sqlDP = f"SELECT datasetID FROM {panda_config.schemaJEDI}.JEDI_Datasets "
            sqlDP += "WHERE jediTaskID=:jediTaskID AND type IN (:type1,:type2,:type3,:type4,:type5) "
            # sql to rollback files
            sqlF = f"UPDATE {panda_config.schemaJEDI}.JEDI_Dataset_Contents SET status=:nStatus "
            sqlF += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND status=:oStatus AND keepTrack=:keepTrack "
            # sql to reset nFilesUsed
            sqlDU = f"UPDATE {panda_config.schemaJEDI}.JEDI_Datasets SET nFilesUsed=nFilesUsed-:nFileRow "
            sqlDU += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            # sql to unlock task
            sqlTU = f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks SET lockedBy=NULL,lockedTime=NULL "
            sqlTU += "WHERE jediTaskID=:jediTaskID "
            # sql to re-lock task
            sqlRL = f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks SET lockedTime=CURRENT_DATE "
            sqlRL += "WHERE jediTaskID=:jediTaskID AND lockedBy=:lockedBy AND lockedTime<:timeLimit "
            # sql to re-lock task with nowait
            sqlNW = f"SELECT jediTaskID FROM {panda_config.schemaJEDI}.JEDI_Tasks "
            sqlNW += "WHERE jediTaskID=:jediTaskID AND lockedBy=:lockedBy AND lockedTime<:timeLimit "
            sqlNW += "FOR UPDATE NOWAIT "
            # begin transaction
            self.conn.begin()
            self.cur.arraysize = 10000
            timeLimit = naive_utcnow() - datetime.timedelta(minutes=waitTime)
            # get orphaned tasks
            varMap = {}
            varMap[":status1"] = "ready"
            varMap[":status2"] = "scouting"
            varMap[":status3"] = "running"
            varMap[":status4"] = "merging"
            varMap[":timeLimit"] = timeLimit
            if vo not in [None, "any"]:
                varMap[":vo"] = vo
            if prodSourceLabel not in [None, "any"]:
                varMap[":prodSourceLabel"] = prodSourceLabel
            self.cur.execute(sqlTR + comment, varMap)
            resTaskList = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # loop over all tasks
            nTasks = 0
            for jediTaskID, lockedBy in resTaskList:
                tmpLog.debug(f"[jediTaskID={jediTaskID}] rescue")
                self.conn.begin()
                # re-lock the task with NOWAIT
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                varMap[":lockedBy"] = lockedBy
                varMap[":timeLimit"] = timeLimit
                toSkip = False
                try:
                    self.cur.execute(sqlNW + comment, varMap)
                except Exception:
                    errType, errValue = sys.exc_info()[:2]
                    if self.isNoWaitException(errValue):
                        tmpLog.debug(f"[jediTaskID={jediTaskID}] skip to rescue since locked by another")
                        toSkip = True
                    else:
                        # failed with something else
                        raise errType(errValue)
                if not toSkip:
                    # re-lock the task
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    varMap[":lockedBy"] = lockedBy
                    varMap[":timeLimit"] = timeLimit
                    self.cur.execute(sqlRL + comment, varMap)
                    nRow = self.cur.rowcount
                    if nRow == 0:
                        tmpLog.debug(f"[jediTaskID={jediTaskID}] skip to rescue since failed to re-lock")
                    else:
                        # get input datasets
                        varMap = {}
                        varMap[":jediTaskID"] = jediTaskID
                        varMap[":type1"] = "input"
                        varMap[":type2"] = "trn_log"
                        varMap[":type3"] = "trn_output"
                        varMap[":type4"] = "pseudo_input"
                        varMap[":type5"] = "random_seed"
                        self.cur.execute(sqlDP + comment, varMap)
                        resDatasetList = self.cur.fetchall()
                        # loop over all input datasets
                        for (datasetID,) in resDatasetList:
                            # update contents
                            varMap = {}
                            varMap[":jediTaskID"] = jediTaskID
                            varMap[":datasetID"] = datasetID
                            varMap[":nStatus"] = "ready"
                            varMap[":oStatus"] = "picked"
                            varMap[":keepTrack"] = 1
                            self.cur.execute(sqlF + comment, varMap)
                            nFileRow = self.cur.rowcount
                            tmpLog.debug(f"[jediTaskID={jediTaskID}] reset {nFileRow} rows for datasetID={datasetID}")
                            if nFileRow > 0:
                                # reset nFilesUsed
                                varMap = {}
                                varMap[":jediTaskID"] = jediTaskID
                                varMap[":datasetID"] = datasetID
                                varMap[":nFileRow"] = nFileRow
                                self.cur.execute(sqlDU + comment, varMap)
                        # unlock task
                        tmpLog.debug(f"[jediTaskID={jediTaskID}] unlock")
                        varMap = {}
                        varMap[":jediTaskID"] = jediTaskID
                        self.cur.execute(sqlTU + comment, varMap)
                        nRows = self.cur.rowcount
                        tmpLog.debug(f"[jediTaskID={jediTaskID}] done with nRows={nRows}")
                        if nRows == 1:
                            nTasks += 1
                # commit
                if not self._commit():
                    raise RuntimeError("Commit error")
            tmpLog.debug("done")
            return nTasks
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return None

    # rescue unlocked tasks with picked files
    def rescueUnLockedTasksWithPicked_JEDI(self, vo, prodSourceLabel, waitTime, pid):
        comment = " /* JediDBProxy.rescueUnLockedTasksWithPicked_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"vo={vo} label={prodSourceLabel}")
        tmpLog.debug("start")
        try:
            timeToCheck = naive_utcnow() - datetime.timedelta(minutes=waitTime)
            varMap = {}
            varMap[":taskstatus1"] = "running"
            varMap[":taskstatus2"] = "scouting"
            varMap[":taskstatus3"] = "ready"
            varMap[":prodSourceLabel"] = prodSourceLabel
            varMap[":timeLimit"] = timeToCheck
            # sql to get tasks and datasetsto be checked
            sqlRL = "SELECT tabT.jediTaskID,tabD.datasetID "
            sqlRL += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_AUX_Status_MinTaskID tabA,{0}.JEDI_Datasets tabD ".format(panda_config.schemaJEDI)
            sqlRL += "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID "
            sqlRL += "AND tabT.jediTaskID=tabD.jediTaskID "
            sqlRL += "AND tabT.status IN (:taskstatus1,:taskstatus2,:taskstatus3) AND prodSourceLabel=:prodSourceLabel "
            sqlRL += "AND tabT.lockedBy IS NULL AND tabT.lockedTime IS NULL "
            sqlRL += "AND tabT.modificationTime<:timeLimit "
            sqlRL += "AND (tabT.rescueTime IS NULL OR tabT.rescueTime<:timeLimit) "
            if vo is not None:
                sqlRL += "AND tabT.vo=:vo "
                varMap[":vo"] = vo
            sqlRL += "AND tabT.lockedBy IS NULL "
            sqlRL += "AND tabD.masterID IS NULL AND tabD.nFilesTobeUsed=tabD.nFilesUsed "
            sqlRL += "AND tabD.nFilesTobeUsed>0 AND tabD.nFilesTobeUsed>(tabD.nFilesFinished+tabD.nFilesFailed) "
            sqlRL += f"AND tabD.type IN ({PROCESS_TYPES_var_str}) "
            varMap.update(PROCESS_TYPES_var_map)
            # sql to check if there is picked file
            sqlDP = f"SELECT * FROM {panda_config.schemaJEDI}.JEDI_Dataset_Contents "
            sqlDP += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND status=:fileStatus AND rownum<2 "
            # sql to set dummy lock to task
            sqlTU = f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks "
            sqlTU += "SET lockedBy=:lockedBy,lockedTime=:lockedTime,rescueTime=CURRENT_DATE "
            sqlTU += "WHERE jediTaskID=:jediTaskID AND lockedBy IS NULL AND lockedTime IS NULL "
            sqlTU += "AND modificationTime<:timeLimit "
            # sql to lock task with nowait
            sqlNW = f"SELECT jediTaskID FROM {panda_config.schemaJEDI}.JEDI_Tasks "
            sqlNW += "WHERE jediTaskID=:jediTaskID AND lockedBy IS NULL AND lockedTime IS NULL "
            sqlNW += "AND (rescueTime IS NULL OR rescueTime<:timeLimit) "
            sqlNW += "FOR UPDATE NOWAIT "
            # begin transaction
            self.conn.begin()
            self.cur.arraysize = 10000
            # get tasks
            self.cur.execute(sqlRL + comment, varMap)
            resTaskList = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            taskDsMap = dict()
            for jediTaskID, datasetID in resTaskList:
                if jediTaskID not in taskDsMap:
                    taskDsMap[jediTaskID] = []
                taskDsMap[jediTaskID].append(datasetID)
            tmpLog.debug(f"got {len(taskDsMap)} tasks")
            # loop over all tasks
            ngTasks = set()
            for jediTaskID, datasetIDs in taskDsMap.items():
                self.conn.begin()
                # lock task
                toSkip = False
                try:
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    varMap[":timeLimit"] = timeToCheck
                    self.cur.execute(sqlNW + comment, varMap)
                    resNW = self.cur.fetchone()
                    if resNW is None:
                        tmpLog.debug(f"[jediTaskID={jediTaskID} datasetID={datasetID}] skip since checked by another")
                        toSkip = True
                except Exception:
                    errType, errValue = sys.exc_info()[:2]
                    if self.isNoWaitException(errValue):
                        tmpLog.debug(f"[jediTaskID={jediTaskID} datasetID={datasetID}] skip since locked by another")
                        toSkip = True
                    else:
                        # failed with something else
                        raise errType(errValue)
                if not toSkip:
                    # loop over all datasets
                    allOK = True
                    for datasetID in datasetIDs:
                        tmpLog.debug(f"[jediTaskID={jediTaskID} datasetID={datasetID}] to check")
                        # check if there is picked file
                        varMap = {}
                        varMap[":jediTaskID"] = jediTaskID
                        varMap[":datasetID"] = datasetID
                        varMap[":fileStatus"] = "picked"
                        self.cur.execute(sqlDP + comment, varMap)
                        resDP = self.cur.fetchone()
                        varMap = {}
                        varMap[":jediTaskID"] = jediTaskID
                        varMap[":timeLimit"] = timeToCheck
                        if resDP is not None:
                            allOK = False
                            break
                    # set lock
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    varMap[":timeLimit"] = timeToCheck
                    if allOK:
                        # OK
                        varMap[":lockedBy"] = None
                        varMap[":lockedTime"] = None
                    else:
                        varMap[":lockedBy"] = pid
                        varMap[":lockedTime"] = naive_utcnow() - datetime.timedelta(hours=24)
                        tmpLog.debug(f"[jediTaskID={jediTaskID} datasetID={datasetID}] set dummy lock to trigger rescue")
                        ngTasks.add(jediTaskID)
                    self.cur.execute(sqlTU + comment, varMap)
                    nRow = self.cur.rowcount
                    tmpLog.debug(f"[jediTaskID={jediTaskID}] done with {nRow}")
                # commit
                if not self._commit():
                    raise RuntimeError("Commit error")
            nTasks = len(ngTasks)
            tmpLog.debug(f"done {nTasks} stuck tasks")
            return nTasks
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return None

    # unlock tasks
    def unlockTasks_JEDI(self, vo, prodSourceLabel, waitTime, hostName, pgid):
        comment = " /* JediDBProxy.unlockTasks_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"vo={vo} label={prodSourceLabel} host={hostName} pgid={pgid}")
        tmpLog.debug("start")
        try:
            # sql to look for locked tasks
            sqlTR = f"SELECT jediTaskID,lockedBy,lockedTime FROM {panda_config.schemaJEDI}.JEDI_Tasks "
            sqlTR += "WHERE lockedBy IS NOT NULL AND lockedTime<:timeLimit "
            if vo not in [None, "", "any"]:
                sqlTR += "AND vo=:vo "
            if prodSourceLabel not in [None, "", "any"]:
                sqlTR += "AND prodSourceLabel=:prodSourceLabel "
            if hostName is not None:
                sqlTR += "AND lockedBy LIKE :patt "
            # sql to unlock
            sqlTU = f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks "
            sqlTU += "SET lockedBy=NULL,lockedTime=NULL "
            sqlTU += "WHERE jediTaskID=:jediTaskID AND lockedBy=:lockedBy AND lockedTime<:timeLimit "
            timeNow = naive_utcnow()
            # sql to get picked datasets
            sqlDP = f"SELECT datasetID FROM {panda_config.schemaJEDI}.JEDI_Datasets "
            sqlDP += "WHERE jediTaskID=:jediTaskID AND type IN (:type1,:type2,:type3,:type4,:type5) "
            # sql to rollback files
            sqlF = f"UPDATE {panda_config.schemaJEDI}.JEDI_Dataset_Contents SET status=:nStatus "
            sqlF += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND status=:oStatus AND keepTrack=:keepTrack "
            # sql to reset nFilesUsed
            sqlDU = f"UPDATE {panda_config.schemaJEDI}.JEDI_Datasets SET nFilesUsed=nFilesUsed-:nFileRow "
            sqlDU += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            # begin transaction
            self.conn.begin()
            self.cur.arraysize = 1000
            # get locked task list
            timeLimit = timeNow - datetime.timedelta(minutes=waitTime)
            varMap = {}
            varMap[":timeLimit"] = timeLimit
            if vo not in [None, "", "any"]:
                varMap[":vo"] = vo
            if prodSourceLabel not in [None, "", "any"]:
                varMap[":prodSourceLabel"] = prodSourceLabel
            if hostName is not None:
                varMap[":patt"] = f"{hostName}-%"
            self.cur.execute(sqlTR + comment, varMap)
            taskList = self.cur.fetchall()
            # unlock tasks
            nTasks = 0
            for jediTaskID, lockedBy, lockedTime in taskList:
                # extract PID
                if hostName is not None:
                    # hostname mismatch
                    if not lockedBy.startswith(hostName):
                        continue
                    tmpMatch = re.search(f"^{hostName}-\\d+_(\\d+)-", lockedBy)
                    # no PGID
                    if tmpMatch is None:
                        continue
                    tmpPGID = int(tmpMatch.group(1))
                    # active process
                    if tmpPGID == pgid:
                        continue
                varMap = {}
                varMap[":lockedBy"] = lockedBy
                varMap[":timeLimit"] = timeLimit
                varMap[":jediTaskID"] = jediTaskID
                self.cur.execute(sqlTU + comment, varMap)
                iTasks = self.cur.rowcount
                if iTasks == 1:
                    tmpLog.debug(f"unlocked jediTaskID={jediTaskID} lockedBy={lockedBy} lockedTime={lockedTime}")
                    # get input datasets
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    varMap[":type1"] = "input"
                    varMap[":type2"] = "trn_log"
                    varMap[":type3"] = "trn_output"
                    varMap[":type4"] = "pseudo_input"
                    varMap[":type5"] = "random_seed"
                    self.cur.execute(sqlDP + comment, varMap)
                    resDatasetList = self.cur.fetchall()
                    # loop over all input datasets
                    for (datasetID,) in resDatasetList:
                        # update contents
                        varMap = {}
                        varMap[":jediTaskID"] = jediTaskID
                        varMap[":datasetID"] = datasetID
                        varMap[":nStatus"] = "ready"
                        varMap[":oStatus"] = "picked"
                        varMap[":keepTrack"] = 1
                        self.cur.execute(sqlF + comment, varMap)
                        nFileRow = self.cur.rowcount
                        tmpLog.debug(f"unlocked jediTaskID={jediTaskID} released {nFileRow} rows for datasetID={datasetID}")
                        if nFileRow > 0:
                            # reset nFilesUsed
                            varMap = {}
                            varMap[":jediTaskID"] = jediTaskID
                            varMap[":datasetID"] = datasetID
                            varMap[":nFileRow"] = nFileRow
                            self.cur.execute(sqlDU + comment, varMap)
                nTasks += iTasks
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmpLog.debug(f"done with {nTasks} tasks")
            return nTasks
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return None

    # get the size of input files which will be copied to the site
    def getMovingInputSize_JEDI(self, siteName):
        comment = " /* JediDBProxy.getMovingInputSize_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"site={siteName}")
        tmpLog.debug("start")
        try:
            # sql to get size
            sql = f"SELECT SUM(inputFileBytes)/1024/1024/1024 FROM {panda_config.schemaPANDA}.jobsDefined4 "
            sql += "WHERE computingSite=:computingSite "
            # begin transaction
            self.conn.begin()
            varMap = {}
            varMap[":computingSite"] = siteName
            # exec
            self.cur.execute(sql + comment, varMap)
            resSum = self.cur.fetchone()
            retVal = 0
            if resSum is not None:
                (retVal,) = resSum
            if retVal is None:
                retVal = 0
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmpLog.debug("done")
            return retVal
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return None

    # get typical number of input files for each gshare+processingType
    def getTypicalNumInput_JEDI(self, vo, prodSourceLabel, workQueue):
        comment = " /* JediDBProxy.getTypicalNumInput_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"vo={vo} label={prodSourceLabel} queue={workQueue.queue_name}")
        tmpLog.debug("start")

        try:
            # sql to get size
            var_map = {}
            var_map[":vo"] = vo
            var_map[":prodSourceLabel"] = prodSourceLabel
            sql = f"SELECT processingtype, nInputDataFiles FROM {panda_config.schemaPANDA}.typical_num_input "
            sql += "WHERE vo=:vo AND agg_type=:agg_type AND agg_key=:agg_key AND prodSourceLabel=:prodSourceLabel "

            if workQueue.is_global_share:
                var_map[":agg_type"] = "gshare"
                var_map[":agg_key"] = workQueue.queue_name
            else:
                var_map[":agg_type"] = "workqueue"
                var_map[":agg_key"] = str(workQueue.queue_id)

            # sql to get config
            sqlC = "SELECT key,value FROM ATLAS_PANDA.CONFIG "
            sqlC += "WHERE app=:app AND component=:component AND vo=:vo AND key LIKE :patt "

            # begin transaction
            self.conn.begin()

            # get values from cache
            self.cur.execute(sql + comment, var_map)
            resList = self.cur.fetchall()
            retMap = {}
            for processingType, numFile in resList:
                if numFile is None:
                    numFile = 0
                retMap[processingType] = int(math.ceil(numFile))

            # get from DB config
            var_map = {}
            var_map[":vo"] = vo
            var_map[":app"] = "jedi"
            var_map[":component"] = "jobgen"
            var_map[":patt"] = f"TYPNFILES_{prodSourceLabel}_%"
            self.cur.execute(sqlC + comment, var_map)
            resC = self.cur.fetchall()
            for tmpKey, tmpVal in resC:
                tmpItems = tmpKey.split("_")
                if len(tmpItems) != 4:
                    continue
                confWorkQueue = tmpItems[2]
                confProcessingType = tmpItems[3]
                if confWorkQueue != "" and confWorkQueue != workQueue.queue_name:
                    continue
                retMap[confProcessingType] = int(tmpVal)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")

            # use predefined values from config file
            tmpLog.debug(hasattr(self.jedi_config.jobgen, "typicalNumFile"))
            try:
                if hasattr(self.jedi_config.jobgen, "typicalNumFile"):
                    for tmpItem in self.jedi_config.jobgen.typicalNumFile.split(","):
                        confVo, confProdSourceLabel, confWorkQueue, confProcessingType, confNumFiles = tmpItem.split(":")
                        if vo != confVo and confVo not in [None, "", "any"]:
                            continue
                        if prodSourceLabel != confProdSourceLabel and confProdSourceLabel not in [None, "", "any"]:
                            continue
                        if workQueue != confWorkQueue and confWorkQueue not in [None, "", "any"]:
                            continue
                        retMap[confProcessingType] = int(confNumFiles)
            except Exception:
                pass
            tmpLog.debug(f"done -> {retMap}")

            return retMap
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return None

    # get highest prio jobs with workQueueID
    def getHighestPrioJobStat_JEDI(self, prodSourceLabel, cloudName, workQueue, resource_name=None):
        comment = " /* JediDBProxy.getHighestPrioJobStat_JEDI */"
        tmp_log = self.create_tagged_logger(comment, f"cloud={cloudName} queue={workQueue.queue_name} resource_type={resource_name}")
        tmp_log.debug("start")
        var_map = {}
        var_map[":cloud"] = cloudName
        var_map[":prodSourceLabel"] = prodSourceLabel

        sql_sum = f"SELECT MAX_PRIORITY, SUM(MAX_PRIORITY_COUNT) FROM {panda_config.schemaPANDA}.JOB_STATS_HP "
        sql_max = f"SELECT MAX(MAX_PRIORITY) FROM {panda_config.schemaPANDA}.JOB_STATS_HP "

        sql_where = "WHERE prodSourceLabel=:prodSourceLabel AND cloud=:cloud "

        if resource_name:
            sql_where += "AND resource_type=:resource_type "
            var_map[":resource_type"] = resource_name

        if workQueue.is_global_share:
            sql_where += "AND gshare=:wq_name "
            sql_where += "AND workqueue_id IN ("
            sql_where += f"SELECT UNIQUE workqueue_id FROM {panda_config.schemaPANDA}.JOB_STATS_HP "
            sql_where += "MINUS "
            sql_where += f"SELECT queue_id FROM {panda_config.schemaPANDA}.jedi_work_queue WHERE queue_function = 'Resource') "
            var_map[":wq_name"] = workQueue.queue_name
        else:
            sql_where += "AND workQueue_ID=:wq_id "
            var_map[":wq_id"] = workQueue.queue_id

        sql_max += sql_where
        sql_where += f"AND MAX_PRIORITY=({sql_max}) "
        sql_where += "GROUP BY MAX_PRIORITY"
        sql_sum += sql_where

        # make return map
        max_priority_tag = "highestPrio"
        max_priority_count_tag = "nNotRun"
        ret_map = {max_priority_tag: 0, max_priority_count_tag: 0}

        try:
            # start transaction
            self.conn.begin()
            self.cur.arraysize = 100

            tmp_log.debug((sql_sum + comment) + str(var_map))
            self.cur.execute((sql_sum + comment), var_map)
            res = self.cur.fetchone()
            if res:
                max_priority, count = res
                if max_priority and count:  # otherwise leave it to 0
                    ret_map[max_priority_tag] = max_priority
                    ret_map[max_priority_count_tag] = count

            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            tmp_log.debug(str(ret_map))
            return True, ret_map
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return False, None

    # get the list of tasks to refine
    def getTasksToRefine_JEDI(self, vo=None, prodSourceLabel=None):
        comment = " /* JediDBProxy.getTasksToRefine_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"vo={vo} label={prodSourceLabel}")
        tmpLog.debug("start")
        retTaskIDs = []
        try:
            # sql to get jediTaskIDs to refine from the command table
            sqlC = f"SELECT taskid,parent_tid FROM {panda_config.schemaDEFT}.T_TASK "
            sqlC += "WHERE status=:status "
            varMap = {}
            varMap[":status"] = "waiting"
            if vo not in [None, "any"]:
                varMap[":vo"] = vo
                sqlC += "AND vo=:vo "
            if prodSourceLabel not in [None, "any"]:
                varMap[":prodSourceLabel"] = prodSourceLabel
                sqlC += "AND prodSourceLabel=:prodSourceLabel "
            sqlC += "ORDER BY submit_time "
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
            for jediTaskID, parent_tid in resList:
                tmpLog.debug(f"start jediTaskID={jediTaskID}")
                # start transaction
                self.conn.begin()
                # lock
                varMap = {}
                varMap[":taskid"] = jediTaskID
                varMap[":status"] = "waiting"
                sqlLock = f"SELECT taskid FROM {panda_config.schemaDEFT}.T_TASK WHERE taskid=:taskid AND status=:status "
                sqlLock += "FOR UPDATE "
                toSkip = False
                try:
                    tmpLog.debug(sqlLock + comment + str(varMap))
                    self.cur.execute(sqlLock + comment, varMap)
                except Exception:
                    errType, errValue = sys.exc_info()[:2]
                    if self.isNoWaitException(errValue):
                        # resource busy and acquire with NOWAIT specified
                        toSkip = True
                        tmpLog.debug(f"skip locked jediTaskID={jediTaskID}")
                    else:
                        # failed with something else
                        raise errType(errValue)
                if not toSkip:
                    resLock = self.cur.fetchone()
                    if resLock is None:
                        # already processed
                        toSkip = True
                        tmpLog.debug(f"skip jediTaskID={jediTaskID} already processed")
                isOK = True
                if not toSkip:
                    if isOK:
                        # insert task to JEDI
                        varMap = {}
                        varMap[":jediTaskID"] = jediTaskID
                        import uuid

                        varMap[":taskName"] = str(uuid.uuid4())
                        varMap[":status"] = "registered"
                        varMap[":userName"] = "tobeset"
                        varMap[":parent_tid"] = parent_tid
                        sqlIT = f"INSERT INTO {panda_config.schemaJEDI}.JEDI_Tasks "
                        sqlIT += "(jediTaskID,taskName,status,userName,creationDate,modificationTime,parent_tid,stateChangeTime"
                        if vo is not None:
                            sqlIT += ",vo"
                        if prodSourceLabel is not None:
                            sqlIT += ",prodSourceLabel"
                        sqlIT += ") "
                        sqlIT += "VALUES(:jediTaskID,:taskName,:status,:userName,CURRENT_DATE,CURRENT_DATE,:parent_tid,CURRENT_DATE"
                        if vo is not None:
                            sqlIT += ",:vo"
                            varMap[":vo"] = vo
                        if prodSourceLabel is not None:
                            sqlIT += ",:prodSourceLabel"
                            varMap[":prodSourceLabel"] = prodSourceLabel
                        sqlIT += ") "
                        try:
                            tmpLog.debug(sqlIT + comment + str(varMap))
                            self.cur.execute(sqlIT + comment, varMap)
                        except Exception:
                            errtype, errvalue = sys.exc_info()[:2]
                            tmpLog.error(f"failed to insert jediTaskID={jediTaskID} with {errtype} {errvalue}")
                            isOK = False
                            try:
                                # delete task and param until DEFT bug is fixed
                                tmpLog.debug(f"trying to delete jediTaskID={jediTaskID}")
                                # check status
                                sqlDelCK = f"SELECT status FROM {panda_config.schemaJEDI}.JEDI_Tasks "
                                sqlDelCK += "WHERE jediTaskID=:jediTaskID "
                                varMap = {}
                                varMap[":jediTaskID"] = jediTaskID
                                self.cur.execute(sqlDelCK + comment, varMap)
                                resDelCK = self.cur.fetchone()
                                if resDelCK is not None:
                                    (delStatus,) = resDelCK
                                else:
                                    delStatus = None
                                # get size of DEFT param
                                sqlDelDZ = f"SELECT LENGTH(jedi_task_parameters) FROM {panda_config.schemaDEFT}.T_TASK "
                                sqlDelDZ += "WHERE taskid=:jediTaskID "
                                varMap = {}
                                varMap[":jediTaskID"] = jediTaskID
                                self.cur.execute(sqlDelDZ + comment, varMap)
                                resDelDZ = self.cur.fetchone()
                                if resDelDZ is not None:
                                    (delDeftSize,) = resDelDZ
                                else:
                                    delDeftSize = None
                                # get size of JEDI param
                                sqlDelJZ = f"SELECT LENGTH(taskParams) FROM {panda_config.schemaJEDI}.JEDI_TaskParams "
                                sqlDelJZ += "WHERE jediTaskID=:jediTaskID "
                                varMap = {}
                                varMap[":jediTaskID"] = jediTaskID
                                self.cur.execute(sqlDelJZ + comment, varMap)
                                resDelJZ = self.cur.fetchone()
                                if resDelJZ is not None:
                                    (delJediSize,) = resDelJZ
                                else:
                                    delJediSize = None
                                tmpLog.debug(f"jediTaskID={jediTaskID} has status={delStatus} param size in DEFT {delDeftSize} vs in JEDI {delJediSize}")
                                # delete
                                if delStatus == "registered" and delDeftSize != delJediSize and delJediSize == 2000:
                                    sqlDelJP = f"DELETE FROM {panda_config.schemaJEDI}.JEDI_TaskParams "
                                    sqlDelJP += "WHERE jediTaskID=:jediTaskID "
                                    varMap = {}
                                    varMap[":jediTaskID"] = jediTaskID
                                    self.cur.execute(sqlDelJP + comment, varMap)
                                    nRowP = self.cur.rowcount
                                    tmpLog.debug(f"deleted param for jediTaskID={jediTaskID} with {nRowP}")
                                    sqlDelJT = f"DELETE FROM {panda_config.schemaJEDI}.JEDI_Tasks "
                                    sqlDelJT += "WHERE jediTaskID=:jediTaskID ".format(panda_config.schemaJEDI)
                                    varMap = {}
                                    varMap[":jediTaskID"] = jediTaskID
                                    self.cur.execute(sqlDelJT + comment, varMap)
                                    nRowT = self.cur.rowcount
                                    tmpLog.debug(f"deleted task for jediTaskID={jediTaskID} with {nRowT}")
                                    if nRowP == 1 and nRowT == 1:
                                        # commit
                                        if not self._commit():
                                            raise RuntimeError("Commit error")
                                        # continue to skip subsequent rollback
                                        continue
                            except Exception:
                                errtype, errvalue = sys.exc_info()[:2]
                                tmpLog.error(f"failed to delete jediTaskID={jediTaskID} with {errtype} {errvalue}")
                    if isOK:
                        # check task parameters
                        varMap = {}
                        varMap[":taskid"] = jediTaskID
                        sqlTC = f"SELECT taskid FROM {panda_config.schemaDEFT}.T_TASK WHERE taskid=:taskid "
                        tmpLog.debug(sqlTC + comment + str(varMap))
                        self.cur.execute(sqlTC + comment, varMap)
                        resTC = self.cur.fetchone()
                        if resTC is None or resTC[0] is None:
                            tmpLog.error("task parameters not found in T_TASK")
                            isOK = False
                    if isOK:
                        # copy task parameters
                        varMap = {}
                        varMap[":taskid"] = jediTaskID
                        sqlPaste = f"INSERT INTO {panda_config.schemaJEDI}.JEDI_TaskParams (jediTaskID,taskParams) "
                        sqlPaste += "VALUES(:taskid,:taskParams) "
                        sqlSize = f"SELECT LENGTH(jedi_task_parameters) FROM {panda_config.schemaDEFT}.T_TASK "
                        sqlSize += "WHERE taskid=:taskid "
                        sqlCopy = f"SELECT jedi_task_parameters FROM {panda_config.schemaDEFT}.T_TASK "
                        sqlCopy += "WHERE taskid=:taskid "
                        try:
                            # get size
                            self.cur.execute(sqlSize + comment, varMap)
                            (totalSize,) = self.cur.fetchone()
                            # decomposed to SELECT and INSERT since sometimes oracle truncated params
                            tmpLog.debug(sqlCopy + comment + str(varMap))
                            self.cur.execute(sqlCopy + comment, varMap)
                            retStr = ""
                            for (tmpItem,) in self.cur:
                                retStr = tmpItem
                                break
                            # check size
                            if len(retStr) != totalSize:
                                raise RuntimeError(f"taskParams was truncated {len(retStr)}/{totalSize} bytes")
                            varMap = {}
                            varMap[":taskid"] = jediTaskID
                            varMap[":taskParams"] = retStr
                            self.cur.execute(sqlPaste + comment, varMap)
                            tmpLog.debug(f"inserted taskParams for jediTaskID={jediTaskID} {len(retStr)}/{totalSize}")
                        except Exception:
                            errtype, errvalue = sys.exc_info()[:2]
                            tmpLog.error(f"failed to insert param for jediTaskID={jediTaskID} with {errtype} {errvalue}")
                            isOK = False
                    # update
                    if isOK:
                        deftStatus = "registered"
                        varMap = {}
                        varMap[":taskid"] = jediTaskID
                        varMap[":status"] = deftStatus
                        varMap[":ndone"] = 0
                        varMap[":nreq"] = 0
                        varMap[":tevts"] = 0
                        sqlUC = f"UPDATE {panda_config.schemaDEFT}.T_TASK "
                        sqlUC += "SET status=:status,timestamp=CURRENT_DATE,total_done_jobs=:ndone,total_req_jobs=:nreq,total_events=:tevts "
                        sqlUC += "WHERE taskid=:taskid "
                        tmpLog.debug(sqlUC + comment + str(varMap))
                        self.cur.execute(sqlUC + comment, varMap)
                        self.setSuperStatus_JEDI(jediTaskID, deftStatus)
                    # append
                    if isOK:
                        retTaskIDs.append((jediTaskID, None, "registered", parent_tid))
                # commit
                if isOK:
                    if not self._commit():
                        raise RuntimeError("Commit error")
                else:
                    # roll back
                    self._rollback()
            # find orphaned tasks to rescue
            self.conn.begin()
            varMap = {}
            varMap[":status1"] = "registered"
            varMap[":status2"] = JediTaskSpec.commandStatusMap()["incexec"]["done"]
            varMap[":status3"] = "staged"
            varMap[":timeLimit"] = naive_utcnow() - datetime.timedelta(minutes=10)
            sqlOrpS = "SELECT tabT.jediTaskID,tabT.splitRule,tabT.status,tabT.parent_tid "
            sqlOrpS += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_AUX_Status_MinTaskID tabA ".format(panda_config.schemaJEDI)
            sqlOrpS += "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID "
            sqlOrpS += "AND tabT.status IN (:status1,:status2,:status3) AND tabT.modificationtime<:timeLimit "
            if vo is not None:
                sqlOrpS += "AND vo=:vo "
                varMap[":vo"] = vo
            if prodSourceLabel is not None:
                sqlOrpS += "AND prodSourceLabel=:prodSourceLabel "
                varMap[":prodSourceLabel"] = prodSourceLabel
            sqlOrpS += "FOR UPDATE "
            tmpLog.debug(sqlOrpS + comment + str(varMap))
            self.cur.execute(sqlOrpS + comment, varMap)
            resList = self.cur.fetchall()
            # update modtime to avoid immediate reattempts
            sqlOrpU = f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks SET modificationtime=CURRENT_DATE "
            sqlOrpU += "WHERE jediTaskID=:jediTaskID "
            for jediTaskID, splitRule, taskStatus, parent_tid in resList:
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                tmpLog.debug(sqlOrpU + comment + str(varMap))
                self.cur.execute(sqlOrpU + comment, varMap)
                nRow = self.cur.rowcount
                if nRow == 1 and jediTaskID not in retTaskIDs:
                    retTaskIDs.append((jediTaskID, splitRule, taskStatus, parent_tid))
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            tmpLog.debug(f"return {len(retTaskIDs)} tasks")
            return retTaskIDs
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return None

    # get task parameters with jediTaskID
    def getTaskParamsWithID_JEDI(self, jediTaskID, use_commit=True):
        comment = " /* JediDBProxy.getTaskParamsWithID_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID}")
        tmpLog.debug("start")
        try:
            # sql
            sql = f"SELECT taskParams FROM {panda_config.schemaJEDI}.JEDI_TaskParams WHERE jediTaskID=:jediTaskID "
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            if use_commit:
                # begin transaction
                self.conn.begin()
            self.cur.arraysize = 100
            self.cur.execute(sql + comment, varMap)
            retStr = ""
            totalSize = 0
            for (tmpItem,) in self.cur:
                retStr = tmpItem
                totalSize += len(tmpItem)
                break
            if use_commit:
                # commit
                if not self._commit():
                    raise RuntimeError("Commit error")
            tmpLog.debug(f"read {len(retStr)}/{totalSize} bytes")
            return retStr
        except Exception:
            if use_commit:
                # roll back
                self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return None

    # update jobMetrics
    def updateJobMetrics_JEDI(self, jediTaskID, pandaID, jobMetrics, tags):
        comment = " /* JediDBProxy.updateJobMetrics_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"jediTaskid={jediTaskID} PandaID={pandaID}")
        tmpLog.debug(f"start tags={','.join(tags)}")
        # set new jobMetrics
        tagStr = "scout=" + "|".join(tags)
        if jobMetrics is None:
            newSH = tagStr
        else:
            items = jobMetrics.split(" ")
            items = [item for item in items if not item.startswith("scout=")]
            items.append(tagStr)
            newSH = " ".join(items)
        # cap
        newSH = newSH[:500]
        # update live table
        sqlL = f"UPDATE {panda_config.schemaPANDA}.jobsArchived4 "
        sqlL += "SET jobMetrics=:newStr WHERE PandaID=:PandaID "
        varMap = {}
        varMap[":PandaID"] = pandaID
        varMap[":newStr"] = newSH
        self.cur.execute(sqlL + comment, varMap)
        nRow = self.cur.rowcount
        if nRow != 1:
            # update archive table
            sqlA = f"UPDATE {panda_config.schemaPANDAARCH}.jobsArchived "
            sqlA += "SET jobMetrics=:newStr WHERE PandaID=:PandaID AND modificationTime>(CURRENT_DATE-30) "
            varMap = {}
            varMap[":PandaID"] = pandaID
            varMap[":newStr"] = newSH
            self.cur.execute(sqlA + comment, varMap)
            nRow = self.cur.rowcount
        tmpLog.debug(f"done with {nRow}")
        return

    # get the list of PandaIDs for a task
    def getPandaIDsWithTask_JEDI(self, jediTaskID, onlyActive):
        comment = " /* JediDBProxy.getPandaIDsWithTask_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID} onlyActive={onlyActive}")
        tmpLog.debug("start")
        retPandaIDs = set()
        try:
            # sql to get PandaIDs
            tables = [
                f"{panda_config.schemaPANDA}.jobsDefined4",
                f"{panda_config.schemaPANDA}.jobsActive4",
            ]
            if not onlyActive:
                tables += [f"{panda_config.schemaPANDA}.jobsArchived4", f"{panda_config.schemaPANDAARCH}.jobsArchived"]
            sqlP = ""
            for tableName in tables:
                if sqlP != "":
                    sqlP += "UNION ALL "
                sqlP += f"SELECT PandaID FROM {tableName} WHERE jediTaskID=:jediTaskID "
                if tableName.startswith(panda_config.schemaPANDAARCH):
                    sqlP += "AND modificationTime>(CURRENT_DATE-30) "
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            # start transaction
            self.conn.begin()
            self.cur.arraysize = 1000000
            self.cur.execute(sqlP + comment, varMap)
            resList = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            for (pandaID,) in resList:
                retPandaIDs.add(pandaID)
            # return
            tmpLog.debug(f"return {len(retPandaIDs)} PandaIDs")
            return list(retPandaIDs)
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return None

    # get the list of queued PandaIDs for a task
    def getQueuedPandaIDsWithTask_JEDI(self, jediTaskID):
        comment = " /* JediDBProxy.getQueuedPandaIDsWithTask_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID}")
        tmpLog.debug("start")
        retPandaIDs = []
        try:
            # sql to get PandaIDs
            tables = [
                f"{panda_config.schemaPANDA}.jobsDefined4",
                f"{panda_config.schemaPANDA}.jobsActive4",
            ]
            sqlP = ""
            for tableName in tables:
                if sqlP != "":
                    sqlP += "UNION ALL "
                sqlP += f"SELECT PandaID FROM {tableName} WHERE jediTaskID=:jediTaskID "
                sqlP += "AND jobStatus NOT IN (:st1,:st2,:st3) "
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            varMap[":st1"] = "running"
            varMap[":st2"] = "holding"
            varMap[":st3"] = "transferring"
            # start transaction
            self.conn.begin()
            self.cur.arraysize = 1000000
            self.cur.execute(sqlP + comment, varMap)
            resList = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            for (pandaID,) in resList:
                if pandaID not in retPandaIDs:
                    retPandaIDs.append(pandaID)
            # return
            tmpLog.debug(f"return {len(retPandaIDs)} PandaIDs")
            return retPandaIDs
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return None

    # get jediTaskID/datasetID/FileID with dataset and file names
    def getIDsWithFileDataset_JEDI(self, datasetName, fileName, fileType):
        comment = " /* JediDBProxy.getIDsWithFileDataset_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"dataset={datasetName} file={fileName} type={fileType}")
        tmpLog.debug("start")
        retPandaIDs = []
        try:
            # sql to get jediTaskID and datasetID
            sqlT = f"SELECT jediTaskID,datasetID FROM {panda_config.schemaJEDI}.JEDI_Datasets WHERE "
            sqlT += "datasetName=:datasetName and type=:type "
            # sql to get fileID
            sqlF = f"SELECT FileID FROM {panda_config.schemaJEDI}.JEDI_Dataset_Contents WHERE "
            sqlF += "jediTaskID=:jediTaskID AND datasetID=:datasetID and lfn=:lfn "
            # start transaction
            self.conn.begin()
            varMap = {}
            varMap[":datasetName"] = datasetName
            varMap[":type"] = fileType
            self.cur.arraysize = 1000000
            self.cur.execute(sqlT + comment, varMap)
            resList = self.cur.fetchall()
            retMap = None
            for jediTaskID, datasetID in resList:
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                varMap[":datasetID"] = datasetID
                varMap[":lfn"] = fileName
                self.cur.execute(sqlF + comment, varMap)
                resFileList = self.cur.fetchall()
                if resFileList != []:
                    retMap = {}
                    retMap["jediTaskID"] = jediTaskID
                    retMap["datasetID"] = datasetID
                    retMap["fileID"] = resFileList[0][0]
                    break
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            tmpLog.debug(f"return {str(retMap)}")
            return True, retMap
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return False, None

    # get JOBSARCHVIEW corresponding to a timestamp
    def getArchView(self, timeStamp):
        tableList = [
            (7, "JOBSARCHVIEW_7DAYS"),
            (15, "JOBSARCHVIEW_15DAYS"),
            (30, "JOBSARCHVIEW_30DAYS"),
            (60, "JOBSARCHVIEW_60DAYS"),
            (90, "JOBSARCHVIEW_90DAYS"),
            (180, "JOBSARCHVIEW_180DAYS"),
            (365, "JOBSARCHVIEW_365DAYS"),
        ]
        timeDelta = naive_utcnow() - timeStamp
        for timeLimit, archViewName in tableList:
            # +2 for safety margin
            if timeDelta < datetime.timedelta(days=timeLimit + 2):
                return archViewName
        # range over
        return None

    # get PandaID for a file
    def getPandaIDWithFileID_JEDI(self, jediTaskID, datasetID, fileID):
        comment = " /* JediDBProxy.getPandaIDWithFileID_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID} datasetID={datasetID} fileID={fileID}")
        tmpLog.debug("start")
        retPandaIDs = []
        try:
            # sql to get PandaID
            sqlP = f"SELECT PandaID FROM {panda_config.schemaPANDA}.filesTable4 WHERE "
            sqlP += "jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
            # get creation time of the task
            sqlCT = f"SELECT creationDate FROM {panda_config.schemaJEDI}.JEDI_Tasks WHERE jediTaskID=:jediTaskID "
            # start transaction
            self.conn.begin()
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            varMap[":datasetID"] = datasetID
            varMap[":fileID"] = fileID
            self.cur.arraysize = 100
            self.cur.execute(sqlP + comment, varMap)
            resP = self.cur.fetchone()
            pandaID = None
            if resP is not None:
                # found in live table
                pandaID = resP[0]
            else:
                # get creation time of the task
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                self.cur.execute(sqlCT + comment, varMap)
                resCT = self.cur.fetchone()
                if resCT is not None:
                    (creationDate,) = resCT
                    archView = self.getArchView(creationDate)
                    if archView is None:
                        tmpLog.debug("no JOBSARCHVIEW since creationDate is too old")
                    else:
                        # sql to get PandaID using JOBSARCHVIEW
                        varMap = {}
                        varMap[":jediTaskID"] = jediTaskID
                        varMap[":datasetID"] = datasetID
                        varMap[":fileID"] = fileID
                        sqlAP = "SELECT fTab.PandaID "
                        sqlAP += "FROM {0}.filesTable_ARCH fTab,{0}.{1} aTab WHERE ".format(panda_config.schemaPANDAARCH, archView)
                        sqlAP += "fTab.PandaID=aTab.PandaID AND aTab.jediTaskID=:jediTaskID "
                        sqlAP += "AND fTab.jediTaskID=:jediTaskID AND fTab.datasetID=:datasetID "
                        sqlAP += "AND fTab.fileID=:fileID "
                        tmpLog.debug(sqlAP + comment + str(varMap))
                        self.cur.execute(sqlAP + comment, varMap)
                        resAP = self.cur.fetchone()
                        if resAP is not None:
                            pandaID = resAP[0]
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            tmpLog.debug(f"PandaID -> {pandaID}")
            return True, pandaID
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return False, None

    # get JEDI files for a job
    def getFilesWithPandaID_JEDI(self, pandaID):
        comment = " /* JediDBProxy.getFilesWithPandaID_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"pandaID={pandaID}")
        tmpLog.debug("start")
        retPandaIDs = []
        try:
            # sql to get fileID
            sqlT = f"SELECT jediTaskID,datasetID,fileID FROM {panda_config.schemaPANDA}.filesTable4 WHERE "
            sqlT += "pandaID=:pandaID "
            sqlT += "UNION ALL "
            sqlT += f"SELECT jediTaskID,datasetID,fileID FROM {panda_config.schemaPANDAARCH}.filesTable_ARCH WHERE "
            sqlT += "pandaID=:pandaID "
            sqlT += "AND modificationTime>CURRENT_DATE-180"
            # sql to read files
            sqlFR = f"SELECT {JediFileSpec.columnNames()} "
            sqlFR += f"FROM {panda_config.schemaJEDI}.JEDI_Dataset_Contents WHERE "
            sqlFR += "jediTaskID=:jediTaskID AND datasetID=:datasetID and fileID=:fileID "
            # start transaction
            self.conn.begin()
            varMap = {}
            varMap[":pandaID"] = pandaID
            self.cur.arraysize = 1000000
            self.cur.execute(sqlT + comment, varMap)
            resTC = self.cur.fetchall()
            fileIDList = []
            fileSpecList = []
            # loop over all fileIDs
            for jediTaskID, datasetID, fileID in resTC:
                # skip duplication
                if fileID in fileIDList:
                    continue
                # read files
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                varMap[":datasetID"] = datasetID
                varMap[":fileID"] = fileID
                self.cur.execute(sqlFR + comment, varMap)
                tmpRes = self.cur.fetchone()
                fileSpec = JediFileSpec()
                fileSpec.pack(tmpRes)
                fileSpecList.append(fileSpec)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            tmpLog.debug(f"got {len(fileSpecList)} files")
            return True, fileSpecList
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return False, None

    # update task parameters
    def updateTaskParams_JEDI(self, jediTaskID, taskParams):
        comment = " /* JediDBProxy.updateTaskParams_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID}")
        tmpLog.debug("start")
        retPandaIDs = []
        try:
            # sql to update task params
            sqlT = f"UPDATE {panda_config.schemaJEDI}.JEDI_TaskParams SET taskParams=:taskParams "
            sqlT += "WHERE jediTaskID=:jediTaskID "
            # start transaction
            self.conn.begin()
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            varMap[":taskParams"] = taskParams
            self.cur.execute(sqlT + comment, varMap)
            nRow = self.cur.rowcount
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            tmpLog.debug(f"updated {nRow} rows")
            if nRow == 1:
                return True
            else:
                return False
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return None

    # restart contents update
    def restartTasksForContentsUpdate_JEDI(self, vo, prodSourceLabel, timeLimit):
        comment = " /* JediDBProxy.restartTasksForContentsUpdate_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"vo={vo} label={prodSourceLabel} limit={timeLimit}min")
        tmpLog.debug("start")
        try:
            # sql to get stalled tasks in defined
            varMap = {}
            varMap[":taskStatus1"] = "defined"
            varMap[":taskStatus2"] = "ready"
            varMap[":timeLimit"] = naive_utcnow() - datetime.timedelta(minutes=timeLimit)
            varMap[":dsType"] = "input"
            varMap[":dsState"] = "mutable"
            varMap[":dsStatus1"] = "ready"
            varMap[":dsStatus2"] = "toupdate"
            sqlTL = "SELECT distinct tabT.jediTaskID,tabT.status,tabT.splitRule "
            sqlTL += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_Datasets tabD,{0}.JEDI_AUX_Status_MinTaskID tabA ".format(panda_config.schemaJEDI)
            sqlTL += "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID AND tabT.jediTaskID=tabD.jediTaskID "
            sqlTL += "AND ((tabT.status=:taskStatus1 AND tabD.status=:dsStatus1) OR (tabT.status=:taskStatus2 AND tabD.status=:dsStatus2)) "
            sqlTL += "AND tabD.type=:dsType AND tabD.state=:dsState AND tabT.modificationTime<:timeLimit "
            if vo not in [None, "any"]:
                varMap[":vo"] = vo
                sqlTL += "AND tabT.vo=:vo "
            if prodSourceLabel not in [None, "any"]:
                varMap[":prodSourceLabel"] = prodSourceLabel
                sqlTL += "AND tabT.prodSourceLabel=:prodSourceLabel "
            # get tasks in defined with only ready datasets
            sqlTR = "SELECT distinct tabT.jediTaskID "
            sqlTR += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_Datasets tabD,{0}.JEDI_AUX_Status_MinTaskID tabA ".format(panda_config.schemaJEDI)
            sqlTR += "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID AND tabT.jediTaskID=tabD.jediTaskID "
            sqlTR += "AND tabT.status=:taskStatus1 AND tabD.status=:dsStatus1 "
            sqlTR += "AND tabD.type=:dsType AND tabT.modificationTime<:timeLimit "
            sqlTR += "AND NOT EXISTS "
            sqlTR += f"(SELECT 1 FROM {panda_config.schemaJEDI}.JEDI_Datasets WHERE jediTaskID=tabT.jediTaskID AND type=:dsType AND status<>:dsStatus1) "
            if vo not in [None, "any"]:
                sqlTR += "AND tabT.vo=:vo "
            if prodSourceLabel not in [None, "any"]:
                sqlTR += "AND tabT.prodSourceLabel=:prodSourceLabel "
            # get tasks in ready with defined datasets
            sqlTW = "SELECT distinct tabT.jediTaskID,tabT.splitRule "
            sqlTW += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_Datasets tabD,{0}.JEDI_AUX_Status_MinTaskID tabA ".format(panda_config.schemaJEDI)
            sqlTW += "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID AND tabT.jediTaskID=tabD.jediTaskID "
            sqlTW += "AND tabT.status=:taskStatus1 AND tabD.status=:dsStatus1 "
            sqlTW += "AND tabD.type=:dsType AND tabT.modificationTime<:timeLimit "
            if vo not in [None, "any"]:
                sqlTW += "AND tabT.vo=:vo "
            if prodSourceLabel not in [None, "any"]:
                sqlTW += "AND tabT.prodSourceLabel=:prodSourceLabel "
            # sql to update mutable datasets
            sqlTU = f"UPDATE {panda_config.schemaJEDI}.JEDI_Datasets "
            sqlTU += "SET status=:newStatus "
            sqlTU += "WHERE jediTaskID=:jediTaskID AND type=:type AND state=:state AND status=:oldStatus "
            # sql to update ready datasets
            sqlRD = f"UPDATE {panda_config.schemaJEDI}.JEDI_Datasets "
            sqlRD += "SET status=:newStatus "
            sqlRD += "WHERE jediTaskID=:jediTaskID AND type=:type AND status=:oldStatus "
            # sql to update task
            sqlTD = f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks "
            sqlTD += "SET status=:newStatus,modificationtime=CURRENT_DATE "
            sqlTD += "WHERE jediTaskID=:jediTaskID AND status=:oldStatus "
            # start transaction
            self.conn.begin()
            # get jediTaskIDs
            self.cur.execute(sqlTL + comment, varMap)
            resTL = self.cur.fetchall()
            # loop over all tasks
            nTasks = 0
            msg_driven_taskid_set = set()
            for jediTaskID, taskStatus, splitRule in resTL:
                nRow = 0
                if taskStatus == "defined":
                    # update mutable datasets
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    varMap[":type"] = "input"
                    varMap[":state"] = "mutable"
                    varMap[":oldStatus"] = "ready"
                    varMap[":newStatus"] = "toupdate"
                    self.cur.execute(sqlTU + comment, varMap)
                    nRow = self.cur.rowcount
                    tmpLog.debug(f"jediTaskID={jediTaskID} toupdate {nRow} datasets")
                    if nRow > 0:
                        nTasks += 1
                        # update task
                        varMap = {}
                        varMap[":jediTaskID"] = jediTaskID
                        varMap[":oldStatus"] = "defined"
                        varMap[":newStatus"] = "defined"
                        self.cur.execute(sqlTD + comment, varMap)
                else:
                    # update task
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    varMap[":oldStatus"] = "ready"
                    varMap[":newStatus"] = "defined"
                    self.cur.execute(sqlTD + comment, varMap)
                    nRow = self.cur.rowcount
                    if nRow > 0:
                        tmpLog.debug("jediTaskID={0} back to defined".format(jediTaskID, nRow))
                        nTasks += 1
                if nRow > 0 and is_msg_driven(splitRule):
                    # added msg driven tasks
                    msg_driven_taskid_set.add(jediTaskID)
            # get tasks in defined with only ready datasets
            varMap = {}
            varMap[":taskStatus1"] = "defined"
            varMap[":timeLimit"] = naive_utcnow() - datetime.timedelta(minutes=timeLimit)
            varMap[":dsType"] = "input"
            varMap[":dsStatus1"] = "ready"
            if vo not in [None, "any"]:
                varMap[":vo"] = vo
            if prodSourceLabel not in [None, "any"]:
                varMap[":prodSourceLabel"] = prodSourceLabel
            # get jediTaskIDs
            self.cur.execute(sqlTR + comment, varMap)
            resTR = self.cur.fetchall()
            for (jediTaskID,) in resTR:
                # update ready datasets
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                varMap[":type"] = "input"
                varMap[":oldStatus"] = "ready"
                varMap[":newStatus"] = "defined"
                self.cur.execute(sqlRD + comment, varMap)
                nRow = self.cur.rowcount
                tmpLog.debug(f"jediTaskID={jediTaskID} reset {nRow} datasets in ready")
                if nRow > 0:
                    nTasks += 1
            # get tasks in ready with defined datasets
            varMap = {}
            varMap[":taskStatus1"] = "ready"
            varMap[":timeLimit"] = naive_utcnow() - datetime.timedelta(minutes=timeLimit)
            varMap[":dsType"] = "input"
            varMap[":dsStatus1"] = "defined"
            if vo not in [None, "any"]:
                varMap[":vo"] = vo
            if prodSourceLabel not in [None, "any"]:
                varMap[":prodSourceLabel"] = prodSourceLabel
            # get jediTaskIDs
            self.cur.execute(sqlTW + comment, varMap)
            resTW = self.cur.fetchall()
            for jediTaskID, splitRule in resTW:
                # update task
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                varMap[":oldStatus"] = "ready"
                varMap[":newStatus"] = "defined"
                self.cur.execute(sqlTD + comment, varMap)
                nRow = self.cur.rowcount
                if nRow > 0:
                    self.record_task_status_change(jediTaskID)
                    self.push_task_status_message(None, jediTaskID, varMap[":newStatus"])
                    tmpLog.debug(f"jediTaskID={jediTaskID} reset to defined")
                    nTasks += 1
                    if is_msg_driven(splitRule):
                        # added msg driven tasks
                        msg_driven_taskid_set.add(jediTaskID)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            tmpLog.debug("done")
            return nTasks, msg_driven_taskid_set
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return None, None

    # kick exhausted tasks
    def kickExhaustedTasks_JEDI(self, vo, prodSourceLabel, timeLimit):
        comment = " /* JediDBProxy.kickExhaustedTasks_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"vo={vo} label={prodSourceLabel} limit={timeLimit}h")
        tmpLog.debug("start")
        try:
            # sql to get stalled tasks
            varMap = {}
            varMap[":taskStatus"] = "exhausted"
            varMap[":timeLimit"] = naive_utcnow() - datetime.timedelta(hours=timeLimit)
            sqlTL = "SELECT tabT.jediTaskID,tabT.splitRule "
            sqlTL += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_AUX_Status_MinTaskID tabA ".format(panda_config.schemaJEDI)
            sqlTL += "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID "
            sqlTL += "AND tabT.status=:taskStatus AND tabT.modificationTime<:timeLimit "
            if vo not in [None, "any"]:
                varMap[":vo"] = vo
                sqlTL += "AND tabT.vo=:vo "
            if prodSourceLabel not in [None, "any"]:
                varMap[":prodSourceLabel"] = prodSourceLabel
                sqlTL += "AND tabT.prodSourceLabel=:prodSourceLabel "
            # sql to timeout tasks
            sqlTO = f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks "
            sqlTO += "SET status=:newStatus,modificationtime=CURRENT_DATE,stateChangeTime=CURRENT_DATE "
            sqlTO += "WHERE jediTaskID=:jediTaskID AND status=:oldStatus "
            # start transaction
            self.conn.begin()
            # get jediTaskIDs
            self.cur.execute(sqlTL + comment, varMap)
            resTL = self.cur.fetchall()
            # loop over all tasks
            nTasks = 0
            for jediTaskID, splitRule in resTL:
                taskSpec = JediTaskSpec()
                taskSpec.splitRule = splitRule
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                varMap[":oldStatus"] = "exhausted"
                if taskSpec.disableAutoFinish():
                    # to keep it exhausted since auto finish is disabled
                    varMap[":newStatus"] = "exhausted"
                else:
                    varMap[":newStatus"] = "finishing"
                self.cur.execute(sqlTO + comment, varMap)
                nRow = self.cur.rowcount
                tmpLog.debug(f"jediTaskID={jediTaskID} to {varMap[':newStatus']} with {nRow}")
                if nRow > 0:
                    nTasks += 1
                    # add missing record_task_status_change and push_task_status_message updates
                    self.record_task_status_change(jediTaskID)
                    self.push_task_status_message(taskSpec, jediTaskID, varMap[":newStatus"], splitRule)

            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            tmpLog.debug("done")
            return nTasks
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return None

    # get file spec of lib.tgz
    def get_previous_build_file_spec(
        self, jedi_task_id: int, site_name: str, associated_sites: list
    ) -> tuple[bool, JediFileSpec | None, JediDatasetSpec | None]:
        """
        Get the file and dataset specs of lib.tgz for a given task ID and site name which was generated in a previous submission cycle.

        Args:
            jedi_task_id (int): The JEDI task ID.
            site_name (str): The site name where the lib.tgz is located.
            associated_sites (list): A list of associated site names

        Returns:
            tuple: A tuple containing:
                bool: Success flag
                JediFileSpec | None: The file specification of lib.tgz if found, else None
                JediDatasetSpec | None: The dataset specification if found, else None
        """
        comment = " /* JediDBProxy.get_previous_build_file_spec */"
        tmp_log = self.create_tagged_logger(comment, f"jediTaskID={jedi_task_id} siteName={site_name}")
        tmp_log.debug("start")
        tmp_log.debug(f"associatedSites={str(associated_sites)}")
        try:
            # sql to get dataset
            sql_read_dataset = f"SELECT {JediDatasetSpec.columnNames()} "
            sql_read_dataset += f"FROM {panda_config.schemaJEDI}.JEDI_Datasets "
            sql_read_dataset += "WHERE jediTaskID=:jediTaskID AND type=:type AND site=:site "
            sql_read_dataset += "AND (state IS NULL OR state<>:state) "
            sql_read_dataset += "ORDER BY creationTime DESC "
            # sql to read files
            sql_read_file = f"SELECT {JediFileSpec.columnNames()} "
            sql_read_file += f"FROM {panda_config.schemaJEDI}.JEDI_Dataset_Contents WHERE "
            sql_read_file += "jediTaskID=:jediTaskID AND datasetID=:datasetID AND type=:type "
            sql_read_file += "AND NOt status IN (:status1,:status2) "
            sql_read_file += "ORDER BY creationDate DESC "
            # start transaction
            self.conn.begin()
            found_flag = False
            file_spec = None
            dataset_spec = None
            for tmp_site_name in [site_name] + associated_sites:
                # get dataset
                var_map = {":type": "lib", ":site": tmp_site_name, ":state": "closed", ":jediTaskID": jedi_task_id}
                self.cur.execute(sql_read_dataset + comment, var_map)
                res_list = self.cur.fetchall()
                # loop over all datasets
                for res_item in res_list:
                    dataset_spec = JediDatasetSpec()
                    dataset_spec.pack(res_item)
                    # get file
                    var_map = {":jediTaskID": jedi_task_id, ":datasetID": dataset_spec.datasetID, ":type": "lib", ":status1": "failed", ":status2": "cancelled"}
                    self.cur.execute(sql_read_file + comment, var_map)
                    res_file_list = self.cur.fetchall()
                    for res_file_item in res_file_list:
                        # make FileSpec
                        file_spec = JediFileSpec()
                        file_spec.pack(res_file_item)
                        if file_spec.status == "finished":
                            found_flag = True
                            break
                    # no more dataset lookup
                    if found_flag:
                        break
                # no more lookup with other sites
                if found_flag:
                    break
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            if file_spec is not None:
                tmp_log.debug(f"got lib.tgz={file_spec.lfn} status={file_spec.status}")
            else:
                tmp_log.debug("no lib.tgz")
            return True, file_spec, dataset_spec
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return False, None, None

    # get file spec of old lib.tgz
    def getOldBuildFileSpec_JEDI(self, jediTaskID: int, datasetID: int, fileID: int) -> tuple[bool, JediFileSpec | None, JediDatasetSpec | None]:
        """
        Get the file and dataset specs of an old lib.tgz using jediTaskID, datasetID, and fileID which was generated in the current submission cycle.

        Args:
            jediTaskID (int): The JEDI task ID.
            datasetID (int): The dataset ID.
            fileID (int): The file ID.

        Returns:
            tuple: A tuple containing:
                bool: Success flag
                JediFileSpec | None: The file specification of lib.tgz if found, else None
                JediDatasetSpec | None: The dataset specification if found, else None
        """
        comment = " /* JediDBProxy.getOldBuildFileSpec_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID} datasetID={datasetID} fileID={fileID}")
        tmpLog.debug("start")
        try:
            # sql to get dataset
            sqlRD = f"SELECT {JediDatasetSpec.columnNames()} "
            sqlRD += f"FROM {panda_config.schemaJEDI}.JEDI_Datasets "
            sqlRD += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            # sql to read files
            sqlFR = f"SELECT {JediFileSpec.columnNames()} "
            sqlFR += f"FROM {panda_config.schemaJEDI}.JEDI_Dataset_Contents WHERE "
            sqlFR += "jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
            # start transaction
            self.conn.begin()
            # get dataset
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            varMap[":datasetID"] = datasetID
            self.cur.execute(sqlRD + comment, varMap)
            tmpRes = self.cur.fetchone()
            datasetSpec = JediDatasetSpec()
            datasetSpec.pack(tmpRes)
            # get file
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            varMap[":datasetID"] = datasetID
            varMap[":fileID"] = fileID
            self.cur.execute(sqlFR + comment, varMap)
            tmpRes = self.cur.fetchone()
            fileSpec = JediFileSpec()
            fileSpec.pack(tmpRes)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            if fileSpec is not None:
                tmpLog.debug(f"got lib.tgz={fileSpec.lfn}")
            else:
                tmpLog.debug("no lib.tgz")
            return True, fileSpec, datasetSpec
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return False, None, None

    # get sites used by a task
    def getSitesUsedByTask_JEDI(self, jediTaskID):
        comment = " /* JediDBProxy.getSitesUsedByTask_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID}")
        tmpLog.debug("start")
        try:
            # sql to insert dataset
            sqlDS = f"SELECT distinct site FROM {panda_config.schemaJEDI}.JEDI_Datasets "
            sqlDS += "WHERE jediTaskID=:jediTaskID AND type IN (:type1,:type2) "
            # start transaction
            self.conn.begin()
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            varMap[":type1"] = "output"
            varMap[":type2"] = "log"
            # execute
            self.cur.execute(sqlDS + comment, varMap)
            resList = self.cur.fetchall()
            siteList = set()
            for (siteName,) in resList:
                siteList.add(siteName)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            tmpLog.debug(f"done -> {str(siteList)}")
            return True, siteList
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return False, None

    # get random seed
    def getRandomSeed_JEDI(self, jediTaskID, simul, n_files=1):
        comment = " /* JediDBProxy.getRandomSeed_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID}")
        tmpLog.debug("start")
        try:
            # sql to get pseudo dataset for random seed
            sqlDS = f"SELECT {JediDatasetSpec.columnNames()} "
            sqlDS += f"FROM {panda_config.schemaJEDI}.JEDI_Datasets "
            sqlDS += "WHERE jediTaskID=:jediTaskID AND type=:type "
            # sql to get min random seed
            sqlFR = f"SELECT * FROM (SELECT {JediFileSpec.columnNames()} "
            sqlFR += f"FROM {panda_config.schemaJEDI}.JEDI_Dataset_Contents WHERE "
            sqlFR += "jediTaskID=:jediTaskID AND datasetID=:datasetID AND status=:status "
            sqlFR += f"ORDER BY firstEvent) WHERE rownum<={n_files} "
            # sql to update file status
            sqlFU = f"UPDATE {panda_config.schemaJEDI}.JEDI_Dataset_Contents "
            sqlFU += "SET status=:status "
            sqlFU += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
            # sql to get max random seed
            sqlLR = f"SELECT MAX(firstEvent) FROM {panda_config.schemaJEDI}.JEDI_Dataset_Contents "
            sqlLR += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            # sql to get fileIDs
            sqlFID = f"SELECT {panda_config.schemaJEDI}.JEDI_DATASET_CONT_FILEID_SEQ.nextval FROM "
            sqlFID += "(SELECT level FROM dual CONNECT BY level<=:nIDs) "
            # sql to insert file
            sqlFI = f"INSERT INTO {panda_config.schemaJEDI}.JEDI_Dataset_Contents ({JediFileSpec.columnNames()}) "
            sqlFI += JediFileSpec.bindValuesExpression(useSeq=False)
            # start transaction
            self.conn.begin()
            self.cur.arraysize = 100000
            n_reused = 0
            n_new = 0
            # get pseudo dataset for random seed
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            varMap[":type"] = "random_seed"
            self.cur.execute(sqlDS + comment, varMap)
            resDS = self.cur.fetchone()
            if resDS is None:
                # no random seed
                retVal = (None, None)
                tmpLog.debug("no random seed")
            else:
                datasetSpec = JediDatasetSpec()
                datasetSpec.pack(resDS)
                # use existing random seeds
                randomseed_file_specs = []
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                varMap[":datasetID"] = datasetSpec.datasetID
                varMap[":status"] = "ready"
                self.cur.execute(sqlFR + comment, varMap)
                var_maps = []
                for resFR in self.cur.fetchall():
                    # make FileSpec to reuse the row
                    tmpFileSpec = JediFileSpec()
                    tmpFileSpec.pack(resFR)
                    n_reused += 1
                    # update status
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    varMap[":datasetID"] = datasetSpec.datasetID
                    varMap[":fileID"] = tmpFileSpec.fileID
                    varMap[":status"] = "picked"
                    var_maps.append(varMap)
                    randomseed_file_specs.append(tmpFileSpec)
                if not simul and var_maps:
                    self.cur.executemany(sqlFU + comment, var_maps)
                # add new random seeds if needed
                n_new_files = n_files - len(var_maps)
                if n_new_files > 0:
                    # get max random seed
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    varMap[":datasetID"] = datasetSpec.datasetID
                    self.cur.execute(sqlLR + comment, varMap)
                    resLR = self.cur.fetchone()
                    maxRndSeed = None
                    if resLR is not None:
                        (maxRndSeed,) = resLR
                    if maxRndSeed is None:
                        # first row
                        maxRndSeed = 1
                    else:
                        # increment
                        maxRndSeed += 1
                    # get new fileIDs
                    if not simul:
                        var_map = {}
                        var_map[":nIDs"] = n_new_files
                        self.cur.execute(sqlFID + comment, var_map)
                        new_file_ids = self.cur.fetchall()
                    else:
                        new_file_ids = [(0,) for _ in range(n_new_files)]
                    var_maps = []
                    for (new_file_id,) in new_file_ids:
                        # crate new file
                        tmpFileSpec = JediFileSpec()
                        tmpFileSpec.jediTaskID = jediTaskID
                        tmpFileSpec.datasetID = datasetSpec.datasetID
                        tmpFileSpec.fileID = new_file_id
                        tmpFileSpec.status = "picked"
                        tmpFileSpec.creationDate = naive_utcnow()
                        tmpFileSpec.keepTrack = 1
                        tmpFileSpec.type = "random_seed"
                        tmpFileSpec.lfn = f"{maxRndSeed}"
                        tmpFileSpec.firstEvent = maxRndSeed
                        varMap = tmpFileSpec.valuesMap()
                        var_maps.append(varMap)
                        maxRndSeed += 1
                        n_new += 1
                        tmpFileSpec.status = "ready"
                        randomseed_file_specs.append(tmpFileSpec)
                    if not simul and var_maps:
                        self.cur.executemany(sqlFI + comment, var_maps)
                # cannot return JobFileSpec due to owner.PandaID
                retVal = (randomseed_file_specs, datasetSpec)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            tmpLog.debug(f"done -> {n_reused} reused, {n_new} new")
            return True, retVal
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return False, (None, None)

    # get preprocess metadata
    def getPreprocessMetadata_JEDI(self, jediTaskID):
        comment = " /* JediDBProxy.getPreprocessMetadata_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID}")
        tmpLog.debug("start")
        # sql to get jobPrams for runXYZ
        sqlSCF = "SELECT tabF.PandaID "
        sqlSCF += "FROM {0}.JEDI_Datasets tabD, {0}.JEDI_Dataset_Contents tabF WHERE ".format(panda_config.schemaJEDI)
        sqlSCF += "tabD.jediTaskID=tabF.jediTaskID AND tabD.jediTaskID=:jediTaskID AND tabF.status=:status "
        sqlSCF += "AND tabD.datasetID=tabF.datasetID "
        sqlSCF += "AND tabF.type=:type AND tabD.masterID IS NULL "
        sqlSCD = f"SELECT metaData FROM {panda_config.schemaPANDA}.metaTable "
        sqlSCD += "WHERE PandaID=:pandaID "
        failedRet = False, None
        retVal = failedRet
        try:
            # begin transaction
            self.conn.begin()
            # get files
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            varMap[":status"] = "finished"
            varMap[":type"] = "pp_input"
            self.cur.execute(sqlSCF + comment, varMap)
            tmpRes = self.cur.fetchone()
            if tmpRes is None:
                tmpLog.error("no successful input file")
            else:
                (pandaID,) = tmpRes
                # get metadata
                metaData = None
                varMap = {}
                varMap[":pandaID"] = pandaID
                self.cur.execute(sqlSCD + comment, varMap)
                for (clobMeta,) in self.cur:
                    metaData = clobMeta
                    break
                if metaData is None:
                    tmpLog.error(f"no metaData for PandaID={pandaID}")
                else:
                    retVal = True, metaData
                    tmpLog.debug(f"got metaData from PandaID={pandaID}")
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            return retVal
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return failedRet

    # get log dataset for preprocessing
    def getPreproLog_JEDI(self, jediTaskID, simul):
        comment = " /* JediDBProxy.getPreproLog_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID}")
        tmpLog.debug("start")
        # sql to get dataset
        sqlDS = f"SELECT {JediDatasetSpec.columnNames()} "
        sqlDS += f"FROM {panda_config.schemaJEDI}.JEDI_Datasets "
        sqlDS += "WHERE jediTaskID=:jediTaskID AND type=:type "
        # sql to insert file
        sqlFI = f"INSERT INTO {panda_config.schemaJEDI}.JEDI_Dataset_Contents ({JediFileSpec.columnNames()}) "
        sqlFI += JediFileSpec.bindValuesExpression()
        sqlFI += " RETURNING fileID INTO :newFileID"
        # sql to update dataset
        sqlUD = f"UPDATE {panda_config.schemaJEDI}.JEDI_Datasets "
        sqlUD += "SET nFiles=nFiles+1 "
        sqlUD += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
        failedRet = False, None, None
        retVal = failedRet
        try:
            # begin transaction
            self.conn.begin()
            # get dataset
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            varMap[":type"] = "pp_log"
            self.cur.execute(sqlDS + comment, varMap)
            resDS = self.cur.fetchone()
            if resDS is None:
                tmpLog.error(f"no dataset with type={varMap[':type']}")
            else:
                datasetSpec = JediDatasetSpec()
                datasetSpec.pack(resDS)
                # make file
                datasetSpec.nFiles = datasetSpec.nFiles + 1
                tmpFileSpec = JediFileSpec()
                tmpFileSpec.jediTaskID = jediTaskID
                tmpFileSpec.datasetID = datasetSpec.datasetID
                tmpFileSpec.status = "defined"
                tmpFileSpec.creationDate = naive_utcnow()
                tmpFileSpec.keepTrack = 1
                tmpFileSpec.type = "log"
                tmpFileSpec.lfn = f"{datasetSpec.datasetName}._{datasetSpec.nFiles:06d}.log.tgz"
                if not simul:
                    varMap = tmpFileSpec.valuesMap(useSeq=True)
                    varMap[":newFileID"] = self.cur.var(varNUMBER)
                    self.cur.execute(sqlFI + comment, varMap)
                    val = self.getvalue_corrector(self.cur.getvalue(varMap[":newFileID"]))
                    tmpFileSpec.fileID = int(val)
                    # increment nFiles
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    varMap[":datasetID"] = datasetSpec.datasetID
                    self.cur.execute(sqlUD + comment, varMap)
                # return value
                retVal = True, datasetSpec, tmpFileSpec
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            tmpLog.debug("done")
            return retVal
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return failedRet

    # get JEDI tasks with a selection criteria
    def getTasksWithCriteria_JEDI(
        self, vo, prodSourceLabel, taskStatusList, taskCriteria, datasetCriteria, taskParamList, datasetParamList, taskLockColumn, taskLockInterval
    ):
        comment = " /* JediDBProxy.getTasksWithCriteria_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"vo={vo} label={prodSourceLabel}")
        tmpLog.debug(f"start with tC={str(taskCriteria)} dC={str(datasetCriteria)}")
        # return value for failure
        failedRet = None
        try:
            # sql
            varMap = {}
            sqlRT = "SELECT tabT.jediTaskID,"
            for tmpPar in taskParamList:
                sqlRT += f"tabT.{tmpPar},"
            for tmpPar in datasetParamList:
                sqlRT += f"tabD.{tmpPar},"
            sqlRT = sqlRT[:-1]
            sqlRT += " "
            sqlRT += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_AUX_Status_MinTaskID tabA".format(panda_config.schemaJEDI)
            if datasetCriteria:
                sqlRT += f",{panda_config.schemaJEDI}.JEDI_Datasets tabD"
            sqlRT += " WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID "
            if datasetCriteria:
                sqlRT += "AND tabT.jediTaskID=tabD.jediTaskID "
            status_var_names_str, status_var_map = get_sql_IN_bind_variables(taskStatusList, prefix=":status_", value_as_suffix=True)
            sqlRT += f"AND tabT.status IN ({status_var_names_str}) "
            varMap.update(status_var_map)
            if vo not in [None, "any"]:
                varMap[":vo"] = vo
                sqlRT += "AND tabT.vo=:vo "
            if prodSourceLabel not in [None, "any"]:
                varMap[":prodSourceLabel"] = prodSourceLabel
                sqlRT += "AND tabT.prodSourceLabel=:prodSourceLabel "
            for tmpKey, tmpVal in taskCriteria.items():
                if isinstance(tmpVal, list):
                    tmp_var_names_str, tmp_var_map = get_sql_IN_bind_variables(tmpVal, prefix=f":{tmpKey}_", value_as_suffix=True)
                    sqlRT += f"AND tabT.{tmpKey} IN ({tmp_var_names_str}) "
                    varMap.update(tmp_var_map)
                elif tmpVal is not None:
                    sqlRT += "AND tabT.{0}=:{0} ".format(tmpKey)
                    varMap[f":{tmpKey}"] = tmpVal
                else:
                    sqlRT += f"AND tabT.{tmpKey} IS NULL "
            for tmpKey, tmpVal in datasetCriteria.items():
                if isinstance(tmpVal, list):
                    tmp_var_names_str, tmp_var_map = get_sql_IN_bind_variables(tmpVal, prefix=f":{tmpKey}_", value_as_suffix=True)
                    sqlRT += f"AND tabD.{tmpKey} IN ({tmp_var_names_str}) "
                    varMap.update(tmp_var_map)
                elif tmpVal is not None:
                    sqlRT += "AND tabD.{0}=:{0} ".format(tmpKey)
                    varMap[f":{tmpKey}"] = tmpVal
                else:
                    sqlRT += f"AND tabD.{tmpKey} IS NULL "
            timeLimit = naive_utcnow() - datetime.timedelta(minutes=taskLockInterval)
            if taskLockColumn is not None:
                sqlRT += "AND (tabT.{0} IS NULL OR tabT.{0}<:lockTimeLimit) ".format(taskLockColumn)
                varMap[":lockTimeLimit"] = timeLimit
            sqlRT += "ORDER BY tabT.jediTaskID "
            # sql to lock
            if taskLockColumn is not None:
                sqlLK = f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks "
                sqlLK += f"SET {taskLockColumn}=CURRENT_DATE "
                sqlLK += "WHERE jediTaskID=:jediTaskID AND ({0} IS NULL OR {0}<:lockTimeLimit) ".format(taskLockColumn)
            # begin transaction
            self.conn.begin()
            self.cur.arraysize = 10000
            # get tasks
            tmpLog.debug(sqlRT + comment + str(varMap))
            self.cur.execute(sqlRT + comment, varMap)
            resList = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            retTasks = []
            for resRT in resList:
                jediTaskID = resRT[0]
                taskParMap = {}
                for tmpIdx, tmpPar in enumerate(taskParamList):
                    taskParMap[tmpPar] = resRT[tmpIdx + 1]
                datasetParMap = {}
                for tmpIdx, tmpPar in enumerate(datasetParamList):
                    datasetParMap[tmpPar] = resRT[tmpIdx + 1 + len(taskParamList)]
                # lock
                if taskLockColumn is not None:
                    # begin transaction
                    self.conn.begin()
                    varMap = dict()
                    varMap[":jediTaskID"] = jediTaskID
                    varMap[":lockTimeLimit"] = timeLimit
                    self.cur.execute(sqlLK + comment, varMap)
                    nLK = self.cur.rowcount
                    # commit
                    if not self._commit():
                        raise RuntimeError("Commit error")
                    # not locked
                    if nLK == 0:
                        continue
                retTasks.append((taskParMap, datasetParMap))
            tmpLog.debug(f"got {len(retTasks)} tasks")
            return retTasks
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return failedRet

    # get task status
    def getTaskStatus_JEDI(self, jediTaskID):
        comment = " /* JediDBProxy.getTaskStatus_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID}")
        tmpLog.debug("start")
        try:
            retVal = None
            sql = f"SELECT status FROM {panda_config.schemaJEDI}.JEDI_Tasks "
            sql += "WHERE jediTaskID=:jediTaskID "
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            # start transaction
            self.conn.begin()
            self.cur.execute(sql + comment, varMap)
            resTK = self.cur.fetchone()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            if resTK is not None:
                (retVal,) = resTK
            # return
            tmpLog.debug(f"done with {retVal}")
            return retVal
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return retVal

    # get lib.tgz for waiting jobs
    def getLibForWaitingRunJob_JEDI(self, vo, prodSourceLabel, checkInterval):
        comment = " /* JediDBProxy.getLibForWaitingRunJob_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"vo={vo} label={prodSourceLabel}")
        tmpLog.debug("start")
        try:
            # sql to get the list of user/jobIDs
            sqlL = "SELECT prodUserName,jobsetID,jobDefinitionID,MAX(PandaID) "
            sqlL += f"FROM {panda_config.schemaPANDA}.jobsDefined4 "
            sqlL += "WHERE vo=:vo AND prodSourceLabel=:prodSourceLabel "
            sqlL += "AND lockedBy=:lockedBy AND modificationTime<:timeLimit "
            sqlL += "GROUP BY prodUserName,jobsetID,jobDefinitionID "
            # sql to get data of lib.tgz
            sqlD = "SELECT lfn,dataset,jediTaskID,datasetID,fileID "
            sqlD += f"FROM {panda_config.schemaPANDA}.filesTable4 "
            sqlD += "WHERE PandaID=:PandaID AND type=:type AND status=:status "
            # sql to read file spec
            sqlF = f"SELECT {JediFileSpec.columnNames()} "
            sqlF += f"FROM {panda_config.schemaJEDI}.JEDI_Dataset_Contents "
            sqlF += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
            # sql to update modificationTime
            sqlU = f"UPDATE {panda_config.schemaPANDA}.jobsDefined4 "
            sqlU += "SET modificationTime=CURRENT_DATE "
            sqlU += "WHERE prodUserName=:prodUserName AND jobsetID=:jobsetID AND jobDefinitionID=:jobDefinitionID "
            # start transaction
            self.conn.begin()
            self.cur.arraysize = 100000
            retList = []
            # get the list of waiting user/jobIDs
            varMap = {}
            varMap[":vo"] = vo
            varMap[":prodSourceLabel"] = prodSourceLabel
            varMap[":lockedBy"] = "jedi"
            varMap[":timeLimit"] = naive_utcnow() - datetime.timedelta(minutes=checkInterval)
            self.cur.execute(sqlL + comment, varMap)
            resL = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # loop over all user/jobIDs
            for prodUserName, jobsetID, jobDefinitionID, pandaID in resL:
                self.conn.begin()
                # get data of lib.tgz
                varMap = {}
                varMap[":PandaID"] = pandaID
                varMap[":type"] = "input"
                varMap[":status"] = "unknown"
                self.cur.execute(sqlD + comment, varMap)
                resD = self.cur.fetchall()
                # loop over all files
                for lfn, datasetName, jediTaskID, datasetID, fileID in resD:
                    if re.search("\.lib\.tgz(\.\d+)*$", lfn) is not None:
                        # read file spec
                        varMap = {}
                        varMap[":jediTaskID"] = jediTaskID
                        varMap[":datasetID"] = datasetID
                        varMap[":fileID"] = fileID
                        self.cur.execute(sqlF + comment, varMap)
                        resF = self.cur.fetchone()
                        # make FileSpec
                        if resF is not None:
                            tmpFileSpec = JediFileSpec()
                            tmpFileSpec.pack(resF)
                            retList.append((prodUserName, datasetName, tmpFileSpec))
                            break
                # update modificationTime
                varMap = {}
                varMap[":prodUserName"] = prodUserName
                varMap[":jobsetID"] = jobsetID
                varMap[":jobDefinitionID"] = jobDefinitionID
                self.cur.execute(sqlU + comment, varMap)
                # commit
                if not self._commit():
                    raise RuntimeError("Commit error")
            # return
            tmpLog.debug(f"done with {len(retList)}")
            return retList
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return []

    # get tasks to get reassigned
    def getTasksToReassign_JEDI(self, vo=None, prodSourceLabel=None):
        comment = " /* JediDBProxy.getTasksToReassign_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"vo={vo} label={prodSourceLabel}")
        tmpLog.debug("start")
        retTasks = []
        try:
            # sql to get tasks to reassign
            varMap = {}
            varMap[":status"] = "reassigning"
            varMap[":timeLimit"] = naive_utcnow() - datetime.timedelta(minutes=5)
            sqlSCF = f"SELECT {JediTaskSpec.columnNames('tabT')} "
            sqlSCF += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_AUX_Status_MinTaskID tabA ".format(panda_config.schemaJEDI)
            sqlSCF += "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID "
            sqlSCF += "AND tabT.status=:status AND tabT.modificationTime<:timeLimit "
            if vo not in [None, "any"]:
                varMap[":vo"] = vo
                sqlSCF += "AND vo=:vo "
            if prodSourceLabel not in [None, "any"]:
                varMap[":prodSourceLabel"] = prodSourceLabel
                sqlSCF += "AND prodSourceLabel=:prodSourceLabel "
            sqlSCF += "FOR UPDATE"
            sqlSPC = f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks SET modificationTime=CURRENT_DATE "
            sqlSPC += "WHERE jediTaskID=:jediTaskID "
            # begin transaction
            self.conn.begin()
            # get tasks
            tmpLog.debug(sqlSCF + comment + str(varMap))
            self.cur.execute(sqlSCF + comment, varMap)
            resList = self.cur.fetchall()
            for resRT in resList:
                # make taskSpec
                taskSpec = JediTaskSpec()
                taskSpec.pack(resRT)
                # update modificationTime
                varMap = {}
                varMap[":jediTaskID"] = taskSpec.jediTaskID
                self.cur.execute(sqlSPC + comment, varMap)
                nRow = self.cur.rowcount
                if nRow > 0:
                    retTasks.append(taskSpec)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            tmpLog.debug(f"got {len(retTasks)} tasks")
            return retTasks
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return []

    # lock task
    def lockTask_JEDI(self, jediTaskID, pid):
        comment = " /* JediDBProxy.lockTask_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID} pid={pid}")
        tmpLog.debug("start")
        try:
            # sql to lock task
            sqlPD = f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks "
            sqlPD += "SET lockedTime=CURRENT_DATE,modificationTime=CURRENT_DATE "
            sqlPD += "WHERE jediTaskID=:jediTaskID AND lockedBy=:lockedBy "
            # sql to check lock
            sqlCL = f"SELECT lockedBy,lockedTime FROM {panda_config.schemaJEDI}.JEDI_Tasks "
            sqlCL += "WHERE jediTaskID=:jediTaskID "
            # begin transaction
            self.conn.begin()
            # lock
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            varMap[":lockedBy"] = pid
            self.cur.execute(sqlPD + comment, varMap)
            nRow = self.cur.rowcount
            if nRow == 1:
                retVal = True
                tmpLog.debug(f"done with {retVal}")
            else:
                retVal = False
                # check lock
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                self.cur.execute(sqlCL + comment, varMap)
                tmpLockedBy, tmpLockedTime = self.cur.fetchone()
                tmpLog.debug(f"done with {retVal} locked by another {tmpLockedBy} at {tmpLockedTime}")
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            return retVal
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return False

    # get successful files
    def getSuccessfulFiles_JEDI(self, jediTaskID, datasetID):
        comment = " /* JediDBProxy.getSuccessfulFiles_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID} datasetID={datasetID}")
        tmpLog.debug("start")
        try:
            # sql to get files
            sqlF = f"SELECT lfn FROM {panda_config.schemaJEDI}.JEDI_Dataset_Contents "
            sqlF += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND status=:status "
            # begin transaction
            self.conn.begin()
            # lock
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            varMap[":datasetID"] = datasetID
            varMap[":status"] = "finished"
            self.cur.execute(sqlF + comment, varMap)
            res = self.cur.fetchall()
            lfnList = []
            for (lfn,) in res:
                lfnList.append(lfn)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            tmpLog.debug(f"got {len(lfnList)} files")
            return lfnList
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return None

    # unlock a single task
    def unlockSingleTask_JEDI(self, jediTaskID, pid):
        comment = " /* JediDBProxy.unlockSingleTask_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID} pid={pid}")
        tmpLog.debug("start")
        try:
            # sql to unlock
            sqlTU = f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks "
            sqlTU += "SET lockedBy=NULL,lockedTime=NULL "
            sqlTU += "WHERE jediTaskID=:jediTaskID AND lockedBy=:pid "
            # begin transaction
            self.conn.begin()
            # unlock
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            varMap[":pid"] = pid
            self.cur.execute(sqlTU + comment, varMap)
            nRow = self.cur.rowcount
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmpLog.debug(f"done with {nRow}")
            return True
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return False

    # get JEDI tasks to be throttled
    def throttleTasks_JEDI(self, vo, prodSourceLabel, waitTime):
        comment = " /* JediDBProxy.throttleTasks_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"vo={vo} label={prodSourceLabel}")
        tmpLog.debug(f"start waitTime={waitTime}min")
        try:
            # sql
            varMap = {}
            varMap[":taskStatus"] = "running"
            varMap[":fileStat1"] = "ready"
            varMap[":fileStat2"] = "running"
            sqlRT = "SELECT tabT.jediTaskID,tabT.numThrottled,AVG(tabC.failedAttempt) "
            sqlRT += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_AUX_Status_MinTaskID tabA,".format(panda_config.schemaJEDI)
            sqlRT += "{0}.JEDI_Datasets tabD,{0}.JEDI_Dataset_Contents tabC ".format(panda_config.schemaJEDI)
            sqlRT += "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID "
            sqlRT += "AND tabT.jediTaskID=tabD.jediTaskID AND tabT.jediTaskID=tabC.jediTaskID "
            sqlRT += "AND tabD.datasetID=tabC.datasetID "
            sqlRT += "AND tabT.status IN (:taskStatus) "
            sqlRT += "AND tabT.numThrottled IS NOT NULL "
            sqlRT += f"AND tabD.type IN ({INPUT_TYPES_var_str}) "
            varMap.update(INPUT_TYPES_var_map)
            sqlRT += "AND tabD.masterID IS NULL "
            if vo not in [None, "any"]:
                varMap[":vo"] = vo
                sqlRT += "AND tabT.vo=:vo "
            if prodSourceLabel not in [None, "any"]:
                varMap[":prodSourceLabel"] = prodSourceLabel
                sqlRT += "AND tabT.prodSourceLabel=:prodSourceLabel "
            sqlRT += "AND tabC.status IN (:fileStat1,:fileStat2) "
            sqlRT += "AND tabT.lockedBy IS NULL "
            sqlRT += "GROUP BY tabT.jediTaskID,tabT.numThrottled "
            # begin transaction
            self.conn.begin()
            self.cur.arraysize = 10000
            # get tasks
            tmpLog.debug(sqlRT + comment + str(varMap))
            self.cur.execute(sqlRT + comment, varMap)
            resList = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # sql to throttle tasks
            sqlTH = f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks "
            sqlTH += "SET throttledTime=:releaseTime,modificationTime=CURRENT_DATE,"
            sqlTH += "oldStatus=status,status=:newStatus,errorDialog=:errorDialog,"
            sqlTH += "numThrottled=:numThrottled "
            sqlTH += "WHERE jediTaskID=:jediTaskID AND status=:oldStatus "
            sqlTH += "AND lockedBy IS NULL "
            attemptInterval = 5
            nTasks = 0
            for jediTaskID, numThrottled, largestAttemptNr in resList:
                # check threshold
                if int(largestAttemptNr / attemptInterval) <= numThrottled:
                    continue
                # begin transaction
                self.conn.begin()
                # check task
                try:
                    numThrottled += 1
                    throttledTime = naive_utcnow()
                    releaseTime = throttledTime + datetime.timedelta(minutes=waitTime * numThrottled * numThrottled)
                    errorDialog = "#ATM #KV action=throttle jediTaskID={0} due to reason=many_attempts {0} > {1}x{2} ".format(
                        jediTaskID, largestAttemptNr, numThrottled, attemptInterval
                    )
                    errorDialog += f"from {throttledTime.strftime('%Y/%m/%d %H:%M:%S')} "
                    errorDialog += f"till {releaseTime.strftime('%Y/%m/%d %H:%M:%S')}"
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    varMap[":newStatus"] = "throttled"
                    varMap[":oldStatus"] = "running"
                    varMap[":releaseTime"] = releaseTime
                    varMap[":numThrottled"] = numThrottled
                    varMap[":errorDialog"] = errorDialog
                    tmpLog.debug(sqlTH + comment + str(varMap))
                    self.cur.execute(sqlTH + comment, varMap)
                    if self.cur.rowcount > 0:
                        tmpLog.info(errorDialog)
                        nTasks += 1
                        self.record_task_status_change(jediTaskID)
                        self.push_task_status_message(None, jediTaskID, varMap[":newStatus"])
                except Exception:
                    tmpLog.debug(f"skip locked jediTaskID={jediTaskID}")
                # commit
                if not self._commit():
                    raise RuntimeError("Commit error")
            tmpLog.debug("done")
            return nTasks
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return None

    # throttle one task
    def throttleTask_JEDI(self, jediTaskID, waitTime, errorDialog):
        comment = " /* JediDBProxy.throttleTask_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID}")
        tmpLog.debug(f"start waitTime={waitTime}min")
        try:
            # sql to throttle tasks
            sqlTH = f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks "
            sqlTH += "SET throttledTime=:releaseTime,modificationTime=CURRENT_DATE,"
            sqlTH += "oldStatus=status,status=:newStatus,errorDialog=:errorDialog "
            sqlTH += "WHERE jediTaskID=:jediTaskID AND status=:oldStatus "
            sqlTH += "AND lockedBy IS NULL "
            # begin transaction
            self.conn.begin()
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            varMap[":newStatus"] = "throttled"
            varMap[":oldStatus"] = "running"
            varMap[":releaseTime"] = naive_utcnow() + datetime.timedelta(minutes=waitTime)
            varMap[":errorDialog"] = errorDialog
            self.cur.execute(sqlTH + comment, varMap)
            nRow = self.cur.rowcount
            tmpLog.debug(f"done with {nRow}")
            if nRow > 0:
                self.record_task_status_change(jediTaskID)
                self.push_task_status_message(None, jediTaskID, varMap[":newStatus"])
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            return True
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return False

    # release throttled tasks
    def releaseThrottledTasks_JEDI(self, vo, prodSourceLabel):
        comment = " /* JediDBProxy.releaseThrottledTasks_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"vo={vo} label={prodSourceLabel}")
        tmpLog.debug("start")
        try:
            # sql to get tasks
            varMap = {}
            varMap[":status"] = "throttled"
            sqlTL = "SELECT tabT.jediTaskID,tabT.oldStatus "
            sqlTL += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_AUX_Status_MinTaskID tabA,{0}.JEDI_Datasets tabD ".format(panda_config.schemaJEDI)
            sqlTL += "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID "
            sqlTL += f"AND tabD.jediTaskID=tabT.jediTaskID AND tabD.type IN ({INPUT_TYPES_var_str}) "
            varMap.update(INPUT_TYPES_var_map)
            sqlTL += "AND tabD.masterID IS NULL "
            sqlTL += "AND tabT.status=:status AND tabT.lockedBy IS NULL "
            sqlTL += "AND (tabT.throttledTime<CURRENT_DATE OR "
            sqlTL += "(tabD.nFilesToBeUsed=tabD.nFilesFinished+tabD.nFilesFailed AND tabD.nFiles>0)) "
            if vo not in [None, "any"]:
                varMap[":vo"] = vo
                sqlTL += "AND tabT.vo=:vo "
            if prodSourceLabel not in [None, "any"]:
                varMap[":prodSourceLabel"] = prodSourceLabel
                sqlTL += "AND tabT.prodSourceLabel=:prodSourceLabel "

            # sql to update tasks
            sqlTU = f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks "
            sqlTU += "SET status=oldStatus,oldStatus=NULL,errorDialog=NULL,modificationtime=CURRENT_DATE "
            sqlTU += "WHERE jediTaskID=:jediTaskID AND status=:oldStatus AND lockedBy IS NULL "

            # start transaction
            self.conn.begin()
            tmpLog.debug(sqlTL + comment + str(varMap))
            self.cur.execute(sqlTL + comment, varMap)
            resTL = self.cur.fetchall()

            # loop over all tasks
            nRow = 0
            for jediTaskID, oldStatus in resTL:
                if oldStatus in [None, ""]:
                    tmpLog.debug(f"cannot release jediTaskID={jediTaskID} since oldStatus is invalid")
                    continue
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                varMap[":oldStatus"] = "throttled"
                self.cur.execute(sqlTU + comment, varMap)
                iRow = self.cur.rowcount
                tmpLog.info(f"#ATM #KV action=released jediTaskID={jediTaskID} with {iRow}")
                nRow += iRow
                if iRow > 0:
                    self.record_task_status_change(jediTaskID)
                    self.push_task_status_message(None, jediTaskID, None)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            tmpLog.debug(f"updated {nRow} rows")
            return nRow
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return None

    # release a task with on-hold status
    def release_task_on_hold(self, jedi_task_id: int, target_status: str = None) -> bool:
        """Release a JEDI task with non-empty old status.
        Args:
            jedi_task_id: JEDI task ID to be released.
            target_status: If specified, check that the current status matches this value before releasing.
        Returns:
            True if succeeded, False otherwise.
        """
        comment = " /* JediDBProxy.release_task_on_hold */"
        tmp_log = self.create_tagged_logger(comment, f"jediTaskID={jedi_task_id}")
        tmp_log.debug(f"start target={target_status}")
        try:
            # sql to update tasks
            sql_check = f"SELECT status,oldStatus,lockedBy FROM {panda_config.schemaJEDI}.JEDI_Tasks WHERE jediTaskID=:jediTaskID FOR UPDATE "
            sql_update = (
                f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks "
                "SET status=oldStatus,oldStatus=NULL,errorDialog=NULL,modificationtime=CURRENT_DATE "
                "WHERE jediTaskID=:jediTaskID AND status=:status AND lockedBy IS NULL "
            )
            # start transaction
            self.conn.begin()
            var_map = {":jediTaskID": jedi_task_id}
            self.cur.execute(sql_check + comment, var_map)
            res = self.cur.fetchone()
            return_value = False
            if res is None:
                tmp_log.debug("unknown jediTaskID")
            else:
                status, old_status, locked_by = res
                if locked_by is not None:
                    tmp_log.debug(f"task is locked by {locked_by}")
                elif old_status in [None, ""]:
                    tmp_log.debug("cannot release since oldStatus is empty")
                elif status in JediTaskSpec.statusToRejectExtChange():
                    tmp_log.debug(f"cannot release since current status is {status}")
                elif target_status is not None and status != target_status:
                    tmp_log.debug(f"cannot release since current status {status} != target_status {target_status}")
                else:
                    # release
                    var_map = {":jediTaskID": jedi_task_id, ":status": status}
                    self.cur.execute(sql_update + comment, var_map)
                    n_row = self.cur.rowcount
                    tmp_log.debug(f"done with {n_row}")
                    if n_row > 0:
                        self.record_task_status_change(jedi_task_id)
                        self.push_task_status_message(None, jedi_task_id, None)
                        return_value = True
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            return return_value
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return False

    # get throttled users
    def getThrottledUsersTasks_JEDI(self, vo, prodSourceLabel):
        comment = " /* JediDBProxy.getThrottledUsersTasks_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"vo={vo} label={prodSourceLabel}")
        tmpLog.debug("start")
        try:
            # sql to get tasks
            varMap = {}
            varMap[":status"] = "throttled"
            sqlTL = "SELECT jediTaskID,userName,errorDialog "
            sqlTL += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_AUX_Status_MinTaskID tabA ".format(panda_config.schemaJEDI)
            sqlTL += "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID "
            sqlTL += "AND tabT.status=:status AND tabT.lockedBy IS NULL "
            if vo not in [None, "any"]:
                varMap[":vo"] = vo
                sqlTL += "AND vo=:vo "
            if prodSourceLabel not in [None, "any"]:
                varMap[":prodSourceLabel"] = prodSourceLabel
                sqlTL += "AND prodSourceLabel=:prodSourceLabel "
            # start transaction
            self.conn.begin()
            self.cur.execute(sqlTL + comment, varMap)
            resTL = self.cur.fetchall()
            # loop over all tasks
            userTaskMap = {}
            for jediTaskID, userName, errorDialog in resTL:
                userTaskMap.setdefault(userName, {})
                if errorDialog is None or "type=prestaging" in errorDialog:
                    trasnferType = "prestaging"
                else:
                    trasnferType = "transfer"
                userTaskMap[userName].setdefault(trasnferType, set())
                userTaskMap[userName][trasnferType].add(jediTaskID)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            tmpLog.debug(f"get {len(userTaskMap)} users")
            return userTaskMap
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return {}

    # get JEDI tasks to be assessed
    def getAchievedTasks_JEDI(self, vo, prodSourceLabel, timeLimit, nTasks):
        comment = " /* JediDBProxy.getAchievedTasks_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"vo={vo} label={prodSourceLabel}")
        tmpLog.debug("start")
        # return value for failure
        failedRet = None
        try:
            # sql
            varMap = {}
            varMap[":status1"] = "running"
            varMap[":status2"] = "pending"
            sqlRT = "SELECT tabT.jediTaskID,tabT.status,tabT.goal,tabT.splitRule,parent_tid "
            sqlRT += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_AUX_Status_MinTaskID tabA ".format(panda_config.schemaJEDI)
            sqlRT += "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID "
            sqlRT += "AND tabT.status IN (:status1,:status2) "
            if vo not in [None, "any"]:
                varMap[":vo"] = vo
                sqlRT += "AND tabT.vo=:vo "
            if prodSourceLabel not in [None, "any"]:
                varMap[":prodSourceLabel"] = prodSourceLabel
                sqlRT += "AND tabT.prodSourceLabel=:prodSourceLabel "
            sqlRT += "AND goal IS NOT NULL "
            sqlRT += "AND (assessmentTime IS NULL OR assessmentTime<:timeLimit) "
            sqlRT += f"AND rownum<{nTasks} "
            sqlLK = f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks SET assessmentTime=CURRENT_DATE "
            sqlLK += "WHERE jediTaskID=:jediTaskID AND (assessmentTime IS NULL OR assessmentTime<:timeLimit) AND status=:status "
            sqlDS = "SELECT datasetID,type,nEvents,status "
            sqlDS += f"FROM {panda_config.schemaJEDI}.JEDI_Datasets "
            sqlDS += "WHERE jediTaskID=:jediTaskID "
            sqlDS += f"AND ((type IN ({INPUT_TYPES_var_str}) "
            sqlDS += "AND masterID IS NULL) OR (type=:type1)) "
            sqlFC = "SELECT COUNT(*) "
            sqlFC += f"FROM {panda_config.schemaJEDI}.JEDI_Dataset_Contents "
            sqlFC += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND status=:status AND failedAttempt=:failedAttempt "
            # sql to check parent
            sqlCP = f"SELECT status FROM {panda_config.schemaJEDI}.JEDI_Tasks "
            sqlCP += "WHERE jediTaskID=:parent_tid "
            # begin transaction
            self.conn.begin()
            self.cur.arraysize = 10000
            # get tasks
            timeToCheck = naive_utcnow() - datetime.timedelta(minutes=timeLimit)
            varMap[":timeLimit"] = timeToCheck
            tmpLog.debug(sqlRT + comment + str(varMap))
            self.cur.execute(sqlRT + comment, varMap)
            taskStatList = self.cur.fetchall()
            retTasks = []
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # get tasks and datasets
            for jediTaskID, taskStatus, taskGoal, splitRule, parent_tid in taskStatList:
                # begin transaction
                self.conn.begin()
                # lock task
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                varMap[":timeLimit"] = timeToCheck
                varMap[":status"] = taskStatus
                self.cur.execute(sqlLK + comment, varMap)
                nRow = self.cur.rowcount
                # commit
                if not self._commit():
                    raise RuntimeError("Commit error")
                if nRow == 1:
                    # make a task spec to check if auto finish is disabled
                    taskSpec = JediTaskSpec()
                    taskSpec.splitRule = splitRule
                    if taskSpec.disableAutoFinish():
                        tmpLog.debug(f"skip jediTaskID={jediTaskID} as auto-finish is disabled")
                        continue
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    varMap.update(INPUT_TYPES_var_map)
                    varMap[":type1"] = "output"
                    # begin transaction
                    self.conn.begin()
                    # check parent
                    if parent_tid not in [None, jediTaskID]:
                        varMapTmp = {}
                        varMapTmp[":parent_tid"] = parent_tid
                        self.cur.execute(sqlCP + comment, varMapTmp)
                        resCP = self.cur.fetchone()
                        if resCP[0] not in ["finished", "failed", "done", "aborted", "broken"]:
                            tmpLog.debug(f"skip jediTaskID={jediTaskID} as parent {parent_tid} is still {resCP[0]}")
                            # commit
                            if not self._commit():
                                raise RuntimeError("Commit error")
                            continue
                    # check datasets
                    self.cur.execute(sqlDS + comment, varMap)
                    resDS = self.cur.fetchall()
                    totalInputEvents = 0
                    totalOutputEvents = 0
                    firstOutput = True
                    # loop over all datasets
                    taskToFinish = True
                    for datasetID, datasetType, nEvents, dsStatus in resDS:
                        # to update contents
                        if dsStatus in JediDatasetSpec.statusToUpdateContents():
                            tmpLog.debug(f"skip jediTaskID={jediTaskID} datasetID={datasetID} is in {dsStatus}")
                            taskToFinish = False
                            break
                        # counts events
                        if datasetType in JediDatasetSpec.getInputTypes():
                            # input
                            try:
                                totalInputEvents += nEvents
                            except Exception:
                                pass
                            # check if there are unused files
                            varMap = {}
                            varMap[":jediTaskID"] = jediTaskID
                            varMap[":datasetID"] = datasetID
                            varMap[":status"] = "ready"
                            varMap[":failedAttempt"] = 0
                            self.cur.execute(sqlFC + comment, varMap)
                            (nUnUsed,) = self.cur.fetchone()
                            if nUnUsed != 0:
                                tmpLog.debug(f"skip jediTaskID={jediTaskID} datasetID={datasetID} has {nUnUsed} unused files")
                                taskToFinish = False
                                break
                        else:
                            # only one output
                            if firstOutput:
                                # output
                                try:
                                    totalOutputEvents += nEvents
                                except Exception:
                                    pass
                            firstOutput = False
                    # commit
                    if not self._commit():
                        raise RuntimeError("Commit error")
                    # check number of events
                    if taskToFinish:
                        if totalInputEvents == 0:
                            # input has 0 events
                            tmpLog.debug(f"skip jediTaskID={jediTaskID} input has 0 events")
                            taskToFinish = False
                        elif float(totalOutputEvents) / float(totalInputEvents) * 1000.0 < taskGoal:
                            # goal is not yet reached
                            tmpLog.debug(
                                f"skip jediTaskID={jediTaskID} goal is not yet reached {taskGoal / 10}.{taskGoal % 10}%>{totalOutputEvents}/{totalInputEvents}"
                            )
                            taskToFinish = False
                        else:
                            tmpLog.debug(
                                f"to finsh jediTaskID={jediTaskID} goal is reached {taskGoal / 10}.{taskGoal % 10}%<={totalOutputEvents}/{totalInputEvents}"
                            )
                    # append
                    if taskToFinish:
                        retTasks.append(jediTaskID)
            tmpLog.debug(f"got {len(retTasks)} tasks")
            return retTasks
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return failedRet

    # get inactive sites
    def getInactiveSites_JEDI(self, flag, timeLimit):
        comment = " /* JediDBProxy.getInactiveSites_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"flag={flag} timeLimit={timeLimit}")
        tmpLog.debug("start")
        try:
            retVal = set()
            # sql
            sqlCD = f"SELECT site FROM {panda_config.schemaMETA}.SiteData "
            sqlCD += "WHERE flag=:flag AND hours=:hours AND laststart<:laststart "
            # start transaction
            self.conn.begin()
            # check
            varMap = {}
            varMap[":flag"] = flag
            varMap[":hours"] = 3
            varMap[":laststart"] = naive_utcnow() - datetime.timedelta(hours=timeLimit)
            self.cur.execute(sqlCD + comment, varMap)
            resCD = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            for (tmpSiteID,) in resCD:
                retVal.add(tmpSiteID)
            tmpLog.debug("done")
            return retVal
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return retVal

    # get total walltime
    def getTotalWallTime_JEDI(self, vo, prodSourceLabel, workQueue, resource_name):
        comment = " /* JediDBProxy.getTotalWallTime_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"vo={vo} label={prodSourceLabel} queue={workQueue.queue_name}")
        tmpLog.debug("start")
        try:
            # sql to get size
            var_map = {":vo": vo, ":prodSourceLabel": prodSourceLabel, ":resource_name": resource_name}
            sql = "SELECT total_walltime, n_has_value, n_no_value "
            sql += f"FROM {panda_config.schemaPANDA}.total_walltime_cache "
            sql += "WHERE vo=:vo AND prodSourceLabel=:prodSourceLabel AND resource_type=:resource_name "
            sql += "AND agg_type=:agg_type AND agg_key=:agg_key"

            if workQueue.is_global_share:
                var_map[":agg_type"] = "gshare"
                var_map[":agg_key"] = workQueue.queue_name
            else:
                var_map[":agg_type"] = "workqueue"
                var_map[":agg_key"] = str(workQueue.queue_id)

            # start transaction
            self.conn.begin()
            self.cur.execute(sql + comment, var_map)
            totWalltime, nHasVal, nNoVal = 0, 0, 0
            try:
                tmpTotWalltime, tmpHasVal, tmpNoVal = self.cur.fetchone()
                if tmpTotWalltime is not None:
                    totWalltime = tmpTotWalltime
                if tmpHasVal is not None:
                    nHasVal = tmpHasVal
                if tmpNoVal is not None:
                    nNoVal = tmpNoVal
            except TypeError:  # there was no result
                pass

            tmpLog.debug(f"totWalltime={totWalltime} nHasVal={nHasVal} nNoVal={nNoVal}")
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            if nHasVal != 0:
                totWalltime = int(totWalltime * (1 + float(nNoVal) / float(nHasVal)))
            else:
                totWalltime = None
            tmpLog.debug(f"done totWalltime={totWalltime}")
            return totWalltime
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return None

    # check duplication with internal merge
    def checkDuplication_JEDI(self, jediTaskID):
        comment = " /* JediDBProxy.checkDuplication_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID}")
        tmpLog.debug("start")
        # sql to check useJumbo
        sqlJ = f"SELECT useJumbo FROM {panda_config.schemaJEDI}.JEDI_Tasks WHERE jediTaskID=:jediTaskID "
        # sql to get input datasetID
        sqlM = f"SELECT datasetID FROM {panda_config.schemaJEDI}.JEDI_Datasets "
        sqlM += "WHERE jediTaskID=:jediTaskID "
        sqlM += f"AND type IN ({INPUT_TYPES_var_str}) "
        sqlM += "AND masterID IS NULL "
        # sql to get output datasetID and templateID
        sqlO = f"SELECT datasetID,provenanceID FROM {panda_config.schemaJEDI}.JEDI_Datasets "
        sqlO += "WHERE jediTaskID=:jediTaskID AND type=:type "
        # sql to check duplication without internal merge
        sqlWM = "SELECT distinct outPandaID "
        sqlWM += f"FROM {panda_config.schemaJEDI}.JEDI_Dataset_Contents "
        sqlWM += "WHERE jediTaskID=:jediTaskID AND datasetID=:outDatasetID AND status IN (:statT1,:statT2) "
        sqlWM += "MINUS "
        sqlWM += "SELECT distinct PandaID "
        sqlWM += f"FROM {panda_config.schemaJEDI}.JEDI_Dataset_Contents "
        sqlWM += "WHERE jediTaskID=:jediTaskID AND datasetID=:inDatasetID AND status=:statI "
        # sql to check duplication with jumbo
        sqlJM = "WITH tmpTab AS ("
        sqlJM += f"SELECT f.fileID,f.PandaID FROM {panda_config.schemaPANDA}.filesTable4 f, ("
        sqlJM += f"SELECT PandaID FROM {panda_config.schemaJEDI}.JEDI_Dataset_Contents "
        sqlJM += "WHERE jediTaskID=:jediTaskID AND datasetID=:outDatasetID AND status IN (:statT1,:statT2)) t "
        sqlJM += "WHERE f.PandaID=t.PandaID AND f.datasetID=:inDatasetID "
        sqlJM += "UNION "
        sqlJM += f"SELECT f.fileID,f.PandaID FROM {panda_config.schemaPANDAARCH}.filesTable_Arch f, ("
        sqlJM += f"SELECT PandaID FROM {panda_config.schemaJEDI}.JEDI_Dataset_Contents "
        sqlJM += "WHERE jediTaskID=:jediTaskID AND datasetID=:outDatasetID AND status IN (:statT1,:statT2)) t "
        sqlJM += "WHERE f.PandaID=t.PandaID AND f.datasetID=:inDatasetID AND f.modificationTime>CURRENT_DATE-365 "
        sqlJM += ") "
        sqlJM += "SELECT t1.PandaID FROM tmpTab t1, tmpTab t2 WHERE t1.fileID=t2.fileID AND t1.PandaID>t2.PandaID "
        # sql to check duplication with internal merge
        sqlCM = "SELECT distinct c1.outPandaID "
        sqlCM += "FROM {0}.JEDI_Dataset_Contents c1,{0}.JEDI_Dataset_Contents c2,{0}.JEDI_Datasets d ".format(panda_config.schemaJEDI)
        sqlCM += "WHERE d.jediTaskID=:jediTaskID AND c1.jediTaskID=d.jediTaskID AND c1.datasetID=d.datasetID AND d.templateID=:templateID "
        sqlCM += "AND c1.jediTaskID=c2.jediTaskID AND c2.datasetID=:outDatasetID AND c1.pandaID=c2.pandaID and c2.status IN (:statT1,:statT2) "
        sqlCM += "MINUS "
        sqlCM += "SELECT distinct PandaID "
        sqlCM += f"FROM {panda_config.schemaJEDI}.JEDI_Dataset_Contents "
        sqlCM += "WHERE jediTaskID=:jediTaskID AND datasetID=:inDatasetID and status=:statI "
        try:
            # start transaction
            self.conn.begin()
            # check useJumbo
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            self.cur.execute(sqlJ + comment, varMap)
            resJ = self.cur.fetchone()
            (useJumbo,) = resJ
            # get input datasetID
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            varMap.update(INPUT_TYPES_var_map)
            self.cur.execute(sqlM + comment, varMap)
            resM = self.cur.fetchone()
            if resM is not None:
                (inDatasetID,) = resM
                # get output datasetID and templateID
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                varMap[":type"] = "output"
                self.cur.execute(sqlO + comment, varMap)
                resO = self.cur.fetchone()
                if resO is None:
                    # no output
                    retVal = 0
                else:
                    outDatasetID, templateID = resO
                    # check duplication
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    varMap[":inDatasetID"] = inDatasetID
                    varMap[":outDatasetID"] = outDatasetID
                    varMap[":statI"] = "finished"
                    varMap[":statT1"] = "finished"
                    varMap[":statT2"] = "nooutput"
                    if templateID is not None:
                        # with internal merge
                        varMap[":templateID"] = templateID
                        self.cur.execute(sqlCM + comment, varMap)
                    else:
                        if useJumbo is None:
                            # without internal merge
                            self.cur.execute(sqlWM + comment, varMap)
                        else:
                            # with jumbo
                            del varMap[":statI"]
                            self.cur.execute(sqlJM + comment, varMap)
                    retList = self.cur.fetchall()
                    dupPandaIDs = []
                    for (dupPandaID,) in retList:
                        dupPandaIDs.append(dupPandaID)
                        tmpLog.debug(f"bad PandaID={dupPandaID}")
                    retVal = len(dupPandaIDs)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmpLog.debug(f"dup={retVal}")
            return retVal
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return None

    # get failure counts for a task
    def getFailureCountsForTask_JEDI(self, jediTaskID, timeWindow):
        comment = " /* JediDBProxy.getFailureCountsForTask_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID}")
        tmpLog.debug("start")
        try:
            # sql
            sql = "SELECT COUNT(*),computingSite,jobStatus "
            sql += f"FROM {panda_config.schemaPANDA}.jobsArchived4 "
            sql += f"WHERE jediTaskID=:jediTaskID AND modificationTime>CURRENT_DATE-{timeWindow}/24 "
            sql += "AND ("
            sql += "(jobStatus=:jobFailed AND pilotErrorCode IS NOT NULL AND pilotErrorCode<>0) OR "
            sql += "(jobStatus=:jobClosed AND jobSubStatus=:toReassign AND relocationFlag<>:relThrottled) OR "
            sql += "(jobStatus=:jobFinished) "
            sql += ") "
            sql += "GROUP BY computingSite,jobStatus "
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            varMap[":jobClosed"] = "closed"
            varMap[":jobFailed"] = "failed"
            varMap[":jobFinished"] = "finished"
            varMap[":toReassign"] = "toreassign"
            varMap[":relThrottled"] = 3
            # start transaction
            self.conn.begin()
            self.cur.execute(sql + comment, varMap)
            resList = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # make dict
            retMap = {}
            for cnt, computingSite, jobStatus in resList:
                if computingSite not in retMap:
                    retMap[computingSite] = {}
                if jobStatus not in retMap[computingSite]:
                    retMap[computingSite][jobStatus] = 0
                retMap[computingSite][jobStatus] += cnt
            tmpLog.debug(str(retMap))
            return retMap
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return {}

    # count the number of jobs and cores per user or working group
    def countJobsPerTarget_JEDI(self, target, is_user):
        comment = " /* JediDBProxy.countJobsPerTarget_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"target={target}")
        tmpLog.debug("start")
        try:
            # sql
            sql = "SELECT COUNT(*),SUM(coreCount),jobStatus FROM ("
            sql += f"SELECT PandaID,jobStatus,coreCount FROM {panda_config.schemaPANDA}.jobsDefined4 "
            if is_user:
                sql += "WHERE prodUserName=:target "
            else:
                sql += "WHERE workingGroup=:target "
            sql += "UNION "
            sql += f"SELECT PandaID,jobStatus,coreCount FROM {panda_config.schemaPANDA}.jobsActive4 "
            if is_user:
                sql += "WHERE prodUserName=:target AND workingGroup IS NULL "
            else:
                sql += "WHERE workingGroup=:target "
            sql += ") GROUP BY jobStatus "
            varMap = {}
            varMap[":target"] = target
            # start transaction
            self.conn.begin()
            self.cur.execute(sql + comment, varMap)
            resList = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # make dict
            retMap = {"nQueuedJobs": 0, "nQueuedCores": 0, "nRunJobs": 0, "nRunCores": 0}
            for nJobs, nCores, jobStatus in resList:
                if jobStatus in ["defined", "assigned", "activated", "starting", "throttled"]:
                    retMap["nQueuedJobs"] += nJobs
                    retMap["nQueuedCores"] += nCores
                elif jobStatus in ["running"]:
                    retMap["nRunJobs"] += nJobs
                    retMap["nRunCores"] += nCores
            tmpLog.debug(str(retMap))
            return retMap
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return {}

    # get old merge job PandaIDs
    def getOldMergeJobPandaIDs_JEDI(self, jediTaskID, pandaID):
        comment = " /* JediDBProxy.getOldMergeJobPandaIDs_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID} PandaID={pandaID}")
        tmpLog.debug("start")
        try:
            # sql
            sql = "SELECT distinct tabC.PandaID "
            sql += "FROM {0}.JEDI_Datasets tabD,{0}.JEDI_Dataset_Contents tabC ".format(panda_config.schemaJEDI)
            sql += "WHERE tabD.jediTaskID=:jediTaskID AND tabD.jediTaskID=tabC.jediTaskID "
            sql += "AND tabD.datasetID=tabC.datasetID "
            sql += "AND tabD.type=:dsType AND tabC.outPandaID=:pandaID "
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            varMap[":pandaID"] = pandaID
            varMap[":dsType"] = "trn_log"
            # start transaction
            self.conn.begin()
            self.cur.arraysize = 10000
            self.cur.execute(sql + comment, varMap)
            resList = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            retVal = []
            for (tmpPandaID,) in resList:
                if tmpPandaID != pandaID:
                    retVal.append(tmpPandaID)
            tmpLog.debug(str(retVal))
            return retVal
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return []

    # get jobParms of the first job
    def getJobParamsOfFirstJob_JEDI(self, jediTaskID):
        comment = " /* JediDBProxy.getJobParamsOfFirstJob_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID}")
        tmpLog.debug("start")
        try:
            retVal = None
            outFileMap = dict()
            # sql to get PandaID of the first job
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            sql = "SELECT * FROM ("
            sql += "SELECT tabF.datasetID,tabF.fileID "
            sql += "FROM {0}.JEDI_Datasets tabD, {0}.JEDI_Dataset_Contents tabF ".format(panda_config.schemaJEDI)
            sql += "WHERE tabD.jediTaskID=tabF.jediTaskID AND tabD.jediTaskID=:jediTaskID "
            sql += "AND tabD.datasetID=tabF.datasetID "
            sql += "AND tabD.masterID IS NULL "
            sql += f"AND tabF.type IN ({INPUT_TYPES_var_str}) "
            varMap.update(INPUT_TYPES_var_map)
            sql += "ORDER BY fileID "
            sql += ") WHERE rownum<2 "
            # sql to get PandaIDs
            sqlP = f"SELECT PandaID FROM {panda_config.schemaPANDA}.filesTable4 WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
            sqlPA = "SELECT PandaID FROM {0}.filesTable_arch WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID ".format(
                panda_config.schemaPANDAARCH
            )
            # sql to get jobParms
            sqlJ = f"SELECT jobParameters FROM {panda_config.schemaPANDA}.jobParamsTable WHERE PandaID=:PandaID "
            sqlJA = f"SELECT jobParameters FROM {panda_config.schemaPANDAARCH}.jobParamsTable_ARCH WHERE PandaID=:PandaID"
            # sql to get file
            sqlF = f"SELECT lfn,datasetID FROM {panda_config.schemaPANDA}.filesTable4 where PandaID=:PandaID AND type=:type "
            sqlFA = f"SELECT lfn,datasetID FROM {panda_config.schemaPANDAARCH}.filesTable_Arch where PandaID=:PandaID AND type=:type "
            # start transaction
            self.conn.begin()
            self.cur.execute(sql + comment, varMap)
            res = self.cur.fetchone()
            if res is not None:
                datasetID, fileID = res
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                varMap[":datasetID"] = datasetID
                varMap[":fileID"] = fileID
                self.cur.execute(sqlP + comment, varMap)
                resP = self.cur.fetchone()
                if resP is None:
                    self.cur.execute(sqlPA + comment, varMap)
                    resP = self.cur.fetchone()
                (pandaID,) = resP
                varMap = {}
                varMap[":PandaID"] = pandaID
                self.cur.execute(sqlJ + comment, varMap)
                for (clobJobP,) in self.cur:
                    retVal = clobJobP
                    break
                if retVal is None:
                    self.cur.execute(sqlJA + comment, varMap)
                    for (clobJobP,) in self.cur:
                        retVal = clobJobP
                        break
                # get output
                varMap = dict()
                varMap[":PandaID"] = pandaID
                varMap[":type"] = "output"
                self.cur.execute(sqlF + comment, varMap)
                resF = self.cur.fetchall()
                if len(resF) == 0:
                    self.cur.execute(sqlFA + comment, varMap)
                    resF = self.cur.fetchall()
                for lfn, datasetID in resF:
                    outFileMap[datasetID] = lfn
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmpLog.debug(f"get {len(retVal)} bytes")
            return retVal, outFileMap
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return None, None

    # bulk fetch fileIDs
    def bulkFetchFileIDs_JEDI(self, jediTaskID, nIDs):
        comment = " /* JediDBProxy.bulkFetchFileIDs_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID} nIDs={nIDs}")
        tmpLog.debug("start")
        try:
            newFileIDs = []
            varMap = {}
            varMap[":nIDs"] = nIDs
            # sql to get fileID
            sqlFID = f"SELECT {panda_config.schemaJEDI}.JEDI_DATASET_CONT_FILEID_SEQ.nextval FROM "
            sqlFID += "(SELECT level FROM dual CONNECT BY level<=:nIDs) "
            # start transaction
            self.conn.begin()
            self.cur.arraysize = 10000
            self.cur.execute(sqlFID + comment, varMap)
            resFID = self.cur.fetchall()
            for (fileID,) in resFID:
                newFileIDs.append(fileID)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmpLog.debug(f"got {len(newFileIDs)} IDs")
            return newFileIDs
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return []

    # set del flag to events
    def setDelFlagToEvents_JEDI(self, jediTaskID):
        comment = " /* JediDBProxy.setDelFlagToEvents_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID}")
        tmpLog.debug("start")
        try:
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            varMap[":delFlag"] = "Y"
            # sql to set del flag
            sqlFID = f"UPDATE /*+ INDEX_RS_ASC(JEDI_EVENTS JEDI_EVENTS_PK) */ {panda_config.schemaJEDI}.JEDI_Events "
            sqlFID += "SET file_not_deleted=:delFlag "
            sqlFID += "WHERE jediTaskID=:jediTaskID AND file_not_deleted IS NULL AND objStore_ID IS NOT NULL "
            # start transaction
            self.conn.begin()
            self.cur.execute(sqlFID + comment, varMap)
            nRow = self.cur.rowcount
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmpLog.debug(f"set Y to {nRow} event ranges")
            return nRow
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return None

    # set del flag to events
    def removeFilesIndexInconsistent_JEDI(self, jediTaskID, datasetIDs):
        comment = " /* JediDBProxy.removeFilesIndexInconsistent_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID}")
        tmpLog.debug("start")
        try:
            # sql to get files
            sqlFID = f"SELECT lfn,fileID FROM {panda_config.schemaJEDI}.JEDI_Dataset_Contents "
            sqlFID += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            # start transaction
            self.conn.begin()
            # get files
            lfnMap = {}
            for datasetID in datasetIDs:
                if datasetID not in lfnMap:
                    lfnMap[datasetID] = {}
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                varMap[":datasetID"] = datasetID
                self.cur.execute(sqlFID + comment, varMap)
                tmpRes = self.cur.fetchall()
                for lfn, fileID in tmpRes:
                    items = lfn.split(".")
                    if len(items) < 3:
                        continue
                    idx = items[1] + items[2]
                    if idx not in lfnMap[datasetID]:
                        lfnMap[datasetID][idx] = []
                    lfnMap[datasetID][idx].append(fileID)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # find common elements
            datasetID = datasetIDs[0]
            commonIdx = set(lfnMap[datasetID].keys())
            for datasetID in datasetIDs[1:]:
                commonIdx = commonIdx & set(lfnMap[datasetID].keys())
            tmpLog.debug(f"{len(commonIdx)} common files")
            # sql to remove uncommon
            sqlRF = f"UPDATE {panda_config.schemaJEDI}.JEDI_Dataset_Contents "
            sqlRF += "SET status=:newStatus "
            sqlRF += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
            sqlRF += "AND status=:oldStatus "
            # sql to count files
            sqlCF = f"SELECT COUNT(*) FROM {panda_config.schemaJEDI}.JEDI_Dataset_Contents "
            sqlCF += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND status<>:status "
            # sql to update nFiles
            sqlUD = f"UPDATE {panda_config.schemaJEDI}.JEDI_Datasets "
            sqlUD += "SET nFiles=:nFiles,nFilesTobeUsed=:nFilesTobeUsed "
            sqlUD += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            self.conn.begin()
            # remove uncommon
            for datasetID in datasetIDs:
                nLost = 0
                for idx, fileIDs in lfnMap[datasetID].items():
                    if idx not in commonIdx:
                        for fileID in fileIDs:
                            varMap = {}
                            varMap[":jediTaskID"] = jediTaskID
                            varMap[":datasetID"] = datasetID
                            varMap[":fileID"] = fileID
                            varMap[":oldStatus"] = "ready"
                            varMap[":newStatus"] = "lost"
                            self.cur.execute(sqlRF + comment, varMap)
                            nRow = self.cur.rowcount
                            if nRow > 0:
                                nLost += 1
                tmpLog.debug(f"set {nLost} files to lost for datasetID={datasetID}")
                # count files
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                varMap[":datasetID"] = datasetID
                varMap[":status"] = "lost"
                self.cur.execute(sqlCF + comment, varMap)
                (nFiles,) = self.cur.fetchone()
                # update nFiles
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                varMap[":datasetID"] = datasetID
                varMap[":nFiles"] = nFiles
                varMap[":nFilesTobeUsed"] = nFiles
                self.cur.execute(sqlUD + comment, varMap)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            return True
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return False

    # throttle jobs in pauses tasks
    def throttleJobsInPausedTasks_JEDI(self, vo, prodSourceLabel):
        comment = " /* JediDBProxy.throttleJobsInPausedTasks_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"vo={vo} label={prodSourceLabel}")
        tmpLog.debug("start")
        try:
            # sql to get tasks
            varMap = {}
            varMap[":status"] = "paused"
            varMap[":timeLimit"] = naive_utcnow() - datetime.timedelta(minutes=10)
            sqlTL = "SELECT jediTaskID "
            sqlTL += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_AUX_Status_MinTaskID tabA ".format(panda_config.schemaJEDI)
            sqlTL += "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID "
            sqlTL += "AND tabT.status=:status AND tabT.modificationTime<:timeLimit AND tabT.lockedBy IS NULL "
            if vo not in [None, "any"]:
                varMap[":vo"] = vo
                sqlTL += "AND vo=:vo "
            if prodSourceLabel not in [None, "any"]:
                varMap[":prodSourceLabel"] = prodSourceLabel
                sqlTL += "AND prodSourceLabel=:prodSourceLabel "
            # sql to update tasks
            sqlTU = f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks "
            sqlTU += "SET modificationtime=CURRENT_DATE "
            sqlTU += "WHERE jediTaskID=:jediTaskID AND status=:status AND lockedBy IS NULL "
            # sql to throttle jobs
            sqlJT = f"UPDATE {panda_config.schemaPANDA}.jobsActive4 "
            sqlJT += "SET jobStatus=:newJobStatus "
            sqlJT += "WHERE jediTaskID=:jediTaskID AND jobStatus=:oldJobStatus "
            # sql to get jobs in jobsDefined
            sqlJD = f"SELECT PandaID FROM {panda_config.schemaPANDA}.jobsDefined4 "
            sqlJD += "WHERE jediTaskID=:jediTaskID "
            # start transaction
            self.conn.begin()
            tmpLog.debug(sqlTL + comment + str(varMap))
            self.cur.execute(sqlTL + comment, varMap)
            resTL = self.cur.fetchall()
            # loop over all tasks
            retMap = {}
            for (jediTaskID,) in resTL:
                retMap[jediTaskID] = set()
                # lock task
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                varMap[":status"] = "paused"
                self.cur.execute(sqlTU + comment, varMap)
                iRow = self.cur.rowcount
                if iRow > 0:
                    # throttle jobs
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    varMap[":newJobStatus"] = "throttled"
                    varMap[":oldJobStatus"] = "activated"
                    self.cur.execute(sqlJT + comment, varMap)
                    iRow = self.cur.rowcount
                    tmpLog.debug(f"throttled {iRow} jobs for jediTaskID={jediTaskID}")
                # get jobs
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                self.cur.execute(sqlJD + comment, varMap)
                resJD = self.cur.fetchall()
                for (tmpPandaID,) in resJD:
                    retMap[jediTaskID].add(tmpPandaID)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            tmpLog.debug("done")
            return retMap
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return None

    # get number of jobs for a task
    def getNumJobsForTask_JEDI(self, jediTaskID):
        comment = " /* JediDBProxy.getNumJobsForTask_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID}")
        tmpLog.debug("start")
        try:
            # get num of done jobs
            varMap = dict()
            varMap[":jediTaskID"] = jediTaskID
            sql = "SELECT COUNT(*) FROM ("
            sql += "SELECT distinct c.PandaID "
            sql += "FROM {0}.JEDI_Datasets d,{0}.JEDI_Dataset_Contents c ".format(panda_config.schemaJEDI)
            sql += "WHERE c.jediTaskID=d.jediTaskID AND c.datasetID=d.datasetID "
            sql += "AND d.jediTaskID=:jediTaskID AND d.masterID IS NULL "
            sql += f"AND d.type IN ({INPUT_TYPES_var_str}) "
            varMap.update(INPUT_TYPES_var_map)
            sql += ") "
            # start transaction
            self.conn.begin()
            self.cur.execute(sql + comment, varMap)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            (nDone,) = self.cur.fetchone()
            # return
            tmpLog.debug(f"got {nDone} jobs")
            return nDone
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return None

    # get number map for standby jobs
    def getNumMapForStandbyJobs_JEDI(self, workqueue):
        comment = " /* JediDBProxy.getNumMapForStandbyJobs_JEDI */"
        tmpLog = self.create_tagged_logger(comment)
        tmpLog.debug("start")
        try:
            retMapStatic = dict()
            retMapDynamic = dict()
            # get num of done jobs
            varMap = dict()
            varMap[":status"] = "standby"
            sql = f"SELECT /* use_json_type */ panda_queue, scj.data.catchall FROM {panda_config.schemaJEDI}.schedconfig_json scj "
            sql += "WHERE scj.data.status=:status "
            self.conn.begin()
            self.cur.arraysize = 1000
            self.cur.execute(sql + comment, varMap)
            resList = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # sum per gshare/workqueue and resource type
            for siteid, catchall in resList:
                numMap = JobUtils.parseNumStandby(catchall)
                if numMap is not None:
                    for wq_tag, resource_num in numMap.items():
                        if workqueue.is_global_share:
                            if workqueue.queue_name != wq_tag:
                                continue
                        else:
                            if str(workqueue.queue_id) != wq_tag:
                                continue
                        for resource_type, num in resource_num.items():
                            if num == 0:
                                retMap = retMapDynamic
                                # dynamic : use # of starting jobs as # of standby jobs
                                varMap = dict()
                                varMap[":vo"] = workqueue.VO
                                varMap[":status"] = "starting"
                                varMap[":resource_type"] = resource_type
                                varMap[":computingsite"] = siteid
                                sql = f"SELECT /*+ RESULT_CACHE */ SUM(njobs) FROM {panda_config.schemaPANDA}.JOBS_SHARE_STATS "
                                sql += "WHERE vo=:vo AND resource_type=:resource_type AND jobstatus=:status AND computingsite=:computingsite "
                                if workqueue.is_global_share:
                                    sql += "AND gshare=:gshare "
                                    sql += "AND workqueue_id NOT IN (SELECT queue_id FROM {0}.jedi_work_queue WHERE queue_function=:func) ".format(
                                        panda_config.schemaPANDA
                                    )
                                    varMap[":gshare"] = workqueue.queue_name
                                    varMap[":func"] = "Resource"
                                else:
                                    sql += "AND workqueue_id=:workqueue_id "
                                    varMap[":workqueue_id"] = workqueue.queue_id
                                self.cur.execute(sql, varMap)
                                res = self.cur.fetchone()
                                if res is None:
                                    num = 0
                                else:
                                    (num,) = res
                            else:
                                retMap = retMapStatic
                            if resource_type not in retMap:
                                retMap[resource_type] = 0
                            if num:
                                retMap[resource_type] += num
            # return
            tmpLog.debug(f"got static={str(retMapStatic)} dynamic={str(retMapDynamic)}")
            return retMapStatic, retMapDynamic
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return {}, {}

    # update datasets to finish a task
    def updateDatasetsToFinishTask_JEDI(self, jediTaskID, lockedBy):
        comment = " /* JediDBProxy.updateDatasetsToFinishTask_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID}")
        tmpLog.debug("start")
        try:
            # sql to lock task
            sqlLK = f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks SET lockedBy=:lockedBy,lockedTime=CURRENT_DATE "
            sqlLK += "WHERE jediTaskID=:jediTaskID AND lockedBy IS NULL "
            # sql to get datasets
            sqlAV = f"SELECT datasetID FROM {panda_config.schemaJEDI}.JEDI_Datasets "
            sqlAV += f"WHERE jediTaskID=:jediTaskID AND type IN ({INPUT_TYPES_var_str}) "
            # sql to update attemptNr for files
            sqlFR = f"UPDATE {panda_config.schemaJEDI}.JEDI_Dataset_Contents "
            sqlFR += "SET attemptNr=maxAttempt "
            sqlFR += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            sqlFR += "AND status=:status AND keepTrack=:keepTrack "
            sqlFR += "AND maxAttempt IS NOT NULL AND attemptNr<maxAttempt "
            sqlFR += "AND (maxFailure IS NULL OR failedAttempt<maxFailure) "
            # sql to update output/lib/log datasets
            sqlUO = f"UPDATE {panda_config.schemaJEDI}.JEDI_Datasets "
            sqlUO += "SET nFilesFailed=nFilesFailed+:nDiff "
            sqlUO += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            # sql to release task
            sqlRT = f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks SET lockedBy=NULL,lockedTime=NULL "
            sqlRT += "WHERE jediTaskID=:jediTaskID AND lockedBy=:lockedBy "
            # lock task
            self.conn.begin()
            varMap = dict()
            varMap[":jediTaskID"] = jediTaskID
            varMap[":lockedBy"] = lockedBy
            self.cur.execute(sqlLK + comment, varMap)
            iRow = self.cur.rowcount
            if iRow == 0:
                # commit
                if not self._commit():
                    raise RuntimeError("Commit error")
                tmpLog.debug("cannot lock")
                return False
            # get datasets
            varMap = dict()
            varMap[":jediTaskID"] = jediTaskID
            varMap.update(INPUT_TYPES_var_map)
            self.cur.execute(sqlAV + comment, varMap)
            resAV = self.cur.fetchall()
            for (datasetID,) in resAV:
                # update files
                varMap = dict()
                varMap[":jediTaskID"] = jediTaskID
                varMap[":datasetID"] = datasetID
                varMap[":status"] = "ready"
                varMap[":keepTrack"] = 1
                self.cur.execute(sqlFR + comment, varMap)
                nDiff = self.cur.rowcount
                if nDiff > 0:
                    varMap = dict()
                    varMap[":jediTaskID"] = jediTaskID
                    varMap[":datasetID"] = datasetID
                    varMap[":nDiff"] = nDiff
                    tmpLog.debug(sqlUO + comment + str(varMap))
                    self.cur.execute(sqlUO + comment, varMap)
            # release task
            varMap = dict()
            varMap[":jediTaskID"] = jediTaskID
            varMap[":lockedBy"] = lockedBy
            self.cur.execute(sqlRT + comment, varMap)
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

    # get number of staging files
    def getNumStagingFiles_JEDI(self, jeditaskid):
        comment = " /* JediDBProxy.getNumStagingFiles_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"jediTaskID={jeditaskid}")
        tmpLog.debug("start")
        try:
            retVal = 0
            # varMap
            varMap = dict()
            varMap[":jediTaskID"] = jeditaskid
            varMap[":type1"] = "input"
            varMap[":type2"] = "pseudo_input"
            varMap[":status"] = "staging"
            # sql
            sqlNS = (
                "SELECT COUNT(*) FROM {0}.JEDI_Datasets d, {0}.JEDI_Dataset_Contents c "
                "WHERE d.jediTaskID=:jediTaskID AND d.type IN (:type1,:type2) "
                "AND c.jediTaskID=d.jediTaskID AND c.datasetID=d.datasetID "
                "AND c.status=:status "
            ).format(panda_config.schemaJEDI)
            # begin transaction
            self.conn.begin()
            self.cur.execute(sqlNS + comment, varMap)
            (retVal,) = self.cur.fetchone()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmpLog.debug(f"got {retVal} staging files")
            return retVal
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return None

    # get usage breakdown by users and sites
    def getUsageBreakdown_JEDI(self, prod_source_label="user"):
        comment = " /* JediDBProxy.getUsageBreakdown_JEDI */"
        tmpLog = self.create_tagged_logger(comment)
        tmpLog.debug("start")
        try:
            # get usage breakdown
            usageBreakDownPerUser = {}
            usageBreakDownPerSite = {}
            for table in ["jobsActive4", "jobsArchived4"]:
                varMap = {}
                varMap[":prodSourceLabel"] = prod_source_label
                varMap[":pmerge"] = "pmerge"
                if table == "ATLAS_PANDA.jobsActive4":
                    sqlJ = (
                        "SELECT COUNT(*),prodUserName,jobStatus,workingGroup,computingSite,coreCount "
                        "FROM {0}.{1} "
                        "WHERE prodSourceLabel=:prodSourceLabel AND processingType<>:pmerge "
                        "GROUP BY prodUserName,jobStatus,workingGroup,computingSite,coreCount "
                    ).format(panda_config.schemaPANDA, table)
                else:
                    # with time range for archived table
                    varMap[":modificationTime"] = naive_utcnow() - datetime.timedelta(minutes=60)
                    sqlJ = (
                        "SELECT COUNT(*),prodUserName,jobStatus,workingGroup,computingSite,coreCount "
                        "FROM {0}.{1} "
                        "WHERE prodSourceLabel=:prodSourceLabel AND processingType<>:pmerge AND modificationTime>:modificationTime "
                        "GROUP BY prodUserName,jobStatus,workingGroup,computingSite,coreCount "
                    ).format(panda_config.schemaPANDA, table)
                # exec
                tmpLog.debug(sqlJ + comment + str(varMap))
                self.cur.execute(sqlJ + comment, varMap)
                # result
                res = self.cur.fetchall()
                if res is None:
                    tmpLog.debug(f"total {res} ")
                else:
                    tmpLog.debug(f"total {len(res)} ")
                    # make map
                    for cnt, prodUserName, jobStatus, workingGroup, computingSite, coreCount in res:
                        if coreCount is None:
                            coreCount = 1
                        # append to PerUser map
                        usageBreakDownPerUser.setdefault(prodUserName, {})
                        usageBreakDownPerUser[prodUserName].setdefault(workingGroup, {})
                        usageBreakDownPerUser[prodUserName][workingGroup].setdefault(computingSite, {"rundone": 0, "activated": 0, "running": 0, "runcores": 0})
                        # append to PerSite map
                        usageBreakDownPerSite.setdefault(computingSite, {})
                        usageBreakDownPerSite[computingSite].setdefault(prodUserName, {})
                        usageBreakDownPerSite[computingSite][prodUserName].setdefault(workingGroup, {"rundone": 0, "activated": 0})
                        # count # of running/done and activated
                        if jobStatus in ["activated"]:
                            usageBreakDownPerUser[prodUserName][workingGroup][computingSite]["activated"] += cnt
                            usageBreakDownPerSite[computingSite][prodUserName][workingGroup]["activated"] += cnt
                        elif jobStatus in ["cancelled", "holding"]:
                            pass
                        else:
                            if jobStatus in ["running", "starting", "sent"]:
                                usageBreakDownPerUser[prodUserName][workingGroup][computingSite]["running"] += cnt
                                usageBreakDownPerUser[prodUserName][workingGroup][computingSite]["runcores"] += cnt * coreCount
                            usageBreakDownPerUser[prodUserName][workingGroup][computingSite]["rundone"] += cnt
                            usageBreakDownPerSite[computingSite][prodUserName][workingGroup]["rundone"] += cnt
            # return
            tmpLog.debug("done")
            return usageBreakDownPerUser, usageBreakDownPerSite
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return None

    # get jobs stat of each user
    def getUsersJobsStats_JEDI(self, prod_source_label="user"):
        comment = " /* JediDBProxy.getUsersJobsStats_JEDI */"
        tmpLog = self.create_tagged_logger(comment)
        tmpLog.debug("start")
        try:
            # get users jobs stats
            jobsStatsPerUser = {}
            varMap = {}
            varMap[":prodSourceLabel"] = prod_source_label
            varMap[":pmerge"] = "pmerge"
            sqlJ = (
                "SELECT COUNT(*),prodUserName,jobStatus,gshare,computingSite "
                "FROM {0}.{1} "
                "WHERE prodSourceLabel=:prodSourceLabel AND processingType<>:pmerge "
                "GROUP BY prodUserName,jobStatus,gshare,computingSite "
            ).format(panda_config.schemaPANDA, "jobsActive4")
            # exec
            tmpLog.debug(sqlJ + comment + str(varMap))
            self.cur.execute(sqlJ + comment, varMap)
            # result
            res = self.cur.fetchall()
            if res is None:
                tmpLog.debug(f"total {res} ")
            else:
                tmpLog.debug(f"total {len(res)} ")
                # make map
                for cnt, prodUserName, jobStatus, gshare, computingSite in res:
                    # append to PerUser map
                    jobsStatsPerUser.setdefault(computingSite, {})
                    jobsStatsPerUser[computingSite].setdefault(gshare, {})
                    jobsStatsPerUser[computingSite][gshare].setdefault(
                        prodUserName, {"nDefined": 0, "nAssigned": 0, "nActivated": 0, "nStarting": 0, "nQueue": 0, "nRunning": 0}
                    )
                    jobsStatsPerUser[computingSite][gshare].setdefault(
                        "_total", {"nDefined": 0, "nAssigned": 0, "nActivated": 0, "nStarting": 0, "nQueue": 0, "nRunning": 0}
                    )
                    # count # of running/done and activated
                    if jobStatus in ["defined", "assigned", "activated", "starting"]:
                        status_name = f"n{jobStatus.capitalize()}"
                        jobsStatsPerUser[computingSite][gshare][prodUserName][status_name] += cnt
                        jobsStatsPerUser[computingSite][gshare][prodUserName]["nQueue"] += cnt
                        jobsStatsPerUser[computingSite][gshare]["_total"][status_name] += cnt
                        jobsStatsPerUser[computingSite][gshare]["_total"]["nQueue"] += cnt
                    elif jobStatus in ["running"]:
                        jobsStatsPerUser[computingSite][gshare][prodUserName]["nRunning"] += cnt
                        jobsStatsPerUser[computingSite][gshare]["_total"]["nRunning"] += cnt
            # return
            tmpLog.debug("done")
            return jobsStatsPerUser
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return None

    # insert HPO pseudo event according to message from idds
    def insertHpoEventAboutIdds_JEDI(self, jedi_task_id, event_id_list):
        comment = " /* JediDBProxy.insertHpoEventAboutIdds_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"jediTaskID={jedi_task_id}")
        tmpLog.debug(f"start event_id_list={event_id_list}")
        varMap = dict()
        varMap[":jediTaskID"] = jedi_task_id
        varMap[":modificationHost"] = socket.getfqdn()
        # sql
        sqlJediEvent = (
            "INSERT INTO {0}.JEDI_Events "
            "(jediTaskID,datasetID,PandaID,fileID,attemptNr,status,"
            "job_processID,def_min_eventID,def_max_eventID,processed_upto_eventID,"
            "event_offset) "
            "VALUES(:jediTaskID,"
            "(SELECT datasetID FROM {0}.JEDI_Datasets "
            "WHERE jediTaskID=:jediTaskID AND type=:type AND masterID IS NULL AND containerName LIKE :cont),"
            ":pandaID,:fileID,:attemptNr,:eventStatus,"
            ":startEvent,:startEvent,:lastEvent,:processedEvent,"
            ":eventOffset) "
        ).format(panda_config.schemaJEDI)
        varMaps = []
        n_events = 0
        for event_id, model_id in event_id_list:
            varMap = dict()
            varMap[":jediTaskID"] = jedi_task_id
            varMap[":type"] = "pseudo_input"
            varMap[":pandaID"] = 0
            varMap[":fileID"] = 0
            varMap[":attemptNr"] = 5
            varMap[":eventStatus"] = EventServiceUtils.ST_ready
            varMap[":processedEvent"] = 0
            varMap[":startEvent"] = event_id
            varMap[":lastEvent"] = event_id
            varMap[":eventOffset"] = 0
            varMap[":cont"] = f"%/{model_id}"
            varMaps.append(varMap)
            n_events += 1
        try:
            self.conn.begin()
            self.cur.executemany(sqlJediEvent + comment, varMaps)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmpLog.debug(f"added {n_events} events")
            return True
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return False

    # get event statistics
    def get_event_statistics(self, jedi_task_id):
        comment = " /* JediDBProxy.get_event_statistics */"
        tmpLog = self.create_tagged_logger(comment, f"jediTaskID={jedi_task_id}")
        tmpLog.debug("start")
        try:
            self.conn.begin()
            varMap = dict()
            varMap[":jediTaskID"] = jedi_task_id
            # sql
            sqlGNE = f"SELECT status,COUNT(*) FROM {panda_config.schemaJEDI}.JEDI_Events WHERE jediTaskID=:jediTaskID GROUP BY status "
            self.cur.execute(sqlGNE + comment, varMap)
            # result
            ret_dict = dict()
            res = self.cur.fetchall()
            for s, c in res:
                ret_dict[s] = c
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            tmpLog.debug(f"got {str(ret_dict)}")
            return ret_dict
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return None

    # get site to-running rate statistics
    def getSiteToRunRateStats(self, vo, exclude_rwq, starttime_min, starttime_max):
        """
        :param vo: Virtual Organization
        :param exclude_rwq: True/False. Indicates whether we want to indicate special workqueues from the statistics
        :param starttime_min: float, min start time in hours to compute to-running rate
        :param starttime_max: float, max start time in hours to compute to-running rate
        """
        comment = " /* DBProxy.getSiteToRunRateStats */"
        tmpLog = self.create_tagged_logger(comment, f"vo={vo}")
        tmpLog.debug("start")
        # interval in hours
        real_interval_hours = (starttime_max - starttime_min).total_seconds() / 3600
        # define the var map of query parameters
        var_map = {":vo": vo, ":startTimeMin": starttime_min, ":startTimeMax": starttime_max}
        # sql to query on jobs-tables (jobsactive4 and jobsdefined4)
        sql_jt = """
               SELECT computingSite, COUNT(*) FROM %s
               WHERE vo=:vo
               AND startTime IS NOT NULL AND startTime>=:startTimeMin AND startTime<:startTimeMax
               AND jobStatus IN ('running', 'holding', 'transferring', 'finished', 'cancelled')
               """
        if exclude_rwq:
            sql_jt += f"""
               AND workqueue_id NOT IN
               (SELECT queue_id FROM {panda_config.schemaPANDA}.jedi_work_queue WHERE queue_function = 'Resource')
               """
        sql_jt += """
               GROUP BY computingSite
               """
        # job tables
        tables = [f"{panda_config.schemaPANDA}.jobsActive4", f"{panda_config.schemaPANDA}.jobsDefined4"]
        # get
        return_map = {}
        try:
            for table in tables:
                self.cur.arraysize = 10000
                sql_exe = (sql_jt + comment) % table
                self.cur.execute(sql_exe, var_map)
                res = self.cur.fetchall()
                # create map
                for panda_site, n_count in res:
                    # add site
                    return_map.setdefault(panda_site, 0)
                    # increase to-running rate
                    to_running_rate = n_count / real_interval_hours if real_interval_hours > 0 else 0
                    return_map[panda_site] += to_running_rate
            # end loop
            tmpLog.debug("done")
            return True, return_map
        except Exception:
            self.dump_error_message(tmpLog)
            return False, {}

    # update cache
    def updateCache_JEDI(self, main_key, sub_key, data):
        comment = " /* JediDBProxy.updateCache_JEDI */"
        # defaults
        if sub_key is None:
            sub_key = "default"
        # last update time
        last_update = naive_utcnow()
        last_update_str = last_update.strftime("%Y-%m-%d_%H:%M:%S")
        tmpLog = self.create_tagged_logger(comment, f"main_key={main_key} sub_key={sub_key} last_update={last_update_str}")
        tmpLog.debug("start")
        try:
            retVal = False
            # sql to check
            sqlC = f"SELECT last_update FROM {panda_config.schemaJEDI}.Cache WHERE main_key=:main_key AND sub_key=:sub_key "
            # sql to insert
            sqlI = f"INSERT INTO {panda_config.schemaJEDI}.Cache ({JediCacheSpec.columnNames()}) {JediCacheSpec.bindValuesExpression()} "
            # sql to update
            sqlU = f"UPDATE {panda_config.schemaJEDI}.Cache SET {JediCacheSpec.bindUpdateChangesExpression()} WHERE main_key=:main_key AND sub_key=:sub_key "
            # start transaction
            self.conn.begin()
            # check
            varMap = {}
            varMap[":main_key"] = main_key
            varMap[":sub_key"] = sub_key
            self.cur.execute(sqlC + comment, varMap)
            resC = self.cur.fetchone()
            varMap[":data"] = data
            varMap[":last_update"] = last_update
            if resC is None:
                # insert if missing
                tmpLog.debug("insert")
                self.cur.execute(sqlI + comment, varMap)
            else:
                # update
                tmpLog.debug("update")
                self.cur.execute(sqlU + comment, varMap)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            retVal = True
            tmpLog.debug("done")
            return retVal
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return retVal

    # get cache
    def getCache_JEDI(self, main_key, sub_key):
        comment = " /* JediDBProxy.getCache_JEDI */"
        # defaults
        if sub_key is None:
            sub_key = "default"
        tmpLog = self.create_tagged_logger(comment, f"main_key={main_key} sub_key={sub_key}")
        tmpLog.debug("start")
        try:
            retVal = False
            # sql to get
            sqlC = f"SELECT {JediCacheSpec.columnNames()} FROM {panda_config.schemaJEDI}.Cache WHERE main_key=:main_key AND sub_key=:sub_key "
            # check
            varMap = {}
            varMap[":main_key"] = main_key
            varMap[":sub_key"] = sub_key
            self.cur.execute(sqlC + comment, varMap)
            resC = self.cur.fetchone()
            if resC is None:
                tmpLog.debug("got nothing, skipped")
                return None
            cache_spec = JediCacheSpec()
            cache_spec.pack(resC)
            tmpLog.debug("got cache, done")
            # return
            return cache_spec
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)

    # turn a task into pending status for some reason
    def makeTaskPending_JEDI(self, jedi_taskid, reason):
        comment = " /* JediDBProxy.makeTaskPending_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"jediTaskID={jedi_taskid}")
        try:
            self.conn.begin()
            retVal = False
            # sql to put the task in pending
            sqlPDG = (
                "UPDATE {0}.JEDI_Tasks "
                "SET lockedBy=NULL, lockedTime=NULL, "
                "status=:status, errorDialog=:err, "
                "modificationtime=CURRENT_DATE, oldStatus=status "
                "WHERE jediTaskID=:jediTaskID "
                "AND status IN ('ready','running','scouting') "
                "AND lockedBy IS NULL "
            ).format(panda_config.schemaJEDI)
            varMap = {}
            varMap[":jediTaskID"] = jedi_taskid
            varMap[":err"] = reason
            varMap[":status"] = "pending"
            self.cur.execute(sqlPDG + comment, varMap)
            nRows = self.cur.rowcount
            # add missing record_task_status_change and push_task_status_message updates
            self.record_task_status_change(jedi_taskid)
            self.push_task_status_message(None, jedi_taskid, varMap[":status"])
            if not self._commit():
                raise RuntimeError("Commit error")
            tmpLog.debug(f"done with {nRows} rows")
            # return
            return nRows
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return None

    # query tasks and turn them into pending status for some reason, sql_query should query jeditaskid
    def queryTasksToBePending_JEDI(self, sql_query, params_map, reason):
        comment = " /* JediDBProxy.queryTasksToBePending_JEDI */"
        tmpLog = self.create_tagged_logger(comment)
        try:
            # sql to query
            self.cur.execute(sql_query + comment, params_map)
            taskIDs = self.cur.fetchall()
            # sql to put the task in pending
            sqlPDG = (
                "UPDATE {0}.JEDI_Tasks "
                "SET lockedBy=NULL, lockedTime=NULL, "
                "status=:status, errorDialog=:err, "
                "modificationtime=CURRENT_DATE, oldStatus=status "
                "WHERE jediTaskID=:jediTaskID "
                "AND status IN ('ready','running','scouting') "
                "AND lockedBy IS NULL "
            ).format(panda_config.schemaJEDI)
            # loop over tasks
            n_updated = 0
            for (jedi_taskid,) in taskIDs:
                self.conn.begin()
                varMap = {}
                varMap[":jediTaskID"] = jedi_taskid
                varMap[":err"] = reason
                varMap[":status"] = "pending"
                self.cur.execute(sqlPDG + comment, varMap)
                nRow = self.cur.rowcount
                if nRow == 1:
                    self.record_task_status_change(jedi_taskid)
                    self.push_task_status_message(None, jedi_taskid, varMap[":status"])
                    n_updated += 1
                    tmpLog.debug(f"made pending jediTaskID={jedi_taskid}")
                elif nRow > 1:
                    tmpLog.error(f"updated {nRow} rows with same jediTaskID={jedi_taskid}")
                if not self._commit():
                    raise RuntimeError("Commit error")
            tmpLog.debug(f"done with {n_updated} rows")
            # return
            return n_updated
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return None

    # query tasks and preassign them to dedicate workqueue, sql_query should query jeditaskid
    def queryTasksToPreassign_JEDI(self, sql_query, params_map, site, blacklist, limit):
        comment = " /* JediDBProxy.queryTasksToPreassign_JEDI */"
        tmpLog = self.create_tagged_logger(comment)
        magic_workqueue_id = 400
        try:
            self.conn.begin()
            # sql to query
            self.cur.execute(sql_query + comment, params_map)
            taskIDs = self.cur.fetchall()
            tmpLog.debug(f"{sql_query} {params_map} ; got {len(taskIDs)} taskIDs")
            # sql to preassign the task to a site
            sqlPDG = (
                "UPDATE {0}.JEDI_Tasks "
                "SET lockedBy=NULL, lockedTime=NULL, "
                "site=:site, "
                "workQueue_ID=:workQueue_ID, "
                "modificationtime=CURRENT_DATE "
                "WHERE jediTaskID=:jediTaskID "
                "AND status IN ('ready','running','scouting') "
                "AND site IS NULL "
                "AND lockedBy IS NULL "
            ).format(panda_config.schemaJEDI)
            # loop over tasks
            n_updated = 0
            updated_tasks_attr = []
            for jedi_taskid, orig_workqueue_id in taskIDs:
                if n_updated >= limit:
                    # respect the limit
                    break
                if jedi_taskid in blacklist:
                    # skip blacklisted tasks
                    continue
                varMap = {}
                varMap[":jediTaskID"] = jedi_taskid
                varMap[":site"] = site
                varMap[":workQueue_ID"] = magic_workqueue_id
                self.cur.execute(sqlPDG + comment, varMap)
                nRow = self.cur.rowcount
                if nRow == 1:
                    # self.record_task_status_change(jedi_taskid)
                    n_updated += 1
                    orig_attr = {
                        "workQueue_ID": orig_workqueue_id,
                    }
                    updated_tasks_attr.append((jedi_taskid, orig_attr))
                    tmpLog.debug(f"preassigned jediTaskID={jedi_taskid} to site={site} , orig_attr={orig_attr}")
                elif nRow > 1:
                    tmpLog.error(f"updated {nRow} rows with same jediTaskID={jedi_taskid}")
            if not self._commit():
                raise RuntimeError("Commit error")
            tmpLog.debug(f"done with {n_updated} rows to site={site}")
            # return
            return updated_tasks_attr
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return None

    # undo preassigned tasks
    def undoPreassignedTasks_JEDI(self, jedi_taskids, task_orig_attr_map, params_map, force):
        comment = " /* JediDBProxy.undoPreassignedTasks_JEDI */"
        tmpLog = self.create_tagged_logger(comment)
        magic_workqueue_id = 400
        # sql to undo a preassigned task if it moves off the status to generate jobs
        sqlUPT = (
            "UPDATE {0}.JEDI_Tasks t "
            "SET "
            "t.site=NULL, "
            "t.workQueue_ID=( "
            "CASE "
            "WHEN t.workQueue_ID=:magic_workqueue_id "
            "THEN :orig_workqueue_id "
            "ELSE t.workQueue_ID "
            "END "
            "), "
            "t.modificationtime=CURRENT_DATE "
            "WHERE t.jediTaskID=:jediTaskID "
            "AND t.site IS NOT NULL "
            "AND NOT ( "
            "t.status IN ('ready','running') "
            "AND EXISTS ( "
            "SELECT d.datasetID FROM {0}.JEDI_Datasets d "
            "WHERE t.jediTaskID=d.jediTaskID AND d.type='input' "
            "AND d.nFilesToBeUsed-d.nFilesUsed>=:min_files_ready AND d.nFiles-d.nFilesUsed>=:min_files_remaining "
            ") "
            ") "
        ).format(panda_config.schemaJEDI)
        # sql to force to undo a preassigned task no matter what
        sqlUPTF = (
            "UPDATE {0}.JEDI_Tasks t "
            "SET "
            "t.site=NULL, "
            "t.workQueue_ID=( "
            "CASE "
            "WHEN t.workQueue_ID=:magic_workqueue_id "
            "THEN :orig_workqueue_id "
            "ELSE t.workQueue_ID "
            "END "
            "), "
            "t.modificationtime=CURRENT_DATE "
            "WHERE t.jediTaskID=:jediTaskID "
            "AND t.site IS NOT NULL "
        ).format(panda_config.schemaJEDI)
        try:
            self.conn.begin()
            # loop over tasks
            n_updated = 0
            updated_tasks = []
            force_str = ""
            for jedi_taskid in jedi_taskids:
                try:
                    orig_attr = task_orig_attr_map[str(jedi_taskid)]
                    orig_workqueue_id = orig_attr["workQueue_ID"]
                except KeyError:
                    tmpLog.warning(f"missed original attributes of jediTaskID={jedi_taskid} ; use default values ")
                    orig_workqueue_id = magic_workqueue_id
                varMap = {}
                varMap[":jediTaskID"] = jedi_taskid
                varMap[":orig_workqueue_id"] = orig_workqueue_id
                varMap[":magic_workqueue_id"] = magic_workqueue_id
                if force:
                    force_str = "force"
                    self.cur.execute(sqlUPTF + comment, varMap)
                else:
                    varMap[":min_files_ready"] = params_map[":min_files_ready"]
                    varMap[":min_files_remaining"] = params_map[":min_files_remaining"]
                    self.cur.execute(sqlUPT + comment, varMap)
                nRow = self.cur.rowcount
                if nRow == 1:
                    # self.record_task_status_change(jedi_taskid)
                    n_updated += 1
                    updated_tasks.append(jedi_taskid)
                    tmpLog.debug(f"{force_str} undid preassigned jediTaskID={jedi_taskid}")
                elif nRow > 1:
                    tmpLog.error(f"{force_str} updated {nRow} rows with same jediTaskID={jedi_taskid}")
            if not self._commit():
                raise RuntimeError("Commit error")
            tmpLog.debug(f"{force_str} done with {n_updated} rows")
            # return
            return updated_tasks
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return None

    # set missing files according to iDDS messages
    def setMissingFilesAboutIdds_JEDI(self, jeditaskid, filenames_dict):
        comment = " /* JediDBProxy.setMissingFilesAboutIdds_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"jediTaskID={jeditaskid} nfiles={len(filenames_dict)}")
        tmpLog.debug("start")
        try:
            # sql to set missing files
            sqlF = (
                "UPDATE {0}.JEDI_Dataset_Contents " "SET status=:nStatus " "WHERE jediTaskID=:jediTaskID " "AND lfn LIKE :lfn AND status!=:nStatus "
            ).format(panda_config.schemaJEDI)
            # begin transaction
            self.conn.begin()
            nFileRow = 0
            # update contents
            for filename, (datasetid, fileid) in filenames_dict.items():
                tmp_sqlF = sqlF
                varMap = {}
                varMap[":jediTaskID"] = jeditaskid
                varMap[":lfn"] = "%" + filename
                varMap[":nStatus"] = "missing"
                if datasetid is not None:
                    # with datasetID from message
                    tmp_sqlF += "AND datasetID=:datasetID "
                    varMap[":datasetID"] = datasetid
                if fileid is not None:
                    # with fileID from message
                    tmp_sqlF += "AND fileID=:fileID "
                    varMap[":fileID"] = fileid
                self.cur.execute(tmp_sqlF + comment, varMap)
                nRow = self.cur.rowcount
                nFileRow += nRow
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmpLog.debug(f"done set {nFileRow} missing files")
            return nFileRow
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return None

    # get origin datasets
    def get_origin_datasets(self, jedi_task_id, dataset_name, lfns):
        comment = " /* JediDBProxy.get_origin_datasets */"
        tmp_log = self.create_tagged_logger(comment, f"jediTaskID={jedi_task_id} {dataset_name} n_files={len(lfns)}")
        tmp_log.debug("start")
        try:
            dataset_names = []
            known_lfns = set()
            # sql to get dataset
            sql_d = (
                "SELECT tabD.jediTaskID, tabD.datasetID, tabD.datasetName "
                "FROM {0}.JEDI_Datasets tabD,{0}.JEDI_Dataset_Contents tabC "
                "WHERE tabC.lfn=:lfn AND tabC.type=:type AND tabD.datasetID=tabC.datasetID ".format(panda_config.schemaJEDI)
            )
            sql_c = f"SELECT lfn FROM {panda_config.schemaJEDI}.JEDI_Dataset_Contents WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND status=:status "
            to_break = False
            for lfn in lfns:
                if lfn in known_lfns:
                    continue
                # start transaction
                self.conn.begin()
                # get dataset
                var_map = {":lfn": lfn, ":type": "output"}
                self.cur.execute(sql_d + comment, var_map)
                res = self.cur.fetchone()
                if res:
                    task_id, dataset_id, dataset_name = res
                    dataset_names.append(dataset_name)
                    # get files
                    var_map = {":jediTaskID": task_id, ":datasetID": dataset_id, ":status": "finished"}
                    self.cur.execute(sql_c + comment, var_map)
                    res = self.cur.fetchall()
                    for (tmp_lfn,) in res:
                        known_lfns.add(tmp_lfn)
                else:
                    tmp_log.debug(f"no dataset for {lfn}")
                    # return nothing if any dataset is not found
                    dataset_names = None
                    to_break = True
                # commit
                if not self._commit():
                    raise RuntimeError("Commit error")
                if to_break:
                    break
            # return
            tmp_log.debug(f"found {str(dataset_names)}")
            return dataset_names
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return None

    # get max number of events in a file of the dataset
    def get_max_events_in_dataset(self, jedi_task_id, dataset_id):
        comment = " /* JediDBProxy.get_max_events_in_dataset */"
        tmp_log = self.create_tagged_logger(comment, f"jediTaskID={jedi_task_id} datasetID={dataset_id}")
        tmp_log.debug("start")
        try:
            # sql for get attributes
            sql = f"SELECT MAX(nEvents) FROM {panda_config.schemaJEDI}.JEDI_Dataset_Contents WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            var_map = {":jediTaskID": jedi_task_id, ":datasetID": dataset_id}
            # begin transaction
            self.conn.begin()
            # select
            self.cur.execute(sql + comment, var_map)
            res = self.cur.fetchone()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            (max_events,) = res
            tmp_log.debug(f"got {max_events}")
            return max_events
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return None

    # kick child tasks
    def kickChildTasks_JEDI(self, jediTaskID):
        comment = " /* JediDBProxy.kickChildTasks_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID}")
        tmpLog.debug("start")
        retTasks = []
        try:
            # sql to get child tasks
            sqlGT = f"SELECT jediTaskID,status FROM {panda_config.schemaJEDI}.JEDI_Tasks "
            sqlGT += "WHERE parent_tid=:jediTaskID AND parent_tid<>jediTaskID "
            # sql to change modification time to the time just before pending tasks are reactivated
            timeLimitT = naive_utcnow() - datetime.timedelta(minutes=5)
            sqlCT = f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks "
            sqlCT += "SET modificationTime=CURRENT_DATE-1 "
            sqlCT += "WHERE jediTaskID=:jediTaskID AND modificationTime<:timeLimit "
            sqlCT += "AND status=:status AND lockedBy IS NULL "
            # sql to change state check time
            timeLimitD = naive_utcnow() - datetime.timedelta(minutes=5)
            sqlCC = f"UPDATE {panda_config.schemaJEDI}.JEDI_Datasets "
            sqlCC += "SET stateCheckTime=CURRENT_DATE-1 "
            sqlCC += "WHERE jediTaskID=:jediTaskID AND state=:dsState AND stateCheckTime<:timeLimit "
            # begin transaction
            self.conn.begin()
            # get tasks
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            self.cur.execute(sqlGT + comment, varMap)
            resList = self.cur.fetchall()
            for cJediTaskID, cTaskStatus in resList:
                # no more changes
                if cTaskStatus in JediTaskSpec.statusToRejectExtChange():
                    continue
                # change modification time for pending task
                varMap = {}
                varMap[":jediTaskID"] = cJediTaskID
                varMap[":status"] = "pending"
                varMap[":timeLimit"] = timeLimitT
                self.cur.execute(sqlCT + comment, varMap)
                nRow = self.cur.rowcount
                # add missing record_task_status_change and push_task_status_message updates
                self.record_task_status_change(cJediTaskID)
                self.push_task_status_message(None, cJediTaskID, varMap[":status"])
                tmpLog.debug(f"kicked jediTaskID={cJediTaskID} with {nRow}")
                # change state check time for mutable datasets
                if cTaskStatus not in ["pending"]:
                    varMap = {}
                    varMap[":jediTaskID"] = cJediTaskID
                    varMap[":dsState"] = "mutable"
                    varMap[":timeLimit"] = timeLimitD
                    self.cur.execute(sqlCC + comment, varMap)
                    nRow = self.cur.rowcount
                    tmpLog.debug(f"kicked {nRow} mutable datasets for jediTaskID={cJediTaskID}")
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
