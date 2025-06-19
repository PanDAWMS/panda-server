import datetime
import json
import random
import re
import time

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandautils.PandaUtils import get_sql_IN_bind_variables, naive_utcnow

from pandaserver.config import panda_config
from pandaserver.srvcore import CoreUtils, srv_msg_utils
from pandaserver.taskbuffer import EventServiceUtils, JobUtils
from pandaserver.taskbuffer.db_proxy_mods.base_module import BaseModule
from pandaserver.taskbuffer.FileSpec import FileSpec
from pandaserver.taskbuffer.JobSpec import JobSpec


# Module class to define miscellaneous job-related methods that are independent of another module's methods
class JobStandaloneModule(BaseModule):
    # constructor
    def __init__(self, log_stream: LogWrapper):
        super().__init__(log_stream)

    # activate job. move job from jobsDefined to jobsActive
    def activateJob(self, job):
        comment = " /* DBProxy.activateJob */"
        if job is None:
            tmp_id = None
        else:
            tmp_id = job.PandaID
        tmp_log = self.create_tagged_logger(comment, f"PandaID={tmp_id}")
        updatedFlag = False
        if job is None:
            tmp_log.debug("skip job=None")
            return True
        tmp_log.debug("start")
        sql0 = "SELECT row_ID FROM ATLAS_PANDA.filesTable4 WHERE PandaID=:PandaID AND type=:type AND NOT status IN (:status1,:status2) "
        sql1 = "DELETE FROM ATLAS_PANDA.jobsDefined4 "
        sql1 += "WHERE PandaID=:PandaID AND (jobStatus=:oldJobStatus1 OR jobStatus=:oldJobStatus2) AND commandToPilot IS NULL"
        sql2 = f"INSERT INTO ATLAS_PANDA.jobsActive4 ({JobSpec.columnNames()}) "
        sql2 += JobSpec.bindValuesExpression()
        # host and time information
        job.modificationTime = naive_utcnow()
        # set stateChangeTime for defined->activated but not for assigned->activated
        if job.jobStatus in ["defined"]:
            job.stateChangeTime = job.modificationTime
        nTry = 3
        to_push = False
        for iTry in range(nTry):
            try:
                # check if all files are ready
                allOK = True
                for file in job.Files:
                    if file.type == "input" and file.status not in ["ready", "cached"]:
                        allOK = False
                        break
                # begin transaction
                self.conn.begin()
                # check all inputs are ready
                varMap = {}
                varMap[":type"] = "input"
                varMap[":status1"] = "ready"
                varMap[":status2"] = "cached"
                varMap[":PandaID"] = job.PandaID
                self.cur.arraysize = 100
                self.cur.execute(sql0 + comment, varMap)
                res = self.cur.fetchall()
                if len(res) == 0 or allOK:
                    # check resource share
                    job.jobStatus = "activated"

                    # delete
                    varMap = {}
                    varMap[":PandaID"] = job.PandaID
                    varMap[":oldJobStatus1"] = "assigned"
                    varMap[":oldJobStatus2"] = "defined"
                    self.cur.execute(sql1 + comment, varMap)
                    n = self.cur.rowcount
                    if n == 0:
                        # already killed or activated
                        tmp_log.debug("Job not found to activate")
                    else:
                        # insert
                        self.cur.execute(sql2 + comment, job.valuesMap())
                        # update files
                        for file in job.Files:
                            sqlF = f"UPDATE ATLAS_PANDA.filesTable4 SET {file.bindUpdateChangesExpression()}" + "WHERE row_ID=:row_ID"
                            varMap = file.valuesMap(onlyChanged=True)
                            if varMap != {}:
                                varMap[":row_ID"] = file.row_ID
                                tmp_log.debug(sqlF + comment + str(varMap))
                                self.cur.execute(sqlF + comment, varMap)
                        # job parameters
                        sqlJob = "UPDATE ATLAS_PANDA.jobParamsTable SET jobParameters=:param WHERE PandaID=:PandaID"
                        varMap = {}
                        varMap[":PandaID"] = job.PandaID
                        varMap[":param"] = job.jobParameters
                        self.cur.execute(sqlJob + comment, varMap)
                        updatedFlag = True
                        to_push = job.is_push_job()
                else:
                    # update job
                    sqlJ = (
                        f"UPDATE ATLAS_PANDA.jobsDefined4 SET {job.bindUpdateChangesExpression()} "
                    ) + "WHERE PandaID=:PandaID AND (jobStatus=:oldJobStatus1 OR jobStatus=:oldJobStatus2)"
                    varMap = job.valuesMap(onlyChanged=True)
                    varMap[":PandaID"] = job.PandaID
                    varMap[":oldJobStatus1"] = "assigned"
                    varMap[":oldJobStatus2"] = "defined"
                    tmp_log.debug(sqlJ + comment + str(varMap))
                    self.cur.execute(sqlJ + comment, varMap)
                    n = self.cur.rowcount
                    if n == 0:
                        # already killed or activated
                        tmp_log.debug("Job not found to update")
                    else:
                        # update files
                        for file in job.Files:
                            sqlF = f"UPDATE ATLAS_PANDA.filesTable4 SET {file.bindUpdateChangesExpression()}" + "WHERE row_ID=:row_ID"
                            varMap = file.valuesMap(onlyChanged=True)
                            if varMap != {}:
                                varMap[":row_ID"] = file.row_ID
                                tmp_log.debug(sqlF + comment + str(varMap))
                                self.cur.execute(sqlF + comment, varMap)
                        # job parameters
                        sqlJob = "UPDATE ATLAS_PANDA.jobParamsTable SET jobParameters=:param WHERE PandaID=:PandaID"
                        varMap = {}
                        varMap[":PandaID"] = job.PandaID
                        varMap[":param"] = job.jobParameters
                        self.cur.execute(sqlJob + comment, varMap)
                        updatedFlag = True
                # commit
                if not self._commit():
                    raise RuntimeError("Commit error")
                # record status change
                try:
                    if updatedFlag:
                        self.recordStatusChange(job.PandaID, job.jobStatus, jobInfo=job)
                except Exception:
                    tmp_log.error("recordStatusChange failed")
                self.push_job_status_message(job, job.PandaID, job.jobStatus)
                # push job
                if to_push:
                    mb_proxy_queue = self.get_mb_proxy("panda_pilot_queue")
                    mb_proxy_topic = self.get_mb_proxy("panda_pilot_topic")
                    if mb_proxy_queue and mb_proxy_topic:
                        tmp_log.debug("push job")
                        srv_msg_utils.send_job_message(mb_proxy_queue, mb_proxy_topic, job.jediTaskID, job.PandaID)
                    else:
                        tmp_log.debug("message queue/topic not configured")
                tmp_log.debug("done")
                return True
            except Exception as e:
                # roll back
                self._rollback()
                if iTry + 1 < nTry:
                    tmp_log.debug(f"retry: {iTry}")
                    time.sleep(random.randint(10, 20))
                    continue
                self.dump_error_message(tmp_log)
                return False

    # send job to jobsWaiting
    def keepJob(self, job):
        comment = " /* DBProxy.keepJob */"
        tmp_log = self.create_tagged_logger(comment, f"PandaID={job.PandaID}")
        tmp_log.debug("start")
        # set status
        job.jobStatus = "waiting"
        sql1 = f"UPDATE ATLAS_PANDA.jobsDefined4 SET {job.bindUpdateChangesExpression()} "
        sql1 += "WHERE PandaID=:PandaID AND (jobStatus=:oldJobStatus1 OR jobStatus=:oldJobStatus2) AND commandToPilot IS NULL"
        # time information
        job.modificationTime = naive_utcnow()
        job.stateChangeTime = job.modificationTime
        updatedFlag = False
        nTry = 3
        for iTry in range(nTry):
            try:
                # begin transaction
                self.conn.begin()
                # update
                varMap = job.valuesMap(onlyChanged=True)
                varMap[":PandaID"] = job.PandaID
                varMap[":oldJobStatus1"] = "assigned"
                varMap[":oldJobStatus2"] = "defined"
                self.cur.execute(sql1 + comment, varMap)
                n = self.cur.rowcount
                if n == 0:
                    # already killed
                    tmp_log.debug(f"Not found")
                else:
                    # update files
                    for file in job.Files:
                        sqlF = f"UPDATE ATLAS_PANDA.filesTable4 SET {file.bindUpdateChangesExpression()}" + "WHERE row_ID=:row_ID"
                        varMap = file.valuesMap(onlyChanged=True)
                        if varMap != {}:
                            varMap[":row_ID"] = file.row_ID
                            self.cur.execute(sqlF + comment, varMap)
                    # update parameters
                    sqlJob = "UPDATE ATLAS_PANDA.jobParamsTable SET jobParameters=:param WHERE PandaID=:PandaID"
                    varMap = {}
                    varMap[":PandaID"] = job.PandaID
                    varMap[":param"] = job.jobParameters
                    self.cur.execute(sqlJob + comment, varMap)
                    updatedFlag = True
                # commit
                if not self._commit():
                    raise RuntimeError("Commit error")
                # record status change
                try:
                    if updatedFlag:
                        self.recordStatusChange(job.PandaID, job.jobStatus, jobInfo=job)
                        self.push_job_status_message(job, job.PandaID, job.jobStatus)
                except Exception:
                    tmp_log.error("recordStatusChange in keepJob")
                return True
            except Exception:
                # roll back
                self._rollback()
                if iTry + 1 < nTry:
                    tmp_log.debug(f"retry : {iTry}")
                    time.sleep(random.randint(10, 20))
                    continue
                # dump error
                self.dump_error_message(tmp_log)
                return False

    # reset job in jobsActive
    def resetJob(
        self,
        pandaID,
        activeTable=True,
        keepSite=False,
        getOldSubs=False,
        forPending=True,
    ):
        comment = " /* DBProxy.resetJob */"
        tmp_log = self.create_tagged_logger(comment, f"PandaID={pandaID}")
        tmp_log.debug(f"activeTable={activeTable}")
        # select table
        table = "ATLAS_PANDA.jobsActive4"
        sql1 = f"SELECT {JobSpec.columnNames()} FROM {table} "
        sql1 += "WHERE PandaID=:PandaID"
        sql2 = f"DELETE FROM {table} "
        sql2 += "WHERE PandaID=:PandaID AND (jobStatus=:oldJobStatus1 OR jobStatus=:oldJobStatus2)"
        sql3 = f"INSERT INTO ATLAS_PANDA.jobsDefined4 ({JobSpec.columnNames()}) "
        sql3 += JobSpec.bindValuesExpression()
        try:
            # transaction causes Request ndbd time-out in ATLAS_PANDA.jobsActive4
            self.conn.begin()
            # select
            varMap = {}
            varMap[":PandaID"] = pandaID
            self.cur.arraysize = 10
            self.cur.execute(sql1 + comment, varMap)
            res = self.cur.fetchone()
            # not found
            if res is None:
                # commit
                if not self._commit():
                    raise RuntimeError("Commit error")
                # return
                return None
            # instantiate Job
            job = JobSpec()
            job.pack(res)
            # if already running
            if job.jobStatus != "waiting" and job.jobStatus != "activated" and (forPending and job.jobStatus != "pending"):
                # commit
                if not self._commit():
                    raise RuntimeError("Commit error")
                # return
                return None
            # do nothing for analysis jobs
            if job.prodSourceLabel in ["user", "panda"] and not forPending and job.jobStatus != "pending":
                # commit
                if not self._commit():
                    raise RuntimeError("Commit error")
                # return
                return None
            # delete
            varMap = {}
            varMap[":PandaID"] = pandaID
            if not forPending:
                varMap[":oldJobStatus1"] = "waiting"
            else:
                varMap[":oldJobStatus1"] = "pending"
            varMap[":oldJobStatus2"] = "activated"
            self.cur.execute(sql2 + comment, varMap)
            retD = self.cur.rowcount
            # delete failed
            tmp_log.debug(f"retD = {retD}")
            if retD != 1:
                # commit
                if not self._commit():
                    raise RuntimeError("Commit error")
                return None
            # delete from jobsDefined4 just in case
            varMap = {}
            varMap[":PandaID"] = pandaID
            sqlD = "DELETE FROM ATLAS_PANDA.jobsDefined4 WHERE PandaID=:PandaID"
            self.cur.execute(sqlD + comment, varMap)
            # increase priority
            if job.jobStatus == "activated" and job.currentPriority < 100:
                job.currentPriority = 100
            # reset computing site and dispatchDBlocks
            job.jobStatus = "defined"
            if job.prodSourceLabel not in ["user", "panda"]:
                job.dispatchDBlock = None
                # erase old assignment
                if (not keepSite) and job.relocationFlag not in [1, 2]:
                    job.computingSite = None
                job.computingElement = None
            # host and time information
            job.modificationHost = self.hostname
            job.modificationTime = naive_utcnow()
            job.stateChangeTime = job.modificationTime
            # reset
            job.brokerageErrorDiag = None
            job.brokerageErrorCode = None
            # insert
            self.cur.execute(sql3 + comment, job.valuesMap())
            # job parameters
            sqlJobP = "SELECT jobParameters FROM ATLAS_PANDA.jobParamsTable WHERE PandaID=:PandaID"
            self.cur.execute(sqlJobP + comment, varMap)
            for (clobJobP,) in self.cur:
                try:
                    job.jobParameters = clobJobP.read()
                except AttributeError:
                    job.jobParameters = str(clobJobP)
                break
            # Files
            oldSubList = []
            sqlFile = f"SELECT {FileSpec.columnNames()} FROM ATLAS_PANDA.filesTable4 "
            sqlFile += "WHERE PandaID=:PandaID"
            self.cur.arraysize = 10000
            self.cur.execute(sqlFile + comment, varMap)
            resFs = self.cur.fetchall()
            for resF in resFs:
                file = FileSpec()
                file.pack(resF)
                # reset GUID to trigger LRC/LFC scanning
                if file.status == "missing":
                    file.GUID = None
                # collect old subs
                if job.prodSourceLabel in ["managed", "test"] and file.type in ["output", "log"] and re.search("_sub\d+$", file.destinationDBlock) is not None:
                    if file.destinationDBlock not in oldSubList:
                        oldSubList.append(file.destinationDBlock)
                # reset status, destinationDBlock and dispatchDBlock
                if job.lockedby != "jedi":
                    file.status = "unknown"
                if job.prodSourceLabel not in ["user", "panda"]:
                    file.dispatchDBlock = None
                file.destinationDBlock = re.sub("_sub\d+$", "", file.destinationDBlock)
                # add file
                job.addFile(file)
                # update files
                sqlF = f"UPDATE ATLAS_PANDA.filesTable4 SET {file.bindUpdateChangesExpression()}" + "WHERE row_ID=:row_ID"
                varMap = file.valuesMap(onlyChanged=True)
                if varMap != {}:
                    varMap[":row_ID"] = file.row_ID
                    tmp_log.debug(sqlF + comment + str(varMap))
                    self.cur.execute(sqlF + comment, varMap)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # record status change
            try:
                self.recordStatusChange(job.PandaID, job.jobStatus, jobInfo=job)
            except Exception:
                tmp_log.error("recordStatusChange in resetJobs")
            self.push_job_status_message(job, job.PandaID, job.jobStatus)
            tmp_log.debug(f"done with {job is not None}")
            if getOldSubs:
                return job, oldSubList
            return job
        except Exception:
            # roll back
            self._rollback()
            # error report
            self.dump_error_message(tmp_log)
            return None

    # reset jobs in jobsDefined
    def resetDefinedJob(self, pandaID):
        comment = " /* DBProxy.resetDefinedJob */"
        tmp_log = self.create_tagged_logger(comment, f"PandaID={pandaID}")
        tmp_log.debug("start")
        sql1 = "UPDATE ATLAS_PANDA.jobsDefined4 SET "
        sql1 += "jobStatus=:newJobStatus,"
        sql1 += "modificationTime=CURRENT_DATE,"
        sql1 += "modificationHost=:modificationHost"
        sql1 += " WHERE PandaID=:PandaID AND jobStatus IN (:oldJobStatus1,:oldJobStatus2,:oldJobStatus3) "
        sql2 = f"SELECT {JobSpec.columnNames()} FROM ATLAS_PANDA.jobsDefined4 "
        sql2 += "WHERE PandaID=:PandaID"
        try:
            # begin transaction
            self.conn.begin()
            # update
            varMap = {}
            varMap[":PandaID"] = pandaID
            varMap[":newJobStatus"] = "defined"
            varMap[":oldJobStatus1"] = "assigned"
            varMap[":oldJobStatus2"] = "defined"
            varMap[":oldJobStatus3"] = "pending"
            varMap[":modificationHost"] = self.hostname
            self.cur.execute(sql1 + comment, varMap)
            retU = self.cur.rowcount
            # not found
            updatedFlag = True
            job = None
            if retU == 0:
                tmp_log.debug("Not found for UPDATE")
                updatedFlag = False
            else:
                # select
                varMap = {}
                varMap[":PandaID"] = pandaID
                self.cur.arraysize = 10
                self.cur.execute(sql2 + comment, varMap)
                res = self.cur.fetchone()
                # not found
                if res is None:
                    raise RuntimeError(f"Not found for SELECT")
                # instantiate Job
                job = JobSpec()
                job.pack(res)
                # job parameters
                sqlJobP = "SELECT jobParameters FROM ATLAS_PANDA.jobParamsTable WHERE PandaID=:PandaID"
                self.cur.execute(sqlJobP + comment, varMap)
                for (clobJobP,) in self.cur:
                    try:
                        job.jobParameters = clobJobP.read()
                    except AttributeError:
                        job.jobParameters = str(clobJobP)
                    break
                # Files
                sqlFile = f"SELECT {FileSpec.columnNames()} FROM ATLAS_PANDA.filesTable4 "
                sqlFile += "WHERE PandaID=:PandaID"
                self.cur.arraysize = 10000
                self.cur.execute(sqlFile + comment, varMap)
                resFs = self.cur.fetchall()
                for resF in resFs:
                    file = FileSpec()
                    file.pack(resF)
                    # add file
                    job.addFile(file)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # record status change
            try:
                if updatedFlag:
                    self.recordStatusChange(job.PandaID, job.jobStatus, jobInfo=job)
                    self.push_job_status_message(job, job.PandaID, job.jobStatus)
            except Exception:
                tmp_log.error("recordStatusChange in resetDefinedJobs")
            tmp_log.debug(f"done with {job is not None}")
            return job
        except Exception:
            self.dump_error_message(tmp_log)
            # roll back
            self._rollback()
            return None

    # peek at job
    def peekJob(self, pandaID, fromDefined, fromActive, fromArchived, fromWaiting, forAnal=False):
        comment = " /* DBProxy.peekJob */"
        tmp_log = self.create_tagged_logger(comment, f"PandaID={pandaID}")
        # return None for NULL PandaID
        if pandaID in ["NULL", "", "None", None]:
            return None
        # only int
        try:
            _ = int(pandaID)
        except Exception:
            tmp_log.debug(f"return None for {pandaID}:non-integer")
            return None
        sql1_0 = "SELECT %s FROM %s "
        sql1_1 = "WHERE PandaID=:PandaID"
        nTry = 3
        for iTry in range(nTry):
            try:
                tables = []
                if fromDefined or fromWaiting:
                    tables.append("ATLAS_PANDA.jobsDefined4")
                if fromActive:
                    tables.append("ATLAS_PANDA.jobsActive4")
                if fromArchived:
                    tables.append("ATLAS_PANDA.jobsArchived4")
                if fromDefined:
                    # for jobs which are just reset
                    tables.append("ATLAS_PANDA.jobsDefined4")
                # select
                varMap = {}
                varMap[":PandaID"] = pandaID
                for table in tables:
                    # start transaction
                    self.conn.begin()
                    # select
                    sql = sql1_0 % (JobSpec.columnNames(), table) + sql1_1
                    self.cur.arraysize = 10
                    self.cur.execute(sql + comment, varMap)
                    res = self.cur.fetchall()
                    # commit
                    if not self._commit():
                        raise RuntimeError("Commit error")
                    if len(res) != 0:
                        # Job
                        job = JobSpec()
                        job.pack(res[0])
                        # Files
                        # start transaction
                        self.conn.begin()
                        # select
                        sqlFile = f"SELECT {FileSpec.columnNames()} FROM ATLAS_PANDA.filesTable4 "
                        sqlFile += "WHERE PandaID=:PandaID"
                        self.cur.arraysize = 10000
                        self.cur.execute(sqlFile + comment, varMap)
                        resFs = self.cur.fetchall()
                        # metadata
                        resMeta = None
                        if table == "ATLAS_PANDA.jobsArchived4" or forAnal:
                            # read metadata only for finished/failed production jobs
                            sqlMeta = "SELECT metaData FROM ATLAS_PANDA.metaTable WHERE PandaID=:PandaID"
                            self.cur.execute(sqlMeta + comment, varMap)
                            for (clobMeta,) in self.cur:
                                if clobMeta is not None:
                                    try:
                                        resMeta = clobMeta.read()
                                    except AttributeError:
                                        resMeta = str(clobMeta)
                                break
                        # job parameters
                        job.jobParameters = None
                        sqlJobP = "SELECT jobParameters FROM ATLAS_PANDA.jobParamsTable WHERE PandaID=:PandaID"
                        varMap = {}
                        varMap[":PandaID"] = job.PandaID
                        self.cur.execute(sqlJobP + comment, varMap)
                        for (clobJobP,) in self.cur:
                            if clobJobP is not None:
                                try:
                                    job.jobParameters = clobJobP.read()
                                except AttributeError:
                                    job.jobParameters = str(clobJobP)
                            break
                        # commit
                        if not self._commit():
                            raise RuntimeError("Commit error")
                        # set files
                        for resF in resFs:
                            file = FileSpec()
                            file.pack(resF)
                            job.addFile(file)
                        # set metadata
                        job.metadata = resMeta
                        return job
                tmp_log.debug(f"not found")
                return None
            except Exception:
                # roll back
                self._rollback()
                if iTry + 1 < nTry:
                    tmp_log.debug(f"retry : {iTry}")
                    time.sleep(random.randint(10, 20))
                    continue
                self.dump_error_message(tmp_log)
                # return None for analysis
                if forAnal:
                    return None
                # return 'unknown'
                job = JobSpec()
                job.PandaID = pandaID
                job.jobStatus = "unknown"
                return job

    # get express jobs
    def getExpressJobs(self, dn):
        comment = " /* DBProxy.getExpressJobs */"
        tmp_log = self.create_tagged_logger(comment, f"DN={dn}")
        tmp_log.debug(f"start")
        sqlX = "SELECT specialHandling,COUNT(*) FROM %s "
        sqlX += "WHERE prodUserName=:prodUserName AND prodSourceLabel=:prodSourceLabel1 "
        sqlX += "AND specialHandling IS NOT NULL "
        sqlXJob = "SELECT PandaID,jobStatus,prodSourceLabel,modificationTime,jobDefinitionID,jobsetID,startTime,endTime FROM %s "
        sqlXJob += "WHERE prodUserName=:prodUserName AND prodSourceLabel=:prodSourceLabel1 "
        sqlXJob += "AND specialHandling IS NOT NULL AND specialHandling=:specialHandling "
        sqlQ = sqlX
        sqlQ += "GROUP BY specialHandling "
        sqlQJob = sqlXJob
        sqlA = sqlX
        sqlA += "AND modificationTime>:modificationTime GROUP BY specialHandling "
        sqlAJob = sqlXJob
        sqlAJob += "AND modificationTime>:modificationTime "
        try:
            # get compact DN
            compactDN = CoreUtils.clean_user_id(dn)
            if compactDN in ["", "NULL", None]:
                compactDN = dn
            expressStr = "express"
            activeExpressU = []
            timeUsageU = datetime.timedelta(0)
            executionTimeU = datetime.timedelta(hours=1)
            jobCreditU = 3
            timeCreditU = executionTimeU * jobCreditU
            timeNow = naive_utcnow()
            timeLimit = timeNow - datetime.timedelta(hours=6)
            # loop over tables
            for table in [
                "ATLAS_PANDA.jobsDefined4",
                "ATLAS_PANDA.jobsActive4",
                "ATLAS_PANDA.jobsArchived4",
            ]:
                varMap = {}
                varMap[":prodUserName"] = compactDN
                varMap[":prodSourceLabel1"] = "user"
                if table == "ATLAS_PANDA.jobsArchived4":
                    varMap[":modificationTime"] = timeLimit
                    sql = sqlA % table
                    sqlJob = sqlAJob % table
                else:
                    sql = sqlQ % table
                    sqlJob = sqlQJob % table
                # start transaction
                self.conn.begin()
                # get the number of jobs for each specialHandling
                self.cur.arraysize = 10
                tmp_log.debug(sql + comment + str(varMap))
                self.cur.execute(sql + comment, varMap)
                res = self.cur.fetchall()
                tmp_log.debug(f"{str(res)}")
                for specialHandling, countJobs in res:
                    if specialHandling is None:
                        continue
                    # look for express jobs
                    if expressStr in specialHandling:
                        varMap[":specialHandling"] = specialHandling
                        self.cur.arraysize = 1000
                        self.cur.execute(sqlJob + comment, varMap)
                        resJobs = self.cur.fetchall()
                        tmp_log.debug(f"{str(resJobs)}")
                        for (
                            tmp_PandaID,
                            tmp_jobStatus,
                            tmp_prodSourceLabel,
                            tmp_modificationTime,
                            tmp_jobDefinitionID,
                            tmp_jobsetID,
                            tmp_startTime,
                            tmp_endTime,
                        ) in resJobs:
                            # collect active jobs
                            if tmp_jobStatus not in [
                                "finished",
                                "failed",
                                "cancelled",
                                "closed",
                            ]:
                                activeExpressU.append((tmp_PandaID, tmp_jobsetID, tmp_jobDefinitionID))
                            # get time usage
                            if tmp_jobStatus not in ["defined", "activated"]:
                                # check only jobs which actually use or used CPU on WN
                                if tmp_startTime is not None:
                                    # running or not
                                    if tmp_endTime is None:
                                        # job got started before/after the time limit
                                        if timeLimit > tmp_startTime:
                                            timeDelta = timeNow - timeLimit
                                        else:
                                            timeDelta = timeNow - tmp_startTime
                                    else:
                                        # job got started before/after the time limit
                                        if timeLimit > tmp_startTime:
                                            timeDelta = tmp_endTime - timeLimit
                                        else:
                                            timeDelta = tmp_endTime - tmp_startTime
                                    # add
                                    if timeDelta > datetime.timedelta(0):
                                        timeUsageU += timeDelta
                # commit
                if not self._commit():
                    raise RuntimeError("Commit error")
            # check quota
            rRet = True
            rRetStr = ""
            rQuota = 0
            if len(activeExpressU) >= jobCreditU:
                rRetStr += f"The number of queued runXYZ exceeds the limit = {jobCreditU}. "
                rRet = False
            if timeUsageU >= timeCreditU:
                rRetStr += f"The total execution time for runXYZ exceeds the limit = {timeCreditU.seconds / 60} min. "
                rRet = False
            # calculate available quota
            if rRet:
                tmpQuota = jobCreditU - len(activeExpressU) - timeUsageU.seconds / executionTimeU.seconds
                if tmpQuota < 0:
                    rRetStr += "Quota for runXYZ exceeds. "
                    rRet = False
                else:
                    rQuota = tmpQuota
            # return
            retVal = {
                "status": rRet,
                "quota": rQuota,
                "output": rRetStr,
                "usage": timeUsageU,
                "jobs": activeExpressU,
            }
            tmp_log.debug(f"{str(retVal)}")
            return retVal
        except Exception:
            # roll back
            self._rollback()
            self.dump_error_message(tmp_log)
            return None

    # get active debug jobs
    def getActiveDebugJobs(self, dn=None, workingGroup=None, prodRole=False):
        comment = " /* DBProxy.getActiveDebugJobs */"
        tmp_log = self.create_tagged_logger(comment, f"DN={dn}")
        tmp_log.debug(f"wg={workingGroup} prodRole={prodRole}")
        varMap = {}
        sqlX = "SELECT PandaID,jobStatus,specialHandling FROM %s "
        sqlX += "WHERE "
        if prodRole:
            pass
        elif workingGroup is not None:
            sqlX += "UPPER(workingGroup) IN (:wg1,:wg2) AND "
            varMap[":wg1"] = f"AP_{workingGroup.upper()}"
            varMap[":wg2"] = f"GP_{workingGroup.upper()}"
        else:
            sqlX += "prodUserName=:prodUserName AND "
            # get compact DN
            compactDN = CoreUtils.clean_user_id(dn)
            if compactDN in ["", "NULL", None]:
                compactDN = dn
            varMap[":prodUserName"] = compactDN
        sqlX += "specialHandling IS NOT NULL "
        try:
            debugStr = "debug"
            activeDebugJobs = []
            # loop over tables
            for table in ["ATLAS_PANDA.jobsDefined4", "ATLAS_PANDA.jobsActive4"]:
                sql = sqlX % table
                # start transaction
                self.conn.begin()
                # get jobs with specialHandling
                self.cur.arraysize = 100000
                self.cur.execute(sql + comment, varMap)
                res = self.cur.fetchall()
                # commit
                if not self._commit():
                    raise RuntimeError("Commit error")
                # loop over all PandaIDs
                for pandaID, jobStatus, specialHandling in res:
                    if specialHandling is None:
                        continue
                    # only active jobs
                    if jobStatus not in [
                        "defined",
                        "activated",
                        "running",
                        "sent",
                        "starting",
                    ]:
                        continue
                    # look for debug jobs
                    if debugStr in specialHandling and pandaID not in activeDebugJobs:
                        activeDebugJobs.append(pandaID)
            # return
            activeDebugJobs.sort()
            tmp_log.debug(f"{str(activeDebugJobs)}")
            return activeDebugJobs
        except Exception:
            # roll back
            self._rollback()
            self.dump_error_message(tmp_log)
            return None

    # set debug mode
    def setDebugMode(self, dn, pandaID, prodManager, modeOn, workingGroup):
        comment = " /* DBProxy.setDebugMode */"
        tmp_log = self.create_tagged_logger(comment, f"PandaID={pandaID}")
        tmp_log.debug(f"dn={dn} prod={prodManager} wg={workingGroup} mode={modeOn}")
        sqlX = "SELECT prodUserName,jobStatus,specialHandling,workingGroup FROM %s "
        sqlX += "WHERE PandaID=:PandaID "
        sqlU = "UPDATE %s SET specialHandling=:specialHandling "
        sqlU += "WHERE PandaID=:PandaID "
        try:
            # get compact DN
            compactDN = CoreUtils.clean_user_id(dn)
            if compactDN in ["", "NULL", None]:
                compactDN = dn
            debugStr = "debug"
            retStr = ""
            retCode = False
            # loop over tables
            for table in ["ATLAS_PANDA.jobsDefined4", "ATLAS_PANDA.jobsActive4"]:
                varMap = {}
                varMap[":PandaID"] = pandaID
                sql = sqlX % table
                # start transaction
                self.conn.begin()
                # get jobs with specialHandling
                self.cur.arraysize = 10
                self.cur.execute(sql + comment, varMap)
                res = self.cur.fetchone()
                # not found
                if res is None:
                    retStr = f"PandaID={pandaID} not found in active DB"
                    # commit
                    if not self._commit():
                        raise RuntimeError("Commit error")
                    continue
                prodUserName, jobStatus, specialHandling, wGroup = res
                # not active
                changeableState = [
                    "defined",
                    "activated",
                    "running",
                    "sent",
                    "starting",
                    "assigned",
                ]
                if jobStatus not in changeableState:
                    retStr = f"Cannot set debugMode since the job status is {jobStatus} which is not in one of {str(changeableState)}"
                    # commit
                    if not self._commit():
                        raise RuntimeError("Commit error")
                    break
                # extract workingGroup
                try:
                    wGroup = wGroup.split("_")[-1]
                    wGroup = wGroup.lower()
                except Exception:
                    pass
                # not owner
                notOwner = False
                if not prodManager:
                    if workingGroup is not None:
                        if workingGroup.lower() != wGroup:
                            retStr = f"Permission denied. Not the production manager for workingGroup={wGroup}"
                            notOwner = True
                    else:
                        if prodUserName != compactDN:
                            retStr = "Permission denied. Not the owner or production manager"
                            notOwner = True
                    if notOwner:
                        # commit
                        if not self._commit():
                            raise RuntimeError("Commit error")
                        break
                # set specialHandling
                updateSH = True
                if specialHandling in [None, ""]:
                    if modeOn:
                        # set debug mode
                        specialHandling = debugStr
                    else:
                        # already disabled debug mode
                        updateSH = False
                elif debugStr in specialHandling:
                    if modeOn:
                        # already in debug mode
                        updateSH = False
                    else:
                        # disable debug mode
                        specialHandling = re.sub(debugStr, "", specialHandling)
                        specialHandling = re.sub(",,", ",", specialHandling)
                        specialHandling = re.sub("^,", "", specialHandling)
                        specialHandling = re.sub(",$", "", specialHandling)
                else:
                    if modeOn:
                        # set debug mode
                        specialHandling = debugStr
                    else:
                        # already disabled debug mode
                        updateSH = False

                # no update
                if not updateSH:
                    retStr = "Already set accordingly"
                    # commit
                    if not self._commit():
                        raise RuntimeError("Commit error")
                    break
                # update
                varMap = {}
                varMap[":PandaID"] = pandaID
                varMap[":specialHandling"] = specialHandling
                self.cur.execute((sqlU + comment) % table, varMap)
                retD = self.cur.rowcount
                # commit
                if not self._commit():
                    raise RuntimeError("Commit error")
                if retD == 0:
                    retStr = "Failed to update DB"
                else:
                    retStr = "Succeeded"
                    break
            # return
            tmp_log.debug(f"{retStr}")
            return retStr
        except Exception:
            # roll back
            self._rollback()
            self.dump_error_message(tmp_log)
            return None

    # lock jobs for reassign
    def lockJobsForReassign(
        self,
        tableName,
        timeLimit,
        statList,
        labels,
        processTypes,
        sites,
        clouds,
        useJEDI=False,
        onlyReassignable=False,
        useStateChangeTime=False,
        getEventService=False,
    ):
        comment = " /* DBProxy.lockJobsForReassign */"
        tmp_log = self.create_tagged_logger(comment)
        tmp_log.debug(f"{tableName} {timeLimit} {statList} {labels} {processTypes} {sites} {clouds} {useJEDI}")
        try:
            # make sql
            if not useJEDI:
                sql = f"SELECT PandaID FROM {tableName} "
            elif getEventService:
                sql = f"SELECT PandaID,lockedby,eventService,attemptNr,computingSite FROM {tableName} "
            else:
                sql = f"SELECT PandaID,lockedby FROM {tableName} "
            if not useStateChangeTime:
                sql += "WHERE modificationTime<:modificationTime "
            else:
                sql += "WHERE stateChangeTime<:modificationTime "
            varMap = {}
            varMap[":modificationTime"] = timeLimit
            if statList != []:
                stat_var_names_str, stat_var_map = get_sql_IN_bind_variables(statList, prefix=":stat")
                sql += f"AND jobStatus IN ({stat_var_names_str}) "
                varMap.update(stat_var_map)
            if labels != []:
                label_var_names_str, label_var_map = get_sql_IN_bind_variables(labels, prefix=":label")
                sql += f"AND prodSourceLabel IN ({label_var_names_str}) "
                varMap.update(label_var_map)
            if processTypes != []:
                ptype_var_names_str, ptype_var_map = get_sql_IN_bind_variables(processTypes, prefix=":processType")
                sql += f"AND processingType IN ({ptype_var_names_str}) "
                varMap.update(ptype_var_map)
            if sites != []:
                site_var_names_str, site_var_map = get_sql_IN_bind_variables(sites, prefix=":site")
                sql += f"AND computingSite IN ({site_var_names_str}) "
                varMap.update(site_var_map)
            if clouds != []:
                cloud_var_names_str, cloud_var_map = get_sql_IN_bind_variables(clouds, prefix=":cloud")
                sql += f"AND cloud IN ({cloud_var_names_str}) "
                varMap.update(cloud_var_map)
            if onlyReassignable:
                sql += "AND (relocationFlag IS NULL OR relocationFlag<>:relocationFlag) "
                varMap[":relocationFlag"] = 2
            # sql for lock
            if not useStateChangeTime:
                sqlLock = f"UPDATE {tableName} SET modificationTime=CURRENT_DATE WHERE PandaID=:PandaID"
            else:
                sqlLock = f"UPDATE {tableName} SET stateChangeTime=CURRENT_DATE WHERE PandaID=:PandaID"
            # start transaction
            self.conn.begin()
            # select
            self.cur.arraysize = 1000000
            tmp_log.debug(sql + comment + str(varMap))
            self.cur.execute(sql + comment, varMap)
            resList = self.cur.fetchall()
            tmp_log.debug(f"found {len(resList)}")
            retList = []
            # lock
            for tmpItem in resList:
                tmpID = tmpItem[0]
                varLock = {":PandaID": tmpID}
                self.cur.execute(sqlLock + comment, varLock)
                retList.append(tmpItem)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # sort
            retList.sort()
            tmp_log.debug(f"return {len(retList)}")
            return True, retList
        except Exception:
            # roll back
            self._rollback()
            self.dump_error_message(tmp_log)
            # return empty
            return False, []

    # lock jobs for finisher
    def lockJobsForFinisher(self, timeNow, rownum, highPrio):
        comment = " /* DBProxy.lockJobsForFinisher */"
        tmp_log = self.create_tagged_logger(comment)
        tmp_log.debug(f"{timeNow} {rownum} {highPrio}")
        try:
            varMap = {}
            varMap[":jobStatus"] = "transferring"
            varMap[":currentPriority"] = 800
            varMap[":pLabel1"] = "managed"
            varMap[":pLabel2"] = "test"
            varMap[":esJumbo"] = EventServiceUtils.jumboJobFlagNumber
            # make sql
            sql = "SELECT PandaID FROM ATLAS_PANDA.jobsActive4 "
            sql += "WHERE jobStatus=:jobStatus AND modificationTime<:modificationTime AND prodSourceLabel IN (:pLabel1,:pLabel2) "
            sql += "AND (eventService IS NULL OR eventService<>:esJumbo) "
            if highPrio:
                varMap[":modificationTime"] = timeNow - datetime.timedelta(hours=1)
                sql += f"AND currentPriority>=:currentPriority AND rownum<={rownum} "
            else:
                sql += f"AND currentPriority<:currentPriority AND rownum<={rownum} "
                varMap[":modificationTime"] = timeNow - datetime.timedelta(hours=2)
            sql += "FOR UPDATE "
            # sql for lock
            sqlLock = "UPDATE ATLAS_PANDA.jobsActive4 SET modificationTime=CURRENT_DATE WHERE PandaID=:PandaID"
            # start transaction
            self.conn.begin()
            # select
            self.cur.arraysize = 1000
            self.cur.execute(sql + comment, varMap)
            resList = self.cur.fetchall()
            retList = []
            # lock
            for (tmpID,) in resList:
                varLock = {":PandaID": tmpID}
                self.cur.execute(sqlLock + comment, varLock)
                retList.append(tmpID)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # sort
            retList.sort()
            tmp_log.debug(f"{len(retList)}")
            return True, retList
        except Exception:
            # roll back
            self._rollback()
            self.dump_error_message(tmp_log)
            # return empty
            return False, []

    # lock jobs for activator
    def lockJobsForActivator(self, timeLimit, rownum, prio):
        comment = " /* DBProxy.lockJobsForActivator */"
        tmp_log = self.create_tagged_logger(comment)
        tmp_log.debug("start")
        try:
            varMap = {}
            varMap[":jobStatus"] = "assigned"
            if prio > 0:
                varMap[":currentPriority"] = prio
            varMap[":timeLimit"] = timeLimit
            # make sql
            sql = "SELECT PandaID FROM ATLAS_PANDA.jobsDefined4 "
            sql += "WHERE jobStatus=:jobStatus AND (prodDBUpdateTime IS NULL OR prodDBUpdateTime<:timeLimit) "
            if prio > 0:
                sql += "AND currentPriority>=:currentPriority "
            sql += f"AND rownum<={rownum} "
            sql += "FOR UPDATE "
            # sql for lock
            sqlLock = "UPDATE ATLAS_PANDA.jobsDefined4 SET prodDBUpdateTime=CURRENT_DATE WHERE PandaID=:PandaID"
            # start transaction
            self.conn.begin()
            # select
            self.cur.arraysize = 1000
            self.cur.execute(sql + comment, varMap)
            resList = self.cur.fetchall()
            retList = []
            # lock
            for (tmpID,) in resList:
                varLock = {":PandaID": tmpID}
                self.cur.execute(sqlLock + comment, varLock)
                retList.append(tmpID)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # sort
            retList.sort()
            tmp_log.debug(f"locked {len(retList)} jobs")
            return True, retList
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            # return empty
            return False, []

    # add metadata
    def addMetadata(self, pandaID, metadata, newStatus):
        comment = " /* DBProxy.addMetaData */"
        tmp_log = self.create_tagged_logger(comment, f"PandaID={pandaID}")
        tmp_log.debug(f"start")
        # discard metadata for failed jobs
        if newStatus == "failed":
            tmp_log.debug("skip")
            return True
        sqlJ = "SELECT jobStatus FROM ATLAS_PANDA.jobsActive4 WHERE PandaID=:PandaID "
        sqlJ += "UNION "
        sqlJ += "SELECT jobStatus FROM ATLAS_PANDA.jobsArchived4 WHERE PandaID=:PandaID "
        sql0 = "SELECT PandaID FROM ATLAS_PANDA.metaTable WHERE PandaID=:PandaID"
        sql1 = "INSERT INTO ATLAS_PANDA.metaTable (PandaID,metaData) VALUES (:PandaID,:metaData)"
        nTry = 1
        regStart = naive_utcnow()
        for iTry in range(nTry):
            try:
                # autocommit on
                self.conn.begin()
                self.cur.arraysize = 10
                # check job status
                varMap = {}
                varMap[":PandaID"] = pandaID
                self.cur.execute(sqlJ + comment, varMap)
                resJ = self.cur.fetchone()
                if resJ is not None:
                    (jobStatus,) = resJ
                else:
                    jobStatus = "unknown"
                if jobStatus in ["unknown"]:
                    tmp_log.debug(f"skip jobStatus={jobStatus}")
                    if not self._commit():
                        raise RuntimeError("Commit error")
                    return False
                # skip if in final state
                if jobStatus in ["cancelled", "closed", "finished", "failed"]:
                    tmp_log.debug(f"skip jobStatus={jobStatus}")
                    if not self._commit():
                        raise RuntimeError("Commit error")
                    # return True so that subsequent procedure can keep going
                    return True
                # select
                varMap = {}
                varMap[":PandaID"] = pandaID
                self.cur.arraysize = 10
                self.cur.execute(sql0 + comment, varMap)
                res = self.cur.fetchone()
                # already exist
                if res is not None:
                    tmp_log.debug(f"skip duplicated during jobStatus={jobStatus}")
                    if not self._commit():
                        raise RuntimeError("Commit error")
                    return True
                # truncate
                if metadata is not None:
                    origSize = len(metadata)
                else:
                    origSize = 0
                maxSize = 1024 * 1024
                if newStatus in ["failed"] and origSize > maxSize:
                    metadata = metadata[:maxSize]
                # insert
                varMap = {}
                varMap[":PandaID"] = pandaID
                varMap[":metaData"] = metadata
                self.cur.execute(sql1 + comment, varMap)
                # commit
                if not self._commit():
                    raise RuntimeError("Commit error")
                regTime = naive_utcnow() - regStart
                msgStr = f"done in jobStatus={jobStatus}->{newStatus} took {regTime.seconds} sec"
                if metadata is not None:
                    msgStr += f" for {len(metadata)} (orig {origSize}) bytes"
                tmp_log.debug(msgStr)
                return True
            except Exception:
                # roll back
                self._rollback()
                if iTry + 1 < nTry:
                    tmp_log.debug(f"retry : {iTry}")
                    time.sleep(random.randint(10, 20))
                    continue
                self.dump_error_message(tmp_log)
                return False

    # add stdout
    def addStdOut(self, pandaID, stdOut):
        comment = " /* DBProxy.addStdOut */"
        tmp_log = self.create_tagged_logger(comment, f"PandaID={pandaID}")
        tmp_log.debug(f"start")
        sqlJ = "SELECT PandaID FROM ATLAS_PANDA.jobsActive4 WHERE PandaID=:PandaID FOR UPDATE "
        sqlC = "SELECT PandaID FROM ATLAS_PANDA.jobsDebug WHERE PandaID=:PandaID "
        sqlI = "INSERT INTO ATLAS_PANDA.jobsDebug (PandaID,stdOut) VALUES (:PandaID,:stdOut) "
        sqlU = "UPDATE ATLAS_PANDA.jobsDebug SET stdOut=:stdOut WHERE PandaID=:PandaID "
        try:
            # autocommit on
            self.conn.begin()
            # select
            varMap = {}
            varMap[":PandaID"] = pandaID
            self.cur.arraysize = 10
            # check job table
            self.cur.execute(sqlJ + comment, varMap)
            res = self.cur.fetchone()
            if res is None:
                tmp_log.debug(f"addStdOut : {pandaID} non active")
            else:
                # check debug table
                self.cur.execute(sqlC + comment, varMap)
                res = self.cur.fetchone()
                # already exist
                if res is not None:
                    # update
                    sql = sqlU
                else:
                    # insert
                    sql = sqlI
                # write stdout
                varMap = {}
                varMap[":PandaID"] = pandaID
                varMap[":stdOut"] = stdOut
                self.cur.execute(sql + comment, varMap)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            return True
        except Exception:
            # roll back
            self._rollback()
            self.dump_error_message(tmp_log)
            return False

    # get job statistics
    def getJobStatistics(self):
        comment = " /* DBProxy.getJobStatistics */"
        tmp_log = self.create_tagged_logger(comment)
        tmp_log.debug("start")

        # tables to query
        jobs_active_4_table = f"{panda_config.schemaPANDA}.jobsActive4"
        jobs_defined_4_table = f"{panda_config.schemaPANDA}.jobsDefined4"
        tables = [jobs_active_4_table, jobs_defined_4_table]

        # states that are necessary and irrelevant states
        included_states = ["assigned", "activated", "running"]
        excluded_states = ["merging"]

        # sql template for jobs table
        sql_template = f"SELECT computingSite, jobStatus, COUNT(*) FROM {{table_name}} GROUP BY computingSite, jobStatus"
        # sql template for statistics table (materialized view)
        sql_mv_template = sql_template.replace("COUNT(*)", "SUM(num_of_jobs)")
        sql_mv_template = sql_mv_template.replace("SELECT ", "SELECT /*+ RESULT_CACHE */ ")
        ret = {}
        max_retries = 3

        for retry in range(max_retries):
            try:
                for table in tables:
                    # start transaction
                    self.conn.begin()
                    var_map = {}
                    self.cur.arraysize = 10000

                    # for active jobs we will query the summarized materialized view
                    if table == jobs_active_4_table:
                        table_name = f"{panda_config.schemaPANDA}.MV_JOBSACTIVE4_STATS"
                        sql = (sql_mv_template + comment).format(table_name=table_name)
                    # for defined jobs we will query the actual table
                    else:
                        table_name = table
                        sql = (sql_template + comment).format(table_name=table_name)
                    tmp_log.debug(f"Will execute: {sql} {str(var_map)}")

                    self.cur.execute(sql, var_map)
                    res = self.cur.fetchall()
                    if not self._commit():
                        raise RuntimeError("Commit error")

                    # create map
                    for computing_site, job_status, n_jobs in res:
                        if job_status in excluded_states:  # ignore some job status since they break APF
                            continue

                        ret.setdefault(computing_site, {}).setdefault(job_status, 0)
                        ret[computing_site][job_status] += n_jobs

                # fill in missing states with 0
                for site in ret:
                    for state in included_states:
                        ret[site].setdefault(state, 0)

                tmp_log.debug(f"done")
                return ret

            except Exception:
                self._rollback()

                if retry + 1 < max_retries:  # wait 2 seconds before the next retry
                    tmp_log.debug(f"retry: {retry}")
                    time.sleep(2)
                else:  # reached max retries - leave
                    self.dump_error_message(tmp_log)
                    return {}

    # get job statistics per site and resource type (SCORE, MCORE, ...)
    def getJobStatisticsPerSiteResource(self, time_window):
        comment = " /* DBProxy.getJobStatisticsPerSiteResource */"
        tmp_log = self.create_tagged_logger(comment)
        tmp_log.debug("start")

        tables = ["ATLAS_PANDA.jobsActive4", "ATLAS_PANDA.jobsDefined4", "ATLAS_PANDA.jobsArchived4"]

        # basic SQL for active and defined jobs
        sql = "SELECT computingSite, jobStatus, resource_type, COUNT(*) FROM %s GROUP BY computingSite, jobStatus, resource_type "

        # SQL for archived table including time window
        sql_archive = (
            "SELECT /*+ INDEX_RS_ASC(tab (MODIFICATIONTIME PRODSOURCELABEL)) */ computingSite, jobStatus, resource_type, COUNT(*) "
            "FROM ATLAS_PANDA.jobsArchived4 tab WHERE modificationTime > :modificationTime "
            "GROUP BY computingSite, jobStatus, resource_type "
        )

        # sql for materialized view
        sql_mv = re.sub("COUNT\(\*\)", "SUM(njobs)", sql)
        sql_mv = re.sub("SELECT ", "SELECT /*+ RESULT_CACHE */ ", sql_mv)

        ret = dict()
        try:
            # calculate the time floor based on the window specified by the caller
            if time_window is None:
                time_floor = naive_utcnow() - datetime.timedelta(hours=12)
            else:
                time_floor = naive_utcnow() - datetime.timedelta(minutes=int(time_window))

            for table in tables:
                # start transaction
                self.conn.begin()
                self.cur.arraysize = 10000
                # select

                var_map = {}
                if table == "ATLAS_PANDA.jobsArchived4":
                    var_map[":modificationTime"] = time_floor
                    sql_tmp = sql_archive + comment
                elif table == "ATLAS_PANDA.jobsActive4":
                    sql_tmp = (sql_mv + comment) % "ATLAS_PANDA.JOBS_SHARE_STATS"
                else:
                    sql_tmp = (sql + comment) % table

                self.cur.execute(sql_tmp, var_map)
                res = self.cur.fetchall()

                # commit
                if not self._commit():
                    raise RuntimeError("Commit error")

                # create map
                for computing_site, job_status, resource_type, n_jobs in res:
                    ret.setdefault(computing_site, dict()).setdefault(resource_type, dict()).setdefault(job_status, 0)
                    ret[computing_site][resource_type][job_status] += n_jobs

            # fill in missing states with 0
            included_states = ["assigned", "activated", "running", "finished", "failed"]
            for computing_site in ret:
                for resource_type in ret[computing_site]:
                    for job_status in included_states:
                        ret[computing_site][resource_type].setdefault(job_status, 0)

            tmp_log.debug("done")
            return ret
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return dict()

    # get the number of job for a user
    def getNumberJobsUser(self, dn, workingGroup=None):
        comment = " /* DBProxy.getNumberJobsUser */"
        tmp_log = self.create_tagged_logger(comment, f"DN={dn}")
        tmp_log.debug(f"workingGroup={workingGroup})")
        # get compact DN
        compactDN = CoreUtils.clean_user_id(dn)
        if compactDN in ["", "NULL", None]:
            compactDN = dn
        if workingGroup is not None:
            sql0 = "SELECT COUNT(*) FROM %s WHERE prodUserName=:prodUserName AND prodSourceLabel=:prodSourceLabel AND workingGroup=:workingGroup "
        else:
            sql0 = "SELECT COUNT(*) FROM %s WHERE prodUserName=:prodUserName AND prodSourceLabel=:prodSourceLabel AND workingGroup IS NULL "
        sql0 += "AND NOT jobStatus IN (:failed,:merging) "
        nTry = 1
        nJob = 0
        for iTry in range(nTry):
            try:
                for table in ("ATLAS_PANDA.jobsActive4", "ATLAS_PANDA.jobsDefined4"):
                    # start transaction
                    self.conn.begin()
                    # select
                    varMap = {}
                    varMap[":prodUserName"] = compactDN
                    varMap[":prodSourceLabel"] = "user"
                    varMap[":failed"] = "failed"
                    varMap[":merging"] = "merging"
                    if workingGroup is not None:
                        varMap[":workingGroup"] = workingGroup
                    self.cur.arraysize = 10
                    self.cur.execute((sql0 + comment) % table, varMap)
                    res = self.cur.fetchall()
                    # commit
                    if not self._commit():
                        raise RuntimeError("Commit error")
                    # create map
                    if len(res) != 0:
                        nJob += res[0][0]
                # return
                tmp_log.debug(f"{nJob}")
                return nJob
            except Exception:
                # roll back
                self._rollback()
                if iTry + 1 < nTry:
                    time.sleep(2)
                    continue
                self.dump_error_message(tmp_log)
                return 0

    # get job statistics for ExtIF. Source type is analysis or production
    def getJobStatisticsForExtIF(self, source_type=None):
        comment = " /* DBProxy.getJobStatisticsForExtIF */"
        tmp_log = self.create_tagged_logger(comment)
        tmp_log.debug(f"start source_type={source_type}")

        time_floor = naive_utcnow() - datetime.timedelta(hours=12)

        # analysis
        if source_type == "analysis":
            sql = "SELECT jobStatus, COUNT(*), cloud FROM %s WHERE prodSourceLabel IN (:prodSourceLabel1, :prodSourceLabel2) GROUP BY jobStatus, cloud"

            sql_archived = (
                "SELECT /* use_json_type */ /*+ INDEX_RS_ASC(tab (MODIFICATIONTIME PRODSOURCELABEL)) */ "
                "jobStatus, COUNT(*), tabS.data.cloud "
                "FROM %s tab, ATLAS_PANDA.schedconfig_json tabS "
                "WHERE prodSourceLabel IN (:prodSourceLabel1, :prodSourceLabel2) "
                "AND tab.computingSite = tabS.panda_queue "
                "AND modificationTime>:modificationTime GROUP BY tab.jobStatus,tabS.data.cloud"
            )

        # production
        else:
            prod_source_label_string = ":prodSourceLabel1, " + ", ".join(f":prodSourceLabel_{label}" for label in JobUtils.list_ptest_prod_sources)
            sql = (
                "SELECT /* use_json_type */ tab.jobStatus, COUNT(*), tabS.data.cloud "
                "FROM %s tab, ATLAS_PANDA.schedconfig_json tabS "
                f"WHERE prodSourceLabel IN ({prod_source_label_string}) "
                "AND tab.computingSite = tabS.panda_queue "
                "GROUP BY tab.jobStatus, tabS.data.cloud"
            )

            sql_archived = (
                "SELECT /* use_json_type */ /*+ INDEX_RS_ASC(tab (MODIFICATIONTIME PRODSOURCELABEL)) */ "
                "jobStatus, COUNT(*), tabS.data.cloud "
                "FROM %s tab, ATLAS_PANDA.schedconfig_json tabS "
                f"WHERE prodSourceLabel IN ({prod_source_label_string}) "
                "AND tab.computingSite = tabS.panda_queue "
                "AND modificationTime>:modificationTime GROUP BY tab.jobStatus,tabS.data.cloud"
            )

        # sql for materialized view
        sql_active_mv = re.sub("COUNT\(\*\)", "SUM(num_of_jobs)", sql)
        sql_active_mv = re.sub("SELECT ", "SELECT /*+ RESULT_CACHE */ ", sql_active_mv)

        ret = {}

        tables = ["ATLAS_PANDA.jobsActive4", "ATLAS_PANDA.jobsArchived4", "ATLAS_PANDA.jobsDefined4"]
        try:
            for table in tables:
                # start transaction
                self.conn.begin()

                # select
                var_map = {}
                if source_type == "analysis":
                    var_map[":prodSourceLabel1"] = "user"
                    var_map[":prodSourceLabel2"] = "panda"
                else:
                    var_map[":prodSourceLabel1"] = "managed"
                    for tmp_label in JobUtils.list_ptest_prod_sources:
                        tmp_key = f":prodSourceLabel_{tmp_label}"
                        var_map[tmp_key] = tmp_label

                if table != "ATLAS_PANDA.jobsArchived4":
                    self.cur.arraysize = 10000
                    # active uses materialized view
                    if table == "ATLAS_PANDA.jobsActive4":
                        self.cur.execute(
                            (sql_active_mv + comment) % "ATLAS_PANDA.MV_JOBSACTIVE4_STATS",
                            var_map,
                        )
                    # defined and waiting tables
                    else:
                        self.cur.execute((sql + comment) % table, var_map)
                else:
                    var_map[":modificationTime"] = time_floor
                    self.cur.arraysize = 10000
                    self.cur.execute((sql_archived + comment) % table, var_map)
                res = self.cur.fetchall()

                # commit
                if not self._commit():
                    raise RuntimeError("Commit error")

                # create map
                for job_status, count, cloud in res:
                    ret.setdefault(cloud, dict())
                    ret[cloud].setdefault(job_status, 0)
                    ret[cloud][job_status] += count

            # return
            tmp_log.debug(f"done")
            return ret
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return {}

    # get statistics for production jobs per processingType
    def getJobStatisticsPerProcessingType(self):
        comment = " /* DBProxy.getJobStatisticsPerProcessingType */"
        tmp_log = self.create_tagged_logger(comment)
        tmp_log.debug("start")

        time_floor = naive_utcnow() - datetime.timedelta(hours=12)

        # Job tables we are going to query
        tables = ["ATLAS_PANDA.jobsActive4", "ATLAS_PANDA.jobsArchived4", "ATLAS_PANDA.jobsDefined4"]

        # Define the prodSourceLabel list
        prod_source_labels = ", ".join([f":prodSourceLabel_{label}" for label in JobUtils.list_ptest_prod_sources])

        # Construct the SQL query for active jobs
        sql_active = (
            f"SELECT /* use_json_type */ jobStatus, COUNT(*), tabS.data.cloud, processingType "
            f"FROM %s tab, ATLAS_PANDA.schedconfig_json tabS "
            f"WHERE prodSourceLabel IN (:prodSourceLabelManaged, {prod_source_labels}) "
            f"AND computingSite=tabS.panda_queue "
            f"GROUP BY jobStatus, tabS.data.cloud, processingType"
        )

        # Construct the SQL query for archived jobs
        sql_archived = (
            f"SELECT /* use_json_type */ /*+ INDEX_RS_ASC(tab (MODIFICATIONTIME PRODSOURCELABEL)) */ "
            f"jobStatus, COUNT(*), tabS.data.cloud, processingType "
            f"FROM %s tab, ATLAS_PANDA.schedconfig_json tabS "
            f"WHERE prodSourceLabel IN (:prodSourceLabelManaged, {prod_source_labels}) "
            f"AND modificationTime > :modificationTime "
            f"AND computingSite = tabS.panda_queue "
            f"GROUP BY jobStatus, tabS.data.cloud, processingType"
        )

        # sql for materialized view
        sql_active_mv = re.sub("COUNT\(\*\)", "SUM(num_of_jobs)", sql_active)
        sql_active_mv = re.sub("SELECT ", "SELECT /*+ RESULT_CACHE */ ", sql_active_mv)

        ret = {}
        try:
            for table in tables:
                # start transaction
                self.conn.begin()

                # select
                self.cur.arraysize = 10000
                var_map = {":prodSourceLabelManaged": "managed"}
                var_map.update({f":prodSourceLabel_{label}": label for label in JobUtils.list_ptest_prod_sources})

                if table == "ATLAS_PANDA.jobsArchived4":
                    var_map[":modificationTime"] = time_floor
                    self.cur.execute((sql_archived + comment) % table, var_map)

                elif table == "ATLAS_PANDA.jobsActive4":
                    self.cur.execute(
                        (sql_active_mv + comment) % "ATLAS_PANDA.MV_JOBSACTIVE4_STATS",
                        var_map,
                    )
                else:
                    # use real table since coreCount is unavailable in MatView
                    self.cur.execute((sql_active + comment) % table, var_map)

                results = self.cur.fetchall()

                if not self._commit():
                    raise RuntimeError("Commit error")

                # create map
                for row in results:
                    job_status, count, cloud, processing_type = row
                    ret.setdefault(cloud, {}).setdefault(processing_type, {}).setdefault(job_status, 0)
                    ret[cloud][processing_type][job_status] += count

            tmp_log.debug(f"done")
            return ret
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return {}

    # peek at job
    def peekJobLog(self, pandaID, days=None):
        comment = " /* DBProxy.peekJobLog */"
        tmp_log = self.create_tagged_logger(comment, f"PandaID={pandaID}")
        tmp_log.debug(f"days={days}")
        # return None for NULL PandaID
        if pandaID in ["NULL", "", "None", None]:
            return None
        sql1_0 = "SELECT %s FROM %s "
        sql1_1 = "WHERE PandaID=:PandaID AND modificationTime>(CURRENT_DATE-:days) "
        # select
        varMap = {}
        varMap[":PandaID"] = pandaID
        if days is None:
            days = 30
        varMap[":days"] = days
        nTry = 1
        for iTry in range(nTry):
            try:
                # get list of archived tables
                tables = [f"{panda_config.schemaPANDAARCH}.jobsArchived"]
                # select
                for table in tables:
                    # start transaction
                    self.conn.begin()
                    # select
                    sql = sql1_0 % (JobSpec.columnNames(), table) + sql1_1
                    self.cur.arraysize = 10
                    self.cur.execute(sql + comment, varMap)
                    res = self.cur.fetchall()
                    # commit
                    if not self._commit():
                        raise RuntimeError("Commit error")
                    if len(res) != 0:
                        # Job
                        job = JobSpec()
                        job.pack(res[0])
                        # Files
                        # start transaction
                        self.conn.begin()
                        # select
                        fileTableName = re.sub("jobsArchived", "filesTable_ARCH", table)
                        sqlFile = f"SELECT /*+ INDEX(tab FILES_ARCH_PANDAID_IDX)*/ {FileSpec.columnNames()} "
                        sqlFile += f"FROM {fileTableName} tab "
                        # put constraint on modificationTime to avoid full table scan
                        sqlFile += "WHERE PandaID=:PandaID AND modificationTime>(CURRENT_DATE-:days)"
                        self.cur.arraysize = 10000
                        self.cur.execute(sqlFile + comment, varMap)
                        resFs = self.cur.fetchall()
                        # metadata
                        varMap = {}
                        varMap[":PandaID"] = job.PandaID
                        job.metadata = None
                        metaTableName = re.sub("jobsArchived", "metaTable_ARCH", table)
                        sqlMeta = f"SELECT metaData FROM {metaTableName} WHERE PandaID=:PandaID"
                        self.cur.execute(sqlMeta + comment, varMap)
                        for (clobMeta,) in self.cur:
                            if clobMeta is not None:
                                try:
                                    job.metadata = clobMeta.read()
                                except AttributeError:
                                    job.metadata = str(clobMeta)
                            break
                        # job parameters
                        job.jobParameters = None
                        jobParamTableName = re.sub("jobsArchived", "jobParamsTable_ARCH", table)
                        sqlJobP = f"SELECT jobParameters FROM {jobParamTableName} WHERE PandaID=:PandaID"
                        varMap = {}
                        varMap[":PandaID"] = job.PandaID
                        self.cur.execute(sqlJobP + comment, varMap)
                        for (clobJobP,) in self.cur:
                            if clobJobP is not None:
                                try:
                                    job.jobParameters = clobJobP.read()
                                except AttributeError:
                                    job.jobParameters = str(clobJobP)
                            break
                        # commit
                        if not self._commit():
                            raise RuntimeError("Commit error")
                        # set files
                        for resF in resFs:
                            file = FileSpec()
                            file.pack(resF)
                            # remove redundant white spaces
                            try:
                                file.md5sum = file.md5sum.strip()
                            except Exception:
                                pass
                            try:
                                file.checksum = file.checksum.strip()
                            except Exception:
                                pass
                            job.addFile(file)
                        return job
                tmp_log.debug(f"not found")
                return None
            except Exception:
                # roll back
                self._rollback()
                if iTry + 1 < nTry:
                    tmp_log.error(f"retry {iTry}")
                    time.sleep(random.randint(10, 20))
                    continue
                self.dump_error_message(tmp_log)
                # return None
                return None

    # throttle user jobs
    def throttleUserJobs(self, prodUserName, workingGroup, get_dict):
        comment = " /* DBProxy.throttleUserJobs */"
        tmp_log = self.create_tagged_logger(comment, f"user={prodUserName} group={workingGroup}")
        tmp_log.debug("start")
        try:
            # sql to get tasks
            sqlT = "SELECT /*+ INDEX_RS_ASC(tab JOBSACTIVE4_PRODUSERNAMEST_IDX) */ "
            sqlT += "distinct jediTaskID "
            sqlT += "FROM ATLAS_PANDA.jobsActive4 tab "
            sqlT += "WHERE prodSourceLabel=:prodSourceLabel AND prodUserName=:prodUserName "
            sqlT += "AND jobStatus=:oldJobStatus AND relocationFlag=:oldRelFlag "
            sqlT += "AND maxCpuCount>:maxTime "
            if workingGroup is not None:
                sqlT += "AND workingGroup=:workingGroup "
            else:
                sqlT += "AND workingGroup IS NULL "
            # sql to get jobs
            sqlJ = (
                "SELECT "
                "PandaID, jediTaskID, cloud, computingSite, prodSourceLabel "
                "FROM ATLAS_PANDA.jobsActive4 "
                "WHERE jediTaskID=:jediTaskID "
                "AND jobStatus=:oldJobStatus AND relocationFlag=:oldRelFlag "
                "AND maxCpuCount>:maxTime "
            )
            # sql to update job
            sqlU = (
                "UPDATE {0}.jobsActive4 SET jobStatus=:newJobStatus,relocationFlag=:newRelFlag "
                "WHERE jediTaskID=:jediTaskID AND jobStatus=:oldJobStatus AND maxCpuCount>:maxTime"
            ).format(panda_config.schemaPANDA)
            # start transaction
            self.conn.begin()
            # select
            self.cur.arraysize = 10
            varMap = {}
            varMap[":prodSourceLabel"] = "user"
            varMap[":oldRelFlag"] = 1
            varMap[":prodUserName"] = prodUserName
            varMap[":oldJobStatus"] = "activated"
            varMap[":maxTime"] = 6 * 60 * 60
            if workingGroup is not None:
                varMap[":workingGroup"] = workingGroup
            # get tasks
            self.cur.execute(sqlT + comment, varMap)
            resT = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # loop over all tasks
            tasks = [jediTaskID for jediTaskID, in resT]
            random.shuffle(tasks)
            nRow = 0
            nRows = {}
            for jediTaskID in tasks:
                tmp_log.debug(f"reset jediTaskID={jediTaskID}")
                # start transaction
                self.conn.begin()
                # get jobs
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                varMap[":oldRelFlag"] = 1
                varMap[":oldJobStatus"] = "activated"
                varMap[":maxTime"] = 6 * 60 * 60
                self.cur.execute(sqlJ + comment, varMap)
                resJ = self.cur.fetchall()
                infoMapDict = {
                    pandaID: {
                        "computingSite": computingSite,
                        "cloud": cloud,
                        "prodSourceLabel": prodSourceLabel,
                    }
                    for pandaID, taskID, cloud, computingSite, prodSourceLabel in resJ
                }
                # update jobs
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                varMap[":newRelFlag"] = 3
                varMap[":newJobStatus"] = "throttled"
                varMap[":oldJobStatus"] = "activated"
                varMap[":maxTime"] = 6 * 60 * 60
                self.cur.execute(sqlU + comment, varMap)
                nTmp = self.cur.rowcount
                tmp_log.debug(f"reset {nTmp} jobs")
                if nTmp > 0:
                    nRow += nTmp
                    nRows[jediTaskID] = nTmp
                for pandaID in infoMapDict:
                    infoMap = infoMapDict[pandaID]
                    self.recordStatusChange(
                        pandaID,
                        varMap[":newJobStatus"],
                        infoMap=infoMap,
                        useCommit=False,
                    )
                # commit
                if not self._commit():
                    raise RuntimeError("Commit error")
            if get_dict:
                tmp_log.debug(f"done with {nRows}")
                return nRows
            tmp_log.debug(f"done with {nRow}")
            return nRow
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return None

    # unthrottle user jobs
    def unThrottleUserJobs(self, prodUserName, workingGroup, get_dict):
        comment = " /* DBProxy.unThrottleUserJobs */"
        tmp_log = self.create_tagged_logger(comment, f"user={prodUserName} group={workingGroup}")
        tmp_log.debug("start")
        try:
            # sql to get tasks
            sqlT = "SELECT /*+ INDEX_RS_ASC(tab JOBSACTIVE4_PRODUSERNAMEST_IDX) */ "
            sqlT += "distinct jediTaskID "
            sqlT += "FROM ATLAS_PANDA.jobsActive4 tab "
            sqlT += "WHERE prodSourceLabel=:prodSourceLabel AND prodUserName=:prodUserName "
            sqlT += "AND jobStatus=:oldJobStatus AND relocationFlag=:oldRelFlag "
            if workingGroup is not None:
                sqlT += "AND workingGroup=:workingGroup "
            else:
                sqlT += "AND workingGroup IS NULL "
            # sql to get jobs
            sqlJ = (
                "SELECT "
                "PandaID, jediTaskID, cloud, computingSite, prodSourceLabel "
                "FROM ATLAS_PANDA.jobsActive4 "
                "WHERE jediTaskID=:jediTaskID "
                "AND jobStatus=:oldJobStatus AND relocationFlag=:oldRelFlag "
            )
            # sql to update job
            sqlU = (
                "UPDATE {0}.jobsActive4 SET jobStatus=:newJobStatus,relocationFlag=:newRelFlag " "WHERE jediTaskID=:jediTaskID AND jobStatus=:oldJobStatus "
            ).format(panda_config.schemaPANDA)
            # start transaction
            self.conn.begin()
            # select
            self.cur.arraysize = 10
            varMap = {}
            varMap[":prodSourceLabel"] = "user"
            varMap[":oldRelFlag"] = 3
            varMap[":prodUserName"] = prodUserName
            varMap[":oldJobStatus"] = "throttled"
            if workingGroup is not None:
                varMap[":workingGroup"] = workingGroup
            # get tasks
            self.cur.execute(sqlT + comment, varMap)
            resT = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # loop over all tasks
            tasks = [jediTaskID for jediTaskID, in resT]
            random.shuffle(tasks)
            nRow = 0
            nRows = {}
            for jediTaskID in tasks:
                tmp_log.debug(f"reset jediTaskID={jediTaskID}")
                # start transaction
                self.conn.begin()
                # get jobs
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                varMap[":oldRelFlag"] = 3
                varMap[":oldJobStatus"] = "throttled"
                self.cur.execute(sqlJ + comment, varMap)
                resJ = self.cur.fetchall()
                infoMapDict = {
                    pandaID: {
                        "computingSite": computingSite,
                        "cloud": cloud,
                        "prodSourceLabel": prodSourceLabel,
                    }
                    for pandaID, taskID, cloud, computingSite, prodSourceLabel in resJ
                }
                # update jobs
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                varMap[":newRelFlag"] = 1
                varMap[":newJobStatus"] = "activated"
                varMap[":oldJobStatus"] = "throttled"
                self.cur.execute(sqlU + comment, varMap)
                nTmp = self.cur.rowcount
                tmp_log.debug(f"reset {nTmp} jobs")
                if nTmp > 0:
                    nRow += nTmp
                    nRows[jediTaskID] = nTmp
                for pandaID in infoMapDict:
                    infoMap = infoMapDict[pandaID]
                    self.recordStatusChange(
                        pandaID,
                        varMap[":newJobStatus"],
                        infoMap=infoMap,
                        useCommit=False,
                    )
                # commit
                if not self._commit():
                    raise RuntimeError("Commit error")
            if get_dict:
                tmp_log.debug(f"done with {nRows}")
                return nRows
            tmp_log.debug(f"done with {nRow}")
            return nRow
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return None

    # get the list of jobdefIDs for failed jobs in a task
    def getJobdefIDsForFailedJob(self, jediTaskID):
        comment = " /* DBProxy.getJobdefIDsForFailedJob */"
        tmp_log = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID}")
        tmp_log.debug(f"start")
        try:
            # begin transaction
            self.conn.begin()
            # dql to get jobDefIDs
            sqlGF = "SELECT distinct jobDefinitionID FROM ATLAS_PANDA.jobsActive4 "
            sqlGF += "WHERE jediTaskID=:jediTaskID AND jobStatus=:jobStatus "
            sqlGF += "AND attemptNr<maxAttempt "
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            varMap[":jobStatus"] = "failed"
            self.cur.execute(sqlGF + comment, varMap)
            resGF = self.cur.fetchall()
            retList = []
            for (jobDefinitionID,) in resGF:
                retList.append(jobDefinitionID)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmp_log.debug(f"{str(retList)}")
            return retList
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return []

    # check validity of merge job
    def isValidMergeJob(self, pandaID, jediTaskID):
        comment = " /* DBProxy.isValidMergeJob */"
        tmp_log = self.create_tagged_logger(comment, f"PandaID={pandaID} jediTaskID={jediTaskID}")
        tmp_log.debug("start")
        try:
            retVal = True
            retMsg = ""
            # sql to check if merge job is active
            sqlJ = "SELECT jobStatus FROM ATLAS_PANDA.jobsDefined4 WHERE PandaID=:PandaID "
            sqlJ += "UNION "
            sqlJ += "SELECT jobStatus FROM ATLAS_PANDA.jobsActive4 WHERE PandaID=:PandaID "
            # sql to get input files
            sqlF = "SELECT datasetID,fileID FROM ATLAS_PANDA.filesTable4 "
            sqlF += "WHERE PandaID=:PandaID AND type IN (:type1,:type2) "
            # sql to get PandaIDs for pre-merged jobs
            sqlP = "SELECT outPandaID FROM ATLAS_PANDA.JEDI_Dataset_Contents "
            sqlP += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID AND type<>:type1"
            # sql to check if pre-merge job is active
            sqlC = "SELECT jobStatus FROM ATLAS_PANDA.jobsActive4 WHERE PandaID=:PandaID "
            # begin transaction
            self.conn.begin()
            # check if merge job is active
            varMap = {}
            varMap[":PandaID"] = pandaID
            self.cur.execute(sqlJ + comment, varMap)
            resJ = self.cur.fetchone()
            if resJ is None:
                tmp_log.debug("merge job not found")
            else:
                # get input files
                varMap = {}
                varMap[":PandaID"] = pandaID
                varMap[":type1"] = "input"
                varMap[":type2"] = "pseudo_input"
                self.cur.execute(sqlF + comment, varMap)
                resF = self.cur.fetchall()
                firstDatasetID = None
                fileIDsMap = {}
                for datasetID, fileID in resF:
                    if datasetID not in fileIDsMap:
                        fileIDsMap[datasetID] = set()
                    fileIDsMap[datasetID].add(fileID)
                # get PandaIDs for pre-merged jobs
                pandaIDs = set()
                for datasetID in fileIDsMap:
                    fileIDs = fileIDsMap[datasetID]
                    for fileID in fileIDs:
                        varMap = {}
                        varMap[":jediTaskID"] = jediTaskID
                        varMap[":datasetID"] = datasetID
                        varMap[":fileID"] = fileID
                        varMap[":type1"] = "lib"
                        self.cur.execute(sqlP + comment, varMap)
                        resP = self.cur.fetchone()
                        if resP is not None and resP[0] is not None:
                            pandaIDs.add(resP[0])
                    # only files in the first dataset are enough
                    if len(pandaIDs) > 0:
                        break
                # check pre-merge job
                for tmpPandaID in pandaIDs:
                    varMap = {}
                    varMap[":PandaID"] = tmpPandaID
                    self.cur.execute(sqlC + comment, varMap)
                    resC = self.cur.fetchone()
                    if resC is None:
                        # not found
                        tmp_log.debug(f"pre-merge job {tmpPandaID} not found")
                        retVal = False
                        retMsg = tmpPandaID
                        break
                    elif resC[0] != "merging":
                        # not in merging
                        tmp_log.debug("pre-merge job in {0} != merging".format(tmpPandaID, resC[0]))
                        retVal = False
                        retMsg = tmpPandaID
                        break
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmp_log.debug(f"ret={retVal}")
            return retVal, retMsg
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return None, ""

    # check Job status
    def checkJobStatus(self, pandaID):
        comment = " /* DBProxy.checkJobStatus */"
        tmp_log = self.create_tagged_logger(comment, f"PandaID={pandaID}")
        tmp_log.debug("start")
        retVal = {"command": None, "status": None}
        try:
            sqlC = (
                "SELECT jobStatus,commandToPilot FROM ATLAS_PANDA.jobsActive4 "
                "WHERE PandaID=:pandaID "
                "UNION "
                "SELECT /*+ INDEX_RS_ASC(JOBSARCHIVED4 PART_JOBSARCHIVED4_PK) */ "
                "jobStatus,commandToPilot FROM ATLAS_PANDA.jobsArchived4 "
                "WHERE PandaID=:pandaID AND modificationTime>:timeLimit "
            )
            varMap = dict()
            varMap[":pandaID"] = int(pandaID)
            varMap[":timeLimit"] = naive_utcnow() - datetime.timedelta(hours=1)
            # begin transaction
            self.conn.begin()
            # select
            self.cur.arraysize = 10
            self.cur.execute(sqlC + comment, varMap)
            res = self.cur.fetchone()
            if res is not None:
                retVal["status"], retVal["command"] = res
            else:
                retVal["status"], retVal["command"] = "unknown", "tobekilled"
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmp_log.debug(f"done with {str(retVal)}")
            return retVal
        except Exception:
            # roll back
            self._rollback()
            self.dump_error_message(tmp_log)
            return retVal

    # get active job attribute
    def getActiveJobAttributes(self, pandaID, attrs):
        comment = " /* DBProxy.getActiveJobAttributes */"
        tmp_log = self.create_tagged_logger(comment, f"PandaID={pandaID}")
        tmp_log.debug("start")
        try:
            sqlS = f"SELECT {','.join(attrs)} FROM ATLAS_PANDA.jobsActive4 "
            sqlS += "WHERE PandaID=:PandaID "
            # start transaction
            self.conn.begin()
            varMap = {}
            varMap[":PandaID"] = pandaID
            self.cur.execute(sqlS + comment, varMap)
            res = self.cur.fetchone()
            if res is not None:
                retMap = dict()
                for idx, attr in enumerate(attrs):
                    retMap[attr] = res[idx]
            else:
                retMap = None
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmp_log.debug(f"got {str(retMap)}")
            return retMap
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return None

    # get user job metadata
    def getUserJobMetadata(self, jediTaskID):
        comment = " /* DBProxy.getUserJobMetadata */"
        tmp_log = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID}")
        tmp_log.debug("start")
        try:
            # sql to get workers
            sqlC = "SELECT j.PandaID,m.metaData FROM {0} j,{1} m "
            sqlC += "WHERE j.jediTaskID=:jediTaskID AND j.jobStatus=:jobStatus AND m.PandaID=j.PandaID AND j.prodSourceLabel=:label "
            retMap = dict()
            for a, m in [
                ("ATLAS_PANDA.jobsArchived4", "ATLAS_PANDA.metaTable"),
                ("ATLAS_PANDAARCH.jobsArchived", "ATLAS_PANDAARCH.metaTable_ARCH"),
            ]:
                sql = sqlC.format(a, m)
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                varMap[":label"] = "user"
                varMap[":jobStatus"] = "finished"
                # start transaction
                self.conn.begin()
                self.cur.execute(sql + comment, varMap)
                resCs = self.cur.fetchall()
                for pandaID, clobMeta in resCs:
                    try:
                        metadata = clobMeta.read()
                    except AttributeError:
                        metadata = str(clobMeta)
                    try:
                        retMap[pandaID] = json.loads(metadata)
                    except Exception:
                        pass
                # commit
                if not self._commit():
                    raise RuntimeError("Commit error")
            tmp_log.debug(f"got {len(retMap)} data blocks")
            return retMap
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return {}

    # insert job output report
    def insertJobOutputReport(self, panda_id, prod_source_label, job_status, attempt_nr, data):
        comment = " /* DBProxy.insertJobOutputReport */"
        tmp_log = self.create_tagged_logger(comment, f"PandaID={panda_id} attemptNr={attempt_nr}")
        tmp_log.debug("start")
        # sql to insert
        sqlI = (
            "INSERT INTO {0}.Job_Output_Report "
            "(PandaID, prodSourceLabel, jobStatus, attemptNr, data, timeStamp) "
            "VALUES(:PandaID, :prodSourceLabel, :jobStatus, :attemptNr, :data, :timeStamp) "
        ).format(panda_config.schemaPANDA)
        try:
            retVal = False
            # start transaction
            self.conn.begin()
            # insert
            varMap = {}
            varMap[":PandaID"] = panda_id
            varMap[":prodSourceLabel"] = prod_source_label
            varMap[":jobStatus"] = job_status
            varMap[":attemptNr"] = attempt_nr
            varMap[":data"] = data
            varMap[":timeStamp"] = naive_utcnow()
            self.cur.execute(sqlI + comment, varMap)
            tmp_log.debug("successfully inserted")
            retVal = True
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmp_log.debug("done")
            return retVal
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return retVal

    # update data of job output report
    def updateJobOutputReport(self, panda_id, attempt_nr, data):
        comment = " /* DBProxy.updateJobOutputReport */"
        tmp_log = self.create_tagged_logger(comment, f"PandaID={panda_id} attemptNr={attempt_nr}")
        tmp_log.debug("start")
        # try to lock
        try:
            retVal = False
            # sql to update
            sqlU = f"UPDATE {panda_config.schemaPANDA}.Job_Output_Report SET data=:data, timeStamp=:timeStamp WHERE PandaID=:PandaID AND attemptNr=:attemptNr "
            # start transaction
            self.conn.begin()
            # update
            varMap = {}
            varMap[":PandaID"] = panda_id
            varMap[":attemptNr"] = attempt_nr
            varMap[":data"] = data
            varMap[":timeStamp"] = naive_utcnow()
            self.cur.execute(sqlU + comment, varMap)
            nRow = self.cur.rowcount
            if nRow == 1:
                tmp_log.debug("successfully updated")
                retVal = True
            elif nRow == 0:
                tmp_log.debug("entry not found, not updated")
            else:
                tmp_log.warning(f"updated unspecific number of rows: {nRow}")
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmp_log.debug("done")
            return retVal
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return retVal

    # deleted job output report
    def deleteJobOutputReport(self, panda_id, attempt_nr):
        comment = " /* DBProxy.deleteJobOutputReport */"
        tmp_log = self.create_tagged_logger(comment, f"PandaID={panda_id} attemptNr={attempt_nr}")
        tmp_log.debug("start")
        # sql to delete
        sqlD = f"DELETE FROM {panda_config.schemaPANDA}.Job_Output_Report WHERE PandaID=:PandaID AND attemptNr=:attemptNr "
        try:
            retVal = False
            # start transaction
            self.conn.begin()
            # delete
            varMap = {}
            varMap[":PandaID"] = panda_id
            varMap[":attemptNr"] = attempt_nr
            self.cur.execute(sqlD + comment, varMap)
            tmp_log.debug("successfully deleted")
            retVal = True
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmp_log.debug("done")
            return retVal
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return retVal

    # get record of a job output report
    def getJobOutputReport(self, panda_id, attempt_nr):
        comment = " /* DBProxy.getJobOutputReport */"
        tmp_log = self.create_tagged_logger(comment, f"PandaID={panda_id} attemptNr={attempt_nr}")
        tmp_log.debug("start")
        # try to lock
        try:
            retVal = {}
            # sql to get records
            sqlGR = (
                "SELECT PandaID,prodSourceLabel,jobStatus,attemptNr,data,timeStamp,lockedBy,lockedTime "
                "FROM {0}.Job_Output_Report "
                "WHERE PandaID=:PandaID AND attemptNr=:attemptNr "
            ).format(panda_config.schemaPANDA)
            # start transaction
            self.conn.begin()
            # check
            varMap = {}
            varMap[":PandaID"] = panda_id
            varMap[":attemptNr"] = attempt_nr
            self.cur.execute(sqlGR + comment, varMap)
            resGR = self.cur.fetchall()
            if not resGR:
                tmp_log.debug("record does not exist, skipped")
            for (
                PandaID,
                prodSourceLabel,
                jobStatus,
                attemptNr,
                data,
                timeStamp,
                lockedBy,
                lockedTime,
            ) in resGR:
                # fill result
                retVal = {
                    "PandaID": PandaID,
                    "jobStatus": jobStatus,
                    "attemptNr": attemptNr,
                    "timeStamp": timeStamp,
                    "data": data,
                    "lockedBy": lockedBy,
                    "lockedTime": lockedTime,
                }
                tmp_log.debug("got record")
                break
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmp_log.debug("done")
            return retVal
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return retVal

    # lock job output report
    def lockJobOutputReport(self, panda_id, attempt_nr, pid, time_limit, take_over_from=None):
        comment = " /* DBProxy.lockJobOutputReport */"
        tmp_log = self.create_tagged_logger(comment, f"PandaID={panda_id} attemptNr={attempt_nr}")
        tmp_log.debug("start")
        # try to lock
        try:
            retVal = []
            # sql to get lock
            sqlGL = (
                "SELECT PandaID,attemptNr "
                "FROM {0}.Job_Output_Report "
                "WHERE PandaID=:PandaID AND attemptNr=:attemptNr "
                "AND (lockedBy IS NULL OR lockedBy=:lockedBy OR lockedTime<:lockedTime) "
                "FOR UPDATE NOWAIT "
            ).format(panda_config.schemaPANDA)
            # sql to update lock
            sqlUL = (
                "UPDATE {0}.Job_Output_Report " "SET lockedBy=:lockedBy, lockedTime=:lockedTime " "WHERE PandaID=:PandaID AND attemptNr=:attemptNr "
            ).format(panda_config.schemaPANDA)
            # start transaction
            self.conn.begin()
            # check
            varMap = {}
            varMap[":PandaID"] = panda_id
            varMap[":attemptNr"] = attempt_nr
            if take_over_from is None:
                varMap[":lockedBy"] = pid
            else:
                varMap[":lockedBy"] = take_over_from
            varMap[":lockedTime"] = naive_utcnow() - datetime.timedelta(minutes=time_limit)
            utc_now = naive_utcnow()
            try:
                self.cur.execute(sqlGL + comment, varMap)
                resGL = self.cur.fetchall()
                if not resGL:
                    tmp_log.debug("record already locked by other thread, skipped")
            except Exception:
                resGL = None
                tmp_log.debug("record skipped due to NOWAIT")
            if resGL:
                for panda_id, attempt_nr in resGL:
                    # lock
                    varMap = {}
                    varMap[":PandaID"] = panda_id
                    varMap[":attemptNr"] = attempt_nr
                    varMap[":lockedBy"] = pid
                    varMap[":lockedTime"] = utc_now
                    self.cur.execute(sqlUL + comment, varMap)
                    if take_over_from is None:
                        tmp_log.debug(f"successfully locked record by {pid}")
                    else:
                        tmp_log.debug(f"successfully took over locked record from {take_over_from} by {pid}")
                    retVal = True
                    break
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmp_log.debug("done")
            return retVal
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return retVal

    # unlock job output report
    def unlockJobOutputReport(self, panda_id, attempt_nr, pid, lock_offset):
        comment = " /* DBProxy.unlockJobOutputReport */"
        tmp_log = self.create_tagged_logger(comment, f"PandaID={panda_id} attemptNr={attempt_nr}")
        tmp_log.debug("start")
        # try to lock
        try:
            retVal = []
            # sql to get lock
            sqlGL = (
                "SELECT PandaID,attemptNr "
                "FROM {0}.Job_Output_Report "
                "WHERE PandaID=:PandaID AND attemptNr=:attemptNr "
                "AND lockedBy=:lockedBy "
                "FOR UPDATE"
            ).format(panda_config.schemaPANDA)
            # sql to update lock
            sqlUL = f"UPDATE {panda_config.schemaPANDA}.Job_Output_Report SET lockedTime=:lockedTime WHERE PandaID=:PandaID AND attemptNr=:attemptNr "
            # start transaction
            self.conn.begin()
            # check
            varMap = {}
            varMap[":PandaID"] = panda_id
            varMap[":attemptNr"] = attempt_nr
            varMap[":lockedBy"] = pid
            self.cur.execute(sqlGL + comment, varMap)
            resGL = self.cur.fetchall()
            if not resGL:
                tmp_log.debug("record not locked by this thread, skipped")
            else:
                for panda_id, attempt_nr in resGL:
                    # lock
                    varMap = {}
                    varMap[":PandaID"] = panda_id
                    varMap[":attemptNr"] = attempt_nr
                    varMap[":lockedTime"] = naive_utcnow() - datetime.timedelta(minutes=lock_offset)
                    self.cur.execute(sqlUL + comment, varMap)
                    tmp_log.debug("successfully unlocked record")
                    retVal = True
                    break
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmp_log.debug("done")
            return retVal
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return retVal

    # list pandaID, jobStatus, attemptNr, timeStamp of job output report
    def listJobOutputReport(self, only_unlocked, time_limit, limit, grace_period, labels, anti_labels):
        comment = " /* DBProxy.listJobOutputReport */"
        tmp_log = self.create_tagged_logger(comment)
        tmp_log.debug(f"start label={str(labels)} limit={limit} anti_label={str(anti_labels)}")
        try:
            retVal = None
            if only_unlocked:
                # try to get only records unlocked or with expired lock
                varMap = {}
                varMap[":limit"] = limit * 10
                varMap[":lockedTime"] = naive_utcnow() - datetime.timedelta(minutes=time_limit)
                varMap[":timeStamp"] = naive_utcnow() - datetime.timedelta(seconds=grace_period)
                # sql to get record
                sqlGR = (
                    "SELECT * "
                    "FROM ( "
                    "SELECT PandaID,jobStatus,attemptNr,timeStamp "
                    "FROM {0}.Job_Output_Report "
                    "WHERE (lockedBy IS NULL OR lockedTime<:lockedTime) "
                    "AND timeStamp<:timeStamp ".format(panda_config.schemaPANDA)
                )
                if labels is not None:
                    label_var_names_str, label_var_map = get_sql_IN_bind_variables(labels, prefix=":l_", value_as_suffix=True)
                    sqlGR += f"AND prodSourceLabel IN ({label_var_names_str}) "
                    varMap.update(label_var_map)
                if anti_labels is not None:
                    anti_label_var_names_str, anti_label_var_map = get_sql_IN_bind_variables(anti_labels, prefix=":al_", value_as_suffix=True)
                    sqlGR += f"AND prodSourceLabel NOT IN ({anti_label_var_names_str}) "
                    varMap.update(anti_label_var_map)
                sqlGR += "ORDER BY timeStamp " ") " "WHERE rownum<=:limit "
                # start transaction
                self.conn.begin()
                # check
                self.cur.execute(sqlGR + comment, varMap)
                separator = limit // 10
                retVal = self.cur.fetchall()
                # shuffle tail to avoid conflict
                ret_head = retVal[:separator]
                ret_tail = retVal[separator:]
                random.shuffle(ret_tail)
                retVal = ret_head + ret_tail
                retVal = retVal[:limit]
                tmp_log.debug(f"listed {len(retVal)} unlocked records")
            else:
                # sql to select
                sqlS = (
                    "SELECT * "
                    "FROM ( "
                    "SELECT PandaID,jobStatus,attemptNr,timeStamp "
                    "FROM {0}.Job_Output_Report "
                    "ORDER BY timeStamp "
                    ") "
                    "WHERE rownum<=:limit "
                ).format(panda_config.schemaPANDA)
                # start transaction
                self.conn.begin()
                varMap = {}
                varMap[":limit"] = limit
                # check
                self.cur.execute(sqlS + comment, varMap)
                retVal = self.cur.fetchall()
                tmp_log.debug(f"listed {len(retVal)} records")
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmp_log.debug("done")
            return retVal
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return retVal

    # send command to a job
    def send_command_to_job(self, panda_id, com):
        comment = " /* DBProxy.send_command_to_job */"
        tmp_log = self.create_tagged_logger(comment, f"PandaID={panda_id}")
        tmp_log.debug("start")
        retVal = None
        try:
            # check length
            new_com = JobSpec.truncateStringAttr("commandToPilot", com)
            if len(new_com) != len(com):
                retVal = (
                    False,
                    f"command string too long. must be less than {len(new_com)} chars",
                )
            else:
                sqlR = "SELECT commandToPilot FROM ATLAS_PANDA.{} WHERE PandaID=:PandaID FOR UPDATE "
                sqlU = "UPDATE ATLAS_PANDA.{} SET commandToPilot=:commandToPilot " "WHERE PandaID=:PandaID "
                for table in ["jobsDefined4", "jobsActive4"]:
                    # start transaction
                    self.conn.begin()
                    # read
                    varMap = {}
                    varMap[":PandaID"] = panda_id
                    self.cur.execute(sqlR.format(table) + comment, varMap)
                    data = self.cur.fetchone()
                    if data is not None:
                        (commandToPilot,) = data
                        if commandToPilot == "tobekilled":
                            retVal = (False, "job is being killed")
                        else:
                            varMap = {}
                            varMap[":PandaID"] = panda_id
                            varMap[":commandToPilot"] = com
                            self.cur.execute(sqlU.format(table) + comment, varMap)
                            nRow = self.cur.rowcount
                            if nRow:
                                retVal = (True, "command received")
                    # commit
                    if not self._commit():
                        raise RuntimeError("Commit error")
                    if retVal is not None:
                        break
            if retVal is None:
                retVal = (False, f"no active job with PandaID={panda_id}")
            tmp_log.debug(f"done with {str(retVal)}")
            return retVal
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return False, "database error"
