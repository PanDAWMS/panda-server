"""
proxy for database connection

"""

import atexit
import copy
import datetime
import glob
import json
import logging
import math
import operator
import os
import random
import re
import socket
import sys
import time
import traceback
import uuid
import warnings

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandautils.PandaUtils import naive_utcnow

from pandaserver.config import panda_config
from pandaserver.srvcore import CoreUtils, srv_msg_utils
from pandaserver.taskbuffer import (
    ErrorCode,
    EventServiceUtils,
    GlobalShares,
    JobUtils,
    PrioUtil,
    ProcessGroups,
    SiteSpec,
    task_split_rules,
)
from pandaserver.taskbuffer.DatasetSpec import DatasetSpec
from pandaserver.taskbuffer.db_proxy_mods import job_complex_module
from pandaserver.taskbuffer.db_proxy_mods.base_module import (
    SQL_QUEUE_TOPIC_async_dataset_update,
    convert_dict_to_bind_vars,
    memoize,
    varNUMBER,
)
from pandaserver.taskbuffer.DdmSpec import DdmSpec
from pandaserver.taskbuffer.FileSpec import FileSpec
from pandaserver.taskbuffer.HarvesterMetricsSpec import HarvesterMetricsSpec
from pandaserver.taskbuffer.JobSpec import (
    JobSpec,
    get_task_queued_time,
    push_status_changes,
)
from pandaserver.taskbuffer.ResourceSpec import (
    BASIC_RESOURCE_TYPE,
    ResourceSpec,
    ResourceSpecMapper,
)
from pandaserver.taskbuffer.SupErrors import SupErrors
from pandaserver.taskbuffer.Utils import create_shards
from pandaserver.taskbuffer.WorkerSpec import WorkerSpec
from pandaserver.taskbuffer.WrappedCursor import WrappedCursor

try:
    import idds.common.constants
    import idds.common.utils
    from idds.client.client import Client as iDDS_Client
except ImportError:
    pass

if panda_config.backend == "oracle":
    import oracledb

    from . import wrapped_oracle_conn

    oracledb.init_oracle_client()

elif panda_config.backend == "postgres":
    import psycopg2 as psycopg

    from . import WrappedPostgresConn

else:
    import MySQLdb

warnings.filterwarnings("ignore")

# logger
_logger = PandaLogger().getLogger("DBProxy")
_loggerFiltered = PandaLogger().getLogger("DBProxyFiltered")

# add handlers
for handler in _loggerFiltered.handlers:
    handler.setLevel(logging.INFO)
    _logger.addHandler(handler)
    _loggerFiltered.removeHandler(handler)


# proxy
class DBProxy(job_complex_module.JobComplexModule):
    # constructor
    def __init__(self, useOtherError=False):
        # init modules
        super().__init__(_logger)
        # connection object
        self.conn = None
        # cursor object
        self.cur = None
        # host name
        self.hostname = None
        # retry count
        self.nTry = 5
        # use special error codes for reconnection in querySQL
        self.useOtherError = useOtherError
        # pledge resource ratio
        self.beyondPledgeRatio = {}
        # update time for pledge resource ratio
        self.updateTimeForPledgeRatio = None

        # hostname
        self.myHostName = socket.getfqdn()
        self.backend = panda_config.backend

    # connect to DB
    def connect(
        self,
        dbhost=panda_config.dbhost,
        dbpasswd=panda_config.dbpasswd,
        dbuser=panda_config.dbuser,
        dbname=panda_config.dbname,
        dbtimeout=panda_config.dbtimeout,
        reconnect=False,
        dbport=panda_config.dbport,
    ):
        _logger.debug(f"connect : re={reconnect}")
        # keep parameters for reconnect
        if not reconnect:
            self.dbhost = dbhost
            self.dbpasswd = dbpasswd
            self.dbuser = dbuser
            self.dbname = dbname
            self.dbtimeout = dbtimeout
            self.dbport = dbport
        # close old connection
        if reconnect:
            _logger.debug("closing old connection")
            try:
                self.conn.close()
            except Exception:
                _logger.debug("failed to close old connection")
        # connect
        try:
            if self.backend == "oracle":
                conn = oracledb.connect(dsn=self.dbhost, user=self.dbuser, password=self.dbpasswd)

                def OutputTypeHandler(cursor, name, defaultType, size, precision, scale):
                    if defaultType == oracledb.CLOB:
                        return cursor.var(oracledb.LONG_STRING, arraysize=cursor.arraysize)

                conn.outputtypehandler = OutputTypeHandler
                self.conn = wrapped_oracle_conn.WrappedOracleConn(conn)

            elif self.backend == "postgres":
                dsn = {"dbname": self.dbname, "user": self.dbuser, "keepalives_idle": 30, "keepalives_interval": 30, "keepalives": 1}
                if self.dbpasswd:
                    dsn["password"] = self.dbpasswd
                if self.dbhost:
                    dsn["host"] = self.dbhost
                if self.dbport:
                    dsn["port"] = self.dbport
                conn = psycopg.connect(**dsn)
                self.conn = WrappedPostgresConn.WrappedPostgresConn(conn)
            else:
                self.conn = MySQLdb.connect(
                    host=self.dbhost,
                    db=self.dbname,
                    port=self.dbport,
                    connect_timeout=self.dbtimeout,
                    user=self.dbuser,
                    passwd=self.dbpasswd,
                    charset="utf8",
                )
            self.cur = WrappedCursor(self.conn)
            try:
                # use SQL dumper
                if panda_config.dump_sql:
                    from pandaserver.taskbuffer.SQLDumper import SQLDumper

                    self.cur = SQLDumper(self.cur)
            except Exception:
                pass
            self.hostname = self.cur.initialize()
            if not reconnect:
                atexit.register(self.close_connection)
            _logger.debug(f"connect : re={reconnect} ready")
            return True
        except Exception as e:
            _logger.error(f"connect : {str(e)}")
            return False

    # activate job. move job from jobsDefined to jobsActive
    def activateJob(self, job):
        comment = " /* DBProxy.activateJob */"
        methodName = comment.split(" ")[-2].split(".")[-1]
        if job is None:
            tmp_id = None
        else:
            tmp_id = job.PandaID
        tmpLog = LogWrapper(_logger, methodName + f" < PandaID={tmp_id} >")
        updatedFlag = False
        if job is None:
            tmpLog.debug("skip job=None")
            return True
        tmpLog.debug("start")
        sql0 = "SELECT row_ID FROM ATLAS_PANDA.filesTable4 WHERE PandaID=:PandaID AND type=:type AND NOT status IN (:status1,:status2) "
        sql1 = "DELETE FROM ATLAS_PANDA.jobsDefined4 "
        sql1 += "WHERE PandaID=:PandaID AND (jobStatus=:oldJobStatus1 OR jobStatus=:oldJobStatus2) AND commandToPilot IS NULL"
        sql2 = f"INSERT INTO ATLAS_PANDA.jobsActive4 ({JobSpec.columnNames()}) "
        sql2 += JobSpec.bindValuesExpression()
        # host and time information
        job.modificationTime = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
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
                        tmpLog.debug("Job not found to activate")
                    else:
                        # insert
                        self.cur.execute(sql2 + comment, job.valuesMap())
                        # update files
                        for file in job.Files:
                            sqlF = f"UPDATE ATLAS_PANDA.filesTable4 SET {file.bindUpdateChangesExpression()}" + "WHERE row_ID=:row_ID"
                            varMap = file.valuesMap(onlyChanged=True)
                            if varMap != {}:
                                varMap[":row_ID"] = file.row_ID
                                _logger.debug(sqlF + comment + str(varMap))
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
                    _logger.debug(sqlJ + comment + str(varMap))
                    self.cur.execute(sqlJ + comment, varMap)
                    n = self.cur.rowcount
                    if n == 0:
                        # already killed or activated
                        tmpLog.debug("Job not found to update")
                    else:
                        # update files
                        for file in job.Files:
                            sqlF = f"UPDATE ATLAS_PANDA.filesTable4 SET {file.bindUpdateChangesExpression()}" + "WHERE row_ID=:row_ID"
                            varMap = file.valuesMap(onlyChanged=True)
                            if varMap != {}:
                                varMap[":row_ID"] = file.row_ID
                                _logger.debug(sqlF + comment + str(varMap))
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
                    tmpLog.error("recordStatusChange failed")
                self.push_job_status_message(job, job.PandaID, job.jobStatus)
                # push job
                if to_push:
                    mb_proxy_queue = self.get_mb_proxy("panda_pilot_queue")
                    mb_proxy_topic = self.get_mb_proxy("panda_pilot_topic")
                    if mb_proxy_queue and mb_proxy_topic:
                        tmpLog.debug("push job")
                        srv_msg_utils.send_job_message(mb_proxy_queue, mb_proxy_topic, job.jediTaskID, job.PandaID)
                    else:
                        tmpLog.debug("message queue/topic not configured")
                tmpLog.debug("done")
                return True
            except Exception as e:
                # roll back
                self._rollback()
                if iTry + 1 < nTry:
                    tmpLog.debug(f"retry: {iTry}")
                    time.sleep(random.randint(10, 20))
                    continue
                tmpLog.error(f"failed with {str(e)}")
                return False

    # send job to jobsWaiting
    def keepJob(self, job):
        comment = " /* DBProxy.keepJob */"
        method_name = comment.split(" ")[-2].split(".")[-1]
        method_name += f" <PandaID={job.PandaID}> "
        tmp_log = LogWrapper(_logger, method_name)
        tmp_log.debug("start")
        # set status
        job.jobStatus = "waiting"
        sql1 = f"UPDATE ATLAS_PANDA.jobsDefined4 SET {job.bindUpdateChangesExpression()} "
        sql1 += "WHERE PandaID=:PandaID AND (jobStatus=:oldJobStatus1 OR jobStatus=:oldJobStatus2) AND commandToPilot IS NULL"
        # time information
        job.modificationTime = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
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
                self.dumpErrorMessage(_logger, method_name)
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
        methodName = comment.split(" ")[-2].split(".")[-1]
        methodName += f" <PandaID={pandaID}>"
        tmpLog = LogWrapper(_logger, methodName)
        tmpLog.debug(f"activeTable={activeTable}")
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
            tmpLog.debug(f"retD = {retD}")
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
            job.modificationTime = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
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
                    tmpLog.debug(sqlF + comment + str(varMap))
                    self.cur.execute(sqlF + comment, varMap)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # record status change
            try:
                self.recordStatusChange(job.PandaID, job.jobStatus, jobInfo=job)
            except Exception:
                _logger.error("recordStatusChange in resetJobs")
            self.push_job_status_message(job, job.PandaID, job.jobStatus)
            tmpLog.debug(f"done with {job is not None}")
            if getOldSubs:
                return job, oldSubList
            return job
        except Exception:
            # roll back
            self._rollback()
            # error report
            type, value, traceBack = sys.exc_info()
            tmpLog.error(f"{type} {value}")
            return None

    # reset jobs in jobsDefined
    def resetDefinedJob(self, pandaID):
        comment = " /* DBProxy.resetDefinedJob */"
        method_name = comment.split(" ")[-2].split(".")[-1]
        method_name += f" <PandaID={pandaID}>"
        tmp_log = LogWrapper(_logger, method_name)
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
            self.dumpErrorMessage(_logger, method_name)
            # roll back
            self._rollback()
            return None

    # peek at job
    def peekJob(self, pandaID, fromDefined, fromActive, fromArchived, fromWaiting, forAnal=False):
        comment = " /* DBProxy.peekJob */"
        _logger.debug(f"peekJob : {pandaID}")
        # return None for NULL PandaID
        if pandaID in ["NULL", "", "None", None]:
            return None
        # only int
        try:
            _ = int(pandaID)
        except Exception:
            _logger.debug(f"peekJob : return None for {pandaID}:non-integer")
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
                _logger.debug(f"peekJob() : PandaID {pandaID} not found")
                return None
            except Exception:
                # roll back
                self._rollback()
                if iTry + 1 < nTry:
                    _logger.debug(f"peekJob : {pandaID} retry : {iTry}")
                    time.sleep(random.randint(10, 20))
                    continue
                type, value, traceBack = sys.exc_info()
                _logger.error(f"peekJob : {pandaID} {type} {value}")
                # return None for analysis
                if forAnal:
                    return None
                # return 'unknown'
                job = JobSpec()
                job.PandaID = pandaID
                job.jobStatus = "unknown"
                return job

    # get PandaIDs with TaskID
    def getPandaIDsWithTaskID(self, jediTaskID):
        comment = " /* DBProxy.getPandaIDsWithTaskID */"
        methodName = comment.split(" ")[-2].split(".")[-1]
        tmpLog = LogWrapper(_logger, methodName + f" <jediTaskID={jediTaskID}>")
        tmpLog.debug("start")
        # SQL
        sql = "SELECT PandaID FROM ATLAS_PANDA.jobsDefined4 "
        sql += "WHERE jediTaskID=:jediTaskID "
        sql += "UNION "
        sql += "SELECT PandaID FROM ATLAS_PANDA.jobsActive4 "
        sql += "WHERE jediTaskID=:jediTaskID "
        sql += "UNION "
        sql += "SELECT PandaID FROM ATLAS_PANDA.jobsArchived4 "
        sql += "WHERE jediTaskID=:jediTaskID "
        varMap = {}
        varMap[":jediTaskID"] = jediTaskID
        try:
            # start transaction
            self.conn.begin()
            # select
            self.cur.arraysize = 1000000
            self.cur.execute(sql + comment, varMap)
            res = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            retList = []
            for (pandaID,) in res:
                retList.append(pandaID)

            tmpLog.debug(f"found {len(retList)} IDs")
            return retList
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(_logger, methodName)
            return []

    # get express jobs
    def getExpressJobs(self, dn):
        comment = " /* DBProxy.getExpressJobs */"
        _logger.debug(f"getExpressJobs : {dn}")
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
            timeNow = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
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
                _logger.debug(sql + comment + str(varMap))
                self.cur.execute(sql + comment, varMap)
                res = self.cur.fetchall()
                _logger.debug(f"getExpressJobs {str(res)}")
                for specialHandling, countJobs in res:
                    if specialHandling is None:
                        continue
                    # look for express jobs
                    if expressStr in specialHandling:
                        varMap[":specialHandling"] = specialHandling
                        self.cur.arraysize = 1000
                        self.cur.execute(sqlJob + comment, varMap)
                        resJobs = self.cur.fetchall()
                        _logger.debug(f"getExpressJobs {str(resJobs)}")
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
            _logger.debug(f"getExpressJobs : {str(retVal)}")
            return retVal
        except Exception:
            # roll back
            self._rollback()
            errtype, errvalue = sys.exc_info()[:2]
            _logger.error(f"getExpressJobs : {errtype} {errvalue}")
            return None

    # get active debug jobs
    def getActiveDebugJobs(self, dn=None, workingGroup=None, prodRole=False):
        comment = " /* DBProxy.getActiveDebugJobs */"
        _logger.debug(f"getActiveDebugJobs : DN={dn} wg={workingGroup} prodRole={prodRole}")
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
            _logger.debug(f"getActiveDebugJobs : DN={dn} -> {str(activeDebugJobs)}")
            return activeDebugJobs
        except Exception:
            # roll back
            self._rollback()
            errtype, errvalue = sys.exc_info()[:2]
            _logger.error(f"getActiveDebugJobs : {errtype} {errvalue}")
            return None

    # set debug mode
    def setDebugMode(self, dn, pandaID, prodManager, modeOn, workingGroup):
        comment = " /* DBProxy.setDebugMode */"
        _logger.debug(f"turnDebugModeOn : dn={dn} id={pandaID} prod={prodManager} wg={workingGroup} mode={modeOn}")
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
            _logger.debug(f"setDebugMode : {dn} {pandaID} -> {retStr}")
            return retStr
        except Exception:
            # roll back
            self._rollback()
            errtype, errvalue = sys.exc_info()[:2]
            _logger.error(f"setDebugMode : {errtype} {errvalue}")
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
        _logger.debug(f"lockJobsForReassign : {tableName} {timeLimit} {statList} {labels} {processTypes} {sites} {clouds} {useJEDI}")
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
                sql += "AND jobStatus IN ("
                tmpIdx = 0
                for tmpStat in statList:
                    tmpKey = f":stat{tmpIdx}"
                    varMap[tmpKey] = tmpStat
                    sql += f"{tmpKey},"
                    tmpIdx += 1
                sql = sql[:-1]
                sql += ") "
            if labels != []:
                sql += "AND prodSourceLabel IN ("
                tmpIdx = 0
                for tmpStat in labels:
                    tmpKey = f":label{tmpIdx}"
                    varMap[tmpKey] = tmpStat
                    sql += f"{tmpKey},"
                    tmpIdx += 1
                sql = sql[:-1]
                sql += ") "
            if processTypes != []:
                sql += "AND processingType IN ("
                tmpIdx = 0
                for tmpStat in processTypes:
                    tmpKey = f":processType{tmpIdx}"
                    varMap[tmpKey] = tmpStat
                    sql += f"{tmpKey},"
                    tmpIdx += 1
                sql = sql[:-1]
                sql += ") "
            if sites != []:
                sql += "AND computingSite IN ("
                tmpIdx = 0
                for tmpStat in sites:
                    tmpKey = f":site{tmpIdx}"
                    varMap[tmpKey] = tmpStat
                    sql += f"{tmpKey},"
                    tmpIdx += 1
                sql = sql[:-1]
                sql += ") "
            if clouds != []:
                sql += "AND cloud IN ("
                tmpIdx = 0
                for tmpStat in clouds:
                    tmpKey = f":cloud{tmpIdx}"
                    varMap[tmpKey] = tmpStat
                    sql += f"{tmpKey},"
                    tmpIdx += 1
                sql = sql[:-1]
                sql += ") "
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
            _logger.debug(sql + comment + str(varMap))
            self.cur.execute(sql + comment, varMap)
            resList = self.cur.fetchall()
            _logger.debug(f"lockJobsForReassign : found {len(resList)}")
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
            _logger.debug(f"lockJobsForReassign : return {len(retList)}")
            return True, retList
        except Exception:
            # roll back
            self._rollback()
            errType, errValue = sys.exc_info()[:2]
            _logger.error(f"lockJobsForReassign : {errType} {errValue}")
            # return empty
            return False, []

    # lock jobs for finisher
    def lockJobsForFinisher(self, timeNow, rownum, highPrio):
        comment = " /* DBProxy.lockJobsForFinisher */"
        _logger.debug(f"lockJobsForFinisher : {timeNow} {rownum} {highPrio}")
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
            _logger.debug(f"lockJobsForFinisher : {len(retList)}")
            return True, retList
        except Exception:
            # roll back
            self._rollback()
            errType, errValue = sys.exc_info()[:2]
            _logger.error(f"lockJobsForFinisher : {errType} {errValue}")
            # return empty
            return False, []

    # lock jobs for activator
    def lockJobsForActivator(self, timeLimit, rownum, prio):
        comment = " /* DBProxy.lockJobsForActivator */"
        methodName = comment.split(" ")[-2].split(".")[-1]
        tmpLog = LogWrapper(_logger, methodName)
        tmpLog.debug("start")
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
            tmpLog.debug(f"locked {len(retList)} jobs")
            return True, retList
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(_logger, methodName)
            # return empty
            return False, []

    # add metadata
    def addMetadata(self, pandaID, metadata, newStatus):
        comment = " /* DBProxy.addMetaData */"
        methodName = comment.split(" ")[-2].split(".")[-1]
        tmpLog = LogWrapper(_logger, methodName + f" <PandaID={pandaID}>")
        tmpLog.debug(f"start {newStatus}")
        # discard metadata for failed jobs
        if newStatus == "failed":
            tmpLog.debug("skip")
            return True
        sqlJ = "SELECT jobStatus FROM ATLAS_PANDA.jobsActive4 WHERE PandaID=:PandaID "
        sqlJ += "UNION "
        sqlJ += "SELECT jobStatus FROM ATLAS_PANDA.jobsArchived4 WHERE PandaID=:PandaID "
        sql0 = "SELECT PandaID FROM ATLAS_PANDA.metaTable WHERE PandaID=:PandaID"
        sql1 = "INSERT INTO ATLAS_PANDA.metaTable (PandaID,metaData) VALUES (:PandaID,:metaData)"
        nTry = 1
        regStart = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
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
                    tmpLog.debug(f"skip jobStatus={jobStatus}")
                    if not self._commit():
                        raise RuntimeError("Commit error")
                    return False
                # skip if in final state
                if jobStatus in ["cancelled", "closed", "finished", "failed"]:
                    tmpLog.debug(f"skip jobStatus={jobStatus}")
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
                    tmpLog.debug(f"skip duplicated during jobStatus={jobStatus}")
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
                regTime = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - regStart
                msgStr = f"done in jobStatus={jobStatus}->{newStatus} took {regTime.seconds} sec"
                if metadata is not None:
                    msgStr += f" for {len(metadata)} (orig {origSize}) bytes"
                tmpLog.debug(msgStr)
                return True
            except Exception:
                # roll back
                self._rollback()
                if iTry + 1 < nTry:
                    tmpLog.debug(f"retry : {iTry}")
                    time.sleep(random.randint(10, 20))
                    continue
                self.dumpErrorMessage(_logger, methodName)
                return False

    # add stdout
    def addStdOut(self, pandaID, stdOut):
        comment = " /* DBProxy.addStdOut */"
        _logger.debug(f"addStdOut : {pandaID} start")
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
                _logger.debug(f"addStdOut : {pandaID} non active")
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
            errtype, errvalue = sys.exc_info()[:2]
            _logger.error(f"addStdOut : {errtype} {errvalue}")
            return False

    # insert sandbox file info
    def insertSandboxFileInfo(self, userName, hostName, fileName, fileSize, checkSum):
        comment = " /* DBProxy.insertSandboxFileInfo */"
        _logger.debug(f"insertSandboxFileInfo : {userName} {hostName} {fileName} {fileSize} {checkSum}")
        sqlC = "SELECT userName,fileSize,checkSum FROM ATLAS_PANDAMETA.userCacheUsage "
        sqlC += "WHERE hostName=:hostName AND fileName=:fileName FOR UPDATE"

        sql = "INSERT INTO ATLAS_PANDAMETA.userCacheUsage "
        sql += "(userName,hostName,fileName,fileSize,checkSum,creationTime,modificationTime) "
        sql += "VALUES (:userName,:hostName,:fileName,:fileSize,:checkSum,CURRENT_DATE,CURRENT_DATE) "

        try:
            # begin transaction
            self.conn.begin()
            # check if it already exists
            varMap = {}
            varMap[":hostName"] = hostName
            varMap[":fileName"] = fileName
            self.cur.arraysize = 10
            self.cur.execute(sqlC + comment, varMap)
            res = self.cur.fetchall()
            if len(res) != 0:
                _logger.debug(f"insertSandboxFileInfo : skip {hostName} {fileName} since already exists")
                # commit
                if not self._commit():
                    raise RuntimeError("Commit error")
                return "WARNING: file exist"
            # insert
            varMap = {}
            varMap[":userName"] = userName
            varMap[":hostName"] = hostName
            varMap[":fileName"] = fileName
            varMap[":fileSize"] = fileSize
            varMap[":checkSum"] = checkSum
            self.cur.execute(sql + comment, varMap)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            return "OK"
        except Exception:
            # roll back
            self._rollback()
            # error
            type, value, traceBack = sys.exc_info()
            _logger.error(f"insertSandboxFileInfo : {type} {value}")
            return "ERROR: DB failure"

    # get and lock sandbox files
    def getLockSandboxFiles(self, time_limit, n_files):
        comment = " /* DBProxy.getLockSandboxFiles */"
        methodName = comment.split(" ")[-2].split(".")[-1]
        tmpLog = LogWrapper(_logger, methodName)
        sqlC = (
            "SELECT * FROM ("
            "SELECT userName,hostName,fileName,creationTime,modificationTime FROM ATLAS_PANDAMETA.userCacheUsage "
            "WHERE modificationTime<:timeLimit AND (fileName like 'sources%' OR fileName like 'jobO%') ) "
            "WHERE rownum<:nRows "
        )
        sqlU = "UPDATE ATLAS_PANDAMETA.userCacheUsage SET modificationTime=CURRENT_DATE " "WHERE userName=:userName AND fileName=:fileName "
        try:
            tmpLog.debug("start")
            # begin transaction
            self.conn.begin()
            # check if it already exists
            varMap = {}
            varMap[":timeLimit"] = time_limit
            varMap[":nRows"] = n_files
            self.cur.execute(sqlC + comment, varMap)
            res = self.cur.fetchall()
            retList = []
            for userName, hostName, fileName, creationTime, modificationTime in res:
                retList.append((userName, hostName, fileName, creationTime, modificationTime))
                # lock
                varMap = dict()
                varMap[":userName"] = userName
                varMap[":fileName"] = fileName
                self.cur.execute(sqlU + comment, varMap)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmpLog.debug(f"locked {len(retList)} files")
            return retList
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(_logger, methodName)
            return None

    # check duplicated sandbox file
    def checkSandboxFile(self, dn, fileSize, checkSum):
        comment = " /* DBProxy.checkSandboxFile */"
        _logger.debug(f"checkSandboxFile : {dn} {fileSize} {checkSum}")
        sqlC = "SELECT hostName,fileName FROM ATLAS_PANDAMETA.userCacheUsage "
        sqlC += "WHERE userName=:userName AND fileSize=:fileSize AND checkSum=:checkSum "
        sqlC += "AND hostName<>:ngHostName AND creationTime>CURRENT_DATE-3 "
        sqlC += "AND creationTime>CURRENT_DATE-3 "
        try:
            retStr = "NOTFOUND"
            # get compact DN
            compactDN = CoreUtils.clean_user_id(dn)
            if compactDN in ["", "NULL", None]:
                compactDN = dn
            # begin transaction
            self.conn.begin()
            # check if it already exists
            varMap = {}
            varMap[":userName"] = compactDN
            varMap[":fileSize"] = fileSize
            varMap[":checkSum"] = str(checkSum)
            varMap[":ngHostName"] = "localhost.localdomain"
            self.cur.arraysize = 10
            self.cur.execute(sqlC + comment, varMap)
            res = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            if len(res) != 0:
                hostName, fileName = res[0]
                retStr = f"FOUND:{hostName}:{fileName}"
            _logger.debug(f"checkSandboxFile -> {retStr}")
            return retStr
        except Exception:
            # roll back
            self._rollback()
            # error
            type, value, traceBack = sys.exc_info()
            _logger.error(f"checkSandboxFile : {type} {value}")
            return "ERROR: DB failure"

    # insert dataset
    def insertDataset(self, dataset, tablename="ATLAS_PANDA.Datasets"):
        comment = " /* DBProxy.insertDataset */"
        methodName = comment.split(" ")[-2].split(".")[-1]
        tmpLog = LogWrapper(_logger, methodName + f" <dataset={dataset.name}>")
        tmpLog.debug("start")
        sql0 = f"SELECT COUNT(*) FROM {tablename} WHERE vuid=:vuid "
        sql1 = f"INSERT INTO {tablename} "
        sql1 += f"({DatasetSpec.columnNames()}) "
        sql1 += DatasetSpec.bindValuesExpression()
        sql2 = f"SELECT name FROM {tablename} WHERE vuid=:vuid "
        # time information
        dataset.creationdate = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
        dataset.modificationdate = dataset.creationdate
        try:
            # subtype
            if dataset.subType in ["", "NULL", None]:
                # define using name
                if re.search("_dis\d+$", dataset.name) is not None:
                    dataset.subType = "dis"
                elif re.search("_sub\d+$", dataset.name) is not None:
                    dataset.subType = "sub"
                else:
                    dataset.subType = "top"
            # begin transaction
            self.conn.begin()
            # check if it already exists
            varMap = {}
            varMap[":vuid"] = dataset.vuid
            self.cur.execute(sql0 + comment, varMap)
            (nDS,) = self.cur.fetchone()
            tmpLog.debug(f"nDS={nDS} with {dataset.vuid}")
            if nDS == 0:
                # insert
                tmpLog.debug(sql1 + comment + str(dataset.valuesMap()))
                self.cur.execute(sql1 + comment, dataset.valuesMap())
                # check name in DB
                varMap = {}
                varMap[":vuid"] = dataset.vuid
                self.cur.execute(sql2 + comment, varMap)
                (nameInDB,) = self.cur.fetchone()
                tmpLog.debug(f"inDB -> {nameInDB} {dataset.name == nameInDB}")
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            return True
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(_logger, methodName)
            return False

    # get and lock dataset with a query
    def getLockDatasets(self, sqlQuery, varMapGet, modTimeOffset="", getVersion=False):
        comment = " /* DBProxy.getLockDatasets */"
        _logger.debug(f"getLockDatasets({sqlQuery},{str(varMapGet)},{modTimeOffset})")
        sqlGet = (
            "SELECT /*+ INDEX_RS_ASC(tab(STATUS,TYPE,MODIFICATIONDATE)) */ vuid,name,modificationdate,version,transferStatus FROM ATLAS_PANDA.Datasets tab WHERE "
            + sqlQuery
        )
        sqlLock = "UPDATE ATLAS_PANDA.Datasets SET modificationdate=CURRENT_DATE"
        if modTimeOffset != "":
            sqlLock += f"+{modTimeOffset}"
        sqlLock += ",transferStatus=MOD(transferStatus+1,10)"
        if getVersion:
            sqlLock += ",version=:version"
        sqlLock += " WHERE vuid=:vuid AND transferStatus=:transferStatus"
        retList = []
        try:
            # begin transaction
            self.conn.begin()
            # get datasets
            self.cur.arraysize = 1000000
            self.cur.execute(sqlGet + comment, varMapGet)
            res = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # loop over all datasets
            if res is not None and len(res) != 0:
                for vuid, name, modificationdate, version, transferStatus in res:
                    # lock
                    varMapLock = {}
                    varMapLock[":vuid"] = vuid
                    varMapLock[":transferStatus"] = transferStatus
                    if getVersion:
                        try:
                            varMapLock[":version"] = str(int(version) + 1)
                        except Exception:
                            varMapLock[":version"] = str(1)
                    # begin transaction
                    self.conn.begin()
                    # update for lock
                    self.cur.execute(sqlLock + comment, varMapLock)
                    retU = self.cur.rowcount
                    # commit
                    if not self._commit():
                        raise RuntimeError("Commit error")
                    if retU > 0:
                        # append
                        if not getVersion:
                            retList.append((vuid, name, modificationdate))
                        else:
                            retList.append((vuid, name, modificationdate, version))
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # retrun
            return retList
        except Exception:
            # roll back
            self._rollback()
            # error
            type, value, traceBack = sys.exc_info()
            _logger.error(f"getLockDatasets : {type} {value}")
            return []

    # query dataset with map
    def queryDatasetWithMap(self, map):
        comment = " /* DBProxy.queryDatasetWithMap */"
        _logger.debug(f"queryDatasetWithMap({map})")
        if "name" in map:
            sql1 = """SELECT /*+ BEGIN_OUTLINE_DATA """
            sql1 += """INDEX_RS_ASC(@"SEL$1" "TAB"@"SEL$1" ("DATASETS"."NAME")) """
            sql1 += """OUTLINE_LEAF(@"SEL$1") ALL_ROWS """
            sql1 += """IGNORE_OPTIM_EMBEDDED_HINTS """
            sql1 += """END_OUTLINE_DATA */ """
            sql1 += f"{DatasetSpec.columnNames()} FROM ATLAS_PANDA.Datasets tab"
        else:
            sql1 = f"SELECT {DatasetSpec.columnNames()} FROM ATLAS_PANDA.Datasets"
        varMap = {}
        for key in map:
            if len(varMap) == 0:
                sql1 += f" WHERE {key}=:{key}"
            else:
                sql1 += f" AND {key}=:{key}"
            varMap[f":{key}"] = map[key]
        try:
            # start transaction
            self.conn.begin()
            # select
            self.cur.arraysize = 100
            _logger.debug(sql1 + comment + str(varMap))
            self.cur.execute(sql1 + comment, varMap)
            res = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # instantiate Dataset
            if res is not None and len(res) != 0:
                dataset = DatasetSpec()
                dataset.pack(res[0])
                return dataset
            _logger.error(f"queryDatasetWithMap({map}) : dataset not found")
            return None
        except Exception:
            # roll back
            self._rollback()
            type, value, traceBack = sys.exc_info()
            _logger.error(f"queryDatasetWithMap({map}) : {type} {value}")
            return None

    # update dataset
    def updateDataset(self, datasets, withLock, withCriteria, criteriaMap):
        comment = " /* DBProxy.updateDataset */"
        _logger.debug("updateDataset()")
        sql1 = f"UPDATE ATLAS_PANDA.Datasets SET {DatasetSpec.bindUpdateExpression()} "
        sql1 += "WHERE vuid=:vuid"
        if withCriteria != "":
            sql1 += f" AND {withCriteria}"
        retList = []
        try:
            # start transaction
            self.conn.begin()
            for dataset in datasets:
                _logger.debug(f"updateDataset({dataset.name},{dataset.status})")
                # time information
                dataset.modificationdate = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
                # update
                varMap = dataset.valuesMap()
                varMap[":vuid"] = dataset.vuid
                for cKey in criteriaMap:
                    varMap[cKey] = criteriaMap[cKey]
                _logger.debug(sql1 + comment + str(varMap))
                self.cur.execute(sql1 + comment, varMap)
                retU = self.cur.rowcount
                if retU != 0 and retU != 1:
                    raise RuntimeError(f"Invalid retrun {retU}")
                retList.append(retU)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            _logger.debug(f"updateDataset() ret:{retList}")
            return retList
        except Exception:
            # roll back
            self._rollback()
            type, value, traceBack = sys.exc_info()
            _logger.error(f"updateDataset() : {type} {value}")
            return []

    # trigger cleanup of internal datasets used by a task
    def trigger_cleanup_internal_datasets(self, task_id: int) -> bool:
        """
        Set deleting flag to dispatch datasets used by a task, which triggers deletion in datasetManager
        """
        comment = " /* DBProxy.trigger_cleanup_internal_datasets */"
        method_name = comment.split(" ")[-2].split(".")[-1]
        tmp_log = LogWrapper(_logger, f"{method_name} < jediTaskID={task_id} >")
        tmp_log.debug("start")
        sql1 = (
            f"UPDATE {panda_config.schemaPANDA}.Datasets SET status=:newStatus,modificationdate=CURRENT_DATE "
            "WHERE type=:type AND MoverID=:taskID AND status IN (:status_d,:status_c) "
        )
        try:
            # start transaction
            self.conn.begin()
            # update
            var_map = {
                ":type": "dispatch",
                ":newStatus": "deleting",
                ":taskID": task_id,
                ":status_d": "defined",
                ":status_c": "completed",
            }
            self.cur.execute(sql1 + comment, var_map)
            ret_u = self.cur.rowcount
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmp_log.debug(f"set flag to {ret_u} dispatch datasets")
            return True
        except Exception:
            # roll back
            self._rollback()
            self.dumpErrorMessage(_logger, method_name)
            return False

    # get serial number for dataset, insert dummy datasets to increment SN
    def getSerialNumber(self, datasetname, definedFreshFlag=None):
        comment = " /* DBProxy.getSerialNumber */"
        try:
            _logger.debug(f"getSerialNumber({datasetname},{definedFreshFlag})")
            if isinstance(datasetname, str):
                datasetname = datasetname.encode("ascii", "ignore")
                _logger.debug(f"getSerialNumber converted unicode for {datasetname}")
            # start transaction
            self.conn.begin()
            # check freshness
            if definedFreshFlag is None:
                # select
                varMap = {}
                varMap[":name"] = datasetname
                varMap[":type"] = "output"
                sql = "SELECT /*+ INDEX_RS_ASC(TAB (DATASETS.NAME)) */ COUNT(*) FROM ATLAS_PANDA.Datasets tab WHERE type=:type AND name=:name"
                self.cur.arraysize = 100
                self.cur.execute(sql + comment, varMap)
                res = self.cur.fetchone()
                # fresh dataset or not
                if res is not None and len(res) != 0 and res[0] > 0:
                    freshFlag = False
                else:
                    freshFlag = True
            else:
                # use predefined flag
                freshFlag = definedFreshFlag
            # get serial number
            if self.backend == "oracle":
                sql = "SELECT ATLAS_PANDA.SUBCOUNTER_SUBID_SEQ.nextval FROM dual"
                self.cur.arraysize = 100
                self.cur.execute(sql + comment, {})
                (sn,) = self.cur.fetchone()
            elif self.backend == "postgres":
                sql = f"SELECT {panda_config.schemaPANDA}.SUBCOUNTER_SUBID_SEQ.nextval"
                self.cur.arraysize = 100
                self.cur.execute(sql + comment, {})
                (sn,) = self.cur.fetchone()
            else:
                # panda_config.backend == 'mysql'
                # fake sequence
                sql = " INSERT INTO ATLAS_PANDA.SUBCOUNTER_SUBID_SEQ (col) VALUES (NULL) "
                self.cur.arraysize = 100
                self.cur.execute(sql + comment, {})
                sql2 = """ SELECT LAST_INSERT_ID() """
                self.cur.execute(sql2 + comment, {})
                (sn,) = self.cur.fetchone()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            _logger.debug(f"getSerialNumber : {sn} {freshFlag}")
            return (sn, freshFlag)
        except Exception:
            # roll back
            self._rollback()
            # error
            type, value, traceBack = sys.exc_info()
            _logger.error(f"getSerialNumber() : {type} {value}")
            return (-1, False)

    # count the number of files with map
    def countFilesWithMap(self, map):
        comment = " /* DBProxy.countFilesWithMap */"
        sql1 = "SELECT /*+ index(tab FILESTABLE4_DESTDBLOCK_IDX) */ COUNT(*) FROM ATLAS_PANDA.filesTable4 tab"
        varMap = {}
        for key in map:
            if len(varMap) == 0:
                sql1 += f" WHERE {key}=:{key}"
            else:
                sql1 += f" AND {key}=:{key}"
            varMap[f":{key}"] = map[key]
        nTry = 3
        for iTry in range(nTry):
            try:
                # start transaction
                self.conn.begin()
                # select
                _logger.debug(f"countFilesWithMap() : {sql1} {str(map)}")
                self.cur.arraysize = 10
                retS = self.cur.execute(sql1 + comment, varMap)
                res = self.cur.fetchone()
                _logger.debug(f"countFilesWithMap() : {retS} {str(res)}")
                # commit
                if not self._commit():
                    raise RuntimeError("Commit error")
                nFiles = 0
                if res is not None:
                    nFiles = res[0]
                return nFiles
            except Exception:
                # roll back
                self._rollback()
                if iTry + 1 < nTry:
                    _logger.debug(f"countFilesWithMap() retry : {iTry}")
                    time.sleep(random.randint(10, 20))
                    continue
                type, value, traceBack = sys.exc_info()
                _logger.error(f"countFilesWithMap({map}) : {type} {value}")
                return -1

    # update input files and return corresponding PandaIDs
    def updateInFilesReturnPandaIDs(self, dataset, status, fileLFN=""):
        comment = " /* DBProxy.updateInFilesReturnPandaIDs */"
        _logger.debug(f"updateInFilesReturnPandaIDs({dataset},{fileLFN})")
        sql0 = "SELECT /*+ index(tab FILESTABLE4_DISPDBLOCK_IDX) */ row_ID,PandaID FROM ATLAS_PANDA.filesTable4 tab WHERE status<>:status AND dispatchDBlock=:dispatchDBlock"
        sql1 = "UPDATE /*+ index(tab FILESTABLE4_DISPDBLOCK_IDX) */ ATLAS_PANDA.filesTable4 tab SET status=:status WHERE status<>:status AND dispatchDBlock=:dispatchDBlock"
        varMap = {}
        varMap[":status"] = status
        varMap[":dispatchDBlock"] = dataset
        if fileLFN != "":
            sql0 += " AND lfn=:lfn"
            sql1 += " AND lfn=:lfn"
            varMap[":lfn"] = fileLFN
        for iTry in range(self.nTry):
            try:
                # start transaction
                self.conn.begin()
                # select
                self.cur.arraysize = 10000
                retS = self.cur.execute(sql0 + comment, varMap)
                resS = self.cur.fetchall()
                # update
                retU = self.cur.execute(sql1 + comment, varMap)
                # commit
                if not self._commit():
                    raise RuntimeError("Commit error")
                # collect PandaIDs
                retList = []
                for tmpRowID, tmpPandaID in resS:
                    # append
                    if tmpPandaID not in retList:
                        retList.append(tmpPandaID)
                # return
                _logger.debug(f"updateInFilesReturnPandaIDs : {str(retList)}")
                return retList
            except Exception:
                # roll back
                self._rollback()
                # error report
                if iTry + 1 < self.nTry:
                    _logger.debug(f"updateInFilesReturnPandaIDs retry : {iTry}")
                    time.sleep(random.randint(10, 20))
                    continue
                type, value, traceBack = sys.exc_info()
                _logger.error(f"updateInFilesReturnPandaIDs : {type} {value}")
        return []

    # update output files and return corresponding PandaIDs
    def updateOutFilesReturnPandaIDs(self, dataset, fileLFN=""):
        comment = " /* DBProxy.updateOutFilesReturnPandaIDs */"
        _logger.debug(f"updateOutFilesReturnPandaIDs({dataset},{fileLFN})")
        sql0 = "SELECT /*+ index(tab FILESTABLE4_DESTDBLOCK_IDX) */ row_ID,PandaID FROM ATLAS_PANDA.filesTable4 tab WHERE destinationDBlock=:destinationDBlock AND status=:status"
        sql1 = "UPDATE /*+ index(tab FILESTABLE4_DESTDBLOCK_IDX) */ ATLAS_PANDA.filesTable4 tab SET status='ready' WHERE destinationDBlock=:destinationDBlock AND status=:status"
        varMap = {}
        varMap[":status"] = "transferring"
        varMap[":destinationDBlock"] = dataset
        if fileLFN != "":
            sql0 += " AND lfn=:lfn"
            sql1 += " AND lfn=:lfn"
            varMap[":lfn"] = fileLFN
        for iTry in range(self.nTry):
            try:
                # start transaction
                self.conn.begin()
                # select
                self.cur.arraysize = 10000
                retS = self.cur.execute(sql0 + comment, varMap)
                resS = self.cur.fetchall()
                # update
                retList = []
                retU = self.cur.execute(sql1 + comment, varMap)
                # commit
                if not self._commit():
                    raise RuntimeError("Commit error")
                # collect PandaIDs
                retList = []
                for tmpRowID, tmpPandaID in resS:
                    # append
                    if tmpPandaID not in retList:
                        retList.append(tmpPandaID)
                # return
                _logger.debug(f"updateOutFilesReturnPandaIDs : {str(retList)}")
                return retList
            except Exception:
                # roll back
                self._rollback()
                # error report
                if iTry + 1 < self.nTry:
                    _logger.debug(f"updateOutFilesReturnPandaIDs retry : {iTry}")
                    time.sleep(random.randint(10, 20))
                    continue
                type, value, traceBack = sys.exc_info()
                _logger.error(f"updateOutFilesReturnPandaIDs : {type} {value}")
        return []

    # get _dis datasets associated to _sub
    def getAssociatedDisDatasets(self, subDsName):
        comment = " /* DBProxy.getAssociatedDisDatasets */"
        _logger.debug(f"getAssociatedDisDatasets({subDsName})")
        sqlF = (
            "SELECT /*+ index(tab FILESTABLE4_DESTDBLOCK_IDX) */ distinct PandaID FROM ATLAS_PANDA.filesTable4 tab WHERE destinationDBlock=:destinationDBlock"
        )
        sqlJ = "SELECT distinct dispatchDBlock FROM ATLAS_PANDA.filesTable4 WHERE PandaID=:PandaID AND type=:type"
        try:
            # start transaction
            self.conn.begin()
            # get PandaIDs
            varMap = {}
            varMap[":destinationDBlock"] = subDsName
            self.cur.arraysize = 10000
            self.cur.execute(sqlF + comment, varMap)
            resS = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # loop over all PandaIDs
            retList = []
            for (pandaID,) in resS:
                # start transaction
                self.conn.begin()
                # get _dis name
                varMap = {}
                varMap[":type"] = "input"
                varMap[":PandaID"] = pandaID
                self.cur.arraysize = 1000
                self.cur.execute(sqlJ + comment, varMap)
                resD = self.cur.fetchall()
                # commit
                if not self._commit():
                    raise RuntimeError("Commit error")
                # append
                for (disName,) in resD:
                    if disName is not None and disName not in retList:
                        retList.append(disName)
            # return
            _logger.debug(f"getAssociatedDisDatasets : {str(retList)}")
            return retList
        except Exception:
            # roll back
            self._rollback()
            errType, errValue = sys.exc_info()[:2]
            _logger.error(f"getAssociatedDisDatasets : {subDsName} : {errType} {errValue}")
            return []

    # set GUIDs
    def setGUIDs(self, files):
        comment = " /* DBProxy.setGUIDs */"
        _logger.debug(f"setGUIDs({files})")
        sql0 = "UPDATE ATLAS_PANDA.filesTable4 SET GUID=:GUID,fsize=:fsize,checksum=:checksum,scope=:scope WHERE lfn=:lfn"
        for iTry in range(self.nTry):
            try:
                # start transaction
                self.conn.begin()
                self.cur.arraysize = 1000000
                # update
                for file in files:
                    varMap = {}
                    varMap[":GUID"] = file["guid"]
                    varMap[":lfn"] = file["lfn"]
                    if file["checksum"] in ["", "NULL"]:
                        varMap[":checksum"] = None
                    else:
                        varMap[":checksum"] = file["checksum"]
                    varMap[":fsize"] = file["fsize"]
                    if "scope" not in file or file["scope"] in ["", "NULL"]:
                        varMap[":scope"] = None
                    else:
                        varMap[":scope"] = file["scope"]
                    self.cur.execute(sql0 + comment, varMap)
                    retU = self.cur.rowcount
                    _logger.debug(f"setGUIDs : retU {retU}")
                    if retU < 0:
                        raise RuntimeError("SQL error")
                # commit
                if not self._commit():
                    raise RuntimeError("Commit error")
                return True
            except Exception:
                # roll back
                self._rollback()
                # error report
                if iTry + 1 < self.nTry:
                    _logger.debug(f"setGUIDs retry : {iTry}")
                    time.sleep(random.randint(10, 20))
                    continue
                type, value, traceBack = sys.exc_info()
                _logger.error(f"setGUIDs : {type} {value}")
        return False

    # get job statistics
    def getJobStatistics(self):
        comment = " /* DBProxy.getJobStatistics */"
        method_name = comment.split(" ")[-2].split(".")[-1]
        tmp_log = LogWrapper(_logger, method_name)
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
                    error_type, error_value, _ = sys.exc_info()
                    tmp_log.error(f"excepted: {error_type} {error_value}")
                    return {}

    # get job statistics per site and resource type (SCORE, MCORE, ...)
    def getJobStatisticsPerSiteResource(self, time_window):
        comment = " /* DBProxy.getJobStatisticsPerSiteResource */"
        method_name = comment.split(" ")[-2].split(".")[-1]
        tmp_log = LogWrapper(_logger, method_name)
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
                time_floor = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(hours=12)
            else:
                time_floor = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(minutes=int(time_window))

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
            self.dumpErrorMessage(_logger, method_name)
            return dict()

    # get the number of job for a user
    def getNumberJobsUser(self, dn, workingGroup=None):
        comment = " /* DBProxy.getNumberJobsUser */"
        _logger.debug(f"getNumberJobsUsers({dn},{workingGroup})")
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
                _logger.debug(f"getNumberJobsUsers({dn}) : {nJob}")
                return nJob
            except Exception:
                # roll back
                self._rollback()
                if iTry + 1 < nTry:
                    time.sleep(2)
                    continue
                type, value, traceBack = sys.exc_info()
                _logger.error(f"getNumberJobsUsers : {type} {value}")
                return 0

    # get job statistics for ExtIF. Source type is analysis or production
    def getJobStatisticsForExtIF(self, source_type=None):
        comment = " /* DBProxy.getJobStatisticsForExtIF */"
        method_name = comment.split(" ")[-2].split(".")[-1]
        method_name += f" < source_type={source_type} >"
        tmp_log = LogWrapper(_logger, method_name)
        tmp_log.debug("start")

        time_floor = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(hours=12)

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
            type, value, traceBack = sys.exc_info()
            tmp_log.error(f"excepted with : {type} {value}")
            return {}

    # get statistics for production jobs per processingType
    def getJobStatisticsPerProcessingType(self):
        comment = " /* DBProxy.getJobStatisticsPerProcessingType */"
        method_name = comment.split(" ")[-2].split(".")[-1]
        tmp_log = LogWrapper(_logger, method_name)
        tmp_log.debug("start")

        time_floor = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(hours=12)

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
            error_type, error_value, _ = sys.exc_info()
            _logger.error(f"excepted : {error_type} {error_value}")
            return {}

    # get special dispatcher parameters
    def get_special_dispatch_params(self):
        """
        Get the following special parameters for dispatcher.
          Authorized name lists for proxy, key-pair, and token-key retrieval
          Key pairs
          Token keys
        """
        comment = " /* DBProxy.get_special_dispatch_params */"
        method_name = comment.split(" ")[-2].split(".")[-1]
        tmp_log = LogWrapper(_logger, method_name)
        tmp_log.debug("start")
        try:
            return_map = {}
            # set autocommit on
            self.conn.begin()
            self.cur.arraysize = 100000
            # get token keys
            token_keys = {}
            sql = f"SELECT dn, credname FROM {panda_config.schemaMETA}.proxykey WHERE expires>:limit ORDER BY expires DESC "
            var_map = {":limit": datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)}
            self.cur.execute(sql + comment, var_map)
            res_list = self.cur.fetchall()
            for client_name, token_key in res_list:
                token_keys.setdefault(client_name, {"fullList": [], "latest": token_key})
                token_keys[client_name]["fullList"].append(token_key)
            return_map["tokenKeys"] = token_keys
            tmp_list = [f"""{k}:{len(token_keys[k]["fullList"])}""" for k in token_keys]
            tmp_log.debug(f"""got token keys {",".join(tmp_list)}""")
            # select to get the list of authorized users
            allow_key = []
            allow_proxy = []
            allow_token = []
            sql = "SELECT DISTINCT name, gridpref FROM ATLAS_PANDAMETA.users " "WHERE (status IS NULL OR status<>:ngStatus) AND gridpref IS NOT NULL "
            var_map = {":ngStatus": "disabled"}
            self.cur.execute(sql + comment, var_map)
            res_list = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            for compactDN, gridpref in res_list:
                # users authorized for proxy retrieval
                if PrioUtil.PERMISSION_PROXY in gridpref:
                    if compactDN not in allow_proxy:
                        allow_proxy.append(compactDN)
                # users authorized for key-pair retrieval
                if PrioUtil.PERMISSION_KEY in gridpref:
                    if compactDN not in allow_key:
                        allow_key.append(compactDN)
                # users authorized for token-key retrieval
                if PrioUtil.PERMISSION_TOKEN_KEY in gridpref:
                    if compactDN not in allow_token:
                        allow_token.append(compactDN)
            return_map["allowKeyPair"] = allow_key
            return_map["allowProxy"] = allow_proxy
            return_map["allowTokenKey"] = allow_token
            tmp_log.debug(
                f"got authed users key-pair:{len(return_map['allowKeyPair'])}, proxy:{len(return_map['allowProxy'])}, token-key:{len(return_map['allowTokenKey'])}"
            )
            # read key pairs
            keyPair = {}
            try:
                keyFileNames = glob.glob(panda_config.keyDir + "/*")
                for keyName in keyFileNames:
                    tmpF = open(keyName)
                    keyPair[os.path.basename(keyName)] = tmpF.read()
                    tmpF.close()
            except Exception as e:
                tmp_log.error(f"failed read key-pairs with {str(e)}")
            return_map["keyPair"] = keyPair
            tmp_log.debug(f"got {len(return_map['keyPair'])} key-pair files")
            tmp_log.debug("done")
            return return_map
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(_logger, method_name)
            return {}

    # peek at job
    def peekJobLog(self, pandaID, days=None):
        comment = " /* DBProxy.peekJobLog */"
        _logger.debug(f"peekJobLog : {pandaID} days={days}")
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
                _logger.debug(f"peekJobLog() : PandaID {pandaID} not found")
                return None
            except Exception:
                # roll back
                self._rollback()
                if iTry + 1 < nTry:
                    _logger.error(f"peekJobLog : {pandaID}")
                    time.sleep(random.randint(10, 20))
                    continue
                type, value, traceBack = sys.exc_info()
                _logger.error(f"peekJobLog : {type} {value}")
                # return None
                return None

    # get original consumers
    def getOriginalConsumers(self, jediTaskID, jobsetID, pandaID):
        comment = " /* DBProxy.getOriginalConsumers */"
        methodName = comment.split(" ")[-2].split(".")[-1]
        methodName += f" < jediTaskID={jediTaskID} jobsetID={jobsetID} PandaID={pandaID} >"
        tmpLog = LogWrapper(_logger, methodName)
        tmpLog.debug("start")
        try:
            # sql to get sites where consumers are active
            sqlA = "SELECT computingSite FROM ATLAS_PANDA.jobsActive4 WHERE jediTaskID=:jediTaskID AND jobsetID=:jobsetID "
            sqlA += "UNION "
            sqlA += "SELECT computingSite FROM ATLAS_PANDA.jobsDefined4 WHERE jediTaskID=:jediTaskID AND jobsetID=:jobsetID "
            # sql to get original IDs
            sqlG = f"SELECT oldPandaID FROM {panda_config.schemaJEDI}.JEDI_Job_Retry_History "
            sqlG += "WHERE jediTaskID=:jediTaskID AND newPandaID=:jobsetID AND relationType=:relationType "
            # sql to check computingSite
            sqlC1 = "SELECT computingSite FROM ATLAS_PANDA.jobsArchived4 WHERE PandaID=:PandaID "
            sqlC2 = "SELECT computingSite FROM ATLAS_PANDAARCH.jobsArchived WHERE PandaID=:PandaID AND modificationTime>(CURRENT_DATE-30) "
            # sql to get job info
            sqlJ = f"SELECT {JobSpec.columnNames()} "
            sqlJ += "FROM {0} "
            sqlJ += "WHERE PandaID=:PandaID AND modificationTime>(CURRENT_DATE-30) "
            sqlF = f"SELECT {FileSpec.columnNames()} FROM ATLAS_PANDA.filesTable4 "
            sqlF += "WHERE PandaID=:PandaID "
            sqlP = "SELECT jobParameters FROM {0} WHERE PandaID=:PandaID "
            # get sites
            aSites = set()
            varMap = dict()
            varMap[":jediTaskID"] = jediTaskID
            varMap[":jobsetID"] = jobsetID
            self.cur.execute(sqlA + comment, varMap)
            resA = self.cur.fetchall()
            for (computingSite,) in resA:
                aSites.add(computingSite)
            # get original IDs
            varMap = dict()
            varMap[":jediTaskID"] = jediTaskID
            varMap[":jobsetID"] = jobsetID
            varMap[":relationType"] = EventServiceUtils.relationTypeJS_Map
            self.cur.execute(sqlG + comment, varMap)
            resG = self.cur.fetchall()
            jobList = []
            for (pandaID,) in resG:
                # check computingSite
                varMap = dict()
                varMap[":PandaID"] = pandaID
                self.cur.execute(sqlC1 + comment, varMap)
                resC = self.cur.fetchone()
                if resC is None:
                    # try archived
                    self.cur.execute(sqlC2 + comment, varMap)
                    resC = self.cur.fetchone()
                    inArchived = True
                else:
                    inArchived = False
                # skip since it is not yet archived and thus is still active
                if resC is None:
                    continue
                (computingSite,) = resC
                # skip since there is an active consumer at the site
                if computingSite in aSites:
                    continue
                # get job
                if inArchived:
                    self.cur.execute(sqlJ.format("ATLAS_PANDAARCH.jobsArchived") + comment, varMap)
                else:
                    self.cur.execute(sqlJ.format("ATLAS_PANDA.jobsArchived4") + comment, varMap)
                resJ = self.cur.fetchone()
                if resJ is not None:
                    jobSpec = JobSpec()
                    jobSpec.pack(resJ)
                    # get files
                    self.cur.execute(sqlF + comment, varMap)
                    resFs = self.cur.fetchall()
                    if len(resFs) == 0:
                        continue
                    for resF in resFs:
                        fileSpec = FileSpec()
                        fileSpec.pack(resF)
                        jobSpec.addFile(fileSpec)
                    # get job params
                    if inArchived:
                        self.cur.execute(
                            sqlP.format("ATLAS_PANDAARCH.jobParamsTable_ARCH") + comment,
                            varMap,
                        )
                    else:
                        self.cur.execute(sqlP.format("ATLAS_PANDA.jobParamsTable") + comment, varMap)
                    for (clobJobP,) in self.cur:
                        if clobJobP is not None:
                            try:
                                jobSpec.jobParameters = clobJobP.read()
                            except AttributeError:
                                jobSpec.jobParameters = str(clobJobP)
                        break
                    # add
                    jobList.append(jobSpec)
                    aSites.add(computingSite)
            tmpLog.debug(f"got {len(jobList)} consumers")
            return jobList
        except Exception:
            # error
            self.dumpErrorMessage(_logger, methodName)
            return []

    # dump error message
    def dumpErrorMessage(self, tmpLog, methodName):
        # error
        errtype, errvalue = sys.exc_info()[:2]
        errStr = f"{methodName}: {errtype.__name__} {errvalue}"
        errStr.strip()
        errStr += " "
        errStr += traceback.format_exc()
        tmpLog.error(errStr)

    # update unmerged datasets to trigger merging
    def updateUnmergedDatasets(self, job, finalStatusDS, updateCompleted=False):
        comment = " /* JediDBProxy.updateUnmergedDatasets */"
        methodName = comment.split(" ")[-2].split(".")[-1]
        methodName += f" <PandaID={job.PandaID}>"
        # get PandaID which produced unmerged files
        umPandaIDs = []
        umCheckedIDs = []
        # sql to get file counts
        sqlGFC = "SELECT status,PandaID,outPandaID FROM ATLAS_PANDA.JEDI_Dataset_Contents "
        sqlGFC += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND PandaID IS NOT NULL "
        # sql to update nFiles in JEDI datasets
        sqlUNF = "UPDATE ATLAS_PANDA.JEDI_Datasets "
        sqlUNF += "SET nFilesOnHold=0,nFiles=:nFiles,"
        sqlUNF += "nFilesUsed=:nFilesUsed,nFilesTobeUsed=:nFilesTobeUsed "
        sqlUNF += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
        # sql to check nFiles
        sqlUCF = "SELECT nFilesTobeUsed,nFilesUsed FROM ATLAS_PANDA.JEDI_Datasets "
        sqlUCF += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
        # sql to update dataset status
        sqlUDS = "UPDATE ATLAS_PANDA.JEDI_Datasets "
        sqlUDS += "SET status=:status,modificationTime=CURRENT_DATE "
        sqlUDS += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
        # sql to update dataset status in panda
        sqlUDP = "UPDATE ATLAS_PANDA.Datasets "
        sqlUDP += "SET status=:status "
        sqlUDP += "WHERE vuid=:vuid AND NOT status IN (:statusR,:statusD) "
        try:
            _logger.debug(f"{methodName} start")
            # begin transaction
            self.conn.begin()
            # update dataset in panda
            toSkip = False
            for datasetSpec in finalStatusDS:
                varMap = {}
                varMap[":vuid"] = datasetSpec.vuid
                varMap[":status"] = "tobeclosed"
                varMap[":statusR"] = "tobeclosed"
                if not updateCompleted:
                    varMap[":statusD"] = "completed"
                else:
                    varMap[":statusD"] = "dummy"
                _logger.debug(methodName + " " + sqlUDP + comment + str(varMap))
                self.cur.execute(sqlUDP + comment, varMap)
                nRow = self.cur.rowcount
                if nRow != 1:
                    toSkip = True
                    _logger.debug(f"{methodName} failed to lock {datasetSpec.name}")
            # look for unmerged files
            if not toSkip:
                updatedDS = []
                for tmpFile in job.Files:
                    if tmpFile.isUnMergedOutput():
                        if tmpFile.datasetID in updatedDS:
                            continue
                        updatedDS.append(tmpFile.datasetID)
                        # get file counts
                        varMap = {}
                        varMap[":jediTaskID"] = tmpFile.jediTaskID
                        varMap[":datasetID"] = tmpFile.datasetID
                        self.cur.arraysize = 100000
                        _logger.debug(sqlGFC + comment + str(varMap))
                        self.cur.execute(sqlGFC + comment, varMap)
                        resListGFC = self.cur.fetchall()
                        varMap = {}
                        tmpNumFiles = 0
                        tmpNumReady = 0
                        for tmpFileStatus, tmpPandaID, tmpOutPandaID in resListGFC:
                            if tmpFileStatus in [
                                "finished",
                                "failed",
                                "cancelled",
                                "notmerged",
                                "ready",
                                "lost",
                                "broken",
                                "picked",
                                "nooutput",
                            ]:
                                pass
                            elif tmpFileStatus == "running" and tmpPandaID != tmpOutPandaID:
                                pass
                            else:
                                continue
                            tmpNumFiles += 1
                            if tmpFileStatus in ["ready"]:
                                tmpNumReady += 1
                        # update nFiles
                        varMap = {}
                        varMap[":jediTaskID"] = tmpFile.jediTaskID
                        varMap[":datasetID"] = tmpFile.datasetID
                        varMap[":nFiles"] = tmpNumFiles
                        varMap[":nFilesTobeUsed"] = tmpNumFiles
                        varMap[":nFilesUsed"] = tmpNumFiles - tmpNumReady
                        self.cur.arraysize = 10
                        _logger.debug(sqlUNF + comment + str(varMap))
                        self.cur.execute(sqlUNF + comment, varMap)
                        nRow = self.cur.rowcount
                        if nRow == 1:
                            # check nFilesTobeUsed
                            varMap = {}
                            varMap[":jediTaskID"] = tmpFile.jediTaskID
                            varMap[":datasetID"] = tmpFile.datasetID
                            self.cur.execute(sqlUCF + comment, varMap)
                            resUCF = self.cur.fetchone()
                            if resUCF is not None:
                                nFilesTobeUsed, nFilesUsed = resUCF
                                varMap = {}
                                varMap[":jediTaskID"] = tmpFile.jediTaskID
                                varMap[":datasetID"] = tmpFile.datasetID
                                if nFilesTobeUsed - nFilesUsed > 0:
                                    varMap[":status"] = "ready"
                                else:
                                    varMap[":status"] = "done"
                                # update dataset status
                                _logger.debug(methodName + " " + sqlUDS + comment + str(varMap))
                                self.cur.execute(sqlUDS + comment, varMap)
                        else:
                            _logger.debug(f"{methodName} skip jediTaskID={tmpFile.jediTaskID} datasetID={tmpFile.datasetID}")
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            _logger.debug(f"{methodName} done")
            return True
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(_logger, methodName)
            return False

    # change task priority
    def changeTaskPriorityPanda(self, jediTaskID, newPriority):
        comment = " /* DBProxy.changeTaskPriorityPanda */"
        methodName = comment.split(" ")[-2].split(".")[-1]
        methodName += f" <jediTaskID={jediTaskID}>"
        _logger.debug(f"{methodName} newPrio={newPriority}")
        try:
            # sql to update JEDI task table
            sqlT = f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks SET currentPriority=:newPriority WHERE jediTaskID=:jediTaskID "
            # sql to update DEFT task table
            schemaDEFT = panda_config.schemaDEFT
            sqlD = f"UPDATE {schemaDEFT}.T_TASK SET current_priority=:newPriority,timestamp=CURRENT_DATE WHERE taskid=:jediTaskID "
            # update job priorities
            sqlJ = "UPDATE ATLAS_PANDA.{0} SET currentPriority=:newPriority WHERE jediTaskID=:jediTaskID "
            # start transaction
            self.conn.begin()
            # select
            self.cur.arraysize = 10
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            varMap[":newPriority"] = newPriority
            # update JEDI
            self.cur.execute(sqlT + comment, varMap)
            nRow = self.cur.rowcount
            if nRow == 1:
                # update jobs
                for tableName in ["jobsActive4", "jobsDefined4"]:
                    self.cur.execute(sqlJ.format(tableName) + comment, varMap)
            # update DEFT
            self.cur.execute(sqlD + comment, varMap)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            _logger.debug(f"{methodName} done with {nRow}")
            return nRow
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(_logger, methodName)
            return None

    # throttle user jobs
    def throttleUserJobs(self, prodUserName, workingGroup, get_dict):
        comment = " /* DBProxy.throttleUserJobs */"
        methodName = comment.split(" ")[-2].split(".")[-1]
        methodName += f" <user={prodUserName} group={workingGroup}>"
        tmpLog = LogWrapper(_logger, methodName)
        tmpLog.debug("start")
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
                tmpLog.debug(f"reset jediTaskID={jediTaskID}")
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
                tmpLog.debug(f"reset {nTmp} jobs")
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
                tmpLog.debug(f"done with {nRows}")
                return nRows
            tmpLog.debug(f"done with {nRow}")
            return nRow
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(_logger, methodName)
            return None

    # unthrottle user jobs
    def unThrottleUserJobs(self, prodUserName, workingGroup, get_dict):
        comment = " /* DBProxy.unThrottleUserJobs */"
        methodName = comment.split(" ")[-2].split(".")[-1]
        methodName += f" <user={prodUserName} group={workingGroup}>"
        tmpLog = LogWrapper(_logger, methodName)
        tmpLog.debug("start")
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
                tmpLog.debug(f"reset jediTaskID={jediTaskID}")
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
                tmpLog.debug(f"reset {nTmp} jobs")
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
                tmpLog.debug(f"done with {nRows}")
                return nRows
            tmpLog.debug(f"done with {nRow}")
            return nRow
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(_logger, methodName)
            return None

    # get throttled users
    def getThrottledUsers(self):
        comment = " /* DBProxy.getThrottledUsers */"
        methodName = comment.split(" ")[-2].split(".")[-1]
        tmpLog = LogWrapper(_logger, methodName)
        tmpLog.debug("start")
        retVal = set()
        try:
            # sql to get users
            sqlT = "SELECT distinct prodUserName,workingGroup FROM ATLAS_PANDA.jobsActive4 "
            sqlT += "WHERE prodSourceLabel=:prodSourceLabel AND jobStatus=:jobStatus AND relocationFlag=:relocationFlag "
            # start transaction
            self.conn.begin()
            # select
            self.cur.arraysize = 10
            varMap = {}
            varMap[":prodSourceLabel"] = "user"
            varMap[":relocationFlag"] = 3
            varMap[":jobStatus"] = "throttled"
            # get datasets
            self.cur.execute(sqlT + comment, varMap)
            resPs = self.cur.fetchall()
            for prodUserName, workingGroup in resPs:
                retVal.add((prodUserName, workingGroup))
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmpLog.debug(f"done with {str(retVal)}")
            return retVal
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(_logger, methodName)
            return []

    # get the list of jobdefIDs for failed jobs in a task
    def getJobdefIDsForFailedJob(self, jediTaskID):
        comment = " /* DBProxy.getJobdefIDsForFailedJob */"
        methodName = comment.split(" ")[-2].split(".")[-1]
        methodName += f" <jediTaskID={jediTaskID}>"
        _logger.debug(f"{methodName} : start")
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
            _logger.debug(f"{methodName} : {str(retList)}")
            return retList
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(_logger, methodName)
            return []

    # increase attempt number for unprocessed files
    def increaseAttemptNrPanda(self, jediTaskID, increasedNr):
        comment = " /* DBProxy.increaseAttemptNrPanda */"
        methodName = comment.split(" ")[-2].split(".")[-1]
        methodName += f" <jediTaskID={jediTaskID}>"
        tmpLog = LogWrapper(_logger, methodName, monToken=f"<jediTaskID={jediTaskID}>")
        tmpLog.debug(f"increasedNr={increasedNr}")
        try:
            # sql to check task status
            sqlT = f"SELECT status,oldStatus FROM {panda_config.schemaJEDI}.JEDI_Tasks "
            sqlT += "WHERE jediTaskID=:jediTaskID FOR UPDATE "
            # start transaction
            self.conn.begin()
            # select
            self.cur.arraysize = 10
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            # get task status
            self.cur.execute(sqlT + comment, varMap)
            resT = self.cur.fetchone()
            if resT is None:
                tmpMsg = f"jediTaskID={jediTaskID} not found"
                tmpLog.debug(tmpMsg)
                retVal = 1, tmpMsg
            else:
                taskStatus, oldStatus = resT
                # check task status
                okStatusList = ["running", "scouting", "ready"]
                if taskStatus not in okStatusList and oldStatus not in okStatusList:
                    tmpMsg = f"command rejected since status={taskStatus} or oldStatus={oldStatus} not in {str(okStatusList)}"
                    tmpLog.debug(tmpMsg)
                    retVal = 2, tmpMsg
                else:
                    # sql to get datasetIDs for master
                    sqlM = f"SELECT datasetID FROM {panda_config.schemaJEDI}.JEDI_Datasets "
                    sqlM += "WHERE jediTaskID=:jediTaskID AND type IN (:type1,:type2) "
                    # sql to increase attempt numbers
                    sqlAB = f"UPDATE {panda_config.schemaJEDI}.JEDI_Dataset_Contents "
                    sqlAB += "SET maxAttempt=CASE WHEN maxAttempt > attemptNr THEN maxAttempt+:increasedNr ELSE attemptNr+:increasedNr END "
                    sqlAB += ",proc_status=CASE WHEN maxAttempt > attemptNr AND maxFailure > failedAttempt THEN proc_status ELSE :proc_status END "
                    sqlAB += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND status=:status AND keepTrack=:keepTrack "
                    # sql to increase attempt numbers and failure counts
                    sqlAF = f"UPDATE {panda_config.schemaJEDI}.JEDI_Dataset_Contents "
                    sqlAF += "SET maxAttempt=CASE WHEN maxAttempt > attemptNr THEN maxAttempt+:increasedNr ELSE attemptNr+:increasedNr END "
                    sqlAF += ",maxFailure=maxFailure+:increasedNr "
                    sqlAF += ",proc_status=CASE WHEN maxAttempt > attemptNr AND maxFailure > failedAttempt THEN proc_status ELSE :proc_status END "
                    sqlAF += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND status=:status AND keepTrack=:keepTrack "
                    # sql to update datasets
                    sqlD = f"UPDATE {panda_config.schemaJEDI}.JEDI_Datasets "
                    sqlD += "SET nFilesUsed=nFilesUsed-:nFilesReset,nFilesFailed=nFilesFailed-:nFilesReset "
                    sqlD += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
                    # get datasetIDs for master
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    varMap[":type1"] = "input"
                    varMap[":type2"] = "pseudo_input"
                    self.cur.execute(sqlM + comment, varMap)
                    resM = self.cur.fetchall()
                    total_nFilesIncreased = 0
                    total_nFilesReset = 0
                    for (datasetID,) in resM:
                        # increase attempt numbers
                        varMap = {}
                        varMap[":jediTaskID"] = jediTaskID
                        varMap[":datasetID"] = datasetID
                        varMap[":status"] = "ready"
                        varMap[":proc_status"] = "ready"
                        varMap[":keepTrack"] = 1
                        varMap[":increasedNr"] = increasedNr
                        nFilesIncreased = 0
                        nFilesReset = 0
                        # still active and maxFailure is undefined
                        sqlA = sqlAB + "AND maxAttempt>attemptNr AND maxFailure IS NULL "
                        self.cur.execute(sqlA + comment, varMap)
                        nRow = self.cur.rowcount
                        nFilesIncreased += nRow
                        # still active and maxFailure is defined
                        sqlA = sqlAF + "AND maxAttempt>attemptNr AND (maxFailure IS NOT NULL AND maxFailure>failedAttempt) "
                        self.cur.execute(sqlA + comment, varMap)
                        nRow = self.cur.rowcount
                        nFilesIncreased += nRow
                        # already done and maxFailure is undefined
                        sqlA = sqlAB + "AND maxAttempt<=attemptNr AND maxFailure IS NULL "
                        self.cur.execute(sqlA + comment, varMap)
                        nRow = self.cur.rowcount
                        nFilesReset += nRow
                        nFilesIncreased += nRow
                        # already done and maxFailure is defined
                        sqlA = sqlAF + "AND (maxAttempt<=attemptNr OR (maxFailure IS NOT NULL AND maxFailure=failedAttempt)) "
                        self.cur.execute(sqlA + comment, varMap)
                        nRow = self.cur.rowcount
                        nFilesReset += nRow
                        nFilesIncreased += nRow
                        # update dataset
                        if nFilesReset > 0:
                            varMap = {}
                            varMap[":jediTaskID"] = jediTaskID
                            varMap[":datasetID"] = datasetID
                            varMap[":nFilesReset"] = nFilesReset
                            tmpLog.debug(sqlD + comment + str(varMap))
                            self.cur.execute(sqlD + comment, varMap)
                        total_nFilesIncreased += nFilesIncreased
                        total_nFilesReset += nFilesReset
                    tmpMsg = f"increased attemptNr for {total_nFilesIncreased} inputs ({total_nFilesReset} reactivated)"
                    tmpLog.debug(tmpMsg)
                    tmpLog.sendMsg(tmpMsg, "jedi", "pandasrv")
                    retVal = 0, tmpMsg
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmpLog.debug("done")
            return retVal
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog, methodName)
            return None, "DB error"

    # get jediTaskID from taskName
    def getTaskIDwithTaskNameJEDI(self, userName, taskName):
        comment = " /* DBProxy.getTaskIDwithTaskNameJEDI */"
        methodName = comment.split(" ")[-2].split(".")[-1]
        methodName += f" <userName={userName} taskName={taskName}>"
        _logger.debug(f"{methodName} : start")
        try:
            # begin transaction
            self.conn.begin()
            # sql to get jediTaskID
            sqlGF = f"SELECT MAX(jediTaskID) FROM {panda_config.schemaJEDI}.JEDI_Tasks "
            sqlGF += "WHERE userName=:userName AND taskName=:taskName "
            varMap = {}
            varMap[":userName"] = userName
            varMap[":taskName"] = taskName
            self.cur.execute(sqlGF + comment, varMap)
            resFJ = self.cur.fetchone()
            if resFJ is not None:
                (jediTaskID,) = resFJ
            else:
                jediTaskID = None
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            _logger.debug(f"{methodName} : jediTaskID={jediTaskID}")
            return jediTaskID
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(_logger, methodName)
            return None

    # update error dialog for a jediTaskID
    def updateTaskErrorDialogJEDI(self, jediTaskID, msg):
        comment = " /* DBProxy.updateTaskErrorDialogJEDI */"
        methodName = comment.split(" ")[-2].split(".")[-1]
        methodName += f" <jediTaskID={jediTaskID}>"
        _logger.debug(f"{methodName} : start")
        try:
            # begin transaction
            self.conn.begin()
            # get existing dialog
            sqlGF = f"SELECT errorDialog FROM {panda_config.schemaJEDI}.JEDI_Tasks "
            sqlGF += "WHERE jediTaskID=:jediTaskID "
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            self.cur.execute(sqlGF + comment, varMap)
            resFJ = self.cur.fetchone()
            if resFJ is not None:
                # update existing dialog
                (errorDialog,) = resFJ
                errorDialog = msg
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                varMap[":errorDialog"] = errorDialog
                sqlUE = f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks SET errorDialog=:errorDialog,modificationTime=CURRENT_DATE "
                sqlUE += "WHERE jediTaskID=:jediTaskID "
                self.cur.execute(sqlUE + comment, varMap)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            _logger.debug(f"{methodName} : done")
            return True
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(_logger, methodName)
            return False

    # update modificationtime for a jediTaskID to trigger subsequent process
    def updateTaskModTimeJEDI(self, jediTaskID, newStatus):
        comment = " /* DBProxy.updateTaskErrorDialogJEDI */"
        methodName = comment.split(" ")[-2].split(".")[-1]
        methodName += f" <jediTaskID={jediTaskID}>"
        _logger.debug(f"{methodName} : start")
        try:
            # begin transaction
            self.conn.begin()
            # update mod time
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            if newStatus is not None:
                varMap[":newStatus"] = newStatus
            sqlUE = f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks SET "
            sqlUE += "modificationTime=CURRENT_DATE-1,"
            if newStatus is not None:
                sqlUE += "status=:newStatus,oldStatus=NULL,"
            sqlUE = sqlUE[:-1]
            sqlUE += " WHERE jediTaskID=:jediTaskID "
            self.cur.execute(sqlUE + comment, varMap)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            _logger.debug(f"{methodName} : done")
            return True
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(_logger, methodName)
            return False

    # reset files in JEDI
    def resetFileStatusInJEDI(self, dn, prodManager, datasetName, lostFiles, recoverParent, simul):
        comment = " /* DBProxy.resetFileStatusInJEDI */"
        methodName = comment.split(" ")[-2].split(".")[-1]
        methodName += f" <datasetName={datasetName}>"
        tmpLog = LogWrapper(_logger, methodName)
        tmpLog.debug("start")
        try:
            # list of lost input files
            lostInputFiles = {}
            # get compact DN
            compactDN = CoreUtils.clean_user_id(dn)
            if compactDN in ["", "NULL", None]:
                compactDN = dn
            tmpLog.debug(f"userName={compactDN}")
            toSkip = False
            # begin transaction
            self.conn.begin()
            # get jediTaskID
            varMap = {}
            varMap[":type1"] = "log"
            varMap[":type2"] = "output"
            varMap[":name1"] = datasetName
            varMap[":name2"] = datasetName.split(":")[-1]
            sqlGI = f"SELECT jediTaskID,datasetID FROM {panda_config.schemaJEDI}.JEDI_Datasets "
            sqlGI += "WHERE type IN (:type1,:type2) AND datasetName IN (:name1,:name2) "
            self.cur.execute(sqlGI + comment, varMap)
            resGI = self.cur.fetchall()
            # use the largest datasetID since broken tasks might have been retried
            jediTaskID = None
            datasetID = None
            for tmpJediTaskID, tmpDatasetID in resGI:
                if jediTaskID is None or jediTaskID < tmpJediTaskID:
                    jediTaskID = tmpJediTaskID
                    datasetID = tmpDatasetID
                elif datasetID < tmpDatasetID:
                    datasetID = tmpDatasetID
            if jediTaskID is None:
                tmpLog.debug("jediTaskID not found")
                toSkip = True
            if not toSkip:
                # get task status and owner
                tmpLog.debug(f"jediTaskID={jediTaskID} datasetID={datasetID}")
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                sqlOW = f"SELECT status,userName,useJumbo FROM {panda_config.schemaJEDI}.JEDI_Tasks "
                sqlOW += "WHERE jediTaskID=:jediTaskID "
                self.cur.execute(sqlOW + comment, varMap)
                resOW = self.cur.fetchone()
                taskStatus, ownerName, useJumbo = resOW
                # check ownership
                if not prodManager and ownerName != compactDN:
                    tmpLog.debug(f"not the owner = {ownerName}")
                    toSkip = True
            if not toSkip:
                # get affected PandaIDs
                sqlLP = f"SELECT pandaID FROM {panda_config.schemaJEDI}.JEDI_Dataset_Contents "
                sqlLP += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND lfn=:lfn "
                # sql to update file status
                sqlUFO = f"UPDATE {panda_config.schemaJEDI}.JEDI_Dataset_Contents "
                sqlUFO += "SET status=:newStatus "
                sqlUFO += "WHERE jediTaskID=:jediTaskID AND type=:type AND status=:oldStatus AND PandaID=:PandaID "
                # sql to cancel events
                sqlCE = "UPDATE /*+ INDEX_RS_ASC(tab JEDI_EVENTS_FILEID_IDX) NO_INDEX_FFS(tab JEDI_EVENTS_PK) NO_INDEX_SS(tab JEDI_EVENTS_PK) */ "
                sqlCE += f"{panda_config.schemaJEDI}.JEDI_Events tab "
                sqlCE += "SET status=:status "
                sqlCE += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
                sqlCE += "AND status IN (:esFinished,:esDone,:esMerged) "
                # get affected PandaIDs
                lostPandaIDs = set([])
                nDiff = 0
                for lostFile in lostFiles:
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    varMap[":datasetID"] = datasetID
                    varMap[":lfn"] = lostFile
                    self.cur.execute(sqlLP + comment, varMap)
                    resLP = self.cur.fetchone()
                    if resLP is not None:
                        (pandaID,) = resLP
                        lostPandaIDs.add(pandaID)
                        # update the file and co-produced files to lost
                        varMap = {}
                        varMap[":jediTaskID"] = jediTaskID
                        varMap[":PandaID"] = pandaID
                        varMap[":type"] = "output"
                        varMap[":newStatus"] = "lost"
                        varMap[":oldStatus"] = "finished"
                        if not simul:
                            self.cur.execute(sqlUFO + comment, varMap)
                            nRow = self.cur.rowcount
                            if nRow > 0:
                                nDiff += 1
                        else:
                            tmpLog.debug(sqlUFO + comment + str(varMap))
                            nDiff += 1
                # update output dataset statistics
                sqlUDO = f"UPDATE {panda_config.schemaJEDI}.JEDI_Datasets "
                sqlUDO += "SET nFilesFinished=nFilesFinished-:nDiff "
                sqlUDO += "WHERE jediTaskID=:jediTaskID AND type=:type "
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                varMap[":type"] = "output"
                varMap[":nDiff"] = nDiff
                tmpLog.debug(sqlUDO + comment + str(varMap))
                if not simul:
                    self.cur.execute(sqlUDO + comment, varMap)
                # get nEvents
                sqlGNE = "SELECT SUM(c.nEvents),c.datasetID "
                sqlGNE += "FROM {0}.JEDI_Datasets d,{0}.JEDI_Dataset_Contents c ".format(panda_config.schemaJEDI)
                sqlGNE += "WHERE c.jediTaskID=d.jediTaskID AND c.datasetID=d.datasetID "
                sqlGNE += "AND d.jediTaskID=:jediTaskID AND d.type=:type AND c.status=:status "
                sqlGNE += "GROUP BY c.datasetID "
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                varMap[":type"] = "output"
                varMap[":status"] = "finished"
                self.cur.execute(sqlGNE + comment, varMap)
                resGNE = self.cur.fetchall()
                # update nEvents
                sqlUNE = f"UPDATE {panda_config.schemaJEDI}.JEDI_Datasets "
                sqlUNE += "SET nEvents=:nEvents "
                sqlUNE += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
                for tmpCount, tmpDatasetID in resGNE:
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    varMap[":datasetID"] = tmpDatasetID
                    varMap[":nEvents"] = tmpCount
                    if not simul:
                        self.cur.execute(sqlUNE + comment, varMap)
                        tmpLog.debug(sqlUNE + comment + str(varMap))
                # get input datasets
                sqlID = f"SELECT datasetID,datasetName,masterID FROM {panda_config.schemaJEDI}.JEDI_Datasets "
                sqlID += "WHERE jediTaskID=:jediTaskID AND type=:type "
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                varMap[":type"] = "input"
                self.cur.execute(sqlID + comment, varMap)
                resID = self.cur.fetchall()
                inputDatasets = {}
                masterID = None
                for tmpDatasetID, tmpDatasetName, tmpMasterID in resID:
                    inputDatasets[tmpDatasetID] = tmpDatasetName
                    if tmpMasterID is None:
                        masterID = tmpDatasetID
                # sql to get affected inputs
                if useJumbo is None:
                    sqlAI = f"SELECT fileID,datasetID,lfn,outPandaID FROM {panda_config.schemaJEDI}.JEDI_Dataset_Contents "
                    sqlAI += "WHERE jediTaskID=:jediTaskID AND type IN (:type1,:type2,:type3) AND PandaID=:PandaID "
                else:
                    sqlAI = f"SELECT fileID,datasetID,lfn,NULL FROM {panda_config.schemaPANDA}.filesTable4 "
                    sqlAI += "WHERE PandaID=:PandaID AND type IN (:type1,:type2) "
                    sqlAI += "UNION "
                    sqlAI = f"SELECT fileID,datasetID,lfn,NULL FROM {panda_config.schemaPANDA}.filesTable4 "
                    sqlAI += "WHERE PandaID=:PandaID AND type IN (:type1,:type2) AND modificationTime>CURRENT_TIMESTAMP-365 "
                # sql to update input file status
                sqlUFI = f"UPDATE {panda_config.schemaJEDI}.JEDI_Dataset_Contents "
                sqlUFI += "SET status=:newStatus,attemptNr=attemptNr+1 "
                sqlUFI += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID AND status=:oldStatus "
                # get affected inputs
                datasetCountMap = {}
                for lostPandaID in lostPandaIDs:
                    varMap = {}
                    if useJumbo is None:
                        varMap[":jediTaskID"] = jediTaskID
                        varMap[":PandaID"] = lostPandaID
                        varMap[":type1"] = "input"
                        varMap[":type2"] = "pseudo_input"
                        varMap[":type3"] = "output"
                    else:
                        varMap[":PandaID"] = lostPandaID
                        varMap[":type1"] = "input"
                        varMap[":type2"] = "pseudo_input"
                    self.cur.execute(sqlAI + comment, varMap)
                    resAI = self.cur.fetchall()
                    newResAI = []
                    for tmpItem in resAI:
                        tmpFileID, tmpDatasetID, tmpLFN, tmpOutPandaID = tmpItem
                        # skip output file
                        if lostPandaID == tmpOutPandaID:
                            continue
                        # input for merged files
                        if tmpOutPandaID is not None:
                            varMap = {}
                            varMap[":jediTaskID"] = jediTaskID
                            varMap[":PandaID"] = tmpOutPandaID
                            varMap[":type1"] = "input"
                            varMap[":type2"] = "pseudo_input"
                            varMap[":type3"] = "dummy"
                            self.cur.execute(sqlAI + comment, varMap)
                            resAI2 = self.cur.fetchall()
                            for tmpItem in resAI2:
                                newResAI.append(tmpItem)
                        else:
                            newResAI.append(tmpItem)
                    for tmpFileID, tmpDatasetID, tmpLFN, tmpOutPandaID in newResAI:
                        # collect if dataset was already deleted
                        is_lost = False
                        if recoverParent and tmpDatasetID == masterID:
                            lostInputFiles.setdefault(inputDatasets[tmpDatasetID], [])
                            lostInputFiles[inputDatasets[tmpDatasetID]].append(tmpLFN)
                            is_lost = True
                        # reset file status
                        if tmpDatasetID not in datasetCountMap:
                            datasetCountMap[tmpDatasetID] = 0
                        varMap = {}
                        varMap[":jediTaskID"] = jediTaskID
                        varMap[":datasetID"] = tmpDatasetID
                        varMap[":fileID"] = tmpFileID
                        if is_lost:
                            varMap[":newStatus"] = "lost"
                        else:
                            varMap[":newStatus"] = "ready"
                        varMap[":oldStatus"] = "finished"
                        if not simul:
                            self.cur.execute(sqlUFI + comment, varMap)
                            nRow = self.cur.rowcount
                        else:
                            tmpLog.debug(sqlUFI + comment + str(varMap))
                            nRow = 1
                        if nRow > 0:
                            datasetCountMap[tmpDatasetID] += 1
                            if useJumbo is not None:
                                # cancel events
                                varMap = {}
                                varMap[":jediTaskID"] = jediTaskID
                                varMap[":datasetID"] = tmpDatasetID
                                varMap[":fileID"] = tmpFileID
                                varMap[":status"] = EventServiceUtils.ST_cancelled
                                varMap[":esFinished"] = EventServiceUtils.ST_finished
                                varMap[":esDone"] = EventServiceUtils.ST_done
                                varMap[":esMerged"] = EventServiceUtils.ST_merged
                                if not simul:
                                    self.cur.execute(sqlCE + comment, varMap)
                                else:
                                    tmpLog.debug(sqlCE + comment + str(varMap))
                # update dataset statistics
                sqlUDI = f"UPDATE {panda_config.schemaJEDI}.JEDI_Datasets "
                sqlUDI += "SET nFilesUsed=nFilesUsed-:nDiff,nFilesFinished=nFilesFinished-:nDiff,"
                sqlUDI += "nEventsUsed=(SELECT SUM(CASE WHEN startEvent IS NULL THEN nEvents ELSE endEvent-startEvent+1 END) "
                sqlUDI += f"FROM {panda_config.schemaJEDI}.JEDI_Dataset_Contents "
                sqlUDI += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND status=:status) "
                sqlUDI += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
                for tmpDatasetID in datasetCountMap:
                    nDiff = datasetCountMap[tmpDatasetID]
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    varMap[":datasetID"] = tmpDatasetID
                    varMap[":nDiff"] = nDiff
                    varMap[":status"] = "finished"
                    tmpLog.debug(sqlUDI + comment + str(varMap))
                    if not simul:
                        self.cur.execute(sqlUDI + comment, varMap)
                # update task status
                if taskStatus == "done":
                    sqlUT = f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks SET status=:newStatus WHERE jediTaskID=:jediTaskID "
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    varMap[":newStatus"] = "finished"
                    if not simul:
                        self.cur.execute(sqlUT + comment, varMap)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmpLog.debug("done")
            return True, jediTaskID, lostInputFiles
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(_logger, methodName)
            return False, None, None

    # copy file records
    def copy_file_records(self, new_lfns, file_spec):
        comment = " /* DBProxy.copy_file_records */"
        methodName = comment.split(" ")[-2].split(".")[-1]
        methodName += f" <PandaID={file_spec.PandaID} oldLFN={file_spec.lfn}>"
        tmpLog = LogWrapper(_logger, methodName)
        tmpLog.debug(f"start with {len(new_lfns)} files")
        try:
            # begin transaction
            self.conn.begin()
            for idx_lfn, new_lfn in enumerate(new_lfns):
                # reset rowID
                tmpFileSpec = copy.copy(file_spec)
                tmpFileSpec.lfn = new_lfn
                if idx_lfn > 0:
                    tmpFileSpec.row_ID = None
                # insert file in JEDI
                if idx_lfn > 0 and tmpFileSpec.jediTaskID not in [None, "NULL"] and tmpFileSpec.fileID not in ["", "NULL", None]:
                    # get fileID
                    sqlFileID = "SELECT ATLAS_PANDA.JEDI_DATASET_CONT_FILEID_SEQ.nextval FROM dual "
                    self.cur.execute(sqlFileID + comment)
                    (newFileID,) = self.cur.fetchone()
                    # read file in JEDI
                    varMap = {}
                    varMap[":jediTaskID"] = tmpFileSpec.jediTaskID
                    varMap[":datasetID"] = tmpFileSpec.datasetID
                    varMap[":fileID"] = tmpFileSpec.fileID
                    sqlGI = f"SELECT * FROM {panda_config.schemaJEDI}.JEDI_Dataset_Contents "
                    sqlGI += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
                    self.cur.execute(sqlGI + comment, varMap)
                    resGI = self.cur.fetchone()
                    tmpFileSpec.fileID = newFileID
                    if resGI is not None:
                        # make sql and map
                        sqlJI = f"INSERT INTO {panda_config.schemaJEDI}.JEDI_Dataset_Contents "
                        sqlJI += "VALUES ("
                        varMap = {}
                        for columDesc, columVal in zip(self.cur.description, resGI):
                            columName = columDesc[0]
                            # overwrite fileID
                            if columName.upper() == "FILEID":
                                columVal = tmpFileSpec.fileID
                            keyName = f":{columName}"
                            varMap[keyName] = columVal
                            sqlJI += f"{keyName},"
                        sqlJI = sqlJI[:-1]
                        sqlJI += ") "
                        # insert file in JEDI
                        self.cur.execute(sqlJI + comment, varMap)
                if idx_lfn > 0:
                    # insert file in Panda
                    sqlFile = f"INSERT INTO ATLAS_PANDA.filesTable4 ({FileSpec.columnNames()}) "
                    sqlFile += FileSpec.bindValuesExpression(useSeq=True)
                    varMap = tmpFileSpec.valuesMap(useSeq=True)
                    self.cur.execute(sqlFile + comment, varMap)
                else:
                    # update LFN
                    sqlFSF = "UPDATE ATLAS_PANDA.filesTable4 SET lfn=:lfn "
                    sqlFSF += "WHERE row_ID=:row_ID "
                    varMap = {}
                    varMap[":lfn"] = tmpFileSpec.lfn
                    varMap[":row_ID"] = tmpFileSpec.row_ID
                    self.cur.execute(sqlFSF + comment, varMap)
                # update LFN in JEDI
                if tmpFileSpec.fileID not in ["", "NULL", None]:
                    sqlJF = f"UPDATE {panda_config.schemaJEDI}.JEDI_Dataset_Contents "
                    sqlJF += "SET lfn=:lfn "
                    sqlJF += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
                    varMap = {}
                    varMap[":lfn"] = tmpFileSpec.lfn
                    varMap[":jediTaskID"] = tmpFileSpec.jediTaskID
                    varMap[":datasetID"] = tmpFileSpec.datasetID
                    varMap[":fileID"] = tmpFileSpec.fileID
                    self.cur.execute(sqlJF + comment, varMap)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmpLog.debug("done")
            return True
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(_logger, methodName)
            return False

    # get error definitions from DB (values cached for 1 hour)
    @memoize
    def getRetrialRules(self):
        # Logging
        comment = " /* DBProxy.getRetrialRules */"
        method_name = comment.split(" ")[-2].split(".")[-1]

        tmp_logger = LogWrapper(_logger, method_name)
        tmp_logger.debug("start")

        # SQL to extract the error definitions
        sql = """
        SELECT re.retryerror_id, re.errorsource, re.errorcode, re.errorDiag, re.parameters, re.architecture, re.release, re.workqueue_id, ra.retry_action, re.active, ra.active
        FROM ATLAS_PANDA.RETRYERRORS re, ATLAS_PANDA.RETRYACTIONS ra
        WHERE re.retryaction=ra.retryaction_id
        AND (CURRENT_TIMESTAMP < re.expiration_date or re.expiration_date IS NULL)
        """
        self.cur.execute(sql + comment, {})
        definitions = self.cur.fetchall()  # example of output: [('pilotErrorCode', 1, None, None, None, None, 'no_retry', 'Y', 'Y'),...]

        # commit
        if not self._commit():
            raise RuntimeError("Commit error")

        # tmp_logger.debug("definitions %s"%(definitions))

        retrial_rules = {}
        for definition in definitions:
            (
                retryerror_id,
                error_source,
                error_code,
                error_diag,
                parameters,
                architecture,
                release,
                wqid,
                action,
                e_active,
                a_active,
            ) = definition

            # Convert the parameter string into a dictionary
            try:
                # 1. Convert a string like "key1=value1&key2=value2" into [[key1, value1],[key2,value2]]
                params_list = map(
                    lambda key_value_pair: key_value_pair.split("="),
                    parameters.split("&"),
                )
                # 2. Convert a list [[key1, value1],[key2,value2]] into {key1: value1, key2: value2}
                params_dict = dict((key, value) for (key, value) in params_list)
            except AttributeError:
                params_dict = {}
            except ValueError:
                params_dict = {}

            # Calculate if action and error combination should be active
            if e_active == "Y" and a_active == "Y":
                active = True  # Apply the action for this error
            else:
                active = False  # Do not apply the action for this error, only log

            retrial_rules.setdefault(error_source, {})
            retrial_rules[error_source].setdefault(error_code, [])
            retrial_rules[error_source][error_code].append(
                {
                    "error_id": retryerror_id,
                    "error_diag": error_diag,
                    "action": action,
                    "params": params_dict,
                    "architecture": architecture,
                    "release": release,
                    "wqid": wqid,
                    "active": active,
                }
            )
        # tmp_logger.debug("Loaded retrial rules from DB: %s" %retrial_rules)
        return retrial_rules

    def setMaxAttempt(self, jobID, taskID, files, maxAttempt):
        # Logging
        comment = " /* DBProxy.setMaxAttempt */"
        methodName = comment.split(" ")[-2].split(".")[-1]
        tmpLog = LogWrapper(_logger, methodName)
        tmpLog.debug("start")

        # Update the file entries to avoid JEDI generating new jobs
        input_types = ("input", "pseudo_input", "pp_input", "trn_log", "trn_output")
        input_files = list(
            filter(
                lambda pandafile: pandafile.type in input_types and re.search("DBRelease", pandafile.lfn) is None,
                files,
            )
        )
        input_fileIDs = [input_file.fileID for input_file in input_files]
        input_datasetIDs = [input_file.datasetID for input_file in input_files]

        if input_fileIDs:
            try:
                # Start transaction
                self.conn.begin()

                varMap = {}
                varMap[":taskID"] = taskID
                varMap[":pandaID"] = jobID

                # Bind the files
                f = 0
                for fileID in input_fileIDs:
                    varMap[f":file{f}"] = fileID
                    f += 1
                file_bindings = ",".join(f":file{i}" for i in range(len(input_fileIDs)))

                # Bind the datasets
                d = 0
                for datasetID in input_datasetIDs:
                    varMap[f":dataset{d}"] = datasetID
                    d += 1
                dataset_bindings = ",".join(f":dataset{i}" for i in range(len(input_fileIDs)))

                # Get the minimum maxAttempt value of the files
                sql_select = f"""
                select min(maxattempt) from ATLAS_PANDA.JEDI_Dataset_Contents
                WHERE JEDITaskID = :taskID
                AND datasetID IN ({dataset_bindings})
                AND fileID IN ({file_bindings})
                AND pandaID = :pandaID
                """
                self.cur.execute(sql_select + comment, varMap)
                try:
                    maxAttempt_select = self.cur.fetchone()[0]
                except (TypeError, IndexError):
                    maxAttempt_select = None

                # Don't update the maxAttempt if the new value is higher than the old value
                if maxAttempt_select and maxAttempt_select > maxAttempt:
                    varMap[":maxAttempt"] = min(maxAttempt, maxAttempt_select)

                    sql_update = f"""
                    UPDATE ATLAS_PANDA.JEDI_Dataset_Contents
                    SET maxAttempt=:maxAttempt
                    WHERE JEDITaskID = :taskID
                    AND datasetID IN ({dataset_bindings})
                    AND fileID IN ({file_bindings})
                    AND pandaID = :pandaID
                    """

                    self.cur.execute(sql_update + comment, varMap)

                # Commit updates
                if not self._commit():
                    raise RuntimeError("Commit error")
            except Exception:
                # roll back
                self._rollback()
                # error
                self.dumpErrorMessage(_logger, methodName)
                return False

        tmpLog.debug("done")
        return True

    def increase_max_failure(self, job_id, task_id, files):
        """Increase the max failure number by one for specific files."""
        comment = " /* DBProxy.increase_max_failure */"
        method_name = comment.split(" ")[-2].split(".")[-1]
        tmp_log = LogWrapper(_logger, method_name)
        tmp_log.debug("start")

        # Update the file entries to increase the max attempt number by one
        input_types = ("input", "pseudo_input", "pp_input", "trn_log", "trn_output")
        input_files = [pandafile for pandafile in files if pandafile.type in input_types and re.search("DBRelease", pandafile.lfn) is None]
        input_file_ids = [input_file.fileID for input_file in input_files]
        input_dataset_ids = [input_file.datasetID for input_file in input_files]

        if input_file_ids:
            try:
                # Start transaction
                self.conn.begin()

                var_map = {
                    ":taskID": task_id,
                    ":pandaID": job_id,
                }

                # Bind the files
                file_bindings = []
                for index, file_id in enumerate(input_file_ids):
                    var_map[f":file{index}"] = file_id
                    file_bindings.append(f":file{index}")
                file_bindings_str = ",".join(file_bindings)

                # Bind the datasets
                dataset_bindings = []
                for index, dataset_id in enumerate(input_dataset_ids):
                    var_map[f":dataset{index}"] = dataset_id
                    dataset_bindings.append(f":dataset{index}")
                dataset_bindings_str = ",".join(dataset_bindings)

                sql_update = f"""
                UPDATE ATLAS_PANDA.JEDI_Dataset_Contents
                SET maxFailure = maxFailure + 1
                WHERE JEDITaskID = :taskID
                AND datasetID IN ({dataset_bindings_str})
                AND fileID IN ({file_bindings_str})
                AND pandaID = :pandaID
                """

                self.cur.execute(sql_update + comment, var_map)

                # Commit updates
                if not self._commit():
                    raise RuntimeError("Commit error")

            except Exception:
                # Roll back
                self._rollback()
                # Log error
                self.dumpErrorMessage(_logger, method_name)
                return False

        tmp_log.debug("done")
        return True

    def setNoRetry(self, jobID, taskID, files):
        # Logging
        comment = " /* DBProxy.setNoRetry */"
        methodName = comment.split(" ")[-2].split(".")[-1]
        methodName += f" <PandaID={jobID} jediTaskID={taskID}>"
        tmpLog = LogWrapper(_logger, methodName)
        tmpLog.debug("start")

        # Update the file entries to avoid JEDI generating new jobs
        input_types = ("input", "pseudo_input", "pp_input", "trn_log", "trn_output")
        input_files = list(
            filter(
                lambda pandafile: pandafile.type in input_types and re.search("DBRelease", pandafile.lfn) is None,
                files,
            )
        )
        input_fileIDs = [input_file.fileID for input_file in input_files]
        input_datasetIDs = [input_file.datasetID for input_file in input_files]

        if input_fileIDs:
            try:
                # Start transaction
                self.conn.begin()

                # loop over all datasets
                for datasetID in input_datasetIDs:
                    varMap = {}
                    varMap[":taskID"] = taskID
                    varMap[":datasetID"] = datasetID
                    varMap[":keepTrack"] = 1

                    # Bind the files
                    f = 0
                    for fileID in input_fileIDs:
                        varMap[f":file{f}"] = fileID
                        f += 1
                    file_bindings = ",".join(f":file{i}" for i in range(len(input_fileIDs)))

                    sql_update = f"""
                    UPDATE ATLAS_PANDA.JEDI_Dataset_Contents
                    SET maxAttempt=attemptNr
                    WHERE JEDITaskID = :taskID
                    AND datasetID=:datasetID
                    AND fileID IN ({file_bindings})
                    AND maxAttempt IS NOT NULL AND attemptNr IS NOT NULL
                    AND maxAttempt > attemptNr
                    AND (maxFailure IS NULL OR failedAttempt IS NULL OR maxFailure > failedAttempt)
                    AND keepTrack=:keepTrack
                    AND status=:status
                    """

                    # update files in 'running' status. These files do NOT need to be counted for the nFiles*
                    varMap[":status"] = "running"
                    self.cur.execute(sql_update + comment, varMap)

                    # update files in 'ready' status. These files need to be counted for the nFiles*
                    varMap[":status"] = "ready"
                    self.cur.execute(sql_update + comment, varMap)
                    rowcount = self.cur.rowcount

                    # update datasets
                    if rowcount > 0:
                        sql_dataset = "UPDATE ATLAS_PANDA.JEDI_Datasets "
                        sql_dataset += "SET nFilesUsed=nFilesUsed+:nDiff,nFilesFailed=nFilesFailed+:nDiff "
                        sql_dataset += "WHERE jediTaskID=:taskID AND datasetID=:datasetID "
                        varMap = dict()
                        varMap[":taskID"] = taskID
                        varMap[":datasetID"] = datasetID
                        varMap[":nDiff"] = rowcount
                        self.cur.execute(sql_dataset + comment, varMap)

                # Commit updates
                if not self._commit():
                    raise RuntimeError("Commit error")
            except Exception:
                # roll back
                self._rollback()
                # error
                self.dumpErrorMessage(_logger, methodName)
                return False

        tmpLog.debug("done")
        return True

    def increaseCpuTimeTask(self, jobID, taskID, siteid, files, active):
        """
        Increases the CPU time of a task
        walltime = basewalltime + cpuefficiency*CPUTime*nEvents/Corepower/Corecount

        CPU time: execution time per event
        Walltime: time for a job
        Corepower: HS06 score
        Basewalltime: Setup time, time to download, etc. taken by the pilot
        """
        comment = " /* DBProxy.increaseCpuTimeTask */"
        methodName = comment.split(" ")[-2].split(".")[-1]
        methodName += f" <PandaID={jobID}; TaskID={taskID}>"
        tmpLog = LogWrapper(_logger, methodName)
        tmpLog.debug("start")

        # 1. Get the site information from schedconfig
        sql = """
        SELECT /* use_json_type */ sc.data.maxtime, sc.data.corepower,
            CASE
                WHEN sc.data.corecount IS NULL THEN 1
                ELSE sc.data.corecount
            END as corecount
        FROM ATLAS_PANDA.schedconfig_json sc
        WHERE sc.panda_queue=:siteid
        """
        varMap = {"siteid": siteid}
        self.cur.execute(sql + comment, varMap)
        siteParameters = self.cur.fetchone()  # example of output: [('pilotErrorCode', 1, None, None, None, None, 'no_retry', 'Y', 'Y'),...]

        if not siteParameters:
            tmpLog.debug(f"No site parameters retrieved for {siteid}")

        (maxtime, corepower, corecount) = siteParameters
        tmpLog.debug(f"siteid {siteid} has parameters: maxtime {maxtime}, corepower {corepower}, corecount {corecount}")
        if (not maxtime) or (not corepower) or (not corecount):
            tmpLog.debug(f"One or more site parameters are not defined for {siteid}... nothing to do")
            return None
        else:
            (maxtime, corepower, corecount) = (
                int(maxtime),
                float(corepower),
                int(corecount),
            )

        # 2. Get the task information
        sql = """
        SELECT jt.cputime, jt.walltime, jt.basewalltime, jt.cpuefficiency, jt.cputimeunit
        FROM ATLAS_PANDA.jedi_tasks jt
        WHERE jt.jeditaskid=:jeditaskid
        """
        varMap = {"jeditaskid": taskID}
        self.cur.execute(sql + comment, varMap)
        taskParameters = self.cur.fetchone()

        if not taskParameters:
            tmpLog.debug(f"No task parameters retrieved for jeditaskid {taskID}... nothing to do")
            return None

        (cputime, walltime, basewalltime, cpuefficiency, cputimeunit) = taskParameters
        if not cpuefficiency or not basewalltime:
            tmpLog.debug(f"CPU efficiency and/or basewalltime are not defined for task {taskID}... nothing to do")
            return None

        tmpLog.debug(
            "task {0} has parameters: cputime {1}, walltime {2}, basewalltime {3}, cpuefficiency {4}, cputimeunit {5}".format(
                taskID, cputime, walltime, basewalltime, cpuefficiency, cputimeunit
            )
        )

        # 2. Get the file information
        input_types = ("input", "pseudo_input", "pp_input", "trn_log", "trn_output")
        input_files = list(
            filter(
                lambda pandafile: pandafile.type in input_types and re.search("DBRelease", pandafile.lfn) is None,
                files,
            )
        )
        input_fileIDs = [input_file.fileID for input_file in input_files]
        input_datasetIDs = [input_file.datasetID for input_file in input_files]

        if input_fileIDs:
            varMap = {}
            varMap[":taskID"] = taskID
            varMap[":pandaID"] = jobID

            # Bind the files
            f = 0
            for fileID in input_fileIDs:
                varMap[f":file{f}"] = fileID
                f += 1
            file_bindings = ",".join(f":file{i}" for i in range(len(input_fileIDs)))

            # Bind the datasets
            d = 0
            for datasetID in input_datasetIDs:
                varMap[f":dataset{d}"] = datasetID
                d += 1
            dataset_bindings = ",".join(f":dataset{i}" for i in range(len(input_fileIDs)))

            sql_select = f"""
            SELECT jdc.fileid, jdc.nevents, jdc.startevent, jdc.endevent
            FROM ATLAS_PANDA.JEDI_Dataset_Contents jdc, ATLAS_PANDA.JEDI_Datasets jd
            WHERE jdc.JEDITaskID = :taskID
            AND jdc.datasetID IN ({dataset_bindings})
            AND jdc.fileID IN ({file_bindings})
            AND jd.datasetID = jdc.datasetID
            AND jd.masterID IS NULL
            AND jdc.pandaID = :pandaID
            """
            self.cur.execute(sql_select + comment, varMap)

            resList = self.cur.fetchall()
            nevents_total = 0
            for fileid, nevents, startevent, endevent in resList:
                tmpLog.debug(f"event information: fileid {fileid}, nevents {nevents}, startevent {startevent}, endevent {endevent}")

                if endevent is not None and startevent is not None:
                    nevents_total += endevent - startevent
                elif nevents:
                    nevents_total += nevents

            if not nevents_total:
                tmpLog.debug(f"nevents could not be calculated for job {jobID}... nothing to do")
                return None
        else:
            tmpLog.debug(f"No input files for job {jobID}, so could not update CPU time for task {taskID}")
            return None

        try:
            new_cputime = (maxtime - basewalltime) * corepower * corecount * 1.1 / (cpuefficiency / 100.0) / nevents_total

            if cputime > new_cputime:
                tmpLog.debug(f"Skipping CPU time increase since old CPU time {cputime} > new CPU time {new_cputime}")
                return None

            if active:  # only run the update if active mode. Otherwise return what would have been done
                sql_update_cputime = """
                UPDATE ATLAS_PANDA.jedi_tasks SET cputime=:cputime
                WHERE jeditaskid=:jeditaskid
                """
                varMap = {}
                varMap[":cputime"] = new_cputime
                varMap[":jeditaskid"] = taskID
                self.conn.begin()
                self.cur.execute(sql_update_cputime + comment, varMap)
                if not self._commit():
                    raise RuntimeError("Commit error")

                tmpLog.debug(f"Successfully updated the task CPU time from {cputime} to {new_cputime}")
            return new_cputime

        except (ZeroDivisionError, TypeError):
            return None

    def requestTaskParameterRecalculation(self, taskID):
        """
        Requests the recalculation of the CPU time of a task:
         1. set the walltimeUnit to NULL and the modificationTime to Now
         2. AtlasProdWatchDog > JediDBProxy.setScoutJobDataToTasks will pick up tasks with walltimeUnit == NULL
            and modificationTime > Now - 24h. This will trigger a recalculation of the task parameters (outDiskCount,
            outDiskUnit, outDiskCount, walltime, walltimeUnit, cpuTime, ioIntensity, ioIntensityUnit, ramCount, ramUnit,
            workDiskCount, workDiskUnit, workDiskCount)
        """
        comment = " /* DBProxy.requestTaskParameterRecalculation */"
        methodName = comment.split(" ")[-2].split(".")[-1]
        methodName += f" TaskID={taskID}>"
        tmpLog = LogWrapper(_logger, methodName)
        tmpLog.debug("start")

        timeNow = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
        timeLimit = timeNow - datetime.timedelta(minutes=30)

        # update the task if it was not already updated in the last 30 minutes (avoid continuous recalculation)
        sql = """
               UPDATE ATLAS_PANDA.jedi_tasks
               SET walltimeUnit=NULL, modificationTime=:timeNow
               WHERE jediTaskId=:taskID AND modificationTime < :timeLimit
               """
        varMap = {"taskID": taskID, "timeNow": timeNow, "timeLimit": timeLimit}
        self.conn.begin()
        self.cur.execute(sql, varMap)

        rowcount = self.cur.rowcount

        if not self._commit():
            raise RuntimeError("Commit error")

        tmpLog.debug("Forced recalculation of CPUTime")
        return rowcount

    # add associate sub datasets for single consumer job
    def getDestDBlocksWithSingleConsumer(self, jediTaskID, PandaID, ngDatasets):
        comment = " /* DBProxy.getDestDBlocksWithSingleConsumer */"
        methodName = comment.split(" ")[-2].split(".")[-1]
        methodName += f" <jediTaskID={jediTaskID},PandaID={PandaID}>"
        tmpLog = LogWrapper(_logger, methodName)
        tmpLog.debug("start")
        try:
            retMap = {}
            checkedDS = set()
            # sql to get files
            sqlF = "SELECT datasetID,fileID FROM ATLAS_PANDA.JEDI_Events "
            sqlF += "WHERE jediTaskID=:jediTaskID AND PandaID=:PandaID "
            # sql to get PandaIDs
            sqlP = "SELECT distinct PandaID FROM ATLAS_PANDA.filesTable4 "
            sqlP += "WHERE jediTaskID=:jediTaskID ANd datasetID=:datasetID AND fileID=:fileID "
            # sql to get sub datasets
            sqlD = "SELECT destinationDBlock,datasetID FROM ATLAS_PANDA.filesTable4 "
            sqlD += "WHERE PandaID=:PandaID AND type IN (:type1,:type2) "
            # sql to get PandaIDs in merging
            sqlM = "SELECT distinct PandaID FROM ATLAS_PANDA.filesTable4 "
            sqlM += "WHERE jediTaskID=:jediTaskID ANd datasetID=:datasetID AND status=:status "
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            varMap[":PandaID"] = PandaID
            # begin transaction
            self.conn.begin()
            # get files
            self.cur.execute(sqlF + comment, varMap)
            resF = self.cur.fetchall()
            for datasetID, fileID in resF:
                # get parallel jobs
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                varMap[":datasetID"] = datasetID
                varMap[":fileID"] = fileID
                self.cur.execute(sqlP + comment, varMap)
                resP = self.cur.fetchall()
                for (sPandaID,) in resP:
                    if sPandaID == PandaID:
                        continue
                    # get sub datasets of parallel jobs
                    varMap = {}
                    varMap[":PandaID"] = sPandaID
                    varMap[":type1"] = "output"
                    varMap[":type2"] = "log"
                    self.cur.execute(sqlD + comment, varMap)
                    resD = self.cur.fetchall()
                    subDatasets = []
                    subDatasetID = None
                    for destinationDBlock, datasetID in resD:
                        if destinationDBlock in ngDatasets:
                            continue
                        if destinationDBlock in checkedDS:
                            continue
                        checkedDS.add(destinationDBlock)
                        subDatasets.append(destinationDBlock)
                        subDatasetID = datasetID
                    if subDatasets == []:
                        continue
                    # get merging PandaID which uses sub dataset
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    varMap[":datasetID"] = datasetID
                    varMap[":status"] = "merging"
                    self.cur.execute(sqlM + comment, varMap)
                    resM = self.cur.fetchone()
                    if resM is not None:
                        (mPandaID,) = resM
                        retMap[mPandaID] = subDatasets
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmpLog.debug(f"got {len(retMap)} jobs")
            return retMap
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(_logger, methodName)
            return {}

    # check validity of merge job
    def isValidMergeJob(self, pandaID, jediTaskID):
        comment = " /* DBProxy.isValidMergeJob */"
        methodName = comment.split(" ")[-2].split(".")[-1]
        methodName += f" <Panda={pandaID}>"
        tmpLog = LogWrapper(_logger, methodName)
        tmpLog.debug("start")
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
                tmpLog.debug("merge job not found")
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
                        tmpLog.debug(f"pre-merge job {tmpPandaID} not found")
                        retVal = False
                        retMsg = tmpPandaID
                        break
                    elif resC[0] != "merging":
                        # not in merging
                        tmpLog.debug("pre-merge job in {0} != merging".format(tmpPandaID, resC[0]))
                        retVal = False
                        retMsg = tmpPandaID
                        break
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmpLog.debug(f"ret={retVal}")
            return retVal, retMsg
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(_logger, methodName)
            return None, ""

    # get dispatch datasets per user
    def getDispatchDatasetsPerUser(self, vo, prodSourceLabel, onlyActive, withSize):
        comment = " /* DBProxy.getDispatchDatasetsPerUser */"
        methodName = comment.split(" ")[-2].split(".")[-1]
        methodName += f" <vo={vo} label={prodSourceLabel}>"
        tmpLog = LogWrapper(_logger, methodName)
        tmpLog.debug("start")
        # mapping for table and job status
        tableStatMap = {"jobsDefined4": ["defined", "assigned"]}
        if not onlyActive:
            tableStatMap["jobsActive4"] = None
            tableStatMap["jobsArchived4"] = None
        try:
            userDispMap = {}
            for tableName in tableStatMap:
                statusList = tableStatMap[tableName]
                # make sql to get dispatch datasets
                varMap = {}
                varMap[":vo"] = vo
                varMap[":label"] = prodSourceLabel
                varMap[":dType"] = "dispatch"
                sqlJ = "SELECT distinct prodUserName,dispatchDBlock,jediTaskID,currentFiles "
                sqlJ += "FROM {0}.{1} j, {0}.Datasets d ".format(panda_config.schemaPANDA, tableName)
                sqlJ += "WHERE vo=:vo AND prodSourceLabel=:label "
                if statusList is not None:
                    sqlJ += "AND jobStatus IN ("
                    for tmpStat in statusList:
                        tmpKey = f":jobStat_{tmpStat}"
                        sqlJ += f"{tmpKey},"
                        varMap[tmpKey] = tmpStat
                    sqlJ = sqlJ[:-1]
                    sqlJ += ") "
                sqlJ += "AND dispatchDBlock IS NOT NULL "
                sqlJ += "AND d.name=j.dispatchDBlock AND d.modificationDate>CURRENT_DATE-14 "
                sqlJ += "AND d.type=:dType "
                # begin transaction
                self.conn.begin()
                # get dispatch datasets
                self.cur.execute(sqlJ + comment, varMap)
                resJ = self.cur.fetchall()
                if not self._commit():
                    raise RuntimeError("Commit error")
                # make map
                for prodUserName, dispatchDBlock, jediTaskID, dsSize in resJ:
                    transferType = "transfer"
                    try:
                        if dispatchDBlock.split(".")[4] == "prestaging":
                            transferType = "prestaging"
                    except Exception:
                        pass
                    userDispMap.setdefault(prodUserName, {})
                    userDispMap[prodUserName].setdefault(transferType, {"datasets": set(), "size": 0, "tasks": set()})
                    if dispatchDBlock not in userDispMap[prodUserName][transferType]["datasets"]:
                        userDispMap[prodUserName][transferType]["datasets"].add(dispatchDBlock)
                        userDispMap[prodUserName][transferType]["tasks"].add(jediTaskID)
                        userDispMap[prodUserName][transferType]["size"] += dsSize
            tmpLog.debug("done")
            return userDispMap
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(_logger, methodName)
            return {}

    # get task parameters
    def getTaskParamsPanda(self, jediTaskID):
        comment = " /* DBProxy.getTaskParamsPanda */"
        methodName = comment.split(" ")[-2].split(".")[-1]
        methodName += f" <jediTaskID={jediTaskID}>"
        tmpLog = LogWrapper(_logger, methodName)
        tmpLog.debug("start")
        try:
            # sql to get task parameters
            sqlRR = f"SELECT jedi_task_parameters FROM {panda_config.schemaDEFT}.T_TASK "
            sqlRR += "WHERE taskid=:jediTaskID "
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            # start transaction
            self.conn.begin()
            self.cur.execute(sqlRR + comment, varMap)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # read clob
            taskParams = ""
            for (clobJobP,) in self.cur:
                if clobJobP is not None:
                    try:
                        taskParams = clobJobP.read()
                    except AttributeError:
                        taskParams = str(clobJobP)
                break
            tmpLog.debug("done")
            return taskParams
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(_logger, methodName)
            return ""

    # get task attributes
    def getTaskAttributesPanda(self, jediTaskID, attrs):
        comment = " /* DBProxy.getTaskAttributesPanda */"
        methodName = comment.split(" ")[-2].split(".")[-1]
        methodName += f" <jediTaskID={jediTaskID}>"
        tmpLog = LogWrapper(_logger, methodName)
        tmpLog.debug("start")
        try:
            # sql to get task attributes
            sqlRR = "SELECT "
            for attr in attrs:
                sqlRR += f"{attr},"
            sqlRR = sqlRR[:-1]
            sqlRR += f" FROM {panda_config.schemaJEDI}.JEDI_Tasks "
            sqlRR += "WHERE jediTaskID=:jediTaskID "
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            # start transaction
            self.conn.begin()
            self.cur.execute(sqlRR + comment, varMap)
            resRR = self.cur.fetchone()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            retVal = {}
            if resRR is not None:
                for idx, attr in enumerate(attrs):
                    retVal[attr] = resRR[idx]
            tmpLog.debug(f"done {str(retVal)}")
            return retVal
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(_logger, methodName)
            return {}

    # bulk fetch PandaIDs
    def bulk_fetch_panda_ids(self, num_ids):
        comment = " /* JediDBProxy.bulk_fetch_panda_ids */"
        method_name = comment.split(" ")[-2].split(".")[-1]
        tmp_log = LogWrapper(_logger, method_name + f" <num={num_ids}>")
        tmp_log.debug("start")
        try:
            new_ids = []
            var_map = {}
            var_map[":nIDs"] = num_ids
            # sql to get fileID
            sqlFID = "SELECT ATLAS_PANDA.JOBSDEFINED4_PANDAID_SEQ.nextval FROM "
            sqlFID += "(SELECT level FROM dual CONNECT BY level<=:nIDs) "
            # start transaction
            self.conn.begin()
            self.cur.arraysize = 10000
            self.cur.execute(sqlFID + comment, var_map)
            resFID = self.cur.fetchall()
            for (id,) in resFID:
                new_ids.append(id)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmp_log.debug(f"got {len(new_ids)} IDs")
            return sorted(new_ids)
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmp_log, method_name)
            return []

    # bulk fetch fileIDs
    def bulkFetchFileIDsPanda(self, nIDs):
        comment = " /* JediDBProxy.bulkFetchFileIDsPanda */"
        methodName = comment.split(" ")[-2].split(".")[-1]
        tmpLog = LogWrapper(_logger, methodName + f" <nIDs={nIDs}>")
        tmpLog.debug("start")
        try:
            newFileIDs = []
            varMap = {}
            varMap[":nIDs"] = nIDs
            # sql to get fileID
            sqlFID = "SELECT ATLAS_PANDA.FILESTABLE4_ROW_ID_SEQ.nextval FROM "
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
            self.dumpErrorMessage(tmpLog, methodName)
            return []

    # get task status
    def getTaskStatus(self, jediTaskID):
        comment = " /* DBProxy.getTaskStatus */"
        methodName = comment.split(" ")[-2].split(".")[-1]
        tmpLog = LogWrapper(_logger, methodName + f" <jediTaskID={jediTaskID} >")
        tmpLog.debug("start")
        try:
            # sql to update input file status
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            sql = f"SELECT status FROM {panda_config.schemaJEDI}.JEDI_Tasks "
            sql += "WHERE jediTaskID=:jediTaskID "

            # start transaction
            self.conn.begin()
            self.cur.arraysize = 1000
            self.cur.execute(sql + comment, varMap)
            res = self.cur.fetchone()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            if res:
                tmpLog.debug(f"task {jediTaskID} has status: {res[0]} ")
            else:
                res = []
                tmpLog.debug(f"task {jediTaskID} not found")
            return res
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog, methodName)
            return []

    # reactivate task
    def reactivateTask(self, jediTaskID, keep_attempt_nr=False, trigger_job_generation=False):
        comment = " /* DBProxy.reactivateTask */"
        methodName = comment.split(" ")[-2].split(".")[-1]
        methodName += f" <jediTaskID={jediTaskID}>"
        tmpLog = LogWrapper(_logger, methodName, monToken=f"<jediTaskID={jediTaskID}>")
        tmpLog.debug("start")
        try:
            # sql to update task status
            sql = f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks "
            sql += "SET status=:status "
            sql += "WHERE jediTaskID=:jediTaskID "
            # sql to get datasetIDs for master
            sqlM = f"SELECT datasetID FROM {panda_config.schemaJEDI}.JEDI_Datasets "
            sqlM += "WHERE jediTaskID=:jediTaskID AND type IN (:type1,:type2,:type3) "
            # sql to increase attempt numbers and update status
            sqlAB = f"UPDATE {panda_config.schemaJEDI}.JEDI_Dataset_Contents "
            if keep_attempt_nr:
                sqlAB += "SET status=:status "
            else:
                sqlAB += "SET status=:status,attemptNr=0 "
            sqlAB += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            # sql to update datasets
            sqlD = f"UPDATE {panda_config.schemaJEDI}.JEDI_Datasets "
            sqlD += "SET status=:status,nFilesUsed=0,nFilesTobeUsed=nFiles,nFilesFinished=0,nFilesFailed=0 "
            sqlD += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            # update task status
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            varMap[":status"] = "ready"
            self.cur.execute(sql + comment, varMap)
            res = self.cur.rowcount
            # get datasetIDs for master
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            varMap[":type1"] = "input"
            varMap[":type2"] = "pseudo_input"
            varMap[":type3"] = "random_seed"
            self.cur.execute(sqlM + comment, varMap)
            resM = self.cur.fetchall()
            total_nFiles = 0

            for (datasetID,) in resM:
                # increase attempt numbers
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                varMap[":datasetID"] = datasetID
                varMap[":status"] = "ready"

                # update status and attempt number for datasets
                self.cur.execute(sqlAB + comment, varMap)
                nFiles = self.cur.rowcount

                # update dataset
                if nFiles > 0:
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    varMap[":datasetID"] = datasetID
                    varMap[":status"] = "ready"
                    tmpLog.debug(sqlD + comment + str(varMap))
                    self.cur.execute(sqlD + comment, varMap)
                    total_nFiles += nFiles

            tmpMsg = f"updated {total_nFiles} inputs and task {jediTaskID} was reactivated "
            tmpLog.debug(tmpMsg)
            tmpLog.sendMsg(tmpMsg, "jedi", "pandasrv")
            retVal = 0, tmpMsg
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # send message
            if trigger_job_generation:
                # message
                msg = srv_msg_utils.make_message("generate_job", taskid=jediTaskID)
                mb_proxy = self.get_mb_proxy("panda_jedi")
                if mb_proxy:
                    mb_proxy.send(msg)
                    tmpLog.debug(f"sent generate_job message: {msg}")
                else:
                    tmpLog.debug("message queue is not configured")
            tmpLog.debug("done")
            return retVal
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog, methodName)
            return None, "DB error"

    # get event statistics
    def getEventStat(self, jediTaskID, PandaID):
        comment = " /* DBProxy.getEventStat */"
        methodName = comment.split(" ")[-2].split(".")[-1]
        methodName += f" <jediTaskID={jediTaskID} PandaID={PandaID}>"
        tmpLog = LogWrapper(_logger, methodName)
        tmpLog.debug("start")
        try:
            # sql to get event stats
            sql = f"SELECT status,COUNT(*) FROM {panda_config.schemaJEDI}.JEDI_Events "
            sql += "WHERE jediTaskID=:jediTaskID AND PandaID=:PandaID "
            sql += "GROUP BY status "
            # start transaction
            self.conn.begin()
            self.cur.arraysize = 10000
            # get stats
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            varMap[":PandaID"] = PandaID
            self.cur.execute(sql + comment, varMap)
            resM = self.cur.fetchall()
            retMap = {}
            for eventStatus, cnt in resM:
                retMap[eventStatus] = cnt
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmpLog.debug(f"done with {str(retMap)}")
            return retMap
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog, methodName)
            return {}

    # check Job status
    def checkJobStatus(self, pandaID):
        comment = " /* DBProxy.checkJobStatus */"
        methodName = comment.split(" ")[-2].split(".")[-1]
        tmpLog = LogWrapper(_logger, methodName + f" < PandaID={pandaID} >")
        tmpLog.debug("start")
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
            varMap[":timeLimit"] = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(hours=1)
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
            tmpLog.debug(f"done with {str(retVal)}")
            return retVal
        except Exception:
            # roll back
            self._rollback()
            self.dumpErrorMessage(tmpLog, methodName)
            return retVal

    # get LNFs for jumbo job
    def getLFNsForJumbo(self, jediTaskID):
        comment = " /* DBProxy.getLFNsForJumbo */"
        method_name = comment.split(" ")[-2].split(".")[-1]
        method_name += f"< jediTaskID={jediTaskID} >"
        tmp_log = LogWrapper(_logger, method_name)
        tmp_log.debug("start")
        try:
            sqlS = "SELECT lfn,scope FROM {0}.JEDI_Datasets d, {0}.JEDI_Dataset_Contents c ".format(panda_config.schemaJEDI)
            sqlS += "WHERE d.jediTaskID=c.jediTaskID AND d.datasetID=c.datasetID AND d.jediTaskID=:jediTaskID "
            sqlS += "AND d.type IN (:type1,:type2) AND d.masterID IS NULL "
            retSet = set()
            # start transaction
            self.conn.begin()
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            varMap[":type1"] = "input"
            varMap[":type2"] = "pseudo_input"
            self.cur.execute(sqlS + comment, varMap)
            res = self.cur.fetchall()
            for tmpLFN, tmpScope in res:
                name = f"{tmpScope}:{tmpLFN}"
                retSet.add(name)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmp_log.debug(f"has {len(retSet)} LFNs")
            return retSet
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(_logger, method_name)
            return []

    # get active job attribute
    def getActiveJobAttributes(self, pandaID, attrs):
        comment = " /* DBProxy.getActiveJobAttributes */"
        method_name = comment.split(" ")[-2].split(".")[-1]
        method_name += f"< PandaID={pandaID} >"
        tmp_log = LogWrapper(_logger, method_name)
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
            self.dumpErrorMessage(_logger, method_name)
            return None

    # get number of started events
    def getNumStartedEvents(self, jobSpec):
        comment = " /* DBProxy.getNumStartedEvents */"
        method_name = comment.split(" ")[-2].split(".")[-1]
        method_name += f" < PandaID={jobSpec.PandaID} >"
        tmp_log = LogWrapper(_logger, method_name)
        tmp_log.debug("start")
        try:
            # count the number of started ranges
            sqlCDO = "SELECT /*+ INDEX_RS_ASC(tab JEDI_EVENTS_FILEID_IDX) NO_INDEX_FFS(tab JEDI_EVENTS_PK) NO_INDEX_SS(tab JEDI_EVENTS_PK) */ "
            sqlCDO += f"COUNT(*) FROM {panda_config.schemaJEDI}.JEDI_Events tab "
            sqlCDO += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
            sqlCDO += "AND status IN (:esSent,:esRunning,:esFinished,:esDone) "
            # start transaction
            self.conn.begin()
            nEvt = 0
            for fileSpec in jobSpec.Files:
                if fileSpec.type != "input":
                    continue
                varMap = {}
                varMap[":jediTaskID"] = fileSpec.jediTaskID
                varMap[":datasetID"] = fileSpec.datasetID
                varMap[":fileID"] = fileSpec.fileID
                varMap[":esSent"] = EventServiceUtils.ST_sent
                varMap[":esRunning"] = EventServiceUtils.ST_running
                varMap[":esFinished"] = EventServiceUtils.ST_finished
                varMap[":esDone"] = EventServiceUtils.ST_done
                self.cur.execute(sqlCDO + comment, varMap)
                res = self.cur.fetchone()
                if res is not None:
                    nEvt += res[0]
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmp_log.debug(f"{nEvt} events started")
            return nEvt
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(_logger, method_name)
            return None

    # make fake co-jumbo
    def makeFakeCoJumbo(self, oldJobSpec):
        comment = " /* DBProxy.self.makeFakeCoJumbo */"
        method_name = comment.split(" ")[-2].split(".")[-1]
        method_name += f" < PandaID={oldJobSpec.PandaID} >"
        tmp_log = LogWrapper(_logger, method_name)
        tmp_log.debug("start")
        try:
            # make a new job
            jobSpec = copy.copy(oldJobSpec)
            jobSpec.Files = []
            # reset job attributes
            jobSpec.startTime = None
            jobSpec.creationTime = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
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
            self.dumpErrorMessage(_logger, method_name)
            return 0

    # get JEDI file attributes
    def getJediFileAttributes(self, PandaID, jediTaskID, datasetID, fileID, attrs):
        comment = " /* DBProxy.getJediFileAttributes */"
        methodName = comment.split(" ")[-2].split(".")[-1]
        methodName += f" < PandaID={PandaID} >"
        tmpLog = LogWrapper(_logger, methodName)
        tmpLog.debug(f"start for jediTaskID={jediTaskID} datasetId={datasetID} fileID={fileID}")
        try:
            # sql to get task attributes
            sqlRR = "SELECT "
            for attr in attrs:
                sqlRR += f"{attr},"
            sqlRR = sqlRR[:-1]
            sqlRR += f" FROM {panda_config.schemaJEDI}.JEDI_Dataset_Contents "
            sqlRR += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            varMap[":datasetID"] = datasetID
            varMap[":fileID"] = fileID
            # start transaction
            self.conn.begin()
            self.cur.execute(sqlRR + comment, varMap)
            resRR = self.cur.fetchone()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            retVal = {}
            if resRR is not None:
                for idx, attr in enumerate(attrs):
                    retVal[attr] = resRR[idx]
            tmpLog.debug(f"done {str(retVal)}")
            return retVal
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(_logger, methodName)
            return {}

    # get user job metadata
    def getUserJobMetadata(self, jediTaskID):
        comment = " /* DBProxy.getUserJobMetadata */"
        methodName = comment.split(" ")[-2].split(".")[-1]
        methodName += f" < jediTaskID={jediTaskID} >"
        tmpLog = LogWrapper(_logger, methodName)
        tmpLog.debug("start")
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
            tmpLog.debug(f"got {len(retMap)} data blocks")
            return retMap
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(_logger, methodName)
            return {}

    # get jumbo job datasets
    def getJumboJobDatasets(self, n_days, grace_period):
        comment = " /* DBProxy.getUserJobMetadata */"
        methodName = comment.split(" ")[-2].split(".")[-1]
        methodName += f" < nDays={n_days} >"
        tmpLog = LogWrapper(_logger, methodName)
        tmpLog.debug("start")
        try:
            # sql to get workers
            sqlC = "SELECT t.jediTaskID,d.datasetName,t.status FROM ATLAS_PANDA.JEDI_Tasks t,ATLAS_PANDA.JEDI_Datasets d "
            sqlC += "WHERE t.prodSourceLabel='managed' AND t.useJumbo IS NOT NULL "
            sqlC += "AND t.modificationTime>CURRENT_DATE-:days AND t.modificationTime<CURRENT_DATE-:grace_period "
            sqlC += "AND t.status IN ('finished','done') "
            sqlC += "AND d.jediTaskID=t.jediTaskID AND d.type='output' "
            varMap = {}
            varMap[":days"] = n_days
            varMap[":grace_period"] = grace_period
            # start transaction
            self.conn.begin()
            self.cur.execute(sqlC + comment, varMap)
            resCs = self.cur.fetchall()
            retMap = dict()
            nDS = 0
            for jediTaskID, datasetName, status in resCs:
                retMap.setdefault(jediTaskID, {"status": status, "datasets": []})
                retMap[jediTaskID]["datasets"].append(datasetName)
                nDS += 1
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmpLog.debug(f"got {nDS} datasets")
            return retMap
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(_logger, methodName)
            return {}

    # get output datasets
    def getOutputDatasetsJEDI(self, panda_id):
        comment = " /* DBProxy.getOutputDatasetsJEDI */"
        methodName = comment.split(" ")[-2].split(".")[-1]
        methodName += f" < PandaID={panda_id} >"
        tmpLog = LogWrapper(_logger, methodName)
        tmpLog.debug("start")
        try:
            # sql to get workers
            sqlC = "SELECT d.datasetID,d.datasetName FROM ATLAS_PANDA.filesTable4 f,ATLAS_PANDA.JEDI_Datasets d "
            sqlC += "WHERE f.PandaID=:PandaID AND f.type IN (:type1,:type2) AND d.jediTaskID=f.jediTaskID AND d.datasetID=f.datasetID "
            varMap = {}
            varMap[":PandaID"] = panda_id
            varMap[":type1"] = "output"
            varMap[":type2"] = "log"
            # start transaction
            self.conn.begin()
            self.cur.execute(sqlC + comment, varMap)
            retMap = dict()
            resCs = self.cur.fetchall()
            for datasetID, datasetName in resCs:
                retMap[datasetID] = datasetName
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmpLog.debug(f"got {len(retMap)}")
            return retMap
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(_logger, methodName)
            return {}

    # lock process
    def lockProcess_PANDA(self, component, pid, time_limit, force=False):
        comment = " /* DBProxy.lockProcess_PANDA */"
        method_name = "lockProcess_PANDA"
        # defaults
        vo = "default"
        prodSourceLabel = "default"
        cloud = "default"
        workqueue_id = 0
        resource_name = "default"
        method_name += f" <component={component} pid={pid}>"
        tmp_log = LogWrapper(_logger, method_name)
        tmp_log.debug("start")
        try:
            retVal = False
            # sql to check
            sqlCT = (
                "SELECT lockedBy "
                "FROM {0}.JEDI_Process_Lock "
                "WHERE vo=:vo AND prodSourceLabel=:prodSourceLabel "
                "AND cloud=:cloud AND workqueue_id=:workqueue_id "
                "AND resource_type=:resource_name AND component=:component "
                "AND lockedTime>:lockedTime "
                "FOR UPDATE"
            ).format(panda_config.schemaJEDI)
            # sql to delete
            sqlCD = (
                "DELETE FROM {0}.JEDI_Process_Lock "
                "WHERE vo=:vo AND prodSourceLabel=:prodSourceLabel "
                "AND cloud=:cloud AND workqueue_id=:workqueue_id "
                "AND resource_type=:resource_name AND component=:component "
            ).format(panda_config.schemaJEDI)
            # sql to insert
            sqlFR = (
                "INSERT INTO {0}.JEDI_Process_Lock "
                "(vo, prodSourceLabel, cloud, workqueue_id, resource_type, component, lockedBy, lockedTime) "
                "VALUES(:vo, :prodSourceLabel, :cloud, :workqueue_id, :resource_name, :component, :lockedBy, CURRENT_DATE) "
            ).format(panda_config.schemaJEDI)
            # start transaction
            self.conn.begin()
            # check
            if not force:
                varMap = {}
                varMap[":vo"] = vo
                varMap[":prodSourceLabel"] = prodSourceLabel
                varMap[":cloud"] = cloud
                varMap[":workqueue_id"] = workqueue_id
                varMap[":resource_name"] = resource_name
                varMap[":component"] = component
                varMap[":lockedTime"] = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(minutes=time_limit)
                self.cur.execute(sqlCT + comment, varMap)
                resCT = self.cur.fetchone()
            else:
                resCT = None
            if resCT is not None:
                tmp_log.debug(f"skipped, locked by {resCT[0]}")
            else:
                # delete
                varMap = {}
                varMap[":vo"] = vo
                varMap[":prodSourceLabel"] = prodSourceLabel
                varMap[":cloud"] = cloud
                varMap[":workqueue_id"] = workqueue_id
                varMap[":resource_name"] = resource_name
                varMap[":component"] = component
                self.cur.execute(sqlCD + comment, varMap)
                # insert
                varMap = {}
                varMap[":vo"] = vo
                varMap[":prodSourceLabel"] = prodSourceLabel
                varMap[":cloud"] = cloud
                varMap[":workqueue_id"] = workqueue_id
                varMap[":resource_name"] = resource_name
                varMap[":component"] = component
                varMap[":lockedBy"] = pid
                self.cur.execute(sqlFR + comment, varMap)
                tmp_log.debug("successfully locked")
                retVal = True
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            return retVal
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmp_log, method_name)
            return retVal

    # unlock process
    def unlockProcess_PANDA(self, component, pid):
        comment = " /* DBProxy.unlockProcess_PANDA */"
        method_name = "unlockProcess_PANDA"
        # defaults
        vo = "default"
        prodSourceLabel = "default"
        cloud = "default"
        workqueue_id = 0
        resource_name = "default"
        method_name += f" <component={component} pid={pid}>"
        tmp_log = LogWrapper(_logger, method_name)
        tmp_log.debug("start")
        try:
            retVal = False
            # sql to delete
            sqlCD = (
                "DELETE FROM {0}.JEDI_Process_Lock "
                "WHERE vo=:vo AND prodSourceLabel=:prodSourceLabel AND cloud=:cloud "
                "AND workqueue_id=:workqueue_id AND lockedBy=:lockedBy "
                "AND resource_type=:resource_name AND component=:component "
            ).format(panda_config.schemaJEDI)
            # start transaction
            self.conn.begin()
            # check
            varMap = {}
            varMap[":vo"] = vo
            varMap[":prodSourceLabel"] = prodSourceLabel
            varMap[":cloud"] = cloud
            varMap[":workqueue_id"] = workqueue_id
            varMap[":resource_name"] = resource_name
            varMap[":component"] = component
            varMap[":lockedBy"] = pid
            self.cur.execute(sqlCD + comment, varMap)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmp_log.debug("done")
            retVal = True
            return retVal
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmp_log, method_name)
            return retVal

    # check process lock
    def checkProcessLock_PANDA(self, component, pid, time_limit, check_base=False):
        comment = " /* DBProxy.checkProcessLock_PANDA */"
        method_name = "checkProcessLock_PANDA"
        # defaults
        vo = "default"
        prodSourceLabel = "default"
        cloud = "default"
        workqueue_id = 0
        resource_name = "default"
        method_name += f" <component={component} pid={pid}>"
        tmp_log = LogWrapper(_logger, method_name)
        tmp_log.debug("start")
        try:
            retVal = False, None
            # sql to check
            sqlCT = (
                "SELECT lockedBy, lockedTime "
                "FROM {0}.JEDI_Process_Lock "
                "WHERE vo=:vo AND prodSourceLabel=:prodSourceLabel "
                "AND cloud=:cloud AND workqueue_id=:workqueue_id "
                "AND resource_type=:resource_name AND component=:component "
                "AND lockedTime>:lockedTime "
            ).format(panda_config.schemaJEDI)
            # start transaction
            self.conn.begin()
            # check
            varMap = {}
            varMap[":vo"] = vo
            varMap[":prodSourceLabel"] = prodSourceLabel
            varMap[":cloud"] = cloud
            varMap[":workqueue_id"] = workqueue_id
            varMap[":resource_name"] = resource_name
            varMap[":component"] = component
            varMap[":lockedTime"] = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(minutes=time_limit)
            self.cur.execute(sqlCT + comment, varMap)
            resCT = self.cur.fetchone()
            if resCT is not None:
                lockedBy, lockedTime = resCT
                if check_base:
                    # check only base part
                    if not lockedBy.startswith(pid):
                        retVal = True, lockedTime
                else:
                    # check whole string
                    if lockedBy != pid:
                        retVal = True, lockedTime
                if retVal[0]:
                    tmp_log.debug(f"found locked by {lockedBy} at {lockedTime.strftime('%Y-%m-%d_%H:%M:%S')}")
                else:
                    tmp_log.debug("found unlocked")
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmp_log.debug("done")
            return retVal
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmp_log, method_name)
            return retVal

    # insert job output report
    def insertJobOutputReport(self, panda_id, prod_source_label, job_status, attempt_nr, data):
        comment = " /* DBProxy.insertJobOutputReport */"
        method_name = "insertJobOutputReport"
        # defaults
        method_name += f" <PandaID={panda_id} attemptNr={attempt_nr}>"
        tmp_log = LogWrapper(_logger, method_name)
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
            varMap[":timeStamp"] = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
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
            self.dumpErrorMessage(tmp_log, method_name)
            return retVal

    # update data of job output report
    def updateJobOutputReport(self, panda_id, attempt_nr, data):
        comment = " /* DBProxy.updateJobOutputReport */"
        method_name = "updateJobOutputReport"
        # defaults
        method_name += f" <PandaID={panda_id} attemptNr={attempt_nr}>"
        tmp_log = LogWrapper(_logger, method_name)
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
            varMap[":timeStamp"] = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
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
            self.dumpErrorMessage(tmp_log, method_name)
            return retVal

    # deleted job output report
    def deleteJobOutputReport(self, panda_id, attempt_nr):
        comment = " /* DBProxy.deleteJobOutputReport */"
        method_name = "deleteJobOutputReport"
        # defaults
        method_name += f" <PandaID={panda_id} attemptNr={attempt_nr}>"
        tmp_log = LogWrapper(_logger, method_name)
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
            self.dumpErrorMessage(tmp_log, method_name)
            return retVal

    # get record of a job output report
    def getJobOutputReport(self, panda_id, attempt_nr):
        comment = " /* DBProxy.getJobOutputReport */"
        method_name = "getJobOutputReport"
        method_name += f" <PandaID={panda_id} attemptNr={attempt_nr}>"
        # defaults
        tmp_log = LogWrapper(_logger, method_name)
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
                    "attemptNr": prodSourceLabel,
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
            self.dumpErrorMessage(tmp_log, method_name)
            return retVal

    # lock job output report
    def lockJobOutputReport(self, panda_id, attempt_nr, pid, time_limit, take_over_from=None):
        comment = " /* DBProxy.lockJobOutputReport */"
        method_name = "lockJobOutputReport"
        method_name += f" <PandaID={panda_id} attemptNr={attempt_nr}>"
        # defaults
        tmp_log = LogWrapper(_logger, method_name)
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
            varMap[":lockedTime"] = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(minutes=time_limit)
            utc_now = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
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
            self.dumpErrorMessage(tmp_log, method_name)
            return retVal

    # unlock job output report
    def unlockJobOutputReport(self, panda_id, attempt_nr, pid, lock_offset):
        comment = " /* DBProxy.unlockJobOutputReport */"
        method_name = "unlockJobOutputReport"
        method_name += f" <PandaID={panda_id} attemptNr={attempt_nr}>"
        # defaults
        tmp_log = LogWrapper(_logger, method_name)
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
                    varMap[":lockedTime"] = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(minutes=lock_offset)
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
            self.dumpErrorMessage(tmp_log, method_name)
            return retVal

    # list pandaID, jobStatus, attemptNr, timeStamp of job output report
    def listJobOutputReport(self, only_unlocked, time_limit, limit, grace_period, labels, anti_labels):
        comment = " /* DBProxy.listJobOutputReport */"
        method_name = "listJobOutputReport"
        # defaults
        tmp_log = LogWrapper(_logger, method_name)
        tmp_log.debug(f"start label={str(labels)} anti_label={str(anti_labels)}")
        try:
            retVal = None
            if only_unlocked:
                # try to get only records unlocked or with expired lock
                varMap = {}
                varMap[":limit"] = limit
                varMap[":lockedTime"] = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(minutes=time_limit)
                varMap[":timeStamp"] = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(seconds=grace_period)
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
                    sqlGR += "AND prodSourceLabel IN ("
                    for l in labels:
                        k = f":l_{l}"
                        varMap[k] = l
                        sqlGR += f"{k},"
                    sqlGR = sqlGR[:-1]
                    sqlGR += ") "
                if anti_labels is not None:
                    sqlGR += "AND prodSourceLabel NOT IN ("
                    for l in anti_labels:
                        k = f":al_{l}"
                        varMap[k] = l
                        sqlGR += f"{k},"
                    sqlGR = sqlGR[:-1]
                    sqlGR += ") "
                sqlGR += "ORDER BY timeStamp " ") " "WHERE rownum<=:limit "
                # start transaction
                self.conn.begin()
                # check
                self.cur.execute(sqlGR + comment, varMap)
                retVal = self.cur.fetchall()
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
            self.dumpErrorMessage(tmp_log, method_name)
            return retVal

    # update problematic resource info for user
    def update_problematic_resource_info(self, user_name, jedi_task_id, resource, problem_type):
        comment = " /* DBProxy.update_problematic_resource_info */"
        method_name = comment.split()[1].split(".")[-1]
        method_name += f" < user={user_name} jediTaskID={jedi_task_id} >"
        tmp_log = LogWrapper(_logger, method_name)
        tmp_log.debug("start")
        retVal = False
        try:
            if problem_type not in ["dest", None]:
                tmp_log.debug(f"unknown problem type: {problem_type}")
                return None
            sqlR = "SELECT pagecache FROM ATLAS_PANDAMETA.users " "WHERE name=:name "
            sqlW = "UPDATE ATLAS_PANDAMETA.users SET pagecache=:data " "WHERE name=:name "
            # string to use a dict key
            jedi_task_id = str(jedi_task_id)
            # start transaction
            self.conn.begin()
            # read
            varMap = {}
            varMap[":name"] = user_name
            self.cur.execute(sqlR + comment, varMap)
            data = self.cur.fetchone()
            if data is None:
                tmp_log.debug("user not found")
            else:
                try:
                    data = json.loads(data[0])
                except Exception:
                    data = {}
                if problem_type is not None:
                    data.setdefault(problem_type, {})
                    data[problem_type].setdefault(jedi_task_id, {})
                    data[problem_type][jedi_task_id].setdefault(resource, None)
                    old = data[problem_type][jedi_task_id][resource]
                    if old is None or datetime.datetime.now(datetime.timezone.utc) - datetime.datetime.fromtimestamp(
                        old, datetime.timezone.utc
                    ) > datetime.timedelta(days=1):
                        retVal = True
                        data[problem_type][jedi_task_id][resource] = time.time()
                # delete old data
                for p in list(data):
                    for t in list(data[p]):
                        for r in list(data[p][t]):
                            ts = data[p][t][r]
                            if datetime.datetime.now(datetime.timezone.utc) - datetime.datetime.fromtimestamp(ts, datetime.timezone.utc) > datetime.timedelta(
                                days=7
                            ):
                                del data[p][t][r]
                        if not data[p][t]:
                            del data[p][t]
                    if not data[p]:
                        del data[p]
                # update
                varMap = {}
                varMap[":name"] = user_name
                varMap[":data"] = json.dumps(data)
                self.cur.execute(sqlW + comment, varMap)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmp_log.debug(f"done with {retVal} : {str(data)}")
            return retVal
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(_logger, method_name)
            return None

    # send command to a job
    def send_command_to_job(self, panda_id, com):
        comment = " /* DBProxy.send_command_to_job */"
        method_name = comment.split()[1].split(".")[-1]
        method_name += f" < PandaID={panda_id} >"
        tmp_log = LogWrapper(_logger, method_name)
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
            self.dumpErrorMessage(_logger, method_name)
            return False, "database error"

    # get LFNs in datasets
    def get_files_in_datasets(self, task_id, dataset_types):
        comment = " /* DBProxy.get_lfns_in_datasets */"
        method_name = comment.split(" ")[-2].split(".")[-1]
        method_name += f"< jediTaskID={task_id} >"
        tmp_log = LogWrapper(_logger, method_name)
        tmp_log.debug("start")
        try:
            varMap = {}
            varMap[":jediTaskID"] = task_id
            sqlD = f"SELECT datasetName,datasetID FROM {panda_config.schemaJEDI}.JEDI_Datasets WHERE jediTaskID=:jediTaskID AND type IN ("

            # Old API expects comma separated types, while new API is taking directly a tuple of dataset types
            if type(dataset_types) == str:
                dataset_types = dataset_types.split(",")

            for tmp_type in dataset_types:
                sqlD += f":{tmp_type},"
                varMap[f":{tmp_type}"] = tmp_type
            sqlD = sqlD[:-1]
            sqlD += ") "
            sqlS = f"SELECT lfn,scope,fileID,status FROM {panda_config.schemaJEDI}.JEDI_Dataset_Contents "
            sqlS += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID ORDER BY fileID "
            retVal = []
            # start transaction
            self.conn.begin()
            self.cur.execute(sqlD + comment, varMap)
            res = self.cur.fetchall()
            for datasetName, datasetID in res:
                datasetDict = {}
                datasetDict["name"] = datasetName
                datasetDict["id"] = datasetID
                # read files
                varMap = {}
                varMap[":jediTaskID"] = task_id
                varMap[":datasetID"] = datasetID
                self.cur.execute(sqlS + comment, varMap)
                resF = self.cur.fetchall()
                fileList = []
                for lfn, fileScope, fileID, status in resF:
                    fileDict = {
                        "lfn": lfn,
                        "scope": fileScope,
                        "id": fileID,
                        "status": status,
                    }
                    fileList.append(fileDict)
                retVal.append({"dataset": datasetDict, "files": fileList})
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmp_log.debug("done")
            return retVal
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(_logger, method_name)
            return None

    # update datasets asynchronously outside propagateResultToJEDI to avoid row contentions
    def async_update_datasets(self, panda_id):
        comment = " /* DBProxy.async_update_datasets */"
        methodName = comment.split(" ")[-2].split(".")[-1]
        methodName += f" < PandaID={panda_id} >"
        tmpLog = LogWrapper(_logger, methodName)
        tmpLog.debug("start")
        try:
            if panda_id is not None:
                panda_id_list = [panda_id]
            else:
                # get PandaIDs
                sql = f"SELECT DISTINCT PandaID FROM {panda_config.schemaPANDA}.SQL_QUEUE WHERE topic=:topic AND creationTime<:timeLimit"
                varMap = {
                    ":topic": SQL_QUEUE_TOPIC_async_dataset_update,
                    ":timeLimit": datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(minutes=1),
                }
                # start transaction
                self.conn.begin()
                self.cur.arraysize = 10000
                self.cur.execute(sql + comment, varMap)
                panda_id_list = [i[0] for i in self.cur.fetchall()]
                # commit
                if not self._commit():
                    raise RuntimeError("Commit error")
            if not panda_id_list:
                tmpLog.debug("done since no IDs are available")
                return None
            # loop over all IDs
            for tmp_id in panda_id_list:
                tmp_log = LogWrapper(_logger, methodName + f" < PandaID={tmp_id} >")
                sqlL = "SELECT data FROM {0}.SQL_QUEUE WHERE topic=:topic AND PandaID=:PandaID ORDER BY " "execution_order FOR UPDATE NOWAIT ".format(
                    panda_config.schemaPANDA
                )
                sqlD = f"DELETE FROM {panda_config.schemaPANDA}.SQL_QUEUE WHERE PandaID=:PandaID "
                n_try = 5
                all_ok = True
                for i_try in range(n_try):
                    all_ok = True
                    query_list = []
                    tmp_log.debug(f"Trying PandaID={tmp_id} {i_try+1}/{n_try}")
                    tmp_data_list = None
                    # start transaction
                    self.conn.begin()
                    # lock queries
                    try:
                        var_map = {
                            ":topic": SQL_QUEUE_TOPIC_async_dataset_update,
                            ":PandaID": tmp_id,
                        }
                        self.cur.execute(sqlL + comment, var_map)
                        tmp_data_list = self.cur.fetchall()
                    except Exception:
                        tmp_log.debug("cannot lock queries")
                        all_ok = False
                    if tmp_data_list:
                        # execute queries
                        if all_ok:
                            for (tmp_data,) in tmp_data_list:
                                sql, var_map = json.loads(tmp_data)
                                query_list.append((sql, var_map))
                                try:
                                    self.cur.execute(sql + comment, var_map)
                                except Exception:
                                    tmp_log.error(f'failed to execute "{sql}" var={str(var_map)}')
                                    self.dumpErrorMessage(tmpLog, methodName)
                                    all_ok = False
                                    break
                        # delete queries
                        if all_ok:
                            var_map = {":PandaID": tmp_id}
                            self.cur.execute(sqlD + comment, var_map)
                    # commit
                    if all_ok:
                        if not self._commit():
                            raise RuntimeError("Commit error")
                        for sql, var_map in query_list:
                            tmp_log.debug(sql + str(var_map))
                        tmp_log.debug("done")
                        break
                    else:
                        self._rollback()
                        if i_try + 1 < n_try:
                            time.sleep(1)
                if not all_ok:
                    tmp_log.error("all attempts failed")
            tmpLog.debug(f"processed {len(panda_id_list)} IDs")
            return True
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog, methodName)
            return False
