import atexit
import datetime
import json
import socket
import sys
import time
import traceback
from contextlib import contextmanager

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandautils.PandaUtils import naive_utcnow

from pandaserver.config import panda_config
from pandaserver.taskbuffer.JediTaskSpec import (
    push_status_changes as task_push_status_changes,
)
from pandaserver.taskbuffer.JobSpec import (
    push_status_changes as job_push_status_changes,
)

if panda_config.backend == "oracle":
    import oracledb

    varNUMBER = oracledb.NUMBER
else:
    varNUMBER = int

# topics in SQL_QUEUE
SQL_QUEUE_TOPIC_async_dataset_update = "async_dataset_update"


# Internal caching of a result. Use only for information with low update frequency and low memory footprint
def memoize(f):
    memo = {}
    kwd_mark = object()

    def helper(self, *args, **kwargs):
        now = datetime.datetime.now()
        key = args + (kwd_mark,) + tuple(sorted(kwargs.items()))
        if key not in memo or memo[key]["timestamp"] < now - datetime.timedelta(hours=1):
            tmp_data = {"value": f(self, *args, **kwargs), "timestamp": now}
            memo[key] = tmp_data
        return memo[key]["value"]

    return helper


# convert dict to bind variable dict
def convert_dict_to_bind_vars(item):
    ret = dict()
    for k in item:
        ret[f":{k}"] = item[k]
    return ret


# Base class for DB proxy modules
class BaseModule:
    # constructor
    def __init__(self, log_stream: LogWrapper):
        self._log_stream = log_stream
        self.conn = None
        self.cur = None
        self.mb_proxy_dict = None
        self.useOtherError = False
        self.backend = panda_config.backend
        # retry count
        self.nTry = 5
        # hostname
        self.myHostName = socket.getfqdn()
        self.backend = panda_config.backend
        # host name
        self.hostname = None
        # composite modules
        self.composite_modules = {}

        # typical input cache
        self.typical_input_cache = {}

        # list of work queues
        self.workQueueMap = None
        # update time for work queue map
        self.updateTimeForWorkQueue = None

        # mb proxy for JEDI
        self.jedi_mb_proxy_dict = None
        self.jedi_mb_proxy_dict_setter = None

        # JEDI config
        self.jedi_config = None

    # set JEDI attributes
    def set_jedi_attributes(self, jedi_config, jedi_mb_proxy_dict_setter):
        self.jedi_config = jedi_config
        self.jedi_mb_proxy_dict_setter = jedi_mb_proxy_dict_setter

    # abstract method to commit
    def connect(self, **kwargs):
        """
        Commit the transaction
        """
        raise NotImplementedError("connect is not implemented")

    # commit
    def _commit(self):
        try:
            self.conn.commit()
            return True
        except Exception:
            self._log_stream.error("commit error")
            return False

    # rollback
    def _rollback(self, useOtherError=False):
        return_value = True
        # rollback
        err_code = None
        self._log_stream.debug("rollback")
        try:
            self.conn.rollback()
        except Exception:
            self._log_stream.error("rollback error")
            return_value = False
        # reconnect if needed
        try:
            err_type, err_value = sys.exc_info()[:2]
            # get error code for postgres
            if self.backend == "postgres":
                try:
                    err_code = err_value.pgcode
                except Exception:
                    pass
            # get ORA ErrorCode
            if err_code is None:
                err_code = str(err_value).split()[0]
                err_code = err_code[:-1]
            err_msg = f"rollback EC:{err_code} {err_value}"
            self._log_stream.debug(err_msg)
            # error codes for connection error
            if self.backend == "oracle":
                error_list_for_reconnect = [
                    "ORA-01012",
                    "ORA-01033",
                    "ORA-01034",
                    "ORA-01089",
                    "ORA-03113",
                    "ORA-03114",
                    "ORA-12203",
                    "ORA-12500",
                    "ORA-12571",
                    "ORA-03135",
                    "ORA-25402",
                ]
                # other errors are apparently given when connection lost contact
                if useOtherError:
                    error_list_for_reconnect += ["ORA-01861", "ORA-01008"]
            elif self.backend == "postgres":
                import psycopg2.errorcodes as psycopg_errorcodes

                error_list_for_reconnect = [
                    psycopg_errorcodes.CONNECTION_EXCEPTION,
                    psycopg_errorcodes.SQLSERVER_REJECTED_ESTABLISHMENT_OF_SQLCONNECTION,
                    psycopg_errorcodes.CONNECTION_DOES_NOT_EXIST,
                    psycopg_errorcodes.SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION,
                    psycopg_errorcodes.CONNECTION_FAILURE,
                    psycopg_errorcodes.READ_ONLY_SQL_TRANSACTION,
                ]
            else:
                # mysql error codes for connection error
                import MySQLdb
                from MySQLdb.constants.CR import (
                    CONN_HOST_ERROR,
                    CONNECTION_ERROR,
                    LOCALHOST_CONNECTION,
                    SERVER_LOST,
                )
                from MySQLdb.constants.ER import (
                    ACCESS_DENIED_ERROR,
                    DBACCESS_DENIED_ERROR,
                    ILLEGAL_VALUE_FOR_TYPE,
                    SERVER_SHUTDOWN,
                )

                error_list_for_reconnect = [
                    ACCESS_DENIED_ERROR,
                    DBACCESS_DENIED_ERROR,
                    SERVER_SHUTDOWN,
                    CONNECTION_ERROR,
                    CONN_HOST_ERROR,
                    LOCALHOST_CONNECTION,
                    SERVER_LOST,
                ]
                # other errors are apparently given when connection lost contact
                if useOtherError:
                    error_list_for_reconnect += [ILLEGAL_VALUE_FOR_TYPE]
            if err_code in error_list_for_reconnect:
                # reconnect
                reconnect_stat = self.connect(reconnect=True)
                self._log_stream.debug(f"rollback reconnected {reconnect_stat}")
        except Exception:
            pass
        # return
        return return_value

    # add composite module
    def add_composite_module(self, module_name, module):
        self.composite_modules[module_name] = module

    # get composite module
    def get_composite_module(self, module_name):
        return self.composite_modules.get(module_name, None)

    # dump error message
    def dump_error_message(self, tmp_log: LogWrapper):
        """
        Dump error message to the log

        :param tmp_log: log wrapper
        """
        # error
        err_type, err_value = sys.exc_info()[:2]
        err_str = f"{err_type.__name__} {err_value}"
        err_str.strip()
        err_str += " "
        err_str += traceback.format_exc()
        tmp_log.error(err_str)

    # create logger with tag
    def create_tagged_logger(self, comment: str, tag: str = None) -> LogWrapper:
        """
        Create logger from function comment and tag

        param comment: comment of the function
        param tag: tag to add to the method name
        return: logger
        """
        method_name = comment.split(" ")[-2].split(".")[-1]
        if tag is None:
            tag = naive_utcnow().strftime("%Y-%m-%d/%H:%M:%S.%f")
        method_name += f" < {tag} >"
        tmp_log = LogWrapper(self._log_stream, method_name)
        return tmp_log

    # get configuration value. cached for an hour
    @memoize
    def getConfigValue(self, component, key, app="pandaserver", vo=None):
        comment = " /* DBProxy.getConfigValue */"
        tmp_log = self.create_tagged_logger(comment)
        varMap = {":component": component, ":key": key, ":app": app}
        sql = """
        SELECT value, value_json, type FROM ATLAS_PANDA.CONFIG
        WHERE component=:component
        AND key=:key
        AND app=:app
        """

        # If VO is specified, select only the config values for this VO or VO independent values
        if vo:
            varMap[":vo"] = vo
            sql += "AND (vo=:vo or vo IS NULL)"

        self.cur.execute(sql + comment, varMap)

        try:
            value_str, value_json_str, type = self.cur.fetchone()
        except TypeError:
            error_message = f"Specified key={key} not found for component={component} app={app}"
            tmp_log.debug(error_message)
            return None

        try:
            if type.lower() in ("str", "string"):
                return value_str
            elif type.lower() in ("int", "integer"):
                return int(value_str)
            elif type.lower() == "float":
                return float(value_str)
            elif type.lower() in ("bool", "boolean"):
                if value_str.lower() == "true":
                    return True
                else:
                    return False
            elif type.lower() == "json":
                return json.loads(value_json_str)
            else:
                raise ValueError
        except json.decoder.JSONDecodeError:
            tmp_log.debug(f"Could not decode. Value_json: {value_json_str}, Type: {type}")
            return None
        except ValueError:
            tmp_log.debug(f"Wrong value/type pair. Value: {value_str}, Type: {type}")
            return None
        except Exception as e:
            tmp_log.debug(f"Unexpected error: {str(e)}")
            raise e

    def getvalue_corrector(self, value):
        """
        Needed to support old and new versions of cx_Oracle
        :return:
        """
        if isinstance(value, list):  # cx_Oracle version >= 6.3
            return value[0]
        else:  # cx_Oracle version < 6.3
            return value

    # get mb proxy
    def get_mb_proxy(self, channel):
        if self.mb_proxy_dict is None:
            try:
                if hasattr(panda_config, "mq_configFile") and panda_config.mq_configFile:
                    # delay import to open logger file inside python daemon
                    from pandaserver.taskbuffer.PanDAMsgProcessor import MsgProcAgent

                    out_q_list = [
                        "panda_jobstatus",
                        "panda_jedi",
                        "panda_pilot_topic",
                        "panda_pilot_queue",
                    ]
                    mp_agent = MsgProcAgent(config_file=panda_config.mq_configFile)
                    mb_proxy_dict = mp_agent.start_passive_mode(in_q_list=[], out_q_list=out_q_list)
                    # stop with atexit
                    atexit.register(mp_agent.stop_passive_mode)
                    # return
                    self.mb_proxy_dict = mb_proxy_dict
            except Exception:
                comment = " /* DBProxy.get_mb_proxy */"
                tmp_log = self.create_tagged_logger(comment)
                self.dump_error_message(tmp_log)
                self.mb_proxy_dict = {}
        if not self.mb_proxy_dict or channel not in self.mb_proxy_dict["out"]:
            return None
        return self.mb_proxy_dict["out"][channel]

    # close connection
    def close_connection(self):
        if self.conn:
            try:
                self.conn.close()
            except Exception:
                pass
        return

    # cleanup
    def cleanup(self):
        comment = " /* DBProxy.cleanup */"
        tmp_log = self.create_tagged_logger(comment)
        tmp_log.debug("start")
        self.close_connection()
        atexit.unregister(self.close_connection)
        tmp_log.debug("done")

    # query an SQL
    def querySQL(self, sql, arraySize=1000):
        comment = " /* DBProxy.querySQL */"
        tmp_log = self.create_tagged_logger(comment)
        try:
            tmp_log.debug(f"SQL={sql} ")
            # begin transaction
            self.conn.begin()
            self.cur.arraysize = arraySize
            self.cur.execute(sql + comment)
            res = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            return res
        except Exception:
            # roll back
            self._rollback(self.useOtherError)
            self.dump_error_message(tmp_log)
            return None

    # query an SQL return Status
    def querySQLS(self, sql, varMap, arraySize=1000):
        comment = " /* DBProxy.querySQLS */"
        tmp_log = self.create_tagged_logger(comment)
        try:
            tmp_log.debug(f"SQL={sql} vapMap={varMap} ")
            # begin transaction
            self.conn.begin()
            self.cur.arraysize = arraySize
            ret = self.cur.execute(sql + comment, varMap)
            if ret:
                ret = True
            if sql.startswith("INSERT") or sql.startswith("UPDATE") or sql.startswith("DELETE"):
                res = self.cur.rowcount
            else:
                res = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            return ret, res
        except Exception as e:
            # roll back
            self._rollback(self.useOtherError)
            self.dump_error_message(tmp_log)
            return -1, None

    # execute an SQL return with executemany
    def executemanySQL(self, sql, varMaps, arraySize=1000):
        comment = " /* DBProxy.executemanySQL */"
        try:
            # begin transaction
            self.conn.begin()
            self.cur.arraysize = arraySize
            ret = self.cur.executemany(sql + comment, varMaps)
            if sql.startswith("INSERT") or sql.startswith("UPDATE") or sql.startswith("DELETE"):
                res = self.cur.rowcount
            else:
                raise RuntimeError("Operation unsupported. Only INSERT, UPDATE, DELETE are allowed")
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            return res
        except Exception as e:
            # roll back
            self._rollback(self.useOtherError)
            tmp_log = self.create_tagged_logger(comment)
            tmp_log.error(f"{sql} {str(varMaps)}")
            self.dump_error_message(tmp_log)
            return None

    # get CLOB
    def getClobObj(self, sql, varMap, arraySize=10000, use_commit=True):
        comment = " /* DBProxy.getClobObj */"
        try:
            # begin transaction
            if use_commit:
                self.conn.begin()
                self.cur.arraysize = arraySize
            ret = self.cur.execute(sql + comment, varMap)
            if ret:
                ret = True
            res = []
            for items in self.cur:
                resItem = []
                for item in items:
                    # read CLOB
                    try:
                        itemRead = item.read()
                    except AttributeError:
                        itemRead = item
                    resItem.append(itemRead)
                # append
                res.append(resItem)
            # commit
            if use_commit:
                if not self._commit():
                    raise RuntimeError("Commit error")
            return ret, res
        except Exception as e:
            # roll back
            if use_commit:
                self._rollback()
            tmp_log = self.create_tagged_logger(comment)
            tmp_log.error(f"{sql} {str(varMap)}")
            self.dump_error_message(tmp_log)
            return -1, None

    # wake up connection
    def wakeUp(self):
        comment = " /* DBProxy.wakeUp */"
        tmp_log = self.create_tagged_logger(comment)
        for iTry in range(5):
            try:
                # check if the connection is working
                self.conn.ping()
                return
            except Exception:
                tmp_log.error(f"{iTry} : connection is dead")
                self.dump_error_message(tmp_log)
                # wait for reconnection
                time.sleep(1)
                self.connect(reconnect=True)

    # transaction as a context manager
    @contextmanager
    def transaction(self, name: str):
        """
        Context manager for transaction

        Args:
            name (str): name of the transaction to be shown in the log

        Yields:
            Any: the cursor object for executing SQL commands
            Any: the logger object for logging in DBProxy
        """
        comment = " /* DBProxy.transaction */"
        try:
            tmp_log = self.create_tagged_logger(comment, tag=name)
            tmp_log.debug("start")
            # begin transaction
            self.conn.begin()
            # cursor and logger for the with block
            yield (self.cur, tmp_log)
            # commit transaction
            if not self._commit():
                raise RuntimeError("Commit error")
            tmp_log.debug("done")
        except Exception as e:
            # roll back
            self._rollback()
            self.dump_error_message(tmp_log)
            raise e

    # record status change
    def recordStatusChange(self, pandaID, jobStatus, jobInfo=None, infoMap={}, useCommit=True, no_late_bulk_exec=True, extracted_sqls=None):
        comment = " /* DBProxy.recordStatusChange */"
        tmp_log = self.create_tagged_logger(comment)
        # check config
        if not hasattr(panda_config, "record_statuschange") or panda_config.record_statuschange is not True:
            return
        # get job info
        varMap = {}
        varMap[":PandaID"] = pandaID
        varMap[":jobStatus"] = jobStatus
        varMap[":modificationHost"] = self.myHostName
        if jobInfo is not None:
            varMap[":computingSite"] = jobInfo.computingSite
            varMap[":cloud"] = jobInfo.cloud
            varMap[":prodSourceLabel"] = jobInfo.prodSourceLabel
        elif infoMap is not None:
            varMap[":computingSite"] = infoMap["computingSite"]
            varMap[":cloud"] = infoMap["cloud"]
            varMap[":prodSourceLabel"] = infoMap["prodSourceLabel"]
        else:
            # no info
            return
        # convert NULL to None
        for tmpKey in varMap:
            if varMap[tmpKey] == "NULL":
                varMap[tmpKey] = None
        # insert
        sql = "INSERT INTO ATLAS_PANDA.jobs_StatusLog "
        sql += "(PandaID,modificationTime,jobStatus,prodSourceLabel,cloud,computingSite,modificationHost,modiftime_extended) "
        sql += "VALUES (:PandaID,CURRENT_DATE,:jobStatus,:prodSourceLabel,:cloud,:computingSite,:modificationHost,CURRENT_TIMESTAMP) "
        try:
            # start transaction
            if no_late_bulk_exec:
                if useCommit:
                    self.conn.begin()
                self.cur.execute(sql + comment, varMap)
                # commit
                if useCommit:
                    if not self._commit():
                        raise RuntimeError("Commit error")
            else:
                extracted_sqls.setdefault("state_change", {"sql": sql + comment, "vars": []})
                extracted_sqls["state_change"]["vars"].append(varMap)
        except Exception:
            # roll back
            if useCommit and no_late_bulk_exec:
                self._rollback()
            self.dump_error_message(tmp_log)
            if not useCommit:
                raise RuntimeError("recordStatusChange failed")
        return

    def push_job_status_message(
        self,
        job_spec,
        panda_id,
        status,
        jedi_task_id=None,
        special_handling=None,
        extra_data=None,
    ):
        comment = " /* DBProxy.push_job_status_message */"
        if not (hasattr(panda_config, "mq_configFile") and panda_config.mq_configFile):
            # skip if not configured
            return
        to_push = False
        if special_handling is not None:
            to_push = job_push_status_changes(special_handling)
        elif job_spec is not None:
            to_push = job_spec.push_status_changes()
        # only run if to push status change
        if not to_push:
            return
        # skip statuses unnecessary to push
        if status in ["sent", "holding", "merging"]:
            return
        # skip if no mb to push to
        mb_proxy = self.get_mb_proxy("panda_jobstatus")
        if not mb_proxy:
            return
        if to_push:
            tmp_log = self.create_tagged_logger(comment)
            # push job status change
            try:
                now_time = naive_utcnow()
                now_ts = int(now_time.timestamp())
                # init
                inputs = []
                computingsite = None
                error_tmp_dict = {}
                # info from job spec
                if job_spec is not None:
                    # task id
                    if jedi_task_id is None:
                        jedi_task_id = job_spec.jediTaskID
                    # inputs
                    if job_spec.Files is not None:
                        for file_spec in job_spec.Files:
                            if file_spec.type in ["input", "pseudo_input"]:
                                inputs.append(file_spec.lfn)
                    # computing site
                    if job_spec.computingSite is not None:
                        computingsite = job_spec.computingSite
                    # error codes and diags
                    error_tmp_dict["piloterrorcode"] = job_spec.pilotErrorCode
                    error_tmp_dict["exeerrorcode"] = job_spec.exeErrorCode
                    error_tmp_dict["superrorcode"] = job_spec.supErrorCode
                    error_tmp_dict["ddmerrorcode"] = job_spec.ddmErrorCode
                    error_tmp_dict["brokerageerrorcode"] = job_spec.brokerageErrorCode
                    error_tmp_dict["jobdispatchererrorcode"] = job_spec.jobDispatcherErrorCode
                    error_tmp_dict["taskbuffererrorcode"] = job_spec.taskBufferErrorCode
                    error_tmp_dict["piloterrordiag"] = job_spec.pilotErrorDiag
                    error_tmp_dict["exeerrordiag"] = job_spec.exeErrorDiag
                    error_tmp_dict["superrordiag"] = job_spec.supErrorDiag
                    error_tmp_dict["ddmerrordiag"] = job_spec.ddmErrorDiag
                    error_tmp_dict["brokerageerrordiag"] = job_spec.brokerageErrorDiag
                    error_tmp_dict["jobdispatchererrordiag"] = job_spec.jobDispatcherErrorDiag
                    error_tmp_dict["taskbuffererrordiag"] = job_spec.taskBufferErrorDiag
                # message
                orig_msg_dict = {
                    "msg_type": "job_status",
                    "jobid": panda_id,
                    "taskid": jedi_task_id,
                    "status": status,
                    "timestamp": now_ts,
                }
                update_msg_dict = {
                    "computingsite": computingsite,
                    "inputs": inputs if inputs else None,
                }
                update_msg_dict.update(error_tmp_dict)
                msg_dict = update_msg_dict.copy()
                if extra_data:
                    msg_dict.update(extra_data)
                msg_dict.update(orig_msg_dict)
                msg = json.dumps(msg_dict)
                if mb_proxy.got_disconnected:
                    mb_proxy.restart()
                mb_proxy.send(msg)
                tmp_log.debug(f"sent message: {msg}")
            except Exception:
                self.dump_error_message(tmp_log)

    def insert_to_query_pool(self, topic, panda_id, task_id, sql, var_map, exec_order):
        comment = " /* DBProxy.insert_to_query_pool */"
        sqlI = (
            "INSERT INTO {}.SQL_QUEUE (topic,PandaID,jediTaskID,creationTime,data,execution_order) "
            "VALUES(:topic,:PandaID,:taskID,:creationTime,:data,:execution_order) ".format(panda_config.schemaPANDA)
        )
        varMap = {
            ":topic": topic,
            ":PandaID": panda_id,
            ":taskID": task_id,
            ":creationTime": naive_utcnow(),
            ":execution_order": exec_order,
            ":data": json.dumps((sql, var_map)),
        }
        self.cur.execute(sqlI + comment, varMap)

    # check if exception is from NOWAIT
    def isNoWaitException(self, errValue):
        # for oracle
        ora_err_code = str(errValue).split()[0]
        ora_err_code = ora_err_code[:-1]
        if ora_err_code == "ORA-00054":
            return True
        # for postgres
        if type(errValue).__name__ == "LockNotAvailable":
            return True
        return False

    # set super status
    def setSuperStatus_JEDI(self, jediTaskID, superStatus):
        comment = " /* JediDBProxy.setSuperStatus_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID}")
        retTasks = []
        try:
            # sql to set super status
            sqlCT = f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks "
            sqlCT += "SET superStatus=:superStatus "
            sqlCT += "WHERE jediTaskID=:jediTaskID "
            # set super status
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            varMap[":superStatus"] = superStatus
            self.cur.execute(sqlCT + comment, varMap)
            return True
        except Exception:
            # error
            self.dump_error_message(tmpLog)
            return False

    # set DEFT status
    def setDeftStatus_JEDI(self, jediTaskID, taskStatus):
        comment = " /* JediDBProxy.setDeftStatus_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID}")
        try:
            sqlD = f"UPDATE {panda_config.schemaDEFT}.T_TASK "
            sqlD += "SET status=:status,timeStamp=CURRENT_DATE "
            sqlD += "WHERE taskID=:jediTaskID "
            varMap = {}
            varMap[":status"] = taskStatus
            varMap[":jediTaskID"] = jediTaskID
            tmpLog.debug(sqlD + comment + str(varMap))
            self.cur.execute(sqlD + comment, varMap)
            return True
        except Exception:
            # error
            self.dump_error_message(tmpLog)
            return False

    # task status logging
    def record_task_status_change(self, jedi_task_id):
        comment = " /* JediDBProxy.record_task_status_change */"
        tmpLog = self.create_tagged_logger(comment, f"jediTaskID={jedi_task_id}")
        tmpLog.debug("start")
        varMap = dict()
        varMap[":jediTaskID"] = jedi_task_id
        varMap[":modificationHost"] = socket.getfqdn()
        # sql
        sqlNS = (
            "INSERT INTO {0}.TASKS_STATUSLOG "
            "(jediTaskID,modificationTime,status,modificationHost,attemptNr,reason) "
            "SELECT jediTaskID,CURRENT_TIMESTAMP,status,:modificationHost,attemptNr,"
            "SUBSTR(errorDialog,0,255) "
            "FROM {0}.JEDI_Tasks WHERE jediTaskID=:jediTaskID "
        ).format(panda_config.schemaJEDI)
        self.cur.execute(sqlNS + comment, varMap)
        tmpLog.debug("done")

    # push task status message
    def push_task_status_message(self, task_spec, jedi_task_id, status, split_rule=None):
        to_push = False
        if task_spec is not None:
            to_push = task_spec.push_status_changes()
        elif split_rule is not None:
            to_push = task_push_status_changes(split_rule)
        # only run if to push status change
        if not to_push:
            return
        # skip statuses unnecessary to push
        # if status in ['pending']:
        #     return
        comment = " /* JediDBProxy.push_task_status_message */"
        tmpLog = self.create_tagged_logger(comment, f"jediTaskID={jedi_task_id}")
        tmpLog.debug("start")
        # send task status messages to mq
        try:
            now_time = naive_utcnow()
            now_ts = int(now_time.timestamp())
            msg_dict = {
                "msg_type": "task_status",
                "taskid": jedi_task_id,
                "status": status,
                "timestamp": now_ts,
            }
            msg = json.dumps(msg_dict)
            if self.jedi_mb_proxy_dict is None:
                self.jedi_mb_proxy_dict = self.jedi_mb_proxy_dict_setter()
                if self.jedi_mb_proxy_dict is None:
                    tmpLog.debug("Failed to get mb_proxy of internal MQs. Skipped ")
                    return
            try:
                mb_proxy = self.jedi_mb_proxy_dict["out"]["jedi_jobtaskstatus"]
            except KeyError as e:
                tmpLog.warning(f"Skipped due to {e} ; jedi_mb_proxy_dict is {self.jedi_mb_proxy_dict}")
                return
            if mb_proxy.got_disconnected:
                mb_proxy.restart()
            mb_proxy.send(msg)
        except Exception:
            self.dump_error_message(tmpLog)
        tmpLog.debug("done")

    # push message to message processors which triggers functions of agents
    def push_task_trigger_message(self, msg_type, jedi_task_id, data_dict=None, priority=None, task_spec=None):
        comment = " /* JediDBProxy.push_task_trigger_message */"
        tmpLog = self.create_tagged_logger(comment, f"msg_type={msg_type} jediTaskID={jedi_task_id}")
        tmpLog.debug("start")
        # send task status messages to mq
        try:
            now_time = naive_utcnow()
            now_ts = int(now_time.timestamp())
            # get mbproxy
            msg_dict = {}
            if data_dict:
                msg_dict.update(data_dict)
            msg_dict.update(
                {
                    "msg_type": msg_type,
                    "taskid": jedi_task_id,
                    "timestamp": now_ts,
                }
            )
            msg = json.dumps(msg_dict)
            if self.jedi_mb_proxy_dict is None:
                self.jedi_mb_proxy_dict = self.jedi_mb_proxy_dict_setter()
                if self.jedi_mb_proxy_dict is None:
                    tmpLog.debug("Failed to get mb_proxy of internal MQs. Skipped ")
                    return
            try:
                mq_name = msg_type
                mb_proxy = self.jedi_mb_proxy_dict["out"][mq_name]
            except KeyError as e:
                tmpLog.warning(f"Skipped due to {e} ; jedi_mb_proxy_dict is {self.jedi_mb_proxy_dict}")
                return
            if mb_proxy.got_disconnected:
                mb_proxy.restart()
            # message priority
            msg_priority = None
            if priority is not None:
                msg_priority = priority
            elif task_spec is not None:
                try:
                    if task_spec.prodSourceLabel == "user":
                        if task_spec.gshare in ["User Analysis", "Express Analysis"]:
                            msg_priority = 2
                        else:
                            msg_priority = 1
                except AttributeError:
                    pass
            # send message
            if msg_priority is not None:
                mb_proxy.send(msg, priority=msg_priority)
            else:
                mb_proxy.send(msg)
        except Exception:
            self.dump_error_message(tmpLog)
            return
        tmpLog.debug("done")
        return True
