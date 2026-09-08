import atexit
import datetime
import json
import socket
import sys
import time
import traceback
from collections.abc import Callable
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Callable, Iterator

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandautils.PandaUtils import naive_utcnow

from pandaserver.config import panda_config
from pandaserver.taskbuffer.JediTaskSpec import (
    push_status_changes as task_push_status_changes,
)
from pandaserver.taskbuffer.JobSpec import (
    push_status_changes as job_push_status_changes,
)

if TYPE_CHECKING:
    # imported for annotations only. WrappedCursor imports panda_config, so
    # importing it at runtime here would close an import cycle
    from pandaserver.taskbuffer.JediTaskSpec import JediTaskSpec
    from pandaserver.taskbuffer.JobSpec import JobSpec
    from pandaserver.taskbuffer.WorkQueueMapper import WorkQueueMapper
    from pandaserver.taskbuffer.wrapped_oracle_conn import WrappedOracleConn
    from pandaserver.taskbuffer.WrappedCursor import WrappedCursor
    from pandaserver.taskbuffer.WrappedPostgresConn import WrappedPostgresConn

if panda_config.backend == "oracle":
    import oracledb

    varNUMBER = oracledb.NUMBER
else:
    varNUMBER = int

# topics in SQL_QUEUE
SQL_QUEUE_TOPIC_async_dataset_update = "async_dataset_update"


# Internal caching of a result. Use only for information with low update frequency and low memory footprint
def memoize(f: Callable[..., Any]) -> Callable[..., Any]:
    memo: dict[Any, Any] = {}
    kwd_mark = object()

    def helper(self: "BaseModule", *args: Any, **kwargs: Any) -> Any:
        now = datetime.datetime.now()
        key = args + (kwd_mark,) + tuple(sorted(kwargs.items()))
        if key not in memo or memo[key]["timestamp"] < now - datetime.timedelta(hours=1):
            tmp_data = {"value": f(self, *args, **kwargs), "timestamp": now}
            memo[key] = tmp_data
        return memo[key]["value"]

    return helper


# convert dict to bind variable dict
def convert_dict_to_bind_vars(item: dict[str, Any]) -> dict[str, Any]:
    ret = dict()
    for k in item:
        ret[f":{k}"] = item[k]
    return ret


# Base class for DB proxy modules
class BaseModule:
    # The connection and cursor are installed by DBProxy.connect() before any query
    # runs. They are declared non-Optional on purpose: typing them as Optional would
    # only push a None check onto each of the ~1600 call sites without making any of
    # them safer, since a query issued before connect() is a bug either way.
    conn: "WrappedOracleConn | WrappedPostgresConn"
    cur: "WrappedCursor"

    # The pandajedi jedi_config module, installed by set_jedi_attributes() when the proxy
    # is a JediDBProxy. Only the *_JEDI methods read it and those run nowhere else, so it
    # is declared non-Optional for the same reason as conn/cur above. Its type is Any
    # because pandaserver cannot import pandajedi to name it.
    jedi_config: Any

    # Work queue map, built on demand by EntityModule.refreshWorkQueueMap() together with
    # the time it was last built, which is what decides whether a rebuild is due
    workQueueMap: "WorkQueueMapper | None"
    updateTimeForWorkQueue: datetime.datetime | None

    # Message broker proxies, built on first use. The JEDI one is built by the setter that
    # set_jedi_attributes() installs, which is why both are unset until then
    mb_proxy_dict: dict[str, Any] | None
    jedi_mb_proxy_dict: dict[str, Any] | None
    jedi_mb_proxy_dict_setter: "Callable[[], dict[str, Any] | None] | None"

    # constructor
    def __init__(self, log_stream: LogWrapper):
        self._log_stream = log_stream
        self.conn = None  # type: ignore[assignment]
        self.cur = None  # type: ignore[assignment]
        self.mb_proxy_dict = None
        self.useOtherError = False
        self.backend = panda_config.backend
        # retry count
        self.nTry = 5
        # hostname
        self.myHostName = socket.getfqdn()
        self.backend = panda_config.backend
        # host name
        self.hostname: str | None = None
        # composite modules
        self.composite_modules: dict[str, Any] = {}

        # typical input cache
        self.typical_input_cache: dict[str, Any] = {}

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
    def set_jedi_attributes(self, jedi_config: Any, jedi_mb_proxy_dict_setter: Any) -> None:
        self.jedi_config = jedi_config
        self.jedi_mb_proxy_dict_setter = jedi_mb_proxy_dict_setter

    # abstract method to connect. *args/**kwargs because the concrete signature belongs
    # to the subclass, which is where the connection parameters and their defaults are
    def connect(self, *args: Any, **kwargs: Any) -> Any:
        """
        Connect to the database
        """
        raise NotImplementedError("connect is not implemented")

    # commit
    def _commit(self) -> bool:
        try:
            self.conn.commit()
            return True
        except Exception:
            self._log_stream.error("commit error")
            return False

    # rollback
    def _rollback(self, useOtherError: bool = False) -> bool:
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
                # pgcode is on the psycopg exceptions only, and err_value is whatever the
                # caller is handling -- getattr keeps that as a lookup rather than a raise
                err_code = getattr(err_value, "pgcode", None)
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
    def add_composite_module(self, module_name: str, module: "BaseModule") -> None:
        self.composite_modules[module_name] = module

    # get composite module
    def get_composite_module(self, module_name: str) -> Any:
        return self.composite_modules.get(module_name, None)

    # dump error message
    def dump_error_message(self, tmp_log: LogWrapper) -> None:
        """
        Dump error message to the log

        :param tmp_log: log wrapper
        """
        # error
        err_type, err_value = sys.exc_info()[:2]
        if err_type is None:
            # every one of the ~400 call sites is inside an except block, which is the only
            # place sys.exc_info() returns anything. Log the caller instead of raising
            # AttributeError on None.__name__ if that ever stops being true.
            tmp_log.error(f"dump_error_message() called outside an except block\n{''.join(traceback.format_stack())}")
            return
        err_str = f"{err_type.__name__} {err_value}"
        err_str.strip()
        err_str += " "
        err_str += traceback.format_exc()
        tmp_log.error(err_str)

    # create logger with tag
    def create_tagged_logger(self, comment: str, tag: str | None = None) -> LogWrapper:
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
    def getConfigValue(self, component: str, key: str, app: str = "pandaserver", vo: str | None = None) -> Any:
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

    def getvalue_corrector(self, value: Any) -> Any:
        """
        Needed to support old and new versions of cx_Oracle
        :return:
        """
        if isinstance(value, list):  # cx_Oracle version >= 6.3
            return value[0]
        else:  # cx_Oracle version < 6.3
            return value

    # get mb proxy
    def get_mb_proxy(self, channel: str) -> Any:
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
    def close_connection(self) -> None:
        if self.conn:
            try:
                self.conn.close()
            except Exception:
                pass
        return

    # cleanup
    def cleanup(self) -> None:
        comment = " /* DBProxy.cleanup */"
        tmp_log = self.create_tagged_logger(comment)
        tmp_log.debug("start")
        self.close_connection()
        atexit.unregister(self.close_connection)
        tmp_log.debug("done")

    # query an SQL
    def querySQL(self, sql: str, arraySize: int = 1000) -> Any:
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
    def querySQLS(self, sql: str, varMap: dict[str, Any], arraySize: int = 1000) -> tuple[Any, Any]:
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
    def executemanySQL(self, sql: str, varMaps: list[dict[str, Any]], arraySize: int = 1000) -> Any:
        comment = " /* DBProxy.executemanySQL */"
        try:
            # begin transaction
            self.conn.begin()
            self.cur.arraysize = arraySize
            # the result is not read: the row count below is what this returns
            self.cur.executemany(sql + comment, varMaps)
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
    def getClobObj(self, sql: str, varMap: dict[str, Any], arraySize: int = 10000, use_commit: bool = True) -> tuple[Any, Any]:
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
    def wakeUp(self) -> None:
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
    def transaction(self, name: str | None = None, tmp_log: LogWrapper | None = None) -> Iterator[tuple[Any, LogWrapper]]:
        """
        Context manager for transaction

        Args:
            name (str, optional): name of the transaction to be shown in the log
            tmp_log (LogWrapper, optional): logger to use. If None, a new logger will be created

        Yields:
            Any: the cursor object for executing SQL commands
            Any: the logger object for logging in DBProxy
        """
        comment = " /* DBProxy.transaction */"
        try:
            if tmp_log is None:
                tmp_log = self.create_tagged_logger(comment, tag=name)
            tmp_log.debug("transaction start")
            # begin transaction
            self.conn.begin()
            # cursor and logger for the with block
            yield (self.cur, tmp_log)
            # commit transaction
            if not self._commit():
                raise RuntimeError("commit error")
            tmp_log.debug("transaction done")
        except Exception as e:
            # roll back
            self._rollback()
            self.dump_error_message(tmp_log)
            raise e

    # record status change
    def recordStatusChange(
        self,
        pandaID: int,
        jobStatus: str,
        jobInfo: "JobSpec | None" = None,
        infoMap: dict[str, Any] = {},
        useCommit: bool = True,
        no_late_bulk_exec: bool = True,
        extracted_sqls: dict[str, Any] | None = None,
    ) -> None:
        comment = " /* DBProxy.recordStatusChange */"
        tmp_log = self.create_tagged_logger(comment)
        # check config
        if not hasattr(panda_config, "record_statuschange") or panda_config.record_statuschange is not True:
            return
        # get job info
        varMap: dict[str, Any] = {}
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
            elif extracted_sqls is not None:
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
        job_spec: "JobSpec | None",
        panda_id: int,
        status: str,
        jedi_task_id: int | None = None,
        special_handling: str | None = None,
        extra_data: dict[str, Any] | None = None,
    ) -> None:
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
                error_tmp_dict: dict[str, Any] = {}
                # info from job spec
                if job_spec is not None:
                    # task id
                    if jedi_task_id is None:
                        jedi_task_id = job_spec.jediTaskID  # type: ignore[assignment]  # "NULL" sentinel, see spec_column.py
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
                orig_msg_dict: dict[str, Any] = {
                    "msg_type": "job_status",
                    "jobid": panda_id,
                    "taskid": jedi_task_id,
                    "status": status,
                    "timestamp": now_ts,
                }
                update_msg_dict: dict[str, Any] = {
                    "computingsite": computingsite,
                    "inputs": inputs if inputs else None,
                }
                update_msg_dict.update(error_tmp_dict)  # type: ignore[arg-type]  # "NULL" sentinel, see spec_column.py
                msg_dict = update_msg_dict.copy()
                if extra_data:
                    msg_dict.update(extra_data)
                msg_dict.update(orig_msg_dict)  # type: ignore[arg-type]  # "NULL" sentinel, see spec_column.py
                msg = json.dumps(msg_dict)
                if mb_proxy.got_disconnected:
                    mb_proxy.restart()
                mb_proxy.send(msg)
                tmp_log.debug(f"sent message: {msg}")
            except Exception:
                self.dump_error_message(tmp_log)

    def insert_to_query_pool(self, topic: str, panda_id: int, task_id: int | None, sql: str, var_map: dict[str, Any], exec_order: int) -> None:
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
    def is_no_wait_exception(self, err_value: BaseException) -> bool:
        """
        Return True if the given exception is a lock-not-available error (NOWAIT acquisition failed).

        Recognises Oracle ORA-00054 by inspecting the leading error-code token of the message,
        and Postgres by matching psycopg2.errors.LockNotAvailable via class name (avoids importing
        psycopg2 in code paths that may run against Oracle).

        :param err_value: exception caught in an except clause
        :return: True if the exception represents a NOWAIT lock failure, False otherwise
        """
        # for oracle
        ora_err_code = str(err_value).split()[0]
        ora_err_code = ora_err_code[:-1]
        if ora_err_code == "ORA-00054":
            return True
        # for postgres
        if type(err_value).__name__ == "LockNotAvailable":
            return True
        return False

    def is_unique_violation_exception(self, err_value: BaseException) -> bool:
        """
        Return True if the given exception is a unique-key violation (PK or unique constraint).

        Recognises Oracle ORA-00001 by inspecting the leading error-code token of the message,
        and Postgres by matching psycopg2.errors.UniqueViolation via class name (avoids importing
        psycopg2 in code paths that may run against Oracle).

        :param err_value: exception caught in an except clause
        :return: True if the exception represents a unique-key violation, False otherwise
        """
        # for oracle
        ora_err_code = str(err_value).split()[0]
        ora_err_code = ora_err_code[:-1]
        if ora_err_code == "ORA-00001":
            return True
        # for postgres
        if type(err_value).__name__ == "UniqueViolation":
            return True
        return False

    def is_deadlock_exception(self, err_value: BaseException) -> bool:
        """
        Return True if the given exception is a deadlock error.

        Recognises Oracle ORA-00060 by inspecting the leading error-code token of the message,
        and Postgres by matching psycopg2.errors.DeadlockDetected via class name (avoids importing
        psycopg2 in code paths that may run against Oracle).

        :param err_value: exception caught in an except clause
        :return: True if the exception represents a deadlock, False otherwise
        """
        # for oracle
        ora_err_code = str(err_value).split()[0]
        ora_err_code = ora_err_code[:-1]
        if ora_err_code == "ORA-00060":
            return True
        # for postgres
        if type(err_value).__name__ == "DeadlockDetected":
            return True
        return False

    # set super status
    def setSuperStatus_JEDI(self, jediTaskID: int, superStatus: str) -> bool:
        comment = " /* JediDBProxy.setSuperStatus_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID}")
        retTasks: list[Any] = []
        try:
            # sql to set super status
            sqlCT = f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks "
            sqlCT += "SET superStatus=:superStatus "
            sqlCT += "WHERE jediTaskID=:jediTaskID "
            # set super status
            varMap: dict[str, Any] = {}
            varMap[":jediTaskID"] = jediTaskID
            varMap[":superStatus"] = superStatus
            self.cur.execute(sqlCT + comment, varMap)
            return True
        except Exception:
            # error
            self.dump_error_message(tmpLog)
            return False

    # set DEFT status
    def setDeftStatus_JEDI(self, jediTaskID: int, taskStatus: str) -> bool:
        comment = " /* JediDBProxy.setDeftStatus_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID}")
        try:
            sqlD = f"UPDATE {panda_config.schemaDEFT}.T_TASK "
            sqlD += "SET status=:status,timeStamp=CURRENT_DATE "
            sqlD += "WHERE taskID=:jediTaskID "
            varMap: dict[str, Any] = {}
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
    def record_task_status_change(self, jedi_task_id: int) -> None:
        comment = " /* JediDBProxy.record_task_status_change */"
        tmpLog = self.create_tagged_logger(comment, f"jediTaskID={jedi_task_id}")
        tmpLog.debug("start")
        varMap: dict[str, Any] = dict()
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
    def push_task_status_message(self, task_spec: "JediTaskSpec | None", jedi_task_id: int | None, status: str | None, split_rule: str | None = None) -> None:
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
                if self.jedi_mb_proxy_dict_setter is None:
                    # only a JediDBProxy installs the setter, see set_jedi_attributes()
                    tmpLog.debug("No mb_proxy setter for internal MQs. Skipped ")
                    return
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
    def push_task_trigger_message(
        self,
        msg_type: str,
        jedi_task_id: int | None,
        data_dict: dict[str, Any] | None = None,
        priority: int | None = None,
        task_spec: "JediTaskSpec | None" = None,
    ) -> bool | None:
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
                if self.jedi_mb_proxy_dict_setter is None:
                    # only a JediDBProxy installs the setter, see set_jedi_attributes()
                    tmpLog.debug("No mb_proxy setter for internal MQs. Skipped ")
                    return None
                self.jedi_mb_proxy_dict = self.jedi_mb_proxy_dict_setter()
                if self.jedi_mb_proxy_dict is None:
                    tmpLog.debug("Failed to get mb_proxy of internal MQs. Skipped ")
                    return None
            try:
                mq_name = msg_type
                mb_proxy = self.jedi_mb_proxy_dict["out"][mq_name]
            except KeyError as e:
                tmpLog.warning(f"Skipped due to {e} ; jedi_mb_proxy_dict is {self.jedi_mb_proxy_dict}")
                return None
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
            return None
        tmpLog.debug("done")
        return True
