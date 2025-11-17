import copy
import functools
import json
import os
import random
import re
import socket
import time
import traceback
from collections import namedtuple
from contextlib import contextmanager
from dataclasses import MISSING, InitVar, asdict, dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandautils.base import SpecBase
from pandacommon.pandautils.PandaUtils import get_sql_IN_bind_variables, naive_utcnow

from pandaserver.config import panda_config
from pandaserver.dataservice.ddm import rucioAPI

import polars as pl  # isort:skip

import idds.common.constants  # isort:skip
import idds.common.utils  # isort:skip
from idds.client.client import Client as iDDS_Client  # isort:skip


# main logger
logger = PandaLogger().getLogger(__name__.split(".")[-1])


# ==============================================================

# global DC lock component name
GLOBAL_DC_LOCK_NAME = "DataCarousel"

# schema version of database config
DC_CONFIG_SCHEMA_VERSION = 1

# final task statuses
FINAL_TASK_STATUSES = ["done", "finished", "failed", "exhausted", "aborted", "toabort", "aborting", "broken", "tobroken"]

# named tuple for attribute with type
AttributeWithType = namedtuple("AttributeWithType", ["attribute", "type"])

# DDM rule activity map for data carousel
DDM_RULE_ACTIVITY_MAP = {"anal": "Data Carousel Analysis", "prod": "Data Carousel Production", "_deprecated_": "Staging"}

# template strings
# source replica expression prefix
SRC_REPLI_EXPR_PREFIX = "rse_type=DISK"
# destination replica expression for datasets to pin (with replica on disk but without rule to stay on datadisk)
TO_PIN_DST_REPLI_EXPR = "type=DATADISK"
# source or destination replica expression for available RSEs (exclude downtime)
AVAIL_REPLI_EXPR_SUFFIX = "&availability_write&availability_read"

# DDM rule lifetime in day to keep
STAGING_LIFETIME_DAYS = 15
DONE_LIFETIME_DAYS = 30

# DDM rule refresh hard lifetime limits (refresh only if lifetime within the range)
TO_REFRESH_MAX_LIFETIME_DAYS = 7
TO_REFRESH_MIN_LIFETIME_HOURS = 2

# maximum quota of files for fair share queue before normal queue
QUEUE_FAIR_SHARE_MAX_QUOTA = 10000

# polars config
pl.Config.set_ascii_tables(True)
pl.Config.set_tbl_hide_dataframe_shape(True)
pl.Config.set_tbl_hide_column_data_types(True)
pl.Config.set_tbl_rows(-1)
pl.Config.set_tbl_cols(-1)
pl.Config.set_tbl_width_chars(140)

# ==============================================================


class DataCarouselRequestStatus(object):
    """
    Data carousel request status
    """

    queued = "queued"
    staging = "staging"
    done = "done"
    cancelled = "cancelled"
    retired = "retired"

    active_statuses = [queued, staging]
    final_statuses = [done, cancelled, retired]
    unfinished_statuses = [staging, cancelled]
    reusable_statuses = [queued, staging, done]
    resubmittable_statuses = [staging, cancelled, done, retired]


class DataCarouselRequestSpec(SpecBase):
    """
    Data carousel request specification
    """

    # attributes with types
    attributes_with_types = (
        AttributeWithType("request_id", int),
        AttributeWithType("dataset", str),
        AttributeWithType("source_rse", str),
        AttributeWithType("destination_rse", str),
        AttributeWithType("ddm_rule_id", str),
        AttributeWithType("status", str),
        AttributeWithType("total_files", int),
        AttributeWithType("staged_files", int),
        AttributeWithType("dataset_size", int),
        AttributeWithType("staged_size", int),
        AttributeWithType("creation_time", datetime),
        AttributeWithType("start_time", datetime),
        AttributeWithType("end_time", datetime),
        AttributeWithType("modification_time", datetime),
        AttributeWithType("check_time", datetime),
        AttributeWithType("source_tape", str),
        AttributeWithType("parameters", str),
        AttributeWithType("last_staged_time", datetime),
        AttributeWithType("locked_by", str),
        AttributeWithType("lock_time", datetime),
    )
    # attributes
    attributes = tuple([attr.attribute for attr in attributes_with_types])
    # attributes which have 0 by default
    _zeroAttrs = ()
    # attributes to force update
    _forceUpdateAttrs = ()
    # mapping between sequence and attr
    _seqAttrMap = {"request_id": f"{panda_config.schemaJEDI}.JEDI_DATA_CAROUSEL_REQUEST_ID_SEQ.nextval"}

    @property
    def parameter_map(self) -> dict:
        """
        Get the dictionary parsed by the parameters attribute in JSON
        Possible parameters:
            "reuse_rule" (bool): reuse DDM rule instead of submitting new one
            "resub_from" (int): resubmitted from this oringal request ID
            "prev_src" (str): previous source RSE
            "prev_dst" (str): previous destination RSE
            "excluded_dst_list" (list[str]): list of excluded destination RSEs
            "rule_unfound" (bool): DDM rule not found
            "to_pin" (bool): whether to pin the dataset
            "suggested_dst_list" (list[str]): list of suggested destination RSEs
            "remove_when_done" (bool): remove request and DDM rule asap when request done to save disk space
            "task_id": (int): task_id of the task which initiates the request
            "init_task_gshare": (str): (deprecated) original gshare of the task which initiates the request, for statistics
            "task_gshare": (str): original gshare of the task which initiates the request, for statistics
            "task_type": (str): type of the task (prod, anal) which initiates this request, for statistics
            "task_user": (str): user of the task which initiates this request, for statistics
            "task_group": (str): working group of the task which initiates this request, for statistics

        Returns:
            dict : dict of parameters if it is JSON or empty dict if null
        """
        if self.parameters is None:
            return {}
        else:
            return json.loads(self.parameters)

    @parameter_map.setter
    def parameter_map(self, value_map: dict):
        """
        Set the dictionary and store in parameters attribute in JSON

        Args:
            value_map (dict): dict to set the parameter map
        """
        self.parameters = json.dumps(value_map)

    def get_parameter(self, param: str) -> Any:
        """
        Get the value of one parameter. None as default

        Args:
            param (str): parameter name

        Returns:
            Any : value of the parameter; None if parameter not set
        """
        tmp_dict = self.parameter_map
        return tmp_dict.get(param)

    def set_parameter(self, param: str, value):
        """
        Set the value of one parameter and store in parameters attribute in JSON

        Args:
            param (str): parameter name
            value (Any): value of the parameter to set; must be JSON-serializable
        """
        tmp_dict = self.parameter_map
        tmp_dict[param] = value
        self.parameter_map = tmp_dict

    def update_parameters(self, params: dict):
        """
        Update values of parameters with a dict and store in parameters attribute in JSON

        Args:
            params (dict): dict of parameter names and values to set
        """
        tmp_dict = self.parameter_map
        tmp_dict.update(params)
        self.parameter_map = tmp_dict


class DataCarouselRequestTransaction(object):
    """
    Data Carousel request transaction object
    This is a wrapper for DataCarouselRequestSpec to provide additional methods about DB transaction
    """

    def __init__(self, dc_req_spec: DataCarouselRequestSpec, db_cur, db_log):
        """
        Constructor

        Args:
            dc_req_spec (DataCarouselRequestSpec): request specification
            db_cur: database cursor for DB operations
            db_log: database logger for logging DB operations
        """
        self.spec = dc_req_spec
        self.db_cur = db_cur
        self.db_log = db_log

    def update_spec(self, dc_req_spec: DataCarouselRequestSpec, to_db: bool = True) -> bool | None:
        """
        Update the request specification with a new one

        Args:
            dc_req_spec (DataCarouselRequestSpec): new request specification to update
            to_db (bool): whether to update the request in DB; default True

        Returns:
            bool|None : True if updated in DB successfully, False if failed to update in DB, None if not updating in DB
        """
        if to_db:
            # update the request in DB
            comment = " /* DataCarouselRequestTransaction.update_spec */"
            dc_req_spec.modification_time = naive_utcnow()
            sql_update = (
                f"UPDATE {panda_config.schemaJEDI}.data_carousel_requests " f"SET {dc_req_spec.bindUpdateChangesExpression()} " "WHERE request_id=:request_id "
            )
            var_map = dc_req_spec.valuesMap(useSeq=False, onlyChanged=True)
            var_map[":request_id"] = dc_req_spec.request_id
            self.db_cur.execute(sql_update + comment, var_map)
            if self.db_cur.rowcount == 0:
                # no rows updated; request_id not found
                self.db_log.warning(f"request_id={dc_req_spec.request_id} not updated")
                return False
            else:
                self.spec = dc_req_spec
                self.db_log.debug(f"updated request_id={dc_req_spec.request_id} {dc_req_spec.bindUpdateChangesExpression()}")
                return True
        else:
            self.spec = dc_req_spec
            return None


# ==============================================================
# Dataclasses of configurations #
# ===============================


@dataclass
class SourceTapeConfig:
    """
    Dataclass for source tape configuration parameters

    Fields:
        active                  (bool)  : whether the tape is active
        max_size                (int)   : maximum number of n_files_queued + nfiles_staging from this tape
        max_staging_ratio       (int)   : maximum ratio percent of nfiles_staging / (n_files_queued + nfiles_staging)
        destination_expression  (str)   : rse_expression for DDM to filter the destination RSE
    """

    active: bool = False
    max_size: int = 10000
    max_staging_ratio: int = 50
    destination_expression: str = "type=DATADISK&datapolicynucleus=True&freespace>300"


@dataclass
class SourceRSEConfig:
    """
    Dataclass for source RSE configuration parameters

    Fields:
        tape                    (str)       : is mapped to this source physical tape
        active                  (bool|None) : whether the source RSE is active
        max_size                (int|None)  : maximum number of n_files_queued + nfiles_staging from this RSE
        max_staging_ratio       (int|None)  : maximum ratio percent of nfiles_staging / (n_files_queued + nfiles_staging)
    """

    tape: str
    active: bool | None = None
    max_size: int | None = None
    max_staging_ratio: int | None = None


# Main config; must be bottommost of all config dataclasses
@dataclass
class DataCarouselMainConfig:
    """
    Dataclass for DataCarousel main configuration parameters

    Fields:
        source_tapes_config     (dict)  : configuration of source physical tapes, in form of {"TAPE_1": SourceTapeConfig_of_TAPE_1, ...}
        source_rses_config      (dict)  : configuration of source RSEs, each should be mapped to some source tape, in form of {"RSE_1": SourceRSEConfig_of_RSE_1, ...}
        excluded_destinations   (list)  : excluded destination RSEs
        early_access_users      (list)  : PanDA user names for early access to Data Carousel
    """

    source_tapes_config: Dict[str, Any] = field(default_factory=dict)
    source_rses_config: Dict[str, Any] = field(default_factory=dict)
    excluded_destinations: List[str] = field(default_factory=list)
    early_access_users: List[str] = field(default_factory=list)

    def __post_init__(self):
        # map of the attributes with nested dict and corresponding dataclasses
        converting_attr_type_map = {
            "source_tapes_config": SourceTapeConfig,
            "source_rses_config": SourceRSEConfig,
        }
        # convert the value-dicts of the attributes to corresponding dataclasses
        for attr, klass in converting_attr_type_map.items():
            if isinstance(_map := getattr(self, attr, None), dict):
                converted_dict = {}
                for key, value in _map.items():
                    converted_dict[key] = klass(**value)
                setattr(self, attr, converted_dict)


# ==============================================================
# Functions #
# ===========


def get_resubmit_request_spec(dc_req_spec: DataCarouselRequestSpec, exclude_prev_dst: bool = False) -> DataCarouselRequestSpec | None:
    """
    Get a new request spec to resubmit according to original request spec

    Args:
        dc_req_spec (DataCarouselRequestSpec): oringal spec of the request
        exclude_prev_dst (bool): whether to exclude previous destination

    Returns:
        DataCarouselRequestSpec|None : spec of the request to resubmit, or None if failed
    """
    tmp_log = LogWrapper(logger, f"get_resubmit_request_spec")
    try:
        # make new request spec
        now_time = naive_utcnow()
        dc_req_spec_to_resubmit = DataCarouselRequestSpec()
        # attributes to reset
        dc_req_spec_to_resubmit.staged_files = 0
        dc_req_spec_to_resubmit.staged_size = 0
        dc_req_spec_to_resubmit.status = DataCarouselRequestStatus.queued
        dc_req_spec_to_resubmit.creation_time = now_time
        # get attributes from original request
        # TODO: now copy source_rse and source tape from original; may need to re-choose source in the future
        dc_req_spec_to_resubmit.dataset = dc_req_spec.dataset
        dc_req_spec_to_resubmit.total_files = dc_req_spec.total_files
        dc_req_spec_to_resubmit.dataset_size = dc_req_spec.dataset_size
        dc_req_spec_to_resubmit.source_rse = dc_req_spec.source_rse
        dc_req_spec_to_resubmit.source_tape = dc_req_spec.source_tape
        # parameters according to original requests
        orig_parameter_map = dc_req_spec.parameter_map
        # orig_excluded_dst_set = set(orig_parameter_map.get("excluded_dst_list", []))
        excluded_dst_set = set()
        if exclude_prev_dst:
            # exclude previous destination
            excluded_dst_set.add(dc_req_spec.destination_rse)
        # TODO: mechanism to exclude problematic source or destination RSE (need approach to store historical datasets/RSEs)
        dc_req_spec_to_resubmit.parameter_map = {
            "resub_from": dc_req_spec.request_id,
            "prev_src": dc_req_spec.source_rse,
            "prev_dst": dc_req_spec.destination_rse,
            "excluded_dst_list": list(excluded_dst_set),
            "task_id": orig_parameter_map.get("task_id"),
            "task_gshare": orig_parameter_map.get("task_gshare"),
            "init_task_gshare": orig_parameter_map.get("init_task_gshare"),
        }
        # return
        tmp_log.debug(f"got resubmit request spec for request_id={dc_req_spec.request_id}")
        return dc_req_spec_to_resubmit
    except Exception:
        tmp_log.error(f"got error ; {traceback.format_exc()}")


# ==============================================================


class DataCarouselInterface(object):
    """
    Interface for data carousel methods
    """

    # constructor
    def __init__(self, taskbufferIF, *args, **kwargs):
        # attributes
        self.taskBufferIF = taskbufferIF
        self.ddmIF = rucioAPI
        self.tape_rses = []
        self.datadisk_rses = []
        self.disk_rses = []
        self.dc_config_map = None
        self._last_update_ts_dict = {}
        # full pid
        self.full_pid = f"{socket.getfqdn().split('.')[0]}-{os.getpgrp()}-{os.getpid()}"
        # refresh
        self._refresh_all_attributes()

    def _refresh_all_attributes(self):
        """
        Refresh by calling all update methods
        """
        self._update_rses(time_limit_minutes=30)
        self._update_dc_config(time_limit_minutes=5)

    @staticmethod
    def refresh(func):
        """
        Decorator to call _refresh_all_attributes before the method
        """

        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            self._refresh_all_attributes()
            return func(self, *args, **kwargs)

        return wrapper

    def _acquire_global_dc_lock(self, timeout_sec: int = 10, lock_expiration_sec: int = 30) -> str | None:
        """
        Acquire global Data Carousel lock in DB

        Args:
            timeout_sec (int): timeout in seconds for blocking to retry
            lock_expiration_sec (int): age of lock in seconds to be considered expired and can be acquired immediately

        Returns:
            str|None : full process ID of this process if got lock; None if timeout
        """
        tmp_log = LogWrapper(logger, f"_acquire_global_dc_lock pid={self.full_pid}")
        # time the retry loop
        start_mono_time = time.monotonic()
        while time.monotonic() - start_mono_time <= timeout_sec:
            # try to get the lock
            got_lock = self.taskBufferIF.lockProcess_PANDA(
                component=GLOBAL_DC_LOCK_NAME,
                pid=self.full_pid,
                time_limit=lock_expiration_sec / 60.0,
            )
            if got_lock:
                # got the lock; return
                tmp_log.debug(f"got lock")
                return self.full_pid
            else:
                # did not get lock; retry
                time.sleep(0.05)
        # timeout
        tmp_log.debug(f"timed out; skipped")
        return None

    def _release_global_dc_lock(self, full_pid: str | None):
        """
        Release global Data Carousel lock in DB

        Args:
            full_pid (str|None): full process ID which acquired the lock; if None, use self.full_pid
        """
        tmp_log = LogWrapper(logger, f"_release_global_dc_lock pid={full_pid}")
        # get full_pid
        if full_pid is None:
            full_pid = self.full_pid
        # try to release the lock
        ret = self.taskBufferIF.unlockProcess_PANDA(
            component=GLOBAL_DC_LOCK_NAME,
            pid=full_pid,
        )
        if ret:
            # released the lock
            tmp_log.debug(f"released lock")
        else:
            # failed to release lock; skip
            tmp_log.error(f"failed to released lock; skipped")
        return ret

    @contextmanager
    def global_dc_lock(self, timeout_sec: int = 10, lock_expiration_sec: int = 30):
        """
        Context manager for global Data Carousel lock in DB

        Args:
            timeout_sec (int): timeout in seconds for blocking to retry
            lock_expiration_sec (int): age of lock in seconds to be considered expired and can be acquired immediately

        Yields:
            str : full process ID of this process if got lock; None if timeout
        """
        # tmp_log = LogWrapper(logger, f"global_dc_lock pid={self.full_pid}")
        # try to release the lock
        full_pid = self._acquire_global_dc_lock(timeout_sec, lock_expiration_sec)
        try:
            yield full_pid
        finally:
            self._release_global_dc_lock(full_pid)

    def get_request_by_id(self, request_id: int) -> DataCarouselRequestSpec | None:
        """
        Get the spec of the request specified by request_id

        Args:
            request_id (int): request_id of the request

        Returns:
            DataCarouselRequestSpec|None : spec of the request, or None if failed
        """
        tmp_log = LogWrapper(logger, f"get_request_by_id request_id={request_id}")
        sql = f"SELECT {DataCarouselRequestSpec.columnNames()} " f"FROM {panda_config.schemaJEDI}.data_carousel_requests " f"WHERE request_id=:request_id "
        var_map = {":request_id": request_id}
        res_list = self.taskBufferIF.querySQL(sql, var_map, arraySize=99999)
        if res_list is not None:
            if len(res_list) > 1:
                tmp_log.error("more than one requests; unexpected")
            else:
                for res in res_list:
                    dc_req_spec = DataCarouselRequestSpec()
                    dc_req_spec.pack(res)
                    return dc_req_spec
        else:
            tmp_log.warning("no request found; skipped")
            return None

    def get_request_by_dataset(self, dataset: str) -> DataCarouselRequestSpec | None:
        """
        Get the reusable request (not in cancelled or retired) of the dataset

        Args:
            dataset (str): dataset name

        Returns:
            DataCarouselRequestSpec|None : spec of the request of dataset, or None if failed
        """
        tmp_log = LogWrapper(logger, f"get_request_by_dataset dataset={dataset}")
        status_var_names_str, status_var_map = get_sql_IN_bind_variables(DataCarouselRequestStatus.reusable_statuses, prefix=":status")
        sql = (
            f"SELECT {DataCarouselRequestSpec.columnNames()} "
            f"FROM {panda_config.schemaJEDI}.data_carousel_requests "
            f"WHERE dataset=:dataset "
            f"AND status IN ({status_var_names_str}) "
        )
        var_map = {":dataset": dataset}
        var_map.update(status_var_map)
        res_list = self.taskBufferIF.querySQL(sql, var_map, arraySize=99999)
        if res_list is not None:
            if len(res_list) > 1:
                tmp_log.error("more than one reusable requests; unexpected")
            else:
                for res in res_list:
                    dc_req_spec = DataCarouselRequestSpec()
                    dc_req_spec.pack(res)
                    return dc_req_spec
        else:
            tmp_log.warning("no reusable request; skipped")
            return None

    # @contextmanager
    # def request_transaction_by_id(self, request_id: int):
    #     """
    #     Context manager to get a transaction for the Data Carousel request by request_id

    #     Args:
    #         request_id (int): request ID of the Data Carousel request

    #     Yields:
    #         DataCarouselRequestTransaction : request object specified by request_id
    #     """
    #     tmp_log = LogWrapper(logger, f"request_transaction_by_id pid={self.full_pid} request_id={request_id}")
    #     with self.taskBufferIF.transaction(name="DataCarouselRequestTransaction") as (db_cur, db_log):
    #         # try to get the request
    #         dc_req_spec = None
    #         sql_get = (
    #             f"SELECT {DataCarouselRequestSpec.columnNames()} " f"FROM {panda_config.schemaJEDI}.data_carousel_requests " f"WHERE request_id=:request_id "
    #         )
    #         var_map = {":request_id": request_id}
    #         res_list = db_cur.execute(sql_get, var_map).fetchall()
    #         if res_list is not None:
    #             if len(res_list) > 1:
    #                 tmp_log.error("more than one requests; unexpected")
    #             else:
    #                 for res in res_list:
    #                     dc_req_spec = DataCarouselRequestSpec()
    #                     dc_req_spec.pack(res)
    #         dc_req_txn = DataCarouselRequestTransaction(dc_req_spec, db_cur, db_log)
    #         # yield and run wrapped function
    #         yield dc_req_txn

    # @contextmanager
    # def request_lock_transaction_by_id(self, request_id: int, lock_expiration_sec: int = 120):
    #     """
    #     Context manager to lock and get a transaction for the Data Carousel request for update into DB

    #     Args:
    #         request_id (int): request ID of the Data Carousel request to acquire lock
    #         lock_expiration_sec (int): age of lock in seconds to be considered expired and can be acquired immediately

    #     Yields:
    #         DataCarouselRequestTransaction | None : request object specified by request_id if got lock; None if did not get lock
    #     """
    #     tmp_log = LogWrapper(logger, f"lock_request_for_update pid={self.full_pid} request_id={request_id}")
    #     #
    #     got_lock = False
    #     try:
    #         with self.taskBufferIF.transaction(name="DataCarouselRequestLock") as (db_cur, db_log):
    #             # try to get the lock
    #             sql_lock = (
    #                 f"SELECT request_id FROM {panda_config.schemaJEDI}.data_carousel_requests "
    #                 f"WHERE request_id=:request_id "
    #                 f"AND (locked_by IS NULL OR locked_by=:locked_by OR lock_time < :min_lock_time) "
    #             )
    #             var_map = {
    #                 ":request_id": request_id,
    #                 ":locked_by": self.full_pid,
    #                 ":min_lock_time": naive_utcnow() - timedelta(seconds=lock_expiration_sec),
    #             }
    #             res_list = db_cur.execute(sql_lock, var_map).fetchall()
    #             if res_list is not None:
    #                 if len(res_list) > 1:
    #                     tmp_log.error("more than one requests; unexpected")
    #                 elif len(res_list) == 0:
    #                     # no rows found; did not get the lock
    #                     db_log.debug(f"{self.full_pid} did not get lock for request_id={request_id}")
    #                 else:
    #                     # got the lock; update locked_by and lock_time
    #                     sql_update = (
    #                         f"UPDATE {panda_config.schemaJEDI}.data_carousel_requests "
    #                         f"SET locked_by=:locked_by, lock_time=:lock_time "
    #                         f"WHERE request_id=:request_id "
    #                     )
    #                     var_map = {
    #                         ":locked_by": self.full_pid,
    #                         ":lock_time": naive_utcnow(),
    #                         ":request_id": request_id,
    #                     }
    #                     db_cur.execute(sql_update, var_map)
    #                     got_lock = True
    #                     db_log.debug(f"{self.full_pid} got lock for request_id={request_id}")
    #             else:
    #                 # did not get the lock
    #                 db_log.debug(f"{self.full_pid} did not get lock for request_id={request_id}")
    #         if got_lock:
    #             # got the lock
    #             tmp_log.debug(f"got lock")
    #             with self.request_transaction_by_id(request_id) as dc_req_txn:
    #                 # check if request spec is None
    #                 if dc_req_txn.spec is None:
    #                     tmp_log.error(f"request_id={request_id} not found; skipped")
    #                     yield None
    #                 else:
    #                     # yield and let wrapped function run
    #                     yield dc_req_txn
    #         else:
    #             # did not get the lock
    #             tmp_log.debug(f"did not get lock for request_id={request_id}")
    #             yield None
    #     finally:
    #         if got_lock:
    #             # release the lock
    #             with self.taskBufferIF.transaction(name="DataCarouselRequestUnlock") as (db_cur, db_log):
    #                 sql_unlock = (
    #                     f"UPDATE {panda_config.schemaJEDI}.data_carousel_requests "
    #                     f"SET locked_by=NULL, lock_time=NULL "
    #                     f"WHERE request_id=:request_id AND locked_by=:locked_by "
    #                 )
    #                 var_map = {
    #                     ":request_id": request_id,
    #                     ":locked_by": self.full_pid,
    #                 }
    #                 db_cur.execute(sql_unlock, var_map)
    #                 db_log.debug(f"{self.full_pid} released lock for request_id={request_id}")

    @contextmanager
    def request_lock(self, request_id: int, lock_expiration_sec: int = 120):
        """
        Context manager to lock and unlock the Data Carousel request for update into DB

        Args:
            request_id (int): request ID of the Data Carousel request to acquire lock
            lock_expiration_sec (int): age of lock in seconds to be considered expired and can be acquired immediately

        Yields:
            DataCarouselRequestSpec|None : spec of the request if got lock; None if exception or failed
        """
        tmp_log = LogWrapper(logger, f"request_lock pid={self.full_pid} request_id={request_id}")
        #
        got_lock = None
        locked_spec = None
        try:
            # try to update locked_by and lock_time
            sql_lock = (
                f"UPDATE {panda_config.schemaJEDI}.data_carousel_requests "
                f"SET locked_by=:locked_by, lock_time=:lock_time "
                f"WHERE request_id=:request_id "
                f"AND (locked_by IS NULL OR locked_by=:locked_by OR lock_time < :min_lock_time) "
            )
            var_map = {
                ":locked_by": self.full_pid,
                ":lock_time": naive_utcnow(),
                ":request_id": request_id,
                ":min_lock_time": naive_utcnow() - timedelta(seconds=lock_expiration_sec),
            }
            row_count = self.taskBufferIF.querySQL(sql_lock, var_map)
            if row_count is None:
                tmp_log.error(f"failed to update DB to lock; skipped")
            elif row_count > 1:
                tmp_log.error(f"more than one requests updated to lock; unexpected")
            elif row_count == 0:
                # no row updated; did not get the lock
                got_lock = False
                tmp_log.debug(f"did not get lock; skipped")
            else:
                # got the lock
                got_lock = True
                tmp_log.debug(f"got lock")
            # get the request spec locked
            locked_spec = self.get_request_by_id(request_id)
            # yield and run wrapped function
            yield locked_spec
        finally:
            if got_lock:
                # release the lock
                sql_unlock = (
                    f"UPDATE {panda_config.schemaJEDI}.data_carousel_requests "
                    f"SET locked_by=NULL, lock_time=NULL "
                    f"WHERE request_id=:request_id AND locked_by=:locked_by "
                )
                var_map = {
                    ":request_id": request_id,
                    ":locked_by": self.full_pid,
                }
                row_count = self.taskBufferIF.querySQL(sql_unlock, var_map)
                if row_count is None:
                    tmp_log.error(f"failed to update DB to unlock; skipped")
                elif row_count > 1:
                    tmp_log.error(f"more than one requests updated to unlock; unexpected")
                elif row_count == 0:
                    tmp_log.error(f"no request updated to unlock; skipped")
                else:
                    tmp_log.debug(f"released lock")

    def _update_rses(self, time_limit_minutes: int | float = 30):
        """
        Update RSEs per TAPE and DATADISK cached in this object
        Run if cache outdated; else do nothing
        Check both DATADISK and DISK (including other DISK sources like SCRATCHDISK or LOCALDISK which are transient)

        Args:
            time_limit_minutes (int|float): time limit of the cache in minutes
        """
        tmp_log = LogWrapper(logger, "_update_rses")
        nickname = "rses"
        try:
            # check last update
            now_time = naive_utcnow()
            self._last_update_ts_dict.setdefault(nickname, None)
            last_update_ts = self._last_update_ts_dict[nickname]
            if last_update_ts is None or (now_time - last_update_ts) >= timedelta(minutes=time_limit_minutes):
                # get RSEs from DDM
                tape_rses = self.ddmIF.list_rses("rse_type=TAPE")
                if tape_rses is not None:
                    self.tape_rses = list(tape_rses)
                datadisk_rses = self.ddmIF.list_rses(f"type=DATADISK{AVAIL_REPLI_EXPR_SUFFIX}")
                if datadisk_rses is not None:
                    self.datadisk_rses = list(datadisk_rses)
                disk_rses = self.ddmIF.list_rses(f"rse_type=DISK{AVAIL_REPLI_EXPR_SUFFIX}")
                if disk_rses is not None:
                    self.disk_rses = list(disk_rses)
                # tmp_log.debug(f"TAPE: {self.tape_rses} ; DATADISK: {self.datadisk_rses} ; DISK: {self.disk_rses}")
                # tmp_log.debug(f"got {len(self.tape_rses)} tapes , {len(self.datadisk_rses)} datadisks , {len(self.disk_rses)} disks")
                # update last update timestamp
                self._last_update_ts_dict[nickname] = naive_utcnow()
        except Exception:
            tmp_log.error(f"got error ; {traceback.format_exc()}")

    def _update_dc_config(self, time_limit_minutes: int | float = 5):
        """
        Update Data Carousel configuration from DB
        Run if cache outdated; else do nothing

        Args:
            time_limit_minutes (int|float): time limit of the cache in minutes
        """
        tmp_log = LogWrapper(logger, "_update_dc_config")
        nickname = "main"
        try:
            # check last update
            now_time = naive_utcnow()
            self._last_update_ts_dict.setdefault(nickname, None)
            last_update_ts = self._last_update_ts_dict[nickname]
            if last_update_ts is None or (now_time - last_update_ts) >= timedelta(minutes=time_limit_minutes):
                # get DC config from DB
                res_dict = self.taskBufferIF.getConfigValue("data_carousel", f"DATA_CAROUSEL_CONFIG", "jedi", "atlas")
                if res_dict is None:
                    tmp_log.warning(f"got None from DB ; skipped")
                    return
                # check schema version
                try:
                    schema_version = res_dict["metadata"]["schema_version"]
                except KeyError:
                    tmp_log.error(f"failed to get metadata.schema_version ; skipped")
                    return
                else:
                    if schema_version != DC_CONFIG_SCHEMA_VERSION:
                        tmp_log.error(f"metadata.schema_version does not match ({schema_version} != {DC_CONFIG_SCHEMA_VERSION}); skipped")
                        return
                # get config data
                dc_config_data_dict = res_dict.get("data")
                if dc_config_data_dict is None:
                    tmp_log.error(f"got empty config data; skipped")
                    return
                # update
                self.dc_config_map = DataCarouselMainConfig(**dc_config_data_dict)
                # update last update timestamp
                self._last_update_ts_dict[nickname] = naive_utcnow()
        except Exception:
            tmp_log.error(f"got error ; {traceback.format_exc()}")

    def _get_related_tasks(self, request_id: int) -> list[int] | None:
        """
        Get all related tasks to the give request

        Args:
            request_id (int): request_id of the request

        Returns:
            list[int]|None : list of jediTaskID of related tasks, or None if failed
        """
        # tmp_log = LogWrapper(logger, f"_get_related_tasks request_id={request_id}")
        sql = f"SELECT task_id " f"FROM {panda_config.schemaJEDI}.data_carousel_relations " f"WHERE request_id=:request_id " f"ORDER BY task_id "
        var_map = {":request_id": request_id}
        res = self.taskBufferIF.querySQL(sql, var_map, arraySize=99999)
        if res is not None:
            ret_list = [x[0] for x in res]
            return ret_list
        else:
            return None

    def _get_input_ds_from_task_params(self, task_params_map: dict) -> dict:
        """
        Get input datasets from tasks parameters

        Args:
            task_params_map (dict): task parameter map

        Returns:
            dict : map with elements in form of datasets: jobParameters
        """
        ret_map = {}
        for job_param in task_params_map.get("jobParameters", []):
            if job_param.get("param_type") in ["input", "pseudo_input"]:
                # dataset names can be comma-separated
                raw_dataset_str = job_param.get("dataset")
                jobparam_dataset_list = []
                if raw_dataset_str:
                    jobparam_dataset_list = raw_dataset_str.split(",")
                for dataset in jobparam_dataset_list:
                    ret_map[dataset] = job_param
        return ret_map

    def _get_full_replicas_per_type(self, dataset: str) -> dict:
        """
        Get full replicas per type of a dataset

        Args:
            dataset (str): dataset name

        Returns:
            dict : map in form of datasets: jobParameters
        """
        # tmp_log = LogWrapper(logger, f"_get_full_replicas_per_type dataset={dataset}")
        ds_repli_dict = self.ddmIF.convert_list_dataset_replicas(dataset, use_file_lookup=True, skip_incomplete_element=True)
        tape_replicas = []
        datadisk_replicas = []
        disk_replicas = []
        for rse in ds_repli_dict:
            if rse in self.tape_rses:
                tape_replicas.append(rse)
            if rse in self.datadisk_rses:
                datadisk_replicas.append(rse)
            if rse in self.disk_rses:
                disk_replicas.append(rse)
        # return
        ret = {"tape": tape_replicas, "datadisk": datadisk_replicas, "disk": disk_replicas}
        # tmp_log.debug(f"{ret}")
        return ret

    def _get_filtered_replicas(self, dataset: str) -> tuple[dict, (str | None), bool]:
        """
        Get filtered replicas of a dataset and the staging rule and whether all replicas are without rules

        Args:
            dataset (str): dataset name

        Returns:
            dict : filtered replicas map (rules considered)
            str | None : staging rule, None if not existing
            bool : whether all replicas on datadisk are without rules
            dict : original replicas map
        """
        replicas_map = self._get_full_replicas_per_type(dataset)
        rules = self.ddmIF.list_did_rules(dataset, all_accounts=True)
        rse_expression_list = []
        staging_rule = None
        for rule in rules:
            if rule["account"] in ["panda"] and rule["activity"] in DDM_RULE_ACTIVITY_MAP.values():
                # rule of the dataset from ProdSys or PanDA already exists; reuse it
                staging_rule = rule
            else:
                rse_expression_list.append(rule["rse_expression"])
        filtered_replicas_map = {"tape": [], "datadisk": []}
        has_datadisk_replica = len(replicas_map["datadisk"]) > 0
        has_disk_replica = len(replicas_map["disk"]) > 0
        for replica in replicas_map["tape"]:
            if replica in rse_expression_list:
                filtered_replicas_map["tape"].append(replica)
        if len(replicas_map["tape"]) >= 1 and len(filtered_replicas_map["tape"]) == 0 and len(rules) == 0:
            filtered_replicas_map["tape"] = replicas_map["tape"]
        for replica in replicas_map["datadisk"]:
            if staging_rule is not None or replica in rse_expression_list:
                filtered_replicas_map["datadisk"].append(replica)
        all_disk_repli_ruleless = has_disk_replica and len(filtered_replicas_map["datadisk"]) == 0
        return filtered_replicas_map, staging_rule, all_disk_repli_ruleless, replicas_map

    def _get_datasets_from_collection(self, collection: str) -> list[str] | None:
        """
        Get a list of datasets from DDM collection (container or dataset) in order to support inputs of container containing multiple datasets
        If the collection is a dataset, the method returns a list of the sole dataset
        If the collection is a container, the method returns a list of datasets inside the container

        Args:
            collection (str): name of the DDM collection (container or dataset)

        Returns:
            list[str] | None : list of datasets if successful; None if failed with exception
        """
        tmp_log = LogWrapper(logger, f"_get_datasets_from_collections collection={collection}")
        try:
            # assure the DID format of the collection to be scope:dataset_name
            collection = self.ddmIF.get_did_str(collection)
            tmp_log = LogWrapper(logger, f"_get_datasets_from_collections collection={collection}")
            # check the collection
            ret_list = []
            collection_meta = self.ddmIF.get_dataset_metadata(collection, ignore_missing=True)
            if collection_meta is None:
                # collection metadata not found
                tmp_log.warning(f"collection metadata not found")
                return None
            elif collection_meta["state"] == "missing":
                # DID not found
                tmp_log.warning(f"DID not found")
                return None
            did_type = collection_meta["did_type"]
            if did_type == "CONTAINER":
                # is container, get datasets inside
                jobparam_dataset_list = self.ddmIF.list_datasets_in_container_JEDI(collection)
                if jobparam_dataset_list is None:
                    tmp_log.warning(f"cannot list datasets in this container")
                else:
                    ret_list = jobparam_dataset_list
            elif did_type == "DATASET":
                # is dataset
                ret_list = [collection]
            else:
                tmp_log.warning(f"invalid DID type: {did_type}")
                return None
        except Exception:
            tmp_log.error(f"got error ; {traceback.format_exc()}")
            return None
        return ret_list

    def _get_active_source_tapes(self) -> set[str] | None:
        """
        Get the set of active source physical tapes according to DC config

        Returns:
            set[str] | None : set of source tapes if successful; None if failed with exception
        """
        tmp_log = LogWrapper(logger, f"_get_active_source_tapes")
        try:
            active_source_tapes_set = {tape for tape, tape_config in self.dc_config_map.source_tapes_config.items() if tape_config.active}
        except Exception:
            # other unexpected errors
            tmp_log.error(f"got error ; {traceback.format_exc()}")
            return None
        else:
            return active_source_tapes_set

    def _get_active_source_rses(self) -> set[str] | None:
        """
        Get the set of active source RSEs according to DC config

        Returns:
            set[str] | None : set of source RSEs if successful; None if failed with exception
        """
        tmp_log = LogWrapper(logger, f"_get_active_source_tapes")
        try:
            active_source_tapes = self._get_active_source_tapes()
            active_source_rses_set = set()
            for rse, rse_config in self.dc_config_map.source_rses_config.items():
                try:
                    # both physical tape and RSE are active
                    if rse_config.tape in active_source_tapes and rse_config.active is not False:
                        active_source_rses_set.add(rse)
                except Exception:
                    # errors with the rse
                    tmp_log.error(f"got error with {rse} ; {traceback.format_exc()}")
                    continue
        except Exception:
            # other unexpected errors
            tmp_log.error(f"got error ; {traceback.format_exc()}")
            return None
        else:
            return active_source_rses_set

    def _get_source_type_of_dataset(
        self, dataset: str, active_source_rses_set: set | None = None
    ) -> tuple[(str | None), (set | None), (str | None), bool, list]:
        """
        Get source type and permanent (tape or datadisk) RSEs of a dataset

        Args:
            dataset (str): dataset name
            active_source_rses_set (set | None): active source RSE set to reuse. If None, will get a new one in the method

        Returns:
            str | None : source type of the dataset, "datadisk" if replica on any datadisk, "tape" if replica only on tapes, None if not found
            set | None : set of permanent RSEs, otherwise None
            str | None : staging rule if existing, otherwise None
            bool : whether to pin the dataset
            list : list of suggested destination RSEs; currently only datadisks with full replicas to pin
        """
        tmp_log = LogWrapper(logger, f"_get_source_type_of_dataset dataset={dataset}")
        try:
            # initialize
            source_type = None
            rse_set = None
            to_pin = False
            suggested_destination_rses_set = set()
            # get active source rses
            if active_source_rses_set is None:
                active_source_rses_set = self._get_active_source_rses()
            # get filtered replicas and staging rule of the dataset
            filtered_replicas_map, staging_rule, all_disk_repli_ruleless, orig_replicas_map = self._get_filtered_replicas(dataset)
            # algorithm
            if filtered_replicas_map["datadisk"]:
                # replicas already on datadisk and with rule
                source_type = "datadisk"
                # source datadisk RSEs from DDM
                rse_set = {replica for replica in filtered_replicas_map["datadisk"]}
            elif filtered_replicas_map["tape"]:
                # replicas on tape and without rule to pin it on datadisk
                source_type = "tape"
                # source tape RSEs from DDM
                rse_set = {replica for replica in filtered_replicas_map["tape"]}
                # filter out inactive source tape RSEs according to DC config
                if active_source_rses_set is not None:
                    rse_set &= active_source_rses_set
                # condiser unfound if no active source tape
                if not rse_set:
                    source_type = None
                    tmp_log.warning(f"all its source tapes are inactive")
                # dataset pinning
                if all_disk_repli_ruleless:
                    # replica on disks but without rule to pin on datadisk; to pin the dataset to datadisk
                    to_pin = True
                    suggested_destination_rses_set |= set(orig_replicas_map["datadisk"])
            else:
                # no replica found on tape nor on datadisk (can be on transient disk); skip
                pass
            # return
            tmp_log.debug(
                f"source_type={source_type} rse_set={rse_set} staging_rule={staging_rule} to_pin={to_pin} "
                f"suggested_destination_rses_set={suggested_destination_rses_set}"
            )
            return (source_type, rse_set, staging_rule, to_pin, list(suggested_destination_rses_set))
        except Exception as e:
            # other unexpected errors
            raise e

    def _choose_tape_source_rse(self, dataset: str, rse_set: set, staging_rule, no_cern: bool = True) -> tuple[str, (str | None), (str | None)]:
        """
        Choose a TAPE source RSE
        If with exsiting staging rule, then get source RSE from it

        Args:
            dataset (str): dataset name
            rse_set (set): set of TAPE source RSE set to choose from
            staging_rule: DDM staging rule
            no_cern: skip CERN-PROD RSE whenever possible if True

        Returns:
            str: the dataset name
            str | None : source RSE found or chosen, None if failed
            str | None : DDM rule ID of staging rule if existing, otherwise None
        """
        tmp_log = LogWrapper(logger, f"_choose_tape_source_rse dataset={dataset}")
        try:
            # initialize
            ddm_rule_id = None
            source_rse = None
            random_choose = False
            # whether with existing staging rule
            if staging_rule:
                # with existing staging rule ; prepare to reuse it
                ddm_rule_id = staging_rule["id"]
                # extract source RSE from rule
                source_replica_expression = staging_rule["source_replica_expression"]
                if source_replica_expression is not None:
                    for rse in rse_set:
                        # match tape rses with source_replica_expression
                        tmp_match = re.search(rse, source_replica_expression)
                        if tmp_match is not None:
                            source_rse = rse
                            break
                    if source_rse is None:
                        # direct regex search from source_replica_expression; reluctant as source_replica_expression can be messy
                        tmp_match = re.search(rf"{SRC_REPLI_EXPR_PREFIX}\|([A-Za-z0-9-_]+)", source_replica_expression)
                        if tmp_match is not None:
                            source_rse = tmp_match.group(1)
                    if source_rse is None:
                        # still not getting source RSE from rule; unexpected
                        tmp_log.error(f"ddm_rule_id={ddm_rule_id} cannot get source_rse from source_replica_expression: {source_replica_expression}")
                    else:
                        tmp_log.debug(f"already staging with ddm_rule_id={ddm_rule_id} source_rse={source_rse}")
                else:
                    # no source_replica_expression of the rule; choose any source
                    tmp_log.warning(f"already staging with ddm_rule_id={ddm_rule_id} without source_replica_expression; to choose a random source_rse")
                    random_choose = True
                # keep alive the rule
                if (rule_expiration_time := staging_rule["expires_at"]) and (rule_expiration_time - naive_utcnow()) < timedelta(days=DONE_LIFETIME_DAYS):
                    self._refresh_ddm_rule(ddm_rule_id, 86400 * DONE_LIFETIME_DAYS)
                    tmp_log.debug(f"ddm_rule_id={ddm_rule_id} refreshed rule to be {DONE_LIFETIME_DAYS} days long")
            else:
                # no existing staging rule; to choose randomly
                random_choose = True
            if random_choose:
                # no existing staging rule or cannot get from source_replica_expression; choose source_rse randomly
                rse_list = list(rse_set)
                # choose source RSE
                if len(rse_list) == 1:
                    source_rse = rse_list[0]
                else:
                    non_CERN_rse_list = [rse for rse in rse_list if "CERN-PROD" not in rse]
                    if non_CERN_rse_list and no_cern:
                        # choose non-CERN-PROD source RSE
                        source_rse = random.choice(non_CERN_rse_list)
                    else:
                        source_rse = random.choice(rse_list)
                tmp_log.debug(f"chose source_rse={source_rse}")
            # add to prestage
            return (dataset, source_rse, ddm_rule_id)
        except Exception as e:
            # other unexpected errors
            raise e

    def _get_source_tape_from_rse(self, source_rse: str) -> str:
        """
        Get the source tape of a source RSE

        Args:
            source_rse (str): name of the source RSE

        Returns:
            str : source tape
        """
        try:
            # source_rse is RSE
            source_tape = self.dc_config_map.source_rses_config[source_rse].tape
        except KeyError:
            # source_rse is physical tape
            source_tape = source_rse
        return source_tape

    @refresh
    def get_input_datasets_to_prestage(self, task_id: int, task_params_map: dict, dsname_list: list | None = None) -> tuple[list, dict]:
        """
        Get the input datasets, their source RSEs (tape) of the task which need pre-staging from tapes, and DDM rule ID of existing DDM rule

        Args:
            task_id (int): JEDI task ID of the task params
            task_params_map (dict): task params of the JEDI task
            dsname_list (list|None): if not None, filter only datasets in this list of dataset names to stage, may also including extra datasets if the task is resubmitted/rerefined

        Returns:
            list[tuple[str, str|None, str|None]]: list of tuples in the form of (dataset, source_rse, ddm_rule_id)
            dict[str|list]: dict of list of datasets, including pseudo inputs (meant to be marked as no_staging), already on datadisk (meant to be marked as no_staging), only on tape, and not found
        """
        tmp_log = LogWrapper(logger, f"get_input_datasets_to_prestage task_id={task_id}")
        try:
            # initialize
            all_input_datasets_set = set()
            jobparam_ds_coll_map = {}
            extra_datasets_set = set()
            raw_coll_list = []
            raw_coll_did_list = []
            coll_on_tape_set = set()
            ret_prestaging_list = []
            ret_map = {
                "pseudo_coll_list": [],
                "unfound_coll_list": [],
                "empty_coll_list": [],
                "tape_coll_did_list": [],
                "no_tape_coll_did_list": [],
                "to_skip_ds_list": [],
                "tape_ds_list": [],
                "datadisk_ds_list": [],
                "to_pin_ds_list": [],
                "unfound_ds_list": [],
            }
            # get active source rses
            active_source_rses_set = self._get_active_source_rses()
            # loop over inputs defined in task's job parameters
            input_collection_map = self._get_input_ds_from_task_params(task_params_map)
            for collection, job_param in input_collection_map.items():
                # pseudo inputs
                if job_param.get("param_type") == "pseudo_input":
                    ret_map["pseudo_coll_list"].append(collection)
                    tmp_log.debug(f"collection={collection} is pseudo input ; skipped")
                    continue
                # with real inputs
                raw_coll_list.append(collection)
                raw_coll_did_list.append(self.ddmIF.get_did_str(collection))
                jobparam_dataset_list = self._get_datasets_from_collection(collection)
                if jobparam_dataset_list is None:
                    ret_map["unfound_coll_list"].append(collection)
                    tmp_log.warning(f"collection={collection} not found")
                    continue
                elif not jobparam_dataset_list:
                    ret_map["empty_coll_list"].append(collection)
                    tmp_log.warning(f"collection={collection} is empty")
                    continue
                # with contents to consider
                for dataset in jobparam_dataset_list:
                    jobparam_ds_coll_map[dataset] = collection
            # merge of jobparam_dataset_list and dnsname_list
            jobparam_datasets_set = set(jobparam_ds_coll_map.keys())
            all_input_datasets_set |= jobparam_datasets_set
            if dsname_list is not None:
                # dsname_list is given; filter out extra container slash
                master_datasets_set = set([dsname for dsname in dsname_list if not dsname.endswith("/")])
                # extra dataset not in job parameters when task resubmitted/rerefined
                extra_datasets_set = master_datasets_set - jobparam_datasets_set - set(raw_coll_did_list)
                all_input_datasets_set |= extra_datasets_set
            all_input_datasets_list = sorted(list(all_input_datasets_set))
            if extra_datasets_set:
                tmp_log.debug(f"datasets appended for incexec: {sorted(list(extra_datasets_set))}")
            # check source of each dataset
            for dataset in all_input_datasets_list:
                # check if dataset in the required dsname_list
                if dsname_list is not None and dataset not in dsname_list:
                    # not in dsname_list; skip
                    ret_map["to_skip_ds_list"].append(dataset)
                    tmp_log.debug(f"dataset={dataset} not in dsname_list ; skipped")
                    continue
                # get source type and RSEs
                source_type, rse_set, staging_rule, to_pin, suggested_dst_list = self._get_source_type_of_dataset(dataset, active_source_rses_set)
                if source_type == "datadisk":
                    # replicas already on datadisk; skip
                    ret_map["datadisk_ds_list"].append(dataset)
                    tmp_log.debug(f"dataset={dataset} already has replica on datadisks {rse_set} ; skipped")
                    continue
                elif source_type == "tape":
                    # replicas only on tape
                    ret_map["tape_ds_list"].append(dataset)
                    tmp_log.debug(f"dataset={dataset} on tapes {rse_set} ; choosing one")
                    if collection := jobparam_ds_coll_map.get(dataset):
                        coll_on_tape_set.add(collection)
                    # choose source RSE
                    _, source_rse, ddm_rule_id = self._choose_tape_source_rse(dataset, rse_set, staging_rule)
                    prestaging_tuple = (dataset, source_rse, ddm_rule_id, to_pin, suggested_dst_list)
                    tmp_log.debug(f"got prestaging: {prestaging_tuple}")
                    # add to prestage
                    ret_prestaging_list.append(prestaging_tuple)
                    # dataset to pin
                    if to_pin:
                        ret_map["to_pin_ds_list"].append(dataset)
                else:
                    # no replica found on tape nor on datadisk; skip
                    ret_map["unfound_ds_list"].append(dataset)
                    tmp_log.debug(f"dataset={dataset} has no replica on any tape or datadisk ; skipped")
                    continue
            # collection DID without datasets on tape
            for collection in raw_coll_list:
                collection_did = self.ddmIF.get_did_str(collection)
                if collection in coll_on_tape_set:
                    ret_map["tape_coll_did_list"].append(collection_did)
                else:
                    ret_map["no_tape_coll_did_list"].append(collection_did)
            # return
            tmp_log.debug(f"got {len(ret_prestaging_list)} input datasets to prestage")
            return ret_prestaging_list, ret_map
        except Exception as e:
            tmp_log.error(f"got error ; {traceback.format_exc()}")
            raise e

    def _fill_total_files_and_size(self, dc_req_spec: DataCarouselRequestSpec) -> bool | None:
        """
        Fill total files and dataset size of the Data Carousel request spec

        Args:
            dc_req_spec (DataCarouselRequestSpec): Data Carousel request spec

        Returns:
            bool|None : True if successful, None if getting None from DDM, False if error occurs
        """
        try:
            tmp_log = LogWrapper(logger, f"_fill_total_files_and_size request_id={dc_req_spec.request_id}")
            # get dataset metadata
            dataset_meta = self.ddmIF.get_dataset_metadata(dc_req_spec.dataset)
            # fill
            dc_req_spec.total_files = dataset_meta["length"]
            dc_req_spec.dataset_size = dataset_meta["bytes"]
            if dc_req_spec.total_files is not None:
                return True
            else:
                return None
        except Exception as e:
            tmp_log.error(f"failed to fill total files and size; {e}")
            return False

    def submit_data_carousel_requests(
        self, task_id: int, prestaging_list: list[tuple[str, str | None, str | None]], options: dict | None = None
    ) -> bool | None:
        """
        Submit data carousel requests for a task

        Args:
            task_id (int): JEDI task ID
            prestaging_list (list[tuple[str, str|None, str|None, bool]]): list of tuples in the form of (dataset, source_rse, ddm_rule_id)
            options (dict|None): extra options for submission

        Returns:
            bool | None : True if submission successful, or None if failed
        """
        tmp_log = LogWrapper(logger, f"submit_data_carousel_requests task_id={task_id}")
        n_req_to_submit = len(prestaging_list)
        if options:
            tmp_log.debug(f"options={options} to submit {len(prestaging_list)} requests")
        else:
            tmp_log.debug(f"to submit {len(prestaging_list)} requests")
        # fill dc request spec for each input dataset
        dc_req_spec_list = []
        now_time = naive_utcnow()
        for dataset, source_rse, ddm_rule_id, to_pin, suggested_dst_list in prestaging_list:
            dc_req_spec = DataCarouselRequestSpec()
            dc_req_spec.dataset = dataset
            self._fill_total_files_and_size(dc_req_spec)
            dc_req_spec.staged_files = 0
            dc_req_spec.staged_size = 0
            dc_req_spec.ddm_rule_id = ddm_rule_id
            dc_req_spec.source_rse = source_rse
            dc_req_spec.source_tape = self._get_source_tape_from_rse(dc_req_spec.source_rse)
            if to_pin:
                # to pin the dataset; set to_pin in parameter
                dc_req_spec.set_parameter("to_pin", True)
            dc_req_spec.status = DataCarouselRequestStatus.queued
            dc_req_spec.creation_time = now_time
            if dc_req_spec.ddm_rule_id:
                # already with DDM rule; go to staging directly
                dc_req_spec.status = DataCarouselRequestStatus.staging
                dc_req_spec.start_time = now_time
                dc_req_spec.set_parameter("reuse_rule", True)
            if suggested_dst_list:
                dc_req_spec.set_parameter("suggested_dst_list", suggested_dst_list)
            # options
            if options:
                if options.get("remove_when_done"):
                    # remove rule when done
                    dc_req_spec.set_parameter("remove_when_done", True)
                if task_type := options.get("task_type"):
                    dc_req_spec.set_parameter("task_type", task_type)
            # append to list
            dc_req_spec_list.append(dc_req_spec)
        # insert dc requests for the task
        n_req_inserted = self.taskBufferIF.insert_data_carousel_requests_JEDI(task_id, dc_req_spec_list)
        tmp_log.info(f"submitted {n_req_inserted}/{n_req_to_submit} requests")
        ret = n_req_inserted is not None
        # return
        return ret

    def _get_dc_requests_table_dataframe(self) -> pl.DataFrame | None:
        """
        Get Data Carousel requests table as dataframe for statistics

        Returns:
            polars.DataFrame|None : dataframe of current Data Carousel requests table if successful, or None if failed
        """
        sql = f"SELECT {','.join(DataCarouselRequestSpec.attributes)} " f"FROM {panda_config.schemaJEDI}.data_carousel_requests " f"ORDER BY request_id "
        var_map = {}
        res = self.taskBufferIF.querySQL(sql, var_map, arraySize=99999)
        if res is not None:
            dc_req_df = pl.DataFrame(res, schema=DataCarouselRequestSpec.attributes_with_types, orient="row")
            return dc_req_df
        else:
            return None

    def _get_source_tapes_config_dataframe(self) -> pl.DataFrame:
        """
        Get source tapes config as dataframe

        Returns:
            polars.DataFrame : dataframe of source tapes config
        """
        tmp_list = []
        for k, v in self.dc_config_map.source_tapes_config.items():
            tmp_dict = {"source_tape": k}
            tmp_dict.update(asdict(v))
            tmp_list.append(tmp_dict)
        source_tapes_config_df = pl.DataFrame(tmp_list)
        return source_tapes_config_df

    def _get_source_rses_config_dataframe(self) -> pl.DataFrame:
        """
        Get source RSEs config as dataframe

        Returns:
            polars.DataFrame : dataframe of source RSEs config
        """
        tmp_list = []
        for k, v in self.dc_config_map.source_rses_config.items():
            tmp_dict = {"source_rse": k}
            tmp_dict.update(asdict(v))
            tmp_list.append(tmp_dict)
        source_rses_config_df = pl.DataFrame(tmp_list)
        return source_rses_config_df

    def _get_source_tape_stats_dataframe(self) -> pl.DataFrame | None:
        """
        Get statistics of source tapes as dataframe

        Returns:
            polars.DataFrame : dataframe of statistics of source tapes if successful, or None if failed
        """
        # get Data Carousel requests dataframe of staging requests
        dc_req_df = self._get_dc_requests_table_dataframe()
        if dc_req_df is None:
            return None
        dc_req_df = dc_req_df.filter(pl.col("status") == DataCarouselRequestStatus.staging)
        # get columns from parameters; note that str.json_path_match() always casts to string
        dc_req_df = dc_req_df.with_columns(
            to_pin_str=pl.col("parameters").str.json_path_match(r"$.to_pin").fill_null(False),
            task_gshare=pl.col("parameters").str.json_path_match(r"$.task_gshare"),
            init_task_gshare=pl.col("parameters").str.json_path_match(r"$.init_task_gshare"),
        )
        # get source tapes and RSEs config dataframes
        source_tapes_config_df = self._get_source_tapes_config_dataframe()
        # source_rses_config_df = self._get_source_rses_config_dataframe()
        # dataframe of staging requests with physical tapes, ignoring to_pin requests
        # dc_req_full_df = dc_req_df.join(source_rses_config_df.select("source_rse", "tape"), on="source_rse", how="left")
        dc_req_full_df = dc_req_df.filter(pl.col("to_pin_str") != "true")
        # dataframe of source RSE stats; add staging_files as the remaining files to finish
        source_rse_stats_df = (
            dc_req_full_df.select(
                "source_tape",
                "total_files",
                "staged_files",
                (pl.col("total_files") - pl.col("staged_files")).alias("staging_files"),
            )
            .group_by("source_tape")
            .sum()
        )
        # make dataframe of source tapes stats; add quota_size
        df = source_tapes_config_df.join(source_rse_stats_df, on="source_tape", how="left")
        df = df.with_columns(
            pl.col("total_files").fill_null(strategy="zero"),
            pl.col("staged_files").fill_null(strategy="zero"),
            pl.col("staging_files").fill_null(strategy="zero"),
        )
        df = df.with_columns(quota_size=(pl.col("max_size") - pl.col("staging_files")))
        source_tape_stats_df = df
        # another dataframe of source RSE stats broken down by gshare
        source_rse_gshare_stats_df = (
            dc_req_full_df.select(
                "source_tape",
                "init_task_gshare",
                "total_files",
                "staged_files",
                (pl.col("total_files") - pl.col("staged_files")).alias("staging_files"),
            )
            .group_by(["source_tape", "init_task_gshare"])
            .sum()
            .sort(["source_tape", "init_task_gshare"], nulls_last=True)
        ).with_columns(
            pl.col("total_files").fill_null(strategy="zero"),
            pl.col("staged_files").fill_null(strategy="zero"),
            pl.col("staging_files").fill_null(strategy="zero"),
        )
        # return final dataframes
        return source_tape_stats_df, source_rse_gshare_stats_df

    def _get_gshare_stats(self) -> dict:
        """
        Get current gshare stats

        Returns:
            dict : dictionary of gshares
        """
        # get share and hs info
        gshare_status = self.taskBufferIF.getGShareStatus()
        # initialize
        gshare_dict = dict()
        # rank and data
        for idx, leaf in enumerate(gshare_status):
            rank = idx + 1
            gshare = leaf["name"]
            gshare_dict[gshare] = {
                "gshare": gshare,
                "rank": rank,
                "queuing_hs": leaf["queuing"],
                "running_hs": leaf["running"],
                "target_hs": leaf["target"],
                "usage_perc": leaf["running"] / leaf["target"] if leaf["target"] > 0 else 999999,
                "queue_perc": leaf["queuing"] / leaf["target"] if leaf["target"] > 0 else 999999,
            }
        # return
        return gshare_dict

    def _queued_requests_tasks_to_dataframe(self, queued_requests: list | None) -> pl.DataFrame:
        """
        Transfrom Data Carousel queue requests and their tasks into dataframe

        Args:
            queued_requests (list|None): list of tuples in form of (queued_request, [taskspec1, taskspec2, ...])

        Returns:
            polars.DataFrame : dataframe of queued requests
        """
        # get source RSEs config for tape mapping
        # source_rses_config_df = self._get_source_rses_config_dataframe()
        # get current gshare rank
        gshare_dict = self._get_gshare_stats()
        gshare_rank_dict = {k: v["rank"] for k, v in gshare_dict.items()}
        # make dataframe of queued requests and their tasks
        tmp_list = []
        for dc_req_spec, task_specs in queued_requests:
            for task_spec in task_specs:
                tmp_dict = {
                    "request_id": dc_req_spec.request_id,
                    "dataset": dc_req_spec.dataset,
                    "source_rse": dc_req_spec.source_rse,
                    "source_tape": dc_req_spec.source_tape,
                    "to_pin": dc_req_spec.get_parameter("to_pin"),
                    "total_files": dc_req_spec.total_files,
                    "dataset_size": dc_req_spec.dataset_size,
                    "jediTaskID": task_spec.jediTaskID,
                    "taskType": task_spec.taskType,
                    "userName": task_spec.userName,
                    "workingGroup": task_spec.workingGroup,
                    "gshare": task_spec.gshare,
                    "gshare_rank": gshare_rank_dict.get(task_spec.gshare, 999),
                    "task_priority": task_spec.currentPriority if task_spec.currentPriority else (task_spec.taskPriority if task_spec.taskPriority else 1000),
                }
                tmp_list.append(tmp_dict)
        df = pl.DataFrame(
            tmp_list,
            schema={
                "request_id": pl.datatypes.Int64,
                "dataset": pl.datatypes.String,
                "source_rse": pl.datatypes.String,
                "source_tape": pl.datatypes.String,
                "to_pin": pl.datatypes.Boolean,
                "total_files": pl.datatypes.Int64,
                "dataset_size": pl.datatypes.Int64,
                "jediTaskID": pl.datatypes.Int64,
                "taskType": pl.datatypes.String,
                "userName": pl.datatypes.String,
                "workingGroup": pl.datatypes.String,
                "gshare": pl.datatypes.String,
                "gshare_rank": pl.datatypes.Int64,
                "task_priority": pl.datatypes.Int64,
            },
        )
        # fill null
        df = df.with_columns(
            pl.col("to_pin").fill_null(value=False),
            pl.col("total_files").fill_null(strategy="zero"),
            pl.col("dataset_size").fill_null(strategy="zero"),
        )
        # join to add physical tape
        # df = df.join(source_rses_config_df.select("source_rse", "tape"), on="source_rse", how="left")
        # return final dataframe
        queued_requests_tasks_df = df
        return queued_requests_tasks_df

    @refresh
    def get_requests_to_stage(self, *args, **kwargs) -> list[tuple[DataCarouselRequestSpec, dict]]:
        """
        Get the queued requests which should proceed to get staging

        Args:
            ? (?): ?

        Returns:
            list[tuple[DataCarouselRequestSpec, dict]] : list of requests to stage and dict of extra parameters to set before to stage
        """
        tmp_log = LogWrapper(logger, "get_requests_to_stage")
        ret_list = []
        queued_requests = self.taskBufferIF.get_data_carousel_queued_requests_JEDI()
        if queued_requests is None or not queued_requests:
            tmp_log.debug(f"no requests to stage or to pin ; skipped")
            return ret_list
        # get stats of tapes
        source_tape_stats_df, source_rse_gshare_stats_df = self._get_source_tape_stats_dataframe()
        # tmp_log.debug(f"source_tape_stats_df: \n{source_tape_stats_df}")
        tmp_log.debug(f"source_rse_gshare_stats_df: \n{source_rse_gshare_stats_df.select(['source_tape', 'init_task_gshare', 'total_files', 'staging_files'])}")
        source_tape_stats_dict_list = source_tape_stats_df.to_dicts()
        # map of request_id and dc_req_spec of queued requests
        request_id_spec_map = {dc_req_spec.request_id: dc_req_spec for dc_req_spec, _ in queued_requests}
        # get dataframe of queued requests and tasks
        queued_requests_tasks_df = self._queued_requests_tasks_to_dataframe(queued_requests)
        # sort queued requests : by to_pin, gshare_rank, task_priority, jediTaskID, request_id
        df = queued_requests_tasks_df.sort(["to_pin", "gshare_rank", "task_priority", "request_id"], descending=[True, False, True, False], nulls_last=True)
        # get unique requests with the sorted order
        df = df.unique(subset=["request_id"], keep="first", maintain_order=True)
        # evaluate per tape
        queued_requests_df = df
        for source_tape_stats_dict in source_tape_stats_dict_list:
            source_tape = source_tape_stats_dict["source_tape"]
            quota_size = source_tape_stats_dict["quota_size"]
            # dataframe of the physical tape
            tmp_df = queued_requests_df.filter(pl.col("source_tape") == source_tape)
            # split with to_pin and not to_pin
            to_pin_df = tmp_df.filter(pl.col("to_pin"))
            tmp_queued_df = tmp_df.filter(pl.col("to_pin").not_())
            # fill dummy cumulative sum (0) for reqeusts to pin
            to_pin_df = to_pin_df.with_columns(cum_total_files=pl.lit(0, dtype=pl.datatypes.Int64), cum_dataset_size=pl.lit(0, dtype=pl.datatypes.Int64))
            # fair share for gshares
            if True and not tmp_queued_df.is_empty() and (fair_share_init_quota := min(QUEUE_FAIR_SHARE_MAX_QUOTA, max(quota_size, 0))) > 0:
                queued_gshare_list = tmp_queued_df.select("gshare").unique().to_dict()["gshare"]
                tmp_source_rse_gshare_stats_list = (
                    source_rse_gshare_stats_df.filter((pl.col("source_tape") == source_tape)).select("init_task_gshare", "staging_files").to_dicts()
                )
                gshare_staging_files_map = {x["init_task_gshare"]: x["staging_files"] for x in tmp_source_rse_gshare_stats_list}
                n_queued_gshares = len(queued_gshare_list)
                n_staging_files_of_gshares = sum([gshare_staging_files_map.get(gshare, 0) for gshare in queued_gshare_list])
                # fair share quota per gshare and the virtual one including remaing staging files
                fair_share_quota_per_gshare = fair_share_init_quota // n_queued_gshares
                virtual_fair_share_quota_per_gshare = (fair_share_init_quota + n_staging_files_of_gshares) // n_queued_gshares
                # list of gshares to stage by exluding gshares already having remaining staging files exceeding fair_share_quota_per_gshare
                to_stage_gshare_list = [
                    gshare for gshare in queued_gshare_list if gshare_staging_files_map.get(gshare, 0) < virtual_fair_share_quota_per_gshare
                ]
                n_gshares_to_stage = len(to_stage_gshare_list)
                # initialize dataframe with schema
                fair_share_queued_df = None
                unchosen_queued_df = None
                more_fair_shared_queued_df = tmp_queued_df.clear()
                # fair share quota to distribute to each gshare
                for gshare in to_stage_gshare_list:
                    # remaining_fair_share_quota_of_gshare = fair_share_quota_per_gshare - gshare_staging_files_map.get(gshare, 0)
                    n_staging_files = gshare_staging_files_map.get(gshare, 0)
                    per_gshare_df = tmp_queued_df.filter(pl.col("gshare") == gshare)
                    per_gshare_df = per_gshare_df.with_columns(
                        cum_tot_files_in_gshare=(pl.col("total_files").cum_sum() + pl.lit(n_staging_files, dtype=pl.datatypes.Int64))
                    )
                    if fair_share_queued_df is None:
                        fair_share_queued_df = per_gshare_df.clear()
                    if unchosen_queued_df is None:
                        unchosen_queued_df = per_gshare_df.clear()
                    fair_share_queued_df.extend(per_gshare_df.filter(pl.col("cum_tot_files_in_gshare") <= fair_share_quota_per_gshare))
                    unchosen_queued_df.extend(per_gshare_df.filter(pl.col("cum_tot_files_in_gshare") > fair_share_quota_per_gshare))
                # remaining fair share quota to distribute again
                n_fair_share_files_to_stage = fair_share_queued_df.select("total_files").sum().to_dict()["total_files"][0]
                fair_share_remaining_quota = fair_share_init_quota - n_fair_share_files_to_stage
                if fair_share_remaining_quota > 0:
                    unchosen_queued_df = unchosen_queued_df.sort(["cum_tot_files_in_gshare", "gshare_rank"], descending=[False, False])
                    unchosen_queued_df = unchosen_queued_df.with_columns(tmp_cum_total_files=pl.col("total_files").cum_sum())
                    more_fair_shared_queued_df = unchosen_queued_df.filter(pl.col("tmp_cum_total_files") <= fair_share_remaining_quota)
                    # get one more request among gshares which are not yet staging (may exceed fair_share_remaining_quota)
                    not_yet_staging_gshares_set = (
                        set(to_stage_gshare_list)
                        - set(fair_share_queued_df.select("gshare").unique().to_dict()["gshare"])
                        - set(more_fair_shared_queued_df.select("gshare").unique().to_dict()["gshare"])
                    )
                    if not_yet_staging_gshares_set:
                        lucky_gshare = random.choice(list(not_yet_staging_gshares_set))
                        tmp_lucky_df = unchosen_queued_df.filter(
                            (pl.col("gshare") == lucky_gshare) & (pl.col("tmp_cum_total_files") > fair_share_remaining_quota)
                        ).head(1)
                        more_fair_shared_queued_df.extend(tmp_lucky_df)
                # fill back to all queued requests
                tmp_queued_df = pl.concat(
                    [fair_share_queued_df.select(tmp_queued_df.columns), more_fair_shared_queued_df.select(tmp_queued_df.columns), tmp_queued_df]
                ).unique(subset=["request_id"], keep="first", maintain_order=True)
            # get cumulative sum of queued files per physical tape
            tmp_queued_df = tmp_queued_df.with_columns(cum_total_files=pl.col("total_files").cum_sum(), cum_dataset_size=pl.col("dataset_size").cum_sum())
            # number of queued requests at the physical tape
            n_queued = len(tmp_queued_df)
            n_to_pin = len(to_pin_df)
            n_total = n_queued + n_to_pin
            # print dataframe in log
            tmp_to_print_df = None
            if n_total:
                tmp_to_print_df = pl.concat([to_pin_df, tmp_queued_df])
                tmp_to_print_df = tmp_to_print_df.select(
                    ["request_id", "source_rse", "jediTaskID", "gshare", "gshare_rank", "task_priority", "total_files", "cum_total_files", "to_pin"]
                )
                tmp_to_print_df = tmp_to_print_df.with_columns(gshare_and_rank=pl.concat_str([pl.col("gshare"), pl.col("gshare_rank")], separator=" : "))
            # filter requests to respect the tape quota size; at most one request can reach or exceed quota size if quota size > 0
            to_stage_df = pl.concat(
                [
                    tmp_queued_df.filter(pl.col("cum_total_files") < quota_size),
                    tmp_queued_df.filter((pl.col("cum_total_files") >= quota_size) & (quota_size > 0)).head(1),
                ]
            )
            # append the requests to ret_list
            temp_key_list = ["request_id", "jediTaskID", "gshare", "taskType", "userName", "workingGroup"]
            # to_pin_request_id_list = to_pin_df.select(["request_id"]).to_dict(as_series=False)["request_id"]
            to_pin_request_list = to_pin_df.select(temp_key_list).to_dicts()
            to_stage_request_list = to_stage_df.select(temp_key_list).to_dicts()
            for request_dict in to_pin_request_list:
                request_id = request_dict["request_id"]
                extra_params = {
                    "task_id": request_dict["jediTaskID"],
                    "task_gshare": request_dict["gshare"],
                    # "task_type": request_dict["taskType"],
                    "task_user": request_dict["userName"],
                    "task_group": request_dict["workingGroup"],
                }
                dc_req_spec = request_id_spec_map.get(request_id)
                if dc_req_spec:
                    ret_list.append((dc_req_spec, extra_params))
            to_stage_count = 0
            for request_dict in to_stage_request_list:
                request_id = request_dict["request_id"]
                extra_params = {
                    "task_id": request_dict["jediTaskID"],
                    "task_gshare": request_dict["gshare"],
                    "init_task_gshare": request_dict["gshare"],
                    # "task_type": request_dict["taskType"],
                    "task_user": request_dict["userName"],
                    "task_group": request_dict["workingGroup"],
                }
                dc_req_spec = request_id_spec_map.get(request_id)
                if dc_req_spec:
                    ret_list.append((dc_req_spec, extra_params))
                    to_stage_count += 1
            if tmp_to_print_df is not None and not (to_pin_df.is_empty() and to_stage_df.is_empty()):
                tmp_to_print_df = tmp_to_print_df.with_columns(
                    result=pl.when(pl.col("request_id").is_in(to_pin_df.select(["request_id"])))
                    .then(pl.lit("pin"))
                    .when(pl.col("request_id").is_in(to_stage_df.select(["request_id"])))
                    .then(pl.lit("stage"))
                    .otherwise(pl.lit(" "))
                )
                tmp_to_print_df = tmp_to_print_df.select(
                    ["request_id", "source_rse", "jediTaskID", "gshare_and_rank", "task_priority", "total_files", "cum_total_files", "result"]
                )
                tmp_log.debug(f"  source_tape={source_tape} , quota_size={quota_size} : \n{tmp_to_print_df}")
            if n_total:
                tmp_log.debug(f"source_tape={source_tape} got {to_stage_count}/{n_queued} requests to stage, {n_to_pin} requests to pin")
        tmp_log.debug(f"totally got {len(ret_list)} requests to stage or to pin")
        # return
        return ret_list

    def _choose_destination_rse(self, expression: str, suggested_dst_list: list | None = None, excluded_dst_list: list | None = None) -> str | None:
        """
        Choose a destination (datadisk) RSE based on the destination RSE expression

        Args:
            expression (set): destination RSE expression
            suggested_dst_list (list|None): list of suggested destination list; RSEs in the list will be prioritized
            excluded_dst_list (list|None): list of excluded destination RSEs; RSEs in the list will be excluded from the choice

        Returns:
            str | None : destination RSE chosen, None if failed
        """
        tmp_log = LogWrapper(logger, f"_choose_destination_rse expression={expression} suggested_dst_list={suggested_dst_list}")
        try:
            # initialize
            destination_rse = None
            rse_set = set()
            # get RSEs from DDM
            the_rses = self.ddmIF.list_rses(expression)
            if the_rses is not None:
                rse_set = set(the_rses)
            # exclude RSEs in excluded_destinations from config
            if excluded_destinations_set := set(self.dc_config_map.excluded_destinations):
                rse_set -= excluded_destinations_set
            # exclude RSEs in excluded_dst_list
            if excluded_dst_list:
                excluded_dst_set = set(excluded_dst_list)
                rse_set -= excluded_dst_set
            # prioritize RSEs in suggested_dst_list
            if suggested_dst_list and (prioritized_rse_set := set(suggested_dst_list) & rse_set):
                rse_set = prioritized_rse_set
            # get the list
            rse_list = list(rse_set)
            # tmp_log.debug(f"choosing destination_rse from {rse_list}")
            # choose destination RSE
            if rse_list:
                if len(rse_list) == 1:
                    destination_rse = rse_list[0]
                else:
                    destination_rse = random.choice(rse_list)
                # tmp_log.debug(f"chose destination_rse={destination_rse}")
            else:
                tmp_log.warning(f"no destination_rse match; skipped")
            # return
            return destination_rse
        except Exception as e:
            # other unexpected errors
            tmp_log.error(f"got error ; {traceback.format_exc()}")
            return None

    def _choose_destination_rse_for_request(self, dc_req_spec: DataCarouselRequestSpec) -> str | None:
        """
        Choose destination RSE for the request

        Args:
            dc_req_spec (DataCarouselRequestSpec): spec of the request

        Returns:
            str|None : DDM destination RSE if successful, or None if failed
        """
        tmp_log = LogWrapper(logger, f"_choose_destination_rse_for_request request_id={dc_req_spec.request_id}")
        # initialize
        # get source physical tape
        try:
            source_tape = self._get_source_tape_from_rse(dc_req_spec.source_rse)
        except Exception:
            # other unexpected errors
            tmp_log.error(f"got error ; {traceback.format_exc()}")
            return None
        # parameters about this tape source from DC config
        try:
            source_tape_config = self.dc_config_map.source_tapes_config[source_tape]
        except (KeyError, AttributeError):
            # no destination_expression for this tape; skipped
            tmp_log.warning(f"failed to get destination_expression from config; skipped ; {traceback.format_exc()}")
            return None
        except Exception:
            # other unexpected errors
            tmp_log.error(f"got error ; {traceback.format_exc()}")
            return None
        # destination expression
        if dc_req_spec.get_parameter("to_pin"):
            # to pin; use the simple to pin destination
            tmp_log.debug(f"has to_pin")
            tmp_dst_expr = TO_PIN_DST_REPLI_EXPR
        else:
            # destination_expression from DC config
            tmp_dst_expr = source_tape_config.destination_expression
        # excluded destination RSEs
        excluded_dst_list = dc_req_spec.get_parameter("excluded_dst_list")
        if excluded_dst_list:
            # adjust destination_expression according to excluded_dst_list
            for excluded_dst_rse in excluded_dst_list:
                tmp_dst_expr += f"\\{excluded_dst_rse}"
        # add expression suffix for available RSEs
        tmp_dst_expr += AVAIL_REPLI_EXPR_SUFFIX
        # get suggested_dst_list
        suggested_dst_list = dc_req_spec.get_parameter("suggested_dst_list")
        # choose a destination RSE
        destination_rse = self._choose_destination_rse(expression=tmp_dst_expr, suggested_dst_list=suggested_dst_list, excluded_dst_list=excluded_dst_list)
        if destination_rse is not None:
            tmp_log.debug(f"chose destination RSE to be {destination_rse}")
        else:
            tmp_log.error(f"failed to choose destination RSE; skipped")
            return None
        # return
        return destination_rse

    def _submit_ddm_rule(self, dc_req_spec: DataCarouselRequestSpec, destination_rse: str | None = None) -> tuple[str | None, str | None]:
        """
        Submit DDM replication rule to stage the dataset of the request

        Args:
            dc_req_spec (DataCarouselRequestSpec): spec of the request
            destination_rse (str|None): predetermined destination RSE to stage; if None, will be chosen randomly

        Returns:
            str | None : DDM rule_id of the new rule if submission successful, or None if failed
            str | None : destination RSE chosen
        """
        tmp_log = LogWrapper(logger, f"_submit_ddm_rule request_id={dc_req_spec.request_id}")
        # initialize
        tmp_dst_expr = None
        expression = None
        lifetime = None
        weight = None
        source_replica_expression = None
        # source replica expression
        if dc_req_spec.source_rse:
            source_replica_expression = f"{SRC_REPLI_EXPR_PREFIX}|{dc_req_spec.source_rse}"
        else:
            # no source_rse; unexpected
            tmp_log.warning(f"source_rse is None ; skipped")
            return None
        # get destination expression RSE
        if destination_rse is None:
            destination_rse = self._choose_destination_rse_for_request(dc_req_spec)
        if destination_rse is not None:
            # got a destination RSE
            expression = str(destination_rse)
        else:
            # no match of destination RSE; return None and stay queued
            tmp_log.error(f"failed to get destination RSE; skipped")
            return None
        # get task type for DDM rule activity
        ddm_rule_activity = DDM_RULE_ACTIVITY_MAP["prod"]
        if task_type := dc_req_spec.get_parameter("task_type"):
            ddm_rule_activity = DDM_RULE_ACTIVITY_MAP.get(task_type, ddm_rule_activity)
        # submit ddm staging rule
        ddm_rule_id = self.ddmIF.make_staging_rule(
            dataset_name=dc_req_spec.dataset,
            expression=expression,
            activity=ddm_rule_activity,
            lifetime=lifetime,
            weight=weight,
            notify="P",
            source_replica_expression=source_replica_expression,
        )
        # return
        return ddm_rule_id, destination_rse

    def _submit_idds_stagein_request(self, task_id: int, dc_req_spec: DataCarouselRequestSpec) -> Any:
        """
        Submit corresponding iDDS stage-in request for given Data Carousel request and task
        Currently only used for manual testing or after resubmitting Data Carousel requests

        Args:
            task_id (int): jediTaskID of the task
            dc_req_spec (DataCarouselRequestSpec): request to submit iDDS stage-in request

        Returns:
            Any : iDDS requests ID returned from iDDS
        """
        tmp_log = LogWrapper(logger, f"_submit_idds_stagein_request request_id={dc_req_spec.request_id}")
        # dataset and rule_id
        dataset = dc_req_spec.dataset
        rule_id = dc_req_spec.ddm_rule_id
        ds_str_list = dataset.split(":")
        tmp_scope = ds_str_list[0]
        tmp_name = ds_str_list[1]
        # iDDS request
        c = iDDS_Client(idds.common.utils.get_rest_host())
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
                "workload_id": task_id,
                "rule_id": rule_id,
            },
        }
        tmp_log.debug(f"iDDS request: {req}")
        ret = c.add_request(**req)
        tmp_log.debug(f"done submit; iDDS_requestID={ret}")
        # return
        return ret

    @refresh
    def stage_request(
        self, dc_req_spec: DataCarouselRequestSpec, extra_params: dict | None = None, destination_rse: str | None = None, submit_idds_request=True
    ) -> tuple[bool, str | None, DataCarouselRequestSpec]:
        """
        Stage the dataset of the request and update request status to staging

        Args:
            dc_req_spec (DataCarouselRequestSpec): spec of the request
            extra_params (dict|None): extra parameters of the request to set; None if nothing to set
            destination_rse (str|None): predetermined destination RSE to stage the dataset; None if to choose randomly later
            submit_idds_request (bool): whether to submit IDDS request for the dataset; default is True

        Returns:
            bool : True for success, False otherwise
            str|None : error message if any, None otherwise
            DataCarouselRequestSpec : updated spec of the request
        """
        tmp_log = LogWrapper(logger, f"stage_request request_id={dc_req_spec.request_id}")
        is_ok = False
        err_msg = None
        # renew dc_req_spec from DB
        with self.request_lock(dc_req_spec.request_id) as locked_spec:
            if not locked_spec:
                # not getting lock; skip
                err_msg = "did not get lock; skipped"
                tmp_log.warning(err_msg)
                return is_ok, err_msg, dc_req_spec
            # got locked spec
            dc_req_spec = locked_spec
            # skip if not queued
            if dc_req_spec.status != DataCarouselRequestStatus.queued:
                err_msg = f"status={dc_req_spec.status} not queued; skipped"
                tmp_log.warning(err_msg)
                return is_ok, err_msg, dc_req_spec
            # retry to get DDM dataset metadata and skip if total_files is still None
            if dc_req_spec.total_files is None:
                _got = self._fill_total_files_and_size(dc_req_spec)
                if not _got:
                    err_msg = f"total_files and dataset_size are still None; skipped"
                    tmp_log.warning(err_msg)
                    return is_ok, err_msg, dc_req_spec
            # check existing DDM rule of the dataset
            if (ddm_rule_id := dc_req_spec.ddm_rule_id) is not None:
                # DDM rule exists; no need to submit
                tmp_log.debug(f"dataset={dc_req_spec.dataset} already has active DDM rule ddm_rule_id={ddm_rule_id}")
            else:
                # no existing rule; submit DDM rule
                ddm_rule_id, destination_rse = self._submit_ddm_rule(dc_req_spec, destination_rse=destination_rse)
                if ddm_rule_id:
                    # DDM rule submitted; update ddm_rule_id
                    dc_req_spec.ddm_rule_id = ddm_rule_id
                    tmp_log.debug(f"submitted DDM rule ddm_rule_id={ddm_rule_id}")
                else:
                    # failed to submit
                    err_msg = f"failed to submitted DDM rule ; skipped"
                    tmp_log.warning(err_msg)
                    return is_ok, err_msg, dc_req_spec
            # update extra parameters
            if extra_params:
                dc_req_spec.update_parameters(extra_params)
            # update request to be staging
            now_time = naive_utcnow()
            dc_req_spec.status = DataCarouselRequestStatus.staging
            dc_req_spec.start_time = now_time
            ret = self.taskBufferIF.update_data_carousel_request_JEDI(dc_req_spec)
            if ret:
                tmp_log.info(f"updated DB about staging; status={dc_req_spec.status}")
                is_ok = True
            # log for monitoring
            tmp_log.info(
                f"started staging "
                f"dataset={dc_req_spec.dataset} source_tape={dc_req_spec.source_tape} source_rse={dc_req_spec.source_rse} "
                f"destination_rse={destination_rse} ddm_rule_id={dc_req_spec.ddm_rule_id} "
                f"total_files={dc_req_spec.total_files} dataset_size={dc_req_spec.dataset_size} "
                f"task_id={dc_req_spec.get_parameter('task_id')} task_type={dc_req_spec.get_parameter('task_type')} "
                f"task_user={dc_req_spec.get_parameter('task_user')} task_group={dc_req_spec.get_parameter('task_group')} "
                f"to_pin={dc_req_spec.get_parameter('to_pin')}"
            )
        # to iDDS staging requests
        if is_ok and submit_idds_request:
            # get all tasks related to this request
            task_id_list = self._get_related_tasks(dc_req_spec.request_id)
            if task_id_list:
                tmp_log.debug(f"related tasks: {task_id_list}")
                for task_id in task_id_list:
                    self._submit_idds_stagein_request(task_id, dc_req_spec)
                tmp_log.debug(f"submitted corresponding iDDS requests for related tasks")
            else:
                tmp_log.warning(f"failed to get related tasks; skipped to submit iDDS requests")
        # return
        return is_ok, err_msg, dc_req_spec

    def _refresh_ddm_rule(self, rule_id: str, lifetime: int) -> bool:
        """
        Refresh lifetime of the DDM rule

        Args:
            rule_id (str): DDM rule ID
            lifetime (int): lifetime in seconds to set

        Returns:
            bool : True for success, False otherwise
        """
        set_map = {"lifetime": lifetime}
        ret = self.ddmIF.update_rule_by_id(rule_id, set_map)
        return ret

    def cancel_request(self, dc_req_spec: DataCarouselRequestSpec, by: str = "manual", reason: str | None = None) -> bool | None:
        """
        Cancel a request
        Set corresponding DDM rule to be about to expired (in 5 sec)

        Args:
            dc_req_spec (DataCarouselRequestSpec): spec of the request to cancel
            by (str): annotation of the caller of this method; default is "manual"
            reason (str|None): annotation of the reason for cancelling

        Returns:
            bool|None : True for success, None otherwise
        """
        tmp_log = LogWrapper(logger, f"cancel_request request_id={dc_req_spec.request_id} by={by}" + (f" reason={reason}" if reason else " "))
        # cancel
        ret = self.taskBufferIF.cancel_data_carousel_request_JEDI(dc_req_spec.request_id)
        if ret:
            tmp_log.debug(f"cancelled")
        elif ret == 0:
            tmp_log.debug(f"already terminated; skipped")
        else:
            tmp_log.error(f"failed to cancel")
        # expire DDM rule
        if dc_req_spec.ddm_rule_id:
            short_time = 5
            self._refresh_ddm_rule(dc_req_spec.ddm_rule_id, short_time)
        # return
        return ret

    def retire_request(self, dc_req_spec: DataCarouselRequestSpec, by: str = "manual", reason: str | None = None) -> bool | None:
        """
        Retire a done request so that it will not be reused
        Set corresponding DDM rule to be about to expired (in 5 sec)

        Args:
            dc_req_spec (DataCarouselRequestSpec): spec of the request to cancel
            by (str): annotation of the caller of this method; default is "manual"
            reason (str|None): annotation of the reason for retiring

        Returns:
            bool|None : True for success, None otherwise
        """
        tmp_log = LogWrapper(logger, f"retire_request request_id={dc_req_spec.request_id} by={by}" + (f" reason={reason}" if reason else " "))
        # retire
        ret = self.taskBufferIF.retire_data_carousel_request_JEDI(dc_req_spec.request_id)
        if ret:
            tmp_log.debug(f"retired")
        elif ret == 0:
            tmp_log.debug(f"cannot retire; skipped")
        else:
            tmp_log.error(f"failed to retire")
        # return
        return ret

    def _check_ddm_rule_of_request(self, dc_req_spec: DataCarouselRequestSpec, by: str = "unknown") -> tuple[bool, str | None, dict | None]:
        """
        Check if the DDM rule of the request is valid.
        If rule not found, update the request and try to cancel or retire it.

        Args:
            dc_req_spec (DataCarouselRequestSpec): spec of the request

        Returns:
            bool : True if valid, False otherwise
            str|None : DDM rule ID
            dict|None : DDM rule data if valid, None otherwise
        """
        tmp_log = LogWrapper(logger, f"_check_ddm_rule_of_request request_id={dc_req_spec.request_id} ddm_rule_id={dc_req_spec.ddm_rule_id}")
        is_valid = False
        # get DDM rule
        ddm_rule_id = dc_req_spec.ddm_rule_id
        the_rule = self.ddmIF.get_rule_by_id(ddm_rule_id)
        if the_rule is False:
            # rule not found
            with self.request_lock(dc_req_spec.request_id) as locked_spec:
                if not locked_spec:
                    # not getting lock; skip
                    tmp_log.warning(f"did not get lock; skipped")
                    return is_valid, ddm_rule_id, None
                # got locked spec
                dc_req_spec = locked_spec
                dc_req_spec.set_parameter("rule_unfound", True)
                tmp_log.warning(f"rule not found")
                tmp_ret = self.taskBufferIF.update_data_carousel_request_JEDI(dc_req_spec)
                if tmp_ret:
                    tmp_log.debug(f"updated DB about rule not found")
                else:
                    tmp_log.error(f"failed to update DB ; skipped")
            # try to cancel or retire request
            if dc_req_spec.status == DataCarouselRequestStatus.staging:
                # requests staging but DDM rule not found; to cancel
                self.cancel_request(dc_req_spec, by=by, reason="rule_unfound")
            elif dc_req_spec.status == DataCarouselRequestStatus.done:
                # requests done but DDM rule not found; to retire
                self.retire_request(dc_req_spec, by=by, reason="rule_unfound")
        elif the_rule is None:
            # got error when getting the rule
            tmp_log.error(f"failed to get rule ; skipped")
        else:
            # rule found
            is_valid = True
            # tmp_log.debug(f"rule is valid")
        # return
        return is_valid, ddm_rule_id, the_rule

    def refresh_ddm_rule_of_request(self, dc_req_spec: DataCarouselRequestSpec, lifetime_days: int, force_refresh: bool = False, by: str = "unknown") -> bool:
        """
        Refresh lifetime of the DDM rule of one request

        Args:
            dc_req_spec (DataCarouselRequestSpec): spec of the request
            lifetime_days (int): lifetime in days to set
            force_refresh (bool): force to refresh regardless of to_refresh max/min lifetime
            by (str): annotation of the caller of this method; default is "watchdog"

        Returns:
            bool : True for success, False otherwise
        """
        tmp_log = LogWrapper(logger, f"refresh_ddm_rule_of_request request_id={dc_req_spec.request_id}")
        # initialize
        ret = False
        # check if the rule is valid
        is_valid, ddm_rule_id, the_rule = self._check_ddm_rule_of_request(dc_req_spec, by=by)
        if not is_valid:
            # rule not valid; skipped
            tmp_log.error(f"ddm_rule_id={ddm_rule_id} rule not valid; skipped")
            return ret
        # rule lifetime
        rule_lifetime = None
        if the_rule["expires_at"]:
            now_time = naive_utcnow()
            rule_lifetime = the_rule["expires_at"] - now_time
        # trigger renewal if force_refresh or when lifetime within the range
        if (
            rule_lifetime is None
            or force_refresh
            or (rule_lifetime < timedelta(days=TO_REFRESH_MAX_LIFETIME_DAYS) and rule_lifetime > timedelta(hours=TO_REFRESH_MIN_LIFETIME_HOURS))
        ):
            ret = self._refresh_ddm_rule(ddm_rule_id, 86400 * lifetime_days)
            # tmp_log.debug(f"status={dc_req_spec.status} ddm_rule_id={ddm_rule_id} refreshed lifetime to be {lifetime_days} days long")
        else:
            # rule_lifetime_days = rule_lifetime.total_seconds() / 86400
            # tmp_log.debug(
            #     f"ddm_rule_id={ddm_rule_id} not to refresh as lifetime {rule_lifetime_days:.2f}d not within range {TO_REFRESH_MAX_LIFETIME_DAYS}d to {TO_REFRESH_MIN_LIFETIME_HOURS}h"
            # )
            pass
        # return
        return ret

    def keep_alive_ddm_rules(self, by: str = "watchdog"):
        """
        Keep alive DDM rules of requests of active tasks; also check all requests if their DDM rules are valid

        Args:
            by (str): annotation of the caller of this method; default is "watchdog"
        """
        tmp_log = LogWrapper(logger, "keep_alive_ddm_rules")
        # get all requests
        all_requests_map, _ = self.taskBufferIF.get_data_carousel_requests_by_task_status_JEDI()
        # get requests of active tasks
        active_tasked_requests_map, _ = self.taskBufferIF.get_data_carousel_requests_by_task_status_JEDI(status_exclusion_list=FINAL_TASK_STATUSES)
        for dc_req_spec in all_requests_map.values():
            try:
                if dc_req_spec.status not in [DataCarouselRequestStatus.staging, DataCarouselRequestStatus.done]:
                    # skip requests without need to keep rules alive
                    continue
                if dc_req_spec.request_id in active_tasked_requests_map:
                    # requests of active tasks; decide lifetime in days
                    days = None
                    if dc_req_spec.status == DataCarouselRequestStatus.staging:
                        # for requests staging
                        days = STAGING_LIFETIME_DAYS
                    elif dc_req_spec.status == DataCarouselRequestStatus.done:
                        # for requests done
                        days = DONE_LIFETIME_DAYS
                    # trigger renewal
                    if days is not None:
                        ret = self.refresh_ddm_rule_of_request(dc_req_spec, lifetime_days=days, by=by)
                        if ret:
                            tmp_log.debug(
                                f"request_id={dc_req_spec.request_id} status={dc_req_spec.status} ddm_rule_id={dc_req_spec.ddm_rule_id} refreshed lifetime to be {days} days long"
                            )
                        else:
                            # tmp_log.debug(
                            #     f"request_id={dc_req_spec.request_id} status={dc_req_spec.status} ddm_rule_id={dc_req_spec.ddm_rule_id} not to renew ; skipped"
                            # )
                            pass
                else:
                    # requests of non-active tasks; check if the rule is valid
                    is_valid, ddm_rule_id, _ = self._check_ddm_rule_of_request(dc_req_spec, by=by)
                    # if not is_valid:
                    #     tmp_log.warning(f"ddm_rule_id={ddm_rule_id} rule not valid")
            except Exception:
                tmp_log.error(f"request_id={dc_req_spec.request_id} got error ; {traceback.format_exc()}")

    def _update_staged_files(self, dc_req_spec: DataCarouselRequestSpec) -> bool | None:
        """
        Update status of files in DB Jedi_Dataset_Contents for a request done staging

        Args:
            dc_req_spec (DataCarouselRequestSpec): spec of the request

        Returns:
            bool|None : True for success, None otherwise
        """
        tmp_log = LogWrapper(logger, f"_update_staged_files request_id={dc_req_spec.request_id}")
        try:
            # get scope of dataset
            dataset = dc_req_spec.dataset
            scope, dsname = self.ddmIF.extract_scope(dataset)
            # get lfn of files in the dataset from DDM
            lfn_set = self.ddmIF.get_files_in_dataset(dataset, ignore_unknown=True, lfn_only=True)
            # make filenames_dict for updateInputFilesStaged_JEDI
            filenames_dict = {}
            dummy_value_tuple = (None, None)
            for lfn in lfn_set:
                filenames_dict[lfn] = dummy_value_tuple
            # get all related tasks to update staged files
            task_id_list = self._get_related_tasks(dc_req_spec.request_id)
            if task_id_list:
                # tmp_log.debug(f"related tasks: {task_id_list}")
                n_done_tasks = 0
                for task_id in task_id_list:
                    ret = self.taskBufferIF.updateInputFilesStaged_JEDI(task_id, scope, filenames_dict, by="DataCarousel", check_scope=False)
                    if ret is None:
                        tmp_log.warning(f"failed to update files for task_id={task_id} ; skipped")
                    else:
                        n_done_tasks += 1
                tmp_log.debug(f"updated staged files for {n_done_tasks}/{len(task_id_list)} related tasks")
            else:
                tmp_log.warning(f"failed to get related tasks; skipped")
            # return
            return True
        except Exception:
            tmp_log.error(f"got error ; {traceback.format_exc()}")
            return None

    def check_staging_requests(self):
        """
        Check staging requests
        """
        tmp_log = LogWrapper(logger, "check_staging_requests")
        dc_req_specs = self.taskBufferIF.get_data_carousel_staging_requests_JEDI()
        if dc_req_specs is None:
            tmp_log.warning(f"failed to query requests to check ; skipped")
        elif not dc_req_specs:
            tmp_log.debug(f"got no requests to check ; skipped")
        for dc_req_spec in dc_req_specs:
            try:
                to_update = False
                # get DDM rule
                ddm_rule_id = dc_req_spec.ddm_rule_id
                the_rule = self.ddmIF.get_rule_by_id(ddm_rule_id)
                if the_rule is False:
                    with self.request_lock(dc_req_spec.request_id) as locked_spec:
                        if not locked_spec:
                            # not getting lock; skip
                            tmp_log.warning(f"did not get lock; skipped")
                            continue
                        # got locked spec
                        dc_req_spec = locked_spec
                        # rule not found
                        dc_req_spec.set_parameter("rule_unfound", True)
                        tmp_log.error(f"request_id={dc_req_spec.request_id} ddm_rule_id={ddm_rule_id} rule not found")
                        tmp_ret = self.taskBufferIF.update_data_carousel_request_JEDI(dc_req_spec)
                        if tmp_ret:
                            tmp_log.debug(f"request_id={dc_req_spec.request_id} updated DB about rule not found")
                        else:
                            tmp_log.error(f"request_id={dc_req_spec.request_id} failed to update DB ; skipped")
                    # try to cancel or retire request
                    if dc_req_spec.status == DataCarouselRequestStatus.staging:
                        # requests staging but DDM rule not found; to cancel
                        self.cancel_request(dc_req_spec, by="watchdog", reason="rule_unfound")
                    elif dc_req_spec.status == DataCarouselRequestStatus.done:
                        # requests done but DDM rule not found; to retire
                        self.retire_request(dc_req_spec, by="watchdog", reason="rule_unfound")
                    continue
                elif the_rule is None:
                    # got error when getting the rule
                    tmp_log.error(f"request_id={dc_req_spec.request_id} failed to get rule of ddm_rule_id={ddm_rule_id} ; skipped")
                    continue
                # Got rule; check further
                with self.request_lock(dc_req_spec.request_id) as locked_spec:
                    if not locked_spec:
                        # not getting lock; skip
                        tmp_log.warning(f"did not get lock; skipped")
                        continue
                    # got locked spec
                    dc_req_spec = locked_spec
                    # Destination RSE
                    if dc_req_spec.destination_rse is None:
                        the_replica_locks = self.ddmIF.list_replica_locks_by_id(ddm_rule_id)
                        try:
                            the_first_file = the_replica_locks[0]
                        except IndexError:
                            tmp_log.warning(
                                f"request_id={dc_req_spec.request_id} no file from replica lock of ddm_rule_id={ddm_rule_id} ; destination_rse not updated"
                            )
                        except TypeError:
                            tmp_log.warning(
                                f"request_id={dc_req_spec.request_id} error listing replica lock of ddm_rule_id={ddm_rule_id} ; destination_rse not updated"
                            )
                        else:
                            # fill in destination RSE
                            destination_rse = the_first_file["rse"]
                            dc_req_spec.destination_rse = destination_rse
                            tmp_log.debug(f"request_id={dc_req_spec.request_id} filled destination_rse={destination_rse} of ddm_rule_id={ddm_rule_id}")
                            to_update = True
                    # current staged files
                    now_time = naive_utcnow()
                    current_staged_files = int(the_rule["locks_ok_cnt"])
                    new_staged_files = current_staged_files - dc_req_spec.staged_files
                    if new_staged_files > 0:
                        # have more staged files than before; update request according to DDM rule
                        dc_req_spec.staged_files = current_staged_files
                        dc_req_spec.staged_size = int(dc_req_spec.dataset_size * dc_req_spec.staged_files / dc_req_spec.total_files)
                        dc_req_spec.last_staged_time = now_time
                        to_update = True
                    else:
                        tmp_log.debug(f"request_id={dc_req_spec.request_id} got {new_staged_files} new staged files")
                    # check completion of staging
                    if dc_req_spec.staged_files == dc_req_spec.total_files:
                        # all files staged; process request to done
                        dc_req_spec.status = DataCarouselRequestStatus.done
                        dc_req_spec.end_time = now_time
                        dc_req_spec.staged_size = dc_req_spec.dataset_size
                        to_update = True
                    # update to DB if attribute updated
                    if to_update:
                        ret = self.taskBufferIF.update_data_carousel_request_JEDI(dc_req_spec)
                        if ret is not None:
                            # updated DB about staging
                            tmp_log.info(
                                f"request_id={dc_req_spec.request_id} got {new_staged_files} new staged files; updated DB about staging ; status={dc_req_spec.status}"
                            )
                            # more for done requests
                            if dc_req_spec.status == DataCarouselRequestStatus.done:
                                # force to keep alive the rule
                                tmp_ret = self.refresh_ddm_rule_of_request(dc_req_spec, lifetime_days=DONE_LIFETIME_DAYS, force_refresh=True, by="watchdog")
                                if tmp_ret:
                                    tmp_log.debug(
                                        f"request_id={dc_req_spec.request_id} status={dc_req_spec.status} ddm_rule_id={dc_req_spec.ddm_rule_id} refreshed lifetime to be {DONE_LIFETIME_DAYS} days long"
                                    )
                                # update staged files in DB for done requests
                                tmp_ret = self._update_staged_files(dc_req_spec)
                                if tmp_ret:
                                    tmp_log.debug(f"request_id={dc_req_spec.request_id} done; updated staged files")
                                else:
                                    tmp_log.warning(f"request_id={dc_req_spec.request_id} done; failed to update staged files ; skipped")
                        else:
                            tmp_log.error(f"request_id={dc_req_spec.request_id} failed to update DB for ddm_rule_id={ddm_rule_id} ; skipped")
                            continue
            except Exception:
                tmp_log.error(f"request_id={dc_req_spec.request_id} got error ; {traceback.format_exc()}")

    def _resume_task(self, task_id: int) -> bool:
        """
        Resume task from staging (to staged-pending)

        Args:
            task_id (int): JEDI task ID

        Returns:
            bool : True for success, False otherwise
        """
        tmp_log = LogWrapper(logger, "_resume_task")
        # send resume command
        ret_val, ret_str = self.taskBufferIF.sendCommandTaskPanda(task_id, "Data Carousel. Resumed from staging", True, "resume", properErrorCode=True)
        # check if ok
        if ret_val == 0:
            return True
        else:
            tmp_log.warning(f"task_id={task_id} failed to resume the task: error_code={ret_val} {ret_str}")
            return False

    def resume_tasks_from_staging(self):
        """
        Get tasks with enough staged files and resume them
        """
        tmp_log = LogWrapper(logger, "resume_tasks_from_staging")
        ret_requests_map, ret_relation_map = self.taskBufferIF.get_data_carousel_requests_by_task_status_JEDI(status_filter_list=["staging"])
        n_resumed_tasks = 0
        for task_id, request_id_list in ret_relation_map.items():
            to_resume = False
            try:
                # _, task_spec = self.taskBufferIF.getTaskWithID_JEDI(task_id, fullFlag=False)
                # if not task_spec:
                #     # task not found
                #     tmp_log.error(f"task_id={task_id} task not found; skipped")
                #     continue
                for request_id in request_id_list:
                    dc_req_spec = ret_requests_map[request_id]
                    # if task_spec.taskType == "prod":
                    #     # condition for production tasks: resume if one file staged
                    #     if dc_req_spec.status == DataCarouselRequestStatus.done or (dc_req_spec.staged_files and dc_req_spec.staged_files > 0):
                    #         # got at least one data carousel request done for the task, to resume
                    #         to_resume = True
                    #         break
                    # elif task_spec.taskType == "anal":
                    #     # condition for analysis tasks
                    #     # FIXME: temporary conservative condition for analysis tasks: resume if one dataset staged
                    #     if dc_req_spec.status == DataCarouselRequestStatus.done:
                    #         # got at least one entire dataset staged, to resume
                    #         to_resume = True
                    #         break
                    # resume as soon as DDM rules are created
                    if dc_req_spec.ddm_rule_id and dc_req_spec.status in [
                        DataCarouselRequestStatus.staging,
                        DataCarouselRequestStatus.done,
                        DataCarouselRequestStatus.retired,
                    ]:
                        to_resume = True
                        break
                if to_resume:
                    # resume the task
                    ret_val = self._resume_task(task_id)
                    if ret_val:
                        n_resumed_tasks += 1
                        tmp_log.debug(f"task_id={task_id} resumed the task")
                    else:
                        tmp_log.warning(f"task_id={task_id} failed to resume the task; skipped")
            except Exception:
                tmp_log.error(f"task_id={task_id} got error ; {traceback.format_exc()}")
        # summary
        tmp_log.debug(f"resumed {n_resumed_tasks} tasks")

    def _get_orphan_requests(self) -> list[DataCarouselRequestSpec] | None:
        """
        Get orphan requests, which are active and without any tasks related

        Returns:
            list[DataCarouselRequestSpec]|None : list of orphan requests, or None if failure
        """
        tmp_log = LogWrapper(logger, f"_get_orphan_requests")
        status_var_names_str, status_var_map = get_sql_IN_bind_variables(DataCarouselRequestStatus.active_statuses, prefix=":status")
        sql = (
            f"SELECT {DataCarouselRequestSpec.columnNames()} "
            f"FROM {panda_config.schemaJEDI}.data_carousel_requests "
            f"WHERE request_id NOT IN "
            f"(SELECT rel.request_id FROM {panda_config.schemaJEDI}.data_carousel_relations rel)"
            f"AND status IN ({status_var_names_str}) "
        )
        var_map = {}
        var_map.update(status_var_map)
        res_list = self.taskBufferIF.querySQL(sql, var_map, arraySize=99999)
        if res_list:
            ret_list = []
            for res in res_list:
                dc_req_spec = DataCarouselRequestSpec()
                dc_req_spec.pack(res)
                ret_list.append(dc_req_spec)
            tmp_log.debug(f"got {len(ret_list)} orphan request")
            return ret_list
        else:
            tmp_log.debug("no orphan request; skipped")
            return None

    def clean_up_requests(
        self, done_age_limit_days: int | float = DONE_LIFETIME_DAYS, outdated_age_limit_days: int | float = DONE_LIFETIME_DAYS, by: str = "watchdog"
    ):
        """
        Clean up terminated and outdated requests

        Args:
            done_age_limit_days (int|float): age limit in days for requests done and without active tasks
            outdated_age_limit_days (int|float): age limit in days for outdated requests
            by (str): annotation of the caller of this method; default is "watchdog"
        """
        tmp_log = LogWrapper(logger, "clean_up_requests")
        try:
            # initialize
            done_requests_set = set()
            cancelled_requests_set = set()
            retired_requests_set = set()
            # get requests of terminated and active tasks
            terminated_tasks_requests_map, terminated_tasks_relation_map = self.taskBufferIF.get_data_carousel_requests_by_task_status_JEDI(
                status_filter_list=FINAL_TASK_STATUSES
            )
            active_tasks_requests_map, active_tasks_relation_map = self.taskBufferIF.get_data_carousel_requests_by_task_status_JEDI(
                status_exclusion_list=FINAL_TASK_STATUSES
            )
            now_time = naive_utcnow()
            # set of requests of terminated tasks
            request_ids_of_terminated_tasks = set()
            for request_id_list in terminated_tasks_relation_map.values():
                request_ids_of_terminated_tasks |= set(request_id_list)
            # set of requests of active tasks
            request_ids_of_active_tasks = set()
            for request_id_list in active_tasks_relation_map.values():
                request_ids_of_active_tasks |= set(request_id_list)
            # loop over requests of terminated tasks
            for request_id in request_ids_of_terminated_tasks:
                if request_id in active_tasks_requests_map:
                    # the request is also mapped to some active task, not to be cleaned up; skipped
                    continue
                dc_req_spec = terminated_tasks_requests_map[request_id]
                if dc_req_spec.status == DataCarouselRequestStatus.done and (
                    (dc_req_spec.end_time and dc_req_spec.end_time < now_time - timedelta(days=done_age_limit_days))
                    or dc_req_spec.get_parameter("rule_unfound")
                    or dc_req_spec.get_parameter("remove_when_done")
                ):
                    # requests done, and old enough or DDM rule not found or to remove when done; to clean up
                    done_requests_set.add(request_id)
                elif dc_req_spec.status == DataCarouselRequestStatus.staging:
                    # requests staging while related tasks all terminated; to cancel (to clean up in next cycle)
                    self.cancel_request(dc_req_spec, by=by, reason="staging_while_all_tasks_ended")
                elif dc_req_spec.status == DataCarouselRequestStatus.cancelled:
                    # requests cancelled; to clean up
                    cancelled_requests_set.add(request_id)
                elif dc_req_spec.status == DataCarouselRequestStatus.retired:
                    # requests retired; to clean up
                    retired_requests_set.add(request_id)
            # loop over requests of active tasks to cancel bad ones
            for request_id in request_ids_of_active_tasks:
                dc_req_spec = active_tasks_requests_map[request_id]
                if dc_req_spec.status == DataCarouselRequestStatus.staging and dc_req_spec.get_parameter("rule_unfound"):
                    # requests staging but DDM rule not found; to cancel
                    self.cancel_request(dc_req_spec, by=by, reason="rule_unfound")
            # cancel orphan reqeusts
            orphan_requests = self._get_orphan_requests()
            if orphan_requests:
                for dc_req_spec in orphan_requests:
                    self.cancel_request(dc_req_spec, by=by, reason="orphan")
            # delete ddm rules of terminated requests of terminated tasks
            for request_id in done_requests_set | cancelled_requests_set | retired_requests_set:
                dc_req_spec = terminated_tasks_requests_map[request_id]
                ddm_rule_id = dc_req_spec.ddm_rule_id
                if ddm_rule_id:
                    try:
                        ret = self.ddmIF.delete_replication_rule(ddm_rule_id)
                        if ret is False:
                            tmp_log.warning(f"request_id={request_id} ddm_rule_id={ddm_rule_id} rule not found ; skipped")
                        else:
                            tmp_log.debug(f"request_id={request_id} ddm_rule_id={ddm_rule_id} deleted DDM rule")
                    except Exception:
                        tmp_log.error(f"request_id={request_id} ddm_rule_id={ddm_rule_id} failed to delete DDM rule; {traceback.format_exc()}")
            # delete terminated requests of terminated tasks
            if done_requests_set or cancelled_requests_set or retired_requests_set:
                if done_requests_set:
                    # done requests
                    ret = self.taskBufferIF.delete_data_carousel_requests_JEDI(list(done_requests_set))
                    if ret is None:
                        tmp_log.warning(f"failed to delete done requests; skipped")
                    else:
                        tmp_log.debug(f"deleted {ret} done requests older than {done_age_limit_days} days or rule_unfound or remove_when_done")
                if cancelled_requests_set:
                    # cancelled requests
                    ret = self.taskBufferIF.delete_data_carousel_requests_JEDI(list(cancelled_requests_set))
                    if ret is None:
                        tmp_log.warning(f"failed to delete cancelled requests; skipped")
                    else:
                        tmp_log.debug(f"deleted {ret} cancelled requests")
                if retired_requests_set:
                    # retired requests
                    ret = self.taskBufferIF.delete_data_carousel_requests_JEDI(list(retired_requests_set))
                    if ret is None:
                        tmp_log.warning(f"failed to delete retired requests; skipped")
                    else:
                        tmp_log.debug(f"deleted {ret} retired requests")
            else:
                tmp_log.debug(f"no terminated requests to delete; skipped")
            # clean up outdated requests
            ret_outdated = self.taskBufferIF.clean_up_data_carousel_requests_JEDI(time_limit_days=outdated_age_limit_days)
            if ret_outdated is None:
                tmp_log.warning(f"failed to delete outdated requests; skipped")
            else:
                tmp_log.debug(f"deleted {ret_outdated} outdated requests older than {outdated_age_limit_days} days")
        except Exception:
            tmp_log.error(f"got error ; {traceback.format_exc()}")

    def resubmit_request(
        self, orig_dc_req_spec: DataCarouselRequestSpec, submit_idds_request=True, exclude_prev_dst: bool = False, by: str = "manual", reason: str | None = None
    ) -> tuple[DataCarouselRequestSpec | None, str | None]:
        """
        Resubmit a request by ending the old request and submitting a new request
        The request status must be in staging, done, cancelled, retired
        A staging request will be cancelled, a done request will be retired. Cancelled and retired requests are intact
        Return the spec of newly resubmitted request

        Args:
            orig_dc_req_spec (DataCarouselRequestSpec): original spec of the request to resubmit from
            submit_idds_request (bool): whether to submit corresponding iDDS request; default is True
            exclude_prev_dst (bool): whether to exclude previous destination
            by (str): annotation of the caller of this method; default is "manual"
            reason (str|None): annotation of the reason for resubmitting

        Returns:
            DataCarouselRequestSpec|None : spec of the resubmitted reqeust spec if success, None otherwise
            str|None : error message if any, None otherwise
        """
        tmp_log = LogWrapper(
            logger,
            f"resubmit_request orig_request_id={orig_dc_req_spec.request_id} exclude_prev_dst={exclude_prev_dst} by={by}"
            + (f" reason={reason}" if reason else " "),
        )
        # initialized
        dc_req_spec_resubmitted = None
        err_msg = None
        # get lock
        with self.request_lock(orig_dc_req_spec.request_id) as locked_spec:
            if not locked_spec:
                # not getting lock; skip
                err_msg = "did not get lock; skipped"
                tmp_log.warning(err_msg)
                return None, err_msg
            # got locked spec
            orig_dc_req_spec = locked_spec
            # dummy spec to resubmit
            dummy_dc_req_spec_to_resubmit = get_resubmit_request_spec(orig_dc_req_spec, exclude_prev_dst)
            # check and choose availble destination RSE
            destination_rse = self._choose_destination_rse_for_request(dummy_dc_req_spec_to_resubmit)
            if destination_rse is None:
                err_msg = f"no other available destination RSE for request_id={orig_dc_req_spec.request_id}; skipped"
                tmp_log.warning(err_msg)
                return None, err_msg
            # resubmit
            dc_req_spec_resubmitted = self.taskBufferIF.resubmit_data_carousel_request_JEDI(orig_dc_req_spec.request_id, exclude_prev_dst)
            if dc_req_spec_resubmitted:
                new_request_id = dc_req_spec_resubmitted.request_id
                tmp_log.debug(f"resubmitted request_id={new_request_id}")
                # expire DDM rule of original request
                if orig_dc_req_spec.ddm_rule_id:
                    short_time = 5
                    self._refresh_ddm_rule(orig_dc_req_spec.ddm_rule_id, short_time)
                # stage the resubmitted request immediately without queuing
                is_ok, _, dc_req_spec_resubmitted = self.stage_request(
                    dc_req_spec_resubmitted, destination_rse=destination_rse, submit_idds_request=submit_idds_request
                )
                if is_ok:
                    tmp_log.debug(f"staged resubmitted request_id={new_request_id}")
                else:
                    err_msg = f"failed to stage resubmitted request_id={new_request_id}; skipped"
                    tmp_log.warning(err_msg)
            elif dc_req_spec_resubmitted is False:
                err_msg = f"request not found or not resubmittable; skipped"
                tmp_log.warning(err_msg)
            else:
                err_msg = f"failed to resubmit"
                tmp_log.error(err_msg)
        # return
        return dc_req_spec_resubmitted, err_msg

    # def _unset_ddm_rule_source_rse(self, rule_id: str) -> bool:
    #     """
    #     Unset source_replica_expression of a DDM rule

    #     Args:
    #         rule_id (str): DDM rule ID

    #     Returns:
    #         bool : True for success, False otherwise
    #     """
    #     set_map = {"source_replica_expression": None}
    #     ret = self.ddmIF.update_rule_by_id(rule_id, set_map)
    #     return ret

    def change_request_source_rse(
        self, dc_req_spec: DataCarouselRequestSpec, cancel_fts: bool = False, change_src_expr: bool = False, source_rse: str | None = None
    ) -> tuple[bool | None, DataCarouselRequestSpec, str | None]:
        """
        Change source RSE
        If the request is staging, unset source_replica_expression of its DDM rule
        If still queued, re-choose the source_rse to be another tape
        Skipped if the request is not staging or queued

        Args:
            dc_req_spec (DataCarouselRequestSpec): spec of the request
            cancel_fts (bool): whether to cancel current FTS requests on DDM
            change_src_expr (bool): whether to change source_replica_expression of the DDM rule by replacing old source with new one, instead of just dropping old source
            source_rse (str|None): if set, use this source RSE instead of choosing one randomly, also force change_src_expr to be True; default is None

        Returns:
            bool|None : True for success, False for failure, None if skipped
            DataCarouselRequestSpec|None: spec of the request after changing source
            str|None: error message if any, None otherwise
        """
        tmp_log = LogWrapper(logger, f"change_request_source_rse request_id={dc_req_spec.request_id}")
        if source_rse:
            change_src_expr = True
        ret = None
        err_msg = None
        rse_set = set()
        # re-choose source_rse for queued request
        active_source_rses_set = self._get_active_source_rses()
        dataset = dc_req_spec.dataset
        source_type, rse_set_orig, staging_rule, to_pin, suggested_dst_list = self._get_source_type_of_dataset(dataset, active_source_rses_set)
        replicas_map = self._get_full_replicas_per_type(dataset)
        # exclude original source RSE if possible
        if dc_req_spec.status == DataCarouselRequestStatus.staging and not rse_set_orig:
            # for already staging request, DDM rule already exists, choose source RSE from unfiltered tape replicas if no filtered RSE
            rse_set |= set(replicas_map["tape"])
        if rse_set_orig:
            rse_set |= rse_set_orig
        if (
            staging_rule
            and (source_replica_expression := staging_rule.get("source_replica_expression"))
            and source_replica_expression == f"{SRC_REPLI_EXPR_PREFIX}|{dc_req_spec.source_rse}"
        ):
            # exclude source RSE already in source_replica_expression
            rse_set.discard(dc_req_spec.source_rse)
        # check status of the request
        if dc_req_spec.status == DataCarouselRequestStatus.queued:
            # exclude old source RSE for queued request
            rse_set.discard(dc_req_spec.source_rse)
            if not rse_set:
                # no availible source RSE
                err_msg = f"dataset={dataset} has no other source RSE available; skipped"
                tmp_log.warning(err_msg)
                ret = False
            elif source_type == "datadisk":
                # replicas already on datadisk; skip
                err_msg = f"dataset={dataset} already has replica on datadisks {rse_set} ; skipped"
                tmp_log.debug(err_msg)
                ret = False
            elif source_type == "tape":
                # replicas only on tape
                new_source_rse = None
                if source_rse:
                    if source_rse not in rse_set:
                        # source_rse not in available RSEs
                        err_msg = f"dataset={dataset} source_rse={source_rse} not in available RSEs {rse_set} ; skipped"
                        tmp_log.warning(err_msg)
                        ret = False
                    else:
                        # use source_rse
                        new_source_rse = source_rse
                else:
                    # choose source RSE
                    tmp_log.debug(f"dataset={dataset} on tapes {rse_set} ; choosing one")
                    _, new_source_rse, ddm_rule_id = self._choose_tape_source_rse(dataset, rse_set, staging_rule)
                    if not new_source_rse:
                        # failed to choose source RSE
                        err_msg = f"dataset={dataset} failed to choose source RSE ; skipped"
                        tmp_log.warning(err_msg)
                        ret = False
                # fill new attributes for queued request
                if ret is not False and new_source_rse:
                    with self.request_lock(dc_req_spec.request_id) as locked_spec:
                        if not locked_spec:
                            # not getting lock; skip
                            err_msg = "did not get lock; skipped"
                            tmp_log.warning(err_msg)
                            return False, dc_req_spec, err_msg
                        # got locked spec
                        dc_req_spec = locked_spec
                        # fill attributes
                        dc_req_spec.source_rse = new_source_rse
                        dc_req_spec.ddm_rule_id = ddm_rule_id
                        dc_req_spec.source_tape = self._get_source_tape_from_rse(dc_req_spec.source_rse)
                        if to_pin:
                            dc_req_spec.set_parameter("to_pin", True)
                        if suggested_dst_list:
                            dc_req_spec.set_parameter("suggested_dst_list", suggested_dst_list)
                        # update DB
                        tmp_ret = self.taskBufferIF.update_data_carousel_request_JEDI(dc_req_spec)
                        if tmp_ret:
                            tmp_log.info(
                                f"updated DB about change source of queued request; source_rse={dc_req_spec.source_rse} , ddm_rule_id={dc_req_spec.ddm_rule_id} , to_pin={to_pin} , suggested_dst_list={suggested_dst_list}"
                            )
                            ret = True
                        else:
                            err_msg = f"failed to update DB ; skipped"
                            tmp_log.error(err_msg)
                            ret = False
            else:
                # no replica found on tape nor on datadisk; skip
                err_msg = f"dataset={dataset} has no replica on any tape or datadisk ; skipped"
                tmp_log.debug(err_msg)
                ret = False
        elif dc_req_spec.status == DataCarouselRequestStatus.staging:
            # unset source_replica_expression of DDM rule for staging request
            set_map = {"source_replica_expression": None}
            if change_src_expr:
                # change source_replica_expression by replacing old source with new one
                if not rse_set:
                    # no availible source RSE
                    err_msg = f"dataset={dataset} has no other source RSE available; skipped"
                    tmp_log.warning(err_msg)
                    ret = False
                else:
                    new_source_rse = None
                    if source_rse:
                        if source_rse not in rse_set:
                            # source_rse not in available RSEs
                            err_msg = f"dataset={dataset} source_rse={source_rse} not in available RSEs {rse_set} ; skipped"
                            tmp_log.warning(err_msg)
                            ret = False
                        else:
                            # use source_rse
                            new_source_rse = source_rse
                    else:
                        # choose source RSE
                        tmp_log.debug(f"dataset={dataset} on tapes {rse_set} ; choosing one")
                        new_source_rse = random.choice(list(rse_set))
                        if not new_source_rse:
                            # failed to choose source RSE
                            err_msg = f"dataset={dataset} failed to choose source RSE ; skipped"
                            tmp_log.warning(err_msg)
                            ret = False
                    if ret is not False and new_source_rse:
                        with self.request_lock(dc_req_spec.request_id) as locked_spec:
                            if not locked_spec:
                                # not getting lock; skip
                                err_msg = "did not get lock; skipped"
                                tmp_log.warning(err_msg)
                                return False, dc_req_spec, err_msg
                            else:
                                # got locked spec
                                dc_req_spec = locked_spec
                                # fill new attributes for staging request
                                dc_req_spec.source_rse = new_source_rse
                                dc_req_spec.source_tape = self._get_source_tape_from_rse(dc_req_spec.source_rse)
                                # update source_replica_expression
                                set_map["source_replica_expression"] = f"{SRC_REPLI_EXPR_PREFIX}|{dc_req_spec.source_rse}"
                                # update DB
                                tmp_ret = self.taskBufferIF.update_data_carousel_request_JEDI(dc_req_spec)
                                if tmp_ret is not None:
                                    tmp_log.info(
                                        f"updated DB about change source of staging request; source_rse={dc_req_spec.source_rse} , ddm_rule_id={dc_req_spec.ddm_rule_id}"
                                    )
                                    ret = True
                                else:
                                    err_msg = f"failed to update DB ; skipped"
                                    tmp_log.error(err_msg)
                                    ret = False
            # update DDM rule
            if ret is not False:
                tmp_ret = self.ddmIF.update_rule_by_id(dc_req_spec.ddm_rule_id, set_map)
                if tmp_ret:
                    tmp_log.debug(f"ddm_rule_id={dc_req_spec.ddm_rule_id} params={set_map} done")
                    ret = True
                else:
                    err_msg = f"ddm_rule_id={dc_req_spec.ddm_rule_id} params={set_map} failed to update DDM rule ; skipped"
                    tmp_log.error(f"ddm_rule_id={dc_req_spec.ddm_rule_id} params={set_map} got {tmp_ret}; skipped")
                    ret = False
            # cancel FTS requests in separate DDM rule update calls (otherwise DDM will not cancel FTS)
            if cancel_fts:
                cancel_fts_success = True
                # call first time to cancel requests
                _set_map = {"cancel_requests": True, "state": "STUCK"}
                tmp_ret = self.ddmIF.update_rule_by_id(dc_req_spec.ddm_rule_id, _set_map)
                if tmp_ret:
                    tmp_log.debug(f"ddm_rule_id={dc_req_spec.ddm_rule_id} params={_set_map} done")
                else:
                    tmp_log.warning(f"ddm_rule_id={dc_req_spec.ddm_rule_id} params={_set_map} got {tmp_ret} ; skipped")
                    cancel_fts_success = False
                # call second time to boost rule
                _set_map = {"boost_rule": True}
                tmp_ret = self.ddmIF.update_rule_by_id(dc_req_spec.ddm_rule_id, _set_map)
                if tmp_ret:
                    tmp_log.debug(f"ddm_rule_id={dc_req_spec.ddm_rule_id} params={_set_map} done")
                else:
                    tmp_log.warning(f"ddm_rule_id={dc_req_spec.ddm_rule_id} params={_set_map} got {tmp_ret} ; skipped")
                    cancel_fts_success = False
                if not cancel_fts_success:
                    err_msg = f"ddm_rule_id={dc_req_spec.ddm_rule_id} failed to cancel FTS requests ; skipped"
        return ret, dc_req_spec, err_msg
