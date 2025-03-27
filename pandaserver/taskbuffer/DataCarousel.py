import functools
import json
import random
import re
import traceback
from collections import namedtuple
from dataclasses import MISSING, InitVar, asdict, dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List

import idds.common.constants
import idds.common.utils
import polars as pl
from idds.client.client import Client as iDDS_Client
from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandautils.base import SpecBase
from pandacommon.pandautils.PandaUtils import naive_utcnow

from pandaserver.config import panda_config

logger = PandaLogger().getLogger(__name__.split(".")[-1])


# ==============================================================

# schema version of database config
DC_CONFIG_SCHEMA_VERSION = 1

# final task statuses
FINAL_TASK_STATUSES = ["done", "finished", "failed", "exhausted", "aborted", "toabort", "aborting", "broken", "tobroken"]

# named tuple for attribute with type
AttributeWithType = namedtuple("AttributeWithType", ["attribute", "type"])

# template strings
# source replica expression prefix
SRC_REPLI_EXPR_PREFIX = "rse_type=DISK"
# destination replica expression for datasets to pin (with replica on disk but without rule to stay on datadisk)
TO_PIN_DST_REPLI_EXPR = "type=DATADISK"

# DDM rule lifetime in day to keep
STAGING_LIFETIME_DAYS = 15
DONE_LIFETIME_DAYS = 30

# DDM rule refresh hard lifetime limits (refresh only if lifetime within the range)
TO_REFRESH_MAX_LIFETIME_DAYS = 7
TO_REFRESH_MIN_LIFETIME_HOURS = 2

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
            "reuse_rule" (boot): reuse DDM rule instead of submitting new one
            "resub_from" (int): resubmitted from this oringal request ID
            "prev_src" (str): previous source RSE
            "prev_dst" (str): previous destination RSE
            "excluded_dst_list" (list[str]): list of excluded destination RSEs
            "rule_unfound" (bool): DDM rule not found
            "to_pin" (bool): whether to pin the dataset

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


def get_resubmit_request_spec(dc_req_spec: DataCarouselRequestSpec) -> DataCarouselRequestSpec | None:
    """
    Get a new request spec to resubmit according to original request spec

    Args:
        dc_req_spec (DataCarouselRequestSpec): oringal spec of the request

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
        # orig_parameter_map = dc_req_spec.parameter_map
        # orig_excluded_dst_set = set(orig_parameter_map.get("excluded_dst_list", []))
        # TODO: mechanism to exclude problematic source or destination RSE (need approach to store historical datasets/RSEs)
        dc_req_spec_to_resubmit.parameter_map = {
            "resub_from": dc_req_spec.request_id,
            "prev_src": dc_req_spec.source_rse,
            "prev_dst": dc_req_spec.destination_rse,
            # "excluded_dst_list": list(orig_excluded_dst_set.add(dc_req_spec.destination_rse)),
            "excluded_dst_list": [dc_req_spec.destination_rse],  # default to be previous destination
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
    def __init__(self, taskbufferIF, ddmIF):
        # attributes
        self.taskBufferIF = taskbufferIF
        self.ddmIF = ddmIF
        self.tape_rses = []
        self.datadisk_rses = []
        self.disk_rses = []
        self.dc_config_map = None
        self._last_update_ts_dict = {}
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
                datadisk_rses = self.ddmIF.list_rses("type=DATADISK")
                if datadisk_rses is not None:
                    self.datadisk_rses = list(datadisk_rses)
                disk_rses = self.ddmIF.list_rses("rse_type=DISK")
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
                    tmp_log.error(f"got None from DB ; skipped")
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
                dataset_list = []
                if raw_dataset_str:
                    dataset_list = raw_dataset_str.split(",")
                for dataset in dataset_list:
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
        ds_repli_dict = self.ddmIF.convertOutListDatasetReplicas(dataset, skip_incomplete_element=True)
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
            dict : filtered replicas map
            str | None : staging rule, None if not existing
            bool: whether all replicas on datadisk are without rules
        """
        replicas_map = self._get_full_replicas_per_type(dataset)
        rules = self.ddmIF.list_did_rules(dataset, all_accounts=True)
        rse_expression_list = []
        staging_rule = None
        for rule in rules:
            if rule["account"] in ["panda"] and rule["activity"] == "Staging":
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
        return filtered_replicas_map, staging_rule, all_disk_repli_ruleless

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
            collection_meta = self.ddmIF.getDatasetMetaData(collection, ignore_missing=True)
            if collection_meta["state"] == "missing":
                # DID not found
                tmp_log.warning(f"DID not found")
                return None
            did_type = collection_meta["did_type"]
            if did_type == "CONTAINER":
                # is container, get datasets inside
                dataset_list = self.ddmIF.listDatasetsInContainer(collection)
                if dataset_list is None:
                    tmp_log.warning(f"cannot list datasets in this container")
                else:
                    ret_list = dataset_list
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

    def _get_source_type_of_dataset(self, dataset: str, active_source_rses_set: set | None = None) -> tuple[(str | None), (set | None), (str | None), bool]:
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
        """
        tmp_log = LogWrapper(logger, f"_get_source_type_of_dataset dataset={dataset}")
        try:
            # initialize
            source_type = None
            rse_set = None
            to_pin = False
            # get active source rses
            if active_source_rses_set is None:
                active_source_rses_set = self._get_active_source_rses()
            # get filtered replicas and staging rule of the dataset
            filtered_replicas_map, staging_rule, all_disk_repli_ruleless = self._get_filtered_replicas(dataset)
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
            else:
                # no replica found on tape nor on datadisk (can be on transient disk); skip
                pass
            # return
            return (source_type, rse_set, staging_rule, to_pin)
        except Exception as e:
            # other unexpected errors
            raise e

    def _choose_tape_source_rse(self, dataset: str, rse_set: set, staging_rule) -> tuple[str, (str | None), (str | None)]:
        """
        Choose a TAPE source RSE
        If with exsiting staging rule, then get source RSE from it

        Args:
            dataset (str): dataset name
            rse_set (set): set of TAPE source RSE set to choose from
            staging_rule: DDM staging rule

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
            # whether with existing staging rule
            if staging_rule:
                # with existing staging rule ; prepare to reuse it
                ddm_rule_id = staging_rule["id"]
                # extract source RSE from rule
                source_replica_expression = staging_rule["source_replica_expression"]
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
                # keep alive the rule
                if (rule_expiration_time := staging_rule["expires_at"]) and (rule_expiration_time - naive_utcnow()) < timedelta(days=DONE_LIFETIME_DAYS):
                    self._refresh_ddm_rule(ddm_rule_id, 86400 * DONE_LIFETIME_DAYS)
                    tmp_log.debug(f"ddm_rule_id={ddm_rule_id} refreshed rule to be {DONE_LIFETIME_DAYS} days long")
            else:
                # no existing staging rule ; prepare info for new submission
                rse_list = list(rse_set)
                # choose source RSE
                if len(rse_list) == 1:
                    source_rse = rse_list[0]
                else:
                    non_CERN_rse_list = [rse for rse in rse_list if "CERN-PROD" not in rse]
                    if non_CERN_rse_list:
                        source_rse = random.choice(non_CERN_rse_list)
                    else:
                        source_rse = random.choice(rse_list)
                tmp_log.debug(f"chose source_rse={source_rse}")
            # add to prestage
            return (dataset, source_rse, ddm_rule_id)
        except Exception as e:
            # other unexpected errors
            raise e

    @refresh
    def get_input_datasets_to_prestage(self, task_id: int, task_params_map: dict, dsname_list: list | None = None) -> tuple[list, dict]:
        """
        Get the input datasets, their source RSEs (tape) of the task which need pre-staging from tapes, and DDM rule ID of existing DDM rule

        Args:
            task_id (int): JEDI task ID of the task params
            task_params_map (dict): task params of the JEDI task
            dsname_list (list|None): if not None, filter only datasets in this list of dataset names to stage

        Returns:
            list[tuple[str, str|None, str|None]]: list of tuples in the form of (dataset, source_rse, ddm_rule_id)
            dict[str|list]: dict of list of datasets, including pseudo inputs (meant to be marked as no_staging), already on datadisk (meant to be marked as no_staging), only on tape, and not found
        """
        tmp_log = LogWrapper(logger, f"get_input_datasets_to_prestage task_id={task_id}")
        try:
            # initialize
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
                "unfound_ds_list": [],
            }
            # get active source rses
            active_source_rses_set = self._get_active_source_rses()
            # loop over inputs
            input_collection_map = self._get_input_ds_from_task_params(task_params_map)
            for collection, job_param in input_collection_map.items():
                # pseudo inputs
                if job_param.get("param_type") == "pseudo_input":
                    ret_map["pseudo_coll_list"].append(collection)
                    tmp_log.debug(f"collection={collection} is pseudo input ; skipped")
                    continue
                # with real inputs
                dataset_list = self._get_datasets_from_collection(collection)
                if dataset_list is None:
                    ret_map["unfound_coll_list"].append(collection)
                    tmp_log.warning(f"collection={collection} not found ; skipped")
                    continue
                elif not dataset_list:
                    ret_map["empty_coll_list"].append(collection)
                    tmp_log.warning(f"collection={collection} is empty ; skipped")
                    continue
                # check source of each dataset
                got_on_tape = False
                for dataset in dataset_list:
                    # check if dataset in the required dsname_list
                    if dsname_list is not None and dataset not in dsname_list:
                        # not in dsname_list; skip
                        ret_map["to_skip_ds_list"].append(dataset)
                        tmp_log.debug(f"dataset={dataset} not in dsname_list ; skipped")
                        continue
                    # get source type and RSEs
                    source_type, rse_set, staging_rule, to_pin = self._get_source_type_of_dataset(dataset, active_source_rses_set)
                    if source_type == "datadisk":
                        # replicas already on datadisk; skip
                        ret_map["datadisk_ds_list"].append(dataset)
                        tmp_log.debug(f"dataset={dataset} already has replica on datadisks {list(rse_set)} ; skipped")
                        continue
                    elif source_type == "tape":
                        # replicas only on tape
                        got_on_tape = True
                        ret_map["tape_ds_list"].append(dataset)
                        tmp_log.debug(f"dataset={dataset} on tapes {list(rse_set)} ; choosing one")
                        # choose source RSE
                        _, source_rse, ddm_rule_id = self._choose_tape_source_rse(dataset, rse_set, staging_rule)
                        prestaging_tuple = (dataset, source_rse, ddm_rule_id, to_pin)
                        tmp_log.debug(f"got prestaging: {prestaging_tuple}")
                        # add to prestage
                        ret_prestaging_list.append(prestaging_tuple)
                    else:
                        # no replica found on tape nor on datadisk; skip
                        ret_map["unfound_ds_list"].append(dataset)
                        tmp_log.debug(f"dataset={dataset} has no replica on any tape or datadisk ; skipped")
                        continue
                # collection DID without datasets on tape
                collection_did = self.ddmIF.get_did_str(collection)
                if got_on_tape:
                    ret_map["tape_coll_did_list"].append(collection_did)
                else:
                    ret_map["no_tape_coll_did_list"].append(collection_did)
            # return
            tmp_log.debug(f"got {len(ret_prestaging_list)} input datasets to prestage")
            return ret_prestaging_list, ret_map
        except Exception as e:
            tmp_log.error(f"got error ; {traceback.format_exc()}")
            raise e

    def submit_data_carousel_requests(self, task_id: int, prestaging_list: list[tuple[str, str | None, str | None]]) -> bool | None:
        """
        Submit data carousel requests for a task

        Args:
            task_id (int): JEDI task ID
            prestaging_list (list[tuple[str, str|None, str|None, bool]]): list of tuples in the form of (dataset, source_rse, ddm_rule_id)

        Returns:
            bool | None : True if submission successful, or None if failed
        """
        tmp_log = LogWrapper(logger, f"submit_data_carousel_requests task_id={task_id}")
        n_req_to_submit = len(prestaging_list)
        tmp_log.debug(f"to submit {len(prestaging_list)} requests")
        # fill dc request spec for each input dataset
        dc_req_spec_list = []
        now_time = naive_utcnow()
        for dataset, source_rse, ddm_rule_id, to_pin in prestaging_list:
            dc_req_spec = DataCarouselRequestSpec()
            dc_req_spec.dataset = dataset
            dataset_meta = self.ddmIF.getDatasetMetaData(dataset)
            dc_req_spec.total_files = dataset_meta["length"]
            dc_req_spec.dataset_size = dataset_meta["bytes"]
            dc_req_spec.staged_files = 0
            dc_req_spec.staged_size = 0
            dc_req_spec.ddm_rule_id = ddm_rule_id
            dc_req_spec.source_rse = source_rse
            try:
                # source_rse is RSE
                source_tape = self.dc_config_map.source_rses_config[dc_req_spec.source_rse].tape
            except KeyError:
                # source_rse is physical tape
                source_tape = dc_req_spec.source_rse
            finally:
                dc_req_spec.source_tape = source_tape
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
        # get source tapes and RSEs config dataframes
        source_tapes_config_df = self._get_source_tapes_config_dataframe()
        # source_rses_config_df = self._get_source_rses_config_dataframe()
        # dataframe of staging requests with physical tapes
        # dc_req_full_df = dc_req_df.join(source_rses_config_df.select("source_rse", "tape"), on="source_rse", how="left")
        dc_req_full_df = dc_req_df
        # dataframe of source RSE stats; add remaining_files
        source_rse_stats_df = (
            dc_req_full_df.select(
                "source_tape",
                "total_files",
                "staged_files",
                (pl.col("total_files") - pl.col("staged_files")).alias("remaining_files"),
            )
            .group_by("source_tape")
            .sum()
        )
        # make dataframe of source tapes stats; add quota_size
        df = source_tapes_config_df.join(source_rse_stats_df, on="source_tape", how="left")
        df = df.with_columns(
            pl.col("total_files").fill_null(strategy="zero"),
            pl.col("staged_files").fill_null(strategy="zero"),
            pl.col("remaining_files").fill_null(strategy="zero"),
        )
        df = df.with_columns(quota_size=(pl.col("max_size") - pl.col("remaining_files")))
        # return final dataframe
        source_tape_stats_df = df
        return source_tape_stats_df

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
                    "userName": task_spec.userName,
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
                "userName": pl.datatypes.String,
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
        # join to add phycial tape
        # df = df.join(source_rses_config_df.select("source_rse", "tape"), on="source_rse", how="left")
        # return final dataframe
        queued_requests_tasks_df = df
        return queued_requests_tasks_df

    @refresh
    def get_requests_to_stage(self, *args, **kwargs) -> list[DataCarouselRequestSpec]:
        """
        Get the queued requests which should proceed to get staging

        Args:
            ? (?): ?

        Returns:
            list[DataCarouselRequestSpec] : list of requests to stage
        """
        tmp_log = LogWrapper(logger, "get_requests_to_stage")
        ret_list = []
        queued_requests = self.taskBufferIF.get_data_carousel_queued_requests_JEDI()
        if queued_requests is None:
            return ret_list
        # get stats of tapes
        source_tape_stats_df = self._get_source_tape_stats_dataframe()
        source_tape_stats_dict_list = source_tape_stats_df.to_dicts()
        # map of request_id and dc_req_spec of queued requests
        request_id_spec_map = {dc_req_spec.request_id: dc_req_spec for dc_req_spec, _ in queued_requests}
        # get dataframe of queued requests and tasks
        queued_requests_tasks_df = self._queued_requests_tasks_to_dataframe(queued_requests)
        # sort queued requests : by to_pin, gshare_rank, task_priority, jediTaskID, request_id
        df = queued_requests_tasks_df.sort(
            ["to_pin", "gshare_rank", "task_priority", "jediTaskID", "request_id"], descending=[True, False, True, False, False], nulls_last=True
        )
        # get unique requests with the sorted order
        df = df.unique(subset=["request_id"], keep="first", maintain_order=True)
        # evaluate per tape
        queued_requests_df = df
        for source_tape_stats_dict in source_tape_stats_dict_list:
            source_tape = source_tape_stats_dict["source_tape"]
            quota_size = source_tape_stats_dict["quota_size"]
            # dataframe of the phycial tape
            tmp_df = queued_requests_df.filter(pl.col("source_tape") == source_tape)
            # split with to_pin and not to_pin
            to_pin_df = tmp_df.filter(pl.col("to_pin"))
            tmp_queued_df = tmp_df.filter(pl.col("to_pin").not_())
            # fill dummy cumulative sum (0) for reqeusts to pin
            to_pin_df = to_pin_df.with_columns(cum_total_files=pl.lit(0, dtype=pl.datatypes.Int64), cum_dataset_size=pl.lit(0, dtype=pl.datatypes.Int64))
            # get cumulative sum of queued files per physical tape
            tmp_queued_df = tmp_queued_df.with_columns(cum_total_files=pl.col("total_files").cum_sum(), cum_dataset_size=pl.col("dataset_size").cum_sum())
            # number of queued requests at the physical tape
            n_queued = len(tmp_queued_df)
            n_to_pin = len(to_pin_df)
            n_total = n_queued + n_to_pin
            # print dataframe in log
            if n_total:
                tmp_to_print_df = pl.concat([to_pin_df, tmp_queued_df])
                tmp_to_print_df = tmp_to_print_df.select(
                    ["request_id", "source_rse", "jediTaskID", "gshare", "gshare_rank", "task_priority", "total_files", "cum_total_files", "to_pin"]
                )
                tmp_to_print_df = tmp_to_print_df.with_columns(gshare_and_rank=pl.concat_str([pl.col("gshare"), pl.col("gshare_rank")], separator=" : "))
                tmp_to_print_df = tmp_to_print_df.select(
                    ["request_id", "source_rse", "jediTaskID", "gshare_and_rank", "task_priority", "total_files", "cum_total_files", "to_pin"]
                )
                tmp_log.debug(f"  source_tape={source_tape} , quota_size={quota_size} : \n{tmp_to_print_df}")
            # filter requests to respect the tape quota size; at most one request can reach or exceed quota size
            to_stage_df = pl.concat(
                [tmp_queued_df.filter(pl.col("cum_total_files") < quota_size), tmp_queued_df.filter(pl.col("cum_total_files") >= quota_size).head(1)]
            )
            # append the requests to ret_list
            to_pin_request_id_list = to_pin_df.select(["request_id"]).to_dict(as_series=False)["request_id"]
            to_stage_request_id_list = to_stage_df.select(["request_id"]).to_dict(as_series=False)["request_id"]
            for request_id in to_pin_request_id_list:
                dc_req_spec = request_id_spec_map.get(request_id)
                if dc_req_spec:
                    ret_list.append(dc_req_spec)
            to_stage_count = 0
            for request_id in to_stage_request_id_list:
                dc_req_spec = request_id_spec_map.get(request_id)
                if dc_req_spec:
                    ret_list.append(dc_req_spec)
                    to_stage_count += 1
            if n_total:
                tmp_log.debug(f"source_tape={source_tape} got {to_stage_count}/{n_queued} requests to stage, {n_to_pin} requests to pin")
        tmp_log.debug(f"totally got {len(ret_list)} requests to stage or to pin")
        # return
        return ret_list

    def _submit_ddm_rule(self, dc_req_spec: DataCarouselRequestSpec) -> str | None:
        """
        Submit DDM replication rule to stage the dataset of the request

        Args:
            dc_req_spec (DataCarouselRequestSpec): spec of the request

        Returns:
            str | None : DDM rule_id of the new rule if submission successful, or None if failed
        """
        tmp_log = LogWrapper(logger, f"_submit_ddm_rule request_id={dc_req_spec.request_id}")
        # initialize
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
            return
        # get source physical tape
        try:
            # source_rse is RSE
            source_tape = self.dc_config_map.source_rses_config[dc_req_spec.source_rse].tape
        except KeyError:
            # source_rse is physical tape
            source_tape = dc_req_spec.source_rse
        except Exception:
            # other unexpected errors
            tmp_log.error(f"got error ; {traceback.format_exc()}")
            return
        # parameters about this tape source from DC config
        try:
            source_tape_config = self.dc_config_map.source_tapes_config[source_tape]
        except (KeyError, AttributeError):
            # no destination_expression for this tape; skipped
            tmp_log.warning(f"failed to get destination_expression from config; skipped ; {traceback.format_exc()}")
            return
        except Exception:
            # other unexpected errors
            tmp_log.error(f"got error ; {traceback.format_exc()}")
            return
        # destination expression
        if dc_req_spec.get_parameter("to_pin"):
            # to pin; use the simple to pin destination
            tmp_log.debug(f"has to_pin")
            expression = TO_PIN_DST_REPLI_EXPR
        else:
            # destination_expression from DC config
            expression = source_tape_config.destination_expression
        # adjust destination_expression according to excluded_dst_list
        if excluded_dst_list := dc_req_spec.get_parameter("excluded_dst_list"):
            for excluded_dst_rse in excluded_dst_list:
                expression += f"\\{excluded_dst_rse}"
        # submit ddm staging rule
        ddm_rule_id = self.ddmIF.make_staging_rule(
            dataset_name=dc_req_spec.dataset,
            expression=expression,
            activity="Staging",
            lifetime=lifetime,
            weight=weight,
            notify="P",
            source_replica_expression=source_replica_expression,
        )
        # return
        return ddm_rule_id

    @refresh
    def stage_request(self, dc_req_spec: DataCarouselRequestSpec) -> bool:
        """
        Stage the dataset of the request and update request status to staging

        Args:
            dc_req_spec (DataCarouselRequestSpec): spec of the request

        Returns:
            bool : True for success, False otherwise
        """
        tmp_log = LogWrapper(logger, f"stage_request request_id={dc_req_spec.request_id}")
        is_ok = False
        # check existing DDM rule of the dataset
        if (ddm_rule_id := dc_req_spec.ddm_rule_id) is not None:
            # DDM rule exists; no need to submit
            tmp_log.debug(f"dataset={dc_req_spec.dataset} already has active DDM rule ddm_rule_id={ddm_rule_id}")
        else:
            # no existing rule; submit DDM rule
            ddm_rule_id = self._submit_ddm_rule(dc_req_spec)
            if ddm_rule_id:
                # DDM rule submitted; update ddm_rule_id
                dc_req_spec.ddm_rule_id = ddm_rule_id
                tmp_log.debug(f"submitted DDM rule ddm_rule_id={ddm_rule_id}")
            else:
                # failed to submit
                tmp_log.warning(f"failed to submitted DDM rule ; skipped")
                return is_ok
        # update request to be staging
        now_time = naive_utcnow()
        dc_req_spec.status = DataCarouselRequestStatus.staging
        dc_req_spec.start_time = now_time
        ret = self.taskBufferIF.update_data_carousel_request_JEDI(dc_req_spec)
        if ret is not None:
            tmp_log.info(f"updated DB about staging; status={dc_req_spec.status}")
            dc_req_spec = ret
            is_ok = True
        # return
        return is_ok

    def cancel_request(self, request_id: int, by: str = "manual", reason: str | None = None) -> bool | None:
        """
        Cancel a request

        Args:
            request_id (int): reqeust_id of the request to cancel
            by (str): annotation of the caller of this method; default is "manual"
            reason (str|None): annotation of the reason for cancelling

        Returns:
            bool|None : True for success, None otherwise
        """
        tmp_log = LogWrapper(logger, f"cancel_request request_id={request_id} by={by}" + (f" reason={reason}" if reason else " "))
        # cancel
        ret = self.taskBufferIF.cancel_data_carousel_request_JEDI(request_id)
        if ret:
            tmp_log.debug(f"cancelled")
        elif ret == 0:
            tmp_log.debug(f"already terminated; skipped")
        else:
            tmp_log.error(f"failed to cancel")
        # return
        return ret

    def retire_request(self, request_id: int, by: str = "manual", reason: str | None = None) -> bool | None:
        """
        Retire a done request so that it will not be reused

        Args:
            request_id (int): reqeust_id of the request to retire
            by (str): annotation of the caller of this method; default is "manual"
            reason (str|None): annotation of the reason for retiring

        Returns:
            bool|None : True for success, None otherwise
        """
        tmp_log = LogWrapper(logger, f"retire_request request_id={request_id} by={by}" + (f" reason={reason}" if reason else " "))
        # cancel
        ret = self.taskBufferIF.retire_data_carousel_request_JEDI(request_id)
        if ret:
            tmp_log.debug(f"retired")
        elif ret == 0:
            tmp_log.debug(f"cannot retire; skipped")
        else:
            tmp_log.error(f"failed to retire")
        # return
        return ret

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
        # get DDM rule
        ddm_rule_id = dc_req_spec.ddm_rule_id
        the_rule = self.ddmIF.get_rule_by_id(ddm_rule_id)
        if the_rule is False:
            # rule not found
            dc_req_spec.set_parameter("rule_unfound", True)
            tmp_log.error(f"ddm_rule_id={ddm_rule_id} rule not found")
            tmp_ret = self.taskBufferIF.update_data_carousel_request_JEDI(dc_req_spec)
            if tmp_ret is not None:
                tmp_log.debug(f"updated DB about rule not found")
            else:
                tmp_log.error(f"failed to update DB ; skipped")
            # try to cancel or retire request
            if dc_req_spec.status == DataCarouselRequestStatus.staging:
                # requests staging but DDM rule not found; to cancel
                self.cancel_request(dc_req_spec.request_id, by=by, reason="rule_unfound")
            elif dc_req_spec.status == DataCarouselRequestStatus.done:
                # requests done but DDM rule not found; to retire
                self.retire_request(dc_req_spec.request_id, by=by, reason="rule_unfound")
            return ret
        elif the_rule is None:
            # got error when getting the rule
            tmp_log.error(f"failed to get rule of ddm_rule_id={ddm_rule_id} ; skipped")
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
        Keep alive DDM rules of requests of active tasks

        Args:
            by (str): annotation of the caller of this method; default is "watchdog"
        """
        tmp_log = LogWrapper(logger, "keep_alive_ddm_rules")
        # get requests and relations of active tasks
        ret_requests_map, ret_relation_map = self.taskBufferIF.get_data_carousel_requests_by_task_status_JEDI(status_exclusion_list=FINAL_TASK_STATUSES)
        for dc_req_spec in ret_requests_map.values():
            try:
                if dc_req_spec.status not in [DataCarouselRequestStatus.staging, DataCarouselRequestStatus.done]:
                    # skip requests without need to keep rules alive
                    continue
                # decide lifetime in days
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
            lfn_set = self.ddmIF.getFilesInDataset(dataset, ignoreUnknown=True, lfn_only=True)
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
                    ret = self.taskBufferIF.updateInputFilesStaged_JEDI(task_id, scope, filenames_dict, by="DataCarousel")
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
                    # rule not found
                    dc_req_spec.set_parameter("rule_unfound", True)
                    tmp_log.error(f"request_id={dc_req_spec.request_id} ddm_rule_id={ddm_rule_id} rule not found")
                    tmp_ret = self.taskBufferIF.update_data_carousel_request_JEDI(dc_req_spec)
                    if tmp_ret is not None:
                        tmp_log.debug(f"request_id={dc_req_spec.request_id} updated DB about rule not found")
                    else:
                        tmp_log.error(f"request_id={dc_req_spec.request_id} failed to update DB ; skipped")
                    # try to cancel or retire request
                    if dc_req_spec.status == DataCarouselRequestStatus.staging:
                        # requests staging but DDM rule not found; to cancel
                        self.cancel_request(dc_req_spec.request_id, by="watchdog", reason="rule_unfound")
                    elif dc_req_spec.status == DataCarouselRequestStatus.done:
                        # requests done but DDM rule not found; to retire
                        self.retire_request(dc_req_spec.request_id, by="watchdog", reason="rule_unfound")
                    continue
                elif the_rule is None:
                    # got error when getting the rule
                    tmp_log.error(f"request_id={dc_req_spec.request_id} failed to get rule of ddm_rule_id={ddm_rule_id} ; skipped")
                    continue
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
                current_staged_files = int(the_rule["locks_ok_cnt"])
                new_staged_files = current_staged_files - dc_req_spec.staged_files
                if new_staged_files > 0:
                    # have more staged files than before; update request according to DDM rule
                    dc_req_spec.staged_files = current_staged_files
                    dc_req_spec.staged_size = int(dc_req_spec.dataset_size * dc_req_spec.staged_files / dc_req_spec.total_files)
                    to_update = True
                else:
                    tmp_log.debug(f"request_id={dc_req_spec.request_id} got {new_staged_files} new staged files")
                # check completion of staging
                if dc_req_spec.staged_files == dc_req_spec.total_files:
                    # all files staged; process request to done
                    now_time = naive_utcnow()
                    dc_req_spec.status = DataCarouselRequestStatus.done
                    dc_req_spec.end_time = now_time
                    dc_req_spec.staged_size = dc_req_spec.dataset_size
                    to_update = True
                # update to DB if attribute updated
                if to_update:
                    ret = self.taskBufferIF.update_data_carousel_request_JEDI(dc_req_spec)
                    if ret is not None:
                        tmp_log.info(
                            f"request_id={dc_req_spec.request_id} got {new_staged_files} new staged files; updated DB about staging ; status={dc_req_spec.status}"
                        )
                        dc_req_spec = ret
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
                _, task_spec = self.taskBufferIF.getTaskWithID_JEDI(task_id, fullFlag=False)
                if not task_spec:
                    # task not found
                    tmp_log.error(f"task_id={task_id} task not found; skipped")
                    continue
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
                    if dc_req_spec.ddm_rule_id and dc_req_spec.status in [DataCarouselRequestStatus.staging, DataCarouselRequestStatus.done]:
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
                ):
                    # requests done and old enough or done but DDM rule not found; to clean up
                    done_requests_set.add(request_id)
                elif dc_req_spec.status == DataCarouselRequestStatus.staging:
                    # requests staging while related tasks all terminated; to cancel (to clean up in next cycle)
                    self.cancel_request(request_id, by=by, reason="staging_while_all_tasks_ended")
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
                    self.cancel_request(request_id, by=by, reason="rule_unfound")
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
                        tmp_log.debug(f"deleted {ret} done requests older than {done_age_limit_days} days or rule not found")
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

    def resubmit_request(self, request_id: int, submit_idds_request=True, by: str = "manual", reason: str | None = None) -> DataCarouselRequestSpec | None:
        """
        Resubmit a request by ending the old request and submitting a new request
        The request status must be in staging, done, cancelled, retired
        A staging request will be cancelled, a done request will be retired. Cancelled and retired requests are intact
        Return the spec of newly resubmitted request

        Args:
            request_id (int): request_id of the request to resubmit from
            submit_idds_request (bool): whether to submit corresponding iDDS request; default is True
            by (str): annotation of the caller of this method; default is "manual"
            reason (str|None): annotation of the reason for resubmitting

        Returns:
            DataCarouselRequestSpec|None : spec of the resubmitted reqeust spec if success, None otherwise
        """
        tmp_log = LogWrapper(logger, f"resubmit_request orig_request_id={request_id} by={by}" + (f" reason={reason}" if reason else " "))
        # resubmit
        dc_req_spec_resubmitted = self.taskBufferIF.resubmit_data_carousel_request_JEDI(request_id)
        if dc_req_spec_resubmitted:
            new_request_id = dc_req_spec_resubmitted.request_id
            tmp_log.debug(f"resubmitted request_id={new_request_id}")
            if submit_idds_request:
                # to submit iDDS staging requests
                # get all tasks related to this request
                task_id_list = self._get_related_tasks(new_request_id)
                if task_id_list:
                    tmp_log.debug(f"related tasks: {task_id_list}")
                    for task_id in task_id_list:
                        self._submit_idds_stagein_request(task_id, dc_req_spec_resubmitted)
                    tmp_log.debug(f"submitted corresponding iDDS requests for related tasks")
                else:
                    tmp_log.warning(f"failed to get related tasks; skipped to submit iDDS requests")
        elif dc_req_spec_resubmitted is False:
            tmp_log.warning(f"request not found or not resubmittable; skipped")
        else:
            tmp_log.error(f"failed to resubmit")
        # return
        return dc_req_spec_resubmitted
