"""
find candidate site to distribute input datasets

"""
import datetime
import fnmatch
import re
import sys
import time
import uuid
from typing import Dict, List, Tuple

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandalogger.PandaLogger import PandaLogger

from pandaserver.dataservice.DataServiceUtils import select_scope
from pandaserver.dataservice.ddm import rucioAPI
from pandaserver.taskbuffer import JobUtils

_logger = PandaLogger().getLogger("dyn_data_distributer")

# files in datasets
g_files_in_ds_map = {}


class DynDataDistributer:
    """
    Find candidate site to distribute input datasets.
    """

    def __init__(self, jobs, siteMapper, simul=False, token=None):
        self.jobs = jobs
        self.site_mapper = siteMapper
        if token is None:
            self.token = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None).isoformat(" ")
        else:
            self.token = token
        # use a fixed list since some clouds don't have active T2s
        self.simul = simul
        self.last_message = ""

    def get_replica_locations(self, input_ds: str, check_used_file: bool) -> Tuple[bool, Dict]:
        """
        Get replica locations for a given dataset.

        Args:
            input_ds (str): The name of the input dataset.
            check_used_file (bool): Flag to check used file.

        Returns:
            tuple: A tuple containing the status (bool) and the result (dict or str).
        """
        tmp_log = LogWrapper(_logger, f"get_replica_locations-{datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None).isoformat('/')}")
        tmp_log.debug(f"get_replica_locations {input_ds}")

        # return for failure
        res_for_failure = False, {"": {"": ([], [], [], 0, False, False, 0, 0, [])}}

        # get replica locations
        if input_ds.endswith("/"):
            # container
            status, tmp_rep_maps = self.get_list_dataset_replicas_in_container(input_ds)
            # get used datasets
            if status and check_used_file:
                status, tmp_used_dataset_list = self.get_used_datasets(tmp_rep_maps)
                # remove unused datasets
                new_rep_maps = {}
                for tmp_key, tmp_val in tmp_rep_maps.items():
                    if tmp_key in tmp_used_dataset_list:
                        new_rep_maps[tmp_key] = tmp_val
                tmp_rep_maps = new_rep_maps
        else:
            # normal dataset
            status, tmp_rep_map = self.get_list_dataset_replicas(input_ds)
            tmp_rep_maps = {input_ds: tmp_rep_map}

        if not status:
            # failed
            tmp_log.error("failed to get replica locations for {input_ds}")
            tmp_log.debug("end")
            return res_for_failure

        tmp_log.debug("end")
        return True, tmp_rep_maps

    def get_all_sites(self) -> List:
        """
        Retrieves all sites that meet certain conditions.

        This method filters out sites based on the following conditions:
        - The site should be capable of running analysis.
        - The site should not be a GPU site.
        - The site should not use VP.
        - The status of the site should be "online".

        Returns:
            list: A list of SiteSpec objects that meet the above conditions.
        """
        all_sites = []
        for site_name in self.site_mapper.siteSpecList:
            site_spec = self.site_mapper.siteSpecList[site_name]
            # analysis only
            if not site_spec.runs_analysis():
                continue
            # skip GPU
            if site_spec.isGPU():
                continue
            # skip VP
            if site_spec.use_vp(JobUtils.ANALY_PS):
                continue
            # online
            if site_spec.status not in ["online"]:
                continue
            all_sites.append(site_spec)
        return all_sites

    def get_candidate_sites(self, tmp_rep_maps: Dict, prod_source_label: str, job_label: str) -> Tuple[bool, Dict]:
        """
        Retrieves candidate sites for data distribution based on certain conditions.

        This method filters out candidate sites based on the following condition:
        - The site should have a replica of the dataset.

        Args:
            tmp_rep_maps (dict): A dictionary containing dataset names as keys and their replica maps as values.
            prod_source_label (str): The label of the production source.
            job_label (str): The label of the job.

        Returns:
            tuple: A tuple containing a boolean status and a dictionary. The dictionary has dataset names as keys and
                   another dictionary as values. The inner dictionary has cloud names as keys and a tuple of various
                   site-related lists and values as values.
        """
        all_site_map = self.get_all_sites()
        return_map = {}
        cloud = "WORLD"
        for tmp_ds, tmp_rep_map in tmp_rep_maps.items():
            cand_sites = []
            sites_com_ds = []
            sites_comp_pd2p = []
            t1_has_replica = False
            t1_has_primary = False
            n_sec_replicas = 0
            cand_for_mou = []
            n_user_sub = 0
            for tmp_site_spec in all_site_map:
                tmp_scope_input, tmp_scope_output = select_scope(tmp_site_spec, prod_source_label, job_label)
                if tmp_scope_input not in tmp_site_spec.ddm_endpoints_input:
                    continue
                rses = tmp_site_spec.ddm_endpoints_input[tmp_scope_input].getLocalEndPoints()
                for ddm_endpoint in tmp_rep_map:
                    tmp_stat_map = tmp_rep_map[ddm_endpoint]
                    if ddm_endpoint in rses and tmp_stat_map[0]["total"] == tmp_stat_map[0]["found"] and ddm_endpoint.endswith("DATADISK"):
                        sites_com_ds.append(tmp_site_spec.sitename)
                        break
                cand_sites.append(tmp_site_spec.sitename)
            return_map.setdefault(tmp_ds, {})
            if sites_com_ds:
                cand_sites = sites_com_ds
            return_map[tmp_ds][cloud] = (
                cand_sites,
                sites_com_ds,
                sites_comp_pd2p,
                n_user_sub,
                t1_has_replica,
                t1_has_primary,
                n_sec_replicas,
                0,
                cand_for_mou,
            )
        return True, return_map

    def get_candidates(self, input_ds: str, prod_source_label: str, job_label: str, check_used_file: bool = True) -> Tuple[bool, Dict]:
        """
        Get candidate sites for subscription.

        Args:
            input_ds (str): The name of the input dataset.
            prod_source_label (str): The label of the production source.
            job_label (str): The label of the job.
            check_used_file (bool, optional): Flag to check used file. Defaults to True.

        Returns:
            tuple: A tuple containing the status (bool) and the result (dict or str).
        """
        # Get replica locations
        status, tmp_rep_maps = self.get_replica_locations(input_ds, check_used_file)
        if not status:
            return status, tmp_rep_maps

        # Get candidate sites
        return self.get_candidate_sites(tmp_rep_maps, prod_source_label, job_label)

    def get_list_dataset_replicas(self, dataset: str, max_attempts: int = 3) -> Tuple[bool, Dict]:
        """
        Get the list of replicas for a given dataset.

        Args:
            dataset (str): The name of the dataset.
            max_attempts (int, optional): The maximum number of attempts to get the replicas. Defaults to 3.

        Returns:
            tuple: A tuple containing the status (bool) and the result (dict or str).
        """
        tmp_log = LogWrapper(_logger, f"get_list_dataset_replicas-{datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None).isoformat('/')}")
        tmp_log.debug(f"get_list_dataset_replicas {dataset}")

        for attempt in range(max_attempts):
            tmp_log.debug(f"{attempt}/{max_attempts} listDatasetReplicas {dataset}")
            status, replicas = rucioAPI.list_dataset_replicas(dataset)
            if status == 0:
                tmp_log.debug(f"get_list_dataset_replicas->{str(replicas)}")
                tmp_log.debug("end")
                return True, replicas
            time.sleep(10)

        tmp_log.error(f"bad response for {dataset}")
        tmp_log.debug("end")
        return False, {}

    def get_list_dataset_replicas_in_container(self, container: str, max_attempts: int = 3) -> Tuple[bool, Dict]:
        """
        Get the list of replicas for a given container.

        Args:
            container (str): The name of the container.
            max_attempts (int, optional): The maximum number of attempts to get the replicas. Defaults to 3.

        Returns:
            tuple: A tuple containing the status (bool) and the result (dict or str).
        """
        tmp_log = LogWrapper(
            _logger, f"get_list_dataset_replicas_in_container-{datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None).isoformat('/')}"
        )

        tmp_log.debug(f"get_list_dataset_replicas_in_container {container}")

        # response for failure
        res_for_failure = False, {}

        # get datasets in container
        for attempt in range(max_attempts):
            tmp_log.debug(f"{attempt}/{max_attempts} listDatasetsInContainer {container}")
            datasets, out = rucioAPI.list_datasets_in_container(container)
            if datasets is not None:
                break
            time.sleep(60)

        if datasets is None:
            tmp_log.error(out)
            tmp_log.error(f"bad DDM response for {container}")
            tmp_log.debug("end")
            return res_for_failure

        # loop over all datasets
        all_rep_map = {}
        for dataset in datasets:
            # get replicas
            status, tmp_rep_sites = self.get_list_dataset_replicas(dataset)
            if not status:
                tmp_log.debug("end")
                return res_for_failure
            # append
            all_rep_map[dataset] = tmp_rep_sites

        # return
        tmp_log.debug("end")
        return True, all_rep_map

    def get_used_datasets(self, dataset_map: Dict, max_attempts: int = 3) -> Tuple[bool, Dict]:
        """
        Get the datasets that are used by jobs.

        Args:
            dataset_map (dict): The map of datasets.
            max_attempts (int, optional): The maximum number of attempts to get the file list. Defaults to 3.

        Returns:
            tuple: A tuple containing the status (bool) and the used datasets list.
        """
        tmp_log = LogWrapper(_logger, f"get_used_datasets-{datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None).isoformat('/')}")
        tmp_log.debug(f"get_used_datasets {str(dataset_map)}")

        res_for_failure = (False, [])
        used_ds_list = []

        # loop over all datasets
        for dataset_name in dataset_map:
            # get file list
            for attempt in range(max_attempts):
                try:
                    tmp_log.debug(f"{attempt}/{max_attempts} listFilesInDataset {dataset_name}")
                    file_items, out = rucioAPI.list_files_in_dataset(dataset_name)
                    status = True
                    break
                except Exception:
                    status = False
                    err_type, err_value = sys.exc_info()[:2]
                    out = f"{err_type} {err_value}"
                    time.sleep(60)

            if not status:
                tmp_log.error(out)
                tmp_log.error(f"bad DDM response to get size of {dataset_name}")
                tmp_log.debug("end")
                return res_for_failure

            # check if jobs use the dataset
            used_flag = False
            for tmp_job in self.jobs:
                for tmp_file in tmp_job.Files:
                    if tmp_file.type == "input" and tmp_file.lfn in file_items:
                        used_flag = True
                        break
                # escape
                if used_flag:
                    break

            # used
            if used_flag:
                used_ds_list.append(dataset_name)

        # return
        tmp_log.debug(f"used datasets = {str(used_ds_list)}")
        tmp_log.debug("end")
        return True, used_ds_list

    def get_file_from_dataset(self, dataset_name: str, guid: str, max_attempts: int = 3) -> Tuple[bool, Dict]:
        """
        Get file information from a dataset.

        Args:
            dataset_name (str): The name of the dataset.
            guid (str): The GUID of the file.
            max_attempts (int, optional): The maximum number of attempts to get the file list. Defaults to 3.

        Returns:
            tuple: A tuple containing the status (bool) and the file information (dict or None).
        """
        tmp_log = LogWrapper(_logger, f"get_file_from_dataset-{datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None).isoformat('/')}")
        tmp_log.debug(f"get_file_from_dataset {dataset_name} {guid}")

        res_for_failure = (False, None)

        # get files in datasets
        global g_files_in_ds_map
        if dataset_name not in g_files_in_ds_map:
            # get file list
            for attempt in range(max_attempts):
                try:
                    tmp_log.debug(f"{attempt}/{max_attempts} listFilesInDataset {dataset_name}")
                    file_items, out = rucioAPI.list_files_in_dataset(dataset_name)
                    status = True
                    break
                except Exception:
                    status = False
                    err_type, err_value = sys.exc_info()[:2]
                    out = f"{err_type} {err_value}"
                    time.sleep(60)

            if not status:
                tmp_log.error(out)
                tmp_log.error(f"bad DDM response to get size of {dataset_name}")
                tmp_log.debug("end")
                return res_for_failure
                # append
            g_files_in_ds_map[dataset_name] = file_items

        # check if file is in the dataset
        for tmp_lfn, tmp_val in g_files_in_ds_map[dataset_name].items():
            if uuid.UUID(tmp_val["guid"]) == uuid.UUID(guid):
                ret_map = tmp_val
                ret_map["lfn"] = tmp_lfn
                ret_map["dataset"] = dataset_name
                tmp_log.debug("end")
                return True, ret_map

        tmp_log.debug("end")
        return res_for_failure

    def register_dataset_container_with_datasets(
        self, container_name: str, files: List, replica_map: Dict, n_sites: int = 1, owner: str = None, max_attempts: int = 3
    ) -> Tuple[bool, Dict]:
        """
        Register a new dataset container with datasets.

        Args:
            container_name (str): The name of the container.
            files (list): The list of files to be included in the dataset.
            replica_map (dict): The map of replicas.
            n_sites (int, optional): The number of sites. Defaults to 1.
            owner (str, optional): The owner of the dataset. Defaults to None.
            max_attempts (int, optional): The maximum number of attempts to register the container. Defaults to 3.

        Returns:
            bool: The status of the registration process.
        """
        tmp_logger = LogWrapper(
            _logger, f"register_dataset_container_with_datasets-{datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None).isoformat('/')}"
        )
        tmp_logger.debug(f"register_dataset_container_with_datasets {container_name}")

        # parse DN
        if owner is not None:
            status, user_info = rucioAPI.finger(owner)
            if not status:
                tmp_logger.debug(f"failed to finger: {user_info}")
            else:
                owner = user_info["nickname"]
            tmp_logger.debug(f"parsed DN={owner}")

        # sort by locations
        files_map = {}
        for tmp_file in files:
            tmp_locations = sorted(replica_map[tmp_file["dataset"]])
            new_locations = []
            # skip STAGING
            for tmp_location in tmp_locations:
                if not tmp_location.endswith("STAGING"):
                    new_locations.append(tmp_location)
            if not new_locations:
                continue
            tmp_locations = new_locations
            tmp_key = tuple(tmp_locations)
            files_map.setdefault(tmp_key, [])
            # append file
            files_map[tmp_key].append(tmp_file)

        # get nfiles per dataset
        n_files_per_dataset, _ = divmod(len(files), n_sites)
        if n_files_per_dataset == 0:
            n_files_per_dataset = 1
        max_files_per_dataset = 1000
        if n_files_per_dataset >= max_files_per_dataset:
            n_files_per_dataset = max_files_per_dataset

        # register new datasets
        dataset_names = []
        tmp_index = 1
        for tmp_locations, tmp_files in files_map.items():
            tmp_sub_index = 0
            while tmp_sub_index < len(tmp_files):
                tmp_dataset_name = container_name[:-1] + "_%04d" % tmp_index
                tmp_ret = self.register_dataset_with_location(
                    tmp_dataset_name,
                    tmp_files[tmp_sub_index : tmp_sub_index + n_files_per_dataset],
                    tmp_locations,
                    owner=None,
                )
                # failed
                if not tmp_ret:
                    tmp_logger.error(f"failed to register {tmp_dataset_name}")
                    tmp_logger.debug("end")
                    return False
                # append dataset
                dataset_names.append(tmp_dataset_name)
                tmp_index += 1
                tmp_sub_index += n_files_per_dataset

        # register container
        for attempt in range(max_attempts):
            try:
                tmp_logger.debug(f"{attempt}/{max_attempts} registerContainer {container_name}")
                status = rucioAPI.register_container(container_name, dataset_names)
                out = "OK"
                break
            except Exception:
                status = False
                err_type, err_value = sys.exc_info()[:2]
                out = f"{err_type} {err_value}"
                time.sleep(10)

        if not status:
            tmp_logger.error(out)
            tmp_logger.error(f"bad DDM response to register {container_name}")
            tmp_logger.debug("end")
            return False

        # return
        tmp_logger.debug(out)
        tmp_logger.debug("end")
        return True

    def register_dataset_with_location(self, dataset_name: str, files: List, locations: List, owner: str = None, max_attempts: int = 3) -> bool:
        """
        Register a new dataset with locations.

        Args:
            dataset_name (str): The name of the dataset.
            files (list): The list of files to be included in the dataset.
            locations (list): The list of locations where the dataset will be registered.
            owner (str, optional): The owner of the dataset. Defaults to None.
            max_attempts (int, optional): The maximum number of attempts to register the dataset. Defaults to 3.

        Returns:
            bool: The status of the registration process.
        """
        tmp_logger = LogWrapper(_logger, f"register_dataset_with_location-{datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None).isoformat('/')}")
        tmp_logger.debug(f"register_dataset_with_location {dataset_name}")

        res_for_failure = False

        # get file info
        guids = []
        lfns = []
        file_sizes = []
        checksums = []
        for tmp_file in files:
            guids.append(tmp_file["guid"])
            lfns.append(tmp_file["scope"] + ":" + tmp_file["lfn"])
            file_sizes.append(int(tmp_file["filesize"]))
            checksums.append(tmp_file["checksum"])

        # register new dataset
        for attempt in range(max_attempts):
            try:
                tmp_logger.debug(f"{attempt}/{max_attempts} registerNewDataset {dataset_name} len={len(files)}")
                out = rucioAPI.register_dataset(dataset_name, lfns, guids, file_sizes, checksums, lifetime=14)
                tmp_logger.debug(out)
                break
            except Exception:
                err_type, err_value = sys.exc_info()[:2]
                tmp_logger.error(f"{err_type} {err_value}")
                if attempt + 1 == max_attempts:
                    tmp_logger.error(f"failed to register {dataset_name} in rucio")
                    tmp_logger.debug("end")
                    return res_for_failure
                time.sleep(10)

        # freeze dataset
        for attempt in range(max_attempts):
            tmp_logger.debug(f"{attempt}/{max_attempts} freezeDataset {dataset_name}")
            try:
                rucioAPI.close_dataset(dataset_name)
                status = True
            except Exception:
                err_type, err_value = sys.exc_info()[:2]
                out = f"failed to freeze : {err_type} {err_value}"
                status = False
            if not status:
                time.sleep(10)
            else:
                break
        if not status:
            tmp_logger.error(out)
            tmp_logger.error(f"bad DDM response to freeze {dataset_name}")
            tmp_logger.debug("end")
            return res_for_failure

        # register locations
        for tmp_location in locations:
            for attempt in range(max_attempts):
                try:
                    tmp_logger.debug(f"{attempt}/{max_attempts} registerDatasetLocation {dataset_name} {tmp_location}")
                    out = rucioAPI.register_dataset_location(dataset_name, [tmp_location], 14, owner)
                    tmp_logger.debug(out)
                    status = True
                    break

                except Exception:
                    status = False
                    err_type, err_value = sys.exc_info()[:2]
                    tmp_logger.error(f"{err_type} {err_value}")
                    if attempt + 1 == max_attempts:
                        tmp_logger.error(f"failed to register {dataset_name} in rucio")
                        tmp_logger.debug("end")
                        return res_for_failure
                    time.sleep(10)

            if not status:
                tmp_logger.error(out)
                tmp_logger.error(f"bad DDM response to register location {dataset_name}")
                tmp_logger.debug("end")
                return res_for_failure
        return True

    def get_datasets_by_guids(self, out_map: Dict, guids: List[str], dataset_filters: List[str]) -> Tuple[bool, Dict]:
        """
        Get datasets by GUIDs.

        Args:
            out_map (dict): The output map.
            guids (list): The list of GUIDs.
            dataset_filters (list): The list of dataset filters.

        Returns:
            tuple: A tuple containing a boolean status and a dictionary.
        """
        tmp_logger = LogWrapper(_logger, f"get_datasets_by_guids-{datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None).isoformat('/')}")
        tmp_logger.debug(f"get_datasets_by_guids {str(guids)}")

        ret_map = {}
        try:
            for guid in guids:
                tmp_dataset_names = []
                if guid not in out_map:
                    tmp_logger.error(f"GUID={guid} not found")
                    tmp_logger.debug("end")
                    return False, {}

                for tmp_dataset_name in out_map[guid]:
                    if (
                        tmp_dataset_name.startswith("panda")
                        or tmp_dataset_name.startswith("user")
                        or tmp_dataset_name.startswith("group")
                        or tmp_dataset_name.startswith("archive")
                        or re.search("_sub\d+$", tmp_dataset_name) is not None
                        or re.search("_dis\d+$", tmp_dataset_name) is not None
                        or re.search("_shadow$", tmp_dataset_name) is not None
                    ):
                        continue

                    if dataset_filters:
                        flag_match = False
                        for tmp_filter in dataset_filters:
                            if fnmatch.fnmatchcase(tmp_dataset_name, tmp_filter):
                                flag_match = True
                                break
                        if not flag_match:
                            continue

                    tmp_dataset_names.append(tmp_dataset_name)

                if not tmp_dataset_names:
                    tmp_logger.debug(f"no datasets found for GUID={guid}")
                    continue

                if len(tmp_dataset_names) != 1:
                    tmp_logger.debug(f"use the first dataset in {str(tmp_dataset_names)} for GUID:{guid}")

                ret_map[guid] = tmp_dataset_names[0]
        except Exception as e:
            tmp_logger.error(f"failed to parse get_datasets_by_guids: {e}")
            tmp_logger.debug("end")
            return False, {}

        return True, ret_map

    def list_datasets_by_guids(self, guids: List[str], dataset_filters: List[str], max_attempts: int = 3) -> Tuple[bool, Dict]:
        """
        List datasets by GUIDs.

        Args:
            guids (list): The list of GUIDs.
            dataset_filters (list): The list of dataset filters.
            max_attempts (int, optional): The maximum number of attempts to list the datasets. Defaults to 3.

        Returns:
            tuple: A tuple containing the status (bool) and the result (dict or str).
        """
        tmp_logger = LogWrapper(_logger, f"list_datasets_by_guids-{datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None).isoformat('/')}")
        tmp_logger.debug(f"list_datasets_by_guids {str(guids)}")

        res_for_failure = (False, {})
        res_for_fatal = (False, {"isFatal": True}, [])

        # get size of datasets
        for attempt in range(max_attempts):
            tmp_logger.debug(f"{attempt}/{max_attempts} listDatasetsByGUIDs GUIDs={str(guids)}")
            try:
                out = rucioAPI.list_datasets_by_guids(guids)
                status = True
                break
            except Exception:
                err_type, err_value = sys.exc_info()[:2]
                out = f"failed to get datasets with GUIDs : {err_type} {err_value}"
                status = False
                time.sleep(10)

        if not status:
            tmp_logger.error(out)
            tmp_logger.error(f"bad DDM response to get size of {str(guids)}")
            if "DataIdentifierNotFound" in out:
                tmp_logger.error("DataIdentifierNotFound in listDatasetsByGUIDs")
                tmp_logger.debug("end")
                return res_for_fatal
            tmp_logger.debug("end")
            return res_for_failure

        tmp_logger.debug(out)

        # get map
        status, ret_map = self.get_datasets_by_guids(out, guids, dataset_filters)

        if not status:
            return res_for_failure

        tmp_logger.debug("end")
        return True, ret_map

    def convert_evt_run_to_datasets(
        self, event_run_list: List, dataset_type: str, stream_name: str, dataset_filters: List, ami_tag: str, run_evt_guid_map: Dict
    ) -> Tuple[bool, Dict, List]:
        """
        Convert event/run list to datasets.

        Args:
            event_run_list (list): The list of run events.
            dataset_type (str): The type of the dataset.
            stream_name (str): The name of the stream.
            dataset_filters (list): The list of dataset filters.
            ami_tag (str): The AMI tag.
            run_evt_guid_map (dict): The map of run events to GUIDs.

        Returns:
            tuple: A tuple containing the status (bool), the result (dict or str), and the list of all files.
        """
        tmp_logger = LogWrapper(_logger, f"convert_evt_run_to_datasets-{datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None).isoformat('/')}")
        tmp_logger.debug(f"convert_evt_run_to_datasets type={dataset_type} stream={stream_name} dsPatt={str(dataset_filters)} amitag={ami_tag}")

        # check data type
        failed_ret = False, {}, []
        fatal_ret = False, {"isFatal": True}, []
        stream_ref = "Stream" + dataset_type
        # import event lookup client
        if run_evt_guid_map == {}:
            if len(event_run_list) == 0:
                tmp_logger.error("Empty list for run and events was provided")
                tmp_logger.debug("end")
                return failed_ret
            # Hadoop EI
            from .event_lookup_client_ei import EventLookupClientEI

            event_lookup_if = EventLookupClientEI()
            # loop over all events
            n_events_per_loop = 500
            i_events_total = 0
            while i_events_total < len(event_run_list):
                tmp_event_run_list = event_run_list[i_events_total : i_events_total + n_events_per_loop]
                tmp_logger.debug(f"EI lookup for {i_events_total}/{len(event_run_list)}")
                i_events_total += n_events_per_loop
                reg_start = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
                guid_list_elssi, tmp_com, tmp_out, tmp_err = event_lookup_if.do_lookup(
                    tmp_event_run_list,
                    stream=stream_name,
                    tokens=stream_ref,
                    ami_tag=ami_tag,
                )
                reg_time = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - reg_start
                tmp_logger.debug(f"EI command: {tmp_com}")
                tmp_logger.debug(f"took {reg_time.seconds}.{reg_time.microseconds / 1000:03f} sec for {len(tmp_event_run_list)} events")
                # failed
                if tmp_err not in [None, ""] or len(guid_list_elssi) == 0:
                    tmp_logger.debug(tmp_com)
                    tmp_logger.debug(tmp_out)
                    tmp_logger.debug(tmp_err)
                    tmp_logger.error("invalid return from EventIndex")
                    tmp_logger.debug("end")
                    return failed_ret
                # check events
                for run_nr, evt_nr in tmp_event_run_list:
                    param_str = f"Run:{run_nr} Evt:{evt_nr} Stream:{stream_name}"
                    tmp_logger.debug(param_str)
                    tmp_run_evt_key = (int(run_nr), int(evt_nr))
                    # not found
                    if tmp_run_evt_key not in guid_list_elssi or len(guid_list_elssi[tmp_run_evt_key]) == 0:
                        tmp_logger.debug(tmp_com)
                        tmp_logger.debug(tmp_out)
                        tmp_logger.debug(tmp_err)
                        tmp_logger.error(f"no GUIDs were found in EventIndex for {param_str}")
                        tmp_logger.debug("end")
                        return fatal_ret
                    # append
                    run_evt_guid_map[tmp_run_evt_key] = guid_list_elssi[tmp_run_evt_key]
        # convert to datasets
        all_datasets = []
        all_files = []
        all_locations = {}
        for tmp_idx in run_evt_guid_map:
            tmp_guids = run_evt_guid_map[tmp_idx]
            run_nr, evt_nr = tmp_idx
            tmp_ds_ret, tmp_dataset_map = self.list_datasets_by_guids(tmp_guids, dataset_filters)
            # failed
            if not tmp_ds_ret:
                tmp_logger.error(f"failed to convert GUIDs to datasets")
                if "isFatal" in tmp_dataset_map and tmp_dataset_map["isFatal"] is True:
                    tmp_logger.debug("end")
                    return fatal_ret
                tmp_logger.debug("end")
                return failed_ret
            # empty
            if not tmp_dataset_map:
                tmp_logger.error(f"there is no dataset for Run:{run_nr} Evt:{evt_nr} GUIDs:{str(tmp_guids)}")
                tmp_logger.debug("end")
                return fatal_ret
            if len(tmp_dataset_map) != 1:
                tmp_logger.error(f"there are multiple datasets {str(tmp_dataset_map)} for Run:{run_nr} Evt:{evt_nr} GUIDs:{str(tmp_guids)}")
                tmp_logger.debug("end")
                return fatal_ret

            # append
            for tmp_guid, tmp_dataset_name in tmp_dataset_map.items():
                # collect dataset names
                if tmp_dataset_name not in all_datasets:
                    all_datasets.append(tmp_dataset_name)
                    # get location
                    stat_rep, replica_map = self.get_list_dataset_replicas(tmp_dataset_name)
                    # failed
                    if not stat_rep:
                        tmp_logger.error(f"failed to get locations for {tmp_dataset_name}")
                        tmp_logger.debug("end")
                        return failed_ret
                    # collect locations
                    tmp_location_list = []
                    for tmp_location in replica_map:
                        # use only complete replicas
                        ds_stat_dict = replica_map[tmp_location][0]
                        if ds_stat_dict["total"] is not None and ds_stat_dict["total"] == ds_stat_dict["found"]:
                            if tmp_location not in tmp_location_list:
                                tmp_location_list.append(tmp_location)
                    all_locations[tmp_dataset_name] = tmp_location_list

                # get file info
                tmp_file_ret, tmp_file_info = self.get_file_from_dataset(tmp_dataset_name, tmp_guid)
                # failed
                if not tmp_file_ret:
                    tmp_logger.error(f"failed to get fileinfo for GUID:{tmp_guid} DS:{tmp_dataset_name}")
                    tmp_logger.debug("end")
                    return failed_ret
                # collect files
                all_files.append(tmp_file_info)
        # return
        tmp_logger.debug(f"converted to {str(all_datasets)}, {str(all_locations)}, {str(all_files)}")
        tmp_logger.debug("end")
        return True, all_locations, all_files
