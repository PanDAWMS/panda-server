"""
This module is designed to setup datasets for the ATLAS experiment.
The setup process involves preparing the necessary data and resources for the ATLAS jobs to run.
This includes tasks such as data placement, dataset creation, and job configuration.
The 'SetupperAtlasPlugin' class in this module inherits from the 'SetupperPluginBase' class and overrides its methods to provide ATLAS-specific functionality.
The 'setupper.py' module uses this plugin when setting up datasets for ATLAS jobs.

"""

import datetime
import os
import re
import sys
import time
import traceback
import uuid
from typing import Dict, List, Optional, Tuple

from pandacommon.pandalogger.LogWrapper import LogWrapper
from rucio.common.exception import DataIdentifierNotFound

import pandaserver.brokerage.broker
from pandaserver.brokerage.SiteMapper import SiteMapper
from pandaserver.dataservice import DataServiceUtils, ErrorCode
from pandaserver.dataservice.DataServiceUtils import select_scope
from pandaserver.dataservice.ddm import rucioAPI
from pandaserver.dataservice.setupper_plugin_base import SetupperPluginBase
from pandaserver.taskbuffer import EventServiceUtils, JobUtils
from pandaserver.taskbuffer.DatasetSpec import DatasetSpec


class SetupperAtlasPlugin(SetupperPluginBase):
    """
    This class is a plugin for setting up datasets specifically for the ATLAS experiment.
    It inherits from the SetupperPluginBase class and overrides its methods to provide
    ATLAS-specific functionality.
    """

    # constructor
    def __init__(self, taskBuffer, jobs: List, logger, **params: Dict) -> None:
        """
        Constructor for the SetupperAtlasPlugin class.

        :param taskBuffer: The buffer for tasks.
        :param jobs: The jobs to be processed.
        :param logger: The logger to be used for logging.
        :param params: Additional parameters.
        """
        # defaults
        default_map = {
            "resubmit": False,
        }
        SetupperPluginBase.__init__(self, taskBuffer, jobs, logger, params, default_map)
        # VUIDs of dispatchDBlocks
        self.vuid_map = {}
        # file list for dispDS for PandaDDM
        self.disp_file_list = {}
        # site mapper
        self.site_mapper = None
        # location map
        self.replica_map = {}
        # all replica locations
        self.all_replica_map = {}
        # available files at satellite sites
        self.available_lfns_in_satellites = {}
        # list of missing datasets
        self.missing_dataset_list = {}
        # lfn ds map
        self.lfn_dataset_map = {}
        # source label
        self.prod_source_label = None
        self.job_label = None

    # main
    def run(self) -> None:
        """
        Main method for running the setup process.
        """
        tmp_logger = LogWrapper(self.logger, "<run>")
        try:
            tmp_logger.debug("start")
            self.memory_check()
            bunch_tag = ""
            tag_job = None
            time_start = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
            if self.jobs is not None and len(self.jobs) > 0:
                tag_job = self.jobs[0]
            elif len(self.jumbo_jobs) > 0:
                tag_job = self.jumbo_jobs[0]
            if tag_job is not None:
                bunch_tag = f"PandaID:{tag_job.PandaID} type:{tag_job.prodSourceLabel} taskID:{tag_job.taskID} pType={tag_job.processingType}"
                tmp_logger.debug(bunch_tag)
                self.prod_source_label = tag_job.prodSourceLabel
                self.job_label = tag_job.job_label
            # instantiate site mapper
            self.site_mapper = SiteMapper(self.task_buffer)
            # correctLFN
            self.correct_lfn()
            # run full Setupper
            # invoke brokerage
            tmp_logger.debug("running broker.schedule")
            self.memory_check()
            pandaserver.brokerage.broker.schedule(self.jobs, self.site_mapper)

            # remove waiting jobs
            self.remove_waiting_jobs()

            # setup dispatch dataset
            tmp_logger.debug("running setup_source")
            self.memory_check()
            self.setup_source()
            self.memory_check()

            # sort by site so that larger sub datasets are created in the next step
            if self.jobs != [] and self.jobs[0].prodSourceLabel in [
                "managed",
                "test",
            ]:
                tmp_job_map = {}
                for tmp_job in self.jobs:
                    # add site
                    if tmp_job.computingSite not in tmp_job_map:
                        tmp_job_map[tmp_job.computingSite] = []
                    # add job
                    tmp_job_map[tmp_job.computingSite].append(tmp_job)
                # make new list
                tmp_job_list = []
                for jobs in tmp_job_map.values():
                    tmp_job_list += jobs
                # set new list
                self.jobs = tmp_job_list
            # create dataset for outputs and assign destination
            if self.jobs != [] and self.jobs[0].prodSourceLabel in [
                "managed",
                "test",
            ]:
                # count the number of jobs per _dis
                i_bunch = 0
                prev_dis_ds_name = None
                n_jobs_per_dis_list = []
                for tmp_job in self.jobs:
                    if prev_dis_ds_name is not None and prev_dis_ds_name != tmp_job.dispatchDBlock:
                        n_jobs_per_dis_list.append(i_bunch)
                        i_bunch = 0
                    # increment
                    i_bunch += 1
                    # set _dis name
                    prev_dis_ds_name = tmp_job.dispatchDBlock
                # remaining
                if i_bunch != 0:
                    n_jobs_per_dis_list.append(i_bunch)
                # split sub datasets
                i_bunch = 0
                n_bunch_max = 50
                tmp_index_job = 0
                for n_jobs_per_dis in n_jobs_per_dis_list:
                    # check _dis boundary so that the same _dis doesn't contribute to many _subs
                    if i_bunch + n_jobs_per_dis > n_bunch_max:
                        if i_bunch != 0:
                            self.setup_destination(start_idx=tmp_index_job, n_jobs_in_loop=i_bunch)
                            tmp_index_job += i_bunch
                            i_bunch = 0
                    # increment
                    i_bunch += n_jobs_per_dis
                # remaining
                if i_bunch != 0:
                    self.setup_destination(start_idx=tmp_index_job, n_jobs_in_loop=i_bunch)
            else:
                # make one sub dataset per job so that each job doesn't have to wait for others to be done
                # Special jobs for SW installation and HC seem to be above 6000
                if self.jobs != [] and self.jobs[0].prodSourceLabel in ["user", "panda"] and self.jobs[-1].currentPriority > 6000:
                    for i_bunch in range(len(self.jobs)):
                        self.setup_destination(start_idx=i_bunch, n_jobs_in_loop=1)
                else:
                    # at a burst
                    self.setup_destination()
            # make dis datasets for existing files
            self.memory_check()
            self.make_dis_datasets_for_existing_files()
            self.memory_check()
            # setup jumbo jobs
            self.setup_jumbo_jobs()
            self.memory_check()
            reg_time = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - time_start
            tmp_logger.debug(f"{bunch_tag} took {reg_time.seconds}sec")
            tmp_logger.debug("end")
        except Exception:
            error_type, error_value = sys.exc_info()[:2]
            err_str = f"run() : {error_type} {error_value}"
            err_str.strip()
            err_str += traceback.format_exc()
            tmp_logger.error(err_str)

    # post run
    def post_run(self) -> None:
        """
        Post run method for running the setup process.
        """
        tmp_logger = LogWrapper(self.logger, "<post_run>")
        try:
            tmp_logger.debug("start")
            self.memory_check()
            # subscribe sites dispatchDBlocks. this must be the last method
            tmp_logger.debug("running subscribe_dispatch_data_block")
            self.subscribe_dispatch_data_block()

            # dynamic data placement for analysis jobs
            self.memory_check()
            tmp_logger.debug("running dynamic_data_placement")
            self.dynamic_data_placement()
            self.memory_check()
            tmp_logger.debug("end")
        except Exception:
            error_type, error_value = sys.exc_info()[:2]
            tmp_logger.error(f"{error_type} {error_value}")

    # make dispatchDBlocks, insert prod/dispatchDBlock to database
    def setup_source(self) -> None:
        """
        Make dispatchDBlocks, insert prod/dispatchDBlock to database
        Transfer input to satellite
        """

        tmp_logger = LogWrapper(self.logger, "<setup_source>")

        file_list = {}
        prod_list = []
        prod_error = {}
        disp_error = {}
        back_end_map = {}
        ds_task_map = dict()
        jedi_task_id = None
        # special datasets in rucio where files are zipped into a file
        use_zip_to_pin_map = dict()
        # extract prodDBlock
        for job in self.jobs:
            # ignore failed jobs
            if job.jobStatus in ["failed", "cancelled"] or job.isCancelled():
                continue
            if jedi_task_id is None and job.jediTaskID not in ["NULL", None, 0]:
                jedi_task_id = job.jediTaskID
            # production datablock
            if job.prodDBlock != "NULL" and job.prodDBlock and (job.prodSourceLabel not in ["user", "panda"]):
                # get VUID and record prodDBlock into DB
                if job.prodDBlock not in prod_error:
                    tmp_logger.debug(f"list_datasets {job.prodDBlock}")
                    prod_error[job.prodDBlock] = ""
                    new_out = None
                    for _ in range(3):
                        new_out, err_msg = rucioAPI.list_datasets(job.prodDBlock)
                        if new_out is None:
                            time.sleep(10)
                        else:
                            break
                    if new_out is None:
                        prod_error[job.prodDBlock] = f"setupper.setup_source() could not get VUID of prodDBlock with {err_msg}"
                        tmp_logger.error(prod_error[job.prodDBlock])
                    else:
                        tmp_logger.debug(new_out)
                        try:
                            vuids = new_out[job.prodDBlock]["vuids"]
                            n_files = 0
                            # dataset spec
                            dataset = DatasetSpec()
                            dataset.vuid = vuids[0]
                            dataset.name = job.prodDBlock
                            dataset.type = "input"
                            dataset.status = "completed"
                            dataset.numberfiles = n_files
                            dataset.currentfiles = n_files
                            prod_list.append(dataset)
                        except Exception:
                            error_type, error_value = sys.exc_info()[:2]
                            tmp_logger.error(f"{error_type} {error_value}")
                            prod_error[job.prodDBlock] = "setupper.setup_source() could not decode VUID of prodDBlock"
                # error
                if prod_error[job.prodDBlock] != "":
                    if job.jobStatus != "failed":
                        job.jobStatus = "failed"
                        job.ddmErrorCode = ErrorCode.EC_Setupper
                        job.ddmErrorDiag = prod_error[job.prodDBlock]
                        tmp_logger.debug(f"failed PandaID={job.PandaID} with {job.ddmErrorDiag}")
                    continue

            # dispatch datablock
            if job.dispatchDBlock != "NULL":
                # useZipToPin mapping
                use_zip_to_pin_map[job.dispatchDBlock] = job.useZipToPin()

                # filelist
                if job.dispatchDBlock not in file_list:
                    file_list[job.dispatchDBlock] = {
                        "lfns": [],
                        "guids": [],
                        "fsizes": [],
                        "md5sums": [],
                        "chksums": [],
                    }
                    ds_task_map[job.dispatchDBlock] = job.jediTaskID
                # DDM backend
                if job.dispatchDBlock not in back_end_map:
                    back_end_map[job.dispatchDBlock] = "rucio"
                # collect LFN and GUID
                for file in job.Files:
                    if file.type == "input" and file.status == "pending":
                        if back_end_map[job.dispatchDBlock] != "rucio":
                            tmp_lfn = file.lfn
                        else:
                            tmp_lfn = f"{file.scope}:{file.lfn}"
                        if tmp_lfn not in file_list[job.dispatchDBlock]["lfns"]:
                            file_list[job.dispatchDBlock]["lfns"].append(tmp_lfn)
                            file_list[job.dispatchDBlock]["guids"].append(file.GUID)
                            file_list[job.dispatchDBlock]["fsizes"].append(None if file.fsize in ["NULL", 0] else int(file.fsize))
                            file_list[job.dispatchDBlock]["chksums"].append(None if file.checksum in ["NULL", ""] else file.checksum)

                            if file.md5sum in ["NULL", ""]:
                                file_list[job.dispatchDBlock]["md5sums"].append(None)
                            elif file.md5sum.startswith("md5:"):
                                file_list[job.dispatchDBlock]["md5sums"].append(file.md5sum)
                            else:
                                file_list[job.dispatchDBlock]["md5sums"].append(f"md5:{file.md5sum}")

                        # get replica locations
                        self.replica_map.setdefault(job.dispatchDBlock, {})
                        if file.dataset not in self.all_replica_map:
                            if file.dataset.endswith("/"):
                                status, out = self.get_list_dataset_replicas_in_container(file.dataset, True)
                            else:
                                status, out = self.get_list_dataset_replicas(file.dataset)
                            if not status:
                                tmp_logger.error(out)
                                disp_error[job.dispatchDBlock] = f"could not get locations for {file.dataset}"
                                tmp_logger.error(disp_error[job.dispatchDBlock])
                            else:
                                tmp_logger.debug(out)
                                self.all_replica_map[file.dataset] = out
                        if file.dataset in self.all_replica_map:
                            self.replica_map[job.dispatchDBlock][file.dataset] = self.all_replica_map[file.dataset]

        # register dispatch dataset
        disp_list = self.register_dispatch_datasets(file_list, use_zip_to_pin_map, ds_task_map, tmp_logger, disp_error,
                                                    jedi_task_id)
        # insert datasets to DB
        self.task_buffer.insertDatasets(prod_list + disp_list)
        # job status
        for job in self.jobs:
            if job.dispatchDBlock in disp_error and disp_error[job.dispatchDBlock] != "":
                if job.jobStatus != "failed":
                    job.jobStatus = "failed"
                    job.ddmErrorCode = ErrorCode.EC_Setupper
                    job.ddmErrorDiag = disp_error[job.dispatchDBlock]
                    tmp_logger.debug(f"failed PandaID={job.PandaID} with {job.ddmErrorDiag}")
        # delete explicitly some huge variables
        del file_list
        del prod_list
        del prod_error

    def register_dispatch_datasets(
            self,
            file_list: Dict[str, Dict[str, List[str]]],
            use_zip_to_pin_map: Dict[str, bool],
            ds_task_map: Dict[str, int],
            tmp_logger: LogWrapper,
            disp_error: Dict[str, str],
            jedi_task_id: Optional[int]
    ) -> List[DatasetSpec]:
        """
        Register dispatch datasets in Rucio.

        This method registers dispatch datasets in Rucio, handles the creation of zip files if necessary,
        and updates the dataset metadata. It also handles errors and retries the registration process if needed.

        :param file_list: Dictionary containing file information for each dispatch dataset block.
        :param use_zip_to_pin_map: Dictionary mapping dispatch dataset blocks to a boolean indicating if zip files should be used.
        :param ds_task_map: Dictionary mapping dispatch dataset blocks to their corresponding JEDI task IDs.
        :param tmp_logger: Logger instance for logging messages.
        :param disp_error: Dictionary to store errors encountered during the registration process.
        :param jedi_task_id: JEDI task ID associated with the jobs.
        :return: List of DatasetSpec objects representing the registered dispatch datasets.
        """
        disp_list = []
        for dispatch_data_block, block_data in file_list.items():
            # ignore empty dataset
            if len(block_data["lfns"]) == 0:
                continue

            # register dispatch dataset
            self.disp_file_list[dispatch_data_block] = file_list[dispatch_data_block]
            if not use_zip_to_pin_map[dispatch_data_block]:
                dis_files = file_list[dispatch_data_block]
            else:
                dids = file_list[dispatch_data_block]["lfns"]
                tmp_zip_stat, tmp_zip_out = rucioAPI.get_zip_files(dids, None)
                if not tmp_zip_stat:
                    tmp_logger.debug(f"failed to get zip files : {tmp_zip_out}")
                    tmp_zip_out = {}
                dis_files = {"lfns": [], "guids": [], "fsizes": [], "chksums": []}
                for tmp_lfn, tmp_guid, tmp_file_size, tmp_checksum in zip(
                        file_list[dispatch_data_block]["lfns"],
                        file_list[dispatch_data_block]["guids"],
                        file_list[dispatch_data_block]["fsizes"],
                        file_list[dispatch_data_block]["chksums"],
                ):
                    if tmp_lfn in tmp_zip_out:
                        tmp_zip_file_name = f"{tmp_zip_out[tmp_lfn]['scope']}:{tmp_zip_out[tmp_lfn]['name']}"
                        if tmp_zip_file_name not in dis_files["lfns"]:
                            dis_files["lfns"].append(tmp_zip_file_name)
                            dis_files["guids"].append(tmp_zip_out[tmp_lfn]["guid"])
                            dis_files["fsizes"].append(tmp_zip_out[tmp_lfn]["bytes"])
                            dis_files["chksums"].append(tmp_zip_out[tmp_lfn]["adler32"])
                    else:
                        dis_files["lfns"].append(tmp_lfn)
                        dis_files["guids"].append(tmp_guid)
                        dis_files["fsizes"].append(tmp_file_size)
                        dis_files["chksums"].append(tmp_checksum)

            metadata = {"hidden": True, "purge_replicas": 0}
            if dispatch_data_block in ds_task_map and ds_task_map[dispatch_data_block] not in ["NULL", 0]:
                metadata["task_id"] = str(ds_task_map[dispatch_data_block])
            tmp_logger.debug(f"register_dataset {dispatch_data_block} {str(metadata)}")
            max_attempt = 3
            is_ok = False
            err_str = ""
            for attempt in range(max_attempt):
                try:
                    out = rucioAPI.register_dataset(
                        dispatch_data_block,
                        dis_files["lfns"],
                        dis_files["guids"],
                        dis_files["fsizes"],
                        dis_files["chksums"],
                        lifetime=7,
                        scope="panda",
                        metadata=metadata,
                    )
                    is_ok = True
                    break
                except Exception:
                    error_type, error_value = sys.exc_info()[:2]
                    err_str = f"{error_type}:{error_value}"
                    tmp_logger.error(f"register_dataset : failed with {err_str}")
                    if attempt + 1 == max_attempt:
                        break
                    self.logger.debug(f"sleep {attempt}/{max_attempt}")
                    time.sleep(10)
            if not is_ok:
                disp_error[
                    dispatch_data_block] = "setupper.setup_source() could not register dispatch_data_block with {0}".format(
                    err_str.split("\n")[-1])
                continue
            tmp_logger.debug(out)
            new_out = out
            # freezeDataset dispatch dataset
            tmp_logger.debug(f"closeDataset {dispatch_data_block}")
            for attempt in range(max_attempt):
                status = False
                try:
                    rucioAPI.close_dataset(dispatch_data_block)
                    status = True
                    break
                except Exception:
                    error_type, error_value = sys.exc_info()[:2]
                    out = f"failed to close : {error_type} {error_value}"
                    time.sleep(10)
            if not status:
                tmp_logger.error(out)
                disp_error[
                    dispatch_data_block] = f"setupper.setup_source() could not freeze dispatch_data_block with {out}"
                continue

            # get VUID
            try:
                vuid = new_out["vuid"]
                # dataset spec. currentfiles is used to count the number of failed jobs
                dataset = DatasetSpec()
                dataset.vuid = vuid
                dataset.name = dispatch_data_block
                dataset.type = "dispatch"
                dataset.status = "defined"
                dataset.numberfiles = len(file_list[dispatch_data_block]["lfns"])
                try:
                    dataset.currentfiles = int(
                        sum(filter(None, file_list[dispatch_data_block]["fsizes"])) / 1024 / 1024)
                except Exception:
                    dataset.currentfiles = 0
                if jedi_task_id is not None:
                    dataset.MoverID = jedi_task_id
                disp_list.append(dataset)
                self.vuid_map[dataset.name] = dataset.vuid
            except Exception:
                error_type, error_value = sys.exc_info()[:2]
                dispatch_data_block.error(f"{error_type} {error_value}")
                disp_error[dispatch_data_block] = "setupper.setup_source() could not decode VUID dispatch_data_block"
        return disp_list

    # create dataset for outputs in the repository and assign destination
    def setup_destination(self, start_idx: int = -1, n_jobs_in_loop: int = 50) -> None:
        """
        Create dataset for outputs in the repository and assign destination

        :param start_idx: The starting index for the jobs to be processed. Defaults to -1.
        :param n_jobs_in_loop: The number of jobs to be processed in a loop. Defaults to 50.
        """
        tmp_logger = LogWrapper(self.logger, "<setup_destination>")
        tmp_logger.debug(f"idx:{start_idx} n:{n_jobs_in_loop}")

        dest_error = {}
        dataset_list = {}
        newname_list = {}
        sn_gotten_ds = []
        if start_idx == -1:
            jobs_list = self.jobs
        else:
            jobs_list = self.jobs[start_idx : start_idx + n_jobs_in_loop]
        for job in jobs_list:
            # ignore failed jobs
            if job.jobStatus in ["failed", "cancelled"] or job.isCancelled():
                continue
            for file in job.Files:
                # ignore input files
                if file.type in ["input", "pseudo_input"]:
                    continue
                # don't touch with outDS for unmerge jobs
                if job.prodSourceLabel == "panda" and job.processingType == "unmerge" and file.type != "log":
                    continue
                # extract destinationDBlock, destinationSE and computing_site
                dest = (
                    file.destinationDBlock,
                    file.destinationSE,
                    job.computingSite,
                    file.destinationDBlockToken,
                )
                if dest not in dest_error:
                    dest_error[dest] = ""
                    original_name = ""
                    if (job.prodSourceLabel == "panda") or (
                        job.prodSourceLabel in JobUtils.list_ptest_prod_sources and job.processingType in ["pathena", "prun", "gangarobot-rctest"]
                    ):
                        # keep original name
                        name_list = [file.destinationDBlock]
                    else:
                        # set freshness to avoid redundant DB lookup
                        defined_fresh_flag = None
                        if file.destinationDBlock in sn_gotten_ds:
                            # already checked
                            defined_fresh_flag = False
                        elif job.prodSourceLabel in ["user", "test", "prod_test"]:
                            # user or test datasets are always fresh in DB
                            defined_fresh_flag = True
                        # get serial number
                        serial_number, fresh_flag = self.task_buffer.getSerialNumber(file.destinationDBlock, defined_fresh_flag)
                        if serial_number == -1:
                            dest_error[dest] = f"setupper.setup_destination() could not get serial num for {file.destinationDBlock}"
                            break
                        if file.destinationDBlock not in sn_gotten_ds:
                            sn_gotten_ds.append(file.destinationDBlock)
                        # new dataset name
                        newname_list[dest] = self.make_sub_dataset_name(file.destinationDBlock, serial_number, job.jediTaskID)
                        if fresh_flag:
                            # register original dataset and new dataset
                            name_list = [file.destinationDBlock, newname_list[dest]]
                            original_name = file.destinationDBlock
                        else:
                            # register new dataset only
                            name_list = [newname_list[dest]]
                    # create dataset
                    for name in name_list:
                        computing_site = job.computingSite
                        tmp_site = self.site_mapper.getSite(computing_site)
                        _, scope_output = select_scope(tmp_site, job.prodSourceLabel, job.job_label)
                        if name == original_name and not name.startswith("panda.um."):
                            # for original dataset
                            computing_site = file.destinationSE
                        new_vuid = None
                        if job.destinationSE != "local":
                            # get src and dest DDM conversion is needed for unknown sites
                            if job.prodSourceLabel == "user" and computing_site not in self.site_mapper.siteSpecList:
                                # DDM ID was set by using --destSE for analysis job to transfer output
                                tmp_src_ddm = tmp_site.ddm_output[scope_output]
                            else:
                                tmp_src_ddm = tmp_site.ddm_output[scope_output]
                            if job.prodSourceLabel == "user" and file.destinationSE not in self.site_mapper.siteSpecList:
                                # DDM ID was set by using --destSE for analysis job to transfer output
                                tmp_dst_ddm = tmp_src_ddm
                            elif DataServiceUtils.getDestinationSE(file.destinationDBlockToken) is not None:
                                # destination is specified
                                tmp_dst_ddm = DataServiceUtils.getDestinationSE(file.destinationDBlockToken)
                            else:
                                tmp_dst_site = self.site_mapper.getSite(file.destinationSE)
                                _, scope_dst_site_output = select_scope(tmp_dst_site, job.prodSourceLabel, job.job_label)
                                tmp_dst_ddm = tmp_dst_site.ddm_output[scope_dst_site_output]
                            # skip registration for _sub when src=dest
                            if (
                                (
                                    (tmp_src_ddm == tmp_dst_ddm and not EventServiceUtils.isMergeAtOS(job.specialHandling))
                                    or DataServiceUtils.getDistributedDestination(file.destinationDBlockToken) is not None
                                )
                                and name != original_name
                                and DataServiceUtils.is_sub_dataset(name)
                            ):
                                # create a fake vuid
                                new_vuid = str(uuid.uuid4())
                            else:
                                # get list of tokens
                                tmp_token_list = file.destinationDBlockToken.split(",")

                                # get locations
                                using_nucleus_as_satellite = False
                                if job.prodSourceLabel == "user" and computing_site not in self.site_mapper.siteSpecList:
                                    ddm_id_list = [tmp_site.ddm_output[scope_output]]
                                else:
                                    if (
                                        tmp_site.cloud != job.getCloud()
                                        and DataServiceUtils.is_sub_dataset(name)
                                        and (job.prodSourceLabel not in ["user", "panda"])
                                    ):
                                        # Nucleus used as Satellite. Use DATADISK as location
                                        ddm_id_list = [tmp_site.ddm_output[scope_output]]
                                        using_nucleus_as_satellite = True
                                    else:
                                        ddm_id_list = [tmp_site.ddm_output[scope_output]]
                                # use another location when token is set
                                if not DataServiceUtils.is_sub_dataset(name) and DataServiceUtils.getDestinationSE(file.destinationDBlockToken) is not None:
                                    # destination is specified
                                    ddm_id_list = [DataServiceUtils.getDestinationSE(file.destinationDBlockToken)]
                                elif (not using_nucleus_as_satellite) and (file.destinationDBlockToken not in ["NULL", ""]):
                                    ddm_id_list = []
                                    for tmp_token in tmp_token_list:
                                        # set default
                                        ddm_id = tmp_site.ddm_output[scope_output]
                                        # convert token to DDM ID
                                        if tmp_token in tmp_site.setokens_output[scope_output]:
                                            ddm_id = tmp_site.setokens_output[scope_output][tmp_token]
                                        # replace or append
                                        if len(tmp_token_list) <= 1 or name != original_name:
                                            # use location consistent with token
                                            ddm_id_list = [ddm_id]
                                            break
                                        else:
                                            # use multiple locations for _tid
                                            if ddm_id not in ddm_id_list:
                                                ddm_id_list.append(ddm_id)
                                # set hidden flag for _sub
                                tmp_life_time = None
                                tmp_metadata = None
                                if name != original_name and DataServiceUtils.is_sub_dataset(name):
                                    tmp_life_time = 14
                                    tmp_metadata = {"hidden": True, "purge_replicas": 0}

                                # register dataset
                                tmp_logger.debug(f"register_dataset {name} metadata={tmp_metadata}")
                                is_ok = False
                                for _ in range(3):
                                    try:
                                        out = rucioAPI.register_dataset(
                                            name,
                                            metadata=tmp_metadata,
                                            lifetime=tmp_life_time,
                                        )
                                        tmp_logger.debug(out)
                                        new_vuid = out["vuid"]
                                        is_ok = True
                                        break
                                    except Exception:
                                        error_type, error_value = sys.exc_info()[:2]
                                        tmp_logger.error(f"register_dataset : failed with {error_type}:{error_value}")
                                        time.sleep(10)
                                if not is_ok:
                                    tmp_msg = f"setupper.setup_destination() could not register : {name}"
                                    dest_error[dest] = tmp_msg
                                    tmp_logger.error(tmp_msg)
                                    break
                                # register dataset locations
                                if (
                                    job.lockedby == "jedi" and job.getDdmBackEnd() == "rucio" and job.prodSourceLabel in ["panda", "user"]
                                ) or DataServiceUtils.getDistributedDestination(file.destinationDBlockToken, ignore_empty=False) is not None:
                                    # skip registerDatasetLocations
                                    status, out = True, ""
                                elif (
                                    name == original_name
                                    or tmp_src_ddm != tmp_dst_ddm
                                    or job.prodSourceLabel == "panda"
                                    or (
                                        job.prodSourceLabel in JobUtils.list_ptest_prod_sources
                                        and job.processingType in ["pathena", "prun", "gangarobot-rctest"]
                                    )
                                    or len(tmp_token_list) > 1
                                    or EventServiceUtils.isMergeAtOS(job.specialHandling)
                                ):
                                    # set replica lifetime to _sub
                                    rep_life_time = None
                                    if (name != original_name and DataServiceUtils.is_sub_dataset(name)) or (
                                        name == original_name and name.startswith("panda.")
                                    ):
                                        rep_life_time = 14
                                    elif name.startswith("hc_test") or name.startswith("panda.install.") or name.startswith("user.gangarbt."):
                                        rep_life_time = 7
                                    # distributed datasets for Event Service output
                                    grouping = None
                                    if name != original_name and DataServiceUtils.is_sub_dataset(name) and EventServiceUtils.isEventServiceJob(job):
                                        ddm_id_list = ["type=DATADISK"]
                                        grouping = "NONE"
                                    # register location
                                    is_ok = True
                                    for ddm_id in ddm_id_list:
                                        activity = DataServiceUtils.getActivityForOut(job.prodSourceLabel)
                                        tmp_logger.debug(
                                            f"register_dataset_location {name} {ddm_id} lifetime={rep_life_time} activity={activity} grouping={grouping}"
                                        )
                                        status = False
                                        # invalid location
                                        if ddm_id is None:
                                            tmp_logger.error(f"wrong location : {ddm_id}")
                                            break
                                        for _ in range(3):
                                            try:
                                                out = rucioAPI.register_dataset_location(
                                                    name,
                                                    [ddm_id],
                                                    lifetime=rep_life_time,
                                                    activity=activity,
                                                    grouping=grouping,
                                                )
                                                tmp_logger.debug(out)
                                                status = True
                                                break
                                            except Exception:
                                                error_type, error_value = sys.exc_info()[:2]
                                                out = f"{error_type}:{error_value}"
                                                tmp_logger.error(f"register_dataset_location : failed with {out}")
                                                time.sleep(10)
                                        # failed
                                        if not status:
                                            break
                                else:
                                    # skip registerDatasetLocations
                                    status, out = True, ""
                                if not status:
                                    dest_error[dest] = f"Could not register location : {name} {out.splitlines()[-1]}"
                                    break
                        # already failed
                        if dest_error[dest] != "" and name == original_name:
                            break
                        # get vuid
                        if new_vuid is None:
                            tmp_logger.debug("listDatasets " + name)
                            for _ in range(3):
                                new_out, err_msg = rucioAPI.list_datasets(name)
                                if new_out is None:
                                    time.sleep(10)
                                else:
                                    break
                            if new_out is None:
                                tmp_logger.error(f"failed to get VUID for {name} with {err_msg}")
                            else:
                                tmp_logger.debug(new_out)
                                new_vuid = new_out[name]["vuids"][0]
                        try:
                            # dataset spec
                            dataset = DatasetSpec()
                            dataset.vuid = new_vuid
                            dataset.name = name
                            dataset.type = "output"
                            dataset.numberfiles = 0
                            dataset.currentfiles = 0
                            dataset.status = "defined"
                            # append
                            dataset_list[(name, file.destinationSE, computing_site)] = dataset
                        except Exception:
                            # set status
                            error_type, error_value = sys.exc_info()[:2]
                            tmp_logger.error(f"{error_type} {error_value}")
                            dest_error[dest] = f"setupper.setup_destination() could not get VUID : {name}"
                # set new destDBlock
                if dest in newname_list:
                    file.destinationDBlock = newname_list[dest]
            for file in job.Files:
                dest = (
                    file.destinationDBlock,
                    file.destinationSE,
                    job.computingSite,
                    file.destinationDBlockToken,
                )
                # update job status if failed
                if dest in dest_error and dest_error[dest] != "":
                    if job.jobStatus != "failed":
                        job.jobStatus = "failed"
                        job.ddmErrorCode = ErrorCode.EC_Setupper
                        job.ddmErrorDiag = dest_error[dest]
                        tmp_logger.debug(f"failed PandaID={job.PandaID} with {job.ddmErrorDiag}")
                    break
                else:
                    new_dest = (
                        file.destinationDBlock,
                        file.destinationSE,
                        job.computingSite,
                    )
                    # increment number of files
                    if new_dest in dataset_list:
                        dataset_list[new_dest].numberfiles = dataset_list[new_dest].numberfiles + 1
        # dump
        for dataset_name, dataset in dataset_list.items():
            # Ensure dataset_name is a string
            if isinstance(dataset_name, tuple):
                dataset_name = dataset_name[0]
            if DataServiceUtils.is_sub_dataset(dataset_name):
                tmp_logger.debug(f"made sub:{dataset_name} for nFiles={dataset.numberfiles}")
        # insert datasets to DB
        return self.task_buffer.insertDatasets(dataset_list.values())

    #  subscribe sites to dispatchDBlocks
    def subscribe_dispatch_data_block(self) -> None:
        """
        Subscribe dispatch db method for running the setup process.
        """
        tmp_logger = LogWrapper(self.logger, "<subscribe_dispatch_data_block>")

        disp_error = {}
        failed_jobs = []

        for job in self.jobs:
            # ignore failed jobs
            if job.jobStatus in ["failed", "cancelled"] or job.isCancelled():
                continue
            # ignore no dispatch jobs
            if job.dispatchDBlock == "NULL" or job.computingSite == "NULL":
                continue

            # extract dispatchDBlock and computingSite
            disp = (job.dispatchDBlock, job.computingSite)
            if disp not in disp_error:
                disp_error[disp] = ""

                site_spec = self.site_mapper.getSite(job.computingSite)

                # source
                _, scope_src_output = select_scope(site_spec, job.prodSourceLabel, job.job_label)
                src_ddm_id = site_spec.ddm_output[scope_src_output]

                # destination
                scope_dst_input, _ = select_scope(site_spec, job.prodSourceLabel, job.job_label)
                if site_spec.ddm_endpoints_input[scope_dst_input].isAssociated(src_ddm_id):
                    dst_ddm_id = src_ddm_id
                else:
                    dst_ddm_id = site_spec.ddm_input[scope_dst_input]

                # Get the ATLASDATADISK DDM endpoint in site, it will have preference for pre-staging
                try:
                    dst_datadisk_id = site_spec.setokens_input[scope_dst_input]["ATLASDATADISK"]
                except KeyError:
                    dst_datadisk_id = None

                if dst_datadisk_id and dst_ddm_id != dst_datadisk_id:
                    ddm_id = dst_datadisk_id
                else:
                    ddm_id = dst_ddm_id
                tmp_logger.debug(f"use {ddm_id} for pre-staging")

                # set share and activity
                option_activity = "Production Input"
                if job.prodSourceLabel in ["user", "panda"]:
                    option_activity = "Analysis Input"
                elif job.processingType == "urgent" or job.currentPriority > 1000:
                    option_activity = "Express"

                # taskID
                option_comment = None
                if job.jediTaskID not in ["NULL", 0]:
                    option_comment = f"task_id:{job.jediTaskID}"

                option_owner = None

                tmp_logger.debug(
                    f"register_dataset_subscription {job.dispatchDBlock, ddm_id} "
                    f"{{'activity': {option_activity}, 'lifetime': 7, 'dn': {option_owner}, 'comment': {option_comment}}}"
                )
                for _ in range(3):
                    try:
                        status = rucioAPI.register_dataset_subscription(
                            job.dispatchDBlock,
                            [ddm_id],
                            activity=option_activity,
                            lifetime=7,
                            distinguished_name=option_owner,
                            comment=option_comment,
                        )
                        out = "register_dataset_subscription finished correctly"
                        break
                    except Exception as error:
                        status = False
                        out = f"register_dataset_subscription failed with {str(error)} {traceback.format_exc()}"
                        time.sleep(10)

                if not status:
                    tmp_logger.error(out)
                    disp_error[disp] = "setupper.subscribe_dispatch_data_block() could not register subscription"
                else:
                    tmp_logger.debug(out)

            # failed jobs
            if disp_error[disp] != "":
                if job.jobStatus != "failed":
                    job.jobStatus = "failed"
                    job.ddmErrorCode = ErrorCode.EC_Setupper
                    job.ddmErrorDiag = disp_error[disp]
                    tmp_logger.debug(f"failed PandaID={job.PandaID} with {job.ddmErrorDiag}")
                    failed_jobs.append(job)
        # update failed jobs only. succeeded jobs should be activated by DDM callback
        self.update_failed_jobs(failed_jobs)

    def collect_input_lfns(self):
        # collect input LFNs
        input_lfns = set()
        for tmp_job in self.jobs:
            for tmp_file in tmp_job.Files:
                if tmp_file.type == "input":
                    input_lfns.add(tmp_file.lfn)
                    # Removes attemptNr from LFN name
                    gen_lfn = re.sub("\.\d+$", "", tmp_file.lfn)
                    input_lfns.add(gen_lfn)
                    if tmp_file.GUID not in ["NULL", "", None]:
                        if tmp_file.dataset not in self.lfn_dataset_map:
                            self.lfn_dataset_map[tmp_file.dataset] = {}
                        self.lfn_dataset_map[tmp_file.dataset][tmp_file.lfn] = {
                            "guid": tmp_file.GUID,
                            "chksum": tmp_file.checksum,
                            "md5sum": tmp_file.md5sum,
                            "fsize": tmp_file.fsize,
                            "scope": tmp_file.scope,
                        }
        return input_lfns

    # correct LFN for attemptNr
    def correct_lfn(self) -> None:
        """
        Correct lfn method for running the setup process.
        """

        tmp_logger = LogWrapper(self.logger, "<correct_lfn>")

        lfn_map = {}
        val_map = {}
        prod_error = {}
        missing_datasets = {}
        jobs_waiting = []
        jobs_failed = []
        jobs_processed = []
        all_lfns = {}
        all_guids = {}
        all_scopes = {}
        lfn_ds_map = {}
        replica_map = {}
        tmp_logger.debug("start")

        # collect input LFNs
        input_lfns = self.collect_input_lfns()

        for job in self.jobs:
            # check if sitename is known
            if job.computingSite != "NULL" and job.computingSite not in self.site_mapper.siteSpecList:
                job.jobStatus = "failed"
                job.ddmErrorCode = ErrorCode.EC_Setupper
                job.ddmErrorDiag = f"computingSite:{job.computingSite} is unknown"
                # append job for downstream process
                jobs_processed.append(job)
                continue

            # ignore no prodDBlock jobs or container dataset
            if job.prodDBlock == "NULL":
                # append job to processed list
                jobs_processed.append(job)
                continue

            # collect datasets
            datasets = []
            for file in job.Files:
                # make a list of input datasets
                if file.type == "input" and file.dispatchDBlock == "NULL" and (file.GUID == "NULL" or job.prodSourceLabel in ["managed", "test", "ptest"]):
                    if file.dataset not in datasets:
                        datasets.append(file.dataset)

            # get file information for the LFNs
            for dataset in datasets:
                # make a dataset to lfn map
                if dataset not in lfn_map:
                    prod_error[dataset] = ""
                    lfn_map[dataset] = {}

                    # get the file information (size, checksum, ...) for the selected LFNs
                    status, out = self.get_list_files_in_dataset(dataset, input_lfns)
                    # issue getting the files in dataset
                    if status != 0:
                        tmp_logger.error(out)
                        error_message = f"could not get file list of prodDBlock {dataset}"
                        prod_error[dataset] = error_message
                        tmp_logger.error(error_message)
                        # doesn't exist in DDM, record it as missing
                        if status == -1:
                            missing_datasets[dataset] = f"DS:{dataset} not found in DDM"
                        else:
                            missing_datasets[dataset] = out
                    # issue getting the files in dataset
                    else:
                        # make map (key: LFN w/o attemptNr, value: LFN with attemptNr)
                        items = out
                        try:
                            # loop over all files
                            for tmp_lfn in items:
                                vals = items[tmp_lfn]
                                val_map[tmp_lfn] = vals
                                # Removes attemptNr from LFN name
                                gen_lfn = re.sub("\.\d+$", "", tmp_lfn)
                                if gen_lfn in lfn_map[dataset]:
                                    # get attemptNr
                                    new_att_nr = 0
                                    new_mat = re.search("\.(\d+)$", tmp_lfn)
                                    if new_mat is not None:
                                        new_att_nr = int(new_mat.group(1))
                                    old_att_nr = 0
                                    old_mat = re.search("\.(\d+)$", lfn_map[dataset][gen_lfn])
                                    if old_mat is not None:
                                        old_att_nr = int(old_mat.group(1))
                                    # compare
                                    if new_att_nr > old_att_nr:
                                        lfn_map[dataset][gen_lfn] = tmp_lfn
                                else:
                                    lfn_map[dataset][gen_lfn] = tmp_lfn
                                # mapping from LFN to DS
                                lfn_ds_map[lfn_map[dataset][gen_lfn]] = dataset
                        except Exception:
                            prod_error[dataset] = f"could not convert HTTP-res to map for prodDBlock {dataset}"
                            tmp_logger.error(prod_error[dataset])
                            tmp_logger.error(out)

                    # get replica locations
                    if (job.prodSourceLabel in ["managed", "test"]) and prod_error[dataset] == "" and dataset not in replica_map:
                        if dataset.endswith("/"):
                            status, out = self.get_list_dataset_replicas_in_container(dataset, True)
                        else:
                            status, out = self.get_list_dataset_replicas(dataset)
                        if not status:
                            prod_error[dataset] = f"could not get locations for {dataset}"
                            tmp_logger.error(prod_error[dataset])
                            tmp_logger.error(out)
                        else:
                            replica_map[dataset] = out

            # mark job with a missing dataset as failed and update files as missing
            is_failed = False
            for dataset in datasets:
                if dataset in missing_datasets:
                    job.jobStatus = "failed"
                    job.ddmErrorCode = ErrorCode.EC_GUID
                    job.ddmErrorDiag = missing_datasets[dataset]

                    # set missing
                    for tmp_file in job.Files:
                        if tmp_file.dataset == dataset:
                            tmp_file.status = "missing"

                    # append to job_failed
                    jobs_failed.append(job)
                    is_failed = True
                    tmp_logger.debug(f"{job.PandaID} failed with {missing_datasets[dataset]}")
                    break
            # skip to the next job
            if is_failed:
                continue

            # check for waiting jobs
            is_failed = False
            for dataset in datasets:
                if prod_error[dataset] != "":
                    # append job to waiting list
                    jobs_waiting.append(job)
                    is_failed = True
                    break
            if is_failed:
                continue

            # replace generic LFN with real LFN
            replace_list = []
            is_failed = False
            for file in job.Files:
                if file.type == "input" and file.dispatchDBlock == "NULL":
                    add_to_lfn_map = True
                    if file.GUID == "NULL":
                        # get LFN w/o attemptNr
                        # regexp to remove a dot followed by one or more digits at the end of the file name
                        # for example "data_file.123" will become "data_file"
                        basename = re.sub("\.\d+$", "", file.lfn)
                        if basename == file.lfn:
                            # replace
                            if basename in lfn_map[file.dataset]:
                                file.lfn = lfn_map[file.dataset][basename]
                                replace_list.append((basename, file.lfn))
                        # set GUID
                        if file.lfn in val_map:
                            file.GUID = val_map[file.lfn]["guid"]
                            file.fsize = val_map[file.lfn]["fsize"]
                            file.md5sum = val_map[file.lfn]["md5sum"]
                            file.checksum = val_map[file.lfn]["chksum"]
                            file.scope = val_map[file.lfn]["scope"]
                            # remove white space
                            if file.md5sum is not None:
                                file.md5sum = file.md5sum.strip()
                            if file.checksum is not None:
                                file.checksum = file.checksum.strip()
                    else:
                        if job.prodSourceLabel not in ["managed", "test"]:
                            add_to_lfn_map = False

                    # check missing file
                    if file.GUID == "NULL" or job.prodSourceLabel in [
                        "managed",
                        "test",
                    ]:
                        if file.lfn not in val_map:
                            # append job to waiting list
                            err_msg = f"GUID for {file.lfn} not found in rucio"
                            tmp_logger.error(err_msg)
                            file.status = "missing"
                            if job not in jobs_failed:
                                job.jobStatus = "failed"
                                job.ddmErrorCode = ErrorCode.EC_GUID
                                job.ddmErrorDiag = err_msg
                                jobs_failed.append(job)
                                is_failed = True
                            continue

                    # add to all_lfns/all_guids
                    if add_to_lfn_map:
                        tmp_cloud = job.getCloud()
                        all_lfns.setdefault(tmp_cloud, []).append(file.lfn)
                        all_guids.setdefault(tmp_cloud, []).append(file.GUID)
                        all_scopes.setdefault(tmp_cloud, []).append(file.scope)

            # modify jobParameters
            if not is_failed:
                for patt, repl in replace_list:
                    job.jobParameters = re.sub(f"{patt} ", f"{repl} ", job.jobParameters)
                # append job to processed list
                jobs_processed.append(job)

        # set data summary fields
        for tmp_job in self.jobs:
            try:
                # set only for production/analysis/test
                if tmp_job.prodSourceLabel not in ["managed", "test", "user", "prod_test"] + JobUtils.list_ptest_prod_sources:
                    continue

                # loop over all files
                tmp_job.nInputDataFiles = 0
                tmp_job.inputFileBytes = 0
                tmp_input_file_project = None
                for tmp_file in tmp_job.Files:
                    # use input files and ignore DBR/lib.tgz
                    # lib.tgz is the user sandbox
                    if tmp_file.type == "input" and (not tmp_file.dataset.startswith("ddo")) and not tmp_file.lfn.endswith(".lib.tgz"):
                        tmp_job.nInputDataFiles += 1
                        if tmp_file.fsize not in ["NULL", None, 0, "0"]:
                            tmp_job.inputFileBytes += tmp_file.fsize
                        # get input type and project
                        if tmp_input_file_project is None:
                            tmp_input_items = tmp_file.dataset.split(".")
                            # input project
                            tmp_input_file_project = tmp_input_items[0].split(":")[-1]
                # set input type and project
                if tmp_job.prodDBlock not in ["", None, "NULL"]:
                    # input project
                    if tmp_input_file_project is not None:
                        tmp_job.inputFileProject = tmp_input_file_project
                # protection
                max_input_file_bytes = 10**16 - 1  # 15 digit precision in database column
                tmp_job.inputFileBytes = min(tmp_job.inputFileBytes, max_input_file_bytes)
                # set background-able flag
                tmp_job.setBackgroundableFlag()
                # set input and output file types
                tmp_job.set_input_output_file_types()
            except Exception:
                error_type, error_value = sys.exc_info()[:2]
                tmp_logger.error(f"failed to set data summary fields for PandaID={tmp_job.PandaID}: {error_type} {error_value}")

        # send jobs to jobs_waiting
        self.task_buffer.keepJobs(jobs_waiting)
        # update failed job
        self.update_failed_jobs(jobs_failed)
        # remove waiting/failed jobs
        self.jobs = jobs_processed

        tmp_logger.debug("done")

        # delete huge variables
        del lfn_map
        del val_map
        del prod_error
        del jobs_waiting
        del jobs_processed
        del all_lfns
        del all_guids

    # remove waiting jobs
    def remove_waiting_jobs(self) -> None:
        """
        Remove waiting jobs method for running the setup process.
        """
        jobs_waiting = []
        jobs_processed = []
        for tmp_job in self.jobs:
            if tmp_job.jobStatus == "waiting":
                jobs_waiting.append(tmp_job)
            else:
                jobs_processed.append(tmp_job)
        # send jobs to jobs_waiting
        self.task_buffer.keepJobs(jobs_waiting)
        # remove waiting/failed jobs
        self.jobs = jobs_processed

    # memory checker
    def memory_check(self) -> None:
        """
        Memory check method for running the setup process.
        """
        tmp_logger = LogWrapper(self.logger, "<memory_check>")

        try:
            proc_status = f"/proc/{os.getpid()}/status"
            proc_file = open(proc_status)
            name = ""
            vm_size = ""
            vm_rss = ""
            # extract Name,VmSize,VmRSS
            for line in proc_file:
                if line.startswith("Name:"):
                    name = line.split()[-1]
                    continue
                if line.startswith("VmSize:"):
                    vm_size = ""
                    for item in line.split()[1:]:
                        vm_size += item
                    continue
                if line.startswith("VmRSS:"):
                    vm_rss = ""
                    for item in line.split()[1:]:
                        vm_rss += item
                    continue
            proc_file.close()
            tmp_logger.debug(f"PID={os.getpid()} Name={name} VSZ={vm_size} RSS={vm_rss}")
        except Exception:
            error_type, error_value = sys.exc_info()[:2]
            tmp_logger.error(f"{error_type} {error_value}")
            tmp_logger.debug(f"PID={os.getpid()} unknown")
            return

    # get list of files in dataset
    def get_list_files_in_dataset(self, dataset: str, file_list: Optional[List[str]] = None, use_cache: bool = True) -> Tuple[int, List[str]]:
        """
        Get list files in dataset method for running the setup process.

        :param dataset: The dataset to get the list of files from.
        :param file_list: The list of files. Defaults to None.
        :param use_cache: Whether to use cache. Defaults to True.
        :return: A tuple containing the status and the list of files.
        """

        tmp_logger = LogWrapper(self.logger, "<get_list_files_in_dataset>")

        # use cache data
        if use_cache and dataset in self.lfn_dataset_map:
            return 0, self.lfn_dataset_map[dataset]
        status = None
        items = []
        for _ in range(3):
            try:
                tmp_logger.debug(f"list_files_in_dataset {dataset}")
                items, _ = rucioAPI.list_files_in_dataset(dataset, file_list=file_list)
                status = 0
                break
            except DataIdentifierNotFound:
                status = -1
                break
            except Exception:
                status = -2

        if status != 0:
            error_type, error_value = sys.exc_info()[:2]
            out = f"{error_type} {error_value}"
            return status, out
        # keep to avoid redundant lookup
        self.lfn_dataset_map[dataset] = items
        return status, items

    # get list of datasets in container
    def get_list_dataset_in_container(self, container: str) -> Tuple[bool, List[str]]:
        """
        Get list dataset in container method for running the setup process.

        :param container: The container to get the list of datasets from.
        :return: A tuple containing a boolean indicating the status and the list of datasets.
        """

        tmp_logger = LogWrapper(self.logger, "<get_list_dataset_in_container>")

        # get datasets in container
        tmp_logger.debug(container)
        out = ""
        for _ in range(3):
            datasets, out = rucioAPI.list_datasets_in_container(container)
            if datasets is not None:
                return True, datasets
            time.sleep(10)
        tmp_logger.error(out)
        return False, out

    # get datasets in container
    def get_list_dataset_replicas_in_container(self, container: str, get_map: bool = False) -> Tuple[int, str]:
        """
        Get list dataset replicas in container method for running the setup process.

        :param container: The container to get the list of dataset replicas from.
        :param get_map: Whether to get the map. Defaults to False.
        :return: A tuple containing the status and the map of dataset replicas.
        """
        tmp_logger = LogWrapper(self.logger, "<get_list_dataset_replicas_in_container>")
        tmp_logger.debug(container)

        datasets = None
        out = ""
        for _ in range(3):
            datasets, out = rucioAPI.list_datasets_in_container(container)
            if datasets is None:
                time.sleep(10)
            else:
                break
        if datasets is None:
            tmp_logger.error(out)
            if get_map:
                return False, out
            return 1, out

        # loop over all datasets
        all_rep_map = {}
        for dataset in datasets:
            tmp_logger.debug(f"listDatasetReplicas {dataset}")
            status, out = self.get_list_dataset_replicas(dataset)
            tmp_logger.debug(out)
            if not status:
                if get_map:
                    return False, out
                return status, out
            tmp_rep_sites = out
            # get map
            if get_map:
                all_rep_map[dataset] = tmp_rep_sites
                continue
            # otherwise get sum
            for site_id in tmp_rep_sites:
                stat_list = tmp_rep_sites[site_id]
                if site_id not in all_rep_map:
                    # append
                    all_rep_map[site_id] = [
                        stat_list[-1],
                    ]
                else:
                    # add
                    new_st_map = {}
                    for st_name in all_rep_map[site_id][0]:
                        st_num = all_rep_map[site_id][0][st_name]
                        if st_name in stat_list[-1]:
                            # try mainly for archived=None
                            try:
                                new_st_map[st_name] = st_num + stat_list[-1][st_name]
                            except Exception:
                                new_st_map[st_name] = st_num
                        else:
                            new_st_map[st_name] = st_num
                    all_rep_map[site_id] = [
                        new_st_map,
                    ]
        # return
        tmp_logger.debug(str(all_rep_map))
        if get_map:
            return True, all_rep_map
        return 0, str(all_rep_map)

    # get list of replicas for a dataset
    def get_list_dataset_replicas(self, dataset: str, get_map: bool = True) -> Tuple[bool, str]:
        """
        Get list dataset replicas method for running the setup process.

        :param dataset: The dataset to get the list of replicas from.
        :param get_map: Whether to get the map. Defaults to True.
        :return: A tuple containing a boolean indicating the status and the map of dataset replicas.
        """

        tmp_logger = LogWrapper(self.logger, "<get_list_dataset_replicas>")

        out = ""
        for attempt in range(3):
            tmp_logger.debug(f"{attempt}/{3} listDatasetReplicas {dataset}")
            status, out = rucioAPI.list_dataset_replicas(dataset)
            if status != 0:
                time.sleep(10)
            else:
                break
        # result
        if status != 0:
            tmp_logger.error(out)
            tmp_logger.error(f"bad response for {dataset}")
            if get_map:
                return False, {}
            else:
                return 1, str({})
        try:
            ret_map = out
            tmp_logger.debug(f"list_dataset_replicas->{str(ret_map)}")
            if get_map:
                return True, ret_map
            else:
                return 0, str(ret_map)
        except Exception:
            tmp_logger.error(out)
            tmp_logger.error(f"could not convert HTTP-res to replica map for {dataset}")
            if get_map:
                return False, {}
            else:
                return 1, str({})

    # dynamic data placement for analysis jobs
    def dynamic_data_placement(self) -> None:
        """
        Dynamic data placement method for running the setup process.
        """
        # only first submission
        if not self.first_submission:
            return
        # no jobs
        if len(self.jobs) == 0:
            return
        # only successful analysis
        if self.jobs[0].jobStatus in ["failed", "cancelled"] or self.jobs[0].isCancelled() or (not self.jobs[0].prodSourceLabel in ["user", "panda"]):
            return
        # disable for JEDI
        if self.jobs[0].lockedby == "jedi":
            return
        return

    def collect_existing_files(self):
        """
        Collects existing files to avoid deletion when jobs are queued.
        This method iterates over all jobs and collects files that should not be deleted,
        organizing them by destination DDM endpoint and log dataset name.

        :return: A dictionary mapping (destination DDM endpoint, log dataset name) to a list of files.
        """
        dataset_file_map = {}
        n_max_jobs = 20
        n_jobs_map = {}
        for tmp_job in self.jobs:
            # use production or test jobs only
            if tmp_job.prodSourceLabel not in ["managed", "test"]:
                continue
            # skip for prefetcher or transferType=direct
            if tmp_job.usePrefetcher() or tmp_job.transferType == "direct":
                continue
            # ignore inappropriate status
            if tmp_job.jobStatus in ["failed", "cancelled", "waiting"] or tmp_job.isCancelled():
                continue
            # check cloud
            if tmp_job.getCloud() == "ND" and self.site_mapper.getSite(tmp_job.computingSite).cloud == "ND":
                continue
            # look for log _sub dataset to be used as a key
            log_sub_ds_name = ""
            for tmp_file in tmp_job.Files:
                if tmp_file.type == "log":
                    log_sub_ds_name = tmp_file.destinationDBlock
                    break
            # append site
            dest_site = self.site_mapper.getSite(tmp_job.computingSite)
            scope_dest_input, _ = select_scope(dest_site, tmp_job.prodSourceLabel, tmp_job.job_label)
            dest_ddm_id = dest_site.ddm_input[scope_dest_input]
            map_key_job = (dest_ddm_id, log_sub_ds_name)
            # increment the number of jobs per key
            if map_key_job not in n_jobs_map:
                n_jobs_map[map_key_job] = 0
            map_key = (
                dest_ddm_id,
                log_sub_ds_name,
                n_jobs_map[map_key_job] // n_max_jobs,
            )
            n_jobs_map[map_key_job] += 1
            if map_key not in dataset_file_map:
                dataset_file_map[map_key] = {}
            # add files
            for tmp_file in tmp_job.Files:
                if tmp_file.type != "input":
                    continue
                # if files are unavailable at the dest site normal dis datasets contain them
                # or files are cached
                if tmp_file.status not in ["ready"]:
                    continue
                # if available at Satellite
                real_dest_ddm_id = (dest_ddm_id,)
                if (
                    tmp_job.getCloud() in self.available_lfns_in_satellites
                    and tmp_file.dataset in self.available_lfns_in_satellites[tmp_job.getCloud()]
                    and tmp_job.computingSite in self.available_lfns_in_satellites[tmp_job.getCloud()][tmp_file.dataset]["sites"]
                    and tmp_file.lfn in self.available_lfns_in_satellites[tmp_job.getCloud()][tmp_file.dataset]["sites"][tmp_job.computingSite]
                ):
                    real_dest_ddm_id = self.available_lfns_in_satellites[tmp_job.getCloud()][tmp_file.dataset]["siteDQ2IDs"][tmp_job.computingSite]
                    real_dest_ddm_id = tuple(real_dest_ddm_id)
                # append
                if real_dest_ddm_id not in dataset_file_map[map_key]:
                    dataset_file_map[map_key][real_dest_ddm_id] = {
                        "taskID": tmp_job.taskID,
                        "PandaID": tmp_job.PandaID,
                        "useZipToPin": tmp_job.useZipToPin(),
                        "files": {},
                    }
                if tmp_file.lfn not in dataset_file_map[map_key][real_dest_ddm_id]["files"]:
                    # add scope
                    tmp_lfn = f"{tmp_file.scope}:{tmp_file.lfn}"
                    dataset_file_map[map_key][real_dest_ddm_id]["files"][tmp_file.lfn] = {
                        "lfn": tmp_lfn,
                        "guid": tmp_file.GUID,
                        "fileSpecs": [],
                    }
                # add file spec
                dataset_file_map[map_key][real_dest_ddm_id]["files"][tmp_file.lfn]["fileSpecs"].append(tmp_file)
        return dataset_file_map

    def create_dispatch_datasets(self, dataset_file_map):
        """
        Creates dispatch datasets for the collected files.
        Returns a list of datasets to be inserted into the database.
        """

        tmp_logger = LogWrapper(self.logger, "<create_dispatch_datasets>")

        # loop over all locations
        disp_list = []
        for _, tmp_dum_val in dataset_file_map.items():
            for tmp_location_list in tmp_dum_val:
                tmp_val = tmp_dum_val[tmp_location_list]
                for tmp_location in tmp_location_list:
                    tmp_file_list = tmp_val["files"]
                    if tmp_file_list == {}:
                        continue
                    n_max_files = 500
                    i_files = 0
                    i_loop = 0
                    while i_files < len(tmp_file_list):
                        sub_file_names = list(tmp_file_list)[i_files : i_files + n_max_files]
                        if len(sub_file_names) == 0:
                            break
                        # dis name
                        dis_dispatch_block = f"panda.{tmp_val['taskID']}.{time.strftime('%m.%d')}.GEN.{str(uuid.uuid4())}_dis0{i_loop}{tmp_val['PandaID']}"
                        i_files += n_max_files
                        lfns = []
                        guids = []
                        fsizes = []
                        chksums = []
                        tmp_zip_out = {}
                        if tmp_val["useZipToPin"]:
                            dids = [tmp_file_list[tmp_sub_file_name]["lfn"] for tmp_sub_file_name in sub_file_names]
                            tmp_zip_stat, tmp_zip_out = rucioAPI.get_zip_files(dids, [tmp_location])
                            if not tmp_zip_stat:
                                tmp_logger.debug(f"failed to get zip files : {tmp_zip_out}")
                                tmp_zip_out = {}
                        for tmp_sub_file_name in sub_file_names:
                            tmp_lfn = tmp_file_list[tmp_sub_file_name]["lfn"]
                            if tmp_lfn in tmp_zip_out:
                                tmp_zip_file_name = f"{tmp_zip_out[tmp_lfn]['scope']}:{tmp_zip_out[tmp_lfn]['name']}"
                                if tmp_zip_file_name not in lfns:
                                    lfns.append(tmp_zip_file_name)
                                    guids.append(tmp_zip_out[tmp_lfn]["guid"])
                                    fsizes.append(tmp_zip_out[tmp_lfn]["bytes"])
                                    chksums.append(tmp_zip_out[tmp_lfn]["adler32"])
                            else:
                                lfns.append(tmp_lfn)
                                guids.append(tmp_file_list[tmp_sub_file_name]["guid"])
                                fsizes.append(int(tmp_file_list[tmp_sub_file_name]["fileSpecs"][0].fsize))
                                chksums.append(tmp_file_list[tmp_sub_file_name]["fileSpecs"][0].checksum)
                            # set dis name
                            for tmp_file_spec in tmp_file_list[tmp_sub_file_name]["fileSpecs"]:
                                if tmp_file_spec.status in ["ready"] and tmp_file_spec.dispatchDBlock == "NULL":
                                    tmp_file_spec.dispatchDBlock = dis_dispatch_block
                        # register datasets
                        i_loop += 1
                        max_attempt = 3
                        is_ok = False
                        metadata = {"hidden": True, "purge_replicas": 0}
                        if tmp_val["taskID"] not in [None, "NULL"]:
                            metadata["task_id"] = str(tmp_val["taskID"])

                        tmp_logger.debug(f"ext registerNewDataset {dis_dispatch_block} {str(lfns)} {str(guids)} {str(fsizes)} {str(chksums)} {str(metadata)}")

                        for attempt in range(max_attempt):
                            try:
                                out = rucioAPI.register_dataset(
                                    dis_dispatch_block,
                                    lfns,
                                    guids,
                                    fsizes,
                                    chksums,
                                    lifetime=7,
                                    scope="panda",
                                    metadata=metadata,
                                )
                                self.logger.debug(out)
                                is_ok = True
                                break
                            except Exception:
                                error_type, error_value = sys.exc_info()[:2]
                                tmp_logger.error(f"ext registerDataset : failed with {error_type}:{error_value}" + traceback.format_exc())
                                if attempt + 1 == max_attempt:
                                    break
                                tmp_logger.debug(f"sleep {attempt}/{max_attempt}")
                                time.sleep(10)
                        # failure
                        if not is_ok:
                            continue
                        # get VUID
                        try:
                            vuid = out["vuid"]
                            # dataset spec. currentfiles is used to count the number of failed jobs
                            dataset = DatasetSpec()
                            dataset.vuid = vuid
                            dataset.name = dis_dispatch_block
                            dataset.type = "dispatch"
                            dataset.status = "defined"
                            dataset.numberfiles = len(lfns)
                            dataset.currentfiles = 0
                            if tmp_val["taskID"] not in [None, "NULL"]:
                                dataset.MoverID = tmp_val["taskID"]
                            disp_list.append(dataset)
                        except Exception:
                            error_type, error_value = sys.exc_info()[:2]
                            tmp_logger.error(f"ext registerNewDataset : failed to decode VUID for {dis_dispatch_block} - {error_type} {error_value}")
                            continue
                        # freezeDataset dispatch dataset
                        tmp_logger.debug(f"freezeDataset {dis_dispatch_block}")

                        for attempt in range(3):
                            status = False
                            try:
                                rucioAPI.close_dataset(dis_dispatch_block)
                                status = True
                                break
                            except Exception:
                                error_type, error_value = sys.exc_info()[:2]
                                out = f"failed to close : {error_type} {error_value}"
                                time.sleep(10)
                        if not status:
                            tmp_logger.error(out)
                            continue
                        # register location
                        is_ok = False
                        tmp_logger.debug(f"ext registerDatasetLocation {dis_dispatch_block} {tmp_location} {7}days asynchronous=True")
                        max_attempt = 3
                        for attempt in range(max_attempt):
                            try:
                                out = rucioAPI.register_dataset_location(
                                    dis_dispatch_block,
                                    [tmp_location],
                                    7,
                                    activity="Production Input",
                                    scope="panda",
                                    grouping="NONE",
                                )
                                tmp_logger.debug(out)
                                is_ok = True
                                break
                            except Exception:
                                error_type, error_value = sys.exc_info()[:2]
                                tmp_logger.error(f"ext registerDatasetLocation : failed with {error_type}:{error_value}")
                                if attempt + 1 == max_attempt:
                                    break
                                tmp_logger.debug(f"sleep {attempt}/{max_attempt}")
                                time.sleep(10)

                        # failure
                        if not is_ok:
                            continue
        return disp_list

    # make dis datasets for existing files to avoid deletion when jobs are queued
    def make_dis_datasets_for_existing_files(self) -> None:
        """
        Make dis datasets for existing files method for running the setup process.
        """
        tmp_logger = LogWrapper(self.logger, "<make_dis_datasets_for_existing_files>")
        tmp_logger.debug("start")

        # collect existing files
        dataset_file_map = self.collect_existing_files()
        # create dispatch datasets for the collected files
        disp_list = self.create_dispatch_datasets(dataset_file_map)

        # insert datasets to DB
        self.task_buffer.insertDatasets(disp_list)
        tmp_logger.debug("finished")
        return

    # make subscription
    def make_subscription(self, dataset: str, ddm_id: str) -> bool:
        """
        Make subscription method for running the setup process.

        :param dataset: The dataset to make the subscription for.
        :param ddm_id: The DDM ID.
        :return: A boolean indicating whether the subscription was made successfully.
        """
        ret_failed = False
        tmp_logger = LogWrapper(self.logger, "<make_subscription>")
        tmp_logger.debug(f"register_dataset_subscription {dataset} {ddm_id}")
        for _ in range(3):
            try:
                # register subscription
                status = rucioAPI.register_dataset_subscription(dataset, [ddm_id], activity="Production Input")
                out = "OK"
                break
            except Exception:
                status = False
                error_type, error_value = sys.exc_info()[:2]
                out = f"{error_type} {error_value}"
                time.sleep(10)

        if not status:
            tmp_logger.error(out)
            return ret_failed

        tmp_logger.debug(f"{status} {out}")
        return True

    # setup jumbo jobs
    def setup_jumbo_jobs(self) -> None:
        """
        Setup jumbo jobs method for running the setup process.
        """

        tmp_logger = LogWrapper(self.logger, "<setup_jumbo_jobs>")

        if len(self.jumbo_jobs) == 0:
            return
        tmp_logger.debug("start")
        # get files in datasets
        datasets_lfns_map = {}
        failed_ds = set()
        for jumbo_job_spec in self.jumbo_jobs:
            for tmp_file_spec in jumbo_job_spec.Files:
                # only input
                if tmp_file_spec.type not in ["input"]:
                    continue
                # get files
                if tmp_file_spec.dataset not in datasets_lfns_map:
                    if tmp_file_spec.dataset not in failed_ds:
                        tmp_stat, tmp_map = self.get_list_files_in_dataset(tmp_file_spec.dataset, use_cache=False)
                        # failed
                        if tmp_stat != 0:
                            failed_ds.add(tmp_file_spec.dataset)
                            tmp_logger.debug(f"failed to get files in {tmp_file_spec.dataset} with {tmp_map}")
                        else:
                            # append
                            datasets_lfns_map[tmp_file_spec.dataset] = tmp_map
                # set failed if file lookup failed
                if tmp_file_spec.dataset in failed_ds:
                    jumbo_job_spec.jobStatus = "failed"
                    jumbo_job_spec.ddmErrorCode = ErrorCode.EC_GUID
                    jumbo_job_spec.ddmErrorDiag = f"failed to get files in {tmp_file_spec.dataset}"
                    break
        # make dis datasets
        ok_jobs = []
        ng_jobs = []
        for jumbo_job_spec in self.jumbo_jobs:
            # skip failed
            if jumbo_job_spec.jobStatus == "failed":
                ng_jobs.append(jumbo_job_spec)
                continue
            # get datatype
            try:
                tmp_data_type = jumbo_job_spec.prodDBlock.split(".")[-2]
                if len(tmp_data_type) > 20:
                    raise RuntimeError(f"data type is too log : {len(tmp_data_type)} chars")
            except Exception:
                # default
                tmp_data_type = "GEN"
            # files for jumbo job
            lfns_for_jumbo = self.task_buffer.getLFNsForJumbo(jumbo_job_spec.jediTaskID)
            # make dis dataset name
            dispatch_data_block = f"panda.{jumbo_job_spec.taskID}.{time.strftime('%m.%d.%H%M')}.{tmp_data_type}.jumbo_dis{jumbo_job_spec.PandaID}"
            # collect file attributes
            lfns = []
            guids = []
            sizes = []
            checksums = []
            for tmp_file_spec in jumbo_job_spec.Files:
                # only input
                if tmp_file_spec.type not in ["input"]:
                    continue
                for tmp_lfn in datasets_lfns_map[tmp_file_spec.dataset]:
                    tmp_var = datasets_lfns_map[tmp_file_spec.dataset][tmp_lfn]
                    tmp_lfn = f"{tmp_var['scope']}:{tmp_lfn}"
                    if tmp_lfn not in lfns_for_jumbo:
                        continue
                    lfns.append(tmp_lfn)
                    guids.append(tmp_var["guid"])
                    sizes.append(tmp_var["fsize"])
                    checksums.append(tmp_var["chksum"])
                # set dis dataset
                tmp_file_spec.dispatchDBlock = dispatch_data_block
            # register and subscribe dis dataset
            if len(lfns) != 0:
                # set dis dataset
                jumbo_job_spec.dispatchDBlock = dispatch_data_block
                # register dis dataset
                try:
                    tmp_logger.debug(f"register_dataset {dispatch_data_block} with {len(lfns)} files")
                    out = rucioAPI.register_dataset(dispatch_data_block, lfns, guids, sizes, checksums, lifetime=14)
                    vuid = out["vuid"]
                    rucioAPI.close_dataset(dispatch_data_block)
                except Exception:
                    error_type, error_value = sys.exc_info()[:2]
                    tmp_logger.debug(f"failed to register jumbo dis dataset {dispatch_data_block} with {error_type}:{error_value}")
                    jumbo_job_spec.jobStatus = "failed"
                    jumbo_job_spec.ddmErrorCode = ErrorCode.EC_Setupper
                    jumbo_job_spec.ddmErrorDiag = f"failed to register jumbo dispatch dataset {dispatch_data_block}"
                    ng_jobs.append(jumbo_job_spec)
                    continue
                # subscribe dis dataset
                try:
                    tmp_site_spec = self.site_mapper.getSite(jumbo_job_spec.computingSite)
                    scope_input, _ = select_scope(
                        tmp_site_spec,
                        jumbo_job_spec.prodSourceLabel,
                        jumbo_job_spec.job_label,
                    )
                    end_point = tmp_site_spec.ddm_input[scope_input]
                    tmp_logger.debug(f"register_dataset_subscription {dispatch_data_block} to {end_point}")
                    rucioAPI.register_dataset_subscription(
                        dispatch_data_block,
                        [end_point],
                        lifetime=14,
                        activity="Production Input",
                    )
                except Exception:
                    error_type, error_value = sys.exc_info()[:2]
                    tmp_logger.debug(f"failed to subscribe jumbo dis dataset {dispatch_data_block} to {end_point} with {error_type}:{error_value}")
                    jumbo_job_spec.jobStatus = "failed"
                    jumbo_job_spec.ddmErrorCode = ErrorCode.EC_Setupper
                    jumbo_job_spec.ddmErrorDiag = f"failed to subscribe jumbo dispatch dataset {dispatch_data_block} to {end_point}"
                    ng_jobs.append(jumbo_job_spec)
                    continue

                # add dataset in DB
                dataset = DatasetSpec()
                dataset.vuid = vuid
                dataset.name = dispatch_data_block
                dataset.type = "dispatch"
                dataset.status = "defined"
                dataset.numberfiles = len(lfns)
                dataset.currentfiles = 0
                dataset.MoverID = jumbo_job_spec.jediTaskID
                self.task_buffer.insertDatasets([dataset])
            # set destination
            jumbo_job_spec.destinationSE = jumbo_job_spec.computingSite
            for tmp_file_spec in jumbo_job_spec.Files:
                if tmp_file_spec.type in ["output", "log"] and DataServiceUtils.getDistributedDestination(tmp_file_spec.destinationDBlockToken) is None:
                    tmp_file_spec.destinationSE = jumbo_job_spec.computingSite
            ok_jobs.append(jumbo_job_spec)
        # update failed jobs
        self.update_failed_jobs(ng_jobs)
        self.jumbo_jobs = ok_jobs
        tmp_logger.debug("done")
        return

    # make sub dataset name
    def make_sub_dataset_name(self, original_name: str, serial_number: int, task_id: int) -> str:
        """
        Make sub dataset name method for running the setup process.

        :param original_name: The original name of the dataset.
        :param serial_number: The serial number.
        :param task_id: The task ID.
        :return: The sub dataset name.
        """
        try:
            task_id = int(task_id)
            if original_name.startswith("user") or original_name.startswith("panda"):
                part_name = ".".join(original_name.split(".")[:3])
            else:
                part_name = f"{'.'.join(original_name.split('.')[:2])}.NA.{'.'.join(original_name.split('.')[3:5])}"
            return f"{part_name}.{task_id}_sub{serial_number}"
        except Exception:
            return f"{original_name}_sub{serial_number}"
