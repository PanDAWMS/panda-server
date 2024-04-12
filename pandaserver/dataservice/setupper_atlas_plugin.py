"""
setup dataset for ATLAS

"""

import datetime
import re
import sys
import time
import traceback
import uuid

import pandaserver.brokerage.broker

from typing import List, Dict, Tuple, Optional
from pandaserver.brokerage.SiteMapper import SiteMapper
from pandaserver.config import panda_config
from pandaserver.dataservice import DataServiceUtils, ErrorCode
from pandaserver.dataservice.DataServiceUtils import select_scope
from pandaserver.dataservice.ddm import rucioAPI
from pandaserver.dataservice.setupper_plugin_base import SetupperPluginBase
from pandaserver.taskbuffer import EventServiceUtils, JobUtils
from pandaserver.taskbuffer.DatasetSpec import DatasetSpec
from rucio.common.exception import (
    DataIdentifierAlreadyExists,
    DataIdentifierNotFound,
    Duplicate,
    FileAlreadyExists,
)


class SetupperAtlasPlugin(SetupperPluginBase):
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
            "panda_ddm": False,
            "ddm_attempt": 0,
            "only_ta": False,
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
        # replica map for special brokerage
        self.replica_map_for_broker = {}
        # available files at T2
        self.available_lfns_in_t2 = {}
        # list of missing datasets
        self.missing_dataset_list = {}
        # lfn ds map
        self.lfn_dataset_map = {}
        # missing files at T1
        self.missing_files_in_t1 = {}
        # source label
        self.prod_source_label = None
        self.job_label = None

    # main
    def run(self) -> None:
        """
        Main method for running the setup process.
        """
        try:
            self.logger.debug("start run()")
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
                self.logger.debug(bunch_tag)
                self.prod_source_label = tag_job.prodSourceLabel
                self.job_label = tag_job.job_label
            # instantiate site mapper
            self.site_mapper = SiteMapper(self.taskBuffer)
            # correctLFN
            self.correct_lfn()
            # run full Setupper
            if not self.only_ta:
                # invoke brokerage
                self.logger.debug("brokerSchedule")
                self.memory_check()
                pandaserver.brokerage.broker.schedule(
                    self.jobs,
                    self.taskBuffer,
                    self.site_mapper,
                    replicaMap=self.replica_map_for_broker,
                    t2FilesMap=self.available_lfns_in_t2,
                )
                # remove waiting jobs
                self.remove_waiting_jobs()
                # setup dispatch dataset
                self.logger.debug("setupSource")
                self.memory_check()
                self.setup_source()
                self.memory_check()
                # sort by site so that larger subs are created in the next step
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
                    for tmp_site_key in tmp_job_map:
                        tmp_job_list += tmp_job_map[tmp_site_key]
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
                    # make one sub per job so that each job doesn't have to wait for others to be done
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
            self.logger.debug(f"{bunch_tag} took {reg_time.seconds}sec")
            self.logger.debug("end run()")
        except Exception:
            error_type, error_value = sys.exc_info()[:2]
            err_str = f"run() : {error_type} {error_value}"
            err_str.strip()
            err_str += traceback.format_exc()
            self.logger.error(err_str)

    # post run
    def post_run(self) -> None:
        """
        Post run method for running the setup process.
        """
        try:
            if not self.only_ta:
                self.logger.debug("start postRun()")
                self.memory_check()
                # subscribe sites distpatchDBlocks. this must be the last method
                self.logger.debug("subscribeDistpatchDB")
                self.subscribe_distpatch_db()
                # dynamic data placement for analysis jobs
                self.memory_check()
                self.dynamic_data_placement()
                # make subscription for missing
                self.memory_check()
                self.make_subscription_for_missing()
                self.memory_check()
                self.logger.debug("end postRun()")
        except Exception:
            error_type, error_value = sys.exc_info()[:2]
            self.logger.error(f"postRun() : {error_type} {error_value}")

    # make dispatchDBlocks, insert prod/dispatchDBlock to database
    def setup_source(self) -> None:
        """
        Setup source method for running the setup process.
        """
        file_list = {}
        prod_list = []
        prod_error = {}
        disp_site_map = {}
        disp_error = {}
        back_end_map = {}
        ds_task_map = dict()
        use_zip_to_pin_map = dict()
        # extract prodDBlock
        for job in self.jobs:
            # ignore failed jobs
            if job.jobStatus in ["failed", "cancelled"] or job.isCancelled():
                continue
            # production datablock
            if job.prodDBlock != "NULL" and job.prodDBlock and (job.prodSourceLabel not in ["user", "panda"]):
                # get VUID and record prodDBlock into DB
                if job.prodDBlock not in prod_error:
                    self.logger.debug("listDatasets " + job.prodDBlock)
                    prod_error[job.prodDBlock] = ""
                    for attempt in range(3):
                        new_out, err_msg = rucioAPI.list_datasets(job.prodDBlock)
                        if new_out is None:
                            time.sleep(10)
                        else:
                            break
                    if new_out is None:
                        prod_error[job.prodDBlock] = f"Setupper._setupSource() could not get VUID of prodDBlock with {err_msg}"
                        self.logger.error(prod_error[job.prodDBlock])
                    else:
                        self.logger.debug(new_out)
                        try:
                            vuids = new_out[job.prodDBlock]["vuids"]
                            n_files = 0
                            # dataset spec
                            ds = DatasetSpec()
                            ds.vuid = vuids[0]
                            ds.name = job.prodDBlock
                            ds.type = "input"
                            ds.status = "completed"
                            ds.numberfiles = n_files
                            ds.currentfiles = n_files
                            prod_list.append(ds)
                        except Exception:
                            error_type, error_value = sys.exc_info()[:2]
                            self.logger.error(f"_setupSource() : {error_type} {error_value}")
                            prod_error[job.prodDBlock] = "Setupper._setupSource() could not decode VUID of prodDBlock"
                # error
                if prod_error[job.prodDBlock] != "":
                    if job.jobStatus != "failed":
                        job.jobStatus = "failed"
                        job.ddmErrorCode = ErrorCode.EC_Setupper
                        job.ddmErrorDiag = prod_error[job.prodDBlock]
                        self.logger.debug(f"failed PandaID={job.PandaID} with {job.ddmErrorDiag}")
                    continue
            # dispatch datablock
            if job.dispatchDBlock != "NULL":
                # useZipToPin mapping
                use_zip_to_pin_map[job.dispatchDBlock] = job.useZipToPin()
                # src/dst sites
                tmp_src_id = "BNL_ATLAS_1"
                if self.site_mapper.checkCloud(job.getCloud()):
                    # use cloud's source
                    tmp_src_id = self.site_mapper.getCloud(job.getCloud())["source"]

                src_site_spec = self.site_mapper.getSite(tmp_src_id)
                scope_src_input, scope_src_output = select_scope(src_site_spec, job.prodSourceLabel, job.job_label)
                src_ddm_id = src_site_spec.ddm_output[scope_src_output]
                # use src_ddm_id as dst_dq2_id when it is associated to dest
                dst_site_spec = self.site_mapper.getSite(job.computingSite)
                scope_dst_input, scope_dst_output = select_scope(dst_site_spec, job.prodSourceLabel, job.job_label)
                if dst_site_spec.ddm_endpoints_input[scope_dst_input].isAssociated(src_ddm_id):
                    dst_dq2_id = src_ddm_id
                else:
                    dst_dq2_id = dst_site_spec.ddm_input[scope_dst_input]
                disp_site_map[job.dispatchDBlock] = {
                    "src": src_ddm_id,
                    "dst": dst_dq2_id,
                    "site": job.computingSite,
                }
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
                            if file.fsize in ["NULL", 0]:
                                file_list[job.dispatchDBlock]["fsizes"].append(None)
                            else:
                                file_list[job.dispatchDBlock]["fsizes"].append(int(file.fsize))
                            if file.md5sum in ["NULL", ""]:
                                file_list[job.dispatchDBlock]["md5sums"].append(None)
                            elif file.md5sum.startswith("md5:"):
                                file_list[job.dispatchDBlock]["md5sums"].append(file.md5sum)
                            else:
                                file_list[job.dispatchDBlock]["md5sums"].append(f"md5:{file.md5sum}")
                            if file.checksum in ["NULL", ""]:
                                file_list[job.dispatchDBlock]["chksums"].append(None)
                            else:
                                file_list[job.dispatchDBlock]["chksums"].append(file.checksum)
                        # get replica locations
                        self.replica_map.setdefault(job.dispatchDBlock, {})
                        if file.dataset not in self.all_replica_map:
                            if file.dataset.endswith("/"):
                                status, out = self.get_list_dataset_replicas_in_container(file.dataset, True)
                            else:
                                status, out = self.get_list_dataset_replicas(file.dataset)
                            if not status:
                                self.logger.error(out)
                                disp_error[job.dispatchDBlock] = f"could not get locations for {file.dataset}"
                                self.logger.error(disp_error[job.dispatchDBlock])
                            else:
                                self.logger.debug(out)
                                self.all_replica_map[file.dataset] = out
                        if file.dataset in self.all_replica_map:
                            self.replica_map[job.dispatchDBlock][file.dataset] = self.all_replica_map[file.dataset]
        # register dispatch dataset
        disp_list = []
        for dispatch_data_block in file_list:
            # ignore empty dataset
            if len(file_list[dispatch_data_block]["lfns"]) == 0:
                continue
            # use Rucio
            if (not self.panda_ddm) and job.prodSourceLabel != "ddm":
                # register dispatch dataset
                self.disp_file_list[dispatch_data_block] = file_list[dispatch_data_block]
                if not use_zip_to_pin_map[dispatch_data_block]:
                    dis_files = file_list[dispatch_data_block]
                else:
                    dids = file_list[dispatch_data_block]["lfns"]
                    tmp_zip_stat, tmp_zip_out = rucioAPI.get_zip_files(dids, None)
                    if not tmp_zip_stat:
                        self.logger.debug(f"failed to get zip files : {tmp_zip_out}")
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
                ddm_back_end = back_end_map[dispatch_data_block]
                if ddm_back_end is None:
                    ddm_back_end = "rucio"
                metadata = {"hidden": True, "purge_replicas": 0}
                if dispatch_data_block in ds_task_map and ds_task_map[dispatch_data_block] not in [
                    "NULL",
                    0,
                ]:
                    metadata["task_id"] = str(ds_task_map[dispatch_data_block])
                tmp_msg = "registerDataset {ds} {meta}"
                self.logger.debug(tmp_msg.format(ds=dispatch_data_block, meta=str(metadata)))
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
                        self.logger.error(f"registerDataset : failed with {err_str}")
                        if attempt + 1 == max_attempt:
                            break
                        self.logger.debug(f"sleep {attempt}/{max_attempt}")
                        time.sleep(10)
                if not is_ok:
                    disp_error[dispatch_data_block] = "Setupper._setupSource() could not register dispatch_data_block with {0}".format(err_str.split("\n")[-1])
                    continue
                self.logger.debug(out)
                new_out = out
                # freezeDataset dispatch dataset
                self.logger.debug("closeDataset " + dispatch_data_block)
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
                    self.logger.error(out)
                    disp_error[dispatch_data_block] = f"Setupper._setupSource() could not freeze dispatch_data_block with {out}"
                    continue
            else:
                # use PandaDDM
                self.disp_file_list[dispatch_data_block] = file_list[dispatch_data_block]
                # create a fake vuid
                new_out = {"vuid": str(uuid.uuid4())}
            # get VUID
            try:
                vuid = new_out["vuid"]
                # dataset spec. currentfiles is used to count the number of failed jobs
                ds = DatasetSpec()
                ds.vuid = vuid
                ds.name = dispatch_data_block
                ds.type = "dispatch"
                ds.status = "defined"
                ds.numberfiles = len(file_list[dispatch_data_block]["lfns"])
                try:
                    ds.currentfiles = int(sum(filter(None, file_list[dispatch_data_block]["fsizes"])) / 1024 / 1024)
                except Exception:
                    ds.currentfiles = 0
                disp_list.append(ds)
                self.vuid_map[ds.name] = ds.vuid
            except Exception:
                error_type, error_value = sys.exc_info()[:2]
                self.logger.error(f"_setupSource() : {error_type} {error_value}")
                disp_error[dispatch_data_block] = "Setupper._setupSource() could not decode VUID dispatch_data_block"
        # insert datasets to DB
        self.taskBuffer.insertDatasets(prod_list + disp_list)
        # job status
        for job in self.jobs:
            if job.dispatchDBlock in disp_error and disp_error[job.dispatchDBlock] != "":
                if job.jobStatus != "failed":
                    job.jobStatus = "failed"
                    job.ddmErrorCode = ErrorCode.EC_Setupper
                    job.ddmErrorDiag = disp_error[job.dispatchDBlock]
                    self.logger.debug(f"failed PandaID={job.PandaID} with {job.ddmErrorDiag}")
        # delete explicitly some huge variables
        del file_list
        del prod_list
        del prod_error
        del disp_site_map

    # create dataset for outputs in the repository and assign destination
    def setup_destination(self, start_idx: int = -1, n_jobs_in_loop: int = 50) -> None:
        """
        Setup destination method for running the setup process.

        :param start_idx: The starting index for the jobs to be processed. Defaults to -1.
        :param n_jobs_in_loop: The number of jobs to be processed in a loop. Defaults to 50.
        """
        self.logger.debug(f"setupDestination idx:{start_idx} n:{n_jobs_in_loop}")
        dest_error = {}
        dataset_list = {}
        newname_list = {}
        sn_gotten_ds = []
        if start_idx == -1:
            jobs_list = self.jobs
        else:
            jobs_list = self.jobs[start_idx: start_idx + n_jobs_in_loop]
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
                        sn, fresh_flag = self.taskBuffer.getSerialNumber(file.destinationDBlock, defined_fresh_flag)
                        if sn == -1:
                            dest_error[dest] = f"Setupper._setupDestination() could not get serial num for {file.destinationDBlock}"
                            continue
                        if file.destinationDBlock not in sn_gotten_ds:
                            sn_gotten_ds.append(file.destinationDBlock)
                        # new dataset name
                        newname_list[dest] = self.make_sub_dataset_name(file.destinationDBlock, sn, job.jediTaskID)
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
                        scope_input, scope_output = select_scope(tmp_site, job.prodSourceLabel, job.job_label)
                        if name == original_name and not name.startswith("panda.um."):
                            # for original dataset
                            computing_site = file.destinationSE
                        new_vuid = None
                        if (not self.panda_ddm) and (job.prodSourceLabel != "ddm") and (job.destinationSE != "local"):
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
                                scope_dst_site_input, scope_dst_site_output = select_scope(tmp_dst_site, job.prodSourceLabel, job.job_label)
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
                                using_t1_as_t2 = False
                                if job.prodSourceLabel == "user" and computing_site not in self.site_mapper.siteSpecList:
                                    ddm_id_list = [tmp_site.ddm_output[scope_output]]
                                else:
                                    if (
                                        tmp_site.cloud != job.getCloud()
                                        and DataServiceUtils.is_sub_dataset(name)
                                        and (job.prodSourceLabel not in ["user", "panda"])
                                        and (not tmp_site.ddm_output[scope_output].endswith("PRODDISK"))
                                    ):
                                        # T1 used as T2. Use both DATADISK and PRODDISK as locations while T1 PRODDISK is phasing out
                                        ddm_id_list = [tmp_site.ddm_output[scope_output]]
                                        if scope_output in tmp_site.setokens_output and "ATLASPRODDISK" in tmp_site.setokens_output[scope_output]:
                                            ddm_id_list += [tmp_site.setokens_output[scope_output]["ATLASPRODDISK"]]
                                        using_t1_as_t2 = True
                                    else:
                                        ddm_id_list = [tmp_site.ddm_output[scope_output]]
                                # use another location when token is set
                                if not DataServiceUtils.is_sub_dataset(name) and DataServiceUtils.getDestinationSE(file.destinationDBlockToken) is not None:
                                    # destination is specified
                                    ddm_id_list = [DataServiceUtils.getDestinationSE(file.destinationDBlockToken)]
                                elif (not using_t1_as_t2) and (file.destinationDBlockToken not in ["NULL", ""]):
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
                                tmp_activity = None
                                tmp_life_time = None
                                tmp_metadata = None
                                if name != original_name and DataServiceUtils.is_sub_dataset(name):
                                    tmp_activity = "Production Output"
                                    tmp_life_time = 14
                                    tmp_metadata = {"hidden": True, "purge_replicas": 0}
                                # backend
                                ddm_back_end = job.getDdmBackEnd()
                                if ddm_back_end is None:
                                    ddm_back_end = "rucio"
                                # register dataset
                                self.logger.debug(f"registerNewDataset {name} metadata={tmp_metadata}")
                                is_ok = False
                                for attempt in range(3):
                                    try:
                                        out = rucioAPI.register_dataset(
                                            name,
                                            metadata=tmp_metadata,
                                            lifetime=tmp_life_time,
                                        )
                                        self.logger.debug(out)
                                        new_vuid = out["vuid"]
                                        is_ok = True
                                        break
                                    except Exception:
                                        error_type, error_value = sys.exc_info()[:2]
                                        self.logger.error(f"registerDataset : failed with {error_type}:{error_value}")
                                        time.sleep(10)
                                if not is_ok:
                                    tmp_msg = f"Setupper._setupDestination() could not register : {name}"
                                    dest_error[dest] = tmp_msg
                                    self.logger.error(tmp_msg)
                                    continue
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
                                    # distributed datasets for es outputs
                                    grouping = None
                                    if name != original_name and DataServiceUtils.is_sub_dataset(name) and EventServiceUtils.isEventServiceJob(job):
                                        ddm_id_list = ["type=DATADISK"]
                                        grouping = "NONE"
                                    # register location
                                    is_ok = True
                                    for ddm_id in ddm_id_list:
                                        activity = DataServiceUtils.getActivityForOut(job.prodSourceLabel)
                                        new_out = "registerDatasetLocation {name} {dq2_id} lifetime={rep_life_time} activity={activity} grouping={grouping}"
                                        self.logger.debug(
                                            tmp_str.format(
                                                name=name,
                                                dq2ID=ddm_id,
                                                repLifeTime=rep_life_time,
                                                activity=activity,
                                                grouping=grouping,
                                            )
                                        )
                                        status = False
                                        # invalid location
                                        if ddm_id is None:
                                            out = f"wrong location : {ddm_id}"
                                            self.logger.error(out)
                                            break
                                        for attempt in range(3):
                                            try:
                                                out = rucioAPI.register_dataset_location(
                                                    name,
                                                    [ddm_id],
                                                    lifetime=rep_life_time,
                                                    activity=activity,
                                                    grouping=grouping,
                                                )
                                                self.logger.debug(out)
                                                status = True
                                                break
                                            except Exception:
                                                error_type, error_value = sys.exc_info()[:2]
                                                out = f"{error_type}:{error_value}"
                                                self.logger.error(f"registerDatasetLocation : failed with {out}")
                                                time.sleep(10)
                                        # failed
                                        if not status:
                                            break
                                else:
                                    # skip registerDatasetLocations
                                    status, out = True, ""
                                if not status:
                                    dest_error[dest] = "Could not register location : %s %s" % (
                                        name,
                                        out.split("\n")[-1],
                                    )
                        # already failed
                        if dest_error[dest] != "" and name == original_name:
                            break
                        # get vuid
                        if new_vuid is None:
                            self.logger.debug("listDatasets " + name)
                            for attempt in range(3):
                                new_out, err_msg = rucioAPI.list_datasets(name)
                                if new_out is None:
                                    time.sleep(10)
                                else:
                                    break
                            if new_out is None:
                                err_msg = f"failed to get VUID for {name} with {err_msg}"
                                self.logger.error(err_msg)
                            else:
                                self.logger.debug(new_out)
                                new_vuid = new_out[name]["vuids"][0]
                        try:
                            # dataset spec
                            ds = DatasetSpec()
                            ds.vuid = new_vuid
                            ds.name = name
                            ds.type = "output"
                            ds.numberfiles = 0
                            ds.currentfiles = 0
                            ds.status = "defined"
                            # append
                            dataset_list[(name, file.destinationSE, computing_site)] = ds
                        except Exception:
                            # set status
                            error_type, error_value = sys.exc_info()[:2]
                            self.logger.error(f"_setupDestination() : {error_type} {error_value}")
                            dest_error[dest] = f"Setupper._setupDestination() could not get VUID : {name}"
                # set new destDBlock
                if dest in newname_list:
                    file.destinationDBlock = newname_list[dest]
                # update job status if failed
                if dest_error[dest] != "":
                    if job.jobStatus != "failed":
                        job.jobStatus = "failed"
                        job.ddmErrorCode = ErrorCode.EC_Setupper
                        job.ddmErrorDiag = dest_error[dest]
                        self.logger.debug(f"failed PandaID={job.PandaID} with {job.ddmErrorDiag}")
                else:
                    new_dest = (
                        file.destinationDBlock,
                        file.destinationSE,
                        job.computingSite,
                    )
                    # increment number of files
                    dataset_list[new_dest].numberfiles = dataset_list[new_dest].numberfiles + 1
        # dump
        for tmp_ds_key in dataset_list:
            if DataServiceUtils.is_sub_dataset(tmp_ds_key[0]):
                self.logger.debug(f"made sub:{tmp_ds_key[0]} for nFiles={dataset_list[tmp_ds_key].numberfiles}")
        # insert datasets to DB
        return self.taskBuffer.insertDatasets(dataset_list.values())

    #  subscribe sites to distpatchDBlocks
    def subscribe_distpatch_db(self) -> None:
        """
        Subscribe distpatch db method for running the setup process.
        """
        disp_error = {}
        failed_jobs = []
        ddm_jobs = []
        ddm_user = "NULL"
        for job in self.jobs:
            # ignore failed jobs
            if job.jobStatus in ["failed", "cancelled"] or job.isCancelled():
                continue
            # ignore no dispatch jobs
            if job.dispatchDBlock == "NULL" or job.computingSite == "NULL":
                continue
            # backend
            ddm_back_end = job.getDdmBackEnd()
            if ddm_back_end is None:
                ddm_back_end = "rucio"
            # extract dispatchDBlock and computingSite
            disp = (job.dispatchDBlock, job.computingSite)
            if disp not in disp_error:
                disp_error[disp] = ""
                # DDM IDs
                tmp_src_id = "BNL_ATLAS_1"
                if job.prodSourceLabel in ["user", "panda"]:
                    tmp_src_id = job.computingSite
                elif self.site_mapper.checkCloud(job.getCloud()):
                    # use cloud's source
                    tmp_src_id = self.site_mapper.getCloud(job.getCloud())["source"]
                src_site = self.site_mapper.getSite(tmp_src_id)
                scope_src_site_input, scope_src_site_output = select_scope(src_site, job.prodSourceLabel, job.job_label)
                src_ddm_id = src_site.ddm_output[scope_src_site_output]
                # destination
                tmp_dst_id = job.computingSite
                tmp_site_spec = self.site_mapper.getSite(job.computingSite)
                scope_tmp_site_input, scope_tmp_site_output = select_scope(tmp_site_spec, job.prodSourceLabel, job.job_label)
                if src_ddm_id != tmp_site_spec.ddm_input[scope_tmp_site_input] and src_ddm_id in tmp_site_spec.setokens_input[scope_tmp_site_input].values():
                    # direct usage of remote SE. Mainly for prestaging
                    tmp_dst_id = tmp_src_id
                    self.logger.debug(f"use remote SiteSpec of {tmp_dst_id} for {job.computingSite}")
                # use src_ddm_id as dst_ddm_id when it is associated to dest
                dst_site_spec = self.site_mapper.getSite(tmp_dst_id)
                scope_dst_input, scope_dst_output = select_scope(dst_site_spec, job.prodSourceLabel, job.job_label)
                if dst_site_spec.ddm_endpoints_input[scope_dst_input].isAssociated(src_ddm_id):
                    dst_ddm_id = src_ddm_id
                else:
                    dst_ddm_id = dst_site_spec.ddm_input[scope_dst_input]
                # check if missing at T1
                missing_at_t1 = False
                if job.prodSourceLabel in ["managed", "test"]:
                    for tmp_lfn in self.disp_file_list[job.dispatchDBlock]["lfns"]:
                        if job.getCloud() not in self.missing_files_in_t1:
                            break
                        if tmp_lfn in self.missing_files_in_t1[job.getCloud()] or tmp_lfn.split(":")[-1] in self.missing_files_in_t1[job.getCloud()]:
                            missing_at_t1 = True
                            break
                    self.logger.debug(f"{job.dispatchDBlock} missing at T1 : {missing_at_t1}")
                # use DDM
                if (not self.panda_ddm) and job.prodSourceLabel != "ddm":
                    # look for replica
                    ddm_id = src_ddm_id
                    ddm_id_list = []
                    # register replica
                    is_ok = False
                    if ddm_id != dst_ddm_id or missing_at_t1:
                        # make list
                        if job.dispatchDBlock in self.replica_map:
                            # set DQ2 ID for DISK
                            if not src_ddm_id.endswith("_DATADISK"):
                                hot_id = re.sub("_MCDISK", "_HOTDISK", src_ddm_id)
                                disk_id = re.sub("_MCDISK", "_DATADISK", src_ddm_id)
                                tape_id = re.sub("_MCDISK", "_DATATAPE", src_ddm_id)
                                mc_tape_id = re.sub("_MCDISK", "_MCTAPE", src_ddm_id)
                            else:
                                hot_id = re.sub("_DATADISK", "_HOTDISK", src_ddm_id)
                                disk_id = re.sub("_DATADISK", "_DATADISK", src_ddm_id)
                                tape_id = re.sub("_DATADISK", "_DATATAPE", src_ddm_id)
                                mc_tape_id = re.sub("_DATADISK", "_MCTAPE", src_ddm_id)
                            # DDM ID is mixed with TAIWAN-LCG2 and TW-FTT
                            if job.getCloud() in [
                                "TW",
                            ]:
                                tmp_site_spec = self.site_mapper.getSite(tmp_src_id)
                                scope_input, scope_output = select_scope(tmp_site_spec, job.prodSourceLabel, job.job_label)
                                if "ATLASDATADISK" in tmp_site_spec.setokens_input[scope_input]:
                                    disk_id = tmp_site_spec.setokens_input[scope_input]["ATLASDATADISK"]
                                if "ATLASDATATAPE" in tmp_site_spec.setokens_input[scope_input]:
                                    tape_id = tmp_site_spec.setokens_input[scope_input]["ATLASDATATAPE"]
                                if "ATLASMCTAPE" in tmp_site_spec.setokens_input[scope_input]:
                                    mc_tape_id = tmp_site_spec.setokens_input[scope_input]["ATLASMCTAPE"]
                                hot_id = "TAIWAN-LCG2_HOTDISK"
                            for tmp_dataset in self.replica_map[job.dispatchDBlock]:
                                tmp_rep_map = self.replica_map[job.dispatchDBlock][tmp_dataset]
                                if hot_id in tmp_rep_map:
                                    # HOTDISK
                                    if hot_id not in ddm_id_list:
                                        ddm_id_list.append(hot_id)
                                if src_ddm_id in tmp_rep_map:
                                    # MCDISK
                                    if src_ddm_id not in ddm_id_list:
                                        ddm_id_list.append(src_ddm_id)
                                if disk_id in tmp_rep_map:
                                    # DATADISK
                                    if disk_id not in ddm_id_list:
                                        ddm_id_list.append(disk_id)
                                if tape_id in tmp_rep_map:
                                    # DATATAPE
                                    if tape_id not in ddm_id_list:
                                        ddm_id_list.append(tape_id)
                                if mc_tape_id in tmp_rep_map:
                                    # MCTAPE
                                    if mc_tape_id not in ddm_id_list:
                                        ddm_id_list.append(mc_tape_id)
                            # consider cloudconfig.tier1se
                            tmp_cloud_ses = DataServiceUtils.getEndpointsAtT1(tmp_rep_map, self.site_mapper, job.getCloud())
                            use_cloud_ses = []
                            for tmp_cloud_se in tmp_cloud_ses:
                                if tmp_cloud_se not in ddm_id_list:
                                    use_cloud_ses.append(tmp_cloud_se)
                            if use_cloud_ses:
                                ddm_id_list += use_cloud_ses
                                self.logger.debug(f"use additional endpoints {str(use_cloud_ses)} from cloudconfig")
                        # use default location if empty
                        if not ddm_id_list:
                            ddm_id_list = [ddm_id]
                        # register dataset locations
                        if missing_at_t1:
                            # without locatios to let DDM find sources
                            is_ok = True
                        else:
                            is_ok = True
                    else:
                        # register locations later for prestaging
                        is_ok = True
                    if not is_ok:
                        disp_error[disp] = "Setupper._subscribeDistpatchDB() could not register location"
                    else:
                        is_ok = False
                        # assign destination
                        optSub = {
                            "DATASET_COMPLETE_EVENT": [f"http://{panda_config.pserverhosthttp}:{panda_config.pserverporthttp}/server/panda/datasetCompleted"]
                        }
                        opt_source = {}
                        ddm_id = dst_ddm_id
                        # prestaging
                        if src_ddm_id == dst_ddm_id and not missing_at_t1:
                            # prestage to associated endpoints
                            if job.prodSourceLabel in ["user", "panda"]:
                                tmp_site_spec = self.site_mapper.getSite(job.computingSite)
                                (
                                    scope_tmp_site_input,
                                    scope_tmp_site_output,
                                ) = select_scope(tmp_site_spec, job.prodSourceLabel, job.job_label)
                                # use DATADISK if possible
                                changed = False
                                if "ATLASDATADISK" in tmp_site_spec.setokens_input[scope_tmp_site_input]:
                                    tmp_ddm_id = tmp_site_spec.setokens_input[scope_tmp_site_input]["ATLASDATADISK"]
                                    if tmp_ddm_id in tmp_site_spec.ddm_endpoints_input[scope_tmp_site_input].getLocalEndPoints():
                                        self.logger.debug(f"use {tmp_ddm_id} instead of {ddm_id} for tape prestaging")
                                        ddm_id = tmp_ddm_id
                                        changed = True
                                # use default input endpoint
                                if not changed:
                                    ddm_id = tmp_site_spec.ddm_endpoints_input[scope_tmp_site_input].getDefaultRead()
                                    self.logger.debug(f"use default_read {ddm_id} for tape prestaging")
                            self.logger.debug(f"use {ddm_id} for tape prestaging")
                            # register dataset locations
                            is_ok = True
                        else:
                            is_ok = True
                            # set sources to handle T2s in another cloud and to transfer dis datasets being split in multiple sites
                            if not missing_at_t1:
                                for tmp_ddm_id in ddm_id_list:
                                    opt_source[tmp_ddm_id] = {"policy": 0}
                            # T1 used as T2
                            if (
                                job.getCloud() != self.site_mapper.getSite(tmp_dst_id).cloud
                                and (not dst_ddm_id.endswith("PRODDISK"))
                                and (job.prodSourceLabel not in ["user", "panda"])
                                and self.site_mapper.getSite(tmp_dst_id).cloud in ["US"]
                            ):
                                tmp_dst_site_spec = self.site_mapper.getSite(tmp_dst_id)
                                scope_input, scope_output = select_scope(tmp_dst_site_spec, job.prodSourceLabel, job.job_label)
                                se_tokens = tmp_dst_site_spec.setokens_input[scope_input]
                                # use T1_PRODDISK
                                if "ATLASPRODDISK" in se_tokens:
                                    ddm_id = se_tokens["ATLASPRODDISK"]
                            elif job.prodSourceLabel in ["user", "panda"]:
                                # use DATADISK
                                tmp_site_spec = self.site_mapper.getSite(job.computingSite)
                                (
                                    scope_tmp_site_input,
                                    scope_tmp_site_output,
                                ) = select_scope(tmp_site_spec, job.prodSourceLabel, job.job_label)
                                if "ATLASDATADISK" in tmp_site_spec.setokens_input[scope_tmp_site_input]:
                                    tmp_ddm_id = tmp_site_spec.setokens_input[scope_tmp_site_input]["ATLASDATADISK"]
                                    if tmp_ddm_id in tmp_site_spec.ddm_endpoints_input[scope_tmp_site_input].getLocalEndPoints():
                                        self.logger.debug(f"use {tmp_ddm_id} instead of {ddm_id} for analysis input staging")
                                        ddm_id = tmp_ddm_id
                        # set share and activity
                        if job.prodSourceLabel in ["user", "panda"]:
                            opt_share = "production"
                            opt_activity = "Analysis Input"
                            opt_owner = None
                        else:
                            opt_share = "production"
                            opt_owner = None
                            if job.processingType == "urgent" or job.currentPriority > 1000:
                                opt_activity = "Express"
                            else:
                                opt_activity = "Production Input"
                        # taskID
                        if job.jediTaskID not in ["NULL", 0]:
                            opt_comment = f"task_id:{job.jediTaskID}"
                        else:
                            opt_comment = None
                        if not is_ok:
                            disp_error[disp] = "Setupper._subscribeDistpatchDB() could not register location for prestage"
                        else:
                            # register subscription
                            self.logger.debug(
                                "%s %s %s"
                                % (
                                    "registerDatasetSubscription",
                                    (job.dispatchDBlock, ddm_id),
                                    {
                                        "activity": opt_activity,
                                        "lifetime": 7,
                                        "dn": opt_owner,
                                        "comment": opt_comment,
                                    },
                                )
                            )
                            for attempt in range(3):
                                try:
                                    status = rucioAPI.register_dataset_subscription(
                                        job.dispatchDBlock,
                                        [ddm_id],
                                        activity=opt_activity,
                                        lifetime=7,
                                        distinguished_name=opt_owner,
                                        comment=opt_comment,
                                    )
                                    out = "OK"
                                    break
                                except Exception as e:
                                    status = False
                                    out = f"registerDatasetSubscription failed with {str(e)} {traceback.format_exc()}"
                                    time.sleep(10)
                            if not status:
                                self.logger.error(out)
                                disp_error[disp] = "Setupper._subscribeDistpatchDB() could not register subscription"
                            else:
                                self.logger.debug(out)
            # failed jobs
            if disp_error[disp] != "":
                if job.jobStatus != "failed":
                    job.jobStatus = "failed"
                    job.ddmErrorCode = ErrorCode.EC_Setupper
                    job.ddmErrorDiag = disp_error[disp]
                    self.logger.debug(f"failed PandaID={job.PandaID} with {job.ddmErrorDiag}")
                    failed_jobs.append(job)
        # update failed jobs only. succeeded jobs should be activate by DDM callback
        self.update_failed_jobs(failed_jobs)
        # submit ddm jobs
        if ddm_jobs != []:
            ddm_ret = self.taskBuffer.storeJobs(ddm_jobs, ddm_user, joinThr=True)
            # update datasets
            ddm_index = 0
            ddm_ds_list = []
            for ddm_panda_id, ddm_job_def, ddm_job_name in ddm_ret:
                # invalid PandaID
                if ddm_panda_id in ["NULL", None]:
                    continue
                # get dispatch dataset
                ds_name = ddm_jobs[ddm_index].jobParameters.split()[-1]
                ddm_index += 1
                tmp_ds = self.taskBuffer.queryDatasetWithMap({"name": ds_name})
                if tmp_ds is not None:
                    # set MoverID
                    tmp_ds.MoverID = ddm_panda_id
                    ddm_ds_list.append(tmp_ds)
            # update
            if ddm_ds_list:
                self.taskBuffer.updateDatasets(ddm_ds_list)

    # correct LFN for attemptNr
    def correct_lfn(self) -> None:
        """
        Correct lfn method for running the setup process.
        """
        lfnMap = {}
        valMap = {}
        prodError = {}
        missingDS = {}
        jobsWaiting = []
        jobsFailed = []
        jobsProcessed = []
        allLFNs = {}
        allGUIDs = {}
        allScopes = {}
        cloudMap = {}
        lfnDsMap = {}
        replicaMap = {}
        self.logger.debug("go into LFN correction")
        # collect input LFNs
        inputLFNs = set()
        for tmpJob in self.jobs:
            for tmpFile in tmpJob.Files:
                if tmpFile.type == "input":
                    inputLFNs.add(tmpFile.lfn)
                    genLFN = re.sub("\.\d+$", "", tmpFile.lfn)
                    inputLFNs.add(genLFN)
                    if tmpFile.GUID not in ["NULL", "", None]:
                        if tmpFile.dataset not in self.lfn_dataset_map:
                            self.lfn_dataset_map[tmpFile.dataset] = {}
                        self.lfn_dataset_map[tmpFile.dataset][tmpFile.lfn] = {
                            "guid": tmpFile.GUID,
                            "chksum": tmpFile.checksum,
                            "md5sum": tmpFile.md5sum,
                            "fsize": tmpFile.fsize,
                            "scope": tmpFile.scope,
                        }
        # loop over all jobs
        for job in self.jobs:
            if self.only_ta:
                self.logger.debug(f"start TA session {job.taskID}")
            # check if sitename is known
            if job.computingSite != "NULL" and job.computingSite not in self.site_mapper.siteSpecList:
                job.jobStatus = "failed"
                job.ddmErrorCode = ErrorCode.EC_Setupper
                job.ddmErrorDiag = f"computingSite:{job.computingSite} is unknown"
                # append job for downstream process
                jobsProcessed.append(job)
                # error message for TA
                if self.only_ta:
                    self.logger.error(job.ddmErrorDiag)
                continue
            # ignore no prodDBlock jobs or container dataset
            if job.prodDBlock == "NULL":
                # append job to processed list
                jobsProcessed.append(job)
                continue
            # check if T1
            tmpSrcID = self.site_mapper.getCloud(job.getCloud())["source"]
            srcSiteSpec = self.site_mapper.getSite(tmpSrcID)
            src_scope_input, src_scope_output = select_scope(srcSiteSpec, job.prodSourceLabel, job.job_label)
            # could happen if wrong configuration or downtime
            if src_scope_output in srcSiteSpec.ddm_output:
                srcDQ2ID = srcSiteSpec.ddm_output[src_scope_output]
            else:
                errMsg = f"Source site {tmpSrcID} has no {src_scope_output} endpoint (ddm_output {srcSiteSpec.ddm_output})"
                self.logger.error(f"< jediTaskID={job.taskID} PandaID={job.PandaID} > {errMsg}")
                job.jobStatus = "failed"
                job.ddmErrorCode = ErrorCode.EC_RSE
                job.ddmErrorDiag = errMsg
                jobsFailed.append(job)
                continue
            dstSiteSpec = self.site_mapper.getSite(job.computingSite)
            dst_scope_input, dst_scope_output = select_scope(dstSiteSpec, job.prodSourceLabel, job.job_label)
            # could happen if wrong configuration or downtime
            if dst_scope_input in dstSiteSpec.ddm_input:
                dstDQ2ID = dstSiteSpec.ddm_input[dst_scope_input]
            else:
                errMsg = f"computingsite {job.computingSite} has no {dst_scope_input} endpoint (ddm_input {dstSiteSpec.ddm_input})"
                self.logger.error(f"< jediTaskID={job.taskID} PandaID={job.PandaID} > {errMsg}")
                job.jobStatus = "failed"
                job.ddmErrorCode = ErrorCode.EC_RSE
                job.ddmErrorDiag = errMsg
                jobsFailed.append(job)
                continue
            # collect datasets
            datasets = []
            for file in job.Files:
                if file.type == "input" and file.dispatchDBlock == "NULL" and (file.GUID == "NULL" or job.prodSourceLabel in ["managed", "test", "ptest"]):
                    if file.dataset not in datasets:
                        datasets.append(file.dataset)
                if srcDQ2ID == dstDQ2ID and file.type == "input" and job.prodSourceLabel in ["managed", "test", "ptest"] and file.status != "ready":
                    if job.getCloud() not in self.missing_files_in_t1:
                        self.missing_files_in_t1[job.getCloud()] = set()
                    self.missing_files_in_t1[job.getCloud()].add(file.lfn)
            # get LFN list
            for dataset in datasets:
                if dataset not in lfnMap:
                    prodError[dataset] = ""
                    lfnMap[dataset] = {}
                    # get LFNs
                    status, out = self.get_list_files_in_dataset(dataset, inputLFNs)
                    if status != 0:
                        self.logger.error(out)
                        prodError[dataset] = f"could not get file list of prodDBlock {dataset}"
                        self.logger.error(prodError[dataset])
                        # doesn't exist in DQ2
                        if status == -1:
                            missingDS[dataset] = f"DS:{dataset} not found in DDM"
                        else:
                            missingDS[dataset] = out
                    else:
                        # make map (key: LFN w/o attemptNr, value: LFN with attemptNr)
                        items = out
                        try:
                            # loop over all files
                            for tmpLFN in items:
                                vals = items[tmpLFN]
                                valMap[tmpLFN] = vals
                                genLFN = re.sub("\.\d+$", "", tmpLFN)
                                if genLFN in lfnMap[dataset]:
                                    # get attemptNr
                                    newAttNr = 0
                                    newMat = re.search("\.(\d+)$", tmpLFN)
                                    if newMat is not None:
                                        newAttNr = int(newMat.group(1))
                                    oldAttNr = 0
                                    oldMat = re.search("\.(\d+)$", lfnMap[dataset][genLFN])
                                    if oldMat is not None:
                                        oldAttNr = int(oldMat.group(1))
                                    # compare
                                    if newAttNr > oldAttNr:
                                        lfnMap[dataset][genLFN] = tmpLFN
                                else:
                                    lfnMap[dataset][genLFN] = tmpLFN
                                # mapping from LFN to DS
                                lfnDsMap[lfnMap[dataset][genLFN]] = dataset
                        except Exception:
                            prodError[dataset] = f"could not convert HTTP-res to map for prodDBlock {dataset}"
                            self.logger.error(prodError[dataset])
                            self.logger.error(out)
                    # get replica locations
                    if (self.only_ta or job.prodSourceLabel in ["managed", "test"]) and prodError[dataset] == "" and dataset not in replicaMap:
                        if dataset.endswith("/"):
                            status, out = self.get_list_dataset_replicas_in_container(dataset, True)
                        else:
                            status, out = self.get_list_dataset_replicas(dataset)
                        if not status:
                            prodError[dataset] = f"could not get locations for {dataset}"
                            self.logger.error(prodError[dataset])
                            self.logger.error(out)
                        else:
                            replicaMap[dataset] = out
                            # append except DBR
                            if not dataset.startswith("ddo"):
                                self.replica_map_for_broker[dataset] = out
            # error
            isFailed = False
            # check for failed
            for dataset in datasets:
                if dataset in missingDS:
                    job.jobStatus = "failed"
                    job.ddmErrorCode = ErrorCode.EC_GUID
                    job.ddmErrorDiag = missingDS[dataset]
                    # set missing
                    for tmpFile in job.Files:
                        if tmpFile.dataset == dataset:
                            tmpFile.status = "missing"
                    # append
                    jobsFailed.append(job)
                    isFailed = True
                    self.logger.debug(f"{job.PandaID} failed with {missingDS[dataset]}")
                    break
            if isFailed:
                continue
            # check for waiting
            for dataset in datasets:
                if prodError[dataset] != "":
                    # append job to waiting list
                    jobsWaiting.append(job)
                    isFailed = True
                    # message for TA
                    if self.only_ta:
                        self.logger.error(prodError[dataset])
                    break
            if isFailed:
                continue
            if not self.only_ta:
                # replace generic LFN with real LFN
                replaceList = []
                isFailed = False
                for file in job.Files:
                    if file.type == "input" and file.dispatchDBlock == "NULL":
                        addToLfnMap = True
                        if file.GUID == "NULL":
                            # get LFN w/o attemptNr
                            basename = re.sub("\.\d+$", "", file.lfn)
                            if basename == file.lfn:
                                # replace
                                if basename in lfnMap[file.dataset]:
                                    file.lfn = lfnMap[file.dataset][basename]
                                    replaceList.append((basename, file.lfn))
                            # set GUID
                            if file.lfn in valMap:
                                file.GUID = valMap[file.lfn]["guid"]
                                file.fsize = valMap[file.lfn]["fsize"]
                                file.md5sum = valMap[file.lfn]["md5sum"]
                                file.checksum = valMap[file.lfn]["chksum"]
                                file.scope = valMap[file.lfn]["scope"]
                                # remove white space
                                if file.md5sum is not None:
                                    file.md5sum = file.md5sum.strip()
                                if file.checksum is not None:
                                    file.checksum = file.checksum.strip()
                        else:
                            if job.prodSourceLabel not in ["managed", "test"]:
                                addToLfnMap = False
                        # check missing file
                        if file.GUID == "NULL" or job.prodSourceLabel in [
                            "managed",
                            "test",
                        ]:
                            if file.lfn not in valMap:
                                # append job to waiting list
                                errMsg = f"GUID for {file.lfn} not found in rucio"
                                self.logger.error(errMsg)
                                file.status = "missing"
                                if job not in jobsFailed:
                                    job.jobStatus = "failed"
                                    job.ddmErrorCode = ErrorCode.EC_GUID
                                    job.ddmErrorDiag = errMsg
                                    jobsFailed.append(job)
                                    isFailed = True
                                continue
                        # add to allLFNs/allGUIDs
                        if addToLfnMap:
                            allLFNs.setdefault(job.getCloud(), [])
                            allGUIDs.setdefault(job.getCloud(), [])
                            allScopes.setdefault(job.getCloud(), [])
                            allLFNs[job.getCloud()].append(file.lfn)
                            allGUIDs[job.getCloud()].append(file.GUID)
                            allScopes[job.getCloud()].append(file.scope)
                # modify jobParameters
                if not isFailed:
                    for patt, repl in replaceList:
                        job.jobParameters = re.sub(f"{patt} ", f"{repl} ", job.jobParameters)
                    # append job to processed list
                    jobsProcessed.append(job)
        # return if TA only
        if self.only_ta:
            self.logger.debug("end TA sessions")
            return
        # set data summary fields
        for tmpJob in self.jobs:
            try:
                # set only for production/analysis/test
                if tmpJob.prodSourceLabel not in ["managed", "test", "user", "prod_test"] + JobUtils.list_ptest_prod_sources:
                    continue
                # loop over all files
                tmpJob.nInputDataFiles = 0
                tmpJob.inputFileBytes = 0
                tmpInputFileProject = None
                tmpInputFileType = None
                for tmpFile in tmpJob.Files:
                    # use input files and ignore DBR/lib.tgz
                    if tmpFile.type == "input" and (not tmpFile.dataset.startswith("ddo")) and not tmpFile.lfn.endswith(".lib.tgz"):
                        tmpJob.nInputDataFiles += 1
                        if tmpFile.fsize not in ["NULL", None, 0, "0"]:
                            tmpJob.inputFileBytes += tmpFile.fsize
                        # get input type and project
                        if tmpInputFileProject is None:
                            tmpInputItems = tmpFile.dataset.split(".")
                            # input project
                            tmpInputFileProject = tmpInputItems[0].split(":")[-1]
                            # input type. ignore user/group/groupXY
                            if (
                                len(tmpInputItems) > 4
                                and (not tmpInputItems[0] in ["", "NULL", "user", "group"])
                                and (not tmpInputItems[0].startswith("group"))
                                and not tmpFile.dataset.startswith("panda.um.")
                            ):
                                tmpInputFileType = tmpInputItems[4]
                # set input type and project
                if tmpJob.prodDBlock not in ["", None, "NULL"]:
                    # input project
                    if tmpInputFileProject is not None:
                        tmpJob.inputFileProject = tmpInputFileProject
                    # input type
                    if tmpInputFileType is not None:
                        tmpJob.inputFileType = tmpInputFileType
                # protection
                maxInputFileBytes = 99999999999
                if tmpJob.inputFileBytes > maxInputFileBytes:
                    tmpJob.inputFileBytes = maxInputFileBytes
                # set background-able flag
                tmpJob.setBackgroundableFlag()
            except Exception:
                error_type, error_value = sys.exc_info()[:2]
                self.logger.error(f"failed to set data summary fields for PandaID={tmpJob.PandaID}: {error_type} {error_value}")
        # send jobs to jobsWaiting
        self.taskBuffer.keepJobs(jobsWaiting)
        # update failed job
        self.update_failed_jobs(jobsFailed)
        # remove waiting/failed jobs
        self.jobs = jobsProcessed
        # delete huge variables
        del lfnMap
        del valMap
        del prodError
        del jobsWaiting
        del jobsProcessed
        del allLFNs
        del allGUIDs
        del cloudMap

    # remove waiting jobs
    def remove_waiting_jobs(self) -> None:
        """
        Remove waiting jobs method for running the setup process.
        """
        jobsWaiting = []
        jobsProcessed = []
        for tmpJob in self.jobs:
            if tmpJob.jobStatus == "waiting":
                jobsWaiting.append(tmpJob)
            else:
                jobsProcessed.append(tmpJob)
        # send jobs to jobsWaiting
        self.taskBuffer.keepJobs(jobsWaiting)
        # remove waiting/failed jobs
        self.jobs = jobsProcessed

    # memory checker
    def memory_check(self) -> None:
        """
        Memory check method for running the setup process.
        """
        try:
            import os

            proc_status = "/proc/%d/status" % os.getpid()
            procfile = open(proc_status)
            name = ""
            vmSize = ""
            vmRSS = ""
            # extract Name,VmSize,VmRSS
            for line in procfile:
                if line.startswith("Name:"):
                    name = line.split()[-1]
                    continue
                if line.startswith("VmSize:"):
                    vmSize = ""
                    for item in line.split()[1:]:
                        vmSize += item
                    continue
                if line.startswith("VmRSS:"):
                    vmRSS = ""
                    for item in line.split()[1:]:
                        vmRSS += item
                    continue
            procfile.close()
            self.logger.debug(f"MemCheck PID={os.getpid()} Name={name} VSZ={vmSize} RSS={vmRSS}")
        except Exception:
            error_type, error_value = sys.exc_info()[:2]
            self.logger.error(f"memoryCheck() : {error_type} {error_value}")
            self.logger.debug(f"MemCheck PID={os.getpid()} unknown")
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
        # use cache data
        if use_cache and dataset in self.lfn_dataset_map:
            return 0, self.lfn_dataset_map[dataset]
        for iDDMTry in range(3):
            try:
                self.logger.debug("listFilesInDataset " + dataset)
                items, tmpDummy = rucioAPI.list_files_in_dataset(dataset, file_list=file_list)
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
        # get datasets in container
        self.logger.debug("listDatasetsInContainer " + container)
        for iDDMTry in range(3):
            datasets, out = rucioAPI.list_datasets_in_container(container)
            if datasets is None:
                time.sleep(10)
            else:
                break
        if datasets is None:
            self.logger.error(out)
            return False, out
        return True, datasets

    # get datasets in container
    def get_list_dataset_replicas_in_container(self, container: str, get_map: bool = False) -> Tuple[int, str]:
        """
        Get list dataset replicas in container method for running the setup process.

        :param container: The container to get the list of dataset replicas from.
        :param get_map: Whether to get the map. Defaults to False.
        :return: A tuple containing the status and the map of dataset replicas.
        """
        self.logger.debug("listDatasetsInContainer " + container)
        for iDDMTry in range(3):
            datasets, out = rucioAPI.list_datasets_in_container(container)
            if datasets is None:
                time.sleep(10)
            else:
                break
        if datasets is None:
            self.logger.error(out)
            if get_map:
                return False, out
            return 1, out
        # loop over all datasets
        allRepMap = {}
        for dataset in datasets:
            self.logger.debug("listDatasetReplicas " + dataset)
            status, out = self.get_list_dataset_replicas(dataset)
            self.logger.debug(out)
            if not status:
                if get_map:
                    return False, out
                return status, out
            tmpRepSites = out
            # get map
            if get_map:
                allRepMap[dataset] = tmpRepSites
                continue
            # otherwise get sum
            for siteId in tmpRepSites:
                statList = tmpRepSites[siteId]
                if siteId not in allRepMap:
                    # append
                    allRepMap[siteId] = [
                        statList[-1],
                    ]
                else:
                    # add
                    newStMap = {}
                    for stName in allRepMap[siteId][0]:
                        stNum = allRepMap[siteId][0][stName]
                        if stName in statList[-1]:
                            # try mainly for archived=None
                            try:
                                newStMap[stName] = stNum + statList[-1][stName]
                            except Exception:
                                newStMap[stName] = stNum
                        else:
                            newStMap[stName] = stNum
                    allRepMap[siteId] = [
                        newStMap,
                    ]
        # return
        self.logger.debug(str(allRepMap))
        if get_map:
            return True, allRepMap
        return 0, str(allRepMap)

    # get list of replicas for a dataset
    def get_list_dataset_replicas(self, dataset: str, get_map: bool = True) -> Tuple[bool, str]:
        """
        Get list dataset replicas method for running the setup process.

        :param dataset: The dataset to get the list of replicas from.
        :param get_map: Whether to get the map. Defaults to True.
        :return: A tuple containing a boolean indicating the status and the map of dataset replicas.
        """
        nTry = 3
        for iDDMTry in range(nTry):
            self.logger.debug(f"{iDDMTry}/{nTry} listDatasetReplicas {dataset}")
            status, out = rucioAPI.list_dataset_replicas(dataset)
            if status != 0:
                time.sleep(10)
            else:
                break
        # result
        if status != 0:
            self.logger.error(out)
            self.logger.error(f"bad response for {dataset}")
            if get_map:
                return False, {}
            else:
                return 1, str({})
        try:
            retMap = out
            self.logger.debug(f"getListDatasetReplicas->{str(retMap)}")
            if get_map:
                return True, retMap
            else:
                return 0, str(retMap)
        except Exception:
            self.logger.error(out)
            self.logger.error(f"could not convert HTTP-res to replica map for {dataset}")
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
        if not self.firstSubmission:
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

    # make dis datasets for existing files to avoid deletion when jobs are queued
    def make_dis_datasets_for_existing_files(self) -> None:
        """
        Make dis datasets for existing files method for running the setup process.
        """
        self.logger.debug("make dis datasets for existing files")
        # collect existing files
        dsFileMap = {}
        nMaxJobs = 20
        nJobsMap = {}
        for tmpJob in self.jobs:
            # use production or test jobs only
            if tmpJob.prodSourceLabel not in ["managed", "test"]:
                continue
            # skip for prefetcher or transferType=direct
            if tmpJob.usePrefetcher() or tmpJob.transferType == "direct":
                continue
            # ignore inappropriate status
            if tmpJob.jobStatus in ["failed", "cancelled", "waiting"] or tmpJob.isCancelled():
                continue
            # check cloud
            if tmpJob.getCloud() == "ND" and self.site_mapper.getSite(tmpJob.computingSite).cloud == "ND":
                continue
            # look for log _sub dataset to be used as a key
            logSubDsName = ""
            for tmpFile in tmpJob.Files:
                if tmpFile.type == "log":
                    logSubDsName = tmpFile.destinationDBlock
                    break
            # append site
            destSite = self.site_mapper.getSite(tmpJob.computingSite)
            scope_dest_input, scope_dest_output = select_scope(destSite, tmpJob.prodSourceLabel, tmpJob.job_label)
            destDQ2ID = destSite.ddm_input[scope_dest_input]
            # T1 used as T2
            if (
                tmpJob.getCloud() != self.site_mapper.getSite(tmpJob.computingSite).cloud
                and not destDQ2ID.endswith("PRODDISK")
                and self.site_mapper.getSite(tmpJob.computingSite).cloud in ["US"]
            ):
                tmpSiteSpec = self.site_mapper.getSite(tmpJob.computingSite)
                scope_tmpSite_input, scope_tmpSite_output = select_scope(tmpSiteSpec, tmpJob.prodSourceLabel, tmpJob.job_label)
                tmpSeTokens = tmpSiteSpec.setokens_input[scope_tmpSite_input]
                if "ATLASPRODDISK" in tmpSeTokens:
                    destDQ2ID = tmpSeTokens["ATLASPRODDISK"]
            # backend
            ddmBackEnd = "rucio"
            mapKeyJob = (destDQ2ID, logSubDsName)
            # increment the number of jobs per key
            if mapKeyJob not in nJobsMap:
                nJobsMap[mapKeyJob] = 0
            mapKey = (
                destDQ2ID,
                logSubDsName,
                nJobsMap[mapKeyJob] // nMaxJobs,
                ddmBackEnd,
            )
            nJobsMap[mapKeyJob] += 1
            if mapKey not in dsFileMap:
                dsFileMap[mapKey] = {}
            # add files
            for tmpFile in tmpJob.Files:
                if tmpFile.type != "input":
                    continue
                # if files are unavailable at the dest site normal dis datasets contain them
                # or files are cached
                if tmpFile.status not in ["ready"]:
                    continue
                # if available at T2
                realDestDQ2ID = (destDQ2ID,)
                if (
                    tmpJob.getCloud() in self.available_lfns_in_t2
                    and tmpFile.dataset in self.available_lfns_in_t2[tmpJob.getCloud()]
                    and tmpJob.computingSite in self.available_lfns_in_t2[tmpJob.getCloud()][tmpFile.dataset]["sites"]
                    and tmpFile.lfn in self.available_lfns_in_t2[tmpJob.getCloud()][tmpFile.dataset]["sites"][tmpJob.computingSite]
                ):
                    realDestDQ2ID = self.available_lfns_in_t2[tmpJob.getCloud()][tmpFile.dataset]["siteDQ2IDs"][tmpJob.computingSite]
                    realDestDQ2ID = tuple(realDestDQ2ID)
                # append
                if realDestDQ2ID not in dsFileMap[mapKey]:
                    dsFileMap[mapKey][realDestDQ2ID] = {
                        "taskID": tmpJob.taskID,
                        "PandaID": tmpJob.PandaID,
                        "files": {},
                    }
                if tmpFile.lfn not in dsFileMap[mapKey][realDestDQ2ID]["files"]:
                    # add scope
                    if ddmBackEnd != "rucio":
                        tmpLFN = tmpFile.lfn
                    else:
                        tmpLFN = f"{tmpFile.scope}:{tmpFile.lfn}"
                    dsFileMap[mapKey][realDestDQ2ID]["files"][tmpFile.lfn] = {
                        "lfn": tmpLFN,
                        "guid": tmpFile.GUID,
                        "fileSpecs": [],
                    }
                # add file spec
                dsFileMap[mapKey][realDestDQ2ID]["files"][tmpFile.lfn]["fileSpecs"].append(tmpFile)
        # loop over all locations
        dispList = []
        for tmpMapKey in dsFileMap:
            tmpDumVal = dsFileMap[tmpMapKey]
            for tmpLocationList in tmpDumVal:
                tmpVal = tmpDumVal[tmpLocationList]
                for tmpLocation in tmpLocationList:
                    tmpFileList = tmpVal["files"]
                    if tmpFileList == {}:
                        continue
                    nMaxFiles = 500
                    iFiles = 0
                    iLoop = 0
                    while iFiles < len(tmpFileList):
                        subFileNames = list(tmpFileList)[iFiles : iFiles + nMaxFiles]
                        if len(subFileNames) == 0:
                            break
                        # dis name
                        disDBlock = f"panda.{tmpVal['taskID']}.{time.strftime('%m.%d')}.GEN.{str(uuid.uuid4())}_dis0{iLoop}{tmpVal['PandaID']}"
                        iFiles += nMaxFiles
                        lfns = []
                        guids = []
                        fsizes = []
                        chksums = []
                        tmpZipOut = {}
                        if tmpJob.useZipToPin():
                            dids = [tmpFileList[tmpSubFileName]["lfn"] for tmpSubFileName in subFileNames]
                            tmpZipStat, tmpZipOut = rucioAPI.get_zip_files(dids, [tmpLocation])
                            if not tmpZipStat:
                                self.logger.debug(f"failed to get zip files : {tmpZipOut}")
                                tmpZipOut = {}
                        for tmpSubFileName in subFileNames:
                            tmpLFN = tmpFileList[tmpSubFileName]["lfn"]
                            if tmpLFN in tmpZipOut:
                                tmpZipFileName = f"{tmpZipOut[tmpLFN]['scope']}:{tmpZipOut[tmpLFN]['name']}"
                                if tmpZipFileName not in lfns:
                                    lfns.append(tmpZipFileName)
                                    guids.append(tmpZipOut[tmpLFN]["guid"])
                                    fsizes.append(tmpZipOut[tmpLFN]["bytes"])
                                    chksums.append(tmpZipOut[tmpLFN]["adler32"])
                            else:
                                lfns.append(tmpLFN)
                                guids.append(tmpFileList[tmpSubFileName]["guid"])
                                fsizes.append(int(tmpFileList[tmpSubFileName]["fileSpecs"][0].fsize))
                                chksums.append(tmpFileList[tmpSubFileName]["fileSpecs"][0].checksum)
                            # set dis name
                            for tmpFileSpec in tmpFileList[tmpSubFileName]["fileSpecs"]:
                                if tmpFileSpec.status in ["ready"] and tmpFileSpec.dispatchDBlock == "NULL":
                                    tmpFileSpec.dispatchDBlock = disDBlock
                        # register datasets
                        iLoop += 1
                        nDDMTry = 3
                        isOK = False
                        metadata = {"hidden": True, "purge_replicas": 0}
                        if tmpVal["taskID"] not in [None, "NULL"]:
                            metadata["task_id"] = str(tmpVal["taskID"])
                        tmpMsg = "ext registerNewDataset {ds} {lfns} {guids} {fsizes} {chksums} {meta}"
                        self.logger.debug(
                            tmpMsg.format(
                                ds=disDBlock,
                                lfns=str(lfns),
                                guids=str(guids),
                                fsizes=str(fsizes),
                                chksums=str(chksums),
                                meta=str(metadata),
                            )
                        )
                        for iDDMTry in range(nDDMTry):
                            try:
                                out = rucioAPI.register_dataset(
                                    disDBlock,
                                    lfns,
                                    guids,
                                    fsizes,
                                    chksums,
                                    lifetime=7,
                                    scope="panda",
                                    metadata=metadata,
                                )
                                self.logger.debug(out)
                                isOK = True
                                break
                            except Exception:
                                error_type, error_value = sys.exc_info()[:2]
                                self.logger.error(f"ext registerDataset : failed with {error_type}:{error_value}" + traceback.format_exc())
                                if iDDMTry + 1 == nDDMTry:
                                    break
                                self.logger.debug(f"sleep {iDDMTry}/{nDDMTry}")
                                time.sleep(10)
                        # failure
                        if not isOK:
                            continue
                        # get VUID
                        try:
                            vuid = out["vuid"]
                            # dataset spec. currentfiles is used to count the number of failed jobs
                            ds = DatasetSpec()
                            ds.vuid = vuid
                            ds.name = disDBlock
                            ds.type = "dispatch"
                            ds.status = "defined"
                            ds.numberfiles = len(lfns)
                            ds.currentfiles = 0
                            dispList.append(ds)
                        except Exception:
                            error_type, error_value = sys.exc_info()[:2]
                            self.logger.error(f"ext registerNewDataset : failed to decode VUID for {disDBlock} - {error_type} {error_value}")
                            continue
                        # freezeDataset dispatch dataset
                        self.logger.debug("freezeDataset " + disDBlock)
                        for iDDMTry in range(3):
                            status = False
                            try:
                                rucioAPI.close_dataset(disDBlock)
                                status = True
                                break
                            except Exception:
                                error_type, error_value = sys.exc_info()[:2]
                                out = f"failed to close : {error_type} {error_value}"
                                time.sleep(10)
                        if not status:
                            self.logger.error(out)
                            continue
                        # register location
                        isOK = False
                        self.logger.debug(f"ext registerDatasetLocation {disDBlock} {tmpLocation} {7}days asynchronous=True")
                        nDDMTry = 3
                        for iDDMTry in range(nDDMTry):
                            try:
                                out = rucioAPI.register_dataset_location(
                                    disDBlock,
                                    [tmpLocation],
                                    7,
                                    activity="Production Input",
                                    scope="panda",
                                    grouping="NONE",
                                )
                                self.logger.debug(out)
                                isOK = True
                                break
                            except Exception:
                                error_type, error_value = sys.exc_info()[:2]
                                self.logger.error(f"ext registerDatasetLocation : failed with {error_type}:{error_value}")
                                if iDDMTry + 1 == nDDMTry:
                                    break
                                self.logger.debug(f"sleep {iDDMTry}/{nDDMTry}")
                                time.sleep(10)

                        # failure
                        if not isOK:
                            continue
        # insert datasets to DB
        self.taskBuffer.insertDatasets(dispList)
        self.logger.debug("finished to make dis datasets for existing files")
        return

    # pin input dataset
    def pin_input_datasets(self) -> None:
        """
        Pin input datasets method for running the setup process.
        """
        self.logger.debug("pin input datasets")
        # collect input datasets and locations
        doneList = []
        allReplicaMap = {}
        useShortLivedReplicasFlag = False
        for tmpJob in self.jobs:
            # ignore HC jobs
            if tmpJob.processingType.startswith("gangarobot") or tmpJob.processingType.startswith("hammercloud"):
                continue
            # not pin if --useShortLivedReplicas was used
            if tmpJob.metadata is not None and "--useShortLivedReplicas" in tmpJob.metadata:
                if not useShortLivedReplicasFlag:
                    self.logger.debug("   skip pin due to --useShortLivedReplicas")
                    useShortLivedReplicasFlag = True
                continue
            # use production or test or user jobs only
            if tmpJob.prodSourceLabel not in ["managed", "test", "user"]:
                continue
            # ignore inappropriate status
            if tmpJob.jobStatus in ["failed", "cancelled", "waiting"] or tmpJob.isCancelled():
                continue
            # set lifetime
            if tmpJob.prodSourceLabel in ["managed", "test"]:
                pinLifeTime = 7
            else:
                pinLifeTime = 7
            # get source
            if tmpJob.prodSourceLabel in ["managed", "test"]:
                tmpSrcID = self.site_mapper.getCloud(tmpJob.getCloud())["source"]
                scope_input, scope_output = select_scope(tmpSrcID, tmpJob.prodSourceLabel, tmpJob.job_label)
                srcDQ2ID = self.site_mapper.getSite(tmpSrcID).ddm_input[scope_input]
            else:
                tmpSrcID = self.site_mapper.getSite(tmpJob.computingSite)
                scope_input, scope_output = select_scope(tmpSrcID, tmpJob.prodSourceLabel, tmpJob.job_label)
                srcDQ2ID = tmpSrcID.ddm_input[scope_input]
            # prefix of DQ2 ID
            srcDQ2IDprefix = re.sub("_[A-Z,0-9]+DISK$", "", srcDQ2ID)
            # loop over all files
            for tmpFile in tmpJob.Files:
                # use input files and ignore DBR/lib.tgz
                if (
                    tmpFile.type == "input"
                    and not tmpFile.lfn.endswith(".lib.tgz")
                    and not tmpFile.dataset.startswith("ddo")
                    and not tmpFile.dataset.startswith("user")
                    and not tmpFile.dataset.startswith("group")
                ):
                    # ignore pre-merged datasets
                    if tmpFile.dataset.startswith("panda.um."):
                        continue
                    # get replica locations
                    if tmpFile.dataset not in allReplicaMap:
                        if tmpFile.dataset.endswith("/"):
                            (
                                status,
                                tmpRepSitesMap,
                            ) = self.get_list_dataset_replicas_in_container(tmpFile.dataset, get_map=True)
                            if status == 0:
                                status = True
                            else:
                                status = False
                        else:
                            status, tmpRepSites = self.get_list_dataset_replicas(tmpFile.dataset)
                            tmpRepSitesMap = {}
                            tmpRepSitesMap[tmpFile.dataset] = tmpRepSites
                        # append
                        if status:
                            allReplicaMap[tmpFile.dataset] = tmpRepSitesMap
                        else:
                            # set empty to avoid further lookup
                            allReplicaMap[tmpFile.dataset] = {}
                        # loop over constituent datasets
                        self.logger.debug(f"pin DQ2 prefix={srcDQ2IDprefix}")
                        for tmpDsName in allReplicaMap[tmpFile.dataset]:
                            tmpRepSitesMap = allReplicaMap[tmpFile.dataset][tmpDsName]
                            # loop over locations
                            for tmpRepSite in tmpRepSitesMap:
                                if (
                                    tmpRepSite.startswith(srcDQ2IDprefix)
                                    and "TAPE" not in tmpRepSite
                                    and "_LOCALGROUP" not in tmpRepSite
                                    and "_DAQ" not in tmpRepSite
                                    and "_TZERO" not in tmpRepSite
                                    and "_USERDISK" not in tmpRepSite
                                    and "_RAW" not in tmpRepSite
                                    and "SCRATCH" not in tmpRepSite
                                ):
                                    tmpKey = (tmpDsName, tmpRepSite)
                                    # already done
                                    if tmpKey in doneList:
                                        continue
                                    # append to avoid repetition
                                    doneList.append(tmpKey)
                                    # set pin lifetime
                                    # status = self.setReplicaMetadata(tmpDsName,tmpRepSite,'pin_lifetime','%s days' % pinLifeTime)
        # retrun
        self.logger.debug("pin input datasets done")
        return

    # make T1 subscription for missing files
    def make_subscription_for_missing(self) -> None:
        """
        Make subscription for missing method for running the setup process.
        """
        self.logger.debug("make subscriptions for missing files")
        # collect datasets
        missingList = {}
        for tmpCloud in self.missing_dataset_list:
            tmpMissDatasets = self.missing_dataset_list[tmpCloud]
            # append cloud
            if tmpCloud not in missingList:
                missingList[tmpCloud] = []
            # loop over all datasets
            for tmpDsName in tmpMissDatasets:
                tmpMissFiles = tmpMissDatasets[tmpDsName]
                # check if datasets in container are used
                if tmpDsName.endswith("/"):
                    # convert container to datasets
                    tmpStat, tmpDsList = self.get_list_dataset_in_container(tmpDsName)
                    if not tmpStat:
                        self.logger.error(f"failed to get datasets in container:{tmpDsName}")
                        continue
                    # check if each dataset is actually used
                    for tmpConstDsName in tmpDsList:
                        # skip if already checked
                        if tmpDsName in missingList[tmpCloud]:
                            continue
                        # get files in each dataset
                        tmpStat, tmpFilesInDs = self.get_list_files_in_dataset(tmpConstDsName)
                        if not tmpStat:
                            self.logger.error(f"failed to get files in dataset:{tmpConstDsName}")
                            continue
                        # loop over all files to check the dataset is used
                        for tmpLFN in tmpMissFiles:
                            # append if used
                            if tmpLFN in tmpFilesInDs:
                                missingList[tmpCloud].append(tmpConstDsName)
                                break
                else:
                    # append dataset w/o checking
                    if tmpDsName not in missingList[tmpCloud]:
                        missingList[tmpCloud].append(tmpDsName)
        # make subscriptions
        for tmpCloud in missingList:
            missDsNameList = missingList[tmpCloud]
            # get distination
            tmpDstID = self.site_mapper.getCloud(tmpCloud)["source"]
            tmpDstSpec = self.site_mapper.getSite(tmpDstID)
            scope_input, scope_output = select_scope(tmpDstSpec, self.prod_source_label, self.job_label)
            dstDQ2ID = tmpDstSpec.ddm_input[scope_input]
            # register subscription
            for missDsName in missDsNameList:
                self.logger.debug(f"make subscription at {dstDQ2ID} for missing {missDsName}")
                self.make_subscription(missDsName, dstDQ2ID)
        # retrun
        self.logger.debug("make subscriptions for missing files done")
        return

    # make subscription
    def make_subscription(self, dataset: str, ddm_id: str) -> bool:
        """
        Make subscription method for running the setup process.

        :param dataset: The dataset to make the subscription for.
        :param ddm_id: The DDM ID.
        :return: A boolean indicating whether the subscription was made successfully.
        """
        # return for failure
        retFailed = False
        self.logger.debug(f"registerDatasetSubscription {dataset} {ddm_id}")
        nTry = 3
        for iDDMTry in range(nTry):
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
        # result
        if not status:
            self.logger.error(out)
            return retFailed
        # update
        self.logger.debug(f"{status} {out}")
        # return
        return True

    # setup jumbo jobs
    def setup_jumbo_jobs(self) -> None:
        """
        Setup jumbo jobs method for running the setup process.
        """
        if len(self.jumboJobs) == 0:
            return
        self.logger.debug("setup jumbo jobs")
        # get files in datasets
        dsLFNsMap = {}
        failedDS = set()
        for jumboJobSpec in self.jumboJobs:
            for tmpFileSpec in jumboJobSpec.Files:
                # only input
                if tmpFileSpec.type not in ["input"]:
                    continue
                # get files
                if tmpFileSpec.dataset not in dsLFNsMap:
                    if tmpFileSpec.dataset not in failedDS:
                        tmpStat, tmpMap = self.get_list_files_in_dataset(tmpFileSpec.dataset, use_cache=False)
                        # failed
                        if tmpStat != 0:
                            failedDS.add(tmpFileSpec.dataset)
                            self.logger.debug(f"failed to get files in {tmpFileSpec.dataset} with {tmpMap}")
                        else:
                            # append
                            dsLFNsMap[tmpFileSpec.dataset] = tmpMap
                # set failed if file lookup failed
                if tmpFileSpec.dataset in failedDS:
                    jumboJobSpec.jobStatus = "failed"
                    jumboJobSpec.ddmErrorCode = ErrorCode.EC_GUID
                    jumboJobSpec.ddmErrorDiag = f"failed to get files in {tmpFileSpec.dataset}"
                    break
        # make dis datasets
        okJobs = []
        ngJobs = []
        for jumboJobSpec in self.jumboJobs:
            # skip failed
            if jumboJobSpec.jobStatus == "failed":
                ngJobs.append(jumboJobSpec)
                continue
            # get datatype
            try:
                tmpDataType = jumboJobSpec.prodDBlock.split(".")[-2]
                if len(tmpDataType) > 20:
                    raise RuntimeError(f"data type is too log : {len(tmpDataType)} chars")
            except Exception:
                # default
                tmpDataType = "GEN"
            # files for jumbo job
            lfnsForJumbo = self.taskBuffer.getLFNsForJumbo(jumboJobSpec.jediTaskID)
            # make dis dataset name
            dispatchDBlock = f"panda.{jumboJobSpec.taskID}.{time.strftime('%m.%d.%H%M')}.{tmpDataType}.jumbo_dis{jumboJobSpec.PandaID}"
            # collect file attributes
            lfns = []
            guids = []
            sizes = []
            checksums = []
            for tmpFileSpec in jumboJobSpec.Files:
                # only input
                if tmpFileSpec.type not in ["input"]:
                    continue
                for tmpLFN in dsLFNsMap[tmpFileSpec.dataset]:
                    tmpVar = dsLFNsMap[tmpFileSpec.dataset][tmpLFN]
                    tmpLFN = f"{tmpVar['scope']}:{tmpLFN}"
                    if tmpLFN not in lfnsForJumbo:
                        continue
                    lfns.append(tmpLFN)
                    guids.append(tmpVar["guid"])
                    sizes.append(tmpVar["fsize"])
                    checksums.append(tmpVar["chksum"])
                # set dis dataset
                tmpFileSpec.dispatchDBlock = dispatchDBlock
            # register and subscribe dis dataset
            if len(lfns) != 0:
                # set dis dataset
                jumboJobSpec.dispatchDBlock = dispatchDBlock
                # register dis dataset
                try:
                    self.logger.debug(f"registering jumbo dis dataset {dispatchDBlock} with {len(lfns)} files")
                    out = rucioAPI.register_dataset(dispatchDBlock, lfns, guids, sizes, checksums, lifetime=14)
                    vuid = out["vuid"]
                    rucioAPI.close_dataset(dispatchDBlock)
                except Exception:
                    error_type, error_value = sys.exc_info()[:2]
                    self.logger.debug(f"failed to register jumbo dis dataset {dispatchDBlock} with {error_type}:{error_value}")
                    jumboJobSpec.jobStatus = "failed"
                    jumboJobSpec.ddmErrorCode = ErrorCode.EC_Setupper
                    jumboJobSpec.ddmErrorDiag = f"failed to register jumbo dispatch dataset {dispatchDBlock}"
                    ngJobs.append(jumboJobSpec)
                    continue
                # subscribe dis dataset
                try:
                    tmpSiteSpec = self.site_mapper.getSite(jumboJobSpec.computingSite)
                    scope_input, scope_output = select_scope(
                        tmpSiteSpec,
                        jumboJobSpec.prodSourceLabel,
                        jumboJobSpec.job_label,
                    )
                    endPoint = tmpSiteSpec.ddm_input[scope_input]
                    self.logger.debug(f"subscribing jumbo dis dataset {dispatchDBlock} to {endPoint}")
                    rucioAPI.register_dataset_subscription(
                        dispatchDBlock,
                        [endPoint],
                        lifetime=14,
                        activity="Production Input",
                    )
                except Exception:
                    error_type, error_value = sys.exc_info()[:2]
                    self.logger.debug(f"failed to subscribe jumbo dis dataset {dispatchDBlock} to {endPoint} with {error_type}:{error_value}")
                    jumboJobSpec.jobStatus = "failed"
                    jumboJobSpec.ddmErrorCode = ErrorCode.EC_Setupper
                    jumboJobSpec.ddmErrorDiag = f"failed to subscribe jumbo dispatch dataset {dispatchDBlock} to {endPoint}"
                    ngJobs.append(jumboJobSpec)
                    continue
                # add dataset in DB
                ds = DatasetSpec()
                ds.vuid = vuid
                ds.name = dispatchDBlock
                ds.type = "dispatch"
                ds.status = "defined"
                ds.numberfiles = len(lfns)
                ds.currentfiles = 0
                self.taskBuffer.insertDatasets([ds])
            # set destination
            jumboJobSpec.destinationSE = jumboJobSpec.computingSite
            for tmpFileSpec in jumboJobSpec.Files:
                if tmpFileSpec.type in ["output", "log"] and DataServiceUtils.getDistributedDestination(tmpFileSpec.destinationDBlockToken) is None:
                    tmpFileSpec.destinationSE = jumboJobSpec.computingSite
            okJobs.append(jumboJobSpec)
        # update failed jobs
        self.update_failed_jobs(ngJobs)
        self.jumbo_jobs = okJobs
        self.logger.debug("done for jumbo jobs")
        return

    # make sub dataset name
    def make_sub_dataset_name(self, original_name: str, sn: int, task_id: int) -> str:
        """
        Make sub dataset name method for running the setup process.

        :param original_name: The original name of the dataset.
        :param sn: The serial number.
        :param task_id: The task ID.
        :return: The sub dataset name.
        """
        try:
            task_id = int(task_id)
            if original_name.startswith("user") or original_name.startswith("panda"):
                part_name = ".".join(original_name.split(".")[:3])
            else:
                part_name = ".".join(original_name.split(".")[:2]) + ".NA." + ".".join(original_name.split(".")[3:5])
            return f"{part_name}.{task_id}_sub{sn}"
        except Exception:
            return f"{original_name}_sub{sn}"
