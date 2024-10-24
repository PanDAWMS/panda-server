"""
Plugin class for adding data to datasets in ATLAS.
Inherits from AdderPluginBase.

"""

import datetime
import gc
import re
import sys
import time
import traceback
from typing import Dict, List

from rucio.common.exception import (
    DataIdentifierNotFound,
    FileConsistencyMismatch,
    InsufficientAccountLimit,
    InvalidObject,
    InvalidPath,
    InvalidRSEExpression,
    RSENotFound,
    RSEProtocolNotSupported,
    UnsupportedOperation,
)

from pandaserver.config import panda_config
from pandaserver.dataservice import DataServiceUtils, ErrorCode
from pandaserver.dataservice.adder_plugin_base import AdderPluginBase
from pandaserver.dataservice.DataServiceUtils import select_scope
from pandaserver.dataservice.ddm import rucioAPI
from pandaserver.srvcore.MailUtils import MailUtils
from pandaserver.taskbuffer import EventServiceUtils, JobUtils


class AdderAtlasPlugin(AdderPluginBase):
    """
    Plugin class for adding data to datasets in ATLAS.
    Inherits from AdderPluginBase.
    """

    # constructor
    def __init__(self, job, **params):
        """
        Constructor for AdderAtlasPlugin.

        Args:
            job: The job object containing job details.
            **params: Additional parameters.
        """
        AdderPluginBase.__init__(self, job, params)
        self.job_id = self.job.PandaID
        self.job_status = self.job.jobStatus
        self.dataset_map = {}
        self.add_to_top_only = False
        self.go_to_transferring = False
        self.log_transferring = False
        self.subscription_map = {}
        self.go_to_merging = False

    # main
    def execute(self) -> None:
        """
        Main execution method for the plugin.
        Handles the logic for adding files to datasets based on job status and other conditions.
        """
        try:
            self.logger.debug(f"start plugin : {self.job_status}")
            # add files only to top-level datasets for transferring jobs
            if self.job.jobStatus == "transferring":
                self.add_to_top_only = True
                self.logger.debug("adder for transferring")
            # check if the job goes to merging
            if self.job.produceUnMerge():
                self.go_to_merging = True
            # check if the job should go to transferring
            src_site_spec = self.siteMapper.getSite(self.job.computingSite)
            _, scope_src_site_spec_output = select_scope(src_site_spec, self.job.prodSourceLabel, self.job.job_label)
            tmp_src_ddm = src_site_spec.ddm_output[scope_src_site_spec_output]
            if self.job.prodSourceLabel == "user" and self.job.destinationSE not in self.siteMapper.siteSpecList:
                # DQ2 ID was set by using --destSE for analysis job to transfer output
                tmp_dst_ddm = self.job.destinationSE
            else:
                dst_site_spec = self.siteMapper.getSite(self.job.destinationSE)
                _, scope_dst_site_spec_output = select_scope(dst_site_spec, self.job.prodSourceLabel, self.job.job_label)
                tmp_dst_ddm = dst_site_spec.ddm_output[scope_dst_site_spec_output]
                # protection against disappearance of dest from schedconfig
                if self.job.destinationSE not in ["NULL", None] and not self.siteMapper.checkSite(self.job.destinationSE) and self.job.destinationSE != "local":
                    self.job.ddmErrorCode = ErrorCode.EC_Adder
                    self.job.ddmErrorDiag = f"destinationSE {self.job.destinationSE} is unknown in schedconfig"
                    self.logger.error(f"{self.job.ddmErrorDiag}")
                    # set fatal error code and return
                    self.result.set_fatal()
                    return

            # protection against disappearance of src from schedconfig
            if not self.siteMapper.checkSite(self.job.computingSite):
                self.job.ddmErrorCode = ErrorCode.EC_Adder
                self.job.ddmErrorDiag = f"computingSite {self.job.computingSite} is unknown in schedconfig"
                self.logger.error(f"{self.job.ddmErrorDiag}")
                # set fatal error code and return
                self.result.set_fatal()
                return
            # check if the job has something to transfer
            self.logger.debug(f"alt stage-out:{str(self.job.altStgOutFileList())}")
            something_to_transfer = False
            for file in self.job.Files:
                if file.type in {"output", "log"}:
                    if file.status == "nooutput":
                        continue
                    if DataServiceUtils.getDistributedDestination(file.destinationDBlockToken) is None and file.lfn not in self.job.altStgOutFileList():
                        something_to_transfer = True
                        break
            self.logger.debug(f"DDM src:{tmp_src_ddm} dst:{tmp_dst_ddm}")
            job_type = JobUtils.translate_prodsourcelabel_to_jobtype(src_site_spec.type, self.job.prodSourceLabel)
            if re.search("^ANALY_", self.job.computingSite) is not None:
                # analysis site. Should be obsoleted by the next check
                pass
            elif job_type == JobUtils.ANALY_PS:
                # analysis job
                pass
            elif self.job.computingSite == self.job.destinationSE:
                # same site ID for computingSite and destinationSE
                pass
            elif tmp_src_ddm == tmp_dst_ddm:
                # same DDMID for src/dest
                pass
            elif self.job.destinationSE in ["NULL", None]:
                # jobs stayed without destinationSE, e.g. there is no job output
                pass
            elif self.add_to_top_only:
                # already in transferring
                pass
            elif self.go_to_merging:
                # no transferring for merging
                pass
            elif self.job.jobStatus == "failed":
                # failed jobs
                if self.job.prodSourceLabel in ["managed", "test"]:
                    self.log_transferring = True
            elif (
                self.job.jobStatus == "finished"
                and (EventServiceUtils.isEventServiceJob(self.job) or EventServiceUtils.isJumboJob(self.job))
                and not EventServiceUtils.isJobCloningJob(self.job)
            ):
                # transfer only log file for normal ES jobs
                self.log_transferring = True
            elif not something_to_transfer:
                # nothing to transfer
                pass
            else:
                self.go_to_transferring = True
            self.logger.debug(f"somethingToTransfer={something_to_transfer}")
            self.logger.debug(f"goToTransferring={self.go_to_transferring}")
            self.logger.debug(f"logTransferring={self.log_transferring}")
            self.logger.debug(f"goToMerging={self.go_to_merging}")
            self.logger.debug(f"addToTopOnly={self.add_to_top_only}")
            return_output = self.update_outputs()
            self.logger.debug(f"added outputs with {return_output}")
            if return_output != 0:
                self.logger.debug("terminated when adding")
                return
            # succeeded
            self.result.set_succeeded()
            self.logger.debug("end plugin")
        except Exception:
            error_type, error_value = sys.exc_info()[:2]
            err_str = f"execute() : {error_type} {error_value}"
            err_str += traceback.format_exc()
            self.logger.debug(err_str)
            # set fatal error code
            self.result.set_fatal()
        # return
        return

    # update output files
    def update_outputs(self):
        """
        Update output files for the job.
        Handles the logic for adding files to datasets and registering them with Rucio.

        Returns:
            int: 0 if successful, 1 if there was an error.
        """
        # return if non-DQ2
        if self.job.destinationSE == "local":
            return 0

        # Get the computingsite spec and scope
        src_site_spec = self.siteMapper.getSite(self.job.computingSite)
        _, scope_src_site_spec_output = select_scope(src_site_spec, self.job.prodSourceLabel, self.job.job_label)

        # Get the zip file map
        zip_file_map = self.job.getZipFileMap()

        # Get campaign and nEvents input
        campaign = None
        n_events_input = {}

        if self.job.jediTaskID not in [0, None, "NULL"]:
            tmp_ret = self.taskBuffer.getTaskAttributesPanda(self.job.jediTaskID, ["campaign"])
            campaign = tmp_ret.get("campaign")
            for file_spec in self.job.Files:
                if file_spec.type == "input":
                    tmp_dict = self.taskBuffer.getJediFileAttributes(
                        file_spec.PandaID, file_spec.jediTaskID, file_spec.datasetID, file_spec.fileID, ["nEvents"]
                    )
                    if "nEvents" in tmp_dict:
                        n_events_input[file_spec.lfn] = tmp_dict["nEvents"]

        # Add nEvents info for zip files
        for tmp_zip_file_name, tmp_zip_contents in zip_file_map.items():
            if tmp_zip_file_name not in self.extra_info["nevents"]:
                for tmp_zip_content in tmp_zip_contents:
                    for tmp_lfn in list(n_events_input):
                        # checking if a logical file name (LFN) matches a pattern corresponding to a zip file content and if the LFN exists in the n_events_input dictionary with a non-None value.
                        # If these conditions are met, the code updates the extra_info["nevents"] dictionary by setting a default value of 0 for the zip file name.
                        if re.search("^" + tmp_zip_content + "$", tmp_lfn) is not None and tmp_lfn in n_events_input and n_events_input[tmp_lfn] is not None:
                            self.extra_info["nevents"].setdefault(tmp_zip_file_name, 0)
                            self.extra_info["nevents"][tmp_zip_file_name] += n_events_input[tmp_lfn]

        # check files
        id_map = {}
        # fileList = []
        sub_map = {}
        dataset_destination_map = {}
        dist_datasets = set()
        map_for_alt_stage_out = {}
        alt_staged_files = set()
        zip_files = {}
        cont_zip_map = {}
        sub_to_ds_map = {}
        ds_id_to_ds_map = self.taskBuffer.getOutputDatasetsJEDI(self.job.PandaID)
        self.logger.debug(f"dsInJEDI={str(ds_id_to_ds_map)}")

        for file in self.job.Files:
            # gc
            gc.collect()
            if file.type not in {"output", "log"}:
                continue

            # prepare the site spec and scope for the destinationSE site
            destination_se_site_spec = self.siteMapper.getSite(file.destinationSE)
            _, scope_dst_se_site_spec_output = select_scope(destination_se_site_spec, self.job.prodSourceLabel, self.job.job_label)

            # added to map
            sub_to_ds_map[file.destinationDBlock] = ds_id_to_ds_map.get(file.datasetID, file.dataset)

            # add only log file for failed jobs
            if self.job_status == "failed" and file.type != "log":
                continue
            # add only log file for successful ES jobs
            if (
                self.job.jobStatus == "finished"
                and EventServiceUtils.isEventServiceJob(self.job)
                and not EventServiceUtils.isJobCloningJob(self.job)
                and file.type != "log"
            ):
                continue
            # add only log for jumbo jobs
            if EventServiceUtils.isJumboJob(self.job) and file.type != "log":
                continue
            # skip no output or failed
            if file.status in ["nooutput", "failed"]:
                continue

            # check if zip file
            if file.lfn in zip_file_map:
                is_zip_file = True
                if file.lfn not in zip_files and not self.add_to_top_only:
                    zip_files[file.lfn] = {}
            else:
                is_zip_file = False

            # check if zip content
            zip_file_name = None
            if not is_zip_file and not self.add_to_top_only:
                for tmp_zip_file_name in zip_file_map:
                    tmp_zip_contents = zip_file_map[tmp_zip_file_name]
                    for tmp_zip_content in tmp_zip_contents:
                        if re.search("^" + tmp_zip_content + "$", file.lfn) is not None:
                            zip_file_name = tmp_zip_file_name
                            break
                    if zip_file_name is not None:
                        break
                if zip_file_name is not None:
                    if zip_file_name not in zip_files:
                        zip_files[zip_file_name] = {}
                    cont_zip_map[file.lfn] = zip_file_name

            try:
                # check nevents
                if file.type == "output" and not is_zip_file and not self.add_to_top_only and self.job.prodSourceLabel == "managed":
                    if file.lfn not in self.extra_info["nevents"]:
                        to_skip = False
                        # exclude some formats
                        for patt in ["TXT", "NTUP", "HIST"]:
                            if file.lfn.startswith(patt):
                                to_skip = True
                        if not to_skip:
                            err_msg = f"nevents is missing in jobReport for {file.lfn}"
                            self.logger.warning(err_msg)
                            self.job.ddmErrorCode = ErrorCode.EC_MissingNumEvents
                            raise ValueError(err_msg)
                    if file.lfn not in self.extra_info["guid"] or file.GUID != self.extra_info["guid"][file.lfn]:
                        self.logger.debug(f"extra_info = {str(self.extra_info)}")
                        err_msg = f"GUID is inconsistent between jobReport and pilot report for {file.lfn}"
                        self.logger.warning(err_msg)
                        self.job.ddmErrorCode = ErrorCode.EC_InconsistentGUID
                        raise ValueError(err_msg)

                # fsize
                fsize = None
                if file.fsize not in ["NULL", "", 0]:
                    try:
                        fsize = int(file.fsize)
                    except Exception:
                        error_type, error_value, _ = sys.exc_info()
                        self.logger.error(f"{self.job_id} : {error_type} {error_value}")
                # use top-level dataset name for alternative stage-out
                if file.lfn not in self.job.altStgOutFileList():
                    file_destination_dispatch_block = file.destinationDBlock
                else:
                    file_destination_dispatch_block = file.dataset
                # append to map
                if file_destination_dispatch_block not in id_map:
                    id_map[file_destination_dispatch_block] = []
                file_attrs = {
                    "guid": file.GUID,
                    "lfn": file.lfn,
                    "size": fsize,
                    "checksum": file.checksum,
                    "ds": file_destination_dispatch_block,
                }
                # add SURLs if LFC registration is required
                if not self.add_to_top_only:
                    file_attrs["surl"] = self.extra_info["surl"][file.lfn]
                    if file_attrs["surl"] is None:
                        del file_attrs["surl"]

                    # get destination
                    if file_destination_dispatch_block not in dataset_destination_map:
                        if file.lfn in self.job.altStgOutFileList():
                            # alternative stage-out
                            tmp_dest_list = (
                                [DataServiceUtils.getDestinationSE(file.destinationDBlockToken)]
                                if DataServiceUtils.getDestinationSE(file.destinationDBlockToken)
                                else [
                                    destination_se_site_spec.setokens_output[scope_dst_se_site_spec_output].get(
                                        file.destinationDBlockToken, destination_se_site_spec.ddm_output[scope_dst_se_site_spec_output]
                                    )
                                ]
                            )
                        elif file.destinationDBlockToken in ["", None, "NULL"]:
                            # use default endpoint
                            tmp_dest_list = [src_site_spec.ddm_output[scope_src_site_spec_output]]
                        elif (
                            DataServiceUtils.getDestinationSE(file.destinationDBlockToken)
                            and src_site_spec.ddm_output[scope_src_site_spec_output] == destination_se_site_spec.ddm_output[scope_dst_se_site_spec_output]
                        ):
                            tmp_dest_list = [DataServiceUtils.getDestinationSE(file.destinationDBlockToken)]
                            # RSE is specified
                        elif DataServiceUtils.getDistributedDestination(file.destinationDBlockToken):
                            tmp_dest_list = [DataServiceUtils.getDistributedDestination(file.destinationDBlockToken)]
                            dist_datasets.add(file_destination_dispatch_block)
                            # RSE is specified for distributed datasets
                        elif src_site_spec.cloud != self.job.cloud and (self.job.prodSourceLabel not in ["user", "panda"]):
                            # T1 used as T2
                            tmp_dest_list = [src_site_spec.ddm_output[scope_src_site_spec_output]]
                        else:
                            tmp_dest_list = []
                            tmp_se_tokens = src_site_spec.setokens_output[scope_src_site_spec_output]
                            for tmp_dest_token in file.destinationDBlockToken.split(","):
                                if tmp_dest_token in tmp_se_tokens:
                                    tmp_dest = tmp_se_tokens[tmp_dest_token]
                                else:
                                    tmp_dest = src_site_spec.ddm_output[scope_src_site_spec_output]
                                if tmp_dest not in tmp_dest_list:
                                    tmp_dest_list.append(tmp_dest)
                        # add
                        dataset_destination_map[file_destination_dispatch_block] = tmp_dest_list

                # extra meta data
                if file.lfn in self.extra_info["lbnr"]:
                    file_attrs["lumiblocknr"] = self.extra_info["lbnr"][file.lfn]
                if file.lfn in self.extra_info["nevents"]:
                    file_attrs["events"] = self.extra_info["nevents"][file.lfn]
                elif self.extra_info["nevents"] != {}:
                    file_attrs["events"] = None
                # if file.jediTaskID not in [0,None,'NULL']:
                #    fileAttrs['task_id'] = file.jediTaskID
                file_attrs["panda_id"] = file.PandaID
                if campaign not in ["", None]:
                    file_attrs["campaign"] = campaign

                # For files uploaded to alternative RSEs
                has_normal_url = True
                if file.lfn in self.extra_info["endpoint"] and self.extra_info["endpoint"][file.lfn] != []:
                    for pilot_end_point in self.extra_info["endpoint"][file.lfn]:
                        # pilot uploaded to endpoint consistently with original job definition
                        if pilot_end_point in dataset_destination_map[file_destination_dispatch_block]:
                            has_normal_url = True
                        else:
                            # alternative stage-out
                            alt_staged_files.add(file.lfn)
                            map_for_alt_stage_out.setdefault(pilot_end_point, {})
                            if file_destination_dispatch_block in dist_datasets:
                                # add files to top-level distributed datasets without triggering aggregation
                                tmp_destination_dispatch_block = sub_to_ds_map[file_destination_dispatch_block]
                            else:
                                # add files to dispatch blocks for aggregation to the destination
                                tmp_destination_dispatch_block = file_destination_dispatch_block
                            map_for_alt_stage_out[pilot_end_point].setdefault(tmp_destination_dispatch_block, [])
                            map_for_alt_stage_out[pilot_end_point][tmp_destination_dispatch_block].append(file_attrs)

                if has_normal_url:
                    # add file to be added to dataset
                    id_map[file_destination_dispatch_block].append(file_attrs)
                    # add file to be added to zip
                    if not is_zip_file and zip_file_name:
                        if "files" not in zip_files[zip_file_name]:
                            zip_files[zip_file_name]["files"] = []
                        zip_files[zip_file_name]["files"].append(file_attrs)
                    if is_zip_file and not self.add_to_top_only:
                        # copy file attribute for zip file registration
                        for tmp_file_attr_name, tmp_file_attr_val in file_attrs.items():
                            zip_files[file.lfn][tmp_file_attr_name] = tmp_file_attr_val
                        zip_files[file.lfn]["scope"] = file.scope
                        zip_files[file.lfn]["rse"] = dataset_destination_map[file_destination_dispatch_block]

                # for subscription
                if re.search("_sub\d+$", file_destination_dispatch_block) and (not self.add_to_top_only) and self.job.destinationSE != "local":
                    if not self.siteMapper:
                        self.logger.error("SiteMapper is None")
                    else:
                        # get dataset spec
                        if file_destination_dispatch_block not in self.dataset_map:
                            tmp_dataset = self.taskBuffer.queryDatasetWithMap({"name": file_destination_dispatch_block})
                            self.dataset_map[file_destination_dispatch_block] = tmp_dataset
                        # check if valid dataset
                        if self.dataset_map[file_destination_dispatch_block] is None:
                            self.logger.error(f": cannot find {file_destination_dispatch_block} in DB")
                        else:
                            if self.dataset_map[file_destination_dispatch_block].status not in ["defined"]:
                                # not a fresh dataset
                                self.logger.debug(
                                    f": subscription was already made for {self.dataset_map[file_destination_dispatch_block].status}:{file_destination_dispatch_block}"
                                )
                            else:
                                # get DDM IDs
                                tmp_src_ddm = src_site_spec.ddm_output[scope_src_site_spec_output]
                                if self.job.prodSourceLabel == "user" and file.destinationSE not in self.siteMapper.siteSpecList:
                                    # DDM ID was set by using --destSE for analysis job to transfer output
                                    tmp_dst_ddm = file.destinationSE
                                else:
                                    if DataServiceUtils.getDestinationSE(file.destinationDBlockToken):
                                        tmp_dst_ddm = DataServiceUtils.getDestinationSE(file.destinationDBlockToken)
                                    else:
                                        tmp_dst_ddm = destination_se_site_spec.ddm_output[scope_dst_se_site_spec_output]
                                # if src != dest or multi-token
                                if (tmp_src_ddm != tmp_dst_ddm) or (tmp_src_ddm == tmp_dst_ddm and file.destinationDBlockToken.count(",") != 0):
                                    opt_sub = {
                                        "DATASET_COMPLETE_EVENT": [
                                            f"http://{panda_config.pserverhosthttp}:{panda_config.pserverporthttp}/server/panda/datasetCompleted"
                                        ]
                                    }
                                    # append
                                    if file_destination_dispatch_block not in sub_map:
                                        sub_map[file_destination_dispatch_block] = []
                                        # sources
                                        opt_source = {}
                                        # set sources
                                        if file.destinationDBlockToken in [
                                            "NULL",
                                            "",
                                        ]:
                                            # use default DDM ID as source
                                            opt_source[tmp_src_ddm] = {"policy": 0}
                                        else:
                                            # convert token to DDM ID
                                            ddm_id = tmp_src_ddm
                                            # use the first token's location as source for T1D1
                                            tmp_src_token = file.destinationDBlockToken.split(",")[0]
                                            if tmp_src_token in src_site_spec.setokens_output[scope_src_site_spec_output]:
                                                ddm_id = src_site_spec.setokens_output[scope_src_site_spec_output][tmp_src_token]
                                            opt_source[ddm_id] = {"policy": 0}
                                        # T1 used as T2
                                        if src_site_spec.cloud != self.job.cloud and (self.job.prodSourceLabel not in ["user", "panda"]):
                                            if tmp_src_ddm not in opt_source:
                                                opt_source[tmp_src_ddm] = {"policy": 0}
                                        # use another location when token is set
                                        if file.destinationDBlockToken not in [
                                            "NULL",
                                            "",
                                        ]:
                                            tmp_ddm_id_list = []
                                            tmp_dst_tokens = file.destinationDBlockToken.split(",")
                                            # remove the first one because it is already used as a location
                                            if tmp_src_ddm == tmp_dst_ddm:
                                                tmp_dst_tokens = tmp_dst_tokens[1:]
                                            # loop over all tokens
                                            for idx_token, tmp_dst_token in enumerate(tmp_dst_tokens):
                                                ddm_id = tmp_dst_ddm
                                                if tmp_dst_token in destination_se_site_spec.setokens_output[scope_dst_se_site_spec_output]:
                                                    ddm_id = destination_se_site_spec.setokens_output[scope_dst_se_site_spec_output][tmp_dst_token]
                                                # keep the fist destination for multi-hop
                                                if idx_token == 0:
                                                    first_destination_ddm = ddm_id
                                                else:
                                                    # use the fist destination as source for T1D1
                                                    opt_source = {}
                                                    opt_source[first_destination_ddm] = {"policy": 0}
                                                # remove looping subscription
                                                if ddm_id == tmp_src_ddm:
                                                    continue
                                                # avoid duplication
                                                if ddm_id not in tmp_ddm_id_list:
                                                    sub_map[file_destination_dispatch_block].append((ddm_id, opt_sub, opt_source))
                                        else:
                                            # use default DDM
                                            for ddm_id in tmp_dst_ddm.split(","):
                                                sub_map[file_destination_dispatch_block].append((ddm_id, opt_sub, opt_source))
            except Exception as e:
                err_str = str(e)
                self.logger.error(err_str + " " + traceback.format_exc())
                self.result.set_fatal()
                self.job.ddmErrorDiag = "failed before adding files : " + err_str
                return 1

        # release some memory
        del ds_id_to_ds_map
        gc.collect()

        # zipping input files
        if len(zip_file_map) > 0 and not self.add_to_top_only:
            for file_spec in self.job.Files:
                if file_spec.type != "input":
                    continue

                zip_file_name = None
                for tmp_zip_file_name in zip_file_map:
                    tmp_zip_contents = zip_file_map[tmp_zip_file_name]
                    for tmp_zip_content in tmp_zip_contents:
                        if re.search("^" + tmp_zip_content + "$", file_spec.lfn) is not None:
                            zip_file_name = tmp_zip_file_name
                            break
                    if zip_file_name:
                        break
                if zip_file_name:
                    if zip_file_name not in zip_files:
                        continue
                    if "files" not in zip_files[zip_file_name]:
                        zip_files[zip_file_name]["files"] = []
                    file_attrs = {
                        "guid": file_spec.GUID,
                        "lfn": file_spec.lfn,
                        "size": file_spec.fsize,
                        "checksum": file_spec.checksum,
                        "ds": file_spec.dataset,
                    }
                    zip_files[zip_file_name]["files"].append(file_attrs)

        # cleanup submap
        sub_map = {k: v for k, v in sub_map.items() if v}

        # add data to original dataset
        for destination_dispatch_block in list(id_map):
            orig_dispatch_block = None
            match = re.search("^(.+)_sub\d+$", destination_dispatch_block)
            if match:
                # add files to top-level datasets
                orig_dispatch_block = sub_to_ds_map[destination_dispatch_block]
                if (not self.go_to_transferring) or (not self.add_to_top_only and destination_dispatch_block in dist_datasets):
                    id_map[orig_dispatch_block] = id_map[destination_dispatch_block]
            # add files to top-level datasets only
            if self.add_to_top_only or self.go_to_merging or (destination_dispatch_block in dist_datasets and orig_dispatch_block):
                del id_map[destination_dispatch_block]
            # skip sub unless getting transferred
            if orig_dispatch_block:
                if not self.go_to_transferring and not self.log_transferring and destination_dispatch_block in id_map:
                    del id_map[destination_dispatch_block]

        # print idMap
        self.logger.debug(f"dataset to files map = {id_map}")
        self.logger.debug(f"transfer rules = {sub_map}")
        self.logger.debug(f"dataset to destination map = {dataset_destination_map}")
        self.logger.debug(f"extra info = {str(self.extra_info)}")
        self.logger.debug(f"""alternatively staged files = {",".join(list(alt_staged_files)) if alt_staged_files else "NA"}""")

        # check consistency of destinationDBlock
        has_sub = False
        for destination_dispatch_block in id_map:
            match = re.search("^(.+)_sub\d+$", destination_dispatch_block)
            if match is not None:
                has_sub = True
                break
        if id_map and self.go_to_transferring and not has_sub:
            err_str = "no sub datasets for transferring. destinationDBlock may be wrong"
            self.logger.error(err_str)
            self.result.set_fatal()
            self.job.ddmErrorDiag = "failed before adding files : " + err_str
            return 1

        # add data
        self.logger.debug("addFiles start")
        # count the number of files
        reg_file_list = {tmp_reg_item["lfn"] for tmp_reg_list in id_map.values() for tmp_reg_item in tmp_reg_list}
        reg_num_files = len(reg_file_list)

        # decompose idMap
        if self.add_to_top_only:
            dest_id_map = {None: id_map}
        else:
            dest_id_map = self.decompose_id_map(id_map, dataset_destination_map, map_for_alt_stage_out, sub_to_ds_map, alt_staged_files)

        # add files
        result = self.register_files(reg_num_files, zip_files, dest_id_map, cont_zip_map)
        if result == 1:
            return 1

        # release some memory
        del dest_id_map
        del dataset_destination_map
        del map_for_alt_stage_out
        del zip_files
        del cont_zip_map
        gc.collect()

        if self.job.processingType == "urgent" or self.job.currentPriority > 1000:
            sub_activity = "Express"
        else:
            sub_activity = "Production Output"

        # register dataset subscription
        self.process_subscriptions(sub_map, sub_to_ds_map, dist_datasets, sub_activity)

        # collect list of merging files
        if self.go_to_merging and self.job_status not in ["failed", "cancelled", "closed"]:
            for tmp_file_list in id_map.values():
                for tmp_file in tmp_file_list:
                    if tmp_file["lfn"] not in self.result.merging_files:
                        self.result.merging_files.append(tmp_file["lfn"])

        # register ES files
        if (EventServiceUtils.isEventServiceJob(self.job) or EventServiceUtils.isJumboJob(self.job)) and not EventServiceUtils.isJobCloningJob(self.job):
            if self.job.registerEsFiles():
                try:
                    self.register_event_service_files()
                except Exception:
                    err_type, err_value = sys.exc_info()[:2]
                    self.logger.error(f"failed to register ES files with {err_type}:{err_value}")
                    self.result.set_temporary()
                    return 1

        # properly finished
        self.logger.debug("addFiles end")
        return 0

    def register_files(self, reg_num_files: int, zip_files: list, dest_id_map: dict, cont_zip_map: dict) -> int | None:
        """
        Register files with Rucio.

        :param reg_num_files: Number of files to register.
        :param zip_files: List of zip files to register.
        :param dest_id_map: Destination ID map.
        :param cont_zip_map: Container zip map.
        :return: 1 if registration fails, None otherwise.
        """
        max_attempt = 3
        for attempt_number in range(max_attempt):
            is_fatal = False
            is_failed = False
            reg_start = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
            try:
                if self.add_to_top_only:
                    reg_msg_str = f"File registration for {reg_num_files} files "
                else:
                    reg_msg_str = f"File registration with rucio for {reg_num_files} files "
                if len(zip_files) > 0:
                    self.logger.debug(f"registerZipFiles {str(zip_files)}")
                    rucioAPI.register_zip_files(zip_files)
                self.logger.debug(f"registerFilesInDatasets {str(dest_id_map)} zip={str(cont_zip_map)}")
                out = rucioAPI.register_files_in_dataset(dest_id_map, cont_zip_map)
            except (
                DataIdentifierNotFound,
                FileConsistencyMismatch,
                UnsupportedOperation,
                InvalidPath,
                InvalidObject,
                RSENotFound,
                RSEProtocolNotSupported,
                InvalidRSEExpression,
                KeyError,
            ):
                # fatal errors
                err_type, err_value = sys.exc_info()[:2]
                out = f"{err_type} : {err_value}"
                out += traceback.format_exc()
                is_fatal = True
                is_failed = True
            except Exception:
                # unknown errors
                err_type, err_value = sys.exc_info()[:2]
                out = f"{err_type} : {err_value}"
                out += traceback.format_exc()
                is_fatal = (
                    "value too large for column" in out
                    or "unique constraint (ATLAS_RUCIO.DIDS_GUID_IDX) violate" in out
                    or "unique constraint (ATLAS_RUCIO.DIDS_PK) violated" in out
                    or "unique constraint (ATLAS_RUCIO.ARCH_CONTENTS_PK) violated" in out
                )
                is_failed = True
            reg_time = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - reg_start
            self.logger.debug(f"{reg_msg_str} took {reg_time.seconds}.{reg_time.microseconds // 1000:03d} sec")
            # failed
            if is_failed or is_fatal:
                self.logger.error(f"{out}")
                if (attempt_number + 1) == max_attempt or is_fatal:
                    self.job.ddmErrorCode = ErrorCode.EC_Adder
                    # extract important error string
                    extracted_err_str = DataServiceUtils.extractImportantError(out)
                    err_msg = "Could not add files to DDM: "
                    if extracted_err_str == "":
                        self.job.ddmErrorDiag = err_msg + out.split("\n")[-1]
                    else:
                        self.job.ddmErrorDiag = err_msg + extracted_err_str
                    if is_fatal:
                        self.result.set_fatal()
                    else:
                        self.result.set_temporary()
                    return 1
                self.logger.error(f"Try:{attempt_number}")
                # sleep
                time.sleep(10)
            else:
                self.logger.debug(f"{str(out)}")
                break

    def process_subscriptions(self, sub_map: Dict[str, str], sub_to_ds_map: Dict[str, List[str]], dist_datasets: List[str], sub_activity: str):
        """
        Process the subscriptions for the job.

        This method handles the processing of subscriptions, including keeping subscriptions,
        collecting transferring jobs, and sending requests to DaTRI.

        :param sub_map: A dictionary mapping subscription names to their values.
        :param sub_to_ds_map: A dictionary mapping subscriptions to datasets.
        :param dist_datasets: A list of distributed datasets.
        :param sub_activity: The subscription activity type.
        """
        if self.job.prodSourceLabel not in ["user"]:
            for tmp_name, tmp_val in sub_map.items():
                for ddm_id, opt_sub, opt_source in tmp_val:
                    if not self.go_to_merging:
                        # make subscription for prod jobs
                        rep_life_time = 14
                        self.logger.debug(
                            f"registerDatasetSubscription {tmp_name} {ddm_id} {{'activity': {sub_activity}, 'replica_lifetime': {rep_life_time}}}"
                        )
                        for _ in range(3):
                            is_failed = False
                            try:
                                status = rucioAPI.register_dataset_subscription(
                                    tmp_name,
                                    [ddm_id],
                                    owner="panda",
                                    activity=sub_activity,
                                    lifetime=rep_life_time,
                                )
                                out = "OK"
                                break
                            except InvalidRSEExpression:
                                status = False
                                err_type, err_value = sys.exc_info()[:2]
                                out = f"{err_type} {err_value}"
                                is_failed = True
                                self.job.ddmErrorCode = ErrorCode.EC_Subscription
                                break
                            except Exception:
                                status = False
                                err_type, err_value = sys.exc_info()[:2]
                                out = f"{err_type} {err_value}"
                                is_failed = True
                                # retry for temporary errors
                                time.sleep(10)
                        if is_failed:
                            self.logger.error(f"{out}")
                            if self.job.ddmErrorCode == ErrorCode.EC_Subscription:
                                # fatal error
                                self.job.ddmErrorDiag = f"subscription failure with {out}"
                                self.result.set_fatal()
                            else:
                                # temporary errors
                                self.job.ddmErrorCode = ErrorCode.EC_Adder
                                self.job.ddmErrorDiag = f"could not register subscription : {tmp_name}"
                                self.result.set_temporary()
                            return 1
                        self.logger.debug(f"{str(out)}")
                    else:
                        # register location
                        tmp_dataset_name_location = sub_to_ds_map[tmp_name]
                        rep_life_time = 14
                        for tmp_location_name in opt_source:
                            self.logger.debug(f"registerDatasetLocation {tmp_dataset_name_location} {tmp_location_name} {{'lifetime': '14 days'}}")

                            for _ in range(3):
                                is_failed = False
                                try:
                                    rucioAPI.register_dataset_location(
                                        tmp_dataset_name_location,
                                        [tmp_location_name],
                                        owner="panda",
                                        activity=sub_activity,
                                        lifetime=rep_life_time,
                                    )
                                    out = "OK"
                                    break
                                except Exception:
                                    status = False
                                    err_type, err_value = sys.exc_info()[:2]
                                    out = f"{err_type} {err_value}"
                                    is_failed = True
                                    # retry for temporary errors
                                    time.sleep(10)
                            if is_failed:
                                self.logger.error(f"{out}")
                                if self.job.ddmErrorCode == ErrorCode.EC_Location:
                                    # fatal error
                                    self.job.ddmErrorDiag = f"location registration failure with {out}"
                                    self.result.set_fatal()
                                else:
                                    # temporary errors
                                    self.job.ddmErrorCode = ErrorCode.EC_Adder
                                    self.job.ddmErrorDiag = f"could not register location : {tmp_dataset_name_location}"
                                    self.result.set_temporary()
                                return 1
                            self.logger.debug(f"{str(out)}")

                    # set dataset status
                    self.dataset_map[tmp_name].status = "running"

            # keep subscriptions
            self.subscription_map = sub_map

            # collect list of transferring jobs
            for tmp_file in self.job.Files:
                if tmp_file.type in ["log", "output"]:
                    if self.go_to_transferring or (self.log_transferring and tmp_file.type == "log"):
                        # don't go to transferring for successful ES jobs
                        if (
                            self.job.jobStatus == "finished"
                            and (EventServiceUtils.isEventServiceJob(self.job) and EventServiceUtils.isJumboJob(self.job))
                            and not EventServiceUtils.isJobCloningJob(self.job)
                        ):
                            continue
                        # skip distributed datasets
                        if tmp_file.destinationDBlock in dist_datasets:
                            continue
                        # skip no output
                        if tmp_file.status == "nooutput":
                            continue
                        # skip alternative stage-out
                        if tmp_file.lfn in self.job.altStgOutFileList():
                            continue
                        self.result.transferring_files.append(tmp_file.lfn)

        elif "--mergeOutput" not in self.job.jobParameters:
            # send request to DaTRI unless files will be merged
            tmp_top_datasets = {}
            # collect top-level datasets
            for tmp_name, tmp_val in sub_map.items():
                for ddm_id, opt_sub, opt_source in tmp_val:
                    tmp_top_name = sub_to_ds_map[tmp_name]
                    # append
                    if tmp_top_name not in tmp_top_datasets:
                        tmp_top_datasets[tmp_top_name] = []
                    if ddm_id not in tmp_top_datasets[tmp_top_name]:
                        tmp_top_datasets[tmp_top_name].append(ddm_id)
            # remove redundant CN from DN
            tmp_dn = self.job.prodUserID
            # send request
            if tmp_top_datasets and self.job_status == "finished":
                try:
                    status, user_info = rucioAPI.finger(tmp_dn)
                    if not status:
                        raise RuntimeError(f"user info not found for {tmp_dn} with {user_info}")
                    user_endpoints = []
                    # loop over all output datasets
                    for tmp_dataset_name, ddm_id_list in tmp_top_datasets.items():
                        for tmp_ddm_id in ddm_id_list:
                            if tmp_ddm_id == "NULL":
                                continue
                            if tmp_ddm_id not in user_endpoints:
                                user_endpoints.append(tmp_ddm_id)
                            # use group account for group.*
                            if tmp_dataset_name.startswith("group") and self.job.workingGroup not in ["", "NULL", None]:
                                tmp_dn = self.job.workingGroup
                            else:
                                tmp_dn = user_info["nickname"]
                            tmp_msg = f"registerDatasetLocation for Rucio ds={tmp_dataset_name} site={tmp_ddm_id} id={tmp_dn}"
                            self.logger.debug(tmp_msg)
                            rucioAPI.register_dataset_location(
                                tmp_dataset_name,
                                [tmp_ddm_id],
                                owner=tmp_dn,
                                activity="Analysis Output",
                            )
                    # set dataset status
                    for tmp_name in sub_map:
                        self.dataset_map[tmp_name].status = "running"
                except (InsufficientAccountLimit, InvalidRSEExpression) as err_type:
                    tmp_msg = f"Rucio rejected to transfer files to {','.join(user_endpoints)} since {err_type}"
                    self.logger.error(tmp_msg)
                    self.job.ddmErrorCode = ErrorCode.EC_Adder
                    self.job.ddmErrorDiag = f"Rucio failed with {err_type}"
                    # set dataset status
                    for tmp_name in sub_map:
                        self.dataset_map[tmp_name].status = "running"
                    # send warning
                    tmp_st = self.taskBuffer.update_problematic_resource_info(self.job.prodUserName, self.job.jediTaskID, user_endpoints[0], "dest")
                    if not tmp_st:
                        self.logger.debug("skip to send warning since already done")
                    else:
                        to_adder = self.taskBuffer.getEmailAddr(self.job.prodUserName)
                        if to_adder is None or to_adder.startswith("notsend"):
                            self.logger.debug("skip to send warning since suppressed")
                        else:
                            tmp_sm = self.send_email(to_adder, tmp_msg, self.job.jediTaskID)
                            self.logger.debug(f"sent warning with {tmp_sm}")
                except Exception:
                    err_type, err_value = sys.exc_info()[:2]
                    tmp_msg = f"registerDatasetLocation failed with {err_type} {err_value}"
                    self.logger.error(tmp_msg)
                    self.job.ddmErrorCode = ErrorCode.EC_Adder
                    self.job.ddmErrorDiag = f"Rucio failed with {err_type} {err_value}"

    # decompose idMap
    def decompose_id_map(self, id_map, dataset_destination_map, map_for_alt_stage_out, sub_to_dataset_map, alt_staged_files):
        """
        Decompose the idMap into a structure suitable for file registration.

        Args:
            id_map (dict): A dictionary mapping dataset names to file attributes.
            dataset_destination_map (dict): A dictionary mapping dataset names to destination sites.
            map_for_alt_stage_out (dict): A dictionary mapping alternative destination sites to file attributes.
            sub_to_dataset_map (dict): A dictionary mapping sub-dataset names to top-level dataset names.
            alt_staged_files (set): A set of files uploaded with alternative stage-out.

        Returns:
            dict: A decomposed idMap suitable for file registration.
        """
        # add item for top datasets
        for tmp_dataset in list(dataset_destination_map):
            tmp_top_dataset = sub_to_dataset_map[tmp_dataset]
            if tmp_top_dataset != tmp_dataset:
                dataset_destination_map[tmp_top_dataset] = dataset_destination_map[tmp_dataset]

        destination_id_map = {}
        for tmp_dataset in id_map:
            # exclude files uploaded with alternative stage-out
            tmp_files = [f for f in id_map[tmp_dataset] if f["lfn"] not in alt_staged_files]
            if not tmp_files:
                continue
            for tmp_dest in dataset_destination_map[tmp_dataset]:
                if tmp_dest not in destination_id_map:
                    destination_id_map[tmp_dest] = {}
                destination_id_map[tmp_dest][tmp_dataset] = tmp_files

        # add alternative stage-out info
        for tmp_dest in map_for_alt_stage_out:
            tmp_id_map = map_for_alt_stage_out[tmp_dest]
            for tmp_dataset in tmp_id_map:
                tmp_files = tmp_id_map[tmp_dataset]
                if tmp_dest not in destination_id_map:
                    destination_id_map[tmp_dest] = {}
                destination_id_map[tmp_dest][tmp_dataset] = tmp_files
        return destination_id_map

    # send email notification
    def send_email(self, to_adder, message, jedi_task_id):
        """
        Send an email notification.

        Args:
            to_adder (str): The email address to send the notification to.
            message (str): The message body of the email.
            jedi_task_id (int): The JEDI task ID associated with the email.

        Returns:
            bool: True if the email was sent successfully, False otherwise.
        """
        # subject
        mail_subject = f"PANDA WARNING for TaskID:{jedi_task_id} with --destSE"
        # message
        mail_body = f"Hello,\n\nTaskID:{jedi_task_id} cannot process the --destSE request\n\n"
        mail_body += f"Reason : {message}\n"
        # send
        ret_val = MailUtils().send(to_adder, mail_subject, mail_body)
        # return
        return ret_val

    # register ES files
    def register_event_service_files(self) -> None:
        """
        Register Event Service (ES) files with Rucio.

        Raises:
            Exception: If there is an error during the registration process.
        """
        self.logger.debug("registering ES files")
        try:
            # get ES dataset name
            event_service_dataset = EventServiceUtils.getEsDatasetName(self.job.jediTaskID)
            # collect files
            id_map = {}
            file_set = set()
            for file_spec in self.job.Files:
                if file_spec.type != "zipoutput":
                    continue
                if file_spec.lfn in file_set:
                    continue
                file_set.add(file_spec.lfn)
                # make file data
                file_data = {
                    "scope": EventServiceUtils.esScopeDDM,
                    "name": file_spec.lfn,
                    "bytes": file_spec.fsize,
                    "panda_id": file_spec.PandaID,
                    "task_id": file_spec.jediTaskID,
                }
                if file_spec.GUID not in [None, "NULL", ""]:
                    file_data["guid"] = file_spec.GUID
                if file_spec.dispatchDBlockToken not in [None, "NULL", ""]:
                    try:
                        file_data["events"] = int(file_spec.dispatchDBlockToken)
                    except Exception:
                        pass
                if file_spec.checksum not in [None, "NULL", ""]:
                    file_data["checksum"] = file_spec.checksum
                # get endpoint ID
                endpoint_id = int(file_spec.destinationSE.split("/")[0])
                # convert to DDM endpoint
                rse = self.taskBuffer.convertObjIDtoEndPoint(panda_config.endpoint_mapfile, endpoint_id)
                if rse is not None and rse["is_deterministic"]:
                    endpoint_name = rse["name"]
                    if endpoint_name not in id_map:
                        id_map[endpoint_name] = {}
                    if event_service_dataset not in id_map[endpoint_name]:
                        id_map[endpoint_name][event_service_dataset] = []
                    id_map[endpoint_name][event_service_dataset].append(file_data)

            # add files to dataset
            if id_map:
                self.logger.debug(f"adding ES files {str(id_map)}")
                try:
                    rucioAPI.register_files_in_dataset(id_map)
                except DataIdentifierNotFound:
                    self.logger.debug("ignored DataIdentifierNotFound")
        except Exception:
            err_type, err_value = sys.exc_info()[:2]
            err_str = f" : {err_type} {err_value}"
            err_str += traceback.format_exc()
            self.logger.error(err_str)
            raise
        self.logger.debug("done")
