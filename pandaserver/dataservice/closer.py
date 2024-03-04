"""
update dataset DB, and then close dataset and start Activator if needed

"""

import re
import sys
import datetime

from typing import Dict, List

from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandalogger.LogWrapper import LogWrapper

from pandaserver.config import panda_config
from pandaserver.dataservice.Activator import Activator
from pandaserver.taskbuffer import EventServiceUtils

# logger
_logger = PandaLogger().getLogger("closer")

class Closer:
    """
    Update dataset DB, and then close dataset and start Activator if needed.
    The class updates output-related dataset records (_sub, output, and log) in
    the database, closes _sub datasets if necessary, and activates downstream
    runJobs if the output dataset is for buildJob (libDS, library dataset).
    Activator changes job status from defined/assigned to activated.
    """

    # constructor
    def __init__(self, taskBuffer, destination_data_blocks: List[str], job, panda_ddm: bool = False,
                 dataset_map: Dict = None) -> None:
        """
        Constructor

        Args:
            task_buffer: Task buffer.
            destination_data_blocks (List[str]): Destination Dispatch blocks.
            job: Job.
            panda_ddm (bool, optional): Panda DDM flag. Defaults to False.
            dataset_map (Dict, optional): Dataset map. Defaults to {}.
        """
        self.task_buffer = taskBuffer
        self.destination_data_blocks = destination_data_blocks
        self.job = job
        self.panda_id = job.PandaID
        self.panda_ddm = panda_ddm
        self.site_mapper = None
        self.dataset_map = dataset_map if dataset_map is not None else {}
        self.all_subscription_finished = None
        self.using_merger = False
        self.top_user_dataset_list = []

    def is_top_level_dataset(self, dataset_name: str) -> bool:
        """
        Check if top dataset

        Args:
            dataset_name (str): Dataset name.

        Returns:
            bool: True if top dataset, False otherwise.
        """
        return re.sub("_sub\d+$", "", dataset_name) == dataset_name

    def check_sub_datasets_in_jobset(self) -> bool:
        """
        Check sub datasets with the same jobset

        Returns:
            bool: True if all sub datasets are done, False otherwise.
        """
        tmp_log = LogWrapper(_logger,
                             f"check_sub_datasets_in_jobset-{datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None).isoformat('/')}")
        # skip already checked
        if self.all_subscription_finished is not None:
            return self.all_subscription_finished
        # get consumers in the jobset
        jobs = self.task_buffer.getOriginalConsumers(self.job.jediTaskID, self.job.jobsetID, self.job.panda_id)
        checked_dataset = set()
        for job_spec in jobs:
            # collect all sub datasets
            sub_datasets = set()
            for file_spec in job_spec.Files:
                if file_spec.type == "output":
                    sub_datasets.add(file_spec.destinationDBlock)
            sub_datasets = sorted(sub_datasets)
            if len(sub_datasets) > 0:
                # use the first sub dataset
                sub_dataset = sub_datasets[0]
                # skip if already checked
                if sub_dataset in checked_dataset:
                    continue
                checked_dataset.add(sub_dataset)
                # count the number of unfinished
                not_finish = self.task_buffer.countFilesWithMap({"destinationDBlock": sub_dataset, "status": "unknown"})
                if not_finish != 0:
                    tmp_log.debug(
                        f"related sub dataset {sub_dataset} from {job_spec.PandaID} has {not_finish} unfinished files")
                    self.all_subscription_finished = False
                    break
        if self.all_subscription_finished is None:
            tmp_log.debug("all related sub datasets are done")
            self.all_subscription_finished = True
        return self.all_subscription_finished

    def determine_final_status(self, destination_data_block: str) -> str:
        """
        Determine the final status of a dispatch block.

        Args:
            destination_data_block (str): The destination dispatch block.

        Returns:
            str: The final status.
        """
        # for queues without rucio storage element attached
        if self.job.destinationSE == "local" and self.job.prodSourceLabel in ["user", "panda"]:
            # close non-Rucio destinationDBlock immediately
            final_status = "closed"
        elif self.job.lockedby == "jedi" and self.is_top_level_dataset(destination_data_block):
            # set it closed in order not to trigger DDM cleanup. It will be closed by JEDI
            final_status = "closed"
        elif self.job.produceUnMerge():
            final_status = "doing"
        else:
            # set status to 'tobeclosed' to trigger Rucio closing
            final_status = "tobeclosed"
        return final_status

    def perform_vo_actions(self, final_status_dataset: list) -> None:
        """
        Perform special actions for vo.

        Args:
            final_status_dataset (list): The final status dataset.
        """
        closer_plugin_class = panda_config.getPlugin("closer_plugins", self.job.VO)
        if closer_plugin_class is None and self.job.VO == "atlas":
            # use ATLAS plugin for ATLAS
            from pandaserver.dataservice.closer_atlas_plugin import (
                CloserAtlasPlugin,
            )

            closer_plugin_class = CloserAtlasPlugin
        if closer_plugin_class is not None:
            closer_plugin = closer_plugin_class(self.job, final_status_dataset, _logger)
            closer_plugin.execute()

    def close_user_datasets(self, dataset, final_status: str):
        """
        Close user datasets

        Args:
            dataset: Dataset.
            final_status (str): Final status.
        """
        tmp_log = LogWrapper(_logger,
                             f"close_user_datasets-{datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None).isoformat('/')}")

        if (
                self.job.prodSourceLabel in ["user"]
                and self.job.destinationDBlock.endswith("/")
                and (dataset.name.startswith("user") or dataset.name.startswith("group"))
        ):
            # get top-level user dataset
            top_user_dataset_name = re.sub("_sub\d+$", "", dataset.name)
            # update if it is the first attempt
            if top_user_dataset_name != dataset.name and top_user_dataset_name not in self.top_user_dataset_list and self.job.lockedby != "jedi":
                top_user_dataset = self.task_buffer.queryDatasetWithMap({"name": top_user_dataset_name})
                if top_user_dataset is not None:
                    # check status
                    if top_user_dataset.status in [
                        "completed",
                        "cleanup",
                        "tobeclosed",
                        "deleted",
                        "tobemerged",
                        "merging",
                    ]:
                        tmp_log.debug(
                            f"skip {top_user_dataset_name} due to status={top_user_dataset.status}")
                    else:
                        # set status
                        if self.job.processingType.startswith(
                                "gangarobot") or self.job.processingType.startswith("hammercloud"):
                            # not trigger freezing for HC datasets so that files can be appended
                            top_user_dataset.status = "completed"
                        elif not self.using_merger:
                            top_user_dataset.status = final_status
                        else:
                            top_user_dataset.status = "merging"
                        # append to avoid repetition
                        self.top_user_dataset_list.append(top_user_dataset_name)
                        # update DB
                        ret_top_t = self.task_buffer.updateDatasets(
                            [top_user_dataset],
                            withLock=True,
                            withCriteria="status<>:crStatus",
                            criteriaMap={":crStatus": top_user_dataset.status},
                        )
                        if len(ret_top_t) > 0 and ret_top_t[0] == 1:
                            tmp_log.debug(
                                f"set {top_user_dataset.status} to top dataset : {top_user_dataset_name}")
                        else:
                            tmp_log.debug(
                                f"failed to update top dataset : {top_user_dataset_name}")
            # get parent dataset for merge job
            if self.job.processingType == "usermerge":
                tmp_match = re.search("--parentDS ([^ '\"]+)", self.job.jobParameters)
                if tmp_match is None:
                    tmp_log.error("failed to extract parentDS")
                else:
                    unmerged_dataset_name = tmp_match.group(1)
                    # update if it is the first attempt
                    if unmerged_dataset_name not in self.top_user_dataset_list:
                        unmerged_dataset = self.task_buffer.queryDatasetWithMap({"name": unmerged_dataset_name})
                        if unmerged_dataset is None:
                            tmp_log.error(
                                f"failed to get parentDS={unmerged_dataset_name} from DB")
                        else:
                            # check status
                            if unmerged_dataset.status in [
                                "completed",
                                "cleanup",
                                "tobeclosed",
                            ]:
                                tmp_log.debug(
                                    f"skip {unmerged_dataset_name} due to status={unmerged_dataset.status}")
                            else:
                                # set status
                                unmerged_dataset.status = final_status
                                # append to avoid repetition
                                self.top_user_dataset_list.append(unmerged_dataset_name)
                                # update DB
                                ret_top_t = self.task_buffer.updateDatasets(
                                    [unmerged_dataset],
                                    withLock=True,
                                    withCriteria="status<>:crStatus",
                                    criteriaMap={":crStatus": unmerged_dataset.status},
                                )
                                if len(ret_top_t) > 0 and ret_top_t[0] == 1:
                                    tmp_log.debug(
                                        f"set {unmerged_dataset.status} to parent dataset : {unmerged_dataset_name}")
                                else:
                                    tmp_log.debug(
                                        f"failed to update parent dataset : {unmerged_dataset_name}")
        # start Activator
        if re.search("_sub\d+$", dataset.name) is None:
            if self.job.prodSourceLabel == "panda" and self.job.processingType in ["merge", "unmerge"]:
                # don't trigger Activator for merge jobs
                pass
            else:
                if self.job.jobStatus == "finished":
                    activator_thread = Activator(self.task_buffer, dataset)
                    activator_thread.start()
                    activator_thread.join()
        else:
            # unset flag since another thread already updated
            # flag_complete = False
            pass

    # main
    def run(self):
        """
        Main method to run the Closer class. It processes each destination dispatch block,
        updates the dataset status and finalizes pending jobs if necessary.
        """
        try:
            tmp_log = LogWrapper(_logger,
                                 f"run-{datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None).isoformat('/')}-{self.panda_id}")
            tmp_log.debug(f"Start with job status: {self.job.jobStatus}")
            flag_complete = True
            final_status_dataset = []

            for destination_data_block in self.destination_data_blocks:
                dataset_list = []
                tmp_log.debug(f"start with destination dispatch block: {destination_data_block}")

                # ignore task output datasets (tid) datasets
                if re.search("_tid[\d_]+$", destination_data_block):
                    tmp_log.debug(f"skip {destination_data_block}")
                    continue

                # ignore HC datasets
                if (re.search("^hc_test\.", destination_data_block) is not None or
                        re.search("^user\.gangarbt\.", destination_data_block) is not None):
                    if (re.search("_sub\d+$", destination_data_block) is None and
                            re.search("\.lib$", destination_data_block) is None):
                        tmp_log.debug(f"skip HC {destination_data_block}")
                        continue

                # query dataset
                if destination_data_block in self.dataset_map:
                    dataset = self.dataset_map[destination_data_block]
                else:
                    dataset = self.task_buffer.queryDatasetWithMap({"name": destination_data_block})

                if dataset is None:
                    tmp_log.error(f"Not found : {destination_data_block}")
                    flag_complete = False
                    continue

                # skip tobedeleted/tobeclosed
                if dataset.status in ["cleanup", "tobeclosed", "completed", "deleted"]:
                    tmp_log.debug(f"skip {destination_data_block} due to dataset status: {dataset.status}")
                    continue

                dataset_list.append(dataset)
                dataset_list.sort()

                # count number of completed files
                not_finish = self.task_buffer.countFilesWithMap(
                    {"destinationDBlock": destination_data_block, "status": "unknown"})
                if not_finish < 0:
                    tmp_log.error(f"Invalid dispatch block file count: {not_finish}")
                    flag_complete = False
                    continue

                # check if completed
                tmp_log.debug(f"Pending file count: {not_finish}")
                final_status = self.determine_final_status(destination_data_block)

                if not_finish == 0 and EventServiceUtils.isEventServiceMerge(self.job):
                    all_in_jobset_finished = self.check_sub_datasets_in_jobset()
                else:
                    all_in_jobset_finished = True

                if not_finish == 0 and all_in_jobset_finished:
                    tmp_log.debug(f"Set final status: {final_status} to dataset: {destination_data_block}")
                    # set status
                    dataset.status = final_status
                    # update dataset in DB
                    ret_t = self.task_buffer.updateDatasets(
                        dataset_list,
                        withLock=True,
                        withCriteria="status<>:crStatus AND status<>:lockStatus ",
                        criteriaMap={":crStatus": final_status, ":lockStatus": "locked"},
                    )
                    if len(ret_t) > 0 and ret_t[0] == 1:
                        final_status_dataset += dataset_list
                        self.close_user_datasets(dataset, final_status)
                    else:
                        pass
                else:
                    # update dataset in DB
                    self.task_buffer.updateDatasets(
                        dataset_list,
                        withLock=True,
                        withCriteria="status<>:crStatus AND status<>:lockStatus ",
                        criteriaMap={":crStatus": final_status, ":lockStatus": "locked"},
                    )
                    # unset flag
                    flag_complete = False
                # end
                tmp_log.debug(f"end {destination_data_block}")
            # special actions for vo
            if flag_complete:
                self.perform_vo_actions(final_status_dataset)

            # update unmerged datasets in JEDI to trigger merging
            if flag_complete and self.job.produceUnMerge() and final_status_dataset:
                tmp_stat = self.task_buffer.updateUnmergedDatasets(self.job, final_status_dataset)
                tmp_log.debug(f"updated unmerged datasets with {tmp_stat}")

            tmp_log.debug("End")
        except Exception:
            err_type, err_value = sys.exc_info()[:2]
            tmp_log.error(f"{err_type} {err_value}")
