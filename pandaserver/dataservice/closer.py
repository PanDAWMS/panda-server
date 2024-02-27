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
from pandaserver.dataservice import Notifier
from pandaserver.dataservice.Activator import Activator
from pandaserver.taskbuffer import EventServiceUtils

# logger
_logger = PandaLogger().getLogger("closer")


def init_logger(p_logger: PandaLogger) -> None:
    """
    Redirect logging to parent as it doesn't work in nested threads

    Args:
        p_logger (PandaLogger): The parent logger.
    """
    # redirect logging to parent as it doesn't work in nested threads
    global _logger
    _logger = p_logger
    Notifier.initLogger(_logger)


class Closer:
    """
    Update dataset DB, and then close dataset and start Activator if needed
    """

    # constructor
    def __init__(self, taskBuffer, destination_dispatch_blocks: List[str], job, panda_ddm: bool = False,
                 dataset_map: Dict = None) -> None:
        """
        Constructor

        Args:
            task_buffer: Task buffer.
            destination_dispatch_blocks (List[str]): Destination Dispatch blocks.
            job: Job.
            panda_ddm (bool, optional): Panda DDM flag. Defaults to False.
            dataset_map (Dict, optional): Dataset map. Defaults to {}.
        """
        self.task_buffer = taskBuffer
        self.destination_dispatch_blocks = destination_dispatch_blocks
        self.job = job
        self.panda_id = job.PandaID
        self.panda_ddm = panda_ddm
        self.site_mapper = None
        self.dataset_map = dataset_map if dataset_map is not None else {}
        self.all_subscription_finished = None
        self.first_indv_ds = True
        self.using_merger = False
        self.disable_notifier = False

    def start(self) -> None:
        """
        To keep backward compatibility
        """
        self.run()

    def join(self) -> None:
        """
        Join
        """
        pass

    def is_top_level_ds(self, dataset_name: str) -> bool:
        """
        Check if top dataset

        Args:
            dataset_name (str): Dataset name.

        Returns:
            bool: True if top dataset, False otherwise.
        """
        top_ds = re.sub("_sub\d+$", "", dataset_name)
        if top_ds == dataset_name:
            return True
        return False

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
        checked_ds = set()
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
                if sub_dataset in checked_ds:
                    continue
                checked_ds.add(sub_dataset)
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

    def determine_final_status(self, destination_dispatch_block: str) -> str:
        """
        Determine the final status of a dispatch block.

        Args:
            destination_dispatch_block (str): The destination dispatch block.

        Returns:
            str: The final status.
        """
        if self.job.destinationSE == "local" and self.job.prodSourceLabel in ["user", "panda"]:
            # close non-DQ2 destinationDBlock immediately
            final_status = "closed"
        elif self.job.lockedby == "jedi" and self.is_top_level_ds(destination_dispatch_block):
            # set it closed in order not to trigger DDM cleanup. It will be closed by JEDI
            final_status = "closed"
        elif self.job.prodSourceLabel in ["user"] and "--mergeOutput" in self.job.jobParameters and self.job.processingType != "usermerge":
            # merge output files
            if self.first_indv_ds:
                # set 'tobemerged' to only the first dataset to avoid triggering many Mergers for --individualOutDS
                final_status = "tobemerged"
                self.first_indv_ds = False
            else:
                final_status = "tobeclosed"
            # set merging to top dataset
            self.using_merger = True
            # disable Notifier
            self.disable_notifier = True
        elif self.job.produceUnMerge():
            final_status = "doing"
        else:
            # set status to 'tobeclosed' to trigger DQ2 closing
            final_status = "tobeclosed"
        return final_status

    # main
    def run(self):
        """
        Main method to run the Closer class. It processes each destination dispatch block,
        updates the dataset status, finalizes pending jobs if necessary, and starts the notifier.
        """
        try:
            tmp_log = LogWrapper(_logger,
                                 f"run-{datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None).isoformat('/')}-{self.panda_id}")
            tmp_log.debug(f"Start {self.job.jobStatus}")
            flag_complete = True
            top_user_dataset_list = []
            final_status_ds = []

            for destination_dispatch_block in self.destination_dispatch_blocks:
                dataset_list = []
                tmp_log.debug(f"start {destination_dispatch_block}")

                # ignore tid datasets
                if re.search("_tid[\d_]+$", destination_dispatch_block):
                    tmp_log.debug(f"skip {destination_dispatch_block}")
                    continue

                # ignore HC datasets
                if (re.search("^hc_test\.", destination_dispatch_block) is not None or
                        re.search("^user\.gangarbt\.", destination_dispatch_block) is not None):
                    if (re.search("_sub\d+$", destination_dispatch_block) is None and
                            re.search("\.lib$", destination_dispatch_block) is None):
                        tmp_log.debug(f"skip HC {destination_dispatch_block}")
                        continue

                # query dataset
                if destination_dispatch_block in self.dataset_map:
                    dataset = self.dataset_map[destination_dispatch_block]
                else:
                    dataset = self.task_buffer.queryDatasetWithMap({"name": destination_dispatch_block})

                if dataset is None:
                    tmp_log.error(f"Not found : {destination_dispatch_block}")
                    flag_complete = False
                    continue

                # skip tobedeleted/tobeclosed
                if dataset.status in ["cleanup", "tobeclosed", "completed", "deleted"]:
                    tmp_log.debug(f"skip {destination_dispatch_block} due to {dataset.status}")
                    continue

                dataset_list.append(dataset)
                dataset_list.sort()

                # count number of completed files
                not_finish = self.task_buffer.countFilesWithMap(
                    {"destinationDBlock": destination_dispatch_block, "status": "unknown"})
                if not_finish < 0:
                    tmp_log.error(f"Invalid DB return : {not_finish}")
                    flag_complete = False
                    continue

                # check if completed
                tmp_log.debug(f"notFinish:{not_finish}")
                final_status = self.determine_final_status(destination_dispatch_block)

                if not_finish == 0 and EventServiceUtils.isEventServiceMerge(self.job):
                    all_in_jobset_finished = self.check_sub_datasets_in_jobset()
                else:
                    all_in_jobset_finished = True

                if not_finish == 0 and all_in_jobset_finished:
                    tmp_log.debug(f"set {final_status} to dataset : {destination_dispatch_block}")
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
                        final_status_ds += dataset_list
                        # close user datasets
                        if (
                                self.job.prodSourceLabel in ["user"]
                                and self.job.destinationDBlock.endswith("/")
                                and (dataset.name.startswith("user") or dataset.name.startswith("group"))
                        ):
                            # get top-level user dataset
                            top_user_dataset_name = re.sub("_sub\d+$", "", dataset.name)
                            # update if it is the first attempt
                            if top_user_dataset_name != dataset.name and top_user_dataset_name not in top_user_dataset_list and self.job.lockedby != "jedi":
                                top_user_ds = self.task_buffer.queryDatasetWithMap({"name": top_user_dataset_name})
                                if top_user_ds is not None:
                                    # check status
                                    if top_user_ds.status in [
                                        "completed",
                                        "cleanup",
                                        "tobeclosed",
                                        "deleted",
                                        "tobemerged",
                                        "merging",
                                    ]:
                                        tmp_log.debug(
                                            f"skip {top_user_dataset_name} due to status={top_user_ds.status}")
                                    else:
                                        # set status
                                        if self.job.processingType.startswith(
                                                "gangarobot") or self.job.processingType.startswith("hammercloud"):
                                            # not trigger freezing for HC datasets so that files can be appended
                                            top_user_ds.status = "completed"
                                        elif not self.using_merger:
                                            top_user_ds.status = final_status
                                        else:
                                            top_user_ds.status = "merging"
                                        # append to avoid repetition
                                        top_user_dataset_list.append(top_user_dataset_name)
                                        # update DB
                                        ret_top_t = self.task_buffer.updateDatasets(
                                            [top_user_ds],
                                            withLock=True,
                                            withCriteria="status<>:crStatus",
                                            criteriaMap={":crStatus": top_user_ds.status},
                                        )
                                        if len(ret_top_t) > 0 and ret_top_t[0] == 1:
                                            tmp_log.debug(
                                                f"set {top_user_ds.status} to top dataset : {top_user_dataset_name}")
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
                                    if unmerged_dataset_name not in top_user_dataset_list:
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
                                                top_user_dataset_list.append(unmerged_dataset_name)
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
                tmp_log.debug(f"end {destination_dispatch_block}")
            # special actions for vo
            if flag_complete:
                closer_plugin_class = panda_config.getPlugin("closer_plugins", self.job.VO)
                if closer_plugin_class is None and self.job.VO == "atlas":
                    # use ATLAS plugin for ATLAS
                    from pandaserver.dataservice.closer_atlas_plugin import (
                        CloserAtlasPlugin,
                    )

                    closer_plugin_class = CloserAtlasPlugin
                if closer_plugin_class is not None:
                    closer_plugin = closer_plugin_class(self.job, final_status_ds, _logger)
                    closer_plugin.execute()
            # change pending jobs to failed
            finalized_flag = True
            if flag_complete and self.job.prodSourceLabel == "user":
                tmp_log.debug(f"finalize {self.job.prodUserName} {self.job.jobDefinitionID}")
                finalized_flag = self.task_buffer.finalizePendingJobs(self.job.prodUserName, self.job.jobDefinitionID)
                tmp_log.debug(f"finalized with {finalized_flag}")
            # update unmerged datasets in JEDI to trigger merging
            if flag_complete and self.job.produceUnMerge() and final_status_ds:
                if finalized_flag:
                    tmp_stat = self.task_buffer.updateUnmergedDatasets(self.job, final_status_ds)
                    tmp_log.debug(f"updated unmerged datasets with {tmp_stat}")
            # start notifier
            tmp_log.debug(f"source:{self.job.prodSourceLabel} complete:{flag_complete}")
            if (
                    (self.job.jobStatus != "transferring")
                    and ((flag_complete and self.job.prodSourceLabel == "user") or (
                    self.job.jobStatus == "failed" and self.job.prodSourceLabel == "panda"))
                    and self.job.lockedby != "jedi"
            ):
                # don't send email for merge jobs
                if (not self.disable_notifier) and self.job.processingType not in [
                    "merge",
                    "unmerge",
                ]:
                    use_notifier = True
                    summary_info = {}
                    # check all jobDefIDs in jobsetID
                    if self.job.jobsetID not in [0, None, "NULL"]:
                        (
                            use_notifier,
                            summary_info,
                        ) = self.task_buffer.checkDatasetStatusForNotifier(
                            self.job.jobsetID,
                            self.job.jobDefinitionID,
                            self.job.prodUserName,
                        )
                        tmp_log.debug(f"use_notifier:{use_notifier}")
                    if use_notifier:
                        tmp_log.debug("start Notifier")
                        notifier_thread = Notifier.Notifier(
                            self.task_buffer,
                            self.job,
                            self.destination_dispatch_blocks,
                            summary_info,
                        )
                        notifier_thread.run()
                        tmp_log.debug("end Notifier")
            tmp_log.debug("End")
        except Exception:
            err_type, err_value = sys.exc_info()[:2]
            tmp_log.error(f"{err_type} {err_value}")
