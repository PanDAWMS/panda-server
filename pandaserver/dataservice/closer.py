"""
update dataset DB, and then close dataset and start Activator if needed

"""

import re
import sys
from typing import Dict, List

from pandacommon.pandalogger.PandaLogger import PandaLogger

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
    def __init__(self, taskBuffer, destination_db_blocks: List[str], job, panda_ddm: bool = False,
                 dataset_map: Dict = {}) -> None:
        """
        Constructor

        Args:
            task_buffer: Task buffer.
            destination_db_blocks (List[str]): Destination DB blocks.
            job: Job.
            panda_ddm (bool, optional): Panda DDM flag. Defaults to False.
            dataset_map (Dict, optional): Dataset map. Defaults to {}.
        """
        self.task_buffer = taskBuffer
        self.destination_db_blocks = destination_db_blocks
        self.job = job
        self.panda_id = job.PandaID
        self.panda_ddm = panda_ddm
        self.site_mapper = None
        self.dataset_map = dataset_map
        self.all_sub_finished = None

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

    # main
    def run(self):
        """
        Main
        """
        try:
            _logger.debug(f"{self.panda_id} Start {self.job.jobStatus}")
            flag_complete = True
            top_user_ds_list = []
            using_merger = False
            disable_notifier = False
            first_indv_ds = True
            final_status_ds = []
            for destination_db_block in self.destination_db_blocks:
                ds_list = []
                _logger.debug(f"{self.panda_id} start {destination_db_block}")
                # ignore tid datasets
                if re.search("_tid[\d_]+$", destination_db_block):
                    _logger.debug(f"{self.panda_id} skip {destination_db_block}")
                    continue
                # ignore HC datasets
                if (re.search("^hc_test\.", destination_db_block) is not None or
                        re.search("^user\.gangarbt\.", destination_db_block) is not None):
                    if (re.search("_sub\d+$", destination_db_block) is None and
                            re.search("\.lib$", destination_db_block) is None):
                        _logger.debug(f"{self.panda_id} skip HC {destination_db_block}")
                        continue
                # query dataset
                if destination_db_block in self.dataset_map:
                    dataset = self.dataset_map[destination_db_block]
                else:
                    dataset = self.task_buffer.queryDatasetWithMap({"name": destination_db_block})
                if dataset is None:
                    _logger.error(f"{self.panda_id} Not found : {destination_db_block}")
                    flag_complete = False
                    continue
                # skip tobedeleted/tobeclosed
                if dataset.status in ["cleanup", "tobeclosed", "completed", "deleted"]:
                    _logger.debug(f"{self.panda_id} skip {destination_db_block} due to {dataset.status}")
                    continue
                ds_list.append(dataset)
                # sort
                ds_list.sort()
                # count number of completed files
                not_finish = self.task_buffer.countFilesWithMap(
                    {"destinationDBlock": destination_db_block, "status": "unknown"})
                if not_finish < 0:
                    _logger.error(f"{self.panda_id} Invalid DB return : {not_finish}")
                    flag_complete = False
                    continue
                # check if completed
                _logger.debug(f"{self.panda_id} notFinish:{not_finish}")
                if self.job.destinationSE == "local" and self.job.prodSourceLabel in [
                    "user",
                    "panda",
                ]:
                    # close non-DQ2 destinationDBlock immediately
                    final_status = "closed"
                elif self.job.lockedby == "jedi" and self.is_top_level_ds(destination_db_block):
                    # set it closed in order not to trigger DDM cleanup. It will be closed by JEDI
                    final_status = "closed"
                elif self.job.prodSourceLabel in [
                    "user"] and "--mergeOutput" in self.job.jobParameters and self.job.processingType != "usermerge":
                    # merge output files
                    if first_indv_ds:
                        # set 'tobemerged' to only the first dataset to avoid triggering many Mergers for --individualOutDS
                        final_status = "tobemerged"
                        first_indv_ds = False
                    else:
                        final_status = "tobeclosed"
                    # set merging to top dataset
                    using_merger = True
                    # disable Notifier
                    disable_notifier = True
                elif self.job.produceUnMerge():
                    final_status = "doing"
                else:
                    # set status to 'tobeclosed' to trigger DQ2 closing
                    final_status = "tobeclosed"
                if not_finish == 0 and EventServiceUtils.isEventServiceMerge(self.job):
                    all_in_jobset_finished = self.check_sub_datasets_in_jobset()
                else:
                    all_in_jobset_finished = True
                if not_finish == 0 and all_in_jobset_finished:
                    _logger.debug(f"{self.panda_id} set {final_status} to dataset : {destination_db_block}")
                    # set status
                    dataset.status = final_status
                    # update dataset in DB
                    ret_t = self.task_buffer.updateDatasets(
                        ds_list,
                        withLock=True,
                        withCriteria="status<>:crStatus AND status<>:lockStatus ",
                        criteriaMap={":crStatus": final_status, ":lockStatus": "locked"},
                    )
                    if len(ret_t) > 0 and ret_t[0] == 1:
                        final_status_ds += ds_list
                        # close user datasets
                        if (
                                self.job.prodSourceLabel in ["user"]
                                and self.job.destinationDBlock.endswith("/")
                                and (dataset.name.startswith("user") or dataset.name.startswith("group"))
                        ):
                            # get top-level user dataset
                            top_user_ds_name = re.sub("_sub\d+$", "", dataset.name)
                            # update if it is the first attempt
                            if top_user_ds_name != dataset.name and top_user_ds_name not in top_user_ds_list and self.job.lockedby != "jedi":
                                top_user_ds = self.task_buffer.queryDatasetWithMap({"name": top_user_ds_name})
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
                                        _logger.debug(
                                            f"{self.panda_id} skip {top_user_ds_name} due to status={top_user_ds.status}")
                                    else:
                                        # set status
                                        if self.job.processingType.startswith(
                                                "gangarobot") or self.job.processingType.startswith("hammercloud"):
                                            # not trigger freezing for HC datasets so that files can be appended
                                            top_user_ds.status = "completed"
                                        elif not using_merger:
                                            top_user_ds.status = final_status
                                        else:
                                            top_user_ds.status = "merging"
                                        # append to avoid repetition
                                        top_user_ds_list.append(top_user_ds_name)
                                        # update DB
                                        ret_top_t = self.task_buffer.updateDatasets(
                                            [top_user_ds],
                                            withLock=True,
                                            withCriteria="status<>:crStatus",
                                            criteriaMap={":crStatus": top_user_ds.status},
                                        )
                                        if len(ret_top_t) > 0 and ret_top_t[0] == 1:
                                            _logger.debug(
                                                f"{self.panda_id} set {top_user_ds.status} to top dataset : {top_user_ds_name}")
                                        else:
                                            _logger.debug(
                                                f"{self.panda_id} failed to update top dataset : {top_user_ds_name}")
                            # get parent dataset for merge job
                            if self.job.processingType == "usermerge":
                                tmp_match = re.search("--parentDS ([^ '\"]+)", self.job.jobParameters)
                                if tmp_match is None:
                                    _logger.error(f"{self.panda_id} failed to extract parentDS")
                                else:
                                    unmerged_ds_name = tmp_match.group(1)
                                    # update if it is the first attempt
                                    if unmerged_ds_name not in top_user_ds_list:
                                        unmerged_ds = self.task_buffer.queryDatasetWithMap({"name": unmerged_ds_name})
                                        if unmerged_ds is None:
                                            _logger.error(
                                                f"{self.panda_id} failed to get parentDS={unmerged_ds_name} from DB")
                                        else:
                                            # check status
                                            if unmerged_ds.status in [
                                                "completed",
                                                "cleanup",
                                                "tobeclosed",
                                            ]:
                                                _logger.debug(
                                                    f"{self.panda_id} skip {unmerged_ds_name} due to status={unmerged_ds.status}")
                                            else:
                                                # set status
                                                unmerged_ds.status = final_status
                                                # append to avoid repetition
                                                top_user_ds_list.append(unmerged_ds_name)
                                                # update DB
                                                ret_top_t = self.task_buffer.updateDatasets(
                                                    [unmerged_ds],
                                                    withLock=True,
                                                    withCriteria="status<>:crStatus",
                                                    criteriaMap={":crStatus": unmerged_ds.status},
                                                )
                                                if len(ret_top_t) > 0 and ret_top_t[0] == 1:
                                                    _logger.debug(
                                                        f"{self.panda_id} set {unmerged_ds.status} to parent dataset : {unmerged_ds_name}")
                                                else:
                                                    _logger.debug(
                                                        f"{self.panda_id} failed to update parent dataset : {unmerged_ds_name}")
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
                        ds_list,
                        withLock=True,
                        withCriteria="status<>:crStatus AND status<>:lockStatus ",
                        criteriaMap={":crStatus": final_status, ":lockStatus": "locked"},
                    )
                    # unset flag
                    flag_complete = False
                # end
                _logger.debug(f"{self.panda_id} end {destination_db_block}")
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
                _logger.debug(f"{self.panda_id} finalize {self.job.prodUserName} {self.job.jobDefinitionID}")
                finalized_flag = self.task_buffer.finalizePendingJobs(self.job.prodUserName, self.job.jobDefinitionID)
                _logger.debug(f"{self.panda_id} finalized with {finalized_flag}")
            # update unmerged datasets in JEDI to trigger merging
            if flag_complete and self.job.produceUnMerge() and final_status_ds:
                if finalized_flag:
                    tmp_stat = self.task_buffer.updateUnmergedDatasets(self.job, final_status_ds)
                    _logger.debug(f"{self.panda_id} updated unmerged datasets with {tmp_stat}")
            # start notifier
            _logger.debug(f"{self.panda_id} source:{self.job.prodSourceLabel} complete:{flag_complete}")
            if (
                    (self.job.jobStatus != "transferring")
                    and ((flag_complete and self.job.prodSourceLabel == "user") or (
                    self.job.jobStatus == "failed" and self.job.prodSourceLabel == "panda"))
                    and self.job.lockedby != "jedi"
            ):
                # don't send email for merge jobs
                if (not disable_notifier) and self.job.processingType not in [
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
                        _logger.debug(f"{self.panda_id} use_notifier:{use_notifier}")
                    if use_notifier:
                        _logger.debug(f"{self.panda_id} start Notifier")
                        notifier_thread = Notifier.Notifier(
                            self.task_buffer,
                            self.job,
                            self.destination_db_blocks,
                            summary_info,
                        )
                        notifier_thread.run()
                        _logger.debug(f"{self.panda_id} end Notifier")
            _logger.debug(f"{self.panda_id} End")
        except Exception:
            err_type, err_value = sys.exc_info()[:2]
            _logger.error(f"{err_type} {err_value}")

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
        # skip already checked
        if self.all_sub_finished is not None:
            return self.all_sub_finished
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
                    _logger.debug(
                        f"{self.panda_id} related sub dataset {sub_dataset} from {job_spec.PandaID} has {not_finish} unfinished files")
                    self.all_sub_finished = False
                    break
        if self.all_sub_finished is None:
            _logger.debug(f"{self.panda_id} all related sub datasets are done")
            self.all_sub_finished = True
        return self.all_sub_finished
