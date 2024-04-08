"""
update dataset DB, and then close dataset and start Activator if needed

"""

import datetime
import sys
from typing import Dict, List

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandalogger.PandaLogger import PandaLogger

from pandaserver.config import panda_config
from pandaserver.dataservice import DataServiceUtils
from pandaserver.dataservice.activator import Activator
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
    def __init__(self, taskBuffer, destination_data_blocks: List[str], job, dataset_map: Dict = None) -> None:
        """
        Constructor

        Args:
            taskBuffer: Task buffer.
            destination_data_blocks (List[str]): Destination Dispatch blocks.
            job: Job.
            dataset_map (Dict, optional): Dataset map. Defaults to {}.
        """
        self.task_buffer = taskBuffer
        self.destination_data_blocks = destination_data_blocks
        self.job = job
        self.panda_id = job.PandaID
        self.site_mapper = None
        self.dataset_map = dataset_map if dataset_map is not None else {}
        self.all_subscription_finished = None

    def check_sub_datasets_in_jobset(self) -> bool:
        """
        Check sub datasets with the same jobset

        Returns:
            bool: True if all sub datasets are done, False otherwise.
        """
        tmp_log = LogWrapper(_logger, f"check_sub_datasets_in_jobset-{datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None).isoformat('/')}")
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
                    tmp_log.debug(f"related sub dataset {sub_dataset} from {job_spec.PandaID} has {not_finish} unfinished files")
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
            return "closed"
        if self.job.lockedby == "jedi" and DataServiceUtils.is_top_level_dataset(destination_data_block):
            # set it closed in order not to trigger DDM cleanup. It will be closed by JEDI
            return "closed"
        if self.job.produceUnMerge():
            return "doing"

        # set status to 'tobeclosed' to trigger Rucio closing
        return "tobeclosed"

    def perform_vo_actions(self, final_status_dataset: list) -> None:
        """
        Perform special actions for vo.

        Args:
            final_status_dataset (list): The final status dataset.
        """
        closer_plugin_class = panda_config.getPlugin("closer_plugins", self.job.VO)
        if closer_plugin_class is None and self.job.VO == "atlas":
            # use ATLAS plugin for ATLAS
            from pandaserver.dataservice.closer_atlas_plugin import CloserAtlasPlugin

            closer_plugin_class = CloserAtlasPlugin
        if closer_plugin_class is not None:
            closer_plugin = closer_plugin_class(self.job, final_status_dataset, _logger)
            closer_plugin.execute()

    def start_activator(self, dataset):
        """
        Start the activator

        Args:
            dataset: Dataset.
            final_status (str): Final status.
        """
        # start Activator
        if (
            not DataServiceUtils.is_sub_dataset(dataset.name)
            and self.job.jobStatus == "finished"
            and (self.job.prodSourceLabel != "panda" or self.job.processingType not in ["merge", "unmerge"])
        ):
            activator_thread = Activator(self.task_buffer, dataset)
            activator_thread.run()

    # main
    def run(self):
        """
        Main method to run the Closer class. It processes each destination dispatch block,
        updates the dataset status and finalizes pending jobs if necessary.
        """
        try:
            tmp_log = LogWrapper(_logger, f"run-{datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None).isoformat('/')}-{self.panda_id}")
            tmp_log.debug(f"Start with job status: {self.job.jobStatus}")
            flag_complete = True
            final_status_dataset = []

            for destination_data_block in self.destination_data_blocks:
                dataset_list = []
                tmp_log.debug(f"start with destination dispatch block: {destination_data_block}")

                # ignore task output datasets (tid) datasets
                if DataServiceUtils.is_tid_dataset(destination_data_block):
                    tmp_log.debug(f"skip {destination_data_block}")
                    continue

                # ignore HC datasets
                if DataServiceUtils.is_hammercloud_dataset(destination_data_block) or DataServiceUtils.is_user_gangarbt_dataset(destination_data_block):
                    if not DataServiceUtils.is_sub_dataset(destination_data_block) and not DataServiceUtils.is_lib_dataset(destination_data_block):
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
                not_finish = self.task_buffer.countFilesWithMap({"destinationDBlock": destination_data_block, "status": "unknown"})
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
                        self.start_activator(dataset)
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
