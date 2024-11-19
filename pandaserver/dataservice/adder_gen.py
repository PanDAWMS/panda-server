"""
General Adder plugin. Add data to dataset

"""

import datetime
import json
import re
import sys
import time
import traceback

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandalogger.PandaLogger import PandaLogger

import pandaserver.dataservice.ErrorCode
import pandaserver.taskbuffer.ErrorCode
from pandaserver.config import panda_config
from pandaserver.dataservice import closer
from pandaserver.taskbuffer import EventServiceUtils, JobUtils, retryModule

_logger = PandaLogger().getLogger("adder")

panda_config.setupPlugin()


class AdderGen:
    """
    General Adder plugin.
    """

    # constructor
    def __init__(
        self,
        taskBuffer,
        job_id,
        job_status,
        attempt_nr,
        ignore_tmp_error=True,
        siteMapper=None,
        pid=None,
        prelock_pid=None,
        lock_offset=10,
        lock_pool=None,
    ) -> None:
        """
        Initialize the AdderGen.

        :param job: The job object.
        :param params: Additional parameters.
        """
        self.job = None
        self.job_id = job_id
        self.job_status = job_status
        self.taskBuffer = taskBuffer
        self.ignore_tmp_error = ignore_tmp_error
        self.lock_offset = lock_offset
        self.siteMapper = siteMapper
        self.dataset_map = {}
        self.extra_info = {
            "surl": {},
            "nevents": {},
            "lbnr": {},
            "endpoint": {},
            "guid": {},
        }
        self.attempt_nr = attempt_nr
        self.pid = pid
        self.prelock_pid = prelock_pid
        self.data = None
        self.lock_pool = lock_pool
        self.report_dict = None
        self.adder_plugin = None
        self.add_result = None
        self.adder_plugin_class = None
        # logger
        self.logger = LogWrapper(_logger, str(self.job_id))

    # main
    def run(self):
        """
        Run the AdderGen plugin.
        """
        try:
            start_time = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
            self.logger.debug(f"new start: {self.job_status} attemptNr={self.attempt_nr}")

            # got lock, get the report
            self.get_report()

            # query job
            self.job = self.taskBuffer.peekJobs([self.job_id], fromDefined=False, fromWaiting=False, forAnal=True)[0]

            # check if job has finished
            self.check_job_status()

            duration = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - start_time
            self.logger.debug("end: took %s.%03d sec in total" % (duration.seconds, duration.microseconds / 1000))

            # remove Catalog
            self.taskBuffer.deleteJobOutputReport(panda_id=self.job_id, attempt_nr=self.attempt_nr)

            del self.data
            del self.report_dict
        except Exception as e:
            err_str = f"{str(e)} {traceback.format_exc()}"
            self.logger.error(err_str)
            duration = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - start_time
            self.logger.error("except: took %s.%03d sec in total" % (duration.seconds, duration.microseconds / 1000))
            # unlock job output report
            self.taskBuffer.unlockJobOutputReport(
                panda_id=self.job_id,
                attempt_nr=self.attempt_nr,
                pid=self.pid,
                lock_offset=self.lock_offset,
            )

    # dump file report
    def dump_file_report(self, file_catalog, attempt_nr):
        """
        Dump the file report.

        :param file_catalog: The file catalog.
        :param attempt_nr: The attempt number.
        """
        self.logger.debug("dump file report")

        attempt_nr = 0 if attempt_nr is None else attempt_nr
        if self.job is None:
            self.job = self.taskBuffer.peekJobs([self.job_id], fromDefined=False, fromWaiting=False, forAnal=True)[0]
        if self.job:
            self.taskBuffer.insertJobOutputReport(
                panda_id=self.job_id,
                prod_source_label=self.job.prodSourceLabel,
                job_status=self.job_status,
                attempt_nr=attempt_nr,
                data=file_catalog,
            )

    # get plugin class
    def get_plugin_class(self, tmp_vo, tmp_group):
        """
        Get the plugin class for the given VO and group.

        This method attempts to retrieve a specific plugin class based on the provided
        VO (Virtual Organization) and group. If no specific plugin is found, it defaults
        to using the ATLAS plugin.

        :param tmp_vo: The Virtual Organization identifier.
        :param tmp_group: The group identifier.
        :return: The plugin class.
        """
        # instantiate concrete plugin
        self.adder_plugin_class = panda_config.getPlugin("adder_plugins", tmp_vo, tmp_group)
        if self.adder_plugin_class is None:
            # use ATLAS plugin by default
            from pandaserver.dataservice.adder_atlas_plugin import AdderAtlasPlugin

            self.adder_plugin_class = AdderAtlasPlugin
        self.logger.debug(f"plugin name {self.adder_plugin_class.__name__}")
        return self.adder_plugin_class

    def get_report(self) -> None:
        """
        Get the job output report.
        """
        self.report_dict = self.taskBuffer.getJobOutputReport(panda_id=self.job_id, attempt_nr=self.attempt_nr)
        self.data = self.report_dict.get("data")

    def register_event_service_files(self) -> None:
        """
        Register Event Service (ES) files.
        """
        # instantiate concrete plugin
        self.adder_plugin_class = self.get_plugin_class(self.job.VO, self.job.cloud)
        self.adder_plugin = self.adder_plugin_class(
            self.job,
            taskBuffer=self.taskBuffer,
            siteMapper=self.siteMapper,
            logger=self.logger,
        )
        self.logger.debug("plugin is ready for ES file registration")
        self.adder_plugin.register_event_service_files()

    def check_file_status_in_jedi(self) -> None:
        """
        Check the file status in JEDI.
        """
        if not self.job.isCancelled() and self.job.taskBufferErrorCode not in [pandaserver.taskbuffer.ErrorCode.EC_PilotRetried]:
            file_check_in_jedi = self.taskBuffer.checkInputFileStatusInJEDI(self.job)
            self.logger.debug(f"check file status in JEDI : {file_check_in_jedi}")
            if file_check_in_jedi is None:
                raise RuntimeError("failed to check file status in JEDI")
            if file_check_in_jedi is False:
                # set job status to failed since some file status is wrong in JEDI
                self.job_status = "failed"
                self.job.ddmErrorCode = pandaserver.dataservice.ErrorCode.EC_Adder
                err_str = "inconsistent file status between Panda and JEDI. "
                err_str += "failed to avoid duplicated processing caused by synchronization failure"
                self.job.ddmErrorDiag = err_str
                self.logger.debug(f"set jobStatus={self.job_status} since input is inconsistent between Panda and JEDI")
            elif self.job.jobSubStatus in ["pilot_closed"]:
                # terminated by the pilot
                self.logger.debug("going to closed since terminated by the pilot")
                ret_closed = self.taskBuffer.killJobs([self.job_id], "pilot", "60", True)
                if ret_closed[0] is True:
                    self.logger.debug("end")
                    # remove Catalog
                    self.taskBuffer.deleteJobOutputReport(panda_id=self.job_id, attempt_nr=self.attempt_nr)
                    return
            # check for cloned jobs
            if EventServiceUtils.isJobCloningJob(self.job) and self.job_status == "finished":
                # get semaphore for storeonce
                if EventServiceUtils.getJobCloningType(self.job) == "storeonce":
                    self.taskBuffer.getEventRanges(self.job.PandaID, self.job.jobsetID, self.job.jediTaskID, 1, False, False, None)
                # check semaphore
                check_jc = self.taskBuffer.checkClonedJob(self.job)
                if check_jc is None:
                    raise RuntimeError("failed to check the cloned job")
                # failed to lock semaphore
                if check_jc["lock"] is False:
                    self.job_status = "failed"
                    self.job.ddmErrorCode = pandaserver.dataservice.ErrorCode.EC_Adder
                    self.job.ddmErrorDiag = "failed to lock semaphore for job cloning"
                    self.logger.debug(f"set jobStatus={self.job_status} since did not get semaphore for job cloning")

    def execute_plugin(self) -> None:
        """
        Execute the Adder plugin.
        """
        # interaction with DDM
        try:
            # instantiate concrete plugin
            self.adder_plugin_class = self.get_plugin_class(self.job.VO, self.job.cloud)
            self.adder_plugin = self.adder_plugin_class(
                self.job,
                taskBuffer=self.taskBuffer,
                siteMapper=self.siteMapper,
                extra_info=self.extra_info,
                logger=self.logger,
            )
            self.logger.debug("plugin is ready")
            self.adder_plugin.execute()
            self.add_result = self.adder_plugin.result
            self.logger.debug(f"plugin done with {self.add_result.status_code}")
        except Exception:
            err_type, err_value = sys.exc_info()[:2]
            self.logger.error(f"failed to execute AdderPlugin for VO={self.job.VO} with {err_type}:{err_value}")
            self.logger.error(f"failed to execute AdderPlugin for VO={self.job.VO} with {traceback.format_exc()}")
            self.add_result = None
            self.job.ddmErrorCode = pandaserver.dataservice.ErrorCode.EC_Adder
            self.job.ddmErrorDiag = "AdderPlugin failure"

    def finalize_job_status(self) -> None:
        """
        Finalize the job status.
        """
        if self.job.jobStatus == "failed" or self.job_status == "failed":
            self.handle_failed_job()
        else:
            # reset errors
            self.job.jobDispatcherErrorCode = 0
            self.job.jobDispatcherErrorDiag = "NULL"
            # set status
            if self.add_result is not None and self.add_result.merging_files != []:
                # set status for merging:
                for file in self.job.Files:
                    if file.lfn in self.add_result.merging_files:
                        file.status = "merging"
                self.job.jobStatus = "merging"
                # propagate transition to prodDB
                self.job.stateChangeTime = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
            elif self.add_result is not None and self.add_result.transferring_files != []:
                # set status for transferring
                for file in self.job.Files:
                    if file.lfn in self.add_result.transferring_files:
                        file.status = "transferring"
                self.job.jobStatus = "transferring"
                self.job.jobSubStatus = None
                # propagate transition to prodDB
                self.job.stateChangeTime = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
            else:
                self.job.jobStatus = "finished"

    def handle_failed_job(self) -> None:
        """
        Handle failed job status.
        """
        # First of all: check if job failed and in this case take first actions according to error table
        source, error_code, error_diag = None, None, None
        errors = []
        if self.job.pilotErrorCode:
            source = "pilotErrorCode"
            error_code = self.job.pilotErrorCode
            error_diag = self.job.pilotErrorDiag
            errors.append(
                {
                    "source": source,
                    "error_code": error_code,
                    "error_diag": error_diag,
                }
            )
        if self.job.exeErrorCode:
            source = "exeErrorCode"
            error_code = self.job.exeErrorCode
            error_diag = self.job.exeErrorDiag
            errors.append(
                {
                    "source": source,
                    "error_code": error_code,
                    "error_diag": error_diag,
                }
            )
        if self.job.ddmErrorCode:
            source = "ddmErrorCode"
            error_code = self.job.ddmErrorCode
            error_diag = self.job.ddmErrorDiag
            errors.append(
                {
                    "source": source,
                    "error_code": error_code,
                    "error_diag": error_diag,
                }
            )
        if self.job.transExitCode:
            source = "transExitCode"
            error_code = self.job.transExitCode
            error_diag = ""
            errors.append(
                {
                    "source": source,
                    "error_code": error_code,
                    "error_diag": error_diag,
                }
            )

        if source and error_code:
            try:
                self.logger.debug("AdderGen.run will call apply_retrial_rules")
                retryModule.apply_retrial_rules(
                    self.taskBuffer,
                    self.job.PandaID,
                    errors,
                    self.job.attemptNr,
                )
                self.logger.debug("apply_retrial_rules is back")
            except Exception as e:
                self.logger.error(f"apply_retrial_rules excepted and needs to be investigated ({e}): {traceback.format_exc()}")

        self.job.jobStatus = "failed"
        for file in self.job.Files:
            if file.type in ["output", "log"]:
                if self.add_result is not None and file.lfn in self.add_result.merging_files:
                    file.status = "merging"
                else:
                    file.status = "failed"

    def check_job_status(self) -> None:
        """
        Check the job status and log appropriate messages.
        """
        if self.job is None:
            self.logger.debug("job not found in DB")
            return
        if self.job.jobStatus in ["finished", "failed", "unknown", "merging"]:
            self.logger.error(f"invalid state -> {self.job.jobStatus}")
            return
        if self.attempt_nr is not None and self.job.attemptNr != self.attempt_nr:
            self.logger.error(f"wrong attemptNr -> job={self.job.attemptNr} <> {self.attempt_nr}")
            return
        if self.job_status == EventServiceUtils.esRegStatus:
            self.register_event_service_files()
            return

        # check file status in JEDI
        self.check_file_status_in_jedi()

        # use failed for cancelled/closed jobs
        if self.job.isCancelled():
            self.job_status = "failed"
            # reset error codes to skip retrial module
            self.job.pilotErrorCode = 0
            self.job.exeErrorCode = 0
            self.job.ddmErrorCode = 0

        # keep old status
        old_job_status = self.job.jobStatus

        # set job status
        if self.job.jobStatus not in ["transferring"]:
            self.job.jobStatus = self.job_status
        self.add_result = None
        self.adder_plugin = None

        # parse Job Output Report JSON
        parse_result = self.parse_job_output_report()

        # If the parse_result is less than 2, it means that the parsing either succeeded or encountered a harmless error
        if parse_result < 2:
            self.execute_plugin()

            # ignore temporary errors
            if self.ignore_tmp_error and self.add_result is not None and self.add_result.is_temporary():
                self.logger.debug(f"ignore {self.job.ddmErrorDiag} ")
                self.logger.debug("escape")
                # unlock job output report
                self.taskBuffer.unlockJobOutputReport(
                    panda_id=self.job_id,
                    attempt_nr=self.attempt_nr,
                    pid=self.pid,
                    lock_offset=self.lock_offset,
                )
                return
            # failed
            if self.add_result is None or not self.add_result.is_succeeded():
                self.job.jobStatus = "failed"

        # set file status for failed jobs or failed transferring jobs
        self.logger.debug(f"status after plugin call :job.jobStatus={self.job.jobStatus} jobStatus={self.job_status}")

        self.finalize_job_status()

        # endtime
        if self.job.endTime == "NULL":
            self.job.endTime = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
        # output size and # of outputs
        self.job.nOutputDataFiles = 0
        self.job.outputFileBytes = 0
        for tmp_file in self.job.Files:
            if tmp_file.type == "output":
                self.job.nOutputDataFiles += 1
                try:
                    self.job.outputFileBytes += tmp_file.fsize
                except Exception:
                    pass
        # protection
        max_output_file_bytes = 99999999999
        self.job.outputFileBytes = min(self.job.outputFileBytes, max_output_file_bytes)
        # set cancelled state
        if self.job.commandToPilot == "tobekilled" and self.job.jobStatus == "failed":
            self.job.jobStatus = "cancelled"
        # update job
        if old_job_status in ["cancelled", "closed"]:
            pass
        else:
            self.logger.debug("updating DB")
            update_result = self.taskBuffer.updateJobs(
                [self.job],
                False,
                oldJobStatusList=[old_job_status],
                extraInfo=self.extra_info,
                async_dataset_update=True,
            )
            self.logger.debug(f"retU: {update_result}")

            # failed
            if not update_result[0]:
                self.logger.error(f"failed to update DB for pandaid={self.job.PandaID}")
                # unlock job output report
                self.taskBuffer.unlockJobOutputReport(
                    panda_id=self.job_id,
                    attempt_nr=self.attempt_nr,
                    pid=self.pid,
                    lock_offset=self.lock_offset,
                )
                return

            try:
                # updateJobs was successful and it failed a job with taskBufferErrorCode
                self.logger.debug("AdderGen.run will peek the job")
                job_tmp = self.taskBuffer.peekJobs(
                    [self.job.PandaID],
                    fromDefined=False,
                    fromArchived=True,
                    fromWaiting=False,
                )[0]
                self.logger.debug(
                    f"status {job_tmp.jobStatus}, taskBufferErrorCode {job_tmp.taskBufferErrorCode}, taskBufferErrorDiag {job_tmp.taskBufferErrorDiag}"
                )
                if job_tmp.jobStatus == "failed" and job_tmp.taskBufferErrorCode:
                    source = "taskBufferErrorCode"
                    error_code = job_tmp.taskBufferErrorCode
                    error_diag = job_tmp.taskBufferErrorDiag
                    errors = [
                        {
                            "source": source,
                            "error_code": error_code,
                            "error_diag": error_diag,
                        }
                    ]
                    self.logger.debug("AdderGen.run 2 will call apply_retrial_rules")
                    retryModule.apply_retrial_rules(
                        self.taskBuffer,
                        job_tmp.PandaID,
                        errors,
                        job_tmp.attemptNr,
                    )
                    self.logger.debug("apply_retrial_rules 2 is back")
            except IndexError:
                pass
            except Exception as e:
                self.logger.error(f"apply_retrial_rules 2 excepted and needs to be investigated ({e}): {traceback.format_exc()}")

            self.setup_closer()

    def setup_closer(self) -> None:
        """
        Setup closer for the job.
        """
        # setup for closer
        if not (EventServiceUtils.isEventServiceJob(self.job) and self.job.isCancelled()):
            destination_dispatch_block_list = []
            guid_list = []
            for file in self.job.Files:
                # ignore inputs
                if file.type == "input":
                    continue
                # skip pseudo datasets
                if file.destinationDBlock in ["", None, "NULL"]:
                    continue
                # start closer for output/log datasets
                if file.destinationDBlock not in destination_dispatch_block_list:
                    destination_dispatch_block_list.append(file.destinationDBlock)
                # collect GUIDs
                if (
                    self.job.prodSourceLabel == "panda"
                    or (
                        self.job.prodSourceLabel in ["rucio_test"] + JobUtils.list_ptest_prod_sources
                        and self.job.processingType
                        in [
                            "pathena",
                            "prun",
                            "gangarobot-rctest",
                            "hammercloud",
                        ]
                    )
                ) and file.type == "output":
                    # extract base LFN since LFN was changed to full LFN for CMS
                    base_lfn = file.lfn.split("/")[-1]
                    guid_list.append(
                        {
                            "lfn": base_lfn,
                            "guid": file.GUID,
                            "type": file.type,
                            "checksum": file.checksum,
                            "md5sum": file.md5sum,
                            "fsize": file.fsize,
                            "scope": file.scope,
                        }
                    )
            if guid_list:
                self.taskBuffer.setGUIDs(guid_list)
            if destination_dispatch_block_list:
                # start Closer
                if self.adder_plugin is not None and hasattr(self.adder_plugin, "dataset_map") and self.adder_plugin.dataset_map != {}:
                    closer_thread = closer.Closer(
                        self.taskBuffer,
                        destination_dispatch_block_list,
                        self.job,
                        dataset_map=self.adder_plugin.dataset_map,
                    )
                else:
                    closer_thread = closer.Closer(self.taskBuffer, destination_dispatch_block_list, self.job)
                self.logger.debug("start Closer")
                closer_thread.run()
                del closer_thread
                self.logger.debug("end Closer")
            # run closer for associate parallel jobs
            if EventServiceUtils.isJobCloningJob(self.job):
                associate_dispatch_block_map = self.taskBuffer.getDestDBlocksWithSingleConsumer(
                    self.job.jediTaskID, self.job.PandaID, destination_dispatch_block_list
                )
                for associate_job_id in associate_dispatch_block_map:
                    associate_dispatch_blocks = associate_dispatch_block_map[associate_job_id]
                    associate_job = self.taskBuffer.peekJobs(
                        [associate_job_id],
                        fromDefined=False,
                        fromArchived=False,
                        fromWaiting=False,
                        forAnal=True,
                    )[0]
                    if self.job is None:
                        self.logger.debug(f"associated job PandaID={associate_job_id} not found in DB")
                    else:
                        closer_thread = closer.Closer(self.taskBuffer, associate_dispatch_blocks, associate_job)
                        self.logger.debug(f"start Closer for PandaID={associate_job_id}")
                        closer_thread.run()
                        del closer_thread
                        self.logger.debug(f"end Closer for PandaID={associate_job_id}")

    # parse JSON
    # 0: succeeded, 1: harmless error to exit, 2: fatal error, 3: event service
    def parse_job_output_report(self):
        """
        Parse the JSON data associated with the job to extract file information.

        This method processes the JSON data to retrieve Logical File Names (LFNs),
        Globally Unique Identifiers (GUIDs), file sizes, checksums, and other metadata.
        It updates the job's file information and ensures consistency between the JSON
        data and the job's file records.

        :return: An integer indicating the result of the parsing process.
                 0 - succeeded
                 1 - harmless error to exit
                 2 - fatal error
                 3 - event service
        """
        # get LFN and GUID
        log_out = [f for f in self.job.Files if f.type in ["log", "output"]]

        # no outputs
        if not log_out:
            self.logger.debug("has no outputs")
            self.logger.debug("parse_job_output_report end")
            return 0

        # get input files
        input_lfns = [file.lfn for file in self.job.Files if file.type == "input"]

        # parse JSON
        lfns = []
        guids = []
        fsizes = []
        md5sums = []
        chksums = []
        surls = []
        full_lfn_map = {}
        n_events_map = {}
        guid_map = {}

        try:
            json_dict = json.loads(self.data)
            for lfn in json_dict:
                file_data = json_dict[lfn]
                lfn = str(lfn)
                fsize = None
                md5sum = None
                adler32 = None
                surl = None
                full_lfn = None
                guid = str(file_data["guid"])
                if "fsize" in file_data:
                    fsize = int(file_data["fsize"])
                if "md5sum" in file_data:
                    md5sum = str(file_data["md5sum"])
                    # check the md5sum is a 32-character hexadecimal number
                    if re.search("^[a-fA-F0-9]{32}$", md5sum) is None:
                        md5sum = None
                if "adler32" in file_data:
                    adler32 = str(file_data["adler32"])
                if "surl" in file_data:
                    surl = str(file_data["surl"])
                if "full_lfn" in file_data:
                    full_lfn = str(file_data["full_lfn"])
                # endpoints
                self.extra_info["endpoint"][lfn] = []
                if "endpoint" in file_data:
                    self.extra_info["endpoint"][lfn] = [file_data["endpoint"]]
                # error check
                if (lfn not in input_lfns) and (fsize is None or (md5sum is None and adler32 is None)):
                    if not EventServiceUtils.isEventServiceMerge(self.job):
                        raise RuntimeError("fsize/md5sum/adler32/surl=None")
                # append
                lfns.append(lfn)
                guids.append(guid)
                fsizes.append(fsize)
                md5sums.append(md5sum)
                surls.append(surl)
                if adler32 is not None:
                    # use adler32 if available
                    chksums.append(f"ad:{adler32}")
                else:
                    chksums.append(f"md5:{md5sum}")
                if full_lfn is not None:
                    full_lfn_map[lfn] = full_lfn
        except Exception:
            exc_type, value, _ = sys.exc_info()
            self.logger.warning("Issue with parsing JSON")
            self.logger.error(f"{exc_type} {value}")
            # set failed anyway
            self.job.jobStatus = "failed"
            # JSON error happens when pilot got killed due to wall-time limit or failures in wrapper
            if (
                (self.job.pilotErrorCode in [0, "0", "NULL"])
                and (self.job.taskBufferErrorCode not in [pandaserver.taskbuffer.ErrorCode.EC_WorkerDone])
                and (self.job.transExitCode in [0, "0", "NULL"])
            ):
                self.job.ddmErrorCode = pandaserver.dataservice.ErrorCode.EC_Adder
                self.job.ddmErrorDiag = "Could not get GUID/LFN/MD5/FSIZE/SURL from pilot JSON"
            return 2

        # parse metadata to get nEvents
        n_events_from = None
        # parse json
        try:
            json_dict = json.loads(self.job.metadata)
            for json_file_item in json_dict["files"]["output"]:
                for json_sub_file_item in json_file_item["subFiles"]:
                    lfn = str(json_sub_file_item["name"])
                    try:
                        n_events = int(json_sub_file_item["nentries"])
                        n_events_map[lfn] = n_events
                    except Exception:
                        pass
                    try:
                        guid = str(json_sub_file_item["file_guid"])
                        guid_map[lfn] = guid
                    except Exception:
                        pass
            n_events_from = "json"
        except Exception:
            self.logger.warning("Issue with parsing JSON for nEvents")
            pass

        # use nEvents and GUIDs reported by the pilot if no job report
        if self.job.metadata == "NULL" and self.job_status == "finished" and self.job.nEvents > 0 and self.job.prodSourceLabel in ["managed"]:
            for file in self.job.Files:
                if file.type == "output":
                    n_events_map[file.lfn] = self.job.nEvents
            for lfn, guid in zip(lfns, guids):
                guid_map[lfn] = guid
            n_events_from = "pilot"

        self.logger.debug(f"nEventsMap={str(n_events_map)}")
        self.logger.debug(f"nEventsFrom={str(n_events_from)}")
        self.logger.debug(f"guidMap={str(guid_map)}")
        self.logger.debug(f"self.job.jobStatus={self.job.jobStatus} in parse_job_output_report")
        self.logger.debug(f"isES={EventServiceUtils.isEventServiceJob(self.job)} isJumbo={EventServiceUtils.isJumboJob(self.job)}")

        # get lumi block number
        lumi_block_nr = self.job.getLumiBlockNr()

        # copy files for variable number of outputs
        tmp_stat = self.copy_files_for_variable_num_outputs(lfns)
        if not tmp_stat:
            err_msg = "failed to copy files for variable number of outputs"
            self.logger.error(err_msg)
            self.job.ddmErrorCode = pandaserver.dataservice.ErrorCode.EC_Adder
            self.job.ddmErrorDiag = err_msg
            self.job.jobStatus = "failed"
            return 2

        # check files
        lfns_set = set(lfns)
        file_list = []
        for file in self.job.Files:
            file_list.append(file.lfn)
            if file.type == "input":
                if file.lfn in lfns_set:
                    if self.job.prodSourceLabel in ["user", "panda"] or self.job.is_on_site_merging():
                        # skipped file
                        file.status = "skipped"
                        self.logger.debug(f"skipped input : {file.lfn}")
                    elif self.job.prodSourceLabel in ["managed", "test"] + JobUtils.list_ptest_prod_sources:
                        # failed by pilot
                        file.status = "failed"
                        self.logger.debug(f"failed input : {file.lfn}")
            elif file.type in {"output", "log"}:
                # add only log file for failed jobs
                if self.job_status == "failed" and file.type != "log":
                    file.status = "failed"
                    continue
                # set failed if it is missing in JSON
                if file.lfn not in lfns_set:
                    if (self.job.jobStatus == "finished" and EventServiceUtils.isEventServiceJob(self.job)) or EventServiceUtils.isJumboJob(self.job):
                        # unset file status for ES jobs
                        pass
                    elif file.isAllowedNoOutput():
                        # allowed not to be produced
                        file.status = "nooutput"
                        self.logger.debug(f"set {file.lfn} to status={file.status}")
                    else:
                        file.status = "failed"
                        self.job.jobStatus = "failed"
                        self.job.ddmErrorCode = pandaserver.dataservice.ErrorCode.EC_Adder
                        self.job.ddmErrorDiag = f"expected output {file.lfn} is missing in pilot JSON"
                        self.logger.error(self.job.ddmErrorDiag)
                    continue

                # look for GUID with LFN
                try:
                    i = lfns.index(file.lfn)
                    file.GUID = guids[i]
                    file.fsize = fsizes[i]
                    file.md5sum = md5sums[i]
                    file.checksum = chksums[i]
                    surl = surls[i]
                    # status
                    file.status = "ready"
                    # change to full LFN
                    if file.lfn in full_lfn_map:
                        file.lfn = full_lfn_map[file.lfn]
                    # add SURL to extraInfo
                    self.extra_info["surl"][file.lfn] = surl
                    # add nevents
                    if file.lfn in n_events_map:
                        self.extra_info["nevents"][file.lfn] = n_events_map[file.lfn]
                except Exception:
                    # status
                    file.status = "failed"
                    exc_type, value, _ = sys.exc_info()
                    self.logger.error(f"{exc_type} {value}")

                # set lumi block number
                if lumi_block_nr is not None and file.status != "failed":
                    self.extra_info["lbnr"][file.lfn] = lumi_block_nr

        self.extra_info["guid"] = guid_map

        # check consistency between JSON and filesTable
        for lfn in lfns:
            if lfn not in file_list:
                self.logger.error(f"{lfn} is not found in filesTable")
                self.job.jobStatus = "failed"
                for tmp_file in self.job.Files:
                    tmp_file.status = "failed"
                self.job.ddmErrorCode = pandaserver.dataservice.ErrorCode.EC_Adder
                self.job.ddmErrorDiag = f"pilot produced {lfn} inconsistently with jobdef"
                return 2
        # return
        self.logger.debug("parse_job_output_report end")
        return 0

    # copy files for variable number of outputs
    def copy_files_for_variable_num_outputs(self, lfns):
        """
        Copy files for variable number of outputs.

        This method handles the copying of file records when there is a variable number of output files.
        It identifies new Logical File Names (LFNs) that correspond to the original output files and
        updates the job information accordingly.

        :param lfns: List of new Logical File Names (LFNs).
        :return: True if the operation is successful, False otherwise.
        """
        # get original output files
        original_output_files = {}
        for tmp_file in self.job.Files:
            if tmp_file.type in ["output", "log"]:
                original_output_files[tmp_file.lfn] = tmp_file
        # look for unknown files
        orig_to_new_map = {}
        for new_lfn in lfns:
            if new_lfn not in original_output_files:
                # look for corresponding original output
                for original_lfn in original_output_files:
                    # match LFNs that have a similar base name but may have additional suffixes
                    tmp_patt = r"^{0}\.*_\d+$".format(original_lfn)
                    # removing any prefix up to and including the first pipe character (|) from the original LFN
                    reg_patt = re.sub(r"^[^|]+\|", "", original_lfn)
                    if re.search(tmp_patt, new_lfn) or (original_lfn.startswith("regex|") and re.search(reg_patt, new_lfn)):
                        self.logger.debug(f"use new LFN {new_lfn} for {original_lfn}")
                        # collect new filenames
                        orig_to_new_map.setdefault(original_lfn, [])
                        orig_to_new_map[original_lfn].append(new_lfn)
                        break
        # copy file records
        for original_lfn, new_lfn in orig_to_new_map.items():
            tmp_stat = self.taskBuffer.copy_file_records(new_lfn, original_output_files[original_lfn])
            if not tmp_stat:
                return False
        # refresh job info
        if orig_to_new_map:
            self.job = self.taskBuffer.peekJobs([self.job_id], fromDefined=False, fromWaiting=False, forAnal=True)[0]
        # return
        return True
