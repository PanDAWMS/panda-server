"""
Post-processor implementation for ATLAS analysis tasks.

Handles dataset freezing, email notifications, and carbon-footprint reporting
for user analysis tasks.
"""

import datetime
import random
import re
import time
import traceback
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any

from pandacommon.pandautils.PandaUtils import naive_utcnow

from pandajedi.jedicore import Interaction
from pandajedi.jedicore.JediTaskBufferInterface import JediTaskBufferInterface
from pandajedi.jedicore.MsgWrapper import MsgWrapper
from pandajedi.jediddm.DDMInterface import DDMInterface
from pandajedi.jedirefine import RefinerUtils
from pandaserver.taskbuffer import EventServiceUtils
from pandaserver.taskbuffer.JediTaskSpec import JediTaskSpec

from .MailTemplates import html_head, jedi_task_html_body, jedi_task_plain
from .PostProcessorBase import PostProcessorBase


def format_weight(weight: float) -> str:
    """Convert a CO2 weight in grams to a human-readable string with the appropriate unit."""
    power = 1000
    n = 0
    power_labels = {0: "gCO2", 1: "kgCO2", 2: "tCO2", 3: "MtCO2", 4: "GtCO2"}
    while weight > power:
        weight /= power
        n += 1
    weight_str = f"{weight:.2f} {power_labels[n]}"
    return weight_str


class AtlasAnalPostProcessor(PostProcessorBase):
    """Post-processor for ATLAS analysis tasks."""

    def __init__(self, taskBufferIF: JediTaskBufferInterface, ddmIF: DDMInterface) -> None:
        PostProcessorBase.__init__(self, taskBufferIF, ddmIF)
        self.taskParamMap: dict[str, Any] | None = None
        self.user_container_lifetime = taskBufferIF.getConfigValue("user_output", "OUTPUT_CONTAINER_LIFETIME", "jedi")
        if not self.user_container_lifetime:
            self.user_container_lifetime = 14
        self.user_container_lifetime *= 24 * 60 * 60

    def doPostProcess(self, taskSpec: JediTaskSpec, tmp_logger: MsgWrapper) -> Interaction.StatusCode:
        """
        Run post-processing steps for a finished ATLAS analysis task.

        Steps performed in order:
        1. For each output/log/lib dataset: remove wrong files, freeze,
           delete transient/empty datasets, and extend replication-rule lifetimes.
        2. Set an error dialog if the build step produced no successful jobs.
        3. Call doBasicPostProcess for common bookkeeping.

        Returns SC_SUCCEEDED, SC_FAILED, or SC_FATAL.
        """
        # freeze datasets
        try:
            ddmIF = self.ddmIF.getInterface(taskSpec.vo)
            if ddmIF is None:
                # the same answer this file already gives when the DDM lookup below fails
                tmp_logger.error(f"no DDM interface for vo={taskSpec.vo}")
                return self.SC_FAILED

            # shuffle to avoid always processing in the same order under partial failures
            random.shuffle(taskSpec.datasetSpecList)

            use_lib = False
            n_ok_lib = 0
            lock_update_time = naive_utcnow()
            done_containers = set()

            for datasetSpec in taskSpec.datasetSpecList:
                # both columns are declared Optional because a freshly constructed spec has them
                # unset; one read back with the task always carries them, so a spec that does not
                # is reported and skipped rather than dereferenced below
                if datasetSpec.type is None or datasetSpec.datasetName is None:
                    tmp_logger.warning(f"skip datasetID={datasetSpec.datasetID} with type={datasetSpec.type} name={datasetSpec.datasetName}")
                    continue
                # ignore template datasets
                if datasetSpec.type.startswith("tmpl_"):
                    continue
                # only process output, log, or lib datasets
                if not datasetSpec.type.endswith("log") and not datasetSpec.type.endswith("output") and not datasetSpec.type == "lib":
                    continue
                # only user, group, or panda datasets
                if (
                    not datasetSpec.datasetName.startswith("user")
                    and not datasetSpec.datasetName.startswith("panda")
                    and not datasetSpec.datasetName.startswith("group")
                ):
                    continue

                # check if already closed
                dataset_attrs = self.taskBufferIF.getDatasetAttributes_JEDI(datasetSpec.jediTaskID, datasetSpec.datasetID, ["state"])
                if "state" in dataset_attrs and dataset_attrs["state"] == "closed":
                    tmp_logger.info(f"skip freezing closed datasetID={datasetSpec.datasetID}:Name={datasetSpec.datasetName}")
                    closed_flag = True
                else:
                    closed_flag = False

                # remove files from DDM that are not in the DB success list
                if not closed_flag and datasetSpec.type in ["output"]:
                    ok_files = self.taskBufferIF.getSuccessfulFiles_JEDI(datasetSpec.jediTaskID, datasetSpec.datasetID)
                    if ok_files is None:
                        tmp_logger.warning(f"failed to get successful files for {datasetSpec.datasetName}")
                        return self.SC_FAILED
                    ddm_files = ddmIF.getFilesInDataset(datasetSpec.datasetName, skipDuplicate=False)
                    tmp_logger.debug(
                        f"datasetID={datasetSpec.datasetID}:Name={datasetSpec.datasetName} has {len(ok_files)} files in DB, {len(ddm_files)} files in DDM"
                    )
                    to_delete = []
                    for tmpGUID, attMap in ddm_files.items():
                        if attMap["lfn"] not in ok_files:
                            did = {"scope": attMap["scope"], "name": attMap["lfn"]}
                            to_delete.append(did)
                            tmp_logger.debug(f"delete {attMap['lfn']} from {datasetSpec.datasetName}")
                    if to_delete:
                        ddmIF.deleteFilesFromDataset(datasetSpec.datasetName, to_delete)

                # freeze datasets, skipping transient ones (trn_*), except trn_log
                if not closed_flag and not (datasetSpec.type.startswith("trn_") and datasetSpec.type not in ["trn_log"]):
                    tmp_logger.debug(f"freeze datasetID={datasetSpec.datasetID}:Name={datasetSpec.datasetName}")
                    ddmIF.freezeDataset(datasetSpec.datasetName, ignoreUnknown=True)
                else:
                    if datasetSpec.type.startswith("trn_") and datasetSpec.type not in ["trn_log"]:
                        tmp_logger.debug(f"skip freezing transient datasetID={datasetSpec.datasetID}:Name={datasetSpec.datasetName}")

                # update dataset state
                datasetSpec.state = "closed"
                datasetSpec.stateCheckTime = naive_utcnow()

                # check if build step succeeded
                if datasetSpec.type == "lib":
                    use_lib = True
                else:
                    n_ok_lib += 1

                # delete transient or empty datasets
                if not closed_flag:
                    empty_only = True
                    if datasetSpec.type.startswith("trn_") and datasetSpec.type not in ["trn_log"]:
                        empty_only = False
                    retStr = ddmIF.deleteDataset(datasetSpec.datasetName, empty_only, ignoreUnknown=True)
                    tmp_logger.debug(retStr)

                # extend replication-rule lifetime for user output datasets
                if datasetSpec.type in ["output"] and datasetSpec.datasetName.startswith("user"):
                    tmp_logger.debug(f"extend lifetime datasetID={datasetSpec.datasetID}:Name={datasetSpec.datasetName}")
                    ddmIF.updateReplicationRules(
                        datasetSpec.datasetName, {"type=.+": {"lifetime": 14 * 24 * 60 * 60}, "(SCRATCH|USER)DISK": {"lifetime": 14 * 24 * 60 * 60}}
                    )
                    # extend container rules
                    if datasetSpec.containerName and datasetSpec.containerName not in done_containers:
                        tmp_logger.debug(f"extend lifetime container:Name={datasetSpec.containerName}")
                        ddmIF.updateReplicationRules(
                            datasetSpec.containerName,
                            {"type=.+": {"lifetime": self.user_container_lifetime}, "(SCRATCH|USER)DISK": {"lifetime": self.user_container_lifetime}},
                        )
                        done_containers.add(datasetSpec.containerName)

                # persist dataset state to DB
                self.taskBufferIF.updateDatasetAttributes_JEDI(
                    datasetSpec.jediTaskID, datasetSpec.datasetID, {"state": datasetSpec.state, "stateCheckTime": datasetSpec.stateCheckTime}
                )

                # refresh task lock every 5 minutes to avoid expiry during long loops
                if naive_utcnow() - lock_update_time > datetime.timedelta(minutes=5):
                    lock_update_time = naive_utcnow()
                    self.taskBufferIF.updateTaskLock_JEDI(taskSpec.jediTaskID)

            # set error dialog if the build job produced no output
            if use_lib and n_ok_lib == 0:
                taskSpec.setErrDiag("No build jobs succeeded", True)

        except Exception as e:
            tmp_logger.warning(f"failed to freeze datasets with {type(e).__name__}:{e}")

        ret_val = self.SC_SUCCEEDED
        try:
            self.doBasicPostProcess(taskSpec, tmp_logger)
        except Exception as e:
            tmp_logger.error(f"doBasicPostProcess failed with {type(e).__name__}:{e}")
            ret_val = self.SC_FATAL

        return ret_val

    def doFinalProcedure(self, taskSpec: JediTaskSpec, tmp_logger: MsgWrapper) -> Interaction.StatusCode:
        """
        Send an email notification to the task owner with a task summary.

        Resolves the user's email address (with a 1-hour cache), calculates the
        task carbon footprint, and sends a multipart HTML/plain-text message.
        Email is suppressed if the user has opted out or the taskParams contain
        ``noEmail: true``.

        Returns SC_SUCCEEDED.
        """
        # a notification needs all three, and none of them is optional on a task read from the DB
        if taskSpec.jediTaskID is None or taskSpec.userName is None or taskSpec.vo is None:
            tmp_logger.error(f"cannot notify for jediTaskID={taskSpec.jediTaskID} userName={taskSpec.userName} vo={taskSpec.vo}")
            return self.SC_FAILED

        # resolve the recipient email address
        to_add = self.getEmail(taskSpec.userName, taskSpec.vo, tmp_logger)

        # calculate carbon footprint. compose_message() indexes all four keys unconditionally,
        # so they are filled in before the lookup that can fail: leaving the map empty on an
        # exception turned a footprint failure into a KeyError that suppressed the whole
        # notification.
        zero = "0 gCO2"
        carbon_footprint_redacted = {job_status: zero for job_status in ["finished", "failed", "cancelled", "total"]}
        try:
            carbon_footprint = self.taskBufferIF.get_task_carbon_footprint(taskSpec.jediTaskID, level="global")
            for job_status in list(carbon_footprint_redacted):
                if carbon_footprint and job_status in carbon_footprint:
                    carbon_footprint_redacted[job_status] = format_weight(carbon_footprint[job_status])
        except Exception as e:
            tmp_logger.error(f"failed to calculate task carbon footprint {type(e).__name__}:{e}")

        # read task parameters
        try:
            task_parameters = self.taskBufferIF.getTaskParamsWithID_JEDI(taskSpec.jediTaskID)
            self.taskParamMap = RefinerUtils.decodeJSON(task_parameters)
        except Exception as e:
            tmp_logger.error(f"task param conversion from json failed with {type(e).__name__}:{e}")

        # send email notification unless suppressed
        if to_add is None or (self.taskParamMap is not None and "noEmail" in self.taskParamMap and self.taskParamMap["noEmail"] is True):
            tmp_logger.debug("email notification is suppressed")
        else:
            try:
                from_add = self.senderAddress()
                html_text, plain_text, subject = self.compose_message(taskSpec, carbon_footprint_redacted)

                msg = MIMEMultipart("alternative")
                msg["Subject"] = subject
                msg["From"] = from_add
                msg["To"] = to_add

                # attach plain text first, HTML last (RFC 2046: last part is preferred)
                part1 = MIMEText(plain_text, "plain")
                part2 = MIMEText(html_text, "html")
                msg.attach(part1)
                msg.attach(part2)

                self.sendMail(taskSpec.jediTaskID, from_add, to_add, msg.as_string(), 3, False, tmp_logger)
            except Exception:
                tmp_logger.error(traceback.format_exc())

        return self.SC_SUCCEEDED

    def compose_message(self, taskSpec: JediTaskSpec, carbon_footprint: dict[str, str]) -> tuple[str, str, str]:
        """
        Build HTML and plain-text email bodies summarising the task outcome.

        Returns a tuple of (html_message, plain_message, subject).
        """
        input_datasets = []
        output_datasets = []
        log_datasets = []
        n_total_jobs = 0
        n_succeeded_jobs = 0
        n_failed_jobs = 0
        n_cancelled_jobs = 0

        if not taskSpec.is_hpo_workflow():
            input_str = "Inputs"
            cancelled_str = "Cancelled  "

            for datasetSpec in taskSpec.datasetSpecList:
                # collect dataset names by type
                if datasetSpec.type == "log":
                    if datasetSpec.containerName not in log_datasets:
                        log_datasets.append(datasetSpec.containerName)
                elif datasetSpec.type == "input":
                    if datasetSpec.containerName not in input_datasets:
                        input_datasets.append(datasetSpec.containerName)
                elif datasetSpec.type == "output":
                    if datasetSpec.containerName not in output_datasets:
                        output_datasets.append(datasetSpec.containerName)

                # accumulate job counts from the master input dataset
                if datasetSpec.isMasterInput():
                    if datasetSpec.status == "removed":
                        continue
                    # a NULL counter is a dataset that was never counted and adds nothing;
                    # the bare try/except this replaces got the same result
                    n_total_jobs += datasetSpec.nFiles or 0
                    n_succeeded_jobs += datasetSpec.nFilesFinished or 0
                    n_failed_jobs += datasetSpec.nFilesFailed or 0
        else:
            input_str = "Points"
            cancelled_str = "Unprocessed"
            n_total_jobs = taskSpec.get_total_num_jobs() or 0
            event_stat = self.taskBufferIF.get_event_statistics(taskSpec.jediTaskID)
            if event_stat is not None:
                n_succeeded_jobs = event_stat.get(EventServiceUtils.ST_finished, 0)
                n_failed_jobs = event_stat.get(EventServiceUtils.ST_failed, 0)

        try:
            n_cancelled_jobs = n_total_jobs - n_succeeded_jobs - n_failed_jobs
        except Exception:
            pass

        msg_succeeded = "All Succeeded" if n_succeeded_jobs == n_total_jobs else "Succeeded"

        input_datasets.sort()
        output_datasets.sort()
        log_datasets.sort()
        dataset_summary = ""
        for tmpDS in input_datasets:
            dataset_summary += f"In  : {tmpDS}\n"
        for tmpDS in output_datasets:
            dataset_summary += f"Out : {tmpDS}\n"
        for tmpDS in log_datasets:
            dataset_summary += f"Log : {tmpDS}\n"
        dataset_summary = dataset_summary[:-1]

        # CLI parameters
        cli_parameters = self.taskParamMap.get("cliParams") if self.taskParamMap else None

        # build HTML message
        head = html_head.format(title="Task summary notification")
        body = jedi_task_html_body.format(
            jedi_task_id=taskSpec.jediTaskID,
            creation_time=taskSpec.creationDate,
            end_time=taskSpec.endTime,
            task_status=taskSpec.status,
            error_dialog=self.removeTags(taskSpec.errorDialog),
            command=cli_parameters,
            n_total=n_total_jobs,
            n_succeeded=n_succeeded_jobs,
            n_failed=n_failed_jobs,
            n_cancelled=n_cancelled_jobs,
            carbon_succeeded=carbon_footprint["finished"],
            carbon_failed=carbon_footprint["failed"],
            carbon_cancelled=carbon_footprint["cancelled"],
            carbon_total=carbon_footprint["total"],
            datasets_in=input_datasets,
            datasets_out=output_datasets,
            datasets_log=log_datasets,
            msg_succeeded=msg_succeeded,
            input_str=input_str,
            cancelled_str=cancelled_str,
        )
        message_html = head + body

        # build plain-text message
        message_plain = jedi_task_plain.format(
            jedi_task_id=taskSpec.jediTaskID,
            creation_time=taskSpec.creationDate,
            end_time=taskSpec.endTime,
            task_status=taskSpec.status,
            error_dialog=self.removeTags(taskSpec.errorDialog),
            command=cli_parameters,
            n_total=n_total_jobs,
            n_succeeded=n_succeeded_jobs,
            n_failed=n_failed_jobs,
            n_cancelled=n_cancelled_jobs,
            carbon_succeeded=carbon_footprint["finished"],
            carbon_failed=carbon_footprint["failed"],
            carbon_cancelled=carbon_footprint["cancelled"],
            carbon_total=carbon_footprint["total"],
            dataset_summary=dataset_summary,
            msg_succeeded=msg_succeeded,
            input_str=input_str,
            cancelled_str=cancelled_str,
        )

        subject = f"JEDI notification for TaskID:{taskSpec.jediTaskID} ({n_succeeded_jobs}/{n_total_jobs} {msg_succeeded})"

        return message_html, message_plain, subject

    def getEmail(self, user_name: str, vo: str, tmp_logger: MsgWrapper) -> str | None:
        """
        Resolve the email address for user_name, using a 1-hour DB cache.

        Returns the email address string, or None if the user has opted out
        or no address could be found.
        """
        ret_suppressed = None
        tmp_logger.debug(f"getting email for {user_name}")

        # check the metadata DB cache first
        mail_address_db, dn, db_uptime = self.taskBufferIF.getEmailAddr(user_name, withDN=True)
        tmp_logger.debug(f"email from MetaDB : {mail_address_db}")

        not_send_mail = mail_address_db is not None and mail_address_db.startswith("notsend")

        if dn in ("", None):
            tmp_logger.debug("DN is empty")
        else:
            # use the cached value if it is less than 1 hour old
            if db_uptime is not None and naive_utcnow() - db_uptime < datetime.timedelta(hours=1):
                tmp_logger.debug("no lookup")
                if not_send_mail or mail_address_db in (None, ""):
                    return ret_suppressed
                else:
                    return mail_address_db.split(":")[-1]
            else:
                # look up the address via DDM finger
                tmp_logger.debug(f"getting email using rucio.finger({dn})")
                n_tries = 3
                for iDDMTry in range(n_tries):
                    try:
                        ddm_interface = self.ddmIF.getInterface(vo)
                        if ddm_interface is None:
                            tmp_logger.error(f"no DDM interface for vo={vo}")
                            return ret_suppressed
                        user_info = ddm_interface.finger(dn)
                        mail_address = user_info["email"]
                        tmp_logger.debug(f"email from Rucio : {mail_address}")
                        if mail_address is None:
                            mail_address = ""

                        # build the DB entry (prefix with "notsend:" if opted out)
                        mail_addr_to_db = ""
                        if not_send_mail:
                            mail_addr_to_db += "notsend:"
                        mail_addr_to_db += mail_address

                        tmp_logger.debug(f"update email to {mail_addr_to_db}")
                        self.taskBufferIF.setEmailAddr(user_name, mail_addr_to_db)

                        if not_send_mail or mail_address == "":
                            return ret_suppressed
                        return mail_address
                    except Exception as e:
                        if iDDMTry + 1 < n_tries:
                            tmp_logger.debug(f"sleep for retry {iDDMTry}/{n_tries}")
                            time.sleep(10)
                        else:
                            tmp_logger.error(f"{type(e)}:{e}")

        return ret_suppressed

    def removeTags(self, tmp_str: str | None) -> str | None:
        """Strip HTML tags from tmp_str, returning the cleaned string."""
        try:
            if tmp_str is not None:
                tmp_str = re.sub(">[^<]+<", "><", tmp_str)
                tmp_str = re.sub("<[^<]+>", "", tmp_str)
        except Exception:
            pass
        return tmp_str
