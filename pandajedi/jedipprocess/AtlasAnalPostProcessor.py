import datetime
import random
import re
import sys
import time
import traceback
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from pandacommon.pandautils.PandaUtils import naive_utcnow

from pandajedi.jedirefine import RefinerUtils
from pandaserver.taskbuffer import EventServiceUtils

from .MailTemplates import html_head, jedi_task_html_body, jedi_task_plain
from .PostProcessorBase import PostProcessorBase


def format_weight(weight):
    power = 1000
    n = 0
    power_labels = {0: "gCO2", 1: "kgCO2", 2: "tCO2", 3: "MtCO2", 4: "GtCO2"}
    while weight > power:
        weight /= power
        n += 1

    weight_str = f"{weight:.2f} {power_labels[n]}"
    return weight_str


# post processor for ATLAS production
class AtlasAnalPostProcessor(PostProcessorBase):
    # constructor
    def __init__(self, taskBufferIF, ddmIF):
        PostProcessorBase.__init__(self, taskBufferIF, ddmIF)
        self.taskParamMap = None

    # main
    def doPostProcess(self, taskSpec, tmp_logger):
        # freeze datasets
        try:
            # get DDM I/F
            ddmIF = self.ddmIF.getInterface(taskSpec.vo)
            # shuffle datasets
            random.shuffle(taskSpec.datasetSpecList)
            # loop over all datasets
            use_lib = False
            n_ok_lib = 0
            lock_update_time = naive_utcnow()
            for datasetSpec in taskSpec.datasetSpecList:
                # ignore template
                if datasetSpec.type.startswith("tmpl_"):
                    continue
                # only output, log or lib datasets
                if not datasetSpec.type.endswith("log") and not datasetSpec.type.endswith("output") and not datasetSpec.type == "lib":
                    continue
                # only user group, or panda dataset
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
                # remove wrong files
                if not closed_flag and datasetSpec.type in ["output"]:
                    # get successful files
                    ok_files = self.taskBufferIF.getSuccessfulFiles_JEDI(datasetSpec.jediTaskID, datasetSpec.datasetID)
                    if ok_files is None:
                        tmp_logger.warning(f"failed to get successful files for {datasetSpec.datasetName}")
                        return self.SC_FAILED
                    # get files in dataset
                    ddm_files = ddmIF.getFilesInDataset(datasetSpec.datasetName, skipDuplicate=False)
                    tmp_logger.debug(
                        f"datasetID={datasetSpec.datasetID}:Name={datasetSpec.datasetName} has {len(ok_files)} files in DB, {len(ddm_files)} files in DDM"
                    )
                    # check all files
                    to_delete = []
                    for tmpGUID, attMap in ddm_files.items():
                        if attMap["lfn"] not in ok_files:
                            did = {"scope": attMap["scope"], "name": attMap["lfn"]}
                            to_delete.append(did)
                            tmp_logger.debug(f"delete {attMap['lfn']} from {datasetSpec.datasetName}")
                    # delete
                    if to_delete:
                        ddmIF.deleteFilesFromDataset(datasetSpec.datasetName, to_delete)

                # freeze datasets
                if not closed_flag and not (datasetSpec.type.startswith("trn_") and datasetSpec.type not in ["trn_log"]):
                    tmp_logger.debug(f"freeze datasetID={datasetSpec.datasetID}:Name={datasetSpec.datasetName}")
                    ddmIF.freezeDataset(datasetSpec.datasetName, ignoreUnknown=True)
                else:
                    if datasetSpec.type.startswith("trn_") and datasetSpec.type not in ["trn_log"]:
                        tmp_logger.debug(f"skip freezing transient datasetID={datasetSpec.datasetID}:Name={datasetSpec.datasetName}")
                # update dataset
                datasetSpec.state = "closed"
                datasetSpec.stateCheckTime = naive_utcnow()

                # check if build step was succeeded
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
                # extend lifetime
                if datasetSpec.type in ["output"] and datasetSpec.datasetName.startswith("user"):
                    tmp_logger.debug(f"extend lifetime datasetID={datasetSpec.datasetID}:Name={datasetSpec.datasetName}")
                    ddmIF.updateReplicationRules(
                        datasetSpec.datasetName, {"type=.+": {"lifetime": 14 * 24 * 60 * 60}, "(SCRATCH|USER)DISK": {"lifetime": 14 * 24 * 60 * 60}}
                    )
                # update dataset in DB
                self.taskBufferIF.updateDatasetAttributes_JEDI(
                    datasetSpec.jediTaskID, datasetSpec.datasetID, {"state": datasetSpec.state, "stateCheckTime": datasetSpec.stateCheckTime}
                )
                # update task lock
                if naive_utcnow() - lock_update_time > datetime.timedelta(minutes=5):
                    lock_update_time = naive_utcnow()
                    # update lock
                    self.taskBufferIF.updateTaskLock_JEDI(taskSpec.jediTaskID)
            # dialog
            if use_lib and n_ok_lib == 0:
                taskSpec.setErrDiag("No build jobs succeeded", True)
        except Exception:
            err_type, err_value = sys.exc_info()[:2]
            tmp_logger.warning(f"failed to freeze datasets with {err_type.__name__}:{err_value}")
        ret_val = self.SC_SUCCEEDED
        try:
            self.doBasicPostProcess(taskSpec, tmp_logger)
        except Exception:
            err_type, err_value = sys.exc_info()[:2]
            tmp_logger.error(f"doBasicPostProcess failed with {err_type.__name__}:{err_value}")
            ret_val = self.SC_FATAL
        return ret_val

    # final procedure
    def doFinalProcedure(self, taskSpec, tmp_logger):
        # check email address
        to_add = self.getEmail(taskSpec.userName, taskSpec.vo, tmp_logger)

        # calculate carbon footprint for the task
        try:
            carbon_footprint = self.taskBufferIF.get_task_carbon_footprint(taskSpec.jediTaskID, level="global")
            carbon_footprint_redacted = {}
            zero = "0 gCO2"

            for job_status in ["finished", "failed", "cancelled", "total"]:
                if carbon_footprint and job_status in carbon_footprint:
                    carbon_footprint_redacted[job_status] = format_weight(carbon_footprint[job_status])
                else:
                    carbon_footprint_redacted[job_status] = zero
        except Exception:
            carbon_footprint_redacted = {}
            err_type, err_value = sys.exc_info()[:2]
            tmp_logger.error(f"failed to calculate task carbon footprint {err_type.__name__}:{err_value}")

        # read task parameters
        try:
            task_parameters = self.taskBufferIF.getTaskParamsWithID_JEDI(taskSpec.jediTaskID)
            self.taskParamMap = RefinerUtils.decodeJSON(task_parameters)
        except Exception:
            err_type, err_value = sys.exc_info()[:2]
            tmp_logger.error(f"task param conversion from json failed with {err_type.__name__}:{err_value}")
        if to_add is None or (self.taskParamMap is not None and "noEmail" in self.taskParamMap and self.taskParamMap["noEmail"] is True):
            tmp_logger.debug("email notification is suppressed")
        else:
            try:
                # send email notification
                from_add = self.senderAddress()
                html_text, plain_text, subject = self.compose_message(taskSpec, carbon_footprint_redacted)
                msg = MIMEMultipart("alternative")

                msg["Subject"] = subject
                msg["From"] = from_add
                msg["To"] = to_add

                # Record the MIME types of both parts - text/plain and text/html.
                part1 = MIMEText(plain_text, "plain")
                part2 = MIMEText(html_text, "html")

                # Attach parts into message container.
                # According to RFC 2046, the last part of a multipart message, in this case
                # the HTML message, is best and preferred.
                msg.attach(part1)
                msg.attach(part2)
                self.sendMail(taskSpec.jediTaskID, from_add, to_add, msg.as_string(), 3, False, tmp_logger)
            except Exception:
                tmp_logger.error(traceback.format_exc())
        return self.SC_SUCCEEDED

    # compose mail message
    def compose_message(self, taskSpec, carbon_footprint):
        # summary
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
                # dataset summary
                if datasetSpec.type == "log":
                    if datasetSpec.containerName not in log_datasets:
                        log_datasets.append(datasetSpec.containerName)
                elif datasetSpec.type == "input":
                    if datasetSpec.containerName not in input_datasets:
                        input_datasets.append(datasetSpec.containerName)
                elif datasetSpec.type == "output":
                    if datasetSpec.containerName not in output_datasets:
                        output_datasets.append(datasetSpec.containerName)
                # process summary
                if datasetSpec.isMasterInput():
                    if datasetSpec.status == "removed":
                        continue
                    try:
                        n_total_jobs += datasetSpec.nFiles
                        n_succeeded_jobs += datasetSpec.nFilesFinished
                        n_failed_jobs += datasetSpec.nFilesFailed
                    except Exception:
                        pass
        else:
            input_str = "Points"
            cancelled_str = "Unprocessed"
            n_total_jobs = taskSpec.get_total_num_jobs()
            event_stat = self.taskBufferIF.get_event_statistics(taskSpec.jediTaskID)
            if event_stat is not None:
                n_succeeded_jobs = event_stat.get(EventServiceUtils.ST_finished, 0)
                n_failed_jobs = event_stat.get(EventServiceUtils.ST_failed, 0)
        try:
            n_cancelled_jobs = n_total_jobs - n_succeeded_jobs - n_failed_jobs
        except Exception:
            pass
        if n_succeeded_jobs == n_total_jobs:
            msg_succeeded = "All Succeeded"
        else:
            msg_succeeded = "Succeeded"
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

        # CLI param
        if "cliParams" in self.taskParamMap:
            cli_parameters = self.taskParamMap["cliParams"]
        else:
            cli_parameters = None

        # make message
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

        # return
        return message_html, message_plain, subject

    # get email
    def getEmail(self, user_name, vo, tmp_logger):
        # return to suppress mail
        ret_suppressed = None
        # get DN
        tmp_logger.debug(f"getting email for {user_name}")

        # get email from PANDAMETA DB
        mail_address_db, dn, db_uptime = self.taskBufferIF.getEmailAddr(user_name, withDN=True)
        tmp_logger.debug(f"email from MetaDB : {mail_address_db}")
        # email notification is suppressed
        not_send_mail = False
        if mail_address_db is not None and mail_address_db.startswith("notsend"):
            not_send_mail = True
        # DN is unavailable
        if dn in ["", None]:
            # there will be no email
            tmp_logger.debug("DN is empty")
        else:
            # avoid too frequent lookup
            if db_uptime is not None and naive_utcnow() - db_uptime < datetime.timedelta(hours=1):
                tmp_logger.debug("no lookup")
                if not_send_mail or mail_address_db in [None, ""]:
                    return ret_suppressed
                else:
                    return mail_address_db.split(":")[-1]
            else:
                # get email from DQ2
                tmp_logger.debug(f"getting email using dq2Info.finger({dn})")
                n_tries = 3
                for iDDMTry in range(n_tries):
                    try:
                        user_info = self.ddmIF.getInterface(vo).finger(dn)
                        mail_address = user_info["email"]
                        tmp_logger.debug(f"email from DQ2 : {mail_address}")
                        if mail_address is None:
                            mail_address = ""
                        # make email field to update DB
                        mail_addr_to_db = ""
                        if not_send_mail:
                            mail_addr_to_db += "notsend:"
                        mail_addr_to_db += mail_address
                        # update database
                        tmp_logger.debug(f"update email to {mail_addr_to_db}")
                        self.taskBufferIF.setEmailAddr(user_name, mail_addr_to_db)

                        if not_send_mail or mail_address == "":
                            return ret_suppressed
                        return mail_address
                    except Exception:
                        if iDDMTry + 1 < n_tries:
                            tmp_logger.debug(f"sleep for retry {iDDMTry}/{n_tries}")
                            time.sleep(10)
                        else:
                            err_type, err_value = sys.exc_info()[:2]
                            tmp_logger.error(f"{err_type}:{err_value}")
        # not send email
        return ret_suppressed

    # remove tags
    def removeTags(self, tmp_str):
        try:
            if tmp_str is not None:
                tmp_str = re.sub(">[^<]+<", "><", tmp_str)
                tmp_str = re.sub("<[^<]+>", "", tmp_str)
        except Exception:
            pass
        return tmp_str
