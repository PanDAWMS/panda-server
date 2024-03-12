"""
add data to dataset

"""

import datetime
import fcntl
import os
import re
import sys
import traceback

import pandaserver.brokerage.broker
from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandaserver.dataservice import dyn_data_distributer
from pandaserver.dataservice.DataServiceUtils import select_scope
from pandaserver.dataservice.DDM import rucioAPI
from pandaserver.srvcore import CoreUtils
from pandaserver.taskbuffer import JobUtils
from pandaserver.taskbuffer.JobSpec import JobSpec
from pandaserver.userinterface import Client

# logger
_logger = PandaLogger().getLogger("event_picker")


class EventPicker:
    """
    A class used to add data to a dataset.
    """
    # constructor
    def __init__(self, taskBuffer, siteMapper, evpFileName: str, ignoreError: bool):
        """
        Constructs all the necessary attributes for the EventPicker object.

        Parameters:
            taskBuffer : TaskBuffer
                The task buffer that contains the jobs.
            siteMapper : SiteMapper
                The site mapper.
            evpFileName : str
                The name of the event picking file.
            ignoreError : bool
                Whether to ignore errors.
        """
        self.task_buffer = taskBuffer
        self.site_mapper = siteMapper
        self.ignore_error = ignoreError
        self.event_picking_file_name = evpFileName
        self.token = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None).isoformat(" ")
        # logger
        self.logger = LogWrapper(_logger, self.token)
        self.pd2p = dyn_data_distributer.DynDataDistributer([], self.site_mapper, token=" ")
        self.user_dataset_name = ""
        self.creation_time = ""
        self.params = ""
        self.locked_by = ""
        self.event_picking_file = None
        self.user_task_name = ""
        # JEDI
        self.jedi_task_id = None
        self.prod_source_label = None
        self.job_label = None

    # main
    def run(self) -> bool:
        """
        Starts the event picker.

        Returns:
            bool: True if the event picker ran successfully, False otherwise.
        """
        try:
            self.putLog(f"start {self.event_picking_file_name}")
            # lock event picking file
            self.event_picking_file = open(self.event_picking_file_name)
            try:
                fcntl.flock(self.event_picking_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except Exception:
                # release
                self.putLog(f"cannot lock {self.event_picking_file_name}")
                self.event_picking_file.close()
                return True

            options = {
                "runEvent": [],
                "eventPickDataType": "",
                "eventPickStreamName": "",
                "eventPickDS": [],
                "eventPickAmiTag": "",
                "eventPickNumSites": 1,
                "inputFileList": [],
                "tagDS": [],
                "tagQuery": "",
                "tagStreamRef": "",
                "runEvtGuidMap": {},
                "ei_api": "",
            }

            for tmpLine in self.event_picking_file:
                tmpMatch = re.search("^([^=]+)=(.+)$", tmpLine)
                if tmpMatch is not None:
                    key, value = tmpMatch.groups()
                    if key in options:
                        if key == "runEvent":
                            options[key].append(value.split(","))
                        elif key in ["eventPickDS", "inputFileList", "tagDS"]:
                            options[key] = value.split(",")
                        elif key == "eventPickNumSites":
                            options[key] = int(value)
                        else:
                            options[key] = value

            self.userDN = options["userName"]
            self.user_task_name = options["userTaskName"]
            self.user_dataset_name = options["userDatasetName"]
            self.locked_by = options["lockedBy"]
            self.creation_time = options["creationTime"]
            self.params = options["params"]

            # extract task name
            if self.user_task_name == "" and self.params != "":
                tmpMatch = re.search("--outDS(=| ) *([^ ]+)", self.params)
                if tmpMatch is not None:
                    self.user_task_name = tmpMatch.group(2)
                    if not self.user_task_name.endswith("/"):
                        self.user_task_name += "/"

            # suppress DaTRI
            if "--eventPickSkipDaTRI" in self.params:
                self.putLog("skip DaTRI")
                self.task_buffer.updateTaskModTimeJEDI(self.jedi_task_id)

            compactDN = self.task_buffer.cleanUserID(self.userDN)
            self.jedi_task_id = self.task_buffer.getTaskIDwithTaskNameJEDI(compactDN, self.user_task_name)
            self.prod_source_label, self.job_label = self.task_buffer.getProdSourceLabelwithTaskID(self.jedi_task_id)

            tmpRet, locationMap, allFiles = self.pd2p.convert_evt_run_to_datasets(
                options["runEvent"],
                options["eventPickDataType"],
                options["eventPickStreamName"],
                options["eventPickDS"],
                options["eventPickAmiTag"],
                self.userDN,
                options["runEvtGuidMap"],
                options["ei_api"],
            )

            if not tmpRet:
                if "isFatal" in locationMap and locationMap["isFatal"] is True:
                    self.ignore_error = False
                self.endWithError("Failed to convert the run/event list to a dataset/file list")
                return False

            # use only files in the list
            allFiles = [tmpFile for tmpFile in allFiles if tmpFile["lfn"] in options["inputFileList"]]

            # remove redundant CN from DN
            tmpDN = CoreUtils.get_id_from_dn(self.userDN)
            tmpRet = self.pd2p.register_dataset_container_with_datasets(
                self.user_dataset_name,
                allFiles,
                locationMap,
                n_sites=options["eventPickNumSites"],
                owner=tmpDN,
            )

            if not tmpRet:
                return False

                fcntl.flock(self.event_picking_file.fileno(), fcntl.LOCK_UN)
                self.event_picking_file.close()
                os.remove(self.event_picking_file_name)

                self.putLog(f"end {self.event_picking_file_name}")
                return True
            except Exception:
            errType, errValue = sys.exc_info()[:2]
            self.endWithError(f"Got exception {errType}:{errValue} {traceback.format_exc()}")
            return False

    # end with error
    def endWithError(self, message: str):
        """
        Ends the event picker with an error.

        This method is called when an error occurs during the event picking process. It logs the error message,
        unlocks and closes the event picking file, and removes it if the error is not to be ignored. It then uploads
        the log and updates the task status in the task buffer.

        Parameters:
            message (str): The error message to be logged.
        """
        self.putLog(message, "error")
        # unlock evp file
        try:
            fcntl.flock(self.event_picking_file.fileno(), fcntl.LOCK_UN)
            self.event_picking_file.close()
            if not self.ignore_error:
                # remove evp file
                os.remove(self.event_picking_file_name)
        except Exception:
            pass
        # upload log
        if self.jedi_task_id is not None:
            outLog = self.uploadLog()
            self.task_buffer.updateTaskErrorDialogJEDI(self.jedi_task_id, "event picking failed. " + outLog)
            # update task
            if not self.ignore_error:
                self.task_buffer.updateTaskModTimeJEDI(self.jedi_task_id, "tobroken")
            self.putLog(outLog)
        self.putLog(f"end {self.event_picking_file_name}")

    # put log
    def putLog(self, msg: str, type: str = "debug"):
        """
        Logs a message with a specified type.

        This method logs a message with a specified type. The type can be either "debug" or "error".
        If the type is "error", the message is logged as an error. Otherwise, it is logged as a debug message.

        Parameters:
            msg (str): The message to be logged.
            type (str): The type of the log. It can be either "debug" or "error". Default is "debug".

        Returns:
            None
        """
        tmpMsg = msg
        if type == "error":
            self.logger.error(tmpMsg)
        else:
            self.logger.debug(tmpMsg)

    # upload log
    def uploadLog(self) -> str:
        """
        Uploads the log.

        This method uploads the log of the EventPicker. It first checks if the jediTaskID is not None.
        If it is None, it returns a message indicating that the jediTaskID could not be found.
        Otherwise, it dumps the logger content to a string and attempts to upload it using the Client's uploadLog method.
        If the upload is not successful, it returns a message indicating the failure.
        If the upload is successful and the output starts with "http", it returns a hyperlink to the log.
        Otherwise, it returns the output of the uploadLog method.

        Returns:
            str: The result of the log upload. This can be a message indicating an error, a hyperlink to the log, or the output of the uploadLog method.
        """
        if self.jedi_task_id is None:
            return "cannot find jediTaskID"
        strMsg = self.logger.dumpToString()
        s, o = Client.uploadLog(strMsg, self.jedi_task_id)
        if s != 0:
            return f"failed to upload log with {s}."
        if o.startswith("http"):
            return f'<a href="{o}">log</a>'
        return o
