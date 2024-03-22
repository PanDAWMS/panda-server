"""
add data to dataset

"""

import fcntl
import os
import re
import sys
import traceback

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandaserver.dataservice import dyn_data_distributer
from pandaserver.srvcore import CoreUtils
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
        # logger
        self.logger = LogWrapper(_logger)
        self.pd2p = dyn_data_distributer.DynDataDistributer([], self.site_mapper, token=" ")
        self.user_dataset_name = ""
        self.creation_time = ""
        self.params = ""
        self.locked_by = ""
        self.event_picking_file = None
        self.user_task_name = ""
        self.user_dn = ""
        # JEDI
        self.jedi_task_id = None

    # end with error
    def end_with_error(self, message: str):
        """
        Ends the event picker with an error.

        This method is called when an error occurs during the event picking process. It logs the error message,
        unlocks and closes the event picking file, and removes it if the error is not to be ignored. It then uploads
        the log and updates the task status in the task buffer.

        Parameters:
            message (str): The error message to be logged.
        """
        self.logger.error(message)
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
            out_log = self.upload_log()
            self.task_buffer.updateTaskErrorDialogJEDI(self.jedi_task_id, f"event picking failed. {out_log}")
            # update task
            if not self.ignore_error:
                self.task_buffer.updateTaskModTimeJEDI(self.jedi_task_id, "tobroken")
            self.logger.debug(out_log)
        self.logger.debug(f"end {self.event_picking_file_name}")

    # upload log
    def upload_log(self) -> str:
        """
        Uploads the log.

        This method uploads the log of the EventPicker. It first checks if the jediTaskID is not None.
        If it is None, it returns a message indicating that the jediTaskID could not be found.
        Otherwise, it dumps the logger content to a string and attempts to upload it using the Client's upload_log method.
        If the upload is not successful, it returns a message indicating the failure.
        If the upload is successful and the output starts with "http", it returns a hyperlink to the log.
        Otherwise, it returns the output of the upload_log method.

        Returns:
            str: The result of the log upload. This can be a message indicating an error, a hyperlink to the log, or the output of the upload_log method.
        """
        if self.jedi_task_id is None:
            return "cannot find jediTaskID"
        str_msg = self.logger.dumpToString()
        status, output = Client.uploadLog(str_msg, self.jedi_task_id)
        if status != 0:
            return f"failed to upload log with {status}."
        if output.startswith("http"):
            return f'<a href="{output}">log</a>'
        return output

    def get_options_from_file(self) -> dict:
        """
        Gets options from the event picking file.

        This method reads the event picking file and extracts options from it. The options are stored in a dictionary and returned.

        Returns:
            dict: A dictionary containing the options extracted from the event picking file.
        """
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
            "userName": "",
            "userTaskName": "",
            "userDatasetName": "",
            "lockedBy": "",
            "creationTime": "",
            "params": "",
        }

        for tmp_line in self.event_picking_file:
            # regular expression to parse lines where a key and value are separated by an equal sign
            tmp_match = re.search("^([^=]+)=(.+)$", tmp_line)
            if tmp_match is not None:
                key, value = tmp_match.groups()
                if key in options:
                    if key == "runEvent":
                        tmp_run_event = value.split(",")
                        if len(tmp_run_event) == 2:
                            options[key].append(tmp_run_event)
                    elif key in ["eventPickDS", "inputFileList", "tagDS"]:
                        options[key] = value.split(",")
                        if key == "inputFileList":
                            options[key] = [item for item in options[key] if item != ""]
                    elif key == "eventPickNumSites":
                        options[key] = int(value)
                    elif key == "runEvtGuidMap":
                        options[key] = eval(value)
                    else:
                        options[key] = value
                        if key == "tagStreamRef" and not options[key].endswith("_ref"):
                            options[key] += "_ref"
        return options

    def get_jedi_task_id(self, options: dict) -> int:
        """
        Gets the jediTaskID.

        This method gets the jediTaskID from the task buffer using the user's DN and task name.

        Parameters:
            options (dict): A dictionary containing the options extracted from the event picking file.

        Returns:
            int: The jediTaskID.
        """
        self.user_dn = options["userName"]
        self.user_task_name = options["userTaskName"]
        self.user_dataset_name = options["userDatasetName"]
        self.locked_by = options["lockedBy"]
        self.creation_time = options["creationTime"]
        self.params = options["params"]

        # extract task name
        if self.user_task_name == "" and self.params != "":
            # regular expression to parse the dataset name from a string where it is prefixed with --outDS and followed by either an equal sign or a space
            tmp_match = re.search("--outDS(=| ) *([^ ]+)", self.params)
            if tmp_match is not None:
                self.user_task_name = tmp_match.group(2)
                if not self.user_task_name.endswith("/"):
                    self.user_task_name += "/"

        compact_dn = self.task_buffer.cleanUserID(self.user_dn)
        return self.task_buffer.getTaskIDwithTaskNameJEDI(compact_dn, self.user_task_name)

    # main
    def run(self) -> bool:
        """
        Starts the event picker.

        Returns:
            bool: True if the event picker ran successfully, False otherwise.
        """
        try:
            self.logger.debug(f"start {self.event_picking_file_name}")
            # lock event picking file
            with open(self.event_picking_file_name) as self.event_picking_file:
                try:
                    fcntl.flock(self.event_picking_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                except Exception:
                    # release
                    self.logger.debug(f"cannot lock {self.event_picking_file_name}")
                    return True

                options = self.get_options_from_file()

                self.jedi_task_id = self.get_jedi_task_id(options)

                tmp_ret, location_map, all_files = self.pd2p.convert_evt_run_to_datasets(
                    options["runEvent"],
                    options["eventPickDataType"],
                    options["eventPickStreamName"],
                    options["eventPickDS"],
                    options["eventPickAmiTag"],
                    options["runEvtGuidMap"],
                )

                if not tmp_ret:
                    if "isFatal" in location_map and location_map["isFatal"] is True:
                        self.ignore_error = False
                    self.end_with_error("Failed to convert the run/event list to a dataset/file list")
                    return False

                # use only files in the list
                if options["inputFileList"]:
                    all_files = [tmp_file for tmp_file in all_files if tmp_file["lfn"] in options["inputFileList"]]

                # remove redundant CN from DN
                tmp_dn = CoreUtils.get_id_from_dn(self.user_dn)
                tmp_ret = self.pd2p.register_dataset_container_with_datasets(
                    self.user_dataset_name,
                    all_files,
                    location_map,
                    n_sites=options["eventPickNumSites"],
                    owner=tmp_dn,
                )

                if not tmp_ret:
                    return False

                fcntl.flock(self.event_picking_file.fileno(), fcntl.LOCK_UN)
                os.remove(self.event_picking_file_name)

                self.logger.debug(f"end {self.event_picking_file_name}")
                return True
        except Exception:
            error_type, error_value = sys.exc_info()[:2]
            self.end_with_error(f"Got exception {error_type}:{error_value} {traceback.format_exc()}")
            return False
