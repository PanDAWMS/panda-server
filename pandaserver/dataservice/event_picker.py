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
            # lock evp file
            self.event_picking_file = open(self.event_picking_file_name)
            try:
                fcntl.flock(self.event_picking_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except Exception:
                # relase
                self.putLog(f"cannot lock {self.event_picking_file_name}")
                self.event_picking_file.close()
                return True
            # options
            runEvtList = []
            eventPickDataType = ""
            eventPickStreamName = ""
            eventPickDS = []
            eventPickAmiTag = ""
            eventPickNumSites = 1
            inputFileList = []
            tagDsList = []
            tagQuery = ""
            tagStreamRef = ""
            skipDaTRI = False
            runEvtGuidMap = {}
            ei_api = ""
            # read evp file
            for tmpLine in self.event_picking_file:
                tmpMatch = re.search("^([^=]+)=(.+)$", tmpLine)
                # check format
                if tmpMatch is None:
                    continue
                tmpItems = tmpMatch.groups()
                if tmpItems[0] == "runEvent":
                    # get run and event number
                    tmpRunEvt = tmpItems[1].split(",")
                    if len(tmpRunEvt) == 2:
                        runEvtList.append(tmpRunEvt)
                elif tmpItems[0] == "eventPickDataType":
                    # data type
                    eventPickDataType = tmpItems[1]
                elif tmpItems[0] == "eventPickStreamName":
                    # stream name
                    eventPickStreamName = tmpItems[1]
                elif tmpItems[0] == "eventPickDS":
                    # dataset pattern
                    eventPickDS = tmpItems[1].split(",")
                elif tmpItems[0] == "eventPickAmiTag":
                    # AMI tag
                    eventPickAmiTag = tmpItems[1]
                elif tmpItems[0] == "eventPickNumSites":
                    # the number of sites where datasets are distributed
                    try:
                        eventPickNumSites = int(tmpItems[1])
                    except Exception:
                        pass
                elif tmpItems[0] == "userName":
                    # user name
                    self.userDN = tmpItems[1]
                    self.putLog(f"user={self.userDN}")
                elif tmpItems[0] == "userTaskName":
                    # user task name
                    self.user_task_name = tmpItems[1]
                elif tmpItems[0] == "userDatasetName":
                    # user dataset name
                    self.user_dataset_name = tmpItems[1]
                elif tmpItems[0] == "lockedBy":
                    # client name
                    self.locked_by = tmpItems[1]
                elif tmpItems[0] == "creationTime":
                    # creation time
                    self.creation_time = tmpItems[1]
                elif tmpItems[0] == "params":
                    # parameters
                    self.params = tmpItems[1]
                elif tmpItems[0] == "ei_api":
                    # ei api parameter for MC
                    ei_api = tmpItems[1]
                elif tmpItems[0] == "inputFileList":
                    # input file list
                    inputFileList = tmpItems[1].split(",")
                    try:
                        inputFileList.remove("")
                    except Exception:
                        pass
                elif tmpItems[0] == "tagDS":
                    # TAG dataset
                    tagDsList = tmpItems[1].split(",")
                elif tmpItems[0] == "tagQuery":
                    # query for TAG
                    tagQuery = tmpItems[1]
                elif tmpItems[0] == "tagStreamRef":
                    # StreamRef for TAG
                    tagStreamRef = tmpItems[1]
                    if not tagStreamRef.endswith("_ref"):
                        tagStreamRef += "_ref"
                elif tmpItems[0] == "runEvtGuidMap":
                    # GUIDs
                    try:
                        runEvtGuidMap = eval(tmpItems[1])
                    except Exception:
                        pass
            # extract task name
            if self.user_task_name == "" and self.params != "":
                try:
                    tmpMatch = re.search("--outDS(=| ) *([^ ]+)", self.params)
                    if tmpMatch is not None:
                        self.user_task_name = tmpMatch.group(2)
                        if not self.user_task_name.endswith("/"):
                            self.user_task_name += "/"
                except Exception:
                    pass
            # suppress DaTRI
            if self.params != "":
                if "--eventPickSkipDaTRI" in self.params:
                    skipDaTRI = True
            # get compact user name
            compactDN = self.task_buffer.cleanUserID(self.userDN)
            # get jediTaskID
            self.jedi_task_id = self.task_buffer.getTaskIDwithTaskNameJEDI(compactDN, self.user_task_name)
            # get prodSourceLabel
            (
                self.prod_source_label,
                self.job_label,
            ) = self.task_buffer.getProdSourceLabelwithTaskID(self.jedi_task_id)
            # convert run/event list to dataset/file list
            tmpRet, locationMap, allFiles = self.pd2p.convert_evt_run_to_datasets(
                runEvtList,
                eventPickDataType,
                eventPickStreamName,
                eventPickDS,
                eventPickAmiTag,
                self.userDN,
                runEvtGuidMap,
                ei_api,
            )
            if not tmpRet:
                if "isFatal" in locationMap and locationMap["isFatal"] is True:
                    self.ignore_error = False
                self.endWithError("Failed to convert the run/event list to a dataset/file list")
                return False
            # use only files in the list
            if inputFileList != []:
                tmpAllFiles = []
                for tmpFile in allFiles:
                    if tmpFile["lfn"] in inputFileList:
                        tmpAllFiles.append(tmpFile)
                allFiles = tmpAllFiles
            # remove redundant CN from DN
            tmpDN = CoreUtils.get_id_from_dn(self.userDN)
            # make dataset container
            tmpRet = self.pd2p.register_dataset_container_with_datasets(
                self.user_dataset_name,
                allFiles,
                locationMap,
                n_sites=eventPickNumSites,
                owner=tmpDN,
            )
            if not tmpRet:
                self.endWithError(f"Failed to make a dataset container {self.user_dataset_name}")
                return False
            # skip DaTRI
            if skipDaTRI:
                # successfully terminated
                self.putLog("skip DaTRI")
                # update task
                self.task_buffer.updateTaskModTimeJEDI(self.jedi_task_id)
            else:
                # get candidates
                tmpRet, candidateMaps = self.pd2p.get_candidates(
                    self.user_dataset_name,
                    self.prod_source_label,
                    self.job_label,
                    check_used_file=False,
                )
                if not tmpRet:
                    self.endWithError("Failed to find candidate for destination")
                    return False
                # collect all candidates
                allCandidates = []
                for tmpDS in candidateMaps:
                    tmpDsVal = candidateMaps[tmpDS]
                    for tmpCloud in tmpDsVal:
                        tmpCloudVal = tmpDsVal[tmpCloud]
                        for tmpSiteName in tmpCloudVal[0]:
                            if tmpSiteName not in allCandidates:
                                allCandidates.append(tmpSiteName)
                if allCandidates == []:
                    self.endWithError("No candidate for destination")
                    return False
                # get list of dataset (container) names
                if eventPickNumSites > 1:
                    # decompose container to transfer datasets separately
                    tmpRet, tmpOut = self.pd2p.get_list_dataset_replicas_in_container(self.user_dataset_name)
                    if not tmpRet:
                        self.endWithError(f"Failed to get replicas in {self.user_dataset_name}")
                        return False
                    userDatasetNameList = list(tmpOut)
                else:
                    # transfer container at once
                    userDatasetNameList = [self.user_dataset_name]
                # loop over all datasets
                sitesUsed = []
                for tmpUserDatasetName in userDatasetNameList:
                    # get size of dataset container
                    tmpRet, totalInputSize = rucioAPI.getDatasetSize(tmpUserDatasetName)
                    if not tmpRet:
                        self.endWithError(f"Failed to get the size of {tmpUserDatasetName} with {totalInputSize}")
                        return False
                    # run brokerage
                    tmpJob = JobSpec()
                    tmpJob.AtlasRelease = ""
                    self.putLog(f"run brokerage for {tmpDS}")
                    pandaserver.brokerage.broker.schedule(
                        [tmpJob],
                        self.task_buffer,
                        self.site_mapper,
                        True,
                        allCandidates,
                        True,
                        datasetSize=totalInputSize,
                    )
                    if tmpJob.computingSite.startswith("ERROR"):
                        self.endWithError(f"brokerage failed with {tmpJob.computingSite}")
                        return False
                    self.putLog(f"site -> {tmpJob.computingSite}")
                    # send transfer request
                    try:
                        tmpSiteSpec = self.site_mapper.getSite(tmpJob.computingSite)
                        scope_input, scope_output = select_scope(tmpSiteSpec, JobUtils.PROD_PS, JobUtils.PROD_PS)
                        tmpDQ2ID = tmpSiteSpec.ddm_output[scope_output]
                        tmpMsg = f"registerDatasetLocation for EventPicking  ds={tmpUserDatasetName} site={tmpDQ2ID} id={None}"
                        self.putLog(tmpMsg)
                        rucioAPI.registerDatasetLocation(
                            tmpDS,
                            [tmpDQ2ID],
                            lifetime=14,
                            owner=None,
                            activity="Analysis Output",
                        )
                        self.putLog("OK")
                    except Exception:
                        errType, errValue = sys.exc_info()[:2]
                        tmpStr = f"Failed to send transfer request : {errType} {errValue}"
                        tmpStr.strip()
                        tmpStr += traceback.format_exc()
                        self.endWithError(tmpStr)
                        return False
                    # list of sites already used
                    sitesUsed.append(tmpJob.computingSite)
                    self.putLog(f"used {len(sitesUsed)} sites")
                    # set candidates
                    if len(sitesUsed) >= eventPickNumSites:
                        # reset candidates to limit the number of sites
                        allCandidates = sitesUsed
                        sitesUsed = []
                    else:
                        # remove site
                        allCandidates.remove(tmpJob.computingSite)
                # send email notification for success
                tmpMsg = "A transfer request was successfully sent to Rucio.\n"
                tmpMsg += "Your task will get started once transfer is completed."

            try:
                # unlock and delete evp file
                fcntl.flock(self.event_picking_file.fileno(), fcntl.LOCK_UN)
                self.event_picking_file.close()
                os.remove(self.event_picking_file_name)
            except Exception:
                pass
            # successfully terminated
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
