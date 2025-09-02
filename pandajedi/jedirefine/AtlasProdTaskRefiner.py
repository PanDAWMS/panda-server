import datetime
import json
import random
import re
import traceback

from pandacommon.pandautils.PandaUtils import naive_utcnow

from pandajedi.jedicore import JediException
from pandaserver.dataservice import DataServiceUtils

from .TaskRefinerBase import TaskRefinerBase


# brokerage for ATLAS production
class AtlasProdTaskRefiner(TaskRefinerBase):
    # constructor
    def __init__(self, taskBufferIF, ddmIF):
        TaskRefinerBase.__init__(self, taskBufferIF, ddmIF)

    # extract common parameters
    def extractCommon(self, jediTaskID, taskParamMap, workQueueMapper, splitRule):
        tmpLog = self.tmpLog
        # set ddmBackEnd
        if "ddmBackEnd" not in taskParamMap:
            taskParamMap["ddmBackEnd"] = "rucio"
        # get number of unprocessed events for event service
        autoEsConversion = False
        if "esConvertible" in taskParamMap and taskParamMap["esConvertible"] is True:
            maxPrio = self.taskBufferIF.getConfigValue("taskrefiner", "AES_MAXTASKPRIORITY", "jedi", "atlas")
            minPrio = self.taskBufferIF.getConfigValue("taskrefiner", "AES_MINTASKPRIORITY", "jedi", "atlas")
            if maxPrio is not None and maxPrio < taskParamMap["taskPriority"]:
                pass
            elif minPrio is not None and minPrio > taskParamMap["taskPriority"]:
                pass
            else:
                # get threshold
                minNumEvents = self.taskBufferIF.getConfigValue("taskrefiner", "AES_EVENTPOOLSIZE", "jedi", "atlas")
                maxPending = self.taskBufferIF.getConfigValue("taskrefiner", "AES_MAXPENDING", "jedi", "atlas")
                nEvents, lastTaskTime, nPendingTasks = self.taskBufferIF.getNumUnprocessedEvents_JEDI(
                    taskParamMap["vo"], taskParamMap["prodSourceLabel"], {"eventService": 1}, {"gshare": "Validation"}
                )
                tmpStr = "check for ES "
                tmpStr += f"tot_num_unprocessed_events_AES={nEvents} target_num_events_AES={minNumEvents} last_AES_task_time={lastTaskTime} "
                tmpStr += f"num_pending_tasks_AES={nPendingTasks} max_pending_tasks_AES={maxPending} "
                tmpLog.info(tmpStr)
                # not chane many tasks at once
                if lastTaskTime is None or (lastTaskTime < naive_utcnow() - datetime.timedelta(minutes=5)):
                    if minNumEvents is not None and nEvents < minNumEvents and maxPending is not None and (maxPending is None or maxPending > nPendingTasks):
                        autoEsConversion = True
                        tmpLog.info("will be converted to AES unless it goes to pending")
        # add ES paramsters
        if ("esFraction" in taskParamMap and taskParamMap["esFraction"] > 0) or ("esConvertible" in taskParamMap and taskParamMap["esConvertible"] is True):
            tmpStr = "<PANDA_ES_ONLY>--eventService=True</PANDA_ES_ONLY>"
            taskParamMap["jobParameters"].append({"type": "constant", "value": tmpStr})
        if ("esFraction" in taskParamMap and taskParamMap["esFraction"] > 0) or autoEsConversion:
            if "nEventsPerWorker" not in taskParamMap and (("esFraction" in taskParamMap and taskParamMap["esFraction"] > random.random()) or autoEsConversion):
                taskParamMap["nEventsPerWorker"] = 1
                taskParamMap["registerEsFiles"] = True
                if "nEsConsumers" not in taskParamMap:
                    tmpVal = self.taskBufferIF.getConfigValue("taskrefiner", "AES_NESCONSUMERS", "jedi", "atlas")
                    if tmpVal is None:
                        tmpVal = 1
                    taskParamMap["nEsConsumers"] = tmpVal
                if "nSitesPerJob" not in taskParamMap:
                    tmpVal = self.taskBufferIF.getConfigValue("taskrefiner", "AES_NSITESPERJOB", "jedi", "atlas")
                    if tmpVal is not None:
                        taskParamMap["nSitesPerJob"] = tmpVal
                if "mergeEsOnOS" not in taskParamMap:
                    taskParamMap["mergeEsOnOS"] = True
                if "maxAttemptES" not in taskParamMap:
                    taskParamMap["maxAttemptES"] = 1
                if "maxAttemptEsJob" not in taskParamMap:
                    taskParamMap["maxAttemptEsJob"] = 0
                if "notDiscardEvents" not in taskParamMap:
                    taskParamMap["notDiscardEvents"] = True
                if "decAttOnFailedES" not in taskParamMap:
                    taskParamMap["decAttOnFailedES"] = True
                taskParamMap["coreCount"] = 0
                taskParamMap["resurrectConsumers"] = True
        # push status changes, choose N % of tasks to enable
        if "pushStatusChanges" not in taskParamMap:
            prod_pc_percent = self.taskBufferIF.getConfigValue("taskrefiner", "PROD_TASKS_PUSH_STATUS_CHANGES_PERCENT", "jedi", "atlas")
            if prod_pc_percent and random.uniform(0, 100) <= prod_pc_percent:
                taskParamMap["pushStatusChanges"] = True
        # serialize architecture
        if "architecture" in taskParamMap and isinstance(taskParamMap["architecture"], dict):
            # convert to string
            taskParamMap["architecture"] = json.dumps(taskParamMap["architecture"])
        # call base method
        TaskRefinerBase.extractCommon(self, jediTaskID, taskParamMap, workQueueMapper, splitRule)

    # main
    def doRefine(self, jediTaskID, taskParamMap):
        # make logger
        tmpLog = self.tmpLog
        tmpLog.debug(f"start taskType={self.taskSpec.taskType}")
        try:
            # basic refine
            self.doBasicRefine(taskParamMap)
            # set nosplit+repeat for DBR
            for datasetSpec in self.inSecDatasetSpecList:
                if DataServiceUtils.isDBR(datasetSpec.datasetName):
                    datasetSpec.attributes = "repeat,nosplit"
            # enable consistency check
            if self.taskSpec.parent_tid not in [None, self.taskSpec.jediTaskID]:
                for datasetSpec in self.inMasterDatasetSpec:
                    if datasetSpec.isMaster() and datasetSpec.type == "input":
                        datasetSpec.enableCheckConsistency()
            # append attempt number
            for tmpKey, tmpOutTemplateMapList in self.outputTemplateMap.items():
                for tmpOutTemplateMap in tmpOutTemplateMapList:
                    outFileTemplate = tmpOutTemplateMap["filenameTemplate"]
                    if re.search("\.\d+$", outFileTemplate) is None and not outFileTemplate.endswith(".panda.um"):
                        tmpOutTemplateMap["filenameTemplate"] = outFileTemplate + ".1"
            # extract input datatype
            datasetTypeListIn = []
            for datasetSpec in self.inMasterDatasetSpec + self.inSecDatasetSpecList:
                datasetType = DataServiceUtils.getDatasetType(datasetSpec.datasetName)
                if datasetType not in ["", None]:
                    datasetTypeListIn.append(datasetType)
            # extract datatype and set destination if necessary
            datasetTypeList = []
            for datasetSpec in self.outDatasetSpecList:
                datasetType = DataServiceUtils.getDatasetType(datasetSpec.datasetName)
                if datasetType not in ["", None]:
                    datasetTypeList.append(datasetType)
            # set numThrottled to use the task throttling mechanism
            if "noThrottle" not in taskParamMap:
                self.taskSpec.numThrottled = 0
            # set to register datasets
            self.taskSpec.setToRegisterDatasets()
            # set transient to parent datasets
            if self.taskSpec.processingType in ["merge"] and self.taskSpec.parent_tid not in [None, self.taskSpec.jediTaskID]:
                # get parent
                tmpStat, parentTaskSpec = self.taskBufferIF.getTaskDatasetsWithID_JEDI(self.taskSpec.parent_tid, None, False)
                if tmpStat and parentTaskSpec is not None:
                    # set transient to parent datasets
                    metaData = {"transient": True}
                    for datasetSpec in parentTaskSpec.datasetSpecList:
                        if datasetSpec.type in ["log", "output"]:
                            datasetType = DataServiceUtils.getDatasetType(datasetSpec.datasetName)
                            if datasetType not in ["", None] and datasetType in datasetTypeList and datasetType in datasetTypeListIn:
                                tmpLog.info(
                                    "set metadata={0} to parent jediTaskID={1}:datasetID={2}:Name={3}".format(
                                        str(metaData), self.taskSpec.parent_tid, datasetSpec.datasetID, datasetSpec.datasetName
                                    )
                                )
                                for metadataName, metadaValue in metaData.items():
                                    self.ddmIF.getInterface(self.taskSpec.vo).setDatasetMetadata(datasetSpec.datasetName, metadataName, metadaValue)
            # input prestaging
            if self.taskSpec.inputPreStaging():
                # set first contents feed flag
                self.taskSpec.set_first_contents_feed(True)
        except JediException.UnknownDatasetError as e:
            tmpLog.debug(f"in doRefine. {str(e)}")
            raise e
        except Exception as e:
            tmpLog.error(f"doRefine failed with {str(e)} {traceback.format_exc()}")
            raise e
        tmpLog.debug("done")
        return self.SC_SUCCEEDED
