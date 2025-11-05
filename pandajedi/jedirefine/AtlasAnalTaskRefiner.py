import json
import random
import re
import traceback

from pandaserver.config import panda_config
from pandaserver.dataservice import DataServiceUtils
from pandaserver.taskbuffer import JobUtils
from pandaserver.taskbuffer.JediTaskSpec import JediTaskSpec

from .TaskRefinerBase import TaskRefinerBase


# brokerage for ATLAS analysis
class AtlasAnalTaskRefiner(TaskRefinerBase):
    # constructor
    def __init__(self, taskBufferIF, ddmIF):
        TaskRefinerBase.__init__(self, taskBufferIF, ddmIF)

    # extract common parameters
    def extractCommon(self, jediTaskID, taskParamMap, workQueueMapper, splitRule):
        processingTypes = taskParamMap["processingType"].split("-")
        # set ddmBackEnd
        if "ddmBackEnd" not in taskParamMap:
            taskParamMap["ddmBackEnd"] = "rucio"
        # set sourceURL
        try:
            if "sourceURL" in taskParamMap:
                for tmpItem in taskParamMap["jobParameters"]:
                    if "value" in tmpItem:
                        tmpItem["value"] = re.sub("\$\{SURL\}", taskParamMap["sourceURL"], tmpItem["value"])
        except Exception:
            pass
        if hasattr(panda_config, "wn_script_base_url"):
            base_url = panda_config.wn_script_base_url
        else:
            protocol = "https" if panda_config.pserverportcache == 443 else "http"
            base_url = f"{protocol}://{panda_config.pserveralias}:{panda_config.pserverportcache}"
        # set transPath
        if "transPath" not in taskParamMap:
            if "athena" in processingTypes:
                # athena
                taskParamMap["transPath"] = f"{base_url}/trf/user/runAthena-00-00-12"
            elif "cont" in processingTypes:
                # container
                taskParamMap["transPath"] = f"{base_url}/trf/user/runcontainer"
            else:
                # general executable
                taskParamMap["transPath"] = f"{base_url}/trf/user/runGen-00-00-02"
                # shorter base walltime
                if "baseWalltime" not in taskParamMap:
                    taskParamMap["baseWalltime"] = 60
        # set transPath for build
        if "buildSpec" in taskParamMap and "transPath" not in taskParamMap["buildSpec"]:
            if "athena" in processingTypes:
                # athena
                taskParamMap["buildSpec"]["transPath"] = f"{base_url}/trf/user/buildJob-00-00-03"
            else:
                # general executable
                taskParamMap["buildSpec"]["transPath"] = f"{base_url}/trf/user/buildGen-00-00-01"
        # set transPath for preprocessing
        if "preproSpec" in taskParamMap and "transPath" not in taskParamMap["preproSpec"]:
            if "evp" in processingTypes:
                # event picking
                taskParamMap["preproSpec"]["transPath"] = f"{base_url}/trf/user/preEvtPick-00-00-01"
            elif "grl" in processingTypes:
                # good run list
                taskParamMap["preproSpec"]["transPath"] = f"{base_url}/trf/user/preGoodRunList-00-00-01"
        # set transPath for merge
        if "mergeSpec" in taskParamMap and "transPath" not in taskParamMap["mergeSpec"]:
            taskParamMap["mergeSpec"]["transPath"] = f"{base_url}/trf/user/runMerge-00-00-02"
        # min ram count
        if "ramCount" not in taskParamMap:
            taskParamMap["ramCount"] = 2000
            taskParamMap["ramUnit"] = "MBPerCore"
        # disk count
        if "outDiskCount" not in taskParamMap:
            out_disk_count_default = self.taskBufferIF.getConfigValue("taskrefiner", "OUTDISKCOUNT_ANALY_KB", "jedi", "atlas")
            if out_disk_count_default is None or out_disk_count_default < 0:
                out_disk_count_default = 500
            taskParamMap["outDiskCount"] = out_disk_count_default
            taskParamMap["outDiskUnit"] = "kB"
        # set cpu time unit to use HS06
        if "cpuTimeUnit" not in taskParamMap:
            taskParamMap["cpuTimeUnit"] = "HS06sPerEvent"
        # use local IO for ancient releases since inputfilepeeker+xrootd is problematic
        if "transUses" in taskParamMap and taskParamMap["transUses"]:
            try:
                ver = taskParamMap["transUses"].split("-")[1]
                m = re.search("^(\d{2})\.(\d{2})\.", ver)
                if m is not None:
                    major = int(m.group(1))
                    minor = int(m.group(2))
                    if major < 20 or (major == 20 and minor <= 20):
                        taskParamMap["useLocalIO"] = 1
            except Exception:
                pass
        # scout success rate
        if "scoutSuccessRate" not in taskParamMap:
            taskParamMap["scoutSuccessRate"] = 5
        # directIO
        if "useLocalIO" not in taskParamMap and "allowInputLAN" not in taskParamMap:
            taskParamMap["allowInputLAN"] = "use"
        # current priority
        if "currentPriority" in taskParamMap and (taskParamMap["currentPriority"] < 900 or taskParamMap["currentPriority"] > 1100):
            taskParamMap["currentPriority"] = 1000
        isSU, isSG = self.taskBufferIF.isSuperUser(taskParamMap["userName"])
        if isSU or (isSG and "workingGroup" in taskParamMap):
            # super high priority to jump over others
            if "currentPriority" not in taskParamMap or taskParamMap["currentPriority"] < JobUtils.priorityTasksToJumpOver:
                taskParamMap["currentPriority"] = JobUtils.priorityTasksToJumpOver
        # max attempts
        if "maxAttempt" not in taskParamMap:
            taskParamMap["maxAttempt"] = 10
        if "maxFailure" not in taskParamMap:
            taskParamMap["maxFailure"] = 3
        # target walltime
        if "maxWalltime" not in taskParamMap:
            tgtWalltime = self.taskBufferIF.getConfigValue("taskrefiner", "USER_JOB_TARGET_WALLTIME", "jedi", "atlas")
            if tgtWalltime:
                taskParamMap["maxWalltime"] = tgtWalltime
        # choose N % of tasks to enable input data motion
        fracTaskWithDataMotion = self.taskBufferIF.getConfigValue("taskrefiner", "USER_TASKS_MOVE_INPUT", "jedi", "atlas")
        if fracTaskWithDataMotion is not None and fracTaskWithDataMotion > 0:
            if random.randint(1, 100) <= fracTaskWithDataMotion:
                if "currentPriority" not in taskParamMap:
                    taskParamMap["currentPriority"] = taskParamMap["taskPriority"]
                taskParamMap["taskPriority"] = 1001
        # image name
        if "container_name" not in taskParamMap:
            try:
                for tmpItem in taskParamMap["jobParameters"]:
                    if "value" in tmpItem:
                        tmpM = re.search("--containerImage\s+([^\s]+)", tmpItem["value"])
                        if tmpM is not None:
                            taskParamMap["container_name"] = tmpM.group(1)
                            break
            except Exception:
                pass
        # extract container name from architecture
        if "container_name" not in taskParamMap and "architecture" in taskParamMap and isinstance(taskParamMap["architecture"], str):
            try:
                # json-serialized architecture
                check_str = json.loads(taskParamMap["architecture"]).get("encoded_platform", "")
            except Exception:
                # encoded architecture
                check_str = taskParamMap["architecture"]
            m = re.search(r"\+([^\s+@#&]+)", check_str)
            if m:
                # use the architecture as container name
                tmp_container_name = m.group(1)
                taskParamMap["container_name"] = tmp_container_name
                # remove the container name from architecture
                tmp_architecture = re.sub(rf"\+{tmp_container_name}", "", taskParamMap["architecture"])
                taskParamMap["architecture"] = tmp_architecture
                self.tmpLog.debug(f"tweaked architecture: container_name={tmp_container_name} new_architecture={tmp_architecture}")

        # message driven, choose N % of tasks to enable
        if "messageDriven" not in taskParamMap:
            analy_md_percent = self.taskBufferIF.getConfigValue("taskrefiner", "USER_TASKS_MESSAGE_DRIVEN_PERCENT", "jedi", "atlas")
            if analy_md_percent and random.uniform(0, 100) <= analy_md_percent:
                taskParamMap["messageDriven"] = True
        # push status changes, choose N % of tasks to enable
        if "pushStatusChanges" not in taskParamMap:
            analy_pc_percent = self.taskBufferIF.getConfigValue("taskrefiner", "USER_TASKS_PUSH_STATUS_CHANGES_PERCENT", "jedi", "atlas")
            if analy_pc_percent and random.uniform(0, 100) <= analy_pc_percent:
                taskParamMap["pushStatusChanges"] = True
        # disable skipScout fot tasks without for group production role
        if "skipScout" in taskParamMap and ("official" not in taskParamMap or not taskParamMap["official"]):
            del taskParamMap["skipScout"]
        # max core count
        if "maxCoreCount" not in taskParamMap:
            max_core_count = self.taskBufferIF.getConfigValue("taskrefiner", "USER_TASKS_MAX_CORE_COUNT", "jedi", "atlas")
            if max_core_count is None:
                max_core_count = 16
            taskParamMap["maxCoreCount"] = max_core_count
        # update task parameters
        self.updatedTaskParams = taskParamMap
        # call base method
        TaskRefinerBase.extractCommon(self, jediTaskID, taskParamMap, workQueueMapper, splitRule)

    # main
    def doRefine(self, jediTaskID, taskParamMap):
        # make logger
        tmpLog = self.tmpLog
        tmpLog.debug(f"start taskType={self.taskSpec.taskType}")
        try:
            # preprocessing
            tmpStat, taskParamMap = self.doPreProRefine(taskParamMap)
            if tmpStat is True:
                tmpLog.debug("done for preprocessing")
                return self.SC_SUCCEEDED
            if tmpStat is False:
                # failed
                tmpLog.error("doPreProRefine failed")
                return self.SC_FAILED
            # normal refine
            self.doBasicRefine(taskParamMap)
            # set nosplit+repeat for DBR
            for datasetSpec in self.inSecDatasetSpecList:
                # get the latest version of DBR
                if datasetSpec.datasetName == "DBR_LATEST":
                    tmpLog.debug(f"resolving real name for {datasetSpec.datasetName}")
                    datasetSpec.datasetName = self.ddmIF.getInterface(self.taskSpec.vo).getLatestDBRelease(useResultCache=3600)
                    datasetSpec.containerName = datasetSpec.datasetName
                # set attributes to DBR
                if DataServiceUtils.isDBR(datasetSpec.datasetName):
                    datasetSpec.attributes = "repeat,nosplit"
            # check invalid characters
            for datasetSpec in self.outDatasetSpecList:
                if not DataServiceUtils.checkInvalidCharacters(datasetSpec.datasetName):
                    errStr = f"invalid characters in {datasetSpec.datasetName}"
                    tmpLog.error(errStr)
                    self.taskSpec.setErrDiag(errStr, None)
                    return self.SC_FATAL
            # destination
            if "destination" in taskParamMap:
                for datasetSpec in self.outDatasetSpecList:
                    datasetSpec.destination = taskParamMap["destination"]
            # use build
            if "buildSpec" in taskParamMap:
                self.setSplitRule(None, 1, JediTaskSpec.splitRuleToken["useBuild"])
            # use template dataset
            self.setSplitRule(None, 1, JediTaskSpec.splitRuleToken["instantiateTmpl"])
            self.setSplitRule(None, 1, JediTaskSpec.splitRuleToken["instantiateTmplSite"])
            for datasetSpec in self.outDatasetSpecList:
                datasetSpec.type = f"tmpl_{datasetSpec.type}"
            # get jobsetID
            tmpStat, tmpJobID = self.taskBufferIF.getUserJobsetID_JEDI(self.taskSpec.userName)
            if not tmpStat:
                tmpLog.error("failed to get jobsetID failed")
                return self.SC_FAILED
            self.taskSpec.reqID = tmpJobID
            # site limitation
            if "excludedSite" in taskParamMap and "includedSite" in taskParamMap:
                self.taskSpec.setLimitedSites("incexc")
            elif "excludedSite" in taskParamMap:
                self.taskSpec.setLimitedSites("exc")
            elif "includedSite" in taskParamMap:
                self.taskSpec.setLimitedSites("inc")
            # input prestaging
            if self.taskSpec.inputPreStaging():
                # set first contents feed flag
                self.taskSpec.set_first_contents_feed(True)
        except Exception as e:
            errStr = f"doRefine failed with {str(e)}"
            tmpLog.error(f"{errStr} {traceback.format_exc()}")
            self.taskSpec.setErrDiag(errStr, None)
            raise e
        tmpLog.debug("done")
        return self.SC_SUCCEEDED
