import re

from pandaserver.taskbuffer.JediTaskSpec import JediTaskSpec

from .TaskRefinerBase import TaskRefinerBase


# refiner for general purpose
class GenTaskRefiner(TaskRefinerBase):
    # constructor
    def __init__(self, taskBufferIF, ddmIF):
        TaskRefinerBase.__init__(self, taskBufferIF, ddmIF)

    # extract common parameters
    def extractCommon(self, jediTaskID, taskParamMap, workQueueMapper, splitRule):
        if "cloud" not in taskParamMap and "workingGroup" in taskParamMap:
            taskParamMap["cloud"] = taskParamMap["workingGroup"]
        if "transPath" not in taskParamMap:
            taskParamMap["transPath"] = "https://pandaserver-doma.cern.ch/trf/user/runGen-00-00-02"
        # set sourceURL
        try:
            if "sourceURL" in taskParamMap:
                for tmpItem in taskParamMap["jobParameters"]:
                    if "value" in tmpItem:
                        tmpItem["value"] = re.sub("\$\{SURL\}", taskParamMap["sourceURL"], tmpItem["value"])
        except Exception:
            pass
        # min ram count
        if "ramCount" not in taskParamMap:
            taskParamMap["ramCount"] = 2000
            taskParamMap["ramUnit"] = "MBPerCore"
        # push status changes
        if "pushStatusChanges" not in taskParamMap:
            taskParamMap["pushStatusChanges"] = True
        # fine-grained
        if "fineGrainedProc" in taskParamMap:
            taskParamMap["notDiscardEvents"] = True
            taskParamMap["maxAttemptEsJob"] = 0
            taskParamMap["maxAttemptES"] = 1
        # message driven
        if "messageDriven" not in taskParamMap:
            taskParamMap["messageDriven"] = True
        # use cloud as VO
        taskParamMap["cloudAsVO"] = True
        # update task parameters
        self.updatedTaskParams = taskParamMap
        # call base method
        TaskRefinerBase.extractCommon(self, jediTaskID, taskParamMap, workQueueMapper, splitRule)

    # main
    def doRefine(self, jediTaskID, taskParamMap):
        # normal refine
        self.doBasicRefine(taskParamMap)
        # get DDM I/F to check
        if self.ddmIF.getInterface(self.taskSpec.vo, self.taskSpec.cloud):
            # use template dataset
            self.setSplitRule(None, 1, JediTaskSpec.splitRuleToken["instantiateTmpl"])
            self.setSplitRule(None, 1, JediTaskSpec.splitRuleToken["instantiateTmplSite"])
            for datasetSpec in self.outDatasetSpecList:
                datasetSpec.type = f"tmpl_{datasetSpec.type}"
        return self.SC_SUCCEEDED
