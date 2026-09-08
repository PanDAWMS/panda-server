import datetime
import enum
import json
import math
import re
from typing import TYPE_CHECKING, Any, Sequence

from pandaserver.taskbuffer import task_split_rules

if TYPE_CHECKING:
    from pandaserver.taskbuffer.JediDatasetSpec import JediDatasetSpec
    from pandaserver.taskbuffer.NucleusSpec import NucleusSpec
    from pandaserver.taskbuffer.SiteSpec import SiteSpec

"""
task specification for JEDI

"""


class JediTaskSpec(object):
    # attributes
    attributes = (
        "jediTaskID",
        "taskName",
        "status",
        "userName",
        "creationDate",
        "modificationTime",
        "startTime",
        "endTime",
        "frozenTime",
        "prodSourceLabel",
        "workingGroup",
        "vo",
        "coreCount",
        "taskType",
        "processingType",
        "taskPriority",
        "currentPriority",
        "architecture",
        "transUses",
        "transHome",
        "transPath",
        "lockedBy",
        "lockedTime",
        "termCondition",
        "splitRule",
        "walltime",
        "walltimeUnit",
        "outDiskCount",
        "outDiskUnit",
        "workDiskCount",
        "workDiskUnit",
        "ramCount",
        "ramUnit",
        "ioIntensity",
        "ioIntensityUnit",
        "workQueue_ID",
        "progress",
        "failureRate",
        "errorDialog",
        "reqID",
        "oldStatus",
        "cloud",
        "site",
        "countryGroup",
        "parent_tid",
        "eventService",
        "ticketID",
        "ticketSystemType",
        "stateChangeTime",
        "superStatus",
        "campaign",
        "mergeRamCount",
        "mergeRamUnit",
        "mergeWalltime",
        "mergeWalltimeUnit",
        "throttledTime",
        "numThrottled",
        "mergeCoreCount",
        "goal",
        "assessmentTime",
        "cpuTime",
        "cpuTimeUnit",
        "cpuEfficiency",
        "baseWalltime",
        "nucleus",
        "baseRamCount",
        "ttcRequested",
        "ttcPredicted",
        "ttcPredictionDate",
        "rescueTime",
        "requestType",
        "gshare",
        "resource_type",
        "useJumbo",
        "diskIO",
        "diskIOUnit",
        "memory_leak_core",
        "memory_leak_x2",
        "attemptNr",
        "container_name",
        "framework",
        "activatedTime",
        "queuedTime",
        "actionTime",
    )

    # Column types, taken from the Oracle schema of ATLAS_PANDA.JEDI_TASKS (panda-database
    # repo, schema/oracle). The columns are installed by __init__ via setattr, so a type
    # checker sees none of them without these declarations. They carry no value, which
    # both keeps them out of the class dict and keeps __slots__ classes importable.
    # Unset columns really are None here -- this class has no "NULL" sentinel.
    jediTaskID: int | None
    taskName: str | None
    status: str | None
    userName: str | None
    creationDate: datetime.datetime | None
    modificationTime: datetime.datetime | None
    startTime: datetime.datetime | None
    endTime: datetime.datetime | None
    frozenTime: datetime.datetime | None
    prodSourceLabel: str | None
    workingGroup: str | None
    vo: str | None
    coreCount: int | None
    taskType: str | None
    processingType: str | None
    taskPriority: int | None
    currentPriority: int | None
    architecture: str | None
    transUses: str | None
    transHome: str | None
    transPath: str | None
    lockedBy: str | None
    lockedTime: datetime.datetime | None
    termCondition: str | None
    splitRule: str | None
    walltime: int | None
    walltimeUnit: str | None
    outDiskCount: int | None
    outDiskUnit: str | None
    workDiskCount: int | None
    workDiskUnit: str | None
    ramCount: int | None
    ramUnit: str | None
    ioIntensity: int | None
    ioIntensityUnit: str | None
    workQueue_ID: int | None
    progress: int | None
    failureRate: int | None
    errorDialog: str | None
    reqID: int | None
    oldStatus: str | None
    cloud: str | None
    site: str | None
    countryGroup: str | None
    parent_tid: int | None
    eventService: int | None
    ticketID: str | None
    ticketSystemType: str | None
    stateChangeTime: datetime.datetime | None
    superStatus: str | None
    campaign: str | None
    mergeRamCount: int | None
    mergeRamUnit: str | None
    mergeWalltime: int | None
    mergeWalltimeUnit: str | None
    throttledTime: datetime.datetime | None
    numThrottled: int | None
    mergeCoreCount: int | None
    goal: int | None
    assessmentTime: datetime.datetime | None
    cpuTime: int | None
    cpuTimeUnit: str | None
    cpuEfficiency: int | None
    baseWalltime: int | None
    nucleus: str | None
    baseRamCount: int | None
    ttcRequested: datetime.datetime | None
    ttcPredicted: datetime.datetime | None
    ttcPredictionDate: datetime.datetime | None
    rescueTime: datetime.datetime | None
    requestType: str | None
    gshare: str | None
    resource_type: str | None
    useJumbo: str | None
    diskIO: int | None
    diskIOUnit: str | None
    memory_leak_core: int | None
    memory_leak_x2: float | None
    attemptNr: int | None
    container_name: str | None
    framework: str | None
    activatedTime: datetime.datetime | None
    queuedTime: datetime.datetime | None
    actionTime: datetime.datetime | None
    # attributes which have 0 by default
    _zeroAttrs = ()
    # attributes to force update
    _forceUpdateAttrs = ("lockedBy", "lockedTime")
    # mapping between sequence and attr
    _seqAttrMap: dict[str, Any] = {}
    # limit length
    _limitLength = {"errorDialog": 510}
    # attribute length
    _attrLength = {"workingGroup": 32}

    # tokens for split rule
    splitRuleToken = task_split_rules.split_rule_dict

    # enum for preprocessing (derived from task_split_rules.enum_usePrePro)
    _inv = {v: k for k, v in task_split_rules.enum_usePrePro.items()}
    enum_toPreProcess = _inv["toPreProcess"]
    enum_preProcessed = _inv["preProcessed"]
    enum_postPProcess = _inv["postPProcess"]
    # enum for limited sites (single source of truth in task_split_rules)
    enum_limitedSites = task_split_rules.enum_limitedSites
    # enum for scout (derived from task_split_rules.enum_useScout)
    _inv = {v: k for k, v in task_split_rules.enum_useScout.items()}
    enum_noScout = _inv["no_use"]
    enum_useScout = _inv["will_update_requirements"]
    enum_postScout = _inv["updated_requirements"]
    # enum for dataset registration (derived from task_split_rules.enum_registerDatasets)
    _inv = {v: k for k, v in task_split_rules.enum_registerDatasets.items()}
    enum_toRegisterDS = _inv["registering"]
    enum_registeredDS = _inv["registered"]
    enum_moveDS = _inv["moving"]
    # enum for IP connectivity (single source of truth in task_split_rules)
    enum_ipConnectivity = task_split_rules.enum_ipConnectivity
    # enum for IP stack (single source of truth in task_split_rules)
    enum_ipStack = task_split_rules.enum_ipStack
    # enum for alternative stage-out (single source of truth in task_split_rules)
    enum_altStageOut = task_split_rules.enum_altStageOut
    # enum for local direct access (single source of truth in task_split_rules)
    enum_inputLAN = task_split_rules.enum_inputLAN
    # world cloud name
    worldCloudName = "WORLD"

    # enum for contents feeder
    class FirstContentsFeed(enum.Enum):
        TRUE = "1"
        FALSE = "0"

    # enum for useJumbo
    enum_useJumbo = {"waiting": "W", "running": "R", "pending": "P", "lack": "L", "disabled": "D"}

    # enum for input prestaging (inverted from task_split_rules.enum_inputPreStaging)
    enum_inputPreStaging = {v: k for k, v in task_split_rules.enum_inputPreStaging.items()}

    # enum for full chain
    class FullChain(str, enum.Enum):
        Only = "1"
        Require = "2"
        Capable = "3"

    # enum for order input by
    class OrderInputBy(str, enum.Enum):
        eventsAlignment = "1"

    # Bookkeeping attributes installed by __init__ via object.__setattr__, so a type
    # checker sees none of them without these declarations.
    # _changedAttrs maps a column name to the value last assigned to it.
    _changedAttrs: dict[str, Any]
    # jobParamsTemplate holds the JSON template used to build job parameters.
    jobParamsTemplate: str
    # datasetSpecList is filled by the DB proxy when the task is read with its datasets.
    datasetSpecList: list["JediDatasetSpec"]
    # origErrorDialog keeps the full errorDialog before it is truncated by __setattr__.
    origErrorDialog: str | None
    # origUserName keeps the requester name before it is overwritten for a retried task.
    origUserName: str | None

    # constructor
    def __init__(self) -> None:
        # install attributes
        for attr in self.attributes:
            if attr in self._zeroAttrs:
                object.__setattr__(self, attr, 0)
            else:
                object.__setattr__(self, attr, None)
        # map of changed attributes
        object.__setattr__(self, "_changedAttrs", {})
        # template to generate job parameters
        object.__setattr__(self, "jobParamsTemplate", "")
        # associated datasets
        object.__setattr__(self, "datasetSpecList", [])
        # original error dialog
        object.__setattr__(self, "origErrorDialog", None)
        # original user name
        object.__setattr__(self, "origUserName", None)

    # override __setattr__ to collect the changed attributes
    def __setattr__(self, name: str, value: Any) -> None:
        oldVal = getattr(self, name)
        if name in self._limitLength and value is not None:
            # keep original dialog
            if name == "errorDialog":
                object.__setattr__(self, "origErrorDialog", value)
            value = value[: self._limitLength[name]]
        object.__setattr__(self, name, value)
        newVal = getattr(self, name)
        # collect changed attributes
        if oldVal != newVal or name in self._forceUpdateAttrs:
            self._changedAttrs[name] = value

    # copy old attributes
    def copyAttributes(self, oldTaskSpec: "JediTaskSpec") -> None:
        for attr in self.attributes + ("jobParamsTemplate",):
            if "Time" in attr:
                continue
            if "Date" in attr:
                continue
            if attr in ["progress", "failureRate", "errorDialog", "status", "oldStatus", "lockedBy"]:
                continue
            self.__setattr__(attr, getattr(oldTaskSpec, attr))

    # reset changed attribute list
    def resetChangedList(self) -> None:
        object.__setattr__(self, "_changedAttrs", {})

    # reset changed attribute
    def resetChangedAttr(self, name: str) -> None:
        try:
            del self._changedAttrs[name]
        except Exception:
            pass

    # reserve old attributes
    def reserve_old_attributes(self) -> None:
        for attName in ["ramCount", "walltime", "cpuTime", "startTime", "cpuTimeUnit", "outDiskCount", "workDiskCount", "ioIntensity", "diskIO"]:
            self.resetChangedAttr(attName)

    # force update
    def forceUpdate(self, name: str) -> None:
        if name in self.attributes:
            self._changedAttrs[name] = getattr(self, name)

    # return map of values
    def valuesMap(self, useSeq: bool = False, onlyChanged: bool = False) -> dict[str, Any]:
        ret = {}
        for attr in self.attributes:
            # use sequence
            if useSeq and attr in self._seqAttrMap:
                continue
            # only changed attributes
            if onlyChanged:
                if attr not in self._changedAttrs:
                    continue
            val = getattr(self, attr)
            if val is None:
                if attr in self._zeroAttrs:
                    val = 0
                else:
                    val = None
            # truncate too long values
            if attr in self._limitLength:
                if val is not None:
                    val = val[: self._limitLength[attr]]
            ret[f":{attr}"] = val
        return ret

    # pack tuple into TaskSpec
    def pack(self, values: Sequence[Any]) -> None:
        for i in range(len(self.attributes)):
            attr = self.attributes[i]
            val = values[i]
            object.__setattr__(self, attr, val)

    # return column names for INSERT
    @classmethod
    def columnNames(cls, prefix: str | None = None) -> str:
        ret = ""
        for attr in cls.attributes:
            if prefix is not None:
                ret += f"{prefix}."
            ret += f"{attr},"
        ret = ret[:-1]
        return ret

    # return expression of bind variables for INSERT
    @classmethod
    def bindValuesExpression(cls, useSeq: bool = True) -> str:
        ret = "VALUES("
        for attr in cls.attributes:
            if useSeq and attr in cls._seqAttrMap:
                ret += f"{cls._seqAttrMap[attr]},"
            else:
                ret += f":{attr},"
        ret = ret[:-1]
        ret += ")"
        return ret

    # return an expression of bind variables for UPDATE to update only changed attributes
    def bindUpdateChangesExpression(self) -> str:
        ret = ""
        for attr in self.attributes:
            if attr in self._changedAttrs:
                ret += f"{attr}=:{attr},"
        ret = ret[:-1]
        ret += " "
        return ret

    # check split rule
    def check_split_rule(self, key: str) -> bool:
        if self.splitRule is not None:
            if re.search(self.splitRuleToken[key] + r"=(\d+)", self.splitRule):
                return True
        return False

    # get the max size per job if defined
    def getMaxSizePerJob(self) -> int | None:
        if self.splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["nGBPerJob"] + "=(\d+)", self.splitRule)
            if tmpMatch is not None:
                nGBPerJob = int(tmpMatch.group(1)) * 1024 * 1024 * 1024
                return nGBPerJob
        return None

    # remove nGBPerJob
    def removeMaxSizePerJob(self) -> None:
        self.removeSplitRule(self.splitRuleToken["nGBPerJob"])

    # get the max size per merge job if defined
    def getMaxSizePerMergeJob(self) -> int | None:
        if self.splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["nGBPerMergeJob"] + "=(\d+)", self.splitRule)
            if tmpMatch is not None:
                nGBPerJob = int(tmpMatch.group(1)) * 1024 * 1024 * 1024
                return nGBPerJob
        return None

    # get the maxnumber of files per job if defined
    def getMaxNumFilesPerJob(self) -> int | None:
        if self.splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["nMaxFilesPerJob"] + "=(\d+)", self.splitRule)
            if tmpMatch is not None:
                return int(tmpMatch.group(1))
        return None

    # set MaxNumFilesPerJob
    def setMaxNumFilesPerJob(self, value: str) -> None:
        self.setSplitRule("nMaxFilesPerJob", value)

    # get the maxnumber of files per merge job if defined
    def getMaxNumFilesPerMergeJob(self) -> int:
        if self.splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["nMaxFilesPerMergeJob"] + "=(\d+)", self.splitRule)
            if tmpMatch is not None:
                return int(tmpMatch.group(1))
        return 50

    # get the number of events per merge job if defined
    def getNumEventsPerMergeJob(self) -> int | None:
        if self.splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["nEventsPerMergeJob"] + "=(\d+)", self.splitRule)
            if tmpMatch is not None:
                return int(tmpMatch.group(1))
        return None

    # check if using jumbo
    def usingJumboJobs(self) -> bool:
        if self.useJumbo in self.enum_useJumbo.values() and self.useJumbo != self.enum_useJumbo["disabled"]:
            return True
        return False

    # get the number of jumbo jobs if defined
    def getNumJumboJobs(self) -> int | None:
        if self.usingJumboJobs() and self.splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["nJumboJobs"] + "=(\d+)", self.splitRule)
            if tmpMatch is not None:
                return int(tmpMatch.group(1))
        return None

    # get the max number of jumbo jobs per site if defined
    def getMaxJumboPerSite(self) -> int:
        if self.usingJumboJobs() and self.splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["maxJumboPerSite"] + "=(\d+)", self.splitRule)
            if tmpMatch is not None:
                return int(tmpMatch.group(1))
        return 1

    # get the number of sites per job
    def getNumSitesPerJob(self) -> int:
        if not self.useEventService():
            return 1
        if self.splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["nSitesPerJob"] + "=(\d+)", self.splitRule)
            if tmpMatch is not None:
                return int(tmpMatch.group(1))
        return 1

    # get the number of files per job if defined
    def getNumFilesPerJob(self) -> int | None:
        if self.splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["nFilesPerJob"] + "=(\d+)", self.splitRule)
            if tmpMatch is not None:
                n = int(tmpMatch.group(1))
                if self.dynamicNumEvents():
                    inn = self.get_num_events_per_input()
                    dyn = self.get_min_granularity()
                    if inn and dyn and inn > dyn:
                        n *= inn // dyn
                return n
        return None

    # remove nFilesPerJob
    def removeNumFilesPerJob(self) -> None:
        self.removeSplitRule(self.splitRuleToken["nFilesPerJob"])

    # get the number of files per merge job if defined
    def getNumFilesPerMergeJob(self) -> int | None:
        if self.splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["nFilesPerMergeJob"] + "=(\d+)", self.splitRule)
            if tmpMatch is not None:
                return int(tmpMatch.group(1))
        return None

    # get the number of events per job if defined
    def getNumEventsPerJob(self) -> int | None:
        if self.splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["nEventsPerJob"] + "=(\d+)", self.splitRule)
            if tmpMatch is not None:
                return int(tmpMatch.group(1))
        return None

    # get offset for random seed
    def getRndmSeedOffset(self) -> int:
        if self.splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["randomSeed"] + "=(\d+)", self.splitRule)
            if tmpMatch is not None:
                return int(tmpMatch.group(1))
        return 0

    # get offset for first event
    def getFirstEventOffset(self) -> int:
        if self.splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["firstEvent"] + "=(\d+)", self.splitRule)
            if tmpMatch is not None:
                return int(tmpMatch.group(1))
        return 0

    # grouping with boundaryID
    def useGroupWithBoundaryID(self) -> dict[str, Any] | None:
        if self.splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["groupBoundaryID"] + "=(\d+)", self.splitRule)
            if tmpMatch is not None:
                gbID = int(tmpMatch.group(1))
                # 1 : input - can split,    output - free
                # 2 : input - can split,    output - mapped with provenanceID
                # 3 : input - cannot split, output - free
                # 4 : input - cannot split, output - mapped with provenanceID
                #
                # * rule for master
                # 1 : can split. one boundayID per sub chunk
                # 2 : cannot split. one boundayID per sub chunk
                # 3 : cannot split. multiple boundayIDs per sub chunk
                #
                # * rule for secondary
                # 1 : must have same boundayID. cannot split
                #
                retMap: dict[str, Any] = {}
                if gbID in [1, 2]:
                    retMap["inSplit"] = 1
                else:
                    retMap["inSplit"] = 2
                if gbID in [1, 3]:
                    retMap["outMap"] = False
                else:
                    retMap["outMap"] = True
                retMap["secSplit"] = None
                return retMap
        return None

    # use build
    def useBuild(self) -> bool:
        return self.check_split_rule("useBuild")

    # use sjob cloning
    def useJobCloning(self) -> bool:
        return self.check_split_rule("useJobCloning")

    # get job cloning type
    def getJobCloningType(self) -> str:
        if self.splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["useJobCloning"] + "=(\d+)", self.splitRule)
            if tmpMatch is not None:
                return tmpMatch.group(1)
        return ""

    # reuse secondary on demand
    def reuseSecOnDemand(self) -> bool:
        return self.check_split_rule("reuseSecOnDemand")

    # not wait for completion of parent
    def noWaitParent(self) -> bool:
        return self.check_split_rule("noWaitParent")

    # check splitRule if not wait for completion of parent
    @classmethod
    def noWaitParentSL(cls, splitRule: str | None) -> bool:
        if splitRule is not None:
            tmpMatch = re.search(cls.splitRuleToken["noWaitParent"] + "=(\d+)", splitRule)
            if tmpMatch is not None:
                return True
        return False

    # use only limited sites
    def useLimitedSites(self) -> bool:
        return self.check_split_rule("limitedSites")

    # set limited sites
    def setLimitedSites(self, policy: str) -> None:
        tag = None
        for tmpIdx, tmpPolicy in self.enum_limitedSites.items():
            if policy == tmpPolicy:
                tag = tmpIdx
                break
        # not found
        if tag is None:
            return
        # set
        if self.splitRule is None:
            # new
            self.splitRule = self.splitRuleToken["limitedSites"] + "=" + tag
        else:
            tmpMatch = re.search(self.splitRuleToken["limitedSites"] + "=(\d+)", self.splitRule)
            if tmpMatch is None:
                # append
                self.splitRule += "," + self.splitRuleToken["limitedSites"] + "=" + tag
            else:
                # replace
                self.splitRule = re.sub(self.splitRuleToken["limitedSites"] + "=(\d+)", self.splitRuleToken["limitedSites"] + "=" + tag, self.splitRule)

    # use local IO
    def useLocalIO(self) -> bool:
        if self.splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["useLocalIO"] + "=(\d+)", self.splitRule)
            if tmpMatch is not None and int(tmpMatch.group(1)):
                return True
        return False

    # use Event Service
    def useEventService(self, siteSpec: "SiteSpec | None" = None) -> bool:
        if self.eventService in [1, 2]:
            # check site if ES is disabled
            if self.switchEStoNormal() and siteSpec is not None and siteSpec.getJobSeed() in ["all"]:
                return False
            return True
        return False

    # get the number of events per worker for Event Service
    def getNumEventsPerWorker(self) -> int | None:
        if self.splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["nEventsPerWorker"] + "=(\d+)", self.splitRule)
            if tmpMatch is not None:
                return int(tmpMatch.group(1))
        return None

    # get the number of event service consumers
    def getNumEventServiceConsumer(self) -> int | None:
        if not self.useEventService():
            return None
        if self.splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["nEsConsumers"] + "=(\d+)", self.splitRule)
            if tmpMatch is not None:
                return int(tmpMatch.group(1))
        return None

    # disable automatic retry
    def disableAutoRetry(self) -> bool:
        return self.check_split_rule("disableAutoRetry")

    # disable reassign
    def disableReassign(self) -> bool:
        return self.check_split_rule("disableReassign")

    # allow empty input
    def allowEmptyInput(self) -> bool:
        return self.check_split_rule("allowEmptyInput")

    # use PFN list
    def useListPFN(self) -> bool:
        return self.check_split_rule("pfnList")

    # set preprocessing
    def setPrePro(self) -> None:
        if self.splitRule is None:
            # new
            self.splitRule = self.splitRuleToken["usePrePro"] + "=" + self.enum_toPreProcess
        else:
            # append
            self.splitRule += "," + self.splitRuleToken["usePrePro"] + "=" + self.enum_toPreProcess

    # use preprocessing
    def usePrePro(self) -> bool:
        if self.splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["usePrePro"] + "=(\d+)", self.splitRule)
            if tmpMatch is not None and tmpMatch.group(1) == self.enum_toPreProcess:
                return True
        return False

    # set preprocessed
    def setPreProcessed(self) -> None:
        if self.splitRule is None:
            # new
            self.splitRule = self.splitRuleToken["usePrePro"] + "=" + self.enum_preProcessed
        else:
            tmpMatch = re.search(self.splitRuleToken["usePrePro"] + "=(\d+)", self.splitRule)
            if tmpMatch is None:
                # append
                self.splitRule += "," + self.splitRuleToken["usePrePro"] + "=" + self.enum_preProcessed
            else:
                # replace
                self.splitRule = re.sub(
                    self.splitRuleToken["usePrePro"] + "=(\d+)", self.splitRuleToken["usePrePro"] + "=" + self.enum_preProcessed, self.splitRule
                )
        return

    # check preprocessed
    def checkPreProcessed(self) -> bool:
        if self.splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["usePrePro"] + "=(\d+)", self.splitRule)
            if tmpMatch is not None and tmpMatch.group(1) == self.enum_preProcessed:
                return True
        return False

    # set post preprocess
    def setPostPreProcess(self) -> None:
        if self.splitRule is None:
            # new
            self.splitRule = self.splitRuleToken["usePrePro"] + "=" + self.enum_postPProcess
        else:
            tmpMatch = re.search(self.splitRuleToken["usePrePro"] + "=(\d+)", self.splitRule)
            if tmpMatch is None:
                # append
                self.splitRule += "," + self.splitRuleToken["usePrePro"] + "=" + self.enum_postPProcess
            else:
                # replace
                self.splitRule = re.sub(
                    self.splitRuleToken["usePrePro"] + "=(\d+)", self.splitRuleToken["usePrePro"] + "=" + self.enum_postPProcess, self.splitRule
                )
        return

    # instantiate template datasets
    def instantiateTmpl(self) -> bool:
        return self.check_split_rule("instantiateTmpl")

    # instantiate template datasets at site
    def instantiateTmplSite(self) -> bool:
        return self.check_split_rule("instantiateTmplSite")

    # merge output
    def mergeOutput(self) -> bool:
        return self.check_split_rule("mergeOutput")

    # use random seed
    def useRandomSeed(self) -> bool:
        return self.check_split_rule("randomSeed")

    # get the size of workDisk in bytes
    def getWorkDiskSize(self) -> int:
        safetyMargin = 300 * 1024 * 1024
        tmpSize = self.workDiskCount
        if tmpSize is None:
            return 0
        if self.workDiskUnit == "GB":
            tmpSize = tmpSize * 1024 * 1024 * 1024
        elif self.workDiskUnit == "MB":
            tmpSize = tmpSize * 1024 * 1024
        elif self.workDiskUnit == "kB":
            tmpSize = tmpSize * 1024
        tmpSize += safetyMargin
        return tmpSize

    # get the size of outDisk in bytes
    def getOutDiskSize(self) -> int:
        tmpSize = self.outDiskCount
        if tmpSize is None or tmpSize < 0:
            return 0
        if self.outDiskUnit is not None:
            if self.outDiskUnit.startswith("GB"):
                tmpSize = tmpSize * 1024 * 1024 * 1024
            elif self.outDiskUnit.startswith("MB"):
                tmpSize = tmpSize * 1024 * 1024
            elif self.outDiskUnit.startswith("kB"):
                tmpSize = tmpSize * 1024
        return tmpSize

    # output scales with the number of events
    def outputScaleWithEvents(self) -> bool:
        if self.outDiskUnit is not None and "PerEvent" in self.outDiskUnit:
            return True
        return False

    # return list of status to update contents
    @classmethod
    def statusToUpdateContents(cls) -> list[str]:
        return ["defined"]

    # set task status on hold
    def setOnHold(self) -> None:
        # change status
        if self.status in ["ready", "running", "merging", "scouting", "defined", "topreprocess", "preprocessing", "registered", "prepared", "rerefine"]:
            self.oldStatus = self.status
            self.status = "pending"

    # return list of status to reject external changes
    @classmethod
    def statusToRejectExtChange(cls) -> list[str]:
        return ["finished", "done", "prepared", "broken", "tobroken", "aborted", "toabort", "aborting", "failed", "passed"]

    # return list of status for retry
    @classmethod
    def statusToRetry(cls) -> list[str]:
        return ["finished", "failed", "aborted", "exhausted"]

    # return list of status for incexec
    @classmethod
    def statusToIncexec(cls) -> list[str]:
        return ["done"] + cls.statusToRetry()

    # return list of status for reassign
    @classmethod
    def statusToReassign(cls) -> list[str]:
        return ["registered", "defined", "ready", "running", "scouting", "scouted", "pending", "assigning", "exhausted"]

    # return list of status for Job Generator
    @classmethod
    def statusForJobGenerator(cls) -> list[str]:
        return ["ready", "running", "scouting", "topreprocess", "preprocessing"]

    # return list of status to not pause
    @classmethod
    def statusNotToPause(cls) -> list[str]:
        return ["finished", "failed", "done", "aborted", "broken", "paused"]

    # return mapping of command and status
    @classmethod
    def commandStatusMap(cls) -> dict[str, dict[str, str]]:
        return {
            "kill": {"doing": "aborting", "done": "toabort"},
            "finish": {"doing": "finishing", "done": "passed"},
            "retry": {"doing": "toretry", "done": "ready"},
            "incexec": {"doing": "toincexec", "done": "rerefine"},
            "reassign": {"doing": "toreassign", "done": "reassigning"},
            "pause": {"doing": "paused", "done": "dummy"},
            "resume": {"doing": "dummy", "done": "dummy"},
            "avalanche": {"doing": "dummy", "done": "dummy"},
            "release": {"doing": "dummy", "done": "dummy"},
        }

    # qualifiers for retry command
    @classmethod
    def get_retry_command_qualifiers(
        cls,
        no_child_retry: bool = False,
        discard_events: bool = False,
        disable_staging_mode: bool = False,
        keep_gshare_priority: bool = False,
        ignore_hard_exhausted: bool = False,
    ) -> list[str]:
        """
        Get the list of qualifiers for the retry command.
        :param no_child_retry: If True, retry will not be attempted on child tasks.
        :param discard_events: If True, events will be discarded.
        :param disable_staging_mode: If True, staging mode will be disabled.
        :param keep_gshare_priority: If True, current gshare and priority will be kept.
        :param ignore_hard_exhausted: If True, the limits for hard exhausted will be ignored.
        :return: A list of qualifiers.
        """
        qualifiers = []
        if no_child_retry:
            qualifiers.append("sole")
        if discard_events:
            qualifiers.append("discard")
        if disable_staging_mode:
            qualifiers.append("staged")
        if keep_gshare_priority:
            qualifiers.append("keep")
        if ignore_hard_exhausted:
            qualifiers.append("transcend")
        return qualifiers

    # set error dialog
    def setErrDiag(self, diag: str | None, append: bool | None = False, prepend: bool = False) -> None:
        # check if message can be encoded with UTF-8
        if diag:
            try:
                diag.encode()
            except UnicodeEncodeError:
                # remove non-ascii chars
                diag = re.sub(r"[^\x00-\x7F]+", "<non-ASCII char>", diag)
        # set error dialog
        if append is True and self.errorDialog is not None:
            if not prepend:
                self.errorDialog = f"{self.errorDialog} {diag}"
            else:
                self.errorDialog = f"{diag} {self.errorDialog}"
        elif append is None:
            # keep old log
            if self.errorDialog is None:
                self.errorDialog = diag
        else:
            self.errorDialog = diag

    # use loadXML
    def useLoadXML(self) -> bool:
        return self.check_split_rule("loadXML")

    # make VOMS FQANs
    def makeFQANs(self) -> list[str]:
        # no working group
        if self.workingGroup is not None:
            fqan = f"/{self.vo}/{self.workingGroup}/Role=production"
        else:
            if self.vo is not None:
                fqan = f"/{self.vo}/Role=NULL"
            else:
                return []
        # return
        return [fqan]

    # set split rule
    def setSplitRule(self, ruleName: str, ruleValue: str) -> None:
        if self.splitRule is None:
            # new
            self.splitRule = self.splitRuleToken[ruleName] + "=" + ruleValue
        else:
            tmpMatch = re.search(self.splitRuleToken[ruleName] + "=(\d+)", self.splitRule)
            if tmpMatch is None:
                # append
                self.splitRule += "," + self.splitRuleToken[ruleName] + "=" + ruleValue
            else:
                # replace
                self.splitRule = re.sub(self.splitRuleToken[ruleName] + "=(\d+)", self.splitRuleToken[ruleName] + "=" + ruleValue, self.splitRule)

    # remove split rule
    def removeSplitRule(self, ruleName: str) -> None:
        if self.splitRule is not None:
            items = self.splitRule.split(",")
            newItems = []
            for item in items:
                # remove rule
                tmpRuleName = item.split("=")[0]
                if ruleName == tmpRuleName:
                    continue
                newItems.append(item)
            self.splitRule = ",".join(newItems)

    # set to use scout
    def setUseScout(self, useFlag: bool) -> None:
        if useFlag:
            self.setSplitRule("useScout", self.enum_useScout)
        else:
            self.setSplitRule("useScout", self.enum_noScout)

    # set post scout
    def setPostScout(self) -> None:
        self.setSplitRule("useScout", self.enum_postScout)

    # use scout
    def useScout(self, splitRule: str | None = None) -> bool:
        if splitRule is None:
            splitRule = self.splitRule
        if splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["useScout"] + "=(\d+)", splitRule)
            if tmpMatch is not None and tmpMatch.group(1) == self.enum_useScout:
                return True
        return False

    # use exhausted
    def useExhausted(self) -> bool:
        return self.check_split_rule("useExhausted")

    # use real number of events
    def useRealNumEvents(self) -> bool:
        return self.check_split_rule("useRealNumEvents")

    # use input LFN as source for output LFN
    def useFileAsSourceLFN(self) -> bool:
        return self.check_split_rule("useFileAsSourceLFN")

    # post scout
    def isPostScout(self) -> bool:
        if self.splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["useScout"] + "=(\d+)", self.splitRule)
            if tmpMatch is not None and tmpMatch.group(1) == self.enum_postScout:
                return True
        return False

    # wait until input shows up
    def waitInput(self) -> bool:
        return self.check_split_rule("waitInput")

    # input prestaging
    def inputPreStaging(self) -> bool:
        if self.splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["inputPreStaging"] + "=" + self.enum_inputPreStaging["use"], self.splitRule)
            if tmpMatch is not None:
                return True
        return False

    # set DDM backend
    def setDdmBackEnd(self, backEnd: str) -> None:
        if self.splitRule is None:
            # new
            self.splitRule = self.splitRuleToken["ddmBackEnd"] + "=" + backEnd
        else:
            tmpMatch = re.search(self.splitRuleToken["ddmBackEnd"] + "=([^,$]+)", self.splitRule)
            if tmpMatch is None:
                # append
                self.splitRule += "," + self.splitRuleToken["ddmBackEnd"] + "=" + backEnd
            else:
                # replace
                self.splitRule = re.sub(self.splitRuleToken["ddmBackEnd"] + "=([^,$]+)", self.splitRuleToken["ddmBackEnd"] + "=" + backEnd, self.splitRule)

    # get DDM backend
    def getDdmBackEnd(self) -> str | None:
        if self.splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["ddmBackEnd"] + "=([^,$]+)", self.splitRule)
            if tmpMatch is not None:
                return tmpMatch.group(1)
        return None

    # get field number to add middle name to LFN
    def getFieldNumToLFN(self) -> list[int] | None:
        try:
            if self.splitRule is not None:
                tmpMatch = re.search(self.splitRuleToken["addNthFieldToLFN"] + "=([,\d]+)", self.splitRule)
                if tmpMatch is not None:
                    tmpList = tmpMatch.group(1).split(",")
                    try:
                        tmpList.remove("")
                    except Exception:
                        pass
                    return list(map(int, tmpList))
        except Exception:
            pass
        return None

    # get required success rate for scout jobs
    def getScoutSuccessRate(self) -> int | None:
        if self.splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["scoutSuccessRate"] + "=(\d+)", self.splitRule)
            if tmpMatch is not None:
                return int(tmpMatch.group(1))
        return None

    # get T1 weight
    def getT1Weight(self) -> int:
        if self.splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["t1Weight"] + "=(-*\d+)", self.splitRule)
            if tmpMatch is not None:
                return int(tmpMatch.group(1))
        return 0

    # respect Lumiblock boundaries
    def respectLumiblock(self) -> bool:
        return self.check_split_rule("respectLB")

    # release files per Lumiblock
    def releasePerLumiblock(self) -> bool:
        return self.check_split_rule("releasePerLB")

    # order by Lumiblock numbers
    def orderByLB(self) -> bool:
        return self.check_split_rule("orderByLB")

    # respect split rule
    def respectSplitRule(self) -> bool:
        return self.check_split_rule("respectSplitRule")

    # allow partial finish
    def allowPartialFinish(self) -> bool:
        return self.check_split_rule("allowPartialFinish")

    # check if datasets should be registered or moved
    def toRegisterDatasets(self) -> bool:
        if self.splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["registerDatasets"] + "=(\d+)", self.splitRule)
            if tmpMatch is not None and tmpMatch.group(1) in [self.enum_toRegisterDS, self.enum_moveDS]:
                return True
        return False

    # check if datasets should be moved
    def toMoveDatasets(self) -> bool:
        if self.splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["registerDatasets"] + "=(\d+)", self.splitRule)
            if tmpMatch is not None and tmpMatch.group(1) == self.enum_moveDS:
                return True
        return False

    # datasets were registered
    def registeredDatasets(self) -> None:
        self.setSplitRule("registerDatasets", self.enum_registeredDS)

    # set datasets to be registered
    def setToRegisterDatasets(self) -> None:
        self.setSplitRule("registerDatasets", self.enum_toRegisterDS)

    # set datasets to be moved
    def setToMoveDatasets(self) -> None:
        self.setSplitRule("registerDatasets", self.enum_moveDS)

    # get the max number of attempts for ES events
    def getMaxAttemptES(self) -> int | None:
        if self.splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["maxAttemptES"] + "=(\d+)", self.splitRule)
            if tmpMatch is not None:
                return int(tmpMatch.group(1))
        return None

    # get the max number of attempts for ES jobs
    def getMaxAttemptEsJob(self) -> int | None:
        if self.splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["maxAttemptEsJob"] + "=(\d+)", self.splitRule)
            if tmpMatch is not None:
                return int(tmpMatch.group(1))
        return self.getMaxAttemptES()

    # check attribute length
    def checkAttrLength(self) -> bool:
        for attrName, attrLength in self._attrLength.items():
            attrVal = getattr(self, attrName)
            if attrVal is None:
                continue
            if len(attrVal) > attrLength:
                setattr(self, attrName, None)
                self.errorDialog = f"{attrName} is too long (actual: {len(attrVal)}, maximum: {attrLength})"
                return False
        return True

    # set IP connectivity and stack
    def setIpConnectivity(self, value: str | None) -> None:
        if not value:
            return
        values = value.split("#")
        if not values:
            return
        ipConnectivity = values[0]
        if ipConnectivity in self.enum_ipConnectivity.values():
            for tmpKey, tmpVal in self.enum_ipConnectivity.items():
                if ipConnectivity == tmpVal:
                    self.setSplitRule("ipConnectivity", tmpKey)
                    break
        if len(values) > 1:
            ipStack = values[1]
            if ipStack in self.enum_ipStack.values():
                for tmpKey, tmpVal in self.enum_ipStack.items():
                    if ipStack == tmpVal:
                        self.setSplitRule("ipStack", tmpKey)
                        break

    # get IP connectivity
    def getIpConnectivity(self) -> str | None:
        if self.splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["ipConnectivity"] + "=(\d+)", self.splitRule)
            if tmpMatch is not None:
                return self.enum_ipConnectivity[tmpMatch.group(1)]
        return None

    # get IP connectivity
    def getIpStack(self) -> str | None:
        if self.splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["ipStack"] + "=(\d+)", self.splitRule)
            if tmpMatch is not None:
                return self.enum_ipStack[tmpMatch.group(1)]
        return None

    # use HS06 for walltime estimation
    def useHS06(self) -> bool:
        return self.cpuTimeUnit in ["HS06sPerEvent", "HS06sPerEventFixed", "mHS06sPerEvent", "mHS06sPerEventFixed"]

    # get CPU time in sec
    def getCpuTime(self) -> float | None:
        if not self.useHS06():
            return None
        try:
            if self.cpuTimeUnit is not None and self.cpuTimeUnit.startswith("m") and self.cpuTime is not None:
                return float(self.cpuTime) / 1000.0
        except Exception:
            pass
        return self.cpuTime

    # RAM scales with nCores
    def ramPerCore(self) -> bool:
        return self.ramUnit in ["MBPerCore", "MBPerCoreFixed"]

    # run until input is closed
    def runUntilClosed(self) -> bool:
        return self.check_split_rule("runUntilClosed")

    # stay output on site
    def stayOutputOnSite(self) -> bool:
        return self.check_split_rule("stayOutputOnSite")

    # fail when goal unreached
    def failGoalUnreached(self) -> bool:
        return self.check_split_rule("failGoalUnreached")

    # unset fail when goal unreached
    def unsetFailGoalUnreached(self) -> None:
        self.removeSplitRule(self.splitRuleToken["failGoalUnreached"])

    # switch ES to normal when jobs land at normal sites
    def switchEStoNormal(self) -> bool:
        return self.check_split_rule("switchEStoNormal")

    # use world cloud
    def useWorldCloud(self) -> bool:
        return self.cloud == self.worldCloudName

    # dynamic number of events
    def dynamicNumEvents(self) -> bool:
        if self.splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["dynamicNumEvents"] + "=(\d+)", self.splitRule)
            if tmpMatch is not None:
                return True
        return False

    # get min granularity for dynamic number of events
    def get_min_granularity(self) -> int | None:
        if self.splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["dynamicNumEvents"] + "=(\d+)", self.splitRule)
            if tmpMatch is not None:
                return int(tmpMatch.group(1))
        return None

    # set alternative stage-out
    def setAltStageOut(self, value: str) -> None:
        if value in self.enum_altStageOut.values():
            for tmpKey, tmpVal in self.enum_altStageOut.items():
                if value == tmpVal:
                    self.setSplitRule("altStageOut", tmpKey)
                    break

    # get alternative stage-out
    def getAltStageOut(self) -> str | None:
        if self.splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["altStageOut"] + "=(\d+)", self.splitRule)
            if tmpMatch is not None:
                return self.enum_altStageOut[tmpMatch.group(1)]
        return None

    # allow WAN for input access
    def allowInputWAN(self) -> bool:
        return self.check_split_rule("allowInputWAN")

    # set mode for input LAN access
    def setAllowInputLAN(self, value: str) -> None:
        if value in self.enum_inputLAN.values():
            for tmpKey, tmpVal in self.enum_inputLAN.items():
                if value == tmpVal:
                    self.setSplitRule("allowInputLAN", tmpKey)
                    break

    # check if LAN is used for input access
    def allowInputLAN(self) -> str | None:
        if self.splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["allowInputLAN"] + "=(\d+)", self.splitRule)
            if tmpMatch is not None:
                return self.enum_inputLAN[tmpMatch.group(1)]
        return None

    # put log files to OS
    def putLogToOS(self) -> bool:
        return self.check_split_rule("putLogToOS")

    # merge ES on Object Store
    def mergeEsOnOS(self) -> bool:
        return self.check_split_rule("mergeEsOnOS")

    # write input to file
    def writeInputToFile(self) -> bool:
        return self.check_split_rule("writeInputToFile")

    # ignore missing input datasets
    def ignoreMissingInDS(self) -> bool:
        return self.check_split_rule("ignoreMissingInDS")

    # suppress execute string conversion
    def noExecStrCnv(self) -> bool:
        return self.check_split_rule("noExecStrCnv")

    # in-file positional event number
    def inFilePosEvtNum(self) -> bool:
        return self.check_split_rule("inFilePosEvtNum")

    # register event service files
    def registerEsFiles(self) -> bool:
        return self.check_split_rule("registerEsFiles")

    # disable auto finish
    def disableAutoFinish(self) -> bool:
        return self.check_split_rule("disableAutoFinish")

    # reset refined attributes which may confuse they system
    def resetRefinedAttrs(self) -> None:
        self.resetChangedAttr("splitRule")
        self.resetChangedAttr("eventService")
        self.reserve_old_attributes()

    # resurrect consumers
    def resurrectConsumers(self) -> bool:
        return self.check_split_rule("resurrectConsumers")

    # use prefetcher
    def usePrefetcher(self) -> bool:
        return self.check_split_rule("usePrefetcher")

    # no input pooling
    def noInputPooling(self) -> bool:
        return self.check_split_rule("noInputPooling")

    # get num of input chunks to wait
    def nChunksToWait(self) -> int | None:
        if self.splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["nChunksToWait"] + "=(\d+)", self.splitRule)
            if tmpMatch is not None:
                return int(tmpMatch.group(1))
        return None

    # get max walltime
    def getMaxWalltime(self) -> int | None:
        if self.splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["maxWalltime"] + "=(\d+)", self.splitRule)
            if tmpMatch is not None:
                return int(tmpMatch.group(1)) * 60 * 60
        return None

    # set max walltime
    def set_max_walltime(self, value: int) -> None:
        self.setSplitRule("maxWalltime", str(value))

    # get target size of the largest output to reset NG
    def getTgtMaxOutputForNG(self) -> int | None:
        if self.splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["tgtMaxOutputForNG"] + "=(\d+)", self.splitRule)
            if tmpMatch is not None:
                return int(tmpMatch.group(1))
        return None

    # not discard events
    def notDiscardEvents(self) -> bool:
        return self.check_split_rule("notDiscardEvents")

    # get min CPU efficiency
    def getMinCpuEfficiency(self) -> int | None:
        if self.splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["minCpuEfficiency"] + "=(\d+)", self.splitRule)
            if tmpMatch is not None:
                return int(tmpMatch.group(1))
        return None

    # decrement attemptNr of events only when failed
    def decAttOnFailedES(self) -> bool:
        return self.check_split_rule("decAttOnFailedES")

    # use zip files to pin input files
    def useZipToPin(self) -> bool:
        return self.check_split_rule("useZipToPin")

    # architecture:
    # old format: sw_platform<@base_platform><#host_cpu_spec><&host_gpu_spec>
    #             host_cpu_spec: architecture<-vendor<-instruction_set>>
    #             host_gpu_spec: vendor<-model>
    #
    # new format: a json dict with keys of sw_platform, base_platform, cpu_specs, and gpu_spec
    #             cpu_specs: a list of json dicts with keys of arch, vendor, and instr
    #             gpu_spec: a json dict with keys of vendor and model

    # reformat architecture into JSON
    def reformat_architecture(self) -> None:
        if self.architecture is None:
            return
        encoded_platform = ""
        try:
            # JSON format without encoded platform
            tmp_dict = json.loads(self.architecture)
            # skip if encoded platform is not present
            if "encoded_platform" not in tmp_dict:
                return
            encoded_platform = tmp_dict["encoded_platform"]
        except Exception:
            pass
        # convert to new format
        new_dict: dict[str, Any] = {}
        sw_platform = self.get_sw_platform()
        if sw_platform:
            new_dict["sw_platform"] = sw_platform
        base_platform = self.get_base_platform(encoded_platform)
        if base_platform:
            new_dict["base_platform"] = base_platform
        host_cpu_spec = self.get_host_cpu_spec(encoded_platform)
        if host_cpu_spec:
            # remove wildcard entries and empty specs
            cpu_specs = [x for x in [{k: v for k, v in d.items() if v != "*"} for d in host_cpu_spec] if x]
            if cpu_specs:
                new_dict["cpu_specs"] = cpu_specs
        host_gpu_spec = self.get_host_gpu_spec()
        if host_gpu_spec:
            # remove wildcard entries and empty specs
            gpu_spec = {k: v for k, v in host_gpu_spec.items() if v != "*"}
            if gpu_spec:
                new_dict["gpu_spec"] = gpu_spec
        self.architecture = json.dumps(new_dict)

    # get SW platform
    def get_sw_platform(self) -> str | None:
        try:
            d = json.loads(self.architecture or "{}")
            sw_platform: str = d.get("sw_platform", "")
            return sw_platform
        except Exception:
            pass
        if self.architecture is not None:
            m = re.search("^([^@#&]*)", self.architecture)
            if m:
                return m.group(1)
        return self.architecture

    # get base platform
    def get_base_platform(self, encoded_platform: str | None = None) -> str | None:
        try:
            d = json.loads(self.architecture or "{}")
            val: str | None = d.get("base_platform", None)
            if val is not None or encoded_platform is None:
                return val
        except Exception:
            pass
        architecture: str | None
        if encoded_platform:
            architecture = encoded_platform
        else:
            architecture = self.architecture
        if architecture is None:
            return None
        # the group matches the empty string, so the search fails exactly when there is no
        # "@" in the platform
        m = re.search("@([^#&]*)", architecture)
        if m is None:
            return None
        img: str | None = m.group(1)
        if img == "":
            img = None
        return img

    # get platforms
    def get_platforms(self) -> str | None:
        if self.architecture is not None:
            platform = self.get_sw_platform()
            base = self.get_base_platform()
            # the platform can be absent even when the architecture is not, when the JSON form
            # carries no sw_platform. There is then nothing to append the base platform to
            if platform and base:
                platform += "@" + base
            return platform
        return self.architecture

    # get host CPU spec
    def get_host_cpu_spec(self, encoded_platform: str | None = None) -> list[dict[str, Any]] | None:
        try:
            d = json.loads(self.architecture or "{}")
            specs = d.get("cpu_specs", None)
            if not specs and encoded_platform is None:
                return None
            else:
                for spec in specs:
                    spec.setdefault("vendor", "*")
                    spec.setdefault("instr", "*")
                cpu_specs: list[dict[str, Any]] = specs
                return cpu_specs
        except Exception:
            pass
        architecture: str | None
        if encoded_platform:
            architecture = encoded_platform
        else:
            architecture = self.architecture
        try:
            if not architecture:
                return None
            # the group matches the empty string, so the search fails exactly when there is
            # no "#" in the platform, which is the case the branch below handles
            m = re.search(r"#([^\^@&]*)", architecture)
            if m is None:
                if re.search(r"^[\^@&]", architecture):
                    return None
                arch = architecture.split("-")[0]
                if arch:
                    return [{"arch": arch, "vendor": "*", "instr": "*"}]
                return None
            spec_strs = m.group(1)
            if not spec_strs:
                return None
            # remove ()
            if spec_strs.startswith("("):
                spec_strs = spec_strs[1:]
            if spec_strs.endswith(")"):
                spec_strs = spec_strs[:-1]
            specs = []
            for spec_str in spec_strs.split("|"):
                spec_str += "-*" * (2 - spec_str.count("-"))
                if "-" not in spec_str:
                    spec_str += "-*"
                items = spec_str.split("-")
                spec = {"arch": items[0], "vendor": items[1], "instr": items[2]}
                specs.append(spec)
            return specs
        except Exception:
            return None

    def get_host_cpu_preference(self) -> Any:
        try:
            d = json.loads(self.architecture or "{}")
            cpu_pref = d.get("cpu_pref", None)
            return cpu_pref
        except Exception:
            return None

    # get host GPU spec
    def get_host_gpu_spec(self) -> dict[str, Any] | None:
        try:
            d = json.loads(self.architecture or "{}")
            spec = d.get("gpu_spec", None)
            spec.setdefault("vendor", "*")
            spec.setdefault("model", "*")
            gpu_spec: dict[str, Any] = spec
            return gpu_spec
        except Exception:
            pass
        try:
            if self.architecture is None:
                return None
            # the group matches the empty string, so the search fails exactly when there is
            # no "&" in the platform
            m = re.search(r"&([^\^@#]*)", self.architecture)
            if m is None:
                return None
            spec_str = m.group(1)
            if not spec_str:
                return None
            # split into legacy vendor<-model> part and optional colon-separated key=value attributes
            parts = spec_str.split(":")
            legacy = parts[0]
            legacy += "-*" * (1 - legacy.count("-"))
            items = legacy.split("-")
            spec = {"vendor": items[0], "model": items[1]}
            # parse extended attributes: cuda>=12.0, vram=40960, uarch=Ampere, driver>=575.0, model=.*A100.*
            shorthand_map = {"cuda": "version", "uarch": "microarchitecture", "driver": "driver_version", "model": "model", "vram": "vram"}
            for part in parts[1:]:
                attr_m = re.match(r"(\w+)(>=|<=|!=|==|>|<|=)(.*)", part)
                if not attr_m:
                    continue
                key, op, val = attr_m.group(1), attr_m.group(2), attr_m.group(3)
                mapped = shorthand_map.get(key)
                if not mapped:
                    continue
                if mapped == "model":
                    spec[mapped] = {"pattern": val, "excl": True} if op == "!=" else val
                elif mapped == "microarchitecture":
                    spec[mapped] = val
                else:
                    spec[mapped] = ("==" if op == "=" else op) + val

            return spec
        except Exception:
            return None

    # HPO workflow
    def is_hpo_workflow(self) -> bool:
        return self.check_split_rule("hpoWorkflow")

    # debug mode
    def is_debug_mode(self) -> bool:
        return self.check_split_rule("debugMode")

    # multi-step execution
    def is_multi_step_exec(self) -> bool:
        return self.check_split_rule("multiStepExec")

    # get max number of jobs
    def get_max_num_jobs(self) -> int | None:
        if self.splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["maxNumJobs"] + "=(\d+)", self.splitRule)
            if tmpMatch is not None:
                return int(tmpMatch.group(1))
        return None

    # get total number of jobs
    def get_total_num_jobs(self) -> int | None:
        if self.splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["totNumJobs"] + "=(\d+)", self.splitRule)
            if tmpMatch is not None:
                return int(tmpMatch.group(1))
        return None

    # use only tags for fat container
    def use_only_tags_fc(self) -> bool:
        return self.check_split_rule("onlyTagsForFC")

    # avoid VP
    def avoid_vp(self) -> bool:
        return self.check_split_rule("avoidVP")

    # set first contents feed
    def set_first_contents_feed(self, is_first: bool) -> None:
        if is_first:
            self.setSplitRule("firstContentsFeed", self.FirstContentsFeed.TRUE.value)
        else:
            self.setSplitRule("firstContentsFeed", self.FirstContentsFeed.FALSE.value)

    # check if first contents feed
    def is_first_contents_feed(self) -> bool:
        if self.splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["firstContentsFeed"] + "=(\d+)", self.splitRule)
            if tmpMatch is not None and tmpMatch.group(1) == self.FirstContentsFeed.TRUE.value:
                return True
        return False

    # check if work is segmented
    def is_work_segmented(self) -> bool:
        return self.check_split_rule("segmentedWork")

    # check if looping check is disabled
    def no_looping_check(self) -> bool:
        return self.check_split_rule("noLoopingCheck")

    # encode job parameters
    def encode_job_params(self) -> bool:
        return self.check_split_rule("encJobParams")

    # get original error dialog
    def get_original_error_dialog(self) -> str:
        if not self.origErrorDialog:
            return ""
        # remove log URL
        tmpStr = re.sub("<a href.+</a> : ", "", self.origErrorDialog)
        return tmpStr.split(". ")[-1]

    # check if secrets are used
    def use_secrets(self) -> bool:
        return self.check_split_rule("useSecrets")

    # get max core count
    def get_max_core_count(self) -> int | None:
        if self.splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["maxCoreCount"] + "=(\d+)", self.splitRule)
            if tmpMatch is not None:
                return int(tmpMatch.group(1))
        return None

    # push status changes
    def push_status_changes(self) -> bool:
        return push_status_changes(self.splitRule)

    # use cloud as VO
    def cloud_as_vo(self) -> bool:
        return self.check_split_rule("cloudAsVO")

    # push job
    def push_job(self) -> bool:
        return self.check_split_rule("pushJob")

    # fine-grained process
    def is_fine_grained_process(self) -> bool:
        return self.check_split_rule("fineGrainedProc")

    # on site merging
    def on_site_merging(self) -> bool:
        return self.check_split_rule("onSiteMerging")

    # set full chain flag
    def set_full_chain(self, mode: str) -> None:
        var = None
        if mode == "only":
            var = self.FullChain.Only
        elif mode == "require":
            var = self.FullChain.Require
        elif mode == "capable":
            var = self.FullChain.Capable
        if var:
            self.setSplitRule("fullChain", var)

    # get full chain flag
    def get_full_chain(self) -> str | None:
        if self.splitRule:
            tmpMatch = re.search(self.splitRuleToken["fullChain"] + r"=(\d+)", self.splitRule)
            if tmpMatch:
                return tmpMatch.group(1)
        return None

    # check full chain with mode
    def check_full_chain_with_mode(self, mode: str) -> bool:
        task_flag = self.get_full_chain()
        if mode == "only":
            if task_flag == self.FullChain.Only:
                return True
        elif mode == "require":
            if task_flag == self.FullChain.Require:
                return True
        elif mode == "capable":
            if task_flag == self.FullChain.Capable:
                return True
        return False

    # check full chain with nucleus
    def check_full_chain_with_nucleus(self, nucleus: "NucleusSpec") -> bool:
        if self.get_full_chain() and nucleus.get_bare_nucleus_mode():
            return True
        return False

    # get RAM for retry
    def get_ram_for_retry(self, current_ram: int | None) -> int | None:
        if not self.splitRule:
            return None
        tmpMatch = re.search(self.splitRuleToken["retryRamOffset"] + r"=(\d+)", self.splitRule)
        if not tmpMatch:
            return None
        offset = int(tmpMatch.group(1))
        tmpMatch = re.search(self.splitRuleToken["retryRamStep"] + r"=(\d+)", self.splitRule)
        if tmpMatch:
            step = int(tmpMatch.group(1))
        else:
            step = 0
        tmpMatch = re.search(self.splitRuleToken["retryRamMax"] + r"=(\d+)", self.splitRule)
        if tmpMatch:
            max_ram = int(tmpMatch.group(1))
        else:
            max_ram = None
        if not current_ram:
            return current_ram
        if current_ram < offset:
            if max_ram is None:
                return offset
            else:
                return min(offset, max_ram)
        if not step:
            return current_ram
        if max_ram is None:
            return offset + math.ceil((current_ram - offset) / step) * step
        else:
            return min(offset + math.ceil((current_ram - offset) / step) * step, max_ram)

    # get number of events per input
    def get_num_events_per_input(self) -> int | None:
        if self.splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["nEventsPerInput"] + "=(\d+)", self.splitRule)
            if tmpMatch is not None:
                return int(tmpMatch.group(1))
        return None

    def get_max_events_per_job(self) -> int | None:
        if self.splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["maxEventsPerJob"] + "=(\d+)", self.splitRule)
            if tmpMatch is not None:
                return int(tmpMatch.group(1))
        return None

    # set order input by
    def set_order_input_by(self, mode: str) -> None:
        var = None
        if mode == "eventsAlignment":
            var = self.OrderInputBy.eventsAlignment
        if var:
            self.setSplitRule("orderInputBy", var)

    # get full chain flag
    def order_input_by(self) -> str | None:
        if self.splitRule:
            tmpMatch = re.search(self.splitRuleToken["orderInputBy"] + r"=(\d+)", self.splitRule)
            if tmpMatch:
                if tmpMatch.group(1) == self.OrderInputBy.eventsAlignment:
                    return "eventsAlignment"
        return None

    # check if intermediate task
    def is_intermediate_task(self) -> bool:
        return self.check_split_rule("intermediateTask")

    # check if message driven
    def is_msg_driven(self) -> bool:
        return is_msg_driven(self.splitRule)

    # check if incomplete input datasets are allowed
    def allow_incomplete_input(self) -> bool:
        return self.check_split_rule("allowIncompleteInDS")

    # check if workflow holdup
    def is_workflow_holdup(self) -> bool:
        return self.check_split_rule("workflowHoldup")

    # set workflow holdup
    def set_workflow_holdup(self, value: bool) -> None:
        if value:
            self.setSplitRule("workflowHoldup", "1")
        else:
            self.removeSplitRule(self.splitRuleToken["workflowHoldup"])

    # get queued time
    def get_queued_time(self) -> float | None:
        """
        Get queued time in timestamp
        :return: queued time in timestamp. None if not set
        """
        if self.queuedTime is None:
            return None
        return self.queuedTime.timestamp()


# utils


# check split rule with positive integer
def check_split_rule_positive_int(key: str, split_rule: str | None) -> bool:
    if not split_rule:
        return False
    tmpMatch = re.search(JediTaskSpec.splitRuleToken[key] + r"=(\d+)", split_rule)
    if not tmpMatch or int(tmpMatch.group(1)) <= 0:
        return False
    return True


# check if push status changes without class instance
def push_status_changes(split_rule: str | None) -> bool:
    return check_split_rule_positive_int("pushStatusChanges", split_rule)


# check if message driven without class instance
def is_msg_driven(split_rule: str | None) -> bool:
    return check_split_rule_positive_int("messageDriven", split_rule)


# check if auto pause is disabled
def is_auto_pause_disabled(split_rule: str | None) -> bool:
    return not check_split_rule_positive_int("noAutoPause", split_rule)
