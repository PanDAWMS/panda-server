import enum
import json
import math
import re

from pandaserver.taskbuffer import task_split_rules

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
    )
    # attributes which have 0 by default
    _zeroAttrs = ()
    # attributes to force update
    _forceUpdateAttrs = ("lockedBy", "lockedTime")
    # mapping between sequence and attr
    _seqAttrMap = {}
    # limit length
    _limitLength = {"errorDialog": 510}
    # attribute length
    _attrLength = {"workingGroup": 32}

    # tokens for split rule
    splitRuleToken = task_split_rules.split_rule_dict

    # enum for preprocessing
    enum_toPreProcess = "1"
    enum_preProcessed = "2"
    enum_postPProcess = "3"
    # enum for limited sites
    enum_limitedSites = {"1": "inc", "2": "exc", "3": "incexc"}
    # enum for scout
    enum_noScout = "1"
    enum_useScout = "2"
    enum_postScout = "3"
    # enum for dataset registration
    enum_toRegisterDS = "1"
    enum_registeredDS = "2"
    # enum for IP connectivity
    enum_ipConnectivity = {"1": "full", "2": "http"}
    # enum for IP stack
    enum_ipStack = {"4": "IPv4", "6": "IPv6"}
    # enum for alternative stage-out
    enum_altStageOut = {"1": "on", "2": "off", "3": "force"}
    # enum for local direct access
    enum_inputLAN = {"1": "use", "2": "only"}
    # world cloud name
    worldCloudName = "WORLD"

    # enum for contents feeder
    class FirstContentsFeed(enum.Enum):
        TRUE = "1"
        FALSE = "0"

    # enum for useJumbo
    enum_useJumbo = {"waiting": "W", "running": "R", "pending": "P", "lack": "L", "disabled": "D"}

    # enum for input prestaging
    enum_inputPreStaging = {"use": "1", "notUse": "0"}

    # enum for full chain
    class FullChain(str, enum.Enum):
        Only = "1"
        Require = "2"
        Capable = "3"

    # enum for order input by
    class OrderInputBy(str, enum.Enum):
        eventsAlignment = "1"

    # constructor
    def __init__(self):
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
    def __setattr__(self, name, value):
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
    def copyAttributes(self, oldTaskSpec):
        for attr in self.attributes + ("jobParamsTemplate",):
            if "Time" in attr:
                continue
            if "Date" in attr:
                continue
            if attr in ["progress", "failureRate", "errorDialog", "status", "oldStatus", "lockedBy"]:
                continue
            self.__setattr__(attr, getattr(oldTaskSpec, attr))

    # reset changed attribute list
    def resetChangedList(self):
        object.__setattr__(self, "_changedAttrs", {})

    # reset changed attribute
    def resetChangedAttr(self, name):
        try:
            del self._changedAttrs[name]
        except Exception:
            pass

    # reserve old attributes
    def reserve_old_attributes(self):
        for attName in ["ramCount", "walltime", "cpuTime", "startTime", "cpuTimeUnit", "outDiskCount", "workDiskCount", "ioIntensity", "diskIO"]:
            self.resetChangedAttr(attName)

    # force update
    def forceUpdate(self, name):
        if name in self.attributes:
            self._changedAttrs[name] = getattr(self, name)

    # return map of values
    def valuesMap(self, useSeq=False, onlyChanged=False):
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
    def pack(self, values):
        for i in range(len(self.attributes)):
            attr = self.attributes[i]
            val = values[i]
            object.__setattr__(self, attr, val)

    # return column names for INSERT
    def columnNames(cls, prefix=None):
        ret = ""
        for attr in cls.attributes:
            if prefix is not None:
                ret += f"{prefix}."
            ret += f"{attr},"
        ret = ret[:-1]
        return ret

    columnNames = classmethod(columnNames)

    # return expression of bind variables for INSERT
    def bindValuesExpression(cls, useSeq=True):
        ret = "VALUES("
        for attr in cls.attributes:
            if useSeq and attr in cls._seqAttrMap:
                ret += f"{cls._seqAttrMap[attr]},"
            else:
                ret += f":{attr},"
        ret = ret[:-1]
        ret += ")"
        return ret

    bindValuesExpression = classmethod(bindValuesExpression)

    # return an expression of bind variables for UPDATE to update only changed attributes
    def bindUpdateChangesExpression(self):
        ret = ""
        for attr in self.attributes:
            if attr in self._changedAttrs:
                ret += f"{attr}=:{attr},"
        ret = ret[:-1]
        ret += " "
        return ret

    # check split rule
    def check_split_rule(self, key):
        if self.splitRule is not None:
            if re.search(self.splitRuleToken[key] + r"=(\d+)", self.splitRule):
                return True
        return False

    # get the max size per job if defined
    def getMaxSizePerJob(self):
        if self.splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["nGBPerJob"] + "=(\d+)", self.splitRule)
            if tmpMatch is not None:
                nGBPerJob = int(tmpMatch.group(1)) * 1024 * 1024 * 1024
                return nGBPerJob
        return None

    # remove nGBPerJob
    def removeMaxSizePerJob(self):
        self.removeSplitRule(self.splitRuleToken["nGBPerJob"])

    # get the max size per merge job if defined
    def getMaxSizePerMergeJob(self):
        if self.splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["nGBPerMergeJob"] + "=(\d+)", self.splitRule)
            if tmpMatch is not None:
                nGBPerJob = int(tmpMatch.group(1)) * 1024 * 1024 * 1024
                return nGBPerJob
        return None

    # get the maxnumber of files per job if defined
    def getMaxNumFilesPerJob(self):
        if self.splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["nMaxFilesPerJob"] + "=(\d+)", self.splitRule)
            if tmpMatch is not None:
                return int(tmpMatch.group(1))
        return None

    # set MaxNumFilesPerJob
    def setMaxNumFilesPerJob(self, value):
        self.setSplitRule("nMaxFilesPerJob", value)

    # get the maxnumber of files per merge job if defined
    def getMaxNumFilesPerMergeJob(self):
        if self.splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["nMaxFilesPerMergeJob"] + "=(\d+)", self.splitRule)
            if tmpMatch is not None:
                return int(tmpMatch.group(1))
        return 50

    # get the number of events per merge job if defined
    def getNumEventsPerMergeJob(self):
        if self.splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["nEventsPerMergeJob"] + "=(\d+)", self.splitRule)
            if tmpMatch is not None:
                return int(tmpMatch.group(1))
        return None

    # check if using jumbo
    def usingJumboJobs(self):
        if self.useJumbo in self.enum_useJumbo.values() and self.useJumbo != self.enum_useJumbo["disabled"]:
            return True
        return False

    # get the number of jumbo jobs if defined
    def getNumJumboJobs(self):
        if self.usingJumboJobs() and self.splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["nJumboJobs"] + "=(\d+)", self.splitRule)
            if tmpMatch is not None:
                return int(tmpMatch.group(1))
        return None

    # get the max number of jumbo jobs per site if defined
    def getMaxJumboPerSite(self):
        if self.usingJumboJobs() and self.splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["maxJumboPerSite"] + "=(\d+)", self.splitRule)
            if tmpMatch is not None:
                return int(tmpMatch.group(1))
        return 1

    # get the number of sites per job
    def getNumSitesPerJob(self):
        if not self.useEventService():
            return 1
        if self.splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["nSitesPerJob"] + "=(\d+)", self.splitRule)
            if tmpMatch is not None:
                return int(tmpMatch.group(1))
        return 1

    # get the number of files per job if defined
    def getNumFilesPerJob(self):
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
    def removeNumFilesPerJob(self):
        self.removeSplitRule(self.splitRuleToken["nFilesPerJob"])

    # get the number of files per merge job if defined
    def getNumFilesPerMergeJob(self):
        if self.splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["nFilesPerMergeJob"] + "=(\d+)", self.splitRule)
            if tmpMatch is not None:
                return int(tmpMatch.group(1))
        return None

    # get the number of events per job if defined
    def getNumEventsPerJob(self):
        if self.splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["nEventsPerJob"] + "=(\d+)", self.splitRule)
            if tmpMatch is not None:
                return int(tmpMatch.group(1))
        return None

    # get offset for random seed
    def getRndmSeedOffset(self):
        if self.splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["randomSeed"] + "=(\d+)", self.splitRule)
            if tmpMatch is not None:
                return int(tmpMatch.group(1))
        return 0

    # get offset for first event
    def getFirstEventOffset(self):
        if self.splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["firstEvent"] + "=(\d+)", self.splitRule)
            if tmpMatch is not None:
                return int(tmpMatch.group(1))
        return 0

    # grouping with boundaryID
    def useGroupWithBoundaryID(self):
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
                # * rule for secodary
                # 1 : must have same boundayID. cannot split
                #
                retMap = {}
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
    def useBuild(self):
        return self.check_split_rule("useBuild")

    # use sjob cloning
    def useJobCloning(self):
        return self.check_split_rule("useJobCloning")

    # get job cloning type
    def getJobCloningType(self):
        if self.splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["useJobCloning"] + "=(\d+)", self.splitRule)
            if tmpMatch is not None:
                return tmpMatch.group(1)
        return ""

    # reuse secondary on demand
    def reuseSecOnDemand(self):
        return self.check_split_rule("reuseSecOnDemand")

    # not wait for completion of parent
    def noWaitParent(self):
        return self.check_split_rule("noWaitParent")

    # check splitRule if not wait for completion of parent
    def noWaitParentSL(cls, splitRule):
        if splitRule is not None:
            tmpMatch = re.search(cls.splitRuleToken["noWaitParent"] + "=(\d+)", splitRule)
            if tmpMatch is not None:
                return True
        return False

    noWaitParentSL = classmethod(noWaitParentSL)

    # use only limited sites
    def useLimitedSites(self):
        return self.check_split_rule("limitedSites")

    # set limited sites
    def setLimitedSites(self, policy):
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
    def useLocalIO(self):
        if self.splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["useLocalIO"] + "=(\d+)", self.splitRule)
            if tmpMatch is not None and int(tmpMatch.group(1)):
                return True
        return False

    # use Event Service
    def useEventService(self, siteSpec=None):
        if self.eventService in [1, 2]:
            # check site if ES is disabled
            if self.switchEStoNormal() and siteSpec is not None and siteSpec.getJobSeed() in ["all"]:
                return False
            return True
        return False

    # get the number of events per worker for Event Service
    def getNumEventsPerWorker(self):
        if self.splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["nEventsPerWorker"] + "=(\d+)", self.splitRule)
            if tmpMatch is not None:
                return int(tmpMatch.group(1))
        return None

    # get the number of event service consumers
    def getNumEventServiceConsumer(self):
        if not self.useEventService():
            return None
        if self.splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["nEsConsumers"] + "=(\d+)", self.splitRule)
            if tmpMatch is not None:
                return int(tmpMatch.group(1))
        return None

    # disable automatic retry
    def disableAutoRetry(self):
        return self.check_split_rule("disableAutoRetry")

    # disable reassign
    def disableReassign(self):
        return self.check_split_rule("disableReassign")

    # allow empty input
    def allowEmptyInput(self):
        return self.check_split_rule("allowEmptyInput")

    # use PFN list
    def useListPFN(self):
        return self.check_split_rule("pfnList")

    # set preprocessing
    def setPrePro(self):
        if self.splitRule is None:
            # new
            self.splitRule = self.splitRuleToken["usePrePro"] + "=" + self.enum_toPreProcess
        else:
            # append
            self.splitRule += "," + self.splitRuleToken["usePrePro"] + "=" + self.enum_toPreProcess

    # use preprocessing
    def usePrePro(self):
        if self.splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["usePrePro"] + "=(\d+)", self.splitRule)
            if tmpMatch is not None and tmpMatch.group(1) == self.enum_toPreProcess:
                return True
        return False

    # set preprocessed
    def setPreProcessed(self):
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
    def checkPreProcessed(self):
        if self.splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["usePrePro"] + "=(\d+)", self.splitRule)
            if tmpMatch is not None and tmpMatch.group(1) == self.enum_preProcessed:
                return True
        return False

    # set post preprocess
    def setPostPreProcess(self):
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
    def instantiateTmpl(self):
        return self.check_split_rule("instantiateTmpl")

    # instantiate template datasets at site
    def instantiateTmplSite(self):
        return self.check_split_rule("instantiateTmplSite")

    # merge output
    def mergeOutput(self):
        return self.check_split_rule("mergeOutput")

    # use random seed
    def useRandomSeed(self):
        return self.check_split_rule("randomSeed")

    # get the size of workDisk in bytes
    def getWorkDiskSize(self):
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
    def getOutDiskSize(self):
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
    def outputScaleWithEvents(self):
        if self.outDiskUnit is not None and "PerEvent" in self.outDiskUnit:
            return True
        return False

    # return list of status to update contents
    def statusToUpdateContents(cls):
        return ["defined"]

    statusToUpdateContents = classmethod(statusToUpdateContents)

    # set task status on hold
    def setOnHold(self):
        # change status
        if self.status in ["ready", "running", "merging", "scouting", "defined", "topreprocess", "preprocessing", "registered", "prepared", "rerefine"]:
            self.oldStatus = self.status
            self.status = "pending"

    # return list of status to reject external changes
    def statusToRejectExtChange(cls):
        return ["finished", "done", "prepared", "broken", "tobroken", "aborted", "toabort", "aborting", "failed", "passed"]

    statusToRejectExtChange = classmethod(statusToRejectExtChange)

    # return list of status for retry
    def statusToRetry(cls):
        return ["finished", "failed", "aborted", "exhausted"]

    statusToRetry = classmethod(statusToRetry)

    # return list of status for incexec
    def statusToIncexec(cls):
        return ["done"] + cls.statusToRetry()

    statusToIncexec = classmethod(statusToIncexec)

    # return list of status for reassign
    def statusToReassign(cls):
        return ["registered", "defined", "ready", "running", "scouting", "scouted", "pending", "assigning", "exhausted"]

    statusToReassign = classmethod(statusToReassign)

    # return list of status for Job Generator
    def statusForJobGenerator(cls):
        return ["ready", "running", "scouting", "topreprocess", "preprocessing"]

    statusForJobGenerator = classmethod(statusForJobGenerator)

    # return list of status to not pause
    def statusNotToPause(cls):
        return ["finished", "failed", "done", "aborted", "broken", "paused"]

    statusNotToPause = classmethod(statusNotToPause)

    # return mapping of command and status
    def commandStatusMap(cls):
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

    commandStatusMap = classmethod(commandStatusMap)

    # qualifiers for retry command
    def get_retry_command_qualifiers(
        cls,
        no_child_retry: bool = False,
        discard_events: bool = False,
        disable_staging_mode: bool = False,
        keep_gshare_priority: bool = False,
        ignore_hard_exhausted: bool = False,
    ) -> list:
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

    get_retry_command_qualifiers = classmethod(get_retry_command_qualifiers)

    # set error dialog
    def setErrDiag(self, diag, append=False, prepend=False):
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
    def useLoadXML(self):
        return self.check_split_rule("loadXML")

    # make VOMS FQANs
    def makeFQANs(self):
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
    def setSplitRule(self, ruleName, ruleValue):
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
    def removeSplitRule(self, ruleName):
        if self.splitRule is not None:
            items = self.splitRule.split(",")
            newItems = []
            for item in items:
                # remove rile
                tmpRuleName = item.split("=")[0]
                if ruleName == tmpRuleName:
                    continue
                newItems.append(item)
            self.splitRule = ",".join(newItems)

    # set to use scout
    def setUseScout(self, useFlag):
        if useFlag:
            self.setSplitRule("useScout", self.enum_useScout)
        else:
            self.setSplitRule("useScout", self.enum_noScout)

    # set post scout
    def setPostScout(self):
        self.setSplitRule("useScout", self.enum_postScout)

    # use scout
    def useScout(self, splitRule=None):
        if splitRule is None:
            splitRule = self.splitRule
        if splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["useScout"] + "=(\d+)", splitRule)
            if tmpMatch is not None and tmpMatch.group(1) == self.enum_useScout:
                return True
        return False

    # use exhausted
    def useExhausted(self):
        return self.check_split_rule("useExhausted")

    # use real number of events
    def useRealNumEvents(self):
        return self.check_split_rule("useRealNumEvents")

    # use input LFN as source for output LFN
    def useFileAsSourceLFN(self):
        return self.check_split_rule("useFileAsSourceLFN")

    # post scout
    def isPostScout(self):
        if self.splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["useScout"] + "=(\d+)", self.splitRule)
            if tmpMatch is not None and tmpMatch.group(1) == self.enum_postScout:
                return True
        return False

    # wait until input shows up
    def waitInput(self):
        return self.check_split_rule("waitInput")

    # input prestaging
    def inputPreStaging(self):
        if self.splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["inputPreStaging"] + "=" + self.enum_inputPreStaging["use"], self.splitRule)
            if tmpMatch is not None:
                return True
        return False

    # set DDM backend
    def setDdmBackEnd(self, backEnd):
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
    def getDdmBackEnd(self):
        if self.splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["ddmBackEnd"] + "=([^,$]+)", self.splitRule)
            if tmpMatch is not None:
                return tmpMatch.group(1)
        return None

    # get field number to add middle name to LFN
    def getFieldNumToLFN(self):
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
    def getScoutSuccessRate(self):
        if self.splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["scoutSuccessRate"] + "=(\d+)", self.splitRule)
            if tmpMatch is not None:
                return int(tmpMatch.group(1))
        return None

    # get T1 weight
    def getT1Weight(self):
        if self.splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["t1Weight"] + "=(-*\d+)", self.splitRule)
            if tmpMatch is not None:
                return int(tmpMatch.group(1))
        return 0

    # respect Lumiblock boundaries
    def respectLumiblock(self):
        return self.check_split_rule("respectLB")

    # release files per Lumiblock
    def releasePerLumiblock(self):
        return self.check_split_rule("releasePerLB")

    # order by Lumiblock numbers
    def orderByLB(self):
        return self.check_split_rule("orderByLB")

    # respect split rule
    def respectSplitRule(self):
        return self.check_split_rule("respectSplitRule")

    # allow partial finish
    def allowPartialFinish(self):
        return self.check_split_rule("allowPartialFinish")

    # check if datasets should be registered
    def toRegisterDatasets(self):
        if self.splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["registerDatasets"] + "=(\d+)", self.splitRule)
            if tmpMatch is not None and tmpMatch.group(1) == self.enum_toRegisterDS:
                return True
        return False

    # datasets were registered
    def registeredDatasets(self):
        self.setSplitRule("registerDatasets", self.enum_registeredDS)

    # set datasets to be registered
    def setToRegisterDatasets(self):
        self.setSplitRule("registerDatasets", self.enum_toRegisterDS)

    # get the max number of attempts for ES events
    def getMaxAttemptES(self):
        if self.splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["maxAttemptES"] + "=(\d+)", self.splitRule)
            if tmpMatch is not None:
                return int(tmpMatch.group(1))
        return None

    # get the max number of attempts for ES jobs
    def getMaxAttemptEsJob(self):
        if self.splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["maxAttemptEsJob"] + "=(\d+)", self.splitRule)
            if tmpMatch is not None:
                return int(tmpMatch.group(1))
        return self.getMaxAttemptES()

    # check attribute length
    def checkAttrLength(self):
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
    def setIpConnectivity(self, value):
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
    def getIpConnectivity(self):
        if self.splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["ipConnectivity"] + "=(\d+)", self.splitRule)
            if tmpMatch is not None:
                return self.enum_ipConnectivity[tmpMatch.group(1)]
        return None

    # get IP connectivity
    def getIpStack(self):
        if self.splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["ipStack"] + "=(\d+)", self.splitRule)
            if tmpMatch is not None:
                return self.enum_ipStack[tmpMatch.group(1)]
        return None

    # use HS06 for walltime estimation
    def useHS06(self):
        return self.cpuTimeUnit in ["HS06sPerEvent", "HS06sPerEventFixed", "mHS06sPerEvent", "mHS06sPerEventFixed"]

    # get CPU time in sec
    def getCpuTime(self):
        if not self.useHS06():
            return None
        try:
            if self.cpuTimeUnit.startswith("m"):
                return float(self.cpuTime) / 1000.0
        except Exception:
            pass
        return self.cpuTime

    # RAM scales with nCores
    def ramPerCore(self):
        return self.ramUnit in ["MBPerCore", "MBPerCoreFixed"]

    # run until input is closed
    def runUntilClosed(self):
        return self.check_split_rule("runUntilClosed")

    # stay output on site
    def stayOutputOnSite(self):
        return self.check_split_rule("stayOutputOnSite")

    # fail when goal unreached
    def failGoalUnreached(self):
        return self.check_split_rule("failGoalUnreached")

    # unset fail when goal unreached
    def unsetFailGoalUnreached(self):
        self.removeSplitRule(self.splitRuleToken["failGoalUnreached"])

    # switch ES to normal when jobs land at normal sites
    def switchEStoNormal(self):
        return self.check_split_rule("switchEStoNormal")

    # use world cloud
    def useWorldCloud(self):
        return self.cloud == self.worldCloudName

    # dynamic number of events
    def dynamicNumEvents(self):
        if self.splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["dynamicNumEvents"] + "=(\d+)", self.splitRule)
            if tmpMatch is not None:
                return True
        return False

    # get min granularity for dynamic number of events
    def get_min_granularity(self):
        if self.splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["dynamicNumEvents"] + "=(\d+)", self.splitRule)
            if tmpMatch is not None:
                return int(tmpMatch.group(1))
        return None

    # set alternative stage-out
    def setAltStageOut(self, value):
        if value in self.enum_altStageOut.values():
            for tmpKey, tmpVal in self.enum_altStageOut.items():
                if value == tmpVal:
                    self.setSplitRule("altStageOut", tmpKey)
                    break

    # get alternative stage-out
    def getAltStageOut(self):
        if self.splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["altStageOut"] + "=(\d+)", self.splitRule)
            if tmpMatch is not None:
                return self.enum_altStageOut[tmpMatch.group(1)]
        return None

    # allow WAN for input access
    def allowInputWAN(self):
        return self.check_split_rule("allowInputWAN")

    # set mode for input LAN access
    def setAllowInputLAN(self, value):
        if value in self.enum_inputLAN.values():
            for tmpKey, tmpVal in self.enum_inputLAN.items():
                if value == tmpVal:
                    self.setSplitRule("allowInputLAN", tmpKey)
                    break

    # check if LAN is used for input access
    def allowInputLAN(self):
        if self.splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["allowInputLAN"] + "=(\d+)", self.splitRule)
            if tmpMatch is not None:
                return self.enum_inputLAN[tmpMatch.group(1)]
        return None

    # put log files to OS
    def putLogToOS(self):
        return self.check_split_rule("putLogToOS")

    # merge ES on Object Store
    def mergeEsOnOS(self):
        return self.check_split_rule("mergeEsOnOS")

    # write input to file
    def writeInputToFile(self):
        return self.check_split_rule("writeInputToFile")

    # ignore missing input datasets
    def ignoreMissingInDS(self):
        return self.check_split_rule("ignoreMissingInDS")

    # suppress execute string conversion
    def noExecStrCnv(self):
        return self.check_split_rule("noExecStrCnv")

    # in-file positional event number
    def inFilePosEvtNum(self):
        return self.check_split_rule("inFilePosEvtNum")

    # register event service files
    def registerEsFiles(self):
        return self.check_split_rule("registerEsFiles")

    # disable auto finish
    def disableAutoFinish(self):
        return self.check_split_rule("disableAutoFinish")

    # reset refined attributes which may confuse they system
    def resetRefinedAttrs(self):
        self.resetChangedAttr("splitRule")
        self.resetChangedAttr("eventService")
        self.reserve_old_attributes()

    # resurrect consumers
    def resurrectConsumers(self):
        return self.check_split_rule("resurrectConsumers")

    # use prefetcher
    def usePrefetcher(self):
        return self.check_split_rule("usePrefetcher")

    # no input pooling
    def noInputPooling(self):
        return self.check_split_rule("noInputPooling")

    # get num of input chunks to wait
    def nChunksToWait(self):
        if self.splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["nChunksToWait"] + "=(\d+)", self.splitRule)
            if tmpMatch is not None:
                return int(tmpMatch.group(1))
        return None

    # get max walltime
    def getMaxWalltime(self):
        if self.splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["maxWalltime"] + "=(\d+)", self.splitRule)
            if tmpMatch is not None:
                return int(tmpMatch.group(1)) * 60 * 60
        return None

    # set max walltime
    def set_max_walltime(self, value):
        self.setSplitRule("maxWalltime", str(value))

    # get target size of the largest output to reset NG
    def getTgtMaxOutputForNG(self):
        if self.splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["tgtMaxOutputForNG"] + "=(\d+)", self.splitRule)
            if tmpMatch is not None:
                return int(tmpMatch.group(1))
        return None

    # not discard events
    def notDiscardEvents(self):
        return self.check_split_rule("notDiscardEvents")

    # get min CPU efficiency
    def getMinCpuEfficiency(self):
        if self.splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["minCpuEfficiency"] + "=(\d+)", self.splitRule)
            if tmpMatch is not None:
                return int(tmpMatch.group(1))
        return None

    # decrement attemptNr of events only when failed
    def decAttOnFailedES(self):
        return self.check_split_rule("decAttOnFailedES")

    # use zip files to pin input files
    def useZipToPin(self):
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
    def reformat_architecture(self):
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
        new_dict = {}
        val = self.get_sw_platform()
        if val:
            new_dict["sw_platform"] = val
        val = self.get_base_platform(encoded_platform)
        if val:
            new_dict["base_platform"] = val
        val = self.get_host_cpu_spec(encoded_platform)
        if val:
            # remove wildcard entries and empty specs
            l = [x for x in [{k: v for k, v in d.items() if v != "*"} for d in val] if x]
            if l:
                new_dict["cpu_specs"] = l
        val = self.get_host_gpu_spec()
        if val:
            # remove wildcard entries and empty specs
            l = {k: v for k, v in val.items() if v != "*"}
            if l:
                new_dict["gpu_spec"] = l
        self.architecture = json.dumps(new_dict)

    # get SW platform
    def get_sw_platform(self):
        try:
            d = json.loads(self.architecture)
            return d.get("sw_platform", "")
        except Exception:
            pass
        if self.architecture is not None:
            m = re.search("^([^@#&]*)", self.architecture)
            if m:
                return m.group(1)
        return self.architecture

    # get base platform
    def get_base_platform(self, encoded_platform=None):
        try:
            d = json.loads(self.architecture)
            val = d.get("base_platform", None)
            if val is not None or encoded_platform is None:
                return val
        except Exception:
            pass
        if encoded_platform:
            architecture = encoded_platform
        else:
            architecture = self.architecture
        if architecture is None or "@" not in architecture:
            return None
        m = re.search("@([^#&]*)", architecture)
        img = m.group(1)
        if img == "":
            img = None
        return img

    # get platforms
    def get_platforms(self):
        if self.architecture is not None:
            platform = self.get_sw_platform()
            base = self.get_base_platform()
            if base:
                platform += "@" + base
            return platform
        return self.architecture

    # get host CPU spec
    def get_host_cpu_spec(self, encoded_platform=None):
        try:
            d = json.loads(self.architecture)
            specs = d.get("cpu_specs", None)
            if not specs and encoded_platform is None:
                return None
            else:
                for spec in specs:
                    spec.setdefault("vendor", "*")
                    spec.setdefault("instr", "*")
                return specs
        except Exception:
            pass
        if encoded_platform:
            architecture = encoded_platform
        else:
            architecture = self.architecture
        try:
            if not architecture:
                return None
            if "#" not in architecture:
                if re.search(r"^[\^@&]", architecture):
                    return None
                arch = architecture.split("-")[0]
                if arch:
                    return [{"arch": arch, "vendor": "*", "instr": "*"}]
                return None
            m = re.search(r"#([^\^@&]*)", architecture)
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

    # get host GPU spec
    def get_host_gpu_spec(self):
        try:
            d = json.loads(self.architecture)
            spec = d.get("gpu_spec", None)
            spec.setdefault("vendor", "*")
            spec.setdefault("model", "*")
            return spec
        except Exception:
            pass
        try:
            if self.architecture is None or "&" not in self.architecture:
                return None
            m = re.search(r"&([^\^@#]*)", self.architecture)
            spec_str = m.group(1)
            if not spec_str:
                return None
            spec_str += "-*" * (1 - spec_str.count("-"))
            items = spec_str.split("-")
            spec = {"vendor": items[0], "model": items[1]}
            return spec
        except Exception:
            return None

    # HPO workflow
    def is_hpo_workflow(self):
        return self.check_split_rule("hpoWorkflow")

    # debug mode
    def is_debug_mode(self):
        return self.check_split_rule("debugMode")

    # multi-step execution
    def is_multi_step_exec(self):
        return self.check_split_rule("multiStepExec")

    # get max number of jobs
    def get_max_num_jobs(self):
        if self.splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["maxNumJobs"] + "=(\d+)", self.splitRule)
            if tmpMatch is not None:
                return int(tmpMatch.group(1))
        return None

    # get total number of jobs
    def get_total_num_jobs(self):
        if self.splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["totNumJobs"] + "=(\d+)", self.splitRule)
            if tmpMatch is not None:
                return int(tmpMatch.group(1))
        return None

    # use only tags for fat container
    def use_only_tags_fc(self):
        return self.check_split_rule("onlyTagsForFC")

    # avoid VP
    def avoid_vp(self):
        return self.check_split_rule("avoidVP")

    # set first contents feed
    def set_first_contents_feed(self, is_first):
        if is_first:
            self.setSplitRule("firstContentsFeed", self.FirstContentsFeed.TRUE.value)
        else:
            self.setSplitRule("firstContentsFeed", self.FirstContentsFeed.FALSE.value)

    # check if first contents feed
    def is_first_contents_feed(self):
        if self.splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["firstContentsFeed"] + "=(\d+)", self.splitRule)
            if tmpMatch is not None and tmpMatch.group(1) == self.FirstContentsFeed.TRUE.value:
                return True
        return False

    # check if work is segmented
    def is_work_segmented(self):
        return self.check_split_rule("segmentedWork")

    # check if looping check is disabled
    def no_looping_check(self):
        return self.check_split_rule("noLoopingCheck")

    # encode job parameters
    def encode_job_params(self):
        return self.check_split_rule("encJobParams")

    # get original error dialog
    def get_original_error_dialog(self):
        if not self.origErrorDialog:
            return ""
        # remove log URL
        tmpStr = re.sub("<a href.+</a> : ", "", self.origErrorDialog)
        return tmpStr.split(". ")[-1]

    # check if secrets are used
    def use_secrets(self):
        return self.check_split_rule("useSecrets")

    # get max core count
    def get_max_core_count(self):
        if self.splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["maxCoreCount"] + "=(\d+)", self.splitRule)
            if tmpMatch is not None:
                return int(tmpMatch.group(1))
        return None

    # push status changes
    def push_status_changes(self):
        return push_status_changes(self.splitRule)

    # use cloud as VO
    def cloud_as_vo(self):
        return self.check_split_rule("cloudAsVO")

    # push job
    def push_job(self):
        return self.check_split_rule("pushJob")

    # fine-grained process
    def is_fine_grained_process(self):
        return self.check_split_rule("fineGrainedProc")

    # on site merging
    def on_site_merging(self):
        return self.check_split_rule("onSiteMerging")

    # set full chain flag
    def set_full_chain(self, mode):
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
    def get_full_chain(self):
        if self.splitRule:
            tmpMatch = re.search(self.splitRuleToken["fullChain"] + r"=(\d+)", self.splitRule)
            if tmpMatch:
                return tmpMatch.group(1)
        return None

    # check full chain with mode
    def check_full_chain_with_mode(self, mode):
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
    def check_full_chain_with_nucleus(self, nucleus):
        if self.get_full_chain() and nucleus.get_bare_nucleus_mode():
            return True
        return False

    # get RAM for retry
    def get_ram_for_retry(self, current_ram):
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
        if not current_ram:
            return current_ram
        if current_ram < offset:
            return offset
        if not step:
            return current_ram
        return offset + math.ceil((current_ram - offset) / step) * step

    # get number of events per input
    def get_num_events_per_input(self):
        if self.splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["nEventsPerInput"] + "=(\d+)", self.splitRule)
            if tmpMatch is not None:
                return int(tmpMatch.group(1))
        return None

    def get_max_events_per_job(self):
        if self.splitRule is not None:
            tmpMatch = re.search(self.splitRuleToken["maxEventsPerJob"] + "=(\d+)", self.splitRule)
            if tmpMatch is not None:
                return int(tmpMatch.group(1))
        return None

    # set order input by
    def set_order_input_by(self, mode):
        var = None
        if mode == "eventsAlignment":
            var = self.OrderInputBy.eventsAlignment
        if var:
            self.setSplitRule("orderInputBy", var)

    # get full chain flag
    def order_input_by(self):
        if self.splitRule:
            tmpMatch = re.search(self.splitRuleToken["orderInputBy"] + r"=(\d+)", self.splitRule)
            if tmpMatch:
                if tmpMatch.group(1) == self.OrderInputBy.eventsAlignment:
                    return "eventsAlignment"
        return None

    # check if intermediate task
    def is_intermediate_task(self):
        return self.check_split_rule("intermediateTask")

    # check if message driven
    def is_msg_driven(self):
        return is_msg_driven(self.splitRule)

    # check if incomplete input datasets are allowed
    def allow_incomplete_input(self):
        return self.check_split_rule("allowIncompleteInDS")

    # get queued time
    def get_queued_time(self):
        """
        Get queued time in timestamp
        :return: queued time in timestamp. None if not set
        """
        if self.queuedTime is None:
            return None
        return self.queuedTime.timestamp()


# utils


# check split rule with positive integer
def check_split_rule_positive_int(key, split_rule):
    if not split_rule:
        return False
    tmpMatch = re.search(JediTaskSpec.splitRuleToken[key] + r"=(\d+)", split_rule)
    if not tmpMatch or int(tmpMatch.group(1)) <= 0:
        return False
    return True


# check if push status changes without class instance
def push_status_changes(split_rule):
    return check_split_rule_positive_int("pushStatusChanges", split_rule)


# check if message driven without class instance
def is_msg_driven(split_rule):
    return check_split_rule_positive_int("messageDriven", split_rule)


# check if auto pause is disabled
def is_auto_pause_disabled(split_rule):
    return not check_split_rule_positive_int("noAutoPause", split_rule)
