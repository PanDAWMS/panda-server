"""
job specification

"""

import datetime
import json
import re
from typing import Any, Sequence

from pandacommon.pandautils.PandaUtils import naive_utcnow

from pandaserver.taskbuffer.FileSpec import FileSpec
from pandaserver.taskbuffer.spec_column import SpecColumn

reserveChangedState = False


class JobSpec(object):
    # attributes
    _attributes = (
        "PandaID",
        "jobDefinitionID",
        "schedulerID",
        "pilotID",
        "creationTime",
        "creationHost",
        "modificationTime",
        "modificationHost",
        "AtlasRelease",
        "transformation",
        "homepackage",
        "prodSeriesLabel",
        "prodSourceLabel",
        "prodUserID",
        "assignedPriority",
        "currentPriority",
        "attemptNr",
        "maxAttempt",
        "jobStatus",
        "jobName",
        "maxCpuCount",
        "maxCpuUnit",
        "maxDiskCount",
        "maxDiskUnit",
        "ipConnectivity",
        "minRamCount",
        "minRamUnit",
        "startTime",
        "endTime",
        "cpuConsumptionTime",
        "cpuConsumptionUnit",
        "commandToPilot",
        "transExitCode",
        "pilotErrorCode",
        "pilotErrorDiag",
        "exeErrorCode",
        "exeErrorDiag",
        "supErrorCode",
        "supErrorDiag",
        "ddmErrorCode",
        "ddmErrorDiag",
        "brokerageErrorCode",
        "brokerageErrorDiag",
        "jobDispatcherErrorCode",
        "jobDispatcherErrorDiag",
        "taskBufferErrorCode",
        "taskBufferErrorDiag",
        "computingSite",
        "computingElement",
        "jobParameters",
        "metadata",
        "prodDBlock",
        "dispatchDBlock",
        "destinationDBlock",
        "destinationSE",
        "nEvents",
        "grid",
        "cloud",
        "cpuConversion",
        "sourceSite",
        "destinationSite",
        "transferType",
        "taskID",
        "cmtConfig",
        "stateChangeTime",
        "prodDBUpdateTime",
        "lockedby",
        "relocationFlag",
        "jobExecutionID",
        "VO",
        "pilotTiming",
        "workingGroup",
        "processingType",
        "prodUserName",
        "nInputFiles",
        "countryGroup",
        "batchID",
        "parentID",
        "specialHandling",
        "jobsetID",
        "coreCount",
        "nInputDataFiles",
        "inputFileType",
        "inputFileProject",
        "inputFileBytes",
        "nOutputDataFiles",
        "outputFileBytes",
        "jobMetrics",
        "workQueue_ID",
        "jediTaskID",
        "jobSubStatus",
        "actualCoreCount",
        "reqID",
        "maxRSS",
        "maxVMEM",
        "maxSWAP",
        "maxPSS",
        "avgRSS",
        "avgVMEM",
        "avgSWAP",
        "avgPSS",
        "maxWalltime",
        "nucleus",
        "eventService",
        "failedAttempt",
        "hs06sec",
        "gshare",
        "hs06",
        "totRCHAR",
        "totWCHAR",
        "totRBYTES",
        "totWBYTES",
        "rateRCHAR",
        "rateWCHAR",
        "rateRBYTES",
        "rateWBYTES",
        "resource_type",
        "diskIO",
        "memory_leak",
        "memory_leak_x2",
        "container_name",
        "job_label",
        "gco2_regional",
        "gco2_global",
        "cpu_architecture_level",
        "outputFileType",
    )

    # Column types, taken from the Oracle schema of ATLAS_PANDA.JOBSACTIVE4 (panda-database
    # repo, schema/oracle). The columns are installed by __init__ via setattr, so a type
    # checker sees none of them without these declarations. They carry no value: this
    # class uses __slots__ built from an expression, and a value here would raise at
    # import time.
    #
    # SpecColumn is not decoration. __getattribute__ below substitutes the string "NULL"
    # for a column that is still None, so a read yields either the column type or that
    # sentinel -- which is what makes `spec.numberfiles + 1` a latent TypeError -- while a
    # write takes the column type or None. See spec_column.py.
    PandaID: SpecColumn[int]
    jobDefinitionID: SpecColumn[int]
    schedulerID: SpecColumn[str]
    pilotID: SpecColumn[str]
    creationTime: SpecColumn[datetime.datetime]
    creationHost: SpecColumn[str]
    modificationTime: SpecColumn[datetime.datetime]
    modificationHost: SpecColumn[str]
    AtlasRelease: SpecColumn[str]
    transformation: SpecColumn[str]
    homepackage: SpecColumn[str]
    prodSeriesLabel: SpecColumn[str]
    prodSourceLabel: SpecColumn[str]
    prodUserID: SpecColumn[str]
    assignedPriority: SpecColumn[int]
    currentPriority: SpecColumn[int]
    attemptNr: SpecColumn[int]
    maxAttempt: SpecColumn[int]
    jobStatus: SpecColumn[str]
    jobName: SpecColumn[str]
    maxCpuCount: SpecColumn[int]
    maxCpuUnit: SpecColumn[str]
    maxDiskCount: SpecColumn[int]
    maxDiskUnit: SpecColumn[str]
    ipConnectivity: SpecColumn[str]
    minRamCount: SpecColumn[int]
    minRamUnit: SpecColumn[str]
    startTime: SpecColumn[datetime.datetime]
    endTime: SpecColumn[datetime.datetime]
    cpuConsumptionTime: SpecColumn[int]
    cpuConsumptionUnit: SpecColumn[str]
    commandToPilot: SpecColumn[str]
    transExitCode: SpecColumn[str]
    pilotErrorCode: SpecColumn[int]
    pilotErrorDiag: SpecColumn[str]
    exeErrorCode: SpecColumn[int]
    exeErrorDiag: SpecColumn[str]
    supErrorCode: SpecColumn[int]
    supErrorDiag: SpecColumn[str]
    ddmErrorCode: SpecColumn[int]
    ddmErrorDiag: SpecColumn[str]
    brokerageErrorCode: SpecColumn[int]
    brokerageErrorDiag: SpecColumn[str]
    jobDispatcherErrorCode: SpecColumn[int]
    jobDispatcherErrorDiag: SpecColumn[str]
    taskBufferErrorCode: SpecColumn[int]
    taskBufferErrorDiag: SpecColumn[str]
    computingSite: SpecColumn[str]
    computingElement: SpecColumn[str]
    jobParameters: SpecColumn[str]
    # a CLOB on the way to and from the DB, but set_task_attribute() and the ES merge
    # bookkeeping keep a list in it while the job is in memory, so the read type is Any
    metadata: SpecColumn[Any]
    prodDBlock: SpecColumn[str]
    dispatchDBlock: SpecColumn[str]
    destinationDBlock: SpecColumn[str]
    destinationSE: SpecColumn[str]
    nEvents: SpecColumn[int]
    grid: SpecColumn[str]
    cloud: SpecColumn[str]
    cpuConversion: SpecColumn[float]
    sourceSite: SpecColumn[str]
    destinationSite: SpecColumn[str]
    transferType: SpecColumn[str]
    taskID: SpecColumn[int]
    cmtConfig: SpecColumn[str]
    stateChangeTime: SpecColumn[datetime.datetime]
    prodDBUpdateTime: SpecColumn[datetime.datetime]
    lockedby: SpecColumn[str]
    relocationFlag: SpecColumn[int]
    jobExecutionID: SpecColumn[int]
    VO: SpecColumn[str]
    pilotTiming: SpecColumn[str]
    workingGroup: SpecColumn[str]
    processingType: SpecColumn[str]
    prodUserName: SpecColumn[str]
    nInputFiles: SpecColumn[int]
    countryGroup: SpecColumn[str]
    batchID: SpecColumn[str]
    parentID: SpecColumn[int]
    specialHandling: SpecColumn[str]
    jobsetID: SpecColumn[int]
    coreCount: SpecColumn[int]
    nInputDataFiles: SpecColumn[int]
    inputFileType: SpecColumn[str]
    inputFileProject: SpecColumn[str]
    inputFileBytes: SpecColumn[int]
    nOutputDataFiles: SpecColumn[int]
    outputFileBytes: SpecColumn[int]
    jobMetrics: SpecColumn[str]
    workQueue_ID: SpecColumn[int]
    jediTaskID: SpecColumn[int]
    jobSubStatus: SpecColumn[str]
    actualCoreCount: SpecColumn[int]
    reqID: SpecColumn[int]
    maxRSS: SpecColumn[int]
    maxVMEM: SpecColumn[int]
    maxSWAP: SpecColumn[int]
    maxPSS: SpecColumn[int]
    avgRSS: SpecColumn[int]
    avgVMEM: SpecColumn[int]
    avgSWAP: SpecColumn[int]
    avgPSS: SpecColumn[int]
    maxWalltime: SpecColumn[int]
    nucleus: SpecColumn[str]
    eventService: SpecColumn[int]
    failedAttempt: SpecColumn[int]
    hs06sec: SpecColumn[int]
    gshare: SpecColumn[str]
    hs06: SpecColumn[int]
    totRCHAR: SpecColumn[int]
    totWCHAR: SpecColumn[int]
    totRBYTES: SpecColumn[int]
    totWBYTES: SpecColumn[int]
    rateRCHAR: SpecColumn[int]
    rateWCHAR: SpecColumn[int]
    rateRBYTES: SpecColumn[int]
    rateWBYTES: SpecColumn[int]
    resource_type: SpecColumn[str]
    diskIO: SpecColumn[int]
    memory_leak: SpecColumn[int]
    memory_leak_x2: SpecColumn[float]
    container_name: SpecColumn[str]
    job_label: SpecColumn[str]
    gco2_regional: SpecColumn[float]
    gco2_global: SpecColumn[float]
    cpu_architecture_level: SpecColumn[str]
    outputFileType: SpecColumn[str]
    # the file list this spec carries. Declared here rather than only in __slots__
    # below, since __slots__ is built from an expression and type checkers cannot
    # see the names it adds
    Files: list[FileSpec]
    # slots
    __slots__ = _attributes + ("Files", "_changedAttrs", "_reserveChangedState")
    # attributes which have 0 by default
    _zeroAttrs = (
        "assignedPriority",
        "currentPriority",
        "attemptNr",
        "maxAttempt",
        "maxCpuCount",
        "maxDiskCount",
        "minRamCount",
        "cpuConsumptionTime",
        "pilotErrorCode",
        "exeErrorCode",
        "supErrorCode",
        "ddmErrorCode",
        "brokerageErrorCode",
        "jobDispatcherErrorCode",
        "taskBufferErrorCode",
        "nEvents",
        "relocationFlag",
        "jobExecutionID",
        "nOutputDataFiles",
        "outputFileBytes",
    )
    # attribute to be suppressed. They are in another table
    _suppAttrs = ("jobParameters", "metadata")
    # mapping between sequence and attr
    _seqAttrMap = {"PandaID": "ATLAS_PANDA.JOBSDEFINED4_PANDAID_SEQ.nextval"}
    # limit length
    _limitLength = {
        "ddmErrorDiag": 500,
        "taskBufferErrorDiag": 300,
        "jobDispatcherErrorDiag": 250,
        "brokerageErrorDiag": 250,
        "pilotErrorDiag": 500,
        "exeErrorDiag": 500,
        "jobSubStatus": 80,
        "supErrorDiag": 250,
        "commandToPilot": 250,
        "inputFileType": 32,
        "outputFileType": 32,
        "grid": 50,
        "sourceSite": 36,
        "destinationSite": 36,
    }
    # tag for special handling
    _tagForSH = {
        "altStgOut": "ao",
        "acceptPartial": "ap",
        "allOkEvents": "at",
        "buildJob": "bj",
        "notDiscardEvents": "de",
        "decAttOnFailedES": "df",
        "debugMode": "dm",
        "dynamicNumEvents": "dy",
        "encJobParams": "ej",
        "fakeJobToIgnore": "fake",
        "homeCloud": "hc",
        "hpoWorkflow": "ho",
        "inFilePosEvtNum": "if",
        "inputPrestaging": "ip",
        "lumiBlock": "lb",
        "noLoopingCheck": "lc",
        "mergeAtOs": "mo",
        "noExecStrCnv": "nc",
        "onSiteMerging": "om",
        "pushStatusChanges": "pc",
        "pushJob": "pj",
        "pmergeJob": "pm",
        "putLogToOS": "po",
        "registerEsFiles": "re",
        "retryRam": "rr",
        "resurrectConsumers": "rs",
        "requestType": "rt",
        "jobCloning": "sc",
        "scoutJob": "sj",
        "taskQueuedTime": "tq",
        "usePrefetcher": "up",
        "useSecrets": "us",
        "useZipToPin": "uz",
        "writeInputToFile": "wf",
    }

    # constructor
    def __init__(self) -> None:
        # install attributes
        for attr in self._attributes:
            object.__setattr__(self, attr, None)
        # files list
        object.__setattr__(self, "Files", [])
        # map of changed attributes
        object.__setattr__(self, "_changedAttrs", {})
        # reserve changed state at instance level
        object.__setattr__(self, "_reserveChangedState", False)

    # override __getattribute__ for SQL
    def __getattribute__(self, name: str) -> Any:
        ret = object.__getattribute__(self, name)
        if ret is None:
            return "NULL"
        return ret

    # override __setattr__ to collect the changed attributes
    def __setattr__(self, name: str, value: Any) -> None:
        oldVal = getattr(self, name)
        object.__setattr__(self, name, value)
        newVal = getattr(self, name)
        if name == "jobStatus":
            if oldVal != newVal:
                self.stateChangeTime = naive_utcnow()
        # collect changed attributes
        if oldVal != newVal and name not in self._suppAttrs:
            self._changedAttrs[name] = value

    # reset changed attribute list
    def resetChangedList(self) -> None:
        object.__setattr__(self, "_changedAttrs", {})

    # add File to files list
    def addFile(self, file: FileSpec) -> None:
        # set owner
        file.setOwner(self)
        # append
        self.Files.append(file)

    # pack tuple into JobSpec
    def pack(self, values: Sequence[Any]) -> None:
        for i in range(len(self._attributes)):
            attr = self._attributes[i]
            val = values[i]
            object.__setattr__(self, attr, val)

    # return a tuple of values
    def values(self) -> tuple[Any, ...]:
        ret = []
        for attr in self._attributes:
            val = getattr(self, attr)
            ret.append(val)
        return tuple(ret)

    # return map of values
    def valuesMap(self, useSeq: bool = False, onlyChanged: bool = False) -> dict[str, Any]:
        ret = {}
        for attr in self._attributes:
            if useSeq and attr in self._seqAttrMap:
                continue
            if onlyChanged:
                if attr not in self._changedAttrs:
                    continue
            val = getattr(self, attr)
            if val == "NULL":
                if attr in self._zeroAttrs:
                    val = 0
                else:
                    val = None
            # jobParameters/metadata go to another table
            if attr in self._suppAttrs:
                val = None
            # truncate too long values
            if attr in self._limitLength:
                if val is not None:
                    val = val[: self._limitLength[attr]]
            ret[f":{attr}"] = val
        return ret

    # return state values to be pickled
    def __getstate__(self) -> list[Any]:
        state = []
        for attr in self._attributes:
            val = getattr(self, attr)
            state.append(val)
        if reserveChangedState or self._reserveChangedState:
            state.append(self._changedAttrs)
        # append File info
        state.append(self.Files)
        return state

    # restore state from the unpickled state values
    def __setstate__(self, state: list[Any]) -> None:
        for i in range(len(self._attributes)):
            # schema evolution is supported only when adding attributes
            if i + 1 < len(state):
                object.__setattr__(self, self._attributes[i], state[i])
            else:
                object.__setattr__(self, self._attributes[i], "NULL")
        object.__setattr__(self, "Files", state[-1])
        if not hasattr(self, "_reserveChangedState"):
            object.__setattr__(self, "_reserveChangedState", False)
        if reserveChangedState or self._reserveChangedState:
            object.__setattr__(self, "_changedAttrs", state[-2])
        else:
            object.__setattr__(self, "_changedAttrs", {})

    # return column names for INSERT or full SELECT
    @classmethod
    def columnNames(cls) -> str:
        ret = ""
        for attr in cls._attributes:
            if ret != "":
                ret += ","
            ret += attr
        return ret

    # return expression of values for INSERT
    @classmethod
    def valuesExpression(cls) -> str:
        ret = "VALUES("
        for attr in cls._attributes:
            ret += "%s"
            if attr != cls._attributes[len(cls._attributes) - 1]:
                ret += ","
        ret += ")"
        return ret

    # return expression of bind values for INSERT
    @classmethod
    def bindValuesExpression(cls, useSeq: bool = False) -> str:
        from pandaserver.config import panda_config

        ret = "VALUES("
        for attr in cls._attributes:
            if useSeq and attr in cls._seqAttrMap:
                if panda_config.backend == "mysql":
                    # mysql
                    ret += "NULL,"
                else:
                    # oracle
                    ret += f"{cls._seqAttrMap[attr]},"
            else:
                ret += f":{attr},"
        ret = ret[:-1]
        ret += ")"
        return ret

    # return an expression for UPDATE
    @classmethod
    def updateExpression(cls) -> str:
        ret = ""
        for attr in cls._attributes:
            ret = ret + attr + "=%s"
            if attr != cls._attributes[len(cls._attributes) - 1]:
                ret += ","
        return ret

    # return an expression of bind variables for UPDATE
    @classmethod
    def bindUpdateExpression(cls) -> str:
        ret = ""
        for attr in cls._attributes:
            ret += f"{attr}=:{attr},"
        ret = ret[:-1]
        ret += " "
        return ret

    # comparison function for sort
    @classmethod
    def compFunc(cls, a: Sequence[Any], b: Sequence[Any]) -> int:
        iPandaID = list(cls._attributes).index("PandaID")
        iPriority = list(cls._attributes).index("currentPriority")
        if a[iPriority] > b[iPriority]:
            return -1
        elif a[iPriority] < b[iPriority]:
            return 1
        else:
            if a[iPandaID] > b[iPandaID]:
                return 1
            elif a[iPandaID] < b[iPandaID]:
                return -1
            else:
                return 0

    # return an expression of bind variables for UPDATE to update only changed attributes
    def bindUpdateChangesExpression(self) -> str:
        ret = ""
        for attr in self._attributes:
            if attr in self._changedAttrs:
                ret += f"{attr}=:{attr},"
        ret = ret[:-1]
        ret += " "
        return ret

    # check if goint to merging
    def produceUnMerge(self) -> bool:
        for tmpFile in self.Files:
            if tmpFile.isUnMergedOutput():
                return True
        return False

    # truncate string attribute
    @classmethod
    def truncateStringAttr(cls, attr: str, val: Any) -> Any:
        if attr not in cls._limitLength:
            return val
        if val is None:
            return val
        return val[: cls._limitLength[attr]]

    # set DDM backend
    def setDdmBackEnd(self, backEnd: str) -> None:
        if self.specialHandling in [None, ""]:
            self.specialHandling = "ddm:" + backEnd
        else:
            if "ddm:" in self.specialHandling:
                self.specialHandling = re.sub("ddm:[,]+", "ddm:" + backEnd, self.specialHandling)
            else:
                self.specialHandling = self.specialHandling + "," + "ddm:" + backEnd

    # set LB number
    def setLumiBlockNr(self, lumiBlockNr: int) -> None:
        if self.specialHandling in ["", None, "NULL"]:
            self.specialHandling = f"lb:{lumiBlockNr}"
        else:
            self.specialHandling += f",lb:{lumiBlockNr}"

    # get LB number
    def getLumiBlockNr(self) -> int | None:
        if self.specialHandling is not None:
            for tmpItem in self.specialHandling.split(","):
                if tmpItem.startswith("lb:"):
                    return int(tmpItem.split(":")[-1])
        return None

    # get DDM backend
    def getDdmBackEnd(self) -> str | None:
        if self.specialHandling is None:
            return None
        for tmpItem in self.specialHandling.split(","):
            if tmpItem.startswith("ddm:"):
                return tmpItem.split(":")[-1]
        return None

    # set to accept partial finish
    def setToAcceptPartialFinish(self) -> None:
        self.set_special_handling("acceptPartial")

    # accept partial finish
    def acceptPartialFinish(self) -> bool:
        return self.check_special_handling("acceptPartial")

    # set home cloud
    def setHomeCloud(self, homeCloud: str) -> None:
        if self.specialHandling in ["", None, "NULL"]:
            self.specialHandling = f"hc:{homeCloud}"
        else:
            self.specialHandling += f",hc:{homeCloud}"

    # get cloud
    def getCloud(self) -> str:
        if self.specialHandling is not None:
            for tmpItem in self.specialHandling.split(","):
                if tmpItem.startswith("hc:"):  # hc: Home Cloud
                    return tmpItem.split(":")[-1]
        return self.cloud

    # check if cancelled or it's flavor
    def isCancelled(self) -> bool:
        return self.jobStatus in ["cancelled", "closed"]

    # get file names which were uploaded to alternative locations
    def altStgOutFileList(self) -> list[str]:
        try:
            if self.jobMetrics is not None:
                for item in self.jobMetrics.split():
                    if item.startswith("altTransferred="):
                        return item.split("=")[-1].split(",")
        except Exception:
            pass
        return []

    # check special handling
    def check_special_handling(self, key: str) -> bool:
        if self.specialHandling:
            return self._tagForSH[key] in self.specialHandling.split(",")
        return False

    # set special handling
    def set_special_handling(self, key: str) -> None:
        if self.specialHandling:
            items = self.specialHandling.split(",")
        else:
            items = []
        if self._tagForSH[key] not in items:
            items.append(self._tagForSH[key])
        self.specialHandling = ",".join(items)

    # get mode for alternative stage-out
    def getAltStgOut(self) -> str | None:
        if self.specialHandling is not None:
            for tmpItem in self.specialHandling.split(","):
                if tmpItem.startswith(f"{self._tagForSH['altStgOut']}:"):
                    return tmpItem.split(":")[-1]
        return None

    # set alternative stage-out
    def setAltStgOut(self, mode: str) -> None:
        if self.specialHandling is not None:
            items = self.specialHandling.split(",")
        else:
            items = []
        # remove old value
        newItems = []
        for tmpItem in items:
            if tmpItem.startswith(f"{self._tagForSH['altStgOut']}:"):
                continue
            newItems.append(tmpItem)
        newItems.append(f"{self._tagForSH['altStgOut']}:{mode}")
        self.specialHandling = ",".join(newItems)

    # put log files to OS
    def putLogToOS(self) -> bool:
        return self.check_special_handling("putLogToOS")

    # set to put log files to OS
    def setToPutLogToOS(self) -> None:
        self.set_special_handling("putLogToOS")

    # write input to file
    def writeInputToFile(self) -> bool:
        return self.check_special_handling("writeInputToFile")

    # set to write input to file
    def setToWriteInputToFile(self) -> None:
        self.set_special_handling("writeInputToFile")

    # set request type
    def setRequestType(self, reqType: str) -> None:
        if self.specialHandling is not None:
            items = self.specialHandling.split(",")
        else:
            items = []
        newItems = []
        setFlag = False
        for item in items:
            if not item.startswith(self._tagForSH["requestType"]):
                newItems.append(item)
        newItems.append(f"{self._tagForSH['requestType']}={reqType}")
        self.specialHandling = ",".join(newItems)

    # sort files
    def sortFiles(self) -> None:
        try:
            lfnMap: dict[str, Any] = {}
            for tmpFile in self.Files:
                if tmpFile.lfn not in lfnMap:
                    lfnMap[tmpFile.lfn] = []
                lfnMap[tmpFile.lfn].append(tmpFile)
            lfns = sorted(lfnMap)
            newFiles = []
            for tmpLFN in lfns:
                for tmpFile in lfnMap[tmpLFN]:
                    newFiles.append(tmpFile)
            self.Files = newFiles
        except Exception:
            pass

    # get zip file map
    def getZipFileMap(self) -> dict[str, list[str]]:
        zipMap: dict[str, list[str]] = {}
        try:
            if self.jobParameters is not None:
                zipStr = re.search("<ZIP_MAP>(.+)</ZIP_MAP>", self.jobParameters)
                if zipStr is not None:
                    for item in zipStr.group(1).split():
                        zipFile, conFileStr = item.split(":")
                        zipMap[zipFile] = conFileStr.split(",")
        except Exception:
            pass
        return zipMap

    # add multi step exec
    def addMultiStepExec(self, steps: dict[str, Any]) -> None:
        if not self.jobParameters:
            self.jobParameters = ""
        self.jobParameters += "<MULTI_STEP_EXEC>" + json.dumps(steps) + "</MULTI_STEP_EXEC>"

    # extract multi step exec
    def extractMultiStepExec(self) -> tuple[str, Any]:
        try:
            if "<MULTI_STEP_EXEC>" in self.jobParameters and "</MULTI_STEP_EXEC>" in self.jobParameters:
                pp_1, pp_2 = self.jobParameters.split("<MULTI_STEP_EXEC>")
                pp_2 = pp_2.split("</MULTI_STEP_EXEC>")[0]
                return pp_1, json.loads(pp_2)
        except Exception:
            pass
        return self.jobParameters, None

    # suppress execute string conversion
    def noExecStrCnv(self) -> bool:
        return self.check_special_handling("noExecStrCnv")

    # set to suppress execute string conversion
    def setNoExecStrCnv(self) -> None:
        self.set_special_handling("noExecStrCnv")

    # in-file positional event number
    def inFilePosEvtNum(self) -> bool:
        return self.check_special_handling("inFilePosEvtNum")

    # set to use in-file positional event number
    def setInFilePosEvtNum(self) -> None:
        self.set_special_handling("inFilePosEvtNum")

    # register event service files
    def registerEsFiles(self) -> bool:
        return self.check_special_handling("registerEsFiles")

    # set to register event service files
    def setRegisterEsFiles(self) -> None:
        self.set_special_handling("registerEsFiles")

    # set background-able flag
    def setBackgroundableFlag(self) -> None:
        self.jobExecutionID = 0
        if self.prodSourceLabel not in ["managed", "test"]:
            return
        try:
            if self.inputFileBytes / self.maxWalltime > 5000:  # type: ignore[operator]  # "NULL" sentinel, see spec_column.py
                return
        except Exception:
            return
        try:
            if self.coreCount <= 1:  # type: ignore[operator]  # unset spec column reads back as the "NULL" sentinel; see spec_column.py
                return
        except Exception:
            return
        if self.currentPriority > 250:  # type: ignore[operator]  # unset spec column reads back as the "NULL" sentinel; see spec_column.py
            return
        self.jobExecutionID = 1

    # use prefetcher
    def usePrefetcher(self) -> bool:
        return self.check_special_handling("usePrefetcher")

    # set to use prefetcher
    def setUsePrefetcher(self) -> None:
        self.set_special_handling("usePrefetcher")

    # use zip to pin
    def useZipToPin(self) -> bool:
        return self.check_special_handling("useZipToPin")

    # set to use zip to pin
    def setUseZipToPin(self) -> None:
        self.set_special_handling("useZipToPin")

    # use secrets
    def use_secrets(self) -> bool:
        return self.check_special_handling("useSecrets")

    # set to use secrets
    def set_use_secrets(self) -> None:
        self.set_special_handling("useSecrets")

    # not discard events
    def notDiscardEvents(self) -> bool:
        return self.check_special_handling("notDiscardEvents")

    # set not to discard events
    def setNotDiscardEvents(self) -> None:
        self.set_special_handling("notDiscardEvents")

    # all events are done
    def allOkEvents(self) -> bool:
        return self.check_special_handling("allOkEvents")

    # set all events are done
    def setAllOkEvents(self) -> None:
        self.set_special_handling("allOkEvents")

    # set scout job flag
    def setScoutJobFlag(self) -> None:
        self.set_special_handling("scoutJob")

    # check if scout job
    def isScoutJob(self) -> bool:
        return self.check_special_handling("scoutJob")

    # set build job flag
    def set_build_job_flag(self) -> None:
        self.set_special_handling("buildJob")

    # check if build job
    def is_build_job(self) -> bool:
        return self.check_special_handling("buildJob")

    # set pmerge job flag
    def set_pmerge_job_flag(self) -> None:
        self.set_special_handling("pmergeJob")

    # check if pmerge job
    def is_pmerge_job(self) -> bool:
        return self.check_special_handling("pmergeJob")

    # decrement attemptNr of events only when failed
    def decAttOnFailedES(self) -> bool:
        return self.check_special_handling("decAttOnFailedES")

    # set to decrement attemptNr of events only when failed
    def setDecAttOnFailedES(self) -> None:
        self.set_special_handling("decAttOnFailedES")

    # set fake flag to ignore in monigoring
    def setFakeJobToIgnore(self) -> None:
        self.set_special_handling("fakeJobToIgnore")

    # remove fake flag to ignore in monigoring
    def removeFakeJobToIgnore(self) -> None:
        if self.specialHandling is not None:
            items = self.specialHandling.split(",")
        else:
            items = []
        if self._tagForSH["fakeJobToIgnore"] in items:
            items.remove(self._tagForSH["fakeJobToIgnore"])
        self.specialHandling = ",".join(items)

    # set task attribute
    def set_task_attribute(self, key: str, value: Any) -> None:
        if not isinstance(self.metadata, list):
            self.metadata = [None, None]
        if len(self.metadata) != 3:
            self.metadata.append({})
        self.metadata[2][key] = value

    # get task attribute
    def get_task_attribute(self, key: str) -> Any:
        try:
            return self.metadata[2][key]  # type: ignore[index]  # "NULL" sentinel, see spec_column.py
        except Exception:
            return None

    # set input prestaging
    def setInputPrestaging(self) -> None:
        self.set_special_handling("inputPrestaging")

    # use input prestaging
    def useInputPrestaging(self) -> bool:
        return self.check_special_handling("inputPrestaging")

    # to a dictionary
    def to_dict_advanced(self, add_extra_info: bool = False) -> dict[str, Any]:
        """
        Convert the JobSpec to a dictionary, including file information and optionally extra info.

        :param add_extra_info: If True, include extra info such as job duration and extended prodSourceLabel. Default is False.
        :return: A dictionary representation of the JobSpec
        """
        ret = {}

        # Get the job attributes
        for a in self._attributes:
            v = getattr(self, a)
            if v == "NULL":
                v = None
            ret[a] = v

        # Add the files explicitly, as they are not included under _attributes
        a = "Files"
        files = getattr(self, a)
        file_stats = []
        for f in files:
            file_stats.append(f.to_dict())
        ret[a] = file_stats

        # Add extra info if requested
        if add_extra_info:
            # Calculate job duration if startTime and endTime are available
            if ret["startTime"] and ret["endTime"]:
                ret["jobDuration"] = (ret["endTime"] - ret["startTime"]).total_seconds()
            else:
                ret["jobDuration"] = None

            # Add extended prodSourceLabel info
            label = self.prodSourceLabel
            if self.is_build_job():
                label = f"{label}_build"
            elif self.is_pmerge_job():
                label = f"{label}_pmerge"
            elif self.isScoutJob():
                label = f"{label}_scout"
            else:
                label = f"{label}_normal"
            ret["extendedProdSourceLabel"] = label

        return ret

    def to_dict(self) -> dict[str, Any]:
        ret = {}
        for a in self._attributes:
            v = getattr(self, a)
            if isinstance(v, datetime.datetime):
                v = str(v)
            elif v == "NULL":
                v = None
            ret[a] = v
        return ret

    # check if HPO workflow flag
    def is_hpo_workflow(self) -> bool:
        return self.check_special_handling("hpoWorkflow")

    # set HPO workflow flag
    def set_hpo_workflow(self) -> None:
        self.set_special_handling("hpoWorkflow")

    # check if looping check is disabled
    def is_no_looping_check(self) -> bool:
        return self.check_special_handling("noLoopingCheck")

    # disable looping check
    def disable_looping_check(self) -> None:
        self.set_special_handling("noLoopingCheck")

    # check if encode job parameters
    def to_encode_job_params(self) -> bool:
        return self.check_special_handling("encJobParams")

    # encode job parameters
    def set_encode_job_params(self) -> None:
        self.set_special_handling("encJobParams")

    # check if debug mode
    def is_debug_mode(self) -> bool:
        if self.specialHandling is not None:
            items = self.specialHandling.split(",")
            return self._tagForSH["debugMode"] in items or "debug" in items
        return False

    # set debug mode
    def set_debug_mode(self) -> None:
        self.set_special_handling("debugMode")

    # set push status changes
    def set_push_status_changes(self) -> None:
        self.set_special_handling("pushStatusChanges")

    # check if to push status changes
    def push_status_changes(self) -> bool:
        return push_status_changes(self.specialHandling)

    # set push job
    def set_push_job(self) -> None:
        self.set_special_handling("pushJob")

    # check if to push job
    def is_push_job(self) -> bool:
        return self.check_special_handling("pushJob")

    # set on-site merging
    def set_on_site_merging(self) -> None:
        self.set_special_handling("onSiteMerging")

    # check if on-site merging
    def is_on_site_merging(self) -> bool:
        return self.check_special_handling("onSiteMerging")

    # get RAM for retry
    def get_ram_for_retry(self) -> int | None:
        if self.specialHandling is not None:
            for tmpItem in self.specialHandling.split(","):
                if tmpItem.startswith(f"{self._tagForSH['retryRam']}:"):
                    return int(tmpItem.split(":")[-1])
        return None

    # set RAM for retry
    def set_ram_for_retry(self, val: int) -> None:
        if self.specialHandling:
            items = self.specialHandling.split(",")
        else:
            items = []
        # remove old value
        newItems = []
        for tmpItem in items:
            if tmpItem.startswith(f"{self._tagForSH['retryRam']}:"):
                continue
            newItems.append(tmpItem)
        newItems.append(f"{self._tagForSH['retryRam']}:{val}")
        self.specialHandling = ",".join(newItems)

    # dump to json-serializable
    def dump_to_json_serializable(self) -> list[Any]:
        job_state = self.__getstate__()
        file_state_list = []
        for file_spec in job_state[-1]:
            file_stat = file_spec.dump_to_json_serializable()
            file_state_list.append(file_stat)
        job_state = job_state[:-1]
        # append files
        job_state.append(file_state_list)
        return job_state

    # load from json-serializable
    def load_from_json_serializable(self, job_state: list[Any]) -> None:
        # initialize with empty file list
        self.__setstate__(job_state[:-1] + [[]])
        # add files
        for file_stat in job_state[-1]:
            file_spec = FileSpec()
            file_spec.__setstate__(file_stat)
            self.addFile(file_spec)

    def load_from_dict(self, job_dict: dict[str, Any]) -> None:
        # Extract job attributes (excluding files)
        job_attrs = []
        for slot in self.__slots__:
            if slot == "Files":  # skip files here
                continue
            job_attrs.append(job_dict.get(slot, None))

        # Initialize with empty file list
        self.__setstate__(job_attrs + [[]])

        # Add files
        for file_data in job_dict.get("Files", []):
            file_spec = FileSpec()
            file_spec.__setstate__([file_data.get(s, None) for s in file_spec.__slots__])
            self.addFile(file_spec)

    # set input and output file types
    def set_input_output_file_types(self) -> None:
        """
        Set input and output file types based on the input and output file names
        """
        in_types = set()
        out_types = set()
        for tmp_file in self.Files:
            # ignore DBRelease/lib.tgz files
            if tmp_file.dataset.startswith("ddo") or tmp_file.lfn.endswith(".lib.tgz"):
                continue
            # extract type while ignoring user/group/groupXY
            tmp_items = tmp_file.dataset.split(".")
            if (
                len(tmp_items) > 4
                and (not tmp_items[0] in ["", "NULL", "user", "group", "hc_test"])
                and (not tmp_items[0].startswith("group"))
                and not tmp_file.dataset.startswith("panda.um.")
            ):
                tmp_type = tmp_items[4]
            else:
                continue
            if tmp_file.type == "input":
                in_types.add(tmp_type)
            elif tmp_file.type == "output":
                out_types.add(tmp_type)
        # set types
        if in_types:
            self.inputFileType = ",".join(sorted(in_types))[: self._limitLength["inputFileType"]]
        if out_types:
            self.outputFileType = ",".join(sorted(out_types))[: self._limitLength["outputFileType"]]

    # set task queued time
    def set_task_queued_time(self, queued_time: float | None) -> None:
        """
        Set task queued time in job metrics. Skip if queued_time is None

        :param queued_time: task queued time in seconds since epoch
        """
        if queued_time is None:
            return
        if self.specialHandling in [None, "", "NULL"]:
            items = []
        else:
            items = self.specialHandling.split(",")
        items.append(f"{self._tagForSH['taskQueuedTime']}={queued_time}")
        self.specialHandling = ",".join(items)


# utils


# check if to push status changes without class instance
def push_status_changes(special_handling: str | None) -> bool:
    if special_handling is not None:
        items = special_handling.split(",")
        return JobSpec._tagForSH["pushStatusChanges"] in items
    return False


# get task queued time
def get_task_queued_time(special_handling: str | None) -> datetime.datetime | None:
    """
    Get task queued time from job metrics

    :param special_handling: special handling string
    :return: task queued time. None if unset
    """
    try:
        if special_handling is not None:
            for item in special_handling.split(","):
                if item.startswith(f"{JobSpec._tagForSH['taskQueuedTime']}="):
                    return datetime.datetime.fromtimestamp(float(item.split("=")[-1]), datetime.timezone.utc)
    except Exception:
        pass
    return None
