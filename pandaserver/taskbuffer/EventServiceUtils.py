import re
from typing import Any

from pandaserver.taskbuffer.JobSpec import JobSpec

# status codes for each event range
ST_ready = 0
ST_sent = 1
ST_running = 2
ST_finished = 3
ST_cancelled = 4
ST_discarded = 5
ST_done = 6
ST_failed = 7
ST_fatal = 8
ST_merged = 9
ST_corrupted = 10
ST_reserved_fail = 98
ST_reserved_get = 99

ES_status_map = {
    ST_ready: "ready",
    ST_sent: "sent",
    ST_running: "running",
    ST_finished: "finished",
    ST_cancelled: "cancelled",
    ST_discarded: "discarded",
    ST_done: "done",
    ST_failed: "failed",
    ST_fatal: "fatal",
    ST_merged: "merged",
    ST_corrupted: "corrupted",
    ST_reserved_fail: "reserved_fail",
    ST_reserved_get: "reserved_get",
}

# identifiers for specialHandling
esHeader = "es:"
singleConsumerType = {"runonce": "1", "storeonce": "2"}

# tags for special handling. check JobSpec._tagForSH for duplication
esToken = "eventservice"
esMergeToken = "esmerge"
dynamicNumEventsToken = JobSpec._tagForSH["dynamicNumEvents"]
mergeAtOsToken = JobSpec._tagForSH["mergeAtOs"]
resurrectConsumersToken = JobSpec._tagForSH["resurrectConsumers"]
singleToken = JobSpec._tagForSH["jobCloning"]


# values for job.eventService
esJobFlagNumber = 1
esMergeJobFlagNumber = 2
jobCloningFlagNumber = 3
jumboJobFlagNumber = 4
coJumboJobFlagNumber = 5
fineGrainedFlagNumber = 6


# values for task.eventService
TASK_NORMAL = 0
TASK_EVENT_SERVICE = 1
TASK_JOB_CLONING = 2
TASK_FINE_GRAINED = 3

# values for event.is_jumbo
eventTableIsJumbo = 1


# siteid for waiting co-jumbo jobs
siteIdForWaitingCoJumboJobs = "WAITING_CO_JUMBO"


# relation type for jobsets
relationTypeJS_ID = "jobset_id"
relationTypeJS_Retry = "jobset_retry"
relationTypeJS_Map = "jobset_map"
relationTypesForJS = [relationTypeJS_ID, relationTypeJS_Retry, relationTypeJS_Map]


# suffix for ES dataset and files to register to DDM
esSuffixDDM = ".events"
esScopeDDM = "transient"
esRegStatus = "esregister"

# default max number of ES job attempt
defMaxAttemptEsJob = 3


# pilot error code
PEC_corruptedInputFiles = [1171, 1145, 1175, 1103]
PEC_corruptedInputFilesTmp = [1099]


# encode file info
def encodeFileInfo(
    lfn: str,
    startEvent: int,
    endEvent: int,
    nEventsPerWorker: int,
    maxAttempt: int | None = None,
    firstOffset: int | None = None,
    firstEvent: int | None = None,
) -> str:
    if maxAttempt is None:
        maxAttempt = 10
    if firstOffset is None:
        return f"{lfn}/{startEvent}/{endEvent}/{nEventsPerWorker}/{maxAttempt}^"
    else:
        if firstEvent is None:
            # no event to take the offset from; the arithmetic below used to reach the except
            totalOffset = 0
        else:
            try:
                totalOffset = firstEvent - firstOffset
            except Exception:
                totalOffset = 0
        return f"{lfn}/{startEvent}/{endEvent}/{nEventsPerWorker}/{maxAttempt}/{totalOffset}^"


# get header for specialHandling
def getHeaderForES(esIndex: int) -> str:
    return f"{esHeader}{esIndex}:"


# decode file info
def decodeFileInfo(specialHandling: str) -> tuple[dict[str, dict[str, int]], str, str | None]:
    eventServiceInfo: dict[str, dict[str, int]] = {}
    newSpecialHandling = ""
    esIndex: str | None = None
    try:
        for tmpItem in specialHandling.split(","):
            if tmpItem.startswith(esHeader):
                tmpItem = re.sub("^" + esHeader, "", tmpItem)
                # look for event service index
                tmpMatch = re.search("^(\d+):", tmpItem)
                if tmpMatch is not None:
                    esIndex = tmpMatch.group(1)
                    tmpItem = re.sub("^(\d+):", "", tmpItem)
                for esItem in tmpItem.split("^"):
                    if esItem == "":
                        continue
                    esItems = esItem.split("/")
                    # each of these is either a field of the split token or a default or a
                    # count derived from two of them, so a name holds text in one branch
                    # and a number in another. int() below takes either.
                    maxAttempt: str | int = 10
                    esOffset: str | int = 0
                    esEvents: str | int
                    esStartEvent: str | int
                    if len(esItems) == 3:
                        esLFN, esEvents, esRange = esItems
                        esStartEvent = 0
                    elif len(esItems) == 5:
                        esLFN, esStartEvent, esEndEvent, esRange, maxAttempt = esItems
                        esEvents = int(esEndEvent) - int(esStartEvent) + 1
                    elif len(esItems) == 6:
                        (
                            esLFN,
                            esStartEvent,
                            esEndEvent,
                            esRange,
                            maxAttempt,
                            esOffset,
                        ) = esItems
                        esEvents = int(esEndEvent) - int(esStartEvent) + 1
                    else:
                        esLFN, esStartEvent, esEndEvent, esRange = esItems
                        esEvents = int(esEndEvent) - int(esStartEvent) + 1
                    eventServiceInfo[esLFN] = {
                        "nEvents": int(esEvents),
                        "startEvent": int(esStartEvent),
                        "nEventsPerRange": int(esRange),
                        "maxAttempt": int(maxAttempt),
                        "offset": int(esOffset),
                    }
                newSpecialHandling += f"{esToken},"
            else:
                newSpecialHandling += f"{tmpItem},"
        newSpecialHandling = newSpecialHandling[:-1]
    except Exception:
        newSpecialHandling = specialHandling
    return eventServiceInfo, newSpecialHandling, esIndex


# check if event service job
def isEventServiceJob(job: JobSpec) -> bool:
    # fine-grained job
    if is_fine_grained_job(job):
        return False
    return isEventServiceSH(job.specialHandling)


# check if event service merge job
def isEventServiceMerge(job: JobSpec) -> bool:
    return isEventServiceMergeSH(job.specialHandling)


# check if specialHandling for event service
def isEventServiceSH(specialHandling: str | None) -> bool:
    try:
        if specialHandling is not None and esToken in specialHandling.split(","):
            return True
    except Exception:
        pass
    return False


# check if specialHandling for event service merge
def isEventServiceMergeSH(specialHandling: str | None) -> bool:
    try:
        if specialHandling is not None and esMergeToken in specialHandling.split(","):
            return True
    except Exception:
        pass
    return False


# set event service merge
def setEventServiceMerge(job: JobSpec) -> None:
    try:
        # set ES flag
        job.eventService = esMergeJobFlagNumber
        # set gshare to express
        job.gshare = "Express"
        # set flag for merging
        if job.specialHandling is None:
            job.specialHandling = esMergeToken
        else:
            newSpecialHandling = ""
            # remove flag for event service
            for tmpFlag in job.specialHandling.split(","):
                if tmpFlag in ["", esToken]:
                    continue
                newSpecialHandling += tmpFlag
                newSpecialHandling += ","
            newSpecialHandling += esMergeToken
            job.specialHandling = newSpecialHandling
        # remove fake flag
        job.removeFakeJobToIgnore()
    except Exception:
        pass


# check if specialHandling for job cloning
def isJobCloningSH(specialHandling: str | None) -> bool:
    try:
        if specialHandling is not None:
            for token in specialHandling.split(","):
                if singleToken == token.split(":")[0]:
                    return True
    except Exception:
        pass
    return False


# check if event service job
def isJobCloningJob(job: JobSpec) -> bool:
    return isJobCloningSH(job.specialHandling)


# set header for job cloning
def setHeaderForJobCloning(specialHandling: str | None, scType: str) -> str:
    if specialHandling is None:
        specialHandling = ""
    tokens = specialHandling.split(",")
    while True:
        try:
            tokens.remove("")
        except Exception:
            break
    if scType in singleConsumerType.values():
        tokens.append(f"{singleToken}:{scType}")
    return ",".join(tokens)


# get consumer type
def getJobCloningType(job: JobSpec) -> str:
    if job.specialHandling is not None:
        for token in job.specialHandling.split(","):
            if singleToken == token.split(":")[0]:
                for tmpKey in singleConsumerType:
                    tmpVal = singleConsumerType[tmpKey]
                    if tmpVal == token.split(":")[-1]:
                        return tmpKey
    return ""


# get consumer value
def getJobCloningValue(scType: str) -> str:
    if scType in singleConsumerType:
        return singleConsumerType[scType]
    return ""


# set header for dynamic number of events
def setHeaderForDynNumEvents(specialHandling: str | None) -> str:
    if specialHandling is None:
        specialHandling = ""
    tokens = specialHandling.split(",")
    while True:
        try:
            tokens.remove("")
        except Exception:
            break
    tokens.append(dynamicNumEventsToken)
    return ",".join(tokens)


# check if specialHandling for dynamic number of events
def isDynNumEventsSH(specialHandling: str | None) -> bool:
    try:
        if specialHandling is not None:
            if dynamicNumEventsToken in specialHandling.split(","):
                return True
    except Exception:
        pass
    return False


# remove event service header
def removeHeaderForES(job: JobSpec) -> None:
    if job.specialHandling is not None:
        items = job.specialHandling.split(",")
        newItems = []
        for item in items:
            if re.search("^" + esHeader + ".+", item) is None:
                newItems.append(item)
    job.specialHandling = ",".join(newItems)


# check if jumbo job
def isJumboJob(job: JobSpec) -> bool:
    return job.eventService == jumboJobFlagNumber


# check if cooperative with jumbo job
def isCoJumboJob(job: JobSpec) -> bool:
    return job.eventService == coJumboJobFlagNumber


# set header for merge at OS
def setHeaderForMergeAtOS(specialHandling: str | None) -> str:
    if specialHandling is None:
        specialHandling = ""
    tokens = specialHandling.split(",")
    while True:
        try:
            tokens.remove("")
        except Exception:
            break
    if mergeAtOsToken not in tokens:
        tokens.append(mergeAtOsToken)
    return ",".join(tokens)


# check if specialHandling for merge at OS
def isMergeAtOS(specialHandling: str | None) -> bool:
    try:
        if specialHandling is not None:
            if mergeAtOsToken in specialHandling.split(","):
                return True
    except Exception:
        pass
    return False


# get dataset name for event service
def getEsDatasetName(taskID: int | str | None) -> str:
    esDataset = f"{esScopeDDM}:{taskID}{esSuffixDDM}"
    return esDataset


# set header to resurrect consumers
def setHeaderToResurrectConsumers(specialHandling: str | None) -> str:
    if specialHandling is None:
        specialHandling = ""
    tokens = specialHandling.split(",")
    while True:
        try:
            tokens.remove("")
        except Exception:
            break
    if resurrectConsumersToken not in tokens:
        tokens.append(resurrectConsumersToken)
    return ",".join(tokens)


# check if specialHandling to resurrect consumers
def isResurrectConsumers(specialHandling: str | None) -> bool:
    try:
        if specialHandling is not None:
            if resurrectConsumersToken in specialHandling.split(","):
                return True
    except Exception:
        pass
    return False


# check if fine-grained job
def is_fine_grained_job(job: JobSpec) -> bool:
    return job.eventService == fineGrainedFlagNumber


# set fine-grained
def set_fine_grained(job: JobSpec) -> None:
    job.eventService = fineGrainedFlagNumber
