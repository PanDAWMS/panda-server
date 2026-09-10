import copy
import math
import random
from typing import TYPE_CHECKING, Any

from pandacommon.pandalogger.PandaLogger import PandaLogger

from pandaserver.srvcore import CoreUtils

if TYPE_CHECKING:
    from pandaserver.taskbuffer.JediTaskSpec import JediTaskSpec

logger = PandaLogger().getLogger(__name__.split(".")[-1])


# class for input
class InputChunk:
    # default output size 1G + 500MB (safety merging)
    defaultOutputSize = 1500 * 1024 * 1024
    # max input size for scouts in MB
    maxInputSizeScouts = 50000
    # max input size for jobs after avalanche in MB
    maxInputSizeAvalanche = 500000
    # max number of input files
    maxTotalNumFiles = 1000

    def __str__(self) -> str:
        sb = []
        for key in self.__dict__:
            sb.append(f"{key}='{self.__dict__[key]}'")

        return ", ".join(sb)

    def __repr__(self) -> str:
        return self.__str__()

    # constructor
    def __init__(
        self,
        taskSpec: "JediTaskSpec",
        masterDataset: Any = None,
        secondaryDatasetList: list[Any] = [],
        ramCount: int | None = 0,
    ) -> None:
        # task spec
        self.taskSpec = taskSpec
        # The datasets here are JediDatasetSpec objects and are deliberately not declared as
        # such. Naming the type makes their columns visible to a checker, and getSubChunk() then
        # reports about twenty-five places where a column declared optional -- fsize, nEvents,
        # startEvent, endEvent -- is added, multiplied or compared without a test. Each of those
        # is a real question about what a NULL means when splitting a job, and answering them
        # with a default would change how work is divided; that belongs in its own change.
        self.secondaryDatasetList: list[Any]
        if secondaryDatasetList is None:
            self.secondaryDatasetList = []
        else:
            self.secondaryDatasetList = secondaryDatasetList
        # the list of site candidates. These are pandajedi.jedicore.SiteCandidate objects,
        # which this package does not import: nothing in pandaserver imports pandajedi, and an
        # annotation is not the place to start
        self.siteCandidates: dict[str, Any] = {}
        # the list of site candidates for jumbo jobs
        self.siteCandidatesJumbo: dict[str, Any] = {}
        # the name of master index
        self.masterIndexName: int | None = None
        # dataset mapping including indexes of files/events, keyed by datasetID
        self.datasetMap: dict[Any, dict[str, Any]] = {}
        # the master dataset
        self.masterDataset: Any = None
        self.addMasterDS(masterDataset)
        # the list of secondary datasets
        self.secondaryDatasetList = []
        for secondaryDS in secondaryDatasetList:
            self.addSecondaryDS(secondaryDS)
        # read in a block
        self.readBlock: bool | None = None
        # merging
        self.isMerging = False
        # use scout
        self.useScoutFlag: bool | None = None
        # memory requirements for the inputChunk
        self.ramCount = ramCount
        # flag to set if inputchunk is empty
        self.isEmpty = False
        # flag to use jumbo jobs
        self.useJumbo: str | None = None
        # checkpoint of used counters
        self.file_checkpoints: dict[int, int] = {}
        self.intermediate_file_checkpoints: dict[int, int] = {}
        # list of bootstrapped sites
        self.bootstrapped: set[str] = set()

    # add master dataset
    def addMasterDS(self, masterDataset: Any) -> None:
        if masterDataset is not None:
            self.masterDataset = masterDataset
            self.masterIndexName = self.masterDataset.datasetID
            self.datasetMap[self.masterDataset.datasetID] = {"used": 0, "datasetSpec": masterDataset}

    # add secondary dataset
    def addSecondaryDS(self, secondaryDataset: Any) -> None:
        if secondaryDataset not in self.secondaryDatasetList:
            self.secondaryDatasetList.append(secondaryDataset)
            self.datasetMap[secondaryDataset.datasetID] = {"used": 0, "datasetSpec": secondaryDataset}

    # return list of datasets
    def getDatasets(self, includePseudo: bool = False) -> list[Any]:
        dataList = []
        if self.masterDataset is not None:
            dataList.append(self.masterDataset)
        dataList += self.secondaryDatasetList
        # ignore pseudo datasets
        if not includePseudo:
            newDataList = []
            for datasetSpec in dataList:
                if not datasetSpec.isPseudo():
                    newDataList.append(datasetSpec)
            dataList = newDataList
        return dataList

    # return dataset with datasetID
    def getDatasetWithID(self, datasetID: Any) -> Any:
        if datasetID in self.datasetMap:
            return self.datasetMap[datasetID]["datasetSpec"]
        return None

    # return dataset with datasetName
    def getDatasetWithName(self, datasetName: str) -> Any:
        for tmpDatasetID, tmpDatasetVal in self.datasetMap.items():
            if tmpDatasetVal["datasetSpec"].datasetName == datasetName:
                return tmpDatasetVal["datasetSpec"]
        return None

    # reset used counters
    def resetUsedCounters(self) -> None:
        for tmpKey, tmpVal in self.datasetMap.items():
            tmpVal["used"] = 0

    # checkpoint file usage
    def checkpoint_file_usage(self, intermediate: bool = False) -> None:
        """
        checkpoint (intermediate) file usage
        :param intermediate: True to checkpoint intermediate
        :return: None
        """
        checkpoints = self.intermediate_file_checkpoints if intermediate else self.file_checkpoints
        for tmpKey, tmpVal in self.datasetMap.items():
            checkpoints[tmpKey] = tmpVal["used"]

    # rollback file usage
    def rollback_file_usage(self, intermediate: bool = False) -> None:
        """
        rollback file usage to the last (intermediate) checkpoint
        :param intermediate: True to roll back to the intermediate checkpoint
        :return: None
        """
        checkpoints = self.intermediate_file_checkpoints if intermediate else self.file_checkpoints
        for tmpKey, tmpVal in self.datasetMap.items():
            tmpVal["used"] = checkpoints.get(tmpKey, 0)

    # add site candidates
    def addSiteCandidate(self, siteCandidateSpec: Any) -> None:
        self.siteCandidates[siteCandidateSpec.siteName] = siteCandidateSpec
        return

    # add site candidates
    def addSiteCandidateForJumbo(self, siteCandidateSpec: Any) -> None:
        self.siteCandidatesJumbo[siteCandidateSpec.siteName] = siteCandidateSpec
        return

    # has candidate for jumbo jobs
    def hasCandidatesForJumbo(self) -> bool:
        return len(self.siteCandidatesJumbo) > 0

    # get one site candidate randomly
    def getOneSiteCandidate(self, nSubChunks: int = 0, ngSites: list[str | None] | None = None, get_msg: bool = False) -> Any:
        retSiteCandidate = None
        if ngSites is None:
            ngSites = []
        ngSites = copy.copy(ngSites)
        if self.masterDataset is None:
            # every file and event this method walks comes from the master dataset,
            # so a chunk built without one has nothing to work on here
            raise RuntimeError("getOneSiteCandidate() called without a master dataset")

        # skip sites for distributed master dataset
        dist_str = ""
        if self.masterDataset.isDistributed():
            dist_str = "(distributed"
            datasetUsage = self.datasetMap[self.masterDataset.datasetID]
            ng_for_dist = []
            ok_for_dist = False
            if len(self.masterDataset.Files) > datasetUsage["used"]:
                tmpFileSpec = self.masterDataset.Files[datasetUsage["used"]]
                for siteCandidate in self.siteCandidates.values():
                    # skip if the first file is unavailable at the site
                    if not siteCandidate.isAvailableFile(tmpFileSpec):
                        ng_for_dist.append(siteCandidate.siteName)
                    else:
                        ok_for_dist = True
                if ok_for_dist:
                    ngSites += ng_for_dist
                else:
                    dist_str += ": disabled locality constraint since the first file is unnavigable anywhere"
            dist_str += ")"

        # check if to be bootstrapped
        siteCandidateList = list(self.siteCandidates.values())
        newSiteCandidateList = []
        for siteCandidate in siteCandidateList:
            if siteCandidate.weight == 0 and siteCandidate.siteName not in self.bootstrapped and siteCandidate.siteName not in ngSites:
                newSiteCandidateList.append(siteCandidate)
        if newSiteCandidateList:
            retSiteCandidate = random.choice(newSiteCandidateList)
            self.bootstrapped.add(retSiteCandidate.siteName)
            retMsg = f"to bootstrap: {retSiteCandidate.siteName}"
        else:
            # get total weight
            totalWeight = 0
            nNG = 0
            nOK = 0
            nBoosted = 0
            nFull = 0
            fullStr = ""
            for siteCandidate in siteCandidateList:
                # remove NG sites
                if siteCandidate.siteName in ngSites:
                    nNG += 1
                    continue
                # already bootstrapped
                if siteCandidate.weight == 0 and siteCandidate.siteName in self.bootstrapped:
                    nBoosted += 1
                    continue
                # skip incapable
                if not siteCandidate.can_accept_jobs():
                    nFull += 1
                    fullStr += f"{siteCandidate.siteName}:{siteCandidate.nQueuedJobs}/{siteCandidate.nRunningJobsCap} "
                    continue
                totalWeight += siteCandidate.weight
                newSiteCandidateList.append(siteCandidate)
                nOK += 1
            siteCandidateList = newSiteCandidateList
            if fullStr:
                fullStr = f" (skipped {fullStr[:-1]})"
            retMsg = f"OK={nOK} NG={','.join([site for site in ngSites if site is not None]) if ngSites else None} {dist_str} n_bootstrapped={len(self.bootstrapped)} n_occupied={nFull}{fullStr}"
            # empty
            if not siteCandidateList:
                if get_msg:
                    return None, retMsg
                return None
            # get random number
            rNumber = random.random() * totalWeight
            for siteCandidate in siteCandidateList:
                rNumber -= siteCandidate.weight
                if rNumber <= 0:
                    retSiteCandidate = siteCandidate
                    break
            # return something as a protection against precision of float
            if retSiteCandidate is None:
                retSiteCandidate = random.choice(siteCandidateList)
            # modify weight
            try:
                if retSiteCandidate.nQueuedJobs is not None and retSiteCandidate.nAssignedJobs is not None:
                    oldNumQueued = retSiteCandidate.nQueuedJobs
                    retSiteCandidate.nQueuedJobs += nSubChunks
                    newNumQueued = retSiteCandidate.nQueuedJobs
                    retSiteCandidate.nAssignedJobs += nSubChunks
                    siteCandidate.weight = siteCandidate.weight * float(oldNumQueued + 1) / float(newNumQueued + 1)
            except Exception:
                pass
        if get_msg:
            return retSiteCandidate, retMsg
        return retSiteCandidate

    # get sites for parallel execution
    def getParallelSites(self, nSites: int, nSubChunks: int, usedSites: list[str | None]) -> str:
        newSiteCandidate = self.getOneSiteCandidate(nSubChunks, usedSites)
        if newSiteCandidate is not None:
            usedSites.append(newSiteCandidate.siteName)
            if nSites > len(usedSites):
                return self.getParallelSites(nSites, nSubChunks, usedSites)
        return ",".join([site for site in usedSites if site is not None])

    # get one site for jumbo jobs
    def getOneSiteCandidateForJumbo(self, ngSites: list[str]) -> Any:
        # get total weight
        totalWeight = 0
        siteCandidateList = list(self.siteCandidatesJumbo.values())
        newSiteCandidateList = []
        for siteCandidate in siteCandidateList:
            # remove NG sites
            if siteCandidate.siteName in ngSites:
                continue
            totalWeight += siteCandidate.weight
            newSiteCandidateList.append(siteCandidate)
        siteCandidateList = newSiteCandidateList
        # empty
        if siteCandidateList == []:
            return None
        # get random number
        rNumber = random.random() * totalWeight
        for siteCandidate in siteCandidateList:
            rNumber -= siteCandidate.weight
            if rNumber <= 0:
                retSiteCandidate = siteCandidate
                break
        # return something as a protection against precision of float
        if retSiteCandidate is None:
            retSiteCandidate = random.choice(siteCandidateList)
        return retSiteCandidate

    # check if unused files/events remain
    def checkUnused(self) -> bool:
        # master is undefined
        if self.masterIndexName is None:
            return False
        indexVal = self.datasetMap[self.masterIndexName]
        used: int = indexVal["used"]
        return used < len(indexVal["datasetSpec"].Files)

    # get master used index
    def getMasterUsedIndex(self) -> int:
        # master is undefined
        if self.masterIndexName is None:
            return 0
        indexVal = self.datasetMap[self.masterIndexName]
        used: int = indexVal["used"]
        return used

    # get num of files in master
    def getNumFilesInMaster(self) -> int:
        # master is undefined
        if self.masterIndexName is None:
            return 0
        indexVal = self.datasetMap[self.masterIndexName]
        return len(indexVal["datasetSpec"].Files)

    # check if secondary datasets use event ratios
    def useEventRatioForSec(self) -> bool:
        for datasetSpec in self.secondaryDatasetList:
            if datasetSpec.getEventRatio() is not None:
                return True
        return False

    # get maximum size of atomic subchunk
    def getMaxAtomSize(self, effectiveSize: bool = False, getNumEvents: bool = False) -> float:
        # number of files per job if defined
        if not self.isMerging:
            nFilesPerJob = self.taskSpec.getNumFilesPerJob()
        else:
            nFilesPerJob = self.taskSpec.getNumFilesPerMergeJob()
        nEventsPerJob = None
        if nFilesPerJob is None:
            # number of events per job
            if not self.isMerging:
                nEventsPerJob = self.taskSpec.getNumEventsPerJob()
            else:
                nEventsPerJob = self.taskSpec.getNumEventsPerMergeJob()
            if nEventsPerJob is None:
                nFilesPerJob = 1
        # grouping with boundaryID
        useBoundary = self.taskSpec.useGroupWithBoundaryID()
        # LB
        respectLB = self.taskSpec.respectLumiblock()
        maxAtomSize: float = 0
        while True:
            if not self.isMerging:
                maxNumFiles = self.taskSpec.getMaxNumFilesPerJob()
            else:
                maxNumFiles = self.taskSpec.getMaxNumFilesPerMergeJob()
            # get one subchunk
            subChunk, _ = self.getSubChunk(
                None, nFilesPerJob=nFilesPerJob, nEventsPerJob=nEventsPerJob, useBoundary=useBoundary, respectLB=respectLB, maxNumFiles=maxNumFiles
            )
            if subChunk is None:
                break
            # get size
            tmpAtomSize: float = 0
            lfn_set = set()
            for tmpDatasetSpec, tmpFileSpecList in subChunk:
                if (effectiveSize or getNumEvents) and not tmpDatasetSpec.isMaster():
                    continue
                for tmpFileSpec in tmpFileSpecList:
                    if effectiveSize:
                        tmpAtomSize += CoreUtils.getEffectiveFileSize(tmpFileSpec.fsize, tmpFileSpec.startEvent, tmpFileSpec.endEvent, tmpFileSpec.nEvents)
                    elif getNumEvents:
                        tmpAtomSize += tmpFileSpec.getEffectiveNumEvents()
                    else:
                        if tmpFileSpec.lfn in lfn_set:
                            continue
                        lfn_set.add(tmpFileSpec.lfn)
                        tmpAtomSize += tmpFileSpec.fsize
            if maxAtomSize < tmpAtomSize:
                maxAtomSize = tmpAtomSize
        # reset counters
        self.resetUsedCounters()
        # return
        return maxAtomSize

    # use scout
    def useScout(self) -> bool:
        if self.masterDataset is not None and self.useScoutFlag is not None:
            return self.useScoutFlag
        if self.masterDataset is not None and self.masterDataset.nFiles > self.masterDataset.nFilesToBeUsed:
            return True
        return False

    # set use scout
    def setUseScout(self, useScoutFlag: bool) -> None:
        self.useScoutFlag = useScoutFlag

    # get preassigned site
    def getPreassignedSite(self) -> str | None:
        if self.masterDataset is not None:
            site: str | None = self.masterDataset.site
            return site
        return None

    # get max output size
    def getOutSize(self, outSizeMap: dict[Any, int]) -> int:
        values = sorted(outSizeMap.values())
        try:
            return values[-1]
        except Exception:
            return 0

    # get value with unit
    def get_value_str(self, value: float | None) -> str | None:
        if value is None:
            return None
        try:
            return f"{int(math.ceil(value / 1024 / 1024))}MB"
        except Exception:
            return None

    # get subchunk with a selection criteria
    # tmpLog is a pandajedi.jedicore.MsgWrapper; see the note in __init__ on why the name is
    # in this comment rather than in the annotation. The first element of the result is the
    # sub-chunks, None when there is no master dataset and empty when nothing could be taken
    def getSubChunk(
        self,
        siteName: str | None,
        maxNumFiles: int | None = None,
        maxSize: int | None = None,
        sizeGradients: float = 0,
        sizeIntercepts: float = 0,
        nFilesPerJob: int | None = None,
        multiplicand: int = 1,
        walltimeGradient: float | None = 0,
        maxWalltime: int | None = 0,
        nEventsPerJob: int | None = None,
        useBoundary: dict[str, Any] | None = None,
        sizeGradientsPerInSize: float | None = None,
        maxOutSize: int | None = None,
        coreCount: int | None = 1,
        respectLB: bool = False,
        corePower: float | None = None,
        dynNumEvents: bool = False,
        multiplicity: int | None = None,
        splitByFields: list[int] | None = None,
        tmpLog: Any = None,
        useDirectIO: bool = False,
        maxDiskSize: int | None = None,
        enableLog: bool = False,
        no_split: bool = False,
        min_walltime: float | None = None,
        max_events: int | None = None,
        skip_short_output: bool = False,
    ) -> tuple[list[Any] | None, bool]:
        if coreCount is None:
            # the same value the parameter defaults to; float(None) below would raise
            coreCount = 1
        is_short = False
        if self.masterDataset is None:
            # every file and event this method walks comes from the master dataset,
            # so a chunk built without one has nothing to work on here
            raise RuntimeError("getSubChunk() called without a master dataset")
        # read once: the guard above is what makes this a dataset rather than an optional one,
        # and the calls between here and the uses below hide that from a checker
        master_dataset = self.masterDataset
        # check if there are unused files/events
        if not self.checkUnused():
            return None, is_short
        # protection against unreasonable values
        if nFilesPerJob == 0 or dynNumEvents:
            nFilesPerJob = None
        if nEventsPerJob == 0 or dynNumEvents:
            nEventsPerJob = None
        # set default max number of files
        if maxNumFiles is None:
            maxNumFiles = 200
        # set default max size
        if maxSize is None and nFilesPerJob is None and nEventsPerJob is None:
            # 20 GB at most by default
            maxSize = 20 * 1024 * 1024 * 1024
        # set default output size
        minOutSize = self.defaultOutputSize
        # set default max number of events
        maxNumEvents = None
        # ignore negative walltime gradient
        if walltimeGradient is None or walltimeGradient < 0:
            walltimeGradient = 0
        # overwrite parameters when nFiles/EventsPerJob is used
        if nFilesPerJob is not None and not dynNumEvents:
            maxNumFiles = nFilesPerJob
            if not respectLB:
                multiplicand = nFilesPerJob
        if nEventsPerJob is not None and not dynNumEvents:
            maxNumEvents = nEventsPerJob
        elif max_events and dynNumEvents:
            maxNumEvents = max_events
        # split with boundayID
        splitWithBoundaryID = False
        if useBoundary is not None:
            splitWithBoundaryID = True
            if useBoundary["inSplit"] == 2:
                # unset max values to split only with boundaryID
                maxNumFiles = None
                maxSize = None
                maxWalltime = 0
                maxNumEvents = None
                multiplicand = 1
        # get site when splitting per site
        if siteName is not None:
            siteCandidate = self.siteCandidates[siteName]
        # use event ratios
        useEventRatio = self.useEventRatioForSec()
        # start splitting
        inputNumFiles = 0
        inputNumEvents = 0
        fileSize: float = 0
        firstLoop = True
        firstMaster = True
        inputFileMap: dict[str, Any] = {}
        expWalltime = 0
        nextStartEvent = None
        boundaryID = None
        newBoundaryID = False
        eventJump = False
        nSecFilesMap = {}
        nSecEventsMap = {}
        numMaster = 0
        masterSize = 0
        outSizeMap: dict[Any, Any] = {}  # keyed by datasetID
        lumiBlockNr = None
        newLumiBlockNr = False
        siteAvailable = True
        inputFileSet: set[Any] = set()
        fieldStr = None
        diskSize = 0
        totalNumFiles = 0
        dumpStr = ""
        currentLFN = None
        while no_split or (
            (maxNumFiles is None or (not dynNumEvents and inputNumFiles <= maxNumFiles) or (dynNumEvents and len(inputFileSet) <= maxNumFiles))
            and (maxSize is None or (maxSize is not None and fileSize <= maxSize))
            and (maxWalltime is None or maxWalltime <= 0 or expWalltime <= maxWalltime)
            and (maxNumEvents is None or (maxNumEvents is not None and inputNumEvents <= maxNumEvents))
            and (maxOutSize is None or self.getOutSize(outSizeMap) <= maxOutSize)
            and (maxDiskSize is None or diskSize <= maxDiskSize)
            and totalNumFiles <= self.maxTotalNumFiles
        ):
            # get one file (or one file group for MP) from master
            datasetUsage = self.datasetMap[master_dataset.datasetID]
            if master_dataset.datasetID not in outSizeMap:
                outSizeMap[master_dataset.datasetID] = 0
            boundaryIDs = set()
            primaryHasEvents = False
            for tmpFileSpec in master_dataset.Files[datasetUsage["used"] : datasetUsage["used"] + multiplicand]:
                # check start event to keep continuity
                if (maxNumEvents is not None or dynNumEvents) and tmpFileSpec.startEvent is not None:
                    if nextStartEvent is not None and nextStartEvent != tmpFileSpec.startEvent:
                        eventJump = True
                        break
                    elif nextStartEvent and nextStartEvent == tmpFileSpec.startEvent and currentLFN and currentLFN != tmpFileSpec.lfn:
                        eventJump = True
                        break
                # check boundaryID
                if (
                    splitWithBoundaryID
                    and useBoundary is not None
                    and boundaryID is not None
                    and boundaryID != tmpFileSpec.boundaryID
                    and useBoundary["inSplit"] != 3
                ):
                    newBoundaryID = True
                    break
                # check LB
                if respectLB and lumiBlockNr is not None and lumiBlockNr != tmpFileSpec.lumiBlockNr:
                    newLumiBlockNr = True
                    break
                # check field
                if splitByFields is not None:
                    tmpFieldStr = tmpFileSpec.extractFieldsStr(splitByFields)
                    if fieldStr is None:
                        fieldStr = tmpFieldStr
                    elif tmpFieldStr != fieldStr:
                        newBoundaryID = True
                        break
                # check for distributed datasets
                # if master_dataset.isDistributed() and siteName is not None and \
                #        not siteCandidate.isAvailableFile(tmpFileSpec):
                #    siteAvailable = False
                #    break
                if master_dataset.datasetID not in inputFileMap:
                    inputFileMap[master_dataset.datasetID] = []
                inputFileMap[master_dataset.datasetID].append(tmpFileSpec)
                inputFileSet.add(tmpFileSpec.lfn)
                datasetUsage["used"] += 1
                numMaster += 1
                # get effective file size
                effectiveFsize = CoreUtils.getEffectiveFileSize(tmpFileSpec.fsize, tmpFileSpec.startEvent, tmpFileSpec.endEvent, tmpFileSpec.nEvents)
                # get num of events
                effectiveNumEvents = tmpFileSpec.getEffectiveNumEvents()
                # sum
                inputNumFiles += 1
                totalNumFiles += 1
                if self.taskSpec.outputScaleWithEvents():
                    tmpOutSize = int(sizeGradients * effectiveNumEvents)
                    fileSize += tmpOutSize
                    diskSize += tmpOutSize
                    if not dynNumEvents or tmpFileSpec.lfn not in inputFileSet:
                        fileSize += int(tmpFileSpec.fsize)
                        masterSize += int(tmpFileSpec.fsize)
                        if not useDirectIO:
                            diskSize += int(tmpFileSpec.fsize)
                    outSizeMap[master_dataset.datasetID] += int(sizeGradients * effectiveNumEvents)
                else:
                    tmpOutSize = int(sizeGradients * effectiveFsize)
                    fileSize += tmpOutSize
                    diskSize += tmpOutSize
                    if not dynNumEvents or tmpFileSpec.lfn not in inputFileSet:
                        fileSize += int(tmpFileSpec.fsize)
                        masterSize += int(tmpFileSpec.fsize)
                        if not useDirectIO:
                            diskSize += int(tmpFileSpec.fsize)
                    outSizeMap[master_dataset.datasetID] += int(sizeGradients * effectiveFsize)
                if sizeGradientsPerInSize is not None:
                    tmpOutSize = int(effectiveFsize * sizeGradientsPerInSize)
                    fileSize += tmpOutSize
                    diskSize += tmpOutSize
                    outSizeMap[master_dataset.datasetID] += int(effectiveFsize * sizeGradientsPerInSize)
                # sum offset only for the first master
                if firstMaster:
                    fileSize += sizeIntercepts
                # walltime
                if self.taskSpec.useHS06():
                    if firstMaster and self.taskSpec.baseWalltime is not None:
                        expWalltime += self.taskSpec.baseWalltime
                    tmpExpWalltime = walltimeGradient * effectiveNumEvents / float(coreCount)
                    if corePower not in [None, 0]:
                        tmpExpWalltime /= corePower
                    if not self.taskSpec.cpuEfficiency:
                        tmpExpWalltime = 0
                    else:
                        tmpExpWalltime /= float(self.taskSpec.cpuEfficiency) / 100.0
                    if multiplicity is not None:
                        tmpExpWalltime /= float(multiplicity)
                    expWalltime += int(tmpExpWalltime)
                else:
                    tmpExpWalltime = walltimeGradient * effectiveFsize / float(coreCount)
                    if multiplicity is not None:
                        tmpExpWalltime /= float(multiplicity)
                    expWalltime += int(tmpExpWalltime)
                # the number of events
                if (maxNumEvents is not None or useEventRatio or dynNumEvents) and tmpFileSpec.startEvent is not None and tmpFileSpec.endEvent is not None:
                    primaryHasEvents = True
                    inputNumEvents += tmpFileSpec.endEvent - tmpFileSpec.startEvent + 1
                    # set next start event
                    nextStartEvent = tmpFileSpec.endEvent + 1
                    if nextStartEvent == tmpFileSpec.nEvents:
                        nextStartEvent = 0
                    currentLFN = tmpFileSpec.lfn
                # boundaryID
                if splitWithBoundaryID:
                    boundaryID = tmpFileSpec.boundaryID
                    if boundaryID not in boundaryIDs:
                        boundaryIDs.add(boundaryID)
                # LB
                if respectLB:
                    lumiBlockNr = tmpFileSpec.lumiBlockNr
                firstMaster = False
            # get files from secondaries
            firstSecondary = True
            for datasetSpec in self.secondaryDatasetList:
                if datasetSpec.datasetID not in outSizeMap:
                    outSizeMap[datasetSpec.datasetID] = 0
                if datasetSpec.isNoSplit():
                    # every job uses dataset without splitting
                    if firstLoop:
                        datasetUsage = self.datasetMap[datasetSpec.datasetID]
                        for tmpFileSpec in datasetSpec.Files:
                            if datasetSpec.datasetID not in inputFileMap:
                                inputFileMap[datasetSpec.datasetID] = []
                            inputFileMap[datasetSpec.datasetID].append(tmpFileSpec)
                            # sum
                            fileSize += tmpFileSpec.fsize
                            if not useDirectIO:
                                diskSize += tmpFileSpec.fsize
                            if sizeGradientsPerInSize is not None:
                                tmpOutSize = tmpFileSpec.fsize * sizeGradientsPerInSize
                                fileSize += tmpOutSize
                                diskSize += tmpOutSize
                                outSizeMap[datasetSpec.datasetID] += tmpFileSpec.fsize * sizeGradientsPerInSize
                            datasetUsage["used"] += 1
                            totalNumFiles += 1
                else:
                    if datasetSpec.datasetID not in nSecFilesMap:
                        nSecFilesMap[datasetSpec.datasetID] = 0
                    # get number of files to be used for the secondary
                    nSecondary = datasetSpec.getNumFilesPerJob()
                    if nSecondary is not None and firstLoop is False:
                        # read files only in the first bunch when number of files per job is specified
                        continue
                    if nSecondary is None:
                        nSecondary = datasetSpec.getNumMultByRatio(numMaster) - nSecFilesMap[datasetSpec.datasetID]
                        if (datasetSpec.getEventRatio() is not None and inputNumEvents > 0) or (
                            splitWithBoundaryID and useBoundary is not None and useBoundary["inSplit"] != 3
                        ):
                            # set large number to get all associated secondary files
                            nSecondary = 10000
                    datasetUsage = self.datasetMap[datasetSpec.datasetID]
                    # reset nUsed
                    if datasetSpec.isReusable() and datasetUsage["used"] + nSecondary > len(datasetSpec.Files):
                        datasetUsage["used"] = 0
                    for tmpFileSpec in datasetSpec.Files[datasetUsage["used"] : datasetUsage["used"] + nSecondary]:
                        # check boundaryID
                        if (
                            (splitWithBoundaryID or (useBoundary is not None and useBoundary["inSplit"] == 3 and datasetSpec.getRatioToMaster() > 1))
                            and boundaryID is not None
                            and not (boundaryID == tmpFileSpec.boundaryID or tmpFileSpec.boundaryID in boundaryIDs)
                        ):
                            break
                        # check for distributed datasets
                        # if datasetSpec.isDistributed() and siteName is not None and \
                        #        not siteCandidate.isAvailableFile(tmpFileSpec):
                        #    break
                        # check ratio
                        if datasetSpec.datasetID not in nSecEventsMap:
                            nSecEventsMap[datasetSpec.datasetID] = 0
                        if datasetSpec.getEventRatio() is not None and inputNumEvents > 0:
                            if float(nSecEventsMap[datasetSpec.datasetID]) / float(inputNumEvents) >= datasetSpec.getEventRatio():
                                break
                        if datasetSpec.datasetID not in inputFileMap:
                            inputFileMap[datasetSpec.datasetID] = []
                        inputFileMap[datasetSpec.datasetID].append(tmpFileSpec)
                        # sum
                        fileSize += tmpFileSpec.fsize
                        if not useDirectIO:
                            diskSize += tmpFileSpec.fsize
                        if sizeGradientsPerInSize is not None:
                            tmpOutSize = tmpFileSpec.fsize * sizeGradientsPerInSize
                            fileSize += tmpOutSize
                            diskSize += tmpOutSize
                            outSizeMap[datasetSpec.datasetID] += tmpFileSpec.fsize * sizeGradientsPerInSize
                        datasetUsage["used"] += 1
                        nSecFilesMap[datasetSpec.datasetID] += 1
                        totalNumFiles += 1
                        # the number of events
                        if firstSecondary and maxNumEvents is not None and not primaryHasEvents:
                            if tmpFileSpec.startEvent is not None and tmpFileSpec.endEvent is not None:
                                inputNumEvents += tmpFileSpec.endEvent - tmpFileSpec.startEvent + 1
                            elif tmpFileSpec.nEvents is not None:
                                inputNumEvents += tmpFileSpec.nEvents
                        if tmpFileSpec.nEvents is not None:
                            nSecEventsMap[datasetSpec.datasetID] += tmpFileSpec.nEvents
                    # use only the first secondary
                    firstSecondary = False
            # unset first loop flag
            firstLoop = False
            # check if there are unused files/evets
            if not self.checkUnused():
                dumpStr = "no more files"
                break
            # break if nFilesPerJob is used as multiplicand
            if nFilesPerJob is not None and not respectLB:
                dumpStr = "nFilesPerJob specified"
                break
            # boundaryID is changed
            if newBoundaryID:
                dumpStr = "new BoundaryID"
                break
            # LB is changed
            if newLumiBlockNr:
                dumpStr = "new LB"
                break
            # event jump
            if eventJump:
                dumpStr = "event jump"
                break
            # distributed files are unavailable
            if not siteAvailable:
                dumpStr = "distributed files are unavailable"
                break
            primaryHasEvents = False
            # check master in the next loop
            datasetUsage = self.datasetMap[master_dataset.datasetID]
            newInputNumFiles = inputNumFiles
            newInputNumEvents = inputNumEvents
            newFileSize = fileSize
            newExpWalltime = expWalltime
            newNextStartEvent = nextStartEvent
            newNumMaster = numMaster
            terminateFlag = False
            newOutSizeMap = copy.copy(outSizeMap)
            newBoundaryIDs = set()
            newInputFileSet = copy.copy(inputFileSet)
            newDiskSize = diskSize
            new_nSecEventsMap = copy.copy(nSecEventsMap)
            newTotalNumFiles = totalNumFiles
            if master_dataset.datasetID not in newOutSizeMap:
                newOutSizeMap[master_dataset.datasetID] = 0
            for tmpFileSpec in master_dataset.Files[datasetUsage["used"] : datasetUsage["used"] + multiplicand]:
                # check continuity of event
                if maxNumEvents is not None and tmpFileSpec.startEvent is not None and tmpFileSpec.endEvent is not None:
                    primaryHasEvents = True
                    newInputNumEvents += tmpFileSpec.endEvent - tmpFileSpec.startEvent + 1
                    # continuity of event is broken
                    if newNextStartEvent is not None and newNextStartEvent != tmpFileSpec.startEvent:
                        # no files in the next loop
                        if newInputNumFiles == 0:
                            dumpStr = "no files with continuous events in the next loop"
                            terminateFlag = True
                        break
                    newNextStartEvent = tmpFileSpec.endEvent + 1
                # check boundary
                if (
                    splitWithBoundaryID
                    and useBoundary is not None
                    and boundaryID is not None
                    and boundaryID != tmpFileSpec.boundaryID
                    and useBoundary["inSplit"] != 3
                ):
                    # no files in the next loop
                    if newInputNumFiles == 0:
                        dumpStr = "no files with the same BoundaryID in the next loop"
                        terminateFlag = True
                    break
                # check LB
                if respectLB and lumiBlockNr is not None and lumiBlockNr != tmpFileSpec.lumiBlockNr:
                    # no files in the next loop
                    if newInputNumFiles == 0:
                        dumpStr = "no files with the same LB in the next loop"
                        terminateFlag = True
                    break
                # check field
                if splitByFields is not None:
                    tmpFieldStr = tmpFileSpec.extractFieldsStr(splitByFields)
                    if tmpFieldStr != fieldStr:
                        # no files in the next loop
                        if newInputNumFiles == 0:
                            dumpStr = "no files with the same LFN field in the next loop"
                            terminateFlag = True
                        break
                # check for distributed datasets
                # if master_dataset.isDistributed() and siteName is not None and \
                #        not siteCandidate.isAvailableFile(tmpFileSpec):
                #    # no files in the next loop
                #    if newInputNumFiles == 0:
                #        terminateFlag = True
                #    break
                # get effective file size
                effectiveFsize = CoreUtils.getEffectiveFileSize(tmpFileSpec.fsize, tmpFileSpec.startEvent, tmpFileSpec.endEvent, tmpFileSpec.nEvents)
                # get num of events
                effectiveNumEvents = tmpFileSpec.getEffectiveNumEvents()
                newInputNumFiles += 1
                newNumMaster += 1
                newTotalNumFiles += 1
                newInputFileSet.add(tmpFileSpec.lfn)
                if self.taskSpec.outputScaleWithEvents():
                    tmpOutSize = int(sizeGradients * effectiveNumEvents)
                    newFileSize += tmpOutSize
                    newDiskSize += tmpOutSize
                    if not dynNumEvents or tmpFileSpec.lfn not in inputFileSet:
                        newFileSize += int(tmpFileSpec.fsize)
                        if not useDirectIO:
                            newDiskSize += int(tmpFileSpec.fsize)
                    newOutSizeMap[master_dataset.datasetID] += int(sizeGradients * effectiveNumEvents)
                else:
                    tmpOutSize = int(sizeGradients * effectiveFsize)
                    newFileSize += tmpOutSize
                    newDiskSize += tmpOutSize
                    if not dynNumEvents or tmpFileSpec.lfn not in inputFileSet:
                        newFileSize += int(tmpFileSpec.fsize)
                        if not useDirectIO:
                            newDiskSize += int(tmpFileSpec.fsize)
                    newOutSizeMap[master_dataset.datasetID] += int(sizeGradients * effectiveFsize)
                if sizeGradientsPerInSize is not None:
                    tmpOutSize = int(effectiveFsize * sizeGradientsPerInSize)
                    newFileSize += tmpOutSize
                    newDiskSize += tmpOutSize
                    newOutSizeMap[master_dataset.datasetID] += int(effectiveFsize * sizeGradientsPerInSize)
                if self.taskSpec.useHS06():
                    tmpExpWalltime = walltimeGradient * effectiveNumEvents / float(coreCount)
                    if corePower not in [None, 0]:
                        tmpExpWalltime /= corePower
                    if not self.taskSpec.cpuEfficiency:
                        tmpExpWalltime = 0
                    else:
                        tmpExpWalltime /= float(self.taskSpec.cpuEfficiency) / 100.0
                    if multiplicity is not None:
                        tmpExpWalltime /= float(multiplicity)
                    newExpWalltime += int(tmpExpWalltime)
                else:
                    tmpExpWalltime = walltimeGradient * effectiveFsize / float(coreCount)
                    if multiplicity is not None:
                        tmpExpWalltime /= float(multiplicity)
                    newExpWalltime += int(tmpExpWalltime)
                # boundaryID
                if splitWithBoundaryID:
                    newBoundaryIDs.add(tmpFileSpec.boundaryID)
            # check secondaries
            firstSecondary = True
            newSecMap: dict[str, Any] = {}
            for datasetSpec in self.secondaryDatasetList:
                if datasetSpec.datasetID not in newOutSizeMap:
                    newOutSizeMap[datasetSpec.datasetID] = 0
                if not datasetSpec.isNoSplit() and datasetSpec.getNumFilesPerJob() is None:
                    # check boundaryID
                    if (
                        splitWithBoundaryID
                        and useBoundary is not None
                        and boundaryID is not None
                        and boundaryID != tmpFileSpec.boundaryID
                        and useBoundary["inSplit"] != 3
                    ):
                        break
                    newSecMap.setdefault(datasetSpec.datasetID, {"in_size": 0, "out_size": 0})
                    newNumSecondary = datasetSpec.getNumMultByRatio(newNumMaster) - nSecFilesMap[datasetSpec.datasetID]
                    if newNumSecondary < 0:
                        newNumSecondary = 0
                    newSecMap[datasetSpec.datasetID]["nSec"] = newNumSecondary
                    newSecMap[datasetSpec.datasetID]["nSecReal"] = 0
                    datasetUsage = self.datasetMap[datasetSpec.datasetID]
                    for tmpFileSpec in datasetSpec.Files[datasetUsage["used"] : datasetUsage["used"] + newNumSecondary]:
                        # check boundaryID
                        if (
                            splitWithBoundaryID
                            and boundaryID is not None
                            and boundaryID != tmpFileSpec.boundaryID
                            and tmpFileSpec.boundaryID not in boundaryIDs
                            and tmpFileSpec.boundaryID not in newBoundaryIDs
                        ):
                            break
                        # check ratio
                        if datasetSpec.getEventRatio() is not None and newInputNumEvents > 0:
                            if float(new_nSecEventsMap[datasetSpec.datasetID]) / float(newInputNumEvents) >= datasetSpec.getEventRatio():
                                break
                        newFileSize += tmpFileSpec.fsize
                        newSecMap[datasetSpec.datasetID]["in_size"] += tmpFileSpec.fsize
                        newSecMap[datasetSpec.datasetID]["nSecReal"] += 1
                        newTotalNumFiles += 1
                        if not useDirectIO:
                            newDiskSize += tmpFileSpec.fsize
                        if sizeGradientsPerInSize is not None:
                            tmpOutSize = tmpFileSpec.fsize * sizeGradientsPerInSize
                            newFileSize += tmpOutSize
                            newDiskSize += tmpOutSize
                            newOutSizeMap[datasetSpec.datasetID] += tmpOutSize
                            newSecMap[datasetSpec.datasetID]["out_size"] += tmpOutSize
                        # the number of events
                        if firstSecondary and maxNumEvents is not None and not primaryHasEvents:
                            if tmpFileSpec.startEvent is not None and tmpFileSpec.endEvent is not None:
                                newInputNumEvents += tmpFileSpec.endEvent - tmpFileSpec.startEvent + 1
                            elif tmpFileSpec.nEvents is not None:
                                newInputNumEvents += tmpFileSpec.nEvents
                        if tmpFileSpec.nEvents is not None:
                            new_nSecEventsMap[datasetSpec.datasetID] += tmpFileSpec.nEvents
                    firstSecondary = False
            # no split:
            if no_split:
                continue
            # termination
            if terminateFlag:
                break
            # check
            newOutSize = self.getOutSize(newOutSizeMap)
            if (
                (maxNumFiles is not None and ((not dynNumEvents and newInputNumFiles > maxNumFiles) or (dynNumEvents and (len(newInputFileSet) > maxNumFiles))))
                or (maxSize is not None and newFileSize > maxSize)
                or (maxSize is not None and newOutSize < minOutSize and maxSize - minOutSize < newFileSize - newOutSize)
                or (maxWalltime is not None and 0 < maxWalltime < newExpWalltime)
                or (maxNumEvents is not None and newInputNumEvents > maxNumEvents)
                or (maxOutSize is not None and self.getOutSize(newOutSizeMap) > maxOutSize)
                or (maxDiskSize is not None and newDiskSize > maxDiskSize)
                or newTotalNumFiles > self.maxTotalNumFiles
            ):
                dumpStr = "check for the next loop. "
                if maxNumFiles is not None and (not dynNumEvents and newInputNumFiles > maxNumFiles):
                    dumpStr += f"maxNumFiles exceeds maxNumFiles={maxNumFiles} inputNumFiles={inputNumFiles} newInputNumFiles={newInputNumFiles}. "
                if maxSize is not None and newFileSize > maxSize:
                    dumpStr += "maxSize exceeds maxSize={} fileSize={} masterSize={} newFileSize={} newSecMap={}. ".format(
                        self.get_value_str(maxSize),
                        self.get_value_str(fileSize),
                        self.get_value_str(masterSize),
                        self.get_value_str(newFileSize),
                        str(newSecMap),
                    )
                if maxSize is not None and newOutSize < minOutSize and maxSize - minOutSize < newFileSize - newOutSize:
                    dumpStr += "maxSize exceeds with outSize maxSize={} minOutSize={} fileSize={} newFileSize={} newOutSize={}. ".format(
                        self.get_value_str(maxSize),
                        self.get_value_str(minOutSize),
                        self.get_value_str(fileSize),
                        self.get_value_str(newFileSize),
                        self.get_value_str(newOutSize),
                    )
                if maxWalltime is not None and 0 < maxWalltime < newExpWalltime:
                    dumpStr += f"maxWalltime exceeds maxWalltime={maxWalltime} expWalltime={expWalltime} newExpWalltime={newExpWalltime}. "
                if maxNumEvents is not None and newInputNumEvents > maxNumEvents:
                    dumpStr += f"maxNumEvents exceeds maxNumEvents={maxNumEvents} inputNumEvents={inputNumEvents} newInputNumEvents={newInputNumEvents}. "
                if maxOutSize is not None and self.getOutSize(newOutSizeMap) > maxOutSize:
                    dumpStr += "maxOutSize exceeds maxOutSize={} getOutSize(newOutSizeMap)={}. ".format(
                        self.get_value_str(maxOutSize), self.get_value_str(self.getOutSize(newOutSizeMap))
                    )
                if maxDiskSize is not None and newDiskSize > maxDiskSize:
                    dumpStr += "maxDiskSize exceeds maxDiskSize={} diskSize={} newDiskSize={}. ".format(
                        self.get_value_str(maxDiskSize), self.get_value_str(diskSize), self.get_value_str(newDiskSize)
                    )
                if newTotalNumFiles > self.maxTotalNumFiles:
                    dumpStr += f"total num of input files exceeds {self.maxTotalNumFiles}. "
                break
        # reset nUsed for repeated datasets
        for tmpDatasetID, datasetUsage in self.datasetMap.items():
            tmpDatasetSpec = datasetUsage["datasetSpec"]
            if tmpDatasetSpec.isRepeated():
                if len(tmpDatasetSpec.Files) > 0:
                    datasetUsage["used"] %= len(tmpDatasetSpec.Files)
        # check min walltime
        if dynNumEvents and min_walltime and min_walltime > expWalltime:
            if enableLog and tmpLog:
                tmpLog.debug(f"expected walltime {expWalltime} less than min walltime {min_walltime} at {siteName}")
                return [], is_short
        # make copy to return
        returnList = []
        for tmpDatasetID, inputFileList in inputFileMap.items():
            tmpRetList = []
            for tmpFileSpec in inputFileList:
                # split par site or get atomic subchunk
                if siteName is not None:
                    # make copy to individually set locality
                    newFileSpec = copy.copy(tmpFileSpec)
                    # set locality
                    newFileSpec.locality = siteCandidate.getFileLocality(tmpFileSpec)
                    if newFileSpec.locality == "remote":
                        newFileSpec.sourceName = siteCandidate.remoteSource
                    # append
                    tmpRetList.append(newFileSpec)
                else:
                    # getting atomic subchunk
                    tmpRetList.append(tmpFileSpec)
            # add to return map
            tmpDatasetSpec = self.getDatasetWithID(tmpDatasetID)
            returnList.append((tmpDatasetSpec, tmpRetList))
        # dump only problematic splitting
        err_msg = ""
        if returnList:
            if maxNumEvents and inputNumEvents < maxNumEvents:
                is_short = True
                err_msg = f"not enough events {maxNumEvents}>{inputNumEvents} at {siteName}: numMaster={numMaster}. {dumpStr}"
            elif nFilesPerJob and inputNumFiles < nFilesPerJob:
                is_short = True
                err_msg = f"not enough files {nFilesPerJob}>{inputNumFiles} at {siteName}. {dumpStr}"
        # return empty to skip input chunks producing output files with insufficient events
        if is_short:
            if enableLog and tmpLog:
                tmpLog.debug(err_msg)
            if skip_short_output:
                return None, is_short
        # return
        return returnList, is_short

    # check if master is mutable
    def isMutableMaster(self) -> bool:
        if self.masterDataset is not None and self.masterDataset.state == "mutable":
            return True
        return False

    # figure out if output will go through express stream
    def isExpress(self) -> bool:
        if self.taskSpec.processingType == "urgent" or (self.taskSpec.currentPriority is not None and self.taskSpec.currentPriority > 1000):
            return True

        return False

    # get max ramCount
    def getMaxRamCount(self) -> int:
        ram_count = self.ramCount if self.ramCount is not None else 0
        if self.isMerging:
            return max(self.taskSpec.mergeRamCount, ram_count, 2000) if self.taskSpec.mergeRamCount else ram_count
        else:
            return max(self.taskSpec.ramCount, ram_count) if self.taskSpec.ramCount else ram_count

    # get site candidate
    def getSiteCandidate(self, name: str) -> Any:
        if name in self.siteCandidates:
            return self.siteCandidates[name]
        return None

    # get list of candidate names
    def get_candidate_names(self) -> list[str]:
        return list(self.siteCandidates.keys())

    # update number of queued jobs
    def update_n_queue(self, live_counter: Any) -> str:
        sites = []
        for siteCandidate in self.siteCandidates.values():
            if live_counter is not None:
                n = live_counter.get(siteCandidate.siteName)
                if n > 0:
                    siteCandidate.nQueuedJobs += n
                    sites.append(siteCandidate.siteName)
        return ",".join(sites)

    # check event continuity
    def check_event_jump_and_sum(self) -> tuple[bool, int]:
        if self.masterDataset is None:
            # every file and event this method walks comes from the master dataset,
            # so a chunk built without one has nothing to work on here
            raise RuntimeError("check_event_jump_and_sum() called without a master dataset")
        nextStartEvent = None
        eventJump = False
        totalEvents = 0
        currentLFN = None
        maxChunk = 0
        datasetUsage = self.datasetMap[self.masterDataset.datasetID]
        for tmpFileSpec in self.masterDataset.Files[datasetUsage["used"] :]:
            if tmpFileSpec.startEvent is not None:
                if nextStartEvent is not None and nextStartEvent != tmpFileSpec.startEvent:
                    maxChunk = max(maxChunk, totalEvents)
                    eventJump = True
                    totalEvents = 0
                elif nextStartEvent and currentLFN != tmpFileSpec.lfn:
                    maxChunk = max(maxChunk, totalEvents)
                    eventJump = True
                    totalEvents = 0
                nextStartEvent = tmpFileSpec.endEvent + 1
                if nextStartEvent == tmpFileSpec.nEvents:
                    nextStartEvent = 0
                totalEvents += tmpFileSpec.endEvent - tmpFileSpec.startEvent + 1
            elif tmpFileSpec.endEvent:
                totalEvents += tmpFileSpec.endEvent
            currentLFN = tmpFileSpec.lfn
        maxChunk = max(maxChunk, totalEvents)
        return eventJump, maxChunk
