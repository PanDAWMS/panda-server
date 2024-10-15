import datetime
import functools
import re
import time
import traceback
import uuid

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandalogger.PandaLogger import PandaLogger

from pandaserver.config import panda_config
from pandaserver.dataservice import DataServiceUtils
from pandaserver.dataservice.DataServiceUtils import select_scope
from pandaserver.dataservice.ddm import rucioAPI
from pandaserver.taskbuffer import ProcessGroups

_log = PandaLogger().getLogger("broker")

# all known sites
_allSites = []

# processingType to skip brokerage
skipBrokerageProTypes = ["prod_test"]


# comparison function for sort
def _compFunc(job_a, job_b):
    # This comparison helps in sorting the jobs based on the order of their computing sites in the _allSites list.
    # append site if not in list
    if job_a.computingSite not in _allSites:
        _allSites.append(job_a.computingSite)
    if job_b.computingSite not in _allSites:
        _allSites.append(job_b.computingSite)

    # compare site indices
    index_a = _allSites.index(job_a.computingSite)
    index_b = _allSites.index(job_b.computingSite)

    if index_a > index_b:
        return 1
    elif index_a < index_b:
        return -1
    else:
        return 0


# get list of files which already exist at the site
def _getOkFiles(
    v_ce,
    v_files,
    allLFNs,
    allOkFilesMap,
    prodsourcelabel,
    job_label,
    tmpLog=None,
    allScopeList=None,
):
    scope_association_input, _ = select_scope(v_ce, prodsourcelabel, job_label)
    rucio_sites = list(v_ce.setokens_input[scope_association_input].values())
    try:
        rucio_sites.remove("")
    except Exception:
        pass
    rucio_sites.sort()
    if not rucio_sites:
        rucio_site = v_ce.ddm_input[scope_association_input]
    else:
        rucio_site = ""
        for tmpID in rucio_sites:
            rucio_site += f"{tmpID},"
        rucio_site = rucio_site[:-1]

    # set LFC and SE name
    rucio_url = "rucio://atlas-rucio.cern.ch:/grid/atlas"
    tmpSE = v_ce.ddm_endpoints_input[scope_association_input].getAllEndPoints()
    if tmpLog is not None:
        tmpLog.debug(f"getOkFiles for {v_ce.sitename} with rucio_site:{rucio_site}, rucio_url:{rucio_url}, SE:{str(tmpSE)}")
    anyID = "any"

    # use bulk lookup
    if allLFNs != []:
        # get all replicas
        if rucio_url not in allOkFilesMap:
            allOkFilesMap[rucio_url] = {}
            tmpStat, tmpAvaFiles = rucioAPI.list_file_replicas(allScopeList, allLFNs, tmpSE)
            if not tmpStat and tmpLog is not None:
                tmpLog.debug("getOkFile failed to get file replicas")
                tmpAvaFiles = {}
            allOkFilesMap[rucio_url][anyID] = tmpAvaFiles

        # get files for each rucio_site
        if rucio_site not in allOkFilesMap[rucio_url]:
            allOkFilesMap[rucio_url][rucio_site] = allOkFilesMap[rucio_url][anyID]

        # make return map
        retMap = {}
        for tmpLFN in v_files:
            if tmpLFN in allOkFilesMap[rucio_url][rucio_site]:
                retMap[tmpLFN] = allOkFilesMap[rucio_url][rucio_site][tmpLFN]
        tmpLog.debug("getOkFiles done")

        return retMap
    else:
        # old style
        tmpLog.debug("getOkFiles old")
        return {}


# set 'ready' if files are already there
def _setReadyToFiles(tmpJob, okFiles, siteMapper, tmpLog):
    tmpLog.debug(str(okFiles))
    allOK = True
    tmpSiteSpec = siteMapper.getSite(tmpJob.computingSite)
    scope_association_site_input, _ = select_scope(tmpSiteSpec, tmpJob.prodSourceLabel, tmpJob.job_label)
    tmpTapeEndPoints = tmpSiteSpec.ddm_endpoints_input[scope_association_site_input].getTapeEndPoints()

    for tmpFile in tmpJob.Files:
        if tmpFile.type == "input":
            if tmpFile.status == "ready":
                tmpFile.dispatchDBlock = "NULL"
            elif DataServiceUtils.isCachedFile(tmpFile.dataset, tmpSiteSpec):
                # cached file
                tmpFile.status = "cached"
                tmpFile.dispatchDBlock = "NULL"
            # use DDM prestage only for on-tape files
            elif len(tmpTapeEndPoints) > 0 and tmpFile.lfn in okFiles:
                tapeOnly = True
                tapeCopy = False
                for tmpSE in okFiles[tmpFile.lfn]:
                    if tmpSE not in tmpTapeEndPoints:
                        tapeOnly = False
                    else:
                        # there is a tape copy
                        tapeCopy = True

                # trigger prestage when disk copy doesn't exist or token is TAPE
                if tapeOnly or (tapeCopy and tmpFile.dispatchDBlockToken in ["ATLASDATATAPE", "ATLASMCTAPE"]):
                    allOK = False
                else:
                    # set ready
                    tmpFile.status = "ready"
                    tmpFile.dispatchDBlock = "NULL"
            else:
                # set ready if the file exists and the site doesn't use prestage
                tmpFile.status = "ready"
                tmpFile.dispatchDBlock = "NULL"
    # unset disp dataset
    if allOK:
        tmpJob.dispatchDBlock = "NULL"


def schedule(jobs, taskBuffer, siteMapper):
    timestamp = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None).isoformat("/")
    tmpLog = LogWrapper(_log, f"start_ts={timestamp}")

    try:
        # no jobs
        if len(jobs) == 0:
            tmpLog.debug("finished : no jobs")
            return

        allOkFilesMap = {}

        nJob = 20
        iJob = 0
        nFile = 20
        fileList = []
        scopeList = []
        okFiles = {}
        prioInterval = 50
        totalNumInputs = 0
        totalInputSize = 0
        chosen_panda_queue = None
        prodDBlock = None
        computingSite = None
        dispatchDBlock = None
        previousCloud = None
        prevRelease = None
        prevMemory = None
        prevCmtConfig = None
        prevProType = None
        prevSourceLabel = None
        prevDiskCount = None
        prevDirectAcc = None
        prevCoreCount = None
        prevIsJEDI = None
        prevDDM = None
        prevBrokerageSiteList = None
        prevWorkingGroup = None
        prevMaxCpuCount = None
        prevPriority = None

        indexJob = 0

        prestageSites = []

        # JEDI sets lockedby='jedi' to all jobs. The only jobs in ATLAS that bypass JEDI are coming from hammercloud and they are setting the computingSite
        # check if all jobs come from JEDI
        onlyJEDI = all(tmpJob.lockedby == "jedi" for tmpJob in jobs)

        # get statistics
        newJobStatWithPrio = {}
        jobStatBrokerCloudsWithPrio = {}

        # sort jobs by siteID. Some jobs may already define computingSite
        jobs = sorted(jobs, key=functools.cmp_to_key(_compFunc))

        loggerMessages = []
        # get all input files for bulk LFC lookup
        allLFNs = []
        allGUIDs = []
        allScopes = []
        for tmpJob in jobs:
            if tmpJob.prodSourceLabel in ("test", "managed") or tmpJob.prodUserName in ["gangarbt"]:
                for tmpFile in tmpJob.Files:
                    if tmpFile.type == "input" and tmpFile.lfn not in allLFNs:
                        allLFNs.append(tmpFile.lfn)
                        allGUIDs.append(tmpFile.GUID)
                        allScopes.append(tmpFile.scope)

        # loop over all jobs + terminator(None)
        for job in jobs + [None]:
            indexJob += 1
            # ignore failed jobs
            if job is None:
                pass
            elif job.jobStatus == "failed":
                continue

            # list of sites for special brokerage
            specialBrokerageSiteList = []

            # manually set site
            if job is not None and job.computingSite != "NULL" and job.prodSourceLabel in ("test", "managed") and specialBrokerageSiteList == []:
                specialBrokerageSiteList = [job.computingSite]

            overwriteSite = False

            # check JEDI
            isJEDI = False
            if job is not None and job.lockedby == "jedi":
                isJEDI = True

            # new bunch or terminator
            if (
                job is None
                or len(fileList) >= nFile
                or (dispatchDBlock is None and job.homepackage.startswith("AnalysisTransforms"))
                or prodDBlock != job.prodDBlock
                or job.computingSite != computingSite
                or iJob > nJob
                or previousCloud != job.getCloud()
                or prevRelease != job.AtlasRelease
                or prevCmtConfig != job.cmtConfig
                or (prevProType in skipBrokerageProTypes and iJob > 0)
                or prevDirectAcc != job.transferType
                or (prevMemory != job.minRamCount and not isJEDI)
                or (prevDiskCount != job.maxDiskCount and not isJEDI)
                or prevCoreCount != job.coreCount
                or prevWorkingGroup != job.workingGroup
                or prevProType != job.processingType
                or (prevMaxCpuCount != job.maxCpuCount and not isJEDI)
                or prevBrokerageSiteList != specialBrokerageSiteList
                or prevIsJEDI != isJEDI
                or prevDDM != job.getDdmBackEnd()
            ):
                if indexJob > 1:
                    tmpLog.debug("new bunch")
                    tmpLog.debug(f"  iJob           {iJob}")
                    tmpLog.debug(f"  cloud          {previousCloud}")
                    tmpLog.debug(f"  rel            {prevRelease}")
                    tmpLog.debug(f"  sourceLabel    {prevSourceLabel}")
                    tmpLog.debug(f"  cmtConfig      {prevCmtConfig}")
                    tmpLog.debug(f"  memory         {prevMemory}")
                    tmpLog.debug(f"  priority       {prevPriority}")
                    tmpLog.debug(f"  prodDBlock     {prodDBlock}")
                    tmpLog.debug(f"  computingSite  {computingSite}")
                    tmpLog.debug(f"  processingType {prevProType}")
                    tmpLog.debug(f"  workingGroup   {prevWorkingGroup}")
                    tmpLog.debug(f"  coreCount      {prevCoreCount}")
                    tmpLog.debug(f"  maxCpuCount    {prevMaxCpuCount}")
                    tmpLog.debug(f"  transferType   {prevDirectAcc}")
                    tmpLog.debug(f"  DDM            {prevDDM}")

                # determine site
                if (iJob == 0 or chosen_panda_queue != "TOBEDONE") and prevBrokerageSiteList in [None, []]:
                    # file scan for pre-assigned jobs
                    jobsInBunch = jobs[indexJob - iJob - 1 : indexJob - 1]
                    if (
                        jobsInBunch != []
                        and fileList != []
                        and (computingSite not in prestageSites)
                        and (jobsInBunch[0].prodSourceLabel in ["managed", "software"] or re.search("test", jobsInBunch[0].prodSourceLabel) is not None)
                    ):
                        # get site spec
                        tmp_chosen_panda_queue = siteMapper.getSite(computingSite)
                        # get files
                        okFiles = _getOkFiles(
                            tmp_chosen_panda_queue,
                            fileList,
                            allLFNs,
                            allOkFilesMap,
                            jobsInBunch[0].prodSourceLabel,
                            jobsInBunch[0].job_label,
                            tmpLog,
                            allScopes,
                        )

                        nOkFiles = len(okFiles)
                        tmpLog.debug(f"site:{computingSite} - nFiles:{nOkFiles}/{len(fileList)} {str(fileList)} {str(okFiles)}")
                        # loop over all jobs
                        for tmpJob in jobsInBunch:
                            # set 'ready' if files are already there
                            _setReadyToFiles(tmpJob, okFiles, siteMapper, tmpLog)
                else:
                    # load balancing
                    minSites = {}
                    if prevBrokerageSiteList != []:
                        # special brokerage
                        scanSiteList = prevBrokerageSiteList
                    else:
                        if siteMapper.checkCloud(previousCloud):
                            # use cloud sites
                            scanSiteList = siteMapper.getCloud(previousCloud)["sites"]

                    # the number/size of inputs per job
                    nFilesPerJob = float(totalNumInputs) / float(iJob)
                    inputSizePerJob = float(totalInputSize) / float(iJob)

                    # loop over all sites
                    for site in scanSiteList:
                        tmpLog.debug(f"calculate weight for site:{site}")
                        # _allSites may contain NULL after sort()
                        if site == "NULL":
                            tmpLog.debug("site is NULL")
                            continue

                        winv = 1

                        tmpLog.debug(f"Site:{site} 1/Weight:{winv}")

                        # choose largest nMinSites weights
                        minSites[site] = winv

                    # choose site
                    tmpLog.debug(f"Min Sites:{minSites}")
                    if len(fileList) == 0 or prevIsJEDI is True:
                        # choose min 1/weight
                        minSite = list(minSites)[0]
                        chosenCE = siteMapper.getSite(minSite)

                    # set job spec
                    tmpLog.debug(f"indexJob      : {indexJob}")
                    tmpLog.debug(f"nInputs/Job   : {nFilesPerJob}")
                    tmpLog.debug(f"inputSize/Job : {inputSizePerJob}")
                    for tmpJob in jobs[indexJob - iJob - 1 : indexJob - 1]:
                        # set computingSite
                        tmpJob.computingSite = chosenCE.sitename
                        tmpLog.debug(f"PandaID:{tmpJob.PandaID} -> site:{tmpJob.computingSite}")

                        # set ready if files are already there
                        if prevIsJEDI is False:
                            _setReadyToFiles(tmpJob, okFiles, siteMapper, tmpLog)
                        # update statistics
                        tmpProGroup = ProcessGroups.getProcessGroup(tmpJob.processingType)
                        if tmpJob.processingType in skipBrokerageProTypes:
                            # use original processingType since prod_test is in the test category and thus is interfered by validations
                            tmpProGroup = tmpJob.processingType

                        # update statistics by taking priorities into account
                        if prevSourceLabel in ["managed", "test"]:
                            newJobStatWithPrio.setdefault(prevPriority, {})
                            newJobStatWithPrio[prevPriority].setdefault(tmpJob.getCloud(), {})
                            newJobStatWithPrio[prevPriority][tmpJob.getCloud()].setdefault(tmpJob.computingSite, {})
                            newJobStatWithPrio[prevPriority][tmpJob.getCloud()][tmpJob.computingSite].setdefault(tmpProGroup, 0)
                            newJobStatWithPrio[prevPriority][tmpJob.getCloud()][tmpJob.computingSite][tmpProGroup] += 1
                # terminate
                if job is None:
                    break
                # reset iJob
                iJob = 0
                # reset file list
                fileList = []
                scopeList = []
                okFiles = {}
                totalNumInputs = 0
                totalInputSize = 0
                # create new dispDBlock
                if job.prodDBlock != "NULL":
                    # get datatype
                    try:
                        tmpDataType = job.prodDBlock.split(".")[-2]
                    except Exception:
                        # default
                        tmpDataType = "GEN"
                    if len(tmpDataType) > 20:
                        # avoid too long name
                        tmpDataType = "GEN"
                    transferType = "transfer"
                    if job.useInputPrestaging():
                        transferType = "prestaging"
                    dispatchDBlock = f"panda.{job.taskID}.{time.strftime('%m.%d')}.{tmpDataType}.{transferType}.{str(uuid.uuid4())}_dis{job.PandaID}"
                    tmpLog.debug(f"New dispatchDBlock: {dispatchDBlock}")
                prodDBlock = job.prodDBlock
                # already define computingSite
                if job.computingSite != "NULL":
                    # instantiate KnownSite
                    chosen_panda_queue = siteMapper.getSite(job.computingSite)

                    # if site doesn't exist, use the default site
                    if job.homepackage.startswith("AnalysisTransforms"):
                        if chosen_panda_queue.sitename == panda_config.def_sitename:
                            chosen_panda_queue = siteMapper.getSite(panda_config.def_queue)
                            overwriteSite = True
                else:
                    # default for Analysis jobs
                    if job.homepackage.startswith("AnalysisTransforms"):
                        chosen_panda_queue = siteMapper.getSite(panda_config.def_queue)
                        overwriteSite = True
                    else:
                        # set chosen_panda_queue
                        chosen_panda_queue = "TOBEDONE"
            # increment iJob
            iJob += 1
            # reserve computingSite and cloud
            computingSite = job.computingSite
            previousCloud = job.getCloud()
            prevRelease = job.AtlasRelease
            prevMemory = job.minRamCount
            prevCmtConfig = job.cmtConfig
            prevProType = job.processingType
            prevSourceLabel = job.prodSourceLabel
            prevDiskCount = job.maxDiskCount
            prevDirectAcc = job.transferType
            prevCoreCount = job.coreCount
            prevMaxCpuCount = job.maxCpuCount
            prevBrokerageSiteList = specialBrokerageSiteList
            prevWorkingGroup = job.workingGroup
            prevIsJEDI = isJEDI
            prevDDM = job.getDdmBackEnd()
            # truncate prio to avoid too many lookups
            if job.currentPriority not in [None, "NULL"]:
                prevPriority = (job.currentPriority / prioInterval) * prioInterval
            # assign site
            if chosen_panda_queue != "TOBEDONE":
                job.computingSite = chosen_panda_queue.sitename
                tmpLog.debug(f"PandaID:{job.PandaID} -> preset site:{chosen_panda_queue.sitename}")
                # set cloud
                if job.cloud in ["NULL", None, ""]:
                    job.cloud = chosen_panda_queue.cloud

            # set destinationSE
            destSE = job.destinationSE
            if siteMapper.checkCloud(job.getCloud()):
                # use cloud dest for non-existing sites
                if job.prodSourceLabel != "user" and job.destinationSE not in siteMapper.siteSpecList and job.destinationSE != "local":
                    if DataServiceUtils.checkJobDestinationSE(job) is not None:
                        destSE = DataServiceUtils.checkJobDestinationSE(job)
                    job.destinationSE = destSE

            if overwriteSite:
                # overwrite SE for analysis jobs which set non-existing sites
                destSE = job.computingSite
                job.destinationSE = destSE

            # set dispatchDBlock and destinationSE
            first = True
            for file in job.Files:
                # dispatchDBlock. Set dispDB for prestaging jobs too
                if (
                    file.type == "input"
                    and file.dispatchDBlock == "NULL"
                    and ((file.status not in ["ready", "missing", "cached"]) or job.computingSite in prestageSites)
                ):
                    if first:
                        first = False
                        job.dispatchDBlock = dispatchDBlock
                    file.dispatchDBlock = dispatchDBlock
                    file.status = "pending"
                    if file.lfn not in fileList:
                        fileList.append(file.lfn)
                        scopeList.append(file.scope)
                        try:
                            # get total number/size of inputs except DBRelease
                            # tgz inputs for evgen may be negligible
                            if re.search("\.tar\.gz", file.lfn) is None:
                                totalNumInputs += 1
                                totalInputSize += file.fsize
                        except Exception:
                            pass
                # destinationSE
                if file.type in ["output", "log"] and destSE != "":
                    if job.prodSourceLabel == "user" and job.computingSite == file.destinationSE:
                        pass
                    elif job.prodSourceLabel == "user" and prevIsJEDI is True and file.destinationSE not in ["", "NULL"]:
                        pass
                    elif destSE == "local":
                        pass
                    elif DataServiceUtils.getDistributedDestination(file.destinationDBlockToken) is not None:
                        pass
                    else:
                        file.destinationSE = destSE
                # pre-assign GUID to log
                if file.type == "log":
                    # generate GUID
                    file.GUID = str(uuid.uuid4())
        # send log messages
        try:
            for message in loggerMessages:
                # get logger
                _pandaLogger = PandaLogger()
                _pandaLogger.lock()
                _pandaLogger.setParams({"Type": "brokerage"})
                logger = _pandaLogger.getHttpLogger(panda_config.loggername)
                # add message
                logger.warning(message)
                # release HTTP handler
                _pandaLogger.release()
                time.sleep(1)
        except Exception:
            pass

        # finished
        tmpLog.debug(f"N lookup for prio : {len(jobStatBrokerCloudsWithPrio)}")
        tmpLog.debug("finished")

    except Exception as e:
        tmpLog.error(f"schedule : {str(e)} {traceback.format_exc()}")
