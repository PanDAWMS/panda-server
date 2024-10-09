import datetime
import functools
import re
import sys
import time
import traceback
import uuid

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandalogger.PandaLogger import PandaLogger

from pandaserver.brokerage import ErrorCode
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


# release checker
def _checkRelease(job_releases, site_releases):
    # Check for all on/off
    if "True" in site_releases:
        return True
    if "False" in site_releases:
        return False

    # Loop over all releases
    for release in job_releases.split("\n"):
        release_version = re.sub(r"^Atlas-", "", release)
        # Release not available
        if release_version not in site_releases:
            return False

    return True


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


# Check if the number/size of inputs is too large
def _isTooMuchInput(n_files_per_job, input_size_per_job):
    if n_files_per_job > 5 or input_size_per_job > 500 * 1024 * 1024:
        return True
    return False


# send analysis brokerage info to logger with HTTP
def sendMsgToLoggerHTTP(msgList, job):
    try:
        # logging
        iMsg = 0
        # message type
        msgType = "analy_brokerage"
        # make header
        if job.jobsetID not in [None, "NULL"]:
            msgHead = f"dn='{job.prodUserName}' : jobset={job.jobsetID} jobdef={job.jobDefinitionID}"
        else:
            msgHead = f"dn='{job.prodUserName}' : jobdef={job.jobDefinitionID}"
        for msgBody in msgList:
            # make message
            message = msgHead + " : " + msgBody
            # dump locally
            _log.debug(message)

            # get logger
            _pandaLogger = PandaLogger()
            _pandaLogger.lock()
            _pandaLogger.setParams({"Type": msgType})
            logger = _pandaLogger.getHttpLogger(panda_config.loggername)
            # add message
            logger.info(message)
            # release HTTP handler
            _pandaLogger.release()
            # sleep
            iMsg += 1
            if iMsg % 5 == 0:
                time.sleep(1)
    except Exception:
        errType, errValue = sys.exc_info()[:2]
        _log.error(f"sendMsgToLoggerHTTP : {errType} {errValue}")


# get Satellite candidates when files are missing at Satellite
def get_satellite_candidate_list(tmp_job, satellites_files_map):
    # no job or cloud information
    if not tmp_job or tmp_job.getCloud() not in satellites_files_map:
        return []

    # loop over all files
    tmp_t2_candidates = None
    for tmp_file in tmp_job.Files:
        if tmp_file.type == "input" and tmp_file.status == "missing":
            # no dataset info
            if tmp_file.dataset not in satellites_files_map[tmp_job.getCloud()]:
                return []
            # initial candidates
            if tmp_t2_candidates is None:
                tmp_t2_candidates = satellites_files_map[tmp_job.getCloud()][tmp_file.dataset]["sites"]

            # check all candidates
            new_t2_candidates = []
            for tmp_t2_candidate in tmp_t2_candidates:
                # site doesn't have the dataset
                if tmp_t2_candidate not in satellites_files_map[tmp_job.getCloud()][tmp_file.dataset]["sites"]:
                    continue

                # site has the file
                if tmp_file.lfn in satellites_files_map[tmp_job.getCloud()][tmp_file.dataset]["sites"][tmp_t2_candidate]:
                    if tmp_t2_candidate not in new_t2_candidates:
                        new_t2_candidates.append(tmp_t2_candidate)

            # set new candidates
            tmp_t2_candidates = new_t2_candidates
            if not tmp_t2_candidates:
                break

    # return [] if no missing files
    if tmp_t2_candidates is None:
        return []

    # return
    tmp_t2_candidates.sort()
    return tmp_t2_candidates


# make compact dialog message
def makeCompactDiagMessage(header, results):
    # Limit for compact format
    max_site_list = 5

    # Types for compact format
    compact_type_list = ["status", "cpucore"]

    # Message mapping for different result types
    message_map = {
        "rel": "missing rel/cache",
        "pilot": "no pilot",
        "status": "not online",
        "disk": "SE full",
        "memory": "RAM shortage",
        "transferring": "many transferring",
        "share": "zero share",
        "maxtime": "short walltime",
        "cpucore": "CPU core mismatch",
        "scratch": "small scratch disk",
    }

    # Initialize the return string with the header
    if header in ["", None]:
        return_string = "No candidate - "
    else:
        return_string = f"special brokerage for {header} - "

    # Count the number of sites per type
    num_type_map = {}
    for result_type, result_list in results.items():
        if len(result_list) == 0:
            continue
        num_sites = len(result_list)
        if num_sites not in num_type_map:
            num_type_map[num_sites] = []
        num_type_map[num_sites].append(result_type)

    # Sort the keys and determine the largest types
    num_type_keys = sorted(num_type_map)
    large_types = num_type_map[num_type_keys[-1]] if num_type_keys else None

    # Loop over all types and construct the message
    for num_type_key in num_type_keys:
        for result_type in num_type_map[num_type_key]:
            # Add the label for the result type
            if result_type in message_map:
                return_string += f"{message_map[result_type]} at "
            else:
                return_string += f"{result_type} at "

            # Use compact format or list all sites
            if (result_type in compact_type_list + large_types or len(results[result_type]) >= max_site_list) and header in ["", None, "reprocessing"]:
                return_string += f"{len(results[result_type])} site" if len(results[result_type]) == 1 else f"{len(results[result_type])} sites"
            else:
                return_string += ",".join(results[result_type])

            return_string += ". "

    # Remove the trailing period and space
    return_string = return_string.rstrip(". ")
    return return_string


def schedule(jobs, taskBuffer, siteMapper, replicaMap={}):
    timestamp = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None).isoformat("/")
    tmpLog = LogWrapper(_log, f"start_ts={timestamp}")

    try:
        tmpLog.debug(f"start replicaMap : {str(replicaMap)}")

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
        prevManualPreset = None
        prevWorkingGroup = None
        prevMaxCpuCount = None
        prevBrokerageNote = None
        prevPriority = None

        nWNmap = {}
        indexJob = 0

        prestageSites = []

        # JEDI sets lockedby='jedi' to all jobs. The only jobs in ATLAS that bypass JEDI are coming from hammercloud and they are setting the computingSite
        # check if all jobs come from JEDI
        onlyJEDI = all(tmpJob.lockedby == "jedi" for tmpJob in jobs)

        # get statistics
        newJobStatWithPrio = {}
        jobStatBrokerCloudsWithPrio = {}
        if len(jobs) > 0 and (jobs[0].processingType.startswith("gangarobot") or jobs[0].processingType.startswith("hammercloud") or onlyJEDI):
            # disable redundant counting for HC
            jobStatistics = {}
            jobStatBroker = {}
            jobStatBrokerClouds = {}

        else:
            jobStatistics = taskBuffer.getJobStatistics(forAnal=False)
            jobStatBroker = {}
            jobStatBrokerClouds = taskBuffer.getJobStatisticsBrokerage()

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
            # note for brokerage
            brokerageNote = ""

            # manually set site
            manualPreset = False
            if job is not None and job.computingSite != "NULL" and job.prodSourceLabel in ("test", "managed") and specialBrokerageSiteList == []:
                specialBrokerageSiteList = [job.computingSite]
                manualPreset = True
                brokerageNote = "presetSite"
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
                # brokerage decisions
                resultsForAnal = {
                    "rel": [],
                    "pilot": [],
                    "disk": [],
                    "status": [],
                    "weight": [],
                    "memory": [],
                    "share": [],
                    "transferring": [],
                    "cpucore": [],
                    "reliability": [],
                    "maxtime": [],
                    "scratch": [],
                }
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
                    nMinSites = 2
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

                    # found candidate
                    foundOneCandidate = False

                    # loop over all sites
                    for site in scanSiteList:
                        tmpLog.debug(f"calculate weight for site:{site}")
                        # _allSites may contain NULL after sort()
                        if site == "NULL":
                            continue
                        if prevIsJEDI:
                            winv = 1
                        else:
                            # get SiteSpec
                            if siteMapper.checkSite(site):
                                tmpSiteSpec = siteMapper.getSite(site)
                            else:
                                tmpLog.debug(f" skip: {site} doesn't exist in DB")
                                continue
                            # ignore test sites
                            if (prevManualPreset is False) and (site.endswith("test") or site.endswith("Test") or site.startswith("Test")):
                                continue
                            # ignore analysis queues
                            if not tmpSiteSpec.runs_production():
                                continue
                            # check status
                            if tmpSiteSpec.status in [
                                "offline",
                                "brokeroff",
                            ] and computingSite in ["NULL", None, ""]:
                                tmpLog.debug(f" skip: status {tmpSiteSpec.status}")
                                resultsForAnal["status"].append(site)
                                continue
                            if (
                                tmpSiteSpec.status == "test"
                                and (
                                    prevProType
                                    not in [
                                        "prod_test",
                                        "hammercloud",
                                        "gangarobot",
                                        "gangarobot-squid",
                                    ]
                                )
                                and prevSourceLabel not in ["test", "prod_test"]
                            ):
                                tmpLog.debug(f" skip: status {tmpSiteSpec.status} for {prevProType}")
                                resultsForAnal["status"].append(site)
                                continue
                            tmpLog.debug(f"   status={tmpSiteSpec.status}")
                            # check core count
                            if tmpSiteSpec.coreCount > 1:
                                # use multi-core queue only for multi-core jobs
                                if prevCoreCount not in [None, "NULL"] and prevCoreCount > 1:
                                    pass
                                else:
                                    tmpLog.debug(f"  skip: multi-core site ({tmpSiteSpec.coreCount} core) for job.coreCount={prevCoreCount}")
                                    resultsForAnal["cpucore"].append(site)
                                    continue
                            else:
                                # use single-core for single-core jobs
                                if prevCoreCount not in [None, "NULL"] and prevCoreCount > 1:
                                    tmpLog.debug(f"  skip: single-core site ({tmpSiteSpec.coreCount} core) for job.coreCount={prevCoreCount}")
                                    resultsForAnal["cpucore"].append(site)
                                    continue
                            # check max memory
                            if tmpSiteSpec.memory != 0 and prevMemory not in [
                                None,
                                0,
                                "NULL",
                            ]:
                                try:
                                    if int(tmpSiteSpec.memory) < int(prevMemory):
                                        tmpLog.debug(f"  skip: site memory shortage {tmpSiteSpec.memory}<{prevMemory}")
                                        resultsForAnal["memory"].append(site)
                                        continue
                                except Exception:
                                    errtype, errvalue = sys.exc_info()[:2]
                                    tmpLog.error(f"max memory check : {errtype} {errvalue}")
                            # check maxcpucount
                            if tmpSiteSpec.maxtime != 0 and prevMaxCpuCount not in [None, 0, "NULL"]:
                                try:
                                    if int(tmpSiteSpec.maxtime) < int(prevMaxCpuCount):
                                        tmpLog.debug(f"  skip: insufficient maxtime {tmpSiteSpec.maxtime}<{prevMaxCpuCount}")
                                        resultsForAnal["maxtime"].append(site)
                                        continue
                                except Exception:
                                    errtype, errvalue = sys.exc_info()[:2]
                                    tmpLog.error(f"maxtime check : {errtype} {errvalue}")
                            if tmpSiteSpec.mintime != 0 and prevMaxCpuCount not in [None, 0, "NULL"]:
                                try:
                                    if int(tmpSiteSpec.mintime) > int(prevMaxCpuCount):
                                        tmpLog.debug(f"  skip: insufficient job maxtime {prevMaxCpuCount}<{tmpSiteSpec.mintime}")
                                        resultsForAnal["maxtime"].append(site)
                                        continue
                                except Exception:
                                    errtype, errvalue = sys.exc_info()[:2]
                                    tmpLog.error(f"mintime check : {errtype} {errvalue}")
                            # check max work dir size
                            if tmpSiteSpec.maxwdir != 0 and (prevDiskCount not in [None, 0, "NULL"]):
                                try:
                                    if int(tmpSiteSpec.maxwdir) < int(prevDiskCount):
                                        tmpLog.debug(f"  skip: not enough disk {tmpSiteSpec.maxwdir}<{prevDiskCount}")
                                        resultsForAnal["scratch"].append(site)
                                        continue
                                except Exception:
                                    errtype, errvalue = sys.exc_info()[:2]
                                    tmpLog.error(f"disk check : {errtype} {errvalue}")
                            tmpLog.debug(f"   maxwdir={tmpSiteSpec.maxwdir}")

                            # get pilot statistics
                            nPilotsGet = 0
                            nPilotsUpdate = 0
                            if nWNmap == {}:
                                nWNmap = taskBuffer.getCurrentSiteData()
                            if site in nWNmap:
                                nPilots = nWNmap[site]["getJob"] + nWNmap[site]["updateJob"]
                                nPilotsGet = nWNmap[site]["getJob"]
                                nPilotsUpdate = nWNmap[site]["updateJob"]
                            elif site.split("/")[0] in nWNmap:
                                tmpID = site.split("/")[0]
                                nPilots = nWNmap[tmpID]["getJob"] + nWNmap[tmpID]["updateJob"]
                                nPilotsGet = nWNmap[tmpID]["getJob"]
                                nPilotsUpdate = nWNmap[tmpID]["updateJob"]
                            else:
                                nPilots = 0
                            tmpLog.debug(f" original nPilots:{nPilots} get:{nPilotsGet} update:{nPilotsUpdate}")
                            # limit on (G+1)/(U+1)
                            limitOnGUmax = 1.1
                            limitOnGUmin = 0.9
                            guRatio = float(1 + nPilotsGet) / float(1 + nPilotsUpdate)
                            if guRatio > limitOnGUmax:
                                nPilotsGet = limitOnGUmax * float(1 + nPilotsUpdate) - 1.0
                            elif guRatio < limitOnGUmin:
                                nPilotsGet = limitOnGUmin * float(1 + nPilotsUpdate) - 1.0
                            tmpLog.debug(f" limited nPilots:{nPilots} get:{nPilotsGet} update:{nPilotsUpdate}")
                            # if no pilots
                            if nPilots == 0 and nWNmap != {}:
                                tmpLog.debug(f" skip: {site} no pilot")
                                resultsForAnal["pilot"].append(site)
                                continue
                            # if no jobs in jobsActive/jobsDefined
                            jobStatistics.setdefault(
                                site,
                                {
                                    "assigned": 0,
                                    "activated": 0,
                                    "running": 0,
                                    "transferring": 0,
                                },
                            )

                            # get the process group
                            tmpProGroup = ProcessGroups.getProcessGroup(prevProType)
                            if prevProType in skipBrokerageProTypes:
                                # use original processingType since prod_test is in the test category and thus is interfered by validations
                                tmpProGroup = prevProType
                            # the number of assigned and activated
                            jobStatBrokerClouds.setdefault(previousCloud, {})
                            # use number of jobs in the cloud
                            jobStatBroker = jobStatBrokerClouds[previousCloud]
                            if site not in jobStatBroker:
                                jobStatBroker[site] = {}
                            if tmpProGroup not in jobStatBroker[site]:
                                jobStatBroker[site][tmpProGroup] = {
                                    "assigned": 0,
                                    "activated": 0,
                                    "running": 0,
                                    "transferring": 0,
                                }
                            # count # of assigned and activated jobs for prod by taking priorities in to account
                            nRunJobsPerGroup = None
                            if prevSourceLabel in [
                                "managed",
                                "test",
                            ]:
                                jobStatBrokerCloudsWithPrio.setdefault(
                                    prevPriority,
                                    taskBuffer.getJobStatisticsBrokerage(prevPriority, prevPriority + prioInterval),
                                )
                                jobStatBrokerCloudsWithPrio[prevPriority].setdefault(previousCloud, {})
                                jobStatBrokerCloudsWithPrio[prevPriority][previousCloud].setdefault(site, {})
                                jobStatBrokerCloudsWithPrio[prevPriority][previousCloud][site].setdefault(
                                    tmpProGroup,
                                    {
                                        "assigned": 0,
                                        "activated": 0,
                                        "running": 0,
                                        "transferring": 0,
                                    },
                                )
                                nAssJobs = jobStatBrokerCloudsWithPrio[prevPriority][previousCloud][site][tmpProGroup]["assigned"]
                                nActJobs = jobStatBrokerCloudsWithPrio[prevPriority][previousCloud][site][tmpProGroup]["activated"]
                                nRunJobsPerGroup = jobStatBrokerCloudsWithPrio[prevPriority][previousCloud][site][tmpProGroup]["running"]
                                # add newly assigned jobs
                                for tmpNewPriority in newJobStatWithPrio:
                                    if tmpNewPriority < prevPriority:
                                        continue
                                    if previousCloud not in newJobStatWithPrio[tmpNewPriority]:
                                        continue
                                    if site not in newJobStatWithPrio[tmpNewPriority][previousCloud]:
                                        continue
                                    if tmpProGroup not in newJobStatWithPrio[tmpNewPriority][previousCloud][site]:
                                        continue
                                    nAssJobs += newJobStatWithPrio[tmpNewPriority][previousCloud][site][tmpProGroup]
                            else:
                                nAssJobs = jobStatBroker[site][tmpProGroup]["assigned"]
                                nActJobs = jobStatBroker[site][tmpProGroup]["activated"]

                            # limit of the number of transferring jobs
                            if tmpSiteSpec.transferringlimit == 0:
                                maxTransferring = 2000
                            else:
                                maxTransferring = tmpSiteSpec.transferringlimit
                            # get ratio of transferring to running
                            if tmpSiteSpec.cloud not in ["ND"]:
                                nTraJobs = 0
                                nRunJobs = 0
                                for tmpGroupForTra in jobStatBroker[site]:
                                    tmpCountsForTra = jobStatBroker[site][tmpGroupForTra]
                                    if "running" in tmpCountsForTra:
                                        nRunJobs += tmpCountsForTra["running"]
                                    if "transferring" in tmpCountsForTra:
                                        nTraJobs += tmpCountsForTra["transferring"]
                                tmpLog.debug(f"   running={nRunJobs} transferring={nTraJobs} max={maxTransferring}")
                                if max(maxTransferring, 2 * nRunJobs) < nTraJobs:
                                    tmpLog.debug(f" skip: {site} many transferring={nTraJobs} > max({maxTransferring},2*running={nRunJobs})")
                                    resultsForAnal["transferring"].append(site)
                                    if prevSourceLabel in ["managed", "test"]:
                                        # make message
                                        message = f"{site} - too many transferring"
                                        if message not in loggerMessages:
                                            loggerMessages.append(message)
                                    continue

                            if nRunJobsPerGroup is None:
                                tmpLog.debug(
                                    f"   {site} assigned:{nAssJobs} activated:{nActJobs} running:{jobStatistics[site]['running']} "
                                    f"nPilotsGet:{nPilotsGet} nPilotsUpdate:{nPilotsUpdate}"
                                )
                            else:
                                tmpLog.debug(
                                    f"   {site} assigned:{nAssJobs} activated:{nActJobs} runningGroup:{nRunJobsPerGroup} nPilotsGet:{nPilotsGet} "
                                    f"nPilotsUpdate:{nPilotsUpdate}"
                                )

                            if nRunJobsPerGroup is None:
                                winv = (
                                    float(nAssJobs + nActJobs) / float(1 + jobStatistics[site]["running"]) / (float(1 + nPilotsGet) / float(1 + nPilotsUpdate))
                                )
                            else:
                                winv = float(nAssJobs + nActJobs) / float(1 + nRunJobsPerGroup) / (float(1 + nPilotsGet) / float(1 + nPilotsUpdate))

                        # found at least one candidate
                        foundOneCandidate = True
                        tmpLog.debug(f"Site:{site} 1/Weight:{winv}")

                        # choose largest nMinSites weights
                        minSites[site] = winv
                        if len(minSites) > nMinSites:
                            maxSite = site
                            maxWinv = winv
                            for tmpSite in minSites:
                                tmpWinv = minSites[tmpSite]
                                if tmpWinv > maxWinv:
                                    maxSite = tmpSite
                                    maxWinv = tmpWinv
                            # delte max one
                            del minSites[maxSite]
                        # remove too different weights
                        if len(minSites) >= 2:
                            # look for minimum
                            minSite = list(minSites)[0]
                            minWinv = minSites[minSite]
                            for tmpSite in minSites:
                                tmpWinv = minSites[tmpSite]
                                if tmpWinv < minWinv:
                                    minSite = tmpSite
                                    minWinv = tmpWinv
                            # look for too different weights
                            difference = 2
                            removeSites = []
                            for tmpSite in minSites:
                                tmpWinv = minSites[tmpSite]
                                if tmpWinv > minWinv * difference:
                                    removeSites.append(tmpSite)
                            # remove
                            for tmpSite in removeSites:
                                del minSites[tmpSite]
                    # set default
                    if len(minSites) == 0:
                        # cloud's list
                        if siteMapper.checkCloud(previousCloud):
                            minSites[scanSiteList[0]] = 0
                        else:
                            minSites[panda_config.def_sitename] = 0

                    # use only one site for prod_test to skip LFC scan
                    if prevProType in skipBrokerageProTypes:
                        if len(minSites) > 1:
                            minSites = {list(minSites)[0]: 0}
                    # choose site
                    tmpLog.debug(f"Min Sites:{minSites}")
                    if len(fileList) == 0 or prevIsJEDI is True:
                        # choose min 1/weight
                        minSite = list(minSites)[0]
                        minWinv = minSites[minSite]
                        for tmpSite in minSites:
                            tmpWinv = minSites[tmpSite]
                            if tmpWinv < minWinv:
                                minSite = tmpSite
                                minWinv = tmpWinv
                        chosenCE = siteMapper.getSite(minSite)
                    else:
                        # compare # of files
                        maxNfiles = -1
                        for site in minSites:
                            tmp_chosen_panda_queue = siteMapper.getSite(site)

                            # get files
                            tmpOKFiles = _getOkFiles(
                                tmp_chosen_panda_queue,
                                fileList,
                                allLFNs,
                                allOkFilesMap,
                                job.proSourceLabel,
                                job.job_label,
                                tmpLog,
                                allScopes,
                            )

                            nFiles = len(tmpOKFiles)
                            tmpLog.debug(f"site:{site} - nFiles:{nFiles}/{len(fileList)} {str(tmpOKFiles)}")
                            # choose site holding max # of files
                            if nFiles > maxNfiles:
                                chosenCE = tmp_chosen_panda_queue
                                maxNfiles = nFiles
                                okFiles = tmpOKFiles

                    # set job spec
                    tmpLog.debug(f"indexJob      : {indexJob}")
                    tmpLog.debug(f"nInputs/Job   : {nFilesPerJob}")
                    tmpLog.debug(f"inputSize/Job : {inputSizePerJob}")
                    for tmpJob in jobs[indexJob - iJob - 1 : indexJob - 1]:
                        # set computingSite
                        tmpJob.computingSite = chosenCE.sitename
                        tmpLog.debug(f"PandaID:{tmpJob.PandaID} -> site:{tmpJob.computingSite}")

                        # fail jobs if no sites have the release
                        if ((tmpJob.relocationFlag != 1 and not foundOneCandidate)) and (tmpJob.prodSourceLabel in ["managed", "test"]):
                            # reset
                            if tmpJob.relocationFlag not in [1, 2]:
                                tmpJob.computingSite = None
                                tmpJob.computingElement = None
                            # go to waiting
                            tmpJob.jobStatus = "waiting"
                            tmpJob.brokerageErrorCode = ErrorCode.EC_Release
                            if tmpJob.relocationFlag in [1, 2]:
                                try:
                                    if resultsForAnal["pilot"]:
                                        tmpJob.brokerageErrorDiag = f"{tmpJob.computingSite} no pilots"
                                    elif resultsForAnal["disk"]:
                                        tmpJob.brokerageErrorDiag = f"SE full at {tmpJob.computingSite}"
                                    elif resultsForAnal["memory"]:
                                        tmpJob.brokerageErrorDiag = f"RAM shortage at {tmpJob.computingSite}"
                                    elif resultsForAnal["status"]:
                                        tmpJob.brokerageErrorDiag = f"{tmpJob.computingSite} not online"
                                    elif resultsForAnal["share"]:
                                        tmpJob.brokerageErrorDiag = f"{tmpJob.computingSite} zero share"
                                    elif resultsForAnal["cpucore"]:
                                        tmpJob.brokerageErrorDiag = f"CPU core mismatch at {tmpJob.computingSite}"
                                    elif resultsForAnal["maxtime"]:
                                        tmpJob.brokerageErrorDiag = f"short walltime at {tmpJob.computingSite}"
                                    elif resultsForAnal["transferring"]:
                                        tmpJob.brokerageErrorDiag = f"too many transferring at {tmpJob.computingSite}"
                                    elif resultsForAnal["scratch"]:
                                        tmpJob.brokerageErrorDiag = f"small scratch disk at {tmpJob.computingSite}"
                                    else:
                                        tmpJob.brokerageErrorDiag = f"{tmpJob.AtlasRelease}/{tmpJob.cmtConfig} not found at {tmpJob.computingSite}"
                                except Exception:
                                    errtype, errvalue = sys.exc_info()[:2]
                                    tmpLog.error(f"failed to set diag for {tmpJob.PandaID}: {errtype} {errvalue}")
                                    tmpJob.brokerageErrorDiag = "failed to set diag. see brokerage log in the panda server"
                            elif prevBrokerageSiteList not in [[], None]:
                                try:
                                    # make message
                                    tmpJob.brokerageErrorDiag = makeCompactDiagMessage(prevBrokerageNote, resultsForAnal)
                                except Exception:
                                    errtype, errvalue = sys.exc_info()[:2]
                                    tmpLog.error(f"failed to set special diag for {tmpJob.PandaID}: {errtype} {errvalue}")
                                    tmpJob.brokerageErrorDiag = "failed to set diag. see brokerage log in the panda server"
                            elif prevProType in ["reprocessing"]:
                                tmpJob.brokerageErrorDiag = f"{tmpJob.homepackage}/{tmpJob.cmtConfig} not found at reprocessing sites"
                            else:
                                try:
                                    tmpJob.brokerageErrorDiag = makeCompactDiagMessage("", resultsForAnal)
                                except Exception:
                                    errtype, errvalue = sys.exc_info()[:2]
                                    tmpLog.error(f"failed to set compact diag for {tmpJob.PandaID}: {errtype} {errvalue}")
                                    tmpJob.brokerageErrorDiag = "failed to set diag. see brokerage log in the panda server"
                            tmpLog.debug(f"PandaID:{tmpJob.PandaID} {tmpJob.brokerageErrorDiag}")
                            continue
                        # set ready if files are already there
                        if prevIsJEDI is False:
                            _setReadyToFiles(tmpJob, okFiles, siteMapper, tmpLog)
                        # update statistics
                        tmpProGroup = ProcessGroups.getProcessGroup(tmpJob.processingType)
                        if tmpJob.processingType in skipBrokerageProTypes:
                            # use original processingType since prod_test is in the test category and thus is interfered by validations
                            tmpProGroup = tmpJob.processingType
                        jobStatistics.setdefault(
                            tmpJob.computingSite,
                            {"assigned": 0, "activated": 0, "running": 0},
                        )
                        jobStatBroker.setdefault(tmpJob.computingSite, {})
                        jobStatBroker[tmpJob.computingSite].setdefault(tmpProGroup, {"assigned": 0, "activated": 0, "running": 0})
                        jobStatistics[tmpJob.computingSite]["assigned"] += 1
                        jobStatBroker[tmpJob.computingSite][tmpProGroup]["assigned"] += 1
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
            prevManualPreset = manualPreset
            prevWorkingGroup = job.workingGroup
            prevBrokerageNote = brokerageNote
            prevIsJEDI = isJEDI
            prevDDM = job.getDdmBackEnd()
            # truncate prio to avoid too many lookups
            if job.currentPriority not in [None, "NULL"]:
                prevPriority = (job.currentPriority / prioInterval) * prioInterval
            # assign site
            if chosen_panda_queue != "TOBEDONE":
                job.computingSite = chosen_panda_queue.sitename
                # update statistics
                jobStatistics.setdefault(job.computingSite, {"assigned": 0, "activated": 0, "running": 0})
                jobStatistics[job.computingSite]["assigned"] += 1
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
        # send analysis brokerage info when jobs are submitted
        if len(jobs) > 0 and jobs[0] is not None:
            # for analysis job. FIXME once ganga is updated to send analy brokerage info
            if jobs[0].prodSourceLabel in ["user", "panda"] and jobs[0].processingType in ["pathena", "prun"]:
                # send countryGroup
                tmpMsgList = []
                tmpNumJobs = len(jobs)
                if jobs[0].prodSourceLabel == "panda":
                    tmpNumJobs -= 1
                tmpMsg = f"nJobs={tmpNumJobs} "
                if jobs[0].countryGroup in ["NULL", "", None]:
                    tmpMsg += "countryGroup=None"
                else:
                    tmpMsg += f"countryGroup={jobs[0].countryGroup}"
                tmpMsgList.append(tmpMsg)
                # send log
                sendMsgToLoggerHTTP(tmpMsgList, jobs[0])

        # finished
        tmpLog.debug(f"N lookup for prio : {len(jobStatBrokerCloudsWithPrio)}")
        tmpLog.debug("finished")

    except Exception as e:
        tmpLog.error(f"schedule : {str(e)} {traceback.format_exc()}")
