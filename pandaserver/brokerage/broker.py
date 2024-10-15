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
def _get_ok_files(
    v_ce,
    v_files,
    all_lfns,
    all_ok_files_map,
    prod_source_label,
    job_label,
    tmp_log,
    all_scope_list=None,
):
    if not all_lfns:
        return {}

    scope_association_input, _ = select_scope(v_ce, prod_source_label, job_label)
    rucio_sites = list(v_ce.setokens_input[scope_association_input].values())
    try:
        rucio_sites.remove("")
    except Exception:
        pass
    rucio_sites.sort()
    if not rucio_sites:
        rucio_site = v_ce.ddm_input[scope_association_input]
    else:
        rucio_site = ",".join(rucio_sites)

    # set LFC and SE name
    rucio_url = "rucio://atlas-rucio.cern.ch:/grid/atlas"
    tmp_se = v_ce.ddm_endpoints_input[scope_association_input].getAllEndpoints()
    tmp_log.debug(f"get_ok_files for {v_ce.sitename} with rucio_site:{rucio_site}, " f"rucio_url:{rucio_url}, SE:{str(tmp_se)}")
    any_id = "any"

    # use bulk lookup
    # get all replicas
    if rucio_url not in all_ok_files_map:
        all_ok_files_map[rucio_url] = {}
        tmp_stat, tmp_ava_files = rucioAPI.list_file_replicas(all_scope_list, all_lfns, tmp_se)
        if not tmp_stat:
            tmp_log.debug("get_ok_file failed to get file replicas")
            tmp_ava_files = {}
        all_ok_files_map[rucio_url][any_id] = tmp_ava_files

    # get files for each rucio_site
    if rucio_site not in all_ok_files_map[rucio_url]:
        all_ok_files_map[rucio_url][rucio_site] = all_ok_files_map[rucio_url][any_id]

    # make return map
    ret_map = {}
    for tmp_lfn in v_files:
        if tmp_lfn in all_ok_files_map[rucio_url][rucio_site]:
            ret_map[tmp_lfn] = all_ok_files_map[rucio_url][rucio_site][tmp_lfn]
    tmp_log.debug("get_ok_files done")

    return ret_map


# set 'ready' if files are already there
def _setReadyToFiles(tmpJob, okFiles, siteMapper, tmp_log):
    tmp_log.debug(str(okFiles))
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


def schedule(jobs, siteMapper):
    timestamp = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None).isoformat("/")
    tmp_log = LogWrapper(_log, f"start_ts={timestamp}")

    try:
        # no jobs
        if len(jobs) == 0:
            tmp_log.debug("finished : no jobs")
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

        # sort jobs by siteID. Some jobs may already define computingSite
        jobs = sorted(jobs, key=functools.cmp_to_key(_compFunc))

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
            if job and job.computingSite != "NULL" and job.prodSourceLabel in ("test", "managed") and specialBrokerageSiteList == []:
                specialBrokerageSiteList = [job.computingSite]

            overwriteSite = False

            # check JEDI
            isJEDI = False
            if job and job.lockedby == "jedi":
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
                    tmp_log.debug("new bunch")
                    tmp_log.debug(f"  iJob           {iJob}")
                    tmp_log.debug(f"  cloud          {previousCloud}")
                    tmp_log.debug(f"  rel            {prevRelease}")
                    tmp_log.debug(f"  sourceLabel    {prevSourceLabel}")
                    tmp_log.debug(f"  cmtConfig      {prevCmtConfig}")
                    tmp_log.debug(f"  memory         {prevMemory}")
                    tmp_log.debug(f"  priority       {prevPriority}")
                    tmp_log.debug(f"  prodDBlock     {prodDBlock}")
                    tmp_log.debug(f"  computingSite  {computingSite}")
                    tmp_log.debug(f"  processingType {prevProType}")
                    tmp_log.debug(f"  workingGroup   {prevWorkingGroup}")
                    tmp_log.debug(f"  coreCount      {prevCoreCount}")
                    tmp_log.debug(f"  maxCpuCount    {prevMaxCpuCount}")
                    tmp_log.debug(f"  transferType   {prevDirectAcc}")
                    tmp_log.debug(f"  DDM            {prevDDM}")

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
                        okFiles = _get_ok_files(
                            tmp_chosen_panda_queue,
                            fileList,
                            allLFNs,
                            allOkFilesMap,
                            jobsInBunch[0].prodSourceLabel,
                            jobsInBunch[0].job_label,
                            tmp_log,
                            allScopes,
                        )

                        nOkFiles = len(okFiles)
                        tmp_log.debug(f"site:{computingSite} - nFiles:{nOkFiles}/{len(fileList)} {str(fileList)} {str(okFiles)}")
                        # loop over all jobs
                        for tmpJob in jobsInBunch:
                            # set 'ready' if files are already there
                            _setReadyToFiles(tmpJob, okFiles, siteMapper, tmp_log)
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

                    # loop over all sites
                    for site in scanSiteList:
                        tmp_log.debug(f"calculate weight for site:{site}")
                        # _allSites may contain NULL after sort()
                        if site == "NULL":
                            tmp_log.debug("site is NULL")
                            continue

                        winv = 1

                        tmp_log.debug(f"Site:{site} 1/Weight:{winv}")

                        # choose largest nMinSites weights
                        minSites[site] = winv

                    # choose site
                    tmp_log.debug(f"Min Sites:{minSites}")
                    if len(fileList) == 0 or prevIsJEDI is True:
                        # choose min 1/weight
                        minSite = list(minSites)[0]
                        chosenCE = siteMapper.getSite(minSite)

                    # set job spec
                    tmp_log.debug(f"indexJob      : {indexJob}")

                    for tmpJob in jobs[indexJob - iJob - 1 : indexJob - 1]:
                        # set computingSite
                        tmpJob.computingSite = chosenCE.sitename
                        tmp_log.debug(f"PandaID:{tmpJob.PandaID} -> site:{tmpJob.computingSite}")

                        # set ready if files are already there
                        if prevIsJEDI is False:
                            _setReadyToFiles(tmpJob, okFiles, siteMapper, tmp_log)

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
                    tmp_log.debug(f"New dispatchDBlock: {dispatchDBlock}")
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
                tmp_log.debug(f"PandaID:{job.PandaID} -> preset site:{chosen_panda_queue.sitename}")
                # set cloud
                if job.cloud in ["NULL", None, ""]:
                    job.cloud = chosen_panda_queue.cloud

            # set destinationSE
            destSE = job.destinationSE
            if siteMapper.checkCloud(job.getCloud()):
                # use cloud dest for non-existing sites
                if job.prodSourceLabel != "user" and job.destinationSE not in siteMapper.siteSpecList and job.destinationSE != "local":
                    if DataServiceUtils.checkJobDestinationSE(job):
                        destSE = DataServiceUtils.checkJobDestinationSE(job)
                    job.destinationSE = destSE

            if overwriteSite:
                # overwrite SE for analysis jobs which set non-existing sites
                destSE = job.computingSite
                job.destinationSE = destSE

            # set dispatchDBlock and destinationSE
            first = True
            for file in job.Files:
                # dispatchDBlock. Set dispDB for pre-stating jobs too
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
                    elif DataServiceUtils.getDistributedDestination(file.destinationDBlockToken):
                        pass
                    else:
                        file.destinationSE = destSE

                # pre-assign GUID to log
                if file.type == "log":
                    # generate GUID
                    file.GUID = str(uuid.uuid4())

        tmp_log.debug("finished")

    except Exception as e:
        tmp_log.error(f"schedule : {str(e)} {traceback.format_exc()}")
