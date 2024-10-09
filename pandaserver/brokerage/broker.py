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

_log = PandaLogger().getLogger("broker")

# all known sites
_allSites = []

# processingType to skip brokerage
skipBrokerageProTypes = ["prod_test"]


# comparison function for sort
def _comparison_function(job_a, job_b):
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


# set 'ready' if files are already there
def _set_files_to_ready(tmp_job, ok_files, site_mapper, tmp_logger):
    tmp_logger.debug(str(ok_files))

    # all files could be set to ready
    all_ok = True

    # get the panda queue spec and the associated tape endpoints
    tmp_site_spec = site_mapper.getSite(tmp_job.computingSite)
    scope_association_site_input, _ = select_scope(tmp_site_spec, tmp_job.prodSourceLabel, tmp_job.job_label)
    tmp_tape_endpoints = tmp_site_spec.ddm_endpoints_input[scope_association_site_input].getTapeEndPoints()

    # loop through all files and process the input files
    for tmp_file in tmp_job.Files:
        if tmp_file.type == "input":
            # file is in ready state
            if tmp_file.status == "ready":
                tmp_file.dispatchDBlock = "NULL"

            # file is cached
            elif DataServiceUtils.isCachedFile(tmp_file.dataset, tmp_site_spec):
                tmp_file.status = "cached"
                tmp_file.dispatchDBlock = "NULL"

            # use DDM pre-stage only for on-tape files
            elif len(tmp_tape_endpoints) > 0 and tmp_file.lfn in ok_files:
                tape_only = True
                tape_copy = False
                for tmp_storage_endpoint in ok_files[tmp_file.lfn]:
                    if tmp_storage_endpoint not in tmp_tape_endpoints:
                        tape_only = False
                    else:
                        # there is a tape copy
                        tape_copy = True
                # trigger pre-stage when disk copy doesn't exist or token is TAPE
                if tape_only or (tape_copy and tmp_file.dispatchDBlockToken in ["ATLASDATATAPE", "ATLASMCTAPE"]):
                    all_ok = False
                else:
                    # set ready
                    tmp_file.status = "ready"
                    tmp_file.dispatchDBlock = "NULL"

            # set ready if the file exists and the site doesn't use pre-stage
            else:
                tmp_file.status = "ready"
                tmp_file.dispatchDBlock = "NULL"

    # unset the dispatch dataset
    if all_ok:
        tmp_job.dispatchDBlock = "NULL"


def schedule(jobs, siteMapper):
    timestamp = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None).isoformat("/")
    tmp_logger = LogWrapper(_log, f"start_ts={timestamp}")

    try:
        tmp_logger.debug(f"start")

        # no jobs
        if len(jobs) == 0:
            tmp_logger.debug("finished : no jobs")
            return

        nJob = 20
        iJob = 0
        nFile = 20
        fileList = []
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
        prevWorkingGroup = None
        prevMaxCpuCount = None
        prevPriority = None

        indexJob = 0
        prestageSites = []

        # sort jobs by siteID. Some jobs may already define computingSite
        jobs = sorted(jobs, key=functools.cmp_to_key(_comparison_function))

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
            if job and job.jobStatus == "failed":
                continue

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
                or prevIsJEDI != isJEDI
            ):
                if indexJob > 1:
                    tmp_logger.debug(
                        f"new bunch\n"
                        f"  iJob           {iJob}\n"
                        f"  cloud          {previousCloud}\n"
                        f"  rel            {prevRelease}\n"
                        f"  sourceLabel    {prevSourceLabel}\n"
                        f"  cmtConfig      {prevCmtConfig}\n"
                        f"  memory         {prevMemory}\n"
                        f"  priority       {prevPriority}\n"
                        f"  prodDBlock     {prodDBlock}\n"
                        f"  computingSite  {computingSite}\n"
                        f"  processingType {prevProType}\n"
                        f"  workingGroup   {prevWorkingGroup}\n"
                        f"  coreCount      {prevCoreCount}\n"
                        f"  maxCpuCount    {prevMaxCpuCount}\n"
                        f"  transferType   {prevDirectAcc}\n"
                    )

                # the number/size of inputs per job
                nFilesPerJob = float(totalNumInputs) / float(iJob)
                inputSizePerJob = float(totalInputSize) / float(iJob)

                chosen_panda_queue = siteMapper.getSite(job.computingSite)

                # set job spec
                tmp_logger.debug(f"indexJob      : {indexJob}")
                tmp_logger.debug(f"nInputs/Job   : {nFilesPerJob}")
                tmp_logger.debug(f"inputSize/Job : {inputSizePerJob}")
                for tmpJob in jobs[indexJob - iJob - 1 : indexJob - 1]:
                    # set computingSite
                    tmpJob.computingSite = chosen_panda_queue.sitename
                    tmp_logger.debug(f"PandaID:{tmpJob.PandaID} -> site:{tmpJob.computingSite}")

                    # set ready if files are already there
                    if not prevIsJEDI:
                        _set_files_to_ready(tmpJob, okFiles, siteMapper, tmp_logger)

                # terminate
                if not job:
                    break

                # reset iJob
                iJob = 0

                # reset file list
                fileList = []
                okFiles = {}
                totalNumInputs = 0
                totalInputSize = 0
                # create new dispDBlock
                if job.prodDBlock != "NULL":
                    # get datatype
                    try:
                        tmpDataType = job.prodDBlock.split(".")[-2]
                    except Exception:
                        tmpDataType = "GEN"  # set a default value

                    # avoid too long name
                    if len(tmpDataType) > 20:
                        tmpDataType = "GEN"

                    transferType = "transfer"
                    if job.useInputPrestaging():
                        transferType = "prestaging"
                    dispatchDBlock = f"panda.{job.taskID}.{time.strftime('%m.%d')}.{tmpDataType}.{transferType}.{str(uuid.uuid4())}_dis{job.PandaID}"
                    tmp_logger.debug(f"New dispatchDBlock: {dispatchDBlock}")
                prodDBlock = job.prodDBlock

                # already defined computingSite
                if job.computingSite != "NULL":
                    # instantiate the known computing site
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
            prevWorkingGroup = job.workingGroup
            prevIsJEDI = isJEDI

            # truncate prio to avoid too many lookups
            if job.currentPriority not in [None, "NULL"]:
                prevPriority = (job.currentPriority / prioInterval) * prioInterval
            # assign site
            if chosen_panda_queue != "TOBEDONE":
                job.computingSite = chosen_panda_queue.sitename
                if job.computingElement == "NULL":
                    if job.prodSourceLabel == "ddm":
                        # use nickname for ddm jobs
                        job.computingElement = chosen_panda_queue.nickname

                tmp_logger.debug(f"PandaID:{job.PandaID} -> preset site:{chosen_panda_queue.sitename}")
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
                # dispatchDBlock. Set dispDB for pre-staging jobs too
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
                        try:
                            # get total number/size of inputs except DBRelease
                            # tgz inputs for evgen may be negligible
                            if re.search("\.tar\.gz", file.lfn) is None:
                                totalNumInputs += 1
                                totalInputSize += file.fsize
                        except Exception:
                            pass

                # destinationSE
                if file.type in ["output", "log"] and destSE:
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

        tmp_logger.debug("finished")

    except Exception as e:
        tmp_logger.error(f"schedule : {str(e)} {traceback.format_exc()}")
