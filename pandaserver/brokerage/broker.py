import datetime
import functools
import time
import traceback
import uuid

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandalogger.PandaLogger import PandaLogger

from pandaserver.config import panda_config
from pandaserver.dataservice import DataServiceUtils

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


def schedule(jobs, siteMapper):
    timestamp = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None).isoformat("/")
    tmp_log = LogWrapper(_log, f"start_ts={timestamp}")

    try:
        # no jobs
        if len(jobs) == 0:
            tmp_log.debug("finished : no jobs")
            return

        max_jobs = 20
        max_files = 20

        iJob = 0
        fileList = []
        chosen_panda_queue = None
        prodDBlock = None
        computingSite = None
        dispatchDBlock = None
        previousCloud = None
        prevProType = None
        prevSourceLabel = None
        prevDirectAcc = None
        prevIsJEDI = None
        prevBrokerageSiteList = None

        indexJob = 0

        # sort jobs by siteID. Some jobs may already define computingSite
        jobs = sorted(jobs, key=functools.cmp_to_key(_compFunc))

        # loop over all jobs + terminator(None)
        for job in jobs + [None]:
            indexJob += 1

            # ignore failed jobs
            if job and job.jobStatus == "failed":
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
                or len(fileList) >= max_files
                or (dispatchDBlock is None and job.homepackage.startswith("AnalysisTransforms"))
                or prodDBlock != job.prodDBlock
                or job.computingSite != computingSite
                or iJob > max_jobs
                or previousCloud != job.getCloud()
                or (prevProType in skipBrokerageProTypes and iJob > 0)
                or prevDirectAcc != job.transferType
                or prevProType != job.processingType
                or prevBrokerageSiteList != specialBrokerageSiteList
                or prevIsJEDI != isJEDI
            ):
                if indexJob > 1:
                    tmp_log.debug("new bunch")
                    tmp_log.debug(f"  iJob           {iJob}")
                    tmp_log.debug(f"  cloud          {previousCloud}")
                    tmp_log.debug(f"  sourceLabel    {prevSourceLabel}")
                    tmp_log.debug(f"  prodDBlock     {prodDBlock}")
                    tmp_log.debug(f"  computingSite  {computingSite}")
                    tmp_log.debug(f"  processingType {prevProType}")
                    tmp_log.debug(f"  transferType   {prevDirectAcc}")

                if (iJob != 0 and chosen_panda_queue == "TOBEDONE") or prevBrokerageSiteList not in [None, []]:
                    # load balancing
                    minSites = {}
                    if prevBrokerageSiteList:
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
                        chosen_panda_queue = siteMapper.getSite(minSite)

                    # set job spec
                    tmp_log.debug(f"indexJob      : {indexJob}")

                    for tmpJob in jobs[indexJob - iJob - 1 : indexJob - 1]:
                        # set computingSite
                        tmpJob.computingSite = chosen_panda_queue.sitename
                        tmp_log.debug(f"PandaID:{tmpJob.PandaID} -> site:{tmpJob.computingSite}")

                # terminate
                if job is None:
                    break
                # reset iJob
                iJob = 0
                # reset file list
                fileList = []
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
            prevProType = job.processingType
            prevSourceLabel = job.prodSourceLabel
            prevDirectAcc = job.transferType
            prevBrokerageSiteList = specialBrokerageSiteList
            prevIsJEDI = isJEDI

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
                # Set dispatch data block for pre-stating jobs too
                if file.type == "input" and file.dispatchDBlock == "NULL" and file.status not in ["ready", "missing", "cached"]:
                    if first:
                        first = False
                        job.dispatchDBlock = dispatchDBlock
                    file.dispatchDBlock = dispatchDBlock
                    file.status = "pending"
                    if file.lfn not in fileList:
                        fileList.append(file.lfn)

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
