import copy

from pandacommon.pandalogger.PandaLogger import PandaLogger

from pandajedi.jedicore import Interaction
from pandajedi.jedicore.MsgWrapper import MsgWrapper
from pandaserver.srvcore import CoreUtils
from pandaserver.taskbuffer.InputChunk import InputChunk

logger = PandaLogger().getLogger(__name__.split(".")[-1])


# class to split job
class JobSplitter:
    # constructor
    def __init__(self):
        self.sizeGradientsPerInSizeForMerge = 1.2
        self.interceptsMerginForMerge = 500 * 1024 * 1024

    # split
    def doSplit(self, taskSpec, inputChunk, siteMapper, allow_chunk_size_limit=False):
        # return for failure
        retFatal = self.SC_FATAL, []
        retTmpError = self.SC_FAILED, []
        # make logger
        tmpLog = MsgWrapper(logger, f"< jediTaskID={taskSpec.jediTaskID} datasetID={inputChunk.masterIndexName} >")
        tmpLog.debug(f"--- start chunk_size_limit={allow_chunk_size_limit}")
        if not inputChunk.isMerging:
            # set maxNumFiles using taskSpec if specified
            maxNumFiles = taskSpec.getMaxNumFilesPerJob()
            # set fsize gradients using taskSpec
            sizeGradients = taskSpec.getOutDiskSize()
            # set fsize intercepts using taskSpec
            sizeIntercepts = taskSpec.getWorkDiskSize()
            # walltime
            if not taskSpec.useHS06():
                walltimeGradient = taskSpec.walltime
            else:
                walltimeGradient = taskSpec.getCpuTime()
            # number of events per job if defined
            nEventsPerJob = taskSpec.getNumEventsPerJob()
            # number of files per job if defined
            nFilesPerJob = taskSpec.getNumFilesPerJob()
            if nFilesPerJob is None and nEventsPerJob is None and inputChunk.useScout() and not taskSpec.useLoadXML() and not taskSpec.respectSplitRule():
                nFilesPerJob = 1
            # grouping with boundaryID
            useBoundary = taskSpec.useGroupWithBoundaryID()
            # fsize intercepts per input size
            sizeGradientsPerInSize = None
            # set max primay output size to avoid producing huge unmerged files
            maxOutSize = 10 * 1024 * 1024 * 1024
            # max size per job
            maxSizePerJob = taskSpec.getMaxSizePerJob()
            if maxSizePerJob is not None:
                maxSizePerJob += InputChunk.defaultOutputSize
            # multiplicity of jobs
            if taskSpec.useJobCloning():
                multiplicity = 1
            else:
                multiplicity = taskSpec.getNumEventServiceConsumer()
            # split with fields
            if taskSpec.getFieldNumToLFN() is not None and taskSpec.useFileAsSourceLFN():
                splitByFields = taskSpec.getFieldNumToLFN()
            else:
                splitByFields = None
        else:
            # set parameters for merging
            maxNumFiles = taskSpec.getMaxNumFilesPerMergeJob()
            sizeGradients = 0
            walltimeGradient = 0
            nFilesPerJob = taskSpec.getNumFilesPerMergeJob()
            nEventsPerJob = taskSpec.getNumEventsPerMergeJob()
            maxSizePerJob = None
            useBoundary = {"inSplit": 3}
            multiplicity = None
            # gradients per input size is 1 + margin
            sizeGradientsPerInSize = self.sizeGradientsPerInSizeForMerge
            # intercepts for libDS
            sizeIntercepts = taskSpec.getWorkDiskSize()
            # mergein of 500MB
            interceptsMergin = self.interceptsMerginForMerge
            if sizeIntercepts < interceptsMergin:
                sizeIntercepts = interceptsMergin
            maxOutSize = taskSpec.getMaxSizePerMergeJob()
            if maxOutSize is None:
                # max output size is 5GB for merging by default
                maxOutSize = 5 * 1024 * 1024 * 1024
            # split with fields
            if taskSpec.getFieldNumToLFN() is not None and taskSpec.useFileAsSourceLFN():
                splitByFields = list(range(4 + 1, 4 + 1 + len(taskSpec.getFieldNumToLFN())))
            else:
                splitByFields = None
        # LB
        respectLB = taskSpec.respectLumiblock()
        # no split for on-site merging
        if taskSpec.on_site_merging():
            no_split = True
        else:
            no_split = False
        # dump
        tmpLog.debug(f"maxNumFiles={maxNumFiles} sizeGradients={sizeGradients} sizeIntercepts={sizeIntercepts} useBoundary={useBoundary}")
        tmpLog.debug(f"walltimeGradient={walltimeGradient} nFilesPerJob={nFilesPerJob} nEventsPerJob={nEventsPerJob}")
        tmpLog.debug(f"useScout={inputChunk.useScout()} isMerging={inputChunk.isMerging}")
        tmpLog.debug(f"sizeGradientsPerInSize={sizeGradientsPerInSize} maxOutSize={maxOutSize} respectLB={respectLB}")
        tmpLog.debug(
            f"multiplicity={multiplicity} splitByFields={str(splitByFields)} "
            f"nFiles={inputChunk.getNumFilesInMaster()} no_split={no_split} "
            f"maxEventsPerJob={taskSpec.get_max_events_per_job()}"
        )
        tmpLog.debug("--- main loop")
        # split
        returnList = []
        subChunks = []
        iSubChunks = 0
        if inputChunk.useScout() and not inputChunk.isMerging:
            default_nSubChunks = 2
        elif taskSpec.is_hpo_workflow():
            default_nSubChunks = 2
        else:
            default_nSubChunks = 25
        subChunk = None
        nSubChunks = default_nSubChunks
        strict_chunkSize = False
        tmp_ng_list = []
        change_site_for_dist_dataset = False
        while True:
            # change site
            if iSubChunks % nSubChunks == 0 or subChunk == [] or change_site_for_dist_dataset:
                change_site_for_dist_dataset = False
                # append to return map
                if subChunks != []:
                    # get site names for parallel execution
                    if taskSpec.getNumSitesPerJob() > 1 and not inputChunk.isMerging and inputChunk.useJumbo != "fake":
                        siteName = inputChunk.getParallelSites(taskSpec.getNumSitesPerJob(), nSubChunks, [siteName])
                    returnList.append(
                        {
                            "siteName": siteName,
                            "subChunks": subChunks,
                            "siteCandidate": siteCandidate,
                        }
                    )
                    try:
                        gshare = taskSpec.gshare.replace(" ", "_")
                    except Exception:
                        gshare = None
                    tmpLog.info(f"split to nJobs={len(subChunks)} at site={siteName} gshare={gshare}")
                    # checkpoint
                    inputChunk.checkpoint_file_usage()
                    # reset
                    subChunks = []
                # skip PQs with chunk size limit
                ngList = copy.copy(tmp_ng_list)
                if not allow_chunk_size_limit:
                    for siteName in inputChunk.get_candidate_names():
                        siteSpec = siteMapper.getSite(siteName)
                        if siteSpec.get_job_chunk_size() is not None:
                            ngList.append(siteName)
                # new candidate
                siteCandidate, getCandidateMsg = inputChunk.getOneSiteCandidate(nSubChunks, ngSites=ngList, get_msg=True)
                if siteCandidate is None:
                    tmpLog.debug(f"no candidate: {getCandidateMsg}")
                    break
                tmp_ng_list = []
                siteName = siteCandidate.siteName
                siteSpec = siteMapper.getSite(siteName)
                # set chunk size
                nSubChunks = siteSpec.get_job_chunk_size()
                if nSubChunks is None:
                    nSubChunks = default_nSubChunks
                    strict_chunkSize = False
                else:
                    strict_chunkSize = True
                # directIO
                if not CoreUtils.use_direct_io_for_job(taskSpec, siteSpec, inputChunk):
                    useDirectIO = False
                else:
                    useDirectIO = True
                # get maxSize if it is set in taskSpec
                maxSize = maxSizePerJob
                if maxSize is None:
                    # use maxwdir as the default maxSize
                    if not useDirectIO:
                        maxSize = siteCandidate.get_overridden_attribute("maxwdir")
                        if maxSize is None:
                            maxSize = siteSpec.maxwdir
                        if maxSize:
                            maxSize *= 1024 * 1024
                    elif nEventsPerJob is not None or nFilesPerJob is not None:
                        maxSize = None
                    else:
                        maxSize = siteCandidate.get_overridden_attribute("maxwdir")
                        if maxSize is None:
                            maxSize = siteSpec.maxwdir
                        if inputChunk.useScout():
                            maxSize = max(inputChunk.maxInputSizeScouts, maxSize) * 1024 * 1024
                        else:
                            maxSize = max(inputChunk.maxInputSizeAvalanche, maxSize) * 1024 * 1024
                else:
                    # add offset
                    maxSize += sizeIntercepts
                # max disk size
                maxDiskSize = siteCandidate.get_overridden_attribute("maxwdir")
                if maxDiskSize is None:
                    maxDiskSize = siteSpec.maxwdir
                if maxDiskSize:
                    maxDiskSize *= 1024 * 1024
                # max walltime
                maxWalltime = None
                if not inputChunk.isMerging:
                    maxWalltime = taskSpec.getMaxWalltime()
                if maxWalltime is None:
                    maxWalltime = siteSpec.maxtime
                # core count
                if siteSpec.coreCount:
                    coreCount = siteSpec.coreCount
                else:
                    coreCount = 1
                # core power
                corePower = siteSpec.corepower
                if taskSpec.dynamicNumEvents() and siteSpec.mintime:
                    dynNumEvents = True
                else:
                    dynNumEvents = False
                tmpLog.debug(f"chosen {siteName} : {getCandidateMsg} : nQueue={siteCandidate.nQueuedJobs} nRunCap={siteCandidate.nRunningJobsCap}")
                tmpLog.debug(f"new weight {siteCandidate.weight}")
                tmpLog.debug(
                    f"maxSize={maxSize} maxWalltime={maxWalltime} coreCount={coreCount} corePower={corePower} maxDisk={maxDiskSize} dynNumEvents={dynNumEvents}"
                )
                tmpLog.debug(f"useDirectIO={useDirectIO} label={taskSpec.prodSourceLabel}")
            # get sub chunk
            subChunk, _ = inputChunk.getSubChunk(
                siteName,
                maxSize=maxSize,
                maxNumFiles=maxNumFiles,
                sizeGradients=sizeGradients,
                sizeIntercepts=sizeIntercepts,
                nFilesPerJob=nFilesPerJob,
                walltimeGradient=walltimeGradient,
                maxWalltime=maxWalltime,
                nEventsPerJob=nEventsPerJob,
                useBoundary=useBoundary,
                sizeGradientsPerInSize=sizeGradientsPerInSize,
                maxOutSize=maxOutSize,
                coreCount=coreCount,
                respectLB=respectLB,
                corePower=corePower,
                dynNumEvents=dynNumEvents,
                multiplicity=multiplicity,
                splitByFields=splitByFields,
                tmpLog=tmpLog,
                useDirectIO=useDirectIO,
                maxDiskSize=maxDiskSize,
                enableLog=True,
                no_split=no_split,
                min_walltime=siteSpec.mintime,
                max_events=taskSpec.get_max_events_per_job(),
            )
            if subChunk is None:
                break
            if subChunk != []:
                # append
                subChunks.append(subChunk)
                # intermediate checkpoint
                inputChunk.checkpoint_file_usage(intermediate=True)
                # check if the last file in master dist dataset is locally available
                for tmp_dataset, tmp_files in subChunk:
                    if tmp_dataset.isMaster():
                        if tmp_dataset.isDistributed() and not siteCandidate.isAvailableFile(tmp_files[-1]) and siteCandidate.isAvailableFile(tmp_files[0]):
                            change_site_for_dist_dataset = True
                            tmpLog.debug(
                                f"change site since the last file in distributed sub-dataset was unavailable at {siteName} while the first file was available"
                            )
                        break
            else:
                tmp_ng_list.append(siteName)
                inputChunk.rollback_file_usage(intermediate=True)
                tmpLog.debug("rollback to intermediate file usage")
            iSubChunks += 1
        # append to return map if remain
        isSkipped = False
        if subChunks != []:
            # skip if chunk size is not enough
            if allow_chunk_size_limit and strict_chunkSize and len(subChunks) < nSubChunks:
                tmpLog.debug(f"skip splitting since chunk size {len(subChunks)} is less than chunk size limit {nSubChunks} at {siteName}")
                inputChunk.rollback_file_usage()
                isSkipped = True
            else:
                # get site names for parallel execution
                if taskSpec.getNumSitesPerJob() > 1 and not inputChunk.isMerging:
                    siteName = inputChunk.getParallelSites(taskSpec.getNumSitesPerJob(), nSubChunks, [siteName])
                returnList.append(
                    {
                        "siteName": siteName,
                        "subChunks": subChunks,
                        "siteCandidate": siteCandidate,
                    }
                )
                try:
                    gshare = taskSpec.gshare.replace(" ", "_")
                except Exception:
                    gshare = None
                tmpLog.info(f"split to nJobs={len(subChunks)} at site={siteName} gshare={gshare}")
        # return
        tmpLog.debug("--- done")
        return self.SC_SUCCEEDED, returnList, isSkipped


Interaction.installSC(JobSplitter)
