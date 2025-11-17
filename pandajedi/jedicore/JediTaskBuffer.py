# DB API for JEDI

from pandacommon.pandalogger.PandaLogger import PandaLogger

from pandajedi.jediconfig import jedi_config
from pandaserver.taskbuffer import TaskBuffer

from . import JediDBProxyPool
from .Interaction import CommandReceiveInterface

logger = PandaLogger().getLogger(__name__.split(".")[-1])

# use customized proxy pool
TaskBuffer.DBProxyPool = JediDBProxyPool.DBProxyPool


class JediTaskBuffer(TaskBuffer.TaskBuffer, CommandReceiveInterface):
    # constructor
    def __init__(self, conn, nDBConnection=1):
        CommandReceiveInterface.__init__(self, conn)
        TaskBuffer.TaskBuffer.__init__(self)
        TaskBuffer.TaskBuffer.init(self, jedi_config.db.dbhost, jedi_config.db.dbpasswd, nDBConnection=nDBConnection)
        logger.debug("__init__")

    # query an SQL
    def querySQL(self, sql, varMap, arraySize=1000):
        with self.proxyPool.get() as proxy:
            return proxy.querySQLS(sql, varMap, arraySize)[1]

    # get work queue map
    def getWorkQueueMap(self):
        with self.proxyPool.get() as proxy:
            return proxy.getWorkQueueMap()

    # get the list of datasets to feed contents to DB
    def getDatasetsToFeedContents_JEDI(self, vo=None, prodSourceLabel=None, task_id=None, force_read=False):
        with self.proxyPool.get() as proxy:
            return proxy.getDatasetsToFeedContents_JEDI(vo, prodSourceLabel, task_id, force_read)

    # feed files to the JEDI contents table
    def insertFilesForDataset_JEDI(
        self,
        datasetSpec,
        fileMap,
        datasetState,
        stateUpdateTime,
        nEventsPerFile,
        nEventsPerJob,
        maxAttempt,
        firstEventNumber,
        nMaxFiles,
        nMaxEvents,
        useScout,
        fileList,
        useFilesWithNewAttemptNr,
        nFilesPerJob,
        nEventsPerRange,
        nChunksForScout,
        includePatt,
        excludePatt,
        xmlConfig,
        noWaitParent,
        parent_tid,
        pid,
        maxFailure,
        useRealNumEvents,
        respectLB,
        tgtNumEventsPerJob,
        skipFilesUsedBy,
        ramCount,
        taskSpec,
        skipShortInput,
        inputPreStaging,
        order_by,
        maxFileRecords,
        skip_short_output,
    ):
        with self.proxyPool.get() as proxy:
            return proxy.insertFilesForDataset_JEDI(
                datasetSpec,
                fileMap,
                datasetState,
                stateUpdateTime,
                nEventsPerFile,
                nEventsPerJob,
                maxAttempt,
                firstEventNumber,
                nMaxFiles,
                nMaxEvents,
                useScout,
                fileList,
                useFilesWithNewAttemptNr,
                nFilesPerJob,
                nEventsPerRange,
                nChunksForScout,
                includePatt,
                excludePatt,
                xmlConfig,
                noWaitParent,
                parent_tid,
                pid,
                maxFailure,
                useRealNumEvents,
                respectLB,
                tgtNumEventsPerJob,
                skipFilesUsedBy,
                ramCount,
                taskSpec,
                skipShortInput,
                inputPreStaging,
                order_by,
                maxFileRecords,
                skip_short_output,
            )

    # get files from the JEDI contents table with jediTaskID and/or datasetID
    def getFilesInDatasetWithID_JEDI(self, jediTaskID=None, datasetID=None, nFiles=None, status=None):
        with self.proxyPool.get() as proxy:
            return proxy.getFilesInDatasetWithID_JEDI(jediTaskID, datasetID, nFiles, status)

    # insert dataset to the JEDI datasets table
    def insertDataset_JEDI(self, datasetSpec):
        with self.proxyPool.get() as proxy:
            return proxy.insertDataset_JEDI(datasetSpec)

    # update JEDI dataset
    def updateDataset_JEDI(self, datasetSpec, criteria, lockTask=False):
        with self.proxyPool.get() as proxy:
            return proxy.updateDataset_JEDI(datasetSpec, criteria, lockTask)

    # update JEDI dataset attributes
    def updateDatasetAttributes_JEDI(self, jediTaskID, datasetID, attributes):
        with self.proxyPool.get() as proxy:
            return proxy.updateDatasetAttributes_JEDI(jediTaskID, datasetID, attributes)

    # get JEDI dataset attributes
    def getDatasetAttributes_JEDI(self, jediTaskID, datasetID, attributes):
        with self.proxyPool.get() as proxy:
            return proxy.getDatasetAttributes_JEDI(jediTaskID, datasetID, attributes)

    # get JEDI dataset attributes with map
    def getDatasetAttributesWithMap_JEDI(self, jediTaskID, criteria, attributes):
        with self.proxyPool.get() as proxy:
            return proxy.getDatasetAttributesWithMap_JEDI(jediTaskID, criteria, attributes)

    # get JEDI dataset with jediTaskID and datasetID
    def getDatasetWithID_JEDI(self, jediTaskID, datasetID):
        with self.proxyPool.get() as proxy:
            return proxy.getDatasetWithID_JEDI(jediTaskID, datasetID)

    # get JEDI datasets with jediTaskID
    def getDatasetsWithJediTaskID_JEDI(self, jediTaskID, datasetTypes=None, getFiles=False):
        with self.proxyPool.get() as proxy:
            retStat, datasetSpecList = proxy.getDatasetsWithJediTaskID_JEDI(jediTaskID, datasetTypes=datasetTypes)
            if retStat is True and getFiles is True:
                for datasetSpec in datasetSpecList:
                    # read files
                    retStat, fileSpecList = proxy.getFilesInDatasetWithID_JEDI(jediTaskID, datasetSpec.datasetID, None, None)
                    if retStat is False:
                        break
                    for fileSpec in fileSpecList:
                        datasetSpec.addFile(fileSpec)
            # return
            return retStat, datasetSpecList

    # get jediTaskIDs with dataset attributes
    def get_task_ids_with_dataset_attributes(self, dataset_attributes, only_active_tasks=True):
        with self.proxyPool.get() as proxy:
            return proxy.get_task_ids_with_dataset_attributes(dataset_attributes, only_active_tasks)

    # insert task to the JEDI tasks table
    def insertTask_JEDI(self, taskSpec):
        with self.proxyPool.get() as proxy:
            return proxy.insertTask_JEDI(taskSpec)

    # update JEDI task
    def updateTask_JEDI(self, taskSpec, criteria, oldStatus=None, updateDEFT=False, insertUnknown=None, setFrozenTime=True, setOldModTime=False):
        with self.proxyPool.get() as proxy:
            return proxy.updateTask_JEDI(taskSpec, criteria, oldStatus, updateDEFT, insertUnknown, setFrozenTime, setOldModTime)

    # update JEDI task lock
    def updateTaskLock_JEDI(self, jediTaskID):
        with self.proxyPool.get() as proxy:
            return proxy.updateTaskLock_JEDI(jediTaskID)

    # update JEDI task status by ContentsFeeder
    def updateTaskStatusByContFeeder_JEDI(self, jediTaskID, taskSpec=None, getTaskStatus=False, pid=None, setFrozenTime=True, useWorldCloud=False):
        with self.proxyPool.get() as proxy:
            return proxy.updateTaskStatusByContFeeder_JEDI(jediTaskID, taskSpec, getTaskStatus, pid, setFrozenTime, useWorldCloud)

    # get JEDI task with jediTaskID
    def getTaskWithID_JEDI(self, jediTaskID, fullFlag=False, lockTask=False, pid=None, lockInterval=None, clearError=False):
        with self.proxyPool.get() as proxy:
            return proxy.getTaskWithID_JEDI(jediTaskID, fullFlag, lockTask, pid, lockInterval, clearError)

    # get JEDI task and tasks with ID and lock it
    def getTaskDatasetsWithID_JEDI(self, jediTaskID, pid, lockTask=True):
        with self.proxyPool.get() as proxy:
            return proxy.getTaskDatasetsWithID_JEDI(jediTaskID, pid, lockTask)

    # get JEDI tasks with selection criteria
    def getTaskIDsWithCriteria_JEDI(self, criteria, nTasks=50):
        with self.proxyPool.get() as proxy:
            return proxy.getTaskIDsWithCriteria_JEDI(criteria, nTasks)

    # get JEDI tasks to be finished
    def getTasksToBeFinished_JEDI(self, vo, prodSourceLabel, pid, nTasks=50, target_tasks=None):
        with self.proxyPool.get() as proxy:
            return proxy.getTasksToBeFinished_JEDI(vo, prodSourceLabel, pid, nTasks, target_tasks)

    # get tasks to be processed
    def getTasksToBeProcessed_JEDI(
        self,
        pid,
        vo,
        workQueue,
        prodSourceLabel,
        cloudName,
        nTasks=50,
        nFiles=100,
        simTasks=None,
        minPriority=None,
        maxNumJobs=None,
        typicalNumFilesMap=None,
        fullSimulation=False,
        simDatasets=None,
        mergeUnThrottled=None,
        readMinFiles=False,
        numNewTaskWithJumbo=0,
        resource_name=None,
        ignore_lock=False,
        target_tasks=None,
    ):
        with self.proxyPool.get() as proxy:
            return proxy.getTasksToBeProcessed_JEDI(
                pid,
                vo,
                workQueue,
                prodSourceLabel,
                cloudName,
                nTasks,
                nFiles,
                simTasks=simTasks,
                minPriority=minPriority,
                maxNumJobs=maxNumJobs,
                typicalNumFilesMap=typicalNumFilesMap,
                fullSimulation=fullSimulation,
                simDatasets=simDatasets,
                mergeUnThrottled=mergeUnThrottled,
                readMinFiles=readMinFiles,
                numNewTaskWithJumbo=numNewTaskWithJumbo,
                resource_name=resource_name,
                ignore_lock=ignore_lock,
                target_tasks=target_tasks,
            )

    # get tasks to be processed
    def checkWaitingTaskPrio_JEDI(self, vo, workQueue, prodSourceLabel, cloudName, resource_name=None):
        with self.proxyPool.get() as proxy:
            return proxy.getTasksToBeProcessed_JEDI(None, vo, workQueue, prodSourceLabel, cloudName, isPeeking=True, resource_name=resource_name)

    # get job statistics with work queue
    def getJobStatisticsWithWorkQueue_JEDI(self, vo, prodSourceLabel, minPriority=None):
        with self.proxyPool.get() as proxy:
            return proxy.getJobStatisticsWithWorkQueue_JEDI(vo, prodSourceLabel, minPriority)

    # get core statistics with VO and prodSourceLabel
    def get_core_statistics(self, vo, prod_source_label):
        with self.proxyPool.get() as proxy:
            return proxy.get_core_statistics(vo, prod_source_label)

    # get job statistics by global share
    def getJobStatisticsByGlobalShare(self, vo, exclude_rwq=False):
        with self.proxyPool.get() as proxy:
            return proxy.getJobStatisticsByGlobalShare(vo, exclude_rwq)

    # get whether a gshare rtype combination is active
    def get_active_gshare_rtypes(self, vo):
        with self.proxyPool.get() as proxy:
            return proxy.get_active_gshare_rtypes(vo)

    # get job statistics by resource type
    def getJobStatisticsByResourceType(self, workqueue):
        with self.proxyPool.get() as proxy:
            return proxy.getJobStatisticsByResourceType(workqueue)

    # get job statistics by site and resource type
    def getJobStatisticsByResourceTypeSite(self, workqueue):
        with self.proxyPool.get() as proxy:
            return proxy.getJobStatisticsByResourceTypeSite(workqueue)

    # generate output files for task
    def getOutputFiles_JEDI(
        self,
        jediTaskID,
        provenanceID,
        simul,
        instantiateTmpl=False,
        instantiatedSite=None,
        isUnMerging=False,
        isPrePro=False,
        xmlConfigJob=None,
        siteDsMap=None,
        middleName="",
        registerDatasets=False,
        parallelOutMap=None,
        fileIDPool=[],
        n_files_per_chunk=1,
        bulk_fetch_for_multiple_jobs=False,
        master_dataset_id=None,
    ):
        with self.proxyPool.get() as proxy:
            return proxy.getOutputFiles_JEDI(
                jediTaskID,
                provenanceID,
                simul,
                instantiateTmpl,
                instantiatedSite,
                isUnMerging,
                isPrePro,
                xmlConfigJob,
                siteDsMap,
                middleName,
                registerDatasets,
                parallelOutMap,
                fileIDPool,
                n_files_per_chunk,
                bulk_fetch_for_multiple_jobs,
                master_dataset_id,
            )

    # insert output file templates
    def insertOutputTemplate_JEDI(self, templates):
        with self.proxyPool.get() as proxy:
            return proxy.insertOutputTemplate_JEDI(templates)

    # insert JobParamsTemplate
    def insertJobParamsTemplate_JEDI(self, jediTaskID, templ):
        with self.proxyPool.get() as proxy:
            return proxy.insertJobParamsTemplate_JEDI(jediTaskID, templ)

    # insert TaskParams
    def insertTaskParams_JEDI(self, vo, prodSourceLabel, userName, taskName, taskParams, parent_tid=None):
        with self.proxyPool.get() as proxy:
            return proxy.insertTaskParams_JEDI(vo, prodSourceLabel, userName, taskName, taskParams, parent_tid)

    # reset unused files
    def resetUnusedFiles_JEDI(self, jediTaskID, inputChunk):
        with self.proxyPool.get() as proxy:
            return proxy.resetUnusedFiles_JEDI(jediTaskID, inputChunk)

    # insert TaskParams
    def insertUpdateTaskParams_JEDI(self, jediTaskID, vo, prodSourceLabel, updateTaskParams, insertTaskParamsList):
        with self.proxyPool.get() as proxy:
            return proxy.insertUpdateTaskParams_JEDI(jediTaskID, vo, prodSourceLabel, updateTaskParams, insertTaskParamsList)

    # set missing files
    def setMissingFiles_JEDI(self, jediTaskID, datasetID, fileIDs):
        with self.proxyPool.get() as proxy:
            return proxy.setMissingFiles_JEDI(jediTaskID, datasetID, fileIDs)

    # rescue picked files
    def rescuePickedFiles_JEDI(self, vo, prodSourceLabel, waitTime):
        with self.proxyPool.get() as proxy:
            return proxy.rescuePickedFiles_JEDI(vo, prodSourceLabel, waitTime)

    # rescue unlocked tasks with picked files
    def rescueUnLockedTasksWithPicked_JEDI(self, vo, prodSourceLabel, waitTime, pid):
        with self.proxyPool.get() as proxy:
            return proxy.rescueUnLockedTasksWithPicked_JEDI(vo, prodSourceLabel, waitTime, pid)

    # unlock tasks
    def unlockTasks_JEDI(self, vo, prodSourceLabel, waitTime, hostName=None, pgid=None):
        with self.proxyPool.get() as proxy:
            return proxy.unlockTasks_JEDI(vo, prodSourceLabel, waitTime, hostName, pgid)

    # get the size of input files which will be copied to the site
    def getMovingInputSize_JEDI(self, siteName):
        with self.proxyPool.get() as proxy:
            return proxy.getMovingInputSize_JEDI(siteName)

    # get typical number of input files for each workQueue+processingType
    def getTypicalNumInput_JEDI(self, vo, prodSourceLabel, workQueue):
        with self.proxyPool.get() as proxy:
            return proxy.getTypicalNumInput_JEDI(vo, prodSourceLabel, workQueue)

    # get highest prio jobs with workQueueID
    def getHighestPrioJobStat_JEDI(self, prodSourceLabel, cloudName, workQueue, resource_name=None):
        with self.proxyPool.get() as proxy:
            return proxy.getHighestPrioJobStat_JEDI(prodSourceLabel, cloudName, workQueue, resource_name)

    # get the list of tasks to refine
    def getTasksToRefine_JEDI(self, vo=None, prodSourceLabel=None):
        with self.proxyPool.get() as proxy:
            return proxy.getTasksToRefine_JEDI(vo, prodSourceLabel)

    # get task parameters with jediTaskID
    def getTaskParamsWithID_JEDI(self, jediTaskID):
        with self.proxyPool.get() as proxy:
            return proxy.getTaskParamsWithID_JEDI(jediTaskID)

    # register task/dataset/templ/param in a single transaction
    def registerTaskInOneShot_JEDI(
        self,
        jediTaskID,
        taskSpec,
        inMasterDatasetSpec,
        inSecDatasetSpecList,
        outDatasetSpecList,
        outputTemplateMap,
        jobParamsTemplate,
        taskParams,
        unmergeMasterDatasetSpec,
        unmergeDatasetSpecMap,
        uniqueTaskName,
        oldTaskStatus,
    ):
        with self.proxyPool.get() as proxy:
            return proxy.registerTaskInOneShot_JEDI(
                jediTaskID,
                taskSpec,
                inMasterDatasetSpec,
                inSecDatasetSpecList,
                outDatasetSpecList,
                outputTemplateMap,
                jobParamsTemplate,
                taskParams,
                unmergeMasterDatasetSpec,
                unmergeDatasetSpecMap,
                uniqueTaskName,
                oldTaskStatus,
            )

    # set tasks to be assigned
    def setScoutJobDataToTasks_JEDI(self, vo, prodSourceLabel):
        with self.proxyPool.get() as proxy:
            tmp_site_mapper = self.get_site_mapper()
            return proxy.setScoutJobDataToTasks_JEDI(vo, prodSourceLabel, tmp_site_mapper)

    # prepare tasks to be finished
    def prepareTasksToBeFinished_JEDI(self, vo, prodSourceLabel, nTasks=50, simTasks=None, pid="lock", noBroken=False):
        with self.proxyPool.get() as proxy:
            tmp_site_mapper = self.get_site_mapper()
            return proxy.prepareTasksToBeFinished_JEDI(vo, prodSourceLabel, nTasks, simTasks, pid, noBroken, tmp_site_mapper)

    # get tasks to be assigned
    def getTasksToAssign_JEDI(self, vo, prodSourceLabel, workQueue, resource_name):
        with self.proxyPool.get() as proxy:
            return proxy.getTasksToAssign_JEDI(vo, prodSourceLabel, workQueue, resource_name)

    # get tasks to check task assignment
    def getTasksToCheckAssignment_JEDI(self, vo, prodSourceLabel, workQueue, resource_name):
        with self.proxyPool.get() as proxy:
            return proxy.getTasksToCheckAssignment_JEDI(vo, prodSourceLabel, workQueue, resource_name)

    # calculate RW with a priority
    def calculateRWwithPrio_JEDI(self, vo, prodSourceLabel, workQueue, priority):
        with self.proxyPool.get() as proxy:
            return proxy.calculateRWwithPrio_JEDI(vo, prodSourceLabel, workQueue, priority)

    # calculate RW for tasks
    def calculateTaskRW_JEDI(self, jediTaskID):
        with self.proxyPool.get() as proxy:
            return proxy.calculateTaskRW_JEDI(jediTaskID)

    # calculate WORLD RW with a priority
    def calculateWorldRWwithPrio_JEDI(self, vo, prodSourceLabel, workQueue, priority):
        with self.proxyPool.get() as proxy:
            return proxy.calculateWorldRWwithPrio_JEDI(vo, prodSourceLabel, workQueue, priority)

    # calculate WORLD RW for tasks
    def calculateTaskWorldRW_JEDI(self, jediTaskID):
        with self.proxyPool.get() as proxy:
            return proxy.calculateTaskWorldRW_JEDI(jediTaskID)

    # set cloud to tasks
    def setCloudToTasks_JEDI(self, taskCloudMap):
        with self.proxyPool.get() as proxy:
            return proxy.setCloudToTasks_JEDI(taskCloudMap)

    # get the list of tasks to exec command
    def getTasksToExecCommand_JEDI(self, vo, prodSourceLabel):
        with self.proxyPool.get() as proxy:
            return proxy.getTasksToExecCommand_JEDI(vo, prodSourceLabel)

    # get the list of PandaIDs for a task
    def getPandaIDsWithTask_JEDI(self, jediTaskID, onlyActive):
        with self.proxyPool.get() as proxy:
            return proxy.getPandaIDsWithTask_JEDI(jediTaskID, onlyActive)

    # get the list of queued PandaIDs for a task
    def getQueuedPandaIDsWithTask_JEDI(self, jediTaskID):
        with self.proxyPool.get() as proxy:
            return proxy.getQueuedPandaIDsWithTask_JEDI(jediTaskID)

    # get jediTaskID/datasetID/FileID with dataset and file names
    def getIDsWithFileDataset_JEDI(self, datasetName, fileName, fileType):
        with self.proxyPool.get() as proxy:
            return proxy.getIDsWithFileDataset_JEDI(datasetName, fileName, fileType)

    # get PandaID for a file
    def getPandaIDWithFileID_JEDI(self, jediTaskID, datasetID, fileID):
        with self.proxyPool.get() as proxy:
            return proxy.getPandaIDWithFileID_JEDI(jediTaskID, datasetID, fileID)

    # get JEDI files for a job
    def getFilesWithPandaID_JEDI(self, pandaID):
        with self.proxyPool.get() as proxy:
            return proxy.getFilesWithPandaID_JEDI(pandaID)

    # update task parameters
    def updateTaskParams_JEDI(self, jediTaskID, taskParams):
        with self.proxyPool.get() as proxy:
            return proxy.updateTaskParams_JEDI(jediTaskID, taskParams)

    # reactivate pending tasks
    def reactivatePendingTasks_JEDI(self, vo, prodSourceLabel, timeLimit, timeoutLimit=None, minPriority=None):
        with self.proxyPool.get() as proxy:
            return proxy.reactivatePendingTasks_JEDI(vo, prodSourceLabel, timeLimit, timeoutLimit, minPriority)

    # restart contents update
    def restartTasksForContentsUpdate_JEDI(self, vo, prodSourceLabel, timeLimit=30):
        with self.proxyPool.get() as proxy:
            return proxy.restartTasksForContentsUpdate_JEDI(vo, prodSourceLabel, timeLimit=timeLimit)

    # kick exhausted tasks
    def kickExhaustedTasks_JEDI(self, vo, prodSourceLabel, timeLimit):
        with self.proxyPool.get() as proxy:
            return proxy.kickExhaustedTasks_JEDI(vo, prodSourceLabel, timeLimit)

    # get file spec of lib.tgz
    def get_previous_build_file_spec(self, jediTaskID, siteName, associatedSites):
        with self.proxyPool.get() as proxy:
            return proxy.get_previous_build_file_spec(jediTaskID, siteName, associatedSites)

    # get file spec of old lib.tgz
    def getOldBuildFileSpec_JEDI(self, jediTaskID, datasetID, fileID):
        with self.proxyPool.get() as proxy:
            return proxy.getOldBuildFileSpec_JEDI(jediTaskID, datasetID, fileID)

    # insert lib dataset and files
    def insertBuildFileSpec_JEDI(self, jobSpec, reusedDatasetID, simul):
        with self.proxyPool.get() as proxy:
            return proxy.insertBuildFileSpec_JEDI(jobSpec, reusedDatasetID, simul)

    # get sites used by a task
    def getSitesUsedByTask_JEDI(self, jediTaskID):
        with self.proxyPool.get() as proxy:
            return proxy.getSitesUsedByTask_JEDI(jediTaskID)

    # get random seed
    def getRandomSeed_JEDI(self, jediTaskID, simul, n_files=1):
        with self.proxyPool.get() as proxy:
            return proxy.getRandomSeed_JEDI(jediTaskID, simul, n_files)

    # get preprocess metadata
    def getPreprocessMetadata_JEDI(self, jediTaskID):
        with self.proxyPool.get() as proxy:
            return proxy.getPreprocessMetadata_JEDI(jediTaskID)

    # get log dataset for preprocessing
    def getPreproLog_JEDI(self, jediTaskID, simul):
        with self.proxyPool.get() as proxy:
            return proxy.getPreproLog_JEDI(jediTaskID, simul)

    # get jobsetID
    def getUserJobsetID_JEDI(self, userName):
        with self.proxyPool.get() as proxy:
            tmpJobID, tmpDummy, tmpStat = proxy.getUserParameter(userName, 1, None)
            # return
            return tmpStat, tmpJobID

    # retry or incrementally execute a task
    def retryTask_JEDI(
        self,
        jediTaskID,
        commStr,
        maxAttempt=5,
        retryChildTasks=True,
        discardEvents=False,
        release_unstaged=False,
        keep_share_priority=False,
        ignore_hard_exhausted=False,
    ):
        with self.proxyPool.get() as proxy:
            return proxy.retryTask_JEDI(
                jediTaskID,
                commStr,
                maxAttempt,
                retryChildTasks=retryChildTasks,
                discardEvents=discardEvents,
                release_unstaged=release_unstaged,
                keep_share_priority=keep_share_priority,
                ignore_hard_exhausted=ignore_hard_exhausted,
            )

    # append input datasets for incremental execution
    def appendDatasets_JEDI(self, jediTaskID, inMasterDatasetSpecList, inSecDatasetSpecList):
        with self.proxyPool.get() as proxy:
            return proxy.appendDatasets_JEDI(jediTaskID, inMasterDatasetSpecList, inSecDatasetSpecList)

    # record retry history
    def recordRetryHistory_JEDI(self, jediTaskID, oldNewPandaIDs, relationType):
        with self.proxyPool.get() as proxy:
            return proxy.recordRetryHistory_JEDI(jediTaskID, oldNewPandaIDs, relationType)

    # get JEDI tasks with a selection criteria
    def getTasksWithCriteria_JEDI(
        self,
        vo,
        prodSourceLabel,
        taskStatusList,
        taskCriteria={},
        datasetCriteria={},
        taskParamList=[],
        datasetParamList=[],
        taskLockColumn=None,
        taskLockInterval=60,
    ):
        with self.proxyPool.get() as proxy:
            return proxy.getTasksWithCriteria_JEDI(
                vo, prodSourceLabel, taskStatusList, taskCriteria, datasetCriteria, taskParamList, datasetParamList, taskLockColumn, taskLockInterval
            )

    # check parent task status
    def checkParentTask_JEDI(self, parent_task_id, jedi_task_id=None):
        with self.proxyPool.get() as proxy:
            return proxy.checkParentTask_JEDI(parent_task_id, jedi_task_id)

    # get task status
    def getTaskStatus_JEDI(self, jediTaskID):
        with self.proxyPool.get() as proxy:
            return proxy.getTaskStatus_JEDI(jediTaskID)

    # get lib.tgz for waiting jobs
    def getLibForWaitingRunJob_JEDI(self, vo, prodSourceLabel, checkInterval):
        with self.proxyPool.get() as proxy:
            return proxy.getLibForWaitingRunJob_JEDI(vo, prodSourceLabel, checkInterval)

    # get tasks to get reassigned
    def getTasksToReassign_JEDI(self, vo=None, prodSourceLabel=None):
        with self.proxyPool.get() as proxy:
            return proxy.getTasksToReassign_JEDI(vo, prodSourceLabel)

    # kill child tasks
    def killChildTasks_JEDI(self, jediTaskID, taskStatus):
        with self.proxyPool.get() as proxy:
            return proxy.killChildTasks_JEDI(jediTaskID, taskStatus)

    # kick child tasks
    def kickChildTasks_JEDI(self, jediTaskID):
        with self.proxyPool.get() as proxy:
            return proxy.kickChildTasks_JEDI(jediTaskID)

    # lock task
    def lockTask_JEDI(self, jediTaskID, pid):
        with self.proxyPool.get() as proxy:
            return proxy.lockTask_JEDI(jediTaskID, pid)

    # get successful files
    def getSuccessfulFiles_JEDI(self, jediTaskID, datasetID):
        with self.proxyPool.get() as proxy:
            return proxy.getSuccessfulFiles_JEDI(jediTaskID, datasetID)

    # unlock a single task
    def unlockSingleTask_JEDI(self, jediTaskID, pid):
        with self.proxyPool.get() as proxy:
            return proxy.unlockSingleTask_JEDI(jediTaskID, pid)

    # throttle JEDI tasks
    def throttleTasks_JEDI(self, vo, prodSourceLabel, waitTime):
        with self.proxyPool.get() as proxy:
            return proxy.throttleTasks_JEDI(vo, prodSourceLabel, waitTime)

    # throttle a JEDI task
    def throttleTask_JEDI(self, jediTaskID, waitTime, errorDialog):
        with self.proxyPool.get() as proxy:
            return proxy.throttleTask_JEDI(jediTaskID, waitTime, errorDialog)

    # release throttled tasks
    def releaseThrottledTasks_JEDI(self, vo, prodSourceLabel):
        with self.proxyPool.get() as proxy:
            return proxy.releaseThrottledTasks_JEDI(vo, prodSourceLabel)

    # release a task with on-hold status
    def release_task_on_hold(self, jedi_task_id, target_status=None):
        with self.proxyPool.get() as proxy:
            return proxy.release_task_on_hold(jedi_task_id, target_status)

    # get throttled users
    def getThrottledUsersTasks_JEDI(self, vo, prodSourceLabel):
        with self.proxyPool.get() as proxy:
            return proxy.getThrottledUsersTasks_JEDI(vo, prodSourceLabel)

    # lock process
    def lockProcess_JEDI(self, vo, prodSourceLabel, cloud, workqueue_id, resource_name, component, pid, forceOption=False, timeLimit=5):
        with self.proxyPool.get() as proxy:
            return proxy.lockProcess_JEDI(vo, prodSourceLabel, cloud, workqueue_id, resource_name, component, pid, forceOption, timeLimit)

    # unlock process
    def unlockProcess_JEDI(self, vo, prodSourceLabel, cloud, workqueue_id, resource_name, component, pid):
        with self.proxyPool.get() as proxy:
            return proxy.unlockProcess_JEDI(vo, prodSourceLabel, cloud, workqueue_id, resource_name, component, pid)

    # unlock process with PID
    def unlockProcessWithPID_JEDI(self, vo, prodSourceLabel, workqueue_id, resource_name, pid, useBase):
        with self.proxyPool.get() as proxy:
            return proxy.unlockProcessWithPID_JEDI(vo, prodSourceLabel, workqueue_id, resource_name, pid, useBase)

    # check process lock
    def checkProcessLock_JEDI(self, vo, prodSourceLabel, cloud, workqueue_id, resource_name, component, pid, checkBase):
        with self.proxyPool.get() as proxy:
            return proxy.checkProcessLock_JEDI(vo, prodSourceLabel, cloud, workqueue_id, resource_name, component, pid, checkBase)

    # get JEDI tasks to be assessed
    def getAchievedTasks_JEDI(self, vo, prodSourceLabel, timeLimit=60, nTasks=50):
        with self.proxyPool.get() as proxy:
            return proxy.getAchievedTasks_JEDI(vo, prodSourceLabel, timeLimit, nTasks)

    # get inactive sites
    def getInactiveSites_JEDI(self, flag, timeLimit):
        with self.proxyPool.get() as proxy:
            return proxy.getInactiveSites_JEDI(flag, timeLimit)

    # get total walltime
    def getTotalWallTime_JEDI(self, vo, prodSourceLabel, workQueue, resource_name):
        with self.proxyPool.get() as proxy:
            return proxy.getTotalWallTime_JEDI(vo, prodSourceLabel, workQueue, resource_name)

    # check duplication with internal merge
    def checkDuplication_JEDI(self, jediTaskID):
        with self.proxyPool.get() as proxy:
            return proxy.checkDuplication_JEDI(jediTaskID)

    # get network metrics for brokerage
    def getNetworkMetrics(self, dst, keyList):
        with self.proxyPool.get() as proxy:
            return proxy.getNetworkMetrics(dst, keyList)

    # get nuclei that have built up a long backlog
    def getBackloggedNuclei(self):
        with self.proxyPool.get() as proxy:
            return proxy.getBackloggedNuclei()

    # get network metrics for brokerage
    def getPandaSiteToOutputStorageSiteMapping(self):
        with self.proxyPool.get() as proxy:
            return proxy.getPandaSiteToOutputStorageSiteMapping()

    # get failure counts for a task
    def getFailureCountsForTask_JEDI(self, jediTaskID, timeWindow):
        with self.proxyPool.get() as proxy:
            return proxy.getFailureCountsForTask_JEDI(jediTaskID, timeWindow)

        # count the number of queued jobs per user or working group

    def countJobsPerTarget_JEDI(self, target, is_user):
        with self.proxyPool.get() as proxy:
            return proxy.countJobsPerTarget_JEDI(target, is_user)

    # get old merge job PandaIDs
    def getOldMergeJobPandaIDs_JEDI(self, jediTaskID, pandaID):
        with self.proxyPool.get() as proxy:
            return proxy.getOldMergeJobPandaIDs_JEDI(jediTaskID, pandaID)

    # get active jumbo jobs for a task
    def getActiveJumboJobs_JEDI(self, jediTaskID):
        with self.proxyPool.get() as proxy:
            return proxy.getActiveJumboJobs_JEDI(jediTaskID)

    # get jobParams of the first job
    def getJobParamsOfFirstJob_JEDI(self, jediTaskID):
        with self.proxyPool.get() as proxy:
            return proxy.getJobParamsOfFirstJob_JEDI(jediTaskID)

    # bulk fetch fileIDs
    def bulkFetchFileIDs_JEDI(self, jediTaskID, nIDs):
        with self.proxyPool.get() as proxy:
            return proxy.bulkFetchFileIDs_JEDI(jediTaskID, nIDs)

    # set del flag to events
    def setDelFlagToEvents_JEDI(self, jediTaskID):
        with self.proxyPool.get() as proxy:
            return proxy.setDelFlagToEvents_JEDI(jediTaskID)

    # set del flag to events
    def removeFilesIndexInconsistent_JEDI(self, jediTaskID, datasetIDs):
        with self.proxyPool.get() as proxy:
            return proxy.removeFilesIndexInconsistent_JEDI(jediTaskID, datasetIDs)

    # throttle jobs in pauses tasks
    def throttleJobsInPausedTasks_JEDI(self, vo, prodSourceLabel):
        with self.proxyPool.get() as proxy:
            return proxy.throttleJobsInPausedTasks_JEDI(vo, prodSourceLabel)

    # set useJumbo flag
    def setUseJumboFlag_JEDI(self, jediTaskID, statusStr):
        with self.proxyPool.get() as proxy:
            return proxy.setUseJumboFlag_JEDI(jediTaskID, statusStr)

    # get number of tasks with running jumbo jobs
    def getNumTasksWithRunningJumbo_JEDI(self, vo, prodSourceLabel, cloudName, workqueue):
        with self.proxyPool.get() as proxy:
            return proxy.getNumTasksWithRunningJumbo_JEDI(vo, prodSourceLabel, cloudName, workqueue)

    # get number of unprocessed events
    def getNumUnprocessedEvents_JEDI(self, vo, prodSourceLabel, criteria, neg_criteria):
        with self.proxyPool.get() as proxy:
            return proxy.getNumUnprocessedEvents_JEDI(vo, prodSourceLabel, criteria, neg_criteria)

    # get number of jobs for a task
    def getNumJobsForTask_JEDI(self, jediTaskID):
        with self.proxyPool.get() as proxy:
            return proxy.getNumJobsForTask_JEDI(jediTaskID)

    # get number map for standby jobs
    def getNumMapForStandbyJobs_JEDI(self, workqueue):
        with self.proxyPool.get() as proxy:
            return proxy.getNumMapForStandbyJobs_JEDI(workqueue)

    # update datasets to finish a task
    def updateDatasetsToFinishTask_JEDI(self, jediTaskID, lockedBy):
        with self.proxyPool.get() as proxy:
            return proxy.updateDatasetsToFinishTask_JEDI(jediTaskID, lockedBy)

    # get tasks with jumbo jobs
    def getTaskWithJumbo_JEDI(self, vo, prodSourceLabel):
        with self.proxyPool.get() as proxy:
            return proxy.getTaskWithJumbo_JEDI(vo, prodSourceLabel)

    # kick pending tasks with jumbo jobs
    def kickPendingTasksWithJumbo_JEDI(self, jediTaskID):
        with self.proxyPool.get() as proxy:
            return proxy.kickPendingTasksWithJumbo_JEDI(jediTaskID)

    # reset input to re-generate co-jumbo jobs
    def resetInputToReGenCoJumbo_JEDI(self, jediTaskID):
        with self.proxyPool.get() as proxy:
            return proxy.resetInputToReGenCoJumbo_JEDI(jediTaskID)

    # get averaged disk IO
    def getAvgDiskIO_JEDI(self):
        with self.proxyPool.get() as proxy:
            return proxy.getAvgDiskIO_JEDI()

    # update input files stage-in done (according to message from iDDS, called by other methods, etc.)
    def updateInputFilesStaged_JEDI(self, jeditaskid, scope, filenames_dict, chunk_size=500, by=None, check_scope=True):
        with self.proxyPool.get() as proxy:
            return proxy.updateInputFilesStaged_JEDI(jeditaskid, scope, filenames_dict, chunk_size, by, check_scope)

    # update input datasets stage-in done (according to message from iDDS, called by other methods, etc.)
    def updateInputDatasetsStaged_JEDI(self, jeditaskid, scope, dsnames_dict, use_commit=True, by=None):
        with self.proxyPool.get() as proxy:
            return proxy.updateInputDatasetsStaged_JEDI(jeditaskid, scope, dsnames_dict, use_commit, by)

    # get number of staging files
    def getNumStagingFiles_JEDI(self, jeditaskid):
        with self.proxyPool.get() as proxy:
            return proxy.getNumStagingFiles_JEDI(jeditaskid)

    # get usage breakdown by users and sites
    def getUsageBreakdown_JEDI(self, prod_source_label="user"):
        with self.proxyPool.get() as proxy:
            return proxy.getUsageBreakdown_JEDI(prod_source_label)

    # get jobs stat of each user
    def getUsersJobsStats_JEDI(self, prod_source_label="user"):
        with self.proxyPool.get() as proxy:
            return proxy.getUsersJobsStats_JEDI(prod_source_label)

    # insert HPO pseudo event according to message from idds
    def insertHpoEventAboutIdds_JEDI(self, jedi_task_id, event_id_list):
        with self.proxyPool.get() as proxy:
            return proxy.insertHpoEventAboutIdds_JEDI(jedi_task_id, event_id_list)

    # get event statistics
    def get_event_statistics(self, jedi_task_id):
        with self.proxyPool.get() as proxy:
            return proxy.get_event_statistics(jedi_task_id)

    # get site to-running rate statistics by global share
    def getSiteToRunRateStats(self, vo, exclude_rwq, starttime_min, starttime_max):
        with self.proxyPool.get() as proxy:
            return proxy.getSiteToRunRateStats(vo, exclude_rwq, starttime_min, starttime_max)

    # update cache
    def updateCache_JEDI(self, main_key, sub_key, data):
        with self.proxyPool.get() as proxy:
            return proxy.updateCache_JEDI(main_key, sub_key, data)

    # get cache
    def getCache_JEDI(self, main_key, sub_key):
        with self.proxyPool.get() as proxy:
            return proxy.getCache_JEDI(main_key, sub_key)

    # get cache
    def extendSandboxLifetime_JEDI(self, jedi_taskid, file_name):
        with self.proxyPool.get() as proxy:
            return proxy.extendSandboxLifetime_JEDI(jedi_taskid, file_name)

    # turn a task into pending status for some reason
    def makeTaskPending_JEDI(self, jedi_taskid, reason="unknown"):
        with self.proxyPool.get() as proxy:
            return proxy.makeTaskPending_JEDI(jedi_taskid, reason)

    # query tasks and turn them into pending status for some reason, sql_query should query jeditaskid
    def queryTasksToBePending_JEDI(self, sql_query, params_map, reason):
        with self.proxyPool.get() as proxy:
            return proxy.queryTasksToBePending_JEDI(sql_query, params_map, reason)

    # get IDs of all datasets of input and lib, to update data locality records
    def get_tasks_inputdatasets_JEDI(self, vo):
        with self.proxyPool.get() as proxy:
            return proxy.get_tasks_inputdatasets_JEDI(vo)

    # update dataset locality
    def updateDatasetLocality_JEDI(self, jedi_taskid, datasetid, rse):
        with self.proxyPool.get() as proxy:
            return proxy.updateDatasetLocality_JEDI(jedi_taskid, datasetid, rse)

    # delete outdated dataset locality records
    def deleteOutdatedDatasetLocality_JEDI(self, before_timestamp):
        with self.proxyPool.get() as proxy:
            return proxy.deleteOutdatedDatasetLocality_JEDI(before_timestamp)

    # query tasks and preassign them to dedicate workqueue, sql_query should query jeditaskid
    def queryTasksToPreassign_JEDI(self, sql_query, params_map, site, blacklist=set(), limit=1):
        with self.proxyPool.get() as proxy:
            return proxy.queryTasksToPreassign_JEDI(sql_query, params_map, site, blacklist, limit)

    # close and reassign N jobs of a preassigned task
    def reassignJobsInPreassignedTask_JEDI(self, jedi_taskid, site, n_jobs_to_close):
        with self.proxyPool.get() as proxy:
            return proxy.reassignJobsInPreassignedTask_JEDI(jedi_taskid, site, n_jobs_to_close)

    # undo preassigned tasks
    def undoPreassignedTasks_JEDI(self, jedi_taskids, task_orig_attr_map, params_map, force=False):
        with self.proxyPool.get() as proxy:
            return proxy.undoPreassignedTasks_JEDI(jedi_taskids, task_orig_attr_map, params_map, force)

    # set missing files according to iDDS messages
    def setMissingFilesAboutIdds_JEDI(self, jeditaskid, filenames_dict):
        with self.proxyPool.get() as proxy:
            return proxy.setMissingFilesAboutIdds_JEDI(jeditaskid, filenames_dict)

    # set missing files according to iDDS messages
    def load_sw_map(self):
        with self.proxyPool.get() as proxy:
            return proxy.load_sw_map()

    # get origin datasets
    def get_origin_datasets(self, jedi_task_id, dataset_name, lfns):
        with self.proxyPool.get() as proxy:
            return proxy.get_origin_datasets(jedi_task_id, dataset_name, lfns)

    # push message to message processors which triggers functions of agents
    def push_task_trigger_message(self, msg_type, jedi_task_id, data_dict=None, priority=None, task_spec=None):
        with self.proxyPool.get() as proxy:
            return proxy.push_task_trigger_message(msg_type, jedi_task_id, data_dict, priority, task_spec)

    # aggregate carbon footprint of a task
    def get_task_carbon_footprint(self, jedi_task_id, level="global"):
        with self.proxyPool.get() as proxy:
            return proxy.get_task_carbon_footprint(jedi_task_id, level)

    # get pending data carousel tasks and their input datasets
    def get_pending_dc_tasks_JEDI(self, task_type="prod", time_limit_minutes=60):
        with self.proxyPool.get() as proxy:
            return proxy.get_pending_dc_tasks_JEDI(task_type=task_type, time_limit_minutes=time_limit_minutes)

    # get max number of events in a file of the dataset
    def get_max_events_in_dataset(self, jedi_task_id, dataset_id):
        with self.proxyPool.get() as proxy:
            return proxy.get_max_events_in_dataset(jedi_task_id, dataset_id)

    # insert data carousel requests
    def insert_data_carousel_requests_JEDI(self, task_id, dc_req_specs):
        with self.proxyPool.get() as proxy:
            return proxy.insert_data_carousel_requests_JEDI(task_id, dc_req_specs)

    # update a data carousel request
    def update_data_carousel_request_JEDI(self, dc_req_spec):
        with self.proxyPool.get() as proxy:
            return proxy.update_data_carousel_request_JEDI(dc_req_spec)

    # get data carousel queued requests and info of their related tasks
    def get_data_carousel_queued_requests_JEDI(self):
        with self.proxyPool.get() as proxy:
            return proxy.get_data_carousel_queued_requests_JEDI()

    # get data carousel requests of tasks by task status
    def get_data_carousel_requests_by_task_status_JEDI(self, status_filter_list=None, status_exclusion_list=None):
        with self.proxyPool.get() as proxy:
            return proxy.get_data_carousel_requests_by_task_status_JEDI(status_filter_list=status_filter_list, status_exclusion_list=status_exclusion_list)

    # get data carousel staging requests
    def get_data_carousel_staging_requests_JEDI(self):
        with self.proxyPool.get() as proxy:
            return proxy.get_data_carousel_staging_requests_JEDI()

    # delete data carousel requests
    def delete_data_carousel_requests_JEDI(self, request_id_list):
        with self.proxyPool.get() as proxy:
            return proxy.delete_data_carousel_requests_JEDI(request_id_list)

    # clean up data carousel requests
    def clean_up_data_carousel_requests_JEDI(self, time_limit_days=30):
        with self.proxyPool.get() as proxy:
            return proxy.clean_up_data_carousel_requests_JEDI(time_limit_days)

    # cancel a data carousel request
    def cancel_data_carousel_request_JEDI(self, request_id):
        with self.proxyPool.get() as proxy:
            return proxy.cancel_data_carousel_request_JEDI(request_id)

    # retire a data carousel request
    def retire_data_carousel_request_JEDI(self, request_id):
        with self.proxyPool.get() as proxy:
            return proxy.retire_data_carousel_request_JEDI(request_id)

    # resubmit a data carousel request
    def resubmit_data_carousel_request_JEDI(self, request_id, exclude_prev_dst=False):
        with self.proxyPool.get() as proxy:
            return proxy.resubmit_data_carousel_request_JEDI(request_id, exclude_prev_dst)

    # get task failure metrics
    def get_task_failure_metrics(self, jedi_task_id):
        with self.proxyPool.get() as proxy:
            return proxy.get_task_failure_metrics(jedi_task_id)
