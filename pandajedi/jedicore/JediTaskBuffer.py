# DB API for JEDI

import datetime
from typing import Any, Sequence

from pandacommon.pandalogger.PandaLogger import PandaLogger

from pandajedi.jediconfig import jedi_config
from pandaserver.taskbuffer import ParseJobXML, TaskBuffer
from pandaserver.taskbuffer.InputChunk import InputChunk
from pandaserver.taskbuffer.JediCacheSpec import JediCacheSpec
from pandaserver.taskbuffer.JediDatasetSpec import JediDatasetSpec
from pandaserver.taskbuffer.JediFileSpec import JediFileSpec
from pandaserver.taskbuffer.JediTaskSpec import JediTaskSpec
from pandaserver.taskbuffer.JobSpec import JobSpec
from pandaserver.taskbuffer.WorkQueue import WorkQueue
from pandaserver.taskbuffer.WorkQueueMapper import WorkQueueMapper

from . import JediDBProxyPool
from .Interaction import CommandReceiveInterface

logger = PandaLogger().getLogger(__name__.split(".")[-1])

# use customized proxy pool. The module attribute is deliberately rebound to the JEDI
# subclass, which is what TaskBuffer.init() then instantiates
TaskBuffer.DBProxyPool = JediDBProxyPool.DBProxyPool  # type: ignore[misc,assignment]


class JediTaskBuffer(TaskBuffer.TaskBuffer, CommandReceiveInterface):
    # constructor
    def __init__(self, conn: Any, nDBConnection: int = 1) -> None:
        CommandReceiveInterface.__init__(self, conn)
        TaskBuffer.TaskBuffer.__init__(self)
        TaskBuffer.TaskBuffer.init(self, jedi_config.db.dbhost, jedi_config.db.dbpasswd, nDBConnection=nDBConnection)
        logger.debug("__init__")

    # query an SQL
    def querySQL(self, sql: str, varMap: dict[str, Any], arraySize: int = 1000) -> Any:
        with self.proxyPool.get() as proxy:
            return proxy.querySQLS(sql, varMap, arraySize)[1]

    # get work queue map
    def getWorkQueueMap(self) -> "WorkQueueMapper | None":
        with self.proxyPool.get() as proxy:
            return proxy.getWorkQueueMap()

    # get the list of datasets to feed contents to DB
    def getDatasetsToFeedContents_JEDI(
        self, vo: str | None = None, prodSourceLabel: str | None = None, task_id: int | None = None, force_read: bool = False
    ) -> list[Any] | None:
        with self.proxyPool.get() as proxy:
            return proxy.getDatasetsToFeedContents_JEDI(vo, prodSourceLabel, task_id, force_read)

    # feed files to the JEDI contents table
    def insertFilesForDataset_JEDI(
        self,
        datasetSpec: JediDatasetSpec,
        fileMap: dict[str, Any],
        datasetState: str,
        stateUpdateTime: datetime.datetime | None,
        nEventsPerFile: Any,
        nEventsPerJob: Any,
        maxAttempt: Any,
        firstEventNumber: Any,
        nMaxFiles: Any,
        nMaxEvents: Any,
        useScout: bool,
        fileList: list[Any],
        useFilesWithNewAttemptNr: bool,
        nFilesPerJob: Any,
        nEventsPerRange: Any,
        nChunksForScout: Any,
        includePatt: list[str],
        excludePatt: list[str],
        xmlConfig: Any,
        noWaitParent: bool,
        parent_tid: int | None,
        pid: str | None,
        maxFailure: Any,
        useRealNumEvents: bool,
        respectLB: bool,
        tgtNumEventsPerJob: Any,
        skipFilesUsedBy: str | None,
        ramCount: Any,
        taskSpec: JediTaskSpec,
        skipShortInput: bool,
        inputPreStaging: bool,
        order_by: str | None,
        maxFileRecords: Any,
        skip_short_output: bool,
        skip_empty_input: bool,
        lfn_constituent_map: dict[str, Any] | None = None,
    ) -> Any:
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
                skip_empty_input,
                lfn_constituent_map=lfn_constituent_map,
            )

    # get files from the JEDI contents table with jediTaskID and/or datasetID
    def getFilesInDatasetWithID_JEDI(
        self, jediTaskID: int | None = None, datasetID: int | None = None, nFiles: int | None = None, status: str | None = None
    ) -> tuple[bool, Any]:
        with self.proxyPool.get() as proxy:
            return proxy.getFilesInDatasetWithID_JEDI(jediTaskID, datasetID, nFiles, status)

    # insert dataset to the JEDI datasets table
    def insertDataset_JEDI(self, datasetSpec: JediDatasetSpec) -> tuple[bool, int | None]:
        with self.proxyPool.get() as proxy:
            return proxy.insertDataset_JEDI(datasetSpec)

    # update JEDI dataset
    def updateDataset_JEDI(self, datasetSpec: JediDatasetSpec, criteria: dict[str, Any], lockTask: bool = False) -> tuple[bool, int | None]:
        with self.proxyPool.get() as proxy:
            return proxy.updateDataset_JEDI(datasetSpec, criteria, lockTask)

    # update JEDI dataset attributes
    def updateDatasetAttributes_JEDI(self, jediTaskID: int, datasetID: int, attributes: dict[str, Any]) -> Any:
        with self.proxyPool.get() as proxy:
            return proxy.updateDatasetAttributes_JEDI(jediTaskID, datasetID, attributes)

    # get JEDI dataset attributes
    def getDatasetAttributes_JEDI(self, jediTaskID: int, datasetID: int, attributes: list[str]) -> dict[str, Any]:
        with self.proxyPool.get() as proxy:
            return proxy.getDatasetAttributes_JEDI(jediTaskID, datasetID, attributes)

    # get JEDI dataset attributes with map
    def getDatasetAttributesWithMap_JEDI(self, jediTaskID: int | str, criteria: dict[str, Any], attributes: list[str]) -> dict[str, Any]:
        with self.proxyPool.get() as proxy:
            return proxy.getDatasetAttributesWithMap_JEDI(jediTaskID, criteria, attributes)

    # get JEDI dataset with jediTaskID and datasetID
    def getDatasetWithID_JEDI(self, jediTaskID: int, datasetID: int) -> tuple[bool, JediDatasetSpec | None]:
        with self.proxyPool.get() as proxy:
            return proxy.getDatasetWithID_JEDI(jediTaskID, datasetID)

    # get JEDI datasets with jediTaskID
    def getDatasetsWithJediTaskID_JEDI(
        self, jediTaskID: int, datasetTypes: list[str] | None = None, getFiles: bool = False
    ) -> tuple[bool, list[JediDatasetSpec] | None]:
        with self.proxyPool.get() as proxy:
            retStat, datasetSpecList = proxy.getDatasetsWithJediTaskID_JEDI(jediTaskID, datasetTypes=datasetTypes)
            if retStat is True and getFiles is True and datasetSpecList is not None:
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
    def get_task_ids_with_dataset_attributes(self, dataset_attributes: dict[str, Any], only_active_tasks: bool = True) -> tuple[bool, list[int] | None]:
        with self.proxyPool.get() as proxy:
            return proxy.get_task_ids_with_dataset_attributes(dataset_attributes, only_active_tasks)

    # insert task to the JEDI tasks table
    def insertTask_JEDI(self, taskSpec: JediTaskSpec) -> bool:
        with self.proxyPool.get() as proxy:
            return proxy.insertTask_JEDI(taskSpec)

    # update JEDI task
    def updateTask_JEDI(
        self,
        taskSpec: JediTaskSpec,
        criteria: dict[str, Any],
        oldStatus: list[str] | None = None,
        updateDEFT: bool = False,
        insertUnknown: list[str] | None = None,
        setFrozenTime: bool = True,
        setOldModTime: bool = False,
    ) -> tuple[bool, int | None]:
        with self.proxyPool.get() as proxy:
            return proxy.updateTask_JEDI(taskSpec, criteria, oldStatus, updateDEFT, insertUnknown, setFrozenTime, setOldModTime)

    # update JEDI task lock
    def updateTaskLock_JEDI(self, jediTaskID: int) -> bool:
        with self.proxyPool.get() as proxy:
            return proxy.updateTaskLock_JEDI(jediTaskID)

    # update JEDI task status by ContentsFeeder
    def updateTaskStatusByContFeeder_JEDI(
        self,
        jediTaskID: int,
        taskSpec: JediTaskSpec | None = None,
        getTaskStatus: bool = False,
        pid: str | None = None,
        setFrozenTime: bool = True,
        useWorldCloud: bool = False,
    ) -> Any:
        with self.proxyPool.get() as proxy:
            return proxy.updateTaskStatusByContFeeder_JEDI(jediTaskID, taskSpec, getTaskStatus, pid, setFrozenTime, useWorldCloud)

    # get JEDI task with jediTaskID
    def getTaskWithID_JEDI(
        self, jediTaskID: int, fullFlag: bool = False, lockTask: bool = False, pid: str | None = None, lockInterval: int | None = None, clearError: bool = False
    ) -> tuple[bool, JediTaskSpec | None]:
        with self.proxyPool.get() as proxy:
            return proxy.getTaskWithID_JEDI(jediTaskID, fullFlag, lockTask, pid, lockInterval, clearError)

    # get JEDI task and tasks with ID and lock it
    def getTaskDatasetsWithID_JEDI(self, jediTaskID: int, pid: str | None, lockTask: bool = True) -> tuple[bool, JediTaskSpec | None]:
        with self.proxyPool.get() as proxy:
            return proxy.getTaskDatasetsWithID_JEDI(jediTaskID, pid, lockTask)

    # get JEDI tasks to be finished
    def getTasksToBeFinished_JEDI(
        self, vo: str | None, prodSourceLabel: str | None, pid: str, nTasks: int = 50, target_tasks: list[int] | None = None
    ) -> list[Any] | None:
        with self.proxyPool.get() as proxy:
            return proxy.getTasksToBeFinished_JEDI(vo, prodSourceLabel, pid, nTasks, target_tasks)

    # get tasks to be processed
    def getTasksToBeProcessed_JEDI(
        self,
        pid: str | None,
        vo: str | None,
        workQueue: WorkQueue | None,
        prodSourceLabel: str | None,
        cloudName: str | None,
        nTasks: int = 50,
        nFiles: int = 100,
        simTasks: list[int] | None = None,
        minPriority: int | None = None,
        maxNumJobs: int | None = None,
        typicalNumFilesMap: dict[str, int] | None = None,
        fullSimulation: bool | None = False,
        simDatasets: list[int] | None = None,
        mergeUnThrottled: bool | None = None,
        readMinFiles: bool = False,
        numNewTaskWithJumbo: int = 0,
        resource_name: str | None = None,
        ignore_lock: bool = False,
        target_tasks: list[int] | None = None,
    ) -> list[tuple[int, list[tuple[JediTaskSpec, str, InputChunk]]]] | int | None:
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
    def checkWaitingTaskPrio_JEDI(
        self,
        vo: str | None,
        workQueue: WorkQueue | None,
        prodSourceLabel: str | None,
        cloudName: str | None,
        resource_name: str | None = None,
        pid: str | None = None,
    ) -> list[tuple[int, list[tuple[JediTaskSpec, str, InputChunk]]]] | int | None:
        with self.proxyPool.get() as proxy:
            return proxy.getTasksToBeProcessed_JEDI(pid, vo, workQueue, prodSourceLabel, cloudName, isPeeking=True, resource_name=resource_name)

    # get job statistics with work queue
    def getJobStatisticsWithWorkQueue_JEDI(self, vo: str, prodSourceLabel: str, minPriority: int | None = None) -> tuple[bool, dict[str, Any]]:
        with self.proxyPool.get() as proxy:
            return proxy.getJobStatisticsWithWorkQueue_JEDI(vo, prodSourceLabel, minPriority)

    # get core statistics with VO and prodSourceLabel
    def get_core_statistics(self, vo: str, prod_source_label: str) -> tuple[bool, dict[str, dict[str, int]]]:
        with self.proxyPool.get() as proxy:
            return proxy.get_core_statistics(vo, prod_source_label)

    # get job statistics by global share
    def getJobStatisticsByGlobalShare(self, vo: str, exclude_rwq: bool = False) -> tuple[bool, dict[str, Any]]:
        with self.proxyPool.get() as proxy:
            return proxy.getJobStatisticsByGlobalShare(vo, exclude_rwq)

    # get whether a gshare rtype combination is active
    def get_active_gshare_rtypes(self, vo: str) -> dict[str, Any]:
        with self.proxyPool.get() as proxy:
            return proxy.get_active_gshare_rtypes(vo)

    # get job statistics by resource type
    def getJobStatisticsByResourceType(self, workqueue: "WorkQueue") -> tuple[bool, dict[str, Any]]:
        with self.proxyPool.get() as proxy:
            return proxy.getJobStatisticsByResourceType(workqueue)

    # get job statistics by site and resource type
    def getJobStatisticsByResourceTypeSite(self, workqueue: "WorkQueue") -> tuple[bool, dict[str, Any]]:
        with self.proxyPool.get() as proxy:
            return proxy.getJobStatisticsByResourceTypeSite(workqueue)

    # generate output files for task
    # The proxy's return spells out the two shapes this switches between: one output map per
    # job when bulk_fetch_for_multiple_jobs is set, a single map otherwise, and Nones on
    # failure. Repeating that five-way union here would put it on three call sites that each
    # pass the flag as a literal and so already know which shape they get; the precise types
    # stay on the proxy, where the switch is. An overload pair on Literal[True]/Literal[False]
    # would carry them through, and is the follow-up if a caller ever needs them.
    def getOutputFiles_JEDI(
        self,
        jediTaskID: int,
        provenanceID: int | None,
        simul: bool,
        instantiateTmpl: bool = False,
        instantiatedSite: str | None = None,
        isUnMerging: bool = False,
        isPrePro: bool = False,
        xmlConfigJob: ParseJobXML.dom_job | None = None,
        siteDsMap: dict[Any, Any] | None = None,
        middleName: str = "",
        registerDatasets: bool = False,
        parallelOutMap: dict[Any, Any] | None = None,
        fileIDPool: Sequence[int] = [],
        n_files_per_chunk: int = 1,
        bulk_fetch_for_multiple_jobs: bool = False,
        master_dataset_id: int | None = None,
    ) -> tuple[Any, Any, Any, Any, Any]:
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
    def insertOutputTemplate_JEDI(self, templates: list[dict[str, Any]]) -> bool:
        with self.proxyPool.get() as proxy:
            return proxy.insertOutputTemplate_JEDI(templates)

    # insert JobParamsTemplate
    def insertJobParamsTemplate_JEDI(self, jediTaskID: int, templ: str) -> bool:
        with self.proxyPool.get() as proxy:
            return proxy.insertJobParamsTemplate_JEDI(jediTaskID, templ)

    # insert TaskParams
    def insertTaskParams_JEDI(
        self, vo: str, prodSourceLabel: str, userName: str, taskName: str, taskParams: str, parent_tid: int | None = None
    ) -> tuple[bool, int | None]:
        with self.proxyPool.get() as proxy:
            return proxy.insertTaskParams_JEDI(vo, prodSourceLabel, userName, taskName, taskParams, parent_tid)

    # reset unused files
    def resetUnusedFiles_JEDI(self, jediTaskID: int, inputChunk: InputChunk) -> int:
        with self.proxyPool.get() as proxy:
            return proxy.resetUnusedFiles_JEDI(jediTaskID, inputChunk)

    # insert TaskParams
    def insertUpdateTaskParams_JEDI(
        self, jediTaskID: int, vo: str, prodSourceLabel: str, updateTaskParams: str | None, insertTaskParamsList: list[str]
    ) -> tuple[bool, list[int] | None]:
        with self.proxyPool.get() as proxy:
            return proxy.insertUpdateTaskParams_JEDI(jediTaskID, vo, prodSourceLabel, updateTaskParams, insertTaskParamsList)

    # set missing files
    def setMissingFiles_JEDI(self, jediTaskID: int, datasetID: int, fileIDs: list[int]) -> bool:
        with self.proxyPool.get() as proxy:
            return proxy.setMissingFiles_JEDI(jediTaskID, datasetID, fileIDs)

    # rescue picked files
    def rescuePickedFiles_JEDI(self, vo: str | None, prodSourceLabel: str | None, waitTime: int) -> int | None:
        with self.proxyPool.get() as proxy:
            return proxy.rescuePickedFiles_JEDI(vo, prodSourceLabel, waitTime)

    # rescue unlocked tasks with picked files
    def rescueUnLockedTasksWithPicked_JEDI(self, vo: str | None, prodSourceLabel: str | None, waitTime: int, pid: str) -> int | None:
        with self.proxyPool.get() as proxy:
            return proxy.rescueUnLockedTasksWithPicked_JEDI(vo, prodSourceLabel, waitTime, pid)

    # unlock tasks
    def unlockTasks_JEDI(self, vo: str | None, prodSourceLabel: str | None, waitTime: int, hostName: str | None = None, pgid: int | None = None) -> int | None:
        with self.proxyPool.get() as proxy:
            return proxy.unlockTasks_JEDI(vo, prodSourceLabel, waitTime, hostName, pgid)

    # get the size of input files which will be copied to the site
    def getMovingInputSize_JEDI(self, siteName: str) -> int | None:
        with self.proxyPool.get() as proxy:
            return proxy.getMovingInputSize_JEDI(siteName)

    # get typical number of input files for each workQueue+processingType
    def getTypicalNumInput_JEDI(self, vo: str, prodSourceLabel: str, workQueue: WorkQueue) -> dict[str, Any] | None:
        with self.proxyPool.get() as proxy:
            return proxy.getTypicalNumInput_JEDI(vo, prodSourceLabel, workQueue)

    # get highest prio jobs with workQueueID
    def getHighestPrioJobStat_JEDI(
        self, prodSourceLabel: str, cloudName: str, workQueue: WorkQueue, resource_name: str | None = None
    ) -> tuple[bool, dict[str, Any] | None]:
        with self.proxyPool.get() as proxy:
            return proxy.getHighestPrioJobStat_JEDI(prodSourceLabel, cloudName, workQueue, resource_name)

    # get the list of tasks to refine
    def getTasksToRefine_JEDI(self, vo: str | None = None, prodSourceLabel: str | None = None) -> list[tuple[Any, ...]] | None:
        with self.proxyPool.get() as proxy:
            return proxy.getTasksToRefine_JEDI(vo, prodSourceLabel)

    # get task parameters with jediTaskID
    def getTaskParamsWithID_JEDI(self, jediTaskID: int) -> str | None:
        with self.proxyPool.get() as proxy:
            return proxy.getTaskParamsWithID_JEDI(jediTaskID)

    # register task/dataset/templ/param in a single transaction
    def registerTaskInOneShot_JEDI(
        self,
        jediTaskID: int,
        taskSpec: JediTaskSpec,
        inMasterDatasetSpec: list[JediDatasetSpec],
        inSecDatasetSpecList: list[JediDatasetSpec],
        outDatasetSpecList: list[JediDatasetSpec],
        outputTemplateMap: dict[str, list[dict[str, Any]]],
        jobParamsTemplate: str,
        taskParams: str | None,
        unmergeMasterDatasetSpec: dict[str, JediDatasetSpec],
        unmergeDatasetSpecMap: dict[str, JediDatasetSpec],
        uniqueTaskName: bool,
        oldTaskStatus: str,
        in_content_dataset_spec_list: list[JediDatasetSpec],
    ) -> tuple[bool, str | None]:
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
                in_content_dataset_spec_list,
            )

    # set tasks to be assigned
    def setScoutJobDataToTasks_JEDI(self, vo: str | None, prodSourceLabel: str | None) -> bool | None:
        with self.proxyPool.get() as proxy:
            tmp_site_mapper = self.get_site_mapper()
            return proxy.setScoutJobDataToTasks_JEDI(vo, prodSourceLabel, tmp_site_mapper)

    # prepare tasks to be finished
    def prepareTasksToBeFinished_JEDI(
        self, vo: str | None, prodSourceLabel: str | None, nTasks: int = 50, simTasks: list[int] | None = None, pid: str = "lock", noBroken: bool = False
    ) -> list[int] | None:
        with self.proxyPool.get() as proxy:
            tmp_site_mapper = self.get_site_mapper()
            return proxy.prepareTasksToBeFinished_JEDI(vo, prodSourceLabel, nTasks, simTasks, pid, noBroken, tmp_site_mapper)

    # get tasks to be assigned
    def getTasksToAssign_JEDI(self, vo: str | None, prodSourceLabel: str | None, workQueue: WorkQueue, resource_name: str) -> list[int] | None:
        with self.proxyPool.get() as proxy:
            return proxy.getTasksToAssign_JEDI(vo, prodSourceLabel, workQueue, resource_name)

    # get tasks to check task assignment
    def getTasksToCheckAssignment_JEDI(self, vo: str | None, prodSourceLabel: str | None, workQueue: WorkQueue, resource_name: str) -> list[int] | None:
        with self.proxyPool.get() as proxy:
            return proxy.getTasksToCheckAssignment_JEDI(vo, prodSourceLabel, workQueue, resource_name)

    # calculate RW with a priority
    def calculateRWwithPrio_JEDI(self, vo: str, prodSourceLabel: str, workQueue: WorkQueue | None, priority: int | None) -> dict[str, Any] | None:
        with self.proxyPool.get() as proxy:
            return proxy.calculateRWwithPrio_JEDI(vo, prodSourceLabel, workQueue, priority)

    # calculate RW for tasks
    def calculateTaskRW_JEDI(self, jediTaskID: int) -> int | None:
        with self.proxyPool.get() as proxy:
            return proxy.calculateTaskRW_JEDI(jediTaskID)

    # calculate WORLD RW with a priority
    def calculateWorldRWwithPrio_JEDI(self, vo: str, prodSourceLabel: str, workQueue: WorkQueue | None, priority: int | None) -> dict[str, Any] | None:
        with self.proxyPool.get() as proxy:
            return proxy.calculateWorldRWwithPrio_JEDI(vo, prodSourceLabel, workQueue, priority)

    # calculate WORLD RW for tasks
    def calculateTaskWorldRW_JEDI(self, jediTaskID: int) -> int | None:
        with self.proxyPool.get() as proxy:
            return proxy.calculateTaskWorldRW_JEDI(jediTaskID)

    # set cloud to tasks
    def setCloudToTasks_JEDI(self, taskCloudMap: dict[int, Any]) -> bool:
        with self.proxyPool.get() as proxy:
            return proxy.setCloudToTasks_JEDI(taskCloudMap)

    # get the list of tasks to exec command
    def getTasksToExecCommand_JEDI(self, vo: str | None, prodSourceLabel: str | None) -> list[tuple[int, dict[str, Any]]] | None:
        with self.proxyPool.get() as proxy:
            return proxy.getTasksToExecCommand_JEDI(vo, prodSourceLabel)

    # get the list of PandaIDs for a task
    def getPandaIDsWithTask_JEDI(self, jediTaskID: int, onlyActive: bool) -> list[int] | None:
        with self.proxyPool.get() as proxy:
            return proxy.getPandaIDsWithTask_JEDI(jediTaskID, onlyActive)

    # get the list of queued PandaIDs for a task
    def getQueuedPandaIDsWithTask_JEDI(self, jediTaskID: int) -> list[int] | None:
        with self.proxyPool.get() as proxy:
            return proxy.getQueuedPandaIDsWithTask_JEDI(jediTaskID)

    # get jediTaskID/datasetID/FileID with dataset and file names
    def getIDsWithFileDataset_JEDI(self, datasetName: str, fileName: str, fileType: str) -> tuple[bool, dict[str, Any] | None]:
        with self.proxyPool.get() as proxy:
            return proxy.getIDsWithFileDataset_JEDI(datasetName, fileName, fileType)

    # get PandaID for a file
    def getPandaIDWithFileID_JEDI(self, jediTaskID: int, datasetID: int, fileID: int) -> tuple[bool, int | None]:
        with self.proxyPool.get() as proxy:
            return proxy.getPandaIDWithFileID_JEDI(jediTaskID, datasetID, fileID)

    # get JEDI files for a job
    def getFilesWithPandaID_JEDI(self, pandaID: int) -> tuple[bool, list[JediFileSpec] | None]:
        with self.proxyPool.get() as proxy:
            return proxy.getFilesWithPandaID_JEDI(pandaID)

    # update task parameters
    def updateTaskParams_JEDI(self, jediTaskID: int, taskParams: str) -> bool | None:
        with self.proxyPool.get() as proxy:
            return proxy.updateTaskParams_JEDI(jediTaskID, taskParams)

    # reactivate pending tasks
    def reactivatePendingTasks_JEDI(
        self, vo: str | None, prodSourceLabel: str | None, timeLimit: int, timeoutLimit: int | None = None, minPriority: int | None = None
    ) -> tuple[int | None, set[int] | None]:
        with self.proxyPool.get() as proxy:
            return proxy.reactivatePendingTasks_JEDI(vo, prodSourceLabel, timeLimit, timeoutLimit, minPriority)

    # restart contents update
    def restartTasksForContentsUpdate_JEDI(self, vo: str | None, prodSourceLabel: str | None, timeLimit: int = 30) -> tuple[int | None, set[int] | None]:
        with self.proxyPool.get() as proxy:
            return proxy.restartTasksForContentsUpdate_JEDI(vo, prodSourceLabel, timeLimit=timeLimit)

    # kick exhausted tasks
    def kickExhaustedTasks_JEDI(self, vo: str | None, prodSourceLabel: str | None, timeLimit: int) -> int | None:
        with self.proxyPool.get() as proxy:
            return proxy.kickExhaustedTasks_JEDI(vo, prodSourceLabel, timeLimit)

    # get file spec of lib.tgz
    def get_previous_build_file_spec(
        self, jediTaskID: int, siteName: str, associatedSites: list[str]
    ) -> tuple[bool, JediFileSpec | None, JediDatasetSpec | None]:
        with self.proxyPool.get() as proxy:
            return proxy.get_previous_build_file_spec(jediTaskID, siteName, associatedSites)

    # get file spec of old lib.tgz
    def getOldBuildFileSpec_JEDI(self, jediTaskID: int, datasetID: int, fileID: int) -> tuple[bool, JediFileSpec | None, JediDatasetSpec | None]:
        with self.proxyPool.get() as proxy:
            return proxy.getOldBuildFileSpec_JEDI(jediTaskID, datasetID, fileID)

    # insert lib dataset and files
    def insertBuildFileSpec_JEDI(self, jobSpec: JobSpec, reusedDatasetID: int | None, simul: bool) -> tuple[bool, dict[str, Any] | None]:
        with self.proxyPool.get() as proxy:
            return proxy.insertBuildFileSpec_JEDI(jobSpec, reusedDatasetID, simul)

    # get sites used by a task
    def getSitesUsedByTask_JEDI(self, jediTaskID: int) -> tuple[bool, set[str] | None]:
        with self.proxyPool.get() as proxy:
            return proxy.getSitesUsedByTask_JEDI(jediTaskID)

    # get random seed
    def getRandomSeed_JEDI(self, jediTaskID: int, simul: bool, n_files: int = 1) -> tuple[bool, tuple[Any, Any]]:
        with self.proxyPool.get() as proxy:
            return proxy.getRandomSeed_JEDI(jediTaskID, simul, n_files)

    # get preprocess metadata
    def getPreprocessMetadata_JEDI(self, jediTaskID: int) -> Any:
        with self.proxyPool.get() as proxy:
            return proxy.getPreprocessMetadata_JEDI(jediTaskID)

    # get log dataset for preprocessing
    def getPreproLog_JEDI(self, jediTaskID: int, simul: bool) -> Any:
        with self.proxyPool.get() as proxy:
            return proxy.getPreproLog_JEDI(jediTaskID, simul)

    # get jobsetID
    def getUserJobsetID_JEDI(self, userName: str) -> tuple[bool, int]:
        with self.proxyPool.get() as proxy:
            tmpJobID, tmpDummy, tmpStat = proxy.getUserParameter(userName, 1, None)
            # return
            return tmpStat, tmpJobID

    # retry or incrementally execute a task
    def retryTask_JEDI(
        self,
        jediTaskID: int,
        commStr: str,
        maxAttempt: int = 5,
        retryChildTasks: bool = True,
        discardEvents: bool = False,
        release_unstaged: bool = False,
        keep_share_priority: bool = False,
        ignore_hard_exhausted: bool = False,
    ) -> tuple[bool | None, str | None, list[int]]:
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
    def appendDatasets_JEDI(
        self,
        jediTaskID: int,
        inMasterDatasetSpecList: list[JediDatasetSpec],
        inSecDatasetSpecList: list[JediDatasetSpec],
        in_content_dataset_specs: list[JediDatasetSpec],
    ) -> bool:
        with self.proxyPool.get() as proxy:
            return proxy.appendDatasets_JEDI(jediTaskID, inMasterDatasetSpecList, inSecDatasetSpecList, in_content_dataset_specs)

    # record retry history
    def recordRetryHistory_JEDI(self, jediTaskID: int, oldNewPandaIDs: dict[int, list[int]], relationType: str | None) -> bool:
        with self.proxyPool.get() as proxy:
            return proxy.recordRetryHistory_JEDI(jediTaskID, oldNewPandaIDs, relationType)

    # get JEDI tasks with a selection criteria
    def getTasksWithCriteria_JEDI(
        self,
        vo: str | None,
        prodSourceLabel: str | None,
        taskStatusList: list[str],
        taskCriteria: dict[str, Any] = {},
        datasetCriteria: dict[str, Any] | None = {},
        taskParamList: list[str] = [],
        datasetParamList: list[str] = [],
        taskLockColumn: str | None = None,
        taskLockInterval: int = 60,
    ) -> list[Any] | None:
        with self.proxyPool.get() as proxy:
            return proxy.getTasksWithCriteria_JEDI(
                vo, prodSourceLabel, taskStatusList, taskCriteria, datasetCriteria, taskParamList, datasetParamList, taskLockColumn, taskLockInterval
            )

    # check parent task status
    def checkParentTask_JEDI(self, parent_task_id: int, jedi_task_id: int | None = None) -> str | None:
        with self.proxyPool.get() as proxy:
            return proxy.checkParentTask_JEDI(parent_task_id, jedi_task_id)

    # get task status
    def getTaskStatus_JEDI(self, jediTaskID: int) -> str | None:
        with self.proxyPool.get() as proxy:
            return proxy.getTaskStatus_JEDI(jediTaskID)

    # get lib.tgz for waiting jobs
    def getLibForWaitingRunJob_JEDI(self, vo: str, prodSourceLabel: str, checkInterval: int) -> list[Any]:
        with self.proxyPool.get() as proxy:
            return proxy.getLibForWaitingRunJob_JEDI(vo, prodSourceLabel, checkInterval)

    # get tasks to get reassigned
    def getTasksToReassign_JEDI(self, vo: str | None = None, prodSourceLabel: str | None = None) -> list[JediTaskSpec]:
        with self.proxyPool.get() as proxy:
            return proxy.getTasksToReassign_JEDI(vo, prodSourceLabel)

    # kill child tasks
    def killChildTasks_JEDI(self, jediTaskID: int, taskStatus: str) -> bool:
        with self.proxyPool.get() as proxy:
            return proxy.killChildTasks_JEDI(jediTaskID, taskStatus)

    # kick child tasks
    def kickChildTasks_JEDI(self, jediTaskID: int) -> bool:
        with self.proxyPool.get() as proxy:
            return proxy.kickChildTasks_JEDI(jediTaskID)

    # lock task
    def lockTask_JEDI(self, jediTaskID: int, pid: str) -> bool:
        with self.proxyPool.get() as proxy:
            return proxy.lockTask_JEDI(jediTaskID, pid)

    # get successful files
    def getSuccessfulFiles_JEDI(self, jediTaskID: int, datasetID: int) -> list[str] | None:
        with self.proxyPool.get() as proxy:
            return proxy.getSuccessfulFiles_JEDI(jediTaskID, datasetID)

    # unlock a single task
    def unlockSingleTask_JEDI(self, jediTaskID: int, pid: str) -> bool:
        with self.proxyPool.get() as proxy:
            return proxy.unlockSingleTask_JEDI(jediTaskID, pid)

    # throttle JEDI tasks
    def throttleTasks_JEDI(self, vo: str | None, prodSourceLabel: str | None, waitTime: int) -> int | None:
        with self.proxyPool.get() as proxy:
            return proxy.throttleTasks_JEDI(vo, prodSourceLabel, waitTime)

    # throttle a JEDI task
    def throttleTask_JEDI(self, jediTaskID: int, waitTime: int, errorDialog: str | None) -> bool:
        with self.proxyPool.get() as proxy:
            return proxy.throttleTask_JEDI(jediTaskID, waitTime, errorDialog)

    # throttle tasks of a user or working group and get the list of throttled tasks
    def throttle_tasks_with_uid(self, vo: str, prod_source_label: str, wait_time: int, dialog: str, uid: str, is_user: bool = True) -> list[int]:
        with self.proxyPool.get() as proxy:
            return proxy.throttle_tasks_with_uid(vo, prod_source_label, wait_time, dialog, uid, is_user)

    # release throttled tasks
    def releaseThrottledTasks_JEDI(self, vo: str | None, prodSourceLabel: str | None) -> int | None:
        with self.proxyPool.get() as proxy:
            return proxy.releaseThrottledTasks_JEDI(vo, prodSourceLabel)

    # release a task with on-hold status
    def release_task_on_hold(self, jedi_task_id: int, target_status: str | None = None) -> bool:
        with self.proxyPool.get() as proxy:
            return proxy.release_task_on_hold(jedi_task_id, target_status)

    # get throttled users
    def getThrottledUsersTasks_JEDI(self, vo: str | None, prodSourceLabel: str | None) -> dict[str, dict[str, dict[int, int]]]:
        with self.proxyPool.get() as proxy:
            return proxy.getThrottledUsersTasks_JEDI(vo, prodSourceLabel)

    # lock process
    def lockProcess_JEDI(
        self,
        vo: str,
        prodSourceLabel: str,
        cloud: str | None,
        workqueue_id: int | None,
        resource_name: str | None,
        component: str | None,
        pid: str,
        forceOption: bool = False,
        timeLimit: int = 5,
    ) -> bool:
        with self.proxyPool.get() as proxy:
            return proxy.lockProcess_JEDI(vo, prodSourceLabel, cloud, workqueue_id, resource_name, component, pid, forceOption, timeLimit)

    # unlock process
    def unlockProcess_JEDI(
        self, vo: str, prodSourceLabel: str, cloud: str | None, workqueue_id: int | None, resource_name: str | None, component: str | None, pid: str
    ) -> bool:
        with self.proxyPool.get() as proxy:
            return proxy.unlockProcess_JEDI(vo, prodSourceLabel, cloud, workqueue_id, resource_name, component, pid)

    # unlock process with PID
    def unlockProcessWithPID_JEDI(self, vo: str, prodSourceLabel: str, workqueue_id: int | None, resource_name: str | None, pid: str, useBase: bool) -> bool:
        with self.proxyPool.get() as proxy:
            return proxy.unlockProcessWithPID_JEDI(vo, prodSourceLabel, workqueue_id, resource_name, pid, useBase)

    # check process lock
    def checkProcessLock_JEDI(
        self,
        vo: str,
        prodSourceLabel: str,
        cloud: str | None,
        workqueue_id: int | None,
        resource_name: str | None,
        component: str | None,
        pid: str,
        checkBase: bool,
    ) -> bool:
        with self.proxyPool.get() as proxy:
            return proxy.checkProcessLock_JEDI(vo, prodSourceLabel, cloud, workqueue_id, resource_name, component, pid, checkBase)

    # get JEDI tasks to be assessed
    def getAchievedTasks_JEDI(self, vo: str | None, prodSourceLabel: str | None, timeLimit: int = 60, nTasks: int = 50) -> list[Any] | None:
        with self.proxyPool.get() as proxy:
            return proxy.getAchievedTasks_JEDI(vo, prodSourceLabel, timeLimit, nTasks)

    # get tasks to take periodic action
    def get_tasks_for_periodic_action(self, vo: str | None, prod_source_label: str | None, time_limit: int = 12, n_tasks: int = 100) -> list[int] | None:
        with self.proxyPool.get() as proxy:
            return proxy.get_tasks_for_periodic_action(vo, prod_source_label, time_limit, n_tasks)

    # get inactive sites
    def getInactiveSites_JEDI(self, flag: str, timeLimit: int) -> set[str]:
        with self.proxyPool.get() as proxy:
            return proxy.getInactiveSites_JEDI(flag, timeLimit)

    # get total walltime
    def getTotalWallTime_JEDI(self, vo: str, prodSourceLabel: str, workQueue: WorkQueue, resource_name: str | None) -> float | None:
        with self.proxyPool.get() as proxy:
            return proxy.getTotalWallTime_JEDI(vo, prodSourceLabel, workQueue, resource_name)

    # check duplication with internal merge
    def checkDuplication_JEDI(self, jediTaskID: int) -> int | None:
        with self.proxyPool.get() as proxy:
            return proxy.checkDuplication_JEDI(jediTaskID)

    # get network metrics for brokerage
    def getNetworkMetrics(self, dst: str, keyList: list[str]) -> dict[str, Any]:
        with self.proxyPool.get() as proxy:
            return proxy.getNetworkMetrics(dst, keyList)

    # get nuclei that have built up a long backlog
    def getBackloggedNuclei(self) -> list[str]:
        with self.proxyPool.get() as proxy:
            return proxy.getBackloggedNuclei()

    # get network metrics for brokerage
    def getPandaSiteToOutputStorageSiteMapping(self) -> dict[str, Any]:
        with self.proxyPool.get() as proxy:
            return proxy.getPandaSiteToOutputStorageSiteMapping()

    # get failure counts for a task
    def getFailureCountsForTask_JEDI(self, jediTaskID: int, timeWindow: int) -> dict[str, Any]:
        with self.proxyPool.get() as proxy:
            return proxy.getFailureCountsForTask_JEDI(jediTaskID, timeWindow)

    # count the number of queued jobs per user or working group
    def countJobsPerTarget_JEDI(self, target: str, is_user: bool) -> dict[str, Any]:
        with self.proxyPool.get() as proxy:
            return proxy.countJobsPerTarget_JEDI(target, is_user)

    # count the number of jobs and cores per user or working group in VO and production source label
    def count_jobs_per_uid_JEDI(self, vo: str, prod_source_label: str) -> dict[str, dict[str, int]]:
        with self.proxyPool.get() as proxy:
            return proxy.count_jobs_per_uid_JEDI(vo, prod_source_label)

    # get old merge job PandaIDs
    def getOldMergeJobPandaIDs_JEDI(self, jediTaskID: int, pandaID: int) -> list[int]:
        with self.proxyPool.get() as proxy:
            return proxy.getOldMergeJobPandaIDs_JEDI(jediTaskID, pandaID)

    # get active jumbo jobs for a task
    def getActiveJumboJobs_JEDI(self, jediTaskID: int) -> dict[Any, Any]:
        with self.proxyPool.get() as proxy:
            return proxy.getActiveJumboJobs_JEDI(jediTaskID)

    # get jobParams of the first job
    def getJobParamsOfFirstJob_JEDI(self, jediTaskID: int) -> tuple[str | None, dict[str, Any] | None]:
        with self.proxyPool.get() as proxy:
            return proxy.getJobParamsOfFirstJob_JEDI(jediTaskID)

    # bulk fetch fileIDs
    def bulkFetchFileIDs_JEDI(self, jediTaskID: int, nIDs: int) -> list[int]:
        with self.proxyPool.get() as proxy:
            return proxy.bulkFetchFileIDs_JEDI(jediTaskID, nIDs)

    # set del flag to events
    def setDelFlagToEvents_JEDI(self, jediTaskID: int) -> int | None:
        with self.proxyPool.get() as proxy:
            return proxy.setDelFlagToEvents_JEDI(jediTaskID)

    # set del flag to events
    def removeFilesIndexInconsistent_JEDI(self, jediTaskID: int, datasetIDs: list[int]) -> bool:
        with self.proxyPool.get() as proxy:
            return proxy.removeFilesIndexInconsistent_JEDI(jediTaskID, datasetIDs)

    # throttle jobs in pauses tasks
    def throttleJobsInPausedTasks_JEDI(self, vo: str | None, prodSourceLabel: str | None) -> dict[str, Any] | None:
        with self.proxyPool.get() as proxy:
            return proxy.throttleJobsInPausedTasks_JEDI(vo, prodSourceLabel)

    # set useJumbo flag
    def setUseJumboFlag_JEDI(self, jediTaskID: int, statusStr: str) -> bool:
        with self.proxyPool.get() as proxy:
            return proxy.setUseJumboFlag_JEDI(jediTaskID, statusStr)

    # get number of tasks with running jumbo jobs
    def getNumTasksWithRunningJumbo_JEDI(self, vo: str, prodSourceLabel: str, cloudName: str | None, workqueue: WorkQueue) -> int:
        with self.proxyPool.get() as proxy:
            return proxy.getNumTasksWithRunningJumbo_JEDI(vo, prodSourceLabel, cloudName, workqueue)

    # get number of unprocessed events
    def getNumUnprocessedEvents_JEDI(
        self, vo: str, prodSourceLabel: str, criteria: dict[str, Any], neg_criteria: dict[str, Any]
    ) -> tuple[int | None, Any, int | None]:
        with self.proxyPool.get() as proxy:
            return proxy.getNumUnprocessedEvents_JEDI(vo, prodSourceLabel, criteria, neg_criteria)

    # get number of jobs for a task
    def getNumJobsForTask_JEDI(self, jediTaskID: int) -> int | None:
        with self.proxyPool.get() as proxy:
            return proxy.getNumJobsForTask_JEDI(jediTaskID)

    # get number map for standby jobs
    def getNumMapForStandbyJobs_JEDI(self, workqueue: WorkQueue) -> tuple[dict[str, Any], dict[str, Any]]:
        with self.proxyPool.get() as proxy:
            return proxy.getNumMapForStandbyJobs_JEDI(workqueue)

    # update datasets to finish a task
    def updateDatasetsToFinishTask_JEDI(self, jediTaskID: int, lockedBy: str) -> bool:
        with self.proxyPool.get() as proxy:
            return proxy.updateDatasetsToFinishTask_JEDI(jediTaskID, lockedBy)

    # get tasks with jumbo jobs
    def getTaskWithJumbo_JEDI(self, vo: str, prodSourceLabel: str) -> dict[Any, Any]:
        with self.proxyPool.get() as proxy:
            return proxy.getTaskWithJumbo_JEDI(vo, prodSourceLabel)

    # kick pending tasks with jumbo jobs
    def kickPendingTasksWithJumbo_JEDI(self, jediTaskID: int) -> int | None:
        with self.proxyPool.get() as proxy:
            return proxy.kickPendingTasksWithJumbo_JEDI(jediTaskID)

    # reset input to re-generate co-jumbo jobs
    def resetInputToReGenCoJumbo_JEDI(self, jediTaskID: int) -> int | None:
        with self.proxyPool.get() as proxy:
            return proxy.resetInputToReGenCoJumbo_JEDI(jediTaskID)

    # get averaged disk IO
    def getAvgDiskIO_JEDI(self) -> dict[str, Any]:
        with self.proxyPool.get() as proxy:
            return proxy.getAvgDiskIO_JEDI()

    # update input files stage-in done (according to message from iDDS, called by other methods, etc.)
    def updateInputFilesStaged_JEDI(
        self,
        jeditaskid: int,
        scope: str | None,
        filenames_dict: dict[str, tuple[int | None, int | None]],
        chunk_size: int = 500,
        by: str | None = None,
        check_scope: bool = True,
    ) -> int | None:
        with self.proxyPool.get() as proxy:
            return proxy.updateInputFilesStaged_JEDI(jeditaskid, scope, filenames_dict, chunk_size, by, check_scope)

    # update input datasets stage-in done (according to message from iDDS, called by other methods, etc.)
    def updateInputDatasetsStaged_JEDI(
        self, jeditaskid: int, scope: str | None, dsnames_dict: list[str | None] | None, use_commit: bool = True, by: str | None = None
    ) -> int | None:
        with self.proxyPool.get() as proxy:
            return proxy.updateInputDatasetsStaged_JEDI(jeditaskid, scope, dsnames_dict, use_commit, by)

    # get number of staging files
    def getNumStagingFiles_JEDI(self, jeditaskid: int) -> int | None:
        with self.proxyPool.get() as proxy:
            return proxy.getNumStagingFiles_JEDI(jeditaskid)

    # get usage breakdown by users and sites
    def getUsageBreakdown_JEDI(self, prod_source_label: str = "user") -> tuple[dict[str, Any], dict[str, Any]] | None:
        with self.proxyPool.get() as proxy:
            return proxy.getUsageBreakdown_JEDI(prod_source_label)

    # get jobs stat of each user
    def getUsersJobsStats_JEDI(self, prod_source_label: str = "user") -> dict[str, Any] | None:
        with self.proxyPool.get() as proxy:
            return proxy.getUsersJobsStats_JEDI(prod_source_label)

    # insert HPO pseudo event according to message from idds
    def insertHpoEventAboutIdds_JEDI(self, jedi_task_id: int, event_id_list: list[tuple[Any, Any]]) -> bool:
        with self.proxyPool.get() as proxy:
            return proxy.insertHpoEventAboutIdds_JEDI(jedi_task_id, event_id_list)

    # get event statistics
    def get_event_statistics(self, jedi_task_id: int) -> dict[int, int] | None:
        with self.proxyPool.get() as proxy:
            return proxy.get_event_statistics(jedi_task_id)

    # get site to-running rate statistics by global share
    def getSiteToRunRateStats(
        self, vo: str, exclude_rwq: bool, starttime_min: datetime.datetime, starttime_max: datetime.datetime
    ) -> tuple[bool, dict[str, Any]]:
        with self.proxyPool.get() as proxy:
            return proxy.getSiteToRunRateStats(vo, exclude_rwq, starttime_min, starttime_max)

    # update cache
    def updateCache_JEDI(self, main_key: str, sub_key: str | None, data: str) -> bool:
        with self.proxyPool.get() as proxy:
            return proxy.updateCache_JEDI(main_key, sub_key, data)

    # get cache
    def getCache_JEDI(self, main_key: str, sub_key: str | None) -> JediCacheSpec | None:
        with self.proxyPool.get() as proxy:
            return proxy.getCache_JEDI(main_key, sub_key)

    # get cache
    def extendSandboxLifetime_JEDI(self, jedi_taskid: int, file_name: str) -> int | None:
        with self.proxyPool.get() as proxy:
            return proxy.extendSandboxLifetime_JEDI(jedi_taskid, file_name)

    # turn a task into pending status for some reason
    def makeTaskPending_JEDI(self, jedi_taskid: int, reason: str = "unknown") -> int | None:
        with self.proxyPool.get() as proxy:
            return proxy.makeTaskPending_JEDI(jedi_taskid, reason)

    # query tasks and turn them into pending status for some reason, sql_query should query jeditaskid
    def queryTasksToBePending_JEDI(self, sql_query: str, params_map: dict[str, Any], reason: str) -> int | None:
        with self.proxyPool.get() as proxy:
            return proxy.queryTasksToBePending_JEDI(sql_query, params_map, reason)

    # get IDs of all datasets of input and lib, to update data locality records
    def get_tasks_inputdatasets_JEDI(self, vo: str) -> list[Any] | None:
        with self.proxyPool.get() as proxy:
            return proxy.get_tasks_inputdatasets_JEDI(vo)

    # get dataset locality for a task and dataset
    def get_dataset_locality(self, jedi_taskid: int, datasetid: int) -> list[str] | None:
        with self.proxyPool.get() as proxy:
            return proxy.get_dataset_locality(jedi_taskid, datasetid)

    # update dataset locality
    def updateDatasetLocality_JEDI(self, jedi_taskid: int, datasetid: int, rse: str) -> bool:
        with self.proxyPool.get() as proxy:
            return proxy.updateDatasetLocality_JEDI(jedi_taskid, datasetid, rse)

    # delete outdated dataset locality records
    def deleteOutdatedDatasetLocality_JEDI(self, before_timestamp: datetime.datetime) -> int | None:
        with self.proxyPool.get() as proxy:
            return proxy.deleteOutdatedDatasetLocality_JEDI(before_timestamp)

    # query tasks and preassign them to dedicate workqueue, sql_query should query jeditaskid
    def queryTasksToPreassign_JEDI(
        self, sql_query: str, params_map: dict[str, Any], site: str, blacklist: list[str] | set[str] = set(), limit: int = 1
    ) -> list[tuple[Any, dict[str, Any]]] | None:
        with self.proxyPool.get() as proxy:
            return proxy.queryTasksToPreassign_JEDI(sql_query, params_map, site, blacklist, limit)

    # close and reassign N jobs of a preassigned task
    def reassignJobsInPreassignedTask_JEDI(self, jedi_taskid: int, site: str, n_jobs_to_close: int) -> int | None:
        with self.proxyPool.get() as proxy:
            return proxy.reassignJobsInPreassignedTask_JEDI(jedi_taskid, site, n_jobs_to_close)

    # undo preassigned tasks
    def undoPreassignedTasks_JEDI(
        self, jedi_taskids: list[int], task_orig_attr_map: dict[str, Any], params_map: dict[str, Any], force: bool = False
    ) -> list[int] | None:
        with self.proxyPool.get() as proxy:
            return proxy.undoPreassignedTasks_JEDI(jedi_taskids, task_orig_attr_map, params_map, force)

    # set missing files according to iDDS messages
    def setMissingFilesAboutIdds_JEDI(self, jeditaskid: int, filenames_dict: dict[str, Any]) -> int | None:
        with self.proxyPool.get() as proxy:
            return proxy.setMissingFilesAboutIdds_JEDI(jeditaskid, filenames_dict)

    # set missing files according to iDDS messages
    def load_sw_map(self) -> dict[str, Any] | None:
        with self.proxyPool.get() as proxy:
            return proxy.load_sw_map()

    # get origin datasets
    def get_origin_datasets(self, jedi_task_id: int, dataset_name: str, lfns: list[str]) -> list[str] | None:
        with self.proxyPool.get() as proxy:
            return proxy.get_origin_datasets(jedi_task_id, dataset_name, lfns)

    # push message to message processors which triggers functions of agents
    def push_task_trigger_message(
        self,
        msg_type: str,
        jedi_task_id: int | None,
        data_dict: dict[str, Any] | None = None,
        priority: int | None = None,
        task_spec: "JediTaskSpec | None" = None,
    ) -> bool | None:
        with self.proxyPool.get() as proxy:
            return proxy.push_task_trigger_message(msg_type, jedi_task_id, data_dict, priority, task_spec)

    # aggregate carbon footprint of a task
    def get_task_carbon_footprint(self, jedi_task_id: int, level: str = "global") -> dict[str, Any] | None:
        with self.proxyPool.get() as proxy:
            return proxy.get_task_carbon_footprint(jedi_task_id, level)

    # get pending data carousel tasks and their input datasets
    def get_pending_dc_tasks_JEDI(self, task_type: str = "prod", time_limit_minutes: int = 60) -> dict[str, list[int]] | None:
        with self.proxyPool.get() as proxy:
            return proxy.get_pending_dc_tasks_JEDI(task_type=task_type, time_limit_minutes=time_limit_minutes)

    # get max number of events in a file of the dataset
    def get_max_events_in_dataset(self, jedi_task_id: int, dataset_id: int) -> int | None:
        with self.proxyPool.get() as proxy:
            return proxy.get_max_events_in_dataset(jedi_task_id, dataset_id)

    # get task failure metrics
    def get_task_failure_metrics(self, jedi_task_id: int) -> dict[str, Any] | None:
        with self.proxyPool.get() as proxy:
            return proxy.get_task_failure_metrics(jedi_task_id)

    # reset frozen time of a task to avoid being exhausted
    def reset_frozen_time_for_task(self, task_id: int) -> bool:
        with self.proxyPool.get() as proxy:
            return proxy.reset_frozen_time_for_task(task_id)
