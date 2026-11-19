# DB API for JEDI

from typing import Any

from pandacommon.pandalogger.PandaLogger import PandaLogger

from pandajedi.jediconfig import jedi_config
from pandaserver.taskbuffer import TaskBuffer
from pandaserver.taskbuffer.InputChunk import InputChunk
from pandaserver.taskbuffer.JediDatasetSpec import JediDatasetSpec
from pandaserver.taskbuffer.JediTaskSpec import JediTaskSpec
from pandaserver.taskbuffer.WorkQueue import WorkQueue

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

    # set tasks to be assigned
    def setScoutJobDataToTasks_JEDI(self, vo: str | None, prodSourceLabel: str | None) -> bool | None:
        # get the site mapper before taking a proxy: rebuilding it takes one from the same pool,
        # which holds a single proxy in a task-buffer child process
        tmp_site_mapper = self.get_site_mapper()
        with self.proxyPool.get() as proxy:
            return proxy.set_scout_job_data_to_tasks_jedi_db(vo, prodSourceLabel, tmp_site_mapper)

    # prepare tasks to be finished
    def prepareTasksToBeFinished_JEDI(
        self, vo: str | None, prodSourceLabel: str | None, nTasks: int = 50, simTasks: list[int] | None = None, pid: str = "lock", noBroken: bool = False
    ) -> list[int] | None:
        # get the site mapper before taking a proxy, as in setScoutJobDataToTasks_JEDI
        tmp_site_mapper = self.get_site_mapper()
        with self.proxyPool.get() as proxy:
            return proxy.prepare_tasks_to_be_finished_jedi_db(vo, prodSourceLabel, nTasks, simTasks, pid, noBroken, tmp_site_mapper)

    # get jobsetID
    def getUserJobsetID_JEDI(self, userName: str) -> tuple[bool, int]:
        with self.proxyPool.get() as proxy:
            tmpJobID, tmpDummy, tmpStat = proxy.getUserParameter(userName, 1, None)
            # return
            return tmpStat, tmpJobID
