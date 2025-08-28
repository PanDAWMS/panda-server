import sys

from pandacommon.pandalogger.PandaLogger import PandaLogger

from pandajedi.jedicore.JediTaskBufferInterface import JediTaskBufferInterface
from pandajedi.jedicore.MsgWrapper import MsgWrapper
from pandajedi.jediddm.DDMInterface import DDMInterface
from pandajedi.jediorder.TaskBroker import TaskBroker
from pandajedi.jedirefine import RefinerUtils

logger = PandaLogger().getLogger("TaskBroker")
tmpLog = MsgWrapper(logger)

tbIF = JediTaskBufferInterface()
tbIF.setupInterface()

siteMapper = tbIF.get_site_mapper()

ddmIF = DDMInterface()
ddmIF.setupInterface()

jediTaskID = int(sys.argv[1])

s, taskSpec = tbIF.getTaskWithID_JEDI(jediTaskID, False)
body = TaskBroker(None, tbIF, ddmIF, taskSpec.vo, taskSpec.prodSourceLabel)
body.initializeMods(tbIF, ddmIF)

taskParam = tbIF.getTaskParamsWithID_JEDI(jediTaskID)
taskParamMap = RefinerUtils.decodeJSON(taskParam)

vo = taskParamMap["vo"]
prodSourceLabel = taskParamMap["prodSourceLabel"]
taskType = taskParamMap["taskType"]

workQueueMapper = tbIF.getWorkQueueMap()
workQueue = workQueueMapper.getQueueWithIDGshare(taskSpec.workQueue_ID, taskSpec.gshare)

impl = body.getImpl(vo, prodSourceLabel)


tmpListItem = tbIF.getTasksToBeProcessed_JEDI(None, None, None, None, None, simTasks=[jediTaskID], readMinFiles=True)
impl.doBrokerage(tmpListItem, taskSpec.vo, taskSpec.prodSourceLabel, workQueue, taskSpec.resource_type)
