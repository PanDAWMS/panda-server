import sys

from pandacommon.pandalogger.PandaLogger import PandaLogger

from pandajedi.jedicore.JediTaskBufferInterface import JediTaskBufferInterface
from pandajedi.jedicore.MsgWrapper import MsgWrapper
from pandajedi.jediddm.DDMInterface import DDMInterface
from pandajedi.jediorder.TaskRefiner import TaskRefiner
from pandajedi.jedirefine import RefinerUtils

logger = PandaLogger().getLogger("TaskRefiner")
tmpLog = MsgWrapper(logger)

tbIF = JediTaskBufferInterface()
tbIF.setupInterface()

siteMapper = tbIF.get_site_mapper()

ddmIF = DDMInterface()
ddmIF.setupInterface()


jediTaskID = int(sys.argv[1])

s, taskSpec = tbIF.getTaskWithID_JEDI(jediTaskID)
refiner = TaskRefiner(None, tbIF, ddmIF, taskSpec.vo, taskSpec.prodSourceLabel)
refiner.initializeMods(tbIF, ddmIF)


taskParam = tbIF.getTaskParamsWithID_JEDI(jediTaskID)
taskParamMap = RefinerUtils.decodeJSON(taskParam)

vo = taskParamMap["vo"]
prodSourceLabel = taskParamMap["prodSourceLabel"]
taskType = taskParamMap["taskType"]

cloudName = taskSpec.cloud
vo = taskSpec.vo
prodSourceLabel = taskSpec.prodSourceLabel
queueID = taskSpec.workQueue_ID
gshare_name = taskSpec.gshare

impl = refiner.instantiateImpl(vo, prodSourceLabel, taskType, tbIF, ddmIF)

workQueueMapper = tbIF.getWorkQueueMap()

impl.initializeRefiner(tmpLog)
impl.extractCommon(jediTaskID, taskParamMap, workQueueMapper, taskSpec.splitRule)
impl.doRefine(jediTaskID, taskParamMap)
