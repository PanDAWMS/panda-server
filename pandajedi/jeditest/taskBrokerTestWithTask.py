import sys

from pandajedi.jedicore.FactoryBase import FactoryBase
from pandajedi.jedicore.JediTaskBufferInterface import JediTaskBufferInterface
from pandajedi.jediddm.DDMInterface import DDMInterface
from pandajedi.jediorder.TaskBroker import TaskBroker

tbIF = JediTaskBufferInterface()
tbIF.setupInterface()

siteMapper = tbIF.get_site_mapper()


ddmIF = DDMInterface()
ddmIF.setupInterface()

jedi_task_id = int(sys.argv[1])

s, task_spec = tbIF.getTaskWithID_JEDI(jedi_task_id)

vo = task_spec.vo
prodSourceLabel = task_spec.prodSourceLabel
resource_type = task_spec.resource_type
queue_id = task_spec.workQueue_ID
gshare_name = task_spec.gshare

work_queue = tbIF.getWorkQueueMap().getQueueWithIDGshare(queue_id, gshare_name)


task_list = tbIF.getTasksToBeProcessed_JEDI(None, None, None, None, None, simTasks=[jedi_task_id], readMinFiles=True, fullSimulation=True)
for ts in task_list:
    for t in ts[1]:
        t[0].nucleus = None

task_broker = TaskBroker(None, tbIF, ddmIF, [vo], [prodSourceLabel])
FactoryBase.initializeMods(task_broker, task_broker.taskBufferIF, task_broker.ddmIF)
impl = task_broker.getImpl(vo, prodSourceLabel)
impl.doBrokerage(task_list, vo, prodSourceLabel, work_queue, resource_type)
