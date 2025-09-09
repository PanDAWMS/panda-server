import multiprocessing
import sys

from pandajedi.jedicore.JediTaskBufferInterface import JediTaskBufferInterface
from pandajedi.jediddm.DDMInterface import DDMInterface
from pandajedi.jediorder import TaskBroker

try:
    testTaskType = sys.argv[1]
except Exception:
    testTaskType = "test"

tbIF = JediTaskBufferInterface()
tbIF.setupInterface()

ddmIF = DDMInterface()
ddmIF.setupInterface()

parent_conn, child_conn = multiprocessing.Pipe()

taskBroker = multiprocessing.Process(target=TaskBroker.launcher, args=(child_conn, tbIF, ddmIF, "atlas", testTaskType))
taskBroker.start()
