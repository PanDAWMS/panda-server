import sys

try:
    testTaskType = sys.argv[1]
except Exception:
    testTaskType = "test"

try:
    vo = sys.argv[2]
except Exception:
    vo = "atlas"

import multiprocessing

from pandajedi.jedicore.JediTaskBufferInterface import JediTaskBufferInterface
from pandajedi.jediddm.DDMInterface import DDMInterface
from pandajedi.jediorder import TaskRefiner

tbIF = JediTaskBufferInterface()
tbIF.setupInterface()


ddmIF = DDMInterface()
ddmIF.setupInterface()


parent_conn, child_conn = multiprocessing.Pipe()

taskRefiner = multiprocessing.Process(target=TaskRefiner.launcher, args=(child_conn, tbIF, ddmIF, vo, testTaskType))
taskRefiner.start()
