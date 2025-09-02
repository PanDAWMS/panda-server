import sys

try:
    testTaskType = sys.argv[1]
except Exception:
    testTaskType = "test"

import multiprocessing

from pandajedi.jedicore.JediTaskBufferInterface import JediTaskBufferInterface
from pandajedi.jediddm.DDMInterface import DDMInterface
from pandajedi.jediorder import WatchDog

tbIF = JediTaskBufferInterface()
tbIF.setupInterface()


ddmIF = DDMInterface()
ddmIF.setupInterface()


parent_conn, child_conn = multiprocessing.Pipe()

watchDog = multiprocessing.Process(target=WatchDog.launcher, args=(child_conn, tbIF, ddmIF, "atlas", testTaskType))
watchDog.start()
