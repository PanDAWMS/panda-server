import multiprocessing

from pandajedi.jedicore.JediTaskBufferInterface import JediTaskBufferInterface
from pandajedi.jediddm.DDMInterface import DDMInterface
from pandajedi.jediorder import TaskCommando

tbIF = JediTaskBufferInterface()
tbIF.setupInterface()


ddmIF = DDMInterface()
ddmIF.setupInterface()


parent_conn, child_conn = multiprocessing.Pipe()

try:
    testVO = sys.argv[1]
except Exception:
    testVO = "any"

try:
    testTaskType = sys.argv[2]
except Exception:
    testTaskType = "any"

taskCommando = multiprocessing.Process(target=TaskCommando.launcher, args=(child_conn, tbIF, ddmIF, testVO, testTaskType))
taskCommando.start()
