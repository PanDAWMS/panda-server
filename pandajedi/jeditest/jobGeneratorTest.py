import multiprocessing
import sys

from pandajedi.jedicore.JediTaskBufferInterface import JediTaskBufferInterface
from pandajedi.jediddm.DDMInterface import DDMInterface
from pandajedi.jediorder import JobGenerator

tbIF = JediTaskBufferInterface()
tbIF.setupInterface()


ddmIF = DDMInterface()
ddmIF.setupInterface()


parent_conn, child_conn = multiprocessing.Pipe()

try:
    testVO = sys.argv[1]
except Exception:
    testVO = "atlas"

try:
    testTaskType = sys.argv[2]
except Exception:
    testTaskType = "test"

try:
    execJob = False
    if sys.argv[3] == "y":
        execJob = True
except Exception:
    pass

try:
    testClouds = sys.argv[4].split(",")
except Exception:
    testClouds = [None]

print(f"{testVO} {testTaskType} {testClouds}")

gen = multiprocessing.Process(target=JobGenerator.launcher, args=(child_conn, tbIF, ddmIF, testVO, testTaskType, testClouds, False, execJob))
gen.start()
