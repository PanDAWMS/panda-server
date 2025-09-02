import os
import socket
import sys

from pandajedi.jedicore.JediTaskBufferInterface import JediTaskBufferInterface
from pandajedi.jedicore.ThreadUtils import ListWithLock, ThreadPool
from pandajedi.jediddm.DDMInterface import DDMInterface
from pandajedi.jediorder.JobGenerator import JobGeneratorThread
from pandajedi.jediorder.TaskSetupper import TaskSetupper

tbIF = JediTaskBufferInterface()
tbIF.setupInterface()

siteMapper = tbIF.get_site_mapper()

ddmIF = DDMInterface()
ddmIF.setupInterface()

jediTaskID = int(sys.argv[1])

# get task attributes
s, taskSpec = tbIF.getTaskWithID_JEDI(jediTaskID)
pid = f"{socket.getfqdn().split('.')[0]}-{os.getpid()}_{os.getpgrp()}-sgen"
vo = taskSpec.vo
prodSourceLabel = taskSpec.prodSourceLabel
workQueue = tbIF.getWorkQueueMap().getQueueWithIDGshare(taskSpec.workQueue_ID, taskSpec.gshare)

# get inputs
tmpList = tbIF.getTasksToBeProcessed_JEDI(pid, None, workQueue, None, None, nFiles=1000, target_tasks=[jediTaskID])
inputList = ListWithLock(tmpList)

# create thread
threadPool = ThreadPool()
taskSetupper = TaskSetupper(vo, prodSourceLabel)
taskSetupper.initializeMods(tbIF, ddmIF)
gen = JobGeneratorThread(inputList, threadPool, tbIF, ddmIF, siteMapper, True, taskSetupper, pid, workQueue, "sgen", None, None, None, False)
gen.start()
gen.join()
