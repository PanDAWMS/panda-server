import sys

from pandajedi.jedicore.JediTaskBufferInterface import JediTaskBufferInterface
from pandajedi.jediddm.DDMInterface import DDMInterface
from pandajedi.jediorder.ContentsFeeder import ContentsFeederThread

jedi_task_id = int(sys.argv[1])

tbIF = JediTaskBufferInterface()
tbIF.setupInterface()

ddmIF = DDMInterface()
ddmIF.setupInterface()

task_ds_list = tbIF.getDatasetsToFeedContents_JEDI(task_id=jedi_task_id, force_read=True)

c = ContentsFeederThread(taskDsList=None, threadPool=None, taskbufferIF=tbIF, ddmIF=ddmIF, pid="")
c.feed_contents_to_tasks(task_ds_list, real_run=False)
