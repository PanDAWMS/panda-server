import multiprocessing

from pandajedi.jedicore.JediTaskBufferInterface import JediTaskBufferInterface
from pandajedi.jediddm.DDMInterface import DDMInterface
from pandajedi.jediorder import PostProcessor

tbIF = JediTaskBufferInterface()
tbIF.setupInterface()


ddmIF = DDMInterface()
ddmIF.setupInterface()


parent_conn, child_conn = multiprocessing.Pipe()

postProcessor = multiprocessing.Process(target=PostProcessor.launcher, args=(child_conn, tbIF, ddmIF))
postProcessor.start()
