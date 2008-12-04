'''
master hander for DDM

'''

import re
import threading

from Waker     import Waker
from Finisher  import Finisher
from Activator import Activator

from pandalogger.PandaLogger import PandaLogger

# logger
_logger = PandaLogger().getLogger('DDMHandler')


class DDMHandler (threading.Thread):
    # constructor
    def __init__(self,taskBuffer,vuid):
        threading.Thread.__init__(self)
        self.vuid    = vuid
        self.taskBuffer = taskBuffer


    # main
    def run(self):
        # query dataset
        _logger.debug("start: %s" % self.vuid)
        dataset = self.taskBuffer.queryDatasetWithMap({'vuid':self.vuid})
        if dataset == None:
            _logger.error("Not found : %s" % self.vuid)
            _logger.debug("end: %s" % self.vuid)            
            return
        _logger.debug("vuid:%s type:%s name:%s" % (self.vuid,dataset.type,dataset.name))
        if dataset.type == 'dispatch':
            # activate jobs in jobsDefined
            Activator(self.taskBuffer,dataset).start()
        if dataset.type == 'output':
            # finish transferring jobs
            Finisher(self.taskBuffer,dataset).start()
            # awake jobs in jobsWaiting
            #Waker(self.taskBuffer,dataset).start()
        _logger.debug("end: %s" % self.vuid)
