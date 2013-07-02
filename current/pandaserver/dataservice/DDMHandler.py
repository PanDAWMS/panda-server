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
    def __init__(self,taskBuffer,vuid,site=None):
        threading.Thread.__init__(self)
        self.vuid       = vuid
        self.taskBuffer = taskBuffer
        self.site       = site


    # main
    def run(self):
        # query dataset
        _logger.debug("start: %s %s" % (self.vuid,self.site))
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
            if dataset.name != None and re.search('^panda\..*_zip$',dataset.name) != None:
                # start unmerge jobs
                Activator(self.taskBuffer,dataset,enforce=True).start()
            else:
                # finish transferring jobs
                Finisher(self.taskBuffer,dataset,site=self.site).start()
        _logger.debug("end: %s" % self.vuid)
