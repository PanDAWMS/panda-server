'''
master hander for DDM

'''

import re
import threading

from pandaserver.dataservice.Finisher  import Finisher
from pandaserver.dataservice.Activator import Activator

from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandalogger.LogWrapper import LogWrapper

# logger
_logger = PandaLogger().getLogger('DDMHandler')


class DDMHandler (threading.Thread):
    # constructor
    def __init__(self,taskBuffer,vuid,site=None,dataset=None,scope=None):
        threading.Thread.__init__(self)
        self.vuid       = vuid
        self.taskBuffer = taskBuffer
        self.site       = site
        self.scope      = scope
        self.dataset    = dataset


    # main
    def run(self):
        # get logger
        tmpLog = LogWrapper(_logger,'<vuid={0} site={1} name={2}>'.format(self.vuid,
                                                                          self.site,
                                                                          self.dataset))
        # query dataset
        tmpLog.debug("start")
        if self.vuid is not None:
            dataset = self.taskBuffer.queryDatasetWithMap({'vuid':self.vuid})
        else:
            dataset = self.taskBuffer.queryDatasetWithMap({'name':self.dataset})
        if dataset is None:
            tmpLog.error("Not found")
            tmpLog.debug("end")
            return
        tmpLog.debug("type:%s name:%s" % (dataset.type,dataset.name))
        if dataset.type == 'dispatch':
            # activate jobs in jobsDefined
            Activator(self.taskBuffer,dataset).start()
        if dataset.type == 'output':
            if dataset.name is not None and re.search('^panda\..*_zip$',dataset.name) is not None:
                # start unmerge jobs
                Activator(self.taskBuffer,dataset,enforce=True).start()
            else:
                # finish transferring jobs
                Finisher(self.taskBuffer,dataset,site=self.site).start()
        tmpLog.debug("end")
