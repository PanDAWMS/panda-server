'''
awake jobs in waiting table

'''

import time
import threading
from DDM import ddm

from pandalogger.PandaLogger import PandaLogger

# logger
_logger = PandaLogger().getLogger('Waker')


class Waker (threading.Thread):
    # constructor
    def __init__(self,taskBuffer,dataset):
        threading.Thread.__init__(self)
        self.dataset = dataset
        self.taskBuffer = taskBuffer


    # main
    def run(self):
        _logger.debug("start: %s" % self.dataset.name)
        # get file list from DDM
        for iDDMTry in range(3):        
            status,out = ddm.DQ2.main('listFilesInDataset',self.dataset.name)
            if status != 0 and out.find("DQ2 unknown dataset exception") != -1:
                break
            elif status != 0 or out.find("DQ2 internal server exception") != -1:
                time.sleep(60)
            else:
                break
        if status != 0 or out.startswith('Error'):
            _logger.error(out)
            _logger.debug("failed: %s" % self.dataset.name)            
            return
        # parse
        lfns = []
        try:
            exec "resDQ=%s" % out
            for guid,vals in resDQ[0].iteritems():
                lfns.append(vals['lfn'])
        except:
            _logger.error("could not parse %s" % out)
        # get PandaIDs of jobs which use files with LFNs
        if len(lfns) != 0:
            ids = self.taskBuffer.queryPandaIDwithLFN(lfns)
            _logger.debug("IDs: %s" % ids)
            if len(ids) != 0:
                # awake jobs
                self.taskBuffer.awakeJobs(ids)
        _logger.debug("finished: %s" % self.dataset.name)
