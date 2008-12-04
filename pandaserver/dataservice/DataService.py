"""
provide web service for DDM

"""

import re
from config import panda_config
from pandalogger.PandaLogger import PandaLogger

# logger
_logger = PandaLogger().getLogger('DataService')


class DataService:
    # constructor
    def __init__(self):
        self.taskBuffer = None

    # set taskbuffer
    def init(self,taskBuffer):
        self.taskBuffer = taskBuffer

# Singleton
dataService = DataService()
del DataService


'''
web interface

'''

from DDMHandler import DDMHandler


# callback for dataset verification
def datasetCompleted(req,vuid,site=None):
    DDMHandler(dataService.taskBuffer,vuid).start()
    # logging
    try:
        # get siteID
        rhost = req.get_remote_host()
        siteID = site
        # make message
        message = '%s - siteID:%s vuid:%s' % (rhost,siteID,vuid)
        _logger.debug(message)
        # get logger
        _pandaLogger = PandaLogger()
        _pandaLogger.lock()
        _pandaLogger.setParams({'Type':'datasetCompleted'})
        logger = _pandaLogger.getHttpLogger(panda_config.loggername)
        # add message
        logger.info(message)
        # release HTTP handler
        _pandaLogger.release()
    except:
        pass
    return True
    
