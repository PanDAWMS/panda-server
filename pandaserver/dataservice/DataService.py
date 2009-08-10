"""
provide web service for DDM

"""

import re
import sys
import cPickle as pickle
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
    DDMHandler(dataService.taskBuffer,vuid,site).start()
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
    

# get FQANs
def _getFQAN(req):
    fqans = []
    for tmpKey,tmpVal in req.subprocess_env.iteritems():
        # compact credentials
        if tmpKey.startswith('GRST_CRED_'):
            # VOMS attribute
            if tmpVal.startswith('VOMS'):
                # FQAN
                fqan = tmpVal.split()[-1]
                # append
                fqans.append(fqan)
        # old style         
        elif tmpKey.startswith('GRST_CONN_'):
            tmpItems = tmpVal.split(':')
            # FQAN
            if len(tmpItems)==2 and tmpItems[0]=='fqan':
                fqans.append(tmpItems[-1])
    # return
    return fqans


# set file status
def updateFileStatusInDisp(req,dataset,fileStatus):
    try:
        # get FQAN
        fqans = _getFQAN(req)
        roleOK = False
        # loop over all FQANs
        for fqan in fqans:
            # check production role
            for rolePat in ['/atlas/usatlas/Role=production','/atlas/Role=production']:
                if fqan.startswith(rolePat):
                    roleOK = True
                    break
        if not roleOK:
            _logger.error('updateFileStatusInDisp : invalid proxy %s' % fqans)
            return "False"
        # deserialize fileStatus
        fileStatusMap = pickle.loads(fileStatus)
        _logger.debug('updateFileStatusInDisp : start %s - %s' % (dataset,fileStatusMap))
        # update status
        dataService.taskBuffer.updateFileStatusInDisp(dataset,fileStatusMap)
        _logger.debug('updateFileStatusInDisp : done')
        return "True"
    except:
        type,value,traceBack = sys.exc_info()
        _logger.error("updateFileStatusInDisp : %s %s" % (type,value))
        return "False"
        
