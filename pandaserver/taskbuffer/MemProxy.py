# proxy for memcached

import sys

from config import panda_config

from pandalogger.PandaLogger import PandaLogger

# logger
_logger = PandaLogger().getLogger('MemProxy')


# proxy
class MemProxy:

    # constructor
    def __init__(self):
        try:
            import memcache
            # initialize memcached client
            _logger.debug("initialize memcache client")
            self.mclient = memcache.Client(panda_config.memcached_srvs.split(','))
            # server statistics
            _logger.debug(self.mclient.get_stats())
            _logger.debug("memcache client is ready")
        except:
            errType,errValue = sys.exc_info()[:2]
            _logger.error("failed to initialize memcach client : %s %s" % (errType,errValue))

        
    # insert files
    def setFiles(self,pandaID,site,node,files):
        try:
            _logger.debug("setFiles PandaID=%s start" % pandaID)
            # get short WN name
            shortWN = node.split('.')[0]
            # key prefix
            keyPrefix = '%s_%s_' % (site,shortWN)
            # loop over all files
            varMap = {}
            for tmpFile in files:
                newKey = tmpFile
                varMap[newKey] = True
            # bulk insert        
            failedList = self.mclient.set_multi(varMap,time=panda_config.memcached_exptime,
                                                key_prefix=keyPrefix)
            # failed
            if failedList != []:
                _logger.error("setFiles failed to insert %s values for PandaID=%s" % \
                              (len(failedList),pandaID))
                return False
            _logger.debug("setFiles PandaID=%s completed" % pandaID)
            return True
        except:
            errType,errValue = sys.exc_info()[:2]
            _logger.error("setFiles failed with %s %s" % (errType,errValue))
            return False
        

    # delete files
    def deleteFiles(self,site,node,files):
        try:
            fileList = files.split(',')
            # remove ''
            try:
                fileList.remove('')
            except:
                pass
            _logger.debug("deleteFiles for %s:%s:%s start" % (site,node,len(fileList)))
            # empty list
            if len(fileList) == 0:
                _logger.debug("deleteFiles skipped for empty list")
                return True
            # get short WN name
            shortWN = node.split('.')[0]
            # key prefix
            keyPrefix = '%s_%s_' % (site,shortWN)
            # get the number of bunches
            nKeys = 100
            tmpDiv,tmpMod = divmod(len(fileList),nKeys)
            if tmpMod != 0:
                tmpDiv += 1
            # loop over all bunches
            retMap = {True:0,False:0}
            for idxB in range(tmpDiv):
                # delete
                retD = self.mclient.delete_multi(fileList[idxB*nKeys:(idxB+1)*nKeys],
                                                 key_prefix=keyPrefix)
                if retD == 1:
                    retMap[True] += 1
                else:
                    retMap[False] += 1                     
            # failed
            if retMap[False] != 0:
                _logger.error("deleteFiles failed %s/%s" % (retMap[False],
                                                            retMap[True]+retMap[False]))
                return False
            _logger.debug("deleteFiles succeeded")
            return True
        except:
            errType,errValue = sys.exc_info()[:2]
            _logger.error("deleteFiles failed with %s %s" % (errType,errValue))
            return False
                           

    # check files
    def checkFiles(self,pandaID,files,site,node):
        try:
            _logger.debug("checkFiles PandaID=%s with %s:%s start" % (pandaID,site,node))
            # get short WN name
            shortWN = node.split('.')[0]
            # key prefix
            keyPrefix = '%s_%s_' % (site,shortWN)
            # loop over all files
            keyList = []
            for tmpFile in files:
                newKey = tmpFile
                if not newKey in keyList:
                    keyList.append(newKey)
            # bulk get
            retMap = self.mclient.get_multi(keyList,key_prefix=keyPrefix)
            _logger.debug("checkFiles PandaID=%s with %s:%s has %s files" % \
                          (pandaID,site,node,len(retMap)))
            return len(retMap)
        except:
            errType,errValue = sys.exc_info()[:2]
            _logger.error("checkFiles failed with %s %s" % (errType,errValue))
            return 0
