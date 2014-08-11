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
            _logger.debug("initialize memcache client with %s" % panda_config.memcached_srvs)
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
            _logger.debug("setFiles site=%s node=%s start" % (site,node))
            # key prefix
            keyPrefix = self.getKeyPrefix(site,node)
            # failed to get key prefix   
            if keyPrefix == None:
                _logger.error("setFiles failed to get key prefix")
                return False
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
                _logger.error("setFiles failed to insert %s values for site=%s node=%s" % \
                              (len(failedList),site,node))
                return False
            _logger.debug("setFiles site=%s node=%s completed" % (site,node))
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
            # key prefix
            keyPrefix = self.getKeyPrefix(site,node)
            # non-existing key    
            if keyPrefix == None:
                _logger.debug("deleteFiles skipped for non-existing key")
                return True
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
    def checkFiles(self,pandaID,files,site,node,keyPrefix='',getDetail=False):
        try:
            _logger.debug("checkFiles PandaID=%s with %s:%s start" % (pandaID,site,node))
            # get key prefix
            if keyPrefix == '':
                keyPrefix = self.getKeyPrefix(site,node)
            # non-existing key    
            if keyPrefix == None:
                _logger.debug("checkFiles PandaID=%s with %s:%s doesn't exist" % \
                              (pandaID,site,node))
                return 0
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
            # return detailed string
            if getDetail:
                retStr = ''
                for tmpFile in files:
                    if retMap.has_key(tmpFile):
                        retStr += '1,'
                    else:
                        retStr += '0,'
                retStr = retStr[:-1]
                return retStr
            # return number of files
            return len(retMap)
        except:
            errType,errValue = sys.exc_info()[:2]
            _logger.error("checkFiles failed with %s %s" % (errType,errValue))
            return 0


    # flush files
    def flushFiles(self,site,node):
        try:
            _logger.debug("flushFiles for %s:%s start" % (site,node))
            # key prefix stored in memcached
            keyPrefix = self.getInternalKeyPrefix(site,node)
            # increment
            serNum = self.mclient.incr(keyPrefix)
            # return if not exist
            if serNum == None:
                _logger.debug("flushFiles skipped for non-existing key")
                return True
            # avoid overflow
            if serNum > 1024:
                serNum = 0
            # set    
            retS = self.mclient.set(keyPrefix, serNum, time=panda_config.memcached_exptime)
            if retS == 0:
                # failed
                _logger.error("flushFiles failed to set new SN") 
                return False
            _logger.error("flushFiles completed")
            return True
        except:
            errType,errValue = sys.exc_info()[:2]
            _logger.error("flushFiles failed with %s %s" % (errType,errValue))
            return False


    # get internal key prefix
    def getInternalKeyPrefix(self,site,node):
        # get short WN name
        shortWN = node.split('.')[0]
        # key prefix stored in memcached
        keyPrefix = '%s_%s' % (site,shortWN)
        return keyPrefix
    
        
    # get key prefix
    def getKeyPrefix(self,site,node):
        # key prefix stored in memcached
        keyPrefix = self.getInternalKeyPrefix(site,node)
        # get serial number from memcached
        serNum = self.mclient.get(keyPrefix)
        # use 0 if not exist
        if serNum == None:
            serNum = 0
        # set to avoid expiration   
        retS = self.mclient.set(keyPrefix,serNum,time=panda_config.memcached_exptime)
        if retS == 0:
            # failed
            return None
        else:
            # return prefix site_node_sn_
            newPrefix = '%s_%s' % (keyPrefix,serNum)
            return newPrefix
