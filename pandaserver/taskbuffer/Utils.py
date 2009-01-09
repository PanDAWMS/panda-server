"""
utility service

"""
import os
import re
import sys
import zlib
import jobdispatcher.Protocol as Protocol
from config import panda_config

from pandalogger.PandaLogger import PandaLogger

# logger
_logger = PandaLogger().getLogger('Utils')

# check if server is alive
def isAlive(req):
    return "alive=yes"


# upload file 
def putFile(req,file):
    if not Protocol.isSecure(req):
        return False
    fo = open('%s/%s' % (panda_config.cache_dir,file.filename),'wb')
    fo.write(file.file.read())
    fo.close()
    return True


# delete file 
def deleteFile(req,file):
    if not Protocol.isSecure(req):
        return 'False'
    try:
        os.remove('%s/%s' % (panda_config.cache_dir,file))
        return 'True'
    except:
        return 'False'        
                        

# get server name:port for SSL
def getServer(req):
    return "%s:%s" % (panda_config.pserverhost,panda_config.pserverport)

 
# update stdout
def updateLog(req,file):
    _logger.debug("updateLog : %s start" % file.filename)
    # stdout name
    logName  = '%s/%s' % (panda_config.cache_dir,file.filename)
    # write to file
    try:
        # expand
        extStr = zlib.decompress(file.file.read())
        # append
        ft = open(logName,'wa')
        ft.write(extStr)
        ft.close()
    except:
        type, value, traceBack = sys.exc_info()
        _logger.error("updateLog : %s %s" % (type,value))
    _logger.debug("updateLog : %s end" % file.filename)
    return True


# fetch stdout
def fetchLog(req,PandaID,offset=0):
    _logger.debug("fetchLog : %s start offset=%s" % (PandaID,offset))
    # stdout name
    logName  = '%s/stdout.%s' % (panda_config.cache_dir,PandaID)
    # put dummy char to avoid Internal Server Error
    retStr = ' '
    try:
        # read
        ft = open(logName,'r')
        ft.seek(long(offset))
        retStr += ft.read()
        ft.close()
    except:
        type, value, traceBack = sys.exc_info()
        _logger.error("fetchLog : %s %s" % (type,value))
    _logger.debug("fetchLog : %s end read=%s" % (PandaID,len(retStr)))
    return retStr
