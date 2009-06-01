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
    _logger.debug("putFile : start %s" % req.subprocess_env['SSL_CLIENT_S_DN'])
    # size check
    sizeLimit = 10*1024*1024
    if not file.filename.startswith('sources.'):
        # get file size
        contentLength = 0
        try:
            contentLength = long(req.headers_in["content-length"])
        except:
            if req.headers_in.has_key("content-length"):
                _logger.error("cannot get CL : %s" % req.headers_in["content-length"])
            else:
                _logger.error("no CL")
        _logger.debug("size %s" % contentLength)
        if contentLength > sizeLimit:
            errStr = "ERROR : Upload failure. Exceeded size limit %s>%s. Please submit job without --noBuild/--libDS" % (contentLength,sizeLimit)
            _logger.error(errStr)
            _logger.debug("putFile : end")            
            return errStr
    fo = open('%s/%s' % (panda_config.cache_dir,file.filename),'wb')
    fo.write(file.file.read())
    fo.close()
    _logger.debug("putFile : %s end" % file.filename)
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
def fetchLog(req,logName,offset=0):
    _logger.debug("fetchLog : %s start offset=%s" % (logName,offset))
    # stdout name
    fullLogName  = '%s/%s' % (panda_config.cache_dir,logName)
    # put dummy char to avoid Internal Server Error
    retStr = ' '
    try:
        # read
        ft = open(fullLogName,'r')
        ft.seek(long(offset))
        retStr += ft.read()
        ft.close()
    except:
        type, value, traceBack = sys.exc_info()
        _logger.error("fetchLog : %s %s" % (type,value))
    _logger.debug("fetchLog : %s end read=%s" % (logName,len(retStr)))
    return retStr
