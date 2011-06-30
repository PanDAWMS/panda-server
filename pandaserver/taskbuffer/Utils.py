"""
utility service

"""
import os
import re
import sys
import zlib
import uuid
import datetime
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
    _logger.debug("putFile : start %s %s" % (req.subprocess_env['SSL_CLIENT_S_DN'],file.filename))
    # size check
    fullSizeLimit = 768*1024*1024
    if not file.filename.startswith('sources.'):
        noBuild = True
        sizeLimit = 10*1024*1024
    else:
        noBuild = False
        sizeLimit = fullSizeLimit
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
        errStr = "ERROR : Upload failure. Exceeded size limit %s>%s." % (contentLength,sizeLimit)
        if noBuild:
            errStr += " Please submit the job without --noBuild/--libDS since those options impose a tighter size limit"
        else:
            errStr += " Please remove redundant files from your workarea"
        _logger.error(errStr)
        _logger.debug("putFile : end")            
        return errStr
    fo = open('%s/%s' % (panda_config.cache_dir,file.filename.split('/')[-1]),'wb')
    fo.write(file.file.read())
    fo.close()
    _logger.debug("putFile : %s end" % file.filename)
    return True


# get event picking request
def putEventPickingRequest(req,runEventList='',eventPickDataType='',eventPickStreamName='',
                           eventPickDS='',eventPickAmiTag='',userDatasetName='',lockedBy='',
                           params=''):
    if not Protocol.isSecure(req):
        return "ERROR : no HTTPS"
    userName = req.subprocess_env['SSL_CLIENT_S_DN']
    creationTime = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    _logger.debug("putEventPickingRequest : %s start" % userName)
    # size check
    sizeLimit = 10*1024*1024
    # get total size
    try:
        contentLength = long(req.headers_in["content-length"])
    except:
        errStr = "cannot get content-length from HTTP request."
        _logger.error("putEventPickingRequest : " + errStr + " " + userName)
        _logger.debug("putEventPickingRequest : %s end" % userName)
        return "ERROR : " + errStr
    _logger.debug("size %s" % contentLength)
    if contentLength > sizeLimit:
        errStr = "Too large run/event list. Exceeded size limit %s>%s." % (contentLength,sizeLimit)
        _logger.error("putEventPickingRequest : " + errStr + " " + userName)
        _logger.debug("putEventPickingRequest : %s end" % userName)
        return "ERROR : " + errStr
    try:
        # make filename
        evpFileName = '%s/evp.%s' % (panda_config.cache_dir,str(uuid.uuid4()))
        _logger.debug("putEventPickingRequest : %s -> %s" % (userName,evpFileName))
        # write
        fo = open(evpFileName,'wb')
        fo.write("userName=%s\n" % userName)
        fo.write("creationTime=%s\n" % creationTime)
        fo.write("eventPickDataType=%s\n" % eventPickDataType)
        fo.write("eventPickStreamName=%s\n" % eventPickStreamName)
        fo.write("eventPickDS=%s\n" % eventPickDS)
        fo.write("eventPickAmiTag=%s\n" % eventPickAmiTag)
        fo.write("userDatasetName=%s\n" % userDatasetName)
        fo.write("lockedBy=%s\n" % lockedBy)
        fo.write("params=%s\n" % params)
        for tmpLine in runEventList.split('\n'):
            tmpItems = tmpLine.split()
            if len(tmpItems) != 2:
                continue
            fo.write("runEvent=%s,%s\n" % tuple(tmpItems))
        fo.close()
    except:
        errType,errValue = sys.exc_info()[:2]
        errStr = "cannot put request due to %s %s" % (errType,errValue) 
        _logger.error("putEventPickingRequest : " + errStr + " " + userName)
        return "ERROR : " + errStr
    _logger.debug("putEventPickingRequest : %s end" % userName)
    return True


# delete file 
def deleteFile(req,file):
    if not Protocol.isSecure(req):
        return 'False'
    try:
        # may be reused for rebrokreage 
        #os.remove('%s/%s' % (panda_config.cache_dir,file.split('/')[-1]))
        return 'True'
    except:
        return 'False'        


# touch file 
def touchFile(req,filename):
    if not Protocol.isSecure(req):
        return 'False'
    try:
        os.utime('%s/%s' % (panda_config.cache_dir,filename.split('/')[-1]),None)
        return 'True'
    except:
        errtype,errvalue = sys.exc_info()[:2]
        _logger.error("touchFile : %s %s" % (errtype,errvalue))
        return 'False'        
                        

# get server name:port for SSL
def getServer(req):
    return "%s:%s" % (panda_config.pserverhost,panda_config.pserverport)

 
# update stdout
def updateLog(req,file):
    _logger.debug("updateLog : %s start" % file.filename)
    # write to file
    try:
        # expand
        extStr = zlib.decompress(file.file.read())
        # stdout name
        logName  = '%s/%s' % (panda_config.cache_dir,file.filename.split('/')[-1])
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
    # put dummy char to avoid Internal Server Error
    retStr = ' '
    try:
        # stdout name
        fullLogName  = '%s/%s' % (panda_config.cache_dir,logName.split('/')[-1])
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


# get VOMS attributes
def getVomsAttr(req):
    vomsAttrs = []
    for tmpKey,tmpVal in req.subprocess_env.iteritems():
        # compact credentials
        if tmpKey.startswith('GRST_CRED_'):
            vomsAttrs.append('%s : %s\n' % (tmpKey,tmpVal))
    vomsAttrs.sort()
    retStr = ''
    for tmpStr in vomsAttrs:
        retStr += tmpStr
    return retStr
