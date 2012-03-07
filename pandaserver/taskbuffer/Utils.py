"""
utility service

"""
import os
import re
import sys
import zlib
import uuid
import socket
import struct
import datetime
import jobdispatcher.Protocol as Protocol
import ErrorCode
from config import panda_config

from pandalogger.PandaLogger import PandaLogger

# logger
_logger = PandaLogger().getLogger('Utils')

# check if server is alive
def isAlive(req):
    return "alive=yes"


# touch in Cassandra
def touchFileCassa(filefamily,fileKeyName,timeNow):
    try:
        # get old timestamp
        oldFileInfo = filefamily.get(fileKeyName)
    except:
        _logger.warning('cannot get old fileinfo for %s from Cassandra' % fileKeyName)
        return False
    try:
        # update time in fileTable
        for splitIdx in range(oldFileInfo['nSplit']):
            tmpFileKeyName = fileKeyName
            if splitIdx != 0:
                tmpFileKeyName += '_%s' % splitIdx
            filefamily.insert(tmpFileKeyName,{'year'   : timeNow.year,
                                              'month'  : timeNow.month,
                                              'day'    : timeNow.day,
                                              'hour'   : timeNow.hour,
                                              'minute' : timeNow.minute,
                                              'second' : timeNow.second})
        return True
    except:
        errType,errValue = sys.exc_info()[:2]
        errStr = "cannot touch %s due to %s %s" % (fileKeyName,errType,errValue) 
        _logger.error(errStr)
        return False
    

# upload file 
def putFile(req,file):
    if not Protocol.isSecure(req):
        return False
    if '/CN=limited proxy' in req.subprocess_env['SSL_CLIENT_S_DN']:
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
    try:
        fileFullPath = '%s/%s' % (panda_config.cache_dir,file.filename.split('/')[-1])
        # avoid overwriting
        if os.path.exists(fileFullPath):
            # touch
            os.utime(fileFullPath,None)
            # send error message
            errStr = "ERROR : Cannot overwrite file"
            _logger.error(errStr)
            _logger.debug("putFile : end")
            return errStr
        # write
        fo = open(fileFullPath,'wb')
        fileContent = file.file.read()
        fo.write(fileContent)
        fo.close()
    except:
        errStr = "ERROR : Cannot write file"
        _logger.error(errStr)
        _logger.debug("putFile : end")
        return errStr
    # store to cassandra
    if hasattr(panda_config,'cacheUseCassandra') and panda_config.cacheUseCassandra == True:
        try:
            # time-stamp
            timeNow = datetime.datetime.utcnow()
            creationTime = timeNow.strftime('%Y-%m-%d %H:%M:%S')
            # checksum
            try:
                # decode Footer
                footer = fileContent[-8:]
                checkSum,isize = struct.unpack("II",footer)
                _logger.debug("CRC from gzip Footer %s" % checkSum)
            except:
                # calculate on the fly
                import zlib
                checkSum = zlib.adler32(fileContent) & 0xFFFFFFFF
                _logger.debug("CRC calculated %s" % checkSum)                
            # user name
            username = req.subprocess_env['SSL_CLIENT_S_DN']
            username = username.replace('/CN=proxy','')
            username = username.replace('/CN=limited proxy','')
            # file size
            fileSize = len(fileContent)
            # key
            fileKeyName = file.filename.split('/')[-1]
            sizeCheckSum = '%s:%s' % (fileSize,checkSum)
            # insert to cassandra
            import pycassa
            pool = pycassa.ConnectionPool(panda_config.cacheKeySpace)
            filefamily = pycassa.ColumnFamily(pool,panda_config.cacheFileTable)
            # avoid overwriting
            gotoNextCassa = True
            if filefamily.get_count(fileKeyName) > 0:
                # touch
                touchFlag = touchFileCassa(filefamily,fileKeyName,timeNow)
                if touchFlag:
                    gotoNextCassa = False
                    # send error message
                    errStr = "ERROR : Cannot overwrite file in Cassandra"
                    _logger.error(errStr)
                    if not panda_config.cacheIgnoreCassandraError:
                        _logger.debug("putFile : end")
                        return errStr
            # check uniqueness with size and checksum
            if gotoNextCassa:
                try:
                    uniqExp = pycassa.index.create_index_expression('uniqID',sizeCheckSum)
                    userExp = pycassa.index.create_index_expression('user',username)
                    tmpClause = pycassa.index.create_index_clause([uniqExp,userExp])
                    tmpResults = filefamily.get_indexed_slices(tmpClause,columns=['creationTime'])
                    for oldFileKeyName,tmpDict in tmpResults:
                        _logger.debug('The same size and chksum %s found in old:%s and new:%s' % \
                                      (sizeCheckSum,oldFileKeyName,fileKeyName))
                        # touch
                        touchFlag = touchFileCassa(filefamily,oldFileKeyName,timeNow)
                        if touchFlag:
                            # make alias
                            _logger.debug('Making alias %s->%s' % (fileKeyName,oldFileKeyName))
                            filefamily.insert(fileKeyName,{'alias':oldFileKeyName,
                                                           'creationTime':creationTime,
                                                           'nSplit':0,
                                                           }
                                              )
                            # set time
                            touchFileCassa(filefamily,fileKeyName,timeNow)
                            _logger.debug("putFile : end")
                            return True
                except:
                    gotoNextCassa = False
                    errType,errValue = sys.exc_info()[:2]
                    errStr = "cannot make alias for %s due to %s %s" % (fileKeyName,errType,errValue)
                    _logger.error(errStr)
                    if not panda_config.cacheIgnoreCassandraError:
                        _logger.debug("putFile : end")
                        return errStr
            # insert new record
            if gotoNextCassa:
                splitIdx = 0
                splitSize = 5 * 1024 * 1024
                nSplit,tmpMod = divmod(len(fileContent),splitSize)
                if tmpMod != 0:
                    nSplit += 1
                _logger.debug('Inserting %s with %s blocks' % (fileKeyName,nSplit))                    
                for splitIdx in range(nSplit): 
                    # split to small chunks since cassandra is not good at large files
                    tmpFileContent = fileContent[splitSize*splitIdx:splitSize*(splitIdx+1)]
                    tmpFileKeyName = fileKeyName
                    tmpAttMap = {'file':tmpFileContent,
                                 'user':username,
                                 'creationTime':creationTime,
                                 }
                    if splitIdx == 0:
                        tmpAttMap['size']     = fileSize
                        tmpAttMap['nSplit']   = nSplit
                        tmpAttMap['uniqID']   = sizeCheckSum
                        tmpAttMap['checkSum'] = str(checkSum)
                    else:
                        tmpFileKeyName += '_%s' % splitIdx
                        tmpAttMap['size']   = 0
                        tmpAttMap['nSplit'] = 0
                    filefamily.insert(tmpFileKeyName,tmpAttMap)
                # set time
                touchFileCassa(filefamily,fileKeyName,timeNow)
        except:
            errType,errValue = sys.exc_info()[:2]
            errStr = "cannot put %s into Cassandra due to %s %s" % (fileKeyName,errType,errValue)
            _logger.error(errStr)
            # send error message
            errStr = "ERROR : " + errStr
            if not panda_config.cacheIgnoreCassandraError:
                _logger.debug("putFile : end")
                return errStr
    _logger.debug("putFile : %s end" % file.filename)
    return True


# get file
def getFile(req,fileName):
    _logger.debug("getFile : %s start" % fileName)
    try:
        # look into cassandra
        import pycassa
        pool = pycassa.ConnectionPool(panda_config.cacheKeySpace)
        filefamily = pycassa.ColumnFamily(pool,panda_config.cacheFileTable)
        fileInfo = filefamily.get(fileName)
        # check alias
        if fileInfo.has_key('alias') and fileInfo['alias'] != '':
            realFileName = fileInfo['alias']
            fileInfo = filefamily.get(realFileName)
            _logger.debug("getFile : %s use alias=%s" % (fileName,realFileName))
        else:
            realFileName = fileName
        # check cached file
        hostKey = socket.gethostname() + '_cache'
        if fileInfo.has_key(hostKey) and fileInfo[hostKey] != '':
            _logger.debug("getFile : %s found cache=%s" % (fileName,fileInfo[hostKey]))
            try:
                fileFullPath = '%s%s' % (panda_config.cache_dir,fileInfo[hostKey])
                # touch
                os.utime(fileFullPath,None)
                _logger.debug("getFile : %s end" % fileName)                
                # return
                return ErrorCode.EC_Redirect('/cache%s' % fileInfo[hostKey])
            except:
                errtype,errvalue = sys.exc_info()[:2]
                _logger.debug("getFile : %s failed to touch %s due to %s:%s" % (fileName,fileFullPath,errtype,errvalue))
        # write to cache file
        fileRelPath  = '/cassacache/%s' % str(uuid.uuid4())
        fileFullPath = '%s%s' % (panda_config.cache_dir,fileRelPath)
        _logger.debug("getFile : %s write cache to %s" % (fileName,fileFullPath))
        fo = open(fileFullPath,'wb')
        fo.write(fileInfo['file'])
        if fileInfo['nSplit'] > 1:
            for splitIdx in range(fileInfo['nSplit']):
                if splitIdx == 0:
                    continue
                fileInfo = filefamily.get(realFileName+'_%s' % splitIdx)
                fo.write(fileInfo['file'])
        fo.close()
        # set cache name in DB
        filefamily.insert(realFileName,{hostKey:fileRelPath})
        _logger.debug("getFile : %s end" % fileName)
        # return
        return ErrorCode.EC_Redirect('/cache%s' % fileRelPath)
    except pycassa.NotFoundException:
        _logger.error("getFile : %s not found" % fileName)
        return ErrorCode.EC_NotFound
    except:
        errtype,errvalue = sys.exc_info()[:2]
        errStr = "getFile : %s %s for %s" % (errtype,errvalue,fileName)
        _logger.error(errStr)
        raise RuntimeError,errStr


# get event picking request
def putEventPickingRequest(req,runEventList='',eventPickDataType='',eventPickStreamName='',
                           eventPickDS='',eventPickAmiTag='',userDatasetName='',lockedBy='',
                           params='',inputFileList=''):
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
        fo.write("inputFileList=%s\n" % inputFileList)
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


# get all attributes
def getAttr(req):
    allAttrs = []
    for tmpKey,tmpVal in req.subprocess_env.iteritems():
            allAttrs.append('%s : %s\n' % (tmpKey,tmpVal))
    allAttrs.sort()
    retStr = ''
    for tmpStr in allAttrs:
        retStr += tmpStr
    return retStr
