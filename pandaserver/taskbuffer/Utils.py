"""
utility service

"""
import os
import re
import sys
import zlib
import uuid
import time
import socket
import struct
import datetime
import pandaserver.jobdispatcher.Protocol as Protocol
from pandaserver.taskbuffer import ErrorCode
from pandaserver.userinterface import Client
from pandaserver.config import panda_config

from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandalogger.LogWrapper import LogWrapper

try:
    long
except NameError:
    long = int

# logger
_logger = PandaLogger().getLogger('Utils')

# check if server is alive
def isAlive(req):
    return "alive=yes"


# extract name from DN
def cleanUserID(id):
    try:
        up = re.compile('/(DC|O|OU|C|L)=[^\/]+')
        username = up.sub('', id)
        up2 = re.compile('/CN=[0-9]+')
        username = up2.sub('', username)
        up3 = re.compile(' [0-9]+')
        username = up3.sub('', username)
        up4 = re.compile('_[0-9]+')
        username = up4.sub('', username)
        username = username.replace('/CN=proxy','')
        username = username.replace('/CN=limited proxy','')
        username = username.replace('limited proxy','')
        username = re.sub('/CN=Robot:[^/]+','',username)
        pat = re.compile('.*/CN=([^\/]+)/CN=([^\/]+)')
        mat = pat.match(username)
        if mat:
            username = mat.group(2)
        else:
            username = username.replace('/CN=','')
        if username.lower().find('/email') > 0:
            username = username[:username.lower().find('/email')]
        pat = re.compile('.*(limited.*proxy).*')
        mat = pat.match(username)
        if mat:
            username = mat.group(1)
        username = username.replace('(','')
        username = username.replace(')','')
        username = username.replace("'",'')
        return username
    except Exception:
        return id


# insert with rety
def insertWithRetryCassa(familyName,keyName,valMap,msgStr,nTry=3):
    for iTry in range(nTry):
        try:    
            familyName.insert(keyName,valMap)
        except pycassa.MaximumRetryException(tmpE):
            if iTry+1 < nTry:
                _logger.debug("%s sleep %s/%s" % (msgStr,iTry,nTry))
                time.sleep(30)
            else:
                raise pycassa.MaximumRetryException(tmpE.value)
        else:
            break
    

# touch in Cassandra
def touchFileCassa(filefamily,fileKeyName,timeNow):
    try:
        # get old timestamp
        oldFileInfo = filefamily.get(fileKeyName)
    except Exception:
        _logger.warning('cannot get old fileinfo for %s from Cassandra' % fileKeyName)
        return False
    try:
        # update time in fileTable
        for splitIdx in range(oldFileInfo['nSplit']):
            tmpFileKeyName = fileKeyName
            if splitIdx != 0:
                tmpFileKeyName += '_%s' % splitIdx
            insertWithRetryCassa(filefamily,tmpFileKeyName,
                                 {'year'   : timeNow.year,
                                  'month'  : timeNow.month,
                                  'day'    : timeNow.day,
                                  'hour'   : timeNow.hour,
                                  'minute' : timeNow.minute,
                                  'second' : timeNow.second},
                                 'touchFileCassa : %s' % fileKeyName
                                 )
        return True
    except Exception:
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
        sizeLimit = 100*1024*1024
    else:
        noBuild = False
        sizeLimit = fullSizeLimit
    # get file size
    contentLength = 0
    try:
        contentLength = long(req.headers_in["content-length"])
    except Exception:
        if "content-length" in req.headers_in:
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
            _logger.debug('putFile : cannot overwrite file %s' % file.filename)  
            _logger.debug("putFile : end")
            return errStr
        # write
        fo = open(fileFullPath,'wb')
        fileContent = file.file.read()
        fo.write(fileContent)
        fo.close()
    except Exception:
        errStr = "ERROR : Cannot write file"
        _logger.error(errStr)
        _logger.debug("putFile : end")
        return errStr
    # checksum
    try:
        # decode Footer
        footer = fileContent[-8:]
        checkSum,isize = struct.unpack("II",footer)
        _logger.debug("CRC from gzip Footer %s" % checkSum)
    except Exception:
        # calculate on the fly
        """
        import zlib
        checkSum = zlib.adler32(fileContent) & 0xFFFFFFFF
        """
        # use None to avoid delay for now
        checkSum = None
        _logger.debug("CRC calculated %s" % checkSum)
    # file size
    fileSize = len(fileContent)
    # user name
    username = cleanUserID(req.subprocess_env['SSL_CLIENT_S_DN'])    
    _logger.debug("putFile : written dn=%s file=%s size=%s crc=%s" % \
                  (username,file.filename,fileSize,checkSum))
    # put file info to DB
    statClient,outClient = Client.insertSandboxFileInfo(username,file.filename,
                                                        fileSize,checkSum)
    if statClient != 0 or outClient.startswith("ERROR"):
        _logger.error("putFile : failed to put sandbox to DB with %s %s" % (statClient,outClient))
        #_logger.debug("putFile : end")
        #return "ERROR : Cannot insert sandbox to DB"
    else:
        _logger.debug("putFile : inserted sandbox to DB with %s" % outClient)
    # store to cassandra
    if hasattr(panda_config,'cacheUseCassandra') and panda_config.cacheUseCassandra == True:
        try:
            # time-stamp
            timeNow = datetime.datetime.utcnow()
            creationTime = timeNow.strftime('%Y-%m-%d %H:%M:%S')
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
                            insertWithRetryCassa(filefamily,fileKeyName,
                                                 {'alias':oldFileKeyName,
                                                  'creationTime':creationTime,
                                                  'nSplit':0,
                                                  },
                                                 'putFile : make alias for %s' % file.filename 
                                                 )
                            # set time
                            touchFileCassa(filefamily,fileKeyName,timeNow)
                            _logger.debug("putFile : end")
                            return True
                except Exception:
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
                    # insert with retry
                    insertWithRetryCassa(filefamily,tmpFileKeyName,tmpAttMap,
                                         'putFile : insert %s' % file.filename)
                # set time
                touchFileCassa(filefamily,fileKeyName,timeNow)
        except Exception:
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
        if 'alias' in fileInfo and fileInfo['alias'] != '':
            realFileName = fileInfo['alias']
            fileInfo = filefamily.get(realFileName)
            _logger.debug("getFile : %s use alias=%s" % (fileName,realFileName))
        else:
            realFileName = fileName
        # check cached file
        hostKey = socket.gethostname() + '_cache'
        if hostKey in fileInfo and fileInfo[hostKey] != '':
            _logger.debug("getFile : %s found cache=%s" % (fileName,fileInfo[hostKey]))
            try:
                fileFullPath = '%s%s' % (panda_config.cache_dir,fileInfo[hostKey])
                # touch
                os.utime(fileFullPath,None)
                _logger.debug("getFile : %s end" % fileName)                
                # return
                return ErrorCode.EC_Redirect('/cache%s' % fileInfo[hostKey])
            except Exception:
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
        insertWithRetryCassa(filefamily,realFileName,{hostKey:fileRelPath},
                             'getFile : set cache for %s' % fileName)
        _logger.debug("getFile : %s end" % fileName)
        # return
        return ErrorCode.EC_Redirect('/cache%s' % fileRelPath)
    except pycassa.NotFoundException:
        _logger.error("getFile : %s not found" % fileName)
        return ErrorCode.EC_NotFound
    except Exception:
        errtype,errvalue = sys.exc_info()[:2]
        errStr = "getFile : %s %s for %s" % (errtype,errvalue,fileName)
        _logger.error(errStr)
        raise RuntimeError(errStr)


# get event picking request
def putEventPickingRequest(req,runEventList='',eventPickDataType='',eventPickStreamName='',
                           eventPickDS='',eventPickAmiTag='',userDatasetName='',lockedBy='',
                           params='',inputFileList='',eventPickNumSites='',userTaskName='',
                           ei_api='',giveGUID=None):
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
    except Exception:
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
    if giveGUID == 'True':
        giveGUID = True
    else:
        giveGUID = False
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
        fo.write("eventPickNumSites=%s\n" % eventPickNumSites)
        fo.write("userTaskName=%s\n" % userTaskName)
        fo.write("userDatasetName=%s\n" % userDatasetName)
        fo.write("lockedBy=%s\n" % lockedBy)
        fo.write("params=%s\n" % params)
        fo.write("inputFileList=%s\n" % inputFileList)
        fo.write("ei_api=%s\n" % ei_api)
        runEvtGuidMap = {}
        for tmpLine in runEventList.split('\n'):
            tmpItems = tmpLine.split()
            if (len(tmpItems) != 2 and not giveGUID) or \
                    (len(tmpItems) != 3 and giveGUID):
                continue
            fo.write("runEvent=%s,%s\n" % tuple(tmpItems[:2]))
            if giveGUID:
                runEvtGuidMap[tuple(tmpItems[:2])] = [tmpItems[2]]
        fo.write("runEvtGuidMap=%s\n" % str(runEvtGuidMap))
        fo.close()
    except Exception:
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
    except Exception:
        return 'False'        


# touch file 
def touchFile(req,filename):
    if not Protocol.isSecure(req):
        return 'False'
    try:
        os.utime('%s/%s' % (panda_config.cache_dir,filename.split('/')[-1]),None)
        return 'True'
    except Exception:
        errtype,errvalue = sys.exc_info()[:2]
        _logger.error("touchFile : %s %s" % (errtype,errvalue))
        return 'False'        
                        

# get server name:port for SSL
def getServer(req):
    return "%s:%s" % (panda_config.pserverhost,panda_config.pserverport)

# get server name:port for HTTP
def getServerHTTP(req):
    return "%s:%s" % (panda_config.pserverhosthttp,panda_config.pserverporthttp)

 
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
        ft = open(logName,'a')
        ft.write(extStr)
        ft.close()
    except Exception:
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
    except Exception:
        type, value, traceBack = sys.exc_info()
        _logger.error("fetchLog : %s %s" % (type,value))
    _logger.debug("fetchLog : %s end read=%s" % (logName,len(retStr)))
    return retStr


# get VOMS attributes
def getVomsAttr(req):
    vomsAttrs = []
    for tmpKey in req.subprocess_env:
        tmpVal = req.subprocess_env[tmpKey]
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
    for tmpKey in req.subprocess_env:
        tmpVal = req.subprocess_env[tmpKey]
        allAttrs.append('%s : %s\n' % (tmpKey,tmpVal))
    allAttrs.sort()
    retStr = ''
    for tmpStr in allAttrs:
        retStr += tmpStr
    return retStr


# upload log
def uploadLog(req,file):
    if not Protocol.isSecure(req):
        return False
    if '/CN=limited proxy' in req.subprocess_env['SSL_CLIENT_S_DN']:
        return False
    tmpLog = LogWrapper(_logger,'uploadLog <{0}>'.format(file.filename))
    tmpLog.debug("start {0}".format(req.subprocess_env['SSL_CLIENT_S_DN']))
    # size check
    sizeLimit = 100*1024*1024
    # get file size
    contentLength = 0
    try:
        contentLength = long(req.headers_in["content-length"])
    except Exception:
        if "content-length" in req.headers_in:
            tmpLog.error("cannot get CL : %s" % req.headers_in["content-length"])
        else:
            tmpLog.error("no CL")
    tmpLog.debug("size %s" % contentLength)
    if contentLength > sizeLimit:
        errStr = "failed to upload log due to size limit"
        tmpLog.error(errStr)
        tmpLog.debug("end")            
        return errStr
    jediLogDir = '/jedilog'
    retStr = ''
    try:
        fileBaseName = file.filename.split('/')[-1]
        fileFullPath = '{0}{1}/{2}'.format(panda_config.cache_dir,jediLogDir,fileBaseName)
        # delete old file 
        if os.path.exists(fileFullPath):
            os.remove(fileFullPath)
        # write
        fo = open(fileFullPath,'wb')
        fileContent = file.file.read()
        fo.write(fileContent)
        fo.close()
        tmpLog.debug("written to {0}".format(fileFullPath))
        retStr = 'http://{0}/cache{1}/{2}'.format(getServerHTTP(None),jediLogDir,fileBaseName) 
    except Exception:
        errtype,errvalue = sys.exc_info()[:2]
        errStr = "failed to write log with {0}:{1}".format(errtype.__name__,errvalue)
        tmpLog.error(errStr)
        tmpLog.debug("end")
        return errStr
    tmpLog.debug("end")
    return retStr


# partitions input into shards of a given size for bulk operations in the DB
def create_shards(input_list, size):
    """
    Creates shards of size n from the input list.
    @author: Miguel Branco in DQ2 Site Services code
    """
    shard, i = [], 0
    for element in input_list:
        shard.append(element)
        i += 1
        if i == size:
            yield shard
            shard, i = [], 0

    if i > 0:
        yield shard
