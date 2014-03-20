import re
import urllib
import time
import sys
import types
import commands
import xml.dom.minidom


from config import panda_config
from pandalogger.PandaLogger import PandaLogger
_log = PandaLogger().getLogger('broker_util')

# curl class
class _Curl:
    # constructor
    def __init__(self,useProxy=False):
        # path to curl
        self.path = 'curl --user-agent "dqcurl" -m 180'
        # verification of the host certificate
        self.verifyHost = False
        # use proxy
        if useProxy and panda_config.httpProxy != '':
            self.path = 'env http_proxy=%s %s' % (panda_config.httpProxy,self.path)
        
    # GET method
    def get(self,url,data={}):
        # make command
        com = '%s --silent --get' % self.path
        if not self.verifyHost:
            com += ' --insecure'
        # data
        for key,value in data.iteritems():
            com += ' --data "%s"' % urllib.urlencode({key:value})
        com += ' %s' % url
        # execute
        _log.debug(com)
        ret = commands.getstatusoutput(com)
        _log.debug(ret)
        return ret


# get default storage
def _getDefaultStorage(baseURL,sePath=None,seProdPath={}):
    _log.debug('_getDefaultStorage (%s %s %s)' % (baseURL,sePath,seProdPath))
    # use se+seprodpath when baseURL=''
    if baseURL=='':
        # get token
        match = re.search('^token:([^:]+):',sePath)
        if match == None:
            _log.error("could not get token from %s" % sePath)
            return ""
        token = match.group(1)
        # get corresponding path
        if not seProdPath.has_key(token):
            _log.error("could not find path for % in %s" % (token,seProdPath))
            return ""
        # set se+seprodpath
        out = sePath+seProdPath[token]
        # append /
        if not out.endswith('/'):
            out += '/'
        _log.debug(out)    
    else:
        # check port to set proxy
        useProxy = False
        if panda_config.httpProxy != '':
            pMatch = re.search('http://[^:/]+:*(\d+)/',baseURL)
            if pMatch == None:
                # default port
                useProxy = True
            elif pMatch.group(1) == '80':
                # standard port
                useProxy = True        
        # instantiate curl
        curl = _Curl(useProxy)
        # get default storage
        url = baseURL + 'storages/default'
        status,out = curl.get(url)
        _log.debug(out)    
        if status != 0:
            _log.error("could not get default storage from %s:%s" % (baseURL,status))
            return ""
    # parse
    match = re.search('^[^/]+://[^/]+(/.+)$',out)
    if match == None:
        _log.error("could not parse string : %s" % out)
        return ""
    return match.group(1)
                                                                                        

# get PoolFileCatalog
def _getPoolFileCatalog(lfns,dq2url):
    _log.debug('_getPoolFileCatalog')    
    # check port to set proxy
    useProxy = False
    if panda_config.httpProxy != '':
        pMatch = re.search('http://[^:/]+:*(\d+)/',dq2url)
        if pMatch == None:
            # default port
            useProxy = True
        elif pMatch.group(1) == '80':
            # standard port
            useProxy = True        
    # instantiate curl
    curl = _Curl(useProxy)
    # get PoolFileCatalog
    iLFN = 0
    outXML =''    
    strLFNs = ''
    if not dq2url.endswith('_'):
        url = dq2url + '/lrc/PoolFileCatalog'
    else:
        # NDGF LRC
        url = dq2url + 'lrc/PoolFileCatalog'        
    for lfn in lfns:
        iLFN += 1
        # make argument
        strLFNs += '%s ' % lfn
        if iLFN % 40 == 0 or iLFN == len(lfns):
            # get PoolFileCatalog
            strLFNs = strLFNs.rstrip()
            data = {'lfns':strLFNs}
            # avoid too long argument
            strLFNs = ''
            # execute
            status,out = curl.get(url,data)
            _log.debug(status)            
            # sleep
            time.sleep(2)
            if status != 0:
                _log.error("_getPoolFileCatalog : %s %s %s" % (dq2url,status,out))
                return status
            if status != 0 or out.startswith('Error'):
                continue
            if not out.startswith('<?xml'):
                continue
            # append
            outXML += out
    # remove redundant trailer and header
    th = \
"""
</POOLFILECATALOG><\?xml version="1.0" encoding="UTF-8" standalone="no" \?>
<!-- Edited By POOL -->
<!DOCTYPE POOLFILECATALOG SYSTEM "InMemory">
<POOLFILECATALOG>
"""
    outXML = re.sub(th,'',outXML)
    outXML = re.sub("""\s*<META name="fsize" type="string"/>""",'',outXML)
    outXML = re.sub("""\s*<META name="md5sum" type="string"/>""",'',outXML)
    outXML = re.sub("""\s*<META name="lastmodified" type="string"/>""",'',outXML)
    outXML = re.sub("""\s*<META name="archival" type="string"/>""",'',outXML)
    outXML = re.sub("""\s*<META name="permanent" type="string"/>""",'',outXML)
    outXML = re.sub("""\s*<META name="adler32" type="string"/>""",'',outXML)

    # return XML
    return outXML


# get files from MySQL
def _getPFNFromMySQL(lfns,dq2url):
    _log.debug('_getPFNFromMySQL')
    import MySQLdb
    comment = ' /* broker_util._getPFNFromMySQL */'
    outStr = ''
    # parse connection string
    match = re.search('^mysql://([^:]+):([^@]+)@([^/:]+):(\d+)/(.+)$',dq2url)
    if match == None:
        return outStr
    # parameters for DB connection
    connStr = "mysql -h %s -u %s -p%s -P %s %s"
    dbhost = match.group(3)
    dbuser = match.group(1)
    dbpswd = match.group(2)
    dbport = int(match.group(4))
    dbname = match.group(5)
    connStr = "mysql -h %s -u %s -p%s -P %s %s" % (dbhost,dbuser,dbpswd,dbport,dbname)
    try:
        _log.debug(connStr)
        # connect
        dbConn = MySQLdb.connect(db=dbname,host=dbhost,port=dbport,user=dbuser,passwd=dbpswd)
        # make cursor
        dbCur = dbConn.cursor()
        # query files
        iLFN = 0
        strLFNs = ''    
        for lfn in lfns:
            iLFN += 1
            # make argument
            strLFNs += " lfname='%s' OR " % lfn
            if iLFN % 40 == 0 or iLFN == len(lfns):
                # get PoolFileCatalog
                strLFNs = strLFNs[:-3]
                # construct SQL
                sql = 'SELECT lfname FROM t_lfn WHERE %s' % strLFNs
                # reset
                strLFNs = ''
                # execute
                _log.debug(sql)
                dbCur.execute(sql+comment)
                res = dbCur.fetchall()
                _log.debug(res)
                # append LFNs
                if res != None and len(res) != 0:
                    for resLFN in res:
                        outStr += '%s ' % resLFN
        # close cursor
        dbCur.close()
        # close connection
        dbConn.close()
    except:
        type, value, traceBack = sys.exc_info()
        _log.error("_getPFNFromMySQL : %s %s %s" % (dq2url,type,value))
        return -1
    # return
    return outStr


# get files from LFC
def _getPFNFromLFC(lfns,dq2url,guids,storageName,scopeList=[]):
    _log.debug('_getPFNFromLFC')
    outStr = ''
    # check paramter
    if guids == [] or storageName == [] or (len(lfns) != len(guids)):
        return outStr
    # loop over all LFNs
    iLFN = 0
    nLFN = 1000
    strFiles = ''    
    outStr = ''
    for iLFN in range(len(lfns)):
        if scopeList != []:
            strFiles  += '%s %s %s\n' % (lfns[iLFN],guids[iLFN],scopeList[iLFN]) 
        else:
            strFiles  += '%s %s\n' % (lfns[iLFN],guids[iLFN]) 
        # bulk operation
        if (iLFN+1) % nLFN == 0 or (iLFN+1) >= len(lfns):
            # write to file
            inFileName = '%s/lfcin.%s'  % (panda_config.logdir,commands.getoutput('uuidgen'))
            ifile = open(inFileName,'w')
            ifile.write(strFiles)
            ifile.close()
            # construct commands
            strStorage = ''
            for storage in storageName:
                strStorage += '%s,' % storage
            strStorage = strStorage[:-1]
            com = 'cd %s > /dev/null 2>&1; export HOME=%s; ' % (panda_config.home_dir_cwd,panda_config.home_dir_cwd)            
            com+= 'unset LD_LIBRARY_PATH; unset PYTHONPATH; export PATH=/usr/local/bin:/bin:/usr/bin; '
            com+= 'source %s; %s/python -Wignore %s/LFCclient.py -f %s -l %s -s %s' % \
                  (panda_config.glite_source,panda_config.native_python32,panda_config.lfcClient_dir,
                   inFileName,dq2url,strStorage)
            _log.debug(com)
            # exeute
            status,output = commands.getstatusoutput(com)
            _log.debug(status)
            if status == 0:
                outStr += output
            else:
                _log.error("_getPFNFromLFC : %s %s %s" % (dq2url,status,output))
                # send message to logger
                try:
                    # make message
                    message = 'LFC access : %s %s %s' % (dq2url,status,output)
                    # get logger
                    _pandaLogger = PandaLogger()
                    _pandaLogger.lock()
                    _pandaLogger.setParams({'Type':'broker_util'})
                    logger = _pandaLogger.getHttpLogger(panda_config.loggername)
                    # add message
                    logger.error(message)
                    # release HTTP handler
                    _pandaLogger.release()
                except:
                    pass
                return status
            # reset
            strFiles = ''
    # return
    return outStr
                            

# get files from LRC
def getFilesFromLRC(files,url,guids=[],storageName=[],terminateWhenFailed=False,getPFN=False,
                    scopeList=[]):
    _log.debug('getFilesFromLRC "%s" %s' % (url,str(storageName)))    
    # get PFC
    outSTR = ''
    if url.startswith('mysql://'):
        # from MySQL
        outSTR = _getPFNFromMySQL(files,url)
        # get PFN
        if getPFN:
            outPFN = {}
            # FIXME
            _log.debug('RetPFN:%s ' % str(outPFN))            
            return outPFN
    elif url.startswith('http://'):
        # from HTTP I/F
        outSTR = _getPoolFileCatalog(files,url)
        # get PFN
        if getPFN:
            outPFN = {}
            try:
                if not outSTR in ['',None]:
                    root  = xml.dom.minidom.parseString(outSTR)
                    fileNodes = root.getElementsByTagName('File')
                    for file in fileNodes:
                        # get PFN and LFN nodes
                        physical = file.getElementsByTagName('physical')[0]
                        pfnNode  = physical.getElementsByTagName('pfn')[0]
                        logical  = file.getElementsByTagName('logical')[0]
                        lfnNode  = logical.getElementsByTagName('lfn')[0]
                        # convert UTF8 to Raw
                        pfn = str(pfnNode.getAttribute('name'))
                        lfn = str(lfnNode.getAttribute('name'))
                        # assign
                        if not outPFN.has_key(lfn):
                            outPFN[lfn] = []
                        outPFN[lfn].append(pfn)
            except:
                type, value, traceBack = sys.exc_info()
                _log.error(outSTR)
                _log.error("could not parse XML - %s %s" % (type, value))
            _log.debug('RetPFN:%s ' % str(outPFN))                
            return outPFN
    elif url.startswith('lfc://') or url.startswith('rucio://'):
        # from LFC
        outSTR = _getPFNFromLFC(files,url,guids,storageName,scopeList=scopeList)
        # get PFN
        if getPFN:
            outPFN = {}
            try:
                if not outSTR in ['',None]:
                    tmpItems = outSTR.split('LFCRet :')
                    tmpItems.remove('')
                    # loop over all returns
                    for tmpItem in tmpItems:
                        exec "tmpLFNmap = %s" % tmpItem
                        for tmpLFN,tmpPFN in tmpLFNmap.iteritems():
                            outPFN[tmpLFN] = tmpPFN
            except:
                type, value, traceBack = sys.exc_info()
                _log.error(outSTR)
                _log.error("could not parse LFC ret - %s %s" % (type, value))
            _log.debug('RetPFN:%s ' % str(outPFN))
            return outPFN
    # check return
    if not isinstance(outSTR,types.StringType):
        if terminateWhenFailed:
            return None
        # set empty string
        outSTR = ''
    # collect OK Files
    okFiles = []
    for file in files:
        if re.search(file,outSTR) != None:
            okFiles.append(file)
    _log.debug('Ret:%s ' % str(okFiles))
    return okFiles


# get # of files from LRC
def getNFilesFromLRC(files,url):
    _log.debug('getNFilesFromLRC')        
    # get okFiles
    okFiles = getFilesFromLRC(files,url)
    nFiles = len(okFiles)
    _log.debug('Ret:%s ' % nFiles)
    return nFiles        

                                            
# get list of missing LFNs from LRC
def getMissLFNsFromLRC(files,url,guids=[],storageName=[],scopeList=[]):
    _log.debug('getMissLFNsFromLRC')    
    # get OF files
    okFiles = getFilesFromLRC(files,url,guids,storageName,scopeList=scopeList)
    # collect missing files
    missFiles = []
    for file in files:
        if not file in okFiles:
            missFiles.append(file)
    _log.debug('Ret:%s ' % str(missFiles))
    return missFiles


# extract list of se hosts from schedconfig                
def getSEfromSched(seStr):
    tmpSE = []
    if seStr != None:
        for tmpSrcSiteSE in seStr.split(','):
            # extract host
            match = re.search('.+://([^:/]+):*\d*/*',tmpSrcSiteSE)
            if match != None:
                tmpSE.append(match.group(1))
    # sort
    tmpSE.sort()
    # return
    return tmpSE
    
    
