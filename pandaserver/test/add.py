import os
import re
import sys
import time
import random
import datetime
import commands
from taskbuffer.DBProxy import DBProxy
from taskbuffer.TaskBuffer import taskBuffer
from pandalogger.PandaLogger import PandaLogger
from dataservice.Adder import Adder
from brokerage.SiteMapper import SiteMapper

# password
from config import panda_config
passwd = panda_config.dbpasswd

# logger
_logger = PandaLogger().getLogger('add')

_logger.debug("===================== start =====================")
    
# instantiate DB proxies
proxyS = DBProxy()
proxyS.connect(panda_config.dbhost,panda_config.dbpasswd,panda_config.dbuser,panda_config.dbname)

# delete
_logger.debug("Del session")
status,retSel = proxyS.querySQLS("SELECT MAX(PandaID) FROM jobsDefined4")
if status == 1 and retSel != None:
    try:
        maxID = retSel[0][0]
        _logger.debug("maxID : %s" % maxID)
        if maxID != None:
            status,retDel = proxyS.querySQLS("DELETE FROM jobsDefined4 WHERE PandaID<%s AND (jobStatus='activated' OR jobStatus='waiting' OR jobStatus='failed')" % maxID)
    except:
        pass
    
# instantiate TB
taskBuffer.init(panda_config.dbhost,panda_config.dbpasswd,nDBConnection=1)

# instantiate sitemapper
aSiteMapper = SiteMapper(taskBuffer)

# get buildJobs in the holding state
holdingAna = []
status,res = proxyS.querySQLS("SELECT PandaID from jobsActive4 WHERE prodSourceLabel='panda' AND jobStatus='holding'")
if res != None:
    for id, in res:
        holdingAna.append(id)
_logger.debug("holding Ana %s " % holdingAna)
    
# add files
_logger.debug("Adder session")
timeNow = datetime.datetime.utcnow()
timeInt = datetime.datetime.utcnow()
dirName = panda_config.logdir
fileList = os.listdir(dirName)
fileList.sort() 
# remove duplicated files
tmpList = []
uMap = {}
for file in fileList:
    match = re.search('^(\d+)_([^_]+)_.{36}$',file)
    if match != None:
        fileName = '%s/%s' % (dirName,file)
        id = match.group(1)
        if uMap.has_key(id):
            try:
                os.remove(fileName)
            except:
                pass
        else:
            uMap[id] = fileName
            if long(id) in holdingAna:
                # give a priority to buildJobs
                tmpList.insert(0,file)
            else:
                tmpList.append(file)
fileList = tmpList            

# add
while len(fileList) != 0:
    # time limit to aviod too many copyArchve running at the sametime
    #if (datetime.datetime.utcnow() - timeNow) > datetime.timedelta(hours=1):
    if (datetime.datetime.utcnow() - timeNow) > datetime.timedelta(minutes=40):
        _logger.debug("time over in Adder session")
        break
    # get fileList
    if (datetime.datetime.utcnow() - timeInt) > datetime.timedelta(minutes=15):
        timeInt = datetime.datetime.utcnow()
        # get file
        fileList = os.listdir(dirName)
        fileList.sort() 
        # remove duplicated files
        tmpList = []
        uMap = {}
        for file in fileList:
            match = re.search('^(\d+)_([^_]+)_.{36}$',file)
            if match != None:
                fileName = '%s/%s' % (dirName,file)
                id = match.group(1)
                if uMap.has_key(id):
                    try:
                        os.remove(fileName)
                    except:
                        pass
                else:
                    uMap[id] = fileName
                    if long(id) in holdingAna:
                        # give a priority to buildJob
                        tmpList.insert(0,file)
                    else:
                        tmpList.append(file)
        fileList = tmpList
    # choose a file
    file = fileList.pop(0)
    match = re.search('^(\d+)_([^_]+)_.{36}$',file)
    if match != None:
        fileName = '%s/%s' % (dirName,file)
        if not os.path.exists(fileName):
            continue
        try:
            modTime = datetime.datetime(*(time.gmtime(os.path.getmtime(fileName))[:7]))
            if (timeNow - modTime) > datetime.timedelta(hours=24):
                # last chance
                _logger.debug("Last Add File : %s" % fileName)
                thr = Adder(taskBuffer,match.group(1),"",match.group(2),xmlFile=fileName,ignoreDDMError=False,
                            joinCloser=True,addOutput=True,siteMapper=aSiteMapper)
                thr.start()
                thr.join()
            elif (timeInt - modTime) > datetime.timedelta(minutes=3):
                # add
                _logger.debug("Add File : %s" % fileName)            
                thr = Adder(taskBuffer,match.group(1),"",match.group(2),xmlFile=fileName,joinCloser=True,
                            addOutput=True,siteMapper=aSiteMapper)            
                thr.start()
                thr.join()
        except:
            type, value, traceBack = sys.exc_info()
            _logger.error("%s %s" % (type,value))
_logger.debug("===================== end =====================")
