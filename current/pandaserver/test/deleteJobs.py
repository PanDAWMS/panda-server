import os
import re
import sys
import time
import fcntl
import types
import shelve
import random
import datetime
import commands
import threading
import userinterface.Client as Client
from dataservice.DDM import ddm
from dataservice.DDM import dashBorad
from taskbuffer.OraDBProxy import DBProxy
from taskbuffer.TaskBuffer import taskBuffer
from pandalogger.PandaLogger import PandaLogger
from jobdispatcher.Watcher import Watcher
from brokerage.SiteMapper import SiteMapper
from dataservice.Adder import Adder
from dataservice.Finisher import Finisher
from dataservice.MailUtils import MailUtils
from taskbuffer import ProcessGroups
import brokerage.broker_util
import brokerage.broker
import taskbuffer.ErrorCode
import dataservice.DDM

# password
from config import panda_config
passwd = panda_config.dbpasswd

# logger
_logger = PandaLogger().getLogger('deleteJobs')

_logger.debug("===================== start =====================")

# memory checker
def _memoryCheck(str):
    try:
        proc_status = '/proc/%d/status' % os.getpid()
        procfile = open(proc_status)
        name   = ""
        vmSize = ""
        vmRSS  = ""
        # extract Name,VmSize,VmRSS
        for line in procfile:
            if line.startswith("Name:"):
                name = line.split()[-1]
                continue
            if line.startswith("VmSize:"):
                vmSize = ""
                for item in line.split()[1:]:
                    vmSize += item
                continue
            if line.startswith("VmRSS:"):
                vmRSS = ""
                for item in line.split()[1:]:
                    vmRSS += item
                continue
        procfile.close()
        _logger.debug('MemCheck - %s Name=%s VSZ=%s RSS=%s : %s' % (os.getpid(),name,vmSize,vmRSS,str))
    except:
        type, value, traceBack = sys.exc_info()
        _logger.error("memoryCheck() : %s %s" % (type,value))
        _logger.debug('MemCheck - %s unknown : %s' % (os.getpid(),str))
    return

_memoryCheck("start")

# kill old process
try:
    # time limit
    timeLimit = datetime.datetime.utcnow() - datetime.timedelta(hours=2)
    # get process list
    scriptName = sys.argv[0]
    out = commands.getoutput('ps axo user,pid,lstart,args | grep %s' % scriptName)
    for line in out.split('\n'):
        items = line.split()
        # owned process
        if not items[0] in ['sm','atlpan','root']: # ['os.getlogin()']: doesn't work in cron
            continue
        # look for python
        if re.search('python',line) == None:
            continue
        # PID
        pid = items[1]
        # start time
        timeM = re.search('(\S+\s+\d+ \d+:\d+:\d+ \d+)',line)
        startTime = datetime.datetime(*time.strptime(timeM.group(1),'%b %d %H:%M:%S %Y')[:6])
        # kill old process
        if startTime < timeLimit:
            _logger.debug("old process : %s %s" % (pid,startTime))
            _logger.debug(line)            
            commands.getoutput('kill -9 %s' % pid)
except:
    type, value, traceBack = sys.exc_info()
    _logger.error("kill process : %s %s" % (type,value))
    

# instantiate TB
taskBuffer.init(panda_config.dbhost,panda_config.dbpasswd,nDBConnection=1)

# instantiate sitemapper
siteMapper = SiteMapper(taskBuffer)


# table names
jobATableName   = "ATLAS_PANDAARCH.jobsArchived"
filesATableName = "ATLAS_PANDAARCH.filesTable_ARCH"
paramATableName = "ATLAS_PANDAARCH.jobParamsTable_ARCH"
metaATableName  = "ATLAS_PANDAARCH.metaTable_ARCH"

# time limit
timeLimit = datetime.datetime.utcnow() - datetime.timedelta(days=3)

# delete
_logger.debug("get PandaIDs for Delete")
sql = "SELECT COUNT(*) FROM ATLAS_PANDA.jobsArchived4 WHERE modificationTime<:modificationTime"
varMap = {}
varMap[':modificationTime'] = timeLimit
status,res = taskBuffer.querySQLS(sql,varMap)
if res != None:
    tmpTotal = res[0][0]
else:
    tmpTotal = None
maxBunch = 1000
nBunch = 500
tmpIndex = 0
while True:
    sql  = "SELECT PandaID,modificationTime FROM ATLAS_PANDA.jobsArchived4 "
    sql += "WHERE modificationTime<:modificationTime AND archivedFlag=:archivedFlag AND rownum<=:rowRange"
    varMap = {}
    varMap[':modificationTime'] = timeLimit
    varMap[':archivedFlag'] = 1    
    varMap[':rowRange'] = maxBunch
    status,res = taskBuffer.querySQLS(sql,varMap)
    if res == None:
        _logger.error("failed to get PandaIDs to be deleted")
        break
    else:
        _logger.debug("got %s for deletion" % len(res))            
        if len(res) == 0:
            _logger.debug("no jobs left for for deletion")
            break
        else:
            maxBunch = len(res)
            random.shuffle(res)
            res = res[:nBunch]
            # loop over all jobs
            for (id,srcEndTime) in res:
                tmpIndex += 1        
                try:
                    # check
                    sql = "SELECT PandaID from %s WHERE PandaID=:PandaID" % jobATableName
                    varMap = {}
                    varMap[':PandaID'] = id
                    status,check = taskBuffer.querySQLS(sql,varMap)
                    if check == None or len(check) == 0:
                        # no record in ArchivedDB
                        _logger.error("No backup for %s" % id)
                    else:
                        # delete
                        _logger.debug("DEL %s : endTime %s" % (id,srcEndTime))
                        proxyS = taskBuffer.proxyPool.getProxy()
                        proxyS.deleteJobSimple(id)
                        taskBuffer.proxyPool.putProxy(proxyS)
                    if tmpIndex % 1000 == 1:
                        _logger.debug(" deleted %s/%s" % (tmpIndex,tmpTotal))
                except:
                    pass
            # terminate
            if maxBunch < nBunch:
                break
_logger.debug("===================== end =====================")
