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
_logger = PandaLogger().getLogger('copyArchive')

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

# kill old dq2 process
try:
    # time limit
    timeLimit = datetime.datetime.utcnow() - datetime.timedelta(hours=2)
    # get process list
    scriptName = sys.argv[0]
    out = commands.getoutput('ps axo user,pid,lstart,args | grep dq2.clientapi | grep -v PYTHONPATH | grep -v grep')
    for line in out.split('\n'):
        if line == '':
            continue
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
            _logger.debug("old dq2 process : %s %s" % (pid,startTime))
            _logger.debug(line)            
            commands.getoutput('kill -9 %s' % pid)
except:
    type, value, traceBack = sys.exc_info()
    _logger.error("kill dq2 process : %s %s" % (type,value))


# kill old process
try:
    # time limit
    timeLimit = datetime.datetime.utcnow() - datetime.timedelta(hours=4)
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

# copy
_logger.debug("get PandaIDs for Archive")
varMap = {}
varMap[':archivedFlag'] = 0
status,res = taskBuffer.querySQLS("SELECT PandaID,modificationTime FROM ATLAS_PANDA.jobsArchived4 WHERE archivedFlag=:archivedFlag ORDER BY PandaID",
                              varMap,arraySize=1000000)
if res == None:
    _logger.debug("total %s " % res)
else:
    _logger.debug("total %s " % len(res))
    # copy
    for (id,srcEndTime) in res:
        try:
            # check if already recorded
            _logger.debug("check  %s " % id)
            sql = "SELECT PandaID from %s WHERE PandaID=:PandaID" % jobATableName
            varMap = {}
            varMap[':PandaID'] = id
            status,check = taskBuffer.querySQLS(sql,varMap)
            # copy
            if len(check) == 0:
                # get jobs
                job = taskBuffer.peekJobs([id],False,False,True,False)[0]
                # insert to archived
                if job != None and job.jobStatus != 'unknown':
                    proxyS = taskBuffer.proxyPool.getProxy()
                    proxyS.insertJobSimple(job,jobATableName,filesATableName,paramATableName,metaATableName)
                    taskBuffer.proxyPool.putProxy(proxyS)
                    _logger.debug("INSERT %s" % id)
                else:
                    _logger.error("Failed to peek at %s" % id)
            else:
                # set archivedFlag            
                varMap = {}
                varMap[':PandaID'] = id
                varMap[':archivedFlag'] = 1
                sqlUpdate = "UPDATE ATLAS_PANDA.jobsArchived4 SET archivedFlag=:archivedFlag WHERE PandaID=:PandaID"
                taskBuffer.querySQLS(sqlUpdate,varMap)
        except:
            pass
        
# delete
_logger.debug("get PandaIDs for Delete")
varMap = {}
varMap[':modificationTime'] = timeLimit
status,res = taskBuffer.querySQLS("SELECT PandaID,modificationTime FROM ATLAS_PANDA.jobsArchived4 WHERE modificationTime<:modificationTime",
                              varMap,arraySize=1000000)
if res == None:
    _logger.debug("total %s " % res)
else:
    _logger.debug("total %s " % len(res))
    # loop over all jobs
    for (id,srcEndTime) in res:
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
        except:
            pass
del res


# increase priorities for long-waiting analysis jobs
_logger.debug("Analysis Priorities")
timeLimit = datetime.datetime.utcnow() - datetime.timedelta(hours=24)
sql = "UPDATE ATLAS_PANDA.jobsActive4 SET currentPriority=currentPriority+40,modificationTime=CURRENT_DATE WHERE jobStatus=:jobStatus "
sql+= "AND modificationTime<:modificationTime AND prodSourceLabel=:prodSourceLabel "
varMap = {}
varMap[':jobStatus'] = 'activated'
varMap[':prodSourceLabel'] = 'user'
varMap[':modificationTime'] = timeLimit
status,res = taskBuffer.querySQLS(sql,varMap)
_logger.debug("increased priority for %s" % status)
time.sleep(1)


# send email for access requests
_logger.debug("Site Access")
try:
    # get contact
    contactAddr = {}
    siteContactAddr = {}
    sql = "SELECT name,email FROM ATLAS_PANDAMETA.cloudconfig"
    status,res = taskBuffer.querySQLS(sql,{})
    for cloudName,cloudEmail in res:
        contactAddr[cloudName] = cloudEmail
    # get requests 
    sql  = "SELECT pandaSite,status,dn FROM ATLAS_PANDAMETA.siteaccess WHERE status IN (:status1,:status2,:status3) "
    sql += "ORDER BY pandaSite,status " 
    varMap = {}
    varMap[':status1'] = 'requested'
    varMap[':status2'] = 'tobeapproved'
    varMap[':status3'] = 'toberejected'
    status,res = taskBuffer.querySQLS(sql,varMap)
    requestsInCloud = {}
    mailUtils = MailUtils()
    # loop over all requests
    for pandaSite,reqStatus,userName in res:
        cloud = siteMapper.getSite(pandaSite).cloud
        _logger.debug("request : '%s' site=%s status=%s cloud=%s" % (userName,pandaSite,reqStatus,cloud))
        # send emails to user
        if reqStatus in ['tobeapproved','toberejected']:
            # set status
            if reqStatus == 'tobeapproved':
                newStatus = 'approved'
            else:
                newStatus = 'rejected'
            # get mail address for user
            userMailAddr = ''
            sqlUM = "SELECT email FROM ATLAS_PANDAMETA.users WHERE name=:userName"
            varMap = {}
            varMap[':userName'] = userName
            stUM,resUM = taskBuffer.querySQLS(sqlUM,varMap)
            if resUM == None or len(resUM) == 0:
                _logger.error("email address is unavailable for '%s'" % userName)
            else:
                userMailAddr = resUM[0][0]
            # send
            if not userMailAddr in ['',None,'None','notsend']:
                _logger.debug("send update to %s" % userMailAddr)
                retMail = mailUtils.sendSiteAccessUpdate(userMailAddr,newStatus,pandaSite)
                _logger.debug(retMail)
            # update database
            sqlUp  = "UPDATE ATLAS_PANDAMETA.siteaccess SET status=:newStatus "
            sqlUp += "WHERE pandaSite=:pandaSite AND dn=:userName"
            varMap = {}
            varMap[':userName']  = userName
            varMap[':newStatus'] = newStatus            
            varMap[':pandaSite'] = pandaSite
            stUp,resUp = taskBuffer.querySQLS(sqlUp,varMap)
        else:
            # append cloud
            if not requestsInCloud.has_key(cloud):
                requestsInCloud[cloud] = {}
            # append site
            if not requestsInCloud[cloud].has_key(pandaSite):
                requestsInCloud[cloud][pandaSite] = []
            # append user
            requestsInCloud[cloud][pandaSite].append(userName)
    # send requests to the cloud responsible
    for cloud,requestsMap in requestsInCloud.iteritems():
        _logger.debug("requests for approval : cloud=%s" % cloud)
        # send
        if contactAddr.has_key(cloud) and (not contactAddr[cloud] in ['',None,'None']):
            # get site contact
            for pandaSite,userNames in requestsMap.iteritems():
                if not siteContactAddr.has_key(pandaSite):
                    varMap = {}
                    varMap[':siteid'] = pandaSite
                    sqlSite = "SELECT email FROM ATLAS_PANDAMETA.schedconfig WHERE siteid=:siteid AND rownum<=1"
                    status,res = taskBuffer.querySQLS(sqlSite,varMap)
                    siteContactAddr[pandaSite] = res[0][0]
                    # append
                    if not siteContactAddr[pandaSite] in ['',None,'None']:
                        contactAddr[cloud] += ',%s' % siteContactAddr[pandaSite]
            # send            
            _logger.debug("send request to %s" % contactAddr[cloud])
            retMail = mailUtils.sendSiteAccessRequest(contactAddr[cloud],requestsMap,cloud)
            _logger.debug(retMail)
            # update database
            if retMail:
                sqlUp  = "UPDATE ATLAS_PANDAMETA.siteaccess SET status=:newStatus "
                sqlUp += "WHERE pandaSite=:pandaSite AND dn=:userName"
                for pandaSite,userNames in requestsMap.iteritems():
                    for userName in userNames:
                        varMap = {}
                        varMap[':userName']  = userName
                        varMap[':newStatus'] = 'inprocess'
                        varMap[':pandaSite'] = pandaSite
                        stUp,resUp = taskBuffer.querySQLS(sqlUp,varMap)
        else:
            _logger.error("contact email address is unavailable for %s" % cloud)
except:
    type, value, traceBack = sys.exc_info()
    _logger.error("Failed with %s %s" % (type,value))
_logger.debug("Site Access : done")    
            
_memoryCheck("watcher")

_logger.debug("Watcher session")
# check heartbeat for analysis jobs
timeLimit = datetime.datetime.utcnow() - datetime.timedelta(hours=2)
varMap = {}
varMap[':modificationTime'] = timeLimit
varMap[':prodSourceLabel1'] = 'panda'
varMap[':prodSourceLabel2'] = 'user'
varMap[':jobStatus1'] = 'running'
varMap[':jobStatus2'] = 'starting'
varMap[':jobStatus3'] = 'stagein'
varMap[':jobStatus4'] = 'stageout'
sql  = "SELECT PandaID FROM ATLAS_PANDA.jobsActive4 WHERE (prodSourceLabel=:prodSourceLabel1 OR prodSourceLabel=:prodSourceLabel2) "
sql += "AND (jobStatus=:jobStatus1 OR jobStatus=:jobStatus2 OR jobStatus=:jobStatus3 OR jobStatus=:jobStatus4) AND modificationTime<:modificationTime"
status,res = taskBuffer.querySQLS(sql,varMap)
if res == None:
    _logger.debug("# of Anal Watcher : %s" % res)
else:
    _logger.debug("# of Anal Watcher : %s" % len(res))    
    for (id,) in res:
        _logger.debug("Anal Watcher %s" % id)    
        thr = Watcher(taskBuffer,id,single=True,sleepTime=60)
        thr.start()
        thr.join()
        time.sleep(1)

# check heartbeat for sent jobs
timeLimit = datetime.datetime.utcnow() - datetime.timedelta(minutes=30)
varMap = {}
varMap[':jobStatus'] = 'sent'
varMap[':modificationTime'] = timeLimit
status,res = taskBuffer.querySQLS("SELECT PandaID FROM ATLAS_PANDA.jobsActive4 WHERE jobStatus=:jobStatus AND modificationTime<:modificationTime",
                              varMap)
if res == None:
    _logger.debug("# of Sent Watcher : %s" % res)
else:
    _logger.debug("# of Sent Watcher : %s" % len(res))
    for (id,) in res:
        _logger.debug("Sent Watcher %s" % id)        
        thr = Watcher(taskBuffer,id,single=True,sleepTime=30)
        thr.start()
        thr.join()
        time.sleep(1)

# check heartbeat for 'holding' analysis/ddm jobs
timeLimit = datetime.datetime.utcnow() - datetime.timedelta(hours=3)
# get XMLs
xmlIDs = []
xmlFiles = os.listdir(panda_config.logdir)
for file in xmlFiles:
    match = re.search('^(\d+)_([^_]+)_.{36}$',file)
    if match != None:
        id = match.group(1)
        xmlIDs.append(int(id))
sql = "SELECT PandaID FROM ATLAS_PANDA.jobsActive4 WHERE jobStatus=:jobStatus AND (modificationTime<:modificationTime OR (endTime IS NOT NULL AND endTime<:endTime)) AND (prodSourceLabel=:prodSourceLabel1 OR prodSourceLabel=:prodSourceLabel2 OR prodSourceLabel=:prodSourceLabel3)"
varMap = {}
varMap[':modificationTime'] = timeLimit
varMap[':endTime'] = timeLimit
varMap[':jobStatus'] = 'holding'
varMap[':prodSourceLabel1'] = 'panda'
varMap[':prodSourceLabel2'] = 'user'
varMap[':prodSourceLabel3'] = 'ddm'
status,res = taskBuffer.querySQLS(sql,varMap)
if res == None:
    _logger.debug("# of Holding Anal/DDM Watcher : %s" % res)
else:
    _logger.debug("# of Holding Anal/DDM Watcher : %s - XMLs : %s" % (len(res),len(xmlIDs)))
    for (id,) in res:
        _logger.debug("Holding Anal/DDM Watcher %s" % id)
        if int(id) in xmlIDs:
            _logger.debug("   found XML -> skip %s" % id)
            continue
        thr = Watcher(taskBuffer,id,single=True,sleepTime=180)
        thr.start()
        thr.join()
        time.sleep(1)

# check heartbeat for production jobs
timeOutVal = 48
timeLimit = datetime.datetime.utcnow() - datetime.timedelta(hours=timeOutVal)
sql = "SELECT PandaID FROM ATLAS_PANDA.jobsActive4 WHERE jobStatus=:jobStatus AND (modificationTime<:modificationTime OR (endTime IS NOT NULL AND endTime<:endTime))"
varMap = {}
varMap[':modificationTime'] = timeLimit
varMap[':endTime'] = timeLimit
varMap[':jobStatus'] = 'holding'
status,res = taskBuffer.querySQLS(sql,varMap)
if res == None:
    _logger.debug("# of Holding Watcher : %s" % res)
else:
    _logger.debug("# of Holding Watcher : %s" % len(res))    
    for (id,) in res:
        _logger.debug("Holding Watcher %s" % id)
        thr = Watcher(taskBuffer,id,single=True,sleepTime=60*timeOutVal)
        thr.start()
        thr.join()
        time.sleep(1)

# check heartbeat for ddm jobs
timeLimit = datetime.datetime.utcnow() - datetime.timedelta(hours=2)
varMap = {}
varMap[':modificationTime'] = timeLimit
varMap[':jobStatus1'] = 'running'
varMap[':jobStatus2'] = 'starting'
varMap[':jobStatus3'] = 'stagein'
varMap[':jobStatus4'] = 'stageout'
varMap[':prodSourceLabel'] = 'ddm'
status,res = taskBuffer.querySQLS("SELECT PandaID FROM ATLAS_PANDA.jobsActive4 WHERE (jobStatus=:jobStatus1 OR jobStatus=:jobStatus2 OR jobStatus=:jobStatus3 OR jobStatus=:jobStatus4) AND modificationTime<:modificationTime AND prodSourceLabel=:prodSourceLabel",
                              varMap)
if res == None:
    _logger.debug("# of DDM Watcher : %s" % res)
else:
    _logger.debug("# of DDM Watcher : %s" % len(res))    
    for (id,) in res:
        _logger.debug("DDM Watcher %s" % id)
        thr = Watcher(taskBuffer,id,single=True,sleepTime=120)
        thr.start()
        thr.join()
        time.sleep(1)

# check heartbeat for production jobs
timeLimit = datetime.datetime.utcnow() - datetime.timedelta(hours=6)
varMap = {}
varMap[':modificationTime'] = timeLimit
varMap[':jobStatus1'] = 'running'
varMap[':jobStatus2'] = 'starting'
varMap[':jobStatus3'] = 'stagein'
varMap[':jobStatus4'] = 'stageout'
status,res = taskBuffer.querySQLS("SELECT PandaID FROM ATLAS_PANDA.jobsActive4 WHERE (jobStatus=:jobStatus1 OR jobStatus=:jobStatus2 OR jobStatus=:jobStatus3 OR jobStatus=:jobStatus4) AND modificationTime<:modificationTime",
                              varMap)
if res == None:
    _logger.debug("# of General Watcher : %s" % res)
else:
    _logger.debug("# of General Watcher : %s" % len(res))    
    for (id,) in res:
        _logger.debug("General Watcher %s" % id)
        thr = Watcher(taskBuffer,id,single=True,sitemapper=siteMapper)
        thr.start()
        thr.join()
        time.sleep(1)

_memoryCheck("reassign")

# erase dispatch datasets
def eraseDispDatasets(ids):
    _logger.debug("eraseDispDatasets")
    datasets = []
    # get jobs
    status,jobs = Client.getJobStatus(ids)
    if status != 0:
        return
    # gather dispDBlcoks
    for job in jobs:
        # dispatchDS is not a DQ2 dataset in US
        if job.cloud == 'US':
            continue
        # erase disp datasets for production jobs only
        if job.prodSourceLabel != 'managed':
            continue
        for file in job.Files:
            if file.dispatchDBlock == 'NULL':
                continue
            if (not file.dispatchDBlock in datasets) and \
               re.search('_dis\d+$',file.dispatchDBlock) != None:
                datasets.append(file.dispatchDBlock)
    # erase
    for dataset in datasets:
        _logger.debug('erase %s' % dataset)
        status,out = ddm.DQ2.main('eraseDataset',dataset)
        _logger.debug(out)


# kill long-waiting jobs in defined table
timeLimit = datetime.datetime.utcnow() - datetime.timedelta(days=7)
status,res = taskBuffer.querySQLS("SELECT PandaID,cloud,prodSourceLabel FROM ATLAS_PANDA.jobsDefined4 WHERE creationTime<:creationTime",
                              {':creationTime':timeLimit})
jobs=[]
dashFileMap = {}
if res != None:
    for pandaID,cloud,prodSourceLabel in res:
        # collect PandaIDs
        jobs.append(pandaID)
        try:
            if cloud in ['US']:
                # skip US since file info is not available in dashboard
                continue
            # check file status for production
            if not prodSourceLabel in ['managed']:
                pass
            else:
                # get T1 site
                tmpT1siteID = siteMapper.getCloud(cloud)['source']
                t1Site = siteMapper.getSite(tmpT1siteID)
                # get pending input files
                sqlF  = "SELECT lfn,GUID,dispatchDBlock FROM ATLAS_PANDA.filesTable4 WHERE PandaID=:PandaID "
                sqlF += "AND type=:type AND status=:status"
                varMap = {}
                varMap[':type']    = 'input'
                varMap[':status']  = 'pending'
                varMap[':PandaID'] = pandaID
                stFile,resFile = taskBuffer.querySQLS(sqlF,varMap)
                if resFile != None:
                    # loop over all files
                    for tmpLFN,tmpGUID,tmpDispDBlock in resFile:
                        # get file events
                        tmpDQ2IDs = t1Site.setokens.values()
                        tmpKey = (tuple(tmpDQ2IDs),tmpLFN)
                        if not dashFileMap.has_key(tmpKey):
                            _logger.debug('getting fileEvents for %s:%s' % tmpKey)
                            tmpStat,tmpOut = dashBorad.listFileEvents(tmpDQ2IDs,tmpGUID)
                            _logger.debug(tmpStat)
                            _logger.debug(tmpOut)                            
                            if tmpStat != 0:
                                # failed
                                continue
                            # convert to list
                            try:
                                exec "tmpEvens = %s" % tmpOut
                                if not isinstance(tmpEvens,types.ListType):
                                    raise TypeError,"%s is not a list" % type(tmpEvens)
                            except:
                                errType,errValue = sys.exc_info()[:2]
                                _logger.error(tmpOut)                                
                                _logger.error("invalid dashboard response %s %s" % (errType,errValue))
                                continue
                            dashFileMap[tmpKey] = None
                            # look for latest events
                            tmpLastTime = ''
                            for tmpEvt in tmpEvens:
                                # pickup only DQ2 events
                                if not tmpEvt['tool_id'] in ['DQ2',None]:
                                    continue
                                # pickup first one or newer
                                if tmpLastTime == '' or tmpLastTime < tmpEvt['modified_time']:
                                    tmpLastTime = tmpEvt['modified_time']
                                    dashFileMap[tmpKey] = tmpEvt['state']
                            _logger.debug('got status=%s' % dashFileMap[tmpKey])
                        # update failed files
                        if dashFileMap[tmpKey] in ['FAILED_TRANSFER','BAD']:
                            sqlUpF  = "UPDATE ATLAS_PANDA.filesTable4 SET status=:newStatus "
                            sqlUpF += "WHERE PandaID=:PandaID AND lfn=:lfn"
                            varMap = {}
                            varMap[':PandaID'] = pandaID
                            varMap[':lfn'] = tmpLFN
                            varMap[':newStatus'] = dashFileMap[tmpKey].lower()
                            taskBuffer.querySQLS(sqlUpF,varMap)
                            _logger.debug('set status=%s to %s:%s' % (dashFileMap[tmpKey],pandaID,tmpLFN))
        except:
            errType,errValue = sys.exc_info()[:2]
            _logger.error("dashboard access failed with %s %s" % (errType,errValue))
if len(jobs):
    eraseDispDatasets(jobs)
    _logger.debug("killJobs for Defined (%s)" % str(jobs))
    Client.killJobs(jobs,2)

# kill long-waiting jobs in active table
timeLimit = datetime.datetime.utcnow() - datetime.timedelta(days=7)
varMap = {}
varMap[':jobStatus'] = 'activated'
varMap[':creationTime'] = timeLimit
status,res = taskBuffer.querySQLS("SELECT PandaID from ATLAS_PANDA.jobsActive4 WHERE jobStatus=:jobStatus AND creationTime<:creationTime",
                              varMap)
jobs=[]
if res != None:
    for (id,) in res:
        jobs.append(id)
if len(jobs):
    eraseDispDatasets(jobs)
    _logger.debug("killJobs for Active (%s)" % str(jobs))
    Client.killJobs(jobs,2)


# kill long-waiting ddm jobs for dispatch
_logger.debug("kill PandaMovers")
timeLimit = datetime.datetime.utcnow() - datetime.timedelta(hours=12)
sql = "SELECT PandaID from ATLAS_PANDA.jobsActive4 WHERE prodSourceLabel=:prodSourceLabel AND transferType=:transferType AND creationTime<:creationTime"
varMap = {}
varMap[':creationTime']    = timeLimit
varMap[':prodSourceLabel'] = 'ddm'
varMap[':transferType']    = 'dis'
_logger.debug(sql+str(varMap))
status,res = taskBuffer.querySQLS(sql,varMap)
_logger.debug(res)
jobs=[]
if res != None:
    for (id,) in res:
        jobs.append(id)
if len(jobs):
    _logger.debug("kill DDM Jobs (%s)" % str(jobs))
    Client.killJobs(jobs,2)

# kill hang-up movers
timeLimit = datetime.datetime.utcnow() - datetime.timedelta(hours=3)
sql = "SELECT PandaID from ATLAS_PANDA.jobsActive4 WHERE prodSourceLabel=:prodSourceLabel AND transferType=:transferType AND jobStatus=:jobStatus AND startTime<:startTime"
varMap = {}
varMap[':startTime'] = timeLimit
varMap[':prodSourceLabel'] = 'ddm'
varMap[':transferType']    = 'dis'
varMap[':jobStatus'] = 'running'
_logger.debug(sql+str(varMap))
status,res = taskBuffer.querySQLS(sql,varMap)
_logger.debug(res)
jobs   = []
movers = []
if res != None:
    for id, in res:
        movers.append(id)
        # get dispatch dataset
        sql = 'SELECT name FROM ATLAS_PANDA.Datasets WHERE MoverID=:MoverID'
        stDS,resDS = taskBuffer.querySQLS(sql,{':MoverID':id})
        if resDS != None:
            disDS = resDS[0][0]
            # get PandaIDs associated to the dis dataset
            sql = "SELECT PandaID FROM ATLAS_PANDA.jobsDefined4 WHERE jobStatus=:jobStatus AND dispatchDBlock=:dispatchDBlock"
            varMap = {}
            varMap[':jobStatus'] = 'assigned'
            varMap[':dispatchDBlock'] = disDS
            stP,resP = taskBuffer.querySQLS(sql,varMap)
            if resP != None:
                for pandaID, in resP:
                    jobs.append(pandaID)
# kill movers                    
if len(movers):
    _logger.debug("kill hangup DDM Jobs (%s)" % str(movers))
    Client.killJobs(movers,2)
# reassign jobs
if len(jobs):
    nJob = 100
    iJob = 0
    while iJob < len(jobs):
        _logger.debug('reassignJobs for hangup movers (%s)' % jobs[iJob:iJob+nJob])
        Client.reassignJobs(jobs[iJob:iJob+nJob])
        iJob += nJob
        time.sleep(60)

# reassign defined jobs in defined table
timeLimit = datetime.datetime.utcnow() - datetime.timedelta(hours=4)
while True:
    # get PandaIDs
    varMap = {}
    varMap[':jobStatus'] = 'defined'
    varMap[':prodSourceLabel'] = 'managed'
    varMap[':modificationTime'] = timeLimit
    status,res = taskBuffer.querySQLS("SELECT PandaID from ATLAS_PANDA.jobsDefined4 where jobStatus=:jobStatus AND modificationTime<:modificationTime AND prodSourceLabel=:prodSourceLabel ORDER BY PandaID",
                                  varMap)
    # escape
    if res == None or len(res) == 0:
        break
    # convert to list
    jobs = []
    for id, in res:
        jobs.append(id)
    # reassign
    nJob = 100
    iJob = 0
    while iJob < len(jobs):
        _logger.debug('reassignJobs for Defined (%s)' % jobs[iJob:iJob+nJob])
        Client.reassignJobs(jobs[iJob:iJob+nJob])
        iJob += nJob
        time.sleep(60)

                        
# reassign when ratio of running/notrunning is too unbalanced
_logger.debug("reassign Unbalanced")
timeLimit = datetime.datetime.utcnow() - datetime.timedelta(hours=4)
jobStat = {}
rangeValues = ['all','limit']
for rangeVal in rangeValues:
    for jobStatus in ['running','activated','assigned']:
        table = 'ATLAS_PANDA.jobsDefined4'
        if jobStatus in ['running','activated']:
            table = 'ATLAS_PANDA.jobsActive4'
        varMap = {}    
        varMap[':prodSourceLabel'] = 'managed'
        varMap[':jobStatus'] = jobStatus
        if rangeVal == 'all':
            sql = "SELECT computingSite,cloud,processingType,count(*) FROM %s WHERE prodSourceLabel=:prodSourceLabel AND jobStatus=:jobStatus GROUP BY computingSite,cloud,processingType" \
                  % table
        else:
            sql = "SELECT computingSite,cloud,processingType,count(*) FROM %s WHERE prodSourceLabel=:prodSourceLabel AND jobStatus=:jobStatus AND modificationTime<:modificationTime GROUP BY computingSite,cloud,processingType" \
                  % table
            varMap[':modificationTime'] = timeLimit
        # execute
        status,res = taskBuffer.querySQLS(sql,varMap)
        if res != None:
            for computingSite,cloud,processingType,nJobs in res:
                # add cloud
                if not jobStat.has_key(cloud):
                    jobStat[cloud] = {}
                # add site
                if not jobStat[cloud].has_key(computingSite):
                    jobStat[cloud][computingSite] = {}
                # add range
                if not jobStat[cloud][computingSite].has_key(rangeVal):
                    jobStat[cloud][computingSite][rangeVal] = {}
                # add process group
                tmpProGroup = ProcessGroups.getProcessGroup(processingType)
                if not jobStat[cloud][computingSite][rangeVal].has_key(tmpProGroup):
                    jobStat[cloud][computingSite][rangeVal][tmpProGroup] = {}
                # set status
                tmpStatus = jobStatus
                if jobStatus != 'running':
                    tmpStatus = 'notrunning'
                # add status
                if not jobStat[cloud][computingSite][rangeVal][tmpProGroup].has_key(tmpStatus):
                    jobStat[cloud][computingSite][rangeVal][tmpProGroup][tmpStatus] = 0
                # add
                jobStat[cloud][computingSite][rangeVal][tmpProGroup][tmpStatus] += nJobs
# look for unbalanced site
for cloud,siteVal in jobStat.iteritems():
    jobsCloud = {}
    ngSites   = {}
    t1Site = siteMapper.getCloud(cloud)['source']
    _logger.debug("Cloud:%s" % cloud)
    for computingSite,jobVal in siteVal.iteritems():
        # set 0
        for rangeVal in rangeValues:
            for pgType,pgList in ProcessGroups.processGroups:
                # add range
                if not jobVal.has_key(rangeVal):
                    jobVal[rangeVal] = {}
                # add process group
                if not jobVal[rangeVal].has_key(pgType):
                    jobVal[rangeVal][pgType] = {}
                # number of jobs
                if not jobVal[rangeVal][pgType].has_key('running'):
                    jobVal[rangeVal][pgType]['running'] = 0
                if not jobVal[rangeVal][pgType].has_key('notrunning'):
                    jobVal[rangeVal][pgType]['notrunning'] = 0            
        # check ratio
        for pgType,pgList in ProcessGroups.processGroups:
            # add process group to map
            if not jobsCloud.has_key(pgType):
                jobsCloud[pgType] = {'notrunning':0,'running':0,'notfull':False}
            if not ngSites.has_key(pgType):
                ngSites[pgType] = []
            # get ratio
            checkRatio = jobVal['limit'][pgType]['notrunning'] > jobVal['all'][pgType]['running']*4
            jobsCloud[pgType]['running']    += jobVal['all'][pgType]['running']            
            jobsCloud[pgType]['notrunning'] += jobVal['all'][pgType]['notrunning']
            # check ratio
            if computingSite in [t1Site,'NULL']:
                # skip T1
                statStr = '--'
            else:
                if checkRatio:
                    statStr = 'NG'
                    ngSites[pgType].append(computingSite)
                else:
                    statStr = '--'
                    # not full
                    if jobVal['all'][pgType]['notrunning'] < jobVal['all'][pgType]['running']*2:
                        jobsCloud[pgType]['notfull'] = True
            _logger.debug("%20s : %14s %s n:%-5s r:%-5s" % (computingSite,pgType,statStr,jobVal['limit'][pgType]['notrunning'],
                                                            jobVal['all'][pgType]['running']))
    #  reassign
    for pgType,pgList in ProcessGroups.processGroups:
        _logger.debug("  %14s : n:%-5s r:%-5s %s" % (pgType,jobsCloud[pgType]['notrunning'],
                                                     jobsCloud[pgType]['running'],jobsCloud[pgType]['notfull']))
        if jobsCloud[pgType]['notrunning'] > jobsCloud[pgType]['running']*2 and ngSites[pgType] != [] and jobsCloud[pgType]['notfull']:
            # reassign except reprocessing
            if pgType in ['reprocessing']:
                continue
            # get PandaIDs
            jobs = []
            for table in ['ATLAS_PANDA.jobsDefined4','ATLAS_PANDA.jobsActive4']:
                varMap = {}
                varMap[':prodSourceLabel'] = 'managed'
                varMap[':jobStatus1'] = 'activated'
                varMap[':jobStatus2'] = 'assigned'
                sql  = "SELECT PandaID FROM %s WHERE prodSourceLabel=:prodSourceLabel AND jobStatus IN (:jobStatus1,:jobStatus2) AND computingSite IN (" % table
                idxSite = 1
                for ngSite in ngSites[pgType]:
                    tmpSiteKey = ':computingSite%s' % idxSite
                    sql += "%s," % tmpSiteKey
                    varMap[tmpSiteKey] = ngSite
                    idxSite += 1
                sql = sql[:-1]
                if pgList != []:
                    sql += ") AND processingType IN ("
                    tmpPgList = pgList
                else:
                    sql += ") AND processingType NOT IN ("
                    # get types to be excluded
                    tmpPgList = []
                    for tmpExPgType,tmpExPgList in ProcessGroups.processGroups:
                        if tmpExPgType != pgType:
                            tmpPgList += tmpExPgList
                idxPro = 1
                for pgItem in tmpPgList:
                    tmpProKey = ':processingType%s' % idxPro
                    sql += "%s," % tmpProKey
                    varMap[tmpProKey] = pgItem
                    idxPro += 1
                sql = sql[:-1]
                sql += ") AND modificationTime<:modificationTime ORDER BY PandaID"
                varMap[':modificationTime'] = timeLimit
                # execute
                _logger.debug(sql+str(varMap))
                status,res = taskBuffer.querySQLS(sql,varMap)
                if res != None:
                    # get IDs
                    for id, in res:
                        jobs.append(id)
            # reassign
            if jobs != []:
                if len(jobs):
                    nJob = 100
                    iJob = 0
                    while iJob < len(jobs):
                        #_logger.debug('reassignJobs for Unbalanced (%s)' % jobs[iJob:iJob+nJob])
                        #eraseDispDatasets(jobs[iJob:iJob+nJob])
                        #Client.reassignJobs(jobs[iJob:iJob+nJob])
                        iJob += nJob
                        #time.sleep(60)



# reassign long-waiting jobs in defined table
timeLimit = datetime.datetime.utcnow() - datetime.timedelta(hours=12)
varMap = {}
varMap[':prodSourceLabel'] = 'managed'
varMap[':modificationTime'] = timeLimit
status,res = taskBuffer.querySQLS("SELECT PandaID from ATLAS_PANDA.jobsDefined4 WHERE prodSourceLabel=:prodSourceLabel AND modificationTime<:modificationTime ORDER BY PandaID",
                              varMap)
jobs=[]
if res != None:
    for (id,) in res:
        jobs.append(id)
# reassign
if len(jobs):
    nJob = 100
    iJob = 0
    while iJob < len(jobs):
        _logger.debug('reassignJobs in Defined (%s)' % jobs[iJob:iJob+nJob])
        eraseDispDatasets(jobs[iJob:iJob+nJob])
        Client.reassignJobs(jobs[iJob:iJob+nJob])
        iJob += nJob
        time.sleep(60)


# reassign too long-standing jobs in active table
timeLimit = datetime.datetime.utcnow() - datetime.timedelta(days=2)
varMap = {}
varMap[':jobStatus'] = 'activated'
varMap[':prodSourceLabel'] = 'managed'
varMap[':modificationTime'] = timeLimit
status,res = taskBuffer.querySQLS("SELECT PandaID FROM ATLAS_PANDA.jobsActive4 WHERE jobStatus=:jobStatus AND prodSourceLabel=:prodSourceLabel AND modificationTime<:modificationTime ORDER BY PandaID",
                              varMap)
jobs = []
if res != None:
    for (id,) in res:
        jobs.append(id)
if len(jobs):
    nJob = 100
    iJob = 0
    while iJob < len(jobs):
        _logger.debug('reassignJobs for Active (%s)' % jobs[iJob:iJob+nJob])
        eraseDispDatasets(jobs[iJob:iJob+nJob])        
        Client.reassignJobs(jobs[iJob:iJob+nJob])
        iJob += nJob
        time.sleep(60)


# kill too long-standing analysis jobs in active table
timeLimit = datetime.datetime.utcnow() - datetime.timedelta(days=7)
varMap = {}
varMap[':prodSourceLabel1'] = 'test'
varMap[':prodSourceLabel2'] = 'panda'
varMap[':prodSourceLabel3'] = 'user'
varMap[':modificationTime'] = timeLimit
status,res = taskBuffer.querySQLS("SELECT PandaID FROM ATLAS_PANDA.jobsActive4 WHERE (prodSourceLabel=:prodSourceLabel1 OR prodSourceLabel=:prodSourceLabel2 OR prodSourceLabel=:prodSourceLabel3) AND modificationTime<:modificationTime ORDER BY PandaID",
                              varMap)
jobs = []
if res != None:
    for (id,) in res:
        jobs.append(id)
# kill
if len(jobs):
    Client.killJobs(jobs,2)
    _logger.debug("killJobs for Anal Active (%s)" % str(jobs))

# kill too long waiting jobs
timeLimit = datetime.datetime.utcnow() - datetime.timedelta(days=1)
status,res = taskBuffer.querySQLS("SELECT PandaID FROM ATLAS_PANDA.jobsWaiting4 WHERE creationTime<:creationTime",
                              {':creationTime':timeLimit})
jobs = []
if res != None:
    for (id,) in res:
        jobs.append(id)
# kill
if len(jobs):
    Client.killJobs(jobs,4)
    _logger.debug("killJobs for Waiting (%s)" % str(jobs))


# reassign long waiting jobs
timeLimit = datetime.datetime.utcnow() - datetime.timedelta(hours=7)
varMap = {}
varMap[':prodSourceLabel'] = 'managed'
varMap[':modificationTime'] = timeLimit
status,res = taskBuffer.querySQLS("SELECT PandaID FROM ATLAS_PANDA.jobsWaiting4 WHERE prodSourceLabel=:prodSourceLabel AND modificationTime<:modificationTime ORDER BY PandaID",
                              varMap)
jobs = []
if res != None:
    for (id,) in res:
        jobs.append(id)
if len(jobs):
    nJob = 100
    iJob = 0
    while iJob < len(jobs):
        _logger.debug('reassignJobs for Waiting (%s)' % jobs[iJob:iJob+nJob])
        Client.reassignJobs(jobs[iJob:iJob+nJob])
        iJob += nJob
        time.sleep(60)

# kill too long running jobs
timeLimit = datetime.datetime.utcnow() - datetime.timedelta(days=21)
status,res = taskBuffer.querySQLS("SELECT PandaID FROM ATLAS_PANDA.jobsActive4 WHERE creationTime<:creationTime",
                              {':creationTime':timeLimit})
jobs = []
if res != None:
    for (id,) in res:
        jobs.append(id)
# kill
if len(jobs):
    nJob = 100
    iJob = 0
    while iJob < len(jobs):
        # set tobekill
        _logger.debug('killJobs for Running (%s)' % jobs[iJob:iJob+nJob])
        Client.killJobs(jobs[iJob:iJob+nJob],2)
        # run watcher
        for id in jobs[iJob:iJob+nJob]:
            thr = Watcher(taskBuffer,id,single=True,sitemapper=siteMapper,sleepTime=60*24*21)
            thr.start()
            thr.join()
            time.sleep(1)
        iJob += nJob
        time.sleep(10)

# kill too long waiting ddm jobs
timeLimit = datetime.datetime.utcnow() - datetime.timedelta(days=5)
varMap = {}
varMap[':prodSourceLabel'] = 'ddm'
varMap[':creationTime'] = timeLimit
status,res = taskBuffer.querySQLS("SELECT PandaID FROM ATLAS_PANDA.jobsActive4 WHERE prodSourceLabel=:prodSourceLabel AND creationTime<:creationTime",
                              varMap)
jobs = []
if res != None:
    for (id,) in res:
        jobs.append(id)
# kill
if len(jobs):
    Client.killJobs(jobs,2)
    _logger.debug("killJobs for DDM (%s)" % str(jobs))

_memoryCheck("closing")


# delete old datasets
timeLimitDnS = datetime.datetime.utcnow() - datetime.timedelta(days=60)
timeLimitTop = datetime.datetime.utcnow() - datetime.timedelta(days=90)
nDelDS = 1000
for dsType,dsPrefix in [('output','sub'),('dispatch','dis'),('','top')]:
    sql = "DELETE FROM ATLAS_PANDA.Datasets "
    if dsType != '':
        # dis or sub
        sql += "WHERE type=:type AND modificationdate<:modificationdate "
        sql += "AND REGEXP_LIKE(name,:pattern) AND rownum <= %s" % nDelDS
        varMap = {}
        varMap[':modificationdate'] = timeLimitDnS
        varMap[':type'] = dsType
        varMap[':pattern'] = '_%s[[:digit:]]+$' % dsPrefix
    else:
        # top level datasets
        sql+= "WHERE modificationdate<:modificationdate AND rownum <= %s" % nDelDS
        varMap = {}
        varMap[':modificationdate'] = timeLimitTop
    for i in range(100):
        # del datasets
        ret,res = taskBuffer.querySQLS(sql, varMap)
        _logger.debug("# of %s datasets deleted: %s" % (dsPrefix,res))
        # no more datasets    
        if res != nDelDS:
            break

# thread pool
class ThreadPool:
    def __init__(self):
        self.lock = threading.Lock()
        self.list = []

    def add(self,obj):
        self.lock.acquire()
        self.list.append(obj)
        self.lock.release()

    def remove(self,obj):
        self.lock.acquire()
        self.list.remove(obj)
        self.lock.release()

    def join(self):
        self.lock.acquire()
        thrlist = tuple(self.list)
        self.lock.release()
        for thr in thrlist:
            thr.join()


# thread to close dataset
class CloserThr (threading.Thread):
    def __init__(self,lock,proxyLock,datasets,pool):
        threading.Thread.__init__(self)
        self.datasets   = datasets
        self.lock       = lock
        self.proxyLock  = proxyLock
        self.pool       = pool
        self.pool.add(self)
                                        
    def run(self):
        self.lock.acquire()
        try:
            # loop over all datasets
            for vuid,name,modDate in self.datasets:
                _logger.debug("Close %s %s" % (modDate,name))
                if not name.startswith('pandaddm_'):
                    status,out = ddm.DQ2.main('freezeDataset',name)
                else:
                    status,out = 0,''
                if status != 0 and out.find('DQFrozenDatasetException') == -1 and \
                       out.find("DQUnknownDatasetException") == -1 and out.find("DQSecurityException") == -1 and \
                       out.find("DQDeletedDatasetException") == -1 and out.find("DQUnknownDatasetException") == -1:
                    _logger.error(out)
                else:
                    self.proxyLock.acquire()
                    varMap = {}
                    varMap[':vuid'] = vuid
                    varMap[':status'] = 'completed'
                    taskBuffer.querySQLS("UPDATE ATLAS_PANDA.Datasets SET status=:status,modificationdate=CURRENT_DATE WHERE vuid=:vuid",
                                     varMap)
                    self.proxyLock.release()                    
                    if name.startswith('pandaddm_'):
                        continue
                    # count # of files
                    status,out = ddm.DQ2.main('getNumberOfFiles',name)
                    _logger.debug(out)                                            
                    if status != 0:
                        _logger.error(out)                            
                    else:
                        try:
                            nFile = int(out)
                            _logger.debug(nFile)
                            if nFile == 0:
                                # erase dataset
                                _logger.debug('erase %s' % name)
                                status,out = ddm.DQ2.main('eraseDataset',name)
                                _logger.debug(out)                            
                        except:
                            pass
        except:
            pass
        self.pool.remove(self)
        self.lock.release()

# close datasets
timeLimitU = datetime.datetime.utcnow() - datetime.timedelta(minutes=30)
timeLimitL = datetime.datetime.utcnow() - datetime.timedelta(days=3)
closeLock = threading.Semaphore(5)
closeProxyLock = threading.Lock()
closeThreadPool = ThreadPool()
while True:
    # lock
    closeLock.acquire()
    # get datasets
    closeProxyLock.acquire()
    varMap = {}
    varMap[':modificationdateU'] = timeLimitU
    varMap[':modificationdateL'] = timeLimitL    
    varMap[':type']   = 'output'
    varMap[':status'] = 'tobeclosed'
    status,res = taskBuffer.querySQLS("SELECT vuid,name,modificationdate FROM ATLAS_PANDA.Datasets WHERE type=:type AND status=:status AND (modificationdate BETWEEN :modificationdateL AND :modificationdateU) AND rownum <= 10",
                                  varMap)
    if res == None:
        _logger.debug("# of datasets to be closed: %s" % res)
    else:
        _logger.debug("# of datasets to be closed: %s" % len(res))
    if res==None or len(res)==0:
        closeProxyLock.release()
        closeLock.release()
        break
    # update to prevent other process from picking up
    for (vuid,name,modDate) in res:
        taskBuffer.querySQLS("UPDATE ATLAS_PANDA.Datasets SET modificationdate=CURRENT_DATE WHERE vuid=:vuid", {':vuid':vuid})
    # release
    closeProxyLock.release()
    closeLock.release()
    # run thread
    closerThr = CloserThr(closeLock,closeProxyLock,res,closeThreadPool)
    closerThr.start()

closeThreadPool.join()


# thread to freeze dataset
class Freezer (threading.Thread):
    def __init__(self,lock,proxyLock,datasets,pool):
        threading.Thread.__init__(self)
        self.datasets   = datasets
        self.lock       = lock
        self.proxyLock  = proxyLock
        self.pool       = pool
        self.pool.add(self)
                                        
    def run(self):
        self.lock.acquire()
        try:
            for vuid,name,modDate in self.datasets:
                _logger.debug("start %s %s" % (modDate,name))
                self.proxyLock.acquire()
                retF,resF = taskBuffer.querySQLS("SELECT /*+ index(tab FILESTABLE4_DESTDBLOCK_IDX) */ lfn FROM ATLAS_PANDA.filesTable4 tab WHERE destinationDBlock=:destinationDBlock",
                                             {':destinationDBlock':name})
                self.proxyLock.release()
                if retF<0:
                    _logger.error("SQL error")
                else:
                    # no files in filesTable
                    if len(resF) == 0:
                        _logger.debug("freeze %s " % name)
                        if not name.startswith('pandaddm_'):
                            status,out = ddm.DQ2.main('freezeDataset',name)
                        else:
                            status,out = 0,''
                        if status != 0 and out.find('DQFrozenDatasetException') == -1 and \
                               out.find("DQUnknownDatasetException") == -1 and out.find("DQSecurityException") == -1 and \
                               out.find("DQDeletedDatasetException") == -1 and out.find("DQUnknownDatasetException") == -1:
                            _logger.error(out)
                        else:
                            self.proxyLock.acquire()
                            varMap = {}
                            varMap[':vuid'] = vuid
                            varMap[':status'] = 'completed' 
                            taskBuffer.querySQLS("UPDATE ATLAS_PANDA.Datasets SET status=:status,modificationdate=CURRENT_DATE WHERE vuid=:vuid",
                                             varMap)
                            self.proxyLock.release()                            
                            if name.startswith('pandaddm_'):
                                continue
                            # count # of files
                            status,out = ddm.DQ2.main('getNumberOfFiles',name)
                            _logger.debug(out)                                            
                            if status != 0:
                                _logger.error(out)                            
                            else:
                                try:
                                    nFile = int(out)
                                    _logger.debug(nFile)
                                    if nFile == 0:
                                        # erase dataset
                                        _logger.debug('erase %s' % name)                                
                                        status,out = ddm.DQ2.main('eraseDataset',name)
                                        _logger.debug(out)                                                                
                                except:
                                    pass
                    else:
                        _logger.debug("wait %s " % name)
                        self.proxyLock.acquire()                        
                        taskBuffer.querySQLS("UPDATE ATLAS_PANDA.Datasets SET modificationdate=CURRENT_DATE WHERE vuid=:vuid", {':vuid':vuid})
                        self.proxyLock.release()                                                    
                _logger.debug("end %s " % name)
        except:
            pass
        self.pool.remove(self)
        self.lock.release()
                            
# freeze dataset
timeLimitU = datetime.datetime.utcnow() - datetime.timedelta(days=4)
timeLimitL = datetime.datetime.utcnow() - datetime.timedelta(days=14)
freezeLock = threading.Semaphore(5)
freezeProxyLock = threading.Lock()
freezeThreadPool = ThreadPool()
while True:
    # lock
    freezeLock.acquire()
    # get datasets
    sql = "SELECT vuid,name,modificationdate FROM ATLAS_PANDA.Datasets " + \
          "WHERE type=:type AND status IN (:status1,:status2,:status3) " + \
          "AND (modificationdate BETWEEN :modificationdateL AND :modificationdateU) AND REGEXP_LIKE(name,:pattern) AND rownum <= 20"
    varMap = {}
    varMap[':modificationdateU'] = timeLimitU
    varMap[':modificationdateL'] = timeLimitL    
    varMap[':type'] = 'output'
    varMap[':status1'] = 'running'
    varMap[':status2'] = 'created'
    varMap[':status3'] = 'defined'
    varMap[':pattern'] = '_sub[[:digit:]]+$'
    freezeProxyLock.acquire()
    ret,res = taskBuffer.querySQLS(sql, varMap)
    if res == None:
        _logger.debug("# of datasets to be frozen: %s" % res)
    else:
        _logger.debug("# of datasets to be frozen: %s" % len(res))
    if res==None or len(res)==0:
        freezeProxyLock.release()
        freezeLock.release()
        break
    # update to prevent other process from picking up
    for (vuid,name,modDate) in res:
        taskBuffer.querySQLS("UPDATE ATLAS_PANDA.Datasets SET modificationdate=CURRENT_DATE WHERE vuid=:vuid", {':vuid':vuid})        
    freezeProxyLock.release()            
    # release
    freezeLock.release()
    # run freezer
    freezer = Freezer(freezeLock,freezeProxyLock,res,freezeThreadPool)
    freezer.start()

freezeThreadPool.join()



_memoryCheck("delete XML")

# delete old files in DA cache
timeLimit = datetime.datetime.utcnow() - datetime.timedelta(days=7)
files = os.listdir(panda_config.cache_dir)
for file in files:
    # skip special test file
    if file == 'sources.72c48dc5-f055-43e5-a86e-4ae9f8ea3497.tar.gz':
        continue
    if file == 'sources.090f3f51-fc81-4e80-9749-a5e4b2bd58de.tar.gz':
        continue
    try:
        # get timestamp
        timestamp = datetime.datetime.fromtimestamp(os.stat('%s/%s' % (panda_config.cache_dir,file)).st_mtime)
        # delete
        if timestamp < timeLimit:
            _logger.debug("delete %s " % file)
            os.remove('%s/%s' % (panda_config.cache_dir,file))
    except:
        pass

_memoryCheck("delete core")

# delete core
dirName = '%s/..' % panda_config.logdir
for file in os.listdir(dirName):
    if file.startswith('core.'):
        _logger.debug("delete %s " % file)
        try:
            os.remove('%s/%s' % (dirName,file))
        except:
            pass

_memoryCheck("finisher")

# finish transferring jobs
timeNow   = datetime.datetime.utcnow()
timeLimit = datetime.datetime.utcnow() - datetime.timedelta(hours=12)
sql = "SELECT PandaID FROM ATLAS_PANDA.jobsActive4 WHERE jobStatus=:jobStatus AND modificationTime<:modificationTime AND rownum<=20"
for ii in range(1000):
    varMap = {}
    varMap[':jobStatus'] = 'transferring'
    varMap[':modificationTime'] = timeLimit
    ret,res = taskBuffer.querySQLS(sql, varMap)
    if res == None:
        _logger.debug("# of jobs to be finished : %s" % res)
        break
    else:
        _logger.debug("# of jobs to be finished : %s" % len(res))
        if len(res) == 0:
            break
        # get jobs from DB
        ids = []
        for (id,) in res:
            ids.append(id)
        jobs = taskBuffer.peekJobs(ids,fromDefined=False,fromArchived=False,fromWaiting=False)
        # update modificationTime to lock jobs
        for job in jobs:
            if job != None and job.jobStatus != 'unknown':
                taskBuffer.updateJobStatus(job.PandaID,job.jobStatus,{})
        upJobs = []
        finJobs = []
        for job in jobs:
            if job == None or job.jobStatus == 'unknown':
                continue
            # use BNL by default
            dq2URL = siteMapper.getSite('BNL_ATLAS_1').dq2url
            dq2SE  = []
            # get LFC and SEs
            if job.prodSourceLabel == 'user' and not siteMapper.siteSpecList.has_key(job.destinationSE):
                # using --destSE for analysis job to transfer output
                try:
                    dq2URL = dataservice.DDM.toa.getLocalCatalog(job.destinationSE)[-1]
                    match = re.search('.+://([^:/]+):*\d*/*',dataservice.DDM.toa.getSiteProperty(job.destinationSE,'srm')[-1])
                    if match != None:
                        dq2SE.append(match.group(1))
                except:
                    type, value, traceBack = sys.exc_info()
                    _logger.error("Failed to get DQ2/SE for %s with %s %s" % (job.PandaID,type,value))
                    continue
            elif siteMapper.checkCloud(job.cloud):
                # normal production jobs
                tmpDstID   = siteMapper.getCloud(job.cloud)['dest']
                tmpDstSite = siteMapper.getSite(tmpDstID)
                if not tmpDstSite.lfchost in [None,'']:
                    # LFC
                    dq2URL = 'lfc://'+tmpDstSite.lfchost+':/grid/atlas/'
                    if tmpDstSite.se != None:
                        for tmpDstSiteSE in tmpDstSite.se.split(','):
                            match = re.search('.+://([^:/]+):*\d*/*',tmpDstSiteSE)
                            if match != None:
                                dq2SE.append(match.group(1))
                else:
                    # LRC
                    dq2URL = tmpDstSite.dq2url
                    dq2SE  = []
            # get LFN list
            lfns  = []
            guids = []
            nTokens = 0
            for file in job.Files:
                # only output files are checked
                if file.type == 'output' or file.type == 'log':
                    lfns.append(file.lfn)
                    guids.append(file.GUID)
                    nTokens += len(file.destinationDBlockToken.split(','))
            # get files in LRC
            _logger.debug("Cloud:%s DQ2URL:%s" % (job.cloud,dq2URL))
            okFiles = brokerage.broker_util.getFilesFromLRC(lfns,dq2URL,guids,dq2SE,getPFN=True)
            # count files
            nOkTokens = 0
            for okLFN,okPFNs in okFiles.iteritems():
                nOkTokens += len(okPFNs)
            # check all files are ready    
            _logger.debug(" nToken:%s nOkToken:%s" % (nTokens,nOkTokens))
            if nTokens <= nOkTokens:
                _logger.debug("Finisher : Finish %s" % job.PandaID)
                for file in job.Files:
                    if file.type == 'output' or file.type == 'log':
                        file.status = 'ready'
                # append to run Finisher
                finJobs.append(job)                        
            else:
                endTime = job.endTime
                if endTime == 'NULL':
                    endTime = job.startTime
                # priority-dependent timeout
                tmpCloudSpec = siteMapper.getCloud(job.cloud)
                if job.currentPriority >= 900 and (not job.prodSourceLabel in ['user']):
                    if tmpCloudSpec.has_key('transtimehi'):
                        timeOutValue = tmpCloudSpec['transtimehi']
                    else:
                        timeOutValue = 1
                else:
                    if tmpCloudSpec.has_key('transtimelo'):                    
                        timeOutValue = tmpCloudSpec['transtimelo']
                    else:
                        timeOutValue = 2                        
                # protection
                if timeOutValue < 1:
                    timeOutValue  = 1
                timeOut = timeNow - datetime.timedelta(days=timeOutValue)
                _logger.debug("  Priority:%s Limit:%s End:%s" % (job.currentPriority,str(timeOut),str(endTime)))
                if endTime < timeOut:
                    # timeout
                    _logger.debug("Finisher : Kill %s" % job.PandaID)
                    strMiss = ''
                    for lfn in lfns:
                        if not lfn in okFiles:
                            strMiss += ' %s' % lfn
                    job.jobStatus = 'failed'
                    job.taskBufferErrorCode = taskbuffer.ErrorCode.EC_Transfer
                    job.taskBufferErrorDiag = 'transfer timeout for '+strMiss
                    guidMap = {}
                    for file in job.Files:
                        # set file status
                        if file.status == 'transferring':
                            file.status = 'failed'
                        # collect GUIDs to delete files from _tid datasets
                        if file.type == 'output' or file.type == 'log':
                            if not guidMap.has_key(file.destinationDBlock):
                                guidMap[file.destinationDBlock] = []
                            guidMap[file.destinationDBlock].append(file.GUID)
                else:
                    # wait
                    _logger.debug("Finisher : Wait %s" % job.PandaID)
                    for lfn in lfns:
                        if not lfn in okFiles:
                            _logger.debug("    -> %s" % lfn)
            upJobs.append(job)
            time.sleep(2)
        # update
        _logger.debug("updating ...")
        taskBuffer.updateJobs(upJobs,False)
        # run Finisher
        for job in finJobs:
            fThr = Finisher(taskBuffer,None,job)
            fThr.start()
            fThr.join()
        _logger.debug("done")

# update email DB        
_memoryCheck("email")
_logger.debug("Update emails")

# lock file
_lockGetMail = open(panda_config.lockfile_getMail, 'w')
# lock email DB
fcntl.flock(_lockGetMail.fileno(), fcntl.LOCK_EX)
# open email DB
pDB = shelve.open(panda_config.emailDB)
# read
mailMap = {}
for name,addr in pDB.iteritems():
    mailMap[name] = addr
# close DB
pDB.close()
# release file lock
fcntl.flock(_lockGetMail.fileno(), fcntl.LOCK_UN)
# set email address
for name,addr in mailMap.iteritems():
    # remove _
    name = re.sub('_$','',name)
    status,res = taskBuffer.querySQLS("SELECT email FROM ATLAS_PANDAMETA.users WHERE name=:name",{':name':name})
    # failed or not found
    if status == -1 or len(res) == 0:
        _logger.error("%s not found in user DB" % name)
        continue
    # already set
    if not res[0][0] in ['','None',None]:
        continue
    # update email
    _logger.debug("set '%s' to %s" % (name,addr))
    status,res = taskBuffer.querySQLS("UPDATE ATLAS_PANDAMETA.users SET email=:addr WHERE name=:name",{':addr':addr,':name':name})

# reassign reprocessing jobs in defined table
_memoryCheck("repro")
class ReassginRepro (threading.Thread):
    def __init__(self,taskBuffer,lock,jobs):
        threading.Thread.__init__(self)
        self.jobs       = jobs
        self.lock       = lock
        self.taskBuffer = taskBuffer

    def run(self):
        self.lock.acquire()
        try:
            if len(self.jobs):
                nJob = 100
                iJob = 0
                while iJob < len(self.jobs):
                    eraseDispDatasets(self.jobs[iJob:iJob+nJob])
                    # reassign jobs one by one to break dis dataset formation
                    for job in self.jobs[iJob:iJob+nJob]:
                        _logger.debug('reassignJobs in Pepro (%s)' % [job])
                        self.taskBuffer.reassignJobs([job],joinThr=True,forkSetupper=True)
                    iJob += nJob
        except:
            pass
        self.lock.release()
        
reproLock = threading.Semaphore(3)

nBunch = 20
iBunch = 0
timeLimitMod = datetime.datetime.utcnow() - datetime.timedelta(hours=8)
timeLimitCre = datetime.datetime.utcnow() - datetime.timedelta(hours=24)
firstFlag = True
while True:
    # lock
    reproLock.acquire()
    # get jobs
    varMap = {}
    varMap[':jobStatus'] = 'assigned'
    varMap[':prodSourceLabel'] = 'managed'
    varMap[':modificationTime'] = timeLimitMod
    varMap[':creationTime'] = timeLimitCre
    varMap[':processingType'] = 'reprocessing'
    if firstFlag:
        firstFlag = False
        status,res = taskBuffer.querySQLS("SELECT PandaID FROM ATLAS_PANDA.jobsDefined4 WHERE jobStatus=:jobStatus AND prodSourceLabel=:prodSourceLabel AND modificationTime<:modificationTime AND creationTime<:creationTime AND processingType=:processingType ORDER BY PandaID",
                                      varMap)
        if res != None:
            _logger.debug('total Repro for reassignJobs : %s' % len(res))
    # get a bunch    
    status,res = taskBuffer.querySQLS("SELECT * FROM (SELECT PandaID FROM ATLAS_PANDA.jobsDefined4 WHERE jobStatus=:jobStatus AND prodSourceLabel=:prodSourceLabel AND modificationTime<:modificationTime AND creationTime<:creationTime AND processingType=:processingType ORDER BY PandaID) WHERE rownum<=%s" % nBunch,
                                  varMap)
    # escape
    if res == None or len(res) == 0:
        reproLock.release()
        break

    # get IDs
    jobs=[]
    for id, in res:
        jobs.append(id)
        
    # reassign
    _logger.debug('reassignJobs for Pepro %s' % (iBunch*nBunch))
    # lock
    currentTime = datetime.datetime.utcnow()
    for jobID in jobs:
        varMap = {}
        varMap[':PandaID'] = jobID
        varMap[':modificationTime'] = currentTime
        status,res = taskBuffer.querySQLS("UPDATE ATLAS_PANDA.jobsDefined4 SET modificationTime=:modificationTime WHERE PandaID=:PandaID",
                                          varMap)
    reproLock.release()
    # run thr
    reproThr = ReassginRepro(taskBuffer,reproLock,jobs)
    reproThr.start()
    iBunch += 1

_memoryCheck("end")

_logger.debug("===================== end =====================")
