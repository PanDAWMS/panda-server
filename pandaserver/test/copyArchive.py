import os
import re
import sys
import time
import fcntl
import shelve
import datetime
import pandaserver.userinterface.Client as Client
from pandaserver.taskbuffer.TaskBuffer import taskBuffer
from pandaserver.taskbuffer import EventServiceUtils
from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandaserver.jobdispatcher.Watcher import Watcher
from pandaserver.brokerage.SiteMapper import SiteMapper
from pandaserver.dataservice.MailUtils import MailUtils
from pandaserver.srvcore.CoreUtils import commands_get_status_output

# password
from pandaserver.config import panda_config

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
    except Exception:
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
    out = commands_get_status_output(
        'ps axo user,pid,lstart,args | grep dq2.clientapi | grep -v PYTHONPATH | grep -v grep')[-1]
    for line in out.split('\n'):
        if line == '':
            continue
        items = line.split()
        # owned process
        if not items[0] in ['sm','atlpan','pansrv','root']: # ['os.getlogin()']: doesn't work in cron
            continue
        # look for python
        if re.search('python',line) is None:
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
            commands_get_status_output('kill -9 %s' % pid)
except Exception:
    type, value, traceBack = sys.exc_info()
    _logger.error("kill dq2 process : %s %s" % (type,value))


# kill old process
try:
    # time limit
    timeLimit = datetime.datetime.utcnow() - datetime.timedelta(hours=7)
    # get process list
    scriptName = sys.argv[0]
    out = commands_get_status_output('ps axo user,pid,lstart,args | grep %s' % scriptName)[-1]
    for line in out.split('\n'):
        items = line.split()
        # owned process
        if not items[0] in ['sm','atlpan','pansrv','root']: # ['os.getlogin()']: doesn't work in cron
            continue
        # look for python
        if re.search('python',line) is None:
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
            commands_get_status_output('kill -9 %s' % pid)
except Exception:
    type, value, traceBack = sys.exc_info()
    _logger.error("kill process : %s %s" % (type,value))
    

# instantiate TB
taskBuffer.init(panda_config.dbhost,panda_config.dbpasswd,nDBConnection=1)

# instantiate sitemapper
siteMapper = SiteMapper(taskBuffer)



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
            if resUM is None or len(resUM) == 0:
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
            requestsInCloud.setdefault(cloud, {})
            # append site
            requestsInCloud[cloud].setdefault(pandaSite, [])
            # append user
            requestsInCloud[cloud][pandaSite].append(userName)
    # send requests to the cloud responsible
    for cloud in requestsInCloud:
        requestsMap = requestsInCloud[cloud]
        _logger.debug("requests for approval : cloud=%s" % cloud)
        # send
        if cloud in contactAddr and contactAddr[cloud] not in ['',None,'None']:
            # get site contact
            for pandaSite in requestsMap:
                userNames = requestsMap[pandaSite]
                if pandaSite not in siteContactAddr:
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
                for pandaSite in requestsMap:
                    userNames = requestsMap[pandaSite]
                    for userName in userNames:
                        varMap = {}
                        varMap[':userName']  = userName
                        varMap[':newStatus'] = 'inprocess'
                        varMap[':pandaSite'] = pandaSite
                        stUp,resUp = taskBuffer.querySQLS(sqlUp,varMap)
        else:
            _logger.error("contact email address is unavailable for %s" % cloud)
except Exception:
    type, value, traceBack = sys.exc_info()
    _logger.error("Failed with %s %s" % (type,value))
_logger.debug("Site Access : done")    


# finalize failed jobs
_logger.debug("AnalFinalizer session")
try:
    # get min PandaID for failed jobs in Active table
    sql  = "SELECT MIN(PandaID),prodUserName,jobDefinitionID,jediTaskID,computingSite FROM ATLAS_PANDA.jobsActive4 "
    sql += "WHERE prodSourceLabel=:prodSourceLabel AND jobStatus=:jobStatus "
    sql += "GROUP BY prodUserName,jobDefinitionID,jediTaskID,computingSite "
    varMap = {}
    varMap[':jobStatus']       = 'failed'
    varMap[':prodSourceLabel'] = 'user'
    status,res = taskBuffer.querySQLS(sql,varMap)
    if res is not None:
        # loop over all user/jobdefID
        for pandaID,prodUserName,jobDefinitionID,jediTaskID,computingSite in res:
            # check
            _logger.debug("check finalization for %s task=%s jobdefID=%s site=%s" % (prodUserName,jediTaskID,
                                                                                     jobDefinitionID,
                                                                                     computingSite))
            sqlC  = "SELECT COUNT(*) FROM ("
            sqlC += "SELECT PandaID FROM ATLAS_PANDA.jobsActive4 "
            sqlC += "WHERE prodSourceLabel=:prodSourceLabel AND prodUserName=:prodUserName "
            sqlC += "AND jediTaskID=:jediTaskID "
            sqlC += "AND computingSite=:computingSite "
            sqlC += "AND NOT jobStatus IN (:jobStatus1,:jobStatus2) "
            sqlC += "UNION "
            sqlC += "SELECT PandaID FROM ATLAS_PANDA.jobsDefined4 "
            sqlC += "WHERE prodSourceLabel=:prodSourceLabel AND prodUserName=:prodUserName "
            sqlC += "AND jediTaskID=:jediTaskID "
            sqlC += "AND computingSite=:computingSite "
            sqlC += "AND NOT jobStatus IN (:jobStatus1,:jobStatus2) "
            sqlC += ") "
            varMap = {}
            varMap[':jobStatus1']      = 'failed'
            varMap[':jobStatus2']      = 'merging'
            varMap[':prodSourceLabel'] = 'user'
            varMap[':jediTaskID']      = jediTaskID
            varMap[':computingSite']   = computingSite
            varMap[':prodUserName']    = prodUserName
            statC,resC = taskBuffer.querySQLS(sqlC,varMap)
            # finalize if there is no non-failed jobs
            if resC is not None:
                _logger.debug("n of non-failed jobs : %s" % resC[0][0])
                if resC[0][0] == 0:
                    jobSpecs = taskBuffer.peekJobs([pandaID],fromDefined=False,fromArchived=False,fromWaiting=False)
                    jobSpec = jobSpecs[0]
                    if jobSpec is None:
                        _logger.debug("skip PandaID={0} not found in jobsActive".format(pandaID))
                        continue
                    _logger.debug("finalize %s %s" % (prodUserName,jobDefinitionID)) 
                    finalizedFlag = taskBuffer.finalizePendingJobs(prodUserName,jobDefinitionID)
                    _logger.debug("finalized with %s" % finalizedFlag)
                    if finalizedFlag and jobSpec.produceUnMerge():
                        # collect sub datasets
                        subDsNames = set()
                        subDsList = []
                        for tmpFileSpec in jobSpec.Files:
                            if tmpFileSpec.type in ['log','output'] and \
                                    re.search('_sub\d+$',tmpFileSpec.destinationDBlock) is not None:
                                if tmpFileSpec.destinationDBlock in subDsNames:
                                    continue
                                subDsNames.add(tmpFileSpec.destinationDBlock)
                                datasetSpec = taskBuffer.queryDatasetWithMap({'name':tmpFileSpec.destinationDBlock})
                                subDsList.append(datasetSpec)
                        _logger.debug("update unmerged datasets")
                        taskBuffer.updateUnmergedDatasets(jobSpec,subDsList)
            else:
                _logger.debug("n of non-failed jobs : None")
except Exception:
    errType,errValue = sys.exc_info()[:2]
    _logger.error("AnalFinalizer failed with %s %s" % (errType,errValue))

# finalize failed jobs
_logger.debug("check stuck mergeing jobs")
try:
    # get PandaIDs
    varMap = {}
    varMap[':prodSourceLabel'] = 'managed'
    varMap[':jobStatus'] = 'merging'
    varMap[':timeLimit'] = timeLimit
    sql  = "SELECT distinct jediTaskID FROM ATLAS_PANDA.jobsActive4 "
    sql += "WHERE prodSourceLabel=:prodSourceLabel AND jobStatus=:jobStatus and modificationTime<:timeLimit "
    tmp,res = taskBuffer.querySQLS(sql,varMap)
    checkedDS = set()
    for jediTaskID, in res:
        varMap = {}
        varMap[':jediTaskID'] = jediTaskID
        varMap[':dsType'] = 'trn_log'
        sql = "SELECT datasetID FROM ATLAS_PANDA.JEDI_Datasets WHERE jediTaskID=:jediTaskID AND type=:dsType AND nFilesUsed=nFilesTobeUsed "
        tmpP,resD = taskBuffer.querySQLS(sql,varMap)
        for datasetID, in resD:
            varMap = {}
            varMap[':jediTaskID'] = jediTaskID
            varMap[':fileStatus'] = 'ready'
            varMap[':datasetID'] = datasetID
            sql  = "SELECT PandaID FROM ATLAS_PANDA.JEDI_Dataset_Contents "
            sql += "WHERE jediTaskID=:jediTaskID AND datasetid=:datasetID AND status=:fileStatus AND PandaID=OutPandaID AND rownum<=1 "
            tmpP,resP = taskBuffer.querySQLS(sql,varMap)
            if resP == []:
                continue
            PandaID = resP[0][0]
            varMap = {}
            varMap[':PandaID'] = PandaID
            varMap[':fileType'] = 'log'
            sql = "SELECT d.status FROM ATLAS_PANDA.filesTable4 f,ATLAS_PANDA.datasets d WHERE PandaID=:PandaID AND f.type=:fileType AND d.name=f.destinationDBlock "
            tmpS,resS = taskBuffer.querySQLS(sql,varMap)
            if resS is not None:
                subStatus, = resS[0]
                if subStatus in ['completed']:
                    jobSpecs = taskBuffer.peekJobs([PandaID],fromDefined=False,fromArchived=False,fromWaiting=False)
                    jobSpec = jobSpecs[0]
                    subDsNames = set()
                    subDsList = []
                    for tmpFileSpec in jobSpec.Files:
                        if tmpFileSpec.type in ['log','output'] and \
                                re.search('_sub\d+$',tmpFileSpec.destinationDBlock) is not None:
                            if tmpFileSpec.destinationDBlock in subDsNames:
                                continue
                            subDsNames.add(tmpFileSpec.destinationDBlock)
                            datasetSpec = taskBuffer.queryDatasetWithMap({'name':tmpFileSpec.destinationDBlock})
                            subDsList.append(datasetSpec)
                    _logger.debug("update unmerged datasets for jediTaskID={0} PandaID={1}".format(jediTaskID,PandaID))
                    taskBuffer.updateUnmergedDatasets(jobSpec,subDsList,updateCompleted=True)
except Exception:
    errType,errValue = sys.exc_info()[:2]
    _logger.error("check for stuck merging jobs failed with %s %s" % (errType,errValue))

# get sites to skip various timeout
varMap = {}
varMap[':status'] = 'paused'
sql = "SELECT siteid FROM ATLAS_PANDAMETA.schedconfig WHERE status=:status "
sitesToSkipTO = set()
status,res = taskBuffer.querySQLS(sql,varMap)
for siteid, in res:
    sitesToSkipTO.add(siteid)

_logger.debug("PQs to skip timeout : {0}".format(','.join(sitesToSkipTO)))
    
_memoryCheck("watcher")

_logger.debug("Watcher session")

# get the list of workflows
sql = "SELECT DISTINCT workflow FROM ATLAS_PANDAMETA.schedconfig WHERE status='online' "
status, res = taskBuffer.querySQLS(sql, {})
workflow_timeout_map = {}
for workflow, in res:
    timeout = taskBuffer.getConfigValue('watcher', 'HEARTBEAT_TIMEOUT_{0}'.format(workflow), 'pandaserver', 'atlas')
    if timeout is not None:
        workflow_timeout_map[workflow] = timeout
workflows = list(workflow_timeout_map)
workflows.append(None)

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
sql += "AND jobStatus IN (:jobStatus1,:jobStatus2,:jobStatus3,:jobStatus4) AND modificationTime<:modificationTime"
status,res = taskBuffer.querySQLS(sql,varMap)
if res is None:
    _logger.debug("# of Anal Watcher : %s" % res)
else:
    _logger.debug("# of Anal Watcher : %s" % len(res))    
    for (id,) in res:
        _logger.debug("Anal Watcher %s" % id)    
        thr = Watcher(taskBuffer,id,single=True,sleepTime=60,sitemapper=siteMapper)
        thr.start()
        thr.join()

# check heartbeat for analysis jobs in transferring
timeLimit = datetime.datetime.utcnow() - datetime.timedelta(hours=2)
varMap = {}
varMap[':modificationTime'] = timeLimit
varMap[':prodSourceLabel1'] = 'panda'
varMap[':prodSourceLabel2'] = 'user'
varMap[':jobStatus1'] = 'transferring'
sql  = "SELECT PandaID FROM ATLAS_PANDA.jobsActive4 WHERE prodSourceLabel IN (:prodSourceLabel1,:prodSourceLabel2) "
sql += "AND jobStatus=:jobStatus1 AND modificationTime<:modificationTime"
status,res = taskBuffer.querySQLS(sql,varMap)
if res is None:
    _logger.debug("# of Transferring Anal Watcher : %s" % res)
else:
    _logger.debug("# of Transferring Anal Watcher : %s" % len(res))    
    for (id,) in res:
        _logger.debug("Trans Anal Watcher %s" % id)    
        thr = Watcher(taskBuffer,id,single=True,sleepTime=60,sitemapper=siteMapper)
        thr.start()
        thr.join()

# check heartbeat for sent jobs
timeLimit = datetime.datetime.utcnow() - datetime.timedelta(minutes=30)
varMap = {}
varMap[':jobStatus'] = 'sent'
varMap[':modificationTime'] = timeLimit
status,res = taskBuffer.querySQLS("SELECT PandaID FROM ATLAS_PANDA.jobsActive4 WHERE jobStatus=:jobStatus AND modificationTime<:modificationTime",
                              varMap)
if res is None:
    _logger.debug("# of Sent Watcher : %s" % res)
else:
    _logger.debug("# of Sent Watcher : %s" % len(res))
    for (id,) in res:
        _logger.debug("Sent Watcher %s" % id)        
        thr = Watcher(taskBuffer,id,single=True,sleepTime=30,sitemapper=siteMapper)
        thr.start()
        thr.join()

# check heartbeat for 'holding' analysis/ddm jobs
timeLimit = datetime.datetime.utcnow() - datetime.timedelta(hours=3)
# get XMLs
xmlIDs = []
xmlFiles = os.listdir(panda_config.logdir)
for file in xmlFiles:
    match = re.search('^(\d+)_([^_]+)_.{36}$',file)
    if match is not None:
        id = match.group(1)
        xmlIDs.append(int(id))
sql = "SELECT PandaID FROM ATLAS_PANDA.jobsActive4 WHERE jobStatus=:jobStatus AND (modificationTime<:modificationTime OR (endTime IS NOT NULL AND endTime<:endTime)) AND (prodSourceLabel=:prodSourceLabel1 OR prodSourceLabel=:prodSourceLabel2 OR prodSourceLabel=:prodSourceLabel3) AND stateChangeTime != modificationTime"
varMap = {}
varMap[':modificationTime'] = timeLimit
varMap[':endTime'] = timeLimit
varMap[':jobStatus'] = 'holding'
varMap[':prodSourceLabel1'] = 'panda'
varMap[':prodSourceLabel2'] = 'user'
varMap[':prodSourceLabel3'] = 'ddm'
status,res = taskBuffer.querySQLS(sql,varMap)
if res is None:
    _logger.debug("# of Holding Anal/DDM Watcher : %s" % res)
else:
    _logger.debug("# of Holding Anal/DDM Watcher : %s - XMLs : %s" % (len(res),len(xmlIDs)))
    for (id,) in res:
        _logger.debug("Holding Anal/DDM Watcher %s" % id)
        if int(id) in xmlIDs:
            _logger.debug("   found XML -> skip %s" % id)
            continue
        thr = Watcher(taskBuffer,id,single=True,sleepTime=180,sitemapper=siteMapper)
        thr.start()
        thr.join()


# check heartbeat for high prio production jobs
timeOutVal = 3
timeLimit = datetime.datetime.utcnow() - datetime.timedelta(hours=timeOutVal)
sql  = "SELECT PandaID FROM ATLAS_PANDA.jobsActive4 WHERE jobStatus=:jobStatus AND currentPriority>:pLimit "
sql += "AND (modificationTime<:modificationTime OR (endTime IS NOT NULL AND endTime<:endTime))"
varMap = {}
varMap[':modificationTime'] = timeLimit
varMap[':endTime'] = timeLimit
varMap[':jobStatus'] = 'holding'
varMap[':pLimit'] = 800
status,res = taskBuffer.querySQLS(sql,varMap)
if res is None:
    _logger.debug("# of High prio Holding Watcher : %s" % res)
else:
    _logger.debug("# of High prio Holding Watcher : %s" % len(res))    
    for (id,) in res:
        _logger.debug("High prio Holding Watcher %s" % id)
        thr = Watcher(taskBuffer,id,single=True,sleepTime=60*timeOutVal,sitemapper=siteMapper)
        thr.start()
        thr.join()

# check heartbeat for production jobs
timeOutVal = 48
timeLimit = datetime.datetime.utcnow() - datetime.timedelta(hours=timeOutVal)
sql = "SELECT PandaID FROM ATLAS_PANDA.jobsActive4 WHERE jobStatus=:jobStatus AND (modificationTime<:modificationTime OR (endTime IS NOT NULL AND endTime<:endTime))"
varMap = {}
varMap[':modificationTime'] = timeLimit
varMap[':endTime'] = timeLimit
varMap[':jobStatus'] = 'holding'
status,res = taskBuffer.querySQLS(sql,varMap)
if res is None:
    _logger.debug("# of Holding Watcher : %s" % res)
else:
    _logger.debug("# of Holding Watcher : %s" % len(res))    
    for (id,) in res:
        _logger.debug("Holding Watcher %s" % id)
        thr = Watcher(taskBuffer,id,single=True,sleepTime=60*timeOutVal,sitemapper=siteMapper)
        thr.start()
        thr.join()

# check heartbeat for production jobs with internal stage-out
default_timeOutVal = 4
sql = "SELECT PandaID,jobStatus,jobSubStatus FROM ATLAS_PANDA.jobsActive4 j,ATLAS_PANDAMETA.schedconfig s "
sql += "WHERE j.computingSite=s.siteid AND jobStatus=:jobStatus1 AND jobSubStatus IS NOT NULL AND modificationTime<:modificationTime "
for workflow in workflows:
    varMap = {}
    varMap[':modificationTime'] = timeLimit
    varMap[':jobStatus1'] = 'transferring'
    sqlX = sql
    if workflow is None:
        timeOutVal = default_timeOutVal
        if len(workflows) > 1:
            sqlX += "AND (s.workflow IS NULL OR s.workflow NOT IN ("
            for ng_workflow in workflows[:-1]:
                tmp_key = ':w_{0}'.format(ng_workflow)
                varMap[tmp_key] = ng_workflow
                sqlX += '{0},'.format(tmp_key)
            sqlX = sqlX[:-1]
            sqlX += ")) "
    else:
        timeOutVal = workflow_timeout_map[workflow]
        tmp_key = ':w_{0}'.format(workflow)
        sqlX += "AND s.workflow={0} ".format(tmp_key)
        varMap[tmp_key] = workflow
    timeLimit = datetime.datetime.utcnow() - datetime.timedelta(hours=timeOutVal)
    varMap[':modificationTime'] = timeLimit
    status,res = taskBuffer.querySQLS(sqlX, varMap)
    if res is None:
        _logger.debug("# of Internal Staging Watcher with workflow={0}: {1}".format(workflow, res))
    else:
        _logger.debug("# of Internal Staging Watcher with workflow={0}: {1}".format(workflow, len(res)))
        for pandaID, jobStatus, jobSubStatus in res:
            _logger.debug("Internal Staging  Watcher %s %s:%s" % (pandaID, jobStatus, jobSubStatus))
            thr = Watcher(taskBuffer,pandaID,single=True,sleepTime=60*timeOutVal,sitemapper=siteMapper)
            thr.start()
            thr.join()

# check heartbeat for production jobs
default_timeOutVal = 2
sql = "SELECT PandaID,jobStatus,j.computingSite FROM ATLAS_PANDA.jobsActive4 j, ATLAS_PANDAMETA.schedconfig s "
sql += "WHERE j.computingSite=s.siteid AND jobStatus IN (:jobStatus1,:jobStatus2,:jobStatus3,:jobStatus4) AND modificationTime<:modificationTime "
for workflow in workflows:
    varMap = {}
    varMap[':jobStatus1'] = 'running'
    varMap[':jobStatus2'] = 'starting'
    varMap[':jobStatus3'] = 'stagein'
    varMap[':jobStatus4'] = 'stageout'
    sqlX = sql
    if workflow is None:
        timeOutVal = default_timeOutVal
        if len(workflows) > 1:
            sqlX += "AND (s.workflow IS NULL OR s.workflow NOT IN ("
            for ng_workflow in workflows[:-1]:
                tmp_key = ':w_{0}'.format(ng_workflow)
                varMap[tmp_key] = ng_workflow
                sqlX += '{0},'.format(tmp_key)
            sqlX = sqlX[:-1]
            sqlX += ")) "
    else:
        timeOutVal = workflow_timeout_map[workflow]
        tmp_key = ':w_{0}'.format(workflow)
        sqlX += "AND s.workflow={0} ".format(tmp_key)
        varMap[tmp_key] = workflow
    timeLimit = datetime.datetime.utcnow() - datetime.timedelta(hours=timeOutVal)
    varMap[':modificationTime'] = timeLimit
    status,res = taskBuffer.querySQLS(sqlX, varMap)
    if res is None:
        _logger.debug("# of General Watcher with workflow={0}: {1}".format(workflow, res))
    else:
        _logger.debug("# of General Watcher with workflow={0}: {1}".format(workflow, len(res)))
        for pandaID,jobStatus,computingSite in res:
            if computingSite in sitesToSkipTO:
                _logger.debug("skip General Watcher for PandaID={0} at {1} since timeout is disabled for {2}".format(pandaID,computingSite,jobStatus))
                continue
            _logger.debug("General Watcher %s" % pandaID)
            thr = Watcher(taskBuffer,pandaID,single=True,sleepTime=60*timeOutVal,sitemapper=siteMapper)
            thr.start()
            thr.join()

_memoryCheck("reassign")

# kill long-waiting jobs in defined table
timeLimit = datetime.datetime.utcnow() - datetime.timedelta(days=7)
status,res = taskBuffer.querySQLS("SELECT PandaID,cloud,prodSourceLabel FROM ATLAS_PANDA.jobsDefined4 WHERE creationTime<:creationTime",
                              {':creationTime':timeLimit})
jobs=[]
dashFileMap = {}
if res is not None:
    for pandaID,cloud,prodSourceLabel in res:
        # collect PandaIDs
        jobs.append(pandaID)
if len(jobs):
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
if res is not None:
    for (id,) in res:
        jobs.append(id)
if len(jobs):
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
if res is not None:
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
if res is not None:
    for id, in res:
        movers.append(id)
        # get dispatch dataset
        sql = 'SELECT name FROM ATLAS_PANDA.Datasets WHERE MoverID=:MoverID'
        stDS,resDS = taskBuffer.querySQLS(sql,{':MoverID':id})
        if resDS is not None:
            disDS = resDS[0][0]
            # get PandaIDs associated to the dis dataset
            sql = "SELECT PandaID FROM ATLAS_PANDA.jobsDefined4 WHERE jobStatus=:jobStatus AND dispatchDBlock=:dispatchDBlock"
            varMap = {}
            varMap[':jobStatus'] = 'assigned'
            varMap[':dispatchDBlock'] = disDS
            stP,resP = taskBuffer.querySQLS(sql,varMap)
            if resP is not None:
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
        taskBuffer.reassignJobs(jobs[iJob:iJob+nJob],joinThr=True)
        iJob += nJob

# reassign activated jobs in inactive sites
inactiveTimeLimitSite = 2
inactiveTimeLimitJob  = 4
inactivePrioLimit = 800
timeLimitSite = datetime.datetime.utcnow() - datetime.timedelta(hours=inactiveTimeLimitSite)
timeLimitJob  = datetime.datetime.utcnow() - datetime.timedelta(hours=inactiveTimeLimitJob)
# get PandaIDs
sql  = 'SELECT distinct computingSite FROM ATLAS_PANDA.jobsActive4 '
sql += 'WHERE prodSourceLabel=:prodSourceLabel '
sql += 'AND ((modificationTime<:timeLimit AND jobStatus=:jobStatus1) '
sql += 'OR (stateChangeTime<:timeLimit AND jobStatus=:jobStatus2)) '
sql += 'AND lockedby=:lockedby AND currentPriority>=:prioLimit '
sql += 'AND NOT processingType IN (:pType1) AND relocationFlag<>:rFlag1 '
varMap = {}
varMap[':prodSourceLabel'] = 'managed'
varMap[':jobStatus1'] = 'activated'
varMap[':jobStatus2'] = 'starting'
varMap[':lockedby']  = 'jedi'
varMap[':timeLimit'] = timeLimitJob
varMap[':prioLimit'] = inactivePrioLimit
varMap[':pType1']    = 'pmerge'
varMap[':rFlag1']    = 2
stDS,resDS = taskBuffer.querySQLS(sql,varMap)
sqlSS  = 'SELECT laststart FROM ATLAS_PANDAMETA.siteData '
sqlSS += 'WHERE site=:site AND flag=:flag AND hours=:hours AND laststart<:laststart '
sqlPI  = 'SELECT PandaID,eventService,attemptNr FROM ATLAS_PANDA.jobsActive4 '
sqlPI += 'WHERE prodSourceLabel=:prodSourceLabel AND jobStatus IN (:jobStatus1,:jobStatus2) '
sqlPI += 'AND (modificationTime<:timeLimit OR stateChangeTime<:timeLimit) '
sqlPI += 'AND lockedby=:lockedby AND currentPriority>=:prioLimit '
sqlPI += 'AND computingSite=:site AND NOT processingType IN (:pType1) AND relocationFlag<>:rFlag1 '
for tmpSite, in resDS:
    if tmpSite in sitesToSkipTO:
        _logger.debug('skip reassignJobs at inactive site %s since timeout is disabled' % (tmpSite))
        continue
    # check if the site is inactive
    varMap = {}
    varMap[':site']  = tmpSite
    varMap[':flag']  = 'production'
    varMap[':hours'] = 3
    varMap[':laststart'] = timeLimitSite
    stSS,resSS = taskBuffer.querySQLS(sqlSS,varMap)
    if stSS is not None and len(resSS) > 0:
        # get jobs
        varMap = {}
        varMap[':prodSourceLabel'] = 'managed'
        varMap[':jobStatus1'] = 'activated'
        varMap[':jobStatus2'] = 'starting'
        varMap[':lockedby']  = 'jedi'
        varMap[':timeLimit'] = timeLimitJob
        varMap[':prioLimit'] = inactivePrioLimit
        varMap[':site']      = tmpSite
        varMap[':pType1']    = 'pmerge'
        varMap[':rFlag1']    = 2
        stPI,resPI = taskBuffer.querySQLS(sqlPI,varMap)
        jediJobs = []
        # reassign
        _logger.debug('reassignJobs for JEDI at inactive site %s laststart=%s' % (tmpSite,resSS[0][0]))
        if resPI is not None:
            for pandaID, eventService, attemptNr in resPI:
                if eventService in [EventServiceUtils.esMergeJobFlagNumber]:
                    _logger.debug('retrying es merge %s at inactive site %s' % (pandaID,tmpSite))
                    taskBuffer.retryJob(pandaID,{},getNewPandaID=True,attemptNr=attemptNr,
                                                 recoverableEsMerge=True)
                jediJobs.append(pandaID)
        if len(jediJobs) != 0:
            nJob = 100
            iJob = 0
            while iJob < len(jediJobs):
                _logger.debug('reassignJobs for JEDI at inactive site %s (%s)' % (tmpSite,jediJobs[iJob:iJob+nJob]))
                Client.killJobs(jediJobs[iJob:iJob+nJob],51,keepUnmerged=True)
                iJob += nJob

# reassign defined jobs in defined table
timeLimit = datetime.datetime.utcnow() - datetime.timedelta(hours=4)
# get PandaIDs
status,res = taskBuffer.lockJobsForReassign("ATLAS_PANDA.jobsDefined4",timeLimit,['defined'],['managed'],[],[],[],
                                            True)
jobs = []
jediJobs = []
if res is not None:
    for (id,lockedby) in res:
        if lockedby == 'jedi':
            jediJobs.append(id)
        else:
            jobs.append(id)
# reassign
_logger.debug('reassignJobs for defined jobs -> #%s' % len(jobs))
if len(jobs) > 0:
    nJob = 100
    iJob = 0
    while iJob < len(jobs):
        _logger.debug('reassignJobs for defined jobs (%s)' % jobs[iJob:iJob+nJob])
        taskBuffer.reassignJobs(jobs[iJob:iJob+nJob],joinThr=True)
        iJob += nJob
_logger.debug('reassignJobs for JEDI defined jobs -> #%s' % len(jediJobs))
if len(jediJobs) != 0:
    nJob = 100
    iJob = 0
    while iJob < len(jediJobs):
        _logger.debug('reassignJobs for JEDI defined jobs (%s)' % jediJobs[iJob:iJob+nJob])
        Client.killJobs(jediJobs[iJob:iJob+nJob],51,keepUnmerged=True)
        iJob += nJob

# reassign long-waiting jobs in defined table
timeLimit = datetime.datetime.utcnow() - datetime.timedelta(hours=12)
status,res = taskBuffer.lockJobsForReassign("ATLAS_PANDA.jobsDefined4",timeLimit,[],['managed'],[],[],[],
                                            True)
jobs = []
jediJobs = []
if res is not None:
    for (id,lockedby) in res:
        if lockedby == 'jedi':
            jediJobs.append(id)
        else:
            jobs.append(id)
# reassign
_logger.debug('reassignJobs for long in defined table -> #%s' % len(jobs))
if len(jobs) > 0:
    nJob = 100
    iJob = 0
    while iJob < len(jobs):
        _logger.debug('reassignJobs for long in defined table (%s)' % jobs[iJob:iJob+nJob])
        taskBuffer.reassignJobs(jobs[iJob:iJob+nJob],joinThr=True)
        iJob += nJob
_logger.debug('reassignJobs for long JEDI in defined table -> #%s' % len(jediJobs))
if len(jediJobs) != 0:
    nJob = 100
    iJob = 0
    while iJob < len(jediJobs):
        _logger.debug('reassignJobs for long JEDI in defined table (%s)' % jediJobs[iJob:iJob+nJob])
        Client.killJobs(jediJobs[iJob:iJob+nJob],51,keepUnmerged=True)
        iJob += nJob


# reassign too long-standing evgen/simul jobs with active state at T1
timeLimit = datetime.datetime.utcnow() - datetime.timedelta(hours=6)
for tmpCloud in siteMapper.getCloudList():
    # ignore special clouds
    if tmpCloud in ['CERN','OSG']:
        continue
    status,res = taskBuffer.lockJobsForReassign("ATLAS_PANDA.jobsActive4",timeLimit,['activated'],['managed'],
                                                ['evgen','simul'],[siteMapper.getCloud(tmpCloud)['tier1']],[],
                                                True,onlyReassignable=True)
    jobs = []
    jediJobs = []
    if res is not None:
        for (id,lockedby) in res:
            if lockedby == 'jedi':
                jediJobs.append(id)
            else:
                jobs.append(id)
    _logger.debug('reassignJobs for Active T1 evgensimul in %s -> #%s' % (tmpCloud,len(jobs)))
    if len(jobs) != 0:
        nJob = 100
        iJob = 0
        while iJob < len(jobs):
            _logger.debug('reassignJobs for Active T1 evgensimul (%s)' % jobs[iJob:iJob+nJob])
            taskBuffer.reassignJobs(jobs[iJob:iJob+nJob],joinThr=True)
            iJob += nJob
    _logger.debug('reassignJobs for Active T1 JEDI evgensimul in %s -> #%s' % (tmpCloud,len(jediJobs)))
    if len(jediJobs) != 0:
        nJob = 100
        iJob = 0
        while iJob < len(jediJobs):
            _logger.debug('reassignJobs for Active T1 JEDI evgensimul (%s)' % jediJobs[iJob:iJob+nJob])
            Client.killJobs(jediJobs[iJob:iJob+nJob],51,keepUnmerged=True)
            iJob += nJob

# reassign too long-standing evgen/simul jobs with active state at T2
try:
    _logger.debug('looking for stuck T2s to reassign evgensimul')
    timeLimit = datetime.datetime.utcnow() - datetime.timedelta(hours=6)
    varMap = {}
    varMap[':jobStatus1']       = 'activated'
    varMap[':jobStatus2']       = 'running'
    varMap[':prodSourceLabel']  = 'managed'
    varMap[':processingType1']  = 'evgen'
    varMap[':processingType2']  = 'simul'
    status,res = taskBuffer.querySQLS("SELECT cloud,computingSite,jobStatus,COUNT(*) FROM ATLAS_PANDA.jobsActive4 WHERE jobStatus IN (:jobStatus1,:jobStatus2) AND prodSourceLabel=:prodSourceLabel AND processingType IN (:processingType1,:processingType2) GROUP BY cloud,computingSite,jobStatus",
                                      varMap)
    if res is not None:
        # get ratio of activated/running
        siteStatData = {}
        for tmpCloud,tmpComputingSite,tmpJobStatus,tmpCount in res:
            # skip T1
            if tmpComputingSite == siteMapper.getCloud(tmpCloud)['tier1']:
                continue
            # skip if timeout is disabled
            if tmpComputingSite in sitesToSkipTO:
                continue
            # add cloud/site
            tmpKey = (tmpCloud,tmpComputingSite)
            if tmpKey not in siteStatData:
                siteStatData[tmpKey] = {'activated':0,'running':0}
            # add the number of jobs
            if tmpJobStatus in siteStatData[tmpKey]:
                siteStatData[tmpKey][tmpJobStatus] += tmpCount
        # look for stuck site
        stuckThr = 10
        stuckSites = []
        for tmpKey in siteStatData:
            tmpStatData = siteStatData[tmpKey]
            if tmpStatData['running'] == 0 or \
                   float(tmpStatData['activated'])/float(tmpStatData['running']) > stuckThr:
                tmpCloud,tmpComputingSite = tmpKey
                _logger.debug(' %s:%s %s/%s > %s' % (tmpCloud,tmpComputingSite,tmpStatData['activated'],tmpStatData['running'],stuckThr))
                # get stuck jobs
                status,res = taskBuffer.lockJobsForReassign("ATLAS_PANDA.jobsActive4",timeLimit,['activated'],['managed'],
                                                            ['evgen','simul'],[tmpComputingSite],[tmpCloud],True,
                                                            onlyReassignable=True)
                jobs = []
                jediJobs = []
                if res is not None:
                    for (id,lockedby) in res:
                        if lockedby == 'jedi':
                            jediJobs.append(id)
                        else:
                            jobs.append(id)
                _logger.debug('reassignJobs for Active T2 evgensimul %s:%s -> #%s' % (tmpCloud,tmpComputingSite,len(jobs)))
                if len(jobs) > 0:
                    nJob = 100
                    iJob = 0
                    while iJob < len(jobs):
                        _logger.debug('reassignJobs for Active T2 evgensimul (%s)' % jobs[iJob:iJob+nJob])
                        taskBuffer.reassignJobs(jobs[iJob:iJob+nJob],joinThr=True)
                        iJob += nJob
                _logger.debug('reassignJobs for Active T2 JEDI evgensimul %s:%s -> #%s' % (tmpCloud,tmpComputingSite,len(jediJobs)))
                if len(jediJobs) > 0:
                    nJob = 100
                    iJob = 0
                    while iJob < len(jediJobs):
                        _logger.debug('reassignJobs for Active T2 JEDI evgensimul (%s)' % jediJobs[iJob:iJob+nJob])
                        Client.killJobs(jediJobs[iJob:iJob+nJob],51,keepUnmerged=True)
                        iJob += nJob
except Exception:
    errType,errValue = sys.exc_info()[:2]
    _logger.error("failed to reassign T2 evgensimul with %s:%s" % (errType,errValue))

# reassign too long activated jobs in active table
timeLimit = datetime.datetime.utcnow() - datetime.timedelta(days=2)
status,res = taskBuffer.lockJobsForReassign("ATLAS_PANDA.jobsActive4",timeLimit,['activated'],['managed'],[],[],[],True,
                                            onlyReassignable=True,getEventService=True)
jobs = []
jediJobs = []
if res is not None:
    for pandaID, lockedby, eventService, attemptNr, computingSite in res:
        if computingSite in sitesToSkipTO:
            _logger.debug('skip reassignJobs for PandaID={0} for long activated in active table since timeout is disabled at {1}'.format(pandaID,computingSite))
            continue
        if lockedby == 'jedi':
            if eventService in [EventServiceUtils.esMergeJobFlagNumber]:
                _logger.debug('retrying {0} in long activated' % pandaID)
                taskBuffer.retryJob(pandaID,{},getNewPandaID=True,attemptNr=attemptNr,
                                    recoverableEsMerge=True)
            jediJobs.append(pandaID)
        else:
            jobs.append(pandaID)
_logger.debug('reassignJobs for long activated in active table -> #%s' % len(jobs))
if len(jobs) != 0:
    nJob = 100
    iJob = 0
    while iJob < len(jobs):
        _logger.debug('reassignJobs for long activated in active table (%s)' % jobs[iJob:iJob+nJob])
        taskBuffer.reassignJobs(jobs[iJob:iJob+nJob],joinThr=True)
        iJob += nJob
_logger.debug('reassignJobs for long activated JEDI in active table -> #%s' % len(jediJobs))
if len(jediJobs) != 0:
    nJob = 100
    iJob = 0
    while iJob < len(jediJobs):
        _logger.debug('reassignJobs for long activated JEDI in active table (%s)' % jediJobs[iJob:iJob+nJob])
        Client.killJobs(jediJobs[iJob:iJob+nJob],51,keepUnmerged=True)
        iJob += nJob

# reassign too long starting jobs in active table
timeLimit = datetime.datetime.utcnow() - datetime.timedelta(hours=48)
status,res = taskBuffer.lockJobsForReassign("ATLAS_PANDA.jobsActive4",timeLimit,['starting'],['managed'],[],[],[],True,
                                            onlyReassignable=True,useStateChangeTime=True,getEventService=True)
jobs = []
jediJobs = []
if res is not None:
    for pandaID, lockedby, eventService, attemptNr, computingSite in res:
        if computingSite in sitesToSkipTO:
            _logger.debug('skip reassignJobs for PandaID={0} for long starting in active table since timeout is disabled at {1}'.format(pandaID,computingSite))
            continue
        if lockedby == 'jedi':
            jediJobs.append(pandaID)
        else:
            jobs.append(pandaID)
_logger.debug('reassignJobs for long starting in active table -> #%s' % len(jobs))
if len(jobs) != 0:
    nJob = 100
    iJob = 0
    while iJob < len(jobs):
        _logger.debug('reassignJobs for long starting in active table (%s)' % jobs[iJob:iJob+nJob])
        taskBuffer.reassignJobs(jobs[iJob:iJob+nJob],joinThr=True)
        iJob += nJob
_logger.debug('reassignJobs for long starting JEDI in active table -> #%s' % len(jediJobs))
if len(jediJobs) != 0:
    nJob = 100
    iJob = 0
    while iJob < len(jediJobs):
        _logger.debug('reassignJobs for long stating JEDI in active table (%s)' % jediJobs[iJob:iJob+nJob])
        Client.killJobs(jediJobs[iJob:iJob+nJob],51,keepUnmerged=True)
        iJob += nJob
        

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
if res is not None:
    for (id,) in res:
        jobs.append(id)
# kill
if len(jobs):
    Client.killJobs(jobs,2)
    _logger.debug("killJobs for Anal Active (%s)" % str(jobs))


# kill too long pending jobs
timeLimit = datetime.datetime.utcnow() - datetime.timedelta(hours=1)
varMap = {}
varMap[':jobStatus']    = 'pending'
varMap[':creationTime'] = timeLimit
status,res = taskBuffer.querySQLS("SELECT PandaID FROM ATLAS_PANDA.jobsWaiting4 WHERE jobStatus=:jobStatus AND creationTime<:creationTime",
                                  varMap)
jobs = []
if res is not None:
    for (id,) in res:
        jobs.append(id)
# kill
if len(jobs):
    if len(jobs):
        nJob = 100
        iJob = 0
        while iJob < len(jobs):
            _logger.debug("killJobs for Pending (%s)" % str(jobs[iJob:iJob+nJob]))
            Client.killJobs(jobs[iJob:iJob+nJob],4)
            iJob += nJob


# kick waiting ES merge jobs which were generated from fake co-jumbo
timeLimit = datetime.datetime.utcnow() - datetime.timedelta(minutes=10)
varMap = {}
varMap[':jobStatus']    = 'waiting'
varMap[':creationTime'] = timeLimit
varMap[':esMerge'] = EventServiceUtils.esMergeJobFlagNumber
sql  = "SELECT PandaID,computingSite FROM ATLAS_PANDA.jobsWaiting4 WHERE jobStatus=:jobStatus AND creationTime<:creationTime "
sql += "AND eventService=:esMerge ORDER BY jediTaskID "
status,res = taskBuffer.querySQLS(sql, varMap)
jobsMap = {}
if res is not None:
    for id,site in res:
        if site not in jobsMap:
            jobsMap[site] = []
        jobsMap[site].append(id)
# kick
if len(jobsMap):
    for site in jobsMap:
        jobs = jobsMap[site]
        nJob = 100
        iJob = 0
        while iJob < len(jobs):
            _logger.debug("kick waiting ES merge (%s)" % str(jobs[iJob:iJob+nJob]))
            Client.reassignJobs(jobs[iJob:iJob+nJob], )
            iJob += nJob

# kill too long waiting jobs
timeLimit = datetime.datetime.utcnow() - datetime.timedelta(hours=1)
varMap = {}
varMap[':jobStatus']    = 'waiting'
varMap[':creationTime'] = timeLimit
varMap[':coJumbo'] = EventServiceUtils.coJumboJobFlagNumber
sql  = "SELECT PandaID FROM ATLAS_PANDA.jobsWaiting4 WHERE jobStatus=:jobStatus AND creationTime<:creationTime "
sql += "AND (eventService IS NULL OR eventService<>:coJumbo) "
status,res = taskBuffer.querySQLS(sql, varMap)
jobs = []
if res is not None:
    for (id,) in res:
        jobs.append(id)
# kill
if len(jobs):
    if len(jobs):
        nJob = 100
        iJob = 0
        while iJob < len(jobs):
            _logger.debug("killJobs for Waiting (%s)" % str(jobs[iJob:iJob+nJob]))
            Client.killJobs(jobs[iJob:iJob+nJob],4)
            iJob += nJob

# kill too long running ES jobs
timeLimit = datetime.datetime.utcnow() - datetime.timedelta(hours=24)
varMap = {}
varMap[':jobStatus1']  = 'running'
varMap[':jobStatus2']  = 'starting'
varMap[':timeLimit'] = timeLimit
varMap[':esJob'] = EventServiceUtils.esJobFlagNumber
varMap[':coJumbo'] = EventServiceUtils.coJumboJobFlagNumber
sql  = "SELECT PandaID FROM ATLAS_PANDA.jobsActive4 WHERE jobStatus IN (:jobStatus1,:jobStatus2) AND stateChangeTime<:timeLimit "
sql += "AND eventService IN (:esJob,:coJumbo) AND currentPriority>=900 "
status,res = taskBuffer.querySQLS(sql, varMap)
jobs = []
if res is not None:
    for (id,) in res:
        jobs.append(id)
# kill
if len(jobs):
    nJob = 100
    iJob = 0
    while iJob < len(jobs):
        _logger.debug("killJobs for long running ES jobs (%s)" % str(jobs[iJob:iJob+nJob]))
        Client.killJobs(jobs[iJob:iJob+nJob], 2, keepUnmerged=True, jobSubStatus='es_toolong')
        iJob += nJob

# kill too long running ES merge jobs
timeLimit = datetime.datetime.utcnow() - datetime.timedelta(hours=24)
varMap = {}
varMap[':jobStatus1']  = 'running'
varMap[':jobStatus2']  = 'starting'
varMap[':timeLimit'] = timeLimit
varMap[':esMergeJob'] = EventServiceUtils.esMergeJobFlagNumber
sql  = "SELECT PandaID FROM ATLAS_PANDA.jobsActive4 WHERE jobStatus IN (:jobStatus1,:jobStatus2) AND stateChangeTime<:timeLimit "
sql += "AND eventService=:esMergeJob "
status,res = taskBuffer.querySQLS(sql, varMap)
jobs = []
if res is not None:
    for (id,) in res:
        jobs.append(id)
# kill
if len(jobs):
    nJob = 100
    iJob = 0
    while iJob < len(jobs):
        _logger.debug("killJobs for long running ES merge jobs (%s)" % str(jobs[iJob:iJob+nJob]))
        Client.killJobs(jobs[iJob:iJob+nJob], 2)
        iJob += nJob

# kill too long waiting jobs
timeLimit = datetime.datetime.utcnow() - datetime.timedelta(days=7)
sql = "SELECT PandaID FROM ATLAS_PANDA.jobsWaiting4 WHERE ((creationTime<:timeLimit AND (eventService IS NULL OR eventService<>:coJumbo)) "
sql += "OR modificationTime<:timeLimit) "
varMap = {}
varMap[':timeLimit'] = timeLimit
varMap[':coJumbo'] = EventServiceUtils.coJumboJobFlagNumber
status,res = taskBuffer.querySQLS(sql, varMap)
jobs = []
if res is not None:
    for (id,) in res:
        jobs.append(id)
# kill
if len(jobs):
    Client.killJobs(jobs,4)
    _logger.debug("killJobs in jobsWaiting (%s)" % str(jobs))


# reassign long waiting jobs
"""
timeLimit = datetime.datetime.utcnow() - datetime.timedelta(minutes=30)
status,res = taskBuffer.lockJobsForReassign("ATLAS_PANDA.jobsWaiting4",timeLimit,['waiting'],['managed'],[],[],[],True)
jobs = []
jediJobs = []
if res is not None:
    for (id,lockedby) in res:
        if lockedby == 'jedi':
            jediJobs.append(id)
        else:
            jobs.append(id)
_logger.debug('reassignJobs for Waiting -> #%s' % len(jobs))
if len(jobs):
    nJob = 100
    iJob = 0
    while iJob < len(jobs):
        _logger.debug('reassignJobs for Waiting (%s)' % jobs[iJob:iJob+nJob])
        taskBuffer.reassignJobs(jobs[iJob:iJob+nJob],joinThr=True)
        iJob += nJob
_logger.debug('reassignJobs for JEDI Waiting -> #%s' % len(jediJobs))
if len(jediJobs) != 0:
    nJob = 100
    iJob = 0
    while iJob < len(jediJobs):
        _logger.debug('reassignJobs for JEDI Waiting (%s)' % jediJobs[iJob:iJob+nJob])
        Client.killJobs(jediJobs[iJob:iJob+nJob],51,keepUnmerged=True)
        iJob += nJob
"""

# kill too long running jobs
timeLimit = datetime.datetime.utcnow() - datetime.timedelta(days=21)
status,res = taskBuffer.querySQLS("SELECT PandaID FROM ATLAS_PANDA.jobsActive4 WHERE creationTime<:creationTime",
                              {':creationTime':timeLimit})
jobs = []
if res is not None:
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
if res is not None:
    for (id,) in res:
        jobs.append(id)
# kill
if len(jobs):
    Client.killJobs(jobs,2)
    _logger.debug("killJobs for DDM (%s)" % str(jobs))


# kill too long throttled jobs
timeLimit = datetime.datetime.utcnow() - datetime.timedelta(days=7)
varMap = {}
varMap[':jobStatus'] = 'throttled'
varMap[':creationTime'] = timeLimit
status,res = taskBuffer.querySQLS("SELECT PandaID FROM ATLAS_PANDA.jobsActive4 WHERE jobStatus=:jobStatus AND creationTime<:creationTime ",
                                  varMap)
jobs = []
if res is not None:
    for (id,) in res:
        jobs.append(id)
# kill
if len(jobs):
    Client.killJobs(jobs,2)
    _logger.debug("killJobs for throttled (%s)" % str(jobs))


# check if merge job is valid
_logger.debug('kill invalid pmerge')
varMap = {}
varMap[':processingType'] = 'pmerge'
varMap[':timeLimit'] = datetime.datetime.utcnow() - datetime.timedelta(minutes=30)
sql  = "SELECT PandaID,jediTaskID FROM ATLAS_PANDA.jobsDefined4 WHERE processingType=:processingType AND modificationTime<:timeLimit "
sql += "UNION "
sql += "SELECT PandaID,jediTaskID FROM ATLAS_PANDA.jobsActive4 WHERE processingType=:processingType AND modificationTime<:timeLimit "
status,res = taskBuffer.querySQLS(sql,varMap)
nPmerge = 0
badPmerge = 0
_logger.debug('check {0} pmerge'.format(len(res)))
for pandaID,jediTaskID in res:
    nPmerge += 1
    isValid,tmpMsg = taskBuffer.isValidMergeJob(pandaID,jediTaskID)
    if isValid == False:
        _logger.debug("kill pmerge {0} since {1} gone".format(pandaID,tmpMsg))
        taskBuffer.killJobs([pandaID],'killed since pre-merge job {0} gone'.format(tmpMsg),
                            '52',True)
        badPmerge += 1
_logger.debug('killed invalid pmerge {0}/{1}'.format(badPmerge,nPmerge))


# cleanup of jumbo jobs
_logger.debug('jumbo job cleanup')
res = taskBuffer.cleanupJumboJobs()
_logger.debug(res)


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
    except Exception:
        pass


_memoryCheck("delete core")

# delete core
dirName = '%s/..' % panda_config.logdir
for file in os.listdir(dirName):
    if file.startswith('core.'):
        _logger.debug("delete %s " % file)
        try:
            os.remove('%s/%s' % (dirName,file))
        except Exception:
            pass

                    
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
for name in pDB:
    addr = pDB[name]
    mailMap[name] = addr
# close DB
pDB.close()
# release file lock
fcntl.flock(_lockGetMail.fileno(), fcntl.LOCK_UN)
# set email address
for name in mailMap:
    addr = mailMap[name]
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

_memoryCheck("end")

_logger.debug("===================== end =====================")
