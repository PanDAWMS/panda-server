import os
import re
import sys
import time
import fcntl
import shelve
import random
import datetime
import commands
import userinterface.Client as Client
from dataservice.DDM import ddm
from taskbuffer.OraDBProxy import DBProxy
from taskbuffer.TaskBuffer import taskBuffer
from pandalogger.PandaLogger import PandaLogger
from jobdispatcher.Watcher import Watcher
from brokerage.SiteMapper import SiteMapper
from dataservice.Adder import Adder
from dataservice.Finisher import Finisher
import brokerage.broker_util
import brokerage.broker
import taskbuffer.ErrorCode

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
    
    
# instantiate DB proxies
proxyS = DBProxy()
proxyS.connect(panda_config.dbhost,panda_config.dbpasswd,panda_config.dbuser,panda_config.dbname)

# table names
jobATableName   = "ATLAS_PANDAARCH.jobsArchived"
filesATableName = "ATLAS_PANDAARCH.filesTable_ARCH"
paramATableName = "ATLAS_PANDAARCH.jobParamsTable_ARCH"
metaATableName  = "ATLAS_PANDAARCH.metaTable_ARCH"

# time limit
timeLimit = datetime.datetime.utcnow() - datetime.timedelta(days=3)

# get PandaIDs and endTimes from source
_logger.debug("get PandaIDs from Archive")
status,res = proxyS.querySQLS("SELECT PandaID,modificationTime from ATLAS_PANDA.jobsArchived4 ORDER BY PandaID",{})
if res == None:
    _logger.debug("total %s " % res)
else:
    _logger.debug("total %s " % len(res))
    # get PandaIDs in last 3 days
    archIDs = []
    try:
        stArch,resArch = proxyS.querySQLS("SELECT PandaID FROM %s WHERE modificationTime>:modificationTime" % jobATableName,
                                          {':modificationTime':timeLimit},arraySize=1000000)
        for idArch, in resArch:
            archIDs.append(idArch)
    except:
        pass
    # copy, and delete if old
    for (id,srcEndTime) in res:
        try:
            # check if already recorded
            copyFound = True
            if not id in archIDs:
                _logger.debug("check  %s " % id)
                sql = "SELECT PandaID from %s WHERE PandaID=:PandaID" % jobATableName
                varMap = {}
                varMap[':PandaID'] = id
                status,check = proxyS.querySQLS(sql,varMap)
                # copy
                if len(check) == 0:
                    copyFound = False
                    # get jobs
                    job = proxyS.peekJob(id,False,False,True,False)
                    # insert to archived
                    proxyS.insertJobSimple(job,jobATableName,filesATableName,paramATableName,metaATableName)
                    _logger.debug("INSERT %s " % id)
            # delete
            if srcEndTime==None or srcEndTime < timeLimit:
                if not copyFound:
                    _logger.error("No backup for %s" % id)
                else:
                    sql = 'DELETE from ATLAS_PANDA.jobsArchived4 WHERE PandaID=:PandaID'
                    varMap = {}
                    varMap[':PandaID']  = id
                    proxyS.querySQLS(sql,varMap)
                    _logger.debug("DEL %s : endTime %s" % (id,srcEndTime))
                    # delete files
                    status,retDel = proxyS.querySQLS("DELETE from ATLAS_PANDA.filesTable4 WHERE PandaID=:PandaID",varMap)
                    _logger.debug("DEL Files for %s " % id)
                    # delete metadata
                    status,retDel = proxyS.querySQLS("DELETE from ATLAS_PANDA.metaTable WHERE PandaID=:PandaID",varMap)
                    _logger.debug("DEL metadata for %s " % id)
                    # delete job parameters
                    status,retDel = proxyS.querySQLS("DELETE from ATLAS_PANDA.jobParamsTable WHERE PandaID=:PandaID",varMap)
                    _logger.debug("DEL jobParams for %s " % id)
            else:
                pass
        except:
            pass
del res

# instantiate TB
taskBuffer.init(panda_config.dbhost,panda_config.dbpasswd,nDBConnection=1)

# instantiate sitemapper
siteMapper = SiteMapper(taskBuffer)

# increase priorities for long-waiting analysis jobs
_logger.debug("Analysis Priorities")
timeLimit = datetime.datetime.utcnow() - datetime.timedelta(hours=24)
sql = "UPDATE ATLAS_PANDA.jobsActive4 SET currentPriority=currentPriority+40,modificationTime=CURRENT_DATE WHERE jobStatus='activated' "
sql+= "AND modificationTime<:modificationTime AND ((computingSite IS NULL) OR (computingSite<>'CHARMM' AND computingSite<>'TESTCHARMM')) AND prodSourceLabel='user' "
varMap = {}
varMap[':modificationTime'] = timeLimit
status,res = proxyS.querySQLS(sql,varMap)
_logger.debug("increased priority for %s" % status)
time.sleep(1)


_memoryCheck("watcher")

_logger.debug("Watcher session")
# check heartbeat for analysis jobs
timeLimit = datetime.datetime.utcnow() - datetime.timedelta(hours=2)
status,res = proxyS.querySQLS("SELECT PandaID FROM ATLAS_PANDA.jobsActive4 WHERE (prodSourceLabel='panda' OR prodSourceLabel='user') AND (jobStatus='running' OR jobStatus='starting' OR jobStatus='stagein' OR jobStatus='stageout') AND modificationTime<:modificationTime AND ((computingSite IS NULL) OR (computingSite<>'CHARMM' AND computingSite<>'TESTCHARMM'))",
                              {':modificationTime':timeLimit})
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
status,res = proxyS.querySQLS("SELECT PandaID FROM ATLAS_PANDA.jobsActive4 WHERE jobStatus='sent' AND modificationTime<:modificationTime AND ((computingSite IS NULL) OR (computingSite<>'CHARMM' AND computingSite<>'TESTCHARMM'))",
                              {':modificationTime':timeLimit})
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
sql = "SELECT PandaID FROM ATLAS_PANDA.jobsActive4 WHERE jobStatus='holding' AND (modificationTime<:modificationTime OR (endTime IS NOT NULL AND endTime<:endTime)) AND (prodSourceLabel='panda' OR prodSourceLabel='user' OR prodSourceLabel='ddm')"
varMap = {}
varMap[':modificationTime'] = timeLimit
varMap[':endTime'] = timeLimit
status,res = proxyS.querySQLS(sql,varMap)
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
sql = "SELECT PandaID FROM ATLAS_PANDA.jobsActive4 WHERE jobStatus='holding' AND (modificationTime<:modificationTime OR (endTime IS NOT NULL AND endTime<:endTime)) AND ((computingSite IS NULL) OR (computingSite<>'CHARMM' AND computingSite<>'TESTCHARMM'))"
varMap = {}
varMap[':modificationTime'] = timeLimit
varMap[':endTime'] = timeLimit
status,res = proxyS.querySQLS(sql,varMap)
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
status,res = proxyS.querySQLS("SELECT PandaID FROM ATLAS_PANDA.jobsActive4 WHERE (jobStatus='running' OR jobStatus='starting' OR jobStatus='stagein' OR jobStatus='stageout') AND modificationTime<:modificationTime AND ((computingSite IS NULL) OR (computingSite<>'CHARMM' AND computingSite<>'TESTCHARMM')) AND prodSourceLabel='ddm'",
                              {':modificationTime':timeLimit})
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
status,res = proxyS.querySQLS("SELECT PandaID FROM ATLAS_PANDA.jobsActive4 WHERE (jobStatus='running' OR jobStatus='starting' OR jobStatus='stagein' OR jobStatus='stageout') AND modificationTime<:modificationTime AND ((computingSite IS NULL) OR (computingSite<>'CHARMM' AND computingSite<>'TESTCHARMM'))",
                              {':modificationTime':timeLimit})
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
status,res = proxyS.querySQLS("SELECT PandaID from ATLAS_PANDA.jobsDefined4 WHERE creationTime<:creationTime",
                              {':creationTime':timeLimit})
jobs=[]
if res != None:
    for (id,) in res:
        jobs.append(id)
if len(jobs):
    eraseDispDatasets(jobs)
    _logger.debug("killJobs for Defined (%s)" % str(jobs))
    Client.killJobs(jobs,2)

# kill long-waiting jobs in active table
timeLimit = datetime.datetime.utcnow() - datetime.timedelta(days=7)
status,res = proxyS.querySQLS("SELECT PandaID from ATLAS_PANDA.jobsActive4 WHERE jobStatus='activated' AND creationTime<:creationTime",
                              {':creationTime':timeLimit})                              
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
sql = "SELECT PandaID from ATLAS_PANDA.jobsActive4 WHERE prodSourceLabel='ddm' AND transferType='dis' AND creationTime<:creationTime"
varMap = {}
varMap[':creationTime'] = timeLimit
_logger.debug(sql+str(varMap))
status,res = proxyS.querySQLS(sql,varMap)
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
sql = "SELECT PandaID from ATLAS_PANDA.jobsActive4 WHERE prodSourceLabel='ddm' AND transferType='dis' AND jobStatus='running' AND startTime<:startTime"
varMap = {}
varMap[':startTime'] = timeLimit
_logger.debug(sql+str(varMap))
status,res = proxyS.querySQLS(sql,varMap)
_logger.debug(res)
jobs   = []
movers = []
if res != None:
    for id, in res:
        movers.append(id)
        # get dispatch dataset
        sql = 'SELECT name FROM ATLAS_PANDA.Datasets WHERE MoverID=:MoverID'
        stDS,resDS = proxyS.querySQLS(sql,{':MoverID':id})
        if resDS != None:
            disDS = resDS[0][0]
            # get PandaIDs associated to the dis dataset
            sql = "SELECT PandaID FROM ATLAS_PANDA.jobsDefined4 WHERE jobStatus='assigned' AND dispatchDBlock=:dispatchDBlock"
            stP,resP = proxyS.querySQLS(sql,{':dispatchDBlock':disDS})
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
    status,res = proxyS.querySQLS("SELECT PandaID from ATLAS_PANDA.jobsDefined4 where jobStatus='defined' AND modificationTime<:modificationTime AND prodSourceLabel='managed' AND ((computingSite IS NULL) OR (computingSite<>'CHARMM' AND computingSite<>'TESTCHARMM')) ORDER BY PandaID",
                                  {':modificationTime':timeLimit})                                  
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

                        
# reassign reprocessing jobs in defined table
timeLimit = datetime.datetime.utcnow() - datetime.timedelta(hours=8)
status,res = proxyS.querySQLS("SELECT PandaID,transformation,processingType FROM ATLAS_PANDA.jobsDefined4 WHERE jobStatus='assigned' AND prodSourceLabel='managed' AND modificationTime<:modificationTime ORDER BY PandaID",
                              {':modificationTime':timeLimit})
jobs=[]
if res != None:
    # tmp class to adjust API
    class Tmp:
        pass
    for id,trf,processingType in res:
        # check trf. Should be changed once a proper flag is defined in prodDB
        tmpObj = Tmp()
        tmpObj.transformation = trf
        tmpObj.processingType = processingType
        if brokerage.broker._isReproJob(tmpObj):
            jobs.append(id)
# reassign
if len(jobs):
    nJob = 100
    iJob = 0
    while iJob < len(jobs):
        eraseDispDatasets(jobs[iJob:iJob+nJob])
        # reassign jobs one by one to break dis dataset formation
        for job in jobs[iJob:iJob+nJob]:
            _logger.debug('reassignJobs in Pepro (%s)' % [job])
            Client.reassignJobs([job])
            time.sleep(5)            
        iJob += nJob
        time.sleep(60)


# reassign long-waiting jobs in defined table
timeLimit = datetime.datetime.utcnow() - datetime.timedelta(hours=12)
status,res = proxyS.querySQLS("SELECT PandaID from ATLAS_PANDA.jobsDefined4 WHERE prodSourceLabel='managed' AND ((computingSite IS NULL) OR (computingSite<>'CHARMM' AND computingSite<>'TESTCHARMM')) AND modificationTime<:modificationTime ORDER BY PandaID",
                              {':modificationTime':timeLimit})
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
status,res = proxyS.querySQLS("SELECT PandaID FROM ATLAS_PANDA.jobsActive4 WHERE jobStatus='activated' AND prodSourceLabel='managed' AND ((computingSite IS NULL) OR (computingSite<>'CHARMM' AND computingSite<>'TESTCHARMM')) AND modificationTime<:modificationTime ORDER BY PandaID",
                              {':modificationTime':timeLimit})
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
status,res = proxyS.querySQLS("SELECT PandaID FROM ATLAS_PANDA.jobsActive4 WHERE (prodSourceLabel='test' OR prodSourceLabel='panda' OR prodSourceLabel='user') AND modificationTime<:modificationTime AND ((computingSite IS NULL) OR (computingSite<>'CHARMM' AND computingSite<>'TESTCHARMM')) ORDER BY PandaID",
                              {':modificationTime':timeLimit})
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
status,res = proxyS.querySQLS("SELECT PandaID FROM ATLAS_PANDA.jobsWaiting4 WHERE creationTime<:creationTime",
                              {':creationTime':timeLimit})
jobs = []
if res != None:
    for (id,) in res:
        jobs.append(id)
# kill
if len(jobs):
    Client.killJobs(jobs,2)
    _logger.debug("killJobs for Waiting (%s)" % str(jobs))


# reassign long waiting jobs
timeLimit = datetime.datetime.utcnow() - datetime.timedelta(hours=7)
status,res = proxyS.querySQLS("SELECT PandaID FROM ATLAS_PANDA.jobsWaiting4 WHERE prodSourceLabel='managed' AND modificationTime<:modificationTime ORDER BY PandaID",
                              {':modificationTime':timeLimit})
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
status,res = proxyS.querySQLS("SELECT PandaID FROM ATLAS_PANDA.jobsActive4 WHERE creationTime<:creationTime",
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
status,res = proxyS.querySQLS("SELECT PandaID FROM ATLAS_PANDA.jobsActive4 WHERE prodSourceLabel='ddm' AND creationTime<:creationTime",
                              {':creationTime':timeLimit})
jobs = []
if res != None:
    for (id,) in res:
        jobs.append(id)
# kill
if len(jobs):
    Client.killJobs(jobs,2)
    _logger.debug("killJobs for DDM (%s)" % str(jobs))

_memoryCheck("closing")

# time limit for dataset closing
timeLimit = datetime.datetime.utcnow() - datetime.timedelta(minutes=30)

# close datasets
while True:
    status,res = proxyS.querySQLS("SELECT vuid,name,modificationdate FROM ATLAS_PANDA.Datasets WHERE type='output' and status='tobeclosed' and modificationdate<:modificationdate AND rownum <= 10",
                                  {':modificationdate':timeLimit})
    if res == None:
        _logger.debug("# of datasets to be closed: %s" % res)
    else:
        _logger.debug("# of datasets to be closed: %s" % len(res))
    if res==None or len(res)==0:
        break
    # update to prevent other process from picking up
    for (vuid,name,modDate) in res:
        # convert string to datetime
        if re.search('^\d+/\d+/\d+',modDate) != None:
            datetimeTime = datetime.datetime(*time.strptime(modDate,'%Y/%m/%d %H:%M:%S')[:6])
        else:
            datetimeTime = datetime.datetime(*time.strptime(modDate,'%Y-%m-%d %H:%M:%S')[:6])
        # delete
        if datetimeTime < timeLimit:
            proxyS.querySQLS("UPDATE ATLAS_PANDA.Datasets SET modificationdate=CURRENT_DATE WHERE vuid=:vuid", {':vuid':vuid})
    for (vuid,name,modDate) in res:
        # convert string to datetime
        if re.search('^\d+/\d+/\d+',modDate) != None:
            datetimeTime = datetime.datetime(*time.strptime(modDate,'%Y/%m/%d %H:%M:%S')[:6])
        else:
            datetimeTime = datetime.datetime(*time.strptime(modDate,'%Y-%m-%d %H:%M:%S')[:6])
        # delete
        if datetimeTime < timeLimit:
            _logger.debug("Close %s %s" % (modDate,name))
            if not name.startswith('testpanda.ddm.'):
                status,out = ddm.DQ2.main('freezeDataset',name)
            else:
                status,out = 0,''
            if status != 0 and out.find('DQFrozenDatasetException') == -1 and \
                   out.find("DQUnknownDatasetException") == -1 and out.find("DQSecurityException") == -1 and \
                   out.find("DQDeletedDatasetException") == -1 and out.find("DQUnknownDatasetException") == -1:
                _logger.error(out)
            else:
                proxyS.querySQLS("UPDATE ATLAS_PANDA.Datasets SET status='completed',modificationdate=CURRENT_DATE WHERE vuid=:vuid", {':vuid':vuid})
                if name.startswith('testpanda.ddm.'):
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
                # logging
                try:
                    # make message
                    message = '%s - vuid:%s name:%s' % (commands.getoutput('hostname'),vuid,name)
                    # get logger
                    _pandaLogger = PandaLogger()
                    _pandaLogger.lock()
                    _pandaLogger.setParams({'Type':'freezeDataset'})
                    logger = _pandaLogger.getHttpLogger(panda_config.loggername)
                    # add message
                    logger.info(message)
                    # release HTTP handler
                    _pandaLogger.release()
                except:
                    pass
        else:
            _logger.debug("Wait  %s %s" % (modDate,name))

_memoryCheck("freezing")

# freeze dataset
timeLimit = datetime.datetime.utcnow() - datetime.timedelta(days=4)
sql = "SELECT vuid,name,modificationdate FROM ATLAS_PANDA.Datasets " + \
      "WHERE type='output' AND (status='running' OR status='created' OR status='defined') " + \
      "AND modificationdate<:modificationdate AND REGEXP_LIKE(name,'_sub[[:digit:]]+$') AND rownum <= 20"
for i in range(100):
    ret,res = proxyS.querySQLS(sql, {':modificationdate':timeLimit})
    if res == None:
        _logger.debug("# of datasets to be frozen: %s" % res)
    else:
        _logger.debug("# of datasets to be frozen: %s" % len(res))
    if res==None or len(res)==0:
        break
    # update to prevent other process from picking up
    for (vuid,name,modDate) in res:
        proxyS.querySQLS("UPDATE ATLAS_PANDA.Datasets SET modificationdate=CURRENT_DATE WHERE vuid=:vuid", {':vuid':vuid})
    if len(res) != 0:
        for (vuid,name,modDate) in res:
            _logger.debug("start %s %s" % (modDate,name))
            retF,resF = proxyS.querySQLS("SELECT lfn FROM ATLAS_PANDA.filesTable4 WHERE destinationDBlock=:destinationDBlock",
                                         {':destinationDBlock':name})
            if retF<0:
                _logger.error("SQL error")
            else:
                # no files in filesTable
                if len(resF) == 0:
                    _logger.debug("freeze %s " % name)
                    if not name.startswith('testpanda.ddm.'):
                        status,out = ddm.DQ2.main('freezeDataset',name)
                    else:
                        status,out = 0,''
                    if status != 0 and out.find('DQFrozenDatasetException') == -1 and \
                           out.find("DQUnknownDatasetException") == -1 and out.find("DQSecurityException") == -1 and \
                           out.find("DQDeletedDatasetException") == -1 and out.find("DQUnknownDatasetException") == -1:
                        _logger.error(out)
                    else:
                        proxyS.querySQLS("UPDATE ATLAS_PANDA.Datasets SET status='completed',modificationdate=CURRENT_DATE WHERE vuid=:vuid",
                                         {':vuid':vuid})
                        if name.startswith('testpanda.ddm.'):
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
                    proxyS.querySQLS("UPDATE ATLAS_PANDA.Datasets SET modificationdate=CURRENT_DATE WHERE vuid=:vuid", {':vuid':vuid})
            _logger.debug("end %s " % name)
            time.sleep(1)

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
sql = "SELECT PandaID FROM ATLAS_PANDA.jobsActive4 WHERE jobStatus='transferring' AND modificationTime<:modificationTime AND rownum<=20"
for ii in range(1000):
    ret,res = proxyS.querySQLS(sql, {':modificationTime':timeLimit})
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
            if job != None:
                taskBuffer.updateJobStatus(job.PandaID,job.jobStatus,{})
        upJobs = []
        finJobs = []
        for job in jobs:
            if job == None:
                continue
            # use BNL by default
            dq2URL = siteMapper.getSite('BNL_ATLAS_1').dq2url
            dq2SE  = []
            # use cloud's destination
            if siteMapper.checkCloud(job.cloud):
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
            # FIXME : space tokens are not yet ready in US
            if job.cloud in ['US']:
                # use the number of LFNs instead of PFNs for checking
                nTokens   = len(lfns)
                nOkTokens = len(okFiles)
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
                if job.currentPriority >= 900:
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
                    # delete files from _tid datasets
                    for destDBlock in guidMap.keys():
                        match = re.findall('(.+)_sub\d+$',destDBlock)
                        if len(match):
                            origDBlock = match[0]
                            _logger.debug(('deleteFilesFromDataset',origDBlock,guidMap[destDBlock]))
                            status,out = ddm.DQ2.main('deleteFilesFromDataset',origDBlock,guidMap[destDBlock])
                            _logger.debug(out)
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
    status,res = proxyS.querySQLS("SELECT email FROM ATLAS_PANDAMETA.users WHERE name=:name",{':name':name})
    # failed or not found
    if status == -1 or len(res) == 0:
        _logger.error("%s not found in user DB" % name)
        continue
    # already set
    if res[0][0] != '':
        continue
    # update email
    _logger.debug("set '%s' to %s" % (name,addr))
    status,res = proxyS.querySQLS("UPDATE ATLAS_PANDAMETA.users SET email=:addr WHERE name=:name",{':addr':addr,':name':name})

_memoryCheck("end")

_logger.debug("===================== end =====================")
