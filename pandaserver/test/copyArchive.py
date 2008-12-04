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
from taskbuffer.DBProxy import DBProxy
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
        if not items[0] in ['sm','atlpan']: # ['os.getlogin()']: doesn't work in cron
            continue
        # look for python
        if re.search('python',line) == None:
            continue
        # PID
        pid = items[1]
        # start time
        timeM = re.search('(\S+ \d+ \d+:\d+:\d+ \d+)',line)
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
proxyN = DBProxy()
proxyD = DBProxy()
proxyS.connect(panda_config.dbhost,panda_config.dbpasswd,panda_config.dbuser,panda_config.dbname)
proxyN.connect(panda_config.logdbhost,panda_config.logdbpasswd,panda_config.logdbuser,'PandaArchiveDB')
proxyD.connect(panda_config.logdbhost,panda_config.logdbpasswd,panda_config.logdbuser,'PandaLogDB')

# table names
cdate = datetime.datetime.utcnow()
if cdate.month==1:
    cdate = cdate.replace(year = (cdate.year-1))
    cdate = cdate.replace(month = 12, day = 1)
else:
    cdate = cdate.replace(month = (cdate.month/2)*2, day = 1)
jobATableName   = "jobsArchived_%s%s" % (cdate.strftime('%b'),cdate.year)
filesATableName = "filesTable_%s%s" % (cdate.strftime('%b'),cdate.year)
metaATableName  = "metaTable_%s%s" % (cdate.strftime('%b'),cdate.year)
dsATableName    = "Datasets_%s%s" % (cdate.strftime('%b'),cdate.year)

if cdate.month > 2:
    odate = cdate.replace(month = (cdate.month-2))
else:
    odate = cdate.replace(year = (cdate.year-1), month = 12)
oldJobAtableName = "jobsArchived_%s%s" % (odate.strftime('%b'),odate.year)

_logger.debug("Tables : %s %s %s %s %s" %
              (jobATableName,filesATableName,metaATableName,oldJobAtableName,dsATableName))

# time limit
timeLimit = datetime.datetime.utcnow() - datetime.timedelta(days=3)

# get PandaIDs and endTimes from source
_logger.debug("get PandaIDs from Archive")
status,res = proxyS.querySQLS("SELECT PandaID,modificationTime from jobsArchived4 ORDER BY PandaID")
if res == None:
    _logger.debug("total %s " % res)
else:
    _logger.debug("total %s " % len(res))
    # copy, and delete if old
    for (id,srcEndTime) in res:
        try:
            # check if already recorded
            sql = "SELECT PandaID from %s WHERE PandaID=%s" % (jobATableName,id)
            status,check = proxyN.querySQLS(sql)
            sql = "SELECT PandaID from %s WHERE PandaID=%s" % (oldJobAtableName,id)
            statusOld,checkOld = proxyN.querySQLS(sql)
            # copy
            if len(check) == 0 and len(checkOld) == 0:
                # get jobs
                job = proxyS.peekJob(id,False,False,True,False)
                # insert to dest
                proxyN.insertJobSimple(job,jobATableName,filesATableName)
                # metadata
                status,meta = proxyS.querySQLS("SELECT metaData from metaTable WHERE PandaID=%s" % id)
                if len(meta) != 0:
                    proxyN.querySQLwList("INSERT INTO "+ metaATableName+" (PandaID,metaData) VALUE(%s,%s)",
                                         (id,meta[0][0]))
                    _logger.debug("meta INSERT %s " % id)

                _logger.debug("INSERT %s " % id)
            # delete
            if srcEndTime==None or srcEndTime < timeLimit:
                sql = 'DELETE from jobsArchived4 WHERE PandaID=%s' % id
                proxyS.querySQLS(sql)
                _logger.debug("DEL %s : endTime %s" % (id,srcEndTime))
                # delete files
                status,retDel = proxyS.querySQLS("DELETE from filesTable4 WHERE PandaID=%s" % id)
                _logger.debug("DEL Files for %s " % id)
                # delete metadata
                status,retDel = proxyS.querySQLS("DELETE from metaTable WHERE PandaID=%s" % id)
                _logger.debug("DEL metadata for %s " % id)
            else:
                pass
        except:
            pass

# instantiate TB
taskBuffer.init(panda_config.dbhost,panda_config.dbpasswd,nDBConnection=1)
del res

# instantiate sitemapper
siteMapper = SiteMapper(taskBuffer)

# increase priorities for long-waiting analysis jobs
_logger.debug("Analysis Priorities")
timeLimit = datetime.datetime.utcnow() - datetime.timedelta(hours=24)
nAnal = 100
while True:
    status,res = proxyS.querySQLS("UPDATE jobsActive4 SET currentPriority=currentPriority+40,modificationTime=UTC_TIMESTAMP() WHERE jobStatus='activated' AND modificationTime<'%s' AND computingSite<>'CHARMM' AND computingSite<>'TESTCHARMM' AND prodSourceLabel='user' limit %s"
                      % (timeLimit.strftime('%Y-%m-%d %H:%M:%S'),nAnal))
    _logger.debug("increased priority for %s" % status)
    if status != nAnal:
        break
    time.sleep(1)
        
# archive dataset
timeLimit  = datetime.datetime.utcnow() - datetime.timedelta(days=60)
timeLimitA = datetime.datetime.utcnow() - datetime.timedelta(days=90)
for dsType,dsPrefix in [('output','sub'),('dispatch','dis'),('','')]:
    sql = "SELECT vuid,name,modificationdate FROM Datasets "
    if dsType != '':
        # dis or sub
        sql+= "WHERE type='%s' AND modificationdate<'%s' AND name REGEXP '_%s[[:digit:]]+$' LIMIT 50" \
              % (dsType,timeLimit.strftime('%Y-%m-%d %H:%M:%S'),dsPrefix)
    else:
        # delete datasets older than 3 months
        sql+= "WHERE creationdate<'%s' LIMIT 50" \
              % timeLimitA.strftime('%Y-%m-%d %H:%M:%S')
    for i in range(100000):
        ret,res = proxyS.querySQLS(sql)
        if res == None:
            _logger.debug("# of datasets to be archived: %s" % res)
        else:
            _logger.debug("# of datasets to be archived: %s" % len(res))
        if res==None or len(res)==0:
            break
        if len(res) != 0:
            for (vuid,name,modDate) in res:
                _logger.debug("move %s %s" % (modDate,name))
                ds = proxyS.queryDatasetWithMap({'vuid':vuid})
                if ds == None:
                    logger.error("cannot find %s " % name)
                    continue
                # insert to archived
                ret = proxyD.insertDataset(ds,dsATableName)
                if not ret:
                    _logger.error("cannot insert %s into dsATableName" % (name,dsATableName))
                    continue
                # delete
                retDel,resDel = proxyS.querySQLS("DELETE FROM Datasets WHERE vuid='%s'" % vuid)
                if not retDel:
                    _logger.error("cannot delete %s " % name)
                    continue
                _logger.debug("done %s %s" % (modDate,name))                
        time.sleep(2)


_memoryCheck("watcher")

_logger.debug("Watcher session")
# check heartbeat for analysis jobs
timeLimit = datetime.datetime.utcnow() - datetime.timedelta(hours=2)
status,res = proxyS.querySQLS("SELECT PandaID FROM jobsActive4 WHERE (prodSourceLabel='panda' OR prodSourceLabel='user') AND (jobStatus='running' OR jobStatus='starting' OR jobStatus='stagein' OR jobStatus='stageout') AND modificationTime<'%s' AND computingSite<>'CHARMM' AND computingSite<>'TESTCHARMM'"
                              % timeLimit.strftime('%Y-%m-%d %H:%M:%S'))
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
status,res = proxyS.querySQLS("SELECT PandaID FROM jobsActive4 WHERE jobStatus='sent' AND modificationTime<'%s' AND computingSite<>'CHARMM' AND computingSite<>'TESTCHARMM'"
                              % timeLimit.strftime('%Y-%m-%d %H:%M:%S'))
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
sql = "SELECT PandaID FROM jobsActive4 WHERE jobStatus='holding' AND (modificationTime<'%s' OR (endTime<>'0000-00-00 00:00:00' AND endTime<'%s')) AND (prodSourceLabel='panda' OR prodSourceLabel='user' OR prodSourceLabel='ddm')" \
      % (timeLimit.strftime('%Y-%m-%d %H:%M:%S'),timeLimit.strftime('%Y-%m-%d %H:%M:%S'))
status,res = proxyS.querySQLS(sql)
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
sql = "SELECT PandaID FROM jobsActive4 WHERE jobStatus='holding' AND (modificationTime<'%s' OR (endTime<>'0000-00-00 00:00:00' AND endTime<'%s')) AND computingSite<>'CHARMM' AND computingSite<>'TESTCHARMM'" \
      % (timeLimit.strftime('%Y-%m-%d %H:%M:%S'),timeLimit.strftime('%Y-%m-%d %H:%M:%S'))
status,res = proxyS.querySQLS(sql)
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
status,res = proxyS.querySQLS("SELECT PandaID FROM jobsActive4 WHERE (jobStatus='running' OR jobStatus='starting' OR jobStatus='stagein' OR jobStatus='stageout') AND modificationTime<'%s' AND computingSite<>'CHARMM' AND computingSite<>'TESTCHARMM' AND prodSourceLabel='ddm'"
                      % timeLimit.strftime('%Y-%m-%d %H:%M:%S'))
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
status,res = proxyS.querySQLS("SELECT PandaID FROM jobsActive4 WHERE (jobStatus='running' OR jobStatus='starting' OR jobStatus='stagein' OR jobStatus='stageout') AND modificationTime<'%s' AND computingSite<>'CHARMM' AND computingSite<>'TESTCHARMM'"
                      % timeLimit.strftime('%Y-%m-%d %H:%M:%S'))
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
status,res = proxyS.querySQLS("SELECT PandaID from jobsDefined4 WHERE creationTime<'%s'"
                              % timeLimit.strftime('%Y-%m-%d %H:%M:%S'))
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
status,res = proxyS.querySQLS("SELECT PandaID from jobsActive4 WHERE jobStatus='activated' AND creationTime<'%s'"
                              % timeLimit.strftime('%Y-%m-%d %H:%M:%S'))
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
status,res = proxyS.querySQLS("SELECT PandaID from jobsActive4 WHERE prodSourceLabel='ddm' AND transferType='dis' AND creationTime<'%s'"
                              % timeLimit.strftime('%Y-%m-%d %H:%M:%S'))
_logger.debug("SELECT PandaID from jobsActive4 WHERE prodSourceLabel='ddm' AND transferType='dis' AND creationTime<'%s'"
              % timeLimit.strftime('%Y-%m-%d %H:%M:%S'))
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
sql = "SELECT PandaID from jobsActive4 WHERE prodSourceLabel='ddm' AND transferType='dis' AND jobStatus='running' AND startTime<'%s'" \
      % timeLimit.strftime('%Y-%m-%d %H:%M:%S')
status,res = proxyS.querySQLS(sql)
_logger.debug(sql)
_logger.debug(res)
jobs   = []
movers = []
if res != None:
    for id, in res:
        movers.append(id)
        # get dispatch dataset
        sql = 'SELECT name FROM Datasets WHERE MoverID=%s' % id
        stDS,resDS = proxyS.querySQLS(sql)
        if resDS != None:
            disDS = resDS[0][0]
            # get PandaIDs associated to the dis dataset
            sql = "SELECT PandaID FROM jobsDefined4 WHERE jobStatus='assigned' AND dispatchDBlock='%s'" % disDS
            stP,resP = proxyS.querySQLS(sql)
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
    status,res = proxyS.querySQLS("SELECT PandaID from jobsDefined4 where jobStatus='defined' and modificationTime<'%s' and prodSourceLabel='managed' AND computingSite<>'CHARMM' AND computingSite<>'TESTCHARMM' ORDER BY PandaID"
                          % timeLimit.strftime('%Y-%m-%d %H:%M:%S'))  
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
status,res = proxyS.querySQLS("SELECT PandaID,transformation,processingType FROM jobsDefined4 WHERE jobStatus='assigned' AND prodSourceLabel='managed' AND modificationTime<'%s' ORDER BY PandaID"
                              % timeLimit.strftime('%Y-%m-%d %H:%M:%S'))
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
status,res = proxyS.querySQLS("SELECT PandaID from jobsDefined4 WHERE prodSourceLabel='managed' AND computingSite<>'CHARMM' AND computingSite<>'TESTCHARMM' AND modificationTime<'%s' ORDER BY PandaID"
                              % timeLimit.strftime('%Y-%m-%d %H:%M:%S'))
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
status,res = proxyS.querySQLS("SELECT PandaID FROM jobsActive4 WHERE jobStatus='activated' AND prodSourceLabel='managed' AND computingSite<>'CHARMM' AND computingSite<>'TESTCHARMM' AND modificationTime<'%s' ORDER BY PandaID"
                              % timeLimit.strftime('%Y-%m-%d %H:%M:%S'))
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
status,res = proxyS.querySQLS("SELECT PandaID FROM jobsActive4 WHERE (prodSourceLabel='test' OR prodSourceLabel='panda' OR prodSourceLabel='user') AND modificationTime<'%s' AND computingSite<>'CHARMM' AND computingSite<>'TESTCHARMM' ORDER BY PandaID"
                              % timeLimit.strftime('%Y-%m-%d %H:%M:%S'))
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
status,res = proxyS.querySQLS("SELECT PandaID FROM jobsWaiting4 WHERE creationTime<'%s'"
                              % timeLimit.strftime('%Y-%m-%d %H:%M:%S'))
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
status,res = proxyS.querySQLS("SELECT PandaID FROM jobsWaiting4 WHERE prodSourceLabel='managed' AND modificationTime<'%s' ORDER BY PandaID"
                              % timeLimit.strftime('%Y-%m-%d %H:%M:%S'))
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
status,res = proxyS.querySQLS("SELECT PandaID FROM jobsActive4 WHERE creationTime<'%s'"
                              % timeLimit.strftime('%Y-%m-%d %H:%M:%S'))
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
status,res = proxyS.querySQLS("SELECT PandaID FROM jobsActive4 WHERE prodSourceLabel='ddm' AND creationTime<'%s'"
                              % timeLimit.strftime('%Y-%m-%d %H:%M:%S'))
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
    status,res = proxyS.querySQLS("SELECT vuid,name,modificationdate FROM Datasets WHERE type='output' and status='tobeclosed' and modificationdate<'%s' limit 10" % timeLimit.strftime('%Y-%m-%d %H:%M:%S'))
    if res == None:
        _logger.debug("# of datasets to be closed: %s" % res)
    else:
        _logger.debug("# of datasets to be closed: %s" % len(res))
    if res==None or len(res)==0:
        break
    # update to prevent other process from picking up
    for (vuid,name,modDate) in res:
        # convert string to datetime
        datetimeTime = datetime.datetime(*time.strptime(modDate,'%Y-%m-%d %H:%M:%S')[:6])
        # delete
        if datetimeTime < timeLimit:
            proxyS.querySQLS("UPDATE Datasets SET modificationdate=UTC_TIMESTAMP() WHERE vuid='%s'" % vuid)
    for (vuid,name,modDate) in res:
        # convert string to datetime
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
                proxyS.querySQLS("UPDATE Datasets SET status='completed',modificationdate=UTC_TIMESTAMP() WHERE vuid='%s'" % vuid)
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
sql = "SELECT vuid,name,modificationdate FROM Datasets " + \
      "WHERE type='output' AND (status='running' OR status='created' OR status='defined') " + \
      "AND modificationdate<'%s' AND name REGEXP '_sub[[:digit:]]+$' LIMIT 20"
for i in range(100):
    ret,res = proxyS.querySQLS(sql % timeLimit.strftime('%Y-%m-%d %H:%M:%S'))
    if res == None:
        _logger.debug("# of datasets to be frozen: %s" % res)
    else:
        _logger.debug("# of datasets to be frozen: %s" % len(res))
    if res==None or len(res)==0:
        break
    # update to prevent other process from picking up
    for (vuid,name,modDate) in res:
        proxyS.querySQLS("UPDATE Datasets SET modificationdate=UTC_TIMESTAMP() WHERE vuid='%s'" % vuid)
    if len(res) != 0:
        for (vuid,name,modDate) in res:
            _logger.debug("start %s %s" % (modDate,name))
            retF,resF = proxyS.querySQLS("SELECT lfn FROM filesTable4 WHERE destinationDBlock='%s'" % name)
            if retF<0 or retF == None or retF!=len(resF):
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
                        proxyS.querySQLS("UPDATE Datasets SET status='completed',modificationdate=UTC_TIMESTAMP() WHERE vuid='%s'" \
                                         % vuid)
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
                    proxyS.querySQLS("UPDATE Datasets SET modificationdate=UTC_TIMESTAMP() WHERE vuid='%s'" % vuid)                
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
sql = "SELECT PandaID FROM jobsActive4 WHERE jobStatus='transferring' AND modificationTime<'%s' LIMIT 20" \
      % timeLimit.strftime('%Y-%m-%d %H:%M:%S')
for ii in range(1000):
    ret,res = proxyS.querySQLS(sql)
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
# connect to MetaDB
proxyM = DBProxy()
proxyM.connect(panda_config.logdbhost,panda_config.logdbpasswd,panda_config.logdbuser,'PandaMetaDB')
# set email address
for name,addr in mailMap.iteritems():
    # remove _
    name = re.sub('_$','',name)
    status,res = proxyM.querySQLS("SELECT email FROM users WHERE name='%s'" % name)
    # failed or not found
    if status == -1 or len(res) == 0:
        _logger.error("%s not found in user DB" % name)
        continue
    # already set
    if res[0][0] != '':
        continue
    # update email
    _logger.debug("set '%s' to %s" % (name,addr))
    status,res = proxyM.querySQLS("UPDATE users SET email='%s' WHERE name='%s'" % (addr,name))

_memoryCheck("end")

_logger.debug("===================== end =====================")
