import datetime
from pandaserver.taskbuffer.TaskBuffer import taskBuffer
from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandaserver.brokerage.SiteMapper import SiteMapper
from pandaserver.taskbuffer import ErrorCode

# password
from pandaserver.config import panda_config

# logger
_logger = PandaLogger().getLogger('esPreemption')
tmpLog = LogWrapper(_logger)


tmpLog.debug("================= start ==================")

# instantiate TB
taskBuffer.init(panda_config.dbhost,panda_config.dbpasswd,nDBConnection=1)

# instantiate sitemapper
siteMapper = SiteMapper(taskBuffer)

# time limit
timeLimit = datetime.datetime.utcnow()-datetime.timedelta(minutes=15)

# get low priority ES jobs per site
sqlEsJobs  = "SELECT PandaID,computingSite,commandToPilot,startTime "
sqlEsJobs += "FROM {0}.jobsActive4 ".format(panda_config.schemaPANDA)
sqlEsJobs += "WHERE prodSourceLabel IN (:label1,:label2) AND eventService=:es "
sqlEsJobs += "AND currentPriority<:prio AND jobStatus=:jobStat "
sqlEsJobs += "ORDER BY currentPriority,PandaID "

varMap = {}
varMap[':label1'] = 'managed'
varMap[':label2'] = 'test'
varMap[':es'] = 1
varMap[':prio'] = 200
varMap[':jobStat'] = 'running'
# exec 	
status,res = taskBuffer.querySQLS(sqlEsJobs,varMap,arraySize=100000)
if res is None:
    tmpLog.debug("total %s " % res)
else:
    tmpLog.debug("total %s " % len(res))
    # get number of jobs per site
    siteJobsMap = {}
    for pandaID,siteName,commandToPilot,startTime in res:
        if not siteName in siteJobsMap:
            siteJobsMap[siteName] = {'running':[],
                                     'killing':[]}
        if commandToPilot == 'tobekilled':
            siteJobsMap[siteName]['killing'].append(pandaID)
        else:
            # kill only old jobs
            if startTime < timeLimit:
                siteJobsMap[siteName]['running'].append(pandaID)
    # sql to get number of high priority jobs
    sqlHiJobs  = "SELECT count(*) FROM {0}.jobsActive4 ".format(panda_config.schemaPANDA)
    sqlHiJobs += "WHERE prodSourceLabel=:label AND jobStatus IN (:jobStat1,:jobStat2) "
    sqlHiJobs += "AND currentPriority>=:prio AND computingSite=:site AND eventService IS NULL "
    sqlHiJobs += "AND startTime<:timeLimit "
    # sql to kill job
    sqlKill  = "UPDATE {0}.jobsActive4 ".format(panda_config.schemaPANDA)
    sqlKill += "SET commandToPilot=:com,supErrorCode=:code,supErrorDiag=:diag "
    sqlKill += "WHERE PandaID=:pandaID AND jobStatus=:jobStatus "
    # check all sites
    for siteName in siteJobsMap:
        jobsMap = siteJobsMap[siteName]
        # check jobseed
        siteSpec = siteMapper.getSite(siteName)
        # skip ES-only sites
        if siteSpec.getJobSeed() == 'es':
            continue
        # get number of high priority jobs
        varMap = {}
        varMap[':label'] = 'managed'
        varMap[':jobStat1'] = 'activated'
        varMap[':jobStat2'] = 'starting'
        varMap[':prio'] = 800
        varMap[':timeLimit'] = timeLimit
        status,res = taskBuffer.querySQLS(sqlHiJobs,varMap)
        if res is not None:
            nJobs = res[0][0]
            nJobsToKill = nJobs-len(siteJobsMap[siteName]['killing'])
            tmpLog.debug("site={0} nHighPrioJobs={1} nRunnigES={2} nKillingES={3} nESToKill={4}".format(siteName,nJobs,
                                                                                                        len(siteJobsMap[siteName]['running']),
                                                                                                        len(siteJobsMap[siteName]['killing']),
                                                                                                        nJobsToKill))
            # not enough jobs are being killed
            if nJobsToKill > 0:
                # kill ES jobs
                for pandaID in siteJobsMap[siteName]['running'][:nJobsToKill]:
                    tmpLog.debug("  kill PandaID={0}".format(pandaID))
                    varMap = {}
                    varMap[':pandaID'] = pandaID
                    varMap[':jobStatus'] = 'running'
                    varMap[':code'] = ErrorCode.EC_EventServicePreemption
                    varMap[':diag'] = 'preempted'
                    varMap[':com'] = 'tobekilled'
                    status,res = taskBuffer.querySQLS(sqlKill,varMap)

tmpLog.debug("================= end ==================")
