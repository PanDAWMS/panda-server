import os
import re
import sys
import datetime
from taskbuffer.TaskBuffer import taskBuffer
from pandalogger.PandaLogger import PandaLogger
from brokerage.SiteMapper import SiteMapper

# password
from config import panda_config
passwd = panda_config.dbpasswd

# logger
_logger = PandaLogger().getLogger('shareMgr')

_logger.debug("================= start ==================")

# instantiate TB
taskBuffer.init(panda_config.dbhost,panda_config.dbpasswd,nDBConnection=1)

# instantiate sitemapper
siteMapper = SiteMapper(taskBuffer)

# number of jobs to be activated per queue
nJobsPerQueue = 50

# priority threshold
prioCutoff = 950

# get high prio jobs without throttling
sql  = "SELECT distinct computingSite FROM ATLAS_PANDA.jobsActive4 "
sql += "WHERE jobStatus=:s1 AND prodSourceLabel IN (:p1) AND lockedBy=:lockedBy "
sql += "AND currentPriority>=:prioCutoff "
varMap = {}
varMap[':s1'] = 'throttled'
varMap[':p1'] = 'managed'
varMap[':lockedBy'] = 'jedi'
varMap[':prioCutoff'] = prioCutoff

# exec 	
status,res = taskBuffer.querySQLS(sql,varMap,arraySize=10000)
if res == None:
    _logger.debug("total %s " % res)
else:
    # release high prio jobs
    sql  = "UPDATE ATLAS_PANDA.jobsActive4 SET jobStatus=:newStatus "
    sql += "WHERE jobStatus=:oldStatus AND prodSourceLabel IN (:p1) AND lockedBy=:lockedBy "
    sql += "AND currentPriority>=:prioCutoff AND computingSite=:computingSite "
    # loop over computing sites
    for computingSite, in res:
	# get site spec
	if not siteMapper.checkSite(computingSite):
	    continue
	siteSpec = siteMapper.getSite(computingSite)
        # check if resource fair share is used
        if siteSpec.useResourceFairShare():
            varMap = {}
            varMap[':newStatus'] = 'activated'
            varMap[':oldStatus'] = 'throttled'
            varMap[':p1'] = 'managed'
            varMap[':lockedBy'] = 'jedi'
            varMap[':prioCutoff'] = prioCutoff
            varMap[':computingSite'] = computingSite
            status,res = taskBuffer.querySQLS(sql,varMap,arraySize=10000)


# get statistics
sql  = "SELECT COUNT(*),jobStatus,computingSite,cloud FROM ATLAS_PANDA.jobsActive4 "
sql += "WHERE jobStatus IN (:s1,:s2,:s3) AND prodSourceLabel IN (:p1) AND lockedBy=:lockedBy "
sql += "AND currentPriority<:prioCutoff "
sql += "GROUP BY jobStatus,computingSite,cloud "
varMap = {}
varMap[':s1'] = 'activated'
varMap[':s2'] = 'throttled'
varMap[':s3'] = 'running'
varMap[':p1'] = 'managed'
varMap[':lockedBy'] = 'jedi'
varMap[':prioCutoff'] = prioCutoff

# exec 	
status,res = taskBuffer.querySQLS(sql,varMap,arraySize=10000)
cloudJobStatMap = {}
cloudShareMap = {}
siteJobStatMap = {}
siteShareMap = {}
siteToRelease = set()
if res == None:
    _logger.debug("total %s " % res)
else:
    # make map
    for cnt,jobStatus,computingSite,jobCloud in res:
	# get site spec
	if not siteMapper.checkSite(computingSite):
	    continue
	siteSpec = siteMapper.getSite(computingSite)
        # check if resource fair share is used
        if not siteSpec.useResourceFairShare():
            if jobStatus =='throttled':
                siteToRelease.add(computingSite)
            continue
        # site or cloud
        if siteSpec.useSiteResourceFairShare():
            pandaSite = siteSpec.pandasite
            # add panda site
            if not pandaSite in siteJobStatMap:
                siteJobStatMap[pandaSite] = {}
            # add resource type
            resourceType = siteSpec.getResourceType()
            if not resourceType in siteJobStatMap[pandaSite]:
                siteJobStatMap[pandaSite][resourceType] = {}
            # add site
            if not computingSite in siteJobStatMap[pandaSite][resourceType]:
                siteJobStatMap[pandaSite][resourceType][computingSite] = {}
            # add job status
            if not jobStatus in siteJobStatMap[pandaSite][resourceType][computingSite]:
                siteJobStatMap[pandaSite][resourceType][computingSite][jobStatus] = 0
            siteJobStatMap[pandaSite][resourceType][computingSite][jobStatus] += (cnt*siteSpec.coreCount)
            # share map
            if not pandaSite in siteShareMap:
                siteShareMap[pandaSite] = {}
            if not resourceType in siteShareMap[pandaSite]:
                siteShareMap[pandaSite][resourceType] = {'share':siteSpec.sitershare,
                                                         'sites':set()}
            siteShareMap[pandaSite][resourceType]['sites'].add(computingSite)
        else:
            # use home cloud
            cloud = siteSpec.cloud
            # add cloud
            if not cloud in cloudJobStatMap:
                cloudJobStatMap[cloud] = {}
            # add resource type
            resourceType = siteSpec.getResourceType()
            if not resourceType in cloudJobStatMap[cloud]:
                cloudJobStatMap[cloud][resourceType] = {}
            # add site
            if not computingSite in cloudJobStatMap[cloud][resourceType]:
                cloudJobStatMap[cloud][resourceType][computingSite] = {}
            # add job status
            if not jobStatus in cloudJobStatMap[cloud][resourceType][computingSite]:
                cloudJobStatMap[cloud][resourceType][computingSite][jobStatus] = 0
            cloudJobStatMap[cloud][resourceType][computingSite][jobStatus] += (cnt*siteSpec.coreCount)
            # share map
            if not cloud in cloudShareMap:
                cloudShareMap[cloud] = {}
            if not resourceType in cloudShareMap[cloud]:
                cloudShareMap[cloud][resourceType] = {'share':siteSpec.cloudrshare,
                                                      'sites':set()}
            cloudShareMap[cloud][resourceType]['sites'].add(computingSite)


# site shares
for pandaSite,pandaSiteVal in siteJobStatMap.iteritems():
    _logger.debug("PandaSite : {0} {1}".format(pandaSite,str(pandaSiteVal)))
    # number of jobs per resource type
    resourceJobStatMap = {}
    for resourceType,resourceVal in pandaSiteVal.iteritems():
        resourceJobStatMap[resourceType] = {}
        for computingSite,siteVal in resourceVal.iteritems():
            for jobStatus,cnt in siteVal.iteritems():
                if not jobStatus in resourceJobStatMap[resourceType]:
                    resourceJobStatMap[resourceType][jobStatus] = 0
                resourceJobStatMap[resourceType][jobStatus] += cnt
    # skip if there are no activated/throttled jobs
    totalRunning = 0
    totalShare = 0
    for resourceType in resourceJobStatMap.keys():
        if not 'activated' in resourceJobStatMap[resourceType] and \
                not 'throttled' in resourceJobStatMap[resourceType]:
            del resourceJobStatMap[resourceType]
            continue
        if 'running' in resourceJobStatMap[resourceType]:
            totalRunning += resourceJobStatMap[resourceType]['running']
        totalShare += siteShareMap[pandaSite][resourceType]['share']
    # check if to be throttled
    for resourceType,resourceVal in resourceJobStatMap.iteritems():
        toThrottle = True
        if not 'running' in resourceVal:
            # no running and non-zero share
            if siteShareMap[pandaSite][resourceType]['share'] > 0:
                toThrottle = False
                _logger.debug("release {0} due to no running with share>0".format(resourceType))
        elif resourceVal['running']*totalShare < totalRunning*siteShareMap[pandaSite][resourceType]['share']:
            # less than share
            toThrottle = False
            _logger.debug("release {0} due to no running/totalRunning={1}/{2} < share/totalShare={3}/{4}".format(resourceType,
                                                                                                                 resourceVal['running'],
                                                                                                                 totalRunning,
                                                                                                                 siteShareMap[pandaSite][resourceType]['share'],
                                                                                                                 totalShare))
        # take action
        if toThrottle:
            # throttle
            for computingSite in siteShareMap[pandaSite][resourceType]['sites']:
                nJobs = taskBuffer.throttleJobsForResourceShare(computingSite)
                _logger.debug("throttled {0} jobs at {1} for site share".format(nJobs,computingSite))
        else:
            # activate
            for computingSite in siteShareMap[pandaSite][resourceType]['sites']:
                nJobs = taskBuffer.activateJobsForResourceShare(computingSite,nJobsPerQueue)
                _logger.debug("activated {0} jobs at {1} for site share".format(nJobs,computingSite))


# cloud shares        
for cloud,cloudVal in cloudJobStatMap.iteritems():
    _logger.debug("Cloud : {0} {1}".format(cloud,str(cloudVal)))
    # number of jobs per resource type
    resourceJobStatMap = {}
    for resourceType,resourceVal in cloudVal.iteritems():
        resourceJobStatMap[resourceType] = {}
        for computingSite,siteVal in resourceVal.iteritems():
            for jobStatus,cnt in siteVal.iteritems():
                if not jobStatus in resourceJobStatMap[resourceType]:
                    resourceJobStatMap[resourceType][jobStatus] = 0
                resourceJobStatMap[resourceType][jobStatus] += cnt
    # skip if there are no activated/throttled jobs
    totalRunning = 0
    totalShare = 0
    for resourceType in resourceJobStatMap.keys():
        if not 'activated' in resourceJobStatMap[resourceType] and \
                not 'throttled' in resourceJobStatMap[resourceType]:
            del resourceJobStatMap[resourceType]
            continue
        if 'running' in resourceJobStatMap[resourceType]:
            totalRunning += resourceJobStatMap[resourceType]['running']
        totalShare += cloudShareMap[cloud][resourceType]['share']
    # check if to be throttled
    for resourceType,resourceVal in resourceJobStatMap.iteritems():
        toThrottle = True
        if not 'running' in resourceVal:
            if cloudShareMap[cloud][resourceType]['share'] > 0:
                # no running and non-zero share
                toThrottle = False
                _logger.debug("release {0} due to no running with share>0".format(resourceType))
        elif resourceVal['running']*totalShare < totalRunning*cloudShareMap[cloud][resourceType]['share']:
            # less than share
            toThrottle = False
            _logger.debug("release {0} due to no running/totalRunning={1}/{2} < share/totalShare={3}/{4}".format(resourceType,
                                                                                                                 resourceVal['running'],
                                                                                                                 totalRunning,
                                                                                                                 cloudShareMap[cloud][resourceType]['share'],
                                                                                                                 totalShare))
        # take action
        if toThrottle:
            # throttle
            for computingSite in cloudShareMap[cloud][resourceType]['sites']:
                nJobs = taskBuffer.throttleJobsForResourceShare(computingSite)
                _logger.debug("throttled {0} jobs at {1} for cloud share".format(nJobs,computingSite))
        else:
            # activate
            for computingSite in cloudShareMap[cloud][resourceType]['sites']:
                nJobs = taskBuffer.activateJobsForResourceShare(computingSite,nJobsPerQueue)
                _logger.debug("activated {0} jobs at {1} for cloud share".format(nJobs,computingSite))


# release jobs
sql  = "UPDATE ATLAS_PANDA.jobsActive4 SET jobStatus=:newStatus "
sql += "WHERE jobStatus=:oldStatus AND computingSite=:computingSite AND lockedBy=:lockedBy "
for computingSite in siteToRelease:
    _logger.debug("release {0}".format(computingSite))
    varMap = {}
    varMap[':newStatus']     = 'activated'
    varMap[':oldStatus']     = 'throttled'
    varMap[':computingSite'] = computingSite
    varMap[':lockedBy'] = 'jedi'
    status,res = taskBuffer.querySQLS(sql,varMap)
    
			    
_logger.debug("================= end ==================")
