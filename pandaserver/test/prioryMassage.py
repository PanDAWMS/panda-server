import re
import sys
import datetime
import traceback
from pandaserver.taskbuffer.TaskBuffer import taskBuffer
from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandaserver.brokerage.SiteMapper import SiteMapper


# password
from pandaserver.config import panda_config
passwd = panda_config.dbpasswd

# logger
_logger = PandaLogger().getLogger('prioryMassage')
tmpLog = LogWrapper(_logger)


tmpLog.debug("================= start ==================")

# instantiate TB
taskBuffer.init(panda_config.dbhost,panda_config.dbpasswd,nDBConnection=1)

# instantiate sitemapper
siteMapper = SiteMapper(taskBuffer)

# get usage breakdown
usageBreakDownPerUser = {}
usageBreakDownPerSite = {}
workingGroupList = []
for table in ['ATLAS_PANDA.jobsActive4','ATLAS_PANDA.jobsArchived4']:
	varMap = {}
	varMap[':prodSourceLabel'] = 'user'
	varMap[':pmerge'] = 'pmerge'
	if table == 'ATLAS_PANDA.jobsActive4':
		sql = "SELECT COUNT(*),prodUserName,jobStatus,workingGroup,computingSite FROM %s WHERE prodSourceLabel=:prodSourceLabel AND processingType<>:pmerge GROUP BY prodUserName,jobStatus,workingGroup,computingSite" % table
	else:
		# with time range for archived table
		varMap[':modificationTime'] = datetime.datetime.utcnow() - datetime.timedelta(minutes=60)
		sql = "SELECT COUNT(*),prodUserName,jobStatus,workingGroup,computingSite FROM %s WHERE prodSourceLabel=:prodSourceLabel AND processingType<>:pmerge AND modificationTime>:modificationTime GROUP BY prodUserName,jobStatus,workingGroup,computingSite" % table
	# exec 	
	status,res = taskBuffer.querySQLS(sql,varMap,arraySize=10000)
	if res is None:
		tmpLog.debug("total %s " % res)
	else:
		tmpLog.debug("total %s " % len(res))
		# make map
		for cnt,prodUserName,jobStatus,workingGroup,computingSite in res:
			# append to PerUser map
			usageBreakDownPerUser.setdefault(prodUserName, {})
			usageBreakDownPerUser[prodUserName].setdefault(workingGroup, {})
			usageBreakDownPerUser[prodUserName][workingGroup].setdefault(computingSite,
																		 {'rundone':0,'activated':0,'running':0})
			# append to PerSite map
			usageBreakDownPerSite.setdefault(computingSite, {})
			usageBreakDownPerSite[computingSite].setdefault(prodUserName, {})
			usageBreakDownPerSite[computingSite][prodUserName].setdefault(workingGroup,
																		  {'rundone':0,'activated':0})
			# count # of running/done and activated
			if jobStatus in ['activated']:
				usageBreakDownPerUser[prodUserName][workingGroup][computingSite]['activated'] += cnt
				usageBreakDownPerSite[computingSite][prodUserName][workingGroup]['activated'] += cnt
			elif jobStatus in ['cancelled','holding']:
				pass
			else:
				if jobStatus in ['running','starting','sent']:
					usageBreakDownPerUser[prodUserName][workingGroup][computingSite]['running'] += cnt
				usageBreakDownPerUser[prodUserName][workingGroup][computingSite]['rundone'] += cnt
				usageBreakDownPerSite[computingSite][prodUserName][workingGroup]['rundone'] += cnt				

# get total number of users and running/done jobs
totalUsers = 0
totalRunDone = 0
usersTotalJobs = {}
for prodUserName in usageBreakDownPerUser:
	wgValMap = usageBreakDownPerUser[prodUserName]
	for workingGroup in wgValMap:
		siteValMap = wgValMap[workingGroup]
		totalUsers += 1
		for computingSite in siteValMap:
			statValMap = siteValMap[computingSite]
			totalRunDone += statValMap['rundone']
			usersTotalJobs.setdefault(prodUserName, {})
			usersTotalJobs[prodUserName].setdefault(workingGroup, 0)
			usersTotalJobs[prodUserName][workingGroup] += statValMap['running']

tmpLog.debug("total users    : %s" % totalUsers)
tmpLog.debug("total RunDone  : %s" % totalRunDone)

if totalUsers == 0:
	sys.exit(0)


# cap num of running jobs
tmpLog.debug("=== cap running jobs")
prodUserName = None
maxNumRunPerUser = taskBuffer.getConfigValue('prio_mgr','CAP_RUNNING_USER_JOBS')
maxNumRunPerGroup = taskBuffer.getConfigValue('prio_mgr','CAP_RUNNING_GROUP_JOBS')
if maxNumRunPerUser is None:
	maxNumRunPerUser = 10000
if maxNumRunPerGroup is None:
	maxNumRunPerGroup = 10000
try:
	throttledUsers = taskBuffer.getThrottledUsers()
	for prodUserName in usersTotalJobs:
		wgDict = usersTotalJobs[prodUserName]
		for workingGroup in wgDict:
			tmpNumTotal = wgDict[workingGroup]
			print (prodUserName,workingGroup,tmpNumTotal)
			if workingGroup is None:
				maxNumRun = maxNumRunPerUser
			else:
				maxNumRun = maxNumRunPerGroup
			if tmpNumTotal >= maxNumRun:
				# throttle user
				tmpNumJobs = taskBuffer.throttleUserJobs(prodUserName, workingGroup)
				if tmpNumJobs is not None and tmpNumJobs > 0:
					msg = 'throttled {0} jobs for user="{1}" group={2} since too many ({3}) running jobs'.format(
						tmpNumJobs, prodUserName, workingGroup,	maxNumRun)
					tmpLog.debug(msg)
					tmpLog.sendMsg(msg,panda_config.loggername,'userCap','warning')
			elif tmpNumTotal < maxNumRun*0.9 and (prodUserName, workingGroup) in throttledUsers:
				# throttle user
				tmpNumJobs = taskBuffer.unThrottleUserJobs(prodUserName, workingGroup)
				if tmpNumJobs is not None and tmpNumJobs > 0:
					msg = 'released jobs for user="{0}" group={1} since number of running jobs is less than {2}'.format(
						prodUserName, workingGroup,	maxNumRun)
					tmpLog.debug(msg)
					tmpLog.sendMsg(msg,panda_config.loggername,'userCap')
except Exception as e:
	errStr = "cap failed for %s : %s" % (prodUserName, str(e))
	errStr.strip()
	errStr += traceback.format_exc()
	tmpLog.error(errStr)
	

# global average 
tmpLog.debug("=== boost jobs")
globalAverageRunDone = float(totalRunDone)/float(totalUsers)

tmpLog.debug("global average : %s" % globalAverageRunDone)

# count the number of users and run/done jobs for each site
siteRunDone = {}
siteUsers = {}
for computingSite in usageBreakDownPerSite:
	userValMap = usageBreakDownPerSite[computingSite]
	for prodUserName in userValMap:
		wgValMap = userValMap[prodUserName]
		for workingGroup in wgValMap:
			statValMap = wgValMap[workingGroup]
			# count the number of users and running/done jobs
			siteUsers.setdefault(computingSite, 0)
			siteUsers[computingSite] += 1
			siteRunDone.setdefault(computingSite, 0)
			siteRunDone[computingSite] += statValMap['rundone']

# get site average
tmpLog.debug("site average")
siteAverageRunDone = {}
for computingSite in siteRunDone:
	nRunDone = siteRunDone[computingSite]
	siteAverageRunDone[computingSite] = float(nRunDone)/float(siteUsers[computingSite])
	tmpLog.debug(" %-25s : %s" % (computingSite,siteAverageRunDone[computingSite]))	
	
# check if the number of user's jobs is lower than the average 
for prodUserName in usageBreakDownPerUser:
	wgValMap = usageBreakDownPerUser[prodUserName]
	for workingGroup in wgValMap:
		tmpLog.debug("---> %s group=%s" % (prodUserName, workingGroup))
		# count the number of running/done jobs 
		userTotalRunDone = 0
		for computingSite in wgValMap[workingGroup]:
			statValMap = wgValMap[workingGroup][computingSite]
			userTotalRunDone += statValMap['rundone']
		# no priority boost when the number of jobs is higher than the average			
		if userTotalRunDone >= globalAverageRunDone:
			tmpLog.debug("enough running %s > %s (global average)" % (userTotalRunDone,globalAverageRunDone))
			continue
		tmpLog.debug("user total:%s global average:%s" % (userTotalRunDone,globalAverageRunDone))
		# check with site average
		toBeBoostedSites = [] 
		for computingSite in wgValMap[workingGroup]:
			statValMap = wgValMap[workingGroup][computingSite]
			# the number of running/done jobs is lower than the average and activated jobs are waiting
			if statValMap['rundone'] >= siteAverageRunDone[computingSite]:
				tmpLog.debug("enough running %s > %s (site average) at %s" % \
							  (statValMap['rundone'],siteAverageRunDone[computingSite],computingSite))
			elif statValMap['activated'] == 0:
				tmpLog.debug("no activated jobs at %s" % computingSite)
			else:
				toBeBoostedSites.append(computingSite)
		# no boost is required
		if toBeBoostedSites == []:
			tmpLog.debug("no sites to be boosted")
			continue
		# check special prioritized site 
		siteAccessForUser = {}
		varMap = {}
		varMap[':dn'] = prodUserName
		sql = "SELECT pandaSite,pOffset,status,workingGroups FROM ATLAS_PANDAMETA.siteAccess WHERE dn=:dn"
		status,res = taskBuffer.querySQLS(sql,varMap,arraySize=10000)
		if res is not None:
			for pandaSite,pOffset,pStatus,workingGroups in res:
				# ignore special working group for now
				if not workingGroups in ['',None]:
					continue
				# only approved sites
				if pStatus != 'approved':
					continue
				# no priority boost
				if pOffset == 0:
					continue
				# append
				siteAccessForUser[pandaSite] = pOffset
		# set weight
		totalW = 0
		defaultW = 100
		for computingSite in toBeBoostedSites:
			totalW += defaultW
			if computingSite in siteAccessForUser:
				totalW += siteAccessForUser[computingSite]
		totalW = float(totalW)		
		# the total number of jobs to be boosted
		numBoostedJobs = globalAverageRunDone - float(userTotalRunDone)
		# get quota
		quotaFactor = 1.0 + taskBuffer.checkQuota(prodUserName)
		tmpLog.debug("quota factor:%s" % quotaFactor)	
		# make priority boost
		nJobsPerPrioUnit = 5
		highestPrio = 1000
		for computingSite in toBeBoostedSites:
			weight = float(defaultW)
			if computingSite in siteAccessForUser:
				weight += float(siteAccessForUser[computingSite])
			weight /= totalW
			# the number of boosted jobs at the site
			numBoostedJobsSite = int(numBoostedJobs * weight / quotaFactor)
			tmpLog.debug("nSite:%s nAll:%s W:%s Q:%s at %s" % (numBoostedJobsSite,numBoostedJobs,weight,quotaFactor,computingSite))
			if numBoostedJobsSite/nJobsPerPrioUnit == 0:
				tmpLog.debug("too small number of jobs %s to be boosted at %s" % (numBoostedJobsSite,computingSite))
				continue
			# get the highest prio of activated jobs at the site
			varMap = {}
			varMap[':jobStatus'] = 'activated'
			varMap[':prodSourceLabel'] = 'user'
			varMap[':pmerge'] = 'pmerge'
			varMap[':prodUserName'] = prodUserName
			varMap[':computingSite'] = computingSite
			sql  = "SELECT MAX(currentPriority) FROM ATLAS_PANDA.jobsActive4 "
			sql += "WHERE prodSourceLabel=:prodSourceLabel AND jobStatus=:jobStatus AND computingSite=:computingSite "
			sql += "AND processingType<>:pmerge AND prodUserName=:prodUserName "
			if workingGroup is not None:
				varMap[':workingGroup'] = workingGroup
				sql += "AND workingGroup=:workingGroup "
			else:
				sql += "AND workingGroup IS NULL "
			status,res = taskBuffer.querySQLS(sql,varMap,arraySize=10)
			maxPrio = None
			if res is not None:
				try:
					maxPrio = res[0][0]
				except Exception:
					pass
			if maxPrio is None:
				tmpLog.debug("cannot get the highest prio at %s" % computingSite)
				continue
			# delta for priority boost
			prioDelta = highestPrio - maxPrio
			# already boosted
			if prioDelta <= 0:
				tmpLog.debug("already boosted (prio=%s) at %s" % (maxPrio,computingSite))
				continue
			# lower limit
			minPrio = maxPrio - numBoostedJobsSite/nJobsPerPrioUnit
			# SQL for priority boost
			varMap = {}
			varMap[':jobStatus'] = 'activated'
			varMap[':prodSourceLabel'] = 'user'
			varMap[':prodUserName'] = prodUserName
			varMap[':computingSite'] = computingSite
			varMap[':prioDelta'] = prioDelta
			varMap[':maxPrio'] = maxPrio
			varMap[':minPrio'] = minPrio
			varMap[':rlimit'] = numBoostedJobsSite
			sql  = "UPDATE ATLAS_PANDA.jobsActive4 SET currentPriority=currentPriority+:prioDelta "
			sql += "WHERE prodSourceLabel=:prodSourceLabel AND prodUserName=:prodUserName "
			if workingGroup is not None:
				varMap[':workingGroup'] = workingGroup
				sql += "AND workingGroup=:workingGroup "
			else:
				sql += "AND workingGroup IS NULL "
			sql += "AND jobStatus=:jobStatus AND computingSite=:computingSite AND currentPriority>:minPrio "
			sql += "AND currentPriority<=:maxPrio AND rownum<=:rlimit"
			tmpLog.debug("boost %s" % str(varMap))
			status,res = taskBuffer.querySQLS(sql,varMap,arraySize=10)	
			tmpLog.debug("   database return : %s" % res)


# redo stalled analysis jobs
tmpLog.debug("=== redo stalled jobs")
try:
	varMap = {}
	varMap[':prodSourceLabel'] = 'user'
	sqlJ =  "SELECT jobDefinitionID,prodUserName FROM ATLAS_PANDA.jobsDefined4 "
	sqlJ += "WHERE prodSourceLabel=:prodSourceLabel AND modificationTime<CURRENT_DATE-2/24 "
	sqlJ += "GROUP BY jobDefinitionID,prodUserName"
	sqlP  = "SELECT PandaID FROM ATLAS_PANDA.jobsDefined4 "
	sqlP += "WHERE jobDefinitionID=:jobDefinitionID ANd prodSourceLabel=:prodSourceLabel AND prodUserName=:prodUserName AND rownum <= 1"
	sqlF  = "SELECT lfn,type,destinationDBlock FROM ATLAS_PANDA.filesTable4 WHERE PandaID=:PandaID AND status=:status"
	sqlL  = "SELECT guid,status,PandaID,dataset FROM ATLAS_PANDA.filesTable4 WHERE lfn=:lfn AND type=:type"
	sqlA  = "SELECT PandaID FROM ATLAS_PANDA.jobsDefined4 "
	sqlA += "WHERE jobDefinitionID=:jobDefinitionID ANd prodSourceLabel=:prodSourceLabel AND prodUserName=:prodUserName"
	sqlU  = "UPDATE ATLAS_PANDA.jobsDefined4 SET modificationTime=CURRENT_DATE "
	sqlU += "WHERE jobDefinitionID=:jobDefinitionID ANd prodSourceLabel=:prodSourceLabel AND prodUserName=:prodUserName"
	# get stalled jobs
	staJ,resJ = taskBuffer.querySQLS(sqlJ,varMap)
	if resJ is None or len(resJ) == 0:
		pass
	else:
		# loop over all jobID/users
		for jobDefinitionID,prodUserName in resJ:
			tmpLog.debug(" user:%s jobID:%s" % (prodUserName,jobDefinitionID))
			# get stalled jobs
			varMap = {}
			varMap[':prodSourceLabel'] = 'user'
			varMap[':jobDefinitionID'] = jobDefinitionID
			varMap[':prodUserName']    = prodUserName
			stP,resP = taskBuffer.querySQLS(sqlP,varMap)
			if resP is None or len(resP) == 0:
				tmpLog.debug("  no PandaID")
				continue
			useLib    = False
			libStatus = None
			libGUID   = None
			libLFN    = None
			libDSName = None
			destReady = False
			# use the first PandaID
			for PandaID, in resP:
				tmpLog.debug("  check PandaID:%s" % PandaID)
				# get files
				varMap = {}
				varMap[':PandaID'] = PandaID
				varMap[':status']  = 'unknown'
				stF,resF = taskBuffer.querySQLS(sqlF,varMap)
				if resF is None or len(resF) == 0:
					tmpLog.debug("  no files")
				else:
					# get lib.tgz and destDBlock
					for lfn,filetype,destinationDBlock in resF:
						if filetype == 'input' and lfn.endswith('.lib.tgz'):
							useLib = True
							libLFN = lfn
							varMap = {}
							varMap[':lfn'] = lfn
							varMap[':type']  = 'output'
							stL,resL = taskBuffer.querySQLS(sqlL,varMap)
							# not found
							if resL is None or len(resL) == 0:
								tmpLog.error("  cannot find status of %s" % lfn)
								continue
							# check status
							guid,outFileStatus,pandaIDOutLibTgz,tmpLibDsName = resL[0]
							tmpLog.debug("  PandaID:%s produces %s:%s GUID=%s status=%s" % (pandaIDOutLibTgz,tmpLibDsName,lfn,guid,outFileStatus))
							libStatus = outFileStatus
							libGUID   = guid
							libDSName = tmpLibDsName
						elif filetype in ['log','output']:
							if destinationDBlock is not None and re.search('_sub\d+$',destinationDBlock) is not None:
								destReady = True
					break
			tmpLog.debug("  useLib:%s libStatus:%s libDsName:%s libLFN:%s libGUID:%s destReady:%s" % (useLib,libStatus,libDSName,libLFN,libGUID,destReady))
			if libStatus == 'failed':
				# delete downstream jobs
				tmpLog.debug("  -> delete downstream jobs")
				# FIXME
				#taskBuffer.deleteStalledJobs(libLFN)
			else:
				# activate
				if useLib and libStatus == 'ready' and (not libGUID in [None,'']) and (not libDSName in [None,'']):
					# update GUID
					tmpLog.debug("  set GUID:%s for %s" % (libGUID,libLFN))
					#retG = taskBuffer.setGUIDs([{'lfn':libLFN,'guid':libGUID}])
					# FIXME
					retG = True
					if not retG:
						tmpLog.error("  failed to update GUID for %s" % libLFN)
					else:
						# get PandaID with lib.tgz
						#ids = taskBuffer.updateInFilesReturnPandaIDs(libDSName,'ready')
						ids = []
						# get jobs
						jobs = taskBuffer.peekJobs(ids,fromActive=False,fromArchived=False,fromWaiting=False)
						# remove None and unknown
						acJobs = []
						for job in jobs:
							if job is None or job.jobStatus == 'unknown':
								continue
							acJobs.append(job)
						# activate
						tmpLog.debug("  -> activate downstream jobs")
						#taskBuffer.activateJobs(acJobs)
				else:
					# wait
					tmpLog.debug("  -> wait")
					varMap = {}
					varMap[':prodSourceLabel'] = 'user'
					varMap[':jobDefinitionID'] = jobDefinitionID
					varMap[':prodUserName']    = prodUserName
					# FIXME
					#stU,resU = taskBuffer.querySQLS(sqlU,varMap)
except Exception:
	errtype,errvalue = sys.exc_info()[:2]
	tmpLog.error("failed to redo stalled jobs with %s %s" % (errtype,errvalue))


# throttle WAN data access
tmpLog.debug("=== throttle WAN data access")
try:
	# max number of activated jobs with WAN access
	maxActivated = 5
	# get WAN data matrix
	wanMX = taskBuffer.getWanDataFlowMaxtrix()
	throttleForSink     = {}
	throttleForSource   = {}
	totalFlowFromSource = {}
	# loop over all sources to get total flows
	tmpLog.debug(" >>> checking limits")
	for sinkSite in wanMX:
		sinkMap = wanMX[sinkSite]
		totalFlowToSink = 0 
		# loop over all sinks
		for sourceSite in sinkMap:
			sourceMap = sinkMap[sourceSite]
			# get total flows
			totalFlowToSink += sourceMap['flow']
			totalFlowFromSource.setdefault(sourceSite, 0)
			totalFlowFromSource[sourceSite] += sourceMap['flow']
		# check limit for sink
		tmpSiteSpec = siteMapper.getSite(sinkSite)
		if siteMapper.checkSite(sinkSite) and tmpSiteSpec.wansinklimit*1024*1024*1024 > totalFlowToSink:
			throttleForSink[sinkSite] = False
			tmpLog.debug(" release Sink {0} : {1}bps (total) < {2}Gbps (limit)".format(sinkSite,totalFlowToSink,
												    tmpSiteSpec.wansinklimit))
		else:
			throttleForSink[sinkSite] = True
			tmpLog.debug(" throttle Sink {0} : {1}bps (total) > {2}Gbps (limit)".format(sinkSite,totalFlowToSink,
												     tmpSiteSpec.wansinklimit))
	# check limit for source
	for sourceSite in totalFlowFromSource:
		totalFlow = totalFlowFromSource[sourceSite]
		tmpSiteSpec = siteMapper.getSite(sourceSite)
		if siteMapper.checkSite(sourceSite) and tmpSiteSpec.wansourcelimit*1024*1024*1024 > totalFlow:
			throttleForSource[sourceSite] = False
			tmpLog.debug(" release Src {0} : {1}bps (total) < {2}Gbps (limit)".format(sourceSite,totalFlow,
																					  tmpSiteSpec.wansourcelimit))
		else:
			throttleForSource[sourceSite] = True
			tmpLog.debug(" throttle Src {0} : {1}bps (total) > {2}Gbps (limit)".format(sourceSite,totalFlow,
												    tmpSiteSpec.wansourcelimit))
	# loop over all sources to adjust flows
	tmpLog.debug(" >>> adjusting flows")
	for sinkSite in wanMX:
		sinkMap = wanMX[sinkSite]
		if throttleForSink[sinkSite]:
			# sink is throttled
			iJobs = 0
			for sourceSite in sinkMap:
				sourceMap = sinkMap[sourceSite]
				for prodUserName in sourceMap['user']:
					userMap = sourceMap['user'][prodUserName]
					for currentPriority in userMap['activated']['jobList']:
						jobList = userMap['activated']['jobList'][currentPriority]
						for pandaID in jobList:
							tmpStat = taskBuffer.throttleJob(pandaID)
							if tmpStat == 1:
								iJobs += 1
			tmpLog.debug(" throttled {0} jobs to {1}".format(iJobs,sinkSite))
		else:
			# no throttle on sink
			for sourceSite in sinkMap:
				sourceMap = sinkMap[sourceSite]
				# check if source is throttled
				if throttleForSource[sourceSite]:
					# throttled
					iJobs = 0
					for prodUserName in sourceMap['user']:
						userMap = sourceMap['user'][prodUserName]
						for currentPriority in userMap['activated']['jobList']:
							jobList = userMap['activated']['jobList'][currentPriority]
							for pandaID in jobList:
								tmpStat = taskBuffer.throttleJob(pandaID)
								if tmpStat == 1:
									iJobs += 1
					tmpLog.debug(" throttled {0} jobs from {1} to {2}".format(iJobs,sourceSite,sinkSite)) 
				else:
					# unthrottled
					iJobs = 0
					for prodUserName in sourceMap['user']:
						userMap = sourceMap['user'][prodUserName]
						if userMap['activated']['nJobs'] < maxActivated:
							# activate jobs with higher priorities
							currentPriorityList = list(userMap['throttled']['jobList'])
							currentPriorityList.sort()
							currentPriorityList.reverse()
							nActivated = userMap['activated']['nJobs']
							for currentPriority in currentPriorityList:
								if nActivated > maxActivated:
									break
								panaIDs = userMap['throttled']['jobList'][currentPriority]
								panaIDs.sort()
								for pandaID in panaIDs:
									tmpStat = taskBuffer.unThrottleJob(pandaID)
									if tmpStat == 1:
										nActivated += 1
										iJobs += 1
									if nActivated > maxActivated:
										break
					tmpLog.debug(" activated {0} jobs from {1} to {2}".format(iJobs,sourceSite,sinkSite)) 
except Exception:
	errtype,errvalue = sys.exc_info()[:2]
	tmpLog.error("failed to throttle WAN data access with %s %s" % (errtype,errvalue))


			    
tmpLog.debug("-------------- end")
