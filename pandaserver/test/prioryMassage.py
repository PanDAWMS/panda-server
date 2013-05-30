import os
import re
import sys
import datetime
from taskbuffer.TaskBuffer import taskBuffer
from pandalogger.PandaLogger import PandaLogger

# password
from config import panda_config
passwd = panda_config.dbpasswd

# logger
_logger = PandaLogger().getLogger('prioryMassage')

_logger.debug("================= start ==================")

# instantiate TB
taskBuffer.init(panda_config.dbhost,panda_config.dbpasswd,nDBConnection=1)

# get usage breakdown
usageBreakDownPerUser = {}
usageBreakDownPerSite = {}
workingGroupList = []
for table in ['ATLAS_PANDA.jobsActive4','ATLAS_PANDA.jobsArchived4']:
	varMap = {}
	varMap[':prodSourceLabel'] = 'user'
	if table == 'ATLAS_PANDA.jobsActive4':
		sql = "SELECT COUNT(*),prodUserName,jobStatus,workingGroup,computingSite FROM %s WHERE prodSourceLabel=:prodSourceLabel GROUP BY prodUserName,jobStatus,workingGroup,computingSite" % table
	else:
		# with time range for archived table
		varMap[':modificationTime'] = datetime.datetime.utcnow() - datetime.timedelta(minutes=60)
		sql = "SELECT COUNT(*),prodUserName,jobStatus,workingGroup,computingSite FROM %s WHERE prodSourceLabel=:prodSourceLabel AND modificationTime>:modificationTime GROUP BY prodUserName,jobStatus,workingGroup,computingSite" % table
	# exec 	
	status,res = taskBuffer.querySQLS(sql,varMap,arraySize=10000)
	if res == None:
		_logger.debug("total %s " % res)
	else:
		_logger.debug("total %s " % len(res))
		# make map
		for cnt,prodUserName,jobStatus,workingGroup,computingSite in res:
			# use workingGroup name as prodUserName
			if workingGroup != None:
				if not workingGroup in workingGroupList:
					workingGroupList.append(workingGroup)
				prodUserName = workingGroup
				workingGroup = None
			# append to PerUser map
			if not usageBreakDownPerUser.has_key(prodUserName):
				usageBreakDownPerUser[prodUserName] = {}
			if not usageBreakDownPerUser[prodUserName].has_key(workingGroup):
				usageBreakDownPerUser[prodUserName][workingGroup] = {}
			if not usageBreakDownPerUser[prodUserName][workingGroup].has_key(computingSite):
				usageBreakDownPerUser[prodUserName][workingGroup][computingSite] = {'rundone':0,'activated':0}
			# append to PerSite map
			if not usageBreakDownPerSite.has_key(computingSite):
				usageBreakDownPerSite[computingSite] = {}
			if not usageBreakDownPerSite[computingSite].has_key(prodUserName):
				usageBreakDownPerSite[computingSite][prodUserName] = {}
			if not usageBreakDownPerSite[computingSite][prodUserName].has_key(workingGroup):
				usageBreakDownPerSite[computingSite][prodUserName][workingGroup] = {'rundone':0,'activated':0}
			# count # of running/done and activated
			if jobStatus in ['activated']:
				usageBreakDownPerUser[prodUserName][workingGroup][computingSite]['activated'] += cnt
				usageBreakDownPerSite[computingSite][prodUserName][workingGroup]['activated'] += cnt
			elif jobStatus in ['cancelled','holding']:
				pass
			else:
				usageBreakDownPerUser[prodUserName][workingGroup][computingSite]['rundone'] += cnt
				usageBreakDownPerSite[computingSite][prodUserName][workingGroup]['rundone'] += cnt				

# get total number of users and running/done jobs
totalUsers = 0
totalRunDone = 0
for prodUserName,wgValMap in usageBreakDownPerUser.iteritems():
	for workingGroup,siteValMap in wgValMap.iteritems():
		# ignore group production
		if workingGroup != None:
			continue
		totalUsers += 1
		for computingSite,statValMap in siteValMap.iteritems():
			totalRunDone += statValMap['rundone']

_logger.debug("total users    : %s" % totalUsers)
_logger.debug("total RunDone  : %s" % totalRunDone)
_logger.debug("")

if totalUsers == 0:
	sys.exit(0)

# global average 
globalAverageRunDone = float(totalRunDone)/float(totalUsers)

_logger.debug("global average : %s" % globalAverageRunDone)

# count the number of users and run/done jobs for each site
siteRunDone = {}
siteUsers = {}
for computingSite,userValMap in usageBreakDownPerSite.iteritems():
	for prodUserName,wgValMap in userValMap.iteritems():
		for workingGroup,statValMap in wgValMap.iteritems():
			# ignore group production
			if workingGroup != None:
				continue
			# count the number of users and running/done jobs
			if not siteUsers.has_key(computingSite):
				siteUsers[computingSite] = 0
			siteUsers[computingSite] += 1
			if not siteRunDone.has_key(computingSite):
				siteRunDone[computingSite] = 0
			siteRunDone[computingSite] += statValMap['rundone']

# get site average
_logger.debug("site average")
siteAverageRunDone = {}
for computingSite,nRunDone in siteRunDone.iteritems():
	siteAverageRunDone[computingSite] = float(nRunDone)/float(siteUsers[computingSite])
	_logger.debug(" %-25s : %s" % (computingSite,siteAverageRunDone[computingSite]))	
	
# check if the number of user's jobs is lower than the average 
for prodUserName,wgValMap in usageBreakDownPerUser.iteritems():
	_logger.debug("---> %s" % prodUserName)
	# no private jobs
	if not wgValMap.has_key(None):
		_logger.debug("no private jobs")
		continue
	# count the number of running/done jobs 
	userTotalRunDone = 0
	for workingGroup,siteValMap in wgValMap.iteritems():
		if workingGroup != None:
			continue
		for computingSite,statValMap in siteValMap.iteritems():
			userTotalRunDone += statValMap['rundone']
	# no priority boost when the number of jobs is higher than the average			
	if userTotalRunDone >= globalAverageRunDone:
		_logger.debug("enough running %s > %s (global average)" % (userTotalRunDone,globalAverageRunDone))
		continue
	_logger.debug("user total:%s global average:%s" % (userTotalRunDone,globalAverageRunDone))
	# check with site average
	toBeBoostedSites = [] 
	for computingSite,statValMap in wgValMap[None].iteritems():
		# the number of running/done jobs is lower than the average and activated jobs are waiting
		if statValMap['rundone'] >= siteAverageRunDone[computingSite]:
			_logger.debug("enough running %s > %s (site average) at %s" % \
						  (statValMap['rundone'],siteAverageRunDone[computingSite],computingSite))
		elif statValMap['activated'] == 0:
			_logger.debug("no activated jobs at %s" % computingSite)
		else:
			toBeBoostedSites.append(computingSite)
	# no boost is required
	if toBeBoostedSites == []:
		_logger.debug("no sites to be boosted")
		continue
	# check special prioritized site 
	siteAccessForUser = {}
	varMap = {}
	varMap[':dn'] = prodUserName
	sql = "SELECT pandaSite,pOffset,status,workingGroups FROM ATLAS_PANDAMETA.siteAccess WHERE dn=:dn"
	status,res = taskBuffer.querySQLS(sql,varMap,arraySize=10000)
	if res != None:
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
		if siteAccessForUser.has_key(computingSite):
			totalW += siteAccessForUser[computingSite]
	totalW = float(totalW)		
	# the total number of jobs to be boosted
	numBoostedJobs = globalAverageRunDone - float(userTotalRunDone)
	# get quota
	quotaFactor = 1.0 + taskBuffer.checkQuota(prodUserName)
	_logger.debug("quota factor:%s" % quotaFactor)	
	# make priority boost
	nJobsPerPrioUnit = 5
	highestPrio = 1000
	for computingSite in toBeBoostedSites:
		weight = float(defaultW)
		if siteAccessForUser.has_key(computingSite):
			weight += float(siteAccessForUser[computingSite])
		weight /= totalW
		# the number of boosted jobs at the site
		numBoostedJobsSite = int(numBoostedJobs * weight / quotaFactor)
		_logger.debug("nSite:%s nAll:%s W:%s Q:%s at %s" % (numBoostedJobsSite,numBoostedJobs,weight,quotaFactor,computingSite))
		if numBoostedJobsSite/nJobsPerPrioUnit == 0:
			_logger.debug("too small number of jobs %s to be boosted at %s" % (numBoostedJobsSite,computingSite))
			continue
		# get the highest prio of activated jobs at the site
		varMap = {}
		varMap[':jobStatus'] = 'activated'
		varMap[':prodSourceLabel'] = 'user'
		varMap[':prodUserName'] = prodUserName
		varMap[':computingSite'] = computingSite
		sql = "SELECT MAX(currentPriority) FROM ATLAS_PANDA.jobsActive4 WHERE prodSourceLabel=:prodSourceLabel AND prodUserName=:prodUserName AND workingGroup IS NULL AND jobStatus=:jobStatus AND computingSite=:computingSite"
		status,res = taskBuffer.querySQLS(sql,varMap,arraySize=10)
		maxPrio = None
		if res != None:
			try:
				maxPrio = res[0][0]
			except:
				pass
		if maxPrio == None:
			_logger.debug("cannot get the highest prio at %s" % computingSite)
			continue
		# delta for priority boost
		prioDelta = highestPrio - maxPrio
		# already boosted
		if prioDelta <= 0:
			_logger.debug("already boosted (prio=%s) at %s" % (maxPrio,computingSite))
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
		sql += "WHERE prodSourceLabel=:prodSourceLabel "
		if prodUserName in workingGroupList:
			sql += "AND workingGroup=:prodUserName "
		else:
			sql += "AND prodUserName=:prodUserName AND workingGroup IS NULL "
		sql += "AND jobStatus=:jobStatus AND computingSite=:computingSite AND currentPriority>:minPrio "
		sql += "AND currentPriority<=:maxPrio AND rownum<=:rlimit"
		_logger.debug("boost %s" % str(varMap))
		status,res = taskBuffer.querySQLS(sql,varMap,arraySize=10)	
		_logger.debug("   database return : %s" % res)


# redo stalled analysis jobs
_logger.debug("=== redo stalled jobs")
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
	if resJ == None or len(resJ) == 0:
		pass
	else:
		# loop over all jobID/users
		for jobDefinitionID,prodUserName in resJ:
			_logger.debug(" user:%s jobID:%s" % (prodUserName,jobDefinitionID))
			# get stalled jobs
			varMap = {}
			varMap[':prodSourceLabel'] = 'user'
			varMap[':jobDefinitionID'] = jobDefinitionID
			varMap[':prodUserName']    = prodUserName
			stP,resP = taskBuffer.querySQLS(sqlP,varMap)
			if resP == None or len(resP) == 0:
				_logger.debug("  no PandaID")
				continue
			useLib    = False
			libStatus = None
			libGUID   = None
			libLFN    = None
			libDSName = None
			destReady = False
			# use the first PandaID
			for PandaID, in resP:
				_logger.debug("  check PandaID:%s" % PandaID)
				# get files
				varMap = {}
				varMap[':PandaID'] = PandaID
				varMap[':status']  = 'unknown'
				stF,resF = taskBuffer.querySQLS(sqlF,varMap)
				if resF == None or len(resF) == 0:
					_logger.debug("  no files")
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
							if resL == None or len(resL) == 0:
								_logger.error("  cannot find status of %s" % lfn)
								continue
							# check status
							guid,outFileStatus,pandaIDOutLibTgz,tmpLibDsName = resL[0]
							_logger.debug("  PandaID:%s produces %s:%s GUID=%s status=%s" % (pandaIDOutLibTgz,tmpLibDsName,lfn,guid,outFileStatus))
							libStatus = outFileStatus
							libGUID   = guid
							libDSName = tmpLibDsName
						elif filetype in ['log','output']:
							if destinationDBlock != None and re.search('_sub\d+$',destinationDBlock) != None:
								destReady = True
					break
			_logger.debug("  useLib:%s libStatus:%s libDsName:%s libLFN:%s libGUID:%s destReady:%s" % (useLib,libStatus,libDSName,libLFN,libGUID,destReady))
			if libStatus == 'failed':
				# delete downstream jobs
				_logger.debug("  -> delete downstream jobs")
				# FIXME
				#taskBuffer.deleteStalledJobs(libLFN)
			else:
				# activate
				if useLib and libStatus == 'ready' and (not libGUID in [None,'']) and (not libDSName in [None,'']):
					# update GUID
					_logger.debug("  set GUID:%s for %s" % (libGUID,libLFN))
					#retG = taskBuffer.setGUIDs([{'lfn':libLFN,'guid':libGUID}])
					# FIXME
					retG = True
					if not retG:
						_logger.error("  failed to update GUID for %s" % libLFN)
					else:
						# get PandaID with lib.tgz
						#ids = taskBuffer.updateInFilesReturnPandaIDs(libDSName,'ready')
						ids = []
						# get jobs
						jobs = taskBuffer.peekJobs(ids,fromActive=False,fromArchived=False,fromWaiting=False)
						# remove None and unknown
						acJobs = []
						for job in jobs:
							if job == None or job.jobStatus == 'unknown':
								continue
							acJobs.append(job)
						# activate
						_logger.debug("  -> activate downstream jobs")
						#taskBuffer.activateJobs(acJobs)
				else:
					# wait
					_logger.debug("  -> wait")
					varMap = {}
					varMap[':prodSourceLabel'] = 'user'
					varMap[':jobDefinitionID'] = jobDefinitionID
					varMap[':prodUserName']    = prodUserName
					# FIXME
					#stU,resU = taskBuffer.querySQLS(sqlU,varMap)
except:
	errtype,errvalue = sys.exc_info()[:2]
	_logger.error("failed to redo stalled jobs with %s %s" % (errtype,errvalue))
			    
_logger.debug("-------------- end")
