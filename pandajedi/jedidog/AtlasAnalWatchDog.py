import os
import re
import socket
import sys
import traceback

from pandacommon.pandalogger.PandaLogger import PandaLogger

from pandajedi.jedicore.MsgWrapper import MsgWrapper
from pandaserver.dataservice.activator import Activator

from .TypicalWatchDogBase import TypicalWatchDogBase

logger = PandaLogger().getLogger(__name__.split(".")[-1])


# watchdog for ATLAS analysis
class AtlasAnalWatchDog(TypicalWatchDogBase):
    # constructor
    def __init__(self, taskBufferIF, ddmIF):
        TypicalWatchDogBase.__init__(self, taskBufferIF, ddmIF)
        self.pid = f"{socket.getfqdn().split('.')[0]}-{os.getpid()}-dog"

    # main
    def doAction(self):
        try:
            # get logger
            origTmpLog = MsgWrapper(logger)
            origTmpLog.debug("start")
            # handle waiting jobs
            self.doForWaitingJobs()
            # throttle tasks if so many prestaging requests
            self.doForPreStaging()
            # priority massage
            self.doForPriorityMassage()
            # redo stalled analysis jobs
            self.doForRedoStalledJobs()
            # task share and priority boost
            self.doForTaskBoost()
            # action to set scout job data w/o scouts
            self.doActionToSetScoutJobData(origTmpLog)
        except Exception:
            errtype, errvalue = sys.exc_info()[:2]
            origTmpLog.error(f"failed with {errtype} {errvalue}")
        # return
        origTmpLog.debug("done")
        return self.SC_SUCCEEDED

    # handle waiting jobs
    def doForWaitingJobs(self):
        try:
            tmpLog = MsgWrapper(logger, "doForWaitingJobs label=user")
            # lock
            got_lock = self.taskBufferIF.lockProcess_JEDI(
                vo=self.vo,
                prodSourceLabel=self.prodSourceLabel,
                cloud=None,
                workqueue_id=None,
                resource_name=None,
                component="AtlasAnalWatchDog.doForWaitingJobs",
                pid=self.pid,
                timeLimit=0.5,
            )
            if not got_lock:
                tmpLog.debug("locked by another process. Skipped")
                return
            # check every 60 min
            checkInterval = 60
            # get lib.tgz for waiting jobs
            libList = self.taskBufferIF.getLibForWaitingRunJob_JEDI(self.vo, self.prodSourceLabel, checkInterval)
            tmpLog.debug(f"got {len(libList)} lib.tgz files")
            # activate or kill orphan jobs which were submitted to use lib.tgz when the lib.tgz was being produced
            for prodUserName, datasetName, tmpFileSpec in libList:
                tmpLog = MsgWrapper(logger, f"< #ATM #KV doForWaitingJobs jediTaskID={tmpFileSpec.jediTaskID} label=user >")
                tmpLog.debug("start")
                # check status of lib.tgz
                if tmpFileSpec.status == "failed":
                    # get buildJob
                    pandaJobSpecs = self.taskBufferIF.peekJobs([tmpFileSpec.PandaID], fromDefined=False, fromActive=False, fromWaiting=False)
                    pandaJobSpec = pandaJobSpecs[0]
                    if pandaJobSpec is not None:
                        # kill
                        self.taskBufferIF.updateJobs([pandaJobSpec], False)
                        tmpLog.debug(f'  action=killed_downstream_jobs for user="{prodUserName}" with libDS={datasetName}')
                    else:
                        # PandaJobSpec not found
                        tmpLog.error(f'  cannot find PandaJobSpec for user="{prodUserName}" with PandaID={tmpFileSpec.PandaID}')
                elif tmpFileSpec.status == "finished":
                    # set metadata
                    self.taskBufferIF.setGUIDs(
                        [
                            {
                                "guid": tmpFileSpec.GUID,
                                "lfn": tmpFileSpec.lfn,
                                "checksum": tmpFileSpec.checksum,
                                "fsize": tmpFileSpec.fsize,
                                "scope": tmpFileSpec.scope,
                            }
                        ]
                    )
                    # get lib dataset
                    dataset = self.taskBufferIF.queryDatasetWithMap({"name": datasetName})
                    if dataset is not None:
                        # activate jobs
                        aThr = Activator(self.taskBufferIF, dataset)
                        aThr.run()
                        tmpLog.debug(f'  action=activated_downstream_jobs for user="{prodUserName}" with libDS={datasetName}')
                    else:
                        # datasetSpec not found
                        tmpLog.error(f'  cannot find datasetSpec for user="{prodUserName}" with libDS={datasetName}')
                else:
                    # lib.tgz is not ready
                    tmpLog.debug(f'  keep waiting for user="{prodUserName}" libDS={datasetName}')
        except Exception:
            errtype, errvalue = sys.exc_info()[:2]
            tmpLog.error(f"failed with {errtype} {errvalue} {traceback.format_exc()}")

    # throttle tasks if so many prestaging requests
    def doForPreStaging(self):
        try:
            tmpLog = MsgWrapper(logger, " #ATM #KV doForPreStaging label=user")
            tmpLog.debug("start")
            # lock
            got_lock = self.taskBufferIF.lockProcess_JEDI(
                vo=self.vo,
                prodSourceLabel=self.prodSourceLabel,
                cloud=None,
                workqueue_id=None,
                resource_name=None,
                component="AtlasAnalWatchDog.doForPreStaging",
                pid=self.pid,
                timeLimit=0.5,
            )
            if not got_lock:
                tmpLog.debug("locked by another process. Skipped")
                return
            # get throttled users
            thrUserTasks = self.taskBufferIF.getThrottledUsersTasks_JEDI(self.vo, self.prodSourceLabel)
            # get dispatch datasets
            dispUserTasks = self.taskBufferIF.getDispatchDatasetsPerUser(self.vo, self.prodSourceLabel, True, True)
            # max size of prestaging requests in GB
            maxPrestaging = self.taskBufferIF.getConfigValue("anal_watchdog", "USER_PRESTAGE_LIMIT", "jedi", "atlas")
            if maxPrestaging is None:
                maxPrestaging = 1024
            # max size of transfer requests in GB
            maxTransfer = self.taskBufferIF.getConfigValue("anal_watchdog", "USER_TRANSFER_LIMIT", "jedi", "atlas")
            if maxTransfer is None:
                maxTransfer = 1024
            # throttle interval
            thrInterval = 120
            # loop over all users
            for userName, userDict in dispUserTasks.items():
                # loop over all transfer types
                for transferType, maxSize in [("prestaging", maxPrestaging), ("transfer", maxTransfer)]:
                    if transferType not in userDict:
                        continue
                    userTotal = int(userDict[transferType]["size"] / 1024)
                    tmpLog.debug(f"user={userName} {transferType} total={userTotal} GB")
                    # too large
                    if userTotal > maxSize:
                        tmpLog.debug(f"user={userName} has too large {transferType} total={userTotal} GB > limit={maxSize} GB")
                        # throttle tasks
                        for taskID in userDict[transferType]["tasks"]:
                            if userName not in thrUserTasks or transferType not in thrUserTasks[userName] or taskID not in thrUserTasks[userName][transferType]:
                                tmpLog.debug(f"action=throttle_{transferType} jediTaskID={taskID} for user={userName}")
                                errDiag = f"throttled since transferring large data volume in total={userTotal}GB > limit={maxSize}GB type={transferType}"
                                self.taskBufferIF.throttleTask_JEDI(taskID, thrInterval, errDiag)
                        # remove the user from the list
                        if userName in thrUserTasks and transferType in thrUserTasks[userName]:
                            del thrUserTasks[userName][transferType]
            # release users
            for userName, taskData in thrUserTasks.items():
                for transferType, taskIDs in taskData.items():
                    tmpLog.debug(f"user={userName} release throttled tasks with {transferType}")
                    # unthrottle tasks
                    for taskID in taskIDs:
                        tmpLog.debug(f"action=release_{transferType} jediTaskID={taskID} for user={userName}")
                        self.taskBufferIF.release_task_on_hold(taskID, "throttled")
            tmpLog.debug("done")
        except Exception:
            errtype, errvalue = sys.exc_info()[:2]
            tmpLog.error(f"failed with {errtype} {errvalue} {traceback.format_exc()}")

    # priority massage
    def doForPriorityMassage(self):
        tmpLog = MsgWrapper(logger, " #ATM #KV doForPriorityMassage label=user")
        tmpLog.debug("start")
        # lock
        got_lock = self.taskBufferIF.lockProcess_JEDI(
            vo=self.vo,
            prodSourceLabel=self.prodSourceLabel,
            cloud=None,
            workqueue_id=None,
            resource_name=None,
            component="AtlasAnalWatchDog.doForPriorityMassage",
            pid=self.pid,
            timeLimit=6,
        )
        if not got_lock:
            tmpLog.debug("locked by another process. Skipped")
            return
        try:
            # get usage breakdown
            usageBreakDownPerUser, usageBreakDownPerSite = self.taskBufferIF.getUsageBreakdown_JEDI(self.prodSourceLabel)
            # get total number of users and running/done jobs
            totalUsers = 0
            totalRunDone = 0
            usersTotalJobs = {}
            usersTotalCores = {}
            for prodUserName in usageBreakDownPerUser:
                wgValMap = usageBreakDownPerUser[prodUserName]
                for workingGroup in wgValMap:
                    siteValMap = wgValMap[workingGroup]
                    totalUsers += 1
                    for computingSite in siteValMap:
                        statValMap = siteValMap[computingSite]
                        totalRunDone += statValMap["rundone"]
                        usersTotalJobs.setdefault(prodUserName, {})
                        usersTotalJobs[prodUserName].setdefault(workingGroup, 0)
                        usersTotalJobs[prodUserName][workingGroup] += statValMap["running"]
                        usersTotalCores.setdefault(prodUserName, {})
                        usersTotalCores[prodUserName].setdefault(workingGroup, 0)
                        usersTotalCores[prodUserName][workingGroup] += statValMap["runcores"]
            tmpLog.debug(f"total {totalUsers} users, {totalRunDone} RunDone jobs")
            # skip if no user
            if totalUsers == 0:
                tmpLog.debug("no user. Skipped...")
                return
            # cap num of running jobs
            tmpLog.debug("cap running jobs")
            prodUserName = None
            maxNumRunPerUser = self.taskBufferIF.getConfigValue("prio_mgr", "CAP_RUNNING_USER_JOBS")
            maxNumRunPerGroup = self.taskBufferIF.getConfigValue("prio_mgr", "CAP_RUNNING_GROUP_JOBS")
            maxNumCorePerUser = self.taskBufferIF.getConfigValue("prio_mgr", "CAP_RUNNING_USER_CORES")
            maxNumCorePerGroup = self.taskBufferIF.getConfigValue("prio_mgr", "CAP_RUNNING_GROUP_CORES")
            if maxNumRunPerUser is None:
                maxNumRunPerUser = 10000
            if maxNumRunPerGroup is None:
                maxNumRunPerGroup = 10000
            if maxNumCorePerUser is None:
                maxNumCorePerUser = 10000
            if maxNumCorePerGroup is None:
                maxNumCorePerGroup = 10000
            try:
                throttledUsers = self.taskBufferIF.getThrottledUsers()
                for prodUserName in usersTotalJobs:
                    for workingGroup in usersTotalJobs[prodUserName]:
                        tmpNumTotalJobs = usersTotalJobs[prodUserName][workingGroup]
                        tmpNumTotalCores = usersTotalCores[prodUserName][workingGroup]
                        if workingGroup is None:
                            maxNumRun = maxNumRunPerUser
                            maxNumCore = maxNumCorePerUser
                        else:
                            maxNumRun = maxNumRunPerGroup
                            maxNumCore = maxNumCorePerGroup
                        if tmpNumTotalJobs >= maxNumRun or tmpNumTotalCores >= maxNumCore:
                            # throttle user
                            tmpNumJobs = self.taskBufferIF.throttleUserJobs(prodUserName, workingGroup, get_dict=True)
                            if tmpNumJobs is not None:
                                for tmpJediTaskID, tmpNumJob in tmpNumJobs.items():
                                    msg = (
                                        'throttled {} jobs in jediTaskID={} for user="{}" group={} ' "since too many running jobs ({} > {}) or cores ({} > {}) "
                                    ).format(tmpNumJob, tmpJediTaskID, prodUserName, workingGroup, tmpNumTotalJobs, maxNumRun, tmpNumTotalCores, maxNumCore)
                                    tmpLog.debug(msg)
                                    tmpLog.sendMsg(msg, "userCap", msgLevel="warning")
                        elif tmpNumTotalJobs < maxNumRun * 0.9 and tmpNumTotalCores < maxNumCore * 0.9 and (prodUserName, workingGroup) in throttledUsers:
                            # unthrottle user
                            tmpNumJobs = self.taskBufferIF.unThrottleUserJobs(prodUserName, workingGroup, get_dict=True)
                            if tmpNumJobs is not None:
                                for tmpJediTaskID, tmpNumJob in tmpNumJobs.items():
                                    msg = (
                                        'released {} jobs in jediTaskID={} for user="{}" group={} '
                                        "since number of running jobs and cores are less than {} and {}"
                                    ).format(tmpNumJob, tmpJediTaskID, prodUserName, workingGroup, maxNumRun, maxNumCore)
                                    tmpLog.debug(msg)
                                    tmpLog.sendMsg(msg, "userCap")
            except Exception as e:
                errStr = f"cap failed for {prodUserName} : {str(e)}"
                errStr.strip()
                errStr += traceback.format_exc()
                tmpLog.error(errStr)
            # to boost
            tmpLog.debug("boost jobs")
            # global average
            globalAverageRunDone = float(totalRunDone) / float(totalUsers)
            tmpLog.debug(f"global average: {globalAverageRunDone}")
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
                        siteRunDone[computingSite] += statValMap["rundone"]
            # get site average
            tmpLog.debug("site average")
            siteAverageRunDone = {}
            for computingSite in siteRunDone:
                nRunDone = siteRunDone[computingSite]
                siteAverageRunDone[computingSite] = float(nRunDone) / float(siteUsers[computingSite])
                tmpLog.debug(" %-25s : %s" % (computingSite, siteAverageRunDone[computingSite]))
            # check if the number of user's jobs is lower than the average
            for prodUserName in usageBreakDownPerUser:
                wgValMap = usageBreakDownPerUser[prodUserName]
                for workingGroup in wgValMap:
                    tmpLog.debug(f"---> {prodUserName} group={workingGroup}")
                    # count the number of running/done jobs
                    userTotalRunDone = 0
                    for computingSite in wgValMap[workingGroup]:
                        statValMap = wgValMap[workingGroup][computingSite]
                        userTotalRunDone += statValMap["rundone"]
                    # no priority boost when the number of jobs is higher than the average
                    if userTotalRunDone >= globalAverageRunDone:
                        tmpLog.debug(f"enough running {userTotalRunDone} > {globalAverageRunDone} (global average)")
                        continue
                    tmpLog.debug(f"user total:{userTotalRunDone} global average:{globalAverageRunDone}")
                    # check with site average
                    toBeBoostedSites = []
                    for computingSite in wgValMap[workingGroup]:
                        statValMap = wgValMap[workingGroup][computingSite]
                        # the number of running/done jobs is lower than the average and activated jobs are waiting
                        if statValMap["rundone"] >= siteAverageRunDone[computingSite]:
                            tmpLog.debug(f"enough running {statValMap['rundone']} > {siteAverageRunDone[computingSite]} (site average) at {computingSite}")
                        elif statValMap["activated"] == 0:
                            tmpLog.debug(f"no activated jobs at {computingSite}")
                        else:
                            toBeBoostedSites.append(computingSite)
                    # no boost is required
                    if toBeBoostedSites == []:
                        tmpLog.debug("no sites to be boosted")
                        continue

                    # set weight
                    totalW = 0
                    defaultW = 100
                    for _ in toBeBoostedSites:
                        totalW += defaultW

                    totalW = float(totalW)
                    # the total number of jobs to be boosted
                    numBoostedJobs = globalAverageRunDone - float(userTotalRunDone)
                    # get quota
                    quotaFactor = 1.0 + self.taskBufferIF.checkQuota(prodUserName)
                    tmpLog.debug(f"quota factor:{quotaFactor}")
                    # make priority boost
                    nJobsPerPrioUnit = 5
                    highestPrio = 1000
                    for computingSite in toBeBoostedSites:
                        weight = float(defaultW)
                        weight /= totalW
                        # the number of boosted jobs at the site
                        numBoostedJobsSite = int(numBoostedJobs * weight / quotaFactor)
                        tmpLog.debug(f"nSite:{numBoostedJobsSite} nAll:{numBoostedJobs} W:{weight} Q:{quotaFactor} at {computingSite}")
                        if numBoostedJobsSite / nJobsPerPrioUnit == 0:
                            tmpLog.debug(f"too small number of jobs {numBoostedJobsSite} to be boosted at {computingSite}")
                            continue
                        # get the highest prio of activated jobs at the site
                        varMap = {}
                        varMap[":jobStatus"] = "activated"
                        varMap[":prodSourceLabel"] = self.prodSourceLabel
                        varMap[":pmerge"] = "pmerge"
                        varMap[":prodUserName"] = prodUserName
                        varMap[":computingSite"] = computingSite
                        sql = "SELECT MAX(currentPriority) FROM ATLAS_PANDA.jobsActive4 "
                        sql += "WHERE prodSourceLabel=:prodSourceLabel AND jobStatus=:jobStatus AND computingSite=:computingSite "
                        sql += "AND processingType<>:pmerge AND prodUserName=:prodUserName "
                        if workingGroup is not None:
                            varMap[":workingGroup"] = workingGroup
                            sql += "AND workingGroup=:workingGroup "
                        else:
                            sql += "AND workingGroup IS NULL "
                        res = self.taskBufferIF.querySQL(sql, varMap, arraySize=10)
                        maxPrio = None
                        if res is not None:
                            try:
                                maxPrio = res[0][0]
                            except Exception:
                                pass
                        if maxPrio is None:
                            tmpLog.debug(f"cannot get the highest prio at {computingSite}")
                            continue
                        # delta for priority boost
                        prioDelta = highestPrio - maxPrio
                        # already boosted
                        if prioDelta <= 0:
                            tmpLog.debug(f"already boosted (prio={maxPrio}) at {computingSite}")
                            continue
                        # lower limit
                        minPrio = maxPrio - numBoostedJobsSite / nJobsPerPrioUnit
                        # SQL for priority boost
                        varMap = {}
                        varMap[":jobStatus"] = "activated"
                        varMap[":prodSourceLabel"] = self.prodSourceLabel
                        varMap[":prodUserName"] = prodUserName
                        varMap[":computingSite"] = computingSite
                        varMap[":prioDelta"] = prioDelta
                        varMap[":maxPrio"] = maxPrio
                        varMap[":minPrio"] = minPrio
                        varMap[":rlimit"] = numBoostedJobsSite
                        sql = "UPDATE ATLAS_PANDA.jobsActive4 SET currentPriority=currentPriority+:prioDelta "
                        sql += "WHERE prodSourceLabel=:prodSourceLabel AND prodUserName=:prodUserName "
                        if workingGroup is not None:
                            varMap[":workingGroup"] = workingGroup
                            sql += "AND workingGroup=:workingGroup "
                        else:
                            sql += "AND workingGroup IS NULL "
                        sql += "AND jobStatus=:jobStatus AND computingSite=:computingSite AND currentPriority>:minPrio "
                        sql += "AND currentPriority<=:maxPrio AND rownum<=:rlimit"
                        tmpLog.debug(f"boost {str(varMap)}")
                        res = self.taskBufferIF.querySQL(sql, varMap, arraySize=10)
                        tmpLog.debug(f"   database return : {res}")
            # done
            tmpLog.debug("done")
        except Exception:
            errtype, errvalue = sys.exc_info()[:2]
            tmpLog.error(f"failed with {errtype} {errvalue} {traceback.format_exc()}")

    # redo stalled analysis jobs
    def doForRedoStalledJobs(self):
        tmpLog = MsgWrapper(logger, " #ATM #KV doForRedoStalledJobs label=user")
        tmpLog.debug("start")
        # lock
        got_lock = self.taskBufferIF.lockProcess_JEDI(
            vo=self.vo,
            prodSourceLabel=self.prodSourceLabel,
            cloud=None,
            workqueue_id=None,
            resource_name=None,
            component="AtlasAnalWatchDog.doForRedoStalledJobs",
            pid=self.pid,
            timeLimit=6,
        )
        if not got_lock:
            tmpLog.debug("locked by another process. Skipped")
            return
        # redo stalled analysis jobs
        tmpLog.debug("redo stalled jobs")
        try:
            varMap = {":prodSourceLabel": self.prodSourceLabel}

            sqlJ = "SELECT jobDefinitionID,prodUserName FROM ATLAS_PANDA.jobsDefined4 "
            sqlJ += "WHERE prodSourceLabel=:prodSourceLabel AND modificationTime<CURRENT_DATE-2/24 "
            sqlJ += "GROUP BY jobDefinitionID,prodUserName"

            sqlP = "SELECT PandaID FROM ATLAS_PANDA.jobsDefined4 "
            sqlP += "WHERE jobDefinitionID=:jobDefinitionID ANd prodSourceLabel=:prodSourceLabel AND prodUserName=:prodUserName AND rownum <= 1"

            sqlF = "SELECT lfn,type,destinationDBlock FROM ATLAS_PANDA.filesTable4 WHERE PandaID=:PandaID AND status=:status"

            sqlL = "SELECT guid,status,PandaID,dataset FROM ATLAS_PANDA.filesTable4 WHERE lfn=:lfn AND type=:type"

            sqlA = "SELECT PandaID FROM ATLAS_PANDA.jobsDefined4 "
            sqlA += "WHERE jobDefinitionID=:jobDefinitionID ANd prodSourceLabel=:prodSourceLabel AND prodUserName=:prodUserName"

            sqlU = "UPDATE ATLAS_PANDA.jobsDefined4 SET modificationTime=CURRENT_DATE "
            sqlU += "WHERE jobDefinitionID=:jobDefinitionID ANd prodSourceLabel=:prodSourceLabel AND prodUserName=:prodUserName"

            # get stalled jobs
            resJ = self.taskBufferIF.querySQL(sqlJ, varMap)
            if resJ is None or len(resJ) == 0:
                pass
            else:
                # loop over all jobID/users
                for jobDefinitionID, prodUserName in resJ:
                    tmpLog.debug(f" user:{prodUserName} jobID:{jobDefinitionID}")
                    # get stalled jobs
                    varMap = {":prodSourceLabel": self.prodSourceLabel, ":jobDefinitionID": jobDefinitionID, ":prodUserName": prodUserName}
                    resP = self.taskBufferIF.querySQL(sqlP, varMap)
                    if resP is None or len(resP) == 0:
                        tmpLog.debug("  no PandaID")
                        continue
                    useLib = False
                    libStatus = None
                    libGUID = None
                    libLFN = None
                    libDSName = None
                    destReady = False
                    # use the first PandaID
                    for (PandaID,) in resP:
                        tmpLog.debug(f"  check PandaID:{PandaID}")
                        # get files
                        varMap = {}
                        varMap[":PandaID"] = PandaID
                        varMap[":status"] = "unknown"
                        resF = self.taskBufferIF.querySQL(sqlF, varMap)
                        if resF is None or len(resF) == 0:
                            tmpLog.debug("  no files")
                        else:
                            # get lib.tgz and destDBlock
                            for lfn, filetype, destinationDBlock in resF:
                                if filetype == "input" and lfn.endswith(".lib.tgz"):
                                    useLib = True
                                    libLFN = lfn
                                    varMap = {}
                                    varMap[":lfn"] = lfn
                                    varMap[":type"] = "output"
                                    resL = self.taskBufferIF.querySQL(sqlL, varMap)
                                    # not found
                                    if resL is None or len(resL) == 0:
                                        tmpLog.error(f"  cannot find status of {lfn}")
                                        continue
                                    # check status
                                    guid, outFileStatus, pandaIDOutLibTgz, tmpLibDsName = resL[0]
                                    tmpLog.debug(f"  PandaID:{pandaIDOutLibTgz} produces {tmpLibDsName}:{lfn} GUID={guid} status={outFileStatus}")
                                    libStatus = outFileStatus
                                    libGUID = guid
                                    libDSName = tmpLibDsName
                                elif filetype in ["log", "output"]:
                                    if destinationDBlock is not None and re.search("_sub\d+$", destinationDBlock) is not None:
                                        destReady = True
                            break
                    tmpLog.debug(f"  useLib:{useLib} libStatus:{libStatus} libDsName:{libDSName} libLFN:{libLFN} libGUID:{libGUID} destReady:{destReady}")
                    if libStatus == "failed":
                        # delete downstream jobs
                        tmpLog.debug("  -> delete downstream jobs")
                        # FIXME
                        # self.taskBufferIF.deleteStalledJobs(libLFN)
                    else:
                        # activate
                        if useLib and libStatus == "ready" and (libGUID not in [None, ""]) and (libDSName not in [None, ""]):
                            # update GUID
                            tmpLog.debug(f"  set GUID:{libGUID} for {libLFN}")
                            # retG = self.taskBufferIF.setGUIDs([{'lfn':libLFN,'guid':libGUID}])
                            # FIXME
                            retG = True
                            if not retG:
                                tmpLog.error(f"  failed to update GUID for {libLFN}")
                            else:
                                # get PandaID with lib.tgz
                                # ids = self.taskBufferIF.updateInFilesReturnPandaIDs(libDSName,'ready')
                                ids = []
                                # get jobs
                                jobs = self.taskBufferIF.peekJobs(ids, fromActive=False, fromArchived=False, fromWaiting=False)
                                # remove None and unknown
                                acJobs = []
                                for job in jobs:
                                    if job is None or job.jobStatus == "unknown":
                                        continue
                                    acJobs.append(job)
                                # activate
                                tmpLog.debug("  -> activate downstream jobs")
                                # self.taskBufferIF.activateJobs(acJobs)
                        else:
                            # wait
                            tmpLog.debug("  -> wait")
                            varMap = {":prodSourceLabel": self.prodSourceLabel, ":jobDefinitionID": jobDefinitionID, ":prodUserName": prodUserName}
                            # FIXME
                            # resU = self.taskBufferIF.querySQL(sqlU, varMap)
            # done
            tmpLog.debug("done")
        except Exception:
            errtype, errvalue = sys.exc_info()[:2]
            tmpLog.error(f"failed to redo stalled jobs with {errtype} {errvalue} {traceback.format_exc()}")

    # task share and priority boost
    def doForTaskBoost(self):
        tmpLog = MsgWrapper(logger, " #ATM #KV doForTaskBoost label=user")
        tmpLog.debug("start")
        # lock
        got_lock = self.taskBufferIF.lockProcess_JEDI(
            vo=self.vo,
            prodSourceLabel=self.prodSourceLabel,
            cloud=None,
            workqueue_id=None,
            resource_name=None,
            component="AtlasAnalWatchDog.doForTaskBoost",
            pid=self.pid,
            timeLimit=5,
        )
        if not got_lock:
            tmpLog.debug("locked by another process. Skipped")
            return
        try:
            # get active tasks in S-class
            sql_get_tasks = (
                """SELECT tev.value_json.task_id, tev.value_json."user" """
                """FROM ATLAS_PANDA.Task_Evaluation tev, ATLAS_PANDA.JEDI_Tasks t """
                """WHERE tev.value_json.task_id=t.jediTaskID """
                """AND tev.metric='analy_task_eval' """
                """AND tev.value_json.class=:s_class """
                """AND tev.value_json.gshare=:gshare """
                """AND t.gshare=:gshare """
            )
            # varMap
            varMap = {
                ":s_class": 2,
                ":gshare": "User Analysis",
            }
            # result
            res = self.taskBufferIF.querySQL(sql_get_tasks, varMap)
            #  Assign to Express Analysis
            new_share = "Express Analysis"
            for task_id, user in res:
                tmpLog.info(
                    f" >>> action=gshare_reassignment jediTaskID={task_id} from gshare_old={varMap[':gshare']} to gshare_new={new_share} #ATM #KV label=user"
                )
                self.taskBufferIF.reassignShare([task_id], new_share, True)
                # tweak split rule
                _, task_spec = self.taskBufferIF.getTaskWithID_JEDI(task_id)
                # set max walltime to 1 hour
                if task_spec.getNumFilesPerJob() is None and task_spec.getNumEventsPerJob() is None:
                    tmpLog.info(f" >>> set max walltime to 1 hr for jediTaskID={task_id}")
                    task_spec.set_max_walltime(1)
                    self.taskBufferIF.updateTask_JEDI(task_spec, {"jediTaskID": task_id})
                tmpLog.info(f">>> done jediTaskID={task_id}")
            # done
            tmpLog.debug("done")
        except Exception:
            errtype, errvalue = sys.exc_info()[:2]
            tmpLog.error(f"failed with {errtype} {errvalue} {traceback.format_exc()}")
