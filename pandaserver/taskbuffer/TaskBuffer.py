import re
import datetime
import ProcessGroups
from threading import Lock
from DBProxyPool import DBProxyPool
from brokerage.SiteMapper import SiteMapper
from dataservice.Setupper import Setupper
from dataservice.Closer import Closer
from dataservice.TaLauncher import TaLauncher


class TaskBuffer:
    """
    task queue
    
    """

    # constructor 
    def __init__(self):
        self.proxyPool = None
        self.lock = Lock()


    # initialize
    def init(self,dbname,dbpass,nDBConnection=10,useTimeout=False):
        # lock
        self.lock.acquire()
        # create Proxy Pool
        if self.proxyPool == None:
            self.proxyPool = DBProxyPool(dbname,dbpass,nDBConnection,useTimeout)
        # release
        self.lock.release()


    # check production role
    def checkProdRole(self,fqans):
        for fqan in fqans:
            # check production role
            match = re.search('/([^/]+)/Role=production',fqan)
            if match != None:
                return True,match.group(1)
        return False,None

                                                        
    # store Jobs into DB
    def storeJobs(self,jobs,user,joinThr=False,forkSetupper=False,fqans=[],hostname='',resetLocInSetupper=False):
        # check quota for priority calculation
        weight         = 0.0
        userJobID      = -1
        userJobsetID   = -1
        userStatus     = True
        priorityOffset = 0
        userVO         = 'atlas'
        userCountry    = None
        if len(jobs) > 0 and (jobs[0].prodSourceLabel in ['user','panda','ptest','rc_test']) \
               and (not jobs[0].processingType in ['merge','unmerge']):
            # get DB proxy
            proxy = self.proxyPool.getProxy()
            # check quota
            weight = proxy.checkQuota(user)
            # get JobID and status
            userJobID,userJobsetID,userStatus = proxy.getUserParameter(user,jobs[0].jobDefinitionID,jobs[0].jobsetID)
            # get site access
            userSiteAccess = proxy.checkSiteAccess(jobs[0].computingSite,user)
            # release proxy
            self.proxyPool.putProxy(proxy)
            # get site spec
            siteMapper  = SiteMapper(self)
            tmpSiteSpec = siteMapper.getSite(jobs[0].computingSite)
            # check allowed groups
            if userStatus and hasattr(tmpSiteSpec,'allowedgroups') and (not tmpSiteSpec.allowedgroups in ['',None]):
                # set status to False when allowedgroups is defined
                userStatus = False
                # loop over all groups
                for tmpGroup in tmpSiteSpec.allowedgroups.split(','):
                    if tmpGroup == '':
                        continue
                    # loop over all FQANs
                    for tmpFQAN in fqans:
                        if re.search('^%s' % tmpGroup,tmpFQAN) != None:
                            userStatus = True
                            break
                    # escape
                    if userStatus:
                        break
            # get priority offset
            if hasattr(tmpSiteSpec,'priorityoffset') and (not tmpSiteSpec.priorityoffset in ['',None]):
                # loop over all groups
                for tmpGP in tmpSiteSpec.priorityoffset.split(','):
                    if tmpGP == '':
                        continue
                    # get group and offset
                    tmpGroup = tmpGP.split(':')[0]
                    try:
                        tmpOffset = int(tmpGP.split(':')[-1])
                    except:
                        tmpOffset = 0
                    # loop over all FQANs
                    for tmpFQAN in fqans:
                        if re.search('^%s/' % tmpGroup,tmpFQAN) != None:
                            # use the largest offset
                            if tmpOffset > priorityOffset:
                                priorityOffset = tmpOffset
                            break
            # check site access
            if hasattr(tmpSiteSpec,'accesscontrol') and tmpSiteSpec.accesscontrol == 'grouplist':
                if userSiteAccess == {} or userSiteAccess['status'] != 'approved':
                    # user is not allowed
                    userStatus = False
            # set priority offset
            if userStatus:        
                if userSiteAccess.has_key('poffset') and userSiteAccess['poffset'] > priorityOffset: 
                    priorityOffset = userSiteAccess['poffset']
            # extract country group
            for tmpFQAN in fqans:
                match = re.search('^/atlas/([^/]+)/',tmpFQAN)
                if match != None:
                    tmpCountry = match.group(1)
                    # use country code or usatlas
                    if len(tmpCountry) == 2:
                        userCountry = tmpCountry
                        break
                    # usatlas
                    if tmpCountry in ['usatlas']:
                        userCountry = 'us'
                        break
        # check production role
        withProdRole,workingGroup = self.checkProdRole(fqans)
        if withProdRole:
            # check dataset name
            for tmpFile in jobs[-1].Files:
                if tmpFile.type in ['output','log'] and not tmpFile.lfn.startswith('group'):
                    # reset
                    withProdRole,workingGroup = False,None
                    break
        # return if DN is blocked
        if not userStatus:
            return []
        # extract VO
        for tmpFQAN in fqans:
            match = re.search('^/([^/]+)/',tmpFQAN)
            if match != None:
                userVO = match.group(1)
                break
        # get DB proxy
        proxy = self.proxyPool.getProxy()
        # get number of jobs currently in PandaDB
        serNum = 0
        userDefinedWG = False
        validWorkingGroup = False
        usingBuild = False
        if len(jobs) > 0 and (jobs[0].prodSourceLabel in ['user','panda']) \
               and (not jobs[0].processingType in ['merge','unmerge']):
            # set high prioryty for production role
            if withProdRole:
                serNum = 0
                weight = 0.0                
                priorityOffset = 2000
            # check workingGroup
            if not jobs[0].workingGroup in ['',None,'NULL']:
                userDefinedWG = True
                if userSiteAccess != {}:
                    if userSiteAccess['status'] == 'approved' and jobs[0].workingGroup in userSiteAccess['workingGroups']:
                        # valid workingGroup
                        validWorkingGroup = True
            # get nJob
            if userDefinedWG and validWorkingGroup:
                serNum = proxy.getNumberJobsUser(user,workingGroup=jobs[0].workingGroup)
            else:
                serNum = proxy.getNumberJobsUser(user,workingGroup=None)
            # using build for analysis
            if jobs[0].prodSourceLabel == 'panda':
                usingBuild = True
            # reset nJob/weight for HC   
            if jobs[0].processingType in ['hammercloud','gangarobot','gangarobot-squid']:
                serNum = 0
                weight = 0.0
        # get group job serial number
        groupJobSerialNum = 0
        if len(jobs) > 0 and (jobs[0].prodSourceLabel in ['user','panda']) \
               and (not jobs[0].processingType in ['merge','unmerge']):
            for tmpFile in jobs[-1].Files:
                if tmpFile.type in ['output','log'] and '$GROUPJOBSN' in tmpFile.lfn:
                    tmpSnRet = proxy.getSerialNumberForGroupJob(user)
                    if tmpSnRet['status']: 
                        groupJobSerialNum = tmpSnRet['sn']
                    break
        # loop over all jobs
        ret =[]
        newJobs=[]
        usePandaDDM = False
        firstLiveLog = True
        for job in jobs:
            # set JobID. keep original JobID when retry
            if userJobID != -1 and job.prodSourceLabel in ['user','panda'] \
                   and (job.attemptNr in [0,'0','NULL'] or (not job.jobExecutionID in [0,'0','NULL'])) \
                   and (not jobs[0].processingType in ['merge','unmerge']):
                job.jobDefinitionID = userJobID
            # set jobsetID    
            if job.prodSourceLabel in ['user','panda','ptest','rc_test']:
                job.jobsetID = userJobsetID
            # set relocation flag
            if job.computingSite != 'NULL':
                job.relocationFlag = 1
            # protection agains empty jobParameters
            if job.jobParameters in ['',None,'NULL']:
                job.jobParameters = ' '
            # set country group and nJobs (=taskID)
            if job.prodSourceLabel in ['user','panda']:
                job.countryGroup = userCountry
                # set workingGroup
                if not validWorkingGroup:
                    if withProdRole:
                        # set country group if submitted with production role
                        job.workingGroup = workingGroup
                    else:
                        if userDefinedWG:
                            # reset invalid working group
                            job.workingGroup = None
                # set nJobs (=taskID)
                if usingBuild:
                    tmpNumBuild = 1
                    tmpNunRun = len(jobs) - 1 
                else:
                    tmpNumBuild = 0
                    tmpNunRun = len(jobs)
                # encode    
                job.taskID = tmpNumBuild + (tmpNunRun << 1)
                # change TRF URL just in case
                if job.transformation.startswith('http://www.usatlas.bnl.gov/svn/panda/pathena/trf'):
                    job.transformation = re.sub('^http://www.usatlas.bnl.gov/svn/panda/pathena/trf/',
                                                'http://pandaserver.cern.ch:25080/trf/user/',
                                                job.transformation)
            # set hostname
            if hostname != '':
                job.creationHost = hostname
            # insert job to DB
            if not proxy.insertNewJob(job,user,serNum,weight,priorityOffset,userVO,groupJobSerialNum):
                # reset if failed
                job.PandaID = None
            else:
                # live log
                if job.prodSourceLabel in ['user','panda']:
                    if ' --liveLog ' in job.jobParameters:
                        # enable liveLog only for the first one
                        if firstLiveLog:
                            # set file name
                            repPatt = ' --liveLog stdout.%s ' % job.PandaID
                        else:
                            # remove the option
                            repPatt = ' '
                        job.jobParameters = re.sub(' --liveLog ',repPatt,job.jobParameters)
                        firstLiveLog = False
                # append
                newJobs.append(job)
            if job.prodSourceLabel in ['user','panda','ptest','rc_test']:                
                ret.append((job.PandaID,job.jobDefinitionID,{'jobsetID':job.jobsetID}))
            else:
                ret.append((job.PandaID,job.jobDefinitionID,job.jobName))                
            serNum += 1
        # release DB proxy
        self.proxyPool.putProxy(proxy)
        # set up dataset
        if joinThr:
            thr = Setupper(self,newJobs,pandaDDM=usePandaDDM,forkRun=forkSetupper,resetLocation=resetLocInSetupper)
            thr.start()
            thr.join()
        else:
            # cannot use 'thr =' because it may trigger garbage collector
            Setupper(self,newJobs,pandaDDM=usePandaDDM,forkRun=forkSetupper,resetLocation=resetLocInSetupper).start()
        # return jobIDs
        return ret


    # get number of activated/defined jobs with output datasets
    def getNumWaitingJobsWithOutDS(self,outputDSs):
        # get DB proxy
        proxy = self.proxyPool.getProxy()
        # exec
        res = proxy.getNumWaitingJobsWithOutDS(outputDSs)
        # release DB proxy
        self.proxyPool.putProxy(proxy)
        # return
        return res


    # resubmit jobs
    def resubmitJobs(self,jobIDs):
        # get DB proxy
        proxy = self.proxyPool.getProxy()
        jobs=[]
        # get jobs
        for jobID in jobIDs:
            res = proxy.peekJob(jobID,True,False,False,False)
            if res:
                jobs.append(res)
        # release DB proxy
        self.proxyPool.putProxy(proxy)
        # set up dataset
        if len(jobs) > 0:
            Setupper(self,jobs).start()
        # return jobIDs
        return True
    

    # update overall job information
    def updateJobs(self,jobs,inJobsDefined):
        # get DB proxy
        proxy = self.proxyPool.getProxy()        
        # loop over all jobs
        returns    = []
        ddmIDs     = []
        ddmAttempt = 0
        newMover   = None
        for job in jobs:
            # update DB
            tmpddmIDs = []
            if job.jobStatus in ['finished','failed','cancelled']:
                ret,tmpddmIDs,ddmAttempt,newMover = proxy.archiveJob(job,inJobsDefined)
            else:
                ret = proxy.updateJob(job,inJobsDefined)
            returns.append(ret)
            # collect IDs for reassign
            if ret:
                ddmIDs += tmpddmIDs
        # release proxy
        self.proxyPool.putProxy(proxy)
        # retry mover
        if newMover != None:
            self.storeJobs([newMover],None,joinThr=True)
        # reassign jobs when ddm failed
        if ddmIDs != []:
            self.reassignJobs(ddmIDs,ddmAttempt,joinThr=True)
        # return
        return returns


    # update job jobStatus only
    def updateJobStatus(self,jobID,jobStatus,param,updateStateChange=False):
        # get DB proxy
        proxy = self.proxyPool.getProxy()        
        # update DB and buffer
        if re.match('^finished$',jobStatus,re.I) or re.match('^failed$',jobStatus,re.I):
            ret = proxy.archiveJobLite(jobID,jobStatus,param)
        else:
            ret = proxy.updateJobStatus(jobID,jobStatus,param,updateStateChange)
        # release proxy
        self.proxyPool.putProxy(proxy)
        return ret


    # retry job
    def retryJob(self,jobID,param):
        # get DB proxy
        proxy = self.proxyPool.getProxy()
        # update DB
        ret = proxy.retryJob(jobID,param)
        # release proxy
        self.proxyPool.putProxy(proxy)
        return ret
    

    # activate jobs
    def activateJobs(self,jobs):
        # get DB proxy
        proxy = self.proxyPool.getProxy()        
        # loop over all jobs
        returns = []
        for job in jobs:
            # update DB
            ret = proxy.activateJob(job)
            returns.append(ret) 
        # release proxy
        self.proxyPool.putProxy(proxy)
        return returns


    # send jobs to jobsWaiting
    def keepJobs(self,jobs):
        # get DB proxy
        proxy = self.proxyPool.getProxy()        
        # loop over all jobs
        returns = []
        for job in jobs:
            # update DB
            ret = proxy.keepJob(job)
            returns.append(ret) 
        # release proxy
        self.proxyPool.putProxy(proxy)
        return returns


    # delete stalled jobs
    def deleteStalledJobs(self,libFileName):
        # get DB proxy
        proxy = self.proxyPool.getProxy()        
        # execute
        ret = proxy.deleteStalledJobs(libFileName)
        # release proxy
        self.proxyPool.putProxy(proxy)
        return ret


    # get jobs
    def getJobs(self,nJobs,siteName,prodSourceLabel,cpu,mem,diskSpace,node,timeout,computingElement,
                atlasRelease,prodUserID,getProxyKey,countryGroup,workingGroup,allowOtherCountry):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get waiting jobs
        jobs,nSent = proxy.getJobs(nJobs,siteName,prodSourceLabel,cpu,mem,diskSpace,node,timeout,computingElement,
                                   atlasRelease,prodUserID,countryGroup,workingGroup,allowOtherCountry)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # get Proxy Key
        proxyKey = {}
        if getProxyKey and len(jobs) > 0:
            # get MetaDB proxy
            proxy = self.proxyPool.getProxy()
            # get Proxy Key
            proxyKey = proxy.getProxyKey(jobs[0].prodUserID)
            # release proxy
            self.proxyPool.putProxy(proxy)
        # return
        return jobs+[nSent,proxyKey]
        

    # run task assignment
    def runTaskAssignment(self,jobs):
        # get DB proxy
        proxy = self.proxyPool.getProxy()
        # loop over all jobs
        retList =[]
        newJobs =[]
        for job in jobs:
            ret = None
            if not job.taskID in ['NULL',0,'']:
                # get cloud
                cloudTask = proxy.getCloudTask(job.taskID)
                if cloudTask != None and cloudTask.status == 'assigned':
                    ret = cloudTask.cloud
            if ret == None:
                # append for TA
                newJobs.append(job)
            retList.append(ret)    
        # release DB proxy
        self.proxyPool.putProxy(proxy)
        # run setupper
        if newJobs != []:
            TaLauncher(self,newJobs).start()
        # return clouds
        return retList


    # get assigning task
    def getAssigningTask(self):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # run
        res = proxy.getAssigningTask()
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return res
        

    
    # get job status
    def getJobStatus(self,jobIDs,fromDefined=True,fromActive=True,fromArchived=True,fromWaiting=True):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        retStatus = []
        # peek at job
        for jobID in jobIDs:
            res = proxy.peekJob(jobID,fromDefined,fromActive,fromArchived,fromWaiting)
            if res:
                retStatus.append(res.jobStatus)
            else:
                retStatus.append(None)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retStatus


    # peek at jobs
    def peekJobs(self,jobIDs,fromDefined=True,fromActive=True,fromArchived=True,fromWaiting=True,forAnal=False):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        retJobs = []
        # peek at job
        for jobID in jobIDs:
            res = proxy.peekJob(jobID,fromDefined,fromActive,fromArchived,fromWaiting,forAnal)
            if res:
                retJobs.append(res)
            else:
                retJobs.append(None)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retJobs


    # get slimmed file info with PandaIDs
    def getSlimmedFileInfoPandaIDs(self,pandaIDs):
        iPandaID = 0
        nPandaID = 100
        retInfo = {}
        while iPandaID < len(pandaIDs):
            # get DBproxy
            proxy = self.proxyPool.getProxy()
            # get
            tmpRetInfo = proxy.getSlimmedFileInfoPandaIDs(pandaIDs[iPandaID:iPandaID+nPandaID])
            # release proxy
            self.proxyPool.putProxy(proxy)
            iPandaID += nPandaID 
            if retInfo == {}:
                retInfo = tmpRetInfo
            else:
                for outKey in tmpRetInfo.keys():
                    if not retInfo.has_key(outKey):
                        retInfo[outKey] = []
                    # append
                    for tmpItemRetInfo in tmpRetInfo[outKey]:
                        if not tmpItemRetInfo in retInfo[outKey]:
                            retInfo[outKey].append(tmpItemRetInfo)
        # return
        return retInfo


    # get JobIDs in a time range
    def getJobIDsInTimeRange(self,dn,timeRangeStr):
        # check DN
        if dn in ['NULL','','None',None]:
            return []
        # check timeRange
        match = re.match('^(\d+)-(\d+)-(\d+) (\d+):(\d+):(\d+)$',timeRangeStr)
        if match == None:
            return []
        timeRange = datetime.datetime(year   = int(match.group(1)),
                                      month  = int(match.group(2)),
                                      day    = int(match.group(3)),
                                      hour   = int(match.group(4)),
                                      minute = int(match.group(5)),
                                      second = int(match.group(6)))
        # max range is 3 months
        maxRange = datetime.datetime.utcnow() - datetime.timedelta(days=30)
        if timeRange < maxRange:
            timeRange = maxRange
        retJobIDs = []
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get JobIDs
        retJobIDs = proxy.getJobIDsInTimeRange(dn,timeRange,retJobIDs)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # get ArchiveDBproxy
        proxy = self.proxyPool.getProxy()
        # get JobIDs
        retJobIDs = proxy.getJobIDsInTimeRangeLog(dn,timeRange,retJobIDs)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retJobIDs


    # get PandaIDs for a JobID
    def getPandIDsWithJobID(self,dn,jobID,nJobs):
        idStatus = {}
        # check DN
        if dn in ['NULL','','None',None]:
            return idStatus
        # check JobID
        try:
            jobID = long(jobID)
            nJobs = long(nJobs)
        except:
            return idStatus
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get IDs
        idStatus,buildJobID = proxy.getPandIDsWithJobID(dn,jobID,idStatus,nJobs)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # get ArchiveDBproxy
        proxy = self.proxyPool.getProxy()
        # get IDs
        idStatus = proxy.getPandIDsWithJobIDLog(dn,jobID,idStatus,nJobs,buildJobID)        
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return idStatus


    # lock job for re-brokerage
    def lockJobForReBrokerage(self,dn,jobID,simulation,forceOpt):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get IDs
        ret = proxy.lockJobForReBrokerage(dn,jobID,simulation,forceOpt)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # reset buildJob for re-brokerage
    def resetBuildJobForReBrokerage(self,pandaID):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get IDs
        ret = proxy.resetBuildJobForReBrokerage(pandaID)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # get PandaIDs using libDS for re-brokerage
    def getPandaIDsForReBrokerage(self,userName,jobID,fromActive):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get IDs
        ret = proxy.getPandaIDsForReBrokerage(userName,jobID,fromActive)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # get input datasets for rebroerage
    def getInDatasetsForReBrokerage(self,jobID,userName):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get IDs
        ret = proxy.getInDatasetsForReBrokerage(jobID,userName)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret
    

    # get outDSs with userName/jobID
    def getOutDSsForReBrokerage(self,userName,jobID):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get IDs
        ret = proxy.getOutDSsForReBrokerage(userName,jobID)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # get full job status
    def getFullJobStatus(self,jobIDs):
        retJobMap = {}
        # peek at job
        for jobID in jobIDs:
            # get DBproxy for each job to avoid occupying connection for long time
            proxy = self.proxyPool.getProxy()
            # peek job
            res = proxy.peekJob(jobID,True,True,True,True,True)
            retJobMap[jobID] = res
            # release proxy
            self.proxyPool.putProxy(proxy)
        # get IDs
        for jobID in jobIDs:
            if retJobMap[jobID] == None:
                # get ArchiveDBproxy
                proxy = self.proxyPool.getProxy()
                # peek job
                res = proxy.peekJobLog(jobID)
                retJobMap[jobID] = res
                # release proxy
                self.proxyPool.putProxy(proxy)
        # sort
        retJobs = []
        for jobID in jobIDs:
            retJobs.append(retJobMap[jobID])
        # return
        return retJobs

    
    # kill jobs
    def killJobs(self,ids,user,code,prodManager):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        rets = []
        # kill jobs
        pandaIDforCloser = None 
        for id in ids:
            ret,userInfo = proxy.killJob(id,user,code,prodManager,True)
            rets.append(ret)
            if ret and userInfo['prodSourceLabel'] == 'user':
                pandaIDforCloser = id
        # release proxy
        self.proxyPool.putProxy(proxy)
        # run Closer
        try:
            if pandaIDforCloser != None:
                tmpJobs = self.peekJobs([pandaIDforCloser])
                tmpJob = tmpJobs[0]
                if tmpJob != None:
                    tmpDestDBlocks = []
                    # get destDBlock
                    for tmpFile in tmpJob.Files:
                        if tmpFile.type in ['output','log']:
                            if not tmpFile.destinationDBlock in tmpDestDBlocks:
                                tmpDestDBlocks.append(tmpFile.destinationDBlock)
                    # run            
                    Closer(self,tmpDestDBlocks,tmpJob).start()
        except:
            pass
        # return
        return rets


    # reassign jobs
    def reassignJobs(self,ids,attempt=0,joinThr=False,forkSetupper=False):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        jobs = []
        # keep old assignment
        keepSiteFlag = False
        if (attempt % 2) != 0:
            keepSiteFlag = True
        # reset jobs
        for id in ids:
            # try to reset active job
            ret = proxy.resetJob(id,keepSite=keepSiteFlag)
            if ret != None:
                jobs.append(ret)
                continue
            # try to reset waiting job
            ret = proxy.resetJob(id,False,keepSite=keepSiteFlag)
            if ret != None:
                jobs.append(ret)
                continue
            # try to reset defined job
            ret = proxy.resetDefinedJob(id,keepSite=keepSiteFlag)
            if ret != None:
                jobs.append(ret)
                continue
        # release DB proxy
        self.proxyPool.putProxy(proxy)
        # setup dataset
        if jobs != []:
            if joinThr:
                thr = Setupper(self,jobs,resubmit=True,ddmAttempt=attempt,forkRun=forkSetupper)
                thr.start()
                thr.join()
            else:
                # cannot use 'thr =' because it may trigger garbage collector
                Setupper(self,jobs,resubmit=True,ddmAttempt=attempt,forkRun=forkSetupper).start()
        # return
        return True


    # awake jobs in jobsWaiting
    def awakeJobs(self,ids):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        jobs = []
        # reset jobs
        for id in ids:
            # try to reset waiting job
            ret = proxy.resetJob(id,False)
            if ret != None:
                jobs.append(ret)
        # release DB proxy
        self.proxyPool.putProxy(proxy)
        # setup dataset
        Setupper(self,jobs).start()
        # return
        return True


    # query PandaIDs
    def queryPandaIDs(self,jobDefIDs):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        pandaIDs = []
        # query PandaID
        for jobDefID in jobDefIDs:
            id = proxy.queryPandaID(jobDefID)
            pandaIDs.append(id)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return pandaIDs


    # query job info per cloud
    def queryJobInfoPerCloud(self,cloud,schedulerID=None):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # query job info
        ret = proxy.queryJobInfoPerCloud(cloud,schedulerID)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret

    
    # get PandaIDs to be updated in prodDB
    def getPandaIDsForProdDB(self,limit,lockedby):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # query PandaID
        ret = proxy.getPandaIDsForProdDB(limit,lockedby)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # update prodDBUpdateTime 
    def updateProdDBUpdateTimes(self,paramList):
        retList = []
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # update
        for param in paramList:
            ret = proxy.updateProdDBUpdateTime(param)
            retList.append(ret)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retList


    # get PandaIDs at Site
    def getPandaIDsSite(self,site,status,limit):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # query PandaID
        ids = proxy.getPandaIDsSite(site,status,limit)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ids


    # get input files currently in used for analysis
    def getFilesInUseForAnal(self,outDataset):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        retList = []
        # query LFNs
        retList = proxy.getFilesInUseForAnal(outDataset)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retList


    # update input files and return corresponding PandaIDs
    def updateInFilesReturnPandaIDs(self,dataset,status):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        retList = []
        # query PandaID
        retList = proxy.updateInFilesReturnPandaIDs(dataset,status)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retList


    # update file status in dispatch dataset
    def updateFileStatusInDisp(self,dataset,fileStatusMap):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # query PandaID
        retVal = proxy.updateFileStatusInDisp(dataset,fileStatusMap)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal


    # update output files and return corresponding PandaIDs
    def updateOutFilesReturnPandaIDs(self,dataset):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        retList = []
        # query PandaID
        retList = proxy.updateOutFilesReturnPandaIDs(dataset)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retList


    # insert datasets
    def insertDatasets(self,datasets):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        retList = []
        # insert
        for dataset in datasets:
            ret= proxy.insertDataset(dataset)
            retList.append(ret)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retList


    # query Dataset
    def queryDatasetWithMap(self,map):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # query Dataset
        ret = proxy.queryDatasetWithMap(map)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # query last files in a dataset
    def queryLastFilesInDataset(self,datasets):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # query files
        ret = proxy.queryLastFilesInDataset(datasets)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret

    
    # set GUIDs
    def setGUIDs(self,files):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # set GUIDs
        ret = proxy.setGUIDs(files)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret

    
    # query PandaID with dataset
    def queryPandaIDwithDataset(self,datasets):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # query Dataset
        ret = proxy.queryPandaIDwithDataset(datasets)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret

    
    # query PandaID with filenames
    def queryPandaIDwithLFN(self,lfns):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # query Dataset
        ret = proxy.queryPandaIDwithLFN(lfns)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # update dataset
    def updateDatasets(self,datasets,withLock=False,withCriteria="",criteriaMap={}):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # update Dataset
        retList = proxy.updateDataset(datasets,withLock,withCriteria,criteriaMap)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retList
    

    # delete dataset
    def deleteDatasets(self,datasets):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        retList = []
        # query Dataset
        for dataset in datasets:
            ret = proxy.deleteDataset(dataset)
            retList.append(ret)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retList


    # query files with map
    def queryFilesWithMap(self,map):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # query files
        ret = proxy.queryFilesWithMap(map)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # count the number of files with map
    def countFilesWithMap(self,map):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # query files
        ret = proxy.countFilesWithMap(map)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # get serial number for dataset
    def getSerialNumber(self,datasetname):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get serial number
        ret = proxy.getSerialNumber(datasetname)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # get serial number for group job
    def getSerialNumberForGroupJob(self,name):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get serial number
        ret = proxy.getSerialNumberForGroupJob(name)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # add metadata
    def addMetadata(self,ids,metadataList):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # add metadata
        index = 0
        retList = []
        for id in ids:
            ret = proxy.addMetadata(id,metadataList[index])
            retList.append(ret)
            index += 1
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retList


    # get destinationDBlockToken for a dataset
    def getDestTokens(self,dsname):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get token
        ret = proxy.getDestTokens(dsname)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret

    
    # get destinationSE for a dataset
    def getDestSE(self,dsname):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get token
        ret = proxy.getDestSE(dsname)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret

    
    # get job statistics
    def getJobStatistics(self,archived=False,predefined=False,workingGroup='',countryGroup='',jobType=''):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get serial number
        ret = proxy.getJobStatistics(archived,predefined,workingGroup,countryGroup,jobType)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # get job statistics for brokerage
    def getJobStatisticsBrokerage(self):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get stat
        ret = proxy.getJobStatisticsBrokerage()
        # release proxy
        self.proxyPool.putProxy(proxy)
        # convert
        conRet = ProcessGroups.countJobsPerGroup(ret)
        # return
        return conRet


    # get highest prio jobs
    def getHighestPrioJobStat(self):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get stat
        ret = proxy.getHighestPrioJobStat()
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # get queued analysis jobs at a site
    def getQueuedAnalJobs(self,site,dn):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get stat
        ret = proxy.getQueuedAnalJobs(site,dn)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # get job statistics for ExtIF
    def getJobStatisticsForExtIF(self,sourcetype=None):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get serial number
        ret = proxy.getJobStatisticsForExtIF(sourcetype)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # get job statistics for Bamboo
    def getJobStatisticsForBamboo(self):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get serial number
        ret = proxy.getJobStatisticsPerProcessingType()
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # get number of analysis jobs per user
    def getNUserJobs(self,siteName,nJobs):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get number of analysis jobs per user
        tmpRet = proxy.getNUserJobs(siteName,nJobs)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # get log proxy
        proxy = self.proxyPool.getProxy()
        # get Proxy Key
        ret = {}
        for userID,nJobs in tmpRet.iteritems():
            proxyKey = proxy.getProxyKey(userID)
            if proxyKey != {}:
                # add nJobs
                proxyKey['nJobs'] = nJobs
                # append
                ret[userID] = proxyKey
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret

    
    # get number of activated analysis jobs
    def getNAnalysisJobs(self,nProcesses):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # count
        ret = proxy.getNAnalysisJobs(nProcesses)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # update transfer status for a dataset
    def updateTransferStatus(self,datasetname,bitMap):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # update
        ret = proxy.updateTransferStatus(datasetname,bitMap)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret

    
    # get CloudTask
    def getCloudTask(self,tid):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # count
        ret = proxy.getCloudTask(tid)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # set cloud to CloudTask
    def setCloudTask(self,cloudTask):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # count
        ret = proxy.setCloudTask(cloudTask)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # see CloudTask
    def seeCloudTask(self,tid):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # count
        ret = proxy.seeCloudTask(tid)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # set cloud to CloudTask by user
    def setCloudTaskByUser(self,user,tid,cloud,status):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # count
        ret = proxy.setCloudTaskByUser(user,tid,cloud,status)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # update site data
    def updateSiteData(self,hostID,pilotRequests):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get serial number
        ret = proxy.updateSiteData(hostID,pilotRequests)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # get current site data
    def getCurrentSiteData(self):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get serial number
        ret = proxy.getCurrentSiteData()
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # get site list
    def getSiteList(self):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get site info
        ret = proxy.getSiteList()
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # get site info
    def getSiteInfo(self):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get site info
        ret = proxy.getSiteInfo()
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # get cloud list
    def getCloudList(self):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get cloud list
        ret = proxy.getCloudList()
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # check sites with release/cache
    def checkSitesWithRelease(self,sites,releases=None,caches=None):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # check
        ret = proxy.checkSitesWithRelease(sites,releases,caches)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # get sites with release/cache in cloud 
    def getSitesWithReleaseInCloud(self,cloud,releases=None,caches=None,validation=False):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # check
        ret = proxy.getSitesWithReleaseInCloud(cloud,releases,caches,validation)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # get list of cache prefix
    def getCachePrefixes(self):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # check
        ret = proxy.getCachePrefixes()
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # get pilot owners
    def getPilotOwners(self):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get pilot owners
        ret = proxy.getPilotOwners()
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # get allowed nodes
    def getAllowedNodes(self):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get
        ret = proxy.getAllowedNodes()
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # get email address
    def getEmailAddr(self,name):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get 
        ret = proxy.getEmailAddr(name)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # get client version
    def getPandaClientVer(self):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get 
        ret = proxy.getPandaClientVer()
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # register proxy key
    def registerProxyKey(self,params):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # register proxy key
        ret = proxy.registerProxyKey(params)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # register proxy key
    def registerProxyKey(self,params):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # register proxy key
        ret = proxy.registerProxyKey(params)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # get proxy key
    def getProxyKey(self,dn):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get proxy key
        ret = proxy.getProxyKey(dn)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # add account to siteaccess  
    def addSiteAccess(self,siteID,dn):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # add account to siteaccess
        ret = proxy.addSiteAccess(siteID,dn)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # list site access
    def listSiteAccess(self,siteid,dn,longFormat=False):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # list site access
        ret = proxy.listSiteAccess(siteid,dn,longFormat)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # update site access
    def updateSiteAccess(self,method,siteid,requesterDN,userName,attrValue):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # update site access
        ret = proxy.updateSiteAccess(method,siteid,requesterDN,userName,attrValue)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret

    
    # generate pilot token
    def genPilotToken(self,schedulerhost,scheduleruser,schedulerid):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get
        ret = proxy.genPilotToken(schedulerhost,scheduleruser,schedulerid)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # add files to memcached
    def addFilesToMemcached(self,site,node,files):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get
        ret = proxy.addFilesToMemcached(site,node,files)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret

    
    # delete files from memcached
    def deleteFilesFromMemcached(self,site,node,files):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get
        ret = proxy.deleteFilesFromMemcached(site,node,files)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # flush memcached
    def flushMemcached(self,site,node):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get
        ret = proxy.flushMemcached(site,node)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret

    # check files with memcached
    def checkFilesWithMemcached(self,site,node,files):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get
        ret = proxy.checkFilesWithMemcached(site,node,files)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # get list of scheduler users
    def getListSchedUsers(self):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get
        ret = proxy.getListSchedUsers()
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret
    

    # query an SQL return Status  
    def querySQLS(self,sql,varMap,arraySize=1000):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get
        ret = proxy.querySQLS(sql,varMap,arraySize)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # check quota
    def checkQuota(self,dn):
        # query an SQL return Status  
        proxy = self.proxyPool.getProxy()
        # get
        ret = proxy.checkQuota(dn)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # get user subscriptions
    def getUserSubscriptions(self,datasetName,timeRange):
        # query an SQL return Status  
        proxy = self.proxyPool.getProxy()
        # get
        ret = proxy.getUserSubscriptions(datasetName,timeRange)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # add user subscriptions
    def addUserSubscription(self,datasetName,dq2IDs):
        # query an SQL return Status  
        proxy = self.proxyPool.getProxy()
        # get
        ret = proxy.addUserSubscription(datasetName,dq2IDs)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # increment counter for subscription
    def incrementUsedCounterSubscription(self,datasetName):
        # query an SQL return Status  
        proxy = self.proxyPool.getProxy()
        # get
        ret = proxy.incrementUsedCounterSubscription(datasetName)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # get active datasets
    def getActiveDatasets(self,computingSite,prodSourceLabel):
        # query an SQL return Status  
        proxy = self.proxyPool.getProxy()
        # get
        ret = proxy.getActiveDatasets(computingSite,prodSourceLabel)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # check status of all sub datasets to trigger Notifier
    def checkDatasetStatusForNotifier(self,jobsetID,jobDefinitionID,prodUserName):
        # query an SQL return Status  
        proxy = self.proxyPool.getProxy()
        # get
        ret = proxy.checkDatasetStatusForNotifier(jobsetID,jobDefinitionID,prodUserName)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


# Singleton
taskBuffer = TaskBuffer()
del TaskBuffer

