import re
import datetime
import ProcessGroups
from threading import Lock
from DBProxyPool import DBProxyPool
from brokerage.SiteMapper import SiteMapper
from dataservice.Setupper import Setupper
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
    def init(self,dbname,dbpass,nDBConnection=10):
        # lock
        self.lock.acquire()
        # create Proxy Pool
        if self.proxyPool == None:
            self.proxyPool = DBProxyPool(dbname,dbpass,nDBConnection)
        # release
        self.lock.release()


    # store Jobs into DB
    def storeJobs(self,jobs,user,joinThr=False,forkSetupper=False,fqans=[]):
        # check quota for priority calculation
        weight         = 0.0
        userJobID      = -1
        userStatus     = True
        priorityOffset = 0
        userVO         = 'atlas'
        if len(jobs) > 0 and (jobs[0].prodSourceLabel in ['user','panda']):
            # get DB proxy
            proxy = self.proxyPool.getProxy()
            # check quota
            weight = proxy.checkQuota(user)
            # get JobID and status
            userJobID,userStatus = proxy.getUserParameter(user,jobs[0].jobDefinitionID)
            # get site access
            userSiteAccess = proxy.checkSiteAccess(jobs[0].computingSite,user)
            # release proxy
            self.proxyPool.putProxy(proxy)
            # get site spec
            siteMapper  = SiteMapper(self)
            tmpSiteSpec = siteMapper.getSite(jobs[0].computingSite)
            # check allowed groups
            if hasattr(tmpSiteSpec,'allowedgroups') and (not tmpSiteSpec.allowedgroups in ['',None]):
                # set status to False when allowedgroups is defined
                userStatus = False
                # loop over all groups
                for tmpGroup in tmpSiteSpec.allowedgroups.split(','):
                    if tmpGroup == '':
                        continue
                    # loop over all FQANs
                    for tmpFQAN in fqans:
                        if re.search('^%s/' % tmpGroup,tmpFQAN) != None:
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
                else:
                    # set priority offset
                    if userSiteAccess['poffset'] > priorityOffset: 
                        priorityOffset = userSiteAccess['poffset'] 
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
        if len(jobs) > 0 and (jobs[0].prodSourceLabel in ['user','panda']):
            # get nJob
            serNum = proxy.getNumberJobsUser(user)
        # loop over all jobs
        ret =[]
        newJobs=[]
        usePandaDDM = False
        firstLiveLog = True
        for job in jobs:
            # set JobID. keep original JobID when retry
            if userJobID != -1 and job.prodSourceLabel in ['user','panda'] \
                   and (job.attemptNr in [0,'0','NULL'] or (not job.jobExecutionID in [0,'0','NULL'])):
                job.jobDefinitionID = userJobID
            # set relocation flag
            if job.computingSite != 'NULL':
                job.relocationFlag = 1
            # insert job to DB
            if not proxy.insertNewJob(job,user,serNum,weight,priorityOffset,userVO):
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
            ret.append((job.PandaID,job.jobDefinitionID,job.jobName))
            serNum += 1
        # release DB proxy
        self.proxyPool.putProxy(proxy)
        # set up dataset
        if joinThr:
            thr = Setupper(self,newJobs,pandaDDM=usePandaDDM,forkRun=forkSetupper)
            thr.start()
            thr.join()
        else:
            # cannot use 'thr =' because it may trigger garbage collector
            Setupper(self,newJobs,pandaDDM=usePandaDDM,forkRun=forkSetupper).start()
        # return jobIDs
        return ret


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
            if re.match('^finished$',job.jobStatus,re.I) or re.match('^failed$',job.jobStatus,re.I):
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
    def updateJobStatus(self,jobID,jobStatus,param):
        # get DB proxy
        proxy = self.proxyPool.getProxy()        
        # update DB and buffer
        if re.match('^finished$',jobStatus,re.I) or re.match('^failed$',jobStatus,re.I):
            ret = proxy.archiveJobLite(jobID,jobStatus,param)
        else:
            ret = proxy.updateJobStatus(jobID,jobStatus,param)
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


    # get jobs
    def getJobs(self,nJobs,siteName,prodSourceLabel,cpu,mem,diskSpace,node,timeout,computingElement,
                atlasRelease,prodUserID,getProxyKey):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get waiting jobs
        jobs,nSent = proxy.getJobs(nJobs,siteName,prodSourceLabel,cpu,mem,diskSpace,node,timeout,computingElement,
                                   atlasRelease,prodUserID)
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
    def peekJobs(self,jobIDs,fromDefined=True,fromActive=True,fromArchived=True,fromWaiting=True):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        retJobs = []
        # peek at job
        for jobID in jobIDs:
            res = proxy.peekJob(jobID,fromDefined,fromActive,fromArchived,fromWaiting)
            if res:
                retJobs.append(res)
            else:
                retJobs.append(None)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retJobs


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
        idStatus = proxy.getPandIDsWithJobID(dn,jobID,idStatus,nJobs)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # get ArchiveDBproxy
        proxy = self.proxyPool.getProxy()
        # get IDs
        idStatus = proxy.getPandIDsWithJobIDLog(dn,jobID,idStatus,nJobs)        
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return idStatus
    

    # get full job status
    def getFullJobStatus(self,jobIDs):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        retJobMap = {}
        # peek at job
        for jobID in jobIDs:
            res = proxy.peekJob(jobID,True,True,True,True,True)
            retJobMap[jobID] = res
        # release proxy
        self.proxyPool.putProxy(proxy)
        # get ArchiveDBproxy
        proxy = self.proxyPool.getProxy()
        # get IDs
        for jobID in jobIDs:
            if retJobMap[jobID] == None:
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
        for id in ids:
            ret = proxy.killJob(id,user,code,prodManager)
            rets.append(ret)
        # release proxy
        self.proxyPool.putProxy(proxy)
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
    def getJobStatistics(self,archived=False,predefined=False):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get serial number
        ret = proxy.getJobStatistics(archived,predefined)
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
    def listSiteAccess(self,siteid,dn):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # list site access
        ret = proxy.listSiteAccess(siteid,dn)
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
    

# Singleton
taskBuffer = TaskBuffer()
del TaskBuffer

