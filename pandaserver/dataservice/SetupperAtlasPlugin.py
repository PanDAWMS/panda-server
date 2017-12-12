'''
setup dataset for ATLAS

'''

import re
import sys
import time
import uuid
import types
import urllib
import hashlib
import datetime
import commands
import threading
import traceback
import ErrorCode
from DDM import rucioAPI
from taskbuffer.JobSpec import JobSpec
from taskbuffer.FileSpec import FileSpec
from taskbuffer.DatasetSpec import DatasetSpec
from taskbuffer import retryModule
from taskbuffer import EventServiceUtils
from brokerage.SiteMapper import SiteMapper
from brokerage.PandaSiteIDs import PandaMoverIDs
import brokerage.broker
import DataServiceUtils
from rucio.client import Client as RucioClient
from rucio.common.exception import FileAlreadyExists,DataIdentifierAlreadyExists,Duplicate,\
    DataIdentifierNotFound

from SetupperPluginBase import SetupperPluginBase

from config import panda_config


class SetupperAtlasPlugin (SetupperPluginBase):
    # constructor
    def __init__(self,taskBuffer,jobs,logger,**params):
        # defaults
        defaultMap = {'resubmit'      : False,
                      'pandaDDM'      : False,
                      'ddmAttempt'    : 0,
                      'onlyTA'        : False,
                      'resetLocation' : False,
                      'useNativeDQ2'  : True,
                     }
        SetupperPluginBase.__init__(self,taskBuffer,jobs,logger,params,defaultMap)
        # VUIDs of dispatchDBlocks
        self.vuidMap = {}
        # file list for dispDS for PandaDDM
        self.dispFileList = {}
        # site mapper
        self.siteMapper = None
        # location map
        self.replicaMap  = {}
        # all replica locations
        self.allReplicaMap = {}
        # replica map for special brokerage
        self.replicaMapForBroker = {}
        # available files at T2
        self.availableLFNsInT2 = {}
        # list of missing datasets
        self.missingDatasetList = {}
        # lfn ds map
        self.lfnDatasetMap = {}
        # missing files at T1
        self.missingFilesInT1 = {}
        
        
    # main
    def run(self):
        try:
            self.logger.debug('start run()')
            self._memoryCheck()
            bunchTag = ''
            timeStart = datetime.datetime.utcnow()
            if self.jobs != None and len(self.jobs) > 0:
                bunchTag = 'PandaID:%s type:%s taskID:%s pType=%s' % (self.jobs[0].PandaID,
                                                                      self.jobs[0].prodSourceLabel,
                                                                      self.jobs[0].taskID,
                                                                      self.jobs[0].processingType)
                self.logger.debug(bunchTag)
            # instantiate site mapper
            self.siteMapper = SiteMapper(self.taskBuffer)
            # correctLFN
            self._correctLFN()
            # run full Setupper
            if not self.onlyTA:
                # invoke brokerage
                self.logger.debug('brokerSchedule')
                self._memoryCheck()
                brokerage.broker.schedule(self.jobs,self.taskBuffer,self.siteMapper,
                                          replicaMap=self.replicaMapForBroker,
                                          t2FilesMap=self.availableLFNsInT2)
                # remove waiting jobs
                self.removeWaitingJobs()
                # setup dispatch dataset
                self.logger.debug('setupSource')
                self._memoryCheck()
                self._setupSource()
                self._memoryCheck()
                # sort by site so that larger subs are created in the next step 
                if self.jobs != [] and self.jobs[0].prodSourceLabel in ['managed','test']:
                    tmpJobMap = {}
                    for tmpJob in self.jobs:
                        # add site
                        if not tmpJobMap.has_key(tmpJob.computingSite):
                            tmpJobMap[tmpJob.computingSite] = []
                        # add job    
                        tmpJobMap[tmpJob.computingSite].append(tmpJob)
                    # make new list    
                    tmpJobList = []
                    for tmpSiteKey in tmpJobMap.keys():
                        tmpJobList += tmpJobMap[tmpSiteKey]
                    # set new list
                    self.jobs = tmpJobList
                # create dataset for outputs and assign destination
                if self.jobs != [] and self.jobs[0].prodSourceLabel in ['managed','test'] and self.jobs[0].getCloud() in ['DE']:
                    # count the number of jobs per _dis 
                    iBunch = 0
                    prevDisDsName = None
                    nJobsPerDisList = []
                    for tmpJob in self.jobs:
                        if prevDisDsName != None and prevDisDsName != tmpJob.dispatchDBlock:
                            nJobsPerDisList.append(iBunch)
                            iBunch = 0
                        # increment
                        iBunch += 1
                        # set _dis name
                        prevDisDsName = tmpJob.dispatchDBlock
                    # remaining
                    if iBunch != 0:
                        nJobsPerDisList.append(iBunch)
                    # split sub datasets
                    iBunch = 0
                    nBunchMax = 50
                    tmpIndexJob = 0
                    for nJobsPerDis in nJobsPerDisList:
                        # check _dis boundary so that the same _dis doesn't contribute to many _subs
                        if iBunch+nJobsPerDis > nBunchMax:
                            if iBunch != 0:
                                self._setupDestination(startIdx=tmpIndexJob,nJobsInLoop=iBunch)
                                tmpIndexJob += iBunch
                                iBunch = 0
                        # increment        
                        iBunch += nJobsPerDis    
                    # remaining
                    if iBunch != 0:
                        self._setupDestination(startIdx=tmpIndexJob,nJobsInLoop=iBunch)                            
                else:
                    # at a burst
                    self._setupDestination()
                # make dis datasets for existing files
                self._memoryCheck()
                self._makeDisDatasetsForExistingfiles()
                self._memoryCheck()
                # setup jumbo jobs
                self._setupJumbojobs()
                self._memoryCheck()
            regTime = datetime.datetime.utcnow() - timeStart
            self.logger.debug('{0} took {1}sec'.format(bunchTag,regTime.seconds))
            self.logger.debug('end run()')
        except:
            errtype,errvalue = sys.exc_info()[:2]
            errStr = "run() : %s %s" % (errtype,errvalue)
            errStr.strip()
            errStr += traceback.format_exc()
            self.logger.error(errStr)



    # post run
    def postRun(self):
        try:
            if not self.onlyTA:
                self.logger.debug('start postRun()')
                self._memoryCheck()
                # subscribe sites distpatchDBlocks. this must be the last method
                self.logger.debug('subscribeDistpatchDB')
                self._subscribeDistpatchDB()
                # dynamic data placement for analysis jobs
                self._memoryCheck()
                self._dynamicDataPlacement()
                # pin input datasets
                self._memoryCheck()
                self._pinInputDatasets()
                # make subscription for missing
                self._memoryCheck()
                self._makeSubscriptionForMissing()
                self._memoryCheck()
                self.logger.debug('end postRun()')
        except:
            errtype,errvalue = sys.exc_info()[:2]
            self.logger.error("postRun() : %s %s" % (errtype,errvalue))
        
        

    # make dispatchDBlocks, insert prod/dispatchDBlock to database
    def _setupSource(self):
        fileList    = {}
        prodList    = []
        prodError   = {}
        dispSiteMap = {}
        dispError   = {}
        backEndMap  = {}
        dsTaskMap = dict()
        # extract prodDBlock
        for job in self.jobs:
            # ignore failed jobs
            if job.jobStatus in ['failed','cancelled'] or job.isCancelled():
                continue
            # production datablock
            if job.prodDBlock != 'NULL' and (not self.pandaDDM) and (not job.prodSourceLabel in ['user','panda']):
                # get VUID and record prodDBlock into DB
                if not prodError.has_key(job.prodDBlock):
                    self.logger.debug('listDatasets '+job.prodDBlock)
                    prodError[job.prodDBlock] = ''
                    for iDDMTry in range(3):
                        newOut,errMsg = rucioAPI.listDatasets(job.prodDBlock)
                        if newOut is None:
                            time.sleep(10)
                        else:
                            break
                    if newOut is None:
                        prodError[job.prodDBlock] = "Setupper._setupSource() could not get VUID of prodDBlock with {0}".format(errMsg)
                        self.logger.error(errMsg)                                            
                    else:
                        self.logger.debug(newOut)
                        try:
                            vuids = newOut[job.prodDBlock]['vuids']
                            nfiles = 0
                            # dataset spec
                            ds = DatasetSpec()
                            ds.vuid = vuids[0]
                            ds.name = job.prodDBlock
                            ds.type = 'input'
                            ds.status = 'completed'
                            ds.numberfiles  = nfiles
                            ds.currentfiles = nfiles
                            prodList.append(ds)
                        except:
                            errtype,errvalue = sys.exc_info()[:2]
                            self.logger.error("_setupSource() : %s %s" % (errtype,errvalue))
                            prodError[job.prodDBlock] = "Setupper._setupSource() could not decode VUID of prodDBlock"
                # error
                if prodError[job.prodDBlock] != '':
                    job.jobStatus = 'failed'
                    job.ddmErrorCode = ErrorCode.EC_Setupper
                    job.ddmErrorDiag = prodError[job.prodDBlock]
                    continue
            # dispatch datablock
            if job.dispatchDBlock != 'NULL':
                # src/dst sites
                tmpSrcID = 'BNL_ATLAS_1'
                if self.siteMapper.checkCloud(job.getCloud()):
                    # use cloud's source
                    tmpSrcID = self.siteMapper.getCloud(job.getCloud())['source']
                srcDQ2ID = self.siteMapper.getSite(tmpSrcID).ddm_output # TODO: check with tadashi
                # use srcDQ2ID as dstDQ2ID when it is associated to dest
                dstSiteSpec = self.siteMapper.getSite(job.computingSite)
                if dstSiteSpec.ddm_endpoints_input.isAssociated(srcDQ2ID):
                    dstDQ2ID = srcDQ2ID
                else:
                    dstDQ2ID = dstSiteSpec.ddm_input
                dispSiteMap[job.dispatchDBlock] = {'src':srcDQ2ID,'dst':dstDQ2ID,'site':job.computingSite}
                # filelist
                if not fileList.has_key(job.dispatchDBlock):
                    fileList[job.dispatchDBlock] = {'lfns':[],'guids':[],'fsizes':[],'md5sums':[],
                                                    'chksums':[]}
                    dsTaskMap[job.dispatchDBlock] = job.jediTaskID
                # DDM backend
                if not job.dispatchDBlock in backEndMap:
                    backEndMap[job.dispatchDBlock] = 'rucio'
                # collect LFN and GUID
                for file in job.Files:
                    if file.type == 'input' and file.status == 'pending':
                        if backEndMap[job.dispatchDBlock] != 'rucio':
                            tmpLFN = file.lfn
                        else:
                            tmpLFN = '{0}:{1}'.format(file.scope,file.lfn)
                        if not tmpLFN in fileList[job.dispatchDBlock]['lfns']:
                            fileList[job.dispatchDBlock]['lfns'].append(tmpLFN)
                            fileList[job.dispatchDBlock]['guids'].append(file.GUID)
                            if file.fsize in ['NULL',0]:
                                fileList[job.dispatchDBlock]['fsizes'].append(None)
                            else:
                                fileList[job.dispatchDBlock]['fsizes'].append(long(file.fsize))
                            if file.md5sum in ['NULL','']:
                                fileList[job.dispatchDBlock]['md5sums'].append(None)
                            elif file.md5sum.startswith("md5:"):
                                fileList[job.dispatchDBlock]['md5sums'].append(file.md5sum)                      
                            else:
                                fileList[job.dispatchDBlock]['md5sums'].append("md5:%s" % file.md5sum)                      
                            if file.checksum in ['NULL','']:
                                fileList[job.dispatchDBlock]['chksums'].append(None)
                            else:
                                fileList[job.dispatchDBlock]['chksums'].append(file.checksum)
                        # get replica locations
                        if not self.replicaMap.has_key(job.dispatchDBlock):
                            self.replicaMap[job.dispatchDBlock] = {}
                        if not self.allReplicaMap.has_key(file.dataset):
                            if file.dataset.endswith('/'):
                                status,out = self.getListDatasetReplicasInContainer(file.dataset)
                            else:
                                status,out = self.getListDatasetReplicas(file.dataset,False)
                            if status != 0 or out.startswith('Error'):
                                self.logger.error(out)
                                dispError[job.dispatchDBlock] = 'could not get locations for %s' % file.dataset
                                self.logger.error(dispError[job.dispatchDBlock])
                            else:
                                self.logger.debug(out)
                                tmpRepSites = {}
                                try:
                                    # convert res to map
                                    exec "tmpRepSites = %s" % out
                                    self.allReplicaMap[file.dataset] = tmpRepSites
                                except:
                                    dispError[job.dispatchDBlock] = 'could not convert HTTP-res to replica map for %s' % file.dataset
                                    self.logger.error(dispError[job.dispatchDBlock])
                                    self.logger.error(out)
                        if self.allReplicaMap.has_key(file.dataset):
                            self.replicaMap[job.dispatchDBlock][file.dataset] = self.allReplicaMap[file.dataset]
        # register dispatch dataset
        dispList = []
        for dispatchDBlock in fileList.keys():
            # ignore empty dataset
            if len(fileList[dispatchDBlock]['lfns']) == 0:
                continue
            # use DQ2
            if (not self.pandaDDM) and job.prodSourceLabel != 'ddm':
                # register dispatch dataset
                self.dispFileList[dispatchDBlock] = fileList[dispatchDBlock]
                disFiles = fileList[dispatchDBlock]
                ddmBackEnd = backEndMap[dispatchDBlock]
                if ddmBackEnd == None:
                    ddmBackEnd = 'rucio'
                metadata = {'hidden':True,
                            'purge_replicas': 0}
                if dispatchDBlock in dsTaskMap and dsTaskMap[dispatchDBlock] not in ['NULL',0]:
                    metadata['task_id'] = str(dsTaskMap[dispatchDBlock])
                tmpMsg = 'registerDataset {ds} {meta}'
                self.logger.debug(tmpMsg.format(ds=dispatchDBlock,meta=str(metadata)))
                nDDMTry = 3
                isOK = False
                for iDDMTry in range(nDDMTry):
                    try:
                        out = rucioAPI.registerDataset(dispatchDBlock,disFiles['lfns'],disFiles['guids'],
                                                       disFiles['fsizes'],disFiles['chksums'],
                                                       lifetime=7,scope='panda',metadata=metadata)
                        isOK = True
                        break
                    except:
                        errType,errValue = sys.exc_info()[:2]
                        self.logger.error("registerDataset : failed with {0}:{1}".format(errType,errValue))
                        if iDDMTry+1 == nDDMTry:
                            break
                        self.logger.debug("sleep {0}/{1}".format(iDDMTry,nDDMTry))
                        time.sleep(10)
                if not isOK:
                    dispError[dispatchDBlock] = "Setupper._setupSource() could not register dispatchDBlock"
                    continue
                self.logger.debug(out)
                newOut = out
                # freezeDataset dispatch dataset
                self.logger.debug('closeDataset '+dispatchDBlock)
                for iDDMTry in range(3):
                    status = False
                    try:
                        rucioAPI.closeDataset(dispatchDBlock)
                        status = True
                        break
                    except:
                        errtype,errvalue = sys.exc_info()[:2]
                        out = 'failed to close : {0} {1}'.format(errtype,errvalue)
                        time.sleep(10)
                if not status:
                    self.logger.error(out)
                    dispError[dispatchDBlock] = "Setupper._setupSource() could not freeze dispatchDBlock with {0}".format(out)
                    continue
            else:
                # use PandaDDM
                self.dispFileList[dispatchDBlock] = fileList[dispatchDBlock]
                # create a fake vuid
                newOut  = {'vuid': str(uuid.uuid4())}
            # get VUID
            try:
                vuid = newOut['vuid']
                # dataset spec. currentfiles is used to count the number of failed jobs
                ds = DatasetSpec()
                ds.vuid = vuid
                ds.name = dispatchDBlock
                ds.type = 'dispatch'
                ds.status = 'defined'
                ds.numberfiles  = len(fileList[dispatchDBlock]['lfns'])
                try:
                    ds.currentfiles = long(sum(filter(None,fileList[dispatchDBlock]['fsizes']))/1024/1024)
                except:
                    ds.currentfiles = 0
                dispList.append(ds)
                self.vuidMap[ds.name] = ds.vuid
            except:
                errtype,errvalue = sys.exc_info()[:2]
                self.logger.error("_setupSource() : %s %s" % (errtype,errvalue))
                dispError[dispatchDBlock] = "Setupper._setupSource() could not decode VUID dispatchDBlock"
        # insert datasets to DB
        self.taskBuffer.insertDatasets(prodList+dispList)
        # job status
        for job in self.jobs:
            if dispError.has_key(job.dispatchDBlock) and dispError[job.dispatchDBlock] != '':
                job.jobStatus = 'failed'
                job.ddmErrorCode = ErrorCode.EC_Setupper                
                job.ddmErrorDiag = dispError[job.dispatchDBlock]
        # delete explicitly some huge variables        
        del fileList
        del prodList
        del prodError
        del dispSiteMap
                

    # create dataset for outputs in the repository and assign destination
    def _setupDestination(self,startIdx=-1,nJobsInLoop=50):
        self.logger.debug('setupDestination idx:%s n:%s' % (startIdx,nJobsInLoop))
        destError   = {}
        datasetList = {}
        newnameList = {}
        snGottenDS  = []
        if startIdx == -1:
            jobsList = self.jobs
        else:
            jobsList = self.jobs[startIdx:startIdx+nJobsInLoop]
        for job in jobsList:
            # ignore failed jobs
            if job.jobStatus in ['failed','cancelled'] or job.isCancelled():
                continue
            zipFileMap = job.getZipFileMap()
            for file in job.Files:
                # ignore input files
                if file.type in ['input','pseudo_input']:
                    continue
                # don't touch with outDS for unmerge jobs
                if job.prodSourceLabel == 'panda' and job.processingType == 'unmerge' and file.type != 'log':
                    continue
                # extract destinationDBlock, destinationSE and computingSite
                dest = (file.destinationDBlock,file.destinationSE,job.computingSite,file.destinationDBlockToken)
                if not destError.has_key(dest):
                    destError[dest] = ''
                    originalName = ''
                    if (job.prodSourceLabel == 'panda') or (job.prodSourceLabel in ['ptest','rc_test'] and \
                                                            job.processingType in ['pathena','prun','gangarobot-rctest']):
                        # keep original name
                        nameList = [file.destinationDBlock]
                    else:
                        # set freshness to avoid redundant DB lookup
                        definedFreshFlag = None
                        if file.destinationDBlock in snGottenDS:
                            # already checked
                            definedFreshFlag = False
                        elif job.prodSourceLabel in ['user','test','prod_test']:
                            # user or test datasets are always fresh in DB
                            definedFreshFlag = True
                        # get serial number
                        sn,freshFlag = self.taskBuffer.getSerialNumber(file.destinationDBlock,definedFreshFlag)
                        if sn == -1:
                            destError[dest] = "Setupper._setupDestination() could not get serial num for %s" % file.destinationDBlock
                            continue
                        if not file.destinationDBlock in snGottenDS:
                            snGottenDS.append(file.destinationDBlock)
                        # new dataset name
                        newnameList[dest] = "%s_sub0%s" % (file.destinationDBlock,sn)
                        if freshFlag or self.resetLocation:
                            # register original dataset and new dataset
                            nameList = [file.destinationDBlock,newnameList[dest]]
                            originalName = file.destinationDBlock
                        else:
                            # register new dataset only
                            nameList = [newnameList[dest]]
                    # create dataset
                    for name in nameList:
                        computingSite = job.computingSite
                        if name == originalName and not name.startswith('panda.um.'):
                            # for original dataset
                            computingSite = file.destinationSE
                        newVUID = None
                        # skip dataset registraton for zip files
                        if file.lfn in zipFileMap:
                            newVUID = str(uuid.uuid4())
                        elif (not self.pandaDDM) and (job.prodSourceLabel != 'ddm') and (job.destinationSE != 'local'):
                            # get src and dest DDM conversion is needed for unknown sites
                            if job.prodSourceLabel == 'user' and not self.siteMapper.siteSpecList.has_key(computingSite):
                                # DQ2 ID was set by using --destSE for analysis job to transfer output
                                tmpSrcDDM = self.siteMapper.getSite(job.computingSite).ddm_output
                            else:                            
                                tmpSrcDDM = self.siteMapper.getSite(computingSite).ddm_output
                            if job.prodSourceLabel == 'user' and not self.siteMapper.siteSpecList.has_key(file.destinationSE):
                                # DQ2 ID was set by using --destSE for analysis job to transfer output 
                                tmpDstDDM = tmpSrcDDM
                            elif DataServiceUtils.getDestinationSE(file.destinationDBlockToken) != None:
                                # destination is specified
                                tmpDstDDM = DataServiceUtils.getDestinationSE(file.destinationDBlockToken)
                            else:
                                tmpDstDDM = self.siteMapper.getSite(file.destinationSE).ddm_output
                            # skip registration for _sub when src=dest
                            if ((tmpSrcDDM == tmpDstDDM and not EventServiceUtils.isMergeAtOS(job.specialHandling)) \
                                    or DataServiceUtils.getDistributedDestination(file.destinationDBlockToken) != None) \
                                    and name != originalName and re.search('_sub\d+$',name) != None:
                                # create a fake vuid
                                newVUID = str(uuid.uuid4())
                            else:
                                # get list of tokens    
                                tmpTokenList = file.destinationDBlockToken.split(',')
                                # get locations
                                usingT1asT2 = False
                                if job.prodSourceLabel == 'user' and not self.siteMapper.siteSpecList.has_key(computingSite):
                                    dq2IDList = [self.siteMapper.getSite(job.computingSite).ddm_output]
                                else:
                                    if self.siteMapper.getSite(computingSite).cloud != job.getCloud() and \
                                            re.search('_sub\d+$',name) != None and \
                                            (not job.prodSourceLabel in ['user','panda']) and \
                                            (not self.siteMapper.getSite(computingSite).ddm_output.endswith('PRODDISK')):
                                        # T1 used as T2. Use both DATADISK and PRODDISK as locations while T1 PRODDISK is phasing out
                                        dq2IDList = [self.siteMapper.getSite(computingSite).ddm_output]
                                        if self.siteMapper.getSite(computingSite).setokens_output.has_key('ATLASPRODDISK'):
                                            dq2IDList += [self.siteMapper.getSite(computingSite).setokens_output['ATLASPRODDISK']]
                                        usingT1asT2 = True
                                    else:
                                        dq2IDList = [self.siteMapper.getSite(computingSite).ddm_output]
                                # use another location when token is set
                                if re.search('_sub\d+$',name) == None and DataServiceUtils.getDestinationSE(file.destinationDBlockToken) != None:
                                    # destination is specified
                                    dq2IDList = [DataServiceUtils.getDestinationSE(file.destinationDBlockToken)]
                                elif (not usingT1asT2) and (not file.destinationDBlockToken in ['NULL','']):
                                    dq2IDList = []
                                    for tmpToken in tmpTokenList:
                                        # set default
                                        dq2ID = self.siteMapper.getSite(computingSite).ddm_output
                                        # convert token to DQ2ID
                                        if self.siteMapper.getSite(computingSite).setokens_output.has_key(tmpToken):
                                            dq2ID = self.siteMapper.getSite(computingSite).setokens_output[tmpToken]
                                        # replace or append    
                                        if len(tmpTokenList) <= 1 or name != originalName:
                                            # use location consistent with token
                                            dq2IDList = [dq2ID]
                                            break
                                        else:
                                            # use multiple locations for _tid
                                            if not dq2ID in dq2IDList:
                                                dq2IDList.append(dq2ID)
                                # set hidden flag for _sub
                                tmpActivity = None
                                tmpLifeTime = None
                                tmpMetadata = None
                                if name != originalName and re.search('_sub\d+$',name) != None:
                                    tmpActivity = 'Production Output'
                                    tmpLifeTime = 14
                                    tmpMetadata = {'hidden':True,
                                                   'purge_replicas': 0}
                                # backend
                                ddmBackEnd = job.getDdmBackEnd()
                                if ddmBackEnd == None:
                                    ddmBackEnd = 'rucio'
                                # register dataset
                                self.logger.debug('registerNewDataset {name} metadata={meta}'.format(name=name,
                                                                                                     meta=tmpMetadata))
                                isOK = False
                                for iDDMTry in range(3):
                                    try:
                                        out = rucioAPI.registerDataset(name,metadata=tmpMetadata,
                                                                       lifetime=tmpLifeTime)
                                        self.logger.debug(out)
                                        newVUID = out['vuid']
                                        isOK = True
                                        break
                                    except:
                                        errType,errValue = sys.exc_info()[:2]
                                        self.logger.error("registerDataset : failed with {0}:{1}".format(errType,errValue))
                                        time.sleep(10)
                                if not isOK:
                                    tmpMsg = "Setupper._setupDestination() could not register : %s" % name
                                    destError[dest] = tmpMsg
                                    self.logger.error(tmpMsg)
                                    continue
                                # register dataset locations
                                if (job.lockedby == 'jedi' and job.getDdmBackEnd() == 'rucio' and job.prodSourceLabel in ['panda','user']) or \
                                        DataServiceUtils.getDistributedDestination(file.destinationDBlockToken) != None:
                                    # skip registerDatasetLocations
                                    status,out = True,''
                                elif name == originalName or tmpSrcDDM != tmpDstDDM or \
                                       job.prodSourceLabel == 'panda' or (job.prodSourceLabel in ['ptest','rc_test'] and \
                                                                          job.processingType in ['pathena','prun','gangarobot-rctest']) \
                                       or len(tmpTokenList) > 1 or EventServiceUtils.isMergeAtOS(job.specialHandling):
                                    # set replica lifetime to _sub
                                    repLifeTime = None
                                    if (name != originalName and re.search('_sub\d+$',name) != None) or \
                                            (name == originalName and name.startswith('panda.')):
                                        repLifeTime = 14
                                    elif name.startswith('hc_test') or \
                                            name.startswith('panda.install.') or \
                                            name.startswith('user.gangarbt.'):
                                        repLifeTime = 7
                                    # register location
                                    isOK = True
                                    for dq2ID in dq2IDList:
                                        activity = DataServiceUtils.getActivityForOut(job.prodSourceLabel)
                                        tmpStr = 'registerDatasetLocation {name} {dq2ID} lifetime={repLifeTime} activity={activity}'
                                        self.logger.debug(tmpStr.format(name=name,
                                                                        dq2ID=dq2ID,
                                                                        repLifeTime=repLifeTime,
                                                                        activity=activity,
                                                                        ))
                                        status = False
                                        # invalid location
                                        if dq2ID is None:
                                            out = "wrong location : {0}".format(dq2ID)
                                            self.logger.error(out)
                                            break
                                        for iDDMTry in range(3):
                                            try:
                                                out = rucioAPI.registerDatasetLocation(name,[dq2ID],lifetime=repLifeTime,
                                                                                       activity=activity)
                                                self.logger.debug(out)
                                                status = True
                                                break
                                            except:
                                                errType,errValue = sys.exc_info()[:2]
                                                out = "{0}:{1}".format(errType,errValue)
                                                self.logger.error("registerDatasetLocation : failed with {0}".format(out))
                                                time.sleep(10)
                                        # failed
                                        if not status:
                                            break
                                else:
                                    # skip registerDatasetLocations
                                    status,out = True,''
                                if not status:
                                    destError[dest] = "Could not register location : %s %s" % (name,out.split('\n')[-1])
                        # already failed    
                        if destError[dest] != '' and name == originalName:
                            break
                        # get vuid
                        if newVUID is None:
                            self.logger.debug('listDatasets '+name)
                            for iDDMTry in range(3):
                                newOut,errMsg = rucioAPI.listDatasets(name)
                                if newOut is None:
                                    time.sleep(10)
                                else:
                                    break
                            if newOut is None:
                                self.logger.error(errMsg)
                            else:
                                self.logger.debug(newOut)
                                newVUID = newOut[name]['vuids'][0]
                        try:
                            # dataset spec
                            ds = DatasetSpec()
                            ds.vuid         = newVUID
                            ds.name         = name
                            ds.type         = 'output'
                            ds.numberfiles  = 0
                            ds.currentfiles = 0
                            ds.status       = 'defined'
                            # append
                            datasetList[(name,file.destinationSE,computingSite)] = ds
                        except:
                            # set status
                            errtype,errvalue = sys.exc_info()[:2]
                            self.logger.error("_setupDestination() : %s %s" % (errtype,errvalue))
                            destError[dest] = "Setupper._setupDestination() could not get VUID : %s" % name
                # set new destDBlock
                if newnameList.has_key(dest):
                    file.destinationDBlock = newnameList[dest]
                # update job status if failed
                if destError[dest] != '':
                    job.jobStatus = 'failed'
                    job.ddmErrorCode = ErrorCode.EC_Setupper                
                    job.ddmErrorDiag = destError[dest]
                else:
                    newdest = (file.destinationDBlock,file.destinationSE,job.computingSite)
                    # increment number of files
                    datasetList[newdest].numberfiles = datasetList[newdest].numberfiles + 1
        # dump
        for tmpDsKey in datasetList.keys():
            if re.search('_sub\d+$',tmpDsKey[0]) != None:
                self.logger.debug('made sub:%s for nFiles=%s' % (tmpDsKey[0],datasetList[tmpDsKey].numberfiles))
        # insert datasets to DB
        return self.taskBuffer.insertDatasets(datasetList.values())
        

    #  subscribe sites to distpatchDBlocks
    def _subscribeDistpatchDB(self):
        dispError  = {}
        failedJobs = []
        ddmJobs    = []
        ddmUser    = 'NULL'
        for job in self.jobs:
            # ignore failed jobs
            if job.jobStatus in ['failed','cancelled'] or job.isCancelled():
                continue
            # ignore no dispatch jobs
            if job.dispatchDBlock=='NULL' or job.computingSite=='NULL':
                continue
            # backend                                                                                                                                                         
            ddmBackEnd = job.getDdmBackEnd()
            if ddmBackEnd == None:
                ddmBackEnd = 'rucio'
            # extract dispatchDBlock and computingSite
            disp = (job.dispatchDBlock,job.computingSite)
            if dispError.has_key(disp) == 0:
                dispError[disp] = ''
                # DQ2 IDs
                tmpSrcID = 'BNL_ATLAS_1'
                if job.prodSourceLabel in ['user','panda']:
                    tmpSrcID = job.computingSite
                elif self.siteMapper.checkCloud(job.getCloud()):
                    # use cloud's source
                    tmpSrcID = self.siteMapper.getCloud(job.getCloud())['source']
                srcDQ2ID = self.siteMapper.getSite(tmpSrcID).ddm_output # TODO: check with Tadashi
                # destination
                tmpDstID = job.computingSite
                if srcDQ2ID != self.siteMapper.getSite(job.computingSite).ddm_input and \
                       srcDQ2ID in self.siteMapper.getSite(job.computingSite).setokens_input.values():
                    # direct usage of remote SE. Mainly for prestaging
                    tmpDstID = tmpSrcID
                    self.logger.debug('use remote SiteSpec of %s for %s' % (tmpDstID,job.computingSite))
                # use srcDQ2ID as dstDQ2ID when it is associated to dest
                dstSiteSpec = self.siteMapper.getSite(tmpDstID)
                if dstSiteSpec.ddm_endpoints_input.isAssociated(srcDQ2ID):
                    dstDQ2ID = srcDQ2ID
                else:
                    dstDQ2ID = dstSiteSpec.ddm_input
                # check if missing at T1
                missingAtT1 = False
                if job.prodSourceLabel in ['managed','test']:
                    for tmpLFN in self.dispFileList[job.dispatchDBlock]['lfns']:
                        if not job.getCloud() in self.missingFilesInT1:
                            break
                        if tmpLFN in self.missingFilesInT1[job.getCloud()] or \
                                tmpLFN.split(':')[-1] in self.missingFilesInT1[job.getCloud()]:
                            missingAtT1 = True
                            break
                    self.logger.debug('{0} missing at T1 : {1}'.format(job.dispatchDBlock,missingAtT1))
                # use DQ2
                if (not self.pandaDDM) and job.prodSourceLabel != 'ddm':
                    # look for replica
                    dq2ID = srcDQ2ID
                    dq2IDList = []
                    # register replica
                    isOK = False
                    if dq2ID != dstDQ2ID or missingAtT1:
                        # make list
                        if self.replicaMap.has_key(job.dispatchDBlock):
                            # set DQ2 ID for DISK
                            if not srcDQ2ID.endswith('_DATADISK'):
                                hotID  = re.sub('_MCDISK','_HOTDISK', srcDQ2ID)
                                diskID = re.sub('_MCDISK','_DATADISK',srcDQ2ID)
                                tapeID = re.sub('_MCDISK','_DATATAPE',srcDQ2ID)
                                mctapeID = re.sub('_MCDISK','_MCTAPE',srcDQ2ID)
                            else:
                                hotID  = re.sub('_DATADISK','_HOTDISK', srcDQ2ID)
                                diskID = re.sub('_DATADISK','_DATADISK',srcDQ2ID)
                                tapeID = re.sub('_DATADISK','_DATATAPE',srcDQ2ID)
                                mctapeID = re.sub('_DATADISK','_MCTAPE',srcDQ2ID)
                            # DQ2 ID is mixed with TAIWAN-LCG2 and TW-FTT     
                            if job.getCloud() in ['TW',]:
                                tmpSiteSpec = self.siteMapper.getSite(tmpSrcID)
                                if tmpSiteSpec.setokens_input.has_key('ATLASDATADISK'):
                                    diskID = tmpSiteSpec.setokens_input['ATLASDATADISK']
                                if tmpSiteSpec.setokens_input.has_key('ATLASDATATAPE'):
                                    tapeID = tmpSiteSpec.setokens_input['ATLASDATATAPE']
                                if tmpSiteSpec.setokens_input.has_key('ATLASMCTAPE'):
                                    mctapeID = tmpSiteSpec.setokens_input['ATLASMCTAPE']
                                hotID  = 'TAIWAN-LCG2_HOTDISK'
                            for tmpDataset,tmpRepMap in self.replicaMap[job.dispatchDBlock].iteritems():
                                if tmpRepMap.has_key(hotID):
                                    # HOTDISK
                                    if not hotID in dq2IDList:
                                        dq2IDList.append(hotID)
                                if tmpRepMap.has_key(srcDQ2ID):
                                    # MCDISK
                                    if not srcDQ2ID in dq2IDList:
                                        dq2IDList.append(srcDQ2ID)
                                if tmpRepMap.has_key(diskID):
                                    # DATADISK
                                    if not diskID in dq2IDList:
                                        dq2IDList.append(diskID)
                                if job.getCloud() == 'US' and tmpRepMap.has_key('BNLPANDA'):
                                    # BNLPANDA
                                    if not 'BNLPANDA' in dq2IDList:
                                        dq2IDList.append('BNLPANDA')
                                if tmpRepMap.has_key(tapeID):
                                    # DATATAPE
                                    if not tapeID in dq2IDList:
                                        dq2IDList.append(tapeID)
                                if tmpRepMap.has_key(mctapeID):
                                    # MCTAPE
                                    if not mctapeID in dq2IDList:
                                        dq2IDList.append(mctapeID)
                            # consider cloudconfig.tier1se 
                            tmpCloudSEs = DataServiceUtils.getEndpointsAtT1(tmpRepMap,self.siteMapper,job.getCloud())
                            useCloudSEs = []
                            for tmpCloudSE in tmpCloudSEs:
                                if not tmpCloudSE in dq2IDList:
                                    useCloudSEs.append(tmpCloudSE)
                            if useCloudSEs != []:
                                dq2IDList += useCloudSEs
                                self.logger.debug('use additional endpoints %s from cloudconfig' % (str(useCloudSEs)))
                        # use default location if empty
                        if dq2IDList == []:
                            dq2IDList = [dq2ID]
                        # register dataset locations
                        if missingAtT1:
                            # without locatios to let DDM find sources
                            isOK = True
                        else:
                            isOK = True
                    else:
                        # register locations later for prestaging
                        isOK = True
                    if not isOK:
                        dispError[disp] = "Setupper._subscribeDistpatchDB() could not register location"
                    else:
                        isOK = False
                        # assign destination
                        optSub = {'DATASET_COMPLETE_EVENT' : ['http://%s:%s/server/panda/datasetCompleted' % \
                                                              (panda_config.pserverhosthttp,panda_config.pserverporthttp)]}
                        optSource = {}
                        optSrcPolicy = 001000 | 010000
                        dq2ID = dstDQ2ID
                        # prestaging
                        if srcDQ2ID == dstDQ2ID and not missingAtT1:
                            # prestage to associated endpoints 
                            if job.prodSourceLabel in ['user','panda']:
                                # use DATADISK
                                tmpSiteSpec = self.siteMapper.getSite(job.computingSite)
                                if 'ATLASDATADISK' in tmpSiteSpec.setokens_input:
                                    tmpDq2ID = tmpSiteSpec.setokens_input['ATLASDATADISK']
                                    if tmpDq2ID in tmpSiteSpec.ddm_endpoints_input.getLocalEndPoints():
                                        self.logger.debug('use {0} instead of {1} for tape prestaging'.format(tmpDq2ID, dq2ID))
                                        dq2ID = tmpDq2ID
                            self.logger.debug('use {0} for tape prestaging'.format(dq2ID))
                            optSrcPolicy = 000010
                            # register dataset locations
                            isOK = True
                        else:
                            isOK = True
                            # set sources to handle T2s in another cloud and to transfer dis datasets being split in multiple sites 
                            if not missingAtT1:
                                for tmpDQ2ID in dq2IDList:
                                    optSource[tmpDQ2ID] = {'policy' : 0}
                            # T1 used as T2
                            if job.getCloud() != self.siteMapper.getSite(tmpDstID).cloud and \
                                   (not dstDQ2ID.endswith('PRODDISK')) and \
                                   (not job.prodSourceLabel in ['user','panda']) and \
                                   self.siteMapper.getSite(tmpDstID).cloud in ['US']:
                                seTokens = self.siteMapper.getSite(tmpDstID).setokens_input
                                # use T1_PRODDISK
                                if seTokens.has_key('ATLASPRODDISK'):
                                    dq2ID = seTokens['ATLASPRODDISK']
                        # set share and activity
                        if job.prodSourceLabel in ['user','panda']:
                            optShare = "production"
                            optActivity = "Analysis Input"
                            #optOwner = DataServiceUtils.cleanupDN(job.prodUserID)
                            optOwner = None
                        else:
                            optShare = "production"
                            optOwner = None
                            if job.processingType == 'urgent' or job.currentPriority > 1000:
                                optActivity = 'Express'
                            else:
                                optActivity = "Production Input"
                        # taskID
                        if job.jediTaskID not in ['NULL',0]:
                            optComment = 'task_id:{0}'.format(job.jediTaskID)
                        else:
                            optComment = None
                        if not isOK:
                            dispError[disp] = "Setupper._subscribeDistpatchDB() could not register location for prestage"
                        else:
                            # register subscription
                            self.logger.debug('%s %s %s' % ('registerDatasetSubscription',
                                                            (job.dispatchDBlock,dq2ID),
                                                            {'activity':optActivity,'lifetime':7,'dn':optOwner,
                                                             'comment':optComment}))
                            for iDDMTry in range(3):
                                try:
                                    status = rucioAPI.registerDatasetSubscription(job.dispatchDBlock,[dq2ID],
                                                                                  activity=optActivity,
                                                                                  lifetime=7,
                                                                                  dn=optOwner,
                                                                                  comment=optComment)
                                    out = 'OK'
                                    break
                                except:
                                    status = False
                                    errType,errValue = sys.exc_info()[:2]
                                    out = "%s %s" % (errType,errValue)
                                    time.sleep(10)
                            if not status:
                                self.logger.error(out)
                                dispError[disp] = "Setupper._subscribeDistpatchDB() could not register subscription"
                            else:
                                self.logger.debug(out)
            # failed jobs
            if dispError[disp] != '':
                job.jobStatus = 'failed'
                job.ddmErrorCode = ErrorCode.EC_Setupper                
                job.ddmErrorDiag = dispError[disp]
                failedJobs.append(job)
        # update failed jobs only. succeeded jobs should be activate by DDM callback
        self.updateFailedJobs(failedJobs)
        # submit ddm jobs
        if ddmJobs != []:
            ddmRet = self.taskBuffer.storeJobs(ddmJobs,ddmUser,joinThr=True)
            # update datasets
            ddmIndex = 0
            ddmDsList = []
            for ddmPandaID,ddmJobDef,ddmJobName in ddmRet:
                # invalid PandaID
                if ddmPandaID in ['NULL',None]:
                    continue
                # get dispatch dataset
                dsName = ddmJobs[ddmIndex].jobParameters.split()[-1]
                ddmIndex += 1
                tmpDS = self.taskBuffer.queryDatasetWithMap({'name':dsName})
                if tmpDS != None:
                    # set MoverID
                    tmpDS.MoverID = ddmPandaID
                    ddmDsList.append(tmpDS)
            # update
            if ddmDsList != []:
                self.taskBuffer.updateDatasets(ddmDsList)


    # correct LFN for attemptNr
    def _correctLFN(self):
        lfnMap = {}
        valMap = {}
        prodError = {}
        missingDS = {}
        jobsWaiting   = []
        jobsFailed    = []
        jobsProcessed = []
        allLFNs   = {}
        allGUIDs  = {}
        allScopes = {}
        cloudMap  = {}
        lfnDsMap  = {}
        replicaMap = {}
        self.logger.debug('go into LFN correction')
        # collect input LFNs
        inputLFNs = set()
        for tmpJob in self.jobs:
            for tmpFile in tmpJob.Files:
                if tmpFile.type == 'input':
                    inputLFNs.add(tmpFile.lfn)
                    genLFN = re.sub('\.\d+$','',tmpFile.lfn)
                    inputLFNs.add(genLFN)
                    if not tmpFile.GUID in ['NULL','',None]:
                        if not tmpFile.dataset in self.lfnDatasetMap:
                            self.lfnDatasetMap[tmpFile.dataset] = {}
                        self.lfnDatasetMap[tmpFile.dataset][tmpFile.lfn] = {'guid':tmpFile.GUID,
                                                                            'chksum':tmpFile.checksum,
                                                                            'md5sum':tmpFile.md5sum,
                                                                            'fsize':tmpFile.fsize,
                                                                            'scope':tmpFile.scope}
        # loop over all jobs
        for job in self.jobs:
            if self.onlyTA:            
                self.logger.debug("start TA session %s" % (job.taskID))
            # check if sitename is known
            if job.computingSite != 'NULL' and (not job.computingSite in self.siteMapper.siteSpecList.keys()):
                job.jobStatus    = 'failed'
                job.ddmErrorCode = ErrorCode.EC_Setupper                
                job.ddmErrorDiag = "computingSite:%s is unknown" % job.computingSite
                # append job for downstream process
                jobsProcessed.append(job)
                # error message for TA
                if self.onlyTA:                            
                    self.logger.error(job.ddmErrorDiag)
                continue
            # ignore no prodDBlock jobs or container dataset
            if job.prodDBlock == 'NULL':
                # append job to processed list
                jobsProcessed.append(job)
                continue
            # check if T1
            tmpSrcID = self.siteMapper.getCloud(job.getCloud())['source']
            srcDQ2ID = self.siteMapper.getSite(tmpSrcID).ddm_output
            dstDQ2ID = self.siteMapper.getSite(job.computingSite).ddm_input
            # collect datasets
            datasets = []
            for file in job.Files:
                if file.type == 'input' and file.dispatchDBlock == 'NULL' \
                        and (file.GUID == 'NULL' or job.prodSourceLabel in ['managed','test','ptest']):
                    if not file.dataset in datasets:
                        datasets.append(file.dataset)
                if srcDQ2ID == dstDQ2ID and file.type == 'input' and job.prodSourceLabel in ['managed','test','ptest'] \
                        and file.status != 'ready':
                    if not job.getCloud() in self.missingFilesInT1:
                        self.missingFilesInT1[job.getCloud()] = set()
                    self.missingFilesInT1[job.getCloud()].add(file.lfn)
            # get LFN list
            for dataset in datasets:
                if not dataset in lfnMap.keys():
                    prodError[dataset] = ''
                    lfnMap[dataset] = {}
                    # get LFNs
                    status,out = self.getListFilesInDataset(dataset,inputLFNs)
                    if status != 0:
                        self.logger.error(out)
                        prodError[dataset] = 'could not get file list of prodDBlock %s' % dataset
                        self.logger.error(prodError[dataset])
                        # doesn't exist in DQ2
                        if status == -1:
                            missingDS[dataset] = "DS:%s not found in DDM" % dataset
                        else:
                            missingDS[dataset] = out
                    else:
                        # make map (key: LFN w/o attemptNr, value: LFN with attemptNr)
                        items = out
                        try:
                            # loop over all files    
                            for tmpLFN,vals in items.iteritems():
                                valMap[tmpLFN] = vals
                                genLFN = re.sub('\.\d+$','',tmpLFN)
                                if lfnMap[dataset].has_key(genLFN):
                                    # get attemptNr
                                    newAttNr = 0
                                    newMat = re.search('\.(\d+)$',tmpLFN)
                                    if newMat != None:
                                        newAttNr = int(newMat.group(1))
                                    oldAttNr = 0
                                    oldMat = re.search('\.(\d+)$',lfnMap[dataset][genLFN])
                                    if oldMat != None:
                                        oldAttNr = int(oldMat.group(1))
                                    # compare
                                    if newAttNr > oldAttNr:
                                        lfnMap[dataset][genLFN] = tmpLFN
                                else:
                                    lfnMap[dataset][genLFN] = tmpLFN
                                # mapping from LFN to DS
                                lfnDsMap[lfnMap[dataset][genLFN]] = dataset
                        except:
                            prodError[dataset] = 'could not convert HTTP-res to map for prodDBlock %s' % dataset
                            self.logger.error(prodError[dataset])
                            self.logger.error(out)
                    # get replica locations        
                    if (self.onlyTA or job.prodSourceLabel in ['managed','test']) \
                           and prodError[dataset] == '' and (not replicaMap.has_key(dataset)):
                        if dataset.endswith('/'):
                            status,out = self.getListDatasetReplicasInContainer(dataset)
                        else:
                            status,out = self.getListDatasetReplicas(dataset,False)
                        if status != 0 or out.startswith('Error'):
                            prodError[dataset] = 'could not get locations for %s' % dataset
                            self.logger.error(prodError[dataset])
                            self.logger.error(out)
                        else:
                            tmpRepSites = {}
                            try:
                                # convert res to map
                                exec "tmpRepSites = %s" % out
                                replicaMap[dataset] = tmpRepSites
                            except:
                                prodError[dataset] = 'could not convert HTTP-res to replica map for %s' % dataset
                                self.logger.error(prodError[dataset])
                                self.logger.error(out)
                            # append except DBR
                            if not dataset.startswith('ddo'):
                                self.replicaMapForBroker[dataset] = tmpRepSites
            # error
            isFailed = False
            # check for failed
            for dataset in datasets:
                if missingDS.has_key(dataset):
                    job.jobStatus    = 'failed'
                    job.ddmErrorCode = ErrorCode.EC_GUID
                    job.ddmErrorDiag = missingDS[dataset]
                    # set missing
                    for tmpFile in job.Files:
                        if tmpFile.dataset == dataset:
                            tmpFile.status = 'missing'
                    # append        
                    jobsFailed.append(job)
                    isFailed = True
                    # message for TA
                    if self.onlyTA:                            
                        self.logger.error(missingDS[dataset])
                        self.sendTaMesg("%s %s" % (job.taskID,missingDS[dataset]),msgType='error')
                    else:
                        self.logger.debug("%s failed with %s" % (job.PandaID,missingDS[dataset]))
                    break
            if isFailed:
                continue
            # check for waiting
            for dataset in datasets:
                if prodError[dataset] != '':
                    # append job to waiting list
                    jobsWaiting.append(job)
                    isFailed = True
                    # message for TA
                    if self.onlyTA:                            
                        self.logger.error(prodError[dataset])
                    break
            if isFailed:
                continue
            if not self.onlyTA:
                # replace generic LFN with real LFN
                replaceList = []
                isFailed = False
                for file in job.Files:
                    if file.type == 'input' and file.dispatchDBlock == 'NULL':
                        addToLfnMap = True
                        if file.GUID == 'NULL':
                            # get LFN w/o attemptNr
                            basename = re.sub('\.\d+$','',file.lfn)
                            if basename == file.lfn:
                                # replace
                                if basename in lfnMap[file.dataset].keys():
                                    file.lfn = lfnMap[file.dataset][basename]
                                    replaceList.append((basename,file.lfn))
                            # set GUID
                            if file.lfn in valMap:
                                file.GUID     = valMap[file.lfn]['guid']
                                file.fsize    = valMap[file.lfn]['fsize']
                                file.md5sum   = valMap[file.lfn]['md5sum']
                                file.checksum = valMap[file.lfn]['chksum']
                                file.scope    = valMap[file.lfn]['scope']
                                # remove white space
                                if file.md5sum != None:
                                    file.md5sum = file.md5sum.strip()
                                if file.checksum != None:
                                    file.checksum = file.checksum.strip()
                        else:
                            if not job.prodSourceLabel in ['managed','test']:
                                addToLfnMap = False
                        # check missing file
                        if file.GUID == 'NULL' or job.prodSourceLabel in ['managed','test']:
                            if not file.lfn in valMap:
                                # append job to waiting list
                                errMsg = "GUID for %s not found in rucio" % file.lfn
                                self.logger.error(errMsg)
                                file.status = 'missing'
                                if not job in jobsFailed:
                                    job.jobStatus    = 'failed'                        
                                    job.ddmErrorCode = ErrorCode.EC_GUID
                                    job.ddmErrorDiag = errMsg
                                    jobsFailed.append(job)
                                    isFailed = True
                                continue
                        # add to allLFNs/allGUIDs
                        if addToLfnMap:
                            if not allLFNs.has_key(job.getCloud()):
                                allLFNs[job.getCloud()] = []
                            if not allGUIDs.has_key(job.getCloud()):
                                allGUIDs[job.getCloud()] = []
                            if not allScopes.has_key(job.getCloud()):
                                allScopes[job.getCloud()] = []
                            allLFNs[job.getCloud()].append(file.lfn)
                            allGUIDs[job.getCloud()].append(file.GUID)
                            allScopes[job.getCloud()].append(file.scope)
                # modify jobParameters
                if not isFailed:
                    for patt,repl in replaceList:
                        job.jobParameters = re.sub('%s ' % patt, '%s ' % repl, job.jobParameters)
                    # append job to processed list
                    jobsProcessed.append(job)
        # return if TA only
        if self.onlyTA:
            self.logger.debug("end TA sessions")
            return
        self.logger.debug('checking missing files at T1')
        # get missing LFNs from source LRC/LFC
        missLFNs = {}
        for cloudKey in allLFNs.keys():
            # use cloud's source
            tmpSrcID   = self.siteMapper.getCloud(cloudKey)['source']
            srcSiteSpec = self.siteMapper.getSite(tmpSrcID)
            allSEs = srcSiteSpec.ddm_endpoints_input.getAllEndPoints() # TODO: confirm with Tadashi. TAPE endpoints!!!
            tapeSEs = srcSiteSpec.ddm_endpoints_input.getTapeEndPoints()
            # get availabe files
            tmpStat,tmpAvaFiles = rucioAPI.listFileReplicas(allScopes[cloudKey],
                                                            allLFNs[cloudKey],
                                                            allSEs)
            if not tmpStat:
                self.logger.error('failed to get file replicas')
                tmpAvaFiles = {}
            # look for missing or tape files
            tmpMissLFNs = set()
            tmpTapeLFNs = set()
            for tmpLFN in allLFNs[cloudKey]:
                if tmpLFN not in tmpAvaFiles:
                    tmpMissLFNs.add(tmpLFN)
                else:
                    for tmpTapeSE in tapeSEs:
                        if tmpTapeSE in tmpAvaFiles[tmpLFN]:
                            tmpTapeLFNs.add(tmpLFN)
                            break
            # append
            if not missLFNs.has_key(cloudKey):
                missLFNs[cloudKey] = []
            missLFNs[cloudKey] += tmpMissLFNs
            if not cloudKey in self.missingFilesInT1:
                self.missingFilesInT1[cloudKey] = set()
            for tmpMissLFN in tmpMissLFNs:
                self.missingFilesInT1[cloudKey].add(tmpMissLFN)
            # delete tape files to trigger prestaging
            for tmpTapeLFN in tmpTapeLFNs:
                if tmpTapeLFN in self.missingFilesInT1[cloudKey]:
                    self.missingFilesInT1[cloudKey].remove(tmpTapeLFN)
        self.logger.debug('checking T2 LFC')
        # check availability of files at T2
        for cloudKey,tmpAllLFNs in allLFNs.iteritems():
            if len(self.jobs) > 0 and (self.jobs[0].prodSourceLabel in ['user','panda','ddm'] or \
                                       self.jobs[0].processingType.startswith('gangarobot') or \
                                       self.jobs[0].processingType.startswith('hammercloud')):
                continue
            # add cloud
            if not self.availableLFNsInT2.has_key(cloudKey):
                self.availableLFNsInT2[cloudKey] = {}
            # loop over all files to find datasets
            for tmpCheckLFN in tmpAllLFNs:
                # add dataset
                if not lfnDsMap.has_key(tmpCheckLFN):
                    continue
                tmpDsName = lfnDsMap[tmpCheckLFN]
                if not self.availableLFNsInT2[cloudKey].has_key(tmpDsName):
                    # collect sites
                    tmpSiteNameDQ2Map = DataServiceUtils.getSitesWithDataset(tmpDsName,self.siteMapper,replicaMap,cloudKey,getDQ2ID=True)
                    if tmpSiteNameDQ2Map == {}:
                        continue
                    self.availableLFNsInT2[cloudKey][tmpDsName] = {'allfiles':[],'allguids':[],'allscopes':[],'sites':{}}
                    for tmpSiteName in tmpSiteNameDQ2Map.keys():
                        self.availableLFNsInT2[cloudKey][tmpDsName]['sites'][tmpSiteName] = []
                    self.availableLFNsInT2[cloudKey][tmpDsName]['siteDQ2IDs'] = tmpSiteNameDQ2Map   
                # add files    
                if not tmpCheckLFN in self.availableLFNsInT2[cloudKey][tmpDsName]:
                    self.availableLFNsInT2[cloudKey][tmpDsName]['allfiles'].append(tmpCheckLFN)
                    self.availableLFNsInT2[cloudKey][tmpDsName]['allguids'].append(allGUIDs[cloudKey][allLFNs[cloudKey].index(tmpCheckLFN)])
                    self.availableLFNsInT2[cloudKey][tmpDsName]['allscopes'].append(allScopes[cloudKey][allLFNs[cloudKey].index(tmpCheckLFN)])
            # get available files at each T2
            for tmpDsName in self.availableLFNsInT2[cloudKey].keys():
                checkedDq2SiteMap = {}
                checkLfcSeMap = {}
                for tmpSiteName in self.availableLFNsInT2[cloudKey][tmpDsName]['sites'].keys():
                    tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                    # get catalog 
                    catURL = 'rucio://atlas-rucio.cern.ch:/grid/atlas'
                    # add catalog
                    if not checkLfcSeMap.has_key(catURL):
                        checkLfcSeMap[catURL] = {}
                    # add site
                    if not checkLfcSeMap[catURL].has_key(tmpSiteName):
                        checkLfcSeMap[catURL][tmpSiteName] = []
                    # add SE        
                        checkLfcSeMap[catURL][tmpSiteName] += tmpSiteSpec.ddm_endpoints_input.getTapeEndPoints()
                # LFC lookup
                for tmpCatURL in checkLfcSeMap.keys():  
                    # get SEs
                    tmpSEList = []
                    tmpSiteNameList = []
                    for tmpSiteName in checkLfcSeMap[tmpCatURL].keys():
                        tmpSEList += checkLfcSeMap[tmpCatURL][tmpSiteName]
                        tmpSiteNameList.append(tmpSiteName)
                    # get available file list
                    self.logger.debug('checking T2 LFC=%s for %s %s' % (tmpCatURL,tmpSEList,','.join(tmpSiteNameList)))
                    tmpStat,bulkAvFiles = rucioAPI.listFileReplicas(self.availableLFNsInT2[cloudKey][tmpDsName]['allscopes'],
                                                                    self.availableLFNsInT2[cloudKey][tmpDsName]['allfiles'],
                                                                    tmpSEList)
                    if not tmpStat:
                        self.logger.error('failed to get file replicas')
                        bulkAvFiles = {}
                    # check each site
                    for tmpSiteName,tmpSiteSEs in checkLfcSeMap[tmpCatURL].iteritems():
                        tmpSiteSEs = set(tmpSiteSEs)
                        self.availableLFNsInT2[cloudKey][tmpDsName]['sites'][tmpSiteName] = []                        
                        for tmpLFNck,tmpFileSEs in bulkAvFiles.iteritems():
                            siteHasFileFlag = False
                            for tmpFileSE in tmpFileSEs:
                                if tmpFileSE in tmpSiteSEs:
                                    siteHasFileFlag = True
                                    break
                            # append
                            if siteHasFileFlag:
                                self.availableLFNsInT2[cloudKey][tmpDsName]['sites'][tmpSiteName].append(tmpLFNck)
                        self.logger.debug('available %s files at %s T2=%s for %s' % \
                                              (len(self.availableLFNsInT2[cloudKey][tmpDsName]['sites'][tmpSiteName]),
                                               cloudKey,tmpSiteName,tmpDsName))
        self.logger.debug('missLFNs at T1 %s' % missLFNs)
        self.logger.debug('missLFNs at T1 with JEDI {0}'.format(str(self.missingFilesInT1)))
        # check if files in source LRC/LFC
        tmpJobList = tuple(jobsProcessed)
        for job in tmpJobList:
            # check only production/test jobs
            if not job.prodSourceLabel in ['managed','test','software','rc_test','ptest']:
                continue
            # don't check if site is already set
            if job.prodSourceLabel in ['managed','test'] and not job.computingSite in ['NULL','',None]:
                continue
            missingFlag = False
            for file in job.Files:
                if file.type == 'input':
                    if missLFNs.has_key(job.getCloud()) and file.lfn in missLFNs[job.getCloud()]:
                        # set file status
                        file.status = 'missing'
                        missingFlag = True
            # check if missing files are available at T2s
            goToT2 = None
            if missingFlag:
                tmpCandT2s = None
                for tmpFile in job.Files:
                    if tmpFile.type == 'input' and tmpFile.status == 'missing':
                        # no cloud info
                        if not self.availableLFNsInT2.has_key(job.getCloud()):
                            goToT2 = False 
                            break
                        # no dataset info
                        if not self.availableLFNsInT2[job.getCloud()].has_key(tmpFile.dataset):
                            goToT2 = False                             
                            break
                        # initial candidates
                        if tmpCandT2s == None:
                            tmpCandT2s = self.availableLFNsInT2[job.getCloud()][tmpFile.dataset]['sites']
                        # check all candidates    
                        newCandT2s = []    
                        for tmpCandT2 in tmpCandT2s:
                            # site doesn't have the dataset
                            if not self.availableLFNsInT2[job.getCloud()][tmpFile.dataset]['sites'].has_key(tmpCandT2):
                                continue
                            # site has the file
                            if tmpFile.lfn in self.availableLFNsInT2[job.getCloud()][tmpFile.dataset]['sites'][tmpCandT2]:
                                if not tmpCandT2 in newCandT2s:
                                    newCandT2s.append(tmpCandT2)
                        # set new candidates
                        tmpCandT2s = newCandT2s
                        # no candidates left
                        if tmpCandT2s == []:
                            goToT2 = False
                            break
                # go to T2
                if goToT2 == None:
                    goToT2 = True
            # remove job not to process further
            if missingFlag and goToT2 != True:
                jobsProcessed.remove(job)
                # revert
                for oJob in self.jobs:
                    if oJob.PandaID == job.PandaID:
                        jobsWaiting.append(oJob)
                        break
            # get missing datasets
            if missingFlag:    
                if job.processingType.startswith('gangarobot') or \
                       job.processingType.startswith('hammercloud'):
                    pass
                elif not job.prodSourceLabel in ['managed']:
                    pass
                else:
                    for tmpFile in job.Files:
                        if tmpFile.type == 'input' and tmpFile.status == 'missing' and \
                               not tmpFile.dataset.startswith('ddo'):
                            # append
                            if not self.missingDatasetList.has_key(job.getCloud()):
                                self.missingDatasetList[job.getCloud()] = {}
                            if not self.missingDatasetList[job.getCloud()].has_key(tmpFile.dataset):
                                self.missingDatasetList[job.getCloud()][tmpFile.dataset] = set()
                            if not tmpFile.lfn in self.missingDatasetList[job.getCloud()][tmpFile.dataset]:
                                self.missingDatasetList[job.getCloud()][tmpFile.dataset].add(tmpFile.lfn)
        # set data summary fields
        for tmpJob in self.jobs:
            try:
                # set only for production/analysis/test
                if not tmpJob.prodSourceLabel in ['managed','test','rc_test','ptest','user','prod_test']:
                    continue
                # loop over all files
                tmpJob.nInputDataFiles = 0
                tmpJob.inputFileBytes = 0
                tmpInputFileProject = None
                tmpInputFileType = None
                for tmpFile in tmpJob.Files:
                    # use input files and ignore DBR/lib.tgz
                    if tmpFile.type == 'input' and (not tmpFile.dataset.startswith('ddo')) \
                       and not tmpFile.lfn.endswith('.lib.tgz'):
                        tmpJob.nInputDataFiles += 1
                        if not tmpFile.fsize in ['NULL',None,0,'0']:
                            tmpJob.inputFileBytes += tmpFile.fsize
                        # get input type and project
                        if tmpInputFileProject == None:
                            tmpInputItems = tmpFile.dataset.split('.')
                            # input project
                            tmpInputFileProject = tmpInputItems[0].split(':')[-1]
                            # input type. ignore user/group/groupXY 
                            if len(tmpInputItems) > 4 and (not tmpInputItems[0] in ['','NULL','user','group']) \
                                   and (not tmpInputItems[0].startswith('group')) \
                                   and not tmpFile.dataset.startswith('panda.um.'):
                                tmpInputFileType = tmpInputItems[4]
                # set input type and project
                if not tmpJob.prodDBlock in ['',None,'NULL']:
                    # input project
                    if tmpInputFileProject != None:
                        tmpJob.inputFileProject = tmpInputFileProject
                    # input type
                    if tmpInputFileType != None:
                        tmpJob.inputFileType = tmpInputFileType
                # protection
                maxInputFileBytes = 99999999999
                if tmpJob.inputFileBytes > maxInputFileBytes:
                    tmpJob.inputFileBytes = maxInputFileBytes
                # set background-able flag
                tmpJob.setBackgroundableFlag()
            except:
                errType,errValue = sys.exc_info()[:2]
                self.logger.error("failed to set data summary fields for PandaID=%s: %s %s" % (tmpJob.PandaID,errType,errValue))
        # send jobs to jobsWaiting
        self.taskBuffer.keepJobs(jobsWaiting)
        # update failed job
        self.updateFailedJobs(jobsFailed)
        # remove waiting/failed jobs
        self.jobs = jobsProcessed
        # delete huge variables
        del lfnMap
        del valMap
        del prodError
        del jobsWaiting
        del jobsProcessed
        del allLFNs
        del allGUIDs
        del cloudMap
        del missLFNs


    # remove waiting jobs
    def removeWaitingJobs(self):
        jobsWaiting   = []
        jobsProcessed = []
        for tmpJob in self.jobs:
            if tmpJob.jobStatus == 'waiting':
                jobsWaiting.append(tmpJob)
            else:
                jobsProcessed.append(tmpJob)
        # send jobs to jobsWaiting
        self.taskBuffer.keepJobs(jobsWaiting)
        # remove waiting/failed jobs
        self.jobs = jobsProcessed
        

    # memory checker
    def _memoryCheck(self):
        try:
            import os
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
            self.logger.debug('MemCheck PID=%s Name=%s VSZ=%s RSS=%s' % (os.getpid(),name,vmSize,vmRSS))
        except:
            errtype,errvalue = sys.exc_info()[:2]
            self.logger.error("memoryCheck() : %s %s" % (errtype,errvalue))
            self.logger.debug('MemCheck PID=%s unknown' % os.getpid())
            return


    # get list of files in dataset
    def getListFilesInDataset(self,dataset,fileList=None,useCache=True):
        # use cache data
        if useCache and self.lfnDatasetMap.has_key(dataset):
            return 0,self.lfnDatasetMap[dataset]
        for iDDMTry in range(3):
            try:
                self.logger.debug('listFilesInDataset '+dataset)
                items,tmpDummy = rucioAPI.listFilesInDataset(dataset,fileList=fileList)
                status = 0
                break
            except DataIdentifierNotFound:
                status = -1
                break
            except:
                status = -2
        if status != 0:
            errType,errValue = sys.exc_info()[:2]
            out = '{0} {1}'.format(errType,errValue)
            return status,out
        # keep to avoid redundant lookup
        self.lfnDatasetMap[dataset] = items
        return status,items

        
    # get list of datasets in container
    def getListDatasetInContainer(self,container):
        # get datasets in container
        self.logger.debug('listDatasetsInContainer '+container)
        for iDDMTry in range(3):
            datasets,out = rucioAPI.listDatasetsInContainer(container)
            if datasets is None:
                time.sleep(10)
            else:
                break
        if datasets is None:
            self.logger.error(out)
            return False,out
        return True,datasets

        
    def getListDatasetReplicasInContainer(self,container,getMap=False):
        # get datasets in container
        self.logger.debug('listDatasetsInContainer '+container)
        for iDDMTry in range(3):
            datasets,out = rucioAPI.listDatasetsInContainer(container)
            if datasets is None:
                time.sleep(10)
            else:
                break
        if datasets is None:
            self.logger.error(out)
            return 1,out
        # loop over all datasets
        allRepMap = {}
        for dataset in datasets:
            self.logger.debug('listDatasetReplicas '+dataset)
            status,out = self.getListDatasetReplicas(dataset,False)
            self.logger.debug(out)
            if status != 0 or out.startswith('Error'):
                return status,out
            tmpRepSites = {}
            try:
                # convert res to map
                exec "tmpRepSites = %s" % out
            except:
                return status,out
            # get map
            if getMap:
                allRepMap[dataset] = tmpRepSites
                continue
            # otherwise get sum    
            for siteId,statList in tmpRepSites.iteritems():
                if not allRepMap.has_key(siteId):
                    # append
                    allRepMap[siteId] = [statList[-1],]
                else:
                    # add
                    newStMap = {}
                    for stName,stNum in allRepMap[siteId][0].iteritems():
                        if statList[-1].has_key(stName):
                            # try mainly for archived=None
                            try:
                                newStMap[stName] = stNum + statList[-1][stName]
                            except:
                                newStMap[stName] = stNum
                        else:
                            newStMap[stName] = stNum
                    allRepMap[siteId] = [newStMap,]
        # return
        self.logger.debug(str(allRepMap))
        if not getMap:
            return 0,str(allRepMap)
        else:
            return 0,allRepMap


    # get list of replicas for a dataset
    def getListDatasetReplicas(self,dataset,getMap=True):
        nTry = 3
        for iDDMTry in range(nTry):
            self.logger.debug("%s/%s listDatasetReplicas %s" % (iDDMTry,nTry,dataset))
            status,out = rucioAPI.listDatasetReplicas(dataset)
            if status != 0:
                time.sleep(10)
            else:
                break
        # result    
        if status != 0:
            self.logger.error(out)
            self.logger.error('bad response for %s' % dataset)
            if getMap:
                return False,{}
            else:
                return 1,str({})
        try:
            retMap = out
            self.logger.debug('getListDatasetReplicas->%s' % str(retMap))
            if getMap:
                return True,retMap
            else:
                return 0,str(retMap)
        except:
            self.logger.error(out)            
            self.logger.error('could not convert HTTP-res to replica map for %s' % dataset)
            if getMap:
                return False,{}
            else:
                return 1,str({})


    # dynamic data placement for analysis jobs
    def _dynamicDataPlacement(self):
        # only first submission
        if not self.firstSubmission:
            return
        # no jobs
        if len(self.jobs) == 0:
            return
        # only successful analysis
        if self.jobs[0].jobStatus in ['failed','cancelled'] or self.jobs[0].isCancelled() \
                or (not self.jobs[0].prodSourceLabel in ['user','panda']):
            return
        # disable for JEDI
        if self.jobs[0].lockedby == 'jedi':
            return
        # execute
        self.logger.debug('execute PD2P')
        from DynDataDistributer import DynDataDistributer
        ddd = DynDataDistributer(self.jobs,self.taskBuffer,self.siteMapper)
        ddd.run()
        self.logger.debug('finished PD2P')
        return


    # make dis datasets for existing files to avoid deletion when jobs are queued
    def _makeDisDatasetsForExistingfiles(self):
        self.logger.debug('make dis datasets for existing files')
        # collect existing files
        dsFileMap = {}
        nMaxJobs  = 20
        nJobsMap  = {}
        for tmpJob in self.jobs:
            # use production or test jobs only
            if not tmpJob.prodSourceLabel in ['managed','test']:
                continue
            # skip for prefetcher
            if tmpJob.usePrefetcher():
                continue
            # ignore inappropriate status
            if tmpJob.jobStatus in ['failed','cancelled','waiting'] or tmpJob.isCancelled():
                continue
            # check cloud
            if (tmpJob.getCloud() == 'ND' and self.siteMapper.getSite(tmpJob.computingSite).cloud == 'ND'):
                continue
            # check SE to use T2 only
            tmpSrcID = self.siteMapper.getCloud(tmpJob.getCloud())['source']
            srcSiteSpec = self.siteMapper.getSite(tmpSrcID)
            dstSiteSpec = self.siteMapper.getSite(tmpJob.computingSite)
            if dstSiteSpec.ddm_endpoints_input.isAssociated(srcSiteSpec.ddm_input):
                continue
            # look for log _sub dataset to be used as a key
            logSubDsName = ''
            for tmpFile in tmpJob.Files:
                if tmpFile.type == 'log':
                    logSubDsName = tmpFile.destinationDBlock
                    break
            # append site
            destDQ2ID = self.siteMapper.getSite(tmpJob.computingSite).ddm_input
            # T1 used as T2
            if tmpJob.getCloud() != self.siteMapper.getSite(tmpJob.computingSite).cloud and \
               not destDQ2ID.endswith('PRODDISK') and \
               self.siteMapper.getSite(tmpJob.computingSite).cloud in ['US']:
                tmpSeTokens = self.siteMapper.getSite(tmpJob.computingSite).setokens_input
                if tmpSeTokens.has_key('ATLASPRODDISK'):
                    destDQ2ID = tmpSeTokens['ATLASPRODDISK']
            # backend
            ddmBackEnd = 'rucio'
            mapKeyJob = (destDQ2ID,logSubDsName)
            # increment the number of jobs per key
            if not nJobsMap.has_key(mapKeyJob):
                nJobsMap[mapKeyJob] = 0
            mapKey = (destDQ2ID,logSubDsName,nJobsMap[mapKeyJob]/nMaxJobs,ddmBackEnd)
            nJobsMap[mapKeyJob] += 1
            if not dsFileMap.has_key(mapKey):
                dsFileMap[mapKey] = {}
            # add files
            for tmpFile in tmpJob.Files:
                if tmpFile.type != 'input':
                    continue
                # if files are unavailable at the dest site normal dis datasets contain them
                # or files are cached
                if not tmpFile.status in ['ready']:
                    continue
                # if available at T2
                realDestDQ2ID = (destDQ2ID,)
                if self.availableLFNsInT2.has_key(tmpJob.getCloud()) and self.availableLFNsInT2[tmpJob.getCloud()].has_key(tmpFile.dataset) \
                   and self.availableLFNsInT2[tmpJob.getCloud()][tmpFile.dataset]['sites'].has_key(tmpJob.computingSite) \
                   and tmpFile.lfn in self.availableLFNsInT2[tmpJob.getCloud()][tmpFile.dataset]['sites'][tmpJob.computingSite]:
                    realDestDQ2ID = self.availableLFNsInT2[tmpJob.getCloud()][tmpFile.dataset]['siteDQ2IDs'][tmpJob.computingSite]
                    realDestDQ2ID = tuple(realDestDQ2ID)
                # append
                if not dsFileMap[mapKey].has_key(realDestDQ2ID):
                    dsFileMap[mapKey][realDestDQ2ID] = {'taskID':tmpJob.taskID,
                                                        'PandaID':tmpJob.PandaID,
                                                        'files':{}}
                if not dsFileMap[mapKey][realDestDQ2ID]['files'].has_key(tmpFile.lfn):
                    # add scope
                    if ddmBackEnd != 'rucio':
                        tmpLFN = tmpFile.lfn
                    else:
                        tmpLFN = '{0}:{1}'.format(tmpFile.scope,tmpFile.lfn)
                    dsFileMap[mapKey][realDestDQ2ID]['files'][tmpFile.lfn] = {'lfn' :tmpLFN,
                                                                              'guid':tmpFile.GUID,
                                                                              'fileSpecs':[]}
                # add file spec
                dsFileMap[mapKey][realDestDQ2ID]['files'][tmpFile.lfn]['fileSpecs'].append(tmpFile)
        # loop over all locations
        dispList = []
        for tmpMapKey,tmpDumVal in dsFileMap.iteritems():
            tmpDumLocation,tmpLogSubDsName,tmpBunchIdx,tmpDdmBackEnd = tmpMapKey
            for tmpLocationList,tmpVal in tmpDumVal.iteritems():
                for tmpLocation in tmpLocationList:
                    tmpFileList = tmpVal['files']
                    if tmpFileList == {}:
                        continue
                    nMaxFiles = 500
                    iFiles = 0
                    iLoop = 0
                    while iFiles < len(tmpFileList):
                        subFileNames = tmpFileList.keys()[iFiles:iFiles+nMaxFiles]
                        if len(subFileNames) == 0:
                            break
                        # dis name
                        disDBlock = "panda.%s.%s.%s.%s_dis0%s%s" % (tmpVal['taskID'],time.strftime('%m.%d'),'GEN',
                                                                    commands.getoutput('uuidgen'),iLoop,
                                                                    tmpVal['PandaID'])
                        iFiles += nMaxFiles
                        lfns    = []
                        guids   = []
                        fsizes  = []
                        chksums = []
                        for tmpSubFileName in subFileNames:
                            lfns.append(tmpFileList[tmpSubFileName]['lfn'])
                            guids.append(tmpFileList[tmpSubFileName]['guid'])
                            fsizes.append(long(tmpFileList[tmpSubFileName]['fileSpecs'][0].fsize))
                            chksums.append(tmpFileList[tmpSubFileName]['fileSpecs'][0].checksum)
                            # set dis name
                            for tmpFileSpec in tmpFileList[tmpSubFileName]['fileSpecs']:
                                if tmpFileSpec.status in ['ready'] and tmpFileSpec.dispatchDBlock == 'NULL':
                                    tmpFileSpec.dispatchDBlock = disDBlock
                        # register datasets
                        iLoop += 1
                        nDDMTry = 3
                        isOK = False
                        metadata = {'hidden':True,
                                    'purge_replicas': 0}
                        if not tmpVal['taskID'] in [None,'NULL']:
                            metadata['task_id'] = str(tmpVal['taskID'])
                        tmpMsg = 'ext registerNewDataset {ds} {lfns} {guids} {fsizes} {chksums} {meta}'
                        self.logger.debug(tmpMsg.format(ds=disDBlock,
                                                        lfns=str(lfns),
                                                        guids=str(guids),
                                                        fsizes=str(fsizes),
                                                        chksums=str(chksums),
                                                        meta=str(metadata)
                                                        ))
                        for iDDMTry in range(nDDMTry):
                            try:
                                out = rucioAPI.registerDataset(disDBlock,lfns,guids,fsizes,chksums,
                                                               lifetime=7,scope='panda',metadata=metadata)
                                self.logger.debug(out)
                                isOK = True
                                break
                            except:
                                errType,errValue = sys.exc_info()[:2]
                                self.logger.error("ext registerDataset : failed with {0}:{1}".format(errType,errValue)+traceback.format_exc())
                                if iDDMTry+1 == nDDMTry:
                                    break
                                self.logger.debug("sleep {0}/{1}".format(iDDMTry,nDDMTry))
                                time.sleep(10)
                        # failure
                        if not isOK:
                            continue
                        # get VUID
                        try:
                            exec "vuid = %s['vuid']" % str(out)
                            # dataset spec. currentfiles is used to count the number of failed jobs
                            ds = DatasetSpec()
                            ds.vuid = vuid
                            ds.name = disDBlock
                            ds.type = 'dispatch'
                            ds.status = 'defined'
                            ds.numberfiles  = len(lfns)
                            ds.currentfiles = 0
                            dispList.append(ds)
                        except:
                            errType,errValue = sys.exc_info()[:2]
                            self.logger.error("ext registerNewDataset : failed to decode VUID for %s - %s %s" % (disDBlock,errType,errValue))
                            continue
                        # freezeDataset dispatch dataset
                        self.logger.debug('freezeDataset '+disDBlock)
                        for iDDMTry in range(3):
                            status = False
                            try:
                                rucioAPI.closeDataset(disDBlock)
                                status = True
                                break
                            except:
                                errtype,errvalue = sys.exc_info()[:2]
                                out = 'failed to close : {0} {1}'.format(errtype,errvalue)
                                time.sleep(10)
                        if not status:
                            self.logger.error(out)
                            continue
                        # register location
                        isOK = False
                        self.logger.debug('ext registerDatasetLocation {ds} {dq2ID} {lifeTime}days asynchronous=True'.format(ds=disDBlock,
                                                                                                                             dq2ID=tmpLocation,
                                                                                                                             lifeTime=7,
                                                                                                                             ))
                        nDDMTry = 3
                        for iDDMTry in range(nDDMTry):
                            try:
                                out = rucioAPI.registerDatasetLocation(disDBlock,[tmpLocation],7,
                                                                       activity='Production Input',
                                                                       scope='panda',asynchronous=True,
                                                                       grouping='NONE')
                                self.logger.debug(out)
                                isOK = True
                                break
                            except:
                                errType,errValue = sys.exc_info()[:2]
                                self.logger.error("ext registerDatasetLocation : failed with {0}:{1}".format(errType,errValue))
                                if iDDMTry+1 == nDDMTry:
                                    break
                                self.logger.debug("sleep {0}/{1}".format(iDDMTry,nDDMTry))
                                time.sleep(10)
                                
                        # failure
                        if not isOK:
                            continue
        # insert datasets to DB
        self.taskBuffer.insertDatasets(dispList)
        self.logger.debug('finished to make dis datasets for existing files')
        return


    # pin input dataset 
    def _pinInputDatasets(self):
        self.logger.debug('pin input datasets')
        # collect input datasets and locations
        doneList = []
        allReplicaMap = {}
        useShortLivedReplicasFlag = False 
        for tmpJob in self.jobs:
            # ignore HC jobs
            if tmpJob.processingType.startswith('gangarobot') or \
               tmpJob.processingType.startswith('hammercloud'):
                continue
            # not pin if --useShortLivedReplicas was used
            if tmpJob.metadata != None and '--useShortLivedReplicas' in tmpJob.metadata:
                if not useShortLivedReplicasFlag:
                    self.logger.debug('   skip pin due to --useShortLivedReplicas')
                    useShortLivedReplicasFlag = True
                continue
            # use production or test or user jobs only
            if not tmpJob.prodSourceLabel in ['managed','test','user']:
                continue
            # ignore inappropriate status
            if tmpJob.jobStatus in ['failed','cancelled','waiting'] or tmpJob.isCancelled():
                continue
            # set lifetime
            if tmpJob.prodSourceLabel in ['managed','test']:
                pinLifeTime = 7
            else:
                pinLifeTime = 7
            # get source
            if tmpJob.prodSourceLabel in ['managed','test']:
                tmpSrcID = self.siteMapper.getCloud(tmpJob.getCloud())['source']
                srcDQ2ID = self.siteMapper.getSite(tmpSrcID).ddm_input
            else:
                srcDQ2ID = self.siteMapper.getSite(tmpJob.computingSite).ddm_input
            # prefix of DQ2 ID
            srcDQ2IDprefix = re.sub('_[A-Z,0-9]+DISK$','',srcDQ2ID)
            # loop over all files
            for tmpFile in tmpJob.Files:
                # use input files and ignore DBR/lib.tgz
                if tmpFile.type == 'input' and \
                       not tmpFile.lfn.endswith('.lib.tgz') and \
                       not tmpFile.dataset.startswith('ddo') and \
                       not tmpFile.dataset.startswith('user') and \
                       not tmpFile.dataset.startswith('group'):
                    # ignore pre-merged datasets
                    if tmpFile.dataset.startswith('panda.um.'):
                        continue
                    # get replica locations
                    if not allReplicaMap.has_key(tmpFile.dataset):
                        if tmpFile.dataset.endswith('/'):
                            status,tmpRepSitesMap = self.getListDatasetReplicasInContainer(tmpFile.dataset,getMap=True)
                            if status == 0:
                                status = True
                            else:
                                status = False
                        else:
                            status,tmpRepSites = self.getListDatasetReplicas(tmpFile.dataset)
                            tmpRepSitesMap = {}
                            tmpRepSitesMap[tmpFile.dataset] = tmpRepSites
                        # append    
                        if status:
                            allReplicaMap[tmpFile.dataset] = tmpRepSitesMap
                        else:
                            # set empty to avoid further lookup
                            allReplicaMap[tmpFile.dataset] = {}
                        # loop over constituent datasets
                        self.logger.debug('pin DQ2 prefix=%s' % srcDQ2IDprefix)
                        for tmpDsName,tmpRepSitesMap in allReplicaMap[tmpFile.dataset].iteritems():
                            # loop over locations                        
                            for tmpRepSite in tmpRepSitesMap.keys():
                                if tmpRepSite.startswith(srcDQ2IDprefix) \
                                       and not 'TAPE' in tmpRepSite \
                                       and not '_LOCALGROUP' in tmpRepSite \
                                       and not '_DAQ' in tmpRepSite \
                                       and not '_TZERO' in tmpRepSite \
                                       and not '_USERDISK' in tmpRepSite \
                                       and not '_RAW' in tmpRepSite \
                                       and not 'SCRATCH' in tmpRepSite:
                                    tmpKey = (tmpDsName,tmpRepSite)
                                    # already done
                                    if tmpKey in doneList:
                                        continue
                                    # append to avoid repetition
                                    doneList.append(tmpKey)
                                    # set pin lifetime
                                    #status = self.setReplicaMetadata(tmpDsName,tmpRepSite,'pin_lifetime','%s days' % pinLifeTime)
        # retrun                    
        self.logger.debug('pin input datasets done')
        return


    # make T1 subscription for missing files
    def _makeSubscriptionForMissing(self):
        self.logger.debug('make subscriptions for missing files')
        # collect datasets
        missingList = {}
        for tmpCloud,tmpMissDatasets in self.missingDatasetList.iteritems():
            # append cloud
            if not missingList.has_key(tmpCloud):
                missingList[tmpCloud] = []
            # loop over all datasets    
            for tmpDsName,tmpMissFiles in tmpMissDatasets.iteritems():
                # check if datasets in container are used 
                if tmpDsName.endswith('/'):
                    # convert container to datasets
                    tmpStat,tmpDsList = self.getListDatasetInContainer(tmpDsName)
                    if not tmpStat:
                        self.logger.error('failed to get datasets in container:%s' % tmpDsName)
                        continue
                    # check if each dataset is actually used
                    for tmpConstDsName in tmpDsList:
                        # skip if already checked
                        if tmpDsName in missingList[tmpCloud]:
                            continue
                        # get files in each dataset
                        tmpStat,tmpFilesInDs = self.getListFilesInDataset(tmpConstDsName)
                        if not tmpStat:
                            self.logger.error('failed to get files in dataset:%s' % tmpConstDsName)
                            continue
                        # loop over all files to check the dataset is used
                        for tmpLFN in tmpMissFiles:
                            # append if used
                            if tmpLFN in tmpFilesInDs:
                                missingList[tmpCloud].append(tmpConstDsName)
                                break
                else:
                    # append dataset w/o checking
                    if not tmpDsName in missingList[tmpCloud]:
                        missingList[tmpCloud].append(tmpDsName)
        # make subscriptions
        for tmpCloud,missDsNameList in missingList.iteritems():
            # get distination
            tmpDstID = self.siteMapper.getCloud(tmpCloud)['source']
            dstDQ2ID = self.siteMapper.getSite(tmpDstID).ddm_input # TODO: check with Tadashi
            # register subscription
            for missDsName in missDsNameList:
                self.logger.debug('make subscription at %s for missing %s' % (dstDQ2ID,missDsName))
                self.makeSubscription(missDsName,dstDQ2ID)
        # retrun
        self.logger.debug('make subscriptions for missing files done')
        return
    

    
    # make subscription
    def makeSubscription(self,dataset,dq2ID):
        # return for failuer
        retFailed = False
        self.logger.debug('registerDatasetSubscription %s %s' % (dataset,dq2ID))
        nTry = 3
        for iDDMTry in range(nTry):
            try:
                # register subscription
                status = rucioAPI.registerDatasetSubscription(dataset,[dq2ID],
                                                              activity='Production Input')
                out = 'OK'
                break
            except:
                status = False
                errType,errValue = sys.exc_info()[:2]
                out = "%s %s" % (errType,errValue)
                time.sleep(10)
        # result
        if not status:
            self.logger.error(out)
            return retFailed
        # update 
        self.logger.debug('%s %s' % (status,out))
        # return
        return True

    
    # send task brokerage message to logger
    def sendTaMesg(self,message,msgType=None):
        try:
            # get logger
            tmpPandaLogger = PandaLogger()
            # lock HTTP handler
            tmpPandaLogger.lock()
            tmpPandaLogger.setParams({'Type':'taskbrokerage'})
            # use bamboo for loggername
            if panda_config.loggername == 'prod':
                tmpLogger = tmpPandaLogger.getHttpLogger('bamboo')
            else:
                # for dev
                tmpLogger = tmpPandaLogger.getHttpLogger(panda_config.loggername)
            # add message
            if msgType=='error':
                tmpLogger.error(message)
            elif msgType=='warning':
                tmpLogger.warning(message)
            elif msgType=='info':
                tmpLogger.info(message)
            else:
                tmpLogger.debug(message)                
            # release HTTP handler
            tmpPandaLogger.release()
        except:
            pass
        time.sleep(1)


    # setup jumbo jobs
    def _setupJumbojobs(self):
        if len(self.jumboJobs) == 0:
            return
        self.logger.debug('setup jumbo jobs')
        # get files in datasets
        dsLFNsMap = {}
        failedDS  = set()
        for jumboJobSpec in self.jumboJobs:
            for tmpFileSpec in jumboJobSpec.Files:
                # only input
                if not tmpFileSpec.type in ['input']:
                    continue
                # get files
                if not tmpFileSpec.dataset in dsLFNsMap:
                    if not tmpFileSpec.dataset in failedDS:
                        tmpStat,tmpMap = self.getListFilesInDataset(tmpFileSpec.dataset,
                                                                    useCache=False)
                        # failed
                        if tmpStat != 0:
                            failedDS.add(tmpFileSpec.dataset)
                            self.logger.debug('failed to get files in {0} with {1}'.format(tmpFileSpec.dataset,
                                                                                           tmpMap))
                        else:
                            # append
                            dsLFNsMap[tmpFileSpec.dataset] = tmpMap
                # set failed if file lookup failed
                if tmpFileSpec.dataset in failedDS:
                    jumboJobSpec.jobStatus    = 'failed'
                    jumboJobSpec.ddmErrorCode = ErrorCode.EC_GUID
                    jumboJobSpec.ddmErrorDiag = 'failed to get files in {0}'.format(tmpFileSpec.dataset)
                    break
        # make dis datasets
        okJobs = []
        ngJobs = []
        for jumboJobSpec in self.jumboJobs:
            # skip failed
            if jumboJobSpec.jobStatus == 'failed':
                ngJobs.append(jumboJobSpec)
                continue
            # get datatype
            try:
                tmpDataType = jumboJobSpec.prodDBlock.split('.')[-2]
                if len(tmpDataType) > 20:
                    raise RuntimeError,''
            except:
                # default
                tmpDataType = 'GEN'
            # make dis dataset name 
            dispatchDBlock = "panda.%s.%s.%s.%s_dis%s" % (jumboJobSpec.taskID,time.strftime('%m.%d'),tmpDataType,
                                                          'jumbo',jumboJobSpec.PandaID)
            # collect file attributes
            lfns = []
            guids = []
            sizes = []
            checksums = []
            for tmpFileSpec in jumboJobSpec.Files:
                # only input
                if not tmpFileSpec.type in ['input']:
                    continue
                for tmpLFN,tmpVar in dsLFNsMap[tmpFileSpec.dataset].iteritems():
                    tmpLFN = '{0}:{1}'.format(tmpVar['scope'],tmpLFN)
                    lfns.append(tmpLFN)
                    guids.append(tmpVar['guid'])
                    sizes.append(tmpVar['fsize'])
                    checksums.append(tmpVar['chksum'])
                # set dis dataset
                tmpFileSpec.dispatchDBlock = dispatchDBlock
            # register and subscribe dis dataset
            if len(lfns) != 0:
                # set dis dataset
                jumboJobSpec.dispatchDBlock = dispatchDBlock
                # register dis dataset
                try:
                    self.logger.debug('registering jumbo dis dataset {0} with {1} files'.format(dispatchDBlock,
                                                                                                len(lfns)))
                    out = rucioAPI.registerDataset(dispatchDBlock,lfns,guids,sizes,
                                                   checksums,lifetime=14)
                    vuid = out['vuid']
                except:
                    errType,errValue = sys.exc_info()[:2]
                    self.logger.debug('failed to register jumbo dis dataset {0} with {1}:{2}'.format(dispatchDBlock,
                                                                                                     errType,errValue))
                    jumboJobSpec.jobStatus    = 'failed'
                    jumboJobSpec.ddmErrorCode = ErrorCode.EC_Setupper
                    jumboJobSpec.ddmErrorDiag = 'failed to register jumbo dispatch dataset {0}'.format(dispatchDBlock)
                    ngJobs.append(jumboJobSpec)
                    continue
                # subscribe dis dataset
                try:
                    endPoint = self.siteMapper.getSite(jumboJobSpec.computingSite).ddm_input
                    self.logger.debug('subscribing jumbo dis dataset {0} to {1}'.format(dispatchDBlock,endPoint))
                    rucioAPI.registerDatasetLocation(dispatchDBlock,[endPoint],lifetime=14,activity='Production Input')
                except:
                    errType,errValue = sys.exc_info()[:2]
                    self.logger.debug('failed to subscribe jumbo dis dataset {0} to {1} with {2}:{3}'.format(dispatchDBlock,
                                                                                                             endPoint,
                                                                                                             errType,errValue))
                    jumboJobSpec.jobStatus    = 'failed'
                    jumboJobSpec.ddmErrorCode = ErrorCode.EC_Setupper
                    jumboJobSpec.ddmErrorDiag = 'failed to subscribe jumbo dispatch dataset {0} to {1}'.format(dispatchDBlock,
                                                                                                               endPoint)
                    ngJobs.append(jumboJobSpec)
                    continue
                # add dataset in DB
                ds = DatasetSpec()
                ds.vuid = vuid
                ds.name = dispatchDBlock
                ds.type = 'dispatch'
                ds.status = 'defined'
                ds.numberfiles  = len(lfns)
                ds.currentfiles = 0
                self.taskBuffer.insertDatasets([ds])
            # set destination
            jumboJobSpec.destinationSE = jumboJobSpec.computingSite
            for tmpFileSpec in jumboJobSpec.Files:
                if tmpFileSpec.type in ['output','log'] and \
                        DataServiceUtils.getDistributedDestination(tmpFileSpec.destinationDBlockToken) == None:
                    tmpFileSpec.destinationSE = jumboJobSpec.computingSite
            okJobs.append(jumboJobSpec)
        # update failed jobs
        self.updateFailedJobs(ngJobs)
        self.jumboJobs = okJobs
        self.logger.debug('done for jumbo jobs')
        return
