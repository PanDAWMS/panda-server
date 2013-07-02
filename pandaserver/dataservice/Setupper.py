'''
setup dataset

'''

import re
import sys
import time
import types
import urllib
import datetime
import commands
import threading
import traceback
import ErrorCode
import TaskAssigner
from DDM import ddm
from dataservice.DDM import dq2Common
from taskbuffer.JobSpec import JobSpec
from taskbuffer.FileSpec import FileSpec
from taskbuffer.DatasetSpec import DatasetSpec
from brokerage.SiteMapper import SiteMapper
from brokerage.PandaSiteIDs import PandaMoverIDs
import brokerage.broker
import brokerage.broker_util
import DataServiceUtils


from config import panda_config
from pandalogger.PandaLogger import PandaLogger

# logger
_logger = PandaLogger().getLogger('Setupper')


# temporary
PandaDDMSource = ['BNLPANDA','BNL-OSG2_MCDISK','BNL-OSG2_DATADISK','BNL-OSG2_MCTAPE','BNL-OSG2_DATATAPE']


class Setupper (threading.Thread):
    # constructor
    def __init__(self,taskBuffer,jobs,resubmit=False,pandaDDM=False,ddmAttempt=0,forkRun=False,onlyTA=False,
                 resetLocation=False,useNativeDQ2=True):
        threading.Thread.__init__(self)
        self.jobs       = jobs
        self.taskBuffer = taskBuffer
        # VUIDs of dispatchDBlocks
        self.vuidMap = {}
        # resubmission or not
        self.resubmit = resubmit
        # time stamp
        self.timestamp = datetime.datetime.utcnow().isoformat(' ')
        # use PandaDDM
        self.pandaDDM = pandaDDM
        # file list for dispDS for PandaDDM
        self.dispFileList = {}
        # priority for ddm job
        self.ddmAttempt = ddmAttempt
        # site mapper
        self.siteMapper = None
        # fork another process because python doesn't release memory
        self.forkRun = forkRun
        # run task assignment only
        self.onlyTA = onlyTA
        # location map
        self.replicaMap  = {}
        # all replica locations
        self.allReplicaMap = {}
        # reset locations
        self.resetLocation = resetLocation
        # replica map for special brokerage
        self.replicaMapForBroker = {}
        # available files at T2
        self.availableLFNsInT2 = {}
        # use DQ2 in the same process
        self.useNativeDQ2 = useNativeDQ2
        # list of missing datasets
        self.missingDatasetList = {}
        # lfn ds map
        self.lfnDatasetMap = {}
        
        
    # main
    def run(self):
        try:
            _logger.debug('%s startRun' % self.timestamp)
            self._memoryCheck()
            # run main procedure in the same process
            if not self.forkRun:
                if self.jobs != None and len(self.jobs) > 0:
                    _logger.debug('%s PandaID:%s type:%s taskID:%s' % (self.timestamp,
                                                             self.jobs[0].PandaID,
                                                             self.jobs[0].prodSourceLabel,
                                                             self.jobs[0].taskID))
                # instantiate site mapper
                self.siteMapper = SiteMapper(self.taskBuffer)
                # use native DQ2
                if self.useNativeDQ2:
                    ddm.useDirectDQ2()
                # correctLFN
                self._correctLFN()
                # run full Setupper
                if not self.onlyTA:
                    # invoke brokerage
                    _logger.debug('%s brokerSchedule' % self.timestamp)        
                    brokerage.broker.schedule(self.jobs,self.taskBuffer,self.siteMapper,
                                              replicaMap=self.replicaMapForBroker,
                                              t2FilesMap=self.availableLFNsInT2)
                    # remove waiting jobs
                    self.removeWaitingJobs()
                    # setup dispatch dataset
                    _logger.debug('%s setupSource' % self.timestamp)        
                    self._setupSource()
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
                    if self.jobs != [] and self.jobs[0].prodSourceLabel in ['managed','test'] and self.jobs[0].cloud in ['DE']:
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
                    self._makeDisDatasetsForExistingfiles()
                    # update jobs
                    _logger.debug('%s updateJobs' % self.timestamp)        
                    self._updateJobs()
                    # then subscribe sites distpatchDBlocks. this must be the last method
                    _logger.debug('%s subscribeDistpatchDB' % self.timestamp)        
                    self._subscribeDistpatchDB()
                    # dynamic data placement for analysis jobs
                    self._dynamicDataPlacement()
                    # pin input datasets
                    self._pinInputDatasets()
                    # make subscription for missing
                    self._makeSubscriptionForMissing()
            else:
                # write jobs to file
                import os
                import cPickle as pickle
                outFileName = '%s/set.%s_%s' % (panda_config.logdir,self.jobs[0].PandaID,commands.getoutput('uuidgen'))
                outFile = open(outFileName,'w')
                pickle.dump(self.jobs,outFile)
                outFile.close()
                # run main procedure in another process because python doesn't release memory
                com =  'cd %s > /dev/null 2>&1; export HOME=%s; ' % (panda_config.home_dir_cwd,panda_config.home_dir_cwd)
                com += 'source /opt/glite/etc/profile.d/grid-env.sh; '
                com += 'env PYTHONPATH=%s:%s %s/python -Wignore %s/dataservice/forkSetupper.py -i %s' % \
                       (panda_config.pandaCommon_dir,panda_config.pandaPython_dir,panda_config.native_python,
                        panda_config.pandaPython_dir,outFileName)
                if self.onlyTA:
                    com += " -t"
                _logger.debug(com)
                # exeute
                status,output = self.taskBuffer.processLimiter.getstatusoutput(com)
                _logger.debug("Ret from another process: %s %s" % (status,output))                
            self._memoryCheck()            
            _logger.debug('%s endRun' % self.timestamp)
        except:
            type, value, traceBack = sys.exc_info()
            _logger.error("%s run() : %s %s" % (self.timestamp,type,value))
        

    # make dipatchDBlocks, insert prod/dispatchDBlock to database
    def _setupSource(self):
        fileList    = {}
        prodList    = []
        prodError   = {}
        dispSiteMap = {}
        dispError   = {}
        # extract prodDBlock
        for job in self.jobs:
            # ignore failed jobs
            if job.jobStatus in ['failed','cancelled']:
                continue
            # production datablock
            if job.prodDBlock != 'NULL' and (not self.pandaDDM) and (not job.prodSourceLabel in ['user','panda']):
                # get VUID and record prodDBlock into DB
                if not prodError.has_key(job.prodDBlock):
                    time.sleep(1)
                    _logger.debug((self.timestamp,'queryDatasetByName',job.prodDBlock))
                    prodError[job.prodDBlock] = ''
                    for iDDMTry in range(3):
                        status,out = ddm.repositoryClient.main('queryDatasetByName',job.prodDBlock)
                        if status != 0 or out.find("DQ2 internal server exception") != -1 \
                               or out.find("An error occurred on the central catalogs") != -1 \
                               or out.find("MySQL server has gone away") != -1:
                            time.sleep(60)
                        else:
                            break
                    _logger.debug("%s %s" % (self.timestamp,out))                        
                    if status != 0 or out.find('Error') != -1:
                        prodError[job.prodDBlock] = "Setupper._setupSource() could not get VUID of prodDBlock"
                        _logger.error(out)                                            
                    else:
                        try:
                            exec "vuids = %s['%s']['vuids']" % (out.split('\n')[0],job.prodDBlock)
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
                            type, value, traceBack = sys.exc_info()
                            _logger.error("_setupSource() : %s %s" % (type,value))
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
                if self.siteMapper.checkCloud(job.cloud):
                    # use cloud's source
                    tmpSrcID = self.siteMapper.getCloud(job.cloud)['source']
                srcDQ2ID = self.siteMapper.getSite(tmpSrcID).ddm
                # use srcDQ2ID as dstDQ2ID when dst SE is same as src SE
                srcSEs = brokerage.broker_util.getSEfromSched(self.siteMapper.getSite(tmpSrcID).se)
                dstSEs = brokerage.broker_util.getSEfromSched(self.siteMapper.getSite(job.computingSite).se)
                if srcSEs == dstSEs:
                    dstDQ2ID = srcDQ2ID
                else:
                    dstDQ2ID = self.siteMapper.getSite(job.computingSite).ddm
                dispSiteMap[job.dispatchDBlock] = {'src':srcDQ2ID,'dst':dstDQ2ID,'site':job.computingSite}
                # filelist
                if not fileList.has_key(job.dispatchDBlock):
                    fileList[job.dispatchDBlock] = {'lfns':[],'guids':[],'fsizes':[],'md5sums':[],'chksums':[]}
                # collect LFN and GUID
                for file in job.Files:
                    if file.type == 'input' and file.status == 'pending':
                        if not file.lfn in fileList[job.dispatchDBlock]['lfns']:
                            fileList[job.dispatchDBlock]['lfns'].append(file.lfn)
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
                                for iDDMTry in range(3):
                                    _logger.debug((self.timestamp,'listDatasetReplicas',file.dataset))
                                    status,out = ddm.DQ2.main('listDatasetReplicas',file.dataset,0,None,False)
                                    if status != 0 or out.find("DQ2 internal server exception") != -1 \
                                           or out.find("An error occurred on the central catalogs") != -1 \
                                           or out.find("MySQL server has gone away") != -1 \
                                           or out == '()':
                                        time.sleep(60)
                                    else:
                                        break
                            if status != 0 or out.startswith('Error'):
                                _logger.error("%s %s" % (self.timestamp,out))
                                dispError[job.dispatchDBlock] = 'could not get locations for %s' % file.dataset
                                _logger.error(dispError[job.dispatchDBlock])
                            else:
                                _logger.debug("%s %s" % (self.timestamp,out))
                                tmpRepSites = {}
                                try:
                                    # convert res to map
                                    exec "tmpRepSites = %s" % out
                                    self.allReplicaMap[file.dataset] = tmpRepSites
                                except:
                                    dispError[job.dispatchDBlock] = 'could not convert HTTP-res to replica map for %s' % file.dataset
                                    _logger.error(dispError[job.dispatchDBlock])
                                    _logger.error(out)
                        if self.allReplicaMap.has_key(file.dataset):
                            self.replicaMap[job.dispatchDBlock][file.dataset] = self.allReplicaMap[file.dataset]
        # register dispatch dataset
        dispList = []
        for dispatchDBlock in fileList.keys():
            # ignore empty dataset
            if len(fileList[dispatchDBlock]['lfns']) == 0:
                continue
            # use DQ2
            if (not self.pandaDDM) and (not dispSiteMap[dispatchDBlock]['src'] in PandaDDMSource or \
                                        self.siteMapper.getSite(dispSiteMap[dispatchDBlock]['site']).cloud != 'US') \
                   and (job.prodSourceLabel != 'ddm') and (not dispSiteMap[dispatchDBlock]['site'].endswith("_REPRO")):
                # register dispatch dataset
                disFiles = fileList[dispatchDBlock]
                _logger.debug((self.timestamp,'registerNewDataset',dispatchDBlock,disFiles['lfns'],disFiles['guids'],
                               disFiles['fsizes'],disFiles['chksums'],None,None,None,True))
                for iDDMTry in range(3):
                    status,out = ddm.DQ2.main('registerNewDataset',dispatchDBlock,disFiles['lfns'],disFiles['guids'],
                              disFiles['fsizes'],disFiles['chksums'],None,None,None,True)
                    if status != 0 and out.find('DQDatasetExistsException') != -1:
                        break
                    elif status != 0 or out.find("DQ2 internal server exception") != -1 \
                             or out.find("An error occurred on the central catalogs") != -1 \
                             or out.find("MySQL server has gone away") != -1:
                        _logger.debug("%s sleep %s for %s" % (self.timestamp,iDDMTry,dispatchDBlock))
                        _logger.debug(status)
                        _logger.debug(out)
                        _logger.debug("-------------")                                                                
                        time.sleep(60)
                    else:
                        break
                if status != 0 or out.find('Error') != -1:
                    _logger.error("%s %s" % (self.timestamp,out))                
                    dispError[dispatchDBlock] = "Setupper._setupSource() could not register dispatchDBlock"
                    continue
                _logger.debug("%s %s" % (self.timestamp,out))                
                vuidStr = out
                # freezeDataset dispatch dataset
                time.sleep(1)            
                _logger.debug((self.timestamp,'freezeDataset',dispatchDBlock))
                for iDDMTry in range(3):            
                    status,out = ddm.DQ2.main('freezeDataset',dispatchDBlock)
                    if status != 0 or out.find("DQ2 internal server exception") != -1 \
                           or out.find("An error occurred on the central catalogs") != -1 \
                           or out.find("MySQL server has gone away") != -1:
                        time.sleep(60)
                    else:
                        break
                if status != 0 or (out.find('Error') != -1 and out.find("is frozen") == -1):
                    _logger.error("%s %s" % (self.timestamp,out))                                    
                    dispError[dispatchDBlock] = "Setupper._setupSource() could not freeze dispatchDBlock"
                    continue
                _logger.debug("%s %s" % (self.timestamp,out))                
            else:
                # use PandaDDM
                self.dispFileList[dispatchDBlock] = fileList[dispatchDBlock]
                # create a fake vuidStr for PandaDDM
                tmpMap  = {'vuid':commands.getoutput('uuidgen')}
                vuidStr = "%s" % tmpMap
            # get VUID
            try:
                exec "vuid = %s['vuid']" % vuidStr                
                # dataset spec. currentfiles is used to count the number of failed jobs
                ds = DatasetSpec()
                ds.vuid = vuid
                ds.name = dispatchDBlock
                ds.type = 'dispatch'
                ds.status = 'defined'
                ds.numberfiles  = len(fileList[dispatchDBlock])/2
                ds.currentfiles = 0
                dispList.append(ds)
                self.vuidMap[ds.name] = ds.vuid
            except:
                type, value, traceBack = sys.exc_info()
                _logger.error("_setupSource() : %s %s" % (type,value))
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
        _logger.debug('%s setupDestination idx:%s n:%s' % (self.timestamp,startIdx,nJobsInLoop))
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
            if job.jobStatus in ['failed','cancelled']:
                continue
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
                        if name == originalName:
                            # for original dataset
                            computingSite = file.destinationSE
                        # use DQ2
                        if (not self.pandaDDM) and (job.prodSourceLabel != 'ddm') and (job.destinationSE != 'local'):
                            # get src and dest DDM conversion is needed for unknown sites
                            if job.prodSourceLabel == 'user' and not self.siteMapper.siteSpecList.has_key(computingSite):
                                # DQ2 ID was set by using --destSE for analysis job to transfer output
                                tmpSrcDDM = self.siteMapper.getSite(job.computingSite).ddm
                            else:                            
                                tmpSrcDDM = self.siteMapper.getSite(computingSite).ddm
                            if job.prodSourceLabel == 'user' and not self.siteMapper.siteSpecList.has_key(file.destinationSE):
                                # DQ2 ID was set by using --destSE for analysis job to transfer output 
                                tmpDstDDM = tmpSrcDDM
                            else:
                                tmpDstDDM = self.siteMapper.getSite(file.destinationSE).ddm
                            # skip registration for _sub when src=dest
                            if tmpSrcDDM == tmpDstDDM and name != originalName and re.search('_sub\d+$',name) != None:
                                # create a fake vuidStr
                                vuidStr = 'vuid="%s"' % commands.getoutput('uuidgen')
                            else:
                                # register dataset
                                time.sleep(1)
                                # set hidden flag for _sub
                                tmpHiddenFlag = False
                                if name != originalName and re.search('_sub\d+$',name) != None:
                                    tmpHiddenFlag = True
                                _logger.debug((self.timestamp,'registerNewDataset',name,[],[],[],[],
                                               None,None,None,tmpHiddenFlag))
                                atFailed = 0
                                for iDDMTry in range(3):
                                    status,out = ddm.DQ2.main('registerNewDataset',name,[],[],[],[],
                                                              None,None,None,tmpHiddenFlag)
                                    if status != 0 and out.find('DQDatasetExistsException') != -1:
                                        atFailed = iDDMTry
                                        break
                                    elif status != 0 or out.find("DQ2 internal server exception") != -1 \
                                             or out.find("An error occurred on the central catalogs") != -1 \
                                             or out.find("MySQL server has gone away") != -1:
                                        _logger.debug("%s sleep %s for %s" % (self.timestamp,iDDMTry,name))
                                        _logger.debug(status)
                                        _logger.debug(out)
                                        _logger.debug("-------------")                                                                
                                        time.sleep(60)
                                    else:
                                        break
                                if status != 0 or out.find('Error') != -1:
                                    # unset vuidStr
                                    vuidStr = ""
                                    # ignore 'already exists' ERROR because original dataset may be registered by upstream.
                                    # atFailed > 0 is for the case in which the first attempt succeeded but report failure
                                    if (job.prodSourceLabel == 'panda' or (job.prodSourceLabel in ['ptest','rc_test'] and \
                                                                           job.processingType in ['pathena','prun','gangarobot-rctest']) \
                                        or name == originalName or atFailed > 0) and \
                                           out.find('DQDatasetExistsException') != -1:
                                        _logger.debug('%s ignored DQDatasetExistsException' % self.timestamp)
                                    else:
                                        destError[dest] = "Setupper._setupDestination() could not register : %s" % name
                                        _logger.error("%s %s" % (self.timestamp,out))
                                        continue
                                else:
                                    _logger.debug("%s %s" % (self.timestamp,out))                                
                                    vuidStr = "vuid = %s['vuid']" % out
                                # get list of tokens    
                                tmpTokenList = file.destinationDBlockToken.split(',')
                                # register datasetsets
                                if name == originalName or tmpSrcDDM != tmpDstDDM or \
                                       job.prodSourceLabel == 'panda' or (job.prodSourceLabel in ['ptest','rc_test'] and \
                                                                          job.processingType in ['pathena','prun','gangarobot-rctest']) \
                                       or len(tmpTokenList) > 1:
                                    time.sleep(1)
                                    # register location
                                    usingT1asT2 = False
                                    if job.prodSourceLabel == 'user' and not self.siteMapper.siteSpecList.has_key(computingSite):
                                        dq2IDList = [self.siteMapper.getSite(job.computingSite).ddm]
                                    else:
                                        if self.siteMapper.getSite(computingSite).cloud != job.cloud and \
                                           re.search('_sub\d+$',name) != None and \
                                           (not job.prodSourceLabel in ['user','panda']) and \
                                           (not self.siteMapper.getSite(computingSite).ddm.endswith('PRODDISK')):
                                            # T1 used as T2. Use both DATADISK and PRODDISK as locations while T1 PRODDISK is phasing out
                                            dq2IDList = [self.siteMapper.getSite(computingSite).ddm]
                                            if self.siteMapper.getSite(computingSite).setokens.has_key('ATLASPRODDISK'):
                                                dq2IDList += [self.siteMapper.getSite(computingSite).setokens['ATLASPRODDISK']]
                                            usingT1asT2 = True
                                        else:
                                            dq2IDList = [self.siteMapper.getSite(computingSite).ddm]
                                    # use another location when token is set
                                    if (not usingT1asT2) and (not file.destinationDBlockToken in ['NULL','']):
                                        dq2IDList = []
                                        for tmpToken in tmpTokenList:
                                            # set default
                                            dq2ID = self.siteMapper.getSite(computingSite).ddm
                                            # convert token to DQ2ID
                                            if self.siteMapper.getSite(computingSite).setokens.has_key(tmpToken):
                                                dq2ID = self.siteMapper.getSite(computingSite).setokens[tmpToken]
                                            # replace or append    
                                            if len(tmpTokenList) <= 1 or name != originalName:
                                                # use location consistent with token
                                                dq2IDList = [dq2ID]
                                                break
                                            else:
                                                # use multiple locations for _tid
                                                if not dq2ID in dq2IDList:
                                                    dq2IDList.append(dq2ID)
                                    # loop over all locations
                                    repLifeTime = None
                                    if name != originalName and re.search('_sub\d+$',name) != None:
                                        repLifeTime = "14 days"
                                    for dq2ID in dq2IDList:
                                        _logger.debug((self.timestamp,'registerDatasetLocation',name,dq2ID,0,0,None,None,None,repLifeTime))
                                        for iDDMTry in range(3):                            
                                            status,out = ddm.DQ2.main('registerDatasetLocation',name,dq2ID,0,0,None,None,None,repLifeTime)
                                            if status != 0 and out.find('DQLocationExistsException') != -1:
                                                break
                                            elif status != 0 or out.find("DQ2 internal server exception") != -1 \
                                                     or out.find("An error occurred on the central catalogs") != -1 \
                                                     or out.find("MySQL server has gone away") != -1:
                                                time.sleep(60)
                                            else:
                                                break
                                        # ignore "already exists at location XYZ"
                                        if out.find('DQLocationExistsException') != -1:
                                            _logger.debug('%s ignored DQLocationExistsException' % self.timestamp)
                                            status,out = 0,''
                                        else:
                                            _logger.debug("%s %s" % (self.timestamp,out))
                                            if status == 0 and out.find('Error') == -1:
                                                # change replica ownership for user datasets
                                                if self.resetLocation and ((name == originalName and job.prodSourceLabel == 'user') or \
                                                                           job.prodSourceLabel=='panda'):
                                                    # remove /CN=proxy and /CN=limited from DN
                                                    tmpRealDN = job.prodUserID
                                                    tmpRealDN = re.sub('/CN=limited proxy','',tmpRealDN)
                                                    tmpRealDN = re.sub('/CN=proxy','',tmpRealDN)
                                                    status,out = dq2Common.parse_dn(tmpRealDN)
                                                    if status != 0:
                                                        _logger.error("%s %s" % (self.timestamp,out))
                                                        status,out = 1,'failed to truncate DN:%s' % job.prodUserID
                                                    else:
                                                        tmpRealDN = out
                                                        _logger.debug((self.timestamp,'setReplicaMetaDataAttribute',name,dq2ID,'owner',tmpRealDN))
                                                        for iDDMTry in range(3):
                                                            status,out = ddm.DQ2.main('setReplicaMetaDataAttribute',name,dq2ID,'owner',tmpRealDN)
                                                            if status != 0 or out.find("DQ2 internal server exception") != -1 \
                                                                   or out.find("An error occurred on the central catalogs") != -1 \
                                                                   or out.find("MySQL server has gone away") != -1:
                                                                time.sleep(60)
                                                            else:
                                                                break
                                                        # failed
                                                        if status != 0 or out.find('Error') != -1:
                                                            _logger.error("%s %s" % (self.timestamp,out))
                                                            break
                                                        # delete old replicas
                                                        tmpDelStat = self.deleteDatasetReplicas([name],[dq2ID])
                                                        if not tmpDelStat:
                                                            status,out = 1,'failed to delete old replicas for %s' % name
                                                            break
                                        # failed
                                        if status != 0 or out.find('Error') != -1:
                                            _logger.error("%s %s" % (self.timestamp,out))
                                            break
                                else:
                                    # skip registerDatasetLocations
                                    status,out = 0,''
                                if status != 0 or out.find('Error') != -1:
                                    destError[dest] = "Could not register location : %s %s" % (name,out.split('\n')[-1])
                                elif job.prodSourceLabel == 'panda' or (job.prodSourceLabel in ['ptest','rc_test'] and \
                                                                        job.processingType in ['pathena','prun','gangarobot-rctest']):
                                    # do nothing for "panda" job
                                    pass
                                elif name == originalName and job.prodSourceLabel in ['managed','test','rc_test','ptest']:
                                    # set metadata
                                    time.sleep(1)
                                    dq2ID = self.siteMapper.getSite(file.destinationSE).ddm
                                    # use another location when token is set
                                    if not file.destinationDBlockToken in ['NULL','']:
                                        # register only the first token becasue it is used as the location
                                        tmpFirstToken = file.destinationDBlockToken.split(',')[0] 
                                        if self.siteMapper.getSite(file.destinationSE).setokens.has_key(tmpFirstToken):
                                            dq2ID = self.siteMapper.getSite(file.destinationSE).setokens[tmpFirstToken]
                                    _logger.debug((self.timestamp,'setMetaDataAttribute',name,'origin',dq2ID))
                                    for iDDMTry in range(3):
                                        status,out = ddm.DQ2.main('setMetaDataAttribute',name,'origin',dq2ID)
                                        if status != 0 or out.find("DQ2 internal server exception") != -1 \
                                               or out.find("An error occurred on the central catalogs") != -1 \
                                               or out.find("MySQL server has gone away") != -1:
                                            time.sleep(60)
                                        else:
                                            break
                                    _logger.debug("%s %s" % (self.timestamp,out))
                                    if status != 0 or (out != 'None' and out.find('already exists') == -1):
                                        _logger.error(out)
                                        destError[dest] = "Setupper._setupDestination() could not set metadata : %s" % name
                        # use PandaDDM or non-DQ2
                        else:
                            # create a fake vuidStr
                            vuidStr = 'vuid="%s"' % commands.getoutput('uuidgen')
                        # already failed    
                        if destError[dest] != '' and name == originalName:
                            break
                        # get vuid
                        if vuidStr == '':
                            _logger.debug((self.timestamp,'queryDatasetByName',name))
                            for iDDMTry in range(3):                    
                                status,out = ddm.repositoryClient.main('queryDatasetByName',name)
                                if status != 0 or out.find("DQ2 internal server exception") != -1 \
                                       or out.find("An error occurred on the central catalogs") != -1 \
                                       or out.find("MySQL server has gone away") != -1:
                                    time.sleep(60)
                                else:
                                    break
                            _logger.debug("%s %s" % (self.timestamp,out))
                            if status != 0 or out.find('Error') != -1:                                
                                _logger.error(out)
                            vuidStr = "vuid = %s['%s']['vuids'][0]" % (out.split('\n')[0],name)
                        try:
                            exec vuidStr
                            # dataset spec
                            ds = DatasetSpec()
                            ds.vuid         = vuid
                            ds.name         = name
                            ds.type         = 'output'
                            ds.numberfiles  = 0
                            ds.currentfiles = 0
                            ds.status       = 'defined'
                            # append
                            datasetList[(name,file.destinationSE,computingSite)] = ds
                        except:
                            # set status
                            type, value, traceBack = sys.exc_info()
                            _logger.error("_setupDestination() : %s %s" % (type,value))
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
                _logger.debug('%s made sub:%s for nFiles=%s' % (self.timestamp,tmpDsKey[0],datasetList[tmpDsKey].numberfiles))
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
            if job.jobStatus in ['failed','cancelled']:
                continue
            # ignore no dispatch jobs
            if job.dispatchDBlock=='NULL' or job.computingSite=='NULL':
                continue
            # extract dispatchDBlock and computingSite
            disp = (job.dispatchDBlock,job.computingSite)
            if dispError.has_key(disp) == 0:
                dispError[disp] = ''
                # DQ2 IDs
                tmpSrcID = 'BNL_ATLAS_1'
                if self.siteMapper.checkCloud(job.cloud):
                    # use cloud's source
                    tmpSrcID = self.siteMapper.getCloud(job.cloud)['source']
                srcDQ2ID = self.siteMapper.getSite(tmpSrcID).ddm
                # destination
                tmpDstID = job.computingSite
                if srcDQ2ID != self.siteMapper.getSite(job.computingSite).ddm and \
                       srcDQ2ID in self.siteMapper.getSite(job.computingSite).setokens.values():
                    # direct usage of remote SE. Mainly for prestaging
                    tmpDstID = tmpSrcID
                    _logger.debug('%s use remote SiteSpec of %s for %s' % (self.timestamp,tmpDstID,job.computingSite))
                # use srcDQ2ID as dstDQ2ID when dst SE is same as src SE
                srcSEs = brokerage.broker_util.getSEfromSched(self.siteMapper.getSite(tmpSrcID).se)
                dstSEs = brokerage.broker_util.getSEfromSched(self.siteMapper.getSite(tmpDstID).se)
                if srcSEs == dstSEs or job.computingSite.endswith("_REPRO"):
                    dstDQ2ID = srcDQ2ID
                else:
                    dstDQ2ID = self.siteMapper.getSite(job.computingSite).ddm
                # use DQ2
                if (not self.pandaDDM) and (not srcDQ2ID in PandaDDMSource or self.siteMapper.getSite(tmpDstID).cloud != 'US') \
                       and (job.prodSourceLabel != 'ddm') and (not job.computingSite.endswith("_REPRO")):
                    # look for replica
                    dq2ID = srcDQ2ID
                    dq2IDList = []
                    # register replica
                    if dq2ID != dstDQ2ID:
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
                            if job.cloud in ['TW',]:
                                tmpSiteSpec = self.siteMapper.getSite(tmpSrcID)
                                if tmpSiteSpec.setokens.has_key('ATLASDATADISK'):
                                    diskID = tmpSiteSpec.setokens['ATLASDATADISK']
                                if tmpSiteSpec.setokens.has_key('ATLASDATATAPE'):    
                                    tapeID = tmpSiteSpec.setokens['ATLASDATATAPE']
                                if tmpSiteSpec.setokens.has_key('ATLASMCTAPE'):                                        
                                    mctapeID = tmpSiteSpec.setokens['ATLASMCTAPE']
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
                                if job.cloud == 'US' and tmpRepMap.has_key('BNLPANDA'):
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
                            # hack for split T1
                            splitT1IDsHaveDS = []
                            for tmpSplitT1Key in tmpRepMap.keys():
                                if tmpSplitT1Key.startswith('NIKHEF-ELPROD'):
                                    splitT1IDsHaveDS.append(tmpSplitT1Key)
                            if job.cloud == 'NL' and splitT1IDsHaveDS != [] \
                                   and not tmpRepMap.has_key('SARA-MATRIX_MCDISK') \
                                   and not tmpRepMap.has_key('SARA-MATRIX_DATADISK') \
                                   and not tmpRepMap.has_key('SARA-MATRIX_MCTAPE') \
                                   and not tmpRepMap.has_key('SARA-MATRIX_DATATAPE'):
                                for tmpSplitT1Key in splitT1IDsHaveDS:
                                    if not tmpSplitT1Key in dq2IDList:
                                        dq2IDList.append(tmpSplitT1Key)
                            # consider cloudconfig.tier1se 
                            tmpCloudSEs = DataServiceUtils.getEndpointsAtT1(tmpRepMap,self.siteMapper,job.cloud)
                            useCloudSEs = []
                            for tmpCloudSE in tmpCloudSEs:
                                if not tmpCloudSE in dq2IDList:
                                    useCloudSEs.append(tmpCloudSE)
                            if useCloudSEs != []:
                                dq2IDList += useCloudSEs
                                _logger.debug('%s use additional endpoints %s from cloudconfig' % (self.timestamp,str(useCloudSEs)))
                        # use default location if empty
                        if dq2IDList == []:
                            dq2IDList = [dq2ID]
                        for dq2ID in dq2IDList:
                            time.sleep(1)
                            _logger.debug((self.timestamp,'registerDatasetLocation',job.dispatchDBlock,dq2ID,0,1,None,None,None,"7 days"))
                            for iDDMTry in range(3):                                            
                                status,out = ddm.DQ2.main('registerDatasetLocation',job.dispatchDBlock,dq2ID,0,1,None,None,None,"7 days")
                                if status != 0 or out.find("DQ2 internal server exception") != -1 \
                                       or out.find("An error occurred on the central catalogs") != -1 \
                                       or out.find("MySQL server has gone away") != -1:
                                    time.sleep(60)
                                else:
                                    break
                            _logger.debug("%s %s" % (self.timestamp,out))
                            # failure
                            if status != 0 or out.find('Error') != -1:
                                break
                    else:
                        # skip registerDatasetLocations
                        status,out = 0,''
                    if status != 0 or out.find('Error') != -1:
                        _logger.error(out)                    
                        dispError[disp] = "Setupper._subscribeDistpatchDB() could not register location"
                    else:
                        # assign destination
                        time.sleep(1)
                        optSub = {'DATASET_COMPLETE_EVENT' : ['https://%s:%s/server/panda/datasetCompleted' % \
                                                              (panda_config.pserverhost,panda_config.pserverport)]}
                        optSource = {}
                        optSrcPolicy = 001000 | 010000
                        dq2ID = dstDQ2ID
                        # prestaging
                        if srcDQ2ID == dstDQ2ID:
                            # stage-in callback
                            optSub['DATASET_STAGED_EVENT'] = ['https://%s:%s/server/panda/datasetCompleted' % \
                                                              (panda_config.pserverhost,panda_config.pserverport)]
                            # use ATLAS*TAPE
                            seTokens = self.siteMapper.getSite(tmpDstID).setokens
                            if seTokens.has_key('ATLASDATATAPE') and seTokens.has_key('ATLASMCTAPE'):
                                dq2ID = seTokens['ATLASDATATAPE']
                                # use MCDISK if needed
                                for tmpDataset,tmpRepMap in self.replicaMap[job.dispatchDBlock].iteritems():
                                    if (not tmpRepMap.has_key(dq2ID)) and tmpRepMap.has_key(seTokens['ATLASMCTAPE']):
                                        dq2ID = seTokens['ATLASMCTAPE']
                                        break
                                # for CERN and BNL
                                if job.cloud in ['CERN','US'] and self.replicaMap.has_key(job.dispatchDBlock):
                                    setNewIDflag = False
                                    if job.cloud == 'CERN':
                                        otherIDs = ['CERN-PROD_DAQ','CERN-PROD_TZERO','CERN-PROD_TMPDISK']
                                    else:
                                        otherIDs = ['BNLPANDA']
                                    for tmpDataset,tmpRepMap in self.replicaMap[job.dispatchDBlock].iteritems():
                                        if not tmpRepMap.has_key(dq2ID):
                                            # look for another id
                                            for cernID in otherIDs:
                                                if tmpRepMap.has_key(cernID):
                                                    dq2ID = cernID
                                                    setNewIDflag = True
                                                    break
                                            # break
                                            if setNewIDflag:
                                                break
                            optSrcPolicy = 000010
                            optSource[dq2ID] = {'policy' : 0}
                        else:
                            # set sources to handle T2s in another cloud and to transfer dis datasets being split in multiple sites 
                            for tmpDQ2ID in dq2IDList:
                                optSource[tmpDQ2ID] = {'policy' : 0}
                            # T1 used as T2
                            if job.cloud != self.siteMapper.getSite(tmpDstID).cloud and \
                                   (not dstDQ2ID.endswith('PRODDISK')) and \
                                   (not job.prodSourceLabel in ['user','panda']) and \
                                   self.siteMapper.getSite(tmpDstID).cloud in ['US']:
                                seTokens = self.siteMapper.getSite(tmpDstID).setokens
                                # use T1_PRODDISK
                                if seTokens.has_key('ATLASPRODDISK'):
                                    dq2ID = seTokens['ATLASPRODDISK']
                        # register subscription        
                        _logger.debug('%s %s %s %s' % (self.timestamp,'registerDatasetSubscription',
                                                       (job.dispatchDBlock,dq2ID),
                                                       {'version':0,'archived':0,'callbacks':optSub,'sources':optSource,'sources_policy':optSrcPolicy,
                                                        'wait_for_sources':0,'destination':None,'query_more_sources':0,'sshare':"production",'group':None,
                                                        'activity':"Production",'acl_alias':None,'replica_lifetime':"7 days"}))
                        for iDDMTry in range(3):                                                                
                            status,out = ddm.DQ2.main('registerDatasetSubscription',job.dispatchDBlock,dq2ID,version=0,archived=0,callbacks=optSub,
                                                      sources=optSource,sources_policy=optSrcPolicy,wait_for_sources=0,destination=None,
                                                      query_more_sources=0,sshare="production",group=None,activity="Production",
                                                      acl_alias=None,replica_lifetime="7 days")
                            if status != 0 or out.find("DQ2 internal server exception") != -1 \
                                   or out.find("An error occurred on the central catalogs") != -1 \
                                   or out.find("MySQL server has gone away") != -1:
                                time.sleep(60)
                            else:
                                break
                        _logger.debug("%s %s" % (self.timestamp,out))
                        if status != 0 or (out != 'None' and len(out) != 35):
                            _logger.error(out)
                            dispError[disp] = "Setupper._subscribeDistpatchDB() could not register subscription"
                        # logging
                        try:
                            # make message
                            dq2ID = dstDQ2ID
                            message = '%s - siteID:%s type:dispatch vuid:%s' % (commands.getoutput('hostname'),dq2ID,
                                                                                self.vuidMap[job.dispatchDBlock])
                            # get logger
                            _pandaLogger = PandaLogger()
                            _pandaLogger.lock()
                            _pandaLogger.setParams({'Type':'registerSubscription'})
                            logger = _pandaLogger.getHttpLogger(panda_config.loggername)
                            # add message
                            logger.info(message)
                            # release HTTP handler
                            _pandaLogger.release()
                        except:
                            pass
                # use PandaDDM
                else:
                    # set DDM user DN
                    if ddmUser == 'NULL':
                        ddmUser = job.prodUserID
                    # create a DDM job
                    ddmjob = JobSpec()
                    ddmjob.jobDefinitionID   = int(time.time()) % 10000
                    ddmjob.jobName           = "%s" % commands.getoutput('uuidgen')
                    ddmjob.transformation    = 'http://pandaserver.cern.ch:25080/trf/mover/run_dq2_cr'
                    ddmjob.destinationDBlock = 'pandaddm_%s.%s' % (time.strftime('%y.%m.%d'),ddmjob.jobName)
                    if job.cloud == 'NULL':
                        ddmjob.cloud         = 'US'
                    else:
                        ddmjob.cloud         = job.cloud 
                    if not PandaMoverIDs.has_key(job.cloud):
                        ddmjob.computingSite = "BNL_ATLAS_DDM"
                    else:
                        ddmjob.computingSite = PandaMoverIDs[job.cloud]
                    ddmjob.destinationSE     = ddmjob.computingSite
                    ddmjob.assignedPriority  = 200000
                    if job.prodSourceLabel in ['software']:
                        # set higher priority for installation jobs
                        ddmjob.assignedPriority += 1000
                    else:
                        ddmjob.assignedPriority += job.currentPriority
                    ddmjob.currentPriority   = ddmjob.assignedPriority
                    if self.ddmAttempt != 0:
                        # keep count of attemptNr
                        ddmjob.attemptNr = self.ddmAttempt + 1
                    else:
                        ddmjob.attemptNr = 1
                    # check attemptNr to avoid endless loop
                    if ddmjob.attemptNr > 10:
                        err = "Too many attempts %s for %s" % (ddmjob.attemptNr,job.dispatchDBlock)
                        _logger.error(err)
                        dispError[disp] = err
                        continue
                    ddmjob.prodSourceLabel   = 'ddm'
                    ddmjob.transferType      = 'dis'
                    ddmjob.processingType    = 'pandamover'
                    # append log file
                    fileOL = FileSpec()
                    fileOL.lfn = "%s.job.log.tgz.%s" % (ddmjob.destinationDBlock,ddmjob.attemptNr)
                    fileOL.destinationDBlock = ddmjob.destinationDBlock
                    fileOL.destinationSE     = ddmjob.destinationSE
                    fileOL.dataset           = ddmjob.destinationDBlock
                    fileOL.type = 'log'
                    ddmjob.addFile(fileOL)
                    # make arguments
                    callBackURL = 'https://%s:%s/server/panda/datasetCompleted?vuid=%s&site=%s' % \
                                  (panda_config.pserverhost,panda_config.pserverport,
                                   self.vuidMap[job.dispatchDBlock],dstDQ2ID)
                    callBackURL = urllib.quote(callBackURL)
                    lfnsStr = ''
                    for tmpLFN in self.dispFileList[job.dispatchDBlock]['lfns']:
                        lfnsStr += '%s,' % tmpLFN
                    guidStr = ''
                    for tmpGUID in self.dispFileList[job.dispatchDBlock]['guids']:
                        guidStr += '%s,' % tmpGUID
                    guidStr = guidStr[:-1]
                    lfnsStr = lfnsStr[:-1]
                    # check input token
                    moverUseTape = False
                    for tmpFile in job.Files:
                        if tmpFile.type == 'input' and tmpFile.dispatchDBlockToken in ['ATLASDATATAPE']:
                            moverUseTape = True
                            break
                    if srcDQ2ID != dstDQ2ID:
                        # get destination dir
                        tmpSpec = self.siteMapper.getSite(job.computingSite)
                        destDir = brokerage.broker_util._getDefaultStorage(tmpSpec.dq2url,tmpSpec.se,tmpSpec.seprodpath)
                        if destDir == '':
                            err = "could not get default storage for %s" % job.computingSite
                            _logger.error(err)
                            dispError[disp] = err
                            continue
                        # normal jobs
                        argStr = ""
                        if moverUseTape:
                            argStr += "--useTape "
                        argStr += "-t 7200 -n 3 -s %s -r %s --guids %s --lfns %s --tapePriority %s --callBack %s -d %spanda/dis/%s%s %s" % \
                                  (srcDQ2ID,dstDQ2ID,guidStr,lfnsStr,job.currentPriority,callBackURL,destDir,
                                   time.strftime('%y/%m/%d/'),job.dispatchDBlock,job.dispatchDBlock)
                    else:
                        # prestaging jobs
                        argStr = ""
                        if moverUseTape:
                            argStr += "--useTape "
                        argStr += "-t 540 -n 2 -s %s -r %s --guids %s --lfns %s --tapePriority %s --callBack %s --prestage --cloud %s %s" % \
                                  (srcDQ2ID,dstDQ2ID,guidStr,lfnsStr,job.currentPriority,callBackURL,job.cloud,job.dispatchDBlock)
                    # set job parameters
                    ddmjob.jobParameters = argStr
                    _logger.debug('%s pdq2_cr %s' % (self.timestamp,ddmjob.jobParameters))
                    # set src/dest
                    ddmjob.sourceSite      = srcDQ2ID
                    ddmjob.destinationSite = dstDQ2ID
                    ddmJobs.append(ddmjob)
            # failed jobs
            if dispError[disp] != '':
                job.jobStatus = 'failed'
                job.ddmErrorCode = ErrorCode.EC_Setupper                
                job.ddmErrorDiag = dispError[disp]
                failedJobs.append(job)
        # update failed jobs only. succeeded jobs should be activate by DDM callback
        self.taskBuffer.updateJobs(failedJobs,True)
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


    #  update jobs
    def _updateJobs(self):
        updateJobs   = []
        failedJobs   = []
        activateJobs = []
        # sort out jobs
        for job in self.jobs:
            # failed jobs
            if job.jobStatus in ['failed','cancelled']:
                failedJobs.append(job)
            # no input jobs
            elif job.dispatchDBlock=='NULL':
                activateJobs.append(job)
            # normal jobs
            else:
                # change status
                job.jobStatus = "assigned"
                updateJobs.append(job)
        # update DB
        self.taskBuffer.activateJobs(activateJobs)
        self.taskBuffer.updateJobs(updateJobs,True)
        self.taskBuffer.updateJobs(failedJobs,True)
        # delete local values
        del updateJobs
        del failedJobs
        del activateJobs
        

    # correct LFN for attemptNr
    def _correctLFN(self):
        lfnMap = {}
        valMap = {}
        prodError = {}
        missingDS = {}
        jobsWaiting   = []
        jobsFailed    = []
        jobsProcessed = []
        allLFNs  = {}
        allGUIDs = {}
        cloudMap = {}
        lfnDsMap = {}
        replicaMap = {}
        _logger.debug('%s go into LFN correction' % self.timestamp)
        for job in self.jobs:
            if self.onlyTA:            
                _logger.debug("%s start TA session %s" % (self.timestamp,job.taskID))
            # check if sitename is known
            if job.computingSite != 'NULL' and (not job.computingSite in self.siteMapper.siteSpecList.keys()):
                job.jobStatus    = 'failed'
                job.ddmErrorCode = ErrorCode.EC_Setupper                
                job.ddmErrorDiag = "computingSite:%s is unknown" % job.computingSite
                # append job for downstream process
                jobsProcessed.append(job)
                # error message for TA
                if self.onlyTA:                            
                    _logger.error("%s %s" % (self.timestamp,job.ddmErrorDiag))
                continue
            # ignore no prodDBlock jobs or container dataset
            if job.prodDBlock == 'NULL':
                # set cloud
                if panda_config.enableDynamicTA and job.prodSourceLabel in ['managed','validation'] \
                       and job.cloud in ['NULL',''] and (not job.taskID in [None,'NULL',0]):
                    # look into map to check if it is already gotten
                    if not cloudMap.has_key(job.taskID):
                        # instantiate TaskAssigner
                        cloudResolver = TaskAssigner.TaskAssigner(self.taskBuffer,self.siteMapper,
                                                                  job.taskID,job.prodSourceLabel,job)
                        # check cloud
                        _logger.debug("%s check cloud for %s" % (self.timestamp,job.taskID))
                        retCloud = cloudResolver.checkCloud()
                        _logger.debug("%s checkCloud() -> %s" % (self.timestamp,retCloud))
                        # failed
                        if retCloud == None:
                            _logger.error("failed to check cloud for %s" % job.taskID)
                            # append job to waiting list
                            jobsWaiting.append(job)
                            continue
                        # to be set
                        elif retCloud == "":
                            # collect LFN/GUID
                            tmpLFNs  = []
                            tmpGUIDs = []
                            # set cloud
                            _logger.debug("%s set cloud for %s" % (self.timestamp,job.taskID))                        
                            retCloud = cloudResolver.setCloud(tmpLFNs,tmpGUIDs,metadata=job.metadata)
                            _logger.debug("%s setCloud() -> %s" % (self.timestamp,retCloud))
                            if retCloud == None:
                                _logger.debug("failed to set cloud for %s" % job.taskID)
                                # append job to waiting list
                                jobsWaiting.append(job)
                                continue
                        # append to map
                        cloudMap[job.taskID] = retCloud 
                    # set cloud
                    job.cloud = cloudMap[job.taskID]
                    # message for TA
                    if self.onlyTA:            
                        _logger.debug("%s set %s:%s" % (self.timestamp,job.taskID,job.cloud))
                # append job to processed list
                jobsProcessed.append(job)
                continue
            # collect datasets
            datasets = []
            for file in job.Files:
                if file.type == 'input' and file.dispatchDBlock == 'NULL' \
                        and (file.GUID == 'NULL' or job.prodSourceLabel in ['managed','test','ptest']):
                    if not file.dataset in datasets:
                        datasets.append(file.dataset)
            # get LFN list
            for dataset in datasets:
                if not dataset in lfnMap.keys():
                    prodError[dataset] = ''
                    lfnMap[dataset] = {}
                    # get LFNs
                    time.sleep(1)
                    for iDDMTry in range(3):
                        _logger.debug((self.timestamp,'listFilesInDataset',dataset))
                        status,out = ddm.DQ2.main('listFilesInDataset',dataset)
                        if out.find("DQUnknownDatasetException") != -1:
                            break
                        elif status == -1:
                            break
                        elif status != 0 or out.find("DQ2 internal server exception") != -1 \
                                 or out.find("An error occurred on the central catalogs") != -1 \
                                 or out.find("MySQL server has gone away") != -1:
                            time.sleep(60)
                        else:
                            break
                    if status != 0 or out.startswith('Error'):
                        _logger.error(out)                                                                    
                        prodError[dataset] = 'could not get file list of prodDBlock %s' % dataset
                        _logger.error(prodError[dataset])
                        # doesn't exist in DQ2
                        if out.find('DQUnknownDatasetException') != -1:
                            missingDS[dataset] = "DS:%s not found in DQ2" % dataset
                        elif status == -1:
                            missingDS[dataset] = out
                    else:
                        # make map (key: LFN w/o attemptNr, value: LFN with attemptNr)
                        items = {}
                        try:
                            # protection for empty dataset
                            if out != '()':
                                exec "items = %s[0]" % out
                            # keep values to avoid redundant lookup
                            self.lfnDatasetMap[dataset] = items
                            # loop over all files    
                            for guid,vals in items.iteritems():
                                valMap[vals['lfn']] = {'guid' : guid, 'fsize' : vals['filesize'],
                                                       'md5sum' : vals['checksum'],
                                                       'chksum' : vals['checksum'],
                                                       'scope'  : vals['scope']}
                                genLFN = re.sub('\.\d+$','',vals['lfn'])
                                if lfnMap[dataset].has_key(genLFN):
                                    # get attemptNr
                                    newAttNr = 0
                                    newMat = re.search('\.(\d+)$',vals['lfn'])
                                    if newMat != None:
                                        newAttNr = int(newMat.group(1))
                                    oldAttNr = 0
                                    oldMat = re.search('\.(\d+)$',lfnMap[dataset][genLFN])
                                    if oldMat != None:
                                        oldAttNr = int(oldMat.group(1))
                                    # compare
                                    if newAttNr > oldAttNr:
                                        lfnMap[dataset][genLFN] = vals['lfn']
                                else:
                                    lfnMap[dataset][genLFN] = vals['lfn']
                                # mapping from LFN to DS
                                lfnDsMap[lfnMap[dataset][genLFN]] = dataset
                        except:
                            prodError[dataset] = 'could not convert HTTP-res to map for prodDBlock %s' % dataset
                            _logger.error(prodError[dataset])
                            _logger.error(out)
                    # get replica locations        
                    if (self.onlyTA or job.prodSourceLabel in ['managed','test']) \
                           and prodError[dataset] == '' and (not replicaMap.has_key(dataset)):
                        if dataset.endswith('/'):
                            status,out = self.getListDatasetReplicasInContainer(dataset)
                        else:                
                            for iDDMTry in range(3):
                                _logger.debug((self.timestamp,'listDatasetReplicas',dataset))
                                status,out = ddm.DQ2.main('listDatasetReplicas',dataset,0,None,False)
                                if status != 0 or out.find("DQ2 internal server exception") != -1 \
                                       or out.find("An error occurred on the central catalogs") != -1 \
                                       or out.find("MySQL server has gone away") != -1 \
                                       or out == '()':
                                    time.sleep(60)
                                else:
                                    break
                        if status != 0 or out.startswith('Error'):
                            prodError[dataset] = 'could not get locations for %s' % dataset
                            _logger.error(prodError[dataset])
                            _logger.error(out)
                        else:
                            tmpRepSites = {}
                            try:
                                # convert res to map
                                exec "tmpRepSites = %s" % out
                                replicaMap[dataset] = tmpRepSites
                            except:
                                prodError[dataset] = 'could not convert HTTP-res to replica map for %s' % dataset
                                _logger.error(prodError[dataset])
                                _logger.error(out)
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
                        _logger.error("%s %s" % (self.timestamp,missingDS[dataset]))
                        self.sendTaMesg("%s %s" % (job.taskID,missingDS[dataset]),msgType='error')
                    else:
                        _logger.debug("%s %s failed with %s" % (self.timestamp,job.PandaID,missingDS[dataset]))
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
                        _logger.error("%s %s" % (self.timestamp,prodError[dataset]))
                    break
            if isFailed:
                continue
            # set cloud
            if panda_config.enableDynamicTA and job.prodSourceLabel in ['managed','validation'] \
               and job.cloud in ['NULL',''] and (not job.taskID in [None,'NULL',0]):
                # look into map to check if it is already gotten
                if not cloudMap.has_key(job.taskID):
                    # instantiate TaskAssigner
                    cloudResolver = TaskAssigner.TaskAssigner(self.taskBuffer,self.siteMapper,
                                                              job.taskID,job.prodSourceLabel,job)
                    # check cloud
                    _logger.debug("%s check cloud for %s" % (self.timestamp,job.taskID))
                    retCloud = cloudResolver.checkCloud()
                    _logger.debug("%s checkCloud() -> %s" % (self.timestamp,retCloud))
                    # failed
                    if retCloud == None:
                        _logger.error("failed to check cloud for %s" % job.taskID)
                        # append job to waiting list
                        jobsWaiting.append(job)
                        continue
                    # to be set
                    elif retCloud == "":
                        # collect LFN/GUID
                        tmpLFNs  = []
                        tmpGUIDs = []
                        tmpReLoc = {}
                        tmpCountMap = {}
                        for dataset in datasets:
                            # get LFNs
                            eachDSLFNs = lfnMap[dataset].values()
                            tmpLFNs += eachDSLFNs
                            # get GUIDs
                            for oneLFN in eachDSLFNs:
                                tmpGUIDs.append(valMap[oneLFN]['guid'])
                            # locations
                            tmpReLoc[dataset] = replicaMap[dataset]
                            # file counts
                            tmpCountMap[dataset] = len(eachDSLFNs)
                        # set cloud
                        _logger.debug("%s set cloud for %s" % (self.timestamp,job.taskID))                        
                        retCloud = cloudResolver.setCloud(tmpLFNs,tmpGUIDs,tmpReLoc,metadata=job.metadata,
                                                          fileCounts=tmpCountMap)
                        _logger.debug("%s setCloud() -> %s" % (self.timestamp,retCloud))
                        if retCloud == None:
                            _logger.debug("failed to set cloud for %s" % job.taskID)
                            # append job to waiting list
                            jobsWaiting.append(job)
                            continue
                    # append to map
                    cloudMap[job.taskID] = retCloud 
                # set cloud
                job.cloud = cloudMap[job.taskID]
                # message for TA
                if self.onlyTA:            
                    _logger.debug("%s set %s:%s" % (self.timestamp,job.taskID,job.cloud))
            _logger.debug('%s replacing generic LFNs' % self.timestamp)                    
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
                            errMsg = "GUID for %s not found in DQ2" % file.lfn
                            _logger.debug("%s %s" % (self.timestamp,errMsg))
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
                        if not allLFNs.has_key(job.cloud):
                            allLFNs[job.cloud] = []
                        if not allGUIDs.has_key(job.cloud):
                            allGUIDs[job.cloud] = []
                        allLFNs[job.cloud].append(file.lfn)
                        allGUIDs[job.cloud].append(file.GUID)                    
            # modify jobParameters
            if not isFailed:
                for patt,repl in replaceList:
                    job.jobParameters = re.sub('%s ' % patt, '%s ' % repl, job.jobParameters)
                # append job to processed list
                jobsProcessed.append(job)
            # return if TA only
        if self.onlyTA:
            _logger.debug("%s end TA sessions" % self.timestamp)
            return
        _logger.debug('%s checking missing files at T1' % self.timestamp)        
        # get missing LFNs from source LRC/LFC
        missLFNs = {}
        for cloudKey in allLFNs.keys():
            # use BNL by default
            dq2URL = self.siteMapper.getSite('BNL_ATLAS_1').dq2url
            dq2SE  = []
            # use cloud's source
            if self.siteMapper.checkCloud(cloudKey):
                tmpSrcID   = self.siteMapper.getCloud(cloudKey)['source']
                tmpSrcSite = self.siteMapper.getSite(tmpSrcID)
                # get LRC/LFC URL
                if not tmpSrcSite.lfchost in [None,'']:
                    # LFC
                    dq2URL = 'lfc://'+tmpSrcSite.lfchost+':/grid/atlas/'
                    if tmpSrcSite.se != None:
                        for tmpSrcSiteSE in tmpSrcSite.se.split(','):
                            match = re.search('.+://([^:/]+):*\d*/*',tmpSrcSiteSE)
                            if match != None:
                                dq2SE.append(match.group(1))
                    # hack for split T1
                    if cloudKey == 'NL':
                        tmpSplitSite = self.siteMapper.getSite('NIKHEF-ELPROD')
                        if tmpSplitSite.se != None:
                            for tmpSrcSiteSE in tmpSplitSite.se.split(','):
                                match = re.search('.+://([^:/]+):*\d*/*',tmpSrcSiteSE)
                                if match != None:
                                    dq2SE.append(match.group(1)) 
                else:
                    # LRC
                    dq2URL = tmpSrcSite.dq2url
                    dq2SE  = []
            # get missing files
            tmpMissLFNs = brokerage.broker_util.getMissLFNsFromLRC(allLFNs[cloudKey],dq2URL,allGUIDs[cloudKey],dq2SE)
            # append
            if not missLFNs.has_key(cloudKey):
                missLFNs[cloudKey] = []
            missLFNs[cloudKey] += tmpMissLFNs
        _logger.debug('%s checking T2 LFC' % self.timestamp)
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
                    self.availableLFNsInT2[cloudKey][tmpDsName] = {'allfiles':[],'allguids':[],'sites':{}}
                    for tmpSiteName in tmpSiteNameDQ2Map.keys():
                        self.availableLFNsInT2[cloudKey][tmpDsName]['sites'][tmpSiteName] = []
                    self.availableLFNsInT2[cloudKey][tmpDsName]['siteDQ2IDs'] = tmpSiteNameDQ2Map   
                # add files    
                if not tmpCheckLFN in self.availableLFNsInT2[cloudKey][tmpDsName]:
                    self.availableLFNsInT2[cloudKey][tmpDsName]['allfiles'].append(tmpCheckLFN)
                    self.availableLFNsInT2[cloudKey][tmpDsName]['allguids'].append(allGUIDs[cloudKey][allLFNs[cloudKey].index(tmpCheckLFN)])
            # get available files at each T2
            for tmpDsName in self.availableLFNsInT2[cloudKey].keys():
                checkedDq2SiteMap = {}
                checkLfcSeMap = {}
                for tmpSiteName in self.availableLFNsInT2[cloudKey][tmpDsName]['sites'].keys():
                    tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                    # add LFC
                    if not checkLfcSeMap.has_key(tmpSiteSpec.lfchost):
                        checkLfcSeMap[tmpSiteSpec.lfchost] = {}
                    # add site
                    if not checkLfcSeMap[tmpSiteSpec.lfchost].has_key(tmpSiteName):
                        checkLfcSeMap[tmpSiteSpec.lfchost][tmpSiteName] = []
                    # add SE        
                    if tmpSiteSpec.se != None:
                        for tmpSrcSiteSE in tmpSiteSpec.se.split(','):
                            match = re.search('.+://([^:/]+):*\d*/*',tmpSrcSiteSE)
                            if match != None:
                                checkLfcSeMap[tmpSiteSpec.lfchost][tmpSiteName].append(match.group(1))
                # LFC lookup
                for tmpLfcHost in checkLfcSeMap.keys():  
                    # get SEs
                    tmpSEList = []
                    for tmpSiteName in checkLfcSeMap[tmpLfcHost].keys():
                        tmpSEList += checkLfcSeMap[tmpLfcHost][tmpSiteName]
                    # get available file list
                    _logger.debug('%s checking T2 LFC=%s for %s' % (self.timestamp,tmpLfcHost,tmpSEList))
                    bulkAvFiles = brokerage.broker_util.getFilesFromLRC(self.availableLFNsInT2[cloudKey][tmpDsName]['allfiles'],
                                                                        'lfc://'+tmpLfcHost+':/grid/atlas/',
                                                                        self.availableLFNsInT2[cloudKey][tmpDsName]['allguids'],
                                                                        storageName=tmpSEList,getPFN=True)
                    # check each site
                    for tmpSiteName in checkLfcSeMap[tmpLfcHost].keys():
                        self.availableLFNsInT2[cloudKey][tmpDsName]['sites'][tmpSiteName] = []                        
                        for tmpLFNck,tmpPFNlistck in bulkAvFiles.iteritems():
                            siteHasFileFlag = False
                            for tmpPFNck in tmpPFNlistck:
                                # check se
                                for tmpSE in checkLfcSeMap[tmpLfcHost][tmpSiteName]:
                                    if '://'+tmpSE in tmpPFNck:
                                        siteHasFileFlag = True
                                        break
                                # escape
                                if siteHasFileFlag:
                                    break
                            # append
                            if siteHasFileFlag:
                                self.availableLFNsInT2[cloudKey][tmpDsName]['sites'][tmpSiteName].append(tmpLFNck)
                        _logger.debug('%s available %s files at %s T2=%s for %s' % \
                                      (self.timestamp,
                                       len(self.availableLFNsInT2[cloudKey][tmpDsName]['sites'][tmpSiteName]),
                                       cloudKey,tmpSiteName,tmpDsName))
        _logger.debug('%s missLFNs at T1 %s' % (self.timestamp,missLFNs))
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
                    if missLFNs.has_key(job.cloud) and file.lfn in missLFNs[job.cloud]:
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
                        if not self.availableLFNsInT2.has_key(job.cloud):
                            goToT2 = False 
                            break
                        # no dataset info
                        if not self.availableLFNsInT2[job.cloud].has_key(tmpFile.dataset):
                            goToT2 = False                             
                            break
                        # initial candidates
                        if tmpCandT2s == None:
                            tmpCandT2s = self.availableLFNsInT2[job.cloud][tmpFile.dataset]['sites']
                        # check all candidates    
                        newCandT2s = []    
                        for tmpCandT2 in tmpCandT2s:
                            # site doesn't have the dataset
                            if not self.availableLFNsInT2[job.cloud][tmpFile.dataset]['sites'].has_key(tmpCandT2):
                                continue
                            # site has the file
                            if tmpFile.lfn in self.availableLFNsInT2[job.cloud][tmpFile.dataset]['sites'][tmpCandT2]:
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
                            if not self.missingDatasetList.has_key(job.cloud):
                                self.missingDatasetList[job.cloud] = {}
                            if not self.missingDatasetList[job.cloud].has_key(tmpFile.dataset):
                                self.missingDatasetList[job.cloud][tmpFile.dataset] = []
                            if not tmpFile.GUID in self.missingDatasetList[job.cloud][tmpFile.dataset]:
                                self.missingDatasetList[job.cloud][tmpFile.dataset].append(tmpFile.GUID)
        # set data summary fields
        for tmpJob in self.jobs:
            try:
                # set only for production/analysis/test
                if not tmpJob.prodSourceLabel in ['managed','test','rc_test','ptest','user']:
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
                            tmpInputFileProject = tmpInputItems[0]
                            # input type. ignore user/group/groupXY 
                            if len(tmpInputItems) > 4 and (not tmpInputItems[0] in ['','NULL','user','group']) \
                                   and (not tmpInputItems[0].startswith('group')):
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
            except:
                errType,errValue = sys.exc_info()[:2]
                _logger.error("failed to set data summary fields for PandaID=%s: %s %s" % (tmpJob.PandaID,errType,errValue))
        # send jobs to jobsWaiting
        self.taskBuffer.keepJobs(jobsWaiting)
        # update failed job
        self.taskBuffer.updateJobs(jobsFailed,True)        
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
            _logger.debug('%s MemCheck PID=%s Name=%s VSZ=%s RSS=%s' % (self.timestamp,os.getpid(),name,vmSize,vmRSS))
        except:
            type, value, traceBack = sys.exc_info()
            _logger.error("memoryCheck() : %s %s" % (type,value))
            _logger.debug('%s MemCheck PID=%s unknown' % (self.timestamp,os.getpid()))
            return


    # check DDM response
    def isDQ2ok(self,out):
        if out.find("DQ2 internal server exception") != -1 \
               or out.find("An error occurred on the central catalogs") != -1 \
               or out.find("MySQL server has gone away") != -1 \
               or out == '()':
            return False
        return True


    # get list of files in dataset
    def getListFilesInDataset(self,dataset):
        # use cache data
        if self.lfnDatasetMap.has_key(dataset):
            return True,self.lfnDatasetMap[dataset]
        for iDDMTry in range(3):
            _logger.debug((self.timestamp,'listFilesInDataset',dataset))
            status,out = ddm.DQ2.main('listFilesInDataset',dataset)
            if out.find("DQUnknownDatasetException") != -1:
                break
            elif status == -1:
                break
            elif status != 0 or (not self.isDQ2ok(out)):
                time.sleep(60)
            else:
                break
        if status != 0 or out.startswith('Error'):
            _logger.error("%s %s" % (self.timestamp,out))
            return False,{}
        # convert
        items = {}
        try:
            exec "items = %s[0]" % out
        except:
            return False,{}
        return True,items

        
    # get list of datasets in container
    def getListDatasetInContainer(self,container):
        # get datasets in container
        _logger.debug((self.timestamp,'listDatasetsInContainer',container))
        for iDDMTry in range(3):
            status,out = ddm.DQ2.main('listDatasetsInContainer',container)
            if status != 0 or (not self.isDQ2ok(out)):
                time.sleep(60)
            else:
                break
        _logger.debug('%s %s' % (self.timestamp,out))
        if status != 0 or out.startswith('Error'):
            return False,out
        datasets = []
        try:
            # convert to list
            exec "datasets = %s" % out
        except:
            return False,out
        return True,datasets

        
    def getListDatasetReplicasInContainer(self,container,getMap=False):
        # get datasets in container
        _logger.debug((self.timestamp,'listDatasetsInContainer',container))
        for iDDMTry in range(3):
            status,out = ddm.DQ2.main('listDatasetsInContainer',container)
            if status != 0 or out.find("DQ2 internal server exception") != -1 \
                   or out.find("An error occurred on the central catalogs") != -1 \
                   or out.find("MySQL server has gone away") != -1 \
                   or out == '()':
                time.sleep(60)
            else:
                break
        _logger.debug('%s %s' % (self.timestamp,out))
        if status != 0 or out.startswith('Error'):
            return status,out
        datasets = []
        try:
            # convert to list
            exec "datasets = %s" % out
        except:
            return status,out
        # loop over all datasets
        allRepMap = {}
        for dataset in datasets:
            _logger.debug((self.timestamp,'listDatasetReplicas',dataset))
            for iDDMTry in range(3):
                status,out = ddm.DQ2.main('listDatasetReplicas',dataset,0,None,False)
                if status != 0 or out.find("DQ2 internal server exception") != -1 \
                       or out.find("An error occurred on the central catalogs") != -1 \
                       or out.find("MySQL server has gone away") != -1 \
                       or out == '()':
                    time.sleep(60)
                else:
                    break
            _logger.debug('%s %s' % (self.timestamp,out))                
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
        _logger.debug('%s %s' % (self.timestamp,str(allRepMap)))
        if not getMap:
            return 0,str(allRepMap)
        else:
            return 0,allRepMap


    # get list of replicas for a dataset
    def getListDatasetReplicas(self,dataset):
        nTry = 3
        for iDDMTry in range(nTry):
            _logger.debug("%s %s/%s listDatasetReplicas %s" % (self.timestamp,iDDMTry,nTry,dataset))
            status,out = ddm.DQ2.main('listDatasetReplicas',dataset,0,None,False)
            if status != 0 or (not self.isDQ2ok(out)):
                time.sleep(60)
            else:
                break
        # result    
        if status != 0 or out.startswith('Error'):
            _logger.error(self.timestamp+' '+out)
            _logger.error('%s bad DQ2 response for %s' % (self.timestamp,dataset))            
            return False,{}
        try:
            # convert res to map
            exec "tmpRepSites = %s" % out
            _logger.debug('%s getListDatasetReplicas->%s' % (self.timestamp,str(tmpRepSites)))
            return True,tmpRepSites
        except:
            _logger.error(self.timestamp+' '+out)            
            _logger.error('%s could not convert HTTP-res to replica map for %s' % (self.timestamp,dataset))
            return False,{}


    # delete original locations
    def deleteDatasetReplicas(self,datasets,keepSites):
        # loop over all datasets
        for dataset in datasets:
            # get locations
            status,tmpRepSites = self.getListDatasetReplicas(dataset)
            if not status:
                return False
            # no replicas
            if len(tmpRepSites.keys()) == 0:
                continue
            delSites = []
            for tmpRepSite in tmpRepSites.keys():
                if not tmpRepSite in keepSites:
                    delSites.append(tmpRepSite)
            # no repilicas to be deleted
            if delSites == []:
                continue
            # delete
            nTry = 3
            for iDDMTry in range(nTry):
                _logger.debug("%s %s/%s deleteDatasetReplicas %s %s" % (self.timestamp,iDDMTry,nTry,dataset,str(delSites)))
                status,out = ddm.DQ2.main('deleteDatasetReplicas',dataset,delSites)
                if status != 0 or (not self.isDQ2ok(out)):
                    time.sleep(60)
                else:
                    break
            # result
            if status != 0 or out.startswith('Error'):
                _logger.error(self.timestamp+' '+out)
                _logger.error('%s bad DQ2 response for %s' % (self.timestamp,dataset))            
                return False
            _logger.debug(self.timestamp+' '+out)
        # return
        _logger.debug('%s deleted replicas for %s' % (self.timestamp,str(datasets)))
        return True


    # dynamic data placement for analysis jobs
    def _dynamicDataPlacement(self):
        # no jobs
        if len(self.jobs) == 0:
            return
        # only successful analysis
        if self.jobs[0].jobStatus in ['failed','cancelled'] or (not self.jobs[0].prodSourceLabel in ['user','panda']):
            return
        # execute
        _logger.debug('%s execute PD2P' % self.timestamp)
        from DynDataDistributer import DynDataDistributer
        ddd = DynDataDistributer(self.jobs,self.taskBuffer,self.siteMapper)
        ddd.run()
        _logger.debug('%s finished PD2P' % self.timestamp)        
        return


    # make dis datasets for existing files to avoid deletion when jobs are queued
    def _makeDisDatasetsForExistingfiles(self):
        _logger.debug('%s make dis datasets for existing files' % self.timestamp)        
        # collect existing files
        dsFileMap = {}
        nMaxJobs  = 20
        nJobsMap  = {}
        for tmpJob in self.jobs:
            # use production or test jobs only
            if not tmpJob.prodSourceLabel in ['managed','test']:
                continue
            # ignore inappropriate status
            if tmpJob.jobStatus in ['failed','cancelled','waiting']:
                continue
            # check cloud
            if (tmpJob.cloud == 'ND' and self.siteMapper.getSite(tmpJob.computingSite).cloud == 'ND') or \
                   (tmpJob.cloud == 'US' and self.siteMapper.getSite(tmpJob.computingSite).cloud == 'US'):
                continue
            # check SE to use T2 only
            tmpSrcID = self.siteMapper.getCloud(tmpJob.cloud)['source']
            srcSEs = brokerage.broker_util.getSEfromSched(self.siteMapper.getSite(tmpSrcID).se)
            dstSEs = brokerage.broker_util.getSEfromSched(self.siteMapper.getSite(tmpJob.computingSite).se)
            if srcSEs == dstSEs:
                continue
            # look for log _sub dataset to be used as a key
            logSubDsName = ''
            for tmpFile in tmpJob.Files:
                if tmpFile.type == 'log':
                    logSubDsName = tmpFile.destinationDBlock
                    break
            # append site
            destDQ2ID = self.siteMapper.getSite(tmpJob.computingSite).ddm
            # T1 used as T2
            if tmpJob.cloud != self.siteMapper.getSite(tmpJob.computingSite).cloud and \
               not destDQ2ID.endswith('PRODDISK') and \
               self.siteMapper.getSite(tmpJob.computingSite).cloud in ['US']:
                tmpSeTokens = self.siteMapper.getSite(tmpJob.computingSite).setokens
                if tmpSeTokens.has_key('ATLASPRODDISK'):
                    destDQ2ID = tmpSeTokens['ATLASPRODDISK']
            mapKeyJob = (destDQ2ID,logSubDsName)
            # increment the number of jobs per key
            if not nJobsMap.has_key(mapKeyJob):
                nJobsMap[mapKeyJob] = 0
            mapKey = (destDQ2ID,logSubDsName,nJobsMap[mapKeyJob]/nMaxJobs)
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
                if self.availableLFNsInT2.has_key(tmpJob.cloud) and self.availableLFNsInT2[tmpJob.cloud].has_key(tmpFile.dataset) \
                   and self.availableLFNsInT2[tmpJob.cloud][tmpFile.dataset]['sites'].has_key(tmpJob.computingSite) \
                   and tmpFile.lfn in self.availableLFNsInT2[tmpJob.cloud][tmpFile.dataset]['sites'][tmpJob.computingSite]:
                    realDestDQ2ID = self.availableLFNsInT2[tmpJob.cloud][tmpFile.dataset]['siteDQ2IDs'][tmpJob.computingSite]
                    realDestDQ2ID = tuple(realDestDQ2ID)
                # append
                if not dsFileMap[mapKey].has_key(realDestDQ2ID):
                    dsFileMap[mapKey][realDestDQ2ID] = {'taskID':tmpJob.taskID,
                                                        'PandaID':tmpJob.PandaID,
                                                        'files':{}}
                if not dsFileMap[mapKey][realDestDQ2ID]['files'].has_key(tmpFile.lfn):
                    dsFileMap[mapKey][realDestDQ2ID]['files'][tmpFile.lfn] = {'lfn' :tmpFile.lfn,
                                                                              'guid':tmpFile.GUID,
                                                                              'fileSpecs':[]}
                # add file spec
                dsFileMap[mapKey][realDestDQ2ID]['files'][tmpFile.lfn]['fileSpecs'].append(tmpFile)
        # loop over all locations
        dispList = []
        for tmpMapKey,tmpDumVal in dsFileMap.iteritems():
            tmpDumLocation,tmpLogSubDsName,tmpBunchIdx = tmpMapKey
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
                            fsizes.append(None)
                            chksums.append(None)
                            # set dis name
                            for tmpFileSpec in tmpFileList[tmpSubFileName]['fileSpecs']:
                                if tmpFileSpec.status in ['ready'] and tmpFileSpec.dispatchDBlock == 'NULL':
                                    tmpFileSpec.dispatchDBlock = disDBlock
                        # register datasets
                        iLoop += 1
                        _logger.debug((self.timestamp,'ext registerNewDataset',disDBlock,lfns,guids,fsizes,chksums,
                                       None,None,None,True))
                        for iDDMTry in range(3):
                            status,out = ddm.DQ2.main('registerNewDataset',disDBlock,lfns,guids,fsizes,chksums,
                                                      None,None,None,True)
                            if status != 0 and out.find('DQDatasetExistsException') != -1:
                                break
                            elif status != 0 or out.find("DQ2 internal server exception") != -1 \
                                     or out.find("An error occurred on the central catalogs") != -1 \
                                     or out.find("MySQL server has gone away") != -1:
                                _logger.debug("%s sleep %s for %s" % (self.timestamp,iDDMTry,disDBlock))
                                _logger.debug(status)
                                _logger.debug(out)
                                time.sleep(60)
                            else:
                                break
                        if status != 0 or out.find('Error') != -1:
                            _logger.error("%s %s" % (self.timestamp,out))                
                            continue
                        _logger.debug("%s %s" % (self.timestamp,out))
                        # get VUID
                        try:
                            exec "vuid = %s['vuid']" % out
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
                            _logger.error("ext registerNewDataset : failed to decode VUID for %s - %s %s" % (disDBlock,errType,errValue))
                            continue
                        # freezeDataset dispatch dataset
                        _logger.debug((self.timestamp,'freezeDataset',disDBlock))
                        for iDDMTry in range(3):            
                            status,out = ddm.DQ2.main('freezeDataset',disDBlock)
                            if status != 0 or out.find("DQ2 internal server exception") != -1 \
                                   or out.find("An error occurred on the central catalogs") != -1 \
                                   or out.find("MySQL server has gone away") != -1:
                                time.sleep(60)
                            else:
                                break
                        if status != 0 or (out.find('Error') != -1 and out.find("is frozen") == -1):
                            _logger.error("%s %s" % (self.timestamp,out))                                    
                            continue
                        _logger.debug("%s %s" % (self.timestamp,out))                
                        # register location
                        _logger.debug((self.timestamp,'registerDatasetLocation',disDBlock,tmpLocation,0,1,None,None,None,"7 days"))
                        for iDDMTry in range(3):
                            status,out = ddm.DQ2.main('registerDatasetLocation',disDBlock,tmpLocation,0,1,None,None,None,"7 days")
                            if status != 0 or out.find("DQ2 internal server exception") != -1 \
                                   or out.find("An error occurred on the central catalogs") != -1 \
                                   or out.find("MySQL server has gone away") != -1:
                                time.sleep(60)
                            else:
                                break
                        _logger.debug("%s %s" % (self.timestamp,out))
                        # failure
                        if status != 0 or out.find('Error') != -1:
                            _logger.error("%s %s" % (self.timestamp,out))                                    
                            continue
        # insert datasets to DB
        self.taskBuffer.insertDatasets(dispList)
        _logger.debug('%s finished to make dis datasets for existing files' % self.timestamp)
        return


    # pin input dataset 
    def _pinInputDatasets(self):
        _logger.debug('%s pin input datasets' % self.timestamp)        
        # collect input datasets and locations
        doneList = []
        allReplicaMap = {}
        for tmpJob in self.jobs:
            # ignore HC jobs
            if tmpJob.processingType.startswith('gangarobot') or \
               tmpJob.processingType.startswith('hammercloud'):
                continue
            # use production or test or user jobs only
            if not tmpJob.prodSourceLabel in ['managed','test','user']:
                continue
            # ignore inappropriate status
            if tmpJob.jobStatus in ['failed','cancelled','waiting']:
                continue
            # set lifetime
            if tmpJob.prodSourceLabel in ['managed','test']:
                pinLifeTime = 7
            else:
                pinLifeTime = 7
            # get source
            if tmpJob.prodSourceLabel in ['managed','test']:
                tmpSrcID = self.siteMapper.getCloud(tmpJob.cloud)['source']
                srcDQ2ID = self.siteMapper.getSite(tmpSrcID).ddm
            else:
                srcDQ2ID = self.siteMapper.getSite(tmpJob.computingSite).ddm
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
                        _logger.debug('%s pin DQ2 prefix=%s' % (self.timestamp,srcDQ2IDprefix))
                        for tmpDsName,tmpRepSitesMap in allReplicaMap[tmpFile.dataset].iteritems():
                            # loop over locations                        
                            for tmpRepSite in tmpRepSitesMap.keys():
                                if tmpRepSite.startswith(srcDQ2IDprefix) \
                                       and not 'TAPE' in tmpRepSite \
                                       and not 'SCRATCH' in tmpRepSite:
                                    tmpKey = (tmpDsName,tmpRepSite)
                                    # already done
                                    if tmpKey in doneList:
                                        continue
                                    # append to avoid repetition
                                    doneList.append(tmpKey)
                                    # get metadata
                                    status,tmpMetadata = self.getReplicaMetadata(tmpDsName,tmpRepSite)
                                    if not status:
                                        continue
                                    # check pin lifetime                            
                                    if tmpMetadata.has_key('pin_expirationdate'):
                                        if isinstance(tmpMetadata['pin_expirationdate'],types.StringType) and tmpMetadata['pin_expirationdate'] != 'None':
                                            # keep original pin lifetime if it is longer 
                                            origPinLifetime = datetime.datetime.strptime(tmpMetadata['pin_expirationdate'],'%Y-%m-%d %H:%M:%S')
                                            if origPinLifetime > datetime.datetime.utcnow()+datetime.timedelta(days=pinLifeTime):
                                                _logger.debug('%s skip pinning for %s:%s due to longer lifetime %s' % (self.timestamp,
                                                                                                                       tmpDsName,tmpRepSite,
                                                                                                                       tmpMetadata['pin_expirationdate']))
                                                continue
                                    # set pin lifetime
                                    status = self.setReplicaMetadata(tmpDsName,tmpRepSite,'pin_lifetime','%s days' % pinLifeTime)
        # retrun                    
        _logger.debug('%s pin input datasets done' % self.timestamp)
        return


    # make T1 subscription for missing files
    def _makeSubscriptionForMissing(self):
        _logger.debug('%s make subscriptions for missing files' % self.timestamp)
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
                        _logger.error('%s failed to get datasets in container:%s' % (self.timestamp,tmpDsName))
                        continue
                    # check if each dataset is actually used
                    for tmpConstDsName in tmpDsList:
                        # skip if already checked
                        if tmpDsName in missingList[tmpCloud]:
                            continue
                        # get files in each dataset
                        tmpStat,tmpFilesInDs = self.getListFilesInDataset(tmpConstDsName)
                        if not tmpStat:
                            _logger.error('%s failed to get files in dataset:%s' % (self.timestamp,tmpConstDsName))
                            continue
                        # loop over all files to check the dataset is used
                        for tmpGUID in tmpMissFiles:
                            # append if used
                            if tmpFilesInDs.has_key(tmpGUID):
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
            dstDQ2ID = self.siteMapper.getSite(tmpDstID).ddm
            # register subscription
            for missDsName in missDsNameList:
                _logger.debug('%s make subscription at %s for missing %s' % (self.timestamp,dstDQ2ID,missDsName))
                self.makeSubscription(missDsName,dstDQ2ID)
        # retrun
        _logger.debug('%s make subscriptions for missing files done' % self.timestamp)        
        return
    

    # check DDM response
    def isDQ2ok(self,out):
        if out.find("DQ2 internal server exception") != -1 \
               or out.find("An error occurred on the central catalogs") != -1 \
               or out.find("MySQL server has gone away") != -1 \
               or out == '()':
            return False
        return True

    
    # make subscription
    def makeSubscription(self,dataset,dq2ID):
        # return for failuer
        retFailed = False
        # make subscription    
        optSrcPolicy = 000001
        nTry = 3
        for iDDMTry in range(nTry):
            # register subscription
            _logger.debug('%s %s/%s registerDatasetSubscription %s %s' % (self.timestamp,iDDMTry,nTry,dataset,dq2ID))
            status,out = ddm.DQ2.main('registerDatasetSubscription',dataset,dq2ID,version=0,archived=0,
                                      callbacks={},sources={},sources_policy=optSrcPolicy,
                                      wait_for_sources=0,destination=None,query_more_sources=0,
                                      sshare="production",group=None,activity='Production',acl_alias='secondary')
            status,out = 0,''
            if out.find('DQSubscriptionExistsException') != -1:
                break
            elif out.find('DQLocationExistsException') != -1:
                break
            elif status != 0 or (not self.isDQ2ok(out)):
                time.sleep(60)
            else:
                break
        # result
        if out.find('DQSubscriptionExistsException') != -1:
            pass
        elif status != 0 or out.startswith('Error'):
            _logger.error("%s %s" % (self.timestamp,out))
            return retFailed
        # update 
        _logger.debug('%s %s %s' % (self.timestamp,status,out))
        # return
        return True

    
    # get replica metadata
    def getReplicaMetadata(self,datasetName,locationName):
        # response for failure
        resForFailure = False,{}
        # get metadata
        nTry = 3
        for iDDMTry in range(nTry):
            _logger.debug('%s %s/%s listMetaDataReplica %s %s' % (self.timestamp,iDDMTry,nTry,datasetName,locationName))
            status,out = ddm.DQ2.main('listMetaDataReplica',locationName,datasetName)
            if status != 0 or (not self.isDQ2ok(out)):
                time.sleep(60)
            else:
                break
        if status != 0 or out.startswith('Error'):
            _logger.error("%s %s" % (self.timestamp,out))
            return resForFailure
        metadata = {}
        try:
            # convert to map
            exec "metadata = %s" % out
        except:
            _logger.error('%s could not convert HTTP-res to replica metadata for %s:%s' % \
                          (self.timestamp,datasetName,locationName))
            return resForFailure
        # return
        _logger.debug('%s getReplicaMetadata -> %s' % (self.timestamp,str(metadata)))
        return True,metadata


    # set replica metadata
    def setReplicaMetadata(self,datasetName,locationName,attrname,attrvalue):
        # response for failure
        resForFailure = False
        # get metadata
        nTry = 3
        for iDDMTry in range(nTry):
            _logger.debug('%s %s/%s setReplicaMetaDataAttribute %s %s %s=%s' % (self.timestamp,iDDMTry,nTry,datasetName,
                                                                                locationName,attrname,attrvalue))
            status,out = ddm.DQ2.main('setReplicaMetaDataAttribute',datasetName,locationName,attrname,attrvalue)
            if status != 0 or (not self.isDQ2ok(out)):
                time.sleep(60)
            else:
                break
        if status != 0 or out.startswith('Error'):
            _logger.error("%s %s" % (self.timestamp,out))
            return resForFailure
        # return
        _logger.debug('%s setReplicaMetadata done' % self.timestamp)
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
                            
