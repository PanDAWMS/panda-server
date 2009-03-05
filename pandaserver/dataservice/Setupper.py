'''
setup dataset

'''

import re
import sys
import time
import urllib
import commands
import threading
import traceback
import ErrorCode
import TaskAssigner
from DDM import ddm
from taskbuffer.JobSpec import JobSpec
from taskbuffer.FileSpec import FileSpec
from taskbuffer.DatasetSpec import DatasetSpec
from brokerage.SiteMapper import SiteMapper
from brokerage.PandaSiteIDs import PandaMoverIDs
import brokerage.broker
import brokerage.broker_util


from config import panda_config
from pandalogger.PandaLogger import PandaLogger

# logger
_logger = PandaLogger().getLogger('Setupper')


# temporary
PandaDDMSource = ['BNLPANDA','BNL-OSG2_MCDISK']


class Setupper (threading.Thread):
    # constructor
    def __init__(self,taskBuffer,jobs,resubmit=False,pandaDDM=False,ddmAttempt=0,forkRun=False,onlyTA=False):
        threading.Thread.__init__(self)
        self.jobs       = jobs
        self.taskBuffer = taskBuffer
        # VUIDs of dispatchDBlocks
        self.vuidMap = {}
        # resubmission or not
        self.resubmit = resubmit
        # time stamp
        self.timestamp = time.asctime()
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


    # main
    def run(self):
        try:
            _logger.debug('%s startRun' % self.timestamp)
            self._memoryCheck()
            # run main procedure in the same process
            if not self.forkRun:
                if self.jobs != None and len(self.jobs) > 0:
                    _logger.debug('%s PandaID:%s type:%s' % (self.timestamp,self.jobs[0].PandaID,self.jobs[0].prodSourceLabel))
                # instantiate site mapper
                self.siteMapper = SiteMapper(self.taskBuffer)
                # correctLFN
                self._correctLFN()
                # run full Setupper
                if not self.onlyTA:
                    # invoke brokerage
                    _logger.debug('%s brokerSchedule' % self.timestamp)        
                    brokerage.broker.schedule(self.jobs,self.taskBuffer,self.siteMapper)
                    # setup dispatch dataset
                    _logger.debug('%s setupSource' % self.timestamp)        
                    self._setupSource()
                    # create dataset for outputs and assign destination
                    _logger.debug('%s setupDestination' % self.timestamp)        
                    self._setupDestination()
                    # update jobs
                    _logger.debug('%s updateJobs' % self.timestamp)        
                    self._updateJobs()
                    # then subscribe sites distpatchDBlocks. this must be the last method
                    _logger.debug('%s subscribeDistpatchDB' % self.timestamp)        
                    self._subscribeDistpatchDB()
            else:
                # write jobs to file
                import os
                import cPickle as pickle
                outFileName = '%s/set.%s_%s' % (panda_config.logdir,self.jobs[0].PandaID,commands.getoutput('uuidgen'))
                outFile = open(outFileName,'w')
                pickle.dump(self.jobs,outFile)
                outFile.close()
                # run main procedure in another process because python doesn't release memory
                com = 'env PYTHONPATH=%s:%s %s/python -Wignore %s/dataservice/forkSetupper.py -i %s' % \
                      (panda_config.pandaCommon_dir,panda_config.pandaPython_dir,panda_config.native_python,
                       panda_config.pandaPython_dir,outFileName)
                if self.onlyTA:
                    com += " -t"
                _logger.debug(com)
                # exeute
                status,output = commands.getstatusoutput(com)
                _logger.debug("Ret from another process: %s %s" % (status,output))                
            self._memoryCheck()            
            _logger.debug('%s endRun' % self.timestamp)
        except:
            type, value, traceBack = sys.exc_info()
            _logger.error("run() : %s %s" % (type,value))
        

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
            if job.jobStatus == 'failed':
                continue
            # production datablock. ignore container datasets 
            if job.prodDBlock != 'NULL' and (not self.pandaDDM) and (not job.prodDBlock.endswith('/')):
                # get VUID and record prodDBlock into DB
                if not prodError.has_key(job.prodDBlock):
                    time.sleep(1)
                    _logger.debug(('queryDatasetByName',job.prodDBlock))
                    prodError[job.prodDBlock] = ''
                    for iDDMTry in range(3):
                        status,out = ddm.repositoryClient.main('queryDatasetByName',job.prodDBlock)
                        if status != 0 or out.find("DQ2 internal server exception") != -1 \
                               or out.find("An error occurred on the central catalogs") != -1 \
                               or out.find("MySQL server has gone away") != -1:
                            time.sleep(60)
                        else:
                            break
                    _logger.debug(out)                        
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
                            for iDDMTry in range(3):
                                _logger.debug(('listDatasetReplicas',file.dataset))
                                status,out = ddm.DQ2.main('listDatasetReplicas',file.dataset,0,None,False)
                                if status != 0 or out.find("DQ2 internal server exception") != -1 \
                                       or out.find("An error occurred on the central catalogs") != -1 \
                                       or out.find("MySQL server has gone away") != -1 \
                                       or out == '()':
                                    time.sleep(60)
                                else:
                                    break
                            _logger.debug(out)
                            if status != 0 or out.startswith('Error'):
                                dispError[job.dispatchDBlock] = 'could not get locations for %s' % file.dataset
                                _logger.error(dispError[job.dispatchDBlock])
                            else:
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
        _logger.debug('%s 5' % self.timestamp)                                
        dispList = []
        for dispatchDBlock in fileList.keys():
            # ignore empty dataset
            if len(fileList[dispatchDBlock]['lfns']) == 0:
                continue
            _logger.debug('%s 6' % self.timestamp)            
            # use DQ2
            if (not self.pandaDDM) and (not dispSiteMap[dispatchDBlock]['src'] in PandaDDMSource) \
                   and (job.prodSourceLabel != 'ddm') and (not dispSiteMap[dispatchDBlock]['site'].endswith("_REPRO")):
                # delete dataset from DB and DDM just in case
                if self.resubmit:
                    # make sure if it is dis datasets
                    if re.search('_dis\d+$',dispatchDBlock) != None:
                        time.sleep(1)
                        _logger.debug(('eraseDataset',dispatchDBlock))
                        for iDDMTry in range(3):                
                            status,out = ddm.DQ2.main('eraseDataset',dispatchDBlock)
                            if status != 0 and out.find('DQUnknownDatasetException') != -1:
                                break
                            elif status != 0 or out.find("DQ2 internal server exception") != -1 \
                                     or out.find("An error occurred on the central catalogs") != -1 \
                                     or out.find("MySQL server has gone away") != -1:
                                time.sleep(60)
                            else:
                                break
                        _logger.debug(out)
                        ret = self.taskBuffer.deleteDatasets([dispatchDBlock])
                        _logger.debug(ret)
                # register dispatch dataset
                time.sleep(1)
                disFiles = fileList[dispatchDBlock]
                _logger.debug(('registerNewDataset',dispatchDBlock,disFiles['lfns'],disFiles['guids'],
                               disFiles['fsizes'],disFiles['chksums']))
                for iDDMTry in range(3):
                    status,out = ddm.DQ2.main('registerNewDataset',dispatchDBlock,disFiles['lfns'],disFiles['guids'],
                              disFiles['fsizes'],disFiles['chksums'])
                    if status != 0 and out.find('DQDatasetExistsException') != -1:
                        break
                    elif status != 0 or out.find("DQ2 internal server exception") != -1 \
                             or out.find("An error occurred on the central catalogs") != -1 \
                             or out.find("MySQL server has gone away") != -1:
                        _logger.debug("sleep %s for %s" % (iDDMTry,dispatchDBlock))
                        _logger.debug(status)
                        _logger.debug(out)
                        _logger.debug("-------------")                                                                
                        time.sleep(60)
                    else:
                        break
                _logger.debug(out)                
                if status != 0 or out.find('Error') != -1:
                    _logger.error(out)                
                    dispError[dispatchDBlock] = "Setupper._setupSource() could not register dispatchDBlock"
                    continue
                vuidStr = out
                # freezeDataset dispatch dataset
                time.sleep(1)            
                _logger.debug(('freezeDataset',dispatchDBlock))
                for iDDMTry in range(3):            
                    status,out = ddm.DQ2.main('freezeDataset',dispatchDBlock)
                    if status != 0 or out.find("DQ2 internal server exception") != -1 \
                           or out.find("An error occurred on the central catalogs") != -1 \
                           or out.find("MySQL server has gone away") != -1:
                        time.sleep(60)
                    else:
                        break
                _logger.debug(out)                
                if status != 0 or (out.find('Error') != -1 and out.find("is frozen") == -1):
                    _logger.error(out)                
                    dispError[dispatchDBlock] = "Setupper._setupSource() could not freeze dispatchDBlock"
                    continue
            else:
                # use PandaDDM
                self.dispFileList[dispatchDBlock] = fileList[dispatchDBlock]
                # create a fake vuidStr for PandaDDM
                tmpMap  = {'vuid':commands.getoutput('uuidgen')}
                vuidStr = "%s" % tmpMap
            # get VUID
            try:
                exec "vuid = %s['vuid']" % vuidStr                
                # dataset spec
                ds = DatasetSpec()
                ds.vuid = vuid
                ds.name = dispatchDBlock
                ds.type = 'dispatch'
                ds.status = 'defined'
                ds.numberfiles  = len(fileList[dispatchDBlock])/2
                ds.currentfiles = len(fileList[dispatchDBlock])/2
                dispList.append(ds)
                self.vuidMap[ds.name] = ds.vuid
            except:
                type, value, traceBack = sys.exc_info()
                _logger.error("_setupSource() : %s %s" % (type,value))
                dispError[dispatchDBlock] = "Setupper._setupSource() could not decode VUID dispatchDBlock"
        # insert datasets to DB
        _logger.debug('%s 7' % self.timestamp)                    
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
        _logger.debug('%s 8' % self.timestamp)                                    
                

    # create dataset for outputs in the repository and assign destination
    def _setupDestination(self):
        destError   = {}
        datasetList = {}
        newnameList = {}
        for job in self.jobs:
            # ignore failed jobs
            if job.jobStatus == 'failed':
                continue
            _logger.debug('%s %s in setupDestination 1' % (self.timestamp,job.PandaID))
            for file in job.Files:
                # ignore input files
                if file.type == 'input':
                    continue
                # extract destinationDBlock, destinationSE and computingSite
                dest = (file.destinationDBlock,file.destinationSE,job.computingSite,file.destinationDBlockToken)
                if not destError.has_key(dest):
                    destError[dest] = ''
                    originalName = ''
                    if job.prodSourceLabel != 'panda':
                        # get serial number
                        sn,freshFlag = self.taskBuffer.getSerialNumber(file.destinationDBlock)
                        if sn == -1:
                            destError[dest] = "Setupper._setupDestination() could not get serial num for %s" % name
                            continue
                        # new dataset name
                        newnameList[dest] = "%s_sub0%s" % (file.destinationDBlock,sn)
                        if freshFlag:
                            # register original dataset and new dataset
                            nameList = [file.destinationDBlock,newnameList[dest]]
                            originalName = file.destinationDBlock
                        else:
                            # register new dataset only
                            nameList = [newnameList[dest]]
                    else:
                        # keep original name
                        nameList = [file.destinationDBlock]
                    # create dataset
                    for name in nameList:
                        computingSite = job.computingSite
                        if name == originalName:
                            # for original dataset
                            computingSite = file.destinationSE
                        # use DQ2
                        if (not self.pandaDDM) and (job.prodSourceLabel != 'ddm'):
                            # register dataset
                            time.sleep(1)
                            _logger.debug(('registerNewDataset',name))
                            atFailed = 0
                            for iDDMTry in range(3):
                                status,out = ddm.DQ2.main('registerNewDataset',name)
                                if status != 0 and out.find('DQDatasetExistsException') != -1:
                                    atFailed = iDDMTry
                                    break
                                elif status != 0 or out.find("DQ2 internal server exception") != -1 \
                                         or out.find("An error occurred on the central catalogs") != -1 \
                                         or out.find("MySQL server has gone away") != -1:
                                    _logger.debug("sleep %s for %s" % (iDDMTry,name))
                                    _logger.debug(status)
                                    _logger.debug(out)
                                    _logger.debug("-------------")                                                                
                                    time.sleep(60)
                                else:
                                    break
                            _logger.debug(out)                                                            
                            if status != 0 or out.find('Error') != -1:
                                # unset vuidStr
                                vuidStr = ""
                                # ignore 'already exists' ERROR because original dataset may be registered by upstream.
                                # atFailed > 0 is for the case in which the first attempt succeeded but report failure
                                if (job.prodSourceLabel == 'panda' or name == originalName or atFailed > 0) and \
                                       out.find('DQDatasetExistsException') != -1:
                                    _logger.debug('ignored ERROR')
                                else:
                                    destError[dest] = "Setupper._setupDestination() could not register : %s" % name
                                    _logger.error(out)
                                    continue
                            else:
                                vuidStr = "vuid = %s['vuid']" % out
                            # conversion is needed for unknown sites
                            tmpSrcDDM = self.siteMapper.getSite(computingSite).ddm
                            tmpDstDDM = self.siteMapper.getSite(file.destinationSE).ddm
                            tmpTokenList = file.destinationDBlockToken.split(',')
                            if name == originalName or tmpSrcDDM != tmpDstDDM or \
                                   job.prodSourceLabel == 'panda' or len(tmpTokenList) > 1:
                                time.sleep(1)
                                # register location
                                dq2IDList = [self.siteMapper.getSite(computingSite).ddm]
                                # use another location when token is set
                                if not file.destinationDBlockToken in ['NULL','']:
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
                                for dq2ID in dq2IDList:
                                    _logger.debug(('registerDatasetLocation',name,dq2ID))
                                    for iDDMTry in range(3):                            
                                        status,out = ddm.DQ2.main('registerDatasetLocation',name,dq2ID)
                                        if status != 0 and out.find('DQLocationExistsException') != -1:
                                            break
                                        elif status != 0 or out.find("DQ2 internal server exception") != -1 \
                                                 or out.find("An error occurred on the central catalogs") != -1 \
                                                 or out.find("MySQL server has gone away") != -1:
                                            time.sleep(60)
                                        else:
                                            break
                                    _logger.debug(out)
                                    # ignore "already exists at location XYZ"
                                    if out.find('DQLocationExistsException') != -1:
                                        _logger.debug('ignored ERROR')
                                        status,out = 0,''
                                    # failed
                                    if status != 0 or out.find('Error') != -1:
                                        break
                            else:
                                # skip registerDatasetLocations
                                status,out = 0,''
                            if status != 0 or out.find('Error') != -1:
                                _logger.error(out)
                                destError[dest] = "Setupper._setupDestination() could not register location : %s" % name
                            elif job.prodSourceLabel == 'panda':
                                # do nothing for "panda" job
                                pass
                            elif name == originalName and job.prodSourceLabel in ['managed','test']:
                                # set metadata
                                time.sleep(1)
                                dq2ID = self.siteMapper.getSite(file.destinationSE).ddm
                                # use another location when token is set
                                if not file.destinationDBlockToken in ['NULL','']:
                                    # register only the first token becasue it is used as the location
                                    tmpFirstToken = file.destinationDBlockToken.split(',')[0] 
                                    if self.siteMapper.getSite(file.destinationSE).setokens.has_key(tmpFirstToken):
                                        dq2ID = self.siteMapper.getSite(file.destinationSE).setokens[tmpFirstToken]
                                _logger.debug(('setMetaDataAttribute',name,'origin',dq2ID))
                                for iDDMTry in range(3):
                                    status,out = ddm.DQ2.main('setMetaDataAttribute',name,'origin',dq2ID)
                                    if status != 0 or out.find("DQ2 internal server exception") != -1 \
                                           or out.find("An error occurred on the central catalogs") != -1 \
                                           or out.find("MySQL server has gone away") != -1:
                                        time.sleep(60)
                                    else:
                                        break
                                _logger.debug(out)
                                if status != 0 or (out != 'None' and out.find('already exists') == -1):
                                    _logger.error(out)
                                    destError[dest] = "Setupper._setupDestination() could not set metadata : %s" % name
                        # use PandaDDM
                        else:
                            # create a fake vuidStr for PandaDDM
                            vuidStr = 'vuid="%s"' % commands.getoutput('uuidgen')
                        # get vuid
                        if vuidStr == '':
                            _logger.debug(('queryDatasetByName',name))
                            for iDDMTry in range(3):                    
                                status,out = ddm.repositoryClient.main('queryDatasetByName',name)
                                if status != 0 or out.find("DQ2 internal server exception") != -1 \
                                       or out.find("An error occurred on the central catalogs") != -1 \
                                       or out.find("MySQL server has gone away") != -1:
                                    time.sleep(60)
                                else:
                                    break
                            _logger.debug(out)
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
                            # logging
                            if not self.pandaDDM:
                                dq2ID = self.siteMapper.getSite(file.destinationSE).ddm
                                message = '%s - siteID:%s type:output vuid:%s' % (commands.getoutput('hostname'),dq2ID,vuid)
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
            _logger.debug('%s %s in setupDestination 2' % (self.timestamp,job.PandaID))
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
            if job.jobStatus == 'failed':
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
                # use srcDQ2ID as dstDQ2ID when dst SE is same as src SE
                srcSEs = brokerage.broker_util.getSEfromSched(self.siteMapper.getSite(tmpSrcID).se)
                dstSEs = brokerage.broker_util.getSEfromSched(self.siteMapper.getSite(job.computingSite).se)
                if srcSEs == dstSEs or job.computingSite.endswith("_REPRO"):
                    dstDQ2ID = srcDQ2ID
                else:
                    dstDQ2ID = self.siteMapper.getSite(job.computingSite).ddm
                # use DQ2
                if (not self.pandaDDM) and (not srcDQ2ID in PandaDDMSource) \
                       and (job.prodSourceLabel != 'ddm') and (not job.computingSite.endswith("_REPRO")):
                    # look for replica
                    dq2ID = srcDQ2ID
                    dq2IDList = []
                    # register replica
                    if dq2ID != dstDQ2ID:
                        # make list
                        if self.replicaMap.has_key(job.dispatchDBlock):
                            # set DQ2 ID for DISK 
                            if job.cloud in ['NL']:
                                # split T1
                                diskID = 'NIKHEF-ELPROD_DATADISK'
                                tapeID = 'NIKHEF-ELPROD_DATATAPE'                                
                            else:
                                # others
                                diskID = re.sub('_MCDISK','_DATADISK',srcDQ2ID)
                                tapeID = re.sub('_MCDISK','_DATATAPE',srcDQ2ID)                                
                            for tmpDataset,tmpRepMap in self.replicaMap[job.dispatchDBlock].iteritems():
                                if tmpRepMap.has_key(srcDQ2ID):
                                    # MCDISK
                                    if not srcDQ2ID in dq2IDList:
                                        dq2IDList.append(srcDQ2ID)
                                elif tmpRepMap.has_key(diskID):
                                    # DATADISK
                                    if not diskID in dq2IDList:
                                        dq2IDList.append(diskID)
                                elif tmpRepMap.has_key(tapeID):
                                    # DATATAPE
                                    if not tapeID in dq2IDList:
                                        dq2IDList.append(tapeID)
                        # use default location if empty
                        if dq2IDList == []:
                            dq2IDList = [dq2ID]
                        for dq2ID in dq2IDList:
                            time.sleep(1)
                            _logger.debug(('registerDatasetLocation',job.dispatchDBlock,dq2ID,0,1))
                            for iDDMTry in range(3):                                            
                                status,out = ddm.DQ2.main('registerDatasetLocation',job.dispatchDBlock,dq2ID,0,1)
                                if status != 0 or out.find("DQ2 internal server exception") != -1 \
                                       or out.find("An error occurred on the central catalogs") != -1 \
                                       or out.find("MySQL server has gone away") != -1:
                                    time.sleep(60)
                                else:
                                    break
                            _logger.debug(out)
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
                            # use ATLASDATATAPE
                            seTokens = self.siteMapper.getSite(job.computingSite).setokens
                            if seTokens.has_key('ATLASDATATAPE'):
                                dq2ID = seTokens['ATLASDATATAPE']
                                # for CERN
                                if job.cloud == 'CERN' and self.replicaMap.has_key(job.dispatchDBlock):
                                    setNewIDflag = False
                                    for tmpDataset,tmpRepMap in self.replicaMap[job.dispatchDBlock].iteritems():
                                        if not tmpRepMap.has_key(dq2ID):
                                            # look for another id
                                            cernIDs = ['CERN-PROD_DAQ','CERN-PROD_TZERO']
                                            for cernID in cernIDs:
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
                            # FIXME : only NL/FR for UK emergency
                            if job.cloud in ['NL','FR']:
                                for tmpDQ2ID in dq2IDList:
                                    optSource[tmpDQ2ID] = {'policy' : 0}
                        _logger.debug(('registerDatasetSubscription',job.dispatchDBlock,dq2ID,0,0,optSub,optSource,optSrcPolicy,0,None,0,"production"))
                        for iDDMTry in range(3):                                                                
                            status,out = ddm.DQ2.main('registerDatasetSubscription',job.dispatchDBlock,dq2ID,0,0,optSub,optSource,optSrcPolicy,0,None,0,"production")
                            if status != 0 or out.find("DQ2 internal server exception") != -1 \
                                   or out.find("An error occurred on the central catalogs") != -1 \
                                   or out.find("MySQL server has gone away") != -1:
                                time.sleep(60)
                            else:
                                break
                        _logger.debug(out)                    
                        if status != 0 or out != 'None':
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
                    ddmjob.transformation    = 'http://www.usatlas.bnl.gov/svn/panda/mover/trf/run_dq2_cr'
                    ddmjob.destinationDBlock = 'testpanda.ddm.%s' % ddmjob.jobName
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
                        argStr += "-t 7200 -n 3 -s %s -r %s --guids %s --lfns %s --callBack %s -d %spanda/dis/%s%s %s" % \
                                  (srcDQ2ID,dstDQ2ID,guidStr,lfnsStr,callBackURL,destDir,
                                   time.strftime('%y/%m/%d/'),job.dispatchDBlock,job.dispatchDBlock)
                    else:
                        # prestaging jobs
                        argStr = ""
                        if moverUseTape:
                            argStr += "--useTape "
                        argStr += "-t 540 -n 2 -s %s -r %s --guids %s --lfns %s --callBack %s --prestage --cloud %s %s" % \
                                  (srcDQ2ID,dstDQ2ID,guidStr,lfnsStr,callBackURL,job.cloud,job.dispatchDBlock)
                    # set job parameters
                    ddmjob.jobParameters = argStr
                    _logger.debug('pdq2_cr %s' % ddmjob.jobParameters)
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
            if job.jobStatus=='failed':
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
        jobsWaiting   = []
        jobsFailed    = []
        jobsProcessed = []
        allLFNs  = {}
        allGUIDs = {}
        cloudMap = {}
        replicaMap = {}
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
            if job.prodDBlock == 'NULL' or job.prodDBlock.endswith('/'):
                # set cloud
                if panda_config.enableDynamicTA and job.prodSourceLabel in ['managed','validation'] \
                       and job.cloud in ['NULL',''] and (not job.taskID in [None,'NULL',0]):
                    # look into map to check if it is already gotten
                    if not cloudMap.has_key(job.taskID):
                        # instantiate TaskAssigner
                        cloudResolver = TaskAssigner.TaskAssigner(self.taskBuffer,self.siteMapper,
                                                                  job.taskID,job.prodSourceLabel)
                        # check cloud
                        _logger.debug("check cloud for %s" % job.taskID)
                        retCloud = cloudResolver.checkCloud()
                        _logger.debug("checkCloud() -> %s" % retCloud)
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
                            _logger.debug("set cloud for %s" % job.taskID)                        
                            retCloud = cloudResolver.setCloud(tmpLFNs,tmpGUIDs,metadata=job.metadata)
                            _logger.debug("setCloud() -> %s" % retCloud)
                            if retCloud == None:
                                _logger.error("failed to set cloud for %s" % job.taskID)
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
                        and file.GUID == 'NULL':
                        #and (file.GUID == 'NULL' or re.search('\.(\d+)$',file.lfn) == None):
                    datasets.append(file.dataset)
            # get LFN list
            for dataset in datasets:
                if not dataset in lfnMap.keys():
                    prodError[dataset] = ''
                    lfnMap[dataset] = {}
                    # get LFNs
                    time.sleep(1)
                    for iDDMTry in range(3):
                        _logger.debug(('listFilesInDataset',dataset))
                        status,out = ddm.DQ2.main('listFilesInDataset',dataset)
                        if status != 0 or out.find("DQ2 internal server exception") != -1 \
                               or out.find("An error occurred on the central catalogs") != -1 \
                               or out.find("MySQL server has gone away") != -1 \
                               or out == '()':
                            time.sleep(60)
                        else:
                            break
                    if status != 0 or out.startswith('Error'):
                        prodError[dataset] = 'could not get file list of prodDBlock %s' % dataset
                        _logger.error(prodError[dataset])
                        _logger.error(out)                                                                    
                    else:
                        # make map (key: LFN w/o attemptNr, value: LFN with attemptNr)
                        items = {}
                        try:
                            exec "items = %s[0]" % out
                            for guid,vals in items.iteritems():
                                valMap[vals['lfn']] = {'guid' : guid, 'fsize' : vals['filesize'],
                                                       'md5sum' : vals['checksum'],
                                                       'chksum' : vals['checksum']}
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
                            
                        except:
                            prodError[dataset] = 'could not convert HTTP-res to map for prodDBlock %s' % dataset
                            _logger.error(prodError[dataset])
                            _logger.error(out)
                    # get replica locations        
                    if self.onlyTA and prodError[dataset] == '' and (not replicaMap.has_key(dataset)):
                        for iDDMTry in range(3):
                            _logger.debug(('listDatasetReplicas',dataset))
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
            # error
            isFailed = False
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
                                                              job.taskID,job.prodSourceLabel)
                    # check cloud
                    _logger.debug("check cloud for %s" % job.taskID)
                    retCloud = cloudResolver.checkCloud()
                    _logger.debug("checkCloud() -> %s" % retCloud)
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
                        for dataset in datasets:
                            # get LFNs
                            eachDSLFNs = lfnMap[dataset].values()
                            tmpLFNs += eachDSLFNs
                            # get GUIDs
                            for oneLFN in eachDSLFNs:
                                tmpGUIDs.append(valMap[oneLFN]['guid'])
                            # locations
                            tmpReLoc[dataset] = replicaMap[dataset] 
                        # set cloud
                        _logger.debug("set cloud for %s" % job.taskID)                        
                        retCloud = cloudResolver.setCloud(tmpLFNs,tmpGUIDs,tmpReLoc,metadata=job.metadata)
                        _logger.debug("setCloud() -> %s" % retCloud)
                        if retCloud == None:
                            _logger.error("failed to set cloud for %s" % job.taskID)
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
            # replace generic LFN with real LFN
            replaceList = []
            isFailed = False
            for file in job.Files:
                if file.type == 'input' and file.dispatchDBlock == 'NULL' \
                        and file.GUID == 'NULL':
                        #and (file.GUID == 'NULL' or re.search('\.(\d+)$',file.lfn) == None):
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
                        # remove white space
                        if file.md5sum != None:
                            file.md5sum = file.md5sum.strip()
                        if file.checksum != None:
                            file.checksum = file.checksum.strip()
                    else:
                        # append job to waiting list
                        errMsg = "GUID for %s not found in DQ2" % file.lfn
                        _logger.debug(errMsg)
                        file.status = 'missing'
                        job.jobStatus    = 'failed'                        
                        job.ddmErrorCode = ErrorCode.EC_GUID
                        job.ddmErrorDiag = errMsg
                        jobsFailed.append(job)
                        isFailed = True
                        break
                    # add to allLFNs/allGUIDs
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
        _logger.debug('missLFNs %s' % missLFNs)
        # check if files in source LRC/LFC
        tmpJobList = tuple(jobsProcessed)
        for job in tmpJobList:
            # check only production/test jobs
            if not job.prodSourceLabel in ['managed','test','software']:
                continue
            missingFlag = False
            for file in job.Files:
                if file.type == 'input':
                    if missLFNs.has_key(job.cloud) and file.lfn in missLFNs[job.cloud]:
                        # set file status
                        file.status = 'missing'
                        # remove job not to process further
                        if not missingFlag:
                            missingFlag = True
                            jobsProcessed.remove(job)
                            # revert
                            for oJob in self.jobs:
                                if oJob.PandaID == job.PandaID:
                                    jobsWaiting.append(oJob)
                                    break
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
