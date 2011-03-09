'''
merge files in dataset

'''

import re
import sys
import time
import commands

import dq2.common
from dq2.clientapi import DQ2
import dq2.container.exceptions

from taskbuffer.JobSpec import JobSpec
from taskbuffer.FileSpec import FileSpec

from config import panda_config
from pandalogger.PandaLogger import PandaLogger

# logger
_logger = PandaLogger().getLogger('Merger')


class Merger:
    # constructor
    def __init__(self,taskBuffer,job):
        self.taskBuffer = taskBuffer
        self.job        = job


    # main returns None for unrecoverable 
    def run(self):
        try:
            _logger.debug("%s start" % self.job.PandaID)
            # check source label
            if not self.job.prodSourceLabel in ['user',]:
                _logger.debug("%s do nothing for non-user job" % self.job.PandaID)
                _logger.debug("%s end" % self.job.PandaID)
                return None
            # check command-line parameter
            if not "--mergeOutput" in self.job.jobParameters:
                _logger.debug("%s skip no-merge" % self.job.PandaID)
                _logger.debug("%s end" % self.job.PandaID)
                return None
            # instantiate DQ2
            self.dq2api = DQ2.DQ2()
            # get list of datasets
            dsList = []
            for tmpFile in self.job.Files:
                # use output/log
                if not tmpFile.type in ['log','output']:
                    continue
                tmpContName = tmpFile.dataset
                tmpDsName = tmpFile.destinationDBlock
                # remove _sub
                tmpDsName = re.sub('_sub\d+$','',tmpDsName)
                tmpKey = (tmpContName,tmpDsName)
                if not tmpKey in dsList:
                    dsList.append(tmpKey)
            # loop over all datasets
            mergeJobList = {}
            for tmpContName,tmpDsName in dsList:
                # check prefix
                if (not tmpDsName.startswith('user')) and (not tmpDsName.startswith('group')):
                    _logger.debug("%s ignore non-user/group DS %s" % (self.job.PandaID,tmpDsName))
                    continue
                # get list of files
                _logger.debug("%s listFilesInDataset %s" % (self.job.PandaID,tmpDsName))
                tmpAllFileMap = {}
                nTry = 3
                for iTry in range(nTry):
                    try:
                        tmpRet,tmpTimeStamp = self.dq2api.listFilesInDataset(tmpDsName)
                    except DQ2.DQUnknownDatasetException:
                        _logger.error("%s DQ2 doesn't know %s" % (self.job.PandaID,tmpDsName))
                        _logger.debug("%s end" % self.job.PandaID)
                        return None
                    except:
                        if (iTry+1) == nTry:
                            errType,errValue = sys.exc_info()[:2]
                            _logger.error("%s DQ2 failed with %s:%s to get file list for %s" % (self.job.PandaID,errType,errValue,tmpDsName))
                            _logger.debug("%s end" % self.job.PandaID)
                            return False
                        # sleep
                        time.sleep(60)
                for tmpGUID,tmpVal in tmpRet.iteritems():
                    # set GUID
                    tmpVal['guid'] = tmpGUID
                    # get type
                    tmpLFN = tmpVal['lfn']
                    tmpLFN = re.sub('\.\d+$','',tmpLFN)
                    tmpMatch = re.search('^(.+)\._\d+\.(.+)$',tmpLFN)
                    if tmpMatch == None:
                        _logger.error("%s cannot get type for %s" % (self.job.PandaID,tmpVal['lfn']))
                        _logger.debug("%s end" % self.job.PandaID)
                        return None
                    tmpType = (tmpMatch.group(1),tmpMatch.group(2),tmpContName,tmpDsName)
                    # append
                    if not tmpAllFileMap.has_key(tmpType):
                        tmpAllFileMap[tmpType] = {}
                    tmpAllFileMap[tmpType][tmpVal['lfn']] = tmpVal
                # max size of merged file     
                maxMergedFileSize = 5 * 1024 * 1024 * 1024
                # max number of files to be merged
                maxNumToBeMerged = 200
                # loop over all types
                for tmpType,tmpFileMap in tmpAllFileMap.iteritems():
                    # sort LFNs
                    tmpFileList = tmpFileMap.keys()
                    tmpFileList.sort()
                    # split by size
                    subTotalSize = 0
                    subFileList  = []
                    for tmpFileName in tmpFileList:
                        if (subTotalSize+tmpFileMap[tmpFileName]['filesize'] > maxMergedFileSize and subFileList != []) \
                               or len(subFileList) >= maxNumToBeMerged:
                            # instantiate job
                            tmpMergeJob = self.makeMergeJob(subFileList,tmpFileMap,tmpType)
                            # append
                            if not mergeJobList.has_key(tmpDsName):
                                mergeJobList[tmpDsName] = []
                            mergeJobList[tmpDsName].append(tmpMergeJob)    
                            # reset
                            subTotalSize = 0
                            subFileList = []
                        # append    
                        subTotalSize += tmpFileMap[tmpFileName]['filesize']
                        subFileList.append(tmpFileName)
                    # remaining
                    if subFileList != []:
                        # instantiate job    
                        tmpMergeJob = self.makeMergeJob(subFileList,tmpFileMap,tmpType)
                        # append
                        if not mergeJobList.has_key(tmpDsName):
                            mergeJobList[tmpDsName] = []
                        mergeJobList[tmpDsName].append(tmpMergeJob)    
            # get list of new datasets
            newDatasetMap = {}
            for tmpDsName,tmpJobList in mergeJobList.iteritems():
                # loop over all files 
                for tmpFile in tmpJobList[0].Files:
                    # ignore inputs
                    if not tmpFile.type in ['output','log']:
                        continue
                    # append
                    if not newDatasetMap.has_key(tmpFile.dataset):
                        newDatasetMap[tmpFile.dataset] = []
                    if not tmpFile.destinationDBlock in newDatasetMap[tmpFile.dataset]:
                        newDatasetMap[tmpFile.dataset].append(tmpFile.destinationDBlock)
            # remove /CN=proxy and /CN=limited from DN
            tmpRealDN = self.job.prodUserID
            tmpRealDN = re.sub('/CN=limited proxy','',tmpRealDN)
            tmpRealDN = re.sub('/CN=proxy','',tmpRealDN)
            tmpRealDN = dq2.common.parse_dn(tmpRealDN)
            # register datasets
            for tmpDsContainer,tmpNewDatasets in newDatasetMap.iteritems():
                # loop over all datasets
                for tmpNewDS in tmpNewDatasets:
                    # register
                    _logger.debug("%s registerNewDataset %s" % (self.job.PandaID,tmpNewDS))
                    nTry = 3
                    for iTry in range(nTry):
                        try:
                            self.dq2api.registerNewDataset(tmpNewDS)
                        except DQ2.DQDatasetExistsException:
                            pass
                        except:
                            if (iTry+1) == nTry:                        
                                errType,errValue = sys.exc_info()[:2]
                                _logger.error("%s DQ2 failed with %s:%s to register %s" % (self.job.PandaID,errType,errValue,tmpNewDS))
                                _logger.debug("%s end" % self.job.PandaID)
                                return False
                            # sleep
                            time.sleep(60)
                    # set owner
                    _logger.debug("%s setMetaDataAttribute %s %s" % (self.job.PandaID,tmpNewDS,tmpRealDN))
                    nTry = 3
                    for iTry in range(nTry):
                        try:
                            self.dq2api.setMetaDataAttribute(tmpNewDS,'owner',tmpRealDN)
                        except:
                            if (iTry+1) == nTry:                        
                                errType,errValue = sys.exc_info()[:2]
                                _logger.error("%s DQ2 failed with %s:%s to set owner for %s" % (self.job.PandaID,errType,errValue,tmpNewDS))
                                _logger.debug("%s end" % self.job.PandaID)
                                return False
                            # sleep
                            time.sleep(60)
                # add to container
                if tmpDsContainer.endswith('/'):
                    # add
                    _logger.debug("%s registerDatasetsInContainer %s %s" % (self.job.PandaID,tmpDsContainer,str(tmpNewDatasets)))
                    nTry = 3
                    for iTry in range(nTry):
                        try:
                            self.dq2api.registerDatasetsInContainer(tmpDsContainer,tmpNewDatasets)
                            break
                        except dq2.container.exceptions.DQContainerAlreadyHasDataset:
                            break
                        except:
                            if (iTry+1) == nTry:                        
                                errType,errValue = sys.exc_info()[:2]
                                _logger.error("%s DQ2 failed with %s:%s to add datasets to %s" % (self.job.PandaID,errType,errValue,tmpDsContainer))
                                _logger.debug("%s end" % self.job.PandaID)
                                return False
                            # sleep
                            time.sleep(60)
            # submit new jobs                 
            _logger.debug("%s submit jobs" % self.job.PandaID)                            
            # fake FQANs
            fqans = []
            if not self.job.countryGroup in ['','NULL',None]:
                fqans.append('/atlas/%s/Role=NULL' % self.job.countryGroup)
            if self.job.destinationDBlock.startswith('group') and not self.job.workingGroup in ['','NULL',None]:
                fqans.append('/atlas/%s/Role=production' % self.job.workingGroup)
            # insert jobs
            for tmpDsName,tmpJobList in mergeJobList.iteritems():
                ret = self.taskBuffer.storeJobs(tmpJobList,self.job.prodUserID,True,False,fqans,
                                                self.job.creationHost,True,checkSpecialHandling=False)
                if ret == []:
                    _logger.error("%s storeJobs failed with [] for %s" % (self.job.PandaID,tmpDsName))
                    _logger.debug("%s end" % self.job.PandaID)                    
                    return False
                else:
                    strPandaIDs = ''
                    for tmpItem in ret:
                        strPandaIDs += '%s,' % tmpItem[0]
                    _logger.debug("%s mergeJobs=%s" % (self.job.PandaID,strPandaIDs[:-1]))
            # return
            _logger.debug("%s end" % self.job.PandaID)
            return True
        except:
            errType,errValue = sys.exc_info()[:2]
            _logger.error("%s failed with %s:%s" % (self.job.PandaID,errType,errValue))
            _logger.debug("%s end" % self.job.PandaID)
            return None
        

    # make merge job
    def makeMergeJob(self,fileList,fileMap,fileType):
        # make job spec
        tmpJob = JobSpec()
        # set release and cache
        if not self.job.AtlasRelease in ['','NULL',None]:
            tmpJob.AtlasRelease  = self.job.AtlasRelease
        if not self.job.homepackage in ['','NULL',None]:
            tmpJob.homepackage   = self.job.homepackage
        tmpJob.prodSourceLabel   = 'user'
        tmpJob.prodUserID        = self.job.prodUserID
        tmpJob.assignedPriority  = 5000
        tmpJob.jobName           = 'usermerge.%s' % commands.getoutput('uuidgen')
        tmpJob.computingSite     = self.job.computingSite
        tmpJob.metadata          = self.job.metadata
        tmpJob.prodDBlock        = self.job.prodDBlock
        tmpJob.destinationDBlock = self.job.destinationDBlock
        tmpJob.destinationSE     = self.job.destinationSE
        tmpJob.cloud             = self.job.cloud
        tmpJob.cmtConfig         = self.job.cmtConfig
        tmpJob.lockedby          = self.job.lockedby
        tmpJob.processingType    = 'usermerge'
        tmpJob.jobsetID          = self.job.jobsetID
        tmpJob.jobDefinitionID   = 0
        tmpJob.transformation    = "http://pandaserver.cern.ch:25080/trf/user/runMerge-00-00-01"
        # decompose fileType
        filePrefix,fileSuffix,containerName,datasetName = fileType
        # output dataset name
        outDsName = datasetName+'.merge'
        # job parameter
        params = '--parentDS %s --parentContainer %s --outDS %s' % (datasetName,containerName,outDsName)
        # look for lib.tgz
        for tmpFile in self.job.Files:
            if tmpFile.type == 'input' and tmpFile.lfn.endswith('.lib.tgz'):
                tmpJob.addTmpFile(tmpFile)
                params += " --libTgz %s" % tmpFile.lfn
                break
        # input
        inStr = ' --in '
        serNum = None
        for tmpFileName in fileList:
            # extract serial number
            if serNum == None:
                tmpMatch = re.search('^'+filePrefix+'\.(_\d+)\.'+fileSuffix,tmpFileName)
                if tmpMatch == None:
                    raise RuntimeError,'cannot extract SN from %s' % tmpFileName
                serNum = tmpMatch.group(1)
            # make file spec
            tmpFile = FileSpec()
            vals = fileMap[tmpFileName]
            tmpFile.lfn        = tmpFileName
            tmpFile.GUID       = vals['guid']
            tmpFile.fsize      = vals['filesize']
            tmpFile.md5sum     = vals['checksum']
            tmpFile.checksum   = vals['checksum']            
            tmpFile.dataset    = containerName
            tmpFile.prodDBlock = tmpFile.dataset
            tmpFile.type       = 'input'
            tmpFile.status     = 'ready'
            tmpJob.addFile(tmpFile)
            inStr += '%s,' % tmpFile.lfn
        params += inStr[:-1]    
        # output
        tmpFile = FileSpec()
        tmpFile.lfn = "%s.%s.merge.%s" % (filePrefix,serNum,fileSuffix)
        tmpFile.destinationDBlock = outDsName
        tmpFile.destinationSE     = self.job.destinationSE
        tmpFile.dataset           = containerName
        tmpFile.type = 'output'
        tmpJob.addFile(tmpFile)
        params += " --outFile %s" % tmpFile.lfn
        # log
        tmpFile = FileSpec()
        tmpFile.lfn = '%s._$PANDAID.log.tgz' % filePrefix
        tmpFile.destinationDBlock = outDsName
        tmpFile.destinationSE     = self.job.destinationSE
        tmpFile.dataset           = containerName
        tmpFile.type = 'log'
        tmpJob.addFile(tmpFile)
        # set job parameter
        tmpJob.jobParameters = params
        # return
        return tmpJob
