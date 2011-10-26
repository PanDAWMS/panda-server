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
    def __init__(self,taskBuffer,job,simulFlag=False):
        self.taskBuffer   = taskBuffer
        self.job          = job
        self.mergeType    = ""
        self.mergeScript  = ""
        self.runDir       = "."
        self.mergeTypeMap = {}        
        self.supportedMergeType = ['hist','ntuple','pool','user','log','text']
        self.simulFlag    = simulFlag


    # parse jobParameters and get mergeType specified by the client
    def getMergeType(self):
        type = ""
        try:
            paramList = re.split('\W+',self.job.jobParameters.strip())
            type = paramList[ paramList.index('mergeType') + 1 ]
        except:
            _logger.debug("%s cannot find --mergeType parameter from parent job" % self.job.PandaID)
        return type


    # parse jobParameters and get mergeScript specified by the client
    def getUserMergeScript(self):
        script = ""
        try:
            match = re.search("--mergeScript\s(([^\'\"\s]+)|(\"[^\"]+\")|(\'[^\']+\'))",self.job.jobParameters)
            if match != None:
                script = match.group(1)
        except:
            _logger.debug("%s cannot find --mergeScript parameter from parent job" % self.job.PandaID)
        return script

    # parse jobParameters and get rundir specified by the client
    def getRunDir(self):
        rundir = "."
        try:
            m = re.match(r'.*\-r\s+(\S+)\s+.*', self.job.jobParameters.strip())
            if m:
                rundir = re.sub(r'[\'"]','',m.group(1))
        except:
            _logger.debug("%s cannot find -r parameter from parent job" % self.job.PandaID)
        return rundir


    # get file type
    def getFileType(self,tmpLFN):
        tmpLFN = re.sub('\.\d+$','',tmpLFN)
        tmpMatch = re.search('^(.+)\._\d+\.(.+)$',tmpLFN)
        if tmpMatch != None:
            return (tmpMatch.group(1),tmpMatch.group(2))
        return None
    
        
    # parse jobSpec to get merge type automatically
    def getMergeTypeAuto(self):
        # look for outmap
        try:
            tmpMatch = re.search('-o \"([^\"]+)\"',self.job.jobParameters)
            outMapStr = tmpMatch.group(1)
            exec "outMap="+outMapStr
        except:
            errType,errValue = sys.exc_info()[:2]            
            _logger.debug("%s cannot extract outMap from jobParameters=%s %s:%s" % \
                          (self.job.PandaID,self.job.jobParameters,errType,errValue))
            return False
        # convert output type to merge type
        if '/runGen-' in self.job.transformation:
            # loop over all output files for runGen
            for oldName,newName in outMap.iteritems():
                # get file type
                tmpKey = self.getFileType(newName)
                if tmpKey != None:
                    # check extension
                    if re.search('\.pool\.root(\.\d+)*$',newName) != None:
                        # POOL
                        tmpType = 'pool'
                    elif re.search('\.root(\.\d+)*$',newName) != None:
                        # map all root files to ntuple
                        tmpType = 'ntuple'
                    else:
                        # catch all using zip
                        tmpType = 'text'                        
                    # append
                    self.mergeTypeMap[tmpKey] = tmpType
        else:
            # hist
            if outMap.has_key('hist'):
                tmpType = 'hist'
                tmpKey = self.getFileType(outMap['hist'])
                if tmpKey != None:
                    # append
                    self.mergeTypeMap[tmpKey] = tmpType
            # ntuple        
            if outMap.has_key('ntuple'):
                tmpType = 'ntuple'
                for sName,fName in outMap['ntuple']:
                    tmpKey = self.getFileType(fName)
                    if tmpKey != None:
                        # append
                        self.mergeTypeMap[tmpKey] = tmpType
            # AANT
            if outMap.has_key('AANT'):
                # map AANT to ntuple for now
                tmpType = 'ntuple'
                for aName,sName,fName in outMap['AANT']:
                    tmpKey = self.getFileType(fName)
                    if tmpKey != None:
                        # append
                        self.mergeTypeMap[tmpKey] = tmpType
            # THIST
            if outMap.has_key('THIST'):
                tmpType = 'ntuple'
                for aName,fName in outMap['THIST']:
                    tmpKey = self.getFileType(fName)
                    if tmpKey != None:
                        # append only when the stream is not used by AANT
                        if not self.mergeTypeMap.has_key(tmpKey):
                            self.mergeTypeMap[tmpKey] = tmpType
            # POOL
            for tmpOutType,tmpOutVal in outMap.iteritems():
                # TAG is mapped to POOL for now
                if tmpOutType in ['RDO','ESD','AOD','TAG','Stream1','Stream2']:
                    tmpType = 'pool'
                    tmpKey = self.getFileType(tmpOutVal)
                    if tmpKey != None:
                        # append
                        self.mergeTypeMap[tmpKey] = tmpType
            # general POOL stream
            if outMap.has_key('StreamG'):
                tmpType = 'pool'
                for sName,fName in outMap['StreamG']:
                    tmpKey = self.getFileType(fName)
                    if tmpKey != None:
                        # append
                        self.mergeTypeMap[tmpKey] = tmpType
            # meta
            if outMap.has_key('Meta'):
                tmpType = 'pool'
                for sName,fName in outMap['Meta']:
                    tmpKey = self.getFileType(fName)
                    if tmpKey != None:
                        # append only when the stream is not used by another
                        if not self.mergeTypeMap.has_key(tmpKey):
                            self.mergeTypeMap[tmpKey] = tmpType
            # UserData
            if outMap.has_key('UserData'):
                tmpType = 'pool'
                for fName in outMap['UserData']:
                    tmpKey = self.getFileType(fName)
                    if tmpKey != None:
                        # append
                        self.mergeTypeMap[tmpKey] = tmpType
            # BS
            if outMap.has_key('BS'):
                # ByteStream is mapped to text to use zip for now
                tmpType = 'text'
                tmpKey = self.getFileType(outMap['BS'])
                if tmpKey != None:
                    # append
                    self.mergeTypeMap[tmpKey] = tmpType
            # extra outputs
            if outMap.has_key('IROOT'):
                for oldName,newName in outMap['IROOT']:
                    tmpKey = self.getFileType(newName)
                    if tmpKey != None:
                        # check extension
                        if re.search('\.pool\.root(\.\d+)*$',newName) != None:
                            # POOL
                            tmpType = 'pool'
                        elif re.search('\.root(\.\d+)*$',newName) != None:
                            # map all root files to ntuple
                            tmpType = 'ntuple'
                        else:
                            # catch all using zip
                            tmpType = 'text'                        
                        # append
                        self.mergeTypeMap[tmpKey] = tmpType
        # dump                
        _logger.debug("%s automatic merge type mapping -> %s" % (self.job.PandaID,str(self.mergeTypeMap)))
        return True
            

    # detect merge type with LFN prefix and suffix
    def detectMergeTypeWithLFN(self,filePrefix,fileSuffix):
        tmpKey = (filePrefix,fileSuffix)
        if self.mergeTypeMap.has_key(tmpKey):
            return self.mergeTypeMap[tmpKey]
        # look for matching fileSuffix mainly for --useContElement which has differed prefix
        for tmpKey in self.mergeTypeMap.keys():
            tmpFilePrefix,tmpFileSuffix = tmpKey
            if tmpFileSuffix == fileSuffix:
                _logger.debug("%s updated merge type mapping for %s:%s -> %s" % (self.job.PandaID,filePrefix,fileSuffix,str(self.mergeTypeMap)))
                self.mergeTypeMap[(filePrefix,fileSuffix)] = self.mergeTypeMap[tmpKey]
                return self.mergeTypeMap[tmpKey]
        raise RuntimeError,'cannot find merge type for %s %s' % (filePrefix,fileSuffix)
    
                        
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
            if not self.simulFlag and not "--mergeOutput" in self.job.jobParameters:
                _logger.debug("%s skip no-merge" % self.job.PandaID)
                _logger.debug("%s end" % self.job.PandaID)
                return None

            # get mergeType from jobParams
            self.mergeType   = self.getMergeType()
            self.mergeScript = self.getUserMergeScript()

            # if mergeScript is given by user, it's equivalent to user mode mergeType
            if self.mergeScript:
                self.mergeType = 'user'

            if self.mergeType != '':
                # check if the merging type is given and is supported
                if self.mergeType not in self.supportedMergeType:
                    _logger.error("%s skip not supported merging type \"%s\"" % (self.job.PandaID, self.mergeType))
                    _logger.debug("%s end" % self.job.PandaID)
                    return None
                elif self.mergeType in ['user']:
                    self.runDir      = self.getRunDir()
                    if not self.mergeScript:
                        _logger.error("%s skip: no merging command specified for merging type \"%s\"" % (self.job.PandaID, self.mergeType))
                        _logger.debug("%s end" % self.job.PandaID)
                        return None
            else:
                # automatic merge type detection
                tmpRet = self.getMergeTypeAuto()
                if not tmpRet:
                    _logger.error("%s failed to detect merge type automatically" % self.job.PandaID)
                    _logger.debug("%s end" % self.job.PandaID)
                    return None
            # instantiate DQ2
            self.dq2api = DQ2.DQ2()
            # get list of datasets
            dsList = []
            dsSubDsMap = {}
            for tmpFile in self.job.Files:
                # use output/log
                if not tmpFile.type in ['log','output']:
                    continue
                tmpContName = tmpFile.dataset
                tmpSubDsName = tmpFile.destinationDBlock
                # remove _sub
                tmpDsName = re.sub('_sub\d+$','',tmpSubDsName)
                tmpKey = (tmpContName,tmpDsName)
                if not tmpKey in dsList:
                    dsList.append(tmpKey)
                    dsSubDsMap[tmpDsName] = tmpSubDsName
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
                        tmpRetTimeStamp = self.dq2api.listFilesInDataset(tmpDsName)
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
                # empty
                if tmpRetTimeStamp == ():
                    # close dataset
                    varMap = {}
                    varMap[':name'] = tmpDsName
                    varMap[':status'] = 'tobeclosed'
                    uSQL  = "UPDATE /*+ INDEX(tab DATASETS_NAME_IDX)*/ ATLAS_PANDA.Datasets "
                    uSQL += "SET status=:status,modificationdate=CURRENT_DATE WHERE name=:name "
                    self.taskBuffer.querySQLS(uSQL,varMap)
                    _logger.debug("%s %s is empty" % (self.job.PandaID,tmpDsName))                    
                    continue
                # loop over all GUIDs
                tmpRet,tmpTimeStamp = tmpRetTimeStamp        
                for tmpGUID,tmpVal in tmpRet.iteritems():
                    # set GUID
                    tmpVal['guid'] = tmpGUID
                    # get type
                    tmpMatch = self.getFileType(tmpVal['lfn'])
                    if tmpMatch == None:
                        _logger.error("%s cannot get type for %s" % (self.job.PandaID,tmpVal['lfn']))
                        _logger.debug("%s end" % self.job.PandaID)
                        return None
                    tmpType = (tmpMatch[0],tmpMatch[1],tmpContName,tmpDsName)
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
            # terminate simulation
            if self.simulFlag:
                _logger.debug("%s end simulation" % self.job.PandaID)
                return True
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
                    # set jobDefID
                    tmpJobDefID = ret[0][1]
                    if not tmpJobDefID in ['NULL','',None,-1]:
                        varMap = {}
                        varMap[':name'] = dsSubDsMap[tmpDsName]
                        varMap[':moverID'] = tmpJobDefID
                        uSQL  = "UPDATE /*+ INDEX(tab DATASETS_NAME_IDX)*/ ATLAS_PANDA.Datasets "
                        uSQL += "SET moverID=:moverID WHERE name=:name "
                        self.taskBuffer.querySQLS(uSQL,varMap)
                    # dump    
                    strPandaIDs = ''
                    for tmpItem in ret:
                        strPandaIDs += '%s,' % tmpItem[0]
                    _logger.debug("%s jobDefID=%s mergeJobs=%s" % (self.job.PandaID,tmpJobDefID,strPandaIDs[:-1]))
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
        for tmpLibFile in self.job.Files:
            if tmpLibFile.type == 'input' and tmpLibFile.lfn.endswith('.lib.tgz'):
                tmpFile = FileSpec()
                tmpFile.lfn        = tmpLibFile.lfn
                tmpFile.GUID       = tmpLibFile.GUID
                tmpFile.fsize      = tmpLibFile.fsize
                tmpFile.md5sum     = tmpLibFile.md5sum
                tmpFile.checksum   = tmpLibFile.checksum
                tmpFile.dataset    = tmpLibFile.dataset
                tmpFile.prodDBlock = tmpLibFile.prodDBlock
                tmpFile.type       = 'input'
                tmpFile.status     = 'ready'
                tmpFile.prodDBlockToken = 'local'                
                tmpJob.addFile(tmpFile)
                params += " --libTgz %s" % tmpFile.lfn
                break
        # input
        serNum = None
        attNum = None
        for tmpFileName in fileList:
            # extract serial number
            if serNum == None:
                tmpMatch = re.search('^'+filePrefix+'\.(_\d+)\.'+fileSuffix,tmpFileName)
                if tmpMatch == None:
                    raise RuntimeError,'cannot extract SN from %s' % tmpFileName
                serNum = tmpMatch.group(1)
                # extract attempt number
                tmpMatch = re.search('\.(\d+)$',tmpFileName)
                if tmpMatch != None:
                    attNum = tmpMatch.group(1)
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
            tmpFile.prodDBlockToken = 'local'
            tmpJob.addFile(tmpFile)
            
        # merge type determination
        if fileSuffix.endswith('log.tgz'):
            # log
            usedMergeType = 'log'
        elif self.mergeType != '':
            # user specified merging type
            usedMergeType = self.mergeType
        else:
            # auto detection
            usedMergeType = self.detectMergeTypeWithLFN(filePrefix,fileSuffix)
            
        if usedMergeType in ['user']:
            ## run user mode merging given the merging script
            params += ' -j %s -r %s' % (self.mergeScript, self.runDir)

        params += " -t %s" % usedMergeType
        params += " -i \"%s\"" % repr(fileList)
        
        # output
        tmpFile = FileSpec()
        if attNum == None:
            tmpFile.lfn = "%s.%s.merge.%s" % (filePrefix,serNum,fileSuffix)
        else:
            tmpFile.lfn = "%s.%s.%s.merge.%s" % (filePrefix,serNum,attNum,fileSuffix)
            
        if usedMergeType == 'text' and \
           not tmpFile.lfn.endswith('.tgz') and \
           not tmpFile.lfn.endswith('.tar.gz'):
            tmpFile.lfn += '.tgz'
        tmpFile.destinationDBlock = outDsName
        tmpFile.destinationSE     = self.job.destinationSE
        tmpFile.dataset           = containerName
        tmpFile.type = 'output'
        tmpJob.addFile(tmpFile)
        params += ' -o "%s"' % tmpFile.lfn
        # log
        tmpItems = filePrefix.split('.')
        if len(tmpItems) > 3:
            logPrefix = "%s.%s.%s" % tuple(tmpItems[:3])
        else:
            logPrefix = filePrefix
        tmpFile = FileSpec()
        tmpFile.lfn = '%s._$PANDAID.log.tgz' % logPrefix
        tmpFile.destinationDBlock = outDsName
        tmpFile.destinationSE     = self.job.destinationSE
        tmpFile.dataset           = containerName
        tmpFile.type = 'log'
        tmpJob.addFile(tmpFile)
        # set job parameter
        tmpJob.jobParameters = params
        if self.simulFlag:
            _logger.debug("%s prams %s" % (self.job.PandaID,tmpJob.jobParameters))
        # return
        return tmpJob
