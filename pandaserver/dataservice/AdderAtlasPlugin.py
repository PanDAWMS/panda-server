'''
add data to dataset

'''

import os
import re
import sys
import time
import copy
import fcntl
import datetime
import commands
import exceptions
import traceback
import xml.dom.minidom
import ErrorCode
from rucio.common.exception import FileConsistencyMismatch,DataIdentifierNotFound,UnsupportedOperation,\
    InvalidPath,RSENotFound,InsufficientAccountLimit,RSEProtocolNotSupported

from DDM import rucioAPI

from config import panda_config
from pandalogger.PandaLogger import PandaLogger
from AdderPluginBase import AdderPluginBase
from taskbuffer import EventServiceUtils
from MailUtils import MailUtils
import DataServiceUtils

   
class AdderAtlasPlugin (AdderPluginBase):
    # constructor
    def __init__(self,job,**params):
        AdderPluginBase.__init__(self,job,params)
        self.jobID = self.job.PandaID
        self.jobStatus = self.job.jobStatus
        self.datasetMap = {}
        self.addToTopOnly = False
        self.goToTransferring = False
        self.logTransferring = False
        self.subscriptionMap = {}
        self.pandaDDM = False
        self.goToMerging = False

        
    # main
    def execute(self):
        try:
            self.logger.debug("start plugin : %s" % self.jobStatus)
            # backend
            self.ddmBackEnd = self.job.getDdmBackEnd()
            if self.ddmBackEnd == None:
                self.ddmBackEnd = 'rucio'
            self.logger.debug("ddm backend = {0}".format(self.ddmBackEnd))
            # add files only to top-level datasets for transferring jobs
            if self.job.jobStatus == 'transferring':
                self.addToTopOnly = True
                self.logger.debug("adder for transferring")
            # use PandaDDM for ddm jobs
            if self.job.prodSourceLabel == 'ddm':
                self.pandaDDM = True
            # check if the job goes to merging
            if self.job.produceUnMerge():
                self.goToMerging = True
            # check if the job should go to trasnferring
            srcSiteSpec = self.siteMapper.getSite(self.job.computingSite)
            tmpSrcDDM = srcSiteSpec.ddm
            destSEwasSet = False
            brokenSched = False
            if self.job.prodSourceLabel == 'user' and not self.siteMapper.siteSpecList.has_key(self.job.destinationSE):
                # DQ2 ID was set by using --destSE for analysis job to transfer output
                destSEwasSet = True
                tmpDstDDM = self.job.destinationSE
            else:
                dstSiteSpec = self.siteMapper.getSite(self.job.destinationSE)
                tmpDstDDM = dstSiteSpec.ddm
                # protection against disappearance of dest from schedconfig
                if not self.siteMapper.checkSite(self.job.destinationSE) and self.job.destinationSE != 'local':
                    self.job.ddmErrorCode = ErrorCode.EC_Adder
                    self.job.ddmErrorDiag = "destinaitonSE %s is unknown in schedconfig" % self.job.destinationSE
                    self.logger.error("%s" % self.job.ddmErrorDiag)
                    # set fatal error code and return
                    self.result.setFatal()
                    return 
            # protection against disappearance of src from schedconfig        
            if not self.siteMapper.checkSite(self.job.computingSite):
                self.job.ddmErrorCode = ErrorCode.EC_Adder
                self.job.ddmErrorDiag = "computingSite %s is unknown in schedconfig" % self.job.computingSite
                self.logger.error("%s" % self.job.ddmErrorDiag)
                # set fatal error code and return
                self.result.setFatal()
                return
            # check if the job has something to transfer
            self.logger.debug('alt stage-out:{0}'.format(str(self.job.altStgOutFileList())))
            somethingToTranfer = False
            for file in self.job.Files:
                if file.type == 'output' or file.type == 'log':
                    if DataServiceUtils.getDistributedDestination(file.destinationDBlockToken) == None \
                            and not file.lfn in self.job.altStgOutFileList():
                        somethingToTranfer = True
                        break
            self.logger.debug('DDM src:%s dst:%s' % (tmpSrcDDM,tmpDstDDM))
            if re.search('^ANALY_',self.job.computingSite) != None:
                # analysis site
                pass
            elif self.job.computingSite == self.job.destinationSE:
                # same site ID for computingSite and destinationSE
                pass
            elif tmpSrcDDM == tmpDstDDM:
                # same DQ2ID for src/dest
                pass
            elif self.addToTopOnly:
                # already in transferring
                pass
            elif self.goToMerging:
                # no transferring for merging
                pass
            elif self.job.jobStatus == 'failed':
                # failed jobs
                if self.job.prodSourceLabel in ['managed','test']:
                    self.logTransferring = True
            elif self.job.jobStatus == 'finished' and EventServiceUtils.isEventServiceJob(self.job) \
                    and not EventServiceUtils.isJobCloningJob(self.job):
                # transfer only log file for normal ES jobs 
                self.logTransferring = True
            elif not somethingToTranfer:
                # nothing to transfer
                pass
            else:
                self.goToTransferring = True
            self.logger.debug('somethingToTranfer={0}'.format(somethingToTranfer))
            self.logger.debug('goToTransferring=%s' % self.goToTransferring)
            self.logger.debug('logTransferring=%s' % self.logTransferring)
            self.logger.debug('goToMerging=%s' % self.goToMerging)
            retOut = self._updateOutputs()
            self.logger.debug('added outputs with %s' % retOut)
            if retOut != 0:
                self.logger.debug('terminated when adding')
                return
            # succeeded    
            self.result.setSucceeded()    
            self.logger.debug("end plugin")
        except:
            errtype,errvalue = sys.exc_info()[:2]
            errStr = "execute() : %s %s" % (errtype,errvalue)
            errStr += traceback.format_exc()
            self.logger.debug(errStr)
            # set fatal error code
            self.result.setFatal()
        # return
        return


    # update output files
    def _updateOutputs(self):
        # return if non-DQ2
        if self.pandaDDM or self.job.destinationSE == 'local':
            return 0
        # get campaign
        campaign = None
        if not self.job.jediTaskID in [0,None,'NULL']:
            tmpRet = self.taskBuffer.getTaskAttributesPanda(self.job.jediTaskID,['campaign'])
            if 'campaign' in tmpRet:
                campaign = tmpRet['campaign']
        # zip file map
        zipFileMap = self.job.getZipFileMap()
        # check files
        idMap = {}
        fileList = []
        subMap = {}        
        dsDestMap = {}
        distDSs = set()
        osDsFileMap = {}
        zipFiles = {}
        contZipMap = {}
        for file in self.job.Files:
            if file.type == 'output' or file.type == 'log':
                # append to fileList
                fileList.append(file.lfn)
                # add only log file for failed jobs
                if self.jobStatus == 'failed' and file.type != 'log':
                    continue
                # add only log file for successful ES jobs
                if self.job.jobStatus == 'finished' and EventServiceUtils.isEventServiceJob(self.job) \
                        and not EventServiceUtils.isJobCloningJob(self.job) and file.type != 'log':
                    continue
                # skip no output or failed
                if file.status in ['nooutput','failed']:
                    continue
                # check if zip file
                if file.lfn in zipFileMap:
                    isZipFile = True
                    if not file.lfn in zipFiles and not self.addToTopOnly:
                        zipFiles[file.lfn] = dict()
                else:
                    isZipFile = False
                # skip zip files wjen topOnly
                if self.addToTopOnly and isZipFile:
                    continue
                # check if zip content
                zipFileName = None
                if not isZipFile and not self.addToTopOnly:
                    for tmpZipFileName, tmpZipContents in zipFileMap.iteritems():
                        for tmpZipContent in tmpZipContents:
                            if re.search('^'+tmpZipContent+'$', file.lfn) is not None:
                                zipFileName = tmpZipFileName
                                break
                        if zipFileName is not None:
                            break
                    if zipFileName is not None:
                        if zipFileName not in zipFiles:
                            zipFiles[zipFileName] = dict()
                        contZipMap[file.lfn] = zipFileName
                try:
                    # fsize
                    fsize = None
                    if not file.fsize in ['NULL','',0]:
                        try:
                            fsize = long(file.fsize)
                        except:
                            type, value, traceBack = sys.exc_info()
                            self.logger.error("%s : %s %s" % (self.jobID,type,value))
                    # use top-level dataset name for alternative stage-out
                    if not file.lfn in self.job.altStgOutFileList():
                        fileDestinationDBlock = file.destinationDBlock
                    else:
                        fileDestinationDBlock = re.sub('_sub\d+$','',file.destinationDBlock)
                    # append to map
                    if not idMap.has_key(fileDestinationDBlock) and not isZipFile:
                        idMap[fileDestinationDBlock] = []
                    fileAttrs = {'guid'     : file.GUID,
                                 'lfn'      : file.lfn,
                                 'size'     : fsize,
                                 'checksum' : file.checksum,
                                 'ds'       : fileDestinationDBlock}
                    # add SURLs if LFC registration is required
                    if self.useCentralLFC():
                        fileAttrs['surl'] = self.extraInfo['surl'][file.lfn]
                        if fileAttrs['surl'] == None:
                            del fileAttrs['surl']
                        # get destination
                        if not dsDestMap.has_key(fileDestinationDBlock):
                            toConvert = True
                            if file.lfn in self.job.altStgOutFileList():
                                toConvert = False
                                # alternative stage-out
                                if DataServiceUtils.getDestinationSE(file.destinationDBlockToken) != None:
                                    # RSE is specified
                                    tmpDestList = [DataServiceUtils.getDestinationSE(file.destinationDBlockToken)]
                                else:
                                    if self.siteMapper.getSite(file.destinationSE).setokens.has_key(file.destinationDBlockToken):
                                        # get endpoint for token
                                        tmpDestList = [self.siteMapper.getSite(file.destinationSE).setokens[file.destinationDBlockToken]]
                                    else:
                                        # use defalt endpoint
                                        tmpDestList = [self.siteMapper.getSite(file.destinationSE).ddm]
                            elif file.destinationDBlockToken in ['',None,'NULL']:
                                # use defalt endpoint
                                tmpDestList = [self.siteMapper.getSite(self.job.computingSite).ddm]
                            elif DataServiceUtils.getDestinationSE(file.destinationDBlockToken) != None and \
                                    self.siteMapper.getSite(self.job.computingSite).ddm == self.siteMapper.getSite(file.destinationSE).ddm:
                                tmpDestList = [DataServiceUtils.getDestinationSE(file.destinationDBlockToken)]
                                # RSE is specified
                                toConvert = False
                            elif DataServiceUtils.getDistributedDestination(file.destinationDBlockToken) != None:
                                tmpDestList = [DataServiceUtils.getDistributedDestination(file.destinationDBlockToken)]
                                distDSs.add(fileDestinationDBlock)
                                # RSE is specified for distributed datasets
                                toConvert = False
                            elif self.siteMapper.getSite(self.job.computingSite).cloud != self.job.cloud and \
                                    (not self.siteMapper.getSite(self.job.computingSite).ddm.endswith('PRODDISK')) and  \
                                    (not self.job.prodSourceLabel in ['user','panda']):
                                # T1 used as T2
                                tmpDestList = [self.siteMapper.getSite(self.job.computingSite).ddm]
                            else:
                                tmpDestList = []
                                tmpSeTokens = self.siteMapper.getSite(self.job.computingSite).setokens
                                for tmpDestToken in file.destinationDBlockToken.split(','):
                                    if tmpSeTokens.has_key(tmpDestToken):
                                        tmpDest = tmpSeTokens[tmpDestToken]
                                    else:
                                        tmpDest = self.siteMapper.getSite(self.job.computingSite).ddm
                                    if not tmpDest in tmpDestList:
                                        tmpDestList.append(tmpDest)
                            # add
                            dsDestMap[fileDestinationDBlock] = tmpDestList
                    # extra meta data
                    if self.ddmBackEnd == 'rucio':
                        if file.lfn in self.extraInfo['lbnr']:
                            fileAttrs['lumiblocknr'] = self.extraInfo['lbnr'][file.lfn]
                        if file.lfn in self.extraInfo['nevents']:
                            fileAttrs['events'] = self.extraInfo['nevents'][file.lfn]
                        elif self.extraInfo['nevents'] != {}:
                            fileAttrs['events'] = None
                        #if not file.jediTaskID in [0,None,'NULL']:
                        #    fileAttrs['task_id'] = file.jediTaskID
                        fileAttrs['panda_id'] = file.PandaID
                        if not campaign in ['',None]:
                            fileAttrs['campaign'] = campaign
                    # extract OS files
                    hasNormalURL = True
                    if file.lfn in self.extraInfo['endpoint'] and self.extraInfo['endpoint'][file.lfn] != []:
                        # hasNormalURL = False # FIXME once the pilot chanages to send srm endpoints in addition to OS
                        for pilotEndPoint in self.extraInfo['endpoint'][file.lfn]:
                            # pilot uploaded to endpoint consistently with original job definition
                            if pilotEndPoint in dsDestMap[fileDestinationDBlock]:
                                hasNormalURL = True
                            else:
                                # uploaded to S3
                                if not pilotEndPoint in osDsFileMap:
                                    osDsFileMap[pilotEndPoint] = {}
                                osFileDestinationDBlock = re.sub('_sub\d+$','',fileDestinationDBlock)
                                if not osFileDestinationDBlock in osDsFileMap[pilotEndPoint]:
                                    osDsFileMap[pilotEndPoint][osFileDestinationDBlock] = []
                                copiedFileAttrs = copy.copy(fileAttrs)
                                del copiedFileAttrs['surl']
                                osDsFileMap[pilotEndPoint][osFileDestinationDBlock].append(copiedFileAttrs)
                    if hasNormalURL:
                        if not isZipFile:
                            # add file to be added to dataset
                            idMap[fileDestinationDBlock].append(fileAttrs)
                            # add file to be added to zip
                            if zipFileName is not None:
                                if not 'files' in zipFiles[zipFileName]:
                                    zipFiles[zipFileName]['files'] = []
                                zipFiles[zipFileName]['files'].append(fileAttrs)
                        else:
                            # copy file attribute for zip file registration
                            for tmpFileAttrName, tmpFileAttrVal in fileAttrs.iteritems():
                                zipFiles[file.lfn][tmpFileAttrName] = tmpFileAttrVal
                            zipFiles[file.lfn]['scope'] = file.scope
                            zipFiles[file.lfn]['rse'] = dsDestMap[fileDestinationDBlock]
                            continue
                    # for subscription
                    if self.job.prodSourceLabel in ['managed','test','software','rc_test','ptest','user','rucio_test'] and \
                           re.search('_sub\d+$',fileDestinationDBlock) != None and (not self.addToTopOnly) and \
                           self.job.destinationSE != 'local':
                        if self.siteMapper == None:
                            self.logger.error("SiteMapper==None")
                        else:
                            # get dataset spec
                            if not self.datasetMap.has_key(fileDestinationDBlock):
                                tmpDS = self.taskBuffer.queryDatasetWithMap({'name':fileDestinationDBlock})
                                self.datasetMap[fileDestinationDBlock] = tmpDS
                            # check if valid dataset        
                            if self.datasetMap[fileDestinationDBlock] == None:
                                self.logger.error(": cannot find %s in DB" % fileDestinationDBlock)
                            else:
                                if not self.datasetMap[fileDestinationDBlock].status in ['defined']:
                                    # not a fresh dataset
                                    self.logger.debug(": subscription was already made for %s:%s" % \
                                                  (self.datasetMap[fileDestinationDBlock].status,
                                                   fileDestinationDBlock))
                                else:
                                    # get DQ2 IDs
                                    srcSiteSpec = self.siteMapper.getSite(self.job.computingSite)
                                    tmpSrcDDM = srcSiteSpec.ddm
                                    if self.job.prodSourceLabel == 'user' and not self.siteMapper.siteSpecList.has_key(file.destinationSE):
                                        # DQ2 ID was set by using --destSE for analysis job to transfer output
                                        tmpDstDDM = file.destinationSE
                                    else:
                                        if DataServiceUtils.getDestinationSE(file.destinationDBlockToken) != None:
                                            tmpDstDDM = DataServiceUtils.getDestinationSE(file.destinationDBlockToken)
                                        else:
                                            tmpDstDDM = self.siteMapper.getSite(file.destinationSE).ddm
                                    # if src != dest or multi-token
                                    if (tmpSrcDDM != tmpDstDDM) or \
                                       (tmpSrcDDM == tmpDstDDM and file.destinationDBlockToken.count(',') != 0):
                                        optSub = {'DATASET_COMPLETE_EVENT' : ['http://%s:%s/server/panda/datasetCompleted' % \
                                                                              (panda_config.pserverhosthttp,panda_config.pserverporthttp)]}
                                        # append
                                        if not subMap.has_key(fileDestinationDBlock):
                                            subMap[fileDestinationDBlock] = []
                                            # sources
                                            optSource = {}
                                            # set sources
                                            if file.destinationDBlockToken in ['NULL','']:
                                                # use default DQ2 ID as source
                                                optSource[tmpSrcDDM] = {'policy' : 0}
                                            else:
                                                # convert token to DQ2 ID
                                                dq2ID = tmpSrcDDM
                                                # use the first token's location as source for T1D1
                                                tmpSrcToken = file.destinationDBlockToken.split(',')[0]
                                                if self.siteMapper.getSite(self.job.computingSite).setokens.has_key(tmpSrcToken):
                                                    dq2ID = self.siteMapper.getSite(self.job.computingSite).setokens[tmpSrcToken]
                                                optSource[dq2ID] = {'policy' : 0}
                                            # T1 used as T2
                                            if self.siteMapper.getSite(self.job.computingSite).cloud != self.job.cloud and \
                                               (not tmpSrcDDM.endswith('PRODDISK')) and  \
                                               (not self.job.prodSourceLabel in ['user','panda']):
                                                # register both DATADISK and PRODDISK as source locations
                                                if self.siteMapper.getSite(self.job.computingSite).setokens.has_key('ATLASPRODDISK'):
                                                    dq2ID = self.siteMapper.getSite(self.job.computingSite).setokens['ATLASPRODDISK']
                                                    optSource[dq2ID] = {'policy' : 0}
                                                if not optSource.has_key(tmpSrcDDM):
                                                    optSource[tmpSrcDDM] = {'policy' : 0}
                                            # use another location when token is set
                                            if not file.destinationDBlockToken in ['NULL','']:
                                                tmpDQ2IDList = []
                                                tmpDstTokens = file.destinationDBlockToken.split(',')
                                                # remove the first one because it is already used as a location
                                                if tmpSrcDDM == tmpDstDDM:
                                                    tmpDstTokens = tmpDstTokens[1:]
                                                # loop over all tokens
                                                for idxToken,tmpDstToken in enumerate(tmpDstTokens):
                                                    dq2ID = tmpDstDDM
                                                    if self.siteMapper.getSite(file.destinationSE).setokens.has_key(tmpDstToken):
                                                        dq2ID = self.siteMapper.getSite(file.destinationSE).setokens[tmpDstToken]
                                                    # keep the fist destination for multi-hop
                                                    if idxToken == 0:
                                                        firstDestDDM = dq2ID
                                                    else:
                                                        # use the fist destination as source for T1D1
                                                        optSource = {}                                                        
                                                        optSource[firstDestDDM] = {'policy' : 0}
                                                    # remove looping subscription
                                                    if dq2ID == tmpSrcDDM:
                                                        continue
                                                    # avoid duplication
                                                    if not dq2ID in tmpDQ2IDList:
                                                        subMap[fileDestinationDBlock].append((dq2ID,optSub,optSource))
                                            else:
                                                # use default DDM
                                                for dq2ID in tmpDstDDM.split(','):
                                                    subMap[fileDestinationDBlock].append((dq2ID,optSub,optSource))
                except:
                    errStr = '%s %s' % sys.exc_info()[:2]
                    self.logger.error(traceback.format_exc())
                    self.result.setFatal()
                    self.job.ddmErrorDiag = 'failed before adding files : ' + errStr
                    return 1
        # cleanup submap
        tmpKeys = subMap.keys()
        for tmpKey in tmpKeys:
            if subMap[tmpKey] == []:
                del subMap[tmpKey]
        # add data to original dataset
        for destinationDBlock in idMap.keys():
            origDBlock = None
            match = re.search('^(.+)_sub\d+$',destinationDBlock)
            if match != None:
                # add files to top-level datasets
                origDBlock = match.group(1)
                if (not self.goToTransferring) or (not self.addToTopOnly and destinationDBlock in distDSs):
                    idMap[origDBlock] = idMap[destinationDBlock]
            # add files to top-level datasets only 
            if self.addToTopOnly or self.goToMerging or destinationDBlock in distDSs:
                del idMap[destinationDBlock]
            # skip sub unless getting transferred
            if origDBlock != None:
                if not self.goToTransferring and not self.logTransferring \
                       and idMap.has_key(destinationDBlock):
                    del idMap[destinationDBlock]
        # print idMap
        self.logger.debug("idMap = %s" % idMap)
        self.logger.debug("subMap = %s" % subMap)
        self.logger.debug("dsDestMap = %s" % dsDestMap)
        self.logger.debug("extraInfo = %s" % str(self.extraInfo))
        # check consistency of destinationDBlock
        hasSub = False
        for destinationDBlock in idMap.keys():
            match = re.search('^(.+)_sub\d+$',destinationDBlock)
            if match != None:
                hasSub = True
                break
        if idMap != {} and self.goToTransferring and not hasSub:
            errStr = 'no sub datasets for transferring. destinationDBlock may be wrong'
            self.logger.error(errStr)
            self.result.setFatal()
            self.job.ddmErrorDiag = 'failed before adding files : ' + errStr
            return 1
        # add data
        self.logger.debug("addFiles start")
        # count the number of files
        regNumFiles = 0
        regFileList = []
        for tmpRegDS,tmpRegList in idMap.iteritems():
            for tmpRegItem in tmpRegList:
                if not tmpRegItem['lfn'] in regFileList:
                    regNumFiles += 1
                    regFileList.append(tmpRegItem['lfn'])
        # decompose idMap
        if not self.useCentralLFC():
            destIdMap = {None:idMap}
        else:
            destIdMap = self.decomposeIdMap(idMap,dsDestMap,osDsFileMap)
        # add files
        nTry = 3
        for iTry in range(nTry):
            isFatal  = False
            isFailed = False
            regStart = datetime.datetime.utcnow()
            try:
                if not self.useCentralLFC():
                    regMsgStr = "DQ2 registraion for %s files " % regNumFiles                    
                else:
                    regMsgStr = "LFC+DQ2 registraion with backend={0} for {1} files ".format(self.ddmBackEnd,
                                                                                             regNumFiles)
                if len(zipFiles) > 0:
                    self.logger.debug('{0} {1}'.format('registerZipFiles',str(zipFiles)))
                    rucioAPI.registerZipFiles(zipFiles)
                self.logger.debug('{0} {1} zip={2}'.format('registerFilesInDatasets',str(destIdMap),str(contZipMap)))
                out = rucioAPI.registerFilesInDataset(destIdMap, contZipMap)
            except (DataIdentifierNotFound,
                    FileConsistencyMismatch,
                    UnsupportedOperation,
                    InvalidPath,
                    RSENotFound,
                    RSEProtocolNotSupported,
                    exceptions.KeyError):
                # fatal errors
                errType,errValue = sys.exc_info()[:2]
                out = '%s : %s' % (errType,errValue)
                out += traceback.format_exc()
                isFatal = True
                isFailed = True
            except:
                # unknown errors
                errType,errValue = sys.exc_info()[:2]
                out = '%s : %s' % (errType,errValue)
                out += traceback.format_exc()
                if 'value too large for column' in out or \
                        'unique constraint (ATLAS_RUCIO.DIDS_GUID_IDX) violate' in out:
                    isFatal = True
                else:
                    isFatal = False
                isFailed = True                
            regTime = datetime.datetime.utcnow() - regStart
            self.logger.debug(regMsgStr + \
                                  'took %s.%03d sec' % (regTime.seconds,regTime.microseconds/1000))
            # failed
            if isFailed or isFatal:
                self.logger.error('%s' % out)
                if (iTry+1) == nTry or isFatal:
                    self.job.ddmErrorCode = ErrorCode.EC_Adder
                    # extract important error string
                    extractedErrStr = DataServiceUtils.extractImportantError(out)
                    errMsg = "Could not add files to DDM: "
                    if extractedErrStr == '':
                        self.job.ddmErrorDiag = errMsg + out.split('\n')[-1]
                    else:
                        self.job.ddmErrorDiag = errMsg + extractedErrStr
                    if isFatal:
                        self.result.setFatal()
                    else:
                        self.result.setTemporary()
                    return 1
                self.logger.error("Try:%s" % iTry)
                # sleep
                time.sleep(10)                    
            else:
                self.logger.debug('%s' % str(out))
                break
        # register dataset subscription
        if self.job.processingType == 'urgent' or self.job.currentPriority > 1000:
            subActivity = 'Express'
        else:
            subActivity = 'Production Output'
        if not self.job.prodSourceLabel in ['user']:
            for tmpName,tmpVal in subMap.iteritems():
                for dq2ID,optSub,optSource in tmpVal:
                    if not self.goToMerging:
                        # make subscription for prod jobs
                        repLifeTime = 14
                        self.logger.debug("%s %s %s" % ('registerDatasetSubscription',
                                                        (tmpName,dq2ID),
                                                        {'activity':subActivity,
                                                         'replica_lifetime':repLifeTime}))
                        for iDDMTry in range(3):
                            isFailed = False                        
                            try:
                                status = rucioAPI.registerDatasetSubscription(tmpName,[dq2ID],
                                                                              owner='panda',
                                                                              activity=subActivity,
                                                                              lifetime=repLifeTime)
                                out = 'OK'
                                break
                            except:
                                status = False
                                errType,errValue = sys.exc_info()[:2]
                                out = "%s %s" % (errType,errValue)
                                isFailed = True
                                # retry for temporary errors
                                time.sleep(10)
                        if isFailed:
                            self.logger.error('%s' % out)
                            if self.job.ddmErrorCode == ErrorCode.EC_Subscription:
                                # fatal error
                                self.job.ddmErrorDiag = "subscription failure with %s" % out
                                self.result.setFatal()
                            else:
                                # temoprary errors
                                self.job.ddmErrorCode = ErrorCode.EC_Adder                
                                self.job.ddmErrorDiag = "could not register subscription : %s" % tmpName
                                self.result.setTemporary()
                            return 1
                        self.logger.debug('%s' % str(out))
                    else:
                        # register location
                        tmpDsNameLoc = re.sub('_sub\d+$','',tmpName)
                        repLifeTime = 14
                        for tmpLocName in optSource.keys():
                            self.logger.debug("%s %s %s %s" % ('registerDatasetLocation',tmpDsNameLoc,tmpLocName,
                                                               {'lifetime':"14 days"}))
                            for iDDMTry in range(3):
                                out = 'OK'
                                isFailed = False                        
                                try:                        
                                    rucioAPI.registerDatasetLocation(tmpDsNameLoc,[tmpLocName],
                                                                     owner='panda',
                                                                     activity=subActivity,
                                                                     lifetime=repLifeTime)
                                    out = 'OK'
                                    break
                                except:
                                    status = False
                                    errType,errValue = sys.exc_info()[:2]
                                    out = "%s %s" % (errType,errValue)
                                    isFailed = True
                                    # retry for temporary errors
                                    time.sleep(10)
                            if isFailed:
                                self.logger.error('%s' % out)
                                if self.job.ddmErrorCode == ErrorCode.EC_Location:
                                    # fatal error
                                    self.job.ddmErrorDiag = "location registration failure with %s" % out
                                    self.result.setFatal()
                                else:
                                    # temoprary errors
                                    self.job.ddmErrorCode = ErrorCode.EC_Adder                
                                    self.job.ddmErrorDiag = "could not register location : %s" % tmpDsNameLoc
                                    self.result.setTemporary()
                                return 1
                            self.logger.debug('%s' % str(out))
                    # set dataset status
                    self.datasetMap[tmpName].status = 'running'
            # keep subscriptions
            self.subscriptionMap = subMap
            # collect list of transfring jobs
            for tmpFile in self.job.Files:
                if tmpFile.type in ['log','output']:
                    if self.goToTransferring or (self.logTransferring and tmpFile.type == 'log'):
                        # don't go to tranferring for successful ES jobs 
                        if self.job.jobStatus == 'finished' and EventServiceUtils.isEventServiceJob(self.job) \
                                and not EventServiceUtils.isJobCloningJob(self.job):
                            continue
                        # skip distributed datasets
                        if tmpFile.destinationDBlock in distDSs:
                            continue
                        # skip no output
                        if tmpFile.status == 'nooutput':
                            continue
                        # skip alternative stage-out
                        if tmpFile.lfn in self.job.altStgOutFileList():
                            continue
                        # skip zip files
                        if tmpFile.lfn in zipFileMap:
                            continue
                        self.result.transferringFiles.append(tmpFile.lfn)
        elif not "--mergeOutput" in self.job.jobParameters:
            # send request to DaTRI unless files will be merged
            tmpTopDatasets = {}
            # collect top-level datasets
            for tmpName,tmpVal in subMap.iteritems():
                for dq2ID,optSub,optSource in tmpVal:
                    tmpTopName = re.sub('_sub\d+','',tmpName)
                    # append
                    if not tmpTopDatasets.has_key(tmpTopName):
                        tmpTopDatasets[tmpTopName] = []
                    if not dq2ID in tmpTopDatasets[tmpTopName]:
                        tmpTopDatasets[tmpTopName].append(dq2ID)
            # remove redundant CN from DN
            tmpDN = self.job.prodUserID
            tmpDN = rucioAPI.parse_dn(tmpDN)
            # send request
            if tmpTopDatasets != {} and self.jobStatus == 'finished':
                try:
                    tmpDN = rucioAPI.parse_dn(tmpDN)
                    status,userInfo = rucioAPI.finger(tmpDN)
                    if not status:
                        raise RuntimeError,'user info not found for {0} with {1}'.format(tmpDN,userInfo)
                    userEPs = []
                    # loop over all output datasets
                    for tmpDsName,dq2IDlist in tmpTopDatasets.iteritems():
                        for tmpDQ2ID in dq2IDlist:
                            if tmpDQ2ID == 'NULL':
                                continue
                            if not tmpDQ2ID in userEPs:
                                userEPs.append(tmpDQ2ID)
                            # use group account for group.*
                            if tmpDsName.startswith('group') and not self.job.workingGroup in ['','NULL',None]:
                                tmpDN = self.job.workingGroup
                            else:
                                tmpDN = userInfo['nickname']
                            tmpMsg = "registerDatasetLocation for Rucio ds=%s site=%s id=%s" % (tmpDsName,tmpDQ2ID,tmpDN)
                            self.logger.debug(tmpMsg)
                            rucioAPI.registerDatasetLocation(tmpDsName,[tmpDQ2ID],owner=tmpDN,
                                                             activity="User Subscriptions")
                    # set dataset status
                    for tmpName,tmpVal in subMap.iteritems():
                        self.datasetMap[tmpName].status = 'running'
                except InsufficientAccountLimit as (errType,errValue):
                    tmpMsg = "Rucio failed to make subscriptions at {0} since {1}".format(','.join(userEPs),errType[0])
                    self.logger.error(tmpMsg)
                    self.job.ddmErrorCode = ErrorCode.EC_Adder
                    self.job.ddmErrorDiag = "Rucio failed with {0}".format(errType[0])
                    # set dataset status
                    for tmpName,tmpVal in subMap.iteritems():
                        self.datasetMap[tmpName].status = 'running'
                    if userInfo != None and 'email' in userInfo:
                        self.sendEmail(userInfo['email'],tmpMsg,self.job.jediTaskID)
                except:
                    errType,errValue = sys.exc_info()[:2]
                    tmpMsg = "registerDatasetLocation failed with %s %s" % (errType,errValue)
                    self.logger.error(tmpMsg)
                    self.job.ddmErrorCode = ErrorCode.EC_Adder
                    self.job.ddmErrorDiag = "Rucio failed with %s %s" % (errType,errValue)
        # collect list of merging files
        if self.goToMerging and not self.jobStatus in ['failed','cancelled','closed']:
            for tmpFileList in idMap.values():
                for tmpFile in tmpFileList:
                    if not tmpFile['lfn'] in self.result.mergingFiles:
                        self.result.mergingFiles.append(tmpFile['lfn'])
        # register ES files
        if EventServiceUtils.isEventServiceJob(self.job) \
                and not EventServiceUtils.isJobCloningJob(self.job):
            try:
                pass
                #self.registerEventServiceFiles()
            except:
                errType,errValue = sys.exc_info()[:2]
                self.logger.error('failed to register ES files with {0}:{1}'.format(errType,errValue))
                self.result.setTemporary()
                return 1
        elif EventServiceUtils.isEventServiceMerge(self.job):
            # delete ES files
            if self.job.jobStatus == 'finished' or self.job.attemptNr >= self.job.maxAttempt:
                try:
                    self.deleteEventServiceFiles()
                except:
                    errType,errValue = sys.exc_info()[:2]
                    self.logger.error('failed to delete ES files with {0}:{1}'.format(errType,errValue))
                    self.result.setTemporary()
                    return 1
        # properly finished    
        self.logger.debug("addFiles end")
        return 0


    # use cerntral LFC
    def useCentralLFC(self):
        tmpSiteSpec = self.siteMapper.getSite(self.job.computingSite)
        if not self.addToTopOnly and tmpSiteSpec.lfcregister in ['server']:
            return True
        return False


    # decompose idMap
    def decomposeIdMap(self,idMap,dsDestMap,osDsFileMap):
        # add item for top datasets
        for tmpDS in dsDestMap.keys():
            tmpTopDS = re.sub('_sub\d+$','',tmpDS)
            if tmpTopDS != tmpDS:
                dsDestMap[tmpTopDS] = dsDestMap[tmpDS]
        destIdMap = {}
        for tmpDS,tmpFiles in idMap.iteritems():
            for tmpDest in dsDestMap[tmpDS]:
                if not destIdMap.has_key(tmpDest):
                    destIdMap[tmpDest] = {}
                destIdMap[tmpDest][tmpDS] = tmpFiles
        # add OS stuff
        for tmpDest,tmpIdMap in osDsFileMap.iteritems():
            for tmpDS,tmpFiles in tmpIdMap.iteritems():
                if not destIdMap.has_key(tmpDest):
                    destIdMap[tmpDest] = {}
                destIdMap[tmpDest][tmpDS] = tmpFiles
        return destIdMap


    # send email notification
    def sendEmail(self,toAdder,message,jediTaskID):
        # subject
        mailSubject = "PANDA warning for TaskID:{0} with --destSE".format(jediTaskID)
        # message
        mailBody = "Hello,\n\nTaskID:{0} cannot process the --destSE request\n\n".format(jediTaskID)
        mailBody +=     "Reason : %s\n" % message
        # send
        retVal = MailUtils().send(toAdder,mailSubject,mailBody)
        # return
        return



    # get ES dataset and file
    def getEsDatasetFile(self,fileSpec):
        falseRet = False,None,None
        if not fileSpec.type in ['output']:
            return falseRet
        esDataset = fileSpec.dataset + EventServiceUtils.esSuffixDDM
        esLFN = fileSpec.lfn + EventServiceUtils.esSuffixDDM
        # check if dataset exists
        dsOK = rucioAPI.checkDatasetExist(esDataset)
        if not dsOK:
            self.logger.debug("skip {0} since it is missing in DDM".format(esDataset))
            return falseRet
        return dsOK,esDataset,esLFN



    # register ES files
    def registerEventServiceFiles(self):
        self.logger.debug("registering ES files")
        # get files
        idMap = {}
        for fileSpec in self.job.Files:
            # ES dataset and file name
            dsOK,esDataset,esLFN = self.getEsDatasetFile(fileSpec)
            if not dsOK:
                continue
            # make ES folder
            file = {'scope' : fileSpec.scope,
                    'name'  : esLFN,
                    'bytes' : 0,
                    'state' : "A"}
            # get OS IDs
            osIDs = self.taskBuffer.getObjIDs(self.job.jediTaskID,self.job.PandaID)
            self.logger.debug("got {0} OS IDs for {1}".format(len(osIDs),esLFN))
            for osID in osIDs:
                # convert to DDM endpoint
                rse = self.taskBuffer.convertObjIDtoEndPoint('/cvmfs/atlas.cern.ch/repo/sw/local/etc/agis_ddmendpoints_objectstores.json',
                                                             osID)
                if rse != None:
                    tmpFile = copy.copy(file)
                    tmpFile['pfn'] = '/'.join([rse['path'],fileSpec.scope,esDataset,fileSpec.scope+':'+esLFN])
                    # register file
                    self.logger.debug("registering ES file={0} to {1}".format(str(tmpFile),rse['name']))
                    rucioAPI.registerFiles([tmpFile],rse['name'])
            # collect files
            if len(osIDs) > 0:
                if not None in idMap:
                    idMap[None] = {}
                if not esDataset in idMap[None]:
                    idMap[None][esDataset] = []
                idMap[None][esDataset].append(file)
        # add files to dataset
        if idMap != {}:
            self.logger.debug("adding ES files {0}".format(str(idMap)))
            try:
                rucioAPI.registerFilesInDataset(idMap)
            except DataIdentifierNotFound:
                self.logger.debug("ignored DataIdentifierNotFound")
        self.logger.debug("done")



    # delete ES files
    def deleteEventServiceFiles(self):
        self.logger.debug("deleting ES files")
        # get files
        idMap = {}
        for fileSpec in self.job.Files:
            # ES dataset and file name
            dsOK,esDataset,esLFN = self.getEsDatasetFile(fileSpec)
            if not dsOK:
                continue
            # make ES folder
            file = {'scope' : fileSpec.scope,
                    'name'  : esLFN}
            # collect files
            if not esDataset in idMap:
                idMap[esDataset] = []
            idMap[esDataset].append(file)
        # delete files from datasets
        if idMap != {}:
            self.logger.debug("deleting files {0}".format(str(idMap)))
            for dsn,dids in idMap.iteritems():
                try:
                    rucioAPI.deleteFilesFromDataset(dsn,dids)
                except DataIdentifierNotFound:
                    self.logger.debug("ignored DataIdentifierNotFound")
        self.logger.debug("done")
