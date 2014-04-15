'''
add data to dataset

'''

import os
import re
import sys
import time
import fcntl
import datetime
import commands
import threading
import xml.dom.minidom
import ErrorCode
from dq2.clientapi import DQ2
from dq2.filecatalog.FileCatalogUnknownFactory import FileCatalogUnknownFactory
try:
    from dq2.clientapi.cli import Register2
except:
    pass
try:
    from dq2.filecatalog.rucio.RucioFileCatalogException import RucioFileCatalogException
except:
    # dummy class
    class RucioFileCatalogException:
        pass


import brokerage.broker_util
from config import panda_config
from pandalogger.PandaLogger import PandaLogger
from AdderPluginBase import AdderPluginBase

   
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
        self.dq2api = None
        self.pandaDDM = False
        self.goToMerging = False

        
    # main
    def execute(self):
        try:
            self.logger.debug("start plugin : %s" % self.jobStatus)
            # instantiate DQ2
            self.dq2api = DQ2.DQ2()
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
            tmpSrcDDM = self.siteMapper.getSite(self.job.computingSite).ddm
            tmpSrcSEs = brokerage.broker_util.getSEfromSched(self.siteMapper.getSite(self.job.computingSite).se)
            destSEwasSet = False
            brokenSched = False
            if self.job.prodSourceLabel == 'user' and not self.siteMapper.siteSpecList.has_key(self.job.destinationSE):
                # DQ2 ID was set by using --destSE for analysis job to transfer output
                destSEwasSet = True
                tmpDstDDM = self.job.destinationSE
                tmpDstSEs = self.job.destinationSE
            else:
                tmpDstDDM = self.siteMapper.getSite(self.job.destinationSE).ddm
                tmpDstSEs = brokerage.broker_util.getSEfromSched(self.siteMapper.getSite(self.job.destinationSE).se)
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
            self.logger.debug('DDM src:%s dst:%s' % (tmpSrcDDM,tmpDstDDM))
            self.logger.debug('SE src:%s dst:%s' % (tmpSrcSEs,tmpDstSEs))
            if re.search('^ANALY_',self.job.computingSite) != None:
                # analysis site
                pass
            elif self.job.computingSite == self.job.destinationSE:
                # same site ID for computingSite and destinationSE
                pass
            elif tmpSrcDDM == tmpDstDDM:
                # same DQ2ID for src/dest
                pass
            elif tmpSrcSEs == tmpDstSEs:
                # same SEs
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
            else:
                self.goToTransferring = True
            self.logger.debug('goToTransferring=%s' % self.goToTransferring)
            self.logger.debug('logTransferring=%s' % self.logTransferring)
            self.logger.debug('goToMerging=%s' % self.goToMerging)
            retOut = self._updateOutputs()
            self.logger.debug('added outputs with %s' % retOut)
            if retOut != 0:
                self.logger.debug('terminated when adding')
                return
            # remove unmerged
            if self.job.processingType == 'usermerge' and self.job.prodSourceLabel == 'user' and \
                   self.jobStatus == 'finished' and self.job.ddmErrorDiag == 'NULL':
                retMerge = self._removeUnmerged()
                # failed
                if not retMerge:
                    self.logger.debug('terminated when removing unmerged')
                    return
            # succeeded    
            self.result.setSucceeded()    
            self.logger.debug("end plugin")
        except:
            type, value, traceBack = sys.exc_info()
            self.logger.debug(": %s %s" % (type,value))
            # set fatal error code
            self.result.setFatal()
        # return
        return


    # update output files
    def _updateOutputs(self):
        # return if non-DQ2
        if self.pandaDDM or self.job.destinationSE == 'local':
            return 0
        # check files
        idMap = {}
        fileList = []
        subMap = {}        
        dsDestMap = {}
        for file in self.job.Files:
            if file.type == 'output' or file.type == 'log':
                # append to fileList
                fileList.append(file.lfn)
                # add only log file for failed jobs
                if self.jobStatus == 'failed' and file.type != 'log':
                    continue
                try:
                    # fsize
                    fsize = None
                    if not file.fsize in ['NULL','',0]:
                        try:
                            fsize = long(file.fsize)
                        except:
                            type, value, traceBack = sys.exc_info()
                            self.logger.error("%s : %s %s" % (self.jobID,type,value))
                    # append to map
                    if not idMap.has_key(file.destinationDBlock):
                        idMap[file.destinationDBlock] = []
                    fileAttrs = {'guid'     : file.GUID,
                                 'lfn'      : file.lfn,
                                 'size'     : fsize,
                                 'checksum' : file.checksum}
                    # add SURLs if LFC registration is required
                    if self.useCentralLFC():
                        fileAttrs['surl'] = self.extraInfo['surl'][file.lfn]
                        if fileAttrs['surl'] == None:
                            raise TypeError,"{0} has SURL=None".format(file.lfn)
                        # get destination
                        if not dsDestMap.has_key(file.destinationDBlock):
                            if file.destinationDBlockToken in ['',None,'NULL']:
                                tmpDestList = [self.siteMapper.getSite(self.job.computingSite).ddm]
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
                            dsDestMap[file.destinationDBlock] = tmpDestList
                    idMap[file.destinationDBlock].append(fileAttrs)
                    # for subscription
                    if self.job.prodSourceLabel in ['managed','test','software','rc_test','ptest','user'] and \
                           re.search('_sub\d+$',file.destinationDBlock) != None and (not self.addToTopOnly) and \
                           self.job.destinationSE != 'local':
                        if self.siteMapper == None:
                            self.logger.error("SiteMapper==None")
                        else:
                            # get dataset spec
                            if not self.datasetMap.has_key(file.destinationDBlock):
                                tmpDS = self.taskBuffer.queryDatasetWithMap({'name':file.destinationDBlock})
                                self.datasetMap[file.destinationDBlock] = tmpDS
                            # check if valid dataset        
                            if self.datasetMap[file.destinationDBlock] == None:
                                self.logger.error(": cannot find %s in DB" % file.destinationDBlock)
                            else:
                                if not self.datasetMap[file.destinationDBlock].status in ['defined']:
                                    # not a fresh dataset
                                    self.logger.debug(": subscription was already made for %s:%s" % \
                                                  (self.datasetMap[file.destinationDBlock].status,
                                                   file.destinationDBlock))
                                else:
                                    # get DQ2 IDs
                                    tmpSrcDDM = self.siteMapper.getSite(self.job.computingSite).ddm
                                    tmpSrcSEs = brokerage.broker_util.getSEfromSched(self.siteMapper.getSite(self.job.computingSite).se)
                                    if self.job.prodSourceLabel == 'user' and not self.siteMapper.siteSpecList.has_key(file.destinationSE):
                                        # DQ2 ID was set by using --destSE for analysis job to transfer output
                                        tmpDstDDM = file.destinationSE
                                        tmpDstSEs = file.destinationSE
                                    else:
                                        tmpDstDDM = self.siteMapper.getSite(file.destinationSE).ddm
                                        tmpDstSEs = brokerage.broker_util.getSEfromSched(self.siteMapper.getSite(file.destinationSE).se)
                                    # if src != dest or multi-token
                                    if (tmpSrcDDM != tmpDstDDM and tmpSrcSEs != tmpDstSEs) or \
                                       (tmpSrcDDM == tmpDstDDM and file.destinationDBlockToken.count(',') != 0):
                                        optSub = {'DATASET_COMPLETE_EVENT' : ['https://%s:%s/server/panda/datasetCompleted' % \
                                                                              (panda_config.pserverhost,panda_config.pserverport)]}
                                        # append
                                        if not subMap.has_key(file.destinationDBlock):
                                            subMap[file.destinationDBlock] = []
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
                                                        subMap[file.destinationDBlock].append((dq2ID,optSub,optSource))
                                            else:
                                                # use default DDM
                                                for dq2ID in tmpDstDDM.split(','):
                                                    subMap[file.destinationDBlock].append((dq2ID,optSub,optSource))
                except:
                    errStr = '%s %s' % sys.exc_info()[:2]
                    self.logger.error(errStr)
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
                if not self.goToTransferring:
                    idMap[origDBlock] = idMap[destinationDBlock]
            # add files to top-level datasets only 
            if self.addToTopOnly or self.goToMerging:
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
        # check consistency 
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
            destIdMap = {'DUMMY':idMap}
        else:
            destIdMap = self.decomposeIdMap(idMap,dsDestMap)          
        # loop over all destination
        for tmpDest,tmpIdMap in destIdMap.iteritems():
             # number of retry
             nTry = 3
             for iTry in range(nTry):
                 # empty
                 if idMap == {}:
                     break
                 # add data to datasets
                 time.sleep(1)
                 isFailed = False
                 isFatal  = False
                 setErrorDiag = False
                 out = 'OK'
                 fatalErrStrs = ['[ORA-00001] unique constraint (ATLAS_DQ2.UQ_01_FILES_GUID) violated',
                                 '[USER][OTHER] Parameter value [None] is not a valid uid!']
                 regStart = datetime.datetime.utcnow()
                 try:
                     if not self.useCentralLFC():
                         regMsgStr = "DQ2 registraion for %s files " % regNumFiles                    
                         self.logger.debug('%s %s' % ('registerFilesInDatasets',str(tmpIdMap)))
                         self.dq2api.registerFilesInDatasets(tmpIdMap)
                     else:
                         regMsgStr = "LFC+DQ2 registraion for %s files " % regNumFiles
                         self.logger.debug('%s %s %s' % ('Register.registerFilesInDatasets',tmpDest,str(tmpIdMap)))                    
                         registerAPI = Register2.Register(tmpDest)
                         out = registerAPI.registerFilesInDatasets(tmpIdMap)
                 except DQ2.DQFileExistsInDatasetException:
                     # hamless error 
                     errType,errValue = sys.exc_info()[:2]
                     out = '%s : %s' % (errType,errValue)
                 except (DQ2.DQClosedDatasetException,
                         DQ2.DQFrozenDatasetException,
                         DQ2.DQUnknownDatasetException,
                         DQ2.DQDatasetExistsException,
                         DQ2.DQFileMetaDataMismatchException,
                         FileCatalogUnknownFactory,
                         RucioFileCatalogException):
                     # fatal errors
                     errType,errValue = sys.exc_info()[:2]
                     out = '%s : %s' % (errType,errValue)
                     isFatal = True
                     isFailed = True
                 except:
                     # unknown errors
                     errType,errValue = sys.exc_info()[:2]
                     out = '%s : %s' % (errType,errValue)
                     for tmpFatalErrStr in fatalErrStrs:
                         if tmpFatalErrStr in str(errValue):
                             self.job.ddmErrorDiag = 'failed to add files : ' + tmpFatalErrStr
                             setErrorDiag = True
                             break
                     if setErrorDiag:
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
                         errMsg = "Could not add files to DDM: "
                         self.job.ddmErrorDiag = errMsg + out.split('\n')[-1]
                         if isFatal:
                             self.result.setFatal()
                         else:
                             self.result.setTemporary()
                         return 1
                     self.logger.error("Try:%s" % iTry)
                     # sleep
                     time.sleep(120)                    
                 else:
                     self.logger.debug('%s' % str(out))
                     break
        # register dataset subscription
        subActivity = 'Production'
        if not self.job.prodSourceLabel in ['user']:
            for tmpName,tmpVal in subMap.iteritems():
                for dq2ID,optSub,optSource in tmpVal:
                    if not self.goToMerging:
                        # make DQ2 subscription for prod jobs
                        self.logger.debug("%s %s %s" % ('registerDatasetSubscription',
                                                        (tmpName,dq2ID),
                                                        {'version':0,'archived':0,'callbacks':optSub,
                                                         'sources':optSource,'sources_policy':(001000 | 010000),
                                                         'wait_for_sources':0,'destination':None,'query_more_sources':0,
                                                         'sshare':"production",'group':None,'activity':subActivity,
                                                         'acl_alias':None,'replica_lifetime':"14 days"}))
                        for iDDMTry in range(3):
                            out = 'OK'
                            isFailed = False                        
                            try:                        
                                self.dq2api.registerDatasetSubscription(tmpName,dq2ID,version=0,archived=0,callbacks=optSub,
                                                                        sources=optSource,sources_policy=(001000 | 010000),
                                                                        wait_for_sources=0,destination=None,query_more_sources=0,
                                                                        sshare="production",group=None,activity=subActivity,
                                                                        acl_alias=None,replica_lifetime="14 days")
                            except DQ2.DQSubscriptionExistsException:
                                # harmless error
                                errType,errValue = sys.exc_info()[:2]
                                out = '%s : %s' % (errType,errValue)
                            except:
                                # unknown errors
                                errType,errValue = sys.exc_info()[:2]
                                out = '%s : %s' % (errType,errValue)
                                isFailed = True
                                if 'is not a Tiers of Atlas Destination' in str(errValue) or \
                                        'is not in Tiers of Atlas' in str(errValue):
                                    # fatal error
                                    self.job.ddmErrorCode = ErrorCode.EC_Subscription
                                else:
                                    # retry for temporary errors
                                    time.sleep(60)
                            else:
                                break
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
                        for tmpLocName in optSource.keys():
                            self.logger.debug("%s %s %s %s" % ('registerDatasetLocation',tmpDsNameLoc,tmpLocName,
                                                               {'lifetime':"14 days"}))
                            for iDDMTry in range(3):
                                out = 'OK'
                                isFailed = False                        
                                try:                        
                                    self.dq2api.registerDatasetLocation(tmpDsNameLoc,tmpLocName,lifetime="14 days")
                                except DQ2.DQLocationExistsException:
                                    # harmless error
                                    errType,errValue = sys.exc_info()[:2]
                                    out = '%s : %s' % (errType,errValue)
                                except:
                                    # unknown errors
                                    errType,errValue = sys.exc_info()[:2]
                                    out = '%s : %s' % (errType,errValue)
                                    isFailed = True
                                    # retry for temporary errors
                                    time.sleep(60)
                                else:
                                    break
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
            tmpDN = re.sub('/CN=limited proxy','',tmpDN)
            tmpDN = re.sub('(/CN=proxy)+$','',tmpDN)
            # send request
            if tmpTopDatasets != {} and self.jobStatus == 'finished':
                try:
                    from datriHandler import datriHandler
                    if self.job.lockedby.startswith('Ganga'):
                        tmpHandler = datriHandler(type='ganga')
                    else:
                        tmpHandler = datriHandler(type='pathena')
                    # loop over all output datasets
                    for tmpDsName,dq2IDlist in tmpTopDatasets.iteritems():
                        for tmpDQ2ID in dq2IDlist:
                            if tmpDQ2ID == 'NULL':
                                continue
                            tmpMsg = "%s %s ds=%s site=%s id=%s" % (self.jobID,'datriHandler.sendRequest',
                                                                    tmpDsName,tmpDQ2ID,tmpDN)
                            self.logger.debug(tmpMsg)
                            tmpHandler.setParameters(data_pattern=tmpDsName,
                                                     site=tmpDQ2ID,
                                                     userid=tmpDN)
                            # number of retry
                            nTry = 1
                            for iTry in range(nTry):
                                dhStatus,dhOut = tmpHandler.sendRequest()
                                # succeeded
                                if dhStatus == 0 or "such request is exist" in dhOut:
                                    self.logger.debug("%s %s" % (dhStatus,dhOut))
                                    break
                                # faital errors
                                if "No input data or input data is incorrect" in dhOut:
                                    tmpMsg = "datriHandler failed with %s %s" % (dhStatus,dhOut)
                                    self.logger.error(tmpMsg)
                                    self.job.ddmErrorCode = ErrorCode.EC_Adder
                                    self.job.ddmErrorDiag = "DaTRI failed for %s with %s %s" % (tmpDsName,dhStatus,dhOut)
                                    return 0
                                # retry
                                if iTry+1 < nTry:
                                    # sleep
                                    time.sleep(60)
                                else:
                                    # final attempt failed
                                    tmpMsg = "datriHandler failed with %s %s" % (dhStatus,dhOut)
                                    self.logger.error(tmpMsg)
                                    self.job.ddmErrorCode = ErrorCode.EC_Adder
                                    self.job.ddmErrorDiag = "DaTRI failed for %s with %s %s" % (tmpDsName,dhStatus,dhOut)
                                    return 0
                    # set dataset status
                    for tmpName,tmpVal in subMap.iteritems():
                        self.datasetMap[tmpName].status = 'running'
                except:
                    errType,errValue = sys.exc_info()[:2]
                    tmpMsg = "datriHandler failed with %s %s" % (errType,errValue)
                    self.logger.error(tmpMsg)
                    self.job.ddmErrorCode = ErrorCode.EC_Adder
                    self.job.ddmErrorDiag = "DaTRI failed with %s %s" % (errType,errValue)
                    return 0
        # collect list of merging files
        if self.goToMerging:
            for tmpFileList in idMap.values():
                for tmpFile in tmpFileList:
                    if not tmpFile['lfn'] in self.result.mergingFiles:
                        self.result.mergingFiles.append(tmpFile['lfn'])
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
    def decomposeIdMap(self,idMap,dsDestMap):
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
        return destIdMap


    # remove unmerged files
    def _removeUnmerged(self):
        self.logger.debug("start removeUnmerged")
        # get input files
        inputFileGUIDs = []
        inputFileStr = ''
        for file in self.job.Files:
            if file.type == 'input':
                # remove skipped files
                if file.status in ['skipped']:
                    continue
                # ignore lib.tgz
                if re.search('lib\.tgz\.*\d*',file.lfn) != None:
                    continue
                # ignore DBRelease
                if re.search('DBRelease',file.lfn) != None:
                    continue
                # append
                inputFileGUIDs.append(file.GUID)
                inputFileStr += '%s,' % file.lfn
        # extract parent dataset name
        tmpMatch = re.search('--parentDS ([^ \'\"]+)',self.job.jobParameters)
        # failed
        if tmpMatch == None:
            self.logger.error("failed to extract parentDS from params=%s" % (self.job.jobParameters))
            return False
        parentDS = tmpMatch.group(1)
        # delete
        self.logger.debug("registerNewVersion %s" % parentDS)
        self.logger.debug("deleteFilesFromDataset %s %s" % (parentDS,inputFileStr[:-1]))
        nTry = 3
        for iTry in range(nTry):
            # add data to datasets
            isFailed = False
            isFatal  = False
            out = 'OK'
            try:
                self.dq2api.registerNewVersion(parentDS)
                self.dq2api.deleteFilesFromDataset(parentDS,inputFileGUIDs)
            except (DQ2.DQClosedDatasetException,
                    DQ2.DQFrozenDatasetException,
                    DQ2.DQUnknownDatasetException,
                    DQ2.DQFileMetaDataMismatchException):
                # fatal errors
                errType,errValue = sys.exc_info()[:2]
                out = '%s : %s' % (errType,errValue)
                isFatal = True
            except:
                # unknown errors
                errType,errValue = sys.exc_info()[:2]
                out = '%s : %s' % (errType,errValue)
                isFailed = True
            # failed
            if isFailed or isFatal:
                self.logger.error('%s' % out)
                if (iTry+1) == nTry or isFatal:
                    self.job.ddmErrorCode = ErrorCode.EC_Adder
                    errMsg = "failed to remove unmerged files : "
                    self.job.ddmErrorDiag = errMsg + out.split('\n')[-1]
                    if not isFatal:
                        self.result.setTemporary()
                    else:
                        self.result.setFatal()
                    return False
                self.logger.error("removeUnmerged Try:%s" % iTry)
                # sleep
                time.sleep(120)                    
            else:
                self.logger.debug('%s' % str(out))
                break
        # succeeded    
        self.logger.debug("removeUnmerged end")
        return True
