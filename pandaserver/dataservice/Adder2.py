'''
add data to dataset

'''

import os
import re
import sys
import time
import fcntl
import commands
import threading
import xml.dom.minidom
import ErrorCode
from dq2.clientapi import DQ2
try:
    from dq2.clientapi.cli import Register2
except:
    pass

import brokerage.broker_util
import Closer

from config import panda_config
from pandalogger.PandaLogger import PandaLogger

# logger
_logger = PandaLogger().getLogger('Adder')
Closer.initLogger(_logger)

   
class Adder (threading.Thread):
    # constructor
    def __init__(self,taskBuffer,jobID,fileCatalog,jobStatus,xmlFile='',ignoreDDMError=True,joinCloser=False,
                 addOutput=False,pandaDDM=False,siteMapper=None,attemptNr=None):
        threading.Thread.__init__(self)
        self.job = None
        self.jobID = jobID
        self.jobStatus = jobStatus
        self.taskBuffer = taskBuffer
        self.ignoreDDMError = ignoreDDMError
        self.joinCloser = joinCloser
        self.addOutput = addOutput
        self.pandaDDM = pandaDDM
        self.lockXML = None
        self.datasetMap = {}
        self.siteMapper = siteMapper
        self.addToTopOnly = False
        self.goToTransferring = False
        self.subscriptionMap = {}
        self.dq2api = None
        self.attemptNr = attemptNr        
        # dump Catalog into file
        if xmlFile=='':
            if attemptNr == None:
                self.xmlFile = '%s/%s_%s_%s' % (panda_config.logdir,jobID,jobStatus,
                                                commands.getoutput('uuidgen'))
            else:
                self.xmlFile = '%s/%s_%s_%s_%s' % (panda_config.logdir,jobID,jobStatus,
                                                   commands.getoutput('uuidgen'),attemptNr)
            file = open(self.xmlFile,'w')
            file.write(fileCatalog)
            file.close()
        else:
            self.xmlFile = xmlFile
            # exstract attemptNr
            try:
                tmpAttemptNr = self.xmlFile.split('/')[-1].split('_')[-1]
                if re.search('^\d+$',tmpAttemptNr) != None:
                    self.attemptNr = int(tmpAttemptNr)
            except:
                pass
    # main
    def run(self):
        try:
            _logger.debug("%s new start: %s attemptNr=%s" % (self.jobID,self.jobStatus,self.attemptNr))
            # instantiate DQ2
            self.dq2api = DQ2.DQ2()
            # lock XML except last trial
            if self.addOutput and self.ignoreDDMError:
                self.lockXML = open(self.xmlFile)
                try:
                    fcntl.flock(self.lockXML.fileno(), fcntl.LOCK_EX|fcntl.LOCK_NB)
                except:
                    _logger.debug("%s cannot get lock : %s" % (self.jobID,self.xmlFile))
                    self.lockXML.close()
                    return
            # query job
            self.job = self.taskBuffer.peekJobs([self.jobID],fromDefined=False,
                                                fromArchived=False,
                                                fromWaiting=False)[0]
            # check if job has finished
            if self.job == None:
                _logger.debug('%s : not found' % self.jobID)                
            elif self.job.jobStatus in ['finished','failed','unknown','cancelled']:
                _logger.error('%s : invalid state -> %s' % (self.jobID,self.job.jobStatus))
            elif self.attemptNr != None and self.job.attemptNr != self.attemptNr:
                _logger.error('%s : wrong attemptNr -> job=%s <> %s' % (self.jobID,self.job.attemptNr,self.attemptNr))
            else:
                # add files only to top-level datasets for transferring jobs
                if self.job.jobStatus == 'transferring':
                    self.addToTopOnly = True
                    _logger.debug("%s adder for transferring" % self.jobID)
                # use PandaDDM for ddm jobs
                if self.job.prodSourceLabel == 'ddm':
                    self.pandaDDM = True
                # set job status
                self.job.jobStatus = self.jobStatus
                # add outputs. Cannot add self.pandaDDM here since minidom.parse() produces seg-fault
                if self.addOutput:
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
                        # protection against disappearance from schedconfig
                        if not self.siteMapper.checkSite(self.job.destinationSE):
                            self.job.ddmErrorCode = ErrorCode.EC_Adder
                            self.job.ddmErrorDiag = "destinaitonSE %s is unknown in schedconfig" % self.job.destinationSE
                            self.job.jobStatus = 'failed'
                            self.jobStatus = 'failed'
                            _logger.error("%s %s" % (self.jobID,self.job.ddmErrorDiag))
                            brokenSched = True
                        elif not self.siteMapper.checkSite(self.job.computingSite):
                            self.job.ddmErrorCode = ErrorCode.EC_Adder
                            self.job.ddmErrorDiag = "computingSite %s is unknown in schedconfig" % self.job.computingSite
                            self.job.jobStatus = 'failed'
                            self.jobStatus = 'failed'
                            _logger.error("%s %s" % (self.jobID,self.job.ddmErrorDiag))
                            brokenSched = True
                    _logger.debug('%s DDM src:%s dst:%s' % (self.jobID,tmpSrcDDM,tmpDstDDM))
                    _logger.debug('%s SE src:%s dst:%s' % (self.jobID,tmpSrcSEs,tmpDstSEs))
                    if re.search('^ANALY_',self.job.computingSite) != None:
                        # analysis site
                        pass
                    elif (re.search('BNL', self.job.computingSite) != None or self.job.computingSite == "TPATHENA"):
                        # BNL
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
                    elif self.job.computingSite.endswith("_REPRO"):
                        # reprocessing sites
                        pass
                    elif self.addToTopOnly:
                        # already in transferring
                        pass
                    elif self.job.jobStatus == 'failed':
                        # failed jobs
                        pass
                    else:
                        self.goToTransferring = True
                    _logger.debug('%s goToTransferring=%s' % (self.jobID,self.goToTransferring))
                    if not brokenSched:
                        self._updateOutputs()
                else:
                    _logger.debug('%s : not added' % self.jobID)
                    _logger.debug('%s escape' % self.jobID)
                    return
                _logger.debug('%s updated outputs' % self.jobID)                
                # ignore DDMError
                if self.ignoreDDMError and \
                       (re.search('could not add files',self.job.ddmErrorDiag) != None or \
                        re.search('could not register subscription',self.job.ddmErrorDiag) != None) and \
                        re.search('DQClosedDatasetException',self.job.ddmErrorDiag) == None and \
                        re.search('DQFrozenDatasetException',self.job.ddmErrorDiag) == None and \
                        re.search('DQUnknownDatasetException',self.job.ddmErrorDiag) == None and \
                        re.search('DQFileMetaDataMismatchException',self.job.ddmErrorDiag) == None and \
                        re.search('Exceeded the maximum number of files',self.job.ddmErrorDiag) == None and \
                        re.search('KeyError',self.job.ddmErrorDiag) == None and \
                        not self.job.ddmErrorCode in [ErrorCode.EC_Subscription]:                       
                    _logger.debug('%s : ignore %s ' % (self.jobID,self.job.ddmErrorDiag))
                    _logger.debug('%s escape' % self.jobID)
                    # unlock XML
                    try:
                        fcntl.flock(self.lockXML.fileno(), fcntl.LOCK_UN)
                        self.lockXML.close()
                    except:
                        type, value, traceBack = sys.exc_info()
                        _logger.debug("%s : %s %s" % (self.jobID,type,value))
                        _logger.debug("%s cannot unlock XML" % self.jobID)            
                    return
                # update shadow dataset
                if self.job.prodSourceLabel == 'user' and self.jobStatus == 'finished' and \
                   (self.job.ddmErrorDiag == 'NULL' or re.search('DaTRI failed',self.job.ddmErrorDiag) != None) and \
                   not self.goToTransferring:
                    self._updateShadow()
                    # ignore DDMError
                    if self.ignoreDDMError and re.search('could not add files',self.job.ddmErrorDiag) != None \
                           and re.search('DQClosedDatasetException',self.job.ddmErrorDiag) == None \
                           and re.search('DQFrozenDatasetException',self.job.ddmErrorDiag) == None \
                           and re.search('DQFileMetaDataMismatchException',self.job.ddmErrorDiag) == None \
                           and re.search('Exceeded the maximum number of files',self.job.ddmErrorDiag) == None \
                           and re.search('KeyError',self.job.ddmErrorDiag) == None:                           
                        _logger.debug('%s : ignore %s ' % (self.jobID,self.job.ddmErrorDiag))
                        _logger.debug('%s escape' % self.jobID)
                        # unlock XML
                        try:
                            fcntl.flock(self.lockXML.fileno(), fcntl.LOCK_UN)
                            self.lockXML.close()                            
                        except:
                            type, value, traceBack = sys.exc_info()
                            _logger.debug("%s : %s %s" % (self.jobID,type,value))
                            _logger.debug("%s cannot unlock XML" % self.jobID)            
                        return
                # remove unmerged
                if self.job.processingType == 'usermerge' and self.job.prodSourceLabel == 'user' and \
                   self.jobStatus == 'finished' and self.job.ddmErrorDiag == 'NULL':
                    retMerge = self._removeUnmerged()
                    # ignore DDMError
                    if self.ignoreDDMError and retMerge == None:
                        _logger.debug('%s : ignore %s ' % (self.jobID,self.job.ddmErrorDiag))
                        _logger.debug('%s escape' % self.jobID)
                        # unlock XML
                        try:
                            fcntl.flock(self.lockXML.fileno(), fcntl.LOCK_UN)
                            self.lockXML.close()                            
                        except:
                            type, value, traceBack = sys.exc_info()
                            _logger.debug("%s : %s %s" % (self.jobID,type,value))
                            _logger.debug("%s cannot unlock XML" % self.jobID)            
                        return
                # set file status
                if self.job.jobStatus == 'failed':
                    for file in self.job.Files:
                        if file.type == 'output' or file.type == 'log':
                            file.status = 'failed'
                else:
                    # reset errors
                    self.job.jobDispatcherErrorCode = 0
                    self.job.jobDispatcherErrorDiag = 'NULL'
                    # set job status
                    hasOutput = False
                    if self.goToTransferring or self.subscriptionMap != {}:
                        # set status to transferring
                        for file in self.job.Files:
                            if file.type == 'output' or file.type == 'log' or \
                               self.subscriptionMap.has_key(file.destinationDBlock):
                                file.status = 'transferring'
                                hasOutput = True
                        if hasOutput:                  
                            self.job.jobStatus = 'transferring'
                            # propagate transition to prodDB
                            self.job.stateChangeTime = time.strftime('%Y-%m-%d %H:%M:%S',time.gmtime())
                    # endtime
                    if self.job.endTime=='NULL':
                        self.job.endTime = time.strftime('%Y-%m-%d %H:%M:%S',time.gmtime())
                # set cancelled state
                if self.job.commandToPilot == 'tobekilled' and self.job.jobStatus == 'failed':
                    self.job.jobStatus = 'cancelled'
                # update job
                retU = self.taskBuffer.updateJobs([self.job],False)
                _logger.debug("%s retU: %s" % (self.jobID,retU))
                # failed
                if not retU[0]:
                    _logger.error('failed to update DB for %s' % self.jobID)
                    # unlock XML
                    try:
                        fcntl.flock(self.lockXML.fileno(), fcntl.LOCK_UN)
                        self.lockXML.close()                            
                    except:
                        type, value, traceBack = sys.exc_info()
                        _logger.debug("%s : %s %s" % (self.jobID,type,value))
                        _logger.debug("%s cannot unlock XML" % self.jobID)            
                    return
                # setup for closer
                destDBList = []
                guidList = []
                for file in self.job.Files:
                    # ignore inputs
                    if file.type == 'input':
                        continue
                    # start closer for output/log datasets
                    if not file.destinationDBlock in destDBList:
                        destDBList.append(file.destinationDBlock)
                    # collect GUIDs
                    if (self.job.prodSourceLabel=='panda' or (self.job.prodSourceLabel in ['ptest','rc_test'] and \
                                                              self.job.processingType in ['pathena','prun','gangarobot-rctest'])) \
                           and file.type == 'output':
                        guidList.append({'lfn':file.lfn,'guid':file.GUID,'type':file.type,
                                         'checksum':file.checksum,'md5sum':file.md5sum,
                                         'fsize':file.fsize})
                if guidList != []:
                    retG = self.taskBuffer.setGUIDs(guidList)
                if destDBList != []:
                    # start Closer
                    cThr = Closer.Closer(self.taskBuffer,destDBList,self.job,pandaDDM=self.pandaDDM,
                                         datasetMap=self.datasetMap)
                    _logger.debug("%s start Closer" % self.jobID)
                    cThr.start()
                    if self.joinCloser:
                        cThr.join()
                    _logger.debug("%s end Closer" % self.jobID)
            _logger.debug("%s end" % self.jobID)
            try:
                # remove Catalog
                os.remove(self.xmlFile)
            except:
                pass
            # unlock XML
            if self.lockXML != None:
                fcntl.flock(self.lockXML.fileno(), fcntl.LOCK_UN)
                self.lockXML.close()            
        except:
            type, value, traceBack = sys.exc_info()
            _logger.debug("%s : %s %s" % (self.jobID,type,value))
            _logger.debug("%s except" % self.jobID)            
            # unlock XML just in case
            try:
                if self.lockXML != None:
                    fcntl.flock(self.lockXML.fileno(), fcntl.LOCK_UN)
            except:
                type, value, traceBack = sys.exc_info()
                _logger.debug("%s : %s %s" % (self.jobID,type,value))
                _logger.debug("%s cannot unlock XML" % self.jobID)            


    # update output files
    def _updateOutputs(self):
        # get LFN and GUID
        _logger.debug("%s %s" % (self.jobID,self.xmlFile))
        # no outputs
        if self.job.Files == []:
            _logger.debug("%s has no outputs" % self.jobID)            
            _logger.debug("%s addFiles end" % self.jobID)
            return
        # get input files
        inputLFNs = []
        for file in self.job.Files:
            if file.type == 'input':
                inputLFNs.append(file.lfn)
        # parse XML
        lfns    = []
        guids   = []
        fsizes  = []
        md5sums = []
        chksums = []
        surls   = []
        try:
            root  = xml.dom.minidom.parse(self.xmlFile)
            files = root.getElementsByTagName('File')
            for file in files:
                # get GUID
                guid = str(file.getAttribute('ID'))
                _logger.debug(guid)
                # get PFN and LFN nodes
                logical  = file.getElementsByTagName('logical')[0]
                lfnNode  = logical.getElementsByTagName('lfn')[0]
                # convert UTF8 to Raw
                lfn = str(lfnNode.getAttribute('name'))
                # get metadata
                fsize   = None
                md5sum  = None
                adler32 = None
                surl    = None
                for meta in file.getElementsByTagName('metadata'):
                    # get fsize
                    name = str(meta.getAttribute('att_name'))
                    if name == 'fsize':
                        fsize = long(meta.getAttribute('att_value'))
                    elif name == 'md5sum':
                        md5sum = str(meta.getAttribute('att_value'))
                        # check
                        if re.search("^[a-fA-F0-9]{32}$",md5sum) == None:
                            md5sum = None
                    elif name == 'adler32':
                        adler32 = str(meta.getAttribute('att_value'))
                    elif name == 'surl':
                        surl = str(meta.getAttribute('att_value'))
                # error check
                if (not lfn in inputLFNs) and (fsize == None or (md5sum == None and adler32 == None) \
                                               or (self.useCentralLFC() and surl == None)):
                    raise RuntimeError, 'fsize/md5sum/adler32/surl=None'
                # append
                lfns.append(lfn)
                guids.append(guid)
                fsizes.append(fsize)
                md5sums.append(md5sum)
                surls.append(surl)
                if adler32 != None:
                    # use adler32 if available
                    chksums.append("ad:%s" % adler32)
                else:
                    chksums.append("md5:%s" % md5sum)
        except:
            # check if file exists
            if os.path.exists(self.xmlFile):
                type, value, traceBack = sys.exc_info()
                _logger.error("%s : %s %s" % (self.jobID,type,value))
                # set failed anyway
                self.job.jobStatus = 'failed'
                # XML error happens when pilot got killed due to wall-time limit or failures in wrapper
                if (self.job.pilotErrorCode in [0,'0','NULL']) and \
                   (self.job.transExitCode  in [0,'0','NULL']):
                    self.job.ddmErrorCode = ErrorCode.EC_Adder
                    self.job.ddmErrorDiag = "Adder._updateOutputs() could not get GUID/LFN/MD5/FSIZE/SURL"
                return
            else:
                # XML was deleted
                self.job.ddmErrorDiag = "Adder._updateOutputs() could not add files"
                self.ignoreDDMError = True
                return
        # check files
        idMap = {}
        fileList = []
        subMap = {}        
        for file in self.job.Files:
            if file.type == 'input':
                if file.lfn in lfns:
                    if self.job.prodSourceLabel in ['user','panda']:
                        # skipped file
                        file.status = 'skipped'
                    elif self.job.prodSourceLabel in ['managed','test','rc_test','ptest']:
                        # failed by pilot
                        file.status = 'failed'
            elif file.type == 'output' or file.type == 'log':
                # append to fileList
                fileList.append(file.lfn)
                # add only log file for failed jobs
                if self.jobStatus == 'failed' and file.type != 'log':
                    continue
                # add only log file for unmerge jobs
                if self.job.prodSourceLabel == 'panda' and self.job.processingType in ['unmerge'] \
                   and file.type != 'log':
                    continue
                # look for GUID with LFN
                try:
                    i = lfns.index(file.lfn)
                    file.GUID   = guids[i]
                    file.fsize  = fsizes[i]
                    file.md5sum = md5sums[i]
                    file.checksum = chksums[i]
                    surl = surls[i]
                    # status
                    file.status = 'ready'
                    # fsize
                    fsize = None
                    if not file.fsize in ['NULL','',0]:
                        try:
                            fsize = long(file.fsize)
                        except:
                            type, value, traceBack = sys.exc_info()
                            _logger.error("%s : %s %s" % (self.jobID,type,value))
                    # append to map
                    if not idMap.has_key(file.destinationDBlock):
                        idMap[file.destinationDBlock] = []
                    fileAttrs = {'guid'     : file.GUID,
                                 'lfn'      : lfns[i],
                                 'size'     : fsize,
                                 'checksum' : file.checksum}
                    # add SURLs if LFC registration is required
                    if self.useCentralLFC():
                        fileAttrs['surl'] = surl
                    idMap[file.destinationDBlock].append(fileAttrs)
                    # for subscription
                    if self.job.prodSourceLabel in ['managed','test','software','rc_test','ptest','user'] and \
                           re.search('_sub\d+$',file.destinationDBlock) != None and (not self.addToTopOnly) and \
                           self.job.destinationSE != 'local':
                        if self.siteMapper == None:
                            _logger.error("%s : SiteMapper==None" % self.jobID)                                
                        else:
                            # get dataset spec
                            if not self.datasetMap.has_key(file.destinationDBlock):
                                tmpDS = self.taskBuffer.queryDatasetWithMap({'name':file.destinationDBlock})
                                self.datasetMap[file.destinationDBlock] = tmpDS
                            # check if valid dataset        
                            if self.datasetMap[file.destinationDBlock] == None:
                                _logger.error("%s : cannot find %s in DB" % (self.jobID,file.destinationDBlock))
                            else:
                                if not self.datasetMap[file.destinationDBlock].status in ['defined']:
                                    # not a fresh dataset
                                    _logger.debug("%s : subscription was already made for %s:%s" % \
                                                  (self.jobID,self.datasetMap[file.destinationDBlock].status,
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
                                            # use PRODDISK for T1 used as T2
                                            usingPRODDISK = False
                                            if self.siteMapper.getSite(self.job.computingSite).cloud != self.job.cloud and \
                                               (not tmpSrcDDM.endswith('PRODDISK')) and  \
                                               self.siteMapper.getSite(self.job.computingSite).setokens.has_key('ATLASPRODDISK') and \
                                               (not self.job.prodSourceLabel in ['user','panda']):
                                                dq2ID = self.siteMapper.getSite(self.job.computingSite).setokens['ATLASPRODDISK']
                                                usingPRODDISK = True
                                                optSource[dq2ID] = {'policy' : 0}
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
                    # status
                    file.status = 'failed'
                    type, value, traceBack = sys.exc_info()
                    _logger.error("%s : %s %s" % (self.jobID,type,value))
        # cleanup submap
        tmpKeys = subMap.keys()
        for tmpKey in tmpKeys:
            if subMap[tmpKey] == []:
                del subMap[tmpKey]
        # check consistency between XML and filesTable
        for lfn in lfns:
            if (not lfn in fileList) and (not lfn in inputLFNs):
                _logger.error("%s %s is not found in filesTable" % (self.jobID,lfn))
                self.job.jobStatus = 'failed'
                self.job.ddmErrorCode = ErrorCode.EC_Adder
                self.job.ddmErrorDiag = "Adder._updateOutputs() XML is inconsistent with filesTable"
                return
        # return if PandaDDM is used or non-DQ2
        if self.pandaDDM or self.job.destinationSE == 'local':
            return
        # add data to original dataset
        for destinationDBlock in idMap.keys():
            match = re.findall('(.+)_sub\d+$',destinationDBlock)
            if len(match):
                # add files to top-level datasets
                if not self.goToTransferring:
                    origDBlock = match[0]
                    idMap[origDBlock] = idMap[destinationDBlock]
            # add files to top-level datasets only 
            if self.addToTopOnly:
                del idMap[destinationDBlock]
        # print idMap
        _logger.debug("%s idMap = %s" % (self.jobID,idMap))
        # add data
        _logger.debug("%s addFiles start" % self.jobID)
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
            fatalErrStrs = ['[ORA-00001] unique constraint (ATLAS_DQ2.UQ_01_FILES_GUID) violated']
            try:
                if not self.useCentralLFC():
                    _logger.debug('%s %s %s' % (self.jobID,'registerFilesInDatasets',str(idMap)))
                    self.dq2api.registerFilesInDatasets(idMap)
                else:
                    _logger.debug('%s %s %s' % (self.jobID,'Register.registerFilesInDatasets',str(idMap)))                    
                    registerAPI = Register2.Register(self.siteMapper.getSite(self.job.computingSite).ddm)
                    out = registerAPI.registerFilesInDatasets(idMap)
            except DQ2.DQFileExistsInDatasetException:
                # hamless error 
                errType,errValue = sys.exc_info()[:2]
                out = '%s : %s' % (errType,errValue)
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
                for tmpFatalErrStr in fatalErrStrs:
                    if tmpFatalErrStr in str(errValue):
                        self.job.ddmErrorDiag = 'failed to add files : ' + tmpFatalErrStr
                        setErrorDiag = True
                        break
                isFatal = True
            # failed
            if isFailed or isFatal:
                _logger.error('%s %s' % (self.jobID,out))
                if (iTry+1) == nTry or isFatal:
                    self.job.jobStatus = 'failed'
                    self.job.ddmErrorCode = ErrorCode.EC_Adder
                    if not setErrorDiag:
                        errMsg = "Adder._updateOutputs() could not add files : "
                        self.job.ddmErrorDiag = errMsg + out.split('\n')[-1]
                    return
                _logger.error("%s Try:%s" % (self.jobID,iTry))
                # sleep
                time.sleep(120)                    
            else:
                _logger.debug('%s %s' % (self.jobID,out))
                break
        # register dataset subscription
        subActivity = 'Production'
        if not self.job.prodSourceLabel in ['user']:
            # make DQ2 subscription for prod jobs
            for tmpName,tmpVal in subMap.iteritems():
                for dq2ID,optSub,optSource in tmpVal:
                    _logger.debug("%s %s %s %s" % (self.jobID,'registerDatasetSubscription',
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
                        _logger.error('%s %s' % (self.jobID,out))
                        if self.job.ddmErrorCode == ErrorCode.EC_Subscription:
                            # fatal error
                            self.job.ddmErrorDiag = "subscription failure with %s" % out
                            self.job.jobStatus = 'failed'                            
                        else:
                            # temoprary errors
                            self.job.ddmErrorCode = ErrorCode.EC_Adder                
                            self.job.ddmErrorDiag = "Adder._updateOutputs() could not register subscription : %s" % tmpName
                        return
                    _logger.debug('%s %s' % (self.jobID,out))                                                        
                    # set dataset status
                    self.datasetMap[tmpName].status = 'running'
            # keep subscriptions
            self.subscriptionMap = subMap
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
                            tmpMsg = "%s %s ds=%s site=%s id=%s" % (self.jobID,'datriHandler.sendRequest',
                                                                    tmpDsName,tmpDQ2ID,tmpDN)
                            _logger.debug(tmpMsg)
                            tmpHandler.setParameters(data_pattern=tmpDsName,
                                                     site=tmpDQ2ID,
                                                     userid=tmpDN)
                            # number of retry
                            nTry = 3
                            for iTry in range(nTry):
                                dhStatus,dhOut = tmpHandler.sendRequest()
                                # succeeded
                                if dhStatus == 0 or "such request is exist" in dhOut:
                                    _logger.debug("%s %s %s" % (self.jobID,dhStatus,dhOut))
                                    break
                                if iTry+1 < nTry:
                                    # sleep
                                    time.sleep(60)
                                else:
                                    # final attempt failed
                                    tmpMsg = "%s datriHandler failed with %s %s" % (self.jobID,dhStatus,dhOut)
                                    _logger.error(tmpMsg)
                                    self.job.ddmErrorCode = ErrorCode.EC_Adder
                                    self.job.ddmErrorDiag = "DaTRI failed for %s with %s %s" % (tmpDsName,dhStatus,dhOut)
                                    return
                    # set dataset status
                    for tmpName,tmpVal in subMap.iteritems():
                        self.datasetMap[tmpName].status = 'running'
                except:
                    errType,errValue = sys.exc_info()[:2]
                    tmpMsg = "%s datriHandler failed with %s %s" % (self.jobID,errType,errValue)
                    _logger.error(tmpMsg)
                    self.job.ddmErrorCode = ErrorCode.EC_Adder
                    self.job.ddmErrorDiag = "DaTRI failed with %s %s" % (errType,errValue)
                    return
        # properly finished    
        _logger.debug("%s addFiles end" % self.jobID)


    # update shadow dataset
    def _updateShadow(self):
        # return if PandaDDM is used or non-DQ2
        if self.pandaDDM or self.job.destinationSE == 'local':
            return
        _logger.debug("%s updateShadow" % self.jobID)
        # get shadow DS and contents
        shadowList  = []
        shadowFiles = []
        for file in self.job.Files:
            if file.type == 'output' or file.type == 'log':
                # get shadow name
                shadowDS = re.sub('_sub\d+$','',file.destinationDBlock) + '_shadow'
                if not shadowDS in shadowList:
                    shadowList.append(shadowDS)
            elif file.type == 'input':
                # remove skipped files
                if file.status in ['skipped']:
                    continue
                # ignore lib.tgz
                if re.search('lib\.tgz\.*\d*',file.lfn) != None:
                    continue
                # ignore DBRelease
                if re.search('DBRelease',file.lfn) != None:
                    continue
                # ignore when noshadow is set
                if file.destinationDBlockToken == 'noshadow':
                    continue
                # fsize
                fsize = None
                if not file.fsize in ['NULL','',0]:
                    try:
                        fsize = long(file.fsize)
                    except:
                        type, value, traceBack = sys.exc_info()
                        _logger.error("%s : %s %s" % (self.jobID,type,value))
                # append
                if len(str(file.GUID))==36:
                    shadowFiles.append({'guid'     : file.GUID,
                                        'lfn'      : file.lfn,
                                        'size'     : fsize,
                                        'checksum' : None})
        # create idMap
        idMap = {}
        for shadowDS in shadowList:
            nTry = 3
            findFlag = False
            for iTry in range(nTry):
                # check if shadow dataset exists
                _logger.debug((self.jobID, 'listDatasets',shadowDS,0,True))
                try:
                    out = self.dq2api.listDatasets(shadowDS,0,True)
                    if not out.has_key(shadowDS):
                        _logger.debug("%s shadow %s doesn't exist" % (self.jobID,shadowDS))
                    else:
                        findFlag = True
                    break
                except:
                    # sleep
                    time.sleep(120)                    
            # append
            if findFlag and shadowFiles != []:
                idMap[shadowDS] = shadowFiles
        # add data
        _logger.debug("%s shadow idMap = %s" % (self.jobID,idMap))
        if idMap == {}:
            return
        _logger.debug("%s addFilesToShadow start" % self.jobID)
        # number of retry
        nTry = 3
        for iTry in range(nTry):
            # add data to datasets
            _logger.debug((self.jobID, 'registerFilesInDatasets',idMap))
            isFailed = False
            isFatal  = False
            out = 'OK'
            try:
                self.dq2api.registerFilesInDatasets(idMap)
            except DQ2.DQFileExistsInDatasetException:
                # hamless error
                errType,errValue = sys.exc_info()[:2]
                out = '%s : %s' % (errType,errValue)
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
                isFatal = True
            # failed
            if isFailed or isFatal:
                _logger.error('%s %s' % (self.jobID,out))
                if (iTry+1) == nTry or isFatal:
                    self.job.jobStatus = 'failed'
                    self.job.ddmErrorCode = ErrorCode.EC_Adder
                    errMsg = "Adder._updateOutputs() could not add files : "
                    self.job.ddmErrorDiag = errMsg + out.split('\n')[-1]
                    return
                _logger.error("%s shadow Try:%s" % (self.jobID,iTry))
                # sleep
                time.sleep(120)                    
            else:
                _logger.debug('%s %s' % (self.jobID,out))
                break
        _logger.debug("%s addFilesToShadow end" % self.jobID)


    # use cerntral LFC
    def useCentralLFC(self):
        tmpSiteSpec = self.siteMapper.getSite(self.job.computingSite)
        if not self.addToTopOnly and tmpSiteSpec.lfcregister in ['server']:
            return True
        return False

    # remove unmerged files
    def _removeUnmerged(self):
        _logger.debug("%s removeUnmerged" % self.jobID)
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
            _logger.error("%s failed to extract parentDS from params=%s" % (self.jobID,self.job.jobParameters))
            return False
        parentDS = tmpMatch.group(1)
        # delete
        _logger.debug("%s deleteFilesFromDataset %s %s" % (self.jobID,parentDS,inputFileStr[:-1]))
        nTry = 3
        for iTry in range(nTry):
            # add data to datasets
            isFailed = False
            isFatal  = False
            out = 'OK'
            try:
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
                _logger.error('%s %s' % (self.jobID,out))
                if (iTry+1) == nTry or isFatal:
                    self.job.jobStatus = 'failed'
                    self.job.ddmErrorCode = ErrorCode.EC_Adder
                    errMsg = "failed to remove unmerged files : "
                    self.job.ddmErrorDiag = errMsg + out.split('\n')[-1]
                    if not isFatal:
                        # retrun None to retry later
                        return None
                    return False
                _logger.error("%s removeUnmerged Try:%s" % (self.jobID,iTry))
                # sleep
                time.sleep(120)                    
            else:
                _logger.debug('%s %s' % (self.jobID,out))
                break
        # succeeded    
        _logger.debug("%s removeUnmerged end" % self.jobID)
        return True
