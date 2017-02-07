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
import xml.dom.minidom
import ErrorCode
import uuid

import Closer

from config import panda_config
from pandalogger.PandaLogger import PandaLogger
from pandalogger.LogWrapper import LogWrapper
from taskbuffer import EventServiceUtils
from taskbuffer import retryModule
import taskbuffer.ErrorCode

# logger
_logger = PandaLogger().getLogger('Adder')

panda_config.setupPlugin()

   
class AdderGen:
    # constructor
    def __init__(self,taskBuffer,jobID,jobStatus,xmlFile,ignoreTmpError=True,siteMapper=None):
        self.job = None
        self.jobID = jobID
        self.jobStatus = jobStatus
        self.taskBuffer = taskBuffer
        self.ignoreTmpError = ignoreTmpError
        self.lockXML = None
        self.siteMapper = siteMapper
        self.attemptNr = None
        self.xmlFile = xmlFile
        self.datasetMap = {}
        self.extraInfo = {'surl':{},'nevents':{},'lbnr':{},'endpoint':{}}
        # exstract attemptNr
        try:
            tmpAttemptNr = self.xmlFile.split('/')[-1].split('_')[-1]
            if re.search('^\d+$',tmpAttemptNr) != None:
                self.attemptNr = int(tmpAttemptNr)
        except:
            pass
        # logger
        self.logger = LogWrapper(_logger,str(self.jobID))


    # dump file report
    def dumpFileReport(self,fileCatalog,attemptNr):
        self.logger.debug("dump file report")
        # dump Catalog into file
        if attemptNr == None:
            xmlFile = '%s/%s_%s_%s' % (panda_config.logdir,self.jobID,self.jobStatus,
                                       str(uuid.uuid4()))
        else:
            xmlFile = '%s/%s_%s_%s_%s' % (panda_config.logdir,self.jobID,self.jobStatus,
                                          str(uuid.uuid4()),attemptNr)
        file = open(xmlFile,'w')
        file.write(fileCatalog)
        file.close()


    # main
    def run(self):
        try:
            self.logger.debug("new start: %s attemptNr=%s" % (self.jobStatus,self.attemptNr))
            # lock XML
            self.lockXML = open(self.xmlFile)
            try:
                fcntl.flock(self.lockXML.fileno(), fcntl.LOCK_EX|fcntl.LOCK_NB)
            except:
                self.logger.debug("cannot get lock : %s" % self.xmlFile)
                self.lockXML.close()
                # remove XML just in case for the final attempt
                if not self.ignoreTmpError:
                    try:
                        # remove Catalog
                        os.remove(self.xmlFile)
                    except:
                        pass
                return
            # check if file exists
            if not os.path.exists(self.xmlFile):
                self.logger.debug("not exist : %s" % self.xmlFile)
                try:
                    fcntl.flock(self.lockXML.fileno(), fcntl.LOCK_UN)
                    self.lockXML.close()
                except:
                    pass
                return
            # query job
            self.job = self.taskBuffer.peekJobs([self.jobID],fromDefined=False,
                                                fromWaiting=False,
                                                forAnal=True)[0]
            # check if job has finished
            if self.job == None:
                self.logger.debug(': job not found in DB')
            elif self.job.jobStatus in ['finished','failed','unknown','merging']:
                self.logger.error(': invalid state -> %s' % self.job.jobStatus)
            elif self.attemptNr != None and self.job.attemptNr != self.attemptNr:
                self.logger.error('wrong attemptNr -> job=%s <> %s' % (self.job.attemptNr,self.attemptNr))
            else:
                # check file status in JEDI
                if not self.job.isCancelled() and not self.job.taskBufferErrorCode in [taskbuffer.ErrorCode.EC_PilotRetried]:
                    fileCheckInJEDI = self.taskBuffer.checkInputFileStatusInJEDI(self.job)
                    self.logger.debug("check file status in JEDI : {0}".format(fileCheckInJEDI))                
                    if fileCheckInJEDI == None:
                        raise RuntimeError,'failed to check file status in JEDI'
                    if fileCheckInJEDI == False:
                        # set job status to failed since some file status is wrong in JEDI 
                        self.jobStatus = 'failed'
                        self.job.ddmErrorCode = ErrorCode.EC_Adder
                        self.job.ddmErrorDiag = "wrong file status in source database"
                        self.logger.debug("set jobStatus={0} since input is inconsistent between Panda and JEDI".format(self.jobStatus))
                    elif self.job.jobSubStatus in ['pilot_closed']:
                        # terminated by the pilot
                        self.logger.debug("going to closed since terminated by the pilot")
                        retClosed = self.taskBuffer.killJobs([self.jobID],'pilot','60',True)
                        if retClosed[0] == True:
                            self.logger.debug("end")
                            try:
                                # remove Catalog
                                os.remove(self.xmlFile)
                            except:
                                pass
                            # unlock XML
                            if self.lockXML != None:
                                fcntl.flock(self.lockXML.fileno(), fcntl.LOCK_UN)
                                self.lockXML.close()
                            return
                    # check for cloned jobs
                    if EventServiceUtils.isJobCloningJob(self.job):
                        checkJC = self.taskBuffer.checkClonedJob(self.job)
                        if checkJC == None:
                            raise RuntimeError,'failed to check the cloned job'
                        # failed to lock semaphore
                        if checkJC['lock'] == False:
                            self.jobStatus = 'failed'
                            self.job.ddmErrorCode = ErrorCode.EC_Adder
                            self.job.ddmErrorDiag = "failed to lock semaphore for job cloning"
                            self.logger.debug("set jobStatus={0} since did not get semaphore for job cloning".format(self.jobStatus))
                # use failed for cancelled/closed jobs
                if self.job.isCancelled():
                    self.jobStatus = 'failed'
                    # reset error codes to skip retrial module
                    self.job.pilotErrorCode = 0
                    self.job.exeErrorCode = 0
                    self.job.ddmErrorCode = 0
                # keep old status
                oldJobStatus = self.job.jobStatus
                # set job status
                if not self.job.jobStatus in ['transferring']:
                    self.job.jobStatus = self.jobStatus
                addResult = None
                adderPlugin = None
                # parse XML
                parseResult = self.parseXML()
                if parseResult < 2:
                    # intraction with DDM
                    try:
                        # set VO=local for DDM free
                        if self.job.destinationSE == 'local':
                            tmpVO = 'local'
                        else:
                            tmpVO = self.job.VO
                        # instantiate concrete plugin
                        adderPluginClass = panda_config.getPlugin('adder_plugins',tmpVO)
                        if adderPluginClass == None:
                            # use ATLAS plugin by default
                            from AdderAtlasPlugin import AdderAtlasPlugin
                            adderPluginClass = AdderAtlasPlugin
                        self.logger.debug('plugin name {0}'.format(adderPluginClass.__name__))
                        adderPlugin = adderPluginClass(self.job,
                                                       taskBuffer=self.taskBuffer,
                                                       siteMapper=self.siteMapper,
                                                       extraInfo=self.extraInfo,
                                                       logger=self.logger)
                        # execute
                        self.logger.debug('plugin is ready')
                        adderPlugin.execute()
                        addResult = adderPlugin.result
                        self.logger.debug('plugin done with %s' % (addResult.statusCode))
                    except:
                        errtype,errvalue = sys.exc_info()[:2]
                        self.logger.error("failed to execute AdderPlugin for VO={0} with {1}:{2}".format(tmpVO,
                                                                                                         errtype,
                                                                                                         errvalue)) 
                        addResult = None
                        self.job.ddmErrorCode = ErrorCode.EC_Adder
                        self.job.ddmErrorDiag = "AdderPlugin failure"
                        
                    # ignore temporary errors
                    if self.ignoreTmpError and addResult != None and addResult.isTemporary():
                        self.logger.debug(': ignore %s ' % self.job.ddmErrorDiag)
                        self.logger.debug('escape')
                        # unlock XML
                        try:
                            fcntl.flock(self.lockXML.fileno(), fcntl.LOCK_UN)
                            self.lockXML.close()
                        except:
                            type, value, traceBack = sys.exc_info()
                            self.logger.debug(": %s %s" % (type,value))
                            self.logger.debug("cannot unlock XML")
                        return
                    # failed
                    if addResult == None or not addResult.isSucceeded():
                        self.job.jobStatus = 'failed'
                # set file status for failed jobs or failed transferring jobs
                if self.job.jobStatus == 'failed' or self.jobStatus == 'failed':
                    # First of all: check if job failed and in this case take first actions according to error table
                    source, error_code, error_diag = None, None, None
                    if self.job.pilotErrorCode:
                        source = 'pilotErrorCode'
                        error_code = self.job.pilotErrorCode
                        error_diag = self.job.pilotErrorDiag
                    elif self.job.exeErrorCode:
                        source = 'exeErrorCode'
                        error_code = self.job.exeErrorCode
                        error_diag = self.job.exeErrorDiag
                    elif self.job.ddmErrorCode:
                        source = 'ddmErrorCode'
                        error_code = self.job.ddmErrorCode
                        error_diag = self.job.ddmErrorDiag
            
                    _logger.info("updatejob has source %s, error_code %s and error_diag %s"%(source, error_code, error_diag))
                    
                    if source and error_code:
                        try:
                            self.logger.debug("AdderGen.run will call apply_retrial_rules")
                            retryModule.apply_retrial_rules(self.taskBuffer, self.job.PandaID, source, error_code, error_diag, self.job.attemptNr)
                            self.logger.debug("apply_retrial_rules is back")
                        except Exception as e:
                            self.logger.error("apply_retrial_rules excepted and needs to be investigated (%s)"%(e))
                    
                    self.job.jobStatus = 'failed'
                    for file in self.job.Files:
                        if file.type in ['output','log']:
                            if addResult != None and file.lfn in addResult.mergingFiles:
                                file.status = 'merging'
                            else:
                                file.status = 'failed'
                else:
                    # reset errors
                    self.job.jobDispatcherErrorCode = 0
                    self.job.jobDispatcherErrorDiag = 'NULL'
                    # set status
                    if addResult != None and addResult.mergingFiles != []:
                        # set status for merging:                        
                        for file in self.job.Files:
                            if file.lfn in addResult.mergingFiles:
                                file.status = 'merging'
                        self.job.jobStatus = 'merging'
                        # propagate transition to prodDB
                        self.job.stateChangeTime = time.strftime('%Y-%m-%d %H:%M:%S',time.gmtime())
                    elif addResult != None and addResult.transferringFiles != []:
                        # set status for transferring
                        for file in self.job.Files:
                            if file.lfn in addResult.transferringFiles:
                                file.status = 'transferring'
                        self.job.jobStatus = 'transferring'
                        # propagate transition to prodDB
                        self.job.stateChangeTime = time.strftime('%Y-%m-%d %H:%M:%S',time.gmtime())
                    else:
                        self.job.jobStatus = 'finished'
                # endtime
                if self.job.endTime=='NULL':
                    self.job.endTime = time.strftime('%Y-%m-%d %H:%M:%S',time.gmtime())
                # output size and # of outputs
                self.job.nOutputDataFiles = 0
                self.job.outputFileBytes = 0
                for tmpFile in self.job.Files:
                    if tmpFile.type == 'output':
                        self.job.nOutputDataFiles += 1
                        try:
                            self.job.outputFileBytes += tmpFile.fsize
                        except:
                            pass
                # protection
                maxOutputFileBytes = 99999999999
                if self.job.outputFileBytes > maxOutputFileBytes:
                    self.job.outputFileBytes = maxOutputFileBytes
                # set cancelled state
                if self.job.commandToPilot == 'tobekilled' and self.job.jobStatus == 'failed':
                    self.job.jobStatus = 'cancelled'
                # update job
                if oldJobStatus in ['cancelled','closed']:
                    pass
                else:
                    self.logger.debug("updating DB")
                    retU = self.taskBuffer.updateJobs([self.job],False,oldJobStatusList=[oldJobStatus],
                                                      extraInfo=self.extraInfo)
                    self.logger.debug("retU: %s" % retU)
                    # failed
                    if not retU[0]:
                        self.logger.error('failed to update DB for pandaid={0}'.format(self.job.PandaID))
                        # unlock XML
                        try:
                            fcntl.flock(self.lockXML.fileno(), fcntl.LOCK_UN)
                            self.lockXML.close()                            
                        except:
                            type, value, traceBack = sys.exc_info()
                            self.logger.debug(": %s %s" % (type,value))
                            self.logger.debug("cannot unlock XML")
                        return
                    # setup for closer
                    if not (EventServiceUtils.isEventServiceJob(self.job) and self.job.isCancelled()):
                        destDBList = []
                        guidList = []
                        for file in self.job.Files:
                            # ignore inputs
                            if file.type == 'input':
                                continue
                            # skip pseudo datasets
                            if file.destinationDBlock in ['',None,'NULL']:
                                continue
                            # start closer for output/log datasets
                            if not file.destinationDBlock in destDBList:
                                destDBList.append(file.destinationDBlock)
                            # collect GUIDs
                            if (self.job.prodSourceLabel=='panda' or (self.job.prodSourceLabel in ['ptest','rc_test','rucio_test'] and \
                                                                      self.job.processingType in ['pathena','prun','gangarobot-rctest','hammercloud'])) \
                                                                      and file.type == 'output':
                                # extract base LFN since LFN was changed to full LFN for CMS
                                baseLFN = file.lfn.split('/')[-1]
                                guidList.append({'lfn':baseLFN,'guid':file.GUID,'type':file.type,
                                                 'checksum':file.checksum,'md5sum':file.md5sum,
                                                 'fsize':file.fsize,'scope':file.scope})
                        if guidList != []:
                            retG = self.taskBuffer.setGUIDs(guidList)
                        if destDBList != []:
                            # start Closer
                            if adderPlugin != None and hasattr(adderPlugin,'datasetMap') and adderPlugin.datasetMap != {}:
                                cThr = Closer.Closer(self.taskBuffer,destDBList,self.job,datasetMap=adderPlugin.datasetMap)
                            else:
                                cThr = Closer.Closer(self.taskBuffer,destDBList,self.job)
                            self.logger.debug("start Closer")
                            cThr.start()
                            cThr.join()
                            self.logger.debug("end Closer")
                        # run closer for assocaiate parallel jobs
                        if EventServiceUtils.isJobCloningJob(self.job):
                            assDBlockMap = self.taskBuffer.getDestDBlocksWithSingleConsumer(self.job.jediTaskID,self.job.PandaID,
                                                                                            destDBList)
                            for assJobID,assDBlocks in assDBlockMap.iteritems():
                                assJob = self.taskBuffer.peekJobs([assJobID],fromDefined=False,
                                                                  fromArchived=False,
                                                                  fromWaiting=False,
                                                                  forAnal=True)[0]
                                if self.job == None:
                                    self.logger.debug(': associated job PandaID={0} not found in DB'.format(assJobID))
                                else:
                                    cThr = Closer.Closer(self.taskBuffer,assDBlocks,assJob)
                                    self.logger.debug("start Closer for PandaID={0}".format(assJobID))
                                    cThr.start()
                                    cThr.join()
                                    self.logger.debug("end Closer for PandaID={0}".format(assJobID))
            self.logger.debug("end")
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
            self.logger.debug(": %s %s" % (type,value))
            self.logger.debug("except")
            # unlock XML just in case
            try:
                if self.lockXML != None:
                    fcntl.flock(self.lockXML.fileno(), fcntl.LOCK_UN)
            except:
                type, value, traceBack = sys.exc_info()
                self.logger.debug(": %s %s" % (type,value))
                self.logger.debug("cannot unlock XML")


    # parse XML
    # 0: succeeded, 1: harmless error to exit, 2: fatal error, 3: event service
    def parseXML(self):
        # get LFN and GUID
        self.logger.debug('XML filename : %s' % self.xmlFile)
        # no outputs
        if self.job.Files == []:
            self.logger.debug("has no outputs")
            self.logger.debug("parseXML end")
            return 0
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
        fullLfnMap = {}
        nEventsMap = {}
        try:
            root  = xml.dom.minidom.parse(self.xmlFile)
            files = root.getElementsByTagName('File')
            for file in files:
                # get GUID
                guid = str(file.getAttribute('ID'))
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
                fullLFN = None
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
                    elif name == 'full_lfn':
                        fullLFN = str(meta.getAttribute('att_value'))
                # endpoints
                self.extraInfo['endpoint'][lfn] = []
                for epNode in file.getElementsByTagName('endpoint'):
                    self.extraInfo['endpoint'][lfn].append(str(epNode.firstChild.data))
                # error check
                if (not lfn in inputLFNs) and (fsize == None or (md5sum == None and adler32 == None)):
                    if EventServiceUtils.isEventServiceMerge(self.job):
                        continue
                    else:
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
                if fullLFN != None:
                    fullLfnMap[lfn] = fullLFN
        except:
            # check if file exists
            if os.path.exists(self.xmlFile):
                type, value, traceBack = sys.exc_info()
                self.logger.error(": %s %s" % (type,value))
                # set failed anyway
                self.job.jobStatus = 'failed'
                # XML error happens when pilot got killed due to wall-time limit or failures in wrapper
                if (self.job.pilotErrorCode in [0,'0','NULL']) and \
                   (self.job.transExitCode  in [0,'0','NULL']):
                    self.job.ddmErrorCode = ErrorCode.EC_Adder
                    self.job.ddmErrorDiag = "Could not get GUID/LFN/MD5/FSIZE/SURL from pilot XML"
                return 2
            else:
                # XML was deleted
                return 1
        # parse metadata to get nEvents
        try:
            root  = xml.dom.minidom.parseString(self.job.metadata)
            files = root.getElementsByTagName('File')
            for file in files:
                # get GUID
                guid = str(file.getAttribute('ID'))
                # get PFN and LFN nodes
                logical  = file.getElementsByTagName('logical')[0]
                lfnNode  = logical.getElementsByTagName('lfn')[0]
                # convert UTF8 to Raw
                lfn = str(lfnNode.getAttribute('name'))
                # get metadata
                nevents = None
                for meta in file.getElementsByTagName('metadata'):
                    # get fsize
                    name = str(meta.getAttribute('att_name'))
                    if name == 'events':
                        nevents = long(meta.getAttribute('att_value'))
                        nEventsMap[lfn] = nevents
                        break
        except:
            pass
        self.logger.debug('nEventsMap=%s' % str(nEventsMap))
        # parse json
        try:
            import json
            jsonDict = json.loads(self.job.metadata)
            for jsonFileItem in jsonDict['files']['output']:
                for jsonSubFileItem in jsonFileItem['subFiles']:
                    lfn = str(jsonSubFileItem['name'])
                    try:
                        nevents = long(jsonSubFileItem['nentries'])
                        nEventsMap[lfn] = nevents
                    except:
                        pass
        except:
            pass
        self.logger.debug('nEventsMapJson=%s' % str(nEventsMap))
        # get lumi block number
        lumiBlockNr = self.job.getLumiBlockNr()
        # copy files for variable number of outputs
        tmpStat = self.copyFilesForVariableNumOutputs(lfns)
        if not tmpStat:
            self.logger.error("failed to copy files for variable number of outputs")
            return 2
        # check files
        fileList = []
        for file in self.job.Files:
            fileList.append(file.lfn)
            if file.type == 'input':
                if file.lfn in lfns:
                    if self.job.prodSourceLabel in ['user','panda']:
                        # skipped file
                        file.status = 'skipped'
                    elif self.job.prodSourceLabel in ['managed','test','rc_test','ptest']:
                        # failed by pilot
                        file.status = 'failed'
            elif file.type == 'output' or file.type == 'log':
                # add only log file for failed jobs
                if self.jobStatus == 'failed' and file.type != 'log':
                    file.status = 'failed'
                    continue
                # set failed if it is missing in XML
                if not file.lfn in lfns:
                    if self.job.jobStatus == 'finished' and EventServiceUtils.isEventServiceJob(self.job):
                        # unset file status for ES jobs
                        pass
                    elif file.isAllowedNoOutput():
                        # allowed not to be produced
                        file.status = 'nooutput'
                        self.logger.debug('set {0} to status={1}'.format(file.lfn,file.status))
                    else:
                        file.status = 'failed'
                        self.job.jobStatus = 'failed'
                        self.job.ddmErrorCode = ErrorCode.EC_Adder
                        self.job.ddmErrorDiag = "expected output {0} is missing in pilot XML".format(file.lfn)
                        self.logger.error(self.job.ddmErrorDiag)
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
                    # change to full LFN
                    if fullLfnMap.has_key(file.lfn):
                        file.lfn = fullLfnMap[file.lfn]
                    # add SURL to extraInfo
                    self.extraInfo['surl'][file.lfn] = surl
                    # add nevents 
                    if nEventsMap.has_key(file.lfn):
                        self.extraInfo['nevents'][file.lfn] = nEventsMap[file.lfn]
                except:
                    # status
                    file.status = 'failed'
                    type, value, traceBack = sys.exc_info()
                    self.logger.error(": %s %s" % (type,value))
                # set lumi block number
                if lumiBlockNr != None and file.status != 'failed':
                    self.extraInfo['lbnr'][file.lfn] = lumiBlockNr 
        # check consistency between XML and filesTable
        for lfn in lfns:
            if not lfn in fileList:
                self.logger.error("%s is not found in filesTable" % lfn)
                self.job.jobStatus = 'failed'
                for tmpFile in self.job.Files:
                    tmpFile.status = 'failed'
                self.job.ddmErrorCode = ErrorCode.EC_Adder
                self.job.ddmErrorDiag = "pilot produced {0} inconsistently with jobdef".format(lfn)
                return 2
        # return
        self.logger.debug("parseXML end")
        return 0



    # copy files for variable number of outputs
    def copyFilesForVariableNumOutputs(self,lfns):
        # get original output files
        origOutputs = {}
        updateOrig  = {}
        for tmpFile in self.job.Files:
            if tmpFile.type in ['output','log']:
                origOutputs[tmpFile.lfn] = tmpFile
                if tmpFile.lfn in lfns:
                    # keep original
                    updateOrig[tmpFile.lfn] = False
                else:
                    # overwrite original
                    updateOrig[tmpFile.lfn] = True
        # look for unkown files
        addedNewFiles = False
        for newLFN in lfns:
            if not newLFN in origOutputs:
                # look for corresponding original output
                for origLFN in origOutputs.keys():
                    tmpPatt = '^{0}\.*_\d+$'.format(origLFN)
                    if re.search(tmpPatt,newLFN) != None:
                        # copy file record
                        tmpStat = self.taskBuffer.copyFileRecord(newLFN,origOutputs[origLFN],updateOrig[origLFN])
                        if not tmpStat:
                            return False
                        addedNewFiles = True
                        # disable further overwriting
                        updateOrig[origLFN] = False
                        break
        # refresh job info
        if addedNewFiles:
            self.job = self.taskBuffer.peekJobs([self.jobID],fromDefined=False,
                                                fromWaiting=False,
                                                forAnal=True)[0]
        # return
        return True
