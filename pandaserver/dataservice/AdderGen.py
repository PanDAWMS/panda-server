'''
add data to dataset

'''

import os
import re
import sys
import time
import fcntl
import traceback
import xml.dom.minidom
import uuid

from pandaserver.config import panda_config
from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandaserver.taskbuffer import EventServiceUtils
from pandaserver.taskbuffer import retryModule
from pandaserver.taskbuffer import JobUtils
import pandaserver.taskbuffer.ErrorCode
import pandaserver.dataservice.ErrorCode
from pandaserver.dataservice import Closer


try:
    long
except NameError:
    long = int

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
        self.extraInfo = {'surl':{},'nevents':{},'lbnr':{},'endpoint':{}, 'guid':{}}
        # exstract attemptNr
        try:
            tmpAttemptNr = self.xmlFile.split('/')[-1].split('_')[-1]
            if re.search('^\d+$',tmpAttemptNr) is not None:
                self.attemptNr = int(tmpAttemptNr)
        except Exception:
            pass
        # logger
        self.logger = LogWrapper(_logger,str(self.jobID))


    # dump file report
    def dumpFileReport(self,fileCatalog,attemptNr):
        self.logger.debug("dump file report")
        # dump Catalog into file
        if attemptNr is None:
            xmlFile = '%s/%s_%s_%s' % (panda_config.logdir,self.jobID,self.jobStatus,
                                       str(uuid.uuid4()))
        else:
            xmlFile = '%s/%s_%s_%s_%s' % (panda_config.logdir,self.jobID,self.jobStatus,
                                          str(uuid.uuid4()),attemptNr)
        file = open(xmlFile,'w')
        file.write(fileCatalog)
        file.close()


    # get plugin class
    def getPluginClass(self, tmpVO):
        # instantiate concrete plugin
        adderPluginClass = panda_config.getPlugin('adder_plugins',tmpVO)
        if adderPluginClass is None:
            # use ATLAS plugin by default
            from pandaserver.dataservice.AdderAtlasPlugin import AdderAtlasPlugin
            adderPluginClass = AdderAtlasPlugin
        self.logger.debug('plugin name {0}'.format(adderPluginClass.__name__))
        return adderPluginClass


    # main
    def run(self):
        try:
            self.logger.debug("new start: %s attemptNr=%s" % (self.jobStatus,self.attemptNr))
            # lock XML
            self.lockXML = open(self.xmlFile)
            try:
                fcntl.flock(self.lockXML.fileno(), fcntl.LOCK_EX|fcntl.LOCK_NB)
            except Exception:
                self.logger.debug("cannot get lock : %s" % self.xmlFile)
                self.lockXML.close()
                # remove XML just in case for the final attempt
                if not self.ignoreTmpError:
                    try:
                        # remove Catalog
                        os.remove(self.xmlFile)
                    except Exception:
                        pass
                return
            # check if file exists
            if not os.path.exists(self.xmlFile):
                self.logger.debug("not exist : %s" % self.xmlFile)
                try:
                    fcntl.flock(self.lockXML.fileno(), fcntl.LOCK_UN)
                    self.lockXML.close()
                except Exception:
                    pass
                return
            # query job
            self.job = self.taskBuffer.peekJobs([self.jobID],fromDefined=False,
                                                fromWaiting=False,
                                                forAnal=True)[0]
            # check if job has finished
            if self.job is None:
                self.logger.debug(': job not found in DB')
            elif self.job.jobStatus in ['finished','failed','unknown','merging']:
                self.logger.error(': invalid state -> %s' % self.job.jobStatus)
            elif self.attemptNr is not None and self.job.attemptNr != self.attemptNr:
                self.logger.error('wrong attemptNr -> job=%s <> %s' % (self.job.attemptNr,self.attemptNr))
            elif self.attemptNr is not None and self.job.jobStatus == 'transferring':
                errMsg = 'XML with attemptNr for {0}'.format(self.job.jobStatus)
                self.logger.error(errMsg)
                # FIXME
                raise RuntimeError(errMsg)
            elif self.jobStatus == EventServiceUtils.esRegStatus:
                # instantiate concrete plugin
                adderPluginClass = self.getPluginClass(self.job.VO)
                adderPlugin = adderPluginClass(self.job,
                                               taskBuffer=self.taskBuffer,
                                               siteMapper=self.siteMapper,
                                               logger=self.logger)
                # execute
                self.logger.debug('plugin is ready for ES file registration')
                adderPlugin.registerEventServiceFiles()
            else:
                # check file status in JEDI
                if not self.job.isCancelled() and not self.job.taskBufferErrorCode in \
                                                      [pandaserver.taskbuffer.ErrorCode.EC_PilotRetried]:
                    fileCheckInJEDI = self.taskBuffer.checkInputFileStatusInJEDI(self.job)
                    self.logger.debug("check file status in JEDI : {0}".format(fileCheckInJEDI))                
                    if fileCheckInJEDI is None:
                        raise RuntimeError('failed to check file status in JEDI')
                    if fileCheckInJEDI == False:
                        # set job status to failed since some file status is wrong in JEDI 
                        self.jobStatus = 'failed'
                        self.job.ddmErrorCode = pandaserver.dataservice.ErrorCode.EC_Adder
                        errStr = "inconsistent file status between Panda and JEDI. "
                        errStr += "failed to avoid duplicated processing caused by synchronization failure"
                        self.job.ddmErrorDiag = errStr
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
                            except Exception:
                                pass
                            # unlock XML
                            if self.lockXML is not None:
                                fcntl.flock(self.lockXML.fileno(), fcntl.LOCK_UN)
                                self.lockXML.close()
                            return
                    # check for cloned jobs
                    if EventServiceUtils.isJobCloningJob(self.job):
                        checkJC = self.taskBuffer.checkClonedJob(self.job)
                        if checkJC is None:
                            raise RuntimeError('failed to check the cloned job')
                        # failed to lock semaphore
                        if checkJC['lock'] == False:
                            self.jobStatus = 'failed'
                            self.job.ddmErrorCode = pandaserver.dataservice.ErrorCode.EC_Adder
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
                        # instantiate concrete plugin
                        adderPluginClass = self.getPluginClass(self.job.VO)
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
                    except Exception:
                        errtype,errvalue = sys.exc_info()[:2]
                        self.logger.error("failed to execute AdderPlugin for VO={0} with {1}:{2}".format(self.job.VO,
                                                                                                         errtype,
                                                                                                         errvalue)) 
                        addResult = None
                        self.job.ddmErrorCode = pandaserver.dataservice.ErrorCode.EC_Adder
                        self.job.ddmErrorDiag = "AdderPlugin failure"
                        
                    # ignore temporary errors
                    if self.ignoreTmpError and addResult is not None and addResult.isTemporary():
                        self.logger.debug(': ignore %s ' % self.job.ddmErrorDiag)
                        self.logger.debug('escape')
                        # unlock XML
                        try:
                            fcntl.flock(self.lockXML.fileno(), fcntl.LOCK_UN)
                            self.lockXML.close()
                        except Exception:
                            type, value, traceBack = sys.exc_info()
                            self.logger.debug(": %s %s" % (type,value))
                            self.logger.debug("cannot unlock XML")
                        return
                    # failed
                    if addResult is None or not addResult.isSucceeded():
                        self.job.jobStatus = 'failed'
                # set file status for failed jobs or failed transferring jobs
                self.logger.debug("status after plugin call :job.jobStatus=%s jobStatus=%s" % (self.job.jobStatus, self.jobStatus))
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
                    elif self.job.transExitCode:
                        source = 'transExitCode'
                        error_code = self.job.transExitCode
                        error_diag = ''
            
                    # _logger.info("updatejob has source %s, error_code %s and error_diag %s"%(source, error_code, error_diag))
                    
                    if source and error_code:
                        try:
                            self.logger.debug("AdderGen.run will call apply_retrial_rules")
                            retryModule.apply_retrial_rules(self.taskBuffer, self.job.PandaID, source, error_code, error_diag, self.job.attemptNr)
                            self.logger.debug("apply_retrial_rules is back")
                        except Exception as e:
                            self.logger.error("apply_retrial_rules excepted and needs to be investigated (%s): %s"%(e, traceback.format_exc()))
                    
                    self.job.jobStatus = 'failed'
                    for file in self.job.Files:
                        if file.type in ['output','log']:
                            if addResult is not None and file.lfn in addResult.mergingFiles:
                                file.status = 'merging'
                            else:
                                file.status = 'failed'
                else:
                    # reset errors
                    self.job.jobDispatcherErrorCode = 0
                    self.job.jobDispatcherErrorDiag = 'NULL'
                    # set status
                    if addResult is not None and addResult.mergingFiles != []:
                        # set status for merging:                        
                        for file in self.job.Files:
                            if file.lfn in addResult.mergingFiles:
                                file.status = 'merging'
                        self.job.jobStatus = 'merging'
                        # propagate transition to prodDB
                        self.job.stateChangeTime = time.strftime('%Y-%m-%d %H:%M:%S',time.gmtime())
                    elif addResult is not None and addResult.transferringFiles != []:
                        # set status for transferring
                        for file in self.job.Files:
                            if file.lfn in addResult.transferringFiles:
                                file.status = 'transferring'
                        self.job.jobStatus = 'transferring'
                        self.job.jobSubStatus = None
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
                        except Exception:
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
                        except Exception:
                            type, value, traceBack = sys.exc_info()
                            self.logger.debug(": %s %s" % (type,value))
                            self.logger.debug("cannot unlock XML")
                        return

                    try:
                        # updateJobs was successful and it failed a job with taskBufferErrorCode
                        self.logger.debug("AdderGen.run will peek the job")
                        job_tmp = self.taskBuffer.peekJobs([self.job.PandaID], fromDefined=False, fromArchived=True,
                                                           fromWaiting=False)[0]
                        self.logger.debug("status {0}, taskBufferErrorCode {1}, taskBufferErrorDiag {2}".format(job_tmp.jobStatus,
                                                                                                                job_tmp.taskBufferErrorCode,
                                                                                                                job_tmp.taskBufferErrorDiag))
                        if job_tmp.jobStatus == 'failed' and job_tmp.taskBufferErrorCode:
                            source = 'taskBufferErrorCode'
                            error_code = job_tmp.taskBufferErrorCode
                            error_diag = job_tmp.taskBufferErrorDiag
                            self.logger.debug("AdderGen.run 2 will call apply_retrial_rules")
                            retryModule.apply_retrial_rules(self.taskBuffer, job_tmp.PandaID, source, error_code,
                                                            error_diag, job_tmp.attemptNr)
                            self.logger.debug("apply_retrial_rules 2 is back")
                    except IndexError:
                        pass
                    except Exception as e:
                        self.logger.error("apply_retrial_rules 2 excepted and needs to be investigated (%s): %s" % (e, traceback.format_exc()))

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
                            if (self.job.prodSourceLabel=='panda' or (self.job.prodSourceLabel in ['rucio_test'] + JobUtils.list_ptest_prod_sources and \
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
                            if adderPlugin is not None and hasattr(adderPlugin,'datasetMap') and adderPlugin.datasetMap != {}:
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
                            for assJobID in assDBlockMap:
                                assDBlocks = assDBlockMap[assJobID]
                                assJob = self.taskBuffer.peekJobs([assJobID],fromDefined=False,
                                                                  fromArchived=False,
                                                                  fromWaiting=False,
                                                                  forAnal=True)[0]
                                if self.job is None:
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
            except Exception:
                pass
            # unlock XML
            if self.lockXML is not None:
                fcntl.flock(self.lockXML.fileno(), fcntl.LOCK_UN)
                self.lockXML.close()            
        except Exception:
            type, value, traceBack = sys.exc_info()
            errStr = ": %s %s " % (type,value)
            errStr += traceback.format_exc()
            self.logger.error(errStr)
            self.logger.error("except")
            # unlock XML just in case
            try:
                if self.lockXML is not None:
                    fcntl.flock(self.lockXML.fileno(), fcntl.LOCK_UN)
            except Exception:
                type, value, traceBack = sys.exc_info()
                self.logger.error(": %s %s" % (type,value))
                self.logger.error("cannot unlock XML")


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
        guidMap = dict()
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
                        if re.search("^[a-fA-F0-9]{32}$",md5sum) is None:
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
                if (not lfn in inputLFNs) and (fsize is None or (md5sum is None and adler32 is None)):
                    if EventServiceUtils.isEventServiceMerge(self.job):
                        continue
                    else:
                        raise RuntimeError('fsize/md5sum/adler32/surl=None')
                # append
                lfns.append(lfn)
                guids.append(guid)
                fsizes.append(fsize)
                md5sums.append(md5sum)
                surls.append(surl)
                if adler32 is not None:
                    # use adler32 if available
                    chksums.append("ad:%s" % adler32)
                else:
                    chksums.append("md5:%s" % md5sum)
                if fullLFN is not None:
                    fullLfnMap[lfn] = fullLFN
        except Exception:
            # parse json
            try:
                import json
                with open(self.xmlFile) as tmpF:
                    jsonDict = json.load(tmpF)
                    for lfn in jsonDict:
                        fileData = jsonDict[lfn]
                        lfn = str(lfn)
                        fsize   = None
                        md5sum  = None
                        adler32 = None
                        surl    = None
                        fullLFN = None
                        guid = str(fileData['guid'])
                        if 'fsize' in fileData:
                            fsize = long(fileData['fsize'])
                        if 'md5sum' in fileData:
                            md5sum = str(fileData['md5sum'])
                            # check
                            if re.search("^[a-fA-F0-9]{32}$",md5sum) is None:
                                md5sum = None
                        if 'adler32' in fileData:
                            adler32 = str(fileData['adler32'])
                        if 'surl' in fileData:
                            surl = str(fileData['surl'])
                        if 'full_lfn' in fileData:
                            fullLFN = str(fileData['full_lfn'])
                        # endpoints
                        self.extraInfo['endpoint'][lfn] = []
                        if 'endpoint' in fileData:
                            self.extraInfo['endpoint'][lfn] = fileData['endpoint']
                        # error check
                        if (not lfn in inputLFNs) and (fsize is None or (md5sum is None and adler32 is None)):
                            if EventServiceUtils.isEventServiceMerge(self.job):
                                continue
                            else:
                                raise RuntimeError('fsize/md5sum/adler32/surl=None')
                        # append
                        lfns.append(lfn)
                        guids.append(guid)
                        fsizes.append(fsize)
                        md5sums.append(md5sum)
                        surls.append(surl)
                        if adler32 is not None:
                            # use adler32 if available
                            chksums.append("ad:%s" % adler32)
                        else:
                            chksums.append("md5:%s" % md5sum)
                        if fullLFN is not None:
                            fullLfnMap[lfn] = fullLFN
            except Exception:
                # check if file exists
                if os.path.exists(self.xmlFile):
                    type, value, traceBack = sys.exc_info()
                    self.logger.error(": %s %s" % (type,value))
                    # set failed anyway
                    self.job.jobStatus = 'failed'
                    # XML error happens when pilot got killed due to wall-time limit or failures in wrapper
                    if (self.job.pilotErrorCode in [0,'0','NULL']) and \
                       (self.job.taskBufferErrorCode not in [pandaserver.taskbuffer.ErrorCode.EC_WorkerDone]) and \
                       (self.job.transExitCode  in [0,'0','NULL']):
                        self.job.ddmErrorCode = pandaserver.dataservice.ErrorCode.EC_Adder
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
                guidMap[lfn] = guid
                # get metadata
                nevents = None
                for meta in file.getElementsByTagName('metadata'):
                    # get fsize
                    name = str(meta.getAttribute('att_name'))
                    if name == 'events':
                        nevents = long(meta.getAttribute('att_value'))
                        nEventsMap[lfn] = nevents
                        break
        except Exception:
            pass
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
                    except Exception:
                        pass
                    try:
                        guid = str(jsonSubFileItem['file_guid'])
                        guidMap[lfn] = guid
                    except Exception:
                        pass
        except Exception:
            pass
        self.logger.debug('nEventsMap=%s' % str(nEventsMap))
        self.logger.debug('guidMap=%s' % str(guidMap))
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
                    elif self.job.prodSourceLabel in ['managed','test'] + JobUtils.list_ptest_prod_sources:
                        # failed by pilot
                        file.status = 'failed'
            elif file.type == 'output' or file.type == 'log':
                # add only log file for failed jobs
                if self.jobStatus == 'failed' and file.type != 'log':
                    file.status = 'failed'
                    continue
                # set failed if it is missing in XML
                if not file.lfn in lfns:
                    if self.job.jobStatus == 'finished' and \
                            (EventServiceUtils.isEventServiceJob(self.job) or EventServiceUtils.isJumboJob(self.job)):
                        # unset file status for ES jobs
                        pass
                    elif file.isAllowedNoOutput():
                        # allowed not to be produced
                        file.status = 'nooutput'
                        self.logger.debug('set {0} to status={1}'.format(file.lfn,file.status))
                    else:
                        file.status = 'failed'
                        self.job.jobStatus = 'failed'
                        self.job.ddmErrorCode = pandaserver.dataservice.ErrorCode.EC_Adder
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
                    if file.lfn in fullLfnMap:
                        file.lfn = fullLfnMap[file.lfn]
                    # add SURL to extraInfo
                    self.extraInfo['surl'][file.lfn] = surl
                    # add nevents 
                    if file.lfn in nEventsMap:
                        self.extraInfo['nevents'][file.lfn] = nEventsMap[file.lfn]
                except Exception:
                    # status
                    file.status = 'failed'
                    type, value, traceBack = sys.exc_info()
                    self.logger.error(": %s %s" % (type,value))
                # set lumi block number
                if lumiBlockNr is not None and file.status != 'failed':
                    self.extraInfo['lbnr'][file.lfn] = lumiBlockNr 
        self.extraInfo['guid'] = guidMap
        # check consistency between XML and filesTable
        for lfn in lfns:
            if not lfn in fileList:
                self.logger.error("%s is not found in filesTable" % lfn)
                self.job.jobStatus = 'failed'
                for tmpFile in self.job.Files:
                    tmpFile.status = 'failed'
                self.job.ddmErrorCode = pandaserver.dataservice.ErrorCode.EC_Adder
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
                for origLFN in origOutputs:
                    tmpPatt = '^{0}\.*_\d+$'.format(origLFN)
                    if re.search(tmpPatt,newLFN) is not None:
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
