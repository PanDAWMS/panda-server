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

import Closer

from config import panda_config
from pandalogger.PandaLogger import PandaLogger
from pandalogger.LogWrapper import LogWrapper

# logger
_logger = PandaLogger().getLogger('Adder')

   
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
        self.extraInfo = {'surl':{}}
        # exstract attemptNr
        try:
            tmpAttemptNr = self.xmlFile.split('/')[-1].split('_')[-1]
            if re.search('^\d+$',tmpAttemptNr) != None:
                self.attemptNr = int(tmpAttemptNr)
        except:
            pass
        # logger
        self.logger = LogWrapper(_logger,self.jobID)
        
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
            # query job
            self.job = self.taskBuffer.peekJobs([self.jobID],fromDefined=False,
                                                fromArchived=False,
                                                fromWaiting=False,
                                                forAnal=True)[0]
            # Hassen comment for debug
            self.logger.debug('trying to call the plugins for %s' % self.job)  
            # check if job has finished
            if self.job == None:
                self.logger.debug(': job not found in DB')
            #Hassen commented for debug
            elif self.job.jobStatus in ['finished','failed','unknown','cancelled']:
                self.logger.error(': invalid state -> %s' % self.job.jobStatus)
            elif self.attemptNr != None and self.job.attemptNr != self.attemptNr:
                self.logger.error('wrong attemptNr -> job=%s <> %s' % (self.job.attemptNr,self.attemptNr))
            else:
                # set job status
                if not self.job.jobStatus in ['transferring']:
                    self.job.jobStatus = self.jobStatus
                # parse XML
                parseResult = self.parseXML()
                if parseResult < 2:
                    # instantiate concrete plugin
                    if self.job.VO == 'cms':
                        from AdderCmsPlugin import AdderCmsPlugin
                        adderPlugin = AdderCmsPlugin(self.job,extraInfo=self.extraInfo)
                    else:
                        from AdderAtlasPlugin import AdderAtlasPlugin
                        adderPlugin = AdderAtlasPlugin(self.job,
                                                       taskBuffer=self.taskBuffer,
                                                       siteMapper=self.siteMapper,
                                                       extraInfo=self.extraInfo,
                                                       logger=self.logger)
                    # execute
                    adderPlugin.execute()
                    addResult = adderPlugin.result
                    self.logger.debug('plugin done with %s' % (addResult.statusCode))
                    # ignore temporary errors
                    if self.ignoreTmpError and addResult.isTemporary():
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
                    if not addResult.isSucceeded():
                        self.job.jobStatus = 'failed'
                # set file status for failed jobs or failed transferring jobs
                if self.job.jobStatus == 'failed' or self.jobStatus == 'failed':
                    self.job.jobStatus = 'failed'
                    for file in self.job.Files:
                        if file.type in ['output','log']:
                            file.status = 'failed'
                else:
                    # reset errors
                    self.job.jobDispatcherErrorCode = 0
                    self.job.jobDispatcherErrorDiag = 'NULL'
                    # set status for transferring
                    if addResult.transferringFiles != []:
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
                retU = self.taskBuffer.updateJobs([self.job],False)
                self.logger.debug("retU: %s" % retU)
                # failed
                if not retU[0]:
                    self.logger.error('failed to update DB')
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
                        # extract base LFN since LFN was changed to full LFN for CMS
                        baseLFN = file.lfn.split('/')[-1]
                        guidList.append({'lfn':baseLFN,'guid':file.GUID,'type':file.type,
                                         'checksum':file.checksum,'md5sum':file.md5sum,
                                         'fsize':file.fsize,'scope':file.scope})
                if guidList != []:
                    retG = self.taskBuffer.setGUIDs(guidList)
                if destDBList != []:
                    # start Closer
                    cThr = Closer.Closer(self.taskBuffer,destDBList,self.job)
                    self.logger.debug("start Closer")
                    cThr.start()
                    cThr.join()
                    self.logger.debug("end Closer")
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
    # 0: succeeded, 1: harmless error to exit, 2: fatal error
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
        try:
            root  = xml.dom.minidom.parse(self.xmlFile)
            files = root.getElementsByTagName('File')
            for file in files:
                # get GUID
                guid = str(file.getAttribute('ID'))
                self.logger.debug(guid)
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
                # error check
                if (not lfn in inputLFNs) and (fsize == None or (md5sum == None and adler32 == None)):
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
                    file.status = 'failed'
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
                except:
                    # status
                    file.status = 'failed'
                    type, value, traceBack = sys.exc_info()
                    self.logger.error(": %s %s" % (type,value))
        # check consistency between XML and filesTable
        for lfn in lfns:
            self.logger.debug("lfn %s fileList %s" %(lfn, fileList))
            if not lfn in fileList:
                self.logger.error("%s is not found in filesTable" % lfn)
                self.job.jobStatus = 'failed'
                for tmpFile in self.job.Files:
                    tmpFile.status = 'failed'
                self.job.ddmErrorCode = ErrorCode.EC_Adder
                self.job.ddmErrorDiag = "pilot XML is inconsistent with filesTable"
                return 2
        # return
        self.logger.debug("parseXML end")
        return 0
        
