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
import brokerage.broker
from dataservice import DynDataDistributer
from dataservice.MailUtils import MailUtils
from dataservice.Notifier import Notifier
from taskbuffer.JobSpec import JobSpec
from dataservice.datriHandler import datriHandler


from config import panda_config
from pandalogger.PandaLogger import PandaLogger

# logger
_logger = PandaLogger().getLogger('EventPicker')
DynDataDistributer.initLogger(_logger)


class EventPicker:
    # constructor
    def __init__(self,taskBuffer,siteMapper,evpFileName,ignoreError):
        self.taskBuffer      = taskBuffer
        self.siteMapper      = siteMapper
        self.ignoreError     = ignoreError
        self.evpFileName     = evpFileName
        self.token           = datetime.datetime.utcnow().isoformat(' ')        
        self.pd2p            = DynDataDistributer.DynDataDistributer([],self.taskBuffer,self.siteMapper,
                                                                     token=self.token)
        self.userDatasetName = ''
        self.creationTime    = ''
        self.params          = ''
        self.lockedBy        = ''
        self.evpFile         = None

    # main
    def run(self):
        try:
            self.putLog('start %s' % self.evpFileName)            
            # lock evp file
            self.evpFile = open(self.evpFileName)
            try:
                fcntl.flock(self.evpFile.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
            except:
                # relase
                self.putLog("cannot lock %s" % self.evpFileName)
                self.evpFile.close()
                return True
            # options
            runEvtList          = []
            eventPickDataType   = ''
            eventPickStreamName = ''
            eventPickDS         = []
            eventPickAmiTag     = ''
            inputFileList       = []
            # read evp file
            for tmpLine in self.evpFile:
                tmpMatch = re.search('^([^=]+)=(.+)$',tmpLine)
                # check format
                if tmpMatch == None:
                    continue
                tmpItems = tmpMatch.groups()
                if tmpItems[0] == 'runEvent':
                    # get run and event number
                    tmpRunEvt = tmpItems[1].split(',')
                    if len(tmpRunEvt) == 2:
                        runEvtList.append(tmpRunEvt)
                elif tmpItems[0] == 'eventPickDataType':
                    # data type
                    eventPickDataType = tmpItems[1]
                elif tmpItems[0] == 'eventPickStreamName':
                    # stream name
                    eventPickStreamName = tmpItems[1]
                elif tmpItems[0] == 'eventPickDS':
                    # dataset pattern
                    eventPickDS = tmpItems[1].split(',')
                elif tmpItems[0] == 'eventPickAmiTag':
                    # AMI tag
                    eventPickAmiTag = tmpItems[1]
                elif tmpItems[0] == 'userName':
                    # user name
                    self.userDN = tmpItems[1]
                    self.putLog("user=%s" % self.userDN)
                elif tmpItems[0] == 'userDatasetName':
                    # user dataset name
                    self.userDatasetName = tmpItems[1]
                elif tmpItems[0] == 'lockedBy':
                    # client name
                    self.lockedBy = tmpItems[1]
                elif tmpItems[0] == 'creationTime':
                    # creation time
                    self.creationTime = tmpItems[1]
                elif tmpItems[0] == 'params':
                    # parameters
                    self.params = tmpItems[1]
                elif tmpItems[0] == 'inputFileList':
                    # input file list
                    inputFileList = tmpItems[1].split(',')
                    try:
                        inputFileList.remove('')
                    except:
                        pass
            # convert run/event list to dataset/file list
            tmpRet,locationMap,allFiles = self.pd2p.convertEvtRunToDatasets(runEvtList,
                                                                            eventPickDataType,
                                                                            eventPickStreamName,
                                                                            eventPickDS,
                                                                            eventPickAmiTag)
            if not tmpRet:
                self.endWithError('Failed to convert the run/event list to a dataset/file list')
                return False
            # use only files in the list
            if inputFileList != []:
                tmpAllFiles = []
                for tmpFile in allFiles:
                    if tmpFile['lfn'] in inputFileList:
                        tmpAllFiles.append(tmpFile)
                allFiles = tmpAllFiles        
            # make dataset container
            tmpRet = self.pd2p.registerDatasetContainerWithDatasets(self.userDatasetName,allFiles,locationMap)
            if not tmpRet:
                self.endWithError('Failed to make a dataset container %s' % self.userDatasetName)
                return False
            # get candidates
            tmpRet,candidateMaps = self.pd2p.getCandidates(self.userDatasetName,checkUsedFile=False,
                                                           useHidden=True)
            if not tmpRet:
                self.endWithError('Failed to find candidate for destination')
                return False
            # collect all candidates
            allCandidates = [] 
            for tmpDS,tmpDsVal in candidateMaps.iteritems():
                for tmpCloud,tmpCloudVal in tmpDsVal.iteritems():
                    for tmpSiteName in tmpCloudVal[0]:
                        if not tmpSiteName in allCandidates:
                            allCandidates.append(tmpSiteName)
            if allCandidates == []:
                self.endWithError('No candidate for destination')
                return False
            # get size of dataset container
            tmpRet,totalInputSize = self.pd2p.getDatasetSize(self.userDatasetName)
            if not tmpRet:
                self.endWithError('Failed to get the size of %s' % self.userDatasetName)
                return False
            # run brokerage
            tmpJob = JobSpec()
            tmpJob.AtlasRelease = ''
            self.putLog("run brokerage for %s" % tmpDS)
            brokerage.broker.schedule([tmpJob],self.taskBuffer,self.siteMapper,True,allCandidates,
                                      True,datasetSize=totalInputSize)
            if tmpJob.computingSite.startswith('ERROR'):
                self.endWithError('brokerage failed with %s' % tmpJob.computingSite)
                return False
            self.putLog("site -> %s" % tmpJob.computingSite)
            # send request to DaTRI
            if self.lockedBy.startswith('ganga'):
                tmpHandler = datriHandler(type='ganga')
            else:
                tmpHandler = datriHandler(type='pathena')
            tmpMsg = "%s ds=%s site=%s id=%s" % ('datriHandler.sendRequest',
                                                 self.userDatasetName,
                                                 self.siteMapper.getSite(tmpJob.computingSite).ddm,
                                                 self.userDN)
            self.putLog(tmpMsg)
            tmpHandler.setParameters(data_pattern=self.userDatasetName,
                                     site=self.siteMapper.getSite(tmpJob.computingSite).ddm,
                                     userid=self.userDN)
            nTry = 3
            for iTry in range(nTry):
                dhStatus,dhOut = tmpHandler.sendRequest()
                # succeeded
                if dhStatus == 0 or "such request is exist" in dhOut:
                    self.putLog("%s %s" % (dhStatus,dhOut))
                    break
                if iTry+1 < nTry:
                    # sleep
                    time.sleep(60)
                else:
                    # final attempt failed
                    self.endWithError('Failed to send request to DaTRI : %s %s' % (dhStatus,dhOut))
                    return False
            # send email notification for success
            tmpMsg =  'A transfer request was successfully sent to DaTRI.\n'
            tmpMsg += 'You will receive a notification from DaTRI when it completed.'
            self.sendEmail(True,tmpMsg)
            try:
                # unlock and delete evp file
                fcntl.flock(self.evpFile.fileno(),fcntl.LOCK_UN)
                self.evpFile.close()
                os.remove(self.evpFileName)
            except:
                pass
            # successfully terminated
            self.putLog("end %s" % self.evpFileName)
            return True
        except:
            errType,errValue = sys.exc_info()[:2]
            self.endWithError('Got exception %s:%s' % (errType,errValue))
            return False


    # end with error
    def endWithError(self,message):
        self.putLog(message,'error')
        # unlock evp file
        try:
            fcntl.flock(self.evpFile.fileno(),fcntl.LOCK_UN)
            self.evpFile.close()
            if not self.ignoreError:
                # remove evp file
                os.remove(self.evpFileName)
                # send email notification
                self.sendEmail(False,message)
        except:
            pass
        self.putLog('end %s' % self.evpFileName)
        

    # put log
    def putLog(self,msg,type='debug'):
        tmpMsg = self.token+' '+msg
        if type == 'error':
            _logger.error(tmpMsg)
        else:
            _logger.debug(tmpMsg)


    # send email notification
    def sendEmail(self,isSucceeded,message):
        # mail address
        toAdder = Notifier(self.taskBuffer,None,[]).getEmail(self.userDN)
        if toAdder == '':
            self.putLog('cannot find email address for %s' % self.userDN,'error')
            return
        # subject
        mailSubject = "PANDA notification for Event-Picking Request"
        # message
        mailBody = "Hello,\n\nHere is your request status for event picking\n\n"
        if isSucceeded:
            mailBody += "Status  : Passed to DaTRI\n"
        else:
            mailBody += "Status  : Failed\n"
        mailBody +=     "Created : %s\n" % self.creationTime
        mailBody +=     "Ended   : %s\n" % datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        mailBody +=     "Dataset : %s\n" % self.userDatasetName
        mailBody +=     "\n"        
        mailBody +=     "Parameters : %s %s\n" % (self.lockedBy,self.params)
        mailBody +=     "\n"
        mailBody +=     "%s\n" % message
        # send
        retVal = MailUtils().send(toAdder,mailSubject,mailBody)
        # return
        return
