'''
add data to dataset

'''

import os
import re
import sys
import fcntl
import datetime
import traceback
import pandaserver.brokerage.broker
from pandaserver.dataservice import DynDataDistributer
from pandaserver.dataservice.MailUtils import MailUtils
from pandaserver.dataservice.Notifier import Notifier
from pandaserver.taskbuffer.JobSpec import JobSpec
from pandaserver.userinterface import Client

from pandaserver.dataservice.DDM import rucioAPI

from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandalogger.LogWrapper import LogWrapper

# logger
_logger = PandaLogger().getLogger('EventPicker')


class EventPicker:
    # constructor
    def __init__(self,taskBuffer,siteMapper,evpFileName,ignoreError):
        self.taskBuffer      = taskBuffer
        self.siteMapper      = siteMapper
        self.ignoreError     = ignoreError
        self.evpFileName     = evpFileName
        self.token           = datetime.datetime.utcnow().isoformat(' ')        
        # logger
        self.logger = LogWrapper(_logger,self.token)
        self.pd2p            = DynDataDistributer.DynDataDistributer([],self.taskBuffer,self.siteMapper,
                                                                     token=' ',logger=self.logger)
        self.userDatasetName = ''
        self.creationTime    = ''
        self.params          = ''
        self.lockedBy        = ''
        self.evpFile         = None
        self.userTaskName    = ''
        # message buffer
        self.msgBuffer       = []
        self.lineLimit       = 100
        # JEDI
        self.jediTaskID      = None


    # main
    def run(self):
        try:
            self.putLog('start %s' % self.evpFileName)            
            # lock evp file
            self.evpFile = open(self.evpFileName)
            try:
                fcntl.flock(self.evpFile.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
            except Exception:
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
            eventPickNumSites   = 1
            inputFileList       = []
            tagDsList           = []
            tagQuery            = ''
            tagStreamRef        = ''
            skipDaTRI           = False
            runEvtGuidMap       = {}
            ei_api              = ''
            # read evp file
            for tmpLine in self.evpFile:
                tmpMatch = re.search('^([^=]+)=(.+)$',tmpLine)
                # check format
                if tmpMatch is None:
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
                elif tmpItems[0] == 'eventPickNumSites':
                    # the number of sites where datasets are distributed
                    try:
                        eventPickNumSites = int(tmpItems[1])
                    except Exception:
                        pass
                elif tmpItems[0] == 'userName':
                    # user name
                    self.userDN = tmpItems[1]
                    self.putLog("user=%s" % self.userDN)
                elif tmpItems[0] == 'userTaskName':
                    # user task name
                    self.userTaskName = tmpItems[1]
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
                elif tmpItems[0] == 'ei_api':
                    # ei api parameter for MC
                    ei_api = tmpItems[1]
                elif tmpItems[0] == 'inputFileList':
                    # input file list
                    inputFileList = tmpItems[1].split(',')
                    try:
                        inputFileList.remove('')
                    except Exception:
                        pass
                elif tmpItems[0] == 'tagDS':
                    # TAG dataset
                    tagDsList = tmpItems[1].split(',')
                elif tmpItems[0] == 'tagQuery':
                    # query for TAG
                    tagQuery = tmpItems[1]
                elif tmpItems[0] == 'tagStreamRef':
                    # StreamRef for TAG
                    tagStreamRef = tmpItems[1]
                    if not tagStreamRef.endswith('_ref'):
                        tagStreamRef += '_ref'
                elif tmpItems[0] == 'runEvtGuidMap':
                    # GUIDs
                    try:
                        runEvtGuidMap = eval(tmpItems[1])
                    except Exception:
                        pass
            # extract task name
            if self.userTaskName == '' and self.params != '':
                try:
                    tmpMatch = re.search('--outDS(=| ) *([^ ]+)',self.params)
                    if tmpMatch is not None:
                        self.userTaskName = tmpMatch.group(2)
                        if not self.userTaskName.endswith('/'):
                            self.userTaskName += '/'
                except Exception:
                    pass
            # suppress DaTRI
            if self.params != '':
                if '--eventPickSkipDaTRI' in self.params:
                    skipDaTRI = True
            # get compact user name
            compactDN = self.taskBuffer.cleanUserID(self.userDN)
            # get jediTaskID
            self.jediTaskID = self.taskBuffer.getTaskIDwithTaskNameJEDI(compactDN,self.userTaskName)
            # convert run/event list to dataset/file list
            tmpRet,locationMap,allFiles = self.pd2p.convertEvtRunToDatasets(runEvtList,
                                                                            eventPickDataType,
                                                                            eventPickStreamName,
                                                                            eventPickDS,
                                                                            eventPickAmiTag,
                                                                            self.userDN,
                                                                            runEvtGuidMap,
                                                                            ei_api
                                                                            )
            if not tmpRet:
                if 'isFatal' in locationMap and locationMap['isFatal'] == True:
                    self.ignoreError = False
                self.endWithError('Failed to convert the run/event list to a dataset/file list')
                return False
            # use only files in the list
            if inputFileList != []:
                tmpAllFiles = []
                for tmpFile in allFiles:
                    if tmpFile['lfn'] in inputFileList:
                        tmpAllFiles.append(tmpFile)
                allFiles = tmpAllFiles        
            # remove redundant CN from DN
            tmpDN = self.userDN
            tmpDN = re.sub('/CN=limited proxy','',tmpDN)
            tmpDN = re.sub('(/CN=proxy)+$','',tmpDN)
            # make dataset container
            tmpRet = self.pd2p.registerDatasetContainerWithDatasets(self.userDatasetName,allFiles,
                                                                    locationMap,
                                                                    nSites=eventPickNumSites,
                                                                    owner=tmpDN)
            if not tmpRet:
                self.endWithError('Failed to make a dataset container %s' % self.userDatasetName)
                return False
            # skip DaTRI
            if skipDaTRI:
                # successfully terminated
                self.putLog("skip DaTRI")
                # update task
                self.taskBuffer.updateTaskModTimeJEDI(self.jediTaskID)
            else:
                # get candidates
                tmpRet,candidateMaps = self.pd2p.getCandidates(self.userDatasetName,checkUsedFile=False,
                                                               useHidden=True)
                if not tmpRet:
                    self.endWithError('Failed to find candidate for destination')
                    return False
                # collect all candidates
                allCandidates = [] 
                for tmpDS in candidateMaps:
                    tmpDsVal = candidateMaps[tmpDS]
                    for tmpCloud in tmpDsVal:
                        tmpCloudVal = tmpDsVal[tmpCloud]
                        for tmpSiteName in tmpCloudVal[0]:
                            if not tmpSiteName in allCandidates:
                                allCandidates.append(tmpSiteName)
                if allCandidates == []:
                    self.endWithError('No candidate for destination')
                    return False
                # get list of dataset (container) names
                if eventPickNumSites > 1:
                    # decompose container to transfer datasets separately
                    tmpRet,tmpOut = self.pd2p.getListDatasetReplicasInContainer(self.userDatasetName) 
                    if not tmpRet:
                        self.endWithError('Failed to get replicas in %s' % self.userDatasetName)
                        return False
                    userDatasetNameList = list(tmpOut)
                else:
                    # transfer container at once
                    userDatasetNameList = [self.userDatasetName]
                # loop over all datasets
                sitesUsed = []
                for tmpUserDatasetName in userDatasetNameList:
                    # get size of dataset container
                    tmpRet,totalInputSize = rucioAPI.getDatasetSize(tmpUserDatasetName)
                    if not tmpRet:
                        self.endWithError('Failed to get the size of {0} with {1}'.format(tmpUserDatasetName, totalInputSize))
                        return False
                    # run brokerage
                    tmpJob = JobSpec()
                    tmpJob.AtlasRelease = ''
                    self.putLog("run brokerage for %s" % tmpDS)
                    pandaserver.brokerage.broker.schedule([tmpJob],self.taskBuffer,self.siteMapper,True,allCandidates,
                                              True,datasetSize=totalInputSize)
                    if tmpJob.computingSite.startswith('ERROR'):
                        self.endWithError('brokerage failed with %s' % tmpJob.computingSite)
                        return False
                    self.putLog("site -> %s" % tmpJob.computingSite)
                    # send transfer request
                    try:
                        tmpDN = rucioAPI.parse_dn(tmpDN)
                        tmpStatus,userInfo = rucioAPI.finger(tmpDN)
                        if not tmpStatus:
                            raise RuntimeError('user info not found for {0} with {1}'.format(tmpDN,userInfo))
                        tmpDN = userInfo['nickname']
                        tmpDQ2ID = self.siteMapper.getSite(tmpJob.computingSite).ddm_input
                        tmpMsg = "%s ds=%s site=%s id=%s" % ('registerDatasetLocation for DaTRI ',
                                                             tmpUserDatasetName,
                                                             tmpDQ2ID,
                                                             tmpDN)
                        self.putLog(tmpMsg)
                        rucioAPI.registerDatasetLocation(tmpDS,[tmpDQ2ID],lifetime=14,owner=tmpDN,
                                                         activity="User Subscriptions")
                        self.putLog('OK')
                    except Exception:
                        errType,errValue = sys.exc_info()[:2]
                        tmpStr = 'Failed to send transfer request : %s %s' % (errType,errValue)
                        tmpStr.strip()
                        tmpStr += traceback.format_exc()
                        self.endWithError(tmpStr)
                        return False
                    # list of sites already used
                    sitesUsed.append(tmpJob.computingSite)
                    self.putLog("used %s sites" % len(sitesUsed))
                    # set candidates
                    if len(sitesUsed) >= eventPickNumSites:
                        # reset candidates to limit the number of sites
                        allCandidates = sitesUsed
                        sitesUsed = []
                    else:
                        # remove site
                        allCandidates.remove(tmpJob.computingSite)
                # send email notification for success
                tmpMsg =  'A transfer request was successfully sent to Rucio.\n'
                tmpMsg += 'Your task will get started once transfer is completed.'
                self.sendEmail(True,tmpMsg)
            try:
                # unlock and delete evp file
                fcntl.flock(self.evpFile.fileno(),fcntl.LOCK_UN)
                self.evpFile.close()
                os.remove(self.evpFileName)
            except Exception:
                pass
            # successfully terminated
            self.putLog("end %s" % self.evpFileName)
            return True
        except Exception:
            errType,errValue = sys.exc_info()[:2]
            self.endWithError('Got exception %s:%s %s' % (errType,errValue,traceback.format_exc()))
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
        except Exception:
            pass
        # upload log
        if self.jediTaskID is not None:
            outLog = self.uploadLog()
            self.taskBuffer.updateTaskErrorDialogJEDI(self.jediTaskID,'event picking failed. '+outLog)
            # update task
            if not self.ignoreError:
                self.taskBuffer.updateTaskModTimeJEDI(self.jediTaskID,'tobroken')
            self.putLog(outLog)
        self.putLog('end %s' % self.evpFileName)
        

    # put log
    def putLog(self,msg,type='debug'):
        tmpMsg = msg
        if type == 'error':
            self.logger.error(tmpMsg)
        else:
            self.logger.debug(tmpMsg)


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
            mailBody += "Status  : Passed to Rucio\n"
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

    
    # upload log
    def uploadLog(self):
        if self.jediTaskID is None:
            return 'cannot find jediTaskID'
        strMsg = self.logger.dumpToString()
        s,o = Client.uploadLog(strMsg,self.jediTaskID)
        if s != 0:
            return "failed to upload log with {0}.".format(s)
        if o.startswith('http'):
            return '<a href="{0}">log</a>'.format(o)
        return o
