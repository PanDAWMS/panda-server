'''
update dataset DB, and then close dataset and start Activator if needed

'''

import re
import sys
import time
import urllib
import commands
import threading
from DDM import ddm
import Notifier
import RetryMaker
from Activator import Activator
from pandalogger.PandaLogger import PandaLogger
from taskbuffer.JobSpec import JobSpec
from taskbuffer.FileSpec import FileSpec
from taskbuffer.DatasetSpec import DatasetSpec
from brokerage.SiteMapper import SiteMapper
from config import panda_config
import brokerage.broker_util

# logger
_logger = PandaLogger().getLogger('Closer')

def initLogger(pLogger):
    # redirect logging to parent as it doesn't work in nested threads
    global _logger
    _logger = pLogger
    Notifier.initLogger(_logger)
    RetryMaker.initLogger(_logger)
    

class Closer (threading.Thread):
    # constructor
    def __init__(self,taskBuffer,destinationDBlocks,job,pandaDDM=False,datasetMap={}):
        threading.Thread.__init__(self)
        self.taskBuffer = taskBuffer
        self.destinationDBlocks = destinationDBlocks
        self.job = job
        self.pandaID = job.PandaID
        self.pandaDDM = pandaDDM
        self.siteMapper = None
        self.datasetMap = datasetMap


    # main
    def run(self):
        try:
            _logger.debug('%s Start %s' % (self.pandaID,self.job.jobStatus))
            flagComplete    = True
            ddmJobs         = []
            topUserDsList   = []
            usingMerger     = False        
            disableNotifier = False
            firstIndvDS     = True
            for destinationDBlock in self.destinationDBlocks:
                dsList = []
                _logger.debug('%s start %s' % (self.pandaID,destinationDBlock))
                # ignore tid datasets
                if re.search('_tid[\d_]+$',destinationDBlock):
                    _logger.debug('%s skip %s' % (self.pandaID,destinationDBlock))                
                    continue
                # ignore HC datasets
                if re.search('^hc_test\.',destinationDBlock) != None or re.search('^user\.gangarbt\.',destinationDBlock) != None:
                    if re.search('_sub\d+$',destinationDBlock) == None and re.search('\.lib$',destinationDBlock) == None:
                        _logger.debug('%s skip HC %s' % (self.pandaID,destinationDBlock))                
                        continue
                # query dataset
                if self.datasetMap.has_key(destinationDBlock):
                    dataset = self.datasetMap[destinationDBlock]
                else:
                    dataset = self.taskBuffer.queryDatasetWithMap({'name':destinationDBlock})
                if dataset == None:
                    _logger.error('%s Not found : %s' % (self.pandaID,destinationDBlock))
                    flagComplete = False
                    continue
                # skip tobedeleted/tobeclosed 
                if dataset.status in ['cleanup','tobeclosed','completed']:
                    _logger.debug('%s skip %s due to %s' % (self.pandaID,destinationDBlock,dataset.status))
                    continue
                dsList.append(dataset)
                # sort
                dsList.sort()
                # count number of completed files
                notFinish = self.taskBuffer.countFilesWithMap({'destinationDBlock':destinationDBlock,
                                                               'status':'unknown'})
                if notFinish < 0:
                    _logger.error('%s Invalid DB return : %s' % (self.pandaID,notFinish))
                    flagComplete = False                
                    continue
                # check if completed
                _logger.debug('%s notFinish:%s' % (self.pandaID,notFinish))
                if self.job.destinationSE == 'local' and self.job.prodSourceLabel in ['user','panda']:
                    # close non-DQ2 destinationDBlock immediately
                    finalStatus = 'closed'
                elif self.job.lockedby == 'jedi' and self.isTopLevelDS(destinationDBlock):
                    # set it closed in order not to trigger DDM cleanup. It will be closed by JEDI
                    finalStatus = 'closed'
                elif self.job.prodSourceLabel in ['user'] and "--mergeOutput" in self.job.jobParameters \
                         and self.job.processingType != 'usermerge':
                    # merge output files
                    if firstIndvDS:
                        # set 'tobemerged' to only the first dataset to avoid triggering many Mergers for --individualOutDS
                        finalStatus = 'tobemerged'
                        firstIndvDS = False
                    else:
                        finalStatus = 'tobeclosed'
                    # set merging to top dataset
                    usingMerger = True
                    # disable Notifier
                    disableNotifier = True
                else:
                    # set status to 'tobeclosed' to trigger DQ2 closing
                    finalStatus = 'tobeclosed'
                if notFinish==0: 
                    _logger.debug('%s set %s to dataset : %s' % (self.pandaID,finalStatus,destinationDBlock))
                    # set status
                    dataset.status = finalStatus
                    # update dataset in DB
                    retT = self.taskBuffer.updateDatasets(dsList,withLock=True,withCriteria="status<>:crStatus AND status<>:lockStatus ",
                                                          criteriaMap={':crStatus':finalStatus,':lockStatus':'locked'})
                    if len(retT) > 0 and retT[0]==1:
                        # close user datasets
                        if self.job.prodSourceLabel in ['user'] and self.job.destinationDBlock.endswith('/') \
                               and (dataset.name.startswith('user') or dataset.name.startswith('group')):
                            # get top-level user dataset 
                            topUserDsName = re.sub('_sub\d+$','',dataset.name)
                            # update if it is the first attempt
                            if topUserDsName != dataset.name and not topUserDsName in topUserDsList and self.job.lockedby != 'jedi':
                                topUserDs = self.taskBuffer.queryDatasetWithMap({'name':topUserDsName})
                                if topUserDs != None:
                                    # check status
                                    if topUserDs.status in ['completed','cleanup','tobeclosed',
                                                            'tobemerged','merging']:
                                        _logger.debug('%s skip %s due to status=%s' % (self.pandaID,topUserDsName,topUserDs.status))
                                    else:
                                        # set status
                                        if self.job.processingType.startswith('gangarobot') or \
                                               self.job.processingType.startswith('hammercloud'):
                                            # not trigger freezing for HC datasets so that files can be appended
                                            topUserDs.status = 'completed'
                                        elif not usingMerger:
                                            topUserDs.status = finalStatus
                                        else:
                                            topUserDs.status = 'merging'
                                        # append to avoid repetition
                                        topUserDsList.append(topUserDsName)
                                        # update DB
                                        retTopT = self.taskBuffer.updateDatasets([topUserDs],withLock=True,withCriteria="status<>:crStatus",
                                                                                 criteriaMap={':crStatus':topUserDs.status})
                                        if len(retTopT) > 0 and retTopT[0]==1:
                                            _logger.debug('%s set %s to top dataset : %s' % (self.pandaID,topUserDs.status,topUserDsName))
                                        else:
                                            _logger.debug('%s failed to update top dataset : %s' % (self.pandaID,topUserDsName))
                            # get parent dataset for merge job
                            if self.job.processingType == 'usermerge':
                                tmpMatch = re.search('--parentDS ([^ \'\"]+)',self.job.jobParameters)
                                if tmpMatch == None:
                                    _logger.error('%s failed to extract parentDS' % self.pandaID)
                                else:
                                    unmergedDsName = tmpMatch.group(1)
                                    # update if it is the first attempt
                                    if not unmergedDsName in topUserDsList:
                                        unmergedDs = self.taskBuffer.queryDatasetWithMap({'name':unmergedDsName})
                                        if unmergedDs == None:
                                            _logger.error('%s failed to get parentDS=%s from DB' % (self.pandaID,unmergedDsName))
                                        else:
                                            # check status
                                            if unmergedDs.status in ['completed','cleanup','tobeclosed']:
                                                _logger.debug('%s skip %s due to status=%s' % (self.pandaID,unmergedDsName,unmergedDs.status))
                                            else:
                                                # set status
                                                unmergedDs.status = finalStatus
                                                # append to avoid repetition
                                                topUserDsList.append(unmergedDsName)
                                                # update DB
                                                retTopT = self.taskBuffer.updateDatasets([unmergedDs],withLock=True,withCriteria="status<>:crStatus",
                                                                                         criteriaMap={':crStatus':unmergedDs.status})
                                                if len(retTopT) > 0 and retTopT[0]==1:
                                                    _logger.debug('%s set %s to parent dataset : %s' % (self.pandaID,unmergedDs.status,unmergedDsName))
                                                else:
                                                    _logger.debug('%s failed to update parent dataset : %s' % (self.pandaID,unmergedDsName))
                        if self.pandaDDM and self.job.prodSourceLabel=='managed':
                            # instantiate SiteMapper
                            if self.siteMapper == None:
                                self.siteMapper = SiteMapper(self.taskBuffer)
                            # get file list for PandaDDM
                            retList = self.taskBuffer.queryFilesWithMap({'destinationDBlock':destinationDBlock})
                            lfnsStr = ''
                            guidStr = ''
                            for tmpFile in retList:
                                if tmpFile.type in ['log','output']:
                                    lfnsStr += '%s,' % tmpFile.lfn
                                    guidStr += '%s,' % tmpFile.GUID
                            if lfnsStr != '':
                                guidStr = guidStr[:-1]
                                lfnsStr = lfnsStr[:-1]
                                # create a DDM job
                                ddmjob = JobSpec()
                                ddmjob.jobDefinitionID   = int(time.time()) % 10000
                                ddmjob.jobName           = "%s" % commands.getoutput('uuidgen')
                                ddmjob.transformation    = 'http://pandaserver.cern.ch:25080/trf/mover/run_dq2_cr'
                                ddmjob.destinationDBlock = 'testpanda.%s' % ddmjob.jobName
                                ddmjob.computingSite     = "BNL_ATLAS_DDM"
                                ddmjob.destinationSE     = ddmjob.computingSite
                                ddmjob.currentPriority   = 200000
                                ddmjob.prodSourceLabel   = 'ddm'
                                ddmjob.transferType      = 'sub'
                                # append log file
                                fileOL = FileSpec()
                                fileOL.lfn = "%s.job.log.tgz" % ddmjob.jobName
                                fileOL.destinationDBlock = ddmjob.destinationDBlock
                                fileOL.destinationSE     = ddmjob.destinationSE
                                fileOL.dataset           = ddmjob.destinationDBlock
                                fileOL.type = 'log'
                                ddmjob.addFile(fileOL)
                                # make arguments
                                dstDQ2ID = 'BNLPANDA'
                                srcDQ2ID = self.siteMapper.getSite(self.job.computingSite).ddm
                                callBackURL = 'https://%s:%s/server/panda/datasetCompleted?vuid=%s&site=%s' % \
                                              (panda_config.pserverhost,panda_config.pserverport,
                                               dataset.vuid,dstDQ2ID)
                                _logger.debug(callBackURL)
                                # set src/dest
                                ddmjob.sourceSite      = srcDQ2ID
                                ddmjob.destinationSite = dstDQ2ID
                                # if src==dst, send callback without ddm job
                                if dstDQ2ID == srcDQ2ID:
                                    comout = commands.getoutput('curl -k %s' % callBackURL)
                                    _logger.debug(comout)
                                else:
                                    # run dq2_cr
                                    callBackURL = urllib.quote(callBackURL)
                                    # get destination dir
                                    destDir = brokerage.broker_util._getDefaultStorage(self.siteMapper.getSite(self.job.computingSite).dq2url)
                                    argStr = "-s %s -r %s --guids %s --lfns %s --callBack %s -d %s/%s %s" % \
                                             (srcDQ2ID,dstDQ2ID,guidStr,lfnsStr,callBackURL,destDir,
                                              destinationDBlock,destinationDBlock)
                                    # set job parameters
                                    ddmjob.jobParameters = argStr
                                    _logger.debug('%s pdq2_cr %s' % (self.pandaID,ddmjob.jobParameters))
                                    ddmJobs.append(ddmjob)
                        # start Activator
                        if re.search('_sub\d+$',dataset.name) == None:
                            if self.job.prodSourceLabel=='panda' and self.job.processingType in ['merge','unmerge']:
                                # don't trigger Activator for merge jobs
                                pass
                            else:
                                if self.job.jobStatus == 'finished':
                                    aThr = Activator(self.taskBuffer,dataset)
                                    aThr.start()
                                    aThr.join()
                    else:
                        # unset flag since another thread already updated 
                        flagComplete = False
                else:
                    # update dataset in DB
                    self.taskBuffer.updateDatasets(dsList,withLock=True,withCriteria="status<>:crStatus AND status<>:lockStatus ",
                                                   criteriaMap={':crStatus':finalStatus,':lockStatus':'locked'})
                    # unset flag
                    flagComplete = False
                # end
                _logger.debug('%s end %s' % (self.pandaID,destinationDBlock))
            # start DDM jobs
            if ddmJobs != []:
                self.taskBuffer.storeJobs(ddmJobs,self.job.prodUserID,joinThr=True)
            # change pending jobs to failed
            if flagComplete and self.job.prodSourceLabel=='user':
                #_logger.debug('%s call RetryMaker for %s %s' % (self.pandaID,self.job.prodUserName,self.job.jobDefinitionID))
                #retryMaker = RetryMaker.RetryMaker(self.taskBuffer,self.job)
                #retryMaker.run()
                _logger.debug('%s finalize %s %s' % (self.pandaID,self.job.prodUserName,self.job.jobDefinitionID))
                self.taskBuffer.finalizePendingJobs(self.job.prodUserName,self.job.jobDefinitionID)
            # update unmerged datasets in JEDI to trigger merging
            if flagComplete and self.job.produceUnMerge():
                self.taskBuffer.updateUnmergedDatasets(self.job)
            # start notifier
            _logger.debug('%s source:%s complete:%s' % (self.pandaID,self.job.prodSourceLabel,flagComplete))
            if (self.job.jobStatus != 'transferring') and ((flagComplete and self.job.prodSourceLabel=='user') or \
               (self.job.jobStatus=='failed' and self.job.prodSourceLabel=='panda')) and \
               self.job.lockedby != 'jedi':
                # don't send email for merge jobs
                if (not disableNotifier) and not self.job.processingType in ['merge','unmerge']:
                    useNotifier = True
                    summaryInfo = {}
                    # check all jobDefIDs in jobsetID
                    if not self.job.jobsetID in [0,None,'NULL']:
                        useNotifier,summaryInfo = self.taskBuffer.checkDatasetStatusForNotifier(self.job.jobsetID,self.job.jobDefinitionID,
                                                                                                self.job.prodUserName)
                        _logger.debug('%s useNotifier:%s' % (self.pandaID,useNotifier))
                    if useNotifier:
                        _logger.debug('%s start Notifier' % self.pandaID)
                        nThr = Notifier.Notifier(self.taskBuffer,self.job,self.destinationDBlocks,summaryInfo)
                        nThr.run()
                        _logger.debug('%s end Notifier' % self.pandaID)                    
            _logger.debug('%s End' % self.pandaID)
        except:
            errType,errValue = sys.exc_info()[:2]
            _logger.error("%s %s" % (errType,errValue))
            


    # check if top dataset
    def isTopLevelDS(self,datasetName):
        topDS = re.sub('_sub\d+$','',datasetName)
        if topDS == datasetName:
            return True
        return False

