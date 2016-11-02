'''
watch job

'''

import re
import sys
import time
import commands
import datetime
import threading
import ErrorCode

import taskbuffer.ErrorCode

from taskbuffer import EventServiceUtils
from taskbuffer import retryModule

from brokerage.PandaSiteIDs import PandaSiteIDs

from dataservice.Closer  import Closer
from pandalogger.PandaLogger import PandaLogger

# logger
_logger = PandaLogger().getLogger('Watcher')


class Watcher (threading.Thread):
    # constructor
    def __init__(self,taskBuffer,pandaID,single=False,sleepTime=360,sitemapper=None):
        threading.Thread.__init__(self)
        self.pandaID    = pandaID
        self.taskBuffer = taskBuffer
        self.sleepTime  = sleepTime
        self.single     = single
        self.siteMapper = sitemapper

    # main
    def run(self):
        try:
            while True:
                _logger.debug('%s start' % self.pandaID)
                # query job
                job = self.taskBuffer.peekJobs([self.pandaID],fromDefined=False,
                                               fromArchived=False,fromWaiting=False)[0]
                # check job status
                if job == None:
                    _logger.debug('%s escape : not found' % self.pandaID)
                    return
                if not job.jobStatus in ['running','sent','starting','holding',
                                         'stagein','stageout']:
                    if job.jobStatus == 'transferring' and job.prodSourceLabel in ['user','panda']:
                        pass
                    else:
                        _logger.debug('%s escape : %s' % (self.pandaID,job.jobStatus))
                        return
                # time limit
                timeLimit = datetime.datetime.utcnow() - datetime.timedelta(minutes=self.sleepTime)
                if job.modificationTime < timeLimit or (job.endTime != 'NULL' and job.endTime < timeLimit):
                    _logger.debug('%s %s lastmod:%s endtime:%s' % (job.PandaID,job.jobStatus,
                                                                   str(job.modificationTime),
                                                                   str(job.endTime)))
                    # retry ES merge jobs
                    if EventServiceUtils.isEventServiceMerge(job):
                        self.taskBuffer.retryJob(job.PandaID,{},getNewPandaID=True,
                                                 attemptNr=job.attemptNr,
                                                 recoverableEsMerge=True)
                        # read back
                        job = self.taskBuffer.peekJobs([self.pandaID],fromDefined=False,
                                                       fromArchived=False,fromWaiting=False)[0]
                    destDBList = []
                    # retry analysis jobs 
                    if (job.prodSourceLabel in ['user','panda']) and (job.attemptNr<2 or job.jobStatus == 'sent') \
                             and job.commandToPilot != 'tobekilled' and (not job.processingType in ['ITB_INTEGRATION']) \
                             and not job.taskBufferErrorCode in [taskbuffer.ErrorCode.EC_Reassigned,
                                                                 taskbuffer.ErrorCode.EC_Retried,
                                                                 taskbuffer.ErrorCode.EC_PilotRetried] \
                             and not job.processingType.startswith('gangarobot') \
                             and not job.processingType.startswith('hammercloud'):
                        # reset
                        _logger.debug(' -> reset %s job with %s : PandaID:%s #%s' % (job.prodSourceLabel,job.jobStatus,job.PandaID,job.attemptNr))
                        job.jobStatus = 'activated'
                        job.startTime = None
                        job.endTime   = None                                                
                        job.attemptNr = job.attemptNr + 1
                        # remove flag regarding to pledge-resource handling
                        if not job.specialHandling in [None,'NULL','']:
                            newSpecialHandling = re.sub(',*localpool','',job.specialHandling)
                            if newSpecialHandling == '':
                                job.specialHandling = None
                            else:
                                job.specialHandling = newSpecialHandling
                        # TEMPORARY : send it to long queue
                        oldComputingSite = job.computingSite
                        if job.jobStatus != 'sent' and job.computingSite.startswith('ANALY') and (not job.computingSite.startswith('ANALY_LONG_')):
                            tmpLongSiteList = []
                            tmpLongSite = re.sub('^ANALY_','ANALY_LONG_',job.computingSite)
                            tmpLongSite = re.sub('_\d+$','',tmpLongSite)
                            tmpLongSiteList.append(tmpLongSite)
                            tmpLongSite = job.computingSite + '_LONG'
                            tmpLongSiteList.append(tmpLongSite)
                            tmpLongSite = re.sub('SHORT','LONG',job.computingSite)
                            if tmpLongSite != job.computingSite:
                                tmpLongSiteList.append(tmpLongSite)
                            for longSite in tmpLongSiteList:
                                if self.siteMapper.checkSite(longSite):
                                    tmpSiteSpec = self.siteMapper.getSite(longSite)
                                    if tmpSiteSpec.status == 'online':
                                        job.computingSite = longSite
                                        _logger.debug(' -> sending PandaID:%s to %s' % (job.PandaID,job.computingSite))
                                        # set destinationSE
                                        if job.destinationSE == oldComputingSite:
                                            job.destinationSE = job.computingSite
                                        break    
                        # modify LFNs and destinationSE
                        for file in job.Files:
                            modTypes = ('output','log')
                            if file.type in modTypes:                            
                                # set destinationSE
                                if file.destinationSE == oldComputingSite:
                                    file.destinationSE = job.computingSite
                            if job.prodSourceLabel == 'panda':
                                # doesn't change output for buildJob
                                modTypes = ('log',)                                
                            if file.type in modTypes:
                                # set new GUID
                                if file.type == 'log':
                                    file.GUID = commands.getoutput('uuidgen')
                                # add attempt nr
                                oldName  = file.lfn
                                file.lfn = re.sub("\.\d+$","",file.lfn)
                                file.lfn = "%s.%d" % (file.lfn,job.attemptNr)
                                newName  = file.lfn
                                # modify jobParameters
                                sepPatt = "(\'|\"|%20|:)" + oldName + "(\'|\"|%20| )"
                                matches = re.findall(sepPatt,job.jobParameters)
                                for match in matches:
                                    oldPatt = match[0]+oldName+match[-1]
                                    newPatt = match[0]+newName+match[-1]
                                    job.jobParameters = re.sub(oldPatt,newPatt,job.jobParameters)
                    else:
                        if job.jobStatus == 'sent':
                            # sent job didn't receive reply from pilot within 30 min
                            job.jobDispatcherErrorCode = ErrorCode.EC_SendError
                            job.jobDispatcherErrorDiag = "Sent job didn't receive reply from pilot within 30 min"
                        elif job.exeErrorDiag == 'NULL' and job.pilotErrorDiag == 'NULL':
                            # lost heartbeat
                            job.jobDispatcherErrorCode = ErrorCode.EC_Watcher
                            if job.jobDispatcherErrorDiag == 'NULL':
                                if job.endTime == 'NULL':
                                    # normal lost heartbeat
                                    job.jobDispatcherErrorDiag = 'lost heartbeat : %s' % str(job.modificationTime)
                                else:
                                    # job recovery failed
                                    job.jobDispatcherErrorDiag = 'lost heartbeat : %s' % str(job.endTime)
                                    if job.jobStatus == 'transferring':
                                        job.jobDispatcherErrorDiag += ' in transferring'
                        else:
                            # job recovery failed
                            job.jobDispatcherErrorCode = ErrorCode.EC_Recovery
                            job.jobDispatcherErrorDiag = 'job recovery failed for %s hours' % (self.sleepTime/60)
                        # set job status
                        job.jobStatus = 'failed'
                        # set endTime for lost heartbeat
                        if job.endTime == 'NULL':
                            # normal lost heartbeat
                            job.endTime = job.modificationTime
                        # set files status
                        for file in job.Files:
                            if file.type == 'output' or file.type == 'log':
                                file.status = 'failed'
                                if not file.destinationDBlock in destDBList:
                                    destDBList.append(file.destinationDBlock)
                        # event service
                        if EventServiceUtils.isEventServiceJob(job) and not EventServiceUtils.isJobCloningJob(job):
                            eventStat = self.taskBuffer.getEventStat(job.jediTaskID, job.PandaID)
                            # set sub status when no sucessful events
                            if EventServiceUtils.ST_finished not in eventStat:
                                job.jobSubStatus = 'es_heartbeat'
                    # update job
                    self.taskBuffer.updateJobs([job],False)
                    # start closer
                    if job.jobStatus == 'failed':

                        source = 'jobDispatcherErrorCode'
                        error_code = job.jobDispatcherErrorCode
                        error_diag = job.jobDispatcherErrorDiag

                        try:
                            _logger.debug("Watcher will call apply_retrial_rules")
                            retryModule.apply_retrial_rules(self.taskBuffer, job.PandaID, source, error_code, error_diag, job.attemptNr)
                            _logger.debug("apply_retrial_rules is back")
                        except Exception as e:
                            _logger.debug("apply_retrial_rules excepted and needs to be investigated (%s)"%(e))

                        cThr = Closer(self.taskBuffer,destDBList,job)
                        cThr.start()
                        cThr.join()
                    _logger.debug('%s end' % job.PandaID)                        
                    return
                # single action
                if self.single:
                    return
                # sleep
                time.sleep(60*self.sleepTime)
        except:
            type, value, traceBack = sys.exc_info()
            _logger.error("run() : %s %s" % (type,value))
            return
