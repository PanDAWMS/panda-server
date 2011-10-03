'''
notifier

'''

import re
import sys
import commands
import urllib
import datetime
import time

from config import panda_config
from userinterface import ReBroker
from pandalogger.PandaLogger import PandaLogger

# logger
_logger = PandaLogger().getLogger('RetryMaker')


def initLogger(pLogger):
    # redirect logging to parent as it doesn't work in nested threads
    global _logger
    _logger = pLogger
    ReBroker.initLogger(_logger)


class RetryMaker:
    # constructor
    def __init__(self,taskBuffer,job):
        self.job = job
        self.taskBuffer = taskBuffer

    # main
    def run(self):
        _logger.debug("%s start" % self.job.PandaID)
        try:
            # check the number of server retry
            nRetry = self.job.specialHandling.split(',').count('sretry')
            _logger.debug("%s nRetry=%s" % (self.job.PandaID,nRetry))
            # too many reattempts
            maxRetry = 2
            if nRetry >= maxRetry:
                _logger.debug("%s end : too many reattempts %s>=%s" % (self.job.PandaID,nRetry,maxRetry))
                return True
            # get all job status in Active
            idStatus,buildID = self.taskBuffer.getPandIDsWithJobID(self.job.prodUserName,
                                                                   self.job.jobDefinitionID,
                                                                   {},0)
            # count # of failed in active
            nFailed = 0
            for tmpID,tmpVar in idStatus.iteritems():
                # ignore buildJob
                if tmpID == buildID:
                    continue
                # count
                tmpStatus,tmpCommand = tmpVar
                if tmpStatus == 'failed':
                    nFailed += 1
                elif tmpStatus == 'cancelled' or tmpCommand == 'tobekilled':
                    # killed
                    _logger.debug("%s end : cancelled" % self.job.PandaID)
                    return True
            _logger.debug("%s : nFailed=%s in Active" % (self.job.PandaID,nFailed))
            # no failed
            if nFailed == 0:
                _logger.debug("%s end : no failed jobs" % self.job.PandaID)
                return True
            # get all job status including Archived
            idStatus,buildID = self.taskBuffer.getPandIDsWithJobIDLog(self.job.prodUserName,
                                                                      self.job.jobDefinitionID,
                                                                      idStatus,0,buildID)
            # count # of failed and others in archived
            nFailed = 0
            nOthers = 0
            for tmpID,tmpVar in idStatus.iteritems():
                # ignore buildJob
                if tmpID == buildID:
                    continue
                # count
                tmpStatus,tmpCommand = tmpVar
                if tmpStatus == 'failed':
                    nFailed += 1
                elif tmpStatus == 'cancelled' or tmpCommand == 'tobekilled':
                    # killed
                    _logger.debug("%s end : cancelled" % self.job.PandaID)
                    return True
                else:
                    nOthers += 1
            _logger.debug("%s : nFailed=%s nOthers=%s in Active+Archived" % (self.job.PandaID,nFailed,nOthers))
            # no successful jobs
            if nOthers == 0:
                _logger.debug("%s end : no successful jobs" % self.job.PandaID)
                return True
            # no failed jobs just in case
            if nFailed == 0:
                _logger.debug("%s end : no failed jobs" % self.job.PandaID)
                return True
            # check ratio
            maxFailedRatio = 0.8
            failedRatio = float(nFailed) / float(nOthers+nFailed)
            if failedRatio > maxFailedRatio:
                _logger.debug("%s end : too many failed jobs %s/%s>%s" % (self.job.PandaID,
                                                                          nFailed,
                                                                          nOthers+nFailed,
                                                                          maxFailedRatio))
                return True
            # instantiate rebrokerage since server-side retry relies on that
            rebro = ReBroker.ReBroker(self.taskBuffer,forFailed=True,avoidSameSite=True)
            # lock job for retry                
            reSt,reVal = rebro.lockJob(self.job.prodUserID,self.job.jobDefinitionID)
            if not reSt:
                _logger.debug("%s end : failed to lock jobs with %s" % (self.job.PandaID,eVal))
                return False
            # execute
            _logger.debug("%s : execute ReBroker" % self.job.PandaID)
            rebro.start()
            rebro.join()
            _logger.debug("%s end : successfully" % self.job.PandaID)
            return True
        except:
            errType,errValue = sys.exc_info()[:2]            
            _logger.error("%s %s %s" % (self.job.PandaID,errType,errValue))
            _logger.debug("%s end : failed" % self.job.PandaID)
            return False
