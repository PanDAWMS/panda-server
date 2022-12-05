import os
import re
import sys
import time
import glob
import queue
import random
import datetime
import traceback
import threading
import multiprocessing

import pandaserver.userinterface.Client as Client
import pandaserver.taskbuffer.ErrorCode

from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandautils import PandaUtils
from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandaserver.config import panda_config
from pandaserver.brokerage.SiteMapper import SiteMapper
from pandaserver.srvcore.CoreUtils import commands_get_status_output
from pandaserver.taskbuffer.TaskBuffer import TaskBuffer


# logger
_logger = PandaLogger().getLogger('add_sub')


# main
def main(argv=tuple(), tbuf=None, **kwargs):

    try:
        long
    except NameError:
        long = int

    tmpLog = LogWrapper(_logger,None)

    tmpLog.debug("===================== start =====================")

    # current minute
    currentMinute = datetime.datetime.utcnow().minute

    # instantiate TB
    if tbuf is None:
        from pandaserver.taskbuffer.TaskBuffer import taskBuffer
        taskBuffer.init(panda_config.dbhost,panda_config.dbpasswd,nDBConnection=1, useTimeout=True)
    else:
        taskBuffer = tbuf

    # instantiate sitemapper
    aSiteMapper = SiteMapper(taskBuffer)

    # delete
    tmpLog.debug("Del session")
    status,retSel = taskBuffer.querySQLS("SELECT MAX(PandaID) FROM ATLAS_PANDA.jobsDefined4",{})
    if retSel is not None:
        try:
            maxID = retSel[0][0]
            tmpLog.debug("maxID : %s" % maxID)
            if maxID is not None:
                varMap = {}
                varMap[':maxID'] = maxID
                varMap[':jobStatus1'] = 'activated'
                varMap[':jobStatus2'] = 'waiting'
                varMap[':jobStatus3'] = 'failed'
                varMap[':jobStatus4'] = 'cancelled'
                status,retDel = taskBuffer.querySQLS("DELETE FROM ATLAS_PANDA.jobsDefined4 WHERE PandaID<:maxID AND jobStatus IN (:jobStatus1,:jobStatus2,:jobStatus3,:jobStatus4)",varMap)
        except Exception:
            pass

    # count # of getJob/updateJob in dispatcher's log
    try:
        # don't update when logrotate is running
        timeNow = datetime.datetime.utcnow()
        logRotateTime = timeNow.replace(hour=3,minute=2,second=0,microsecond=0)
        if (timeNow > logRotateTime and (timeNow-logRotateTime) < datetime.timedelta(minutes=5)) or \
               (logRotateTime > timeNow and (logRotateTime-timeNow) < datetime.timedelta(minutes=5)):
            tmpLog.debug("skip pilotCounts session for logrotate")
        else:
            # log filename
            dispLogName = '%s/panda-PilotRequests.log' % panda_config.logdir
            # time limit
            timeLimit  = datetime.datetime.utcnow() - datetime.timedelta(hours=3)
            timeLimitS  = datetime.datetime.utcnow() - datetime.timedelta(hours=1)
            # check if tgz is required
            com = 'head -1 %s' % dispLogName
            lostat,loout = commands_get_status_output(com)
            useLogTgz = True
            if lostat == 0:
                match = re.search('^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}',loout)
                if match is not None:
                    startTime = datetime.datetime(*time.strptime(match.group(0),'%Y-%m-%d %H:%M:%S')[:6])
                    # current log contains all info
                    if startTime<timeLimit:
                        useLogTgz = False
            # log files
            dispLogNameList = [dispLogName]
            if useLogTgz:
                today = datetime.date.today()
                dispLogNameList.append('{0}-{1}.gz'.format(dispLogName, today.strftime('%Y%m%d')))
            # delete tmp
            commands_get_status_output('rm -f %s.tmp-*' % dispLogName)
            # tmp name
            tmpLogName = '%s.tmp-%s' % (dispLogName,datetime.datetime.utcnow().strftime('%Y-%m-%d-%H-%M-%S'))
            # loop over all files
            pilotCounts = {}
            pilotCountsS = {}
            for tmpDispLogName in dispLogNameList:
                # expand or copy
                if tmpDispLogName.endswith('.gz'):
                    com = 'gunzip -c %s > %s' % (tmpDispLogName,tmpLogName)
                else:
                    com = 'cp %s %s' % (tmpDispLogName,tmpLogName)
                lostat,loout = commands_get_status_output(com)
                if lostat != 0:
                    errMsg = 'failed to expand/copy %s with : %s' % (tmpDispLogName,loout)
                    raise RuntimeError(errMsg)
                # search string
                sStr  = '^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}).*'
                sStr += 'method=(.+),site=(.+),node=(.+),type=(.+)'
                # read
                logFH = open(tmpLogName)
                for line in logFH:
                    # check format
                    match = re.search(sStr,line)
                    if match is not None:
                        # check timerange
                        timeStamp = datetime.datetime(*time.strptime(match.group(1),'%Y-%m-%d %H:%M:%S')[:6])
                        if timeStamp < timeLimit:
                            continue
                        tmpMethod = match.group(2)
                        tmpSite   = match.group(3)
                        tmpNode   = match.group(4)
                        tmpType   = match.group(5)

                        # protection against corrupted entries from pilot,
                        # e.g. pilot reading site json from cvmfs while it was being updated
                        if tmpSite not in aSiteMapper.siteSpecList:
                            continue
                        # sum
                        pilotCounts.setdefault(tmpSite, {})
                        pilotCounts[tmpSite].setdefault(tmpMethod, {})
                        pilotCounts[tmpSite][tmpMethod].setdefault(tmpNode, 0)
                        pilotCounts[tmpSite][tmpMethod][tmpNode] += 1
                        # short
                        if timeStamp > timeLimitS:
                            if tmpSite not in pilotCountsS:
                                pilotCountsS[tmpSite] = dict()
                            if tmpMethod not in pilotCountsS[tmpSite]:
                                pilotCountsS[tmpSite][tmpMethod] = dict()
                            if tmpNode not in pilotCountsS[tmpSite][tmpMethod]:
                                pilotCountsS[tmpSite][tmpMethod][tmpNode] = 0
                            pilotCountsS[tmpSite][tmpMethod][tmpNode] += 1
                # close
                logFH.close()
            # delete tmp
            commands_get_status_output('rm %s' % tmpLogName)
            # update
            hostID = panda_config.pserverhost.split('.')[0]
            tmpLog.debug("pilotCounts session")
            retPC = taskBuffer.updateSiteData(hostID,pilotCounts,interval=3)
            tmpLog.debug(retPC)
            retPC = taskBuffer.updateSiteData(hostID,pilotCountsS,interval=1)
            tmpLog.debug(retPC)
    except Exception:
        errType,errValue = sys.exc_info()[:2]
        tmpLog.error("updateJob/getJob : %s %s" % (errType,errValue))


    # nRunning
    tmpLog.debug("nRunning session")
    try:
        if (currentMinute / panda_config.nrun_interval) % panda_config.nrun_hosts == panda_config.nrun_snum:
            retNR = taskBuffer.insertnRunningInSiteData()
            tmpLog.debug(retNR)
    except Exception:
        errType,errValue = sys.exc_info()[:2]
        tmpLog.error("nRunning : %s %s" % (errType,errValue))


    # session for co-jumbo jobs
    tmpLog.debug("co-jumbo session")
    try:
        ret = taskBuffer.getCoJumboJobsToBeFinished(30,0,1000)
        if ret is None:
            tmpLog.debug("failed to get co-jumbo jobs to finish")
        else:
            coJumboA,coJumboD,coJumboW,coJumboTokill = ret
            tmpLog.debug("finish {0} co-jumbo jobs in Active".format(len(coJumboA)))
            if len(coJumboA) > 0:
                jobSpecs = taskBuffer.peekJobs(coJumboA,fromDefined=False,fromActive=True,fromArchived=False,fromWaiting=False)
                for jobSpec in jobSpecs:
                    fileCheckInJEDI = taskBuffer.checkInputFileStatusInJEDI(jobSpec)
                    if not fileCheckInJEDI:
                        jobSpec.jobStatus = 'closed'
                        jobSpec.jobSubStatus = 'cojumbo_wrong'
                        jobSpec.taskBufferErrorCode = pandaserver.taskbuffer.ErrorCode.EC_EventServiceInconsistentIn
                    taskBuffer.archiveJobs([jobSpec],False)
            tmpLog.debug("finish {0} co-jumbo jobs in Defined".format(len(coJumboD)))
            if len(coJumboD) > 0:
                jobSpecs = taskBuffer.peekJobs(coJumboD,fromDefined=True,fromActive=False,fromArchived=False,fromWaiting=False)
                for jobSpec in jobSpecs:
                    fileCheckInJEDI = taskBuffer.checkInputFileStatusInJEDI(jobSpec)
                    if not fileCheckInJEDI:
                        jobSpec.jobStatus = 'closed'
                        jobSpec.jobSubStatus = 'cojumbo_wrong'
                        jobSpec.taskBufferErrorCode = pandaserver.taskbuffer.ErrorCode.EC_EventServiceInconsistentIn
                    taskBuffer.archiveJobs([jobSpec],True)
            tmpLog.debug("finish {0} co-jumbo jobs in Waiting".format(len(coJumboW)))
            if len(coJumboW) > 0:
                jobSpecs = taskBuffer.peekJobs(coJumboW,fromDefined=False,fromActive=False,fromArchived=False,fromWaiting=True)
                for jobSpec in jobSpecs:
                    fileCheckInJEDI = taskBuffer.checkInputFileStatusInJEDI(jobSpec)
                    if not fileCheckInJEDI:
                        jobSpec.jobStatus = 'closed'
                        jobSpec.jobSubStatus = 'cojumbo_wrong'
                        jobSpec.taskBufferErrorCode = pandaserver.taskbuffer.ErrorCode.EC_EventServiceInconsistentIn
                    taskBuffer.archiveJobs([jobSpec],False,True)
            tmpLog.debug("kill {0} co-jumbo jobs in Waiting".format(len(coJumboTokill)))
            if len(coJumboTokill) > 0:
                jediJobs = list(coJumboTokill)
                nJob = 100
                iJob = 0
                while iJob < len(jediJobs):
                    tmpLog.debug(' killing %s' % str(jediJobs[iJob:iJob+nJob]))
                    Client.killJobs(jediJobs[iJob:iJob+nJob],51,keepUnmerged=True)
                    iJob += nJob
    except Exception:
        errStr = traceback.format_exc()
        tmpLog.error(errStr)


    tmpLog.debug("Fork session")
    # thread for fork
    class ForkThr (threading.Thread):
        def __init__(self,fileName):
            threading.Thread.__init__(self)
            self.fileName = fileName

        def run(self):
            if 'VIRTUAL_ENV' in os.environ:
                prefix = os.environ['VIRTUAL_ENV']
            else:
                prefix = ''
            setupStr = 'source {0}/etc/sysconfig/panda_server; '.format(prefix)
            runStr  = '%s/python -Wignore ' % panda_config.native_python
            runStr += panda_config.pandaPython_dir + '/dataservice/forkSetupper.py -i '
            runStr += self.fileName
            if self.fileName.split('/')[-1].startswith('set.NULL.'):
                runStr += ' -t'
            comStr = setupStr + runStr
            tmpLog.debug(comStr)
            commands_get_status_output(comStr)

    # get set.* files
    filePatt = panda_config.logdir + '/' + 'set.*'
    fileList = glob.glob(filePatt)

    # the max number of threads
    maxThr = 10
    nThr = 0

    # loop over all files
    forkThrList = []
    timeNow = datetime.datetime.utcnow()
    for tmpName in fileList:
        if not os.path.exists(tmpName):
            continue
        try:
            # takes care of only recent files
            modTime = datetime.datetime(*(time.gmtime(os.path.getmtime(tmpName))[:7]))
            if (timeNow - modTime) > datetime.timedelta(minutes=1) and \
                    (timeNow - modTime) < datetime.timedelta(hours=1):
                cSt,cOut = commands_get_status_output('ps aux | grep fork | grep -v PYTH')
                # if no process is running for the file
                if cSt == 0 and tmpName not in cOut:
                    nThr += 1
                    thr = ForkThr(tmpName)
                    thr.start()
                    forkThrList.append(thr)
                    if nThr > maxThr:
                        break
        except Exception:
            errType,errValue = sys.exc_info()[:2]
            tmpLog.error("%s %s" % (errType,errValue))

    # join fork threads
    for thr in forkThrList:
        thr.join()

    # stop taskBuffer if created inside this script
    if tbuf is None:
        taskBuffer.cleanup()

    tmpLog.debug("===================== end =====================")


# run
if __name__ == '__main__':
    main(argv=sys.argv)
