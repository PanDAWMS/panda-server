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
from pandacommon.pandautils.thread_utils import GenericThread
from pandaserver.config import panda_config
from pandaserver.taskbuffer import EventServiceUtils
from pandaserver.brokerage.SiteMapper import SiteMapper
from pandaserver.srvcore.CoreUtils import commands_get_status_output
from pandaserver.taskbuffer.TaskBuffer import TaskBuffer
# from pandaserver.taskbuffer.TaskBufferInterface import TaskBufferInterface
from pandaserver.dataservice.AdderGen import AdderGen


# logger
_logger = PandaLogger().getLogger('add')


# main
def main(argv=tuple(), tbuf=None, **kwargs):

    try:
        long
    except NameError:
        long = int



    tmpLog = LogWrapper(_logger,None)

    tmpLog.debug("===================== start =====================")

    # overall timeout value
    overallTimeout = 20

    # grace period
    try:
        gracePeriod = int(argv[1])
    except Exception:
        gracePeriod = 3

    # current minute
    currentMinute = datetime.datetime.utcnow().minute

    # # kill old process
    # try:
    #     # time limit
    #     timeLimit = datetime.datetime.utcnow() - datetime.timedelta(minutes=overallTimeout)
    #     # get process list
    #     scriptName = sys.argv[0]
    #     out = commands_get_status_output('env TZ=UTC ps axo user,pid,lstart,args | grep %s' % scriptName)[-1]
    #     for line in out.split('\n'):
    #         items = line.split()
    #         # owned process
    #         if items[0] not in ['sm','atlpan','pansrv','root']: # ['os.getlogin()']: doesn't work in cron
    #             continue
    #         # look for python
    #         if re.search('python',line) is None:
    #             continue
    #         # PID
    #         pid = items[1]
    #         # start time
    #         timeM = re.search('(\S+\s+\d+ \d+:\d+:\d+ \d+)',line)
    #         startTime = datetime.datetime(*time.strptime(timeM.group(1),'%b %d %H:%M:%S %Y')[:6])
    #         # kill old process
    #         if startTime < timeLimit:
    #             tmpLog.debug("old process : %s %s" % (pid,startTime))
    #             tmpLog.debug(line)
    #             commands_get_status_output('kill -9 %s' % pid)
    # except Exception:
    #     type, value, traceBack = sys.exc_info()
    #     tmpLog.error("kill process : %s %s" % (type,value))


    # instantiate TB
    if tbuf is None:
        from pandaserver.taskbuffer.TaskBuffer import taskBuffer
        taskBuffer.init(panda_config.dbhost,panda_config.dbpasswd,nDBConnection=1)
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
    try:
        tmpLog.debug("nRunning session")
        if (currentMinute / panda_config.nrun_interval) % panda_config.nrun_hosts == panda_config.nrun_snum:
            retNR = taskBuffer.insertnRunningInSiteData()
            tmpLog.debug(retNR)
    except Exception:
        errType,errValue = sys.exc_info()[:2]
        tmpLog.error("nRunning : %s %s" % (errType,errValue))


    # # mail sender
    # class MailSender (threading.Thread):
    #     def __init__(self):
    #         threading.Thread.__init__(self)
    #
    #     def run(self):
    #         tmpLog.debug("mail : start")
    #         tmpFileList = glob.glob('%s/mail_*' % panda_config.logdir)
    #         for tmpFile in tmpFileList:
    #             # check timestamp to avoid too new files
    #             timeStamp = os.path.getmtime(tmpFile)
    #             if datetime.datetime.utcnow() - datetime.datetime.fromtimestamp(timeStamp) < datetime.timedelta(minutes=1):
    #                 continue
    #             # lock
    #             mailFile = open(tmpFile)
    #             try:
    #                 fcntl.flock(mailFile.fileno(), fcntl.LOCK_EX|fcntl.LOCK_NB)
    #             except Exception:
    #                 tmpLog.debug("mail : failed to lock %s" % tmpFile.split('/')[-1])
    #                 mailFile.close()
    #                 continue
    #             # start notifier
    #             from pandaserver.dataservice.Notifier import Notifier
    #             nThr = Notifier(None,None,None,None,mailFile,tmpFile)
    #             nThr.run()
    #             # remove
    #             try:
    #                 os.remove(tmpFile)
    #             except Exception:
    #                 pass
    #             # unlock
    #             try:
    #                 fcntl.flock(self.lockXML.fileno(), fcntl.LOCK_UN)
    #                 mailFile.close()
    #             except Exception:
    #                 pass
    #
    # # start sender
    # mailSender =  MailSender()
    # mailSender.start()


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


    # thread pool
    # class ThreadPool:
    #     def __init__(self):
    #         self.lock = threading.Lock()
    #         self.list = []
    #
    #     def add(self,obj):
    #         self.lock.acquire()
    #         self.list.append(obj)
    #         self.lock.release()
    #
    #     def remove(self,obj):
    #         self.lock.acquire()
    #         self.list.remove(obj)
    #         self.lock.release()
    #
    #     def join(self):
    #         self.lock.acquire()
    #         thrlist = tuple(self.list)
    #         self.lock.release()
    #         for thr in thrlist:
    #             thr.join()

    # # process for adder
    # class AdderProcess:
    # thread for adder
    class AdderThread(GenericThread):

        def __init__(self, taskBuffer, aSiteMapper, holdingAna, job_output_reports, report_index_list):
            GenericThread.__init__(self)
            self.taskBuffer = taskBuffer
            self.aSiteMapper = aSiteMapper
            self.holdingAna = holdingAna
            self.job_output_reports = job_output_reports
            self.report_index_list = report_index_list

        # main loop
        def run(self):
            # get logger
            # _logger = PandaLogger().getLogger('add_process')
            # initialize
            taskBuffer = self.taskBuffer
            aSiteMapper = self.aSiteMapper
            holdingAna = self.holdingAna
            # get file list
            timeNow = datetime.datetime.utcnow()
            timeInt = datetime.datetime.utcnow()
            dirName = panda_config.logdir
            # fileList = os.listdir(dirName)
            # fileList.sort()
            # job_output_report_list = taskBuffer.listJobOutputReport(only_unlocked=True, time_limit=10, limit=1000)
            # try to pre-lock records for a short period of time, so that multiple nodes can get different records
            prelock_pid = self.get_pid()
            prelocked_JOR_list = []
            job_output_report_list = []
            nFixed = 1000
            while True:
                try:
                    report_index = self.report_index_list.get(timeout=10)
                except queue.Empty:
                    break
                one_JOR = self.job_output_reports[report_index]
                panda_id, job_status, attempt_nr, time_stamp = one_JOR
                if len(job_output_report_list) < nFixed:
                    # only pre-locked first nFixed records
                    got_lock = taskBuffer.lockJobOutputReport(
                                    panda_id=panda_id, attempt_nr=attempt_nr,
                                    pid=prelock_pid, time_limit=10)
                    if got_lock:
                        # continue with the records this process pre-locked
                        job_output_report_list.append(one_JOR)
                        prelocked_JOR_list.append(one_JOR)
                else:
                    job_output_report_list.append(one_JOR)
                if len(job_output_report_list) >= 1500:
                    # at most 1500 records in this thread in this cycle
                    break
            # remove duplicated files
            tmp_list = []
            uMap = {}
            # for file in fileList:
            # if job_output_report_list is not None:
            for panda_id, job_status, attempt_nr, time_stamp in job_output_report_list:
                # match = re.search('^(\d+)_([^_]+)_.{36}(_\d+)*$',file)
                # if match is not None:
                # fileName = '%s/%s' % (dirName,file)
                # panda_id = match.group(1)
                # job_status = match.group(2)
                if panda_id in uMap:
                    # try:
                    #     os.remove(fileName)
                    # except Exception:
                    #     pass
                    taskBuffer.deleteJobOutputReport(panda_id=panda_id, attempt_nr=attempt_nr)
                else:
                    if job_status != EventServiceUtils.esRegStatus:
                        # uMap[panda_id] = fileName
                        record = (panda_id, job_status, attempt_nr, time_stamp)
                        uMap[panda_id] = record
                    if long(panda_id) in holdingAna:
                        # give a priority to buildJobs
                        tmp_list.insert(0, record)
                    else:
                        tmp_list.append(record)
            randTmp = tmp_list[nFixed:]
            random.shuffle(randTmp)
            job_output_report_list = tmp_list[:nFixed] + randTmp
            tmpLog.debug("Add pid={0} got {1} job records to process".format(prelock_pid, len(job_output_report_list)))
            # add
            while len(job_output_report_list) != 0:
                # time limit to avoid too many copyArchive running at the same time
                if (datetime.datetime.utcnow() - timeNow) > datetime.timedelta(minutes=overallTimeout):
                    tmpLog.debug("time over in Adder session")
                    break
                # get fileList
                if (datetime.datetime.utcnow() - timeInt) > datetime.timedelta(minutes=15):
                    timeInt = datetime.datetime.utcnow()
                    # get file
                    # fileList = os.listdir(dirName)
                    # fileList.sort()
                    # remove duplicated files
                    tmp_list = []
                    uMap = {}
                    # for file in fileList:
                    if job_output_report_list is not None:
                        for panda_id, job_status, attempt_nr, time_stamp in job_output_report_list:
                            # match = re.search('^(\d+)_([^_]+)_.{36}(_\d+)*$',file)
                            # if match is not None:
                            # fileName = '%s/%s' % (dirName,file)
                            # panda_id = match.group(1)
                            # job_status = match.group(2)
                            if panda_id in uMap:
                                # try:
                                #     os.remove(fileName)
                                # except Exception:
                                #     pass
                                taskBuffer.deleteJobOutputReport(panda_id=panda_id, attempt_nr=attempt_nr)
                            else:
                                if job_status != EventServiceUtils.esRegStatus:
                                    # uMap[panda_id] = fileName
                                    record = (panda_id, job_status, attempt_nr, time_stamp)
                                    uMap[panda_id] = record
                                if long(panda_id) in holdingAna:
                                    # give a priority to buildJobs
                                    tmp_list.insert(0, record)
                                else:
                                    tmp_list.append(record)
                    # fileList = tmp_list
                    job_output_report_list = tmp_list
                # check if
                if PandaUtils.isLogRotating(5,5):
                    tmpLog.debug("terminate since close to log-rotate time")
                    break
                # choose a file
                # file = fileList.pop(0)
                # choose a report record
                record = job_output_report_list.pop(0)
                panda_id, job_status, attempt_nr, time_stamp = record
                # check format
                # match = re.search('^(\d+)_([^_]+)_.{36}(_\d+)*$',file)
                # if match is not None:
                #     fileName = '%s/%s' % (dirName,file)
                #     if not os.path.exists(fileName):
                #         continue
                # unique pid
                GenericThread.__init__(self)
                uniq_pid = self.get_pid()
                try:
                    # modTime = datetime.datetime(*(time.gmtime(os.path.getmtime(fileName))[:7]))
                    modTime = time_stamp
                    adder_gen = None
                    if (timeNow - modTime) > datetime.timedelta(hours=24):
                        # last chance
                        # tmpLog.debug("Last Add File {0} : {1}".format(os.getpid(),fileName))
                        # thr = AdderGen(taskBuffer,match.group(1),match.group(2),fileName,
                        #                ignoreTmpError=False,siteMapper=aSiteMapper)
                        tmpLog.debug("Last Add pid={0} job={1}.{2} st={3}".format(uniq_pid, panda_id, attempt_nr, job_status))
                        adder_gen = AdderGen(taskBuffer, panda_id, job_status, attempt_nr,
                                       ignoreTmpError=False, siteMapper=aSiteMapper, pid=uniq_pid, prelock_pid=prelock_pid)
                    elif (timeInt - modTime) > datetime.timedelta(minutes=gracePeriod):
                        # add
                        # tmpLog.debug("Add File {0} : {1}".format(os.getpid(),fileName))
                        # thr = AdderGen(taskBuffer,match.group(1),match.group(2),fileName,
                        #                ignoreTmpError=True,siteMapper=aSiteMapper)
                        tmpLog.debug("Add pid={0} job={1}.{2} st={3}".format(uniq_pid, panda_id, attempt_nr, job_status))
                        adder_gen = AdderGen(taskBuffer, panda_id, job_status, attempt_nr,
                                       ignoreTmpError=True, siteMapper=aSiteMapper, pid=uniq_pid, prelock_pid=prelock_pid)
                    if adder_gen is not None:
                        adder_gen.run()
                        del adder_gen
                except Exception:
                    type, value, traceBack = sys.exc_info()
                    tmpLog.error("%s %s" % (type,value))
            # unlock prelocked reports if possible
            for panda_id, job_status, attempt_nr, time_stamp in prelocked_JOR_list:
                taskBuffer.unlockJobOutputReport(
                            panda_id=panda_id, attempt_nr=attempt_nr, pid=prelock_pid)
            # close taskBuffer connection
            while True:
                try:
                    proxy = taskBuffer.proxyPool.proxyList.get(block=False)
                except Exception:
                    break
                else:
                    proxy.conn.close()

        # launcher, run with multiprocessing
        # def launch(self,taskBuffer,aSiteMapper,holdingAna):
        def proc_launch(self):
            # run
            self.process = multiprocessing.Process(target=self.run)
            self.process.start()

        # join of multiprocessing
        def proc_join(self):
            self.process.join()


    # get buildJobs in the holding state
    holdingAna = []
    varMap = {}
    varMap[':prodSourceLabel'] = 'panda'
    varMap[':jobStatus'] = 'holding'
    status,res = taskBuffer.querySQLS("SELECT PandaID from ATLAS_PANDA.jobsActive4 WHERE prodSourceLabel=:prodSourceLabel AND jobStatus=:jobStatus",varMap)
    if res is not None:
        for id, in res:
            holdingAna.append(id)
    tmpLog.debug("number of holding Ana %s " % len(holdingAna))

    # add files
    tmpLog.debug("Adder session")

    # make TaskBuffer IF
    # taskBufferIF = TaskBufferInterface()
    # taskBufferIF.launch(taskBuffer)

    # p = AdderProcess()
    # p.run(taskBuffer, aSiteMapper, holdingAna)

    adderThrList = []
    nThr = 10
    # get some job output reports
    jor_list = taskBuffer.listJobOutputReport(only_unlocked=True, time_limit=10, limit=3000*nThr,
                                              grace_period=gracePeriod)
    job_output_reports = dict()
    report_index_list = multiprocessing.Queue()
    for one_jor in jor_list:
        panda_id, job_status, attempt_nr, time_stamp = one_jor
        report_index = (panda_id, attempt_nr)
        job_output_reports[report_index] = one_jor
        report_index_list.put(report_index)
    for i in range(nThr):
        # p = AdderProcess()
        # p.launch(taskBufferIF.getInterface(),aSiteMapper,holdingAna)
        tbuf = TaskBuffer()
        tbuf.init(panda_config.dbhost, panda_config.dbpasswd, nDBConnection=1)
        thr = AdderThread(tbuf, aSiteMapper, holdingAna, job_output_reports, report_index_list)
        adderThrList.append(thr)
    # start all threads
    for thr in adderThrList:
        # thr.start()
        thr.proc_launch()
        time.sleep(0.25)

    # join all threads
    for thr in adderThrList:
        # thr.join()
        thr.proc_join()

    # join sender
    # mailSender.join()

    # join fork threads
    for thr in forkThrList:
        thr.join()

    # terminate TaskBuffer IF
    # taskBufferIF.terminate()

    tmpLog.debug("===================== end =====================")


# run
if __name__ == '__main__':
    main(argv=sys.argv)
