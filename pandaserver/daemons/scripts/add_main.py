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

import pandaserver.taskbuffer.ErrorCode

from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandautils import PandaUtils
from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandautils.thread_utils import GenericThread
from pandaserver.config import panda_config
from pandaserver.taskbuffer import EventServiceUtils
from pandaserver.brokerage.SiteMapper import SiteMapper
from pandaserver.taskbuffer.TaskBuffer import TaskBuffer
from pandaserver.dataservice.AdderGen import AdderGen


# logger
_logger = PandaLogger().getLogger('add_main')


# main
def main(argv=tuple(), tbuf=None, **kwargs):

    try:
        long
    except NameError:
        long = int

    tmpLog = LogWrapper(_logger,None)

    tmpLog.debug("===================== start =====================")

    # return value, true to run main again in next daemon loop
    ret_val = True

    # overall timeout value
    overallTimeout = 20

    # grace period
    try:
        gracePeriod = int(argv[1])
    except Exception:
        gracePeriod = 3

    # current minute
    currentMinute = datetime.datetime.utcnow().minute


    # instantiate TB
    if tbuf is None:
        from pandaserver.taskbuffer.TaskBuffer import taskBuffer
        taskBuffer.init(panda_config.dbhost,panda_config.dbpasswd,nDBConnection=1)
    else:
        taskBuffer = tbuf

    # instantiate sitemapper
    aSiteMapper = SiteMapper(taskBuffer)


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
    tmpLog.debug("get holding build jobs")
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
    tmpLog.debug("run Adder processes")

    # make TaskBuffer IF
    # taskBufferIF = TaskBufferInterface()
    # taskBufferIF.launch(taskBuffer)

    # p = AdderProcess()
    # p.run(taskBuffer, aSiteMapper, holdingAna)

    adderThrList = []
    nThr = 6

    n_jors_per_batch = 2000

    # get some job output reports
    jor_list = taskBuffer.listJobOutputReport(only_unlocked=True, time_limit=10, limit=n_jors_per_batch*nThr,
                                              grace_period=gracePeriod)
    if len(jor_list) < n_jors_per_batch*nThr*0.875:
        # too few job output reports, can stop the daemon loop
        ret_val = False

    # fill in queue
    job_output_reports = dict()
    report_index_list = multiprocessing.Queue()
    for one_jor in jor_list:
        panda_id, job_status, attempt_nr, time_stamp = one_jor
        report_index = (panda_id, attempt_nr)
        job_output_reports[report_index] = one_jor
        report_index_list.put(report_index)

    # adder consumer processes
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

    # terminate TaskBuffer IF
    # taskBufferIF.terminate()

    tmpLog.debug("===================== end =====================")

    # return
    return ret_val


# run
if __name__ == '__main__':
    main(argv=sys.argv)
