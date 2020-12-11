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

from concurrent.futures import ThreadPoolExecutor

import pandaserver.taskbuffer.ErrorCode

from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandautils import PandaUtils
from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandautils.thread_utils import GenericThread
from pandaserver.config import panda_config
from pandaserver.taskbuffer import EventServiceUtils
from pandaserver.brokerage.SiteMapper import SiteMapper
from pandaserver.taskbuffer.TaskBuffer import TaskBuffer
from pandaserver.taskbuffer.TaskBufferInterface import TaskBufferInterface
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

        def __init__(self, taskBuffer, aSiteMapper, holdingAna,
                        job_output_reports, report_index_list, prelock_pid):
            GenericThread.__init__(self)
            self.taskBuffer = taskBuffer
            self.aSiteMapper = aSiteMapper
            self.holdingAna = holdingAna
            self.job_output_reports = job_output_reports
            self.report_index_list = report_index_list
            self.prelock_pid = prelock_pid

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
            # unique pid
            GenericThread.__init__(self)
            uniq_pid = self.get_pid()
            # log pid
            tmpLog.debug("pid={0} run".format(uniq_pid))
            # stats
            n_processed = 0
            n_skipped = 0
            # loop
            while True:
                # time limit to avoid too many copyArchive running at the same time
                if (datetime.datetime.utcnow() - timeNow) > datetime.timedelta(minutes=overallTimeout):
                    tmpLog.debug("time over in Adder session")
                    break
                # check if near to logrotate
                if PandaUtils.isLogRotating(5, 5):
                    tmpLog.debug("terminate since close to log-rotate time")
                    break

                # get report index from queue
                try:
                    report_index = self.report_index_list.get(timeout=1)
                except queue.Empty:
                    break

                # got a job report
                one_jor = self.job_output_reports[report_index]
                panda_id, job_status, attempt_nr, time_stamp = one_jor

                # add
                try:
                    # modTime = datetime.datetime(*(time.gmtime(os.path.getmtime(fileName))[:7]))
                    modTime = time_stamp
                    adder_gen = None
                    if (timeNow - modTime) > datetime.timedelta(hours=24):
                        # last chance
                        tmpLog.debug("Last Add pid={0} job={1}.{2} st={3}".format(uniq_pid, panda_id, attempt_nr, job_status))
                        adder_gen = AdderGen(taskBuffer, panda_id, job_status, attempt_nr,
                                       ignoreTmpError=False, siteMapper=aSiteMapper, pid=uniq_pid, prelock_pid=self.prelock_pid)
                        n_processed += 1
                    elif (timeInt - modTime) > datetime.timedelta(minutes=gracePeriod):
                        # add
                        tmpLog.debug("Add pid={0} job={1}.{2} st={3}".format(uniq_pid, panda_id, attempt_nr, job_status))
                        adder_gen = AdderGen(taskBuffer, panda_id, job_status, attempt_nr,
                                       ignoreTmpError=True, siteMapper=aSiteMapper, pid=uniq_pid, prelock_pid=self.prelock_pid)
                        n_processed += 1
                    else:
                        n_skipped += 1
                    if adder_gen is not None:
                        adder_gen.run()
                        del adder_gen
                except Exception:
                    type, value, traceBack = sys.exc_info()
                    tmpLog.error("%s %s" % (type,value))
                # unlock prelocked reports if possible
                taskBuffer.unlockJobOutputReport(
                            panda_id=panda_id, attempt_nr=attempt_nr, pid=self.prelock_pid)
            # stats
            tmpLog.debug("pid={0} : processed {1} , skipped {2}".format(uniq_pid, n_processed, n_skipped))

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
    tmpLog.debug("run Adder")

    # p = AdderProcess()
    # p.run(taskBuffer, aSiteMapper, holdingAna)

    adderThrList = []
    nThr = 10

    n_jors_per_batch = 2000

    # get some job output reports
    jor_list = taskBuffer.listJobOutputReport(only_unlocked=True, time_limit=10,
                                                limit=n_jors_per_batch*nThr,
                                                grace_period=gracePeriod)
    if len(jor_list) < n_jors_per_batch*nThr*0.875:
        # got too few job output reports from DB, can stop the daemon loop
        ret_val = False

    # TaskBuffer with more connections behind TaskBufferInterface
    n_connections = 4
    _tbuf = TaskBuffer()
    _tbuf.init(panda_config.dbhost, panda_config.dbpasswd, nDBConnection=n_connections)

    # try to pre-lock records for a short period of time, so that multiple nodes can get different records
    prelock_pid = GenericThread().get_pid()
    def lock_one_jor(one_jor):
        panda_id, job_status, attempt_nr, time_stamp = one_jor
        # get lock
        got_lock = _tbuf.lockJobOutputReport(
                        panda_id=panda_id, attempt_nr=attempt_nr,
                        pid=prelock_pid, time_limit=10)
        return got_lock
    with ThreadPoolExecutor(n_connections) as thread_pool:
        jor_lock_list = thread_pool.map(lock_one_jor, jor_list)

    # fill in queue
    job_output_reports = dict()
    report_index_list = multiprocessing.Queue()
    for one_jor, got_lock in zip(jor_list, jor_lock_list):
        if got_lock:
            panda_id, job_status, attempt_nr, time_stamp = one_jor
            report_index = (panda_id, attempt_nr)
            job_output_reports[report_index] = one_jor
            report_index_list.put(report_index)
    # number of job reports to consume
    n_jors = report_index_list.qsize()
    tmpLog.debug("prelock_pid={0} got {1} job reports".format(prelock_pid, n_jors))
    if n_jors == 0:
        # no job report to consume, return
        tmpLog.debug("===================== end =====================")
        return ret_val

    # taskBuffer interface for multiprocessing
    taskBufferIF = TaskBufferInterface()
    taskBufferIF.launch(_tbuf)

    # adder consumer processes
    _n_thr_with_tbuf = 0
    for i in range(nThr):
        if i < _n_thr_with_tbuf:
            tbuf = TaskBuffer()
            tbuf.init(panda_config.dbhost, panda_config.dbpasswd, nDBConnection=1)
            thr = AdderThread(tbuf, aSiteMapper, holdingAna,
                                job_output_reports, report_index_list, prelock_pid)
        else:
            thr = AdderThread(taskBufferIF.getInterface(), aSiteMapper, holdingAna,
                                job_output_reports, report_index_list, prelock_pid)
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

    # stop TaskBuffer IF
    taskBufferIF.stop()

    tmpLog.debug("===================== end =====================")

    # return
    return ret_val


# run
if __name__ == '__main__':
    main(argv=sys.argv)
