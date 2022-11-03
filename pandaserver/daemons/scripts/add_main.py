import random
import sys
import time
import datetime
import multiprocessing
import traceback

from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandautils.thread_utils import GenericThread, WeightedLists
from pandaserver.config import panda_config
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

    prelock_pid = GenericThread().get_pid()
    tmpLog = LogWrapper(_logger,"<pid={}>".format(prelock_pid))

    tmpLog.debug("===================== start =====================")

    # return value, true to run main again in next daemon loop
    ret_val = True

    # grace period
    try:
        gracePeriod = int(argv[1])
    except Exception:
        gracePeriod = 1

    # lock interval in minutes
    lock_interval = 10

    # retry interval in minutes
    retry_interval = 3

    # instantiate TB
    if tbuf is None:
        from pandaserver.taskbuffer.TaskBuffer import taskBuffer
        taskBuffer.init(panda_config.dbhost,panda_config.dbpasswd,nDBConnection=1, useTimeout=True)
    else:
        taskBuffer = tbuf

    # instantiate sitemapper
    aSiteMapper = SiteMapper(taskBuffer)


    # thread for adder
    class AdderThread(GenericThread):

        def __init__(self, taskBuffer, aSiteMapper,
                        job_output_reports):
            GenericThread.__init__(self)
            self.taskBuffer = taskBuffer
            self.aSiteMapper = aSiteMapper
            self.job_output_reports = job_output_reports

        # main loop
        def run(self):
            # initialize
            taskBuffer = self.taskBuffer
            aSiteMapper = self.aSiteMapper
            # get file list
            timeNow = datetime.datetime.utcnow()
            timeInt = datetime.datetime.utcnow()
            # unique pid
            GenericThread.__init__(self)
            uniq_pid = self.get_pid()
            # log pid
            tmpLog.debug("pid={0} : run".format(uniq_pid))
            # stats
            n_processed = 0
            # loop
            while True:
                # get report
                one_jor = self.job_output_reports.pop()
                if not one_jor:
                    break
                # lock
                panda_id, job_status, attempt_nr, time_stamp = one_jor
                got_lock = taskBuffer.lockJobOutputReport(
                    panda_id=panda_id, attempt_nr=attempt_nr,
                    pid=uniq_pid, time_limit=lock_interval)
                if not got_lock:
                    continue
                # add
                try:
                    modTime = time_stamp
                    if (timeNow - modTime) > datetime.timedelta(hours=24):
                        # last add
                        tmpLog.debug("pid={0} : last add job={1}.{2} st={3}".format(uniq_pid, panda_id,
                                                                                    attempt_nr, job_status))
                        ignoreTmpError = False
                    else:
                        # usual add
                        tmpLog.debug("pid={0} : add job={1}.{2} st={3}".format(uniq_pid, panda_id,
                                                                               attempt_nr, job_status))
                        ignoreTmpError = True
                    # get adder
                    adder_gen = AdderGen(taskBuffer, panda_id, job_status, attempt_nr,
                                         ignoreTmpError=ignoreTmpError, siteMapper=aSiteMapper, pid=uniq_pid,
                                         prelock_pid=uniq_pid, lock_offset=lock_interval-retry_interval)
                    n_processed += 1
                    # execute
                    adder_gen.run()
                    del adder_gen
                except Exception as e:
                    tmpLog.error("pid={} : failed to run with {} {}".format(uniq_pid, str(e), traceback.format_exc()))
            # stats
            tmpLog.debug("pid={} : processed {}".format(uniq_pid, n_processed))

        # launcher, run with multiprocessing
        def proc_launch(self):
            # run
            self.process = multiprocessing.Process(target=self.run)
            self.process.start()

        # join of multiprocessing
        def proc_join(self):
            self.process.join()


    # TaskBuffer with more connections behind TaskBufferInterface
    tmpLog.debug("setup taskBufferIF")
    n_connections = 4
    _tbuf = TaskBuffer()
    _tbuf.init(panda_config.dbhost, panda_config.dbpasswd, nDBConnection=n_connections, useTimeout=True)
    taskBufferIF = TaskBufferInterface()
    taskBufferIF.launch(_tbuf)

    # add files
    tmpLog.debug("run Adder")

    interval = 10
    nLoop = 10
    for iLoop in range(10):
        tmpLog.debug('start iLoop={}/{}'.format(iLoop, nLoop))
        start_time = datetime.datetime.utcnow()
        adderThrList = []
        nThr = 10

        n_jors_per_batch = 1000

        jor_lists = WeightedLists(multiprocessing.Lock())

        # get some job output reports
        jor_list_others = taskBuffer.listJobOutputReport(only_unlocked=True, time_limit=lock_interval,
                                                         limit=n_jors_per_batch*nThr,
                                                         grace_period=gracePeriod,
                                                         anti_labels=['user'])
        jor_lists.add(3, jor_list_others)
        jor_list_user = taskBuffer.listJobOutputReport(only_unlocked=True, time_limit=lock_interval,
                                                       limit=n_jors_per_batch*nThr,
                                                       grace_period=gracePeriod,
                                                       labels=['user'])
        jor_lists.add(7, jor_list_user)

        # adder consumer processes
        _n_thr_with_tbuf = 0
        tbuf_list = []
        tmpLog.debug("got {} job reports".format(len(jor_lists)))
        for i in range(nThr):
            if i < _n_thr_with_tbuf:
                tbuf = TaskBuffer()
                tbuf_list.append(tbuf)
                tbuf.init(panda_config.dbhost, panda_config.dbpasswd, nDBConnection=1, useTimeout=True)
                thr = AdderThread(tbuf, aSiteMapper, jor_lists)
            else:
                thr = AdderThread(taskBufferIF.getInterface(), aSiteMapper, jor_lists)
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
        [tbuf.cleanup() for tbuf in tbuf_list]
        end_time = datetime.datetime.utcnow()
        sleep_time = interval - (end_time - start_time).seconds
        if sleep_time > 0 and iLoop+1 < nLoop:
            sleep_time = random.randint(1, sleep_time)
            tmpLog.debug("sleep {} sec".format(sleep_time))
            time.sleep(sleep_time)

    # stop TaskBuffer IF
    taskBufferIF.stop()

    tmpLog.debug("===================== end =====================")

    # return
    return ret_val


# run
if __name__ == '__main__':
    main(argv=sys.argv)
