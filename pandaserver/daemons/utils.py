import os
import sys
import time
import datetime
import json
import copy
import threading
import multiprocessing
import queue
import socket
import importlib
import traceback
import signal
import gc

from pandacommon.pandalogger import logger_utils
from pandaserver.config import panda_config, daemon_config


# list of signals accepted to end the main process
END_SIGNALS = [
        signal.SIGINT,
        signal.SIGHUP,
        signal.SIGTERM,
    ]

# mandatory attributes and thier type of daemon
MANDATORY_ATTRS = [
        ('module', str),
        ('period', int),
        ('arguments', list),
    ]

# command to send in pipe to stop daemon worker processes
CMD_STOP = '__STOP'

# epoch datetime
EPOCH = datetime.datetime.fromtimestamp(0)


# worker process loop of daemon
def daemon_loop(dem_config, msg_queue, pipe_conn, worker_lifetime, tbuf=None):
    # pid of the worker
    my_pid = os.getpid()
    my_full_pid = '{0}-{1}-{2}'.format(socket.getfqdn().split('.')[0], os.getpgrp(), my_pid)
    # logger to log in file
    base_logger = logger_utils.setup_logger('daemons')
    tmp_log = logger_utils.make_logger(base_logger, 'worker_pid={pid}'.format(pid=my_pid))
    tmp_log.info('daemon worker start')
    # signal handler
    def got_end_sig(sig, frame):
        tmp_log.warning('(got signal {sig})'.format(sig=sig))
    for sig in END_SIGNALS:
        signal.signal(sig, got_end_sig)
    # dict of all daemons and their script module object
    module_map = {}
    # package of daemon scripts
    mod_package = getattr(daemon_config, 'package')
    # start timestamp
    start_ts = time.time()
    # expiry time
    expiry_ts = start_ts + worker_lifetime
    # create taskBuffer object if not given
    if tbuf is None:
        # initialize cx_Oracle using dummy connection
        try:
            from pandaserver.taskbuffer.Initializer import initializer
            initializer.init()
        except Exception as e:
            tmp_log.error('failed to launch initializer with {err} ; terminated'.format(
                                err='{0}: {1}'.format(e.__class__.__name__, e)))
            return
        # taskBuffer object
        try:
            from pandaserver.taskbuffer.TaskBuffer import taskBuffer as tbuf
            tbuf.init(panda_config.dbhost, panda_config.dbpasswd, nDBConnection=1)
            tmp_log.debug('taskBuffer initialized')
        except Exception as e:
            tmp_log.error('failed to initialize taskBuffer with {err} ; terminated'.format(
                                err='{0}: {1}'.format(e.__class__.__name__, e)))
            return
    # import module of all daemons
    for dem_name, attrs in dem_config.items():
        mod_name = attrs['module']
        try:
            the_module = importlib.import_module('.{mod}'.format(mod=mod_name), mod_package)
            module_map[dem_name] = the_module
        except Exception as e:
            tmp_log.warning('for daemon {dem}, failed to import {mod} with {err} ; skipped it'.format(
                                dem=dem_name, mod=mod_name, err='{0}: {1}'.format(e.__class__.__name__, e)))
        else:
            module_map[dem_name] = the_module
    tmp_log.debug('initialized, running')
    # loop
    while True:
        # stop the worker since when reaches its lifetime
        if time.time() > expiry_ts:
            tmp_log.info('worker reached its lifetime, stop this worker')
            break
        # get command from pipe
        if pipe_conn.poll():
            cmd = pipe_conn.recv()
            if cmd == CMD_STOP:
                # got stop command, stop the process
                tmp_log.info('got stop command, stop this worker')
                break
            else:
                tmp_log.debug('got invalid command "{cmd}" ; skipped it'.format(cmd=cmd))
        # clean up memory
        gc.collect()
        # get a message from queue
        tmp_log.debug('waiting for message...')
        keep_going = True
        one_msg = None
        while True:
            try:
                one_msg = msg_queue.get(timeout=5)
                break
            except queue.Empty:
                # timeout to get from queue, check whether to keep going
                if time.time() > expiry_ts:
                    # worker expired, do not keep going
                    keep_going = False
                    break
        # keep going
        if not keep_going:
            continue
        # process message
        if one_msg in module_map and one_msg is not None:
            # got a daemon name, get the module object and corresponding attributes
            dem_name = one_msg
            tmp_log.debug('got message of {dem}'.format(dem=dem_name))
            the_module = module_map[dem_name]
            attrs = dem_config[dem_name]
            mod_args = attrs['arguments']
            mod_argv = tuple([__file__] + mod_args)
            dem_period = attrs['period']
            dem_period_in_minute = dem_period/60.
            is_sync = attrs['sync']
            is_loop = attrs['loop']
            # initialize variables
            to_run_daemon = False
            has_run = False
            last_run_start_ts = 0
            last_run_end_ts = 0
            # component name in lock table
            component = 'pandaD.{dem}'.format(dem=dem_name)
            # whether the daemon shoule be synchronized among nodes
            if is_sync:
                # sychronized daemon, check process lock in DB
                ret_val, locked_time = tbuf.checkProcessLock_PANDA(component=component, pid=my_full_pid, time_limit=dem_period_in_minute)
                if ret_val:
                    # locked by some process on other nodes
                    last_run_start_ts = int((locked_time - EPOCH).total_seconds())
                    tmp_log.debug('found {dem} is locked by other process ; skipped it'.format(dem=dem_name))
                else:
                    # try to get the lock
                    got_lock = tbuf.lockProcess_PANDA(component=component, pid=my_full_pid, time_limit=dem_period_in_minute)
                    if got_lock:
                        # got the lock
                        to_run_daemon = True
                        tmp_log.debug('got lock of {dem}'.format(dem=dem_name))
                    else:
                        # did not get lock, skip
                        last_run_start_ts = int(time.time())
                        tmp_log.debug('did not get lock of {dem} ; skipped it'.format(dem=dem_name))
            else:
                to_run_daemon = True
            # run daemon
            if to_run_daemon:
                last_run_start_ts = int(time.time())
                try:
                    if is_loop:
                        # go looping the script until reaching daemon period
                        tmp_log.info('{dem} start looping'.format(dem=dem_name))
                        start_ts = time.time()
                        while True:
                            ret_val = the_module.main(argv=mod_argv, tbuf=tbuf)
                            now_ts = time.time()
                            if not ret_val:
                                # daemon main function says stop the loop
                                break
                            if now_ts > start_ts + dem_period:
                                # longer than the period, stop the loop
                                break
                        tmp_log.info('{dem} finish looping'.format(dem=dem_name))
                    else:
                        # execute the module script with arguments
                        tmp_log.info('{dem} start'.format(dem=dem_name))
                        the_module.main(argv=mod_argv, tbuf=tbuf)
                        tmp_log.info('{dem} finish'.format(dem=dem_name))
                except Exception as e:
                    tb = traceback.format_exc()
                    tmp_log.error('failed to run daemon {dem} with {err} ; stop this worker'.format(
                                    dem=dem_name, err='{0}: {1}\n{2}\n'.format(e.__class__.__name__, e, tb)))
                    break
                # daemon has run
                last_run_end_ts = int(time.time())
                has_run = True
            # send daemon status back to master
            status_tuple = (dem_name, has_run, last_run_start_ts, last_run_end_ts)
            pipe_conn.send(status_tuple)
            # FIXME: stop and spawn worker in every run for now since some script breaks the worker without exception
            # tmp_log.info('as script done, stop this worker')
            # break
        else:
            # got invalid message
            tmp_log.warning('got invalid message "{msg}", skipped it'.format(msg=one_msg))
        # sleep
        time.sleep(2**-5)


# worker class of daemon process for PanDA server
class DaemonWorker(object):

    __slots__ = (
            'wid',
            'parent_conn',
            'child_conn',
            'process',
        )

    # class lock
    _lock = threading.Lock()

    # constructor
    def __init__(self, dem_config, msg_queue, worker_lifetime, tbuf=None):
        # synchronized with lock
        with self._lock:
            self._make_pipe()
            self._make_process( dem_config=dem_config,
                                msg_queue=msg_queue,
                                worker_lifetime=worker_lifetime,
                                tbuf=tbuf)

    # make pipe connection pairs for the worker
    def _make_pipe(self):
        self.parent_conn, self.child_conn = multiprocessing.Pipe()

    # make associated process
    def _make_process(self, dem_config, msg_queue, worker_lifetime, tbuf):
        args = (dem_config, msg_queue, self.child_conn, worker_lifetime, tbuf)
        self.process = multiprocessing.Process(target=daemon_loop, args=args)

    # start worker process
    def start(self):
        self.process.start()

    # whether worker process is alive
    def is_alive(self):
        return self.process.is_alive()


# master class of main daemon process for PanDA server
class DaemonMaster(object):

    # constructor
    def __init__(self, logger, n_workers=1, n_dbconn=1, worker_lifetime=28800):
        # logger
        self.logger = logger
        # number of daemon worker processes
        self.n_workers = n_workers
        # number of db connections for common taskBuffer interface
        self.n_dbconn = n_dbconn
        # lifetime of daemon worker processes
        self.worker_lifetime = worker_lifetime
        # locks
        self._worker_lock = threading.Lock()
        self._status_lock = threading.Lock()
        # make message queue
        self.msg_queue = multiprocessing.Queue()
        # process pool
        self.proc_pool = []
        # worker pool
        self.worker_pool = set()
        # whether to stop scheduler
        self.to_stop_scheduler = False
        # make daemon config
        self.dem_config = {}
        self._parse_config()
        # map of run status of daemons
        self.dem_run_map = {}
        self._make_dem_run_map()
        # shared taskBufferIF
        self.tbif = None
        self._make_tbif()
        # spawn workers
        self._spawn_workers(self.n_workers)

    # make common taskBuffer interface for daemon workers
    def _make_tbif(self):
        try:
            from pandaserver.taskbuffer.TaskBuffer import TaskBuffer
            from pandaserver.taskbuffer.TaskBufferInterface import TaskBufferInterface
            # taskBuffer
            _tbuf = TaskBuffer()
            _tbuf.init(panda_config.dbhost, panda_config.dbpasswd, nDBConnection=self.n_dbconn)
            # taskBuffer interface for multiprocessing
            taskBufferIF = TaskBufferInterface()
            taskBufferIF.launch(_tbuf)
            self.logger.debug('taskBuffer interface initialized')
            self.tbif = taskBufferIF
        except Exception as e:
            self.logger.error('failed to initialize taskBuffer interface with {err} ; terminated'.format(
                                err='{0}: {1}'.format(e.__class__.__name__, e)))
            raise e

    # spawn new workers and put into worker pool
    def _spawn_workers(self, n_workers=1, auto_start=False):
        for j in range(n_workers):
            with self._worker_lock:
                worker = DaemonWorker(  dem_config=self.dem_config,
                                        msg_queue=self.msg_queue,
                                        worker_lifetime=self.worker_lifetime,
                                        tbuf=self.tbif.getInterface())
                self.worker_pool.add(worker)
                if auto_start:
                    worker.start()

    # remove a worker from pool
    def _remove_worker(self, worker):
        with self._worker_lock:
            self.worker_pool.discard(worker)

    # parse daemon config
    def _parse_config(self):
        try:
            config_json = daemon_config.config
            config_dict = json.loads(config_json)
            self.dem_config = copy.deepcopy(config_dict)
            # loop over daemons
            for dem_name, attrs in config_dict.items():
                # remove disabled daemons
                if 'enable' in attrs and attrs['enable'] is False:
                    del self.dem_config[dem_name]
                    continue
                # handle option attributes
                if 'module' not in attrs:
                    self.dem_config[dem_name]['module'] = dem_name
                if 'arguments' not in attrs:
                    self.dem_config[dem_name]['arguments'] = []
                if 'sync' not in attrs:
                    self.dem_config[dem_name]['sync'] = False
                if 'loop' not in attrs:
                    self.dem_config[dem_name]['loop'] = False
                # check mandatory attributes
                the_attrs = copy.deepcopy(self.dem_config[dem_name])
                for attr, attr_type in MANDATORY_ATTRS:
                    if attr not in the_attrs:
                        self.logger.warning('daemon config missing attribute "{attr}" for {dem} ; skipped'.format(
                                            attr=attr, dem=dem_name))
                        del self.dem_config[dem_name]
                        break
                    elif not isinstance(the_attrs[attr], attr_type):
                        self.logger.warning('daemon config has invalid type of attribute "{attr}" for {dem} (type must be {typ}) ; skipped'.format(
                                            attr=attr, dem=dem_name, typ=attr_type.__name__))
                        del self.dem_config[dem_name]
                        break
        except Exception as e:
            tb = traceback.format_exc()
            self.logger.error('failed to parse daemon config, {err}'.format(
                                err='{0}: {1}\n{2}\n'.format(e.__class__.__name__, e, tb)))

    # make daemon run status map
    def _make_dem_run_map(self):
        dem_run_map = {}
        for dem in self.dem_config:
            attrs = {}
            attrs['last_run_start_ts'] = 0
            attrs['last_warn_ts'] = 0
            attrs['msg_ongoing'] = False
            dem_run_map[dem] = attrs
        self.dem_run_map = dem_run_map

    # one scheduler cycle
    def _scheduler_cycle(self):
        now_ts = int(time.time())
        # check last run time from pipes
        for worker in list(self.worker_pool):
            # remove dead worker from worker pool
            if not worker.is_alive():
                self._remove_worker(worker)
            # lock daemon run status
            with self._status_lock:
                # get message from the worker
                while worker.parent_conn.poll():
                    dem_name, has_run, last_run_start_ts, last_run_end_ts = worker.parent_conn.recv()
                    # update run status map
                    dem_run_attrs = self.dem_run_map[dem_name]
                    old_last_run_start_ts = dem_run_attrs['last_run_start_ts']
                    if last_run_start_ts > old_last_run_start_ts:
                        # take latest timestamp of run start
                        dem_run_attrs['last_run_start_ts'] = last_run_start_ts
                    if has_run and last_run_end_ts >= last_run_start_ts:
                        run_duration = last_run_end_ts - last_run_start_ts
                        run_period = self.dem_config[dem_name].get('period')
                        is_loop = self.dem_config[dem_name].get('loop')
                        if run_duration > run_period and not is_loop:
                            # warning since daemon run duration longer than daemon period (non-looping)
                            self.logger.warning('daemon {dem} took {dur} sec , longer than its period {period} sec'.format(
                                                dem=dem_name, dur=run_duration, period=run_period))
                    dem_run_attrs['msg_ongoing'] = False
        # send message to workers
        for dem_name, attrs in self.dem_config.items():
            run_period = attrs.get('period')
            dem_run_attrs = self.dem_run_map[dem_name]
            last_run_start_ts = dem_run_attrs['last_run_start_ts']
            last_warn_ts = dem_run_attrs['last_warn_ts']
            if run_period is None or last_run_start_ts is None:
                continue
            if last_run_start_ts + run_period <= now_ts:
                # time to send new message to run the daemon
                with self._status_lock:
                    dem_run_attrs = self.dem_run_map[dem_name]
                    msg_ongoing = dem_run_attrs['msg_ongoing']
                    if msg_ongoing:
                        # old message not processed yet, maybe daemon still running, skip
                        run_delay = now_ts - (last_run_start_ts + run_period)
                        warn_since_ago = now_ts - last_warn_ts
                        if last_run_start_ts > 0 \
                                and run_delay > max(300, run_period//2) \
                                and warn_since_ago > 900:
                            # make warning if delay too much
                            self.logger.warning('{dem} delayed to run for {delay} sec '.format(
                                                dem=dem_name, delay=run_delay))
                            dem_run_attrs['last_warn_ts'] = now_ts
                    else:
                        # old message processed, send new message
                        self.msg_queue.put(dem_name)
                        self.logger.debug('scheduled to run {dem}'.format(
                                            dem=dem_name))
                        dem_run_attrs['msg_ongoing'] = True
                        # dem_run_attrs['last_run_start_ts'] = now_ts
        # spawn new workers if ther are less than n_workers
        now_n_workers = len(self.worker_pool)
        if now_n_workers < self.n_workers:
            n_up = self.n_workers - now_n_workers
            self._spawn_workers(n_workers=n_up, auto_start=True)
        # sleep
        time.sleep(0.5)

    # send stop command to all worker processes
    def _stop_proc(self):
        for worker in self.worker_pool:
            worker.parent_conn.send(CMD_STOP)

    # stop master
    def stop(self):
        self.logger.info('daemon master got stop')
        # stop scheduler from sending more message
        self.to_stop_scheduler = True
        # send stop command to workers
        self._stop_proc()
        # stop taskBuffer interface
        self.tbif.stop()
        # wait a bit
        time.sleep(2.5)

    # run
    def run(self):
        # pid
        pid = os.getpid()
        self.logger.info('daemon master started ; pid={pid}'.format(pid=pid))
        # start daemon workers
        for worker in self.worker_pool:
            worker.start()
        self.logger.debug('daemon master launched all worker processes')
        # loop of scheduler
        while not self.to_stop_scheduler:
            self._scheduler_cycle()
        # end
        self.logger.info('daemon master ended')
