import copy
import datetime
import gc
import importlib
import json
import multiprocessing
import os
import queue
import signal
import socket
import sys
import threading
import time
import traceback

import psutil
from pandacommon.pandalogger import logger_utils
from pandacommon.pandautils.thread_utils import GenericThread, LockPool

from pandaserver.config import daemon_config, panda_config

# list of signals accepted to end the main process
END_SIGNALS = [
    signal.SIGINT,
    signal.SIGHUP,
    signal.SIGTERM,
]

# mandatory attributes and their type of daemon
MANDATORY_ATTRS = [
    ("module", str),
    ("period", int),
    ("arguments", list),
]

# command to send in pipe to stop daemon worker processes
CMD_STOP = "__STOP"

# epoch datetime
EPOCH = datetime.datetime.fromtimestamp(0)

# requester id for taskbuffer
requester_id = GenericThread().get_full_id(__name__, sys.modules[__name__].__file__)


def kill_proc_tree(pid, sig=signal.SIGKILL, include_parent=True, timeout=None, on_terminate=None):
    """
    Kill a process tree (including grandchildren) with signal "sig" and return a (gone, still_alive) tuple.
    "on_terminate", if specified, is a callback function which is called as soon as a child terminates.
    """
    assert pid != os.getpid(), "will not kill myself"
    parent = psutil.Process(pid)
    children = parent.children(recursive=True)
    if include_parent:
        children.append(parent)
    for p in children:
        try:
            p.send_signal(sig)
        except psutil.NoSuchProcess:
            pass
    gone, alive = psutil.wait_procs(children, timeout=timeout, callback=on_terminate)
    return (gone, alive)


def daemon_loop(dem_config, msg_queue, pipe_conn, worker_lifetime, tbuf=None, lock_pool=None):
    """
    Main loop of daemon worker process
    """
    # pid of the worker
    my_pid = os.getpid()
    my_full_pid = f"{socket.getfqdn().split('.')[0]}-{os.getpgrp()}-{my_pid}"
    # logger to log in file
    base_logger = logger_utils.setup_logger("daemons")
    tmp_log = logger_utils.make_logger(base_logger, f"worker_pid={my_pid}")
    tmp_log.info("daemon worker start")

    # signal handler
    def got_end_sig(sig, frame):
        tmp_log.warning(f"(got signal {sig})")

    for sig in END_SIGNALS:
        signal.signal(sig, got_end_sig)

    # dict of all daemons and their script module object
    module_map = {}
    # package of daemon scripts
    mod_package = getattr(daemon_config, "package")
    # start timestamp
    start_ts = time.time()
    # expiry time
    expiry_ts = start_ts + worker_lifetime
    # timestamp of getting last message
    last_msg_ts = start_ts
    # timestamp of last warning of no message
    last_no_msg_warn_ts = start_ts
    # interval in second for warning of no message
    no_msg_warn_interval = 300
    # create taskBuffer object if not given
    if tbuf is None:
        # initialize oracledb using dummy connection
        try:
            from pandaserver.taskbuffer.Initializer import initializer

            initializer.init()
        except Exception as e:
            tmp_log.error(f"failed to launch initializer with {e.__class__.__name__}: {e} ; terminated")
            return
        # taskBuffer object
        try:
            from pandaserver.taskbuffer.TaskBuffer import taskBuffer as tbuf

            tbuf.init(
                panda_config.dbhost,
                panda_config.dbpasswd,
                nDBConnection=1,
                useTimeout=True,
                requester=requester_id,
            )
            tmp_log.debug("taskBuffer initialized")
        except Exception as e:
            tmp_log.error(f"failed to initialize taskBuffer with {e.__class__.__name__}: {e} ; terminated")
            return
    # import module of all daemons
    for dem_name, attrs in dem_config.items():
        mod_name = attrs["module"]
        try:
            the_module = importlib.import_module(f".{mod_name}", mod_package)
            module_map[dem_name] = the_module
        except Exception as e:
            tmp_log.warning(f"for daemon {dem_name}, failed to import {mod_name} with {e.__class__.__name__}: {e} ; skipped it")
        else:
            module_map[dem_name] = the_module
    tmp_log.debug("initialized, running")
    # loop
    while True:
        # stop the worker since when reaches its lifetime
        if time.time() > expiry_ts:
            tmp_log.info("worker reached its lifetime, stop this worker")
            break
        # get command from pipe
        if pipe_conn.poll():
            cmd = pipe_conn.recv()
            if cmd == CMD_STOP:
                # got stop command, stop the process
                tmp_log.info("got stop command, stop this worker")
                break
            else:
                tmp_log.debug(f'got invalid command "{cmd}" ; skipped it')
        # clean up memory
        gc.collect()
        # get a message from queue
        tmp_log.debug("waiting for message...")
        keep_going = True
        one_msg = None
        while True:
            try:
                one_msg = msg_queue.get(timeout=5)
                last_msg_ts = time.time()
                break
            except queue.Empty:
                now_ts = time.time()
                # timeout warning if not getting messages for long time
                no_msg_duration = now_ts - last_msg_ts
                last_no_msg_warn_duration = now_ts - last_no_msg_warn_ts
                if no_msg_duration >= no_msg_warn_interval and last_no_msg_warn_duration >= no_msg_warn_interval:
                    tmp_log.warning(f"no message gotten (qid={id(msg_queue)}) for {no_msg_duration:.3f} sec")
                    last_no_msg_warn_ts = now_ts
                # timeout to get from queue, check whether to keep going
                if now_ts > expiry_ts:
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
            tmp_log.debug(f"got message of {dem_name}")
            the_module = module_map[dem_name]
            attrs = dem_config[dem_name]
            mod_args = attrs["arguments"]
            mod_argv = tuple([__file__] + mod_args)
            dem_period = attrs["period"]
            dem_period_in_minute = dem_period / 60.0
            is_sync = attrs["sync"]
            is_loop = attrs["loop"]
            # initialize variables
            to_run_daemon = False
            has_run = False
            last_run_start_ts = 0
            last_run_end_ts = 0
            # component name in lock table
            component = f"pandaD.{dem_name}"
            # whether the daemon should be synchronized among nodes
            if is_sync:
                # synchronized daemon, check process lock in DB
                ret_val, locked_time = tbuf.checkProcessLock_PANDA(
                    component=component,
                    pid=my_full_pid,
                    time_limit=dem_period_in_minute,
                )
                if ret_val:
                    # locked by some process on other nodes
                    last_run_start_ts = int((locked_time - EPOCH).total_seconds())
                    tmp_log.debug(f"found {dem_name} is locked by other process ; skipped it")
                else:
                    # try to get the lock
                    got_lock = tbuf.lockProcess_PANDA(
                        component=component,
                        pid=my_full_pid,
                        time_limit=dem_period_in_minute,
                    )
                    if got_lock:
                        # got the lock
                        to_run_daemon = True
                        tmp_log.debug(f"got lock of {dem_name}")
                    else:
                        # did not get lock, skip
                        last_run_start_ts = int(time.time())
                        tmp_log.debug(f"did not get lock of {dem_name} ; skipped it")
            else:
                to_run_daemon = True
            # run daemon
            if to_run_daemon:
                last_run_start_ts = int(time.time())
                # send daemon status back to master
                status_tuple = (dem_name, to_run_daemon, has_run, last_run_start_ts, last_run_end_ts)
                pipe_conn.send(status_tuple)
                try:
                    if is_loop:
                        # go looping the script until reaching daemon period
                        tmp_log.info(f"{dem_name} start looping")
                        start_ts = time.time()
                        while True:
                            ret_val = the_module.main(argv=mod_argv, tbuf=tbuf, lock_pool=lock_pool)
                            now_ts = time.time()
                            if not ret_val:
                                # daemon main function says stop the loop
                                break
                            if now_ts > start_ts + dem_period:
                                # longer than the period, stop the loop
                                break
                        tmp_log.info(f"{dem_name} finish looping")
                    else:
                        # execute the module script with arguments
                        tmp_log.info(f"{dem_name} start")
                        the_module.main(argv=mod_argv, tbuf=tbuf, lock_pool=lock_pool)
                        tmp_log.info(f"{dem_name} finish")
                except Exception as e:
                    # with error
                    tb = traceback.format_exc()
                    tmp_log.error(f"failed to run daemon {dem_name} with {e.__class__.__name__}: {e}\n{tb}\n ; stop this worker")
                    # daemon has run but failed
                    last_run_end_ts = int(time.time())
                    has_run = True
                    # send daemon status back to master
                    status_tuple = (
                        dem_name,
                        to_run_daemon,
                        has_run,
                        last_run_start_ts,
                        last_run_end_ts,
                    )
                    pipe_conn.send(status_tuple)
                    # stop the worker
                    break
                else:
                    # daemon has run
                    last_run_end_ts = int(time.time())
                    has_run = True
            # send daemon status back to master
            status_tuple = (dem_name, to_run_daemon, has_run, last_run_start_ts, last_run_end_ts)
            pipe_conn.send(status_tuple)
        else:
            # got invalid message
            tmp_log.warning(f'got invalid message "{one_msg}", skipped it')
        # sleep
        time.sleep(2**-5)


class DaemonWorker(object):
    """
    Class of worker process of PanDA daemon
    """

    __slots__ = (
        "pid",
        "parent_conn",
        "child_conn",
        "process",
        "dem_name",
        "dem_ts",
    )

    # class lock
    _lock = threading.Lock()

    # constructor
    def __init__(self, dem_config, msg_queue, worker_lifetime, tbuf=None, lock_pool=None):
        # synchronized with lock
        with self._lock:
            self._make_pipe()
            self._make_process(
                dem_config=dem_config,
                msg_queue=msg_queue,
                worker_lifetime=worker_lifetime,
                tbuf=tbuf,
                lock_pool=lock_pool,
            )

    def _make_pipe(self):
        """
        make pipe connection pairs between master and this worker
        """
        self.parent_conn, self.child_conn = multiprocessing.Pipe()

    def _close_pipe(self):
        """
        close pipe connection pairs between master and this worker
        """
        self.parent_conn.close()
        self.child_conn.close()

    def _make_process(self, dem_config, msg_queue, worker_lifetime, tbuf, lock_pool):
        """
        make associate process of this worker
        """
        args = (
            dem_config,
            msg_queue,
            self.child_conn,
            worker_lifetime,
            tbuf,
            lock_pool,
        )
        self.process = multiprocessing.Process(target=daemon_loop, args=args)

    def start(self):
        """
        start the worker process
        """
        self.unset_dem()
        self.process.start()
        self.pid = self.process.pid

    def is_alive(self):
        """
        whether the worker process is alive
        """
        return self.process.is_alive()

    def kill(self):
        """
        kill the worker process and all its subprocesses
        """
        self._close_pipe()
        return kill_proc_tree(self.process.pid)

    def is_running_dem(self):
        """
        whether the worker is still running a daemon script
        """
        return not (self.dem_name is None and self.dem_ts is None)

    def set_dem(self, dem_name, dem_ts):
        """
        set current running daemon in this worker
        """
        if not self.is_running_dem() or dem_ts >= self.dem_ts:
            self.dem_name = dem_name
            self.dem_ts = dem_ts

    def unset_dem(self):
        """
        unset current running daemon in this worker
        """
        self.dem_ts = None
        self.dem_name = None


class DaemonMaster(object):
    """
    Class of master process of PanDA daemon
    """

    # constructor
    def __init__(self, logger, n_workers=1, n_dbconn=1, worker_lifetime=28800, use_tbif=False):
        # logger
        self.logger = logger
        # number of daemon worker processes
        self.n_workers = n_workers
        # number of db connections for common taskBuffer interface
        self.n_dbconn = n_dbconn
        # lifetime of daemon worker processes
        self.worker_lifetime = worker_lifetime
        # whether to use TaskBufferInterface to save DB sessions while prone to taskbuffer hanging
        self.use_tbif = use_tbif
        # locks
        self._worker_lock = threading.Lock()
        self._status_lock = threading.Lock()
        # make message queue
        self._reset_msg_queue()
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
        # shared lock pool
        self.lock_pool = LockPool()
        # spawn workers
        self._spawn_workers(self.n_workers)

    def _reset_msg_queue(self):
        """
        reset the message queue for sending commands to workers
        """
        self.msg_queue = multiprocessing.Queue()
        self.logger.info(f"reset message queue (qid={id(self.msg_queue)})")

    def _make_tbif(self):
        """
        make common taskBuffer interface for daemon workers
        """
        try:
            # import is always required to have reserveChangedState consistent in *Spec
            from pandaserver.taskbuffer.TaskBuffer import TaskBuffer
            from pandaserver.taskbuffer.TaskBufferInterface import TaskBufferInterface

            if not self.use_tbif:
                return
            # taskBuffer
            _tbuf = TaskBuffer()
            _tbuf.init(
                panda_config.dbhost,
                panda_config.dbpasswd,
                nDBConnection=self.n_dbconn,
                useTimeout=True,
                requester=requester_id,
            )
            # taskBuffer interface for multiprocessing
            taskBufferIF = TaskBufferInterface()
            taskBufferIF.launch(_tbuf)
            self.logger.debug("taskBuffer interface initialized")
            self.tbif = taskBufferIF
        except Exception as e:
            self.logger.error(f"failed to initialize taskBuffer interface with {e.__class__.__name__}: {e} ; terminated")
            raise e

    def _spawn_workers(self, n_workers=1, auto_start=False):
        """
        spawn new workers and put them into worker pool
        """
        for j in range(n_workers):
            with self._worker_lock:
                if self.use_tbif:
                    tbuf = self.tbif.getInterface()
                else:
                    tbuf = None
                worker = DaemonWorker(
                    dem_config=self.dem_config,
                    msg_queue=self.msg_queue,
                    worker_lifetime=self.worker_lifetime,
                    tbuf=tbuf,
                    lock_pool=self.lock_pool,
                )
                self.worker_pool.add(worker)
                if auto_start:
                    worker.start()
                    self.logger.debug(f"launched new worker_pid={worker.pid}")

    def _remove_worker(self, worker):
        """
        remove a worker from pool
        """
        with self._worker_lock:
            self.worker_pool.discard(worker)

    def _parse_config(self):
        """
        parse configuration of PanDA daemon
        """
        try:
            config_json = daemon_config.config
            config_dict = json.loads(config_json)
            self.dem_config = copy.deepcopy(config_dict)
            # loop over daemons
            for dem_name, attrs in config_dict.items():
                # remove disabled daemons
                if "enable" in attrs and attrs["enable"] is False:
                    del self.dem_config[dem_name]
                    continue
                # handle option attributes
                if "module" not in attrs:
                    self.dem_config[dem_name]["module"] = dem_name
                if "arguments" not in attrs:
                    self.dem_config[dem_name]["arguments"] = []
                if "sync" not in attrs:
                    self.dem_config[dem_name]["sync"] = False
                if "loop" not in attrs:
                    self.dem_config[dem_name]["loop"] = False
                if "timeout" not in attrs:
                    self.dem_config[dem_name]["timeout"] = min(attrs["period"] * 3, attrs["period"] + 3600)
                # check mandatory attributes
                the_attrs = copy.deepcopy(self.dem_config[dem_name])
                for attr, attr_type in MANDATORY_ATTRS:
                    if attr not in the_attrs:
                        self.logger.warning(f'daemon config missing attribute "{attr}" for {dem_name} ; skipped')
                        del self.dem_config[dem_name]
                        break
                    elif not isinstance(the_attrs[attr], attr_type):
                        self.logger.warning(
                            f'daemon config has invalid type of attribute "{attr}" for {dem_name} (type must be {attr_type.__name__}) ; skipped'
                        )
                        del self.dem_config[dem_name]
                        break
        except Exception as e:
            tb = traceback.format_exc()
            self.logger.error(f"failed to parse daemon config, {e.__class__.__name__}: {e}\n{tb}\n")

    def _make_dem_run_map(self):
        """
        initialize daemon run status map
        """
        dem_run_map = {}
        for dem in self.dem_config:
            attrs = {}
            attrs["last_run_start_ts"] = 0
            attrs["last_warn_ts"] = 0
            attrs["msg_ongoing"] = False
            attrs["dem_running"] = False
            dem_run_map[dem] = attrs
        self.dem_run_map = dem_run_map

    def _kill_one_worker(self, worker):
        """
        kill one (stuck) worker (and new worker will be re-spawned in scheduler cycle)
        """
        # kill worker process and remove it from pool
        worker.kill()
        self._remove_worker(worker)
        # reset daemon run status map of the daemon run by the worker
        self.dem_run_map[worker.dem_name]["msg_ongoing"] = False
        self.dem_run_map[worker.dem_name]["dem_running"] = False

    def _scheduler_cycle(self):
        """
        main scheduler cycle
        """
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
                    (
                        dem_name,
                        to_run_daemon,
                        has_run,
                        last_run_start_ts,
                        last_run_end_ts,
                    ) = worker.parent_conn.recv()
                    # update run status map
                    dem_run_attrs = self.dem_run_map[dem_name]
                    old_last_run_start_ts = dem_run_attrs["last_run_start_ts"]
                    if last_run_start_ts > old_last_run_start_ts:
                        # take latest timestamp of run start
                        dem_run_attrs["last_run_start_ts"] = last_run_start_ts
                    if not has_run and last_run_start_ts > 0:
                        # worker not yet finishes running a daemon script
                        worker.set_dem(dem_name, last_run_start_ts)
                        if to_run_daemon:
                            dem_run_attrs["dem_running"] = True
                    if has_run and last_run_end_ts >= last_run_start_ts:
                        # worker already finishes running a daemon script
                        worker.unset_dem()
                        dem_run_attrs["dem_running"] = False
                        run_duration = last_run_end_ts - last_run_start_ts
                        run_period = self.dem_config[dem_name].get("period")
                        is_loop = self.dem_config[dem_name].get("loop")
                        if run_duration > run_period and not is_loop:
                            # warning since daemon run duration longer than daemon period (non-looping)
                            self.logger.warning(f"worker_pid={worker.pid} daemon {dem_name} took {run_duration} sec , exceeding its period {run_period} sec")
                    dem_run_attrs["msg_ongoing"] = False
                # kill the worker due to daemon run timeout
                if worker.is_running_dem():
                    run_till_now = now_ts - worker.dem_ts
                    run_timeout = self.dem_config[worker.dem_name].get("timeout")
                    if run_till_now > run_timeout:
                        self.logger.warning(
                            "worker_pid={pid} killing worker since daemon {dem} took {run} sec , exceeding its timeout {timeout} sec".format(
                                pid=worker.pid,
                                dem=worker.dem_name,
                                run=run_till_now,
                                timeout=run_timeout,
                            )
                        )
                        self._kill_one_worker(worker)
        # counter for super delayed daemons
        n_super_delayed_dems = 0
        # send message to workers
        for dem_name, attrs in self.dem_config.items():
            with self._status_lock:
                run_period = attrs.get("period")
                dem_run_attrs = self.dem_run_map[dem_name]
                last_run_start_ts = dem_run_attrs["last_run_start_ts"]
                last_warn_ts = dem_run_attrs["last_warn_ts"]
                if run_period is None or last_run_start_ts is None:
                    continue
                if last_run_start_ts + run_period <= now_ts:
                    # time to send new message to run the daemon
                    msg_ongoing = dem_run_attrs["msg_ongoing"]
                    dem_running = dem_run_attrs["dem_running"]
                    if msg_ongoing or dem_running:
                        # old message not processed yet or daemon still running, skip
                        run_delay = now_ts - (last_run_start_ts + run_period)
                        warn_since_ago = now_ts - last_warn_ts
                        if last_run_start_ts > 0 and run_delay > max(300, run_period // 2) and warn_since_ago > 900:
                            # make warning if super delayed
                            self.logger.warning(f"{dem_name} delayed to run for {run_delay} sec ")
                            dem_run_attrs["last_warn_ts"] = now_ts
                            n_super_delayed_dems += 1
                    else:
                        # old message processed, send new message
                        self.msg_queue.put(dem_name)
                        self.logger.debug(f"scheduled to run {dem_name} ; qsize={self.msg_queue.qsize()}")
                        dem_run_attrs["msg_ongoing"] = True
                        # dem_run_attrs['last_run_start_ts'] = now_ts
        # call revive if too many daemons are delayed too much (probably the queue is stuck)
        if n_super_delayed_dems >= min(4, int(len(self.dem_config) * 0.667)):
            self.logger.warning(f"found {n_super_delayed_dems} daemons delayed too much; start to revive")
            self.revive()
        # spawn new workers if there are less than n_workers
        now_n_workers = len(self.worker_pool)
        if now_n_workers < self.n_workers:
            n_up = self.n_workers - now_n_workers
            self._spawn_workers(n_workers=n_up, auto_start=True)
        # sleep
        time.sleep(0.5)

    def _stop_all_workers(self):
        """
        stop all workers gracefully by sending stop command to them
        """
        # send stop command
        for worker in self.worker_pool:
            worker.parent_conn.send(CMD_STOP)
            self.logger.debug(f"sent stop command to worker_pid={worker.pid}")
        # reset daemon run status map of all daemons
        for dem_name in self.dem_config:
            self.dem_run_map[dem_name]["msg_ongoing"] = False
            self.dem_run_map[dem_name]["dem_running"] = False

    def stop(self):
        """
        stop the master (and all workers)
        """
        self.logger.info("daemon master got stop")
        # stop scheduler from sending more message
        self.to_stop_scheduler = True
        # stop all workers gracefully
        self._stop_all_workers()
        # wait a bit
        time.sleep(1)
        # close message queue
        self.msg_queue.close()
        # stop taskBuffer interface
        if self.use_tbif:
            self.tbif.stop()
        # wait a bit
        time.sleep(2)

    def revive(self):
        """
        revive: kill all workers, reset a new message queue, and spawn new workers with new queue
        """
        self.logger.info("daemon master reviving")
        # stop all workers gracefully
        self._stop_all_workers()
        # wait a bit
        time.sleep(3)
        # kill and remove workers
        for worker in list(self.worker_pool):
            worker.kill()
            self._remove_worker(worker)
        # close message queue
        self.msg_queue.close()
        # reset message queue
        self._reset_msg_queue()
        # spawn new workers with new queues
        self._spawn_workers(self.n_workers, auto_start=True)
        # done
        self.logger.info("daemon master revived")

    def run(self):
        """
        main function to run the master
        """
        # master pid
        master_pid = os.getpid()
        self.logger.info(f"daemon master started ; master_pid={master_pid}")
        # start daemon workers
        with self._worker_lock:
            for worker in self.worker_pool:
                worker.start()
                self.logger.debug(f"launched worker_pid={worker.pid}")
        self.logger.debug("daemon master launched all worker processes")
        # initialize old worker pid set
        worker_pid_list_old = []
        # loop of scheduler
        while not self.to_stop_scheduler:
            with self._worker_lock:
                worker_pid_list = [worker.pid for worker in self.worker_pool]
            if worker_pid_list != worker_pid_list_old:
                # log when worker pid list changes
                self.logger.debug(f"master_pid: {master_pid} ; worker_pids: {str(worker_pid_list)} ")
                worker_pid_list_old = worker_pid_list
            self._scheduler_cycle()
        # end
        self.logger.info("daemon master ended")
