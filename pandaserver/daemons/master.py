import os
import sys
import time
import json
import copy
import pwd
import grp
import signal
import argparse
import datetime
import threading
import multiprocessing
import socket
import importlib
import traceback
import logging

try:
    import queue
except ImportError:
    import Queue as queue

import daemon
import lockfile

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
    ]

# command to send in pipe to stop daemon worker processes
CMD_STOP = '__STOP'

# epoch datetime
EPOCH = datetime.datetime.fromtimestamp(0)


# get the logger
def get_logger():
    my_logger = logging.getLogger('PanDA-Daemon-Master')
    # remove existing handlers
    while my_logger.hasHandlers():
        my_logger.removeHandler(my_logger.handlers[0])
    # make new handler
    _log_handler = logging.StreamHandler(sys.stdout)
    _log_formatter = logging.Formatter('%(asctime)s %(name)-12s: %(levelname)-8s %(message)s')
    _log_handler.setFormatter(_log_formatter)
    # add new handler
    my_logger.addHandler(_log_handler)
    # debug log level
    my_logger.setLevel(logging.DEBUG)
    # return logger
    return my_logger


# kill the whole process group
def kill_whole():
    os.killpg(os.getpgrp(), signal.SIGKILL)


# worker process loop of daemon
def _process_loop(dem_config, msg_queue, pipe_conn):
    # pid of the worker
    my_pid = os.getpid()
    my_full_pid = '{0}-{1}-{2}'.format(socket.getfqdn().split('.')[0], my_pid, os.getpgrp(), my_pid)
    # logger to log in file
    base_logger = logger_utils.setup_logger('daemons')
    tmp_log = logger_utils.make_logger(base_logger, 'worker_pid={pid}'.format(pid=my_pid))
    tmp_log.debug('worker start')
    # dict of all daemons and their script module object
    module_map = {}
    # package of daemon scripts
    mod_package = getattr(daemon_config, 'package')
    # taskBuffer object
    try:
        from pandaserver.taskbuffer.TaskBuffer import taskBuffer as tbif
        tbif.init(panda_config.dbhost, panda_config.dbpasswd, nDBConnection=1)
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
    tmp_log.debug('initialized, running')
    # loop
    while True:
        # get command from pipe
        if pipe_conn.poll():
            cmd = pipe_conn.recv()
            if cmd == CMD_STOP:
                # got stop command, stop the process
                tmp_log.debug('got stop command, stop this process')
                break
            else:
                tmp_log.debug('got invalid command "{cmd}" ; skipped it'.format(cmd=cmd))
        # get a message from queue
        tmp_log.debug('getting message to run daemon...')
        one_msg = msg_queue.get()
        # process message
        if one_msg in module_map:
            tmp_log.debug('got message to run daemon {dem}'.format(dem=dem_name))
            # got a daemon name, get the module object and corresponding attributes
            dem_name = one_msg
            the_module = module_map[dem_name]
            attrs = dem_config[dem_name]
            mod_args = attrs['arguments']
            dem_period = attrs['period']
            dem_period_in_minute = dem_period/60.
            is_sync = attrs['sync']
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
                ret_val, locked_time = tbif.checkProcessLock_PANDA(component=component, pid=my_pid, time_limit=dem_period_in_minute)
                if ret_val:
                    # locked by some process on other nodes
                    last_run_start_ts = int((locked_time - EPOCH).total_seconds())
                else:
                    # try to get the lock
                    got_lock = tbif.lockProcess_PANDA(component=component, pid=my_pid, time_limit=dem_period_in_minute)
                    if got_lock:
                        # got the lock
                        to_run_daemon = True
                    else:
                        # did not get lock, skip
                        pass
            else:
                to_run_daemon = True
            # run daemon
            if to_run_daemon:
                last_run_start_ts = int(time.time())
                try:
                    # execute the module script with arguments
                    tmp_log.debug('start daemon {dem}'.format(dem=dem_name))
                    the_module.main(argv=mod_args, tbif=tbif)
                    tmp_log.debug('finish daemon {dem}'.format(dem=dem_name))
                except Exception as e:
                    tb = traceback.format_exc()
                    tmp_log.error('failed to run daemon {dem} with {err} ; skipped it'.format(
                                    dem=dem_name, err='{0}: {1}\n{2}\n'.format(e.__class__.__name__, e, tb)))
                # daemon has run
                last_run_end_ts = int(time.time())
                has_run = True
            # send daemon status back to master
            status_tuple = (dem_name, has_run, last_run_start_ts, last_run_end_ts)
            pipe_conn.send(status_tuple)
        else:
            # got invalid message
            tmp_log.error('got invalid message "{msg}", skipped it'.format(msg=dem_name))
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
    def __init__(self, dem_config, msg_queue):
        # synchronized with lock
        with self._lock:
            self._make_pipe()
            self._make_process(dem_config=dem_config, msg_queue=msg_queue)

    # make pipe connection pairs for the worker
    def _make_pipe(self):
        self.parent_conn, self.child_conn = multiprocessing.Pipe()

    # make associated process
    def _make_process(self, dem_config, msg_queue):
        args = (dem_config, msg_queue, self.child_conn)
        self.process = multiprocessing.Process(target=_process_loop, args=args)

    # start worker process
    def start(self):
        self.process.start()

    # whether worker process is alive
    def is_alive(self):
        return self.process.is_alive()


# master class of main daemon process for PanDA server
class DaemonMaster(object):

    # constructor
    def __init__(self, logger, n_workers=1):
        # logger
        self.logger = logger
        # number of daemon worker processes
        self.n_workers = n_workers
        # locks
        self._worker_lock = threading.Lock()
        self._status_lock = threading.Lock()
        # make message queue
        self.msg_queue = multiprocessing.SimpleQueue()
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
        # spawn workers
        self._spawn_workers(self.n_workers)

    # spawn new workers and put into worker pool
    def _spawn_workers(self, n_workers=1, auto_start=False):
        for j in range(n_workers):
            with self._worker_lock:
                worker = DaemonWorker(dem_config=self.dem_config, msg_queue=self.msg_queue)
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
                # handle option attributes
                if 'module' not in attrs:
                    self.dem_config[dem_name]['module'] = dem_name
                if 'arguments' in attrs:
                    self.dem_config[dem_name]['arguments'] = tuple(attrs['arguments'])
                else:
                    self.dem_config[dem_name]['arguments'] = tuple()
                if 'sync' not in attrs:
                    self.dem_config[dem_name]['sync'] = False
                # check mandatory attributes
                for attr, attr_type in MANDATORY_ATTRS:
                    if attr not in attrs or not isinstance(attrs[attr], attr_type):
                        self.logger.warning('daemon config missing "{attr}" attribute for {dem} ; skipped'.format(
                                            attr=attr, dem=dem_name))
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
                        if run_duration > run_period:
                            # warning since daemon run duration longer than daemon period
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
                        if run_delay > max(300, run_period//2) and warn_since_ago > 900:
                            # make warning if delay too much
                            self.logger.warning('daemon {dem} delayed to run for {delay} sec '.format(
                                                dem=dem_name, delay=run_delay))
                            dem_run_attrs['last_warn_ts'] = now_ts
                    else:
                        # old message processed, send new message
                        self.msg_queue.put(dem_name)
                        self.logger.info('scheduled daemon {dem} to run'.format(
                                            dem=dem_name))
                        dem_run_attrs['msg_ongoing'] = True
                        dem_run_attrs['last_run_start_ts'] = now_ts
        # spwan new workers if ther are less than n_workers
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
        # stop scheduler from sending more message
        self.to_stop_scheduler = True
        # send stop signal to workers
        self._stop_proc()
        # wait a bit
        time.sleep(3)

    # run
    def run(self):
        # pid
        pid = os.getpid()
        self.logger.info('daemon master started ; pid={pid}'.format(pid=pid))
        # start daemon workers
        for worker in self.worker_pool:
            worker.start()
        self.logger.info('daemon master launched all worker processes')
        # loop of scheduler
        while not self.to_stop_scheduler:
            self._scheduler_cycle()
        # end
        self.logger.info('daemon master ended')


# main function
def main():
    # whether to run daemons
    if not getattr(daemon_config, 'enable', False):
        return
    # get logger
    main_log = get_logger()
    # parse option
    parser = argparse.ArgumentParser()
    parser.add_argument('-P', '--pidfile', action='store', dest='pidfile',
                        default=None, help='pid filename')
    options = parser.parse_args()
    uname = getattr(daemon_config, 'uname', 'nobody')
    gname = getattr(daemon_config, 'gname', 'nobody')
    uid = pwd.getpwnam(uname).pw_uid
    gid = grp.getgrnam(gname).gr_gid
    n_workers = getattr(daemon_config, 'n_proc', 1)
    main_log.info('main start')
    # daemon context
    dc = daemon.DaemonContext(  stdout=sys.stdout, stderr=sys.stderr,
                                uid=uid, gid=gid,
                                pidfile=lockfile.FileLock(options.pidfile))
    with dc:
        # get logger inside daemon context
        tmp_log = get_logger()
        # record in PID file
        with open(options.pidfile, 'w') as pid_file:
            pid_file.write('{0}'.format(os.getpid()))
        # master object
        master = DaemonMaster(logger=tmp_log, n_workers=n_workers)
        # function to end master when end signal caught
        def end_master(sig, frame):
            master.stop()
            kill_whole()
        # set signal handler
        for sig in END_SIGNALS:
            signal.signal(sig, end_master)
        # start master
        master.run()
    # get logger again
    main_log = get_logger()
    main_log.info('main end')


# run
if __name__ == '__main__':
    main()
