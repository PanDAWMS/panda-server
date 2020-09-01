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
import multiprocessing
import importlib
import traceback
import logging

import daemon

from pandaserver.config import daemon_config


# list of signals accepted to end the main process
END_SIGNALS = [
        signal.SIGINT,
        signal.SIGHUP,
        signal.SIGTERM,
    ]


# command to send in pipe to stop daemon worker processes
CMD_STOP = '__STOP'


# epoch datetime
EPOCH = datetime(1970, 1, 1)

# get the logger
def get_logger():
    my_logger = logging.getLogger('PanDA-Daemon')
    # remove existing handlers
    while my_logger.hasHandlers():
        my_logger.removeHandler(logger.handlers[0])
    # make new handler
    _log_handler = logging.StreamHandler(sys.stdout)
    _log_formatter = logging.Formatter('%(asctime)s %(name)-12s: %(levelname)-8s %(message)s')
    _log_handler.setFormatter(_log_formatter)
    # add new handler
    my_logger.addHandler(_log_handler)
    # return logger
    return my_logger


# kill the whole process group
def kill_whole():
    os.killpg(os.getpgrp(), signal.SIGKILL)


# worker process loop of daemon
def _process_loop(mod_config, msg_queue, pipe_conn):
    # logger
    tmp_log = get_logger()
    # pid of the worker
    my_pid = os.getpid()
    # dict of all script modules
    module_map = {}
    # taskBuffer object
    from pandaserver.taskbuffer.TaskBuffer import taskBuffer as tbif
    tbif.init(panda_config.dbhost, panda_config.dbpasswd, nDBConnection=1)
    # import all modules
    for mod_name in mod_config:
        try:
            module_map[mod_name] = importlib.import_module(mod_name)
        except Exception as e:
            tmp_log.warning('<worker_pid={pid}> failed to import {mod} with {err} ; skipped it'.format(
                                pid=my_pid, mod=mod_name, err='{0}: {1}'.format(e.__class__.__name__, e)))
    tmp_log.debug('<worker_pid={pid}> initialized, running'.format(pid=my_pid))
    # loop
    while True:
        # get command from pipe
        if pipe_conn.poll():
            cmd = pipe_conn.recv()
            if cmd == CMD_STOP:
                # got stop command, stop the process
                tmp_log.debug('<worker_pid={pid}> got stop command, stop this process'.format(pid=my_pid))
                break
            else:
                tmp_log.debug('<worker_pid={pid}> got invalid command "{cmd}" ; skipped it'.format(pid=my_pid, cmd=cmd))
        # get a message from queue
        one_msg = msg_queue.get()
        # process message
        if one_msg in module_map:
            # got a module name, get the module object and corresponding attributes
            mod_name = one_msg
            the_module = module_map[mod_name]
            attrs = mod_config[mod_name]
            mod_period = attrs['period']
            mod_period_in_minute = mod_period/60.
            is_sync = attrs['sync']
            # initialize variables
            to_run_module = False
            last_run_ts = 0
            # component name in lock table
            component = 'pandaD.{mod}'.format(mod=mod_name)
            # whether the daemon shoule be synchronized among nodes
            if is_sync:
                # sychronized daemon, check process lock in DB
                ret_val, locked_time = tbif.checkProcessLock_PANDA(component=component, pid=my_pid, time_limit=mod_period_in_minute)
                if ret_val:
                    # locked by some process on other nodes
                    last_run_ts = int((locked_time - EPOCH).total_seconds())
                else:
                    # try to get the lock
                    got_lock = tbif.lockProcess_PANDA(component=component, pid=my_pid, time_limit=mod_period_in_minute)
                    if got_lock:
                        # got the lock
                        to_run_module = True
                    else:
                        # did not get lock, skip
                        pass
            else:
                to_run_module = True
            # run module
            if to_run_module:
                last_run_ts = int(time.time())
                try:
                    # execute the module script
                    tmp_log.debug('<worker_pid={pid}> start running module {mod}'.format(pid=my_pid, mod=mod_name))
                    the_module.main(tbif=tbif)
                    tmp_log.debug('<worker_pid={pid}> finish running module {mod}'.format(pid=my_pid, mod=mod_name))
                except Exception as e:
                    tb = traceback.format_exc()
                    tmp_log.error('<worker_pid={pid}> failed to run module {mod} with {err} ; skipped it'.format(
                                    pid=my_pid, mod=mod_name, err='{0}: {1}\n{2}\n'.format(e.__class__.__name__, e, tb)))
            # send module last run timestamp back to master
            pipe_conn.send((mod_name, last_run_ts))
        else:
            # got invalid message
            tmp_log.error('<worker_pid={pid}> got invalid message "{msg}", skipped it'.format(pid=my_pid, msg=mod_name))
        # sleep
        time.sleep(2**-5)


# master class of main daemon process for PanDA server
class DaemonMaster(object):

    # constructor
    def __init__(self, logger, n_workers=1):
        # logger
        self.logger = logger
        # number of daemon worker processes
        self.n_workers = n_workers
        # make message queue
        self.msg_queue = multiprocessing.SimpleQueue()
        # list of pipe connection pairs
        self.pipe_list = []
        self._make_pipes(self.n_workers)
        # make process pool
        self.proc_pool = multiprocessing.Pool(processes=self.n_workers)
        # whether to stop scheduler
        self.to_stop_scheduler = False
        # make module config
        self.mod_config = {}
        self._parse_config()
        # map of timestamp of last time the module run
        self.last_run_map = { mod: 0 for mod in self.mod_config }

    # make pipe connection pairs for each worker and put them into the list
    def _make_pipes(self, n_workers):
        self.pipe_list = []
        for j in range(n_workers):
            parent_conn, child_conn = multiprocessing.Pipe()
            self.pipe_list.append((parent_conn, child_conn))

    # parse daemon config
    def _parse_config(self):
        try:
            config_json = daemon_config.config
            config_dict = json.loads(config_json)
            self.mod_config = copy.deepcopy(config_dict)
            # loop over modules
            for mod_name, attrs in config_dict.items():
                # remove disabled modules
                if 'enable' in attrs and attrs['enable'] is False:
                    del self.mod_config[mod_name]
                # check mandatory fields
                if 'period' not in attrs or not isinstance(attrs['enable'], int):
                    self.logger.warning('daemon config missing period field for {mod} ; skipped'.format(
                                        mod=mod_name))
                    del self.mod_config[mod_name]
                if 'sync' not in attrs:
                    self.mod_config[mod_name]['sync'] = False
        except Exception as e:
            tb = traceback.format_exc()
            self.logger.error('failed to parse daemon config, {err}'.format(
                                err='{0}: {1}\n{2}\n'.format(e.__class__.__name__, e, tb)))

    # one scheduler cycle
    def _scheduler_cycle(self):
        now_ts = int(time.time())
        # check last run time from pipes
        for parent_conn, child_conn in self.pipe_list:
            # get message from the worker
            while parent_conn.poll():
                mod_name, last_run_ts = parent_conn.recv()
                # update last_run_map
                old_last_run_ts = self.last_run_map[mod_name]
                if last_run_ts > old_last_run_ts:
                    self.last_run_map[mod_name] = last_run_ts
        # send message to workers
        for mod_name, attrs in self.mod_config.items():
            run_period = attrs.get('period')
            last_run_ts = self.last_run_map.get(mod_name)
            if run_period is None or last_run_ts is None:
                continue
            if last_run_ts + run_period <= now_ts:
                self.msg_queue.put(mod_name)
        # sleep
        time.sleep(0.5)

    # send stop command to all worker processes
    def _stop_proc(self):
        for parent_conn, child_conn in self.pipe_list:
            parent_conn.send(CMD_STOP)

    # stop master
    def stop(self):
        # stop scheduler from sending more message
        self.to_stop_scheduler = True
        # send stop signal to workers
        self._send_stop()
        # wait a bit
        time.sleep(3)

    # run
    def run(self):
        self.logger.debug('daemon master started')
        # make argument list for workers
        args_list = []
        for parent_conn, child_conn in self.pipe_list:
            args = (self.mod_config, self.msg_queue, child_conn)
            args_list.append(args)
        # start daemon processes
        self.proc_pool.starmap_async(_process_loop, args_list)
        self.proc_pool.close()
        self.logger.debug('daemon master launched all worker processes')
        # loop of scheduler
        while not self.to_stop_scheduler:
            self._scheduler_cycle()
        # end
        self.logger.debug('daemon master ended')


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
    main_log.info('main start')
    # daemon context
    dc = daemon.DaemonContext(  stdout=sys.stdout, stderr=sys.stderr,
                                uid=uid, gid=gid, files_preserve=files_preserve,
                                pidfile=daemon.pidfile.PIDLockFile(options.pid))
    with dc:
        # get logger inside daemon context
        tmp_log = get_logger()
        # record in PID file
        with open(options.pidfile, 'w') as pid_file:
            pid_file.write('{0}'.format(os.getpid()))
        # master object
        master = DaemonMaster(logger=tmp_log)
        # function to end master when end signal caught
        def end_master(sig, frame):
            master.stop()
            kill_whole()
        # set signal handler
        for sig in END_SIGNALS:
            signal.signal(sig, end_master)
        # start master
        master.start()
    # get logger again
    main_log = get_logger()
    main_log.info('main end')


# run
if __name__ == '__main__':
    main()
