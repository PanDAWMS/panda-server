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


# passphrase to send in queue to stop daemon worker processes
STOP_PASSPHRASE = '__STOP_PROCESS__'


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
def _process_loop(module_list, msg_queue):
    # logger
    tmp_log = get_logger()
    # pid of the worker
    my_pid = os.getpid()
    # dict of all script modules
    module_map = {}
    # import all modules
    for module_name in module_list:
        try:
            module_map[module_name] = importlib.import_module(module_name)
        except Exception as e:
            tmp_log.warning('<worker_pid={pid}> failed to import {mod} with {err} ; skipped it'.format(
                                pid=my_pid, mod=module_name, err='{0}: {1}'.format(e.__class__.__name__, e)))
    tmp_log.debug('<worker_pid={pid}> initialized, running'.format(pid=my_pid))
    # loop
    while True:
        # get a message from queue
        one_msg = msg_queue.get()
        # process message
        if one_msg == STOP_PASSPHRASE:
            # got stop passphrase, stop the process
            tmp_log.debug('<worker_pid={pid}> got stop signal, stop this process'.format(pid=my_pid))
            break
        elif one_msg in module_map:
            # got a module name, go run the module
            the_module = module_map[one_msg]
            # execute the module script
            try:
                tmp_log.debug('<worker_pid={pid}> start running module {mod}'.format(pid=my_pid, mod=one_msg))
                the_module.main()
                tmp_log.debug('<worker_pid={pid}> finish running module {mod}'.format(pid=my_pid, mod=one_msg))
            except Exception as e:
                tb = traceback.format_exc()
                tmp_log.error('<worker_pid={pid}> failed to run module {mod} with {err} ; skipped it'.format(
                                pid=my_pid, mod=module_name, err='{0}: {1}\n{2}\n'.format(e.__class__.__name__, e, tb)))
        else:
            # got invalid message
            tmp_log.error('<worker_pid={pid}> got invalid message "{msg}", skipped it'.format(pid=my_pid, msg=one_msg))
        # sleep
        time.sleep(2**-5)


# master class of main daemon process for PanDA server
class DaemonMaster(object):

    # constructor
    def __init__(self, logger):
        # logger
        self.logger = logger
        # make message queue
        self.msg_queue = multiprocessing.SimpleQueue()
        # make process pool
        self.proc_pool = multiprocessing.Pool(processes=4)
        # whether to stop scheduler
        self.to_stop_scheduler = False
        # make module config
        self.mod_config = {}
        self._parse_config()
        # map of timestamp of last time the module run
        self.last_run_map = { mod: 0 for mod in self.mod_config }

    # parse daemon config
    def _parse_config(self):
        try:
            config_json = daemon_config.config
            config_dict = json.loads(config_json)
            self.mod_config = copy.deepcopy(config_dict)
            # remove disabled modules
            for mod, attrs in config_dict.items():
                if 'enable' in attrs and attrs['enable'] is False:
                    del self.mod_config[mod]
        except Exception as e:
            tb = traceback.format_exc()
            self.logger.error('failed to parse daemon config, {err}'.format(
                                err='{0}: {1}\n{2}\n'.format(e.__class__.__name__, e, tb)))

    # one scheduler cycle
    def _scheduler_cycle(self):
        now_ts = int(time.time())
        for x in self.mod_config:
            if last_run_ts + run_period <= now_ts:
                self.msg_queue.put(module_name)
        # sleep
        time.sleep(0.5)

    # send stop signal to processes
    def _stop_proc(self):
        for i in range(100):
            self.msg_queue.put(STOP_PASSPHRASE)

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
        # start daemon processes
        self.proc_pool.map_async()
        self.proc_pool.close()
        self.logger.debug('daemon master launched all worker processes')
        # loop
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
