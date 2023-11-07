import argparse
import grp
import logging
import os
import pwd
import signal
import sys

import daemon
import lockfile
from pandaserver.config import daemon_config, panda_config
from pandaserver.daemons.utils import END_SIGNALS, DaemonMaster


# get the logger
def get_logger():
    my_logger = logging.getLogger("PanDA-Daemon-Master")
    # remove existing handlers
    while my_logger.hasHandlers():
        my_logger.removeHandler(my_logger.handlers[0])
    # make new handler
    _log_handler = logging.StreamHandler(sys.stdout)
    _log_formatter = logging.Formatter("%(asctime)s %(name)-12s: %(levelname)-8s %(message)s")
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


# main function
def main():
    # whether to run daemons
    if not getattr(daemon_config, "enable", False):
        return

    # get logger
    main_log = get_logger()

    # parse option
    parser = argparse.ArgumentParser()

    if "PANDA_NO_ROOT" in os.environ:
        uid = None
        gid = None
    else:
        uname = getattr(daemon_config, "uname", "nobody")
        gname = getattr(daemon_config, "gname", "nobody")
        uid = pwd.getpwnam(uname).pw_uid
        gid = grp.getgrnam(gname).gr_gid

    n_workers = getattr(daemon_config, "n_proc", 1)
    n_dbconn = getattr(daemon_config, "n_dbconn", 1)
    worker_lifetime = getattr(daemon_config, "proc_lifetime", 28800)
    use_tbif = getattr(daemon_config, "use_tbif", False)

    main_log.info("main start")

    # get logger inside daemon context
    tmp_log = get_logger()

    # master object
    master = DaemonMaster(
        logger=tmp_log,
        n_workers=n_workers,
        n_dbconn=n_dbconn,
        worker_lifetime=worker_lifetime,
        use_tbif=use_tbif,
    )

    # function to end master when end signal caught
    def end_master(sig, frame):
        tmp_log.info(f"got end signal: {sig}")
        master.stop()
        kill_whole()

    # set signal handler
    for sig in END_SIGNALS:
        signal.signal(sig, end_master)

    # start master
    master.run()

    main_log.info("main end")


# run
if __name__ == "__main__":
    main()
