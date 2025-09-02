from pandacommon.pandalogger import logger_utils

from pandajedi.jediconfig import jedi_config
from pandajedi.jedidaemons.utils import DaemonMaster

base_logger = logger_utils.setup_logger(__name__.split(".")[-1])


# launch
def launcher(taskBufferIF, ddmIF):
    tmp_log = logger_utils.make_logger(base_logger, method_name="launcher")
    tmp_log.debug("start")
    try:
        jedi_config.daemon.config
    except Exception as e:
        tmp_log.error(f"failed to read config json file; should not happen... {e.__class__.__name__}: {e}")
        raise e
    # whether to run daemons
    if not getattr(jedi_config.daemon, "enable", False):
        tmp_log.debug("daemon disabled ; skipped")
        return
    # parameters
    n_workers = getattr(jedi_config.daemon, "n_proc", 1)
    worker_lifetime = getattr(jedi_config.daemon, "proc_lifetime", 28800)
    # start
    agent = DaemonMaster(logger=tmp_log, n_workers=n_workers, worker_lifetime=worker_lifetime, tbuf=taskBufferIF, ddmif=ddmIF)
    agent.run()
