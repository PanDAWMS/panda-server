from typing import TYPE_CHECKING

from pandacommon.pandalogger import logger_utils

from pandajedi.jediconfig import jedi_config

# pandajedi/jedidaemons was not brought over when jedi was copied into this repo, so this
# module does not exist here and importing it raises. It is not a wrong path for
# pandaserver.daemons.utils.DaemonMaster either: that one takes no tbuf or ddmif, which is
# what launcher() below passes. Reaching this needs a [daemon] section with enable set, and
# panda_jedi.cfg.rpmnew.template has no [daemon] section at all, so nothing does today --
# but JediMaster launches this knight when one appears and SIGKILLs the whole process group
# when a knight dies in initialization. The ignore keeps mypy able to report the next
# first-party path that names a module which is not there.
from pandajedi.jedidaemons.utils import DaemonMaster  # type: ignore[import-not-found]

if TYPE_CHECKING:
    from pandajedi.jedicore.JediTaskBufferInterface import JediTaskBufferInterface
    from pandajedi.jediddm.DDMInterface import DDMInterface

base_logger = logger_utils.setup_logger(__name__.split(".")[-1])


# launch
def launcher(taskBufferIF: "JediTaskBufferInterface", ddmIF: "DDMInterface") -> None:
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
