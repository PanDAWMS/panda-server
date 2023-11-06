import sys
import time

from pandacommon.pandalogger import logger_utils
from pandacommon.pandautils.thread_utils import GenericThread
from pandaserver.config import panda_config
from pandaserver.configurator import Carbon as carbon_module
from pandaserver.configurator.Carbon import CarbonEmissions

# logger
base_logger = carbon_module._logger


# main
def main(argv=tuple(), tbuf=None, **kwargs):
    requester_id = GenericThread().get_full_id(__name__, sys.modules[__name__].__file__)

    # instantiate TB
    if tbuf is None:
        from pandaserver.taskbuffer.TaskBuffer import taskBuffer

        taskBuffer.init(
            panda_config.dbhost,
            panda_config.dbpasswd,
            nDBConnection=1,
            useTimeout=True,
            requester=requester_id,
        )
    else:
        taskBuffer = tbuf

    # If no argument, call the basic configurator
    if len(argv) == 1:
        _logger = logger_utils.make_logger(base_logger, "Carbon")
        t1 = time.time()
        carbon_emissions = CarbonEmissions(taskBuffer=taskBuffer)
        carbon_emissions.run()
        t2 = time.time()
        _logger.debug(f"Carbon run took {t2 - t1}s")

    else:
        base_logger.error("Carbon module being called with wrong arguments. Use no arguments")

    # stop taskBuffer if created inside this script
    if tbuf is None:
        taskBuffer.cleanup(requester=requester_id)


if __name__ == "__main__":
    main(argv=sys.argv)
