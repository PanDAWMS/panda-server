import sys
import time

from pandacommon.pandalogger import logger_utils
from pandacommon.pandautils.thread_utils import GenericThread
from pandaserver.config import panda_config
from pandaserver.configurator import Configurator as configurator_module
from pandaserver.configurator.Configurator import (
    Configurator,
    NetworkConfigurator,
    SchedconfigJsonDumper,
    SWTagsDumper,
)

# logger
base_logger = configurator_module._logger


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
        _logger = logger_utils.make_logger(base_logger, "Configurator")
        t1 = time.time()
        configurator = Configurator(taskBuffer=taskBuffer, log_stream=_logger)

        if not configurator.retrieve_data():
            _logger.error("Data was not retrieved correctly")
            return

        if not configurator.run():
            _logger.error("Configurator loop FAILED")
        t2 = time.time()
        _logger.debug(f"Configurator run took {t2 - t1}s")

    # If --network argument, call the network configurator
    elif len(argv) == 2 and argv[1].lower() == "--network":
        _logger = logger_utils.make_logger(base_logger, "NetworkConfigurator")
        t1 = time.time()
        network_configurator = NetworkConfigurator(taskBuffer=taskBuffer, log_stream=_logger)
        if not network_configurator.retrieve_data():
            _logger.error("Data was not retrieved correctly")
            return
        if not network_configurator.run():
            _logger.error("Configurator loop FAILED")
        t2 = time.time()
        _logger.debug(f" run took {t2 - t1}s")

    # If --json_dump
    elif len(argv) == 2 and argv[1].lower() == "--json_dump":
        _logger = logger_utils.make_logger(base_logger, "SchedconfigJsonDumper")
        t1 = time.time()
        json_dumper = SchedconfigJsonDumper(taskBuffer=taskBuffer, log_stream=_logger)
        out_msg = json_dumper.run()
        _logger.debug(f"Json_dumper finished with {out_msg}")
        t2 = time.time()
        _logger.debug(f" run took {t2 - t1}s")

    # If --sw_tags
    elif len(argv) == 2 and argv[1].lower() == "--sw_tags":
        _logger = logger_utils.make_logger(base_logger, "SWTagsDumper")
        t1 = time.time()
        sw_tag_collector = SWTagsDumper(taskBuffer=taskBuffer, log_stream=_logger)
        out_msg = sw_tag_collector.run()
        _logger.debug(f"sw_tag_collector finished with {out_msg}")
        t2 = time.time()
        _logger.debug(f" run took {t2 - t1}s")

    else:
        base_logger.error("Configurator being called with wrong arguments. Use either no arguments or --network or --json_dump")

    # stop taskBuffer if created inside this script
    if tbuf is None:
        taskBuffer.cleanup(requester=requester_id)


if __name__ == "__main__":
    main(argv=sys.argv)
