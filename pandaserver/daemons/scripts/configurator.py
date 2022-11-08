import sys
import time

from pandaserver.config import panda_config
from pandacommon.pandalogger import logger_utils
from pandaserver.configurator import Configurator as configurator_module
from pandaserver.configurator.Configurator import Configurator, NetworkConfigurator, SchedconfigJsonDumper, SWTagsDumper

# logger
base_logger = configurator_module._logger


# main
def main(argv=tuple(), tbuf=None, **kwargs):
    # instantiate TB
    if tbuf is None:
        from pandaserver.taskbuffer.TaskBuffer import taskBuffer
        taskBuffer.init(panda_config.dbhost, panda_config.dbpasswd, nDBConnection=1, useTimeout=True)
    else:
        taskBuffer = tbuf

    # If no argument, call the basic configurator
    if len(argv) == 1:
        _logger = logger_utils.make_logger(base_logger, 'Configurator')
        t1 = time.time()
        configurator = Configurator(taskBuffer=taskBuffer)
        if not configurator.run():
            _logger.critical('Configurator loop FAILED')
        t2 = time.time()
        _logger.debug('Configurator run took {0}s'.format(t2-t1))

    # If --network argument, call the network configurator
    elif len(argv) == 2 and argv[1].lower() == '--network':
        _logger = logger_utils.make_logger(base_logger, 'NetworkConfigurator')
        t1 = time.time()
        network_configurator = NetworkConfigurator(taskBuffer=taskBuffer)
        if not network_configurator.run():
            _logger.critical('Configurator loop FAILED')
        t2 = time.time()
        _logger.debug(' run took {0}s'.format(t2-t1))

    # If --json_dump
    elif len(argv) == 2 and argv[1].lower() == '--json_dump':
        _logger = logger_utils.make_logger(base_logger, 'SchedconfigJsonDumper')
        t1 = time.time()
        json_dumper = SchedconfigJsonDumper(taskBuffer=taskBuffer)
        out_msg = json_dumper.run()
        _logger.debug('Json_dumper finished with {0}'.format(out_msg))
        t2 = time.time()
        _logger.debug(' run took {0}s'.format(t2-t1))

    # If --sw_tags
    elif len(argv) == 2 and argv[1].lower() == '--sw_tags':
        _logger = logger_utils.make_logger(base_logger, 'SWTagsDumper')
        t1 = time.time()
        sw_tag_collector = SWTagsDumper(taskBuffer=taskBuffer)
        out_msg = sw_tag_collector.run()
        _logger.debug('sw_tag_collector finished with {0}'.format(out_msg))
        t2 = time.time()
        _logger.debug(' run took {0}s'.format(t2-t1))

    else:
        base_logger.error('Configurator being called with wrong arguments. Use either no arguments or --network or --json_dump')


if __name__ == '__main__':
    main(argv=sys.argv)
