"""
Daemon wrapper for async request processing on panda-server machines (service_name="server").
For JEDI machines, use pandajedi/jedidog/AsyncRequestWatchDog.py which calls the same processor.
"""

import sys

from pandacommon.pandautils.thread_utils import GenericThread

from pandaserver.asyncprocess import processor
from pandaserver.config import panda_config


def main(argv=tuple(), tbuf=None, **kwargs):
    requester_id = GenericThread().get_full_id(__name__, sys.modules[__name__].__file__)

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

    processor.run(service_name="server", tbuf=taskBuffer)

    if tbuf is None:
        taskBuffer.cleanup(requester=requester_id)


if __name__ == "__main__":
    main(argv=sys.argv)
