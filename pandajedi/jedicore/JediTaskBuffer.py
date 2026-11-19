# The task buffer of a JEDI process

from typing import Any

from pandacommon.pandalogger.PandaLogger import PandaLogger

from pandajedi.jediconfig import jedi_config
from pandaserver.taskbuffer import TaskBuffer

from . import JediDBProxyPool
from .Interaction import CommandReceiveInterface

logger = PandaLogger().getLogger(__name__.split(".")[-1])

# use customized proxy pool. The module attribute is deliberately rebound to the JEDI
# subclass, which is what TaskBuffer.init() then instantiates
TaskBuffer.DBProxyPool = JediDBProxyPool.DBProxyPool  # type: ignore[misc,assignment]


class JediTaskBuffer(TaskBuffer.TaskBuffer, CommandReceiveInterface):
    """
    TaskBuffer as a JEDI process runs it

    The API is TaskBuffer's. What this class adds is how JEDI runs it: it answers the commands
    that JediTaskBufferInterface sends over a pipe (CommandReceiveInterface), and it connects
    with the JEDI database settings through JEDI's proxy pool (rebound above).
    """

    # constructor
    def __init__(self, conn: Any, nDBConnection: int = 1) -> None:
        CommandReceiveInterface.__init__(self, conn)
        TaskBuffer.TaskBuffer.__init__(self)
        TaskBuffer.TaskBuffer.init(self, jedi_config.db.dbhost, jedi_config.db.dbpasswd, nDBConnection=nDBConnection)
        logger.debug("__init__")
