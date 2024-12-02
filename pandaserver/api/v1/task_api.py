from pandacommon.pandalogger.PandaLogger import PandaLogger

from pandaserver.api.v1.common import MESSAGE_DATABASE, get_dn, request_validation
from pandaserver.taskbuffer.TaskBuffer import TaskBuffer

_logger = PandaLogger().getLogger("task_api")

global_task_buffer = None


def init_task_buffer(task_buffer: TaskBuffer) -> None:
    """
    Initialize the task buffer. This method needs to be called before any other method in this module.
    """
    global global_task_buffer
    global_task_buffer = task_buffer
