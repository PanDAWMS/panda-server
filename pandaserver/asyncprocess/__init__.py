from typing import TYPE_CHECKING, TypeAlias

if TYPE_CHECKING:
    from pandaserver.taskbuffer.TaskBuffer import TaskBuffer

# The task buffer an async request handler is given. Two objects arrive here:
# pandaserver.taskbuffer.TaskBuffer from async_request_daemon, and pandajedi's
# JediTaskBufferInterface -- a proxy, not a subclass -- from AsyncRequestWatchDog. The proxy's
# base is JediTaskBuffer to a reader of the source and Any to mypy, so it is accepted where a
# TaskBuffer is declared, and TaskBuffer covers both without pandaserver naming pandajedi.
TaskBufferLike: TypeAlias = "TaskBuffer"
