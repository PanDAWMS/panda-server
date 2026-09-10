from typing import Any, TypeAlias

# The task buffer an async request handler is given. Two unrelated objects arrive here:
# pandaserver.taskbuffer.TaskBuffer from async_request_daemon, and pandajedi's
# JediTaskBufferInterface -- a proxy, not a subclass -- from AsyncRequestWatchDog. No
# nominal type covers both, and naming the JEDI one would make pandaserver import
# pandajedi, so the parameter is left open and the alias says why.
TaskBufferLike: TypeAlias = Any
