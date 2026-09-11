import json
import sys

from pandajedi.jedicore import JediTaskBuffer
from pandaserver.taskbuffer.Initializer import initializer

jediTaskID = sys.argv[1]

# initialize DB using dummy connection
initializer.init()


taskBuffer = JediTaskBuffer.JediTaskBuffer(None)
proxy = taskBuffer.proxyPool.getProxy()

s, o = proxy.getClobObj("select task_param from atlas_deft.deft_task where task_id=:task_id", {":task_id": jediTaskID})

taskParamStr = o[0][0]

# the task attributes the insert needs are in the parameters themselves, which is where the
# other drivers in this directory read them from
taskParamMap = json.loads(taskParamStr)

proxy.insertTaskParams_JEDI(
    taskParamMap["vo"],
    taskParamMap["prodSourceLabel"],
    taskParamMap["userName"],
    taskParamMap["taskName"],
    taskParamStr,
)
