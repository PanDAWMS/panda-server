import sys

from taskbuffer.Initializer import initializer

from pandajedi.jedicore import JediTaskBuffer

jediTaskID = sys.argv[1]

# initialize DB using dummy connection
initializer.init()


taskBuffer = JediTaskBuffer.JediTaskBuffer(None)
proxy = taskBuffer.proxyPool.getProxy()

s, o = proxy.getClobObj("select task_param from atlas_deft.deft_task where task_id=:task_id", {":task_id": jediTaskID})

taskParamStr = o[0][0]

proxy.insertTaskParams_JEDI(None, taskParamStr)
