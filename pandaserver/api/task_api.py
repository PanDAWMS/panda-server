import datetime
from typing import List, Tuple

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandalogger.PandaLogger import PandaLogger

from pandaserver.api.common import MESSAGE_DATABASE, get_dn, request_validation
from pandaserver.api.timed_method import TIME_OUT, TimedMethod
from pandaserver.srvcore.panda_request import PandaRequest
from pandaserver.taskbuffer.TaskBuffer import TaskBuffer

_logger = PandaLogger().getLogger("task_api")

global_task_buffer = None


def init_task_buffer(task_buffer: TaskBuffer) -> None:
    """
    Initialize the task buffer. This method needs to be called before any other method in this module.
    """
    global global_task_buffer
    global_task_buffer = task_buffer


@request_validation(_logger, secure=True)
def retry_task(
    req,
    jedi_task_id,
    new_parameters=None,
    no_child_retry=None,
    discard_events=None,
    disable_staging_mode=None,
    keep_gshare_priority=None,
):
    no_child_retry = resolve_true(no_child_retry)
    discard_events = resolve_true(discard_events)
    disable_staging_mode = resolve_true(disable_staging_mode)
    keep_gshare_priority = resolve_true(keep_gshare_priority)

    user = _getDN(req)
    is_production_role = _has_production_role(req)

    # retry with new params
    if new_parameters:
        try:
            # convert to dict
            newParams = PrioUtil.decodeJSON(newParams)
            # get original params
            taskParams = self.taskBuffer.getTaskParamsPanda(jediTaskID)
            taskParamsJson = PrioUtil.decodeJSON(taskParams)
            # replace with new values
            for newKey in newParams:
                newVal = newParams[newKey]
                taskParamsJson[newKey] = newVal
            taskParams = json.dumps(taskParamsJson)
            # retry with new params
            ret = self.taskBuffer.insertTaskParamsPanda(
                taskParams,
                user,
                prodRole,
                [],
                properErrorCode=properErrorCode,
                allowActiveTask=True,
            )
        except Exception:
            err_type, err_value = sys.exc_info()[:2]
            ret = 1, f"server error with {err_type}:{err_value}"
    else:
        com_qualifier = ""
        for com_key, com_param in [
            ("sole", noChildRetry),
            ("discard", discardEvents),
            ("staged", disable_staging_mode),
            ("keep", keep_gshare_priority),
        ]:
            if com_param:
                com_qualifier += f"{com_key} "
        com_qualifier = com_qualifier.strip()
        # normal retry
        ret = self.taskBuffer.sendCommandTaskPanda(
            jediTaskID,
            user,
            prodRole,
            "retry",
            properErrorCode=properErrorCode,
            comQualifier=com_qualifier,
        )
    if properErrorCode is True and ret[0] == 5:
        # retry failed analysis jobs
        jobdefList = self.taskBuffer.getJobdefIDsForFailedJob(jediTaskID)
        cUID = self.taskBuffer.cleanUserID(user)
        for jobID in jobdefList:
            self.taskBuffer.finalizePendingJobs(cUID, jobID)
        self.taskBuffer.increaseAttemptNrPanda(jediTaskID, 5)
        return_str = f"retry has been triggered for failed jobs while the task is still {ret[1]}"
        if newParams is None:
            ret = 0, return_str
        else:
            ret = 3, return_str
    return ret
    return WrappedPickle.dumps(ret)
