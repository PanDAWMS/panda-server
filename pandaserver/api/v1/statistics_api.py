import datetime
import json
import re
from typing import Any, Dict, List

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandalogger.PandaLogger import PandaLogger

from pandaserver.api.v1.common import generate_response, request_validation
from pandaserver.srvcore.panda_request import PandaRequest
from pandaserver.taskbuffer.TaskBuffer import TaskBuffer

_logger = PandaLogger().getLogger("statistics_api")

global_task_buffer = None


def init_task_buffer(task_buffer: TaskBuffer) -> None:
    """
    Initialize the task buffer. This method needs to be called before any other method in this module.
    """
    global global_task_buffer
    global_task_buffer = task_buffer


@request_validation(_logger, secure=False, request_method="GET")
def get_job_stats(req: PandaRequest, sourcetype: str = None) -> Dict[str, Any]:
    """
    Task retry

    Retry a given task. Requires a secure connection without a production role to retry own tasks and with a production role to retry others' tasks.

    API details:
        HTTP Method: GET
        Path: /task/v1/retry

    Args:
        req(PandaRequest): internally generated request object
        jedi_task_id(int): JEDI Task ID

    Returns:
        dict: The system response. True for success, False for failure, and an error message. Return code in the data field, 0 for success, others for failure.
    """
    ret = global_task_buffer.getJobStatisticsForExtIF(sourcetype)
    return WrappedPickle.dumps(ret)


@request_validation(_logger, secure=False, request_method="GET")
def getJobStatisticsForBamboo(req: PandaRequest, useMorePG: str = None) -> Dict[str, Any]:
    """
    Task retry

    Retry a given task. Requires a secure connection without a production role to retry own tasks and with a production role to retry others' tasks.

    API details:
        HTTP Method: GET
        Path: /task/v1/retry

    Args:
        req(PandaRequest): internally generated request object
        jedi_task_id(int): JEDI Task ID

    Returns:
        dict: The system response. True for success, False for failure, and an error message. Return code in the data field, 0 for success, others for failure.
    """
    if useMorePG == "True":
        useMorePG = pandaserver.taskbuffer.ProcessGroups.extensionLevel_1
    elif useMorePG in ["False", None]:
        useMorePG = False
    else:
        try:
            useMorePG = int(useMorePG)
        except Exception:
            useMorePG = False

    ret = global_task_buffer.getJobStatisticsForBamboo(useMorePG)
    return generate_response(success, message, data)


# get job statistics per site and resource
@request_validation(_logger, secure=False, request_method="GET")
def getJobStatisticsPerSiteResource(req: PandaRequest, time_window: int = None) -> Dict[str, Any]:
    """
    Task retry

    Retry a given task. Requires a secure connection without a production role to retry own tasks and with a production role to retry others' tasks.

    API details:
        HTTP Method: GET
        Path: /task/v1/retry

    Args:
        req(PandaRequest): internally generated request object
        jedi_task_id(int): JEDI Task ID

    Returns:
        dict: The system response. True for success, False for failure, and an error message. Return code in the data field, 0 for success, others for failure.
    """
    ret = global_task_buffer.getJobStatisticsPerSiteResource(time_window)
    return generate_response(success, message, data)


# get job statistics per site and resource
@request_validation(_logger, secure=False, request_method="GET")
def get_job_statistics_per_site_label_resource(req: PandaRequest, time_window: int = None) -> Dict[str, Any]:
    """
    Task retry

    Retry a given task. Requires a secure connection without a production role to retry own tasks and with a production role to retry others' tasks.

    API details:
        HTTP Method: GET
        Path: /task/v1/retry

    Args:
        req(PandaRequest): internally generated request object
        jedi_task_id(int): JEDI Task ID

    Returns:
        dict: The system response. True for success, False for failure, and an error message. Return code in the data field, 0 for success, others for failure.
    """
    ret = global_task_buffer.get_job_statistics_per_site_label_resource(time_window)
    return generate_response(success, message, data)


# get job statistics per site
@request_validation(_logger, secure=False, request_method="GET")
def getJobStatisticsPerSite(
    req: PandaRequest,
    predefined: bool = "False",
    workingGroup: str = "",
    countryGroup: str = "",
    jobType: str = "",
    minPriority: int = None,
    readArchived: bool = None,
) -> Dict[str, Any]:
    """
    Task retry

    Retry a given task. Requires a secure connection without a production role to retry own tasks and with a production role to retry others' tasks.

    API details:
        HTTP Method: GET
        Path: /task/v1/retry

    Args:
        req(PandaRequest): internally generated request object
        jedi_task_id(int): JEDI Task ID

    Returns:
        dict: The system response. True for success, False for failure, and an error message. Return code in the data field, 0 for success, others for failure.
    """
    predefined = resolve_true(predefined)

    if minPriority is not None:
        try:
            minPriority = int(minPriority)
        except Exception:
            minPriority = None

    if readArchived == "True":
        readArchived = True
    elif readArchived == "False":
        readArchived = False
    else:
        host = req.get_remote_host()
        # read jobsArchived for panglia
        if re.search("panglia.*\.triumf\.ca$", host) is not None or host in ["gridweb.triumf.ca"]:
            readArchived = True
        else:
            readArchived = False

    # get job statistics
    ret = global_task_buffer.getJobStatistics(
        readArchived,
        predefined,
        workingGroup,
        countryGroup,
        jobType,
        minPriority=minPriority,
    )
    return generate_response(success, message, data)
