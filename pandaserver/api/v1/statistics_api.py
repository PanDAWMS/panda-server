from typing import Any, Dict

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandalogger.PandaLogger import PandaLogger

from pandaserver.api.v1.common import generate_response, request_validation
from pandaserver.srvcore.panda_request import PandaRequest
from pandaserver.taskbuffer.TaskBuffer import TaskBuffer

_logger = PandaLogger().getLogger("api_statistics")

global_task_buffer = None

MAX_TIME_WINDOW = 60 * 24 * 7  # 7 days


def init_task_buffer(task_buffer: TaskBuffer) -> None:
    """
    Initialize the task buffer. This method needs to be called before any other method in this module.
    """
    global global_task_buffer
    global_task_buffer = task_buffer


@request_validation(_logger, secure=False, request_method="GET")
def job_stats_by_cloud(req: PandaRequest, type: str = "production") -> Dict[str, Any]:
    """
    Job statistics by cloud.

    Get the job statistics by cloud, which includes the active jobs and jobs in final states modified in the last 12 hours. You have to filter the statistics by type, which can be either "production" or "analysis". Used by panglia monitoring. Requires a secure connection.

    API details:
        HTTP Method: GET
        Path: /v1/statistics/job_stats_by_cloud

    Args:
        req(PandaRequest): internally generated request object
        type(str): can be "analysis" or "production". Defaults to "production" when not provided.

    Returns:
        dict: The system response `{"success": success, "message": message, "data": data}`. When successful, the data field contains the job statistics by cloud. When unsuccessful, the message field contains the error message.
    """
    tmp_logger = LogWrapper(_logger, f"job_stats_by_cloud < type={type} >")

    tmp_logger.debug("Start")

    if type not in ["production", "analysis"]:
        tmp_logger.error("Invalid type parameter")
        return generate_response(False, 'Parameter "type" needs to be either "production" or "analysis" ', {})

    data = global_task_buffer.getJobStatisticsForExtIF(type)
    success = data != {}
    message = "" if success else "Database failure getting the job statistics"
    tmp_logger.debug(f"Done. {message}")

    return generate_response(success, message, data)


@request_validation(_logger, secure=False, request_method="GET")
def production_job_stats_by_cloud_and_processing_type(req: PandaRequest) -> Dict[str, Any]:
    """
    Production job statistics by cloud and processing type.

    Get the production job statistics by cloud and processing type, which includes the active jobs and jobs in final states modified in the last 12 hours. Used by panglia monitoring. Requires a secure connection.

    API details:
        HTTP Method: GET
        Path: /v1/statistics/production_job_stats_by_cloud_and_processing_type

    Args:
        req(PandaRequest): internally generated request object

    Returns:
        dict: The system response `{"success": success, "message": message, "data": data}`. When successful, the data field contains the job statistics by cloud. When unsuccessful, the message field contains the error message.
    """

    tmp_logger = LogWrapper(_logger, "production_job_stats_by_cloud_and_processing_type")

    tmp_logger.debug("Start")
    data = global_task_buffer.getJobStatisticsForBamboo()
    success = data != {}
    message = "" if success else "Database failure getting the job statistics"
    tmp_logger.debug(f"Done. {message}")

    return generate_response(success, message, data)


@request_validation(_logger, secure=False, request_method="GET")
def active_job_stats_by_site(req: PandaRequest) -> Dict[str, Any]:
    """
    Active job statistics by site

    Get the active (not in a final state) job statistics by site. Used by Harvester. Requires a secure connection.

    API details:
        HTTP Method: GET
        Path: /v1/statistics/active_job_stats_by_site

    Args:
        req(PandaRequest): internally generated request object

    Returns:
        dict: The system response `{"success": success, "message": message, "data": data}`. When successful, the data field contains the job statistics by cloud. When unsuccessful, the message field contains the error message.
    """
    tmp_logger = LogWrapper(_logger, "active_job_stats_by_site")

    tmp_logger.debug("Start")
    data = global_task_buffer.getJobStatistics()
    success = data != {}
    message = "" if success else "Database failure getting the job statistics"
    tmp_logger.debug(f"Done. {message}")

    return generate_response(success, message, data)


@request_validation(_logger, secure=False, request_method="GET")
def job_stats_by_site_and_resource_type(req: PandaRequest, time_window: int = None) -> Dict[str, Any]:
    """
    Job statistics by site and resource type

    Get the job statistics by computing site (PanDA queue) and resource type (SCORE, MCORE, ...). This includes the active jobs and jobs in final states modified in the specified time window (default of 12 hours). Requires a secure connection.

    API details:
        HTTP Method: GET
        Path: /v1/statistics/job_stats_by_site_and_resource_type

    Args:
        req(PandaRequest): internally generated request object
        time_window(int, optional): time window in minutes for the statistics (affects only archived jobs)

    Returns:
        dict: The system response `{"success": success, "message": message, "data": data}`. When successful, the data field contains the job statistics by cloud. When unsuccessful, the message field contains the error message.
    """
    tmp_logger = LogWrapper(_logger, f"job_stats_by_site_and_resource_type < time_window={time_window} >")

    tmp_logger.debug("Start")
    if time_window is not None and (not isinstance(time_window, int) or time_window < 0 or time_window > MAX_TIME_WINDOW):
        tmp_logger.error("Invalid time_window parameter")
        return generate_response(False, 'Parameter "time_window" needs to be a positive integer smaller than 10080 min (7 days)', {})

    data = global_task_buffer.getJobStatisticsPerSiteResource(time_window)
    success = data != {}
    message = "" if success else "Database failure getting the job statistics"
    tmp_logger.debug(f"Done. {message}")

    return generate_response(success, message, data)


@request_validation(_logger, secure=False, request_method="GET")
def job_stats_by_site_share_and_resource_type(req: PandaRequest, time_window: int = None) -> Dict[str, Any]:
    """
    Job statistics by site, global share and resource type

    Get the job statistics by computing site (PanDA queue), global share and resource type (SCORE, MCORE, ...). This includes the active jobs and jobs in final states modified in the specified time window (default of 12 hours). Requires a secure connection.

    API details:
        HTTP Method: GET
        Path: /v1/statistics/retry

    Args:
        req(PandaRequest): internally generated request object
        time_window(int): time window in minutes for the statistics (affects only archived jobs)

    Returns:
        dict: The system response `{"success": success, "message": message, "data": data}`. When successful, the data field contains the job statistics by cloud. When unsuccessful, the message field contains the error message.
    """
    tmp_logger = LogWrapper(_logger, f"job_stats_by_site_share_and_resource_type < time_window={time_window} >")

    tmp_logger.debug("Start")
    if time_window is not None and (not isinstance(time_window, int) or time_window < 0 or time_window > MAX_TIME_WINDOW):
        tmp_logger.error("Invalid time_window parameter")
        return generate_response(False, 'Parameter "time_window" needs to be a positive integer smaller than 10080 min (7 days)', {})

    data = global_task_buffer.get_job_statistics_per_site_label_resource(time_window)
    success = data != {}
    message = "" if success else "Database failure getting the job statistics"
    tmp_logger.debug(f"Done. {message}")

    return generate_response(success, message, data)
