"""
The Adder module is the core for add_main to post-process jobs’ output data, such as data registration, trigger data aggregation and so on.
Those post-processing procedures are experiment-dependent so that the Adder also has a plugin structure to load an experiment-specific plugin.

"""

from typing import TYPE_CHECKING, Any

from pandaserver.taskbuffer.JobSpec import JobSpec

from .adder_result import AdderResult

if TYPE_CHECKING:
    from pandacommon.pandalogger.LogWrapper import LogWrapper

    from pandaserver.brokerage.SiteMapper import SiteMapper
    from pandaserver.taskbuffer.TaskBuffer import TaskBuffer


class AdderPluginBase:
    """
    Base class for Adder plugins.
    """

    # Every caller passes logger= in params, and the loop below installs it, so the
    # placeholder assigned in __init__ never survives construction. Declared
    # non-Optional because a plugin method running without a logger is a bug either
    # way, and an Optional type would only push a None check onto every log call.
    logger: "LogWrapper"
    # Installed by the loop below from the params. AdderGen always passes both, and the
    # site mapper it forwards is the one add_main builds at startup, so neither is None
    # by the time a plugin method runs.
    taskBuffer: "TaskBuffer"
    siteMapper: "SiteMapper"

    def __init__(self, job: JobSpec, params: dict[str, Any]) -> None:
        """
        Initialize the AdderPluginBase.

        :param job: The job object.
        :param params: Additional parameters.
        """
        self.job = job
        self.logger = None  # type: ignore[assignment]
        self.result = AdderResult()
        self.extra_info: dict[str, Any] = {}
        for key, value in params.items():
            setattr(self, key, value)
