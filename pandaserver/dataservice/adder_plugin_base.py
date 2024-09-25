"""
The Adder module is the core for add_main to post-process jobs’ output data, such as data registration, trigger data aggregation and so on.
Those post-processing procedures are experiment-dependent so that the Adder also has a plugin structure to load an experiment-specific plugin.

"""

from .adder_result import AdderResult

class AdderPluginBase:
    """
    Base class for Adder plugins.
    """
    def __init__(self, job, params):
        """
        Initialize the AdderPluginBase.

        :param job: The job object.
        :param params: Additional parameters.
        """
        self.job = job
        self.logger = None
        self.result = AdderResult()
        for key, value in params.items():
            setattr(self, key, value)
