"""
This class is a dummy plugin. It inherits from the SetupperPluginBase class.
"""
import uuid

from typing import List, Dict

from pandaserver.dataservice.setupper_plugin_base import SetupperPluginBase


class SetupperDummyPlugin(SetupperPluginBase):
    """
    This class is a dummy plugin. It inherits from the SetupperPluginBase class.
    """

    # constructor
    def __init__(self, taskBuffer, jobs: List, logger, **params: Dict) -> None:
        """
        Constructor for the SetupperDummyPlugin class.

        :param taskBuffer: The buffer for tasks.
        :param jobs: The jobs to be processed.
        :param logger: The logger to be used for logging.
        :param params: Additional parameters.
        """
        # defaults
        default_map = {}
        SetupperPluginBase.__init__(self, taskBuffer, jobs, logger, params, default_map)

    # main
    def run(self) -> None:
        """
        The main method that runs the plugin. It iterates over the jobs and their files.
        If a file is of type "log", it generates a GUID for it.
        """
        for job_spec in self.jobs:
            for file_spec in job_spec.Files:
                if file_spec.type == "log":
                    # generate GUID
                    file_spec.GUID = str(uuid.uuid4())

    # post run
    def post_run(self) -> None:
        """
        This method is called after the run method. Currently, it does nothing.
        """
        pass
