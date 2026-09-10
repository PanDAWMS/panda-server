"""
Dummy plugin of Adder for VOs which don't need DDM access

"""

from typing import Any

from pandaserver.taskbuffer.JobSpec import JobSpec

from .adder_plugin_base import AdderPluginBase


class AdderDummyPlugin(AdderPluginBase):
    """
    Dummy plugin of Adder for VOs which don't need DDM access.
    """

    # constructor
    def __init__(self, job: JobSpec, **params: Any) -> None:
        """
        Initialize the AdderDummyPlugin.

        :param job: The job object.
        :param params: Additional parameters.
        """
        AdderPluginBase.__init__(self, job, params)

    # main
    def execute(self) -> None:
        """
        Execute the dummy adder plugin.

        :return: Status code indicating success.
        """
        self.result.set_succeeded()
        return
