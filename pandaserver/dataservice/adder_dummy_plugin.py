"""
Dummy plugin of Adder for VOs which don't need DDM access

"""

from .adder_plugin_base import AdderPluginBase


class AdderDummyPlugin(AdderPluginBase):
    """
    Dummy plugin of Adder for VOs which don't need DDM access.
    """
    # constructor
    def __init__(self, job, **params):
        """
        Initialize the AdderDummyPlugin.

        :param job: The job object.
        :param params: Additional parameters.
        """
        AdderPluginBase.__init__(self, job, params)

    # main
    def execute(self):
        """
        Execute the dummy adder plugin.

        :return: Status code indicating success.
        """
        self.result.set_succeeded()
        return
