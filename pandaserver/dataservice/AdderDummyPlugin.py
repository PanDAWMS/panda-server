# dummy plugin of Adder for VOs which don't need DDM access

from .AdderPluginBase import AdderPluginBase


class AdderDummyPlugin(AdderPluginBase):
    
    # constructor
    def __init__(self, job, **params):
        AdderPluginBase.__init__(self, job, params)


    # main
    def execute(self):
        self.result.setSucceeded()
        return
