import uuid

from pandaserver.dataservice.SetupperPluginBase import SetupperPluginBase


class SetupperDummyPlugin(SetupperPluginBase):
    # constructor
    def __init__(self, taskBuffer, jobs, logger, **params):
        # defaults
        defaultMap = {}
        SetupperPluginBase.__init__(self, taskBuffer, jobs, logger, params, defaultMap)

    # main
    def run(self):
        for jobSpec in self.jobs:
            for fileSpec in jobSpec.Files:
                if fileSpec.type == "log":
                    # generate GUID
                    fileSpec.GUID = str(uuid.uuid4())

    # post run
    def postRun(self):
        pass
