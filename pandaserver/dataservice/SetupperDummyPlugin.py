from pandaserver.dataservice.SetupperPluginBase import SetupperPluginBase


class SetupperDummyPlugin (SetupperPluginBase):
    # constructor
    def __init__(self,taskBuffer,jobs,logger,**params):
        # defaults
        defaultMap = {}
        SetupperPluginBase.__init__(self,taskBuffer,jobs,logger,params,defaultMap)


    # main
    def run(self):
        pass


    # post run
    def postRun(self):
        pass
