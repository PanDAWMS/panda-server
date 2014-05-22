class SetupperPluginBase(object):
    def __init__(self,taskBuffer,jobs,logger,params,defaultMap):
        self.jobs = jobs
        self.taskBuffer = taskBuffer
        self.logger = logger
        # set named parameters
        for tmpKey,tmpVal in params.iteritems():
            setattr(self,tmpKey,tmpVal)
        # set defaults
        for tmpKey,tmpVal in defaultMap.iteritems():
            if not hasattr(self,tmpKey):
                setattr(self,tmpKey,tmpVal)


    # abstracts
    def run(self):
        pass
    def postRun(self):
        pass
