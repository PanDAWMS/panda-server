from AdderResult import AdderResult

class AdderPluginBase(object):
    def __init__(self,job,params):
        self.job = job
        self.result = AdderResult()
        for tmpKey,tmpVal in params.iteritems():
            setattr(self,tmpKey,tmpVal)
