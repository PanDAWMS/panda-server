from AdderResult import AdderResult

class AdderPluginBase(object):
    def __init__(self,job,params):
        self.job = job
        self.logger = None
        self.result = AdderResult()
        for tmpKey in params:
            tmpVal = params[tmpKey]
            setattr(self,tmpKey,tmpVal)
