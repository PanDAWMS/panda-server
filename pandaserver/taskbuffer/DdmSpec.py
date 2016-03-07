"""
ddm specification

"""

import re

class DdmSpec(object):

    # constructor
    def __init__(self):
        self.all = {}
        self.local = set()
        self.default = None



    # add endpoint
    def add(self,endPoint,endpointDict):
        name = endPoint['ddm_endpoint_name']
        # all endpoints
        self.all[name] = endpointDict[name]
        # local endpoints
        if endPoint['is_local'] != 'N':
            self.local.add(name)
        # default
        if endPoint['is_default'] == 'Y':
            self.default = name



    # get endpoint
    def getEndPoint(self,endpointName):
        if endpointName in self.all:
            return  self.all[endpointName]
        return None



    # get local endpoints
    def getLocalEndPoints(self):
        return tuple(self.local)



    # get default endpoint
    def getDefault(self):
        return self.default


    # check association
    def isAssociated(self,endpointName):
        return endpointName in self.all



    # check local
    def isLocal(self,endpointName):
        return endpointName in self.local



    # get DDM endpoint associated with a pattern
    def getAssoicatedEndpoint(self,patt):
        patt = patt.split('/')[-1]
        if patt in self.all:
            return self.all[patt]
        for endPointName in self.all.keys():
            # ignore TEST or SPECIAL
            if self.all[endPointName]['type'] in ['TEST','SPECIAL']:
                continue
            # check name
            if re.search(patt,endPointName) != None:
                return self.all[endPointName]
            # check type
            pattwoVO = re.sub('ATLAS','',patt)
            if self.all[endPointName]['type'] == pattwoVO:
                return self.all[endPointName]
        return None
