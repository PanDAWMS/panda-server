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
        # local endpoints # FIXME once is_local becomes reliable
        if True: # endPoint['is_local'] == 'Y':
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
