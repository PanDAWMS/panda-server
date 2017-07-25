"""
ddm specification

"""

import re

class DdmSpec(object):

    # constructor
    def __init__(self):
        self.all = {}
        self.local = set()
        self.default_read = None
        self.default_write = None
        self.tape = set()



    # add endpoint
    def add(self, endPoint, endpointDict):
        name = endPoint['ddm_endpoint_name']
        # all endpoints
        self.all[name] = endpointDict[name]
        # local endpoints
        if endPoint['is_local'] != 'N':
            self.local.add(name)
        # defaults
        if endPoint['default_read'] == 'Y':
            self.default_read = name
        if endPoint['default_write'] == 'Y':
            self.default_write = name
        # tape
        if endPoint['is_tape'] == 'Y':
            self.tape.add(name)


    # get all endpoints
    def getAllEndPoints(self):
        return self.all.keys()



    # get endpoint
    def getEndPoint(self,endpointName):
        if endpointName in self.all:
            return self.all[endpointName]
        return None



    # get local endpoints
    def getLocalEndPoints(self):
        tmpRet = list(self.local)
        tmpRet.sort()
        return tmpRet



    # get default write endpoint
    def getDefaultWrite(self):
        return self.default_write

    # get default read endpoint
    def getDefaultRead(self):
        return self.default_read

    # get tape endpoints
    def getTapeEndPoints(self):
        return tuple(self.tape)


    # check association
    def isAssociated(self,endpointName):
        return endpointName in self.all



    # check local
    def isLocal(self,endpointName):
        return endpointName in self.local



    # get DDM endpoint associated with a pattern
    def getAssociatedEndpoint(self,patt):
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


    # get mapping between tokens and endpoint names
    def getTokenMap(self, mode):
        # TODO: review this part and discuss with Tadashi
        retMap = {}
        for tmpName, tmpVal in self.all.iteritems():
            token = tmpVal['ddm_spacetoken_name']
            # already exists
            if token in retMap:
                # use default
                if retMap[token] == self.default_read and mode=='input':
                    continue
                if retMap[token] == self.default_write and mode=='output':
                    continue
                # use local
                if retMap[token] in self.local:
                    continue
                # use first remote
                if tmpName not in self.local:
                    continue
            # add
            retMap[token] = tmpName
        return retMap

