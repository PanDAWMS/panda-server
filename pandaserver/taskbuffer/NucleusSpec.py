"""
nucleus specification

"""

import re


class NucleusSpec(object):

    # constructor
    def __init__(self, name):
        self.name = name
        self.allPandaSites = []
        self.allDdmEndPoints = {}
        self.all_ddm_endpoints_in = {}
        self.state = None

    # add
    def add(self, siteName, ddmSpecDict, ddmSpecDictForInput=None):
        if siteName not in self.allPandaSites:
            self.allPandaSites.append(siteName)
            # add local endpoints
            for scope in ddmSpecDict:
                ddmSpec = ddmSpecDict[scope]
                for localEndPoint in ddmSpec.getLocalEndPoints():
                    if localEndPoint not in self.allDdmEndPoints:
                        self.allDdmEndPoints[localEndPoint] = ddmSpec.getEndPoint(localEndPoint)
            if ddmSpecDictForInput is not None:
                # add endpoints
                for scope in ddmSpecDictForInput:
                    ddmSpec = ddmSpecDictForInput[scope]
                    for localEndPoint in ddmSpec.getAllEndPoints():
                        if localEndPoint not in self.all_ddm_endpoints_in:
                            self.all_ddm_endpoints_in[localEndPoint] = ddmSpec.getEndPoint(localEndPoint)

    # check if associated panda site
    def isAssociatedPandaSite(self, siteName):
        return siteName in self.allPandaSites

    # check if associated DDM endpoint
    def isAssociatedEndpoint(self, endPoint):
        return endPoint in self.allDdmEndPoints

    # check if associated endpoint for input
    def is_associated_for_input(self, endpoint):
        return endpoint in self.all_ddm_endpoints_in

    # get associated DDM endpoint
    def getEndpoint(self, endPoint):
        try:
            if endPoint in self.allDdmEndPoints:
                return self.allDdmEndPoints[endPoint]
            return self.all_ddm_endpoints_in[endPoint]
        except Exception:
            None

    # get associated DDM endpoint
    def getAssociatedEndpoint(self, patt):
        patt = patt.split('/')[-1]
        if patt.startswith('dst:'):
            patt = patt.split(':')[-1]
        if patt in self.allDdmEndPoints:
            return self.allDdmEndPoints[patt]
        for endPointName in self.allDdmEndPoints:
            # ignore TEST or SPECIAL
            # if self.allDdmEndPoints[endPointName]['type'] in ['TEST','SPECIAL']:
            #    continue
            # check name
            if re.search(patt, endPointName) is not None:
                return self.allDdmEndPoints[endPointName]
            # check type
            pattwoVO = re.sub('ATLAS', '', patt)
            if self.allDdmEndPoints[endPointName]['type'] == pattwoVO:
                return self.allDdmEndPoints[endPointName]
        return None

    # get one panda site
    def getOnePandaSite(self):
        if len(self.allPandaSites) > 0:
            return self.allPandaSites[0]
        return None
