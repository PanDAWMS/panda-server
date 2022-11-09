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
        self.bareNucleus = None
        self.secondaryNucleus = None
        self.nucleus = True
        self.default_ddm_endpoint_out = None

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

    # set bare nucleus mode
    def set_bare_nucleus_mode(self, mode):
        self.bareNucleus = mode

    # get bare nucleus mode
    def get_bare_nucleus_mode(self):
        return self.bareNucleus

    # set secondary nucleus
    def set_secondary_nucleus(self, nucleus):
        self.secondaryNucleus = nucleus

    # get secondary nucleus
    def get_secondary_nucleus(self):
        return self.secondaryNucleus

    # set satellite
    def set_satellite(self):
        self.nucleus = False

    # check if nucleus
    def is_nucleus(self):
        return self.nucleus

    # set default endpoint for output
    def set_default_endpoint_out(self, rse):
        self.default_ddm_endpoint_out = rse

    # get default endpoint for output
    def get_default_endpoint_out(self):
        return self.getEndpoint(self.default_ddm_endpoint_out)
