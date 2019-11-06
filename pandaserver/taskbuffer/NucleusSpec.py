"""
nucleus specification

"""

import re

class NucleusSpec(object):

    # constructor
    def __init__(self,name):
        self.name = name
        self.allPandaSites = []
        self.allDdmEndPoints = {}
        self.state = None



    # add 
    def add(self,siteName,ddmSpec):
        if not siteName in self.allPandaSites:
            self.allPandaSites.append(siteName)
            # add local endpoints
            for localEndPoint in ddmSpec.getLocalEndPoints():
                if not localEndPoint in self.allDdmEndPoints:
                    self.allDdmEndPoints[localEndPoint] = ddmSpec.getEndPoint(localEndPoint)



    # check if associated panda site
    def isAssociatedPandaSite(self,siteName):
        return siteName in self.allPandaSites



    # check if associated DDM endpoint
    def isAssociatedEndpoint(self,endPoint):
        return endPoint in self.allDdmEndPoints



    # get associated DDM endpoint
    def getEndpoint(self,endPoint):
        try:
            return self.allDdmEndPoints[endPoint]
        except Exception:
            None



    # get associated DDM endpoint
    def getAssociatedEndpoint(self,patt):
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
            if re.search(patt,endPointName) is not None:
                return self.allDdmEndPoints[endPointName]
            # check type
            pattwoVO = re.sub('ATLAS','',patt)
            if self.allDdmEndPoints[endPointName]['type'] == pattwoVO:
                return self.allDdmEndPoints[endPointName]
        return None



    # get one panda site
    def getOnePandaSite(self):
        if len(self.allPandaSites) > 0:
            return self.allPandaSites[0]
        return None

