"""
nucleus specification

"""

import re
from typing import Any

from pandaserver.taskbuffer.DdmSpec import DdmSpec


class NucleusSpec(object):
    # constructor
    def __init__(self, name: str) -> None:
        self.name = name
        self.allPandaSites: list[str] = []
        # endpoint name -> the endpoint's properties, which is what DdmSpec.all holds
        self.allDdmEndPoints: dict[str, dict[str, Any]] = {}
        self.all_ddm_endpoints_in: dict[str, dict[str, Any]] = {}
        self.state: str | None = None
        self.bareNucleus: str | None = None
        self.secondaryNucleus: str | None = None
        self.nucleus = True
        self.default_ddm_endpoint_out: str | None = None

    # add
    def add(self, siteName: str, ddmSpecDict: dict[str, DdmSpec], ddmSpecDictForInput: dict[str, DdmSpec] | None = None) -> None:
        if siteName not in self.allPandaSites:
            self.allPandaSites.append(siteName)
            # add local endpoints
            for scope in ddmSpecDict:
                ddmSpec = ddmSpecDict[scope]
                for localEndPoint in ddmSpec.getLocalEndPoints():
                    if localEndPoint not in self.allDdmEndPoints:
                        # the names come out of the same map getEndPoint reads, so this is set;
                        # everything that reads these entries indexes them, so a None cannot go in
                        endPointDict = ddmSpec.getEndPoint(localEndPoint)
                        if endPointDict is not None:
                            self.allDdmEndPoints[localEndPoint] = endPointDict
            if ddmSpecDictForInput is not None:
                # add endpoints
                for scope in ddmSpecDictForInput:
                    ddmSpec = ddmSpecDictForInput[scope]
                    for localEndPoint in ddmSpec.getAllEndPoints():
                        if localEndPoint not in self.all_ddm_endpoints_in:
                            endPointDict = ddmSpec.getEndPoint(localEndPoint)
                            if endPointDict is not None:
                                self.all_ddm_endpoints_in[localEndPoint] = endPointDict

    # check if associated panda site
    def isAssociatedPandaSite(self, siteName: str) -> bool:
        return siteName in self.allPandaSites

    # check if associated DDM endpoint
    def isAssociatedEndpoint(self, endPoint: str) -> bool:
        return endPoint in self.allDdmEndPoints

    # check if associated endpoint for input
    def is_associated_for_input(self, endpoint: str) -> bool:
        return endpoint in self.all_ddm_endpoints_in

    # get associated DDM endpoint
    def getEndpoint(self, endpoint: str | None) -> dict[str, Any] | None:
        # both maps are keyed by endpoint name, so a None is not one of them
        if endpoint is None:
            return None
        try:
            if endpoint in self.allDdmEndPoints:
                return self.allDdmEndPoints[endpoint]
            return self.all_ddm_endpoints_in[endpoint]
        except Exception:
            return None

    def getAssociatedEndpoint(self, pattern: str | None) -> dict[str, Any] | None:
        if pattern is None:
            return None
        pattern = pattern.split("/")[-1]
        if pattern.startswith("dst:"):
            pattern = pattern.split(":")[-1]

        if pattern in self.allDdmEndPoints:
            return self.allDdmEndPoints[pattern]

        for endpoint_name in self.allDdmEndPoints:
            if re.search(pattern, endpoint_name) is not None:
                return self.allDdmEndPoints[endpoint_name]

            pattern_without_vo = re.sub("ATLAS", "", pattern)

            if self.allDdmEndPoints[endpoint_name]["type"] == pattern_without_vo:
                return self.allDdmEndPoints[endpoint_name]

        return None

    # get one panda site
    def getOnePandaSite(self) -> str | None:
        if len(self.allPandaSites) > 0:
            return self.allPandaSites[0]
        return None

    # set bare nucleus mode
    def set_bare_nucleus_mode(self, mode: str) -> None:
        self.bareNucleus = mode

    # get bare nucleus mode
    def get_bare_nucleus_mode(self) -> str | None:
        return self.bareNucleus

    # set secondary nucleus
    def set_secondary_nucleus(self, nucleus: str) -> None:
        self.secondaryNucleus = nucleus

    # get secondary nucleus
    def get_secondary_nucleus(self) -> str | None:
        return self.secondaryNucleus

    # set satellite
    def set_satellite(self) -> None:
        self.nucleus = False

    # check if nucleus
    def is_nucleus(self) -> bool:
        return self.nucleus

    # set default endpoint for output
    def set_default_endpoint_out(self, rse: str | None) -> None:
        self.default_ddm_endpoint_out = rse

    # get default endpoint for output
    def get_default_endpoint_out(self) -> dict[str, Any] | None:
        return self.getEndpoint(self.default_ddm_endpoint_out)
