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
    def add(self, relation, endpointDict):
        name = relation["ddm_endpoint_name"]

        # protection against inconsistent dict
        if name not in endpointDict:
            return

        # all endpoints, copy all properties about ddm endpoint and relation
        self.all[name] = {}
        for key in endpointDict[name]:
            value = endpointDict[name][key]
            self.all[name][key] = value
        for key in relation:
            value = relation[key]
            self.all[name][key] = value

        # local endpoints
        if relation["is_local"] != "N":
            self.local.add(name)
        # defaults
        if relation["default_read"] == "Y":
            self.default_read = name
        if relation["default_write"] == "Y":
            self.default_write = name
        # tape
        if relation["is_tape"] == "Y":
            self.tape.add(name)

    # get all endpoints
    def getAllEndPoints(self):
        return list(self.all)

    # get endpoint
    def getEndPoint(self, endpointName):
        if endpointName in self.all:
            return self.all[endpointName]
        return None

    # get local endpoints
    def getLocalEndPoints(self):
        tmpRet = sorted(self.local)
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
    def isAssociated(self, endpointName):
        return endpointName in self.all

    # check local
    def isLocal(self, endpointName):
        return endpointName in self.local

    # get DDM endpoint associated with a pattern
    def getAssociatedEndpoint(self, patt, mode="output"):
        patt = patt.split("/")[-1]
        if patt in self.all:
            return self.all[patt]

        endpoint = None
        order = 10**6  # Like infinite
        for tmp_ddm_endpoint_name in self.all:
            tmp_ddm_endpoint_dict = self.all[tmp_ddm_endpoint_name]
            # get the order of the current loop endpoint
            if mode == "input":
                tmp_order = tmp_ddm_endpoint_dict["order_read"]
            elif mode == "output":
                tmp_order = tmp_ddm_endpoint_dict["order_write"]
            # we already have a closer endpoint, so skip the looping one
            if tmp_order > order:
                continue

            # check name
            if re.search(patt, tmp_ddm_endpoint_name) is not None:
                endpoint = self.all[tmp_ddm_endpoint_name]
                order = tmp_order

            # check type
            pattwoVO = re.sub("ATLAS", "", patt)
            if self.all[tmp_ddm_endpoint_name]["type"] == pattwoVO:
                endpoint = self.all[tmp_ddm_endpoint_name]
                order = tmp_order

        return endpoint

    # get mapping between tokens and endpoint names
    def getTokenMap(self, mode):
        ret_map = {}
        orders = {}
        for tmp_ddm_endpoint_name in self.all:
            tmp_ddm_endpoint_dict = self.all[tmp_ddm_endpoint_name]
            token = tmp_ddm_endpoint_dict["ddm_spacetoken_name"]

            # get the order
            if mode == "input":
                order = tmp_ddm_endpoint_dict["order_read"]
            elif mode == "output":
                order = tmp_ddm_endpoint_dict["order_write"]

            # map already contains this token
            if token in ret_map and orders[token] < order:
                continue

            # add
            ret_map[token] = tmp_ddm_endpoint_name
            orders[token] = order

        return ret_map
