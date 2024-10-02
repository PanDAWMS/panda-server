"""
ddm endpoint specification object

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

    def add(self, relation, endpoint_dictionary):
        """
        Add an endpoint to the DDM specification.

        This method adds an endpoint to the DDM specification by copying all properties
        about the DDM endpoint and relation. It also updates local endpoints, default
        read/write endpoints, and tape endpoints.

        Args:
            relation (dict): A dictionary containing the relation properties.
            endpoint_dictionary (dict): A dictionary containing the endpoint properties.
        """
        name = relation["ddm_endpoint_name"]

        # Protection against inconsistent dict
        if name not in endpoint_dictionary:
            return

        # All endpoints, copy all properties about DDM endpoint and relation
        self.all[name] = {}
        for key in endpoint_dictionary[name]:
            value = endpoint_dictionary[name][key]
            self.all[name][key] = value
        for key in relation:
            value = relation[key]
            self.all[name][key] = value

        # Local endpoints
        if relation["is_local"] != "N":
            self.local.add(name)

        # Default read and write
        if relation["default_read"] == "Y":
            self.default_read = name
        if relation["default_write"] == "Y":
            self.default_write = name

        # Tape
        if relation["is_tape"] == "Y":
            self.tape.add(name)

    def getAllEndPoints(self):
        """
        Get all DDM endpoints. This method returns a list of all DDM endpoints.

        Returns:
            list: A list of all DDM endpoints.
        """
        return list(self.all)

    def getEndPoint(self, endpoint_name):
        """
        Get a specific DDM endpoint.

        This method returns the properties of a specific DDM endpoint.

        Args:
            endpoint_name (str): The name of the DDM endpoint.
        Returns:
            dict or None: A dictionary containing the properties of the DDM endpoint, or None if not found.
        """
        if endpoint_name in self.all:
            return self.all[endpoint_name]
        return None

    def getLocalEndPoints(self):
        """
        This method returns a sorted list of local DDM endpoints.

        Returns:
            list: A sorted list of local DDM endpoints.
        """
        sorted_endpoints = sorted(self.local)
        return sorted_endpoints

    def getDefaultWrite(self):
        """
        This method returns the default write DDM endpoint.

        Returns:
            str or None: The default write DDM endpoint, or None if not set.
        """
        return self.default_write

    def getDefaultRead(self):
        """
        This method returns the default read DDM endpoint.

        Returns:
            str or None: The default write DDM endpoint, or None if not set.
        """
        return self.default_read

    def getTapeEndPoints(self):
        """
        This method returns a tuple of tape DDM endpoints.

        Returns:
            tuple: A tuple of tape DDM endpoints.
        """
        return tuple(self.tape)

    def isAssociated(self, endpoint_name):
        """
        This method checks if a given endpoint name is associated with any DDM endpoint.

        Args:
            endpoint_name (str): The name of the DDM endpoint.

        Returns:
            bool: True if the endpoint is associated, False otherwise.
        """
        return endpoint_name in self.all

    def getAssociatedEndpoint(self, patt, mode="output"):
        """
        This method returns the DDM endpoint associated with a given pattern and of the lowest order.

        Args:
            patt (str): The pattern to match.
            mode (str): The mode, either "input" or "output". Default is "output".
        Returns:
            dict or None: A dictionary containing the properties of the associated DDM endpoint, or None if not found.
        """
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
            pattern_without_vo = re.sub("ATLAS", "", patt)
            if self.all[tmp_ddm_endpoint_name]["type"] == pattern_without_vo:
                endpoint = self.all[tmp_ddm_endpoint_name]
                order = tmp_order

        return endpoint

    def getTokenMap(self, mode):
        """
        This method returns a mapping between space tokens and endpoint names based on the mode.

        Args:
            mode (str): The mode, either "input" or "output".
        Returns:
            dict: A dictionary mapping tokens to endpoint names.
        """
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
