"""
work queue specification

"""

import re

from pandaserver.taskbuffer.GlobalShares import Share

RESOURCE = "Resource"
ACTIVE_FUNCTIONS = [RESOURCE]


class WorkQueue(object):
    # attributes
    _attributes = (
        "queue_id",
        "queue_name",
        "queue_type",
        "VO",
        "queue_share",
        "queue_order",
        "criteria",
        "variables",
        "partitionID",
        "stretchable",
        "status",
        "queue_function",
    )

    # parameters for selection criteria
    _paramsForSelection = ("prodSourceLabel", "workingGroup", "processingType", "coreCount", "site", "eventService", "splitRule", "campaign")

    # correspondence with Global Shares attributes and parameters
    _attributes_gs_conversion_dic = {"name": "queue_name", "value": "queue_share", "prodsourcelabel": "queue_type", "queue_id": "queue_id", "vo": "VO"}

    _params_gs_conversion_dic = {
        "prodsourcelabel": "prodSourceLabel",
        "workinggroup": "workingGroup",
        "campaign": "campaign",
        "processingtype": "processingType",
    }

    def __init__(self):
        """
        Constructor
        """
        # install attributes
        for attr in self._attributes:
            setattr(self, attr, None)

        # global share is by default false
        self.is_global_share = False
        # throttled is set to True by default. Some Global Shares will overwrite it to False
        self.throttled = True

    def __str__(self):
        """
        String representation of a workqueue
        :return: string with the representation of the work queue
        """
        return str(self.queue_name)

    def dump(self):
        """
        Creates a human-friendly string with the work queue information
        :return: string representation of the work queue
        """
        dump_str = f"id:{self.queue_id} order:{self.queue_order} name:{self.queue_name} share:{self.queue_share} "

        # normal queue
        if self.is_global_share:
            dump_str += f"gs_name:{self.queue_name} (global share)"
        else:
            dump_str += f"criteria:{self.criteria} var:{str(self.variables)} eval:{self.evalString}"

        return dump_str

    def getID(self):
        """
        get ID
        :return: returns a list with the ID of the work queue
        """
        return self.queue_id

    def pack(self, values):
        """
        Packs tuple into the object
        :param values: list with the values in the order declared in the attributes section
        :return: nothing
        """
        for i, attr in enumerate(self._attributes):
            val = values[i]
            setattr(self, attr, val)

        # disallow negative share
        if self.queue_share is not None and self.queue_share < 0:
            self.queue_share = 0

        # convert variables string to a map of bind-variables
        tmp_map = {}
        try:
            for item in self.variables.split(","):
                # look for key: value
                item = item.strip()
                items = item.split(":")
                if len(items) != 2:
                    continue
                # add
                tmp_map[f":{items[0]}"] = items[1]
        except Exception:
            pass
        # assign map
        self.variables = tmp_map
        # make a python statement for eval
        if self.criteria in ["", None]:
            # catch all
            self.evalString = "True"
        else:
            tmp_eval_str = self.criteria
            # replace IN/OR/AND to in/or/and
            tmp_eval_str = re.sub(" IN ", " in ", tmp_eval_str, re.I)
            tmp_eval_str = re.sub(" OR ", " or ", tmp_eval_str, re.I)
            tmp_eval_str = re.sub(" AND ", " and ", tmp_eval_str, re.I)
            # replace = to ==
            tmp_eval_str = tmp_eval_str.replace("=", "==")
            # replace LIKE
            tmp_eval_str = re.sub("(?P<var>[^ \(]+)\s+LIKE\s+(?P<pat>[^ \(]+)", "re.search(\g<pat>,\g<var>,re.I) is not None", tmp_eval_str, re.I)
            # NULL
            tmp_eval_str = re.sub(" IS NULL", "==None", tmp_eval_str)
            tmp_eval_str = re.sub(" IS NOT NULL", "!=None", tmp_eval_str)
            # replace NOT to not
            tmp_eval_str = re.sub(" NOT ", " not ", tmp_eval_str, re.I)
            # fomat cases
            for tmp_param in self._paramsForSelection:
                tmp_eval_str = re.sub(tmp_param, tmp_param, tmp_eval_str, re.I)
            # replace bind-variables
            for tmp_key, tmp_val in self.variables.items():
                if "%" in tmp_val:
                    # wildcard
                    tmp_val = tmp_val.replace("%", ".*")
                    tmp_val = f"'^{tmp_val}$'"
                else:
                    # normal variable
                    tmp_val = f"'{tmp_val}'"
                tmp_eval_str = tmp_eval_str.replace(tmp_key, tmp_val)
            # assign
            self.evalString = tmp_eval_str

    def pack_gs(self, gshare):
        """
        Packs tuple into the object
        :param gshare: global share
        :return: nothing
        """

        # the object becomes a global share wq
        self.is_global_share = True
        try:
            tmp_map = {}
            for i, attr in enumerate(gshare._attributes):
                # global share attributes can be mapped to a wq attribute(1), to a wq param(2), or to none of both
                # 1. if the gs attribute is mapped to a wq attribute, do a get and a set
                if attr in self._attributes_gs_conversion_dic:
                    attr_wq = self._attributes_gs_conversion_dic[attr]
                    val = getattr(gshare, attr)
                    setattr(self, attr_wq, val)
                # 2. if the gs attribute is mapped to a wq param, add it to the bind variables dictionary
                # Probably we don't need this, we just care about matching the gs name
                if attr in self._params_gs_conversion_dic:
                    param_wq = self._params_gs_conversion_dic[attr]
                    val = getattr(gshare, attr)
                    tmp_map[f":{param_wq}"] = val

                # 3. Special case for throttled. This is defined additionally, since it's not present in WQs
                if attr == "throttled" and gshare.throttled == "N":
                    self.throttled = False

            self.variables = tmp_map
        except Exception:
            pass

    # evaluate in python
    def evaluate(self, param_map):
        # only active queues are evaluated
        if self.isActive():
            # normal queue
            # expand parameters to local namespace
            for tmp_param_key, tmp_param_val in param_map.items():
                if isinstance(tmp_param_val, str):
                    # add quotes for string
                    exec(f'{tmp_param_key}="{tmp_param_val}"', globals())
                else:
                    exec(f"{tmp_param_key}={tmp_param_val}", globals())
            # add default parameters if missing
            for tmp_param in self._paramsForSelection:
                if tmp_param not in param_map:
                    exec(f"{tmp_param}=None", globals())
            # evaluate
            exec(f"ret_var = {self.evalString}", globals())
            return self, ret_var

        # return False
        return self, False

    # check if active
    def isActive(self):
        if self.status != "inactive":  # and self.queue_function in ACTIVE_FUNCTIONS:
            return True
        return False

    # check if its eligible after global share alignment
    def isAligned(self):
        if self.queue_function == RESOURCE or self.is_global_share:
            return True
        return False

    # return column names for INSERT
    def column_names(cls):
        ret = ""
        for attr in cls._attributes:
            if ret != "":
                ret += ","
            ret += attr
        return ret

    column_names = classmethod(column_names)
