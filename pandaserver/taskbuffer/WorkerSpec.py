"""
worker specification

"""

import datetime


class WorkerSpec(object):
    # attributes
    _attributes = (
        "harvesterID",
        "workerID",
        "batchID",
        "queueName",
        "status",
        "computingSite",
        "nCore",
        "nodeID",
        "submitTime",
        "startTime",
        "endTime",
        "lastUpdate",
        "stdOut",
        "stdErr",
        "batchLog",
        "jdl",
        "resourceType",
        "nativeExitCode",
        "nativeStatus",
        "diagMessage",
        "nJobs",
        "computingElement",
        "submissionHost",
        "harvesterHost",
        "errorCode",
        "jobType",
        "minRamCount",
    )
    # slots
    __slots__ = _attributes + ("_changedAttrs",)
    # attributes which have 0 by default
    _zeroAttrs = ()
    # catchall resouce type
    RT_catchall = "ANY"

    # constructor
    def __init__(self):
        # install attributes
        for attr in self._attributes:
            object.__setattr__(self, attr, None)
        # map of changed attributes
        object.__setattr__(self, "_changedAttrs", {})

    # override __setattr__ to collect the changed attributes
    def __setattr__(self, name, value):
        oldVal = getattr(self, name)
        # convert string to datetime
        if isinstance(value, str) and value.startswith("datetime/"):
            value = datetime.datetime.strptime(value.split("/")[-1], "%Y-%m-%d %H:%M:%S.%f")
        object.__setattr__(self, name, value)
        # collect changed attributes
        if oldVal != value:
            self._changedAttrs[name] = value

    # reset changed attribute list
    def resetChangedList(self):
        self._oldPandaID = self.PandaID
        object.__setattr__(self, "_changedAttrs", {})

    # return map of values
    def valuesMap(self, onlyChanged=False):
        ret = {}
        for attr in self._attributes:
            if onlyChanged and attr not in self._changedAttrs:
                continue
            val = getattr(self, attr)
            if val is None:
                if attr in self._zeroAttrs:
                    val = 0
            ret[f":{attr}"] = val
        return ret

    # pack tuple into FileSpec
    def pack(self, values):
        for i in range(len(self._attributes)):
            attr = self._attributes[i]
            val = values[i]
            object.__setattr__(self, attr, val)

    # return column names for INSERT
    def columnNames(cls, prefix=None):
        ret = ""
        for attr in cls._attributes:
            if prefix is not None:
                ret += f"{prefix}."
            ret += f"{attr},"
        ret = ret[:-1]
        return ret

    columnNames = classmethod(columnNames)

    # return expression of bind variables for INSERT
    def bindValuesExpression(cls):
        from pandaserver.config import panda_config

        ret = "VALUES("
        for attr in cls._attributes:
            ret += f":{attr},"
        ret = ret[:-1]
        ret += ")"
        return ret

    bindValuesExpression = classmethod(bindValuesExpression)

    # return an expression of bind variables for UPDATE to update only changed attributes
    def bindUpdateChangesExpression(self):
        ret = ""
        for attr in self._attributes:
            if attr not in self._changedAttrs:
                continue
            ret += "{0}=:{0},".format(attr)
        ret = ret[:-1]
        return ret

    # return state values to be pickled
    def __getstate__(self):
        state = []
        for attr in self._attributes:
            val = getattr(self, attr)
            state.append(val)
        state.append(self._changedAttrs)
        return state

    # restore state from the unpickled state values
    def __setstate__(self, state):
        i = 0
        for attr in self._attributes:
            if i >= len(state) - 1:
                break
            object.__setattr__(self, attr, state[i])
            i += 1
        object.__setattr__(self, "_changedAttrs", state[-1])
