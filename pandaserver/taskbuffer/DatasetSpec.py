"""
dataset specification

"""


class DatasetSpec(object):
    # attributes
    _attributes = (
        "vuid",
        "name",
        "version",
        "type",
        "status",
        "numberfiles",
        "currentfiles",
        "creationdate",
        "modificationdate",
        "MoverID",
        "transferStatus",
        "subType",
    )

    # attributes which have 0 by default
    _zeroAttrs = ("MoverID", "transferStatus")

    # constructor
    def __init__(self):
        # install attributes
        for attr in self._attributes:
            setattr(self, attr, None)

    # override __getattribute__ for SQL
    def __getattribute__(self, name):
        ret = object.__getattribute__(self, name)
        if ret is None:
            return "NULL"
        return ret

    # return a tuple of values
    def values(self):
        ret = []
        for attr in self._attributes:
            val = getattr(self, attr)
            ret.append(val)
        return tuple(ret)

    # return map of values
    def valuesMap(self):
        ret = {}
        for attr in self._attributes:
            val = getattr(self, attr)
            if val == "NULL":
                if attr in self._zeroAttrs:
                    val = 0
                else:
                    val = None
            ret[f":{attr}"] = val
        return ret

    # pack tuple into DatasetSpec
    def pack(self, values):
        for i in range(len(self._attributes)):
            attr = self._attributes[i]
            val = values[i]
            setattr(self, attr, val)

    # return column names for INSERT
    def columnNames(cls):
        ret = ""
        for attr in cls._attributes:
            if ret != "":
                ret += ","
            ret += attr
        return ret

    columnNames = classmethod(columnNames)

    # return expression of values for INSERT
    def valuesExpression(cls):
        ret = "VALUES("
        for attr in cls._attributes:
            ret += "%s"
            if attr != cls._attributes[len(cls._attributes) - 1]:
                ret += ","
        ret += ")"
        return ret

    valuesExpression = classmethod(valuesExpression)

    # return expression of bind values for INSERT
    def bindValuesExpression(cls):
        ret = "VALUES("
        for attr in cls._attributes:
            ret += f":{attr},"
        ret = ret[:-1]
        ret += ")"
        return ret

    bindValuesExpression = classmethod(bindValuesExpression)

    # return an expression for UPDATE
    def updateExpression(cls):
        ret = ""
        for attr in cls._attributes:
            ret = ret + attr + "=%s"
            if attr != cls._attributes[len(cls._attributes) - 1]:
                ret += ","
        return ret

    updateExpression = classmethod(updateExpression)

    # return an expression of bind variables for UPDATE
    def bindUpdateExpression(cls):
        ret = ""
        for attr in cls._attributes:
            ret += f"{attr}=:{attr},"
        ret = ret[:-1]
        return ret

    bindUpdateExpression = classmethod(bindUpdateExpression)

    # return state values to be pickled
    def __getstate__(self):
        state = []
        for attr in self._attributes:
            val = getattr(self, attr)
            state.append(val)
        return state

    # restore state from the unpickled state values
    def __setstate__(self, state):
        for i, attr in enumerate(self._attributes):
            if i < len(state):
                setattr(self, attr, state[i])
            else:
                setattr(self, attr, None)
