"""
file specification

"""

reserveChangedState = False


class FileSpec(object):
    # attributes
    _attributes = (
        "row_ID",
        "PandaID",
        "GUID",
        "lfn",
        "type",
        "dataset",
        "status",
        "prodDBlock",
        "prodDBlockToken",
        "dispatchDBlock",
        "dispatchDBlockToken",
        "destinationDBlock",
        "destinationDBlockToken",
        "destinationSE",
        "fsize",
        "md5sum",
        "checksum",
        "scope",
        "jediTaskID",
        "datasetID",
        "fileID",
        "attemptNr",
    )
    # slots
    __slots__ = _attributes + (
        "_owner",
        "_changedAttrs",
        "_oldPandaID",
        "_reserveChangedState",
    )
    # attributes which have 0 by default
    _zeroAttrs = ("fsize",)
    # mapping between sequence and attr
    _seqAttrMap = {"row_ID": "ATLAS_PANDA.FILESTABLE4_ROW_ID_SEQ.nextval"}

    # constructor
    def __init__(self):
        # install attributes
        for attr in self._attributes:
            object.__setattr__(self, attr, None)
        # set owner to synchronize PandaID
        object.__setattr__(self, "_owner", None)
        # map of changed attributes
        object.__setattr__(self, "_changedAttrs", {})
        # old PandaID
        object.__setattr__(self, "_oldPandaID", "NULL")
        # reserve changed state at instance level
        object.__setattr__(self, "_reserveChangedState", False)

    # override __getattribute__ for SQL and PandaID
    def __getattribute__(self, name):
        # PandaID
        if name == "PandaID":
            if self._owner is None:
                return "NULL"
            return self._owner.PandaID
        # others
        ret = object.__getattribute__(self, name)
        if ret is None:
            return "NULL"
        return ret

    # override __setattr__ to collect the changed attributes
    def __setattr__(self, name, value):
        oldVal = getattr(self, name)
        object.__setattr__(self, name, value)
        newVal = getattr(self, name)
        # collect changed attributes
        if oldVal != newVal:
            self._changedAttrs[name] = value

    # set owner
    def setOwner(self, owner):
        self._owner = owner
        self._oldPandaID = self.PandaID

    # reset changed attribute list
    def resetChangedList(self):
        self._oldPandaID = self.PandaID
        object.__setattr__(self, "_changedAttrs", {})

    # return a tuple of values
    def values(self):
        ret = []
        for attr in self._attributes:
            val = getattr(self, attr)
            ret.append(val)
        return tuple(ret)

    # return map of values
    def valuesMap(self, useSeq=False, onlyChanged=False):
        ret = {}
        for attr in self._attributes:
            if useSeq and attr in self._seqAttrMap:
                continue
            if onlyChanged:
                if attr == "PandaID":
                    if self.PandaID == self._oldPandaID:
                        continue
                elif attr not in self._changedAttrs:
                    continue
            val = getattr(self, attr)
            if val == "NULL":
                if attr in self._zeroAttrs:
                    val = 0
                else:
                    val = None
            ret[f":{attr}"] = val
        return ret

    # pack tuple into FileSpec
    def pack(self, values):
        for i in range(len(self._attributes)):
            attr = self._attributes[i]
            val = values[i]
            object.__setattr__(self, attr, val)

    # return state values to be pickled
    def __getstate__(self):
        state = []
        for attr in self._attributes:
            val = getattr(self, attr)
            state.append(val)
        if reserveChangedState or self._reserveChangedState:
            state.append(self._changedAttrs)
        # append owner info
        state.append(self._owner)
        return state

    # restore state from the unpickled state values
    def __setstate__(self, state):
        pandaID = "NULL"
        for i in range(len(self._attributes)):
            if i + 1 < len(state):
                object.__setattr__(self, self._attributes[i], state[i])
            else:
                object.__setattr__(self, self._attributes[i], "NULL")
            if self._attributes[i] == "PandaID":
                pandaID = state[i]
        object.__setattr__(self, "_owner", state[-1])
        object.__setattr__(self, "_oldPandaID", pandaID)
        if not hasattr(self, "_reserveChangedState"):
            object.__setattr__(self, "_reserveChangedState", False)
        if reserveChangedState or self._reserveChangedState:
            object.__setattr__(self, "_changedAttrs", state[-2])
        else:
            object.__setattr__(self, "_changedAttrs", {})

    # return column names for INSERT
    def columnNames(cls, withMod=False):
        ret = ""
        for attr in cls._attributes:
            if ret != "":
                ret += ","
            ret += attr
        # add modificationTime
        if withMod:
            ret += ",modificationTime"
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

    # return expression of bind variables for INSERT
    def bindValuesExpression(cls, useSeq=False, withMod=False):
        from pandaserver.config import panda_config

        ret = "VALUES("
        for attr in cls._attributes:
            if useSeq and attr in cls._seqAttrMap:
                if panda_config.backend == "mysql":
                    # mysql
                    ret += f"NULL,"
                else:
                    # oracle
                    ret += f"{cls._seqAttrMap[attr]},"
            else:
                ret += f":{attr},"
        ret = ret[:-1]
        # add modificationTime
        if withMod:
            ret += ",:modificationTime"
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
        ret += " "
        return ret

    bindUpdateExpression = classmethod(bindUpdateExpression)

    # return an expression of bind variables for UPDATE to update only changed attributes
    def bindUpdateChangesExpression(self):
        ret = ""
        for attr in self._attributes:
            if attr in self._changedAttrs or (attr == "PandaID" and self.PandaID != self._oldPandaID):
                ret += f"{attr}=:{attr},"
        ret = ret[:-1]
        ret += " "
        return ret

    # check if unmerged input
    def isUnMergedInput(self):
        if self.type == "input" and self.dispatchDBlockToken == "TOMERGE":
            return True
        return False

    # check if unmerged output
    def isUnMergedOutput(self):
        if self.type in ["output", "log"] and self.destinationDBlockToken == "TOMERGE":
            return True
        return False

    # allow no output
    def allowNoOutput(self):
        if self.dispatchDBlockToken in ["NULL", None, ""]:
            items = []
        else:
            items = self.dispatchDBlockToken.split(",")
        if "an" not in items:
            items.append("an")
            self.dispatchDBlockToken = ",".join(items)

    # check if no output is allowed
    def isAllowedNoOutput(self):
        try:
            if "an" in self.dispatchDBlockToken.split(","):
                return True
        except Exception:
            pass
        return False

    # dump to be json-serializable
    def dump_to_json_serializable(self):
        stat = self.__getstate__()[:-1]
        # set None as _owner
        stat.append(None)
        return stat
