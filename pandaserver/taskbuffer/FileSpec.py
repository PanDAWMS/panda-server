"""
file specification

"""

from typing import TYPE_CHECKING, Any, Sequence

from pandaserver.taskbuffer.spec_column import SpecColumn

if TYPE_CHECKING:
    # JobSpec imports this module, so naming it for real here would close the cycle
    from pandaserver.taskbuffer.JobSpec import JobSpec

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

    # Column types, taken from the Oracle schema of ATLAS_PANDA.FILESTABLE4 (panda-database
    # repo, schema/oracle). The columns are installed by __init__ via setattr, so a type
    # checker sees none of them without these declarations. They carry no value: this
    # class uses __slots__ built from an expression, and a value here would raise at
    # import time.
    #
    # SpecColumn is not decoration. __getattribute__ below substitutes the string "NULL"
    # for a column that is still None, so a read yields either the column type or that
    # sentinel -- which is what makes `spec.numberfiles + 1` a latent TypeError -- while a
    # write takes the column type or None. See spec_column.py.
    row_ID: SpecColumn[int]
    PandaID: SpecColumn[int]
    GUID: SpecColumn[str]
    lfn: SpecColumn[str]
    type: SpecColumn[str]
    dataset: SpecColumn[str]
    status: SpecColumn[str]
    prodDBlock: SpecColumn[str]
    prodDBlockToken: SpecColumn[str]
    dispatchDBlock: SpecColumn[str]
    dispatchDBlockToken: SpecColumn[str]
    destinationDBlock: SpecColumn[str]
    destinationDBlockToken: SpecColumn[str]
    destinationSE: SpecColumn[str]
    fsize: SpecColumn[int]
    md5sum: SpecColumn[str]
    checksum: SpecColumn[str]
    scope: SpecColumn[str]
    jediTaskID: SpecColumn[int]
    datasetID: SpecColumn[int]
    fileID: SpecColumn[int]
    attemptNr: SpecColumn[int]
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
    def __init__(self) -> None:
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
    def __getattribute__(self, name: str) -> Any:
        # PandaID
        if name == "PandaID":
            # read _owner without going through this method again, which would replace an
            # absent owner with the "NULL" sentinel and make the check below never true
            owner = object.__getattribute__(self, "_owner")
            if owner is None:
                return "NULL"
            return owner.PandaID
        # others
        ret = object.__getattribute__(self, name)
        if ret is None:
            return "NULL"
        return ret

    # override __setattr__ to collect the changed attributes
    def __setattr__(self, name: str, value: Any) -> None:
        oldVal = getattr(self, name)
        object.__setattr__(self, name, value)
        newVal = getattr(self, name)
        # collect changed attributes
        if oldVal != newVal:
            self._changedAttrs[name] = value

    # set owner
    def setOwner(self, owner: "JobSpec") -> None:
        self._owner = owner
        self._oldPandaID = self.PandaID

    # reset changed attribute list
    def resetChangedList(self) -> None:
        self._oldPandaID = self.PandaID
        object.__setattr__(self, "_changedAttrs", {})

    # return a tuple of values
    def values(self) -> tuple[Any, ...]:
        ret = []
        for attr in self._attributes:
            val = getattr(self, attr)
            ret.append(val)
        return tuple(ret)

    # return map of values
    def valuesMap(self, useSeq: bool = False, onlyChanged: bool = False) -> dict[str, Any]:
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
    def pack(self, values: Sequence[Any]) -> None:
        for i in range(len(self._attributes)):
            attr = self._attributes[i]
            val = values[i]
            object.__setattr__(self, attr, val)

    # return state values to be pickled
    def __getstate__(self) -> list[Any]:
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
    def __setstate__(self, state: list[Any]) -> None:
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
    @classmethod
    def columnNames(cls, withMod: bool = False) -> str:
        ret = ""
        for attr in cls._attributes:
            if ret != "":
                ret += ","
            ret += attr
        # add modificationTime
        if withMod:
            ret += ",modificationTime"
        return ret

    # return expression of values for INSERT
    @classmethod
    def valuesExpression(cls) -> str:
        ret = "VALUES("
        for attr in cls._attributes:
            ret += "%s"
            if attr != cls._attributes[len(cls._attributes) - 1]:
                ret += ","
        ret += ")"
        return ret

    # return expression of bind variables for INSERT
    @classmethod
    def bindValuesExpression(cls, useSeq: bool = False, withMod: bool = False) -> str:
        from pandaserver.config import panda_config

        ret = "VALUES("
        for attr in cls._attributes:
            if useSeq and attr in cls._seqAttrMap:
                if panda_config.backend == "mysql":
                    # mysql
                    ret += "NULL,"
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

    # return an expression for UPDATE
    @classmethod
    def updateExpression(cls) -> str:
        ret = ""
        for attr in cls._attributes:
            ret = ret + attr + "=%s"
            if attr != cls._attributes[len(cls._attributes) - 1]:
                ret += ","
        return ret

    # return an expression of bind variables for UPDATE
    @classmethod
    def bindUpdateExpression(cls) -> str:
        ret = ""
        for attr in cls._attributes:
            ret += f"{attr}=:{attr},"
        ret = ret[:-1]
        ret += " "
        return ret

    # return an expression of bind variables for UPDATE to update only changed attributes
    def bindUpdateChangesExpression(self) -> str:
        ret = ""
        for attr in self._attributes:
            if attr in self._changedAttrs or (attr == "PandaID" and self.PandaID != self._oldPandaID):
                ret += f"{attr}=:{attr},"
        ret = ret[:-1]
        ret += " "
        return ret

    # check if unmerged input
    def isUnMergedInput(self) -> bool:
        if self.type == "input" and self.dispatchDBlockToken == "TOMERGE":
            return True
        return False

    # check if unmerged output
    def isUnMergedOutput(self) -> bool:
        if self.type in ["output", "log"] and self.destinationDBlockToken == "TOMERGE":
            return True
        return False

    # allow no output
    def allowNoOutput(self) -> None:
        if self.dispatchDBlockToken in ["NULL", None, ""]:
            items = []
        else:
            items = self.dispatchDBlockToken.split(",")
        if "an" not in items:
            items.append("an")
            self.dispatchDBlockToken = ",".join(items)

    # check if no output is allowed
    def isAllowedNoOutput(self) -> bool:
        try:
            if "an" in self.dispatchDBlockToken.split(","):
                return True
        except Exception:
            pass
        return False

    # dump to be json-serializable
    def dump_to_json_serializable(self) -> list[Any]:
        stat = self.__getstate__()[:-1]
        # set None as _owner
        stat.append(None)
        return stat

    # to a dictionary
    def to_dict(self) -> dict[str, Any]:
        ret = {}
        for a in self._attributes:
            v = getattr(self, a)
            if v == "NULL":
                v = None
            ret[a] = v
        return ret
