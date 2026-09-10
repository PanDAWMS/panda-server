"""
worker specification

"""

import datetime
from typing import Any, Sequence


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

    # Column types, taken from the Oracle schema of ATLAS_PANDA.HARVESTER_WORKERS (panda-database
    # repo, schema/oracle). The columns are installed by __init__ via setattr, so a type
    # checker sees none of them without these declarations. They carry no value, which
    # both keeps them out of the class dict and keeps __slots__ classes importable.
    # Unset columns really are None here -- this class has no "NULL" sentinel.
    harvesterID: str | None
    workerID: int | None
    batchID: str | None
    queueName: str | None
    status: str | None
    computingSite: str | None
    nCore: int | None
    nodeID: str | None
    submitTime: datetime.datetime | None
    startTime: datetime.datetime | None
    endTime: datetime.datetime | None
    lastUpdate: datetime.datetime | None
    stdOut: str | None
    stdErr: str | None
    batchLog: str | None
    jdl: str | None
    resourceType: str | None
    nativeExitCode: int | None
    nativeStatus: str | None
    diagMessage: str | None
    nJobs: int | None
    computingElement: str | None
    submissionHost: str | None
    harvesterHost: str | None
    errorCode: int | None
    jobType: str | None
    minRamCount: int | None
    # slots
    __slots__ = _attributes + ("_changedAttrs",)
    # attributes which have 0 by default
    _zeroAttrs = ()
    # catchall resource type
    RT_catchall = "ANY"

    # Bookkeeping attribute installed by __init__ via object.__setattr__, so a type
    # checker does not see it without this declaration. It maps a column name to the
    # value last assigned to it.
    _changedAttrs: dict[str, Any]

    # constructor
    def __init__(self) -> None:
        # install attributes
        for attr in self._attributes:
            object.__setattr__(self, attr, None)
        # map of changed attributes
        object.__setattr__(self, "_changedAttrs", {})

    # override __setattr__ to collect the changed attributes
    def __setattr__(self, name: str, value: Any) -> None:
        oldVal = getattr(self, name)
        # convert string to datetime
        if isinstance(value, str) and value.startswith("datetime/"):
            value = datetime.datetime.strptime(value.split("/")[-1], "%Y-%m-%d %H:%M:%S.%f")
        object.__setattr__(self, name, value)
        # collect changed attributes
        if oldVal != value:
            self._changedAttrs[name] = value

    # reset changed attribute list
    def resetChangedList(self) -> None:
        object.__setattr__(self, "_changedAttrs", {})

    # return map of values
    def valuesMap(self, onlyChanged: bool = False) -> dict[str, Any]:
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
    def pack(self, values: Sequence[Any]) -> None:
        for i in range(len(self._attributes)):
            attr = self._attributes[i]
            val = values[i]
            object.__setattr__(self, attr, val)

    # return column names for INSERT
    @classmethod
    def columnNames(cls, prefix: str | None = None) -> str:
        ret = ""
        for attr in cls._attributes:
            if prefix is not None:
                ret += f"{prefix}."
            ret += f"{attr},"
        ret = ret[:-1]
        return ret

    # return expression of bind variables for INSERT
    @classmethod
    def bindValuesExpression(cls) -> str:
        from pandaserver.config import panda_config

        ret = "VALUES("
        for attr in cls._attributes:
            ret += f":{attr},"
        ret = ret[:-1]
        ret += ")"
        return ret

    # return an expression of bind variables for UPDATE to update only changed attributes
    def bindUpdateChangesExpression(self) -> str:
        ret = ""
        for attr in self._attributes:
            if attr not in self._changedAttrs:
                continue
            ret += "{0}=:{0},".format(attr)
        ret = ret[:-1]
        return ret

    # return state values to be pickled
    def __getstate__(self) -> list[Any]:
        state = []
        for attr in self._attributes:
            val = getattr(self, attr)
            state.append(val)
        state.append(self._changedAttrs)
        return state

    # restore state from the unpickled state values
    def __setstate__(self, state: list[Any]) -> None:
        i = 0
        for attr in self._attributes:
            if i >= len(state) - 1:
                break
            object.__setattr__(self, attr, state[i])
            i += 1
        object.__setattr__(self, "_changedAttrs", state[-1])
