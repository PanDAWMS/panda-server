"""
file specification for JEDI

"""

import datetime
import re
import types
from typing import TYPE_CHECKING, Any, Sequence

from pandaserver.taskbuffer.FileSpec import FileSpec as JobFileSpec

if TYPE_CHECKING:
    # JediDatasetSpec imports this module, so naming it for real here would close the cycle
    from pandaserver.taskbuffer.JediDatasetSpec import JediDatasetSpec


class JediFileSpec(object):
    # attributes
    _attributes = (
        "jediTaskID",
        "datasetID",
        "fileID",
        "creationDate",
        "lastAttemptTime",
        "lfn",
        "GUID",
        "type",
        "status",
        "fsize",
        "checksum",
        "scope",
        "attemptNr",
        "maxAttempt",
        "nEvents",
        "keepTrack",
        "startEvent",
        "endEvent",
        "firstEvent",
        "boundaryID",
        "PandaID",
        "failedAttempt",
        "lumiBlockNr",
        "outPandaID",
        "maxFailure",
        "ramCount",
        "is_waiting",
        "proc_status",
        "constituent_id",
    )

    # Column types, taken from the Oracle schema of ATLAS_PANDA.JEDI_DATASET_CONTENTS (panda-database
    # repo, schema/oracle). The columns are installed by __init__ via setattr, so a type
    # checker sees none of them without these declarations. They carry no value, which
    # both keeps them out of the class dict and keeps __slots__ classes importable.
    # Unset columns really are None here -- this class has no "NULL" sentinel.
    jediTaskID: int | None
    datasetID: int | None
    fileID: int | None
    creationDate: datetime.datetime | None
    lastAttemptTime: datetime.datetime | None
    lfn: str | None
    GUID: str | None
    type: str | None
    status: str | None
    fsize: int | None
    checksum: str | None
    scope: str | None
    attemptNr: int | None
    maxAttempt: int | None
    nEvents: int | None
    keepTrack: int | None
    startEvent: int | None
    endEvent: int | None
    firstEvent: int | None
    boundaryID: int | None
    PandaID: int | None
    failedAttempt: int | None
    lumiBlockNr: int | None
    outPandaID: int | None
    maxFailure: int | None
    ramCount: int | None
    is_waiting: str | None
    proc_status: str | None
    constituent_id: int | None
    # attributes which have 0 by default
    _zeroAttrs = ("fsize", "attemptNr", "failedAttempt", "ramCount")
    # mapping between sequence and attr
    _seqAttrMap = {"fileID": "ATLAS_PANDA.JEDI_DATASET_CONT_FILEID_SEQ.nextval"}

    # Bookkeeping attribute installed by __init__ via object.__setattr__, so a type
    # checker does not see it without this declaration. It maps a column name to the
    # value last assigned to it.
    _changedAttrs: dict[str, Any]

    # constructor
    def __init__(self) -> None:
        # install attributes
        for attr in self._attributes:
            if attr in self._zeroAttrs:
                object.__setattr__(self, attr, 0)
            else:
                object.__setattr__(self, attr, None)
        # map of changed attributes
        object.__setattr__(self, "_changedAttrs", {})
        # locality
        object.__setattr__(self, "locality", {})
        # source name
        object.__setattr__(self, "sourceName", None)

    # override __setattr__ to collecte the changed attributes
    def __setattr__(self, name: str, value: Any) -> None:
        oldVal = getattr(self, name)
        object.__setattr__(self, name, value)
        newVal = getattr(self, name)
        # collect changed attributes
        if oldVal != newVal:
            self._changedAttrs[name] = value

    # reset changed attribute list
    def resetChangedList(self) -> None:
        object.__setattr__(self, "_changedAttrs", {})

    # return map of values
    def valuesMap(self, useSeq: bool = False, onlyChanged: bool = False) -> dict[str, Any]:
        ret = {}
        for attr in self._attributes:
            # use sequence
            if useSeq and attr in self._seqAttrMap:
                continue
            # only changed attributes
            if onlyChanged:
                if attr not in self._changedAttrs:
                    continue
            val = getattr(self, attr)
            if val is None:
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

    # return column names for INSERT
    @classmethod
    def columnNames(cls, useSeq: bool = False, defaultVales: dict[str, Any] | None = None, skipDefaultAttr: bool = False) -> str:
        if defaultVales is None:
            defaultVales = {}
        ret = ""
        for attr in cls._attributes:
            if skipDefaultAttr and (attr in cls._seqAttrMap or attr in defaultVales):
                continue
            if ret != "":
                ret += ","
            if useSeq and attr in cls._seqAttrMap:
                ret += f"{cls._seqAttrMap[attr]}"
                continue
            if attr in defaultVales:
                arg = defaultVales[attr]
                if arg is None:
                    ret += "NULL"
                elif isinstance(arg, str):
                    ret += f"'{arg}'"
                else:
                    ret += f"{arg}"
                continue
            ret += attr
        return ret

    # return expression of bind variables for INSERT
    @classmethod
    def bindValuesExpression(cls, useSeq: bool = True) -> str:
        ret = "VALUES("
        for attr in cls._attributes:
            if useSeq and attr in cls._seqAttrMap:
                ret += f"{cls._seqAttrMap[attr]},"
            else:
                ret += f":{attr},"
        ret = ret[:-1]
        ret += ")"
        return ret

    # return an expression of bind variables for UPDATE to update only changed attributes
    def bindUpdateChangesExpression(self) -> str:
        ret = ""
        for attr in self._attributes:
            if attr in self._changedAttrs:
                ret += f"{attr}=:{attr},"
        ret = ret[:-1]
        ret += " "
        return ret

    # convert to job's FileSpec
    def convertToJobFileSpec(self, datasetSpec: "JediDatasetSpec", setType: str | None = None, useEventService: bool = False) -> JobFileSpec:
        jobFileSpec = JobFileSpec()
        jobFileSpec.fileID = self.fileID
        jobFileSpec.datasetID = datasetSpec.datasetID
        jobFileSpec.jediTaskID = datasetSpec.jediTaskID
        jobFileSpec.lfn = self.lfn
        jobFileSpec.GUID = self.GUID
        if setType is None:
            jobFileSpec.type = self.type
        else:
            jobFileSpec.type = setType
        jobFileSpec.scope = self.scope
        jobFileSpec.fsize = self.fsize
        jobFileSpec.checksum = self.checksum
        jobFileSpec.attemptNr = self.attemptNr
        # dataset attribute
        if datasetSpec is not None:
            # dataset
            if datasetSpec.containerName not in [None, ""]:
                jobFileSpec.dataset = datasetSpec.containerName
            else:
                jobFileSpec.dataset = datasetSpec.datasetName
            if self.type in datasetSpec.getInputTypes() or setType in datasetSpec.getInputTypes():
                # prodDBlock
                jobFileSpec.prodDBlock = datasetSpec.datasetName
                # storage token
                if datasetSpec.storageToken not in ["", None]:
                    jobFileSpec.dispatchDBlockToken = datasetSpec.storageToken
            else:
                # destinationDBlock
                jobFileSpec.destinationDBlock = datasetSpec.datasetName
                # storage token
                if datasetSpec.storageToken not in ["", None]:
                    jobFileSpec.destinationDBlockToken = datasetSpec.storageToken.split("/")[0]
                # destination
                if datasetSpec.destination not in ["", None]:
                    jobFileSpec.destinationSE = datasetSpec.destination
                # set prodDBlockToken for Event Service
                if useEventService and datasetSpec.getObjectStore() is not None:
                    jobFileSpec.prodDBlockToken = f"objectstore^{datasetSpec.getObjectStore()}"
                # allow no output
                if datasetSpec.isAllowedNoOutput():
                    jobFileSpec.allowNoOutput()
        # return
        return jobFileSpec

    # convert from job's FileSpec
    def convertFromJobFileSpec(self, jobFileSpec: JobFileSpec) -> None:
        self.fileID = jobFileSpec.fileID  # type: ignore[assignment]  # "NULL" sentinel, see spec_column.py
        self.datasetID = jobFileSpec.datasetID  # type: ignore[assignment]  # "NULL" sentinel, see spec_column.py
        self.jediTaskID = jobFileSpec.jediTaskID  # type: ignore[assignment]  # "NULL" sentinel, see spec_column.py
        self.lfn = jobFileSpec.lfn
        self.GUID = jobFileSpec.GUID
        self.type = jobFileSpec.type
        self.scope = jobFileSpec.scope
        self.fsize = jobFileSpec.fsize  # type: ignore[assignment]  # "NULL" sentinel, see spec_column.py
        self.checksum = jobFileSpec.checksum
        self.attemptNr = jobFileSpec.attemptNr  # type: ignore[assignment]  # "NULL" sentinel, see spec_column.py
        # convert NULL to None
        for attr in self._attributes:
            val = getattr(self, attr)
            if val == "NULL":
                object.__setattr__(self, attr, None)
        # return
        return

    # get effective number of events
    def getEffectiveNumEvents(self) -> int:
        if self.endEvent is not None and self.startEvent is not None:
            evtCounts = self.endEvent - self.startEvent + 1
            if evtCounts > 0:
                return evtCounts
            return 1
        if self.nEvents is not None and self.nEvents > 0:
            return self.nEvents
        return 1

    # extract fields string
    def extractFieldsStr(self, fieldNumList: list[int]) -> str:
        tmpFieldStr = ""
        if self.lfn is None:
            return tmpFieldStr
        try:
            tmpMidStrList = re.split("\.|_tid\d+", self.lfn)
            for tmpFieldNum in fieldNumList:
                tmpFieldStr += "." + tmpMidStrList[tmpFieldNum - 1]
        except Exception:
            pass
        return tmpFieldStr
