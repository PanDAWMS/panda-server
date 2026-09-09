"""
dataset specification for JEDI

"""

import datetime
import math
import re
from typing import Any, Sequence

from pandacommon.pandautils.PandaUtils import get_sql_IN_bind_variables

from pandaserver.config import panda_config
from pandaserver.taskbuffer.JediFileSpec import JediFileSpec


class JediDatasetSpec(object):
    # the file list this spec carries, created in __init__ with
    # object.__setattr__. Declared here so that the element type is stated:
    # JobSpec.Files holds FileSpec and this one holds JediFileSpec, and nothing
    # but the class it belongs to tells the two apart
    Files: list[JediFileSpec]

    def __str__(self) -> str:
        sb = []
        for key in self.__dict__:
            if key == "Files":
                sb.append(f"{key}='{len(self.__dict__[key])}'")
            else:
                sb.append(f"{key}='{self.__dict__[key]}'")
        return ", ".join(sb)

    def __repr__(self) -> str:
        return self.__str__()

    # attributes
    _attributes = (
        "jediTaskID",
        "datasetID",
        "datasetName",
        "containerName",
        "type",
        "creationTime",
        "modificationTime",
        "vo",
        "cloud",
        "site",
        "masterID",
        "provenanceID",
        "status",
        "state",
        "stateCheckTime",
        "stateCheckExpiration",
        "frozenTime",
        "nFiles",
        "nFilesToBeUsed",
        "nFilesUsed",
        "nFilesFinished",
        "nFilesFailed",
        "nFilesOnHold",
        "nEvents",
        "nEventsToBeUsed",
        "nEventsUsed",
        "lockedBy",
        "lockedTime",
        "attributes",
        "streamName",
        "storageToken",
        "destination",
        "templateID",
        "nFilesWaiting",
        "nFilesMissing",
    )

    # Column types, taken from the Oracle schema of ATLAS_PANDA.JEDI_DATASETS (panda-database
    # repo, schema/oracle). The columns are installed by __init__ via setattr, so a type
    # checker sees none of them without these declarations. They carry no value, which
    # both keeps them out of the class dict and keeps __slots__ classes importable.
    # Unset columns really are None here -- this class has no "NULL" sentinel.
    jediTaskID: int | None
    datasetID: int | None
    datasetName: str | None
    containerName: str | None
    type: str | None
    creationTime: datetime.datetime | None
    modificationTime: datetime.datetime | None
    vo: str | None
    cloud: str | None
    site: str | None
    masterID: int | None
    provenanceID: int | None
    status: str | None
    state: str | None
    stateCheckTime: datetime.datetime | None
    stateCheckExpiration: datetime.datetime | None
    frozenTime: datetime.datetime | None
    nFiles: int | None
    nFilesToBeUsed: int | None
    nFilesUsed: int | None
    nFilesFinished: int | None
    nFilesFailed: int | None
    nFilesOnHold: int | None
    nEvents: int | None
    nEventsToBeUsed: int | None
    nEventsUsed: int | None
    lockedBy: str | None
    lockedTime: datetime.datetime | None
    attributes: str | None
    streamName: str | None
    storageToken: str | None
    destination: str | None
    templateID: int | None
    nFilesWaiting: int | None
    nFilesMissing: int | None
    # attributes which have 0 by default
    _zeroAttrs = ()
    # attributes to force update
    _forceUpdateAttrs = ("lockedBy", "lockedTime")
    # mapping between sequence and attr
    _seqAttrMap = {"datasetID": f"{panda_config.schemaJEDI}.JEDI_DATASETS_ID_SEQ.nextval"}
    # token for attributes
    attrToken = {
        "allowNoOutput": "an",
        "consistencyCheck": "cc",
        "eventRatio": "er",
        "indexConsistent": "ic",
        "mergeOnly": "mo",
        "nFilesPerJob": "np",
        "num_records": "nr",
        "offset": "of",
        "objectStore": "os",
        "pseudo": "ps",
        "random": "rd",
        "reusable": "ru",
        "transient": "tr",
        "useDuplicated": "ud",
        "no_staging": "ns",
    }

    # Bookkeeping attribute installed by __init__ via object.__setattr__, so a type
    # checker does not see it without this declaration. It maps a column name to the
    # value last assigned to it.
    _changedAttrs: dict[str, Any]

    # constructor
    def __init__(self) -> None:
        # install attributes
        for attr in self._attributes:
            object.__setattr__(self, attr, None)
        # file list
        object.__setattr__(self, "Files", [])
        # map of changed attributes
        object.__setattr__(self, "_changedAttrs", {})
        # distributed
        object.__setattr__(self, "distributed", False)

    # override __setattr__ to collect the changed attributes
    def __setattr__(self, name: str, value: Any) -> None:
        oldVal = getattr(self, name)
        object.__setattr__(self, name, value)
        newVal = getattr(self, name)
        # collect changed attributes
        if oldVal != newVal or name in self._forceUpdateAttrs:
            self._changedAttrs[name] = value

    # add File to files list
    def addFile(self, fileSpec: JediFileSpec) -> None:
        # append
        self.Files.append(fileSpec)

    # reset changed attribute list
    def resetChangedList(self) -> None:
        object.__setattr__(self, "_changedAttrs", {})

    # force update
    def forceUpdate(self, name: str) -> None:
        if name in self._attributes:
            self._changedAttrs[name] = getattr(self, name)

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

    # set dataset attribute
    def setDatasetAttribute(self, attr: str) -> None:
        if self.attributes is None:
            self.attributes = ""
        else:
            self.attributes += ","
        self.attributes += attr

    # set dataset attribute with label
    def setDatasetAttributeWithLabel(self, label: str) -> None:
        if label not in self.attrToken:
            return
        attr = self.attrToken[label]
        if self.attributes is None:
            self.attributes = ""
        else:
            self.attributes += ","
        self.attributes += attr

    # return list of status to update contents
    @classmethod
    def statusToUpdateContents(cls) -> list[str]:
        return ["defined", "toupdate"]

    # return list of types for input
    @classmethod
    def getInputTypes(cls) -> list[str]:
        return ["input", "pseudo_input"]

    # return list of types to generate jobs
    @classmethod
    def getProcessTypes(cls) -> list[str]:
        return cls.getInputTypes() + ["pp_input"] + cls.getMergeProcessTypes()

    # return list of types for merging
    @classmethod
    def getMergeProcessTypes(cls) -> list[str]:
        return ["trn_log", "trn_output"]

    # get type of unknown input
    @classmethod
    def getUnknownInputType(cls) -> str:
        return "trn_unknown"

    # get type of constituent input
    @classmethod
    def get_constituent_input_type(cls) -> str:
        return "in_constituent"

    # check if JEDI needs to keep track of file usage
    def toKeepTrack(self) -> bool:
        if self.isNoSplit() and self.isRepeated():
            return False
        elif self.isReusable():
            return False
        else:
            return True

    # check if it is not split
    def isNoSplit(self) -> bool:
        if self.attributes is not None and "nosplit" in self.attributes:
            return True
        else:
            return False

    # check if it is repeatedly used
    def isRepeated(self) -> bool:
        if self.attributes is not None and "repeat" in self.attributes:
            return True
        else:
            return False

    # check if it is randomly used
    def isRandom(self) -> bool:
        if self.attributes is not None and "rd" in self.attributes.split(","):
            return True
        else:
            return False

    # check if it is reusable
    def isReusable(self) -> bool:
        if self.attributes is not None and "ru" in self.attributes.split(","):
            return True
        else:
            return False

    # check if consistency is checked
    def checkConsistency(self) -> bool:
        if self.attributes is not None and "cc" in self.attributes.split(","):
            return True
        else:
            return False

    # set consistency is checked
    def enableCheckConsistency(self) -> None:
        if not self.attributes:
            self.attributes = "cc"
        elif "cc" not in self.attributes.split(","):
            self.attributes += ",cc"

    # check if it is pseudo
    def isPseudo(self) -> bool:
        if self.datasetName in ["pseudo_dataset", "seq_number"] or self.type in ["pp_input"]:
            return True
        if self.attributes is not None and self.attrToken["pseudo"] in self.attributes.split(","):
            return True
        return False

    # check if it is a many-time dataset which is treated as long-standing at T2s
    def isManyTime(self) -> bool:
        if self.attributes is not None and "manytime" in self.attributes:
            return True
        else:
            return False

    # check if it is seq number
    def isSeqNumber(self) -> bool:
        if self.datasetName in ["seq_number"]:
            return True
        else:
            return False

    # check if duplicated files are used
    def useDuplicatedFiles(self) -> bool:
        if self.attributes is not None and ("usedup" in self.attributes or "ud" in self.attributes.split(",")):
            return True
        else:
            return False

    # check if it is a master dataset
    def isMaster(self) -> bool:
        if self.masterID is None and self.type in self.getProcessTypes():
            return True
        else:
            return False

    # check if it is a master input dataset
    def isMasterInput(self) -> bool:
        if self.masterID is None and self.type in self.getInputTypes():
            return True
        else:
            return False

    # remove nosplit attribute
    def remAttribute(self, attrName: str) -> None:
        if self.attributes is not None:
            self.attributes = re.sub(attrName, "", self.attributes)
            self.attributes = re.sub(",,", ",", self.attributes)
            self.attributes = re.sub("^,", "", self.attributes)
            self.attributes = re.sub(",$", "", self.attributes)
            if self.attributes == "":
                self.attributes = None

    # remove nosplit attribute
    def remNoSplit(self) -> None:
        self.remAttribute("nosplit")

    # remove repeat attribute
    def remRepeat(self) -> None:
        self.remAttribute("repeat")

    # get the ratio to master
    def getRatioToMaster(self) -> int | float:
        try:
            tmpMatch = re.search("ratio=(\d+(\.\d+)*)", self.attributes or "")
            if tmpMatch is not None:
                ratioStr = tmpMatch.group(1)
                try:
                    # integer
                    return int(ratioStr)
                except Exception:
                    pass
                try:
                    # float
                    return float(ratioStr)
                except Exception:
                    pass
        except Exception:
            pass
        return 1

    # get N multiplied by ratio
    def getNumMultByRatio(self, num: int) -> int | None:
        # no split
        if self.isNoSplit():
            return None
        # get ratio
        ratioVal = self.getRatioToMaster()
        # integer or float
        if isinstance(ratioVal, int):
            retVal = num * ratioVal
        else:
            scaled = float(num) * ratioVal
            retVal = int(math.ceil(scaled))
        return retVal

    # unique map key for output
    def outputMapKey(self) -> str:
        mapKey = f"{self.datasetName}#{self.provenanceID}"
        return mapKey

    # unique map key
    def uniqueMapKey(self) -> str:
        mapKey = f"{self.datasetName}#{self.datasetID}"
        return mapKey

    # set offset
    def setOffset(self, offset: int) -> None:
        self.setDatasetAttribute(f"{self.attrToken['offset']}={offset}")

    # get offset
    def getOffset(self) -> int:
        if self.attributes is not None:
            tmpMatch = re.search(self.attrToken["offset"] + "=(\d+)", self.attributes)
            if tmpMatch is not None:
                offset = int(tmpMatch.group(1))
                return offset
        return 0

    # set number of records
    def setNumRecords(self, n: int) -> None:
        self.setDatasetAttribute(f"{self.attrToken['num_records']}={n}")

    # get number of records
    def getNumRecords(self) -> int | None:
        if self.attributes is not None:
            for item in self.attributes.split(","):
                tmpMatch = re.search(self.attrToken["num_records"] + "=(\d+)", item)
                if tmpMatch is not None:
                    num_records = int(tmpMatch.group(1))
                    return num_records
        return None

    # set object store
    def setObjectStore(self, objectStore: str) -> None:
        self.setDatasetAttribute(f"{self.attrToken['objectStore']}={objectStore}")

    # get object store
    def getObjectStore(self) -> str | None:
        if self.attributes is not None:
            tmpMatch = re.search(self.attrToken["objectStore"] + "=([^,]+)", self.attributes)
            if tmpMatch is not None:
                return tmpMatch.group(1)
        return None

    # set the number of files per job
    def setNumFilesPerJob(self, num: int) -> None:
        self.setDatasetAttribute(f"{self.attrToken['nFilesPerJob']}={num}")

    # get the number of files per job
    def getNumFilesPerJob(self) -> int | None:
        if self.attributes is not None:
            tmpMatch = re.search(self.attrToken["nFilesPerJob"] + "=(\d+)", self.attributes)
            if tmpMatch is not None:
                num = int(tmpMatch.group(1))
                return num
        # use continuous numbers for seq_number
        if self.isSeqNumber():
            return 1
        return None

    # check if unmerged dataset
    def toMerge(self) -> bool:
        if self.type is not None and self.type.startswith("trn_"):
            return True
        return False

    # set transient
    def setTransient(self, val: bool) -> None:
        num_repr = 1 if val is True else 0
        self.setDatasetAttribute(f"{self.attrToken['transient']}={num_repr}")

    # get transient
    def getTransient(self) -> bool | None:
        if self.attributes is not None:
            for item in self.attributes.split(","):
                tmpMatch = re.search(self.attrToken["transient"] + "=(\d+)", item)
                if tmpMatch is not None:
                    val = int(tmpMatch.group(1))
                    if val == 1:
                        return True
                    else:
                        return False
        return None

    # check if no output is allowed
    def isAllowedNoOutput(self) -> bool:
        if self.attributes is not None and self.attrToken["allowNoOutput"] in self.attributes.split(","):
            return True
        else:
            return False

    # allow no output
    def allowNoOutput(self) -> None:
        if not self.attributes:
            items: list[Any] = []
        else:
            items = self.attributes.split(",")
        if self.attrToken["allowNoOutput"] not in items:
            items.append(self.attrToken["allowNoOutput"])
            self.attributes = ",".join(items)

    # check if index consistency is required
    def indexConsistent(self) -> bool:
        if self.attributes is not None and self.attrToken["indexConsistent"] in self.attributes.split(","):
            return True
        else:
            return False

    # set distributed
    def setDistributed(self) -> None:
        self.distributed = True

    # reset distributed
    def reset_distributed(self) -> None:
        self.distributed = False

    # check if distributed
    def isDistributed(self) -> bool:
        return self.distributed

    # set event ratio
    def setEventRatio(self, num: int | float) -> None:
        self.setDatasetAttribute(f"{self.attrToken['eventRatio']}={num}")

    # get event ratio
    def getEventRatio(self) -> int | float | None:
        if self.attributes is not None:
            for item in self.attributes.split(","):
                tmpMatch = re.search(self.attrToken["eventRatio"] + "=(\d+(\.\d+)*)", item)
                if tmpMatch is not None:
                    ratioStr = tmpMatch.group(1)
                    try:
                        # integer
                        return int(ratioStr)
                    except Exception:
                        pass
                    try:
                        # float
                        return float(ratioStr)
                    except Exception:
                        pass
        return None

    # set pseudo
    def setPseudo(self) -> None:
        if not self.attributes:
            items: list[Any] = []
        else:
            items = self.attributes.split(",")
        if self.attrToken["pseudo"] not in items:
            items.append(self.attrToken["pseudo"])
            self.attributes = ",".join(items)

    # merge only
    def is_merge_only(self) -> bool:
        try:
            return self.attrToken["mergeOnly"] in (self.attributes or "").split(",")
        except Exception:
            return False

    # sort files by old PandaIDs and move files with no PandaIDs to the end
    def sort_files_by_panda_ids(self) -> None:
        # files with no PandaID sort last, and keep the order they came in
        self.Files = sorted(self.Files, key=lambda x: (x.PandaID is None, x.PandaID or 0))

    # set no staging
    def set_no_staging(self, value: bool) -> None:
        """
        Set no_staging (ns) attribute for dataset

        Args:
            value (bool): value of no_staging; will set attribute = 1 for True or 0 for False
        """
        num_repr = 1 if value else 0
        self.setDatasetAttribute(f"{self.attrToken['no_staging']}={num_repr}")

    # get whether is no_staging
    def is_no_staging(self) -> bool:
        """
        Set no_staging (ns) attribute for dataset

        Returns:
            bool: whether with no_staging
        """
        if self.attributes is not None:
            for item in self.attributes.split(","):
                tmpMatch = re.search(self.attrToken["no_staging"] + "=(\d+)", item)
                if tmpMatch is not None:
                    num_repr = int(tmpMatch.group(1))
                    value = bool(num_repr)
                    return value
        return False


# often-used bind variables
INPUT_TYPES_var_str, INPUT_TYPES_var_map = get_sql_IN_bind_variables(JediDatasetSpec.getInputTypes(), prefix=":type_", value_as_suffix=True)
PROCESS_TYPES_var_str, PROCESS_TYPES_var_map = get_sql_IN_bind_variables(JediDatasetSpec.getProcessTypes(), prefix=":type_", value_as_suffix=True)
MERGE_TYPES_var_str, MERGE_TYPES_var_map = get_sql_IN_bind_variables(JediDatasetSpec.getMergeProcessTypes(), prefix=":type_", value_as_suffix=True)
