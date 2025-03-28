"""
dataset specification for JEDI

"""

import math
import re

from pandacommon.pandautils.PandaUtils import get_sql_IN_bind_variables

from pandaserver.config import panda_config


class JediDatasetSpec(object):
    def __str__(self):
        sb = []
        for key in self.__dict__:
            if key == "Files":
                sb.append(f"{key}='{len(self.__dict__[key])}'")
            else:
                sb.append(f"{key}='{self.__dict__[key]}'")
        return ", ".join(sb)

    def __repr__(self):
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

    # constructor
    def __init__(self):
        # install attributes
        for attr in self._attributes:
            object.__setattr__(self, attr, None)
        # file list
        object.__setattr__(self, "Files", [])
        # map of changed attributes
        object.__setattr__(self, "_changedAttrs", {})
        # distributed
        object.__setattr__(self, "distributed", False)

    # override __setattr__ to collecte the changed attributes
    def __setattr__(self, name, value):
        oldVal = getattr(self, name)
        object.__setattr__(self, name, value)
        newVal = getattr(self, name)
        # collect changed attributes
        if oldVal != newVal or name in self._forceUpdateAttrs:
            self._changedAttrs[name] = value

    # add File to files list
    def addFile(self, fileSpec):
        # append
        self.Files.append(fileSpec)

    # reset changed attribute list
    def resetChangedList(self):
        object.__setattr__(self, "_changedAttrs", {})

    # force update
    def forceUpdate(self, name):
        if name in self._attributes:
            self._changedAttrs[name] = getattr(self, name)

    # return map of values
    def valuesMap(self, useSeq=False, onlyChanged=False):
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
    def bindValuesExpression(cls, useSeq=True):
        ret = "VALUES("
        for attr in cls._attributes:
            if useSeq and attr in cls._seqAttrMap:
                ret += f"{cls._seqAttrMap[attr]},"
            else:
                ret += f":{attr},"
        ret = ret[:-1]
        ret += ")"
        return ret

    bindValuesExpression = classmethod(bindValuesExpression)

    # return an expression of bind variables for UPDATE to update only changed attributes
    def bindUpdateChangesExpression(self):
        ret = ""
        for attr in self._attributes:
            if attr in self._changedAttrs:
                ret += f"{attr}=:{attr},"
        ret = ret[:-1]
        ret += " "
        return ret

    # set dataset attribute
    def setDatasetAttribute(self, attr):
        if self.attributes is None:
            self.attributes = ""
        else:
            self.attributes += ","
        self.attributes += attr

    # set dataset attribute with label
    def setDatasetAttributeWithLabel(self, label):
        if label not in self.attrToken:
            return
        attr = self.attrToken[label]
        if self.attributes is None:
            self.attributes = ""
        else:
            self.attributes += ","
        self.attributes += attr

    # return list of status to update contents
    def statusToUpdateContents(cls):
        return ["defined", "toupdate"]

    statusToUpdateContents = classmethod(statusToUpdateContents)

    # return list of types for input
    def getInputTypes(cls):
        return ["input", "pseudo_input"]

    getInputTypes = classmethod(getInputTypes)

    # return list of types to generate jobs
    def getProcessTypes(cls):
        return cls.getInputTypes() + ["pp_input"] + cls.getMergeProcessTypes()

    getProcessTypes = classmethod(getProcessTypes)

    # return list of types for merging
    def getMergeProcessTypes(cls):
        return ["trn_log", "trn_output"]

    getMergeProcessTypes = classmethod(getMergeProcessTypes)

    # get type of unkown input
    def getUnknownInputType(cls):
        return "trn_unknown"

    getUnknownInputType = classmethod(getUnknownInputType)

    # check if JEDI needs to keep track of file usage
    def toKeepTrack(self):
        if self.isNoSplit() and self.isRepeated():
            return False
        elif self.isReusable():
            return False
        else:
            return True

    # check if it is not split
    def isNoSplit(self):
        if self.attributes is not None and "nosplit" in self.attributes:
            return True
        else:
            return False

    # check if it is repeatedly used
    def isRepeated(self):
        if self.attributes is not None and "repeat" in self.attributes:
            return True
        else:
            return False

    # check if it is randomly used
    def isRandom(self):
        if self.attributes is not None and "rd" in self.attributes.split(","):
            return True
        else:
            return False

    # check if it is reusable
    def isReusable(self):
        if self.attributes is not None and "ru" in self.attributes.split(","):
            return True
        else:
            return False

    # check if consistency is checked
    def checkConsistency(self):
        if self.attributes is not None and "cc" in self.attributes.split(","):
            return True
        else:
            return False

    # set consistency is checked
    def enableCheckConsistency(self):
        if self.attributes in [None, ""]:
            self.attributes = "cc"
        elif "cc" not in self.attributes.split(","):
            self.attributes += ",cc"

    # check if it is pseudo
    def isPseudo(self):
        if self.datasetName in ["pseudo_dataset", "seq_number"] or self.type in ["pp_input"]:
            return True
        if self.attributes is not None and self.attrToken["pseudo"] in self.attributes.split(","):
            return True
        return False

    # check if it is a many-time dataset which is treated as long-standing at T2s
    def isManyTime(self):
        if self.attributes is not None and "manytime" in self.attributes:
            return True
        else:
            return False

    # check if it is seq number
    def isSeqNumber(self):
        if self.datasetName in ["seq_number"]:
            return True
        else:
            return False

    # check if duplicated files are used
    def useDuplicatedFiles(self):
        if self.attributes is not None and ("usedup" in self.attributes or "ud" in self.attributes.split(",")):
            return True
        else:
            return False

    # check if it is a master dataset
    def isMaster(self):
        if self.masterID is None and self.type in self.getProcessTypes():
            return True
        else:
            return False

    # check if it is a master input dataset
    def isMasterInput(self):
        if self.masterID is None and self.type in self.getInputTypes():
            return True
        else:
            return False

    # remove nosplit attribute
    def remAttribute(self, attrName):
        if self.attributes is not None:
            self.attributes = re.sub(attrName, "", self.attributes)
            self.attributes = re.sub(",,", ",", self.attributes)
            self.attributes = re.sub("^,", "", self.attributes)
            self.attributes = re.sub(",$", "", self.attributes)
            if self.attributes == "":
                self.attributes = None

    # remove nosplit attribute
    def remNoSplit(self):
        self.remAttribute("nosplit")

    # remove repeat attribute
    def remRepeat(self):
        self.remAttribute("repeat")

    # get the ratio to master
    def getRatioToMaster(self):
        try:
            tmpMatch = re.search("ratio=(\d+(\.\d+)*)", self.attributes)
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
    def getNumMultByRatio(self, num):
        # no split
        if self.isNoSplit():
            return None
        # get ratio
        ratioVal = self.getRatioToMaster()
        # integer or float
        if isinstance(ratioVal, int):
            retVal = num * ratioVal
        else:
            retVal = float(num) * ratioVal
            retVal = int(math.ceil(retVal))
        return retVal

    # unique map key for output
    def outputMapKey(self):
        mapKey = f"{self.datasetName}#{self.provenanceID}"
        return mapKey

    # unique map key
    def uniqueMapKey(self):
        mapKey = f"{self.datasetName}#{self.datasetID}"
        return mapKey

    # set offset
    def setOffset(self, offset):
        self.setDatasetAttribute(f"{self.attrToken['offset']}={offset}")

    # get offset
    def getOffset(self):
        if self.attributes is not None:
            tmpMatch = re.search(self.attrToken["offset"] + "=(\d+)", self.attributes)
            if tmpMatch is not None:
                offset = int(tmpMatch.group(1))
                return offset
        return 0

    # set number of records
    def setNumRecords(self, n):
        self.setDatasetAttribute(f"{self.attrToken['num_records']}={n}")

    # get number of records
    def getNumRecords(self):
        if self.attributes is not None:
            for item in self.attributes.split(","):
                tmpMatch = re.search(self.attrToken["num_records"] + "=(\d+)", item)
                if tmpMatch is not None:
                    num_records = int(tmpMatch.group(1))
                    return num_records
        return None

    # set object store
    def setObjectStore(self, objectStore):
        self.setDatasetAttribute(f"{self.attrToken['objectStore']}={objectStore}")

    # get object store
    def getObjectStore(self):
        if self.attributes is not None:
            tmpMatch = re.search(self.attrToken["objectStore"] + "=([^,]+)", self.attributes)
            if tmpMatch is not None:
                return tmpMatch.group(1)
        return None

    # set the number of files per job
    def setNumFilesPerJob(self, num):
        self.setDatasetAttribute(f"{self.attrToken['nFilesPerJob']}={num}")

    # get the number of files per job
    def getNumFilesPerJob(self):
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
    def toMerge(self):
        if self.type.startswith("trn_"):
            return True
        return False

    # set transient
    def setTransient(self, val):
        if val is True:
            val = 1
        else:
            val = 0
        self.setDatasetAttribute(f"{self.attrToken['transient']}={val}")

    # get transient
    def getTransient(self):
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
    def isAllowedNoOutput(self):
        if self.attributes is not None and self.attrToken["allowNoOutput"] in self.attributes.split(","):
            return True
        else:
            return False

    # allow no output
    def allowNoOutput(self):
        if self.attributes in [None, ""]:
            items = []
        else:
            items = self.attributes.split(",")
        if self.attrToken["allowNoOutput"] not in items:
            items.append(self.attrToken["allowNoOutput"])
            self.attributes = ",".join(items)

    # check if index consistency is required
    def indexConsistent(self):
        if self.attributes is not None and self.attrToken["indexConsistent"] in self.attributes.split(","):
            return True
        else:
            return False

    # set distributed
    def setDistributed(self):
        self.distributed = True

    # reset distributed
    def reset_distributed(self):
        self.distributed = False

    # check if distributed
    def isDistributed(self):
        return self.distributed

    # set event ratio
    def setEventRatio(self, num):
        self.setDatasetAttribute(f"{self.attrToken['eventRatio']}={num}")

    # get event ratio
    def getEventRatio(self):
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
    def setPseudo(self):
        if self.attributes in [None, ""]:
            items = []
        else:
            items = self.attributes.split(",")
        if self.attrToken["pseudo"] not in items:
            items.append(self.attrToken["pseudo"])
            self.attributes = ",".join(items)

    # merge only
    def is_merge_only(self):
        try:
            return self.attrToken["mergeOnly"] in self.attributes.split(",")
        except Exception:
            return False

    # sort files by old PandaIDs and move files with no PandaIDs to the end
    def sort_files_by_panda_ids(self):
        self.Files = sorted([f for f in self.Files if f.PandaID is not None], key=lambda x: x.PandaID) + [f for f in self.Files if f.PandaID is None]

    # set no staging
    def set_no_staging(self, value: bool):
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
