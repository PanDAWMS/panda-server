import json
import re


# convert UTF-8 to ASCII in json dumps
def unicodeConvert(input):
    if isinstance(input, dict):
        retMap = {}
        for tmpKey, tmpVal in input.items():
            retMap[unicodeConvert(tmpKey)] = unicodeConvert(tmpVal)
        return retMap
    elif isinstance(input, list):
        retList = []
        for tmpItem in input:
            retList.append(unicodeConvert(tmpItem))
        return retList

    return input


# decode
def decodeJSON(inputStr):
    return json.loads(inputStr, object_hook=unicodeConvert)


# encode
def encodeJSON(inputMap):
    return json.dumps(inputMap)


# extract stream name
def extractStreamName(valStr):
    tmpMatch = re.search("\$\{([^\}]+)\}", valStr)
    if tmpMatch is None:
        return None
    # remove decorators
    streamName = tmpMatch.group(1)
    streamName = streamName.split("/")[0]
    return streamName


# extract output filename template and replace the value field
def extractReplaceOutFileTemplate(valStr, streamName):
    outFileTempl = valStr.split("=")[-1]
    outFileTempl = outFileTempl.replace("'", "")
    valStr = valStr.replace(outFileTempl, f"${{{streamName}}}")
    return outFileTempl, valStr


# extract file list
def extractFileList(taskParamMap, datasetName):
    baseDatasetName = datasetName.split(":")[-1]
    if "log" in taskParamMap:
        itemList = taskParamMap["jobParameters"] + [taskParamMap["log"]]
    else:
        itemList = taskParamMap["jobParameters"]
    fileList = []
    includePatt = []
    excludePatt = []
    for tmpItem in itemList:
        if (
            tmpItem["type"] == "template"
            and "dataset" in tmpItem
            and (
                (tmpItem["dataset"] == datasetName or tmpItem["dataset"] == baseDatasetName)
                or ("expandedList" in tmpItem and (datasetName in tmpItem["expandedList"] or baseDatasetName in tmpItem["expandedList"]))
            )
        ):
            if "files" in tmpItem:
                fileList = tmpItem["files"]
            if "include" in tmpItem:
                includePatt = tmpItem["include"].split(",")
            if "exclude" in tmpItem:
                excludePatt = tmpItem["exclude"].split(",")
    return fileList, includePatt, excludePatt


# append dataset
def appendDataset(taskParamMap, datasetSpec, fileList):
    # make item for dataset
    tmpItem = {}
    tmpItem["type"] = "template"
    tmpItem["value"] = ""
    tmpItem["dataset"] = datasetSpec.datasetName
    tmpItem["param_type"] = datasetSpec.type
    if fileList != []:
        tmpItem["files"] = fileList
    # append
    if "jobParameters" not in taskParamMap:
        taskParamMap["jobParameters"] = []
    taskParamMap["jobParameters"].append(tmpItem)
    return taskParamMap


# check if use random seed
def useRandomSeed(taskParamMap):
    for tmpItem in taskParamMap["jobParameters"]:
        if "value" in tmpItem:
            # get offset for random seed
            if tmpItem["type"] == "template" and tmpItem["param_type"] == "number":
                if "${RNDMSEED}" in tmpItem["value"]:
                    return True
    return False


# get initial global share
def get_initial_global_share(task_buffer, task_id, task_spec=None, task_param_map=None):
    """
    Get the initial global share for a task
    :param task_buffer: task buffer interface
    :param task_id: task ID
    :param task_spec: task specification object. read through task_buffer if None
    :param task_param_map: task parameter map. read through task_buffer if None
    :return:
    """
    if task_param_map is None:
        # get task parameters from DB
        tmp_str = task_buffer.getTaskParamsWithID_JEDI(task_id)
        task_param_map = decodeJSON(tmp_str)
    if "gshare" in task_param_map and task_buffer.is_valid_share(task_param_map["gshare"]):
        # global share was already specified in ProdSys
        gshare = task_param_map["gshare"]
    else:
        if task_spec is None:
            # get task specification from DB
            _, task_spec = task_buffer.getTaskWithID_JEDI(task_id)
        # get share based on definition
        gshare = task_buffer.get_share_for_task(task_spec)
        if gshare == "Undefined":
            error_message = "task definition does not match any global share"
            raise RuntimeError(error_message)
    return gshare


# get sandbox name
def get_sandbox_name(task_param_map: dict) -> str | None:
    """
    Get the sandbox name from the task parameters
    :param task_param_map: dictionary of task parameters
    :return: sandbox name or None
    """
    sandbox_name = None
    if "fixedSandbox" in task_param_map:
        sandbox_name = task_param_map["fixedSandbox"]
    elif "buildSpec" in task_param_map:
        sandbox_name = task_param_map["buildSpec"]["archiveName"]
    else:
        for tmpParam in task_param_map["jobParameters"]:
            if tmpParam["type"] == "constant":
                m = re.search("^-a ([^ ]+)$", tmpParam["value"])
                if m is not None:
                    sandbox_name = m.group(1)
                    break
    return sandbox_name
