import json
import sys

# special permission
PERMISSION_KEY = "k"
PERMISSION_PROXY = "p"
PERMISSION_TOKEN_KEY = "t"
PERMISSION_SUPER_USER = "s"
PERMISSION_SUPER_GROUP = "g"


# convert UTF-8 to ASCII in json dumps
def unicodeConvert(input):
    if isinstance(input, dict):
        retMap = {}
        for tmpKey in input:
            tmpVal = input[tmpKey]
            retMap[unicodeConvert(tmpKey)] = unicodeConvert(tmpVal)
        return retMap
    elif isinstance(input, list):
        retList = []
        for tmpItem in input:
            retList.append(unicodeConvert(tmpItem))
        return retList
    elif isinstance(input, str) and sys.version_info[0] == 2:
        return input.encode("utf-8")
    return input


# decode
def decodeJSON(inputStr):
    return json.loads(inputStr, object_hook=unicodeConvert)


# calculate priority for user jobs
def calculatePriority(priorityOffset, serNum, weight):
    priority = int(1000 + priorityOffset - (serNum / 5) - int(100 * weight))
    return priority
