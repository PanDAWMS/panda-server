# split rules
split_rule_dict = {
    "allowEmptyInput": "AE",
    "allowIncompleteInDS": "AI",
    "addNthFieldToLFN": "AN",
    "allowPartialFinish": "AP",
    "altStageOut": "AT",
    "avoidVP": "AV",
    "maxCoreCount": "CC",
    "cloudAsVO": "CV",
    "ddmBackEnd": "DE",
    "disableAutoFinish": "DF",
    "disableReassign": "DI",
    "debugMode": "DM",
    "disableAutoRetry": "DR",
    "dynamicNumEvents": "DY",
    "nEsConsumers": "EC",
    "nEventsPerInput": "EI",
    "encJobParams": "EJ",
    "nEventsPerWorker": "ES",
    "firstContentsFeed": "FC",
    "failGoalUnreached": "FG",
    "fineGrainedProc": "FP",
    "firstEvent": "FT",
    "fullChain": "FU",
    "groupBoundaryID": "GB",
    "hpoWorkflow": "HO",
    "instantiateTmplSite": "IA",
    "inFilePosEvtNum": "IF",
    "ipStack": "IK",
    "allowInputLAN": "IL",
    "ignoreMissingInDS": "IM",
    "intermediateTask": "IN",
    "ipConnectivity": "IP",
    "inputPreStaging": "IS",
    "instantiateTmpl": "IT",
    "allowInputWAN": "IW",
    "noLoopingCheck": "LC",
    "useLocalIO": "LI",
    "limitedSites": "LS",
    "loadXML": "LX",
    "minCpuEfficiency": "MC",
    "messageDriven": "MD",
    "mergeEsOnOS": "ME",
    "nMaxFilesPerJob": "MF",
    "maxJumboPerSite": "MJ",
    "maxNumJobs": "MN",
    "mergeOutput": "MO",
    "multiStepExec": "MS",
    "maxWalltime": "MW",
    "maxEventsPerJob": "MX",
    "noExecStrCnv": "NC",
    "notDiscardEvents": "ND",
    "nEventsPerJob": "NE",
    "nFilesPerJob": "NF",
    "nGBPerJob": "NG",
    "noInputPooling": "NI",
    "nJumboJobs": "NJ",
    "nSitesPerJob": "NS",
    "nChunksToWait": "NT",
    "noWaitParent": "NW",
    "orderInputBy": "OI",
    "orderByLB": "OL",
    "onSiteMerging": "OM",
    "osMatching": "OS",
    "onlyTagsForFC": "OT",
    "pushStatusChanges": "PC",
    "pushJob": "PJ",
    "pfnList": "PL",
    "putLogToOS": "PO",
    "runUntilClosed": "RC",
    "registerDatasets": "RD",
    "registerEsFiles": "RE",
    "respectLB": "RL",
    "retryModuleRules": "RM",
    "reuseSecOnDemand": "RO",
    "releasePerLB": "RP",
    "respectSplitRule": "RR",
    "randomSeed": "RS",
    "retryRamOffset": "RX",
    "retryRamStep": "RY",
    "resurrectConsumers": "SC",
    "switchEStoNormal": "SE",
    "stayOutputOnSite": "SO",
    "scoutSuccessRate": "SS",
    "useSecrets": "ST",
    "segmentedWork": "SW",
    "totNumJobs": "TJ",
    "tgtMaxOutputForNG": "TN",
    "t1Weight": "TW",
    "useBuild": "UB",
    "useJobCloning": "UC",
    "useRealNumEvents": "UE",
    "useFileAsSourceLFN": "UF",
    "usePrePro": "UP",
    "useScout": "US",
    "usePrefetcher": "UT",
    "useExhausted": "UX",
    "useZipToPin": "UZ",
    "writeInputToFile": "WF",
    "waitInput": "WI",
    "maxAttemptES": "XA",
    "decAttOnFailedES": "XF",
    "maxAttemptEsJob": "XJ",
    "nEventsPerMergeJob": "ZE",
    "nFilesPerMergeJob": "ZF",
    "nGBPerMergeJob": "ZG",
    "nMaxFilesPerMergeJob": "ZM",
}


# extract rules
def extract_rule_values(split_rules, rule_names, is_sub_rule=False):
    """
    Extract rule values from split rule string
    :param split_rules: comma separated string
    :param rule_names: list of rule names
    :param is_sub_rule: bool to indicate if the rule is a sub-rule
    :return: dict of rule names and values
    """
    if split_rules is None:
        split_rules = ""
    ret = {}
    if is_sub_rule:
        rule_separator = "|"
        key_value_separator = ":"
    else:
        rule_separator = ","
        key_value_separator = "="
    for tmp_rule in split_rules.split(rule_separator):
        for tmp_name in rule_names:
            if tmp_name in split_rule_dict and tmp_rule.startswith(split_rule_dict[tmp_name] + key_value_separator):
                ret[tmp_name] = tmp_rule.split(key_value_separator)[-1]
                break
    for tmp_name in rule_names:
        if tmp_name not in ret:
            ret[tmp_name] = None
    return ret


# replace a rule
def replace_rule(split_rules, rule_name, rule_value, is_sub_rule=False):
    """
    Replace a rule in the split rule string
    :param split_rules: comma separated string
    :param rule_name: rule name
    :param rule_value: rule value
    :param is_sub_rule: bool to indicate if the rule is a sub-rule
    :return: string of split rules
    """
    if rule_name not in split_rule_dict:
        return split_rules
    if split_rules is None:
        split_rules = ""
    if is_sub_rule:
        rule_separator = "|"
        key_value_separator = ":"
    else:
        rule_separator = ","
        key_value_separator = "="
    tmp_str = ""
    for tmp_rule in split_rules.split(rule_separator):
        if tmp_rule.startswith(split_rule_dict[rule_name] + key_value_separator):
            continue
        if tmp_str != "":
            tmp_str += rule_separator
        tmp_str += tmp_rule
    if tmp_str != "":
        tmp_str += rule_separator
    tmp_str += split_rule_dict[rule_name] + key_value_separator + str(rule_value)
    return tmp_str


# remove a rule
def remove_rule(split_rules, rule_token, is_sub_rule=False):
    """
    Remove a rule from the split rule string
    :param split_rules: comma separated string
    :param rule_token: rule token
    :param is_sub_rule: bool to indicate if the rule is a sub-rule
    :return: string of split rules
    """
    if split_rules is None:
        split_rules = ""
    if is_sub_rule:
        rule_separator = "|"
        key_value_separator = ":"
    else:
        rule_separator = ","
        key_value_separator = "="
    tmp_str = ""
    for tmp_rule in split_rules.split(rule_separator):
        if tmp_rule.startswith(rule_token + key_value_separator):
            continue
        if tmp_str != "":
            tmp_str += rule_separator
        tmp_str += tmp_rule
    return tmp_str
