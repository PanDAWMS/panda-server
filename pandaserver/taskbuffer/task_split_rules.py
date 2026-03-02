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
    "noAutoPause": "NP",
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

# changeable split rules
changeable_split_rule_names = [
    "allowIncompleteInDS",
    "t1Weight",
    "nEsConsumers",
    "nMaxFilesPerJob",
    "nGBPerJob",
    "noInputPooling",
    "nFilesPerJob",
    "nEventsPerWorker",
    "nJumboJobs",
    "avoidVP",
    "allowInputLAN",
    "useLocalIO",
    "noLoopingCheck",
    "maxCoreCount",
    "onlyTagsForFC",
    "useZipToPin",
    "ignoreMissingInDS",
    "noAutoPause",
]

changeable_split_rule_tags = [split_rule_dict[tmp_name] for tmp_name in changeable_split_rule_names]


# extract rules
def extract_rule_values(split_rules: str, rule_names: list, is_sub_rule: bool = False) -> dict:
    """
    Extract rule values from split rule string

    :param split_rules: a comma separated string
    :param rule_names: a list of rule names
    :param is_sub_rule: True to indicate the rule is a subrule
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
def replace_rule(split_rules: str, rule_name: str, rule_value: int | str, is_sub_rule=False):
    """
    Replace a rule in the split rule string

    :param split_rules: s comma separated string
    :param rule_name: rule name
    :param rule_value: rule value
    :param is_sub_rule: True to indicate the rule is a subrule
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
def remove_rule(split_rules: str, rule_token: str, is_sub_rule: bool = False):
    """
    Remove a rule from the split rule string

    :param split_rules: s comma separated string
    :param rule_token: rule token
    :param is_sub_rule: TRue to indicate the rule is a subrule
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


# decode split rule string: replace two-letter tags with human-readable names
def decode_split_rule(split_rules: str) -> str:
    """
    Convert a compact split-rule string into a human-readable one by replacing
    two-letter tags with their full names from split_rule_dict.

    The input is a comma-separated string of ``TAG=value`` pairs, e.g.::

        "AE=1,AN=1,2,3,AI=2"

    Special case: ``AN`` (addNthFieldToLFN) may carry a comma-separated list
    as its value (e.g. ``AN=1,2,3``).  All other tags treat ``=`` as the sole
    separator and never embed commas in their values.

    :param split_rules: compact comma-separated split-rule string, or None/empty
    :return: human-readable comma-separated string, e.g.
             ``"allowEmptyInput=1,addNthFieldToLFN=1,2,3,allowIncompleteInDS=2"``
             Unknown tags are kept as-is.
    """
    if not split_rules:
        return split_rules or ""

    # build reverse map: two-letter tag -> full name
    tag_to_name = {tag: name for name, tag in split_rule_dict.items()}

    # The AN tag is the only one whose value may contain commas.
    an_tag = split_rule_dict["addNthFieldToLFN"]  # "AN"

    # Split on commas, then re-group tokens into (tag, value) pairs.
    # A token starts a new rule when it matches "XX=..." where XX is a known
    # two-letter tag.  Otherwise, it is appended to the current rule's value.
    tokens = split_rules.split(",")
    rules = []  # list of [tag, value_parts...]
    for token in tokens:
        # Check whether this token begins a new TAG=value entry
        eq_pos = token.find("=")
        if eq_pos == 2:
            candidate_tag = token[:2]
            if candidate_tag in tag_to_name or candidate_tag == an_tag:
                # Start a new rule
                rules.append([candidate_tag, token[3:]])  # token[3:] = value after "XX="
                continue
        # Not a recognized new rule:
        # - if there are existing rules and the last one is AN, treat as value continuation
        # - otherwise keep the token verbatim as its own entry
        if rules and rules[-1][0] == an_tag:
            rules[-1].append(token)
        else:
            # unknown / non-standard token — keep as-is using a sentinel
            rules.append([None, token])

    # Reconstruct using full names; rejoin multipart AN values with commas
    result_parts = []
    for parts in rules:
        tag = parts[0]
        if tag is None:
            # unknown / non-standard token kept verbatim
            result_parts.append(parts[1])
        else:
            value = ",".join(parts[1:])
            name = tag_to_name.get(tag, tag)  # fall back to raw tag if unknown
            result_parts.append(f"{name}={value}")

    return ",".join(result_parts)


# remove a rule with name
def remove_rule_with_name(split_rules: str, rule_name: str, is_sub_rule: bool = False) -> str:
    """
    Remove a rule from the split rule string using the rule name

    :param split_rules: a comma separated string
    :param rule_name: rule name
    :param is_sub_rule: True to indicate the rule is a subrule
    :return: string of split rules
    """
    split_rules = remove_rule(split_rules, split_rule_dict[rule_name], is_sub_rule)
    return split_rules
