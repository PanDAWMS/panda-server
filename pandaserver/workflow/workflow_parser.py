import copy
import json
import os
import re
import shlex
import sys
import tempfile
import traceback

import requests
from idds.atlas.workflowv2.atlaslocalpandawork import ATLASLocalPandaWork
from idds.atlas.workflowv2.atlaspandawork import ATLASPandaWork
from idds.workflowv2.workflow import AndCondition, Condition, OrCondition, Workflow
from pandaclient import PhpoScript, PrunScript
from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandalogger.PandaLogger import PandaLogger
from ruamel.yaml import YAML

from pandaserver.srvcore.CoreUtils import clean_user_id, commands_get_status_output
from pandaserver.workflow import pcwl_utils, workflow_utils
from pandaserver.workflow.snakeparser import Parser

# supported workflow description languages
SUPPORTED_WORKFLOW_LANGUAGES = ["cwl", "snakemake"]

# main logger
logger = PandaLogger().getLogger(__name__.split(".")[-1])


# ==============================================================================
# Native PanDA workflow functions
# ==============================================================================


def json_serialize_default(obj):
    """
    Default JSON serializer for non-serializable objects of Node object

    Args:
        obj (Any): Object to serialize

    Returns:
        Any: JSON serializable object
    """
    # convert set to list
    if isinstance(obj, set):
        return list(obj)
    elif isinstance(obj, workflow_utils.Node):
        return obj.id
    return obj


def parse_raw_request(sandbox_url, log_token, user_name, raw_request_dict) -> tuple[bool, bool, dict]:
    """
    Parse raw request with files in sandbox into workflow definition

    Args:
        sandbox_url (str): URL to download sandbox
        log_token (str): Log token
        user_name (str): User name
        raw_request_dict (dict): Raw request dictionary

    Returns:
        bool: Whether the parsing is successful
        bool: Whether the failure is fatal
        dict: Workflow definition dictionary
    """
    tmp_log = LogWrapper(logger, log_token)
    is_ok = True
    is_fatal = False
    # request_id = None
    workflow_definition_dict = dict()
    try:
        # go to temp dir
        cur_dir = os.getcwd()
        with tempfile.TemporaryDirectory() as tmp_dirname:
            os.chdir(tmp_dirname)
            # download sandbox
            tmp_log.info(f"downloading sandbox from {sandbox_url}")
            with requests.get(sandbox_url, allow_redirects=True, verify=False, stream=True) as r:
                if r.status_code == 400:
                    tmp_log.error("not found")
                    is_fatal = True
                    is_ok = False
                elif r.status_code != 200:
                    tmp_log.error(f"bad HTTP response {r.status_code}")
                    is_ok = False
                # extract sandbox
                if is_ok:
                    with open(raw_request_dict["sandbox"], "wb") as fs:
                        for chunk in r.raw.stream(1024, decode_content=False):
                            if chunk:
                                fs.write(chunk)
                        fs.close()
                        tmp_stat, tmp_out = commands_get_status_output(f"tar xvfz {raw_request_dict['sandbox']}")
                        if tmp_stat != 0:
                            tmp_log.error(tmp_out)
                            dump_str = f"failed to extract {raw_request_dict['sandbox']}"
                            tmp_log.error(dump_str)
                            is_fatal = True
                            is_ok = False
                # parse workflow files
                if is_ok:
                    tmp_log.info("parse workflow")
                    workflow_name = None
                    if (wf_lang := raw_request_dict["language"]) in SUPPORTED_WORKFLOW_LANGUAGES:
                        if wf_lang == "cwl":
                            workflow_name = raw_request_dict.get("workflow_name")
                            nodes, root_in = pcwl_utils.parse_workflow_file(raw_request_dict["workflowSpecFile"], tmp_log)
                            with open(raw_request_dict["workflowInputFile"]) as workflow_input:
                                yaml = YAML(typ="safe", pure=True)
                                data = yaml.load(workflow_input)
                        elif wf_lang == "snakemake":
                            parser = Parser(raw_request_dict["workflowSpecFile"], logger=tmp_log)
                            nodes, root_in = parser.parse_nodes()
                            data = dict()
                        # resolve nodes
                        s_id, t_nodes, nodes = workflow_utils.resolve_nodes(nodes, root_in, data, 0, set(), raw_request_dict["outDS"], tmp_log)
                        workflow_utils.set_workflow_outputs(nodes)
                        id_node_map = workflow_utils.get_node_id_map(nodes)
                        [node.resolve_params(raw_request_dict["taskParams"], id_node_map) for node in nodes]
                        dump_str = "the description was internally converted as follows\n" + workflow_utils.dump_nodes(nodes)
                        tmp_log.info(dump_str)
                        for node in nodes:
                            s_check, o_check = node.verify()
                            tmp_str = f"Verification failure in ID:{node.id} {o_check}"
                            if not s_check:
                                tmp_log.error(tmp_str)
                                dump_str += tmp_str
                                dump_str += "\n"
                                is_fatal = True
                                is_ok = False
                    else:
                        dump_str = "{} is not supported to describe the workflow"
                        tmp_log.error(dump_str)
                        is_fatal = True
                        is_ok = False
                # genertate workflow definition
                if is_ok:
                    # root inputs
                    root_inputs_dict = dict()
                    for k in root_in:
                        kk = k.split("#")[-1]
                        if kk in data:
                            root_inputs_dict[k] = data[kk]
                    # root outputs
                    root_outputs_dict = dict()
                    nodes_list = []
                    # nodes
                    for node in nodes:
                        nodes_list.append(vars(node))
                        if node.is_tail:
                            root_outputs_dict.update(node.outputs)
                            for out_val in root_outputs_dict.values():
                                out_val["output_types"] = node.output_types
                    # workflow definition
                    workflow_definition_dict = {
                        "workflow_name": workflow_name,
                        "user_name": user_name,
                        "root_inputs": root_inputs_dict,
                        "root_outputs": root_outputs_dict,
                        "nodes": nodes_list,
                    }
        os.chdir(cur_dir)
    except Exception as e:
        is_ok = False
        is_fatal = True
        tmp_log.error(f"failed to run with {str(e)} {traceback.format_exc()}")

    # with tempfile.NamedTemporaryFile(delete=False, mode="w") as tmp_json:
    #     json.dump([is_ok, is_fatal, request_id, tmp_log.dumpToString()], tmp_json)
    #     print(tmp_json.name)

    return is_ok, is_fatal, workflow_definition_dict
