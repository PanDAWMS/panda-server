import copy
import json
import os
import re
import shlex
import sys
import tarfile
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

# from pandaserver.srvcore.CoreUtils import clean_user_id
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

    def _is_within_directory(base_dir: str, target_path: str) -> bool:
        abs_base_dir = os.path.abspath(base_dir)
        abs_target_path = os.path.abspath(target_path)
        return os.path.commonpath([abs_base_dir, abs_target_path]) == abs_base_dir

    def _safe_extract_tar_gz(tar_path: str, extract_dir: str):
        with tarfile.open(tar_path, mode="r:gz") as tar:
            members = tar.getmembers()
            for member in members:
                member_name = member.name
                normalized_name = os.path.normpath(member_name)
                # security checks for tar member name
                if os.path.isabs(member_name):
                    raise ValueError(f"absolute path in tar member is not allowed: {member_name}")
                if normalized_name in ("", ".", "..") or normalized_name.startswith(".." + os.path.sep):
                    raise ValueError(f"path traversal in tar member is not allowed: {member_name}")
                if member.issym() or member.islnk():
                    raise ValueError(f"links in tar archive are not allowed: {member_name}")
                if member.ischr() or member.isblk() or member.isfifo():
                    raise ValueError(f"special file in tar archive is not allowed: {member_name}")
                # check that the extraction target is within the extract_dir
                extraction_target = os.path.join(extract_dir, normalized_name)
                if not _is_within_directory(extract_dir, extraction_target):
                    raise ValueError(f"tar member extracts outside target directory: {member_name}")
            # all checks passed, safe to extract
            tar.extractall(path=extract_dir, members=members)

    try:
        # use an isolated temp dir without changing process cwd
        with tempfile.TemporaryDirectory() as tmp_dirname:
            # download sandbox
            tmp_log.info(f"downloading sandbox from {sandbox_url}")
            with requests.get(sandbox_url, allow_redirects=True, stream=True) as r:
                if r.status_code == 400:
                    tmp_log.error("not found")
                    is_fatal = True
                    is_ok = False
                elif r.status_code != 200:
                    tmp_log.error(f"bad HTTP response {r.status_code}")
                    is_ok = False
                # validate sandbox filename
                sandbox_name = raw_request_dict.get("sandbox")
                if is_ok:
                    if not isinstance(sandbox_name, str):
                        tmp_log.error("sandbox filename is missing or not a string")
                        is_fatal = True
                        is_ok = False
                    else:
                        # sandbox filename must not contain any path separators
                        seps = [os.path.sep]
                        if os.path.altsep:
                            seps.append(os.path.altsep)
                        if any(sep in sandbox_name for sep in seps):
                            tmp_log.error("sandbox filename must not contain path separators")
                            is_fatal = True
                            is_ok = False
                        else:
                            sandbox_name = os.path.basename(sandbox_name)
                # extract sandbox
                if is_ok:
                    sandbox_path = os.path.join(tmp_dirname, sandbox_name)
                    with open(sandbox_path, "wb") as fs:
                        for chunk in r.raw.stream(1024, decode_content=False):
                            if chunk:
                                fs.write(chunk)
                        fs.close()
                        try:
                            _safe_extract_tar_gz(sandbox_path, tmp_dirname)
                        except Exception as e:
                            dump_str = f"failed to extract {sandbox_name}: {traceback.format_exc()}"
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
                            workflow_spec_file = os.path.join(tmp_dirname, raw_request_dict["workflowSpecFile"])
                            workflow_input_file = os.path.join(tmp_dirname, raw_request_dict["workflowInputFile"])
                            nodes, root_in = pcwl_utils.parse_workflow_file(workflow_spec_file, tmp_log)
                            with open(workflow_input_file) as workflow_input:
                                yaml = YAML(typ="safe", pure=True)
                                data = yaml.load(workflow_input)
                        elif wf_lang == "snakemake":
                            workflow_spec_file = os.path.join(tmp_dirname, raw_request_dict["workflowSpecFile"])
                            parser = Parser(workflow_spec_file, logger=tmp_log)
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
                        dump_str = f"{wf_lang} is not supported to describe the workflow"
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
    except Exception as e:
        is_ok = False
        is_fatal = True
        tmp_log.error(f"failed to run with {str(e)} {traceback.format_exc()}")

    # with tempfile.NamedTemporaryFile(delete=False, mode="w") as tmp_json:
    #     json.dump([is_ok, is_fatal, request_id, tmp_log.dumpToString()], tmp_json)
    #     print(tmp_json.name)

    return is_ok, is_fatal, workflow_definition_dict
