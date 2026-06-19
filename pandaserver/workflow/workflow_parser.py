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
from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandalogger.PandaLogger import PandaLogger
from ruamel.yaml import YAML

# from pandaserver.srvcore.CoreUtils import clean_user_id
from pandaserver.workflow import pcwl_utils, workflow_native_utils
from pandaserver.workflow.snakeparser import Parser as SnakeParser

# supported workflow description languages
SUPPORTED_WORKFLOW_LANGUAGES = ["yaml", "cwl", "snakemake"]

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
    elif isinstance(obj, workflow_native_utils.Node):
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
                    workflow_options = None
                    if (wf_lang := raw_request_dict["language"]) in SUPPORTED_WORKFLOW_LANGUAGES:
                        if wf_lang == "yaml":
                            workflow_spec_file = os.path.join(tmp_dirname, raw_request_dict["workflowSpecFile"])
                            with open(workflow_spec_file) as workflow_spec:
                                yaml = YAML(typ="safe", pure=True)
                                wfd = yaml.load(workflow_spec)
                            workflow_name = wfd.get("name")
                            id_counter = [0]
                            nodes, root_in = workflow_native_utils.parse_workflow_data(wfd, tmp_log, _id_counter=id_counter)
                            data = wfd.get("inputs", dict())
                            workflow_options = wfd.get("options", None)
                            # Resolve reference-based sub-workflow nodes (workflow_ref field)
                            named_blocks = wfd.get("workflow_blocks", {})
                            for node in list(nodes):
                                if node.workflow_ref is None:
                                    continue
                                ref = node.workflow_ref
                                ref_data = None
                                # named block in the same file
                                if ref in named_blocks:
                                    ref_data = named_blocks[ref]
                                else:
                                    # external YAML file in the same sandbox directory
                                    ref_path = os.path.join(tmp_dirname, ref)
                                    if os.path.isfile(ref_path):
                                        with open(ref_path) as ref_file:
                                            yaml2 = YAML(typ="safe", pure=True)
                                            ref_data = yaml2.load(ref_file)
                                    else:
                                        tmp_log.error(f"workflow_ref '{ref}' not found as a named block or file")
                                        is_fatal = True
                                        is_ok = False
                                        break
                                if ref_data is not None:
                                    child_nodes, _ = workflow_native_utils.parse_workflow_data(ref_data, tmp_log, _id_counter=id_counter)
                                    # Keep the child template nodes as Node objects on sub_nodes (a
                                    # topologically-sorted list, not flattened into the outer node
                                    # list) so resolve_nodes can resolve them in their own recursive
                                    # scope -- restarting member_id at 1 -- and splice them back as a
                                    # flat, id-keyed list. resolve_nodes replaces sub_nodes with the
                                    # resolved child ids.
                                    node.sub_nodes = child_nodes
                                    node.workflow_ref = None
                                    # child nodes are template nodes within the scatter parent; clear is_tail
                                    # so they do not appear as tail nodes of the outer workflow
                                    for child_node in child_nodes:
                                        child_node.is_tail = False
                                    # Stash the raw root_outputs from the child YAML so they can be
                                    # resolved to actual values after resolve_nodes runs (output
                                    # dataset names are not set until resolve_nodes assigns IDs).
                                    node.child_root_outputs_raw = ref_data.get("outputs", {}) if isinstance(ref_data, dict) else {}
                                    # Resolve scatter_inputs name references to actual value lists
                                    if node.scatter_inputs:
                                        resolved = {}
                                        for param_name, root_input_ref in node.scatter_inputs.items():
                                            if root_input_ref in data:
                                                val = data[root_input_ref]
                                                resolved[param_name] = val if isinstance(val, list) else [val]
                                            else:
                                                tmp_log.warning(f"scatter_inputs ref '{root_input_ref}' not found in workflow inputs for node '{node.name}'")
                                        node.scatter_inputs = resolved
                        elif wf_lang == "cwl":
                            workflow_name = raw_request_dict.get("workflow_name")
                            workflow_spec_file = os.path.join(tmp_dirname, raw_request_dict["workflowSpecFile"])
                            workflow_input_file = os.path.join(tmp_dirname, raw_request_dict["workflowInputFile"])
                            nodes, root_in = pcwl_utils.parse_workflow_file(workflow_spec_file, tmp_log)
                            with open(workflow_input_file) as workflow_input:
                                yaml = YAML(typ="safe", pure=True)
                                data = yaml.load(workflow_input)
                        elif wf_lang == "snakemake":
                            workflow_spec_file = os.path.join(tmp_dirname, raw_request_dict["workflowSpecFile"])
                            parser = SnakeParser(workflow_spec_file, logger=tmp_log)
                            nodes, root_in = parser.parse_nodes()
                            data = dict()
                        # resolve nodes
                        s_id, t_nodes, nodes = workflow_native_utils.resolve_nodes(nodes, root_in, data, 0, set(), raw_request_dict["outDS"], tmp_log)
                        workflow_native_utils.set_workflow_outputs(nodes)
                        id_node_map = workflow_native_utils.get_node_id_map(nodes)
                        [node.resolve_params(raw_request_dict["taskParams"], id_node_map) for node in nodes]
                        # Resolve child_root_outputs_raw now that resolve_nodes has set output values
                        # and resolve_params has set output_types on all nodes.
                        # Build a map from step-output-name (e.g. "combine/outDS") to resolved output dict.
                        node_out_map = {}
                        for _n in nodes:
                            for _out_name, _out_data in (_n.outputs or {}).items():
                                node_out_map[_out_name] = _out_data
                        for _n in nodes:
                            if getattr(_n, "child_root_outputs_raw", None):
                                _resolved = {}
                                for _rout_name, _rout_spec in _n.child_root_outputs_raw.items():
                                    if isinstance(_rout_spec, dict):
                                        _from_key = _rout_spec.get("from")
                                        _from_data = node_out_map.get(_from_key, {}) if _from_key else {}
                                        _resolved[_rout_name] = {
                                            "value": _from_data.get("value") if isinstance(_from_data, dict) else None,
                                            "output_types": _rout_spec.get("output_types") or [],
                                        }
                                _n.child_root_outputs = _resolved
                        dump_str = "the description was internally converted as follows\n" + workflow_native_utils.dump_nodes(nodes)
                        tmp_log.info(dump_str)
                        # scatter template child nodes have unresolved scatter-parameter inputs
                        # (e.g. {signal}, {background}) that are filled in at runtime — skip them
                        scatter_template_ids = set()
                        for node in nodes:
                            if node.scatter_inputs is not None:
                                scatter_template_ids |= node.sub_nodes
                        for node in nodes:
                            if node.id in scatter_template_ids:
                                continue
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
                        # Stored so scatter grandchild dispatch can uniquify output dataset names
                        # per iteration (all names share this prefix; see submit_sub_workflow).
                        "out_ds_name": raw_request_dict.get("outDS"),
                    }
                    if workflow_options is not None:
                        workflow_definition_dict["options"] = workflow_options
    except Exception as e:
        is_ok = False
        is_fatal = True
        tmp_log.error(f"failed to run with {str(e)} {traceback.format_exc()}")

    # with tempfile.NamedTemporaryFile(delete=False, mode="w") as tmp_json:
    #     json.dump([is_ok, is_fatal, request_id, tmp_log.dumpToString()], tmp_json)
    #     print(tmp_json.name)

    return is_ok, is_fatal, workflow_definition_dict
