import copy
import json
import re
import shlex
import tempfile

from pandaclient import PhpoScript, PrunScript

from pandaserver.workflow.workflow_base import TASKID_PLACEHOLDER

# Step types whose task parameters are supplied verbatim by the author instead of being
# generated from a command line. See make_task_params / verify_task_params.
RAW_TASK_PARAMS_STEP_TYPES = ("task",)

# Job-parameter param_type values whose dataset field names a real DDM collection and can
# therefore take part in the workflow data graph. "pseudo_input" is deliberately excluded:
# its dataset is a JEDI-internal pseudo collection (e.g. "seq_number") which does not exist
# in DDM, and treating it as an input would leave the workflow waiting for it forever.
DATA_INPUT_PARAM_TYPES = ("input",)

# A dataset field written wholly inside braces is a reference rather than a literal dataset
# name: "{workflow_input}" points at a workflow-level input, "{step_name/output_key}" at
# another step's output. Anything else is passed through to JEDI untouched.
RE_DATASET_REFERENCE = re.compile(r"^\{([^{}]+)\}$")

# Task parameter keys which every raw-task-params step must define
REQUIRED_TASK_PARAM_KEYS = ("taskName", "jobParameters", "log", "transPath", "vo", "prodSourceLabel")


def extract_dataset_reference(dataset):
    """
    Extract the reference target from a dataset field, if it is a reference

    Args:
        dataset (Any): Value of a job parameter's dataset field

    Returns:
        str | None: The referenced name without braces, or None if this is a literal dataset name
    """
    if not isinstance(dataset, str):
        return None
    match = RE_DATASET_REFERENCE.match(dataset.strip())
    return match.group(1) if match else None


def extract_job_param_option(job_param):
    """
    Extract the leading command line option name from a job parameter's value

    Args:
        job_param (dict): Job parameter dictionary

    Returns:
        str | None: Option name without leading dashes, e.g. "outputDAOD_PHYSFile"; None if absent
    """
    value = job_param.get("value")
    if not isinstance(value, str):
        return None
    match = re.match(r"^--?([A-Za-z0-9_]+)", value.strip())
    return match.group(1) if match else None


def derive_output_key(job_param, output_overrides=None):
    """
    Derive the short output key of an output job parameter

    The key is taken from the leading command line option with the "output" prefix and "File"
    suffix stripped, so --outputDAOD_PHYSFile becomes DAOD_PHYS. An explicit override wins, which
    lets merge steps expose e.g. --outputHITS_MRGFile as the plainer "HITS".

    Args:
        job_param (dict): Job parameter dictionary with param_type "output"
        output_overrides (dict | None): Map of desired key to the option name it refers to

    Returns:
        str | None: The output key, or None if no option name could be extracted
    """
    option = extract_job_param_option(job_param)
    if option is None:
        return None
    # an explicit override takes precedence over the derived name
    for key, overridden_option in (output_overrides or {}).items():
        if isinstance(overridden_option, str) and overridden_option.lstrip("-") == option:
            return key
    match = re.match(r"^output(.+?)File$", option)
    if match:
        return match.group(1)
    return option


def build_task_step_outputs(step_name, task_params, output_overrides, log_stream):
    """
    Build the outputs of a raw-task-params step from its output job parameters

    One workflow output is registered per output job parameter, so a step producing several
    datasets (e.g. DAOD_PHYS and DAOD_PHYSLITE) exposes each of them independently and downstream
    steps can consume whichever they need. The dataset name is taken verbatim from the author's
    task parameters, since it encodes the physics and any late-bound ID placeholder.

    Args:
        step_name (str): Name of the step
        task_params (dict): Raw task parameters of the step
        output_overrides (dict | None): Optional map of desired output key to the option it refers to
        log_stream: Logger

    Returns:
        dict: Map of "{step_name}/{output_key}" to a dict holding the output dataset name
    """
    outputs = {}
    for job_param in task_params.get("jobParameters") or []:
        if not isinstance(job_param, dict) or job_param.get("param_type") != "output":
            continue
        output_key = derive_output_key(job_param, output_overrides)
        if output_key is None:
            # verify_task_params reports this as a fatal error; keep parsing to collect them all
            log_stream.warning(f"cannot derive an output key from {job_param.get('value')} in step {step_name}")
            continue
        full_name = f"{step_name}/{output_key}"
        if full_name in outputs:
            log_stream.warning(f"duplicated output key {output_key} in step {step_name}; disambiguating")
            suffix = 2
            while f"{full_name}_{suffix}" in outputs:
                suffix += 1
            full_name = f"{full_name}_{suffix}"
        outputs[full_name] = {"value": job_param.get("dataset")}
    return outputs


def build_task_step_inputs(step_name, task_params, log_stream):
    """
    Build the inputs of a raw-task-params step from its input job parameters

    Only dataset fields written as references take part in the workflow data graph; a literal
    dataset name is an external input which the engine does not track. The reference form is
    normalised to what the parent-resolution pass expects: a "step/output" reference is stored
    bare so it resolves to a parent edge, while a workflow input reference keeps its braces so it
    is recognised as a root input.

    Args:
        step_name (str): Name of the step
        task_params (dict): Raw task parameters of the step
        log_stream: Logger

    Returns:
        dict: Map of input name to a dict with source and default keys
    """
    inputs = {}
    for index, job_param in enumerate(task_params.get("jobParameters") or []):
        if not isinstance(job_param, dict) or job_param.get("param_type") not in DATA_INPUT_PARAM_TYPES:
            continue
        reference = extract_dataset_reference(job_param.get("dataset"))
        if reference is None:
            # a literal dataset name: an external input, not produced inside this workflow
            continue
        # name the input after its command line option so it is recognisable in logs and unique
        # within the step; fall back to the job parameter position if there is no option
        option = extract_job_param_option(job_param) or f"input_{index}"
        input_name = f"{step_name}/{option}"
        if input_name in inputs:
            log_stream.warning(f"duplicated input option {option} in step {step_name}; disambiguating")
            input_name = f"{input_name}_{index}"
        # "step/output" resolves to a parent edge; a bare name resolves to a workflow input
        source = reference if "/" in reference else f"{{{reference}}}"
        inputs[input_name] = {"default": None, "source": source}
    return inputs


# merge job parameters
def merge_job_params(base_params, io_params):
    new_params = []
    # remove exec stuff from base_params
    exec_start = False
    end_exec = False
    for tmp_item in base_params:
        if tmp_item["type"] == "constant" and tmp_item["value"].startswith("-p "):
            exec_start = True
            continue
        if exec_start:
            if end_exec:
                pass
            elif tmp_item["type"] == "constant" and "padding" not in tmp_item:
                end_exec = True
                continue
        if exec_start and not end_exec:
            continue
        new_params.append(tmp_item)
    # take exec and IO stuff from io_params
    exec_start = False
    for tmp_item in io_params:
        if tmp_item["type"] == "constant" and tmp_item["value"] == "__delimiter__":
            exec_start = True
            continue
        # ignore archive option
        if tmp_item["type"] == "constant" and tmp_item["value"].startswith("-a "):
            continue
        if not exec_start:
            continue
        new_params.append(tmp_item)
    return new_params


# DAG vertex
class Node(object):
    def __init__(self, id, node_type, data, is_leaf, name):
        self.id = id
        # Per-workflow-scope sequence number (starts at 1), assigned in resolve_nodes.
        # Used only for building output dataset names. node.id stays unique within a single
        # parsed definition (the graph key used in id maps / parents / sub_nodes); it is not
        # unique across scatter iterations, which are separate workflows built later from the
        # same template. Independent scopes (each sub-workflow, each scatter template) restart at 1.
        self.member_id = None
        self.type = node_type
        self.data = data
        self.is_leaf = is_leaf
        self.is_tail = False
        self.is_head = False
        self.inputs = {}
        self.outputs = {}
        self.output_types = []
        self.scatter = None
        self.parents = set()
        self.name = name
        self.sub_nodes = set()
        self.root_inputs = None
        self.task_params = None
        self.condition = None
        self.is_workflow_output = False
        self.loop = False
        self.in_loop = False
        self.upper_root_inputs = None
        self.workflow_ref = None  # path or named block reference for type="workflow" nodes
        # True for native (parse_workflow_data) type="workflow" orchestration nodes: they own an
        # output dataset and submit a child workflow at runtime. CWL/snakemake sub-workflow nodes
        # are built by other parsers and stay False, keeping the transparent recursion semantics.
        self.is_sub_workflow = False
        self.scatter_inputs = None  # {param_name: [val1, val2, ...]} resolved at parse time; None if not a scatter step
        self.scatter_mode = None  # scatter mode string, e.g. "zip"
        # Raw root_outputs from the referenced child YAML (set before resolve_nodes, resolved after).
        # Used so scatter templates can use the child YAML's actual tail-step output values instead
        # of the parent scatter step's pre-baked container name (which is never created for
        # panda_task-only child workflows).
        self.child_root_outputs_raw = None
        self.child_root_outputs = None

    def add_parent(self, id):
        self.parents.add(id)

    # set real input values
    def set_input_value(self, key, src_key, src_value):
        # replace the value with a list of parameter names and indexes if value is a list,
        # and src and dst are looping params
        if isinstance(src_value, list):
            src_loop_param_name = self.get_loop_param_name(src_key)
            loop_params = self.get_loop_param_name(key.split("/")[-1]) is not None and src_loop_param_name is not None
            if loop_params:
                src_value = [{"src": src_loop_param_name, "idx": i} for i in range(len(src_value))]
        # resolve values
        if isinstance(self.inputs[key]["source"], list):
            self.inputs[key].setdefault("value", copy.copy(self.inputs[key]["source"]))
            tmp_list = []
            for k in self.inputs[key]["value"]:
                if k == src_key:
                    tmp_list.append(src_value)
                else:
                    tmp_list.append(k)
            self.inputs[key]["value"] = tmp_list
        else:
            self.inputs[key]["value"] = src_value

    # convert inputs to dict inputs
    def convert_dict_inputs(self, skip_suppressed=False):
        data = {}
        for k, v in self.inputs.items():
            if skip_suppressed and "suppressed" in v and v["suppressed"]:
                continue
            y_name = k.split("/")[-1]
            if "value" in v:
                data[y_name] = v["value"]
            elif "default" in v:
                data[y_name] = v["default"]
            else:
                raise ReferenceError(f"{k} is not resolved")
        return data

    # convert outputs to set
    def convert_set_outputs(self):
        data = set()
        for k, v in self.outputs.items():
            if "value" in v:
                data.add(v["value"])
        return data

    # verify
    def verify(self):
        if self.is_leaf:
            dict_inputs = self.convert_dict_inputs(True)
            # check input
            for k, v in dict_inputs.items():
                if v is None:
                    return False, f"{k} is unresolved"
            # check args
            for k in ["opt_exec", "opt_args"]:
                test_str = dict_inputs.get(k)
                if test_str:
                    m = re.search(r"%{[A-Z]*DS(\d+|\*)}", test_str)
                    if m:
                        return False, f"{m.group(0)} is unresolved in {k}"
            if self.type == "prun":
                for k in dict_inputs:
                    if k not in [
                        "opt_inDS",
                        "opt_inDsType",
                        "opt_secondaryDSs",
                        "opt_secondaryDsTypes",
                        "opt_args",
                        "opt_exec",
                        "opt_useAthenaPackages",
                        "opt_containerImage",
                    ]:
                        return False, f"unknown input parameter {k} for {self.type}"
            elif self.type in ["junction", "reana"]:
                for k in dict_inputs:
                    if k not in [
                        "opt_inDS",
                        "opt_inDsType",
                        "opt_args",
                        "opt_exec",
                        "opt_containerImage",
                    ]:
                        return False, f"unknown input parameter {k} for {self.type}"
            elif self.type == "phpo":
                for k in dict_inputs:
                    if k not in ["opt_trainingDS", "opt_trainingDsType", "opt_args"]:
                        return False, f"unknown input parameter {k} for {self.type}"
            elif self.type == "gitlab":
                for k in dict_inputs:
                    if k not in [
                        "opt_inDS",
                        "opt_args",
                        "opt_api",
                        "opt_projectID",
                        "opt_ref",
                        "opt_triggerToken",
                        "opt_accessToken",
                        "opt_site",
                        "opt_input_location",
                    ]:
                        return False, f"unknown input parameter {k} for {self.type}"
            elif self.type in RAW_TASK_PARAMS_STEP_TYPES:
                return self.verify_task_params()
        elif self.type == "workflow":
            reserved_params = ["i"]
            loop_global, workflow_global = self.get_global_parameters()
            if loop_global:
                for k in reserved_params:
                    if k in loop_global:
                        return (
                            False,
                            f"parameter {k} cannot be used since it is reserved by the system",
                        )
        return True, ""

    # verify raw task parameters supplied by the author
    def verify_task_params(self):
        task_params = self.task_params or {}
        if not isinstance(task_params, dict) or not task_params:
            return False, f"task_params is missing or empty for {self.type} step"
        # required keys
        for key in REQUIRED_TASK_PARAM_KEYS:
            if not task_params.get(key):
                return False, f"task_params is missing required key {key}"
        if not isinstance(task_params["jobParameters"], list):
            return False, "task_params.jobParameters must be a list"
        # Chaining is driven by the workflow engine through workflowHoldup, not by JEDI's parent
        # bookkeeping. parentTaskName is resolved when the task is inserted and fails outright if
        # the named parent does not exist yet, which it generally will not inside a workflow.
        if task_params.get("parentTaskName"):
            return False, "parentTaskName cannot be used in a workflow step; the workflow engine orders the steps"
        # taskName is stored in its own column before the task ID exists, so a ${TASKID} there
        # would silently diverge from the substituted copy inside the task parameters.
        if TASKID_PLACEHOLDER in task_params["taskName"]:
            return False, f"{TASKID_PLACEHOLDER} cannot be used in taskName"
        # every output must name the dataset it produces, since the engine registers workflow data
        # for it and downstream steps refer to it
        n_outputs = 0
        for job_param in task_params["jobParameters"]:
            if not isinstance(job_param, dict) or job_param.get("param_type") != "output":
                continue
            n_outputs += 1
            if not job_param.get("dataset"):
                return False, f"output job parameter {job_param.get('value')} has no dataset"
            if derive_output_key(job_param) is None:
                return False, f"cannot derive an output key from job parameter {job_param.get('value')}"
        if not n_outputs:
            return False, "task_params has no output job parameter"
        return True, ""

    # string representation
    def __str__(self):
        outstr = f"ID:{self.id} Name:{self.name} Type:{self.type}\n"
        outstr += f"  Parent:{','.join([str(p) for p in self.parents])}\n"
        outstr += "  Input:\n"
        for k, v in self.convert_dict_inputs().items():
            outstr += f"     {k}: {v}\n"
        outstr += "  Output:\n"
        for k, v in self.outputs.items():
            if "value" in v:
                v = v["value"]
            else:
                v = "NA"
            outstr += f"     {v}\n"
        return outstr

    # short description
    def short_desc(self):
        return f"ID:{self.id} Name:{self.name} Type:{self.type}"

    # resolve workload-specific parameters
    def resolve_params(self, task_template=None, id_map=None, workflow=None):
        if self.type in ["prun", "junction", "reana"]:
            dict_inputs = self.convert_dict_inputs()
            if "opt_secondaryDSs" in dict_inputs:
                # look for secondaryDsTypes if missing
                if "opt_secondaryDsTypes" not in dict_inputs:
                    dict_inputs["opt_secondaryDsTypes"] = []
                    for ds_name in dict_inputs["opt_secondaryDSs"]:
                        added = False
                        for pid in self.parents:
                            parent_node = id_map[pid]
                            if ds_name in parent_node.convert_set_outputs():
                                dict_inputs["opt_secondaryDsTypes"].append(parent_node.output_types[0] if parent_node.output_types else None)
                                added = True
                                break
                        if not added:
                            # use None if not found
                            dict_inputs["opt_secondaryDsTypes"].append(None)
                # resolve secondary dataset names
                idx = 1
                list_sec_ds = []
                for ds_name, ds_type in zip(dict_inputs["opt_secondaryDSs"], dict_inputs["opt_secondaryDsTypes"]):
                    if ds_type and "*" in ds_type:
                        ds_type = ds_type.replace("*", "XYZ")
                        ds_type += ".tgz"
                    src = f"%{{SECDS{idx}}}"
                    if ds_type:
                        dst = f"{ds_name}_{ds_type}/"
                    else:
                        dst = f"{ds_name}/"
                    dict_inputs["opt_exec"] = re.sub(src, dst, dict_inputs["opt_exec"])
                    dict_inputs["opt_args"] = re.sub(src, dst, dict_inputs["opt_args"])
                    idx += 1
                    list_sec_ds.append(src)
                if list_sec_ds:
                    src = r"%{SECDS\*}"
                    if "opt_exec" in dict_inputs:
                        dict_inputs["opt_exec"] = re.sub(src, ",".join(list_sec_ds), dict_inputs["opt_exec"])
                    if "opt_args" in dict_inputs:
                        dict_inputs["opt_args"] = re.sub(src, ",".join(list_sec_ds), dict_inputs["opt_args"])
                for k, v in self.inputs.items():
                    if k.endswith("opt_exec"):
                        v["value"] = dict_inputs["opt_exec"]
                    elif k.endswith("opt_args"):
                        v["value"] = dict_inputs["opt_args"]
                    # Set requirement for secondary datasets
                    if k.endswith("opt_secondaryDSs"):
                        v.setdefault("requirements", {})["requires_complete"] = True
        # A raw-task-params step brings its own task parameters, so it needs no CLI task template
        if self.is_leaf and (task_template or self.type in RAW_TASK_PARAMS_STEP_TYPES):
            self.task_params = self.make_task_params(task_template, id_map, workflow)
        # only recurse into nested Node objects (CWL/snakemake); native sub-workflow steps hold
        # resolved int IDs and are processed directly as part of the flat node list
        if _sub_nodes_are_objects(self.sub_nodes):
            [n.resolve_params(task_template, id_map, self) for n in self.sub_nodes]

    # create task params
    def make_task_params(self, task_template, id_map, workflow_node):
        # A raw-task-params step carries task parameters written by the author, so there is no
        # command line to parse and no task template to merge. The parameters are passed through
        # as they are: dataset references and ${TASKID} stay unresolved here on purpose, since
        # neither the producing step's real dataset names nor the JEDI task ID exist yet. Both are
        # resolved by the step handler when the task is actually submitted.
        if self.type in RAW_TASK_PARAMS_STEP_TYPES:
            return copy.deepcopy(self.task_params or {})
        # task name
        for k, v in self.outputs.items():
            task_name = v["value"]
            break
        if self.type in ["prun", "junction", "reana"]:
            dict_inputs = self.convert_dict_inputs(skip_suppressed=True)
            # check type
            use_athena = False
            if "opt_useAthenaPackages" in dict_inputs and dict_inputs["opt_useAthenaPackages"] and self.type != "reana":
                use_athena = True
            container_image = None
            if "opt_containerImage" in dict_inputs and dict_inputs["opt_containerImage"]:
                container_image = dict_inputs["opt_containerImage"]
            if use_athena:
                task_params = copy.deepcopy(task_template["athena"])
            else:
                task_params = copy.deepcopy(task_template["container"])
            task_params["taskName"] = task_name
            # cli params
            com = ["prun"]
            if self.type == "junction":
                # add default output for junction
                if "opt_args" not in dict_inputs:
                    dict_inputs["opt_args"] = ""
                results_json = "results.json"
                if "--outputs" not in dict_inputs["opt_args"]:
                    dict_inputs["opt_args"] += f" --outputs {results_json}"
                else:
                    m = re.search("(--outputs)( +|=)([^ ]+)", dict_inputs["opt_args"])
                    if results_json not in m.group(3):
                        tmp_dst = m.group(1) + "=" + m.group(3) + "," + results_json
                        dict_inputs["opt_args"] = re.sub(m.group(0), tmp_dst, dict_inputs["opt_args"])
            com += shlex.split(dict_inputs["opt_args"])
            if "opt_inDS" in dict_inputs and dict_inputs["opt_inDS"]:
                list_in_ds = self.get_input_ds_list(dict_inputs, id_map)
                if self.type not in ["reana"]:
                    in_ds_str = ",".join(list_in_ds)
                    com += ["--inDS", in_ds_str, "--notExpandInDS", "--notExpandSecDSs"]
                    if self.type in ["junction"]:
                        com += ["--forceStaged", "--forceStagedSecondary"]
                if self.type in ["prun", "junction", "reana"]:
                    # replace placeholders in opt_exec and opt_args
                    for idx, dst in enumerate(list_in_ds):
                        src = f"%{{DS{idx + 1}}}"
                        if "opt_exec" in dict_inputs:
                            dict_inputs["opt_exec"] = re.sub(src, dst, dict_inputs["opt_exec"])
                        if "opt_args" in dict_inputs:
                            dict_inputs["opt_args"] = re.sub(src, dst, dict_inputs["opt_args"])
                    if list_in_ds:
                        src = r"%{DS\*}"
                        if "opt_exec" in dict_inputs:
                            dict_inputs["opt_exec"] = re.sub(src, ",".join(list_in_ds), dict_inputs["opt_exec"])
                        if "opt_args" in dict_inputs:
                            dict_inputs["opt_args"] = re.sub(src, ",".join(list_in_ds), dict_inputs["opt_args"])
                    for k, v in self.inputs.items():
                        if k.endswith("opt_exec"):
                            v["value"] = dict_inputs["opt_exec"]
                        elif k.endswith("opt_args"):
                            v["value"] = dict_inputs["opt_args"]
            # global parameters
            if workflow_node:
                tmp_global, tmp_workflow_global = workflow_node.get_global_parameters()
                src_dst_list = []
                # looping globals
                if tmp_global:
                    for k in tmp_global:
                        tmp_src = f"%{{{k}}}"
                        tmp_dst = f"___idds___user_{k}___"
                        src_dst_list.append((tmp_src, tmp_dst))
                # workflow globls
                if tmp_workflow_global:
                    for k, v in tmp_workflow_global.items():
                        tmp_src = f"%{{{k}}}"
                        tmp_dst = f"{v}"
                        src_dst_list.append((tmp_src, tmp_dst))
                # iteration count
                tmp_src = "%{i}"
                tmp_dst = "___idds___num_run___"
                src_dst_list.append((tmp_src, tmp_dst))
                # replace
                for tmp_src, tmp_dst in src_dst_list:
                    if "opt_exec" in dict_inputs:
                        dict_inputs["opt_exec"] = re.sub(tmp_src, tmp_dst, dict_inputs["opt_exec"])
                    if "opt_args" in dict_inputs:
                        dict_inputs["opt_args"] = re.sub(tmp_src, tmp_dst, dict_inputs["opt_args"])
            com += ["--exec", dict_inputs["opt_exec"]]
            com += ["--outDS", task_name]
            if container_image:
                com += ["--containerImage", container_image]
                parse_com = copy.copy(com[1:])
            else:
                # add dummy container to keep build step consistent
                parse_com = copy.copy(com[1:])
                parse_com += ["--containerImage", None]
            # force a writable temp base for dry parsing regardless of process cwd
            parse_com += ["--tmpDir", tempfile.gettempdir()]
            athena_tag = False
            if use_athena:
                com += ["--useAthenaPackages"]
                athena_tag = "--athenaTag" in com
                # add cmtConfig
                if athena_tag and "--cmtConfig" not in parse_com:
                    parse_com += [
                        "--cmtConfig",
                        task_params["architecture"].split("@")[0],
                    ]
            # parse args without setting --useAthenaPackages since it requires real Athena runtime
            parsed_params = PrunScript.main(True, parse_com, dry_mode=True)
            task_params["cliParams"] = " ".join(shlex.quote(x) for x in com)
            # set parsed parameters
            for p_key, p_value in parsed_params.items():
                if p_key in ["buildSpec"]:
                    continue
                if p_key not in task_params or p_key in [
                    "log",
                    "container_name",
                    "multiStepExec",
                    "site",
                    "excludedSite",
                    "includedSite",
                    "nMaxFilesPerJob",
                    "nGBPerJob",
                ]:
                    task_params[p_key] = p_value
                elif p_key == "architecture":
                    task_params[p_key] = p_value
                    if not container_image:
                        if task_params[p_key] is None:
                            task_params[p_key] = ""
                        if "@" not in task_params[p_key] and "basePlatform" in task_params:
                            task_params[p_key] = f"{task_params[p_key]}@{task_params['basePlatform']}"
                elif athena_tag:
                    if p_key in ["transUses", "transHome"]:
                        task_params[p_key] = p_value
            # merge job params
            task_params["jobParameters"] = merge_job_params(task_params["jobParameters"], parsed_params["jobParameters"])
            # outputs
            for tmp_item in task_params["jobParameters"]:
                if tmp_item["type"] == "template" and tmp_item["param_type"] == "output":
                    if tmp_item["value"].startswith("regex|"):
                        self.output_types.append(re.search(r"_([^_]+)/$", tmp_item["dataset"]).group(1))
                    else:
                        self.output_types.append(re.search(r"}\.(.+)$", tmp_item["value"]).group(1))
            # add a dummy output if empty. this is to allow association to downstream steps which is described through outputs
            if not self.output_types:
                self.output_types.append("dummy")
            # container
            if not container_image:
                if "container_name" in task_params:
                    del task_params["container_name"]
                if "multiStepExec" in task_params:
                    del task_params["multiStepExec"]
            if "basePlatform" in task_params:
                del task_params["basePlatform"]
            # no build
            if use_athena and "--noBuild" in parse_com:
                for tmp_item in task_params["jobParameters"]:
                    if tmp_item["type"] == "constant" and tmp_item["value"] == "-l ${LIB}":
                        tmp_item["value"] = f"-a {task_params['buildSpec']['archiveName']}"
                del task_params["buildSpec"]
            # parent
            # if self.parents and len(self.parents) == 1:
            #     task_params["noWaitParent"] = True
            #     task_params["parentTaskName"] = id_map[list(self.parents)[0]].task_params["taskName"]
            # notification
            if not self.is_workflow_output:
                task_params["noEmail"] = True
            # use instant PQs
            if self.type in ["junction", "reana"]:
                task_params["runOnInstant"] = True
            # return
            return task_params
        elif self.type == "phpo":
            dict_inputs = self.convert_dict_inputs(skip_suppressed=True)
            # extract source and base URL
            source_url = task_template["container"]["sourceURL"]
            source_name = None
            for tmp_item in task_template["container"]["jobParameters"]:
                if tmp_item["type"] == "constant" and tmp_item["value"].startswith("-a "):
                    source_name = tmp_item["value"].split()[-1]
            # cli params
            com = shlex.split(dict_inputs["opt_args"])
            if "opt_trainingDS" in dict_inputs and dict_inputs["opt_trainingDS"]:
                if "opt_trainingDsType" not in dict_inputs or not dict_inputs["opt_trainingDsType"]:
                    in_ds_suffix = None
                    for parent_id in self.parents:
                        parent_node = id_map[parent_id]
                        if dict_inputs["opt_trainingDS"] in parent_node.convert_set_outputs():
                            in_ds_suffix = parent_node.output_types[0] if parent_node.output_types else None
                            break
                else:
                    in_ds_suffix = dict_inputs["opt_inDsType"]
                in_ds_str = f"{dict_inputs['opt_trainingDS']}_{in_ds_suffix}/"
                com += ["--trainingDS", in_ds_str]
            com += ["--outDS", task_name]
            # get task params
            task_params = PhpoScript.main(True, com, dry_mode=True)
            # change sandbox
            new_job_params = []
            for tmp_item in task_params["jobParameters"]:
                if tmp_item["type"] == "constant" and tmp_item["value"].startswith("-a "):
                    tmp_item["value"] = f"-a {source_name} --sourceURL {source_url}"
                new_job_params.append(tmp_item)
            task_params["jobParameters"] = new_job_params
            # return
            return task_params
        elif self.type == "gitlab":
            dict_inputs = self.convert_dict_inputs(skip_suppressed=True)
            list_in_ds = self.get_input_ds_list(dict_inputs, id_map)
            task_params = copy.copy(task_template["container"])
            task_params["taskName"] = task_name
            task_params["noInput"] = True
            task_params["nEventsPerJob"] = 1
            task_params["nEvents"] = 1
            task_params["processingType"] = re.sub(r"-[^-]+$", "-gitlab", task_params["processingType"])
            task_params["useSecrets"] = True
            task_params["site"] = dict_inputs["opt_site"]
            task_params["cliParams"] = ""
            task_params["log"]["container"] = task_params["log"]["dataset"] = f"{task_name}.log/"
            # set gitlab parameters
            task_params["jobParameters"] = [
                {
                    "type": "constant",
                    "value": json.dumps(
                        {
                            "project_api": dict_inputs["opt_api"],
                            "project_id": int(dict_inputs["opt_projectID"]),
                            "ref": dict_inputs["opt_ref"],
                            "trigger_token": dict_inputs["opt_triggerToken"],
                            "access_token": dict_inputs["opt_accessToken"],
                            "input_datasets": ",".join(list_in_ds),
                            "input_location": dict_inputs.get("opt_input_location"),
                        }
                    ),
                }
            ]

            del task_params["container_name"]
            del task_params["multiStepExec"]
            return task_params
        return None

    # get global parameters in the workflow
    def get_global_parameters(self):
        if self.is_leaf:
            root_inputs = self.upper_root_inputs
        else:
            root_inputs = self.root_inputs
        if root_inputs is None:
            return None, None
        loop_params = {}
        workflow_params = {}
        for k, v in root_inputs.items():
            m = self.get_loop_param_name(k)
            if m:
                loop_params[m] = v
            else:
                param = k.split("#")[-1]
                workflow_params[param] = v
        return loop_params, workflow_params

    # get all sub node IDs
    def get_all_sub_node_ids(self, all_ids=None):
        if all_ids is None:
            all_ids = set()
        all_ids.add(self.id)
        # only nested Node objects (CWL/snakemake) carry .id; native sub-workflow steps hold
        # resolved int IDs already accounted for in the flat node list
        if _sub_nodes_are_objects(self.sub_nodes):
            for sub_node in self.sub_nodes:
                all_ids.add(sub_node.id)
                if not sub_node.is_leaf:
                    sub_node.get_all_sub_node_ids(all_ids)
        return all_ids

    # get loop param name
    def get_loop_param_name(self, k):
        param = k.split("#")[-1]
        m = re.search(r"^param_(.+)", param)
        if m:
            return m.group(1)
        return None

    # def get input dataset list
    def get_input_ds_list(self, dict_inputs, id_map):
        if "opt_inDS" not in dict_inputs:
            return []
        if isinstance(dict_inputs["opt_inDS"], list):
            is_list_in_ds = True
        else:
            is_list_in_ds = False
        if "opt_inDsType" not in dict_inputs or not dict_inputs["opt_inDsType"]:
            if is_list_in_ds:
                in_ds_suffix = []
                in_ds_list = dict_inputs["opt_inDS"]
            else:
                in_ds_suffix = None
                in_ds_list = [dict_inputs["opt_inDS"]]
            for tmp_in_ds in in_ds_list:
                for parent_id in self.parents:
                    parent_node = id_map[parent_id]
                    if tmp_in_ds in parent_node.convert_set_outputs():
                        if is_list_in_ds:
                            in_ds_suffix.append(parent_node.output_types[0] if parent_node.output_types else None)
                        else:
                            in_ds_suffix = parent_node.output_types[0] if parent_node.output_types else None
                        break
        else:
            in_ds_suffix = dict_inputs["opt_inDsType"]
            if "*" in in_ds_suffix:
                in_ds_suffix = in_ds_suffix.replace("*", "XYZ") + ".tgz"
        if is_list_in_ds:
            list_in_ds = [f"{s1}_{s2}/" if s2 else s1 for s1, s2 in zip(dict_inputs["opt_inDS"], in_ds_suffix)]
        else:
            list_in_ds = [f"{dict_inputs['opt_inDS']}_{in_ds_suffix}/" if in_ds_suffix else dict_inputs["opt_inDS"]]
        return list_in_ds


def _sub_nodes_are_objects(sub_nodes):
    # After resolve_nodes, a native sub-workflow node stores its children as resolved int IDs
    # (the children are spliced into the flat node list and processed there). CWL/snakemake
    # sub-workflows instead keep their children as nested Node objects. Recurse only into the
    # latter; iterating int IDs as if they were nodes would crash.
    return bool(sub_nodes) and all(isinstance(n, Node) for n in sub_nodes)


# dump nodes
def dump_nodes(node_list, dump_str=None, only_leaves=False):
    if dump_str is None:
        dump_str = "\n"
    for node in node_list:
        if node.is_leaf:
            dump_str += f"{node}"
            if node.task_params is not None:
                dump_str += json.dumps(node.task_params, indent=4, sort_keys=True)
                dump_str += "\n\n"
        else:
            if not only_leaves:
                dump_str += f"{node}\n"
            if _sub_nodes_are_objects(node.sub_nodes):
                dump_str = dump_nodes(node.sub_nodes, dump_str, only_leaves)
    return dump_str


# get id map
def get_node_id_map(node_list, id_map=None):
    if id_map is None:
        id_map = {}
    for node in node_list:
        id_map[node.id] = node
        # native sub-workflow children are flat int IDs (already in node_list); only recurse into
        # nested Node objects (CWL/snakemake)
        if _sub_nodes_are_objects(node.sub_nodes):
            id_map = get_node_id_map(node.sub_nodes, id_map)
    return id_map


# get all parents
def get_all_parents(node_list, all_parents=None):
    if all_parents is None:
        all_parents = set()
    for node in node_list:
        all_parents |= node.parents
        # native sub-workflow nodes store resolved int IDs in sub_nodes (children are flat); only
        # recurse into nested Node objects (CWL/snakemake)
        if _sub_nodes_are_objects(node.sub_nodes):
            all_parents = get_all_parents(node.sub_nodes, all_parents)
    return all_parents


# set workflow outputs
def set_workflow_outputs(node_list, all_parents=None):
    if all_parents is None:
        all_parents = get_all_parents(node_list)
    for node in node_list:
        if node.is_leaf and node.id not in all_parents:
            node.is_workflow_output = True
        # native sub-workflow nodes store resolved int IDs in sub_nodes (children are flat); only
        # recurse into nested Node objects (CWL/snakemake)
        if _sub_nodes_are_objects(node.sub_nodes):
            set_workflow_outputs(node.sub_nodes, all_parents)


# NOTE: condition features are not yet implemented
# TODO: implement condition support
# def convert_params_in_condition_to_parent_ids(condition_item, input_data, id_map):
#     for item in ["left", "right"]:
#         param = getattr(condition_item, item)
#         if isinstance(param, str):
#             m = re.search(r"^[^\[]+\[(\d+)\]", param)
#             if m:
#                 param = param.split("[")[0]
#                 idx = int(m.group(1))
#             else:
#                 idx = None
#             isOK = False
#             for tmp_name, tmp_data in input_data.items():
#                 if param == tmp_name.split("/")[-1]:
#                     isOK = True
#                     if isinstance(tmp_data["parent_id"], list):
#                         if idx is not None:
#                             if idx < 0 or idx >= len(tmp_data["parent_id"]):
#                                 raise IndexError(f"index {idx} is out of bounds for parameter {param} with {len(tmp_data['parent_id'])} parents")
#                             parent_id = tmp_data["parent_id"][idx]
#                             if parent_id not in id_map:
#                                 raise ReferenceError(f"unresolved parent_id {parent_id} for parameter {param}[{idx}]")
#                             setattr(condition_item, item, id_map[parent_id])
#                         else:
#                             resolved_parent_ids = set()
#                             for parent_id in tmp_data["parent_id"]:
#                                 if parent_id not in id_map:
#                                     raise ReferenceError(f"unresolved parent_id {parent_id} for parameter {param}")
#                                 resolved_parent_ids |= id_map[parent_id]
#                             setattr(condition_item, item, list(resolved_parent_ids))
#                     else:
#                         if tmp_data["parent_id"] not in id_map:
#                             raise ReferenceError(f"unresolved parent_id {tmp_data['parent_id']} for parameter {param}")
#                         setattr(condition_item, item, id_map[tmp_data["parent_id"]])
#                     break
#             if not isOK:
#                 raise ReferenceError(f"unresolved parameter {param} in the condition string")


# resolve nodes
def resolve_nodes(node_list, root_inputs, data, serial_id, parent_ids, out_ds_name, log_stream):
    # member_id is a per-call sequence (starts at 1) used only for output dataset names. node.id
    # stays unique within this parsed definition; across scatter iterations it repeats, since each
    # iteration is a separate workflow built later from this template.
    #
    # member_counter is local to this call, so each recursive resolve_nodes scope (an inline
    # sub-workflow, or a scatter parent's child template) restarts member_id at 1 naturally.
    member_counter = [0]

    def _next_member():
        member_counter[0] += 1
        return member_counter[0]

    for k in root_inputs:
        kk = k.split("#")[-1]
        if kk in data:
            root_inputs[k] = data[kk]
    tmp_to_real_id_map = {}
    resolved_map = {}
    # map of object identity to original temporary node ID used in resolved_map keys
    node_key_map = {}
    all_nodes = []
    # Resolved sub-workflow template children, spliced into all_nodes after the tail computation
    # below (they belong to a recursive scope, so they have no resolved_map entry in this call).
    sub_workflow_child_nodes = []
    # Inline (steps-based) sub-workflow nodes whose int-id sub_nodes reference children resolved in
    # THIS call (merged into node_list at parse time). Their sub_nodes are remapped from parse-time
    # ids to resolved ids after the full id map is built (see remap pass below).
    inline_sub_workflow_nodes = []
    for node in node_list:
        # resolve input
        for tmp_name, tmp_data in node.inputs.items():
            if not tmp_data["source"]:
                continue
            if isinstance(tmp_data["source"], list):
                tmp_sources = tmp_data["source"]
                if "parent_id" in tmp_data:
                    # Make a copy to avoid mutating the original list stored in node.inputs
                    tmp_parent_ids = list(tmp_data["parent_id"])
                    tmp_parent_ids += [None] * (len(tmp_sources) - len(tmp_parent_ids))
                else:
                    tmp_parent_ids = [None] * len(tmp_sources)
            else:
                tmp_sources = [tmp_data["source"]]
                if "parent_id" in tmp_data:
                    tmp_parent_ids = [tmp_data["parent_id"]]
                else:
                    tmp_parent_ids = [None] * len(tmp_sources)
            for tmp_source, tmp_parent_id in zip(tmp_sources, tmp_parent_ids):
                isOK = False
                # check root input
                if tmp_source in root_inputs:
                    node.is_head = True
                    node.set_input_value(tmp_name, tmp_source, root_inputs[tmp_source])
                    continue
                # check parent output
                for i in node.parents:
                    for r_node in resolved_map[i]:
                        if tmp_source in r_node.outputs:
                            node.set_input_value(
                                tmp_name,
                                tmp_source,
                                r_node.outputs[tmp_source]["value"],
                            )
                            isOK = True
                            break
                    if isOK:
                        break
                if isOK:
                    continue
                # check resolved parent outputs
                if tmp_parent_id is not None:
                    values = [list(r_node.outputs.values())[0]["value"] for r_node in resolved_map[tmp_parent_id]]
                    if len(values) == 1:
                        values = values[0]
                    node.set_input_value(tmp_name, tmp_source, values)
                    continue
        # scatter
        if node.scatter:
            # resolve scattered parameters
            scatters = None
            sc_nodes = []
            for item in node.scatter:
                if scatters is None:
                    scatters = [{item: v} for v in node.inputs[item]["value"]]
                else:
                    [i.update({item: v}) for i, v in zip(scatters, node.inputs[item]["value"])]
            for idx, item in enumerate(scatters):
                sc_node = copy.deepcopy(node)
                for k, v in item.items():
                    sc_node.inputs[k]["value"] = v
                for tmp_node in sc_node.sub_nodes:
                    tmp_node.scatter_index = idx
                    tmp_node.upper_root_inputs = sc_node.root_inputs
                sc_nodes.append(sc_node)
        else:
            sc_nodes = [node]
        # loop over scattered nodes
        for sc_node in sc_nodes:
            original_node_id = sc_node.id
            all_nodes.append(sc_node)
            node_key_map[id(sc_node)] = original_node_id
            # set real node ID
            resolved_map.setdefault(original_node_id, [])
            tmp_to_real_id_map.setdefault(original_node_id, set())
            # resolve parents
            real_parens = set()
            for i in sc_node.parents:
                real_parens |= tmp_to_real_id_map[i]
            sc_node.parents = real_parens
            if sc_node.is_head:
                sc_node.parents |= parent_ids
            # A native sub-workflow node (scatter, workflow_ref, or inline steps) owns no task
            # itself; it is an orchestration step that gets its own output dataset and submits a
            # child workflow at runtime. Any Node-object child template it carries is resolved in
            # its own recursive scope below (see sub-workflow-child block). Here it is treated like
            # a leaf so it gets a serial id, a member_id in this scope, and its own output dataset
            # name. CWL/snakemake sub-workflow nodes (is_sub_workflow False) keep the transparent
            # recursion semantics: they own no dataset and expose their child tail outputs directly.
            is_scatter_workflow = sc_node.scatter_inputs is not None
            if sc_node.is_leaf or sc_node.is_sub_workflow:
                resolved_map[original_node_id].append(sc_node)
                tmp_to_real_id_map[original_node_id].add(serial_id)
            else:
                serial_id, sub_tail_nodes, sc_node.sub_nodes = resolve_nodes(
                    sc_node.sub_nodes,
                    sc_node.root_inputs,
                    sc_node.convert_dict_inputs(),
                    serial_id,
                    sc_node.parents,
                    out_ds_name,
                    log_stream,
                )
                resolved_map[original_node_id] += sub_tail_nodes
                tmp_to_real_id_map[original_node_id] |= set([n.id for n in sub_tail_nodes])
            # assign this node's serial id and per-call member_id (common to both branches)
            sc_node.id = serial_id
            sc_node.member_id = _next_member()
            serial_id += 1
            # convert parameters to parent IDs in conditions
            # TODO: condition features not yet implemented
            if sc_node.condition:
                pass
                # convert_params_in_condition_to_parent_ids(sc_node.condition, sc_node.inputs, tmp_to_real_id_map)
            # resolve outputs
            if sc_node.is_leaf or sc_node.is_sub_workflow:
                for tmp_name, tmp_data in sc_node.outputs.items():
                    # A raw-task-params step's output dataset names are supplied by the author and
                    # already set at parse time; they encode the physics (and any late-bound ID
                    # placeholder) and must not be replaced by a generated name. Every other parser
                    # -- native prun steps, CWL, snakemake -- creates outputs as empty dicts, so
                    # this only ever skips names that were deliberately set.
                    if "value" in tmp_data:
                        continue
                    tmp_data["value"] = f"{out_ds_name}_{sc_node.member_id:03d}_{sc_node.name}"
                    # add loop count for nodes in a loop
                    if sc_node.in_loop:
                        tmp_data["value"] += ".___idds___num_run___"
            # Resolve a native sub-workflow node's child template in its own recursive scope.
            # sub_nodes holds the child Node objects (a topo-sorted list) parsed from the referenced
            # workflow; the recursion restarts member_id at 1, threads serial_id so child ids stay
            # globally unique, and resolves the children as if they were top-level steps. The
            # resolved children are spliced back into this flat node list and sub_nodes is replaced
            # with their real ids, which extract_child_workflow_definition and the runtime sub-
            # workflow dispatch look up. Inline (steps-based) sub-workflows keep their int-id
            # sub_nodes -- their children are already merged into the flat node list at parse time.
            if sc_node.is_sub_workflow and _sub_nodes_are_objects(sc_node.sub_nodes):
                if is_scatter_workflow:
                    # scatter children are dispatched per-item at runtime against the parent's inputs
                    child_root_inputs, child_data, child_parent_ids = root_inputs, data, parent_ids
                    # keep the bare prefix: submit_sub_workflow applies the per-iteration
                    # "_{parent_member:03d}s{N}" scatter prefix (e.g. "_001s1_003") at runtime
                    child_out_ds_name = out_ds_name
                else:
                    # a plain sub-workflow resolves its template against its own declared inputs
                    child_root_inputs = sc_node.root_inputs or {}
                    child_data = sc_node.convert_dict_inputs()
                    child_parent_ids = sc_node.parents
                    # Embed this sub-workflow step's own member_id into the prefix so child
                    # dataset names reflect the hierarchy: e.g. the parent step
                    # "_002_sig_bg_comb" yields children "_002_001_make_signal",
                    # "_002_002_make_background_1", .... Deeper nesting appends further
                    # segments naturally (e.g. "_002_007_001_...").
                    child_out_ds_name = f"{out_ds_name}_{sc_node.member_id:03d}"
                serial_id, _child_tails, child_nodes = resolve_nodes(
                    list(sc_node.sub_nodes),
                    child_root_inputs,
                    child_data,
                    serial_id,
                    child_parent_ids,
                    child_out_ds_name,
                    log_stream,
                )
                sub_workflow_child_nodes.extend(child_nodes)
                sc_node.sub_nodes = {child.id for child in child_nodes}
            elif sc_node.is_sub_workflow and sc_node.sub_nodes:
                # inline (steps-based) sub-workflow: its children were merged into this node_list
                # and are resolved here as top-level nodes; their parse-time ids in sub_nodes must
                # be remapped to resolved ids once the full id map is available (see remap below).
                inline_sub_workflow_nodes.append(sc_node)
    # Remap inline sub-workflow nodes' parse-time child ids to resolved ids now that every node in
    # this scope has an entry in tmp_to_real_id_map.
    for sc_node in inline_sub_workflow_nodes:
        remapped = set()
        for old_id in sc_node.sub_nodes:
            remapped |= tmp_to_real_id_map.get(old_id, set())
        sc_node.sub_nodes = remapped
    # return tails
    tail_nodes = []
    for node in all_nodes:
        original_node_id = node_key_map.get(id(node), node.id)
        if node.is_tail:
            tail_nodes.append(node)
        else:
            tail_nodes += resolved_map[original_node_id]
    # Splice resolved sub-workflow template children into the flat node list now that tails are
    # computed; they are template steps (never workflow tails) and keep their own resolved ids.
    all_nodes.extend(sub_workflow_child_nodes)
    return serial_id, tail_nodes, all_nodes


def extract_child_workflow_definition(workflow_node: dict, all_nodes: list) -> dict:
    """
    Build a child workflow definition dict from a workflow-type node and its sub-nodes.

    Args:
        workflow_node (dict): Serialised Node dict (from vars(node)) with type="workflow"
        all_nodes (list): Full list of serialised Node dicts from the parent workflow definition

    Returns:
        dict: Child workflow definition with keys workflow_name, root_inputs, root_outputs, nodes
    """
    sub_node_ids = set(workflow_node.get("sub_nodes", []))
    child_nodes = [n for n in all_nodes if n["id"] in sub_node_ids]
    return {
        "workflow_name": workflow_node.get("name"),
        "root_inputs": workflow_node.get("root_inputs") or {},
        "root_outputs": workflow_node.get("outputs") or {},
        "nodes": child_nodes,
    }


# parse workflow data for native YAML workflow
def parse_workflow_data(data, log_stream, _id_counter=None):
    # _id_counter is a mutable [int] shared across recursive calls to guarantee unique node IDs
    if _id_counter is None:
        _id_counter = [0]

    # Handle both nested (workflow:{...}) and flat ({...}) structures
    workflow_data = data.get("workflow", data)

    # extract root inputs and outputs
    root_inputs = workflow_data.get("inputs", {})
    root_outputs = workflow_data.get("outputs", {})
    tail_node_names = {output_spec["from"].split("/")[0] for output_spec in root_outputs.values() if isinstance(output_spec, dict) and "from" in output_spec}

    # parse steps
    steps = workflow_data.get("steps", {})
    node_list = []
    node_name_map = {}
    all_child_nodes = []  # child nodes from inline sub-workflows, to be merged at the end

    # first pass: create all nodes
    for step_name, step_spec in steps.items():
        _id_counter[0] += 1
        serial_id = _id_counter[0]
        step_type = step_spec.get("type", "prun")
        is_leaf = step_type in ["prun", "phpo", "junction", "reana", "gitlab"] + list(RAW_TASK_PARAMS_STEP_TYPES)
        node = Node(serial_id, step_type, None, is_leaf, step_name)
        node_name_map[step_name] = node

        if step_type in RAW_TASK_PARAMS_STEP_TYPES:
            # A raw-task-params step describes itself entirely through its task parameters: the
            # output job parameters name the datasets it produces, and the input job parameters
            # name what it consumes. Both the data graph and the dependency edges are derived from
            # them, so there is no separate inDS/args/exec to parse.
            node.task_params = copy.deepcopy(step_spec.get("task_params") or {})
            node.inputs = build_task_step_inputs(step_name, node.task_params, log_stream)
            node.outputs = build_task_step_outputs(step_name, node.task_params, step_spec.get("outputs"), log_stream)
        else:
            # parse inputs
            inputs = {}
            for key, yaml_key in [
                ("inDS", "opt_inDS"),
                ("args", "opt_args"),
                ("exec", "opt_exec"),
                ("containerImage", "opt_containerImage"),
                ("useAthenaPackages", "opt_useAthenaPackages"),
                ("secondaryDSs", "opt_secondaryDSs"),
                ("secondaryDsTypes", "opt_secondaryDsTypes"),
            ]:
                if key in step_spec:
                    inputs[f"{step_name}/{yaml_key}"] = {
                        "default": step_spec.get(key) if key not in ["inDS", "secondaryDSs"] else None,
                        "source": step_spec.get(key) if key in ["inDS", "secondaryDSs"] else None,
                    }

            node.inputs = inputs
            node.outputs = {f"{step_name}/outDS": {}}
        node.is_tail = step_name in tail_node_names

        # handle sub-workflow nodes
        if step_type == "workflow":
            # native orchestration node: owns an outDS and submits a child workflow at runtime
            node.is_sub_workflow = True
            node.root_inputs = step_spec.get("inputs", {})
            if "scatter_inputs" in step_spec:
                # Store raw name references; caller (parse_raw_request) resolves to actual value lists
                node.scatter_inputs = step_spec.get("scatter_inputs", {})
                node.scatter_mode = step_spec.get("scatter_mode", "zip")
            if "steps" in step_spec:
                # inline sub-workflow: recursively parse the nested steps block. Treat it exactly
                # like a reference-based sub-workflow (shape B): keep the children as Node objects on
                # sub_nodes -- a topologically-sorted list, NOT flattened into the outer node list --
                # so resolve_nodes resolves them in their own recursive scope against this node's own
                # inputs. Without this, the inline children would be resolved as top-level siblings
                # and their {name} references would bind to the outer workflow's inputs instead of
                # the sub-workflow's own declared inputs.
                child_nodes, child_root_in = parse_workflow_data(step_spec, log_stream, _id_counter=_id_counter)
                # Input resolution mirrors the ref-based path:
                #  - scatter: the parent's scatter inputs replace the corresponding child inputs per
                #    iteration at runtime, so the template's own declared inputs are not used here.
                #  - ordinary: the inline sub-workflow uses its own declared inputs (defaults), with
                #    the node's explicit inputs overriding them.
                if not node.scatter_inputs:
                    node.root_inputs = {**(child_root_in or {}), **(node.root_inputs or {})}
                node.sub_nodes = child_nodes
                # child nodes are template steps within the sub-workflow; clear is_tail so they do
                # not appear as tail nodes of the outer workflow
                for child_node in child_nodes:
                    child_node.is_tail = False
                # Stash the raw root_outputs from the inline block so they can be resolved to actual
                # values after resolve_nodes assigns IDs (mirrors the ref-based path in the parser).
                node.child_root_outputs_raw = step_spec.get("outputs", {})
            elif "workflow_ref" in step_spec:
                # reference-based sub-workflow: mark for later resolution by the caller
                node.workflow_ref = step_spec["workflow_ref"]

        node_list.append(node)

    # merge child nodes so they are visible for parent resolution
    combined_node_list = node_list + all_child_nodes

    # second pass: resolve parent relationships; note that the parent_id is not used in core workflow execution but only for parameter resolution
    for node in combined_node_list:
        for input_name, input_data in node.inputs.items():
            source = input_data.get("source")
            if not source:
                continue

            # resolve single source
            if isinstance(source, str):
                if source.startswith("{") and source.endswith("}"):
                    input_data["source"] = source[1:-1]
                elif "/" in source:
                    source_node_name = source.split("/")[0]
                    if source_node_name in node_name_map:
                        parent = node_name_map[source_node_name]
                        node.add_parent(parent.id)
                        input_data["parent_id"] = parent.id
            # resolve list of sources
            elif isinstance(source, list):
                parent_ids = []
                for src in source:
                    if isinstance(src, str) and "/" in src:
                        source_node_name = src.split("/")[0]
                        if source_node_name in node_name_map:
                            parent = node_name_map[source_node_name]
                            node.add_parent(parent.id)
                            parent_ids.append(parent.id)
                if parent_ids:
                    input_data["parent_id"] = parent_ids

    # topological sort over all nodes (parents + child nodes together)
    visited = set()
    sorted_nodes = []
    node_id_map = {n.id: n for n in combined_node_list}

    def visit(n):
        if n.id in visited:
            return
        for parent_id in n.parents:
            if parent_id in node_id_map:
                visit(node_id_map[parent_id])
        visited.add(n.id)
        sorted_nodes.append(n)

    for node in combined_node_list:
        visit(node)

    return sorted_nodes, root_inputs
