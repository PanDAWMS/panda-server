import copy
import json
import re
import shlex

from idds.atlas.workflowv2.atlaslocalpandawork import ATLASLocalPandaWork
from idds.atlas.workflowv2.atlaspandawork import ATLASPandaWork
from idds.workflowv2.workflow import AndCondition, Condition, OrCondition, Workflow
from pandaclient import PhpoScript, PrunScript


# extract argument value from execution string
def get_arg_value(arg, exec_str):
    args = shlex.split(exec_str)
    if arg in args:
        return args[args.index(arg) + 1]
    for item in args:
        if item.startswith(arg):
            return item.split("=")[-1]
    return None


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
                                dict_inputs["opt_secondaryDsTypes"].append(parent_node.output_types[0])
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
        if self.is_leaf and task_template:
            self.task_params = self.make_task_params(task_template, id_map, workflow)
        [n.resolve_params(task_template, id_map, self) for n in self.sub_nodes]

    # create task params
    def make_task_params(self, task_template, id_map, workflow_node):
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
            if self.parents and len(self.parents) == 1:
                task_params["noWaitParent"] = True
                task_params["parentTaskName"] = id_map[list(self.parents)[0]].task_params["taskName"]
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
                            in_ds_suffix = parent_node.output_types[0]
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
                            in_ds_suffix.append(parent_node.output_types[0])
                        else:
                            in_ds_suffix = parent_node.output_types[0]
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
            dump_str = dump_nodes(node.sub_nodes, dump_str, only_leaves)
    return dump_str


# get id map
def get_node_id_map(node_list, id_map=None):
    if id_map is None:
        id_map = {}
    for node in node_list:
        id_map[node.id] = node
        if node.sub_nodes:
            id_map = get_node_id_map(node.sub_nodes, id_map)
    return id_map


# get all parents
def get_all_parents(node_list, all_parents=None):
    if all_parents is None:
        all_parents = set()
    for node in node_list:
        all_parents |= node.parents
        if node.sub_nodes:
            all_parents = get_all_parents(node.sub_nodes, all_parents)
    return all_parents


# set workflow outputs
def set_workflow_outputs(node_list, all_parents=None):
    if all_parents is None:
        all_parents = get_all_parents(node_list)
    for node in node_list:
        if node.is_leaf and node.id not in all_parents:
            node.is_workflow_output = True
        if node.sub_nodes:
            set_workflow_outputs(node.sub_nodes, all_parents)


# condition item
class ConditionItem(object):
    def __init__(self, left, right=None, operator=None):
        if operator not in ["and", "or", "not", None]:
            raise TypeError(f"unknown operator '{operator}'")
        if operator in ["not", None] and right:
            raise TypeError(f"right param is given for operator '{operator}'")
        self.left = left
        self.right = right
        self.operator = operator

    def get_dict_form(self, serial_id=None, dict_form=None):
        if dict_form is None:
            dict_form = {}
            is_entry = True
        else:
            is_entry = False
        if serial_id is None:
            serial_id = 0
        if isinstance(self.left, ConditionItem):
            serial_id, dict_form = self.left.get_dict_form(serial_id, dict_form)
            left_id = serial_id
            serial_id += 1
        else:
            left_id = self.left
        if isinstance(self.right, ConditionItem):
            serial_id, dict_form = self.right.get_dict_form(serial_id, dict_form)
            right_id = serial_id
            serial_id += 1
        else:
            right_id = self.right
        dict_form[serial_id] = {
            "left": left_id,
            "right": right_id,
            "operator": self.operator,
        }
        if is_entry:
            # sort
            keys = sorted(dict_form.keys())
            return [(k, dict_form[k]) for k in keys]
        else:
            return serial_id, dict_form


# convert nodes to workflow
def convert_nodes_to_workflow(nodes, workflow_node=None, workflow=None, workflow_name=None):
    if workflow is None:
        is_top = True
        workflow = Workflow()
        workflow.name = workflow_name
    else:
        is_top = False
    id_work_map = {}
    all_sub_id_work_map = {}
    sub_to_id_map = {}
    cond_dump_str = "  Conditions\n"
    class_dump_str = f"===== Workflow ID:{workflow_node.id if workflow_node else workflow_name} ====\n"
    class_dump_str += "  Works\n"
    dump_str_list = []
    # create works or workflows
    for node in nodes:
        if node.is_leaf:
            # work
            if node.type == "junction":
                work = ATLASLocalPandaWork(task_parameters=node.task_params)
                work.add_custom_condition("to_exit", True)
            else:
                work = ATLASPandaWork(task_parameters=node.task_params)
            workflow.add_work(work)
            id_work_map[node.id] = work
            class_dump_str += f"    {node.short_desc()} Class:{work.__class__.__name__}\n"
        else:
            # sub workflow
            sub_workflow = Workflow()
            id_work_map[node.id] = sub_workflow
            class_dump_str += f"    {node.short_desc()} Class:{sub_workflow.__class__.__name__}\n"
            sub_id_work_map, tmp_dump_str_list = convert_nodes_to_workflow(node.sub_nodes, node, sub_workflow)
            dump_str_list += tmp_dump_str_list
            for sub_id in node.get_all_sub_node_ids():
                all_sub_id_work_map[sub_id] = sub_workflow
                sub_to_id_map[sub_id] = node.id
            # add loop condition
            if node.loop:
                for sub_node in node.sub_nodes:
                    if sub_node.type == "junction":
                        # use to_continue for loop termination
                        j_work = sub_id_work_map[sub_node.id]
                        j_work.add_custom_condition(key="to_continue", value=True)
                        cond = Condition(cond=j_work.get_custom_condition_status)
                        sub_workflow.add_loop_condition(cond)
                        cond_dump_str += f"    Loop in ID:{node.id} with terminator ID:{sub_node.id}\n"
                        break
            workflow.add_work(sub_workflow)
    # add conditions
    for node in nodes:
        if node.parents:
            c_work = id_work_map[node.id]
            if not node.condition:
                # default conditions if unspecified
                cond_func_list = []
                for p_id in node.parents:
                    if p_id in id_work_map:
                        p_work = id_work_map[p_id]
                        str_p_id = p_id
                    elif p_id in all_sub_id_work_map:
                        p_work = all_sub_id_work_map[p_id]
                        str_p_id = sub_to_id_map[p_id]
                    else:
                        # head node
                        continue
                    if len(node.parents) > 1 or isinstance(p_work, Workflow) or node.type in ["junction", "reana", "gitlab"]:
                        cond_function = p_work.is_processed
                    else:
                        cond_function = p_work.is_started
                    if cond_function not in cond_func_list:
                        cond_func_list.append(cond_function)
                        cond_dump_str += f"    Default Link ID:{str_p_id} {cond_function.__name__} -> ID:{node.id}\n"
                cond = AndCondition(true_works=[c_work], conditions=cond_func_list)
                workflow.add_condition(cond)
            else:
                # convert conditions
                cond_list = node.condition.get_dict_form()
                base_cond_map = {}
                str_cond_map = {}
                root_condition = None
                for tmp_idx, base_cond in cond_list:
                    # leaf condition
                    if base_cond["right"] is None:
                        # condition based on works
                        cond_func_list = []
                        str_func_list = []
                        for p_id in base_cond["left"]:
                            if p_id in id_work_map:
                                p_work = id_work_map[p_id]
                                str_p_id = p_id
                            else:
                                p_work = all_sub_id_work_map[p_id]
                                str_p_id = sub_to_id_map[p_id]
                            # finished or failed
                            if base_cond["operator"] is None:
                                cond_function = p_work.is_processed
                            else:
                                cond_function = p_work.is_failed
                            cond_func_list.append(cond_function)
                            str_func_list.append(f"ID:{str_p_id} {cond_function.__name__}")
                        cond = AndCondition(conditions=cond_func_list)
                        base_cond_map[tmp_idx] = cond
                        str_func = "AND ".join(str_func_list)
                        str_cond_map[tmp_idx] = str_func
                        cond_dump_str += f"    Unary Ops {cond.__class__.__name__}({str_func}) -> ID:{node.id}\n"
                        root_condition = cond
                    else:
                        # composite condition
                        l_str_func_list = []
                        r_str_func_list = []
                        if isinstance(base_cond["left"], set):
                            cond_func_list = []
                            for p_id in base_cond["left"]:
                                if p_id in id_work_map:
                                    p_work = id_work_map[p_id]
                                    str_p_id = p_id
                                else:
                                    p_work = all_sub_id_work_map[p_id]
                                    str_p_id = sub_to_id_map[p_id]
                                cond_function = p_work.is_processed
                                cond_func_list.append(cond_function)
                                l_str_func_list.append(f"ID:{str_p_id} {cond_function.__name__}")
                            l_cond = AndCondition(conditions=cond_func_list)
                            l_str_func = "AND ".join(l_str_func_list)
                            str_cond_map[base_cond["left"]] = l_str_func
                        else:
                            l_cond = base_cond_map[base_cond["left"]]
                            l_str_func = str_cond_map[base_cond["left"]]
                        if isinstance(base_cond["right"], set):
                            cond_func_list = []
                            for p_id in base_cond["right"]:
                                if p_id in id_work_map:
                                    p_work = id_work_map[p_id]
                                    str_p_id = p_id
                                else:
                                    p_work = all_sub_id_work_map[p_id]
                                    str_p_id = sub_to_id_map[p_id]
                                cond_function = p_work.is_processed
                                cond_func_list.append(cond_function)
                                r_str_func_list.append(f"ID:{str_p_id} {cond_function.__name__}")
                            r_cond = AndCondition(conditions=cond_func_list)
                            r_str_func = "AND ".join(r_str_func_list)
                            str_cond_map[base_cond["right"]] = r_str_func
                        else:
                            r_cond = base_cond_map[base_cond["right"]]
                            r_str_func = str_cond_map[base_cond["right"]]
                        if base_cond["operator"] == "and":
                            cond = AndCondition(
                                conditions=[
                                    l_cond.is_condition_true,
                                    r_cond.is_condition_true,
                                ]
                            )
                        else:
                            cond = OrCondition(
                                conditions=[
                                    l_cond.is_condition_true,
                                    r_cond.is_condition_true,
                                ]
                            )
                        base_cond_map[tmp_idx] = cond
                        cond_dump_str += f"    Binary Ops {cond.__class__.__name__}({l_str_func}, {r_str_func}) for ID:{node.id}\n"
                        root_condition = cond
                # set root condition
                if root_condition:
                    root_condition.true_works = [c_work]
                    workflow.add_condition(root_condition)
    # global parameters
    if workflow_node:
        tmp_global, tmp_workflow_global = workflow_node.get_global_parameters()
        if tmp_global:
            loop_locals = {}
            loop_slices = []
            for k, v in tmp_global.items():
                if not isinstance(v, dict):
                    # normal looping locals
                    loop_locals["user_" + k] = tmp_global[k]
                else:
                    # sliced locals
                    v["src"] = "user_" + v["src"]
                    loop_slices.append([k, v])
            if loop_locals:
                workflow.set_global_parameters(loop_locals)
            for k, v in loop_slices:
                workflow.set_sliced_global_parameters(source=v["src"], index=v["idx"], name="user_" + k)
            cond_dump_str += "\n  Looping local variables\n"
            cond_dump_str += f"    {tmp_global}\n"
        if tmp_workflow_global:
            cond_dump_str += "\n  Workflow local variable\n"
            cond_dump_str += f"    {tmp_workflow_global}\n"
    # dump strings
    dump_str_list.insert(0, class_dump_str + "\n" + cond_dump_str + "\n\n")
    # return
    if not is_top:
        return id_work_map, dump_str_list
    return workflow, dump_str_list
