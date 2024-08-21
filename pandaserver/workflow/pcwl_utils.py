import copy
import os.path
import re
from pathlib import Path
from urllib.parse import urlparse

from .workflow_utils import ConditionItem, Node

WORKFLOW_NAMES = ["prun", "phpo", "junction", "reana", "gitlab"]


# extract id
def extract_id(id_str):
    if not id_str:
        return id_str
    if not isinstance(id_str, list):
        id_str = [id_str]
        not_list = True
    else:
        not_list = False
    items = [re.search(r"[^/]+#.+$", s).group(0) for s in id_str]
    if not_list:
        return items[0]
    return items


# topological sorting
def top_sort(list_data, visited):
    if not list_data:
        return []
    new_list = []
    new_visited = []
    for node in list_data:
        isOK = True
        for p in node.parents:
            if p not in visited:
                isOK = False
                break
        if isOK:
            new_visited.append(node)
            visited.add(node.id)
        else:
            new_list.append(node)
    return new_visited + top_sort(new_list, visited)


# parse CWL file
def parse_workflow_file(workflow_file, log_stream, in_loop=False):
    # read the file from yaml
    cwl_file = Path(os.path.abspath(workflow_file))

    from cwl_utils.parser import load_document_by_uri

    # make fake executables to skip validation check in CWL
    for tmp_exec in WORKFLOW_NAMES:
        if not os.path.exists(tmp_exec):
            Path(tmp_exec).touch()

    # Import CWL Object
    root_obj = load_document_by_uri(cwl_file)

    # root inputs
    root_inputs = {extract_id(s.id): s.default for s in root_obj.inputs}

    root_outputs = set([re.sub(re.sub(extract_id(s.id), "", s.id), "", s.outputSource) for s in root_obj.outputs])

    # loop over steps
    node_list = []
    output_map = {}
    serial_id = 0
    for step in root_obj.steps:
        cwl_name = os.path.basename(step.run)
        # check cwl command
        if not cwl_name.endswith(".cwl") and cwl_name not in WORKFLOW_NAMES:
            log_stream.error(f"Unknown workflow {step.run}")
            return False, None
        serial_id += 1
        workflow_name = step.id.split("#")[-1]
        # leaf workflow and sub-workflow
        if cwl_name == "prun.cwl":
            node = Node(serial_id, "prun", None, True, workflow_name)
        elif cwl_name == "phpo.cwl":
            node = Node(serial_id, "phpo", None, True, workflow_name)
        elif cwl_name in WORKFLOW_NAMES:
            node = Node(serial_id, cwl_name, None, True, workflow_name)
        else:
            node = Node(serial_id, "workflow", None, False, workflow_name)
        node.inputs = {extract_id(s.id): {"default": s.default, "source": extract_id(s.source)} for s in step.in_}
        node.outputs = {extract_id(s): {} for s in step.out}
        # add outDS if no output is defined
        if not node.outputs:
            node.outputs = {extract_id(step.id + "/outDS"): {}}
        output_map.update({name: serial_id for name in node.outputs})
        if step.scatter:
            node.scatter = [extract_id(s) for s in step.scatter]
        if hasattr(step, "when") and step.when:
            # parse condition
            node.condition = parse_condition_string(step.when)
            # suppress inputs based on condition
            suppress_inputs_based_on_condition(node.condition, node.inputs)
        if step.hints and "loop" in step.hints:
            node.loop = True
        if node.loop or in_loop:
            node.in_loop = True
        # expand sub-workflow
        if not node.is_leaf:
            p = urlparse(step.run)
            tmp_path = os.path.abspath(os.path.join(p.netloc, p.path))
            node.sub_nodes, node.root_inputs = parse_workflow_file(tmp_path, log_stream, node.in_loop)
        # check if tail
        if root_outputs & set(node.outputs):
            node.is_tail = True
        node_list.append(node)

    # look for parents
    for node in node_list:
        for tmp_name, tmp_data in node.inputs.items():
            if not tmp_data["source"]:
                continue
            if isinstance(tmp_data["source"], list):
                sources = tmp_data["source"]
                is_str = False
            else:
                sources = [tmp_data["source"]]
                is_str = True
            parent_ids = []
            for tmp_source in sources:
                if tmp_source in output_map:
                    parent_id = output_map[tmp_source]
                    node.add_parent(parent_id)
                    parent_ids.append(parent_id)
            if parent_ids:
                if is_str:
                    parent_ids = parent_ids[0]
                tmp_data["parent_id"] = parent_ids

    # sort
    node_list = top_sort(node_list, set())
    return node_list, root_inputs


# resolve nodes
def resolve_nodes(node_list, root_inputs, data, serial_id, parent_ids, out_ds_name, log_stream):
    for k in root_inputs:
        kk = k.split("#")[-1]
        if kk in data:
            root_inputs[k] = data[kk]
    tmp_to_real_id_map = {}
    resolved_map = {}
    all_nodes = []
    for node in node_list:
        # resolve input
        for tmp_name, tmp_data in node.inputs.items():
            if not tmp_data["source"]:
                continue
            if isinstance(tmp_data["source"], list):
                tmp_sources = tmp_data["source"]
                if "parent_id" in tmp_data:
                    tmp_parent_ids = tmp_data["parent_id"]
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
            all_nodes.append(sc_node)
            # set real node ID
            resolved_map.setdefault(sc_node.id, [])
            tmp_to_real_id_map.setdefault(sc_node.id, set())
            # resolve parents
            real_parens = set()
            for i in sc_node.parents:
                real_parens |= tmp_to_real_id_map[i]
            sc_node.parents = real_parens
            if sc_node.is_head:
                sc_node.parents |= parent_ids
            if sc_node.is_leaf:
                resolved_map[sc_node.id].append(sc_node)
                tmp_to_real_id_map[sc_node.id].add(serial_id)
                sc_node.id = serial_id
                serial_id += 1
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
                resolved_map[sc_node.id] += sub_tail_nodes
                tmp_to_real_id_map[sc_node.id] |= set([n.id for n in sub_tail_nodes])
                sc_node.id = serial_id
                serial_id += 1
            # convert parameters to parent IDs in conditions
            if sc_node.condition:
                convert_params_in_condition_to_parent_ids(sc_node.condition, sc_node.inputs, tmp_to_real_id_map)
            # resolve outputs
            if sc_node.is_leaf:
                for tmp_name, tmp_data in sc_node.outputs.items():
                    tmp_data["value"] = f"{out_ds_name}_{sc_node.id:03d}_{sc_node.name}"
                    # add loop count for nodes in a loop
                    if sc_node.in_loop:
                        tmp_data["value"] += ".___idds___num_run___"
    # return tails
    tail_nodes = []
    for node in all_nodes:
        if node.is_tail:
            if node.is_tail:
                tail_nodes.append(node)
            else:
                tail_nodes += resolved_map[node.id]
    return serial_id, tail_nodes, all_nodes


# parse condition string
def parse_condition_string(cond_string):
    # remove $()
    cond_string = re.sub(r"\$\((?P<aaa>.+)\)", r"\g<aaa>", cond_string)
    cond_map = {}
    id = 0
    while True:
        # look for the most inner parentheses
        item_list = re.findall(r"\(([^\(\)]+)\)", cond_string)
        if not item_list:
            return convert_plain_condition_string(cond_string, cond_map)
        else:
            for item in item_list:
                cond = convert_plain_condition_string(item, cond_map)
                key = f"___{id}___"
                id += 1
                cond_map[key] = cond
                cond_string = cond_string.replace("(" + item + ")", key)


# extract parameter from token
def extract_parameter(token):
    m = re.search(r"self\.([^!=]+)", token)
    return m.group(1)


# convert plain condition string
def convert_plain_condition_string(cond_string, cond_map):
    cond_string = re.sub(r" *! *", r"!", cond_string)
    cond_string = re.sub(r"\|\|", r" || ", cond_string)
    cond_string = re.sub(r"&&", r" && ", cond_string)

    tokens = cond_string.split()
    left = None
    operator = None
    for token in tokens:
        token = token.strip()
        if token == "||":
            operator = "or"
            continue
        elif token == "&&":
            operator = "and"
            continue
        elif token.startswith("self."):
            param = extract_parameter(token)
            right = ConditionItem(param)
            if not left:
                left = right
                continue
        elif token.startswith("!self."):
            param = extract_parameter(token)
            right = ConditionItem(param, operator="not")
            if not left:
                left = right
                continue
        elif re.search(r"^___\d+___$", token) and token in cond_map:
            right = cond_map[token]
            if not left:
                left = right
                continue
        elif re.search(r"^!___\d+___$", token) and token[1:] in cond_map:
            right = ConditionItem(cond_map[token[1:]], operator="not")
            if not left:
                left = right
                continue
        else:
            raise TypeError(f'unknown token "{token}"')

        left = ConditionItem(left, right, operator)
    return left


# convert parameter names to parent IDs
def convert_params_in_condition_to_parent_ids(condition_item, input_data, id_map):
    for item in ["left", "right"]:
        param = getattr(condition_item, item)
        if isinstance(param, str):
            m = re.search(r"^[^\[]+\[(\d+)\]", param)
            if m:
                param = param.split("[")[0]
                idx = int(m.group(1))
            else:
                idx = None
            isOK = False
            for tmp_name, tmp_data in input_data.items():
                if param == tmp_name.split("/")[-1]:
                    isOK = True
                    if isinstance(tmp_data["parent_id"], list):
                        if idx is not None:
                            setattr(condition_item, item, id_map[tmp_data["parent_id"][idx]])
                        else:
                            setattr(condition_item, item, id_map[tmp_data["parent_id"]])
                    else:
                        setattr(condition_item, item, id_map[tmp_data["parent_id"]])
                    break
            if not isOK:
                raise ReferenceError(f"unresolved paramter {param} in the condition string")
        elif isinstance(param, ConditionItem):
            convert_params_in_condition_to_parent_ids(param, input_data, id_map)


# suppress inputs based on condition
def suppress_inputs_based_on_condition(condition_item, input_data):
    if condition_item.right is None and condition_item.operator == "not" and isinstance(condition_item.left, str):
        for tmp_name, tmp_data in input_data.items():
            if condition_item.left == tmp_name.split("/")[-1]:
                tmp_data["suppressed"] = True
    else:
        for item in ["left", "right"]:
            param = getattr(condition_item, item)
            if isinstance(param, ConditionItem):
                suppress_inputs_based_on_condition(param, input_data)
