import copy
import re
import six
import os.path
from pathlib import Path
from ruamel import yaml
from urllib.parse import urlparse
import sys

from workflow_utils import Node


# extract id
def extract_id(id_str):
    if not id_str:
        return id_str
    if not isinstance(id_str, list):
        id_str = [id_str]
        not_list = True
    else:
        not_list = False
    items = [re.search(r'[^/]+#.+$', s).group(0) for s in id_str]
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
def parse_workflow_file(workflow_file, log_stream):
    # read the file from yaml
    cwl_file = Path(os.path.abspath(workflow_file))
    with open(cwl_file, "r") as cwl_h:
        yaml_obj = yaml.main.round_trip_load(cwl_h, preserve_quotes=True)

    # Check CWLVersion
    if 'cwlVersion' not in list(yaml_obj.keys()):
        log_stream.error("could not get the cwlVersion in {}".format(cwl_file.as_uri()))
        return False, None

    # Import parser based on CWL Version
    if yaml_obj['cwlVersion'] == 'v1.0':
        from cwl_utils import parser_v1_0 as parser
    elif yaml_obj['cwlVersion'] == 'v1.1':
        from cwl_utils import parser_v1_1 as parser
    elif yaml_obj['cwlVersion'] == 'v1.2':
        from cwl_utils import parser_v1_2 as parser
    else:
        log_stream.error("Version error. Did not recognise {} as a CWL version".format(yaml_obj["CWLVersion"]))
        return False, None

    # Import CWL Object
    root_obj = parser.load_document_by_yaml(yaml_obj, cwl_file.as_uri())

    # root inputs
    root_inputs = {extract_id(s.id): s.default for s in root_obj.inputs}

    # root outputs
    root_outputs = set([extract_id(s.id.split('#')[0] + '#' + re.sub(s.id+'/', '', s.outputSource))
                        for s in root_obj.outputs])

    # loop over steps
    node_list = []
    output_map = {}
    serial_id = 0
    for step in root_obj.steps:
        # exclude command
        if not step.run.endswith('.cwl'):
            log_stream.error("Unknown workflow {}".format(step.run))
            return False, None
        serial_id += 1
        cwl_name = os.path.basename(step.run)
        workflow_name = step.id.split('#')[-1]
        # leaf workflow and sub-workflow
        if cwl_name == 'prun.cwl':
            node = Node(serial_id, 'prun', None, True, workflow_name)
        else:
            node = Node(serial_id, 'workflow', None, False, workflow_name)
        node.inputs = {extract_id(s.id): {'default': s.default, 'source': extract_id(s.source)}
                       for s in step.in_}
        node.outputs = {extract_id(s): {} for s in step.out}
        output_map.update({name: serial_id for name in node.outputs})
        if step.scatter:
            node.scatter = [extract_id(s) for s in step.scatter]
        # expand sub-workflow
        if not node.is_leaf:
            p = urlparse(step.run)
            tmp_path = os.path.abspath(os.path.join(p.netloc, p.path))
            node.sub_nodes, node.root_inputs = parse_workflow_file(tmp_path, log_stream)
        # check if tail
        if root_outputs & set(node.outputs):
            node.is_tail = True
        node_list.append(node)

    # look for parents
    for node in node_list:
        for tmp_name, tmp_data in six.iteritems(node.inputs):
            if not tmp_data['source']:
                continue
            if isinstance(tmp_data['source'], list):
                sources = tmp_data['source']
                is_str = False
            else:
                sources = [tmp_data['source']]
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
                tmp_data['parent_id'] = parent_ids

    # sort
    node_list = top_sort(node_list, set())
    return node_list, root_inputs


# resolve nodes
def resolve_nodes(node_list, root_inputs, data, serial_id, parent_ids, outDsName, log_stream):
    for k in root_inputs:
        kk = k.split('#')[-1]
        if kk in data:
            root_inputs[k] = data[kk]
    tmp_to_real_id_map = {}
    resolved_map = {}
    all_nodes = []
    for node in node_list:
        log_stream.debug('tmpID:{} name:{} is_leaf:{}'.format(node.id, node.name, node.is_leaf))
        # resolve input
        is_head = False
        for tmp_name, tmp_data in six.iteritems(node.inputs):
            if not tmp_data['source']:
                continue
            if isinstance(tmp_data['source'], list):
                tmp_sources = tmp_data['source']
                if 'parent_id' in tmp_data:
                    tmp_parent_ids = tmp_data['parent_id']
                else:
                    tmp_parent_ids = [None] * len(tmp_sources)
            else:
                tmp_sources = [tmp_data['source']]
                if 'parent_id' in tmp_data:
                    tmp_parent_ids = [tmp_data['parent_id']]
                else:
                    tmp_parent_ids = [None] * len(tmp_sources)
            for tmp_source, tmp_parent_id in zip(tmp_sources, tmp_parent_ids):
                isOK = False
                # check root input
                if tmp_source in root_inputs:
                    is_head = True
                    node.set_input_value(tmp_name, tmp_source, root_inputs[tmp_source])
                    continue
                # check parent output
                for i in node.parents:
                    for r_node in resolved_map[i]:
                        if tmp_source in r_node.outputs:
                            node.set_input_value(tmp_name, tmp_source, r_node.outputs[tmp_source]['value'])
                            isOK = True
                            break
                    if isOK:
                        break
                if isOK:
                    continue
                # check resolved parent outputs
                if tmp_parent_id is not None:
                    values = [list(r_node.outputs.values())[0]['value'] for r_node in resolved_map[tmp_parent_id]]
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
                    scatters = [{item: v} for v in node.inputs[item]['value']]
                else:
                    [i.update({item: v}) for i, v in zip(scatters, node.inputs[item]['value'])]
            for item in scatters:
                sc_node = copy.deepcopy(node)
                for k, v in six.iteritems(item):
                    sc_node.inputs[k]['value'] = v
                sc_nodes.append(sc_node)
        else:
            sc_nodes = [node]
        # loop over scattered nodes
        for sc_node in sc_nodes:
            all_nodes.append(sc_node)
            # set real node ID
            resolved_map.setdefault(sc_node.id, [])
            tmp_to_real_id_map.setdefault(sc_node.id, set())
            if sc_node.is_leaf:
                resolved_map[sc_node.id].append(sc_node)
                tmp_to_real_id_map[sc_node.id].add(serial_id)
                sc_node.id = serial_id
                serial_id += 1
            else:
                serial_id, sub_tail_nodes, sc_node.sub_nodes = resolve_nodes(sc_node.sub_nodes, sc_node.root_inputs,
                                                                             sc_node.convert_dict_inputs(),
                                                                             serial_id, parent_ids, outDsName,
                                                                             log_stream)
                resolved_map[sc_node.id] += sub_tail_nodes
                tmp_to_real_id_map[sc_node.id] |= set([n.id for n in sub_tail_nodes])
            # resolve parents
            real_parens = set()
            for i in sc_node.parents:
                real_parens |= tmp_to_real_id_map[i]
            sc_node.parents = real_parens
            if is_head:
                sc_node.parents |= parent_ids
            # resolve outputs
            if sc_node.is_leaf:
                for tmp_name, tmp_data in six.iteritems(sc_node.outputs):
                    tmp_data['value'] = "{}_{:03d}_{}".format(outDsName, sc_node.id, sc_node.name)

    # return tails
    tail_nodes = []
    for node in all_nodes:
        if node.is_tail:
            if node.is_tail:
                tail_nodes.append(node)
            else:
                tail_nodes += resolved_map[node.id]
    return serial_id, tail_nodes, all_nodes


if __name__ == '__main__':
    import logging
    logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.DEBUG)
    with open(sys.argv[2]) as f:
        data = yaml.safe_load(f)
    nodes, root_in = parse_workflow_file(sys.argv[1], logging)
    s_id, t_nodes, nodes = resolve_nodes(nodes, root_in, data, 0, set(), 'hogehoge', logging)
    [node.resolve_params() for node in nodes]
    # task template
    task_template = {"buildSpec": {"jobParameters": "-i ${IN} -o ${OUT} --sourceURL ${SURL} -r . ",
                                   "archiveName": "sources.bfb28dae-cc83-4945-b110-486f0b9b9657.tar.gz",
                                   "prodSourceLabel": "panda"},
                     "sourceURL": "https://aipanda059.cern.ch:25443",
                     "site": None,
                     "vo": "atlas",
                     "respectSplitRule": True,
                     "osInfo": "Linux-3.10.0-1160.25.1.el7.x86_64-x86_64-with-centos-7.9.2009-Core",
                     "transUses": "", "excludedSite": [],
                     "nMaxFilesPerJob": 200, "uniqueTaskName": True,
                     "transHome": None,
                     "includedSite": None,
                     "jobParameters": [{"type": "constant", "value": "-j \"\" --sourceURL ${SURL}"},
                                       {"type": "constant", "value": "-r ."},
                                       {"padding": False, "type": "constant", "value": "-p \""},
                                       {"padding": False, "type": "constant",
                                       "value": "__dummy_exec_str__"},
                                       {"type": "constant", "value": "\""},
                                       {"type": "constant", "value": "-l ${LIB}"}],
                     "prodSourceLabel": "user",
                     "processingType": "panda-client-1.4.79-jedi-run",
                     }
    from workflow_utils import dump_nodes
    dump_nodes(nodes, task_template=task_template)

