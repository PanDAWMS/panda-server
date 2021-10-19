import copy
import six
import re
import shlex
import json

from pandaclient import PrunScript
from pandaclient import PhpoScript


# extract argument value from execution string
def get_arg_value(arg, exec_str):
    args = shlex.split(exec_str)
    if arg in args:
        return args[args.index(arg)+1]
    for item in args:
        if item.startswith(arg):
            return item.split('=')[-1]
    return None


# merge job parameters
def merge_job_params(base_params, io_params):
    new_params = []
    # remove exec stuff from base_params
    exec_start = False
    end_exec = False
    for tmp_item in base_params:
        if tmp_item['type'] == 'constant' and tmp_item["value"].startswith('-p '):
            exec_start = True
            continue
        if exec_start:
            if end_exec:
                pass
            elif tmp_item['type'] == 'constant' and "padding" not in tmp_item:
                end_exec = True
                continue
        if exec_start and not end_exec:
            continue
        new_params.append(tmp_item)
    # take exec and IO stuff from io_params
    exec_start = False
    for tmp_item in io_params:
        if tmp_item['type'] == 'constant' and tmp_item["value"].startswith('-p '):
            exec_start = True
        # ignore archive option
        if tmp_item['type'] == 'constant' and tmp_item["value"].startswith('-a '):
            continue
        if not exec_start:
            continue
        new_params.append(tmp_item)
    return new_params


# DAG vertex
class Node (object):

    def __init__(self, id, node_type, data, is_leaf, name):
        self.id = id
        self.type = node_type
        self.data = data
        self.is_leaf = is_leaf
        self.is_tail = False
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
        if isinstance(self.inputs[key]['source'], list):
            self.inputs[key].setdefault('value', copy.copy(self.inputs[key]['source']))
            tmp_list = []
            for k in self.inputs[key]['value']:
                if k == src_key:
                    tmp_list.append(src_value)
                else:
                    tmp_list.append(k)
            self.inputs[key]['value'] = tmp_list
        else:
            self.inputs[key]['value'] = src_value

    # convert inputs to dict inputs
    def convert_dict_inputs(self, skip_suppressed=False):
        data = {}
        for k, v in six.iteritems(self.inputs):
            if skip_suppressed and 'suppressed' in v and v['suppressed']:
                continue
            y_name = k.split('/')[-1]
            if 'value' in v:
                data[y_name] = v['value']
            elif 'default' in v:
                data[y_name] = v['default']
            else:
                raise ReferenceError("{} is not resolved".format(k))
        return data

    # convert outputs to set
    def convert_set_outputs(self):
        data = set()
        for k, v in six.iteritems(self.outputs):
            if 'value' in v:
                data.add(v['value'])
        return data

    # verify
    def verify(self):
        if self.is_leaf:
            dict_inputs = self.convert_dict_inputs(True)
            # check input
            for k, v in six.iteritems(dict_inputs):
                if v is None:
                    return False, '{} is unresolved'.format(k)
            # check args
            for k in ['opt_exec', 'opt_args']:
                test_str = dict_inputs.get(k)
                if test_str:
                    m = re.search(r'%%DS\d+%%', test_str)
                    if m:
                        return False, '{} is unresolved in {}'.format(m.group(0), k)
            if self.type == 'prun':
                for k in dict_inputs:
                    if k not in ['opt_inDS', 'opt_inDsType', 'opt_secondaryDSs', 'opt_secondaryDsTypes',
                                 'opt_args', 'opt_exec', 'opt_useAthenaPackages', 'opt_containerImage']:
                        return False, 'unknown input parameter {}'.format(k)
            elif self.type == 'phpo':
                for k in dict_inputs:
                    if k not in ['opt_trainingDS', 'opt_trainingDsType', 'opt_args']:
                        return False, 'unknown input parameter {}'.format(k)
        return True, ''

    # string representation
    def __str__(self):
        outstr = "ID:{} Name:{} Type:{}\n".format(self.id, self.name, self.type)
        outstr += "  Parent:{}\n".format(','.join([str(p) for p in self.parents]))
        outstr += "  Input:\n"
        for k, v in six.iteritems(self.convert_dict_inputs()):
            outstr += "     {}: {}\n".format(k, v)
        outstr += "  Output:\n"
        for k, v in six.iteritems(self.outputs):
            if 'value' in v:
                v = v['value']
            else:
                v = 'NA'
            outstr += "     {}\n".format(v)
        return outstr

    # resolve workload-specific parameters
    def resolve_params(self, task_template=None, id_map=None):
        if self.type == 'prun':
            dict_inputs = self.convert_dict_inputs()
            if 'opt_secondaryDSs' in dict_inputs:
                idx = 1
                for ds_name, ds_type in zip(dict_inputs['opt_secondaryDSs'], dict_inputs['opt_secondaryDsTypes']):
                    src = "%%DS{}%%".format(idx)
                    dst = "{}.{}".format(ds_name, ds_type)
                    dict_inputs['opt_exec'] = re.sub(src, dst, dict_inputs['opt_exec'])
                    dict_inputs['opt_args'] = re.sub(src, dst, dict_inputs['opt_args'])
                    idx += 1
                for k, v in six.iteritems(self.inputs):
                    if k.endswith('opt_exec'):
                        v['value'] = dict_inputs['opt_exec']
                    elif k.endswith('opt_args'):
                        v['value'] = dict_inputs['opt_args']
        if self.is_leaf and task_template:
            self.task_params = self.make_task_params(task_template, id_map)
        [n.resolve_params(task_template, id_map) for n in self.sub_nodes]

    # create task params
    def make_task_params(self, task_template, id_map):
        # task name
        for k, v in six.iteritems(self.outputs):
            task_name = v['value']
            break
        if self.type == 'prun':
            dict_inputs = self.convert_dict_inputs(skip_suppressed=True)
            # check type
            use_athena = False
            if 'opt_useAthenaPackages' in dict_inputs and dict_inputs['opt_useAthenaPackages']:
                use_athena = True
            container_image = None
            if 'opt_containerImage' in dict_inputs and dict_inputs['opt_containerImage']:
                container_image = dict_inputs['opt_containerImage']
            if use_athena:
                task_params = copy.deepcopy(task_template['athena'])
            else:
                task_params = copy.deepcopy(task_template['container'])
            task_params['taskName'] = task_name
            # tweak opt_exec for looping scatter
            opt_exec = dict_inputs['opt_exec']
            if self.in_loop and self.scatter_index is not None and self.get_looping_parameters():
                for k in self.get_looping_parameters():
                    opt_exec = opt_exec.replace('%%{}%%'.format(k), '%%{}_{}%%'.format(k, self.scatter_index))
            # cli params
            com = ['prun', '--exec', opt_exec, *shlex.split(dict_inputs['opt_args'])]
            in_ds_str = None
            if 'opt_inDS' in dict_inputs and dict_inputs['opt_inDS']:
                if isinstance(dict_inputs['opt_inDS'], list):
                    is_list_in_ds = True
                else:
                    is_list_in_ds = False
                if 'opt_inDsType' not in dict_inputs or not dict_inputs['opt_inDsType']:
                    if is_list_in_ds:
                        in_ds_suffix = []
                        in_ds_list = dict_inputs['opt_inDS']
                    else:
                        in_ds_suffix = None
                        in_ds_list = [dict_inputs['opt_inDS']]
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
                    in_ds_suffix = dict_inputs['opt_inDsType']
                if is_list_in_ds:
                    in_ds_str = ','.join(['{}_{}/'.format(s1, s2) for s1, s2 in zip(dict_inputs['opt_inDS'],
                                                                                    in_ds_suffix)])
                else:
                    in_ds_str = '{}_{}/'.format(dict_inputs['opt_inDS'], in_ds_suffix)
                com += ['--inDS', in_ds_str]
            com += ['--outDS', task_name]
            if container_image:
                com += ['--containerImage', container_image]
                parse_com = copy.copy(com[1:])
            else:
                # add dummy container to keep build step consistent
                parse_com = copy.copy(com[1:])
                parse_com += ['--containerImage', None]
            athena_tag = False
            if use_athena:
                com += ['--useAthenaPackages']
                athena_tag = '--athenaTag' in com
                # add cmtConfig
                if athena_tag and '--cmtConfig' not in parse_com:
                    parse_com += ['--cmtConfig', task_params['architecture'].split('@')[0]]
            # parse args without setting --useAthenaPackages since it requires real Athena runtime
            parsed_params = PrunScript.main(True, parse_com, dry_mode=True)
            task_params['cliParams'] = ' '.join(shlex.quote(x) for x in com)
            # set parsed parameters
            for p_key, p_value in six.iteritems(parsed_params):
                if p_key in ['buildSpec']:
                    continue
                if p_key not in task_params:
                    task_params[p_key] = p_value
                elif p_key == 'architecture':
                    task_params[p_key] = p_value
                    if not container_image:
                        if task_params[p_key] is None:
                            task_params[p_key] = ''
                        if '@' not in task_params[p_key] and 'basePlatform' in task_params:
                            task_params[p_key] = '{}@{}'.format(task_params[p_key], task_params['basePlatform'])
                elif athena_tag:
                    if p_key in ['transUses', 'transHome']:
                        task_params[p_key] = p_value
            # merge job params
            task_params['jobParameters'] = merge_job_params(task_params['jobParameters'],
                                                            parsed_params['jobParameters'])
            # outputs
            for tmp_item in task_params['jobParameters']:
                if tmp_item['type'] == 'template' and tmp_item["param_type"] == "output":
                    self.output_types.append(re.search(r'}\.(.+)$', tmp_item["value"]).group(1))
            # container
            if not container_image:
                if 'container_name' in task_params:
                    del task_params['container_name']
                if 'multiStepExec' in task_params:
                    del task_params['multiStepExec']
            if 'basePlatform' in task_params:
                del task_params['basePlatform']
            # no build
            if use_athena and '--noBuild' in parse_com:
                del task_params['buildSpec']
            # parent
            if self.parents and len(self.parents) == 1:
                task_params['noWaitParent'] = True
                task_params['parentTaskName'] = id_map[list(self.parents)[0]].task_params['taskName']
            # notification
            if not self.is_workflow_output:
                task_params['noEmail'] = True
            # return
            return task_params
        elif self.type == 'phpo':
            dict_inputs = self.convert_dict_inputs(skip_suppressed=True)
            # extract source and base URL
            source_url = task_template['container']['sourceURL']
            for tmp_item in task_template['container']["jobParameters"]:
                if tmp_item['type'] == "constant" and tmp_item["value"].startswith("-a "):
                    source_name = tmp_item["value"].split()[-1]
            # cli params
            com = shlex.split(dict_inputs['opt_args'])
            if 'opt_trainingDS' in dict_inputs and dict_inputs['opt_trainingDS']:
                if 'opt_trainingDsType' not in dict_inputs or not dict_inputs['opt_trainingDsType']:
                    in_ds_suffix = None
                    for parent_id in self.parents:
                        parent_node = id_map[parent_id]
                        if dict_inputs['opt_trainingDS'] in parent_node.convert_set_outputs():
                            in_ds_suffix = parent_node.output_types[0]
                            break
                else:
                    in_ds_suffix = dict_inputs['opt_inDsType']
                in_ds_str = '{}_{}/'.format(dict_inputs['opt_trainingDS'], in_ds_suffix)
                com += ['--trainingDS', in_ds_str]
            com += ['--outDS', task_name]
            # get task params
            task_params = PhpoScript.main(True, com, dry_mode=True)
            # change sandbox
            new_job_params = []
            for tmp_item in task_params["jobParameters"]:
                if tmp_item['type'] == "constant" and tmp_item["value"].startswith("-a "):
                    tmp_item["value"] = '-a {} --sourceURL {}'.format(source_name, source_url)
                new_job_params.append(tmp_item)
            task_params["jobParameters"] = new_job_params
            # return
            return task_params
        elif self.type == 'junction':
            return {}
        return None

    # get parameters in a loop
    def get_looping_parameters(self):
        if self.is_leaf:
            root_inputs = self.upper_root_inputs
        else:
            root_inputs = self.root_inputs
        if root_inputs is None:
            return None
        params = {}
        for k, v in six.iteritems(root_inputs):
            param = k.split('#')[-1]
            m = re.search(r'^param_(.+)', param)
            if m:
                params[m.group(1)] = v
        return params


# dump nodes
def dump_nodes(node_list, dump_str=None, only_leaves=True):
    if dump_str is None:
        dump_str = '\n'
    for node in node_list:
        if node.is_leaf:
            dump_str += "{}".format(node)
            if node.task_params is not None:
                dump_str += json.dumps(node.task_params, indent=4, sort_keys=True)
                dump_str += '\n\n'
        else:
            if not only_leaves:
                dump_str += "{}".format(node)
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
class ConditionItem (object):

    def __init__(self, left, right=None, operator=None):
        if operator not in ['and', 'or', 'not', None]:
            raise TypeError("unknown operator '{}'".format(operator))
        if operator in ['not', None] and right:
            raise TypeError("right param is given for operator '{}'".format(operator))
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
        dict_form[serial_id] = {'left': left_id, 'right': right_id, 'operator': self.operator}
        if is_entry:
            # sort
            keys = list(dict_form.keys())
            keys.sort()
            return [(k, dict_form[k]) for k in keys]
        else:
            return serial_id, dict_form
