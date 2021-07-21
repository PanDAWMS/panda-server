import copy
import six
import re
import shlex
import json
from urllib.parse import quote


# extract argument value from execution string
def get_arg_value(arg, exec_str):
    args = shlex.split(exec_str)
    if arg in args:
        return args[args.index(arg)+1]
    for item in args:
        if item.startswith(arg):
            return item.split('=')[-1]
    return None


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

    def add_parent(self, id):
        self.parents.add(id)

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
    def convert_dict_inputs(self):
        data = {}
        for k, v in six.iteritems(self.inputs):
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
        if task_template:
            self.task_params = self.make_task_params(task_template, id_map)
        [n.resolve_params(task_template, id_map) for n in self.sub_nodes]

    # create task params
    def make_task_params(self, task_template, id_map):
        if self.type == 'prun':
            dict_inputs = self.convert_dict_inputs()
            task_params = copy.deepcopy(task_template)
            # task name
            for k, v in six.iteritems(self.outputs):
                task_name = v['value']
                break
            task_params['taskName'] = task_name
            # architecture
            if 'opt_architecture' in dict_inputs and dict_inputs['opt_architecture']:
                task_params['architecture'] = dict_inputs['opt_architecture']
            # cli params
            com = ['prun', '--exec', dict_inputs['opt_exec'], *shlex.split(dict_inputs['opt_args'])]
            in_ds_str = None
            if 'opt_inDS' in dict_inputs and dict_inputs['opt_inDS']:
                if isinstance(dict_inputs['opt_inDS'], list):
                    is_list_in_ds = True
                else:
                    is_list_in_ds = False
                if 'opt_inDsType' not in dict_inputs or not dict_inputs['opt_inDsType']:
                    if is_list_in_ds:
                        in_ds_suffix = []
                    else:
                        in_ds_suffix = None
                    for tmp_in_ds in dict_inputs['opt_inDS']:
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
                    in_ds_str = ','.join(['{}.{}'.format(s1, s2) for s1, s2 in zip(dict_inputs['opt_inDS'],
                                                                                   in_ds_suffix)])
                else:
                    in_ds_str = '{}.{}'.format(dict_inputs['opt_inDS'], in_ds_suffix)
                com += ['--inDS', in_ds_str]
            else:
                # no input
                n_jobs = get_arg_value('--nJobs', dict_inputs['opt_args'])
                if not n_jobs:
                    n_jobs = 1
                else:
                    n_jobs = int(n_jobs)
                task_params['noInput'] = True
                task_params['nEvents'] = n_jobs
                task_params['nEventsPerJob'] = 1
            com += ['--outDS', task_name]
            container_image = None
            if 'opt_containerImage' in dict_inputs and dict_inputs['opt_containerImage']:
                container_image = dict_inputs['opt_containerImage']
                com += ['--containerImage', container_image]
            task_params['cliParams'] = ' '.join(shlex.quote(x) for x in com)
            # exec
            for item in task_params['jobParameters']:
                if item['value'] == '__dummy_exec_str__':
                    item['value'] = quote(dict_inputs['opt_exec'])
            # log
            task_params['log'] = {"type": "template", "param_type": "log",
                                  "container": '{}/'.format(task_name),
                                  "value": task_name + ".log.$JEDITASKID.${SN}.log.tgz",
                                  "dataset": '{}/'.format(task_name)}
            # job params
            if in_ds_str:
                # input dataset
                dict_item = {'value': '-i "${IN/T}"', 'dataset': in_ds_str,
                             'param_type': 'input', 'exclude': '\\.log\\.tgz(\\.\\d+)*$',
                             'type': 'template', 'expand': True},
                task_params['jobParameters'].append(dict_item)
                # secondary datasets
                if 'opt_secondaryDSs' in dict_inputs:
                    # parse
                    in_map = {}
                    secondaryDSs = get_arg_value('--secondaryDSs', dict_inputs['opt_args'])
                    for tmpItem in secondaryDSs.split(','):
                        tmpItems = tmpItem.split(':')
                        if len(tmpItems) >= 3:
                            in_map.setdefault('IN', "dummy_IN")
                            dict_item = {'nFilesPerJob': int(tmpItems[1]),
                                         'value': '${%s}' % tmpItems[0],
                                         'dataset': tmpItems[2],
                                         'param_type': 'input', 'hidden': True, 'type': 'template', 'expand': True}
                            in_map[tmpItems[0]] = 'dummy_%s' % tmpItems[0]
                            task_params['jobParameters'].append(dict_item)
                    if in_map:
                        str_in_map = str(in_map)
                        for k in in_map:
                            str_in_map = str_in_map.replace("'dummy_" + k + "'", '${' + k + '/T}')
                        dict_item = {'type': 'constant',
                                     'value': '--inMap "%s"' % str_in_map,
                                     }
                        task_params['jobParameters'].append(dict_item)
            # outputs
            outputs = get_arg_value('--outputs', dict_inputs['opt_args'])
            if outputs:
                outMap = {}
                for tmpLFN in outputs.split(','):
                    tmpDsSuffix = ''
                    if ':' in tmpLFN:
                        tmpDsSuffix,tmpLFN = tmpLFN.split(':')
                    tmpNewLFN = tmpLFN
                    # change * to XYZ and add .tgz
                    if '*' in tmpNewLFN:
                        tmpNewLFN = tmpNewLFN.replace('*', 'XYZ')
                        tmpNewLFN += '.tgz'
                    lfn = task_name
                    lfn += '.$JEDITASKID._${{SN/P}}.{0}'.format(tmpNewLFN)
                    dataset = '{}_{}/'.format(task_name, tmpNewLFN)
                    dict_item = {'container': dataset,
                                 'value': lfn,
                                 'dataset': dataset,
                                 'param_type': 'output', 'hidden': True, 'type': 'template'},
                    task_params['jobParameters'].append(dict_item)
                    outMap[tmpLFN] = lfn
                    self.output_types.append(tmpLFN)
                if outMap:
                    dict_item = {'type':'constant',
                                 'value': '-o "{0}"'.format(str(outMap)),
                                 }
                    task_params['jobParameters'].append(dict_item)
            # container
            if container_image:
                task_params['container_name'] = container_image
                task_params['multiStepExec']['containerOptions']['containerImage'] = container_image
            else:
                del task_params['container_name']
                del task_params['multiStepExec']
            # parent
            if self.parents and len(self.parents) == 1:
                task_params['noWaitParent'] = True
                task_params['parentTaskName'] = id_map[list(self.parents)[0]].task_params['taskName']
            # return
            return task_params
        return None


# dump nodes
def dump_nodes(node_list, dump_str=None, only_leaves=True):
    if dump_str is None:
        dump_str = '\n'
    for node in node_list:
        if node.is_leaf:
            dump_str += "{}\n".format(node)
            if node.task_params:
                dump_str += json.dumps(node.task_params, indent=4, sort_keys=True)
                dump_str += '\n'
        else:
            if not only_leaves:
                dump_str += "{}\n".format(node)
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
