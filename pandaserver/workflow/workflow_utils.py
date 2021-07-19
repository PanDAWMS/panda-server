import copy
import six
import re
import shlex
from urllib.parse import quote


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
        self.scatter = None
        self.parents = set()
        self.name = name
        self.sub_nodes = set()
        self.root_inputs = None

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
    def resolve_params(self):
        if self.type == 'prun':
            dict_inputs = self.convert_dict_inputs()
            if 'opt_secondaryDSs' in dict_inputs:
                idx = 1
                print (dict_inputs['opt_secondaryDSs'])
                print (dict_inputs['opt_secondaryDsTypes'])
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
        [n.resolve_params() for n in self.sub_nodes]

    # get arg value from exec string
    def get_arg_value(self, arg, exec_str):
        args = shlex.split(exec_str)
        if arg in args:
            return args[args.index(arg)+1]
        for item in args:
            if item.startswith(arg):
                return item.split('=')[-1]
        return None

    # create task params
    def make_task_params(self, task_template):
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
                    in_ds_str = ','.join(dict_inputs['opt_inDS'])
                else:
                    in_ds_str = dict_inputs['opt_inDS']
                com += ['--inDS', in_ds_str]
            else:
                # no input
                n_jobs = self.get_arg_value('--nJobs', dict_inputs['opt_args'])
                if not n_jobs:
                    n_jobs = 1
                else:
                    n_jobs = int(n_jobs)
                task_params['noInput'] = True
                task_params['nEvents'] = n_jobs
                task_params['nEventsPerJob'] = 1
            com += ['--outDS', task_name]
            task_params['cliParams'] = ' '.join(shlex.quote(x) for x in com)
            # exec
            for item in task_params['jobParameters']:
                if item['value'] == '__dummy_exec_str__':
                    item['value'] = quote(dict_inputs['opt_exec'])
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
                    secondaryDSs = self.get_arg_value('--secondaryDSs', dict_inputs['opt_args'])
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
            outputs = self.get_arg_value('--outputs', dict_inputs['opt_args'])
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
                if outMap:
                    dict_item = {'type':'constant',
                                 'value': '-o "{0}"'.format(str(outMap)),
                                 }
                    task_params['jobParameters'].append(dict_item)
            # return
            return task_params


# dump nodes
def dump_nodes(node_list, only_leaves=True, task_template=None):
    for node in node_list:
        if node.is_leaf:
            print(node)
            if task_template:
                task_params = node.make_task_params(task_template)
                if task_params:
                    ks = list(task_params.keys())
                    ks.sort()
                    for k in ks:
                        print('{} : {}'.format(k, task_params[k]))
                print('')
        else:
            if not only_leaves:
                print(node)
            dump_nodes(node.sub_nodes, only_leaves, task_template)
