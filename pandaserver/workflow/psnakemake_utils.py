__author__ = 'retmas'

import os, re, pathlib, six
from types import SimpleNamespace
from itertools import chain
import snakemake.workflow
import snakemake.parser
import snakemake.dag
import snakemake.persistence
from .workflow_utils import Node

WORKFLOW_NAMES = ['prun', 'phpo', 'junction', 'reana']


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


def top_sort(list_data, visited):
    if not list_data:
        return []
    new_list = []
    new_visited = []
    for node in list_data:
        is_ok = True
        for p in node.parents:
            if p not in visited:
                is_ok = False
                break
        if is_ok:
            new_visited.append(node)
            visited.add(node.id)
        else:
            new_list.append(node)
    return new_visited + top_sort(new_list, visited)


# noinspection DuplicatedCode
def parse_workflow_file(workflow_file, logging):
    snakefile = os.path.abspath(workflow_file)
    workdir = os.path.dirname(snakefile)
    snake_workflow = snakemake.workflow.Workflow(snakefile=snakefile, overwrite_workdir=None)
    snake_workflow.workdir(workdir)
    snake_workflow.include(snakefile, overwrite_default_target=True)
    snake_workflow.check()
    snake_workflow_code, _, __ = snakemake.parser.parse(
        snakemake.workflow.GenericSourceFile(workflow_file),
        snake_workflow
    )
    target_rules = list(filter(lambda o: o.name == snake_workflow.default_target, snake_workflow.rules))
    dag = snakemake.dag.DAG(snake_workflow, rules=snake_workflow.rules, targetrules=target_rules, targetfiles=set())
    snake_workflow.persistence = snakemake.persistence.Persistence(dag=dag)
    dag.init()
    dag.update_checkpoint_dependencies()
    dag.check_dynamic()
    define_id = lambda name: f'{pathlib.Path(os.path.abspath(workflow_file)).as_uri()}#{name}'
    define_object = lambda d: SimpleNamespace(**d)
    root_job = next(filter(lambda o: o.rule.name == snake_workflow.default_target, dag.jobs))
    root_inputs = {extract_id(define_id(name)): value for name, value in root_job.params.items()}
    root_outputs = set([extract_id(s.id.split('#')[0] + '#' + re.sub(s.id + '/', '', s.outputSource))
                        for s in list(chain(*[map(lambda output: define_object(
            {'id': define_id(output), 'outputSource': define_id(f'{dep.name}/{output}')}), dep.output) for dep in
                                              dag.dependencies[root_job]]))])
    node_list = []
    output_map = {}
    serial_id = 0
    for job in filter(lambda o: o.name != snake_workflow.default_target, dag.jobs):
        # todo: is_script, is_run
        workflow_name = os.path.basename(job.shellcmd)
        if workflow_name not in WORKFLOW_NAMES:
            logging.error(f'Unknown workflow {job.shellcmd}')
            return False, None
        serial_id += 1
        if workflow_name in WORKFLOW_NAMES:
            node = Node(serial_id, workflow_name, None, True, job.name)
        else:
            node = Node(serial_id, 'workflow', None, False, job.name)
        for name, value in job.params.items():
            if isinstance(value, list):
                value = ' '.join(value)
            if node.inputs is None:
                node.inputs = dict()
            default_value = value
            source = None
            if job.dependencies:
                if default_value in job.dependencies.keys():
                    source = extract_id(define_id(f'{job.dependencies[default_value].name}/{default_value}'))
                    default_value = None
            node.inputs.update(
                {extract_id(define_id(f'{job.name}/{name}')): {'default': default_value, 'source': source}})
        node.outputs = {extract_id(define_id(f'{job.name}/{output}')): {} for output in job.output}
        if not node.outputs:
            node.outputs = {extract_id(define_id(f'{job.name}/outDS')): {}}
        output_map.update({name: serial_id for name in node.outputs})
        # todo: scatter, loop, sub-workflow
        node.scatter = None
        if root_outputs & set(node.outputs):
            node.is_tail = True
        node_list.append(node)
    for node in node_list:
        for name, data in six.iteritems(node.inputs):
            if not data['source']:
                continue
            if isinstance(data['source'], list):
                sources = data['source']
                is_str = False
            else:
                sources = [data['source']]
                is_str = True
            parent_id_list = list()
            for source in sources:
                if source in output_map:
                    parent_id = output_map[source]
                    node.add_parent(parent_id)
                    parent_id_list.append(parent_id)
            if parent_id_list:
                if is_str:
                    parent_id_list = parent_id_list[0]
                data['parent_id'] = parent_id_list
    node_list = top_sort(node_list, set())
    return node_list, root_inputs
