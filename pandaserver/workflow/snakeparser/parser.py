__author__ = 'retmas'

import re
import os.path
import pathlib
import six
from types import SimpleNamespace
from itertools import chain
import snakemake.workflow
import snakemake.parser
import snakemake.dag
import snakemake.persistence
from pandaserver.workflow.workflow_utils import Node
from .log import Logger
from .names import WORKFLOW_NAMES


class Parser(object):
    def __init__(self, workflow_file, level=None):
        self._workflow = None
        self._dag = None
        self._logger = Logger().get()
        if level:
            self._logger.setLevel(level)
        snakefile = os.path.abspath(workflow_file)
        workdir = os.path.dirname(snakefile)
        self._logger.debug('create workflow')
        self._workflow = snakemake.workflow.Workflow(snakefile=snakefile, overwrite_workdir=None)
        self._workflow.workdir(workdir)
        self._workflow.include(self._workflow.main_snakefile, overwrite_default_target=True)

    @property
    def jobs(self):
        if self._dag is None:
            return list()
        return self._dag.jobs

    def parse_code(self):
        if self._workflow is None:
            return None
        code, _, __ = snakemake.parser.parse(
            snakemake.workflow.GenericSourceFile(self._workflow.main_snakefile),
            self._workflow
        )
        return code

    def parse_nodes(self):
        if self._dag is None:
            self._build_dag()
        root_job = next(filter(lambda o: o.rule.name == self._workflow.default_target, self.jobs))
        root_inputs = {Parser._extract_job_id(self._define_id(name)): value for name, value in root_job.params.items()}
        root_outputs = set([Parser._extract_job_id(s.id.split('#')[0] + '#' + re.sub(s.id + '/', '', s.outputSource))
                            for s in list(chain(*[map(lambda output: Parser._define_object(
                {'id': self._define_id(output), 'outputSource': self._define_id(f'{dep.name}/{output}')}), dep.output)
                                                  for dep in
                                                  self._dag.dependencies[root_job]]))])
        node_list = []
        output_map = {}
        serial_id = 0
        for job in filter(lambda o: o.name != self._workflow.default_target, self.jobs):
            # todo: is_script, is_run
            workflow_name = os.path.basename(job.shellcmd)
            if workflow_name not in WORKFLOW_NAMES:
                self._logger.error(f'Unknown workflow {job.shellcmd}')
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
                        source = Parser._extract_job_id(
                            self._define_id(f'{job.dependencies[default_value].name}/{default_value}'))
                        default_value = None
                node.inputs.update(
                    {Parser._extract_job_id(self._define_id(f'{job.name}/{name}')): {'default': default_value,
                                                                                     'source': source}})
            node.outputs = {Parser._extract_job_id(self._define_id(f'{job.name}/{output}')): {} for output in
                            job.output}
            if not node.outputs:
                node.outputs = {Parser._extract_job_id(self._define_id(f'{job.name}/outDS')): {}}
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
        node_list = self._sort_node_list(node_list, set())
        return node_list, root_inputs

    def verify_workflow(self):
        if self._workflow is None:
            return False
        self._workflow.check()
        return True

    def get_dot_data(self):
        if self._dag is None:
            self._build_dag()
        return str(self._dag)

    def _build_dag(self):
        target_rules = list(filter(lambda o: o.name == self._workflow.default_target, self._workflow.rules))
        self._dag = snakemake.dag.DAG(
            self._workflow,
            rules=self._workflow.rules,
            targetrules=target_rules,
            targetfiles=set()
        )
        self._workflow.persistence = snakemake.persistence.Persistence(dag=self._dag)
        self._dag.init()
        self._dag.update_checkpoint_dependencies()
        self._dag.check_dynamic()

    @staticmethod
    def _extract_job_id(job_id):
        if not job_id:
            return job_id
        if not isinstance(job_id, list):
            job_id = [job_id]
            not_list = True
        else:
            not_list = False
        items = [re.search(r'[^/]+#.+$', s).group(0) for s in job_id]
        if not_list:
            return items[0]
        return items

    @staticmethod
    def _sort_node_list(node_list, visited):
        if not node_list:
            return []
        new_list = []
        new_visited = []
        for node in node_list:
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
        return new_visited + Parser._sort_node_list(new_list, visited)

    def _define_id(self, name):
        return f'{pathlib.Path(os.path.abspath(self._workflow.main_snakefile)).as_uri()}#{name}'

    @staticmethod
    def _define_object(dict_):
        return SimpleNamespace(**dict_)
