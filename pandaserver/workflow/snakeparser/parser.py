__author__ = "retmas"

import logging
import os
import os.path
import pathlib
import re
from itertools import chain
from types import SimpleNamespace

import snakemake.dag
import snakemake.parser
import snakemake.persistence
import snakemake.workflow
from pandaserver.workflow.snakeparser.utils import ParamRule, param_of
from pandaserver.workflow.workflow_utils import ConditionItem, Node

from .extensions import inject
from .log import Logger
from .names import WORKFLOW_NAMES


class ParamNotFoundException(Exception):
    def __init__(self, param_name, rule_name):
        super().__init__(f"Parameter {param_name} is not found in {rule_name} rule")


class UnknownWorkflowTypeException(Exception):
    def __init__(self, rule_name):
        super().__init__(f"Unknown workflow type for rule {rule_name}")


class UnknownRuleShellException(Exception):
    def __init__(self, rule_name):
        super().__init__(f"Unknown shellcmd for rule {rule_name}")


class NoRuleShellException(Exception):
    def __init__(self, rule_name):
        super().__init__(f"No shellcmd for rule {rule_name}")


class UnknownConditionTokenException(Exception):
    def __init__(self, token):
        super().__init__(f"Unknown token {token}")


# noinspection DuplicatedCode
class Parser(object):
    def __init__(self, workflow_file, level=None, logger=None):
        self._workflow = None
        self._dag = None
        if logger:
            self._logger = logger
        else:
            self._logger = Logger().get()
            if level:
                self._logger.setLevel(level)
        snakefile = os.path.abspath(workflow_file)
        workdir = os.path.dirname(snakefile)
        self._logger.debug("create workflow")
        self._workflow = snakemake.workflow.Workflow(snakefile=snakefile, overwrite_workdir=None)
        self._workflow.default_target = "all"
        current_workdir = os.getcwd()
        try:
            inject()
            self._workflow.workdir(workdir)
            self._workflow.include(self._workflow.main_snakefile, overwrite_default_target=True)
        finally:
            if current_workdir:
                os.chdir(current_workdir)

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
            self._workflow,
        )
        return code

    def parse_nodes(self, in_loop=False):
        try:
            return self._parse_nodes(in_loop)
        except NoRuleShellException as ex:
            self._logger.error(str(ex))
            raise ex
        except UnknownRuleShellException as ex:
            self._logger.error(str(ex))
            raise ex
        except UnknownWorkflowTypeException as ex:
            self._logger.error(str(ex))
            raise ex
        except ParamNotFoundException as ex:
            self._logger.error(str(ex))
            raise ex

    def _parse_nodes(self, in_loop):
        if self._dag is None:
            self._build_dag()
        root_job = next(filter(lambda o: o.rule.name == self._workflow.default_target, self.jobs))
        root_inputs = {Parser._extract_job_id(self._define_id(name)): value for name, value in root_job.params.items()}
        root_outputs = set(
            [
                Parser._extract_job_id(s.id.split("#")[0] + "#" + re.sub(s.id + "/", "", s.outputSource))
                for s in list(
                    chain(
                        *[
                            map(
                                lambda output: Parser._define_object(
                                    {
                                        "id": self._define_id(output),
                                        "outputSource": self._define_id(f"{dep.name}/{output}"),
                                    }
                                ),
                                dep.output,
                            )
                            for dep in self._dag.dependencies[root_job]
                        ]
                    )
                )
            ]
        )
        node_list = []
        output_map = {}
        serial_id = 0
        for job in filter(lambda o: o.name != self._workflow.default_target, self.jobs):
            if job.is_shell:
                if job.shellcmd is None:
                    raise NoRuleShellException(job.rule.name)
            else:
                raise UnknownRuleShellException(job.rule.name)
            workflow_name = os.path.basename(job.shellcmd)
            serial_id += 1
            if workflow_name in WORKFLOW_NAMES:
                node = Node(serial_id, workflow_name, None, True, job.name)
            elif workflow_name.lower() == "Snakefile".lower():
                node = Node(serial_id, "workflow", None, False, job.name)
            else:
                raise UnknownWorkflowTypeException(job.rule.name)
            for name, value in job.params.items():
                if isinstance(value, ParamRule):
                    if value.rule is None:
                        param_job = job
                    else:
                        param_job = next(filter(lambda o: o.rule.name == value.rule.name, self.jobs))
                    if value.name not in param_job.rule.params.keys():
                        raise ParamNotFoundException(value.name, param_job.rule.name)
                    source = Parser._extract_job_id(self._define_id(f"{param_job.name}/{value.name}"))
                    if param_job.name == self._workflow.default_target:
                        source = source.replace(f"{param_job.name}/", "")
                    node.inputs.update({Parser._extract_job_id(self._define_id(f"{job.name}/{name}")): {"default": None, "source": source}})
                    continue
                if node.inputs is None:
                    node.inputs = dict()
                default_value = value
                source = None
                if job.dependencies:
                    if isinstance(value, list):
                        for item in list(value):
                            if item in job.dependencies.keys():
                                if source is None:
                                    source = list()
                                source.append(Parser._extract_job_id(self._define_id(f"{job.dependencies[item].name}/{item}")))
                        if source is not None:
                            default_value = None
                    else:
                        if default_value in job.dependencies.keys():
                            source = Parser._extract_job_id(self._define_id(f"{job.dependencies[default_value].name}/{default_value}"))
                            default_value = None
                node.inputs.update(
                    {
                        Parser._extract_job_id(self._define_id(f"{job.name}/{name}")): {
                            "default": default_value,
                            "source": source,
                        }
                    }
                )
            node.outputs = {Parser._extract_job_id(self._define_id(f"{job.name}/{output}")): {} for output in job.output}
            if not node.outputs:
                node.outputs = {Parser._extract_job_id(self._define_id(f"{job.name}/outDS")): {}}
            output_map.update({name: serial_id for name in node.outputs})
            scatter = getattr(job.rule, "scatter", None)
            if scatter:
                node.scatter = [
                    Parser._extract_job_id(self._define_id(f"{next(filter(lambda o: o.rule.name == v.rule.name, self.jobs)).name}/{v.name}"))
                    for v in [ParamRule(v.name, job.rule if v.rule is None else v.rule) for v in job.rule.scatter]
                ]
            else:
                node.scatter = None
            condition = getattr(job.rule, "condition", None)
            if condition:
                pattern = f'{param_of("")}(\w+)'
                for param_name in re.findall(pattern, condition):
                    if param_name not in job.rule.params.keys():
                        raise ParamNotFoundException(param_name, job.rule.name)
                condition = re.sub(r" *! *", r"!", condition)
                condition = re.sub(r"\|\|", r" || ", condition)
                condition = re.sub(r"&&", r" && ", condition)
                tokens = condition.split()
                param_left = None
                param_operator = None
                for token in tokens:
                    token = token.strip()
                    if token == "||":
                        param_operator = "or"
                        continue
                    elif token == "&&":
                        param_operator = "and"
                        continue
                    elif token.startswith(str(param_of(""))):
                        param_right = ConditionItem(str(re.findall(pattern, token)[0]))
                        if not param_left:
                            param_left = param_right
                            continue
                    elif token.startswith(f'!{param_of("")}'):
                        param_right = ConditionItem(str(re.findall(pattern, token)[0]), operator="not")
                        if not param_left:
                            param_left = param_right
                            continue
                    else:
                        raise UnknownConditionTokenException(token)
                    param_left = ConditionItem(param_left, param_right, param_operator)
                node.condition = param_left
                self._suppress_inputs(node.condition, node.inputs)
            node.loop = bool(getattr(job.rule, "loop", False))
            if node.loop or in_loop:
                node.in_loop = True
            if not node.is_leaf:
                subworkflow_file = os.path.join(self._workflow.basedir, job.shellcmd)
                subworkflow_parser = Parser(subworkflow_file, level=logging.DEBUG)
                node.sub_nodes, node.root_inputs = subworkflow_parser.parse_nodes()
            if root_outputs & set(node.outputs):
                node.is_tail = True
            node_list.append(node)
        for node in node_list:
            for name, data in node.inputs.items():
                if not data["source"]:
                    continue
                if isinstance(data["source"], list):
                    sources = data["source"]
                    is_str = False
                else:
                    sources = [data["source"]]
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
                    data["parent_id"] = parent_id_list
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
            targetfiles=set(),
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
        items = [re.search(r"[^/]+#.+$", s).group(0) for s in job_id]
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
        return f"{pathlib.Path(os.path.abspath(self._workflow.main_snakefile)).as_uri()}#{name}"

    @staticmethod
    def _define_object(dict_):
        return SimpleNamespace(**dict_)

    def _suppress_inputs(self, condition: ConditionItem, inputs):
        if condition.right is None and condition.operator == "not" and isinstance(condition.left, str):
            for name, data in inputs.items():
                if condition.left == name.split("/")[-1]:
                    data["suppressed"] = True
        else:
            for item in ["left", "right"]:
                condition_item = getattr(condition, item)
                if isinstance(condition_item, ConditionItem):
                    self._suppress_inputs(condition_item, inputs)
