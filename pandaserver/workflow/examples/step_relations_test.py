"""
Offline check of WorkflowInterface.get_step_relations.

The engine is data-driven: a step starts because its inputs are good, never because a parent step
finished, so it holds no step-to-step edge of its own. Consumers that model a chain as related
tasks -- DEFT above all -- need those edges, and this derives them from what is recorded: each
datum's source_step_id, and each step's input_data_dict.

The shape checked here is the production chain PanDA ran as workflow 133, including the branch
where deriv_prw and deriv_phys both consume merge_aod/AOD, and the external background dataset
that must contribute no parent.

Run from the repository root:  python3 pandaserver/workflow/examples/step_relations_test.py
"""

import importlib.abc
import importlib.machinery
import json
import os
import sys
import types
import warnings

# Unrelated modules in the import graph still carry unescaped regex literals; their SyntaxWarnings
# say nothing about this check.
warnings.filterwarnings("ignore", category=SyntaxWarning)

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
sys.path.insert(0, REPO_ROOT)

AUTO_STUB_ROOTS = ("idds", "pandaclient", "ruamel", "requests", "rucio")


class AutoStubFinder(importlib.abc.MetaPathFinder, importlib.abc.Loader):
    def find_spec(self, name, path=None, target=None):
        if name.split(".")[0] in AUTO_STUB_ROOTS:
            return importlib.machinery.ModuleSpec(name, self, is_package=True)
        return None

    def create_module(self, spec):
        module = types.ModuleType(spec.name)
        module.__path__ = []
        return module

    def exec_module(self, module):
        class Anything:
            def __init__(self, *a, **k):
                pass

            def __call__(self, *a, **k):
                return self

        module.__getattr__ = lambda name: Anything


sys.meta_path.insert(0, AutoStubFinder())

LOGGED: dict[str, list[str]] = {"warning": [], "debug": []}


class Log:
    def __init__(self, *a, **k):
        pass

    def info(self, m):
        pass

    def debug(self, m):
        LOGGED["debug"].append(str(m))

    def warning(self, m):
        LOGGED["warning"].append(str(m))

    def error(self, m):
        LOGGED["warning"].append(str(m))


class SpecBase:
    """Enough of pandacommon's SpecBase for the real spec classes to be instantiated offline"""

    def __init__(self):
        for attribute in getattr(self, "attributes", ()):
            setattr(self, attribute, None)


def stub(name, **attrs):
    module = types.ModuleType(name)
    for key, value in attrs.items():
        setattr(module, key, value)
    sys.modules[name] = module
    return module


stub("pandacommon")
stub("pandacommon.pandautils").__path__ = []
stub("pandacommon.pandautils.base", SpecBase=SpecBase)
stub("pandacommon.pandautils.PandaUtils", naive_utcnow=lambda: None, get_sql_IN_bind_variables=lambda *a, **k: (None, None))
stub("pandacommon.pandautils.thread_utils", GenericThread=object)
stub("pandacommon.pandalogger").__path__ = []
stub("pandacommon.pandalogger.LogWrapper", LogWrapper=Log)
stub("pandacommon.pandalogger.PandaLogger", PandaLogger=lambda: types.SimpleNamespace(getLogger=lambda n: None))
stub("pandaserver.config", panda_config=types.SimpleNamespace(schemaJEDI="ATLAS_PANDA", schemaDEFT="ATLAS_DEFT"))

from pandaserver.workflow import workflow_core  # noqa: E402
from pandaserver.workflow.workflow_base import (  # noqa: E402
    WFDataSpec,
    WFDataType,
    WFStepSpec,
    WFStepStatus,
    WFStepType,
)

# The production chain of workflow 133: step name -> (inputs it consumes, outputs it produces).
# recon also reads rdo_bkg, which the workflow does not produce.
CHAIN = [
    ("evgen", [], ["evgen/EVNT"]),
    ("merge_evnt", ["evgen/EVNT"], ["merge_evnt/EVNT"]),
    ("simul", ["merge_evnt/EVNT"], ["simul/HITS"]),
    ("merge_hits", ["simul/HITS"], ["merge_hits/HITS"]),
    ("recon", ["merge_hits/HITS", "rdo_bkg"], ["recon/AOD"]),
    ("merge_aod", ["recon/AOD"], ["merge_aod/AOD"]),
    ("deriv_prw", ["merge_aod/AOD"], ["deriv_prw/NTUP_PILEUP"]),
    ("merge_ntup", ["deriv_prw/NTUP_PILEUP"], ["merge_ntup/NTUP_PILEUP"]),
    ("deriv_phys", ["merge_aod/AOD"], ["deriv_phys/DAOD_PHYS", "deriv_phys/DAOD_PHYSLITE"]),
]

TASK_IDS = {
    "evgen": "52382519",
    "merge_evnt": "52382898",
    "simul": "52383720",
    "merge_hits": "52397622",
    "recon": "52401216",
    "merge_aod": "52416493",
    "deriv_prw": "52421202",
    "merge_ntup": "52421461",
    "deriv_phys": "52421203",
}


def make_step(step_id, name, inputs, flavor="panda_task", status=WFStepStatus.done, target_id=None, workflow_id=133):
    step_spec = WFStepSpec()
    step_spec.step_id = step_id
    step_spec.name = name
    step_spec.workflow_id = workflow_id
    step_spec.type = WFStepType.sub_workflow if flavor != "panda_task" else WFStepType.ordinary
    step_spec.flavor = flavor
    step_spec.status = status
    step_spec.target_id = target_id
    step_spec.definition_json = json.dumps({"input_data_dict": {name: {} for name in inputs}})
    return step_spec


def make_data(name, source_step_id, data_type=WFDataType.mid, workflow_id=133):
    data_spec = WFDataSpec()
    data_spec.name = name
    data_spec.workflow_id = workflow_id
    data_spec.source_step_id = source_step_id
    data_spec.type = data_type
    return data_spec


def build_chain():
    """The workflow-133 steps and data, with step ids 1..9 in chain order"""
    step_specs = []
    data_specs = [make_data("rdo_bkg", None, WFDataType.input)]
    for index, (name, inputs, outputs) in enumerate(CHAIN, start=1):
        step_specs.append(make_step(index, name, inputs, target_id=TASK_IDS[name]))
        for output_name in outputs:
            data_specs.append(make_data(output_name, index))
    return step_specs, data_specs


class FakeTaskBuffer:
    """Serves one or more workflows, so the nested cases can be built"""

    def __init__(self, step_specs, data_specs, by_workflow=None):
        self.by_workflow = dict(by_workflow or {})
        self.by_workflow.setdefault(133, (step_specs, data_specs))

    def get_steps_of_workflow(self, workflow_id, status_filter_list=None, status_exclusion_list=None):
        return list(self.by_workflow.get(workflow_id, ([], []))[0])

    def get_data_of_workflow(self, workflow_id, status_filter_list=None, status_exclusion_list=None, type_filter_list=None):
        return list(self.by_workflow.get(workflow_id, ([], []))[1])

    def get_steps_by_target_id(self, target_id, flavor_filter_list=None):
        found = []
        for step_specs, _ in self.by_workflow.values():
            for step_spec in step_specs:
                if step_spec.target_id == target_id and (not flavor_filter_list or step_spec.flavor in flavor_filter_list):
                    found.append(step_spec)
        return found


def make_interface(step_specs, data_specs, by_workflow=None):
    """A WorkflowInterface with no message broker or DDM behind it"""
    interface = workflow_core.WorkflowInterface.__new__(workflow_core.WorkflowInterface)
    interface.tbif = FakeTaskBuffer(step_specs, data_specs, by_workflow)
    interface.full_pid = "test-0-0"
    interface.plugin_map = {}
    interface.mb_proxy = None
    return interface


def check(label, condition, detail=""):
    print(f"  {'PASS' if condition else 'FAIL'}  {label}{'  ' + str(detail) if detail and not condition else ''}")
    return condition


def by_name(result, name):
    """The task entry with that step name, or None"""
    return next((task for task in result["tasks"] if task["name"] == name), None)


def task_by_key_name(result, key):
    """The step name behind a task key, so expectations read as names rather than ids"""
    return next(task["name"] for task in result["tasks"] if task["key"] == key)


def parents_by_name(result):
    by_id = {step["step_id"]: step["name"] for step in result["steps"]}
    return {step["name"]: sorted(by_id[parent] for parent in step["parent_step_ids"]) for step in result["steps"]}


def main():
    failures = 0

    print("\n=== the production chain of workflow 133 ===")
    step_specs, data_specs = build_chain()
    result = make_interface(step_specs, data_specs).get_step_relations(133)
    failures += not check("a result is returned", result is not None)
    if result is None:
        print("\n1 CHECK(S) FAILED")
        return 1
    failures += not check("the workflow id is echoed back", result["workflow_id"] == 133, result["workflow_id"])
    failures += not check("every step is reported", len(result["steps"]) == 9, len(result["steps"]))
    failures += not check("steps come back in step_id order", [s["step_id"] for s in result["steps"]] == list(range(1, 10)))

    relations = parents_by_name(result)
    expected = {
        "evgen": [],
        "merge_evnt": ["evgen"],
        "simul": ["merge_evnt"],
        "merge_hits": ["simul"],
        "recon": ["merge_hits"],
        "merge_aod": ["recon"],
        "deriv_prw": ["merge_aod"],
        "merge_ntup": ["deriv_prw"],
        "deriv_phys": ["merge_aod"],
    }
    for name, expected_parents in expected.items():
        failures += not check(f"{name} <- {expected_parents or 'nothing'}", relations[name] == expected_parents, relations[name])

    print("\n=== what the edges do and do not come from ===")
    failures += not check("an entry step has no parent rather than itself", relations["evgen"] == [])
    failures += not check("the external rdo_bkg contributes no parent to recon", relations["recon"] == ["merge_hits"], relations["recon"])
    branch = [name for name, parents in relations.items() if parents == ["merge_aod"]]
    failures += not check("merge_aod is the parent of two steps", sorted(branch) == ["deriv_phys", "deriv_prw"], branch)

    print("\n=== each step carries what a task view needs ===")
    evgen = next(s for s in result["steps"] if s["name"] == "evgen")
    failures += not check("the step's target is reported", evgen["target_id"] == "52382519", evgen["target_id"])
    failures += not check("so is its flavor", evgen["flavor"] == "panda_task", evgen["flavor"])
    failures += not check("and its status", evgen["status"] == WFStepStatus.done, evgen["status"])

    print("\n=== a step with several parents ===")
    # A join: one step consuming the outputs of two independent steps.
    step_specs = [
        make_step(1, "left", []),
        make_step(2, "right", []),
        make_step(3, "join", ["left/OUT", "right/OUT"]),
    ]
    data_specs = [make_data("left/OUT", 1), make_data("right/OUT", 2)]
    result = make_interface(step_specs, data_specs).get_step_relations(133)
    join = next(s for s in result["steps"] if s["name"] == "join")
    failures += not check("both parents are reported", join["parent_step_ids"] == [1, 2], join["parent_step_ids"])

    print("\n=== a running workflow ===")
    # simul is running and has a task; everything after it has neither started nor got a task.
    step_specs, data_specs = build_chain()
    for step_spec in step_specs:
        if step_spec.step_id > 3:
            step_spec.status = WFStepStatus.registered
            step_spec.target_id = None
        elif step_spec.step_id == 3:
            step_spec.status = WFStepStatus.running
    # only the data produced so far has a producer recorded
    for data_spec in data_specs:
        if data_spec.source_step_id is not None and data_spec.source_step_id > 3:
            data_spec.source_step_id = None
    result = make_interface(step_specs, data_specs).get_step_relations(133)
    relations = parents_by_name(result)
    failures += not check("the whole graph is reported, not only the started part", len(result["steps"]) == 9, len(result["steps"]))
    failures += not check("edges among started steps are there", relations["simul"] == ["merge_evnt"], relations["simul"])
    # simul is running, so its output is already bound to it: the edge into the next step exists
    # before that output is complete, which is what makes the graph usable mid-flight.
    failures += not check("a running step is already a parent", relations["merge_hits"] == ["simul"], relations["merge_hits"])
    failures += not check("a step whose producer has not started has no parent yet", relations["recon"] == [], relations["recon"])
    not_started = next(s for s in result["steps"] if s["name"] == "recon")
    failures += not check("a step that has not started reports no target", not_started["target_id"] is None, not_started["target_id"])
    failures += not check("...but is still in the answer with its status", not_started["status"] == WFStepStatus.registered, not_started["status"])

    print("\n=== a nested sub-workflow step ===")
    step_specs = [
        make_step(1, "prepare", [], target_id="52400001"),
        make_step(2, "child", ["prepare/OUT"], flavor="sub_workflow", target_id="277"),
    ]
    data_specs = [make_data("prepare/OUT", 1)]
    result = make_interface(step_specs, data_specs).get_step_relations(133)
    child = next(s for s in result["steps"] if s["name"] == "child")
    failures += not check("the sub-workflow step is reported as itself", child["parent_step_ids"] == [1], child["parent_step_ids"])
    failures += not check("its target is the child workflow id", child["target_id"] == "277", child["target_id"])
    failures += not check("its flavor says a caller can recurse", child["flavor"] == "sub_workflow", child["flavor"])

    print("\n=== data the engine cannot relate ===")
    del LOGGED["warning"][:]
    step_specs = [make_step(1, "producer", []), make_step(2, "consumer", ["producer/OUT", "never/heard/of"])]
    data_specs = [make_data("producer/OUT", 1)]
    result = make_interface(step_specs, data_specs).get_step_relations(133)
    consumer = next(s for s in result["steps"] if s["name"] == "consumer")
    failures += not check("an unknown input is skipped, not fatal", consumer["parent_step_ids"] == [1], consumer["parent_step_ids"])

    # A datum naming a producer outside this workflow must not become a dangling edge.
    del LOGGED["warning"][:]
    data_specs = [make_data("producer/OUT", 1), make_data("never/heard/of", 99)]
    result = make_interface(step_specs, data_specs).get_step_relations(133)
    consumer = next(s for s in result["steps"] if s["name"] == "consumer")
    failures += not check("a producer outside the workflow is not reported as a parent", consumer["parent_step_ids"] == [1], consumer["parent_step_ids"])
    failures += not check("...and is warned about", any("not in this workflow" in m for m in LOGGED["warning"]), LOGGED["warning"])

    # A step recorded as producing its own input would make a consumer walking parents loop.
    del LOGGED["warning"][:]
    step_specs = [make_step(1, "selfish", ["selfish/OUT"])]
    data_specs = [make_data("selfish/OUT", 1)]
    result = make_interface(step_specs, data_specs).get_step_relations(133)
    failures += not check("a step is never its own parent", result["steps"][0]["parent_step_ids"] == [], result["steps"][0]["parent_step_ids"])
    failures += not check("...and is warned about", any("its own input" in m for m in LOGGED["warning"]), LOGGED["warning"])

    print("\n=== a workflow with no steps ===")
    failures += not check("reports nothing rather than an empty graph", make_interface([], []).get_step_relations(999) is None)

    print("\n=== the answer is JSON-serializable ===")
    step_specs, data_specs = build_chain()
    result = make_interface(step_specs, data_specs).get_step_relations(133)
    try:
        json.dumps(result)
        failures += not check("json.dumps accepts it as it stands", True)
    except Exception as exc:
        failures += not check("json.dumps accepts it as it stands", False, exc)

    print("\n=== the task view of workflow 133 ===")
    step_specs, data_specs = build_chain()
    result = make_interface(step_specs, data_specs).get_task_relations(133)
    failures += not check("a result is returned", result is not None)
    task_by_key = {task["key"]: task for task in result["tasks"]}
    failures += not check("every step became a task", len(result["tasks"]) == 9, len(result["tasks"]))
    failures += not check("task ids come from the step targets", task_by_key["133:5"]["task_id"] == 52401216, task_by_key["133:5"]["task_id"])
    task_parents = {task["name"]: sorted(task_by_key[key]["name"] for key in task["parents"]) for task in result["tasks"]}
    failures += not check("the chain is unchanged by the projection", task_parents == expected, task_parents)

    print("\n=== a step that is not a task is collapsed, not dropped ===")
    # task A -> a step with some other target -> task B must report A as B's parent.
    step_specs = [
        make_step(1, "A", [], target_id="52400001"),
        make_step(2, "middle", ["A/OUT"], flavor="future_thing", target_id="whatever"),
        make_step(3, "B", ["middle/OUT"], target_id="52400003"),
    ]
    data_specs = [make_data("A/OUT", 1), make_data("middle/OUT", 2)]
    result = make_interface(step_specs, data_specs).get_task_relations(133)
    names = sorted(task["name"] for task in result["tasks"])
    failures += not check("the non-task step is not a node", names == ["A", "B"], names)
    task_b = next(task for task in result["tasks"] if task["name"] == "B")
    failures += not check("the relation passes through it", [task_by_key_name(result, k) for k in task_b["parents"]] == ["A"], task_b["parents"])

    print("\n=== a nested workflow is replaced by the tasks inside it ===")
    # outer: prepare -> child(workflow 277) -> after
    outer_steps = [
        make_step(1, "prepare", [], target_id="52400001"),
        make_step(2, "child", ["prepare/OUT"], flavor="sub_workflow", target_id="277"),
        make_step(3, "after", ["child/OUT"], target_id="52400009"),
    ]
    outer_data = [make_data("prepare/OUT", 1), make_data("child/OUT", 2)]
    inner_steps = [
        make_step(1, "inner_first", [], target_id="52400005", workflow_id=277),
        make_step(2, "inner_last", ["inner_first/OUT"], target_id="52400006", workflow_id=277),
    ]
    inner_data = [make_data("inner_first/OUT", 1, workflow_id=277)]
    interface = make_interface(outer_steps, outer_data, by_workflow={133: (outer_steps, outer_data), 277: (inner_steps, inner_data)})
    result = interface.get_task_relations(133)
    names = sorted(task["name"] for task in result["tasks"])
    failures += not check("the inner tasks appear", names == ["after", "inner_first", "inner_last", "prepare"], names)
    failures += not check("the sub-workflow step itself is not a node", "child" not in names)
    failures += not check(
        "the child's entry task takes the outer parent", [task_by_key_name(result, k) for k in by_name(result, "inner_first")["parents"]] == ["prepare"]
    )
    failures += not check(
        "the outer consumer takes the child's tail task", [task_by_key_name(result, k) for k in by_name(result, "after")["parents"]] == ["inner_last"]
    )
    failures += not check("the inner chain is kept", [task_by_key_name(result, k) for k in by_name(result, "inner_last")["parents"]] == ["inner_first"])
    failures += not check("keys are unique across the workflows walked", len({task["key"] for task in result["tasks"]}) == 4)

    print("\n=== a step with no task yet is a placeholder ===")
    step_specs = [make_step(1, "done_one", [], target_id="52400001"), make_step(2, "not_yet", ["done_one/OUT"], status=WFStepStatus.registered)]
    data_specs = [make_data("done_one/OUT", 1)]
    result = make_interface(step_specs, data_specs).get_task_relations(133)
    pending = by_name(result, "not_yet")
    failures += not check("it is still reported", pending is not None)
    failures += not check("with no task id", pending["task_id"] is None, pending["task_id"])
    failures += not check("and its parent, so the shape is visible", [task_by_key_name(result, k) for k in pending["parents"]] == ["done_one"])

    print("\n=== steps are resolved parents-first whatever their ids ===")
    # ids deliberately against the flow: step 3 feeds step 1.
    step_specs = [make_step(3, "first", [], target_id="52400001"), make_step(1, "second", ["first/OUT"], target_id="52400002")]
    data_specs = [make_data("first/OUT", 3)]
    result = make_interface(step_specs, data_specs).get_task_relations(133)
    failures += not check("the edge survives the ordering", [task_by_key_name(result, k) for k in by_name(result, "second")["parents"]] == ["first"])

    print("\n=== entering by task id ===")
    step_specs, data_specs = build_chain()
    interface = make_interface(step_specs, data_specs)
    result = interface.get_task_relations_of_task(52401216)
    failures += not check("the workflow of that task is reported", result is not None and result["workflow_id"] == 133)
    failures += not check("the whole chain comes back", len(result["tasks"]) == 9, len(result["tasks"]))
    failures += not check("the task asked about is named", result["asked_for"] == "133:5", result["asked_for"])
    failures += not check("a task no workflow runs reports nothing", interface.get_task_relations_of_task(99999999) is None)

    print(f"\n{'ALL CHECKS PASSED' if not failures else f'{failures} CHECK(S) FAILED'}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
