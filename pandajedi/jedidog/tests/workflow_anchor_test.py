"""
Offline check of how a native workflow's brokerage anchor is computed and stored.

The anchor is what lets a step with no input of its own learn where the rest of its workflow has
to run. Two properties matter beyond the arithmetic, and both are the kind that only show up in
production: it has to exist before the first step submits a task, and storing it must not clobber
what the engine writes to the same JSON column.

Run from the repository root:  python3 pandajedi/jedidog/tests/workflow_anchor_test.py
"""

import datetime
import importlib.abc
import importlib.machinery
import json
import os
import sys
import types
import warnings
from typing import Any

warnings.filterwarnings("ignore", category=SyntaxWarning)

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))

AUTO_STUB_ROOTS = ("pandacommon", "idds", "pandaclient", "ruamel", "requests", "rucio", "numpy")


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


def stub(name, **attrs):
    module = types.ModuleType(name)
    for key, value in attrs.items():
        setattr(module, key, value)
    sys.modules[name] = module
    return module


class Log:
    def __getattr__(self, name):
        return lambda *a, **k: None


stub("pandacommon.pandalogger").__path__ = []
stub("pandacommon.pandalogger.PandaLogger", PandaLogger=lambda *a, **k: types.SimpleNamespace(getLogger=lambda n: Log()))
stub("pandacommon.pandautils").__path__ = []
stub("pandacommon.pandautils.base", SpecBase=object)
stub("pandacommon.pandautils.PandaUtils", naive_utcnow=datetime.datetime.utcnow, get_sql_IN_bind_variables=lambda *a, **k: (None, None))


class _AnyConfig:
    """Answers any attribute, so importing a module that reads a cfg file does not need one"""

    def __getattr__(self, name):
        return _AnyConfig()

    def __bool__(self):
        return False


stub("pandaserver.config", panda_config=_AnyConfig())
stub("pandajedi.jediconfig", jedi_config=_AnyConfig())

from pandajedi.jedibrokerage.workflow_affinity import (  # noqa: E402
    WORKFLOW_ANCHOR_PARAM,
    build_anchor_weights,
)


class FakeWorkflowSpec:
    """A workflow row, with the parameters column behaving as the real spec's does"""

    def __init__(self, workflow_id, parameters=None):
        self.workflow_id = workflow_id
        self.status = "running"
        self.parameters = json.dumps(parameters) if parameters else None

    def get_parameter(self, name):
        return json.loads(self.parameters).get(name) if self.parameters else None

    def set_parameter(self, name, value):
        parameter_map = json.loads(self.parameters) if self.parameters else {}
        parameter_map[name] = value
        self.parameters = json.dumps(parameter_map)


def check(label, condition, detail=""):
    print(f"  {'PASS' if condition else 'FAIL'}  {label}{'  ' + str(detail) if detail and not condition else ''}")
    return condition


def main():
    failures = 0

    print("\n=== the motivating workflow ===")
    # s1 has no input, s2's input is at SITE_A, s3 takes both outputs. Nothing has run yet, so the
    # only knowledge is where s2's input sits -- and that is what has to reach s1.
    anchor = build_anchor_weights(input_sites=["SITE_A"])
    failures += not check("the site holding a sibling's input is an anchor", anchor == {"SITE_A": 1.0}, anchor)
    failures += not check("...before anything has run", "SITE_A" in anchor)

    print("\n=== a step that has already run pulls harder than a prediction ===")
    anchor = build_anchor_weights(output_sites=["SITE_B"], input_sites=["SITE_A"])
    failures += not check("where output already sits outweighs where input might go", anchor["SITE_B"] > anchor["SITE_A"], anchor)

    print("\n=== a pinned sibling is the strongest signal ===")
    anchor = build_anchor_weights(pinned_sites=["SITE_P"], output_sites=["SITE_B"], input_sites=["SITE_A"])
    failures += not check("the pinned site leads", max(anchor.items(), key=lambda item: item[1])[0] == "SITE_P", anchor)

    print("\n=== a workflow nothing is known about has no anchor ===")
    failures += not check("empty in, empty out", build_anchor_weights() == {})
    failures += not check("...and an empty anchor is not stored", not build_anchor_weights(input_sites=[]))

    print("\n=== storing the anchor keeps what the engine wrote ===")
    # parameters is one JSON column that the engine also uses, for steps_final_time and the
    # failure reason among others. Writing a blob read before the engine's own write would drop
    # them, and losing steps_final_time restarts the output-settle grace period.
    spec = FakeWorkflowSpec(1, {"steps_final_time": "2026-11-05T09:00:00", "fail_reason": "step x failed"})
    spec.set_parameter(WORKFLOW_ANCHOR_PARAM, {"weights": {"SITE_A": 1.0}, "computed_at": "2026-11-05T10:00:00"})
    failures += not check("the engine's grace timestamp survives", spec.get_parameter("steps_final_time") == "2026-11-05T09:00:00")
    failures += not check("so does its failure reason", spec.get_parameter("fail_reason") == "step x failed")
    failures += not check("and the anchor is there", spec.get_parameter(WORKFLOW_ANCHOR_PARAM)["weights"] == {"SITE_A": 1.0})

    print("\n=== a stale copy is what would lose them ===")
    # This is the bug the workflow lock exists to prevent: compute against a spec read earlier,
    # and whatever the engine added in between is gone.
    engine_view = FakeWorkflowSpec(2, {"steps_final_time": "2026-11-05T09:00:00"})
    stale_copy = FakeWorkflowSpec(2, {})
    stale_copy.set_parameter(WORKFLOW_ANCHOR_PARAM, {"weights": {"SITE_A": 1.0}})
    failures += not check(
        "a blob written from a stale read drops the engine's key",
        stale_copy.get_parameter("steps_final_time") is None and engine_view.get_parameter("steps_final_time") is not None,
    )

    print("\n=== freshness decides whether the lookups are repeated ===")
    from pandajedi.jedidog import AtlasWorkflowManagerWatchDog as wd

    watchdog = wd.AtlasWorkflowManagerWatchDog.__new__(wd.AtlasWorkflowManagerWatchDog)
    now = datetime.datetime.utcnow()
    failures += not check("a workflow with no anchor is never fresh", not watchdog.anchor_is_fresh(FakeWorkflowSpec(3)))
    failures += not check(
        "...nor one whose anchor has no timestamp",
        not watchdog.anchor_is_fresh(FakeWorkflowSpec(3, {WORKFLOW_ANCHOR_PARAM: {"weights": {"A": 1}}})),
    )
    just_now = FakeWorkflowSpec(3, {WORKFLOW_ANCHOR_PARAM: {"weights": {"A": 1}, "computed_at": now.isoformat()}})
    failures += not check("one just computed is fresh", watchdog.anchor_is_fresh(just_now))
    old = now - datetime.timedelta(seconds=wd.ANCHOR_REFRESH_SEC + 60)
    aged = FakeWorkflowSpec(3, {WORKFLOW_ANCHOR_PARAM: {"weights": {"A": 1}, "computed_at": old.isoformat()}})
    failures += not check("an aged one is recomputed", not watchdog.anchor_is_fresh(aged))
    broken = FakeWorkflowSpec(3, {WORKFLOW_ANCHOR_PARAM: {"weights": {"A": 1}, "computed_at": "not-a-time"}})
    failures += not check("an unparsable timestamp is recomputed rather than trusted", not watchdog.anchor_is_fresh(broken))

    print("\n=== replica lookups go through the per-VO DDM client ===")
    # The watchdog is handed the multi-VO DDMInterface wrapper, which has no replica methods of
    # its own. Passing it straight through failed every lookup, so a workflow had no input sites
    # and therefore no anchor before its steps were submitted.
    vo_client = object()

    class FakeDDMInterface:
        def getInterface(self, vo, group=None):
            return vo_client if vo == "atlas" else None

    received: dict[str, Any] = {}

    def fake_get_sites_with_data(site_list, site_mapper, ddm_if, dataset_name, *args):
        received["ddm_if"] = ddm_if
        return wd.Interaction.SC_SUCCEEDED, {"SITE_A": {}}

    def fake_sites_with_disk(data_site_map, includeTape=False):
        return list(data_site_map)

    # untyped handles, since these stand-ins deliberately do not match the real signatures
    stand_in: Any = watchdog
    broker_utils: Any = wd.AtlasBrokerUtils
    stand_in.ddmIF = FakeDDMInterface()
    stand_in.vo = "atlas"
    original = (broker_utils.get_sites_with_data, broker_utils.getAnalSitesWithDataDisk)
    broker_utils.get_sites_with_data = fake_get_sites_with_data
    broker_utils.getAnalSitesWithDataDisk = fake_sites_with_disk
    try:
        sites = stand_in.sites_holding_dataset(["SITE_A"], None, "some:dataset", Log())
    finally:
        broker_utils.get_sites_with_data, broker_utils.getAnalSitesWithDataDisk = original
    failures += not check("the per-VO client is what reaches the lookup", received.get("ddm_if") is vo_client, received)
    failures += not check("...and its answer comes back", sites == ["SITE_A"], sites)

    print("\n=== workflow 148: the free step waits for the data-bound ones ===")
    # The live case. make_signal and make_background_1 read root inputs and are placed by data
    # locality; generate_some reads nothing and was brokered before them, against a prediction.
    # make_background_2 reads a root input and generate_some's output, so it must not be waited
    # for: it waits on generate_some itself.
    from pandaserver.taskbuffer.JediTaskSpec import (
        WORKFLOW_HOLDUP_BROKERAGE,
        JediTaskSpec,
    )

    def step(step_id, name, inputs, outputs, task_id=None, takes_input=True, status="running"):
        job_params = [{"param_type": "input", "dataset": "x"}] if takes_input else []
        definition = {"task_params": {"jobParameters": job_params}, "input_data_dict": {i: {} for i in inputs}, "output_data_list": outputs}
        return types.SimpleNamespace(
            step_id=step_id, name=name, flavor="panda_task", status=status, target_id=str(task_id) if task_id else None, definition_json_map=definition
        )

    def task(task_id, status="running", split_rule=None):
        task_spec = JediTaskSpec()
        task_spec.jediTaskID = task_id
        task_spec.status = status
        task_spec.site = None
        task_spec.splitRule = split_rule
        return task_spec

    class FakeTaskBuffer:
        def __init__(self, steps, tasks, output_sites):
            self.steps, self.tasks, self.output_sites = steps, tasks, output_sites
            self.events = []
            self.workflow = FakeWorkflowSpec(148)

        def get_steps_of_workflow(self, workflow_id):
            return self.steps

        def get_data_of_workflow(self, workflow_id, type_filter_list=None):
            return [types.SimpleNamespace(name="signal", target_id="ds:signal"), types.SimpleNamespace(name="background", target_id="ds:background/")]

        def get_site_mapper(self):
            return types.SimpleNamespace(siteSpecList={}, getSite=lambda name: types.SimpleNamespace(get_unified_name=lambda: name))

        def getTaskWithID_JEDI(self, task_id, *args):
            return True, self.tasks[task_id]

        def getDatasetsWithJediTaskID_JEDI(self, task_id, types_):
            # an outDS and a log dataset at the same site, as analysis instantiates them
            return True, [types.SimpleNamespace(site=site) for site in self.output_sites.get(task_id, [])]

        def getConfigValue(self, *args, **kwargs):
            return True

        def query_workflows(self, **kwargs):
            return [self.workflow]

        def update_workflow(self, workflow_spec):
            self.events.append("store anchor")

        def updateTask_JEDI(self, task_spec, criteria, *args, **kwargs):
            self.events.append(f"update {task_spec.jediTaskID} {task_spec.splitRule}")

        def release_task_on_hold(self, task_id):
            self.events.append(f"release {task_id}")
            return True

        def push_task_trigger_message(self, msg_type, task_id):
            self.events.append(f"trigger {msg_type} {task_id}")

    class FakeWorkflowInterface:
        def __init__(self, tbif, locked=False):
            self.tbif, self.locked = tbif, locked

        def workflow_lock(self, workflow_id):
            import contextlib

            @contextlib.contextmanager
            def lock():
                yield None if self.locked else self.tbif.workflow

            return lock()

    def run_pass(tbif, locked=False):
        dog: Any = wd.AtlasWorkflowManagerWatchDog.__new__(wd.AtlasWorkflowManagerWatchDog)
        dog.taskBufferIF = tbif
        dog.vo = "atlas"
        dog.workflow_interface = FakeWorkflowInterface(tbif, locked)
        dog.get_process_lock = lambda *a, **k: True
        replicas = {"ds:signal": ["CERN-T0", "CERN"], "ds:background/": ["CERN-T0", "CERN", "INFN-CNAF"]}
        dog.sites_holding_dataset = lambda analysis_sites, site_mapper, dataset_name, log: replicas[dataset_name]
        dog.doComputeWorkflowAnchors()
        return dog

    def workflow_148(background_placed):
        steps = [
            step(1, "make_signal", ["signal"], ["make_signal/outDS"], task_id=101),
            step(2, "make_background_1", ["background"], ["make_background_1/outDS"], task_id=102),
            step(3, "generate_some", [], ["generate_some/outDS"], task_id=103, takes_input=False),
            step(4, "premix", ["make_signal/outDS", "make_background_1/outDS"], ["premix/outDS"], status="registered"),
            step(5, "make_background_2", ["background", "generate_some/outDS"], ["make_background_2/outDS"], status="registered"),
        ]
        tasks = {101: task(101), 102: task(102), 103: task(103, status="pending", split_rule=f"WH={WORKFLOW_HOLDUP_BROKERAGE},NG=3")}
        output_sites = {101: ["CERN-T0", "CERN-T0"]}
        if background_placed:
            output_sites[102] = ["CERN-T0", "CERN-T0"]
        return FakeTaskBuffer(steps, tasks, output_sites)

    tbif = workflow_148(background_placed=False)
    dog = run_pass(tbif)
    anchor_inputs, holdup = dog.gather_anchor_inputs(148, Log())
    failures += not check("only make_background_1 is waited for", holdup["unplaced_bound_step_ids"] == [2], holdup["unplaced_bound_step_ids"])
    failures += not check("generate_some is the held task", [t.jediTaskID for t in holdup["held_task_specs"]] == [103])
    failures += not check("the workflow is unsettled", holdup["unsettled"] is True)
    failures += not check("an outDS and a log at one site count as one step there", anchor_inputs["output_sites"] == ["CERN-T0"], anchor_inputs["output_sites"])
    # a bound step submitted but not yet placed still says where it is going
    failures += not check(
        "an unplaced submitted step still contributes its input sites",
        anchor_inputs["input_sites"].count("INFN-CNAF") == 2,
        anchor_inputs["input_sites"],
    )
    failures += not check("the anchor is stored", "store anchor" in tbif.events, tbif.events)
    failures += not check("nothing is released while a bound step is unplaced", not any(e.startswith("release") for e in tbif.events), tbif.events)
    stored = tbif.workflow.get_parameter(WORKFLOW_ANCHOR_PARAM)
    failures += not check("an unsettled anchor is never fresh, so the next pass looks again", not dog.anchor_is_fresh(tbif.workflow), stored)

    print("\n=== ...and is released once they are placed ===")
    tbif = workflow_148(background_placed=True)
    run_pass(tbif)
    failures += not check("the held task is released", "release 103" in tbif.events, tbif.events)
    failures += not check(
        "the anchor is stored before anything is released",
        "store anchor" in tbif.events and tbif.events.index("store anchor") < tbif.events.index("release 103"),
        tbif.events,
    )
    failures += not check("the brokerage bit is cleared and the rest of the rule kept", "update 103 NG=3" in tbif.events, tbif.events)
    failures += not check("the contents feeder is triggered", "trigger jedi_contents_feeder 103" in tbif.events, tbif.events)
    weights = tbif.workflow.get_parameter(WORKFLOW_ANCHOR_PARAM)["weights"]
    failures += not check("the anchor names where the bound steps went", max(weights, key=weights.get) == "CERN-T0", weights)

    print("\n=== a workflow the engine holds is neither anchored nor released ===")
    # the released tasks must see the new anchor, so no anchor means no release either
    tbif = workflow_148(background_placed=True)
    run_pass(tbif, locked=True)
    failures += not check("nothing released when the anchor could not be stored", not any(e.startswith("release") for e in tbif.events), tbif.events)

    print("\n=== the hold expires with the regular pending cycle ===")
    from pandajedi.jedibrokerage.workflow_affinity import brokerage_holdup_expired

    created = datetime.datetime(2026, 11, 16, 10, 0)
    failures += not check("a first feed minutes after creation still holds", not brokerage_holdup_expired(created, created + datetime.timedelta(minutes=3), 60))
    failures += not check("a feed after the pending reactivation lets go", brokerage_holdup_expired(created, created + datetime.timedelta(minutes=61), 60))
    failures += not check("no creation time never holds for ever", brokerage_holdup_expired(None, created, 60))

    print("\n=== the pass runs before the workflows are processed ===")
    # An analysis task is job-generated almost as soon as it is submitted, so an anchor computed
    # after the engine advances a workflow would arrive too late for the first step.
    import inspect

    source = inspect.getsource(wd.AtlasWorkflowManagerWatchDog.doAction)
    failures += not check(
        "doComputeWorkflowAnchors is called before doProcessWorkflows",
        source.index("doComputeWorkflowAnchors") < source.index("doProcessWorkflows"),
        source,
    )

    print(f"\n{'ALL CHECKS PASSED' if not failures else f'{failures} CHECK(S) FAILED'}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
