"""
Offline check of when a running workflow is allowed to become done.

A step reaches done as soon as its task does, but the task's output dataset is closed in DDM
slightly later, so the data pass that moves an output from generating_suffice to done_generated
necessarily runs a cycle behind the step transition. Finishing the workflow on the step transition
alone leaves that output non-terminal for good, since a done workflow is no longer in
active_statuses and is never processed again. This was seen for real: workflow 133 finished with
merge_ntup/NTUP_PILEUP frozen in generating_suffice.

So the workflow waits for its outputs, and the wait is bounded, so that an output whose dataset is
never closed cannot keep the workflow running forever.

Run from the repository root:  python3 pandaserver/workflow/examples/workflow_done_transition_test.py
"""

import importlib.abc
import importlib.machinery
import os
import sys
import types
import warnings
from datetime import datetime, timedelta

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


def stub(name, **attrs):
    module = types.ModuleType(name)
    for key, value in attrs.items():
        setattr(module, key, value)
    sys.modules[name] = module
    return module


class Log:
    """Collects what the code under test logs, so the warnings it emits can be asserted on"""

    def __init__(self, *a, **k):
        self.messages = []

    def info(self, m):
        self.messages.append(("info", str(m)))

    def debug(self, m):
        self.messages.append(("debug", str(m)))

    def warning(self, m):
        self.messages.append(("warning", str(m)))

    def error(self, m):
        self.messages.append(("error", str(m)))


class SpecBase:
    """Enough of pandacommon's SpecBase for the real spec classes to be instantiated offline"""

    def __init__(self):
        for attribute in getattr(self, "attributes", ()):
            setattr(self, attribute, None)


stub("pandacommon")
stub("pandacommon.pandautils").__path__ = []
stub("pandacommon.pandautils.base", SpecBase=SpecBase)
stub("pandacommon.pandautils.PandaUtils", naive_utcnow=datetime.utcnow, get_sql_IN_bind_variables=lambda *a, **k: (None, None))
stub("pandacommon.pandalogger").__path__ = []
stub("pandacommon.pandalogger.LogWrapper", LogWrapper=Log)
stub("pandacommon.pandalogger.PandaLogger", PandaLogger=lambda: types.SimpleNamespace(getLogger=lambda n: None))
stub("pandaserver.config", panda_config=types.SimpleNamespace(schemaJEDI="ATLAS_PANDA", schemaDEFT="ATLAS_DEFT"))

from pandaserver.workflow import workflow_core  # noqa: E402
from pandaserver.workflow.workflow_base import (  # noqa: E402
    WFDataStatus,
    WFDataTargetCheckStatus,
    WFDataType,
    WFStepStatus,
    WorkflowSpec,
    WorkflowStatus,
)

NOW = datetime(2026, 9, 10, 12, 0, 0)

# process_workflow_running stamps its own "now" from naive_utcnow, which the bounded-wait case has
# to be able to place relative to the recorded wait, so the clock is pinned here.
workflow_core.naive_utcnow = lambda: NOW


class FakeData:
    def __init__(self, name, status, data_type=WFDataType.output, data_id=1, flavor="ddm_collection"):
        self.name = name
        self.status = status
        self.type = data_type
        self.data_id = data_id
        self.workflow_id = 133
        self.flavor = flavor
        self.end_time = None
        self.check_time = None


class FakeStep:
    def __init__(self, name, status=WFStepStatus.done):
        self.name = name
        self.status = status
        self.flavor = "panda_task"
        self.target_id = "1"
        self.member_id = 1


class FakeTaskBuffer:
    def __init__(self, data_specs, step_specs):
        self.data_specs = data_specs
        self.step_specs = step_specs
        self.updated_workflows = []
        self.updated_data = []

    def get_data_of_workflow(self, workflow_id, status_exclusion_list=None, type_filter_list=None):
        return list(self.data_specs)

    def get_steps_of_workflow(self, workflow_id, status_filter_list=None):
        return list(self.step_specs)

    def update_workflow(self, workflow_spec):
        self.updated_workflows.append((workflow_spec.status, workflow_spec.parameters))
        return True

    def update_workflow_data(self, data_spec):
        self.updated_data.append((data_spec.name, data_spec.status))
        return True


def make_workflow(steps_final_time=None):
    workflow_spec = WorkflowSpec()
    workflow_spec.workflow_id = 133
    workflow_spec.status = WorkflowStatus.running
    if steps_final_time is not None:
        workflow_spec.set_parameter(workflow_core.STEPS_FINAL_TIME_PARAM, steps_final_time)
    return workflow_spec


class FakeDataHandler:
    """Reports whatever DDM state a case wants, so the waiting transitions can be driven"""

    def __init__(self, check_status):
        self.check_status = check_status

    def check_target(self, data_spec, **kwargs):
        result = types.SimpleNamespace(success=True, check_status=self.check_status, message="", metadata={})
        return result


class StubbedInterface(workflow_core.WorkflowInterface):
    """The real workflow transitions, with the step and data passes replaced

    Subclassed rather than monkey-patched so the stubs are checked against the methods they stand
    in for. __init__ is bypassed because the real one opens a message broker and a DDM client.
    """

    def __init__(self, data_specs, step_specs, all_steps_final=True, check_status=None):
        self.tbif = FakeTaskBuffer(data_specs, step_specs)
        self.full_pid = "test-0-0"
        self.plugin_map = {}
        self.mb_proxy = None
        self._all_steps_final = all_steps_final
        self._data_handler = FakeDataHandler(check_status if check_status is not None else WFDataTargetCheckStatus.suffice)

    def get_plugin(self, plugin_type, flavor):
        return self._data_handler

    # The data pass is what would advance an output on a later cycle; here it changes nothing, so
    # each case controls the output statuses directly.
    def process_datas(self, data_specs, by="dog"):
        return {"n_processed": len(data_specs), "processed": {}, "changed": {}}

    def process_steps(self, step_specs, data_spec_map=None, by="dog"):
        status = WFStepStatus.done if self._all_steps_final else WFStepStatus.running
        return {"n_processed": len(step_specs), "processed": {status: len(step_specs)}, "changed": {}}


def make_interface(data_specs, step_specs, all_steps_final=True, check_status=None):
    return StubbedInterface(data_specs, step_specs, all_steps_final, check_status)


def check(label, condition, detail=""):
    print(f"  {'PASS' if condition else 'FAIL'}  {label}{'  ' + str(detail) if detail and not condition else ''}")
    return condition


def main():
    failures = 0
    interface = make_interface([], [])

    print("\n=== are_all_outputs_good is three-valued ===")
    failures += not check("no output at all -> None", interface.are_all_outputs_good({}) is None)
    good = {"a": FakeData("a", WFDataStatus.done_generated), "b": FakeData("b", WFDataStatus.done_waited)}
    failures += not check("every output terminal -> True", interface.are_all_outputs_good(good) is True)
    mixed = {"a": FakeData("a", WFDataStatus.done_generated), "b": FakeData("b", WFDataStatus.generating_suffice)}
    failures += not check("one output still generating -> False", interface.are_all_outputs_good(mixed) is False)

    print("\n=== record_steps_final_time measures the wait ===")
    workflow_spec = make_workflow()
    failures += not check("first cycle reports 0", interface.record_steps_final_time(workflow_spec, NOW) == 0)
    failures += not check(
        "first cycle records the time",
        workflow_spec.get_parameter(workflow_core.STEPS_FINAL_TIME_PARAM) == NOW.isoformat(),
        workflow_spec.parameters,
    )
    failures += not check(
        "a later cycle reports the elapsed seconds",
        interface.record_steps_final_time(workflow_spec, NOW + timedelta(seconds=125)) == 125,
    )
    broken = make_workflow(steps_final_time="not-a-timestamp")
    failures += not check("an unparsable value restarts the wait instead of hanging", interface.record_steps_final_time(broken, NOW) == 0)
    failures += not check("...and is replaced", broken.get_parameter(workflow_core.STEPS_FINAL_TIME_PARAM) == NOW.isoformat())

    print("\n=== a workflow whose steps are all final waits for its outputs ===")
    # This is workflow 133: every step done, the last output still generating_suffice because its
    # dataset is closed in DDM only after the task reached done.
    lagging = FakeData("merge_ntup/NTUP_PILEUP", WFDataStatus.generating_suffice)
    data_specs = [FakeData("deriv_phys/DAOD_PHYS", WFDataStatus.done_generated), lagging]
    interface = make_interface(data_specs, [FakeStep("merge_ntup"), FakeStep("deriv_phys")])
    workflow_spec = make_workflow()
    result = interface.process_workflow_running(workflow_spec)
    failures += not check("stays running", workflow_spec.status == WorkflowStatus.running, workflow_spec.status)
    failures += not check("reports success rather than an error", result.success is True, result.message)
    failures += not check("does not announce a new status", result.new_status is None, result.new_status)
    failures += not check("the lagging output is untouched", lagging.status == WFDataStatus.generating_suffice, lagging.status)
    failures += not check("the wait is recorded on the workflow", workflow_spec.get_parameter(workflow_core.STEPS_FINAL_TIME_PARAM) is not None)
    failures += not check("the recorded wait is persisted", interface.tbif.updated_workflows and interface.tbif.updated_workflows[-1][1])

    print("\n=== the output catching up finishes the workflow ===")
    data_specs = [FakeData("deriv_phys/DAOD_PHYS", WFDataStatus.done_generated), FakeData("merge_ntup/NTUP_PILEUP", WFDataStatus.done_generated)]
    interface = make_interface(data_specs, [FakeStep("merge_ntup"), FakeStep("deriv_phys")])
    workflow_spec = make_workflow(steps_final_time=NOW.isoformat())
    result = interface.process_workflow_running(workflow_spec)
    failures += not check("becomes done", workflow_spec.status == WorkflowStatus.done, workflow_spec.status)
    failures += not check("nothing had to be settled", not interface.tbif.updated_data, interface.tbif.updated_data)

    print("\n=== the wait is bounded ===")
    stuck = FakeData("merge_ntup/NTUP_PILEUP", WFDataStatus.generating_suffice)
    interface = make_interface([stuck], [FakeStep("merge_ntup")])
    expired = (NOW - timedelta(seconds=workflow_core.OUTPUT_SETTLE_GRACE_SEC + 1)).isoformat()
    workflow_spec = make_workflow(steps_final_time=expired)
    result = interface.process_workflow_running(workflow_spec)
    failures += not check("becomes done once the grace period is over", workflow_spec.status == WorkflowStatus.done, workflow_spec.status)
    failures += not check("the outstanding output is settled", stuck.status == WFDataStatus.done_generated, stuck.status)
    failures += not check("the settled output is persisted", ("merge_ntup/NTUP_PILEUP", WFDataStatus.done_generated) in interface.tbif.updated_data)

    print("\n=== settle_pending_outputs only settles what it can ===")
    log = Log()
    generating = FakeData("out/generating", WFDataStatus.generating_insuffi)
    waiting = FakeData("out/waiting", WFDataStatus.waiting_suffice)
    already = FakeData("out/already", WFDataStatus.done_waited)
    never_bound = FakeData("out/never_bound", WFDataStatus.checking)
    interface = make_interface([], [])
    interface.settle_pending_outputs(log, {d.name: d for d in [generating, waiting, already, never_bound]}, NOW)
    failures += not check("generating -> done_generated", generating.status == WFDataStatus.done_generated, generating.status)
    failures += not check("waiting -> done_waited", waiting.status == WFDataStatus.done_waited, waiting.status)
    failures += not check("an already terminal output is left alone", already.status == WFDataStatus.done_waited)
    failures += not check("one that was never bound is not given a final status", never_bound.status == WFDataStatus.checking, never_bound.status)
    failures += not check("every settled output is reported as a warning", sum(1 for level, _ in log.messages if level == "warning") == 3, log.messages)

    print("\n=== a workflow with no output data still finishes ===")
    interface = make_interface([], [FakeStep("only_step")])
    workflow_spec = make_workflow()
    result = interface.process_workflow_running(workflow_spec)
    failures += not check("becomes done rather than waiting for outputs it does not have", workflow_spec.status == WorkflowStatus.done, workflow_spec.status)

    print("\n=== steps that are not all final are unaffected ===")
    interface = make_interface([FakeData("out/a", WFDataStatus.generating_suffice)], [FakeStep("a", WFStepStatus.running)], all_steps_final=False)
    workflow_spec = make_workflow()
    result = interface.process_workflow_running(workflow_spec)
    failures += not check("stays running", workflow_spec.status == WorkflowStatus.running, workflow_spec.status)
    failures += not check("no wait is recorded", workflow_spec.get_parameter(workflow_core.STEPS_FINAL_TIME_PARAM) is None)

    print("\n=== a root input already parked in waiting_suffice ===")
    # Workflow 133's rdo_bkg: it exists and has files, but DDM never closes it. A datum parked
    # there before the rule existed is let out here; a new one never gets parked at all.
    root_input = FakeData("rdo_bkg", WFDataStatus.waiting_suffice, WFDataType.input)
    interface = make_interface([root_input], [])
    result = interface.process_data_waiting(root_input)
    failures += not check("it becomes done_waited", root_input.status == WFDataStatus.done_waited, root_input.status)
    failures += not check("the transition is reported", result.new_status == WFDataStatus.done_waited, result.new_status)
    failures += not check("an end time is stamped", root_input.end_time is not None)

    print("\n=== but data produced inside the workflow still waits ===")
    for data_type in (WFDataType.mid, WFDataType.output):
        produced = FakeData(f"step/{data_type}", WFDataStatus.waiting_suffice, data_type)
        make_interface([produced], []).process_data_waiting(produced)
        failures += not check(f"{data_type} stays waiting_suffice", produced.status == WFDataStatus.waiting_suffice, produced.status)

    print("\n=== a root input that is not sufficient yet still waits ===")
    not_enough = FakeData("rdo_bkg", WFDataStatus.waiting_insuffi, WFDataType.input)
    make_interface([not_enough], [], check_status=WFDataTargetCheckStatus.insuffi).process_data_waiting(not_enough)
    failures += not check("it stays waiting_insuffi", not_enough.status == WFDataStatus.waiting_insuffi, not_enough.status)

    print("\n=== a closed collection is unchanged by this ===")
    closed = FakeData("rdo_bkg", WFDataStatus.waiting_suffice, WFDataType.input)
    make_interface([closed], [], check_status=WFDataTargetCheckStatus.complete).process_data_waiting(closed)
    failures += not check("complete still means done_waited", closed.status == WFDataStatus.done_waited, closed.status)

    print("\n=== and the step can now finish ===")
    # The point of the change: a done root input makes all_inputs_complete true, which is what
    # releases workflowHoldup so the task is allowed to finish.
    stats = make_interface([], [])._check_all_inputs_of_step(Log(), ["rdo_bkg"], {"rdo_bkg": root_input})
    failures += not check("all_inputs_complete is true once it is done", stats["all_inputs_complete"] is True, stats)
    still_waiting = FakeData("rdo_bkg", WFDataStatus.waiting_suffice, WFDataType.input)
    stats = make_interface([], [])._check_all_inputs_of_step(Log(), ["rdo_bkg"], {"rdo_bkg": still_waiting})
    failures += not check("...and was false while it waited", stats["all_inputs_complete"] is False, stats)
    failures += not check("...though it was already good enough to start on", stats["all_inputs_sufficient"] is True, stats)

    print("\n=== a root input never enters the waiting path in the first place ===")
    # The first check is where it is settled: an open but sufficient collection is called complete,
    # so the datum goes checked_complete -> done_skipped and is terminal straight away.
    fresh = FakeData("rdo_bkg", WFDataStatus.checking, WFDataType.input)
    interface = make_interface([fresh], [], check_status=WFDataTargetCheckStatus.suffice)
    interface.process_data_checking(fresh)
    failures += not check("checked_complete, not checked_suffice", fresh.status == WFDataStatus.checked_complete, fresh.status)
    interface.process_data_checked(fresh)
    failures += not check("and then done_skipped", fresh.status == WFDataStatus.done_skipped, fresh.status)

    print("\n=== data produced inside the workflow is unaffected at that check ===")
    for data_type in (WFDataType.mid, WFDataType.output):
        produced = FakeData(f"step/{data_type}", WFDataStatus.checking, data_type)
        make_interface([produced], [], check_status=WFDataTargetCheckStatus.suffice).process_data_checking(produced)
        failures += not check(f"{data_type} is still checked_suffice", produced.status == WFDataStatus.checked_suffice, produced.status)

    print("\n=== an insufficient root input is still not complete ===")
    thin = FakeData("rdo_bkg", WFDataStatus.checking, WFDataType.input)
    make_interface([thin], [], check_status=WFDataTargetCheckStatus.insuffi).process_data_checking(thin)
    failures += not check("checked_insuffi is untouched", thin.status == WFDataStatus.checked_insuffi, thin.status)

    print("\n=== a missing root input is still missing ===")
    gone = FakeData("rdo_bkg", WFDataStatus.checking, WFDataType.input)
    make_interface([gone], [], check_status=WFDataTargetCheckStatus.nonexist).process_data_checking(gone)
    failures += not check("checked_nonexist is untouched", gone.status == WFDataStatus.checked_nonexist, gone.status)

    print(f"\n{'ALL CHECKS PASSED' if not failures else f'{failures} CHECK(S) FAILED'}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
