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

Run from the repository root:  python3 pandaserver/workflow/tests/workflow_done_transition_test.py
"""

import copy
import importlib.abc
import importlib.machinery
import json
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
    _next_step_id = 500

    def __init__(self, name, status=WFStepStatus.done, flavor="panda_task", target_id="1", step_id=None):
        self.name = name
        self.status = status
        self.flavor = flavor
        self.target_id = target_id
        self.member_id = 1
        self.workflow_id = 133
        self.end_time = None
        if step_id is None:
            FakeStep._next_step_id += 1
            step_id = FakeStep._next_step_id
        self.step_id = step_id
        self.parameters = None

    # The engine records why a step reached a terminal status through these, and stores them as
    # JSON, so the fake serializes too rather than keeping a dict a non-serializable value would
    # pass through unnoticed.
    def get_parameter(self, name):
        return json.loads(self.parameters).get(name) if self.parameters else None

    def set_parameter(self, name, value):
        parameter_map = json.loads(self.parameters) if self.parameters else {}
        parameter_map[name] = value
        self.parameters = json.dumps(parameter_map)


class FakeWorkflowRow:
    """A child workflow as the task buffer would hand it back"""

    def __init__(self, workflow_id, status=WorkflowStatus.running):
        self.workflow_id = workflow_id
        self.status = status


class FakeStepHandler:
    """Records the targets it was asked to kill, so the kills themselves can be asserted on"""

    def __init__(self, succeed=True):
        self.succeed = succeed
        self.cancelled_target_ids = []

    def cancel_target(self, step_spec, **kwargs):
        if step_spec.target_id is None:
            # as the real handler does: no task was ever submitted, so there is nothing to kill and
            # the step is treated as already cancelled
            return types.SimpleNamespace(success=True, message="target_id is None so considered already cancelled; skipped")
        self.cancelled_target_ids.append(step_spec.target_id)
        message = "" if self.succeed else "failed to cancel the task: error_code=1"
        return types.SimpleNamespace(success=self.succeed, message=message)


class FakeTaskBuffer:
    def __init__(self, data_specs, step_specs):
        self.data_specs = data_specs
        self.step_specs = step_specs
        self.updated_workflows = []
        self.updated_data = []
        # one ordered record of everything written, so the order of the cleanup against the status
        # write can be asserted on rather than assumed
        self.call_log = []
        self.child_workflows = {}
        self.unlockable_step_ids = set()
        self.n_get_steps_calls = 0
        self.raise_on_get_steps_call = None

    def get_data_of_workflow(self, workflow_id, status_exclusion_list=None, type_filter_list=None):
        return list(self.data_specs)

    def get_steps_of_workflow(self, workflow_id, status_filter_list=None):
        self.n_get_steps_calls += 1
        if self.raise_on_get_steps_call == self.n_get_steps_calls:
            raise RuntimeError("database is unavailable")
        # Copies, because a real query returns rows read at the time it ran. Handing back the live
        # objects would hide the staleness this cleanup has to survive: the caller's list is read
        # before the steps are processed, and process_steps never writes its locked copies back.
        return [copy.copy(step_spec) for step_spec in self.step_specs]

    def get_workflow_step(self, step_id):
        for step_spec in self.step_specs:
            if step_spec.step_id == step_id:
                return step_spec
        return None

    def lock_workflow_step(self, step_id, locked_by, lock_expiration_sec=120):
        return step_id not in self.unlockable_step_ids

    def unlock_workflow_step(self, step_id, locked_by):
        return True

    def update_workflow_step(self, step_spec):
        self.call_log.append(("update_workflow_step", step_spec.step_id, step_spec.status))
        return True

    def get_workflow(self, workflow_id):
        return self.child_workflows.get(workflow_id)

    def update_workflow(self, workflow_spec):
        self.call_log.append(("update_workflow", workflow_spec.status))
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

    def __init__(self, data_specs, step_specs, all_steps_final=True, check_status=None, processed_stats=None, step_mutations=(), kills_succeed=True):
        self.tbif = FakeTaskBuffer(data_specs, step_specs)
        self.full_pid = "test-0-0"
        self.plugin_map = {}
        self.mb_proxy = None
        self._all_steps_final = all_steps_final
        self._data_handler = FakeDataHandler(check_status if check_status is not None else WFDataTargetCheckStatus.suffice)
        self._processed_stats = processed_stats
        self._step_mutations = step_mutations
        self.step_handler = FakeStepHandler(kills_succeed)
        self.cancelled_workflow_ids = []
        self.cancel_reasons = []

    def get_plugin(self, plugin_type, flavor):
        if plugin_type == "step_handler":
            return self.step_handler
        return self._data_handler

    # Recorded rather than run: what this harness checks is that the failing workflow asks for the
    # right children to be cancelled, and cancel_workflow taking the child final is what the
    # de-duplication between cancel_step's recursion and the orphan sweep relies on.
    def cancel_workflow(self, workflow_id, force=False, reason=None):
        self.cancelled_workflow_ids.append(workflow_id)
        self.cancel_reasons.append(reason)
        child_workflow = self.tbif.child_workflows.get(workflow_id)
        if child_workflow is not None:
            child_workflow.status = WorkflowStatus.cancelled
        return True

    # The data pass is what would advance an output on a later cycle; here it changes nothing, so
    # each case controls the output statuses directly.
    def process_datas(self, data_specs, by="dog"):
        return {"n_processed": len(data_specs), "processed": {}, "changed": {}}

    def process_steps(self, step_specs, data_spec_map=None, by="dog"):
        # The real one advances the stored steps while leaving the caller's list as it was, so the
        # mutations go to the stored objects and not to the list that was passed in.
        for step_id, new_status, new_target_id in self._step_mutations:
            stored_step = self.tbif.get_workflow_step(step_id)
            stored_step.status = new_status
            if new_target_id is not None:
                stored_step.target_id = new_target_id
        if self._processed_stats is not None:
            return {"n_processed": len(step_specs), "processed": dict(self._processed_stats), "changed": {}}
        status = WFStepStatus.done if self._all_steps_final else WFStepStatus.running
        return {"n_processed": len(step_specs), "processed": {status: len(step_specs)}, "changed": {}}


def make_interface(data_specs, step_specs, all_steps_final=True, check_status=None, **kwargs):
    return StubbedInterface(data_specs, step_specs, all_steps_final, check_status, **kwargs)


def status_of(interface, step_id):
    return interface.tbif.get_workflow_step(step_id).status


def index_of_first(call_log, name):
    for index, entry in enumerate(call_log):
        if entry[0] == name:
            return index
    return -1


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
    failures += not check(
        "nothing is killed while the workflow is healthy", interface.step_handler.cancelled_target_ids == [], interface.step_handler.cancelled_target_ids
    )

    print("\n=== a failed workflow cancels its unfinished steps and kills their tasks ===")
    # The failure a consumer actually meets: one task of a chain fails, and everything else the
    # chain had running must be stopped rather than left on the grid with nothing to end it.
    broke = FakeStep("recon", WFStepStatus.running, target_id="48810706")
    sibling = FakeStep("deriv_phys", WFStepStatus.running, target_id="48810720")
    never_started = FakeStep("merge_ntup", WFStepStatus.pending, target_id=None)
    finished = FakeStep("evgen", WFStepStatus.done, target_id="48810693")
    skipped = FakeStep("optional", WFStepStatus.closed, target_id=None)
    interface = make_interface(
        [],
        [broke, sibling, never_started, finished, skipped],
        processed_stats={WFStepStatus.failed: 1, WFStepStatus.running: 1, WFStepStatus.pending: 1, WFStepStatus.done: 1, WFStepStatus.closed: 1},
        step_mutations=[(broke.step_id, WFStepStatus.failed, None)],
    )
    workflow_spec = make_workflow()
    result = interface.process_workflow_running(workflow_spec)
    failures += not check("the workflow fails", workflow_spec.status == WorkflowStatus.failed, workflow_spec.status)
    failures += not check(
        "the running sibling is cancelled", status_of(interface, sibling.step_id) == WFStepStatus.cancelled, status_of(interface, sibling.step_id)
    )
    failures += not check("...and its end time is stamped", sibling.end_time is not None)
    failures += not check(
        "...and the cancellation is persisted",
        ("update_workflow_step", sibling.step_id, WFStepStatus.cancelled) in interface.tbif.call_log,
        interface.tbif.call_log,
    )
    # The load-bearing one: without it this passes on a version that only rewrites statuses in the
    # database and leaves the tasks running, which is the defect being fixed.
    failures += not check(
        "the sibling's task is actually killed",
        interface.step_handler.cancelled_target_ids == ["48810720"],
        interface.step_handler.cancelled_target_ids,
    )
    failures += not check(
        "the step that failed keeps its status", status_of(interface, broke.step_id) == WFStepStatus.failed, status_of(interface, broke.step_id)
    )
    failures += not check("...and is not killed a second time", "48810706" not in interface.step_handler.cancelled_target_ids)
    failures += not check(
        "a step that never started is cancelled too",
        status_of(interface, never_started.step_id) == WFStepStatus.cancelled,
        status_of(interface, never_started.step_id),
    )
    failures += not check(
        "...without a kill being sent for it", len(interface.step_handler.cancelled_target_ids) == 1, interface.step_handler.cancelled_target_ids
    )
    failures += not check("a finished step is left done", status_of(interface, finished.step_id) == WFStepStatus.done, status_of(interface, finished.step_id))
    failures += not check("a closed step stays closed", status_of(interface, skipped.step_id) == WFStepStatus.closed, status_of(interface, skipped.step_id))
    failures += not check("neither is killed", "48810693" not in interface.step_handler.cancelled_target_ids)
    failures += not check("no step is reported unreachable", workflow_spec.get_parameter(workflow_core.UNCANCELLED_STEPS_PARAM) is None)

    print("\n=== the failure says which step caused it, and the cancellations say they were collateral ===")
    # Status alone cannot tell a step cancelled with a failing workflow from one cancelled on
    # purpose, and the workflow would otherwise say only that it failed.
    workflow_reason = workflow_spec.get_parameter(workflow_core.WORKFLOW_FAIL_REASON_PARAM)
    failures += not check("the workflow names the step that failed", workflow_reason and "recon" in workflow_reason, workflow_reason)
    failures += not check("...and its step_id", workflow_reason and f"step_id={broke.step_id}" in workflow_reason, workflow_reason)
    sibling_reason = interface.tbif.get_workflow_step(sibling.step_id).get_parameter(workflow_core.STEP_REASON_PARAM)
    failures += not check(
        "a cancelled sibling says it went with the workflow", sibling_reason and "cancelled with the workflow" in sibling_reason, sibling_reason
    )
    failures += not check("...and repeats which step failed", sibling_reason and "recon" in sibling_reason, sibling_reason)

    print("\n=== the cancelling happens before the workflow is marked failed ===")
    # An exception part way through must leave the workflow running so the next cycle retries;
    # marking it failed first would strand whatever had not been reached yet.
    first_status_write = index_of_first(interface.tbif.call_log, "update_workflow")
    last_step_write = max(index for index, entry in enumerate(interface.tbif.call_log) if entry[0] == "update_workflow_step")
    failures += not check("every step write precedes the workflow write", last_step_write < first_status_write, interface.tbif.call_log)

    print("\n=== a failed step records what its target reported ===")
    # check_target knows the JEDI status and the engine used to throw it away, leaving "failed" as
    # the only thing anyone could read afterwards.
    step = FakeStep("recon", WFStepStatus.running, target_id="48810706")
    interface = make_interface([], [step])
    check_result = types.SimpleNamespace(native_status="broken", message="", step_status=WFStepStatus.failed)
    step.status = WFStepStatus.failed
    interface.record_step_reason(step, check_result)
    reason = step.get_parameter(workflow_core.STEP_REASON_PARAM)
    failures += not check("it names the task and what it reported", reason == "task 48810706 reported broken", reason)
    sub = FakeStep("sig_bg_comb", WFStepStatus.failed, flavor="sub_workflow", target_id="900")
    interface.record_step_reason(sub, types.SimpleNamespace(native_status="failed", message="", step_status=WFStepStatus.failed))
    failures += not check(
        "a sub-workflow step names the child workflow",
        sub.get_parameter(workflow_core.STEP_REASON_PARAM) == "child workflow 900 reported failed",
        sub.get_parameter(workflow_core.STEP_REASON_PARAM),
    )
    done_step = FakeStep("evgen", WFStepStatus.done, target_id="1")
    interface.record_step_reason(done_step, types.SimpleNamespace(native_status="done", message="", step_status=WFStepStatus.done))
    failures += not check("a step that did not fail records nothing", done_step.get_parameter(workflow_core.STEP_REASON_PARAM) is None)

    print("\n=== a sub-workflow step that failed during this very cycle is not missed ===")
    # The staleness regression. The list read before the steps were processed says running with no
    # target; only a re-read sees the failure and the child workflow it left behind.
    sub_step = FakeStep("sig_bg_comb", WFStepStatus.running, flavor="sub_workflow", target_id=None)
    interface = make_interface(
        [],
        [sub_step],
        processed_stats={WFStepStatus.failed: 1},
        step_mutations=[(sub_step.step_id, WFStepStatus.failed, "900")],
    )
    interface.tbif.child_workflows[900] = FakeWorkflowRow(900, WorkflowStatus.running)
    workflow_spec = make_workflow()
    result = interface.process_workflow_running(workflow_spec)
    failures += not check("the orphaned child workflow is cancelled", interface.cancelled_workflow_ids == [900], interface.cancelled_workflow_ids)
    failures += not check("the workflow fails", workflow_spec.status == WorkflowStatus.failed, workflow_spec.status)

    print("\n=== an unfinished sub-workflow step cancels its child exactly once ===")
    live_sub = FakeStep("sig_bg_comb", WFStepStatus.running, flavor="sub_workflow", target_id="901")
    other = FakeStep("broke", WFStepStatus.failed, target_id="48810706")
    interface = make_interface([], [live_sub, other], processed_stats={WFStepStatus.failed: 1, WFStepStatus.running: 1})
    interface.tbif.child_workflows[901] = FakeWorkflowRow(901, WorkflowStatus.running)
    workflow_spec = make_workflow()
    result = interface.process_workflow_running(workflow_spec)
    failures += not check("the child is cancelled once, not twice", interface.cancelled_workflow_ids == [901], interface.cancelled_workflow_ids)
    failures += not check("the step is cancelled", status_of(interface, live_sub.step_id) == WFStepStatus.cancelled, status_of(interface, live_sub.step_id))
    failures += not check(
        "a sub-workflow step issues no task kill", interface.step_handler.cancelled_target_ids == [], interface.step_handler.cancelled_target_ids
    )

    print("\n=== a child workflow already final is left alone ===")
    done_sub = FakeStep("sig_bg_comb", WFStepStatus.done, flavor="sub_workflow", target_id="902")
    interface = make_interface(
        [], [done_sub, FakeStep("broke", WFStepStatus.failed, target_id="1")], processed_stats={WFStepStatus.failed: 1, WFStepStatus.done: 1}
    )
    interface.tbif.child_workflows[902] = FakeWorkflowRow(902, WorkflowStatus.done)
    interface.process_workflow_running(make_workflow())
    failures += not check("nothing is cancelled", interface.cancelled_workflow_ids == [], interface.cancelled_workflow_ids)

    print("\n=== a kill that fails still fails the workflow ===")
    stubborn = FakeStep("deriv_phys", WFStepStatus.running, target_id="48810720")
    interface = make_interface(
        [],
        [stubborn, FakeStep("broke", WFStepStatus.failed, target_id="48810706")],
        processed_stats={WFStepStatus.failed: 1, WFStepStatus.running: 1},
        kills_succeed=False,
    )
    workflow_spec = make_workflow()
    result = interface.process_workflow_running(workflow_spec)
    failures += not check("the kill was attempted", interface.step_handler.cancelled_target_ids == ["48810720"], interface.step_handler.cancelled_target_ids)
    failures += not check(
        "the step is force cancelled anyway", status_of(interface, stubborn.step_id) == WFStepStatus.cancelled, status_of(interface, stubborn.step_id)
    )
    failures += not check("the workflow still fails", workflow_spec.status == WorkflowStatus.failed, workflow_spec.status)
    failures += not check("the step is not reported unreachable", workflow_spec.get_parameter(workflow_core.UNCANCELLED_STEPS_PARAM) is None)
    stubborn_reason = interface.tbif.get_workflow_step(stubborn.step_id).get_parameter(workflow_core.STEP_REASON_PARAM)
    failures += not check("the row admits the target was not cancelled", stubborn_reason and "target was not cancelled" in stubborn_reason, stubborn_reason)

    print("\n=== a step that cannot be locked is named on the workflow ===")
    held = FakeStep("deriv_phys", WFStepStatus.running, target_id="48810720")
    interface = make_interface(
        [], [held, FakeStep("broke", WFStepStatus.failed, target_id="1")], processed_stats={WFStepStatus.failed: 1, WFStepStatus.running: 1}
    )
    interface.tbif.unlockable_step_ids.add(held.step_id)
    workflow_spec = make_workflow()
    result = interface.process_workflow_running(workflow_spec)
    failures += not check("the workflow fails regardless", workflow_spec.status == WorkflowStatus.failed, workflow_spec.status)
    failures += not check(
        "the step that could not be reached is recorded",
        workflow_spec.get_parameter(workflow_core.UNCANCELLED_STEPS_PARAM) == [held.step_id],
        workflow_spec.get_parameter(workflow_core.UNCANCELLED_STEPS_PARAM),
    )

    print("\n=== a later cycle over the same workflow issues no new kills ===")
    interface = make_interface(
        [],
        [FakeStep("broke", WFStepStatus.failed, target_id="48810706"), FakeStep("deriv_phys", WFStepStatus.cancelled, target_id="48810720")],
        processed_stats={WFStepStatus.failed: 1, WFStepStatus.cancelled: 1},
    )
    interface.process_workflow_running(make_workflow())
    failures += not check("no kill is sent", interface.step_handler.cancelled_target_ids == [], interface.step_handler.cancelled_target_ids)
    failures += not check(
        "no step is written",
        not [entry for entry in interface.tbif.call_log if entry[0] == "update_workflow_step"],
        interface.tbif.call_log,
    )

    print("\n=== an error during the cleanup leaves the workflow to the next cycle ===")
    interface = make_interface([], [FakeStep("broke", WFStepStatus.failed, target_id="1")], processed_stats={WFStepStatus.failed: 1})
    # the first query is the caller's own; the cleanup's re-read is the second
    interface.tbif.raise_on_get_steps_call = 2
    workflow_spec = make_workflow()
    result = interface.process_workflow_running(workflow_spec)
    failures += not check("the workflow stays running", workflow_spec.status == WorkflowStatus.running, workflow_spec.status)
    failures += not check("nothing is written", interface.tbif.updated_workflows == [], interface.tbif.updated_workflows)
    failures += not check("the error is reported", result.success is not True, result.success)

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
