"""
Offline check of PandaTaskDataHandler.check_target.

The status this returns drives the workflow data state machine, so a wrong answer here stalls the
step that consumes or produces the data, and with it the whole workflow. In particular a dataset
name still holding ${TASKID} must be reported as non-existent rather than as "no change": reporting
no change leaves the data in checking forever, which leaves the step in checking and the workflow in
starting.

Run from the repository root:  python3 pandaserver/workflow/examples/data_handler_test.py
"""

import os
import sys
import types

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
sys.path.insert(0, REPO_ROOT)

WARNINGS = []


def stub(name, **attrs):
    module = types.ModuleType(name)
    for key, value in attrs.items():
        setattr(module, key, value)
    sys.modules[name] = module
    return module


class Log:
    def __init__(self, *a, **k):
        pass

    def info(self, m):
        pass

    def debug(self, m):
        pass

    def warning(self, m):
        WARNINGS.append(m)

    def error(self, m):
        pass


stub("pandacommon")
stub("pandacommon.pandautils").__path__ = []
stub("pandacommon.pandautils.base", SpecBase=object)
stub("pandacommon.pandalogger").__path__ = []
stub("pandacommon.pandalogger.LogWrapper", LogWrapper=Log)
stub("pandacommon.pandalogger.PandaLogger", PandaLogger=lambda: types.SimpleNamespace(getLogger=lambda n: None))
stub("pandaserver.config", panda_config=types.SimpleNamespace(schemaJEDI="ATLAS_PANDA", schemaDEFT="ATLAS_DEFT"))

from pandaserver.workflow.data_handler_plugins.panda_task_data_handler import (  # noqa: E402
    PandaTaskDataHandler,
)
from pandaserver.workflow.workflow_base import (  # noqa: E402
    TASKID_PLACEHOLDER,
    WFDataStatus,
    WFDataTargetCheckStatus,
    WFStepStatus,
)


class FakeData:
    def __init__(self, target_id, output_types, source_step_id=None):
        self.target_id = target_id
        self.output_types = output_types
        self.source_step_id = source_step_id
        self.flavor = "panda_task"
        self.workflow_id = 10
        self.data_id = 2

    def get_parameter(self, key):
        return self.output_types if key == "output_types" else None


class FakeStep:
    def __init__(self, status):
        self.status = status
        self.step_id = 27


class FakeDDM:
    def __init__(self, metadata):
        self.metadata = metadata
        self.queried = []

    def get_dataset_metadata(self, name, **kwargs):
        self.queried.append(name)
        return self.metadata


class FakeTaskBuffer:
    def __init__(self, step=None):
        self.step = step

    def get_workflow(self, workflow_id):
        return None

    def get_workflow_step(self, step_id):
        return self.step


CLOSED = {"state": "closed", "content_state": "closed", "length": 42}
MISSING = {"state": "missing"}
UNRESOLVED = f"mc23_13p6TeV.526140.x.evgen.EVNT.e8590_wfid10_tid{TASKID_PLACEHOLDER}_00"
RESOLVED = "mc23_13p6TeV.526140.x.evgen.EVNT.e8590_wfid10_tid49900001_00"


def check(label, condition, detail=""):
    print(f"  {'PASS' if condition else 'FAIL'}  {label}{'  ' + str(detail) if detail and not condition else ''}")
    return condition


def main():
    failures = 0

    print("\n=== a name still holding ${TASKID} ===")
    ddm = FakeDDM(CLOSED)
    handler = PandaTaskDataHandler(FakeTaskBuffer(), ddm)
    result = handler.check_target(FakeData(UNRESOLVED, []))
    failures += not check(
        "reported as non-existent", result.success is True and result.check_status == WFDataTargetCheckStatus.nonexist, (result.success, result.check_status)
    )
    # nonexist is the only check status that advances an output to binding, which is what lets the
    # step generating it leave checking. Anything else stalls the workflow.
    failures += not check("nonexist is a checked status the state machine acts on", WFDataStatus.checked_nonexist in WFDataStatus.checked_statuses)
    failures += not check("no DDM query made with an unresolved name", ddm.queried == [], ddm.queried)

    print("\n=== an unresolved name on a finished step is surfaced ===")
    del WARNINGS[:]
    handler = PandaTaskDataHandler(FakeTaskBuffer(step=FakeStep(WFStepStatus.done)), FakeDDM(CLOSED))
    result = handler.check_target(FakeData(UNRESOLVED, [], source_step_id=27))
    failures += not check("still reported as non-existent", result.check_status == WFDataTargetCheckStatus.nonexist)
    failures += not check("a warning names the situation", any("still unresolved" in m for m in WARNINGS), WARNINGS)
    del WARNINGS[:]
    handler = PandaTaskDataHandler(FakeTaskBuffer(step=FakeStep(WFStepStatus.running)), FakeDDM(CLOSED))
    handler.check_target(FakeData(UNRESOLVED, [], source_step_id=27))
    failures += not check("no warning while the step is still running", not any("still unresolved" in m for m in WARNINGS), WARNINGS)

    print("\n=== a resolved name with no output types (production shape) ===")
    ddm = FakeDDM(CLOSED)
    handler = PandaTaskDataHandler(FakeTaskBuffer(), ddm)
    result = handler.check_target(FakeData(RESOLVED, []))
    failures += not check("closed and non-empty -> complete", result.check_status == WFDataTargetCheckStatus.complete, result.check_status)
    failures += not check("queried the bare target_id", ddm.queried == [RESOLVED], ddm.queried)
    ddm = FakeDDM(MISSING)
    handler = PandaTaskDataHandler(FakeTaskBuffer(), ddm)
    failures += not check("missing -> nonexist", handler.check_target(FakeData(RESOLVED, [])).check_status == WFDataTargetCheckStatus.nonexist)

    print("\n=== an analysis output with output types is unaffected ===")
    ddm = FakeDDM(CLOSED)
    handler = PandaTaskDataHandler(FakeTaskBuffer(), ddm)
    result = handler.check_target(FakeData("user.me.myOut_002_b", ["aaa.root", "bbb.root"]))
    failures += not check(
        "expanded per output type",
        ddm.queried == ["user.me.myOut_002_b_aaa.root", "user.me.myOut_002_b_bbb.root"],
        ddm.queried,
    )
    failures += not check("complete", result.check_status == WFDataTargetCheckStatus.complete)

    print("\n=== a done source step short-circuits to complete ===")
    handler = PandaTaskDataHandler(FakeTaskBuffer(step=FakeStep(WFStepStatus.done)), FakeDDM(MISSING))
    result = handler.check_target(FakeData(RESOLVED, [], source_step_id=27))
    failures += not check("complete regardless of DDM", result.check_status == WFDataTargetCheckStatus.complete)

    print(f"\n{'ALL CHECKS PASSED' if not failures else f'{failures} CHECK(S) FAILED'}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
