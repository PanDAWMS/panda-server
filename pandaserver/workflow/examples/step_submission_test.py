"""
Offline check of workflow step task submission.

Covers the pieces that turn a parsed step into a queued task: resolving input dataset references
against the datasets actually produced upstream, refusing a production label without the role,
resolving the late-bound task ID into the step's output dataset names, and the task status mapping.

Run from the repository root:  python3 pandaserver/workflow/examples/step_submission_test.py
"""

import copy
import importlib.abc
import importlib.machinery
import json
import os
import sys
import types

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
sys.path.insert(0, REPO_ROOT)

AUTO_STUB_ROOTS = ("idds", "pandaclient", "ruamel", "requests", "snakemake")


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
    def __init__(self, *a, **k):
        self.errors = []

    def info(self, m):
        pass

    def debug(self, m):
        pass

    def warning(self, m):
        pass

    def error(self, m):
        self.errors.append(m)


stub("pandacommon")
pandautils = stub("pandacommon.pandautils")
pandautils.__path__ = []
stub("pandacommon.pandautils.base", SpecBase=object)
pandalogger = stub("pandacommon.pandalogger")
pandalogger.__path__ = []
stub("pandacommon.pandalogger.LogWrapper", LogWrapper=Log)
stub("pandacommon.pandalogger.PandaLogger", PandaLogger=lambda: types.SimpleNamespace(getLogger=lambda n: None))
stub("pandaserver.config", panda_config=types.SimpleNamespace(schemaJEDI="ATLAS_PANDA", schemaDEFT="ATLAS_DEFT"))

from pandaserver.workflow.step_handler_plugins.panda_task_step_handler import (  # noqa: E402
    PandaTaskStepHandler,
)
from pandaserver.workflow.workflow_base import (  # noqa: E402
    TASKID_PLACEHOLDER,
    WFStepStatus,
)


class FakeData:
    def __init__(self, name, target_id):
        self.name = name
        self.target_id = target_id
        self.workflow_id = 1
        self.data_id = abs(hash(name)) % 1000


class FakeStep:
    def __init__(self, definition, parameters=None):
        self.workflow_id = 1
        self.step_id = 7
        self.flavor = "panda_task"
        self.target_id = None
        self.status = WFStepStatus.ready
        self._definition = definition
        self._parameters = parameters or {}

    @property
    def definition_json_map(self):
        return self._definition

    def get_parameter(self, key):
        return self._parameters.get(key)

    def set_parameter(self, key, value):
        self._parameters[key] = value


class FakeTaskBuffer:
    def __init__(self, data_by_name=None, task_id=49900001, error=""):
        self.data = data_by_name or {}
        self.task_id = task_id
        self.error = error
        self.inserted = []
        self.updated_data = []

    def get_workflow_data_by_name(self, name, workflow_id):
        return self.data.get(name)

    def update_workflow_data(self, data_spec):
        self.updated_data.append((data_spec.name, data_spec.target_id))

    def update_workflow_step(self, step_spec):
        pass

    def insert_step_task(self, task_params_map, user_dn, parent_tid=None):
        self.inserted.append(copy.deepcopy(task_params_map))
        if self.task_id is None:
            return None, self.error
        return self.task_id, ""

    def getTaskStatusSuperstatus(self, task_id):
        return self._status

    def set_status(self, status, superstatus=None):
        self._status = (status, superstatus or status)


def check(label, condition, detail=""):
    print(f"  {'PASS' if condition else 'FAIL'}  {label}{'  ' + str(detail) if detail and not condition else ''}")
    return condition


def main():
    failures = 0
    wfd = json.load(open(os.path.join(os.path.dirname(__file__), "production_chain_wfd.json")))
    # the simul step consumes {merge_evnt/EVNT} and produces one HITS dataset
    simul_params = copy.deepcopy(wfd["steps"]["simul"]["task_params"])
    # resolve ${WFID} the way registration would, so this works whether or not the description uses it
    simul_params = json.loads(json.dumps(simul_params).replace("${WFID}", "12345"))

    def make_step(prod_role=True, all_inputs_complete=True, params=None):
        return FakeStep(
            {
                "task_params": copy.deepcopy(params if params is not None else simul_params),
                "user_dn": "/DC=ch/CN=test",
                "prod_role": prod_role,
                "output_data_list": ["simul/HITS"],
            },
            {"all_inputs_complete": all_inputs_complete},
        )

    produced_evnt = "mc23_13p6TeV.526140.x.merge.EVNT.e8590_e8586_wfid12345_tid48810699_00"

    def make_tbif(**kw):
        data = {
            "merge_evnt/EVNT": FakeData("merge_evnt/EVNT", produced_evnt),
            "simul/HITS": FakeData("simul/HITS", f"mc23_13p6TeV.526140.x.simul.HITS.e8590_e8586_a934_wfid12345_tid{TASKID_PLACEHOLDER}_00"),
        }
        return FakeTaskBuffer(data_by_name=data, **kw)

    print("\n=== submit_target: the happy path ===")
    tbif = make_tbif()
    handler = PandaTaskStepHandler(tbif)
    step = make_step()
    res = handler.submit_target(step)
    failures += not check("submitted", res.success is True, res.message)
    failures += not check("target_id is the task id", res.target_id == "49900001", res.target_id)
    submitted = tbif.inserted[0]
    inputs = [p["dataset"] for p in submitted["jobParameters"] if p.get("param_type") == "input"]
    failures += not check("input reference resolved to the produced dataset", inputs == [produced_evnt], inputs)
    failures += not check("no brace reference left in any input dataset", not any("{" in d for d in inputs), inputs)
    # Output dataset names still carry ${TASKID} at this point on purpose: the ID does not exist
    # until the insert allocates it, so insert_step_task resolves it inside the same transaction.
    outputs = [p["dataset"] for p in submitted["jobParameters"] if p.get("param_type") == "output"]
    failures += not check("output datasets still carry the placeholder for the DB layer to resolve", all(TASKID_PLACEHOLDER in d for d in outputs), outputs)
    failures += not check("pseudo_input left untouched", any(p.get("dataset") == "seq_number" for p in submitted["jobParameters"]))
    failures += not check("workflowHoldup not set when inputs are complete", "workflowHoldup" not in submitted)
    failures += not check(
        "output dataset name resolved from the task id",
        tbif.updated_data == [("simul/HITS", f"mc23_13p6TeV.526140.x.simul.HITS.e8590_e8586_a934_wfid12345_tid49900001_00")],
        tbif.updated_data,
    )
    failures += not check("submission attempt recorded", step.get_parameter("submit_attempt_task_name") == simul_params["taskName"])

    print("\n=== workflowHoldup is set while inputs are incomplete ===")
    tbif = make_tbif()
    handler = PandaTaskStepHandler(tbif)
    handler.submit_target(make_step(all_inputs_complete=False))
    failures += not check("workflowHoldup set", tbif.inserted[0].get("workflowHoldup") is True)

    print("\n=== a production label without the role is refused ===")
    # every label JobUtils.prod_sources treats as production must be guarded, not just "managed"
    for label in ["managed", "prod_test"]:
        params = copy.deepcopy(simul_params)
        params["prodSourceLabel"] = label
        tbif = make_tbif()
        handler = PandaTaskStepHandler(tbif)
        res = handler.submit_target(make_step(prod_role=False, params=params))
        failures += not check(f"{label} refused without the role", res.success is not True)
        failures += not check(f"{label} reason mentions the production role", "production role" in res.message, res.message)
        failures += not check(f"{label} submitted nothing", tbif.inserted == [])
        # ... and is accepted once the submitter holds it
        tbif = make_tbif()
        handler = PandaTaskStepHandler(tbif)
        res = handler.submit_target(make_step(prod_role=True, params=params))
        failures += not check(f"{label} accepted with the role", res.success is True, res.message)
    # a non-production label needs no role
    params = copy.deepcopy(simul_params)
    params["prodSourceLabel"] = "user"
    tbif = make_tbif()
    handler = PandaTaskStepHandler(tbif)
    res = handler.submit_target(make_step(prod_role=False, params=params))
    failures += not check("a non-production label needs no role", res.success is True, res.message)

    print("\n=== an unresolved upstream output blocks submission ===")
    tbif = make_tbif()
    tbif.data["merge_evnt/EVNT"].target_id = f"mc23...merge.EVNT.e8590_wfid12345_tid{TASKID_PLACEHOLDER}_00"
    handler = PandaTaskStepHandler(tbif)
    res = handler.submit_target(make_step())
    failures += not check("refused", res.success is not True)
    failures += not check("reason mentions it is not resolved yet", "not resolved yet" in res.message, res.message)
    failures += not check("nothing submitted", tbif.inserted == [])

    print("\n=== a missing upstream output blocks submission ===")
    tbif = make_tbif()
    del tbif.data["merge_evnt/EVNT"]
    handler = PandaTaskStepHandler(tbif)
    res = handler.submit_target(make_step())
    failures += not check("refused", res.success is not True)
    failures += not check("nothing submitted", tbif.inserted == [])

    print("\n=== a second attempt for the same taskName is refused ===")
    tbif = make_tbif()
    handler = PandaTaskStepHandler(tbif)
    step = make_step()
    handler.submit_target(step)
    res2 = handler.submit_target(step)
    failures += not check("second attempt refused", res2.success is not True)
    failures += not check("reason mentions the previous attempt", "previous attempt" in res2.message, res2.message)
    failures += not check("submitted only once", len(tbif.inserted) == 1, len(tbif.inserted))

    print("\n=== a literal (external) dataset is passed through ===")
    recon_params = json.loads(json.dumps(copy.deepcopy(wfd["steps"]["recon"]["task_params"])).replace("${WFID}", "12345"))
    tbif = make_tbif()
    tbif.data["merge_hits/HITS"] = FakeData("merge_hits/HITS", "mc23...merge.HITS..._tid48810713_00")
    tbif.data["rdo_bkg"] = FakeData("rdo_bkg", wfd["inputs"]["rdo_bkg"])
    handler = PandaTaskStepHandler(tbif)
    step = FakeStep({"task_params": recon_params, "user_dn": "/DC=ch/CN=t", "prod_role": True, "output_data_list": []}, {"all_inputs_complete": True})
    res = handler.submit_target(step)
    failures += not check("submitted", res.success is True, res.message)
    datasets = [p["dataset"] for p in tbif.inserted[0]["jobParameters"] if p.get("param_type") == "input"]
    failures += not check("both inputs resolved", set(datasets) == {"mc23...merge.HITS..._tid48810713_00", wfd["inputs"]["rdo_bkg"]}, datasets)

    print("\n=== check_target status mapping ===")
    expectations = {
        WFStepStatus.running: ["running", "scouting", "scouted", "throttled", "prepared", "finishing", "passed", "merging", "toretry", "toincexec", "paused"],
        WFStepStatus.starting: [
            "registered",
            "defined",
            "assigned",
            "activated",
            "starting",
            "ready",
            "topreprocess",
            "preprocessing",
            "staging",
            "staged",
            "rerefine",
        ],
        WFStepStatus.done: ["done", "finished"],
        WFStepStatus.failed: ["failed", "exhausted", "aborted", "toabort", "aborting", "broken", "tobroken"],
    }
    for expected, statuses in expectations.items():
        for status in statuses:
            tbif = make_tbif()
            tbif.set_status(status)
            handler = PandaTaskStepHandler(tbif)
            step = make_step()
            step.status = WFStepStatus.running
            step.target_id = "49900001"
            result = handler.check_target(step)
            if not (result.success and result.step_status == expected):
                failures += not check(f"{status} -> {expected}", False, f"got success={result.success} status={result.step_status}")
    failures += not check(f"all {sum(len(v) for v in expectations.values())} task statuses map without error", True)
    # an unknown status must still be reported rather than silently mapped
    tbif = make_tbif()
    tbif.set_status("nonsense_status")
    handler = PandaTaskStepHandler(tbif)
    step = make_step()
    step.status = WFStepStatus.running
    step.target_id = "49900001"
    result = handler.check_target(step)
    failures += not check("an unknown status is still an error", result.success is False and "unknown" in result.message, result.message)

    print(f"\n{'ALL CHECKS PASSED' if not failures else f'{failures} CHECK(S) FAILED'}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
