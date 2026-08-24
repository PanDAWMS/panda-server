"""
Offline check of the inline workflow-description submission endpoint.

Stubs the server-side dependencies (task buffer, workflow interface, request validation) so that
pandaserver.api.v1.workflow_api.submit_workflow_description can be exercised without a database or
a running server.

Run from the repository root:  python3 pandaserver/workflow/examples/submit_endpoint_test.py
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

# ---- stub what a bare checkout does not have -----------------------------------------------
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
            def __init__(self, *args, **kwargs):
                pass

            def __call__(self, *args, **kwargs):
                return self

        module.__getattr__ = lambda name: Anything


sys.meta_path.insert(0, AutoStubFinder())


def stub(name, **attrs):
    module = types.ModuleType(name)
    for key, value in attrs.items():
        setattr(module, key, value)
    sys.modules[name] = module
    return module


class QuietLogWrapper:
    def __init__(self, *args, **kwargs):
        self.messages = []

    def info(self, message):
        pass

    def debug(self, message):
        pass

    def warning(self, message):
        self.messages.append(message)

    def error(self, message):
        self.messages.append(message)


import datetime

stub("pandacommon")
pandautils = stub("pandacommon.pandautils")
pandautils.__path__ = []
stub("pandacommon.pandautils.base", SpecBase=object)
stub(
    "pandacommon.pandautils.PandaUtils", naive_utcnow=lambda: datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None), get_sql_IN_bind_variables=None
)
pandalogger = stub("pandacommon.pandalogger")
pandalogger.__path__ = []
stub("pandacommon.pandalogger.LogWrapper", LogWrapper=QuietLogWrapper)
stub("pandacommon.pandalogger.PandaLogger", PandaLogger=lambda: types.SimpleNamespace(getLogger=lambda name: None))
stub("pandaserver.config", panda_config=types.SimpleNamespace(schemaJEDI="ATLAS_PANDA", schemaDEFT="ATLAS_DEFT"))

# the VOMS attributes reported to the endpoint; adjusted by the tests
FAKE_ROLE = {"production": True}
FAKE_FQANS = ["/atlas/Role=production", "/atlas/usatlas"]


def fake_request_validation(logger, secure=False, production=False, request_method=None):
    def decorator(func):
        return func

    return decorator


stub(
    "pandaserver.api.v1.common",
    MESSAGE_DATABASE="database error",
    TIME_OUT="timeout",
    TimedMethod=object,
    generate_response=lambda success, message="", data=None: {"success": success, "message": message, "data": data},
    get_dn=lambda req: "/DC=ch/DC=cern/CN=test user",
    get_fqan=lambda req: list(FAKE_FQANS),
    has_production_role=lambda req: FAKE_ROLE["production"],
    request_validation=fake_request_validation,
)
stub("pandaserver.srvcore")
stub("pandaserver.srvcore.panda_request", PandaRequest=object)
stub("pandaserver.taskbuffer")
stub("pandaserver.taskbuffer.TaskBuffer", TaskBuffer=object)
stub("pandaserver.workflow.workflow_core", WorkflowInterface=lambda *a, **k: None)

from pandaserver.api.v1 import workflow_api  # noqa: E402


# ---- fakes standing in for the task buffer and the workflow interface ----------------------
class FakeTaskBuffer:
    def __init__(self, existing=None, fail=False):
        self.existing = existing or {}
        self.fail = fail
        self.queries = []

    def get_existing_task_names(self, vo, prod_source_label, task_names):
        self.queries.append((vo, prod_source_label, sorted(task_names)))
        if self.fail:
            return None
        return {name: info for name, info in self.existing.items() if name in task_names}


class FakeWorkflowInterface:
    def __init__(self, workflow_id=4242):
        self.workflow_id = workflow_id
        self.calls = []

    def register_workflow(
        self, prodsourcelabel, user_dn, workflow_name=None, workflow_definition=None, raw_request_params=None, prod_role=False, fqans=None, *args, **kwargs
    ):
        self.calls.append(
            {
                "prodsourcelabel": prodsourcelabel,
                "user_dn": user_dn,
                "workflow_name": workflow_name,
                "raw_request_params": raw_request_params,
                "prod_role": prod_role,
                "fqans": fqans,
            }
        )
        return self.workflow_id


def install(existing=None, fail=False, workflow_id=4242):
    tbif = FakeTaskBuffer(existing, fail)
    wfif = FakeWorkflowInterface(workflow_id)
    workflow_api.global_task_buffer = tbif
    workflow_api.global_wfif = wfif
    return tbif, wfif


def check(label, condition, detail=""):
    print(f"  {'PASS' if condition else 'FAIL'}  {label}{'  ' + str(detail) if detail and not condition else ''}")
    return condition


def main():
    failures = 0
    wfd = json.load(open(os.path.join(os.path.dirname(__file__), "production_chain_wfd.json")))
    submit = workflow_api.submit_workflow_description

    print("\n=== a valid description registers ===")
    tbif, wfif = install()
    res = submit(None, copy.deepcopy(wfd))
    failures += not check("success", res["success"] is True, res["message"])
    failures += not check("workflow_id returned", res["data"] == {"workflow_id": 4242}, res["data"])
    failures += not check("no warning message", res["message"] == "", res["message"])
    failures += not check("registered exactly once", len(wfif.calls) == 1)
    call = wfif.calls[0]
    failures += not check("workflow_name taken from the description", call["workflow_name"] == wfd["name"], call["workflow_name"])
    failures += not check("prodsourcelabel managed for a production role", call["prodsourcelabel"] == "managed", call["prodsourcelabel"])
    failures += not check(
        "description carried under the inline key",
        list(call["raw_request_params"]) == [workflow_api.INLINE_DESCRIPTION_KEY],
        list(call["raw_request_params"]),
    )
    failures += not check("no sandbox keys invented", "sandbox" not in call["raw_request_params"] and "sourceURL" not in call["raw_request_params"])
    failures += not check("production role captured from VOMS", call["prod_role"] is True, call["prod_role"])
    failures += not check("fqans captured from VOMS", call["fqans"] == FAKE_FQANS, call["fqans"])

    print("\n=== credentials come from VOMS, never from the payload ===")
    spoofed = copy.deepcopy(wfd)
    spoofed["prod_role"] = True
    spoofed["fqans"] = ["/atlas/Role=production"]
    FAKE_ROLE["production"] = False
    FAKE_FQANS_SAVED = list(FAKE_FQANS)
    del FAKE_FQANS[:]
    FAKE_FQANS.extend(["/atlas"])
    tbif_s, wfif_s = install()
    submit(None, spoofed)
    failures += not check("payload cannot claim a production role", wfif_s.calls[0]["prod_role"] is False, wfif_s.calls[0]["prod_role"])
    failures += not check("payload cannot inject fqans", wfif_s.calls[0]["fqans"] == ["/atlas"], wfif_s.calls[0]["fqans"])
    failures += not check("payload cannot force prodsourcelabel", wfif_s.calls[0]["prodsourcelabel"] == "user")
    FAKE_ROLE["production"] = True
    del FAKE_FQANS[:]
    FAKE_FQANS.extend(FAKE_FQANS_SAVED)

    print("\n=== the description is accepted as a JSON string too ===")
    tbif, wfif = install()
    res = submit(None, json.dumps(wfd))
    failures += not check("success", res["success"] is True, res["message"])
    res_bad = submit(None, "{not json")
    failures += not check("malformed JSON rejected", res_bad["success"] is False)
    failures += not check("malformed JSON explained", "Failed to parse" in res_bad["message"], res_bad["message"])

    print("\n=== a non-production submitter gets prodsourcelabel user ===")
    FAKE_ROLE["production"] = False
    tbif, wfif = install()
    submit(None, copy.deepcopy(wfd))
    failures += not check("prodsourcelabel user", wfif.calls[0]["prodsourcelabel"] == "user", wfif.calls[0]["prodsourcelabel"])
    FAKE_ROLE["production"] = True

    print("\n=== an invalid description is rejected before registration ===")
    broken = copy.deepcopy(wfd)
    for job_param in broken["steps"]["simul"]["task_params"]["jobParameters"]:
        if job_param.get("param_type") == "input":
            job_param["dataset"] = "{merge_evnt/NOPE}"
    tbif, wfif = install()
    res = submit(None, broken)
    failures += not check("rejected", res["success"] is False)
    failures += not check("reason reported", "does not match any step output" in res["message"], res["message"])
    failures += not check("nothing registered", wfif.calls == [])
    failures += not check("no duplication query wasted", tbif.queries == [])

    print("\n=== a duplicated taskName warns but still registers ===")
    existing = {
        wfd["steps"]["evgen"]["task_params"]["taskName"]: {"jediTaskID": 48810693, "status": "running"},
        wfd["steps"]["merge_aod"]["task_params"]["taskName"]: {"jediTaskID": 48810733, "status": None},
    }
    tbif, wfif = install(existing=existing)
    res = submit(None, copy.deepcopy(wfd))
    failures += not check("still succeeds", res["success"] is True)
    failures += not check("still registers", len(wfif.calls) == 1)
    failures += not check("warning names both collisions", res["message"].count("jediTaskID=") == 2, res["message"])
    failures += not check("JEDI status reported", "status=running" in res["message"], res["message"])
    failures += not check("DEFT-only collision reported", "queued in DEFT" in res["message"], res["message"])
    failures += not check("queried once per vo/prodSourceLabel group", len(tbif.queries) == 1, tbif.queries)
    failures += not check("queried all 9 taskNames", len(tbif.queries[0][2]) == 9, tbif.queries[0][2])
    failures += not check("queried group scoped by vo and label", tbif.queries[0][:2] == ("atlas", "managed"), tbif.queries[0][:2])

    print("\n=== a failed duplication lookup does not block submission ===")
    tbif, wfif = install(fail=True)
    res = submit(None, copy.deepcopy(wfd))
    failures += not check("still succeeds", res["success"] is True, res["message"])
    failures += not check("still registers", len(wfif.calls) == 1)

    print("\n=== registration failure is reported ===")
    tbif, wfif = install(workflow_id=None)
    res = submit(None, copy.deepcopy(wfd))
    failures += not check("failure reported", res["success"] is False)
    failures += not check("reason given", "Failed to submit" in res["message"], res["message"])

    print("\n=== the deprecated alias delegates to submit_workflow ===")
    raw_params = {"sourceURL": "https://example.org", "sandbox": "sandbox.tgz", "language": "yaml", "workflowSpecFile": "wf.yaml", "outDS": "user.me.out"}
    tbif, wfif = install()
    res_alias = workflow_api.submit_workflow_raw_request(None, copy.deepcopy(raw_params))
    tbif2, wfif2 = install()
    res_direct = workflow_api.submit_workflow(None, copy.deepcopy(raw_params))
    failures += not check("alias succeeds", res_alias["success"] is True, res_alias["message"])
    failures += not check("alias result matches submit_workflow", res_alias == res_direct, (res_alias, res_direct))
    failures += not check("alias registers the raw request unchanged", wfif.calls[0]["raw_request_params"]["sandbox"] == "sandbox.tgz")
    failures += not check("raw request path does not validate a description", "workflow_description" not in wfif.calls[0]["raw_request_params"])

    print("\n=== routing ===")
    # extract_allowed_methods lives in the stubbed common module, so apply its rule directly:
    # module-level functions defined in this module whose name does not start with an underscore
    import inspect

    exported = [
        name
        for name, obj in inspect.getmembers(workflow_api, inspect.isfunction)
        if obj.__module__ == workflow_api.__name__ and name != "init_task_buffer" and not name.startswith("_")
    ]
    print(f"    exported: {sorted(exported)}")
    failures_local = 0
    failures_local += not check("the new endpoint is routed", "submit_workflow_description" in exported)
    failures_local += not check(
        "all four endpoints routed",
        {"submit_workflow", "submit_workflow_description", "submit_workflow_definition", "submit_workflow_raw_request"} <= set(exported),
        sorted(exported),
    )
    failures_local += not check("the deprecated alias is still routed", "submit_workflow_raw_request" in exported)
    failures_local += not check("private helpers not routed", not any(n.startswith("_") for n in exported))
    failures_local += not check(
        "imported helpers not routed",
        "validate_workflow_description" not in exported and "substitute_placeholder" not in exported,
    )

    failures += failures_local
    print(f"\n{'ALL CHECKS PASSED' if not failures else f'{failures} CHECK(S) FAILED'}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
