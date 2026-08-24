"""
Offline check of raw-task-params ("task") workflow step parsing.

Stubs the runtime dependencies which are not installed in a bare checkout (pandaclient,
pandacommon, ruamel, pandaserver.config) so that parse_workflow_data / resolve_nodes /
resolve_params / verify can be exercised directly on the example description.

Run from the repository root:  python3 pandaserver/workflow/examples/parse_task_steps_test.py
"""

import json
import os
import sys
import types

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
sys.path.insert(0, REPO_ROOT)


# ---- stub the dependencies which are absent in a bare checkout ----------------------------
def _stub(name, **attrs):
    mod = types.ModuleType(name)
    for k, v in attrs.items():
        setattr(mod, k, v)
    sys.modules[name] = mod
    return mod


_stub("pandaclient", PhpoScript=types.SimpleNamespace(main=None), PrunScript=types.SimpleNamespace(main=None))
_stub("pandacommon")
_stub("pandacommon.pandautils")
_stub("pandacommon.pandautils.base", SpecBase=object)
_stub("pandaserver.config", panda_config=types.SimpleNamespace(schemaJEDI="ATLAS_PANDA", schemaDEFT="ATLAS_DEFT"))

from pandaserver.workflow import workflow_native_utils as wnu  # noqa: E402
from pandaserver.workflow.workflow_base import (  # noqa: E402
    TASKID_PLACEHOLDER,
    WFID_PLACEHOLDER,
    has_placeholder,
    substitute_placeholder,
)


class Log:
    def info(self, m):
        pass

    def debug(self, m):
        pass

    def warning(self, m):
        print(f"  WARNING: {m}")

    def error(self, m):
        print(f"  ERROR: {m}")


def check(label, cond, detail=""):
    print(f"  {'PASS' if cond else 'FAIL'}  {label}{'  ' + detail if detail and not cond else ''}")
    return cond


def main():
    log = Log()
    failures = 0
    wfd = json.load(open(os.path.join(os.path.dirname(__file__), "production_chain_wfd.json")))

    print("\n=== parse_workflow_data ===")
    nodes, root_in = wnu.parse_workflow_data(wfd, log)
    by_name = {n.name: n for n in nodes}
    failures += not check("all 9 steps parsed", len(nodes) == 9, f"got {len(nodes)}")
    failures += not check("all steps are leaves", all(n.is_leaf for n in nodes))
    failures += not check("all steps typed 'task'", all(n.type == "task" for n in nodes))
    failures += not check(
        "tails are merge_ntup + deriv_phys",
        {n.name for n in nodes if n.is_tail} == {"merge_ntup", "deriv_phys"},
        str({n.name for n in nodes if n.is_tail}),
    )

    print("\n--- derived outputs (one workflow datum per output job parameter)")
    for name in ["evgen", "merge_hits", "deriv_phys"]:
        print(f"    {name}: {sorted(by_name[name].outputs)}")
    failures += not check(
        "deriv_phys exposes both DAODs independently",
        set(by_name["deriv_phys"].outputs) == {"deriv_phys/DAOD_PHYS", "deriv_phys/DAOD_PHYSLITE"},
        str(set(by_name["deriv_phys"].outputs)),
    )
    failures += not check(
        "merge_hits honours the outputs override (HITS, not HITS_MRG)",
        set(by_name["merge_hits"].outputs) == {"merge_hits/HITS"},
        str(set(by_name["merge_hits"].outputs)),
    )
    failures += not check(
        "the 'number' job parameter is not mistaken for an output",
        all("DAOD" in k or k.endswith("PHYSLITE") for k in by_name["deriv_phys"].outputs),
    )
    failures += not check("no outDS alias is invented", not any(k.endswith("/outDS") for n in nodes for k in n.outputs))

    print("\n--- derived inputs and dependency edges")
    failures += not check("evgen has no inputs (noInput head step)", by_name["evgen"].inputs == {})
    recon_sources = {v["source"] for v in by_name["recon"].inputs.values()}
    print(f"    recon sources: {sorted(recon_sources)}")
    # The parent-resolution pass strips the braces from a workflow-input reference so that the
    # source matches the root_inputs key, while a "step/output" reference is left as it is and
    # becomes a parent edge. Both forms are expected here in their post-resolution shape.
    failures += not check(
        "recon takes the upstream HITS and the external pileup input",
        recon_sources == {"merge_hits/HITS", "rdo_bkg"},
        str(recon_sources),
    )
    failures += not check(
        "pseudo_input (seq_number) never becomes an input",
        not any("seq_number" in json.dumps(n.inputs) for n in nodes),
    )

    print("\n=== resolve_nodes ===")
    serial, tails, nodes = wnu.resolve_nodes(nodes, root_in, wfd.get("inputs", {}), 0, set(), None, log)
    by_name = {n.name: n for n in nodes}
    failures += not check("all 9 steps survive resolve_nodes", len(nodes) == 9, str(len(nodes)))
    failures += not check("member_id assigned to every step", all(n.member_id for n in nodes))
    # resolve_nodes returns every leaf in its tail list (pre-existing behaviour, also true for a
    # prun-only workflow). The workflow tails that matter are the is_tail flags, which is what
    # workflow_parser keys root_outputs off, so assert those survive instead.
    failures += not check(
        "is_tail preserved through resolve_nodes",
        {n.name for n in nodes if n.is_tail} == {"merge_ntup", "deriv_phys"},
        str({n.name for n in nodes if n.is_tail}),
    )

    evgen_out = by_name["evgen"].outputs["evgen/EVNT"]["value"]
    print(f"    evgen/EVNT -> {evgen_out}")
    failures += not check("author-supplied output name survives resolve_nodes", evgen_out.startswith("mc23_13p6TeV.526140"))
    failures += not check("no generated name was substituted in", "_001_evgen" not in evgen_out)
    failures += not check(
        "both placeholders still outstanding", has_placeholder(evgen_out, WFID_PLACEHOLDER) and has_placeholder(evgen_out, TASKID_PLACEHOLDER)
    )

    print("\n--- parent edges")
    id_to_name = {n.id: n.name for n in nodes}
    for name in ["merge_evnt", "recon", "deriv_phys"]:
        print(f"    {name} parents: {sorted(id_to_name[p] for p in by_name[name].parents)}")
    failures += not check("deriv_phys branches off merge_aod", {id_to_name[p] for p in by_name["deriv_phys"].parents} == {"merge_aod"})
    failures += not check("recon depends only on merge_hits", {id_to_name[p] for p in by_name["recon"].parents} == {"merge_hits"})

    print("\n--- input values resolved from the producing step's outputs")
    merge_evnt_in = [v.get("value") for v in by_name["merge_evnt"].inputs.values()]
    print(f"    merge_evnt input value: {merge_evnt_in}")
    failures += not check("merge_evnt input resolved to evgen's output dataset", merge_evnt_in == [evgen_out], str(merge_evnt_in))
    rdo = [v.get("value") for v in by_name["recon"].inputs.values() if v["source"] == "rdo_bkg"]
    print(f"    recon workflow-input value: {rdo}")
    failures += not check("workflow input reference resolved", rdo and rdo[0] == wfd["inputs"]["rdo_bkg"], str(rdo))

    print("\n=== resolve_params / verify ===")
    id_map = wnu.get_node_id_map(nodes)
    for n in nodes:
        n.resolve_params(None, id_map)
    failures += not check("task_params built without a CLI template", all(n.task_params for n in nodes))
    for n in nodes:
        ok, msg = n.verify()
        if not ok:
            failures += not check(f"verify {n.name}", False, msg)
    failures += not check("all steps verify", all(n.verify()[0] for n in nodes))

    print("\n--- verify rejects bad task params")
    for label, mutate in [
        ("missing taskName", lambda tp: tp.pop("taskName")),
        ("parentTaskName present", lambda tp: tp.update({"parentTaskName": "some.parent"})),
        ("${TASKID} in taskName", lambda tp: tp.update({"taskName": "x_tid${TASKID}"})),
        ("output without dataset", lambda tp: [j.pop("dataset") for j in tp["jobParameters"] if j.get("param_type") == "output"]),
        ("no output at all", lambda tp: tp.update({"jobParameters": [j for j in tp["jobParameters"] if j.get("param_type") != "output"]})),
    ]:
        node = wnu.Node(1, "task", None, True, "probe")
        node.task_params = json.loads(json.dumps(wfd["steps"]["evgen"]["task_params"]))
        mutate(node.task_params)
        ok, msg = node.verify()
        failures += not check(f"rejects {label}", not ok, "was accepted")
        if not ok:
            print(f"          -> {msg}")

    print("\n=== ${WFID} / ${TASKID} substitution ===")
    resolved_wfid = substitute_placeholder(evgen_out, WFID_PLACEHOLDER, 12345)
    resolved_both = substitute_placeholder(resolved_wfid, TASKID_PLACEHOLDER, 49900001)
    print(f"    authored  {evgen_out}")
    print(f"    + WFID    {resolved_wfid}")
    print(f"    + TASKID  {resolved_both}")
    failures += not check(
        "WFID resolved, TASKID still pending", has_placeholder(resolved_wfid, TASKID_PLACEHOLDER) and not has_placeholder(resolved_wfid, WFID_PLACEHOLDER)
    )
    failures += not check("both resolved", not has_placeholder(resolved_both, TASKID_PLACEHOLDER))
    failures += not check("JEDI per-job templates untouched", "${SN}" in substitute_placeholder("log.${TASKID}._${SN}.tgz", TASKID_PLACEHOLDER, 7))

    print("\n=== regression: prun steps are unaffected ===")
    prun_wfd = {
        "name": "prun_probe",
        "inputs": {"sig": "some:dataset"},
        "outputs": {"out": {"from": "b/outDS", "output_types": ["aaa.root"]}},
        "steps": {
            "a": {"type": "prun", "inDS": "{sig}", "args": "--outputs x.root", "exec": "echo"},
            "b": {"type": "prun", "inDS": "a/outDS", "args": "--outputs aaa.root", "exec": "echo"},
        },
    }
    p_nodes, p_root_in = wnu.parse_workflow_data(prun_wfd, log)
    failures += not check("prun outputs still keyed on outDS", all(set(n.outputs) == {f"{n.name}/outDS"} for n in p_nodes))
    failures += not check("prun outputs start empty (no pre-set value)", all(v == {} for n in p_nodes for v in n.outputs.values()))
    _, _, p_nodes = wnu.resolve_nodes(p_nodes, p_root_in, prun_wfd["inputs"], 0, set(), "user.me.myOut", log)
    p_by = {n.name: n for n in p_nodes}
    gen = p_by["b"].outputs["b/outDS"]["value"]
    print(f"    generated prun name: {gen}")
    failures += not check("prun name generation unchanged", gen == "user.me.myOut_002_b", gen)

    print(f"\n{'ALL CHECKS PASSED' if not failures else f'{failures} CHECK(S) FAILED'}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
