"""
Differential check: prun / nested / scatter parsing must be identical before and after the
raw-task-params ("task") step support was added.

Loads the pre-change workflow_native_utils.py out of a pinned baseline commit alongside the
working-tree version, runs both over the same workflow descriptions, and diffs the resulting node
lists.

The baseline is pinned to an explicit commit rather than HEAD: once the change under test is
committed, HEAD contains it too and the comparison would silently be new-against-new.

The descriptions below mirror the shapes of pandaserver/workflow/examples/*.yaml. They are given as
dicts rather than loaded from the YAML files because no YAML loader is installed in a bare checkout
-- and because the YAML->dict step is not touched by this change, so parsing from an equivalent
dict exercises exactly the code under test.

Run from the repository root:  python3 pandaserver/workflow/examples/regression_diff_test.py
"""

import copy
import difflib
import importlib.util
import json
import os
import subprocess
import sys
import tempfile
import types

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
sys.path.insert(0, REPO_ROOT)
MODULE_PATH = "pandaserver/workflow/workflow_native_utils.py"
# Last commit before raw-task-params ("task") step support was added
MODULE_BASE_COMMIT = "83733672"


def _stub(name, **attrs):
    mod = types.ModuleType(name)
    for k, v in attrs.items():
        setattr(mod, k, v)
    sys.modules[name] = mod


_stub("pandaclient", PhpoScript=types.SimpleNamespace(main=None), PrunScript=types.SimpleNamespace(main=None))
_stub("pandacommon")
_stub("pandacommon.pandautils")
_stub("pandacommon.pandautils.base", SpecBase=object)
_stub("pandaserver.config", panda_config=types.SimpleNamespace(schemaJEDI="ATLAS_PANDA", schemaDEFT="ATLAS_DEFT"))


class Log:
    def info(self, m):
        pass

    def debug(self, m):
        pass

    def warning(self, m):
        pass

    def error(self, m):
        pass


def load_old_module():
    """Load the pre-change module from the pinned baseline commit under its own module name."""
    src = subprocess.run(
        ["git", "show", f"{MODULE_BASE_COMMIT}:{MODULE_PATH}"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    tmp = tempfile.NamedTemporaryFile("w", suffix=".py", delete=False)
    tmp.write(src)
    tmp.close()
    spec = importlib.util.spec_from_file_location("wnu_head", tmp.name)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    os.unlink(tmp.name)
    return mod


# ---- descriptions mirroring the shapes of the checked-in examples -------------------------
CASES = {
    # multistep_merge_wfd.yaml -- a plain serial prun chain
    "serial_prun_chain": {
        "name": "multistep_merge_chain",
        "inputs": {"input_to_merge": "user.sgaid:user.sgaid.some.input"},
        "outputs": {"final_output": {"from": "third/outDS", "output_types": ["merge.root"]}},
        "steps": {
            "first": {"type": "prun", "inDS": "{input_to_merge}", "args": "--outputs merge.root --noBuild", "exec": "merge.sh"},
            "second": {"type": "prun", "inDS": "first/outDS", "args": "--outputs merge.root --noBuild", "exec": "merge.sh"},
            "third": {"type": "prun", "inDS": "second/outDS", "args": "--outputs merge.root --noBuild", "exec": "merge.sh"},
        },
    },
    # signal_background_combine_wfd.yaml -- branches, secondaryDSs, inDsType, containerImage
    "branching_prun_with_secondaries": {
        "name": "signal_background_combine",
        "inputs": {"signal": "mc16_valid:mc16_valid.signal.HITS", "background": "mc16_5TeV.background.HITS/"},
        "outputs": {"outDS": {"from": "combine/outDS", "output_types": ["aaa.root"]}},
        "steps": {
            "make_signal": {
                "type": "prun",
                "inDS": "{signal}",
                "containerImage": "docker://busybox",
                "args": "--outputs abc.dat,def.zip --nFilesPerJob 5",
                "exec": "echo %IN > abc.dat; echo 123 > def.zip",
            },
            "make_background_1": {"type": "prun", "inDS": "{background}", "args": "--outputs opq.root,xyz.pool", "exec": "echo %IN > opq.root"},
            "generate_some": {"type": "prun", "args": "--outputs gen.root --nJobs 10", "exec": "echo %RNDM:10 > gen.root"},
            "premix": {
                "type": "prun",
                "inDS": "make_signal/outDS",
                "inDsType": "def.zip",
                "secondaryDSs": ["make_background_1/outDS"],
                "secondaryDsTypes": ["xyz.pool"],
                "args": "--outputs klm.root --secondaryDSs IN2:13:%{SECDS1}",
                "exec": "echo %IN %IN2 > klm.root",
            },
            "make_background_2": {
                "type": "prun",
                "inDS": "{background}",
                "containerImage": "docker://alpine",
                "secondaryDSs": ["generate_some/outDS"],
                "secondaryDsTypes": ["gen.root"],
                "args": "--outputs ooo.root,jjj.txt --secondaryDSs IN2:10:%{SECDS1}",
                "exec": "echo %IN > ooo.root",
            },
            "combine": {
                "type": "prun",
                "inDS": "make_signal/outDS",
                "inDsType": "abc.dat",
                "secondaryDSs": ["premix/outDS", "make_background_2/outDS"],
                "secondaryDsTypes": ["klm.root", "ooo.root"],
                "args": "--outputs aaa.root --secondaryDSs IN2:2:%{SECDS1},IN3:5:%{SECDS2}",
                "exec": "echo %IN %IN2 %IN3 > aaa.root",
            },
        },
    },
    # nested_workflow_inline_sig_bg_comb_wfd.yaml -- an inline sub-workflow
    "inline_sub_workflow": {
        "name": "nested_inline",
        "inputs": {"signal": "mc16_valid:signal.HITS", "background": "mc16_5TeV:background.HITS/"},
        "outputs": {"final": {"from": "merge/outDS", "output_types": ["merged.root"]}},
        "steps": {
            "sig_bg_comb": {
                "type": "workflow",
                "inputs": {"signal": "{signal}", "background": "{background}"},
                "outputs": {"combined": {"from": "combine/outDS", "output_types": ["aaa.root"]}},
                "steps": {
                    "make_signal": {"type": "prun", "inDS": "{signal}", "args": "--outputs abc.dat", "exec": "echo %IN > abc.dat"},
                    "combine": {"type": "prun", "inDS": "make_signal/outDS", "args": "--outputs aaa.root", "exec": "echo %IN > aaa.root"},
                },
            },
            "merge": {"type": "prun", "inDS": "sig_bg_comb/outDS", "args": "--outputs merged.root", "exec": "echo %IN > merged.root"},
        },
    },
    # scatter_sig_bg_comb_wfd.yaml -- a scatter sub-workflow
    "scatter_sub_workflow": {
        "name": "scatter_nested",
        "inputs": {"signals": ["ds.sig.a", "ds.sig.b", "ds.sig.c"]},
        "outputs": {"final": {"from": "merge/outDS", "output_types": ["merged.root"]}},
        "steps": {
            "per_signal": {
                "type": "workflow",
                "scatter_inputs": {"signal": "signals"},
                "scatter_mode": "zip",
                "outputs": {"combined": {"from": "combine/outDS", "output_types": ["aaa.root"]}},
                "steps": {
                    "combine": {"type": "prun", "inDS": "{signal}", "args": "--outputs aaa.root", "exec": "echo %IN > aaa.root"},
                },
            },
            "merge": {"type": "prun", "inDS": "per_signal/outDS", "args": "--outputs merged.root", "exec": "echo %IN > merged.root"},
        },
    },
}


def normalise(nodes):
    """Serialise a node list to a stable, comparable form."""

    def default(obj):
        if isinstance(obj, set):
            return sorted(obj, key=str)
        if obj.__class__.__name__ == "Node":
            return f"<Node id={obj.id}>"
        return str(obj)

    return json.dumps([{k: v for k, v in sorted(vars(n).items())} for n in nodes], indent=2, sort_keys=True, default=default)


def run(module, description, out_ds_name):
    log = Log()
    nodes, root_in = module.parse_workflow_data(copy.deepcopy(description), log)
    data = description.get("inputs", {})
    serial, tails, nodes = module.resolve_nodes(nodes, dict(root_in), copy.deepcopy(data), 0, set(), out_ds_name, log)
    module.set_workflow_outputs(nodes)
    id_map = module.get_node_id_map(nodes)
    for n in nodes:
        # task_template is None so make_task_params (which needs a real PrunScript) is skipped
        n.resolve_params(None, id_map)
    verdicts = {n.name: n.verify() for n in nodes}
    return normalise(nodes), sorted(t.name for t in tails), verdicts


def main():
    old = load_old_module()
    from pandaserver.workflow import workflow_native_utils as new

    print(f"comparing working tree against {MODULE_BASE_COMMIT} ({MODULE_PATH})\n")
    failures = 0
    for case_name, description in CASES.items():
        out_ds_name = "user.me.myOutDS"
        old_dump, old_tails, old_verdicts = run(old, description, out_ds_name)
        new_dump, new_tails, new_verdicts = run(new, description, out_ds_name)

        identical = old_dump == new_dump and old_tails == new_tails and old_verdicts == new_verdicts
        print(f"  {'PASS' if identical else 'FAIL'}  {case_name}  ({len(json.loads(new_dump))} nodes)")
        if not identical:
            failures += 1
            if old_tails != new_tails:
                print(f"        tails differ: {old_tails} -> {new_tails}")
            if old_verdicts != new_verdicts:
                print(f"        verify differs: {old_verdicts} -> {new_verdicts}")
            for line in list(difflib.unified_diff(old_dump.splitlines(), new_dump.splitlines(), MODULE_BASE_COMMIT, "working-tree", lineterm=""))[:40]:
                print(f"        {line}")

    print(f"\n{'ALL CASES IDENTICAL' if not failures else f'{failures} CASE(S) DIFFER'}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
