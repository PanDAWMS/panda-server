"""
Check that every task buffer method the workflow code calls actually exists on TaskBuffer.

The other harnesses drive the workflow against a fake task buffer, which implements whatever the
code happens to call -- so a method that exists on the DB proxy but was never wrapped on TaskBuffer
is invisible to them. That is exactly how on_all_inputs_done shipped calling updateTask_JEDI,
release_task_on_hold and push_task_trigger_message, none of which TaskBuffer exposed: the hook raised
AttributeError inside its own except clause on every cycle, so a workflow step's holdup was never
released and the chain stalled with the error only visible in the handler log.

TaskBuffer has no __getattr__, so an unwrapped proxy method is an AttributeError at runtime rather
than a delegated call. This test parses the sources (no DB, no imports) and compares.

Run from the repository root:  python3 pandaserver/workflow/examples/taskbuffer_interface_test.py
"""

import ast
import os
import pathlib
import re
import sys

REPO_ROOT = pathlib.Path(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))
TASK_BUFFER = REPO_ROOT / "pandaserver/taskbuffer/TaskBuffer.py"
# the workflow sources whose self.tbif calls must be satisfied by TaskBuffer
WORKFLOW_SOURCES = [
    "pandaserver/workflow/workflow_core.py",
    "pandaserver/workflow/step_handler_plugins/panda_task_step_handler.py",
    "pandaserver/workflow/step_handler_plugins/base_step_handler.py",
    "pandaserver/workflow/data_handler_plugins/panda_task_data_handler.py",
    "pandaserver/workflow/data_handler_plugins/ddm_collection_data_handler.py",
    "pandaserver/workflow/data_handler_plugins/base_data_handler.py",
]


def task_buffer_methods() -> set:
    tree = ast.parse(TASK_BUFFER.read_text())
    cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "TaskBuffer")
    return {f.name for f in cls.body if isinstance(f, ast.FunctionDef)}


def has_getattr_delegation() -> bool:
    tree = ast.parse(TASK_BUFFER.read_text())
    cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "TaskBuffer")
    return any(isinstance(f, ast.FunctionDef) and f.name in ("__getattr__", "__getattribute__") for f in cls.body)


def called_methods(path: pathlib.Path) -> dict:
    """Map method name -> sorted line numbers, for every self.tbif.<name>( call."""
    calls = {}
    for lineno, line in enumerate(path.read_text().splitlines(), start=1):
        for name in re.findall(r"self\.tbif\.([A-Za-z_][A-Za-z0-9_]*)\s*\(", line):
            calls.setdefault(name, []).append(lineno)
    return calls


def main():
    failures = 0
    available = task_buffer_methods()
    print(f"\nTaskBuffer exposes {len(available)} methods; __getattr__ delegation: {has_getattr_delegation()}")
    if has_getattr_delegation():
        print("  NOTE: TaskBuffer now delegates unknown attributes, so this test is advisory only")

    total = 0
    for rel in WORKFLOW_SOURCES:
        path = REPO_ROOT / rel
        if not path.exists():
            print(f"  FAIL  {rel} does not exist")
            failures += 1
            continue
        calls = called_methods(path)
        total += len(calls)
        missing = {n: ls for n, ls in calls.items() if n not in available}
        status = "PASS" if not missing else "FAIL"
        print(f"  {status}  {rel}  ({len(calls)} distinct calls)")
        for name, lines in sorted(missing.items()):
            print(f"          MISSING on TaskBuffer: {name}  (called at line{'s' if len(lines) > 1 else ''} {', '.join(map(str, lines))})")
            failures += 1

    print(f"\nchecked {total} distinct self.tbif call sites across {len(WORKFLOW_SOURCES)} modules")

    # the three that were missing, called by on_all_inputs_done -- named explicitly so a revert is caught
    print("\nthe methods whose absence stalled workflow 130:")
    for name in ("updateTask_JEDI", "release_task_on_hold", "push_task_trigger_message"):
        present = name in available
        print(f"  {'PASS' if present else 'FAIL'}  {name}")
        if not present:
            failures += 1

    print(f"\n{'ALL CHECKS PASSED' if not failures else f'{failures} CHECK(S) FAILED'}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
