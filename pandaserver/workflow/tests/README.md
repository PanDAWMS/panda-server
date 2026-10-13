# Native workflow offline harnesses

Standalone scripts that exercise the workflow engine with its dependencies stubbed, so they need
no database, no message broker and no PanDA installation. They are run by hand, not by CI, and each
prints one line per assertion and exits non-zero if any of them failed.

Run one, from the repository root:

```
python3 pandaserver/workflow/tests/workflow_done_transition_test.py
```

Or all of them:

```
for t in pandaserver/workflow/tests/*_test.py; do python3 "$t"; done
```

The workflow descriptions they read, and the examples they are written against, are in
`pandaserver/workflow/examples/`.
