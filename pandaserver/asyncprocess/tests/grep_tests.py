# Description: Unit tests for the handler of the async grep requests. They run rg on log files
# written to a temporary directory, so they are skipped where rg is not installed
import gzip
import json
import os
import shutil
import tempfile
import unittest
from typing import Any
from unittest import mock

from pandaserver.asyncprocess import processor
from pandaserver.config import panda_config


@unittest.skipUnless(shutil.which("rg"), "rg is not installed")
class TestHandleGrep(unittest.TestCase):
    def setUp(self) -> None:
        self.logdir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.logdir)
        patcher = mock.patch.object(panda_config, "logdir", self.logdir, create=True)
        patcher.start()
        self.addCleanup(patcher.stop)

    def _write(self, name: str, text: str) -> None:
        with open(os.path.join(self.logdir, name), "w") as f:
            f.write(text)

    def _grep(self, **params: Any) -> dict[str, Any]:
        """Run the handler on a request with these parameters, and give what it stored."""
        task_buffer = mock.MagicMock()
        row = {"request_id": "r1", "parameters": json.dumps(params)}
        processor._handle_grep(row, task_buffer, mock.MagicMock(), "host")
        args, kwargs = task_buffer.finish_async_result.call_args
        return {"status": args[2], **kwargs}

    def test_pattern_starting_with_a_dash_is_searched_for(self) -> None:
        # in the place of an argument of its own, rg would read it as --pre and run the command
        # on every file under the working directory
        self._write("panda-a.log", "2026-10-08 00:00:01 opts --pre=true\n2026-10-08 00:00:02 other\n")
        out = self._grep(pattern="--pre=true", log_filename="panda-a.log")
        self.assertEqual(out["status"], "done")
        self.assertEqual(out["result"], "2026-10-08 00:00:01 opts --pre=true\n")

    def test_pattern_means_the_same_in_a_gz_file(self) -> None:
        # "+" repeats in rg; zgrep read it as a plain character in its basic regular expressions
        text = "2026-10-08 00:00:01 aaab\n2026-10-08 00:00:02 a+b\n"
        self._write("panda-a.log", text)
        with gzip.open(os.path.join(self.logdir, "panda-a.log-20261008.gz"), "wt") as f:
            f.write(text)
        for name in ("panda-a.log", "panda-a.log-20261008.gz"):
            out = self._grep(pattern="a+b", log_filename=name)
            self.assertEqual(out["result"], "2026-10-08 00:00:01 aaab\n", name)


if __name__ == "__main__":
    unittest.main()
