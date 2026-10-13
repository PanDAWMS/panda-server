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

# the lines of a log, a record of several lines among them
RECORDS = [
    "2026-10-08 00:00:01,001 adder: DEBUG start job=7353014619\n",
    "2026-10-08 00:00:02,001 adder: ERROR failed job=7353014619\n",
    "Traceback (most recent call last):\n",
    '  File "/opt/ddm.py", line 522, in f\n',
    "    raise X\n",
    "ValueError: boom\n",
    "2026-10-08 00:00:03,001 adder: DEBUG other\n",
    "2026-10-08 00:00:04,001 adder: DEBUG done job=7353014619\n",
]


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

    def test_fixed_strings(self) -> None:
        # any of the strings matches, each as it is: "." and "+" are no wildcards here
        lines = [
            "2026-10-08 00:00:01 getJobs < SITE 2026-10-08T00:00:01 > start\n",
            "2026-10-08 00:00:02 a.b+c/d\n",
            "2026-10-08 00:00:03 axbbc/d\n",
            "2026-10-08 00:00:04 --pre=true\n",
        ]
        self._write("panda-a.log", "".join(lines))
        out = self._grep(fixed_strings=["getJobs < SITE 2026-10-08T00:00:01 >", "a.b+c/d", "--pre=true"], log_filename="panda-a.log")
        self.assertEqual(out["result"], lines[0] + lines[1] + lines[3])

    def test_matching_lines_alone(self) -> None:
        self._write("panda-a.log", "".join(RECORDS))
        out = self._grep(pattern="7353014619", log_filename="panda-a.log")
        self.assertEqual(out["result"], RECORDS[0] + RECORDS[1] + RECORDS[7])
        self.assertEqual((out["return_code"], out["truncated"]), (0, False))

    def test_whole_records(self) -> None:
        # each matching line comes with the lines of its record after it, up to the next stamp
        self._write("panda-a.log", "".join(RECORDS))
        with gzip.open(os.path.join(self.logdir, "panda-a.log-20261008.gz"), "wt") as f:
            f.write("".join(RECORDS))
        for name in ("panda-a.log", "panda-a.log-20261008.gz"):
            out = self._grep(pattern="7353014619", log_filename=name, whole_records=True)
            self.assertEqual(out["result"], "".join(RECORDS[0:6]) + RECORDS[7], name)
            self.assertEqual((out["return_code"], out["truncated"]), (0, False), name)

    def test_whole_records_matching_within_a_record(self) -> None:
        # a match in the traceback of a kept record adds no line twice, and a match in the
        # traceback of a record that does not match brings the lines after it
        self._write("panda-a.log", "".join(RECORDS))
        out = self._grep(pattern="ddm.py|ERROR failed", log_filename="panda-a.log", whole_records=True)
        self.assertEqual(out["result"], "".join(RECORDS[1:6]))
        out = self._grep(pattern="raise X", log_filename="panda-a.log", whole_records=True)
        self.assertEqual(out["result"], "".join(RECORDS[4:6]))

    def test_whole_records_cut(self) -> None:
        # a record longer than the most lines taken after its match is cut, and says so
        self._write("panda-a.log", "".join(RECORDS))
        with mock.patch.object(processor, "_MAX_CONTINUATION_LINES", 2):
            out = self._grep(pattern="ERROR failed", log_filename="panda-a.log", whole_records=True)
            self.assertEqual(out["result"], "".join(RECORDS[1:4]))
            self.assertTrue(out["truncated"])
        # a record of exactly as many lines as are taken is whole
        with mock.patch.object(processor, "_MAX_CONTINUATION_LINES", 4):
            out = self._grep(pattern="ERROR failed", log_filename="panda-a.log", whole_records=True)
            self.assertEqual(out["result"], "".join(RECORDS[1:6]))
            self.assertFalse(out["truncated"])

    def test_whole_records_in_a_tail_window(self) -> None:
        self._write("panda-a.log", "".join(RECORDS))
        tail_bytes = len("".join(RECORDS[4:]))
        out = self._grep(pattern="raise X|7353014619", log_filename="panda-a.log", whole_records=True, tail_bytes=tail_bytes)
        self.assertEqual(out["result"], RECORDS[4] + RECORDS[5] + RECORDS[7])

    def test_result_size_cap(self) -> None:
        # the matcher is stopped once no more lines fit; the kept lines are whole, and the
        # return code still says that lines matched
        lines = [f"2026-10-08 00:00:01 line {i:06d}\n" for i in range(10000)]
        self._write("panda-a.log", "".join(lines))
        with mock.patch.object(processor, "_MAX_RESULT_BYTES", 1000):
            out = self._grep(pattern="line", log_filename="panda-a.log")
        self.assertTrue(out["truncated"])
        self.assertEqual(out["return_code"], 0)
        self.assertEqual(out["result"], "".join(lines[: 1000 // len(lines[0])]))

    def test_max_matches(self) -> None:
        self._write("panda-a.log", "".join(RECORDS))
        out = self._grep(pattern="7353014619", log_filename="panda-a.log", max_matches=2)
        self.assertEqual(out["result"], RECORDS[0] + RECORDS[1])
        self.assertTrue(out["truncated"])

    def test_no_match(self) -> None:
        self._write("panda-a.log", "".join(RECORDS))
        out = self._grep(pattern="nothing", log_filename="panda-a.log", whole_records=True)
        self.assertEqual((out["status"], out["result"], out["return_code"], out["truncated"]), ("done", "", 1, False))

    def test_timeout(self) -> None:
        self._write("panda-a.log", "".join(RECORDS))
        with (
            mock.patch.object(processor, "_bounded_matcher", return_value=["sleep", "10"]),
            mock.patch.object(processor, "_SUBPROCESS_TIMEOUT", 0.2),
        ):
            out = self._grep(pattern="x", log_filename="panda-a.log")
        self.assertEqual((out["status"], out["error_msg"]), ("failed", "timeout"))


if __name__ == "__main__":
    unittest.main()
