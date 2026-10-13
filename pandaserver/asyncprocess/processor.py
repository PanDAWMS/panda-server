"""
Shared async request processing logic.
Each service wraps this with a thin entrypoint that passes its service_name.
New request types: add a handler function and register it in HANDLERS.
"""

import json
import os
import re
import socket
import subprocess
import tempfile
import threading
import time
from collections.abc import Callable
from typing import Any

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandautils.PandaUtils import naive_utcnow

from pandaserver.asyncprocess import TaskBufferLike, data_carousel_handlers, db_read_handlers
from pandaserver.config import panda_config
from pandaserver.taskbuffer.db_proxy_mods.async_request_module import ANY_MACHINE

_logger = PandaLogger().getLogger("async_request_processor")

MY_HOSTNAME = socket.getfqdn()

# max subprocess timeout in seconds
_SUBPROCESS_TIMEOUT = 240

# stale result threshold: must exceed the longest handler subprocess timeout
_STALE_THRESHOLD_SECONDS = _SUBPROCESS_TIMEOUT * 2

# max result size stored in DB (bytes)
_MAX_RESULT_BYTES = 1_000_000

# time.monotonic() when the cycle running in this process started. The daemon's timeout counts
# from then, over every request the cycle processes, so a handler with a deadline of its own
# reads it through get_cycle_started_at()
_cycle_started_at: float | None = None


def get_cycle_started_at() -> float | None:
    """
    Get when the processing cycle running in this process started.

    Returns:
        float or None: time.monotonic() at the start of the cycle, or None outside one
    """
    return _cycle_started_at


# Cap on how many matching lines a grep may produce.  Log files here reach
# several gigabytes, and an unselective pattern against one of those makes the
# matcher stream the whole file back.  The output is read as it comes and the
# matcher stopped once _MAX_RESULT_BYTES of it are kept, so the daemon holds no
# more than that; the cap stops the matcher sooner when a caller asks for fewer
# lines.  The default is set high enough that the stored result and its
# truncated flag are what they were before.
_DEFAULT_MAX_MATCHES = 20000
_MAX_MAX_MATCHES = 200000

# A log record starts with its time stamp.  The lines of a traceback, or of
# any other message of several lines, continue it without one.
_RECORD_START = re.compile(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}")

# Most lines a grep for whole records takes after a matching line.  A record
# cut there makes the result come back truncated, as reaching any other cap
# does, so that the cut record is not read as whole.
_MAX_CONTINUATION_LINES = 200

# A line of the matcher's output with -n: its number, then ":" when the line
# matches or "-" when it is one of the lines after a match (-A).  Between lines
# that are not adjacent comes a line of "--".
_NUMBERED_LINE = re.compile(r"(\d+)([:-])")

# A file opened at an arbitrary offset is not a valid gzip stream, so a tail
# window is only meaningful on plain text.
_GZIP_SUFFIX = ".gz"

# Kept verbatim because callers distinguish "the file is not there" -- which
# says the code writing it never ran -- from a query that failed for some other
# reason, and that distinction must not depend on which tool reported it.
_NO_SUCH_FILE = "No such file or directory"


def _bounded_matcher(log_path: str, patterns: list[str], fixed: bool, max_matches: int, whole_records: bool, from_stdin: bool) -> list[str]:
    """The matcher command, capped, reading a path or standard input.

    A line matches when any of the patterns does, read as plain text when fixed
    is set and as regular expressions otherwise. Each pattern goes after -e,
    never in a place of its own: there a pattern starting with "-" is read as an
    option, and rg's --pre=COMMAND runs COMMAND on every file under the daemon's
    working directory.

    rg reads a .gz file too, decompressing it with -z, so that a pattern means
    the same in every file. zgrep, used before, read it as a basic regular
    expression of grep, where "+" and "|" are plain characters.

    For whole records, each match comes with one line more than a record may
    continue over, numbered, so that _GrepOutput can tell where a record ends
    and whether it was cut.
    """
    cmd = ["rg", "-m", str(max_matches)]
    if fixed:
        cmd.append("-F")
    if whole_records:
        cmd += ["-n", "-A", str(_MAX_CONTINUATION_LINES + 1)]
    for pattern in patterns:
        cmd += ["-e", pattern]
    if log_path.endswith(_GZIP_SUFFIX):
        cmd.append("-z")
    return cmd if from_stdin else cmd + [log_path]


class _GrepOutput:
    """The lines a grep keeps of the matcher's output, up to _MAX_RESULT_BYTES."""

    def __init__(self, whole_records: bool) -> None:
        self.whole_records = whole_records
        self.lines: list[str] = []
        self.size = 0
        self.matches = 0
        # whether _MAX_RESULT_BYTES are kept, which stops the matcher
        self.full = False
        # whether a record was cut at _MAX_CONTINUATION_LINES
        self.cut = False
        # whether the lines after a match still continue the record it is in,
        # and how many of them have been kept
        self._in_record = False
        self._continuation = 0

    def add(self, line: str) -> bool:
        """Take one line of the matcher's output, and say whether more fit."""
        if not self.whole_records:
            self.matches += 1
            return self._keep(line)

        numbered = _NUMBERED_LINE.match(line)
        if numbered is None:
            # the "--" between lines that are not adjacent
            self._in_record = False
            return True
        text = line[numbered.end() :]
        starts_record = _RECORD_START.match(text) is not None
        if numbered.group(2) == ":":
            self.matches += 1
            # a match without a stamp in a kept record only continues it
            if starts_record or not self._in_record:
                self._in_record = True
                self._continuation = 0
            else:
                self._continuation += 1
            return self._keep(text)
        if not self._in_record or starts_record:
            self._in_record = False
            return True
        if self._continuation >= _MAX_CONTINUATION_LINES:
            self._in_record = False
            self.cut = True
            return True
        self._continuation += 1
        return self._keep(text)

    def _keep(self, text: str) -> bool:
        if self.size + len(text) > _MAX_RESULT_BYTES:
            self.full = True
            return False
        self.lines.append(text)
        self.size += len(text)
        return True


def _run_grep(log_path: str, patterns: list[str], fixed: bool, max_matches: int, tail_bytes: int | None, whole_records: bool) -> tuple[_GrepOutput, int, str]:
    """Run the matcher, optionally over just the tail of the file.

    The pipe is built process to process rather than through a shell: the
    pattern is caller-supplied, so handing it to a shell would hand the caller
    the daemon.

    The output is read as it comes, and the matcher stopped once it no longer
    fits, so the daemon never holds more of it than what is kept.  A matcher so
    stopped had matched, and its return code is given as 0.

    Returns:
        the lines kept, the return code of the matcher, and its standard error
    """
    windowed = bool(tail_bytes) and not log_path.endswith(_GZIP_SUFFIX)
    cmd = _bounded_matcher(log_path, patterns, fixed, max_matches, whole_records, from_stdin=windowed)
    output = _GrepOutput(whole_records)
    # standard error goes to a file, since a pipe nobody reads until the end
    # could fill and stall the matcher
    with tempfile.TemporaryFile() as stderr_file:
        tail = None
        if windowed:
            tail = subprocess.Popen(
                ["tail", "-c", str(tail_bytes), log_path],
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
            )
        matcher = subprocess.Popen(
            cmd,
            stdin=tail.stdout if tail is not None else subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=stderr_file,
            text=True,
            errors="replace",
        )
        # Hand the read end over completely.  Keeping a copy here would leave the
        # matcher's exit at the cap invisible to tail, which would then block on a
        # full pipe instead of being told to stop.
        if tail is not None and tail.stdout is not None:
            tail.stdout.close()

        timed_out = threading.Event()

        def stop_at_timeout() -> None:
            timed_out.set()
            matcher.kill()

        timer = threading.Timer(_SUBPROCESS_TIMEOUT, stop_at_timeout)
        timer.start()
        try:
            if matcher.stdout is not None:
                for line in matcher.stdout:
                    if not output.add(line):
                        break
        finally:
            timer.cancel()
            if matcher.poll() is None:
                matcher.kill()
            matcher.wait()
            if matcher.stdout is not None:
                matcher.stdout.close()
            if tail is not None:
                # tail dying of SIGPIPE is the cap working, not a failure, so its status
                # is deliberately not inspected -- the file was checked before we got here.
                tail.kill()
                tail.wait()
        if timed_out.is_set():
            raise subprocess.TimeoutExpired(cmd, _SUBPROCESS_TIMEOUT)
        stderr_file.seek(0)
        stderr = stderr_file.read(_MAX_RESULT_BYTES + 1).decode(errors="replace")
    return_code = 0 if output.full else matcher.returncode
    return output, return_code, stderr


def _handle_grep(row: dict[str, Any], tb: TaskBufferLike, tmp_logger: LogWrapper, result_machine: str) -> None:
    """Run rg on a log file and store the output under result_machine's result row."""
    params = json.loads(row["parameters"])
    log_filename = params["log_filename"]
    # either one regular expression or any number of plain strings
    fixed_strings = params.get("fixed_strings")
    fixed = fixed_strings is not None
    patterns = fixed_strings if fixed else [params["pattern"]]
    max_matches = min(int(params.get("max_matches") or _DEFAULT_MAX_MATCHES), _MAX_MAX_MATCHES)
    tail_bytes = params.get("tail_bytes")
    whole_records = bool(params.get("whole_records"))
    log_path = os.path.join(panda_config.logdir, log_filename)

    # Checked here rather than left to the tool, because with a tail window the
    # matcher reads standard input and never sees the path -- it would report
    # "no match" for a file that does not exist.  One answer, whichever way the
    # query was built.
    if not os.path.isfile(log_path):
        tmp_logger.debug(f"{log_path}: {_NO_SUCH_FILE}")
        tb.finish_async_result(
            row["request_id"],
            result_machine,
            "failed",
            error_msg=f"{log_path}: {_NO_SUCH_FILE}",
            retriable=False,
        )
        return

    shown = f"{len(patterns)} fixed strings {patterns[:3]!r}" if fixed else repr(patterns[0])
    tmp_logger.debug(f"grep {shown} in {log_path} (max_matches={max_matches}, tail_bytes={tail_bytes}, whole_records={whole_records})")
    try:
        start_time = naive_utcnow()
        output, return_code, stderr = _run_grep(log_path, patterns, fixed, max_matches, tail_bytes, whole_records)
        elapsed = (naive_utcnow() - start_time).total_seconds()
        tmp_logger.debug(f"subprocess completed in {elapsed:.2f} seconds")
    except subprocess.TimeoutExpired:
        tmp_logger.error(f"subprocess timed out after {_SUBPROCESS_TIMEOUT} seconds")
        tb.finish_async_result(
            row["request_id"],
            result_machine,
            "failed",
            error_msg="timeout",
            retriable=False,
        )
        return
    except Exception as e:
        tmp_logger.error(f"subprocess failed with exception: {e}")
        tb.finish_async_result(
            row["request_id"],
            result_machine,
            "failed",
            error_msg=str(e),
            retriable=False,
        )
        return

    # Reaching the cap truncates just as surely as exceeding the byte limit
    # does, and a caller that reads an incomplete result as "this never appears
    # in the log" gets the wrong answer, so both set the same flag.  So does a
    # record cut short.
    capped = output.matches >= max_matches
    truncated = capped or output.full or output.cut or len(stderr) > _MAX_RESULT_BYTES
    tmp_logger.debug(
        f"outcome: return code {return_code}, {output.matches} matches, result size {output.size}, stderr size {len(stderr)}, "
        f"truncated={truncated} (capped={capped} full={output.full} cut={output.cut})"
    )
    tb.finish_async_result(
        row["request_id"],
        result_machine,
        "done",
        result="".join(output.lines),
        stderr=stderr[:_MAX_RESULT_BYTES],
        return_code=return_code,
        truncated=truncated,
    )


def _handle_sleep_echo(row: dict[str, Any], tb: TaskBufferLike, tmp_logger: LogWrapper, result_machine: str) -> None:
    """Sleep for the requested seconds, then echo the message; store echo stdout as the result."""
    params = json.loads(row["parameters"])
    seconds = params["seconds"]
    message = params["message"]

    tmp_logger.debug(f"sleeping {seconds}s then echoing")
    try:
        subprocess.run(["sleep", str(seconds)], capture_output=True, text=True, timeout=_SUBPROCESS_TIMEOUT)
        proc = subprocess.run(["echo", message], capture_output=True, text=True, timeout=_SUBPROCESS_TIMEOUT)
    except subprocess.TimeoutExpired:
        tmp_logger.error(f"subprocess timed out after {_SUBPROCESS_TIMEOUT} seconds")
        tb.finish_async_result(
            row["request_id"],
            result_machine,
            "failed",
            error_msg="timeout",
            retriable=False,
        )
        return
    except Exception as e:
        tmp_logger.error(f"subprocess failed with exception: {e}")
        tb.finish_async_result(
            row["request_id"],
            result_machine,
            "failed",
            error_msg=str(e),
            retriable=False,
        )
        return

    stdout = proc.stdout
    stderr = proc.stderr
    truncated = len(stdout) > _MAX_RESULT_BYTES or len(stderr) > _MAX_RESULT_BYTES
    tmp_logger.debug(f"outcome: return code {proc.returncode}, stdout size {len(stdout)}, truncated={truncated}")
    tb.finish_async_result(
        row["request_id"],
        result_machine,
        "done",
        result=stdout[:_MAX_RESULT_BYTES],
        stderr=stderr[:_MAX_RESULT_BYTES],
        return_code=proc.returncode,
        truncated=truncated,
    )


# Register new request types here — no new daemon needed. The handlers take the same four
# arguments but are otherwise unrelated functions, hence the bare Callable
HANDLERS: dict[str, Callable[..., None]] = {
    "grep": _handle_grep,
    "sleep_echo": _handle_sleep_echo,
    # Data Carousel operations, submitted by pandaserver.api.v1.data_carousel_api
    **data_carousel_handlers.HANDLERS,
    # reads of the database, submitted by pandaserver.api.v1.async_process_api
    **db_read_handlers.HANDLERS,
}


def run(service_name: str, tbuf: TaskBufferLike = None) -> None:
    """
    Process one daemon cycle for the given service.
    Call this from the service-specific entrypoint (daemon script or WatchDog).
    tbuf: an already-initialised TaskBuffer, or None to use the module-level singleton.
    """
    global _cycle_started_at
    _cycle_started_at = time.monotonic()
    _logger.debug(f"start with service={service_name}")
    if tbuf is None:
        from pandaserver.taskbuffer.TaskBuffer import taskBuffer as tbuf

    # keep this machine's liveness record current
    tbuf.upsert_machine_heartbeat(MY_HOSTNAME, service_name)

    # recover stale running rows from previous crashed cycles: this machine's own rows, plus
    # shared "any" rows orphaned by a crashed winner (any machine can reclaim them afterwards)
    tbuf.recover_stale_results(MY_HOSTNAME, max_processing_seconds=_STALE_THRESHOLD_SECONDS)
    tbuf.recover_stale_results(ANY_MACHINE, max_processing_seconds=_STALE_THRESHOLD_SECONDS)

    # find requests this machine should process
    pending = tbuf.get_pending_requests_for_machine(MY_HOSTNAME, service_name, list(HANDLERS.keys()))
    for row in pending:
        request_id = row["request_id"]
        request_type = row["request_type"]
        try:
            requester = json.loads(row["parameters"]).get("requester", "unknown")
        except json.JSONDecodeError:
            requester = "unknown"
        tmp_logger = LogWrapper(_logger, prefix=f"< request_id={request_id} >")
        handler = HANDLERS.get(request_type)
        if handler is None:
            tmp_logger.warning(f"unknown request_type={request_type}")
            continue
        # "any" requests share a single result row keyed by the sentinel, so the claim and the
        # result are written under ANY_MACHINE; single/all requests use this machine's hostname
        result_machine = ANY_MACHINE if row.get("machine_name") == ANY_MACHINE else MY_HOSTNAME
        if not tbuf.claim_async_result(request_id, result_machine):
            # another daemon instance (or, for "any", another machine) claimed it first
            continue
        tmp_logger.debug(f"processing type={request_type} request from '{requester}'")
        handler(row, tbuf, tmp_logger, result_machine)
    _logger.debug("done")
