"""
Shared async request processing logic.
Each service wraps this with a thin entrypoint that passes its service_name.
New request types: add a handler function and register it in HANDLERS.
"""

import datetime
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
from pandaserver.config import daemon_config, panda_config
from pandaserver.taskbuffer.db_proxy_mods.async_request_module import ANY_MACHINE, SERVICE_SERVER

_logger = PandaLogger().getLogger("async_request_processor")

MY_HOSTNAME = socket.getfqdn()

# max subprocess timeout in seconds
_SUBPROCESS_TIMEOUT = 240

# stale result threshold: must exceed the longest handler subprocess timeout
_STALE_THRESHOLD_SECONDS = _SUBPROCESS_TIMEOUT * 2

# max result size stored in DB (bytes)
_MAX_RESULT_BYTES = 1_000_000

# Longest a request of each type may run, for the types whose requests can
# run long.  One is not started when the cycle has less time left than that,
# and stays pending for the next cycle: started anyway, it would be killed with
# the cycle, wait out _STALE_THRESHOLD_SECONDS, and fail after three attempts.
# They also come after the other requests of a cycle, which a long one before
# them would leave too little time: a db_read is refused when the cycle has
# none left for its queries.
_LONGEST_RUN_SECONDS = {"grep": _SUBPROCESS_TIMEOUT}

# time.monotonic() when the cycle running in this process started. The daemon's timeout counts
# from then, over every request the cycle processes, so a handler with a deadline of its own
# reads it through get_cycle_started_at()
_cycle_started_at: float | None = None


def _cycle_seconds(service_name: str) -> int | None:
    """
    Get how long a cycle may take requests for, before the daemon master kills it.

    Only async_request_daemon of the server runs under such a timeout.  JEDI's
    AsyncRequestWatchDog has none, and a cycle run by hand with no enabled
    entry in the daemon config has none either.  The seconds that a db_read
    keeps at the end to store its result are kept here too.

    Args:
        service_name(str): the service the cycle runs for

    Returns:
        int or None: the seconds, or None when nothing kills the cycle
    """
    if service_name != SERVICE_SERVER:
        return None
    daemon = daemon_config.find_daemon(db_read_handlers.DAEMON_MODULE)
    if daemon is None:
        return None
    return daemon_config.get_timeout(daemon) - db_read_handlers.MARGIN_SECONDS


def _fits_in_cycle(request_type: str, cycle_seconds: int | None, cycle_started_at: float) -> bool:
    """
    Tell whether a request of this type can run to its end before the cycle is killed.

    A cycle too short for the longest run of a type at all still starts its
    requests, as it always did, since leaving them pending would leave them
    there for good.

    Args:
        request_type(str): the type of the request
        cycle_seconds(int or None): what _cycle_seconds gives
        cycle_started_at(float): time.monotonic() when the cycle started

    Returns:
        bool: False when the request should wait for the next cycle
    """
    longest = _LONGEST_RUN_SECONDS.get(request_type)
    if longest is None or cycle_seconds is None or longest > cycle_seconds:
        return True
    return time.monotonic() + longest <= cycle_started_at + cycle_seconds


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
_RECORD_START_BYTES = re.compile(rb"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}")

# Most lines a grep for whole records takes after a matching line.  A record
# cut there makes the result come back truncated, as reaching any other cap
# does, so that the cut record is not read as whole.
_MAX_CONTINUATION_LINES = 200

# A line of the matcher's output with -b: the byte offset at which the line
# starts, then ":" when the line matches or "-" when it is one of the lines
# after a match (-A).  Between lines that are not adjacent comes a line of "--".
_OFFSET_LINE = re.compile(r"(\d+)([:-])")

# Room that a record takes in the JSON of a result besides its lines, and room
# for the rest of that JSON; both more than they ever take.
_RECORD_JSON_BYTES = 100
_RESULT_JSON_BYTES = 1000

# Most bytes read from the start of a tail window to find its first time stamp.
_WINDOW_STAMP_SCAN_BYTES = 1_000_000

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
    continue over, each line with its byte offset, so that _GrepOutput can tell
    where a record starts and ends, and whether it was cut.
    """
    cmd = ["rg", "-m", str(max_matches)]
    if fixed:
        cmd.append("-F")
    if whole_records:
        cmd += ["-b", "-A", str(_MAX_CONTINUATION_LINES + 1)]
    for pattern in patterns:
        cmd += ["-e", pattern]
    if log_path.endswith(_GZIP_SUFFIX):
        cmd.append("-z")
    return cmd if from_stdin else cmd + [log_path]


class _GrepOutput:
    """What a grep keeps of the matcher's output, up to _MAX_RESULT_BYTES.

    The matching lines alone are kept as they come.  Whole records are kept as
    {"offset": int, "head": bool, "lines": [str, ...], "cut": bool}, for the
    JSON of the result: offset is where the first line given starts in the
    file, and head tells whether that line starts the record with its time
    stamp, or matched within the lines of a record that did not match.
    """

    def __init__(self, whole_records: bool, base_offset: int = 0) -> None:
        self.whole_records = whole_records
        # where in the file the matcher's input starts, which a tail window moves
        self.base_offset = base_offset
        self.lines: list[str] = []
        self.records: list[dict[str, Any]] = []
        # for a tail window, where it starts and the first time stamp in it
        self.window: dict[str, Any] | None = None
        self.size = _RESULT_JSON_BYTES if whole_records else 0
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
            if not self._fits(len(line)):
                return False
            self.lines.append(line)
            return True

        located = _OFFSET_LINE.match(line)
        if located is None:
            # the "--" between lines that are not adjacent
            self._in_record = False
            return True
        text = line[located.end() :].removesuffix("\n")
        starts_record = _RECORD_START.match(text) is not None
        if located.group(2) == ":":
            self.matches += 1
            # a match without a stamp in a kept record only continues it
            if self._in_record and not starts_record:
                self._continuation += 1
                return self._add_to_record(text)
            if not self._fits(_RECORD_JSON_BYTES + len(json.dumps(text))):
                return False
            self.records.append({"offset": self.base_offset + int(located.group(1)), "head": starts_record, "lines": [text], "cut": False})
            self._in_record = True
            self._continuation = 0
            return True
        if not self._in_record or starts_record:
            self._in_record = False
            return True
        if self._continuation >= _MAX_CONTINUATION_LINES:
            self._in_record = False
            self.records[-1]["cut"] = True
            self.cut = True
            return True
        self._continuation += 1
        return self._add_to_record(text)

    def result(self) -> str:
        """The result to store: the lines as they came, or the records as JSON."""
        if not self.whole_records:
            return "".join(self.lines)
        return json.dumps({"records": self.records, "window": self.window})

    def _add_to_record(self, text: str) -> bool:
        if not self._fits(len(json.dumps(text)) + 2):
            return False
        self.records[-1]["lines"].append(text)
        return True

    def _fits(self, size: int) -> bool:
        if self.size + size > _MAX_RESULT_BYTES:
            self.full = True
            return False
        self.size += size
        return True


def _tail_window(log_path: str, tail_bytes: int) -> tuple[int, str | None]:
    """
    Find where the tail window of a file starts, and the first time stamp in it.

    The window starts with the first line that starts within the last
    tail_bytes of the file, so that it holds no line in part.

    Args:
        log_path(str): the file
        tail_bytes(int): how many bytes from the end the window takes at most

    Returns:
        the byte offset where the window starts, and the time stamp of its first
        line that starts with one, or None when none does within
        _WINDOW_STAMP_SCAN_BYTES
    """
    start = max(0, os.path.getsize(log_path) - tail_bytes)
    with open(log_path, "rb") as f:
        if start > 0:
            f.seek(start - 1)
            # unless a line ends right before the window, skip the rest of the line it cuts
            if f.read(1) != b"\n":
                f.readline()
            start = f.tell()
        scanned = 0
        while scanned < _WINDOW_STAMP_SCAN_BYTES:
            line = f.readline(_WINDOW_STAMP_SCAN_BYTES - scanned)
            if not line:
                break
            scanned += len(line)
            stamp = _RECORD_START_BYTES.match(line)
            if stamp is not None:
                return start, stamp.group().decode()
    return start, None


def _run_grep(log_path: str, patterns: list[str], fixed: bool, max_matches: int, tail_bytes: int | None, whole_records: bool) -> tuple[_GrepOutput, int, str]:
    """Run the matcher, optionally over just the tail of the file.

    The pipe is built process to process rather than through a shell: the
    pattern is caller-supplied, so handing it to a shell would hand the caller
    the daemon.

    The output is read as it comes, and the matcher stopped once it no longer
    fits, so the daemon never holds more of it than what is kept.  A matcher so
    stopped had matched, and its return code is given as 0.

    Returns:
        the output kept, the return code of the matcher, and its standard error
    """
    windowed = bool(tail_bytes) and not log_path.endswith(_GZIP_SUFFIX)
    cmd = _bounded_matcher(log_path, patterns, fixed, max_matches, whole_records, from_stdin=windowed)
    output = _GrepOutput(whole_records)
    if windowed and tail_bytes:
        output.base_offset, first_stamp = _tail_window(log_path, tail_bytes)
        output.window = {"offset": output.base_offset, "first_stamp": first_stamp}
    # standard error goes to a file, since a pipe nobody reads until the end
    # could fill and stall the matcher
    with tempfile.TemporaryFile() as stderr_file:
        tail = None
        if windowed:
            # from the start of the window to the end, the end as it is when tail gets there
            tail = subprocess.Popen(
                ["tail", "-c", f"+{output.base_offset + 1}", log_path],
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
        result=output.result(),
        stderr=stderr[:_MAX_RESULT_BYTES],
        return_code=return_code,
        truncated=truncated,
    )


def _is_named(file_name: str, log_names: set[str]) -> bool:
    """Tell whether a file is one of the logs named or rotated from one: the name followed by "-"."""
    return file_name in log_names or any(file_name[:i] in log_names for i, c in enumerate(file_name) if c == "-")


def _handle_log_list(row: dict[str, Any], tb: TaskBufferLike, tmp_logger: LogWrapper, result_machine: str) -> None:
    """List the log files under panda_config.logdir as JSON, and store it under result_machine's result row.

    Every regular file whose name starts with "panda-" is listed, as a grep may
    read no other, with its size and its time of last change in UTC.  With
    log_names, only the files of those logs are.  Files are listed in the order
    of their names until the list no longer fits in _MAX_RESULT_BYTES, which
    sets truncated.
    """
    # the requests submitted before log_names was taken have none
    log_names = json.loads(row["parameters"]).get("log_names")
    named = None if log_names is None else set(log_names)
    try:
        with os.scandir(panda_config.logdir) as entries:
            files: list[dict[str, Any]] = []
            for entry in entries:
                if not entry.name.startswith("panda-") or (named is not None and not _is_named(entry.name, named)) or not entry.is_file():
                    continue
                stat = entry.stat()
                mtime = datetime.datetime.fromtimestamp(stat.st_mtime, datetime.timezone.utc)
                files.append({"name": entry.name, "size": stat.st_size, "mtime": mtime.strftime("%Y-%m-%d %H:%M:%S")})
    except OSError as e:
        tmp_logger.error(f"failed to list {panda_config.logdir}: {e}")
        tb.finish_async_result(row["request_id"], result_machine, "failed", error_msg=str(e), retriable=False)
        return

    files.sort(key=lambda file: file["name"])
    # one entry at a time, so that a list cut short is still valid JSON
    kept: list[str] = []
    size = 2
    for file in files:
        text = json.dumps(file)
        if size + len(text) + 2 > _MAX_RESULT_BYTES:
            break
        kept.append(text)
        size += len(text) + 2
    truncated = len(kept) < len(files)
    shown = "" if log_names is None else f" of {len(log_names)} logs {log_names[:3]!r}"
    tmp_logger.debug(f"listed {len(kept)} of {len(files)} files{shown} in {panda_config.logdir}")
    tb.finish_async_result(
        row["request_id"],
        result_machine,
        "done",
        result=f"[{', '.join(kept)}]",
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
    "log_list": _handle_log_list,
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
    cycle_started_at = time.monotonic()
    _cycle_started_at = cycle_started_at
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
    cycle_seconds = _cycle_seconds(service_name)
    pending = tbuf.get_pending_requests_for_machine(MY_HOSTNAME, service_name, list(HANDLERS.keys()))
    # the requests that may run long after the others, each kind oldest first
    pending = sorted(pending, key=lambda row: row["request_type"] in _LONGEST_RUN_SECONDS)
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
        if not _fits_in_cycle(request_type, cycle_seconds, cycle_started_at):
            tmp_logger.debug(f"left for the next cycle: a {request_type} may run {_LONGEST_RUN_SECONDS[request_type]} s, more than this cycle has left")
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
