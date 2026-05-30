"""
Shared async request processing logic.
Each service wraps this with a thin entrypoint that passes its service_name.
New request types: add a handler function and register it in HANDLERS.
"""

import json
import os
import socket
import subprocess

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandalogger.PandaLogger import PandaLogger

from pandaserver.config import panda_config

_logger = PandaLogger().getLogger("async_request_processor")

MY_HOSTNAME = socket.getfqdn()

# max subprocess timeout in seconds
_SUBPROCESS_TIMEOUT = 240

# stale result threshold: must exceed the longest handler subprocess timeout
_STALE_THRESHOLD_SECONDS = _SUBPROCESS_TIMEOUT * 2

# max result size stored in DB (bytes)
_MAX_RESULT_BYTES = 1_000_000


def _handle_grep(row, tb, tmp_logger):
    """Run rg or zgrep on a log file and store the output."""
    params = json.loads(row["parameters"])
    log_filename = params["log_filename"]
    pattern = params["pattern"]
    log_path = os.path.join(panda_config.logdir, log_filename)

    if log_path.endswith(".gz"):
        cmd = ["zgrep", pattern, log_path]
    else:
        cmd = ["rg", pattern, log_path]

    tmp_logger.debug(f"command: {' '.join(cmd)}")
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=_SUBPROCESS_TIMEOUT)
    except subprocess.TimeoutExpired:
        tmp_logger.error(f"subprocess timed out after {_SUBPROCESS_TIMEOUT} seconds")
        tb.finish_async_result(
            row["request_id"],
            MY_HOSTNAME,
            "failed",
            error_msg="timeout",
            retriable=False,
        )
        return
    except Exception as e:
        tmp_logger.error(f"subprocess failed with exception: {e}")
        tb.finish_async_result(
            row["request_id"],
            MY_HOSTNAME,
            "failed",
            error_msg=str(e),
            retriable=False,
        )
        return

    stdout = proc.stdout
    stderr = proc.stderr
    truncated = len(stdout) > _MAX_RESULT_BYTES or len(stderr) > _MAX_RESULT_BYTES
    tmp_logger.debug(f"subprocess finished with return code {proc.returncode}, stdout size {len(stdout)}, stderr size {len(stderr)}, truncated={truncated}")
    tb.finish_async_result(
        row["request_id"],
        MY_HOSTNAME,
        "done",
        result=stdout[:_MAX_RESULT_BYTES],
        stderr=stderr[:_MAX_RESULT_BYTES],
        return_code=proc.returncode,
        truncated=truncated,
    )


# Register new request types here — no new daemon needed
HANDLERS = {
    "grep": _handle_grep,
}


def run(service_name, tbuf=None):
    """
    Process one daemon cycle for the given service.
    Call this from the service-specific entrypoint (daemon script or WatchDog).
    tbuf: an already-initialised TaskBuffer, or None to use the module-level singleton.
    """
    _logger.debug(f"stat for service {service_name}")
    if tbuf is None:
        from pandaserver.taskbuffer.TaskBuffer import taskBuffer as tbuf

    # keep this machine's liveness record current
    tbuf.upsert_machine_heartbeat(MY_HOSTNAME, service_name)

    # recover stale running rows from previous crashed cycles
    tbuf.recover_stale_results(MY_HOSTNAME, max_processing_seconds=_STALE_THRESHOLD_SECONDS)

    # find requests this machine should process
    pending = tbuf.get_pending_requests_for_machine(MY_HOSTNAME, service_name, list(HANDLERS.keys()))
    for row in pending:
        request_id = row["request_id"]
        request_type = row["request_type"]
        tmp_logger = LogWrapper(_logger, prefix=f"< request_id={request_id} >")
        handler = HANDLERS.get(request_type)
        if handler is None:

            tmp_logger.warning(f"unknown request_type={request_type}")
            continue
        if not tbuf.claim_async_result(request_id, MY_HOSTNAME):
            # another daemon instance on the same machine claimed it first
            continue
        tmp_logger.debug(f"processing request_id={request_id} type={request_type}")
        handler(row, tbuf, tmp_logger)
    _logger.debug("done")
