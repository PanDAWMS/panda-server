# Standalone script for first testing of error classification rules in the database

import sys

from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandautils.thread_utils import GenericThread

from pandaserver.config import panda_config
from pandaserver.taskbuffer.TaskBuffer import taskBuffer

# instantiate task buffer
requester_id = GenericThread().get_full_id(__name__, sys.modules[__name__].__file__)
taskBuffer.init(panda_config.dbhost, panda_config.dbpasswd, nDBConnection=1, requester=requester_id)

_logger = PandaLogger().getLogger("RetrialModule")


# Taken from Retry code
def safe_match(pattern, message):
    """
    Wrapper around re.match with exception handling.
    """
    matches = False
    try:
        matches = re.match(pattern, message) is not None
    except re.error:
        _logger.error(f"Regex matching failed. Pattern: {pattern}, String: {message}")
    return matches


# Classify error function with regex matching
def classify_error(error_source, error_code, error_diag, rules):
    for rule in rules:
        rule_source, rule_code, rule_diag, rule_class = rule
        # Use safe_match instead of == for pattern matching
        if safe_match(rule_source, error_source) and safe_match(rule_code, error_code) and safe_match(rule_diag, error_diag):
            _logger.info(f"Classified error ({error_source}, {error_code}, {error_diag}) as {rule_class}")
            return rule_class
    _logger.info(f"Error ({error_source}, {error_code}, {error_diag}) classified as Unknown")
    return "Unknown"  # Default if no match found


if __name__ == "__main__":
    try:
        # possibility to specify job as an argument
        job_id = sys.argv[1]
    except IndexError:
        # define some default job ID that we know is in the database
        job_id = 4674371015

    # get the job from the database
    # JobSpec definition: https://github.com/PanDAWMS/panda-server/blob/master/pandaserver/taskbuffer/JobSpec.py
    job_spec = taskBuffer.peekJobs([job_id])[0]
    if not job_spec:
        print(f"Job with ID {job_id} not found")
    else:
        print(f"Got job with ID {job_spec.PandaID} and status {job_spec.jobStatus}")

    # load the error classification rules. This is just
    sql = "SELECT error_source, error_code, error_diag, error_class FROM ATLASPANDA.ERROR_CLASSIFICATION"
    var_map = []
    status, results = taskBuffer.querySQLS(sql, var_map)
    if not results:
        _logger.warning("There are no error classification rules in the database or the query failed")
        return

    classified_errors = []
    for error in results:
        error_source, error_code, error_diag = error
        classification = classify_error(error_source, error_code, error_diag, results)
        classified_errors.append((error_source, error_code, error_diag, classification))
