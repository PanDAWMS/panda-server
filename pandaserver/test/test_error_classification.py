# Standalone script for first testing of error classification rules in the database

import re
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
        print(f"Regex matching failed. Pattern: {pattern}, String: {message}")
    return matches


# List of errors that you can have
error_code_source = ["pilotError", "exeError", "supError", "ddmError", "brokerageError", "jobDispatcherError", "taskBufferError"]


# Step 1: Using the JobID, we need to figure out which error is non-zero
# Only one error_code will be non-zero [this will be the error source]
# Then we need to get the name of the source, the error number, and the diag
def find_error_source(job_id):
    job_spec = taskBuffer.peekJobs([job_id])[0]
    job_errors = []
    # Check that job_id is available
    if not job_spec:
        print(f"Job with ID {job_id} not found")
        return
    else:
        print(f"Got job with ID {job_spec.PandaID} and status {job_spec.jobStatus}")
        for source in error_code_source:
            error_code = getattr(job_spec, source + "Code", None)  # 1099
            error_diag = getattr(job_spec, source + "Diag", None)  # "Test error message"
            error_source = source + "Code"  # pilotErrorCode
            if error_code:
                print(f"-------")  # error name ddmErrorCode
                print(f"Error source: {error_source}")  # error name ddmErrorCode
                print(f"Error code: {error_code}")  # The error code
                print(f"Error diag: {error_diag}")  # The message
                job_errors.append((error_code, error_diag, error_source))

    if job_errors:
        print(f"Job has following error codes: {job_errors}")
    else:
        print("Job has no error codes")

    return job_errors


# Step2: We need to check if the error source is in the error classification database table and classify the error
def classify_error(job_errors):

    # Get the error classification rules
    sql = "SELECT error_source, error_code, error_diag, error_class FROM ATLAS_PANDA.ERROR_CLASSIFICATION"
    var_map = []
    status, rules = taskBuffer.querySQLS(sql, var_map)

    # Iterate job errors and rules to find a match
    for job_error in job_errors:
        err_code, err_diag, err_source = job_error
        for rule in rules:
            rule_source, rule_code, rule_diag, rule_class = rule

            if (
                (rule_source and err_source is not None and rule_source == err_source)
                and (rule_code and err_code is not None and rule_code == err_code)
                and (rule_diag and err_diag is not None and safe_match(rule_diag, err_diag))
            ):
                print(f"The job was classified with error ({err_source}, {err_code}, {err_diag}) as {rule_class}")
                return rule_class

    print(f"The job with {job_errors} did not match any rule and could not be classified")
    return "Unknown"  # Default if no match found


if __name__ == "__main__":
    try:
        # possibility to specify job as an argument
        job_id = sys.argv[1]
    except IndexError:
        # define some default job ID that we know is in the database
        # job_id = 4674371015
        # job_id = 4674371533
        job_id = 4674371533

    print("---------------------------------------")

    # Find the error source and getting the code, diag, and source
    job_errors = find_error_source(job_id)

    print("---------------------------------------")

    # Classify the error
    class_error = classify_error(job_errors)

    print("---------------------------------------")

    print(f"Classification error: {class_error}")
