# Standalone script for first testing of error classification rules in the database

import sys
import re

from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandautils.thread_utils import GenericThread

from pandaserver.config import panda_config
from pandaserver.taskbuffer.TaskBuffer import taskBuffer
from pandaserver.taskbuffer.JobSpec import JobSpec

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


#List of errors that you can have
error_code_source = [
    "pilotError", "exeError", "supError",
    "ddmError", "brokerageError", "jobDispatcherError",
    "taskBufferError"
]

#Step 1: Using the JobID, we need to figure out which error is non-zero
# Only one error_code will be non-zero [this will be the error source]
# Then we need to get the name of the source, the error number, and the diag
def find_error_source (jobId):
    job_spec = taskBuffer.peekJobs([job_id])[0]
    #Check that job_id is available 
    if not job_spec:
        print(f"Job with ID {job_id} not found")
        #print(f": {} not found")
        sys.exit()
    else:
        print(f"Got job with ID {job_spec.PandaID} and status {job_spec.jobStatus}")
        for source in error_code_source:

            error_code = getattr(job_spec, source+"Code", None)
            error_diag = getattr(job_spec, source+"Diag", None)
            error_source = source
            if error_source != 0:
                print(f"Error code is: {error_code}") #The error code
                print(f"Error diag is: {error_diag}") #The message
                print(f"Error source is: {error_source}") #error name ddmErrorCode
                return error_code, error_diag, error_source
    #If there are zero matches then the code will exit here
    print("Error source does not exist")
    sys.exit(1)
    return None, None, None

#Step2: We need to check if the error source is in the error classification database table and classify the error
def classify_error(err_source, err_code, err_diag):
    sql = "SELECT error_source, error_code, error_diag, error_class FROM ATLAS_PANDA.ERROR_CLASSIFICATION"
    var_map = []
    status, results = taskBuffer.querySQLS(sql, var_map)
    for rule in results:
        rule_source, rule_code, rule_diag, rule_class = rule
        # Use safe_match instead of == for pattern matching
        if (rule_source==err_source) and (rule_code == err_code) and safe_match(rule_diag, err_diag):
            _logger.info(f"Classified error ({err_source}, {err_code}, {err_diag})")
            return rule_class
    _logger.info(f"Error ({err_source}, {err_code}, {err_diag}) classified as Unknown")
    sys.exit()
    return None  # Default if no match found





if __name__ == "__main__":
    try:
        # possibility to specify job as an argument
        job_id = sys.argv[1]
    except IndexError:
        # define some default job ID that we know is in the database
        #job_id = 4674371015
        job_id = 4674371533

        #Find the error source and gettig the code, diag, and source
        err_code, err_diag, err_source = find_error_source(job_id)
        #Printing output of job_id errors

        #print(f"Error code is: {err_code}") #The error code
        #print(f"Error diag is: {err_diag}") #The message
        #print(f"Error source is: {err_source}") #error name ddmErrorCode

        #Classify the error 
        class_error = classify_error(err_source, err_code, err_diag)

        print(f"Classification error: {class_error}")

