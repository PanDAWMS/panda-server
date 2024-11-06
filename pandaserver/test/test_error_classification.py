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


#The purpose of this function is to figure out which error code is non-zero
def check_error(job_pilot_error_code, job_ddm_error_code):
    if job_pilot_error_code != 0:
        output = check_database(job_pilot_error_code)
    elif job_ddm_error_code != 0:
        output = check_database(job_ddm_error_code)
    elif (job_pilot_error_code == 0 and job_ddm_error_code ==0):
        output = "Unknown"
        print(f"Error code does not exist in database")
    else:
        output = "Unknown"
    return output

#Check if value exist in the error_classification database
def check_database(output):
    #Getting the information from the error_classification database
    sql = "SELECT error_source, error_code, error_diag, error_class FROM ATLAS_PANDA.ERROR_CLASSIFICATION WHERE error_code=:output"
    var_map={':output': output}
    #Results prints out all oft he rows in the database [class list]
    #Status prints out if everything worked (True) or failed (False)
    status, results = taskBuffer.querySQLS(sql, var_map)
    print(f" Results from SQL: {results}")
    output_results = results[0][1]
    #Note: For some reason the print statement prints out twice --> Fix this error


# Classify error function with regex matching
def classify_error(error_source, error_code, error_diag,error_class, rules):
    for rule in rules:
        rule_source, rule_code, rule_diag, rule_class = rule
        # Use safe_match instead of == for pattern matching
        if (rule_source==error_source) and (rule_code == error_code) and safe_match(rule_diag, error_diag) and (rule_class == error_class):
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
        #job_id = 4674371015
        job_id = 4674371533

    # get the job from the database
    # JobSpec definition: https://github.com/PanDAWMS/panda-server/blob/master/pandaserver/taskbuffer/JobSpec.py
    job_spec = taskBuffer.peekJobs([job_id])[0]
    if not job_spec:
        print(f"Job with ID {job_id} not found")
        #print(f": {} not found")
    else:
        print(f"Got job with ID {job_spec.PandaID} and status {job_spec.jobStatus}")

    #Error code from database
    # If one is zero then the other will be non-zero
    job_pilot_error_code = job_spec.pilotErrorCode # user error
    job_ddm_error_code = job_spec.ddmErrorCode #system error 

    #The purpose of this function is to figure out which error code is non-zero
    error_type = check_error(job_pilot_error_code, job_ddm_error_code)
    #Check if value exist in the error_classification database
    error_in_database = check_database(error_type)

    #for error in results:
    #    print(f"error in for loop: {error}")
    #    error_source, error_code, error_diag , error_class= error
    #    classification = classify_error(error_source, error_code, error_diag,error_class, results)
    #    print(f"Classification: :{classification}")
