import re
import sys
import time
import traceback
from re import error as ReError

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandalogger.PandaLogger import PandaLogger

_logger = PandaLogger().getLogger("RetrialModule")

NO_RETRY = "no_retry"
INCREASE_MEM = "increase_memory"
LIMIT_RETRY = "limit_retry"
INCREASE_CPU = "increase_cputime"
INCREASE_MEM_XTIMES = "increase_memory_xtimes"
REDUCE_INPUT_PER_JOB = "reduce_input_per_job"

SYSTEM_ERROR_CLASS = "system"
NO_ERROR_CLASS = "unknown"


def timeit(method):
    """
    Decorator function to time the execution time of any given method. Use as decorator.
    """

    def timed(*args, **kwargs):
        tmp_log = LogWrapper(_logger, f"timed {method.__name__!r} ({args!r}, {kwargs!r})")
        tmp_log.debug(f"Start")

        ts = time.time()
        result = method(*args, **kwargs)
        te = time.time()

        tmp_log.debug(f"Took {te - ts:.2f} sec")
        return result

    return timed


def safe_match(pattern, message):
    """
    Wrapper around re.search with simple exception handling
    """
    tmp_log = LogWrapper(_logger, f"safe_match")

    matches = False
    try:
        matches = re.match(pattern, message, flags=re.DOTALL)
    except ReError:
        tmp_log.error(f"Regexp matching excepted. \nPattern: {pattern} \nString: {message}")
    finally:
        return matches


def conditions_apply(
    errordiag_job,
    architecture_job,
    release_job,
    wqid_job,
    errordiag_rule,
    architecture_rule,
    release_rule,
    wqid_rule,
):
    """
    Checks that the error regexp, architecture, release and work queue of rule and job match,
    only in case the attributes are defined for the rule
    """
    tmp_log = LogWrapper(_logger, f"conditions_apply")
    tmp_log.debug(f"Start {locals()}")
    if (
        (errordiag_rule and not safe_match(errordiag_rule, errordiag_job))
        or (architecture_rule and architecture_rule != architecture_job)
        or (release_rule and release_rule != release_job)
        or (wqid_rule and wqid_rule != wqid_job)
    ):
        tmp_log.debug("Leaving: False")
        return False

    _logger.debug("Leaving: True")
    return True


def compare_strictness(rule1, rule2):
    """
    Return 1 if rule1 is stricter, 0 if equal, -1 if rule2 is stricter
    """
    tmp_log = LogWrapper(_logger, f"compare_strictness")
    tmp_log.debug("Start")
    rule1_weight = 0
    if rule1["architecture"]:
        rule1_weight += 1
    if rule1["release"]:
        rule1_weight += 1
    if rule1["wqid"]:
        rule1_weight += 1

    rule2_weight = 0
    if rule2["architecture"]:
        rule2_weight += 1
    if rule2["release"]:
        rule2_weight += 1
    if rule2["wqid"]:
        rule2_weight += 1

    if rule1_weight > rule2_weight:
        return 1
    elif rule1_weight < rule2_weight:
        return -1
    else:
        return 0


def preprocess_rules(rules, error_diag_job, release_job, architecture_job, wqid_job):
    """
    Do some preliminary validation of the applicable rules.
    - Duplicate rules, (action=limit_retry, maxAttempt=5) vs (action=limit_retry, maxAttempt=7, release=X):
         resolve to the most specific rule, in our example (action=limit_retry, maxAttempt=7, release=X)
    - Inconsistent rules, e.g. (action=limit_retry, maxAttempt=5) vs (action=limit_retry, maxAttempt=7):
         resolve into the strictest rule, in our example (limit_retry = 5)
    - Bad intended rules, e.g. (action=limit_retry, maxAttempt=5) vs (action=limit_retry, maxAttempt=7, release=X):
    """
    tmp_log = LogWrapper(_logger, f"preprocess_rules")
    tmp_log.debug("Start")
    filtered_rules = []
    limit_retry_rule = {}
    try:
        # See if there is a INCREASE_MEM rule.
        # The effect of INCREASE_MEM rules is the same, so take the first one that appears
        for rule in rules:
            if rule["action"] != INCREASE_MEM or not conditions_apply(
                error_diag_job,
                architecture_job,
                release_job,
                wqid_job,
                rule["error_diag"],
                rule["architecture"],
                rule["release"],
                rule["wqid"],
            ):
                continue
            else:
                filtered_rules.append(rule)
                break

        # See if there is a INCREASE_MEM_XTIMES rule.
        # The effect of INCREASE_MEM_XTIMES rules is the same, so take the first one that appears
        for rule in rules:
            if rule["action"] != INCREASE_MEM_XTIMES or not conditions_apply(
                error_diag_job,
                architecture_job,
                release_job,
                wqid_job,
                rule["error_diag"],
                rule["architecture"],
                rule["release"],
                rule["wqid"],
            ):
                continue
            else:
                filtered_rules.append(rule)
                break

        # See if there is a INCREASE_CPU rule. The effect of INCREASE_CPU rules is the same, so take the first one that appears
        for rule in rules:
            if rule["action"] != INCREASE_CPU or not conditions_apply(
                error_diag_job,
                architecture_job,
                release_job,
                wqid_job,
                rule["error_diag"],
                rule["architecture"],
                rule["release"],
                rule["wqid"],
            ):
                continue
            else:
                filtered_rules.append(rule)
                break

        # See if there is a REDUCE_INPUT_PER_JOB rule.
        for rule in rules:
            if rule["action"] != REDUCE_INPUT_PER_JOB or not conditions_apply(
                error_diag_job,
                architecture_job,
                release_job,
                wqid_job,
                rule["error_diag"],
                rule["architecture"],
                rule["release"],
                rule["wqid"],
            ):
                continue
            else:
                filtered_rules.append(rule)
                break

        # See if there is a LIMIT_RETRY or NO_RETRY rule (they are handled together in this context). Take the narrowest rule, in case of draw take the strictest conditions
        for rule in rules:
            if rule["action"] not in (LIMIT_RETRY, NO_RETRY) or not conditions_apply(
                error_diag_job,
                architecture_job,
                release_job,
                wqid_job,
                rule["error_diag"],
                rule["architecture"],
                rule["release"],
                rule["wqid"],
            ):
                continue
            elif not limit_retry_rule:
                limit_retry_rule = rule
            else:
                comparison = compare_strictness(rule, limit_retry_rule)
                if comparison == 1:
                    limit_retry_rule = rule
                elif comparison == 0:
                    limit_retry_rule["params"]["maxAttempt"] = min(
                        limit_retry_rule["params"]["maxAttempt"],
                        rule["params"]["maxAttempt"],
                    )
                elif comparison == -1:
                    pass
    except KeyError:
        tmp_log.error(f"Rules are not properly defined. Rules: {rules}")

    if limit_retry_rule:
        filtered_rules.append(limit_retry_rule)

    tmp_log.debug("Done")
    return filtered_rules


@timeit
def apply_retrial_rules(task_buffer, job, errors, attemptNr):
    """
    Get rules from DB and applies them to a failed job. Actions can be:
    - flag the job so it is not retried again (error code is a final state and retrying will not help)
    - limit the number of retries
    - increase the memory of a job if it failed because of insufficient memory
    """
    job_id = job.PandaID

    _logger.debug(f"Entered apply_retrial_rules for PandaID={job_id}, errors={errors}, attemptNr={attemptNr}")

    retrial_rules = task_buffer.getRetrialRules()
    _logger.debug("Back from getRetrialRules")
    if not retrial_rules:
        return

    try:
        acted_on_job = False
        for error in errors:
            # in case of multiple errors for a job (e.g. pilot error + exe error) we will only apply one action
            if acted_on_job:
                break

            error_source = error["source"]
            error_code = error["error_code"]
            error_diag = error["error_diag"]

            try:
                error_code = int(error_code)
            except ValueError:
                if error_code != "NULL":
                    _logger.error(f"Error code ({error_code}) can not be casted to int")
                continue
            try:
                rule = retrial_rules[error_source][error_code]
            except KeyError as e:
                _logger.debug(f"Retry rule does not apply for jobID {job_id}, attemptNr {attemptNr}, failed with {errors}. (Exception {e})")
                continue

            applicable_rules = preprocess_rules(rule, error_diag, job.AtlasRelease, job.cmtConfig, job.workQueue_ID)
            _logger.debug(f"Applicable rules for PandaID={job_id}: {applicable_rules}")
            for rule in applicable_rules:
                try:
                    error_id = rule["error_id"]
                    error_diag_rule = rule["error_diag"]
                    action = rule["action"]
                    parameters = rule["params"]
                    architecture = rule["architecture"]  # cmtconfig
                    release = rule["release"]  # transHome
                    wqid = rule["wqid"]  # work queue ID
                    active = rule["active"]  # If False, don't apply rule, only log

                    _logger.debug(
                        "error_diag_rule {0}, action {1}, parameters {2}, architecture {3}, release {4}, wqid {5}, active {6}".format(
                            error_diag_rule,
                            action,
                            parameters,
                            architecture,
                            release,
                            wqid,
                            active,
                        )
                    )

                    _logger.debug(f"Processing rule {rule} for jobID {job_id}, error_source {error_source}, error_code {error_code}, attemptNr {attemptNr}")
                    if not conditions_apply(
                        error_diag,
                        job.cmtConfig,
                        job.AtlasRelease,
                        job.workQueue_ID,
                        error_diag_rule,
                        architecture,
                        release,
                        wqid,
                    ):
                        _logger.debug(
                            f"Skipped rule {rule}. cmtConfig ({architecture} : {job.cmtConfig}) or Release ({release} : {job.AtlasRelease}) did NOT match"
                        )
                        continue

                    if action == NO_RETRY:
                        if active:
                            task_buffer.setNoRetry(job_id, job.jediTaskID, job.Files)
                        message = (
                            f"action=setNoRetry for PandaID={job_id} jediTaskID={job.jediTaskID} prodSourceLabel={job.prodSourceLabel} "
                            f"( ErrorSource={error_source} ErrorCode={error_code} ErrorDiag: {error_diag_rule}. "
                            f"Error/action active={active} error_id={error_id} )"
                        )
                        acted_on_job = True
                        _logger.info(message)

                    elif action == LIMIT_RETRY:
                        try:
                            if active:
                                task_buffer.setMaxAttempt(
                                    job_id,
                                    job.jediTaskID,
                                    job.Files,
                                    int(parameters["maxAttempt"]),
                                )
                            message = (
                                f"action=setMaxAttempt for PandaID={job_id} jediTaskID={job.jediTaskID} prodSourceLabel={job.prodSourceLabel} maxAttempt={int(parameters['maxAttempt'])} "
                                f"( ErrorSource={error_source} ErrorCode={error_code} ErrorDiag: {error_diag_rule}. "
                                f"Error/action active={active} error_id={error_id} )"
                            )
                            acted_on_job = True
                            _logger.info(message)
                        except (KeyError, ValueError):
                            _logger.error(f"Inconsistent definition of limit_retry rule - maxAttempt not defined. parameters: {parameters}")

                    elif action == INCREASE_MEM:
                        try:
                            if active:
                                task_buffer.increaseRamLimitJobJEDI(job, job.minRamCount, job.jediTaskID)
                            message = (
                                f"action=increaseRAMLimit for PandaID={job_id} jediTaskID={job.jediTaskID} prodSourceLabel={job.prodSourceLabel} "
                                f"( ErrorSource={error_source} ErrorCode={error_code} ErrorDiag: {error_diag_rule}. "
                                f"Error/action active={active} error_id={error_id} )"
                            )
                            acted_on_job = True
                            _logger.info(message)
                        except Exception:
                            error_type, error_value = sys.exc_info()[:2]
                            _logger.error(f"Failed to increase RAM limit : {error_type} {error_value}")

                    elif action == INCREASE_MEM_XTIMES:
                        try:
                            if active:
                                task_buffer.increaseRamLimitJobJEDI_xtimes(job, job.minRamCount, job.jediTaskID, attemptNr)
                            message = (
                                f"action=increaseRAMLimit_xtimes for PandaID={job_id} jediTaskID={job.jediTaskID} prodSourceLabel={job.prodSourceLabel} "
                                f"( ErrorSource={error_source} ErrorCode={error_code} ErrorDiag: {error_diag_rule}. "
                                f"Error/action active={active} error_id={error_id} )"
                            )
                            acted_on_job = True
                            _logger.info(message)
                        except Exception:
                            error_type, error_value = sys.exc_info()[:2]
                            _logger.error(f"Failed to increase RAM xtimes limit : {error_type} {error_value}")

                    elif action == INCREASE_CPU:
                        new_cpu_time = None
                        try:
                            # update the task CPU time based on the failed job
                            if active:
                                new_cpu_time, new_cpu_time_unit = task_buffer.initialize_cpu_time_task(
                                    job_id, job.jediTaskID, job.computingSite, job.Files, active
                                )

                            message = (
                                f"action=increaseCpuTime triggered CPU time increase based on failed job PandaID={job_id} "
                                f"jediTaskID={job.jediTaskID} prodSourceLabel={job.prodSourceLabel} (active={active} ), new_cpu_time={new_cpu_time} new_cpu_time_unit={new_cpu_time_unit}. "
                                f"( ErrorSource={error_source} ErrorCode={error_code} ErrorDiag: {error_diag_rule}. "
                                f"Error/action active={active} error_id={error_id} )"
                            )
                            _logger.info(message)
                        except Exception:
                            error_type, error_value = sys.exc_info()[:2]
                            _logger.error(f"Failed to increase CPU-Time based on failed jobs: {error_type} {error_value}")

                        # if the CPU time was not increased based on one failed job, we request recalculation of task parameters
                        if not new_cpu_time:
                            try:
                                # request recalculation of task parameters and see if it applied
                                applied = False

                                if active:
                                    rowcount = task_buffer.requestTaskParameterRecalculation(job.jediTaskID)
                                else:
                                    rowcount = 0

                                if rowcount:
                                    applied = True

                                message = (
                                    f"action=increaseCpuTime requested recalculation of task parameters for PandaID={job_id} "
                                    f"jediTaskID={job.jediTaskID} prodSourceLabel={job.prodSourceLabel} (active={active} ), applied={applied}. "
                                    f"( ErrorSource={error_source} ErrorCode={error_code} ErrorDiag: {error_diag_rule}. "
                                    f"Error/action active={active} error_id={error_id} )"
                                )

                                acted_on_job = True
                                _logger.info(message)
                            except Exception:
                                error_type, error_value = sys.exc_info()[:2]
                                _logger.error(f"Failed to increase CPU-Time : {error_type} {error_value}")

                    elif action == REDUCE_INPUT_PER_JOB:
                        try:
                            applied = False
                            if active:
                                applied = task_buffer.reduce_input_per_job(
                                    job.PandaID, job.jediTaskID, job.attemptNr, parameters.get("excluded_rules"), parameters.get("steps")
                                )
                            message = (
                                f"action=reduceInputPerJob for PandaID={job_id} jediTaskID={job.jediTaskID} prodSourceLabel={job.prodSourceLabel} applied={applied} "
                                f"( ErrorSource={error_source} ErrorCode={error_code} ErrorDiag: {error_code}. "
                                f"Error/action active={active} error_id={error_id} )"
                            )
                            acted_on_job = True
                            _logger.info(message)
                        except Exception as e:
                            _logger.error(f"Failed to reduce input per job : {e} {traceback.format_exc()}")
                            _logger.error(traceback.format_exc())

                    _logger.debug(f"Finished rule {rule} for PandaID={job_id} error_source={error_source} error_code={error_code} attemptNr={attemptNr}")

                except KeyError:
                    _logger.error(f"Rule was missing some field(s). Rule: {rule}")

    except KeyError as e:
        _logger.debug(f"No retrial rules to apply for jobID {job_id}, attemptNr {attemptNr}, failed with {errors}. (Exception {e})")


def get_job_error_details(job_spec):
    # Possible error types
    error_sources = ["pilotError", "exeError", "supError", "ddmError", "brokerageError", "jobDispatcherError", "taskBufferError"]
    job_id = job_spec.PandaID
    job_errors = []

    tmp_log = LogWrapper(_logger, f"get_job_error_details PandaID={job_id}")

    tmp_log.debug(f"Starting for status {job_spec.jobStatus}")

    # Get the error codes and messages that are set for the job
    for source in error_sources:
        error_code = getattr(job_spec, source + "Code", None)  # 1099
        error_diag = getattr(job_spec, source + "Diag", None)  # "Test error message"
        error_source = source + "Code"  # pilotErrorCode
        if error_code:
            job_errors.append((error_code, error_diag, error_source))

    if job_errors:
        tmp_log.debug(f"Job has following error codes: {job_errors}")
    else:
        tmp_log.debug("Job has no error codes")

    return job_errors


def classify_error(task_buffer, job_id, job_errors):
    # Get the confirmed error classification rules
    tmp_log = LogWrapper(_logger, f"classify_error PandaID={job_id}")

    # Query the error classification rules from the database
    sql = "SELECT id, error_source, error_code, error_diag, error_class, active FROM ATLAS_PANDA.ERROR_CLASSIFICATION"
    var_map = []
    status, rules = task_buffer.querySQLS(sql, var_map)
    if not rules:
        tmp_log.debug(f"No error classification rules defined in the database")
        return None

    # Iterate job errors and rules to find a match
    for job_error in job_errors:
        err_code, err_diag, err_source = job_error
        for rule in rules:
            rule_id, rule_source, rule_code, rule_diag, rule_class, rule_active = rule

            if (
                (rule_source and err_source is not None and rule_source == err_source)
                and (rule_code and err_code is not None and rule_code == err_code)
                and (rule_diag and err_diag is not None and safe_match(rule_diag, err_diag))
            ):
                active = rule_active == "Y"
                tmp_log.debug(f"Job classified with rule {rule_id}: ({err_source}, {err_code}, {err_diag}) as {rule_class} (active: {active})")
                return rule_id, rule_source, rule_code, rule_diag, rule_class, active

    tmp_log.debug(f"No matching rule found")
    return None


@timeit
def apply_error_classification_logic(task_buffer, job):
    tmp_log = LogWrapper(_logger, f"apply_error_classification_logic")

    # Find the error source and getting the code, diag, and source
    job_errors = get_job_error_details(job)

    # Classify the error
    ret = classify_error(task_buffer, job.PandaID, job_errors)
    if not ret:
        return

    # Unpack the classification
    rule_id, rule_source, rule_code, rule_diag, rule_class, active = ret

    # System errors should not count towards the user's max attempt. We increase the max attempt, since we can't repeat attempt numbers
    if rule_class == SYSTEM_ERROR_CLASS:
        # Structure the message for logstash parsing and monitoring.
        # We are using a large offset in the rule IDs in the database to avoid overlapping IDs with the retry module
        message = (
            f"action=increase_max_failure for PandaID={job.PandaID} jediTaskID={job.jediTaskID} prodSourceLabel={job.prodSourceLabel} "
            f"( ErrorSource={rule_source} ErrorCode={rule_code} ErrorDiag: {rule_diag}. "
            f"Error/action active={active} error_id={rule_id} )"
        )
        tmp_log.info(message)

        # Apply the rule only for active errors
        if active:
            task_buffer.increase_max_failure(job.PandaID, job.jediTaskID, job.Files)


def job_failure_postprocessing(task_buffer, job_id, errors, attempt_number):
    """
    Entry point for job failure post-processing. This includes applying the retry rules and error classification logic.
    """
    # get the job spec from the ID
    job = task_buffer.peekJobs([job_id], fromDefined=False, fromArchived=True, fromWaiting=False)[0]
    if not job:
        return

    # Run the retry module on the job
    apply_retrial_rules(task_buffer, job, errors, attempt_number)

    # Apply any logic related to error classification
    apply_error_classification_logic(task_buffer, job)
