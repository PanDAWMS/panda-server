import re
import sys
import time
import traceback
from re import error as ReError

from pandacommon.pandalogger.PandaLogger import PandaLogger

# logger
_logger = PandaLogger().getLogger("RetrialModule")

NO_RETRY = "no_retry"
INCREASE_MEM = "increase_memory"
LIMIT_RETRY = "limit_retry"
INCREASE_CPU = "increase_cputime"
INCREASE_MEM_XTIMES = "increase_memory_xtimes"
REDUCE_INPUT_PER_JOB = "reduce_input_per_job"


def timeit(method):
    """
    Decorator function to time the execution time of any given method. Use as decorator.
    """

    def timed(*args, **kwargs):
        _logger.debug("Entered timed")
        ts = time.time()
        result = method(*args, **kwargs)
        te = time.time()

        _logger.debug(f"{method.__name__!r} ({args!r}, {kwargs!r}) took {te - ts:.2f} sec")
        return result

    return timed


def safe_match(pattern, message):
    """
    Wrapper around re.search with simple exception handling
    """
    matches = False
    try:
        matches = re.match(pattern, message)
    except ReError:
        _logger.error(f"Regexp matching excepted. \nPattern: {pattern} \nString: {message}")
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
    _logger.debug(f"Entered conditions_apply {locals()}")
    if (
        (errordiag_rule and not safe_match(errordiag_rule, errordiag_job))
        or (architecture_rule and architecture_rule != architecture_job)
        or (release_rule and release_rule != release_job)
        or (wqid_rule and wqid_rule != wqid_job)
    ):
        _logger.debug("Leaving conditions_apply: False")
        return False
    _logger.debug("Leaving conditions_apply: True")
    return True


def compare_strictness(rule1, rule2):
    """
    Return 1 if rule1 is stricter, 0 if equal, -1 if rule2 is stricter
    """
    _logger.debug("Entered compare_strictness")
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

    if rule1 > rule2:
        return 1
    elif rule1 < rule2:
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
    _logger.debug("Entered preprocess_rules")
    filtered_rules = []
    try:
        # See if there is a  NO_RETRY rule.
        # Effect of NO_RETRY rules is the same, so just take the first one that appears
        for rule in rules:
            if rule["action"] != NO_RETRY or not conditions_apply(
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

        # See if there is a LIMIT_RETRY rule. Take the narrowest rule, in case of draw take the strictest conditions
        limit_retry_rule = {}
        for rule in rules:
            if rule["action"] != LIMIT_RETRY or not conditions_apply(
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
        _logger.error(f"Rules are not properly defined. Rules: {rules}")

    if limit_retry_rule:
        filtered_rules.append(limit_retry_rule)

    return filtered_rules


@timeit
def apply_retrial_rules(task_buffer, jobID, errors, attemptNr):
    """
    Get rules from DB and applies them to a failed job. Actions can be:
    - flag the job so it is not retried again (error code is a final state and retrying will not help)
    - limit the number of retries
    - increase the memory of a job if it failed because of insufficient memory
    """
    _logger.debug(f"Entered apply_retrial_rules for PandaID={jobID}, errors={errors}, attemptNr={attemptNr}")

    retrial_rules = task_buffer.getRetrialRules()
    _logger.debug("Back from getRetrialRules")
    if not retrial_rules:
        return

    try:
        job = task_buffer.peekJobs([jobID], fromDefined=False, fromArchived=True, fromWaiting=False)[0]
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
                _logger.debug(f"Retry rule does not apply for jobID {jobID}, attemptNr {attemptNr}, failed with {errors}. (Exception {e})")
                continue

            applicable_rules = preprocess_rules(rule, error_diag, job.AtlasRelease, job.cmtConfig, job.workQueue_ID)
            _logger.debug(f"Applicable rules for PandaID={jobID}: {applicable_rules}")
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

                    _logger.debug(f"Processing rule {rule} for jobID {jobID}, error_source {error_source}, error_code {error_code}, attemptNr {attemptNr}")
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
                            task_buffer.setNoRetry(jobID, job.jediTaskID, job.Files)
                        # Log to pandamon and logfile
                        message = (
                            f"action=setNoRetry for PandaID={jobID} jediTaskID={job.jediTaskID} prodSourceLabel={job.prodSourceLabel} "
                            f"( ErrorSource={error_source} ErrorCode={error_code} ErrorDiag: {error_diag_rule}. "
                            f"Error/action active={active} error_id={error_id} )"
                        )
                        acted_on_job = True
                        _logger.info(message)

                    elif action == LIMIT_RETRY:
                        try:
                            if active:
                                task_buffer.setMaxAttempt(
                                    jobID,
                                    job.jediTaskID,
                                    job.Files,
                                    int(parameters["maxAttempt"]),
                                )
                            # Log to pandamon and logfile
                            message = (
                                f"action=setMaxAttempt for PandaID={jobID} jediTaskID={job.jediTaskID} prodSourceLabel={job.prodSourceLabel} maxAttempt={int(parameters['maxAttempt'])} "
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
                            # Log to pandamon and logfile
                            message = (
                                f"action=increaseRAMLimit for PandaID={jobID} jediTaskID={job.jediTaskID} prodSourceLabel={job.prodSourceLabel} "
                                f"( ErrorSource={error_source} ErrorCode={error_code} ErrorDiag: {error_diag_rule}. "
                                f"Error/action active={active} error_id={error_id} )"
                            )
                            acted_on_job = True
                            _logger.info(message)
                        except Exception:
                            errtype, errvalue = sys.exc_info()[:2]
                            _logger.error(f"Failed to increase RAM limit : {errtype} {errvalue}")

                    elif action == INCREASE_MEM_XTIMES:
                        try:
                            if active:
                                task_buffer.increaseRamLimitJobJEDI_xtimes(job, job.minRamCount, job.jediTaskID, attemptNr)
                            # Log to pandamon and logfile
                            message = (
                                f"action=increaseRAMLimit_xtimes for PandaID={jobID} jediTaskID={job.jediTaskID} prodSourceLabel={job.prodSourceLabel} "
                                f"( ErrorSource={error_source} ErrorCode={error_code} ErrorDiag: {error_diag_rule}. "
                                f"Error/action active={active} error_id={error_id} )"
                            )
                            acted_on_job = True
                            _logger.info(message)
                        except Exception:
                            errtype, errvalue = sys.exc_info()[:2]
                            _logger.error(f"Failed to increase RAM xtimes limit : {errtype} {errvalue}")

                    elif action == INCREASE_CPU:
                        try:
                            # request recalculation of task parameters and see if it applied
                            applied = False

                            if active:
                                rowcount = task_buffer.requestTaskParameterRecalculation(job.jediTaskID)
                            else:
                                rowcount = 0

                            if rowcount:
                                applied = True

                            # Log to pandamon and logfile
                            message = (
                                f"action=increaseCpuTime requested recalculation of task parameters for PandaID={jobID} "
                                f"jediTaskID={job.jediTaskID} prodSourceLabel={job.prodSourceLabel} (active={active} ), applied={applied}. "
                                f"( ErrorSource={error_source} ErrorCode={error_code} ErrorDiag: {error_diag_rule}. "
                                f"Error/action active={active} error_id={error_id} )"
                            )
                            acted_on_job = True
                            _logger.info(message)
                        except Exception:
                            errtype, errvalue = sys.exc_info()[:2]
                            _logger.error(f"Failed to increase CPU-Time : {errtype} {errvalue}")

                    elif action == REDUCE_INPUT_PER_JOB:
                        try:
                            applied = False
                            if active:
                                applied = task_buffer.reduce_input_per_job(
                                    job.PandaID, job.jediTaskID, job.attemptNr, parameters.get("excluded_rules"), parameters.get("steps")
                                )
                            # Log to pandamon and logfile
                            message = (
                                f"action=reduceInputPerJob for PandaID={jobID} jediTaskID={job.jediTaskID} prodSourceLabel={job.prodSourceLabel} applied={applied} "
                                f"( ErrorSource={error_source} ErrorCode={error_code} ErrorDiag: {error_code}. "
                                f"Error/action active={active} error_id={error_id} )"
                            )
                            acted_on_job = True
                            _logger.info(message)
                        except Exception as e:
                            _logger.error(f"Failed to reduce input per job : {e} {traceback.format_exc()}")

                    _logger.debug(f"Finished rule {rule} for PandaID={jobID} error_source={error_source} error_code={error_code} attemptNr={attemptNr}")

                except KeyError:
                    _logger.error(f"Rule was missing some field(s). Rule: {rule}")

    except KeyError as e:
        _logger.debug(f"No retrial rules to apply for jobID {jobID}, attemptNr {attemptNr}, failed with {errors}. (Exception {e})")
