import sys
import time
from pandalogger.PandaLogger import PandaLogger
from config import panda_config
import re
from re import error as ReError
# logger
_logger = PandaLogger().getLogger('RetrialModule')

NO_RETRY = 'no_retry'
INCREASE_MEM = 'increase_memory'
LIMIT_RETRY = 'limit_retry'


def pandalog(message):
    """Function to send message to panda logger. For the moment dummy placeholder.
    https://github.com/PanDAWMS/panda-jedi/blob/master/pandajedi/jediorder/JobGenerator.py#L405
    """
    try:
        #get logger and lock it
        tmpPandaLogger = PandaLogger()
        tmpPandaLogger.lock()
        #set category (usually prod) and type
        tmpPandaLogger.setParams({'Type':'retryModule'})
        tmpLogger = tmpPandaLogger.getHttpLogger(panda_config.loggername)
        #send the message and release the logger
        tmpLogger.debug(message)
        tmpPandaLogger.release()
        _logger.debug("Uploaded message (%s) to pandamon logger."%(message))
    except Exception as e:
        _logger.warning("Could not upload message (%s) to pandamon logger. (Error: %s)"%(message, e))


def timeit(method):
    """Decorator function to time the execution time of any given method. Use as decorator.
    """
    def timed(*args, **kwargs):
        _logger.debug("Entered timed")
        ts = time.time()
        result = method(*args, **kwargs)
        te = time.time()

        _logger.debug('%r (%r, %r) took %.2f sec' %(method.__name__, args, kwargs, te-ts))
        return result

    return timed


def safe_search(pattern, message):
    """Wrapper around re.search with simple exception handling
    """
    found = False
    try:
        found = re.search(pattern, message)
    except ReError:
        _logger.debug("Regexp matching excepted. \nPattern: %s \nString: %s" %(pattern, message))
    finally:
        return found


def conditions_apply(errordiag_job, architecture_job, release_job, wqid_job, errordiag_rule, architecture_rule, release_rule, wqid_rule):
    """Checks that the error regexp, architecture, release and work queue of rule and job match, 
    only in case the attributes are defined for the rule
    """
    _logger.debug("Entered conditions_apply %s"%(locals()))
    if ((errordiag_rule and not safe_search(errordiag_rule, errordiag_job))
        or (architecture_rule and architecture_rule != architecture_job) 
        or (release_rule and release_rule != release_job)
        or (wqid_rule and wqid_rule != wqid_job)):
        _logger.debug("Leaving conditions_apply: False")
        return False
    _logger.debug("Leaving conditions_apply: True")
    return True


def compare_strictness(rule1, rule2):
    """Return 1 if rule1 is stricter, 0 if equal, -1 if rule2 is stricter
    """
    _logger.debug("Entered compare_strictness")
    rule1_weight = 0
    if rule1['architecture']: 
        rule1_weight += 1
    if rule1['release']: 
        rule1_weight += 1
    if rule1['wqid']: 
        rule1_weight += 1

    rule2_weight = 0
    if rule2['architecture']: 
        rule2_weight += 1
    if rule2['release']: 
        rule2_weight += 1
    if rule2['wqid']: 
        rule2_weight += 1

    if rule1 > rule2:
        return 1
    elif rule1 < rule2:
        return -1
    else:
        return 0


def preprocess_rules(rules, error_diag_job, release_job, architecture_job, wqid_job):
    """Do some preliminary validation of the applicable rules.
    - Duplicate rules, (action=limit_retry, maxAttempt=5) vs (action=limit_retry, maxAttempt=7, release=X):
         resolve to the most specific rule, in our example (action=limit_retry, maxAttempt=7, release=X) 
    - Inconsistent rules, e.g. (action=limit_retry, maxAttempt=5) vs (action=limit_retry, maxAttempt=7):
         resolve into the strictest rule, in our example (limit_retry = 5)
    - Bad intended rules, e.g. (action=limit_retry, maxAttempt=5) vs (action=limit_retry, maxAttempt=7, release=X):
    """
    _logger.debug("Entered preprocess_rules")
    filtered_rules = []
    try:
        #See if there is a  NO_RETRY rule. Effect of NO_RETRY rules is the same, so just take the first one that appears
        for rule in rules:
            if (rule['action']!= NO_RETRY or
                not conditions_apply(error_diag_job, architecture_job, release_job, wqid_job, rule['error_diag'], rule['architecture'], rule['release'], rule['wqid'])):
                continue
            else:
                filtered_rules.append(rule)
        
        #See if there is a INCREASE_MEM rule. The effect of INCREASE_MEM rules is the same, so take the first one that appears
        for rule in rules:
            if (rule['action']!= INCREASE_MEM or
                not conditions_apply(error_diag_job, architecture_job, release_job, wqid_job, rule['error_diag'], rule['architecture'], rule['release'], rule['wqid'])): 
                continue
            else:
                filtered_rules.append(rule)
                
        #See if there is a LIMIT_RETRY rule. Take the narrowest rule, in case of draw take the strictest
        limit_retry_rule = {}
        for rule in rules:
            if (not conditions_apply(error_diag_job, architecture_job, release_job, wqid_job, rule['error_diag'], rule['architecture'], rule['release'], rule['wqid']) or
                rule['action']!= LIMIT_RETRY): 
                continue
            elif not limit_retry_rule:
                limit_retry_rule = rule
            else:
                comparison = compare_strictness(rule, limit_retry_rule)
                if comparison == 1:
                    limit_retry_rule = rule
                elif comparison == 0:
                    limit_retry_rule['params']['maxAttempt'] = min(limit_retry_rule['params']['maxAttempt'], rule['params']['maxAttempt'])
                elif comparison == -1:
                    pass
    except KeyError:
        _logger.error("Rules are not properly defined. Rules: %s"%rules)

    if limit_retry_rule:
        filtered_rules.append(limit_retry_rule)
    
    return filtered_rules


#TODO: Add a call to the retrial rules from the UserIF.killJob
@timeit
def apply_retrial_rules(task_buffer, jobID, error_source, error_code, error_diag, attemptNr):
    """Get rules from DB and applies them to a failed job. Actions can be:
    - flag the job so it is not retried again (error code is a final state and retrying will not help)
    - limit the number of retries
    - increase the memory of a job if it failed because of insufficient memory
    """
    _logger.debug("Entered apply_retrial_rules for job %s, error_source %s, error_code %s, error_diag %s, attemptNr %s" 
                  %(jobID, error_source, error_code, error_diag, attemptNr))

    try:
        error_code = int(error_code)
    except ValueError:
        _logger.error("Error code  (%s) can not be casted to int" %(error_code))
        return

    retrial_rules = task_buffer.getRetrialRules()

    _logger.debug("Back from getRetrialRules: %s"%retrial_rules)
    try:
        #TODO: Check if peeking the job again has any performance penalty and if there is any better way
        job = task_buffer.peekJobs([jobID], fromDefined=False, fromArchived=False, fromWaiting=False)[0]
        applicable_rules = preprocess_rules(retrial_rules[error_source][error_code], error_diag, job.AtlasRelease, job.cmtConfig, job.workQueue_ID)
        
        for rule in applicable_rules:
            try:
                
                action = rule['action']
                parameters = rule['params']
                architecture = rule['architecture'] #cmtconfig
                release = rule['release'] #transHome
                wqid = rule['wqid'] #work queue ID
                active = rule['active'] #If False, don't apply rule, only log
                
                _logger.debug("Processing rule %s for jobID %s, error_source %s, error_code %s, attemptNr %s" %(rule, jobID, error_source, error_code, attemptNr))
                
                if not conditions_apply(job.cmtConfig, job.AtlasRelease, job.workQueue_ID, error_diag, architecture, release, wqid):
                    _logger.debug("Skipped rule %s. cmtConfig (%s : %s) or Release (%s : %s) did NOT match" %(rule, architecture, job.cmtConfig, release, job.AtlasRelease))
                    continue
                
                if action == NO_RETRY:
                    if active:
                        task_buffer.setMaxAttempt(jobID, job.Files, attemptNr)
                    #Log to pandamon and logfile
                    message = "setMaxAttempt jobID: %s (task: %s), maxAttempt: %s. Rule/action active: %s" %(jobID, job.jediTaskID, attemptNr, active)
                    pandalog(message)
                    _logger.debug(message)
                
                elif action == LIMIT_RETRY:
                    try:
                        if active:
                            task_buffer.setMaxAttempt(jobID, job.Files, int(parameters['maxAttempt']))
                        #Log to pandamon and logfile
                        message = "setMaxAttempt jobID: %s (task: %s), maxAttempt: %s. Rule/action active: %s" %(jobID, job.jediTaskID, int(parameters['maxAttempt']), active)
                        pandalog(message)
                        _logger.debug(message)
                    except (KeyError, ValueError):
                        _logger.debug("Inconsistent definition of limit_retry rule - maxAttempt not defined. parameters: %s" %parameters)
                
                elif action == INCREASE_MEM:
                    try:
                        #TODO 1: Complete as in AdderGen (232-240) and delete lines from AdderGen
                        if active and not job.minRamCount in [0,None,'NULL']:
                            task_buffer.increaseRamLimitJobJEDI(job, job.minRamCount)
                        #Log to pandamon and logfile
                        message = "increaseRAMLimit for jobID: %s (task: %s)" %(jobID, job.jediTaskID)
                        pandalog(message)
                        _logger.debug(message)
                    except:
                        errtype,errvalue = sys.exc_info()[:2]
                        _logger.debug("Failed to increase RAM limit : %s %s" % (errtype,errvalue))

                _logger.debug("Finished rule %s for jobID %s, error_source %s, error_code %s, attemptNr %s" %(rule, jobID, error_source, error_code, attemptNr))
            
            except KeyError:
                _logger.debug("Rule was missing some field(s). Rule: %s" %rule)
    except KeyError as e:
        _logger.debug("No retrial rules to apply for jobID %s, attemptNr %s, failed with %s=%s. (Exception %e)" %(jobID, attemptNr, error_source, error_code, e))

