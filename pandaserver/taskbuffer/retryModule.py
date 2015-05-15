import sys
from pandalogger.PandaLogger import PandaLogger

# logger
_logger = PandaLogger().getLogger('Scrooge')

NO_RETRY = 'noretry'
INCREASE_MEM = 'increase_memory'
LIMIT_RETRY = 'limit_retry'


def conditions_apply(architecture_job, release_job, architecture_rule, release_rule):
    if (architecture_rule and architecture_rule != architecture_job) or (release_rule and release_rule != release_job):
        return False
    return True


def compare_strictness(rule1, rule2):
    """Return 1 if rule1 is stricter, 0 if equal, -1 if rule2 is stricter
    """
    rule1_weight = 0
    if rule1['architecture']: 
        rule1_weight += 1
    if rule1['release']: 
        rule1_weight += 1

    rule2_weight = 0
    if rule2['architecture']: 
        rule2_weight += 1
    if rule2['release']: 
        rule2_weight += 1

    if rule1 > rule2:
        return 1
    elif rule1 < rule2:
        return -1
    else:
        return 0


def preprocess_rules(rules, release_job, architecture_job):
    """Do some preliminary validation of the applicable rules.
    - Duplicate rules, (action=limit_retry, maxAttempt=5) vs (action=limit_retry, maxAttempt=7, release=X):
         resolve to the most specific rule, in our example (action=limit_retry, maxAttempt=7, release=X) 
    - Inconsistent rules, e.g. (action=limit_retry, maxAttempt=5) vs (action=limit_retry, maxAttempt=7):
         resolve into the strictest rule, in our example (limit_retry = 5)
    - Bad intended rules, e.g. (action=limit_retry, maxAttempt=5) vs (action=limit_retry, maxAttempt=7, release=X):
    """
    filtered_rules = []
    try:
        #See if there is a  NO_RETRY rule. Effect of NO_RETRY rules is the same, so just take the first one that appears
        for rule in rules:
            if (not conditions_apply(architecture_job, release_job, rule['architecture'], rule['release']) or
                rule['action']!= NO_RETRY): 
                continue
            else:
                filtered_rules.append(rule)
        
        #See if there is a INCREASE_MEM rule. The effect of INCREASE_MEM rules is the same, so take the first one that appears
        for rule in rules:
            if (not conditions_apply(architecture_job, release_job, rule['architecture'], rule['release']) or
                rule['action']!= INCREASE_MEM): 
                continue
            else:
                filtered_rules.append(rule)
                
        #See if there is a LIMIT_RETRY rule. Take the narrowest rule, in case of draw take the strictest
        limit_retry_rule = {}
        for rule in rules:
            if (not conditions_apply(architecture_job, release_job, rule['architecture'], rule['release']) or
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
def apply_retrial_rules(task_buffer, jobID, error_source, error_code, attemptNr):
    """Get rules from DB and applies them to a failed job. Actions can be:
    - flag the job so it is not retried again (error code is a final state and retrying will not help)
    - limit the number of retries
    - increase the memory of a job if it failed because of insufficient memory
    """

    retrial_rules = task_buffer.getRetrialRules()

    try:
        #TODO: Check if peeking the job again has any performance penalty and if there is any better way
        job = task_buffer.peekJobs([jobID], fromDefined=False, fromArchived=False, fromWaiting=False)[0]
        applicable_rules = preprocess_rules(retrial_rules[error_source][error_code], job.AtlasRelease, job.cmtConfig)
        
        for rule in applicable_rules:
            try:
                action = rule['action']
                parameters = rule['params']
                architecture = rule['architecture'] #cmtconfig
                release = rule['release'] #transHome
                
                _logger.debug("Processing rule %s for jobID %s, error_source %s, error_code %s, attemptNr %s" %(rule, jobID, error_source, error_code, attemptNr))
                
                if conditions_apply(job.cmtConfig, job.AtlasRelease, architecture, release):
                    _logger.debug("Skipped rule %s. cmtConfig (%s : %s) or Release (%s : %s) did NOT match" %(rule, architecture, job.cmtConfig, release, job.AtlasRelease))
                    continue
                
                if action == NO_RETRY: 
                    task_buffer.setMaxAttempt(jobID, job.Files, attemptNr)
                    _logger.debug("setMaxAttempt jobID: %s, maxAttempt: %s" %(jobID, attemptNr))
                
                elif action == LIMIT_RETRY:
                    try:
                        task_buffer.setMaxAttempt(jobID, job.Files, int(parameters['maxAttempt']))
                    except (KeyError, ValueError) as e:
                        _logger.debug("Inconsistent definition of limit_retry rule - maxAttempt not defined. parameters: %s" %parameters)
                
                elif action == INCREASE_MEM:
                    try:
                        #TODO 1: Complete as in AdderGen (232-240) and delete lines from AdderGen
                        if not job.minRamCount in [0,None,'NULL']:
                            task_buffer.increaseRamLimitJEDI(job.jediTaskID, job.minRamCount)
                        _logger.debug("Increased RAM limit for jobID: %s, jediTaskID: %s" %(jobID, job.jediTaskID))
                    except:
                        errtype,errvalue = sys.exc_info()[:2]
                        _logger.debug("Failed to increase RAM limit : %s %s" % (errtype,errvalue))

                _logger.debug("Finished rule %s for jobID %s, error_source %s, error_code %s, attemptNr %s" %(rule, jobID, error_source, error_code, attemptNr))
            
            except KeyError:
                        _logger.debug("Rule was missing some field(s). Rule: %s" %rule)
    except KeyError:
        _logger.debug("No retrial rules to apply for jobID %s, attemptNr %s, failed with %s=%s" %(jobID, attemptNr, error_source, error_code))