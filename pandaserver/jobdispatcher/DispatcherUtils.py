import os
import sys
import requests
from config import panda_config
from pandalogger.PandaLogger import PandaLogger

# logger
_logger = PandaLogger().getLogger('JobDispatcher')

# get secret key
def getSecretKey(pandaID):
    try:
        # get parameters for panda proxy
        proxyURLSSL = panda_config.pandaProxy_URLSSL
        ca_certs  = panda_config.pandaProxy_ca_certs
        key_file  = os.environ['X509_USER_PROXY']
        cert_file = os.environ['X509_USER_PROXY']
        # send request
        data = {'pandaID':pandaID}
        res = requests.post(proxyURLSSL+'/insertSecretKeyForPandaID',
                            data=data,
                            verify=ca_certs,
                            cert=(cert_file,key_file))
        tmpDict = res.json()
        statusCode = tmpDict['errorCode']
        secretKey = tmpDict['secretKey']
        if tmpDict['errorCode'] == 0:
            # succeeded
            return secretKey,''
        else:
            # failed
            return None,tmpDict['errorDiag']
    except:
        errType,errValue = sys.exc_info()[:2]
        return None,"{0}:{1}".format(errType,errValue)

def apply_retrial_rules(task_buffer, jobID, error_source, error_code, attemptNr):
    
    retrial_rules = task_buffer.getRetrialRules()
    
    NO_RETRY = 'noretry'
    INCREASE_MEM = 'increase_memory'
    LIMIT_RETRY = 'limit_retry'
    
    try:
        applicable_rules = retrial_rules[error_source][error_code]
        
        for rule in applicable_rules:
            try:
                action = rule['action']
                parameters = rule['params']
                architecture = rule['architecture'] 
                release = rule['release']
                
                #TODO 1: CHECK ARCHITECTURE AND RELEASE MATCH
                #TODO 2: REMOVE DUPLICATE, E.G. limit_retry = 5 vs limit_retry = 7 for release = X
                #TODO 3: REMOVE INCONSISTENT RULES, E.G. limit_retry = 5 vs limit_retry = 7 
                
                if action == NO_RETRY: 
                    task_buffer.setMaxAttempt(jobID, attemptNr)
                    _logger.debug("setMaxAttempt jobID: %s, maxAttempt: %s" %(jobID, attemptNr))
                     
                elif action == INCREASE_MEM:
                    try:
                        #TODO 1: Check if this has any performance penalty
                        #TODO 2: Complete as in AdderGen (232-240) and delete lines from AdderGen
                        job = task_buffer.peekJobs([jobID], fromDefined=False, fromArchived=False, fromWaiting=False)[0]
                        if not job.minRamCount in [0,None,'NULL']:
                            task_buffer.increaseRamLimitJEDI(job.jediTaskID, job.minRamCount)
                        _logger.debug("increased RAM limit for jobID: %s, jediTaskID: %s" %(jobID, job.jediTaskID))
                    except:
                        errtype,errvalue = sys.exc_info()[:2]
                        _logger.debug("failed to increase RAM limit : %s %s" % (errtype,errvalue))
                    
                elif action == LIMIT_RETRY:
                    try:
                        task_buffer.setMaxAttempt(jobID, int(parameters['maxAttempt']))
                    except (KeyError, ValueError) as e:
                        _logger.debug("Inconsistent definition of limit_retry rule - maxAttempt not defined. parameters: %s" %parameters)
            except KeyError:
                        _logger.debug("Rule was missing some field(s). Rule: %s" %rule)
    except KeyError:
        _logger.debug("No retrial rules to apply for jobID %s, attemptNr %s, failed with %s=%s" %(jobID, attemptNr, error_source, error_code))


