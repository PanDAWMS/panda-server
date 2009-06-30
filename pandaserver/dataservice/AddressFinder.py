import re
import commands

from taskbuffer.OraDBProxy import DBProxy
from pandalogger.PandaLogger import PandaLogger

# logger
_logger = PandaLogger().getLogger('AddressFinder')

# get email address using phonebook
def getEmailPhonebook(dn):
    _logger.debug('Getting email for %s' % dn)
    # extract First Last from DN
    dbProxy = DBProxy()
    extractedDN = dbProxy.cleanUserID(dn)
    # replace -.
    extractedDN = re.sub('-|\.',' ',extractedDN)
    # change to lower
    extractedDN = extractedDN.lower()
    # remove ATLAS
    extractedDN = re.sub('\(*atlas\)*','',extractedDN)
    # remove numbers
    extractedDN = re.sub('\d*','',extractedDN)
    # remove whitespaces
    extractedDN = extractedDN.strip()
    # dump
    _logger.debug(extractedDN)
    # construct command
    for sTry in ['full','full_rev','fullwc','fullwc_rev,',
                 'suronly','suronly_rev','suronlywc','suronlywc_rev',
                 'firstonly','firstonly_rev','firstonlywc','firstonlywc_rev',
                 'email']:
        if sTry == 'full':
            # try full name
            com = '~atlpan/phonebook --firstname "%s" --surname "%s" --all' \
                  % (extractedDN.split()[0],extractedDN.split()[-1])
        if sTry == 'full_rev':
            # try full name
            com = '~atlpan/phonebook --firstname "%s" --surname "%s" --all' \
                  % (extractedDN.split()[-1],extractedDN.split()[0])
        elif sTry == 'fullwc':
            # try full name with wildcard
            com = '~atlpan/phonebook --firstname "*%s*" --surname "*%s*" --all' \
                  % (extractedDN.split()[0],extractedDN.split()[-1])
        elif sTry == 'fullwc_rev':
            # try full name with wildcard
            com = '~atlpan/phonebook --firstname "*%s*" --surname "*%s*" --all' \
                  % (extractedDN.split()[-1],extractedDN.split()[0])
        elif sTry == 'suronly':
            # try surname only
            com = '~atlpan/phonebook --surname "%s" --all' \
                  % extractedDN.split()[-1]
        elif sTry == 'suronly_rev':
            # try surname only
            com = '~atlpan/phonebook --surname "%s" --all' \
                  % extractedDN.split()[0]
        elif sTry == 'suronlywc':
            # try surname with wildcard
            com = '~atlpan/phonebook --surname "*%s*" --all' \
                  % extractedDN.split()[-1]
        elif sTry == 'suronlywc_rev':
            # try surname with wildcard
            com = '~atlpan/phonebook --surname "*%s*" --all' \
                  % extractedDN.split()[0]
        elif sTry == 'firstonly':
            # try firstname only
            com = '~atlpan/phonebook --firstname "%s" --all' \
                  % extractedDN.split()[-1]
        elif sTry == 'firstonly_rev':
            # try firstname only
            com = '~atlpan/phonebook --firstname "%s" --all' \
                  % extractedDN.split()[0]
        elif sTry == 'firstonlywc':
            # try firstname with wildcard
            com = '~atlpan/phonebook --firstname "*%s*" --all' \
                  % extractedDN.split()[-1]
        elif sTry == 'firstonlywc_rev':
            # try firstname with wildcard
            com = '~atlpan/phonebook --firstname "*%s*" --all' \
                  % extractedDN.split()[0]
        elif sTry == 'email':
            # try email
            mailPatt = re.sub(' +','*',extractedDN)
            com = '~atlpan/phonebook --email "*%s*" --all' \
                  % mailPatt
        _logger.debug(com)
        # execute
        sStat,sOut = commands.getstatusoutput(com)
        _logger.debug(sOut)
        # failed
        if sStat != 0:
            _logger.debug('phonebook failed with %s' % sStat)
            return []
        # extract email
        emails = []
        groups = []
        for line in sOut.split('\n'):
            if line.startswith('E-mail:'):
                # append
                tmpStr = line.split()[-1]
                emails.append(tmpStr)
            elif line.startswith('Group:'):
                # append
                tmpStr = line.split()[-1]
                groups.append(tmpStr)
        # check groups when multiple candidates
        if len(emails) > 1 and len(emails) == len(groups):
            newEmails = []
            for idx,group in enumerate(groups):
                if group.startswith('A') or group in ['UAT']:
                    newEmails.append(emails[idx])
            # replace
            emails = newEmails
        _logger.debug('emails=%s' % str(emails))
        # return
        if len(emails) == 1:
            _logger.debug('Succeeded')
            return emails
            
    # failed
    _logger.error('Failed for %s' % dn)
    return []
    
