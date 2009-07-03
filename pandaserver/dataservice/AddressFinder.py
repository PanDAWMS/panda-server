import re
import commands

from taskbuffer.OraDBProxy import DBProxy
from pandalogger.PandaLogger import PandaLogger

# logger
_logger = PandaLogger().getLogger('AddressFinder')

# insert *
def insertWC(str):
    retStr = ".*"
    for item in str:
        retStr += item
        retStr += ".*"
    return retStr


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
    # remove Jr
    extractedDN = re.sub(' jr( |$)',' ',extractedDN)    
    # remove whitespaces
    extractedDN = re.sub(' +',' ',extractedDN)
    extractedDN = extractedDN.strip()
    # dump
    _logger.debug(extractedDN)
    # construct command
    for sTry in ['full','full_rev','fullwc','fullwc_rev,',
                 'suronly', 'firstonly','suronly_rev','firstonly_rev',
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
            if len(extractedDN.split()) == 2:
                # try surname only
                com = '~atlpan/phonebook --surname "%s" --all' \
                      % extractedDN.split()[-1]
            else:
                # try surname with wildcard
                com = '~atlpan/phonebook --surname "*%s*" --all' \
                      % extractedDN.split()[-1]
        elif sTry == 'suronly_rev':
            if len(extractedDN.split()) == 2:            
                # try surname only
                com = '~atlpan/phonebook --surname "%s" --all' \
                      % extractedDN.split()[0]
            else:
                # try surname with wildcard
                com = '~atlpan/phonebook --surname "*%s*" --all' \
                      % extractedDN.split()[0]
        elif sTry == 'firstonly':
            if len(extractedDN.split()) == 2:
                # try firstname only
                com = '~atlpan/phonebook --firstname "%s" --all' \
                      % extractedDN.split()[0]
            else:
                # try firstname with wildcard
                com = '~atlpan/phonebook --firstname "*%s*" --all' \
                      % extractedDN.split()[0]
        elif sTry == 'firstonly_rev':
            if len(extractedDN.split()) == 2:
                # try firstname only
                com = '~atlpan/phonebook --firstname "%s" --all' \
                      % extractedDN.split()[-1]
            else:
                # try firstname with wildcard
                com = '~atlpan/phonebook --firstname "*%s*" --all' \
                      % extractedDN.split()[-1]
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
        dnames = []
        for line in sOut.split('\n'):
            if line.startswith('E-mail:'):
                # append
                tmpStr = line.split()[-1]
                emails.append(tmpStr)
            elif line.startswith('Group:'):
                # append
                tmpStr = line.split()[-1]
                groups.append(tmpStr)
            elif line.startswith('Display Name:'):
                # append
                tmpStr = re.sub('^[^:]+:','',line).strip()
                dnames.append(tmpStr)
        # check groups
        newGroups = []
        newEmails = []
        newDNames = []
        for idx,group in enumerate(groups):
            if group.startswith('A') or group in ['UAT','GS','-']:
                newGroups.append(group)
                newEmails.append(emails[idx])
                newDNames.append(dnames[idx])
        # replace
        groups = newGroups
        emails = newEmails
        dnames = newDNames
        # check dname
        if len(emails) > 1 and len(emails) == len(dnames):
            newGroups   = []
            newEmails   = []
            newDNames   = []
            newGroupsWC = []
            newEmailsWC = []
            newDNamesWC = []
            for idx,dname in enumerate(dnames):
                # check fragments
                nameItems = extractedDN.split() 
                nMatch   = 0
                nMatchWC = 0                
                for nameItem in nameItems:
                    # check w/o wildcard
                    if re.search(nameItem,dname,re.I) != None:
                        nMatch += 1
                    # check with wildcard    
                    if re.search(insertWC(nameItem),dname,re.I) != None:
                        nMatchWC += 1
                # append if totally matched or partially matched ignoring middle-name etc
                if len(nameItems) == nMatch or (len(nameItems) > 2 and (len(nameItems)-nMatch) < 2):
                    newGroups.append(groups[idx])
                    newEmails.append(emails[idx])
                    newDNames.append(dname)
                # append if matched with wildcard
                if len(nameItems) == nMatchWC or (len(nameItems) > 2 and (len(nameItems)-nMatchWC) < 2):
                    newGroupsWC.append(groups[idx])
                    newEmailsWC.append(emails[idx])
                    newDNamesWC.append(dname)
            # replace
            if len(newGroups)>0:
                # use strict matching
                groups = newGroups
                emails = newEmails
                dnames = newDNames
            else:
                # use loose matching
                groups = newGroupsWC
                emails = newEmailsWC
                dnames = newDNamesWC
        _logger.debug('emails=%s' % str(emails))
        # return
        if len(emails) == 1:
            _logger.debug('Succeeded %s %s' % (groups[0],emails[0]))
            return emails
            
    # failed
    _logger.error('Failed for %s' % dn)
    return []
    
