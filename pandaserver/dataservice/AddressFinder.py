import re
import sys
import urllib
import commands

from config import panda_config
from taskbuffer.OraDBProxy import DBProxy
from pandalogger.PandaLogger import PandaLogger

# logger
_logger = PandaLogger().getLogger('AddressFinder')

# NG words in email address
_ngWordsInMailAddr = ['support','system','stuff']


# insert *
def insertWC(str):
    retStr = ".*"
    for item in str:
        retStr += item
        retStr += ".*"
    return retStr


# clean name
def cleanName(dn):
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
    # return
    return extractedDN


# get email address using phonebook
def getEmailPhonebook(dn):
    _logger.debug('Getting email via phonebook for %s' % dn)
    # clean DN
    extractedDN = cleanName(dn)
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
    

# get email address using xwho
def getEmailXwho(dn):
    # get email from CERN/xwho
    _logger.debug('Getting email via xwho for %s' % dn)
    for sTry in ['full','firstlastonly']:
        try:
            # remove middle name
            encodedDN = cleanName(dn)
            encodedDN = re.sub(' . ',' ',encodedDN)
            # remove _
            encodedDN = encodedDN.replace('_',' ')
            # use fist and lastnames only
            if sTry == 'firstlastonly':
                newEncodedDN = '%s %s' % (encodedDN.split()[0],encodedDN.split()[-1])
                # skip if it was already tried
                if encodedDN == newEncodedDN:
                    continue
                encodedDN = newEncodedDN
            # URL encode
            encodedDN = encodedDN.replace(' ','%20')
            url = 'http://consult.cern.ch/xwho?'+encodedDN
            if panda_config.httpProxy != '':
                proxies = proxies={'http': panda_config.httpProxy}
            else:
                proxies = proxies={}
            opener = urllib.FancyURLopener(proxies)
            fd=opener.open(url)
            data = fd.read()
            if re.search(' not found',data,re.I) == None:
                break
        except:
            type, value, traceBack = sys.exc_info()
            _logger.error("xwho failure with %s %s" % (type,value))
            return []
    # parse HTML
    emails = []
    headerItem = ["Family Name","First Name","Phone","Dep"]
    findTable = False
    _logger.debug(data)
    for line in data.split('\n'):
        # look for table
        if not findTable:
            # look for header
            tmpFlag = True
            for item in headerItem:
                if re.search(item,line) == None:
                    tmpFlag = False
                    break
            findTable = tmpFlag
            continue
        else:
            # end of table
            if re.search(item,"</table>") != None:
                findTable = False
                continue
            # look for link to individual page
            match = re.search('href="(/xwho/people/\d+)"',line)
            if match == None:
                continue
            link = match.group(1)
            try:
                url = 'http://consult.cern.ch'+link
                if panda_config.httpProxy != '':                    
                    proxies = proxies={'http': panda_config.httpProxy}
                else:
                    proxies = proxies={}
                opener = urllib.FancyURLopener(proxies)
                fd=opener.open(url)
                data = fd.read()
                _logger.debug(data)
            except:
                type, value, traceBack = sys.exc_info()
                _logger.error("xwho failure with %s %s" % (type,value))
                return []
            # get mail adder
            match = re.search("mailto:([^@]+@[^>]+)>",data)
            if match != None:
                adder = match.group(1)
                # check NG words
                okAddr = True
                for ngWord in _ngWordsInMailAddr:
                    if re.search(ngWord,adder,re.I):
                        _logger.error("%s has NG word:%s" % (adder,ngWord))
                        okAddr = False
                        break
                if okAddr and (not adder in emails):
                    emails.append(adder)
    _logger.debug("emails from xwho : '%s'" % emails)
    # return
    if len(emails) == 1:
        _logger.debug('Succeeded : %s %s' % (str(emails),dn))
        return emails
    # multiple candidates
    if len(emails) > 1:
        _logger.error("non unique address : %s for %s" % (str(emails),dn))
        return []
    # failed
    _logger.error('Failed to find address for %s' % dn)
    return []
    
            

                    
        
