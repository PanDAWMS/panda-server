import os
import re
import sys
from ftplib import FTP
from pandalogger.PandaLogger import PandaLogger

# supported architectures
targetArchs = ['Linux-slc5-gcc4.3.tar.gz','Linux-slc5_amd64-gcc4.3.tar.gz']

# destination dir
destDir = '/data/atlpan/srv/var/appdir'

# logger
_logger = PandaLogger().getLogger('copyROOT')

_logger.debug("===================== start =====================")

try:
    # login to root repository
    ftp = FTP('root.cern.ch')
    output = ftp.login()
    _logger.debug(output)
    output = ftp.cwd('root')
    _logger.debug(output)    
    # get list
    flist = ftp.nlst()
    # loop over all files
    for tmpFile in flist:
        # skip RC
        if re.search('-rc\d\.',tmpFile) != None:
            continue
        # check arch
        supportedFlag = False
        for tmpArch in targetArchs:
            if tmpFile.endswith(tmpArch):
                supportedFlag = True
                break
        # copy
        if supportedFlag:
            _logger.debug('start %s' % tmpFile)
            dstFileName = '%s/%s' % (destDir,tmpFile)
            # check local
            if os.path.exists(dstFileName):
                # get remote size
                rsize = ftp.size(tmpFile)
                if rsize == None:
                    _logger.debug(' cannot get remote size for %s' % tmpFile)
                else:
                    # local size
                    lsize = os.path.getsize(dstFileName)
                    if lsize == rsize:
                        _logger.debug('skip since alredy there %s' % tmpFile)                        
                        continue
            # copy
            _logger.debug('copy %s' % tmpFile)
            outFile = open(dstFileName,'wb')
            ftp.retrbinary('RETR %s' % tmpFile,outFile.write)
            outFile.close()
            _logger.debug('end %s' % tmpFile)
    # quit        
    output = ftp.quit()
    _logger.debug(output)
    # make list
    listFileName = 'applist'
    listFilePath = '%s/%s' % (destDir,listFileName)
    listFile = open(listFilePath,'w')
    for tmpFile in os.listdir(destDir):
        # skip hidden files
        if tmpFile.startswith('.'):
            continue
        # skip applist
        if tmpFile == listFileName:
            continue
        listFile.write('%s\n' % tmpFile)
    listFile.close()    
except:
    errType,errValue = sys.exc_info()[:2]
    _logger.error("Failed with %s %s" % (errType,errValue))


_logger.debug("===================== end =====================")
