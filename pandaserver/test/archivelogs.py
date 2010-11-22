import re
import os
import glob
import stat
import commands

from config import panda_config

srcDir = panda_config.logdir
dstDir = '/tmp/logbackup' + srcDir

logFiles = glob.glob(srcDir+'/*log.1.gz')

# check time stamp
for logFile in logFiles:
    baseName = logFile.split('/')[-1]
    print "log name : %s" % baseName
    targetFile = "%s/%s" % (dstDir,baseName)
    # already exists
    if os.path.exists(targetFile) and \
           os.stat(logFile)[stat.ST_SIZE] == os.stat(targetFile)[stat.ST_SIZE]:
        com = 'cmp %s %s' % (logFile,targetFile)
        cmpSt,cmpOut = commands.getstatusoutput(com)
        if cmpSt == 0:
            print "  -> skip : already exists"
            continue
    # increment
    maxIndex = 100
    if os.path.exists(targetFile):
        templateName = re.sub('1\.gz$','%s.gz',baseName)
        for tmpIdx in range(1,maxIndex):
            renameSrc = dstDir + '/' + (templateName % (maxIndex-tmpIdx))
            renameDst = dstDir + '/' + (templateName % (maxIndex-tmpIdx+1))
            if os.path.exists(renameSrc):
                com = 'mv -f %s %s' % (renameSrc,renameDst)
                print com
                print commands.getoutput(com)
    # copy
    com = 'cp -fp %s %s' % (logFile,dstDir)
    print com
    print commands.getoutput(com)

# touch to avoid tmpwatch
com = 'touch %s/*' % dstDir
print commands.getoutput(com)
