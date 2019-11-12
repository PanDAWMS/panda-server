import os
import glob
import optparse
import datetime

# options
optP = optparse.OptionParser(conflict_handler="resolve")
optP.add_option('-t',action='store_const',const=True,dest='test',default=False,
                help='test mode')
optP.add_option('-h',action='store',type='int',dest='limit',default=12,
                help='time limit in hour')
options,args = optP.parse_args()

# patterns of tmp files
tmpPatts = ['/tmp/tmp*','/tmp/atlpan/tmp*','/tmp/pansrv/tmp*']

# limit
timeLimit = datetime.datetime.utcnow() - datetime.timedelta(hours=options.limit)

# loop over all pattern
for tmpPatt in tmpPatts:
    tmpFiles = glob.glob(tmpPatt)
    # loop over all files
    for tmpFile in tmpFiles:
        try:
            print('INFO: tmpfile -> %s' % tmpFile)
            # only file
            if not os.path.isfile(tmpFile):
                continue
            # not symlink
            if os.path.islink(tmpFile):
                continue
            # writable
            if not os.access(tmpFile,os.W_OK):
                continue
            # check time stamp
            timeStamp = os.path.getmtime(tmpFile)
            timeStamp = datetime.datetime.fromtimestamp(timeStamp)
            if timeStamp > timeLimit:
                continue
            # remove
            print('INFO:    remove %s' % tmpFile)
            if not options.test:
                os.remove(tmpFile)
        except Exception:
            errType,errValue = sys.exc_info()[:2]
            print('ERROR:   failed with %s:%s' % (errType,errValue))
