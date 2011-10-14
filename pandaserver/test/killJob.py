import sys
import optparse
import userinterface.Client as Client

optP = optparse.OptionParser(conflict_handler="resolve")
optP.add_option('-9',action='store_const',const=True,dest='forceKill',
                default=False,help='kill jobs before next heartbeat is coming')
optP.add_option('--killOwnProdJobs',action='store_const',const=True,dest='killOwnProdJobs',
                default=True,help='kill own production jobs without a production role')
options,args = optP.parse_args()


aSrvID = None

codeV = None
useMailAsIDV = False

if options.forceKill:
    codeV = 9
if options.killOwnProdJobs:
    useMailAsIDV = True

if len(args) == 1:
    Client.killJobs([args[0]],code=codeV,useMailAsID=useMailAsIDV)
else:
    startID = int(args[0])
    endID   = int(args[1])
    if startID > endID:
        print '%d is less than %d' % (endID,startID)
        sys.exit(1)
    Client.killJobs(range(startID,endID+1),code=codeV,useMailAsID=useMailAsIDV)

