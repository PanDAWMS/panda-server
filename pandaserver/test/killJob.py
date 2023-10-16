import optparse
import sys

import pandaserver.userinterface.Client as Client

optP = optparse.OptionParser(conflict_handler="resolve")
optP.add_option(
    "-9",
    action="store_const",
    const=True,
    dest="forceKill",
    default=False,
    help="kill jobs before next heartbeat is coming",
)
optP.add_option("--codeV", action="store", dest="codeV", default=None, help="kill code")
optP.add_option(
    "--jobSubStatus",
    action="store",
    dest="jobSubStatus",
    default=None,
    help="set job sub status if any",
)
optP.add_option(
    "--killOwnProdJobs",
    action="store_const",
    const=True,
    dest="killOwnProdJobs",
    default=False,
    help="kill own production jobs without a production role",
)
optP.add_option(
    "--killUserJobs",
    action="store_const",
    const=True,
    dest="killUserJobs",
    default=False,
    help="kill user jobs using a production role",
)
optP.add_option(
    "--keepUnmerged",
    action="store_const",
    const=True,
    dest="keepUnmerged",
    default=False,
    help="generate a new job after kiliing, to keep unmerged events",
)
options, args = optP.parse_args()


aSrvID = None

codeV = None
useMailAsIDV = False

if options.forceKill:
    if options.killUserJobs:
        codeV = 99
    else:
        codeV = 9
elif options.killUserJobs:
    codeV = 91
else:
    try:
        codeV = int(options.codeV)
    except Exception:
        pass
if options.killOwnProdJobs:
    useMailAsIDV = True

if len(args) == 1:
    Client.killJobs(
        [args[0]],
        code=codeV,
        useMailAsID=useMailAsIDV,
        keepUnmerged=options.keepUnmerged,
        jobSubStatus=options.jobSubStatus,
    )
else:
    startID = int(args[0])
    endID = int(args[1])
    if startID > endID:
        print("%d is less than %d" % (endID, startID))
        sys.exit(1)
    Client.killJobs(
        range(startID, endID + 1),
        code=codeV,
        useMailAsID=useMailAsIDV,
        keepUnmerged=options.keepUnmerged,
        jobSubStatus=options.jobSubStatus,
    )
