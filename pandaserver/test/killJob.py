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
    ret = Client.kill_jobs([args[0]], code=codeV, keep_unmerged=options.keepUnmerged, job_sub_status=options.jobSubStatus)
else:
    job_id_start = int(args[0])
    job_id_end = int(args[1])
    if job_id_start > job_id_end:
        print(f"{job_id_end} is less than {job_id_start}")
        sys.exit(1)

    ret = Client.kill_jobs(range(job_id_start, job_id_end + 1), code=codeV, keep_unmerged=options.keepUnmerged, job_sub_status=options.jobSubStatus)

print(ret)
