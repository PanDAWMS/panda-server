import datetime
import functools
import time
import traceback
import uuid

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandalogger.PandaLogger import PandaLogger

from pandaserver.dataservice import DataServiceUtils
from pandaserver.dataservice.DataServiceUtils import select_scope

_log = PandaLogger().getLogger("broker")

# all known sites
_allSites = []

DEFAULT_DATA_TYPE = "GEN"
BULK_SIZE = 20
PRIORITY_INTERVAL = 50


def _generate_dispatch_block_name(job):
    # get the data type
    try:
        tmp_data_type = job.prodDBlock.split(".")[-2]
    except Exception:
        tmp_data_type = DEFAULT_DATA_TYPE  # set a default value

    # avoid long names over 20 characters
    if len(tmp_data_type) > 20:
        tmp_data_type = DEFAULT_DATA_TYPE

    transfer_type = "transfer"
    if job.useInputPrestaging():
        transfer_type = "prestaging"

    dispatch_block_name = f"panda.{job.taskID}.{time.strftime('%m.%d')}.{tmp_data_type}.{transfer_type}.{str(uuid.uuid4())}_dis{job.PandaID}"

    return dispatch_block_name


# comparison function for sort
def _comparison_function(job_a, job_b):
    # This comparison helps in sorting the jobs based on the order of their computing sites in the _allSites list.
    # append site if not in list
    if job_a.computingSite not in _allSites:
        _allSites.append(job_a.computingSite)
    if job_b.computingSite not in _allSites:
        _allSites.append(job_b.computingSite)

    # compare site indices
    index_a = _allSites.index(job_a.computingSite)
    index_b = _allSites.index(job_b.computingSite)

    if index_a > index_b:
        return 1
    elif index_a < index_b:
        return -1
    else:
        return 0


# set 'ready' if files are already there
def _set_files_to_ready(tmp_job, ok_files, site_mapper, tmp_logger):
    tmp_logger.debug(str(ok_files))

    # all files could be set to ready
    all_ok = True

    # get the panda queue spec and the associated tape endpoints
    tmp_site_spec = site_mapper.getSite(tmp_job.computingSite)
    scope_association_site_input, _ = select_scope(tmp_site_spec, tmp_job.prodSourceLabel, tmp_job.job_label)
    tmp_tape_endpoints = tmp_site_spec.ddm_endpoints_input[scope_association_site_input].getTapeEndPoints()

    # loop through all files and process the input files
    for tmp_file in tmp_job.Files:
        if tmp_file.type == "input":
            # file is in ready state
            if tmp_file.status == "ready":
                tmp_file.dispatchDBlock = "NULL"

            # file is cached
            elif DataServiceUtils.isCachedFile(tmp_file.dataset, tmp_site_spec):
                tmp_file.status = "cached"
                tmp_file.dispatchDBlock = "NULL"

            # use DDM pre-stage only for on-tape files
            elif len(tmp_tape_endpoints) > 0 and tmp_file.lfn in ok_files:
                tape_only = True
                tape_copy = False
                for tmp_storage_endpoint in ok_files[tmp_file.lfn]:
                    if tmp_storage_endpoint not in tmp_tape_endpoints:
                        tape_only = False
                    else:
                        # there is a tape copy
                        tape_copy = True
                # trigger pre-stage when disk copy doesn't exist or token is TAPE
                if tape_only or (tape_copy and tmp_file.dispatchDBlockToken in ["ATLASDATATAPE", "ATLASMCTAPE"]):
                    all_ok = False
                else:
                    # set ready
                    tmp_file.status = "ready"
                    tmp_file.dispatchDBlock = "NULL"

            # set ready if the file exists and the site doesn't use pre-stage
            else:
                tmp_file.status = "ready"
                tmp_file.dispatchDBlock = "NULL"

    # unset the dispatch dataset
    if all_ok:
        tmp_job.dispatchDBlock = "NULL"


def schedule(jobs, site_mapper):
    timestamp = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None).isoformat("/")
    tmp_logger = LogWrapper(_log, f"start_ts={timestamp}")

    try:
        tmp_logger.debug(f"start")

        # no jobs
        if len(jobs) == 0:
            tmp_logger.debug("finished : no jobs")
            return

        i_job = 0
        file_list = []
        files_ok = {}
        chosen_panda_queue = None
        computing_site = None
        production_data_block = None
        dispatch_data_block = None
        previous_cloud = None
        previous_prod_source_label = None
        previous_transfer_type = None
        prev_created_by_jedi = None
        previous_working_group = None
        previous_priority = None

        index_job = 0

        # sort jobs by siteID. Some jobs may already define computingSite
        jobs = sorted(jobs, key=functools.cmp_to_key(_comparison_function))

        # loop over all jobs + terminator(None)
        for job in jobs + [None]:
            index_job += 1

            # ignore failed jobs
            if job and job.jobStatus == "failed":
                continue

            overwrite_site = False

            # check if the job was created by JEDI
            created_by_jedi = False
            if job and job.lockedby == "jedi":
                created_by_jedi = True

            # new bunch or terminator
            if (
                job is None
                or len(file_list) >= BULK_SIZE
                or (dispatch_data_block is None and job.homepackage.startswith("AnalysisTransforms"))
                or production_data_block != job.prodDBlock
                or job.computingSite != computing_site
                or i_job > BULK_SIZE
                or previous_cloud != job.getCloud()
                or previous_transfer_type != job.transferType
                or previous_working_group != job.workingGroup
                or prev_created_by_jedi != created_by_jedi
            ):
                if index_job > 1:
                    tmp_logger.debug(
                        f"new bunch\n"
                        f"  i_job          {i_job}\n"
                        f"  cloud          {previous_cloud}\n"
                        f"  sourceLabel    {previous_prod_source_label}\n"
                        f"  priority       {previous_priority}\n"
                        f"  prodDBlock     {production_data_block}\n"
                        f"  computingSite  {computing_site}\n"
                        f"  workingGroup   {previous_working_group}\n"
                        f"  transferType   {previous_transfer_type}\n"
                    )

                chosen_panda_queue = site_mapper.getSite(job.computingSite)

                # set job spec
                tmp_logger.debug(f"index_job     : {index_job}")
                for tmpJob in jobs[index_job - i_job - 1 : index_job - 1]:
                    # set computingSite
                    tmpJob.computingSite = chosen_panda_queue.sitename
                    tmp_logger.debug(f"PandaID:{tmpJob.PandaID} -> site:{tmpJob.computingSite}")

                    # set ready if files are already there
                    if not prev_created_by_jedi:
                        _set_files_to_ready(tmpJob, files_ok, site_mapper, tmp_logger)

                # terminate
                if not job:
                    break

                # reset i_job
                i_job = 0

                # reset file list
                file_list = []
                files_ok = {}

                # create new dispDBlock
                if job.prodDBlock != "NULL":
                    dispatch_data_block = _generate_dispatch_block_name(job)
                    tmp_logger.debug(f"New dispatch_data_block: {dispatch_data_block}")

                production_data_block = job.prodDBlock

            # increment i_job
            i_job += 1

            # reserve computing site and cloud
            computing_site = job.computingSite
            previous_cloud = job.getCloud()
            previous_prod_source_label = job.prodSourceLabel
            previous_transfer_type = job.transferType
            previous_working_group = job.workingGroup
            prev_created_by_jedi = created_by_jedi

            # truncate prio to avoid too many lookups
            if job.currentPriority not in [None, "NULL"]:
                previous_priority = (job.currentPriority / PRIORITY_INTERVAL) * PRIORITY_INTERVAL
            # assign site
            if chosen_panda_queue != "TOBEDONE":
                job.computingSite = chosen_panda_queue.sitename

                tmp_logger.debug(f"PandaID:{job.PandaID} -> preset site:{chosen_panda_queue.sitename}")

                # set cloud
                if job.cloud in ["NULL", None, ""]:
                    job.cloud = chosen_panda_queue.cloud

            # set destinationSE
            destination_se = job.destinationSE
            if site_mapper.checkCloud(job.getCloud()):
                # use cloud dest for non-existing sites
                if job.prodSourceLabel != "user" and job.destinationSE not in site_mapper.siteSpecList and job.destinationSE != "local":
                    if DataServiceUtils.checkJobDestinationSE(job) is not None:
                        destination_se = DataServiceUtils.checkJobDestinationSE(job)
                    job.destinationSE = destination_se

            if overwrite_site:
                # overwrite SE for analysis jobs which set non-existing sites
                destination_se = job.computingSite
                job.destinationSE = destination_se

            # set dispatchDBlock and destinationSE
            first = True
            for file in job.Files:
                # Set dispatch block for pre-staging jobs too
                if file.type == "input" and file.dispatchDBlock == "NULL" and (file.status not in ["ready", "missing", "cached"]):
                    if first:
                        first = False
                        job.dispatchDBlock = dispatch_data_block
                    file.dispatchDBlock = dispatch_data_block
                    file.status = "pending"
                    if file.lfn not in file_list:
                        file_list.append(file.lfn)

                # set the file destinationSE
                if file.type in ["output", "log"] and destination_se:
                    if job.prodSourceLabel == "user" and job.computingSite == file.destinationSE:
                        pass
                    elif job.prodSourceLabel == "user" and prev_created_by_jedi is True and file.destinationSE not in ["", "NULL"]:
                        pass
                    elif destination_se == "local":
                        pass
                    elif DataServiceUtils.getDistributedDestination(file.destinationDBlockToken) is not None:
                        pass
                    else:
                        file.destinationSE = destination_se

                # pre-assign GUID to log
                if file.type == "log":
                    file.GUID = str(uuid.uuid4())

        tmp_logger.debug("finished")

    except Exception as e:
        tmp_logger.error(f"schedule : {str(e)} {traceback.format_exc()}")
