import datetime
import json
import os
import sys

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandautils.PandaUtils import get_sql_IN_bind_variables, naive_utcnow

from pandaserver.config import panda_config
from pandaserver.srvcore import CoreUtils
from pandaserver.srvcore.CoreUtils import clean_host_name
from pandaserver.taskbuffer import ErrorCode, SiteSpec
from pandaserver.taskbuffer.db_proxy_mods.base_module import BaseModule, memoize
from pandaserver.taskbuffer.db_proxy_mods.entity_module import get_entity_module
from pandaserver.taskbuffer.HarvesterMetricsSpec import HarvesterMetricsSpec
from pandaserver.taskbuffer.JobSpec import JobSpec
from pandaserver.taskbuffer.ResourceSpec import BASIC_RESOURCE_TYPE
from pandaserver.taskbuffer.WorkerSpec import WorkerSpec

DEFAULT_PRODSOURCELABEL = "managed"


# Module class to define methods related to worker and harvester
class WorkerModule(BaseModule):
    # constructor
    def __init__(self, log_stream: LogWrapper):
        super().__init__(log_stream)

    # update stat of workers with jobtype breakdown
    def reportWorkerStats_jobtype(self, harvesterID, siteName, parameter_list):
        comment = " /* DBProxy.reportWorkerStats_jobtype */"
        tmp_log = self.create_tagged_logger(comment, f"harvesterID={harvesterID} siteName={siteName}")
        tmp_log.debug("start")
        tmp_log.debug(f"params={str(parameter_list)}")
        try:
            # load new site data
            parameter_list = json.loads(parameter_list)
            # set autocommit on
            self.conn.begin()

            # lock the site data rows
            var_map = {":harvesterID": harvesterID, ":siteName": siteName}
            sql_lock = (
                "SELECT harvester_ID, computingSite FROM ATLAS_PANDA.Harvester_Worker_Stats "
                "WHERE harvester_ID=:harvesterID AND computingSite=:siteName FOR UPDATE NOWAIT "
            )
            try:
                self.cur.execute(sql_lock + comment, var_map)
            except Exception:
                self._rollback()
                message = "rows locked by another update"
                tmp_log.debug(message)
                tmp_log.debug("done")
                return False, message

            # delete them
            sql_delete = "DELETE FROM ATLAS_PANDA.Harvester_Worker_Stats WHERE harvester_ID=:harvesterID AND computingSite=:siteName "
            self.cur.execute(sql_delete + comment, var_map)

            # insert new site data
            sql_insert = (
                "INSERT INTO ATLAS_PANDA.Harvester_Worker_Stats (harvester_ID, computingSite, jobType, resourceType, status, n_workers, lastUpdate) "
                "VALUES (:harvester_ID, :siteName, :jobType, :resourceType, :status, :n_workers, CURRENT_DATE) "
            )

            var_map_list = []
            for jobType in parameter_list:
                jt_params = parameter_list[jobType]
                for resourceType in jt_params:
                    params = jt_params[resourceType]
                    if resourceType == "Undefined":
                        continue
                    for status in params:
                        n_workers = params[status]
                        var_map = {
                            ":harvester_ID": harvesterID,
                            ":siteName": siteName,
                            ":status": status,
                            ":jobType": jobType,
                            ":resourceType": resourceType,
                            ":n_workers": n_workers,
                        }
                        var_map_list.append(var_map)

            self.cur.executemany(sql_insert + comment, var_map_list)

            if not self._commit():
                raise RuntimeError("Commit error")

            tmp_log.debug("done")
            return True, "OK"
        except Exception as e:
            self._rollback()
            self.dump_error_message(tmp_log)
            return False, "database error"

    # get stat of workers
    def getWorkerStats(self):
        comment = " /* DBProxy.getWorkerStats */"
        tmp_log = self.create_tagged_logger(comment)
        tmp_log.debug("start")
        try:
            # set autocommit on
            self.conn.begin()

            # sql to get stat of workers
            sql_get_stats = (
                "SELECT SUM(n_workers), computingSite, harvester_ID, jobType, resourceType, status "
                "FROM ATLAS_PANDA.Harvester_Worker_Stats "
                "WHERE lastUpdate>=:time_limit "
                "GROUP BY computingSite,harvester_ID,jobType,resourceType,status "
            )
            var_map = {
                ":time_limit": naive_utcnow() - datetime.timedelta(hours=4),
            }
            self.cur.execute(sql_get_stats + comment, var_map)
            res_active = self.cur.fetchall()
            result_map = {}
            for cnt, computingSite, harvesterID, jobType, resourceType, status in res_active:
                result_map.setdefault(computingSite, {})
                result_map[computingSite].setdefault(harvesterID, {})
                result_map[computingSite][harvesterID].setdefault(jobType, {})
                if resourceType not in result_map[computingSite][harvesterID][jobType]:
                    result_map[computingSite][harvesterID][jobType][resourceType] = dict()
                result_map[computingSite][harvesterID][jobType][resourceType][status] = cnt
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            tmp_log.debug(f"done with {str(result_map)}")
            return result_map
        except Exception:
            # roll back
            self._rollback()
            self.dump_error_message(tmp_log)
            return {}

    # send command to harvester or lock command
    def commandToHarvester(
        self,
        harvester_ID,
        command,
        ack_requested,
        status,
        lockInterval,
        comInterval,
        params,
        useCommit=True,
    ):
        comment = " /* DBProxy.commandToHarvester */"
        tmp_log = self.create_tagged_logger(comment, f"harvesterID={harvester_ID} command={command}")
        tmp_log.debug("start")
        tmp_log.debug(f"params={str(params)}")
        try:
            if useCommit:
                self.conn.begin()
            # check if command exists
            sql_check_command = "SELECT status,status_date FROM ATLAS_PANDA.HARVESTER_COMMANDS WHERE harvester_ID=:harvester_ID AND command=:command "
            var_map = {
                ":harvester_ID": harvester_ID,
                ":command": command,
            }
            self.cur.execute(sql_check_command + comment, var_map)
            existing_command = self.cur.fetchone()
            # check existing command
            to_skip = False
            if existing_command is not None:
                command_status, status_date = existing_command
                # not overwrite existing command
                if (
                    command_status in ["new", "lock", "retrieved"]
                    and lockInterval is not None
                    and status_date > naive_utcnow() - datetime.timedelta(minutes=lockInterval)
                ):
                    to_skip = True
                elif (
                    command_status in ["retrieved", "acknowledged"]
                    and comInterval is not None
                    and status_date > naive_utcnow() - datetime.timedelta(minutes=comInterval)
                ):
                    to_skip = True
                else:
                    # delete existing command
                    sql_delete_command = "DELETE FROM ATLAS_PANDA.HARVESTER_COMMANDS WHERE harvester_ID=:harvester_ID AND command=:command "
                    var_map = {
                        ":harvester_ID": harvester_ID,
                        ":command": command,
                    }
                    self.cur.execute(sql_delete_command + comment, var_map)
            # insert
            if not to_skip:
                var_map = {
                    ":harvester_id": harvester_ID,
                    ":command": command,
                    ":ack_requested": 1 if ack_requested else 0,
                    ":status": status,
                }
                sql_insert_command = (
                    "INSERT INTO ATLAS_PANDA.HARVESTER_COMMANDS (command_id,creation_date,status_date,command,harvester_id,ack_requested,status"
                )
                if params is not None:
                    var_map[":params"] = json.dumps(params)
                    sql_insert_command += ",params"
                sql_insert_command += ") "
                sql_insert_command += (
                    "VALUES (ATLAS_PANDA.HARVESTER_COMMAND_ID_SEQ.nextval,CURRENT_DATE,CURRENT_DATE,:command,:harvester_id,:ack_requested,:status"
                )
                if params is not None:
                    sql_insert_command += ",:params"
                sql_insert_command += ") "
                self.cur.execute(sql_insert_command + comment, var_map)
            if useCommit:
                if not self._commit():
                    raise RuntimeError("Commit error")
            tmp_log.debug("done")
            if to_skip:
                return False
            return True
        except Exception:
            # roll back
            if useCommit:
                self._rollback()
            self.dump_error_message(tmp_log)
            return False

    # send command to harvester to kill all workers
    def sweepPQ(self, panda_queue_des, status_list_des, ce_list_des, submission_host_list_des):
        comment = " /* DBProxy.sweepPQ */"
        tmp_log = self.create_tagged_logger(comment)
        tmp_log.debug("start")
        try:
            # Figure out the harvester instance serving the queues and check the CEs match
            pq_data_des = get_entity_module(self).get_config_for_pq(panda_queue_des)
            if not pq_data_des:
                return "Error retrieving queue configuration from DB"

            harvester_id = pq_data_des["harvester"]
            if not harvester_id:
                return "Queue not served by any harvester ID"

            # check CEs
            if ce_list_des == "ALL":
                ce_list_des_sanitized = "ALL"
            else:
                computing_elements = pq_data_des["queues"]
                ce_names = [str(ce["ce_endpoint"]) for ce in computing_elements]
                ce_list_des_sanitized = [ce for ce in ce_list_des if ce in ce_names]

            # we can't correct submission hosts or the status list

            command = "KILL_WORKERS"
            ack_requested = False
            status = "new"
            lock_interval = None
            com_interval = None
            params = {
                "status": status_list_des,
                "computingSite": [panda_queue_des],
                "computingElement": ce_list_des_sanitized,
                "submissionHost": submission_host_list_des,
            }

            self.commandToHarvester(
                harvester_id,
                command,
                ack_requested,
                status,
                lock_interval,
                com_interval,
                params,
            )

            tmp_log.debug("done")
            return "OK"

        except Exception:
            self.dump_error_message(tmp_log)
            return "Problem generating command. Check PanDA server logs"

    def get_average_memory_workers(self, queue, harvester_id, target):
        """
        Calculates the average memory for running and queued workers at a particular panda queue

        :param queue: name of the PanDA queue
        :param harvester_id: string with the harvester ID serving the queue
        :param target: memory target for the queue in MB. This value is only used in the logging

        :return: average_memory_running_submitted, average_memory_running
        """

        comment = " /* DBProxy.get_average_memory_workers */"
        tmp_log = self.create_tagged_logger(comment)
        tmp_log.debug("start")

        try:
            # sql to calculate the average memory for the queue - harvester_id combination
            # "* 1" in sj.data.blah * 1 is required to notify postgres the data type is an int since json element is
            # treated as text otherwise. This is needed only for the first occurrence of each element in the query
            per_core_attr = SiteSpec.catchall_keys["per_core_attr"]
            sql_running_and_submitted = (
                "SELECT /*+ RESULT_CACHE */ /* use_json_type */ sum(total_memory) / NULLIF(sum(n_workers * corecount), 0) "
                "FROM ( "
                "    SELECT hws.computingsite, "
                "           hws.harvester_id, "
                "           hws.n_workers, "
                "           hws.n_workers * NVL(rt.maxcore, NVL(sj.data.corecount * 1, 1)) * NVL(rt.maxrampercore, "
                f"             CASE WHEN sj.data.catchall LIKE '%{per_core_attr}%' THEN TO_NUMBER(sj.data.maxrss) ELSE sj.data.maxrss * 1 / NVL(sj.data.corecount, 1) END) as total_memory, "
                "           NVL(rt.maxcore, NVL(sj.data.corecount, 1)) as corecount "
                "    FROM ATLAS_PANDA.harvester_worker_stats hws "
                "    JOIN ATLAS_PANDA.resource_types rt ON hws.resourcetype = rt.resource_name "
                "    JOIN ATLAS_PANDA.schedconfig_json sj ON hws.computingsite = sj.panda_queue "
                "    WHERE lastupdate > :time_limit "
                "      AND status IN ('running', 'submitted', 'to_submit') "
                "      AND computingsite=:queue AND harvester_id=:harvester_id"
                ") GROUP BY computingsite, harvester_id "
            )

            sql_running = (
                "SELECT /*+ RESULT_CACHE */ /* use_json_type */ sum(total_memory) / NULLIF(sum(n_workers * corecount), 0) "
                "FROM ( "
                "    SELECT hws.computingsite, "
                "           hws.harvester_id, "
                "           hws.n_workers, "
                "           hws.n_workers * NVL(rt.maxcore, NVL(sj.data.corecount * 1, 1)) * NVL(rt.maxrampercore, "
                f"             CASE WHEN sj.data.catchall LIKE '%{per_core_attr}%' THEN TO_NUMBER(sj.data.maxrss) ELSE sj.data.maxrss * 1 / NVL(sj.data.corecount, 1) END) as total_memory, "
                "           NVL(rt.maxcore, NVL(sj.data.corecount, 1)) as corecount "
                "    FROM ATLAS_PANDA.harvester_worker_stats hws "
                "    JOIN ATLAS_PANDA.resource_types rt ON hws.resourcetype = rt.resource_name "
                "    JOIN ATLAS_PANDA.schedconfig_json sj ON hws.computingsite = sj.panda_queue "
                "    WHERE lastupdate > :time_limit "
                "      AND status = 'running' "
                "      AND computingsite=:queue AND harvester_id=:harvester_id"
                ") GROUP BY computingsite, harvester_id "
            )

            # bind variables including truncated time_limit for result cache
            var_map = {
                ":queue": queue,
                ":harvester_id": harvester_id,
                ":time_limit": (naive_utcnow() - datetime.timedelta(hours=1)).replace(second=0, microsecond=0),
            }

            self.cur.execute(sql_running_and_submitted + comment, var_map)
            results = self.cur.fetchone()
            try:
                average_memory_running_submitted = results[0] if results[0] is not None else 0
            except TypeError:
                average_memory_running_submitted = 0

            self.cur.execute(sql_running + comment, var_map)
            results = self.cur.fetchone()
            try:
                average_memory_running = results[0] if results[0] is not None else 0
            except TypeError:
                average_memory_running = 0

            tmp_log.info(
                f"computingsite={queue} and harvester_id={harvester_id} currently has "
                f"meanrss_running_submitted={average_memory_running_submitted} "
                f"meanrss_running={average_memory_running} "
                f"meanrss_target={target} MB"
            )
            return average_memory_running_submitted, average_memory_running

        except Exception:
            self.dump_error_message(tmp_log)
            return 0, 0

    def ups_new_worker_distribution(self, queue, worker_stats):
        """
        Assuming we want to have n_cores_queued >= n_cores_running * .5, calculate how many pilots need to be submitted
        and choose the number

        :param queue: name of the PanDA queue
        :param worker_stats: queue worker stats
        :return:
        """

        comment = " /* DBProxy.ups_new_worker_distribution */"
        tmp_log = self.create_tagged_logger(comment, queue)
        tmp_log.debug("start")
        n_cores_running = 0
        workers_queued = {}
        n_cores_queued = 0
        harvester_ids_temp = list(worker_stats)

        HIMEM = "HIMEM"
        get_entity_module(self).reload_resource_spec_mapper()

        # get the configuration for maximum workers of each type
        pq_data_des = get_entity_module(self).get_config_for_pq(queue)
        resource_type_limits = {}
        cores_queue = 1
        average_memory_target = None

        if not pq_data_des:
            tmp_log.debug("Error retrieving queue configuration from DB, limits can not be applied")
        else:
            try:
                resource_type_limits = pq_data_des["uconfig"]["resource_type_limits"]
            except KeyError:
                tmp_log.debug("No resource type limits")
                pass
            try:
                if pq_data_des["meanrss"] != 0:
                    average_memory_target = pq_data_des["meanrss"]
                else:
                    tmp_log.debug("meanrss is 0, not using it as average_memory_target")
            except KeyError:
                tmp_log.debug("No average memory defined")
                pass

            try:
                cores_queue = pq_data_des["corecount"]
                if not cores_queue:
                    cores_queue = 1
            except KeyError:
                tmp_log.error("No corecount")

        # Retrieve the assigned harvester instance and submit UPS commands only to this instance. We have had multiple
        # cases of test instances submitting to large queues in classic pull mode and not following commands.
        try:
            assigned_harvester_id = pq_data_des["harvester"]
        except KeyError:
            assigned_harvester_id = None

        # If there is no harvester instance assigned to the queue or there are no statistics, we exit without any action
        if assigned_harvester_id and assigned_harvester_id in harvester_ids_temp:
            harvester_id = assigned_harvester_id
        else:
            # commit for postgres to avoid idle transactions
            if not self._commit():
                raise RuntimeError("Commit error")
            tmp_log.error("No harvester instance assigned or not in statistics")
            return {}

        # There is the case where the grid has no workloads and running HIMEM jobs is better than running no jobs
        ignore_meanrss = self.getConfigValue("meanrss", "IGNORE_MEANRSS")
        if ignore_meanrss == True:
            tmp_log.debug(f"Accepting all resource types since meanrss throttling is ignored")

        # If the site defined a memory target, calculate the memory requested by running and queued workers
        resource_types_under_target = []
        if ignore_meanrss != True and average_memory_target:
            average_memory_workers_running_submitted, average_memory_workers_running = self.get_average_memory_workers(
                queue, harvester_id, average_memory_target
            )
            # if the queue is over memory, we will only submit lower workers in the next cycle
            if average_memory_target < max(average_memory_workers_running_submitted, average_memory_workers_running):
                resource_types_under_target = get_entity_module(self).resource_spec_mapper.filter_out_high_memory_resourcetypes(
                    memory_threshold=average_memory_target
                )
                tmp_log.debug(f"Accepting {resource_types_under_target} resource types to respect mean memory target")
            else:
                tmp_log.debug(f"Accepting all resource types as under memory target")

        # there is only job_type = "managed" in the current implementation, but we keep the structure due to backwards compatibility
        for job_type in worker_stats[harvester_id]:
            workers_queued.setdefault(job_type, {})
            for resource_type in worker_stats[harvester_id][job_type]:
                core_factor = get_entity_module(self).resource_spec_mapper.translate_resourcetype_to_cores(resource_type, cores_queue)
                try:
                    n_cores_running = n_cores_running + worker_stats[harvester_id][job_type][resource_type]["running"] * core_factor

                    # This limit is in #JOBS or #WORKERS, not in #CORES
                    if resource_type in resource_type_limits:
                        resource_type_limits[resource_type] = (
                            resource_type_limits[resource_type] - worker_stats[harvester_id][job_type][resource_type]["running"]
                        )
                        tmp_log.debug(f"Limit for rt {resource_type} down to {resource_type_limits[resource_type]}")

                    # This limit is in #CORES, since it mixes single and multi core jobs
                    if get_entity_module(self).resource_spec_mapper.is_high_memory(resource_type) and HIMEM in resource_type_limits:
                        resource_type_limits[HIMEM] = resource_type_limits[HIMEM] - worker_stats[harvester_id][job_type][resource_type]["running"] * core_factor
                        tmp_log.debug(f"Limit for rt group {HIMEM} down to {resource_type_limits[HIMEM]}")

                except KeyError:
                    pass

                try:  # submitted
                    workers_queued[job_type].setdefault(resource_type, 0)
                    workers_queued[job_type][resource_type] = (
                        workers_queued[job_type][resource_type] + worker_stats[harvester_id][job_type][resource_type]["submitted"]
                    )
                    n_cores_queued = n_cores_queued + worker_stats[harvester_id][job_type][resource_type]["submitted"] * core_factor
                except KeyError:
                    pass

                try:  # ready
                    workers_queued[job_type].setdefault(resource_type, 0)
                    workers_queued[job_type][resource_type] = (
                        workers_queued[job_type][resource_type] + worker_stats[harvester_id][job_type][resource_type]["ready"]
                    )
                    n_cores_queued = n_cores_queued + worker_stats[harvester_id][job_type][resource_type]["ready"] * core_factor
                except KeyError:
                    pass

        tmp_log.debug(f"Queue {queue} queued worker overview: {workers_queued}")

        # For queues that need more pressure towards reaching a target
        n_cores_running_fake = 0
        try:
            if pq_data_des["status"] in [
                "online",
                "brokeroff",
            ]:  # don't flood test sites with workers
                n_cores_running_fake = pq_data_des["params"]["ups_core_target"]
                tmp_log.debug(f"Using ups_core_target {n_cores_running_fake} for queue {queue}")
        except KeyError:  # no value defined in CRIC
            pass

        n_cores_running = max(n_cores_running, n_cores_running_fake)

        n_cores_target = max(int(n_cores_running * 0.4), 75 * cores_queue)
        n_cores_to_submit = max(n_cores_target - n_cores_queued, 5 * cores_queue)
        tmp_log.debug(f"IN CORES: nrunning {n_cores_running}, ntarget {n_cores_target}, nqueued {n_cores_queued}. We need to process {n_cores_to_submit} cores")

        # Get the sorted global shares
        sorted_shares = get_entity_module(self).get_sorted_leaves()

        # Run over the activated jobs by gshare & priority, and subtract them from the queued
        # A negative value for queued will mean more pilots of that resource type are missing
        for share in sorted_shares:
            var_map = {":queue": queue, ":gshare": share.name}
            sql = (
                f"SELECT gshare, prodsourcelabel, resource_type FROM {panda_config.schemaPANDA}.jobsactive4 "
                "WHERE jobstatus = 'activated' "
                "AND computingsite=:queue "
                "AND gshare=:gshare "
            )

            # if we need to filter on resource types
            if resource_types_under_target:
                rtype_var_names_str, rtype_var_map = get_sql_IN_bind_variables(resource_types_under_target, prefix=":", value_as_suffix=True)
                sql += f"   AND resource_type IN ({rtype_var_names_str}) "
                var_map.update(rtype_var_map)

            sql += "ORDER BY currentpriority DESC"
            self.cur.execute(sql + comment, var_map)
            activated_jobs = self.cur.fetchall()

            tmp_log.debug(f"Processing share: {share.name}. Got {len(activated_jobs)} activated jobs")
            for gshare, prodsourcelabel, resource_type in activated_jobs:
                core_factor = get_entity_module(self).resource_spec_mapper.translate_resourcetype_to_cores(resource_type, cores_queue)

                # translate prodsourcelabel to a subset of job types, typically 'user' and 'managed'
                # Harvester worker submission unified production and analysis proxies, so we don't need to separate by job_type anymore
                job_type = DEFAULT_PRODSOURCELABEL
                # if we reached the limit for the resource type, skip the job
                if resource_type in resource_type_limits and resource_type_limits[resource_type] <= 0:
                    # tmp_log.debug('Reached resource type limit for {0}'.format(resource_type))
                    continue

                # if we reached the limit for the HIMEM resource type group, skip the job
                if (
                    get_entity_module(self).resource_spec_mapper.is_high_memory(resource_type)
                    and HIMEM in resource_type_limits
                    and resource_type_limits[HIMEM] <= 0
                ):
                    # tmp_log.debug('Reached resource type limit for {0}'.format(resource_type))
                    continue

                workers_queued.setdefault(job_type, {})
                workers_queued[job_type].setdefault(resource_type, 0)
                workers_queued[job_type][resource_type] = workers_queued[job_type][resource_type] - 1
                if workers_queued[job_type][resource_type] <= 0:
                    # we've gone over the jobs that already have a queued worker, now we go for new workers
                    n_cores_to_submit = n_cores_to_submit - core_factor

                # We reached the number of workers needed
                if n_cores_to_submit <= 0:
                    tmp_log.debug("Reached cores needed (inner)")
                    break

            # We reached the number of workers needed
            if n_cores_to_submit <= 0:
                tmp_log.debug("Reached cores needed (outer)")
                break

        tmp_log.debug(f"workers_queued: {workers_queued}")

        new_workers = {}
        for job_type in workers_queued:
            new_workers.setdefault(job_type, {})
            for resource_type in workers_queued[job_type]:
                if workers_queued[job_type][resource_type] >= 0:
                    # we have too many workers queued already, don't submit more
                    new_workers[job_type][resource_type] = 0
                elif workers_queued[job_type][resource_type] < 0:
                    # we don't have enough workers for this resource type
                    new_workers[job_type][resource_type] = -workers_queued[job_type][resource_type] + 1

        tmp_log.debug(f"preliminary new workers: {new_workers}")

        # We should still submit a basic worker, even if there are no activated jobs to avoid queue deactivation
        workers = False
        for job_type in new_workers:
            for resource_type in new_workers[job_type]:
                if new_workers[job_type][resource_type] > 0:
                    workers = True
                    break
        if not workers:
            new_workers[DEFAULT_PRODSOURCELABEL] = {BASIC_RESOURCE_TYPE: 1}

        tmp_log.debug(f"new workers: {new_workers}")

        new_workers_per_harvester = {harvester_id: new_workers}

        tmp_log.debug(f"Workers to submit: {new_workers_per_harvester}")
        # commit for postgres to avoid idle transactions
        if not self._commit():
            raise RuntimeError("Commit error")
        tmp_log.debug("done")
        return new_workers_per_harvester

    # add command lock
    def addCommandLockHarvester(self, harvester_ID, command, computingSite, resourceType, useCommit=True):
        comment = " /* DBProxy.addCommandLockHarvester */"
        tmp_log = self.create_tagged_logger(comment, f"harvesterID={harvester_ID} command={command} site={computingSite}> resource={resourceType}")
        tmp_log.debug("start")
        try:
            # check if lock is available
            sql_check_lock = (
                "SELECT 1 FROM ATLAS_PANDA.Harvester_Command_Lock "
                "WHERE harvester_ID=:harvester_ID AND computingSite=:siteName AND resourceType=:resourceType AND command=:command "
            )

            # sql to add lock
            sql_add_lock = (
                "INSERT INTO ATLAS_PANDA.Harvester_Command_Lock "
                "(harvester_ID, computingSite, resourceType, command,lockedTime) "
                "VALUES (:harvester_ID, :siteName, :resourceType, :command, CURRENT_DATE-1) "
            )
            if useCommit:
                self.conn.begin()
            # check
            var_map = {
                ":harvester_ID": harvester_ID,
                ":siteName": computingSite,
                ":resourceType": resourceType,
                ":command": command,
            }
            self.cur.execute(sql_check_lock + comment, var_map)
            existing_lock = self.cur.fetchone()
            if existing_lock is None:
                # add lock
                self.cur.execute(sql_add_lock + comment, var_map)
            # commit
            if useCommit:
                if not self._commit():
                    raise RuntimeError("Commit error")
            tmp_log.debug("done")
            return True
        except Exception:
            # roll back
            if useCommit:
                self._rollback()
            self.dump_error_message(tmp_log)
            return False

    # get command locks
    def getCommandLocksHarvester(self, harvester_ID, command, lockedBy, lockInterval, commandInterval):
        comment = " /* DBProxy.getCommandLocksHarvester */"
        tmp_log = self.create_tagged_logger(comment, f"harvesterID={harvester_ID} command={command}")
        tmp_log.debug("start")
        try:
            time_now = naive_utcnow()
            # sql to get commands
            sql_get_locks = (
                "SELECT computingSite,resourceType FROM ATLAS_PANDA.Harvester_Command_Lock "
                "WHERE harvester_ID=:harvester_ID AND command=:command "
                "AND ((lockedBy IS NULL AND lockedTime<:limitComm) OR (lockedBy IS NOT NULL AND lockedTime<:limitLock)) "
                "FOR UPDATE "
            )

            # sql to lock command
            sql_lock_command = (
                "UPDATE ATLAS_PANDA.Harvester_Command_Lock SET lockedBy=:lockedBy,lockedTime=CURRENT_DATE "
                "WHERE harvester_ID=:harvester_ID AND command=:command AND computingSite=:siteName AND resourceType=:resourceType "
                "AND ((lockedBy IS NULL AND lockedTime<:limitComm) OR (lockedBy IS NOT NULL AND lockedTime<:limitLock)) "
            )
            # get commands
            self.conn.begin()
            self.cur.arraysize = 10000
            var_map = {
                ":harvester_ID": harvester_ID,
                ":command": command,
                ":limitComm": time_now - datetime.timedelta(minutes=commandInterval),
                ":limitLock": time_now - datetime.timedelta(minutes=lockInterval),
            }
            self.cur.execute(sql_get_locks + comment, var_map)
            rows = self.cur.fetchall()
            # lock commands
            result_map = dict()
            for computing_site, resource_type in rows:
                var_map = {
                    ":harvester_ID": harvester_ID,
                    ":command": command,
                    ":siteName": computing_site,
                    ":resourceType": resource_type,
                    ":limitComm": time_now - datetime.timedelta(minutes=commandInterval),
                    ":limitLock": time_now - datetime.timedelta(minutes=lockInterval),
                    ":lockedBy": lockedBy,
                }
                self.cur.execute(sql_lock_command + comment, var_map)
                n_row = self.cur.rowcount
                if n_row > 0:
                    if computing_site not in result_map:
                        result_map[computing_site] = []
                    result_map[computing_site].append(resource_type)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmp_log.debug(f"done with {str(result_map)}")
            return result_map
        except Exception:
            # roll back
            self._rollback()
            self.dump_error_message(tmp_log)
            return {}

    # release command lock
    def releaseCommandLockHarvester(self, harvester_ID, command, computingSite, resourceType, lockedBy):
        comment = " /* DBProxy.releaseCommandLockHarvester */"
        tmp_log = self.create_tagged_logger(
            comment, f"harvesterID={harvester_ID} com={command} site={computingSite} resource={resourceType} lockedBy={lockedBy}"
        )
        tmp_log.debug("start")
        try:
            # sql to release lock
            sql_release_lock = (
                "UPDATE ATLAS_PANDA.Harvester_Command_Lock SET lockedBy=NULL "
                "WHERE harvester_ID=:harvester_ID AND command=:command "
                "AND computingSite=:computingSite AND resourceType=:resourceType AND lockedBy=:lockedBy "
            )

            var_map = {
                ":harvester_ID": harvester_ID,
                ":command": command,
                ":computingSite": computingSite,
                ":resourceType": resourceType,
                ":lockedBy": lockedBy,
            }

            # release lock
            self.conn.begin()
            self.cur.execute(sql_release_lock + comment, var_map)
            n_row = self.cur.rowcount
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmp_log.debug(f"done with {n_row}")
            return True
        except Exception:
            # roll back
            self._rollback()
            self.dump_error_message(tmp_log)
            return False

    # heartbeat for harvester
    def harvesterIsAlive(self, user, host, harvesterID, data):
        """
        update harvester instance information
        """
        comment = " /* DBProxy.harvesterIsAlive */"
        tmp_log = self.create_tagged_logger(comment, f"harvesterID={harvesterID}")
        tmp_log.debug("start")
        try:

            # update
            owner = CoreUtils.clean_user_id(user)
            var_map = {":harvesterID": harvesterID, ":owner": owner, ":hostName": host}
            sql_update_instance = "UPDATE ATLAS_PANDA.Harvester_Instances SET owner=:owner,hostName=:hostName,lastUpdate=CURRENT_DATE"
            for key in data:
                val = data[key]
                if key == "commands":
                    continue
                sql_update_instance += ",{0}=:{0}".format(key)
                if isinstance(val, str) and val.startswith("datetime/"):
                    val = datetime.datetime.strptime(val.split("/")[-1], "%Y-%m-%d %H:%M:%S.%f")
                var_map[f":{key}"] = val
            sql_update_instance += " WHERE harvester_ID=:harvesterID "
            # exec
            self.conn.begin()
            self.cur.execute(sql_update_instance + comment, var_map)
            n_row = self.cur.rowcount
            if n_row == 0:
                # insert instance info
                var_map = {
                    ":harvesterID": harvesterID,
                    ":owner": owner,
                    ":hostName": host,
                    ":descr": "automatic",
                }
                sql_insert_instance = (
                    "INSERT INTO ATLAS_PANDA.Harvester_Instances "
                    "(harvester_ID,owner,hostName,lastUpdate,description) "
                    "VALUES(:harvesterID,:owner,:hostName,CURRENT_DATE,:descr) "
                )
                self.cur.execute(sql_insert_instance + comment, var_map)
            # insert command locks
            if "commands" in data:
                for item in data["commands"]:
                    self.addCommandLockHarvester(
                        harvesterID,
                        item["command"],
                        item["computingSite"],
                        item["resourceType"],
                        False,
                    )
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmp_log.debug("done")
            if n_row == 0:
                ret_str = "no instance record"
            else:
                ret_str = "succeeded"
            return ret_str
        except Exception:
            # roll back
            self._rollback()
            self.dump_error_message(tmp_log)
            return None

    # update workers
    def updateWorkers(self, harvesterID, data, useCommit=True):
        """
        Update workers
        """
        comment = " /* DBProxy.updateWorkers */"
        tmp_log = self.create_tagged_logger(comment, f"harvesterID={harvesterID} pid={os.getpid()}")
        try:
            tmp_log.debug(f"start {len(data)} workers")
            reg_start = naive_utcnow()
            sql_check_worker = f"SELECT {WorkerSpec.columnNames()} FROM ATLAS_PANDA.Harvester_Workers WHERE harvesterID=:harvesterID AND workerID=:workerID "
            # loop over all workers
            ret_list = []
            for worker_data in data:
                time_now = naive_utcnow()
                if useCommit:
                    self.conn.begin()
                worker_spec = WorkerSpec()
                worker_spec.harvesterID = harvesterID
                worker_spec.workerID = worker_data["workerID"]
                tmp_log.debug(f"workerID={worker_spec.workerID} start")
                # check if already exists
                var_map = {
                    ":harvesterID": worker_spec.harvesterID,
                    ":workerID": worker_spec.workerID,
                }
                self.cur.execute(sql_check_worker + comment, var_map)
                existing_worker = self.cur.fetchone()
                if existing_worker is None:
                    # not exist
                    to_insert = True
                    old_last_update = None
                else:
                    # already exists
                    to_insert = False
                    worker_spec.pack(existing_worker)
                    old_last_update = worker_spec.lastUpdate
                # set new values
                old_status = worker_spec.status
                for key in worker_data:
                    val = worker_data[key]
                    if hasattr(worker_spec, key):
                        setattr(worker_spec, key, val)
                worker_spec.lastUpdate = time_now
                if old_status in ["finished", "failed", "cancelled", "missed"] and (
                    old_last_update is not None and old_last_update > time_now - datetime.timedelta(hours=3)
                ):
                    tmp_log.debug(f"workerID={worker_spec.workerID} keep old status={old_status} instead of new {worker_spec.status}")
                    worker_spec.status = old_status
                # insert or update
                if to_insert:
                    # insert
                    tmp_log.debug(f"workerID={worker_spec.workerID} insert for status={worker_spec.status}")
                    sql_insert_worker = f"INSERT INTO ATLAS_PANDA.Harvester_Workers ({WorkerSpec.columnNames()}) "
                    sql_insert_worker += WorkerSpec.bindValuesExpression()
                    var_map = worker_spec.valuesMap()
                    self.cur.execute(sql_insert_worker + comment, var_map)
                else:
                    # update
                    tmp_log.debug(f"workerID={worker_spec.workerID} update for status={worker_spec.status}")
                    sql_update_worker = (
                        f"UPDATE ATLAS_PANDA.Harvester_Workers SET {worker_spec.bindUpdateChangesExpression()} "
                        f"WHERE harvesterID=:harvesterID AND workerID=:workerID "
                    )
                    var_map = worker_spec.valuesMap(onlyChanged=True)
                    self.cur.execute(sql_update_worker + comment, var_map)
                # job relation
                if "pandaid_list" in worker_data and len(worker_data["pandaid_list"]) > 0:
                    tmp_log.debug(f"workerID={worker_spec.workerID} update/insert job relation")
                    sql_get_job_rels = "SELECT PandaID FROM ATLAS_PANDA.Harvester_Rel_Jobs_Workers WHERE harvesterID=:harvesterID AND workerID=:workerID "
                    sql_insert_job_rel = (
                        "INSERT INTO ATLAS_PANDA.Harvester_Rel_Jobs_Workers (harvesterID,workerID,PandaID,lastUpdate) "
                        "VALUES (:harvesterID,:workerID,:PandaID,:lastUpdate) "
                    )
                    sql_update_job_rel = (
                        "UPDATE ATLAS_PANDA.Harvester_Rel_Jobs_Workers SET lastUpdate=:lastUpdate "
                        "WHERE harvesterID=:harvesterID AND workerID=:workerID AND PandaID=:PandaID "
                    )
                    # get jobs
                    var_map = {
                        ":harvesterID": harvesterID,
                        ":workerID": worker_data["workerID"],
                    }
                    self.cur.execute(sql_get_job_rels + comment, var_map)
                    existing_job_rels = self.cur.fetchall()
                    existing_panda_ids = set()
                    for (panda_id,) in existing_job_rels:
                        existing_panda_ids.add(panda_id)
                    for panda_id in worker_data["pandaid_list"]:
                        # update or insert
                        var_map = {
                            ":harvesterID": harvesterID,
                            ":workerID": worker_data["workerID"],
                            ":PandaID": panda_id,
                            ":lastUpdate": time_now,
                        }
                        if panda_id not in existing_panda_ids:
                            # insert
                            self.cur.execute(sql_insert_job_rel + comment, var_map)
                        else:
                            # update
                            self.cur.execute(sql_update_job_rel + comment, var_map)
                            existing_panda_ids.discard(panda_id)
                    # delete redundant list
                    sql_delete_job_rel = (
                        "DELETE FROM ATLAS_PANDA.Harvester_Rel_Jobs_Workers WHERE harvesterID=:harvesterID AND workerID=:workerID AND PandaID=:PandaID "
                    )
                    for panda_id in existing_panda_ids:
                        var_map = {
                            ":PandaID": panda_id,
                            ":harvesterID": harvesterID,
                            ":workerID": worker_data["workerID"],
                        }
                        self.cur.execute(sql_delete_job_rel + comment, var_map)
                    tmp_log.debug(f"workerID={worker_spec.workerID} deleted {len(existing_panda_ids)} jobs")
                # comprehensive heartbeat
                tmp_log.debug(f"workerID={worker_spec.workerID} get jobs")
                sql_get_active_jobs = (
                    "SELECT r.PandaID FROM "
                    "ATLAS_PANDA.Harvester_Rel_Jobs_Workers r,ATLAS_PANDA.jobsActive4 j  "
                    "WHERE r.harvesterID=:harvesterID AND r.workerID=:workerID "
                    "AND j.PandaID=r.PandaID AND NOT j.jobStatus IN (:holding) "
                )

                sql_get_job_status = "SELECT jobStatus, prodSourceLabel, attemptNr FROM ATLAS_PANDA.jobsActive4 WHERE PandaID=:PandaID "

                sql_set_job_error = (
                    "UPDATE ATLAS_PANDA.jobsActive4 SET taskBufferErrorCode=:code,taskBufferErrorDiag=:diag,"
                    "startTime=(CASE WHEN jobStatus=:starting THEN NULL ELSE startTime END) "
                    "WHERE PandaID=:PandaID "
                )

                # the table name will be inserted at execution time
                sql_set_sup_error = (
                    "UPDATE {0} SET supErrorCode=:code,supErrorDiag=:diag,stateChangeTime=CURRENT_DATE "
                    "WHERE PandaID=:PandaID AND NOT jobStatus IN (:finished) AND modificationTime>CURRENT_DATE-30"
                )

                var_map = {
                    ":harvesterID": harvesterID,
                    ":workerID": worker_data["workerID"],
                    ":holding": "holding",
                }
                self.cur.execute(sql_get_active_jobs + comment, var_map)
                active_job_rows = self.cur.fetchall()
                tmp_log.debug(f"workerID={worker_spec.workerID} update {len(active_job_rows)} jobs")
                for (panda_id,) in active_job_rows:
                    # check job status when worker is in a final state
                    if worker_spec.status in [
                        "finished",
                        "failed",
                        "cancelled",
                        "missed",
                    ]:
                        var_map = {
                            ":PandaID": panda_id,
                        }
                        self.cur.execute(sql_get_job_status + comment, var_map)
                        job_status_row = self.cur.fetchone()
                        if job_status_row is not None:
                            job_status, prod_source_label, attempt_nr = job_status_row
                            tmp_log.debug(f"workerID={worker_spec.workerID} {worker_spec.status} while PandaID={panda_id} {job_status}")
                            # set failed if out of sync
                            if "syncLevel" in worker_data and worker_data["syncLevel"] == 1 and job_status in ["running", "starting"]:
                                tmp_log.debug(f"workerID={worker_spec.workerID} set failed to PandaID={panda_id} due to sync error")
                                var_map = {
                                    ":PandaID": panda_id,
                                    ":code": ErrorCode.EC_WorkerDone,
                                    ":starting": "starting",
                                    ":diag": f"The worker was {worker_spec.status} while the job was {job_status} : {worker_spec.diagMessage}",
                                }
                                var_map[":diag"] = JobSpec.truncateStringAttr("taskBufferErrorDiag", var_map[":diag"])
                                self.cur.execute(sql_set_job_error + comment, var_map)
                                # make an empty file to trigger registration for zip files in Adder
                                # tmpFileName = '{0}_{1}_{2}'.format(panda_id, 'failed',
                                #                                    uuid.uuid3(uuid.NAMESPACE_DNS,''))
                                # tmpFileName = os.path.join(panda_config.logdir, tmpFileName)
                                # try:
                                #     open(tmpFileName, 'w').close()
                                # except Exception:
                                #     pass
                                # sql to insert empty job output report for adder
                                sql_insert_job_report = (
                                    f"INSERT INTO {panda_config.schemaPANDA}.Job_Output_Report "
                                    "(PandaID, prodSourceLabel, jobStatus, attemptNr, data, timeStamp) "
                                    "VALUES(:PandaID, :prodSourceLabel, :jobStatus, :attemptNr, :data, :timeStamp) "
                                )

                                # insert
                                var_map = {
                                    ":PandaID": panda_id,
                                    ":prodSourceLabel": prod_source_label,
                                    ":jobStatus": "failed",
                                    ":attemptNr": attempt_nr,
                                    ":data": None,
                                    ":timeStamp": naive_utcnow(),
                                }
                                try:
                                    self.cur.execute(sql_insert_job_report + comment, var_map)
                                except Exception:
                                    pass
                                else:
                                    tmp_log.debug(f"successfully inserted job output report {panda_id}.{var_map[':attemptNr']}")
                        if worker_spec.errorCode not in [None, 0]:
                            var_map = {
                                ":PandaID": panda_id,
                                ":code": worker_spec.errorCode,
                                ":diag": f"Diag from worker : {worker_spec.diagMessage}",
                                ":finished": "finished",
                            }
                            var_map[":diag"] = JobSpec.truncateStringAttr("supErrorDiag", var_map[":diag"])
                            for table_name in [
                                "ATLAS_PANDA.jobsActive4",
                                "ATLAS_PANDA.jobsArchived4",
                                "ATLAS_PANDAARCH.jobsArchived",
                            ]:
                                self.cur.execute(sql_set_sup_error.format(table_name) + comment, var_map)

                tmp_log.debug(f"workerID={worker_spec.workerID} end")
                # commit
                if useCommit:
                    if not self._commit():
                        raise RuntimeError("Commit error")
                ret_list.append(True)
            reg_time = naive_utcnow() - reg_start
            tmp_log.debug(f"done. exec_time={reg_time.seconds}.{reg_time.microseconds // 1000:03d} sec")
            return ret_list
        except Exception:
            # roll back
            if useCommit:
                self._rollback()
            self.dump_error_message(tmp_log)
            return None

    # update the worker status as seen by the pilot
    def updateWorkerPilotStatus(self, workerID, harvesterID, status, node_id):
        comment = " /* DBProxy.updateWorkerPilotStatus */"
        tmp_log = self.create_tagged_logger(comment, f"harvesterID={harvesterID} workerID={workerID}")

        timestamp_utc = naive_utcnow()
        var_map = {
            ":status": status,
            ":harvesterID": harvesterID,
            ":workerID": workerID,
            ":nodeID": node_id,
        }
        sql = "UPDATE ATLAS_PANDA.harvester_workers SET pilotStatus=:status,nodeID=:nodeID "

        tmp_log.debug(f"Updating to status={status} nodeID={node_id} at {timestamp_utc}")

        # add the start or end time
        if status == "started":
            sql += ", pilotStartTime=:now "
            var_map[":now"] = timestamp_utc
        elif status == "finished":
            sql += ", pilotEndTime=:now "
            var_map[":now"] = timestamp_utc

        sql += "WHERE workerID=:workerID AND harvesterID=:harvesterID "

        try:
            self.conn.begin()
            self.cur.execute(sql + comment, var_map)
            n_rows = self.cur.rowcount
            if not self._commit():
                raise RuntimeError("Commit error")
            tmp_log.debug(f"Updated successfully with {n_rows}")
            return True

        except Exception:
            # roll back
            self._rollback(True)
            self.dump_error_message(tmp_log)
            return False

    def update_worker_node(
        self,
        site,
        panda_queue,
        host_name,
        cpu_model,
        cpu_model_normalized,
        n_logical_cpus,
        n_sockets,
        cores_per_socket,
        threads_per_core,
        cpu_architecture,
        cpu_architecture_level,
        clock_speed,
        total_memory,
        total_local_disk,
    ):
        comment = " /* DBProxy.update_worker_node */"
        method_name = comment.split(" ")[-2].split(".")[-1]

        tmp_logger = self.create_tagged_logger(comment, f"{method_name} < site={site} panda_queue={panda_queue} host_name={host_name} cpu_model={cpu_model} >")
        tmp_logger.debug("Start")

        timestamp_utc = naive_utcnow()

        # clean up host name from any prefixes
        host_name = clean_host_name(host_name)

        locked_site = True  # Track whether the worker node was locked at site level by another pilot update
        locked_queue = True  # Track whether the worker node was locked at queue level by another pilot update

        try:
            # update the worker node at site level first
            self.conn.begin()

            # Select the worker node to see if it exists in the database
            var_map = {":site": site, ":host_name": host_name, ":cpu_model": cpu_model}

            sql = (
                "SELECT site, host_name, cpu_model "
                "FROM ATLAS_PANDA.worker_node "
                "WHERE site=:site AND host_name=:host_name AND cpu_model=:cpu_model "
                "FOR UPDATE NOWAIT"
            )

            self.cur.execute((sql + comment), var_map)
            res = self.cur.fetchone()
            locked_site = False  # If the row was locked, the NOWAIT clause will make the query except and go to the end

            # The worker node entry exists, we update the worker node's last_seen timestamp
            if res:
                var_map = {":site": site, ":host_name": host_name, ":cpu_model": cpu_model, ":last_seen": timestamp_utc}

                sql = "UPDATE ATLAS_PANDA.worker_node SET last_seen=:last_seen WHERE site=:site AND host_name=:host_name AND cpu_model=:cpu_model"

                self.cur.execute((sql + comment), var_map)
                tmp_logger.debug("Worker node was found in the database. Updated last_seen timestamp.")
                if not self._commit():
                    raise RuntimeError("Commit error")

            else:
                # The worker node entry did not exist, we insert it as a new worker node
                var_map = {
                    ":site": site,
                    ":host_name": host_name,
                    ":cpu_model": cpu_model,
                    ":cpu_model_normalized": cpu_model_normalized,
                    ":n_logical_cpus": n_logical_cpus,
                    "n_sockets": n_sockets,
                    ":cores_per_socket": cores_per_socket,
                    ":threads_per_core": threads_per_core,
                    ":cpu_architecture": cpu_architecture,
                    ":cpu_architecture_level": cpu_architecture_level,
                    ":clock_speed": clock_speed,
                    ":total_memory": total_memory,
                    ":total_local_disk": total_local_disk,
                    ":last_seen": timestamp_utc,
                }

                sql = (
                    "INSERT INTO ATLAS_PANDA.worker_node "
                    "(site, host_name, cpu_model, cpu_model_normalized, n_logical_cpus, n_sockets, cores_per_socket, threads_per_core, "
                    "cpu_architecture, cpu_architecture_level, clock_speed, total_memory, total_local_disk, last_seen) "
                    "VALUES "
                    "(:site, :host_name, :cpu_model, :cpu_model_normalized, :n_logical_cpus, :n_sockets, :cores_per_socket, :threads_per_core, "
                    ":cpu_architecture, :cpu_architecture_level, :clock_speed, :total_memory, :total_local_disk, :last_seen)"
                )

                self.cur.execute(sql + comment, var_map)
                if not self._commit():
                    raise RuntimeError("Commit error")
                tmp_logger.debug("Inserted as new worker node.")

            if panda_queue:
                # now update the worker node at queue level
                self.conn.begin()

                # Select the worker node to see if it exists in the queue table
                var_map = {":site": site, ":host_name": host_name, ":panda_queue": panda_queue}

                sql = (
                    "SELECT site, host_name, panda_queue "
                    "FROM ATLAS_PANDA.worker_node_queue "
                    "WHERE site=:site AND host_name=:host_name AND panda_queue=:panda_queue "
                    "FOR UPDATE NOWAIT"
                )
                self.cur.execute((sql + comment), var_map)
                res = self.cur.fetchone()
                locked_queue = False  # If the row was locked, the NOWAIT clause will make the query except and go to the end

                # The worker node entry exists at queue level, we update the worker node's last_seen timestamp
                if res:
                    var_map = {":site": site, ":host_name": host_name, ":panda_queue": panda_queue, ":last_seen": timestamp_utc}

                    sql = (
                        "UPDATE ATLAS_PANDA.worker_node_queue SET last_seen=:last_seen "
                        "WHERE site=:site AND host_name=:host_name AND panda_queue=:panda_queue"
                    )

                    self.cur.execute((sql + comment), var_map)
                    tmp_logger.debug("Worker node was found in the wn-queue table. Updated last_seen timestamp.")
                    if not self._commit():
                        raise RuntimeError("Commit error")

                else:
                    # The worker node entry did not exist at queue level, we insert it
                    var_map = {
                        ":site": site,
                        ":host_name": host_name,
                        ":panda_queue": panda_queue,
                        ":last_seen": timestamp_utc,
                    }

                    sql = (
                        "INSERT INTO ATLAS_PANDA.worker_node_queue "
                        "(site, host_name, panda_queue, last_seen) "
                        "VALUES "
                        "(:site, :host_name, :panda_queue, :last_seen)"
                    )

                    self.cur.execute(sql + comment, var_map)
                    if not self._commit():
                        raise RuntimeError("Commit error")
                    tmp_logger.debug("Inserted as new worker node.")

            tmp_logger.debug("Done.")
            return True, "Inserted new worker node."

        except Exception:
            # Always roll back the transaction
            self._rollback(True)

            # Failed because of the NOWAIT clause
            if locked_site or locked_queue:
                return True, "Another pilot was updating the worker node at the same time."

            # General failure
            err_type, err_value = sys.exc_info()[:2]
            error_message = f"Worker node update failed with {err_type} {err_value}"
            tmp_logger.error(error_message)
            return False, error_message

    def update_worker_node_gpu(
        self,
        site: str,
        host_name: str,
        vendor: str,
        model: str,
        count: int,
        vram: int,
        architecture: str,
        framework: str,
        framework_version: str,
        driver_version: str,
    ):
        comment = " /* DBProxy.update_worker_node_gpu */"
        method_name = comment.split(" ")[-2].split(".")[-1]

        tmp_logger = self.create_tagged_logger(comment, f"{method_name} < site={site} host_name={host_name} vendor={vendor} model={model} >")
        tmp_logger.debug("Start")

        timestamp_utc = naive_utcnow()

        # clean up host name from any prefixes
        host_name = clean_host_name(host_name)

        locked = True  # Track whether the worker node was locked by another pilot update

        try:
            self.conn.begin()

            # Select the GPU to see if it exists in the database
            var_map = {":site": site, ":host_name": host_name, ":vendor": vendor, ":model": model}

            sql = (
                "SELECT site, host_name, vendor, model "
                "FROM ATLAS_PANDA.worker_node_gpus "
                "WHERE site=:site AND host_name=:host_name AND vendor=:vendor AND model=:model "
                "FOR UPDATE NOWAIT"
            )

            self.cur.execute((sql + comment), var_map)
            res = self.cur.fetchone()
            locked = False  # If the row was locked, the NOWAIT clause will make the query except and go to the end

            # The worker node GPU entry exists, we update the worker node's last_seen timestamp, count, framework, and framework_version
            if res:
                var_map = {
                    ":site": site,
                    ":host_name": host_name,
                    ":vendor": vendor,
                    ":model": model,
                    ":framework": framework,
                    ":framework_version": framework_version,
                    ":count": count,
                    ":last_seen": timestamp_utc,
                }

                sql = (
                    "UPDATE ATLAS_PANDA.worker_node_gpus SET last_seen=:last_seen, framework=:framework, framework_version=:framework_version, count=:count "
                    "WHERE site=:site AND host_name=:host_name AND vendor=:vendor AND model=:model"
                )

                self.cur.execute((sql + comment), var_map)
                tmp_logger.debug("Worker node GPU was found in the database. Updated last_seen timestamp and count.")
                if not self._commit():
                    raise RuntimeError("Commit error")

                return True, "Updated existing worker node GPU."

            # The worker node GPU entry did not exist, we insert it as a new worker node GPU
            var_map = {
                ":site": site,
                ":host_name": host_name,
                ":vendor": vendor,
                ":model": model,
                ":count": count,
                ":vram": vram,
                ":architecture": architecture,
                ":framework": framework,
                ":framework_version": framework_version,
                ":driver_version": driver_version,
                ":last_seen": timestamp_utc,
            }

            sql = (
                "INSERT INTO ATLAS_PANDA.worker_node_gpus "
                "(site, host_name, vendor, model, count, vram, architecture, framework, framework_version, driver_version, last_seen) "
                "VALUES "
                "(:site, :host_name, :vendor, :model, :count, :vram, :architecture, :framework, :framework_version, :driver_version, :last_seen)"
            )

            self.cur.execute(sql + comment, var_map)
            if not self._commit():
                raise RuntimeError("Commit error")
            tmp_logger.debug("Inserted as new worker node GPU.")

            return True, "Inserted new worker node GPU."

        except Exception:
            # Always roll back the transaction
            self._rollback(True)

            # Failed because of the NOWAIT clause
            if locked:
                return False, "Another pilot was updating the worker node GPU at the same time."

            # General failure
            err_type, err_value = sys.exc_info()[:2]
            error_message = f"Worker node GPU update failed with {err_type} {err_value}"
            tmp_logger.error(error_message)
            return False, error_message

    @memoize
    def get_architecture_level_map(self):
        comment = " /* DBProxy.get_architecture_level_map */"
        tmp_log = self.create_tagged_logger(comment)
        tmp_log.debug("Start")

        try:
            sql = "SELECT panda_queue, cpu_architecture_level, total_logical_cpus, pct_within_queue FROM ATLAS_PANDA.MV_WORKER_NODE_SUMMARY "
            self.cur.execute(sql + comment, {})
            results = self.cur.fetchall()

            tmp_log.debug(f"Got {len(results)} entries from MV_WORKER_NODE_SUMMARY")

            # Build a queue dictionary with the results
            architecture_map = {}
            for result in results:
                site, panda_queue, cpu_architecture_level, total_logical_cpus, pct_within_queue = result
                architecture_map.setdefault(panda_queue, {})
                architecture_map[panda_queue][cpu_architecture_level] = {
                    "total_logical_cpus": total_logical_cpus,
                    "pct_within_queue": pct_within_queue,
                }

            tmp_log.debug(f"Done")
            return architecture_map

        except Exception:
            self.dump_error_message(tmp_log)
            return {}

    # get workers for a job
    def getWorkersForJob(self, PandaID):
        comment = " /* DBProxy.getWorkersForJob */"
        tmp_log = self.create_tagged_logger(comment, f"PandaID={PandaID}")
        tmp_log.debug("start")
        try:
            # sql to get workers
            sql_get_workers = (
                f"SELECT {WorkerSpec.columnNames(prefix='w')} FROM ATLAS_PANDA.Harvester_Workers w, ATLAS_PANDA.Harvester_Rel_Jobs_Workers r "
                "WHERE w.harvesterID=r.harvesterID AND w.workerID=r.workerID AND r.PandaID=:PandaID "
            )
            var_map = {":PandaID": PandaID}

            # start transaction
            self.conn.begin()
            self.cur.execute(sql_get_workers + comment, var_map)
            worker_rows = self.cur.fetchall()
            ret_list = []
            for worker_row in worker_rows:
                worker_spec = WorkerSpec()
                worker_spec.pack(worker_row)
                ret_list.append(worker_spec)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmp_log.debug(f"got {len(ret_list)} workers")
            return ret_list
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return []

    # get workers with stale harvester states and newer pilot state
    def get_workers_to_synchronize(self):
        comment = " /* DBProxy.get_workers_to_synchronize */"
        tmp_log = self.create_tagged_logger(comment)
        try:
            tmp_log.debug("Starting")

            # give harvester a chance to discover the status change itself
            discovery_period = naive_utcnow() - datetime.timedelta(minutes=60)
            # don't repeat the same workers in each cycle
            retry_period = naive_utcnow() - datetime.timedelta(minutes=30)

            # Select workers where the status is more advanced according to the pilot than to harvester
            sql_select = (
                "SELECT /*+ INDEX_RS_ASC(harvester_workers HARVESTER_WORKERS_STATUS_IDX) */ harvesterID, workerID, pilotStatus "
                "FROM ATLAS_PANDA.harvester_workers "
                "WHERE (status in ('submitted', 'ready') AND pilotStatus='running' AND pilotStartTime < :discovery_period) "
                "OR (status in ('submitted', 'ready', 'running', 'idle') AND pilotStatus='finished' AND pilotEndTime < :discovery_period) "
                "AND lastupdate > sysdate - interval '7' day "
                "AND submittime > sysdate - interval '14' day "
                "AND (pilotStatusSyncTime > :retry_period OR pilotStatusSyncTime IS NULL) "
                "FOR UPDATE"
            )

            var_map = {
                ":discovery_period": discovery_period,
                ":retry_period": retry_period,
            }

            now_ts = naive_utcnow()
            sql_update = "UPDATE ATLAS_PANDA.harvester_workers SET pilotStatusSyncTime = :lastSync WHERE harvesterID= :harvesterID AND workerID= :workerID "

            # run query to select workers
            self.conn.begin()
            self.cur.arraysize = 10000
            self.cur.execute(sql_select + comment, var_map)
            db_workers = self.cur.fetchall()

            # prepare workers and separate by harvester instance and site
            workers_to_sync = {}
            var_maps = []
            for harvester_id, worker_id, pilot_status in db_workers:
                workers_to_sync.setdefault(harvester_id, {})
                workers_to_sync[harvester_id].setdefault(pilot_status, [])

                # organization for harvester commands
                workers_to_sync[harvester_id][pilot_status].append(worker_id)
                # organization to set lastSync
                var_maps.append(
                    {
                        ":workerID": worker_id,
                        ":harvesterID": harvester_id,
                        ":lastSync": now_ts,
                    }
                )

            self.cur.executemany(sql_update + comment, var_maps)

            # commit
            if not self._commit():
                raise RuntimeError("Commit error")

            tmp_log.debug("Done")
            return workers_to_sync
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return []

    # get the max workerID
    def get_max_worker_id(self, harvester_id):
        comment = " /* DBProxy.get_max_worker_id */"
        tmp_log = self.create_tagged_logger(comment, f"harvesterID={harvester_id}")
        tmp_log.debug("start")
        try:
            # sql to get workers
            sql_get_max = "SELECT MAX(workerID) FROM ATLAS_PANDA.Harvester_Workers WHERE harvesterID=:harvesterID "
            var_map = {":harvesterID": harvester_id}

            # start transaction
            self.conn.begin()
            self.cur.execute(sql_get_max + comment, var_map)
            row = self.cur.fetchone()
            if row:
                (max_id,) = row
            else:
                max_id = None
            if not self._commit():
                raise RuntimeError("Commit error")
            tmp_log.debug(f"got max workerID={max_id}")
            return max_id
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return None

    # add harvester dialog messages
    def addHarvesterDialogs(self, harvesterID, dialogs):
        comment = " /* DBProxy.addHarvesterDialogs */"
        tmp_log = self.create_tagged_logger(comment, f"harvesterID={harvesterID}")
        tmp_log.debug("start")
        try:
            # sql to delete message
            sql_delete_dialog = f"DELETE FROM {panda_config.schemaPANDA}.Harvester_Dialogs WHERE harvester_id=:harvester_id AND diagID=:diagID "

            # sql to insert message
            sql_insert_dialog = (
                f"INSERT INTO {panda_config.schemaPANDA}.Harvester_Dialogs "
                "(harvester_id, diagID, moduleName, identifier, creationTime, messageLevel, diagMessage) "
                "VALUES(:harvester_id, :diagID, :moduleName, :identifier, :creationTime, :messageLevel, :diagMessage) "
            )

            for dialog_dict in dialogs:
                # start transaction
                self.conn.begin()
                # delete
                var_map = {
                    ":diagID": dialog_dict["diagID"],
                    ":harvester_id": harvesterID,
                }
                self.cur.execute(sql_delete_dialog + comment, var_map)
                # insert
                var_map = {
                    ":diagID": dialog_dict["diagID"],
                    ":identifier": dialog_dict["identifier"],
                    ":moduleName": dialog_dict["moduleName"],
                    ":creationTime": datetime.datetime.strptime(dialog_dict["creationTime"], "%Y-%m-%d %H:%M:%S.%f"),
                    ":messageLevel": dialog_dict["messageLevel"],
                    ":diagMessage": dialog_dict["diagMessage"],
                    ":harvester_id": harvesterID,
                }
                self.cur.execute(sql_insert_dialog + comment, var_map)
                # commit
                if not self._commit():
                    raise RuntimeError("Commit error")
            tmp_log.debug(f"added {len(dialogs)} messages")
            return True
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return False

    # set num slots for workload provisioning
    def setNumSlotsForWP(self, pandaQueueName, numSlots, gshare, resourceType, validPeriod):
        comment = " /* DBProxy.setNumSlotsForWP */"
        tmp_log = self.create_tagged_logger(comment, f"pq={pandaQueueName}")
        tmp_log.debug("start")
        # sql to check
        sql_check = "SELECT 1 FROM ATLAS_PANDA.Harvester_Slots "
        sql_check += "WHERE pandaQueueName=:pandaQueueName "
        if gshare is None:
            sql_check += "AND gshare IS NULL "
        else:
            sql_check += "AND gshare=:gshare "
        if resourceType is None:
            sql_check += "AND resourceType IS NULL "
        else:
            sql_check += "AND resourceType=:resourceType "
        # sql to insert
        sql_insert = (
            "INSERT INTO ATLAS_PANDA.Harvester_Slots (pandaQueueName, gshare, resourceType, numSlots, modificationTime, expirationTime) "
            "VALUES (:pandaQueueName, :gshare, :resourceType, :numSlots, :modificationTime, :expirationTime) "
        )
        # sql to update
        if numSlots == -1:
            sql_update = "DELETE FROM ATLAS_PANDA.Harvester_Slots "
        else:
            sql_update = "UPDATE ATLAS_PANDA.Harvester_Slots SET numSlots=:numSlots, modificationTime=:modificationTime, expirationTime=:expirationTime "
        sql_update += "WHERE pandaQueueName=:pandaQueueName "
        if gshare is None:
            sql_update += "AND gshare IS NULL "
        else:
            sql_update += "AND gshare=:gshare "
        if resourceType is None:
            sql_update += "AND resourceType IS NULL "
        else:
            sql_update += "AND resourceType=:resourceType "
        try:
            time_now = naive_utcnow()
            # start transaction
            self.conn.begin()
            # check
            var_map = {
                ":pandaQueueName": pandaQueueName,
            }
            if gshare is not None:
                var_map[":gshare"] = gshare
            if resourceType is not None:
                var_map[":resourceType"] = resourceType
            self.cur.execute(sql_check, var_map)
            existing_slot = self.cur.fetchone()
            # insert or update
            var_map = {
                ":pandaQueueName": pandaQueueName,
            }
            if existing_slot is None or gshare is not None:
                var_map[":gshare"] = gshare
            if existing_slot is None or resourceType is not None:
                var_map[":resourceType"] = resourceType
            if numSlots != -1:
                var_map[":numSlots"] = numSlots
                var_map[":modificationTime"] = time_now
                if validPeriod is None:
                    var_map[":expirationTime"] = None
                else:
                    var_map[":expirationTime"] = time_now + datetime.timedelta(days=int(validPeriod))
                if existing_slot is None:
                    # insert
                    self.cur.execute(sql_insert, var_map)
                else:
                    # update
                    self.cur.execute(sql_update, var_map)
            else:
                # delete
                if existing_slot is not None:
                    self.cur.execute(sql_update, var_map)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            tmp_log.debug(f"set nSlots={numSlots}")
            return (
                0,
                f"set numSlots={numSlots} for PQ={pandaQueueName} gshare={gshare} resource={resourceType}",
            )
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return (1, "database error in the panda server")

    def getCommands(self, harvester_id, n_commands):
        """
        Gets n commands in status 'new' for a particular harvester instance and updates their status to 'retrieved'
        """

        comment = " /* DBProxy.getCommands */"
        tmp_log = self.create_tagged_logger(comment, f"harvesterID={harvester_id}")
        tmp_log.debug("start")

        try:
            self.conn.begin()
            # Prepare the bindings and var map to get the oldest n commands in 'new' status
            var_map = {
                ":harvester_id": harvester_id,
                ":n_commands": n_commands,
                ":status": "new",
            }

            sql = None
            if self.backend in ["oracle", "postgres"]:
                sql = """
                      SELECT command_id, command, params, ack_requested, creation_date FROM
                          (SELECT command_id, command, params, ack_requested, creation_date FROM ATLAS_PANDA.HARVESTER_COMMANDS
                              WHERE harvester_id=:harvester_id AND status=:status
                              ORDER BY creation_date) a
                      WHERE ROWNUM <= :n_commands
                      """
            else:
                sql = """
                      SELECT command_id, command, params, ack_requested, creation_date FROM (SELECT (@rownum:=@rownum+1) AS ROWNUM, command_id, command, params, ack_requested, creation_date FROM
                          (SELECT command_id, command, params, ack_requested, creation_date FROM ATLAS_PANDA.HARVESTER_COMMANDS
                              WHERE harvester_id=:harvester_id AND status=:status
                              ORDER BY creation_date) a, (SELECT @rownum:=0) r ) comm
                      WHERE ROWNUM <= :n_commands
                      """
            self.cur.execute(sql + comment, var_map)
            entries = self.cur.fetchall()
            tmp_log.debug(f"entries {entries}")

            # pack the commands into a dictionary for transmission to harvester
            commands = []
            command_ids = []
            for entry in entries:
                command_dict = {}
                command_dict["command_id"] = entry[0]
                command_ids.append(entry[0])  # we need to update the commands as dispatched
                command_dict["command"] = entry[1]
                command_dict["params"] = entry[2]
                if command_dict["params"] is not None:
                    try:
                        parameters = command_dict["params"].read()
                    except AttributeError:
                        parameters = command_dict["params"]
                    command_dict["params"] = json.loads(parameters)
                command_dict["ack_requested"] = entry[3]
                command_dict["creation_date"] = entry[4].isoformat()
                commands.append(command_dict)

            tmp_log.debug(f"commands {commands}")
            tmp_log.debug(f"command_ids {command_ids}")

            if command_ids:
                # update the commands and set them as retrieved
                # Prepare the bindings and var map
                var_map = {":retrieved": "retrieved"}
                for i, command_id in enumerate(command_ids):
                    var_map[f":command_id{i}"] = command_id
                command_id_bindings = ",".join(f":command_id{i}" for i in range(len(command_ids)))

                sql = f"UPDATE ATLAS_PANDA.HARVESTER_COMMANDS SET status=:retrieved, status_date=CURRENT_DATE WHERE command_id IN({command_id_bindings})"

                # run the update
                self.cur.execute(sql + comment, var_map)

            if not self._commit():
                raise RuntimeError("Commit error")

            tmp_log.debug("done")
            return 0, commands

        except Exception:
            self._rollback()
            self.dump_error_message(tmp_log)
            return -1, []

    def ackCommands(self, command_ids):
        """
        Sets the commands to acknowledged
        """
        comment = " /* DBProxy.ackCommands */"
        tmp_log = self.create_tagged_logger(comment)
        tmp_log.debug("start")

        try:
            # Prepare the bindings and var map
            var_map = {":acknowledged": "acknowledged"}
            for i, command_id in enumerate(command_ids):
                var_map[f":command_id{i}"] = command_id
            command_id_bindings = ",".join(f":command_id{i}" for i in range(len(command_ids)))

            sql = f"UPDATE ATLAS_PANDA.HARVESTER_COMMANDS SET status=:acknowledged, status_date=CURRENT_DATE WHERE command_id IN({command_id_bindings})"

            # run the update
            self.conn.begin()
            self.cur.execute(sql + comment, var_map)
            if not self._commit():
                raise RuntimeError("Commit error")

            return 0

        except Exception:
            # roll back
            self._rollback()
            self.dump_error_message(tmp_log)
            return -1

    # update workers
    def updateServiceMetrics(self, harvesterID, data):
        """
        Update service metrics
        """
        comment = " /* DBProxy.updateServiceMetrics */"
        tmp_log = self.create_tagged_logger(comment, f"harvesterID={harvesterID}")
        try:
            # generate the SQL to insert metrics into the DB
            sql = f"INSERT INTO ATLAS_PANDA.harvester_Metrics ({HarvesterMetricsSpec.columnNames()}) "
            sql += HarvesterMetricsSpec.bindValuesExpression()

            # generate the entries for the DB
            var_maps = []
            for entry in data:
                tmp_log.debug(f"entry {entry}")
                metrics_spec = HarvesterMetricsSpec()
                metrics_spec.harvester_ID = harvesterID
                metrics_spec.creation_time = datetime.datetime.strptime(entry[0], "%Y-%m-%d %H:%M:%S.%f")
                metrics_spec.harvester_host = entry[1]
                metrics_spec.metrics = entry[2]

                var_maps.append(metrics_spec.valuesMap())

            # run the SQL
            self.cur.executemany(sql + comment, var_maps)
            if not self._commit():
                raise RuntimeError("Commit error")
            tmp_log.debug("done")
            return [True]
        except Exception:
            # roll back
            self._rollback()
            self.dump_error_message(tmp_log)
            return None

    def storePilotLog(self, panda_id, pilot_log):
        """
        Stores the pilotlog in the pandalog table
        """
        comment = " /* DBProxy.storePilotLog */"
        tmp_log = self.create_tagged_logger(comment, f"PandaID={panda_id}")
        tmp_log.debug(f"start")

        try:
            # Prepare the bindings and var map
            var_map = {
                ":panda_id": panda_id,
                ":message": pilot_log[:4000],  # clip if longer than 4k characters
                ":now": naive_utcnow(),
                ":name": "panda.mon.prod",
                ":module": "JobDispatcher",
                ":type": "pilotLog",
                ":file_name": "JobDispatcher.py",
                ":log_level": 20,
                ":level_name": "INFO",
            }

            sql = (
                "INSERT INTO ATLAS_PANDA.PANDALOG (BINTIME, NAME, MODULE, TYPE, PID, LOGLEVEL, LEVELNAME, TIME, FILENAME, MESSAGE) "
                "VALUES (:now, :name, :module, :type, :panda_id, :log_level, :level_name, :now, :file_name, :message)"
            )

            # run the insert
            self.conn.begin()
            self.cur.execute(sql + comment, var_map)
            if not self._commit():
                raise RuntimeError("Commit error")

            return 0

        except Exception:
            # roll back
            self._rollback()
            self.dump_error_message(tmp_log)
            return -1

    def ups_load_worker_stats(self):
        """
        Load the harvester worker stats. Historically this would separate between prodsource labels due to different proxies for analysis and production,
        but all workers get submitted with production proxy and the pilot changes to analysis proxy if required. So we are not separating by prodsource label,
        in the query anymore, but we are keeping the prodsource label dimension in the dictionary for compatibility reasons.
        :return: dictionary with worker statistics
        """
        comment = " /* DBProxy.ups_load_worker_stats */"
        tmp_log = self.create_tagged_logger(comment)
        tmp_log.debug("start")

        # start transaction for postgres to avoid idle transactions
        self.conn.begin()

        # get current pilot distribution in harvester for the queue
        sql = (
            "SELECT computingsite, harvester_id, resourceType, status, sum(n_workers) "
            f"FROM {panda_config.schemaPANDA}.harvester_worker_stats WHERE lastupdate > :time_limit "
            "GROUP BY computingsite, harvester_id, resourceType, status"
        )
        var_map = {":time_limit": naive_utcnow() - datetime.timedelta(minutes=60)}

        self.cur.execute(sql + comment, var_map)
        worker_stats_rows = self.cur.fetchall()
        worker_stats_dict = {}
        for (
            computing_site,
            harvester_id,
            resource_type,
            status,
            n_workers,
        ) in worker_stats_rows:
            worker_stats_dict.setdefault(computing_site, {})
            worker_stats_dict[computing_site].setdefault(harvester_id, {})
            worker_stats_dict[computing_site][harvester_id].setdefault(DEFAULT_PRODSOURCELABEL, {})
            worker_stats_dict[computing_site][harvester_id][DEFAULT_PRODSOURCELABEL].setdefault(resource_type, {})
            worker_stats_dict[computing_site][harvester_id][DEFAULT_PRODSOURCELABEL][resource_type][status] = n_workers

        if not self._commit():
            raise RuntimeError("Commit error")
        tmp_log.debug("done")
        return worker_stats_dict

    def get_cpu_benchmarks_by_host(self, site: str, host_name: str) -> list[tuple[str, float]]:
        comment = " /* DBProxy.get_cpu_benchmarks_by_host */"
        tmp_log = self.create_tagged_logger(comment, f"host_name={host_name}")
        tmp_log.debug("Start")

        host_name_clean = clean_host_name(host_name)

        try:
            sql = (
                "SELECT cb.site, score_per_core FROM atlas_panda.worker_node wn, atlas_panda.cpu_benchmarks cb "
                "WHERE wn.site = :site "
                "AND wn.host_name = :host_name "
                "AND cb.cpu_type_normalized = wn.cpu_model_normalized "
                "AND wn.threads_per_core = cb.smt_enabled + 1 "
                "AND wn.n_logical_cpus = cb.ncores "  # protects against site misreporting SMT, e.g. due to VMs
            )

            var_map = {"site": site, "host_name": host_name_clean}

            self.cur.execute(sql + comment, var_map)
            results = self.cur.fetchall()

            tmp_log.debug(f"Got {len(results)} benchmarks")
            return results

        except Exception:
            self.dump_error_message(tmp_log)
            return []


# get worker module
def get_worker_module(base_mod) -> WorkerModule:
    return base_mod.get_composite_module("worker")
