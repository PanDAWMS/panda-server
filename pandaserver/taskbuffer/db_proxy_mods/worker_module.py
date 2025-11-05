import datetime
import json
import os
import re
import sys

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandautils.PandaUtils import get_sql_IN_bind_variables, naive_utcnow

from pandaserver.config import panda_config
from pandaserver.srvcore import CoreUtils
from pandaserver.srvcore.CoreUtils import clean_host_name
from pandaserver.taskbuffer import ErrorCode, JobUtils
from pandaserver.taskbuffer.db_proxy_mods.base_module import BaseModule
from pandaserver.taskbuffer.db_proxy_mods.entity_module import get_entity_module
from pandaserver.taskbuffer.HarvesterMetricsSpec import HarvesterMetricsSpec
from pandaserver.taskbuffer.JobSpec import JobSpec
from pandaserver.taskbuffer.ResourceSpec import BASIC_RESOURCE_TYPE
from pandaserver.taskbuffer.WorkerSpec import WorkerSpec


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
            var_map = dict()
            var_map[":harvesterID"] = harvesterID
            var_map[":siteName"] = siteName
            sql_lock = "SELECT harvester_ID, computingSite FROM ATLAS_PANDA.Harvester_Worker_Stats "
            sql_lock += "WHERE harvester_ID=:harvesterID AND computingSite=:siteName FOR UPDATE NOWAIT "
            try:
                self.cur.execute(sql_lock + comment, var_map)
            except Exception:
                self._rollback()
                message = "rows locked by another update"
                tmp_log.debug(message)
                tmp_log.debug("done")
                return False, message

            # delete them
            sql_delete = "DELETE FROM ATLAS_PANDA.Harvester_Worker_Stats "
            sql_delete += "WHERE harvester_ID=:harvesterID AND computingSite=:siteName "
            self.cur.execute(sql_delete + comment, var_map)

            # insert new site data
            sql_insert = "INSERT INTO ATLAS_PANDA.Harvester_Worker_Stats (harvester_ID, computingSite, jobType, resourceType, status, n_workers, lastUpdate) "
            sql_insert += "VALUES (:harvester_ID, :siteName, :jobType, :resourceType, :status, :n_workers, CURRENT_DATE) "

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
            # sql to get nPilot
            # sqlP = ("SELECT getJob+updateJob FROM ATLAS_PANDAMETA.SiteData "
            #         "WHERE HOURS=:hours AND FLAG IN (:flag1,:flag2) ")
            # varMap = dict()
            # varMap[":hours"] = 1
            # varMap[":flag1"] = "production"
            # varMap[":flag2"] = "analysis"
            # self.cur.execute(sqlP + comment, varMap)
            # res = self.cur.fetchone()
            # if res is not None:
            #     (nPilot,) = res
            # else:
            #     nPilot = 0
            # sql to get stat of workers
            sqlGA = (
                "SELECT SUM(n_workers), computingSite, harvester_ID, jobType, resourceType, status "
                "FROM ATLAS_PANDA.Harvester_Worker_Stats "
                "WHERE lastUpdate>=:time_limit "
                "GROUP BY computingSite,harvester_ID,jobType,resourceType,status "
            )
            varMap = dict()
            varMap[":time_limit"] = naive_utcnow() - datetime.timedelta(hours=4)
            self.cur.execute(sqlGA + comment, varMap)
            res_active = self.cur.fetchall()
            retMap = {}
            for cnt, computingSite, harvesterID, jobType, resourceType, status in res_active:
                retMap.setdefault(computingSite, {})
                retMap[computingSite].setdefault(harvesterID, {})
                retMap[computingSite][harvesterID].setdefault(jobType, {})
                if resourceType not in retMap[computingSite][harvesterID][jobType]:
                    retMap[computingSite][harvesterID][jobType][resourceType] = dict()
                retMap[computingSite][harvesterID][jobType][resourceType][status] = cnt
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            tmp_log.debug(f"done with {str(retMap)}")
            return retMap
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
            sqlC = "SELECT status,status_date FROM ATLAS_PANDA.HARVESTER_COMMANDS "
            sqlC += "WHERE harvester_ID=:harvester_ID AND command=:command "
            varMap = dict()
            varMap[":harvester_ID"] = harvester_ID
            varMap[":command"] = command
            self.cur.execute(sqlC + comment, varMap)
            resC = self.cur.fetchone()
            # check existing command
            toSkip = False
            if resC is not None:
                commandStatus, statusDate = resC
                # not overwrite existing command
                if (
                    commandStatus in ["new", "lock", "retrieved"]
                    and lockInterval is not None
                    and statusDate > naive_utcnow() - datetime.timedelta(minutes=lockInterval)
                ):
                    toSkip = True
                elif (
                    commandStatus in ["retrieved", "acknowledged"]
                    and comInterval is not None
                    and statusDate > naive_utcnow() - datetime.timedelta(minutes=comInterval)
                ):
                    toSkip = True
                else:
                    # delete existing command
                    sqlD = "DELETE FROM ATLAS_PANDA.HARVESTER_COMMANDS "
                    sqlD += "WHERE harvester_ID=:harvester_ID AND command=:command "
                    varMap = dict()
                    varMap[":harvester_ID"] = harvester_ID
                    varMap[":command"] = command
                    self.cur.execute(sqlD + comment, varMap)
            # insert
            if not toSkip:
                varMap = dict()
                varMap[":harvester_id"] = harvester_ID
                varMap[":command"] = command
                varMap[":ack_requested"] = 1 if ack_requested else 0
                varMap[":status"] = status
                sqlI = "INSERT INTO ATLAS_PANDA.HARVESTER_COMMANDS "
                sqlI += "(command_id,creation_date,status_date,command,harvester_id,ack_requested,status"
                if params is not None:
                    varMap[":params"] = json.dumps(params)
                    sqlI += ",params"
                sqlI += ") "
                sqlI += "VALUES (ATLAS_PANDA.HARVESTER_COMMAND_ID_SEQ.nextval,CURRENT_DATE,CURRENT_DATE,:command,:harvester_id,:ack_requested,:status"
                if params is not None:
                    sqlI += ",:params"
                sqlI += ") "
                self.cur.execute(sqlI + comment, varMap)
            if useCommit:
                if not self._commit():
                    raise RuntimeError("Commit error")
            tmp_log.debug("done")
            if toSkip:
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
            sql_running_and_submitted = (
                "SELECT /*+ RESULT_CACHE */ /* use_json_type */ sum(total_memory) / NULLIF(sum(n_workers * corecount), 0) "
                "FROM ( "
                "    SELECT hws.computingsite, "
                "           hws.harvester_id, "
                "           hws.n_workers, "
                "           hws.n_workers * NVL(rt.maxcore, NVL(sj.data.corecount * 1, 1)) * NVL(rt.maxrampercore, sj.data.maxrss * 1 / NVL(sj.data.corecount, 1)) as total_memory, "
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
                "           hws.n_workers * NVL(rt.maxcore, NVL(sj.data.corecount * 1, 1)) * NVL(rt.maxrampercore, sj.data.maxrss * 1 / NVL(sj.data.corecount, 1)) as total_memory, "
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
        queue_type = "production"
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
                queue_type = pq_data_des["type"]
            except KeyError:
                tmp_log.error("No queue type")
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
                job_type = JobUtils.translate_prodsourcelabel_to_jobtype(queue_type, prodsourcelabel)
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
            new_workers["managed"] = {BASIC_RESOURCE_TYPE: 1}

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
            sqlC = "SELECT 1 FROM ATLAS_PANDA.Harvester_Command_Lock "
            sqlC += "WHERE harvester_ID=:harvester_ID AND computingSite=:siteName AND resourceType=:resourceType AND command=:command "
            # sql to add lock
            sqlA = "INSERT INTO ATLAS_PANDA.Harvester_Command_Lock "
            sqlA += "(harvester_ID,computingSite,resourceType,command,lockedTime) "
            sqlA += "VALUES (:harvester_ID,:siteName,:resourceType,:command,CURRENT_DATE-1) "
            if useCommit:
                self.conn.begin()
            # check
            varMap = dict()
            varMap[":harvester_ID"] = harvester_ID
            varMap[":siteName"] = computingSite
            varMap[":resourceType"] = resourceType
            varMap[":command"] = command
            self.cur.execute(sqlC + comment, varMap)
            res = self.cur.fetchone()
            if res is None:
                # add lock
                self.cur.execute(sqlA + comment, varMap)
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
            timeNow = naive_utcnow()
            # sql to get commands
            sqlC = "SELECT computingSite,resourceType FROM ATLAS_PANDA.Harvester_Command_Lock "
            sqlC += "WHERE harvester_ID=:harvester_ID AND command=:command "
            sqlC += "AND ((lockedBy IS NULL AND lockedTime<:limitComm) OR (lockedBy IS NOT NULL AND lockedTime<:limitLock)) "
            sqlC += "FOR UPDATE "
            # sql to lock command
            sqlL = "UPDATE ATLAS_PANDA.Harvester_Command_Lock SET lockedBy=:lockedBy,lockedTime=CURRENT_DATE "
            sqlL += "WHERE harvester_ID=:harvester_ID AND command=:command AND computingSite=:siteName AND resourceType=:resourceType "
            sqlL += "AND ((lockedBy IS NULL AND lockedTime<:limitComm) OR (lockedBy IS NOT NULL AND lockedTime<:limitLock)) "
            # get commands
            self.conn.begin()
            self.cur.arraysize = 10000
            varMap = dict()
            varMap[":harvester_ID"] = harvester_ID
            varMap[":command"] = command
            varMap[":limitComm"] = timeNow - datetime.timedelta(minutes=commandInterval)
            varMap[":limitLock"] = timeNow - datetime.timedelta(minutes=lockInterval)
            self.cur.execute(sqlC + comment, varMap)
            res = self.cur.fetchall()
            # lock commands
            retMap = dict()
            for computingSite, resourceType in res:
                varMap = dict()
                varMap[":harvester_ID"] = harvester_ID
                varMap[":command"] = command
                varMap[":siteName"] = computingSite
                varMap[":resourceType"] = resourceType
                varMap[":limitComm"] = timeNow - datetime.timedelta(minutes=commandInterval)
                varMap[":limitLock"] = timeNow - datetime.timedelta(minutes=lockInterval)
                varMap[":lockedBy"] = lockedBy
                self.cur.execute(sqlL + comment, varMap)
                nRow = self.cur.rowcount
                if nRow > 0:
                    if computingSite not in retMap:
                        retMap[computingSite] = []
                    retMap[computingSite].append(resourceType)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmp_log.debug(f"done with {str(retMap)}")
            return retMap
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
            sqlL = "UPDATE ATLAS_PANDA.Harvester_Command_Lock SET lockedBy=NULL "
            sqlL += "WHERE harvester_ID=:harvester_ID AND command=:command "
            sqlL += "AND computingSite=:computingSite AND resourceType=:resourceType AND lockedBy=:lockedBy "
            varMap = dict()
            varMap[":harvester_ID"] = harvester_ID
            varMap[":command"] = command
            varMap[":computingSite"] = computingSite
            varMap[":resourceType"] = resourceType
            varMap[":lockedBy"] = lockedBy
            # release lock
            self.conn.begin()
            self.cur.execute(sqlL + comment, varMap)
            nRow = self.cur.rowcount
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmp_log.debug(f"done with {nRow}")
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
            varMap = dict()
            varMap[":harvesterID"] = harvesterID
            owner = CoreUtils.clean_user_id(user)
            varMap[":owner"] = owner
            varMap[":hostName"] = host
            sqlC = "UPDATE ATLAS_PANDA.Harvester_Instances SET owner=:owner,hostName=:hostName,lastUpdate=CURRENT_DATE"
            for tmpKey in data:
                tmpVal = data[tmpKey]
                if tmpKey == "commands":
                    continue
                sqlC += ",{0}=:{0}".format(tmpKey)
                if isinstance(tmpVal, str) and tmpVal.startswith("datetime/"):
                    tmpVal = datetime.datetime.strptime(tmpVal.split("/")[-1], "%Y-%m-%d %H:%M:%S.%f")
                varMap[f":{tmpKey}"] = tmpVal
            sqlC += " WHERE harvester_ID=:harvesterID "
            # exec
            self.conn.begin()
            self.cur.execute(sqlC + comment, varMap)
            nRow = self.cur.rowcount
            if nRow == 0:
                # insert instance info
                varMap = dict()
                varMap[":harvesterID"] = harvesterID
                varMap[":owner"] = owner
                varMap[":hostName"] = host
                varMap[":descr"] = "automatic"
                sqlI = (
                    "INSERT INTO ATLAS_PANDA.Harvester_Instances "
                    "(harvester_ID,owner,hostName,lastUpdate,description) "
                    "VALUES(:harvesterID,:owner,:hostName,CURRENT_DATE,:descr) "
                )
                self.cur.execute(sqlI + comment, varMap)
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
            if nRow == 0:
                retStr = "no instance record"
            else:
                retStr = "succeeded"
            return retStr
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
            regStart = naive_utcnow()
            sqlC = f"SELECT {WorkerSpec.columnNames()} FROM ATLAS_PANDA.Harvester_Workers "
            sqlC += "WHERE harvesterID=:harvesterID AND workerID=:workerID "
            # loop over all workers
            retList = []
            for workerData in data:
                timeNow = naive_utcnow()
                if useCommit:
                    self.conn.begin()
                workerSpec = WorkerSpec()
                workerSpec.harvesterID = harvesterID
                workerSpec.workerID = workerData["workerID"]
                tmp_log.debug(f"workerID={workerSpec.workerID} start")
                # check if already exists
                varMap = dict()
                varMap[":harvesterID"] = workerSpec.harvesterID
                varMap[":workerID"] = workerSpec.workerID
                self.cur.execute(sqlC + comment, varMap)
                resC = self.cur.fetchone()
                if resC is None:
                    # not exist
                    toInsert = True
                    oldLastUpdate = None
                else:
                    # already exists
                    toInsert = False
                    workerSpec.pack(resC)
                    oldLastUpdate = workerSpec.lastUpdate
                # set new values
                oldStatus = workerSpec.status
                for key in workerData:
                    val = workerData[key]
                    if hasattr(workerSpec, key):
                        setattr(workerSpec, key, val)
                workerSpec.lastUpdate = timeNow
                if oldStatus in ["finished", "failed", "cancelled", "missed"] and (
                    oldLastUpdate is not None and oldLastUpdate > timeNow - datetime.timedelta(hours=3)
                ):
                    tmp_log.debug(f"workerID={workerSpec.workerID} keep old status={oldStatus} instead of new {workerSpec.status}")
                    workerSpec.status = oldStatus
                # insert or update
                if toInsert:
                    # insert
                    tmp_log.debug(f"workerID={workerSpec.workerID} insert for status={workerSpec.status}")
                    sqlI = f"INSERT INTO ATLAS_PANDA.Harvester_Workers ({WorkerSpec.columnNames()}) "
                    sqlI += WorkerSpec.bindValuesExpression()
                    varMap = workerSpec.valuesMap()
                    self.cur.execute(sqlI + comment, varMap)
                else:
                    # update
                    tmp_log.debug(f"workerID={workerSpec.workerID} update for status={workerSpec.status}")
                    sqlU = f"UPDATE ATLAS_PANDA.Harvester_Workers SET {workerSpec.bindUpdateChangesExpression()} "
                    sqlU += "WHERE harvesterID=:harvesterID AND workerID=:workerID "
                    varMap = workerSpec.valuesMap(onlyChanged=True)
                    self.cur.execute(sqlU + comment, varMap)
                # job relation
                if "pandaid_list" in workerData and len(workerData["pandaid_list"]) > 0:
                    tmp_log.debug(f"workerID={workerSpec.workerID} update/insert job relation")
                    sqlJC = "SELECT PandaID FROM ATLAS_PANDA.Harvester_Rel_Jobs_Workers "
                    sqlJC += "WHERE harvesterID=:harvesterID AND workerID=:workerID "
                    sqlJI = "INSERT INTO ATLAS_PANDA.Harvester_Rel_Jobs_Workers (harvesterID,workerID,PandaID,lastUpdate) "
                    sqlJI += "VALUES (:harvesterID,:workerID,:PandaID,:lastUpdate) "
                    sqlJU = "UPDATE ATLAS_PANDA.Harvester_Rel_Jobs_Workers SET lastUpdate=:lastUpdate "
                    sqlJU += "WHERE harvesterID=:harvesterID AND workerID=:workerID AND PandaID=:PandaID "
                    # get jobs
                    varMap = dict()
                    varMap[":harvesterID"] = harvesterID
                    varMap[":workerID"] = workerData["workerID"]
                    self.cur.execute(sqlJC + comment, varMap)
                    resJC = self.cur.fetchall()
                    exPandaIDs = set()
                    for (pandaID,) in resJC:
                        exPandaIDs.add(pandaID)
                    for pandaID in workerData["pandaid_list"]:
                        # update or insert
                        varMap = dict()
                        varMap[":harvesterID"] = harvesterID
                        varMap[":workerID"] = workerData["workerID"]
                        varMap[":PandaID"] = pandaID
                        varMap[":lastUpdate"] = timeNow
                        if pandaID not in exPandaIDs:
                            # insert
                            self.cur.execute(sqlJI + comment, varMap)
                        else:
                            # update
                            self.cur.execute(sqlJU + comment, varMap)
                            exPandaIDs.discard(pandaID)
                    # delete redundant list
                    sqlJD = "DELETE FROM ATLAS_PANDA.Harvester_Rel_Jobs_Workers "
                    sqlJD += "WHERE harvesterID=:harvesterID AND workerID=:workerID AND PandaID=:PandaID "
                    for pandaID in exPandaIDs:
                        varMap = dict()
                        varMap[":PandaID"] = pandaID
                        varMap[":harvesterID"] = harvesterID
                        varMap[":workerID"] = workerData["workerID"]
                        self.cur.execute(sqlJD + comment, varMap)
                    tmp_log.debug(f"workerID={workerSpec.workerID} deleted {len(exPandaIDs)} jobs")
                # comprehensive heartbeat
                tmp_log.debug(f"workerID={workerSpec.workerID} get jobs")
                sqlCJ = "SELECT r.PandaID FROM "
                sqlCJ += "ATLAS_PANDA.Harvester_Rel_Jobs_Workers r,ATLAS_PANDA.jobsActive4 j  "
                sqlCJ += "WHERE r.harvesterID=:harvesterID AND r.workerID=:workerID "
                sqlCJ += "AND j.PandaID=r.PandaID AND NOT j.jobStatus IN (:holding) "
                sqlJAC = "SELECT jobStatus,prodSourceLabel,attemptNr FROM ATLAS_PANDA.jobsActive4 WHERE PandaID=:PandaID  "
                sqlJAA = "UPDATE ATLAS_PANDA.jobsActive4 SET modificationTime=CURRENT_DATE WHERE PandaID=:PandaID AND jobStatus IN (:js1,:js2) "
                sqlJAE = "UPDATE ATLAS_PANDA.jobsActive4 SET taskBufferErrorCode=:code,taskBufferErrorDiag=:diag,"
                sqlJAE += "startTime=(CASE WHEN jobStatus=:starting THEN NULL ELSE startTime END) "
                sqlJAE += "WHERE PandaID=:PandaID "
                sqlJSE = "UPDATE {0} SET supErrorCode=:code,supErrorDiag=:diag,stateChangeTime=CURRENT_DATE "
                sqlJSE += "WHERE PandaID=:PandaID AND NOT jobStatus IN (:finished) AND modificationTime>CURRENT_DATE-30"
                varMap = dict()
                varMap[":harvesterID"] = harvesterID
                varMap[":workerID"] = workerData["workerID"]
                varMap[":holding"] = "holding"
                self.cur.execute(sqlCJ + comment, varMap)
                resCJ = self.cur.fetchall()
                tmp_log.debug(f"workerID={workerSpec.workerID} update {len(resCJ)} jobs")
                for (pandaID,) in resCJ:
                    # check job status when worker is in a final state
                    if workerSpec.status in [
                        "finished",
                        "failed",
                        "cancelled",
                        "missed",
                    ]:
                        varMap = dict()
                        varMap[":PandaID"] = pandaID
                        self.cur.execute(sqlJAC + comment, varMap)
                        resJAC = self.cur.fetchone()
                        if resJAC is not None:
                            jobStatus, prodSourceLabel, attemptNr = resJAC
                            tmp_log.debug(f"workerID={workerSpec.workerID} {workerSpec.status} while PandaID={pandaID} {jobStatus}")
                            # set failed if out of sync
                            if "syncLevel" in workerData and workerData["syncLevel"] == 1 and jobStatus in ["running", "starting"]:
                                tmp_log.debug(f"workerID={workerSpec.workerID} set failed to PandaID={pandaID} due to sync error")
                                varMap = dict()
                                varMap[":PandaID"] = pandaID
                                varMap[":code"] = ErrorCode.EC_WorkerDone
                                varMap[":starting"] = "starting"
                                varMap[":diag"] = f"The worker was {workerSpec.status} while the job was {jobStatus} : {workerSpec.diagMessage}"
                                varMap[":diag"] = JobSpec.truncateStringAttr("taskBufferErrorDiag", varMap[":diag"])
                                self.cur.execute(sqlJAE + comment, varMap)
                                # make an empty file to triggre registration for zip files in Adder
                                # tmpFileName = '{0}_{1}_{2}'.format(pandaID, 'failed',
                                #                                    uuid.uuid3(uuid.NAMESPACE_DNS,''))
                                # tmpFileName = os.path.join(panda_config.logdir, tmpFileName)
                                # try:
                                #     open(tmpFileName, 'w').close()
                                # except Exception:
                                #     pass
                                # sql to insert empty job output report for adder
                                sqlI = (
                                    "INSERT INTO {0}.Job_Output_Report "
                                    "(PandaID, prodSourceLabel, jobStatus, attemptNr, data, timeStamp) "
                                    "VALUES(:PandaID, :prodSourceLabel, :jobStatus, :attemptNr, :data, :timeStamp) "
                                ).format(panda_config.schemaPANDA)
                                # insert
                                varMap = {}
                                varMap[":PandaID"] = pandaID
                                varMap[":prodSourceLabel"] = prodSourceLabel
                                varMap[":jobStatus"] = "failed"
                                varMap[":attemptNr"] = attemptNr
                                varMap[":data"] = None
                                varMap[":timeStamp"] = naive_utcnow()
                                try:
                                    self.cur.execute(sqlI + comment, varMap)
                                except Exception:
                                    pass
                                else:
                                    tmp_log.debug(f"successfully inserted job output report {pandaID}.{varMap[':attemptNr']}")
                        if workerSpec.errorCode not in [None, 0]:
                            varMap = dict()
                            varMap[":PandaID"] = pandaID
                            varMap[":code"] = workerSpec.errorCode
                            varMap[":diag"] = f"Diag from worker : {workerSpec.diagMessage}"
                            varMap[":diag"] = JobSpec.truncateStringAttr("supErrorDiag", varMap[":diag"])
                            varMap[":finished"] = "finished"
                            for tableName in [
                                "ATLAS_PANDA.jobsActive4",
                                "ATLAS_PANDA.jobsArchived4",
                                "ATLAS_PANDAARCH.jobsArchived",
                            ]:
                                self.cur.execute(sqlJSE.format(tableName) + comment, varMap)
                    """
                    varMap = dict()
                    varMap[':PandaID'] = pandaID
                    varMap[':js1'] = 'running'
                    varMap[':js2'] = 'starting'
                    self.cur.execute(sqlJAA+comment, varMap)
                    nRowJA = self.cur.rowcount
                    if nRowJA > 0:
                        tmp_log.debug('workerID={0} PandaID={1} updated modificationTime'.format(workerSpec.workerID, pandaID))
                    """
                tmp_log.debug(f"workerID={workerSpec.workerID} end")
                # commit
                if useCommit:
                    if not self._commit():
                        raise RuntimeError("Commit error")
                retList.append(True)
            regTime = naive_utcnow() - regStart
            tmp_log.debug("done. exec_time=%s.%03d sec" % (regTime.seconds, regTime.microseconds / 1000))
            return retList
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
            retD = self.cur.rowcount
            if not self._commit():
                raise RuntimeError("Commit error")
            tmp_log.debug(f"Updated successfully with {retD}")
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

                sql = "UPDATE ATLAS_PANDA.worker_node SET last_seen=:last_seen " "WHERE site=:site AND host_name=:host_name AND cpu_model=:cpu_model"

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

    # get workers for a job
    def getWorkersForJob(self, PandaID):
        comment = " /* DBProxy.getWorkersForJob */"
        tmp_log = self.create_tagged_logger(comment, f"PandaID={PandaID}")
        tmp_log.debug("start")
        try:
            # sql to get workers
            sqlC = f"SELECT {WorkerSpec.columnNames(prefix='w')} FROM ATLAS_PANDA.Harvester_Workers w, ATLAS_PANDA.Harvester_Rel_Jobs_Workers r "
            sqlC += "WHERE w.harvesterID=r.harvesterID AND w.workerID=r.workerID AND r.PandaID=:PandaID "
            varMap = {}
            varMap[":PandaID"] = PandaID
            # start transaction
            self.conn.begin()
            self.cur.execute(sqlC + comment, varMap)
            resCs = self.cur.fetchall()
            retList = []
            for resC in resCs:
                workerSpec = WorkerSpec()
                workerSpec.pack(resC)
                retList.append(workerSpec)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmp_log.debug(f"got {len(retList)} workers")
            return retList
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
            sql_select = """
            SELECT /*+ INDEX_RS_ASC(harvester_workers HARVESTER_WORKERS_STATUS_IDX) */ harvesterID, workerID, pilotStatus
            FROM ATLAS_PANDA.harvester_workers
            WHERE (status in ('submitted', 'ready') AND pilotStatus='running' AND pilotStartTime < :discovery_period)
            OR (status in ('submitted', 'ready', 'running', 'idle') AND pilotStatus='finished' AND pilotEndTime < :discovery_period)
            AND lastupdate > sysdate - interval '7' day
            AND submittime > sysdate - interval '14' day
            AND (pilotStatusSyncTime > :retry_period OR pilotStatusSyncTime IS NULL)
            FOR UPDATE
            """
            var_map = {
                ":discovery_period": discovery_period,
                ":retry_period": retry_period,
            }

            now_ts = naive_utcnow()
            sql_update = """
            UPDATE ATLAS_PANDA.harvester_workers
            SET pilotStatusSyncTime = :lastSync
            WHERE harvesterID= :harvesterID
            AND workerID= :workerID
            """

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
            sqlC = "SELECT MAX(workerID) FROM ATLAS_PANDA.Harvester_Workers "
            sqlC += "WHERE harvesterID=:harvesterID "
            varMap = {":harvesterID": harvester_id}
            # start transaction
            self.conn.begin()
            self.cur.execute(sqlC + comment, varMap)
            res = self.cur.fetchone()
            if res:
                (max_id,) = res
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
            sqlC = f"DELETE FROM {panda_config.schemaPANDA}.Harvester_Dialogs "
            sqlC += "WHERE harvester_id=:harvester_id AND diagID=:diagID "
            # sql to insert message
            sqlI = f"INSERT INTO {panda_config.schemaPANDA}.Harvester_Dialogs "
            sqlI += "(harvester_id,diagID,moduleName,identifier,creationTime,messageLevel,diagMessage) "
            sqlI += "VALUES(:harvester_id,:diagID,:moduleName,:identifier,:creationTime,:messageLevel,:diagMessage) "
            for diagDict in dialogs:
                # start transaction
                self.conn.begin()
                # delete
                varMap = dict()
                varMap[":diagID"] = diagDict["diagID"]
                varMap[":harvester_id"] = harvesterID
                self.cur.execute(sqlC + comment, varMap)
                # insert
                varMap = dict()
                varMap[":diagID"] = diagDict["diagID"]
                varMap[":identifier"] = diagDict["identifier"]
                varMap[":moduleName"] = diagDict["moduleName"]
                varMap[":creationTime"] = datetime.datetime.strptime(diagDict["creationTime"], "%Y-%m-%d %H:%M:%S.%f")
                varMap[":messageLevel"] = diagDict["messageLevel"]
                varMap[":diagMessage"] = diagDict["diagMessage"]
                varMap[":harvester_id"] = harvesterID
                self.cur.execute(sqlI + comment, varMap)
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
        sqlC = "SELECT 1 FROM ATLAS_PANDA.Harvester_Slots "
        sqlC += "WHERE pandaQueueName=:pandaQueueName "
        if gshare is None:
            sqlC += "AND gshare IS NULL "
        else:
            sqlC += "AND gshare=:gshare "
        if resourceType is None:
            sqlC += "AND resourceType IS NULL "
        else:
            sqlC += "AND resourceType=:resourceType "
        # sql to insert
        sqlI = "INSERT INTO ATLAS_PANDA.Harvester_Slots (pandaQueueName,gshare,resourceType,numSlots,modificationTime,expirationTime) "
        sqlI += "VALUES (:pandaQueueName,:gshare,:resourceType,:numSlots,:modificationTime,:expirationTime) "
        # sql to update
        if numSlots == -1:
            sqlU = "DELETE FROM ATLAS_PANDA.Harvester_Slots "
        else:
            sqlU = "UPDATE ATLAS_PANDA.Harvester_Slots SET "
            sqlU += "numSlots=:numSlots,modificationTime=:modificationTime,expirationTime=:expirationTime "
        sqlU += "WHERE pandaQueueName=:pandaQueueName "
        if gshare is None:
            sqlU += "AND gshare IS NULL "
        else:
            sqlU += "AND gshare=:gshare "
        if resourceType is None:
            sqlU += "AND resourceType IS NULL "
        else:
            sqlU += "AND resourceType=:resourceType "
        try:
            timeNow = naive_utcnow()
            # start transaction
            self.conn.begin()
            # check
            varMap = dict()
            varMap[":pandaQueueName"] = pandaQueueName
            if gshare is not None:
                varMap[":gshare"] = gshare
            if resourceType is not None:
                varMap[":resourceType"] = resourceType
            self.cur.execute(sqlC, varMap)
            resC = self.cur.fetchone()
            # insert or update
            varMap = dict()
            varMap[":pandaQueueName"] = pandaQueueName
            if resC is None or gshare is not None:
                varMap[":gshare"] = gshare
            if resC is None or resourceType is not None:
                varMap[":resourceType"] = resourceType
            if numSlots != -1:
                varMap[":numSlots"] = numSlots
                varMap[":modificationTime"] = timeNow
                if validPeriod is None:
                    varMap[":expirationTime"] = None
                else:
                    varMap[":expirationTime"] = timeNow + datetime.timedelta(days=int(validPeriod))
                if resC is None:
                    # insert
                    self.cur.execute(sqlI, varMap)
                else:
                    # update
                    self.cur.execute(sqlU, varMap)
            else:
                # delete
                if resC is not None:
                    self.cur.execute(sqlU, varMap)
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
                        theParams = command_dict["params"].read()
                    except AttributeError:
                        theParams = command_dict["params"]
                    command_dict["params"] = json.loads(theParams)
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

                sql = f"""
                      UPDATE ATLAS_PANDA.HARVESTER_COMMANDS
                      SET status=:retrieved,status_date=CURRENT_DATE
                      WHERE command_id IN({command_id_bindings})
                      """

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

            sql = f"""
                  UPDATE ATLAS_PANDA.HARVESTER_COMMANDS
                  SET status=:acknowledged,status_date=CURRENT_DATE
                  WHERE command_id IN({command_id_bindings})
                  """

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

            sql = """
                  INSERT INTO ATLAS_PANDA.PANDALOG (BINTIME, NAME, MODULE, TYPE, PID, LOGLEVEL, LEVELNAME,
                                                    TIME, FILENAME, MESSAGE)
                  VALUES (:now, :name, :module, :type, :panda_id, :log_level, :level_name,
                          :now, :file_name, :message)
                  """

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
        Load the harvester worker stats
        :return: dictionary with worker statistics
        """
        comment = " /* DBProxy.ups_load_worker_stats */"
        tmp_log = self.create_tagged_logger(comment)
        tmp_log.debug("start")

        # start transaction for postgres to avoid idle transactions
        self.conn.begin()

        # get current pilot distribution in harvester for the queue
        sql = f"""
              SELECT computingsite, harvester_id, jobType, resourceType, status, n_workers
              FROM {panda_config.schemaPANDA}.harvester_worker_stats
              WHERE lastupdate > :time_limit
              """
        var_map = {}
        var_map[":time_limit"] = naive_utcnow() - datetime.timedelta(minutes=60)

        self.cur.execute(sql + comment, var_map)
        worker_stats_rows = self.cur.fetchall()
        worker_stats_dict = {}
        for (
            computing_site,
            harvester_id,
            job_type,
            resource_type,
            status,
            n_workers,
        ) in worker_stats_rows:
            worker_stats_dict.setdefault(computing_site, {})
            worker_stats_dict[computing_site].setdefault(harvester_id, {})
            worker_stats_dict[computing_site][harvester_id].setdefault(job_type, {})
            worker_stats_dict[computing_site][harvester_id][job_type].setdefault(resource_type, {})
            worker_stats_dict[computing_site][harvester_id][job_type][resource_type][status] = n_workers

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
                "AND wn.threads_per_core = cb.smt_enabled + 1"
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
