import datetime
import json
import os
import random
import re
import time
import traceback
import uuid

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandautils.PandaUtils import get_sql_IN_bind_variables, naive_utcnow

from pandaserver.config import panda_config
from pandaserver.srvcore import CoreUtils
from pandaserver.taskbuffer import (
    EventServiceUtils,
    GlobalShares,
    JobUtils,
    PrioUtil,
    SiteSpec,
)
from pandaserver.taskbuffer.db_proxy_mods.base_module import (
    BaseModule,
    convert_dict_to_bind_vars,
    memoize,
)
from pandaserver.taskbuffer.DdmSpec import DdmSpec
from pandaserver.taskbuffer.JediDatasetSpec import (
    INPUT_TYPES_var_map,
    INPUT_TYPES_var_str,
)
from pandaserver.taskbuffer.JediTaskSpec import JediTaskSpec
from pandaserver.taskbuffer.ResourceSpec import ResourceSpec, ResourceSpecMapper
from pandaserver.taskbuffer.Utils import create_shards
from pandaserver.taskbuffer.WorkQueueMapper import WorkQueueMapper


# Module class to define methods related to fundamental entities like GlobalShares, Resource Types, CO2, etc
class EntityModule(BaseModule):
    # constructor
    def __init__(self, log_stream: LogWrapper):
        super().__init__(log_stream)
        # global share variables
        self.tree = None  # Pointer to the root of the global shares tree
        self.leave_shares = None  # Pointer to the list with leave shares
        self.__t_update_shares = None  # Timestamp when the shares were last updated
        self.__hs_distribution = None  # HS06s distribution of sites
        self.__t_update_distribution = None  # Timestamp when the HS06s distribution was last updated

        # resource type mapper
        # if you want to use it, you need to call reload_resource_spec_mapper first
        self.resource_spec_mapper = None
        self.__t_update_resource_type_mapper = None

        # priority boost
        self.job_prio_boost_dict = None
        self.job_prio_boost_dict_update_time = None

    def __get_hs_leave_distribution(self, leave_shares):
        """
        Get the current HS06 distribution for running and queued jobs
        """
        comment = " /* DBProxy.get_hs_leave_distribution */"

        sql_hs_distribution = """
            SELECT gshare, jobstatus_grouped, SUM(HS)
            FROM
                (SELECT gshare, HS,
                     CASE
                         WHEN jobstatus IN('activated') THEN 'queued'
                         WHEN jobstatus IN('sent', 'running') THEN 'executing'
                         ELSE 'ignore'
                     END jobstatus_grouped
                 FROM ATLAS_PANDA.JOBS_SHARE_STATS JSS) a
            GROUP BY gshare, jobstatus_grouped
            """

        self.cur.execute(sql_hs_distribution + comment)
        hs_distribution_raw = self.cur.fetchall()

        # get the hs distribution data into a dictionary structure
        hs_distribution_dict = {}
        hs_queued_total = 0
        hs_executing_total = 0
        hs_ignore_total = 0
        for hs_entry in hs_distribution_raw:
            gshare, status_group, hs = hs_entry
            if hs is None:
                continue
            hs = float(hs)
            hs_distribution_dict.setdefault(
                gshare,
                {
                    GlobalShares.PLEDGED: 0,
                    GlobalShares.QUEUED: 0,
                    GlobalShares.EXECUTING: 0,
                },
            )
            hs_distribution_dict[gshare][status_group] = hs
            # calculate totals
            if status_group == GlobalShares.QUEUED:
                hs_queued_total += hs
            elif status_group == GlobalShares.EXECUTING:
                hs_executing_total += hs
            else:
                hs_ignore_total += hs

        # Calculate the ideal HS06 distribution based on shares.
        for share_node in leave_shares:
            share_name, share_value = share_node.name, share_node.value
            hs_pledged_share = hs_executing_total * share_value / 100.0

            hs_distribution_dict.setdefault(
                share_name,
                {
                    GlobalShares.PLEDGED: 0,
                    GlobalShares.QUEUED: 0,
                    GlobalShares.EXECUTING: 0,
                },
            )
            # Pledged HS according to global share definitions
            hs_distribution_dict[share_name]["pledged"] = hs_pledged_share
        return hs_distribution_dict

    # retrieve global shares
    def get_shares(self, parents=""):
        comment = " /* DBProxy.get_shares */"
        tmp_log = self.create_tagged_logger(comment)
        tmp_log.debug("start")

        sql = """
               SELECT NAME, VALUE, PARENT, PRODSOURCELABEL, WORKINGGROUP, CAMPAIGN, PROCESSINGTYPE, TRANSPATH, RTYPE,
               VO, QUEUE_ID, THROTTLED
               FROM ATLAS_PANDA.GLOBAL_SHARES
               """
        var_map = None

        if parents == "":
            # Get all shares
            pass
        elif parents is None:
            # Get top level shares
            sql += "WHERE parent IS NULL"

        elif isinstance(parents, str):
            # Get the children of a specific share
            var_map = {":parent": parents}
            sql += "WHERE parent = :parent"

        elif type(parents) in (list, tuple):
            # Get the children of a list of shares
            var_map = {}
            parent_var_names_str, parent_var_map = get_sql_IN_bind_variables(parents, prefix=":parent")
            sql += f"WHERE parent IN ({parent_var_names_str})"
            var_map.update(parent_var_map)

        self.cur.execute(sql + comment, var_map)
        resList = self.cur.fetchall()

        tmp_log.debug("done")
        return resList

    def reload_shares(self, force=False):
        """
        Reloads the shares from the DB and recalculates distributions
        """
        comment = " /* DBProxy.reload_shares */"
        # Don't reload shares every time
        if (self.__t_update_shares is not None and self.__t_update_shares > datetime.datetime.now() - datetime.timedelta(hours=1)) or force:
            return

        tmp_log = self.create_tagged_logger(comment)

        # Root dummy node
        t_before = time.time()
        tree = GlobalShares.Share("root", 100, None, None, None, None, None, None, None, None, None, None)
        t_after = time.time()
        total = t_after - t_before
        tmp_log.debug(f"Root dummy tree took {total}s")

        # Get top level shares from DB
        t_before = time.time()
        shares_top_level = self.get_shares(parents=None)
        t_after = time.time()
        total = t_after - t_before
        tmp_log.debug(f"Getting shares took {total}s")

        # Load branches
        t_before = time.time()
        for (
            name,
            value,
            parent,
            prodsourcelabel,
            workinggroup,
            campaign,
            processingtype,
            transpath,
            rtype,
            vo,
            queue_id,
            throttled,
        ) in shares_top_level:
            share = GlobalShares.Share(
                name,
                value,
                parent,
                prodsourcelabel,
                workinggroup,
                campaign,
                processingtype,
                transpath,
                rtype,
                vo,
                queue_id,
                throttled,
            )
            tree.children.append(self.__load_branch(share))
        t_after = time.time()
        total = t_after - t_before
        tmp_log.debug(f"Loading the branches took {total}s")

        # Normalize the values in the database
        t_before = time.time()
        tree.normalize()
        t_after = time.time()
        total = t_after - t_before
        tmp_log.debug(f"Normalizing the values took {total}s")

        # get the leave shares (the ones not having more children)
        t_before = time.time()
        leave_shares = tree.get_leaves()
        t_after = time.time()
        total = t_after - t_before
        tmp_log.debug(f"Getting the leaves took {total}s")

        self.leave_shares = leave_shares
        self.__t_update_shares = datetime.datetime.now()

        # get the distribution of shares
        t_before = time.time()
        # Retrieve the current HS06 distribution of jobs from the database and then aggregate recursively up to the root
        hs_distribution = self.__get_hs_leave_distribution(leave_shares)
        tree.aggregate_hs_distribution(hs_distribution)
        t_after = time.time()
        total = t_after - t_before
        tmp_log.debug(f"Aggregating the hs distribution took {total}s")

        self.tree = tree
        self.__hs_distribution = hs_distribution
        self.__t_update_distribution = datetime.datetime.now()
        return

    def __reload_hs_distribution(self):
        """
        Reloads the HS distribution
        """
        comment = " /* DBProxy.__reload_hs_distribution */"
        tmp_log = self.create_tagged_logger(comment)
        tmp_log.debug(self.__t_update_distribution)
        tmp_log.debug(self.__hs_distribution)
        # Reload HS06s distribution every 10 seconds
        if self.__t_update_distribution is not None and self.__t_update_distribution > datetime.datetime.now() - datetime.timedelta(seconds=10):
            tmp_log.debug("release")
            return

        # Retrieve the current HS06 distribution of jobs from the database and then aggregate recursively up to the root
        tmp_log.debug("get dist")
        t_before = time.time()
        hs_distribution = self.__get_hs_leave_distribution(self.leave_shares)
        tmp_log.debug("aggr dist")
        self.tree.aggregate_hs_distribution(hs_distribution)
        t_after = time.time()
        total = t_after - t_before
        tmp_log.debug(f"Reloading the hs distribution took {total}s")

        self.__hs_distribution = hs_distribution
        self.__t_update_distribution = datetime.datetime.now()

        # log the distribution for debugging purposes
        tmp_log.debug(f"Current HS06 distribution is {hs_distribution}")

        return

    def get_sorted_leaves(self):
        """
        Re-loads the shares, then returns the leaves sorted by under usage
        """
        self.reload_shares()
        self.__reload_hs_distribution()
        return self.tree.sort_branch_by_current_hs_distribution(self.__hs_distribution)

    def get_tree_of_gshare_names(self):
        """
        get nested dict of gshare names implying the tree structure
        """

        def get_nested_gshare(share):
            val = None
            if not share.children:
                # leaf
                pass
            else:
                # branch
                val = {}
                for child in share.children:
                    val[child.name] = get_nested_gshare(child)
            return val

        ret_dict = get_nested_gshare(self.tree)
        return ret_dict

    def __load_branch(self, share):
        """
        Recursively load a branch
        """
        node = GlobalShares.Share(
            share.name,
            share.value,
            share.parent,
            share.prodsourcelabel,
            share.workinggroup,
            share.campaign,
            share.processingtype,
            share.transpath,
            share.rtype,
            share.vo,
            share.queue_id,
            share.throttled,
        )

        children = self.get_shares(parents=share.name)
        if not children:
            return node

        for (
            name,
            value,
            parent,
            prodsourcelabel,
            workinggroup,
            campaign,
            processingtype,
            transpath,
            rtype,
            vo,
            queue_id,
            throttled,
        ) in children:
            child = GlobalShares.Share(
                name,
                value,
                parent,
                prodsourcelabel,
                workinggroup,
                campaign,
                processingtype,
                transpath,
                rtype,
                vo,
                queue_id,
                throttled,
            )
            node.children.append(self.__load_branch(child))

        return node

    def getGShareStatus(self):
        """
        Generates a list with sorted leave branches
        """

        comment = " /* DBProxy.getGShareStatus */"
        tmp_log = self.create_tagged_logger(comment)
        tmp_log.debug("start")

        self.reload_shares()
        self.__reload_hs_distribution()
        sorted_shares = self.tree.sort_branch_by_current_hs_distribution(self.__hs_distribution)

        sorted_shares_export = []
        for share in sorted_shares:
            sorted_shares_export.append(
                {
                    "name": share.name,
                    "running": self.__hs_distribution[share.name]["executing"],
                    "target": self.__hs_distribution[share.name]["pledged"],
                    "queuing": self.__hs_distribution[share.name]["queued"],
                }
            )
        return sorted_shares_export

    def is_valid_share(self, share_name):
        """
        Checks whether the share is a valid leave share
        """
        self.reload_shares()
        for share in self.leave_shares:
            if share_name == share.name:
                # Share found
                return True

        # Share not found
        return False

    def reassignShare(self, jedi_task_ids, gshare, reassign_running):
        """
        Will reassign all tasks and their jobs that have not yet completed to specified share
        @param jedi_task_ids: task ids
        @param gshare: dest share
        @param reassign_running: whether to reassign running jobs
        """

        comment = " /* DBProxy.reassignShare */"
        tmp_log = self.create_tagged_logger(comment)
        tmp_log.debug(f"Start reassigning {jedi_task_ids} to gshare={gshare} and reassign_running={reassign_running}")

        try:
            if not self.is_valid_share(gshare):
                error_msg = f"Share {gshare} is not a leave share "
                tmp_log.debug(error_msg)
                ret_val = 1, error_msg
                tmp_log.debug(error_msg)
                return ret_val

            # begin transaction
            self.conn.begin()

            # update in shards of 100 task ids
            for shard in create_shards(jedi_task_ids, 100):
                # Prepare the bindings
                var_map = {}
                shard_taskid_set = set()
                i = 0
                for _task_id in shard:
                    jedi_task_id = int(_task_id)
                    var_map[f":jtid{i}"] = jedi_task_id
                    i += 1
                    shard_taskid_set.add(jedi_task_id)
                jtid_bindings = ",".join(f":jtid{i}" for i in range(len(shard_taskid_set)))

                # select only tasks without lock
                sql_tasks_not_locked = f"""
                       SELECT jediTaskID FROM ATLAS_PANDA.JEDI_Tasks
                       WHERE jediTaskID IN ({jtid_bindings}) AND lockedBy IS NULL
                       """

                self.cur.execute(sql_tasks_not_locked + comment, var_map)
                res = self.cur.fetchall()

                # Update the bindings and prepare var map
                var_map = {":gshare": gshare}
                good_taskid_set = set()
                i = 0
                for (_task_id,) in res:
                    jedi_task_id = int(_task_id)
                    var_map[f":jtid{i}"] = jedi_task_id
                    i += 1
                    good_taskid_set.add(jedi_task_id)
                jtid_bindings = ",".join(f":jtid{i}" for i in range(len(good_taskid_set)))
                locked_taskid_set = shard_taskid_set - good_taskid_set
                if locked_taskid_set:
                    tmp_log.debug(f"skip locked tasks: {','.join([str(i) for i in locked_taskid_set])}")

                # Skip if there are no tasks to update
                if not jtid_bindings:
                    continue

                # update the task
                sql_task = f"""
                       UPDATE ATLAS_PANDA.jedi_tasks set gshare=:gshare
                       WHERE jeditaskid IN ({jtid_bindings})
                       """

                self.cur.execute(sql_task + comment, var_map)
                tmp_log.debug(f"""set tasks {",".join([str(i) for i in good_taskid_set])} to gshare={gshare}""")

                var_map[":pending"] = "pending"
                var_map[":defined"] = "defined"
                var_map[":assigned"] = "assigned"
                var_map[":waiting"] = "waiting"
                var_map[":activated"] = "activated"
                jobstatus = ":pending, :defined, :assigned, :waiting, :activated"

                # add running status in case these jobs also need to be reassigned
                if reassign_running:
                    jobstatus = f"{jobstatus}, :running, :starting"
                    var_map[":running"] = "running"
                    var_map[":starting"] = "starting"

                # update the jobs
                sql_jobs = """
                       UPDATE ATLAS_PANDA.{0} set gshare=:gshare
                       WHERE jeditaskid IN ({1})
                       AND jobstatus IN ({2})
                       """

                for table in ["jobsactive4", "jobsdefined4"]:
                    self.cur.execute(
                        sql_jobs.format(table, jtid_bindings, jobstatus) + comment,
                        var_map,
                    )

            # commit
            if not self._commit():
                raise RuntimeError("Commit error")

            tmp_log.debug("done")
            return 0, None

        except Exception:
            # roll back
            self._rollback()
            # dump error
            self.dump_error_message(tmp_log)
            return -1, None

    def reload_resource_spec_mapper(self):
        # update once per hour only
        if self.__t_update_resource_type_mapper and self.__t_update_resource_type_mapper > datetime.datetime.now() - datetime.timedelta(hours=1):
            return

        # get the resource types from the DB and make the ResourceSpecMapper object
        resource_types = self.load_resource_types(use_commit=False)
        if resource_types:
            self.resource_spec_mapper = ResourceSpecMapper(resource_types)
            self.__t_update_resource_type_mapper = datetime.datetime.now()
        return

    def load_resource_types(self, formatting="spec", use_commit=True):
        """
        Load the resource type table to memory
        """
        comment = " /* JediDBProxy.load_resource_types */"
        tmp_log = self.create_tagged_logger(comment)
        tmp_log.debug("start")
        try:
            sql = f"SELECT {ResourceSpec.column_names()} FROM {panda_config.schemaJEDI}.resource_types "
            self.cur.execute(sql + comment)
            resource_list = self.cur.fetchall()
            resource_spec_list = []
            for row in resource_list:
                resource_name, mincore, maxcore, minrampercore, maxrampercore = row
                if formatting == "dict":
                    resource_dict = {
                        "resource_name": resource_name,
                        "mincore": mincore,
                        "maxcore": maxcore,
                        "minrampercore": minrampercore,
                        "maxrampercore": maxrampercore,
                    }
                    resource_spec_list.append(resource_dict)
                else:
                    resource_spec_list.append(
                        ResourceSpec(
                            resource_name,
                            mincore,
                            maxcore,
                            minrampercore,
                            maxrampercore,
                        )
                    )
            # commit for postgres to avoid idle transactions
            if use_commit:
                if not self._commit():
                    raise RuntimeError("Commit error")
            tmp_log.debug("done")
            return resource_spec_list
        except Exception:
            if use_commit:
                self._rollback()
            self.dump_error_message(tmp_log)
            return []

    def get_resource_type_task(self, task_spec):
        """
        Identify the resource type of the task based on the resource type map.
        Return the name of the resource type
        """
        comment = " /* JediDBProxy.get_resource_type_task */"
        tmp_log = self.create_tagged_logger(comment)
        tmp_log.debug("start")

        resource_map = self.load_resource_types()

        for resource_spec in resource_map:
            if resource_spec.match_task(task_spec):
                tmp_log.debug(f"done. resource_type is {resource_spec.resource_name}")
                return resource_spec.resource_name

        tmp_log.debug("done. resource_type is Undefined")
        return "Undefined"

    def reset_resource_type_task(self, jedi_task_id, use_commit=True):
        """
        Retrieve the relevant task parameters and reset the resource type
        """
        comment = " /* JediDBProxy.reset_resource_type */"
        tmp_log = self.create_tagged_logger(comment)
        tmp_log.debug("start")

        # 1. Get the task parameters
        var_map = {":jedi_task_id": jedi_task_id}
        sql = f"SELECT corecount, ramcount, baseramcount, ramunit FROM {panda_config.schemaJEDI}.jedi_tasks WHERE jeditaskid = :jedi_task_id "
        self.cur.execute(sql + comment, var_map)
        corecount, ramcount, baseramcount, ramunit = self.cur.fetchone()
        tmp_log.debug(
            "retrieved following values for jediTaskid {0}: corecount {1}, ramcount {2}, baseramcount {3}, ramunit {4}".format(
                jedi_task_id, corecount, ramcount, baseramcount, ramunit
            )
        )

        # 2. Load the resource types and figure out the matching one
        resource_map = self.load_resource_types(use_commit=False)
        resource_name = "Undefined"
        for resource_spec in resource_map:
            if resource_spec.match_task_basic(corecount, ramcount, baseramcount, ramunit):
                resource_name = resource_spec.resource_name
                break

        tmp_log.debug(f"decided resource_type {resource_name} jediTaskid {jedi_task_id}")

        # 3. Update the task
        try:
            var_map = {":jedi_task_id": jedi_task_id, ":resource_type": resource_name}
            sql = f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks SET resource_type = :resource_type WHERE jeditaskid = :jedi_task_id "
            tmp_log.debug("conn begin...")
            if use_commit:
                self.conn.begin()
            tmp_log.debug("execute...")
            self.cur.execute(sql + comment, var_map)
            tmp_log.debug("commit...")
            if use_commit:
                if not self._commit():
                    raise RuntimeError("Commit error")
                tmp_log.debug("commited...")
        except Exception:
            # roll back
            if use_commit:
                tmp_log.debug("rolling back...")
                self._rollback()
            self.dump_error_message(tmp_log)
            tmp_log.error(f"{comment}: {sql} {var_map}")
            return False

        tmp_log.debug("done")
        return True

    def get_resource_type_job(self, job_spec):
        """
        Identify the resource type of the job based on the resource type map.
        Return the name of the resource type
        """
        comment = " /* JediDBProxy.get_resource_type_job */"
        tmp_log = self.create_tagged_logger(comment)
        tmp_log.debug("start")

        resource_map = self.load_resource_types()
        tmp_log.debug(
            "going to call match_job for pandaid {0} with minRamCount {1} (type{2}) and coreCount {3} (type{4})".format(
                job_spec.PandaID,
                job_spec.minRamCount,
                type(job_spec.minRamCount),
                job_spec.coreCount,
                type(job_spec.coreCount),
            )
        )
        resource_type = JobUtils.get_resource_type_job(resource_map, job_spec)
        tmp_log.debug(f"done. resource_type is {resource_type}")
        return resource_type

    # get the resource type of a site
    def get_rtype_site(self, site):
        comment = " /* DBProxy.get_rtype_site */"
        tmp_log = self.create_tagged_logger(comment)
        tmp_log.debug("start")
        try:
            var_map = {":site": site}
            sql = "SELECT /* use_json_type */ scj.data.resource_type FROM ATLAS_PANDA.schedconfig_json scj WHERE panda_queue=:site "

            # start transaction
            self.conn.begin()
            self.cur.arraysize = 1
            self.cur.execute(sql + comment, var_map)
            rtype = self.cur.fetchone()[0]
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmp_log.debug(f"site {site} has rtype: {rtype} ")
            return rtype
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return None

    def compare_share_task(self, share, task):
        """
        Logic to compare the relevant fields of share and task.
        Return: False if some condition does NOT match. True if all conditions match.
        """
        if share.prodsourcelabel is not None and task.prodSourceLabel is not None and re.match(share.prodsourcelabel, task.prodSourceLabel) is None:
            return False

        working_group = task.workingGroup
        if working_group is None:
            working_group = " "

        if share.workinggroup is not None and working_group is not None and re.match(share.workinggroup, working_group) is None:
            return False

        if share.campaign is not None and task.campaign and re.match(share.campaign, task.campaign) is None:
            return False

        if share.processingtype is not None and task.processingType is not None and re.match(share.processingtype, task.processingType) is None:
            return False

        if share.transpath is not None and task.transPath is not None and re.match(share.transpath, task.transPath) is None:
            return False

        if share.rtype is not None and task.site is not None:
            try:
                site = task.site.split(",")[0]  # if task assigned to more than one site, take the first one
                rtype_site = self.get_rtype_site(site)
                if rtype_site and re.match(share.rtype, rtype_site) is None:
                    return False
            except Exception:
                return False

        return True

    def compare_share_job(self, share, job):
        """
        Logic to compare the relevant fields of share and job. It's basically the same as compare_share_task, but
        does not check for the campaign field, which is not part of the job
        """

        if share.prodsourcelabel is not None and re.match(share.prodsourcelabel, job.prodSourceLabel) is None:
            return False

        if share.workinggroup is not None and re.match(share.workinggroup, job.workingGroup) is None:
            return False

        if share.processingtype is not None and re.match(share.processingtype, job.processingType) is None:
            return False

        if share.rtype is not None and job.computingSite is not None:
            try:
                site = job.computingSite.split(",")[0]
                rtype_site = self.get_rtype_site(site)
                if re.match(share.rtype, rtype_site) is None:
                    return False
            except Exception:
                return False

        return True

    def get_share_for_task(self, task):
        """
        Return the share based on a task specification
        """
        self.reload_shares()
        selected_share_name = "Undefined"

        for share in self.leave_shares:
            if self.compare_share_task(share, task):
                selected_share_name = share.name
                break

        if selected_share_name == "Undefined":
            self._log_stream.warning(
                "No share matching jediTaskId={0} (prodSourceLabel={1} workingGroup={2} campaign={3} transpath={4} site={5})".format(
                    task.jediTaskID,
                    task.prodSourceLabel,
                    task.workingGroup,
                    task.campaign,
                    task.transPath,
                    task.site,
                )
            )

        return selected_share_name

    def get_share_for_job(self, job):
        """
        Return the share based on a job specification
        """
        # special case: esmerge jobs go to Express share
        if job.eventService == EventServiceUtils.esMergeJobFlagNumber:
            return "Express"

        self.reload_shares()
        selected_share_name = "Undefined"

        for share in self.leave_shares:
            if self.compare_share_job(share, job):
                selected_share_name = share.name
                break

        if selected_share_name == "Undefined":
            self._log_stream.warning(f"No share matching PandaID={job.PandaID} (prodSourceLabel={job.prodSourceLabel} workingGroup={job.workingGroup})")

        return selected_share_name

    # get dispatch sorting criteria
    def getSortingCriteria(self, site_name, max_jobs):
        comment = " /* DBProxy.getSortingCriteria */"
        tmp_log = self.create_tagged_logger(comment)
        # throw the dice to decide the algorithm
        random_number = random.randrange(100)

        sloppy_ratio = self.getConfigValue("jobdispatch", "SLOPPY_DISPATCH_RATIO")
        if not sloppy_ratio:
            sloppy_ratio = 10

        tmp_log.debug(f"random_number: {random_number} sloppy_ratio: {sloppy_ratio}")

        if random_number <= sloppy_ratio:
            # generate the age sorting
            tmp_log.debug(f"sorting by age")
            return self.getCriteriaByAge(site_name, max_jobs)
        else:
            # generate the global share sorting
            tmp_log.debug(f"sorting by gshare")
            return self.getCriteriaForGlobalShares(site_name, max_jobs)

    # get selection criteria for share of production activities
    def getCriteriaForGlobalShares(self, site_name, max_jobs):
        comment = " /* DBProxy.getCriteriaForGlobalShare */"
        tmp_log = self.create_tagged_logger(comment)
        # return for no criteria
        var_map = {}
        ret_empty = "", {}

        try:
            # Get the share leaves sorted by order of under-pledging
            tmp_log.debug(f"Going to call get sorted leaves")
            t_before = time.time()
            sorted_leaves = self.get_sorted_leaves()
            t_after = time.time()
            total = t_after - t_before
            tmp_log.debug(f"Sorting leaves took {total}s")

            i = 0
            tmp_list = []
            for leave in sorted_leaves:
                var_map[f":leave{i}"] = leave.name
                if leave.name == "Test":
                    # Test share will bypass others for the moment
                    tmp_list.append(f"WHEN gshare=:leave{i} THEN 0")
                else:
                    tmp_list.append("WHEN gshare=:leave{0} THEN {0}".format(i))
                i += 1

            # Only get max_jobs, to avoid getting all activated jobs from the table
            var_map[":njobs"] = max_jobs

            # We want to sort by global share, highest priority and lowest pandaid
            leave_bindings = " ".join(tmp_list)
            ret_sql = f"""
                      ORDER BY (CASE {leave_bindings} ELSE {len(sorted_leaves)} END), currentpriority desc, pandaid asc)
                      WHERE ROWNUM <= :njobs
                      """

            tmp_log.debug(f"ret_sql: {ret_sql}. var_map: {var_map}")
            return ret_sql, var_map

        except Exception:
            # roll back
            self._rollback()
            self.dump_error_message(tmp_log)
            return ret_empty

    # get selection criteria for share of production activities
    def getCriteriaByAge(self, site_name, max_jobs):
        comment = " /* DBProxy.getCriteriaByAge */"
        tmp_log = self.create_tagged_logger(comment)
        # return for no criteria
        ret_sql = ""
        var_map = {}
        ret_empty = "", {}

        try:
            # Only get max_jobs, to avoid getting all activated jobs from the table
            var_map[":njobs"] = max_jobs

            # We want to ignore global share and just take the oldest pandaid
            ret_sql = """
                      ORDER BY pandaid asc)
                      WHERE ROWNUM <= :njobs
                      """

            tmp_log.debug(f"ret_sql: {ret_sql}. var_map: {var_map}")
            return ret_sql, var_map

        except Exception:
            # roll back
            self._rollback()
            self.dump_error_message(tmp_log)
            return ret_empty

    # set HS06sec
    def setHS06sec(self, pandaID, inActive=False):
        comment = " /* DBProxy.setHS06sec */"
        tmp_log = self.create_tagged_logger(comment, f"PandaID={pandaID}")
        tmp_log.debug("start")
        hs06sec = None

        # sql to get job attributes
        sqlJ = "SELECT jediTaskID,startTime,endTime,actualCoreCount,coreCount,jobMetrics,computingSite "
        if inActive:
            sqlJ += "FROM ATLAS_PANDA.jobsActive4 WHERE PandaID=:PandaID "
        else:
            sqlJ += "FROM ATLAS_PANDA.jobsArchived4 WHERE PandaID=:PandaID "

        # sql to update HS06sec
        if inActive:
            sqlU = "UPDATE ATLAS_PANDA.jobsActive4 "
        else:
            sqlU = "UPDATE ATLAS_PANDA.jobsArchived4 "
        sqlU += "SET hs06sec=:hs06sec WHERE PandaID=:PandaID "

        # get job attributes
        varMap = {}
        varMap[":PandaID"] = pandaID
        self.cur.execute(sqlJ + comment, varMap)
        resJ = self.cur.fetchone()
        if resJ is None:
            tmp_log.debug("skip since job not found")
        else:
            (
                jediTaskID,
                startTime,
                endTime,
                actualCoreCount,
                defCoreCount,
                jobMetrics,
                computingSite,
            ) = resJ
            # get corePower
            corePower, tmpMsg = self.get_core_power(computingSite)
            if corePower is None:
                tmp_log.debug(f"skip since corePower is undefined for site={computingSite}")
            else:
                # get core count
                coreCount = JobUtils.getCoreCount(actualCoreCount, defCoreCount, jobMetrics)
                # get HS06sec
                hs06sec = JobUtils.getHS06sec(startTime, endTime, corePower, coreCount)
                if hs06sec is None:
                    tmp_log.debug("skip since HS06sec is None")
                else:
                    # cap
                    hs06sec = int(hs06sec)
                    maxHS06sec = 999999999
                    if hs06sec > maxHS06sec:
                        hs06sec = maxHS06sec
                    # update HS06sec
                    varMap = {}
                    varMap[":PandaID"] = pandaID
                    varMap[":hs06sec"] = hs06sec
                    self.cur.execute(sqlU + comment, varMap)
                    tmp_log.debug(f"set HS06sec={hs06sec}")
        # return
        return hs06sec

    def convert_computingsite_to_region(self, computing_site):
        comment = " /* DBProxy.convert_computingsite_to_region */"

        var_map = {":panda_queue": computing_site}
        sql = "SELECT /* use_json_type */ scj.data.region FROM ATLAS_PANDA.schedconfig_json scj WHERE scj.panda_queue=:panda_queue"
        self.cur.arraysize = 100
        self.cur.execute(sql + comment, var_map)
        res_region = self.cur.fetchone()
        region = "GRID"  # when region is not defined, take average values
        if res_region:
            region = res_region[0]

        return region

    def get_co2_emissions_site(self, computing_site):
        comment = " /* DBProxy.get_co2_emissions_site */"
        region = self.convert_computingsite_to_region(computing_site)
        if not region:
            return None

        var_map = {":region": region}
        sql = "SELECT timestamp, region, value FROM ATLAS_PANDA.CARBON_REGION_EMISSIONS WHERE region=:region"
        self.cur.execute(sql + comment, var_map)
        results = self.cur.fetchall()
        return results

    def get_co2_emissions_grid(self):
        comment = " /* DBProxy.get_co2_emissions_grid */"

        sql = "SELECT timestamp, region, value FROM ATLAS_PANDA.CARBON_REGION_EMISSIONS WHERE region='GRID'"
        self.cur.execute(sql + comment)
        results = self.cur.fetchall()
        return results

    # set CO2 emissions
    def set_co2_emissions(self, panda_id, in_active=False):
        comment = " /* DBProxy.set_co2_emissions */"
        tmp_log = self.create_tagged_logger(comment, f"PandaID={panda_id}")
        tmp_log.debug("start")
        gco2_regional, gco2_global = None, None

        # sql to get job attributes
        sql_read = "SELECT jediTaskID, startTime, endTime, actualCoreCount, coreCount, jobMetrics, computingSite "
        if in_active:
            sql_read += "FROM ATLAS_PANDA.jobsActive4 WHERE PandaID=:PandaID "
        else:
            sql_read += "FROM ATLAS_PANDA.jobsArchived4 WHERE PandaID=:PandaID "

        # sql to update CO2 emissions
        if in_active:
            sql_update = "UPDATE ATLAS_PANDA.jobsActive4 "
        else:
            sql_update = "UPDATE ATLAS_PANDA.jobsArchived4 "
        sql_update += "SET gCO2_global=:gco2_global, gCO2_regional=:gco2_regional WHERE PandaID=:PandaID "

        # get job attributes
        var_map = {":PandaID": panda_id}
        self.cur.execute(sql_read + comment, var_map)
        res_read = self.cur.fetchone()
        if res_read is None:
            tmp_log.debug("skip since job not found")
        else:
            (
                task_id,
                start_time,
                end_time,
                actual_cores,
                defined_cores,
                job_metrics,
                computing_site,
            ) = res_read

            # get core count
            core_count = JobUtils.getCoreCount(actual_cores, defined_cores, job_metrics)

            # get the queues watts per core value
            var_map = {":panda_queue": computing_site}
            sql_wpc = "SELECT /* use_json_type */ scj.data.coreenergy FROM ATLAS_PANDA.schedconfig_json scj WHERE scj.panda_queue=:panda_queue"
            self.cur.arraysize = 100
            self.cur.execute(sql_wpc + comment, var_map)
            res_wpc = self.cur.fetchone()
            try:
                watts_per_core = float(res_wpc[0])
            except Exception:
                watts_per_core = 10
            tmp_log.debug(f"using watts_per_core={watts_per_core} for computing_site={computing_site}")

            # get regional CO2 emissions
            co2_emissions = self.get_co2_emissions_site(computing_site)
            if not co2_emissions:
                tmp_log.debug(f"skip since co2_emissions are undefined for site={computing_site}")
            else:
                # get emitted CO2 for the job
                gco2_regional = JobUtils.get_job_co2(start_time, end_time, core_count, co2_emissions, watts_per_core)
                if gco2_regional is None:
                    tmp_log.debug("skip since the co2 emissions could not be calculated")
                else:
                    max_gco2 = 999999999
                    gco2_regional = min(gco2_regional, max_gco2)
                    tmp_log.debug(f"set gco2_regional={gco2_regional}")

            # get globally averaged CO2 emissions
            co2_emissions = self.get_co2_emissions_grid()
            if not co2_emissions:
                tmp_log.debug("skip since co2_emissions are undefined for the grid")
            else:
                # get emitted CO2 for the job
                gco2_global = JobUtils.get_job_co2(start_time, end_time, core_count, co2_emissions, watts_per_core)
                if gco2_global is None:
                    tmp_log.debug("skip since the co2 emissions could not be calculated")
                else:
                    max_gco2 = 999999999
                    gco2_global = min(gco2_global, max_gco2)

                    tmp_log.debug(f"set gco2_global={gco2_global}")

            var_map = {
                ":PandaID": panda_id,
                ":gco2_regional": gco2_regional,
                ":gco2_global": gco2_global,
            }
            self.cur.execute(sql_update + comment, var_map)

        tmp_log.debug("done")
        # return
        return gco2_regional, gco2_global

    # get core power
    @memoize
    def get_core_power(self, site_id):
        comment = " /* DBProxy.get_core_power */"
        tmp_log = self.create_tagged_logger(comment, f"siteid={site_id}")
        tmp_log.debug("start")

        sqlS = "SELECT /* use_json_type */ scj.data.corepower FROM ATLAS_PANDA.schedconfig_json scj "
        sqlS += "WHERE panda_queue=:siteid "

        varMap = {":siteid": site_id}

        try:
            self.cur.arraysize = 100
            self.cur.execute(sqlS + comment, varMap)
            resS = self.cur.fetchone()
            core_power = None
            if resS is not None:
                (core_power,) = resS
                core_power = float(core_power)
            tmp_log.debug(f"got {core_power}")
            return core_power, None

        except Exception:
            # error
            self.dump_error_message(tmp_log)
            return None, "failed to get corePower"

    # convert ObjID to endpoint
    @memoize
    def convertObjIDtoEndPoint(self, srcFileName, objID):
        comment = " /* DBProxy.convertObjIDtoEndPoint */"
        tmp_log = self.create_tagged_logger(comment, f"ID={objID}")
        tmp_log.debug("start")
        try:
            for srcFile in srcFileName.split(","):
                if not os.path.exists(srcFile):
                    continue
                with open(srcFile) as f:
                    data = json.load(f)
                    for rseName in data:
                        rseData = data[rseName]
                        if objID in [rseData["id"], rseName]:
                            retMap = {
                                "name": rseName,
                                "is_deterministic": rseData["is_deterministic"],
                                "type": rseData["type"],
                            }
                            tmp_log.debug(f"got {str(retMap)}")
                            return retMap
            tmp_log.debug("not found")
        except Exception:
            # error
            self.dump_error_message(tmp_log)
            return None

    def get_config_for_pq(self, pq_name):
        """
        Get the CRIC json configuration for a particular queue
        """

        comment = " /* DBProxy.get_config_for_pq */"
        tmp_log = self.create_tagged_logger(comment, pq_name)
        tmp_log.debug("start")

        var_map = {":pq": pq_name}
        sql_get_queue_config = """
        SELECT data FROM ATLAS_PANDA.SCHEDCONFIG_JSON
        WHERE panda_queue = :pq
        """
        tmp_v, pq_data = self.getClobObj(sql_get_queue_config + comment, var_map)
        if pq_data is None:
            tmp_log.error("Could not find queue configuration")
            return None

        try:
            pq_data_des = pq_data[0][0]
            if not isinstance(pq_data_des, dict):
                pq_data_des = json.loads(pq_data_des)
        except Exception:
            tmp_log.error("Could not find queue configuration")
            return None

        tmp_log.debug("done")
        return pq_data_des

    def getQueuesInJSONSchedconfig(self):
        comment = " /* DBProxy.getQueuesInJSONSchedconfig */"
        tmp_log = self.create_tagged_logger(comment)
        tmp_log.debug("start")
        try:
            # sql to get workers
            sqlC = "SELECT /* use_json_type */ panda_queue FROM ATLAS_PANDA.schedconfig_json"
            # start transaction
            self.conn.begin()
            self.cur.execute(sqlC + comment)
            panda_queues = [row[0] for row in self.cur.fetchall()]
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmp_log.debug(f"got {len(panda_queues)} queues")
            return panda_queues
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return None

    # update queues
    def upsertQueuesInJSONSchedconfig(self, schedconfig_dump):
        comment = " /* DBProxy.upsertQueuesInJSONSchedconfig */"
        tmp_log = self.create_tagged_logger(comment)
        tmp_log.debug("start")

        if not schedconfig_dump:
            tmp_log.error("empty schedconfig dump")
            return "ERROR"

        try:
            existing_queues = self.getQueuesInJSONSchedconfig()
            if existing_queues is None:
                tmp_log.error("Could not retrieve already existing queues")
                return None

            # separate the queues to the ones we have to update (existing) and the ones we have to insert (new)
            var_map_insert = []
            var_map_update = []
            utc_now = naive_utcnow()
            for pq in schedconfig_dump:
                data = json.dumps(schedconfig_dump[pq])
                if not data:
                    tmp_log.error(f"no data for {pq}")
                    continue

                if pq in existing_queues:
                    tmp_log.debug(f"pq {pq} present")
                    var_map_update.append({":pq": pq, ":data": data, ":last_update": utc_now})
                else:
                    tmp_log.debug(f"pq {pq} is new")
                    var_map_insert.append({":pq": pq, ":data": data, ":last_update": utc_now})

            # start transaction
            self.conn.begin()

            # run the updates
            if var_map_update:
                sql_update = """
                             UPDATE ATLAS_PANDA.SCHEDCONFIG_JSON SET data = :data, last_update = :last_update
                             WHERE panda_queue = :pq
                             """
                tmp_log.debug("start updates")
                self.cur.executemany(sql_update + comment, var_map_update)
                tmp_log.debug("finished updates")

            # run the inserts
            if var_map_insert:
                sql_insert = """
                             INSERT INTO ATLAS_PANDA.SCHEDCONFIG_JSON (panda_queue, data, last_update)
                             VALUES (:pq, :data, :last_update)
                             """
                tmp_log.debug("start inserts")
                self.cur.executemany(sql_insert + comment, var_map_insert)
                tmp_log.debug("finished inserts")

            # delete inactive queues
            tmp_log.debug("Going to delete obsoleted queues")
            sql_delete = """
                         DELETE FROM ATLAS_PANDA.SCHEDCONFIG_JSON WHERE last_update < current_date - INTERVAL '7' DAY
                         """
            self.cur.execute(sql_delete + comment)
            tmp_log.debug("deleted old queues")

            if not self._commit():
                raise RuntimeError("Commit error")

            tmp_log.debug("done")
            return "OK"

        except Exception:
            # roll back
            self._rollback()
            self.dump_error_message(tmp_log)
            return "ERROR"

    # update queues
    def loadSWTags(self, sw_tags):
        comment = " /* DBProxy.loadSWTags */"
        tmp_log = self.create_tagged_logger(comment)
        tmp_log.debug("start")

        if not sw_tags:
            tmp_log.error("empty sw tag dump")
            return "ERROR"

        try:
            var_map_tags = []

            utc_now = naive_utcnow()
            for pq in sw_tags:
                data = sw_tags[pq]
                var_map_tags.append({":pq": pq, ":data": json.dumps(data), ":last_update": utc_now})

            # start transaction on SW_TAGS table
            # delete everything in the table to start every time from a clean table
            # cleaning and filling needs to be done within the same transaction
            self.conn.begin()

            sql_delete = "DELETE FROM ATLAS_PANDA.SW_TAGS"
            tmp_log.debug("start cleaning up SW_TAGS table")
            self.cur.execute(sql_delete + comment)
            tmp_log.debug("done cleaning up SW_TAGS table")

            sql_insert = "INSERT INTO ATLAS_PANDA.SW_TAGS (panda_queue, data, last_update) VALUES (:pq, :data, :last_update)"
            tmp_log.debug("start filling up SW_TAGS table")
            for shard in create_shards(var_map_tags, 100):  # insert in batches of 100 rows
                self.cur.executemany(sql_insert + comment, shard)
            tmp_log.debug("done filling up table")
            if not self._commit():
                raise RuntimeError("Commit error")

            tmp_log.debug("done")
            return "OK"

        except Exception:
            # roll back
            self._rollback()
            self.dump_error_message(tmp_log)
            return "ERROR"

    # get working group with production role
    def getWorkingGroup(self, fqans):
        for fqan in fqans:
            # check production role
            match = re.search("/[^/]+/([^/]+)/Role=production", fqan)
            if match is not None:
                return match.group(1)
        return None

    # update site data
    def updateSiteData(self, hostID, pilotRequests, interval):
        comment = " /* DBProxy.updateSiteData */"
        tmp_log = self.create_tagged_logger(comment)
        tmp_log.debug("start")

        sqlDel = "DELETE FROM ATLAS_PANDAMETA.SiteData WHERE LASTMOD < :LASTMOD"

        sqlRst = (
            "UPDATE ATLAS_PANDAMETA.SiteData "
            "SET GETJOB = :GETJOB, UPDATEJOB = :UPDATEJOB, NOJOB = :NOJOB, "
            "GETJOBABS = :GETJOBABS, UPDATEJOBABS = :UPDATEJOBABS, NOJOBABS = :NOJOBABS "
            "WHERE HOURS = :HOURS AND LASTMOD < :LASTMOD"
        )

        sqlCh = "SELECT * FROM ATLAS_PANDAMETA.SiteData WHERE FLAG = :FLAG AND HOURS = :HOURS AND SITE = :SITE FOR UPDATE NOWAIT "

        sqlIn = (
            "INSERT INTO ATLAS_PANDAMETA.SiteData "
            "(SITE, FLAG, HOURS, GETJOB, UPDATEJOB, NOJOB, GETJOBABS, UPDATEJOBABS, NOJOBABS, "
            "LASTMOD, NSTART, FINISHED, FAILED, DEFINED, ASSIGNED, WAITING, ACTIVATED, HOLDING, RUNNING, TRANSFERRING) "
            "VALUES (:SITE, :FLAG, :HOURS, :GETJOB, :UPDATEJOB, :NOJOB, :GETJOBABS, :UPDATEJOBABS, :NOJOBABS, CURRENT_DATE, "
            "0, 0, 0, 0, 0, 0, 0, 0, 0, 0)"
        )

        sqlUp = (
            "UPDATE ATLAS_PANDAMETA.SiteData "
            "SET GETJOB = :GETJOB, UPDATEJOB = :UPDATEJOB, NOJOB = :NOJOB, "
            "GETJOBABS = :GETJOBABS, UPDATEJOBABS = :UPDATEJOBABS, NOJOBABS = :NOJOBABS, LASTMOD = CURRENT_DATE "
            "WHERE FLAG = :FLAG AND HOURS = :HOURS AND SITE = :SITE"
        )

        sqlAll = "SELECT GETJOB, UPDATEJOB, NOJOB, GETJOBABS, UPDATEJOBABS, NOJOBABS, FLAG FROM ATLAS_PANDAMETA.SiteData WHERE HOURS = :HOURS AND SITE = :SITE"

        try:
            timeNow = naive_utcnow()
            self.conn.begin()
            # delete old records
            varMap = {}
            varMap[":LASTMOD"] = timeNow - datetime.timedelta(hours=48)
            self.cur.execute(sqlDel + comment, varMap)
            # set 0 to old records
            varMap = {}
            varMap[":HOURS"] = interval
            varMap[":GETJOB"] = 0
            varMap[":UPDATEJOB"] = 0
            varMap[":NOJOB"] = 0
            varMap[":GETJOBABS"] = 0
            varMap[":UPDATEJOBABS"] = 0
            varMap[":NOJOBABS"] = 0
            varMap[":LASTMOD"] = timeNow - datetime.timedelta(hours=interval)
            self.cur.execute(sqlRst + comment, varMap)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # shuffle to avoid concatenation
            tmpSiteList = list(pilotRequests)
            random.shuffle(tmpSiteList)
            # loop over all sites
            for tmpSite in tmpSiteList:
                tmpVal = pilotRequests[tmpSite]
                # start transaction
                self.conn.begin()
                # check individual host info first
                varMap = {}
                varMap[":FLAG"] = hostID
                varMap[":SITE"] = tmpSite
                varMap[":HOURS"] = interval
                self.cur.arraysize = 10
                locked = True
                try:
                    # lock individual row
                    self.cur.execute(sqlCh + comment, varMap)
                except Exception as e:
                    # skip since it is being locked by another
                    tmp_log.debug(f"skip to update {str(varMap)} due to {str(e)}")
                    locked = False
                if locked:
                    res = self.cur.fetchone()
                    # row exists or not
                    if res is None:
                        sql = sqlIn
                    else:
                        sql = sqlUp

                    # getJob, updateJob and noJob entries contain the number of slots/nodes that submitted the request
                    # getJobAbs, updateJobAbs and noJobAbs entries contain the absolute number of requests
                    if "getJob" in tmpVal:
                        varMap[":GETJOB"] = len(tmpVal["getJob"])
                        getJobAbs = 0
                        for node in tmpVal["getJob"]:
                            getJobAbs += tmpVal["getJob"][node]
                        varMap[":GETJOBABS"] = getJobAbs
                    else:
                        varMap[":GETJOB"] = 0
                        varMap[":GETJOBABS"] = 0

                    if "updateJob" in tmpVal:
                        varMap[":UPDATEJOB"] = len(tmpVal["updateJob"])
                        updateJobAbs = 0
                        for node in tmpVal["updateJob"]:
                            updateJobAbs += tmpVal["updateJob"][node]
                        varMap[":UPDATEJOBABS"] = updateJobAbs
                    else:
                        varMap[":UPDATEJOB"] = 0
                        varMap[":UPDATEJOBABS"] = 0

                    if "noJob" in tmpVal:
                        varMap[":NOJOB"] = len(tmpVal["noJob"])
                        noJobAbs = 0
                        for node in tmpVal["noJob"]:
                            noJobAbs += tmpVal["noJob"][node]
                        varMap[":NOJOBABS"] = noJobAbs
                    else:
                        varMap[":NOJOB"] = 0
                        varMap[":NOJOBABS"] = 0

                    # update
                    self.cur.execute(sql + comment, varMap)

                # commit
                if not self._commit():
                    raise RuntimeError("Commit error")

                if locked:
                    # start transaction
                    self.conn.begin()
                    # get all info
                    sumExist = False
                    varMap = {}
                    varMap[":SITE"] = tmpSite
                    varMap[":HOURS"] = interval
                    self.cur.arraysize = 100
                    self.cur.execute(sqlAll + comment, varMap)
                    res = self.cur.fetchall()
                    # get total getJob/updateJob
                    varMap[":GETJOB"] = 0
                    varMap[":UPDATEJOB"] = 0
                    varMap[":NOJOB"] = 0
                    varMap[":GETJOBABS"] = 0
                    varMap[":UPDATEJOBABS"] = 0
                    varMap[":NOJOBABS"] = 0
                    nCol = 0
                    for (
                        tmpGetJob,
                        tmpUpdateJob,
                        tmpNoJob,
                        tmpGetJobAbs,
                        tmpUpdateJobAbs,
                        tmpNoJobAbs,
                        tmpFlag,
                    ) in res:
                        # don't use summed info
                        if tmpFlag == "production":
                            sumExist = True
                            continue
                        if tmpFlag == "analysis":
                            if tmpSite.startswith("ANALY_"):
                                sumExist = True
                            continue
                        if tmpFlag in ["test"]:
                            continue

                        if tmpGetJob is None:
                            tmpGetJob = 0
                        if tmpUpdateJob is None:
                            tmpUpdateJob = 0
                        if tmpNoJob is None:
                            tmpNoJob = 0
                        if tmpGetJobAbs is None:
                            tmpGetJobAbs = 0
                        if tmpUpdateJobAbs is None:
                            tmpUpdateJobAbs = 0
                        if tmpNoJobAbs is None:
                            tmpNoJobAbs = 0

                        # sum
                        varMap[":GETJOB"] += tmpGetJob
                        varMap[":UPDATEJOB"] += tmpUpdateJob
                        varMap[":NOJOB"] += tmpNoJob
                        varMap[":GETJOBABS"] += tmpGetJobAbs
                        varMap[":UPDATEJOBABS"] += tmpUpdateJobAbs
                        varMap[":NOJOBABS"] += tmpNoJobAbs
                        nCol += 1
                    # get average
                    if nCol != 0:
                        if varMap[":GETJOB"] >= nCol:
                            varMap[":GETJOB"] /= nCol
                        if varMap[":UPDATEJOB"] >= nCol:
                            varMap[":UPDATEJOB"] /= nCol
                        if varMap[":NOJOB"] >= nCol:
                            varMap[":NOJOB"] /= nCol
                        if varMap[":GETJOBABS"] >= nCol:
                            varMap[":GETJOBABS"] /= nCol
                        if varMap[":UPDATEJOBABS"] >= nCol:
                            varMap[":UPDATEJOBABS"] /= nCol
                        if varMap[":NOJOBABS"] >= nCol:
                            varMap[":NOJOBABS"] /= nCol

                    if tmpSite.startswith("ANALY_"):
                        varMap[":FLAG"] = "analysis"
                    else:
                        varMap[":FLAG"] = "production"
                    # row exists or not
                    locked_sum = True
                    if sumExist:
                        sql = sqlUp
                    else:
                        sql = sqlIn
                        # lock the summary row
                        var_map = {k: varMap[k] for k in [":FLAG", ":SITE", ":HOURS"]}
                        try:
                            # lock it
                            self.cur.execute(sqlCh + comment, var_map)
                        except Exception as e:
                            # skip since it is being locked by another
                            tmp_log.debug(f"skip to update {str(var_map)} due to {str(e)}")
                            locked_sum = False
                    # update
                    if locked_sum:
                        self.cur.execute(sql + comment, varMap)
                        tmp_log.debug(
                            " %s hours=%s getJob=%s updateJob=%s, noJob=%s, getJobAbs=%s updateJobAbs=%s, noJobAbs=%s"
                            % (
                                tmpSite,
                                interval,
                                varMap[":GETJOB"],
                                varMap[":UPDATEJOB"],
                                varMap[":NOJOB"],
                                varMap[":GETJOBABS"],
                                varMap[":UPDATEJOBABS"],
                                varMap[":NOJOBABS"],
                            )
                        )
                        # commit
                    if not self._commit():
                        raise RuntimeError("Commit error")
            tmp_log.debug("done")
            return True
        except Exception:
            # roll back
            self._rollback()
            self.dump_error_message(tmp_log)
            return False

    # get site data
    def getCurrentSiteData(self):
        comment = " /* DBProxy.getCurrentSiteData */"
        tmp_log = self.create_tagged_logger(comment)
        sql = "SELECT SITE,getJob,updateJob,FLAG FROM ATLAS_PANDAMETA.SiteData WHERE FLAG IN (:FLAG1,:FLAG2) and HOURS=3"
        varMap = {}
        varMap[":FLAG1"] = "production"
        varMap[":FLAG2"] = "analysis"
        try:
            # set autocommit on
            self.conn.begin()
            # select
            self.cur.arraysize = 10000
            self.cur.execute(sql + comment, varMap)
            res = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            ret = {}
            for site, getJob, updateJob, flag in res:
                if site.startswith("ANALY_"):
                    if flag != "analysis":
                        continue
                else:
                    if flag != "production":
                        continue
                ret[site] = {"getJob": getJob, "updateJob": updateJob}
            return ret
        except Exception:
            self.dump_error_message(tmp_log)
            # roll back
            self._rollback()
            return {}

    # insert nRunning in site data
    def insertnRunningInSiteData(self):
        comment = " /* DBProxy.insertnRunningInSiteData */"
        tmp_log = self.create_tagged_logger(comment)
        tmp_log.debug("start")

        sqlDel = "DELETE FROM ATLAS_PANDAMETA.SiteData WHERE FLAG IN (:FLAG1, :FLAG2) AND LASTMOD < CURRENT_DATE - 1"

        sqlRun = (
            "SELECT COUNT(*), computingSite "
            "FROM ATLAS_PANDA.jobsActive4 "
            "WHERE prodSourceLabel IN (:prodSourceLabel1, :prodSourceLabel2) "
            "AND jobStatus = :jobStatus "
            "GROUP BY computingSite"
        )

        sqlCh = "SELECT COUNT(*) FROM ATLAS_PANDAMETA.SiteData WHERE FLAG = :FLAG AND HOURS = :HOURS AND SITE = :SITE"

        sqlIn = (
            "INSERT INTO ATLAS_PANDAMETA.SiteData "
            "(SITE, FLAG, HOURS, GETJOB, UPDATEJOB, LASTMOD, "
            "NSTART, FINISHED, FAILED, DEFINED, ASSIGNED, WAITING, "
            "ACTIVATED, HOLDING, RUNNING, TRANSFERRING) "
            "VALUES (:SITE, :FLAG, :HOURS, 0, 0, CURRENT_DATE, "
            "0, 0, 0, 0, 0, 0, 0, 0, :RUNNING, 0)"
        )

        sqlUp = "UPDATE ATLAS_PANDAMETA.SiteData SET RUNNING = :RUNNING, LASTMOD = CURRENT_DATE WHERE FLAG = :FLAG AND HOURS = :HOURS AND SITE = :SITE"

        sqlMax = "SELECT SITE, MAX(RUNNING) FROM ATLAS_PANDAMETA.SiteData WHERE FLAG = :FLAG GROUP BY SITE"

        try:
            # use offset(1000)+minutes for :HOURS
            timeNow = naive_utcnow()
            nHours = 1000 + timeNow.hour * 60 + timeNow.minute
            # delete old records
            varMap = {}
            varMap[":FLAG1"] = "max"
            varMap[":FLAG2"] = "snapshot"
            self.conn.begin()
            self.cur.execute(sqlDel + comment, varMap)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # get nRunning
            varMap = {}
            varMap[":jobStatus"] = "running"
            varMap[":prodSourceLabel1"] = "user"
            varMap[":prodSourceLabel2"] = "panda"
            self.conn.begin()
            self.cur.arraysize = 10000
            self.cur.execute(sqlRun + comment, varMap)
            res = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # loop over all sites
            for nRunning, computingSite in res:
                # only ANALY_ sites
                if not computingSite.startswith("ANALY_"):
                    continue
                # check if the row is already there
                varMap = {}
                varMap[":FLAG"] = "snapshot"
                varMap[":SITE"] = computingSite
                varMap[":HOURS"] = nHours
                # start transaction
                self.conn.begin()
                self.cur.arraysize = 10
                self.cur.execute(sqlCh + comment, varMap)
                res = self.cur.fetchone()
                # row exists or not
                if res[0] == 0:
                    sql = sqlIn
                else:
                    sql = sqlUp
                # set current nRunning
                varMap = {}
                varMap[":FLAG"] = "snapshot"
                varMap[":SITE"] = computingSite
                varMap[":HOURS"] = nHours
                varMap[":RUNNING"] = nRunning
                # insert or update
                self.cur.execute(sql + comment, varMap)
                # commit
                if not self._commit():
                    raise RuntimeError("Commit error")
            # get max nRunning
            varMap = {}
            varMap[":FLAG"] = "snapshot"
            self.conn.begin()
            self.cur.arraysize = 10000
            self.cur.execute(sqlMax + comment, varMap)
            res = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # loop over all sites
            for computingSite, maxnRunning in res:
                # start transaction
                self.conn.begin()
                # check if the row is already there
                varMap = {}
                varMap[":FLAG"] = "max"
                varMap[":SITE"] = computingSite
                varMap[":HOURS"] = 0
                self.cur.arraysize = 10
                self.cur.execute(sqlCh + comment, varMap)
                res = self.cur.fetchone()
                # row exists or not
                if res[0] == 0:
                    sql = sqlIn
                else:
                    sql = sqlUp
                # set max nRunning
                varMap = {}
                varMap[":FLAG"] = "max"
                varMap[":SITE"] = computingSite
                varMap[":HOURS"] = 0
                varMap[":RUNNING"] = maxnRunning
                self.cur.execute(sql + comment, varMap)
                # commit
                if not self._commit():
                    raise RuntimeError("Commit error")
            tmp_log.debug("done")
            return True
        except Exception:
            # roll back
            self._rollback()
            self.dump_error_message(tmp_log)
            return False

    # get site info
    def getSiteInfo(self):
        comment = " /* DBProxy.getSiteInfo */"
        tmp_log = self.create_tagged_logger(comment)
        tmp_log.debug("start")
        try:
            # get DDM endpoints
            pandaEndpointMap, endpoint_detailed_status_summary = self.getDdmEndpoints()

            # sql to get site spec
            sql = """
                   SELECT /* use_json_type */ panda_queue, data, b.site_name, c.role
                   FROM (ATLAS_PANDA.schedconfig_json a
                   LEFT JOIN ATLAS_PANDA.panda_site b ON a.panda_queue = b.panda_site_name)
                   LEFT JOIN ATLAS_PANDA.site c ON b.site_name = c.site_name
                   WHERE panda_queue IS NOT NULL
                   """
            self.cur.arraysize = 10000
            # self.cur.execute(sql+comment)
            # resList = self.cur.fetchall()
            ret, resList = self.getClobObj(sql, {})
            if not resList:
                tmp_log.error("Empty site list!")

            # set autocommit on
            self.conn.begin()
            # sql to get num slots
            sqlSL = "SELECT pandaQueueName, gshare, resourcetype, numslots FROM ATLAS_PANDA.Harvester_Slots "
            sqlSL += "WHERE (expirationTime IS NULL OR expirationTime>CURRENT_DATE) "

            num_slots_by_site = {}
            self.cur.execute(sqlSL + comment)
            resSL = self.cur.fetchall()

            for sl_queuename, sl_gshare, sl_resourcetype, sl_numslots in resSL:
                if sl_numslots < 0:
                    continue
                num_slots_by_site.setdefault(sl_queuename, {})
                num_slots_by_site[sl_queuename].setdefault(sl_gshare, {})
                num_slots_by_site[sl_queuename][sl_gshare][sl_resourcetype] = sl_numslots

            retList = {}
            if resList is not None:
                # loop over all results
                for res in resList:
                    try:  # don't let a problem with one queue break the whole map
                        # change None to ''
                        resTmp = []
                        for tmpItem in res:
                            if tmpItem is None:
                                tmpItem = ""
                            resTmp.append(tmpItem)

                        siteid, queue_data_json, pandasite, role = resTmp
                        try:
                            if isinstance(queue_data_json, dict):
                                queue_data = queue_data_json
                            else:
                                queue_data = json.loads(queue_data_json)
                        except Exception:
                            tmp_log.error(f"loading json for queue {siteid} excepted. json was: {queue_data_json}")
                            continue

                        # skip invalid siteid
                        if siteid in [None, "", "ALL"] or not queue_data:
                            if siteid != "ALL":  # skip noisy error message for ALL
                                tmp_log.error(f"siteid {siteid} had no queue_data {queue_data}")
                            continue

                        tmp_log.debug(f"processing queue {siteid}")

                        # instantiate SiteSpec
                        ret = SiteSpec.SiteSpec()
                        ret.sitename = siteid
                        ret.pandasite = pandasite
                        ret.role = role

                        ret.type = queue_data.get("type", "production")
                        ret.nickname = queue_data.get("nickname")
                        try:
                            ret.ddm = queue_data.get("ddm", "").split(",")[0]
                        except AttributeError:
                            ret.ddm = ""
                        try:
                            ret.cloud = queue_data.get("cloud", "").split(",")[0]
                        except AttributeError:
                            ret.cloud = ""
                        ret.memory = queue_data.get("memory")
                        ret.maxrss = queue_data.get("maxrss")
                        ret.minrss = queue_data.get("minrss")
                        ret.maxtime = queue_data.get("maxtime")
                        ret.status = queue_data.get("status")
                        ret.space = queue_data.get("space")
                        ret.maxinputsize = queue_data.get("maxinputsize")
                        ret.comment = queue_data.get("comment_")
                        ret.statusmodtime = queue_data.get("lastmod")
                        ret.catchall = queue_data.get("catchall")
                        ret.tier = queue_data.get("tier")
                        ret.jobseed = queue_data.get("jobseed")
                        ret.capability = queue_data.get("capability")
                        ret.workflow = queue_data.get("workflow")
                        ret.maxDiskio = queue_data.get("maxdiskio")
                        ret.pandasite_state = "ACTIVE"
                        ret.fairsharePolicy = queue_data.get("fairsharepolicy")
                        ret.defaulttoken = queue_data.get("defaulttoken")

                        ret.direct_access_lan = queue_data.get("direct_access_lan") is True
                        ret.direct_access_wan = queue_data.get("direct_access_wan") is True

                        ret.iscvmfs = queue_data.get("is_cvmfs") is True

                        if queue_data.get("corepower") is None:
                            ret.corepower = 0
                        else:
                            ret.corepower = queue_data.get("corepower")

                        ret.wnconnectivity = queue_data.get("wnconnectivity")
                        if ret.wnconnectivity == "":
                            ret.wnconnectivity = None

                        # maxwdir
                        try:
                            if queue_data.get("maxwdir") is None:
                                ret.maxwdir = 0
                            else:
                                ret.maxwdir = int(queue_data["maxwdir"])
                        except Exception:
                            if ret.maxinputsize in [0, None]:
                                ret.maxwdir = 0
                            else:
                                try:
                                    ret.maxwdir = ret.maxinputsize + 2000
                                except Exception:
                                    ret.maxwdir = 16336

                        # mintime
                        if queue_data.get("mintime") is not None:
                            ret.mintime = queue_data["mintime"]
                        else:
                            ret.mintime = 0

                        # reliability
                        ret.reliabilityLevel = None

                        # pledged CPUs
                        ret.pledgedCPU = 0
                        if queue_data.get("pledgedcpu") not in ["", None]:
                            try:
                                ret.pledgedCPU = int(queue_data["pledgedcpu"])
                            except Exception:
                                pass

                        # core count
                        ret.coreCount = 0
                        if queue_data.get("corecount") not in ["", None]:
                            try:
                                ret.coreCount = int(queue_data["corecount"])
                            except Exception:
                                pass

                        # convert releases to list
                        ret.releases = []
                        if queue_data.get("releases"):
                            ret.releases = queue_data["releases"]

                        # convert validatedreleases to list
                        ret.validatedreleases = []
                        if queue_data.get("validatedreleases"):
                            for tmpRel in queue_data["validatedreleases"].split("|"):
                                # remove white space
                                tmpRel = tmpRel.strip()
                                if tmpRel != "":
                                    ret.validatedreleases.append(tmpRel)

                        # limit of the number of transferring jobs
                        ret.transferringlimit = 0
                        if queue_data.get("transferringlimit") not in ["", None]:
                            try:
                                ret.transferringlimit = int(queue_data["transferringlimit"])
                            except Exception:
                                pass

                        # FAX
                        ret.allowfax = False
                        try:
                            if queue_data.get("catchall") is not None and "allowfax" in queue_data["catchall"]:
                                ret.allowfax = True
                            if queue_data.get("allowfax") is True:
                                ret.allowfax = True
                        except Exception:
                            pass

                        # DDM endpoints
                        ret.ddm_endpoints_input = {}
                        ret.ddm_endpoints_output = {}
                        if siteid in pandaEndpointMap:
                            for scope in pandaEndpointMap[siteid]:
                                if "input" in pandaEndpointMap[siteid][scope]:
                                    ret.ddm_endpoints_input[scope] = pandaEndpointMap[siteid][scope]["input"]
                                if "output" in pandaEndpointMap[siteid][scope]:
                                    ret.ddm_endpoints_output[scope] = pandaEndpointMap[siteid][scope]["output"]
                        else:
                            # empty
                            ret.ddm_endpoints_input["default"] = DdmSpec()
                            ret.ddm_endpoints_output["default"] = DdmSpec()

                        # initialize dictionary fields
                        ret.setokens_input = {}
                        ret.setokens_output = {}
                        ret.ddm_input = {}
                        for scope in ret.ddm_endpoints_input:
                            # mapping between token and endpoints
                            ret.setokens_input[scope] = ret.ddm_endpoints_input[scope].getTokenMap("input")
                            # set DDM to the default endpoint
                            ret.ddm_input[scope] = ret.ddm_endpoints_input[scope].getDefaultRead()

                        ret.ddm_output = {}
                        for scope in ret.ddm_endpoints_output:
                            # mapping between token and endpoints
                            ret.setokens_output[scope] = ret.ddm_endpoints_output[scope].getTokenMap("output")
                            # set DDM to the default endpoint
                            ret.ddm_output[scope] = ret.ddm_endpoints_output[scope].getDefaultWrite()

                        # object stores
                        try:
                            ret.objectstores = queue_data["objectstores"]
                        except Exception:
                            ret.objectstores = []

                        # default unified flag
                        ret.is_unified = False

                        # num slots
                        ret.num_slots_map = num_slots_by_site.get(siteid, {})

                        # append
                        retList[ret.nickname] = ret
                    except Exception:
                        tmp_log.error(f"exception in queue: {traceback.format_exc()}")
                        continue
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmp_log.debug("done")
            return retList, endpoint_detailed_status_summary
        except Exception as e:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return {}, {}

    def getDdmEndpoints(self):
        """
        get list of ddm input endpoints
        """
        comment = " /* DBProxy.getDdmEndpoints */"
        tmp_log = self.create_tagged_logger(comment)
        tmp_log.debug(f"start")

        # get all ddm endpoints
        sql_ddm = "SELECT * FROM ATLAS_PANDA.ddm_endpoint "
        self.cur.arraysize = 10000
        self.cur.execute(f"{sql_ddm}{comment}")
        results_ddm = self.cur.fetchall()

        # extract the column names from the query
        column_names = [i[0].lower() for i in self.cur.description]

        # save the endpoints into a dictionary
        endpoint_dict = {}
        detailed_status_summary = {}
        for ddm_endpoint_row in results_ddm:
            tmp_endpoint = {}
            # unzip the ddm_endpoint row into a dictionary
            for column_name, column_val in zip(column_names, ddm_endpoint_row):
                tmp_endpoint[column_name] = column_val

            ddm_endpoint_name = tmp_endpoint["ddm_endpoint_name"]
            try:
                tmp_detailed_status = tmp_endpoint["detailed_status"]
                if not isinstance(tmp_detailed_status, dict):
                    tmp_detailed_status = json.loads(tmp_detailed_status)
                # make a summary of detailed status of all endpoints
                if tmp_detailed_status:
                    for tmp_activity, tmp_status in tmp_detailed_status.items():
                        detailed_status_summary.setdefault(tmp_activity, {})
                        detailed_status_summary[tmp_activity].setdefault(tmp_status, [])
                        detailed_status_summary[tmp_activity][tmp_status].append(ddm_endpoint_name)
                tmp_endpoint["detailed_status"] = tmp_detailed_status
            except Exception as e:
                tmp_log.error(f"exception when decoding detailed_status for {ddm_endpoint_name}: {str(e)}")
                tmp_endpoint["detailed_status"] = {}
            endpoint_dict[ddm_endpoint_name] = tmp_endpoint

        # get relationship between panda sites and ddm endpoints
        sql_panda_ddm = """
               SELECT pdr.panda_site_name, pdr.ddm_endpoint_name, pdr.is_local, de.ddm_spacetoken_name,
                      de.is_tape, pdr.default_read, pdr.default_write, pdr.roles, pdr.order_read, pdr.order_write,
                      nvl(pdr.scope, 'default') as scope, de.blacklisted_read
               FROM ATLAS_PANDA.panda_ddm_relation pdr, ATLAS_PANDA.ddm_endpoint de
               WHERE pdr.ddm_endpoint_name = de.ddm_endpoint_name
               """
        if self.backend == "mysql":
            sql_panda_ddm = """
               SELECT pdr.panda_site_name, pdr.ddm_endpoint_name, pdr.is_local, de.ddm_spacetoken_name,
                      de.is_tape, pdr.default_read, pdr.default_write, pdr.roles, pdr.order_read, pdr.order_write,
                      ifnull(pdr.scope, 'default') as scope, de.blacklisted
               FROM ATLAS_PANDA.panda_ddm_relation pdr, ATLAS_PANDA.ddm_endpoint de
               WHERE pdr.ddm_endpoint_name = de.ddm_endpoint_name
               """

        self.cur.execute(f"{sql_panda_ddm}{comment}")
        results_panda_ddm = self.cur.fetchall()
        column_names = [i[0].lower() for i in self.cur.description]

        # save the panda ddm relations into a dictionary
        panda_endpoint_map = {}
        for panda_ddm_row in results_panda_ddm:
            tmp_relation = {}
            for column_name, column_val in zip(column_names, panda_ddm_row):
                # Default unavailable endpoint space to 0
                if column_name.startswith("space_") and column_val is None:
                    column_val = 0
                tmp_relation[column_name] = column_val

            # add the relations to the panda endpoint map
            panda_site_name = tmp_relation["panda_site_name"]
            scope = tmp_relation["scope"]
            panda_endpoint_map.setdefault(panda_site_name, {})
            panda_endpoint_map[panda_site_name].setdefault(scope, {})

            if panda_site_name not in panda_endpoint_map:
                panda_endpoint_map[panda_site_name] = {
                    "input": DdmSpec(),
                    "output": DdmSpec(),
                }
            if "read_lan" in tmp_relation["roles"] and tmp_relation["blacklisted_read"] != "Y":
                panda_endpoint_map[panda_site_name][scope].setdefault("input", DdmSpec())
                panda_endpoint_map[panda_site_name][scope]["input"].add(tmp_relation, endpoint_dict)
            if "write_lan" in tmp_relation["roles"]:
                panda_endpoint_map[panda_site_name][scope].setdefault("output", DdmSpec())
                panda_endpoint_map[panda_site_name][scope]["output"].add(tmp_relation, endpoint_dict)

        tmp_log.debug(f"done")
        return panda_endpoint_map, detailed_status_summary

    def get_cloud_list(self):
        """
        Get a list of distinct cloud names from the database.
        """
        comment = " /* DBProxy.get_cloud_list */"
        tmp_log = self.create_tagged_logger(comment)
        tmp_log.debug("start")
        try:
            with self.conn:
                sql = (
                    f"SELECT /* use_json_type */ DISTINCT sj.data.cloud AS cloud "
                    f"FROM {panda_config.schemaPANDA}.schedconfig_json sj "
                    f"UNION "
                    f"SELECT 'WORLD' AS cloud "
                    f"FROM dual "
                    f"ORDER BY cloud"
                )
                self.cur.arraysize = 100
                self.cur.execute(sql + comment)
                results = self.cur.fetchall()
                clouds = [result[0] for result in results]

            tmp_log.debug("done")
            return clouds
        except Exception:
            self.dump_error_message(tmp_log)
            self._rollback()
            return []

    # get users and groups to boost job priorities
    def get_dict_to_boost_job_prio(self, vo):
        comment = " /* DBProxy.get_dict_to_boost_job_prio */"
        tmp_log = self.create_tagged_logger(comment)

        if self.job_prio_boost_dict_update_time and datetime.datetime.now(datetime.timezone.utc).replace(
            tzinfo=None
        ) - self.job_prio_boost_dict_update_time < datetime.timedelta(minutes=15):
            return self.job_prio_boost_dict
        try:
            self.job_prio_boost_dict_update_time = naive_utcnow()
            self.job_prio_boost_dict = {}
            # get configs
            tmp_log = self.create_tagged_logger(comment)
            # get dicts
            res_dicts = self.getConfigValue("dbproxy", "USER_JOB_PRIO_BOOST_DICTS", "pandaserver")
            # parse list
            if res_dicts:
                for tmp_item in res_dicts:
                    try:
                        tmp_name = tmp_item["name"]
                        tmp_type = tmp_item["type"]
                        tmp_prio = tmp_item["prio"]
                        tmp_expire = tmp_item.get("expire", None)
                        # check expiration
                        if tmp_expire:
                            tmp_expire = datetime.datetime.strptime(tmp_expire, "%Y%m%d")
                            if tmp_expire < naive_utcnow():
                                continue
                        self.job_prio_boost_dict.setdefault(tmp_type, {})
                        self.job_prio_boost_dict[tmp_type][tmp_name] = int(tmp_prio)
                    except Exception as e:
                        tmp_log.error(str(e))
            tmp_log.debug(f"got {self.job_prio_boost_dict}")
            return self.job_prio_boost_dict
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return {}

    # set user secret
    def set_user_secret(self, owner, key, value):
        comment = " /* DBProxy.set_user_secret */"
        tmp_log = self.create_tagged_logger(comment, f"owner={owner} key={key}")
        try:
            # sql to check data
            sqlC = "SELECT data FROM ATLAS_PANDA.Secrets WHERE owner=:owner "
            # sql to insert dummy
            sqlI = "INSERT INTO ATLAS_PANDA.Secrets (owner, updated_at) " "VALUES(:owner,CURRENT_TIMESTAMP) "
            # sql to update data
            sqlU = "UPDATE ATLAS_PANDA.Secrets SET updated_at=CURRENT_TIMESTAMP,data=:data " "WHERE owner=:owner "
            # start transaction
            self.conn.begin()
            # check
            varMap = {}
            varMap[":owner"] = owner
            tmpS, tmpR = self.getClobObj(sqlC, varMap, use_commit=False)
            if not tmpR:
                # insert dummy for new entry
                self.cur.execute(sqlI + comment, varMap)
                data = {}
            else:
                data = json.loads(tmpR[0][0])
            # update
            if key is None:
                # delete all
                data = {}
            elif value is None:
                # delete key
                if key in data:
                    del data[key]
                else:
                    file_key = f"___file___:{key}"
                    if file_key in data:
                        del data[file_key]
            else:
                data[key] = value
            varMap = {}
            varMap[":owner"] = owner
            varMap[":data"] = json.dumps(data)
            self.cur.execute(sqlU + comment, varMap)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmp_log.debug("done")
            return True, "OK"
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return False, "database error"

    # get user secrets
    def get_user_secrets(self, owner, keys=None, get_json=False, use_commit=True):
        comment = " /* DBProxy.get_user_secrets */"
        tmp_log = self.create_tagged_logger(comment, f"owner={owner} keys={keys}")
        try:
            # sql to get data
            sqlC = "SELECT data FROM ATLAS_PANDA.Secrets WHERE owner=:owner "
            # check
            varMap = {}
            varMap[":owner"] = owner
            tmpS, tmpR = self.getClobObj(sqlC, varMap, use_commit=use_commit)
            if not tmpR:
                data = {}
                if not get_json:
                    data = json.dumps({})
            else:
                data = tmpR[0][0]
                # return only interesting keys
                if keys:
                    keys = set(keys.split(","))
                    data = json.loads(data)
                    for k in list(data):
                        if k not in keys:
                            data.pop(k)
                    if not get_json:
                        data = json.dumps(data)
                else:
                    if get_json:
                        data = json.loads(data)
            tmp_log.debug(f"got data with length={len(data)}")
            return True, data
        except Exception:
            # error
            self.dump_error_message(tmp_log)
            return False, "database error"

    def configurator_write_sites(self, site_list):
        """
        Cache the CRIC site information in the PanDA database
        """
        comment = " /* DBProxy.configurator_write_sites */"
        tmp_log = self.create_tagged_logger(comment)
        tmp_log.debug("start")

        try:
            # begin transaction
            self.conn.begin()

            # get existing sites
            tmp_log.debug("getting existing sites")
            sql_get = "SELECT site_name FROM ATLAS_PANDA.site"
            self.cur.execute(sql_get + comment)
            results = self.cur.fetchall()
            site_name_list = list(map(lambda result: result[0], results))
            tmp_log.debug("finished getting existing sites")

            # see which sites need an update and which need to be inserted new
            var_map_insert = []
            var_map_update = []
            for site in site_list:
                if site["site_name"] in site_name_list:
                    var_map_update.append(convert_dict_to_bind_vars(site))
                else:
                    var_map_insert.append(convert_dict_to_bind_vars(site))

            tmp_log.debug("Updating sites")
            sql_update = "UPDATE ATLAS_PANDA.site set role=:role, tier_level=:tier_level WHERE site_name=:site_name"
            for shard in create_shards(var_map_update, 100):
                self.cur.executemany(sql_update + comment, shard)

            tmp_log.debug("Inserting sites")
            sql_insert = "INSERT INTO ATLAS_PANDA.site (site_name, role, tier_level) " "VALUES(:site_name, :role, :tier_level)"
            for shard in create_shards(var_map_insert, 100):
                self.cur.executemany(sql_insert + comment, shard)

            # commit
            if not self._commit():
                raise RuntimeError("Commit error")

            tmp_log.debug("Done")
            return 0, None

        except Exception:
            self._rollback()
            self.dump_error_message(tmp_log)
            return -1, None

    def configurator_write_panda_sites(self, panda_site_list):
        comment = " /* DBProxy.configurator_write_panda_sites */"
        tmp_log = self.create_tagged_logger(comment)
        tmp_log.debug("start")

        try:
            # begin transaction
            self.conn.begin()

            # get existing panda sites
            tmp_log.debug("getting existing panda sites")
            sql_get = "SELECT panda_site_name FROM ATLAS_PANDA.panda_site"
            self.cur.execute(sql_get + comment)
            results = self.cur.fetchall()
            panda_site_name_list = list(map(lambda result: result[0], results))
            tmp_log.debug("finished getting existing panda sites")

            # see which sites need an update and which need to be inserted new
            var_map_insert = []
            var_map_update = []
            for panda_site in panda_site_list:
                if panda_site["panda_site_name"] in panda_site_name_list:
                    var_map_update.append(convert_dict_to_bind_vars(panda_site))
                else:
                    var_map_insert.append(convert_dict_to_bind_vars(panda_site))

            tmp_log.debug("Updating panda sites")
            sql_update = "UPDATE ATLAS_PANDA.panda_site set site_name=:site_name WHERE panda_site_name=:panda_site_name "
            for shard in create_shards(var_map_update, 100):
                self.cur.executemany(sql_update + comment, shard)

            tmp_log.debug("Inserting panda sites")
            sql_insert = "INSERT INTO ATLAS_PANDA.panda_site (panda_site_name, site_name) " "VALUES(:panda_site_name, :site_name)"
            for shard in create_shards(var_map_insert, 100):
                self.cur.executemany(sql_insert + comment, shard)

            # commit
            if not self._commit():
                raise RuntimeError("Commit error")

            tmp_log.debug("Done")
            return 0, None

        except Exception:
            self._rollback()
            self.dump_error_message(tmp_log)
            return -1, None

    def configurator_write_ddm_endpoints(self, ddm_endpoint_list):
        comment = " /* DBProxy.configurator_write_ddm_endpoints */"
        tmp_log = self.create_tagged_logger(comment)
        tmp_log.debug("start")

        try:
            # begin transaction
            self.conn.begin()

            # get existing ddm endpoints
            tmp_log.debug("getting existing ddm endpoints")
            sql_get = "SELECT ddm_endpoint_name FROM ATLAS_PANDA.ddm_endpoint"
            self.cur.execute(sql_get + comment)
            results = self.cur.fetchall()
            ddm_endpoint_name_list = list(map(lambda result: result[0], results))
            tmp_log.debug("finished getting existing ddm endpoints")

            # see which sites need an update and which need to be inserted new
            var_map_insert = []
            var_map_update = []
            for ddm_endpoint in ddm_endpoint_list:
                if ddm_endpoint["ddm_endpoint_name"] in ddm_endpoint_name_list:
                    var_map_update.append(convert_dict_to_bind_vars(ddm_endpoint))
                else:
                    var_map_insert.append(convert_dict_to_bind_vars(ddm_endpoint))

            tmp_log.debug("Updating ddm endpoints")
            sql_update = (
                "UPDATE ATLAS_PANDA.ddm_endpoint set "
                "site_name=:site_name, ddm_spacetoken_name=:ddm_spacetoken_name, type=:type, is_tape=:is_tape, "
                "blacklisted=:blacklisted, blacklisted_write=:blacklisted_write, blacklisted_read=:blacklisted_read, detailed_status=:detailed_status, "
                "space_used=:space_used, space_free=:space_free, space_total=:space_total, space_expired=:space_expired, space_timestamp=:space_timestamp "
                "WHERE ddm_endpoint_name=:ddm_endpoint_name"
            )
            for shard in create_shards(var_map_update, 100):
                self.cur.executemany(sql_update + comment, shard)

            tmp_log.debug("Inserting ddm endpoints")
            sql_insert = (
                "INSERT INTO ATLAS_PANDA.ddm_endpoint (ddm_endpoint_name, site_name, ddm_spacetoken_name, type, is_tape, "
                "blacklisted, blacklisted_write, blacklisted_read,  detailed_status, "
                "space_used, space_free, space_total, space_expired, space_timestamp) "
                "VALUES(:ddm_endpoint_name, :site_name, :ddm_spacetoken_name, :type, :is_tape, "
                ":blacklisted, :blacklisted_write, :blacklisted_read, :detailed_status, "
                ":space_used, :space_free, :space_total, :space_expired, :space_timestamp)"
            )
            for shard in create_shards(var_map_insert, 100):
                self.cur.executemany(sql_insert + comment, shard)

            # commit
            if not self._commit():
                raise RuntimeError("Commit error")

            tmp_log.debug("Done")
            return 0, None

        except Exception:
            self._rollback()
            self.dump_error_message(tmp_log)
            return -1, None

    def configurator_write_panda_ddm_relations(self, relation_list):
        comment = " /* DBProxy.configurator_write_panda_ddm_relations */"
        tmp_log = self.create_tagged_logger(comment)
        tmp_log.debug("start")

        try:
            # begin transaction
            self.conn.begin()

            # Reset the relations. Important to do this inside the transaction
            tmp_log.debug("Deleting existing panda ddm relations")
            sql_delete = "DELETE FROM ATLAS_PANDA.panda_ddm_relation"
            self.cur.execute(sql_delete + comment)

            var_map_insert = []
            for relation in relation_list:
                var_map_insert.append(convert_dict_to_bind_vars(relation))

            tmp_log.debug("Inserting panda ddm relations")
            sql_insert = (
                "INSERT INTO ATLAS_PANDA.panda_ddm_relation (panda_site_name, ddm_endpoint_name, roles, "
                "is_local, order_read, order_write, default_read, default_write, scope) "
                "VALUES(:panda_site_name, :ddm_endpoint_name, :roles, "
                ":is_local, :order_read, :order_write, :default_read, :default_write, :scope)"
            )
            for shard in create_shards(var_map_insert, 100):
                self.cur.executemany(sql_insert + comment, shard)

            # commit
            if not self._commit():
                raise RuntimeError("Commit error")

            tmp_log.debug("Done")
            return 0, None

        except Exception:
            self._rollback()
            self.dump_error_message(tmp_log)
            return -1, None

    def configurator_read_sites(self):
        comment = " /* DBProxy.configurator_read_sites */"
        tmp_log = self.create_tagged_logger(comment)
        tmp_log.debug("start")

        try:
            tmp_log.debug("getting existing panda sites")
            sql_get = "SELECT site_name FROM ATLAS_PANDA.site"
            self.cur.execute(sql_get + comment)
            results = self.cur.fetchall()
            site_names = set(map(lambda result: result[0], results))
            tmp_log.debug("finished getting site names in configurator")

            tmp_log.debug("Done")
            return site_names

        except Exception:
            self.dump_error_message(tmp_log)
            return set()

    def configurator_read_panda_sites(self):
        comment = " /* DBProxy.configurator_read_sites */"
        tmp_log = self.create_tagged_logger(comment)
        tmp_log.debug("start")

        try:
            tmp_log.debug("getting existing panda sites")
            sql_get = "SELECT panda_site_name FROM ATLAS_PANDA.panda_site"
            self.cur.execute(sql_get + comment)
            results = self.cur.fetchall()
            panda_site_names = set(map(lambda result: result[0], results))
            tmp_log.debug("finished getting panda site names in configurator")

            tmp_log.debug("Done")
            return panda_site_names

        except Exception:
            self.dump_error_message(tmp_log)
            return set()

    def configurator_read_ddm_endpoints(self):
        comment = " /* DBProxy.configurator_read_ddm_endpoints */"
        tmp_log = self.create_tagged_logger(comment)
        tmp_log.debug("start")

        try:
            tmp_log.debug("getting existing ddm endpoints")
            sql_get = "SELECT ddm_endpoint_name FROM ATLAS_PANDA.ddm_endpoint"
            self.cur.execute(sql_get + comment)
            results = self.cur.fetchall()
            ddm_endpoint_names = set(map(lambda result: result[0], results))
            tmp_log.debug("finished getting ddm endpoint names in configurator")

            tmp_log.debug("Done")
            return ddm_endpoint_names

        except Exception:
            self.dump_error_message(tmp_log)
            return set()

    def configurator_read_cric_sites(self):
        comment = " /* DBProxy.configurator_read_cric_sites */"
        tmp_log = self.create_tagged_logger(comment)
        tmp_log.debug("start")

        try:
            tmp_log.debug("getting existing CRIC sites")
            sql_get = "SELECT /* use_json_type */ distinct scj.data.atlas_site FROM ATLAS_PANDA.schedconfig_json scj"
            self.cur.arraysize = 1000
            self.cur.execute(sql_get + comment)
            results = self.cur.fetchall()
            site_names = set(map(lambda result: result[0], results))
            tmp_log.debug("finished getting CRIC sites")

            tmp_log.debug("Done")
            return site_names

        except Exception:
            self.dump_error_message(tmp_log)
            return set()

    def configurator_read_cric_panda_sites(self):
        comment = " /* DBProxy.configurator_read_cric_panda_sites */"
        tmp_log = self.create_tagged_logger(comment)
        tmp_log.debug("start")

        try:
            tmp_log.debug("getting existing CRIC panda queues")
            sql_get = "SELECT panda_queue FROM ATLAS_PANDA.schedconfig_json"
            self.cur.execute(sql_get + comment)
            results = self.cur.fetchall()
            panda_site_names = set(map(lambda result: result[0], results))
            tmp_log.debug("finished getting CRIC panda queues")

            tmp_log.debug("Done")
            return panda_site_names

        except Exception:
            self._rollback()
            self.dump_error_message(tmp_log)
            return set()

    def configurator_delete_sites(self, sites_to_delete):
        """
        Delete sites and all dependent entries (panda_sites, ddm_endpoints, panda_ddm_relations).
        Deletion of dependent entries is done through cascade definition in models
        """
        comment = " /* DBProxy.configurator_delete_sites */"
        tmp_log = self.create_tagged_logger(comment)
        tmp_log.debug("start")

        if not sites_to_delete:
            tmp_log.debug("nothing to delete")
            return

        var_map_list = list(map(lambda site_name: {":site_name": site_name}, sites_to_delete))

        try:
            # begin transaction
            self.conn.begin()
            tmp_log.debug(f"deleting sites: {sites_to_delete}")
            sql_update = "DELETE FROM ATLAS_PANDA.site WHERE site_name=:site_name"
            self.cur.executemany(sql_update + comment, var_map_list)
            tmp_log.debug("done deleting sites")

            # commit
            if not self._commit():
                raise RuntimeError("Commit error")

            tmp_log.debug("Done")
            return 0, None

        except Exception:
            self._rollback()
            self.dump_error_message(tmp_log)
            return -1, None

    def configurator_delete_panda_sites(self, panda_sites_to_delete):
        """
        Delete PanDA sites and dependent entries in panda_ddm_relations
        """
        comment = " /* DBProxy.configurator_delete_panda_sites */"
        tmp_log = self.create_tagged_logger(comment)
        tmp_log.debug("start")

        if not panda_sites_to_delete:
            tmp_log.debug("nothing to delete")
            return

        var_map_list = list(
            map(
                lambda panda_site_name: {":panda_site_name": panda_site_name},
                panda_sites_to_delete,
            )
        )

        try:
            # begin transaction
            self.conn.begin()
            tmp_log.debug(f"deleting panda sites: {panda_sites_to_delete}")
            sql_update = "DELETE FROM ATLAS_PANDA.panda_site WHERE panda_site_name=:panda_site_name"
            self.cur.executemany(sql_update + comment, var_map_list)
            tmp_log.debug("done deleting panda sites")

            # commit
            if not self._commit():
                raise RuntimeError("Commit error")

            tmp_log.debug("Done")
            return 0, None

        except Exception:
            self._rollback()
            self.dump_error_message(tmp_log)
            return -1, None

    def configurator_delete_ddm_endpoints(self, ddm_endpoints_to_delete):
        """
        Delete DDM endpoints dependent entries in panda_ddm_relations
        """
        comment = " /* DBProxy.configurator_delete_ddm_endpoints */"
        tmp_log = self.create_tagged_logger(comment)
        tmp_log.debug("start")

        if not ddm_endpoints_to_delete:
            tmp_log.debug("nothing to delete")
            return

        var_map_list = list(
            map(
                lambda ddm_endpoint_name: {":ddm_endpoint_name": ddm_endpoint_name},
                ddm_endpoints_to_delete,
            )
        )

        try:
            # begin transaction
            self.conn.begin()
            tmp_log.debug(f"deleting ddm endpoints: {ddm_endpoints_to_delete}")
            sql_update = "DELETE FROM ATLAS_PANDA.ddm_endpoint WHERE ddm_endpoint_name=:ddm_endpoint_name"
            self.cur.executemany(sql_update + comment, var_map_list)
            tmp_log.debug("done deleting ddm endpoints")

            # commit
            if not self._commit():
                raise RuntimeError("Commit error")

            tmp_log.debug("Done")
            return 0, None

        except Exception:
            self._rollback()
            self.dump_error_message(tmp_log)
            return -1, None

    def carbon_write_region_emissions(self, emissions):
        comment = " /* DBProxy.carbon_write_regional_emissions */"
        tmp_log = self.create_tagged_logger(comment)
        tmp_log.debug("start")

        try:
            # begin transaction
            self.conn.begin()

            tmp_log.debug("Deleting old entries")
            sql_delete = "DELETE FROM ATLAS_PANDA.CARBON_REGION_EMISSIONS " "WHERE timestamp < sysdate - interval '10' day"
            self.cur.execute(sql_delete + comment)

            tmp_log.debug("Inserting emissions by region")

            sql_insert = (
                "INSERT /*+ ignore_row_on_dupkey_index (emissions(region, timestamp)) */ "
                "INTO ATLAS_PANDA.CARBON_REGION_EMISSIONS emissions (REGION, TIMESTAMP, VALUE) "
                "VALUES(:region, :timestamp, :value)"
            )
            for shard in create_shards(emissions, 100):
                self.cur.executemany(sql_insert + comment, shard)

            # commit
            if not self._commit():
                raise RuntimeError("Commit error")

            tmp_log.debug("Done")
            return 0, None

        except Exception:
            self._rollback()
            self.dump_error_message(tmp_log)
            return -1, None

    def carbon_aggregate_emissions(self):
        comment = " /* DBProxy.carbon_aggregate_emissions */"
        tmp_log = self.create_tagged_logger(comment)
        tmp_log.debug("start")

        try:
            # begin transaction
            self.conn.begin()

            # get the percentage each region is contributing to grid computing power
            sql_stat = (
                "WITH tmp_total(total_hs) AS "
                "(SELECT sum(hs) "
                "FROM ATLAS_PANDA.jobs_share_stats) "
                "SELECT scj.data.region, sum(jss.hs)/tmp_total.total_hs "
                "FROM ATLAS_PANDA.jobs_share_stats jss, ATLAS_PANDA.schedconfig_json scj, tmp_total "
                "WHERE jss.computingsite = scj.panda_queue "
                "AND scj.data.region IS NOT NULL AND scj.data.region != 'GRID'"
                "GROUP BY scj.data.region, tmp_total.total_hs "
            )

            region_dic = {}
            self.cur.arraysize = 1000
            stats_raw = self.cur.execute(sql_stat + comment)
            for entry in stats_raw:
                region, per_cent = entry
                region_dic.setdefault(region, {"emissions": 0, "per_cent": 0})
                region_dic[region]["per_cent"] = per_cent

            # get the last emission values for each region
            sql_last = (
                "WITH top_ts(timestamp, region) AS "
                "(SELECT max(timestamp), region "
                "FROM atlas_panda.carbon_region_emissions "
                "GROUP BY region) "
                "SELECT cre.region, cre.value, cre.timestamp "
                "FROM atlas_panda.carbon_region_emissions cre, top_ts "
                "WHERE cre.timestamp = top_ts.timestamp AND cre.region = top_ts.region"
            )

            last_emission_values = self.cur.execute(sql_last + comment)
            for entry in last_emission_values:
                region, value, ts = entry
                region_dic.setdefault(region, {"emissions": 0, "per_cent": 0})
                region_dic[region]["emissions"] = value

            # calculate the grid average emissions
            average_emissions = 0
            for region in region_dic:
                tmp_log.debug(f"Region {region} with per_cent {region_dic[region]['per_cent']} and emissions {region_dic[region]['emissions']}")
                try:
                    average_emissions = average_emissions + region_dic[region]["per_cent"] * region_dic[region]["emissions"]
                except Exception:
                    tmp_log.debug(f"Skipped Region {region} with per_cent {region_dic[region]['per_cent']} and emissions {region_dic[region]['emissions']}")

            # store the average emissions
            tmp_log.debug(f"The grid co2 emissions were averaged to {average_emissions}")
            utc_now = naive_utcnow()
            var_map = {
                ":region": "GRID",
                ":timestamp": utc_now,
                "value": average_emissions,
            }

            sql_insert = "INSERT INTO ATLAS_PANDA.carbon_region_emissions (region, timestamp, value) " "VALUES (:region, :timestamp, :value)"
            self.cur.execute(sql_insert + comment, var_map)

            # commit
            if not self._commit():
                raise RuntimeError("Commit error")

            tmp_log.debug("Done")
            return 0, None

        except Exception:
            self._rollback()
            self.dump_error_message(tmp_log)
            return -1, None

    # check quota
    def checkQuota(self, dn):
        comment = " /* DBProxy.checkQuota */"
        tmp_log = self.create_tagged_logger(comment, f"dn={dn}")
        tmp_log.debug(f"start")
        try:
            # set autocommit on
            self.conn.begin()
            # select
            name = CoreUtils.clean_user_id(dn)
            sql = "SELECT cpua1, cpua7, cpua30, quotaa1, quotaa7, quotaa30 FROM ATLAS_PANDAMETA.users WHERE name=:name"
            varMap = {}
            varMap[":name"] = name
            self.cur.arraysize = 10
            self.cur.execute(sql + comment, varMap)
            res = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            weight = 0.0
            if res is not None and len(res) != 0:
                item = res[0]
                # cpu and quota
                cpu1 = item[0]
                cpu7 = item[1]
                cpu30 = item[2]
                if item[3] in [0, None]:
                    quota1 = 0
                else:
                    quota1 = item[3] * 3600
                if item[4] in [0, None]:
                    quota7 = 0
                else:
                    quota7 = item[4] * 3600
                if item[5] in [0, None]:
                    quota30 = 0
                else:
                    quota30 = item[5] * 3600
                # CPU usage
                if cpu1 is None:
                    cpu1 = 0.0
                # weight
                if quota1 > 0:
                    weight = float(cpu1) / float(quota1)
                # not exceeded the limit
                weight = 0.0
                tmp_log.debug(f"Weight:{weight} Quota:{quota1} CPU:{cpu1}")
            else:
                tmp_log.debug(f"cannot found")
            return weight
        except Exception:
            self.dump_error_message(tmp_log)
            # roll back
            self._rollback()
            return 0.0

    # check if superuser
    def isSuperUser(self, userName):
        comment = " /* DBProxy.isSuperUser */"
        tmp_log = self.create_tagged_logger(comment, f"userName={userName}")
        tmp_log.debug("start")
        try:
            isSU = False
            isSG = False
            # start transaction
            self.conn.begin()
            # check gridpref
            name = CoreUtils.clean_user_id(userName)
            sql = "SELECT gridpref FROM ATLAS_PANDAMETA.users WHERE name=:name"
            varMap = {}
            varMap[":name"] = name
            self.cur.arraysize = 10
            self.cur.execute(sql + comment, varMap)
            res = self.cur.fetchone()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # check if s in gridpref
            if res is not None:
                (gridpref,) = res
                if gridpref is not None:
                    if PrioUtil.PERMISSION_SUPER_USER in gridpref:
                        isSU = True
                    if PrioUtil.PERMISSION_SUPER_GROUP in gridpref:
                        isSG = True
            tmp_log.debug(f"done with superUser={isSU} superGroup={isSG}")
            return isSU, isSG
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return False, False

    # get serialize JobID and status
    def getUserParameter(self, dn, jobID, jobsetID):
        comment = " /* DBProxy.getUserParameter */"
        tmp_log = self.create_tagged_logger(comment, f"dn={dn} jobID={jobID} jobsetID={jobsetID}")
        try:
            # set initial values
            retStatus = True
            if jobsetID == -1:
                # generate new jobsetID
                retJobsetID = jobID
                # new jobID = 1 + new jobsetID
                retJobID = retJobsetID + 1
            elif jobsetID in ["NULL", None, 0]:
                # no jobsetID
                retJobsetID = None
                retJobID = jobID
            else:
                # user specified jobsetID
                retJobsetID = jobsetID
                retJobID = jobID
            # set autocommit on
            self.conn.begin()
            # select
            name = CoreUtils.clean_user_id(dn)
            sql = "SELECT jobid,status FROM ATLAS_PANDAMETA.users WHERE name=:name "
            sql += "FOR UPDATE "
            sqlAdd = "INSERT INTO ATLAS_PANDAMETA.users "
            sqlAdd += "(ID,NAME,LASTMOD,FIRSTJOB,LATESTJOB,CACHETIME,NCURRENT,JOBID) "
            sqlAdd += "VALUES(ATLAS_PANDAMETA.USERS_ID_SEQ.nextval,:name,"
            sqlAdd += "CURRENT_DATE,CURRENT_DATE,CURRENT_DATE,CURRENT_DATE,0,1) "
            varMap = {}
            varMap[":name"] = name
            self.cur.execute(sql + comment, varMap)
            self.cur.arraysize = 10
            res = self.cur.fetchall()
            # insert if no record
            if res is None or len(res) == 0:
                try:
                    self.cur.execute(sqlAdd + comment, varMap)
                    retI = self.cur.rowcount
                    tmp_log.debug(f"inserted new row with {retI}")
                    # emulate DB response
                    res = [[1, ""]]
                except Exception:
                    self.dump_error_message(tmp_log)
            if res is not None and len(res) != 0:
                item = res[0]
                # JobID in DB
                dbJobID = item[0]
                # check status
                if item[1] in ["disabled"]:
                    retStatus = False
                # use larger JobID
                if dbJobID >= int(retJobID) or (jobsetID == -1 and dbJobID >= int(retJobsetID)):
                    if jobsetID == -1:
                        # generate new jobsetID = 1 + exsiting jobID
                        retJobsetID = dbJobID + 1
                        # new jobID = 1 + new jobsetID
                        retJobID = retJobsetID + 1
                    else:
                        # new jobID = 1 + existing jobID
                        retJobID = dbJobID + 1
                # update DB
                varMap = {}
                varMap[":name"] = name
                varMap[":jobid"] = retJobID
                sql = "UPDATE ATLAS_PANDAMETA.users SET jobid=:jobid WHERE name=:name"
                self.cur.execute(sql + comment, varMap)
                tmp_log.debug(f"set JobID={retJobID}")
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmp_log.debug(f"return JobID={retJobID} JobsetID={retJobsetID} Status={retStatus}")
            return retJobID, retJobsetID, retStatus
        except Exception:
            self.dump_error_message(tmp_log)
            # roll back
            self._rollback()
            return retJobID, retJobsetID, retStatus

    # check ban user
    def checkBanUser(self, dn, sourceLabel, jediCheck=False):
        comment = " /* DBProxy.checkBanUser */"
        try:
            methodName = "checkBanUser"
            # set initial values
            retStatus = True
            name = CoreUtils.clean_user_id(dn)
            tmp_log = self.create_tagged_logger(comment, f"name={name}")
            tmp_log.debug(f"start dn={dn} label={sourceLabel} jediCheck={jediCheck}")
            # set autocommit on
            self.conn.begin()
            # select
            sql = "SELECT status,dn FROM ATLAS_PANDAMETA.users WHERE name=:name"
            varMap = {}
            varMap[":name"] = name
            self.cur.execute(sql + comment, varMap)
            self.cur.arraysize = 10
            res = self.cur.fetchone()
            if res is not None:
                # check status
                tmpStatus, dnInDB = res
                if tmpStatus in ["disabled"]:
                    retStatus = False
                elif jediCheck and (dnInDB in ["", None] or dnInDB != dn):
                    # add DN
                    sqlUp = "UPDATE ATLAS_PANDAMETA.users SET dn=:dn WHERE name=:name "
                    varMap = {}
                    varMap[":name"] = name
                    varMap[":dn"] = dn
                    self.cur.execute(sqlUp + comment, varMap)
                    retI = self.cur.rowcount
                    tmp_log.debug(f"update DN with Status={retI}")
                    if retI != 1:
                        retStatus = 1
            else:
                # new user
                if jediCheck:
                    name = CoreUtils.clean_user_id(dn)
                    sqlAdd = "INSERT INTO ATLAS_PANDAMETA.users "
                    sqlAdd += "(ID,NAME,DN,LASTMOD,FIRSTJOB,LATESTJOB,CACHETIME,NCURRENT,JOBID) "
                    sqlAdd += "VALUES(ATLAS_PANDAMETA.USERS_ID_SEQ.nextval,:name,:dn,"
                    sqlAdd += "CURRENT_DATE,CURRENT_DATE,CURRENT_DATE,CURRENT_DATE,0,1) "
                    varMap = {}
                    varMap[":name"] = name
                    varMap[":dn"] = dn
                    self.cur.execute(sqlAdd + comment, varMap)
                    retI = self.cur.rowcount
                    tmp_log.debug(f"inserted new row with Status={retI}")
                    if retI != 1:
                        retStatus = 2
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmp_log.debug(f"done with Status={retStatus}")
            return retStatus
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return retStatus

    # get email address for a user
    def getEmailAddr(self, name, withDN=False, withUpTime=False):
        comment = " /* DBProxy.getEmailAddr */"
        tmp_log = self.create_tagged_logger(comment)
        tmp_log.debug(f"get email for {name}")
        # sql
        if withDN:
            failedRet = "", "", None
            sql = "SELECT email,dn,location FROM ATLAS_PANDAMETA.users WHERE name=:name"
        elif withUpTime:
            failedRet = "", None
            sql = "SELECT email,location FROM ATLAS_PANDAMETA.users WHERE name=:name"
        else:
            failedRet = ""
            sql = "SELECT email FROM ATLAS_PANDAMETA.users WHERE name=:name"
        try:
            # set autocommit on
            self.conn.begin()
            # select
            varMap = {}
            varMap[":name"] = name
            self.cur.execute(sql + comment, varMap)
            self.cur.arraysize = 10
            res = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            if res is not None and len(res) != 0:
                if withDN or withUpTime:
                    if withDN:
                        email, dn, upTime = res[0]
                    else:
                        email, upTime = res[0]
                    # convert time
                    try:
                        upTime = datetime.datetime.strptime(upTime, "%Y-%m-%d %H:%M:%S")
                    except Exception:
                        upTime = None
                    if withDN:
                        return email, dn, upTime
                    else:
                        return email, upTime
                else:
                    return res[0][0]
            # return empty string
            return failedRet
        except Exception:
            self.dump_error_message(tmp_log)
            # roll back
            self._rollback()
            return failedRet

    # set email address for a user
    def setEmailAddr(self, userName, emailAddr):
        comment = " /* DBProxy.setEmailAddr */"
        tmp_log = self.create_tagged_logger(comment)
        tmp_log.debug(f"{userName} to {emailAddr}")
        # sql
        sql = "UPDATE ATLAS_PANDAMETA.users SET email=:email,location=:uptime WHERE name=:name "
        try:
            # set autocommit on
            self.conn.begin()
            # set
            varMap = {}
            varMap[":name"] = userName
            varMap[":email"] = emailAddr
            varMap[":uptime"] = naive_utcnow().strftime("%Y-%m-%d %H:%M:%S")
            self.cur.execute(sql + comment, varMap)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            return True
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return False

    # get ban users
    def get_ban_users(self):
        comment = " /* DBProxy.get_ban_user */"
        tmp_log = self.create_tagged_logger(comment)
        tmp_log.debug("start")
        # sql
        sql = "SELECT name FROM ATLAS_PANDAMETA.users WHERE status=:status "
        try:
            # set autocommit on
            self.conn.begin()
            varMap = {}
            varMap[":status"] = "disabled"
            self.cur.execute(sql + comment, varMap)
            self.cur.arraysize = 10
            res = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            retVal = {name: False for name, in res}
            tmp_log.debug(f"got {retVal}")
            return True, retVal
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return False, None

    # register token key
    def register_token_key(self, client_name: str, lifetime: int) -> bool:
        """
        Register token key for a client with a lifetime and delete expired tokens

        :param client_name: client name who owns the token key
        :param lifetime: lifetime of the token key in hours

        :return: True if succeeded. False otherwise
        """
        comment = " /* DBProxy.register_token_key */"
        tmp_log = self.create_tagged_logger(comment, f"client_name={client_name}")
        tmp_log.debug("start")
        try:
            # set autocommit on
            self.conn.begin()
            time_now = naive_utcnow()
            expire_at = time_now + datetime.timedelta(hours=lifetime)
            # check if a new key was registered recently
            sql = f"SELECT 1 FROM {panda_config.schemaMETA}.proxykey WHERE dn=:dn AND expires>:limit "
            var_map = {":dn": client_name, ":limit": expire_at - datetime.timedelta(hours=1)}
            self.cur.execute(sql + comment, var_map)
            res = self.cur.fetchone()
            if res:
                tmp_log.debug("skip as a new key was registered recently")
            else:
                # get max ID
                max_id = None
                sql = "SELECT MAX(ID) FROM ATLAS_PANDAMETA.proxykey "
                self.cur.execute(sql + comment, {})
                res = self.cur.fetchone()
                if res:
                    (max_id,) = res
                if max_id is None:
                    max_id = 0
                max_id += 1
                max_id %= 10000000
                # register a key
                sql = (
                    f"INSERT INTO {panda_config.schemaMETA}.proxykey (ID,DN,CREDNAME,CREATED,EXPIRES,ORIGIN,MYPROXY) "
                    "VALUES(:id,:dn,:credname,:created,:expires,:origin,:myproxy) "
                )
                var_map = {
                    ":id": max_id,
                    ":dn": client_name,
                    ":credname": str(uuid.uuid4()),
                    ":created": time_now,
                    ":expires": expire_at,
                    ":origin": "panda",
                    ":myproxy": "NA",
                }
                try:
                    self.cur.execute(sql + comment, var_map)
                    tmp_log.debug(f"registered a new key with id={max_id}")
                except Exception as e:
                    # ignore ID duplication error
                    tmp_log.debug(f"ignoring registration failure with {str(e)}")
            # delete obsolete keys
            sql = "DELETE FROM ATLAS_PANDAMETA.proxykey WHERE expires<:limit "
            var_map = {":limit": time_now}
            self.cur.execute(sql + comment, var_map)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return True
            tmp_log.debug("done")
            return True
        except Exception:
            # roll back
            self._rollback()
            # dump error
            self.dump_error_message(tmp_log)
            return False

    # Configurator function: inserts data into the network matrix
    def insertNetworkMatrixData(self, data):
        comment = " /* DBProxy.insertNetworkMatrixData */"
        tmp_log = self.create_tagged_logger(comment)
        tmp_log.debug("start")

        # For performance reasons we will insert the data into a temporary table
        # and then merge this data into the permanent table.

        sql_insert = """
        INSERT INTO ATLAS_PANDA.network_matrix_kv_temp (src, dst, key, value, ts)
        VALUES (:src, :dst, :key, :value, :ts)
        """

        if self.backend == "postgres":
            sql_merge = (
                "INSERT INTO ATLAS_PANDA.network_matrix_kv "
                "(src, dst, key, value, ts) "
                "SELECT  src, dst, key, value, ts FROM ATLAS_PANDA.NETWORK_MATRIX_KV_TEMP "
                "ON CONFLICT (src, dst, key) "
                "DO UPDATE SET value=EXCLUDED.value, ts=EXCLUDED.ts "
            )
        else:
            sql_merge = """
            MERGE /*+ FULL(nm_kv) */ INTO ATLAS_PANDA.network_matrix_kv nm_kv USING
                (SELECT  src, dst, key, value, ts FROM ATLAS_PANDA.NETWORK_MATRIX_KV_TEMP) input
                ON (nm_kv.src = input.src AND nm_kv.dst= input.dst AND nm_kv.key = input.key)
            WHEN NOT MATCHED THEN
                INSERT (src, dst, key, value, ts)
                VALUES (input.src, input.dst, input.key, input.value, input.ts)
            WHEN MATCHED THEN
                UPDATE SET nm_kv.value = input.value, nm_kv.ts = input.ts
            """
        try:
            self.conn.begin()
            for shard in create_shards(data, 100):
                time1 = time.time()
                var_maps = []
                for entry in shard:
                    var_map = {
                        ":src": entry[0],
                        ":dst": entry[1],
                        ":key": entry[2],
                        ":value": entry[3],
                        ":ts": entry[4],
                    }
                    var_maps.append(var_map)

                time2 = time.time()
                self.cur.executemany(sql_insert + comment, var_maps)
                time3 = time.time()
                tmp_log.debug(f"Processing a shard took: {time2 - time1}s of data preparation and {time3 - time2}s of insertion = {time3 - time1}")

            time4 = time.time()
            self.cur.execute(sql_merge + comment)
            time5 = time.time()
            tmp_log.debug(f"Final merge took: {time5 - time4}s")
            if self.backend == "postgres":
                # cleanup since ON CONFLICT DO UPDATE doesn't work with duplicated entries
                self.cur.execute("DELETE FROM ATLAS_PANDA.NETWORK_MATRIX_KV_TEMP " + comment)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")

        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return None, ""

    # Configurator function: delete old network data
    def deleteOldNetworkData(self):
        comment = " /* DBProxy.deleteOldNetworkData */"
        tmp_log = self.create_tagged_logger(comment)
        tmp_log.debug("start")

        # delete any data older than a week
        sql_delete = """
        DELETE FROM ATLAS_PANDA.network_matrix_kv
        WHERE ts < (current_date - 7)
        """
        try:
            self.conn.begin()
            time1 = time.time()
            self.cur.execute(sql_delete + comment)
            time2 = time.time()
            tmp_log.debug(f"Deletion of old network data took: {time2 - time1}s")

            # commit
            if not self._commit():
                raise RuntimeError("Commit error")

        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return None, ""

    def ups_get_queues(self):
        """
        Identify unified pilot streaming (ups) queues: served in pull (late binding) model
        :return: list of panda queues
        """
        comment = " /* DBProxy.ups_get_queues */"
        tmp_log = self.create_tagged_logger(comment)
        tmp_log.debug("start")

        ups_queues = []
        sql = f"""
              SELECT /* use_json_type */ scj.panda_queue FROM {panda_config.schemaPANDA}.schedconfig_json scj
              WHERE scj.data.capability='ucore' AND scj.data.workflow = 'pull_ups'
              """

        self.cur.execute(sql + comment)
        res = self.cur.fetchall()
        for (ups_queue,) in res:
            ups_queues.append(ups_queue)

        tmp_log.debug("done")
        return ups_queues

    # calculate RW for tasks
    def calculateTaskRW_JEDI(self, jediTaskID):
        comment = " /* JediDBProxy.calculateTaskRW_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID}")
        tmpLog.debug("start")
        try:
            # sql to get RW
            sql = "SELECT ROUND(SUM((nFiles-nFilesFinished-nFilesFailed-nFilesOnHold)*walltime)/24/3600) "
            sql += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_Datasets tabD ".format(panda_config.schemaJEDI)
            sql += "WHERE tabT.jediTaskID=tabD.jediTaskID AND masterID IS NULL "
            sql += "AND tabT.jediTaskID=:jediTaskID "
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            # begin transaction
            self.conn.begin()
            # get
            self.cur.execute(sql + comment, varMap)
            resRT = self.cur.fetchone()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # locked by another
            if resRT is None:
                retVal = None
            else:
                retVal = resRT[0]
            tmpLog.debug(f"RW={retVal}")
            # return
            tmpLog.debug("done")
            return retVal
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return None

    # calculate RW with a priority
    def calculateRWwithPrio_JEDI(self, vo, prodSourceLabel, workQueue, priority):
        comment = " /* JediDBProxy.calculateRWwithPrio_JEDI */"
        if workQueue is None:
            tmpLog = self.create_tagged_logger(comment, f"vo={vo} label={prodSourceLabel} queue={None} prio={priority}")
        else:
            tmpLog = self.create_tagged_logger(comment, f"vo={vo} label={prodSourceLabel} queue={workQueue.queue_name} prio={priority}")
        tmpLog.debug("start")
        try:
            # sql to get RW
            varMap = {}
            varMap[":vo"] = vo
            varMap[":prodSourceLabel"] = prodSourceLabel
            if priority is not None:
                varMap[":priority"] = priority
            sql = "SELECT tabT.jediTaskID,tabT.cloud,tabD.datasetID,nFiles-nFilesFinished-nFilesFailed,walltime "
            sql += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_Datasets tabD,{0}.JEDI_AUX_Status_MinTaskID tabA ".format(panda_config.schemaJEDI)
            sql += "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID "
            sql += "AND tabT.jediTaskID=tabD.jediTaskID AND masterID IS NULL "
            sql += "AND (nFiles-nFilesFinished-nFilesFailed)>0 "
            sql += "AND tabT.vo=:vo AND prodSourceLabel=:prodSourceLabel "

            if priority is not None:
                sql += "AND currentPriority>=:priority "

            if workQueue is not None:
                if workQueue.is_global_share:
                    sql += "AND gshare=:wq_name "
                    sql += f"AND tabT.workqueue_id NOT IN (SELECT queue_id FROM {panda_config.schemaJEDI}.jedi_work_queue WHERE queue_function = 'Resource') "
                    varMap[":wq_name"] = workQueue.queue_name
                else:
                    sql += "AND workQueue_ID=:wq_id "
                    varMap[":wq_id"] = workQueue.queue_id

            sql += "AND tabT.status IN (:status1,:status2,:status3,:status4) "
            sql += f"AND tabD.type IN ({INPUT_TYPES_var_str}) "
            varMap.update(INPUT_TYPES_var_map)
            varMap[":status1"] = "ready"
            varMap[":status2"] = "scouting"
            varMap[":status3"] = "running"
            varMap[":status4"] = "pending"
            sql += "AND tabT.cloud IS NOT NULL "
            # begin transaction
            self.conn.begin()
            # set cloud
            self.cur.execute(sql + comment, varMap)
            resList = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # loop over all tasks
            retMap = {}
            sqlF = "SELECT fsize,startEvent,endEvent,nEvents "
            sqlF += f"FROM {panda_config.schemaJEDI}.JEDI_Dataset_Contents "
            sqlF += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND rownum<=1"
            for jediTaskID, cloud, datasetID, nRem, walltime in resList:
                # get effective size
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                varMap[":datasetID"] = datasetID
                # begin transaction
                self.conn.begin()
                # get file
                self.cur.execute(sqlF + comment, varMap)
                resFile = self.cur.fetchone()
                # commit
                if not self._commit():
                    raise RuntimeError("Commit error")
                if resFile is not None:
                    # calculate RW using effective size
                    fsize, startEvent, endEvent, nEvents = resFile
                    effectiveFsize = CoreUtils.getEffectiveFileSize(fsize, startEvent, endEvent, nEvents)
                    tmpRW = nRem * effectiveFsize * walltime
                    if cloud not in retMap:
                        retMap[cloud] = 0
                    retMap[cloud] += tmpRW
            for cloudName, rwValue in retMap.items():
                retMap[cloudName] = int(rwValue / 24 / 3600)
            tmpLog.debug(f"RW={str(retMap)}")
            # return
            tmpLog.debug("done")
            return retMap
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return None

    # calculate WORLD RW with a priority
    def calculateWorldRWwithPrio_JEDI(self, vo, prodSourceLabel, workQueue, priority):
        comment = " /* JediDBProxy.calculateWorldRWwithPrio_JEDI */"
        if workQueue is None:
            tmpLog = self.create_tagged_logger(comment, f"vo={vo} label={prodSourceLabel} queue={None} prio={priority}")
        else:
            tmpLog = self.create_tagged_logger(comment, f"vo={vo} label={prodSourceLabel} queue={workQueue.queue_name} prio={priority}")
        tmpLog.debug("start")
        try:
            # sql to get RW
            varMap = {}
            varMap[":vo"] = vo
            varMap[":prodSourceLabel"] = prodSourceLabel
            varMap[":worldCloud"] = JediTaskSpec.worldCloudName
            if priority is not None:
                varMap[":priority"] = priority
            sql = "SELECT tabT.nucleus,SUM((nEvents-nEventsUsed)*(CASE WHEN cpuTime IS NULL THEN 300 ELSE cpuTime END)) "
            sql += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_Datasets tabD,{0}.JEDI_AUX_Status_MinTaskID tabA ".format(panda_config.schemaJEDI)
            sql += "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID "
            sql += "AND tabT.jediTaskID=tabD.jediTaskID AND masterID IS NULL "
            sql += "AND (nFiles-nFilesFinished-nFilesFailed)>0 "
            sql += "AND tabT.vo=:vo AND prodSourceLabel=:prodSourceLabel "
            sql += "AND tabT.cloud=:worldCloud "

            if priority is not None:
                sql += "AND currentPriority>=:priority "

            if workQueue is not None:
                if workQueue.is_global_share:
                    sql += "AND gshare=:wq_name "
                    sql += f"AND tabT.workqueue_id NOT IN (SELECT queue_id FROM {panda_config.schemaJEDI}.jedi_work_queue WHERE queue_function = 'Resource') "
                    varMap[":wq_name"] = workQueue.queue_name
                else:
                    sql += "AND workQueue_ID=:wq_id "
                    varMap[":wq_id"] = workQueue.queue_id

            sql += "AND tabT.status IN (:status1,:status2,:status3,:status4) "
            sql += f"AND tabD.type IN ({INPUT_TYPES_var_str}) "
            varMap.update(INPUT_TYPES_var_map)
            varMap[":status1"] = "ready"
            varMap[":status2"] = "scouting"
            varMap[":status3"] = "running"
            varMap[":status4"] = "pending"
            sql += "GROUP BY tabT.nucleus "
            # begin transaction
            self.conn.begin()
            # set cloud
            self.cur.execute(sql + comment, varMap)
            resList = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # loop over all nuclei
            retMap = {}
            for nucleus, worldRW in resList:
                retMap[nucleus] = worldRW
            tmpLog.debug(f"RW={str(retMap)}")
            # return
            tmpLog.debug("done")
            return retMap
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return None

    # calculate WORLD RW for tasks
    def calculateTaskWorldRW_JEDI(self, jediTaskID):
        comment = " /* JediDBProxy.calculateTaskWorldRW_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID}")
        tmpLog.debug("start")
        try:
            # sql to get RW
            sql = (
                "SELECT (nEvents-nEventsUsed)*(CASE "
                "WHEN cpuTime IS NULL THEN 300 "
                "WHEN cpuTimeUnit='mHS06sPerEvent' OR cpuTimeUnit='mHS06sPerEventFixed' THEN cpuTime/1000 "
                "ELSE cpuTime END) "
            )
            sql += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_Datasets tabD ".format(panda_config.schemaJEDI)
            sql += "WHERE tabT.jediTaskID=tabD.jediTaskID AND masterID IS NULL "
            sql += "AND tabT.jediTaskID=:jediTaskID "
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            # begin transaction
            self.conn.begin()
            # get
            self.cur.execute(sql + comment, varMap)
            resRT = self.cur.fetchone()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # locked by another
            if resRT is None:
                retVal = None
            else:
                retVal = resRT[0]
            tmpLog.debug(f"RW={retVal}")
            # return
            tmpLog.debug("done")
            return retVal
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return None

    def load_sw_map(self):
        comment = " /* JediDBProxy.load_sw_map */"
        tmp_log = self.create_tagged_logger(comment)
        tmp_log.debug("start")

        sw_map = {}

        try:
            # sql to get size
            sql = f"SELECT PANDA_QUEUE, DATA FROM {panda_config.schemaPANDA}.SW_TAGS"
            self.cur.execute(sql + comment)
            results = self.cur.fetchall()
            for panda_queue, data in results:
                sw_map[panda_queue] = json.loads(data)

            tmp_log.debug("done")
            return sw_map

        except Exception:
            self._rollback()
            self.dump_error_message(tmp_log)
            return None

    def getNetworkMetrics(self, dst, keyList):
        """
        Get the network metrics from a source to all possible destinations
        :param dst: destination site
        :param keyList: activity keys.
        :return: returns a dictionary with network values in the style
        {
            <dest>: {<key>: <value>, <key>: <value>},
            <dest>: {<key>: <value>, <key>: <value>},
            ...
        }
        """
        comment = " /* JediDBProxy.getNetworkMetrics */"
        tmpLog = self.create_tagged_logger(comment)
        tmpLog.debug("start")

        latest_validity = naive_utcnow() - datetime.timedelta(minutes=60)

        varMap = {"dst": dst, "latest_validity": latest_validity}

        key_var_names_str, key_var_map = get_sql_IN_bind_variables(keyList, prefix=":key")

        sql = f"""
        SELECT src, key, value, ts FROM {panda_config.schemaJEDI}.network_matrix_kv
        WHERE dst = :dst AND key IN ({key_var_names_str})
        AND ts > :latest_validity
        """

        varMap.update(key_var_map)

        self.cur.execute(sql + comment, varMap)
        resList = self.cur.fetchall()

        networkMap = {}
        total = {}
        for res in resList:
            src, key, value, ts = res
            networkMap.setdefault(src, {})
            networkMap[src][key] = value
            total.setdefault(key, 0)
            try:
                total[key] += value
            except Exception:
                pass
        networkMap["total"] = total
        tmpLog.debug(f"network map to nucleus {dst} is: {networkMap}")

        return networkMap

    def getBackloggedNuclei(self):
        """
        Return a list of nuclei, which has built up transfer backlog. We will consider a nucleus as backlogged,
         when it has over 2000 output transfers queued and there are more than 3 sites with queues over
        """

        comment = " /* JediDBProxy.getBackloggedNuclei */"
        tmpLog = self.create_tagged_logger(comment)
        tmpLog.debug("start")

        latest_validity = naive_utcnow() - datetime.timedelta(minutes=60)

        nqueued_cap = self.getConfigValue("taskbrokerage", "NQUEUED_NUC_CAP", "jedi")
        if nqueued_cap is None:
            nqueued_cap = 2000

        varMap = {":latest_validity": latest_validity, ":nqueued_cap": nqueued_cap}

        sql = f"""
              SELECT dst
              FROM {panda_config.schemaJEDI}.network_matrix_kv
              WHERE key = 'Production Output_queued'
              AND ts > :latest_validity
              GROUP BY dst
              HAVING SUM(value) > :nqueued_cap
        """

        self.cur.execute(sql + comment, varMap)
        try:
            backlogged_nuclei = [entry[0] for entry in self.cur.fetchall()]
        except IndexError:
            backlogged_nuclei = []

        tmpLog.debug(f"Nuclei with a long backlog are: {backlogged_nuclei}")

        return backlogged_nuclei

    def getPandaSiteToOutputStorageSiteMapping(self):
        """
        Get a  mapping of panda sites to their storage site. We consider the storage site of the default ddm endpoint
        :return: dictionary with panda_site_name keys and site_name values
        """
        comment = " /* JediDBProxy.getPandaSiteToOutputStorageSiteMapping */"
        tmpLog = self.create_tagged_logger(comment)
        tmpLog.debug("start")

        sql = """
        SELECT pdr.panda_site_name, de.site_name, nvl(pdr.scope, 'default')
        FROM atlas_panda.panda_ddm_relation pdr, atlas_panda.ddm_endpoint de
        WHERE pdr.default_write = 'Y'
        AND pdr.ddm_endpoint_name = de.ddm_endpoint_name
        """

        self.cur.execute(sql + comment)
        resList = self.cur.fetchall()
        mapping = {}

        for res in resList:
            pandaSiteName, siteName, scope = res
            mapping.setdefault(pandaSiteName, {})
            mapping[pandaSiteName][scope] = siteName

        # tmpLog.debug('panda site to ATLAS site mapping is: {0}'.format(mapping))

        tmpLog.debug("done")
        return mapping

    def get_active_gshare_rtypes(self, vo):
        """
        Gets the active gshare/resource wq combinations.  Active means they have at least 1 job in (assigned, activate, starting, running, ...)
        :param vo: Virtual Organization
        """
        comment = " /* DBProxy.get_active_gshare_rtypes */"
        tmp_log = self.create_tagged_logger(comment, f"vo={vo}")
        tmp_log.debug("start")

        # define the var map of query parameters
        var_map = {":vo": vo}

        # sql to query on pre-cached job statistics tables, creating a single result set with active gshares and resource workqueues
        sql_get_active_combinations = f"""
            WITH gshare_results AS ( 
            SELECT /*+ RESULT_CACHE */ gshare AS name, resource_type 
            FROM {panda_config.schemaPANDA}.JOBS_SHARE_STATS 
            WHERE vo=:vo 
            UNION 
            SELECT /*+ RESULT_CACHE */ gshare AS name, resource_type 
            FROM {panda_config.schemaPANDA}.JOBSDEFINED_SHARE_STATS 
            WHERE vo=:vo 
            ), wq_results AS ( 
            SELECT jwq.QUEUE_NAME AS name, jss.resource_type 
            FROM {panda_config.schemaPANDA}.JOBS_SHARE_STATS jss 
            JOIN {panda_config.schemaPANDA}.JEDI_WORK_QUEUE jwq ON jss.WORKQUEUE_ID = jwq.QUEUE_ID 
            WHERE jwq.QUEUE_FUNCTION = 'Resource' AND jss.vo=:vo AND jwq.vo=:vo 
            UNION 
            SELECT jwq.QUEUE_NAME AS name, jss.resource_type 
            FROM {panda_config.schemaPANDA}.JOBSDEFINED_SHARE_STATS jss 
            JOIN {panda_config.schemaPANDA}.JEDI_WORK_QUEUE jwq ON jss.WORKQUEUE_ID = jwq.QUEUE_ID 
            WHERE jwq.QUEUE_FUNCTION = 'Resource' AND jss.vo=:vo AND jwq.vo=:vo 
            ) 
            SELECT name, resource_type FROM gshare_results 
            UNION 
            SELECT name, resource_type FROM wq_results 
            GROUP BY name, resource_type
        """

        return_map = {}
        try:
            self.cur.arraysize = 1000
            self.cur.execute(f"{sql_get_active_combinations} {comment}", var_map)
            res = self.cur.fetchall()

            # create map
            for name, resource_type in res:
                return_map.setdefault(name, [])
                return_map[name].append(resource_type)

            tmp_log.debug("done")
            return return_map
        except Exception:
            self.dump_error_message(tmp_log)
            return {}

    # get work queue map
    def getWorkQueueMap(self):
        self.refreshWorkQueueMap()
        return self.workQueueMap

    # refresh work queue map
    def refreshWorkQueueMap(self):
        # avoid frequent lookup
        if self.updateTimeForWorkQueue is not None and (naive_utcnow() - self.updateTimeForWorkQueue) < datetime.timedelta(minutes=10):
            return
        comment = " /* JediDBProxy.refreshWorkQueueMap */"
        tmpLog = self.create_tagged_logger(comment)

        leave_shares = self.get_sorted_leaves()

        if self.workQueueMap is None:
            self.workQueueMap = WorkQueueMapper()
        # SQL
        sql = self.workQueueMap.getSqlQuery()
        try:
            # start transaction
            self.conn.begin()
            self.cur.arraysize = 1000
            self.cur.execute(sql + comment)
            res = self.cur.fetchall()
            if not self._commit():
                raise RuntimeError("Commit error")
            # make map
            self.workQueueMap.makeMap(res, leave_shares)
            tmpLog.debug("done")
            self.updateTimeForWorkQueue = naive_utcnow()
            return True
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return False


# get entity module
def get_entity_module(base_mod) -> EntityModule:
    return base_mod.get_composite_module("entity")
