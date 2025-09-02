import copy
import datetime
import json
import os
import socket
import sys
import time
import traceback

from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandautils.PandaUtils import naive_utcnow

from pandajedi.jedibrokerage import AtlasBrokerUtils
from pandajedi.jediconfig import jedi_config
from pandajedi.jedicore.MsgWrapper import MsgWrapper
from pandaserver.dataservice import DataServiceUtils
from pandaserver.taskbuffer.ResourceSpec import ResourceSpecMapper

from .WatchDogBase import WatchDogBase

logger = PandaLogger().getLogger(__name__.split(".")[-1])

# dry run or not
DRY_RUN = False

# dedicated workqueue for preassigned task
magic_workqueue_id = 400
magic_workqueue_name = "wd_queuefiller"

# wait time before reassign jobs in seconds
reassign_jobs_wait_time = 300


# queue filler watchdog for ATLAS
class AtlasQueueFillerWatchDog(WatchDogBase):
    # constructor
    def __init__(self, taskBufferIF, ddmIF):
        WatchDogBase.__init__(self, taskBufferIF, ddmIF)
        self.pid = f"{socket.getfqdn().split('.')[0]}-{os.getpid()}-dog"
        self.vo = "atlas"
        self.prodSourceLabelList = ["managed"]

        # keys for cache
        self.dc_main_key = "AtlasQueueFillerWatchDog"
        self.dc_sub_key_pt = "PreassignedTasks"
        self.dc_sub_key_bt = "BlacklistedTasks"
        self.dc_sub_key_attr = "OriginalTaskAttributes"
        self.dc_sub_key_ses = "SiteEmptySince"

        # initialize the resource_spec_mapper object
        resource_types = taskBufferIF.load_resource_types()
        self.resource_spec_mapper = ResourceSpecMapper(resource_types)

        self.refresh()

    # refresh information stored in the instance
    def refresh(self):
        # work queue mapper
        self.workQueueMapper = self.taskBufferIF.getWorkQueueMap()
        # site mapper
        self.siteMapper = self.taskBufferIF.get_site_mapper()
        # all sites
        allSiteList = []
        for siteName, tmpSiteSpec in self.siteMapper.siteSpecList.items():
            # if tmpSiteSpec.type == 'analysis' or tmpSiteSpec.is_grandly_unified():
            allSiteList.append(siteName)
        self.allSiteList = allSiteList

    # update preassigned task map to cache
    def _update_to_pt_cache(self, ptmap):
        data_json = json.dumps(ptmap)
        self.taskBufferIF.updateCache_JEDI(main_key=self.dc_main_key, sub_key=self.dc_sub_key_pt, data=data_json)

    # get preassigned task map from cache
    def _get_from_pt_cache(self):
        cache_spec = self.taskBufferIF.getCache_JEDI(main_key=self.dc_main_key, sub_key=self.dc_sub_key_pt)
        if cache_spec is not None:
            ret_map = json.loads(cache_spec.data)
            return ret_map
        else:
            return dict()

    # update blacklisted task map to cache
    def _update_to_bt_cache(self, btmap):
        data_json = json.dumps(btmap)
        self.taskBufferIF.updateCache_JEDI(main_key=self.dc_main_key, sub_key=self.dc_sub_key_bt, data=data_json)

    # get blacklisted task map from cache
    def _get_from_bt_cache(self):
        cache_spec = self.taskBufferIF.getCache_JEDI(main_key=self.dc_main_key, sub_key=self.dc_sub_key_bt)
        if cache_spec is not None:
            ret_map = json.loads(cache_spec.data)
            return ret_map
        else:
            return dict()

    # update task original attributes map to cache
    def _update_to_attr_cache(self, attrmap):
        data_json = json.dumps(attrmap)
        self.taskBufferIF.updateCache_JEDI(main_key=self.dc_main_key, sub_key=self.dc_sub_key_attr, data=data_json)

    # get task original attributes map from cache
    def _get_from_attr_cache(self):
        cache_spec = self.taskBufferIF.getCache_JEDI(main_key=self.dc_main_key, sub_key=self.dc_sub_key_attr)
        if cache_spec is not None:
            ret_map = json.loads(cache_spec.data)
            return ret_map
        else:
            return dict()

    # update site empty-since map to cache
    def _update_to_ses_cache(self, sesmap):
        data_json = json.dumps(sesmap)
        self.taskBufferIF.updateCache_JEDI(main_key=self.dc_main_key, sub_key=self.dc_sub_key_ses, data=data_json)

    # get site empty-since map from cache
    def _get_from_ses_cache(self):
        cache_spec = self.taskBufferIF.getCache_JEDI(main_key=self.dc_main_key, sub_key=self.dc_sub_key_ses)
        if cache_spec is not None:
            ret_map = json.loads(cache_spec.data)
            return ret_map
        else:
            return dict()

    # get process lock to preassign
    def _get_lock(self):
        return self.taskBufferIF.lockProcess_JEDI(
            vo=self.vo,
            prodSourceLabel="managed",
            cloud=None,
            workqueue_id=None,
            resource_name=None,
            component="AtlasQueueFillerWatchDog.preassign",
            pid=self.pid,
            timeLimit=5,
        )

    # get map of site to list of RSEs
    def get_site_rse_map(self, prod_source_label):
        site_rse_map = {}
        for tmpPseudoSiteName in self.allSiteList:
            tmpSiteSpec = self.siteMapper.getSite(tmpPseudoSiteName)
            tmpSiteName = tmpSiteSpec.get_unified_name()
            scope_input, scope_output = DataServiceUtils.select_scope(tmpSiteSpec, prod_source_label, prod_source_label)
            try:
                endpoint_token_map = tmpSiteSpec.ddm_endpoints_input[scope_input].all
            except KeyError:
                continue
            else:
                # fill
                site_rse_map[tmpSiteName] = [k for k, v in endpoint_token_map.items() if v.get("order_read") is not None]
        # return
        return site_rse_map

    # get to-running rate of sites between 24 hours ago ~ 6 hours ago
    def get_site_trr_map(self):
        ret_val, ret_map = AtlasBrokerUtils.getSiteToRunRateStats(self.taskBufferIF, self.vo, time_window=86400, cutoff=0, cache_lifetime=600)
        if ret_val:
            return ret_map
        else:
            return None

    # get available sites sorted list
    def get_available_sites_list(self):
        tmp_log = MsgWrapper(logger, "get_available_sites_list")
        # initialize
        available_sites_dict = {}
        # get global share
        tmpSt, jobStatPrioMap = self.taskBufferIF.getJobStatisticsByGlobalShare(self.vo)
        if not tmpSt:
            # got nothing...
            return available_sites_dict
        # get to-running rate of sites
        site_trr_map = self.get_site_trr_map()
        if site_trr_map is None:
            return available_sites_dict
        # record for excluded site reasons
        excluded_sites_dict = {
            "not_online": set(),
            "has_minrss": set(),
            "es_jobseed": set(),
            "low_trr": set(),
            "enough_nq": set(),
        }
        # loop over sites
        for tmpPseudoSiteName in self.allSiteList:
            tmpSiteSpec = self.siteMapper.getSite(tmpPseudoSiteName)
            tmpSiteName = tmpSiteSpec.get_unified_name()
            # skip site already added
            if tmpSiteName in available_sites_dict:
                continue
            # skip site if not for production
            if not tmpSiteSpec.runs_production():
                continue
            # skip site is not online
            if tmpSiteSpec.status not in ("online"):
                excluded_sites_dict["not_online"].add(tmpSiteName)
                continue
            # skip if site has memory limitations
            if tmpSiteSpec.minrss not in (0, None):
                excluded_sites_dict["has_minrss"].add(tmpPseudoSiteName)
                continue
            # skip if site has event service jobseed
            if tmpSiteSpec.getJobSeed() in ["es"]:
                excluded_sites_dict["es_jobseed"].add(tmpPseudoSiteName)
                continue
            # tmp_num_slots as  num_slots in harvester_slots
            tmp_num_slots = tmpSiteSpec.getNumStandby(None, None)
            tmp_num_slots = 0 if tmp_num_slots is None else tmp_num_slots
            # skip if site has no harvester_slots setup and has not enough activity in the past 24 hours
            site_trr = site_trr_map.get(tmpSiteName)
            if tmp_num_slots == 0 and (site_trr is None or site_trr < 0.6):
                excluded_sites_dict["low_trr"].add(tmpSiteName)
                continue
            # get nQueue and nRunning
            nRunning = AtlasBrokerUtils.getNumJobs(jobStatPrioMap, tmpSiteName, "running")
            nQueue = 0
            for jobStatus in ["activated", "starting"]:
                nQueue += AtlasBrokerUtils.getNumJobs(jobStatPrioMap, tmpSiteName, jobStatus)
            # get nStandby; for queues that specify num_slots in harvester_slots
            tmp_core_count = tmpSiteSpec.coreCount if tmpSiteSpec.coreCount else 8
            nStandby = tmp_num_slots // tmp_core_count
            # available sites: must be idle now
            n_jobs_to_fill = max(20, max(nRunning, nStandby) * 2) * 0.25 - nQueue
            if n_jobs_to_fill > 0:
                available_sites_dict[tmpSiteName] = (tmpSiteName, tmpSiteSpec, n_jobs_to_fill)
            else:
                excluded_sites_dict["enough_nq"].add(tmpSiteName)
        # list
        available_sites_list = list(available_sites_dict.values())
        # sort by n_jobs_to_fill
        available_sites_list.sort(key=(lambda x: x[2]), reverse=True)
        # log message
        for reason, sites_set in excluded_sites_dict.items():
            excluded_sites_str = ",".join(sorted(sites_set))
            tmp_log.debug(f"excluded sites due to {reason} : {excluded_sites_str}")
        included_sites_str = ",".join(sorted([x[0] for x in available_sites_list]))
        tmp_log.debug("included sites : {sites}".format(reason=reason, sites=included_sites_str))
        # return
        return available_sites_list

    # get busy sites
    def get_busy_sites(self):
        busy_sites_dict = {}
        # get global share
        tmpSt, jobStatPrioMap = self.taskBufferIF.getJobStatisticsByGlobalShare(self.vo)
        if not tmpSt:
            # got nothing...
            return busy_sites_dict
        # get to-running rate of sites
        site_trr_map = self.get_site_trr_map()
        if site_trr_map is None:
            return busy_sites_dict
        # loop over sites
        for tmpPseudoSiteName in self.allSiteList:
            tmpSiteSpec = self.siteMapper.getSite(tmpPseudoSiteName)
            tmpSiteName = tmpSiteSpec.get_unified_name()
            # skip site already added
            if tmpSiteName in busy_sites_dict:
                continue
            # initialize
            is_busy = False
            # site is not online viewed as busy
            if tmpSiteSpec.status not in ("online"):
                is_busy = True
            # tmp_num_slots as  num_slots in harvester_slots
            tmp_num_slots = tmpSiteSpec.getNumStandby(None, None)
            tmp_num_slots = 0 if tmp_num_slots is None else tmp_num_slots
            # get nStandby; for queues that specify num_slots in harvester_slots
            tmp_core_count = tmpSiteSpec.coreCount if tmpSiteSpec.coreCount else 8
            nStandby = tmp_num_slots // tmp_core_count
            # get nQueue and nRunning
            nRunning = AtlasBrokerUtils.getNumJobs(jobStatPrioMap, tmpSiteName, "running")
            nQueue = 0
            for jobStatus in ["activated", "starting"]:
                nQueue += AtlasBrokerUtils.getNumJobs(jobStatPrioMap, tmpSiteName, jobStatus)
            # busy sites
            if nQueue > max(max(20, nRunning * 2) * 0.375, nStandby):
                busy_sites_dict[tmpSiteName] = tmpSiteSpec
        # return
        return busy_sites_dict

    # preassign tasks to site
    def do_preassign(self):
        tmp_log = MsgWrapper(logger, "do_preassign")
        # refresh
        self.refresh()
        # list of resource type
        resource_type_list = [rt.resource_name for rt in self.taskBufferIF.load_resource_types()]
        # threshold of time duration in second that the queue keeps empty to trigger preassigning
        empty_duration_threshold = 1800
        # return map
        ret_map = {
            "to_reassign": {},
        }
        # loop
        for prod_source_label in self.prodSourceLabelList:
            # site-rse map
            site_rse_map = self.get_site_rse_map(prod_source_label)
            # parameters from GDP config
            max_preassigned_tasks = self.taskBufferIF.getConfigValue("queue_filler", f"MAX_PREASSIGNED_TASKS_{prod_source_label}", "jedi", self.vo)
            if max_preassigned_tasks is None:
                max_preassigned_tasks = 3
            min_files_ready = self.taskBufferIF.getConfigValue("queue_filler", f"MIN_FILES_READY_{prod_source_label}", "jedi", self.vo)
            if min_files_ready is None:
                min_files_ready = 50
            min_files_remaining = self.taskBufferIF.getConfigValue("queue_filler", f"MIN_FILES_REMAINING_{prod_source_label}", "jedi", self.vo)
            if min_files_remaining is None:
                min_files_remaining = 100
            # load site empty-since map from cache
            site_empty_since_map_orig = self._get_from_ses_cache()
            # available sites
            available_sites_list = self.get_available_sites_list()
            # now timestamp
            now_time = naive_utcnow()
            now_time_ts = int(now_time.timestamp())
            # update site empty-since map
            site_empty_since_map = copy.deepcopy(site_empty_since_map_orig)
            available_site_name_list = [x[0] for x in available_sites_list]
            for site in site_empty_since_map_orig:
                # remove sites that are no longer empty
                if site not in available_site_name_list:
                    del site_empty_since_map[site]
            for site in available_site_name_list:
                # add newly found empty sites
                if site not in site_empty_since_map_orig:
                    site_empty_since_map[site] = now_time_ts
            self._update_to_ses_cache(site_empty_since_map)
            # evaluate sites to preassign according to cache
            # get blacklisted_tasks_map from cache
            blacklisted_tasks_map = self._get_from_bt_cache()
            blacklisted_tasks_set = set()
            for bt_list in blacklisted_tasks_map.values():
                blacklisted_tasks_set |= set(bt_list)
            # loop over available sites to preassign
            for site, tmpSiteSpec, n_jobs_to_fill in available_sites_list:
                # rses of the available site
                available_rses = set()
                try:
                    available_rses.update(set(site_rse_map[site]))
                except KeyError:
                    tmp_log.debug(f"skipped {site} since no good RSE")
                    continue
                # do not consider TAPE rses
                for rse in set(available_rses):
                    if "TAPE" in str(rse):
                        available_rses.remove(rse)
                # skip if no rse for available site
                if not available_rses:
                    tmp_log.debug(f"skipped {site} since no available RSE")
                    continue
                # skip if no coreCount set
                if not tmpSiteSpec.coreCount or tmpSiteSpec.coreCount <= 0:
                    tmp_log.debug(f"skipped {site} since coreCount is not set")
                    continue
                # now timestamp
                now_time = naive_utcnow()
                now_time_ts = int(now_time.timestamp())
                # skip if not empty for long enough
                if site not in site_empty_since_map:
                    tmp_log.error(f"skipped {site} since not in empty-since map (should not happen)")
                    continue
                empty_duration = now_time_ts - site_empty_since_map[site]
                tmp_num_slots = tmpSiteSpec.getNumStandby(None, None)
                if empty_duration < empty_duration_threshold and not tmp_num_slots:
                    tmp_log.debug(f"skipped {site} since not empty for enough time ({empty_duration}s < {empty_duration_threshold}s)")
                    continue
                # only simul tasks if site has fairsharePolicy setup
                processing_type_constraint = ""
                if tmpSiteSpec.fairsharePolicy not in ("NULL", None):
                    if "type=simul:0%" in tmpSiteSpec.fairsharePolicy:
                        # skip if zero share of simul
                        tmp_log.debug(f"skipped {site} since with fairshare but zero for simul")
                        continue
                    else:
                        processing_type_constraint = "AND t.processingType='simul' "
                # site attributes
                site_maxrss = tmpSiteSpec.maxrss if tmpSiteSpec.maxrss not in (0, None) else 999999
                site_corecount = tmpSiteSpec.coreCount
                site_capability = str(tmpSiteSpec.capability).lower()
                # make sql parameters of rses
                available_rses = list(available_rses)
                rse_params_list = []
                rse_params_map = {}
                for j, rse in enumerate(available_rses):
                    rse_param = f":rse_{j + 1}"
                    rse_params_list.append(rse_param)
                    rse_params_map[rse_param] = rse
                rse_params_str = ",".join(rse_params_list)
                # sql
                sql_query = (
                    "SELECT t.jediTaskID, t.workQueue_ID "
                    "FROM {jedi_schema}.JEDI_Tasks t "
                    "WHERE t.status IN ('ready','running') AND t.lockedBy IS NULL "
                    "AND t.prodSourceLabel=:prodSourceLabel "
                    "AND t.resource_type=:resource_type "
                    "AND site IS NULL "
                    "AND (COALESCE(t.baseRamCount, 0) + (CASE WHEN t.ramUnit IN ('MBPerCore','MBPerCoreFixed') THEN t.ramCount*:site_corecount ELSE t.ramCount END))*0.95 < :site_maxrss "
                    "AND t.eventService=0 "
                    "AND EXISTS ( "
                    "SELECT * FROM {jedi_schema}.JEDI_Dataset_Locality dl "
                    "WHERE dl.jediTaskID=t.jediTaskID "
                    "AND dl.rse IN ({rse_params_str}) "
                    ") "
                    "{processing_type_constraint} "
                    "AND EXISTS ( "
                    "SELECT d.datasetID FROM {jedi_schema}.JEDI_Datasets d "
                    "WHERE t.jediTaskID=d.jediTaskID AND d.type='input' "
                    "AND d.nFilesToBeUsed-d.nFilesUsed>=:min_files_ready "
                    "AND d.nFiles-d.nFilesUsed>=:min_files_remaining "
                    ") "
                    "ORDER BY t.currentPriority DESC "
                    "FOR UPDATE "
                ).format(jedi_schema=jedi_config.db.schemaJEDI, rse_params_str=rse_params_str, processing_type_constraint=processing_type_constraint)
                # loop over resource type
                for resource_type in resource_type_list:
                    # key name for preassigned_tasks_map = site + resource_type
                    key_name = f"{site}|{resource_type}"

                    # skip if no match: site is single core and resource_type is multi core
                    if site_capability == "score" and self.resource_spec_mapper.is_multi_core(resource_type):
                        continue

                    # skip if no match: site is multi core and resource_type is single core
                    elif site_capability == "mcore" and self.resource_spec_mapper.is_single_core(resource_type):
                        continue

                    params_map = {
                        ":prodSourceLabel": prod_source_label,
                        ":resource_type": resource_type,
                        ":site_maxrss": site_maxrss,
                        ":site_corecount": site_corecount,
                        ":min_files_ready": min_files_ready,
                        ":min_files_remaining": min_files_remaining,
                    }
                    params_map.update(rse_params_map)
                    # get preassigned_tasks_map from cache
                    preassigned_tasks_map = self._get_from_pt_cache()
                    preassigned_tasks_cached = preassigned_tasks_map.get(key_name, [])
                    # get task_orig_attr_map from cache
                    task_orig_attr_map = self._get_from_attr_cache()
                    # number of tasks already preassigned
                    n_preassigned_tasks = len(preassigned_tasks_cached)
                    # number of tasks to preassign
                    n_tasks_to_preassign = max(max_preassigned_tasks - n_preassigned_tasks, 0)
                    # preassign
                    if n_tasks_to_preassign <= 0:
                        tmp_log.debug(f"{key_name:<64} already has enough preassigned tasks ({n_preassigned_tasks:>3}) ; skipped ")
                    elif DRY_RUN:
                        dry_sql_query = (
                            "SELECT t.jediTaskID, t.workQueue_ID "
                            "FROM {jedi_schema}.JEDI_Tasks t "
                            "WHERE t.status IN ('ready','running') AND t.lockedBy IS NULL "
                            "AND t.prodSourceLabel=:prodSourceLabel "
                            "AND t.resource_type=:resource_type "
                            "AND site IS NULL "
                            "AND (COALESCE(t.baseRamCount, 0) + (CASE WHEN t.ramUnit IN ('MBPerCore','MBPerCoreFixed') THEN t.ramCount*:site_corecount ELSE t.ramCount END))*0.95 < :site_maxrss "
                            "AND t.eventService=0 "
                            "AND EXISTS ( "
                            "SELECT * FROM {jedi_schema}.JEDI_Dataset_Locality dl "
                            "WHERE dl.jediTaskID=t.jediTaskID "
                            "AND dl.rse IN ({rse_params_str}) "
                            ") "
                            "{processing_type_constraint} "
                            "AND EXISTS ( "
                            "SELECT d.datasetID FROM {jedi_schema}.JEDI_Datasets d "
                            "WHERE t.jediTaskID=d.jediTaskID AND d.type='input' "
                            "AND d.nFilesToBeUsed-d.nFilesUsed>=:min_files_ready "
                            "AND d.nFiles-d.nFilesUsed>=:min_files_remaining "
                            ") "
                            "ORDER BY t.currentPriority DESC "
                        ).format(jedi_schema=jedi_config.db.schemaJEDI, rse_params_str=rse_params_str, processing_type_constraint=processing_type_constraint)
                        # tmp_log.debug('[dry run] {} {}'.format(dry_sql_query, params_map))
                        res = self.taskBufferIF.querySQL(dry_sql_query, params_map)
                        n_tasks = 0 if res is None else len(res)
                        if n_tasks > 0:
                            result = [x[0] for x in res if x[0] not in preassigned_tasks_cached]
                            updated_tasks = result[:n_tasks_to_preassign]
                            tmp_log.debug(f"[dry run] {key_name:<64} {n_tasks_to_preassign:>3} tasks would be preassigned ")
                            # update preassigned_tasks_map into cache
                            preassigned_tasks_map[key_name] = list(set(updated_tasks) | set(preassigned_tasks_cached))
                            tmp_log.debug(f"{str(updated_tasks)} ; {str(preassigned_tasks_map[key_name])}")
                            self._update_to_pt_cache(preassigned_tasks_map)
                    else:
                        updated_tasks_orig_attr = self.taskBufferIF.queryTasksToPreassign_JEDI(
                            sql_query, params_map, site, blacklist=blacklisted_tasks_set, limit=n_tasks_to_preassign
                        )
                        if updated_tasks_orig_attr is None:
                            # dbproxy method failed
                            tmp_log.error(f"{key_name:<64} failed to preassign tasks ")
                        else:
                            n_tasks = len(updated_tasks_orig_attr)
                            if n_tasks > 0:
                                updated_tasks = [x[0] for x in updated_tasks_orig_attr]
                                tmp_log.info(f"{key_name:<64} {str(n_tasks):>3} tasks preassigned : {updated_tasks}")
                                # update preassigned_tasks_map into cache
                                preassigned_tasks_map[key_name] = list(set(updated_tasks) | set(preassigned_tasks_cached))
                                self._update_to_pt_cache(preassigned_tasks_map)
                                # update task_orig_attr_map into cache and return map
                                for taskid, orig_attr in updated_tasks_orig_attr:
                                    taskid_str = str(taskid)
                                    task_orig_attr_map[taskid_str] = orig_attr
                                    ret_map["to_reassign"][taskid] = {
                                        "site": site,
                                        "n_jobs_to_fill": n_jobs_to_fill,
                                    }
                                self._update_to_attr_cache(task_orig_attr_map)
                                # Kibana log
                                for taskid in updated_tasks:
                                    tmp_log.debug(f"#ATM #KV jediTaskID={taskid} action=do_preassign site={site} rtype={resource_type} preassigned ")
                            else:
                                tmp_log.debug(f"{key_name:<64} found no proper task to preassign")
        # total preassigned tasks
        preassigned_tasks_map = self._get_from_pt_cache()
        n_pt_tot = sum([len(pt_list) for pt_list in preassigned_tasks_map.values()])
        tmp_log.debug(f"now {n_pt_tot} tasks preassigned in total")
        # return
        return ret_map

    # undo preassign tasks
    def undo_preassign(self):
        tmp_log = MsgWrapper(logger, "undo_preassign")
        # refresh
        self.refresh()
        # busy sites
        busy_sites_dict = self.get_busy_sites()
        # loop to undo preassigning
        for prod_source_label in self.prodSourceLabelList:
            # parameter from GDP config
            max_preassigned_tasks = self.taskBufferIF.getConfigValue("queue_filler", f"MAX_PREASSIGNED_TASKS_{prod_source_label}", "jedi", self.vo)
            if max_preassigned_tasks is None:
                max_preassigned_tasks = 3
            min_files_ready = self.taskBufferIF.getConfigValue("queue_filler", f"MIN_FILES_READY_{prod_source_label}", "jedi", self.vo)
            if min_files_ready is None:
                min_files_ready = 50
            min_files_remaining = self.taskBufferIF.getConfigValue("queue_filler", f"MIN_FILES_REMAINING_{prod_source_label}", "jedi", self.vo)
            if min_files_remaining is None:
                min_files_remaining = 100
            # clean up outdated blacklist
            blacklist_duration_hours = 12
            blacklisted_tasks_map_orig = self._get_from_bt_cache()
            blacklisted_tasks_map = copy.deepcopy(blacklisted_tasks_map_orig)
            now_time = naive_utcnow()
            min_allowed_time = now_time - datetime.timedelta(hours=blacklist_duration_hours)
            min_allowed_ts = int(min_allowed_time.timestamp())
            for ts_str in blacklisted_tasks_map_orig:
                ts = int(ts_str)
                if ts < min_allowed_ts:
                    del blacklisted_tasks_map[ts_str]
            self._update_to_bt_cache(blacklisted_tasks_map)
            n_bt_old = sum([len(bt_list) for bt_list in blacklisted_tasks_map_orig.values()])
            n_bt = sum([len(bt_list) for bt_list in blacklisted_tasks_map.values()])
            tmp_log.debug(f"done cleanup blacklist; before {n_bt_old} , now {n_bt} tasks in blacklist")
            # get a copy of preassigned_tasks_map from cache
            preassigned_tasks_map_orig = self._get_from_pt_cache()
            preassigned_tasks_map = copy.deepcopy(preassigned_tasks_map_orig)
            # clean up task_orig_attr_map in cache
            task_orig_attr_map_orig = self._get_from_attr_cache()
            task_orig_attr_map = copy.deepcopy(task_orig_attr_map_orig)
            all_preassigned_taskids = set()
            for taskid_list in preassigned_tasks_map_orig.values():
                all_preassigned_taskids |= set(taskid_list)
            for taskid_str in task_orig_attr_map_orig:
                taskid = int(taskid_str)
                if taskid not in all_preassigned_taskids:
                    del task_orig_attr_map[taskid_str]
            self._update_to_attr_cache(task_orig_attr_map)
            # loop on preassigned tasks in cache
            for key_name in preassigned_tasks_map_orig:
                # parse key name = site + resource_type
                site, resource_type = key_name.split("|")
                # preassigned tasks in cache
                preassigned_tasks_cached = preassigned_tasks_map.get(key_name, [])
                # force_undo=True for all tasks in busy sites, and force_undo=False for tasks not in status to generate jobs
                force_undo = False
                if site in busy_sites_dict or len(preassigned_tasks_cached) > max_preassigned_tasks:
                    force_undo = True
                reason_str = (
                    "site busy or offline or with too many preassigned tasks" if force_undo else "task paused/terminated or without enough files to process"
                )
                # parameters for undo, kinda ugly
                params_map = {
                    ":min_files_ready": min_files_ready,
                    ":min_files_remaining": min_files_remaining,
                }
                # undo preassign
                had_undo = False
                updated_tasks = []
                if DRY_RUN:
                    if force_undo:
                        updated_tasks = list(preassigned_tasks_cached)
                        n_tasks = len(updated_tasks)
                    else:
                        preassigned_tasks_list = []
                        preassigned_tasks_params_map = {}
                        for j, taskid in enumerate(preassigned_tasks_cached):
                            pt_param = f":pt_{j + 1}"
                            preassigned_tasks_list.append(pt_param)
                            preassigned_tasks_params_map[pt_param] = taskid
                        if not preassigned_tasks_list:
                            continue
                        preassigned_tasks_params_str = ",".join(preassigned_tasks_list)
                        dry_sql_query = (
                            "SELECT t.jediTaskID "
                            "FROM {jedi_schema}.JEDI_Tasks t "
                            "WHERE t.jediTaskID IN ({preassigned_tasks_params_str}) "
                            "AND t.site IS NOT NULL "
                            "AND NOT ( "
                            "t.status IN ('ready','running') "
                            "AND EXISTS ( "
                            "SELECT d.datasetID FROM {0}.JEDI_Datasets d "
                            "WHERE t.jediTaskID=d.jediTaskID AND d.type='input' "
                            "AND d.nFilesToBeUsed-d.nFilesUsed>=:min_files_ready AND d.nFiles-d.nFilesUsed>=:min_files_remaining "
                            ") "
                            ") "
                        ).format(jedi_schema=jedi_config.db.schemaJEDI, preassigned_tasks_params_str=preassigned_tasks_params_str)
                        res = self.taskBufferIF.querySQL(dry_sql_query, preassigned_tasks_params_map)
                        n_tasks = 0 if res is None else len(res)
                        if n_tasks > 0:
                            updated_tasks = [x[0] for x in res]
                    # tmp_log.debug('[dry run] {} {} force={}'.format(key_name, str(updated_tasks), force_undo))
                    had_undo = True
                    if n_tasks > 0:
                        tmp_log.debug(f"[dry run] {key_name:<64} {n_tasks:>3} preassigned tasks would be undone ({reason_str}) ")
                else:
                    updated_tasks = self.taskBufferIF.undoPreassignedTasks_JEDI(
                        preassigned_tasks_cached, task_orig_attr_map=task_orig_attr_map, params_map=params_map, force=force_undo
                    )
                    if updated_tasks is None:
                        # dbproxy method failed
                        tmp_log.error(f"{key_name:<64} failed to undo preassigned tasks (force={force_undo})")
                    else:
                        had_undo = True
                        n_tasks = len(updated_tasks)
                        if n_tasks > 0:
                            tmp_log.info(f"{key_name:<64} {str(n_tasks):>3} preassigned tasks undone ({reason_str}) : {updated_tasks} ")
                            # Kibana log
                            for taskid in updated_tasks:
                                tmp_log.debug(
                                    f"#ATM #KV jediTaskID={taskid} action=undo_preassign site={site} rtype={resource_type} un-preassigned since {reason_str}"
                                )
                # update preassigned_tasks_map into cache
                if had_undo:
                    if force_undo:
                        del preassigned_tasks_map[key_name]
                    else:
                        tmp_tasks_set = set(preassigned_tasks_cached) - set(updated_tasks)
                        if not tmp_tasks_set:
                            del preassigned_tasks_map[key_name]
                        else:
                            preassigned_tasks_map[key_name] = list(tmp_tasks_set)
                    self._update_to_pt_cache(preassigned_tasks_map)
                # update blacklisted_tasks_map into cache
                if had_undo and not force_undo:
                    blacklisted_tasks_map_orig = self._get_from_bt_cache()
                    blacklisted_tasks_map = copy.deepcopy(blacklisted_tasks_map_orig)
                    now_time = naive_utcnow()
                    now_rounded_ts = int(now_time.replace(minute=0, second=0, microsecond=0).timestamp())
                    ts_str = str(now_rounded_ts)
                    if ts_str in blacklisted_tasks_map_orig:
                        tmp_bt_list = blacklisted_tasks_map[ts_str]
                        blacklisted_tasks_map[ts_str] = list(set(tmp_bt_list) | set(updated_tasks))
                    else:
                        blacklisted_tasks_map[ts_str] = list(updated_tasks)
                    self._update_to_bt_cache(blacklisted_tasks_map)

    # close and reassign jobs of preassigned tasks
    def reassign_jobs(self, to_reassign_map):
        tmp_log = MsgWrapper(logger, "reassign_jobs")
        for jedi_taskid, value_map in to_reassign_map.items():
            site = value_map["site"]
            n_jobs_to_fill = value_map["n_jobs_to_fill"]
            # compute n_jobs_to_close from n_jobs_to_fill
            n_jobs_to_close = int(n_jobs_to_fill / 3)
            # reassign
            n_jobs_closed = self.taskBufferIF.reassignJobsInPreassignedTask_JEDI(jedi_taskid, site, n_jobs_to_close)
            if n_jobs_closed is None:
                tmp_log.debug(f"jediTaskID={jedi_taskid} no longer ready/running or not assigned to {site} , skipped")
            else:
                tmp_log.debug(f"jediTaskID={jedi_taskid} to {site} , closed {n_jobs_closed} jobs")

    # main
    def doAction(self):
        try:
            # get logger
            origTmpLog = MsgWrapper(logger)
            origTmpLog.debug("start")
            # lock
            got_lock = self._get_lock()
            if not got_lock:
                origTmpLog.debug("locked by another process. Skipped")
                return self.SC_SUCCEEDED
            origTmpLog.debug("got lock")
            # undo preassigned tasks
            self.undo_preassign()
            # preassign tasks to sites
            ret_map = self.do_preassign()
            # to-reassign map
            to_reassign_map = ret_map["to_reassign"]
            if to_reassign_map:
                # wait some minutes so that preassigned tasks can be brokered, before reassigning jobs
                origTmpLog.debug(f"wait {reassign_jobs_wait_time}s before reassigning jobs")
                time.sleep(reassign_jobs_wait_time)
                # reassign jobs of preassigned tasks
                self.reassign_jobs(to_reassign_map)
        except Exception:
            errtype, errvalue = sys.exc_info()[:2]
            err_str = traceback.format_exc()
            origTmpLog.error(f"failed with {errtype} {errvalue} ; {err_str}")
        # return
        origTmpLog.debug("done")
        return self.SC_SUCCEEDED
