import os
import socket
import sys
import traceback

from pandacommon.pandalogger.PandaLogger import PandaLogger

from pandajedi.jedibrokerage import AtlasBrokerUtils
from pandajedi.jediconfig import jedi_config
from pandajedi.jedicore.MsgWrapper import MsgWrapper
from pandaserver.dataservice import DataServiceUtils

from .WatchDogBase import WatchDogBase

logger = PandaLogger().getLogger(__name__.split(".")[-1])


# task withholder watchdog for ATLAS
class AtlasTaskWithholderWatchDog(WatchDogBase):
    # constructor
    def __init__(self, taskBufferIF, ddmIF):
        WatchDogBase.__init__(self, taskBufferIF, ddmIF)
        self.pid = f"{socket.getfqdn().split('.')[0]}-{os.getpid()}-dog"
        # self.cronActions = {'forPrestage': 'atlas_prs'}
        self.vo = "atlas"
        self.prodSourceLabelList = ["managed"]
        # call refresh
        self.refresh()

    # get process lock
    def _get_lock(self):
        return self.taskBufferIF.lockProcess_JEDI(
            vo=self.vo,
            prodSourceLabel="managed",
            cloud=None,
            workqueue_id=None,
            resource_name=None,
            component="AtlasTaskWithholderWatchDog",
            pid=self.pid,
            timeLimit=5,
        )

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

    # get map of site to list of RSEs and blacklisted RSEs
    def get_site_rse_map_and_blacklisted_rse_set(self, prod_source_label):
        site_rse_map = {}
        blacklisted_rse_set = set()
        for tmpPseudoSiteName in self.allSiteList:
            tmpSiteSpec = self.siteMapper.getSite(tmpPseudoSiteName)
            tmpSiteName = tmpSiteSpec.get_unified_name()
            scope_input, scope_output = DataServiceUtils.select_scope(tmpSiteSpec, prod_source_label, prod_source_label)
            try:
                tmp_ddm_spec = tmpSiteSpec.ddm_endpoints_input[scope_input]
                endpoint_name = tmpSiteSpec.ddm_input[scope_input]
                endpoint_token_map = tmp_ddm_spec.getTokenMap("input")
                tmp_endpoint = tmp_ddm_spec.getEndPoint(endpoint_name)
            except KeyError:
                continue
            else:
                # fill site rse map
                site_rse_map[tmpSiteName] = list(endpoint_token_map.values())
                # blacklisted rse
                if tmp_endpoint is not None and tmp_endpoint["blacklisted"] == "Y":
                    blacklisted_rse_set.add(endpoint_name)
        # return
        return site_rse_map, blacklisted_rse_set

    # get busy sites
    def get_busy_sites(self, gshare, cutoff):
        busy_sites_list = []
        # get global share
        tmpSt, jobStatPrioMap = self.taskBufferIF.getJobStatisticsByGlobalShare(self.vo)
        if not tmpSt:
            # got nothing...
            return busy_sites_list
        for tmpPseudoSiteName in self.allSiteList:
            tmpSiteSpec = self.siteMapper.getSite(tmpPseudoSiteName)
            tmpSiteName = tmpSiteSpec.get_unified_name()
            # get nQueue and nRunning
            nRunning = AtlasBrokerUtils.getNumJobs(jobStatPrioMap, tmpSiteName, "running", workQueue_tag=gshare)
            nQueue = 0
            for jobStatus in ["defined", "assigned", "activated", "starting"]:
                nQueue += AtlasBrokerUtils.getNumJobs(jobStatPrioMap, tmpSiteName, jobStatus, workQueue_tag=gshare)
            # busy sites
            if nQueue > max(cutoff, nRunning * 2):
                busy_sites_list.append(tmpSiteName)
        # return
        return busy_sites_list

    # # handle waiting jobs
    # def do_make_tasks_pending(self, task_list):
    #     tmpLog = MsgWrapper(logger, 'do_make_tasks_pending')
    #     tmpLog.debug('start')
    #     # check every x min
    #     checkInterval = 20
    #     # make task pending
    #     for taskID, pending_reason in task_list:
    #         tmpLog = MsgWrapper(logger, '< #ATM #KV do_make_tasks_pending jediTaskID={0}>'.format(taskID))
    #         retVal = self.taskBufferIF.makeTaskPending_JEDI(taskID, reason=pending_reason)
    #         tmpLog.debug('done with {0}'.format(retVal))

    # set tasks to be pending due to condition of data locality
    def do_for_data_locality(self):
        tmp_log = MsgWrapper(logger)
        # refresh
        self.refresh()
        # list of resource type
        # resource_type_list = [ rt.resource_name for rt in self.taskBufferIF.load_resource_types() ]
        # loop
        for prod_source_label in self.prodSourceLabelList:
            # site-rse map and blacklisted rses
            site_rse_map, blacklisted_rse_set = self.get_site_rse_map_and_blacklisted_rse_set(prod_source_label)
            tmp_log.debug(f"Found {len(blacklisted_rse_set)} blacklisted RSEs : {','.join(list(blacklisted_rse_set))}")
            # parameter from GDP config
            upplimit_ioIntensity = self.taskBufferIF.getConfigValue("task_withholder", f"LIMIT_IOINTENSITY_{prod_source_label}", "jedi", self.vo)
            lowlimit_currentPriority = self.taskBufferIF.getConfigValue("task_withholder", f"LIMIT_PRIORITY_{prod_source_label}", "jedi", self.vo)
            if upplimit_ioIntensity is None:
                upplimit_ioIntensity = 999999
            if lowlimit_currentPriority is None:
                lowlimit_currentPriority = -999999
            upplimit_ioIntensity = max(upplimit_ioIntensity, 100)
            # get work queue for gshare
            work_queue_list = self.workQueueMapper.getAlignedQueueList(self.vo, prod_source_label)
            # loop over work queue
            for work_queue in work_queue_list:
                gshare = work_queue.queue_name
                # get cutoff
                cutoff = self.taskBufferIF.getConfigValue("jobbroker", f"NQUEUELIMITSITE_{gshare}", "jedi", self.vo)
                if not cutoff:
                    cutoff = 20
                # busy sites
                busy_sites_list = self.get_busy_sites(gshare, cutoff)
                # rses of busy sites
                busy_rses = set()
                for site in busy_sites_list:
                    try:
                        busy_rses.update(set(site_rse_map[site]))
                    except KeyError:
                        continue
                # make sql parameters of rses
                to_exclude_rses = list(busy_rses | blacklisted_rse_set)
                if not to_exclude_rses:
                    continue
                rse_params_list = []
                rse_params_map = {}
                for j, rse in enumerate(to_exclude_rses):
                    rse_param = f":rse_{j + 1}"
                    rse_params_list.append(rse_param)
                    rse_params_map[rse_param] = rse
                rse_params_str = ",".join(rse_params_list)
                # sql
                sql_query = (
                    "SELECT t.jediTaskID "
                    "FROM {jedi_schema}.JEDI_Tasks t "
                    "WHERE t.status IN ('ready','running','scouting') AND t.lockedBy IS NULL "
                    "AND t.gshare=:gshare "
                    "AND t.ioIntensity>=:ioIntensity AND t.currentPriority<:currentPriority "
                    "AND EXISTS ( "
                    "SELECT * FROM {jedi_schema}.JEDI_Datasets d "
                    "WHERE d.jediTaskID=t.jediTaskID "
                    "AND d.type='input' "
                    ") "
                    "AND NOT EXISTS ( "
                    "SELECT * FROM {jedi_schema}.JEDI_Dataset_Locality dl "
                    "WHERE dl.jediTaskID=t.jediTaskID "
                    "AND dl.rse NOT IN ({rse_params_str}) "
                    ") "
                ).format(jedi_schema=jedi_config.db.schemaJEDI, rse_params_str=rse_params_str)
                # params map
                params_map = {
                    ":gshare": gshare,
                    ":ioIntensity": upplimit_ioIntensity,
                    ":currentPriority": lowlimit_currentPriority,
                }
                params_map.update(rse_params_map)
                # pending reason
                reason = (
                    "no local input data, ioIntensity>={ioIntensity}, currentPriority<{currentPriority},"
                    "nQueue>max({cutOff},nRunning*2) at all sites where the task can run".format(
                        ioIntensity=upplimit_ioIntensity, currentPriority=lowlimit_currentPriority, cutOff=cutoff
                    )
                )
                # set pending
                dry_run = False
                if dry_run:
                    dry_sql_query = (
                        "SELECT t.jediTaskID "
                        "FROM {jedi_schema}.JEDI_Tasks t "
                        "WHERE t.status IN ('ready','running','scouting') AND t.lockedBy IS NULL "
                        "AND t.gshare=:gshare "
                        "AND t.ioIntensity>=:ioIntensity AND t.currentPriority<:currentPriority "
                        "AND EXISTS ( "
                        "SELECT * FROM {jedi_schema}.JEDI_Datasets d "
                        "WHERE d.jediTaskID=t.jediTaskID "
                        "AND d.type='input' "
                        ") "
                        "AND NOT EXISTS ( "
                        "SELECT * FROM {jedi_schema}.JEDI_Dataset_Locality dl "
                        "WHERE dl.jediTaskID=t.jediTaskID "
                        "AND dl.rse NOT IN ({rse_params_str}) "
                        ") "
                    ).format(jedi_schema=jedi_config.db.schemaJEDI, rse_params_str=rse_params_str)
                    res = self.taskBufferIF.querySQL(dry_sql_query, params_map)
                    n_tasks = 0 if res is None else len(res)
                    if n_tasks > 0:
                        result = [x[0] for x in res]
                        tmp_log.debug(f'[dry run] gshare: {gshare:<16} {n_tasks:>5} tasks would be pending : {result} ; reason="{reason}" ')
                else:
                    n_tasks = self.taskBufferIF.queryTasksToBePending_JEDI(sql_query, params_map, reason)
                    if n_tasks is not None and n_tasks > 0:
                        tmp_log.info(f'gshare: {gshare:<16} {str(n_tasks):>5} tasks got pending ; reason="{reason}" ')

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
            # make tasks pending under certain conditions
            self.do_for_data_locality()
        except Exception:
            errtype, errvalue = sys.exc_info()[:2]
            err_str = traceback.format_exc()
            origTmpLog.error(f"failed with {errtype} {errvalue} ; {err_str}")
        # return
        origTmpLog.debug("done")
        return self.SC_SUCCEEDED
