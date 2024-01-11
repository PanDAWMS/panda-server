import copy
import datetime
import functools
import json
import os
import socket
import sys
import traceback
from zlib import adler32

import numpy as np
from pandacommon.pandalogger import logger_utils
from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandautils.thread_utils import GenericThread
from pandaserver.config import panda_config
from scipy import stats

# logger
main_logger = PandaLogger().getLogger("metric_collector")

# dry run
DRY_RUN = False

# list of metrics in FetchData to fetch data and update to DB. Format: (metric, key_type, period_minutes)
metric_list = [
    ("gshare_preference", "gshare", 5),
    ("analy_pmerge_jobs_wait_time", "site", 30),
    ("analy_site_eval", "site", 30),
    ("users_jobs_stats", "both", 5),
    ("analy_user_eval", "neither", 10),
]

# constant maps
class_value_rank_map = {1: "A_sites", 0: "B_sites", -1: "C_sites"}


def get_now_time_str():
    """
    Return string of nowtime that can be stored in DB
    """
    now_time = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
    ts_str = now_time.strftime("%Y-%m-%d %H:%M:%S")
    return ts_str


def conf_interval_upper(n, mean, stdev, cl=0.95):
    """
    Get estimated confidence level
    """
    max_value = 999999
    ciu = stats.t.ppf(cl, (n - 1), loc=mean, scale=stdev)
    ciu = min(ciu, max_value)
    return ciu


def weighted_stats(values, weights):
    """
    Return sum of weights, weighted mean and standard deviation
    """
    sum_of_weights = np.sum(weights)
    mean = np.average(values, weights=weights)
    variance = np.average((values - mean) ** 2, weights=weights)
    stdev = np.sqrt(variance)
    return sum_of_weights, mean, stdev


# get site slot to-running rate statistics
def get_site_strr_stats(tbuf, time_window=21600, cutoff=300):
    """
    :param time_window: float, time window in hours to compute slot to-running rate
    """
    # log
    tmp_log = logger_utils.make_logger(main_logger, "get_site_strr_stats")
    # timestamps
    current_time = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
    starttime_max = current_time - datetime.timedelta(seconds=cutoff)
    starttime_min = current_time - datetime.timedelta(seconds=time_window)
    # rounded with 10 minutes
    starttime_max_rounded = starttime_max.replace(minute=starttime_max.minute // 10 * 10, second=0, microsecond=0)
    starttime_min_rounded = starttime_min.replace(minute=starttime_min.minute // 10 * 10, second=0, microsecond=0)
    real_interval_hours = (starttime_max_rounded - starttime_min_rounded).total_seconds() / 3600
    # define the var map of query parameters
    var_map = {
        ":startTimeMin": starttime_min_rounded,
        ":startTimeMax": starttime_max_rounded,
    }
    # sql to query on jobs-tables (jobsactive4 and jobsArchived4)
    sql_jt = """
           SELECT computingSite, coreCount, COUNT(*) FROM %s
           WHERE vo='atlas'
           AND startTime IS NOT NULL AND startTime>=:startTimeMin AND startTime<:startTimeMax
           AND jobStatus IN ('running', 'holding', 'transferring', 'finished', 'cancelled')
           """
    sql_jt += """
           GROUP BY computingSite, coreCount
           """
    # job tables
    tables = [
        "ATLAS_PANDA.jobsActive4",
        "ATLAS_PANDA.jobsArchived4",
    ]
    # get
    return_map = {}
    try:
        for table in tables:
            sql_exe = (sql_jt) % table
            res = tbuf.querySQL(sql_exe, var_map)
            # create map
            for panda_site, core_count, n_count in res:
                # add site
                return_map.setdefault(panda_site, 0)
                # skip null coreCount
                if core_count is None or not core_count:
                    continue
                # increase to-running rate
                to_running_rate = n_count * core_count / real_interval_hours if real_interval_hours > 0 else 0
                return_map[panda_site] += to_running_rate
        # end loop
        # tmp_log.debug('done')
        return True, return_map
    except Exception as e:
        tmp_log.error(f"Exception {e.__class__.__name__}: {e}")
        return False, {}


# get each share and its leaf share
def fill_leaf_shares(key, val, the_list):
    if val is None:
        the_list.append(key)
    else:
        for k, v in val.items():
            fill_leaf_shares(k, v, the_list)


class MetricsDB(object):
    """
    Proxy to access the metrics table in DB
    """

    def __init__(self, tbuf):
        self.tbuf = tbuf

    def _decor(method):
        def _decorator(_method, *args, **kwargs):
            @functools.wraps(_method)
            def _wrapped_method(self, *args, **kwargs):
                try:
                    _method(self, *args, **kwargs)
                except Exception as exc:
                    pass

            return _wrapped_method

        return _decorator(method)

    def update(self, metric, key_type, entity_dict):
        tmp_log = logger_utils.make_logger(main_logger, "MetricsDB.update")
        # tmp_log.debug('start key={0} site={1}, gshare={2}'.format(key, site, gshare))
        # sql
        # sql_update = (
        #     """UPDATE ATLAS_PANDA.Metrics SET """
        #         """value_json = json_mergepatch(value_json, :patch_value_json), """
        #         """timestamp = :timestamp """
        #     """WHERE computingSite=:site AND gshare=:gshare AND metric=:metric """
        # )
        sql_update = (
            """UPDATE ATLAS_PANDA.Metrics SET """
            """value_json = :patch_value_json , """
            """timestamp = :timestamp """
            """WHERE computingSite=:site AND gshare=:gshare AND metric=:metric """
        )
        sql_insert = """INSERT INTO ATLAS_PANDA.Metrics """ """VALUES ( """ """:site, :gshare, :metric, :patch_value_json, :timestamp """ """) """
        # now
        now_time = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
        # var map template
        varMap_template = {
            ":site": None,
            ":gshare": None,
            ":metric": metric,
            ":timestamp": now_time,
            ":patch_value_json": None,
        }
        # make var map list
        varMap_list = []
        if key_type == "neither":
            # values to json string
            try:
                patch_value_json = json.dumps(entity_dict)
            except Exception:
                tmp_log.error(traceback.format_exc() + " " + str(v))
                return
            # initialize varMap
            varMap = varMap_template.copy()
            varMap.update(
                {
                    ":site": "NULL",
                    ":gshare": "NULL",
                    ":patch_value_json": patch_value_json,
                }
            )
            # append to the list
            varMap_list.append(varMap)
        else:
            for entity, v in entity_dict.items():
                # values to json string
                try:
                    patch_value_json = json.dumps(v)
                except Exception:
                    tmp_log.error(traceback.format_exc() + " " + str(v))
                    return
                # initialize varMap
                varMap = varMap_template.copy()
                varMap[":patch_value_json"] = patch_value_json
                # update varMap according to key_type
                if key_type == "site":
                    varMap.update(
                        {
                            ":site": entity,
                            ":gshare": "NULL",
                        }
                    )
                elif key_type == "gshare":
                    varMap.update(
                        {
                            ":site": "NULL",
                            ":gshare": entity,
                        }
                    )
                elif key_type == "both":
                    varMap.update(
                        {
                            ":site": entity[0],
                            ":gshare": entity[1],
                        }
                    )
                # append to the list
                varMap_list.append(varMap)
        # update
        n_row = self.tbuf.executemanySQL(sql_update, varMap_list)
        # try insert if not all rows updated
        if n_row < len(varMap_list):
            try:
                tmp_log.debug(f"only {n_row}/{len(varMap_list)} rows updated for metric={metric} ; trying insert")
                for varMap in varMap_list:
                    self.tbuf.querySQLS(sql_insert, varMap)
                tmp_log.debug(f"inserted for metric={metric}")
            except Exception:
                tmp_log.warning(f"failed to insert for metric={metric}")
        else:
            tmp_log.debug(f"updated for metric={metric}")
        # done
        # tmp_log.debug('done key={0} site={1}, gshare={2}'.format(key, site, gshare))

    def get_metrics(self, metric, key_type=None, fresher_than_minutes_ago=120):
        tmp_log = logger_utils.make_logger(main_logger, "MetricsDB.get_metrics")
        # tmp_log.debug('start key={0} site={1}, gshare={2}'.format(key, site, gshare))
        # sql
        sql_query = (
            """SELECT computingSite, gshare, value_json """ """FROM ATLAS_PANDA.Metrics """ """WHERE metric = :metric """ """AND timestamp >= :min_timestamp """
        )
        # now
        now_time = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
        # var map
        varMap = {
            ":metric": metric,
            ":min_timestamp": now_time - datetime.timedelta(minutes=fresher_than_minutes_ago),
        }
        # query
        res = self.tbuf.querySQL(sql_query, varMap)
        if res is None:
            tmp_log.warning(f"failed to query metric={metric}")
            return
        # key type default
        if key_type is None:
            key = {x[0]: x[1] for x in metric_list}.get(metric, "both")
        # return map
        ret_map = {}
        for computingSite, gshare, value_json in res:
            key = (computingSite, gshare)
            if key_type == "site":
                key = computingSite
            elif key_type == "gshare":
                key = gshare
            elif key_type == "neither":
                key = None
            if value_json is None:
                tmp_log.warning("missing data for " + str((computingSite, gshare)))
                continue
            try:
                value_dict = json.loads(value_json)
            except Exception:
                tmp_log.error(traceback.format_exc() + " " + str((computingSite, gshare)) + str(value_json))
                continue
            else:
                if key is None:
                    ret_map = copy.deepcopy(value_dict)
                else:
                    ret_map[key] = value_dict
        # return
        return ret_map


class FetchData(object):
    """
    methods to fetch or evaluate data values to store
    """

    def __init__(self, tbuf):
        self.tbuf = tbuf
        # initialize stored data
        self.gshare_status = None

    def analy_pmerge_jobs_wait_time(self):
        tmp_log = logger_utils.make_logger(main_logger, "FetchData")
        # sql
        sql_get_jobs_archived4 = (
            "SELECT pandaID, computingSite "
            "FROM ATLAS_PANDA.jobsArchived4 "
            "WHERE prodSourceLabel='user' "
            "AND gshare='User Analysis' "
            "AND (processingType='pmerge' OR prodUserName='gangarbt') "
            "AND modificationTime>:modificationTime "
        )
        sql_get_jobs_active4 = (
            "SELECT pandaID, computingSite "
            "FROM ATLAS_PANDA.jobsActive4 "
            "WHERE prodSourceLabel='user' "
            "AND gshare='User Analysis' "
            "AND jobStatus IN ('running', 'holding', 'merging', 'transferring', 'finished', 'failed', 'closed', 'cancelled') "
            "AND (processingType='pmerge' OR prodUserName='gangarbt') "
            "AND modificationTime>:modificationTime "
        )
        sql_get_latest_job_mtime_status = (
            "SELECT jobStatus, MIN(modificationTime) " "FROM ATLAS_PANDA.jobs_StatusLog " "WHERE pandaID=:pandaID " "GROUP BY jobStatus "
        )
        sql_get_site_workflow = "SELECT /* use_json_type */ scj.data.workflow " "FROM ATLAS_PANDA.schedconfig_json scj " "WHERE scj.panda_queue=:computingSite "
        sql_get_long_queuing_job_wait_time_template = (
            "SELECT COUNT(*), AVG(CURRENT_DATE-creationtime) "
            "FROM ATLAS_PANDA.jobsActive4 "
            "WHERE prodSourceLabel='user' "
            "AND gshare='User Analysis' "
            "AND jobStatus IN {q_status_list_str} "
            "AND (processingType='pmerge') "
            "AND computingSite=:computingSite "
            "AND (CURRENT_DATE-creationtime)>:w_mean "
        )
        try:
            # initialize
            tmp_site_dict = dict()
            # now time
            now_time = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
            # get user jobs
            varMap = {
                ":modificationTime": now_time - datetime.timedelta(days=4),
            }
            archived4_jobs_list = self.tbuf.querySQL(sql_get_jobs_archived4, varMap)
            active4_jobs_list = self.tbuf.querySQL(sql_get_jobs_active4, varMap)
            all_jobs_set = set()
            all_jobs_set.update(archived4_jobs_list)
            all_jobs_set.update(active4_jobs_list)
            n_tot_jobs = len(all_jobs_set)
            tmp_log.debug(f"got total {n_tot_jobs} jobs")
            # loop over jobs to get modificationTime when activated and running
            cc = 0
            for pandaID, site in all_jobs_set:
                if not site:
                    continue
                varMap = {":pandaID": pandaID}
                status_mtime_list = self.tbuf.querySQL(sql_get_latest_job_mtime_status, varMap)
                status_mtime_dict = dict(status_mtime_list)
                if "activated" not in status_mtime_dict or "running" not in status_mtime_dict:
                    continue
                wait_time = status_mtime_dict["running"] - status_mtime_dict["activated"]
                wait_time_sec = wait_time.total_seconds()
                if wait_time_sec < 0:
                    tmp_log.warning(f"job {pandaID} has negative wait time")
                    continue
                run_age_sec = int((now_time - status_mtime_dict["running"]).total_seconds())
                if run_age_sec < 0:
                    tmp_log.warning(f"job {pandaID} has negative run age")
                    continue
                tmp_site_dict.setdefault(site, {"wait_time": [], "run_age": []})
                tmp_site_dict[site]["wait_time"].append(wait_time_sec)
                tmp_site_dict[site]["run_age"].append(run_age_sec)
                # log message
                if cc > 0 and cc % 5000 == 0:
                    tmp_log.debug(f"... queried {cc:9d} jobs ...")
                cc += 1
            tmp_log.debug(f"queried {cc} jobs")
            # evaluate stats
            site_dict = dict()
            for site, data_dict in tmp_site_dict.items():
                site_dict.setdefault(site, {})
                n_jobs = len(data_dict["wait_time"])
                # init with nan
                mean = np.nan
                stdev = np.nan
                median = np.nan
                cl95upp = np.nan
                sum_of_weights = np.nan
                w_mean = np.nan
                w_stdev = np.nan
                w_cl95upp = np.nan
                long_q_n = np.nan
                long_q_mean = np.nan
                # fill the stats values
                if n_jobs > 0:
                    wait_time_array = np.array(data_dict["wait_time"])
                    run_age_array = np.array(data_dict["run_age"])
                    # stats
                    mean = np.mean(wait_time_array)
                    stdev = np.std(wait_time_array)
                    median = np.median(wait_time_array)
                    cl95upp = conf_interval_upper(n=n_jobs, mean=mean, stdev=stdev, cl=0.95)
                    # weighted by run age (weight halves every 12 hours)
                    weight_array = np.exp2(-run_age_array / (12 * 60 * 60))
                    sum_of_weights, w_mean, w_stdev = weighted_stats(wait_time_array, weight_array)
                    w_cl95upp = conf_interval_upper(n=sum_of_weights + 1, mean=w_mean, stdev=w_stdev, cl=0.95)
                    # current long queuing jobs
                    if w_mean:
                        q_status_list_str = "('activated', 'sent')"
                        varMap = {
                            ":computingSite": site,
                        }
                        (site_workflow,) = self.tbuf.querySQL(sql_get_site_workflow, varMap)[0]
                        if site_workflow and site_workflow.startswith("push"):
                            q_status_list_str = "('activated', 'sent', 'starting')"
                        varMap = {
                            ":computingSite": site,
                            ":w_mean": w_mean / (24 * 60 * 60),
                        }
                        sql_get_long_queuing_job_wait_time = sql_get_long_queuing_job_wait_time_template.format(q_status_list_str=q_status_list_str)
                        (long_q_n, long_q_mean_day) = self.tbuf.querySQL(sql_get_long_queuing_job_wait_time, varMap)[0]
                        if long_q_mean_day:
                            long_q_mean = long_q_mean_day * (24 * 60 * 60)
                        else:
                            long_q_mean = w_mean
                            long_q_n = 0
                # update
                site_dict[site].update(
                    {
                        "n": n_jobs,
                        "mean": mean,
                        "stdev": stdev,
                        "med": median,
                        "cl95upp": cl95upp,
                        "sum_of_weights": sum_of_weights,
                        "w_mean": w_mean,
                        "w_stdev": w_stdev,
                        "w_cl95upp": w_cl95upp,
                        "long_q_n": long_q_n,
                        "long_q_mean": long_q_mean,
                    }
                )
                # log
                tmp_log.debug(
                    (
                        "site={site}, n={n}, "
                        "mean={mean:.3f}, stdev={stdev:.3f}, med={med:.3f}, cl95upp={cl95upp:.3f}, "
                        "sum_of_weights={sum_of_weights:.3f}, "
                        "w_mean={w_mean:.3f}, w_stdev={w_stdev:.3f}, w_cl95upp={w_cl95upp:.3f}, "
                        "long_q_n={long_q_n}, long_q_mean={long_q_mean:.3f} "
                    ).format(site=site, **site_dict[site])
                )
                # turn nan into None
                for key in site_dict[site]:
                    if np.isnan(site_dict[site][key]):
                        site_dict[site][key] = None
            # return
            return site_dict
        except Exception:
            tmp_log.error(traceback.format_exc())

    def gshare_preference(self):
        tmp_log = logger_utils.make_logger(main_logger, "FetchData")
        try:
            # get share and hs info
            if self.gshare_status is None:
                self.gshare_status = self.tbuf.getGShareStatus()
            share_name_tree_dict = self.tbuf.get_tree_of_gshare_names()
            # initialize
            gshare_dict = dict()
            total_hs = sum([leaf["target"] for leaf in self.gshare_status])
            # rank and data
            for idx, leaf in enumerate(self.gshare_status):
                rank = idx + 1
                gshare = leaf["name"]
                gshare_dict[gshare] = {
                    "gshare": gshare,
                    "rank": rank,
                    "queuing_hs": leaf["queuing"],
                    "running_hs": leaf["running"],
                    "target_hs": leaf["target"],
                    "usage_perc": leaf["running"] / leaf["target"] if leaf["target"] > 0 else 999999,
                    "queue_perc": leaf["queuing"] / leaf["target"] if leaf["target"] > 0 else 999999,
                    "norm_usage_perc": leaf["running"] / total_hs if total_hs > 0 else 999999,
                    "norm_queue_perc": leaf["queuing"] / total_hs if total_hs > 0 else 999999,
                    "norm_target_perc": leaf["target"] / total_hs if total_hs > 0 else 999999,
                    "proj_target_hs": 0,
                    "eqiv_target_hs": 0,
                    "eqiv_usage_perc": 0,
                }
                tmp_log.debug("rank={rank}, gshare={gshare}, usage={usage_perc:.3%}, queue={queue_perc:.3%} ".format(**gshare_dict[gshare]))
            # add L1 share
            tmp_L1_leaves_map = {}
            l1_share_dict = {}
            for l1_share, val in share_name_tree_dict.items():
                tmp_L1_leaves_map.setdefault(l1_share, [])
                fill_leaf_shares(l1_share, val, tmp_L1_leaves_map[l1_share])
            for l1_share, leaves_list in tmp_L1_leaves_map.items():
                l1_share_name = f"L1 {l1_share}"
                l1_share_dict.setdefault(
                    l1_share_name,
                    {
                        "gshare": l1_share_name,
                        "rank": -1,
                        "queuing_hs": 0,
                        "running_hs": 0,
                        "target_hs": 0,
                        "usage_perc": 0,
                        "queue_perc": 0,
                    },
                )
                val_dict = l1_share_dict[l1_share_name]
                for leaf in leaves_list:
                    if leaf in gshare_dict:
                        for field in ["queuing_hs", "running_hs", "target_hs"]:
                            val_dict.setdefault(field, 0)
                            val_dict[field] += gshare_dict[leaf].get(field, 0)
                val_dict["usage_perc"] = val_dict["running_hs"] / val_dict["target_hs"] if val_dict["target_hs"] > 0 else 999999
                val_dict["queue_perc"] = val_dict["queuing_hs"] / val_dict["target_hs"] if val_dict["target_hs"] > 0 else 999999
                val_dict["norm_usage_perc"] = val_dict["running_hs"] / total_hs if total_hs > 0 else 999999
                val_dict["norm_queue_perc"] = val_dict["queuing_hs"] / total_hs if total_hs > 0 else 999999
                val_dict["norm_target_perc"] = val_dict["target_hs"] / total_hs
                val_dict["proj_target_hs"] = min(
                    val_dict["target_hs"],
                    max(val_dict["running_hs"], val_dict["queuing_hs"] / 2),
                )
            proj_total_hs = sum([v["proj_target_hs"] for v in l1_share_dict.values()])
            idle_total_hs = total_hs - proj_total_hs
            idle_l1_share_list = []
            idle_threshold_perc = 0.85
            for l1_share_name, val_dict in l1_share_dict.items():
                if val_dict["usage_perc"] < idle_threshold_perc and val_dict["proj_target_hs"] < val_dict["target_hs"]:
                    # idle l1 shares
                    idle_l1_share_list.append(l1_share_name)
            busy_total_hs = sum([v["target_hs"] for k, v in l1_share_dict.items() if k not in idle_l1_share_list]) + 1
            for l1_share_name, val_dict in l1_share_dict.items():
                if l1_share_name in idle_l1_share_list:
                    # idle l1 shares
                    val_dict["eqiv_target_hs"] = val_dict["target_hs"]
                    val_dict["eqiv_usage_perc"] = val_dict["usage_perc"]
                else:
                    # busy l1 shares
                    val_dict["eqiv_target_hs"] = val_dict["target_hs"] + idle_total_hs * val_dict["target_hs"] / busy_total_hs
                    val_dict["eqiv_usage_perc"] = val_dict["running_hs"] / val_dict["eqiv_target_hs"]
                tmp_log.debug(
                    (
                        "share={gshare}, usage={usage_perc:.3%}, queue={queue_perc:.3%}, "
                        "target_hs={target_hs:.0f}, eqiv_target_hs={eqiv_target_hs:.0f}, "
                        "norm_target_perc={norm_target_perc:.3%}, eqiv_usage_perc={eqiv_usage_perc:.3%}, "
                        "norm_usage={norm_usage_perc:.3%}, norm_queue={norm_queue_perc:.3%} "
                    ).format(**val_dict)
                )
            gshare_dict.update(l1_share_dict)
            # return
            return gshare_dict
        except Exception:
            tmp_log.error(traceback.format_exc())

    def analy_site_eval(self):
        tmp_log = logger_utils.make_logger(main_logger, "FetchData")
        try:
            # initialize
            site_dict = dict()
            class_A_set = set()
            class_B_set = set()
            class_C_set = set()
            # get resource_type of sites (GRID, hpc, cloud, ...) from schedconfig
            res = self.tbuf.querySQL(
                ("SELECT /* use_json_type */ scj.panda_queue, scj.data.resource_type " "FROM ATLAS_PANDA.schedconfig_json scj "),
                {},
            )
            site_resource_type_map = {site: resource_type for site, resource_type in res}
            # MetricsDB
            mdb = MetricsDB(self.tbuf)
            # get analysis jobs wait time stats
            apjwt_dict = mdb.get_metrics("analy_pmerge_jobs_wait_time", "site")
            # evaluate derived values from stats
            # max of w_cl95upp and long_q_mean for ranking. Only consider GRID sites
            ranking_wait_time_list = []
            for site, v in apjwt_dict.items():
                if site_resource_type_map.get(site) != "GRID":
                    continue
                try:
                    if v["w_cl95upp"] is not None and v["long_q_mean"] is not None:
                        ranking_wait_time = np.maximum(v["w_cl95upp"], v["long_q_mean"])
                        ranking_wait_time_list.append(ranking_wait_time)
                    else:
                        tmp_log.warning(
                            ("site={site} none value, skipped : w_cl95upp={w_cl95upp} long_q_mean={long_q_mean} ").format(
                                w_cl95upp=v["w_cl95upp"], long_q_mean=v["long_q_mean"]
                            )
                        )
                        continue
                except KeyError:
                    continue
            first_one_third_wait_time = np.nanquantile(np.array(ranking_wait_time_list), 0.333)
            last_one_third_wait_time = np.nanquantile(np.array(ranking_wait_time_list), 0.667)
            tmp_log.debug(f"GRID n_sites= {len(ranking_wait_time_list)} wait time PR33={first_one_third_wait_time:.3f} PR67={last_one_third_wait_time:.3f}")
            # get to-running rate of sites
            tmp_st, site_6h_strr_map = get_site_strr_stats(self.tbuf, time_window=60 * 60 * 6)
            if not tmp_st:
                tmp_log.error("failed to get 6h-slot-to-running-rate")
            tmp_st, site_1d_strr_map = get_site_strr_stats(self.tbuf, time_window=60 * 60 * 24)
            if not tmp_st:
                tmp_log.error("failed to get 1d-slot-to-running-rate")
            # for each site
            for site in apjwt_dict:
                # from wait time stats
                # TODO: to consider failure rate, site fullness, etc.
                v = apjwt_dict[site]
                # evaluate derived values
                try:
                    if v["w_cl95upp"] is not None and v["long_q_mean"] is not None:
                        v["ranking_wait_time"] = np.maximum(v["w_cl95upp"], v["long_q_mean"])
                        # v['is_slowing_down'] = (v['long_q_mean'] > v['w_cl95upp'] and v['long_q_n'] >= 3)
                    else:
                        tmp_log.warning(f"site={site} none value, skipped : w_cl95upp={v['w_cl95upp']} long_q_mean={v['long_q_mean']} ")
                        continue
                except KeyError as e:
                    tmp_log.warning(f"site={site} misses value, skipped : {e} ")
                    continue
                # initialize
                site_dict[site] = dict()
                # resource type of the site
                site_dict[site]["resource_type"] = site_resource_type_map.get(site)
                # to-running rate
                site_6h_strr = site_6h_strr_map.get(site, 0)
                site_1d_strr = site_1d_strr_map.get(site, 0)
                site_dict[site]["strr_6h"] = site_6h_strr
                site_dict[site]["strr_1d"] = site_1d_strr
                # classify
                if (
                    v["ranking_wait_time"] <= max(first_one_third_wait_time, 3600)
                    or (v["w_cl95upp"] <= max(first_one_third_wait_time, 3600) and v["long_q_n"] <= 3)
                ) and site_1d_strr > 0:
                    # class A (1)
                    site_dict[site]["class"] = 1
                    class_A_set.add(site)
                elif v["ranking_wait_time"] > max(last_one_third_wait_time, 10800):
                    # class C (-1)
                    site_dict[site]["class"] = -1
                    class_C_set.add(site)
                else:
                    # class B (0)
                    site_dict[site]["class"] = 0
                    class_B_set.add(site)
                # log
                tmp_log.debug(
                    ("site={site}, class={class}, type={resource_type}, strr_6h={strr_6h:.3f}, strr_1d={strr_1d:.3f} ").format(site=site, **site_dict[site])
                )
                # turn nan into None
                for key in site_dict[site]:
                    _val = site_dict[site][key]
                    if not isinstance(_val, str) and np.isnan(_val):
                        site_dict[site][key] = None
            # log
            tmp_log.debug(
                ("class_A ({}) : {} ; class_B ({}) : {} ; class_C ({}) : {}").format(
                    len(class_A_set),
                    ",".join(sorted(list(class_A_set))),
                    len(class_B_set),
                    ",".join(sorted(list(class_B_set))),
                    len(class_C_set),
                    ",".join(sorted(list(class_C_set))),
                )
            )
            # return
            return site_dict
        except Exception:
            tmp_log.error(traceback.format_exc())

    def users_jobs_stats(self):
        prod_source_label = "user"
        tmp_log = logger_utils.make_logger(main_logger, "FetchData")
        tmp_log.debug("start")
        try:
            # initialize
            site_gshare_dict = dict()
            # get users jobs stats
            jobsStatsPerUser = {}
            varMap = {}
            varMap[":prodSourceLabel"] = prod_source_label
            varMap[":pmerge"] = "pmerge"
            varMap[":gangarbt"] = "gangarbt"
            sqlJ = (
                "SELECT COUNT(*),SUM(coreCount),prodUserName,jobStatus,gshare,computingSite "
                "FROM ATLAS_PANDA.jobsActive4 "
                "WHERE prodSourceLabel=:prodSourceLabel "
                "AND processingType<>:pmerge "
                "AND prodUserName<>:gangarbt "
                "GROUP BY prodUserName,jobStatus,gshare,computingSite "
            )
            # result
            res = self.tbuf.querySQL(sqlJ, varMap)
            if res is None:
                tmp_log.debug(f"got {res} ")
            else:
                tmp_log.debug(f"total {len(res)} ")
                # make map
                for cnt, n_slots, prodUserName, jobStatus, gshare, computingSite in res:
                    # append to PerUser map
                    jobsStatsPerUser.setdefault(computingSite, {})
                    jobsStatsPerUser[computingSite].setdefault(gshare, {})
                    jobsStatsPerUser[computingSite][gshare].setdefault(
                        prodUserName,
                        {
                            "nDefined": 0,
                            "nAssigned": 0,
                            "nActivated": 0,
                            "nStarting": 0,
                            "nQueue": 0,
                            "nRunning": 0,
                            "slotsDefined": 0,
                            "slotsAssigned": 0,
                            "slotsActivated": 0,
                            "slotsStarting": 0,
                            "slotsQueue": 0,
                            "slotsRunning": 0,
                        },
                    )
                    jobsStatsPerUser[computingSite][gshare].setdefault(
                        "_total",
                        {
                            "nDefined": 0,
                            "nAssigned": 0,
                            "nActivated": 0,
                            "nStarting": 0,
                            "nQueue": 0,
                            "nRunning": 0,
                            "slotsDefined": 0,
                            "slotsAssigned": 0,
                            "slotsActivated": 0,
                            "slotsStarting": 0,
                            "slotsQueue": 0,
                            "slotsRunning": 0,
                        },
                    )
                    # count # of running/done and activated
                    if jobStatus in ["defined", "assigned", "activated", "starting"]:
                        status_name = f"n{jobStatus.capitalize()}"
                        slots_status_name = f"slots{jobStatus.capitalize()}"
                        jobsStatsPerUser[computingSite][gshare][prodUserName][status_name] += cnt
                        jobsStatsPerUser[computingSite][gshare][prodUserName]["nQueue"] += cnt
                        jobsStatsPerUser[computingSite][gshare]["_total"][status_name] += cnt
                        jobsStatsPerUser[computingSite][gshare]["_total"]["nQueue"] += cnt
                        jobsStatsPerUser[computingSite][gshare][prodUserName][slots_status_name] += n_slots
                        jobsStatsPerUser[computingSite][gshare][prodUserName]["slotsQueue"] += n_slots
                        jobsStatsPerUser[computingSite][gshare]["_total"][slots_status_name] += n_slots
                        jobsStatsPerUser[computingSite][gshare]["_total"]["slotsQueue"] += n_slots
                    elif jobStatus in ["running"]:
                        jobsStatsPerUser[computingSite][gshare][prodUserName]["nRunning"] += cnt
                        jobsStatsPerUser[computingSite][gshare]["_total"]["nRunning"] += cnt
                        jobsStatsPerUser[computingSite][gshare][prodUserName]["slotsRunning"] += n_slots
                        jobsStatsPerUser[computingSite][gshare]["_total"]["slotsRunning"] += n_slots
            # fill
            for computingSite in jobsStatsPerUser:
                g_dict = jobsStatsPerUser[computingSite]
                for gshare in g_dict:
                    data_dict = g_dict[gshare]
                    site_gshare_dict[(computingSite, gshare)] = data_dict
                    tmp_log.debug(f"site={computingSite}, gshare={gshare}, stats={data_dict}")
            # done
            tmp_log.debug("done")
            return site_gshare_dict
        except Exception:
            tmp_log.error(traceback.format_exc())

    def analy_user_eval(self):
        tmp_log = logger_utils.make_logger(main_logger, "FetchData")
        try:
            # initialize
            user_dict = dict()
            # MetricsDB
            mdb = MetricsDB(self.tbuf)
            # get analysis site classification evalutation
            ase_dict = mdb.get_metrics("analy_site_eval", "site", fresher_than_minutes_ago=120)
            # get users jobs stats
            ujs_dict = mdb.get_metrics("users_jobs_stats", fresher_than_minutes_ago=15)
            # for each site x gshare
            for (site, gshare), usage_dict in ujs_dict.items():
                # count only User Analysis & Express Analysis
                if gshare not in ["User Analysis", "Express Analysis"]:
                    continue
                # get site evaluation data
                try:
                    site_eval_dict = ase_dict[site]
                except KeyError:
                    tmp_log.warning(f"analy_site_eval missed site={site} gshare={gshare}, skipped ")
                    continue
                # get site class value
                try:
                    site_class_value = site_eval_dict["class"]
                except KeyError:
                    tmp_log.warning(f"analy_site_eval class missed for site={site} gshare={gshare}, skipped ")
                    continue
                else:
                    site_class_rank = class_value_rank_map[site_class_value]
                # site resource_type
                site_resource_type = site_eval_dict.get("resource_type")
                # skip non-GRID sites
                if site_resource_type != "GRID":
                    continue
                # evaluate usage of GRID sites for each user
                for user, v in usage_dict.items():
                    # initialize
                    user_dict.setdefault(user, {"user": user})
                    for _rank in class_value_rank_map.values():
                        user_dict[user].setdefault(
                            _rank,
                            {
                                "nQueue": 0,
                                "nRunning": 0,
                                "slotsQueue": 0,
                                "slotsRunning": 0,
                            },
                        )
                    # fill nQ & nR at each site class of each user
                    user_dict[user][site_class_rank]["nQueue"] += v["nQueue"]
                    user_dict[user][site_class_rank]["nRunning"] += v["nRunning"]
                    user_dict[user][site_class_rank]["slotsQueue"] += v["slotsQueue"]
                    user_dict[user][site_class_rank]["slotsRunning"] += v["slotsRunning"]
            # evaluate derived values for each user
            threshold_A = self.tbuf.getConfigValue("analy_eval", "USER_USAGE_THRESHOLD_A")
            if threshold_A is None:
                threshold_A = 1000
            threshold_B = self.tbuf.getConfigValue("analy_eval", "USER_USAGE_THRESHOLD_B")
            if threshold_B is None:
                threshold_B = 8000
            for user, d in copy.deepcopy(user_dict).items():
                run_slots_A = d["A_sites"]["slotsRunning"]
                run_slots_AB = d["A_sites"]["slotsRunning"] + d["B_sites"]["slotsRunning"]
                # remaining allowed slots
                rem_slots_A = max(threshold_A - run_slots_A, 0)
                rem_slots_B = max(threshold_B - run_slots_AB, 0)
                user_dict[user]["rem_slots_A"] = rem_slots_A
                user_dict[user]["rem_slots_B"] = rem_slots_B
                user_dict[user]["rem_slot_ratio_A"] = rem_slots_A / threshold_A
                user_dict[user]["rem_slot_ratio_B"] = rem_slots_B / threshold_B
            # log
            tmp_log.debug(f"{user_dict}")
            # return
            return user_dict
        except Exception:
            tmp_log.error(traceback.format_exc())


# main
def main(tbuf=None, **kwargs):
    requester_id = GenericThread().get_full_id(__name__, sys.modules[__name__].__file__)

    # instantiate TB
    if tbuf is None:
        from pandaserver.taskbuffer.TaskBuffer import taskBuffer

        taskBuffer.init(
            panda_config.dbhost,
            panda_config.dbpasswd,
            nDBConnection=1,
            useTimeout=True,
            requester=requester_id,
        )
    else:
        taskBuffer = tbuf
    # pid
    my_pid = os.getpid()
    my_full_pid = f"{socket.getfqdn().split('.')[0]}-{os.getpgrp()}-{my_pid}"
    # go
    if DRY_RUN:
        # dry run, regardless of lock, not update DB
        fetcher = FetchData(taskBuffer)
        # loop over all fetch data methods to run and update to DB
        for metric_name, key_type, period in metric_list:
            main_logger.debug(f"(dry-run) start {metric_name}")
            # fetch data and update DB
            the_method = getattr(fetcher, metric_name)
            fetched_data = the_method()
            if fetched_data is None:
                main_logger.warning(f"(dry-run) {metric_name} got no valid data")
                continue
            main_logger.debug(f"(dry-run) done {metric_name}")
    else:
        # real run, will update DB
        # instantiate
        mdb = MetricsDB(taskBuffer)
        fetcher = FetchData(taskBuffer)
        # loop over all fetch data methods to run and update to DB
        for metric_name, key_type, period in metric_list:
            # metric lock
            lock_component_name = f"pandaMetr.{metric_name:.16}.{adler32(metric_name.encode('utf-8')):0x}"
            # try to get lock
            got_lock = taskBuffer.lockProcess_PANDA(component=lock_component_name, pid=my_full_pid, time_limit=period)
            if got_lock:
                main_logger.debug(f"got lock of {metric_name}")
            else:
                main_logger.debug(f"{metric_name} locked by other process; skipped...")
                continue
            main_logger.debug(f"start {metric_name}")
            # fetch data and update DB
            the_method = getattr(fetcher, metric_name)
            fetched_data = the_method()
            if fetched_data is None:
                main_logger.warning(f"{metric_name} got no valid data")
                continue
            mdb.update(metric=metric_name, key_type=key_type, entity_dict=fetched_data)
            main_logger.debug(f"done {metric_name}")
    # stop taskBuffer if created inside this script
    if tbuf is None:
        taskBuffer.cleanup(requester=requester_id)


# run
if __name__ == "__main__":
    main()
