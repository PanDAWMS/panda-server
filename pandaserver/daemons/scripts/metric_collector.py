import os
import socket
import time
import datetime
import json
import functools
import traceback
import copy

import numpy as np

from zlib import adler32

from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandalogger import logger_utils

from pandaserver.config import panda_config

from scipy import stats


# logger
main_logger = PandaLogger().getLogger('metric_collector')

# dry run
DRY_RUN = False

# list of metrics in FetchData to fetch data and update to DB. Format: (metric, type, period_minutes)
metric_list = [
    ('gshare_preference', 'gshare', 20),
    ('analy_pmerge_jobs_wait_time', 'site', 30),
]


def get_now_time_str():
    """
    Return string of nowtime that can be stored in DB
    """
    now_time = datetime.datetime.utcnow()
    ts_str = now_time.strftime('%Y-%m-%d %H:%M:%S')
    return ts_str


def conf_interval_upper(n, mean, stdev, cl=0.95):
    """
    Get estimated confidence level
    """
    max_value = 999999
    ciu = stats.t.ppf(cl, (n-1), loc=mean, scale=stdev)
    ciu = min(ciu, max_value)
    return ciu


def weighted_stats(values, weights):
    """
    Return sum of weights, weighted mean and standard deviation
    """
    sum_of_weights = np.sum(weights)
    mean = np.average(values, weights=weights)
    variance = np.average((values - mean)**2, weights=weights)
    stdev = np.sqrt(variance)
    return sum_of_weights, mean, stdev


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

    def update(self, metric, update_type, entity_dict):
        tmp_log = logger_utils.make_logger(main_logger, 'MetricsDB')
        # tmp_log.debug('start key={0} site={1}, gshare={2}'.format(key, site, gshare))
        # sql
        sql_update = (
            """UPDATE ATLAS_PANDA.Metrics SET """
                """value_json = json_mergepatch(value_json, :patch_value_json), """
                """timestamp = :timestamp """
            """WHERE computingSite=:site AND gshare=:gshare AND metric = :metric """
        )
        sql_insert = (
            """INSERT INTO ATLAS_PANDA.Metrics """
                """VALUES ( """
                    """:site, :gshare, :metric, :patch_value_json, :timestamp """
                """) """
        )
        # now
        now_time = datetime.datetime.utcnow()
        # var map template
        varMap_template = {
            ':site': None,
            ':gshare': None,
            ':metric': metric,
            ':timestamp': now_time,
            ':patch_value_json': None
        }
        # make var map list
        varMap_list = []
        for entity, v in entity_dict.items():
            # values to json string
            try:
                patch_value_json = json.dumps(v)
            except Exception:
                tmp_log.error(traceback.format_exc() + ' ' + str(v))
                return
            # initialize varMap
            varMap = varMap_template.copy()
            varMap[':patch_value_json'] = patch_value_json
            # update varMap according to update_type
            if update_type == 'site':
                varMap.update({
                        ':site': entity,
                        ':gshare': 'NULL',
                    })
            elif update_type == 'gshare':
                varMap.update({
                        ':site': 'NULL',
                        ':gshare': entity,
                    })
            elif update_type == 'both':
                varMap.update({
                        ':site': entity[0],
                        ':gshare': entity[1],
                    })
            # append to the list
            varMap_list.append(varMap)
        # update
        n_row = self.tbuf.executemanySQL(sql_update, varMap_list)
        # try insert if no row updated
        if n_row == 0:
            try:
                tmp_log.debug('no row to update for metric={metric} ; trying insert'.format(metric=metric))
                self.tbuf.executemanySQL(sql_insert, varMap_list)
                tmp_log.debug('inserted for metric={metric}'.format(metric=metric))
            except Exception:
                tmp_log.warning('failed to insert for metric={metric}'.format(metric=metric))
        else:
            tmp_log.debug('updated for metric={metric}'.format(metric=metric))
        # done
        # tmp_log.debug('done key={0} site={1}, gshare={2}'.format(key, site, gshare))


class FetchData(object):
    """
    methods to fetch or evaluate data values to store
    """

    def __init__(self, tbuf):
        self.tbuf = tbuf
        # initialize stored data
        self.gshare_status = None

    def analy_pmerge_jobs_wait_time(self):
        tmp_log = logger_utils.make_logger(main_logger, 'FetchData')
        #sql
        sql_get_jobs_archived4 = (
            "SELECT pandaID, computingSite "
            "FROM ATLAS_PANDA.jobsArchived4 "
            "WHERE prodSourceLabel='user' "
                "AND gshare='User Analysis' "
                "AND processingType='pmerge' "
                "AND modificationTime>:modificationTime "
        )
        sql_get_jobs_active4 = (
            "SELECT pandaID, computingSite "
            "FROM ATLAS_PANDA.jobsActive4 "
            "WHERE prodSourceLabel='user' "
                "AND gshare='User Analysis' "
                "AND jobStatus IN ('running', 'holding', 'merging', 'transferring', 'finished', 'failed', 'closed', 'cancelled') "
                "AND processingType='pmerge' "
                "AND modificationTime>:modificationTime "
        )
        sql_get_latest_job_mtime_status = (
            "SELECT jobStatus, MIN(modificationTime) "
            "FROM ATLAS_PANDA.jobs_StatusLog "
            "WHERE pandaID=:pandaID "
            "GROUP BY jobStatus "
        )
        sql_get_long_queuing_job_wait_time = (
            "SELECT COUNT(*), AVG(CURRENT_DATE-creationtime) "
            "FROM ATLAS_PANDA.jobsActive4 "
            "WHERE prodSourceLabel='user' "
                "AND gshare='User Analysis' "
                "AND jobStatus IN ('activated', 'sent', 'starting') "
                "AND processingType='pmerge' "
                "AND computingSite=:computingSite "
                "AND (CURRENT_DATE-creationtime)>:w_mean "
        )
        try:
            # initialize
            tmp_site_dict = dict()
            # now time
            now_time = datetime.datetime.utcnow()
            # get user jobs
            varMap = {
                    ':modificationTime': now_time - datetime.timedelta(days=4),
                }
            archived4_jobs_list = self.tbuf.querySQL(sql_get_jobs_archived4, varMap)
            active4_jobs_list = self.tbuf.querySQL(sql_get_jobs_active4, varMap)
            all_jobs_set = set()
            all_jobs_set.update(archived4_jobs_list)
            all_jobs_set.update(active4_jobs_list)
            n_tot_jobs = len(all_jobs_set)
            tmp_log.debug('got total {0} jobs'.format(n_tot_jobs))
            # loop over jobs to get modificationTime when activated and running
            cc = 0
            for pandaID, site in all_jobs_set:
                if not site:
                    continue
                varMap = {':pandaID': pandaID}
                status_mtime_list = self.tbuf.querySQL(sql_get_latest_job_mtime_status, varMap)
                status_mtime_dict = dict(status_mtime_list)
                if 'activated' not in status_mtime_dict or 'running' not in status_mtime_dict:
                    continue
                wait_time = status_mtime_dict['running'] - status_mtime_dict['activated']
                wait_time_sec = wait_time.total_seconds()
                if wait_time_sec < 0:
                    tmp_log.warning('job {0} has negative wait time'.format(pandaID))
                    continue
                run_age_sec = int((now_time - status_mtime_dict['running']).total_seconds())
                if run_age_sec < 0:
                    tmp_log.warning('job {0} has negative run age'.format(pandaID))
                    continue
                tmp_site_dict.setdefault(site, {'wait_time': [], 'run_age': []})
                tmp_site_dict[site]['wait_time'].append(wait_time_sec)
                tmp_site_dict[site]['run_age'].append(run_age_sec)
                # log message
                if cc > 0 and cc % 5000 == 0:
                    tmp_log.debug('... queried {0:9d} jobs ...'.format(cc))
                cc += 1
            tmp_log.debug('queried {0} jobs'.format(cc))
            # evaluate stats
            site_dict = dict()
            for site, data_dict in tmp_site_dict.items():
                site_dict.setdefault(site, {})
                n_jobs = len(data_dict['wait_time'])
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
                    wait_time_array = np.array(data_dict['wait_time'])
                    run_age_array = np.array(data_dict['run_age'])
                    # stats
                    mean = np.mean(wait_time_array)
                    stdev = np.std(wait_time_array)
                    median = np.median(wait_time_array)
                    # try:
                    #     quantiles = statistics.quantiles(data_list, n=4, method='inclusive')
                    # except statistics.StatisticsError:
                    #     quantiles = None
                    cl95upp = conf_interval_upper(n=n_jobs, mean=mean, stdev=stdev, cl=0.95)
                    # weighted by run age (weight halves every 12 hours)
                    weight_array = np.exp2(-run_age_array/(12*60*60))
                    sum_of_weights, w_mean, w_stdev = weighted_stats(wait_time_array, weight_array)
                    w_cl95upp = conf_interval_upper(n=sum_of_weights+1, mean=w_mean, stdev=w_stdev, cl=0.95)
                    # current long queuing jobs
                    if w_mean:
                        varMap = {
                                ':computingSite': site,
                                ':w_mean': w_mean/(24*60*60),
                            }
                        (long_q_n, long_q_mean_day) = self.tbuf.querySQL(sql_get_long_queuing_job_wait_time, varMap)[0]
                        if long_q_mean_day:
                            long_q_mean = long_q_mean_day*(24*60*60)
                        else:
                            long_q_mean = w_mean
                            long_q_n = 0
                # update
                site_dict[site].update({
                        'n': n_jobs,
                        'mean': mean,
                        'stdev': stdev,
                        'med': median,
                        # 'quantiles': quantiles,
                        'cl95upp': cl95upp,
                        'sum_of_weights': sum_of_weights,
                        'w_mean': w_mean,
                        'w_stdev': w_stdev,
                        'w_cl95upp': w_cl95upp,
                        'long_q_n': long_q_n,
                        'long_q_mean': long_q_mean,
                    })
                # log
                tmp_log.debug(('site={site}, n={n}, '
                                'mean={mean:.3f}, stdev={stdev:.3f}, med={med:.3f}, cl95upp={cl95upp:.3f}, '
                                'sum_of_weights={sum_of_weights:.3f}, '
                                'w_mean={w_mean:.3f}, w_stdev={w_stdev:.3f}, w_cl95upp={w_cl95upp:.3f}, '
                                'long_q_n={long_q_n}, long_q_mean={long_q_mean:.3f} '
                                ).format(site=site, **site_dict[site]))
                # turn nan into None
                for key in site_dict[site]:
                    if np.isnan(site_dict[site][key]):
                        site_dict[site][key] = None
            # return
            return site_dict
        except Exception:
            tmp_log.error(traceback.format_exc())

    def gshare_preference(self):
        tmp_log = logger_utils.make_logger(main_logger, 'FetchData')
        try:
            # get share and hs info
            if self.gshare_status is None:
                self.gshare_status = self.tbuf.getGShareStatus()
            # initialize
            gshare_dict = dict()
            # rank and data
            for idx, leaf in enumerate(self.gshare_status):
                rank = idx + 1
                gshare = leaf['name']
                gshare_dict[gshare] = {
                    'rank': rank,
                    'running_hs': leaf['running'],
                    'target_hs': leaf['target'],
                }
                tmp_log.debug('rank={rank}, gshare={gshare}'.format(gshare=gshare, **gshare_dict[gshare]))
            # return
            return gshare_dict
        except Exception:
            tmp_log.error(traceback.format_exc())


# main
def main(tbuf=None, **kwargs):
    # instantiate TB
    if tbuf is None:
        from pandaserver.taskbuffer.TaskBuffer import taskBuffer
        taskBuffer.init(panda_config.dbhost, panda_config.dbpasswd, nDBConnection=1)
    else:
        taskBuffer = tbuf
    # pid
    my_pid = os.getpid()
    my_full_pid = '{0}-{1}-{2}'.format(socket.getfqdn().split('.')[0], os.getpgrp(), my_pid)
    # go
    if DRY_RUN:
        # dry run, regardless of lock, not update DB
        fetcher = FetchData(taskBuffer)
        # loop over all fetch data methods to run and update to DB
        for metric_name, update_type, period in metric_list:
            main_logger.debug('(dry-run) start {metric_name}'.format(metric_name=metric_name))
            # fetch data and update DB
            the_method = getattr(fetcher, metric_name)
            fetched_data = the_method()
            if fetched_data is None:
                main_logger.warning('(dry-run) {metric_name} got no valid data'.format(metric_name=metric_name))
                continue
            main_logger.debug('(dry-run) done {metric_name}'.format(metric_name=metric_name))
    else:
        # real run, will update DB
        # instantiate
        mdb = MetricsDB(taskBuffer)
        fetcher = FetchData(taskBuffer)
        # loop over all fetch data methods to run and update to DB
        for metric_name, update_type, period in metric_list:
            # metric lock
            lock_component_name = 'pandaMetr.{0:.30}.{1:0x}'.format(metric_name, adler32(metric_name.encode('utf-8')))
            # try to get lock
            got_lock = taskBuffer.lockProcess_PANDA(component=lock_component_name, pid=my_full_pid, time_limit=period)
            if got_lock:
                main_logger.debug('got lock of {metric_name}'.format(metric_name=metric_name))
            else:
                main_logger.debug('{metric_name} locked by other process; skipped...'.format(metric_name=metric_name))
                continue
            main_logger.debug('start {metric_name}'.format(metric_name=metric_name))
            # fetch data and update DB
            the_method = getattr(fetcher, metric_name)
            fetched_data = the_method()
            if fetched_data is None:
                main_logger.warning('{metric_name} got no valid data'.format(metric_name=metric_name))
                continue
            mdb.update(metric=metric_name, update_type=update_type, entity_dict=fetched_data)
            main_logger.debug('done {metric_name}'.format(metric_name=metric_name))

# run
if __name__ == '__main__':
    main()
