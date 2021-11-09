import os
import socket
import time
import datetime
import json
import functools
import traceback
import copy
import statistics

from zlib import adler32

from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandalogger import logger_utils

from pandaserver.config import panda_config


# logger
main_logger = PandaLogger().getLogger('metric_collector')


# list of metrics in FetchData to fetch data and update to DB. Format: (metric, type, period_sec)
metric_list = [
    ('analy_pmerge_jobs_wait_time', 'site', 1800),
]


def get_now_time_str():
    """
    Return string of nowtime that can be stored in DB
    """
    now_time = datetime.datetime.utcnow()
    ts_str = now_time.strftime('%Y-%m-%d %H:%M:%S')
    return ts_str


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

    def update(self, key, value, site, gshare):
        tmp_log = logger_utils.make_logger(main_logger, 'MetricsDB')
        # tmp_log.debug('start key={0} site={1}, gshare={2}'.format(key, site, gshare))
        # sql
        sql_update_tmp = (
            """UPDATE ATLAS_PANDA.Metrics SET """
                """data_json = json_mergepatch(data_json, '{patch_data_json}') """
            """WHERE computingSite=:site AND gshare=:gshare """
        )
        sql_insert_tmp = (
            """INSERT INTO ATLAS_PANDA.Metrics """
                """VALUES ( """
                    """:site, :gshare, '{patch_data_json}' """
                """) """
        )
        # var map
        varMap = {
            ':site': site,
            ':gshare': gshare,
        }
        # json string evaluated
        try:
            now_time_str = get_now_time_str()
            patch_data_dict = dict()
            patch_data_dict[key] = {
                "value": value,
                "timestamp": now_time_str,
            }
            patch_data_json = json.dumps(patch_data_dict)
        except Exception:
            tmp_log.error(traceback.format_exc())
            return
        # json in sql
        sql_update = sql_update_tmp.format(patch_data_json=patch_data_json)
        sql_insert = sql_insert_tmp.format(patch_data_json=patch_data_json)
        # update
        n_row = self.tbuf.querySQL(sql_update, varMap)
        # try insert if no row updated
        if n_row == 0:
            try:
                tmp_log.debug('no row to update about site={site}, gshare={gshare} ; trying insert'.format(site=site, gshare=gshare))
                self.tbuf.querySQL(sql_insert, varMap)
                tmp_log.debug('inserted site={site}, gshare={gshare}'.format(site=site, gshare=gshare))
            except Exception:
                tmp_log.warning('failed to insert site={site}, gshare={gshare}'.format(site=site, gshare=gshare))
        else:
            tmp_log.debug('updated site={site}, gshare={gshare}'.format(site=site, gshare=gshare))
        # done
        # tmp_log.debug('done key={0} site={1}, gshare={2}'.format(key, site, gshare))

    def update_site(self, key, value, site):
        return self.update(key, value, site=site, gshare='NULL')

    def update_gshare(self, key, value, gshare):
        return self.update(key, value, site='NULL', gshare=gshare)


class FetchData(object):
    """
    methods to fetch or evaluate data values to store
    """

    def __init__(self, tbuf):
        self.tbuf = tbuf

    def analy_pmerge_jobs_wait_time(self):
        tmp_log = logger_utils.make_logger(main_logger, 'FetchData')
        #sql
        sql_get_jobs = (
            "SELECT pandaID, computingSite "
            "FROM ATLAS_PANDA.jobsArchived4 "
            "WHERE prodSourceLabel='user' "
                "AND jobStatus='finished' "
                "AND processingType='pmerge' "
        )
        sql_get_latest_job_mtime_status = (
            "SELECT jobStatus, MIN(modificationTime) "
            "FROM ATLAS_PANDA.jobs_StatusLog "
            "WHERE pandaID=:pandaID "
            "GROUP BY jobStatus "
        )
        try:
            # initialize
            tmp_site_dict = dict()
            # get user jobs
            jobs_list = self.tbuf.querySQL(sql_get_jobs, {})
            n_tot_jobs = len(jobs_list)
            tmp_log.debug('got total {0} jobs'.format(n_tot_jobs))
            # loop over jobs to get modificationTime when activated and running
            cc = 0
            for pandaID, site in jobs_list:
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
                tmp_site_dict.setdefault(site, [])
                tmp_site_dict[site].append(wait_time_sec)
                # log message
                if cc > 0 and cc % 5000 == 0:
                    tmp_log.debug('... queried {0:9d} jobs ...'.format(cc))
                cc += 1
            tmp_log.debug('queried {0} jobs'.format(cc))
            # evaluate stats
            site_dict = dict()
            for site, data_list in tmp_site_dict.items():
                site_dict.setdefault(site, {})
                n_jobs = len(data_list)
                try:
                    mean = statistics.mean(data_list)
                except statistics.StatisticsError:
                    mean = None
                try:
                    stdev = statistics.stdev(data_list)
                except statistics.StatisticsError:
                    stdev = None
                try:
                    stdev = statistics.stdev(data_list)
                except statistics.StatisticsError:
                    stdev = None
                try:
                    median = statistics.median(data_list)
                except statistics.StatisticsError:
                    median = median
                # try:
                #     quantiles = statistics.quantiles(data_list, n=4, method='inclusive')
                # except statistics.StatisticsError:
                #     quantiles = None
                # update
                site_dict[site].update({
                        'n': n_jobs,
                        'mean': mean,
                        'stdev': stdev,
                        'med': median,
                        # 'quantiles': quantiles,
                    })
                tmp_log.debug('site={site}, n={n}, mean={mean:.3f}, stdev={stdev:.3f}, med={med:.3f}'.format(site=site, **site_dict[site]))
            # return
            return site_dict
        except Exception:
            tmp_log.error(traceback.format_exc())

    def gshare_preference(self):
        tmp_log = logger_utils.make_logger(main_logger, 'FetchData')
        # sql
        sql_get_jobs = (
            "SELECT pandaID, computingSite "
            "FROM ATLAS_PANDA.jobsArchived4 "
            "WHERE prodSourceLabel='user' "
                "AND jobStatus='finished' "
                "AND processingType='pmerge' "
        )
        sql_get_latest_job_mtime_status = (
            "SELECT jobStatus, MIN(modificationTime) "
            "FROM ATLAS_PANDA.jobs_StatusLog "
            "WHERE pandaID=:pandaID "
            "GROUP BY jobStatus "
        )
        try:
            # initialize
            tmp_site_dict = dict()
            # get user jobs
            jobs_list = self.tbuf.querySQL(sql_get_jobs, {})
            n_tot_jobs = len(jobs_list)
            tmp_log.debug('got total {0} jobs'.format(n_tot_jobs))
            # loop over jobs to get modificationTime when activated and running
            cc = 0
            for pandaID, site in jobs_list:
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
                tmp_site_dict.setdefault(site, [])
                tmp_site_dict[site].append(wait_time_sec)
                # log message
                if cc > 0 and cc % 5000 == 0:
                    tmp_log.debug('... queried {0:9d} jobs ...'.format(cc))
                cc += 1
            tmp_log.debug('queried {0} jobs'.format(cc))
            # evaluate stats
            site_dict = dict()
            for site, data_list in tmp_site_dict.items():
                site_dict.setdefault(site, {})
                n_jobs = len(data_list)
                try:
                    mean = statistics.mean(data_list)
                except statistics.StatisticsError:
                    mean = None
                try:
                    stdev = statistics.stdev(data_list)
                except statistics.StatisticsError:
                    stdev = None
                try:
                    stdev = statistics.stdev(data_list)
                except statistics.StatisticsError:
                    stdev = None
                # try:
                #     quantiles = statistics.quantiles(data_list, n=4, method='inclusive')
                # except statistics.StatisticsError:
                #     quantiles = None
                # update
                site_dict[site].update({
                        'n': n_jobs,
                        'mean': mean,
                        'stdev': stdev,
                        # 'quantiles': quantiles,
                    })
                tmp_log.debug('site={site}, n={n}, mean={mean:.3f}, stdev={stdev:.3f}'.format(site=site, **site_dict[site]))
            # return
            return site_dict
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
        if update_type == 'site':
            for site, v in fetched_data.items():
                mdb.update_site(key=metric_name, value=v, site=site)
        elif update_type == 'gshare':
            for gshare, v in fetched_data.items():
                mdb.update_gshare(key=metric_name, value=v, gshare=gshare)
        elif update_type == 'both':
            for (site, gshare), v in fetched_data.items():
                mdb.update(key=metric_name, value=v, site=site, gshare=gshare)
        main_logger.debug('done {metric_name}'.format(metric_name=metric_name))

# run
if __name__ == '__main__':
    main()
