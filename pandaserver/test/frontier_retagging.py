"""
Checks for tasks that are having frontier failures from MWT2 Elastic Search repository. Elastic Search queries
based on Ilija Vukotic scripts here: https://github.com/ATLAS-Analytics/AlarmAndAlertService/blob/master/frontier-failed-q.py

These tasks are getting reassigned to a separate, throttled global-share with Frontier heavy tasks. Exceptions that
will not be reassigned are:
 - jobs that don't belong to any task (usually HammerCloud test jobs)
 - analysis tasks, since there has not been any unification yet
"""

import datetime
import sys


from elasticsearch import Elasticsearch

from pandalogger.PandaLogger import PandaLogger
_logger = PandaLogger().getLogger('frontier_retagging')

from config import panda_config
from taskbuffer.TaskBuffer import taskBuffer
taskBuffer.init(panda_config.dbhost, panda_config.dbpasswd, nDBConnection=1)

def get_frontier_failure_count_by_task():
    """
    retrieve failure count by task from Elastic Search
    """
    es_host = 'atlas-kibana.mwt2.org'
    es_port = 9200
    es_index = 'frontier'
    # es_index = 'frontier-%d-%02d' % (ct.year, ct.month)

    # prepare time window for query
    n_hours = 1
    ct = datetime.datetime.utcnow()
    st = ct - datetime.timedelta(hours=n_hours)
    current_time = ct.strftime('%Y%m%dT%H%M%S.%f')[:-3] + 'Z'
    start_time = st.strftime('%Y%m%dT%H%M%S.%f')[:-3] + 'Z'
    _logger.debug('start time: {0} - end time: {1}'.format(start_time, current_time))

    es_query = {
        "size": 0,
        "query": {"range": {"@timestamp": {"gte": start_time, "lte": current_time, "format": "basic_date_time"}}},
        "aggs": {
            "unserved": {
                "filters": {
                    "filters": {
                        "rejected": {"query_string": {"query": "rejected:true"}},
                        "disconnect": {"query_string": {"query": "disconn:true"}},
                        "procerror": {"query_string": {"query": "procerror:true"}}
                    }
                },
                "aggs": {"taskid": {"terms": {"field": "taskid", "size": 5, "order": {"_count": "desc"}}}}
                    }
                }
            }

    es = Elasticsearch(hosts=[{'host': es_host, 'port': es_port}], timeout=60)
    results = es.search(index=es_index, body=es_query, request_timeout=600)

    # parse the results received from ES
    failure_count_by_task = {}
    results = results['aggregations']['unserved']['buckets']
    for reason in results:
        results_clean = results[reason]['taskid']['buckets']
        _logger.debug ('Processing tasks with frontier status {0}: {1} tasks'.format(reason, len(results_clean)))
        for entry in results_clean:
            jedi_task_id = entry['key']
            error_count = entry['doc_count']
            failure_count_by_task.setdefault(jedi_task_id, 0)
            failure_count_by_task[jedi_task_id] += error_count
            _logger.debug('task {0} blocked {1} times'.format(jedi_task_id, error_count))

    return failure_count_by_task


def get_task_pslabel_map(task_id_list):
    var_map = {}
    for i, task_id in enumerate(task_id_list):
        var_map[':task_id{0}'.format(i)] = task_id
    task_id_bindings = ','.join(':task_id{0}'.format(i) for i in xrange(len(task_id_list)))

    sql = """
          SELECT jeditaskid, prodsourcelabel FROM ATLAS_PANDA.jobsDefined4 
          WHERE jeditaskid IN({0})
          """.format(task_id_bindings)

    status, ret_sel = taskBuffer.querySQLS(sql, var_map)
    task_pslabel_map = {}
    if ret_sel:
        for jeditaskid, prodsourcelabel in ret_sel:
            task_pslabel_map[jeditaskid] = prodsourcelabel

    return task_pslabel_map


def filter_tasks(failure_count_by_task):
    """
    filter tasks we don't want to retag:
        - with failures over a threshold
        - only production tasks
        - get rid of jobs without task (jeditaskid=0)
    """
    # failure threshold
    min_failure_threshold = 100
    tasks_filtered_threshold = []
    for jedi_task_id in failure_count_by_task:
        if failure_count_by_task[jedi_task_id] > min_failure_threshold:
            tasks_filtered_threshold.append(jedi_task_id)

    # production-analysis filter
    task_pslabel_map = get_task_pslabel_map(tasks_filtered_threshold)
    tasks_filtered_pslabel = []
    for jedi_task_id in tasks_filtered_threshold:
        pslabel = task_pslabel_map.get(jedi_task_id, '')
        if pslabel != 'user':
            tasks_filtered_pslabel.append(jedi_task_id)

    # remove jeditaskid=0
    try:
        tasks_filtered_pslabel.remove(0)
    except ValueError: # jeditaskid not in list, not an issue
        pass

    tasks_to_retag = tasks_filtered_pslabel

    return tasks_to_retag


def retag_tasks(task_id_list, dry_run):
    """
    change the share for the selected tasks
    """
    destination_gshare = 'Frontier'
    reassign_running = True
    _logger.debug('Reassigning tasks (dry_run: {0}): {1}'.format(dry_run, task_id_list))
    if not dry_run:
        return_code, return_message = taskBuffer.reassignShare(task_id_list, destination_gshare, reassign_running)
    else:
        return_code, return_message = 0, 'DRY RUN'
    return return_code, return_message


if __name__ == "__main__":

    try:
        dry_run = sys.argv[1]
    except IndexError:
        dry_run = True

    dry_run = True # for initial debugging

    # 1. get tasks with frontier failures
    failure_count_by_task = get_frontier_failure_count_by_task()

    # 2. filter out tasks by predefined criteria
    tasks_filtered = filter_tasks(failure_count_by_task)

    # 3. retag the tasks
    return_code, return_message = retag_tasks(tasks_filtered, dry_run)