import json
import glob
import time
import os.path
import datetime
import threading
import traceback
import tempfile
import requests
from ruamel import yaml

from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandaserver.config import panda_config
from pandaserver.workflow import pcwl_utils
from pandaserver.workflow import workflow_utils
from pandaserver.srvcore.CoreUtils import commands_get_status_output, clean_user_id

# import idds modules after PandaLogger not to change message levels of other modules
from idds.client.clientmanager import ClientManager
from idds.common.utils import get_rest_host
from idds.workflow.workflow import Workflow, Condition, AndCondition, OrCondition
from idds.atlas.workflow.atlaspandawork import ATLASPandaWork

# logger
_logger = PandaLogger().getLogger('process_workflow_files')


# main
def main(tbuf=None, **kwargs):
    _logger.debug("===================== start =====================")

    # overall timeout value
    overallTimeout = 300
    # prefix of the files
    if 'target' in kwargs and kwargs['target']:
        evpFilePatt = kwargs['target']
    else:
        prefixEVP = '/workflow.'
        # file pattern of evp files
        evpFilePatt = panda_config.cache_dir + '/' + prefixEVP + '*'

    test_mode = kwargs.get('test_mode', False)
    interactive_mode = kwargs.get('interactive_mode', False)
    dump_workflow = kwargs.get('dump_workflow', False)

    # thread pool
    class ThreadPool:
        def __init__(self):
            self.lock = threading.Lock()
            self.list = []

        def add(self, obj):
            self.lock.acquire()
            self.list.append(obj)
            self.lock.release()

        def remove(self, obj):
            self.lock.acquire()
            self.list.remove(obj)
            self.lock.release()

        def join(self):
            self.lock.acquire()
            thrlist = tuple(self.list)
            self.lock.release()
            for thr in thrlist:
                thr.join()

    # thread
    class EvpThr(threading.Thread):
        def __init__(self, lock, pool, file_name, to_delete, get_log):
            threading.Thread.__init__(self)
            self.lock = lock
            self.pool = pool
            self.fileName = file_name
            self.to_delete = to_delete
            self.get_log = get_log
            self.pool.add(self)

        def run(self):
            self.lock.acquire()
            try:
                is_fatal = False
                is_OK = True
                request_id = None
                dump_str = None
                with open(self.fileName) as f:
                    ops = json.load(f)
                    user_name = clean_user_id(ops["userName"])
                    base_platform = ops['data'].get('base_platform')
                    for task_type in ops['data']['taskParams']:
                        ops['data']['taskParams'][task_type]['userName'] = user_name
                        if base_platform:
                            ops['data']['taskParams'][task_type]['basePlatform'] = base_platform
                    tmpLog = LogWrapper(_logger, '< id="{}" outDS={} >'.format(user_name, ops['data']['outDS']))
                    tmpLog.info('start {}'.format(self.fileName))
                    sandbox_url = os.path.join(ops['data']['sourceURL'], 'cache', ops['data']['sandbox'])
                    # go to temp dir
                    cur_dir = os.getcwd()
                    with tempfile.TemporaryDirectory() as tmp_dirname:
                        os.chdir(tmp_dirname)
                        # download sandbox
                        tmpLog.info('downloading sandbox from {}'.format(sandbox_url))
                        with requests.get(sandbox_url, allow_redirects=True, verify=False, stream=True) as r:
                            if r.status_code == 400:
                                tmpLog.error("not found")
                                is_fatal = True
                                is_OK = False
                            elif r.status_code != 200:
                                tmpLog.error("bad HTTP response {}".format(r.status_code))
                                is_OK = False
                            # extract sandbox
                            if is_OK:
                                with open(ops['data']['sandbox'], 'wb') as fs:
                                    for chunk in r.raw.stream(1024, decode_content=False):
                                        if chunk:
                                            fs.write(chunk)
                                    fs.close()
                                    tmp_stat, tmp_out = commands_get_status_output(
                                        'tar xvfz {}'.format(ops['data']['sandbox']))
                                    if tmp_stat != 0:
                                        tmpLog.error(tmp_out)
                                        tmpLog.error('failed to extract {}'.format(ops['data']['sandbox']))
                                        is_fatal = True
                                        is_OK = False
                            # parse workflow files
                            if is_OK:
                                tmpLog.info('parse workflow')
                                workflow_to_submit = None
                                if ops['data']['language'] == 'cwl':
                                    nodes, root_in = pcwl_utils.parse_workflow_file(ops['data']['workflowSpecFile'],
                                                                                    tmpLog)
                                    with open(ops['data']['workflowInputFile']) as workflow_input:
                                        data = yaml.safe_load(workflow_input)
                                    s_id, t_nodes, nodes = pcwl_utils.resolve_nodes(nodes, root_in, data, 0, set(),
                                                                                    ops['data']['outDS'], tmpLog)
                                    workflow_utils.set_workflow_outputs(nodes)
                                    id_map = workflow_utils.get_node_id_map(nodes)
                                    [node.resolve_params(ops['data']['taskParams'], id_map) for node in nodes]
                                    dump_str = "internally converted as follows\n" + workflow_utils.dump_nodes(nodes)
                                    tmpLog.info(dump_str)
                                    for node in nodes:
                                        s_check, o_check = node.verify()
                                        tmp_str = 'Verification failure in ID:{} {}'.format(node.id, o_check)
                                        if not s_check:
                                            tmpLog.error(tmp_str)
                                            dump_str += tmp_str
                                            dump_str += '\n'
                                            is_fatal = True
                                            is_OK = False
                                    id_work_map = {}
                                    for node in nodes:
                                        if node.is_leaf:
                                            if not workflow_to_submit:
                                                workflow_to_submit = Workflow()
                                            work = ATLASPandaWork(task_parameters=node.task_params)
                                            workflow_to_submit.add_work(work)
                                            id_work_map[node.id] = work
                                else:
                                    tmpLog.error("{} is not supported to describe the workflow")
                                    is_fatal = True
                                    is_OK = False
                                # add conditions
                                if is_OK and workflow_to_submit:
                                    for node in nodes:
                                        if node.is_leaf:
                                            if node.parents:
                                                c_work = id_work_map[node.id]
                                                if not node.condition:
                                                    # default conditions if unspecified
                                                    for p_id in node.parents:
                                                        p_work = id_work_map[p_id]
                                                        if len(node.parents) == 1:
                                                            cond_function = p_work.is_started
                                                        else:
                                                            cond_function = p_work.is_finished
                                                        cond = Condition(cond=cond_function,
                                                                         current_work=p_work,
                                                                         true_work=c_work)
                                                        workflow_to_submit.add_condition(cond)
                                                else:
                                                    # convert conditions
                                                    cond_list = node.condition.get_dict_form()
                                                    base_cond_map = {}
                                                    root_condition = None
                                                    for tmp_idx, base_cond in cond_list:
                                                        # leaf condition
                                                        if base_cond['right'] is None:
                                                            # condition based on works
                                                            cond_func_list = []
                                                            for p_id in base_cond['left']:
                                                                p_work = id_work_map[p_id]
                                                                # finished or failed
                                                                if base_cond['operator'] is None:
                                                                    cond_func_list.append(p_work.is_finished)
                                                                else:
                                                                    cond_func_list.append(p_work.is_failed)
                                                            cond = AndCondition(conditions=cond_func_list)
                                                            base_cond_map[tmp_idx] = cond
                                                            root_condition = cond
                                                        else:
                                                            # composite condition
                                                            if isinstance(base_cond['left'], set):
                                                                cond_func_list = []
                                                                for p_id in base_cond['left']:
                                                                    p_work = id_work_map[p_id]
                                                                    cond_func_list.append(p_work.is_finished)
                                                                l_cond = AndCondition(conditions=cond_func_list)
                                                            else:
                                                                l_cond = base_cond_map[base_cond['left']]
                                                            if isinstance(base_cond['right'], set):
                                                                cond_func_list = []
                                                                for p_id in base_cond['right']:
                                                                    p_work = id_work_map[p_id]
                                                                    cond_func_list.append(p_work.is_finished)
                                                                r_cond = AndCondition(conditions=cond_func_list)
                                                            else:
                                                                r_cond = base_cond_map[base_cond['right']]
                                                            if base_cond['operator'] == 'and':
                                                                cond = AndCondition(conditions=[l_cond, r_cond])
                                                            else:
                                                                cond = OrCondition(conditions=[l_cond, r_cond])
                                                            cond = AndCondition(conditions=cond_func_list)
                                                            base_cond_map[tmp_idx] = cond
                                                            root_condition = cond
                                                    # set root condition
                                                    if root_condition:
                                                        root_condition.true_works = [c_work]
                                                        workflow_to_submit.add_condition(root_condition)
                                    try:
                                        if workflow_to_submit:
                                            if not test_mode:
                                                tmpLog.info('submit workflow')
                                                wm = ClientManager(host=get_rest_host())
                                                request_id = wm.submit(workflow_to_submit)
                                        else:
                                            tmpLog.error('workflow is empty')
                                            is_fatal = True
                                            is_OK = False
                                    except Exception as e:
                                        tmpLog.error('failed to submit the workflow with {} {]'.format(
                                            str(e), traceback.format_exc()))
                                    if dump_workflow:
                                        tmpLog.debug(str(workflow_to_submit))
                    os.chdir(cur_dir)
                    if not self.get_log:
                        if is_OK:
                            tmpLog.info('is_OK={} request_id={}'.format(is_OK, request_id))
                        else:
                            tmpLog.info('is_OK={} is_fatal={} request_id={}'.format(is_OK, is_fatal, request_id))
                    if not test_mode and (is_OK or is_fatal or self.to_delete):
                        tmpLog.debug('delete {}'.format(self.fileName))
                        try:
                            os.remove(self.fileName)
                        except Exception:
                            pass
            except Exception as e:
                tmpLog.error("failed to run with {} {}".format(str(e), traceback.format_exc()))
            self.pool.remove(self)
            self.lock.release()
            if self.get_log:
                ret_val = {'status': is_OK}
                if is_OK:
                    ret_val['log'] = dump_str
                else:
                    ret_val['log'] = tmpLog.dumpToString()
                return ret_val

    # get files
    timeNow = datetime.datetime.utcnow()
    timeInt = datetime.datetime.utcnow()
    fileList = glob.glob(evpFilePatt)
    fileList.sort()

    # create thread pool and semaphore
    adderLock = threading.Semaphore(1)
    adderThreadPool = ThreadPool()

    # add
    while len(fileList) != 0:
        # time limit to aviod too many copyArchve running at the sametime
        if (datetime.datetime.utcnow() - timeNow) > datetime.timedelta(minutes=overallTimeout):
            _logger.debug("time over in main session")
            break
        # try to get Semaphore
        adderLock.acquire()
        # get fileList
        if (datetime.datetime.utcnow() - timeInt) > datetime.timedelta(minutes=15):
            timeInt = datetime.datetime.utcnow()
            # get file
            fileList = glob.glob(evpFilePatt)
            fileList.sort()
        # choose a file
        fileName = fileList.pop(0)
        # release lock
        adderLock.release()
        if not os.path.exists(fileName):
            continue
        try:
            modTime = datetime.datetime(*(time.gmtime(os.path.getmtime(fileName))[:7]))
            if interactive_mode:
                _logger.debug("Interactive attempt : %s" % fileName)
                thr = EvpThr(adderLock, adderThreadPool, fileName, True, True)
                return thr.run()
            elif (timeNow - modTime) > datetime.timedelta(hours=2):
                # last chance
                _logger.debug("Last attempt : %s" % fileName)
                thr = EvpThr(adderLock, adderThreadPool, fileName, False, False)
                thr.start()
            elif (timeInt - modTime) > datetime.timedelta(seconds=5):
                # try
                _logger.debug("Normal attempt : %s" % fileName)
                thr = EvpThr(adderLock, adderThreadPool, fileName, True, False)
                thr.start()
            else:
                _logger.debug("Wait %s : %s" % ((timeInt - modTime), fileName))
        except Exception as e:
            _logger.error("{} {}".format(str(e), traceback.format_exc()))

    # join all threads
    adderThreadPool.join()

    _logger.debug("===================== end =====================")


# run
if __name__ == '__main__':
    import sys
    if len(sys.argv) > 1:
        data = {'target': sys.argv[1],
                'test_mode': True,
                'dump_workflow': True}
    else:
        data = {}
    main(**data)
