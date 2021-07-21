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
from pandaserver.srvcore.CoreUtils import commands_get_status_output

# logger
_logger = PandaLogger().getLogger('process_workflow_files')


# main
def main(tbuf=None, **kwargs):
    _logger.debug("===================== start =====================")

    # overall timeout value
    overallTimeout = 300
    # prefix of the files
    prefixEVP = '/workflow.'
    # file pattern of evp files
    evpFilePatt = panda_config.cache_dir + '/' + prefixEVP + '*'

    from pandaserver.taskbuffer.TaskBuffer import taskBuffer
    taskBuffer.init(panda_config.dbhost, panda_config.dbpasswd, nDBConnection=1)

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
        def __init__(self, lock, pool, tb_if, file_name, to_delete):
            threading.Thread.__init__(self)
            self.lock = lock
            self.pool = pool
            self.fileName = file_name
            self.to_delete = to_delete
            self.taskBuffer = tb_if
            self.pool.add(self)

        def run(self):
            self.lock.acquire()
            try:
                is_fatal = False
                is_OK = True
                with open(self.fileName) as f:
                    ops = json.load(f)
                    user_name = self.taskBuffer.cleanUserID(ops["userName"])
                    ops['data']['taskParams']['userName'] = user_name
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
                                nodes, root_in = pcwl_utils.parse_workflow_file(ops['data']['workflowSpecFile'], tmpLog)
                                with open(ops['data']['workflowInputFile']) as workflow_in:
                                    data = yaml.safe_load(workflow_in)
                                s_id, t_nodes, nodes = pcwl_utils.resolve_nodes(nodes, root_in, data, 0, set(),
                                                                                ops['data']['outDS'], tmpLog)
                                id_map = workflow_utils.get_node_id_map(nodes)
                                [node.resolve_params(ops['data']['taskParams'], id_map) for node in nodes]
                                dump_str = workflow_utils.dump_nodes(nodes)
                                tmpLog.info(dump_str)
                    os.chdir(cur_dir)
                    tmpLog.info('is_OK={} is_fatal={}'.format(is_OK, is_fatal))
                    if is_OK or is_fatal or self.to_delete:
                        tmpLog.debug('delete {}'.format(self.fileName))
                        try:
                            os.remove(self.fileName)
                        except Exception:
                            pass
            except Exception as e:
                tmpLog.error("failed to run with {} {}".format(str(e), traceback.format_exc()))
            self.pool.remove(self)
            self.lock.release()

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
            if (timeNow - modTime) > datetime.timedelta(hours=2):
                # last chance
                _logger.debug("Last attempt : %s" % fileName)
                thr = EvpThr(adderLock, adderThreadPool, taskBuffer, fileName, False)
                thr.start()
            elif (timeInt - modTime) > datetime.timedelta(seconds=5):
                # try
                _logger.debug("Normal attempt : %s" % fileName)
                thr = EvpThr(adderLock, adderThreadPool, taskBuffer, fileName, True)
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
    main()
