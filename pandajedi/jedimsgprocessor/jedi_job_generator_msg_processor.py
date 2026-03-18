import gc
import json
import time

from pandacommon.pandalogger import logger_utils
from pandacommon.pandautils.PandaUtils import try_malloc_trim

from pandajedi.jedicore.ThreadUtils import ListWithLock
from pandajedi.jediddm.DDMInterface import DDMInterface
from pandajedi.jedimsgprocessor.base_msg_processor import BaseMsgProcPlugin
from pandajedi.jediorder.JobGenerator import JobGeneratorThread, get_params_to_get_tasks
from pandajedi.jediorder.TaskSetupper import TaskSetupper
from pandaserver.srvcore import CoreUtils

base_logger = logger_utils.setup_logger(__name__.split(".")[-1])


# Jedi Job Generator message processor plugin
class JediJobGeneratorMsgProcPlugin(BaseMsgProcPlugin):
    """
    Message-driven Job Generator
    """

    def initialize(self):
        BaseMsgProcPlugin.initialize(self)
        # DDM interface
        self.ddmIF = DDMInterface()
        self.ddmIF.setupInterface()
        self.task_setupper_map = {}
        # cache heavy metadata objects with short TTL to reduce allocation churn
        self._cache_ttl_sec = 300
        self._site_mapper = None
        self._site_mapper_ts = 0
        self._work_queue_mapper = None
        self._work_queue_mapper_ts = 0
        self._resource_types = None
        self._resource_types_ts = 0
        self._params_to_get_tasks = {}
        # memory limit to trigger early cleanup to avoid OOM killer. This is not a hard limit, just a threshold to trigger early cleanup.
        self._mem_usage_threshold_mb = 1500
        # get SiteMapper
        # siteMapper = self.tbIF.get_site_mapper()
        # get work queue mapper
        # workQueueMapper = self.tbIF.getWorkQueueMap()
        # get TaskSetupper
        # taskSetupper = TaskSetupper(self.vos, self.prodSourceLabels)
        # taskSetupper.initializeMods(self.tbIF, self.ddmIF)
        self.pid = self.get_pid()

    def _is_cache_valid(self, ts):
        return (time.time() - ts) < self._cache_ttl_sec

    def _get_site_mapper(self):
        if self._site_mapper is None or not self._is_cache_valid(self._site_mapper_ts):
            self._site_mapper = self.tbIF.get_site_mapper()
            self._site_mapper_ts = time.time()
        return self._site_mapper

    def _get_work_queue_mapper(self):
        if self._work_queue_mapper is None or not self._is_cache_valid(self._work_queue_mapper_ts):
            self._work_queue_mapper = self.tbIF.getWorkQueueMap()
            self._work_queue_mapper_ts = time.time()
        return self._work_queue_mapper

    def _get_resource_types(self):
        if self._resource_types is None or not self._is_cache_valid(self._resource_types_ts):
            self._resource_types = self.tbIF.load_resource_types()
            self._resource_types_ts = time.time()
        return self._resource_types

    def process(self, msg_obj):
        tmp_log = logger_utils.make_logger(base_logger, token=self.get_pid(), method_name="process")
        input_list = None
        gen_thr = None
        tmp_list = None
        taskSpec = None
        taskSetupper = None
        # start
        tmp_log.info("start")
        tmp_log.debug(f"sub_id={msg_obj.sub_id} ; msg_id={msg_obj.msg_id}")
        # parse json
        try:
            msg_dict = json.loads(msg_obj.data)
        except Exception as e:
            err_str = f"failed to parse message json {msg_obj.data} , skipped. {e.__class__.__name__} : {e}"
            tmp_log.error(err_str)
            raise
        # sanity check
        try:
            msg_type = msg_dict["msg_type"]
        except Exception as e:
            err_str = f"failed to parse message object dict {msg_dict} , skipped. {e.__class__.__name__} : {e}"
            tmp_log.error(err_str)
            raise
        if msg_type != "jedi_job_generator":
            # FIXME
            err_str = f"got unknown msg_type {msg_type} , skipped"
            tmp_log.error(err_str)
            raise
        # run
        try:
            # get task to generate jobs
            task_id = int(msg_dict["taskid"])
            _, taskSpec = self.tbIF.getTaskWithID_JEDI(task_id)
            if not taskSpec:
                tmp_log.debug(f"unknown task_id={task_id}, skipped")
            else:
                tmp_log.debug(f"processing task_id={task_id} workqueue_id={taskSpec.workQueue_ID} status={taskSpec.status}")
                # get WQ
                vo = taskSpec.vo
                prodSourceLabel = taskSpec.prodSourceLabel
                workQueue = self._get_work_queue_mapper().getQueueWithIDGshare(taskSpec.workQueue_ID, taskSpec.gshare)
                # get resource types
                resource_types = self._get_resource_types()
                if not resource_types:
                    raise RuntimeError("failed to get resource types")
                # nFiles from shared JobGenerator config resolver
                tmp_params = get_params_to_get_tasks(
                    self.tbIF, self._params_to_get_tasks, vo, prodSourceLabel, workQueue.queue_name if workQueue else "", taskSpec.cloud
                )
                nFiles = tmp_params["nFiles"]
                # get inputs
                tmp_list = self.tbIF.getTasksToBeProcessed_JEDI(self.pid, None, workQueue, None, None, nFiles=nFiles, target_tasks=[task_id])
                if tmp_list:
                    input_list = ListWithLock(tmp_list)
                    # run generator inline to avoid creating an extra worker thread.
                    siteMapper = self._get_site_mapper()
                    setupper_key = (vo, prodSourceLabel)
                    taskSetupper = self.task_setupper_map.get(setupper_key)
                    if taskSetupper is None:
                        taskSetupper = TaskSetupper(vo, prodSourceLabel)
                        taskSetupper.initializeMods(self.tbIF, self.ddmIF)
                        self.task_setupper_map[setupper_key] = taskSetupper
                    # log memory usage before running job generator to help identify if this is a source of memory bloat.
                    gen_thr = JobGeneratorThread(
                        input_list,
                        None,
                        self.tbIF,
                        self.ddmIF,
                        siteMapper,
                        True,
                        taskSetupper,
                        self.pid,
                        workQueue,
                        "pjmsg",
                        None,
                        None,
                        None,
                        False,
                        resource_types,
                    )
                    gen_thr.run()
                    tmp_log.info(f"task_id={task_id} generated jobs")
                else:
                    tmp_log.debug(f"task_id={task_id} is not considered to be processed, skipped")
        except Exception as e:
            err_str = f"failed to run, skipped. {e.__class__.__name__} : {e}"
            tmp_log.error(err_str)
            raise
        finally:
            # Explicitly clear large per-run maps to release references as early as possible.
            if gen_thr is not None:
                try:
                    gen_thr.buildSpecMap.clear()
                    gen_thr.finished_lib_specs_map.clear()
                    gen_thr.active_lib_specs_map.clear()
                    gen_thr.inputList = None
                except Exception:
                    pass
            gen_thr = None
            input_list = None
            tmp_list = None
            taskSpec = None
            taskSetupper = None
            # If memory usage is above the threshold, trigger early cleanup to avoid OOM killer.
            mem_usage = CoreUtils.getMemoryUsage()
            if mem_usage is None:
                tmp_log.warning("failed to get memory usage, skipped memory cleanup")
            elif mem_usage > self._mem_usage_threshold_mb:
                gc.collect()
                try_malloc_trim(tmp_log)
                new_mem_usage = CoreUtils.getMemoryUsage()
                tmp_log.debug(f"trimmed memory usage from {mem_usage} MB to {new_mem_usage} MB")
        # done
        tmp_log.info("done")
