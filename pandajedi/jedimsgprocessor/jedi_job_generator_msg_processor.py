import json

from pandacommon.pandalogger import logger_utils

from pandajedi.jedicore.ThreadUtils import ListWithLock, ThreadPool
from pandajedi.jediddm.DDMInterface import DDMInterface
from pandajedi.jedimsgprocessor.base_msg_processor import BaseMsgProcPlugin
from pandajedi.jediorder.JobGenerator import JobGeneratorThread
from pandajedi.jediorder.TaskSetupper import TaskSetupper

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
        # get SiteMapper
        # siteMapper = self.tbIF.get_site_mapper()
        # get work queue mapper
        # workQueueMapper = self.tbIF.getWorkQueueMap()
        # get TaskSetupper
        # taskSetupper = TaskSetupper(self.vos, self.prodSourceLabels)
        # taskSetupper.initializeMods(self.tbIF, self.ddmIF)
        self.pid = self.get_pid()

    def process(self, msg_obj):
        tmp_log = logger_utils.make_logger(base_logger, token=self.get_pid(), method_name="process")
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
            err_str = f"got unknown msg_type {msg_type} , skipped "
            tmp_log.error(err_str)
            raise
        # run
        try:
            # get task to generate jobs
            task_id = int(msg_dict["taskid"])
            _, taskSpec = self.tbIF.getTaskWithID_JEDI(task_id)
            if not taskSpec:
                tmp_log.debug(f"unknown task {task_id}")
            else:
                # get WQ
                vo = taskSpec.vo
                prodSourceLabel = taskSpec.prodSourceLabel
                workQueue = self.tbIF.getWorkQueueMap().getQueueWithIDGshare(taskSpec.workQueue_ID, taskSpec.gshare)
                # get resource types
                resource_types = self.tbIF.load_resource_types()
                if not resource_types:
                    raise RuntimeError("failed to get resource types")
                # get inputs
                tmpList = self.tbIF.getTasksToBeProcessed_JEDI(self.pid, None, workQueue, None, None, nFiles=1000, target_tasks=[task_id])
                if tmpList:
                    inputList = ListWithLock(tmpList)
                    # create thread
                    threadPool = ThreadPool()
                    siteMapper = self.tbIF.get_site_mapper()
                    taskSetupper = TaskSetupper(vo, prodSourceLabel)
                    taskSetupper.initializeMods(self.tbIF, self.ddmIF)
                    gen_thr = JobGeneratorThread(
                        inputList,
                        threadPool,
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
                    gen_thr.start()
                    gen_thr.join()
                    tmp_log.info(f"generated jobs for task {task_id}")
                else:
                    tmp_log.debug(f"task {task_id} is not considered to be processed; skipped ")
        except Exception as e:
            err_str = f"failed to run, skipped. {e.__class__.__name__} : {e}"
            tmp_log.error(err_str)
            raise
        # done
        tmp_log.info("done")
