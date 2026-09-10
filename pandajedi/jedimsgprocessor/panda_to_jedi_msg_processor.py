import json
import os
import socket
from typing import Any

from pandacommon.pandalogger import logger_utils
from pandacommon.pandamsgbkr.msg_bkr_utils import MsgObj

from pandajedi.jedicore.ThreadUtils import ListWithLock, ThreadPool
from pandajedi.jediddm.DDMInterface import DDMInterface
from pandajedi.jedimsgprocessor.base_msg_processor import BaseMsgProcPlugin
from pandajedi.jediorder.JobGenerator import JobGeneratorThread
from pandajedi.jediorder.TaskSetupper import TaskSetupper

base_logger = logger_utils.setup_logger(__name__.split(".")[-1])


# plugin to process messages from Panda to JEDI
class PandaToJediMsgProcPlugin(BaseMsgProcPlugin):
    def initialize(self, in_collective: bool = False) -> None:
        BaseMsgProcPlugin.initialize(self, in_collective)
        self.ddmIF = DDMInterface()
        self.ddmIF.setupInterface()
        self.pid = f"{socket.getfqdn().split('.')[0]}-{os.getpid()}_{os.getpgrp()}-pjmsg"

    def process(self, msg_obj: MsgObj, decoded_data: dict[str, Any] | None = None) -> None:
        tmp_log = logger_utils.make_logger(base_logger, token=self.get_pid(), method_name="process")
        # start
        tmp_log.info("start")
        # parse
        if decoded_data is None:
            # json decode
            try:
                msg_dict = json.loads(msg_obj.data)
            except Exception as e:
                err_str = f"failed to parse message json {msg_obj.data} , skipped. {e.__class__.__name__} : {e}"
                tmp_log.error(err_str)
                raise
        else:
            msg_dict = decoded_data
        # run
        try:
            tmp_log.debug(f"got message {msg_dict}")
            if msg_dict["msg_type"] == "generate_job":
                # get task to generate jobs
                jediTaskID = int(msg_dict["taskid"])
                s, taskSpec = self.tbIF.getTaskWithID_JEDI(jediTaskID)
                if not taskSpec:
                    tmp_log.debug(f"unknown task {jediTaskID}")
                else:
                    # get WQ
                    vo = taskSpec.vo
                    prodSourceLabel = taskSpec.prodSourceLabel
                    # the map is built on first use, so it is there by the time this asks
                    workQueue = self.tbIF.getWorkQueueMap().getQueueWithIDGshare(taskSpec.workQueue_ID, taskSpec.gshare)  # type: ignore[union-attr]
                    # get inputs
                    tmpList = self.tbIF.getTasksToBeProcessed_JEDI(self.pid, None, workQueue, None, None, nFiles=1000, target_tasks=[jediTaskID])
                    if tmpList:
                        # a number comes back only with isPeeking, which this does not ask for
                        inputList = ListWithLock(tmpList)  # type: ignore[arg-type]
                        # create thread
                        threadPool = ThreadPool()
                        siteMapper = self.tbIF.get_site_mapper()
                        taskSetupper = TaskSetupper(vo, prodSourceLabel)
                        taskSetupper.initializeMods(self.tbIF, self.ddmIF)
                        resource_types = self.tbIF.load_resource_types()
                        gen = JobGeneratorThread(
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
                        gen.start()
                        gen.join()
            else:
                tmp_log.debug(f"unknown message type : {msg_dict['msg_type']}")
        except Exception as e:
            err_str = f"failed to run, skipped. {e.__class__.__name__} : {e}"
            tmp_log.error(err_str)
            raise
        # done
        tmp_log.info("done")
