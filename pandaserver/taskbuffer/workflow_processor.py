import json
import os.path
import sys
import tempfile
import traceback

import requests
from idds.client.clientmanager import ClientManager
from idds.common.utils import get_rest_host
from pandacommon.pandalogger.LogWrapper import LogWrapper

# import PandaLogger before idds modules not to change message levels of other modules
from pandacommon.pandalogger.PandaLogger import PandaLogger
from ruamel.yaml import YAML

from pandaserver.srvcore.CoreUtils import clean_user_id, commands_get_status_output
from pandaserver.srvcore.MailUtils import MailUtils
from pandaserver.workflow import pcwl_utils, workflow_utils
from pandaserver.workflow.snakeparser import Parser

_logger = PandaLogger().getLogger("workflow_processor")


# process workflow
class WorkflowProcessor(object):
    # constructor
    def __init__(self, task_buffer=None, log_stream=None):
        self.taskBuffer = task_buffer
        self.log = _logger

    # process a file
    def process(
        self,
        file_name,
        to_delete=False,
        test_mode=False,
        get_log=False,
        dump_workflow=False,
    ):
        is_fatal = False
        is_OK = True
        request_id = None
        dump_str = None
        try:
            with open(file_name) as f:
                ops = json.load(f)
                user_name = clean_user_id(ops["userName"])
                base_platform = ops["data"].get("base_platform")
                for task_type in ops["data"]["taskParams"]:
                    ops["data"]["taskParams"][task_type]["userName"] = user_name
                    if base_platform:
                        ops["data"]["taskParams"][task_type]["basePlatform"] = base_platform
                log_token = f"< id=\"{user_name}\" test={test_mode} outDS={ops['data']['outDS']} >"
                tmpLog = LogWrapper(self.log, log_token)
                tmpLog.info(f"start {file_name}")
                sandbox_url = os.path.join(ops["data"]["sourceURL"], "cache", ops["data"]["sandbox"])
                # IO through json files
                ops_file = tempfile.NamedTemporaryFile(delete=False, mode="w")
                json.dump(ops, ops_file)
                ops_file.close()
                # execute main in another process to avoid chdir mess
                tmp_stat, tmp_out = commands_get_status_output(
                    f"python {__file__} {sandbox_url} '{log_token}' {dump_workflow} {ops_file.name} '{user_name}' {test_mode}"
                )
                if tmp_stat:
                    is_OK = False
                    tmpLog.error(f"main execution failed with {tmp_stat}:{tmp_out}")
                else:
                    with open(tmp_out.split("\n")[-1]) as tmp_out_file:
                        is_OK, is_fatal, request_id, dump_str = json.load(tmp_out_file)
                    try:
                        os.remove(tmp_out)
                    except Exception:
                        pass
                if is_OK:
                    tmpLog.info(f"is_OK={is_OK} request_id={request_id}")
                else:
                    tmpLog.info(f"is_OK={is_OK} is_fatal={is_fatal} request_id={request_id}")
                if to_delete or (not test_mode and (is_OK or is_fatal)):
                    if dump_str:
                        dump_str = dump_str + tmpLog.dumpToString()
                    else:
                        dump_str = tmpLog.dumpToString()
                    tmpLog.debug(f"delete {file_name}")
                    try:
                        os.remove(file_name)
                    except Exception:
                        pass
                    # send notification
                    if not test_mode and self.taskBuffer is not None:
                        toAdder = self.taskBuffer.getEmailAddr(user_name)
                        if toAdder is None or toAdder.startswith("notsend"):
                            tmpLog.debug("skip to send notification since suppressed")
                        else:
                            # message
                            if is_OK:
                                mailSubject = f"PANDA Notification for Workflow {ops['data']['outDS']}"
                                mailBody = f"Hello,\n\nWorkflow:{ops['data']['outDS']} has been accepted with RequestID:{request_id}\n\n"
                            else:
                                mailSubject = f"PANDA WARNING for Workflow={ops['data']['outDS']}"
                                mailBody = "Hello,\n\nWorkflow {} was not accepted\n\n".format(ops["data"]["outDS"], request_id)
                                mailBody += f"Reason : {dump_str}\n"
                            # send
                            tmpSM = MailUtils().send(toAdder, mailSubject, mailBody)
                            tmpLog.debug(f"sent message with {tmpSM}")
        except Exception as e:
            is_OK = False
            tmpLog.error(f"failed to run with {str(e)} {traceback.format_exc()}")
        if get_log:
            ret_val = {"status": is_OK, "request_id": request_id}
            if is_OK:
                ret_val["log"] = dump_str
            else:
                if dump_str is None:
                    ret_val["log"] = tmpLog.dumpToString()
                else:
                    ret_val["log"] = dump_str
            return ret_val


# execute chdir in another process
def core_exec(sandbox_url, log_token, dump_workflow, ops_file, user_name, test_mode):
    tmpLog = LogWrapper(_logger, log_token)
    is_OK = True
    is_fatal = False
    request_id = None
    if dump_workflow == "True":
        dump_workflow = True
    else:
        dump_workflow = False
    if test_mode == "True":
        test_mode = True
    else:
        test_mode = False
    try:
        with open(ops_file) as f:
            ops = json.load(f)
        try:
            os.remove(ops_file)
        except Exception:
            pass
        # go to temp dir
        cur_dir = os.getcwd()
        with tempfile.TemporaryDirectory() as tmp_dirname:
            os.chdir(tmp_dirname)
            # download sandbox
            tmpLog.info(f"downloading sandbox from {sandbox_url}")
            with requests.get(sandbox_url, allow_redirects=True, verify=False, stream=True) as r:
                if r.status_code == 400:
                    tmpLog.error("not found")
                    is_fatal = True
                    is_OK = False
                elif r.status_code != 200:
                    tmpLog.error(f"bad HTTP response {r.status_code}")
                    is_OK = False
                # extract sandbox
                if is_OK:
                    with open(ops["data"]["sandbox"], "wb") as fs:
                        for chunk in r.raw.stream(1024, decode_content=False):
                            if chunk:
                                fs.write(chunk)
                        fs.close()
                        tmp_stat, tmp_out = commands_get_status_output(f"tar xvfz {ops['data']['sandbox']}")
                        if tmp_stat != 0:
                            tmpLog.error(tmp_out)
                            dump_str = f"failed to extract {ops['data']['sandbox']}"
                            tmpLog.error(dump_str)
                            is_fatal = True
                            is_OK = False
                # parse workflow files
                if is_OK:
                    tmpLog.info("parse workflow")
                    workflow_name = None
                    if ops["data"]["language"] == "cwl":
                        workflow_name = ops["data"].get("workflow_name")
                        nodes, root_in = pcwl_utils.parse_workflow_file(ops["data"]["workflowSpecFile"], tmpLog)
                        with open(ops["data"]["workflowInputFile"]) as workflow_input:
                            yaml = YAML(typ="safe", pure=True)
                            data = yaml.load(workflow_input)
                        # noinspection DuplicatedCode
                        s_id, t_nodes, nodes = pcwl_utils.resolve_nodes(nodes, root_in, data, 0, set(), ops["data"]["outDS"], tmpLog)
                        workflow_utils.set_workflow_outputs(nodes)
                        id_node_map = workflow_utils.get_node_id_map(nodes)
                        [node.resolve_params(ops["data"]["taskParams"], id_node_map) for node in nodes]
                        dump_str = "the description was internally converted as follows\n" + workflow_utils.dump_nodes(nodes)
                        tmpLog.info(dump_str)
                        for node in nodes:
                            s_check, o_check = node.verify()
                            tmp_str = f"Verification failure in ID:{node.id} {o_check}"
                            if not s_check:
                                tmpLog.error(tmp_str)
                                dump_str += tmp_str
                                dump_str += "\n"
                                is_fatal = True
                                is_OK = False
                    elif ops["data"]["language"] == "snakemake":
                        parser = Parser(ops["data"]["workflowSpecFile"], logger=tmpLog)
                        nodes, root_in = parser.parse_nodes()
                        data = dict()
                        # noinspection DuplicatedCode
                        s_id, t_nodes, nodes = pcwl_utils.resolve_nodes(nodes, root_in, data, 0, set(), ops["data"]["outDS"], tmpLog)
                        workflow_utils.set_workflow_outputs(nodes)
                        id_node_map = workflow_utils.get_node_id_map(nodes)
                        [node.resolve_params(ops["data"]["taskParams"], id_node_map) for node in nodes]
                        dump_str = "the description was internally converted as follows\n" + workflow_utils.dump_nodes(nodes)
                        tmpLog.info(dump_str)
                        for node in nodes:
                            s_check, o_check = node.verify()
                            tmp_str = f"Verification failure in ID:{node.id} {o_check}"
                            if not s_check:
                                tmpLog.error(tmp_str)
                                dump_str += tmp_str
                                dump_str += "\n"
                                is_fatal = True
                                is_OK = False
                    else:
                        dump_str = "{} is not supported to describe the workflow"
                        tmpLog.error(dump_str)
                        is_fatal = True
                        is_OK = False
                    # convert to workflow
                    if is_OK:
                        (
                            workflow_to_submit,
                            dump_str_list,
                        ) = workflow_utils.convert_nodes_to_workflow(nodes, workflow_name=workflow_name)
                        try:
                            if workflow_to_submit:
                                if not test_mode:
                                    tmpLog.info("submit workflow")
                                    wm = ClientManager(host=get_rest_host())
                                    request_id = wm.submit(workflow_to_submit, username=user_name, use_dataset_name=False)
                                    try:
                                        request_id = int(request_id)
                                    except Exception:
                                        # wrong request_id
                                        is_fatal = True
                                        is_OK = False
                            else:
                                dump_str = "workflow is empty"
                                tmpLog.error(dump_str)
                                is_fatal = True
                                is_OK = False
                        except Exception as e:
                            dump_str = f"failed to submit the workflow with {str(e)}"
                            tmpLog.error(f"{dump_str} {traceback.format_exc()}")
                        if dump_workflow:
                            tmpLog.debug("\n" + "".join(dump_str_list))
        os.chdir(cur_dir)
    except Exception as e:
        is_OK = False
        is_fatal = True
        tmpLog.error(f"failed to run with {str(e)} {traceback.format_exc()}")

    with tempfile.NamedTemporaryFile(delete=False, mode="w") as tmp_json:
        json.dump([is_OK, is_fatal, request_id, tmpLog.dumpToString()], tmp_json)
        print(tmp_json.name)
    sys.exit(0)


if __name__ == "__main__":
    core_exec(*sys.argv[1:])
