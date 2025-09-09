import json

from pandacommon.pandalogger import logger_utils

from pandajedi.jedimsgprocessor.base_msg_processor import BaseMsgProcPlugin

base_logger = logger_utils.setup_logger(__name__.split(".")[-1])


# Hyper-Parameter-Optimization message processing plugin
class HPOMsgProcPlugin(BaseMsgProcPlugin):
    def process(self, msg_obj, decoded_data=None):
        tmp_log = logger_utils.make_logger(base_logger, token=self.get_pid(), method_name="process")
        # start
        tmp_log.info("start")
        tmp_log.debug(f"sub_id={msg_obj.sub_id} ; msg_id={msg_obj.msg_id}")
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
        # sanity check
        try:
            msg_type = msg_dict["msg_type"]
            jeditaskid = int(msg_dict["workload_id"])
            if msg_type == "file_hyperparameteropt":
                target_list = msg_dict["files"]
            elif msg_type == "collection_hyperparameteropt":
                # to finish the task
                pass
            elif msg_type == "work_hyperparameteropt":
                pass
            else:
                raise ValueError(f"invalid msg_type value: {msg_type}")
        except Exception as e:
            err_str = f"failed to parse message object dict {msg_dict} , skipped. {e.__class__.__name__} : {e}"
            tmp_log.error(err_str)
            raise
        # run
        if msg_type == "file_hyperparameteropt":
            # insert HPO events
            try:
                # event ids from the targets
                event_id_list = []
                for target in target_list:
                    if target["status"] != "New":
                        continue
                    model_id = None
                    try:
                        path = json.loads(target["path"])
                        if isinstance(path[0], (list, tuple)):
                            model_id = path[0][0]
                    except Exception:
                        pass
                    event_id_list.append((target["name"], model_id))
                if event_id_list:
                    n_events = len(event_id_list)
                    # insert events
                    res = self.tbIF.insertHpoEventAboutIdds_JEDI(jedi_task_id=jeditaskid, event_id_list=event_id_list)
                    # check if ok
                    if res:
                        tmp_log.debug(f"jeditaskid={jeditaskid}, inserted {n_events} events: {event_id_list}")
                    else:
                        tmp_log.warning(f"jeditaskid={jeditaskid}, failed to insert events: {event_id_list}")
            except Exception as e:
                err_str = f"failed to parse message object, skipped. {e.__class__.__name__} : {e}"
                tmp_log.error(err_str)
                raise
        elif msg_type == "collection_hyperparameteropt":
            # finish the task
            try:
                # send finish command
                retVal, retStr = self.tbIF.sendCommandTaskPanda(jeditaskid, "iDDS. HPO task finished", True, "finish", comQualifier="soft")
                # check if ok
                if retVal:
                    tmp_log.debug(f"jeditaskid={jeditaskid}, finished the task")
                else:
                    tmp_log.warning(f"jeditaskid={jeditaskid}, failed finish the task: {retStr}")
            except Exception as e:
                err_str = f"failed to parse message object, skipped. {e.__class__.__name__} : {e}"
                tmp_log.error(err_str)
                raise
        else:
            # do nothing
            tmp_log.debug(f"jeditaskid={jeditaskid}, msg_type={msg_type}, did nothing")
        # done
        tmp_log.info("done")
