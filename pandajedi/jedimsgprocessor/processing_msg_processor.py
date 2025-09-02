import json

from pandacommon.pandalogger import logger_utils

from pandajedi.jedimsgprocessor.base_msg_processor import BaseMsgProcPlugin

base_logger = logger_utils.setup_logger(__name__.split(".")[-1])


# processing message processing plugin
class ProcessingMsgProcPlugin(BaseMsgProcPlugin):
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
            jeditaskid = int(msg_dict["workload_id"])
            # message type
            msg_type = msg_dict["msg_type"]
            if msg_type == "file_processing":
                target_list = msg_dict["files"]
            elif msg_type == "collection_processing":
                target_list = msg_dict["collections"]
            elif msg_type == "work_processing":
                pass
            else:
                raise ValueError(f"invalid msg_type value: {msg_type}")
            # relation type
            relation_type = msg_dict.get("relation_type")
        except Exception as e:
            err_str = f"failed to parse message object dict {msg_dict} , skipped. {e.__class__.__name__} : {e}"
            tmp_log.error(err_str)
            raise
        # run
        try:
            # initialize to_proceed
            to_proceed = False
            # type filters
            if msg_type in ["file_processing", "collection_processing"] and relation_type in ["input"]:
                to_proceed = True
            # whether to proceed the targets
            if to_proceed:
                # initialize
                scope_name_dict_map = {}
                missing_files_dict = {}
                # loop over targets
                for target in target_list:
                    name = target["name"]
                    scope = target["scope"]
                    datasetid = target.get("external_coll_id", None)
                    fileid = target.get("external_content_id", None)
                    if (msg_type == "file_processing" and target["status"] in ["Available"]) or (
                        msg_type == "collection_processing" and target["status"] in ["Closed"]
                    ):
                        scope_name_dict_map.setdefault(scope, {})
                        scope_name_dict_map[scope][name] = (datasetid, fileid)
                    elif msg_type == "file_processing" and target["status"] in ["Missing"]:
                        # missing files
                        missing_files_dict[name] = (datasetid, fileid)
                    else:
                        # got target in bad attributes, do nothing
                        tmp_log.debug(f"jeditaskid={jeditaskid}, scope={scope}, msg_type={msg_type}, status={target['status']}, did nothing for bad target")
                        pass
                # run by each scope
                for scope, name_dict in scope_name_dict_map.items():
                    # about files or datasets in good status
                    if msg_type == "file_processing":
                        tmp_log.debug(f"jeditaskid={jeditaskid}, scope={scope}, update about files...")
                        res = self.tbIF.updateInputFilesStaged_JEDI(jeditaskid, scope, name_dict, by="iDDS")
                        if res is None:
                            # got error and rollback in dbproxy
                            err_str = f"jeditaskid={jeditaskid}, scope={scope}, failed to update files"
                            raise RuntimeError(err_str)
                        tmp_log.info(f"jeditaskid={jeditaskid}, scope={scope}, updated {res} files")
                    elif msg_type == "collection_processing":
                        tmp_log.debug(f"jeditaskid={jeditaskid}, scope={scope}, update about datasets...")
                        res = self.tbIF.updateInputDatasetsStaged_JEDI(jeditaskid, scope, name_dict, by="iDDS")
                        if res is None:
                            # got error and rollback in dbproxy
                            err_str = f"jeditaskid={jeditaskid}, scope={scope}, failed to update datasets"
                            raise RuntimeError(err_str)
                        tmp_log.info(f"jeditaskid={jeditaskid}, scope={scope}, updated {res} datasets")
                    # send message to contents feeder if new files are staged
                    if res > 0 or msg_type == "collection_processing":
                        tmp_s, task_spec = self.tbIF.getTaskWithID_JEDI(jeditaskid)
                        if tmp_s and task_spec.is_msg_driven():
                            push_ret = self.tbIF.push_task_trigger_message("jedi_contents_feeder", jeditaskid, task_spec=task_spec)
                            if push_ret:
                                tmp_log.debug(f"pushed trigger message to jedi_contents_feeder for jeditaskid={jeditaskid}")
                            else:
                                tmp_log.warning(f"failed to push trigger message to jedi_contents_feeder for jeditaskid={jeditaskid}")
                    # check if all ok
                    if res == len(target_list):
                        tmp_log.debug(f"jeditaskid={jeditaskid}, scope={scope}, all OK")
                    elif res < len(target_list):
                        tmp_log.warning(f"jeditaskid={jeditaskid}, scope={scope}, only {res} out of {len(target_list)} done...")
                    elif res > len(target_list):
                        tmp_log.warning(f"jeditaskid={jeditaskid}, scope={scope}, strangely, {res} out of {len(target_list)} done...")
                    else:
                        tmp_log.warning(f"jeditaskid={jeditaskid}, scope={scope}, something unwanted happened...")
                # handle missing files
                n_missing = len(missing_files_dict)
                if n_missing > 0:
                    res = self.tbIF.setMissingFilesAboutIdds_JEDI(jeditaskid=jeditaskid, filenames_dict=missing_files_dict)
                    if res == n_missing:
                        tmp_log.debug(f"jeditaskid={jeditaskid}, marked all {n_missing} files missing")
                    elif res < n_missing:
                        tmp_log.warning(f"jeditaskid={jeditaskid}, only {res} out of {n_missing} files marked missing...")
                    elif res > n_missing:
                        tmp_log.warning(f"jeditaskid={jeditaskid}, strangely, {res} out of {n_missing} files marked missing...")
                    else:
                        tmp_log.warning(f"jeditaskid={jeditaskid}, res={res}, something unwanted happened about missing files...")
            else:
                # do nothing
                tmp_log.debug(f"jeditaskid={jeditaskid}, msg_type={msg_type}, relation_type={relation_type}, nothing done")
        except Exception as e:
            err_str = f"failed to process the message, skipped. {e.__class__.__name__} : {e}"
            tmp_log.error(err_str)
            raise
        # done
        tmp_log.info("done")
