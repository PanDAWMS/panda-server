import datetime
import json
import sys

from pandacommon.pandalogger.PandaLogger import PandaLogger

from pandaserver.config import panda_config
from pandaserver.srvcore import CoreUtils
from pandaserver.taskbuffer import EventServiceUtils, task_split_rules
from pandaserver.taskbuffer.db_proxy_mods.base_module import BaseModule


# Module class to define task related methods
class TaskModule(BaseModule):
    # constructor
    def __init__(self, log_stream: PandaLogger):
        super().__init__(log_stream)

    # enable job cloning
    def enable_job_cloning(self, jedi_task_id: int, mode: str = None, multiplicity: int = None, num_sites: int = None) -> tuple[bool, str]:
        """
        Enable job cloning for a task

        :param jedi_task_id: jediTaskID
        :param mode: mode of cloning, runonce or storeonce
        :param multiplicity: number of jobs to be created for each target
        :param num_sites: number of sites to be used for each target
        :return: (True, None) if success otherwise (False, error message)
        """
        comment = " /* DBProxy.enable_job_cloning */"
        method_name, tmp_log = self.create_method_name_logger(comment, f"jediTaskID={jedi_task_id}")
        tmp_log.debug("start")
        try:
            ret_value = (True, None)
            # start transaction
            self.conn.begin()
            # get current split rule
            sql_check = f"SELECT splitRule FROM {panda_config.schemaJEDI}.JEDI_Tasks WHERE jediTaskID=:jediTaskID "
            var_map = {":jediTaskID": jedi_task_id}
            self.cur.execute(sql_check + comment, var_map)
            res = self.cur.fetchone()
            if not res:
                # not found
                ret_value = (False, "task not found")
            else:
                (split_rule,) = res
                # set default values
                if mode is None:
                    mode = "runonce"
                if multiplicity is None:
                    multiplicity = 2
                if num_sites is None:
                    num_sites = 2
                # ID of job cloning mode
                mode_id = EventServiceUtils.getJobCloningValue(mode)
                if mode_id == "":
                    ret_value = (False, f"invalid job cloning mode: {mode}")
                else:
                    # set mode
                    split_rule = task_split_rules.replace_rule(split_rule, "useJobCloning", mode_id)
                    # set semaphore size
                    split_rule = task_split_rules.replace_rule(split_rule, "nEventsPerWorker", 1)
                    # set job multiplicity
                    split_rule = task_split_rules.replace_rule(split_rule, "nEsConsumers", multiplicity)
                    # set number of sites
                    split_rule = task_split_rules.replace_rule(split_rule, "nSitesPerJob", num_sites)
                    # update split rule and event service flag
                    sql_update = (
                        f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks SET splitRule=:splitRule,eventService=:eventService WHERE jediTaskID=:jediTaskID "
                    )
                    var_map = {":jediTaskID": jedi_task_id, ":splitRule": split_rule, ":eventService": EventServiceUtils.TASK_JOB_CLONING}
                    self.cur.execute(sql_update + comment, var_map)
                    if not self.cur.rowcount:
                        ret_value = (False, "failed to update task")
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmp_log.debug("done")
            return ret_value
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return False, "failed to enable job cloning"

    # disable job cloning
    def disable_job_cloning(self, jedi_task_id: int) -> tuple[bool, str]:
        """
        Disable job cloning for a task

        :param jedi_task_id: jediTaskID
        :return: (True, None) if success otherwise (False, error message)
        """
        comment = " /* DBProxy.disable_job_cloning */"
        method_name, tmp_log = self.create_method_name_logger(comment, f"jediTaskID={jedi_task_id}")
        tmp_log.debug("start")
        try:
            ret_value = (True, None)
            # start transaction
            self.conn.begin()
            # get current split rule
            sql_check = f"SELECT splitRule FROM {panda_config.schemaJEDI}.JEDI_Tasks WHERE jediTaskID=:jediTaskID "
            var_map = {":jediTaskID": jedi_task_id}
            self.cur.execute(sql_check + comment, var_map)
            res = self.cur.fetchone()
            if not res:
                # not found
                ret_value = (False, "task not found")
            else:
                (split_rule,) = res
                # remove job cloning related rules
                split_rule = task_split_rules.remove_rule_with_name(split_rule, "useJobCloning")
                split_rule = task_split_rules.remove_rule_with_name(split_rule, "nEventsPerWorker")
                split_rule = task_split_rules.remove_rule_with_name(split_rule, "nEsConsumers")
                split_rule = task_split_rules.remove_rule_with_name(split_rule, "nSitesPerJob")
                # update split rule and event service flag
                sql_update = f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks SET splitRule=:splitRule,eventService=:eventService WHERE jediTaskID=:jediTaskID "
                var_map = {":jediTaskID": jedi_task_id, ":splitRule": split_rule, ":eventService": EventServiceUtils.TASK_NORMAL}
                self.cur.execute(sql_update + comment, var_map)
                if not self.cur.rowcount:
                    ret_value = (False, "failed to update task")
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmp_log.debug("done")
            return ret_value
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return False, "failed to disable job cloning"
