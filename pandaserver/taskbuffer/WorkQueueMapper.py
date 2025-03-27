"""
mapper to map task/job to a work queue

"""

import re

from .WorkQueue import WorkQueue


class WorkQueueMapper:
    # constructor
    def __init__(self):
        # Initialize maps
        self.work_queue_map = {}
        self.work_queue_global_dic_by_name = {}
        self.work_queue_global_dic_by_id = {}

    def getSqlQuery(self):
        """
        Generates the SQL to get all work queues
        """
        sql = f"SELECT {WorkQueue.column_names()} FROM ATLAS_PANDA.JEDI_Work_Queue"

        return sql

    def makeMap(self, work_queues, global_leave_shares):
        """
        Creates the mapping with work queues and global shares
        :param work_queues: work queues
        :param global_leave_shares: global leave shares
        :return
        """

        # 1. add all workqueues to the map
        for wq in work_queues:
            # pack
            work_queue = WorkQueue()
            work_queue.pack(wq)

            # skip inactive queues
            if not work_queue.isActive():
                continue

            # add VO
            if work_queue.VO not in self.work_queue_map:
                self.work_queue_map[work_queue.VO] = {}

            # add type
            if work_queue.queue_type not in self.work_queue_map[work_queue.VO]:
                self.work_queue_map[work_queue.VO][work_queue.queue_type] = []

            self.work_queue_map[work_queue.VO][work_queue.queue_type].append(work_queue)
            self.work_queue_global_dic_by_name[work_queue.queue_name] = work_queue
            self.work_queue_global_dic_by_id[work_queue.queue_id] = work_queue

        # sort the queue list by order
        for vo in self.work_queue_map:
            for type in self.work_queue_map[vo]:
                # make ordered map
                ordered_map = {}
                queue_map = self.work_queue_map[vo][type]
                for wq in queue_map:
                    if wq.queue_order not in ordered_map:
                        ordered_map[wq.queue_order] = []
                    # append
                    ordered_map[wq.queue_order].append(wq)
                # make sorted list
                ordered_list = list(ordered_map.keys())
                ordered_list.sort(key=lambda x: (x is None, x))
                new_list = []
                for order_val in ordered_list:
                    new_list += ordered_map[order_val]
                # set new list
                self.work_queue_map[vo][type] = new_list

        # 2. add all the global shares
        for gs in global_leave_shares:
            work_queue_gs = WorkQueue()
            work_queue_gs.pack_gs(gs)

            if work_queue_gs.VO is None:
                vo = "atlas"
            else:
                vo = work_queue_gs.VO

            if vo not in self.work_queue_map:
                self.work_queue_map[vo] = {}

            if work_queue_gs.queue_type not in self.work_queue_map[vo]:
                self.work_queue_map[vo][work_queue_gs.queue_type] = []

            self.work_queue_map[vo][work_queue_gs.queue_type].append(work_queue_gs)
            self.work_queue_global_dic_by_name[work_queue_gs.queue_name] = work_queue_gs
            self.work_queue_global_dic_by_id[work_queue_gs.queue_id] = work_queue_gs

        # return
        return

    def dump(self):
        """
        Creates a human-friendly string showing the work queue mappings
        :return: string representation of the work queue mappings
        """
        dump_str = "WorkQueue mapping\n"
        for VO in self.work_queue_map:
            dump_str += f"  VO={VO}\n"
            for type in self.work_queue_map[VO]:
                dump_str += f"    type={type}\n"
                for workQueue in self.work_queue_map[VO][type]:
                    dump_str += f"    {workQueue.dump()}\n"
        # return
        return dump_str

    def getQueueWithSelParams(self, vo, type, **param_map):
        """
        Used for tagging of work queues in task refiner. Get a work queue based on the selection parameters
        :param vo: vo
        :param type: type (in practice equivalent to prodsourcelabel)
        :param param_map: parameter selection map
        :return: work queue object and explanation in case no queue was found
        """
        ret_str = ""
        if vo not in self.work_queue_map:
            ret_str = f"queues for vo={vo} are undefined"
        elif type not in self.work_queue_map[vo]:
            # check type
            ret_str = f"queues for type={type} are undefined in vo={vo}"
        else:
            for wq in self.work_queue_map[vo][type]:
                # don't return global share IDs for work queues
                if wq.is_global_share:
                    continue

                # evaluate
                try:
                    ret_queue, result = wq.evaluate(param_map)
                    if result:
                        return ret_queue, ret_str
                except Exception:
                    ret_str += f"{wq.queue_name},"

            ret_str = ret_str[:-1]
            if ret_str != "":
                new_ret_str = f"eval with VO={vo} "
                for tmp_param_key, tmp_param_val in param_map.items():
                    new_ret_str += "{0}={1} failed for {0}".format(tmp_param_key, tmp_param_val, ret_str)
                ret_str = new_ret_str

        # no queue matched to selection parameters
        return None, ret_str

    def getQueueByName(self, vo, type, queue_name):
        """
        # get queue by name
        :param queue_name: name of the queue
        :param vo: vo
        :param type: type
        :return: queue object or None if not found
        """
        if vo in self.work_queue_map and type in self.work_queue_map[vo]:
            for wq in self.work_queue_map[vo][type]:
                if wq.queue_name == queue_name:
                    return wq
        return None

    # get queue with ID
    def getQueueWithIDGshare(self, queue_id, gshare_name):
        # 1. Check for a Resource queue
        if queue_id in self.work_queue_global_dic_by_id and self.work_queue_global_dic_by_id[queue_id].queue_function == "Resource":
            return self.work_queue_global_dic_by_id[queue_id]

        # 2. If it wasn't a resource queue, return the global share work queue
        if gshare_name in self.work_queue_global_dic_by_name:
            return self.work_queue_global_dic_by_name[gshare_name]

        # not found
        return None

    # get queue list with VO and type
    def getAlignedQueueList(self, vo, queue_type):
        """
        NOTE: Returns ONLY resource queues and global shares (old non-resource queues are skipped)
        """
        ret_list = []

        if vo in self.work_queue_map:
            # if queue type was specified
            if queue_type not in ["", None, "any"]:
                for map_queue_type in self.work_queue_map[vo]:
                    if re.match(map_queue_type, queue_type):
                        for tmp_wq in self.work_queue_map[vo][map_queue_type]:
                            if tmp_wq.isAligned():
                                ret_list.append(tmp_wq)

            # include all queue types
            else:
                for tmp_type, tmp_wq_list in self.work_queue_map[vo].items():
                    for tmp_wq in tmp_wq_list:
                        if tmp_wq.isAligned():
                            ret_list.append(tmp_wq)

        return ret_list
