import sys
import time
import traceback

from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandautils.thread_utils import GenericThread
from pandaserver.config import panda_config
from pandaserver.taskbuffer.Utils import create_shards

# logger
_logger = PandaLogger().getLogger("worker_sync")


def translate_status_to_command(pilot_status):
    if pilot_status == "running":
        return "SYNC_WORKERS_ACTIVATE"
    if pilot_status == "finished":
        return "SYNC_WORKERS_KILL"
    return None


class WorkerSync(object):
    def __init__(self, tbuf):
        self._logger = _logger
        self.tbuf = tbuf
        return

    def run(self):
        """
        Identifies workers with stale harvester states and newer pilot states
        :return:
        """

        # timing
        time_start = time.time()
        self._logger.debug("Start.")

        # variables for the harvester command
        status = "new"
        ack_requested = False
        lock_interval = None
        com_interval = None

        try:
            stale_workers_per_harvester = self.tbuf.get_workers_to_synchronize()
            for harvester_id in stale_workers_per_harvester:
                for pilot_status in stale_workers_per_harvester[harvester_id]:
                    command = translate_status_to_command(pilot_status)
                    if command:
                        workers = stale_workers_per_harvester[harvester_id][pilot_status]
                        for worker_shard in create_shards(workers, 100):
                            self._logger.debug(f"Processing harvester_id={harvester_id} pilot_status={pilot_status}. Workers to update: {worker_shard}")
                            self.tbuf.commandToHarvester(
                                harvester_id,
                                command,
                                ack_requested,
                                status,
                                lock_interval,
                                com_interval,
                                worker_shard,
                            )
        except Exception:
            self._logger.error(traceback.format_exc())

        # timing
        time_stop = time.time()
        self._logger.debug(f"Done. Worker sync took: {time_stop - time_start} s")

        return


# main
def main(tbuf=None, **kwargs):
    # instantiate TB
    if tbuf is None:
        from pandaserver.taskbuffer.TaskBuffer import taskBuffer

        requester_id = GenericThread().get_full_id(__name__, sys.modules[__name__].__file__)
        taskBuffer.init(
            panda_config.dbhost,
            panda_config.dbpasswd,
            nDBConnection=1,
            useTimeout=True,
            requester=requester_id,
        )
    else:
        taskBuffer = tbuf
    # run
    WorkerSync(tbuf=taskBuffer).run()
    # stop taskBuffer if created inside this script
    if tbuf is None:
        taskBuffer.cleanup(requester=requester_id)


# run
if __name__ == "__main__":
    main()
