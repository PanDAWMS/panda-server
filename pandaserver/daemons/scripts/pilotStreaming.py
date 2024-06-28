import sys
import time
import traceback

from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandautils.thread_utils import GenericThread

from pandaserver.config import panda_config

_logger = PandaLogger().getLogger("pilot_streaming")


class PilotStreaming(object):
    def __init__(self, tbuf):
        self._logger = _logger
        self.tbuf = tbuf
        return

    def run(self):
        """
        Gets and iterates over ups queues, deciding the job requirements and sending these to Harvester
        via the command interface
        :return:
        """

        # timing
        time_start = time.time()
        self._logger.debug("Start.")

        # get unified pilot streaming (ups) queues
        ups_queues = self.tbuf.ups_get_queues()
        self._logger.debug(f"UPS queues: {ups_queues}")

        # load the worker stats from the database
        worker_stats = self.tbuf.ups_load_worker_stats()

        # iterate over the UPS queues
        for ups_queue in ups_queues:
            # separate the worker and job stats for the queue
            try:
                tmp_worker_stats = worker_stats[ups_queue]
                self._logger.debug(f"worker_stats for queue {ups_queue}: {tmp_worker_stats}")
            except KeyError:
                # skip queue if no data available
                self._logger.debug(f"No worker stats for queue {ups_queue}")
                continue

            # calculate the new worker distribution and save the harvester commands in the database
            try:
                new_workers_per_harvester = self.tbuf.ups_new_worker_distribution(ups_queue, tmp_worker_stats)
                self._logger.info(f"queue: {ups_queue}, results: {new_workers_per_harvester}")

                # variables for the harvester command
                command = f"SET_N_WORKERS_JOBTYPE:{ups_queue}"
                status = "new"
                ack_requested = False
                lock_interval = None
                com_interval = None

                for harvester_id in new_workers_per_harvester:
                    params = new_workers_per_harvester[harvester_id]
                    self.tbuf.commandToHarvester(
                        harvester_id,
                        command,
                        ack_requested,
                        status,
                        lock_interval,
                        com_interval,
                        params,
                    )
            except Exception:
                self._logger.error(traceback.format_exc())

        # log the timing
        time_stop = time.time()
        self._logger.debug(f"Done. Pilot streaming took: {time_stop - time_start} s")

        return


# main
def main(tbuf=None, **kwargs):
    requester_id = GenericThread().get_full_id(__name__, sys.modules[__name__].__file__)

    # instantiate TB
    if tbuf is None:
        from pandaserver.taskbuffer.TaskBuffer import taskBuffer

        taskBuffer.init(
            panda_config.dbhost,
            panda_config.dbpasswd,
            nDBConnection=1,
            useTimeout=True,
            requester=requester_id,
        )
    else:
        taskBuffer = tbuf

    # run the pilot streaming logic
    PilotStreaming(tbuf=taskBuffer).run()

    # stop the taskBuffer if it was created inside this script
    if tbuf is None:
        taskBuffer.cleanup(requester=requester_id)


# run
if __name__ == "__main__":
    main()
