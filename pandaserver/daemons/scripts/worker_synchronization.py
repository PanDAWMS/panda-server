import time
import traceback

from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandaserver.config import panda_config
from pandaserver.taskbuffer.Utils import create_shards


# logger
_logger = PandaLogger().getLogger('worker_sync')


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
        self._logger.debug('Start.')

        # variables for the harvester command
        status = 'new'
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
                            self._logger.debug('Processing harvester_id={0} pilot_status={1}. Workers to update: {2}'.
                                               format(harvester_id, pilot_status, worker_shard))
                            self.tbuf.commandToHarvester(harvester_id, command, ack_requested, status,
                                                         lock_interval, com_interval, worker_shard)
        except Exception:
            self._logger.error(traceback.format_exc())

        # timing
        time_stop = time.time()
        self._logger.debug('Done. Worker sync took: {0} s'.format(time_stop - time_start))

        return


# main
def main(tbuf=None, **kwargs):
    # instantiate TB
    if tbuf is None:
        from pandaserver.taskbuffer.TaskBuffer import taskBuffer
        taskBuffer.init(panda_config.dbhost, panda_config.dbpasswd, nDBConnection=1, useTimeout=True)
    else:
        taskBuffer = tbuf
    # run
    WorkerSync(tbuf=taskBuffer).run()


# run
if __name__ == '__main__':
    main()
