import time
import traceback

from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandaserver.config import panda_config


# logger
_logger = PandaLogger().getLogger('worker_sync')


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

        status = 'new'
        ack_requested = False
        lock_interval = None
        com_interval = None

        try:
            stale_workers_per_harvester = self.tbuf.get_workers_to_synchronize()
            # variables for the harvester command
            command = '{0}'.format('WORKER_SYNC')

            for harvester_id in stale_workers_per_harvester:
                workers_instance = stale_workers_per_harvester[harvester_id]
                self._logger.debug('Processing harvester: {0}. Workers to update: {1}'.format(harvester_id,
                                                                                              len(workers_instance)))
                self.tbuf.commandToHarvester(harvester_id, command, ack_requested, status,
                                             lock_interval, com_interval, workers_instance)
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
        taskBuffer.init(panda_config.dbhost, panda_config.dbpasswd, nDBConnection=1)
    else:
        taskBuffer = tbuf
    # run
    WorkerSync(tbuf=taskBuffer).run()


# run
if __name__ == '__main__':
    main()
