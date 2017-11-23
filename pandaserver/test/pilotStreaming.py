from pandalogger.PandaLogger import PandaLogger
from taskbuffer.TaskBuffer import taskBuffer
from config import panda_config

taskBuffer.init(panda_config.dbhost, panda_config.dbpasswd, nDBConnection=1)


class PilotStreaming:
    def __init__(self):
        self._logger = PandaLogger().getLogger('PilotStreaming')
        return

    def run(self):
        """
        Gets and iterates over ups queues, deciding the job requirements and sending these to Harvester
        via the command interface
        :return:
        """

        # get unified pilot streaming (ups) queues
        ups_queues = taskBuffer.ups_get_queues()
        self._logger.debug('UPS queues: {0}'.format(ups_queues))

        # get worker stats
        worker_stats = taskBuffer.ups_load_worker_stats()
        self._logger.debug('worker_stats: {0}'.format(worker_stats))

        # get global share distribution
        # hs_distribution = proxyS.get_hs_distribution()
        # gs_tree = proxyS
        # print(proxyS.tree.pretty_print_hs_distribution(proxyS._DBProxy__hs_distribution

        for ups_queue in ups_queues:
            # get the worker and job stats for the queue
            try:
                tmp_worker_stats = worker_stats[ups_queue]
                # tmp_job_stats = job_stats[ups_queue]
            except KeyError:
                # skip queue if no data available
                self._logger.debug('No worker stats for queue {0}'.format(ups_queue))
                continue

            new_workers_per_harvester = taskBuffer.ups_new_worker_distribution(ups_queue, tmp_worker_stats)

            # variables for the harvester command
            command = 'SET_N_WORKERS'
            status = 'new'
            ack_requested = False
            lock_interval = None
            com_interval = None

            for harvester_id in new_workers_per_harvester:
                params = new_workers_per_harvester[harvester_id]

                # TODO: figure out if a command lock call is necessary or how that works
                taskBuffer.commandToHarvester(harvester_id, command, ack_requested, status,
                                              lock_interval, com_interval, params)

        return


if __name__ == "__main__":
    PilotStreaming().run()
