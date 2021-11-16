from pandaserver.test.testutils import sendCommand

from pandacommon.pandalogger.PandaLogger import PandaLogger
_logger = PandaLogger().getLogger('testJobFlowATLAS')

node = {}
node['workerID'] = 9139456
node['harvesterID'] = 'CERN_central_k8s'
node['status'] = 'started'

function = "updateWorkerPilotStatus"
data = sendCommand(function, node, _logger)
_logger.debug(data)


