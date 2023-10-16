from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandaserver.test.testutils import sendCommand

_logger = PandaLogger().getLogger("testUpdateWorkerPilotStatus")

node = {}
node["workerID"] = 9139456
node["harvesterID"] = "CERN_central_k8s"
node["status"] = "started"
node["site"] = "CERN"

function = "updateWorkerPilotStatus"
data = sendCommand(function, node, _logger)
_logger.debug(data)
