from pandaserver.test.testutils import sendCommand

node = {}
node['workerID'] = 9139456
node['harvesterID'] = 'CERN_central_k8s'
node['status'] = 'started'

function = "updateWorkerPilotStatus"
data = sendCommand(function, node)
print(data)

