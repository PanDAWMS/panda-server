# error code

# killed
EC_Kill = 100

# transfer timeout
EC_Transfer = 101

# expire
EC_Expire = 102

# aborted
EC_Aborted = 103

# reassigned by re-brokerage
EC_Reassigned = 105

# reassigned by server-side retry
EC_Retried = 106

# retried by pilot
EC_PilotRetried = 107

# retried for event service
EC_EventServiceRetried = 111

# merge for event service
EC_EventServiceMerge = 112

# merge job failed
EC_MergeFailed = 113

# max attempt reached for Event Service
EC_EventServiceMaxAttempt = 114

# do nothing since other consumers are still running
EC_EventServiceWaitOthers = 115

# killed since unused and unnecessary anymore
EC_EventServiceUnused = 116

# didn't process any events on WN
EC_EventServiceUnprocessed = 117

# didn't process any events on WN and last consumer
EC_EventServiceLastUnprocessed = 118

# all event ranges failed
EC_EventServiceAllFailed = 119

# associated consumer generated ES merge
EC_EventServiceKillOK = 120

# associated consumer failed
EC_EventServiceKillNG = 121

# killed for preemption
EC_EventServicePreemption = 122

# retried but didn't process any events on WN
EC_EventServiceNoEvent = 123

# input files inconsistent with JEDI
EC_EventServiceInconsistentIn = 124

# No event service queues available for new consumers
EC_EventServiceNoEsQueues = 125

# Closed in bad job status
EC_EventServiceBadStatus = 126

# Fast re-brokerage for overloaded
EC_FastRebrokerage = 130

# failed to lock semaphore for job cloning
EC_JobCloningUnlock = 200

# worker is done before job is done
EC_WorkerDone = 300

# http error codes

# forbidden request
EC_Forbidden = 403

# file not found
EC_NotFound = 404
