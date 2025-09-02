from pandajedi.jedicore.JediTaskBufferInterface import JediTaskBufferInterface
from pandajedi.jediorder.JobThrottler import JobThrottler

tbIF = JediTaskBufferInterface()
tbIF.setupInterface()

vo = "atlas"
prodSourceLabel = "managed"
cloud = "WORLD"

# get SiteMapper
siteMapper = tbIF.get_site_mapper()
wqMap = tbIF.getWorkQueueMap()

jt = JobThrottler(vo, prodSourceLabel)
jt.initializeMods(tbIF)

workQueues = wqMap.getAlignedQueueList(vo, prodSourceLabel)
resource_types = tbIF.load_resource_types()
for workQueue in workQueues:
    for resource_type in resource_types:
        print(jt.toBeThrottled(vo, prodSourceLabel, cloud, workQueue, resource_type.resource_name))
