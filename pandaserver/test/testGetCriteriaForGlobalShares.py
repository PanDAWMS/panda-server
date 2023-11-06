import socket
import time

from pandaserver.config import panda_config
from pandaserver.taskbuffer.OraDBProxy import DBProxy

try:
    from urlparse import parse_qs
except ImportError:
    from urllib.parse import parse_qs

from pandacommon.pandalogger.PandaLogger import PandaLogger
from testutils import sendCommand

_logger = PandaLogger().getLogger("testGetCriteriaGlobalShares")


def retrieveJob(site):
    function = "getJob"
    node = {}
    node["siteName"] = site
    node["mem"] = 1000
    node["node"] = socket.getfqdn()

    data = sendCommand(function, node, _logger)
    jobD = parse_qs(data)  # jobD indicates it's a job in dictionary format, not a JobSpec object
    return jobD


if __name__ == "__main__":
    proxyS = DBProxy()
    proxyS.connect(
        panda_config.dbhost,
        panda_config.dbpasswd,
        panda_config.dbuser,
        panda_config.dbname,
    )

    # proxyS.getCriteriaForGlobalShares('BNL-OSG')

    site = "CERN-PROD"

    DIRECT = "direct"
    WEB = "web"

    mode = WEB

    if mode == DIRECT:
        for i in range(3):
            t_before = time.time()
            _logger.info(
                proxyS.getJobs(
                    1,
                    site,
                    "managed",
                    None,
                    1000,
                    0,
                    "aipanda081.cern.ch",
                    20,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                )
            )
            t_after = time.time()

            total = t_after - t_before
            _logger.info(f"Took {total}s")

    elif mode == WEB:
        job = retrieveJob(site)
        _logger.info(job)
