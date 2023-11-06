import datetime
import threading
import traceback

from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandaserver.config import panda_config
from pandaserver.configurator import aux

_logger = PandaLogger().getLogger("carbon")


class CarbonEmissions(threading.Thread):
    """
    Downloads the carbon information from the relevant sources
    """

    def __init__(self, taskBuffer):
        threading.Thread.__init__(self)

        self.bearer_token = None
        if hasattr(panda_config, "CO2_BEARER_TOKEN"):
            self.bearer_token = panda_config.CO2_BEARER_TOKEN

        self.taskBuffer = taskBuffer

    def download_region_emissions(self):
        # Don't indent the query
        query = """
{"search_type": "query_then_fetch","ignore_unavailable": true,"index": ["monit_prod_green-it_raw_regionmetric*"]}
{"query": {"range": {"metadata.timestamp": {"gte": "now-2h","lt": "now"}}}, "size": 100}}
"""
        try:
            results = aux.query_grafana_proxy(query, self.bearer_token)
            if not results:
                _logger.error("download_region_emissions was not able to download data")
                return None

            status = results["responses"][0]["status"]
            if status != 200:
                _logger.error(f"download_region_emissions was not able to download data with status {status}")
                return None

            hits = results["responses"][0]["hits"]["hits"]  # That's how the json is structured...
            results = []
            for entry in hits:
                simplified_entry = {
                    "region": entry["_source"]["data"]["region"],
                    # timestamps come in ms
                    "timestamp": datetime.datetime.fromtimestamp(entry["_source"]["metadata"]["timestamp"] / 1000),
                    "value": entry["_source"]["data"]["gCO2_perkWh"],
                }
                results.append(simplified_entry)
            return results
        except Exception:
            _logger.error(f"download_region_emissions excepted with {traceback.format_exc()}")
            return None

    def run(self):
        # download emissions and store them in the DB
        results = self.download_region_emissions()
        if results:
            self.taskBuffer.carbon_write_region_emissions(results)

            # aggregate the emissions for the grid
            self.taskBuffer.carbon_aggregate_emissions()
