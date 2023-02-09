import threading
from pandaserver.configurator import aux
import traceback
from pandaserver.config import panda_config
from pandacommon.pandalogger.PandaLogger import PandaLogger

_logger = PandaLogger().getLogger('carbon')

class CarbonEmissions(threading.Thread):
    """
    Downloads the carbon information from the relevant sources
    """
    def __init__(self, taskBuffer):
        threading.Thread.__init__(self)

        self.bearer_token = None
        if hasattr(panda_config, 'CO2_BEARER_TOKEN'):
            self.bearer_token = panda_config.CO2_BEARER_TOKEN

        self.taskBuffer = taskBuffer

    def download_region_emissions(self):
        query = """
        {"search_type": "query_then_fetch","ignore_unavailable": true,"index": ["monit_prod_green-it_raw_regionmetric*"]}
        {"query": {"range": {"metadata.timestamp": {"gte": "now-2h","lt": "now"}}}, "size": 100}}
        """
        results = aux.query_grafana_proxy(query, self.bearer_token)
        status = results['responses']['status']
        if status != 200:
            _logger.error('download_region_emissions was not able to download data with status {0}'.format(status))
            return None
        try:
            hits = results['responses'][0]['hits']['hits']  # That's how the json is structured...
            results = []
            for entry in hits:
                simplified_entry = {
                    'region': entry['_source']['data']['region'],
                    'timestamp': entry['_source']['metadata']['timestamp'],
                    'value': entry['_source']['data']['gCO2_perkWh']
                }
                results.append(simplified_entry)
                #_logger.debug("time: {0}, region: {1}, gCO2_perkWh: {2}".format(timestamp, region, gCO2_perkWh))
                return results
        except Exception:
            _logger.error('download_region_emissions excepted with {0}'.format(traceback.print_exc()))
            return None

    def run(self):
        # download emissions and store them in the DB
        results = self.download_region_emissions()
        self.taskBuffer.carbon_write_region_emissions(results)