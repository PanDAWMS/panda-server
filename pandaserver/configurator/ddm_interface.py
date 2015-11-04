import sys

from rucio.client import Client as RucioClient

from pandalogger.PandaLogger import PandaLogger

_logger = PandaLogger().getLogger('configurator_ddm_interface')
_client = RucioClient()
GB = 1024**3

def get_rse_usage(rse):
    """
    Gets disk usage at RSE (Rucio Storage Element)
    """
    method_name = "get_rse_usage <rse={0}>".format(rse)
    _logger.debug('{0} start'.format(method_name))
    
    rse_usage = {}
    try:
        rse_usage_itr = _client.get_rse_usage(rse)
        #Look for the specified information source
        for item in rse_usage_itr:
            if item['source'] == 'srm':
                try:
                    rse_usage['total'] = item['total']/GB
                except KeyError:
                    rse_usage['total'] = None
                
                try:
                    rse_usage['used'] = item['used']/GB
                except KeyError:
                    rse_usage['used'] = None
                
                try:
                    rse_usage['free'] = item['free']/GB
                except KeyError:
                    rse_usage['free'] = None
                    
                try:
                    rse_usage['space_timestamp'] = item['updated_at']
                except KeyError:
                    rse_usage['space_timestamp'] = None

            if item['source'] == 'expired':
                try:
                    rse_usage['expired'] = item['used']/GB
                except KeyError:
                    rse_usage['expired'] = None
    except:
        _logger.error('{0} Excepted with: {1}'.format(method_name, sys.exc_info()))
        return {}
    
    _logger.debug('{0} done {1}'.format(method_name, rse_usage))
    return rse_usage