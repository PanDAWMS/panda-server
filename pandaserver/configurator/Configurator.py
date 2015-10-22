import urllib2
import json
import time
import threading
import sys

from sqlalchemy import exc

from config import panda_config
from pandalogger.PandaLogger import PandaLogger
import db_interface as dbif
from configurator.models import Schedconfig

_logger = PandaLogger().getLogger('configurator')
_session = dbif.get_session()


class Configurator(threading.Thread):

    def __init__(self):
        threading.Thread.__init__(self)

        if hasattr(panda_config,'AGIS_URL_SITES'):
            self.AGIS_URL_SITES = panda_config.AGIS_URL_SITES
        else:
            self.AGIS_URL_SITES = 'http://atlas-agis-api.cern.ch/request/site/query/?json&vo_name=atlas'
        _logger.debug('Getting site dump...')
        self.site_dump = self.get_dump(self.AGIS_URL_SITES)
        _logger.debug('Done')

        if hasattr(panda_config,'AGIS_URL_DDMENDPOINTS'):
             self.AGIS_URL_DDMENDPOINTS = panda_config.AGIS_URL_DDMENDPOINTS
        else:
            self.AGIS_URL_DDMENDPOINTS = 'http://atlas-agis-api.cern.ch/request/ddmendpoint/query/list/?json'
        _logger.debug('Getting DDM endpoints dump...')
        self.endpoint_dump = self.get_dump(self.AGIS_URL_DDMENDPOINTS)
        _logger.debug('Done')
        _logger.debug('Parsing endpoints...')
        self.endpoint_token_dict = self.parse_endpoints()
        _logger.debug('Done')

        if hasattr(panda_config,'AGIS_URL_SCHEDCONFIG'):
             self.AGIS_URL_SCHEDCONFIG = panda_config.AGIS_URL_SCHEDCONFIG
        else:
            self.AGIS_URL_SCHEDCONFIG = 'http://atlas-agis-api.cern.ch/request/pandaqueue/query/list/?json&preset=schedconf.all&vo_name=atlas'
        _logger.debug('Getting schedconfig dump...')
        self.schedconfig_dump = self.get_dump(self.AGIS_URL_SCHEDCONFIG)
        _logger.debug('Done')
        

    def get_dump(self, url):
        response = urllib2.urlopen(url)
        json_str = response.read()
        dump = json.loads(json_str)
        return dump


    def get_site_info(self, site):
        """
        Gets the relevant information from a site
        """
        
        name = site['rc_site']
        
        #TODO: Think about the best way to store this information, also considering future requests
        if 'TaskNucleus' in site['datapolicies'] or site['rc_tier_level'] <= 1:
            role = 'nucleus'
        else:
            role = 'satelite'
        
        state = site['rc_site_state']
        
        return (name, role, state)


    def parse_endpoints(self):
        """
        Puts the relevant information from endpoint_dump into a more usable format 
        """
        endpoint_token_dict = {}
        for endpoint in self.endpoint_dump:
            endpoint_token_dict[endpoint['name']] = {}
            endpoint_token_dict[endpoint['name']]['token'] = endpoint['token']
            endpoint_token_dict[endpoint['name']]['site_name'] = endpoint['rc_site']
        return endpoint_token_dict


    def process_site_dumps(self):
        """
        Parses the AGIS site and endpoint dumps and prepares a format loadable to the DB
        """        
        #Variables that will contain only the relevant information
        sites_list = []
        included_sites = []
        ddm_endpoints_list = []
        panda_sites_list = []
        
        #Iterate the site dump
        for site in self.site_dump:
            #Add the site info to a list
            (site_name, site_role, site_state) = self.get_site_info(site)
            if site_name not in included_sites: #Avoid duplicate entries
                sites_list.append({'site_name': site_name, 'role': site_role, 'state': site_state})
                included_sites.append(site_name)
            
            #Get the DDM endpoints for the site we are inspecting
            for ddm_endpoint_name in site['ddmendpoints']:
                
                try:
                    ddm_spacetoken_name = self.endpoint_token_dict[ddm_endpoint_name]['token']
                except KeyError:
                    ddm_spacetoken_name = None
                    
                ddm_spacetoken_state = site['ddmendpoints'][ddm_endpoint_name]['state']
                
                ddm_endpoints_list.append({'ddm_endpoint_name': ddm_endpoint_name, 'site_name': site_name, 'ddm_spacetoken_name': ddm_spacetoken_name, 'state': ddm_spacetoken_state})

            #Get the PanDA resources 
            for panda_resource in site['presources']:
                for panda_site in site['presources'][panda_resource]:
                    panda_site_name = panda_site
                    panda_site_state = site['presources'][panda_resource][panda_site]['state']
                    panda_queue_name = None
                    for panda_queue in site['presources'][panda_resource][panda_site]['pandaqueues']:
                        panda_queue_name = panda_queue['name']
                    panda_sites_list.append({'panda_site_name': panda_site_name, 'panda_queue_name': panda_queue_name, 'site_name': site_name, 'state': panda_site_state})
        
        return sites_list, panda_sites_list, ddm_endpoints_list


    def process_schedconfig_dump(self):
        """
        Gets PanDA site to DDM endpoint relationships from Schedconfig 
        and prepares a format loadable to the DB  
        """

        #relationship_tuples = dbif.read_panda_ddm_relationships_schedconfig(_session) #data almost as it comes from schedconfig
        relationships_list = [] #data to be loaded to configurator DB 
        
        for long_panda_site_name in self.schedconfig_dump:
            
            panda_site_name = self.schedconfig_dump[long_panda_site_name]['panda_resource']
            
            ddm_endpoints = [ddm_endpoint.strip() for ddm_endpoint in self.schedconfig_dump[long_panda_site_name]['ddm'].split(',')]
            _logger.debug('panda_site_name: {0}. DDM endopints: {1}'.format(panda_site_name, ddm_endpoints))
            count = 0
            for ddm_endpoint_name in ddm_endpoints:
                try:
                    #The first DDM endpoint in the list should be the primary
                    if count == 0:
                        is_default = 'Y'
                    else:
                        is_default = 'N'
                    
                    #Check if the ddm_endpoint and the panda_site belong to the same site
                    site_name_endpoint = self.endpoint_token_dict[ddm_endpoint_name]['site_name']
                    site_name_pandasite = self.schedconfig_dump[long_panda_site_name]['rc_site']
                    if site_name_endpoint == site_name_pandasite:
                        is_local = 'Y'
                    else:
                        is_local = 'N'
                     
                    relationships_list.append({'panda_site_name': panda_site_name, 
                                               'ddm_endpoint_name': ddm_endpoint_name,
                                               'is_default': is_default,
                                               'is_local': is_local})
                    count += 1
                except KeyError:
                    _logger.debug('DDM endpoint {0} could not be found'.format(ddm_endpoint_name))
        
        return relationships_list
    
    
    def data_quality_check(self):
        """
        Point out sites, panda sites and DDM endpoints that are missing in one of the sources 
        """
        #Check for site inconsistencies
        agis_sites = (self.site_dump[site]['rc_site'] for site in self.site_dump)
        configurator_sites = dbif.read_configurator_sites(_session)
        schedconfig_sites = dbif.read_schedconfig_sites(_session)
        
        inconsistencies_agis_schedconfig = agis_sites - schedconfig_sites
        if inconsistencies_agis_schedconfig:
            _logger.error("Following sites are in AGIS but not in schedconfig: {0}".format(inconsistencies_agis_schedconfig))

        inconsistencies_schedconfig_agis = schedconfig_sites - agis_sites
        if inconsistencies_schedconfig_agis:
            _logger.error("Following sites are in schedconfig but not in AGIS: {0}".format(inconsistencies_schedconfig_agis))

        inconsistencies_agis_configurator = agis_sites - configurator_sites
        if inconsistencies_agis_configurator:
            _logger.error("Following sites are in AGIS but not in configurator: {0}".format(inconsistencies_agis_configurator))

        inconsistencies_configurator_agis = configurator_sites - agis_sites
        if inconsistencies_configurator_agis:
            _logger.error("Following sites are in schedconfig but not in AGIS: {0}".format(inconsistencies_configurator_agis))

        #Check for panda-site inconsistencies
        
        
        #Check for DDM endpoint inconsistencies
        
        
        #Print out results


    def run(self):
        """
        Principal function
        """
        #Get pre-processed AGIS dumps
        sites_list, panda_sites_list, ddm_endpoints_list = self.process_site_dumps()

        #Get pre-processed PanDA site to DDM endpoint relationships from Schedconfig
        relationships_list = self.process_schedconfig_dump()

        #Persist the information to the PanDA DB
        dbif.write_sites_db(_session, sites_list)
        dbif.write_panda_sites_db(_session, panda_sites_list)
        dbif.write_ddm_endpoints_db(_session, ddm_endpoints_list)
        dbif.write_panda_ddm_relations(_session, relationships_list)

        return True


if __name__ == "__main__":
    t1 = time.time()
    configurator = Configurator()
    if not configurator.run():
        _logger.critical("Configurator loop FAILED")
    t2 = time.time()
    _logger.debug("Configurator run took {0}s".format(t2-t1))
    
