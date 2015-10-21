import urllib2
import json
import time
import threading
import sys

from sqlalchemy import exc

from config import panda_config
from pandalogger.PandaLogger import PandaLogger
import db_interface as dbif

_logger = PandaLogger().getLogger('configurator')
_session = dbif.get_session()


class Configurator(threading.Thread):

    def __init__(self):
        threading.Thread.__init__(self)

        if hasattr(panda_config,'AGIS_URL_SITES'):
            self.AGIS_URL_SITES = panda_config.AGIS_URL_SITES
        else:
            self.AGIS_URL_SITES = 'http://atlas-agis-api.cern.ch/request/site/query/?json&vo_name=atlas'
        self.site_dump = self.get_dump(self.AGIS_URL_SITES)

        if hasattr(panda_config,'AGIS_URL_DDMENDPOINTS'):
             self.AGIS_URL_DDMENDPOINTS = panda_config.AGIS_URL_DDMENDPOINTS
        else:
            self.AGIS_URL_DDMENDPOINTS = 'http://atlas-agis-api.cern.ch/request/ddmendpoint/query/list/?json'
        self.endpoint_dump = self.get_dump(self.AGIS_URL_DDMENDPOINTS)
        self.endpoint_token_dict = self.parse_endpoints()

        if hasattr(panda_config,'AGIS_URL_SCHEDCONFIG'):
             self.AGIS_URL_SCHEDCONFIG = panda_config.AGIS_URL_SCHEDCONFIG
        else:
            self.AGIS_URL_SCHEDCONFIG = 'http://atlas-agis-api.cern.ch/request/pandaqueue/query/list/?json&preset=schedconf.all&vo_name=atlas'
        self.schedconfig_dump = self.get_dump(self.AGIS_URL_SCHEDCONFIG)
        

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
        if 'TaskNucleus' in site['datapolicies'] or site['rc_tier_level'] == 1:
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


    def process_dumps(self):
        """
        Parses the AGIS site and endpoint dumps 
        and prepares a format loadable to the DB
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


    def process_schedconfig(self):
        """
        Gets PanDA site to DDM endpoint relationships from Schedconfig 
        and prepares a format loadable to the DB  
        """

        #relationship_tuples = dbif.read_panda_ddm_relationships_schedconfig(_session) #data almost as it comes from schedconfig
        relationships_list = [] #data to be loaded to configurator DB 
        
        for panda_site in self.schedconfig_dump:
            count = 0
            ddm_endpoints = [ddm_endpoint.strip() for ddm_endpoint in self.schedconfig_dump[panda_site]['ddm'].split(',')]
            for ddm_endpoint_name in ddm_endpoints:
                #The first DDM endpoint in the list should be the primary
                if count == 0:
                    is_default = 'Y'
                else:
                    is_default = 'N'
                
                #Check if the ddm_endpoint and the panda_site belong to the same site
                site_name_endpoint = self.endpoint_token_dict[ddm_endpoint_name]['site_name']
                site_name_pandasite = self.schedconfig_dump[panda_site]['rc_site']
                if site_name_endpoint == site_name_pandasite:
                    is_local = 'Y'
                else:
                    is_local = 'N'
                 
                relationships_list.append({'panda_site_name': panda_site_name, 
                                           'ddm_endpoint_name': ddm_endpoint_name,
                                           'is_default': is_default,
                                           'is_local': is_local})
                count += 1
        
        return relationships_list


    def run(self):
        """
        Principal function
        """
        try:
            #Get pre-processed AGIS dumps
            sites_list, panda_sites_list, ddm_endpoints_list = self.process_dumps()
            
            #Get pre-processed PanDA site to DDM endpoint relationships from Schedconfig
            relationships_list = self.process_schedconfig()
            
            #Persist the information to the PanDA DB
            dbif.write_sites_db(_session, sites_list)
            dbif.write_panda_sites_db(_session, panda_sites_list)
            dbif.write_ddm_endpoints_db(_session, ddm_endpoints_list)
            dbif.write_panda_ddm_relations(_session, relationships_list)
            
            return True
        except:
            _logger.critical('process_dumps: Excepted with --> {0}'.format(sys.exc_info()))
            return False


if __name__ == "__main__":
    t1 = time.time()
    configurator = Configurator()
    if not configurator.run():
        _logger.critical("Configurator loop FAILED")
    t2 = time.time()
    _logger.debug("Configurator run took {0}s".format(t2-t1))
    
