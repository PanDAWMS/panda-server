import urllib2
import json
import time
import threading
import sys

from sqlalchemy import exc

from config import panda_config
from pandalogger.PandaLogger import PandaLogger
import db_interface as dbif
from models import Site, PandaSite, DdmEndpoint, PandaDdmRelation

_logger = PandaLogger().getLogger('configurator')
_session = dbif.get_session()


class Configurator(threading.Thread):

    def __init__(self):
        threading.Thread.__init__(self)
        
        if hasattr(panda_config,'AGIS_URL_SITES'):
            self.AGIS_URL_SITES = panda_config.AGIS_URL_SITES
        else:
            self.AGIS_URL_SITES = 'http://atlas-agis-api.cern.ch/request/site/query/?json&vo_name=atlas'
         
        if hasattr(panda_config,'AGIS_URL_DDMENDPOINTS'):
             self.AGIS_URL_DDMENDPOINTS = panda_config.AGIS_URL_DDMENDPOINTS
        else:
            self.AGIS_URL_DDMENDPOINTS = 'http://atlas-agis-api.cern.ch/request/ddmendpoint/query/list/?json'


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
        
        #TODO: parse out any other fields Tadashi needs
        return (name, role)


    def parse_endpoints(self, endpoint_dump):
        """
        Puts the relavant information from endpoint_dump into a more usable format 
        """
        endpoint_token_dict = {}
        for endpoint in endpoint_dump:
            endpoint_token_dict[endpoint['name']] = endpoint['token']
        return endpoint_token_dict


    def process_dumps(self):
        """
        Principal function. Parses the AGIS site and endpoint dumps for loading into the DB
        """
        
        #Get the list of sites
        site_dump = self.get_dump(self.AGIS_URL_SITES)
        
        #Get the list of endpoints and convert the info we need to a more usable format
        endpoint_dump = self.get_dump(self.AGIS_URL_DDMENDPOINTS)
        endpoint_token_dict = self.parse_endpoints(endpoint_dump)
        
        #Variables that will contain only the relevant information
        sites_list = []
        included_sites = []
        ddm_endpoints_list = []
        panda_sites_list = []
        
        #Iterate the site dump
        for site in site_dump:
            #Add the site info to a list
            (site_name, site_role) = self.get_site_info(site)
            if site_name not in included_sites: #Avoid duplicate entries
                sites_list.append({'site_name': site_name, 'role': site_role})
                included_sites.append(site_name)
            
            #Get the DDM endpoints for the site we are inspecting
            for ddm_endpoint_name in site['ddmendpoints']:
                
                try:
                    ddm_spacetoken_name = endpoint_token_dict[ddm_endpoint_name]
                except KeyError:
                    ddm_spacetoken_name = None
                
                ddm_endpoints_list.append({'ddm_endpoint_name': ddm_endpoint_name, 'site_name': site_name, 'ddm_spacetoken_name': ddm_spacetoken_name})
                
                
            #Get the PanDA resources 
            for panda_resource in site['presources']:
                for panda_site in site['presources'][panda_resource]:
                    panda_site_name = panda_site
                    panda_queue_name = None
                    for panda_queue in site['presources'][panda_resource][panda_site]['pandaqueues']:
                        panda_queue_name = panda_queue['name']
                    panda_sites_list.append({'panda_site_name': panda_site_name, 'panda_queue_name': panda_queue_name, 'site_name': site_name, 'datapolicies': site_role})
        
        #Persist the information to the PanDA DB
        self.write_sites_db(sites_list)
        self.write_panda_sites_db(panda_sites_list)
        self.write_ddm_endpoints_db(ddm_endpoints_list)
        
        return True


    def write_sites_db(self, sites_list):
        """
        Cache the AGIS site information in the PanDA database
        """
        try: #TODO: Improve this error handling. Consider writing a decorator
            for site in sites_list:
                _session.merge(Site(site_name = site['site_name'], 
                                          datapolicies = site['role']))
            _session.flush()
            _session.commit()
        except exc.SQLAlchemyError:
            _logger.critical('write_sites_db: Could not persist information --> {0}'.format(sys.exc_info()))


    def write_panda_sites_db(self, panda_sites_list):
        """
        Cache the AGIS panda site information in the PanDA database
        """
        try: #TODO: Improve this error handling. Consider writing a decorator
            for panda_site in panda_sites_list:
                _session.merge(PandaSite(panda_site_name = panda_site['panda_site_name'], 
                                                     site_name = panda_site['site_name'], 
                                                     datapolicies = panda_site['datapolicies']))
            _session.flush()
            _session.commit()
        except  exc.SQLAlchemyError:
            _logger.critical('write_panda_sites_db: Could not persist information --> {0}'.format(sys.exc_info()))


    def write_ddm_endpoints_db(self, ddm_endpoints_list):
        """
        Cache the AGIS ddm endpoints in the PanDA database
        """
        try: #TODO: Improve this error handling. Consider writing a decorator
            for ddm_endpoint in ddm_endpoints_list:
                _session.merge(DdmEndpoint(ddm_endpoint_name = ddm_endpoint['ddm_endpoint_name'], 
                                            site_name = ddm_endpoint['site_name'], 
                                            ddm_spacetoken_name = ddm_endpoint['ddm_spacetoken_name']))
            _session.flush()
            _session.commit()
        except exc.SQLAlchemyError:
            _logger.critical('write_ddm_endpoints_db: Could not persist information --> {0}'.format(sys.exc_info()))


if __name__ == "__main__":
    t1 = time.time()
    configurator = Configurator()
    if not configurator.process_dumps():
        _logger.critical("Configurator loop FAILED")
    t2 = time.time()
    _logger.debug("Processing AGIS dumps took {0}s".format(t2-t1))
    
