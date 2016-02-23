import time
import threading
import sys
import aux
from aux import *
from datetime import datetime, timedelta

from sqlalchemy import exc

from config import panda_config
from pandalogger.PandaLogger import PandaLogger
import db_interface as dbif
from configurator.models import Schedconfig
from taskbuffer.TaskBuffer import taskBuffer

_logger = PandaLogger().getLogger('configurator')
_session = dbif.get_session()


class Configurator(threading.Thread):

    def __init__(self):
        threading.Thread.__init__(self)

        if hasattr(panda_config,'AGIS_URL_SITES'):
            self.AGIS_URL_SITES = panda_config.AGIS_URL_SITES
        else:
            self.AGIS_URL_SITES = 'http://atlas-agis-api.cern.ch/request/site/query/?json&vo_name=atlas&state=ACTIVE'
        _logger.debug('Getting site dump...')
        self.site_dump = aux.get_dump(self.AGIS_URL_SITES)
        _logger.debug('Done')
        self.site_endpoint_dict = self.get_site_endpoint_dictionary()

        if hasattr(panda_config,'AGIS_URL_DDMENDPOINTS'):
             self.AGIS_URL_DDMENDPOINTS = panda_config.AGIS_URL_DDMENDPOINTS
        else:
            self.AGIS_URL_DDMENDPOINTS = 'http://atlas-agis-api.cern.ch/request/ddmendpoint/query/list/?json&state=ACTIVE'
        _logger.debug('Getting DDM endpoints dump...')
        self.endpoint_dump = aux.get_dump(self.AGIS_URL_DDMENDPOINTS)
        _logger.debug('Done')
        _logger.debug('Parsing endpoints...')
        self.endpoint_token_dict = self.parse_endpoints()
        _logger.debug('Done')

        if hasattr(panda_config,'AGIS_URL_SCHEDCONFIG'):
             self.AGIS_URL_SCHEDCONFIG = panda_config.AGIS_URL_SCHEDCONFIG
        else:
            self.AGIS_URL_SCHEDCONFIG = 'http://atlas-agis-api.cern.ch/request/pandaqueue/query/list/?json&preset=schedconf.all&vo_name=atlas&state=ACTIVE'
        _logger.debug('Getting schedconfig dump...')
        self.schedconfig_dump = aux.get_dump(self.AGIS_URL_SCHEDCONFIG)
        _logger.debug('Done')

        if hasattr(panda_config,'AGIS_URL_DDMBLACKLIST'):
             self.AGIS_URL_DDMBLACKLIST = panda_config.AGIS_URL_DDMBLACKLIST
        else:
            self.AGIS_URL_DDMBLACKLIST = 'http://atlas-agis-api.cern.ch/request/ddmendpointstatus/query/list/?json&fstate=OFF&activity=w'
        _logger.debug('Getting schedconfig dump...')
        self.blacklisted_endpoints = aux.get_dump(self.AGIS_URL_DDMBLACKLIST).keys()
        _logger.debug('Blacklisted endpoints {0}'.format(self.blacklisted_endpoints))
        _logger.debug('Done')
        
        if hasattr(panda_config,'RUCIO_RSE_USAGE'):
             self.RUCIO_RSE_USAGE = panda_config.RUCIO_RSE_USAGE
        else:
            self.RUCIO_RSE_USAGE = 'https://rucio-hadoop.cern.ch/dumps/rse_usage/current.json'
        _logger.debug('Getting Rucio RSE usage dump...')
        self.rse_usage = aux.get_dump(self.RUCIO_RSE_USAGE)
        _logger.debug('Done')

    def get_site_info(self, site):
        """
        Gets the relevant information from a site
        """
        
        name = site['name']
        state = site['state']
        tier_level = site['tier_level']
        
        if 'Nucleus' in site['datapolicies']:  # or site['tier_level'] <= 1:
            role = 'nucleus'
        else:
            role = 'satellite'
        
        return (name, role, state, tier_level)

    def parse_endpoints(self):
        """
        Puts the relevant information from endpoint_dump into a more usable format
        """
        endpoint_token_dict = {}
        for endpoint in self.endpoint_dump:
            # Filter out testing and inactive endpoints
            if endpoint['type'] != 'TEST' and endpoint['state'] == 'ACTIVE': 
                endpoint_token_dict[endpoint['name']] = {}
                endpoint_token_dict[endpoint['name']]['token'] = endpoint['token']
                endpoint_token_dict[endpoint['name']]['site_name'] = endpoint['site']
                endpoint_token_dict[endpoint['name']]['type'] = endpoint['type']
                if endpoint['is_tape']:
                    endpoint_token_dict[endpoint['name']]['is_tape'] = 'Y'
                else:
                    endpoint_token_dict[endpoint['name']]['is_tape'] = 'N'
            else:
                _logger.debug('parse_endpoints: skipped endpoint {0} (type: {1}, state: {2})'
                              .format(endpoint['name'], endpoint['type'], endpoint['state']))

        return endpoint_token_dict

    def get_site_endpoint_dictionary(self):
        """
        Converts the AGIS site dump into a site dictionary containing the list of DDM endpoints for each site
        """
        site_to_endpoints_dict = {} 
        for site in self.site_dump:
            site_to_endpoints_dict[site['name']] = site['ddmendpoints'].keys()
        
        return site_to_endpoints_dict

    def process_site_dumps(self):
        """
        Parses the AGIS site and endpoint dumps and prepares a format loadable to the DB
        """
        # Variables that will contain only the relevant information
        sites_list = []
        included_sites = []
        ddm_endpoints_list = []
        panda_sites_list = []
        
        relationship_dict = self.process_schedconfig_dump()
        
        # Iterate the site dump
        for site in self.site_dump:
            # Add the site info to a list
            (site_name, site_role, site_state, tier_level) = self.get_site_info(site)
            if site_state == 'ACTIVE' and site_name not in included_sites:
                sites_list.append({'site_name': site_name,
                                   'role': site_role,
                                   'state': site_state,
                                   'tier_level': tier_level})
                included_sites.append(site_name)
            else:
                _logger.debug('process_site_dumps: skipped site {0} (state: {1})'.format(site_name, site_state))

            # Get the DDM endpoints for the site we are inspecting
            for ddm_endpoint_name in site['ddmendpoints']:
                
                try:
                    ddm_spacetoken_name = self.endpoint_token_dict[ddm_endpoint_name]['token']
                    ddm_endpoint_type = self.endpoint_token_dict[ddm_endpoint_name]['type']
                    ddm_endpoint_is_tape = self.endpoint_token_dict[ddm_endpoint_name]['is_tape']
                    if ddm_endpoint_name in self.blacklisted_endpoints:
                        ddm_endpoint_blacklisted = 'Y'
                        _logger.debug('process_site_dumps: endpoint {0} is blacklisted'.format(ddm_endpoint_name))
                    else:
                        ddm_endpoint_blacklisted = 'N'
                        _logger.debug('process_site_dumps: endpoint {0} is NOT blacklisted'.format(ddm_endpoint_name))
                except KeyError:
                    continue

                # Get the SRM space
                try: 
                    space_used = self.rse_usage[ddm_endpoint_name]['srm']['used']/GB
                    space_free = self.rse_usage[ddm_endpoint_name]['srm']['free']/GB
                    space_total = space_used + space_free
                    space_timestamp = datetime.strptime(self.rse_usage[ddm_endpoint_name]['srm']['updated_at'],
                                                        '%Y-%m-%d %H:%M:%S')
                except KeyError:
                    space_used, space_free, space_total, space_timestamp = None, None, None, None
                    _logger.error('process_site_dumps: no rse SRM usage information for {0}'.format(ddm_endpoint_name))

                # Get the Expired space
                try:
                    space_expired = self.rse_usage[ddm_endpoint_name]['expired']['used']/GB
                except KeyError:
                    _logger.error('process_site_dumps: no rse EXPIRED usage information for {0}'
                                  .format(ddm_endpoint_name))

                ddm_spacetoken_state = site['ddmendpoints'][ddm_endpoint_name]['state']
                if ddm_spacetoken_state == 'ACTIVE':
                    ddm_endpoints_list.append({'ddm_endpoint_name': ddm_endpoint_name, 
                                               'site_name': site_name, 
                                               'ddm_spacetoken_name': ddm_spacetoken_name, 
                                               'state': ddm_spacetoken_state,
                                               'type': ddm_endpoint_type,
                                               'is_tape': ddm_endpoint_is_tape,
                                               'blacklisted': ddm_endpoint_blacklisted,
                                               'space_used': space_used,
                                               'space_free': space_free,
                                               'space_total': space_total,
                                               'space_expired': space_expired,
                                               'space_timestamp': space_timestamp
                                               })
                    _logger.debug('process_site_dumps: added DDM endpoint {0}'.format(ddm_endpoint_name))
                else:
                    _logger.debug('process_site_dumps: skipped DDM endpoint {0} because of state {1}'
                                  .format(ddm_endpoint_name, ddm_spacetoken_state))

            # Get the PanDA resources
            for panda_resource in site['presources']:
                for panda_site in site['presources'][panda_resource]:
                    panda_site_state = site['presources'][panda_resource][panda_site]['state']
                    if panda_site_state != 'ACTIVE':
                        _logger.debug('process_site_dumps: skipped PanDA site {0} (state: {1})'
                                      .format(panda_site, panda_site_state))
                        continue
                    panda_site_name = panda_site
                    panda_queue_name = None
                    for panda_queue in site['presources'][panda_resource][panda_site]['pandaqueues']:
                        panda_queue_name = panda_queue['name']

                    # Get information regarding relationship to storage
                    try:
                        relationship_info = relationship_dict[panda_site_name]
                        default_ddm_endpoint = relationship_info['default_ddm_endpoint']
                        storage_site_name = relationship_info['storage_site_name']
                        is_local = relationship_info['is_local']
                    except KeyError:
                        _logger.error('process_site_dumps: Investigate why panda_site_name {0} not in relationship_info dictionary'
                                      .format(panda_site_name))
                        default_ddm_endpoint = None
                        storage_site_name = None
                        is_local = None

                    panda_sites_list.append({'panda_site_name': panda_site_name,
                                             'panda_queue_name': panda_queue_name,
                                             'site_name': site_name,
                                             'state': panda_site_state,
                                             'default_ddm_endpoint': default_ddm_endpoint,
                                             'storage_site_name': storage_site_name,
                                             'is_local': is_local})
        
        return sites_list, panda_sites_list, ddm_endpoints_list

    def process_schedconfig_dump(self):
        """
        Gets PanDA site to DDM endpoint relationships from Schedconfig 
        and prepares a format loadable to the DB
        """

        # relationship_tuples = dbif.read_panda_ddm_relationships_schedconfig(_session) #data almost as it comes from schedconfig
        relationships_dict = {} # data to be loaded to configurator DB
        
        for long_panda_site_name in self.schedconfig_dump:
            
            panda_site_name = self.schedconfig_dump[long_panda_site_name]['panda_resource']
            
            default_ddm_endpoint = [ddm_endpoint.strip() for ddm_endpoint in self.schedconfig_dump[long_panda_site_name]['ddm'].split(',')][0]
            try:
                storage_site_name = self.endpoint_token_dict[default_ddm_endpoint]['site_name']
            except KeyError:
                _logger.warning("Skipped {0}, because primary associated DDM endpoint {1} not found (e.g. in TEST mode or DISABLED)"
                                .format(long_panda_site_name, default_ddm_endpoint))
                continue

            # Check if the ddm_endpoint and the panda_site belong to the same site
            cpu_site = self.schedconfig_dump[long_panda_site_name]['atlas_site']
            if storage_site_name == cpu_site \
                    and not self.schedconfig_dump[long_panda_site_name]['resource_type'] in ['cloud', 'hpc']:
                is_local = 'Y'
            else:
                is_local = 'N'
            
            _logger.debug("process_schedconfig_dump: long_panda_site_name {0}, panda_site_name {1}, default_ddm_endpoint {2}, storage_site_name {3}, is_local {4}"
                          .format(long_panda_site_name, panda_site_name, default_ddm_endpoint, storage_site_name, is_local))
            relationships_dict[panda_site_name] = {'default_ddm_endpoint': default_ddm_endpoint,
                                                   'storage_site_name': storage_site_name,
                                                   'is_local': is_local}

        return relationships_dict

    def consistency_check(self):
        """
        Point out sites, panda sites and DDM endpoints that are missing in one of the sources 
        """
        # Check for site inconsistencies
        agis_sites = set([site['name'] for site in self.site_dump if site['state']=='ACTIVE'])
        _logger.debug("Sites in AGIS {0}".format(agis_sites))
        configurator_sites = dbif.read_configurator_sites(_session)
        _logger.debug("Sites in Configurator {0}".format(configurator_sites))
        schedconfig_sites = dbif.read_schedconfig_sites(_session)
        _logger.debug("Sites in Schedconfig {0}".format(schedconfig_sites))
        
        all_sites = list(agis_sites | configurator_sites | schedconfig_sites)
        all_sites.sort()
        
        for site in all_sites:
            missing = []
            if site not in agis_sites:
                missing.append('AGIS')
            if site not in configurator_sites:
                missing.append('Configurator')
            if site not in schedconfig_sites:
                missing.append('Schedconfig')
            if missing:
                _logger.error("SITE inconsistency: {0} was not found in {1}".format(site, missing))

        # Check for panda-site inconsistencies
        agis_panda_sites = set([self.schedconfig_dump[long_panda_site_name]['panda_resource']
                                for long_panda_site_name in self.schedconfig_dump])
        _logger.debug("PanDA sites in AGIS {0}".format(agis_panda_sites))
        configurator_panda_sites = dbif.read_configurator_panda_sites(_session)
        _logger.debug("PanDA sites in Configurator {0}".format(configurator_panda_sites))
        schedconfig_panda_sites = dbif.read_schedconfig_panda_sites(_session)
        _logger.debug("PanDA sites in Schedconfig {0}".format(schedconfig_panda_sites))

        all_panda_sites = list(agis_panda_sites | configurator_panda_sites | schedconfig_panda_sites)
        all_panda_sites.sort()
        
        for site in all_panda_sites:
            missing = []
            if site not in agis_panda_sites:
                missing.append('AGIS')
            if site not in configurator_panda_sites:
                missing.append('Configurator')
            if site not in schedconfig_panda_sites:
                missing.append('Schedconfig')
            if missing:
                _logger.error("PanDA SITE inconsistency: {0} was not found in {1}".format(site, missing))

        # Check for DDM endpoint inconsistencies
        agis_ddm_endpoints = set([ddm_endpoint_name for ddm_endpoint_name in self.endpoint_token_dict])
        _logger.debug("DDM endpoints in AGIS {0}".format(agis_ddm_endpoints))
        configurator_ddm_endpoints = dbif.read_configurator_ddm_endpoints(_session)
        _logger.debug("DDM endpoints in Configurator {0}".format(configurator_ddm_endpoints))

        all_ddm_endpoints = list(agis_ddm_endpoints | configurator_ddm_endpoints)
        all_ddm_endpoints.sort()

        for site in all_ddm_endpoints:
            missing = []
            if site not in agis_ddm_endpoints:
                missing.append('AGIS')
            if site not in configurator_ddm_endpoints:
                missing.append('Configurator')
            if missing:
                _logger.error("DDM ENDPOINT inconsistency: {0} was not found in {1}".format(site, missing))

        self.cleanup_configurator(agis_sites, agis_panda_sites, agis_ddm_endpoints, configurator_sites,
                                  configurator_panda_sites, configurator_ddm_endpoints)

    def cleanup_configurator(self, agis_sites, agis_panda_sites, agis_ddm_endpoints, configurator_sites,
                             configurator_panda_sites, configurator_ddm_endpoints):
        """
        Cleans up information from configurator that is not in AGIS
        """
        if not agis_sites or not agis_panda_sites or not agis_ddm_endpoints:
            _logger.warning("Exiting cleanup because one of AGIS sets was empty")
        
        # Clean up sites
        sites_to_delete = configurator_sites - agis_sites
        dbif.delete_sites(_session, sites_to_delete)
        
        # Clean up panda sites
        panda_sites_to_delete = configurator_panda_sites - agis_panda_sites
        dbif.delete_panda_sites(_session, panda_sites_to_delete)
        
        # Clean up DDM endpoints
        ddm_endpoints_to_delete = configurator_ddm_endpoints - agis_ddm_endpoints
        dbif.delete_ddm_endpoints(_session, ddm_endpoints_to_delete)

    def get_corepower_and_cleanup(self):
        """
        Get the snapshot of the current corepower provided by each site. Delete old stats
        """
        dbif.get_cores_by_site(_session)
        dbif.clean_site_stats_key(_session, 'total_corepower', days=7)

    def run(self):
        """
        Principal function
        """
        # Get pre-processed AGIS dumps
        sites_list, panda_sites_list, ddm_endpoints_list = self.process_site_dumps()

        # Persist the information to the PanDA DB
        dbif.write_sites_db(_session, sites_list)
        dbif.write_panda_sites_db(_session, panda_sites_list)
        dbif.write_ddm_endpoints_db(_session, ddm_endpoints_list)
        # dbif.write_panda_ddm_relations(_session, relationships_list)

        #Get a snapshot of the corecount usage by site
        self.get_corepower_and_cleanup()

        # Do a data quality check
        self.consistency_check()
        
        return True

class NetworkConfigurator(threading.Thread):

    def __init__(self):
        threading.Thread.__init__(self)

        taskBuffer.init(panda_config.dbhost, panda_config.dbpasswd, nDBConnection=1)

        if hasattr(panda_config,'NWS_URL'):
            self.NWS_URL = panda_config.NWS_URL
        else:
            self.NWS_URL = 'http://rucio-nagios-prod/ett/latest.json'
        _logger.debug('Getting NWS dump...')
        self.nws_dump = aux.get_dump(self.NWS_URL)
        _logger.debug('Done')

        if hasattr(panda_config,'NWS_FULL_URL'):
            self.NWS_FULL_URL = panda_config.NWS_URL
        else:
            self.NWS_FULL_URL = 'http://aianalytics13.cern.ch/metrics/latest.json'
        _logger.debug('Getting NWS FULL dump...')
        self.nws_full_dump = aux.get_dump(self.NWS_FULL_URL)
        _logger.debug('Done')

        if hasattr(panda_config, 'AGIS_URL_CM'):
            self.AGIS_URL_CM = panda_config.AGIS_URL_CM
        else:
            self.AGIS_URL_CM = 'http://atlas-agis-api.cern.ch/request/site/query/list_links/?json'
        _logger.debug('Getting AGIS cost matrix dump...')
        self.agis_cm_dump = aux.get_dump(self.AGIS_URL_CM)
        _logger.debug('Done')

    def process_nws_dump(self):
        """
        Gets the NWS information dump, filters out irrelevant information
        and prepares it for insertion into the PanDA DB
        """

        data = []

        # Ignore outdated values
        latest_validity = datetime.utcnow() - timedelta(minutes=30)

        for entry in self.nws_dump:
            _logger.debug('Processing NWS entry {0}'.format(entry))

            src = entry['src']
            dst = entry['dst']

            values = {}

            # PanDA is only interested in production input and output statistics
            for activity in [PROD_INPUT, PROD_OUTPUT, EXPRESS]:
                if not entry.has_key(activity):
                    continue

                try:
                    done_1h = entry[activity]['done_1h']
                    done_6h = entry[activity]['done_6h']
                    queued_for_dst = entry[activity]['queued_for_dst']
                    updated_at = datetime.strptime(entry[activity]['updated_at'], '%Y-%m-%dT%H:%M:%S.%f')
                except KeyError:
                    _logger.error("Entry {0} key {1} does not follow standards".format(entry, activity))
                    continue
                except ValueError:
                    _logger.error("Something wrong with {0}".format(entry[activity]))
                    continue

                # Do not consider expired data
                if updated_at < latest_validity:
                    _logger.warning('Skipped activity {0} because it is expired (ts={1}))'
                                    .format(activity, updated_at))
                    continue

                #Prepare data for bulk upserts
                data.append((src, dst, activity+'_done_1h', done_1h, updated_at))
                data.append((src, dst, activity+'_done_6h', done_6h, updated_at))
                data.append((src, dst, activity+'_queued', queued_for_dst, updated_at))

        return data

    def process_nws_full_dump(self):
        """
        Gets the second generation NWS information dump, filters out irrelevant information
        and prepares it for insertion into the PanDA DB
        """

        data = []

        # Ignore outdated values
        latest_validity = datetime.utcnow() - timedelta(minutes=30)

        for src_dst in self.nws_full_dump:
            try:
                source, destination = src_dst.split(':')
            except ValueError:
                _logger.error("Json wrongly formatted. Expected key with format src:dst, but found key {0}"
                               .format(src_dst))
                continue

            # Transferred files
            try:
                done = self.nws_full_dump[src_dst][FILES][DONE]
                for activity in [PROD_INPUT, PROD_OUTPUT, EXPRESS]:
                    if not done.has_key(activity):
                        continue
                    try:
                        updated_at = datetime.strptime(done[activity][TIMESTAMP], '%Y-%m-%dT%H:%M:%S')
                        if updated_at > latest_validity:
                            done_1h = done[activity][H1]
                            done_6h = done[activity][H6]
                            data.append((source, destination, activity+'_done_1h', done_1h, updated_at))
                            data.append((source, destination, activity+'_done_6h', done_6h, updated_at))
                    except KeyError:
                        _logger.debug("Entry {0} ({1}->{2}) key {3} does not follow standards"
                                      .format(done, source, destination, activity))
                        continue
            except KeyError:
                pass

            # Queued files - take TOTAL (ignore FTS and Rucio breakdowns)
            try:
                queued = self.nws_full_dump[src_dst][FILES][QUEUED][TOTAL]
                for activity in [PROD_INPUT, PROD_OUTPUT, EXPRESS]:
                    if not queued.has_key(activity):
                        continue
                    try:
                        updated_at = datetime.strptime(queued[activity][TIMESTAMP], '%Y-%m-%dT%H:%M:%S')
                        if updated_at > latest_validity:
                            total = queued[activity][TOTAL]
                            data.append((source, destination, activity+'_queued', total, updated_at))
                    except KeyError:
                        _logger.error("Entry {0} ({1}->{2}) key {3} does not follow standards"
                                      .format(queued, source, destination, activity))
                        continue
            except KeyError:
                pass

            # MBps for Rucio, FAX, PerfSonar
            try:
                mbps = self.nws_full_dump[src_dst][MBPS]
                for system in mbps:
                    try:
                        updated_at = datetime.strptime(mbps[system][TIMESTAMP], '%Y-%m-%dT%H:%M:%S')
                        if updated_at > latest_validity:
                            mbps_entry = mbps[system][LATEST]
                            data.append((source, destination, system+'_mbps', mbps_entry, updated_at))
                    except KeyError:
                        _logger.debug("Entry {0} ({1}->{2}) key {3} does not follow MBPS standards"
                                      .format(mbps, source, destination, system))
                        continue
            except KeyError:
                pass

            # PerfSonar latency and packetloss
            for metric in [LATENCY, PACKETLOSS]:
                try:
                    struc = self.nws_full_dump[src_dst][metric]
                    try:
                        updated_at = datetime.strptime(struc[TIMESTAMP], '%Y-%m-%dT%H:%M:%S')
                        if updated_at > latest_validity:
                            value = struc[LATEST]
                            data.append((source, destination, metric, value, updated_at))
                    except KeyError:
                        _logger.debug("Entry {0} ({1}->{2}) does not follow {3} standards"
                                      .format(struc, source, destination, metric))
                        pass
                except KeyError:
                    continue

        return data

    def process_agis_cm_dump(self):
        """
        Gets the AGIS CM information dump, filters out irrelevant information
        and prepares it for insertion into the PanDA DB
        """
        data = []
        latest_validity = datetime.utcnow() - timedelta(minutes=30) # Ignore outdated values

        for entry in self.agis_cm_dump:
            _logger.debug('Processing AGIS CM entry {0}'.format(entry))

            try:
                src = entry['src']
                dst = entry['dst']
                closeness = entry['closeness']
                ts = datetime.now()
            except KeyError:
                _logger.warning("AGIS CM entry {0} does not contain one or more of the keys src/dst/closeness".format(entry))
                continue

            #Prepare data for bulk upserts
            data.append((src, dst, 'AGIS_closeness', closeness, ts))

        return data

    def run(self):
        """
        Principal function
        """

        # Process and store the NWS information (NWS=Network Weather Service)
        data_nws = self.process_nws_dump()
        if not data_nws:
            _logger.critical("Could not retrieve any data from the NWS!")

        # Process and store the AGIS connectivity information
        data_agis_cm = self.process_nws_full_dump()
        if not data_agis_cm:
            _logger.critical("Could not retrieve any data from the AGIS Cost Matrix!")

        data_combined = data_nws + data_agis_cm
        if data_combined:
            # Insert the new data
            taskBuffer.insertNetworkMatrixData(data_combined)
            # Do some cleanup of old data
            taskBuffer.deleteOldNetworkData()
            return True
        else:
            return False

if __name__ == "__main__":

    # If no argument, call the basic configurator
    if len(sys.argv)==1:
        t1 = time.time()
        configurator = Configurator()
        if not configurator.run():
            _logger.critical("Configurator loop FAILED")
        t2 = time.time()
        _logger.debug("Configurator run took {0}s".format(t2-t1))

    # If --network argument, call the network configurator
    elif len(sys.argv) == 2 and sys.argv[1].lower() == '--network':
        t1 = time.time()
        network_configurator = NetworkConfigurator()
        if not network_configurator.run():
            _logger.critical("Configurator loop FAILED")
        t2 = time.time()
        _logger.debug("Network-Configurator run took {0}s".format(t2-t1))

    else:
        _logger.error("Configurator being called with wrong arguments. Use either no arguments or --network")