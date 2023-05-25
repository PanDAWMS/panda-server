import sys
import threading
import traceback
from pandaserver.configurator import aux
from pandaserver.configurator.aux import *
from datetime import datetime, timedelta

from pandaserver.config import panda_config
from pandacommon.pandalogger.PandaLogger import PandaLogger

_logger = PandaLogger().getLogger('configurator')

# Definitions of roles
WRITE_LAN = 'write_lan'
READ_LAN = 'read_lan'
DEFAULT = 'default'


class Configurator(threading.Thread):

    def __init__(self, taskBuffer):
        threading.Thread.__init__(self)

        self.taskBuffer = taskBuffer

        if hasattr(panda_config, 'CRIC_URL_SITES'):
            self.CRIC_URL_SITES = panda_config.CRIC_URL_SITES
        else:
            self.CRIC_URL_SITES = 'https://atlas-cric.cern.ch/api/atlas/site/query/?json'

        _logger.debug('Getting site dump...')
        self.site_dump = aux.get_dump(self.CRIC_URL_SITES)
        _logger.debug('Done')
        self.site_endpoint_dict = self.get_site_endpoint_dictionary()

        if hasattr(panda_config, 'CRIC_URL_DDMENDPOINTS'):
            self.CRIC_URL_DDMENDPOINTS = panda_config.CRIC_URL_DDMENDPOINTS
        else:
            self.CRIC_URL_DDMENDPOINTS = 'https://atlas-cric.cern.ch/api/atlas/ddmendpoint/query/?json'
        _logger.debug('Getting DDM endpoints dump...')
        self.endpoint_dump = aux.get_dump(self.CRIC_URL_DDMENDPOINTS)
        _logger.debug('Done')
        _logger.debug('Parsing endpoints...')
        self.endpoint_token_dict = self.parse_endpoints()
        _logger.debug('Done')

        if hasattr(panda_config, 'CRIC_URL_SCHEDCONFIG'):
            self.CRIC_URL_SCHEDCONFIG = panda_config.CRIC_URL_SCHEDCONFIG
        else:
            self.CRIC_URL_SCHEDCONFIG = 'https://atlas-cric.cern.ch/api/atlas/pandaqueue/query/?json'
        _logger.debug('Getting schedconfig dump...')
        self.schedconfig_dump = aux.get_dump(self.CRIC_URL_SCHEDCONFIG)
        _logger.debug('Done')

        if hasattr(panda_config, 'CRIC_URL_DDMBLACKLIST'):
            self.CRIC_URL_DDMBLACKLIST = panda_config.CRIC_URL_DDMBLACKLIST
        else:
            self.CRIC_URL_DDMBLACKLIST = 'https://atlas-cric.cern.ch/api/atlas/ddmendpointstatus/query/?json&activity=write_wan&fstate=OFF'
        _logger.debug('Getting ddmblacklist dump...')
        try:
            self.blacklisted_endpoints = list(aux.get_dump(self.CRIC_URL_DDMBLACKLIST))
            self.blacklisted_endpoints_write = self.blacklisted_endpoints
        except TypeError:
            self.blacklisted_endpoints = []
            self.blacklisted_endpoints_write = []
        _logger.debug('Blacklisted endpoints {0}'.format(self.blacklisted_endpoints))
        _logger.debug('Blacklisted endpoints write {0}'.format(self.blacklisted_endpoints_write))
        _logger.debug('Done')

        if hasattr(panda_config, 'CRIC_URL_DDMBLACKLIST_READ'):
            self.CRIC_URL_DDMBLACKLIST_READ = panda_config.CRIC_URL_DDMBLACKLIST_READ
        else:
            self.CRIC_URL_DDMBLACKLIST_READ = 'https://atlas-cric.cern.ch/api/atlas/ddmendpointstatus/query/?json&activity=read_wan&fstate=OFF'
        _logger.debug('Getting ddmblacklist read dump...')
        try:
            self.blacklisted_endpoints_read = list(aux.get_dump(self.CRIC_URL_DDMBLACKLIST_READ))
        except TypeError:
            self.blacklisted_endpoints_read = []
        _logger.debug('Blacklisted endpoints read {0}'.format(self.blacklisted_endpoints_read))
        _logger.debug('Done')

        if hasattr(panda_config, 'RUCIO_RSE_USAGE'):
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
        state = site['state']
        tier_level = site['tier_level']

        if 'Nucleus' in site['datapolicies']:  # or site['tier_level'] <= 1:
            role = 'nucleus'
        else:
            role = 'satellite'

        return role, state, tier_level

    def parse_endpoints(self):
        """
        Puts the relevant information from endpoint_dump into a more usable format
        """
        endpoint_token_dict = {}
        for endpoint, endpoint_config in self.endpoint_dump.items():
            # Filter out testing and inactive endpoints
            if endpoint_config['state'] == 'ACTIVE': # and endpoint['type'] != 'TEST'
                endpoint_token_dict[endpoint] = {}
                endpoint_token_dict[endpoint]['token'] = endpoint_config['token']
                endpoint_token_dict[endpoint]['site_name'] = endpoint_config['site']
                endpoint_token_dict[endpoint]['type'] = endpoint_config['type']
                if endpoint_config['is_tape']:
                    endpoint_token_dict[endpoint]['is_tape'] = 'Y'
                else:
                    endpoint_token_dict[endpoint]['is_tape'] = 'N'
            else:
                _logger.debug('parse_endpoints: skipped endpoint {0} (type: {1}, state: {2})'
                              .format(endpoint, endpoint_config['type'], endpoint_config['state']))

        return endpoint_token_dict

    def get_site_endpoint_dictionary(self):
        """
        Converts the CRIC site dump into a site dictionary containing the list of DDM endpoints for each site
        """
        site_to_endpoints_dict = {}
        for site, site_config in self.site_dump.items():
            site_to_endpoints_dict[site] = list(site_config['ddmendpoints'])

        return site_to_endpoints_dict

    def process_site_dumps(self):
        """
        Parses the CRIC site and endpoint dumps and prepares a format loadable to the DB
        """
        # Variables that will contain only the relevant information
        sites_list = []
        included_sites = []
        ddm_endpoints_list = []
        panda_sites_list = []

        # New relationship information based on astorages field in CRIC.
        # Used to fill atlas_panda.panda_ddm_relation table
        try:
            panda_ddm_relation_list = self.get_panda_ddm_relations()
        except Exception:
            # Temporary protection to prevent issues
            _logger.critical('get_panda_ddm_relations excepted with {0}'.format(traceback.print_exc()))
            panda_ddm_relation_list = []

        # Iterate the site dump
        for site_name, site_config in self.site_dump.items():
            # Add the site info to a list
            (site_role, site_state, tier_level) = self.get_site_info(site_config)
            if site_state == 'ACTIVE' and site_name not in included_sites:
                sites_list.append({'site_name': site_name,
                                   'role': site_role,
                                   'tier_level': tier_level})
                included_sites.append(site_name)
            else:
                _logger.debug('process_site_dumps: skipped site {0} (state: {1})'.format(site_name, site_state))

            # Get the DDM endpoints for the site we are inspecting
            for ddm_endpoint_name in site_config['ddmendpoints']:

                try:
                    ddm_spacetoken_name = self.endpoint_token_dict[ddm_endpoint_name]['token']
                    ddm_endpoint_type = self.endpoint_token_dict[ddm_endpoint_name]['type']
                    ddm_endpoint_is_tape = self.endpoint_token_dict[ddm_endpoint_name]['is_tape']
                    if ddm_endpoint_name in self.blacklisted_endpoints:
                        ddm_endpoint_blacklisted_write = 'Y'
                        _logger.debug('process_site_dumps: endpoint {0} is blacklisted for write'.format(ddm_endpoint_name))
                    else:
                        ddm_endpoint_blacklisted_write = 'N'
                        _logger.debug('process_site_dumps: endpoint {0} is NOT blacklisted for write'.format(ddm_endpoint_name))

                    if ddm_endpoint_name in self.blacklisted_endpoints_read:
                        ddm_endpoint_blacklisted_read = 'Y'
                        _logger.debug('process_site_dumps: endpoint {0} is blacklisted for read'.format(ddm_endpoint_name))
                    else:
                        ddm_endpoint_blacklisted_read = 'N'
                        _logger.debug('process_site_dumps: endpoint {0} is NOT blacklisted for read'.format(ddm_endpoint_name))
                except KeyError:
                    continue

                # Get the storage space
                try:
                    space_used = self.rse_usage[ddm_endpoint_name]['storage']['used']/GB
                    _logger.debug('process_site_dumps: endpoint {0} has used space {1}GB'.format(ddm_endpoint_name,
                                                                                                 space_used))
                    space_free = self.rse_usage[ddm_endpoint_name]['storage']['free']/GB
                    _logger.debug('process_site_dumps: endpoint {0} has free space {1}GB'.format(ddm_endpoint_name,
                                                                                                 space_free))
                    space_total = space_used + space_free
                    space_timestamp = datetime.strptime(self.rse_usage[ddm_endpoint_name]['storage']['updated_at'],
                                                        '%Y-%m-%d %H:%M:%S')
                    _logger.debug('process_site_dumps: endpoint {0} has space timestamp {1}'.format(ddm_endpoint_name,
                                                                                                    space_timestamp))

                except (KeyError, ValueError):
                    space_used, space_free, space_total, space_timestamp = None, None, None, None
                    _logger.warning('process_site_dumps: no rse storage usage information for {0}'.format(ddm_endpoint_name))

                # Get the Expired space
                try:
                    space_expired = self.rse_usage[ddm_endpoint_name]['expired']['used']/GB
                except KeyError:
                    space_expired = 0
                    _logger.warning('process_site_dumps: no rse EXPIRED usage information for {0}'
                                    .format(ddm_endpoint_name))

                ddm_spacetoken_state = site_config['ddmendpoints'][ddm_endpoint_name]['state']
                if ddm_spacetoken_state == 'ACTIVE':
                    ddm_endpoints_list.append({'ddm_endpoint_name': ddm_endpoint_name,
                                               'site_name': site_name,
                                               'ddm_spacetoken_name': ddm_spacetoken_name,
                                               'type': ddm_endpoint_type,
                                               'is_tape': ddm_endpoint_is_tape,
                                               'blacklisted': ddm_endpoint_blacklisted_write,
                                               'blacklisted_write': ddm_endpoint_blacklisted_write,
                                               'blacklisted_read': ddm_endpoint_blacklisted_read,
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
            for panda_resource in site_config['presources']:
                for panda_site in site_config['presources'][panda_resource]:
                    panda_site_state = site_config['presources'][panda_resource][panda_site]['state']
                    if panda_site_state != 'ACTIVE':
                        _logger.debug('process_site_dumps: skipped PanDA site {0} (state: {1})'
                                      .format(panda_site, panda_site_state))
                        continue
                    panda_site_name = panda_site
                    panda_sites_list.append({'panda_site_name': panda_site_name,
                                             'site_name': site_name})

        return sites_list, panda_sites_list, ddm_endpoints_list, panda_ddm_relation_list

    def parse_role(self, role):
        """
        Traditionally roles have been "read_lan" or "write_lan". We will consider them the default roles.
        If you want to overwrite the role for specific jobs in CRIC, you can define e.g. "read_lan_analysis". Here we
        want to strip the role to the "analysis" tag and return role "read_lan" and scope "analysis"

        Examples:
            "read_lan_analysis" --> "read_lan" and "analysis"
            "read_lan" --> "read_lan" and "default"
        """
        # default roles: "read_lan" or "write_lan"
        if role == READ_LAN or role == WRITE_LAN:
            return role, DEFAULT

        # special read_lan roles, e.g. "read_lan_analysis"
        elif role.startswith(READ_LAN):
            role_clean = READ_LAN
            scope = role.replace("{0}_".format(READ_LAN), '')
            return role_clean, scope

        # special write_lan roles, e.g. "write_lan_analysis"
        elif role.startswith(WRITE_LAN):
            role_clean = WRITE_LAN
            scope = role.replace("{0}_".format(WRITE_LAN), '')
            return role_clean, scope

        # roles we are currently not expecting
        else:
            return role, DEFAULT

    def get_panda_ddm_relations(self):
        """
        Gets the DDM endpoints assigned to a panda queue, based on the CRIC astorages field of the panda queue definition
        """
        relation_list = []

        # iterate on panda queues
        for long_panda_site_name in self.schedconfig_dump:
            panda_site_name = self.schedconfig_dump[long_panda_site_name]['panda_resource']
            cpu_site_name = self.schedconfig_dump[long_panda_site_name]['atlas_site']
            dict_ddm_endpoint = {}

            # get the astorages field
            if self.schedconfig_dump[long_panda_site_name]['astorages']:
                astorages = self.schedconfig_dump[long_panda_site_name]['astorages']

                # iterate the storages to establish their roles and orders
                order_read = {DEFAULT: 1}
                order_write = {DEFAULT: 1}
                for role in astorages:

                    # ignore old roles (pr, pw) that we are not supposed to use
                    if not role.startswith(READ_LAN) and not role.startswith(WRITE_LAN):
                        continue

                    for ddm_endpoint_name in astorages[role]:
                        # set the read/write order and increment the respective counter
                        role_clean, scope = self.parse_role(role)
                        order_read.setdefault(scope, 1)
                        order_write.setdefault(scope, 1)

                        # initialize DDM endpoint and scope if it does not exist
                        dict_ddm_endpoint.setdefault(ddm_endpoint_name, {})
                        dict_ddm_endpoint[ddm_endpoint_name].setdefault(scope, {'order_write': None,
                                                                                'order_read': None,
                                                                                'role': []
                                                                               })

                        if role.startswith(WRITE_LAN):
                            dict_ddm_endpoint[ddm_endpoint_name][scope]['order_write'] = order_write[scope]
                            order_write[scope] += 1
                        elif role.startswith(READ_LAN):
                            dict_ddm_endpoint[ddm_endpoint_name][scope]['order_read'] = order_read[scope]
                            order_read[scope] += 1
                        # append the roles
                        dict_ddm_endpoint[ddm_endpoint_name][scope]['role'].append(role_clean)

                for ddm_endpoint_name, ddm_endpoint_values in dict_ddm_endpoint.items():
                    for scope, scope_values in ddm_endpoint_values.items():
                        # unpack the values
                        roles = scope_values['role']
                        order_write = scope_values['order_write']
                        order_read = scope_values['order_read']

                        # figure out the ATLAS site the DDM endpoint belongs to
                        try:
                            storage_site_name = self.endpoint_token_dict[ddm_endpoint_name]['site_name']
                        except KeyError:
                            _logger.warning("Skipped {0}, because primary associated DDM endpoint {1} not found (e.g.DISABLED)"
                                            .format(long_panda_site_name, ddm_endpoint_name))
                            continue

                        # figure out if the storage is local to the cpu
                        if storage_site_name == cpu_site_name:
                            is_local = 'Y'
                        else:
                            is_local = 'N'

                        # endpoints with order 1 will be considered default
                        if order_read == 1:
                            default_read = 'Y'
                        else:
                            default_read = 'N'
                        if order_write == 1:
                            default_write = 'Y'
                        else:
                            default_write = 'N'

                        # add the values to the list of relations if the ddm endpoint is valid
                        if ddm_endpoint_name in self.endpoint_token_dict:
                            relation_list.append({'panda_site_name': panda_site_name,
                                                  'ddm_endpoint_name': ddm_endpoint_name,
                                                  'roles': ','.join(roles),
                                                  'default_read': default_read,
                                                  'default_write': default_write,
                                                  'is_local': is_local,
                                                  'order_read': order_read,
                                                  'order_write': order_write,
                                                  'scope': scope,
                                                  })

        return relation_list

    def consistency_check(self):
        """
        Point out sites, panda sites and DDM endpoints that are missing in one of the sources
        """
        # Check for site inconsistencies
        CRIC_sites = set([site_name for site_name, site_config in self.site_dump.items() if site_config['state'] == 'ACTIVE'])
        _logger.debug("Sites in CRIC {0}".format(CRIC_sites))
        configurator_sites = self.taskBuffer.configurator_read_sites()
        _logger.debug("Sites in Configurator {0}".format(configurator_sites))
        schedconfig_sites = self.taskBuffer.configurator_read_cric_sites()
        _logger.debug("Sites in Schedconfig {0}".format(schedconfig_sites))

        all_sites = list(CRIC_sites | configurator_sites | schedconfig_sites)
        all_sites.sort()

        for site in all_sites:
            missing = []
            if site not in CRIC_sites:
                missing.append('CRIC')
            if site not in configurator_sites:
                missing.append('Configurator')
            if site not in schedconfig_sites:
                missing.append('Schedconfig')
            if missing:
                _logger.warning("SITE inconsistency: {0} was not found in {1}".format(site, missing))

        # Check for panda-site inconsistencies
        CRIC_panda_sites = set([self.schedconfig_dump[long_panda_site_name]['panda_resource']
                                for long_panda_site_name in self.schedconfig_dump])
        _logger.debug("PanDA sites in CRIC {0}".format(CRIC_panda_sites))
        configurator_panda_sites = self.taskBuffer.configurator_read_panda_sites()
        _logger.debug("PanDA sites in Configurator {0}".format(configurator_panda_sites))
        schedconfig_panda_sites = self.taskBuffer.configurator_read_cric_panda_sites()
        _logger.debug("PanDA sites in Schedconfig {0}".format(schedconfig_panda_sites))

        all_panda_sites = list(CRIC_panda_sites | configurator_panda_sites | schedconfig_panda_sites)
        all_panda_sites.sort()

        for site in all_panda_sites:
            missing = []
            if site not in CRIC_panda_sites:
                missing.append('CRIC')
            if site not in configurator_panda_sites:
                missing.append('Configurator')
            if site not in schedconfig_panda_sites:
                missing.append('Schedconfig')
            if missing:
                _logger.warning("PanDA SITE inconsistency: {0} was not found in {1}".format(site, missing))

        # Check for DDM endpoint inconsistencies
        CRIC_ddm_endpoints = set([ddm_endpoint_name for ddm_endpoint_name in self.endpoint_token_dict])
        _logger.debug("DDM endpoints in CRIC {0}".format(CRIC_ddm_endpoints))
        configurator_ddm_endpoints = self.taskBuffer.configurator_read_ddm_endpoints()
        _logger.debug("DDM endpoints in Configurator {0}".format(configurator_ddm_endpoints))

        all_ddm_endpoints = list(CRIC_ddm_endpoints | configurator_ddm_endpoints)
        all_ddm_endpoints.sort()

        for site in all_ddm_endpoints:
            missing = []
            if site not in CRIC_ddm_endpoints:
                missing.append('CRIC')
            if site not in configurator_ddm_endpoints:
                missing.append('Configurator')
            if missing:
                _logger.warning("DDM ENDPOINT inconsistency: {0} was not found in {1}".format(site, missing))

        self.cleanup_configurator(CRIC_sites, CRIC_panda_sites, CRIC_ddm_endpoints, configurator_sites,
                                  configurator_panda_sites, configurator_ddm_endpoints)

    def cleanup_configurator(self, CRIC_sites, CRIC_panda_sites, CRIC_ddm_endpoints, configurator_sites,
                             configurator_panda_sites, configurator_ddm_endpoints):
        """
        Cleans up information from configurator that is not in CRIC
        """
        if not CRIC_sites or not CRIC_panda_sites or not CRIC_ddm_endpoints:
            _logger.warning("Exiting cleanup because one of CRIC sets was empty")

        # Clean up sites
        sites_to_delete = configurator_sites - CRIC_sites
        self.taskBuffer.configurator_delete_sites(sites_to_delete)

        # Clean up panda sites
        panda_sites_to_delete = configurator_panda_sites - CRIC_panda_sites
        self.taskBuffer.configurator_delete_panda_sites(panda_sites_to_delete)

        # Clean up DDM endpoints
        ddm_endpoints_to_delete = configurator_ddm_endpoints - CRIC_ddm_endpoints
        self.taskBuffer.configurator_delete_ddm_endpoints(ddm_endpoints_to_delete)

    def run(self):
        """
        Principal function
        """
        if self.schedconfig_dump is None:
            _logger.critical("SKIPPING RUN. Failed to download {0}".format(self.CRIC_URL_SCHEDCONFIG))
            return False

        if self.endpoint_dump is None:
            _logger.critical("SKIPPING RUN. Failed to download {0}".format(self.CRIC_URL_DDMENDPOINTS))
            return False

        if self.site_dump is None:
            _logger.critical("SKIPPING RUN. Failed to download {0}".format(self.CRIC_URL_SITES))
            return False

        # Get pre-processed CRIC dumps
        sites_list, panda_sites_list, ddm_endpoints_list, panda_ddm_relation_dict = self.process_site_dumps()

        # Persist the information to the PanDA DB
        self.taskBuffer.configurator_write_sites(sites_list)
        self.taskBuffer.configurator_write_panda_sites(panda_sites_list)
        self.taskBuffer.configurator_write_ddm_endpoints(ddm_endpoints_list)
        self.taskBuffer.configurator_write_panda_ddm_relations(panda_ddm_relation_dict)

        # Do a data quality check
        self.consistency_check()

        return True


class NetworkConfigurator(threading.Thread):

    def __init__(self, taskBuffer):
        threading.Thread.__init__(self)

        self.taskBuffer = taskBuffer

        if hasattr(panda_config, 'NWS_URL'):
            self.NWS_URL = panda_config.NWS_URL
        else:
            self.NWS_URL = 'http://atlas-adc-netmetrics-lb.cern.ch/metrics/latest.json'
        _logger.debug('Getting NWS dump...')
        self.nws_dump = aux.get_dump(self.NWS_URL)
        _logger.debug('Done')

        if hasattr(panda_config, 'CRIC_URL_CM'):
            self.CRIC_URL_CM = panda_config.CRIC_URL_CM
        else:
            self.CRIC_URL_CM = 'https://atlas-cric.cern.ch/api/core/sitematrix/query/?json&json_pretty=0'
        _logger.debug('Getting CRIC cost matrix dump...')
        self.CRIC_cm_dump = aux.get_dump(self.CRIC_URL_CM)
        _logger.debug('Done')

    def process_nws_dump(self):
        """
        Gets the second generation NWS information dump, filters out irrelevant information
        and prepares it for insertion into the PanDA DB
        """

        data = []
        sites_list = self.taskBuffer.configurator_read_sites()

        # Ignore outdated values
        latest_validity = datetime.utcnow() - timedelta(minutes=30)

        for src_dst in self.nws_dump:
            try:
                source, destination = src_dst.split(':')
                skip_sites = []

                # Skip entries with sites not recognized by configurator
                if source not in sites_list:
                    skip_sites.append(source)
                if destination not in sites_list:
                    skip_sites.append(destination)
                if skip_sites:
                    _logger.warning("Could not find site(s) {0} in configurator sites".format(skip_sites))
                    continue

            except ValueError:
                _logger.error("Json wrongly formatted. Expected key with format src:dst, but found key {0}"
                              .format(src_dst))
                continue

            # Transferred files
            try:
                done = self.nws_dump[src_dst][FILES][DONE]
                for activity in [PROD_INPUT, PROD_OUTPUT, EXPRESS]:
                    if activity not in done:
                        continue
                    try:
                        updated_at = datetime.strptime(done[activity][TIMESTAMP], '%Y-%m-%dT%H:%M:%S')
                        if updated_at > latest_validity:
                            done_1h = done[activity][H1]
                            done_6h = done[activity][H6]
                            data.append((source, destination, activity+'_done_1h', done_1h, updated_at))
                            data.append((source, destination, activity+'_done_6h', done_6h, updated_at))
                    except (KeyError, ValueError):
                        _logger.debug("Entry {0} ({1}->{2}) key {3} does not follow standards"
                                      .format(done, source, destination, activity))
                        continue
            except KeyError:
                pass

            # Queued files
            try:
                queued = self.nws_dump[src_dst][FILES][QUEUED]
                for activity in [PROD_INPUT, PROD_OUTPUT, EXPRESS]:
                    if activity not in queued:
                        continue
                    try:
                        updated_at = datetime.strptime(queued[activity][TIMESTAMP], '%Y-%m-%dT%H:%M:%S')
                        if updated_at > latest_validity:
                            nqueued = queued[activity][LATEST]
                            data.append((source, destination, activity+'_queued', nqueued, updated_at))
                    except (KeyError, ValueError):
                        _logger.error("Entry {0} ({1}->{2}) key {3} does not follow standards"
                                      .format(queued, source, destination, activity))
                        continue
            except KeyError:
                pass

            # MBps for Rucio, FAX, PerfSonar
            try:
                mbps = self.nws_dump[src_dst][MBPS]
                for system in mbps:
                    try:
                        updated_at = datetime.strptime(mbps[system][TIMESTAMP], '%Y-%m-%dT%H:%M:%S')
                    except ValueError:
                        _logger.debug("Entry {0} has wrong timestamp for system {1}".format(mbps, system))
                    if updated_at > latest_validity:
                        for duration in [H1, D1, W1]:
                            try:
                                mbps_entry = mbps[system][duration]
                                data.append((source, destination, '{0}_mbps_{1}'.format(system, duration), mbps_entry, updated_at))
                            except KeyError:
                                _logger.debug("Entry {0} ({1}->{2}) system {3} duration {4} not available or wrongly formatted"
                                              .format(mbps, source, destination, system, duration))
                                _logger.debug(sys.exc_info())
            except KeyError:
                pass

            # PerfSonar latency and packetloss
            for metric in [LATENCY, PACKETLOSS]:
                try:
                    struc = self.nws_dump[src_dst][metric]
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

    def process_CRIC_cm_dump(self):
        """
        Gets the CRIC CM information dump, filters out irrelevant information
        and prepares it for insertion into the PanDA DB
        """
        data = []
        sites_list = self.taskBuffer.configurator_read_sites()

        for entry in self.CRIC_cm_dump:

            _logger.debug('Processing CRIC CM entry {0}'.format(entry))

            try:
                src = entry['src']
                dst = entry['dst']
                closeness = entry['closeness']
                ts = datetime.now()

                # filter out sites that are not in CRIC
                skip_sites = []
                if src not in sites_list:
                    skip_sites.append(src)
                if dst not in sites_list:
                    skip_sites.append(dst)
                if skip_sites:
                    _logger.warning("Could not find site(s) {0} in configurator sites".format(skip_sites))
                    continue

                # Skip broken entries (protection against errors in CRIC)
                if not src or not dst:
                    continue

                # Prepare data for bulk upserts
                data.append((src, dst, 'AGIS_closeness', closeness, ts))

            except KeyError:
                _logger.warning("CRIC CM entry {0} does not contain one or more of the keys src/dst/closeness".format(entry))
                continue

        return data

    def run(self):
        """
        Principal function
        """

        # Process and store the NWS information (NWS=Network Weather Service)
        data_nws = self.process_nws_dump()
        if not data_nws:
            _logger.critical("Could not retrieve any data from the NWS!")

        # Process and store the CRIC connectivity information
        data_CRIC_cm = self.process_CRIC_cm_dump()
        if not data_CRIC_cm:
            _logger.critical("Could not retrieve any data from the CRIC Cost Matrix!")

        data_combined = data_nws + data_CRIC_cm
        if data_combined:
            # Insert the new data
            self.taskBuffer.insertNetworkMatrixData(data_combined)
            # Do some cleanup of old data
            self.taskBuffer.deleteOldNetworkData()
            return True
        else:
            return False


class SchedconfigJsonDumper(threading.Thread):
    """
    Downloads the CRIC schedconfig dump and stores it in the DB, one row per queue
    """
    def __init__(self, taskBuffer):
        """
        Initialization and configuration
        """
        threading.Thread.__init__(self)

        self.taskBuffer = taskBuffer

        if hasattr(panda_config, 'CRIC_URL_SCHEDCONFIG'):
            self.CRIC_URL_SCHEDCONFIG = panda_config.CRIC_URL_SCHEDCONFIG
        else:
            self.CRIC_URL_SCHEDCONFIG = 'https://atlas-cric.cern.ch/api/atlas/pandaqueue/query/?json'

        _logger.debug('Getting schedconfig dump...')
        self.schedconfig_dump = aux.get_dump(self.CRIC_URL_SCHEDCONFIG)
        _logger.debug('Done')

    def run(self):
        """
        Principal function
        """
        if self.schedconfig_dump is None:
            _logger.critical("SKIPPING RUN. Failed to download {0}".format(self.CRIC_URL_SCHEDCONFIG))
            return False

        return self.taskBuffer.upsertQueuesInJSONSchedconfig(self.schedconfig_dump)


class SWTagsDumper(threading.Thread):
    """
    Downloads the CRIC tags dump, flattens it out and stores it in the DB, one row per queue
    """
    def __init__(self, taskBuffer):
        """
        Initialization and configuration
        """
        threading.Thread.__init__(self)

        self.taskBuffer = taskBuffer

        if hasattr(panda_config, 'CRIC_URL_SCHEDCONFIG'):
            self.CRIC_URL_TAGS = panda_config.CRIC_URL_TAGS
        else:
            self.CRIC_URL_TAGS = 'https://atlas-cric.cern.ch/api/atlas/pandaqueue/query/?json&preset=tags'

        _logger.debug('Getting tags dump...')
        self.tags_dump = aux.get_dump(self.CRIC_URL_TAGS)
        _logger.debug('Done')

    def run(self):
        """
        Principal function
        """
        if self.tags_dump is None:
            _logger.critical("SKIPPING RUN. Failed to download {0}".format(self.CRIC_URL_TAGS))
            return False

        return self.taskBuffer.loadSWTags(self.tags_dump)