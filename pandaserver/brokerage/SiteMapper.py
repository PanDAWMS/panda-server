import copy
import sys
import traceback

from pandacommon.pandalogger.PandaLogger import PandaLogger

from pandaserver.config import panda_config
from pandaserver.dataservice.DataServiceUtils import select_scope
from pandaserver.taskbuffer.NucleusSpec import NucleusSpec
from pandaserver.taskbuffer.SiteSpec import SiteSpec

_logger = PandaLogger().getLogger("SiteMapper")

DEFAULT_SITE = SiteSpec()
DEFAULT_SITE.sitename = panda_config.def_sitename
DEFAULT_SITE.nickname = panda_config.def_nickname
DEFAULT_SITE.ddm_input = {"default": panda_config.def_ddm}
DEFAULT_SITE.ddm_output = {"default": panda_config.def_ddm}
DEFAULT_SITE.type = panda_config.def_type
DEFAULT_SITE.status = panda_config.def_status
DEFAULT_SITE.setokens_input = {}
DEFAULT_SITE.setokens_output = {}
DEFAULT_SITE.ddm_endpoints_input = {}
DEFAULT_SITE.ddm_endpoints_output = {}

# constants
WORLD_CLOUD = "WORLD"
NUCLEUS_TAG = "nucleus:"


class SiteMapper:
    def __init__(self, taskBuffer, verbose=False):
        _logger.debug("__init__ SiteMapper")
        try:
            self.siteSpecList = {}
            self.cloudSpec = {}  # in reality this is a dictionary of clouds, not a "spec" object
            self.worldCloudSpec = {}
            self.nuclei = {}
            self.satellites = {}

            # get resource types
            resource_types = taskBuffer.load_resource_types()

            # read sites information from database in the format
            # {'PANDA_QUEUE_1': < pandaserver.taskbuffer.SiteSpec.SiteSpec object1 >,
            #  'PANDA_QUEUE_2': < pandaserver.taskbuffer.SiteSpec.SiteSpec object2 >, ...}
            site_spec_dictionary = taskBuffer.getSiteInfo()

            # create dictionary with clouds
            clouds = taskBuffer.get_cloud_list()
            for tmp_name in clouds:
                if tmp_name == WORLD_CLOUD:
                    self.worldCloudSpec = {"sites": []}
                else:
                    self.cloudSpec[tmp_name] = {"sites": []}

            # read DB to produce parameters in site info dynamically
            for site_name_tmp, site_spec_tmp in site_spec_dictionary.items():
                # we can't find the site info for the queue
                if not site_spec_tmp:
                    _logger.error(f"Could not read site info for {site_name_tmp}")

                # if the current site has not been processed yet
                # or the current site has been processed and is offline or has no status
                elif (site_name_tmp not in self.siteSpecList) or (
                    site_name_tmp in self.siteSpecList and self.siteSpecList[site_name_tmp].status in ["offline", ""]
                ):
                    # remove existing offline site
                    if site_name_tmp in self.siteSpecList and self.siteSpecList[site_name_tmp].status in ["offline", ""]:
                        del self.siteSpecList[site_name_tmp]

                    # add the site information
                    if site_name_tmp not in self.siteSpecList:
                        # don't use site for production when cloud is undefined
                        if site_spec_tmp.runs_production() and not site_spec_tmp.cloud:
                            _logger.error(f"Empty cloud for {site_name_tmp}")
                        else:
                            self.siteSpecList[site_name_tmp] = site_spec_tmp

                # if the current site has been processed and is not offline or has a status
                else:
                    # overwrite status
                    if site_spec_tmp.status not in ["offline", ""]:
                        if self.siteSpecList[site_name_tmp].status != "online":
                            self.siteSpecList[site_name_tmp].status = site_spec_tmp.status

                        # use larger maxinputsize and memory
                        try:
                            if site_spec_tmp.status in ["online"]:
                                if self.siteSpecList[site_name_tmp].maxinputsize < site_spec_tmp.maxinputsize or site_spec_tmp.maxinputsize == 0:
                                    self.siteSpecList[site_name_tmp].maxinputsize = site_spec_tmp.maxinputsize
                                if (
                                    self.siteSpecList[site_name_tmp].memory != 0 and self.siteSpecList[site_name_tmp].memory < site_spec_tmp.memory
                                ) or site_spec_tmp.memory == 0:
                                    self.siteSpecList[site_name_tmp].memory = site_spec_tmp.memory
                        except Exception:
                            error_type, error_value = sys.exc_info()[:2]
                            _logger.error(f"{site_name_tmp} memory/inputsize failure : {error_type} {error_value}")

            # make virtual queues from unified queues
            try:
                for site_name_tmp in list(self.siteSpecList):
                    site_spec_tmp = self.siteSpecList[site_name_tmp]
                    if site_spec_tmp.capability == "ucore":
                        # add child sites
                        for resource_spec_tmp in resource_types:
                            child_site_spec = self.get_child_site_spec(site_spec_tmp, resource_spec_tmp)
                            if child_site_spec:
                                self.siteSpecList[child_site_spec.sitename] = child_site_spec

                        # set unified flag
                        site_spec_tmp.is_unified = True
            except Exception:
                _logger.error(traceback.format_exc())

            # collect nuclei and satellites for main and child sites
            for tmp_site_spec in self.siteSpecList.values():
                self.collect_nuclei_and_satellites(tmp_site_spec)

            # group sites by cloud
            for tmp_site_spec in self.siteSpecList.values():
                # choose only prod or grandly unified sites
                if not tmp_site_spec.runs_production():
                    continue

                # append prod site to cloud structure
                if tmp_site_spec.cloud and tmp_site_spec.cloud in self.cloudSpec:
                    if tmp_site_spec.sitename not in self.cloudSpec[tmp_site_spec.cloud]["sites"]:
                        self.cloudSpec[tmp_site_spec.cloud]["sites"].append(tmp_site_spec.sitename)

                # add to WORLD cloud
                if tmp_site_spec.sitename not in self.worldCloudSpec["sites"]:
                    self.worldCloudSpec["sites"].append(tmp_site_spec.sitename)

            # dump site information in verbose mode and cloud information
            if verbose:
                self.dump_site_information()
            self.dump_cloud_information()

        except Exception:
            error_type, error_value, _ = sys.exc_info()
            _logger.error(f"__init__ SiteMapper : {error_type} {error_value}")
            _logger.error(traceback.format_exc())

        _logger.debug("__init__ SiteMapper done")

    def get_child_site_spec(self, site_spec, resource_spec):
        core_count = max(1, site_spec.coreCount)

        # make sure our queue is compatible with the resource type
        if resource_spec.mincore is not None and core_count < resource_spec.mincore:
            return None

        # copy the site spec for the child site and later overwrite relevant fields
        child_site_spec = copy.copy(site_spec)
        child_site_spec.sitename = f"{site_spec.sitename}/{resource_spec.resource_name}"

        # calculate the core count for the child queue
        if resource_spec.maxcore is None:
            child_site_spec.coreCount = max(core_count, resource_spec.mincore)
        else:
            child_site_spec.coreCount = max(
                min(core_count, resource_spec.maxcore),
                resource_spec.mincore,
            )

        # calculate the minRSS for the child queue
        if resource_spec.minrampercore is not None:
            child_site_spec.minrss = max(
                child_site_spec.coreCount * resource_spec.minrampercore,
                site_spec.minrss * child_site_spec.coreCount / core_count,
            )
        else:
            child_site_spec.minrss = max(
                site_spec.minrss * child_site_spec.coreCount / core_count,
                site_spec.minrss,
            )

        # calculate the maxRSS for the child queue
        if resource_spec.maxrampercore is not None:
            child_site_spec.maxrss = min(
                child_site_spec.coreCount * resource_spec.maxrampercore,
                site_spec.maxrss * child_site_spec.coreCount / core_count,
            )
        else:
            child_site_spec.maxrss = min(
                site_spec.maxrss * child_site_spec.coreCount / core_count,
                site_spec.maxrss,
            )

        # set unified name
        child_site_spec.unified_name = site_spec.sitename

        return child_site_spec

    # collect nuclei and satellites
    def collect_nuclei_and_satellites(self, ret):
        # only consider production sites
        if not ret.runs_production():
            return

        # target will point to the nuclei or satellites attribute in the SiteMapper object
        if ret.role == "nucleus":
            target = self.nuclei
        elif ret.role == "satellite":
            target = self.satellites
        else:
            return

        if ret.pandasite not in target:
            atom = NucleusSpec(ret.pandasite)
            atom.state = ret.pandasite_state

            # set if there is a bare nucleus mode (only, allow)
            mode = ret.bare_nucleus_mode()
            if mode:
                atom.set_bare_nucleus_mode(mode)

            # set if this is a secondary nucleus
            secondary = ret.secondary_nucleus()
            if secondary:
                atom.set_secondary_nucleus(secondary)

            # set if this is a satellite
            if ret.role == "satellite":
                atom.set_satellite()

            # set the default endpoint out
            try:
                atom.set_default_endpoint_out(ret.ddm_endpoints_output["default"].getDefaultWrite())
            except Exception:
                pass

            # add the atom to the dictionary of nuclei/satellites in the SiteMapper object
            target[ret.pandasite] = atom

        # add the site name and ddm endpoints
        target[ret.pandasite].add(ret.sitename, ret.ddm_endpoints_output, ret.ddm_endpoints_input)

    def clean_site_name(self, site_name):
        try:
            if site_name.startswith(NUCLEUS_TAG):
                tmp_name = site_name.split(":")[-1]
                if tmp_name in self.nuclei:
                    site_name = self.nuclei[tmp_name].getOnePandaSite()
                elif tmp_name in self.satellites:
                    site_name = self.satellites[tmp_name].getOnePandaSite()
        except Exception:
            pass

        return site_name

    # accessor for site
    def getSite(self, site_name):
        site_name = self.clean_site_name(site_name)

        # Return the site spec if it exists
        if site_name in self.siteSpecList:
            return self.siteSpecList[site_name]

        # return default site
        return DEFAULT_SITE

    # check if site exists
    def checkSite(self, site_name):
        site_name = self.clean_site_name(site_name)
        return site_name in self.siteSpecList

    # resolve nucleus
    def resolveNucleus(self, site_name):
        site_name = self.clean_site_name(site_name)
        if site_name == "NULL":
            site_name = None
        return site_name

    # accessor for cloud
    def getCloud(self, cloud):
        if cloud in self.cloudSpec:
            return self.cloudSpec[cloud]

        if cloud == WORLD_CLOUD:
            return self.worldCloudSpec

        # return some default sites
        default_cloud = {
            "source": "default",
            "sites": self.cloudSpec["US"]["sites"],
        }
        return default_cloud

    # accessor for cloud
    def checkCloud(self, cloud):
        if cloud in self.cloudSpec:
            return True

        if cloud == WORLD_CLOUD:
            return True

        return False

    # accessor for cloud list
    def getCloudList(self):
        return list(self.cloudSpec)

    # get DDM endpoint
    def getDdmEndpoint(self, site_name, storage_token, prod_source_label, job_label):
        # Skip if site doesn't exist
        if not self.checkSite(site_name):
            return None

        # Get the site spec and the input/output scope
        site_spec = self.getSite(site_name)
        scope_input, scope_output = select_scope(site_spec, prod_source_label, job_label)

        if storage_token in site_spec.setokens_output[scope_output]:
            return site_spec.setokens_output[scope_output][storage_token]

        return site_spec.ddm_output[scope_output]

    # get nucleus
    def getNucleus(self, site_name):
        if site_name in self.nuclei:
            return self.nuclei[site_name]
        if site_name in self.satellites:
            return self.satellites[site_name]
        return None

    def dump_site_information(self):
        _logger.debug("========= site dump =========")
        for tmp_site_spec in self.siteSpecList.values():
            _logger.debug(f"Site->{str(tmp_site_spec)}")

    def dump_cloud_information(self):
        for cloud_name_tmp, cloud_spec_tmp in self.cloudSpec.items():
            # Generate lists of sites with special cases (offline or not existing)
            sites_with_issues = []
            sites_offline = []
            for site_name_tmp in cloud_spec_tmp["sites"]:
                if site_name_tmp not in self.siteSpecList:
                    sites_with_issues.append({site_name_tmp})
                    continue

                site_spec_tmp = self.siteSpecList[site_name_tmp]
                if site_spec_tmp.status == "offline":
                    sites_offline.append(f"{site_name_tmp}")

            _logger.debug(f"========= {cloud_name_tmp} cloud dump =========")
            _logger.debug(f"Cloud:{cloud_name_tmp} has {cloud_spec_tmp['sites']}")
            if sites_offline:
                _logger.debug(f"Cloud:{cloud_name_tmp} has sites offline:{sites_offline}")
            if sites_with_issues:
                _logger.debug(f"Cloud:{cloud_name_tmp} has sites that don't exist:{sites_with_issues}")

        # dump the WORLD cloud sites
        _logger.debug("========= WORLD cloud dump =========")
        _logger.debug(f"Cloud:WORLD has {self.worldCloudSpec['sites']}")
