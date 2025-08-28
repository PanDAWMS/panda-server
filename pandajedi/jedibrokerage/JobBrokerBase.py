import math

from pandajedi.jedicore import Interaction


# base class for job brokerage
class JobBrokerBase(object):
    def __init__(self, ddmIF, taskBufferIF):
        self.ddmIF = ddmIF
        self.taskBufferIF = taskBufferIF
        self.liveCounter = None
        self.lockID = None
        self.baseLockID = None
        self.useLock = False
        self.testMode = False
        self.refresh()
        self.task_common = None
        self.summaryList = None

    # set task common dictionary
    def set_task_common_dict(self, task_common):
        self.task_common = task_common

    # get task common attribute
    def get_task_common(self, attr_name):
        if self.task_common:
            return self.task_common.get(attr_name)

    # set task common attribute
    def set_task_common(self, attr_name, attr_value):
        self.task_common[attr_name] = attr_value

    def refresh(self):
        self.siteMapper = self.taskBufferIF.get_site_mapper()

    def setLiveCounter(self, liveCounter):
        self.liveCounter = liveCounter

    def getLiveCount(self, siteName):
        if self.liveCounter is None:
            return 0
        return self.liveCounter.get(siteName)

    def setLockID(self, pid, tid):
        self.baseLockID = f"{pid}-jbr"
        self.lockID = f"{self.baseLockID}-{tid}"

    def getBaseLockID(self):
        if self.useLock:
            return self.baseLockID
        return None

    def checkSiteLock(self, vo, prodSourceLabel, siteName, queue_id, resource_name):
        return self.taskBufferIF.checkProcessLock_JEDI(
            vo=vo,
            prodSourceLabel=prodSourceLabel,
            cloud=siteName,
            workqueue_id=queue_id,
            resource_name=resource_name,
            component=None,
            pid=self.baseLockID,
            checkBase=True,
        )

    def setTestMode(self):
        self.testMode = True

    # get list of unified sites
    def get_unified_sites(self, scan_site_list):
        unified_list = set()
        for tmpSiteName in scan_site_list:
            tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
            unifiedName = tmpSiteSpec.get_unified_name()
            unified_list.add(unifiedName)
        return tuple(unified_list)

    # get list of pseudo sites
    def get_pseudo_sites(self, unified_list, scan_site_list):
        unified_list = set(unified_list)
        pseudo_list = set()
        for tmpSiteName in scan_site_list:
            tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
            if tmpSiteSpec.get_unified_name() in unified_list:
                pseudo_list.add(tmpSiteName)
        return tuple(pseudo_list)

    # add pseudo sites to skip
    def add_pseudo_sites_to_skip(self, unified_dict, scan_site_list, skipped_dict):
        for tmpSiteName in scan_site_list:
            tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
            if tmpSiteSpec.get_unified_name() in unified_dict:
                skipped_dict[tmpSiteName] = unified_dict[tmpSiteSpec.get_unified_name()]
        return skipped_dict

    # init summary list
    def init_summary_list(self, header, comment, initial_list):
        self.summaryList = []
        self.summaryList.append(f"===== {header} =====")
        if comment:
            self.summaryList.append(comment)
        self.summaryList.append(f"the number of initial candidates: {len(initial_list)}")

    # dump summary
    def dump_summary(self, tmp_log, final_candidates=None):
        if not self.summaryList:
            return
        tmp_log.info("")
        for m in self.summaryList:
            tmp_log.info(m)
        if not final_candidates:
            final_candidates = []
        tmp_log.info(f"the number of final candidates: {len(final_candidates)}")
        tmp_log.info("")

    # make summary
    def add_summary_message(self, old_list, new_list, message):
        if old_list and len(old_list) != len(new_list):
            red = int(math.ceil(((len(old_list) - len(new_list)) * 100) / len(old_list)))
            self.summaryList.append(f"{len(old_list):>5} -> {len(new_list):>3} candidates, {red:>3}% cut : {message}")


Interaction.installSC(JobBrokerBase)
