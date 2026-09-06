import math
from collections.abc import Collection
from typing import TYPE_CHECKING, Any

from pandajedi.jedicore import Interaction

if TYPE_CHECKING:
    # Importing these for real makes this module read a configuration file at import time,
    # and it has no other reason to need one. Annotations are evaluated at runtime in this
    # tree, so the uses below are quoted.
    from pandacommon.pandalogger.LogWrapper import LogWrapper

    from pandajedi.jedicore.JediTaskBufferInterface import JediTaskBufferInterface
    from pandajedi.jedicore.ThreadUtils import MapWithLock


# base class for job brokerage
class JobBrokerBase(object):
    # installed on this class by Interaction.installSC() at the bottom of this module
    SC_SUCCEEDED: Interaction.StatusCode
    SC_FAILED: Interaction.StatusCode
    SC_FATAL: Interaction.StatusCode

    def __init__(self, ddmIF: Interaction.CommandSendInterface, taskBufferIF: "JediTaskBufferInterface") -> None:
        self.ddmIF = ddmIF
        self.taskBufferIF = taskBufferIF
        self.liveCounter: "MapWithLock | None" = None
        self.lockID: str | None = None
        self.baseLockID: str | None = None
        self.useLock = False
        self.testMode = False
        self.refresh()
        # replaced by set_task_common_dict() with the dict shared across the brokers of
        # one task; empty until then, so a write before that is not lost to an exception
        self.task_common: dict[str, Any] = {}
        self.summaryList: list[str] = []

    # set task common dictionary
    def set_task_common_dict(self, task_common: dict[str, Any]) -> None:
        self.task_common = task_common

    # get task common attribute
    def get_task_common(self, attr_name: str) -> Any:
        if self.task_common:
            return self.task_common.get(attr_name)

    # set task common attribute
    def set_task_common(self, attr_name: str, attr_value: Any) -> None:
        self.task_common[attr_name] = attr_value

    def refresh(self) -> None:
        self.siteMapper = self.taskBufferIF.get_site_mapper()

    def setLiveCounter(self, liveCounter: "MapWithLock") -> None:
        self.liveCounter = liveCounter

    def getLiveCount(self, siteName: str) -> Any:
        if self.liveCounter is None:
            return 0
        return self.liveCounter.get(siteName)

    # only interpolated into the lock ID below, and the callers pass both a str and an int
    def setLockID(self, pid: str | int, tid: int) -> None:
        self.baseLockID = f"{pid}-jbr"
        self.lockID = f"{self.baseLockID}-{tid}"

    def getBaseLockID(self) -> str | None:
        if self.useLock:
            return self.baseLockID
        return None

    # every argument is bound into the lock query, where a NULL simply matches no row
    def checkSiteLock(self, vo: str | None, prodSourceLabel: str | None, siteName: str, queue_id: int | None, resource_name: str | None) -> bool:
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

    def setTestMode(self) -> None:
        self.testMode = True

    # get list of unified sites
    def get_unified_sites(self, scan_site_list: Collection[str]) -> list[str]:
        unified_list = set()
        for tmpSiteName in scan_site_list:
            tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
            unifiedName = tmpSiteSpec.get_unified_name()
            unified_list.add(unifiedName)
        return list(unified_list)

    # get list of pseudo sites
    def get_pseudo_sites(self, unified_list: Collection[str], scan_site_list: Collection[str]) -> list[str]:
        unified_names = set(unified_list)
        pseudo_list = set()
        for tmpSiteName in scan_site_list:
            tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
            if tmpSiteSpec.get_unified_name() in unified_names:
                pseudo_list.add(tmpSiteName)
        return list(pseudo_list)

    # add pseudo sites to skip
    def add_pseudo_sites_to_skip(self, unified_dict: dict[str, Any], scan_site_list: Collection[str], skipped_dict: dict[str, Any]) -> dict[str, Any]:
        for tmpSiteName in scan_site_list:
            tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
            if tmpSiteSpec.get_unified_name() in unified_dict:
                skipped_dict[tmpSiteName] = unified_dict[tmpSiteSpec.get_unified_name()]
        return skipped_dict

    # init summary list
    def init_summary_list(self, header: str, comment: str | None, initial_list: Collection[Any]) -> None:
        self.summaryList = []
        self.summaryList.append(f"===== {header} =====")
        if comment:
            self.summaryList.append(comment)
        self.summaryList.append(f"the number of initial candidates: {len(initial_list)}")

    # dump summary
    def dump_summary(self, tmp_log: "LogWrapper", final_candidates: Collection[Any] | None = None) -> None:
        if not self.summaryList:
            return
        tmp_log.info("")
        for m in self.summaryList:
            tmp_log.info(m)
        if not final_candidates:
            final_candidates = []
        tmp_log.info(f"the number of final candidates: {len(final_candidates)}")
        tmp_log.info("")

    # add summary entry and show intermediate message
    def add_summary_message(self, old_list: Collection[str], new_list: Collection[str], message: str, tmp_log: Any, msg_map: dict[str, str]) -> None:
        # consolidate lists to emit messages only for unified sites
        old_unified = self.get_unified_sites(old_list)
        new_unified = self.get_unified_sites(new_list)
        # get skipped sites
        skipped = [i for i in old_unified if i not in new_unified]
        for skipped_site in skipped:
            if skipped_site in msg_map:
                tmp_log.info(msg_map[skipped_site])
        tmp_log.info(f"{len(new_unified)} candidates passed {message}")
        # add a summary entry
        if old_unified and len(old_unified) != len(new_unified):
            red = int(math.ceil(((len(old_unified) - len(new_unified)) * 100) / len(old_unified)))
            self.summaryList.append(f"{len(old_unified):>5} -> {len(new_unified):>3} candidates, {red:>3}% cut : {message}")


Interaction.installSC(JobBrokerBase)
