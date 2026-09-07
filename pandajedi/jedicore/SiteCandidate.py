from collections.abc import Iterable
from typing import Any

from pandaserver.taskbuffer.FileSpec import FileSpec
from pandaserver.taskbuffer.JediFileSpec import JediFileSpec

# The sets below hold file IDs. A JediFileSpec leaves an unset fileID None, and the job
# FileSpec substitutes the string "NULL" for it (see spec_column.py), so both forms of
# "no ID" can reach these sets
FileID = int | str | None


class SiteCandidate(object):
    def __init__(self, siteName: str, unifiedName: str | None = None) -> None:
        # the site name
        self.siteName = siteName
        # unified name
        self.unifiedName = unifiedName
        # the weight for the brokerage
        self.weight: float = 0
        # the list of files copied from SE disk
        self.localDiskFiles: set[FileID] = set()
        # the list of files copied from SE tape
        self.localTapeFiles: set[FileID] = set()
        # the list of files cached in non-SE, e.g. on CVMFS
        self.cacheFiles: set[FileID] = set()
        # the list of files read from SE using remote I/O
        self.remoteFiles: set[FileID] = set()
        # the list of all files, or None when nobody has declared which files are available
        self.allFiles: set[FileID] | None = None
        # remote access protocol
        self.remoteProtocol: str | None = None
        # remote source if any
        self.remoteSource: str | None = None
        # number of running job
        self.nRunningJobs: int | None = None
        # number of queued jobs
        self.nQueuedJobs: int | None = None
        # number of assigned jobs
        self.nAssignedJobs: int | None = None
        # cap on running jobs. The brokers install a number here; None means no cap was
        # worked out for this candidate, which can_accept_jobs below reads as no cap.
        self.nRunningJobsCap: int | None = None
        # overridden attributes
        self.overriddenAttrs: dict[str, Any] = {}

    # add local disk files
    def add_local_disk_files(self, files: Iterable[JediFileSpec]) -> None:
        self.localDiskFiles = self.localDiskFiles.union([f.fileID for f in files])

    # add local tape files
    def add_local_tape_files(self, files: Iterable[JediFileSpec]) -> None:
        self.localTapeFiles = self.localTapeFiles.union([f.fileID for f in files])

    # add cache files
    def add_cache_files(self, files: Iterable[JediFileSpec]) -> None:
        self.cacheFiles = self.cacheFiles.union([f.fileID for f in files])

    # add remote files
    def add_remote_files(self, files: Iterable[JediFileSpec]) -> None:
        self.remoteFiles = self.remoteFiles.union([f.fileID for f in files])

    # get locality of a file. The callers pass a JEDI file spec from a dataset and a job
    # file spec built from one, so both have to be accepted
    def getFileLocality(self, fileSpec: FileSpec | JediFileSpec) -> str | None:
        if fileSpec.fileID in self.localDiskFiles:
            return "localdisk"
        if fileSpec.fileID in self.localTapeFiles:
            return "localtape"
        if fileSpec.fileID in self.cacheFiles:
            return "cache"
        if fileSpec.fileID in self.remoteFiles:
            return "remote"
        return None

    # add available files
    def addAvailableFiles(self, fileList: Iterable[JediFileSpec]) -> None:
        if self.allFiles is None:
            self.allFiles = set()
        for tmpFileSpec in fileList:
            self.allFiles.add(tmpFileSpec.fileID)

    # check if file is available
    def isAvailableFile(self, tmpFileSpec: FileSpec | JediFileSpec) -> bool:
        # N/A
        if self.allFiles is None:
            return True
        return tmpFileSpec.fileID in self.allFiles

    # check if still can accept jobs
    def can_accept_jobs(self) -> bool:
        if self.nRunningJobsCap is None or self.nQueuedJobs is None:
            return True
        if self.nRunningJobsCap > self.nQueuedJobs:
            return True
        return False

    # override an attribute
    def override_attribute(self, key: str, value: Any) -> None:
        self.overriddenAttrs[key] = value

    # get an overridden attribute
    def get_overridden_attribute(self, key: str) -> Any:
        return self.overriddenAttrs.get(key)
