class SiteCandidate(object):
    def __init__(self, siteName, unifiedName=None):
        # the site name
        self.siteName = siteName
        # unified name
        self.unifiedName = unifiedName
        # the weight for the brokerage
        self.weight = 0
        # the list of files copied from SE disk
        self.localDiskFiles = set()
        # the list of files copied from SE tape
        self.localTapeFiles = set()
        # the list of files cached in non-SE, e.g. on CVMFS
        self.cacheFiles = set()
        # the list of files read from SE using remote I/O
        self.remoteFiles = set()
        # the list of all files
        self.allFiles = None
        # remote access protocol
        self.remoteProtocol = None
        # remote source if any
        self.remoteSource = None
        # number of running job
        self.nRunningJobs = None
        # number of queued jobs
        self.nQueuedJobs = None
        # number of assigned jobs
        self.nAssignedJobs = None
        # cap on running jobs
        self.nRunningJobsCap = None
        # overridden attributes
        self.overriddenAttrs = {}

    # add local disk files
    def add_local_disk_files(self, files):
        self.localDiskFiles = self.localDiskFiles.union([f.fileID for f in files])

    # add local tape files
    def add_local_tape_files(self, files):
        self.localTapeFiles = self.localTapeFiles.union([f.fileID for f in files])

    # add cache files
    def add_cache_files(self, files):
        self.cacheFiles = self.cacheFiles.union([f.fileID for f in files])

    # add remote files
    def add_remote_files(self, files):
        self.remoteFiles = self.remoteFiles.union([f.fileID for f in files])

    # get locality of a file
    def getFileLocality(self, fileSpec):
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
    def addAvailableFiles(self, fileList):
        if self.allFiles is None:
            self.allFiles = set()
        for tmpFileSpec in fileList:
            self.allFiles.add(tmpFileSpec.fileID)

    # check if file is available
    def isAvailableFile(self, tmpFileSpec):
        # N/A
        if self.allFiles is None:
            return True
        return tmpFileSpec.fileID in self.allFiles

    # check if still can accept jobs
    def can_accept_jobs(self):
        if self.nRunningJobsCap is None or self.nQueuedJobs is None:
            return True
        if self.nRunningJobsCap > self.nQueuedJobs:
            return True
        return False

    # override an attribute
    def override_attribute(self, key, value):
        self.overriddenAttrs[key] = value

    # get an overridden attribute
    def get_overridden_attribute(self, key):
        return self.overriddenAttrs.get(key)
