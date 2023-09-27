class AdderResult(object):
    # slot
    __slot__ = ("statusCode", "transferringFiles")

    # return code
    RC_Succeeded = 0
    RC_Temporary = 1
    RC_Fatal = 2

    # constructor
    def __init__(self):
        # result of interactions with data management system (DMS)
        self.statusCode = None

        # list of files which are being transferred by DMS
        self.transferringFiles = []

        # list of files which are being merged
        self.mergingFiles = []

    # succeeded
    def setSucceeded(self):
        self.statusCode = self.RC_Succeeded

    # temporary error to retry later
    def setTemporary(self):
        self.statusCode = self.RC_Temporary

    # fatal error
    def setFatal(self):
        self.statusCode = self.RC_Fatal

    # check if succeeded
    def isSucceeded(self):
        return self.statusCode == self.RC_Succeeded

    # check if temporary
    def isTemporary(self):
        return self.statusCode == self.RC_Temporary

    # check if fatal error
    def isFatal(self):
        return self.statusCode == self.RC_Fatal
