# exception for unknown dataset
class UnknownDatasetError(Exception):
    pass


# temporary error in external system
class ExternalTempError(Exception):
    pass


# temporarily unavailable storage
class TempBadStorageError(Exception):
    pass


# no plugin configured for a vo/label pair
class NoPluginError(Exception):
    pass
