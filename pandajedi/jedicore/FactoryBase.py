import sys

from .MsgWrapper import MsgWrapper

_factoryModuleName = __name__.split(".")[-1]


# base class for factory
class FactoryBase:
    # constructor
    def __init__(self, vos, sourceLabels, logger, modConfig):
        if isinstance(vos, list):
            self.vos = vos
        else:
            try:
                self.vos = vos.split("|")
            except Exception:
                self.vos = [vos]
        if isinstance(sourceLabels, list):
            self.sourceLabels = sourceLabels
        else:
            try:
                self.sourceLabels = sourceLabels.split("|")
            except Exception:
                self.sourceLabels = [sourceLabels]
        self.modConfig = modConfig
        self.logger = MsgWrapper(logger, _factoryModuleName)
        self.implMap = {}
        self.className = None
        self.classMap = {}

    # initialize all modules
    def initializeMods(self, *args):
        # parse config
        for configStr in self.modConfig.split(","):
            configStr = configStr.strip()
            items = configStr.split(":")
            # check format
            try:
                vos = items[0].split("|")
                sourceLabels = items[1].split("|")
                moduleName = items[2]
                className = items[3]
                try:
                    subTypes = items[4].split("|")
                except Exception:
                    subTypes = ["any"]
            except Exception:
                self.logger.error(f"wrong config definition : {configStr}")
                continue
            # loop over all VOs
            for vo in vos:
                # loop over all labels
                for sourceLabel in sourceLabels:
                    # check vo and sourceLabel if specified
                    if vo not in ["", "any"] and vo not in self.vos and None not in self.vos and "any" not in self.vos:
                        continue
                    if (
                        sourceLabel not in ["", "any"]
                        and sourceLabel not in self.sourceLabels
                        and None not in self.sourceLabels
                        and "any" not in self.sourceLabels
                    ):
                        continue
                    # loop over all sub types
                    for subType in subTypes:
                        # import
                        try:
                            # import module
                            self.logger.info(f"vo={vo} label={sourceLabel} subtype={subType}")
                            self.logger.info(f"importing {moduleName}")
                            mod = __import__(moduleName)
                            for subModuleName in moduleName.split(".")[1:]:
                                mod = getattr(mod, subModuleName)
                            # get class
                            self.logger.info(f"getting class {className}")
                            cls = getattr(mod, className)
                            # instantiate
                            self.logger.info("instantiating")
                            impl = cls(*args)
                            # set vo
                            impl.vo = vo
                            impl.prodSourceLabel = sourceLabel
                            # append
                            if vo not in self.implMap:
                                self.implMap[vo] = {}
                                self.classMap[vo] = {}
                            if sourceLabel not in self.implMap[vo]:
                                self.implMap[vo][sourceLabel] = {}
                                self.classMap[vo][sourceLabel] = {}
                            self.implMap[vo][sourceLabel][subType] = impl
                            self.classMap[vo][sourceLabel][subType] = cls
                            self.logger.info(f"{cls} is ready for {vo}:{sourceLabel}:{subType}")
                        except Exception:
                            errtype, errvalue = sys.exc_info()[:2]
                            self.logger.error(
                                "failed to import {mn}.{cn} for vo={vo} label={lb} subtype={st} due to {et} {ev}".format(
                                    et=errtype.__name__, ev=errvalue, st=subType, vo=vo, lb=sourceLabel, cn=className, mn=moduleName
                                )
                            )
                            raise ImportError(f"failed to import {moduleName}.{className}")
        # return
        return True

    # get implementation for vo and sourceLabel. Only work with initializeMods()
    def getImpl(self, vo, sourceLabel, subType="any", doRefresh=True):
        # check VO
        if vo in self.implMap:
            # match VO
            voImplMap = self.implMap[vo]
        elif "any" in self.implMap:
            # catch all
            voImplMap = self.implMap["any"]
        else:
            return None
        # check sourceLabel
        if sourceLabel in voImplMap:
            # match sourceLabel
            srcImplMap = voImplMap[sourceLabel]
        elif "any" in voImplMap:
            # catch all
            srcImplMap = voImplMap["any"]
        else:
            return None
        # check subType
        if subType in srcImplMap:
            # match subType
            tmpImpl = srcImplMap[subType]
            if doRefresh:
                tmpImpl.refresh()
            return tmpImpl
        elif "any" in srcImplMap:
            # catch all
            tmpImpl = srcImplMap["any"]
            if doRefresh:
                tmpImpl.refresh()
            return tmpImpl
        else:
            return None

    # instantiate implementation for vo and sourceLabel. Only work with initializeMods()
    def instantiateImpl(self, vo, sourceLabel, subType, *args):
        # check VO
        if vo in self.classMap:
            # match VO
            voImplMap = self.classMap[vo]
        elif "any" in self.classMap:
            # catch all
            voImplMap = self.classMap["any"]
        else:
            return None
        # check sourceLabel
        if sourceLabel in voImplMap:
            # match sourceLabel
            srcImplMap = voImplMap[sourceLabel]
        elif "any" in voImplMap:
            # catch all
            srcImplMap = voImplMap["any"]
        else:
            return None
        # check subType
        if subType in srcImplMap:
            # match subType
            impl = srcImplMap[subType](*args)
            impl.vo = vo
            impl.prodSourceLabel = sourceLabel
            return impl
        elif "any" in srcImplMap:
            # catch all
            impl = srcImplMap["any"](*args)
            impl.vo = vo
            impl.prodSourceLabel = sourceLabel
            return impl
        else:
            return None

    # get class name of impl
    def getClassName(self, vo=None, sourceLabel=None):
        impl = self.getImpl(vo, sourceLabel, doRefresh=False)
        if impl is None:
            return None
        return impl.__class__.__name__
