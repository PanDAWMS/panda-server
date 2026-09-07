import logging
from collections.abc import Sequence
from typing import Any

from .MsgWrapper import MsgWrapper

_factoryModuleName = __name__.split(".")[-1]


# base class for factory
class FactoryBase:
    # constructor
    def __init__(self, vos: str | None | Sequence[str | None], sourceLabels: str | None | Sequence[str | None], logger: logging.Logger, modConfig: str) -> None:
        # A None vo or source label lands in the list as itself -- the .split() below
        # raises on it -- which is what the "None not in" tests further down look for.
        # Both are read-only after this, so a covariant Sequence is enough
        self.vos: Sequence[str | None]
        self.sourceLabels: Sequence[str | None]
        if isinstance(vos, list):
            self.vos = vos
        else:
            try:
                self.vos = vos.split("|")  # type: ignore[union-attr]  # None is what the except is for
            except Exception:
                self.vos = [vos]  # type: ignore[list-item]  # narrowed to str | None by the line above
        if isinstance(sourceLabels, list):
            self.sourceLabels = sourceLabels
        else:
            try:
                self.sourceLabels = sourceLabels.split("|")  # type: ignore[union-attr]  # None is what the except is for
            except Exception:
                self.sourceLabels = [sourceLabels]  # type: ignore[list-item]  # narrowed to str | None by the line above
        self.modConfig = modConfig
        self.logger = MsgWrapper(logger, _factoryModuleName)
        # vo -> source label -> sub type -> the plugin named in modConfig. Which class that
        # is comes from the config at runtime, so nothing narrower than Any can be said here
        self.implMap: dict[str, dict[str, dict[str, Any]]] = {}
        self.classMap: dict[str, dict[str, dict[str, type[Any]]]] = {}

    # initialize all modules. Returns True, or does not return at all: a plugin that fails
    # to import raises rather than being skipped
    def initializeMods(self, *args: Any) -> bool:
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
                        except Exception as e:
                            self.logger.error(
                                "failed to import {mn}.{cn} for vo={vo} label={lb} subtype={st} due to {et} {ev}".format(
                                    et=type(e).__name__, ev=e, st=subType, vo=vo, lb=sourceLabel, cn=className, mn=moduleName
                                )
                            )
                            raise ImportError(f"failed to import {moduleName}.{className}")
        # return
        return True

    # get implementation for vo and sourceLabel. Only work with initializeMods()
    def getImpl(self, vo: str | None, sourceLabel: str | None, subType: str | None = "any", doRefresh: bool = True) -> Any:
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
    def instantiateImpl(self, vo: str | None, sourceLabel: str | None, subType: str | None, *args: Any) -> Any:
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
    def getClassName(self, vo: str | None = None, sourceLabel: str | None = None) -> str | None:
        impl = self.getImpl(vo, sourceLabel, doRefresh=False)
        if impl is None:
            return None
        return impl.__class__.__name__
