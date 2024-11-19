import decimal
import pickle
import sys
from copyreg import _reconstructor as map__reconstructor
from io import BytesIO


class Common_Unpickler(pickle.Unpickler):
    def __setattr__(self, key, value):
        if key == "find_global":
            pickle.Unpickler.__setattr__(self, "find_class", value)
        else:
            pickle.Unpickler.__setattr__(self, key, value)


# conversion for unserializable values
def conversion_func(item):
    if isinstance(item, list):
        return [conversion_func(i) for i in item]
    if isinstance(item, dict):
        return {k: conversion_func(item[k]) for k in item}
    if isinstance(item, decimal.Decimal):
        if item == item.to_integral_value():
            item = int(item)
        else:
            item = float(item)
    return item


# wrapper to avoid de-serializing unsafe objects
class WrappedPickle(object):
    # allowed modules and classes
    allowedModClass = {
        "copy_reg": ["_reconstructor"],
        "__builtin__": ["object"],
        "datetime": ["datetime"],
        "taskbuffer.JobSpec": ["JobSpec"],
        "taskbuffer.FileSpec": ["FileSpec"],
        "pandaserver.taskbuffer.JobSpec": ["JobSpec"],
        "pandaserver.taskbuffer.FileSpec": ["FileSpec"],
    }
    # bare modules
    bareMods = {"taskbuffer.": "pandaserver."}
    # predefined class map
    predefined_class = {
        ("copy_reg", "_reconstructor"): map__reconstructor,
        ("__builtin__", "object"): object,
    }

    # check module and class
    @classmethod
    def find_class(cls, module, name):
        # append prefix to bare modules
        for bareMod in cls.bareMods:
            if module.startswith(bareMod):
                module = cls.bareMods[bareMod] + module
                break
        # check module
        if module not in cls.allowedModClass:
            raise pickle.UnpicklingError(f"Attempting to import disallowed module {module}")
        # return predefined class
        key = (module, name)
        if key in cls.predefined_class:
            return cls.predefined_class[key]
        # import module
        __import__(module)
        mod = sys.modules[module]
        # check class
        if name not in cls.allowedModClass[module]:
            raise pickle.UnpicklingError(f"Attempting to get disallowed class {name} in {module}")
        klass = getattr(mod, name)
        return klass

    # loads
    @classmethod
    def loads(cls, pickle_string):
        if isinstance(pickle_string, str):
            pickle_string = pickle_string.encode()
        pickle_obj = Common_Unpickler(BytesIO(pickle_string))
        pickle_obj.find_global = cls.find_class
        return pickle_obj.load()

    # dumps
    @classmethod
    def dumps(cls, obj, convert_to_safe=False):
        if convert_to_safe:
            obj = conversion_func(obj)
        return pickle.dumps(obj, protocol=0)
