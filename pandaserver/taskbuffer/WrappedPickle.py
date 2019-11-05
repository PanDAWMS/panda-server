import sys
from io import BytesIO
try:
    import cPickle as pickle
except ImportError:
    import pickle

# wrapper to avoid de-serializing unsafe objects
class WrappedPickle(object):
    # allowed modules and classes
    allowedModClass = {
        'copy_reg'            : ['_reconstructor'],
        '__builtin__'         : ['object'],
        'datetime'            : ['datetime'],
        'taskbuffer.JobSpec'  : ['JobSpec'],
        'taskbuffer.FileSpec' : ['FileSpec'],
        'pandaserver.taskbuffer.JobSpec': ['JobSpec'],
        'pandaserver.taskbuffer.FileSpec' : ['FileSpec'],
        }
    # bare modules
    bareMods = {'taskbuffer.': 'pandaserver.'}

    # check module and class 
    @classmethod
    def find_class(cls,module,name):
        # append prefix to bare modules
        for bareMod in cls.bareMods:
            if module.startswith(bareMod):
                module = cls.bareMods[bareMod] + module
                break
        # check module
        if module not in cls.allowedModClass:
            raise pickle.UnpicklingError('Attempting to import disallowed module %s' % module)
        # import module
        __import__(module)
        mod = sys.modules[module]
        # check class
        if name not in cls.allowedModClass[module]:
            raise pickle.UnpicklingError('Attempting to get disallowed class %s in %s' % (name,module))
        klass = getattr(mod,name)
        return klass

    # loads
    @classmethod
    def loads(cls,pickle_string):
        pickle_obj = pickle.Unpickler(BytesIO(pickle_string))
        pickle_obj.find_global = cls.find_class
        return pickle_obj.load()
