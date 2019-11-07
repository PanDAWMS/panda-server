import sys
from io import BytesIO
try:
    # python 2
    import cPickle as pickle
    from copy_reg import _reconstructor as map__reconstructor
except ImportError:
    # python 3
    import pickle
    from copyreg import _reconstructor as map__reconstructor

# define Unpickler
if pickle.__name__ == 'cPickle':
    # python 2
    Common_Unpickler = pickle.Unpickler
else:
    # python 3
    class Common_Unpickler(pickle.Unpickler):
        def __setattr__(self, key, value):
            if key == 'find_global':
                pickle.Unpickler.__setattr__(self, 'find_class', value)
            else:
                pickle.Unpickler.__setattr__(self, key, value)


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
    # predefined class map
    predefined_class = {('copy_reg', '_reconstructor'): map__reconstructor,
                        ('__builtin__', 'object'): object}

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
        # return predefined class
        key = (module, name)
        if key in cls.predefined_class:
            return cls.predefined_class[key]
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
        pickle_obj = Common_Unpickler(BytesIO(pickle_string.encode()))
        pickle_obj.find_global = cls.find_class
        return pickle_obj.load()

    # dumps
    @classmethod
    def dumps(cls, obj):
        return pickle.dumps(obj, protocol=0)
