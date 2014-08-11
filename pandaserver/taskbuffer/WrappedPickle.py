import sys
import StringIO
import cPickle as pickle

# wrapper to avoid de-serializing unsafe objects
class WrappedPickle(object):
    # allowed modules and classes
    allowedModClass = {
        'copy_reg'            : ['_reconstructor'],
        '__builtin__'         : ['object'],
        'datetime'            : ['datetime'],
        'taskbuffer.JobSpec'  : ['JobSpec'],
        'taskbuffer.FileSpec' : ['FileSpec'],
        'taskbuffer.JobSpecHTCondor'  : ['JobSpecHTCondor'],
        }

    # check module and class 
    @classmethod
    def find_class(cls,module,name):
        # check module
        if not cls.allowedModClass.has_key(module):
            raise pickle.UnpicklingError,'Attempting to import disallowed module %s' % module
        # import module
        __import__(module)
        mod = sys.modules[module]
        # check class
        if not name in cls.allowedModClass[module]:
            raise pickle.UnpicklingError,'Attempting to get disallowed class %s in %s' % (name,module)
        klass = getattr(mod,name)
        return klass

    # loads
    @classmethod
    def loads(cls,pickle_string):
        pickle_obj = pickle.Unpickler(StringIO.StringIO(pickle_string))
        pickle_obj.find_global = cls.find_class
        return pickle_obj.load()

    
