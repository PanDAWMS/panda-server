"""
provide primitive methods for DDM

"""

import sys
import types
import commands
from config import panda_config


# change cwd
_cwd = 'cd %s > /dev/null 2>&1; export HOME=%s; ' % (panda_config.home_dir_cwd,panda_config.home_dir_cwd)

# environment variables
_env = 'PATH=%s:%s:$PATH '    % (panda_config.native_python,panda_config.globus_dir+'/bin')
_env+= 'LD_LIBRARY_PATH=%s '  % (panda_config.globus_dir+'/lib')
_env+= 'DQ2_HOME=%s/opt/dq2 ' % panda_config.dq2_dir
_env+= 'http_proxy=%s '       % panda_config.httpProxy
_env+= 'https_proxy=%s '      % panda_config.httpProxy

_env+= 'PYTHONPATH=%s/usr/lib/python2.3/site-packages:$PYTHONPATH' \
       % panda_config.dq2_dir

# method object wrapping DQ2 method
class _DQMethod:
    # constructor
    def __init__(self,moduleName,methodName):
        self.moduleName = moduleName
        self.methodName = methodName

    # method emulation
    def __call__(self,*args,**kwargs):
        # main method has disappeared since 0.3
        args = list(args)
        if self.methodName == 'main':
            self.methodName = args[0]
            args.pop(0)
        # build command
        com  = 'from dq2.clientapi import DQ2; '
        if 'force_backend' in kwargs and kwargs['force_backend'] != None:
            com += 'dq2api = DQ2(force_backend="{0}"); '.format(kwargs['force_backend'])
        else:
            com += 'dq2api = DQ2(); '
        try:
            del kwargs['force_backend']
        except:
            pass
        if self.moduleName == 'DQ2':
            # DQ2 is top-level module
            com += 'print dq2api.%s(' % self.methodName
        elif self.moduleName == 'DQ2_iter':
            # iterator
            com += 'iter = dq2api.%s(' % self.methodName
        else:
            com += 'print dq2api.%s.%s(' % (self.moduleName,self.methodName)
        # expand args
        for i in range(len(args)):
            arg = args[i]
            if isinstance(arg,types.StringType):
                # check invalid characters
                for invCh in ['"',"'",'(',')',';']:
                    if invCh in arg:
                        return -1,"invalid character %s in %s" % (invCh,arg)
                com = "%s'%s'," % (com,arg)
            else:
                com = '%s%s,' % (com,str(arg))
        for tmpK,tmpV in kwargs.iteritems():
            if isinstance(tmpV,types.StringType):
                com += "%s='%s'," % (tmpK,tmpV)
            else:
                com += "%s=%s," % (tmpK,tmpV)
        com = com[:-1]        
        com += ")"
        # loop over iterator
        if self.moduleName == 'DQ2_iter':
            com += ";exec 'for item in iter:print item'"
        # execute
        return commands.getstatusoutput('%s env %s python -c "%s"' % (_cwd,_env,com))
        

# DQ module class
class _DQModule:
    # constructor
    def __init__(self,moduleName):
        self.moduleName = moduleName

    # factory method
    def __getattr__(self,methodName):
        return _DQMethod(self.moduleName,methodName)


# native DQ2 method class
class NativeDQ2Method:
    # constructor
    def __init__(self):
        self.moduleName = None
        self.methodName = None
    # set module and method name
    def setNames(self,moduleName,methodName):
        self.moduleName = moduleName
        self.methodName = methodName
    # method emulation
    def __call__(self,*args,**kwargs):
        try:
            # make dq2api locally since global dq2 object is not thread-safe
            from dq2.clientapi import DQ2
            if 'force_backend' in kwargs and kwargs['force_backend'] != None:
                dq2api = DQ2.DQ2(force_backend=kwargs['force_backend'])
            else:
                dq2api = DQ2.DQ2()
            try:
                del kwargs['force_backend']
            except:
                pass
            # main method has disappeared since 0.3
            args = list(args)
            if self.methodName == 'main':
                self.methodName = args[0]
                args.pop(0)
            # get method object
            if self.moduleName in ['DQ2','DQ2_iter']:
                methodObj = getattr(dq2api,self.methodName)
            else:
                methodObj = getattr(getattr(dq2api,self.moduleName),self.methodName)
            # execute
            retVal = apply(methodObj,args,kwargs)
            # loop over for iterator
            if self.moduleName == 'DQ2_iter':
                strRet = ''
                for item in retVal:
                    strRet += str(item)
            else:
                strRet = str(retVal)
            # return
            return 0,strRet
        except:
            errType,errVale = sys.exc_info()[:2]
            return 1,'%s %s' % (errType,errVale)
        
    
        
# native DQ2 module class
class NativeDQ2Module:
    # constructor
    def __init__(self):
        self.moduleName = None
    # set module name
    def setModName(self,moduleName):
        self.moduleName = moduleName
    # getter
    def __getattr__(self,methodName):
        # set method name
        api = NativeDQ2Method()        
        api.setNames(self.moduleName,methodName)
        return api
        

# factory class
class DDM:
    # constructor
    def __init__(self):
        self.usingNativeDQ2 = False
    # switch to use DQ2 in the same session
    def useDirectDQ2(self):
        self.usingNativeDQ2 = True
    # getter    
    def __getattr__(self,moduleName):
        if not self.usingNativeDQ2:
            # run dq2 comamnd in another session
            return _DQModule(moduleName)
        else:
            # run dq2 command in the same session
            nativeDQ2 = NativeDQ2Module()
            nativeDQ2.setModName(moduleName)
            return nativeDQ2

# instantiate
ddm = DDM()
del DDM


# method object wrapping TOA method
class _TOAMethod:
    # constructor
    def __init__(self,methodName):
        self.methodName = methodName

    # method emulation
    def __call__(self,*args):
        args = list(args)
        # build command
        com  = 'from dq2.info import TiersOfATLAS; '
        com += 'print TiersOfATLAS.%s(' % self.methodName
        # expand args
        for i in range(len(args)):
            arg = args[i]
            if isinstance(arg,types.StringType):
                com += "'%s'," % arg
            else:
                com += '%s,' % arg
        com = com[:-1]        
        com += ")"
        # execute
        return commands.getstatusoutput('%s env %s python -c "%s"' % (_cwd,_env,com))


# native ToA method class
class NativeTOAMethod:
    # constructor
    def __init__(self):
        self.methodName = None
        from dq2.info import TiersOfATLAS
        self.api = TiersOfATLAS
    # set method name
    def setName(self,methodName):
        self.methodName = methodName
    # method emulation
    def __call__(self,*args,**kwargs):
        try:
            methodObj = getattr(self.api,self.methodName)
            # execute
            retVal = apply(methodObj,args,kwargs)
            strRet = str(retVal)
            # return
            return 0,strRet
        except:
            errType,errVale = sys.exc_info()[:2]
            return 1,'%s %s' % (errType,errVale)


# TOA module class
class TOA:
    # constructor
    def __init__(self):
        self.usingNativeDQ2 = False
        self.nativeTOA = None
    # getter
    def __getattr__(self,methodName):
        if not ddm.usingNativeDQ2:
            # run dq2 comamnd in another session
            return _TOAMethod(methodName)
        else:
            # make method object
            if self.nativeTOA == None:
                self.nativeTOA = NativeTOAMethod()
            # run dq2 command in the same session
            self.nativeTOA.setName(methodName)
            return self.nativeTOA
                                

    
# instantiate
toa = TOA()
del TOA


# method object wrapping Dashboard method
class _DashBoradMethod:
    # constructor
    def __init__(self,methodName):
        self.methodName = methodName

    # method emulation
    def __call__(self,*args):
        args = list(args)
        # build command
        com  = "import sys;sys.stderr=open('/dev/null','w');"
        com += "import datetime;from dashboard.api.data.DataQuery import DataQuery;"
        com += "sys.stderr=sys.__stderr__;"
        com += "dash=DataQuery('dashb-atlas-data.cern.ch', 80);"
        com += "print dash.%s(%s,'%s'," % (self.methodName,args[0],args[1])
        com += "startDate=datetime.datetime.utcnow()-datetime.timedelta(hours=24))"
        # execute
        return commands.getstatusoutput('%s python -c "%s"' % (_cwd,com))


# TOA module class
class DashBorad:
    def __getattr__(self,methodName):
        return _DashBoradMethod(methodName)

# instantiate
dashBorad = DashBorad()
del DashBorad
    

# method object wrapping DQ2Info method
class _DQ2InfoMethod:
    # constructor
    def __init__(self,methodName):
        self.methodName = methodName

    # method emulation
    def __call__(self,*args):
        args = list(args)
        # build command
        com  = 'from dq2.info.client.infoClient import infoClient; '
        com += 'print infoClient().%s(' % self.methodName
        # expand args
        for i in range(len(args)):
            arg = args[i]
            if isinstance(arg,types.StringType):
                com += "'%s'," % arg
            else:
                com = '%s,' % arg
        com = com[:-1]        
        com += ")"
        # execute
        return commands.getstatusoutput('%s env %s python -c "%s"' % (_cwd,_env,com))


# TOA module class
class DQ2Info:
    def __getattr__(self,methodName):
        return _DQ2InfoMethod(methodName)

    
# instantiate
dq2Info = DQ2Info()
del DQ2Info


# method object wrapping dq2 common
class _DQ2CommonMethod:
    # constructor
    def __init__(self,methodName):
        self.methodName = methodName

    # method emulation
    def __call__(self,*args):
        args = list(args)
        # build command
        com  = 'from dq2.common import %s; ' % self.methodName
        com += 'print %s(' % self.methodName
        # expand args
        for i in range(len(args)):
            arg = args[i]
            if isinstance(arg,types.StringType):
                com += "'%s'," % arg
            else:
                com = '%s,' % arg
        com = com[:-1]        
        com += ")"
        # execute
        return commands.getstatusoutput('%s env %s python -c "%s"' % (_cwd,_env,com))


# TOA module class
class DQ2Common:
    def __getattr__(self,methodName):
        return _DQ2CommonMethod(methodName)

    
# instantiate
dq2Common = DQ2Common()
del DQ2Common
