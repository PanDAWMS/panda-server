import re
import os
import sys
import socket
import random

# error codes
EC_Main          = 70
EC_LFC           = 80

# import lfc api
try:
    import dq2.filecatalog
except:
    print "ERROR : could not import dq2.filecatalog"
    sys.exit(EC_LFC)


# get files from LFC
def _getFilesLFC(files,lfcHost,storages,verbose=False):
    if ':' in lfcHost:
        catStr = 'lfc://%s/grid/atlas' % lfcHost
    else:
        catStr = 'lfc://%s:/grid/atlas' % lfcHost
    try:
        # connect
        apiLFC = dq2.filecatalog.create_file_catalog(catStr)
        apiLFC.connect()
        # get PFN
        iGUID = 0
        nGUID = 1000
        pfnMap   = {}
        listGUID = {}
        for guid,tmpLFN in files.iteritems():
            if verbose:
                sys.stdout.write('.')
                sys.stdout.flush()
            iGUID += 1
            listGUID[guid] = tmpLFN
            if iGUID % nGUID == 0 or iGUID == len(files):
                # get replica
                resReplicas = apiLFC.bulkFindReplicas(listGUID)
                for retGUID,resValMap in resReplicas.iteritems():
                    for retSURL in resValMap['surls']:
                        # get host
                        match = re.search('^[^:]+://([^:/]+):*\d*/',retSURL)
                        if match==None:
                            continue
                        # check host
                        host = match.group(1)
                        if storages != [] and (not host in storages):
                            continue
                        # append
                        if not pfnMap.has_key(retGUID):
                            pfnMap[retGUID] = []
                        pfnMap[retGUID].append(retSURL)
                # reset                        
                listGUID = {}
        # disconnect
        apiLFC.disconnect()
    except:
        errType,errValue = sys.exc_info()[:2]
        print "ERROR: %s:%s" % (errType,errValue)
        sys.exit(EC_LFC)
    # collect LFNs
    retLFNs = {}
    for guid,lfn in files.iteritems():
        if guid in pfnMap.keys():
            retLFNs[lfn] = pfnMap[guid]
    # return
    return retLFNs
    


####################################################################
# main
def main():
    import sys
    import getopt
    # option class
    class _options:
        def __init__(self):
            pass
    options = _options()
    del _options
    # set default values
    options.verbose   = False
    options.lfns      = []
    options.guids     = []    
    options.lfchost   = ''
    options.storages  = []
    options.infile    = None
    options.outfile   = None
    # get command-line parameters
    try:
        opts, args = getopt.getopt(sys.argv[1:],"s:i:g:vl:o:f:")
    except:
        _usage()
        print "ERROR : Invalid options"
        sys.exit(EC_Main)    
    # set options
    for o, a in opts:
        if o in ("-v",):
            options.verbose = True
        if o in ("-s",):
            options.storages = a.split(',')
        if o in ("-i",):
            options.lfns = a.split(',')
        if o in ("-g",):
            options.guids = a.split(',')
        if o in ("-l",):
            options.lfchost = a
        if o in ("-f",):
            options.infile = a
        if o in ("-o",):
            options.outfile = a
    # read GUID/LFN
    files = {}
    if options.infile == None:
        for idx in range(len(options.guids)):
            guid = options.guids[idx]
            lfn  = options.lfns[idx]        
            if guid != 'NULL':
                files[guid] = lfn
    else:
        try:
            # read from file
            ifile = open(options.infile)
            for line in ifile:
                items = line.split()
                if len(items) == 2:
                    guid = items[1]
                    lfn  = items[0]
                    if guid != 'NULL':
                        files[guid] = lfn
            # close and delete
            ifile.close()
            os.remove(options.infile)
        except:
            errType,errValue = sys.exc_info()[:2]
            print "ERROR: %s:%s" % (errType,errValue)
            sys.exit(1)
    # get files
    retFiles = _getFilesLFC(files,options.lfchost,options.storages,options.verbose)
    print "LFCRet : %s " % retFiles
    # return
    sys.exit(0)


if __name__ == "__main__":
    main()
        
