# exec
def run(dn,jobID,cloud=None,excludedSite=None):
    # check parameters
    if dn == '':
        return False
    if jobID < 0:
        return False
    # password
    from config import panda_config
    passwd = panda_config.dbpasswd
    # initialize cx_Oracle using dummy connection
    from taskbuffer.Initializer import initializer
    initializer.init()
    # instantiate TB
    from taskbuffer.TaskBuffer import taskBuffer
    taskBuffer.init(panda_config.dbhost,panda_config.dbpasswd,nDBConnection=1)
    # run ReBroker
    from userinterface.ReBroker import ReBroker
    reThr = ReBroker(taskBuffer,cloud,excludedSite,userRequest=True)
    # lock
    stLock,retLock = reThr.lockJob(dn,jobID)
    # failed
    if not stLock:
        return False
    # start
    reThr.start()
    reThr.join()
    return True
        

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
    options.jobID        = -1
    options.dn           = ''
    options.cloud        = None
    options.excludedSite = None
    # get command-line parameters
    try:
        opts, args = getopt.getopt(sys.argv[1:],"j:d:c:e:")
        # set options
        for o, a in opts:
            if o in ("-j",):
                options.jobID = long(a)
            if o in ("-d",):
                options.dn = a
            if o in ("-c",):
                options.cloud = a
            if o in ("-e",):
                options.excludedSite = a.split(',')
    except:
        print("ERROR : Invalid options")
        sys.exit(1)    
    # run
    run(options.dn,options.jobID,options.cloud,options.excludedSite)
    # return
    sys.exit(0)


if __name__ == "__main__":
    main()
