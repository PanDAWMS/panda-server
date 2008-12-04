import re
import sys
import datetime
from taskbuffer.DBProxy  import DBProxy
from taskbuffer.JobSpec  import JobSpec
from taskbuffer.FileSpec import FileSpec

# password
from config import panda_config
passwd = panda_config.dbpasswd

# table names
cdate = datetime.datetime.utcnow()
if cdate.month==1:
    cdate = cdate.replace(year = (cdate.year-1))
    cdate = cdate.replace(month = 12, day = 1)
else:
    cdate = cdate.replace(month = (cdate.month/2)*2, day = 1)
currentSuffix = "_%s%s" % (cdate.strftime('%b'),cdate.year)
if cdate.month > 2:
    odate = cdate.replace(month = (cdate.month-2))
else:
    odate = cdate.replace(year = (cdate.year-1), month = 12)
previousSuffix = "_%s%s" % (odate.strftime('%b'),odate.year)

# instantiate DB proxies
proxyS = DBProxy()
proxyN = DBProxy()
proxyS.connect(panda_config.dbhost,panda_config.dbpasswd,panda_config.dbuser,panda_config.dbname)
proxyN.connect(panda_config.logdbhost,panda_config.logdbpasswd,panda_config.logdbuser,'PandaArchiveDB')

# get tables
fileTables = []
jobsTables = {}
status,res = proxyN.querySQLS("show tables")
if res != None:
    for table, in res:
        if table.startswith('filesTable'):
            fileTables.append(table)
        if table.startswith('jobsArchived'):
            # get MAX PandaID
            statusJ,resJ = proxyN.querySQLS("SELECT MAX(PandaID) FROM %s" % table)
            jobsTables[table] = resJ[0][0]

# for the cumulative tables
cumulativeSuffix  = '4_current'
cumulativePandaID = jobsTables['jobsArchived%s' % cumulativeSuffix]

# create a map between MAX PandaID and suffix
suffixMap = {}
for table,maxPandaID in jobsTables.iteritems():
    # get suffix
    match = re.search('(\d??_.+)$',table)
    suffix = match.group(1)
    # special treatment is required for the cumulative tables
    if suffix == cumulativeSuffix:
        continue
    # name of corresponding file table
    name = "filesTable%s" % suffix
    if not name in fileTables:
        print "%s is not found" % name
        sys.exit(0)
    # check duplication
    if suffixMap.has_key(maxPandaID):
        print "%s is already used by %s" % (maxPandaID,suffixMap[maxPandaID])
        sys.exit(0)
    # append
    suffixMap[maxPandaID] = suffix

# print the cumulative
print "%8d %s" % (cumulativePandaID,cumulativeSuffix)
# sort by max PandaID
suffixKeys = suffixMap.keys()
suffixKeys.sort()
for key in suffixKeys:
    print "%8d %s" % (key,suffixMap[key])

# get files
minPandaID = -1
sql = "SELECT PandaID FROM filesTable4 WHERE PandaID > %s GROUP BY PandaID ORDER BY PandaID LIMIT 100"
#while True:
for i in range(5):
    status,res = proxyS.querySQLS(sql % minPandaID)
    # no more job
    if len(res) == 0:
        break
    # set min
    minPandaID = res[-1][0]
    # loop over all PandaIDs
    for id, in res:
        # look for corresponding table
        tableSuffix = ''
        if id < cumulativePandaID:
            # use the cumulative
            tableSuffix = cumulativeSuffix
        else:
            for key in suffixKeys:
                if id < key:
                    tableSuffix = suffixMap[key]
                    break
        # check suffix
        if tableSuffix in ['',currentSuffix,previousSuffix]:
            print "Terminated since fresh PandID=%s found for '%s'" % (id,tableSuffix)
            sys.exit(0)
        print "PandaID:%s Suffix:%s" % (id,tableSuffix)
        # get FileSpec
        sqlFile = "SELECT %s FROM filesTable4 " % FileSpec.columnNames()
        sqlFile+= "WHERE PandaID=%s" % id
        statusF,resFs = proxyS.querySQLS(sqlFile)
        for resF in resFs:
            file = FileSpec()
            file.pack(resF)
            # create a dummy Job to set PandaID
            job = JobSpec()
            job.PandaID = id
            job.addFile(file)
            # file table
            fileTable = 'filesTable%s' % tableSuffix
            # check
            sqlFileCheck = "SELECT PandaID FROM %s WHERE rowID=%s" % (fileTable,file.rowID)
            statusC,resC = proxyN.querySQLS(sqlFileCheck)
            if len(resC) != 0:
                if resC[0][0] != id:
                    print "PandaID mismatch PandaArchive:%s PandaDB:%s for rowID=%s" % \
                          (resC[0][0],id,file.rowID)
            else:
                print "rowID=%s not found" % file.rowID
            """
            # construct SQL
            sqlFileIn = "INSERT INTO %s " % fileTable
            sqlFileIn+= "(%s) " % FileSpec.columnNames()
            sqlFileIn+= FileSpec.valuesExpression()
            try:
                proxyN.cur.execute("SET AUTOCOMMIT=1")
                ret = proxyN.cur.execute(sqlFileIn,file.values())
                res = proxyN.cur.fetchall()
                # commit
                if not proxyN._commit():
                    raise RuntimeError, 'Commit error'                
            except:
                type, value, traceBack = sys.exc_info()
                print "insert error : %s %s" % (type,value)
                # roll back
                proxyN._rollback()
            """
