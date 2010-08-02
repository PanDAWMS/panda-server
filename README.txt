Release Note

* current
  * set jobsetID for ptest
  * changes for output container and short LFN for analysis

* 0.0.10 (8/2/2010)
  * tagged for output container and short LFN for analysis
  * added setCloudTaskByUser
  * get list of PD2P clouds dynamically
  * send transferType to the pilot
  * imposed a size limit on uploaded files by users
  * fixed the task brokerage to take maxDiskCount into account
  * added a protection againt empty jobParameters only for new jobs
  * fixed PD2P to remove the cloud boundary when counting nSites
  * disable brokerage for gangarobot
  * ignore HC and group jobs in PD2P
  * fixed PD2P to take non-PD2P sites into account when checking comp/incomp
  * fixed AtlasRelese for PD2P
  * enabled WN brokerage for ANALY_GLASGOW
  * updated Adder for --destSE=multiSites
  * use Data Brokering fr PD2P
  * change MWT2_UC_DATADISK to MWT2_DATADISK in PD2P
  * delete replicas from T2 when locations != []
  * protection against meta/para=None in peekJob
  * kill ITB_INTEGRATION jobs in sent status
  * batchID
  * ignore dis/sub in PD2P
  * dispatchDBlockTokenForOut
  * added banUser.py and made --jobID optional in killUser.py 
  * set activity='Data Consolidation' and acl_alias='secondary' to PD2P subscriptions
  * check replica at T1 in PD2P
  * added getActiveDatasets
  * don't move RAW,HITS,RDO by PD2P
  * allow prod proxy to kill anal jobs with 2 or 4
  * added PD2P
  * regard found=None as an incomplete replica
  * invoke listFileReplicasBySites only for incomplete sites in TA
  * fixed re-brokerage
  * fixed used file check for cancelled jobs
  * increased wait interval for reconnection in connection pool
  * updated ConBridge to kill child when connection failure
  * changed URL of panda mover trf
  * added a protection against method execution failure in panda.py
  * set dataset status for DaTRI requests
  * ignore DaTRI failure for duplicated requests
  * use DQ2 for email extraction
  * added -9 to killJob.py 
  * added killUser.py
  * added alias to httpd.conf for trf URL  
  * changed reading order in getPandIDsWithJobID to avoid missing jobs
  * set taskBufferErrorDiag when running jobs are killed
  * prevent prod proxy from killing analysis jobs
  * added priority massager
  * added NG words to Notifier
  * avoid sending DaTRI requests for failed jobs
  * fixed replica registration for --destSE
  * set type in datriHandler for analysis system
  * testpanda -> panda
  * introduced datriHandler
  * delete sub datasets from EGEE T2 when callback is received
  * set REMOTE_HOST to creationHost
  * increased priority boost for activated jobs
  * delete cancelled from jobsDefined4
  * added boostPrio.py
  * added cvs,svn,grid,librarian to NG words
  * True/False for schedconfig.validation
  * added admin to NG words for Notifier
  * added cancelled state

* 0.0.9 (4/13/2010)
  * increased the subscription limit to 600 in TA
  * protection against reassigning analysis jobs
  * enabled cache-matching brokerage for all EGEE clouds
  * enabled cache-matching brokerage for NL/FR
  * added a protection for containers composed of multiple datasets
  * added processingType to runBrokerage for HC
  * doesn't check release matching for CERN
  * cache-matching in the brokerage for DE 
  * added getHighestPrioJobStat
  * changed weight for the task brokerage to use RW instead of fullRW
  * fixed getFilesInUseForAnal for --individualOutDS
  * added getQueuedAnalJobs
  * updated brokerage to assign one prod_test job to a site 
  * disable prod role for non-group activity
  * use maxinputsize in the brokerage
  * added schedconfig stuff to template
  * removed cx_Oracle from FileSpec
  * removed MySQLdb from broker_utils
  * added maxinputsize
  * modified xyzCacheDB to take a list of siteIDs
  * suppressed warning messages in dashboard 
 
* 0.0.8 (2/2/2010)
  * tagging for SLC5 migration
  * added hostname matching for T3 pilots
  * use listFileReplicasBySites in TA
  * added checkFilesWithCacheDB
  * changed the default cmtconfig to SL4 for analysis in brokerage
  * updated the brokerage to allow slc4 jobs on slc5 sites
  * added killTask.py
  * added addFilesToCacheDB and flushCacheDB
  * modified dispatcher to accept service proxy
  * added WN-level file matching to getJob 
  * added MemProxy
  * fixed brokerage to skip release/cache matching for ND
  * use all source locations for dis
  * use long hint for queryDatasetWithMap 
  * added /Engage/LBNE/Role=pilot to acceptable roles
  * added analy_test to getJob for test pilots
  * use poffset regardless of accesscontrol
  * removed / from FQAN check in allowedgroups
  * limit the max number of files in sub dataset
  * use fasttrack only for evgen/simul
  * added cleanup in updateSiteData
  * added chdir to LFC 
  * added chdir for dq2 and fork
  * removed logging updateJob/getJob from dispatcher
  * use averaged updateJob/getJob 
  * ignore test when summing SiteData
  * don't update SiteData when logrotate is running
  * randomized the order of sites in updateSiteData to avoid concatenation
  * fixed checkSitesWithCache	
  * multi-threads in adder.py
  * count number of updateJob/getJob in add.py
  * use taskBuffer in add.py for all DB access
  * use fasttrack for all tasktypes and prio>=700
  * use taskBuffer for reassignment in copyArchived
  * cleanup old PandaSiteIDs for UK
  * set the number of treads to 2 in wsgi daemon
  * set MaxRequestsPerChild
  * enabled KeepAlive for proxy sites
  * check filename FieldStorage when a param is treated as file
  * not delete dis datasets when jobs are reassigned  
  * check useFastCGI before importing flup
  * introduced nDBConForFastCGIWSGI
  * fixed Setupper to re-register location at next attempt when previous was failed
  * changed logLevel in httpd
  * added flag to control verbosity of entry point
  * added FastCGI stuff

* 0.0.7 (11/20/2009)
  * removed verbose message from DBProxyPool 
  * more verbose info to DBProxyPool
  * fixed ReBrokerage to require the same distribution pattern of input datasets
  * set encoded nJobs to taskID for analysis jobs
  * fixed ReBrokerage
  * propagate bad state from dashboard 
  * removed threading in dispatcher and dataservice
  * fixed typo in dashboard access
  * fixed CloudTaskSpec for serialization
  * close non-DQ2 destinationDBlock in Closer
  * use infinite loop in ProxyPool.__init__
  * add random sleep to ConBridge.connect
  * use TaskBuffer instead of DBProxy in copyArchive
  * added querySQLS to DBProxy	
  * use ping for wakeUp
  * degrade message level of child termination in ConBridge
  * added ConBridge for database timeout
  * re-implemented rebrokerage to allow the case where build finished

* 0.0.6 (11/13/2009)
  * destinationSE=local
  * propage failed_transfer from dashboard
  * added activity to subscriptions
  * added cleanup for Datasets table 
  * added workaround for x86_64-slc5-gcc43 
  * removed TO_DATE for Datasets.modificationdate
  * set priority of buildJob back to 2000
  * renamed testpanda.ddm to pandaddm_
  * added /osg/Role=pilot
  * added lower limit for TO_DATE against Datasets table 
  * added protection in JobDispatch against non-proxy pilots
  * added ReBroker
  * removed UAT stuff
  * use long queue in brokerage in addition
  * increased max subjobs in UserIF to 5000
  * send log message from brokerage when disk shortage
  * use ANALY_LONG_BNL_ATLAS for UAT
  * added temporary priority boost for UAT
  * added YY.MM.DD to destinationDBlock of PandaMover
  * skipped release check in brokerage when weight is negative
  * removed T1 constaint on high prio jobs in brokerage only for i686-slc5-gcc43-opt
  * limit matching of cmtconfig=i686-slc5-gcc43-opt to i686-slc5-gcc43-opt jobs only
  * changed brokerage to use only T1 for many input jobs when weight is negative
  * removed computingElement matching in getJob for test jobs 
  * use transtimelo for timeout of analysis transfers
  * fixed for site->siteid in installedSW
  * added protection to _checkRole()
  * use cache version matching for analysis 
  * added 'user' to NG words in Notifier
  * take '_' into account in Closer for new naming convention
  * use onlyNames in dq2.listDatasets
  * changes for destSE
  * changed cmtconfig for slc5 to match to slc4 and slc5
  * set pandamover priorities using original job priorities
  * added HOTDISK to Setupper
  * added PandaMonURL to email notification
  * send email notification to site contact in addition to cloud contact
  * use schedconfig.DN for privilege check in addition to cloudconfig
  * ptest for analy tests 
  * use SARA-MATRIX for all T1 sources
  * more NG words in address finding
  * skip VUID lookup for analysis jobs
  * added getSlimmedFileInfoPandaIDs
  * added a hint for filesTable_ARCH
  * limited modificationTime on filesTable_ARCH queries
  * allowed the pilot to set status for failed input files
  * make subscription for ptest
  * use /atlas for auth of updateFileStatusInDisp 
  * added updateFileStatusInDisp to flag lost files
  * removed double counting of jobs in Notifier
  * updated template
  * changed LogFormat for SLS
  * send prodDBlockToken to the pilot
  * modified Adder to take DQUnknownDatasetException into account
  * make subscriptions for rc_test
  * flagged all missing files in Setupper
  * added jobType to Client.getJobStatisticsPerSite
  * use stage-priority for prestaging
  * updated the brokerage to take input size into account 
  * use cleanUserID in Notifier
  * add copysetup to SiteSpec
  * fixed getCurrentSiteData for analysis
  * use pilotowners for checkRole in dispatcher
  * ignore DBRelease when adding shadow
  * support getJobStatisticsPerSite(countryGroup=None,workingGroup=None)
  * added two more filed to dis datasetname 
  * calculate priority for each workingGroup
  * added finder for email address using phonebook
  * reverted the change in Setupper
  * register location for _sub even when src=dest
  * workingGroup/countryGroup in getJobStatisticsPerSite
  * added getPandaClientVer
  * fixed MailUtils for multiple recipients
  * reuse unknown input files when build failed
  * use T1 in brokerage when too many inputs are required
  * added a timeout to Client
  * set sources of dis for all clouds
  * use MCTAPE for subscriptions
  * added trustIS to runBrokerage
  * added longFormat to listSiteAccess 
  * added set to updateSiteAccess
  * verify workingGroup
  * send email update/request for site access
  * kill old dq2 processes
  * addded updateSiteAccess
  * workingGroup
  * added MailUtils
  * prestaging for MCTAPE
  * set processingType for mover
  * get proxy for each job in getFullJobStatus
  * fixed address-check to trigger xwho
  * introduced NG words in email-adder finding 
  * put size limit in putFile
  * set higher priority for installation mover
  * skip files used by failed/finished jobs in getFilesInUseForAnal
  * removed BNL and old bamboo stuff from Client.py
  * added a hint to updateInFilesReturnPandaIDs
  * added getFilesInUseForAnal
  * set sources for ES
  * added a hint to getJobIDsInTimeRangeLog
  * removed write spaces from md5sum/checksum in peekJobLog

* 0.0.5 (5/15/2009)
  * subtract N*250M from available space in brokerage
  * use tasktype2 for RW recalculation
  * allow transferring in updateJob
  * use job stat per process group in brokerage
  * added prodUserName
  * added validation to test
  * fixed TA
  * use prodUserName for users
  * added nEvents to JD
  * added pilotowners
  * added rc_test
  * added a hint for Datasets.name
  * enabled validatedReleases for all clouds
  * set high priority for production role
  * added realDatasetsIn
  * get empty list of LFNs for empty dataset
  * set modificationTime to ARCH tables
  * fixed getUserParameter
  * added nInputFiles for HC
  * added countryGroup for country share
  * use a hint for filesTable4.dataset
  * fixed lookup for mail addr
  * use PandaMover for US
  * give higher priorities to /atlas/xyz/Role=production
  * set workingGroup when jobs are submitted with prod role
  * fixed peekJobLog
  * replica location lookup for containers
  * fixed broker_util to use proper python
  * use jobParamsTable
  * fixed python path to use 64bit glite
  * fixed for ArchivedDB
  * fixed FQAN extraction for GRST_CONN
  * dispatchDBlockToken
  * converted datetime to str for stateChangeTime
  * use 12hr limit in getJobStatisticsForBamboo
  * use CERN-PROD_DAQ for prestaging when _DATATAPE is not a location
  * ignore token=ATLASDATATAPE when no tape copy
  * pandasrv -> pandaserver
  * set old=False for listDatasetReplicas
  * fixed copyArchived for ArchiveDB
  * added _zStr/_nullAttrs in JobSpec
  * fixed getJobStatisticsForExtIF()
  * fixed for schedID/pilotID
  * removed redundant debug message
  * fixed for Notification
  * input token for mover
  * set NULL for creationHost,AtlasRelease,transformation,homepackage
  * use sequences directly for PandaID and row_ID
  * use SUBCOUNTER_SUBID_SEQ directly
  * added a hint to countFilesWithMap
  * fixed getNUserJobs
  * removed log/cache dirs making
  * put alias to filesTable4 in countFilesWithMap
  * introduced PANDA_URL_MAP
  * suppressed meta in JobSpec
  * error handling in Adder
  * fixed enddate in Notifier
  * use CURRENT_DATE in copyArch
  * added nprestage
  * added startTime/endTime in updateJob
  * validatedreleases and accesscontrol
  * 3 -> 1hour for movers (discarded)
  * added 'IS NULL' to copyArch 
  * added bulk reading for PandaID to copyArch to avoid redundant lookup
  * added a hint to updateOutFilesReturnPandaIDs
  * use Null instead of 'NULL'
  * don't reset jobParameters when reassigned
  * added a hint to all fileTable4+destinationDBlock
  * use JOBSARCHIVED4_MODTIME_IDX
  * addSiteAccess and listSiteAccess
  * hours=1 -> 3 for movers
  * retry in peekJob
  * reconnection in rollback 
  * added hint to queryDatasetWithMap
  * use bind-variables for all queries
  * fixed freezeDS 
  * fixed a duplicated variable in Closer 
  * truncate ddmErrorDiag
  * hint to freezeDS
  * removed deleteFiles in copyArchived
  * not update modTime in copyArchived when peekJob failed
  * container-aware
  * validatedreleases and space check in brokerage
  * added deleteJobSimple
  * use validatedreleases for FR too
  * fixed reassignXYZ
  * use archivedFlag for copy/delete
  * fine lock for reassignRepro 
  * threading for reassignRepro 
  * improved expiration messages
  * failed when input dataset is not found in DQ2
  * debug messages in Setupper
  * added other error codes in rollback

* 0.0.4 (2/23/2009)
  * GSI authentication for pilots
  * tag-based security mechanism for scheduler-pilot-server chain
  * fixed test/add.py to use Oracle instead of MySQL
  * fixed querySQLS for DELETE
  * added panda_server-grid-env.sh
  * merged DB proxies to reduce the number of connections
  * added lock for worker MPM
  * use common write account

* 0.0.3 (2/16/2009)
  * sync to production version

* 0.0.2 (12/18/2008)
  * adjustments for CERN

* 0.0.1 (12/4/2008)
  * first import

 LocalWords:  ConBridge
