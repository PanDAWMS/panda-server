Release Note

* current
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
