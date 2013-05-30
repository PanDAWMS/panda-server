Release Note

* current
  * included group analysis jobs in priority massage 
  * removed priority boost for group analysis jobs
  * fixed brokerage to respect preset computingSite even for too many input
    jobs in cloud with negative t1weight

* 0.0.17 (4/27/2013)
  * giving a higher prio to install jobs
  * split runRebro from copyArchived
  * fixed retryInActive to reset file status 
  * modified dispatcher to send prodSourceLabel for getJob
  * changed ATLAS_PANDALOG.USERS_ID_SEQ to ATLAS_PANDAMETA.USERS_ID_SEQ
  * added TaskMonitor link to email notifications
  * changed getJob() to allow the prod/analy pilot to get installation jobs
  * fixed retryJobsInActive
  * fixed datasetManager to delete sub from foreign T1 instead of home T1
  * improved getDisInUseForAnal
  * added boostUser
  * improved fairshare to support per-cloud shares
  * changed Setupper to register both DATADISK and PRODDISK as locations for sub
  * changed job/task brokerages not to check DBR with DQ2 at CVMFS sites
  * changed the brokerage to skip release checks for releases=ANY
  * fixed for site.priorityoffset
  * fixed T2 cleanup to check if there is active subscription
  * fixed brokerage and copyArchive for RU
  * changed insertNewJob not to insert metadata when it is empty
  * fixed killUser to kill jobs gradually
  * fixed Setupper to make dis for pin at MCP sites in ND cloud
  * fixed Setupper to take cloudconfig.tier1se into account for dis subscriptions
  * set a limit on G/U in the brokerage
  * sending more info in PD2P logging
  * fixed LFC lookup in the brokerage
  * changed PD2P to be triggered by the second job
  * removed multiCloudFactor from the brokerage for NL
  * added a protection to updateJobStatus to prevent holding->transferring
  * fixed getUserParameter to insert new row if the user is missing
  * fixed Setupper to trigger prestaging when sites with multi-endpoints use TAPE
  * put all info to ErrorDiag in the brokerage
  * added modificationTime constraint to URL sent to the user by Notifier
  * introduced ProcessLimiter
  * changed TA to shorten retry interval after refreshing replica info
  * skipping file availability check for log datasets in TA
  * using cloudconfig.tier1SE to count files at T1
  * setting scope only for ATLAS
  * improved the task brokerage to check datasets with fewer replicas first
  * set limit on the number of IDs to be sent to the logger for reassign/killJobs
  * removed LFC lookup from TA
  * changed PD2P to use secondary share
  * fixed to use correct DQ2 site ID for pinning at sites with multi-endpoints
  * modified to send scopes for output files to the pilot
  * added changeJobPriorities
  * using DATADISK for MCP T1 input at all T1s except US
  * added filespec.scope
  * reducing lifetime of dis when corresponding jobs finished and some of them failed
  * improved the brokerage to count the number of running jobs per processingType
  * using transferringlimit in the brokerage
  * fixed the bulk OK file lookup again for unique ddm endpoint sites
  * reduced interval of PandaMover reattempts to 15min from 3h
  * fixed the bulk OK file lookup in the brokerge for multiple ddm endpoints
  * increased the number of PandaMover channels to 15
  * using DATADISK for MCP T1 input at CERN
  * using a default fareshare defined per cloud if T2 doesn't define share
  * added a protection against overwriting of dataset status by datasetMgr
  * implemented a nested fareshare management mechanism
  * fixed the brokerage message when release is missing for repro
  * fixed TA since replicas at T1 non DATADISK prevented T2 replicas from being used
  * using DATADISK for MCP T1 input at ND,ES,DE,NL,TW
  * added a patch for MWT2 to associate MWT2_DATADISK in TA
  * allowed wildcards in cloudconfig.tier1SE
  * fixed Merger for standalone ROOT
  * fixed Closer to trigger merging for cancelled jobs 
  * fixed Setupper to pin DBR as well
  * added a protection to Setupper for file lost after job submission
  * fixed getHighestPrioJobStatPerPG for group queue
  * added group queue to all clouds
  * added FOR UPDATE when getting jobdefID for users
  * removed hard-coded FZK-LCG2_DATATAPE removal in TA
  * set activity=Production to TA subscriptions
  * fixed weight reduction in TA for no input tasks
  * fixed the brokerage to send message to logger for too many transferring's
  * fixed wrong error message in TA when open dataset is incomplete
  * updated TA to use a special weight reduction when only TAPE is available
  * removed selector from fileCallbackListener 
  * fixed for TMPDISK
  * fixed Setupper to scan T2 LFC per LFC host instead of per SE
  * fixed Setupper to use correct location when pinning dis at foreign T1
  * fixed sitemapper to allow multiple DQ2 site IDs to use the same token
  * added DQ2 registration time to SLS
  * fixed vomsrenew.sh to check certificate and proxy lifetime
  * fixed file-check in the brokerage for BNL@non-US 
  * fixed brokerage not to overwrite file's destSE for destSE=local
  * introduced mcore queue in PG
  * added iscvmfs to SiteSpec

* 0.0.16 (8/29/2012)
  * changed Setupper to make sub when data is available only at T2
  * changed Setupper to make sub when data is missing at T1 
  * change TA to pin input and skip replicas with ToBeDeleted
  * using share=secondary for non T2-close-source PD2P  
  * added useWebCache() to Client
  * fixed getJobStatistics not to read archived via http by default
  * fixed Adder2 to skip destSE check for ddm=local 
  * fixed LFCclient to randomly resolve DNS alias for LFC host
  * added makeSlsXml
  * patched smtplib.stderr to send debug info to logger
  * added 32/64 to getScriptOfflineRunning
  * changed JOBSARCHIVED4_MODTIME_IDX hint
  * enabled maxtime check for analysis brokerage
  * fixed to check T2 files when get reassigned
  * removed hints related to JOBSACTIVE4_JOBSTATUS_IDX
  * fixed setOK to check map
  * fixed resetDefinedJob for for recordStatusChange
  * fixed updateJobStatus not to reset modificationTime of holding jobs
  * fixed file check not to use TAPE replicas when T1 is used as T2
  * disabled release check for CERN-RELEASE
  * enabled release check for CERN
  * removed EVNT from PD2P
  * removed the higher priority to phys-higgs
  * added _LONG as a suffix of hospital queue
  * fixed queryLastFilesInDataset agains missing jobs which are still in fileDB
  * added setPriority.py
  * fixed updateJobStatus for endTime
  * updated the brokerage log to have timestamp
  * updated the brokerage to take maxtime into account
  * updated file-level callback
  * added Job Status Monitor
  * added --killUserJobs to killJob.py
  * added reliability-based brokerage for analysis jobs
  * fixed getDestSE to look into ARCH for sub datasets for failed log files
  * fixed rebrokerage when orig replica is set to ToBeDeleted
  * temporally gave a higher priority to phys-higgs for ICHEP2012
  * added code=91 to allow prod role to kill user jobs gracefully 
  * check LFC every hour for high prio transferring jobs
  * fixed datasetManager for T2 cleanup by recognizing T1 PRODDISK correctly
  * delete sub from PRODDISK except US clous 
  * added protection to ReBroker against redundant comma in excludedSite 
  * added fatal errors for datri in Adder2
  * fixed Adder2 for missing src in schedconfig for analysis with destSE
  * changed brokeage to make a chunk for each diskCount/memory
  * added RbLauncher to run ReBroker in grid env
  * added more message to Finisher 
  * fixed Adder2 for failed jobs to add files to sub
  * reduced the number of add.py
  * modified getHighestPrioJobStat to calculate per PG
  * added --noRunning to killTask
  * fixed insertSandboxInfo to use real file size
  * added checkSandboxFile
  * fixed brokerage for nightlies
  * extracting crc from input sandbox in putFile
  * added changes for debug mode
  * setting prestage sites with PandaMover dynamically
  * removed BNL_ATLAS_1 from SiteMapper
  * removed FILESTABLE4_DATASET_IDX
  * added more info to putFile
  * optimized getDisInUseForAnal in TB
  * fixed TA to ignore non-DATADISK replicas at T1 
  * fixed brokerage for preassigned repro jobs
  * fixed dataset update timing check in Notifier 
  * rixed zero suppression with wildcard in brokerage
  * fixed rebro to set the same specialHandling to build since new build may have different specialHandling
  * removed old hints
  * fixed DataServiceUtils to return an empty map when DQ2Map is set
  * using FOR UPDATE in lockJobForReBrokerage 
  * added more debug INFO to Setupper
  * fixed DBProxy not to freeze top datasets for HC when build failed
  * fixed anal brokerage to take # of defined jobs into account
  * setting RUCIO_ACCOUNT and RUCIO_APPID
  * pin dis for foreign T2s in US cloud
  * removed special treatment for BNL from Adder
  * fixed the brokerage to get hospital queues automatically
  * updated brokerage to use coreCount
  * fixed Closer not to freeze any HC datasets
  * fixed Adder since Register2 gives DatasetExist error when it got deleted 
  * enabled cap based on priority for CERN
  * not reset retried jobs in Watcher
  * check attemprNr in retryJob
  * added double quotas to all params in getScriptOfflineRunning
  * added jobMetrics
  * added a protection against non-integer PandaID in peekJob
  * changed to update only changed attributes in job tables
  * fixed runMerge not to be stopped due to a single dataset error
  * added debug message for execution time of DQ2(+LFC) registration  
  * fixed storeJob to reset changed attribute list
  * disabled beyond-pledge for HC jobs
  * changed to update only changed attributes in filesTable4
  * added nOutputDataFiles and outputFileBytes to job tables
  * modified getScriptOfflineRunning to use parallel transfers
  * removed shadow lookup in Adder
  * disabled sub for computingSite=destinationSE
  * added getScriptOfflineRunning
  * added retry to Cassandra operations
  * changed killing with group prod role not to be case-sensitive
  * added getDis/LFNsInUseForAnal
  * added getPledgeResourceRatio to TB
  * added Cassandra file cache
  * added TAG support in EventPicker 
  * added countGuidsClient
  * using SCRIPT_NAME in panda.py
  * removed _shadow creation in ReBroker
  * fixed queryLastFilesInDataset for the fileTable change
  * remove deleting datasets from the Datasets table
  * sending error log to the logger when TA cannot find dataset in DQ2
  * sending fsize and checksum to the pilot
  * added modificationTime<=CURRENT in getFilesInUseForAnal
  * added hint when deleting rows from Datasets 
  * making larger subs by sorting jobs by site
  * instantiating dq2api in each thread
  * added hint to use 11g cashing
  * removed constraint in TA to consider T1 and T2 equally
  * increased the lifetime of the proxy to 96h
  * fixed TA to select candidate T2s correctly
  * getting shadow info from filesTable
  * added vomsrenew.sh
  * fixed TA to count the number of files at US T2
  * check attmptNr
  * fixed for non-MC/DATA space at split T1
  * fixed TA to check completeness at T2 
  * use correct locations for GEN dis when jobs directly go to T2
  * added protection to Adder2 against sites disappearance from schedconfig
  * added preferential analysis brokerage based on countryGroup
  * added more verbose message in Adder
  * Mikhail Titov updated datriHandler
  * fixed cloudlist to skip None
  * added getJobStatisticsPerUserSite
  * added 64bit in copyROOT
  * avoid priority reduction for merge jobs
  * use <= for maxDiskCount in getJob
  * fixed rebrokerage for --destSE
  * updated rebrokerage to be triggered 3 hours after the site is blacklisted
  * set maxAttempt to allow users to disable auto retry
  * changed global file map to local in brokerage
  * fixed Adder2 to use proper destination for token=TAPE when running at T1 as T2
  * updated killJob to take group prod role into account
  * updated brokerage to take priorities into account for prod jobs
  * using native DQ2 call in ToA
  * modified brokerage to do bulk LFC lookup per site  
  * fixed brokerage_util to do LFC lookup per 1000 files instead of 100 files
  * fixed brokerageErrorDiag for repro + missingRel 
  * fixed port of pandamon in email notification
  * fixed brokerageErrorDiag for useT2 + repro
  * set replica pin lifetime before deleting from T2
  * improved brokerage error diag
  * cleaned the brokerage for hospital queues
  * use 0 when memory=0 in one of online sites with the same siteID
  * fixed the brokerage to use RAL-LCG2_H​IME as UK T1
  * touch input sandbox when tried to be overwritten
  * permit overwriting of input sandbox
  * reject limited proxy
  * added priority boost for gangarobot-pft
  * fixed getCriteria for aggregated sites
  * fixed brokerage for group=any:0%
  * fixed brokerage more for type=any:0%
  * fixed brokerage to take zero shares into account
  * fixed getCriteriaForProdShare for zero shares
  * added minPriority to Client.getJobStatisticsPerSite
  * using MV in getJobStatisticsWithLabel
  * added fairshare to getJob
  * fixed retryJob not to change the name of lib.tgz for ptest
  * fixed retryJob not to retry buildJob to keep the PandaID order
  * fixed TB to give higher prio to buildJob with prodRole  
  * fixed Merger to use the largest SN for merged files
  * fixed queryLastFilesInDataset to ignore merged files 
  * fixed brokerageErrorDiag for non missing release errors
  * added tmpwatch.py	
  * changed hint in getJobs
  * fixed updateProdDBUpdateTime for pending jobs
  * fixed brokerage to accept test sites for prod_test jobs
  * changed getJobs for test pilots to get gangarobot jobs
  * setup glite in TaLuncher
  * added lock in lockDatasets
  * added version check in Merger to avoid duplicating merge jobs
  * changed Merger to fail when container name is too long
  * use lockJobsForReassign for reassign in copyArchive
  * use native DQ2 in copyArchive and datasetMgr
  * use python2.5 for copyArchive and prio-mgr
  * use native DQ2 in Setupper 
  * fixed guid generation for user's log
  * introduced 2 staged submission for prod jobs 
  * using T2 in TA
  * using materialized view get getJobStatistics family
  * updated Merger to put log files of merge jobs to a separate container
  * fixed Merger for --transferredDS 
  * enabled rebrokerage for processingType=ganga
  * updated Adder for unique constraint error
  * added copyROOT
  * updated Adder to immediately go to failed when subscription failures 
  * disabled prio boost for gangarobot derivatives
  * added protection to TA against undefined maxinputsize
  * updated TA and brokerage to use T2 datasets in prod
  * updated for DQ2 client 0.1.37

* 0.0.15 (11/07/2011)
  * removed redundant freshness checks in getSN
  * changed hint in getSerialNumber
  * randomized job order in adder
  * decreased the number of adder processes
  * added more tight constraint to getJobStatistics family
  * reduced prio by 10 for pilot-retry jobs
  * increased the factor of the RW limit to 8000
  * updated Merger for --mexec
  * modified rebroekrage to send brokerage log
  * modified brokerage to send user's countryGroup and nJobs to logger 
  * added a protection to httpd.conf for interesting panda.py
  * not attach attemptNr to lib.tgz for rc_test+buildJob
  * fixed parentID for retryJob with new PandaID
  * randomized the order of site check in analysis brokerage 
  * added --killOwnProdJobs to killJob.py and killJobsInTask.py
  * fixed brokerage to require cache=None for release check 
  * pinning input datasets
  * added limitation of exe/pilotErrorDiags in JD
  * fixed short->long mapping in retryJob
  * generates new PandaID for pilot-retried job 
  * using negative errorcode for pilot-retry
  * added invalid character check to DDM
  * fixed the brokerage for --transferredDS

* 0.0.14 (10/11/2011)
  * fixed TaskAssigner for MCshare=0
  * updated brokerage to consider priorities for analysis jobs
  * fixed brokerage for BNL_CVMFS_1
  * modified managed pilots to get prod_test as well
  * call addShadow even if DaTRI failed
  * fixed the error message of location registration in Setupper
  * modified ReBroker for server-side retry
  * reverted the brokerage change
  * changed brokerage to skip sites with memory=0 for analysis with memory
  * increaded MaxClients
  * use DQ2 for foreign T2 in US cloud
  * use IN2P3-CC and IN2P3-CC_SGE_VL as FR T1 in brokerage
  * unset commandToPilot for jobs reassigned by rebrokerage
  * added retryJobsInActive
  * added --maxJobs and --running to killJobLowPrio.py
  * added killJobLowPrio.py
  * fixed killJob
  * simplified anal_finalizer
  * added SiteSpec.lfcregister
  * added getAttr
  * keep failed analysis jobs in Active until all jobs finished

* 0.0.13 (8/30/2011)
  * fixed Adder2.removeUnmerged to catch DQ2 errors correctly
  * using subType in datasetManager
  * filling datasets.subtype
  * added protection against too large inputFileBytes
  * removed CN=Robot: from DN
  * added hint to DBProxy.getLockDatasets
  * reduced the number of table scan in datasetMgr and runMerge
  * fixed brokerage not to count jobs for usermerge or pandamover
  * changed brokerage to use ANALY_CERN_XROOTD and not to use ANALY_CERN
  * added Forker to add.py
  * updated dispatcher to send taskID
  * using schedconfig.multicloud
  * fixed brokerage for test sites
  * fixed brokerage not to count jobs for HC
  * fixed rebrokerage for CERN TMP
  * updated the brokerage to stop assigning prod jobs to sites which have many transferring
  * added jobdefID to libDS in ReBrokerage 
  * disabled short -> long for HC
  * fixed SiteMapper to respect online even if another queue is not online
  * put attempt number to output file name in Merger
  * changed = to == in redundant messages
  * job-chaining for ptest+prun
  * added initLogger to Notifier
  * removed redundant suffix from DN for DaTRI request in EventPicker
  * added more message in EventPicker for DaTRI request
  * changed Notifier to non-thread
  * fixed Notifier to take into account old jobs in Arch
  * implemented new PD2P scheme using MoU and close sites
  * increased the number of concurrent Mergers
  * incrementing Datasets.currentfile only for the first failed job
  * fixed Watcher to append attemptNr when sent->activated
  * fixed resetDefJob
  * limited the number of jobs with the same GEN dis
  * fixed EventPicker to take input files into account
  * fixed Merger to use .tgz for text merging
  * added EventPicker
  * added statusmodtime to SiteSpec
  * updated Merger for runDir
  * updated rebrokerage to take --cloud into account
  * added tags into PD2P logging
  * updated Merger for mergeScript
  * fixed getFilesInUseForAnal to skip NULL dis datasets
  * updated analy_brokerage to use memory size
  * added cmtconfig to broker logging
  * enabled cross-cloud for US in PD2P 
  * enabled banUser in storeJobs
  * enabled role-check in submitJobs
  * added WrappedPickle to avoid deserializing insecure objects
  * added banUser to storeJob
  * added prodSourceLabel check to UserIF

* 0.0.12 (6/13/2011)
  * fixed Merger for --useContElement
  * fixed inputFileProject extraction for wildcard-uses
  * using basename in Utils methods
  * fixed fetchLog to disallow chdir
  * fixed panda.py to disallow unexpected methods
  * added getVomsAttr
  * updated getJob to decompose CERN-XYZ to CERN-PROD+processingType
  * updated the brokerage to use installedsw.cmtConfig
  * use MoU share for T1 PD2P
  * added getNumPilots
  * added prodSourceLabel=ssc as user's label
  * added --prodSourceLabel to killUser
  * fixed archiveJob for failed jobs with multiple dis
  * fixed Setupper to store GEN dis
  * disabled release check in the brokerage for x86_64-slc5-gcc43
  * implemented aggressive cleaning for PRODDISK
  * added priority boost for gangarobot
  * updated T2 cleanup to use grace_period='00:00:00' 
  * cleanup copyArchive
  * changed analysis brokerage to use nRunning(max in last 24h)
  * increased # of active subscriptions to 2 in PD2P
  * added nRunning calculator to add.py
  * disabled priority reduction for merge jods
  * sending analysis brokerage info to logger
  * updated PD2P not to check provenance since group datasets have mc*/data* 
  * disabled PD2P to CERN-PROD_EOSDATADISK
  * added checkMergeGenerationStatus
  * enforce LFN-lookup to trigger getting replica map when reassigned
  * fixed brokerge for test jobs at test sites
  * use release matching for T2s in CERN cloud
  * skip release check for CERN and ND
  * set correct info to brokerageErrorDiag 
  * send jobs to waiting when release/cache is missing
  * remove '' for |pilotOwners|
  * put cloud-boundary back to US
  * use SourcesPolicy.ALL_SOURCES for PD2P subscriptions
  * improved PD2P logger
  * included CERN to trigger PD2P 
  * fixed typo in PD2P skip message
  * fixed zero-division in PD2P
  * enabled T1-T1 in PD2P

* 0.0.11 (4/18/2011)
  * fixed getExpressJobs
  * use c-t-s for all files in merge jobs
  * modified runMerger to kill old process
  * disable Initializer when nDBConnection is 0
  * increased max attempt for rebrokerage to 5
  * changed the rebrokerage interval to 24h
  * skip init for jobDispather,dataService,userIF when nCon=0
  * added parameters in email notification
  * ignore LOCALGROUPDISK in PD2P 
  * fixed auto type detection of Merger for THIST
  * use IN2P3-CC_VL for too many input or high prio jobs
  * gave T1 weight to IN2P3-CC_VL
  * added protection to Adder2 against DQ2 failure for jumbo datasets
  * updated Adder2 to avoid making DaTRI request for unmerged files
  * added protection against generating multiple Mergers for --individualOutDS
  * updated brokerage to give T1 weight to NIKHEF for repro jobs
  * fixed Merger for lib.tgz
  * added automatic merge type detection to Merger
  * updated Closer to redirect logging to parent as it doesn't work in nested threads
  * changed parameter convention for Merger
  * added merge job generation
  * set secondary for TA subscription
  * use TAIWAN-LCG2_HOTDISK for TW HOTDISK
  * disabled PD2P for ESD
  * set file.dispDBlock even if they are already available at the site
  * send jobDefID and cloud to the pilot
  * updated Setupper/Adder2 for T1 used as T2
  * set destDBlockToken to DATADISK
  * using home cloud to skip release check in the brokerage
  * reassign stuck T2 evgensimul more frequently
  * enabled release/cache check for US
  * using nRunning(cloud) in brokerage for multi-cloud
  * added fileGUID to updateInFilesReturnPandaIDs for file-level callback 
  * set source to _subs for all clouds
  * using DQ2 API directly in Adder 
  * added nInputDataFiles,inputFileType,inputFileProject,inputFileBytes
  * add hacks again to TA and Setupper for split T1
  * added EventLookup to PD2P
  * updated SiteMapper for multi-cloud
  * removed hacks from TA and Setupper for split T1
  * added forceOpt to runReBrokerage
  * fixed PD2P not to make sub when dataset is being deleted
  * changed PD2P not to send ESD to EOS
  * added a hint to getPandaIDsForProdDB to enforce function index
  * added comment_ to SiteSpec
  * put hacks back to TA and Setupper for split T1 which uses NIKHEF as src 
  * set hidden metadata to _dis and _sub
  * removed REGEXP from Datasets cleanup
  * enabled rebrokerage for ganga-rbtest
  * fixed ReBroker for EOS
  * fixed ReBroker to add _shadow
  * use DATADISK for all PD2P subscriptions
  * close user datasets in container
  * set lifetime for dis and sub datasets
  * added --jobsetID to killUser.py
  * added protection against missing argument for jobID/jobsetID to killUser.py
  * trigger PD2P for EOS when nUsed >= 3  
  * updated brokerage to take transferType into account
  * update modificationTime when going to Archived4
  * disabled extra replica making in PD2P
  * trigger PD2P for EOS when nUsed >= 2
  * added testG4sim16.py and testEvgen16.py
  * use diskThr=max(5%,3TB)-diskSize in PD2P
  * added killJobsInTask
  * set disk threshold in PD2P to 5GB
  * updated PD2P so that any analysis job using data makes subscriptions to CERN EOS
  * set specialHandling=rebro when reassigned by rebrokerage 
  * fixed DQ2 ID conversion in PD2P for EOS
  * check free disk size in PD2P using DQ2.queryStorageUsage
  * use function index in getPandaIDsForProdDB 
  * reduced the number of rotated logs
  * use cernmx.cern.ch
  * added getLockDatasets
  * added the number of succeeded jobs to the subject of Notification
  * added pd2p logging
  * added deleteJobs.py
  * split arch procedure to another cron
  * call taskbuffer.Initializer in forkSetupper.py to acquire Oracle environment handle correctly
  * use truncated DN when setting dataset owner 
  * reassign evgen/simul with active state at T1 more aggressively
  * made SQLDumper iterable
  * added SQLDumper
  * added reassignTask
  * use getFullJobStatus in Notifier since some jobs can go to ARCH before notification
  * seprate retry for Notifier
  * added retry to Notifier when failing to send notifications
  * express jobs
  * make new dis datasets even if files are already available at T2 
  * short/long mapping for ANALY_LYON-T2
  * updated PD2P to use a negative weight based on the number of subs
  * ignore hidden datasets in PD2P
  * don't use modTime index on jobs_ARCH
  * set/increment nUsed in PD2P
  * use LFN for WN-level matchmaking
  * ignore datasets with provenance=GP for PD2P
  * don't reuse the same site in a single PD2P cycle 
  * fixed brokerage to send warning when cache is missing
  * removed redundant holding for prod jobs in Watcher
  * more fix to SetUpper for rc_test
  * not reset holding analysis jobs when stateChangeTime=modTime
  * set stateChangeTime when job goes to holding for finished/failed
  * job chain for rc_test + gangarobot-rctest
  * added archivelogs
  * set tobeclosed to sub datasets of failed downstream jobs
  * rctest -> rc_test
  * reduced time interval to reassign waiting jobs to 30min
  * enabled user-triggered rebrokerage
  * send currentPriority in dispatcher
  * set localpool to specialHandling when beyond-pledge pilot got the job
  * fixed makeSub in TA for getAva change
  * added random sleep for Finisher in copyArchive
  * improved del in copyArchive to avoid redundant deletion
  * increased timelimit for copyArchive
  * added auto rebrokerage to copyArchive
  * report new PandaID to taskBufferErrorDiag when rebrokered
  * check procesingType in rebrokerage 
  * added code=8 to killJob for rebrokerage
  * first implementation of auto rebrokerage
  * added getCachePrefixes 
  * removed apostrophes from prodUserName
  * fixed useNiotifier in Closer for completed sub datasets
  * changed queryLastFilesInDataset to use MAX(lfn)
  * improved the space shortage message in TA
  * don't check missing files with LFC when site is already set
  * added -9 to killTask
  * added forceKill for prod jobs 
  * changed the brokerage to use CERN-PROD_EOSDATADISK as the dest for CERN-EOS jobs  
  * added enforce to Activator
  * changes for merge/unmerge jobs
  * rctest
  * deleteStalledJobs
  * removed hacks for last_insert_id of InnoDB
  * allowOtherCountry
  * updated datriHandler to prevent false http-requests
  * added a hint to getJobIDsInTimeRange against jobsActive4
  * added a hint to getJobIDsInTimeRange against jobsArchived4
  * changed hint in DBProxy.updateTransferStatus
  * changing TRF URL from BNL to CERN on the server side
  * fixed error message in brokerage for sites with status!=brokeroff
  * fixed brokerage for release check when schedconfig.rel != ''
  * changed countryGroup=ustlas to us
  * ignore gangarobot family in PD2P 
  * disabled priority decreasing for HC jobs
  * use installedSW for base-release matching for analysis
  * $GROUPJOBSN
  * added getSerialNumberForGroupJob
  * use jobsetID in Notifier
  * use max memory/inputsize for each site
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
