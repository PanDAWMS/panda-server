Release Note

* current
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
