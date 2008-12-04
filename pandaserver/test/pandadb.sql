DROP TABLE jobsDefined4;
DROP TABLE jobsActive4;
DROP TABLE jobsArchived4;
DROP TABLE jobsWaiting4;
DROP TABLE filesTable4;
DROP TABLE Datasets;
DROP TABLE metaTable;
DROP TABLE subCounter;


CREATE TABLE jobsDefined4
(
                  PandaID      NUMBER(11) default          0 primary key,
          jobDefinitionID      NUMBER(11) default          0,
              schedulerID    VARCHAR(128),
                  pilotID    VARCHAR(128),
             creationTime            DATE,
             creationHost    VARCHAR(128),
         modificationTime            DATE,
         modificationHost    VARCHAR(128),
             AtlasRelease     VARCHAR(64),
           transformation    VARCHAR(250),
              homepackage     VARCHAR(64),
          prodSeriesLabel     VARCHAR(20) default 'pandatest',
          prodSourceLabel     VARCHAR(20) default   'managed',
               prodUserID    VARCHAR(250),
         assignedPriority       NUMBER(9) default          0,
          currentPriority       NUMBER(9) default          0,
                attemptNr       NUMBER(2) default          0,
               maxAttempt       NUMBER(2) default          0,
                jobStatus     VARCHAR(15) default  'defined',
                  jobName    VARCHAR(128),
              maxCpuCount       NUMBER(9) default          0,
               maxCpuUnit     VARCHAR(32),
             maxDiskCount       NUMBER(9) default          0,
              maxDiskUnit         CHAR(2),
           ipConnectivity         CHAR(3),
              minRamCount       NUMBER(9) default          0,
               minRamUnit         CHAR(2),
                startTime            DATE,
                  endTime            DATE,
       cpuConsumptionTime      NUMBER(20) default          0,
       cpuConsumptionUnit    VARCHAR(128),
           commandToPilot    VARCHAR(250),
            transExitCode    VARCHAR(128),
           pilotErrorCode       NUMBER(6) default          0,
           pilotErrorDiag    VARCHAR(250),
             exeErrorCode       NUMBER(6) default          0,
             exeErrorDiag    VARCHAR(250),
             supErrorCode       NUMBER(6) default          0,
             supErrorDiag    VARCHAR(250) default       NULL,
             ddmErrorCode       NUMBER(6) default          0,
             ddmErrorDiag    VARCHAR(250) default       NULL,
       brokerageErrorCode       NUMBER(6) default          0,
       brokerageErrorDiag    VARCHAR(250) default       NULL,
   jobDispatcherErrorCode       NUMBER(6) default          0,
   jobDispatcherErrorDiag    VARCHAR(250) default       NULL,
      taskBufferErrorCode       NUMBER(6) default          0,
      taskBufferErrorDiag    VARCHAR(250) default       NULL,
            computingSite    VARCHAR(128),
         computingElement    VARCHAR(128),
            jobParameters   VARCHAR(4000) default       NULL,
                 metadata     VARCHAR(32) default       NULL,
               prodDBlock    VARCHAR(250),
           dispatchDBlock    VARCHAR(250),
        destinationDBlock    VARCHAR(250),
            destinationSE    VARCHAR(250),
                  nEvents       NUMBER(9) default          0,
                     grid     VARCHAR(32),
                    cloud     VARCHAR(32),
            cpuConversion     NUMBER(9,4) default       NULL,
               sourceSite     VARCHAR(36),
          destinationSite     VARCHAR(36),
             transferType     VARCHAR(10),
                   taskID       NUMBER(9) default       NULL,
                cmtConfig    VARCHAR(250),
          stateChangeTime            DATE,
         prodDBUpdateTime            DATE,
                 lockedby    VARCHAR(128),
           relocationFlag       NUMBER(1) default          0,
           jobExecutionID      NUMBER(11) default          0,
                       VO     VARCHAR(16),
              pilotTiming    VARCHAR(100),
             workingGroup     VARCHAR(20)
);


CREATE TABLE jobsActive4
(
                  PandaID      NUMBER(11) default          0 primary key,
          jobDefinitionID      NUMBER(11) default          0,
              schedulerID    VARCHAR(128),
                  pilotID    VARCHAR(128),
             creationTime            DATE,
             creationHost    VARCHAR(128),
         modificationTime            DATE,
         modificationHost    VARCHAR(128),
             AtlasRelease     VARCHAR(64),
           transformation    VARCHAR(250),
              homepackage     VARCHAR(64),
          prodSeriesLabel     VARCHAR(20) default 'pandatest',
          prodSourceLabel     VARCHAR(20) default   'managed',
               prodUserID    VARCHAR(250),
         assignedPriority       NUMBER(9) default          0,
          currentPriority       NUMBER(9) default          0,
                attemptNr       NUMBER(2) default          0,
               maxAttempt       NUMBER(2) default          0,
                jobStatus     VARCHAR(15) default 'activated',
                  jobName    VARCHAR(128),
              maxCpuCount       NUMBER(9) default          0,
               maxCpuUnit     VARCHAR(32),
             maxDiskCount       NUMBER(9) default          0,
              maxDiskUnit         CHAR(2),
           ipConnectivity         CHAR(3),
              minRamCount       NUMBER(9) default          0,
               minRamUnit         CHAR(2),
                startTime            DATE,
                  endTime            DATE,
       cpuConsumptionTime      NUMBER(20) default          0,
       cpuConsumptionUnit    VARCHAR(128),
           commandToPilot    VARCHAR(250),
            transExitCode    VARCHAR(128),
           pilotErrorCode       NUMBER(6) default          0,
           pilotErrorDiag    VARCHAR(250),
             exeErrorCode       NUMBER(6) default          0,
             exeErrorDiag    VARCHAR(250),
             supErrorCode       NUMBER(6) default          0,
             supErrorDiag    VARCHAR(250) default       NULL,
             ddmErrorCode       NUMBER(6) default          0,
             ddmErrorDiag    VARCHAR(250) default       NULL,
       brokerageErrorCode       NUMBER(6) default          0,
       brokerageErrorDiag    VARCHAR(250) default       NULL,
   jobDispatcherErrorCode       NUMBER(6) default          0,
   jobDispatcherErrorDiag    VARCHAR(250) default       NULL,
      taskBufferErrorCode       NUMBER(6) default          0,
      taskBufferErrorDiag    VARCHAR(250) default       NULL,
            computingSite    VARCHAR(128),
         computingElement    VARCHAR(128),
            jobParameters   VARCHAR(4000) default       NULL,
                 metadata     VARCHAR(32) default       NULL,
               prodDBlock    VARCHAR(250),
           dispatchDBlock    VARCHAR(250),
        destinationDBlock    VARCHAR(250),
            destinationSE    VARCHAR(250),
                  nEvents       NUMBER(9) default          0,
                     grid     VARCHAR(32),
                    cloud     VARCHAR(32),
            cpuConversion     NUMBER(9,4) default       NULL,
               sourceSite     VARCHAR(36),
          destinationSite     VARCHAR(36),
             transferType     VARCHAR(10),
                   taskID       NUMBER(9) default       NULL,
                cmtConfig    VARCHAR(250),
          stateChangeTime            DATE,
         prodDBUpdateTime            DATE,
                 lockedby    VARCHAR(128),
           relocationFlag       NUMBER(1) default          0,
           jobExecutionID      NUMBER(11) default          0,
                       VO     VARCHAR(16),
              pilotTiming    VARCHAR(100),
             workingGroup     VARCHAR(20)
);

CREATE TABLE jobsWaiting4
(
                  PandaID      NUMBER(11) default          0 primary key,
          jobDefinitionID      NUMBER(11) default          0,
              schedulerID    VARCHAR(128),
                  pilotID    VARCHAR(128),
             creationTime            DATE,
             creationHost    VARCHAR(128),
         modificationTime            DATE,
         modificationHost    VARCHAR(128),
             AtlasRelease     VARCHAR(64),
           transformation    VARCHAR(250),
              homepackage     VARCHAR(64),
          prodSeriesLabel     VARCHAR(20) default 'pandatest',
          prodSourceLabel     VARCHAR(20) default   'managed',
               prodUserID    VARCHAR(250),
         assignedPriority       NUMBER(9) default          0,
          currentPriority       NUMBER(9) default          0,
                attemptNr       NUMBER(2) default          0,
               maxAttempt       NUMBER(2) default          0,
                jobStatus     VARCHAR(15) default 'activated',
                  jobName    VARCHAR(128),
              maxCpuCount       NUMBER(9) default          0,
               maxCpuUnit     VARCHAR(32),
             maxDiskCount       NUMBER(9) default          0,
              maxDiskUnit         CHAR(2),
           ipConnectivity         CHAR(3),
              minRamCount       NUMBER(9) default          0,
               minRamUnit         CHAR(2),
                startTime            DATE,
                  endTime            DATE,
       cpuConsumptionTime      NUMBER(20) default          0,
       cpuConsumptionUnit    VARCHAR(128),
           commandToPilot    VARCHAR(250),
            transExitCode    VARCHAR(128),
           pilotErrorCode       NUMBER(6) default          0,
           pilotErrorDiag    VARCHAR(250),
             exeErrorCode       NUMBER(6) default          0,
             exeErrorDiag    VARCHAR(250),
             supErrorCode       NUMBER(6) default          0,
             supErrorDiag    VARCHAR(250) default       NULL,
             ddmErrorCode       NUMBER(6) default          0,
             ddmErrorDiag    VARCHAR(250) default       NULL,
       brokerageErrorCode       NUMBER(6) default          0,
       brokerageErrorDiag    VARCHAR(250) default       NULL,
   jobDispatcherErrorCode       NUMBER(6) default          0,
   jobDispatcherErrorDiag    VARCHAR(250) default       NULL,
      taskBufferErrorCode       NUMBER(6) default          0,
      taskBufferErrorDiag    VARCHAR(250) default       NULL,
            computingSite    VARCHAR(128),
         computingElement    VARCHAR(128),
            jobParameters   VARCHAR(4000) default       NULL,
                 metadata     VARCHAR(32) default       NULL,
               prodDBlock    VARCHAR(250),
           dispatchDBlock    VARCHAR(250),
        destinationDBlock    VARCHAR(250),
            destinationSE    VARCHAR(250),
                  nEvents       NUMBER(9) default          0,
                     grid     VARCHAR(32),
                    cloud     VARCHAR(32),
            cpuConversion     NUMBER(9,4) default       NULL,
               sourceSite     VARCHAR(36),
          destinationSite     VARCHAR(36),
             transferType     VARCHAR(10),
                   taskID       NUMBER(9) default       NULL,
                cmtConfig    VARCHAR(250),
          stateChangeTime            DATE,
         prodDBUpdateTime            DATE,
                 lockedby    VARCHAR(128),
           relocationFlag       NUMBER(1) default          0,
           jobExecutionID      NUMBER(11) default          0,
                       VO     VARCHAR(16),
              pilotTiming    VARCHAR(100),
             workingGroup     VARCHAR(20)
);

CREATE TABLE jobsArchived4
(
                  PandaID      NUMBER(11) default          0 primary key,
          jobDefinitionID      NUMBER(11) default          0,
              schedulerID    VARCHAR(128),
                  pilotID    VARCHAR(128),
             creationTime            DATE,
             creationHost    VARCHAR(128),
         modificationTime            DATE,
         modificationHost    VARCHAR(128),
             AtlasRelease     VARCHAR(64),
           transformation    VARCHAR(250),
              homepackage     VARCHAR(64),
          prodSeriesLabel     VARCHAR(20) default 'pandatest',
          prodSourceLabel     VARCHAR(20) default   'managed',
               prodUserID    VARCHAR(250),
         assignedPriority       NUMBER(9) default          0,
          currentPriority       NUMBER(9) default          0,
                attemptNr       NUMBER(2) default          0,
               maxAttempt       NUMBER(2) default          0,
                jobStatus     VARCHAR(15) default 'activated',
                  jobName    VARCHAR(128),
              maxCpuCount       NUMBER(9) default          0,
               maxCpuUnit     VARCHAR(32),
             maxDiskCount       NUMBER(9) default          0,
              maxDiskUnit         CHAR(2),
           ipConnectivity         CHAR(3),
              minRamCount       NUMBER(9) default          0,
               minRamUnit         CHAR(2),
                startTime            DATE,
                  endTime            DATE,
       cpuConsumptionTime      NUMBER(20) default          0,
       cpuConsumptionUnit    VARCHAR(128),
           commandToPilot    VARCHAR(250),
            transExitCode    VARCHAR(128),
           pilotErrorCode       NUMBER(6) default          0,
           pilotErrorDiag    VARCHAR(250),
             exeErrorCode       NUMBER(6) default          0,
             exeErrorDiag    VARCHAR(250),
             supErrorCode       NUMBER(6) default          0,
             supErrorDiag    VARCHAR(250) default       NULL,
             ddmErrorCode       NUMBER(6) default          0,
             ddmErrorDiag    VARCHAR(250) default       NULL,
       brokerageErrorCode       NUMBER(6) default          0,
       brokerageErrorDiag    VARCHAR(250) default       NULL,
   jobDispatcherErrorCode       NUMBER(6) default          0,
   jobDispatcherErrorDiag    VARCHAR(250) default       NULL,
      taskBufferErrorCode       NUMBER(6) default          0,
      taskBufferErrorDiag    VARCHAR(250) default       NULL,
            computingSite    VARCHAR(128),
         computingElement    VARCHAR(128),
            jobParameters   VARCHAR(4000) default       NULL,
                 metadata     VARCHAR(32) default       NULL,
               prodDBlock    VARCHAR(250),
           dispatchDBlock    VARCHAR(250),
        destinationDBlock    VARCHAR(250),
            destinationSE    VARCHAR(250),
                  nEvents       NUMBER(9) default          0,
                     grid     VARCHAR(32),
                    cloud     VARCHAR(32),
            cpuConversion     NUMBER(9,4) default       NULL,
               sourceSite     VARCHAR(36),
          destinationSite     VARCHAR(36),
             transferType     VARCHAR(10),
                   taskID       NUMBER(9) default       NULL,
                cmtConfig    VARCHAR(250),
          stateChangeTime            DATE,
         prodDBUpdateTime            DATE,
                 lockedby    VARCHAR(128),
           relocationFlag       NUMBER(1) default          0,
           jobExecutionID      NUMBER(11) default          0,
                       VO     VARCHAR(16),
              pilotTiming    VARCHAR(100),
             workingGroup     VARCHAR(20)
);


CREATE TABLE filesTable4
(
                   row_ID      NUMBER(11) default          0 primary key,
                  PandaID      NUMBER(11) default          0,
                     GUID     VARCHAR(64),
                      lfn    VARCHAR(256),
                     type     VARCHAR(20),
                  dataset    VARCHAR(128),
                   status     VARCHAR(64),
               prodDBlock    VARCHAR(250),
          prodDBlockToken    VARCHAR(250),
           dispatchDBlock    VARCHAR(250),
      dispatchDBlockToken    VARCHAR(250),
        destinationDBlock    VARCHAR(250),
   destinationDBlockToken    VARCHAR(250),
            destinationSE    VARCHAR(250),
                    fsize      NUMBER(10) default          0,
                   md5sum        CHAR(36),
                 checksum        CHAR(36)
);


CREATE TABLE Datasets
(
                     vuid     VARCHAR(40) default       '' primary key,
                     name    VARCHAR(250),
                  version     VARCHAR(10) default       NULL,
                     type     VARCHAR(20) default       NULL,
                   status     VARCHAR(10) default       NULL,
              numberfiles       NUMBER(9) default       NULL,
             currentfiles       NUMBER(9) default       NULL,
             creationdate            DATE,
         modificationdate            DATE,
                  MoverID      NUMBER(11) default          0,
           transferStatus       NUMBER(2) default          0
);


CREATE TABLE metaTable
(
                  PandaID      NUMBER(11) default          0 primary key,
                 metaData   VARCHAR(4000) default       NULL
);


CREATE TABLE subCounter
(
                   subID      NUMBER(11) default       0
);



CREATE INDEX jobsA4_currentPriority_IDX ON jobsActive4 (currentPriority);
CREATE INDEX jobsA4_jobStatus_IDX       ON jobsActive4 (jobStatus);
CREATE INDEX jobsA4_computingSite_IDX   ON jobsActive4 (computingSite);

CREATE INDEX file4_PandaID_IDX          ON filesTable4 (PandaID);
CREATE INDEX file4_status_IDX           ON filesTable4 (status);
CREATE INDEX file4_dispDBlock_IDX       ON filesTable4 (dispatchDBlock);
CREATE INDEX file4_destDBlock_IDX       ON filesTable4 (destinationDBlock);

CREATE INDEX Datasets_name_IDX          ON Datasets (name);

DROP SEQUENCE PandaID_SEQ;
DROP SEQUENCE rowID_SEQ;
DROP SEQUENCE subID_SEQ;


CREATE SEQUENCE PandaID_SEQ;
CREATE SEQUENCE rowID_SEQ;
CREATE SEQUENCE subID_SEQ;


CREATE OR REPLACE TRIGGER PandaID_TRIGGER
BEFORE INSERT ON jobsDefined4
FOR EACH ROW
BEGIN
	IF (:NEW.PandaID IS NULL) THEN
		SELECT PandaID_SEQ.NEXTVAL INTO :NEW.PandaID FROM DUAL ;
	END IF;
END;
/


CREATE OR REPLACE TRIGGER rowID_TRIGGER
BEFORE INSERT ON filesTable4
FOR EACH ROW
BEGIN
	SELECT rowID_SEQ.NEXTVAL INTO :NEW.row_ID FROM DUAL ;
END;
/


CREATE OR REPLACE TRIGGER subID_TRIGGER
BEFORE INSERT ON subCounter
FOR EACH ROW
BEGIN
	SELECT subID_SEQ.NEXTVAL INTO :NEW.subID FROM DUAL ;
END;
/


CREATE OR REPLACE FUNCTION BITOR( P_BITS1 IN NATURAL, P_BITS2 IN NATURAL )
RETURN NATURAL
IS
BEGIN
	RETURN UTL_RAW.CAST_TO_BINARY_INTEGER(
		UTL_RAW.BIT_OR(
			UTL_RAW.CAST_FROM_BINARY_INTEGER(P_BITS1),
			UTL_RAW.CAST_FROM_BINARY_INTEGER(P_BITS2)
	 	)
 	);
END;
/
