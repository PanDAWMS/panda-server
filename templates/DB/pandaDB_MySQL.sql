-- MySQL dump 10.13  Distrib 5.1.73, for redhat-linux-gnu (x86_64)
--
-- Host: 192.168.23.42    Database: pandadbat
-- ------------------------------------------------------
-- Server version	5.1.73

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `cache`
--

DROP TABLE IF EXISTS `cache`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `cache` (
  `TYPE` varchar(250) NOT NULL,
  `VALUE` varchar(250) NOT NULL,
  `QURL` varchar(250) NOT NULL,
  `MODTIME` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `USETIME` datetime NOT NULL DEFAULT '1970-01-01 00:00:00',
  `UPDMIN` int(11) DEFAULT NULL,
  `DATA` text,
  PRIMARY KEY (`TYPE`,`VALUE`),
  UNIQUE KEY `CACHE_PK` (`TYPE`,`VALUE`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `cache`
--

LOCK TABLES `cache` WRITE;
/*!40000 ALTER TABLE `cache` DISABLE KEYS */;
/*!40000 ALTER TABLE `cache` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `cache_old`
--

DROP TABLE IF EXISTS `cache_old`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `cache_old` (
  `TYPE` varchar(250) NOT NULL,
  `VALUE` varchar(250) NOT NULL,
  `QURL` varchar(250) NOT NULL,
  `MODTIME` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `USETIME` datetime NOT NULL DEFAULT '1970-01-01 00:00:00',
  `UPDMIN` int(11) DEFAULT NULL,
  `DATA` text,
  PRIMARY KEY (`TYPE`,`VALUE`),
  UNIQUE KEY `PRIMARY_CACHE` (`TYPE`,`VALUE`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `cache_old`
--

LOCK TABLES `cache_old` WRITE;
/*!40000 ALTER TABLE `cache_old` DISABLE KEYS */;
/*!40000 ALTER TABLE `cache_old` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `cache_old_2`
--

DROP TABLE IF EXISTS `cache_old_2`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `cache_old_2` (
  `TYPE` varchar(250) NOT NULL,
  `VALUE` varchar(250) NOT NULL,
  `QURL` varchar(250) NOT NULL,
  `MODTIME` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `USETIME` datetime NOT NULL DEFAULT '1970-01-01 00:00:00',
  `UPDMIN` int(11) DEFAULT NULL,
  `DATA` text,
  PRIMARY KEY (`TYPE`,`VALUE`),
  UNIQUE KEY `PRIMARY_CACHE_NEW` (`TYPE`,`VALUE`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `cache_old_2`
--

LOCK TABLES `cache_old_2` WRITE;
/*!40000 ALTER TABLE `cache_old_2` DISABLE KEYS */;
/*!40000 ALTER TABLE `cache_old_2` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `cache_track_new`
--

DROP TABLE IF EXISTS `cache_track_new`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `cache_track_new` (
  `TRACK_TIME` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `TYPE` varchar(250) DEFAULT NULL,
  `VALUE` varchar(250) DEFAULT NULL,
  `LOB_LEN_OLD` int(11) DEFAULT NULL,
  `LOB_LEN_NEW` int(11) DEFAULT NULL,
  `USERHOST` varchar(250) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `cache_track_new`
--

LOCK TABLES `cache_track_new` WRITE;
/*!40000 ALTER TABLE `cache_track_new` DISABLE KEYS */;
/*!40000 ALTER TABLE `cache_track_new` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `certificates`
--

DROP TABLE IF EXISTS `certificates`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `certificates` (
  `ID` int(11) NOT NULL,
  `CERT` varchar(4000) NOT NULL,
  PRIMARY KEY (`ID`),
  UNIQUE KEY `PRIMARY_CERTIFICATES` (`ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `certificates`
--

LOCK TABLES `certificates` WRITE;
/*!40000 ALTER TABLE `certificates` DISABLE KEYS */;
/*!40000 ALTER TABLE `certificates` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `classlist`
--

DROP TABLE IF EXISTS `classlist`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `classlist` (
  `CLASS` varchar(30) NOT NULL,
  `NAME` varchar(60) NOT NULL,
  `RIGHTS` varchar(30) NOT NULL,
  `PRIORITY` int(11) DEFAULT NULL,
  `QUOTA1` bigint(20) DEFAULT NULL,
  `QUOTA7` bigint(20) DEFAULT NULL,
  `QUOTA30` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`CLASS`,`NAME`),
  UNIQUE KEY `PRIMARY_CLASSLIST` (`CLASS`,`NAME`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `classlist`
--

LOCK TABLES `classlist` WRITE;
/*!40000 ALTER TABLE `classlist` DISABLE KEYS */;
/*!40000 ALTER TABLE `classlist` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `cloudconfig`
--

DROP TABLE IF EXISTS `cloudconfig`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `cloudconfig` (
  `NAME` varchar(20) NOT NULL,
  `DESCRIPTION` varchar(50) NOT NULL,
  `TIER1` varchar(20) NOT NULL,
  `TIER1SE` varchar(400) NOT NULL,
  `RELOCATION` varchar(10) DEFAULT NULL,
  `WEIGHT` int(11) NOT NULL DEFAULT '0',
  `SERVER` varchar(100) NOT NULL,
  `STATUS` varchar(20) NOT NULL,
  `TRANSTIMELO` int(11) NOT NULL DEFAULT '0',
  `TRANSTIMEHI` int(11) NOT NULL DEFAULT '0',
  `WAITTIME` int(11) NOT NULL DEFAULT '0',
  `COMMENT_` varchar(200) DEFAULT NULL,
  `SPACE` int(11) NOT NULL DEFAULT '0',
  `MODUSER` varchar(30) DEFAULT NULL,
  `MODTIME` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `VALIDATION` varchar(20) DEFAULT NULL,
  `MCSHARE` int(11) NOT NULL DEFAULT '0',
  `COUNTRIES` varchar(80) DEFAULT NULL,
  `FASTTRACK` varchar(20) DEFAULT NULL,
  `NPRESTAGE` int(11) NOT NULL DEFAULT '0',
  `PILOTOWNERS` varchar(300) DEFAULT NULL,
  `DN` varchar(100) DEFAULT NULL,
  `EMAIL` varchar(60) DEFAULT NULL,
  `FAIRSHARE` varchar(128) DEFAULT NULL,
  `AUTO_MCU` tinyint(4) NOT NULL DEFAULT '0',
  PRIMARY KEY (`NAME`),
  UNIQUE KEY `PRIMARY_CLOUDCFG` (`NAME`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `cloudconfig`
--

LOCK TABLES `cloudconfig` WRITE;
/*!40000 ALTER TABLE `cloudconfig` DISABLE KEYS */;
INSERT INTO `cloudconfig` VALUES ('CA','Canada','TRIUMF','TRIUMF-LCG2_DATADISK,TRIUMF-LCG2_DATATAPE,TRIUMF-LCG2_MCTAPE,TRIUMF-LCG2_HOTDISK,TRIUMF-LCG2_PHYS-*,TRIUMF-LCG2_PERF-*',NULL,1000000,'pandasrv.usatlas.bnl.gov','online',4,3,0,'elog.21525',0,'JaroslavaSchovancova','2011-03-03 21:00:00','true',5,'ca','true',3000,NULL,'Bryan L Caron,Bryan Caron,Asoka De Silva,Leslie Groer','caron@triumf.ca,desilva@triumf.ca,groer@physics.utoronto.ca',NULL,0),('CERN','CERN','CERN-PROD','CERN-PROD_DATADISK,CERN-PROD_MCTAPE,CERN-PROD_DATATAPE,CERN-PROD_HOTDISK,CERN-PROD_DATAPREP,CERN-PROD_DET-*,CERN-PROD_PERF-*,CERN-PROD_PHYS-*,CERN-PROD_TRIG-*',NULL,1000000,'pandasrv.usatlas.bnl.gov','online',0,0,0,'elog.23156',0,'Jaroslava.Schovancova','2011-03-14 21:00:00',NULL,3,NULL,NULL,4000,NULL,'Armen Vartapetian','vartap@uta.edu',NULL,0),('DE','Germany','FZK-LCG2','FZK-LCG2_DATADISK,FZK-LCG2_MCTAPE,FZK-LCG2_DATATAPE,FZK-LCG2_HOTDISK,FZK-LCG2_PERF-*',NULL,5000,'pandasrv.usatlas.bnl.gov','online',2,1,0,NULL,0,NULL,'2011-04-12 20:00:00','true',13,'de,pl','true',4000,NULL,'Armen Vartapetian','vartap@uta.edu',NULL,0),('ES','Spain','pic','PIC_DATADISK,PIC_DATATAPE,PIC_MCTAPE,PIC_HOTDISK,PIC_PHYS-*',NULL,5000,'pandasrv.usatlas.bnl.gov','online',4,1,0,'none',0,NULL,'2011-03-22 21:00:00','true',5,'es','true',2000,NULL,'Armen Vartapetian','vartap@uta.edu',NULL,0),('FR','France','IN2P3-CC','IN2P3-CC_DATADISK,IN2P3-CC_MCTAPE,IN2P3-CC_DATATAPE,IN2P3-CC_HOTDISK,IN2P3-CC_PHYS-*,IN2P3-CC_PERF-*',NULL,1000000,'pandasrv.usatlas.bnl.gov','online',2,1,0,NULL,0,'StephaneJezequel','2011-02-07 21:00:00','true',15,'fr,cn,jp','true',3000,NULL,'Armen Vartapetian','vartap@uta.edu',NULL,0),('IT','Italy','INFN-T1','INFN-T1_DATADISK,INFN-T1_MCTAPE,INFN-T1_DATATAPE,INFN-T1_HOTDISK,INFN-T1_PHYS-*',NULL,1000000,'pandasrv.cern.ch','online',2,1,0,'elog.21525',0,'JaroslavaSchovancova','2011-01-25 21:00:00','true',5,'it','true',2000,NULL,'Armen Vartapetian','vartap@uta.edu',NULL,0),('ND','Nordic countries','ARC','NDGF-T1_DATADISK,NDGF-T1_DATATAPE,NDGF-T1_MCTAPE,NDGF-T1_HOTDISK,NDGF-T1_PHYS-*',NULL,-1,'pandasrv.usatlas.bnl.gov','online',2,1,0,'elog.23169',0,'Jaroslava.Schovancova','2011-03-14 21:00:00','true',5,NULL,'true',2000,'Andrej Filipcic','Armen Vartapetian','vartap@uta.edu',NULL,0),('NL','Netherlands','SARA-MATRIX','SARA-MATRIX_DATADISK,SARA-MATRIX_DATATAPE,SARA-MATRIX_MCTAPE,NIKHEF-ELPROD_DATADISK,SARA-MATRIX_HOTDISK,NIKHEF-ELPROD_DET-*,NIKHEF-ELPROD_PERF-*,NIKHEF-ELPROD_PHYS-*',NULL,10,'pandasrv.usatlas.bnl.gov','online',4,1,0,'elog.23497',0,'hclee','2011-03-21 21:00:00','true',15,'nl,ru','true',2000,NULL,'Armen Vartapetian','vartap@uta.edu',NULL,0),('OSG','Open Sciences Grid','BNL_ATLAS_1','BNLPANDA',NULL,0,'pandasrv.usatlas.bnl.gov','brokeroff',0,0,0,'Filler',0,'jcaballero','2010-06-14 20:00:00',NULL,0,'us',NULL,0,'Jose Caballero|Benjamin Timothy Allen Miller','Jose Caballero,Benjamin Timothy Allen Miller','potekhin@bnl.gov,jcaballero@bnl.gov',NULL,0),('RU','RussiaFederation','ANALY_RRC-KI-HPC','RRC_KI_T1_SCRATCHDISK',NULL,1,'vcloud29.grid.kiae.ru','online',4,1,0,'elog.23498',0,'mashinistov','2014-08-31 20:00:00',NULL,0,'ru','true',2000,NULL,'Ruslan Mashinistov','mashinistov@ki',NULL,0),('TW','Taiwan','Taiwan-LCG2','TAIWAN-LCG2_DATADISK,TAIWAN-LCG2_DATATAPE,TAIWAN-LCG2_MCTAPE,TAIWAN-LCG2_HOTDISK,TAIWAN-LCG2_PERF-*,TAIWAN-LCG2_PHYS-*',NULL,2,'pandasrv.usatlas.bnl.gov','online',4,3,0,'elog.23893',0,'iueda','2011-03-31 20:00:00',NULL,5,'tw,au',NULL,2000,NULL,'Armen Vartapetian','vartap@uta.edu',NULL,0),('UK','United Kingdom','RAL-LCG2','RAL-LCG2_DATADISK,RAL-LCG2_MCTAPE,RAL-LCG2_DATATAPE,RAL-LCG2_HOTDISK,RAL-LCG2_PERF-*,RAL-LCG2_PHYS-*',NULL,5000,'pandasrv.usatlas.bnl.gov','online',2,1,0,'https://savannah.cern.ch/support/index.php',0,'AlessandraForti','2011-03-29 20:00:00','true',10,'uk','true',2000,'peter love','graeme stewart,peter love','graeme.andrew.stewart@cern.ch,p.love@lancaster.ac.uk',NULL,0),('US','United States','BNL_CVMFS_1','BNL-OSG2_DATADISK,BNL-OSG2_DATATAPE,BNL-OSG2_MCTAPE,BNL-OSG2_HOTDISK,BNL-OSG2_DET-*,BNL-OSG2_PERF-*,BNL-OSG2_PHYS-*,BNL-OSG2_TRIG-*',NULL,2,'pandasrv.usatlas.bnl.gov','online',4,1,0,'elog.21524',0,'JaroslavaSchovancova','2011-03-30 20:00:00','true',25,'usatlas','true',12000,'Nurcan Ozturk|Jose Caballero|John R. Hover','Armen Vartapetian,Nurcan Ozturk,John R. Hover','vartap@uta.edu,nurcan@hepmail.uta.edu',NULL,0);
/*!40000 ALTER TABLE `cloudconfig` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `cloudspace`
--

DROP TABLE IF EXISTS `cloudspace`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `cloudspace` (
  `CLOUD` varchar(20) NOT NULL,
  `STORE` varchar(50) NOT NULL,
  `SPACE` int(11) NOT NULL DEFAULT '0',
  `FREESPACE` int(11) NOT NULL DEFAULT '0',
  `MODUSER` varchar(30) NOT NULL,
  `MODTIME` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`CLOUD`,`STORE`),
  UNIQUE KEY `PRIMARY_CLOUDSPACE` (`CLOUD`,`STORE`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `cloudspace`
--

LOCK TABLES `cloudspace` WRITE;
/*!40000 ALTER TABLE `cloudspace` DISABLE KEYS */;
/*!40000 ALTER TABLE `cloudspace` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `cloudtasks`
--

DROP TABLE IF EXISTS `cloudtasks`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `cloudtasks` (
  `ID` int(11) NOT NULL AUTO_INCREMENT,
  `TASKNAME` varchar(128) DEFAULT NULL,
  `TASKID` int(11) DEFAULT NULL,
  `CLOUD` varchar(20) DEFAULT NULL,
  `STATUS` varchar(20) DEFAULT NULL,
  `TMOD` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `TENTER` datetime NOT NULL DEFAULT '1970-01-01 00:00:00',
  PRIMARY KEY (`ID`),
  UNIQUE KEY `CLOUDTASKS_ID_PK` (`ID`),
  KEY `CLOUDTASKS_TASK_IDX` (`TASKNAME`,`TASKID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `cloudtasks`
--

LOCK TABLES `cloudtasks` WRITE;
/*!40000 ALTER TABLE `cloudtasks` DISABLE KEYS */;
/*!40000 ALTER TABLE `cloudtasks` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `cloudtasks_id_seq`
--

DROP TABLE IF EXISTS `cloudtasks_id_seq`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `cloudtasks_id_seq` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `col` bit(1) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `cloudtasks_id_seq`
--

LOCK TABLES `cloudtasks_id_seq` WRITE;
/*!40000 ALTER TABLE `cloudtasks_id_seq` DISABLE KEYS */;
INSERT INTO `cloudtasks_id_seq` VALUES (1,NULL);
/*!40000 ALTER TABLE `cloudtasks_id_seq` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `datasets`
--

DROP TABLE IF EXISTS `datasets`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `datasets` (
  `VUID` varchar(40) NOT NULL,
  `NAME` varchar(255) NOT NULL,
  `VERSION` varchar(10) DEFAULT NULL,
  `TYPE` varchar(20) DEFAULT NULL,
  `STATUS` varchar(10) DEFAULT NULL,
  `NUMBERFILES` int(11) DEFAULT NULL,
  `CURRENTFILES` int(11) DEFAULT NULL,
  `CREATIONDATE` datetime DEFAULT NULL,
  `MODIFICATIONDATE` datetime NOT NULL,
  `MOVERID` bigint(20) NOT NULL DEFAULT '0',
  `TRANSFERSTATUS` tinyint(4) NOT NULL DEFAULT '0',
  `SUBTYPE` varchar(5) DEFAULT NULL,
  PRIMARY KEY (`VUID`,`MODIFICATIONDATE`),
  UNIQUE KEY `DATASETS_VUID_MODIFDATE_PK` (`VUID`,`MODIFICATIONDATE`),
  KEY `DATASETS_MOVERID_INDX` (`MOVERID`),
  KEY `DATASETS_STAT_TYPE_MDATE_IDX` (`TYPE`,`STATUS`,`MODIFICATIONDATE`,`SUBTYPE`),
  KEY `DATASETS_NAME_IDX` (`NAME`),
  KEY `DATASETS_MODIFDATE_IDX` (`MODIFICATIONDATE`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `datasets`
--

LOCK TABLES `datasets` WRITE;
/*!40000 ALTER TABLE `datasets` DISABLE KEYS */;
/*!40000 ALTER TABLE `datasets` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `dslist`
--

DROP TABLE IF EXISTS `dslist`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dslist` (
  `ID` int(11) NOT NULL,
  `DUID` varchar(40) DEFAULT NULL,
  `NAME` varchar(200) NOT NULL,
  `UGID` int(11) DEFAULT NULL,
  `PRIORITY` int(11) DEFAULT NULL,
  `STATUS` varchar(10) DEFAULT NULL,
  `LASTUSE` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `PINSTATE` varchar(10) DEFAULT NULL,
  `PINTIME` datetime NOT NULL DEFAULT '1970-01-01 00:00:00',
  `LIFETIME` datetime NOT NULL DEFAULT '1970-01-01 00:00:00',
  `SITE` varchar(60) DEFAULT NULL,
  `PAR1` varchar(30) DEFAULT NULL,
  `PAR2` varchar(30) DEFAULT NULL,
  `PAR3` varchar(30) DEFAULT NULL,
  `PAR4` varchar(30) DEFAULT NULL,
  `PAR5` varchar(30) DEFAULT NULL,
  `PAR6` varchar(30) DEFAULT NULL,
  PRIMARY KEY (`ID`),
  UNIQUE KEY `PRIMARY_DSLIST` (`ID`),
  KEY `DSLIST_NAME_IDX` (`NAME`,`SITE`),
  KEY `DSLIST_DUID_IDX` (`DUID`,`SITE`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `dslist`
--

LOCK TABLES `dslist` WRITE;
/*!40000 ALTER TABLE `dslist` DISABLE KEYS */;
/*!40000 ALTER TABLE `dslist` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `filestable4`
--

DROP TABLE IF EXISTS `filestable4`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `filestable4` (
  `ROW_ID` bigint(20) NOT NULL AUTO_INCREMENT,
  `PANDAID` bigint(20) NOT NULL DEFAULT '0',
  `MODIFICATIONTIME` datetime NOT NULL DEFAULT '1970-01-01 00:00:00',
  `GUID` varchar(64) DEFAULT NULL,
  `LFN` varchar(256) DEFAULT NULL,
  `TYPE` varchar(20) DEFAULT NULL,
  `DATASET` varchar(255) DEFAULT NULL,
  `STATUS` varchar(64) DEFAULT NULL,
  `PRODDBLOCK` varchar(255) DEFAULT NULL,
  `PRODDBLOCKTOKEN` varchar(250) DEFAULT NULL,
  `DISPATCHDBLOCK` varchar(255) DEFAULT NULL,
  `DISPATCHDBLOCKTOKEN` varchar(250) DEFAULT NULL,
  `DESTINATIONDBLOCK` varchar(255) DEFAULT NULL,
  `DESTINATIONDBLOCKTOKEN` varchar(250) DEFAULT NULL,
  `DESTINATIONSE` varchar(250) DEFAULT NULL,
  `FSIZE` bigint(20) NOT NULL DEFAULT '0',
  `MD5SUM` varchar(36) DEFAULT NULL,
  `CHECKSUM` varchar(36) DEFAULT NULL,
  `SCOPE` varchar(30) DEFAULT NULL,
  `JEDITASKID` bigint(20) DEFAULT NULL,
  `DATASETID` bigint(20) DEFAULT NULL,
  `FILEID` bigint(20) DEFAULT NULL,
  `ATTEMPTNR` tinyint(4) DEFAULT NULL,
  PRIMARY KEY (`ROW_ID`,`MODIFICATIONTIME`),
  UNIQUE KEY `PART_FILESTABLE4_PK` (`ROW_ID`,`MODIFICATIONTIME`),
  KEY `FILESTABLE4_PANDAID_IDX` (`PANDAID`),
  KEY `FILESTABLE4_TASKFILEID_IDX` (`JEDITASKID`,`DATASETID`,`FILEID`),
  KEY `FILESTABLE4_DESTDBLOCK_IDX` (`DESTINATIONDBLOCK`),
  KEY `FILESTABLE4_DISPDBLOCK_IDX` (`DISPATCHDBLOCK`),
  KEY `FILESTABLE4_DATASETYPE3COL_IDX` (`DATASET`,`TYPE`,`DESTINATIONDBLOCK`,`STATUS`,`PANDAID`),
  CONSTRAINT `FILESTABLE4_FILEID_FK` FOREIGN KEY (`JEDITASKID`, `DATASETID`, `FILEID`) REFERENCES `jedi_dataset_contents` (`JEDITASKID`, `DATASETID`, `FILEID`)
) ENGINE=InnoDB AUTO_INCREMENT=217 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `filestable4`
--

LOCK TABLES `filestable4` WRITE;
/*!40000 ALTER TABLE `filestable4` DISABLE KEYS */;
/*!40000 ALTER TABLE `filestable4` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `filestable4_row_id_seq`
--

DROP TABLE IF EXISTS `filestable4_row_id_seq`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `filestable4_row_id_seq` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `col` bit(1) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `filestable4_row_id_seq`
--

LOCK TABLES `filestable4_row_id_seq` WRITE;
/*!40000 ALTER TABLE `filestable4_row_id_seq` DISABLE KEYS */;
/*!40000 ALTER TABLE `filestable4_row_id_seq` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `filestable_arch`
--

DROP TABLE IF EXISTS `filestable_arch`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `filestable_arch` (
  `ROW_ID` bigint(20) NOT NULL,
  `PANDAID` bigint(20) NOT NULL DEFAULT '0',
  `MODIFICATIONTIME` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `CREATIONTIME` datetime DEFAULT NULL,
  `GUID` varchar(64) DEFAULT NULL,
  `LFN` varchar(256) DEFAULT NULL,
  `TYPE` varchar(20) DEFAULT NULL,
  `FSIZE` bigint(20) DEFAULT '0',
  `DATASET` varchar(255) DEFAULT NULL,
  `STATUS` varchar(64) DEFAULT NULL,
  `MD5SUM` varchar(40) DEFAULT NULL,
  `CHECKSUM` varchar(40) DEFAULT NULL,
  `PRODDBLOCK` varchar(255) DEFAULT NULL,
  `PRODDBLOCKTOKEN` varchar(250) DEFAULT NULL,
  `DISPATCHDBLOCK` varchar(255) DEFAULT NULL,
  `DISPATCHDBLOCKTOKEN` varchar(250) DEFAULT NULL,
  `DESTINATIONDBLOCK` varchar(255) DEFAULT NULL,
  `DESTINATIONDBLOCKTOKEN` varchar(250) DEFAULT NULL,
  `DESTINATIONSE` varchar(250) DEFAULT NULL,
  `SCOPE` varchar(30) DEFAULT NULL,
  `JEDITASKID` bigint(20) DEFAULT NULL,
  `DATASETID` bigint(20) DEFAULT NULL,
  `FILEID` bigint(20) DEFAULT NULL,
  `ATTEMPTNR` tinyint(4) DEFAULT NULL,
  KEY `FILES_ARCH_ROWID_IDX` (`ROW_ID`),
  KEY `FILES_ARCH_PANDAID_IDX` (`PANDAID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `filestable_arch`
--

LOCK TABLES `filestable_arch` WRITE;
/*!40000 ALTER TABLE `filestable_arch` DISABLE KEYS */;
/*!40000 ALTER TABLE `filestable_arch` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `group_jobid_seq`
--

DROP TABLE IF EXISTS `group_jobid_seq`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `group_jobid_seq` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `col` bit(1) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `group_jobid_seq`
--

LOCK TABLES `group_jobid_seq` WRITE;
/*!40000 ALTER TABLE `group_jobid_seq` DISABLE KEYS */;
/*!40000 ALTER TABLE `group_jobid_seq` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `groups`
--

DROP TABLE IF EXISTS `groups`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `groups` (
  `ID` int(11) NOT NULL,
  `NAME` varchar(60) NOT NULL,
  `DESCRIPTION` varchar(120) NOT NULL,
  `URL` varchar(100) DEFAULT NULL,
  `CLASSA` varchar(30) DEFAULT NULL,
  `CLASSP` varchar(30) DEFAULT NULL,
  `CLASSXP` varchar(30) DEFAULT NULL,
  `NJOBS1` int(11) DEFAULT NULL,
  `NJOBS7` int(11) DEFAULT NULL,
  `NJOBS30` int(11) DEFAULT NULL,
  `CPUA1` bigint(20) DEFAULT NULL,
  `CPUA7` bigint(20) DEFAULT NULL,
  `CPUA30` bigint(20) DEFAULT NULL,
  `CPUP1` bigint(20) DEFAULT NULL,
  `CPUP7` bigint(20) DEFAULT NULL,
  `CPUP30` bigint(20) DEFAULT NULL,
  `CPUXP1` bigint(20) DEFAULT NULL,
  `CPUXP7` bigint(20) DEFAULT NULL,
  `CPUXP30` bigint(20) DEFAULT NULL,
  `ALLCPUA1` bigint(20) DEFAULT NULL,
  `ALLCPUA7` bigint(20) DEFAULT NULL,
  `ALLCPUA30` bigint(20) DEFAULT NULL,
  `ALLCPUP1` bigint(20) DEFAULT NULL,
  `ALLCPUP7` bigint(20) DEFAULT NULL,
  `ALLCPUP30` bigint(20) DEFAULT NULL,
  `ALLCPUXP1` bigint(20) DEFAULT NULL,
  `ALLCPUXP7` bigint(20) DEFAULT NULL,
  `ALLCPUXP30` bigint(20) DEFAULT NULL,
  `QUOTAA1` bigint(20) DEFAULT NULL,
  `QUOTAA7` bigint(20) DEFAULT NULL,
  `QUOTAA30` bigint(20) DEFAULT NULL,
  `QUOTAP1` bigint(20) DEFAULT NULL,
  `QUOTAP7` bigint(20) DEFAULT NULL,
  `QUOTAP30` bigint(20) DEFAULT NULL,
  `QUOTAXP1` bigint(20) DEFAULT NULL,
  `QUOTAXP7` bigint(20) DEFAULT NULL,
  `QUOTAXP30` bigint(20) DEFAULT NULL,
  `ALLQUOTAA1` bigint(20) DEFAULT NULL,
  `ALLQUOTAA7` bigint(20) DEFAULT NULL,
  `ALLQUOTAA30` bigint(20) DEFAULT NULL,
  `ALLQUOTAP1` bigint(20) DEFAULT NULL,
  `ALLQUOTAP7` bigint(20) DEFAULT NULL,
  `ALLQUOTAP30` bigint(20) DEFAULT NULL,
  `ALLQUOTAXP1` bigint(20) DEFAULT NULL,
  `ALLQUOTAXP7` bigint(20) DEFAULT NULL,
  `ALLQUOTAXP30` bigint(20) DEFAULT NULL,
  `SPACE1` int(11) DEFAULT NULL,
  `SPACE7` int(11) DEFAULT NULL,
  `SPACE30` int(11) DEFAULT NULL,
  PRIMARY KEY (`ID`),
  UNIQUE KEY `PRIMARY_GROUPS` (`ID`),
  UNIQUE KEY `GROUPS_NAME_IDX` (`NAME`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `groups`
--

LOCK TABLES `groups` WRITE;
/*!40000 ALTER TABLE `groups` DISABLE KEYS */;
/*!40000 ALTER TABLE `groups` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `history`
--

DROP TABLE IF EXISTS `history`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `history` (
  `ID` int(11) NOT NULL,
  `ENTRYTIME` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `STARTTIME` datetime NOT NULL DEFAULT '1970-01-01 00:00:00',
  `ENDTIME` datetime NOT NULL DEFAULT '1970-01-01 00:00:00',
  `CPU` bigint(20) DEFAULT NULL,
  `CPUXP` bigint(20) DEFAULT NULL,
  `SPACE` int(11) DEFAULT NULL,
  PRIMARY KEY (`ID`),
  UNIQUE KEY `PRIMARY_HISTORY` (`ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `history`
--

LOCK TABLES `history` WRITE;
/*!40000 ALTER TABLE `history` DISABLE KEYS */;
/*!40000 ALTER TABLE `history` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `incidents`
--

DROP TABLE IF EXISTS `incidents`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `incidents` (
  `AT_TIME` datetime DEFAULT '0000-00-00 00:00:00',
  `TYPEKEY` varchar(20) DEFAULT NULL,
  `DESCRIPTION` varchar(200) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `incidents`
--

LOCK TABLES `incidents` WRITE;
/*!40000 ALTER TABLE `incidents` DISABLE KEYS */;
/*!40000 ALTER TABLE `incidents` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `installedsw`
--

DROP TABLE IF EXISTS `installedsw`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `installedsw` (
  `SITEID` varchar(60) NOT NULL DEFAULT '',
  `CLOUD` varchar(10) DEFAULT NULL,
  `RELEASE` varchar(10) NOT NULL DEFAULT '',
  `CACHE` varchar(40) NOT NULL DEFAULT '',
  `VALIDATION` varchar(10) DEFAULT NULL,
  `CMTCONFIG` varchar(40) NOT NULL DEFAULT '',
  PRIMARY KEY (`SITEID`,`RELEASE`,`CACHE`,`CMTCONFIG`),
  UNIQUE KEY `INSTALLEDSW_SITERELCACHECMT_UK` (`SITEID`,`RELEASE`,`CACHE`,`CMTCONFIG`),
  KEY `INSTALLEDSW_RELID_SITE_INDX` (`RELEASE`,`SITEID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `installedsw`
--

LOCK TABLES `installedsw` WRITE;
/*!40000 ALTER TABLE `installedsw` DISABLE KEYS */;
/*!40000 ALTER TABLE `installedsw` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `jdllist`
--

DROP TABLE IF EXISTS `jdllist`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `jdllist` (
  `NAME` varchar(60) NOT NULL,
  `HOST` varchar(60) DEFAULT NULL,
  `SYSTEM` varchar(20) NOT NULL,
  `JDL` varchar(4000) DEFAULT NULL,
  PRIMARY KEY (`NAME`),
  UNIQUE KEY `PRIMARY_JDLLIST` (`NAME`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `jdllist`
--

LOCK TABLES `jdllist` WRITE;
/*!40000 ALTER TABLE `jdllist` DISABLE KEYS */;
/*!40000 ALTER TABLE `jdllist` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `jedi_aux_status_mintaskid`
--

DROP TABLE IF EXISTS `jedi_aux_status_mintaskid`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `jedi_aux_status_mintaskid` (
  `STATUS` varchar(64) NOT NULL,
  `MIN_JEDITASKID` bigint(20) NOT NULL,
  PRIMARY KEY (`STATUS`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `jedi_aux_status_mintaskid`
--

LOCK TABLES `jedi_aux_status_mintaskid` WRITE;
/*!40000 ALTER TABLE `jedi_aux_status_mintaskid` DISABLE KEYS */;
/*!40000 ALTER TABLE `jedi_aux_status_mintaskid` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `jedi_dataset_cont_fileid_seq`
--

DROP TABLE IF EXISTS `jedi_dataset_cont_fileid_seq`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `jedi_dataset_cont_fileid_seq` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `col` bit(1) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `jedi_dataset_cont_fileid_seq`
--

LOCK TABLES `jedi_dataset_cont_fileid_seq` WRITE;
/*!40000 ALTER TABLE `jedi_dataset_cont_fileid_seq` DISABLE KEYS */;
/*!40000 ALTER TABLE `jedi_dataset_cont_fileid_seq` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `jedi_dataset_contents`
--

DROP TABLE IF EXISTS `jedi_dataset_contents`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `jedi_dataset_contents` (
  `JEDITASKID` bigint(20) NOT NULL,
  `DATASETID` bigint(20) NOT NULL,
  `FILEID` bigint(20) NOT NULL,
  `CREATIONDATE` datetime NOT NULL,
  `LASTATTEMPTTIME` datetime DEFAULT NULL,
  `LFN` varchar(256) NOT NULL,
  `GUID` varchar(64) DEFAULT NULL,
  `TYPE` varchar(20) NOT NULL,
  `STATUS` varchar(64) NOT NULL,
  `FSIZE` bigint(20) DEFAULT NULL,
  `CHECKSUM` varchar(36) DEFAULT NULL,
  `SCOPE` varchar(30) DEFAULT NULL,
  `ATTEMPTNR` tinyint(4) DEFAULT NULL,
  `MAXATTEMPT` tinyint(4) DEFAULT NULL,
  `NEVENTS` int(11) DEFAULT NULL,
  `KEEPTRACK` tinyint(4) DEFAULT NULL,
  `STARTEVENT` int(11) DEFAULT NULL,
  `ENDEVENT` int(11) DEFAULT NULL,
  `FIRSTEVENT` int(11) DEFAULT NULL,
  `BOUNDARYID` bigint(20) DEFAULT NULL,
  `PANDAID` bigint(20) DEFAULT NULL,
  `FAILEDATTEMPT` tinyint(4) DEFAULT NULL,
  `LUMIBLOCKNR` int(11) DEFAULT NULL,
  `OUTPANDAID` bigint(20) DEFAULT NULL,
  `MAXFAILURE` int(3) DEFAULT NULL,
  `RAMCOUNT` int(10) DEFAULT NULL,
  PRIMARY KEY (`JEDITASKID`,`DATASETID`,`FILEID`),
  UNIQUE KEY `JEDI_DATASET_CONTENTS_PK` (`JEDITASKID`,`DATASETID`,`FILEID`),
  KEY `JEDI_DATASET_CONTENTS_PID_IDX` (`PANDAID`),
  KEY `JEDI_DATASET_CONTENTS_ID_IDX` (`DATASETID`),
  CONSTRAINT `JEDI_DATASETCONT_DATASETID_FK` FOREIGN KEY (`JEDITASKID`, `DATASETID`) REFERENCES `jedi_datasets` (`JEDITASKID`, `DATASETID`),
  CONSTRAINT `JEDI_DATASETCONT_JEDITASKID_FK` FOREIGN KEY (`JEDITASKID`) REFERENCES `jedi_tasks` (`JEDITASKID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `jedi_dataset_contents`
--

LOCK TABLES `jedi_dataset_contents` WRITE;
/*!40000 ALTER TABLE `jedi_dataset_contents` DISABLE KEYS */;
/*!40000 ALTER TABLE `jedi_dataset_contents` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `jedi_datasets`
--

DROP TABLE IF EXISTS `jedi_datasets`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `jedi_datasets` (
  `JEDITASKID` bigint(20) NOT NULL,
  `DATASETID` bigint(20) NOT NULL,
  `DATASETNAME` varchar(255) NOT NULL,
  `TYPE` varchar(20) NOT NULL,
  `CREATIONTIME` datetime NOT NULL,
  `MODIFICATIONTIME` datetime NOT NULL,
  `VO` varchar(16) DEFAULT NULL,
  `CLOUD` varchar(10) DEFAULT NULL,
  `SITE` varchar(60) DEFAULT NULL,
  `MASTERID` bigint(20) DEFAULT NULL,
  `PROVENANCEID` bigint(20) DEFAULT NULL,
  `CONTAINERNAME` varchar(255) DEFAULT NULL,
  `STATUS` varchar(20) DEFAULT NULL,
  `STATE` varchar(20) DEFAULT NULL,
  `STATECHECKTIME` datetime DEFAULT NULL,
  `STATECHECKEXPIRATION` datetime DEFAULT NULL,
  `FROZENTIME` datetime DEFAULT NULL,
  `NFILES` int(11) DEFAULT NULL,
  `NFILESTOBEUSED` int(11) DEFAULT NULL,
  `NFILESUSED` int(11) DEFAULT NULL,
  `NFILESONHOLD` int(11) DEFAULT NULL,
  `NEVENTS` bigint(20) DEFAULT NULL,
  `NEVENTSTOBEUSED` int(11) DEFAULT NULL,
  `NEVENTSUSED` int(11) DEFAULT NULL,
  `LOCKEDBY` varchar(40) DEFAULT NULL,
  `LOCKEDTIME` datetime DEFAULT NULL,
  `NFILESFINISHED` int(11) DEFAULT NULL,
  `NFILESFAILED` int(11) DEFAULT NULL,
  `ATTRIBUTES` varchar(100) DEFAULT NULL,
  `STREAMNAME` varchar(20) DEFAULT NULL,
  `STORAGETOKEN` varchar(60) DEFAULT NULL,
  `DESTINATION` varchar(60) DEFAULT NULL,
  `TEMPLATEID` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`JEDITASKID`,`DATASETID`),
  UNIQUE KEY `JEDI_DATASETS_PK` (`JEDITASKID`,`DATASETID`),
  KEY `JEDI_DATASETS_DSETID_IDX` (`DATASETID`),
  KEY `JEDI_DATASETS_TASKID_TYPE_IDX` (`JEDITASKID`,`TYPE`),
  KEY `JEDI_DATASET_DNAMETYPETID_IDX` (`DATASETNAME`,`TYPE`,`JEDITASKID`),
  KEY `JEDI_DATASET_CONTAINERNAME_IDX` (`CONTAINERNAME`),
  KEY `JEDI_DATASET_STATECHECKEXP_IDX` (`STATECHECKEXPIRATION`),
  KEY `JEDI_DATASET_LOCKEDBY_IDX` (`LOCKEDBY`),
  CONSTRAINT `JEDI_DATASETS_JEDITASKID_FK` FOREIGN KEY (`JEDITASKID`) REFERENCES `jedi_tasks` (`JEDITASKID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `jedi_datasets`
--

LOCK TABLES `jedi_datasets` WRITE;
/*!40000 ALTER TABLE `jedi_datasets` DISABLE KEYS */;
/*!40000 ALTER TABLE `jedi_datasets` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `jedi_datasets_id_seq`
--

DROP TABLE IF EXISTS `jedi_datasets_id_seq`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `jedi_datasets_id_seq` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `col` bit(1) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `jedi_datasets_id_seq`
--

LOCK TABLES `jedi_datasets_id_seq` WRITE;
/*!40000 ALTER TABLE `jedi_datasets_id_seq` DISABLE KEYS */;
/*!40000 ALTER TABLE `jedi_datasets_id_seq` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `jedi_events`
--

DROP TABLE IF EXISTS `jedi_events`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `jedi_events` (
  `JEDITASKID` bigint(20) NOT NULL,
  `PANDAID` bigint(20) NOT NULL,
  `FILEID` bigint(20) NOT NULL,
  `JOB_PROCESSID` int(11) NOT NULL,
  `DATASETID` bigint(20) NOT NULL,
  `STATUS` tinyint(4) NOT NULL,
  `DEF_MIN_EVENTID` int(11) DEFAULT NULL,
  `DEF_MAX_EVENTID` int(11) DEFAULT NULL,
  `PROCESSED_UPTO_EVENTID` int(11) DEFAULT NULL,
  `ATTEMPTNR` tinyint(4) DEFAULT NULL,
  PRIMARY KEY (`JEDITASKID`,`PANDAID`,`FILEID`,`JOB_PROCESSID`),
  CONSTRAINT `JEDI_EVENTS_TASKID_FK` FOREIGN KEY (`JEDITASKID`) REFERENCES `jedi_tasks` (`JEDITASKID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `jedi_events`
--

LOCK TABLES `jedi_events` WRITE;
/*!40000 ALTER TABLE `jedi_events` DISABLE KEYS */;
/*!40000 ALTER TABLE `jedi_events` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `jedi_job_retry_history`
--

DROP TABLE IF EXISTS `jedi_job_retry_history`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `jedi_job_retry_history` (
  `JEDITASKID` bigint(20) NOT NULL,
  `OLDPANDAID` bigint(20) NOT NULL,
  `NEWPANDAID` bigint(20) NOT NULL,
  `INS_UTC_TSTAMP` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `RELATIONTYPE` varchar(16) DEFAULT NULL,
  `ORIGINPANDAID` bigint(20) NOT NULL DEFAULT '0',
  PRIMARY KEY (`JEDITASKID`,`OLDPANDAID`,`NEWPANDAID`,`ORIGINPANDAID`),
  UNIQUE KEY `JEDI_JOB_RETRY_HISTORY_UQ` (`JEDITASKID`,`NEWPANDAID`,`OLDPANDAID`,`ORIGINPANDAID`),
  KEY `JEDI_JOB_RETRY_HIST_ORIGID_IDX` (`ORIGINPANDAID`),
  CONSTRAINT `JEDI_JOB_RETRY_HIST_TASKID_FK` FOREIGN KEY (`JEDITASKID`) REFERENCES `jedi_tasks` (`JEDITASKID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `jedi_job_retry_history`
--

LOCK TABLES `jedi_job_retry_history` WRITE;
/*!40000 ALTER TABLE `jedi_job_retry_history` DISABLE KEYS */;
/*!40000 ALTER TABLE `jedi_job_retry_history` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `jedi_jobparams_template`
--

DROP TABLE IF EXISTS `jedi_jobparams_template`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `jedi_jobparams_template` (
  `JEDITASKID` bigint(20) NOT NULL,
  `JOBPARAMSTEMPLATE` text,
  PRIMARY KEY (`JEDITASKID`),
  UNIQUE KEY `JEDI_JOBPARAMSTEMPL_PK` (`JEDITASKID`),
  CONSTRAINT `JEDI_JOBPARTEMPL_JEDITASKID_FK` FOREIGN KEY (`JEDITASKID`) REFERENCES `jedi_tasks` (`JEDITASKID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `jedi_jobparams_template`
--

LOCK TABLES `jedi_jobparams_template` WRITE;
/*!40000 ALTER TABLE `jedi_jobparams_template` DISABLE KEYS */;
/*!40000 ALTER TABLE `jedi_jobparams_template` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `jedi_output_template`
--

DROP TABLE IF EXISTS `jedi_output_template`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `jedi_output_template` (
  `JEDITASKID` bigint(20) NOT NULL,
  `DATASETID` bigint(20) NOT NULL,
  `OUTTEMPID` bigint(20) NOT NULL,
  `FILENAMETEMPLATE` varchar(256) NOT NULL,
  `MAXSERIALNR` int(11) DEFAULT NULL,
  `SERIALNR` int(11) DEFAULT NULL,
  `SOURCENAME` varchar(256) DEFAULT NULL,
  `STREAMNAME` varchar(20) DEFAULT NULL,
  `OUTTYPE` varchar(20) DEFAULT NULL,
  PRIMARY KEY (`JEDITASKID`,`DATASETID`,`OUTTEMPID`),
  UNIQUE KEY `JEDI_OUTPUTTEMPL_PK` (`JEDITASKID`,`DATASETID`,`OUTTEMPID`),
  CONSTRAINT `JEDI_OUTPUTTEMPL_DATASETID_FK` FOREIGN KEY (`JEDITASKID`, `DATASETID`) REFERENCES `jedi_datasets` (`JEDITASKID`, `DATASETID`),
  CONSTRAINT `JEDI_OUTPUTTEMPL_JEDITASKID_FK` FOREIGN KEY (`JEDITASKID`) REFERENCES `jedi_tasks` (`JEDITASKID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `jedi_output_template`
--

LOCK TABLES `jedi_output_template` WRITE;
/*!40000 ALTER TABLE `jedi_output_template` DISABLE KEYS */;
/*!40000 ALTER TABLE `jedi_output_template` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `jedi_output_template_id_seq`
--

DROP TABLE IF EXISTS `jedi_output_template_id_seq`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `jedi_output_template_id_seq` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `col` bit(1) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `jedi_output_template_id_seq`
--

LOCK TABLES `jedi_output_template_id_seq` WRITE;
/*!40000 ALTER TABLE `jedi_output_template_id_seq` DISABLE KEYS */;
/*!40000 ALTER TABLE `jedi_output_template_id_seq` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `jedi_process_lock`
--

DROP TABLE IF EXISTS `jedi_process_lock`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `jedi_process_lock` (
  `VO` varchar(16) NOT NULL,
  `PRODSOURCELABEL` varchar(20) NOT NULL,
  `WORKQUEUE_ID` int(11) NOT NULL,
  `CLOUD` varchar(10) NOT NULL,
  `LOCKEDBY` varchar(40) DEFAULT NULL,
  `LOCKEDTIME` datetime DEFAULT NULL,
  PRIMARY KEY (`VO`,`PRODSOURCELABEL`,`WORKQUEUE_ID`,`CLOUD`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `jedi_process_lock`
--

LOCK TABLES `jedi_process_lock` WRITE;
/*!40000 ALTER TABLE `jedi_process_lock` DISABLE KEYS */;
/*!40000 ALTER TABLE `jedi_process_lock` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `jedi_taskparams`
--

DROP TABLE IF EXISTS `jedi_taskparams`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `jedi_taskparams` (
  `JEDITASKID` bigint(20) NOT NULL,
  `TASKPARAMS` text,
  PRIMARY KEY (`JEDITASKID`),
  UNIQUE KEY `JEDI_TASKPARAMS_JEDITASKID_PK` (`JEDITASKID`),
  CONSTRAINT `JEDI_TASKPARAMS_JEDITASKID_FK` FOREIGN KEY (`JEDITASKID`) REFERENCES `jedi_tasks` (`JEDITASKID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `jedi_taskparams`
--

LOCK TABLES `jedi_taskparams` WRITE;
/*!40000 ALTER TABLE `jedi_taskparams` DISABLE KEYS */;
/*!40000 ALTER TABLE `jedi_taskparams` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `jedi_tasks`
--

DROP TABLE IF EXISTS `jedi_tasks`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `jedi_tasks` (
  `JEDITASKID` bigint(20) NOT NULL,
  `TASKNAME` varchar(132) DEFAULT NULL,
  `STATUS` varchar(64) NOT NULL,
  `USERNAME` varchar(128) NOT NULL,
  `CREATIONDATE` datetime NOT NULL,
  `MODIFICATIONTIME` datetime NOT NULL,
  `REQID` int(11) DEFAULT NULL,
  `OLDSTATUS` varchar(64) DEFAULT NULL,
  `CLOUD` varchar(10) DEFAULT NULL,
  `SITE` varchar(60) DEFAULT NULL,
  `STARTTIME` datetime DEFAULT NULL,
  `ENDTIME` datetime DEFAULT NULL,
  `FROZENTIME` datetime DEFAULT NULL,
  `PRODSOURCELABEL` varchar(20) DEFAULT NULL,
  `WORKINGGROUP` varchar(32) DEFAULT NULL,
  `VO` varchar(16) DEFAULT NULL,
  `CORECOUNT` int(11) DEFAULT NULL,
  `TASKTYPE` varchar(64) DEFAULT NULL,
  `PROCESSINGTYPE` varchar(64) DEFAULT NULL,
  `TASKPRIORITY` int(11) DEFAULT NULL,
  `CURRENTPRIORITY` int(11) DEFAULT NULL,
  `ARCHITECTURE` varchar(256) DEFAULT NULL,
  `TRANSUSES` varchar(64) DEFAULT NULL,
  `TRANSHOME` varchar(128) DEFAULT NULL,
  `TRANSPATH` varchar(128) DEFAULT NULL,
  `LOCKEDBY` varchar(40) DEFAULT NULL,
  `LOCKEDTIME` datetime DEFAULT NULL,
  `TERMCONDITION` varchar(100) DEFAULT NULL,
  `SPLITRULE` varchar(100) DEFAULT NULL,
  `WALLTIME` int(11) DEFAULT NULL,
  `WALLTIMEUNIT` varchar(32) DEFAULT NULL,
  `OUTDISKCOUNT` int(11) DEFAULT NULL,
  `OUTDISKUNIT` varchar(32) DEFAULT NULL,
  `WORKDISKCOUNT` int(11) DEFAULT NULL,
  `WORKDISKUNIT` varchar(32) DEFAULT NULL,
  `RAMCOUNT` int(11) DEFAULT NULL,
  `RAMUNIT` varchar(32) DEFAULT NULL,
  `IOINTENSITY` int(11) DEFAULT NULL,
  `IOINTENSITYUNIT` varchar(32) DEFAULT NULL,
  `WORKQUEUE_ID` int(11) DEFAULT NULL,
  `PROGRESS` tinyint(4) DEFAULT NULL,
  `FAILURERATE` tinyint(4) DEFAULT NULL,
  `ERRORDIALOG` varchar(255) DEFAULT NULL,
  `COUNTRYGROUP` varchar(20) DEFAULT NULL,
  `PARENT_TID` bigint(20) DEFAULT NULL,
  `EVENTSERVICE` tinyint(4) DEFAULT NULL,
  `TICKETID` varchar(50) DEFAULT NULL,
  `TICKETSYSTEMTYPE` varchar(16) DEFAULT NULL,
  `STATECHANGETIME` datetime DEFAULT NULL,
  `SUPERSTATUS` varchar(64) DEFAULT NULL,
  `CAMPAIGN` varchar(32) DEFAULT NULL,
  `MERGERAMCOUNT` int(11) DEFAULT NULL,
  `MERGERAMUNIT` varchar(32) DEFAULT NULL,
  `MERGEWALLTIME` int(11) DEFAULT NULL,
  `MERGEWALLTIMEUNIT` varchar(32) DEFAULT NULL,
  `THROTTLEDTIME` datetime DEFAULT NULL,
  `NUMTHROTTLED` tinyint(4) DEFAULT NULL,
  `MERGECORECOUNT` int(11) DEFAULT NULL,
  `GOAL` tinyint DEFAULT NULL,
  `ASSESSMENTTIME` datetime, 
  `CPUTIME` int DEFAULT NULL,
  `CPUTIMEUNIT` varchar(32) DEFAULT NULL,
  `CPUEFFICIENCY` tinyint DEFAULT NULL,
  `BASEWALLTIME` int DEFAULT NULL,
  `AMIFLAG` varchar(10) DEFAULT NULL,
  PRIMARY KEY (`JEDITASKID`),
  UNIQUE KEY `JEDI_TASKS_PK` (`JEDITASKID`),
  KEY `JEDI_TASKS_LOCKEDBY_IDX` (`LOCKEDBY`),
  KEY `JEDI_TASKS_STATUS3ATTR_IDX` (`STATUS`,`WORKQUEUE_ID`,`PRODSOURCELABEL`,`JEDITASKID`),
  KEY `JEDI_TASKS_NAMETASKID_IDX` (`TASKNAME`,`JEDITASKID`),
  KEY `JEDI_UPPER_TASKS_NAME_IDX` (`TASKNAME`),
  KEY `JEDI_TASKS_WORKQUEUE_FK` (`WORKQUEUE_ID`),
  CONSTRAINT `JEDI_TASKS_WORKQUEUE_FK` FOREIGN KEY (`WORKQUEUE_ID`) REFERENCES `jedi_work_queue` (`QUEUE_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `jedi_tasks`
--

LOCK TABLES `jedi_tasks` WRITE;
/*!40000 ALTER TABLE `jedi_tasks` DISABLE KEYS */;
/*!40000 ALTER TABLE `jedi_tasks` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `jedi_work_queue`
--

DROP TABLE IF EXISTS `jedi_work_queue`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `jedi_work_queue` (
  `QUEUE_ID` int(11) NOT NULL,
  `QUEUE_NAME` varchar(16) NOT NULL,
  `QUEUE_TYPE` varchar(16) NOT NULL,
  `VO` varchar(16) NOT NULL,
  `STATUS` varchar(64) DEFAULT NULL,
  `PARTITIONID` int(11) DEFAULT NULL,
  `STRETCHABLE` tinyint(4) DEFAULT NULL,
  `QUEUE_SHARE` tinyint(4) DEFAULT NULL,
  `QUEUE_ORDER` tinyint(4) DEFAULT NULL,
  `CRITERIA` varchar(256) DEFAULT NULL,
  `VARIABLES` varchar(256) DEFAULT NULL,
  PRIMARY KEY (`QUEUE_ID`),
  UNIQUE KEY `JEDI_WORK_QUEUEID_PK` (`QUEUE_ID`),
  UNIQUE KEY `JEDI_WORK_QUEUE_NAMETYPEVO_UK` (`QUEUE_NAME`,`QUEUE_TYPE`,`VO`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `jedi_work_queue`
--

LOCK TABLES `jedi_work_queue` WRITE;
/*!40000 ALTER TABLE `jedi_work_queue` DISABLE KEYS */;
/*!40000 ALTER TABLE `jedi_work_queue` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `jedi_work_queue_id_seq`
--

DROP TABLE IF EXISTS `jedi_work_queue_id_seq`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `jedi_work_queue_id_seq` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `col` bit(1) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `jedi_work_queue_id_seq`
--

LOCK TABLES `jedi_work_queue_id_seq` WRITE;
/*!40000 ALTER TABLE `jedi_work_queue_id_seq` DISABLE KEYS */;
/*!40000 ALTER TABLE `jedi_work_queue_id_seq` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `jobclass`
--

DROP TABLE IF EXISTS `jobclass`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `jobclass` (
  `ID` int(11) NOT NULL,
  `NAME` varchar(30) NOT NULL,
  `DESCRIPTION` varchar(30) NOT NULL,
  `RIGHTS` varchar(30) DEFAULT NULL,
  `PRIORITY` int(11) DEFAULT NULL,
  `QUOTA1` bigint(20) DEFAULT NULL,
  `QUOTA7` bigint(20) DEFAULT NULL,
  `QUOTA30` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`ID`),
  UNIQUE KEY `PRIMARY_JOBCLASS` (`ID`),
  UNIQUE KEY `JOBCLASS_NAME_IDX` (`NAME`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `jobclass`
--

LOCK TABLES `jobclass` WRITE;
/*!40000 ALTER TABLE `jobclass` DISABLE KEYS */;
/*!40000 ALTER TABLE `jobclass` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `jobparamstable`
--

DROP TABLE IF EXISTS `jobparamstable`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `jobparamstable` (
  `PANDAID` bigint(20) NOT NULL,
  `MODIFICATIONTIME` datetime NOT NULL DEFAULT '1970-01-01 00:00:00',
  `JOBPARAMETERS` text,
  PRIMARY KEY (`PANDAID`,`MODIFICATIONTIME`),
  UNIQUE KEY `PART_JOBPARAMSTABLE_PK` (`PANDAID`,`MODIFICATIONTIME`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `jobparamstable`
--

LOCK TABLES `jobparamstable` WRITE;
/*!40000 ALTER TABLE `jobparamstable` DISABLE KEYS */;
/*!40000 ALTER TABLE `jobparamstable` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `jobparamstable_arch`
--

DROP TABLE IF EXISTS `jobparamstable_arch`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `jobparamstable_arch` (
  `PANDAID` bigint(20) NOT NULL,
  `MODIFICATIONTIME` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `JOBPARAMETERS` text,
  KEY `JOBPARAMS_ARCH_PANDAID_IDX` (`PANDAID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `jobparamstable_arch`
--

LOCK TABLES `jobparamstable_arch` WRITE;
/*!40000 ALTER TABLE `jobparamstable_arch` DISABLE KEYS */;
/*!40000 ALTER TABLE `jobparamstable_arch` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `jobs_statuslog`
--

DROP TABLE IF EXISTS `jobs_statuslog`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `jobs_statuslog` (
  `PANDAID` bigint(20) NOT NULL,
  `MODIFICATIONTIME` datetime NOT NULL,
  `JOBSTATUS` varchar(15) NOT NULL,
  `PRODSOURCELABEL` varchar(20) DEFAULT NULL,
  `CLOUD` varchar(50) DEFAULT NULL,
  `COMPUTINGSITE` varchar(128) DEFAULT NULL,
  `MODIFICATIONHOST` varchar(128) DEFAULT NULL,
  KEY `JOBS_STATUSLOG_PANDAID_IDX` (`PANDAID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `jobs_statuslog`
--

LOCK TABLES `jobs_statuslog` WRITE;
/*!40000 ALTER TABLE `jobs_statuslog` DISABLE KEYS */;
/*!40000 ALTER TABLE `jobs_statuslog` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `jobsactive4`
--

DROP TABLE IF EXISTS `jobsactive4`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `jobsactive4` (
  `PANDAID` bigint(20) NOT NULL DEFAULT '0',
  `JOBDEFINITIONID` bigint(20) NOT NULL DEFAULT '0',
  `SCHEDULERID` varchar(128) DEFAULT NULL,
  `PILOTID` varchar(200) DEFAULT NULL,
  `CREATIONTIME` datetime NOT NULL DEFAULT '1970-01-01 00:00:00',
  `CREATIONHOST` varchar(128) DEFAULT NULL,
  `MODIFICATIONTIME` datetime NOT NULL DEFAULT '1970-01-01 00:00:00',
  `MODIFICATIONHOST` varchar(128) DEFAULT NULL,
  `ATLASRELEASE` varchar(64) DEFAULT NULL,
  `TRANSFORMATION` varchar(250) DEFAULT NULL,
  `HOMEPACKAGE` varchar(80) DEFAULT NULL,
  `PRODSERIESLABEL` varchar(20) DEFAULT 'Rome',
  `PRODSOURCELABEL` varchar(20) DEFAULT 'managed',
  `PRODUSERID` varchar(250) DEFAULT NULL,
  `ASSIGNEDPRIORITY` int(11) NOT NULL DEFAULT '0',
  `CURRENTPRIORITY` int(11) NOT NULL DEFAULT '0',
  `ATTEMPTNR` tinyint(4) NOT NULL DEFAULT '0',
  `MAXATTEMPT` tinyint(4) NOT NULL DEFAULT '0',
  `JOBSTATUS` varchar(15) NOT NULL DEFAULT 'activated',
  `JOBNAME` varchar(256) DEFAULT NULL,
  `MAXCPUCOUNT` int(11) NOT NULL DEFAULT '0',
  `MAXCPUUNIT` varchar(32) DEFAULT NULL,
  `MAXDISKCOUNT` int(11) NOT NULL DEFAULT '0',
  `MAXDISKUNIT` char(4) DEFAULT NULL,
  `IPCONNECTIVITY` char(5) DEFAULT NULL,
  `MINRAMCOUNT` int(11) NOT NULL DEFAULT '0',
  `MINRAMUNIT` char(2) DEFAULT NULL,
  `STARTTIME` datetime DEFAULT '1970-01-01 00:00:00',
  `ENDTIME` datetime DEFAULT '1970-01-01 00:00:00',
  `CPUCONSUMPTIONTIME` bigint(20) NOT NULL DEFAULT '0',
  `CPUCONSUMPTIONUNIT` varchar(128) DEFAULT NULL,
  `COMMANDTOPILOT` varchar(250) DEFAULT NULL,
  `TRANSEXITCODE` varchar(128) DEFAULT NULL,
  `PILOTERRORCODE` int(11) NOT NULL DEFAULT '0',
  `PILOTERRORDIAG` varchar(500) DEFAULT NULL,
  `EXEERRORCODE` int(11) NOT NULL DEFAULT '0',
  `EXEERRORDIAG` varchar(500) DEFAULT NULL,
  `SUPERRORCODE` int(11) NOT NULL DEFAULT '0',
  `SUPERRORDIAG` varchar(250) DEFAULT NULL,
  `DDMERRORCODE` int(11) NOT NULL DEFAULT '0',
  `DDMERRORDIAG` varchar(500) DEFAULT NULL,
  `BROKERAGEERRORCODE` int(11) NOT NULL DEFAULT '0',
  `BROKERAGEERRORDIAG` varchar(250) DEFAULT NULL,
  `JOBDISPATCHERERRORCODE` int(11) NOT NULL DEFAULT '0',
  `JOBDISPATCHERERRORDIAG` varchar(250) DEFAULT NULL,
  `TASKBUFFERERRORCODE` int(11) NOT NULL DEFAULT '0',
  `TASKBUFFERERRORDIAG` varchar(300) DEFAULT NULL,
  `COMPUTINGSITE` varchar(128) DEFAULT NULL,
  `COMPUTINGELEMENT` varchar(128) DEFAULT NULL,
  `JOBPARAMETERS` text,
  `METADATA` text,
  `PRODDBLOCK` varchar(255) DEFAULT NULL,
  `DISPATCHDBLOCK` varchar(255) DEFAULT NULL,
  `DESTINATIONDBLOCK` varchar(255) DEFAULT NULL,
  `DESTINATIONSE` varchar(250) DEFAULT NULL,
  `NEVENTS` int(11) NOT NULL DEFAULT '0',
  `GRID` varchar(50) DEFAULT NULL,
  `CLOUD` varchar(50) DEFAULT NULL,
  `CPUCONVERSION` decimal(9,4) DEFAULT NULL,
  `SOURCESITE` varchar(36) DEFAULT NULL,
  `DESTINATIONSITE` varchar(36) DEFAULT NULL,
  `TRANSFERTYPE` varchar(10) DEFAULT NULL,
  `TASKID` int(11) DEFAULT NULL,
  `CMTCONFIG` varchar(250) DEFAULT NULL,
  `STATECHANGETIME` datetime DEFAULT '1970-01-01 00:00:00',
  `PRODDBUPDATETIME` datetime DEFAULT '1970-01-01 00:00:00',
  `LOCKEDBY` varchar(128) DEFAULT NULL,
  `RELOCATIONFLAG` tinyint(4) DEFAULT '0',
  `JOBEXECUTIONID` bigint(20) DEFAULT '0',
  `VO` varchar(16) DEFAULT NULL,
  `PILOTTIMING` varchar(100) DEFAULT NULL,
  `WORKINGGROUP` varchar(20) DEFAULT NULL,
  `PROCESSINGTYPE` varchar(64) DEFAULT NULL,
  `PRODUSERNAME` varchar(60) DEFAULT NULL,
  `NINPUTFILES` int(11) DEFAULT NULL,
  `COUNTRYGROUP` varchar(20) DEFAULT NULL,
  `BATCHID` varchar(80) DEFAULT NULL,
  `PARENTID` bigint(20) DEFAULT NULL,
  `SPECIALHANDLING` varchar(80) DEFAULT NULL,
  `JOBSETID` bigint(20) DEFAULT NULL,
  `CORECOUNT` tinyint(4) DEFAULT NULL,
  `NINPUTDATAFILES` int(11) DEFAULT NULL,
  `INPUTFILETYPE` varchar(32) DEFAULT NULL,
  `INPUTFILEPROJECT` varchar(64) DEFAULT NULL,
  `INPUTFILEBYTES` bigint(20) DEFAULT NULL,
  `NOUTPUTDATAFILES` int(11) DEFAULT NULL,
  `OUTPUTFILEBYTES` bigint(20) DEFAULT NULL,
  `JOBMETRICS` varchar(500) DEFAULT NULL,
  `WORKQUEUE_ID` int(11) DEFAULT NULL,
  `JEDITASKID` bigint(20) DEFAULT NULL,
  `JOBSUBSTATUS` varchar(80) DEFAULT NULL,
  `ACTUALCORECOUNT` int(11) DEFAULT NULL,
  `REQID` int(11) DEFAULT NULL,
  `MAXRSS` int(11) DEFAULT NULL, 
  `MAXVMEM` int(11) DEFAULT NULL,  
  `MAXSWAP` int(11) DEFAULT NULL,
  `MAXPSS` int(11) DEFAULT NULL,
  `AVGRSS` int(11) DEFAULT NULL,
  `AVGMEM` int(11) DEFAULT NULL,
  `AVGSWAP` int(11) DEFAULT NULL,
  `AVGPSS` int(11) DEFAULT NULL,
  `MAXWALLTIME` int(11) DEFAULT NULL,
  `NUCLEUS` varchar(52) DEFAULT NULL,
  `EVENTSERVICE` tinyint(1) DEFAULT NULL,
  PRIMARY KEY (`PANDAID`),
  UNIQUE KEY `JOBSACTIVE4_PANDAID_PK` (`PANDAID`),
  UNIQUE KEY `JOBSACTIVE4_JEDITASKID_IDX` (`JEDITASKID`,`PANDAID`),
  KEY `JOBSACTIVE4_PRIOR_IDX` (`CURRENTPRIORITY`,`PANDAID`),
  KEY `JOBSACTIVE4_MODTIME_IDX` (`MODIFICATIONTIME`),
  KEY `JOBSACTIVE4_CSITE_LABEL_PRIOR3` (`COMPUTINGSITE`,`PRODSOURCELABEL`,`CURRENTPRIORITY`,`JOBSTATUS`,`MAXDISKCOUNT`,`COMMANDTOPILOT`),
  KEY `JOBSACTIVE4_JOBDEFID_IDX` (`JOBDEFINITIONID`),
  KEY `JOBSACTIVE4_PRODDBLOCK_ST_IDX` (`PRODDBLOCK`,`JOBSTATUS`),
  KEY `JOBSACTIVE4_COMPSITESTATUS_IDX` (`COMPUTINGSITE`,`JOBSTATUS`),
  KEY `JOBSACTIVE4_PRODUSERNAMEST_IDX` (`PRODUSERNAME`,`JOBSTATUS`),
  KEY `JOBSACTIVE4_WORKQUEUE_IDX` (`WORKQUEUE_ID`,`CLOUD`,`JOBSTATUS`,`PRODSOURCELABEL`,`CURRENTPRIORITY`),
  KEY `JOBSACTIVE4_STAT_LABEL_WGR_IDX` (`JOBSTATUS`,`PRODSOURCELABEL`,`WORKINGGROUP`),
  KEY `JOBSACTIVE4_REQID_IDX` (`REQID`),
  CONSTRAINT `JOBSACTIVE4_JEDITASKID_FK` FOREIGN KEY (`JEDITASKID`) REFERENCES `jedi_tasks` (`JEDITASKID`),
  CONSTRAINT `JOBSACTIVE4_WORKQUEUE_ID_FK` FOREIGN KEY (`WORKQUEUE_ID`) REFERENCES `jedi_work_queue` (`QUEUE_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `jobsactive4`
--

LOCK TABLES `jobsactive4` WRITE;
/*!40000 ALTER TABLE `jobsactive4` DISABLE KEYS */;
/*!40000 ALTER TABLE `jobsactive4` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `jobsarchived`
--

DROP TABLE IF EXISTS `jobsarchived`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `jobsarchived` (
  `PANDAID` bigint(20) NOT NULL DEFAULT '0',
  `JOBDEFINITIONID` bigint(20) NOT NULL DEFAULT '0',
  `SCHEDULERID` varchar(128) DEFAULT NULL,
  `PILOTID` varchar(200) DEFAULT NULL,
  `CREATIONTIME` datetime DEFAULT NULL,
  `CREATIONHOST` varchar(128) DEFAULT NULL,
  `MODIFICATIONTIME` datetime DEFAULT NULL,
  `MODIFICATIONHOST` varchar(128) DEFAULT NULL,
  `ATLASRELEASE` varchar(64) DEFAULT NULL,
  `TRANSFORMATION` varchar(250) DEFAULT NULL,
  `HOMEPACKAGE` varchar(80) DEFAULT NULL,
  `PRODSERIESLABEL` varchar(20) DEFAULT NULL,
  `PRODSOURCELABEL` varchar(20) DEFAULT NULL,
  `PRODUSERID` varchar(250) DEFAULT NULL,
  `ASSIGNEDPRIORITY` int(11) NOT NULL DEFAULT '0',
  `CURRENTPRIORITY` int(11) NOT NULL DEFAULT '0',
  `ATTEMPTNR` tinyint(4) NOT NULL DEFAULT '0',
  `MAXATTEMPT` tinyint(4) NOT NULL DEFAULT '0',
  `JOBSTATUS` varchar(15) NOT NULL DEFAULT 'unknown',
  `JOBNAME` varchar(256) DEFAULT NULL,
  `MAXCPUCOUNT` int(11) NOT NULL DEFAULT '0',
  `MAXCPUUNIT` varchar(32) DEFAULT NULL,
  `MAXDISKCOUNT` int(11) NOT NULL DEFAULT '0',
  `MAXDISKUNIT` char(4) DEFAULT NULL,
  `IPCONNECTIVITY` char(5) DEFAULT NULL,
  `MINRAMCOUNT` int(11) NOT NULL DEFAULT '0',
  `MINRAMUNIT` char(4) DEFAULT NULL,
  `STARTTIME` datetime DEFAULT NULL,
  `ENDTIME` datetime DEFAULT NULL,
  `CPUCONSUMPTIONTIME` bigint(20) DEFAULT NULL,
  `CPUCONSUMPTIONUNIT` varchar(128) DEFAULT NULL,
  `COMMANDTOPILOT` varchar(250) DEFAULT NULL,
  `TRANSEXITCODE` varchar(128) DEFAULT NULL,
  `PILOTERRORCODE` int(11) NOT NULL DEFAULT '0',
  `PILOTERRORDIAG` varchar(500) DEFAULT NULL,
  `EXEERRORCODE` int(11) NOT NULL DEFAULT '0',
  `EXEERRORDIAG` varchar(500) DEFAULT NULL,
  `SUPERRORCODE` int(11) NOT NULL DEFAULT '0',
  `SUPERRORDIAG` varchar(250) DEFAULT NULL,
  `DDMERRORCODE` int(11) NOT NULL DEFAULT '0',
  `DDMERRORDIAG` varchar(700) DEFAULT NULL,
  `BROKERAGEERRORCODE` int(11) NOT NULL DEFAULT '0',
  `BROKERAGEERRORDIAG` varchar(250) DEFAULT NULL,
  `JOBDISPATCHERERRORCODE` int(11) NOT NULL DEFAULT '0',
  `JOBDISPATCHERERRORDIAG` varchar(250) DEFAULT NULL,
  `TASKBUFFERERRORCODE` int(11) NOT NULL DEFAULT '0',
  `TASKBUFFERERRORDIAG` varchar(300) DEFAULT NULL,
  `COMPUTINGSITE` varchar(128) DEFAULT NULL,
  `COMPUTINGELEMENT` varchar(128) DEFAULT NULL,
  `PRODDBLOCK` varchar(255) DEFAULT NULL,
  `DISPATCHDBLOCK` varchar(255) DEFAULT NULL,
  `DESTINATIONDBLOCK` varchar(255) DEFAULT NULL,
  `DESTINATIONSE` varchar(250) DEFAULT NULL,
  `NEVENTS` int(11) DEFAULT NULL,
  `GRID` varchar(50) DEFAULT NULL,
  `CLOUD` varchar(50) DEFAULT NULL,
  `CPUCONVERSION` decimal(9,4) DEFAULT NULL,
  `SOURCESITE` varchar(36) DEFAULT NULL,
  `DESTINATIONSITE` varchar(36) DEFAULT NULL,
  `TRANSFERTYPE` varchar(10) DEFAULT NULL,
  `TASKID` int(11) DEFAULT NULL,
  `CMTCONFIG` varchar(250) DEFAULT NULL,
  `STATECHANGETIME` datetime DEFAULT NULL,
  `PRODDBUPDATETIME` datetime DEFAULT NULL,
  `LOCKEDBY` varchar(128) DEFAULT NULL,
  `RELOCATIONFLAG` tinyint(4) DEFAULT '0',
  `JOBEXECUTIONID` bigint(20) DEFAULT '0',
  `VO` varchar(16) DEFAULT NULL,
  `PILOTTIMING` varchar(100) DEFAULT NULL,
  `WORKINGGROUP` varchar(20) DEFAULT NULL,
  `PROCESSINGTYPE` varchar(64) DEFAULT NULL,
  `JOBPARAMETERS` text,
  `METADATA` text,
  `PRODUSERNAME` varchar(60) DEFAULT NULL,
  `NINPUTFILES` int(11) DEFAULT NULL,
  `COUNTRYGROUP` varchar(20) DEFAULT NULL,
  `BATCHID` varchar(80) DEFAULT NULL,
  `PARENTID` bigint(20) DEFAULT NULL,
  `SPECIALHANDLING` varchar(80) DEFAULT NULL,
  `JOBSETID` bigint(20) DEFAULT NULL,
  `CORECOUNT` tinyint(4) DEFAULT NULL,
  `NINPUTDATAFILES` int(11) DEFAULT NULL,
  `INPUTFILETYPE` varchar(32) DEFAULT NULL,
  `INPUTFILEPROJECT` varchar(64) DEFAULT NULL,
  `INPUTFILEBYTES` bigint(20) DEFAULT NULL,
  `NOUTPUTDATAFILES` int(11) DEFAULT NULL,
  `OUTPUTFILEBYTES` bigint(20) DEFAULT NULL,
  `JOBMETRICS` varchar(500) DEFAULT NULL,
  `WORKQUEUE_ID` int(11) DEFAULT NULL,
  `JEDITASKID` bigint(20) DEFAULT NULL,
  `JOBSUBSTATUS` varchar(80) DEFAULT NULL,
  `ACTUALCORECOUNT` int(11) DEFAULT NULL,
  `REQID` int(11) DEFAULT NULL,
  `MAXRSS` int(11) DEFAULT NULL, 
  `MAXVMEM` int(11) DEFAULT NULL,  
  `MAXSWAP` int(11) DEFAULT NULL,
  `MAXPSS` int(11) DEFAULT NULL,
  `AVGRSS` int(11) DEFAULT NULL,
  `AVGMEM` int(11) DEFAULT NULL,
  `AVGSWAP` int(11) DEFAULT NULL,
  `AVGPSS` int(11) DEFAULT NULL,
  `MAXWALLTIME` int(11) DEFAULT NULL,
  `NUCLEUS` varchar(52) DEFAULT NULL,
  `EVENTSERVICE` tinyint(1) DEFAULT NULL,
  KEY `JOBS_UPPER_PRODUSERNAME_IDX` (`PRODUSERNAME`),
  KEY `JOBSARCHIVED_REQID_IDX` (`REQID`),
  KEY `JOBS_JEDITASKID_PANDAID_IDX` (`JEDITASKID`,`PANDAID`),
  KEY `JOBS_SPECIALHANDLING_IDX` (`SPECIALHANDLING`),
  KEY `JOBS_BATCHID_IDX` (`BATCHID`),
  KEY `JOBS_PANDAID_IDX` (`PANDAID`),
  KEY `JOBS_TASKID_3ATTR_PANDAID_IDX` (`TASKID`,`PRODSOURCELABEL`,`JOBSTATUS`,`PROCESSINGTYPE`,`PANDAID`),
  KEY `JOBS_COMSITE_3ATTR_PANDAID_IDX` (`COMPUTINGSITE`,`JOBSTATUS`,`PRODSOURCELABEL`,`PROCESSINGTYPE`,`PANDAID`),
  KEY `JOBS_PRODUSERNAME_4ATTR_IDX` (`PRODUSERNAME`,`JOBSTATUS`,`PRODSOURCELABEL`,`JOBSETID`,`JOBDEFINITIONID`),
  KEY `JOBS_JOBSETID_PRODUSERNAME_IDX` (`JOBSETID`,`PRODUSERNAME`),
  KEY `JOBS_JOBDEFID_PRODUSERNAME_IDX` (`JOBDEFINITIONID`,`PRODUSERNAME`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `jobsarchived`
--

LOCK TABLES `jobsarchived` WRITE;
/*!40000 ALTER TABLE `jobsarchived` DISABLE KEYS */;
/*!40000 ALTER TABLE `jobsarchived` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `jobsarchived4`
--

DROP TABLE IF EXISTS `jobsarchived4`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `jobsarchived4` (
  `PANDAID` bigint(20) NOT NULL DEFAULT '0',
  `MODIFICATIONTIME` datetime NOT NULL,
  `JOBDEFINITIONID` bigint(20) NOT NULL DEFAULT '0',
  `SCHEDULERID` varchar(128) DEFAULT NULL,
  `PILOTID` varchar(200) DEFAULT NULL,
  `CREATIONTIME` datetime NOT NULL DEFAULT '1970-01-01 00:00:00',
  `CREATIONHOST` varchar(128) DEFAULT NULL,
  `MODIFICATIONHOST` varchar(128) DEFAULT NULL,
  `ATLASRELEASE` varchar(64) DEFAULT NULL,
  `TRANSFORMATION` varchar(250) DEFAULT NULL,
  `HOMEPACKAGE` varchar(80) DEFAULT NULL,
  `PRODSERIESLABEL` varchar(20) DEFAULT 'Rome',
  `PRODSOURCELABEL` varchar(20) DEFAULT 'managed',
  `PRODUSERID` varchar(250) DEFAULT NULL,
  `ASSIGNEDPRIORITY` int(11) NOT NULL DEFAULT '0',
  `CURRENTPRIORITY` int(11) NOT NULL DEFAULT '0',
  `ATTEMPTNR` tinyint(4) NOT NULL DEFAULT '0',
  `MAXATTEMPT` tinyint(4) NOT NULL DEFAULT '0',
  `JOBSTATUS` varchar(15) NOT NULL DEFAULT 'activated',
  `JOBNAME` varchar(256) DEFAULT NULL,
  `MAXCPUCOUNT` int(11) NOT NULL DEFAULT '0',
  `MAXCPUUNIT` varchar(32) DEFAULT NULL,
  `MAXDISKCOUNT` int(11) NOT NULL DEFAULT '0',
  `MAXDISKUNIT` char(4) DEFAULT NULL,
  `IPCONNECTIVITY` char(5) DEFAULT NULL,
  `MINRAMCOUNT` int(11) NOT NULL DEFAULT '0',
  `MINRAMUNIT` char(2) DEFAULT NULL,
  `STARTTIME` datetime DEFAULT '1970-01-01 00:00:00',
  `ENDTIME` datetime DEFAULT '1970-01-01 00:00:00',
  `CPUCONSUMPTIONTIME` bigint(20) NOT NULL DEFAULT '0',
  `CPUCONSUMPTIONUNIT` varchar(128) DEFAULT NULL,
  `COMMANDTOPILOT` varchar(250) DEFAULT NULL,
  `TRANSEXITCODE` varchar(128) DEFAULT NULL,
  `PILOTERRORCODE` int(11) NOT NULL DEFAULT '0',
  `PILOTERRORDIAG` varchar(500) DEFAULT NULL,
  `EXEERRORCODE` int(11) NOT NULL DEFAULT '0',
  `EXEERRORDIAG` varchar(500) DEFAULT NULL,
  `SUPERRORCODE` int(11) NOT NULL DEFAULT '0',
  `SUPERRORDIAG` varchar(250) DEFAULT NULL,
  `DDMERRORCODE` int(11) NOT NULL DEFAULT '0',
  `DDMERRORDIAG` varchar(700) DEFAULT NULL,
  `BROKERAGEERRORCODE` int(11) NOT NULL DEFAULT '0',
  `BROKERAGEERRORDIAG` varchar(250) DEFAULT NULL,
  `JOBDISPATCHERERRORCODE` int(11) NOT NULL DEFAULT '0',
  `JOBDISPATCHERERRORDIAG` varchar(250) DEFAULT NULL,
  `TASKBUFFERERRORCODE` int(11) NOT NULL DEFAULT '0',
  `TASKBUFFERERRORDIAG` varchar(300) DEFAULT NULL,
  `COMPUTINGSITE` varchar(128) DEFAULT NULL,
  `COMPUTINGELEMENT` varchar(128) DEFAULT NULL,
  `PRODDBLOCK` varchar(255) DEFAULT NULL,
  `DISPATCHDBLOCK` varchar(255) DEFAULT NULL,
  `DESTINATIONDBLOCK` varchar(255) DEFAULT NULL,
  `DESTINATIONSE` varchar(250) DEFAULT NULL,
  `NEVENTS` int(11) NOT NULL DEFAULT '0',
  `GRID` varchar(50) DEFAULT NULL,
  `CLOUD` varchar(50) DEFAULT NULL,
  `CPUCONVERSION` decimal(9,4) DEFAULT NULL,
  `SOURCESITE` varchar(36) DEFAULT NULL,
  `DESTINATIONSITE` varchar(36) DEFAULT NULL,
  `TRANSFERTYPE` varchar(10) DEFAULT NULL,
  `TASKID` int(11) DEFAULT NULL,
  `CMTCONFIG` varchar(250) DEFAULT NULL,
  `STATECHANGETIME` datetime DEFAULT '1970-01-01 00:00:00',
  `PRODDBUPDATETIME` datetime DEFAULT '1970-01-01 00:00:00',
  `LOCKEDBY` varchar(128) DEFAULT NULL,
  `RELOCATIONFLAG` tinyint(4) DEFAULT '0',
  `JOBEXECUTIONID` bigint(20) DEFAULT '0',
  `VO` varchar(16) DEFAULT NULL,
  `PILOTTIMING` varchar(100) DEFAULT NULL,
  `WORKINGGROUP` varchar(20) DEFAULT NULL,
  `PROCESSINGTYPE` varchar(64) DEFAULT NULL,
  `PRODUSERNAME` varchar(60) DEFAULT NULL,
  `NINPUTFILES` int(11) DEFAULT NULL,
  `COUNTRYGROUP` varchar(20) DEFAULT NULL,
  `BATCHID` varchar(80) DEFAULT NULL,
  `PARENTID` bigint(20) DEFAULT NULL,
  `SPECIALHANDLING` varchar(80) DEFAULT NULL,
  `JOBSETID` bigint(20) DEFAULT NULL,
  `CORECOUNT` tinyint(4) DEFAULT NULL,
  `JOBPARAMETERS` text,
  `METADATA` text,
  `NINPUTDATAFILES` int(11) DEFAULT NULL,
  `INPUTFILETYPE` varchar(32) DEFAULT NULL,
  `INPUTFILEPROJECT` varchar(64) DEFAULT NULL,
  `INPUTFILEBYTES` bigint(20) DEFAULT NULL,
  `NOUTPUTDATAFILES` int(11) DEFAULT NULL,
  `OUTPUTFILEBYTES` bigint(20) DEFAULT NULL,
  `JOBMETRICS` varchar(500) DEFAULT NULL,
  `WORKQUEUE_ID` int(11) DEFAULT NULL,
  `JEDITASKID` bigint(20) DEFAULT NULL,
  `JOBSUBSTATUS` varchar(80) DEFAULT NULL,
  `ACTUALCORECOUNT` int(11) DEFAULT NULL,
  `REQID` int(11) DEFAULT NULL,
  `MAXRSS` int(11) DEFAULT NULL, 
  `MAXVMEM` int(11) DEFAULT NULL,  
  `MAXSWAP` int(11) DEFAULT NULL,
  `MAXPSS` int(11) DEFAULT NULL,
  `AVGRSS` int(11) DEFAULT NULL,
  `AVGMEM` int(11) DEFAULT NULL,
  `AVGSWAP` int(11) DEFAULT NULL,
  `AVGPSS` int(11) DEFAULT NULL,
  `MAXWALLTIME` int(11) DEFAULT NULL,
  PRIMARY KEY (`PANDAID`,`MODIFICATIONTIME`),
  UNIQUE KEY `PART_JOBSARCHIVED4_PK` (`PANDAID`,`MODIFICATIONTIME`),
  KEY `JOBSARCHIVED4_BATCHID_IDX` (`BATCHID`),
  KEY `JOBSARCH4_SPECIALHANDLING_IDX` (`SPECIALHANDLING`),
  KEY `JOBS_UPPER_PRODUSERNAME_IDX` (`PRODUSERNAME`),
  KEY `JOBSARCHIVED4_REQID_IDX` (`REQID`),
  KEY `JOBSARCH4_PILOTERRCODE_IDX` (`PILOTERRORCODE`),
  KEY `JOBSARCH4_PRODUSERNAMEST_IDX` (`PRODUSERNAME`,`JOBSTATUS`),
  KEY `JOBSARCH4_MTIMEPRODSLABEL_IDX` (`MODIFICATIONTIME`,`PRODSOURCELABEL`),
  KEY `JOBSARCH4_COMPSITE_3ATTR_IDX` (`COMPUTINGSITE`,`JOBSTATUS`,`PRODSOURCELABEL`,`PROCESSINGTYPE`),
  KEY `JOBSARCH4_TASKID_3ATTR_IDX` (`TASKID`,`PRODSOURCELABEL`,`JOBSTATUS`,`PROCESSINGTYPE`),
  KEY `JOBSARCHIVED4_WORKQUEUE_IDX` (`WORKQUEUE_ID`,`CLOUD`,`JOBSTATUS`,`PRODSOURCELABEL`,`CURRENTPRIORITY`),
  KEY `JOBSARCHIVED4_JEDITASKID_IDX` (`JEDITASKID`,`PANDAID`),
  KEY `JOBSARCHIVED4_JOBDEFID_IDX` (`JOBDEFINITIONID`),
  KEY `JOBSARCHIVED4_JOBSETID_IDX` (`JOBSETID`),
  KEY `JOBS_DESTINATIONDBLOCK_IDX` (`DESTINATIONDBLOCK`),
  CONSTRAINT `JOBSARCHIVED4_JEDITASKID_FK` FOREIGN KEY (`JEDITASKID`) REFERENCES `jedi_tasks` (`JEDITASKID`),
  CONSTRAINT `JOBSARCHIVED4_WORKQUEUE_ID_FK` FOREIGN KEY (`WORKQUEUE_ID`) REFERENCES `jedi_work_queue` (`QUEUE_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `jobsarchived4`
--

LOCK TABLES `jobsarchived4` WRITE;
/*!40000 ALTER TABLE `jobsarchived4` DISABLE KEYS */;
/*!40000 ALTER TABLE `jobsarchived4` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `jobsdebug`
--

DROP TABLE IF EXISTS `jobsdebug`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `jobsdebug` (
  `PANDAID` bigint(20) NOT NULL,
  `STDOUT` varchar(2048) DEFAULT NULL,
  PRIMARY KEY (`PANDAID`),
  CONSTRAINT `JOBSDEBUG_PANDAID_FK` FOREIGN KEY (`PANDAID`) REFERENCES `jobsactive4` (`PANDAID`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `jobsdebug`
--

LOCK TABLES `jobsdebug` WRITE;
/*!40000 ALTER TABLE `jobsdebug` DISABLE KEYS */;
/*!40000 ALTER TABLE `jobsdebug` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `jobsdefined4`
--

DROP TABLE IF EXISTS `jobsdefined4`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `jobsdefined4` (
  `PANDAID` bigint(20) NOT NULL AUTO_INCREMENT,
  `JOBDEFINITIONID` bigint(20) NOT NULL DEFAULT '0',
  `SCHEDULERID` varchar(128) DEFAULT NULL,
  `PILOTID` varchar(200) DEFAULT NULL,
  `CREATIONTIME` datetime NOT NULL DEFAULT '1970-01-01 00:00:00',
  `CREATIONHOST` varchar(128) DEFAULT NULL,
  `MODIFICATIONTIME` datetime NOT NULL DEFAULT '1970-01-01 00:00:00',
  `MODIFICATIONHOST` varchar(128) DEFAULT NULL,
  `ATLASRELEASE` varchar(64) DEFAULT NULL,
  `TRANSFORMATION` varchar(250) DEFAULT NULL,
  `HOMEPACKAGE` varchar(80) DEFAULT NULL,
  `PRODSERIESLABEL` varchar(20) DEFAULT 'Rome',
  `PRODSOURCELABEL` varchar(20) DEFAULT 'managed',
  `PRODUSERID` varchar(250) DEFAULT NULL,
  `ASSIGNEDPRIORITY` int(11) NOT NULL DEFAULT '0',
  `CURRENTPRIORITY` int(11) NOT NULL DEFAULT '0',
  `ATTEMPTNR` tinyint(4) NOT NULL DEFAULT '0',
  `MAXATTEMPT` tinyint(4) NOT NULL DEFAULT '0',
  `JOBSTATUS` varchar(15) NOT NULL DEFAULT 'defined',
  `JOBNAME` varchar(256) DEFAULT NULL,
  `MAXCPUCOUNT` int(11) NOT NULL DEFAULT '0',
  `MAXCPUUNIT` varchar(32) DEFAULT NULL,
  `MAXDISKCOUNT` int(11) NOT NULL DEFAULT '0',
  `MAXDISKUNIT` char(4) DEFAULT NULL,
  `IPCONNECTIVITY` char(5) DEFAULT NULL,
  `MINRAMCOUNT` int(11) NOT NULL DEFAULT '0',
  `MINRAMUNIT` char(2) DEFAULT NULL,
  `STARTTIME` datetime DEFAULT '1970-01-01 00:00:00',
  `ENDTIME` datetime DEFAULT '1970-01-01 00:00:00',
  `CPUCONSUMPTIONTIME` bigint(20) NOT NULL DEFAULT '0',
  `CPUCONSUMPTIONUNIT` varchar(128) DEFAULT NULL,
  `COMMANDTOPILOT` varchar(250) DEFAULT NULL,
  `TRANSEXITCODE` varchar(128) DEFAULT NULL,
  `PILOTERRORCODE` int(11) NOT NULL DEFAULT '0',
  `PILOTERRORDIAG` varchar(250) DEFAULT NULL,
  `EXEERRORCODE` int(11) NOT NULL DEFAULT '0',
  `EXEERRORDIAG` varchar(250) DEFAULT NULL,
  `SUPERRORCODE` int(11) NOT NULL DEFAULT '0',
  `SUPERRORDIAG` varchar(250) DEFAULT NULL,
  `DDMERRORCODE` int(11) NOT NULL DEFAULT '0',
  `DDMERRORDIAG` varchar(250) DEFAULT NULL,
  `BROKERAGEERRORCODE` int(11) NOT NULL DEFAULT '0',
  `BROKERAGEERRORDIAG` varchar(250) DEFAULT NULL,
  `JOBDISPATCHERERRORCODE` int(11) NOT NULL DEFAULT '0',
  `JOBDISPATCHERERRORDIAG` varchar(250) DEFAULT NULL,
  `TASKBUFFERERRORCODE` int(11) NOT NULL DEFAULT '0',
  `TASKBUFFERERRORDIAG` varchar(250) DEFAULT NULL,
  `COMPUTINGSITE` varchar(128) DEFAULT NULL,
  `COMPUTINGELEMENT` varchar(128) DEFAULT NULL,
  `JOBPARAMETERS` text,
  `METADATA` text,
  `PRODDBLOCK` varchar(255) DEFAULT NULL,
  `DISPATCHDBLOCK` varchar(255) DEFAULT NULL,
  `DESTINATIONDBLOCK` varchar(255) DEFAULT NULL,
  `DESTINATIONSE` varchar(250) DEFAULT NULL,
  `NEVENTS` int(11) NOT NULL DEFAULT '0',
  `GRID` varchar(32) DEFAULT NULL,
  `CLOUD` varchar(32) DEFAULT NULL,
  `CPUCONVERSION` decimal(9,4) DEFAULT NULL,
  `SOURCESITE` varchar(36) DEFAULT NULL,
  `DESTINATIONSITE` varchar(36) DEFAULT NULL,
  `TRANSFERTYPE` varchar(10) DEFAULT NULL,
  `TASKID` int(11) DEFAULT NULL,
  `CMTCONFIG` varchar(250) DEFAULT NULL,
  `STATECHANGETIME` datetime DEFAULT '1970-01-01 00:00:00',
  `PRODDBUPDATETIME` datetime DEFAULT '1970-01-01 00:00:00',
  `LOCKEDBY` varchar(128) DEFAULT NULL,
  `RELOCATIONFLAG` tinyint(4) DEFAULT '0',
  `JOBEXECUTIONID` bigint(20) DEFAULT '0',
  `VO` varchar(16) DEFAULT NULL,
  `PILOTTIMING` varchar(100) DEFAULT NULL,
  `WORKINGGROUP` varchar(20) DEFAULT NULL,
  `PROCESSINGTYPE` varchar(64) DEFAULT NULL,
  `PRODUSERNAME` varchar(60) DEFAULT NULL,
  `NINPUTFILES` int(11) DEFAULT NULL,
  `COUNTRYGROUP` varchar(20) DEFAULT NULL,
  `BATCHID` varchar(80) DEFAULT NULL,
  `PARENTID` bigint(20) DEFAULT NULL,
  `SPECIALHANDLING` varchar(80) DEFAULT NULL,
  `JOBSETID` bigint(20) DEFAULT NULL,
  `CORECOUNT` tinyint(4) DEFAULT NULL,
  `NINPUTDATAFILES` int(11) DEFAULT NULL,
  `INPUTFILETYPE` varchar(32) DEFAULT NULL,
  `INPUTFILEPROJECT` varchar(64) DEFAULT NULL,
  `INPUTFILEBYTES` bigint(20) DEFAULT NULL,
  `NOUTPUTDATAFILES` int(11) DEFAULT NULL,
  `OUTPUTFILEBYTES` bigint(20) DEFAULT NULL,
  `JOBMETRICS` varchar(500) DEFAULT NULL,
  `WORKQUEUE_ID` int(11) DEFAULT NULL,
  `JEDITASKID` bigint(20) DEFAULT NULL,
  `JOBSUBSTATUS` varchar(80) DEFAULT NULL,
  `ACTUALCORECOUNT` int(11) DEFAULT NULL,
  `REQID` int(11) DEFAULT NULL,
  `MAXRSS` int(11) DEFAULT NULL, 
  `MAXVMEM` int(11) DEFAULT NULL,  
  `MAXSWAP` int(11) DEFAULT NULL,
  `MAXPSS` int(11) DEFAULT NULL,
  `AVGRSS` int(11) DEFAULT NULL,
  `AVGMEM` int(11) DEFAULT NULL,
  `AVGSWAP` int(11) DEFAULT NULL,
  `AVGPSS` int(11) DEFAULT NULL,
  `MAXWALLTIME` int(11) DEFAULT NULL,
  `NUCLEUS` varchar(52) DEFAULT NULL,
  `EVENTSERVICE` tinyint(1) DEFAULT NULL,
  PRIMARY KEY (`PANDAID`),
  UNIQUE KEY `JOBSDEFINED4_PANDAID_PK` (`PANDAID`),
  UNIQUE KEY `JOBSDEFINED4_JEDITASKID_IDX` (`JEDITASKID`,`PANDAID`),
  KEY `JOBSDEFINED4_JOBSETID_IDX` (`JOBSETID`),
  KEY `JOBSDEFINED4_REQID_IDX` (`REQID`),
  KEY `JOBSDEFINED4_LABEL_CSITE_STAT` (`PRODSOURCELABEL`,`COMPUTINGSITE`,`JOBSTATUS`),
  KEY `JOBSDEFINED4_PRODUSERNAME_IDX` (`PRODUSERNAME`),
  KEY `JOBSDEFINED4_WORKQUEUE_IDX` (`WORKQUEUE_ID`,`CLOUD`,`JOBSTATUS`,`PRODSOURCELABEL`,`CURRENTPRIORITY`),
  CONSTRAINT `JOBSDEFINED4_JEDITASKID_FK` FOREIGN KEY (`JEDITASKID`) REFERENCES `jedi_tasks` (`JEDITASKID`),
  CONSTRAINT `JOBSDEFINED4_WORKQUEUE_ID_FK` FOREIGN KEY (`WORKQUEUE_ID`) REFERENCES `jedi_work_queue` (`QUEUE_ID`)
) ENGINE=InnoDB AUTO_INCREMENT=79 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `jobsdefined4`
--

LOCK TABLES `jobsdefined4` WRITE;
/*!40000 ALTER TABLE `jobsdefined4` DISABLE KEYS */;
/*!40000 ALTER TABLE `jobsdefined4` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `jobswaiting4`
--

DROP TABLE IF EXISTS `jobswaiting4`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `jobswaiting4` (
  `PANDAID` bigint(20) NOT NULL DEFAULT '0',
  `JOBDEFINITIONID` bigint(20) NOT NULL DEFAULT '0',
  `SCHEDULERID` varchar(128) DEFAULT NULL,
  `PILOTID` varchar(200) DEFAULT NULL,
  `CREATIONTIME` datetime NOT NULL DEFAULT '1970-01-01 00:00:00',
  `CREATIONHOST` varchar(128) DEFAULT NULL,
  `MODIFICATIONTIME` datetime NOT NULL DEFAULT '1970-01-01 00:00:00',
  `MODIFICATIONHOST` varchar(128) DEFAULT NULL,
  `ATLASRELEASE` varchar(64) DEFAULT NULL,
  `TRANSFORMATION` varchar(250) DEFAULT NULL,
  `HOMEPACKAGE` varchar(80) DEFAULT NULL,
  `PRODSERIESLABEL` varchar(20) DEFAULT 'Rome',
  `PRODSOURCELABEL` varchar(20) DEFAULT 'managed',
  `PRODUSERID` varchar(250) DEFAULT NULL,
  `ASSIGNEDPRIORITY` int(11) NOT NULL DEFAULT '0',
  `CURRENTPRIORITY` int(11) NOT NULL DEFAULT '0',
  `ATTEMPTNR` tinyint(4) NOT NULL DEFAULT '0',
  `MAXATTEMPT` tinyint(4) NOT NULL DEFAULT '0',
  `JOBSTATUS` varchar(15) NOT NULL DEFAULT 'waiting',
  `JOBNAME` varchar(256) DEFAULT NULL,
  `MAXCPUCOUNT` int(11) NOT NULL DEFAULT '0',
  `MAXCPUUNIT` varchar(32) DEFAULT NULL,
  `MAXDISKCOUNT` int(11) NOT NULL DEFAULT '0',
  `MAXDISKUNIT` char(4) DEFAULT NULL,
  `IPCONNECTIVITY` char(5) DEFAULT NULL,
  `MINRAMCOUNT` int(11) NOT NULL DEFAULT '0',
  `MINRAMUNIT` char(2) DEFAULT NULL,
  `STARTTIME` datetime DEFAULT '1970-01-01 00:00:00',
  `ENDTIME` datetime DEFAULT '1970-01-01 00:00:00',
  `CPUCONSUMPTIONTIME` bigint(20) NOT NULL DEFAULT '0',
  `CPUCONSUMPTIONUNIT` varchar(128) DEFAULT NULL,
  `COMMANDTOPILOT` varchar(250) DEFAULT NULL,
  `TRANSEXITCODE` varchar(128) DEFAULT NULL,
  `PILOTERRORCODE` int(11) NOT NULL DEFAULT '0',
  `PILOTERRORDIAG` varchar(250) DEFAULT NULL,
  `EXEERRORCODE` int(11) NOT NULL DEFAULT '0',
  `EXEERRORDIAG` varchar(250) DEFAULT NULL,
  `SUPERRORCODE` int(11) NOT NULL DEFAULT '0',
  `SUPERRORDIAG` varchar(250) DEFAULT NULL,
  `DDMERRORCODE` int(11) NOT NULL DEFAULT '0',
  `DDMERRORDIAG` varchar(250) DEFAULT NULL,
  `BROKERAGEERRORCODE` int(11) NOT NULL DEFAULT '0',
  `BROKERAGEERRORDIAG` varchar(250) DEFAULT NULL,
  `JOBDISPATCHERERRORCODE` int(11) NOT NULL DEFAULT '0',
  `JOBDISPATCHERERRORDIAG` varchar(250) DEFAULT NULL,
  `TASKBUFFERERRORCODE` int(11) NOT NULL DEFAULT '0',
  `TASKBUFFERERRORDIAG` varchar(250) DEFAULT NULL,
  `COMPUTINGSITE` varchar(128) DEFAULT NULL,
  `COMPUTINGELEMENT` varchar(128) DEFAULT NULL,
  `JOBPARAMETERS` text,
  `METADATA` text,
  `PRODDBLOCK` varchar(255) DEFAULT NULL,
  `DISPATCHDBLOCK` varchar(255) DEFAULT NULL,
  `DESTINATIONDBLOCK` varchar(255) DEFAULT NULL,
  `DESTINATIONSE` varchar(250) DEFAULT NULL,
  `NEVENTS` int(11) NOT NULL DEFAULT '0',
  `GRID` varchar(32) DEFAULT NULL,
  `CLOUD` varchar(32) DEFAULT NULL,
  `CPUCONVERSION` decimal(9,4) DEFAULT NULL,
  `SOURCESITE` varchar(36) DEFAULT NULL,
  `DESTINATIONSITE` varchar(36) DEFAULT NULL,
  `TRANSFERTYPE` varchar(10) DEFAULT NULL,
  `TASKID` int(11) DEFAULT NULL,
  `CMTCONFIG` varchar(250) DEFAULT NULL,
  `STATECHANGETIME` datetime DEFAULT '1970-01-01 00:00:00',
  `PRODDBUPDATETIME` datetime DEFAULT '1970-01-01 00:00:00',
  `LOCKEDBY` varchar(128) DEFAULT NULL,
  `RELOCATIONFLAG` tinyint(4) DEFAULT '0',
  `JOBEXECUTIONID` bigint(20) DEFAULT '0',
  `VO` varchar(16) DEFAULT NULL,
  `PILOTTIMING` varchar(100) DEFAULT NULL,
  `WORKINGGROUP` varchar(20) DEFAULT NULL,
  `PROCESSINGTYPE` varchar(64) DEFAULT NULL,
  `PRODUSERNAME` varchar(60) DEFAULT NULL,
  `NINPUTFILES` int(11) DEFAULT NULL,
  `COUNTRYGROUP` varchar(20) DEFAULT NULL,
  `BATCHID` varchar(80) DEFAULT NULL,
  `PARENTID` bigint(20) DEFAULT NULL,
  `SPECIALHANDLING` varchar(80) DEFAULT NULL,
  `JOBSETID` bigint(20) DEFAULT NULL,
  `CORECOUNT` tinyint(4) DEFAULT NULL,
  `NINPUTDATAFILES` int(11) DEFAULT NULL,
  `INPUTFILETYPE` varchar(32) DEFAULT NULL,
  `INPUTFILEPROJECT` varchar(64) DEFAULT NULL,
  `INPUTFILEBYTES` bigint(20) DEFAULT NULL,
  `NOUTPUTDATAFILES` int(11) DEFAULT NULL,
  `OUTPUTFILEBYTES` bigint(20) DEFAULT NULL,
  `JOBMETRICS` varchar(500) DEFAULT NULL,
  `WORKQUEUE_ID` int(11) DEFAULT NULL,
  `JEDITASKID` bigint(20) DEFAULT NULL,
  `JOBSUBSTATUS` varchar(80) DEFAULT NULL,
  `ACTUALCORECOUNT` int(11) DEFAULT NULL,
  `REQID` int(11) DEFAULT NULL,
  `MAXRSS` int(11) DEFAULT NULL, 
  `MAXVMEM` int(11) DEFAULT NULL,  
  `MAXSWAP` int(11) DEFAULT NULL,
  `MAXPSS` int(11) DEFAULT NULL,
  `AVGRSS` int(11) DEFAULT NULL,
  `AVGMEM` int(11) DEFAULT NULL,
  `AVGSWAP` int(11) DEFAULT NULL,
  `AVGPSS` int(11) DEFAULT NULL,
  `MAXWALLTIME` int(11) DEFAULT NULL,
  `NUCLEUS` varchar(52) DEFAULT NULL,
  `EVENTSERVICE` tinyint(1) DEFAULT NULL,
  PRIMARY KEY (`PANDAID`),
  UNIQUE KEY `JOBSWAITING4_PANDAID_PK` (`PANDAID`),
  UNIQUE KEY `JOBSWAITING4_JEDITASKID_IDX` (`JEDITASKID`,`PANDAID`),
  KEY `JOBSWAITING4_REQID_IDX` (`REQID`),
  KEY `JOBSWAITING4_JOBEXECID_IDX` (`JOBEXECUTIONID`),
  KEY `JOBSWAITING4_WORKQUEUE_IDX` (`WORKQUEUE_ID`,`CLOUD`,`JOBSTATUS`,`PRODSOURCELABEL`,`CURRENTPRIORITY`),
  CONSTRAINT `JOBSWAITING4_JEDITASKID_FK` FOREIGN KEY (`JEDITASKID`) REFERENCES `jedi_tasks` (`JEDITASKID`),
  CONSTRAINT `JOBSWAITING4_WORKQUEUE_ID_FK` FOREIGN KEY (`WORKQUEUE_ID`) REFERENCES `jedi_work_queue` (`QUEUE_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `jobswaiting4`
--

LOCK TABLES `jobswaiting4` WRITE;
/*!40000 ALTER TABLE `jobswaiting4` DISABLE KEYS */;
/*!40000 ALTER TABLE `jobswaiting4` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `logstable`
--

DROP TABLE IF EXISTS `logstable`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `logstable` (
  `PANDAID` int(11) NOT NULL DEFAULT '0',
  `LOG1` text NOT NULL,
  `LOG2` text NOT NULL,
  `LOG3` text NOT NULL,
  `LOG4` text NOT NULL,
  PRIMARY KEY (`PANDAID`),
  UNIQUE KEY `PRIMARY_LOGSTABLE` (`PANDAID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `logstable`
--

LOCK TABLES `logstable` WRITE;
/*!40000 ALTER TABLE `logstable` DISABLE KEYS */;
/*!40000 ALTER TABLE `logstable` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `members`
--

DROP TABLE IF EXISTS `members`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `members` (
  `UNAME` varchar(30) NOT NULL,
  `GNAME` varchar(30) NOT NULL,
  `RIGHTS` varchar(30) DEFAULT NULL,
  `SINCE` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`UNAME`,`GNAME`),
  UNIQUE KEY `PRIMARY_MEMBERS` (`UNAME`,`GNAME`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `members`
--

LOCK TABLES `members` WRITE;
/*!40000 ALTER TABLE `members` DISABLE KEYS */;
/*!40000 ALTER TABLE `members` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `metatable`
--

DROP TABLE IF EXISTS `metatable`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `metatable` (
  `PANDAID` bigint(20) NOT NULL,
  `MODIFICATIONTIME` datetime NOT NULL DEFAULT '1970-01-01 00:00:00',
  `METADATA` text,
  PRIMARY KEY (`PANDAID`,`MODIFICATIONTIME`),
  UNIQUE KEY `PART_METATABLE_PK` (`PANDAID`,`MODIFICATIONTIME`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `metatable`
--

LOCK TABLES `metatable` WRITE;
/*!40000 ALTER TABLE `metatable` DISABLE KEYS */;
/*!40000 ALTER TABLE `metatable` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `metatable_arch`
--

DROP TABLE IF EXISTS `metatable_arch`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `metatable_arch` (
  `PANDAID` bigint(20) NOT NULL DEFAULT '0',
  `MODIFICATIONTIME` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `METADATA` text,
  KEY `META_ARCH_PANDAID_IDX` (`PANDAID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `metatable_arch`
--

LOCK TABLES `metatable_arch` WRITE;
/*!40000 ALTER TABLE `metatable_arch` DISABLE KEYS */;
/*!40000 ALTER TABLE `metatable_arch` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `multicloud_history`
--

DROP TABLE IF EXISTS `multicloud_history`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `multicloud_history` (
  `SITE` varchar(60) NOT NULL,
  `MULTICLOUD` varchar(64) DEFAULT NULL,
  `LAST_UPDATE` datetime NOT NULL,
  KEY `MULTICLOUD_HISTORY_SITE_IDX` (`SITE`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `multicloud_history`
--

LOCK TABLES `multicloud_history` WRITE;
/*!40000 ALTER TABLE `multicloud_history` DISABLE KEYS */;
/*!40000 ALTER TABLE `multicloud_history` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `mv_jobsactive4_stats`
--

DROP TABLE IF EXISTS `mv_jobsactive4_stats`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `mv_jobsactive4_stats` (
  `ID` int(20) NOT NULL AUTO_INCREMENT,
  `CUR_DATE` datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
  `CLOUD` varchar(50) DEFAULT NULL,
  `COMPUTINGSITE` varchar(128) DEFAULT NULL,
  `COUNTRYGROUP` varchar(20) DEFAULT NULL,
  `WORKINGGROUP` varchar(20) DEFAULT NULL,
  `RELOCATIONFLAG` tinyint(4) DEFAULT NULL,
  `JOBSTATUS` varchar(15) NOT NULL,
  `PROCESSINGTYPE` varchar(64) DEFAULT NULL,
  `PRODSOURCELABEL` varchar(20) DEFAULT NULL,
  `CURRENTPRIORITY` int(11) DEFAULT NULL,
  `NUM_OF_JOBS` int(11) DEFAULT NULL,
  `VO` varchar(16) DEFAULT NULL,
  `WORKQUEUE_ID` int(11) DEFAULT NULL,
  PRIMARY KEY (`ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `mv_jobsactive4_stats`
--

LOCK TABLES `mv_jobsactive4_stats` WRITE;
/*!40000 ALTER TABLE `mv_jobsactive4_stats` DISABLE KEYS */;
/*!40000 ALTER TABLE `mv_jobsactive4_stats` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `pandaconfig`
--

DROP TABLE IF EXISTS `pandaconfig`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `pandaconfig` (
  `NAME` varchar(60) NOT NULL,
  `CONTROLLER` varchar(20) NOT NULL,
  `PATHENA` varchar(20) DEFAULT NULL,
  PRIMARY KEY (`NAME`),
  UNIQUE KEY `PRIMARY_PANDACFG` (`NAME`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `pandaconfig`
--

LOCK TABLES `pandaconfig` WRITE;
/*!40000 ALTER TABLE `pandaconfig` DISABLE KEYS */;
/*!40000 ALTER TABLE `pandaconfig` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `pandaids_modiftime`
--

DROP TABLE IF EXISTS `pandaids_modiftime`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `pandaids_modiftime` (
  `PANDAID` bigint(20) NOT NULL,
  `MODIFTIME` datetime NOT NULL,
  PRIMARY KEY (`PANDAID`,`MODIFTIME`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `pandaids_modiftime`
--

LOCK TABLES `pandaids_modiftime` WRITE;
/*!40000 ALTER TABLE `pandaids_modiftime` DISABLE KEYS */;
/*!40000 ALTER TABLE `pandaids_modiftime` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `pandaids_modiftime_errlog`
--

DROP TABLE IF EXISTS `pandaids_modiftime_errlog`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `pandaids_modiftime_errlog` (
  `ORA_ERR_NUMBER$` int(11) DEFAULT NULL,
  `ORA_ERR_MESG$` varchar(2000) DEFAULT NULL,
  `ORA_ERR_ROWID$` bigint(20) DEFAULT NULL,
  `ORA_ERR_OPTYP$` varchar(2) DEFAULT NULL,
  `ORA_ERR_TAG$` varchar(2000) DEFAULT NULL,
  `PANDAID` varchar(4000) DEFAULT NULL,
  `MODIFTIME` varchar(4000) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `pandaids_modiftime_errlog`
--

LOCK TABLES `pandaids_modiftime_errlog` WRITE;
/*!40000 ALTER TABLE `pandaids_modiftime_errlog` DISABLE KEYS */;
/*!40000 ALTER TABLE `pandaids_modiftime_errlog` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `pandalog`
--

DROP TABLE IF EXISTS `pandalog`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `pandalog` (
  `BINTIME` datetime NOT NULL DEFAULT '1970-01-01 00:00:00',
  `NAME` varchar(30) DEFAULT NULL,
  `MODULE` varchar(30) DEFAULT NULL,
  `LOGUSER` varchar(80) DEFAULT NULL,
  `TYPE` varchar(20) DEFAULT NULL,
  `PID` bigint(20) NOT NULL DEFAULT '0',
  `LOGLEVEL` int(11) NOT NULL DEFAULT '0',
  `LEVELNAME` varchar(30) DEFAULT NULL,
  `TIME` varchar(30) DEFAULT NULL,
  `FILENAME` varchar(100) DEFAULT NULL,
  `LINE` int(11) NOT NULL DEFAULT '0',
  `MESSAGE` varchar(4000) DEFAULT NULL,
  KEY `PANDALOG_BINTIME_INDX` (`BINTIME`,`TYPE`),
  KEY `PANDALOG_NAMETYPEBINTIME_INDX` (`TYPE`,`NAME`,`BINTIME`),
  KEY `PANDALOG_MULTICOLUMN_INDX` (`BINTIME`,`NAME`,`TYPE`,`LEVELNAME`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `pandalog`
--

LOCK TABLES `pandalog` WRITE;
/*!40000 ALTER TABLE `pandalog` DISABLE KEYS */;
/*!40000 ALTER TABLE `pandalog` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `pandalog_fax`
--

DROP TABLE IF EXISTS `pandalog_fax`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `pandalog_fax` (
  `BINTIME` datetime NOT NULL DEFAULT '1970-01-01 00:00:00',
  `NAME` varchar(30) DEFAULT NULL,
  `MODULE` varchar(30) DEFAULT NULL,
  `LOGUSER` varchar(80) DEFAULT NULL,
  `TYPE` varchar(20) DEFAULT NULL,
  `PID` bigint(20) DEFAULT '0',
  `LOGLEVEL` int(11) DEFAULT '0',
  `LEVELNAME` varchar(30) DEFAULT NULL,
  `TIME` varchar(30) DEFAULT NULL,
  `FILENAME` varchar(100) DEFAULT NULL,
  `LINE` int(11) DEFAULT '0',
  `MESSAGE` varchar(4000) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `pandalog_fax`
--

LOCK TABLES `pandalog_fax` WRITE;
/*!40000 ALTER TABLE `pandalog_fax` DISABLE KEYS */;
/*!40000 ALTER TABLE `pandalog_fax` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `passwords`
--

DROP TABLE IF EXISTS `passwords`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `passwords` (
  `ID` int(11) NOT NULL,
  `PASS` varchar(60) NOT NULL,
  PRIMARY KEY (`ID`),
  UNIQUE KEY `PRIMARY_PASWD` (`ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `passwords`
--

LOCK TABLES `passwords` WRITE;
/*!40000 ALTER TABLE `passwords` DISABLE KEYS */;
/*!40000 ALTER TABLE `passwords` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `pilotqueue`
--

DROP TABLE IF EXISTS `pilotqueue`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `pilotqueue` (
  `JOBID` varchar(100) NOT NULL,
  `TPID` varchar(60) NOT NULL,
  `URL` varchar(200) DEFAULT NULL,
  `NICKNAME` varchar(60) NOT NULL,
  `SYSTEM` varchar(20) NOT NULL,
  `USER_` varchar(60) NOT NULL,
  `HOST` varchar(60) NOT NULL,
  `SUBMITHOST` varchar(60) NOT NULL,
  `QUEUEID` varchar(60) NOT NULL,
  `TYPE` varchar(20) NOT NULL,
  `PANDAID` int(11) DEFAULT NULL,
  `TCHECK` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `STATE` varchar(30) NOT NULL,
  `TSTATE` datetime NOT NULL DEFAULT '1970-01-01 00:00:00',
  `TENTER` datetime NOT NULL DEFAULT '1970-01-01 00:00:00',
  `TSUBMIT` datetime NOT NULL DEFAULT '1970-01-01 00:00:00',
  `TACCEPT` datetime NOT NULL DEFAULT '1970-01-01 00:00:00',
  `TSCHEDULE` datetime NOT NULL DEFAULT '1970-01-01 00:00:00',
  `TSTART` datetime NOT NULL DEFAULT '1970-01-01 00:00:00',
  `TEND` datetime NOT NULL DEFAULT '1970-01-01 00:00:00',
  `TDONE` datetime NOT NULL DEFAULT '1970-01-01 00:00:00',
  `TRETRIEVE` datetime NOT NULL DEFAULT '1970-01-01 00:00:00',
  `STATUS` varchar(20) NOT NULL,
  `ERRCODE` int(11) NOT NULL DEFAULT '0',
  `ERRINFO` varchar(150) NOT NULL,
  `MESSAGE` varchar(4000) DEFAULT NULL,
  `SCHEDD_NAME` varchar(60) NOT NULL,
  `WORKERNODE` varchar(60) NOT NULL,
  PRIMARY KEY (`JOBID`,`NICKNAME`),
  UNIQUE KEY `PRIMARY_PILOTQUEUE` (`JOBID`,`NICKNAME`),
  UNIQUE KEY `PILOTQUEUE_TPID_IDX` (`TPID`),
  KEY `PILOTQUEUE_PANDAID_IDX` (`PANDAID`),
  KEY `PILOTQUEUE_QUEUEID_IDX` (`QUEUEID`),
  KEY `PILOTQUEUE_NICKNAME_IDX` (`NICKNAME`),
  KEY `PILOTQUEUE_STATE_IDX` (`STATE`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `pilotqueue`
--

LOCK TABLES `pilotqueue` WRITE;
/*!40000 ALTER TABLE `pilotqueue` DISABLE KEYS */;
/*!40000 ALTER TABLE `pilotqueue` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `pilotqueue_bnl`
--

DROP TABLE IF EXISTS `pilotqueue_bnl`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `pilotqueue_bnl` (
  `JOBID` varchar(100) NOT NULL,
  `TPID` varchar(60) NOT NULL,
  `URL` varchar(200) NOT NULL,
  `NICKNAME` varchar(60) NOT NULL,
  `SYSTEM` varchar(20) NOT NULL,
  `USER_` varchar(60) NOT NULL,
  `HOST` varchar(60) NOT NULL,
  `SUBMITHOST` varchar(60) NOT NULL,
  `SCHEDD_NAME` varchar(60) NOT NULL,
  `QUEUEID` varchar(60) NOT NULL,
  `TYPE` varchar(20) NOT NULL,
  `PANDAID` int(11) DEFAULT NULL,
  `TCHECK` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `STATE` varchar(30) NOT NULL,
  `TSTATE` datetime NOT NULL DEFAULT '1970-01-01 00:00:00',
  `TENTER` datetime NOT NULL DEFAULT '1970-01-01 00:00:00',
  `TSUBMIT` datetime NOT NULL DEFAULT '1970-01-01 00:00:00',
  `TACCEPT` datetime NOT NULL DEFAULT '1970-01-01 00:00:00',
  `TSCHEDULE` datetime NOT NULL DEFAULT '1970-01-01 00:00:00',
  `TSTART` datetime NOT NULL DEFAULT '1970-01-01 00:00:00',
  `TEND` datetime NOT NULL DEFAULT '1970-01-01 00:00:00',
  `TDONE` datetime NOT NULL DEFAULT '1970-01-01 00:00:00',
  `TRETRIEVE` datetime NOT NULL DEFAULT '1970-01-01 00:00:00',
  `STATUS` varchar(20) NOT NULL,
  `ERRCODE` int(11) NOT NULL DEFAULT '0',
  `ERRINFO` varchar(150) NOT NULL,
  `MESSAGE` varchar(4000) DEFAULT NULL,
  `WORKERNODE` varchar(60) NOT NULL,
  PRIMARY KEY (`TPID`),
  UNIQUE KEY `PRIMARY_PILOTQUEUEBNL` (`TPID`),
  KEY `PILOTQUEUEBNL_PANDAID_IDX` (`PANDAID`),
  KEY `PILOTQUEUEBNL_NICKNAME_IDX` (`NICKNAME`),
  KEY `PILOTQUEUEBNL_QUEUEID_IDX` (`QUEUEID`),
  KEY `PILOTQUEUEBNL_STATE_IDX` (`STATE`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `pilotqueue_bnl`
--

LOCK TABLES `pilotqueue_bnl` WRITE;
/*!40000 ALTER TABLE `pilotqueue_bnl` DISABLE KEYS */;
/*!40000 ALTER TABLE `pilotqueue_bnl` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `pilottoken`
--

DROP TABLE IF EXISTS `pilottoken`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `pilottoken` (
  `TOKEN` varchar(64) NOT NULL,
  `SCHEDULERHOST` varchar(100) DEFAULT NULL,
  `SCHEDULERUSER` varchar(150) DEFAULT NULL,
  `USAGES` int(11) NOT NULL DEFAULT '1',
  `CREATED` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `EXPIRES` datetime NOT NULL DEFAULT '1970-01-01 00:00:00',
  `SCHEDULERID` varchar(80) DEFAULT NULL,
  PRIMARY KEY (`TOKEN`),
  UNIQUE KEY `PILOTTOKEN_TOKEN_PK` (`TOKEN`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `pilottoken`
--

LOCK TABLES `pilottoken` WRITE;
/*!40000 ALTER TABLE `pilottoken` DISABLE KEYS */;
/*!40000 ALTER TABLE `pilottoken` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `pilottype`
--

DROP TABLE IF EXISTS `pilottype`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `pilottype` (
  `NAME` varchar(60) NOT NULL,
  `SCRIPT` varchar(60) NOT NULL,
  `URL` varchar(150) NOT NULL,
  `SYSTEM` varchar(60) NOT NULL,
  PRIMARY KEY (`NAME`),
  UNIQUE KEY `PRIMARY_PILOTTYPE` (`NAME`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `pilottype`
--

LOCK TABLES `pilottype` WRITE;
/*!40000 ALTER TABLE `pilottype` DISABLE KEYS */;
/*!40000 ALTER TABLE `pilottype` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `productiondatasets`
--

DROP TABLE IF EXISTS `productiondatasets`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `productiondatasets` (
  `NAME` varchar(120) NOT NULL,
  `VERSION` int(11) DEFAULT NULL,
  `VUID` varchar(40) NOT NULL,
  `FILES` int(11) DEFAULT NULL,
  `GB` int(11) DEFAULT NULL,
  `EVENTS` int(11) DEFAULT NULL,
  `SITE` varchar(60) DEFAULT NULL,
  `SW_RELEASE` varchar(20) DEFAULT NULL,
  `GEOMETRY` varchar(20) DEFAULT NULL,
  `JOBID` int(11) DEFAULT NULL,
  `PANDAID` int(11) DEFAULT NULL,
  `PRODTIME` datetime DEFAULT NULL,
  `TIMESTAMP` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `productiondatasets`
--

LOCK TABLES `productiondatasets` WRITE;
/*!40000 ALTER TABLE `productiondatasets` DISABLE KEYS */;
/*!40000 ALTER TABLE `productiondatasets` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `proxykey`
--

DROP TABLE IF EXISTS `proxykey`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `proxykey` (
  `ID` int(11) NOT NULL,
  `DN` varchar(100) NOT NULL,
  `CREDNAME` varchar(40) NOT NULL,
  `CREATED` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `EXPIRES` datetime NOT NULL DEFAULT '1970-01-01 00:00:00',
  `ORIGIN` varchar(80) NOT NULL,
  `MYPROXY` varchar(80) NOT NULL,
  PRIMARY KEY (`ID`),
  UNIQUE KEY `PRIMARY_PROXYKEY` (`ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `proxykey`
--

LOCK TABLES `proxykey` WRITE;
/*!40000 ALTER TABLE `proxykey` DISABLE KEYS */;
/*!40000 ALTER TABLE `proxykey` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `redirect`
--

DROP TABLE IF EXISTS `redirect`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `redirect` (
  `SERVICE` varchar(30) NOT NULL,
  `TYPE` varchar(30) NOT NULL,
  `SITE` varchar(60) NOT NULL,
  `DESCRIPTION` varchar(120) NOT NULL,
  `URL` varchar(250) NOT NULL,
  `TESTURL` varchar(250) DEFAULT NULL,
  `RESPONSE` varchar(30) NOT NULL,
  `ALIVERESPONSE` varchar(30) NOT NULL,
  `RESPONSETIME` int(11) DEFAULT NULL,
  `RANK` int(11) DEFAULT NULL,
  `PERFORMANCE` int(11) DEFAULT NULL,
  `STATUS` varchar(30) NOT NULL,
  `LOG` varchar(250) DEFAULT NULL,
  `STATUSTIME` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `USETIME` datetime NOT NULL DEFAULT '1970-01-01 00:00:00',
  PRIMARY KEY (`URL`),
  UNIQUE KEY `PRIMARY_REDIRECT` (`URL`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `redirect`
--

LOCK TABLES `redirect` WRITE;
/*!40000 ALTER TABLE `redirect` DISABLE KEYS */;
/*!40000 ALTER TABLE `redirect` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `savedpages`
--

DROP TABLE IF EXISTS `savedpages`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `savedpages` (
  `NAME` varchar(30) NOT NULL,
  `FLAG` varchar(20) NOT NULL,
  `HOURS` int(11) NOT NULL DEFAULT '0',
  `HTML` text NOT NULL,
  `LASTMOD` datetime DEFAULT NULL,
  `INTERVAL` int(11) DEFAULT NULL,
  PRIMARY KEY (`NAME`,`FLAG`,`HOURS`),
  UNIQUE KEY `PRIMARY_SAVEDPAGES` (`NAME`,`FLAG`,`HOURS`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `savedpages`
--

LOCK TABLES `savedpages` WRITE;
/*!40000 ALTER TABLE `savedpages` DISABLE KEYS */;
/*!40000 ALTER TABLE `savedpages` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `schedconfig`
--

DROP TABLE IF EXISTS `schedconfig`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `schedconfig` (
  `NAME` varchar(60) NOT NULL DEFAULT 'default',
  `NICKNAME` varchar(60) NOT NULL,
  `QUEUE` varchar(60) DEFAULT NULL,
  `LOCALQUEUE` varchar(50) DEFAULT NULL,
  `SYSTEM` varchar(60) NOT NULL,
  `SYSCONFIG` varchar(20) DEFAULT NULL,
  `ENVIRON` varchar(250) DEFAULT NULL,
  `GATEKEEPER` varchar(120) DEFAULT NULL,
  `JOBMANAGER` varchar(80) DEFAULT NULL,
  `SE` varchar(400) DEFAULT NULL,
  `DDM` varchar(120) DEFAULT NULL,
  `JDLADD` varchar(500) DEFAULT NULL,
  `GLOBUSADD` varchar(100) DEFAULT NULL,
  `JDL` varchar(60) DEFAULT NULL,
  `JDLTXT` varchar(500) DEFAULT NULL,
  `VERSION` varchar(60) DEFAULT NULL,
  `SITE` varchar(60) NOT NULL,
  `REGION` varchar(60) DEFAULT NULL,
  `GSTAT` varchar(60) DEFAULT NULL,
  `TAGS` varchar(200) DEFAULT NULL,
  `CMD` varchar(200) DEFAULT NULL,
  `LASTMOD` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `ERRINFO` varchar(80) DEFAULT NULL,
  `NQUEUE` int(11) NOT NULL DEFAULT '0',
  `COMMENT_` varchar(500) DEFAULT NULL,
  `APPDIR` varchar(500) DEFAULT NULL,
  `DATADIR` varchar(80) DEFAULT NULL,
  `TMPDIR` varchar(80) DEFAULT NULL,
  `WNTMPDIR` varchar(80) DEFAULT NULL,
  `DQ2URL` varchar(80) DEFAULT NULL,
  `SPECIAL_PAR` varchar(80) DEFAULT NULL,
  `PYTHON_PATH` varchar(80) DEFAULT NULL,
  `NODES` int(11) NOT NULL DEFAULT '0',
  `STATUS` varchar(10) DEFAULT 'offline',
  `COPYTOOL` varchar(80) DEFAULT NULL,
  `COPYSETUP` varchar(200) DEFAULT NULL,
  `RELEASES` varchar(500) DEFAULT NULL,
  `SEPATH` varchar(400) DEFAULT NULL,
  `ENVSETUP` varchar(200) DEFAULT NULL,
  `COPYPREFIX` varchar(160) DEFAULT NULL,
  `LFCPATH` varchar(80) DEFAULT NULL,
  `SEOPT` varchar(400) DEFAULT NULL,
  `SEIN` varchar(400) DEFAULT NULL,
  `SEINOPT` varchar(400) DEFAULT NULL,
  `LFCHOST` varchar(80) DEFAULT NULL,
  `CLOUD` varchar(60) DEFAULT NULL,
  `SITEID` varchar(60) DEFAULT NULL,
  `PROXY` varchar(80) DEFAULT NULL,
  `RETRY` varchar(10) DEFAULT NULL,
  `QUEUEHOURS` int(11) NOT NULL DEFAULT '0',
  `ENVSETUPIN` varchar(200) DEFAULT NULL,
  `COPYTOOLIN` varchar(180) DEFAULT NULL,
  `COPYSETUPIN` varchar(200) DEFAULT NULL,
  `SEPRODPATH` varchar(400) DEFAULT NULL,
  `LFCPRODPATH` varchar(80) DEFAULT NULL,
  `COPYPREFIXIN` varchar(360) DEFAULT NULL,
  `RECOVERDIR` varchar(80) DEFAULT NULL,
  `MEMORY` int(11) NOT NULL DEFAULT '0',
  `MAXTIME` int(11) NOT NULL DEFAULT '0',
  `SPACE` int(11) NOT NULL DEFAULT '0',
  `TSPACE` datetime NOT NULL DEFAULT '1970-01-01 00:00:00',
  `CMTCONFIG` varchar(250) DEFAULT NULL,
  `SETOKENS` varchar(80) DEFAULT NULL,
  `GLEXEC` varchar(10) DEFAULT NULL,
  `PRIORITYOFFSET` varchar(60) DEFAULT NULL,
  `ALLOWEDGROUPS` varchar(100) DEFAULT NULL,
  `DEFAULTTOKEN` varchar(100) DEFAULT NULL,
  `PCACHE` varchar(100) DEFAULT NULL,
  `VALIDATEDRELEASES` varchar(500) DEFAULT NULL,
  `ACCESSCONTROL` varchar(20) DEFAULT NULL,
  `DN` varchar(100) DEFAULT NULL,
  `EMAIL` varchar(60) DEFAULT NULL,
  `ALLOWEDNODE` varchar(80) DEFAULT NULL,
  `MAXINPUTSIZE` int(11) DEFAULT NULL,
  `TIMEFLOOR` int(11) DEFAULT NULL,
  `DEPTHBOOST` int(11) DEFAULT NULL,
  `IDLEPILOTSUPRESSION` int(11) DEFAULT NULL,
  `PILOTLIMIT` int(11) DEFAULT NULL,
  `TRANSFERRINGLIMIT` int(11) DEFAULT NULL,
  `CACHEDSE` tinyint(4) DEFAULT NULL,
  `CORECOUNT` tinyint(4) DEFAULT NULL,
  `COUNTRYGROUP` varchar(64) DEFAULT NULL,
  `AVAILABLECPU` varchar(64) DEFAULT NULL,
  `AVAILABLESTORAGE` varchar(64) DEFAULT NULL,
  `PLEDGEDCPU` varchar(64) DEFAULT NULL,
  `PLEDGEDSTORAGE` varchar(64) DEFAULT NULL,
  `STATUSOVERRIDE` varchar(256) DEFAULT 'offline',
  `ALLOWDIRECTACCESS` varchar(10) DEFAULT 'False',
  `GOCNAME` varchar(64) DEFAULT 'site',
  `TIER` varchar(15) DEFAULT NULL,
  `MULTICLOUD` varchar(64) DEFAULT NULL,
  `LFCREGISTER` varchar(10) DEFAULT NULL,
  `STAGEINRETRY` int(11) DEFAULT '2',
  `STAGEOUTRETRY` int(11) DEFAULT '2',
  `FAIRSHAREPOLICY` varchar(512) DEFAULT NULL,
  `ALLOWFAX` varchar(64) DEFAULT NULL,
  `FAXREDIRECTOR` varchar(256) DEFAULT NULL,
  `MAXWDIR` int(11) DEFAULT NULL,
  `CELIST` varchar(4000) DEFAULT NULL,
  `MINMEMORY` int(11) DEFAULT NULL,
  `MAXMEMORY` int(11) DEFAULT NULL,
  `MINTIME` int(11) DEFAULT NULL,
  `ALLOWJEM` varchar(64) DEFAULT NULL,
  `CATCHALL` varchar(512) DEFAULT NULL,
  `FAXDOOR` varchar(128) DEFAULT NULL,
  `WANSOURCELIMIT` int(11) DEFAULT NULL,
  `WANSINKLIMIT` int(11) DEFAULT NULL,
  `AUTO_MCU` tinyint(4) NOT NULL DEFAULT '0',
  `OBJECTSTORE` varchar(512) DEFAULT NULL,
  `ALLOWHTTP` varchar(64) DEFAULT NULL,
  `HTTPREDIRECTOR` varchar(256) DEFAULT NULL,
  `MULTICLOUD_APPEND` varchar(64) DEFAULT NULL,
  PRIMARY KEY (`NICKNAME`),
  UNIQUE KEY `PRIMARY_SCHEDCFG` (`NICKNAME`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `schedconfig`
--

LOCK TABLES `schedconfig` WRITE;
/*!40000 ALTER TABLE `schedconfig` DISABLE KEYS */;
/*!40000 ALTER TABLE `schedconfig` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `schedconfig_1st_attempt`
--

DROP TABLE IF EXISTS `schedconfig_1st_attempt`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `schedconfig_1st_attempt` (
  `NICKNAME` varchar(60) NOT NULL,
  `LOCALQUEUE` varchar(20) DEFAULT NULL,
  `SYSTEM` varchar(60) NOT NULL,
  `GATEKEEPER` varchar(40) DEFAULT NULL,
  `JOBMANAGER` varchar(80) DEFAULT NULL,
  `SE` varchar(400) DEFAULT NULL,
  `DDM` varchar(120) DEFAULT NULL,
  `GLOBUSADD` varchar(100) DEFAULT NULL,
  `SITE` varchar(60) NOT NULL,
  `REGION` varchar(60) DEFAULT NULL,
  `GSTAT` varchar(60) DEFAULT NULL,
  `TAGS` varchar(200) DEFAULT NULL,
  `LASTMOD` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `ERRINFO` varchar(80) DEFAULT NULL,
  `COMMENT_` varchar(500) DEFAULT NULL,
  `APPDIR` varchar(500) DEFAULT NULL,
  `TMPDIR` varchar(80) DEFAULT NULL,
  `WNTMPDIR` varchar(80) DEFAULT NULL,
  `DQ2URL` varchar(80) DEFAULT NULL,
  `SPECIAL_PAR` varchar(80) DEFAULT NULL,
  `PYTHON_PATH` varchar(80) DEFAULT NULL,
  `NODES` int(11) NOT NULL DEFAULT '0',
  `STATUS` varchar(10) DEFAULT 'offline',
  `COPYTOOL` varchar(80) DEFAULT NULL,
  `COPYSETUP` varchar(200) DEFAULT NULL,
  `RELEASES` varchar(500) DEFAULT NULL,
  `SEPATH` varchar(400) DEFAULT NULL,
  `ENVSETUP` varchar(200) DEFAULT NULL,
  `COPYPREFIX` varchar(160) DEFAULT NULL,
  `LFCPATH` varchar(80) DEFAULT NULL,
  `SEOPT` varchar(400) DEFAULT NULL,
  `SEIN` varchar(400) DEFAULT NULL,
  `SEINOPT` varchar(400) DEFAULT NULL,
  `LFCHOST` varchar(80) DEFAULT NULL,
  `CLOUD` varchar(60) DEFAULT NULL,
  `SITEID` varchar(60) DEFAULT NULL,
  `RETRY` varchar(10) DEFAULT NULL,
  `QUEUEHOURS` int(11) NOT NULL DEFAULT '0',
  `ENVSETUPIN` varchar(200) DEFAULT NULL,
  `COPYTOOLIN` varchar(180) DEFAULT NULL,
  `COPYSETUPIN` varchar(200) DEFAULT NULL,
  `SEPRODPATH` varchar(400) DEFAULT NULL,
  `LFCPRODPATH` varchar(80) DEFAULT NULL,
  `COPYPREFIXIN` varchar(360) DEFAULT NULL,
  `RECOVERDIR` varchar(80) DEFAULT NULL,
  `MEMORY` int(11) NOT NULL DEFAULT '0',
  `MAXTIME` int(11) NOT NULL DEFAULT '0',
  `SPACE` int(11) NOT NULL DEFAULT '0',
  `TSPACE` datetime NOT NULL DEFAULT '1970-01-01 00:00:00',
  `CMTCONFIG` varchar(250) DEFAULT NULL,
  `SETOKENS` varchar(80) DEFAULT NULL,
  `GLEXEC` varchar(10) DEFAULT NULL,
  `PRIORITYOFFSET` varchar(60) DEFAULT NULL,
  `ALLOWEDGROUPS` varchar(100) DEFAULT NULL,
  `DEFAULTTOKEN` varchar(100) DEFAULT NULL,
  `PCACHE` varchar(100) DEFAULT NULL,
  `VALIDATEDRELEASES` varchar(500) DEFAULT NULL,
  `ACCESSCONTROL` varchar(20) DEFAULT NULL,
  `DN` varchar(100) DEFAULT NULL,
  `EMAIL` varchar(60) DEFAULT NULL,
  `ALLOWEDNODE` varchar(80) DEFAULT NULL,
  `MAXINPUTSIZE` int(11) DEFAULT NULL,
  `TIMEFLOOR` int(11) DEFAULT NULL,
  `DEPTHBOOST` int(11) DEFAULT NULL,
  `IDLEPILOTSUPRESSION` int(11) DEFAULT NULL,
  `PILOTLIMIT` int(11) DEFAULT NULL,
  `TRANSFERRINGLIMIT` int(11) DEFAULT NULL,
  `CACHEDSE` tinyint(4) DEFAULT NULL,
  `CORECOUNT` tinyint(4) DEFAULT NULL,
  `COUNTRYGROUP` varchar(64) DEFAULT NULL,
  `AVAILABLECPU` varchar(64) DEFAULT NULL,
  `AVAILABLESTORAGE` varchar(64) DEFAULT NULL,
  `PLEDGEDCPU` varchar(64) DEFAULT NULL,
  `PLEDGEDSTORAGE` varchar(64) DEFAULT NULL,
  `STATUSOVERRIDE` varchar(256) DEFAULT 'offline',
  `ALLOWDIRECTACCESS` varchar(10) DEFAULT 'False',
  `GOCNAME` varchar(64) DEFAULT 'site',
  `TIER` varchar(15) DEFAULT NULL,
  `MULTICLOUD` varchar(64) DEFAULT NULL,
  `LFCREGISTER` varchar(10) DEFAULT NULL,
  `STAGEINRETRY` int(11) DEFAULT '2',
  `STAGEOUTRETRY` int(11) DEFAULT '2',
  `FAIRSHAREPOLICY` varchar(512) DEFAULT NULL,
  `ALLOWFAX` varchar(64) DEFAULT NULL,
  `FAXREDIRECTOR` varchar(256) DEFAULT NULL,
  `MAXWDIR` int(11) DEFAULT NULL,
  `CELIST` varchar(4000) DEFAULT NULL,
  `MINMEMORY` int(11) DEFAULT NULL,
  `MAXMEMORY` int(11) DEFAULT NULL,
  `MINTIME` int(11) DEFAULT NULL,
  `ALLOWJEM` varchar(64) DEFAULT NULL,
  `CATCHALL` varchar(512) DEFAULT NULL,
  `FAXDOOR` varchar(128) DEFAULT NULL,
  `WANSOURCELIMIT` int(11) DEFAULT NULL,
  `WANSINKLIMIT` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `schedconfig_1st_attempt`
--

LOCK TABLES `schedconfig_1st_attempt` WRITE;
/*!40000 ALTER TABLE `schedconfig_1st_attempt` DISABLE KEYS */;
/*!40000 ALTER TABLE `schedconfig_1st_attempt` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `schedconfig_2nd_attempt`
--

DROP TABLE IF EXISTS `schedconfig_2nd_attempt`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `schedconfig_2nd_attempt` (
  `NICKNAME` varchar(60) NOT NULL,
  `QUEUE` varchar(60) DEFAULT NULL,
  `LOCALQUEUE` varchar(20) DEFAULT NULL,
  `SYSTEM` varchar(60) NOT NULL,
  `GATEKEEPER` varchar(40) DEFAULT NULL,
  `JOBMANAGER` varchar(80) DEFAULT NULL,
  `SE` varchar(400) DEFAULT NULL,
  `DDM` varchar(120) DEFAULT NULL,
  `GLOBUSADD` varchar(100) DEFAULT NULL,
  `SITE` varchar(60) NOT NULL,
  `REGION` varchar(60) DEFAULT NULL,
  `GSTAT` varchar(60) DEFAULT NULL,
  `TAGS` varchar(200) DEFAULT NULL,
  `LASTMOD` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `ERRINFO` varchar(80) DEFAULT NULL,
  `COMMENT_` varchar(500) DEFAULT NULL,
  `APPDIR` varchar(500) DEFAULT NULL,
  `TMPDIR` varchar(80) DEFAULT NULL,
  `WNTMPDIR` varchar(80) DEFAULT NULL,
  `DQ2URL` varchar(80) DEFAULT NULL,
  `SPECIAL_PAR` varchar(80) DEFAULT NULL,
  `PYTHON_PATH` varchar(80) DEFAULT NULL,
  `NODES` int(11) NOT NULL DEFAULT '0',
  `STATUS` varchar(10) DEFAULT 'offline',
  `COPYTOOL` varchar(80) DEFAULT NULL,
  `COPYSETUP` varchar(200) DEFAULT NULL,
  `RELEASES` varchar(500) DEFAULT NULL,
  `SEPATH` varchar(400) DEFAULT NULL,
  `ENVSETUP` varchar(200) DEFAULT NULL,
  `COPYPREFIX` varchar(160) DEFAULT NULL,
  `LFCPATH` varchar(80) DEFAULT NULL,
  `SEOPT` varchar(400) DEFAULT NULL,
  `SEIN` varchar(400) DEFAULT NULL,
  `SEINOPT` varchar(400) DEFAULT NULL,
  `LFCHOST` varchar(80) DEFAULT NULL,
  `CLOUD` varchar(60) DEFAULT NULL,
  `SITEID` varchar(60) DEFAULT NULL,
  `RETRY` varchar(10) DEFAULT NULL,
  `QUEUEHOURS` int(11) NOT NULL DEFAULT '0',
  `ENVSETUPIN` varchar(200) DEFAULT NULL,
  `COPYTOOLIN` varchar(180) DEFAULT NULL,
  `COPYSETUPIN` varchar(200) DEFAULT NULL,
  `SEPRODPATH` varchar(400) DEFAULT NULL,
  `LFCPRODPATH` varchar(80) DEFAULT NULL,
  `COPYPREFIXIN` varchar(360) DEFAULT NULL,
  `RECOVERDIR` varchar(80) DEFAULT NULL,
  `MEMORY` int(11) NOT NULL DEFAULT '0',
  `MAXTIME` int(11) NOT NULL DEFAULT '0',
  `SPACE` int(11) NOT NULL DEFAULT '0',
  `TSPACE` datetime NOT NULL DEFAULT '1970-01-01 00:00:00',
  `CMTCONFIG` varchar(250) DEFAULT NULL,
  `SETOKENS` varchar(80) DEFAULT NULL,
  `GLEXEC` varchar(10) DEFAULT NULL,
  `PRIORITYOFFSET` varchar(60) DEFAULT NULL,
  `ALLOWEDGROUPS` varchar(100) DEFAULT NULL,
  `DEFAULTTOKEN` varchar(100) DEFAULT NULL,
  `PCACHE` varchar(100) DEFAULT NULL,
  `VALIDATEDRELEASES` varchar(500) DEFAULT NULL,
  `ACCESSCONTROL` varchar(20) DEFAULT NULL,
  `DN` varchar(100) DEFAULT NULL,
  `EMAIL` varchar(60) DEFAULT NULL,
  `ALLOWEDNODE` varchar(80) DEFAULT NULL,
  `MAXINPUTSIZE` int(11) DEFAULT NULL,
  `TIMEFLOOR` int(11) DEFAULT NULL,
  `DEPTHBOOST` int(11) DEFAULT NULL,
  `IDLEPILOTSUPRESSION` int(11) DEFAULT NULL,
  `PILOTLIMIT` int(11) DEFAULT NULL,
  `TRANSFERRINGLIMIT` int(11) DEFAULT NULL,
  `CACHEDSE` tinyint(4) DEFAULT NULL,
  `CORECOUNT` tinyint(4) DEFAULT NULL,
  `COUNTRYGROUP` varchar(64) DEFAULT NULL,
  `AVAILABLECPU` varchar(64) DEFAULT NULL,
  `AVAILABLESTORAGE` varchar(64) DEFAULT NULL,
  `PLEDGEDCPU` varchar(64) DEFAULT NULL,
  `PLEDGEDSTORAGE` varchar(64) DEFAULT NULL,
  `STATUSOVERRIDE` varchar(256) DEFAULT 'offline',
  `ALLOWDIRECTACCESS` varchar(10) DEFAULT 'False',
  `GOCNAME` varchar(64) DEFAULT 'site',
  `TIER` varchar(15) DEFAULT NULL,
  `MULTICLOUD` varchar(64) DEFAULT NULL,
  `LFCREGISTER` varchar(10) DEFAULT NULL,
  `STAGEINRETRY` int(11) DEFAULT '2',
  `STAGEOUTRETRY` int(11) DEFAULT '2',
  `FAIRSHAREPOLICY` varchar(512) DEFAULT NULL,
  `ALLOWFAX` varchar(64) DEFAULT NULL,
  `FAXREDIRECTOR` varchar(256) DEFAULT NULL,
  `MAXWDIR` int(11) DEFAULT NULL,
  `CELIST` varchar(4000) DEFAULT NULL,
  `MINMEMORY` int(11) DEFAULT NULL,
  `MAXMEMORY` int(11) DEFAULT NULL,
  `MINTIME` int(11) DEFAULT NULL,
  `ALLOWJEM` varchar(64) DEFAULT NULL,
  `CATCHALL` varchar(512) DEFAULT NULL,
  `FAXDOOR` varchar(128) DEFAULT NULL,
  `WANSOURCELIMIT` int(11) DEFAULT NULL,
  `WANSINKLIMIT` int(11) DEFAULT NULL,
  PRIMARY KEY (`NICKNAME`),
  UNIQUE KEY `SCHEDCONFIG_PK` (`NICKNAME`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `schedconfig_2nd_attempt`
--

LOCK TABLES `schedconfig_2nd_attempt` WRITE;
/*!40000 ALTER TABLE `schedconfig_2nd_attempt` DISABLE KEYS */;
/*!40000 ALTER TABLE `schedconfig_2nd_attempt` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `schedinstance`
--

DROP TABLE IF EXISTS `schedinstance`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `schedinstance` (
  `NAME` varchar(60) NOT NULL DEFAULT 'default',
  `NICKNAME` varchar(60) NOT NULL,
  `PANDASITE` varchar(60) NOT NULL,
  `NQUEUE` int(11) NOT NULL DEFAULT '5',
  `NQUEUED` int(11) NOT NULL DEFAULT '0',
  `NRUNNING` int(11) NOT NULL DEFAULT '0',
  `NFINISHED` int(11) NOT NULL DEFAULT '0',
  `NFAILED` int(11) NOT NULL DEFAULT '0',
  `NABORTED` int(11) NOT NULL DEFAULT '0',
  `NJOBS` int(11) NOT NULL DEFAULT '0',
  `TVALID` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `LASTMOD` datetime NOT NULL DEFAULT '1970-01-01 00:00:00',
  `ERRINFO` varchar(150) DEFAULT NULL,
  `NDONE` int(11) NOT NULL DEFAULT '0',
  `TOTRUNT` int(11) NOT NULL DEFAULT '0',
  `COMMENT_` varchar(500) DEFAULT NULL,
  PRIMARY KEY (`NICKNAME`,`PANDASITE`),
  UNIQUE KEY `PRIMARY_SCHEDINST` (`NICKNAME`,`PANDASITE`),
  KEY `SCHEDINSTANCE_NICKNAME_IDX` (`NICKNAME`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `schedinstance`
--

LOCK TABLES `schedinstance` WRITE;
/*!40000 ALTER TABLE `schedinstance` DISABLE KEYS */;
/*!40000 ALTER TABLE `schedinstance` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `servicelist`
--

DROP TABLE IF EXISTS `servicelist`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `servicelist` (
  `ID` int(11) NOT NULL,
  `NAME` varchar(60) NOT NULL DEFAULT 'default',
  `HOST` varchar(100) DEFAULT NULL,
  `PID` int(11) DEFAULT NULL,
  `USERID` varchar(40) DEFAULT NULL,
  `TYPE` varchar(30) DEFAULT NULL,
  `GRP` varchar(20) DEFAULT NULL,
  `DESCRIPTION` varchar(200) DEFAULT NULL,
  `URL` varchar(200) DEFAULT NULL,
  `TESTURL` varchar(200) DEFAULT NULL,
  `RESPONSE` varchar(200) DEFAULT NULL,
  `TRESPONSE` int(11) DEFAULT NULL,
  `TSTART` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `TSTOP` datetime NOT NULL DEFAULT '1970-01-01 00:00:00',
  `TCHECK` datetime NOT NULL DEFAULT '1970-01-01 00:00:00',
  `CYCLESEC` int(11) DEFAULT NULL,
  `STATUS` varchar(20) NOT NULL,
  `LASTMOD` datetime NOT NULL DEFAULT '1970-01-01 00:00:00',
  `CONFIG` varchar(200) DEFAULT NULL,
  `MESSAGE` varchar(4000) DEFAULT NULL,
  `RESTARTCMD` varchar(4000) DEFAULT NULL,
  `DOACTION` varchar(4000) DEFAULT NULL,
  PRIMARY KEY (`ID`),
  UNIQUE KEY `PRIMARY_SERVICELST` (`ID`),
  KEY `SERVICELIST_NAMEUSERID_IDX` (`NAME`,`HOST`,`CONFIG`,`USERID`),
  KEY `SERVICELIST_NAME_IDX` (`NAME`,`HOST`,`CONFIG`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `servicelist`
--

LOCK TABLES `servicelist` WRITE;
/*!40000 ALTER TABLE `servicelist` DISABLE KEYS */;
/*!40000 ALTER TABLE `servicelist` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `siteaccess`
--

DROP TABLE IF EXISTS `siteaccess`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `siteaccess` (
  `ID` int(11) NOT NULL AUTO_INCREMENT,
  `DN` varchar(100) DEFAULT NULL,
  `PANDASITE` varchar(100) DEFAULT NULL,
  `POFFSET` int(11) NOT NULL DEFAULT '0',
  `RIGHTS` varchar(30) DEFAULT NULL,
  `STATUS` varchar(20) DEFAULT NULL,
  `WORKINGGROUPS` varchar(100) DEFAULT NULL,
  `CREATED` date DEFAULT NULL,
  PRIMARY KEY (`ID`),
  UNIQUE KEY `SITEACCESS_ID_PRIMARY` (`ID`),
  UNIQUE KEY `SITEACCESS_DNSITE_IDX` (`DN`,`PANDASITE`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `siteaccess`
--

LOCK TABLES `siteaccess` WRITE;
/*!40000 ALTER TABLE `siteaccess` DISABLE KEYS */;
/*!40000 ALTER TABLE `siteaccess` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `sitedata`
--

DROP TABLE IF EXISTS `sitedata`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `sitedata` (
  `SITE` varchar(60) NOT NULL,
  `FLAG` varchar(20) NOT NULL,
  `HOURS` int(11) NOT NULL DEFAULT '0',
  `NWN` int(11) DEFAULT NULL,
  `MEMMIN` int(11) DEFAULT NULL,
  `MEMMAX` int(11) DEFAULT NULL,
  `SI2000MIN` int(11) DEFAULT NULL,
  `SI2000MAX` int(11) DEFAULT NULL,
  `OS` varchar(30) DEFAULT NULL,
  `SPACE` varchar(30) DEFAULT NULL,
  `MINJOBS` int(11) DEFAULT NULL,
  `MAXJOBS` int(11) DEFAULT NULL,
  `LASTSTART` datetime DEFAULT NULL,
  `LASTEND` datetime DEFAULT NULL,
  `LASTFAIL` datetime DEFAULT NULL,
  `LASTPILOT` datetime DEFAULT NULL,
  `LASTPID` int(11) DEFAULT NULL,
  `NSTART` int(11) NOT NULL DEFAULT '0',
  `FINISHED` int(11) NOT NULL DEFAULT '0',
  `FAILED` int(11) NOT NULL DEFAULT '0',
  `DEFINED` int(11) NOT NULL DEFAULT '0',
  `ASSIGNED` int(11) NOT NULL DEFAULT '0',
  `WAITING` int(11) NOT NULL DEFAULT '0',
  `ACTIVATED` int(11) NOT NULL DEFAULT '0',
  `HOLDING` int(11) NOT NULL DEFAULT '0',
  `RUNNING` int(11) NOT NULL DEFAULT '0',
  `TRANSFERRING` int(11) NOT NULL DEFAULT '0',
  `GETJOB` int(11) NOT NULL DEFAULT '0',
  `UPDATEJOB` int(11) NOT NULL DEFAULT '0',
  `LASTMOD` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `NCPU` int(11) DEFAULT NULL,
  `NSLOT` int(11) DEFAULT NULL,
  PRIMARY KEY (`SITE`,`FLAG`,`HOURS`),
  UNIQUE KEY `PRIMARY_SITEDATA` (`SITE`,`FLAG`,`HOURS`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `sitedata`
--

LOCK TABLES `sitedata` WRITE;
/*!40000 ALTER TABLE `sitedata` DISABLE KEYS */;
/*!40000 ALTER TABLE `sitedata` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `siteddm`
--

DROP TABLE IF EXISTS `siteddm`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `siteddm` (
  `NAME` varchar(60) NOT NULL,
  `INCMD` varchar(60) NOT NULL,
  `INPATH` varchar(200) DEFAULT NULL,
  `INOPTS` varchar(60) DEFAULT NULL,
  `OUTCMD` varchar(60) NOT NULL,
  `OUTOPTS` varchar(60) DEFAULT NULL,
  `OUTPATH` varchar(200) NOT NULL,
  PRIMARY KEY (`NAME`),
  UNIQUE KEY `PRIMARY_SITEDDM` (`NAME`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `siteddm`
--

LOCK TABLES `siteddm` WRITE;
/*!40000 ALTER TABLE `siteddm` DISABLE KEYS */;
/*!40000 ALTER TABLE `siteddm` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `sitehistory`
--

DROP TABLE IF EXISTS `sitehistory`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `sitehistory` (
  `SITE` varchar(60) NOT NULL,
  `FLAG` varchar(20) NOT NULL,
  `TIME` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `HOURS` int(11) NOT NULL DEFAULT '0',
  `NWN` int(11) DEFAULT NULL,
  `MEMMIN` int(11) DEFAULT NULL,
  `MEMMAX` int(11) DEFAULT NULL,
  `SI2000MIN` int(11) DEFAULT NULL,
  `SI2000MAX` int(11) DEFAULT NULL,
  `SI2000A` int(11) DEFAULT NULL,
  `SI2000P` int(11) DEFAULT NULL,
  `WALLA` int(11) DEFAULT NULL,
  `WALLP` int(11) DEFAULT NULL,
  `OS` varchar(30) NOT NULL,
  `SPACE` varchar(30) NOT NULL,
  `MINJOBS` int(11) DEFAULT NULL,
  `MAXJOBS` int(11) DEFAULT NULL,
  `LASTSTART` datetime DEFAULT NULL,
  `LASTEND` datetime DEFAULT NULL,
  `LASTFAIL` datetime DEFAULT NULL,
  `LASTPILOT` datetime DEFAULT NULL,
  `LASTPID` int(11) DEFAULT NULL,
  `NSTART` int(11) NOT NULL DEFAULT '0',
  `FINISHED` int(11) NOT NULL DEFAULT '0',
  `FAILED` int(11) NOT NULL DEFAULT '0',
  `DEFINED` int(11) NOT NULL DEFAULT '0',
  `ASSIGNED` int(11) NOT NULL DEFAULT '0',
  `WAITING` int(11) NOT NULL DEFAULT '0',
  `ACTIVATED` int(11) NOT NULL DEFAULT '0',
  `RUNNING` int(11) NOT NULL DEFAULT '0',
  `GETJOB` int(11) NOT NULL DEFAULT '0',
  `UPDATEJOB` int(11) NOT NULL DEFAULT '0',
  `SUBTOT` int(11) NOT NULL DEFAULT '0',
  `SUBDEF` int(11) NOT NULL DEFAULT '0',
  `SUBDONE` int(11) NOT NULL DEFAULT '0',
  `FILEMODS` int(11) NOT NULL DEFAULT '0',
  `NCPU` int(11) DEFAULT NULL,
  `NSLOT` int(11) DEFAULT NULL,
  PRIMARY KEY (`SITE`,`FLAG`,`TIME`,`HOURS`),
  UNIQUE KEY `PRIMARY_SITEHIST` (`SITE`,`FLAG`,`TIME`,`HOURS`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `sitehistory`
--

LOCK TABLES `sitehistory` WRITE;
/*!40000 ALTER TABLE `sitehistory` DISABLE KEYS */;
/*!40000 ALTER TABLE `sitehistory` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `sites_matrix_data`
--

DROP TABLE IF EXISTS `sites_matrix_data`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `sites_matrix_data` (
  `SOURCE` varchar(256) NOT NULL,
  `DESTINATION` varchar(256) NOT NULL,
  `MEAS_DATE` datetime NOT NULL,
  `SONARSMLVAL` int(11) DEFAULT NULL,
  `SONARSMLDEV` int(11) DEFAULT NULL,
  `SONARMEDVAL` int(11) DEFAULT NULL,
  `SONARMEDDEV` int(11) DEFAULT NULL,
  `SONARLRGVAL` int(11) DEFAULT NULL,
  `SONARLRGDEV` int(11) DEFAULT NULL,
  `PERFSONARAVGVAL` int(11) DEFAULT NULL,
  `XRDCPVAL` int(11) DEFAULT NULL,
  `SONARSML_LAST_UPDATE` datetime DEFAULT NULL,
  `SONARMED_LAST_UPDATE` datetime DEFAULT NULL,
  `SONARLRG_LAST_UPDATE` datetime DEFAULT NULL,
  `PERFSONARAVG_LAST_UPDATE` datetime DEFAULT NULL,
  `XRDCP_LAST_UPDATE` date DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `sites_matrix_data`
--

LOCK TABLES `sites_matrix_data` WRITE;
/*!40000 ALTER TABLE `sites_matrix_data` DISABLE KEYS */;
/*!40000 ALTER TABLE `sites_matrix_data` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `sitesinfo`
--

DROP TABLE IF EXISTS `sitesinfo`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `sitesinfo` (
  `NAME` varchar(120) NOT NULL,
  `NICK` varchar(20) NOT NULL,
  `CONTACT` varchar(30) DEFAULT NULL,
  `EMAIL` varchar(30) DEFAULT NULL,
  `STATUS` varchar(12) DEFAULT NULL,
  `LRC` varchar(120) DEFAULT NULL,
  `GRIDCAT` int(11) DEFAULT NULL,
  `MONALISA` varchar(20) DEFAULT NULL,
  `COMPUTINGSITE` varchar(20) DEFAULT NULL,
  `MAINSITE` varchar(20) DEFAULT NULL,
  `HOME` varchar(120) DEFAULT NULL,
  `GANGLIA` varchar(120) DEFAULT NULL,
  `GOC` varchar(20) DEFAULT NULL,
  `GOCCONFIG` tinyint(4) DEFAULT NULL,
  `PRODSYS` varchar(20) DEFAULT NULL,
  `DQ2SVC` varchar(20) DEFAULT NULL,
  `USAGE` varchar(40) DEFAULT NULL,
  `UPDTIME` int(11) DEFAULT NULL,
  `NDATASETS` int(11) DEFAULT NULL,
  `NFILES` int(11) DEFAULT NULL,
  `TIMESTAMP` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `sitesinfo`
--

LOCK TABLES `sitesinfo` WRITE;
/*!40000 ALTER TABLE `sitesinfo` DISABLE KEYS */;
/*!40000 ALTER TABLE `sitesinfo` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `sitestats`
--

DROP TABLE IF EXISTS `sitestats`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `sitestats` (
  `CLOUD` varchar(10) DEFAULT NULL,
  `SITE` varchar(60) DEFAULT NULL,
  `AT_TIME` datetime DEFAULT NULL,
  `TWIDTH` int(11) DEFAULT NULL,
  `TJOB` int(11) DEFAULT NULL,
  `TGETJOB` int(11) DEFAULT NULL,
  `TSTAGEIN` int(11) DEFAULT NULL,
  `TRUN` int(11) DEFAULT NULL,
  `TSTAGEOUT` int(11) DEFAULT NULL,
  `TWAIT` int(11) DEFAULT NULL,
  `NUSERS` int(11) DEFAULT NULL,
  `NWN` int(11) DEFAULT NULL,
  `NJOBS` int(11) DEFAULT NULL,
  `NFINISHED` int(11) DEFAULT NULL,
  `NFAILED` int(11) DEFAULT NULL,
  `NFAILAPP` int(11) DEFAULT NULL,
  `NFAILSYS` int(11) DEFAULT NULL,
  `NFAILDAT` int(11) DEFAULT NULL,
  `NTIMEOUT` int(11) DEFAULT NULL,
  `EFFICIENCY` int(11) DEFAULT NULL,
  `SITEUTIL` int(11) DEFAULT NULL,
  `JOBTYPE` varchar(30) DEFAULT NULL,
  `PROCTYPE` varchar(90) DEFAULT NULL,
  `USERNAME` varchar(90) DEFAULT NULL,
  `NGETJOB` int(11) DEFAULT NULL,
  `NUPDATEJOB` int(11) DEFAULT NULL,
  `RELEASE` varchar(90) DEFAULT NULL,
  `NEVENTS` bigint(20) DEFAULT NULL,
  `SPECTYPE` varchar(90) DEFAULT NULL,
  `TSETUP` int(11) DEFAULT '-1',
  KEY `SITESTATS_TIME_INDX` (`AT_TIME`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `sitestats`
--

LOCK TABLES `sitestats` WRITE;
/*!40000 ALTER TABLE `sitestats` DISABLE KEYS */;
/*!40000 ALTER TABLE `sitestats` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `subcounter_subid_seq`
--

DROP TABLE IF EXISTS `subcounter_subid_seq`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `subcounter_subid_seq` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `col` bit(1) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=130 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `subcounter_subid_seq`
--

LOCK TABLES `subcounter_subid_seq` WRITE;
/*!40000 ALTER TABLE `subcounter_subid_seq` DISABLE KEYS */;
/*!40000 ALTER TABLE `subcounter_subid_seq` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `submithosts`
--

DROP TABLE IF EXISTS `submithosts`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `submithosts` (
  `NAME` varchar(60) NOT NULL,
  `NICKNAME` varchar(20) NOT NULL,
  `HOST` varchar(60) NOT NULL,
  `SYSTEM` varchar(60) NOT NULL,
  `RUNDIR` varchar(200) NOT NULL,
  `RUNURL` varchar(200) NOT NULL,
  `JDLTXT` varchar(4000) DEFAULT NULL,
  `PILOTQUEUE` varchar(20) DEFAULT NULL,
  `OUTURL` varchar(200) DEFAULT NULL,
  PRIMARY KEY (`HOST`),
  UNIQUE KEY `PRIMARY_SUBMITHOST` (`HOST`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `submithosts`
--

LOCK TABLES `submithosts` WRITE;
/*!40000 ALTER TABLE `submithosts` DISABLE KEYS */;
/*!40000 ALTER TABLE `submithosts` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `sysconfig`
--

DROP TABLE IF EXISTS `sysconfig`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `sysconfig` (
  `NAME` varchar(60) NOT NULL,
  `SYSTEM` varchar(20) NOT NULL,
  `CONFIG` varchar(4000) DEFAULT NULL,
  PRIMARY KEY (`NAME`,`SYSTEM`),
  UNIQUE KEY `PRIMARY_SYSCFG` (`NAME`,`SYSTEM`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `sysconfig`
--

LOCK TABLES `sysconfig` WRITE;
/*!40000 ALTER TABLE `sysconfig` DISABLE KEYS */;
/*!40000 ALTER TABLE `sysconfig` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `t_m4regions_replication`
--

DROP TABLE IF EXISTS `t_m4regions_replication`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `t_m4regions_replication` (
  `TIER2` varchar(50) NOT NULL,
  `CLOUD` varchar(30) NOT NULL,
  `PERCENTAGE` double DEFAULT NULL,
  `TIER1` varchar(50) NOT NULL,
  `NSUBS` int(11) DEFAULT NULL,
  `SUBSOPTION` varchar(320) DEFAULT NULL,
  `STATUS` varchar(12) DEFAULT NULL,
  `TIMESTAMP` int(11) DEFAULT NULL,
  `STREAM_PATTERN` varchar(32) DEFAULT NULL,
  `NREPLICAS` int(11) DEFAULT NULL,
  `NSUBS_AOD` int(11) DEFAULT NULL,
  `NSUBS_DPD` int(11) DEFAULT NULL,
  `UPD_FLAG` varchar(4) DEFAULT NULL,
  `ESD` int(11) DEFAULT NULL,
  `ESD_SUBSOPTION` varchar(320) DEFAULT NULL,
  `DESD` int(11) DEFAULT NULL,
  `DESD_SUBSOPTION` varchar(320) DEFAULT NULL,
  `PRIM_FLAG` smallint(6) DEFAULT NULL,
  `T2GROUP` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`TIER2`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `t_m4regions_replication`
--

LOCK TABLES `t_m4regions_replication` WRITE;
/*!40000 ALTER TABLE `t_m4regions_replication` DISABLE KEYS */;
/*!40000 ALTER TABLE `t_m4regions_replication` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `table_part_boundaries`
--

DROP TABLE IF EXISTS `table_part_boundaries`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `table_part_boundaries` (
  `TAB_NAME` varchar(30) NOT NULL,
  `PART_NAME` varchar(30) NOT NULL,
  `PART_POSITION` int(11) DEFAULT NULL,
  `PART_BOUNDARY_DICTIONARY` text,
  `PART_BOUNDARY` datetime DEFAULT NULL,
  `LAST_CREATED_VIEW` varchar(30) DEFAULT NULL,
  PRIMARY KEY (`TAB_NAME`,`PART_NAME`),
  UNIQUE KEY `TABLE_PART_BOUNDARIES_PK` (`TAB_NAME`,`PART_NAME`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `table_part_boundaries`
--

LOCK TABLES `table_part_boundaries` WRITE;
/*!40000 ALTER TABLE `table_part_boundaries` DISABLE KEYS */;
/*!40000 ALTER TABLE `table_part_boundaries` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `tablepart4copying`
--

DROP TABLE IF EXISTS `tablepart4copying`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `tablepart4copying` (
  `TABLE_NAME` varchar(30) NOT NULL,
  `PARTITION_NAME` varchar(30) NOT NULL,
  `COPIED_TO_ARCH` varchar(10) NOT NULL,
  `COPYING_DONE_ON` datetime DEFAULT NULL,
  `DELETED_ON` datetime DEFAULT NULL,
  `DATA_VERIF_PASSED` char(3) DEFAULT NULL,
  `DATA_VERIFIED_ON` date DEFAULT NULL,
  PRIMARY KEY (`TABLE_NAME`,`PARTITION_NAME`),
  UNIQUE KEY `TABLEPART4COPYING_PK` (`TABLE_NAME`,`PARTITION_NAME`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `tablepart4copying`
--

LOCK TABLES `tablepart4copying` WRITE;
/*!40000 ALTER TABLE `tablepart4copying` DISABLE KEYS */;
/*!40000 ALTER TABLE `tablepart4copying` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `taginfo`
--

DROP TABLE IF EXISTS `taginfo`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `taginfo` (
  `TAG` varchar(30) NOT NULL,
  `DESCRIPTION` varchar(100) NOT NULL,
  `NQUEUES` int(11) NOT NULL DEFAULT '0',
  `QUEUES` varchar(4000) DEFAULT NULL,
  PRIMARY KEY (`TAG`),
  UNIQUE KEY `PRIMARY_TAGINFO` (`TAG`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `taginfo`
--

LOCK TABLES `taginfo` WRITE;
/*!40000 ALTER TABLE `taginfo` DISABLE KEYS */;
/*!40000 ALTER TABLE `taginfo` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `tags`
--

DROP TABLE IF EXISTS `tags`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `tags` (
  `ID` int(11) NOT NULL,
  `NAME` varchar(20) NOT NULL,
  `DESCRIPTION` varchar(60) NOT NULL,
  `UGID` int(11) DEFAULT NULL,
  `TYPE` varchar(10) NOT NULL,
  `ITEMID` int(11) DEFAULT NULL,
  `CREATED` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`ID`),
  UNIQUE KEY `PRIMARY_TAGS` (`ID`),
  KEY `TAGS_NAME_IDX` (`NAME`,`UGID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `tags`
--

LOCK TABLES `tags` WRITE;
/*!40000 ALTER TABLE `tags` DISABLE KEYS */;
/*!40000 ALTER TABLE `tags` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `usagereport`
--

DROP TABLE IF EXISTS `usagereport`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `usagereport` (
  `ENTRY` int(11) NOT NULL,
  `FLAG` varchar(20) NOT NULL,
  `HOURS` int(11) DEFAULT NULL,
  `TSTART` datetime DEFAULT NULL,
  `TEND` datetime DEFAULT NULL,
  `TINSERT` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `SITE` varchar(60) NOT NULL,
  `NWN` int(11) DEFAULT NULL,
  PRIMARY KEY (`ENTRY`),
  UNIQUE KEY `PRIMARY_USAGEREPORT` (`ENTRY`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `usagereport`
--

LOCK TABLES `usagereport` WRITE;
/*!40000 ALTER TABLE `usagereport` DISABLE KEYS */;
/*!40000 ALTER TABLE `usagereport` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `usercacheusage`
--

DROP TABLE IF EXISTS `usercacheusage`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `usercacheusage` (
  `USERNAME` varchar(128) NOT NULL,
  `FILENAME` varchar(256) NOT NULL,
  `HOSTNAME` varchar(64) NOT NULL,
  `CREATIONTIME` datetime NOT NULL,
  `MODIFICATIONTIME` datetime DEFAULT NULL,
  `FILESIZE` bigint(20) DEFAULT NULL,
  `CHECKSUM` varchar(36) DEFAULT NULL,
  `ALIASNAME` varchar(256) DEFAULT NULL,
  KEY `USERCACHEUSAGE_USR_CRDATE_INDX` (`USERNAME`,`CREATIONTIME`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `usercacheusage`
--

LOCK TABLES `usercacheusage` WRITE;
/*!40000 ALTER TABLE `usercacheusage` DISABLE KEYS */;
/*!40000 ALTER TABLE `usercacheusage` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `users`
--

DROP TABLE IF EXISTS `users`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `users` (
  `ID` int(11) NOT NULL,
  `NAME` varchar(60) NOT NULL,
  `DN` varchar(150) DEFAULT NULL,
  `EMAIL` varchar(60) DEFAULT NULL,
  `URL` varchar(100) DEFAULT NULL,
  `LOCATION` varchar(60) DEFAULT NULL,
  `CLASSA` varchar(30) DEFAULT NULL,
  `CLASSP` varchar(30) DEFAULT NULL,
  `CLASSXP` varchar(30) DEFAULT NULL,
  `SITEPREF` varchar(60) DEFAULT NULL,
  `GRIDPREF` varchar(20) DEFAULT NULL,
  `QUEUEPREF` varchar(60) DEFAULT NULL,
  `SCRIPTCACHE` varchar(100) DEFAULT NULL,
  `TYPES` varchar(60) DEFAULT NULL,
  `SITES` varchar(250) DEFAULT NULL,
  `NJOBSA` int(11) DEFAULT NULL,
  `NJOBSP` int(11) DEFAULT NULL,
  `NJOBS1` int(11) DEFAULT NULL,
  `NJOBS7` int(11) DEFAULT NULL,
  `NJOBS30` int(11) DEFAULT NULL,
  `CPUA1` bigint(20) DEFAULT NULL,
  `CPUA7` bigint(20) DEFAULT NULL,
  `CPUA30` bigint(20) DEFAULT NULL,
  `CPUP1` bigint(20) DEFAULT NULL,
  `CPUP7` bigint(20) DEFAULT NULL,
  `CPUP30` bigint(20) DEFAULT NULL,
  `CPUXP1` bigint(20) DEFAULT NULL,
  `CPUXP7` bigint(20) DEFAULT NULL,
  `CPUXP30` bigint(20) DEFAULT NULL,
  `QUOTAA1` bigint(20) DEFAULT NULL,
  `QUOTAA7` bigint(20) DEFAULT NULL,
  `QUOTAA30` bigint(20) DEFAULT NULL,
  `QUOTAP1` bigint(20) DEFAULT NULL,
  `QUOTAP7` bigint(20) DEFAULT NULL,
  `QUOTAP30` bigint(20) DEFAULT NULL,
  `QUOTAXP1` bigint(20) DEFAULT NULL,
  `QUOTAXP7` bigint(20) DEFAULT NULL,
  `QUOTAXP30` bigint(20) DEFAULT NULL,
  `SPACE1` int(11) DEFAULT NULL,
  `SPACE7` int(11) DEFAULT NULL,
  `SPACE30` int(11) DEFAULT NULL,
  `LASTMOD` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `FIRSTJOB` datetime NOT NULL DEFAULT '1970-01-01 00:00:00',
  `LATESTJOB` datetime NOT NULL DEFAULT '1970-01-01 00:00:00',
  `PAGECACHE` text,
  `CACHETIME` datetime NOT NULL DEFAULT '1970-01-01 00:00:00',
  `NCURRENT` int(11) NOT NULL DEFAULT '0',
  `JOBID` int(11) NOT NULL DEFAULT '0',
  `STATUS` varchar(20) DEFAULT NULL,
  `VO` varchar(20) DEFAULT NULL,
  PRIMARY KEY (`ID`),
  UNIQUE KEY `PRIMARY_USERS` (`ID`),
  KEY `USERS_NAME_IDX` (`NAME`,`VO`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `users`
--

LOCK TABLES `users` WRITE;
/*!40000 ALTER TABLE `users` DISABLE KEYS */;
/*!40000 ALTER TABLE `users` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `usersubs`
--

DROP TABLE IF EXISTS `usersubs`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `usersubs` (
  `DATASETNAME` varchar(255) NOT NULL,
  `SITE` varchar(64) NOT NULL,
  `CREATIONDATE` datetime DEFAULT NULL,
  `MODIFICATIONDATE` datetime DEFAULT NULL,
  `NUSED` int(11) DEFAULT NULL,
  `STATE` varchar(30) DEFAULT 'subscribed',
  PRIMARY KEY (`DATASETNAME`,`SITE`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `usersubs`
--

LOCK TABLES `usersubs` WRITE;
/*!40000 ALTER TABLE `usersubs` DISABLE KEYS */;
/*!40000 ALTER TABLE `usersubs` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `vo_to_site`
--

DROP TABLE IF EXISTS `vo_to_site`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `vo_to_site` (
  `SITE_NAME` varchar(32) NOT NULL,
  `QUEUE` varchar(64) NOT NULL,
  `VO_NAME` varchar(32) NOT NULL,
  PRIMARY KEY (`SITE_NAME`,`QUEUE`,`VO_NAME`),
  UNIQUE KEY `PRIMARY_VOTOSITE` (`SITE_NAME`,`QUEUE`,`VO_NAME`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `vo_to_site`
--

LOCK TABLES `vo_to_site` WRITE;
/*!40000 ALTER TABLE `vo_to_site` DISABLE KEYS */;
/*!40000 ALTER TABLE `vo_to_site` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `vorspassfail`
--

DROP TABLE IF EXISTS `vorspassfail`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `vorspassfail` (
  `SITE_NAME` varchar(32) NOT NULL,
  `PASSFAIL` char(4) NOT NULL,
  `LAST_CHECKED` date DEFAULT NULL,
  PRIMARY KEY (`SITE_NAME`),
  UNIQUE KEY `PRIMARY_VORSPASSFAIL` (`SITE_NAME`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `vorspassfail`
--

LOCK TABLES `vorspassfail` WRITE;
/*!40000 ALTER TABLE `vorspassfail` DISABLE KEYS */;
/*!40000 ALTER TABLE `vorspassfail` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `wndata`
--

DROP TABLE IF EXISTS `wndata`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `wndata` (
  `SITE` varchar(60) NOT NULL,
  `WN` varchar(50) NOT NULL,
  `FLAG` varchar(20) NOT NULL,
  `HOURS` int(11) NOT NULL DEFAULT '0',
  `MEM` int(11) DEFAULT NULL,
  `SI2000` int(11) DEFAULT NULL,
  `OS` varchar(30) DEFAULT NULL,
  `SPACE` varchar(30) DEFAULT NULL,
  `MAXJOBS` int(11) DEFAULT NULL,
  `LASTSTART` datetime DEFAULT NULL,
  `LASTEND` datetime DEFAULT NULL,
  `LASTFAIL` datetime DEFAULT NULL,
  `LASTPILOT` datetime DEFAULT NULL,
  `LASTPID` int(11) DEFAULT NULL,
  `NSTART` int(11) NOT NULL DEFAULT '0',
  `FINISHED` int(11) NOT NULL DEFAULT '0',
  `FAILED` int(11) NOT NULL DEFAULT '0',
  `HOLDING` int(11) NOT NULL DEFAULT '0',
  `RUNNING` int(11) NOT NULL DEFAULT '0',
  `TRANSFERRING` int(11) NOT NULL DEFAULT '0',
  `GETJOB` int(11) NOT NULL DEFAULT '0',
  `UPDATEJOB` int(11) NOT NULL DEFAULT '0',
  `LASTMOD` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `NCPU` int(11) DEFAULT NULL,
  `NCPUCURRENT` int(11) DEFAULT NULL,
  `NSLOT` int(11) DEFAULT NULL,
  `NSLOTCURRENT` int(11) DEFAULT NULL,
  PRIMARY KEY (`SITE`,`WN`,`FLAG`,`HOURS`),
  UNIQUE KEY `PRIMARY_WNDATA` (`SITE`,`WN`,`FLAG`,`HOURS`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `wndata`
--

LOCK TABLES `wndata` WRITE;
/*!40000 ALTER TABLE `wndata` DISABLE KEYS */;
/*!40000 ALTER TABLE `wndata` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `y2006_filestable_arch`
--

DROP TABLE IF EXISTS `y2006_filestable_arch`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `y2006_filestable_arch` (
  `ROW_ID` bigint(20) NOT NULL,
  `PANDAID` bigint(20) NOT NULL,
  `MODIFICATIONTIME` datetime NOT NULL,
  `CREATIONTIME` datetime DEFAULT NULL,
  `GUID` varchar(64) DEFAULT NULL,
  `LFN` varchar(256) DEFAULT NULL,
  `TYPE` varchar(20) DEFAULT NULL,
  `FSIZE` bigint(20) DEFAULT NULL,
  `DATASET` varchar(255) DEFAULT NULL,
  `STATUS` varchar(64) DEFAULT NULL,
  `MD5SUM` varchar(40) DEFAULT NULL,
  `CHECKSUM` varchar(40) DEFAULT NULL,
  `PRODDBLOCK` varchar(255) DEFAULT NULL,
  `PRODDBLOCKTOKEN` varchar(250) DEFAULT NULL,
  `DISPATCHDBLOCK` varchar(255) DEFAULT NULL,
  `DISPATCHDBLOCKTOKEN` varchar(250) DEFAULT NULL,
  `DESTINATIONDBLOCK` varchar(255) DEFAULT NULL,
  `DESTINATIONDBLOCKTOKEN` varchar(250) DEFAULT NULL,
  `DESTINATIONSE` varchar(250) DEFAULT NULL,
  `SCOPE` varchar(30) DEFAULT NULL,
  `JEDITASKID` bigint(20) DEFAULT NULL,
  `DATASETID` bigint(20) DEFAULT NULL,
  `FILEID` bigint(20) DEFAULT NULL,
  `ATTEMPTNR` tinyint(4) DEFAULT NULL,
  KEY `F2006_PANDAID_IDX` (`PANDAID`),
  KEY `F2006_ARCH_ROWID_IDX` (`ROW_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `y2006_filestable_arch`
--

LOCK TABLES `y2006_filestable_arch` WRITE;
/*!40000 ALTER TABLE `y2006_filestable_arch` DISABLE KEYS */;
/*!40000 ALTER TABLE `y2006_filestable_arch` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `y2006_jobparamstable_arch`
--

DROP TABLE IF EXISTS `y2006_jobparamstable_arch`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `y2006_jobparamstable_arch` (
  `PANDAID` bigint(20) NOT NULL,
  `MODIFICATIONTIME` datetime NOT NULL,
  `JOBPARAMETERS` text,
  KEY `JOBPARAMS2006_ARCH_PANDAID_IDX` (`PANDAID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `y2006_jobparamstable_arch`
--

LOCK TABLES `y2006_jobparamstable_arch` WRITE;
/*!40000 ALTER TABLE `y2006_jobparamstable_arch` DISABLE KEYS */;
/*!40000 ALTER TABLE `y2006_jobparamstable_arch` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `y2006_jobsarchived`
--

DROP TABLE IF EXISTS `y2006_jobsarchived`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `y2006_jobsarchived` (
  `PANDAID` bigint(20) NOT NULL,
  `JOBDEFINITIONID` bigint(20) NOT NULL,
  `SCHEDULERID` varchar(128) DEFAULT NULL,
  `PILOTID` varchar(200) DEFAULT NULL,
  `CREATIONTIME` datetime NOT NULL,
  `CREATIONHOST` varchar(128) DEFAULT NULL,
  `MODIFICATIONTIME` datetime NOT NULL,
  `MODIFICATIONHOST` varchar(128) DEFAULT NULL,
  `ATLASRELEASE` varchar(64) DEFAULT NULL,
  `TRANSFORMATION` varchar(250) DEFAULT NULL,
  `HOMEPACKAGE` varchar(80) DEFAULT NULL,
  `PRODSERIESLABEL` varchar(20) DEFAULT NULL,
  `PRODSOURCELABEL` varchar(20) DEFAULT NULL,
  `PRODUSERID` varchar(250) DEFAULT NULL,
  `ASSIGNEDPRIORITY` int(11) NOT NULL,
  `CURRENTPRIORITY` int(11) NOT NULL,
  `ATTEMPTNR` tinyint(4) NOT NULL,
  `MAXATTEMPT` tinyint(4) NOT NULL,
  `JOBSTATUS` varchar(15) NOT NULL,
  `JOBNAME` varchar(256) DEFAULT NULL,
  `MAXCPUCOUNT` int(11) NOT NULL,
  `MAXCPUUNIT` varchar(32) DEFAULT NULL,
  `MAXDISKCOUNT` int(11) NOT NULL,
  `MAXDISKUNIT` char(4) DEFAULT NULL,
  `IPCONNECTIVITY` char(5) DEFAULT NULL,
  `MINRAMCOUNT` int(11) NOT NULL,
  `MINRAMUNIT` char(4) DEFAULT NULL,
  `STARTTIME` datetime DEFAULT NULL,
  `ENDTIME` datetime DEFAULT NULL,
  `CPUCONSUMPTIONTIME` bigint(20) DEFAULT NULL,
  `CPUCONSUMPTIONUNIT` varchar(128) DEFAULT NULL,
  `COMMANDTOPILOT` varchar(250) DEFAULT NULL,
  `TRANSEXITCODE` varchar(128) DEFAULT NULL,
  `PILOTERRORCODE` int(11) NOT NULL,
  `PILOTERRORDIAG` varchar(500) DEFAULT NULL,
  `EXEERRORCODE` int(11) NOT NULL,
  `EXEERRORDIAG` varchar(500) DEFAULT NULL,
  `SUPERRORCODE` int(11) NOT NULL,
  `SUPERRORDIAG` varchar(250) DEFAULT NULL,
  `DDMERRORCODE` int(11) NOT NULL,
  `DDMERRORDIAG` varchar(700) DEFAULT NULL,
  `BROKERAGEERRORCODE` int(11) NOT NULL,
  `BROKERAGEERRORDIAG` varchar(250) DEFAULT NULL,
  `JOBDISPATCHERERRORCODE` int(11) NOT NULL,
  `JOBDISPATCHERERRORDIAG` varchar(250) DEFAULT NULL,
  `TASKBUFFERERRORCODE` int(11) NOT NULL,
  `TASKBUFFERERRORDIAG` varchar(300) DEFAULT NULL,
  `COMPUTINGSITE` varchar(128) DEFAULT NULL,
  `COMPUTINGELEMENT` varchar(128) DEFAULT NULL,
  `PRODDBLOCK` varchar(255) DEFAULT NULL,
  `DISPATCHDBLOCK` varchar(255) DEFAULT NULL,
  `DESTINATIONDBLOCK` varchar(255) DEFAULT NULL,
  `DESTINATIONSE` varchar(250) DEFAULT NULL,
  `NEVENTS` int(11) DEFAULT NULL,
  `GRID` varchar(50) DEFAULT NULL,
  `CLOUD` varchar(50) DEFAULT NULL,
  `CPUCONVERSION` decimal(9,4) DEFAULT NULL,
  `SOURCESITE` varchar(36) DEFAULT NULL,
  `DESTINATIONSITE` varchar(36) DEFAULT NULL,
  `TRANSFERTYPE` varchar(10) DEFAULT NULL,
  `TASKID` int(11) DEFAULT NULL,
  `CMTCONFIG` varchar(250) DEFAULT NULL,
  `STATECHANGETIME` datetime DEFAULT NULL,
  `PRODDBUPDATETIME` datetime DEFAULT NULL,
  `LOCKEDBY` varchar(128) DEFAULT NULL,
  `RELOCATIONFLAG` tinyint(4) DEFAULT NULL,
  `JOBEXECUTIONID` bigint(20) DEFAULT NULL,
  `VO` varchar(16) DEFAULT NULL,
  `PILOTTIMING` varchar(100) DEFAULT NULL,
  `WORKINGGROUP` varchar(20) DEFAULT NULL,
  `PROCESSINGTYPE` varchar(64) DEFAULT NULL,
  `JOBPARAMETERS` text,
  `METADATA` text,
  `PRODUSERNAME` varchar(60) DEFAULT NULL,
  `NINPUTFILES` int(11) DEFAULT NULL,
  `COUNTRYGROUP` varchar(20) DEFAULT NULL,
  `BATCHID` varchar(80) DEFAULT NULL,
  `PARENTID` bigint(20) DEFAULT NULL,
  `SPECIALHANDLING` varchar(80) DEFAULT NULL,
  `JOBSETID` bigint(20) DEFAULT NULL,
  `CORECOUNT` tinyint(4) DEFAULT NULL,
  `NINPUTDATAFILES` int(11) DEFAULT NULL,
  `INPUTFILETYPE` varchar(32) DEFAULT NULL,
  `INPUTFILEPROJECT` varchar(64) DEFAULT NULL,
  `INPUTFILEBYTES` bigint(20) DEFAULT NULL,
  `NOUTPUTDATAFILES` int(11) DEFAULT NULL,
  `OUTPUTFILEBYTES` bigint(20) DEFAULT NULL,
  `JOBMETRICS` varchar(500) DEFAULT NULL,
  `WORKQUEUE_ID` int(11) DEFAULT NULL,
  `JEDITASKID` bigint(20) DEFAULT NULL,
  `JOBSUBSTATUS` varchar(80) DEFAULT NULL,
  `ACTUALCORECOUNT` int(11) DEFAULT NULL,
  KEY `J2006_SITE_3ATTR_PANDAID_IDX` (`COMPUTINGSITE`,`JOBSTATUS`,`PRODSOURCELABEL`,`PROCESSINGTYPE`,`PANDAID`),
  KEY `J2006_JOBDEFID_PRODUSRNAME_IDX` (`JOBDEFINITIONID`,`PRODUSERNAME`),
  KEY `J2006_JOBSETID_PRODUSRNAME_IDX` (`JOBSETID`,`PRODUSERNAME`),
  KEY `J2006_PANDAID_IDX` (`PANDAID`),
  KEY `J2006_PRODUSRNAME_4ATTR_IDX` (`PRODUSERNAME`,`JOBSTATUS`,`PRODSOURCELABEL`,`JOBSETID`,`JOBDEFINITIONID`),
  KEY `J2006_TASKID_3ATTR_PANDAID_IDX` (`TASKID`,`PRODSOURCELABEL`,`JOBSTATUS`,`PROCESSINGTYPE`,`PANDAID`),
  KEY `J2006_JEDITASKID_IDX` (`JEDITASKID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `y2006_jobsarchived`
--

LOCK TABLES `y2006_jobsarchived` WRITE;
/*!40000 ALTER TABLE `y2006_jobsarchived` DISABLE KEYS */;
/*!40000 ALTER TABLE `y2006_jobsarchived` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `y2006_metatable_arch`
--

DROP TABLE IF EXISTS `y2006_metatable_arch`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `y2006_metatable_arch` (
  `PANDAID` bigint(20) NOT NULL,
  `MODIFICATIONTIME` datetime NOT NULL,
  `METADATA` text,
  KEY `META2006_PANDAID_IDX` (`PANDAID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `y2006_metatable_arch`
--

LOCK TABLES `y2006_metatable_arch` WRITE;
/*!40000 ALTER TABLE `y2006_metatable_arch` DISABLE KEYS */;
/*!40000 ALTER TABLE `y2006_metatable_arch` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `y2007_filestable_arch`
--

DROP TABLE IF EXISTS `y2007_filestable_arch`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `y2007_filestable_arch` (
  `ROW_ID` bigint(20) NOT NULL,
  `PANDAID` bigint(20) NOT NULL,
  `MODIFICATIONTIME` datetime NOT NULL,
  `CREATIONTIME` datetime DEFAULT NULL,
  `GUID` varchar(64) DEFAULT NULL,
  `LFN` varchar(256) DEFAULT NULL,
  `TYPE` varchar(20) DEFAULT NULL,
  `FSIZE` bigint(20) DEFAULT NULL,
  `DATASET` varchar(255) DEFAULT NULL,
  `STATUS` varchar(64) DEFAULT NULL,
  `MD5SUM` varchar(40) DEFAULT NULL,
  `CHECKSUM` varchar(40) DEFAULT NULL,
  `PRODDBLOCK` varchar(255) DEFAULT NULL,
  `PRODDBLOCKTOKEN` varchar(250) DEFAULT NULL,
  `DISPATCHDBLOCK` varchar(255) DEFAULT NULL,
  `DISPATCHDBLOCKTOKEN` varchar(250) DEFAULT NULL,
  `DESTINATIONDBLOCK` varchar(255) DEFAULT NULL,
  `DESTINATIONDBLOCKTOKEN` varchar(250) DEFAULT NULL,
  `DESTINATIONSE` varchar(250) DEFAULT NULL,
  `SCOPE` varchar(30) DEFAULT NULL,
  `JEDITASKID` bigint(20) DEFAULT NULL,
  `DATASETID` bigint(20) DEFAULT NULL,
  `FILEID` bigint(20) DEFAULT NULL,
  `ATTEMPTNR` tinyint(4) DEFAULT NULL,
  KEY `F2007_PANDAID_IDX` (`PANDAID`),
  KEY `F2007_ARCH_ROWID_IDX` (`ROW_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `y2007_filestable_arch`
--

LOCK TABLES `y2007_filestable_arch` WRITE;
/*!40000 ALTER TABLE `y2007_filestable_arch` DISABLE KEYS */;
/*!40000 ALTER TABLE `y2007_filestable_arch` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `y2007_jobparamstable_arch`
--

DROP TABLE IF EXISTS `y2007_jobparamstable_arch`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `y2007_jobparamstable_arch` (
  `PANDAID` bigint(20) NOT NULL,
  `MODIFICATIONTIME` datetime NOT NULL,
  `JOBPARAMETERS` text,
  KEY `JOBPARAMS2007_ARCH_PANDAID_IDX` (`PANDAID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `y2007_jobparamstable_arch`
--

LOCK TABLES `y2007_jobparamstable_arch` WRITE;
/*!40000 ALTER TABLE `y2007_jobparamstable_arch` DISABLE KEYS */;
/*!40000 ALTER TABLE `y2007_jobparamstable_arch` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `y2007_jobsarchived`
--

DROP TABLE IF EXISTS `y2007_jobsarchived`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `y2007_jobsarchived` (
  `PANDAID` bigint(20) NOT NULL,
  `JOBDEFINITIONID` bigint(20) NOT NULL,
  `SCHEDULERID` varchar(128) DEFAULT NULL,
  `PILOTID` varchar(200) DEFAULT NULL,
  `CREATIONTIME` datetime NOT NULL,
  `CREATIONHOST` varchar(128) DEFAULT NULL,
  `MODIFICATIONTIME` datetime NOT NULL,
  `MODIFICATIONHOST` varchar(128) DEFAULT NULL,
  `ATLASRELEASE` varchar(64) DEFAULT NULL,
  `TRANSFORMATION` varchar(250) DEFAULT NULL,
  `HOMEPACKAGE` varchar(80) DEFAULT NULL,
  `PRODSERIESLABEL` varchar(20) DEFAULT NULL,
  `PRODSOURCELABEL` varchar(20) DEFAULT NULL,
  `PRODUSERID` varchar(250) DEFAULT NULL,
  `ASSIGNEDPRIORITY` int(11) NOT NULL,
  `CURRENTPRIORITY` int(11) NOT NULL,
  `ATTEMPTNR` tinyint(4) NOT NULL,
  `MAXATTEMPT` tinyint(4) NOT NULL,
  `JOBSTATUS` varchar(15) NOT NULL,
  `JOBNAME` varchar(256) DEFAULT NULL,
  `MAXCPUCOUNT` int(11) NOT NULL,
  `MAXCPUUNIT` varchar(32) DEFAULT NULL,
  `MAXDISKCOUNT` int(11) NOT NULL,
  `MAXDISKUNIT` char(4) DEFAULT NULL,
  `IPCONNECTIVITY` char(5) DEFAULT NULL,
  `MINRAMCOUNT` int(11) NOT NULL,
  `MINRAMUNIT` char(4) DEFAULT NULL,
  `STARTTIME` datetime DEFAULT NULL,
  `ENDTIME` datetime DEFAULT NULL,
  `CPUCONSUMPTIONTIME` bigint(20) DEFAULT NULL,
  `CPUCONSUMPTIONUNIT` varchar(128) DEFAULT NULL,
  `COMMANDTOPILOT` varchar(250) DEFAULT NULL,
  `TRANSEXITCODE` varchar(128) DEFAULT NULL,
  `PILOTERRORCODE` int(11) NOT NULL,
  `PILOTERRORDIAG` varchar(500) DEFAULT NULL,
  `EXEERRORCODE` int(11) NOT NULL,
  `EXEERRORDIAG` varchar(500) DEFAULT NULL,
  `SUPERRORCODE` int(11) NOT NULL,
  `SUPERRORDIAG` varchar(250) DEFAULT NULL,
  `DDMERRORCODE` int(11) NOT NULL,
  `DDMERRORDIAG` varchar(700) DEFAULT NULL,
  `BROKERAGEERRORCODE` int(11) NOT NULL,
  `BROKERAGEERRORDIAG` varchar(250) DEFAULT NULL,
  `JOBDISPATCHERERRORCODE` int(11) NOT NULL,
  `JOBDISPATCHERERRORDIAG` varchar(250) DEFAULT NULL,
  `TASKBUFFERERRORCODE` int(11) NOT NULL,
  `TASKBUFFERERRORDIAG` varchar(300) DEFAULT NULL,
  `COMPUTINGSITE` varchar(128) DEFAULT NULL,
  `COMPUTINGELEMENT` varchar(128) DEFAULT NULL,
  `PRODDBLOCK` varchar(255) DEFAULT NULL,
  `DISPATCHDBLOCK` varchar(255) DEFAULT NULL,
  `DESTINATIONDBLOCK` varchar(255) DEFAULT NULL,
  `DESTINATIONSE` varchar(250) DEFAULT NULL,
  `NEVENTS` int(11) DEFAULT NULL,
  `GRID` varchar(50) DEFAULT NULL,
  `CLOUD` varchar(50) DEFAULT NULL,
  `CPUCONVERSION` decimal(9,4) DEFAULT NULL,
  `SOURCESITE` varchar(36) DEFAULT NULL,
  `DESTINATIONSITE` varchar(36) DEFAULT NULL,
  `TRANSFERTYPE` varchar(10) DEFAULT NULL,
  `TASKID` int(11) DEFAULT NULL,
  `CMTCONFIG` varchar(250) DEFAULT NULL,
  `STATECHANGETIME` datetime DEFAULT NULL,
  `PRODDBUPDATETIME` datetime DEFAULT NULL,
  `LOCKEDBY` varchar(128) DEFAULT NULL,
  `RELOCATIONFLAG` tinyint(4) DEFAULT NULL,
  `JOBEXECUTIONID` bigint(20) DEFAULT NULL,
  `VO` varchar(16) DEFAULT NULL,
  `PILOTTIMING` varchar(100) DEFAULT NULL,
  `WORKINGGROUP` varchar(20) DEFAULT NULL,
  `PROCESSINGTYPE` varchar(64) DEFAULT NULL,
  `JOBPARAMETERS` text,
  `METADATA` text,
  `PRODUSERNAME` varchar(60) DEFAULT NULL,
  `NINPUTFILES` int(11) DEFAULT NULL,
  `COUNTRYGROUP` varchar(20) DEFAULT NULL,
  `BATCHID` varchar(80) DEFAULT NULL,
  `PARENTID` bigint(20) DEFAULT NULL,
  `SPECIALHANDLING` varchar(80) DEFAULT NULL,
  `JOBSETID` bigint(20) DEFAULT NULL,
  `CORECOUNT` tinyint(4) DEFAULT NULL,
  `NINPUTDATAFILES` int(11) DEFAULT NULL,
  `INPUTFILETYPE` varchar(32) DEFAULT NULL,
  `INPUTFILEPROJECT` varchar(64) DEFAULT NULL,
  `INPUTFILEBYTES` bigint(20) DEFAULT NULL,
  `NOUTPUTDATAFILES` int(11) DEFAULT NULL,
  `OUTPUTFILEBYTES` bigint(20) DEFAULT NULL,
  `JOBMETRICS` varchar(500) DEFAULT NULL,
  `WORKQUEUE_ID` int(11) DEFAULT NULL,
  `JEDITASKID` bigint(20) DEFAULT NULL,
  `JOBSUBSTATUS` varchar(80) DEFAULT NULL,
  `ACTUALCORECOUNT` int(11) DEFAULT NULL,
  KEY `J2007_JEDITASKID_IDX` (`JEDITASKID`),
  KEY `J2007_SITE_3ATTR_PANDAID_IDX` (`COMPUTINGSITE`,`JOBSTATUS`,`PRODSOURCELABEL`,`PROCESSINGTYPE`,`PANDAID`),
  KEY `J2007_JOBDEFID_PRODUSRNAME_IDX` (`JOBDEFINITIONID`,`PRODUSERNAME`),
  KEY `J2007_JOBSETID_PRODUSRNAME_IDX` (`JOBSETID`,`PRODUSERNAME`),
  KEY `J2007_PANDAID_IDX` (`PANDAID`),
  KEY `J2007_PRODUSRNAME_4ATTR_IDX` (`PRODUSERNAME`,`JOBSTATUS`,`PRODSOURCELABEL`,`JOBSETID`,`JOBDEFINITIONID`),
  KEY `J2007_TASKID_3ATTR_PANDAID_IDX` (`TASKID`,`PRODSOURCELABEL`,`JOBSTATUS`,`PROCESSINGTYPE`,`PANDAID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `y2007_jobsarchived`
--

LOCK TABLES `y2007_jobsarchived` WRITE;
/*!40000 ALTER TABLE `y2007_jobsarchived` DISABLE KEYS */;
/*!40000 ALTER TABLE `y2007_jobsarchived` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `y2007_metatable_arch`
--

DROP TABLE IF EXISTS `y2007_metatable_arch`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `y2007_metatable_arch` (
  `PANDAID` bigint(20) NOT NULL,
  `MODIFICATIONTIME` datetime NOT NULL,
  `METADATA` text,
  KEY `META2007_PANDAID_IDX` (`PANDAID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `y2007_metatable_arch`
--

LOCK TABLES `y2007_metatable_arch` WRITE;
/*!40000 ALTER TABLE `y2007_metatable_arch` DISABLE KEYS */;
/*!40000 ALTER TABLE `y2007_metatable_arch` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `y2008_filestable_arch`
--

DROP TABLE IF EXISTS `y2008_filestable_arch`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `y2008_filestable_arch` (
  `ROW_ID` bigint(20) NOT NULL,
  `PANDAID` bigint(20) NOT NULL,
  `MODIFICATIONTIME` datetime NOT NULL,
  `CREATIONTIME` datetime DEFAULT NULL,
  `GUID` varchar(64) DEFAULT NULL,
  `LFN` varchar(256) DEFAULT NULL,
  `TYPE` varchar(20) DEFAULT NULL,
  `FSIZE` bigint(20) DEFAULT NULL,
  `DATASET` varchar(255) DEFAULT NULL,
  `STATUS` varchar(64) DEFAULT NULL,
  `MD5SUM` varchar(40) DEFAULT NULL,
  `CHECKSUM` varchar(40) DEFAULT NULL,
  `PRODDBLOCK` varchar(255) DEFAULT NULL,
  `PRODDBLOCKTOKEN` varchar(250) DEFAULT NULL,
  `DISPATCHDBLOCK` varchar(255) DEFAULT NULL,
  `DISPATCHDBLOCKTOKEN` varchar(250) DEFAULT NULL,
  `DESTINATIONDBLOCK` varchar(255) DEFAULT NULL,
  `DESTINATIONDBLOCKTOKEN` varchar(250) DEFAULT NULL,
  `DESTINATIONSE` varchar(250) DEFAULT NULL,
  `SCOPE` varchar(30) DEFAULT NULL,
  `JEDITASKID` bigint(20) DEFAULT NULL,
  `DATASETID` bigint(20) DEFAULT NULL,
  `FILEID` bigint(20) DEFAULT NULL,
  `ATTEMPTNR` tinyint(4) DEFAULT NULL,
  KEY `F2008_PANDAID_IDX` (`PANDAID`),
  KEY `F2008_ARCH_ROWID_IDX` (`ROW_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `y2008_filestable_arch`
--

LOCK TABLES `y2008_filestable_arch` WRITE;
/*!40000 ALTER TABLE `y2008_filestable_arch` DISABLE KEYS */;
/*!40000 ALTER TABLE `y2008_filestable_arch` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `y2008_jobparamstable_arch`
--

DROP TABLE IF EXISTS `y2008_jobparamstable_arch`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `y2008_jobparamstable_arch` (
  `PANDAID` bigint(20) NOT NULL,
  `MODIFICATIONTIME` datetime NOT NULL,
  `JOBPARAMETERS` text,
  KEY `JOBPARAMS2008_ARCH_PANDAID_IDX` (`PANDAID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `y2008_jobparamstable_arch`
--

LOCK TABLES `y2008_jobparamstable_arch` WRITE;
/*!40000 ALTER TABLE `y2008_jobparamstable_arch` DISABLE KEYS */;
/*!40000 ALTER TABLE `y2008_jobparamstable_arch` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `y2008_jobsarchived`
--

DROP TABLE IF EXISTS `y2008_jobsarchived`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `y2008_jobsarchived` (
  `PANDAID` bigint(20) NOT NULL,
  `JOBDEFINITIONID` bigint(20) NOT NULL,
  `SCHEDULERID` varchar(128) DEFAULT NULL,
  `PILOTID` varchar(200) DEFAULT NULL,
  `CREATIONTIME` datetime NOT NULL,
  `CREATIONHOST` varchar(128) DEFAULT NULL,
  `MODIFICATIONTIME` datetime NOT NULL,
  `MODIFICATIONHOST` varchar(128) DEFAULT NULL,
  `ATLASRELEASE` varchar(64) DEFAULT NULL,
  `TRANSFORMATION` varchar(250) DEFAULT NULL,
  `HOMEPACKAGE` varchar(80) DEFAULT NULL,
  `PRODSERIESLABEL` varchar(20) DEFAULT NULL,
  `PRODSOURCELABEL` varchar(20) DEFAULT NULL,
  `PRODUSERID` varchar(250) DEFAULT NULL,
  `ASSIGNEDPRIORITY` int(11) NOT NULL,
  `CURRENTPRIORITY` int(11) NOT NULL,
  `ATTEMPTNR` tinyint(4) NOT NULL,
  `MAXATTEMPT` tinyint(4) NOT NULL,
  `JOBSTATUS` varchar(15) NOT NULL,
  `JOBNAME` varchar(256) DEFAULT NULL,
  `MAXCPUCOUNT` int(11) NOT NULL,
  `MAXCPUUNIT` varchar(32) DEFAULT NULL,
  `MAXDISKCOUNT` int(11) NOT NULL,
  `MAXDISKUNIT` char(4) DEFAULT NULL,
  `IPCONNECTIVITY` char(5) DEFAULT NULL,
  `MINRAMCOUNT` int(11) NOT NULL,
  `MINRAMUNIT` char(4) DEFAULT NULL,
  `STARTTIME` datetime DEFAULT NULL,
  `ENDTIME` datetime DEFAULT NULL,
  `CPUCONSUMPTIONTIME` bigint(20) DEFAULT NULL,
  `CPUCONSUMPTIONUNIT` varchar(128) DEFAULT NULL,
  `COMMANDTOPILOT` varchar(250) DEFAULT NULL,
  `TRANSEXITCODE` varchar(128) DEFAULT NULL,
  `PILOTERRORCODE` int(11) NOT NULL,
  `PILOTERRORDIAG` varchar(500) DEFAULT NULL,
  `EXEERRORCODE` int(11) NOT NULL,
  `EXEERRORDIAG` varchar(500) DEFAULT NULL,
  `SUPERRORCODE` int(11) NOT NULL,
  `SUPERRORDIAG` varchar(250) DEFAULT NULL,
  `DDMERRORCODE` int(11) NOT NULL,
  `DDMERRORDIAG` varchar(700) DEFAULT NULL,
  `BROKERAGEERRORCODE` int(11) NOT NULL,
  `BROKERAGEERRORDIAG` varchar(250) DEFAULT NULL,
  `JOBDISPATCHERERRORCODE` int(11) NOT NULL,
  `JOBDISPATCHERERRORDIAG` varchar(250) DEFAULT NULL,
  `TASKBUFFERERRORCODE` int(11) NOT NULL,
  `TASKBUFFERERRORDIAG` varchar(300) DEFAULT NULL,
  `COMPUTINGSITE` varchar(128) DEFAULT NULL,
  `COMPUTINGELEMENT` varchar(128) DEFAULT NULL,
  `PRODDBLOCK` varchar(255) DEFAULT NULL,
  `DISPATCHDBLOCK` varchar(255) DEFAULT NULL,
  `DESTINATIONDBLOCK` varchar(255) DEFAULT NULL,
  `DESTINATIONSE` varchar(250) DEFAULT NULL,
  `NEVENTS` int(11) DEFAULT NULL,
  `GRID` varchar(50) DEFAULT NULL,
  `CLOUD` varchar(50) DEFAULT NULL,
  `CPUCONVERSION` decimal(9,4) DEFAULT NULL,
  `SOURCESITE` varchar(36) DEFAULT NULL,
  `DESTINATIONSITE` varchar(36) DEFAULT NULL,
  `TRANSFERTYPE` varchar(10) DEFAULT NULL,
  `TASKID` int(11) DEFAULT NULL,
  `CMTCONFIG` varchar(250) DEFAULT NULL,
  `STATECHANGETIME` datetime DEFAULT NULL,
  `PRODDBUPDATETIME` datetime DEFAULT NULL,
  `LOCKEDBY` varchar(128) DEFAULT NULL,
  `RELOCATIONFLAG` tinyint(4) DEFAULT NULL,
  `JOBEXECUTIONID` bigint(20) DEFAULT NULL,
  `VO` varchar(16) DEFAULT NULL,
  `PILOTTIMING` varchar(100) DEFAULT NULL,
  `WORKINGGROUP` varchar(20) DEFAULT NULL,
  `PROCESSINGTYPE` varchar(64) DEFAULT NULL,
  `JOBPARAMETERS` text,
  `METADATA` text,
  `PRODUSERNAME` varchar(60) DEFAULT NULL,
  `NINPUTFILES` int(11) DEFAULT NULL,
  `COUNTRYGROUP` varchar(20) DEFAULT NULL,
  `BATCHID` varchar(80) DEFAULT NULL,
  `PARENTID` bigint(20) DEFAULT NULL,
  `SPECIALHANDLING` varchar(80) DEFAULT NULL,
  `JOBSETID` bigint(20) DEFAULT NULL,
  `CORECOUNT` tinyint(4) DEFAULT NULL,
  `NINPUTDATAFILES` int(11) DEFAULT NULL,
  `INPUTFILETYPE` varchar(32) DEFAULT NULL,
  `INPUTFILEPROJECT` varchar(64) DEFAULT NULL,
  `INPUTFILEBYTES` bigint(20) DEFAULT NULL,
  `NOUTPUTDATAFILES` int(11) DEFAULT NULL,
  `OUTPUTFILEBYTES` bigint(20) DEFAULT NULL,
  `JOBMETRICS` varchar(500) DEFAULT NULL,
  `WORKQUEUE_ID` int(11) DEFAULT NULL,
  `JEDITASKID` bigint(20) DEFAULT NULL,
  `JOBSUBSTATUS` varchar(80) DEFAULT NULL,
  `ACTUALCORECOUNT` int(11) DEFAULT NULL,
  KEY `J2008_JEDITASKID_IDX` (`JEDITASKID`),
  KEY `J2008_SITE_3ATTR_PANDAID_IDX` (`COMPUTINGSITE`,`JOBSTATUS`,`PRODSOURCELABEL`,`PROCESSINGTYPE`,`PANDAID`),
  KEY `J2008_JOBDEFID_PRODUSRNAME_IDX` (`JOBDEFINITIONID`,`PRODUSERNAME`),
  KEY `J2008_JOBSETID_PRODUSRNAME_IDX` (`JOBSETID`,`PRODUSERNAME`),
  KEY `J2008_PANDAID_IDX` (`PANDAID`),
  KEY `J2008_PRODUSRNAME_4ATTR_IDX` (`PRODUSERNAME`,`JOBSTATUS`,`PRODSOURCELABEL`,`JOBSETID`,`JOBDEFINITIONID`),
  KEY `J2008_TASKID_3ATTR_PANDAID_IDX` (`TASKID`,`PRODSOURCELABEL`,`JOBSTATUS`,`PROCESSINGTYPE`,`PANDAID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `y2008_jobsarchived`
--

LOCK TABLES `y2008_jobsarchived` WRITE;
/*!40000 ALTER TABLE `y2008_jobsarchived` DISABLE KEYS */;
/*!40000 ALTER TABLE `y2008_jobsarchived` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `y2008_metatable_arch`
--

DROP TABLE IF EXISTS `y2008_metatable_arch`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `y2008_metatable_arch` (
  `PANDAID` bigint(20) NOT NULL,
  `MODIFICATIONTIME` datetime NOT NULL,
  `METADATA` text,
  KEY `META2008_PANDAID_IDX` (`PANDAID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `y2008_metatable_arch`
--

LOCK TABLES `y2008_metatable_arch` WRITE;
/*!40000 ALTER TABLE `y2008_metatable_arch` DISABLE KEYS */;
/*!40000 ALTER TABLE `y2008_metatable_arch` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `y2009_filestable_arch`
--

DROP TABLE IF EXISTS `y2009_filestable_arch`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `y2009_filestable_arch` (
  `ROW_ID` bigint(20) NOT NULL,
  `PANDAID` bigint(20) NOT NULL,
  `MODIFICATIONTIME` datetime NOT NULL,
  `CREATIONTIME` datetime DEFAULT NULL,
  `GUID` varchar(64) DEFAULT NULL,
  `LFN` varchar(256) DEFAULT NULL,
  `TYPE` varchar(20) DEFAULT NULL,
  `FSIZE` bigint(20) DEFAULT NULL,
  `DATASET` varchar(255) DEFAULT NULL,
  `STATUS` varchar(64) DEFAULT NULL,
  `MD5SUM` varchar(40) DEFAULT NULL,
  `CHECKSUM` varchar(40) DEFAULT NULL,
  `PRODDBLOCK` varchar(255) DEFAULT NULL,
  `PRODDBLOCKTOKEN` varchar(250) DEFAULT NULL,
  `DISPATCHDBLOCK` varchar(255) DEFAULT NULL,
  `DISPATCHDBLOCKTOKEN` varchar(250) DEFAULT NULL,
  `DESTINATIONDBLOCK` varchar(255) DEFAULT NULL,
  `DESTINATIONDBLOCKTOKEN` varchar(250) DEFAULT NULL,
  `DESTINATIONSE` varchar(250) DEFAULT NULL,
  `SCOPE` varchar(30) DEFAULT NULL,
  `JEDITASKID` bigint(20) DEFAULT NULL,
  `DATASETID` bigint(20) DEFAULT NULL,
  `FILEID` bigint(20) DEFAULT NULL,
  `ATTEMPTNR` tinyint(4) DEFAULT NULL,
  KEY `F2009_PANDAID_IDX` (`PANDAID`),
  KEY `F2009_ARCH_ROWID_IDX` (`ROW_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `y2009_filestable_arch`
--

LOCK TABLES `y2009_filestable_arch` WRITE;
/*!40000 ALTER TABLE `y2009_filestable_arch` DISABLE KEYS */;
/*!40000 ALTER TABLE `y2009_filestable_arch` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `y2009_jobparamstable_arch`
--

DROP TABLE IF EXISTS `y2009_jobparamstable_arch`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `y2009_jobparamstable_arch` (
  `PANDAID` bigint(20) NOT NULL,
  `MODIFICATIONTIME` datetime NOT NULL,
  `JOBPARAMETERS` text,
  KEY `JOBPARAMS2009_ARCH_PANDAID_IDX` (`PANDAID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `y2009_jobparamstable_arch`
--

LOCK TABLES `y2009_jobparamstable_arch` WRITE;
/*!40000 ALTER TABLE `y2009_jobparamstable_arch` DISABLE KEYS */;
/*!40000 ALTER TABLE `y2009_jobparamstable_arch` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `y2009_jobsarchived`
--

DROP TABLE IF EXISTS `y2009_jobsarchived`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `y2009_jobsarchived` (
  `PANDAID` bigint(20) NOT NULL,
  `JOBDEFINITIONID` bigint(20) NOT NULL,
  `SCHEDULERID` varchar(128) DEFAULT NULL,
  `PILOTID` varchar(200) DEFAULT NULL,
  `CREATIONTIME` datetime NOT NULL,
  `CREATIONHOST` varchar(128) DEFAULT NULL,
  `MODIFICATIONTIME` datetime NOT NULL,
  `MODIFICATIONHOST` varchar(128) DEFAULT NULL,
  `ATLASRELEASE` varchar(64) DEFAULT NULL,
  `TRANSFORMATION` varchar(250) DEFAULT NULL,
  `HOMEPACKAGE` varchar(80) DEFAULT NULL,
  `PRODSERIESLABEL` varchar(20) DEFAULT NULL,
  `PRODSOURCELABEL` varchar(20) DEFAULT NULL,
  `PRODUSERID` varchar(250) DEFAULT NULL,
  `ASSIGNEDPRIORITY` int(11) NOT NULL,
  `CURRENTPRIORITY` int(11) NOT NULL,
  `ATTEMPTNR` tinyint(4) NOT NULL,
  `MAXATTEMPT` tinyint(4) NOT NULL,
  `JOBSTATUS` varchar(15) NOT NULL,
  `JOBNAME` varchar(256) DEFAULT NULL,
  `MAXCPUCOUNT` int(11) NOT NULL,
  `MAXCPUUNIT` varchar(32) DEFAULT NULL,
  `MAXDISKCOUNT` int(11) NOT NULL,
  `MAXDISKUNIT` char(4) DEFAULT NULL,
  `IPCONNECTIVITY` char(5) DEFAULT NULL,
  `MINRAMCOUNT` int(11) NOT NULL,
  `MINRAMUNIT` char(4) DEFAULT NULL,
  `STARTTIME` datetime DEFAULT NULL,
  `ENDTIME` datetime DEFAULT NULL,
  `CPUCONSUMPTIONTIME` bigint(20) DEFAULT NULL,
  `CPUCONSUMPTIONUNIT` varchar(128) DEFAULT NULL,
  `COMMANDTOPILOT` varchar(250) DEFAULT NULL,
  `TRANSEXITCODE` varchar(128) DEFAULT NULL,
  `PILOTERRORCODE` int(11) NOT NULL,
  `PILOTERRORDIAG` varchar(500) DEFAULT NULL,
  `EXEERRORCODE` int(11) NOT NULL,
  `EXEERRORDIAG` varchar(500) DEFAULT NULL,
  `SUPERRORCODE` int(11) NOT NULL,
  `SUPERRORDIAG` varchar(250) DEFAULT NULL,
  `DDMERRORCODE` int(11) NOT NULL,
  `DDMERRORDIAG` varchar(700) DEFAULT NULL,
  `BROKERAGEERRORCODE` int(11) NOT NULL,
  `BROKERAGEERRORDIAG` varchar(250) DEFAULT NULL,
  `JOBDISPATCHERERRORCODE` int(11) NOT NULL,
  `JOBDISPATCHERERRORDIAG` varchar(250) DEFAULT NULL,
  `TASKBUFFERERRORCODE` int(11) NOT NULL,
  `TASKBUFFERERRORDIAG` varchar(300) DEFAULT NULL,
  `COMPUTINGSITE` varchar(128) DEFAULT NULL,
  `COMPUTINGELEMENT` varchar(128) DEFAULT NULL,
  `PRODDBLOCK` varchar(255) DEFAULT NULL,
  `DISPATCHDBLOCK` varchar(255) DEFAULT NULL,
  `DESTINATIONDBLOCK` varchar(255) DEFAULT NULL,
  `DESTINATIONSE` varchar(250) DEFAULT NULL,
  `NEVENTS` int(11) DEFAULT NULL,
  `GRID` varchar(50) DEFAULT NULL,
  `CLOUD` varchar(50) DEFAULT NULL,
  `CPUCONVERSION` decimal(9,4) DEFAULT NULL,
  `SOURCESITE` varchar(36) DEFAULT NULL,
  `DESTINATIONSITE` varchar(36) DEFAULT NULL,
  `TRANSFERTYPE` varchar(10) DEFAULT NULL,
  `TASKID` int(11) DEFAULT NULL,
  `CMTCONFIG` varchar(250) DEFAULT NULL,
  `STATECHANGETIME` datetime DEFAULT NULL,
  `PRODDBUPDATETIME` datetime DEFAULT NULL,
  `LOCKEDBY` varchar(128) DEFAULT NULL,
  `RELOCATIONFLAG` tinyint(4) DEFAULT NULL,
  `JOBEXECUTIONID` bigint(20) DEFAULT NULL,
  `VO` varchar(16) DEFAULT NULL,
  `PILOTTIMING` varchar(100) DEFAULT NULL,
  `WORKINGGROUP` varchar(20) DEFAULT NULL,
  `PROCESSINGTYPE` varchar(64) DEFAULT NULL,
  `JOBPARAMETERS` text,
  `METADATA` text,
  `PRODUSERNAME` varchar(60) DEFAULT NULL,
  `NINPUTFILES` int(11) DEFAULT NULL,
  `COUNTRYGROUP` varchar(20) DEFAULT NULL,
  `BATCHID` varchar(80) DEFAULT NULL,
  `PARENTID` bigint(20) DEFAULT NULL,
  `SPECIALHANDLING` varchar(80) DEFAULT NULL,
  `JOBSETID` bigint(20) DEFAULT NULL,
  `CORECOUNT` tinyint(4) DEFAULT NULL,
  `NINPUTDATAFILES` int(11) DEFAULT NULL,
  `INPUTFILETYPE` varchar(32) DEFAULT NULL,
  `INPUTFILEPROJECT` varchar(64) DEFAULT NULL,
  `INPUTFILEBYTES` bigint(20) DEFAULT NULL,
  `NOUTPUTDATAFILES` int(11) DEFAULT NULL,
  `OUTPUTFILEBYTES` bigint(20) DEFAULT NULL,
  `JOBMETRICS` varchar(500) DEFAULT NULL,
  `WORKQUEUE_ID` int(11) DEFAULT NULL,
  `JEDITASKID` bigint(20) DEFAULT NULL,
  `JOBSUBSTATUS` varchar(80) DEFAULT NULL,
  `ACTUALCORECOUNT` int(11) DEFAULT NULL,
  KEY `J2009_PRODUSRNAME_4ATTR_IDX` (`PRODUSERNAME`,`JOBSTATUS`,`PRODSOURCELABEL`,`JOBSETID`,`JOBDEFINITIONID`),
  KEY `J2009_TASKID_3ATTR_PANDAID_IDX` (`TASKID`,`PRODSOURCELABEL`,`JOBSTATUS`,`PROCESSINGTYPE`,`PANDAID`),
  KEY `J2009_JEDITASKID_IDX` (`JEDITASKID`),
  KEY `J2009_SITE_3ATTR_PANDAID_IDX` (`COMPUTINGSITE`,`JOBSTATUS`,`PRODSOURCELABEL`,`PROCESSINGTYPE`,`PANDAID`),
  KEY `J2009_JOBDEFID_PRODUSRNAME_IDX` (`JOBDEFINITIONID`,`PRODUSERNAME`),
  KEY `J2009_JOBSETID_PRODUSRNAME_IDX` (`JOBSETID`,`PRODUSERNAME`),
  KEY `J2009_PANDAID_IDX` (`PANDAID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `y2009_jobsarchived`
--

LOCK TABLES `y2009_jobsarchived` WRITE;
/*!40000 ALTER TABLE `y2009_jobsarchived` DISABLE KEYS */;
/*!40000 ALTER TABLE `y2009_jobsarchived` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `y2009_metatable_arch`
--

DROP TABLE IF EXISTS `y2009_metatable_arch`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `y2009_metatable_arch` (
  `PANDAID` bigint(20) NOT NULL,
  `MODIFICATIONTIME` datetime NOT NULL,
  `METADATA` text,
  KEY `META2009_PANDAID_IDX` (`PANDAID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `y2009_metatable_arch`
--

LOCK TABLES `y2009_metatable_arch` WRITE;
/*!40000 ALTER TABLE `y2009_metatable_arch` DISABLE KEYS */;
/*!40000 ALTER TABLE `y2009_metatable_arch` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `y2010_filestable_arch`
--

DROP TABLE IF EXISTS `y2010_filestable_arch`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `y2010_filestable_arch` (
  `ROW_ID` bigint(20) NOT NULL,
  `PANDAID` bigint(20) NOT NULL,
  `MODIFICATIONTIME` datetime NOT NULL,
  `CREATIONTIME` datetime DEFAULT NULL,
  `GUID` varchar(64) DEFAULT NULL,
  `LFN` varchar(256) DEFAULT NULL,
  `TYPE` varchar(20) DEFAULT NULL,
  `FSIZE` bigint(20) DEFAULT NULL,
  `DATASET` varchar(255) DEFAULT NULL,
  `STATUS` varchar(64) DEFAULT NULL,
  `MD5SUM` varchar(40) DEFAULT NULL,
  `CHECKSUM` varchar(40) DEFAULT NULL,
  `PRODDBLOCK` varchar(255) DEFAULT NULL,
  `PRODDBLOCKTOKEN` varchar(250) DEFAULT NULL,
  `DISPATCHDBLOCK` varchar(255) DEFAULT NULL,
  `DISPATCHDBLOCKTOKEN` varchar(250) DEFAULT NULL,
  `DESTINATIONDBLOCK` varchar(255) DEFAULT NULL,
  `DESTINATIONDBLOCKTOKEN` varchar(250) DEFAULT NULL,
  `DESTINATIONSE` varchar(250) DEFAULT NULL,
  `SCOPE` varchar(30) DEFAULT NULL,
  `JEDITASKID` bigint(20) DEFAULT NULL,
  `DATASETID` bigint(20) DEFAULT NULL,
  `FILEID` bigint(20) DEFAULT NULL,
  `ATTEMPTNR` tinyint(4) DEFAULT NULL,
  KEY `F2010_PANDAID_IDX` (`PANDAID`),
  KEY `F2010_ARCH_ROWID_IDX` (`ROW_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `y2010_filestable_arch`
--

LOCK TABLES `y2010_filestable_arch` WRITE;
/*!40000 ALTER TABLE `y2010_filestable_arch` DISABLE KEYS */;
/*!40000 ALTER TABLE `y2010_filestable_arch` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `y2010_jobparamstable_arch`
--

DROP TABLE IF EXISTS `y2010_jobparamstable_arch`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `y2010_jobparamstable_arch` (
  `PANDAID` bigint(20) NOT NULL,
  `MODIFICATIONTIME` datetime NOT NULL,
  `JOBPARAMETERS` text,
  KEY `JOBPARAMS2010_ARCH_PANDAID_IDX` (`PANDAID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `y2010_jobparamstable_arch`
--

LOCK TABLES `y2010_jobparamstable_arch` WRITE;
/*!40000 ALTER TABLE `y2010_jobparamstable_arch` DISABLE KEYS */;
/*!40000 ALTER TABLE `y2010_jobparamstable_arch` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `y2010_jobsarchived`
--

DROP TABLE IF EXISTS `y2010_jobsarchived`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `y2010_jobsarchived` (
  `PANDAID` bigint(20) NOT NULL,
  `JOBDEFINITIONID` bigint(20) NOT NULL,
  `SCHEDULERID` varchar(128) DEFAULT NULL,
  `PILOTID` varchar(200) DEFAULT NULL,
  `CREATIONTIME` datetime NOT NULL,
  `CREATIONHOST` varchar(128) DEFAULT NULL,
  `MODIFICATIONTIME` datetime NOT NULL,
  `MODIFICATIONHOST` varchar(128) DEFAULT NULL,
  `ATLASRELEASE` varchar(64) DEFAULT NULL,
  `TRANSFORMATION` varchar(250) DEFAULT NULL,
  `HOMEPACKAGE` varchar(80) DEFAULT NULL,
  `PRODSERIESLABEL` varchar(20) DEFAULT NULL,
  `PRODSOURCELABEL` varchar(20) DEFAULT NULL,
  `PRODUSERID` varchar(250) DEFAULT NULL,
  `ASSIGNEDPRIORITY` int(11) NOT NULL,
  `CURRENTPRIORITY` int(11) NOT NULL,
  `ATTEMPTNR` tinyint(4) NOT NULL,
  `MAXATTEMPT` tinyint(4) NOT NULL,
  `JOBSTATUS` varchar(15) NOT NULL,
  `JOBNAME` varchar(256) DEFAULT NULL,
  `MAXCPUCOUNT` int(11) NOT NULL,
  `MAXCPUUNIT` varchar(32) DEFAULT NULL,
  `MAXDISKCOUNT` int(11) NOT NULL,
  `MAXDISKUNIT` char(4) DEFAULT NULL,
  `IPCONNECTIVITY` char(5) DEFAULT NULL,
  `MINRAMCOUNT` int(11) NOT NULL,
  `MINRAMUNIT` char(4) DEFAULT NULL,
  `STARTTIME` datetime DEFAULT NULL,
  `ENDTIME` datetime DEFAULT NULL,
  `CPUCONSUMPTIONTIME` bigint(20) DEFAULT NULL,
  `CPUCONSUMPTIONUNIT` varchar(128) DEFAULT NULL,
  `COMMANDTOPILOT` varchar(250) DEFAULT NULL,
  `TRANSEXITCODE` varchar(128) DEFAULT NULL,
  `PILOTERRORCODE` int(11) NOT NULL,
  `PILOTERRORDIAG` varchar(500) DEFAULT NULL,
  `EXEERRORCODE` int(11) NOT NULL,
  `EXEERRORDIAG` varchar(500) DEFAULT NULL,
  `SUPERRORCODE` int(11) NOT NULL,
  `SUPERRORDIAG` varchar(250) DEFAULT NULL,
  `DDMERRORCODE` int(11) NOT NULL,
  `DDMERRORDIAG` varchar(700) DEFAULT NULL,
  `BROKERAGEERRORCODE` int(11) NOT NULL,
  `BROKERAGEERRORDIAG` varchar(250) DEFAULT NULL,
  `JOBDISPATCHERERRORCODE` int(11) NOT NULL,
  `JOBDISPATCHERERRORDIAG` varchar(250) DEFAULT NULL,
  `TASKBUFFERERRORCODE` int(11) NOT NULL,
  `TASKBUFFERERRORDIAG` varchar(300) DEFAULT NULL,
  `COMPUTINGSITE` varchar(128) DEFAULT NULL,
  `COMPUTINGELEMENT` varchar(128) DEFAULT NULL,
  `PRODDBLOCK` varchar(255) DEFAULT NULL,
  `DISPATCHDBLOCK` varchar(255) DEFAULT NULL,
  `DESTINATIONDBLOCK` varchar(255) DEFAULT NULL,
  `DESTINATIONSE` varchar(250) DEFAULT NULL,
  `NEVENTS` int(11) DEFAULT NULL,
  `GRID` varchar(50) DEFAULT NULL,
  `CLOUD` varchar(50) DEFAULT NULL,
  `CPUCONVERSION` decimal(9,4) DEFAULT NULL,
  `SOURCESITE` varchar(36) DEFAULT NULL,
  `DESTINATIONSITE` varchar(36) DEFAULT NULL,
  `TRANSFERTYPE` varchar(10) DEFAULT NULL,
  `TASKID` int(11) DEFAULT NULL,
  `CMTCONFIG` varchar(250) DEFAULT NULL,
  `STATECHANGETIME` datetime DEFAULT NULL,
  `PRODDBUPDATETIME` datetime DEFAULT NULL,
  `LOCKEDBY` varchar(128) DEFAULT NULL,
  `RELOCATIONFLAG` tinyint(4) DEFAULT NULL,
  `JOBEXECUTIONID` bigint(20) DEFAULT NULL,
  `VO` varchar(16) DEFAULT NULL,
  `PILOTTIMING` varchar(100) DEFAULT NULL,
  `WORKINGGROUP` varchar(20) DEFAULT NULL,
  `PROCESSINGTYPE` varchar(64) DEFAULT NULL,
  `JOBPARAMETERS` text,
  `METADATA` text,
  `PRODUSERNAME` varchar(60) DEFAULT NULL,
  `NINPUTFILES` int(11) DEFAULT NULL,
  `COUNTRYGROUP` varchar(20) DEFAULT NULL,
  `BATCHID` varchar(80) DEFAULT NULL,
  `PARENTID` bigint(20) DEFAULT NULL,
  `SPECIALHANDLING` varchar(80) DEFAULT NULL,
  `JOBSETID` bigint(20) DEFAULT NULL,
  `CORECOUNT` tinyint(4) DEFAULT NULL,
  `NINPUTDATAFILES` int(11) DEFAULT NULL,
  `INPUTFILETYPE` varchar(32) DEFAULT NULL,
  `INPUTFILEPROJECT` varchar(64) DEFAULT NULL,
  `INPUTFILEBYTES` bigint(20) DEFAULT NULL,
  `NOUTPUTDATAFILES` int(11) DEFAULT NULL,
  `OUTPUTFILEBYTES` bigint(20) DEFAULT NULL,
  `JOBMETRICS` varchar(500) DEFAULT NULL,
  `WORKQUEUE_ID` int(11) DEFAULT NULL,
  `JEDITASKID` bigint(20) DEFAULT NULL,
  `JOBSUBSTATUS` varchar(80) DEFAULT NULL,
  `ACTUALCORECOUNT` int(11) DEFAULT NULL,
  KEY `J2010_JEDITASKID_IDX` (`JEDITASKID`),
  KEY `J2010_SITE_3ATTR_PANDAID_IDX` (`COMPUTINGSITE`,`JOBSTATUS`,`PRODSOURCELABEL`,`PROCESSINGTYPE`,`PANDAID`),
  KEY `J2010_JOBDEFID_PRODUSRNAME_IDX` (`JOBDEFINITIONID`,`PRODUSERNAME`),
  KEY `J2010_JOBSETID_PRODUSRNAME_IDX` (`JOBSETID`,`PRODUSERNAME`),
  KEY `J2010_PANDAID_IDX` (`PANDAID`),
  KEY `J2010_PRODUSRNAME_4ATTR_IDX` (`PRODUSERNAME`,`JOBSTATUS`,`PRODSOURCELABEL`,`JOBSETID`,`JOBDEFINITIONID`),
  KEY `J2010_TASKID_3ATTR_PANDAID_IDX` (`TASKID`,`PRODSOURCELABEL`,`JOBSTATUS`,`PROCESSINGTYPE`,`PANDAID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `y2010_jobsarchived`
--

LOCK TABLES `y2010_jobsarchived` WRITE;
/*!40000 ALTER TABLE `y2010_jobsarchived` DISABLE KEYS */;
/*!40000 ALTER TABLE `y2010_jobsarchived` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `y2010_metatable_arch`
--

DROP TABLE IF EXISTS `y2010_metatable_arch`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `y2010_metatable_arch` (
  `PANDAID` bigint(20) NOT NULL,
  `MODIFICATIONTIME` datetime NOT NULL,
  `METADATA` text,
  KEY `META2010_PANDAID_IDX` (`PANDAID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `y2010_metatable_arch`
--

LOCK TABLES `y2010_metatable_arch` WRITE;
/*!40000 ALTER TABLE `y2010_metatable_arch` DISABLE KEYS */;
/*!40000 ALTER TABLE `y2010_metatable_arch` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `y2011_filestable_arch`
--

DROP TABLE IF EXISTS `y2011_filestable_arch`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `y2011_filestable_arch` (
  `ROW_ID` bigint(20) NOT NULL,
  `PANDAID` bigint(20) NOT NULL,
  `MODIFICATIONTIME` datetime NOT NULL,
  `CREATIONTIME` datetime DEFAULT NULL,
  `GUID` varchar(64) DEFAULT NULL,
  `LFN` varchar(256) DEFAULT NULL,
  `TYPE` varchar(20) DEFAULT NULL,
  `FSIZE` bigint(20) DEFAULT NULL,
  `DATASET` varchar(255) DEFAULT NULL,
  `STATUS` varchar(64) DEFAULT NULL,
  `MD5SUM` varchar(40) DEFAULT NULL,
  `CHECKSUM` varchar(40) DEFAULT NULL,
  `PRODDBLOCK` varchar(255) DEFAULT NULL,
  `PRODDBLOCKTOKEN` varchar(250) DEFAULT NULL,
  `DISPATCHDBLOCK` varchar(255) DEFAULT NULL,
  `DISPATCHDBLOCKTOKEN` varchar(250) DEFAULT NULL,
  `DESTINATIONDBLOCK` varchar(255) DEFAULT NULL,
  `DESTINATIONDBLOCKTOKEN` varchar(250) DEFAULT NULL,
  `DESTINATIONSE` varchar(250) DEFAULT NULL,
  `SCOPE` varchar(30) DEFAULT NULL,
  `JEDITASKID` bigint(20) DEFAULT NULL,
  `DATASETID` bigint(20) DEFAULT NULL,
  `FILEID` bigint(20) DEFAULT NULL,
  `ATTEMPTNR` tinyint(4) DEFAULT NULL,
  KEY `F2011_PANDAID_IDX` (`PANDAID`),
  KEY `F2011_ARCH_ROWID_IDX` (`ROW_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `y2011_filestable_arch`
--

LOCK TABLES `y2011_filestable_arch` WRITE;
/*!40000 ALTER TABLE `y2011_filestable_arch` DISABLE KEYS */;
/*!40000 ALTER TABLE `y2011_filestable_arch` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `y2011_jobparamstable_arch`
--

DROP TABLE IF EXISTS `y2011_jobparamstable_arch`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `y2011_jobparamstable_arch` (
  `PANDAID` bigint(20) NOT NULL,
  `MODIFICATIONTIME` datetime NOT NULL,
  `JOBPARAMETERS` text,
  KEY `JOBPARAMS2011_ARCH_PANDAID_IDX` (`PANDAID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `y2011_jobparamstable_arch`
--

LOCK TABLES `y2011_jobparamstable_arch` WRITE;
/*!40000 ALTER TABLE `y2011_jobparamstable_arch` DISABLE KEYS */;
/*!40000 ALTER TABLE `y2011_jobparamstable_arch` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `y2011_jobsarchived`
--

DROP TABLE IF EXISTS `y2011_jobsarchived`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `y2011_jobsarchived` (
  `PANDAID` bigint(20) NOT NULL,
  `JOBDEFINITIONID` bigint(20) NOT NULL,
  `SCHEDULERID` varchar(128) DEFAULT NULL,
  `PILOTID` varchar(200) DEFAULT NULL,
  `CREATIONTIME` datetime NOT NULL,
  `CREATIONHOST` varchar(128) DEFAULT NULL,
  `MODIFICATIONTIME` datetime NOT NULL,
  `MODIFICATIONHOST` varchar(128) DEFAULT NULL,
  `ATLASRELEASE` varchar(64) DEFAULT NULL,
  `TRANSFORMATION` varchar(250) DEFAULT NULL,
  `HOMEPACKAGE` varchar(80) DEFAULT NULL,
  `PRODSERIESLABEL` varchar(20) DEFAULT NULL,
  `PRODSOURCELABEL` varchar(20) DEFAULT NULL,
  `PRODUSERID` varchar(250) DEFAULT NULL,
  `ASSIGNEDPRIORITY` int(11) NOT NULL,
  `CURRENTPRIORITY` int(11) NOT NULL,
  `ATTEMPTNR` tinyint(4) NOT NULL,
  `MAXATTEMPT` tinyint(4) NOT NULL,
  `JOBSTATUS` varchar(15) NOT NULL,
  `JOBNAME` varchar(256) DEFAULT NULL,
  `MAXCPUCOUNT` int(11) NOT NULL,
  `MAXCPUUNIT` varchar(32) DEFAULT NULL,
  `MAXDISKCOUNT` int(11) NOT NULL,
  `MAXDISKUNIT` char(4) DEFAULT NULL,
  `IPCONNECTIVITY` char(5) DEFAULT NULL,
  `MINRAMCOUNT` int(11) NOT NULL,
  `MINRAMUNIT` char(4) DEFAULT NULL,
  `STARTTIME` datetime DEFAULT NULL,
  `ENDTIME` datetime DEFAULT NULL,
  `CPUCONSUMPTIONTIME` bigint(20) DEFAULT NULL,
  `CPUCONSUMPTIONUNIT` varchar(128) DEFAULT NULL,
  `COMMANDTOPILOT` varchar(250) DEFAULT NULL,
  `TRANSEXITCODE` varchar(128) DEFAULT NULL,
  `PILOTERRORCODE` int(11) NOT NULL,
  `PILOTERRORDIAG` varchar(500) DEFAULT NULL,
  `EXEERRORCODE` int(11) NOT NULL,
  `EXEERRORDIAG` varchar(500) DEFAULT NULL,
  `SUPERRORCODE` int(11) NOT NULL,
  `SUPERRORDIAG` varchar(250) DEFAULT NULL,
  `DDMERRORCODE` int(11) NOT NULL,
  `DDMERRORDIAG` varchar(700) DEFAULT NULL,
  `BROKERAGEERRORCODE` int(11) NOT NULL,
  `BROKERAGEERRORDIAG` varchar(250) DEFAULT NULL,
  `JOBDISPATCHERERRORCODE` int(11) NOT NULL,
  `JOBDISPATCHERERRORDIAG` varchar(250) DEFAULT NULL,
  `TASKBUFFERERRORCODE` int(11) NOT NULL,
  `TASKBUFFERERRORDIAG` varchar(300) DEFAULT NULL,
  `COMPUTINGSITE` varchar(128) DEFAULT NULL,
  `COMPUTINGELEMENT` varchar(128) DEFAULT NULL,
  `PRODDBLOCK` varchar(255) DEFAULT NULL,
  `DISPATCHDBLOCK` varchar(255) DEFAULT NULL,
  `DESTINATIONDBLOCK` varchar(255) DEFAULT NULL,
  `DESTINATIONSE` varchar(250) DEFAULT NULL,
  `NEVENTS` int(11) DEFAULT NULL,
  `GRID` varchar(50) DEFAULT NULL,
  `CLOUD` varchar(50) DEFAULT NULL,
  `CPUCONVERSION` decimal(9,4) DEFAULT NULL,
  `SOURCESITE` varchar(36) DEFAULT NULL,
  `DESTINATIONSITE` varchar(36) DEFAULT NULL,
  `TRANSFERTYPE` varchar(10) DEFAULT NULL,
  `TASKID` int(11) DEFAULT NULL,
  `CMTCONFIG` varchar(250) DEFAULT NULL,
  `STATECHANGETIME` datetime DEFAULT NULL,
  `PRODDBUPDATETIME` datetime DEFAULT NULL,
  `LOCKEDBY` varchar(128) DEFAULT NULL,
  `RELOCATIONFLAG` tinyint(4) DEFAULT NULL,
  `JOBEXECUTIONID` bigint(20) DEFAULT NULL,
  `VO` varchar(16) DEFAULT NULL,
  `PILOTTIMING` varchar(100) DEFAULT NULL,
  `WORKINGGROUP` varchar(20) DEFAULT NULL,
  `PROCESSINGTYPE` varchar(64) DEFAULT NULL,
  `JOBPARAMETERS` text,
  `METADATA` text,
  `PRODUSERNAME` varchar(60) DEFAULT NULL,
  `NINPUTFILES` int(11) DEFAULT NULL,
  `COUNTRYGROUP` varchar(20) DEFAULT NULL,
  `BATCHID` varchar(80) DEFAULT NULL,
  `PARENTID` bigint(20) DEFAULT NULL,
  `SPECIALHANDLING` varchar(80) DEFAULT NULL,
  `JOBSETID` bigint(20) DEFAULT NULL,
  `CORECOUNT` tinyint(4) DEFAULT NULL,
  `NINPUTDATAFILES` int(11) DEFAULT NULL,
  `INPUTFILETYPE` varchar(32) DEFAULT NULL,
  `INPUTFILEPROJECT` varchar(64) DEFAULT NULL,
  `INPUTFILEBYTES` bigint(20) DEFAULT NULL,
  `NOUTPUTDATAFILES` int(11) DEFAULT NULL,
  `OUTPUTFILEBYTES` bigint(20) DEFAULT NULL,
  `JOBMETRICS` varchar(500) DEFAULT NULL,
  `WORKQUEUE_ID` int(11) DEFAULT NULL,
  `JEDITASKID` bigint(20) DEFAULT NULL,
  `JOBSUBSTATUS` varchar(80) DEFAULT NULL,
  `ACTUALCORECOUNT` int(11) DEFAULT NULL,
  KEY `J2011_SITE_3ATTR_PANDAID_IDX` (`COMPUTINGSITE`,`JOBSTATUS`,`PRODSOURCELABEL`,`PROCESSINGTYPE`,`PANDAID`),
  KEY `J2011_JOBDEFID_PRODUSRNAME_IDX` (`JOBDEFINITIONID`,`PRODUSERNAME`),
  KEY `J2011_JOBSETID_PRODUSRNAME_IDX` (`JOBSETID`,`PRODUSERNAME`),
  KEY `J2011_PANDAID_IDX` (`PANDAID`),
  KEY `J2011_PRODUSRNAME_4ATTR_IDX` (`PRODUSERNAME`,`JOBSTATUS`,`PRODSOURCELABEL`,`JOBSETID`,`JOBDEFINITIONID`),
  KEY `J2011_TASKID_3ATTR_PANDAID_IDX` (`TASKID`,`PRODSOURCELABEL`,`JOBSTATUS`,`PROCESSINGTYPE`,`PANDAID`),
  KEY `J2011_JEDITASKID_IDX` (`JEDITASKID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `y2011_jobsarchived`
--

LOCK TABLES `y2011_jobsarchived` WRITE;
/*!40000 ALTER TABLE `y2011_jobsarchived` DISABLE KEYS */;
/*!40000 ALTER TABLE `y2011_jobsarchived` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `y2011_metatable_arch`
--

DROP TABLE IF EXISTS `y2011_metatable_arch`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `y2011_metatable_arch` (
  `PANDAID` bigint(20) NOT NULL,
  `MODIFICATIONTIME` datetime NOT NULL,
  `METADATA` text,
  KEY `META2011_PANDAID_IDX` (`PANDAID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `y2011_metatable_arch`
--

LOCK TABLES `y2011_metatable_arch` WRITE;
/*!40000 ALTER TABLE `y2011_metatable_arch` DISABLE KEYS */;
/*!40000 ALTER TABLE `y2011_metatable_arch` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `y2012_filestable_arch`
--

DROP TABLE IF EXISTS `y2012_filestable_arch`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `y2012_filestable_arch` (
  `ROW_ID` bigint(20) NOT NULL,
  `PANDAID` bigint(20) NOT NULL,
  `MODIFICATIONTIME` datetime NOT NULL,
  `CREATIONTIME` datetime DEFAULT NULL,
  `GUID` varchar(64) DEFAULT NULL,
  `LFN` varchar(256) DEFAULT NULL,
  `TYPE` varchar(20) DEFAULT NULL,
  `FSIZE` bigint(20) DEFAULT NULL,
  `DATASET` varchar(255) DEFAULT NULL,
  `STATUS` varchar(64) DEFAULT NULL,
  `MD5SUM` varchar(40) DEFAULT NULL,
  `CHECKSUM` varchar(40) DEFAULT NULL,
  `PRODDBLOCK` varchar(255) DEFAULT NULL,
  `PRODDBLOCKTOKEN` varchar(250) DEFAULT NULL,
  `DISPATCHDBLOCK` varchar(255) DEFAULT NULL,
  `DISPATCHDBLOCKTOKEN` varchar(250) DEFAULT NULL,
  `DESTINATIONDBLOCK` varchar(255) DEFAULT NULL,
  `DESTINATIONDBLOCKTOKEN` varchar(250) DEFAULT NULL,
  `DESTINATIONSE` varchar(250) DEFAULT NULL,
  `SCOPE` varchar(30) DEFAULT NULL,
  `JEDITASKID` bigint(20) DEFAULT NULL,
  `DATASETID` bigint(20) DEFAULT NULL,
  `FILEID` bigint(20) DEFAULT NULL,
  `ATTEMPTNR` tinyint(4) DEFAULT NULL,
  KEY `F2012_PANDAID_IDX` (`PANDAID`),
  KEY `F2012_ARCH_ROWID_IDX` (`ROW_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `y2012_filestable_arch`
--

LOCK TABLES `y2012_filestable_arch` WRITE;
/*!40000 ALTER TABLE `y2012_filestable_arch` DISABLE KEYS */;
/*!40000 ALTER TABLE `y2012_filestable_arch` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `y2012_jobparamstable_arch`
--

DROP TABLE IF EXISTS `y2012_jobparamstable_arch`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `y2012_jobparamstable_arch` (
  `PANDAID` bigint(20) NOT NULL,
  `MODIFICATIONTIME` datetime NOT NULL,
  `JOBPARAMETERS` text,
  KEY `JOBPARAMS2012_ARCH_PANDAID_IDX` (`PANDAID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `y2012_jobparamstable_arch`
--

LOCK TABLES `y2012_jobparamstable_arch` WRITE;
/*!40000 ALTER TABLE `y2012_jobparamstable_arch` DISABLE KEYS */;
/*!40000 ALTER TABLE `y2012_jobparamstable_arch` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `y2012_jobsarchived`
--

DROP TABLE IF EXISTS `y2012_jobsarchived`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `y2012_jobsarchived` (
  `PANDAID` bigint(20) NOT NULL,
  `JOBDEFINITIONID` bigint(20) NOT NULL,
  `SCHEDULERID` varchar(128) DEFAULT NULL,
  `PILOTID` varchar(200) DEFAULT NULL,
  `CREATIONTIME` datetime NOT NULL,
  `CREATIONHOST` varchar(128) DEFAULT NULL,
  `MODIFICATIONTIME` datetime NOT NULL,
  `MODIFICATIONHOST` varchar(128) DEFAULT NULL,
  `ATLASRELEASE` varchar(64) DEFAULT NULL,
  `TRANSFORMATION` varchar(250) DEFAULT NULL,
  `HOMEPACKAGE` varchar(80) DEFAULT NULL,
  `PRODSERIESLABEL` varchar(20) DEFAULT NULL,
  `PRODSOURCELABEL` varchar(20) DEFAULT NULL,
  `PRODUSERID` varchar(250) DEFAULT NULL,
  `ASSIGNEDPRIORITY` int(11) NOT NULL,
  `CURRENTPRIORITY` int(11) NOT NULL,
  `ATTEMPTNR` tinyint(4) NOT NULL,
  `MAXATTEMPT` tinyint(4) NOT NULL,
  `JOBSTATUS` varchar(15) NOT NULL,
  `JOBNAME` varchar(256) DEFAULT NULL,
  `MAXCPUCOUNT` int(11) NOT NULL,
  `MAXCPUUNIT` varchar(32) DEFAULT NULL,
  `MAXDISKCOUNT` int(11) NOT NULL,
  `MAXDISKUNIT` char(4) DEFAULT NULL,
  `IPCONNECTIVITY` char(5) DEFAULT NULL,
  `MINRAMCOUNT` int(11) NOT NULL,
  `MINRAMUNIT` char(4) DEFAULT NULL,
  `STARTTIME` datetime DEFAULT NULL,
  `ENDTIME` datetime DEFAULT NULL,
  `CPUCONSUMPTIONTIME` bigint(20) DEFAULT NULL,
  `CPUCONSUMPTIONUNIT` varchar(128) DEFAULT NULL,
  `COMMANDTOPILOT` varchar(250) DEFAULT NULL,
  `TRANSEXITCODE` varchar(128) DEFAULT NULL,
  `PILOTERRORCODE` int(11) NOT NULL,
  `PILOTERRORDIAG` varchar(500) DEFAULT NULL,
  `EXEERRORCODE` int(11) NOT NULL,
  `EXEERRORDIAG` varchar(500) DEFAULT NULL,
  `SUPERRORCODE` int(11) NOT NULL,
  `SUPERRORDIAG` varchar(250) DEFAULT NULL,
  `DDMERRORCODE` int(11) NOT NULL,
  `DDMERRORDIAG` varchar(700) DEFAULT NULL,
  `BROKERAGEERRORCODE` int(11) NOT NULL,
  `BROKERAGEERRORDIAG` varchar(250) DEFAULT NULL,
  `JOBDISPATCHERERRORCODE` int(11) NOT NULL,
  `JOBDISPATCHERERRORDIAG` varchar(250) DEFAULT NULL,
  `TASKBUFFERERRORCODE` int(11) NOT NULL,
  `TASKBUFFERERRORDIAG` varchar(300) DEFAULT NULL,
  `COMPUTINGSITE` varchar(128) DEFAULT NULL,
  `COMPUTINGELEMENT` varchar(128) DEFAULT NULL,
  `PRODDBLOCK` varchar(255) DEFAULT NULL,
  `DISPATCHDBLOCK` varchar(255) DEFAULT NULL,
  `DESTINATIONDBLOCK` varchar(255) DEFAULT NULL,
  `DESTINATIONSE` varchar(250) DEFAULT NULL,
  `NEVENTS` int(11) DEFAULT NULL,
  `GRID` varchar(50) DEFAULT NULL,
  `CLOUD` varchar(50) DEFAULT NULL,
  `CPUCONVERSION` decimal(9,4) DEFAULT NULL,
  `SOURCESITE` varchar(36) DEFAULT NULL,
  `DESTINATIONSITE` varchar(36) DEFAULT NULL,
  `TRANSFERTYPE` varchar(10) DEFAULT NULL,
  `TASKID` int(11) DEFAULT NULL,
  `CMTCONFIG` varchar(250) DEFAULT NULL,
  `STATECHANGETIME` datetime DEFAULT NULL,
  `PRODDBUPDATETIME` datetime DEFAULT NULL,
  `LOCKEDBY` varchar(128) DEFAULT NULL,
  `RELOCATIONFLAG` tinyint(4) DEFAULT NULL,
  `JOBEXECUTIONID` bigint(20) DEFAULT NULL,
  `VO` varchar(16) DEFAULT NULL,
  `PILOTTIMING` varchar(100) DEFAULT NULL,
  `WORKINGGROUP` varchar(20) DEFAULT NULL,
  `PROCESSINGTYPE` varchar(64) DEFAULT NULL,
  `JOBPARAMETERS` text,
  `METADATA` text,
  `PRODUSERNAME` varchar(60) DEFAULT NULL,
  `NINPUTFILES` int(11) DEFAULT NULL,
  `COUNTRYGROUP` varchar(20) DEFAULT NULL,
  `BATCHID` varchar(80) DEFAULT NULL,
  `PARENTID` bigint(20) DEFAULT NULL,
  `SPECIALHANDLING` varchar(80) DEFAULT NULL,
  `JOBSETID` bigint(20) DEFAULT NULL,
  `CORECOUNT` tinyint(4) DEFAULT NULL,
  `NINPUTDATAFILES` int(11) DEFAULT NULL,
  `INPUTFILETYPE` varchar(32) DEFAULT NULL,
  `INPUTFILEPROJECT` varchar(64) DEFAULT NULL,
  `INPUTFILEBYTES` bigint(20) DEFAULT NULL,
  `NOUTPUTDATAFILES` int(11) DEFAULT NULL,
  `OUTPUTFILEBYTES` bigint(20) DEFAULT NULL,
  `JOBMETRICS` varchar(500) DEFAULT NULL,
  `WORKQUEUE_ID` int(11) DEFAULT NULL,
  `JEDITASKID` bigint(20) DEFAULT NULL,
  `JOBSUBSTATUS` varchar(80) DEFAULT NULL,
  `ACTUALCORECOUNT` int(11) DEFAULT NULL,
  KEY `J2012_PANDAID_IDX` (`PANDAID`),
  KEY `J2012_PRODUSRNAME_4ATTR_IDX` (`PRODUSERNAME`,`JOBSTATUS`,`PRODSOURCELABEL`,`JOBSETID`,`JOBDEFINITIONID`),
  KEY `J2012_TASKID_3ATTR_PANDAID_IDX` (`TASKID`,`PRODSOURCELABEL`,`JOBSTATUS`,`PROCESSINGTYPE`,`PANDAID`),
  KEY `J2012_JEDITASKID_IDX` (`JEDITASKID`),
  KEY `J2012_SITE_3ATTR_PANDAID_IDX` (`COMPUTINGSITE`,`JOBSTATUS`,`PRODSOURCELABEL`,`PROCESSINGTYPE`,`PANDAID`),
  KEY `J2012_JOBDEFID_PRODUSRNAME_IDX` (`JOBDEFINITIONID`,`PRODUSERNAME`),
  KEY `J2012_JOBSETID_PRODUSRNAME_IDX` (`JOBSETID`,`PRODUSERNAME`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `y2012_jobsarchived`
--

LOCK TABLES `y2012_jobsarchived` WRITE;
/*!40000 ALTER TABLE `y2012_jobsarchived` DISABLE KEYS */;
/*!40000 ALTER TABLE `y2012_jobsarchived` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `y2012_metatable_arch`
--

DROP TABLE IF EXISTS `y2012_metatable_arch`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `y2012_metatable_arch` (
  `PANDAID` bigint(20) NOT NULL,
  `MODIFICATIONTIME` datetime NOT NULL,
  `METADATA` text,
  KEY `META2012_PANDAID_IDX` (`PANDAID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `y2012_metatable_arch`
--

LOCK TABLES `y2012_metatable_arch` WRITE;
/*!40000 ALTER TABLE `y2012_metatable_arch` DISABLE KEYS */;
/*!40000 ALTER TABLE `y2012_metatable_arch` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `y2013_filestable_arch`
--

DROP TABLE IF EXISTS `y2013_filestable_arch`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `y2013_filestable_arch` (
  `ROW_ID` bigint(20) NOT NULL,
  `PANDAID` bigint(20) NOT NULL,
  `MODIFICATIONTIME` datetime NOT NULL,
  `CREATIONTIME` datetime DEFAULT NULL,
  `GUID` varchar(64) DEFAULT NULL,
  `LFN` varchar(256) DEFAULT NULL,
  `TYPE` varchar(20) DEFAULT NULL,
  `FSIZE` bigint(20) DEFAULT NULL,
  `DATASET` varchar(255) DEFAULT NULL,
  `STATUS` varchar(64) DEFAULT NULL,
  `MD5SUM` varchar(40) DEFAULT NULL,
  `CHECKSUM` varchar(40) DEFAULT NULL,
  `PRODDBLOCK` varchar(255) DEFAULT NULL,
  `PRODDBLOCKTOKEN` varchar(250) DEFAULT NULL,
  `DISPATCHDBLOCK` varchar(255) DEFAULT NULL,
  `DISPATCHDBLOCKTOKEN` varchar(250) DEFAULT NULL,
  `DESTINATIONDBLOCK` varchar(255) DEFAULT NULL,
  `DESTINATIONDBLOCKTOKEN` varchar(250) DEFAULT NULL,
  `DESTINATIONSE` varchar(250) DEFAULT NULL,
  `SCOPE` varchar(30) DEFAULT NULL,
  `JEDITASKID` bigint(20) DEFAULT NULL,
  `DATASETID` bigint(20) DEFAULT NULL,
  `FILEID` bigint(20) DEFAULT NULL,
  `ATTEMPTNR` tinyint(4) DEFAULT NULL,
  KEY `F2013_PANDAID_IDX` (`PANDAID`),
  KEY `F2013_ARCH_ROWID_IDX` (`ROW_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `y2013_filestable_arch`
--

LOCK TABLES `y2013_filestable_arch` WRITE;
/*!40000 ALTER TABLE `y2013_filestable_arch` DISABLE KEYS */;
/*!40000 ALTER TABLE `y2013_filestable_arch` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `y2013_jobparamstable_arch`
--

DROP TABLE IF EXISTS `y2013_jobparamstable_arch`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `y2013_jobparamstable_arch` (
  `PANDAID` bigint(20) NOT NULL,
  `MODIFICATIONTIME` datetime NOT NULL,
  `JOBPARAMETERS` text,
  KEY `JOBPARAMS2013_ARCH_PANDAID_IDX` (`PANDAID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `y2013_jobparamstable_arch`
--

LOCK TABLES `y2013_jobparamstable_arch` WRITE;
/*!40000 ALTER TABLE `y2013_jobparamstable_arch` DISABLE KEYS */;
/*!40000 ALTER TABLE `y2013_jobparamstable_arch` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `y2013_jobsarchived`
--

DROP TABLE IF EXISTS `y2013_jobsarchived`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `y2013_jobsarchived` (
  `PANDAID` bigint(20) NOT NULL,
  `JOBDEFINITIONID` bigint(20) NOT NULL,
  `SCHEDULERID` varchar(128) DEFAULT NULL,
  `PILOTID` varchar(200) DEFAULT NULL,
  `CREATIONTIME` datetime NOT NULL,
  `CREATIONHOST` varchar(128) DEFAULT NULL,
  `MODIFICATIONTIME` datetime NOT NULL,
  `MODIFICATIONHOST` varchar(128) DEFAULT NULL,
  `ATLASRELEASE` varchar(64) DEFAULT NULL,
  `TRANSFORMATION` varchar(250) DEFAULT NULL,
  `HOMEPACKAGE` varchar(80) DEFAULT NULL,
  `PRODSERIESLABEL` varchar(20) DEFAULT NULL,
  `PRODSOURCELABEL` varchar(20) DEFAULT NULL,
  `PRODUSERID` varchar(250) DEFAULT NULL,
  `ASSIGNEDPRIORITY` int(11) NOT NULL,
  `CURRENTPRIORITY` int(11) NOT NULL,
  `ATTEMPTNR` tinyint(4) NOT NULL,
  `MAXATTEMPT` tinyint(4) NOT NULL,
  `JOBSTATUS` varchar(15) NOT NULL,
  `JOBNAME` varchar(256) DEFAULT NULL,
  `MAXCPUCOUNT` int(11) NOT NULL,
  `MAXCPUUNIT` varchar(32) DEFAULT NULL,
  `MAXDISKCOUNT` int(11) NOT NULL,
  `MAXDISKUNIT` char(4) DEFAULT NULL,
  `IPCONNECTIVITY` char(5) DEFAULT NULL,
  `MINRAMCOUNT` int(11) NOT NULL,
  `MINRAMUNIT` char(4) DEFAULT NULL,
  `STARTTIME` datetime DEFAULT NULL,
  `ENDTIME` datetime DEFAULT NULL,
  `CPUCONSUMPTIONTIME` bigint(20) DEFAULT NULL,
  `CPUCONSUMPTIONUNIT` varchar(128) DEFAULT NULL,
  `COMMANDTOPILOT` varchar(250) DEFAULT NULL,
  `TRANSEXITCODE` varchar(128) DEFAULT NULL,
  `PILOTERRORCODE` int(11) NOT NULL,
  `PILOTERRORDIAG` varchar(500) DEFAULT NULL,
  `EXEERRORCODE` int(11) NOT NULL,
  `EXEERRORDIAG` varchar(500) DEFAULT NULL,
  `SUPERRORCODE` int(11) NOT NULL,
  `SUPERRORDIAG` varchar(250) DEFAULT NULL,
  `DDMERRORCODE` int(11) NOT NULL,
  `DDMERRORDIAG` varchar(700) DEFAULT NULL,
  `BROKERAGEERRORCODE` int(11) NOT NULL,
  `BROKERAGEERRORDIAG` varchar(250) DEFAULT NULL,
  `JOBDISPATCHERERRORCODE` int(11) NOT NULL,
  `JOBDISPATCHERERRORDIAG` varchar(250) DEFAULT NULL,
  `TASKBUFFERERRORCODE` int(11) NOT NULL,
  `TASKBUFFERERRORDIAG` varchar(300) DEFAULT NULL,
  `COMPUTINGSITE` varchar(128) DEFAULT NULL,
  `COMPUTINGELEMENT` varchar(128) DEFAULT NULL,
  `PRODDBLOCK` varchar(255) DEFAULT NULL,
  `DISPATCHDBLOCK` varchar(255) DEFAULT NULL,
  `DESTINATIONDBLOCK` varchar(255) DEFAULT NULL,
  `DESTINATIONSE` varchar(250) DEFAULT NULL,
  `NEVENTS` int(11) DEFAULT NULL,
  `GRID` varchar(50) DEFAULT NULL,
  `CLOUD` varchar(50) DEFAULT NULL,
  `CPUCONVERSION` decimal(9,4) DEFAULT NULL,
  `SOURCESITE` varchar(36) DEFAULT NULL,
  `DESTINATIONSITE` varchar(36) DEFAULT NULL,
  `TRANSFERTYPE` varchar(10) DEFAULT NULL,
  `TASKID` int(11) DEFAULT NULL,
  `CMTCONFIG` varchar(250) DEFAULT NULL,
  `STATECHANGETIME` datetime DEFAULT NULL,
  `PRODDBUPDATETIME` datetime DEFAULT NULL,
  `LOCKEDBY` varchar(128) DEFAULT NULL,
  `RELOCATIONFLAG` tinyint(4) DEFAULT NULL,
  `JOBEXECUTIONID` bigint(20) DEFAULT NULL,
  `VO` varchar(16) DEFAULT NULL,
  `PILOTTIMING` varchar(100) DEFAULT NULL,
  `WORKINGGROUP` varchar(20) DEFAULT NULL,
  `PROCESSINGTYPE` varchar(64) DEFAULT NULL,
  `JOBPARAMETERS` text,
  `METADATA` text,
  `PRODUSERNAME` varchar(60) DEFAULT NULL,
  `NINPUTFILES` int(11) DEFAULT NULL,
  `COUNTRYGROUP` varchar(20) DEFAULT NULL,
  `BATCHID` varchar(80) DEFAULT NULL,
  `PARENTID` bigint(20) DEFAULT NULL,
  `SPECIALHANDLING` varchar(80) DEFAULT NULL,
  `JOBSETID` bigint(20) DEFAULT NULL,
  `CORECOUNT` tinyint(4) DEFAULT NULL,
  `NINPUTDATAFILES` int(11) DEFAULT NULL,
  `INPUTFILETYPE` varchar(32) DEFAULT NULL,
  `INPUTFILEPROJECT` varchar(64) DEFAULT NULL,
  `INPUTFILEBYTES` bigint(20) DEFAULT NULL,
  `NOUTPUTDATAFILES` int(11) DEFAULT NULL,
  `OUTPUTFILEBYTES` bigint(20) DEFAULT NULL,
  `JOBMETRICS` varchar(500) DEFAULT NULL,
  `WORKQUEUE_ID` int(11) DEFAULT NULL,
  `JEDITASKID` bigint(20) DEFAULT NULL,
  `JOBSUBSTATUS` varchar(80) DEFAULT NULL,
  `ACTUALCORECOUNT` int(11) DEFAULT NULL,
  KEY `J2013_JOBDEFID_PRODUSRNAME_IDX` (`JOBDEFINITIONID`,`PRODUSERNAME`),
  KEY `J2013_JOBSETID_PRODUSRNAME_IDX` (`JOBSETID`,`PRODUSERNAME`),
  KEY `J2013_PANDAID_IDX` (`PANDAID`),
  KEY `J2013_PRODUSRNAME_4ATTR_IDX` (`PRODUSERNAME`,`JOBSTATUS`,`PRODSOURCELABEL`,`JOBSETID`,`JOBDEFINITIONID`),
  KEY `J2013_TASKID_3ATTR_PANDAID_IDX` (`TASKID`,`PRODSOURCELABEL`,`JOBSTATUS`,`PROCESSINGTYPE`,`PANDAID`),
  KEY `J2013_JOBS_BATCHID_IDX` (`BATCHID`),
  KEY `J2013_JOBS_SPECIALHANDLING_IDX` (`SPECIALHANDLING`),
  KEY `J2013_JEDITASKID_IDX` (`JEDITASKID`),
  KEY `J2013_SITE_3ATTR_PANDAID_IDX` (`COMPUTINGSITE`,`JOBSTATUS`,`PRODSOURCELABEL`,`PROCESSINGTYPE`,`PANDAID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `y2013_jobsarchived`
--

LOCK TABLES `y2013_jobsarchived` WRITE;
/*!40000 ALTER TABLE `y2013_jobsarchived` DISABLE KEYS */;
/*!40000 ALTER TABLE `y2013_jobsarchived` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `y2013_metatable_arch`
--

DROP TABLE IF EXISTS `y2013_metatable_arch`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `y2013_metatable_arch` (
  `PANDAID` bigint(20) NOT NULL,
  `MODIFICATIONTIME` datetime NOT NULL,
  `METADATA` text,
  KEY `META2013_PANDAID_IDX` (`PANDAID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `y2013_metatable_arch`
--

LOCK TABLES `y2013_metatable_arch` WRITE;
/*!40000 ALTER TABLE `y2013_metatable_arch` DISABLE KEYS */;
/*!40000 ALTER TABLE `y2013_metatable_arch` ENABLE KEYS */;
UNLOCK TABLES;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2015-04-20 13:30:14

----------------------------- ---------------------------
--  Stored procedure migrated to MySQL - 11.09.2015 by Ruslan Mashinistov
--------------------------------------------------------

--------------------------------------------------------
--  Stored procedure `update_jobsactive_stats`
--------------------------------------------------------

DELIMITER //
DROP PROCEDURE IF EXISTS `update_jobsactive_stats`//
CREATE PROCEDURE `update_jobsactive_stats` ()
COMMENT 'Aggregates data for the active jobs'
BEGIN
    DELETE from mv_jobsactive4_stats;
    INSERT INTO mv_jobsactive4_stats
  	(CUR_DATE,
  	CLOUD,
  	COMPUTINGSITE,
  	COUNTRYGROUP,
  	WORKINGGROUP,
  	RELOCATIONFLAG,
  	JOBSTATUS,
  	PROCESSINGTYPE,
  	PRODSOURCELABEL,
  	CURRENTPRIORITY,
  	VO,
  	WORKQUEUE_ID,
  	NUM_OF_JOBS
  	)
  	SELECT
    	sysdate(),
    	cloud,
    	computingSite,
    	countrygroup,
    	workinggroup,
    	relocationflag,
    	jobStatus,
    	processingType,
    	prodSourceLabel,
    	TRUNCATE(currentPriority, -1) AS currentPriority,
    	VO,
    	WORKQUEUE_ID,
    	COUNT(*)  AS num_of_jobs
  	FROM jobsActive4
  	GROUP BY
    	sysdate(),
    	cloud,
    	computingSite,
    	countrygroup,
    	workinggroup,
    	relocationflag,
    	jobStatus,
    	processingType,
    	prodSourceLabel,
    	TRUNCATE(currentPriority, -1),
    	VO,
    	WORKQUEUE_ID;
commit;
END//
DELIMITER ;

--------------------------------------------------------
--  Event `update_jobsactive_stats_event`
--------------------------------------------------------

DROP EVENT IF EXISTS `update_jobsactive_stats_event`;
CREATE EVENT update_jobsactive_stats_event
    ON SCHEDULE EVERY 2 MINUTE
    DO
      CALL update_jobsactive_stats();
