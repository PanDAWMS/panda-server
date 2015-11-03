-- MySQL dump 10.13  Distrib 5.1.73, for redhat-linux-gnu (x86_64)
--
-- Host: localhost    Database: pandadb1
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
-- Table structure for table `auth_group`
--

DROP TABLE IF EXISTS `auth_group`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `auth_group` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(80) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `name` (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `auth_group_permissions`
--

DROP TABLE IF EXISTS `auth_group_permissions`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `auth_group_permissions` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `group_id` int(11) NOT NULL,
  `permission_id` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `group_id` (`group_id`,`permission_id`),
  KEY `auth_group_permissions_bda51c3c` (`group_id`),
  KEY `auth_group_permissions_1e014c8f` (`permission_id`),
  CONSTRAINT `group_id_refs_id_3cea63fe` FOREIGN KEY (`group_id`) REFERENCES `auth_group` (`id`),
  CONSTRAINT `permission_id_refs_id_a7792de1` FOREIGN KEY (`permission_id`) REFERENCES `auth_permission` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `auth_message`
--

DROP TABLE IF EXISTS `auth_message`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `auth_message` (
  `ID` bigint(20) NOT NULL,
  `USER_ID` bigint(20) NOT NULL,
  `MESSAGE` text,
  PRIMARY KEY (`ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `auth_permission`
--

DROP TABLE IF EXISTS `auth_permission`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `auth_permission` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(50) NOT NULL,
  `content_type_id` int(11) NOT NULL,
  `codename` varchar(100) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `content_type_id` (`content_type_id`,`codename`),
  KEY `auth_permission_e4470c6e` (`content_type_id`),
  CONSTRAINT `content_type_id_refs_id_728de91f` FOREIGN KEY (`content_type_id`) REFERENCES `django_content_type` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=19 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `auth_user`
--

DROP TABLE IF EXISTS `auth_user`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `auth_user` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `username` varchar(30) NOT NULL,
  `first_name` varchar(30) NOT NULL,
  `last_name` varchar(30) NOT NULL,
  `email` varchar(75) NOT NULL,
  `password` varchar(128) NOT NULL,
  `is_staff` tinyint(1) NOT NULL,
  `is_active` tinyint(1) NOT NULL,
  `is_superuser` tinyint(1) NOT NULL,
  `last_login` datetime NOT NULL,
  `date_joined` datetime NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `username` (`username`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `auth_user_groups`
--

DROP TABLE IF EXISTS `auth_user_groups`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `auth_user_groups` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `user_id` int(11) NOT NULL,
  `group_id` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `user_id` (`user_id`,`group_id`),
  KEY `auth_user_groups_fbfc09f1` (`user_id`),
  KEY `auth_user_groups_bda51c3c` (`group_id`),
  CONSTRAINT `group_id_refs_id_f0ee9890` FOREIGN KEY (`group_id`) REFERENCES `auth_group` (`id`),
  CONSTRAINT `user_id_refs_id_831107f1` FOREIGN KEY (`user_id`) REFERENCES `auth_user` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `auth_user_user_permissions`
--

DROP TABLE IF EXISTS `auth_user_user_permissions`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `auth_user_user_permissions` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `user_id` int(11) NOT NULL,
  `permission_id` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `user_id` (`user_id`,`permission_id`),
  KEY `auth_user_user_permissions_fbfc09f1` (`user_id`),
  KEY `auth_user_user_permissions_1e014c8f` (`permission_id`),
  CONSTRAINT `permission_id_refs_id_67e79cb` FOREIGN KEY (`permission_id`) REFERENCES `auth_permission` (`id`),
  CONSTRAINT `user_id_refs_id_f2045483` FOREIGN KEY (`user_id`) REFERENCES `auth_user` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

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
  `MODTIME` datetime NOT NULL,
  `USETIME` datetime NOT NULL,
  `UPDMIN` int(11) DEFAULT NULL,
  `DATA` text,
  PRIMARY KEY (`TYPE`,`VALUE`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `certificates`
--

DROP TABLE IF EXISTS `certificates`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `certificates` (
  `ID` int(11) NOT NULL,
  `CERT` varchar(4000) NOT NULL,
  PRIMARY KEY (`ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

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
  PRIMARY KEY (`CLASS`,`NAME`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

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
  `WEIGHT` int(11) NOT NULL,
  `SERVER` varchar(100) NOT NULL,
  `STATUS` varchar(20) NOT NULL,
  `TRANSTIMELO` int(11) NOT NULL,
  `TRANSTIMEHI` int(11) NOT NULL,
  `WAITTIME` int(11) NOT NULL,
  `COMMENT_` varchar(200) DEFAULT NULL,
  `SPACE` int(11) NOT NULL,
  `MODUSER` varchar(30) DEFAULT NULL,
  `MODTIME` datetime NOT NULL,
  `VALIDATION` varchar(20) DEFAULT NULL,
  `MCSHARE` int(11) NOT NULL,
  `COUNTRIES` varchar(80) DEFAULT NULL,
  `FASTTRACK` varchar(20) DEFAULT NULL,
  `NPRESTAGE` bigint(20) NOT NULL,
  `PILOTOWNERS` varchar(300) DEFAULT NULL,
  `DN` varchar(100) DEFAULT NULL,
  `EMAIL` varchar(60) DEFAULT NULL,
  `FAIRSHARE` varchar(128) DEFAULT NULL,
  PRIMARY KEY (`NAME`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `cloudspace`
--

DROP TABLE IF EXISTS `cloudspace`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `cloudspace` (
  `CLOUD` varchar(20) NOT NULL,
  `STORE` varchar(50) NOT NULL,
  `SPACE` int(11) NOT NULL,
  `FREESPACE` int(11) NOT NULL,
  `MODUSER` varchar(30) NOT NULL,
  `MODTIME` datetime NOT NULL,
  PRIMARY KEY (`CLOUD`,`STORE`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

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
  `TMOD` datetime NOT NULL,
  `TENTER` datetime NOT NULL DEFAULT '1970-01-01 00:00:00',
  PRIMARY KEY (`ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

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
) ENGINE=InnoDB AUTO_INCREMENT=5 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

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
  `TYPE` varchar(20) NOT NULL,
  `STATUS` varchar(10) DEFAULT NULL,
  `NUMBERFILES` int(11) DEFAULT NULL,
  `CURRENTFILES` int(11) DEFAULT NULL,
  `CREATIONDATE` datetime DEFAULT NULL,
  `MODIFICATIONDATE` datetime NOT NULL,
  `MOVERID` bigint(20) NOT NULL DEFAULT '0',
  `TRANSFERSTATUS` tinyint(4) NOT NULL DEFAULT '0',
  `SUBTYPE` varchar(5) DEFAULT NULL,
  PRIMARY KEY (`VUID`,`MODIFICATIONDATE`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `deft_dataset`
--

DROP TABLE IF EXISTS `deft_dataset`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `deft_dataset` (
  `DATASET_ID` varchar(256) NOT NULL,
  `DATASET_META` bigint(20) DEFAULT NULL,
  `DATASET_STATE` varchar(16) DEFAULT NULL,
  `DATASET_SOURCE` bigint(20) DEFAULT NULL,
  `DATASET_TARGET` bigint(20) DEFAULT NULL,
  `DATASET_COMMENT` varchar(128) DEFAULT NULL,
  PRIMARY KEY (`DATASET_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `deft_meta`
--

DROP TABLE IF EXISTS `deft_meta`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `deft_meta` (
  `META_ID` bigint(20) NOT NULL,
  `META_STATE` varchar(16) DEFAULT NULL,
  `META_COMMENT` varchar(128) DEFAULT NULL,
  `META_REQ_TS` datetime DEFAULT NULL,
  `META_UPD_TS` datetime DEFAULT NULL,
  `META_REQUESTOR` varchar(16) DEFAULT NULL,
  `META_MANAGER` varchar(16) DEFAULT NULL,
  `META_VO` varchar(16) DEFAULT NULL,
  PRIMARY KEY (`META_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `deft_task`
--

DROP TABLE IF EXISTS `deft_task`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `deft_task` (
  `TASK_ID` bigint(20) NOT NULL AUTO_INCREMENT,
  `TASK_META` bigint(20) DEFAULT NULL,
  `TASK_STATE` varchar(16) DEFAULT NULL,
  `TASK_PARAM` text,
  `TASK_TAG` varchar(16) DEFAULT NULL,
  `TASK_COMMENT` varchar(128) DEFAULT NULL,
  `TASK_VO` varchar(16) DEFAULT NULL,
  `TASK_TRANSPATH` varchar(128) DEFAULT NULL,
  PRIMARY KEY (`TASK_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `django_admin_log`
--

DROP TABLE IF EXISTS `django_admin_log`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `django_admin_log` (
  `ID` bigint(20) NOT NULL,
  `ACTION_TIME` datetime NOT NULL,
  `USER_ID` bigint(20) NOT NULL,
  `CONTENT_TYPE_ID` bigint(20) DEFAULT NULL,
  `OBJECT_ID` text,
  `OBJECT_REPR` varchar(400) DEFAULT NULL,
  `ACTION_FLAG` bigint(20) NOT NULL,
  `CHANGE_MESSAGE` text,
  PRIMARY KEY (`ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `django_content_type`
--

DROP TABLE IF EXISTS `django_content_type`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `django_content_type` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(100) NOT NULL,
  `app_label` varchar(100) NOT NULL,
  `model` varchar(100) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `app_label` (`app_label`,`model`)
) ENGINE=InnoDB AUTO_INCREMENT=7 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `django_session`
--

DROP TABLE IF EXISTS `django_session`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `django_session` (
  `session_key` varchar(40) NOT NULL,
  `session_data` longtext NOT NULL,
  `expire_date` datetime NOT NULL,
  PRIMARY KEY (`session_key`),
  KEY `django_session_c25c2c28` (`expire_date`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `django_site`
--

DROP TABLE IF EXISTS `django_site`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `django_site` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `domain` varchar(100) NOT NULL,
  `name` varchar(50) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

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
  `LASTUSE` datetime NOT NULL,
  `PINSTATE` varchar(10) DEFAULT NULL,
  `PINTIME` datetime NOT NULL,
  `LIFETIME` datetime NOT NULL,
  `SITE` varchar(60) DEFAULT NULL,
  `PAR1` varchar(30) DEFAULT NULL,
  `PAR2` varchar(30) DEFAULT NULL,
  `PAR3` varchar(30) DEFAULT NULL,
  `PAR4` varchar(30) DEFAULT NULL,
  `PAR5` varchar(30) DEFAULT NULL,
  `PAR6` varchar(30) DEFAULT NULL,
  PRIMARY KEY (`ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `etask`
--

DROP TABLE IF EXISTS `etask`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `etask` (
  `TASKID` int(11) NOT NULL,
  `CREATIONTIME` date DEFAULT '1970-01-01',
  `MODIFICATIONTIME` date DEFAULT '1970-01-01',
  `TASKNAME` varchar(128) DEFAULT NULL,
  `STATUS` varchar(64) DEFAULT NULL,
  `ACTUALPARS` varchar(2000) DEFAULT NULL,
  `CPUCOUNT` int(11) DEFAULT NULL,
  `CPUUNIT` varchar(32) DEFAULT NULL,
  `DISKCOUNT` int(11) DEFAULT NULL,
  `DISKUNIT` varchar(32) DEFAULT NULL,
  `RAMCOUNT` int(11) DEFAULT NULL,
  `RAMUNIT` varchar(32) DEFAULT NULL,
  `OUTIP` varchar(3) DEFAULT NULL,
  `USERNAME` varchar(128) DEFAULT NULL,
  `USERGROUP` varchar(32) DEFAULT NULL,
  `USERROLE` varchar(32) DEFAULT NULL,
  `TASKTYPE` varchar(32) DEFAULT NULL,
  `GRID` varchar(32) DEFAULT NULL,
  `TRANSFK` int(11) DEFAULT NULL,
  `TRANSUSES` varchar(256) DEFAULT NULL,
  `TRANSHOME` varchar(128) DEFAULT NULL,
  `TRANSPATH` varchar(128) DEFAULT NULL,
  `TRANSFORMALPARS` varchar(2000) DEFAULT NULL,
  `TIER` varchar(12) DEFAULT NULL,
  `NDONE` int(11) DEFAULT '0',
  `NTOTAL` int(11) DEFAULT NULL,
  `NEVENTS` bigint(20) DEFAULT NULL,
  `RELPRIORITY` varchar(10) DEFAULT NULL,
  `EXPEVTPERJOB` bigint(20) DEFAULT NULL,
  `TASKTRANSINFO` varchar(512) DEFAULT NULL,
  `EXTID1` bigint(20) DEFAULT NULL,
  `REQID` bigint(20) DEFAULT NULL,
  `EXPNTOTAL` bigint(20) DEFAULT NULL,
  `CMTCONFIG` varchar(256) DEFAULT NULL,
  `SITE` varchar(128) DEFAULT NULL,
  `TASKTYPE2` varchar(64) DEFAULT NULL,
  `TASKPRIORITY` int(11) DEFAULT NULL,
  `PARTID` varchar(64) DEFAULT NULL,
  `TASKPARS` varchar(1024) DEFAULT NULL,
  `FILLSTATUS` varchar(64) DEFAULT NULL,
  `RW` bigint(20) DEFAULT NULL,
  `JOBSREMAINING` bigint(20) DEFAULT NULL,
  `CPUPERJOB` int(11) DEFAULT NULL,
  PRIMARY KEY (`TASKID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

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
  `ATTEMPTNR` int(11) DEFAULT NULL,
  PRIMARY KEY (`ROW_ID`,`MODIFICATIONTIME`)
) ENGINE=InnoDB AUTO_INCREMENT=1505 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `filestable_arch`
--

DROP TABLE IF EXISTS `filestable_arch`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `filestable_arch` (
  `ROW_ID` bigint(11) NOT NULL,
  `PANDAID` bigint(11) NOT NULL DEFAULT '0',
  `MODIFICATIONTIME` datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
  `CREATIONTIME` datetime DEFAULT NULL,
  `GUID` varchar(64) DEFAULT NULL,
  `LFN` varchar(256) DEFAULT NULL,
  `TYPE` varchar(20) DEFAULT NULL,
  `FSIZE` bigint(19) DEFAULT '0',
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
  `JEDITASKID` bigint(11) DEFAULT NULL,
  `DATASETID` bigint(11) DEFAULT NULL,
  `FILEID` bigint(11) DEFAULT NULL,
  `ATTEMPTNR` mediumint(3) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

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
) ENGINE=InnoDB AUTO_INCREMENT=5 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

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
  PRIMARY KEY (`ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `history`
--

DROP TABLE IF EXISTS `history`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `history` (
  `ID` int(11) NOT NULL,
  `ENTRYTIME` datetime NOT NULL,
  `STARTTIME` datetime NOT NULL,
  `ENDTIME` datetime NOT NULL,
  `CPU` bigint(20) DEFAULT NULL,
  `CPUXP` bigint(20) DEFAULT NULL,
  `SPACE` int(11) DEFAULT NULL,
  PRIMARY KEY (`ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `incidents`
--

DROP TABLE IF EXISTS `incidents`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `incidents` (
  `AT_TIME` datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
  `TYPEKEY` varchar(20) DEFAULT NULL,
  `DESCRIPTION` varchar(200) DEFAULT NULL,
  PRIMARY KEY (`AT_TIME`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `infomodels_sitestatus`
--

DROP TABLE IF EXISTS `infomodels_sitestatus`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `infomodels_sitestatus` (
  `ID` bigint(20) NOT NULL,
  `SITENAME` varchar(60) DEFAULT NULL,
  `ACTIVE` tinyint(4) DEFAULT NULL,
  PRIMARY KEY (`ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

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
  PRIMARY KEY (`SITEID`,`RELEASE`,`CACHE`,`CMTCONFIG`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `installedsw_old`
--

DROP TABLE IF EXISTS `installedsw_old`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `installedsw_old` (
  `SITEID` varchar(60) NOT NULL DEFAULT '',
  `CLOUD` varchar(10) DEFAULT NULL,
  `RELEASE` varchar(10) DEFAULT NULL,
  `CACHE` varchar(40) DEFAULT NULL,
  `VALIDATION` varchar(10) DEFAULT NULL,
  PRIMARY KEY (`SITEID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

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
  PRIMARY KEY (`NAME`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

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
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

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
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

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
  `ATTEMPTNR` smallint(6) DEFAULT NULL,
  `MAXATTEMPT` smallint(6) DEFAULT NULL,
  `NEVENTS` int(11) DEFAULT NULL,
  `KEEPTRACK` tinyint(4) DEFAULT NULL,
  `STARTEVENT` int(11) DEFAULT NULL,
  `ENDEVENT` int(11) DEFAULT NULL,
  `FIRSTEVENT` int(11) DEFAULT NULL,
  `BOUNDARYID` bigint(20) DEFAULT NULL,
  `PANDAID` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`JEDITASKID`,`DATASETID`,`FILEID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

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
  `CONTAINERNAME` varchar(132) DEFAULT NULL,
  `STATUS` varchar(20) DEFAULT NULL,
  `STATE` varchar(20) DEFAULT NULL,
  `STATECHECKTIME` datetime DEFAULT NULL,
  `STATECHECKEXPIRATION` datetime DEFAULT NULL,
  `FROZENTIME` datetime DEFAULT NULL,
  `NFILES` int(11) DEFAULT NULL,
  `NFILESTOBEUSED` int(11) DEFAULT NULL,
  `NFILESUSED` int(11) DEFAULT NULL,
  `NEVENTS` bigint(20) DEFAULT NULL,
  `NEVENTSTOBEUSED` bigint(20) DEFAULT NULL,
  `NEVENTSUSED` bigint(20) DEFAULT NULL,
  `LOCKEDBY` varchar(40) DEFAULT NULL,
  `LOCKEDTIME` datetime DEFAULT NULL,
  `NFILESFINISHED` int(11) DEFAULT NULL,
  `NFILESFAILED` int(11) DEFAULT NULL,
  `ATTRIBUTES` varchar(100) DEFAULT NULL,
  `STREAMNAME` varchar(20) DEFAULT NULL,
  `STORAGETOKEN` varchar(60) DEFAULT NULL,
  `DESTINATION` varchar(60) DEFAULT NULL,
  `NFILESONHOLD` int(11) DEFAULT NULL,
  `TEMPLATEID` bigint(11) DEFAULT NULL,
  PRIMARY KEY (`JEDITASKID`,`DATASETID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

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
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

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
  `DEF_MIN_EVENTID` int(11) DEFAULT NULL,
  `DEF_MAX_EVENTID` int(11) DEFAULT NULL,
  `PROCESSED_UPTO_EVENTID` int(11) DEFAULT NULL,
  `DATASETID` bigint(11) DEFAULT NULL,
  `STATUS` tinyint(2) DEFAULT NULL,
  `ATTEMPTNR` mediumint(9) DEFAULT NULL,
  PRIMARY KEY (`JEDITASKID`,`PANDAID`,`FILEID`,`JOB_PROCESSID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `jedi_job_retry_history`
--

DROP TABLE IF EXISTS `jedi_job_retry_history`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `jedi_job_retry_history` (
  `JEDITASKID` int(11) NOT NULL,
  `OLDPANDAID` int(11) NOT NULL,
  `NEWPANDAID` int(11) NOT NULL,
  `INS_UTC_TSTAMP` int(11) DEFAULT '0',
  `RELATIONTYPE` varchar(16) DEFAULT NULL,
  PRIMARY KEY (`JEDITASKID`,`OLDPANDAID`,`NEWPANDAID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `jedi_jobparams_template`
--

DROP TABLE IF EXISTS `jedi_jobparams_template`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `jedi_jobparams_template` (
  `JEDITASKID` bigint(20) NOT NULL AUTO_INCREMENT,
  `JOBPARAMSTEMPLATE` text,
  PRIMARY KEY (`JEDITASKID`)
) ENGINE=InnoDB AUTO_INCREMENT=1150 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

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
  PRIMARY KEY (`JEDITASKID`,`DATASETID`,`OUTTEMPID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

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
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `jedi_taskparams`
--

DROP TABLE IF EXISTS `jedi_taskparams`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `jedi_taskparams` (
  `JEDITASKID` bigint(20) NOT NULL,
  `TASKPARAMS` text,
  PRIMARY KEY (`JEDITASKID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `jedi_tasks`
--

DROP TABLE IF EXISTS `jedi_tasks`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `jedi_tasks` (
  `JEDITASKID` bigint(20) NOT NULL,
  `TASKNAME` varchar(128) DEFAULT NULL,
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
  `PROGRESS` smallint(6) DEFAULT NULL,
  `FAILURERATE` smallint(6) DEFAULT NULL,
  `ERRORDIALOG` varchar(255) DEFAULT NULL,
  `COUNTRYGROUP` varchar(20) DEFAULT NULL,
  `PARENT_TID` bigint(12) DEFAULT NULL,
  `EVENTSERVICE` tinyint(4) DEFAULT NULL,
  `TICKETID` varchar(50) DEFAULT NULL,
  `TICKETSYSTEMTYPE` varchar(16) DEFAULT NULL,
  `STATECHANGETIME` datetime DEFAULT NULL,
  `SUPERSTATUS` varchar(64) DEFAULT NULL,
  `CAMPAIGN` varchar(32) DEFAULT NULL,
  PRIMARY KEY (`JEDITASKID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

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
  `QUEUE_SHARE` smallint(6) DEFAULT NULL,
  `QUEUE_ORDER` smallint(6) DEFAULT NULL,
  `CRITERIA` varchar(256) DEFAULT NULL,
  `VARIABLES` varchar(256) DEFAULT NULL,
  PRIMARY KEY (`QUEUE_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

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
  PRIMARY KEY (`ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

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
  PRIMARY KEY (`PANDAID`,`MODIFICATIONTIME`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `jobparamstable_arch`
--

DROP TABLE IF EXISTS `jobparamstable_arch`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `jobparamstable_arch` (
  `PANDAID` bigint(20) NOT NULL DEFAULT '0',
  `MODIFICATIONTIME` datetime NOT NULL DEFAULT '1970-01-01 00:00:00',
  `JOBPARAMETERS` text
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `jobs_progress_tracking_old`
--

DROP TABLE IF EXISTS `jobs_progress_tracking_old`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `jobs_progress_tracking_old` (
  `PANDAID` bigint(20) NOT NULL AUTO_INCREMENT,
  `FILEID` bigint(20) NOT NULL,
  `JOB_PROCESSID` int(11) NOT NULL,
  `DEF_MIN_EVENTID` int(11) DEFAULT NULL,
  `DEF_MAX_EVENTID` int(11) DEFAULT NULL,
  `PROCESSED_UPTO_EVENTID` int(11) DEFAULT NULL,
  PRIMARY KEY (`PANDAID`,`FILEID`,`JOB_PROCESSID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

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
  `MODIFICATIONHOST` varchar(128) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `jobsactive4`
--

DROP TABLE IF EXISTS `jobsactive4`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `jobsactive4` (
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
  `ATTEMPTNR` smallint(6) NOT NULL,
  `MAXATTEMPT` smallint(6) NOT NULL,
  `JOBSTATUS` varchar(15) NOT NULL,
  `JOBNAME` varchar(256) DEFAULT NULL,
  `MAXCPUCOUNT` int(11) NOT NULL,
  `MAXCPUUNIT` varchar(32) DEFAULT NULL,
  `MAXDISKCOUNT` int(11) NOT NULL,
  `MAXDISKUNIT` varchar(4) DEFAULT NULL,
  `IPCONNECTIVITY` varchar(5) DEFAULT NULL,
  `MINRAMCOUNT` int(11) NOT NULL,
  `MINRAMUNIT` varchar(2) DEFAULT NULL,
  `STARTTIME` datetime DEFAULT NULL,
  `ENDTIME` datetime DEFAULT NULL,
  `CPUCONSUMPTIONTIME` bigint(20) NOT NULL,
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
  `DDMERRORDIAG` varchar(500) DEFAULT NULL,
  `BROKERAGEERRORCODE` int(11) NOT NULL,
  `BROKERAGEERRORDIAG` varchar(250) DEFAULT NULL,
  `JOBDISPATCHERERRORCODE` int(11) NOT NULL,
  `JOBDISPATCHERERRORDIAG` varchar(250) DEFAULT NULL,
  `TASKBUFFERERRORCODE` int(11) NOT NULL,
  `TASKBUFFERERRORDIAG` varchar(300) DEFAULT NULL,
  `COMPUTINGSITE` varchar(128) DEFAULT NULL,
  `COMPUTINGELEMENT` varchar(128) DEFAULT NULL,
  `JOBPARAMETERS` text,
  `METADATA` text,
  `PRODDBLOCK` varchar(255) DEFAULT NULL,
  `DISPATCHDBLOCK` varchar(255) DEFAULT NULL,
  `DESTINATIONDBLOCK` varchar(255) DEFAULT NULL,
  `DESTINATIONSE` varchar(250) DEFAULT NULL,
  `NEVENTS` int(11) NOT NULL,
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
  `PRODUSERNAME` varchar(60) DEFAULT NULL,
  `NINPUTFILES` int(11) DEFAULT NULL,
  `COUNTRYGROUP` varchar(20) DEFAULT NULL,
  `BATCHID` varchar(80) DEFAULT NULL,
  `PARENTID` bigint(20) DEFAULT NULL,
  `SPECIALHANDLING` varchar(80) DEFAULT NULL,
  `JOBSETID` bigint(20) DEFAULT NULL,
  `CORECOUNT` smallint(6) DEFAULT NULL,
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
  `ACTUALCORECOUNT` mediumint(9) DEFAULT NULL,
  `REQID` int(10) DEFAULT NULL,
  PRIMARY KEY (`PANDAID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `jobsarch4_taskinfo59_stats_old`
--

DROP TABLE IF EXISTS `jobsarch4_taskinfo59_stats_old`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `jobsarch4_taskinfo59_stats_old` (
  `MODIFICATIONTIME` datetime NOT NULL,
  `CLOUD` varchar(50) DEFAULT NULL,
  `TASKID` int(11) DEFAULT NULL,
  `JOBSTATUS` varchar(15) NOT NULL,
  `PROCESSINGTYPE` varchar(64) DEFAULT NULL,
  `PRODSOURCELABEL` varchar(20) DEFAULT NULL,
  `NUM_OF_JOBS` int(11) DEFAULT NULL,
  `CUR_DATE` datetime DEFAULT NULL,
  PRIMARY KEY (`MODIFICATIONTIME`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `jobsarchived`
--

DROP TABLE IF EXISTS `jobsarchived`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `jobsarchived` (
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
  `ATTEMPTNR` smallint(6) NOT NULL DEFAULT '0',
  `MAXATTEMPT` smallint(6) NOT NULL DEFAULT '0',
  `JOBSTATUS` varchar(15) NOT NULL DEFAULT 'activated',
  `JOBNAME` varchar(256) DEFAULT NULL,
  `MAXCPUCOUNT` int(11) NOT NULL DEFAULT '0',
  `MAXCPUUNIT` varchar(32) DEFAULT NULL,
  `MAXDISKCOUNT` int(11) NOT NULL DEFAULT '0',
  `MAXDISKUNIT` varchar(4) DEFAULT NULL,
  `IPCONNECTIVITY` varchar(5) DEFAULT NULL,
  `MINRAMCOUNT` int(11) NOT NULL DEFAULT '0',
  `MINRAMUNIT` varchar(2) DEFAULT NULL,
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
  `TASKBUFFERERRORCODE` int(11) NOT NULL,
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
  `CORECOUNT` smallint(6) DEFAULT NULL,
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
  `ACTUALCORECOUNT` mediumint(9) DEFAULT NULL,
  `REQID` int(10) DEFAULT NULL,
  PRIMARY KEY (`PANDAID`,`MODIFICATIONTIME`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

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
  `ATTEMPTNR` smallint(6) NOT NULL DEFAULT '0',
  `MAXATTEMPT` smallint(6) NOT NULL DEFAULT '0',
  `JOBSTATUS` varchar(15) NOT NULL DEFAULT 'activated',
  `JOBNAME` varchar(256) DEFAULT NULL,
  `MAXCPUCOUNT` int(11) NOT NULL DEFAULT '0',
  `MAXCPUUNIT` varchar(32) DEFAULT NULL,
  `MAXDISKCOUNT` int(11) NOT NULL DEFAULT '0',
  `MAXDISKUNIT` varchar(4) DEFAULT NULL,
  `IPCONNECTIVITY` varchar(5) DEFAULT NULL,
  `MINRAMCOUNT` int(11) NOT NULL DEFAULT '0',
  `MINRAMUNIT` varchar(2) DEFAULT NULL,
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
  `TASKBUFFERERRORCODE` int(11) NOT NULL,
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
  `CORECOUNT` smallint(6) DEFAULT NULL,
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
  `ACTUALCORECOUNT` mediumint(9) DEFAULT NULL,
  `REQID` int(10) DEFAULT NULL,
  PRIMARY KEY (`PANDAID`,`MODIFICATIONTIME`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `jobsarchived4_wnlist_stats`
--

DROP TABLE IF EXISTS `jobsarchived4_wnlist_stats`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `jobsarchived4_wnlist_stats` (
  `MODIFICATIONTIME` datetime NOT NULL,
  `COMPUTINGSITE` varchar(128) DEFAULT NULL,
  `MODIFICATIONHOST` varchar(128) DEFAULT NULL,
  `JOBSTATUS` varchar(15) NOT NULL,
  `TRANSEXITCODE` varchar(128) DEFAULT NULL,
  `PRODSOURCELABEL` varchar(20) DEFAULT NULL,
  `NUM_OF_JOBS` int(11) DEFAULT NULL,
  `MAX_MODIFICATIONTIME` datetime DEFAULT NULL,
  `CUR_DATE` datetime DEFAULT NULL,
  PRIMARY KEY (`MODIFICATIONTIME`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `jobsdebug`
--

DROP TABLE IF EXISTS `jobsdebug`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `jobsdebug` (
  `PANDAID` bigint(20) NOT NULL,
  `STDOUT` varchar(2048) DEFAULT NULL,
  PRIMARY KEY (`PANDAID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

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
  `PRODSERIESLABEL` varchar(20) DEFAULT 'Roma',
  `PRODSOURCELABEL` varchar(20) DEFAULT 'managed',
  `PRODUSERID` varchar(250) DEFAULT NULL,
  `ASSIGNEDPRIORITY` int(11) NOT NULL DEFAULT '0',
  `CURRENTPRIORITY` int(11) NOT NULL DEFAULT '0',
  `ATTEMPTNR` decimal(2,0) NOT NULL DEFAULT '0',
  `MAXATTEMPT` decimal(2,0) NOT NULL DEFAULT '0',
  `JOBSTATUS` varchar(15) NOT NULL DEFAULT 'defined',
  `JOBNAME` varchar(256) DEFAULT NULL,
  `MAXCPUCOUNT` int(11) NOT NULL DEFAULT '0',
  `MAXCPUUNIT` varchar(32) DEFAULT NULL,
  `MAXDISKCOUNT` int(11) NOT NULL DEFAULT '0',
  `MAXDISKUNIT` varchar(4) DEFAULT NULL,
  `IPCONNECTIVITY` varchar(5) DEFAULT NULL,
  `MINRAMCOUNT` int(11) NOT NULL DEFAULT '0',
  `MINRAMUNIT` varchar(2) DEFAULT NULL,
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
  `CORECOUNT` smallint(6) DEFAULT NULL,
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
  `ACTUALCORECOUNT` mediumint(9) DEFAULT NULL,
  `REQID` int(10) DEFAULT NULL,
  PRIMARY KEY (`PANDAID`)
) ENGINE=InnoDB AUTO_INCREMENT=2887 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `jobsdefined4_pandaid_seq`
--

DROP TABLE IF EXISTS `jobsdefined4_pandaid_seq`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `jobsdefined4_pandaid_seq` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `col` bit(1) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `jobshtcondor`
--

DROP TABLE IF EXISTS `jobshtcondor`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `jobshtcondor` (
  `wmsid` bigint(20) NOT NULL DEFAULT '0',
  `GlobalJobId` varchar(100) NOT NULL DEFAULT 'unknown',
  `OWNER` varchar(60) DEFAULT NULL,
  `SUBMITTED` datetime DEFAULT '1970-01-01 00:00:00',
  `RUN_TIME` int(11) DEFAULT '0',
  `P_END_TIME` datetime DEFAULT '1970-01-01 00:00:00',
  `ST` varchar(1) DEFAULT 'I',
  `PRI` tinyint(4) DEFAULT '0',
  `SIZE` decimal(10,1) DEFAULT '0.0',
  `CMD` varchar(100) DEFAULT NULL,
  `HOST` varchar(120) DEFAULT NULL,
  `STATUS` varchar(11) DEFAULT 'unknown',
  `MANAGER` varchar(60) DEFAULT NULL,
  `EXECUTABLE` varchar(100) DEFAULT NULL,
  `GOODPUT` varchar(5) DEFAULT '?????',
  `CPU_UTIL` varchar(6) DEFAULT '?????',
  `MBPS` decimal(10,1) DEFAULT '0.0',
  `READ_` bigint(20) DEFAULT '0',
  `WRITE_` bigint(20) DEFAULT '0',
  `SEEK` bigint(20) DEFAULT '0',
  `XPUT` decimal(10,1) DEFAULT '0.0',
  `BUFSIZE` bigint(20) DEFAULT '0',
  `BLOCKSIZE` int(11) DEFAULT '0',
  `CPU_TIME` int(11) DEFAULT '0',
  `REMOVED` tinyint(1) DEFAULT '0',
  `CondorID` varchar(100) NOT NULL DEFAULT 'unknown',
  `P_START_TIME` datetime DEFAULT '1970-01-01 00:00:00',
  `P_MODIF_TIME` datetime DEFAULT '1970-01-01 00:00:00',
  `P_FACTORY` varchar(60) DEFAULT NULL,
  `P_SCHEDD` varchar(60) DEFAULT NULL,
  `P_DESCRIPTION` varchar(100) DEFAULT NULL,
  `P_STDOUT` varchar(255) DEFAULT NULL,
  `P_STDERR` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`wmsid`,`GlobalJobId`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

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
  `ATTEMPTNR` decimal(2,0) NOT NULL DEFAULT '0',
  `MAXATTEMPT` decimal(2,0) NOT NULL DEFAULT '0',
  `JOBSTATUS` varchar(15) NOT NULL DEFAULT 'waiting',
  `JOBNAME` varchar(256) DEFAULT NULL,
  `MAXCPUCOUNT` int(11) NOT NULL DEFAULT '0',
  `MAXCPUUNIT` varchar(32) DEFAULT NULL,
  `MAXDISKCOUNT` int(11) NOT NULL DEFAULT '0',
  `MAXDISKUNIT` varchar(4) DEFAULT NULL,
  `IPCONNECTIVITY` varchar(5) DEFAULT NULL,
  `MINRAMCOUNT` int(11) NOT NULL DEFAULT '0',
  `MINRAMUNIT` varchar(2) DEFAULT NULL,
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
  `CORECOUNT` smallint(6) DEFAULT NULL,
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
  `ACTUALCORECOUNT` mediumint(9) DEFAULT NULL,
  `REQID` int(10) DEFAULT NULL,
  PRIMARY KEY (`PANDAID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `logstable`
--

DROP TABLE IF EXISTS `logstable`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `logstable` (
  `PANDAID` int(11) NOT NULL,
  `LOG1` text NOT NULL,
  `LOG2` text NOT NULL,
  `LOG3` text NOT NULL,
  `LOG4` text NOT NULL,
  PRIMARY KEY (`PANDAID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

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
  `SINCE` datetime NOT NULL,
  PRIMARY KEY (`UNAME`,`GNAME`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

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
  PRIMARY KEY (`PANDAID`,`MODIFICATIONTIME`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `metatable_arch`
--

DROP TABLE IF EXISTS `metatable_arch`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `metatable_arch` (
  `PANDAID` bigint(20) NOT NULL DEFAULT '0',
  `MODIFICATIONTIME` datetime NOT NULL DEFAULT '1970-01-01 00:00:00',
  `METADATA` text
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `mv_jobsactive4_stats`
--

DROP TABLE IF EXISTS `mv_jobsactive4_stats`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `mv_jobsactive4_stats` (
  `ID` bigint(20) NOT NULL AUTO_INCREMENT,
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
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `old_subcounter`
--

DROP TABLE IF EXISTS `old_subcounter`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `old_subcounter` (
  `SUBID` bigint(20) NOT NULL,
  PRIMARY KEY (`SUBID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

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
  PRIMARY KEY (`NAME`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `pandaids_deleted`
--

DROP TABLE IF EXISTS `pandaids_deleted`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `pandaids_deleted` (
  `PANDAID` bigint(20) NOT NULL DEFAULT '0',
  `TSTAMP_DATADEL` datetime DEFAULT NULL,
  PRIMARY KEY (`PANDAID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

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
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `pandalog`
--

DROP TABLE IF EXISTS `pandalog`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `pandalog` (
  `ID` bigint(20) NOT NULL AUTO_INCREMENT,
  `BINTIME` datetime NOT NULL,
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
  PRIMARY KEY (`ID`)
) ENGINE=InnoDB AUTO_INCREMENT=5 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `passwords`
--

DROP TABLE IF EXISTS `passwords`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `passwords` (
  `ID` int(11) NOT NULL,
  `PASS` varchar(60) NOT NULL,
  PRIMARY KEY (`ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

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
  `TCHECK` datetime NOT NULL,
  `STATE` varchar(30) NOT NULL,
  `TSTATE` datetime NOT NULL,
  `TENTER` datetime NOT NULL,
  `TSUBMIT` datetime NOT NULL,
  `TACCEPT` datetime NOT NULL,
  `TSCHEDULE` datetime NOT NULL,
  `TSTART` datetime NOT NULL,
  `TEND` datetime NOT NULL,
  `TDONE` datetime NOT NULL,
  `TRETRIEVE` datetime NOT NULL,
  `STATUS` varchar(20) NOT NULL,
  `ERRCODE` int(11) NOT NULL,
  `ERRINFO` varchar(150) NOT NULL,
  `MESSAGE` varchar(4000) DEFAULT NULL,
  `SCHEDD_NAME` varchar(60) NOT NULL,
  `WORKERNODE` varchar(60) NOT NULL,
  PRIMARY KEY (`JOBID`,`NICKNAME`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

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
  `TCHECK` datetime NOT NULL,
  `STATE` varchar(30) NOT NULL,
  `TSTATE` datetime NOT NULL,
  `TENTER` datetime NOT NULL,
  `TSUBMIT` datetime NOT NULL,
  `TACCEPT` datetime NOT NULL,
  `TSCHEDULE` datetime NOT NULL,
  `TSTART` datetime NOT NULL,
  `TEND` datetime NOT NULL,
  `TDONE` datetime NOT NULL,
  `TRETRIEVE` datetime NOT NULL,
  `STATUS` varchar(20) NOT NULL,
  `ERRCODE` int(11) NOT NULL,
  `ERRINFO` varchar(150) NOT NULL,
  `MESSAGE` varchar(4000) DEFAULT NULL,
  `WORKERNODE` varchar(60) NOT NULL,
  PRIMARY KEY (`TPID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

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
  `CREATED` datetime NOT NULL,
  `EXPIRES` datetime NOT NULL DEFAULT '1970-01-01 00:00:00',
  `SCHEDULERID` varchar(80) DEFAULT NULL,
  PRIMARY KEY (`TOKEN`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

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
  PRIMARY KEY (`NAME`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `pool_coll_lock`
--

DROP TABLE IF EXISTS `pool_coll_lock`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `pool_coll_lock` (
  `ID` varchar(50) NOT NULL,
  `COLLECTION` varchar(500) DEFAULT NULL,
  `CLIENT_INFO` varchar(500) DEFAULT NULL,
  `LOCKTYPE` varchar(20) DEFAULT NULL,
  `TIMESTAMP` datetime DEFAULT NULL,
  PRIMARY KEY (`ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `pool_collection_data`
--

DROP TABLE IF EXISTS `pool_collection_data`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `pool_collection_data` (
  `ID` decimal(10,0) NOT NULL,
  `OID_1` decimal(10,0) DEFAULT NULL,
  `OID_2` decimal(10,0) DEFAULT NULL,
  `VAR_1_OID_1` decimal(10,0) DEFAULT NULL,
  `VAR_1_OID_2` decimal(10,0) DEFAULT NULL,
  `VAR_2_OID_1` decimal(10,0) DEFAULT NULL,
  `VAR_2_OID_2` decimal(10,0) DEFAULT NULL,
  `VAR_3` decimal(10,0) DEFAULT NULL,
  `VAR_4` decimal(10,0) DEFAULT NULL,
  `VAR_5` float DEFAULT NULL,
  `VAR_6` decimal(10,0) DEFAULT NULL,
  `VAR_7` decimal(10,0) DEFAULT NULL,
  `VAR_8` float DEFAULT NULL,
  `VAR_9` decimal(10,0) DEFAULT NULL,
  `VAR_10` decimal(10,0) DEFAULT NULL,
  `VAR_11` decimal(10,0) DEFAULT NULL,
  `VAR_12` float DEFAULT NULL,
  `VAR_13` float DEFAULT NULL,
  `VAR_14` float DEFAULT NULL,
  `VAR_15` decimal(1,0) DEFAULT NULL,
  `VAR_16` decimal(1,0) DEFAULT NULL,
  `VAR_17` decimal(1,0) DEFAULT NULL,
  `VAR_18` decimal(1,0) DEFAULT NULL,
  `VAR_19` float DEFAULT NULL,
  `VAR_20` float DEFAULT NULL,
  `VAR_21` float DEFAULT NULL,
  `VAR_22` float DEFAULT NULL,
  `VAR_23` float DEFAULT NULL,
  `VAR_24` float DEFAULT NULL,
  `VAR_25` float DEFAULT NULL,
  `VAR_26` float DEFAULT NULL,
  `VAR_27` float DEFAULT NULL,
  `VAR_28` float DEFAULT NULL,
  `VAR_29` float DEFAULT NULL,
  `VAR_30` float DEFAULT NULL,
  `VAR_31` decimal(10,0) DEFAULT NULL,
  `VAR_32` decimal(10,0) DEFAULT NULL,
  `VAR_33` decimal(10,0) DEFAULT NULL,
  `VAR_34` decimal(10,0) DEFAULT NULL,
  `VAR_35` decimal(10,0) DEFAULT NULL,
  `VAR_36` float DEFAULT NULL,
  `VAR_37` float DEFAULT NULL,
  `VAR_38` float DEFAULT NULL,
  `VAR_39` float DEFAULT NULL,
  `VAR_40` float DEFAULT NULL,
  `VAR_41` float DEFAULT NULL,
  `VAR_42` decimal(10,0) DEFAULT NULL,
  `VAR_43` decimal(10,0) DEFAULT NULL,
  `VAR_44` decimal(10,0) DEFAULT NULL,
  `VAR_45` decimal(10,0) DEFAULT NULL,
  `VAR_46` float DEFAULT NULL,
  `VAR_47` float DEFAULT NULL,
  `VAR_48` float DEFAULT NULL,
  `VAR_49` float DEFAULT NULL,
  `VAR_50` float DEFAULT NULL,
  `VAR_51` float DEFAULT NULL,
  `VAR_52` float DEFAULT NULL,
  `VAR_53` float DEFAULT NULL,
  `VAR_54` decimal(10,0) DEFAULT NULL,
  `VAR_55` decimal(10,0) DEFAULT NULL,
  `VAR_56` decimal(10,0) DEFAULT NULL,
  `VAR_57` decimal(10,0) DEFAULT NULL,
  `VAR_58` float DEFAULT NULL,
  `VAR_59` float DEFAULT NULL,
  `VAR_60` float DEFAULT NULL,
  `VAR_61` float DEFAULT NULL,
  `VAR_62` float DEFAULT NULL,
  `VAR_63` float DEFAULT NULL,
  `VAR_64` float DEFAULT NULL,
  `VAR_65` float DEFAULT NULL,
  `VAR_66` decimal(10,0) DEFAULT NULL,
  `VAR_67` decimal(10,0) DEFAULT NULL,
  `VAR_68` decimal(10,0) DEFAULT NULL,
  `VAR_69` decimal(10,0) DEFAULT NULL,
  `VAR_70` decimal(10,0) DEFAULT NULL,
  `VAR_71` decimal(10,0) DEFAULT NULL,
  `VAR_72` float DEFAULT NULL,
  `VAR_73` float DEFAULT NULL,
  `VAR_74` float DEFAULT NULL,
  `VAR_75` float DEFAULT NULL,
  `VAR_76` decimal(10,0) DEFAULT NULL,
  `VAR_77` decimal(10,0) DEFAULT NULL,
  `VAR_78` float DEFAULT NULL,
  `VAR_79` float DEFAULT NULL,
  `VAR_80` float DEFAULT NULL,
  `VAR_81` float DEFAULT NULL,
  `VAR_82` float DEFAULT NULL,
  `VAR_83` float DEFAULT NULL,
  `VAR_84` float DEFAULT NULL,
  `VAR_85` float DEFAULT NULL,
  `VAR_86` float DEFAULT NULL,
  `VAR_87` float DEFAULT NULL,
  `VAR_88` float DEFAULT NULL,
  `VAR_89` float DEFAULT NULL,
  `VAR_90` float DEFAULT NULL,
  `VAR_91` float DEFAULT NULL,
  `VAR_92` float DEFAULT NULL,
  `VAR_93` float DEFAULT NULL,
  `VAR_94` float DEFAULT NULL,
  `VAR_95` float DEFAULT NULL,
  `VAR_96` float DEFAULT NULL,
  `VAR_97` float DEFAULT NULL,
  `VAR_98` float DEFAULT NULL,
  `VAR_99` float DEFAULT NULL,
  `VAR_100` float DEFAULT NULL,
  `VAR_101` float DEFAULT NULL,
  `VAR_102` float DEFAULT NULL,
  `VAR_103` float DEFAULT NULL,
  `VAR_104` float DEFAULT NULL,
  `VAR_105` float DEFAULT NULL,
  `VAR_106` float DEFAULT NULL,
  `VAR_107` decimal(10,0) DEFAULT NULL,
  `VAR_108` decimal(10,0) DEFAULT NULL,
  `VAR_109` float DEFAULT NULL,
  `VAR_110` float DEFAULT NULL,
  `VAR_111` float DEFAULT NULL,
  `VAR_112` decimal(10,0) DEFAULT NULL,
  `VAR_113` decimal(10,0) DEFAULT NULL,
  `VAR_114` decimal(10,0) DEFAULT NULL,
  `VAR_115` decimal(10,0) DEFAULT NULL,
  `VAR_116` decimal(10,0) DEFAULT NULL,
  `VAR_117` decimal(10,0) DEFAULT NULL,
  `VAR_118` decimal(10,0) DEFAULT NULL,
  `VAR_119` decimal(10,0) DEFAULT NULL,
  `VAR_120` decimal(10,0) DEFAULT NULL,
  `VAR_121` decimal(10,0) DEFAULT NULL,
  `VAR_122` decimal(10,0) DEFAULT NULL,
  `VAR_123` decimal(10,0) DEFAULT NULL,
  `VAR_124` decimal(10,0) DEFAULT NULL,
  `VAR_125` decimal(10,0) DEFAULT NULL,
  `VAR_126` decimal(10,0) DEFAULT NULL,
  `VAR_127` decimal(10,0) DEFAULT NULL,
  `VAR_128` decimal(10,0) DEFAULT NULL,
  `VAR_129` decimal(10,0) DEFAULT NULL,
  `VAR_130` decimal(10,0) DEFAULT NULL,
  `VAR_131` decimal(10,0) DEFAULT NULL,
  `VAR_132` decimal(10,0) DEFAULT NULL,
  `VAR_133` decimal(10,0) DEFAULT NULL,
  `VAR_134` decimal(10,0) DEFAULT NULL,
  `VAR_135` decimal(10,0) DEFAULT NULL,
  `VAR_136` decimal(10,0) DEFAULT NULL,
  `VAR_137` decimal(10,0) DEFAULT NULL,
  `VAR_138` decimal(10,0) DEFAULT NULL,
  `VAR_139` decimal(10,0) DEFAULT NULL,
  `VAR_140` decimal(10,0) DEFAULT NULL,
  `VAR_141` decimal(10,0) DEFAULT NULL,
  `VAR_142` decimal(10,0) DEFAULT NULL,
  `VAR_143` decimal(10,0) DEFAULT NULL,
  `VAR_144` decimal(10,0) DEFAULT NULL,
  `VAR_145` decimal(10,0) DEFAULT NULL,
  `VAR_146` decimal(10,0) DEFAULT NULL,
  `VAR_147` decimal(10,0) DEFAULT NULL,
  `VAR_148` decimal(10,0) DEFAULT NULL,
  `VAR_149` decimal(10,0) DEFAULT NULL,
  `VAR_150` decimal(10,0) DEFAULT NULL,
  `VAR_151` decimal(10,0) DEFAULT NULL,
  `VAR_152` decimal(10,0) DEFAULT NULL,
  `VAR_153` decimal(10,0) DEFAULT NULL,
  `VAR_154` decimal(10,0) DEFAULT NULL,
  `VAR_155` decimal(10,0) DEFAULT NULL,
  `VAR_156` decimal(10,0) DEFAULT NULL,
  `VAR_157` decimal(10,0) DEFAULT NULL,
  `VAR_158` decimal(10,0) DEFAULT NULL,
  `VAR_159` decimal(10,0) DEFAULT NULL,
  `VAR_160` decimal(10,0) DEFAULT NULL,
  `VAR_161` decimal(10,0) DEFAULT NULL,
  `VAR_162` decimal(10,0) DEFAULT NULL,
  `VAR_163` decimal(10,0) DEFAULT NULL,
  `VAR_164` decimal(10,0) DEFAULT NULL,
  `VAR_165` decimal(10,0) DEFAULT NULL,
  `VAR_166` decimal(10,0) DEFAULT NULL,
  `VAR_167` decimal(10,0) DEFAULT NULL,
  `VAR_168` decimal(10,0) DEFAULT NULL,
  `VAR_169` decimal(10,0) DEFAULT NULL,
  `VAR_170` decimal(10,0) DEFAULT NULL,
  `VAR_171` decimal(10,0) DEFAULT NULL,
  `VAR_172` decimal(10,0) DEFAULT NULL,
  `VAR_173` decimal(10,0) DEFAULT NULL,
  `VAR_174` decimal(10,0) DEFAULT NULL,
  `VAR_175` decimal(10,0) DEFAULT NULL,
  `VAR_176` decimal(10,0) DEFAULT NULL,
  `VAR_177` decimal(10,0) DEFAULT NULL,
  `VAR_178` decimal(10,0) DEFAULT NULL,
  `VAR_179` decimal(10,0) DEFAULT NULL,
  `VAR_180` decimal(10,0) DEFAULT NULL,
  `VAR_181` decimal(10,0) DEFAULT NULL,
  `VAR_182` decimal(10,0) DEFAULT NULL,
  `VAR_183` decimal(10,0) DEFAULT NULL,
  `VAR_184` decimal(10,0) DEFAULT NULL,
  `VAR_185` decimal(10,0) DEFAULT NULL,
  `VAR_186` decimal(10,0) DEFAULT NULL,
  `VAR_187` decimal(10,0) DEFAULT NULL,
  `VAR_188` decimal(10,0) DEFAULT NULL,
  `VAR_189` decimal(10,0) DEFAULT NULL,
  `VAR_190` decimal(10,0) DEFAULT NULL,
  `VAR_191` decimal(10,0) DEFAULT NULL,
  `VAR_192` decimal(10,0) DEFAULT NULL,
  `VAR_193` decimal(10,0) DEFAULT NULL,
  `VAR_194` decimal(10,0) DEFAULT NULL,
  `VAR_195` decimal(10,0) DEFAULT NULL,
  `VAR_196` decimal(10,0) DEFAULT NULL,
  `VAR_197` decimal(10,0) DEFAULT NULL,
  `VAR_198` decimal(10,0) DEFAULT NULL,
  `VAR_199` decimal(10,0) DEFAULT NULL,
  `VAR_200` decimal(10,0) DEFAULT NULL,
  `VAR_201` decimal(5,0) DEFAULT NULL,
  `VAR_202` decimal(5,0) DEFAULT NULL,
  `VAR_203` decimal(5,0) DEFAULT NULL,
  `VAR_204` decimal(5,0) DEFAULT NULL,
  `VAR_205` decimal(5,0) DEFAULT NULL,
  `VAR_206` decimal(5,0) DEFAULT NULL,
  `VAR_207` decimal(5,0) DEFAULT NULL,
  `VAR_208` decimal(5,0) DEFAULT NULL,
  `VAR_209` decimal(5,0) DEFAULT NULL,
  `VAR_210` decimal(5,0) DEFAULT NULL,
  `VAR_211` decimal(5,0) DEFAULT NULL,
  `VAR_212` decimal(5,0) DEFAULT NULL,
  `VAR_213` decimal(5,0) DEFAULT NULL,
  `VAR_214` decimal(5,0) DEFAULT NULL,
  `VAR_215` decimal(5,0) DEFAULT NULL,
  `VAR_216` decimal(5,0) DEFAULT NULL,
  `VAR_217` decimal(5,0) DEFAULT NULL,
  `VAR_218` decimal(5,0) DEFAULT NULL,
  `VAR_219` decimal(5,0) DEFAULT NULL,
  `VAR_220` decimal(5,0) DEFAULT NULL,
  `VAR_221` decimal(5,0) DEFAULT NULL,
  `VAR_222` decimal(5,0) DEFAULT NULL,
  `VAR_223` decimal(5,0) DEFAULT NULL,
  `VAR_224` decimal(5,0) DEFAULT NULL,
  `VAR_225` decimal(5,0) DEFAULT NULL,
  `VAR_226` decimal(5,0) DEFAULT NULL,
  `VAR_227` decimal(5,0) DEFAULT NULL,
  `VAR_228` decimal(5,0) DEFAULT NULL,
  `VAR_229` decimal(5,0) DEFAULT NULL,
  `VAR_230` decimal(5,0) DEFAULT NULL,
  `VAR_231` decimal(5,0) DEFAULT NULL,
  `VAR_232` decimal(5,0) DEFAULT NULL,
  `VAR_233` decimal(5,0) DEFAULT NULL,
  `VAR_234` decimal(5,0) DEFAULT NULL,
  `VAR_235` decimal(5,0) DEFAULT NULL,
  `VAR_236` decimal(5,0) DEFAULT NULL,
  `VAR_237` decimal(5,0) DEFAULT NULL,
  `VAR_238` decimal(5,0) DEFAULT NULL,
  `VAR_239` decimal(5,0) DEFAULT NULL,
  `VAR_240` decimal(5,0) DEFAULT NULL,
  `VAR_241` decimal(5,0) DEFAULT NULL,
  `VAR_242` decimal(5,0) DEFAULT NULL,
  `VAR_243` decimal(5,0) DEFAULT NULL,
  `VAR_244` decimal(5,0) DEFAULT NULL,
  `VAR_245` decimal(5,0) DEFAULT NULL,
  `VAR_246` decimal(5,0) DEFAULT NULL,
  `VAR_247` decimal(5,0) DEFAULT NULL,
  `VAR_248` decimal(5,0) DEFAULT NULL,
  `VAR_249` decimal(5,0) DEFAULT NULL,
  `VAR_250` decimal(10,0) DEFAULT NULL,
  `VAR_251` decimal(10,0) DEFAULT NULL,
  `VAR_252` decimal(10,0) DEFAULT NULL,
  `VAR_253` decimal(10,0) DEFAULT NULL,
  `VAR_254` decimal(10,0) DEFAULT NULL,
  `VAR_255` decimal(10,0) DEFAULT NULL,
  `VAR_256` decimal(10,0) DEFAULT NULL,
  `VAR_257` decimal(10,0) DEFAULT NULL,
  `VAR_258` decimal(10,0) DEFAULT NULL,
  `VAR_259` decimal(10,0) DEFAULT NULL,
  `VAR_260` decimal(10,0) DEFAULT NULL,
  `VAR_261` decimal(10,0) DEFAULT NULL,
  `VAR_262` decimal(10,0) DEFAULT NULL,
  `VAR_263` float DEFAULT NULL,
  PRIMARY KEY (`ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `pool_collection_data_1`
--

DROP TABLE IF EXISTS `pool_collection_data_1`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `pool_collection_data_1` (
  `ID` decimal(10,0) NOT NULL,
  `OID_1` decimal(10,0) DEFAULT NULL,
  `OID_2` decimal(10,0) DEFAULT NULL,
  `VAR_1_OID_1` decimal(10,0) DEFAULT NULL,
  `VAR_1_OID_2` decimal(10,0) DEFAULT NULL,
  `VAR_2_OID_1` decimal(10,0) DEFAULT NULL,
  `VAR_2_OID_2` decimal(10,0) DEFAULT NULL,
  `VAR_3` decimal(10,0) DEFAULT NULL,
  `VAR_4` decimal(10,0) DEFAULT NULL,
  `VAR_5` float DEFAULT NULL,
  `VAR_6` decimal(10,0) DEFAULT NULL,
  `VAR_7` decimal(10,0) DEFAULT NULL,
  `VAR_8` float DEFAULT NULL,
  `VAR_9` decimal(10,0) DEFAULT NULL,
  `VAR_10` decimal(10,0) DEFAULT NULL,
  `VAR_11` decimal(10,0) DEFAULT NULL,
  `VAR_12` float DEFAULT NULL,
  `VAR_13` float DEFAULT NULL,
  `VAR_14` float DEFAULT NULL,
  `VAR_15` decimal(1,0) DEFAULT NULL,
  `VAR_16` decimal(1,0) DEFAULT NULL,
  `VAR_17` decimal(1,0) DEFAULT NULL,
  `VAR_18` decimal(1,0) DEFAULT NULL,
  `VAR_19` float DEFAULT NULL,
  `VAR_20` float DEFAULT NULL,
  `VAR_21` float DEFAULT NULL,
  `VAR_22` float DEFAULT NULL,
  `VAR_23` float DEFAULT NULL,
  `VAR_24` float DEFAULT NULL,
  `VAR_25` float DEFAULT NULL,
  `VAR_26` float DEFAULT NULL,
  `VAR_27` float DEFAULT NULL,
  `VAR_28` float DEFAULT NULL,
  `VAR_29` float DEFAULT NULL,
  `VAR_30` float DEFAULT NULL,
  `VAR_31` decimal(10,0) DEFAULT NULL,
  `VAR_32` decimal(10,0) DEFAULT NULL,
  `VAR_33` decimal(10,0) DEFAULT NULL,
  `VAR_34` decimal(10,0) DEFAULT NULL,
  `VAR_35` decimal(10,0) DEFAULT NULL,
  `VAR_36` float DEFAULT NULL,
  `VAR_37` float DEFAULT NULL,
  `VAR_38` float DEFAULT NULL,
  `VAR_39` float DEFAULT NULL,
  `VAR_40` float DEFAULT NULL,
  `VAR_41` float DEFAULT NULL,
  `VAR_42` decimal(10,0) DEFAULT NULL,
  `VAR_43` decimal(10,0) DEFAULT NULL,
  `VAR_44` decimal(10,0) DEFAULT NULL,
  `VAR_45` decimal(10,0) DEFAULT NULL,
  `VAR_46` float DEFAULT NULL,
  `VAR_47` float DEFAULT NULL,
  `VAR_48` float DEFAULT NULL,
  `VAR_49` float DEFAULT NULL,
  `VAR_50` float DEFAULT NULL,
  `VAR_51` float DEFAULT NULL,
  `VAR_52` float DEFAULT NULL,
  `VAR_53` float DEFAULT NULL,
  `VAR_54` decimal(10,0) DEFAULT NULL,
  `VAR_55` decimal(10,0) DEFAULT NULL,
  `VAR_56` decimal(10,0) DEFAULT NULL,
  `VAR_57` decimal(10,0) DEFAULT NULL,
  `VAR_58` float DEFAULT NULL,
  `VAR_59` float DEFAULT NULL,
  `VAR_60` float DEFAULT NULL,
  `VAR_61` float DEFAULT NULL,
  `VAR_62` float DEFAULT NULL,
  `VAR_63` float DEFAULT NULL,
  `VAR_64` float DEFAULT NULL,
  `VAR_65` float DEFAULT NULL,
  `VAR_66` decimal(10,0) DEFAULT NULL,
  `VAR_67` decimal(10,0) DEFAULT NULL,
  `VAR_68` decimal(10,0) DEFAULT NULL,
  `VAR_69` decimal(10,0) DEFAULT NULL,
  `VAR_70` decimal(10,0) DEFAULT NULL,
  `VAR_71` decimal(10,0) DEFAULT NULL,
  `VAR_72` float DEFAULT NULL,
  `VAR_73` float DEFAULT NULL,
  `VAR_74` float DEFAULT NULL,
  `VAR_75` float DEFAULT NULL,
  `VAR_76` decimal(10,0) DEFAULT NULL,
  `VAR_77` decimal(10,0) DEFAULT NULL,
  `VAR_78` float DEFAULT NULL,
  `VAR_79` float DEFAULT NULL,
  `VAR_80` float DEFAULT NULL,
  `VAR_81` float DEFAULT NULL,
  `VAR_82` float DEFAULT NULL,
  `VAR_83` float DEFAULT NULL,
  `VAR_84` float DEFAULT NULL,
  `VAR_85` float DEFAULT NULL,
  `VAR_86` float DEFAULT NULL,
  `VAR_87` float DEFAULT NULL,
  `VAR_88` float DEFAULT NULL,
  `VAR_89` float DEFAULT NULL,
  `VAR_90` float DEFAULT NULL,
  `VAR_91` float DEFAULT NULL,
  `VAR_92` float DEFAULT NULL,
  `VAR_93` float DEFAULT NULL,
  `VAR_94` float DEFAULT NULL,
  `VAR_95` float DEFAULT NULL,
  `VAR_96` float DEFAULT NULL,
  `VAR_97` float DEFAULT NULL,
  `VAR_98` float DEFAULT NULL,
  `VAR_99` float DEFAULT NULL,
  `VAR_100` float DEFAULT NULL,
  `VAR_101` float DEFAULT NULL,
  `VAR_102` float DEFAULT NULL,
  `VAR_103` float DEFAULT NULL,
  `VAR_104` float DEFAULT NULL,
  `VAR_105` float DEFAULT NULL,
  `VAR_106` float DEFAULT NULL,
  `VAR_107` decimal(10,0) DEFAULT NULL,
  `VAR_108` decimal(10,0) DEFAULT NULL,
  `VAR_109` float DEFAULT NULL,
  `VAR_110` float DEFAULT NULL,
  `VAR_111` float DEFAULT NULL,
  `VAR_112` decimal(10,0) DEFAULT NULL,
  `VAR_113` decimal(10,0) DEFAULT NULL,
  `VAR_114` decimal(10,0) DEFAULT NULL,
  `VAR_115` decimal(10,0) DEFAULT NULL,
  `VAR_116` decimal(10,0) DEFAULT NULL,
  `VAR_117` decimal(10,0) DEFAULT NULL,
  `VAR_118` decimal(10,0) DEFAULT NULL,
  `VAR_119` decimal(10,0) DEFAULT NULL,
  `VAR_120` decimal(10,0) DEFAULT NULL,
  `VAR_121` decimal(10,0) DEFAULT NULL,
  `VAR_122` decimal(10,0) DEFAULT NULL,
  `VAR_123` decimal(10,0) DEFAULT NULL,
  `VAR_124` decimal(10,0) DEFAULT NULL,
  `VAR_125` decimal(10,0) DEFAULT NULL,
  `VAR_126` decimal(10,0) DEFAULT NULL,
  `VAR_127` decimal(10,0) DEFAULT NULL,
  `VAR_128` decimal(10,0) DEFAULT NULL,
  `VAR_129` decimal(10,0) DEFAULT NULL,
  `VAR_130` decimal(10,0) DEFAULT NULL,
  `VAR_131` decimal(10,0) DEFAULT NULL,
  `VAR_132` decimal(10,0) DEFAULT NULL,
  `VAR_133` decimal(10,0) DEFAULT NULL,
  `VAR_134` decimal(10,0) DEFAULT NULL,
  `VAR_135` decimal(10,0) DEFAULT NULL,
  `VAR_136` decimal(10,0) DEFAULT NULL,
  `VAR_137` decimal(10,0) DEFAULT NULL,
  `VAR_138` decimal(10,0) DEFAULT NULL,
  `VAR_139` decimal(10,0) DEFAULT NULL,
  `VAR_140` decimal(10,0) DEFAULT NULL,
  `VAR_141` decimal(10,0) DEFAULT NULL,
  `VAR_142` decimal(10,0) DEFAULT NULL,
  `VAR_143` decimal(10,0) DEFAULT NULL,
  `VAR_144` decimal(10,0) DEFAULT NULL,
  `VAR_145` decimal(10,0) DEFAULT NULL,
  `VAR_146` decimal(10,0) DEFAULT NULL,
  `VAR_147` decimal(10,0) DEFAULT NULL,
  `VAR_148` decimal(10,0) DEFAULT NULL,
  `VAR_149` decimal(10,0) DEFAULT NULL,
  `VAR_150` decimal(10,0) DEFAULT NULL,
  `VAR_151` decimal(10,0) DEFAULT NULL,
  `VAR_152` decimal(10,0) DEFAULT NULL,
  `VAR_153` decimal(10,0) DEFAULT NULL,
  `VAR_154` decimal(10,0) DEFAULT NULL,
  `VAR_155` decimal(10,0) DEFAULT NULL,
  `VAR_156` decimal(10,0) DEFAULT NULL,
  `VAR_157` decimal(10,0) DEFAULT NULL,
  `VAR_158` decimal(10,0) DEFAULT NULL,
  `VAR_159` decimal(10,0) DEFAULT NULL,
  `VAR_160` decimal(10,0) DEFAULT NULL,
  `VAR_161` decimal(10,0) DEFAULT NULL,
  `VAR_162` decimal(10,0) DEFAULT NULL,
  `VAR_163` decimal(10,0) DEFAULT NULL,
  `VAR_164` decimal(10,0) DEFAULT NULL,
  `VAR_165` decimal(10,0) DEFAULT NULL,
  `VAR_166` decimal(10,0) DEFAULT NULL,
  `VAR_167` decimal(10,0) DEFAULT NULL,
  `VAR_168` decimal(10,0) DEFAULT NULL,
  `VAR_169` decimal(10,0) DEFAULT NULL,
  `VAR_170` decimal(10,0) DEFAULT NULL,
  `VAR_171` decimal(10,0) DEFAULT NULL,
  `VAR_172` decimal(10,0) DEFAULT NULL,
  `VAR_173` decimal(10,0) DEFAULT NULL,
  `VAR_174` decimal(10,0) DEFAULT NULL,
  `VAR_175` decimal(10,0) DEFAULT NULL,
  `VAR_176` decimal(10,0) DEFAULT NULL,
  `VAR_177` decimal(10,0) DEFAULT NULL,
  `VAR_178` decimal(10,0) DEFAULT NULL,
  `VAR_179` decimal(10,0) DEFAULT NULL,
  `VAR_180` decimal(10,0) DEFAULT NULL,
  `VAR_181` decimal(10,0) DEFAULT NULL,
  `VAR_182` decimal(10,0) DEFAULT NULL,
  `VAR_183` decimal(10,0) DEFAULT NULL,
  `VAR_184` decimal(10,0) DEFAULT NULL,
  `VAR_185` decimal(10,0) DEFAULT NULL,
  `VAR_186` decimal(10,0) DEFAULT NULL,
  `VAR_187` decimal(10,0) DEFAULT NULL,
  `VAR_188` decimal(10,0) DEFAULT NULL,
  `VAR_189` decimal(10,0) DEFAULT NULL,
  `VAR_190` decimal(10,0) DEFAULT NULL,
  `VAR_191` decimal(10,0) DEFAULT NULL,
  `VAR_192` decimal(10,0) DEFAULT NULL,
  `VAR_193` decimal(10,0) DEFAULT NULL,
  `VAR_194` decimal(10,0) DEFAULT NULL,
  `VAR_195` decimal(10,0) DEFAULT NULL,
  `VAR_196` decimal(10,0) DEFAULT NULL,
  `VAR_197` decimal(10,0) DEFAULT NULL,
  `VAR_198` decimal(10,0) DEFAULT NULL,
  `VAR_199` decimal(10,0) DEFAULT NULL,
  `VAR_200` decimal(10,0) DEFAULT NULL,
  `VAR_201` decimal(5,0) DEFAULT NULL,
  `VAR_202` decimal(5,0) DEFAULT NULL,
  `VAR_203` decimal(5,0) DEFAULT NULL,
  `VAR_204` decimal(5,0) DEFAULT NULL,
  `VAR_205` decimal(5,0) DEFAULT NULL,
  `VAR_206` decimal(5,0) DEFAULT NULL,
  `VAR_207` decimal(5,0) DEFAULT NULL,
  `VAR_208` decimal(5,0) DEFAULT NULL,
  `VAR_209` decimal(5,0) DEFAULT NULL,
  `VAR_210` decimal(5,0) DEFAULT NULL,
  `VAR_211` decimal(5,0) DEFAULT NULL,
  `VAR_212` decimal(5,0) DEFAULT NULL,
  `VAR_213` decimal(5,0) DEFAULT NULL,
  `VAR_214` decimal(5,0) DEFAULT NULL,
  `VAR_215` decimal(5,0) DEFAULT NULL,
  `VAR_216` decimal(5,0) DEFAULT NULL,
  `VAR_217` decimal(5,0) DEFAULT NULL,
  `VAR_218` decimal(5,0) DEFAULT NULL,
  `VAR_219` decimal(5,0) DEFAULT NULL,
  `VAR_220` decimal(5,0) DEFAULT NULL,
  `VAR_221` decimal(5,0) DEFAULT NULL,
  `VAR_222` decimal(5,0) DEFAULT NULL,
  `VAR_223` decimal(5,0) DEFAULT NULL,
  `VAR_224` decimal(5,0) DEFAULT NULL,
  `VAR_225` decimal(5,0) DEFAULT NULL,
  `VAR_226` decimal(5,0) DEFAULT NULL,
  `VAR_227` decimal(5,0) DEFAULT NULL,
  `VAR_228` decimal(5,0) DEFAULT NULL,
  `VAR_229` decimal(5,0) DEFAULT NULL,
  `VAR_230` decimal(5,0) DEFAULT NULL,
  `VAR_231` decimal(5,0) DEFAULT NULL,
  `VAR_232` decimal(5,0) DEFAULT NULL,
  `VAR_233` decimal(5,0) DEFAULT NULL,
  `VAR_234` decimal(5,0) DEFAULT NULL,
  `VAR_235` decimal(5,0) DEFAULT NULL,
  `VAR_236` decimal(5,0) DEFAULT NULL,
  `VAR_237` decimal(5,0) DEFAULT NULL,
  `VAR_238` decimal(5,0) DEFAULT NULL,
  `VAR_239` decimal(5,0) DEFAULT NULL,
  `VAR_240` decimal(5,0) DEFAULT NULL,
  `VAR_241` decimal(5,0) DEFAULT NULL,
  `VAR_242` decimal(5,0) DEFAULT NULL,
  `VAR_243` decimal(5,0) DEFAULT NULL,
  `VAR_244` decimal(5,0) DEFAULT NULL,
  `VAR_245` decimal(5,0) DEFAULT NULL,
  `VAR_246` decimal(5,0) DEFAULT NULL,
  `VAR_247` decimal(5,0) DEFAULT NULL,
  `VAR_248` decimal(5,0) DEFAULT NULL,
  `VAR_249` decimal(5,0) DEFAULT NULL,
  `VAR_250` decimal(10,0) DEFAULT NULL,
  `VAR_251` decimal(10,0) DEFAULT NULL,
  `VAR_252` decimal(10,0) DEFAULT NULL,
  `VAR_253` decimal(10,0) DEFAULT NULL,
  `VAR_254` decimal(10,0) DEFAULT NULL,
  `VAR_255` decimal(10,0) DEFAULT NULL,
  `VAR_256` decimal(10,0) DEFAULT NULL,
  `VAR_257` decimal(10,0) DEFAULT NULL,
  `VAR_258` decimal(10,0) DEFAULT NULL,
  `VAR_259` decimal(10,0) DEFAULT NULL,
  `VAR_260` decimal(10,0) DEFAULT NULL,
  `VAR_261` decimal(10,0) DEFAULT NULL,
  `VAR_262` decimal(10,0) DEFAULT NULL,
  `VAR_263` float DEFAULT NULL,
  PRIMARY KEY (`ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `pool_collections`
--

DROP TABLE IF EXISTS `pool_collections`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `pool_collections` (
  `COLLECTION_NAME` varchar(400) NOT NULL,
  `DATA_TABLE_NAME` varchar(400) DEFAULT NULL,
  `LINKS_TABLE_NAME` varchar(400) DEFAULT NULL,
  `RECORDS_WRITTEN` decimal(10,0) DEFAULT NULL,
  `RECORDS_DELETED` decimal(10,0) DEFAULT NULL,
  `CHILD_COLLECTION_NAME` varchar(400) DEFAULT NULL,
  `FOREIGN_KEY_NAME` varchar(400) DEFAULT NULL,
  PRIMARY KEY (`COLLECTION_NAME`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `pool_collections_desc`
--

DROP TABLE IF EXISTS `pool_collections_desc`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `pool_collections_desc` (
  `COLLECTION_NAME` varchar(400) NOT NULL DEFAULT '',
  `VARIABLE_NAME` varchar(400) DEFAULT NULL,
  `VARIABLE_TYPE` varchar(400) DEFAULT NULL,
  `VARIABLE_MAXIMUM_SIZE` decimal(10,0) DEFAULT NULL,
  `VARIABLE_SIZE_IS_FIXED` varchar(5) DEFAULT NULL,
  `VARIABLE_POSITION` decimal(10,0) DEFAULT NULL,
  `VARIABLE_ANNOTATION` varchar(4000) DEFAULT NULL,
  PRIMARY KEY (`COLLECTION_NAME`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `prodsys2_task_id_seq`
--

DROP TABLE IF EXISTS `prodsys2_task_id_seq`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `prodsys2_task_id_seq` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `col` bit(1) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `prodsys_comm`
--

DROP TABLE IF EXISTS `prodsys_comm`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `prodsys_comm` (
  `COMM_TASK` bigint(20) NOT NULL,
  `COMM_META` bigint(20) DEFAULT NULL,
  `COMM_OWNER` varchar(16) DEFAULT NULL,
  `COMM_CMD` varchar(256) DEFAULT NULL,
  `COMM_TS` bigint(20) DEFAULT NULL,
  `COMM_COMMENT` varchar(128) DEFAULT NULL,
  `COMM_PARAMETERS` text,
  PRIMARY KEY (`COMM_TASK`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

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
  `SITE` varchar(10) DEFAULT NULL,
  `SW_RELEASE` varchar(20) DEFAULT NULL,
  `GEOMETRY` varchar(20) DEFAULT NULL,
  `JOBID` int(11) DEFAULT NULL,
  `PANDAID` int(11) DEFAULT NULL,
  `PRODTIME` datetime DEFAULT NULL,
  `TIMESTAMP` int(11) DEFAULT NULL,
  PRIMARY KEY (`NAME`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

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
  `CREATED` datetime NOT NULL,
  `EXPIRES` datetime NOT NULL,
  `ORIGIN` varchar(80) NOT NULL,
  `MYPROXY` varchar(80) NOT NULL,
  PRIMARY KEY (`ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `proxykey_id_seq`
--

DROP TABLE IF EXISTS `proxykey_id_seq`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `proxykey_id_seq` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `col` bit(1) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=8 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `redirect`
--

DROP TABLE IF EXISTS `redirect`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `redirect` (
  `SERVICE` varchar(30) NOT NULL,
  `TYPE` varchar(30) NOT NULL,
  `SITE` varchar(30) NOT NULL,
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
  `STATUSTIME` datetime NOT NULL,
  `USETIME` datetime NOT NULL,
  PRIMARY KEY (`URL`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `request_stats`
--

DROP TABLE IF EXISTS `request_stats`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `request_stats` (
  `ID` int(11) NOT NULL AUTO_INCREMENT,
  `SERVER` varchar(40) NOT NULL DEFAULT '',
  `REMOTE` varchar(40) NOT NULL DEFAULT '',
  `URL` text,
  `LOAD` varchar(40) NOT NULL DEFAULT '',
  `MEM` varchar(40) NOT NULL DEFAULT '',
  `DESCRIPTION` text,
  `QTIME` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  `QDURATION` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  PRIMARY KEY (`ID`)
) ENGINE=MyISAM AUTO_INCREMENT=5130 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `savedpages`
--

DROP TABLE IF EXISTS `savedpages`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `savedpages` (
  `NAME` varchar(30) NOT NULL,
  `FLAG` varchar(20) NOT NULL,
  `HOURS` int(11) NOT NULL,
  `HTML` text NOT NULL,
  `LASTMOD` datetime DEFAULT NULL,
  `INTERVAL` int(11) DEFAULT NULL,
  PRIMARY KEY (`NAME`,`FLAG`,`HOURS`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `schedconfig`
--

DROP TABLE IF EXISTS `schedconfig`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `schedconfig` (
  `NAME` varchar(60) NOT NULL,
  `NICKNAME` varchar(60) NOT NULL,
  `QUEUE` varchar(60) DEFAULT NULL,
  `LOCALQUEUE` varchar(20) DEFAULT NULL,
  `SYSTEM` varchar(60) NOT NULL,
  `SYSCONFIG` varchar(20) DEFAULT NULL,
  `ENVIRON` varchar(250) DEFAULT NULL,
  `GATEKEEPER` varchar(40) DEFAULT NULL,
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
  `LASTMOD` datetime NOT NULL,
  `ERRINFO` varchar(80) DEFAULT NULL,
  `NQUEUE` int(11) NOT NULL,
  `COMMENT_` varchar(500) DEFAULT NULL,
  `APPDIR` varchar(500) DEFAULT NULL,
  `DATADIR` varchar(80) DEFAULT NULL,
  `TMPDIR` varchar(80) DEFAULT NULL,
  `WNTMPDIR` varchar(80) DEFAULT NULL,
  `DQ2URL` varchar(80) DEFAULT NULL,
  `SPECIAL_PAR` varchar(80) DEFAULT NULL,
  `PYTHON_PATH` varchar(80) DEFAULT NULL,
  `NODES` int(11) NOT NULL,
  `STATUS` varchar(10) DEFAULT NULL,
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
  `QUEUEHOURS` int(11) NOT NULL,
  `ENVSETUPIN` varchar(200) DEFAULT NULL,
  `COPYTOOLIN` varchar(180) DEFAULT NULL,
  `COPYSETUPIN` varchar(200) DEFAULT NULL,
  `SEPRODPATH` varchar(400) DEFAULT NULL,
  `LFCPRODPATH` varchar(80) DEFAULT NULL,
  `COPYPREFIXIN` varchar(360) DEFAULT NULL,
  `RECOVERDIR` varchar(80) DEFAULT NULL,
  `MEMORY` int(11) NOT NULL,
  `MAXTIME` int(11) NOT NULL,
  `SPACE` int(11) NOT NULL,
  `TSPACE` datetime NOT NULL,
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
  `CORECOUNT` smallint(6) DEFAULT NULL,
  `COUNTRYGROUP` varchar(64) DEFAULT NULL,
  `AVAILABLECPU` varchar(64) DEFAULT NULL,
  `AVAILABLESTORAGE` varchar(64) DEFAULT NULL,
  `PLEDGEDCPU` varchar(64) DEFAULT NULL,
  `PLEDGEDSTORAGE` varchar(64) DEFAULT NULL,
  `STATUSOVERRIDE` varchar(256) DEFAULT NULL,
  `ALLOWDIRECTACCESS` varchar(10) DEFAULT NULL,
  `GOCNAME` varchar(64) DEFAULT NULL,
  `TIER` varchar(15) DEFAULT NULL,
  `MULTICLOUD` varchar(64) DEFAULT NULL,
  `LFCREGISTER` varchar(10) DEFAULT NULL,
  `STAGEINRETRY` int(11) DEFAULT NULL,
  `STAGEOUTRETRY` int(11) DEFAULT NULL,
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
  `WANSOURCELIMIT` mediumint(5) DEFAULT NULL,
  `WANSINKLIMIT` mediumint(5) DEFAULT NULL,
  `AUTO_MCU` tinyint(1) DEFAULT NULL,
  `OBJECTSTORE` varchar(512) DEFAULT NULL,
  `ALLOWHTTP` varchar(64) DEFAULT NULL,
  `HTTPREDIRECTOR` varchar(256) DEFAULT NULL,
  `MULTICLOUD_APPEND` varchar(64) DEFAULT NULL,
  PRIMARY KEY (`NICKNAME`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `schedconfig_old`
--

DROP TABLE IF EXISTS `schedconfig_old`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `schedconfig_old` (
  `NAME` varchar(60) NOT NULL,
  `NICKNAME` varchar(60) NOT NULL,
  `QUEUE` varchar(60) DEFAULT NULL,
  `LOCALQUEUE` varchar(20) DEFAULT NULL,
  `SYSTEM` varchar(60) NOT NULL,
  `SYSCONFIG` varchar(20) DEFAULT NULL,
  `ENVIRON` varchar(250) DEFAULT NULL,
  `GATEKEEPER` varchar(40) DEFAULT NULL,
  `JOBMANAGER` varchar(80) DEFAULT NULL,
  `SE` varchar(250) DEFAULT NULL,
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
  `LASTMOD` datetime NOT NULL,
  `ERRINFO` varchar(80) DEFAULT NULL,
  `NQUEUE` int(11) NOT NULL,
  `COMMENT_` varchar(500) DEFAULT NULL,
  `APPDIR` varchar(100) DEFAULT NULL,
  `DATADIR` varchar(80) DEFAULT NULL,
  `TMPDIR` varchar(80) DEFAULT NULL,
  `WNTMPDIR` varchar(80) DEFAULT NULL,
  `DQ2URL` varchar(80) DEFAULT NULL,
  `SPECIAL_PAR` varchar(80) DEFAULT NULL,
  `PYTHON_PATH` varchar(80) DEFAULT NULL,
  `NODES` int(11) NOT NULL,
  `STATUS` varchar(10) DEFAULT NULL,
  `COPYTOOL` varchar(80) DEFAULT NULL,
  `COPYSETUP` varchar(200) DEFAULT NULL,
  `RELEASES` varchar(500) DEFAULT NULL,
  `SEPATH` varchar(150) DEFAULT NULL,
  `ENVSETUP` varchar(200) DEFAULT NULL,
  `COPYPREFIX` varchar(160) DEFAULT NULL,
  `LFCPATH` varchar(80) DEFAULT NULL,
  `SEOPT` varchar(300) DEFAULT NULL,
  `SEIN` varchar(60) DEFAULT NULL,
  `SEINOPT` varchar(60) DEFAULT NULL,
  `LFCHOST` varchar(80) DEFAULT NULL,
  `CLOUD` varchar(60) DEFAULT NULL,
  `SITEID` varchar(60) DEFAULT NULL,
  `PROXY` varchar(80) DEFAULT NULL,
  `RETRY` varchar(10) DEFAULT NULL,
  `QUEUEHOURS` int(11) NOT NULL,
  `ENVSETUPIN` varchar(200) DEFAULT NULL,
  `COPYTOOLIN` varchar(180) DEFAULT NULL,
  `COPYSETUPIN` varchar(200) DEFAULT NULL,
  `SEPRODPATH` varchar(200) DEFAULT NULL,
  `LFCPRODPATH` varchar(80) DEFAULT NULL,
  `COPYPREFIXIN` varchar(80) DEFAULT NULL,
  `RECOVERDIR` varchar(80) DEFAULT NULL,
  `MEMORY` int(11) NOT NULL,
  `MAXTIME` int(11) NOT NULL,
  `SPACE` int(11) NOT NULL,
  `TSPACE` datetime NOT NULL,
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
  `CORECOUNT` smallint(6) DEFAULT NULL,
  `COUNTRYGROUP` varchar(64) DEFAULT NULL,
  `AVAILABLECPU` varchar(64) DEFAULT NULL,
  `AVAILABLESTORAGE` varchar(64) DEFAULT NULL,
  `PLEDGEDCPU` varchar(64) DEFAULT NULL,
  `PLEDGEDSTORAGE` varchar(64) DEFAULT NULL,
  `LAST_STATUS` varchar(256) DEFAULT NULL,
  PRIMARY KEY (`NICKNAME`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `schedinstance`
--

DROP TABLE IF EXISTS `schedinstance`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `schedinstance` (
  `NAME` varchar(60) NOT NULL,
  `NICKNAME` varchar(60) NOT NULL,
  `PANDASITE` varchar(60) NOT NULL,
  `NQUEUE` int(11) NOT NULL,
  `NQUEUED` int(11) NOT NULL,
  `NRUNNING` int(11) NOT NULL,
  `NFINISHED` int(11) NOT NULL,
  `NFAILED` int(11) NOT NULL,
  `NABORTED` int(11) NOT NULL,
  `NJOBS` int(11) NOT NULL,
  `TVALID` datetime NOT NULL,
  `LASTMOD` datetime NOT NULL,
  `ERRINFO` varchar(150) DEFAULT NULL,
  `NDONE` int(11) NOT NULL,
  `TOTRUNT` int(11) NOT NULL,
  `COMMENT_` varchar(500) DEFAULT NULL,
  PRIMARY KEY (`NICKNAME`,`PANDASITE`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `servicelist`
--

DROP TABLE IF EXISTS `servicelist`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `servicelist` (
  `ID` int(11) NOT NULL,
  `NAME` varchar(60) NOT NULL,
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
  `TSTART` datetime NOT NULL,
  `TSTOP` datetime NOT NULL,
  `TCHECK` datetime NOT NULL,
  `CYCLESEC` int(11) DEFAULT NULL,
  `STATUS` varchar(20) NOT NULL,
  `LASTMOD` datetime NOT NULL,
  `CONFIG` varchar(200) DEFAULT NULL,
  `MESSAGE` varchar(4000) DEFAULT NULL,
  `RESTARTCMD` varchar(4000) DEFAULT NULL,
  `DOACTION` varchar(4000) DEFAULT NULL,
  PRIMARY KEY (`ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `siteaccess`
--

DROP TABLE IF EXISTS `siteaccess`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `siteaccess` (
  `ID` bigint(20) NOT NULL AUTO_INCREMENT,
  `DN` varchar(100) DEFAULT NULL,
  `PANDASITE` varchar(100) DEFAULT NULL,
  `POFFSET` bigint(20) NOT NULL,
  `RIGHTS` varchar(30) DEFAULT NULL,
  `STATUS` varchar(20) DEFAULT NULL,
  `WORKINGGROUPS` varchar(100) DEFAULT NULL,
  `CREATED` datetime DEFAULT NULL,
  PRIMARY KEY (`ID`)
) ENGINE=InnoDB AUTO_INCREMENT=1142 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `sitedata`
--

DROP TABLE IF EXISTS `sitedata`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `sitedata` (
  `SITE` varchar(30) NOT NULL,
  `FLAG` varchar(20) NOT NULL,
  `HOURS` int(11) NOT NULL,
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
  `NSTART` int(11) NOT NULL,
  `FINISHED` int(11) NOT NULL,
  `FAILED` int(11) NOT NULL,
  `DEFINED` int(11) NOT NULL,
  `ASSIGNED` int(11) NOT NULL,
  `WAITING` int(11) NOT NULL,
  `ACTIVATED` int(11) NOT NULL,
  `HOLDING` int(11) NOT NULL,
  `RUNNING` int(11) NOT NULL,
  `TRANSFERRING` int(11) NOT NULL,
  `GETJOB` int(11) NOT NULL,
  `UPDATEJOB` int(11) NOT NULL,
  `LASTMOD` datetime NOT NULL,
  `NCPU` int(11) DEFAULT NULL,
  `NSLOT` int(11) DEFAULT NULL,
  PRIMARY KEY (`SITE`,`FLAG`,`HOURS`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `sitedata_old`
--

DROP TABLE IF EXISTS `sitedata_old`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `sitedata_old` (
  `SITE` varchar(30) NOT NULL,
  `FLAG` varchar(20) NOT NULL,
  `HOURS` int(11) NOT NULL,
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
  `NSTART` int(11) NOT NULL,
  `FINISHED` int(11) NOT NULL,
  `FAILED` int(11) NOT NULL,
  `DEFINED` int(11) NOT NULL,
  `ASSIGNED` int(11) NOT NULL,
  `WAITING` int(11) NOT NULL,
  `ACTIVATED` int(11) NOT NULL,
  `HOLDING` int(11) NOT NULL,
  `RUNNING` int(11) NOT NULL,
  `TRANSFERRING` int(11) NOT NULL,
  `GETJOB` int(11) NOT NULL,
  `UPDATEJOB` int(11) NOT NULL,
  `LASTMOD` datetime NOT NULL,
  `NCPU` int(11) DEFAULT NULL,
  `NSLOT` int(11) DEFAULT NULL,
  PRIMARY KEY (`SITE`,`FLAG`,`HOURS`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

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
  PRIMARY KEY (`NAME`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `sitehistory`
--

DROP TABLE IF EXISTS `sitehistory`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `sitehistory` (
  `SITE` varchar(30) NOT NULL,
  `FLAG` varchar(20) NOT NULL,
  `TIME` datetime NOT NULL,
  `HOURS` int(11) NOT NULL,
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
  `NSTART` int(11) NOT NULL,
  `FINISHED` int(11) NOT NULL,
  `FAILED` int(11) NOT NULL,
  `DEFINED` int(11) NOT NULL,
  `ASSIGNED` int(11) NOT NULL,
  `WAITING` int(11) NOT NULL,
  `ACTIVATED` int(11) NOT NULL,
  `RUNNING` int(11) NOT NULL,
  `GETJOB` int(11) NOT NULL,
  `UPDATEJOB` int(11) NOT NULL,
  `SUBTOT` int(11) NOT NULL,
  `SUBDEF` int(11) NOT NULL,
  `SUBDONE` int(11) NOT NULL,
  `FILEMODS` int(11) NOT NULL,
  `NCPU` int(11) DEFAULT NULL,
  `NSLOT` int(11) DEFAULT NULL,
  PRIMARY KEY (`SITE`,`FLAG`,`TIME`,`HOURS`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

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
  `SONARSMLVAL` decimal(6,2) DEFAULT NULL,
  `SONARSMLDEV` decimal(6,2) DEFAULT NULL,
  `SONARMEDVAL` decimal(6,2) DEFAULT NULL,
  `SONARMEDDEV` decimal(6,2) DEFAULT NULL,
  `SONARLRGVAL` decimal(6,2) DEFAULT NULL,
  `SONARLRGDEV` decimal(6,2) DEFAULT NULL,
  `PERFSONARAVGVAL` decimal(6,2) DEFAULT NULL,
  `XRDCPVAL` decimal(6,2) DEFAULT NULL,
  PRIMARY KEY (`SOURCE`,`DESTINATION`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

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
  `GOCCONFIG` smallint(6) DEFAULT NULL,
  `PRODSYS` varchar(20) DEFAULT NULL,
  `DQ2SVC` varchar(20) DEFAULT NULL,
  `USAGE` varchar(40) DEFAULT NULL,
  `UPDTIME` int(11) DEFAULT NULL,
  `NDATASETS` int(11) DEFAULT NULL,
  `NFILES` int(11) DEFAULT NULL,
  `TIMESTAMP` int(11) DEFAULT NULL,
  PRIMARY KEY (`NAME`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `sitestats`
--

DROP TABLE IF EXISTS `sitestats`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `sitestats` (
  `CLOUD` varchar(10) NOT NULL DEFAULT '',
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
  `TSETUP` int(11) DEFAULT NULL,
  PRIMARY KEY (`CLOUD`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

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
) ENGINE=InnoDB AUTO_INCREMENT=2800 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

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
  PRIMARY KEY (`HOST`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

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
  PRIMARY KEY (`NAME`,`SYSTEM`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

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
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `t_task`
--

DROP TABLE IF EXISTS `t_task`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `t_task` (
  `TASKID` bigint(20) NOT NULL,
  `PARENT_TID` bigint(20) DEFAULT NULL,
  `STATUS` varchar(12) DEFAULT NULL,
  `TOTAL_DONE_JOBS` int(11) DEFAULT NULL,
  `SUBMIT_TIME` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  `START_TIME` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  `TIMESTAMP` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  `JEDI_TASK_PARAMETERS` text,
  `VO` varchar(16) DEFAULT NULL,
  `PRODSOURCELABEL` varchar(20) DEFAULT NULL,
  `TASKNAME` varchar(256) DEFAULT NULL,
  `USERNAME` varchar(128) DEFAULT NULL,
  `PRIORITY` smallint(6) DEFAULT NULL,
  `CURRENT_PRIORITY` smallint(6) DEFAULT NULL,
  `TOTAL_REQ_JOBS` int(11) DEFAULT NULL,
  `CHAIN_TID` bigint(20) DEFAULT NULL,
  `TOTAL_EVENTS` int(11) DEFAULT NULL,
  PRIMARY KEY (`TASKID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `t_tier2_groups`
--

DROP TABLE IF EXISTS `t_tier2_groups`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `t_tier2_groups` (
  `NAME` varchar(12) NOT NULL DEFAULT '',
  `GID` bigint(20) DEFAULT NULL,
  `NTUP_SHARE` bigint(20) DEFAULT NULL,
  `TIMESTMAP` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`NAME`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

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
  `DATA_VERIF_PASSED` varchar(3) DEFAULT NULL,
  `DATA_VERIFIED_ON` datetime DEFAULT NULL,
  PRIMARY KEY (`TABLE_NAME`,`PARTITION_NAME`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `taginfo`
--

DROP TABLE IF EXISTS `taginfo`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `taginfo` (
  `TAG` varchar(30) NOT NULL,
  `DESCRIPTION` varchar(100) NOT NULL,
  `NQUEUES` int(11) NOT NULL,
  `QUEUES` varchar(4000) DEFAULT NULL,
  PRIMARY KEY (`TAG`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

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
  `CREATED` datetime NOT NULL,
  PRIMARY KEY (`ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `transfercosts`
--

DROP TABLE IF EXISTS `transfercosts`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `transfercosts` (
  `SOURCESITE` varchar(256) NOT NULL,
  `DESTSITE` varchar(256) NOT NULL,
  `TYPE` varchar(256) NOT NULL,
  `STATUS` varchar(64) DEFAULT NULL,
  `LAST_UPDATE` datetime DEFAULT NULL,
  `COST` bigint(20) NOT NULL,
  `MAX_COST` bigint(20) DEFAULT NULL,
  `MIN_COST` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`SOURCESITE`,`DESTSITE`,`TYPE`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `transfercosts_history`
--

DROP TABLE IF EXISTS `transfercosts_history`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `transfercosts_history` (
  `SOURCESITE` varchar(256) NOT NULL,
  `DESTSITE` varchar(256) NOT NULL,
  `TYPE` varchar(256) DEFAULT NULL,
  `STATUS` varchar(64) DEFAULT NULL,
  `LAST_UPDATE` datetime DEFAULT NULL,
  `COST` bigint(20) NOT NULL,
  `MAX_COST` bigint(20) DEFAULT NULL,
  `MIN_COST` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`SOURCESITE`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `triggers_debug`
--

DROP TABLE IF EXISTS `triggers_debug`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `triggers_debug` (
  `WHEN` datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
  `WHAT` varchar(100) DEFAULT NULL,
  `VALUE` varchar(200) DEFAULT NULL,
  PRIMARY KEY (`WHEN`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

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
  `TINSERT` datetime NOT NULL,
  `SITE` varchar(30) NOT NULL,
  `NWN` int(11) DEFAULT NULL,
  PRIMARY KEY (`ENTRY`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

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
  PRIMARY KEY (`FILENAME`,`HOSTNAME`,`CREATIONTIME`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

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
  `LASTMOD` datetime NOT NULL,
  `FIRSTJOB` datetime NOT NULL,
  `LATESTJOB` datetime NOT NULL,
  `PAGECACHE` text,
  `CACHETIME` datetime NOT NULL,
  `NCURRENT` int(11) NOT NULL,
  `JOBID` int(11) NOT NULL,
  `STATUS` varchar(20) DEFAULT NULL,
  `VO` varchar(20) DEFAULT NULL,
  `nickname` varchar(20) DEFAULT NULL,
  PRIMARY KEY (`ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `users_old`
--

DROP TABLE IF EXISTS `users_old`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `users_old` (
  `ID` int(11) NOT NULL,
  `NAME` varchar(60) NOT NULL,
  `DN` varchar(100) DEFAULT NULL,
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
  `LASTMOD` datetime NOT NULL,
  `FIRSTJOB` datetime NOT NULL,
  `LATESTJOB` datetime NOT NULL,
  `PAGECACHE` text,
  `CACHETIME` datetime NOT NULL,
  `NCURRENT` int(11) NOT NULL,
  `JOBID` int(11) NOT NULL,
  `STATUS` varchar(20) DEFAULT NULL,
  `VO` varchar(20) DEFAULT NULL,
  PRIMARY KEY (`ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `userstats`
--

DROP TABLE IF EXISTS `userstats`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `userstats` (
  `NAME` varchar(60) NOT NULL,
  `LABEL` varchar(20) DEFAULT NULL,
  `YR` int(11) NOT NULL,
  `MO` int(11) NOT NULL,
  `JOBS` bigint(20) DEFAULT NULL,
  `IDLO` bigint(20) DEFAULT NULL,
  `IDHI` bigint(20) DEFAULT NULL,
  `INFO` varchar(100) DEFAULT NULL,
  PRIMARY KEY (`NAME`,`YR`,`MO`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

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
  `STATE` varchar(30) DEFAULT NULL,
  PRIMARY KEY (`DATASETNAME`,`SITE`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

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
  PRIMARY KEY (`SITE_NAME`,`QUEUE`,`VO_NAME`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `vorspassfail`
--

DROP TABLE IF EXISTS `vorspassfail`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `vorspassfail` (
  `SITE_NAME` varchar(32) NOT NULL,
  `PASSFAIL` varchar(4) NOT NULL,
  `LAST_CHECKED` datetime DEFAULT NULL,
  PRIMARY KEY (`SITE_NAME`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `wndata`
--

DROP TABLE IF EXISTS `wndata`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `wndata` (
  `SITE` varchar(30) NOT NULL,
  `WN` varchar(50) NOT NULL,
  `FLAG` varchar(20) NOT NULL,
  `HOURS` int(11) NOT NULL,
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
  `NSTART` int(11) NOT NULL,
  `FINISHED` int(11) NOT NULL,
  `FAILED` int(11) NOT NULL,
  `HOLDING` int(11) NOT NULL,
  `RUNNING` int(11) NOT NULL,
  `TRANSFERRING` int(11) NOT NULL,
  `GETJOB` int(11) NOT NULL,
  `UPDATEJOB` int(11) NOT NULL,
  `LASTMOD` datetime NOT NULL,
  `NCPU` int(11) DEFAULT NULL,
  `NCPUCURRENT` int(11) DEFAULT NULL,
  `NSLOT` int(11) DEFAULT NULL,
  `NSLOTCURRENT` int(11) DEFAULT NULL,
  PRIMARY KEY (`SITE`,`WN`,`FLAG`,`HOURS`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2015-07-24  1:22:03
