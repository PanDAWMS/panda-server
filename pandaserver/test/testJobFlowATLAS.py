"""High level smoke test that simulates the whole flow of a job
    1. create a job
    2. get the job (simulating pilot)
    3. finish the job (simulating pilot)
    4. run the Adder cronjob to register the output (TODO)
    5. emulates a DDM callback (TODO)

You can run the test as
$ nosetests test_job_flow_ATLAS.py
$ python test_job_flow_ATLAS.py
"""
import socket
import sys
import time
import uuid

import nose

try:
    from urlparse import parse_qs
except ImportError:
    from urllib.parse import parse_qs

import hashlib

import pandaserver.userinterface.Client as Client
from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandautils.thread_utils import GenericThread
from pandaserver.config import panda_config
from pandaserver.taskbuffer.FileSpec import FileSpec
from pandaserver.taskbuffer.JobSpec import JobSpec
from pandaserver.taskbuffer.TaskBuffer import taskBuffer
from pandaserver.test.testutils import sendCommand

_logger = PandaLogger().getLogger("testJobFlowATLAS")


class JobFlowATLAS(object):
    """
    Class to test the different states of a job. It has functions to submit, get,
    finish and get the status a job. Some of the functions include assert statements
    for nostests that check the output is as expected.
    """

    __datasetName = f"panda.destDB.{uuid.uuid1()}"
    __destName = None
    __jobList = []

    __XMLTEMPLATE_BASE = """<?xml version="1.0" encoding="UTF-8" standalone="no" ?>
<!-- ATLAS file meta-data catalog -->
<!DOCTYPE POOLFILECATALOG SYSTEM "InMemory">
<POOLFILECATALOG>
    {info}
</POOLFILECATALOG>
"""

    __XMLTEMPLATE_FILE = """<File ID="{guid}">
    <logical>
        <lfn name="{lfn}"/>
    </logical>
    <metadata att_name="surl" att_value="{pfn}"/>
    <metadata att_name="fsize" att_value="127340"/>
    <metadata att_name="md5sum" att_value="03cea4013bdb9f2e44050449b6ebf079"/>
</File>
"""

    __XMLTEMPLATE_META = """<META type="string" name="size" value="82484969"/>
    <META type="string" name="conditionsTag" value="COMCOND-BLKPA-006-11"/>
    <META type="string" name="beamType" value="collisions"/>
    <META type="string" name="fileType" value="aod"/>
    <META type="string" name="autoConfiguration" value="['everything']"/>
    <META type="string" name="dataset" value=""/>
    <META type="string" name="maxEvents" value="200"/>
    <META type="string" name="AMITag" value="r5475"/>
    <META type="string" name="postInclude" value="['EventOverlayJobTransforms/Rt_override_BLKPA-006-11.py', 'EventOverlayJobTransforms/muAlign_reco.py']"/>
    <META type="string" name="preExec" value="['from CaloRec.CaloCellFlags import jobproperties;jobproperties.CaloCellFlags.doLArHVCorr.set_Value_and_Lock(False);muonRecFlags.writeSDOs=True', 'TriggerFlags.AODEDMSet=AODSLIM;rec.Commissioning.set_Value_and_Lock(True);jobproperties.Beam.numberOfCollisions.set_Value_and_Lock(20.0)']"/>
    <META type="string" name="triggerConfig" value="MCRECO:DBF:TRIGGERDBMC:325,142,266"/>
    <META type="string" name="preInclude" value="['EventOverlayJobTransforms/UseOracle.py', 'EventOverlayJobTransforms/custom.py', 'EventOverlayJobTransforms/recotrfpre.py']"/>
    <META type="string" name="geometryVersion" value="ATLAS-GEO-20-00-01"/>
    <META type="string" name="events"/>
    <META type="string" name="postExec" value="['from IOVDbSvc.CondDB import conddb"/>
    {files}
"""

    __XMLTEMPLATE_FILEMETA = """<File ID="{guid}">
    <logical>
        <lfn name="{lfn}"/>
    </logical>
    <metadata att_name="fileType" att_value="esd"/>
    <metadata att_name="preExec" att_value="['from CaloRec.CaloCellFlags "/>
    <metadata att_name="postExec" att_value="['from IOVDbSvc.CondDB import conddb; "/>
    <metadata att_name="events" att_value="200"/>
    <metadata att_name="size" att_value="644128836"/>
</File>
"""

    def __init__(self, site, cloud, nJobs):
        """Initialize class with parameters"""
        self.__site = site
        self.__cloud = cloud
        self.__nJobs = nJobs
        requester_id = GenericThread().get_full_id(__name__, sys.modules[__name__].__file__)
        taskBuffer.init(
            panda_config.dbhost,
            panda_config.dbpasswd,
            nDBConnection=1,
            requester=requester_id,
        )

    def defineEvgen16Job(self, i):
        """Define an Evgen16 job based on predefined values and randomly generated names"""

        job = JobSpec()
        job.computingSite = self.__site
        job.cloud = self.__cloud

        job.jobDefinitionID = int(time.time()) % 10000
        job.jobName = "%s_%d" % (uuid.uuid1(), i)
        job.AtlasRelease = "Atlas-16.6.2"
        job.homepackage = "AtlasProduction/16.6.2.1"
        job.transformation = "Evgen_trf.py"
        job.destinationDBlock = self.__datasetName
        job.destinationSE = self.__destName
        job.currentPriority = 10000
        job.prodSourceLabel = "test"
        job.cmtConfig = "i686-slc5-gcc43-opt"

        # Output file
        fileO = FileSpec()
        fileO.lfn = f"{job.jobName}.evgen.pool.root"
        fileO.destinationDBlock = job.destinationDBlock
        fileO.destinationSE = job.destinationSE
        fileO.dataset = job.destinationDBlock
        fileO.destinationDBlockToken = "ATLASDATADISK"
        fileO.type = "output"
        job.addFile(fileO)

        # Log file
        fileL = FileSpec()
        fileL.lfn = f"{job.jobName}.job.log.tgz"
        fileL.destinationDBlock = job.destinationDBlock
        fileL.destinationSE = job.destinationSE
        fileL.dataset = job.destinationDBlock
        fileL.destinationDBlockToken = "ATLASDATADISK"
        fileL.type = "log"
        job.addFile(fileL)

        job.jobParameters = f"2760 105048 19901 101 200 MC10.105048.PythiaB_ccmu3mu1X.py {fileO.lfn} NONE NONE NONE MC10JobOpts-latest-test.tar.gz"
        return job

    def generateJobs(self):
        for i in range(self.__nJobs):
            job = self.defineEvgen16Job(i)
            self.__jobList.append({"jobSpec": job, "jobID": None})

        status, output = Client.submitJobs(
            [job["jobSpec"] for job in self.__jobList]
        )  # Return from submitJobs: ret.append((job.PandaID,job.jobDefinitionID,{'jobsetID':job.jobsetID}))

        assert status == 0, f"Submission of jobs finished with status: {status}"

        assert len(self.__jobList) == len(output), "Not all jobs seem to have been submitted properly"

        for job, ids in zip(self.__jobList, output):
            jobID = ids[0]
            job["jobID"] = jobID
            _logger.info(f"Generated job PandaID = {jobID}")

        return

    def getStatus(self, expectedStates):
        idList = [job["jobID"] for job in self.__jobList]
        _logger.info(f"{idList}")
        status, jobInfoList = Client.getJobStatus(idList)
        _logger.info(f"{jobInfoList}")

        assert status == 0, f"Retrieval of job state finished with status: {status}"

        for job in jobInfoList:
            assert job.jobStatus in expectedStates, "Recently defined job was not in states %s (PandaID: %s jobStatus: %s)" % (
                expectedStates,
                job.PandaID,
                job.jobStatus,
            )

        return jobInfoList

    def retrieveJob(self):
        function = "getJob"
        node = {}
        node["siteName"] = self.__site
        node["mem"] = 1000
        node["node"] = socket.getfqdn()

        data = sendCommand(function, node)
        jobD = parse_qs(data)  # jobD indicates it's a job in dictionary format, not a JobSpec object
        return jobD

    def getJobs(self):
        idList = [job["jobID"] for job in self.__jobList]
        counter = 0
        max = len(idList)

        # This needs to be improved. Currently it tries to get the jobs that were submitted. But what to do when other jobs come in between?
        while idList and counter < max:
            jobD = self.retrieveJob()
            pandaID = int(jobD["PandaID"][0])

            assert pandaID in idList, "There were other jobs queued for the site. Please do some cleanup to let the test complete"

            idList.remove(pandaID)
            counter += 1

    def __calculate_path(self, name, scope="panda"):
        hstr = hashlib.md5(f"{scope}:{name}").hexdigest()
        return f"{hstr[0:2]}/{hstr[2:4]}/"

    def __finishJob(self, job, jobID):
        files_xml = ""
        files_meta = ""
        for file in job.Files:
            if file.type in ["output", "log"]:
                file.GUID = uuid.uuid1()
                srm = "srm://srm-eosatlas.cern.ch/eos/atlas/atlasdatadisk/rucio/panda/"
                path = self.__calculate_path(file.lfn)
                pfn = srm + path + file.lfn

                _logger.info(f"pfn: {pfn}")
                files_xml += self.__XMLTEMPLATE_FILE.format(lfn=file.lfn, guid=file.GUID, srm=srm, pfn=pfn)
                files_meta += self.__XMLTEMPLATE_FILEMETA.format(guid=file.GUID, lfn=file.lfn)

        xml = self.__XMLTEMPLATE_BASE.format(info=files_xml)
        _logger.info(f"{xml}")
        meta = self.__XMLTEMPLATE_BASE.format(info=self.__XMLTEMPLATE_META.format(files=files_meta))
        _logger.info(f"{meta}")

        node = {}
        node["jobId"] = jobID
        node["state"] = "finished"
        # node['state']='failed'
        # node['pilotErrorCode']=1200
        # node['pilotErrorCode']=-1202
        node["pilotErrorDiag"] = "aaaaaaaaaaaaaaaaaaaaaaa"
        node["metaData"] = meta
        node["siteName"] = self.__site
        node["attemptNr"] = 0
        node["jobMetrics"] = "aaaaa=2 bbbb=3"
        node["coreCount"] = 10
        node["xml"] = xml

        function = "updateJob"
        data = sendCommand(function, node)
        _logger.info(f"{data}")

    def finishJobs(self):
        for job in self.__jobList:
            jobS = job["jobSpec"]
            jobID = job["jobID"]
            self.__finishJob(jobS, jobID)

    def sendDDMCallbacks(self):
        # Output dataset
        # TODO: This module should be decoupled from taskBuffer - there should be a web call
        dataset = taskBuffer.queryDatasetWithMap({"name": self.__datasetName})

        node = {}
        node["vuid"] = dataset.vuid
        node["site"] = self.__site

        function = "datasetCompleted"
        data = sendCommand(function, node)

        assert data == "True", f"DDM Callback did not return as expected for OUTPUT dataset. data = {data}"


def testFlow():
    """
    Main function to drive the PanDA job across the different states using the methods
    in the JobFlowATLAS class.
    """
    site = "CERN-PROD"
    cloud = "CERN"
    nJobs = 1

    # Step 0: Create the testing class
    test = JobFlowATLAS(site, cloud, nJobs)
    assert test is not None, "JobFlowATLAS not created correctly"

    # Step 1: Create and submit test jobs
    test.generateJobs()

    # Step 2: Check the state of the jobs. They should all be in state 'activated'
    # TODO: Improve and wait for the jobs in defined state
    time.sleep(10)  # It takes usually 7 seconds on the testbed
    jobInfoList = test.getStatus(["defined", "activated"])

    # Step 3: Get the job (PanDA server believes the pilot got the job)
    time.sleep(1)
    test.getJobs()

    # Step 4: Check the state of the jobs. They should all be in state 'sent'
    time.sleep(1)
    test.getStatus(["sent"])

    # Step 5: Finish the jobs
    time.sleep(1)
    test.finishJobs()

    # Step 6: Check the state of the jobs. They should all be in state 'holding'
    time.sleep(1)
    test.getStatus(["holding"])

    # Step 7: Run the adder to register the output in DDM
    from subprocess import call

    status = call(["python", "add.py"])
    assert status == 0, "Call to Adder failed"

    # Step 8: Simulate a callback from DDM
    time.sleep(120)
    test.sendDDMCallbacks()

    # Step 9: Run the adder to register the output in DDM
    time.sleep(120)
    status = call(["python", "add.py"])
    assert status == 0, "Call to Adder failed"

    time.sleep(1)
    test.getStatus(["finished"])


if __name__ == "__main__":
    nose.runmodule()
