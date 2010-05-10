import commands
from dataservice.DDM import ddm

#print ddm.DQ2ProductionClient.generateUUID()
#print ddm.DQ2.getFilesFromCatalog('aho.xml')
#print ddm.DQ2ProductionClient.dq2_makeblocks('input.data')

ids=['pandatest.000003.dd.input._00047.junk','09801b0a-9fd0-4237-8caf-a37932c26e39',
     'pandatest.000003.dd.input._00050.junk','6dd3d367-4aa3-4e1a-9ac3-9ad14b7311f4',
     'pandatest.000003.dd.input._00037.junk','817c2c92-467b-4a1b-9482-f2ec8468cf2e',
     'pandatest.000003.dd.input._00021.junk','7720527f-817e-40c7-9e29-ce237f59edfa',
     'pandatest.000003.dd.input._00023.junk','5f1f9982-85a3-4d1a-9ee9-f1de22c02544',
     'pandatest.000003.dd.input._00042.junk','610cc91a-c731-4bce-ac7a-ff5133e7d18b',
     'pandatest.000003.dd.input._00027.junk','bd987478-3c59-4551-b12b-2853bac25613',
     'pandatest.000003.dd.input._00032.junk','9d0424f3-7552-4282-92f2-dfe74e9a6c12',
     'pandatest.000003.dd.input._00009.junk','dce33d4a-4569-49ee-95c5-b619b161c777',
     'pandatest.000003.dd.input._00036.junk','2fc9836b-82d6-41b0-b966-a5c37662172d',
     'pandatest.000003.dd.input._00031.junk','65b957e0-5ecc-44bb-a1f9-cccb61ca2d16',
     'pandatest.000003.dd.input._00025.junk','be29fe82-17e2-4122-b4c8-f49a0b76c81f',
     'pandatest.000003.dd.input._00029.junk','afa4322f-409b-4327-9169-229d8d48ad5a',
     'pandatest.000003.dd.input._00013.junk','cf236d3b-45fd-4b58-bdfb-59abc983c886',
     'pandatest.000003.dd.input._00020.junk','b02f98da-0138-4b58-89ba-a88f37214a89',
     'pandatest.000003.dd.input._00001.junk','12ab5bb9-944e-4e75-bb90-b64c462d4cd8',
     'pandatest.000003.dd.input._00001.junk','12ab5bb9-944e-4e75-bb90-b64c462d4cd8',
     'pandatest.000003.dd.input._00006.junk','c0a422ad-e9f1-44bb-9539-cfef7e739da2',
     'pandatest.000003.dd.input._00034.junk','da670db3-3638-4f06-b650-a9315eb2bd63',
     'pandatest.000003.dd.input._00046.junk','2fcef270-2e41-472d-83c0-53749b401b74',
     'pandatest.000003.dd.input._00012.junk','5e212fa1-201f-494d-a2b2-420b229b08fc',
     'pandatest.000003.dd.input._00044.junk','87c8ebcc-a637-4204-b77b-8219e68b98d7',
     'pandatest.000003.dd.input._00030.junk','87ad811f-7d39-43d9-8a13-e117079bb208',
     'pandatest.000003.dd.input._00022.junk','6b902506-1ee1-46b1-a105-1521a8c0dbca',
     'pandatest.000003.dd.input._00017.junk','2bbed213-943c-41be-b9d7-7d86a309b0b2',
     'pandatest.000003.dd.input._00049.junk','8366e269-f9ae-4b9c-bd98-df4027c992c7',
     'pandatest.000003.dd.input._00015.junk','f3c5f37c-b4c2-4933-9633-467ba3a7c364',
     'pandatest.000003.dd.input._00004.junk','35d66be2-9d21-44a3-96f7-903a7abf4a87',
     'pandatest.000003.dd.input._00010.junk','2279ea3e-ebbb-4b19-9a69-9868f0cce694',
     'pandatest.000003.dd.input._00040.junk','a847dbbb-4f98-4b5b-b353-e29e3e3b3fd5',
     'pandatest.000003.dd.input._00007.junk','abfef002-62ca-4d84-9813-6329764e38bd',
     'pandatest.000003.dd.input._00048.junk','52854023-67d8-4a0f-99ac-bb1f0bd1dc98',
     'pandatest.000003.dd.input._00016.junk','bddf7441-6ac9-4087-bafe-32e47448cdc1',
     'pandatest.000003.dd.input._00041.junk','c76999ba-4cdf-49e9-bfa5-ff3525fbf1ab',
     'pandatest.000003.dd.input._00003.junk','4865119e-367f-4dd8-bdff-505bd878dfde',
     'pandatest.000003.dd.input._00019.junk','b9fce1fd-8d4c-4fc4-932f-12b13263ca0c',
     'pandatest.000003.dd.input._00011.junk','f93a4e08-fd4f-45fc-b324-91ff59555b1c',
     'pandatest.000003.dd.input._00018.junk','e4894561-9589-40d8-871b-b57d70564384',
     'pandatest.000003.dd.input._00002.junk','58934980-5ab3-4a66-b3da-55f86d4b54bd',
     'pandatest.000003.dd.input._00005.junk','5993fe60-bc8c-4fd8-aac1-dfd55700c9c3',
     'pandatest.000003.dd.input._00028.junk','6c19e1fc-ee8c-4bae-bd4c-c9e5c73aca27',
     'pandatest.000003.dd.input._00033.junk','98f79ba1-1793-4253-aac7-bdf90a51d1ee',
     'pandatest.000003.dd.input._00039.junk','33660dd5-7cef-422a-a7fc-6c24cb10deb1',
     'pandatest.000003.dd.input._00014.junk','5c0e9ed8-05a6-41c4-8c07-39b2be33ebc1',
     'pandatest.000003.dd.input._00008.junk','b0c184d1-5f5e-45a6-9cc8-8b0f20a85463',
     'pandatest.000003.dd.input._00038.junk','b9171997-4d2b-4075-b154-579ebe9438fa',
     'pandatest.000003.dd.input._00026.junk','89e5bdf1-15de-44ae-a388-06c1e7d7e2fc',
     'pandatest.000003.dd.input._00024.junk','c77b77a2-e6d1-4360-8751-19d9fb77e1f1',
     'pandatest.000003.dd.input._00043.junk','cc6ac2a1-4616-4551-80a7-d96f79252b64',
     'pandatest.000003.dd.input._00045.junk','ddbed17a-6d65-4e8d-890a-21e1eaa3e9d6',
     'pandatest.000003.dd.input._00035.junk','8ed1875a-eb90-4906-8fc4-0449d300ddfe'
     ]

for i in range(1):
    datasetName='testDQ.%s' % commands.getoutput('/usr/bin/uuidgen')
    print datasetName

    #['pandatest.000003.dd.input._00004.junk','35d66be2-9d21-44a3-96f7-903a7abf4a87'] 
    #'pandatest.000003.dd.input._00028.junk','6c19e1fc-ee8c-4bae-bd4c-c9e5c73aca27',
    # 'pandatest.000003.dd.input._00033.junk','98f79ba1-1793-4253-aac7-bdf90a51d1ee']
    print (['registerNewDataset','-c',datasetName]+ids[i*2:i*2+2])
    ddm.DQ2.main(['registerNewDataset','-c',datasetName]+ids[i*2:i*2+2])
    '''
    status,out = ddm.RepositoryClient.main(['queryDatasetByName',datasetName])
    exec "vuids = %s" % out.split('\n')[0]
    if vuids.has_key(datasetName):
    vuid = vuids[datasetName]
    print vuid
    status,out = ddm.RepositoryClient.main(['resolveVUID',vuid])
    status,out = ddm.DQ2.getFilesFromCatalog('baka.xml')
    exec "rets = %s" % out.split('\n')[0]
    print rets[0]
    exec "ids = %s" % out
    print ddm.DQ2.main(['addFilesToDataset',datasetName]+ids)
    status,out = ddm.DQ2.main(['listFilesInDataset',datasetName])
    print out
    '''
    print (['registerDatasetLocations','-c',datasetName,'http://dms02.usatlas.bnl.gov/sites/bnl/lrc'])
    ddm.DQ2.main(['registerDatasetLocations','-c',datasetName,
                        'http://dms02.usatlas.bnl.gov/sites/bnl/lrc'])
    print (['registerDatasetSubscription',datasetName,'http://doe-dhcp241.bu.edu:8000/dq2/'])
    ddm.DQ2.main(['registerDatasetSubscription',datasetName,'http://doe-dhcp241.bu.edu:8000/dq2/'])
#print ddm.DQ2.main(['eraseDataset',datasetName])

#print ddm.DQ2.main(['eraseDataset',datasetName])
#print ddm.DQ2ProductionClient.dq2_create_dataset(datasetName)
#status,out = ddm.DQ2ProductionClient.dq2_assign_destination(datasetName,'BNL_SE')
#print out
#print ddm.DQ2.main(['eraseDataset',datasetName])
#status,out = ddm.DQ2.main(['listFilesInDataset','panda.destDB.11aed982-8079-4db9-964c-37a284b8597a'])
#print out



