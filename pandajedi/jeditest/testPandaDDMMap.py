from pandajedi.jedicore.JediTaskBufferInterface import JediTaskBufferInterface

# Set up the task buffer interface
tb_if = JediTaskBufferInterface()
tb_if.setupInterface()

# Get the site mapper
site_mapper = tb_if.get_site_mapper()

# Define a random site of panda sites
panda_sites = [
    "ANALY_DESY-HH",
    "ANALY_DESY-HH_TEST",
    "WT2_Install",
    "BNL_Test_2_CE_1",
    "ANALY_IN2P3-CC-T2_RD",
    "UKI-SOUTHGRID-OX-HEP_SL6",
    "RU-Protvino-IHEP",
    "ANALY_NICS_Kraken",
    "ANALY_TRIUMF_HIMEM",
    "TESTGLEXEC",
    "ANALY_GRIF-LPNHE",
    "UKI-SCOTGRID-GLASGOW_MCORE",
    "BNL_PROD",
    "RAL-LCG2_SL6",
]

# Test the getSiteInputStorageEndpointMap function
# print tb_if.getSiteInputStorageEndpointMap(panda_sites, site_mapper)

# for panda_site in panda_sites:
#     print panda_site
#     tmp_site_spec = site_mapper.getSite('ANALY_DESY-HH')
#     print '------------------- ddm -------------------'
#     print 'ddm_input: {0}, ddm_output: {1}'.format(tmp_site_spec.ddm_input, tmp_site_spec.ddm_output)
#     print '------------------- setokens values -------------------'
#     print 'setokens_input: {0}, setokens_output: {1}'.format(tmp_site_spec.setokens_input.values(),
#                                                              tmp_site_spec.setokens_output.values())
#     print '------------------- setokens -------------------'
#     print 'setokens_input: {0}, setokens_output: {1}'.format(tmp_site_spec.setokens_input,
#                                                              tmp_site_spec.setokens_output)

from pandaserver.dataservice import DataServiceUtils  # noqa: E402

sites = sorted(site_mapper.getCloud("WORLD")["sites"])
for tmp_site_name in sites:
    print(f"tmp_site_name: {tmp_site_name}")

    tmp_site_spec = site_mapper.getSite(tmp_site_name)

    # print 'tmp_site_spec.ddm_input: {0}'.format(tmp_site_spec.ddm_input)
    # print 'tmp_site_spec.setokens_input: {0}'.format(tmp_site_spec.setokens_input.values())
    # print 'combination: {0}'.format([tmp_site_spec.ddm_input] + tmp_site_spec.setokens_input.values())

    for tmp_ddm_endpoint in [tmp_site_spec.ddm_input] + list(tmp_site_spec.setokens_input.values()):
        try:
            tmp_prefix = DataServiceUtils.getDQ2Prefix(tmp_ddm_endpoint)
            print(f"prefix: {tmp_prefix}")
        except TypeError:
            print("excepted!")

    print("-------------------")
