from pandaserver.dataservice import RecoverLostFilesCore

s, o = RecoverLostFilesCore.main(exec_options={"userName": "CLI", "isProductionManager": True})
if s:
    print("OK")
else:
    print("ERROR:", o)
