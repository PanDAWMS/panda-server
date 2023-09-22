from pandaserver.dataservice import RecoverLostFilesCore

s, o = RecoverLostFilesCore.main()
if s:
    print("OK")
else:
    print("ERROR:", o)
