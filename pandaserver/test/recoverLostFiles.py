from pandaserver.dataservice import RecoverLostFilesCore

s, o = RecoverLostFilesCore.main()
if s:
    print ('OK:', o)
else:
    print ('ERROR:', o)
