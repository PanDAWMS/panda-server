import commands

for patt in ['dq2.clientapi.cli.cliutil.getDQ2','forkSetupper.py','LFCclient.py']:
    out = commands.getoutput('ps aux | grep python | grep %s' % patt)
    for line in out.split('\n'):
        items = line.split()
        print items[1], items[8]
        if items[8] in ['Sep04','Sep05']:
            commands.getoutput('kill -9 %s' % items[1])
                
