#!/usr/bin/python

import os
import sys

os.chdir('..')

option = ''
if len(sys.argv) > 1 and sys.argv[1] == '-n':
    option = ' -n'

packages = ['liveconfigparser','pandalogger','taskbuffer',
            'brokerage','jobdispatcher','userinterface',
            'dataservice','test','server'] #,'config']

for pack in packages:
    com = 'cvs%s update %s' % (option,pack)
    print com
    os.system(com)
