import argparse
from taskbuffer.TaskBuffer import taskBuffer
from config import panda_config
from userinterface import Client

taskBuffer.init(panda_config.dbhost,panda_config.dbpasswd,nDBConnection=1)

# parse option
parser = argparse.ArgumentParser()
parser.add_argument('--ds',action='store',dest='ds',default=None,
                    help='dataset name')
parser.add_argument('--files',action='store',dest='ds',default=None,
                    help='comma-separated list of lost file names')
options = parser.parse_args()

files = options.files.split(',')

s,jediTaskID = taskBuffer.resetFileStatusInJEDI(options.ds,files)
if s:
    print Client.retryTask(jediTaskID)[-1]
    print 'done for jediTaskID={0}'.format(jediTaskID)
else:
    print 'failed'

