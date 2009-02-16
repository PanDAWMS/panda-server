'''
notifier

'''

import shelve

from config import panda_config
from pandalogger.PandaLogger import PandaLogger

# open DB
pDB = shelve.open(panda_config.emailDB)
            

                    
        
