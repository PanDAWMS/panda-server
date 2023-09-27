import optparse

import pandaserver.userinterface.Client as Client

aSrvID = None

optP = optparse.OptionParser(conflict_handler="resolve")
options, args = optP.parse_args()

jediTaskID = args[0]

s, o = Client.killTask(jediTaskID)
print(o)
