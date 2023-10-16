import optparse

import pandaserver.userinterface.Client as Client

optP = optparse.OptionParser(conflict_handler="resolve")
options, args = optP.parse_args()

jediTaskID = args[0]

s, o = Client.finishTask(jediTaskID)
print(o)
