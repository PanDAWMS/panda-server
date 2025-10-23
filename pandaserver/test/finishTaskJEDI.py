import optparse

import pandaserver.userinterface.Client as Client

optP = optparse.OptionParser(conflict_handler="resolve")
options, args = optP.parse_args()

task_id = args[0]

s, o = Client.finish_task(task_id)
print(o)
