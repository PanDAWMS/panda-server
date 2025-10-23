import optparse

import pandaserver.userinterface.Client as Client

optP = optparse.OptionParser(conflict_handler="resolve")
options, args = optP.parse_args()

task_id = int(args[0])

s, o = Client.kill_task(task_id)
print(o)
