import optparse

from pandaserver.userinterface import Client

option_parser = optparse.OptionParser(conflict_handler="resolve")
options, args = option_parser.parse_args()

task_id = int(args[0])

s, o = Client.kill_task(task_id)
print(o)
