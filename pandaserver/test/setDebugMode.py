import optparse
import sys

from pandaserver.userinterface import Client

option_parser = optparse.OptionParser(conflict_handler="resolve", usage="%prog [options] <PandaID>")
option_parser.add_option(
    "--on",
    action="store_const",
    const=True,
    dest="modeOn",
    default=False,
    help="turn the debug mode on",
)
option_parser.add_option(
    "--off",
    action="store_const",
    const=True,
    dest="modeOff",
    default=False,
    help="turn the debug mode off",
)
options, args = option_parser.parse_args()


if (options.modeOn and options.modeOff) or (not options.modeOn and not options.modeOff):
    print("ERROR: please set --on or --off")
    sys.exit(1)

job_id = int(args[0])

if options.modeOn:
    status, output = Client.set_debug_mode(job_id, True)
else:
    status, output = Client.set_debug_mode(job_id, False)

print(f"status: {status}, output: {output}")
sys.exit(0)
