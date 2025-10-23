import optparse
import sys

import pandaserver.userinterface.Client as Client

optP = optparse.OptionParser(conflict_handler="resolve", usage="%prog [options] <PandaID>")
optP.add_option(
    "--on",
    action="store_const",
    const=True,
    dest="modeOn",
    default=False,
    help="turn the debug mode on",
)
optP.add_option(
    "--off",
    action="store_const",
    const=True,
    dest="modeOff",
    default=False,
    help="turn the debug mode off",
)
options, args = optP.parse_args()


if (options.modeOn and options.modeOff) or (not options.modeOn and not options.modeOff):
    print("ERROR: please set --on or --off")
    sys.exit(1)

if options.modeOn:
    s, o = Client.set_debug_mode(args[0], True)
else:
    s, o = Client.set_debug_mode(args[0], False)

print(f"status: {s}, output: {o}")
sys.exit(0)
