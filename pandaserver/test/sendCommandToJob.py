import argparse

from pandaserver.userinterface import Client

# parse option
parser = argparse.ArgumentParser()
parser.add_argument(
    "--panda_id",
    action="store",
    dest="panda_id",
    type=int,
    required=True,
    help="PandaID of the job",
)
parser.add_argument(
    "--com_str",
    action="store",
    dest="com",
    required=True,
    help="The command string passed to the pilot. max 250 chars",
)

options = parser.parse_args()

status, output = Client.send_command_to_job(options.panda_id, options.com)
print(f"Status: {status}. Output: {output}")
