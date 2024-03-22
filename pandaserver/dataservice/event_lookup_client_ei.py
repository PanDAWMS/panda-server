"""
EventLookupClientEI is a class for looking up events in the EventIndex.
"""
import os
import subprocess
import tempfile

from typing import List, Tuple


class EventLookupClientEI:
    """
    EventLookupClientEI is a class for looking up events in the EventIndex.
    """
    def do_lookup(self, event_run_list: List[Tuple[int, int]], stream: str = None, tokens: str = None, ami_tag: str = None) -> Tuple[List[str], str, str, str]:
        """
        Performs a lookup in the EventIndex for the given parameters.

        Parameters:
            event_run_list (List[Tuple[int, int]]): The list of run events.
            stream (str): The name of the stream.
            tokens (str): The tokens.
            ami_tag (str): The AMI tag.

        Returns:
            Tuple[List[str], str, str, str]: A tuple containing the list of GUIDs, the command, the output, and the error.
        """
        command = os.path.join(
            os.getenv(
                "EIDIR",
                "/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase/x86_64/EIClient/current",
            ),
            "bin",
            "event-lookup",
        )
        with tempfile.NamedTemporaryFile(mode="w+t") as tmp_event_file:
            command += f" -F {tmp_event_file.name} "
            for run_event in event_run_list:
                # creating a preformatted string. The :08d and :09d parts are formatting these integers to be eight and nine digits long
                tmp_string = f"{int(run_event[0]):08d} {int(run_event[1]):09d}\n"
                tmp_event_file.write(tmp_string)
            tmp_event_file.flush()
            if stream not in [None, ""]:
                command += f"-s {stream} "
            if ami_tag not in [None, ""]:
                command += f"-a {ami_tag} "
            command += "-c plain "
            with subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True,
                                  universal_newlines=True) as execute_process:
                tmp_output, tmp_error = execute_process.communicate()
            guids = {}
            if tokens == "":
                tokens = None
            try:
                # The output is expected to be a string with multiple lines, each line containing information about a single event.
                # Each line is split into items, the first two items are interpreted as integers representing a run number and an event number.
                # These two numbers are combined into a tuple, run_event, which is used as a key in the guids dictionary.
                # The fourth item in the line is appended to the string "Stream" to form tmp_token, and the third item is assigned to tmp_guid.
                # An example input (tmp_output) is:
                # 123 456 789abc StreamXYZ
                # An example output (guids dictionary) is:
                # (123, 456): {"789abc"}
                for tmp_line in tmp_output.split("\n"):
                    tmp_items = tmp_line.split()
                    run_event = (int(tmp_items[0]), int(tmp_items[1]))
                    guids.setdefault(run_event, set())
                    # check type
                    tmp_token = "Stream" + tmp_items[3]
                    tmp_guid = tmp_items[2]
                    if not tokens or tokens == tmp_token:
                        guids[run_event].add(tmp_guid)
                if not guids:
                    # add dummy
                    guids[None] = None
            except Exception:
                pass
            return guids, command, tmp_output, tmp_error
