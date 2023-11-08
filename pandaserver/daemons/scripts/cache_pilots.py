import shutil
import traceback
from pathlib import Path

from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandaserver.config import panda_config

# logger
_logger = PandaLogger().getLogger("cache_pilot_config")

# list of source files to be copied
source_file_list = [
    "/cvmfs/atlas.cern.ch/repo/sw/PandaPilot/tar/pilot2.tar.gz",
    "/cvmfs/atlas.cern.ch/repo/sw/PandaPilot/tar/pilot3.tar.gz",
]

# default destination directory
default_dest_dir = getattr(panda_config, "pilot_cache_dir", "/var/cache/pandaserver/pilot")


def main(argv=tuple(), tbuf=None, **kwargs):
    _logger.debug("start")
    try:
        # ensure the destination directory
        dest_dir_path = Path(default_dest_dir)
        try:
            dest_dir_path.mkdir(mode=0o755, exist_ok=True)
        except Exception as e:
            _logger.error(f"failed to mkdir {dest_dir_path}: {e}")
        # copy
        for source_file in source_file_list:
            dest_file_path = shutil.copy(source_file, dest_dir_path)
            _logger.debug(f"copied {source_file} to {dest_file_path}")
    except Exception as e:
        err_str = traceback.format_exc()
        _logger.error(f"failed to copy files: {err_str}")
    # done
    _logger.debug("done")


if __name__ == "__main__":
    main()
