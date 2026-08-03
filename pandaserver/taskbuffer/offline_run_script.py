"""
generation of a shell script to rerun a job interactively

The generated script retrieves the input files and then runs the transformations of the job in an
ALRB container. It takes options to use only a subset of the input files and to read them directly
from storage through a PoolFileCatalog.xml instead of downloading them.

This module intentionally depends only on the standard library, so that it can be used and tested
without a server configuration.
"""

import ast
import re
import shlex
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pandaserver.taskbuffer.JobSpec import JobSpec

# python script embedded in the offline running script to generate PoolFileCatalog.xml for direct access.
# it takes an RSE expression, comma-separated protocol schemes, and DIDs of input files as arguments.
# note that this snippet runs with the python3 of the ALRB environment, hence it is deliberately kept
# independent of the conventions of this module
_PFC_GENERATOR = r"""import sys

from rucio.client import Client

rse_expression = sys.argv[1] if sys.argv[1] else None
schemes = [s for s in sys.argv[2].split(",") if s]
if not schemes:
    schemes = None
dids = [tuple(a.split(":", 1)) for a in sys.argv[3:]]

# GUIDs taken from the PanDA DB. None is resolved with rucio
guids = __GUID_MAP__

client = Client()
guid_map = {}
for scope, lfn in dids:
    guid = guids.get(scope + ":" + lfn)
    if guid is None:
        guid = client.get_metadata(scope, lfn)["guid"]
        guid = "-".join([guid[0:8], guid[8:12], guid[12:16], guid[16:20], guid[20:32]])
    guid_map[(scope, lfn)] = guid.upper()

candidates = dict([(key, {}) for key in dids])
for replica in client.list_replicas(
    [{"scope": scope, "name": lfn} for scope, lfn in dids],
    rse_expression=rse_expression,
    schemes=schemes,
):
    key = (str(replica["scope"]), str(replica["name"]))
    if key in candidates:
        candidates[key] = replica.get("pfns") or {}

# exactly one replica per file is required. show the candidates and give up otherwise
bad = [key for key in dids if len(candidates[key]) != 1]
if bad:
    for scope, lfn in bad:
        pfns = candidates[(scope, lfn)]
        if not pfns:
            print("ERROR: no replica is available for {0}:{1}".format(scope, lfn))
            continue
        print("ERROR: {0} replicas are available for {1}:{2}".format(len(pfns), scope, lfn))
        for pfn, attrs in sorted(pfns.items(), key=lambda item: item[1].get("priority") or sys.maxsize):
            print("       rse={0} domain={1} priority={2} : {3}".format(attrs.get("rse"), attrs.get("domain"), attrs.get("priority"), pfn))
    if [key for key in bad if len(candidates[key]) > 1]:
        sys.exit("ERROR: narrow down replicas with --rse_expression=<RSE_EXP> and/or --schemes=<SCHEMES>")
    sys.exit(1)

with open("PoolFileCatalog.xml", "w") as pfc:
    pfc.write("<!--  Edited By POOL  -->\n")
    pfc.write("<POOLFILECATALOG>\n")
    for key in dids:
        pfc.write('<File ID="{0}">\n'.format(guid_map[key]))
        pfc.write("<physical>\n")
        pfc.write('<pfn filetype="ROOT_All" name="{0}"/>\n'.format(list(candidates[key])[0]))
        pfc.write("</physical>\n")
        pfc.write("<logical/>\n")
        pfc.write("</File>\n")
    pfc.write("</POOLFILECATALOG>\n")
print("INFO: generated PoolFileCatalog.xml for {0} input file(s)".format(len(dids)))
"""


def _get_input_file_list_in_params(param_str: str, lfn_set: set[str]) -> tuple[str | None, list[str] | None, str | None]:
    """
    Find the list of input files in trf parameters, so that it can be replaced with a shell variable

    Two styles are recognized: a python list used by runAthena/runGen (e.g. -i "['a', 'b']"), and a
    comma-separated list used by production trfs (e.g. --inputEVNTFile=a,b,c).

    Args:
        param_str (str): trf parameters of the job
        lfn_set (set[str]): LFNs of the input files which may appear in the parameters

    Returns:
        tuple[str | None, list[str] | None, str | None]: the matched substring, the LFNs in the order
            they appear in the parameters, and the string to replace the matched substring with.
            (None, None, None) if no list of input files is found
    """
    # python list style used by runAthena/runGen, e.g. -i "['a', 'b']"
    for match in re.finditer(r"\[[^\[\]]*\]", param_str):
        try:
            lfn_list = ast.literal_eval(match.group(0))
        except Exception:
            continue
        if isinstance(lfn_list, list) and lfn_list and all(isinstance(i, str) and i in lfn_set for i in lfn_list):
            return match.group(0), lfn_list, "[${input_list}]"
    # comma separated style used by production trfs, e.g. --inputEVNTFile=a,b,c
    for match in re.finditer(r"(--input\w*=)(\"?)([^\s\"']+)\2", param_str):
        lfn_list = match.group(3).split(",")
        if all(i in lfn_set for i in lfn_list):
            return (
                match.group(0),
                lfn_list,
                match.group(1) + match.group(2) + "${input_csv}" + match.group(2),
            )
    return None, None, None


def generate_offline_run_script(job_spec: "JobSpec") -> str:
    """
    Generate a shell script to rerun a job interactively

    The script retrieves the input files and then runs the transformations of the job in an ALRB
    container. It takes options to use only a subset of the input files (--nfiles) and to read them
    directly from storage through PoolFileCatalog.xml instead of downloading them (--direct).

    Args:
        job_spec (JobSpec): job specification with the Files attribute filled in

    Returns:
        str: the shell script, or a message starting with "ERROR: " when the script cannot be
            generated from the job specification
    """
    # user job
    is_user = False
    for trf in [
        "runAthena",
        "runGen",
        "runcontainer",
        "runMerge",
        "buildJob",
        "buildGen",
    ]:
        if trf in job_spec.transformation:
            is_user = True
            break
    # check prodSourceLabel
    if job_spec.prodSourceLabel == "user":
        is_user = True
    # the release is optional, i.e. it can be NULL in the DB
    atlas_release_str = job_spec.AtlasRelease
    if atlas_release_str in [None, "NULL"]:
        atlas_release_str = ""
    if is_user:
        atlas_releases = [atlas_release_str]
        home_packages = [re.sub("^AnalysisTransforms-*", "", job_spec.homepackage)]
        job_params_list = [job_spec.jobParameters]
        transformations = [job_spec.transformation]
    else:
        # release and trf
        atlas_releases = atlas_release_str.split("\n")
        home_packages = job_spec.homepackage.split("\n")
        job_params_list = job_spec.jobParameters.split("\n")
        transformations = job_spec.transformation.split("\n")
    if not (len(atlas_releases) == len(home_packages) == len(job_params_list) == len(transformations)):
        return "ERROR: The number of releases or parameters or trfs is inconsistent with others"
    # collect inputs. archives (lib.tgz, DBRelease, ...) are always downloaded since
    # they cannot be read directly from storage
    aux_dids = []
    data_dids = {}
    guid_map = {}
    for tmp_file in job_spec.Files:
        if tmp_file.type != "input":
            continue
        tmp_did = tmp_file.scope + ":" + tmp_file.lfn
        if tmp_file.lfn.endswith(".tgz") or tmp_file.lfn.endswith(".tar.gz"):
            if tmp_did not in aux_dids:
                aux_dids.append(tmp_did)
        elif tmp_file.lfn not in data_dids:
            data_dids[tmp_file.lfn] = tmp_did
            guid_map[tmp_did] = None if tmp_file.GUID in [None, "NULL", ""] else tmp_file.GUID.upper()
    # replace the list of input files in the trf parameters with a shell variable, so that
    # --nfiles can shrink it when the script runs
    ordered_lfns = None
    new_params = []
    for param_str in job_params_list:
        matched_str, matched_lfns, replacement_str = _get_input_file_list_in_params(param_str, set(data_dids))
        if matched_str is not None:
            param_str = param_str.replace(matched_str, replacement_str)
            if ordered_lfns is None:
                ordered_lfns = matched_lfns
        new_params.append(param_str)
    job_params_list = new_params
    if ordered_lfns is None:
        # the list was not found in the parameters, i.e. --nfiles cannot be supported
        data_files = list(data_dids)
    else:
        data_files = [tmp_lfn for tmp_lfn in ordered_lfns if tmp_lfn in data_dids]
        # input files which don't appear in the parameters are simply downloaded
        aux_dids += [tmp_did for tmp_lfn, tmp_did in data_dids.items() if tmp_lfn not in data_files]
    # construct script
    script_str = (
        "#!/bin/bash\n\n"
        "# To rerun the job interactively :\n"
        "#   1) download this script\n"
        "#   2) chmod +x ./<this script>\n"
        "#   3) setupATLAS\n"
        "#   4) ./<this script> [options]\n"
        "#\n"
        "# Options:\n"
        "#   --nfiles=<N>                use only the first N input files\n"
        "#   --direct                    read input files directly from storage instead of\n"
        "#                               downloading them, using PoolFileCatalog.xml\n"
        "#   --rse_expression=<RSE_EXP>  RSE expression to choose replicas for --direct\n"
        "#   --schemes=<SCHEMES>         comma-separated protocols for --direct. default: root\n"
        "\n"
        'usage() { sed -n "/^# To rerun/,/^$/p" "$0"; }\n\n'
        "direct=0\n"
        'nfiles=""\n'
        'rse_expression=""\n'
        'schemes="root"\n'
        'direct_opts=""\n'
        'for arg in "$@"; do\n'
        '  case "$arg" in\n'
    )
    # --usePFCTurl and --directIn are understood only by the trfs for analysis jobs
    if is_user:
        script_str += '    --direct)           direct=1; direct_opts=" --usePFCTurl --directIn" ;;\n'
    else:
        script_str += "    --direct)           direct=1 ;;\n"
    script_str += (
        '    --nfiles=*)         nfiles="${arg#*=}" ;;\n'
        '    --rse_expression=*) rse_expression="${arg#*=}" ;;\n'
        '    --schemes=*)        schemes="${arg#*=}" ;;\n'
        "    -h|--help)          usage; exit 0 ;;\n"
        '    *) echo "ERROR: unknown option: $arg"; usage; exit 1 ;;\n'
        "  esac\n"
        "done\n\n"
    )
    # setupATLAS is required both to retrieve the input files and to setup the container
    script_str += 'if [ -z "$ATLAS_LOCAL_ROOT_BASE" ]; then\n  echo "ERROR: setupATLAS is required to run this script"; exit 1\nfi\n\n'
    # list of input files which are subject to --nfiles and --direct
    if not data_files:
        script_str += (
            'if [ -n "$nfiles" ]; then echo "ERROR: --nfiles is not available for this job"; exit 1; fi\n'
            'if [ "$direct" -eq 1 ]; then echo "ERROR: --direct is not available for this job"; exit 1; fi\n\n'
        )
    else:
        script_str += "#input files\n"
        script_str += "data_dids=(" + " ".join(['"' + data_dids[tmp_lfn] + '"' for tmp_lfn in data_files]) + ")\n"
        if ordered_lfns is None:
            script_str += 'if [ -n "$nfiles" ]; then\n  echo "ERROR: --nfiles is not available for this job"; exit 1\nfi\n'
        else:
            script_str += 'if [ -n "$nfiles" ]; then\n'
            script_str += '  case "$nfiles" in \'\'|*[!0-9]*|0) echo "ERROR: --nfiles must be a positive integer"; exit 1 ;; esac\n'
            script_str += "  nfiles=$((10#$nfiles))\n"
            script_str += '  data_dids=("${data_dids[@]:0:$nfiles}")\n'
            script_str += f'  echo "INFO: using ${{#data_dids[@]}} of {len(data_files)} input files"\n'
            script_str += "fi\n"
        script_str += (
            "data_lfns=()\n"
            'for did in "${data_dids[@]}"; do data_lfns+=("${did#*:}"); done\n'
            'input_csv=$(IFS=,; echo "${data_lfns[*]}")\n'
            'input_list=$(printf ", \'%s\'" "${data_lfns[@]}")\n'
            'input_list="${input_list:2}"\n\n'
        )
    # retrieve inputs. the current directory is shared with the ALRB container, so that the trf sees
    # the downloaded files and PoolFileCatalog.xml. rucio is setup in a subshell to keep its
    # environment out of the container setup and the transformations. ALRB is setup there as well
    # since lsetup is a shell function which is not necessarily inherited by this script
    if data_files or aux_dids:
        script_str += "#retrieve inputs\n(\n"
        script_str += "  source ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh --quiet\n  lsetup rucio\n"
        if data_files:
            # generate the file catalog for direct access
            script_str += (
                '  if [ "$direct" -eq 1 ]; then\n'
                "    #generate PoolFileCatalog.xml with replica PFNs\n"
                '    python3 - "$rse_expression" "$schemes" "${data_dids[@]}" << \'PFCEOF\'\n'
            )
            sub_guid_map = {data_dids[tmp_lfn]: guid_map[data_dids[tmp_lfn]] for tmp_lfn in data_files}
            script_str += _PFC_GENERATOR.replace("__GUID_MAP__", repr(sub_guid_map))
            script_str += "PFCEOF\n"
            script_str += (
                "    if [ $? -ne 0 ]; then exit 1; fi\n"
                "  else\n"
                '    for did in "${data_dids[@]}"; do\n'
                '      rucio download "$did" --no-subdir || exit 1\n'
                "    done\n"
                "  fi\n"
            )
        # archives (lib.tgz, DBRelease, ...) are always downloaded
        for tmp_did in aux_dids:
            script_str += f'  rucio download "{tmp_did}" --no-subdir || exit 1\n'
        script_str += ") || exit 1\n\n"
    if is_user:
        script_str += "#get trf\n"
        script_str += f"wget {transformations[0]} || exit 1\n"
        script_str += f"chmod +x {transformations[0].split('/')[-1]}\n\n"
    # the transformations run in an ALRB container
    script_str += (
        "temp_file=$(mktemp)\n"
        'cat << EOF > "$temp_file"\n\n'
        "source ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh\n"
        "\n#transform commands\n\n"
    )
    cmt_config = ""
    for tmp_idx, home_package in enumerate(home_packages):
        # asetup
        atlas_release = re.sub("Atlas-", "", atlas_releases[tmp_idx])
        atlas_tags = re.split("[/_]", home_package)
        if "" in atlas_tags:
            atlas_tags.remove("")
        if atlas_release != "" and atlas_release not in atlas_tags and (re.search(r"^\d+\.\d+\.\d+$", atlas_release) is None or is_user):
            atlas_tags.append(atlas_release)
        try:
            cmt_config = [s for s in job_spec.cmtConfig.split("@") if s][-1]
        except Exception:
            cmt_config = ""
        script_str += f"asetup --platform={job_spec.cmtConfig.split('@')[0]} {','.join(atlas_tags)}\n"
        # athenaMP
        if job_spec.coreCount not in ["NULL", None] and job_spec.coreCount > 1:
            script_str += f"export ATHENA_PROC_NUMBER={job_spec.coreCount}\n"
            script_str += f"export ATHENA_CORE_NUMBER={job_spec.coreCount}\n"
        # add double quotes for zsh
        param_str = job_params_list[tmp_idx]
        splitter = shlex.shlex(param_str, posix=True)
        splitter.whitespace = " "
        splitter.whitespace_split = True
        # loop for params
        for item in splitter:
            match = re.search("^(-[^=]+=)(.+)$", item)
            if match is not None:
                arg_name = match.group(1)
                arg_value = match.group(2)
                arg_index = param_str.find(arg_name) + len(arg_name)
                # add "
                if param_str[arg_index] != '"':
                    param_str = param_str.replace(match.group(0), arg_name + '"' + arg_value + '"')
        # run trf
        if is_user:
            script_str += "./"
            param_str += " --debug${direct_opts}"
        script_str += f"{transformations[tmp_idx].split('/')[-1]} {param_str}\n\n"
    script_str += 'EOF\n\nchmod +x "$temp_file"\n'
    script_str += f'source ${{ATLAS_LOCAL_ROOT_BASE}}/user/atlasLocalSetup.sh -c {cmt_config} -r "$temp_file"\n'
    script_str += 'rm "$temp_file"\n'
    return script_str
