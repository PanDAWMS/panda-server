"""
generation of a shell script to rerun a job interactively

The generated script sets up an ALRB container, retrieves the input files, and runs the
transformations of the job. It takes options to use only a subset of the input files and to read
them directly from storage through a PoolFileCatalog.xml instead of downloading them.

This module intentionally depends only on the standard library, so that it can be used and tested
without a server configuration.
"""

import ast
import re
import shlex

# python script embedded in the offline running script to generate PoolFileCatalog.xml for direct access.
# it takes an RSE expression, comma-separated protocol schemes, and DIDs of input files as arguments
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


# find the list of input files in trf parameters, so that it can be replaced with a shell variable.
# returns the matched substring, the ordered LFNs, and the replacement string
def _get_input_file_list_in_params(param_str, lfn_set):
    # python list style used by runAthena/runGen, e.g. -i "['a', 'b']"
    for tmp_match in re.finditer(r"\[[^\[\]]*\]", param_str):
        try:
            tmp_list = ast.literal_eval(tmp_match.group(0))
        except Exception:
            continue
        if isinstance(tmp_list, list) and tmp_list and all(isinstance(i, str) and i in lfn_set for i in tmp_list):
            return tmp_match.group(0), tmp_list, "[${input_list}]"
    # comma separated style used by production trfs, e.g. --inputEVNTFile=a,b,c
    for tmp_match in re.finditer(r"(--input\w*=)(\"?)([^\s\"']+)\2", param_str):
        tmp_list = tmp_match.group(3).split(",")
        if all(i in lfn_set for i in tmp_list):
            return (
                tmp_match.group(0),
                tmp_list,
                tmp_match.group(1) + tmp_match.group(2) + "${input_csv}" + tmp_match.group(2),
            )
    return None, None, None


# generate a script to rerun the job interactively
def generate_offline_run_script(tmpJob):
    # user job
    isUser = False
    for trf in [
        "runAthena",
        "runGen",
        "runcontainer",
        "runMerge",
        "buildJob",
        "buildGen",
    ]:
        if trf in tmpJob.transformation:
            isUser = True
            break
    # check prodSourceLabel
    if tmpJob.prodSourceLabel == "user":
        isUser = True
    if isUser:
        tmpAtls = [tmpJob.AtlasRelease]
        tmpRels = [re.sub("^AnalysisTransforms-*", "", tmpJob.homepackage)]
        tmpPars = [tmpJob.jobParameters]
        tmpTrfs = [tmpJob.transformation]
    else:
        # release and trf
        tmpAtls = tmpJob.AtlasRelease.split("\n")
        tmpRels = tmpJob.homepackage.split("\n")
        tmpPars = tmpJob.jobParameters.split("\n")
        tmpTrfs = tmpJob.transformation.split("\n")
    if not (len(tmpRels) == len(tmpPars) == len(tmpTrfs)):
        return "ERROR: The number of releases or parameters or trfs is inconsistent with others"
    # collect inputs. archives (lib.tgz, DBRelease, ...) are always downloaded since
    # they cannot be read directly from storage
    auxDIDs = []
    dataDIDs = {}
    guidMap = {}
    for tmpFile in tmpJob.Files:
        if tmpFile.type != "input":
            continue
        tmpDID = tmpFile.scope + ":" + tmpFile.lfn
        if tmpFile.lfn.endswith(".tgz") or tmpFile.lfn.endswith(".tar.gz"):
            if tmpDID not in auxDIDs:
                auxDIDs.append(tmpDID)
        elif tmpFile.lfn not in dataDIDs:
            dataDIDs[tmpFile.lfn] = tmpDID
            guidMap[tmpDID] = None if tmpFile.GUID in [None, "NULL", ""] else tmpFile.GUID.upper()
    # replace the list of input files in the trf parameters with a shell variable, so that
    # --nfiles can shrink it when the script runs
    orderedLFNs = None
    newPars = []
    for tmpParamStr in tmpPars:
        tmpSubStr, tmpLFNs, tmpReplStr = _get_input_file_list_in_params(tmpParamStr, set(dataDIDs))
        if tmpSubStr is not None:
            tmpParamStr = tmpParamStr.replace(tmpSubStr, tmpReplStr)
            if orderedLFNs is None:
                orderedLFNs = tmpLFNs
        newPars.append(tmpParamStr)
    tmpPars = newPars
    if orderedLFNs is None:
        # the list was not found in the parameters, i.e. --nfiles cannot be supported
        dataFiles = list(dataDIDs)
    else:
        dataFiles = [tmpLFN for tmpLFN in orderedLFNs if tmpLFN in dataDIDs]
        # input files which don't appear in the parameters are simply downloaded
        auxDIDs += [tmpDID for tmpLFN, tmpDID in dataDIDs.items() if tmpLFN not in dataFiles]
    # construct script
    scrStr = (
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
    if isUser:
        scrStr += '    --direct)           direct=1; direct_opts=" --usePFCTurl --directIn" ;;\n'
    else:
        scrStr += "    --direct)           direct=1 ;;\n"
    scrStr += (
        '    --nfiles=*)         nfiles="${arg#*=}" ;;\n'
        '    --rse_expression=*) rse_expression="${arg#*=}" ;;\n'
        '    --schemes=*)        schemes="${arg#*=}" ;;\n'
        "    -h|--help)          usage; exit 0 ;;\n"
        '    *) echo "ERROR: unknown option: $arg"; usage; exit 1 ;;\n'
        "  esac\n"
        "done\n\n"
    )
    # list of input files which are subject to --nfiles and --direct
    if not dataFiles:
        scrStr += (
            'if [ -n "$nfiles" ]; then echo "ERROR: --nfiles is not available for this job"; exit 1; fi\n'
            'if [ "$direct" -eq 1 ]; then echo "ERROR: --direct is not available for this job"; exit 1; fi\n\n'
        )
    else:
        scrStr += "#input files\n"
        scrStr += "data_dids=(" + " ".join(['"' + dataDIDs[tmpLFN] + '"' for tmpLFN in dataFiles]) + ")\n"
        if orderedLFNs is None:
            scrStr += 'if [ -n "$nfiles" ]; then\n' '  echo "ERROR: --nfiles is not available for this job"; exit 1\n' "fi\n"
        else:
            scrStr += 'if [ -n "$nfiles" ]; then\n'
            scrStr += '  case "$nfiles" in \'\'|*[!0-9]*|0) echo "ERROR: --nfiles must be a positive integer"; exit 1 ;; esac\n'
            scrStr += "  nfiles=$((10#$nfiles))\n"
            scrStr += '  data_dids=("${data_dids[@]:0:$nfiles}")\n'
            scrStr += f'  echo "INFO: using ${{#data_dids[@]}} of {len(dataFiles)} input files"\n'
            scrStr += "fi\n"
        scrStr += (
            "data_lfns=()\n"
            'for did in "${data_dids[@]}"; do data_lfns+=("${did#*:}"); done\n'
            'input_csv=$(IFS=,; echo "${data_lfns[*]}")\n'
            'input_list=$(printf ", \'%s\'" "${data_lfns[@]}")\n'
            'input_list="${input_list:2}"\n\n'
        )
        # generate the file catalog for direct access
        scrStr += (
            'if [ "$direct" -eq 1 ]; then\n'
            '  if [ -z "$ATLAS_LOCAL_ROOT_BASE" ]; then\n'
            '    echo "ERROR: setupATLAS is required to run this script"; exit 1\n'
            "  fi\n"
            "  #generate PoolFileCatalog.xml with replica PFNs\n"
            "  lsetup rucio\n"
            '  python3 - "$rse_expression" "$schemes" "${data_dids[@]}" << \'PFCEOF\'\n'
        )
        tmpGuidMap = {dataDIDs[tmpLFN]: guidMap[dataDIDs[tmpLFN]] for tmpLFN in dataFiles}
        scrStr += _PFC_GENERATOR.replace("__GUID_MAP__", repr(tmpGuidMap))
        scrStr += "PFCEOF\n"
        scrStr += "  if [ $? -ne 0 ]; then exit 1; fi\n"
        scrStr += "fi\n\n"
    # the rest of the script runs in an ALRB container
    scrStr += (
        "temp_file=$(mktemp)\n"
        'cat << EOF > "$temp_file"\n\n'
        "source ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh\n"
        "lsetup rucio\n\n"
        "#retrieve inputs\n\n"
        "EOF\n\n"
    )
    if dataFiles:
        scrStr += (
            'if [ "$direct" -eq 0 ]; then\n'
            '  for did in "${data_dids[@]}"; do\n'
            '    echo "rucio download $did --no-subdir" >> "$temp_file"\n'
            "  done\n"
            "else\n"
            '  echo "#input files are read directly from storage. see PoolFileCatalog.xml" >> "$temp_file"\n'
            "fi\n\n"
        )
    scrStr += 'cat << EOF >> "$temp_file"\n'
    for tmpDID in auxDIDs:
        scrStr += f"rucio download {tmpDID} --no-subdir\n"
    if isUser:
        scrStr += "\n#get trf\n"
        scrStr += f"wget {tmpTrfs[0]}\n"
        scrStr += f"chmod +x {tmpTrfs[0].split('/')[-1]}\n"
    scrStr += "\n#transform commands\n\n"
    cmtConfig = ""
    for tmpIdx, tmpRel in enumerate(tmpRels):
        # asetup
        atlRel = re.sub("Atlas-", "", tmpAtls[tmpIdx])
        atlTags = re.split("[/_]", tmpRel)
        if "" in atlTags:
            atlTags.remove("")
        if atlRel != "" and atlRel not in atlTags and (re.search("^\d+\.\d+\.\d+$", atlRel) is None or isUser):
            atlTags.append(atlRel)
        try:
            cmtConfig = [s for s in tmpJob.cmtConfig.split("@") if s][-1]
        except Exception:
            cmtConfig = ""
        scrStr += f"asetup --platform={tmpJob.cmtConfig.split('@')[0]} {','.join(atlTags)}\n"
        # athenaMP
        if tmpJob.coreCount not in ["NULL", None] and tmpJob.coreCount > 1:
            scrStr += f"export ATHENA_PROC_NUMBER={tmpJob.coreCount}\n"
            scrStr += f"export ATHENA_CORE_NUMBER={tmpJob.coreCount}\n"
        # add double quotes for zsh
        tmpParamStr = tmpPars[tmpIdx]
        tmpSplitter = shlex.shlex(tmpParamStr, posix=True)
        tmpSplitter.whitespace = " "
        tmpSplitter.whitespace_split = True
        # loop for params
        for tmpItem in tmpSplitter:
            tmpMatch = re.search("^(-[^=]+=)(.+)$", tmpItem)
            if tmpMatch is not None:
                tmpArgName = tmpMatch.group(1)
                tmpArgVal = tmpMatch.group(2)
                tmpArgIdx = tmpParamStr.find(tmpArgName) + len(tmpArgName)
                # add "
                if tmpParamStr[tmpArgIdx] != '"':
                    tmpParamStr = tmpParamStr.replace(tmpMatch.group(0), tmpArgName + '"' + tmpArgVal + '"')
        # run trf
        if isUser:
            scrStr += "./"
            tmpParamStr += " --debug${direct_opts}"
        scrStr += f"{tmpTrfs[tmpIdx].split('/')[-1]} {tmpParamStr}\n\n"
    scrStr += "EOF\n\n" 'chmod +x "$temp_file"\n'
    scrStr += 'source ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh -c %s -r "$temp_file"\n' % cmtConfig
    scrStr += 'rm "$temp_file"\n'
    return scrStr
