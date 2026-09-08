#!/usr/bin/env python

import sys
import xml.dom.minidom
from typing import TYPE_CHECKING, Any
from urllib.parse import quote

if TYPE_CHECKING:
    from pandaserver.taskbuffer.FileSpec import FileSpec


class dom_job:
    """infiles[inds]=[file1,file2...]
    outfiles = [file1,file2...]
    command  - script that will be executed on the grid
    prepend  - list of (option,value) prepended to output file name
    forward  - list of (option,value) forwarded to the grid job
    """

    def __init__(
        s,
        domjob: xml.dom.minidom.Element | None = None,
        primaryds: str | None = None,
        defaultcmd: str | None = None,
        defaultout: list[str] = [],
    ) -> None:
        """Loads <job></job> from xml file.
        If primaryds is set, makes sure it is present in job spec"""
        s.infiles: dict[str, Any] = {}
        s.outfiles = []
        s.command = defaultcmd
        s.prepend = []
        s.forward = []
        if not domjob:
            return
        # script executed on the grid node for this job
        if len(domjob.getElementsByTagName("command")) > 0:
            s.command = dom_parser.text(domjob.getElementsByTagName("command")[0])
        # input files
        for inds in domjob.getElementsByTagName("inds"):
            name = dom_parser.text(inds.getElementsByTagName("name")[0])
            files = inds.getElementsByTagName("file")
            if len(files) == 0:
                continue
            s.infiles[name] = []
            for file in files:
                s.infiles[name].append(dom_parser.text(file))
        if primaryds and primaryds not in s.infiles.keys():
            print(f"ERROR: primaryds={primaryds} must be present in each job")
            sys.exit(0)
        # output files (also, drop duplicates within this job)
        outfiles = set(defaultout)
        outfiles.update(dom_parser.text(v) for v in domjob.getElementsByTagName("output"))
        s.outfiles = list(outfiles)
        # gearing options
        for o in domjob.getElementsByTagName("option"):
            name = o.attributes["name"].value
            value = dom_parser.text(o)
            prepend = dom_parser.true(o.attributes["prepend"].value)
            forward = dom_parser.true(o.attributes["forward"].value)
            if prepend:
                s.prepend.append((name, value))
            if forward:
                s.forward.append((name, value))

    def to_dom(s) -> xml.dom.minidom.Element:
        """Converts this job to a dom tree branch"""
        x = xml.dom.minidom.Document()
        job = x.createElement("job")
        # appendChild returns the element it appended, so each new node is named rather
        # than reached back through childNodes[-1]: a child list holds text and comments
        # as well as elements, so the last entry of one is not an element to a reader
        for inds in s.infiles.keys():
            inds_node = job.appendChild(x.createElement("inds"))
            name_node = inds_node.appendChild(x.createElement("name"))
            name_node.appendChild(x.createTextNode(inds))
            for file in s.infiles[inds]:
                file_node = inds_node.appendChild(x.createElement("file"))
                file_node.appendChild(x.createTextNode(file))
        for outfile in s.outfiles:
            output_node = job.appendChild(x.createElement("output"))
            output_node.appendChild(x.createTextNode(outfile))
        if s.command:
            command_node = job.appendChild(x.createElement("command"))
            command_node.appendChild(x.createTextNode(s.command))
        for option in s.prepend + list(set(s.prepend + s.forward) - set(s.prepend)):
            option_node = job.appendChild(x.createElement("option"))
            option_node.setAttribute("name", str(option[0]))
            option_node.setAttribute("forward", "true" if option in s.forward else "false")
            option_node.setAttribute("prepend", "true" if option in s.prepend else "false")
            option_node.appendChild(x.createTextNode(str(option[1])))
        return job

    def files_in_DS(s, DS: str) -> list[str]:
        """Returns a list of files used in a given job in a given dataset"""
        if DS in s.infiles:
            return s.infiles[DS]
        else:
            return []

    def forward_opts(s) -> str:
        """passable string of forward options"""
        return " ".join([f"{v[0]}={v[1]}" for v in s.forward])

    def prepend_string(s) -> str:
        """a tag string prepended to output files"""
        return "_".join([f"{v[0]}{v[1]}" for v in s.prepend])

    def exec_string(s) -> str:
        """exec string for prun.
        If user requested to run script run.sh (via <command>run.sh</command>), it will return
        opt1=value1 opt2=value2 opt3=value3 run.sh
        This way, all options will be set inside run.sh
        """
        return f"{s.forward_opts()} {s.command}"

    def exec_string_enc(s) -> str:
        """exec string for prun.
        If user requested to run script run.sh (via <command>run.sh</command>), it will return
        opt1=value1 opt2=value2 opt3=value3 run.sh
        This way, all options will be set inside run.sh
        """
        comStr = f"{s.forward_opts()} {s.command}"
        return quote(comStr)

    def get_outmap_str(s, outMap: dict[str, "FileSpec"]) -> str:
        """return mapping of original and new filenames"""
        newMap = {}
        for oldLFN, fileSpec in outMap.items():
            newMap[oldLFN] = str(fileSpec.lfn)
        return str(newMap)

    def outputs_list(s, prepend: bool = False) -> list[str]:
        """python list with finalized output file names"""
        if prepend and s.prepend_string():
            return [s.prepend_string() + "." + o for o in s.outfiles]
        else:
            return [o for o in s.outfiles]

    def outputs(s, prepend: bool = False) -> str:
        """Comma-separated list of output files accepted by prun"""
        return ",".join(s.outputs_list(prepend))


class dom_parser:
    def __init__(s, fname: str | None = None, xmlStr: str | None = None) -> None:
        """creates a dom object out of a text file (if provided)"""
        s.fname = fname
        s.dom: xml.dom.minidom.Document | None = None
        s.title: str | None = None
        s.tag: str | None = None
        s.command: str | None = None
        s.outds: str | None = None
        # input dataset name -> the stream it is read as
        s.inds: dict[str, str] = {}
        s.global_outfiles: list[str] = []
        s.jobs: list["dom_job"] = []
        s.primaryds: str | None = None
        if fname:
            s.dom = xml.dom.minidom.parse(fname)
            s.parse()
            s.check()
        if xmlStr is not None:
            s.dom = xml.dom.minidom.parseString(xmlStr)
            s.parse()
            s.check()

    @staticmethod
    def true(v: str) -> bool:
        """define True"""
        return v in ("1", "true", "True", "TRUE", "yes", "Yes", "YES")

    @staticmethod
    def text(pnode: xml.dom.minidom.Node) -> str:
        """extracts the value stored in the node"""
        rc = []
        for node in pnode.childNodes:
            if node.nodeType == node.TEXT_NODE:
                rc.append(str(node.data).strip())
        return "".join(rc)

    def parse(s) -> None:
        """loads submission configuration from an xml file"""
        if s.dom is None:
            # __init__ parses the document before it calls this
            raise RuntimeError("parse() called before a document was loaded")
        try:
            # general settings
            if len(s.dom.getElementsByTagName("title")) > 0:
                s.title = dom_parser.text(s.dom.getElementsByTagName("title")[0])
            else:
                s.title = "Default title"
            if len(s.dom.getElementsByTagName("tag")) > 0:
                s.tag = dom_parser.text(s.dom.getElementsByTagName("tag")[0])
            else:
                s.tag = "default_tag"
            s.command = None  # can be overridden in subjobs
            for elm in s.dom.getElementsByTagName("submission")[0].childNodes:
                if elm.nodeName != "command":
                    continue
                s.command = dom_parser.text(elm)
                break
            s.global_outfiles = []  # subjobs can append *additional* outputs
            for elm in s.dom.getElementsByTagName("submission")[0].childNodes:
                if elm.nodeName != "output":
                    continue
                s.global_outfiles.append(dom_parser.text(elm))
            s.outds = dom_parser.text(s.dom.getElementsByTagName("outds")[0])
            # declaration of all input datasets
            primarydss = []
            for elm in s.dom.getElementsByTagName("submission")[0].childNodes:
                # a child list holds text and comments too, and only an element has
                # attributes or elements of its own; the name check alone does not say so
                if not isinstance(elm, xml.dom.minidom.Element) or elm.nodeName != "inds":
                    continue
                if "primary" in elm.attributes.keys() and dom_parser.true(elm.attributes["primary"].value):
                    primary = True
                else:
                    primary = False
                stream = dom_parser.text(elm.getElementsByTagName("stream")[0])
                name = dom_parser.text(elm.getElementsByTagName("name")[0])
                s.inds[name] = stream
                if primary:
                    primarydss.append(name)
            # see if one of the input datasets was explicitly labeled as inDS
            if len(primarydss) == 1:
                s.primaryds = primarydss[0]
            else:
                s.primaryds = None
            for job in s.dom.getElementsByTagName("job"):
                s.jobs.append(dom_job(job, primaryds=s.primaryds, defaultcmd=s.command, defaultout=s.global_outfiles))
        except Exception:
            print(f"ERROR: failed to parse {s.fname}")
            raise

    def to_dom(s) -> xml.dom.minidom.Element:
        """Converts this submission to a dom tree branch"""
        x = xml.dom.minidom.Document()
        submission = x.createElement("submission")
        # named rather than reached back through childNodes[-1], for the reason given in
        # dom_job.to_dom above
        if s.title:
            title_node = submission.appendChild(x.createElement("title"))
            title_node.appendChild(x.createTextNode(s.title))
        if s.tag:
            tag_node = submission.appendChild(x.createElement("tag"))
            tag_node.appendChild(x.createTextNode(s.tag))
        for name, stream in s.inds.items():
            inds_node = submission.appendChild(x.createElement("inds"))
            inds_node.setAttribute("primary", "true" if name == s.primaryds else "false")
            stream_node = inds_node.appendChild(x.createElement("stream"))
            stream_node.appendChild(x.createTextNode(stream))
            name_node = inds_node.appendChild(x.createElement("name"))
            name_node.appendChild(x.createTextNode(name))
        if s.command:
            command_node = submission.appendChild(x.createElement("command"))
            command_node.appendChild(x.createTextNode(s.command))
        for outfile in s.global_outfiles:
            output_node = submission.appendChild(x.createElement("output"))
            output_node.appendChild(x.createTextNode(outfile))
        outds_node = submission.appendChild(x.createElement("outds"))
        # parse() always sets outds, from the document or from the default
        outds_node.appendChild(x.createTextNode(s.outds or ""))
        for job in s.jobs:
            submission.appendChild(job.to_dom())
        return submission

    def check(s) -> None:
        """checks that all output files have unique qualifiers"""
        quals = []
        for j in s.jobs:
            quals += j.outputs_list(True)
        if len(list(set(quals))) != len(quals):
            print("ERROR: found non-unique output file names across the jobs")
            print("(you likely need to review xml options with prepend=true)")
            sys.exit(0)

    def input_datasets(s) -> list[str]:
        """returns a list of all used input datasets"""
        DSs = set()
        for j in s.jobs:
            for ds in j.infiles.keys():
                DSs.add(ds)
        return list(DSs)

    def inDS(s) -> str:
        """chooses a dataset we'll call inDS; others will become secondaryDS"""
        # user manually labeled one of datasets as primary, so make it inDS:
        if s.primaryds:
            return s.primaryds
        # OR: choose inDS dataset randomly
        else:
            return s.input_datasets()[0]

    def secondaryDSs(s) -> list[str]:
        """returns all secondaryDSs. This excludes inDS, unless inDS is managed by prun"""
        return [d for d in s.input_datasets() if d != s.inDS()]

    def writeInputToTxt(s) -> str:
        """Prepares prun option --writeInputToTxt
        comma-separated list of STREAM:STREAM.files.dat
        """
        out = []
        DSs = s.secondaryDSs()
        for i, DS in enumerate(DSs):
            if DS in s.inds:
                stream = s.inds[DS]
            else:
                stream = "IN%d" % (i + 1,)
            out.append(f"{stream}:{stream}.files.dat")
        out.append("IN:IN.files.dat")
        return ",".join(out)

    def files_in_DS(s, DS: str, regex: bool = False) -> str | list[str]:
        """Returns a list of all files from a given dataset
        that will be used in at least one job in this submission
        If regex==True, the list is converted to a regex string
        """
        assert DS in s.input_datasets(), f"ERROR: dataset {DS} was not requested in the xml file"
        files = []
        for j in s.jobs:
            if DS in j.infiles.keys():
                files += j.infiles[DS]
        if regex:
            return "|".join(sorted(list(set(files))))
        else:
            return sorted(list(set(files)))

    def nJobs(s) -> int:
        return len(s.jobs)

    def dump(s, verbose: bool = True) -> None:
        """prints a summary of this submission"""

        def P(key: str, value: Any = "") -> None:
            if value == "":
                print(key)
            else:
                # str() because the counts below are numbers, which the concatenation this
                # replaces could not take
                print((key + ":").ljust(14) + " " + str(value))

        P("XML FILE LOADED", s.fname)
        P("Title", s.title)
        P("Command", s.command)
        P("InDS", s.inDS())
        P("Output DS", s.outds)
        P("njobs", s.nJobs())
        if verbose:
            for i, job in enumerate(s.jobs):
                P("===============> JOB%d" % i)
                P("command", job.exec_string())
                P("outfiles", job.outputs())
                P("INPUTS:")
                j = 0
                for dsname, files in job.infiles.items():
                    P("  Dataset%d" % j, dsname)
                    for k, fname in enumerate(files):
                        P("     File%d" % k, fname)
                    j += 1


if __name__ == "__main__":
    p = dom_parser("./job.xml")
    p.dump()
