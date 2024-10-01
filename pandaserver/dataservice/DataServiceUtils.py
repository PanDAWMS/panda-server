import re

from pandaserver.taskbuffer import JobUtils


def isCachedFile(dataset_name, site_spec):
    """
    Check if the file is cached on a site using CVMFS, e.g. for DB releases.

    Args:
        dataset_name (str): The name of the dataset.
        site_spec (SiteSpec): The site specification object.
    Returns:
        bool: True if the file is cached, False otherwise.
    """
    # Site does not use CVMFS
    if site_spec.iscvmfs is not True:
        return False
    # look for DBR
    if not dataset_name.startswith("ddo"):
        return False
    # look for the letter 'v' followed by 6 digits
    if re.search(r"v\d{6}$", dataset_name) is None:
        return False
    return True


# check invalid characters in dataset name
def checkInvalidCharacters(dataset_name):
    """
    Checks the validity of a dataset name.
    - The dataset name starts with an alphanumeric character ([A-Za-z0-9]).
    - The rest of the dataset name can contain alphanumeric characters, dots(.), hyphens(-),
      underscores(_), or slashes( /), and its length is between 1 and 255 characters({1, 255}).

    Args:
         dataset_name: The name of the dataset.
    Returns:
        True if the dataset name is valid, False otherwise.
    """
    if re.match("^[A-Za-z0-9][A-Za-z0-9\.\-\_/]{1,255}$", dataset_name) is not None:
        return True
    return False


# get dataset type
def getDatasetType(dataset):
    """
    Get the dataset type by extracting the fifth element from the dataset name.

    The dataset name is expected to be a string with elements separated by dots.
    For example:
    - 'mc23_13p6TeV:mc23_13p6TeV.801169.Py8EG_A14NNPDF23LO_jj_JZ4.merge.EVNT.e8514_e8528_tid38750682_00' would return 'EVNT'.
    - 'mc23_13p6TeV.801169.Py8EG_A14NNPDF23LO_jj_JZ4.simul.HITS.e8514_e8528_a934_tid41381346_00' would return 'HITS'.

    Args:
        dataset (str): The name of the dataset.
    Returns:
        str: The dataset type if it can be extracted, otherwise None.
    """
    try:
        dataset_type = dataset.split(".")[4]
    except Exception:
        dataset_type = None
    return dataset_type


def getSitesShareDDM(site_mapper, site_name, prod_source_label, job_label, output_share=False):
    """
    Get sites which share the DDM endpoint.

    Args:
        site_mapper (SiteMapper): The site mapper object.
        site_name (str): The name of the site.
        prod_source_label (str): The production source label.
        job_label (str): The job label.
        output_share (bool): False to get sites which use the output RSE as input,
                             True to get sites which use the input RSEs as output.
    Returns:
        list: A list of site names that share the DDM endpoint.
    """
    if not site_mapper.checkSite(site_name):
        return []

    site_spec = site_mapper.getSite(site_name)
    scope_site_input, scope_site_output = select_scope(site_spec, prod_source_label, job_label)
    runs_production = site_spec.runs_production()
    runs_analysis = site_spec.runs_analysis()

    ret_sites = []
    for tmp_site_name, tmp_site_spec in site_mapper.siteSpecList.items():
        scope_tmp_site_input, scope_tmp_site_output = select_scope(tmp_site_spec, prod_source_label, job_label)

        if (runs_production and not tmp_site_spec.runs_production()) or (runs_analysis and not tmp_site_spec.runs_analysis()):
            continue
        if tmp_site_spec.status != "online":
            continue

        try:
            if not output_share and site_spec.ddm_output[scope_site_output] not in tmp_site_spec.ddm_endpoints_input[scope_tmp_site_input].all:
                continue
            if output_share and tmp_site_spec.ddm_output[scope_site_output] not in site_spec.ddm_endpoints_input[scope_tmp_site_input].all:
                continue
        except Exception:
            continue

        if site_name != tmp_site_spec.sitename and tmp_site_spec.sitename not in ret_sites:
            ret_sites.append(tmp_site_spec.sitename)

    return ret_sites


def checkJobDestinationSE(tmp_job):
    """
    Check if the job has a destination storage element in a file specification.

    Args:
        tmp_job (JobSpec): The job object containing file specifications.
    Returns:
        str: The destination SE if specified, otherwise None.
    """
    for tmp_file in tmp_job.Files:
        destination_se = getDestinationSE(tmp_file.destinationDBlockToken)
        if destination_se is not None:
            return tmp_file.destinationSE
    return None


def getDestinationSE(destination_dblock_token):
    """
    Check if the destination is specified (e.g. dst:CERN-PROD_DATADISK) and extract it.

    Args:
        destination_dblock_token (str): The destination data block token.
    Returns:
        str: The extracted destination if specified, otherwise None.
    """
    if destination_dblock_token is not None:
        for tmp_token in destination_dblock_token.split(","):
            tmp_match = re.search(r"^dst:([^/]*)(/.*)*$", tmp_token)
            if tmp_match is not None:
                return tmp_match.group(1)
    return None


def getDistributedDestination(destination_dblock_token, ignore_empty=True):
    """
    Check if the destination is distributed (e.g. ddd:CERN-PROD_DATADISK) and extract it.

    Args:
        destination_dblock_token (str): The destination data block token.
        ignore_empty (bool): Flag to ignore empty locations.

    Returns:
        str: The extracted location if specified, otherwise None.
    """
    if destination_dblock_token is not None:
        for tmp_token in destination_dblock_token.split(","):
            tmp_match = re.search(r"^ddd:([^/]*)(/.*)*$", tmp_token)
            if tmp_match is not None:
                location = tmp_match.group(1)
                if ignore_empty and not location:
                    return None
                return location
    return None


def extractImportantError(message):
    """
    Extract important error strings from a message. This function searches for specific substrings within a given message and returns
    a concatenated string of lines containing those substrings.

    Args:
        message (str): The message string to search within.
    Returns:
        str: A concatenated string of lines containing the specified substrings.
              Returns an empty string if no matches are found or an exception occurs.
    """
    try:
        # list of strings to search for
        str_list = ["InvalidRSEExpression", "Details:"]
        return_string = " ".join(line for line in message.split("\n") if any(tmp_string in line for tmp_string in str_list))
    except Exception:
        return_string = ""
    return return_string


def getActivityForOut(prod_source_label):
    """
    Get the DDM activity type for the job output based on the production source label.

    Args:
        prod_source_label (str): The production source label.
    Returns:
        str: The activity type for output.
    """
    if prod_source_label in ["managed"]:
        activity = "Production Output"
    elif prod_source_label in ["user", "panda"]:
        activity = "Analysis Output"
    else:
        activity = "Functional Test"
    return activity


def select_scope(site_spec, prod_source_label, job_label):
    """
    Select the scopes of the activity for input and output. The scope was introduced for prod-analy queues where you might want to associate
    different RSEs depending on production or analysis.

    Args:
        site_spec (SiteSpec): The site specification object.
        prod_source_label (str): The production source label.
        job_label (str): The job label.
    Returns:
        tuple: A tuple containing the input scope and output scope.
    """
    scope_input = "default"
    aux_scopes_input = site_spec.ddm_endpoints_input.keys()
    if (job_label == JobUtils.ANALY_PS or prod_source_label in JobUtils.analy_sources) and "analysis" in aux_scopes_input:
        scope_input = "analysis"

    scope_output = "default"
    aux_scopes_output = site_spec.ddm_endpoints_output.keys()
    if (job_label == JobUtils.ANALY_PS or prod_source_label in JobUtils.analy_sources) and "analysis" in aux_scopes_output:
        scope_output = "analysis"

    return scope_input, scope_output


def isDBR(dataset_name):
    """
    Check if the dataset is a DB release. A DB release dataset name starts with 'ddo'.

    Args:
        dataset_name (str): The name of the dataset.

    Returns:
        bool: True if the dataset name starts with 'ddo', False otherwise.
    """
    if dataset_name.startswith("ddo"):
        return True
    return False


def is_top_level_dataset(dataset_name: str) -> bool:
    """
    Check if top dataset. Top datasets do not finish with '_sub' followed by one or more digits.

    Args:
        dataset_name (str): Dataset name.

    Returns:
        bool: True if top dataset, False otherwise.
    """
    return re.sub("_sub\d+$", "", dataset_name) == dataset_name


def is_sub_dataset(dataset_name: str) -> bool:
    """
    Check if the dataset name ends with '_sub' followed by one or more digits.

    Args:
        dataset_name (str): The name of the dataset.

    Returns:
        bool: True if the dataset name ends with '_sub' followed by one or more digits, False otherwise.
    """
    return re.search("_sub\d+$", dataset_name) is not None


def is_tid_dataset(destination_data_block: str) -> bool:
    """
    Check if the destination data block ends with '_tid' followed by one or more digits.

    Args:
        destination_data_block (str): The destination data block.

    Returns:
        bool: True if the destination data block ends with '_tid' followed by one or more digits, False otherwise.
    """
    return re.search("_tid[\d_]+$", destination_data_block) is not None


def is_hammercloud_dataset(destination_data_block: str) -> bool:
    """
    Check if the destination data block starts with 'hc_test.'.

    Args:
        destination_data_block (str): The destination data block.

    Returns:
        bool: True if the destination data block starts with 'hc_test.', False otherwise.
    """
    return re.search("^hc_test\.", destination_data_block) is not None


def is_user_gangarbt_dataset(destination_data_block: str) -> bool:
    """
    Check if the destination data block starts with 'user.gangarbt.'.

    Args:
        destination_data_block (str): The destination data block.

    Returns:
        bool: True if the destination data block starts with 'user.gangarbt.', False otherwise.
    """
    return re.search("^user\.gangarbt\.", destination_data_block) is not None


def is_lib_dataset(destination_data_block: str) -> bool:
    """
    Check if the destination data block ends with '.lib'.

    Args:
        destination_data_block (str): The destination data block.

    Returns:
        bool: True if the destination data block ends with '.lib', False otherwise.
    """
    return re.search("\.lib$", destination_data_block) is not None
