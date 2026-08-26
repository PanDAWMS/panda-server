"""
matching of hardware requirements against actual hardware

The requirement side comes from the task architecture, e.g. JediTaskSpec.get_host_gpu_spec().
The hardware side is a list of GPU dictionaries using the key names of the worker node GPU
monitoring (ATLAS_PANDA.worker_node_gpus / MV_WORKER_NODE_GPU_SUMMARY), i.e. vendor, model,
vram, architecture, framework_version, and driver_version. Those dictionaries either describe
all worker nodes of a PanDA queue, when brokering tasks to queues, or the GPUs of a single
worker node, when dispatching jobs to a pilot.
"""

import re

from packaging import version


def compare_version_string(version_string, comparison_string):
    """
    Compares a version string with another string composed of a comparison operator and a version string.

    Args:
        version_string (str): The version string to compare.
        comparison_string (str): The string containing the comparison operator and version string (e.g., ">=2.0").

    Returns:
        bool or None: True if the version string satisfies the comparison, False if it doesn't,
                       or None if the comparison string is invalid.
    """
    match = re.match(r"([=><!]+)(.+)", comparison_string)
    if not match:
        return None

    operator = match.group(1).strip()
    if operator == "=":
        operator = "=="
    version_to_compare = match.group(2).strip()

    try:
        version1 = version.parse(version_string)
        version2 = version.parse(version_to_compare)
    except version.InvalidVersion:
        return None

    if operator == "==":
        return version1 == version2
    elif operator == "!=":
        return version1 != version2
    elif operator == ">=":
        return version1 >= version2
    elif operator == "<=":
        return version1 <= version2
    elif operator == ">":
        return version1 > version2
    elif operator == "<":
        return version1 < version2
    else:
        return None


def match_gpu_spec(required_gpu_spec, gpus):
    """
    Checks whether GPUs satisfy the GPU requirement of a task.

    Selection attributes (vendor, model, microarchitecture) use an any match, i.e. it is enough that
    one GPU is of the requested type. Minimum-requirement attributes (vram, version, driver_version)
    use an all match, i.e. every GPU has to satisfy the constraint, so that a job cannot end up on a
    non-compliant GPU of a mixed set.

    Args:
        required_gpu_spec (dict): The GPU requirement of the task, with the keys vendor, model, vram,
                                  microarchitecture, version, and driver_version. Only vendor and model
                                  are mandatory and `*` is the wildcard for them. The model is either a
                                  regular expression for inclusion or a dictionary with pattern and excl
                                  keys for exclusion. The version, driver_version, and vram are
                                  operator-prefixed strings, e.g. `>=12.0`.
        gpus (list): List of dictionaries describing the actual GPUs, with the keys vendor, model, vram,
                     architecture, framework_version, and driver_version.

    Returns:
        bool: True if the GPUs satisfy the requirement.
    """
    # check vendor
    required_vendor = required_gpu_spec.get("vendor", "*")
    if required_vendor != "*":
        if not gpus or not any(gpu.get("vendor") and re.match(required_vendor, gpu["vendor"], re.IGNORECASE) for gpu in gpus):
            return False

    # check model (include or exclude pattern)
    required_model = required_gpu_spec.get("model", "*")
    if required_model != "*":
        if isinstance(required_model, dict):
            model_pattern = required_model["pattern"]
            model_excl = required_model.get("excl", False)
        else:
            model_pattern = required_model
            model_excl = False
        if not gpus:
            return False
        matches = any(gpu.get("model") and re.match(model_pattern, gpu["model"], re.IGNORECASE) for gpu in gpus)
        if matches == model_excl:
            return False

    # check VRAM (in MB); supports operators: ==, >=, <=, >, <, != (e.g. ">=40960")
    if "vram" in required_gpu_spec:
        if not gpus or not all(gpu.get("vram") and compare_version_string(str(gpu["vram"]), required_gpu_spec["vram"]) for gpu in gpus):
            return False

    # check GPU microarchitecture generation (e.g. Ampere, Hopper, Ada Lovelace)
    if "microarchitecture" in required_gpu_spec:
        req_arch = required_gpu_spec["microarchitecture"]
        if isinstance(req_arch, str):
            req_arch = [req_arch]
        if not gpus or not any(gpu.get("architecture") in req_arch for gpu in gpus):
            return False

    # check CUDA toolkit version
    if "version" in required_gpu_spec:
        if not gpus or not all(gpu.get("framework_version") and compare_version_string(gpu["framework_version"], required_gpu_spec["version"]) for gpu in gpus):
            return False

    # check GPU kernel driver version (e.g. 575.57.08)
    if "driver_version" in required_gpu_spec:
        if not gpus or not all(
            gpu.get("driver_version") and compare_version_string(gpu["driver_version"], required_gpu_spec["driver_version"]) for gpu in gpus
        ):
            return False

    return True
