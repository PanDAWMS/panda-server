# Description: Unit tests for the hardware matching functions
import unittest

from pandaserver.srvcore.hardware_matching import (
    compare_version_string,
    match_gpu_spec,
)

A100 = {
    "vendor": "NVIDIA",
    "model": "NVIDIA A100-SXM4-40GB",
    "vram": 40960,
    "architecture": "Ampere",
    "framework_version": "12.4",
    "driver_version": "575.57.08",
}

V100 = {
    "vendor": "NVIDIA",
    "model": "Tesla V100-SXM2-16GB",
    "vram": 16384,
    "architecture": "Volta",
    "framework_version": "12.2",
    "driver_version": "535.104.05",
}

ANY_GPU = {"vendor": "*", "model": "*"}


class TestCompareVersionString(unittest.TestCase):
    def test_operators(self):
        self.assertTrue(compare_version_string("12.4", ">=12.0"))
        self.assertFalse(compare_version_string("11.8", ">=12.0"))
        self.assertTrue(compare_version_string("11.8", "<=12.0"))
        self.assertTrue(compare_version_string("12.4", ">12.0"))
        self.assertFalse(compare_version_string("12.0", ">12.0"))
        self.assertTrue(compare_version_string("11.8", "<12.0"))
        self.assertTrue(compare_version_string("12.0", "==12.0"))
        self.assertFalse(compare_version_string("12.4", "==12.0"))

    def test_single_equal_is_equality(self):
        self.assertTrue(compare_version_string("12.0", "=12.0"))
        self.assertFalse(compare_version_string("12.4", "=12.0"))

    def test_not_equal(self):
        # the != operator used to be unreachable since the operator pattern didn't accept !
        self.assertTrue(compare_version_string("12.4", "!=12.0"))
        self.assertFalse(compare_version_string("12.0", "!=12.0"))

    def test_multi_component_version(self):
        self.assertTrue(compare_version_string("575.57.08", ">=575.0"))
        self.assertFalse(compare_version_string("535.104.05", ">=575.0"))

    def test_invalid_input(self):
        # no operator
        self.assertIsNone(compare_version_string("12.0", "12.0"))
        # unparsable versions
        self.assertIsNone(compare_version_string("Ampere", ">=12.0"))
        self.assertIsNone(compare_version_string("12.0", ">=Ampere"))


class TestMatchGpuSpec(unittest.TestCase):
    def test_wildcard_requirement(self):
        self.assertTrue(match_gpu_spec(ANY_GPU, [A100]))
        self.assertTrue(match_gpu_spec(ANY_GPU, [A100, V100]))
        # no GPU information is not a rejection for a wildcard requirement
        self.assertTrue(match_gpu_spec(ANY_GPU, []))

    def test_specific_requirement_without_gpus(self):
        self.assertFalse(match_gpu_spec({"vendor": "NVIDIA", "model": "*"}, []))
        self.assertFalse(match_gpu_spec({"vendor": "*", "model": ".*A100.*"}, []))
        self.assertFalse(match_gpu_spec({"vendor": "*", "model": "*", "vram": ">=40960"}, []))
        self.assertFalse(match_gpu_spec({"vendor": "*", "model": "*", "microarchitecture": "Ampere"}, []))

    def test_vendor(self):
        self.assertTrue(match_gpu_spec({"vendor": "NVIDIA", "model": "*"}, [A100]))
        self.assertTrue(match_gpu_spec({"vendor": "nvidia", "model": "*"}, [A100]))
        self.assertFalse(match_gpu_spec({"vendor": "AMD", "model": "*"}, [A100]))
        # any match, one of the GPUs is enough
        self.assertTrue(match_gpu_spec({"vendor": "NVIDIA", "model": "*"}, [{"vendor": "AMD", "model": "MI250"}, A100]))

    def test_model_inclusion(self):
        self.assertTrue(match_gpu_spec({"vendor": "*", "model": ".*A100.*"}, [A100]))
        # matching is case-insensitive
        self.assertTrue(match_gpu_spec({"vendor": "*", "model": ".*a100.*"}, [A100]))
        self.assertFalse(match_gpu_spec({"vendor": "*", "model": ".*A100.*"}, [V100]))
        # any match
        self.assertTrue(match_gpu_spec({"vendor": "*", "model": ".*A100.*"}, [V100, A100]))

    def test_model_exclusion(self):
        excl_p100 = {"vendor": "*", "model": {"pattern": ".*P100.*", "excl": True}}
        self.assertTrue(match_gpu_spec(excl_p100, [A100]))
        P100 = dict(A100, model="Tesla P100-PCIE-16GB")
        self.assertFalse(match_gpu_spec(excl_p100, [P100]))
        # excluded when any of the GPUs matches the pattern
        self.assertFalse(match_gpu_spec(excl_p100, [A100, P100]))

    def test_vram(self):
        self.assertTrue(match_gpu_spec(dict(ANY_GPU, vram=">=40960"), [A100]))
        self.assertFalse(match_gpu_spec(dict(ANY_GPU, vram=">=40960"), [V100]))
        # all match, every GPU has to meet the minimum
        self.assertFalse(match_gpu_spec(dict(ANY_GPU, vram=">=40960"), [A100, V100]))
        self.assertTrue(match_gpu_spec(dict(ANY_GPU, vram=">=16384"), [A100, V100]))
        self.assertTrue(match_gpu_spec(dict(ANY_GPU, vram="==40960"), [A100]))

    def test_microarchitecture(self):
        self.assertTrue(match_gpu_spec(dict(ANY_GPU, microarchitecture="Ampere"), [A100]))
        self.assertFalse(match_gpu_spec(dict(ANY_GPU, microarchitecture="Ampere"), [V100]))
        # a list of generations is accepted
        self.assertTrue(match_gpu_spec(dict(ANY_GPU, microarchitecture=["Ampere", "Hopper"]), [A100]))
        self.assertFalse(match_gpu_spec(dict(ANY_GPU, microarchitecture=["Ampere", "Hopper"]), [V100]))
        # any match, one of the GPUs is enough
        self.assertTrue(match_gpu_spec(dict(ANY_GPU, microarchitecture="Ampere"), [V100, A100]))

    def test_framework_version(self):
        self.assertTrue(match_gpu_spec(dict(ANY_GPU, version=">=12.0"), [A100, V100]))
        # all match, a single old GPU excludes the whole set
        self.assertFalse(match_gpu_spec(dict(ANY_GPU, version=">=12.3"), [A100, V100]))
        self.assertTrue(match_gpu_spec(dict(ANY_GPU, version=">=12.3"), [A100]))

    def test_driver_version(self):
        self.assertTrue(match_gpu_spec(dict(ANY_GPU, driver_version=">=575.0"), [A100]))
        self.assertFalse(match_gpu_spec(dict(ANY_GPU, driver_version=">=575.0"), [V100]))
        # all match
        self.assertFalse(match_gpu_spec(dict(ANY_GPU, driver_version=">=575.0"), [A100, V100]))

    def test_missing_attributes_in_gpus(self):
        bare = {"vendor": "NVIDIA", "model": "NVIDIA A100-SXM4-40GB"}
        self.assertTrue(match_gpu_spec({"vendor": "NVIDIA", "model": ".*A100.*"}, [bare]))
        # constraints on attributes which the GPU doesn't report are not satisfied
        self.assertFalse(match_gpu_spec(dict(ANY_GPU, vram=">=40960"), [bare]))
        self.assertFalse(match_gpu_spec(dict(ANY_GPU, version=">=12.0"), [bare]))
        self.assertFalse(match_gpu_spec(dict(ANY_GPU, driver_version=">=575.0"), [bare]))
        self.assertFalse(match_gpu_spec(dict(ANY_GPU, microarchitecture="Ampere"), [bare]))

    def test_combined_constraints(self):
        spec = {"vendor": "NVIDIA", "model": ".*A100.*", "vram": ">=40960", "microarchitecture": "Ampere", "version": ">=12.0", "driver_version": ">=575.0"}
        self.assertTrue(match_gpu_spec(spec, [A100]))
        self.assertFalse(match_gpu_spec(spec, [V100]))
        self.assertFalse(match_gpu_spec(spec, [A100, V100]))


if __name__ == "__main__":
    unittest.main()
