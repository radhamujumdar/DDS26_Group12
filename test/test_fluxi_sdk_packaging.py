import importlib
import unittest


class TestFluxiSdkPackaging(unittest.TestCase):

    def test_fluxi_sdk_imports(self):
        fluxi_sdk = importlib.import_module("fluxi_sdk")

        self.assertEqual(fluxi_sdk.__name__, "fluxi_sdk")
        self.assertTrue(hasattr(fluxi_sdk, "__version__"))
