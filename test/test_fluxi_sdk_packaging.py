from pathlib import Path
import os
import subprocess
import sys
import tempfile
import unittest


class TestFluxiSdkPackaging(unittest.TestCase):

    def test_fluxi_sdk_imports_from_installed_package(self):
        repo_root = Path(__file__).resolve().parents[1]
        package_dir = repo_root / "packages" / "fluxi-sdk"

        with tempfile.TemporaryDirectory(prefix="fluxi-sdk-packaging-") as temp_dir:
            venv_dir = Path(temp_dir) / "venv"
            subprocess.run(
                [sys.executable, "-m", "venv", str(venv_dir)],
                check=True,
                capture_output=True,
                text=True,
            )
            python_executable = venv_dir / (
                "Scripts" if os.name == "nt" else "bin"
            ) / ("python.exe" if os.name == "nt" else "python")

            subprocess.run(
                [str(python_executable), "-m", "pip", "install", str(package_dir)],
                check=True,
                capture_output=True,
                text=True,
            )
            completed = subprocess.run(
                [
                    str(python_executable),
                    "-c",
                    "import fluxi_sdk; print(fluxi_sdk.__name__); print(hasattr(fluxi_sdk, '__version__'))",
                ],
                check=True,
                capture_output=True,
                text=True,
            )

        output_lines = completed.stdout.strip().splitlines()
        self.assertEqual(output_lines[0], "fluxi_sdk")
        self.assertEqual(output_lines[1], "True")
