import shutil
import subprocess

import pytest

from conftest import ROOT

NODE = shutil.which("node")


@pytest.mark.skipif(NODE is None, reason="node not installed")
def test_page_against_mock_dom(payload_dir):
    res = subprocess.run([NODE, str(ROOT / "tests/logger_review/page_test.js"), str(payload_dir)], capture_output=True, text=True)
    assert res.returncode == 0, res.stdout + res.stderr
    assert "page tests passed" in res.stdout
