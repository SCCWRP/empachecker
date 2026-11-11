"""Checks that the shipped code sticks to what the checker image has (pandas 1.5, SQLAlchemy 1.4, no PyYAML, no pg8000)."""
import json
import re

import pytest

from conftest import ROOT

LIB = ROOT / "proj" / "logger_review_lib"


def test_no_newer_only_apis_or_dependencies():
    for path in list(LIB.glob("*.py")) + [ROOT / "proj" / "logger_review.py"]:
        text = path.read_text()
        assert "as_unit" not in text, path
        assert not re.search(r"^(import|from)\s+(yaml|pg8000)\b", text, re.M), path


def test_default_thresholds_json_matches_yaml_when_yaml_is_available():
    yaml = pytest.importorskip("yaml")
    src = ROOT.parent / "empa-logger-review" / "app" / "qc" / "thresholds_main.yaml"
    if not src.exists():
        pytest.skip("prototype not checked out")
    assert yaml.safe_load(src.read_text()) == json.loads((LIB / "thresholds_main.json").read_text())


def test_defaults_load_without_yaml():
    from proj.logger_review_lib import series as sr
    cfg = sr.load_defaults()
    assert "h2otemp" in cfg and cfg["h2otemp"]["range"]["fail_max"] == 30
