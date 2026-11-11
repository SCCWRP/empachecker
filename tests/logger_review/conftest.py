import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
FIXTURES = ROOT / "tests" / "logger_review" / "fixtures"

# Importing the real "proj" package needs config.json and the database settings. The tests only need
# proj.logger_review_lib and proj.logger_review, so register an empty "proj" package that points at
# the folder. The app factory import is checked separately (see test_app_import.py).
if "proj" not in sys.modules:
    import types
    _proj = types.ModuleType("proj")
    _proj.__path__ = [str(ROOT / "proj")]
    sys.modules["proj"] = _proj
SUBMISSIONS = [1729879198, 1770672364, 1770678690]


@pytest.fixture(scope="session")
def fixture():
    return json.loads((FIXTURES / "submission_review_fixture.json").read_text())


@pytest.fixture(scope="session")
def python_flags():
    return json.loads((FIXTURES / "submission_review_python_flags.json").read_text())


@pytest.fixture(scope="session")
def rows_by_sid(fixture):
    out = {}
    for r in fixture["rows"]:
        out.setdefault(r["submissionid"], []).append(r)
    return out


@pytest.fixture(scope="session")
def payloads(fixture, rows_by_sid):
    from proj.logger_review_lib import series as sr
    cfg = sr.load_defaults()
    data, flags = {}, {}
    for sid, rows in rows_by_sid.items():
        data[sid], f = sr.build_submission(sid, rows, fixture["sites"], cfg, with_flags=True)
        flags.update(f)
    return data, flags


@pytest.fixture(scope="session")
def payload_dir(payloads, tmp_path_factory):
    import os
    d = Path(os.environ["LOGGER_REVIEW_PAYLOAD_DIR"]) if os.environ.get("LOGGER_REVIEW_PAYLOAD_DIR") else tmp_path_factory.mktemp("payloads")
    d.mkdir(parents=True, exist_ok=True)
    for sid, data in payloads[0].items():
        (d / f"payload_{sid}.json").write_text(json.dumps(data, allow_nan=False))
    return d
