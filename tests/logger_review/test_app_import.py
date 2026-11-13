"""Imports the real app factory with a dummy config (what a restart does) and checks the routes exist.

Runs only when LOGGER_REVIEW_IMPORT_APP=1, in a throwaway copy of the repo that has a dummy
proj/config/config.json. The database URL is a dummy that is never connected to."""
import os
import subprocess
import sys

import pytest

from conftest import ROOT


@pytest.mark.skipif(os.environ.get("LOGGER_REVIEW_IMPORT_APP") != "1", reason="needs a dummy config.json")
def test_real_app_registers_routes_and_serves_sign_in():
    code = """
import proj
rules = {r.rule for r in proj.app.url_map.iter_rules()}
need = {'/logger-review', '/logger-review/api/sign-in', '/logger-review/api/logout',
        '/logger-review/api/submissions', '/logger-review/api/submissions/<sid>',
        '/logger-review/api/submissions/<sid>/review', '/logger-review/api/submissions/<sid>/decision'}
assert need <= rules, need - rules
c = proj.app.test_client()
page = c.get('/logger-review')
assert page.status_code == 200 and b'Sign In' in page.data, page.status_code
assert c.get('/logger-review/api/submissions').status_code == 401
assert c.get('/static/logger_review/plotly.min.js').status_code == 200
print('app import ok')
"""
    env = dict(os.environ, DB_CONNECTION_STRING="postgresql://nobody:nothing@127.0.0.1:1/none", FLASK_APP_SECRET_KEY="dummy")
    res = subprocess.run([sys.executable, "-c", code], cwd=str(ROOT), env=env, capture_output=True, text=True)
    assert res.returncode == 0 and "app import ok" in res.stdout, res.stdout[-800:] + res.stderr[-1500:]
