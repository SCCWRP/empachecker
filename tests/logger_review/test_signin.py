import hashlib
import time

import pytest

from fakes import FakeStore
from proj import logger_review as lr
from test_api import A, B, make_app

URL = "/logger-review/api/"
PASSWORD = "test-password"   # not the real one, which is only on the server
SALT = "00112233445566778899aabbccddeeff"
HASH = "pbkdf2_sha256$1000$" + SALT + "$" + hashlib.pbkdf2_hmac("sha256", PASSWORD.encode(), bytes.fromhex(SALT), 1000).hex()


@pytest.fixture(autouse=True)
def clean_state():
    lr._failures.clear()
    yield


@pytest.fixture()
def client():
    return make_app(FakeStore([], {}), LOGGER_REVIEW_PASSWORD_HASH=HASH).test_client()


def sign_in(c, email, password):
    return c.post(URL + "sign-in", json={"email": email, "password": password})


def test_allowlist_constant():
    assert lr.ALLOWED_EMAILS == ("duyn@sccwrp.org", "janw@sccwrp.org")


def test_listed_email_with_password_signs_in(client):
    assert client.get(URL + "submissions").status_code == 401
    r = sign_in(client, "  DuyN@SCCWRP.org ", PASSWORD)
    assert r.status_code == 200 and r.get_json() == {"email": A}
    with client.session_transaction() as s:
        assert s["logger_review_email"] == A and PASSWORD not in str(dict(s))
    assert client.get(URL + "submissions").status_code == 200


def test_unlisted_email_is_refused_even_with_the_password(client):
    r = sign_in(client, "someone@example.org", PASSWORD)
    assert r.status_code == 403 and r.get_json()["error"]["code"] == "email_not_authorized"
    assert client.get(URL + "submissions").status_code == 401


def test_bad_email_is_rejected(client):
    assert sign_in(client, "not an email", PASSWORD).status_code == 400
    assert sign_in(client, "", PASSWORD).status_code == 400


@pytest.mark.parametrize("bad", ["", "wrong", PASSWORD.upper(), PASSWORD + " "])
def test_wrong_password_is_refused(client, bad):
    r = sign_in(client, A, bad)
    assert r.status_code == 401 and r.get_json()["error"]["code"] == "invalid_password"
    assert client.get(URL + "submissions").status_code == 401


def test_non_string_password_is_refused(client):
    assert client.post(URL + "sign-in", json={"email": A, "password": 123}).status_code == 401


def test_five_wrong_passwords_lock_that_email_for_15_minutes(client, monkeypatch):
    for _ in range(5):
        assert sign_in(client, A, "wrong").status_code == 401
    assert sign_in(client, A, PASSWORD).status_code == 429     # even the right password is locked out
    assert sign_in(client, B, PASSWORD).status_code == 200     # another email is not locked
    real = time.time
    monkeypatch.setattr(lr.time, "time", lambda: real() + 15 * 60 + 1)
    assert sign_in(client, A, PASSWORD).status_code == 200


def test_four_wrong_passwords_then_right_resets_the_count(client):
    for _ in range(4):
        sign_in(client, A, "wrong")
    assert sign_in(client, A, PASSWORD).status_code == 200
    for _ in range(4):
        assert sign_in(client, A, "wrong").status_code == 401  # count started over


def test_missing_hash_config_is_reported():
    c = make_app(FakeStore([], {})).test_client()
    r = sign_in(c, A, PASSWORD)
    assert r.status_code == 503 and r.get_json()["error"]["code"] == "password_not_configured"


def test_logout_ends_the_session(client):
    sign_in(client, A, PASSWORD)
    assert client.get(URL + "submissions").status_code == 200
    assert client.post(URL + "logout").status_code == 200
    assert client.get(URL + "submissions").status_code == 401


def test_page_shows_sign_in_then_review(client):
    page = client.get("/logger-review")
    html = page.get_data(as_text=True)
    assert page.status_code == 200 and "Sign In" in html and 'type="password"' in html and "Verify code" not in html
    sign_in(client, A, PASSWORD)
    page = client.get("/logger-review")
    html = page.get_data(as_text=True)
    assert page.status_code == 200 and "Sign out" in html and 'var LR_EMAIL = "duyn@sccwrp.org";' in html
    assert 'src="/static/logger_review/plotly.min.js"' in html and 'src="/static/logger_review/qc_tests.js"' in html
    assert client.get("/static/logger_review/plotly.min.js").status_code == 200
    assert client.get("/static/logger_review/qc_tests.js").status_code == 200


def test_sign_in_page_flow():
    import shutil
    import subprocess
    from conftest import ROOT
    node = shutil.which("node")
    if node is None:
        pytest.skip("node not installed")
    res = subprocess.run([node, str(ROOT / "tests/logger_review/signin_test.js")], capture_output=True, text=True)
    assert res.returncode == 0, res.stdout + res.stderr
