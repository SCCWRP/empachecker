import re
import time

import pytest

from fakes import FakeStore
from proj import logger_review as lr
from test_api import A, B, make_app

SENT = "If this email is allowed, a code was sent."
URL = "/logger-review/api/"


@pytest.fixture(autouse=True)
def clean_state():
    for d in (lr._codes, lr._latest, lr._last_request):
        d.clear()
    yield


@pytest.fixture()
def mails(monkeypatch):
    sent = []
    monkeypatch.setattr(lr, "send_mail", lambda frm, to, subject, text, **kw: sent.append((frm, to, subject, text, kw)))
    return sent


@pytest.fixture()
def client():
    return make_app(FakeStore([], {})).test_client()


def code_of(mails):
    return re.search(r"\b(\d{6})\b", mails[-1][3]).group(1)


def ask(c, email):
    return c.post(URL + "request-code", json={"email": email})


def verify(c, email, code):
    return c.post(URL + "verify-code", json={"email": email, "code": code})


def test_allowlist_constant():
    assert lr.ALLOWED_EMAILS == ("duyn@sccwrp.org", "janw@sccwrp.org")


def test_listed_email_gets_code_and_signs_in(client, mails):
    r = ask(client, "  DuyN@SCCWRP.org ")
    assert r.status_code == 200 and r.get_json() == {"message": SENT}
    frm, to, subject, text, kw = mails[0]
    assert to == [A] and subject == "EMPA logger review sign-in code" and "10 minutes" in text
    assert frm == "checker@example.org" and kw["server"] == "localhost"
    assert client.get(URL + "submissions").status_code == 401
    ok = verify(client, A, code_of(mails))
    assert ok.status_code == 200 and ok.get_json() == {"email": A}
    with client.session_transaction() as s:
        assert s["logger_review_email"] == A and "logger_review_code" not in s
    assert client.get(URL + "submissions").status_code == 200


def test_unlisted_email_same_response_and_nothing_sent(client, mails):
    listed = ask(make_app(FakeStore([], {})).test_client(), B)
    n = len(mails)
    unlisted = ask(client, "someone@example.org")
    assert (unlisted.status_code, unlisted.get_json()) == (listed.status_code, listed.get_json())
    assert len(mails) == n          # nothing sent for the unlisted address
    assert verify(client, "someone@example.org", "123456").status_code == 401
    assert not lr._codes or all(v["email"] != "someone@example.org" for v in lr._codes.values())


def test_bad_email_is_rejected(client, mails):
    assert ask(client, "not an email").status_code == 400
    assert ask(client, "").status_code == 400 and mails == []


def test_code_is_six_digits_and_stored_hashed(client, mails):
    ask(client, A)
    code = code_of(mails)
    assert re.fullmatch(r"\d{6}", code)
    with client.session_transaction() as s:
        pending = s["logger_review_code"]
    assert code not in str(pending) and len(pending["hash"]) == 64 and pending["email"] == A
    assert pending["expires"] - time.time() == pytest.approx(600, abs=5)


def test_wrong_code_then_right_code(client, mails):
    ask(client, A)
    code = code_of(mails)
    wrong = "000000" if code != "000000" else "111111"
    assert verify(client, A, wrong).status_code == 401
    assert verify(client, A, code).status_code == 200


def test_code_expires_after_10_minutes(client, mails, monkeypatch):
    ask(client, A)
    code = code_of(mails)
    real = time.time
    monkeypatch.setattr(lr.time, "time", lambda: real() + 601)
    assert verify(client, A, code).status_code == 401
    monkeypatch.setattr(lr.time, "time", real)
    assert verify(client, A, code).status_code == 401      # an expired code stays void


def test_code_is_single_use_even_with_a_copied_cookie(client, mails):
    ask(client, A)
    code = code_of(mails)
    with client.session_transaction() as s:
        old = dict(s["logger_review_code"])
    assert verify(client, A, code).status_code == 200
    client.post(URL + "logout")
    with client.session_transaction() as s:
        s["logger_review_code"] = old                     # replay the cookie from before sign-in
    assert verify(client, A, code).status_code == 401


def test_five_wrong_tries_void_the_code(client, mails):
    ask(client, A)
    code = code_of(mails)
    wrong = "000000" if code != "000000" else "111111"
    with client.session_transaction() as s:
        old = dict(s["logger_review_code"])
    for _ in range(5):
        assert verify(client, A, wrong).status_code == 401
    assert verify(client, A, code).status_code == 401      # even the right code no longer works
    with client.session_transaction() as s:
        s["logger_review_code"] = old                     # a replayed cookie does not reset the count
    assert verify(client, A, code).status_code == 401


def test_four_wrong_tries_still_allow_the_right_code(client, mails):
    ask(client, A)
    code = code_of(mails)
    wrong = "000000" if code != "000000" else "111111"
    for _ in range(4):
        verify(client, A, wrong)
    assert verify(client, A, code).status_code == 200


def test_one_code_per_email_per_minute(client, mails, monkeypatch):
    assert ask(client, A).status_code == 200
    r = ask(client, A)
    assert r.status_code == 429 and len(mails) == 1
    assert ask(client, B).status_code == 200              # another email is not limited
    # the same limit applies to unlisted emails, so the limit does not reveal the list
    assert ask(client, "x@example.org").status_code == 200 and ask(client, "x@example.org").status_code == 429
    real = time.time
    monkeypatch.setattr(lr.time, "time", lambda: real() + 61)
    assert ask(client, A).status_code == 200 and len(mails) == 3


def test_new_code_voids_the_previous_one(client, mails, monkeypatch):
    ask(client, A)
    first = code_of(mails)
    real = time.time
    monkeypatch.setattr(lr.time, "time", lambda: real() + 61)
    ask(client, A)
    second = code_of(mails)
    if first != second:
        assert verify(client, A, first).status_code == 401
    assert verify(client, A, second).status_code == 200


def test_code_for_another_email_does_not_work(client, mails):
    ask(client, A)
    assert verify(client, B, code_of(mails)).status_code == 401


def test_mail_failure_is_reported(client, monkeypatch):
    def boom(*a, **k):
        raise OSError("smtp down")
    monkeypatch.setattr(lr, "send_mail", boom)
    r = ask(client, A)
    assert r.status_code == 500 and "smtp" not in r.get_data(as_text=True)


def test_logout_ends_the_session(client, mails):
    ask(client, A)
    verify(client, A, code_of(mails))
    assert client.get(URL + "submissions").status_code == 200
    assert client.post(URL + "logout").status_code == 200
    assert client.get(URL + "submissions").status_code == 401


def test_page_shows_sign_in_then_review(client, mails):
    page = client.get("/logger-review")
    assert page.status_code == 200 and b"Send code" in page.data and b"Sign in" in page.data
    ask(client, A)
    verify(client, A, code_of(mails))
    page = client.get("/logger-review")
    html = page.get_data(as_text=True)
    assert page.status_code == 200 and "Sign out" in html and 'var LR_EMAIL = "duyn@sccwrp.org";' in html
    assert 'src="/static/logger_review/plotly.min.js"' in html and 'src="/static/logger_review/qc_tests.js"' in html
    assert client.get("/static/logger_review/plotly.min.js").status_code == 200
    assert client.get("/static/logger_review/qc_tests.js").status_code == 200
