"""Flatline flags only the point that reaches the limit (QARTOD, Nicholas 2026-11-23)."""
import numpy as np

from proj.logger_review_lib import qc_harness as qh

P, S, F, NE = qh.PASS, qh.SUSPECT, qh.FAIL, qh.NOT_EVAL


def test_only_the_point_reaching_the_limit_is_flagged():
    # 1, then 7 repeats of 2, then 3. m=1: the 3rd repeat is suspect, the 5th and later fail.
    x = np.array([1, 2, 2, 2, 2, 2, 2, 2, 3], float)
    f = qh.flatline_test(x, 1, suspect_n=3, fail_n=5).tolist()
    #           1   2  2r1 2r2 2r3 2r4 2r5 2r6 3
    assert f == [NE, P, P,  P,  S,  S,  F,  F,  P]


def test_multiplier_scales_the_run_length():
    x = np.array([0] + [5] * 7, float)          # 6 repeats after the first 5
    f = qh.flatline_test(x, 2, suspect_n=3, fail_n=5).tolist()
    assert f.count(S) == 1 and f.count(F) == 0 and f[-1] == S
