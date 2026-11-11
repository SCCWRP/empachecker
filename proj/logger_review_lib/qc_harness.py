"""Logger QC harness: Nicholas Trautman's four QARTOD tests (range, spike, ROC, flatline)
with calibration options. Ported from his MATLAB (SCCWRP QC demo.zip, 2026-09-21); the test
functions are copied from calemp-logger-analysis/qaqc_test/python/ (branch qaqc-test) and keep
his MATLAB line references. MATLAB 1-based indices are written "(i)" and converted to 0-based.

Flag codes: 1 pass, 2 not evaluated, 3 suspect, 4 fail, 9 missing (NaN).

Default options reproduce his code exactly (ROC window includes the current point, no minimum
step, exact-repeat flatline). The calibration options are OFF or at his behaviour by default:
  roc_include_current  True (his) or False (the 9/8 meeting direction)
  roc_min_step         0 (his) or an absolute step floor in the parameter's unit
  flatline_tol         0 (his, exact repeat) or a tolerance: a repeat is |x[i]-x[i-1]| <= tol
  interval scaling     multiplier = base_interval / sampling interval (15 min base)
"""
import json
import math
import re
from dataclasses import dataclass, field, replace
from pathlib import Path

import numpy as np
import pandas as pd

PASS, NOT_EVAL, SUSPECT, FAIL, MISSING = 1, 2, 3, 4, 9
TESTS = ('range', 'spike', 'roc', 'flatline')
HERE = Path(__file__).resolve().parent
DEFAULT_CONFIG = HERE / 'thresholds.yaml'


# ---------------------------------------------------------------- his test functions

def _std_omitnan(a):
    """MATLAB std(a,'omitnan'): sample std (N-1). One value -> 0, none -> NaN."""
    a = a[~np.isnan(a)]
    if a.size == 0:
        return np.nan
    if a.size == 1:
        return 0.0
    return float(np.std(a, ddof=1))


def range_test(x, suspect_min, suspect_max, fail_min, fail_max):
    """range_test.m lines 19-33. Fail bounds inclusive (>= max, <= min), checked first, then
    suspect (inclusive), then NaN -> 9, else 1. A bound of None/NaN is not applied."""
    x = np.asarray(x, float)
    smin = -np.inf if suspect_min is None else suspect_min
    smax = np.inf if suspect_max is None else suspect_max
    fmin = -np.inf if fail_min is None else fail_min
    fmax = np.inf if fail_max is None else fail_max
    flag = np.full(len(x), NOT_EVAL, dtype=np.uint8)
    for i in range(len(x)):
        v = x[i]
        if v >= fmax or v <= fmin:
            flag[i] = FAIL
        elif v >= smax or v <= smin:
            flag[i] = SUSPECT
        elif np.isnan(v):
            flag[i] = MISSING
        else:
            flag[i] = PASS
    return flag


def spike_test(x, suspect_threshold, fail_threshold, times, sampling_period):
    """spike_test.m lines 24-42. Compares x(i) with mean of x(i-1), x(i+1) (omitnan). Loop
    i = 2..n-1, so first and last points stay 2. A gap > 2*sampling_period on either side
    gives 1. Thresholds inclusive (>=). Uses the original x."""
    x = np.asarray(x, float)
    n = len(x)
    d = np.diff(times.values)  # d[k] = t[k+1]-t[k]  (MATLAB d(k+1))
    sp = np.timedelta64(int(sampling_period.total_seconds() * 1e9), 'ns')
    flag = np.full(n, NOT_EVAL, dtype=np.uint8)
    for i in range(1, n - 1):  # MATLAB i = 2:length(x)-1
        nb = np.array([x[i - 1], x[i + 1]])
        nb = nb[~np.isnan(nb)]
        ref = nb.mean() if nb.size else np.nan  # mean(...,'omitnan')
        if d[i - 1] > 2 * sp or d[i] > 2 * sp:  # d(i-1) and d(i) in 1-based
            flag[i] = PASS
        elif abs(x[i] - ref) >= fail_threshold:
            flag[i] = FAIL
        elif abs(x[i] - ref) >= suspect_threshold:
            flag[i] = SUSPECT
        elif np.isnan(x[i]):
            flag[i] = MISSING
        else:
            flag[i] = PASS
    return flag


def roc_test(x, nsteps, suspect_dev, fail_dev, times, sampling_period,
             include_current=True, min_step=0.0, min_window_points=5):
    """ROC_test.m lines 24-67. For i = 2..n: SD of a window; his window x(i-nsteps:i)
    INCLUDES the current point (nsteps+1 points) for i > nsteps, and x(1:nsteps) for
    i <= nsteps. x is a working copy: a failed point is set to NaN so it drops out of later
    SDs and the next point's step is NaN (passes). Strict inequalities: a step exactly equal
    to threshold*SD falls through to pass. First point stays 2. Gap > 2*sampling_period -> 1.

    Added options (defaults = his behaviour):
      include_current=False: window is the nsteps points BEFORE i, x(i-nsteps:i-1). Needs at
        least min_window_points non-NaN values, else the point is 2 (not evaluated).
      min_step: a point is only suspect/fail if its step also exceeds this floor (> min_step)."""
    x = np.array(x, float)  # copy, modified in place like MATLAB
    n = len(x)
    d = np.diff(times.values)
    sp = np.timedelta64(int(sampling_period.total_seconds() * 1e9), 'ns')
    flag = np.full(n, NOT_EVAL, dtype=np.uint8)
    for i0 in range(1, n):  # 0-based; MATLAB i = i0+1
        i = i0 + 1
        if include_current:
            if i <= nsteps:
                w = x[0:nsteps]  # x(1:nsteps)
            else:
                w = x[i0 - nsteps:i0 + 1]  # x(i-nsteps:i), includes x(i)
        else:
            w = x[max(0, i0 - nsteps):i0]  # x(i-nsteps:i-1), excludes x(i)
            if np.count_nonzero(~np.isnan(w)) < min_window_points:
                continue  # stays 2
        sd = _std_omitnan(w)
        step = abs(x[i0] - x[i0 - 1])
        if d[i0 - 1] > 2 * sp:
            flag[i0] = PASS
        elif step > suspect_dev * sd and step < fail_dev * sd and step > min_step:
            flag[i0] = SUSPECT
        elif step > fail_dev * sd and step > min_step:
            flag[i0] = FAIL
            x[i0] = np.nan
        elif np.isnan(x[i0]):
            flag[i0] = MISSING
        else:
            flag[i0] = PASS
    return flag


def flatline_test(x, multiplier, suspect_n=3, fail_n=5, tol=0.0):
    """flatline_test.m lines 18-43. count = run length of repeats of the previous value (NaN
    never equals NaN). count >= suspect_n*m and < fail_n*m marks ONLY the last 3 points
    suspect (flag(i-2:i)); count >= fail_n*m marks the last 5 fail (flag(i-4:i)). Earlier
    points of a long run keep the 1 they got earlier. First point stays 2. His code has
    suspect_n=3, fail_n=5 hardcoded. With his demo m=3 the limits are 9 and 15 repeats.
    Added: tol > 0 counts |x[i]-x[i-1]| <= tol as a repeat (tol=0 is exact equality)."""
    x = np.asarray(x, float)
    m = multiplier
    flag = np.full(len(x), NOT_EVAL, dtype=np.uint8)
    count = 0
    for i in range(1, len(x)):
        same = (x[i] == x[i - 1]) if tol <= 0 else (abs(x[i] - x[i - 1]) <= tol)
        count = count + 1 if same else 0
        if count >= m * suspect_n and count < m * fail_n:
            flag[max(0, i - 2):i + 1] = SUSPECT
        elif count >= m * fail_n:
            flag[max(0, i - 4):i + 1] = FAIL
        elif np.isnan(x[i]):
            flag[i] = MISSING
        else:
            flag[i] = PASS
    return flag


# ---------------------------------------------------------------- config and options

@dataclass
class Options:
    """Global switches. Defaults = Nicholas exact."""
    roc_include_current: bool = True
    roc_min_step: float | None = None      # None: use the config value (default 0)
    flatline_tol: float | None = None      # None: use the config value (default 0)
    interval_scaling: bool = True          # multiplier = base / interval, nsteps scaled the same way
    base_interval_minutes: float = 15.0
    roc_nsteps_base: int = 10              # points at the base interval (10 -> 30 at 5 min)
    min_multiplier: float = 1.0            # floor on the flatline multiplier for slow loggers
    min_roc_nsteps: int = 5


def _norm(name):
    return re.sub(r'[^a-z0-9]', '', str(name).lower())


def load_config(path=DEFAULT_CONFIG):
    """Return {'parameters': {...}, 'alias': {normalized alias -> canonical name}}."""
    text = Path(path).read_text()
    if str(path).endswith('.json'):
        cfg = json.loads(text)
    else:
        import yaml  # not installed in the checker image, which ships the thresholds as JSON
        cfg = yaml.safe_load(text)
    alias = {}
    for name, p in cfg['parameters'].items():
        alias[_norm(name)] = name
    for name, p in cfg['parameters'].items():
        for a in p.get('aliases') or []:
            alias.setdefault(_norm(a), name)   # a canonical name always wins (do% used to shadow do)
    cfg['alias'] = alias
    return cfg


def canonical(cfg, parameter):
    return cfg['alias'].get(_norm(parameter))


def param_config(cfg, parameter):
    """Threshold block for a parameter, or None if the parameter is unknown."""
    c = canonical(cfg, parameter)
    return None if c is None else cfg['parameters'][c]


def infer_interval(times, fallback=15.0):
    """Sampling interval in minutes: median consecutive difference, rounded to 0.1 min."""
    d = np.diff(pd.Series(times).values).astype('timedelta64[s]').astype(float)
    d = d[d > 0]
    return fallback if d.size == 0 else round(float(np.median(d)) / 60.0, 1)


def effective_params(opts, interval_min, pc):
    """Resolve nsteps, multiplier, floors and tol for this parameter and interval."""
    if opts.interval_scaling:
        mult = max(opts.min_multiplier, opts.base_interval_minutes / interval_min)
        nsteps = max(opts.min_roc_nsteps, int(round(opts.roc_nsteps_base * opts.base_interval_minutes / interval_min)))
    else:
        mult, nsteps = 1.0, opts.roc_nsteps_base
    roc = pc.get('roc') or {}
    fl = pc.get('flatline') or {}
    if roc.get('nsteps'):
        nsteps = int(roc['nsteps'])          # explicit per-parameter override
    if fl.get('multiplier'):
        mult = float(fl['multiplier'])
    min_step = opts.roc_min_step if opts.roc_min_step is not None else (roc.get('min_step') or 0.0)
    tol = opts.flatline_tol if opts.flatline_tol is not None else (fl.get('tol') or 0.0)
    return dict(nsteps=nsteps, multiplier=mult, min_step=float(min_step), tol=float(tol))


def _have(block, *keys):
    return block is not None and all(block.get(k) is not None for k in keys)


def run_tests(times, values, pc, opts=None, interval_min=None):
    """Run the four tests on one series.

    times: tz-aware or naive pandas datetime Series/Index, sorted. values: floats (NaN ok).
    pc: threshold block for the parameter (a dict as in thresholds.yaml). A test whose
    thresholds are missing returns all 2 (not evaluated).
    Returns dict test -> uint8 flag array, plus 'interval_min' and 'effective' info under '_info'."""
    opts = opts or Options()
    times = pd.Series(pd.to_datetime(times).values if not isinstance(times, pd.Series) else times.values)
    x = np.asarray(values, float)
    n = len(x)
    if interval_min is None:
        interval_min = infer_interval(times, opts.base_interval_minutes)
    sp = pd.Timedelta(minutes=interval_min)
    eff = effective_params(opts, interval_min, pc)
    out = {t: np.full(n, NOT_EVAL, dtype=np.uint8) for t in TESTS}
    r, s, o, f = pc.get('range'), pc.get('spike'), pc.get('roc'), pc.get('flatline')
    if r and any(r.get(k) is not None for k in ('fail_min', 'suspect_min', 'suspect_max', 'fail_max')):
        out['range'] = range_test(x, r.get('suspect_min'), r.get('suspect_max'), r.get('fail_min'), r.get('fail_max'))
    if _have(s, 'suspect', 'fail') and n >= 3:
        out['spike'] = spike_test(x, s['suspect'], s['fail'], times, sp)
    if _have(o, 'suspect_sd', 'fail_sd') and n >= 2:
        out['roc'] = roc_test(x, eff['nsteps'], o['suspect_sd'], o['fail_sd'], times, sp,
                              include_current=opts.roc_include_current, min_step=eff['min_step'])
    if f and f.get('suspect_n') is not None and n >= 2:
        out['flatline'] = flatline_test(x, eff['multiplier'], f['suspect_n'], f['fail_n'], eff['tol'])
    out['_info'] = dict(interval_min=interval_min, **eff)
    return out


def worst_flag(flags):
    """Per-point worst of the four tests: 4 fail > 3 suspect > 1 pass > 9 missing > 2 not evaluated."""
    rank = {PASS: 1, NOT_EVAL: 0, MISSING: 2, SUSPECT: 3, FAIL: 4}
    inv = {v: k for k, v in rank.items()}
    r = np.zeros(len(next(iter(v for k, v in flags.items() if k != '_info'))), dtype=int)
    for t in TESTS:
        r = np.maximum(r, np.vectorize(rank.get)(flags[t]))
    return np.vectorize(inv.get)(r).astype(np.uint8)


# ---------------------------------------------------------------- distribution statistics

def _runs(mask):
    """Longest run of True."""
    best = cur = 0
    for v in mask:
        cur = cur + 1 if v else 0
        best = max(best, cur)
    return best


def _pct(a, q):
    a = np.asarray(a, float)
    a = a[~np.isnan(a)]
    return float(np.percentile(a, q)) if a.size else np.nan


def distribution_stats(times, values, interval_min, nsteps, flat_tols=(0.0,)):
    """Robust summaries that suggest thresholds. Steps across gaps (> 2 intervals) are excluded.
    Returns a flat dict. Percentiles use the data in this window only."""
    t = pd.Series(pd.to_datetime(times).values)
    x = pd.Series(np.asarray(values, float))
    gap = (t.diff() > pd.Timedelta(minutes=2 * interval_min)).to_numpy()
    step = x.diff()
    step[gap] = np.nan
    ast = step.abs()
    nb = (x.shift(1) + x.shift(-1)) / 2
    dev = (x - nb).abs()
    dev[gap | np.roll(gap, -1)] = np.nan
    sd_ex = x.shift(1).rolling(nsteps, min_periods=5).std()
    sd_in = x.rolling(nsteps + 1, min_periods=5).std()
    r_ex = ast / sd_ex.replace(0, np.nan)
    r_in = ast / sd_in.replace(0, np.nan)
    d = dict(n=len(x), n_valid=int(x.notna().sum()), interval_min=interval_min, nsteps=nsteps,
             value_min=_pct(x, 0), value_p0_1=_pct(x, 0.1), value_p1=_pct(x, 1), value_p50=_pct(x, 50),
             value_p99=_pct(x, 99), value_p99_9=_pct(x, 99.9), value_max=_pct(x, 100),
             step_signed_p0_1=_pct(step, 0.1), step_signed_p99_9=_pct(step, 99.9),
             absstep_p50=_pct(ast, 50), absstep_p99=_pct(ast, 99), absstep_p99_9=_pct(ast, 99.9), absstep_max=_pct(ast, 100),
             spikedev_p99=_pct(dev, 99), spikedev_p99_9=_pct(dev, 99.9), spikedev_max=_pct(dev, 100),
             roc_ratio_excl_p99=_pct(r_ex, 99), roc_ratio_excl_p99_9=_pct(r_ex, 99.9), roc_ratio_excl_max=_pct(r_ex, 100),
             roc_ratio_incl_p99=_pct(r_in, 99), roc_ratio_incl_p99_9=_pct(r_in, 99.9), roc_ratio_incl_max=_pct(r_in, 100),
             frac_steps_zero=float((ast == 0).sum() / max(1, ast.notna().sum())))
    xv = x.to_numpy()
    for tol in flat_tols:
        same = np.zeros(len(xv), bool)
        same[1:] = (xv[1:] == xv[:-1]) if tol <= 0 else (np.abs(xv[1:] - xv[:-1]) <= tol)
        L = _runs(same)  # repeats, so the run has L+1 points
        key = 'flat_longest_repeats' if tol <= 0 else f'flat_longest_repeats_tol{tol:g}'
        d[key] = L
        d[key + '_hours'] = round(L * interval_min / 60.0, 2)
    return d


# ---------------------------------------------------------------- window CSV input

CORE_COLS = ('timestamp_utc', 'parameter', 'value')


def load_window_csv(path):
    """Read a window CSV (long format). Required: timestamp_utc, parameter, value. Any other
    column (estuary, site, station, sensor, sensorid, estuary_type, region, season, window_id,
    interval_minutes, ...) is metadata carried into the outputs. Sorted by time."""
    df = pd.read_csv(path)
    df.columns = [c.strip().lower() for c in df.columns]
    miss = [c for c in CORE_COLS if c not in df.columns]
    if miss:
        raise ValueError(f'{path}: missing columns {miss}; need {CORE_COLS}')
    df['timestamp_utc'] = pd.to_datetime(df['timestamp_utc'], utc=True)
    df['value'] = pd.to_numeric(df['value'], errors='coerce')
    meta = [c for c in df.columns if c not in CORE_COLS]
    return df.sort_values('timestamp_utc', kind='stable').reset_index(drop=True), meta


def iter_series(df, meta):
    """Yield (group_meta dict, parameter, sub-DataFrame) per window x parameter.
    The window is window_id if that column exists, else the unique combination of the
    metadata columns excluding interval_minutes."""
    keys = ['window_id'] if 'window_id' in df.columns else [c for c in meta if c != 'interval_minutes']
    keys = keys + ['parameter']
    for k, g in df.groupby(keys, dropna=False, sort=True):
        g = g.drop_duplicates('timestamp_utc', keep='first')
        md = {c: g[c].iloc[0] for c in meta if c in g.columns}
        if 'window_id' not in md:
            md['window_id'] = '|'.join(str(g[c].iloc[0]) for c in keys[:-1]) or 'window'
        yield md, g['parameter'].iloc[0], g.reset_index(drop=True)


def series_interval(g, opts):
    """interval_minutes column if given, else inferred from the timestamps."""
    if 'interval_minutes' in g.columns and g['interval_minutes'].notna().any():
        return float(g['interval_minutes'].dropna().iloc[0])
    return infer_interval(g['timestamp_utc'], opts.base_interval_minutes)
