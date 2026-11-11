// JavaScript port of the four tests in qc_harness.py (range, spike, ROC, flatline), used by the
// viewer to recompute flags when thresholds are edited. Keep in step with qc_harness.py and check
// with verify_viewer.js, which compares these flags with the Python flags on every series.
// Flag codes: 1 pass, 2 not evaluated, 3 suspect, 4 fail, 9 missing.
// t: seconds since the series start (UTC), x: values (NaN for missing), interval in minutes.
const QC = (() => {
  const PASS = 1, NE = 2, SUS = 3, FAIL = 4, MISS = 9;
  const has = v => v !== null && v !== undefined && !Number.isNaN(v);

  // numpy pairwise_sum for n <= 128 (8 accumulators), so std matches np.std bit for bit
  function npSum(a) {
    const n = a.length;
    if (n < 8) { let r = 0; for (let i = 0; i < n; i++) r += a[i]; return r; }
    const r = a.slice(0, 8);
    let i = 8;
    for (; i < n - (n % 8); i += 8) for (let j = 0; j < 8; j++) r[j] += a[i + j];
    let res = ((r[0] + r[1]) + (r[2] + r[3])) + ((r[4] + r[5]) + (r[6] + r[7]));
    for (; i < n; i++) res += a[i];
    return res;
  }
  // MATLAB std(a,'omitnan'), N-1; one value -> 0, none -> NaN
  function stdOmitNaN(w) {
    const a = w.filter(v => !Number.isNaN(v));
    if (a.length === 0) return NaN;
    if (a.length === 1) return 0;
    const m = npSum(a) / a.length;
    return Math.sqrt(npSum(a.map(v => (v - m) * (v - m))) / (a.length - 1));
  }

  function range(x, r) {
    const smin = has(r.suspect_min) ? r.suspect_min : -Infinity, smax = has(r.suspect_max) ? r.suspect_max : Infinity;
    const fmin = has(r.fail_min) ? r.fail_min : -Infinity, fmax = has(r.fail_max) ? r.fail_max : Infinity;
    const f = new Uint8Array(x.length);
    for (let i = 0; i < x.length; i++) {
      const v = x[i];
      f[i] = (v >= fmax || v <= fmin) ? FAIL : (v >= smax || v <= smin) ? SUS : Number.isNaN(v) ? MISS : PASS;
    }
    return f;
  }

  function spike(x, sus, fail, t, gap) {
    const n = x.length, f = new Uint8Array(n).fill(NE);
    for (let i = 1; i < n - 1; i++) {
      const nb = [x[i - 1], x[i + 1]].filter(v => !Number.isNaN(v));
      const ref = nb.length ? npSum(nb) / nb.length : NaN;
      const dev = Math.abs(x[i] - ref);
      if (t[i] - t[i - 1] > gap || t[i + 1] - t[i] > gap) f[i] = PASS;
      else if (dev >= fail) f[i] = FAIL;
      else if (dev >= sus) f[i] = SUS;
      else if (Number.isNaN(x[i])) f[i] = MISS;
      else f[i] = PASS;
    }
    return f;
  }

  // window is the nsteps points before i (current point excluded), needs 5 valid values
  function roc(x0, nsteps, sus, fail, t, gap, minStep) {
    const x = Float64Array.from(x0), n = x.length, f = new Uint8Array(n).fill(NE);
    for (let i = 1; i < n; i++) {
      const w = Array.from(x.subarray(Math.max(0, i - nsteps), i));
      if (w.filter(v => !Number.isNaN(v)).length < 5) continue;
      const sd = stdOmitNaN(w), step = Math.abs(x[i] - x[i - 1]);
      if (t[i] - t[i - 1] > gap) f[i] = PASS;
      else if (step > sus * sd && step < fail * sd && step > minStep) f[i] = SUS;
      else if (step > fail * sd && step > minStep) { f[i] = FAIL; x[i] = NaN; }
      else if (Number.isNaN(x[i])) f[i] = MISS;
      else f[i] = PASS;
    }
    return f;
  }

  function flatline(x, m, sn, fn, tol) {
    const n = x.length, f = new Uint8Array(n).fill(NE);
    let count = 0;
    for (let i = 1; i < n; i++) {
      const same = tol <= 0 ? x[i] === x[i - 1] : Math.abs(x[i] - x[i - 1]) <= tol;
      count = same ? count + 1 : 0;
      if (count >= m * sn && count < m * fn) f.fill(SUS, Math.max(0, i - 2), i + 1);
      else if (count >= m * fn) f.fill(FAIL, Math.max(0, i - 4), i + 1);
      else if (Number.isNaN(x[i])) f[i] = MISS;
      else f[i] = PASS;
    }
    return f;
  }

  // s: {t, x, interval, nsteps, mult}; th: {range:{...}, spike:{...}, roc:{...}, flatline:{...}}
  function run(s, th) {
    const n = s.x.length, gap = 2 * s.interval * 60, ne = () => new Uint8Array(n).fill(NE);
    const r = th.range || {}, sp = th.spike || {}, o = th.roc || {}, fl = th.flatline || {};
    return {
      range: ['fail_min', 'suspect_min', 'suspect_max', 'fail_max'].some(k => has(r[k])) ? range(s.x, r) : ne(),
      spike: has(sp.suspect) && has(sp.fail) && n >= 3 ? spike(s.x, sp.suspect, sp.fail, s.t, gap) : ne(),
      roc: has(o.suspect_sd) && has(o.fail_sd) && n >= 2
        ? roc(s.x, s.nsteps, o.suspect_sd, o.fail_sd, s.t, gap, has(o.min_step) ? o.min_step : 0) : ne(),
      flatline: has(fl.suspect_n) && has(fl.fail_n) && n >= 2
        ? flatline(s.x, s.mult, fl.suspect_n, fl.fail_n, has(fl.tol) ? fl.tol : 0) : ne(),
    };
  }

  // per-point worst flag: fail > suspect > missing > pass > not evaluated
  const RANK = {2: 0, 1: 1, 9: 2, 3: 3, 4: 4}, BACK = [2, 1, 9, 3, 4];
  function worst(fl) {
    const n = fl.range.length, out = new Uint8Array(n);
    for (let i = 0; i < n; i++) {
      let r = 0;
      for (const k of ['range', 'spike', 'roc', 'flatline']) r = Math.max(r, RANK[fl[k][i]]);
      out[i] = BACK[r];
    }
    return out;
  }
  return { run, worst };
})();
if (typeof module !== 'undefined') module.exports = QC;
