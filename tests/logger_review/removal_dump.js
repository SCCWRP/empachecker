// node tests/removal_dump.js <payload dir> <submission id> <case.json>
// Prints {seriesKey: [removed bool per point]} as the page computes it for the review state in case.json.
const fs = require('fs'), path = require('path');
const { boot, sleep } = require('./page_harness');
const [dir, sid, casePath] = process.argv.slice(2);
const kase = JSON.parse(fs.readFileSync(casePath, 'utf8'));
const payload = JSON.parse(fs.readFileSync(path.join(dir, 'payload_' + sid + '.json'), 'utf8'));
const routes = (method, url) => {
  if (url.startsWith('/logger-review/api/submissions?')) return { status: 200, json: { total: 1, items: [{ submissionId: sid, status: 'draft', statusLabel: 'Draft' }] } };
  return { status: 200, json: { submission: payload, review: { exists: true, status: 'draft', notes: '', removeFails: kase.removeFails, removedPeriods: kase.removedPeriods, keptPeriods: kase.keptPeriods, thresholds: kase.thresholds } } };
};
(async () => {
  const app = boot(routes); await sleep(50);
  const out = app.get(`(() => {
    const out = {};
    const entries = Object.entries(currentSubmission().series);
    for (const [id, s] of entries) {
      const spec = plotSpec([[id, s]]);
      out[id] = spec.traces[0].y.map((v, i) => v === null && s.x[i] !== null);
    }
    return out;
  })()`);
  console.log(JSON.stringify(out));
  process.exit(0);
})();
