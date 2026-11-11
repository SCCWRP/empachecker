// node tests/page_test.js <dir with payload_<id>.json files>
const fs = require('fs'), path = require('path');
const { boot, sleep, html, assert } = require('./page_harness');
const dir = process.argv[2];
const payload = id => JSON.parse(fs.readFileSync(path.join(dir, 'payload_' + id + '.json'), 'utf8'));
const IDS = ['1770678690', '1770672364', '1729879198'];
const emptyReview = { exists: false, status: 'draft', notes: '', removeFails: false, removedPeriods: [], keptPeriods: [], thresholds: null };

function makeRoutes(state) {
  return (method, url, body) => {
    if (method === 'GET' && url.startsWith('/logger-review/api/submissions?')) {
      return { status: 200, json: { total: 3, items: IDS.map(id => ({ submissionId: id, status: state.statuses?.[id] || 'not_reviewed', statusLabel: state.labels?.[id] || 'Not reviewed' })) } };
    }
    const m = url.match(/^\/logger-review\/api\/submissions\/(\d+)(\/review|\/decision)?$/);
    if (method === 'GET' && m) {
      if (state.getError) return { status: state.getError, json: { error: { code: 'x', message: 'Submission is too large.' } } };
      return { status: 200, json: { submission: payload(m[1]), review: state.review || emptyReview } };
    }
    if (method === 'PUT') {
      if (state.putError) return { status: 500, json: { error: { message: 'Database error.' } } };
      state.review = { ...body, exists: true, status: 'draft', updatedDate: 'now', decidedAt: null };
      return { status: 200, json: { review: state.review } };
    }
    if (method === 'POST') {
      if (state.postError) return state.postError;
      state.review = { ...state.review, status: body.decision, decidedAt: new Date('2026-11-11T20:00:00Z').toISOString() };
      return { status: 200, json: { review: state.review, finalRows: 1 } };
    }
    return { status: 404, json: { error: { message: 'no route' } } };
  };
}
const puts = app => app.calls.filter(c => c.method === 'PUT');
const posts = app => app.calls.filter(c => c.method === 'POST');
let passed = 0;
const ok = (name) => { passed++; console.log('ok', name); };

(async () => {
  assert(!/localStorage/.test(html) && !/locally/i.test(html), 'no localStorage or "locally" in page');
  assert(!/(–|—)/.test(html.replace(/<style>[\s\S]*?<\/style>/, '')), 'no dashes as punctuation');
  assert(html.includes('/plotly.min.js') && !html.includes('cdn.jsdelivr') && !html.includes('zoom-reset'));
  ok('page has no localStorage, locally, CDN plotly');

  // load list and first submission
  let state = { statuses: { '1770672364': 'finalized' }, labels: { '1770672364': 'Finalized' } };
  let app = boot(makeRoutes(state)); await sleep(50);
  assert.deepEqual(app.ids.submission.options.map(o => o.text), ['1770678690 (Not reviewed)', '1770672364 (Finalized)', '1729879198 (Not reviewed)']);
  assert.equal(app.get("$('submission').value"), '1770678690');
  assert(app.get('Object.keys(D.submissions)').includes('1770678690'));
  assert.equal(app.ids['review-message'].textContent, '');
  assert.equal(app.ids.finalize.disabled, false);
  ok('submission list with status, payload loaded from API');

  // notes debounce, then thresholds in the body
  app.ids.notes.value = 'a'; app.ids.notes.oninput(); app.ids.notes.value = 'ab'; app.ids.notes.oninput();
  assert.equal(puts(app).length, 0);
  await sleep(750);
  assert.equal(puts(app).length, 1);
  const body = puts(app)[0].body;
  assert.equal(body.notes, 'ab');
  const keys = app.get("Object.keys(D.submissions['1770678690'].series)");
  assert.deepEqual(Object.keys(body.thresholds).sort(), [...keys].sort());
  assert.equal(app.ids['review-message'].textContent, 'Draft saved.');
  assert.equal(app.ids.submission.options[0].text, '1770678690 (Draft)');
  ok('notes save is debounced, body carries thresholds, status message');

  // immediate save on remove fails
  app.ids['remove-fails'].checked = true; app.ids['remove-fails'].onchange(); await sleep(20);
  assert.equal(puts(app).length, 2); assert.equal(puts(app)[1].body.removeFails, true);
  // period via the buttons: start picking, simulate the drag by setting the selection, confirm
  app.ids['remove-period'].onclick();
  app.get("const _ps=periodState('remove'); _ps.start='2024-04-01T00:00:00'; _ps.end='2024-04-01T01:00:00'; renderRemoval();");
  app.ids['period-confirm'].onclick(); await sleep(20);
  assert.equal(puts(app).length, 3);
  assert.deepEqual(JSON.parse(JSON.stringify(puts(app)[2].body.removedPeriods)), [{ start: '2024-04-01T00:00:00', end: '2024-04-01T01:00:00' }]);
  ok('immediate saves for remove fails and periods');

  // threshold edit and reset
  const firstKey = keys[0];
  app.get(`thresholds['1770678690|${firstKey}'].range.fail_max = 123; saveThresholds(false);`);
  await sleep(750);
  assert.equal(puts(app).length, 4); assert.equal(puts(app)[3].body.thresholds[firstKey].range.fail_max, 123);
  ok('threshold edits are saved');

  // decision flow, no second PUT because nothing pending
  app.ids.finalize.onclick(); await sleep(30);
  assert.equal(posts(app).length, 1); assert.deepEqual(posts(app)[0].body, { decision: 'finalized' });
  assert(/^Finalized\. .* PT$/.test(app.ids['review-message'].textContent), app.ids['review-message'].textContent);
  assert.equal(app.ids.submission.options[0].text, '1770678690 (Finalized)');
  ok('finalize posts the decision and shows the status');

  // edit after finalize returns to draft
  app.ids.notes.value = 'abc'; app.ids.notes.oninput(); await sleep(750);
  assert.equal(app.ids['review-message'].textContent, 'Draft saved.');
  assert.equal(app.ids.submission.options[0].text, '1770678690 (Draft)');
  ok('editing returns to draft');

  // reject needs a note
  app.ids.notes.value = '  '; app.ids.notes.oninput(); await sleep(750);
  const before = posts(app).length;
  app.ids.reject.onclick(); await sleep(30);
  assert.equal(posts(app).length, before); assert.equal(app.ids['review-message'].textContent, 'Add a note before rejecting.');
  assert(app.ids['review-message'].classes.has('error'));
  app.ids.notes.value = 'bad sensor'; app.ids.notes.oninput(); app.ids.reject.onclick(); await sleep(60);
  assert.equal(posts(app).length, before + 1); assert.deepEqual(posts(app).pop().body, { decision: 'rejected' });
  assert(/^Rejected\. /.test(app.ids['review-message'].textContent));
  ok('reject needs a note, sends pending edits first');

  // first decision with no saved review sends a draft first
  state = {}; app = boot(makeRoutes(state)); await sleep(50);
  app.ids['finalize-qa'].onclick(); await sleep(60);
  assert.equal(puts(app).length, 1); assert.equal(posts(app).length, 1);
  assert(/^Finalized\. Needs another QA\. /.test(app.ids['review-message'].textContent));
  ok('first decision saves the draft then decides');

  // 409 message shown
  state = { review: { ...emptyReview, exists: true, status: 'needs_another_qa', decidedAt: '2026-11-11T20:00:00Z' },
            postError: { status: 409, json: { error: { code: 'same_reviewer', message: 'A different reviewer must finalize a submission that needs another QA.' } } } };
  app = boot(makeRoutes(state)); await sleep(50);
  assert(/^Finalized\. Needs another QA\./.test(app.ids['review-message'].textContent));
  app.ids.finalize.onclick(); await sleep(30);
  assert.equal(puts(app).length, 0);
  assert.equal(app.ids['review-message'].textContent, 'A different reviewer must finalize a submission that needs another QA.');
  assert(app.ids['review-message'].classes.has('error'));
  ok('second reviewer conflict shown, no extra save');

  // save failure
  state = { putError: true }; app = boot(makeRoutes(state)); await sleep(50);
  app.ids['remove-fails'].checked = true; app.ids['remove-fails'].onchange(); await sleep(30);
  assert.equal(app.ids['review-message'].textContent, 'Not saved. Database error.');
  assert(app.ids['review-message'].classes.has('error'));
  ok('failed save shows an error');

  // too large
  state = { getError: 413 }; app = boot(makeRoutes(state)); await sleep(50);
  assert.equal(app.ids['app-message'].textContent, 'Submission is too large.');
  assert.equal(app.ids.finalize.disabled, true);
  ok('413 shows the message and disables review buttons');

  // saved thresholds restored
  const p = payload('1770678690'); const k = Object.keys(p.series)[0];
  const saved = JSON.parse(JSON.stringify(p.series[k].defaults)); saved.range.fail_max = 77;
  state = { review: { ...emptyReview, exists: true, notes: 'hello', removeFails: true, thresholds: { [k]: saved } } };
  app = boot(makeRoutes(state)); await sleep(50);
  assert.equal(app.get(`thresholdsFor('${k}').range.fail_max`), 77);
  assert.equal(app.ids.notes.value, 'hello'); assert.equal(app.ids['remove-fails'].checked, true);
  ok('saved thresholds, notes and switches are restored');
  // hover: timestamp plus one line per plotted parameter, nothing else
  state = { review: { ...emptyReview, exists: true, removedPeriods: [{ start: p.series[k].timestamps[1], end: p.series[k].timestamps[1] }] } };
  app = boot(makeRoutes(state)); await sleep(50);
  const spec = JSON.parse(JSON.stringify(app.get('(() => { const sp = plotSpec(plottedEntries()); return {traces: sp.traces.map(t => ({text: t.text, hovertemplate: t.hovertemplate, uid: t.uid, hovermode: null, x: t.x})), layout: {hovermode: sp.layout.hovermode}}; })()')));
  assert.equal(spec.layout.hovermode, 'closest');
  const entries = app.get('plottedEntries().map(([id,s])=>[s.label,s.unit,s.timestamps[0],s.zones[0],s.x[0]])');
  const first = spec.traces[0];
  assert.equal(first.hovertemplate, '%{text}<extra></extra>');
  const lines = first.text[0].split('<br>');
  assert(/^[A-Z][a-z]{2} \d{1,2}, \d{4} \d{2}:\d{2}( \d{2})? (PDT|PST|UTC)$/.test(lines[0]), lines[0]);
  assert(lines.length >= 2 && lines.slice(1).every(l => /^.+: -?[\d.e+-]+ .+$/.test(l)), first.text[0]);
  assert(!/Range|Spike|Object ID|Overall|Pass/.test(first.text.join(' ')));
  for (const t of spec.traces) assert.equal(t.hovertemplate, '%{text}<extra></extra>');
  // the removed reading (second timestamp of the first series) is left out of its own line
  const removedLine = first.text[1].split('<br>').slice(1).filter(l => l.startsWith(entries[0][0] + ':'));
  assert.equal(removedLine.length, 0);
  ok('hover shows only the timestamp and plotted values, removed readings omitted');
  // single choice test picker
  state = {}; app = boot(makeRoutes(state)); await sleep(50);
  assert(!/role="tab"|role="tablist"|role="tabpanel"|test-tab/.test(html) && html.includes('role="radiogroup"'));
  const radios = () => app.ids['test-options'].children.map(l => l.children[0]);
  const labels = () => app.ids['test-options'].children.map(l => l.children[1].textContent);
  const choose = v => { const r = radios().find(x => x.value === v); radios().forEach(x => { x.checked = false; }); r.checked = true; r.onchange(); };
  const traces = () => JSON.parse(JSON.stringify(app.get('plotSpec(plottedEntries()).traces.map(t=>({uid:t.uid,visible:t.visible,showlegend:t.showlegend}))')));
  const params = app.get('selectedParameters()');
  assert.equal(radios().length, params.length + 1);
  assert.equal(labels()[0], 'None'); assert(radios().every(r => r.type === 'radio'));
  assert.deepEqual(radios().filter(r => r.checked).map(r => r.value), ['']);
  assert.equal(app.ids['threshold-wrap'].hidden, true);
  const uids0 = traces().map(t => t.uid);
  assert(traces().every(t => t.uid.endsWith('|readings') || t.visible === false));
  choose(params[0]);
  assert.deepEqual(radios().map(r => r.checked), radios().map(r => r.value === params[0]));
  assert.equal(app.ids['threshold-wrap'].hidden, false);
  const markers = () => traces().filter(t => !t.uid.endsWith('|readings'));
  const vis = () => [...new Set(markers().filter(t => t.visible).map(t => t.uid.split('|').slice(0,-2).join('|')))];
  const serKey = param => app.get(`parameterEntry('${param}')[0]`);
  assert.deepEqual(vis(), [serKey(params[0])]);
  assert.deepEqual(traces().map(t => t.uid), uids0);
  const legend = () => [...new Set(markers().filter(t => t.showlegend).map(t => t.uid.split('|').slice(0,-2).join('|')))];
  assert.deepEqual(legend(), [serKey(params[0])]);
  const table = () => app.ids['threshold-panel'].children[0];
  assert.equal(app.ids['threshold-panel'].children.length, 1);
  // edit survives switching, reset only the chosen one
  const k0 = serKey(params[0]), k1 = serKey(params[1]);
  const def = (k, f) => app.get(`seriesById('${k}').defaults.range.${f}`);
  app.get(`thresholdsFor('${k0}').range.fail_max = 111; thresholdsFor('${k1}').range.fail_max = 222;`);
  choose(params[1]);
  assert.deepEqual(vis(), [k1]);
  choose(params[0]);
  assert.equal(app.get(`thresholdsFor('${k0}').range.fail_max`), 111);
  const resetBtn = table().children[1].children[0];
  resetBtn.onclick(); await sleep(750);
  assert.equal(app.get(`thresholdsFor('${k0}').range.fail_max`), def(k0, 'fail_max'));
  assert.equal(app.get(`thresholdsFor('${k1}').range.fail_max`), 222);
  assert.equal(puts(app).pop().body.thresholds[k1].range.fail_max, 222);
  // None hides markers and table
  choose('');
  assert.equal(app.ids['threshold-wrap'].hidden, true); assert.equal(vis().length, 0);
  assert.equal(app.ids['threshold-panel'].children.length, 0);
  // removing the chosen parameter from the dropdown falls back to None
  choose(params[1]);
  const check = app.ids['parameter-options'].children.map(l => l.children[0]).find(c => c.value === params[1]);
  check.checked = false; check.onchange();
  assert.deepEqual(radios().filter(r => r.checked).map(r => r.value), ['']);
  assert.equal(radios().length, params.length);
  check.checked = true; check.onchange();
  assert.deepEqual(radios().filter(r => r.checked).map(r => r.value), ['']);
  // remember per submission, nothing sent to the backend
  assert(!app.calls.some(c => JSON.stringify(c.body || {}).includes('testChoice')));
  ok('single choice test picker');
  // Review Panel top: switch, two period buttons, lists
  assert(!/id="remove-period"[^>]*type="checkbox"|id="keep-period"[^>]*type="checkbox"/.test(html));
  assert(/<input id="remove-fails" class="switch" type="checkbox" role="switch">/.test(html));
  assert(/\.switch\s*\{[^}]*appearance:none/.test(html) && /\.switch:checked/.test(html) && /\.switch:focus-visible/.test(html));
  assert(/<label class="switch-row" for="remove-fails"><span>Remove failing points<\/span>/.test(html));
  assert(/class="group-label">Periods</.test(html));
  assert(!/Remove all points that fail|Remove points within the period|Mark points within the period/.test(html));
  assert(radios().every(r => r.className === 'round-check'));
  state = {}; app = boot(makeRoutes(state)); await sleep(50);
  const dragmode = () => app.get('plotSpec(plottedEntries()).layout.dragmode');
  const btn = id => app.ids[id];
  // switch behaves like the old option and saves
  btn('remove-fails').checked = true; btn('remove-fails').onchange(); await sleep(20);
  assert.equal(puts(app).pop().body.removeFails, true);
  assert.equal(dragmode(), 'zoom');
  // start picking
  btn('remove-period').onclick();
  assert.equal(btn('remove-period').textContent, 'Cancel'); assert.equal(btn('remove-period').attrs['aria-pressed'], 'true');
  assert.equal(btn('keep-period').textContent, 'Mark a period as good'); assert.equal(btn('keep-period').attrs['aria-pressed'], 'false');
  assert.equal(dragmode(), 'select');
  assert.equal(btn('keep-period').hidden, true); assert.equal(btn('remove-period').hidden, false);
  assert.equal(app.ids['period-fields'].hidden, false);
  assert.equal(app.ids['period-start-label'].textContent, 'Start Cutting:'); assert.equal(app.ids['period-end-label'].textContent, 'End Cutting:');
  assert.equal(app.ids['period-start-text'].textContent, ''); assert.equal(app.ids['period-end-text'].textContent, '');
  assert.equal(btn('period-confirm').hidden, true); assert.equal(btn('period-undo').hidden, true);
  assert.equal(app.ids['plot-popup'].textContent, 'Drag across the plot to choose the period to remove');
  // switching kinds cancels the first
  btn('keep-period').onclick();
  assert.equal(btn('remove-period').textContent, 'Remove a period'); assert.equal(btn('keep-period').textContent, 'Cancel');
  assert.equal(app.ids['plot-popup'].textContent, 'Drag across the plot to choose the period to mark as good');
  // cancel
  btn('keep-period').onclick();
  assert.equal(btn('keep-period').textContent, 'Mark a period as good'); assert.equal(dragmode(), 'zoom');
  assert.equal(btn('remove-period').hidden, false); assert.equal(btn('keep-period').hidden, false);
  assert.equal(app.ids['plot-popup'].hidden, true); assert.equal(app.ids['period-fields'].hidden, true);
  // a selection shows the preview with the right confirm text, Undo keeps picking
  btn('keep-period').onclick();
  app.get("const ks=periodState('keep'); ks.start='2024-04-02T10:00:00'; ks.end='2024-04-02T12:15:00'; renderRemoval();");
  assert.equal(app.ids['period-start-label'].textContent, 'Start Marking:'); assert.equal(app.ids['period-end-label'].textContent, 'End Marking:');
  assert.equal(app.ids['period-start-text'].textContent, 'Apr 2, 2024 10:00');
  assert.equal(app.ids['period-end-text'].textContent, 'Apr 2, 2024 12:15');
  assert.equal(btn('period-confirm').textContent, 'Mark as good'); assert.equal(btn('period-confirm').hidden, false);
  btn('period-undo').onclick();
  assert.equal(btn('keep-period').textContent, 'Cancel'); assert.equal(dragmode(), 'select');
  assert.equal(btn('period-confirm').hidden, true); assert.equal(app.ids['period-start-text'].textContent, ''); assert.equal(app.ids['period-end-text'].textContent, '');
  assert.equal(app.ids['period-fields'].hidden, false); assert.equal(btn('remove-period').hidden, true);
  // headings only with entries
  assert.equal(app.ids['removed-block'].hidden, true); assert.equal(app.ids['kept-block'].hidden, true);
  app.get("ks.start='2024-04-02T10:00:00'; ks.end='2024-04-02T12:15:00'; renderRemoval();");
  btn('period-confirm').onclick(); await sleep(20);
  assert.deepEqual(JSON.parse(JSON.stringify(puts(app).pop().body.keptPeriods)), [{ start: '2024-04-02T10:00:00', end: '2024-04-02T12:15:00' }]);
  assert.equal(btn('keep-period').textContent, 'Mark a period as good'); assert.equal(btn('keep-period').attrs['aria-pressed'], 'false');
  assert.equal(dragmode(), 'zoom'); assert.equal(app.ids['period-fields'].hidden, true);
  assert.equal(btn('remove-period').hidden, false); assert.equal(btn('keep-period').hidden, false);
  assert.equal(app.ids['kept-block'].hidden, false); assert.equal(app.ids['removed-block'].hidden, true);
  assert.equal(app.ids['keep-list'].children.length, 1);
  assert.equal(app.ids['keep-list'].children[0].children[0].textContent, 'Apr 2, 2024 10:00 to Apr 2, 2024 12:15');
  // restore
  app.ids['keep-list'].children[0].children[1].onclick(); await sleep(20);
  assert.equal(app.ids['kept-block'].hidden, true); assert.deepEqual(JSON.parse(JSON.stringify(puts(app).pop().body.keptPeriods)), []);
  // remove kind: labels, other button hidden, back after confirm
  btn('remove-period').onclick();
  assert.equal(btn('keep-period').hidden, true);
  app.get("const rs=periodState('remove'); rs.start='2024-04-03T01:00:00'; rs.end='2024-04-03T02:00:00'; renderRemoval();");
  assert.equal(app.ids['period-start-text'].textContent, 'Apr 3, 2024 01:00');
  btn('period-undo').onclick();
  assert.equal(app.ids['period-start-text'].textContent, ''); assert.equal(btn('keep-period').hidden, true);
  app.get("rs.start='2024-04-03T01:00:00'; rs.end='2024-04-03T02:00:00'; renderRemoval();");
  btn('period-confirm').onclick(); await sleep(20);
  assert.equal(btn('keep-period').hidden, false); assert.equal(btn('remove-period').textContent, 'Remove a period');
  assert.equal(app.ids['period-fields'].hidden, true);
  assert(!/Start:|End:/.test(html.replace(/<style>[\s\S]*?<\/style>/, '')));
  // changing submission cancels picking
  btn('remove-period').onclick();
  app.ids.submission.value = '1770672364'; await app.get('selectSubmission()');
  assert.equal(btn('remove-period').textContent, 'Remove a period'); assert.equal(dragmode(), 'zoom');
  ok('review panel top: switch, period buttons, picking, lists');
  // legend size
  const lg = app.get('plotSpec(plottedEntries()).layout');
  const L = JSON.parse(JSON.stringify(lg));
  assert.equal(L.legend.font.size, 14); assert.equal(L.legend.itemwidth, 40);
  assert.equal(L.legend.orientation, 'h'); assert.equal(L.legend.x, 0.5); assert.equal(L.legend.xanchor, 'center');
  assert(L.margin.b >= 130 && L.legend.y <= -0.22);
  ok('legend size');
  // axis slots, double click, instruction line (no Reset zoom button)
  assert.equal(L.margin.l, 92 * Math.ceil(app.get('plottedEntries().length') / 2));
  const two = JSON.parse(JSON.stringify(app.get('plotSpec(plottedEntries().slice(0,3)).layout')));
  assert.equal(two.margin.l, 184); assert.equal(two.margin.r, 92);
  assert.equal(two.yaxis2.shift, 0); assert.equal(two.yaxis3.shift, -92);
  assert.equal(two.yaxis2.automargin, false); assert.equal(two.yaxis2.autoshift, undefined);
  assert.equal(app.get('PLOT_CONFIG.doubleClick'), false);
  assert(html.includes('Click and drag to zoom. Double-click to reset zoom.') && !html.includes('Reset zoom"'));
  assert.equal(app.ids['plot-instruction'].hidden, false);
  ok('axis slots, double click reset, instruction line');

  // multi sensor guard
  const multi = payload('1770678690'); multi.meta.more = 1;
  const mstate = { review: undefined };
  const mroutes = (method, url, body) => (method === 'GET' && /submissions\/\d+$/.test(url))
    ? { status: 200, json: { submission: multi, review: emptyReview } } : makeRoutes(mstate)(method, url, body);
  app = boot(mroutes); await sleep(50);
  for (const b of ['finalize', 'finalize-qa', 'reject']) assert.equal(app.ids[b].disabled, true, b);
  assert(app.ids['review-message'].textContent.includes('This submission has more than one sensor and cannot be finalized here yet.'));
  app.ids.notes.value = 'draft'; app.ids.notes.oninput(); await sleep(750);
  assert.equal(app.calls.filter(c => c.method === 'PUT').length, 1);   // drafts still save
  ok('multi sensor submission: decision buttons disabled, drafts save');

  // sign out, signed in email, 401 message
  app = boot(makeRoutes({})); await sleep(50);
  assert(html.includes('id="sign-out"') && html.includes('id="reviewer-email"'));
  assert.equal(app.ids['reviewer-email'].textContent, '');
  app = boot((m, u) => ({ status: 401, json: { error: { message: 'Sign in to use the logger review.' } } })); await sleep(50);
  assert(app.ids['app-message'].textContent.includes('sign-in ended'));
  ok('sign out control and expired sign-in message');
  console.log('page tests passed', passed);
  process.exit(0);
})().catch(e => { console.error(e); process.exit(1); });
