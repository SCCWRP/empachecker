// Mock DOM harness for proj/templates/logger_review.html. No browser. Used by page_test.js and removal_dump.js.
const fs = require('fs'), vm = require('vm'), assert = require('assert'), path = require('path');
const ROOT = path.resolve(__dirname, '../..');
const html = fs.readFileSync(path.join(ROOT, 'proj/templates/logger_review.html'), 'utf8');
const qcSource = fs.readFileSync(path.join(ROOT, 'proj/static/logger_review/qc_tests.js'), 'utf8');

class Element {
  constructor(tag = 'div') {
    this.tagName = tag.toUpperCase(); this.children = []; this.options = []; this.selectedIndex = -1; this.hidden = false;
    this.attrs = {}; this.style = {}; this.dataset = {}; this.classes = new Set(); this.disabled = false; this.textContent = '';
    this.classList = { toggle: (c, on) => (on ? this.classes.add(c) : this.classes.delete(c)) };
  }
  get value() { return this.tagName === 'SELECT' ? (this.options[this.selectedIndex]?.value || '') : (this._value ?? ''); }
  set value(v) { if (this.tagName === 'SELECT') this.selectedIndex = this.options.findIndex(o => o.value === v); else this._value = String(v); }
  get innerHTML() { return this._html; }
  set innerHTML(v) { this._html = v; this.children = []; if (this.tagName === 'SELECT') { this.options = []; this.selectedIndex = -1; } }
  add(o) { this.options.push(o); if (this.selectedIndex < 0) this.selectedIndex = 0; }
  appendChild(c) { this.children.push(c); }
  replaceChildren(...c) { this.children = c; }
  setAttribute(k, v) { this.attrs[k] = v; }
  contains(node) { return this === node || this.children.some(child => child.contains(node)); }
  focus() {}
}

// routes: function (method, url, body) -> {status, json} or a promise of it
function boot(routes, { storage = false } = {}) {
  const ids = {}, calls = [], plots = [];
  for (const [, tag, before, id, after] of html.matchAll(/<([a-z][\w-]*)\b([^>]*?)\bid="([^"]+)"([^>]*)>/g)) {
    assert(!ids[id], 'duplicate id ' + id);
    ids[id] = new Element(tag); ids[id].hidden = /\bhidden\b/.test(before + after);
  }
  ids['parameter-picker'].children = [ids['parameter-toggle'], ids['parameter-options']];
  const fetchMock = async (url, opts = {}) => {
    const method = opts.method || 'GET', body = opts.body ? JSON.parse(opts.body) : undefined;
    calls.push({ method, url, body });
    const r = await routes(method, url, body);
    return { ok: r.status >= 200 && r.status < 300, status: r.status, json: async () => r.json };
  };
  const ctx = vm.createContext({
    console, Float64Array, Uint8Array, setTimeout, clearTimeout, fetch: fetchMock,
    Option: function (text, value) { this.text = text; this.value = value; },
    document: { getElementById: id => ids[id], createElement: tag => new Element(tag), addEventListener() {} },
    Plotly: { react: () => Promise.resolve(), newPlot: () => Promise.resolve(), relayout: () => Promise.resolve(), purge: () => Promise.resolve() },
    get localStorage() { throw new Error('localStorage must not be used'); },
  });
  vm.runInContext(qcSource, ctx);
  for (const [, attrs, code] of html.matchAll(/<script([^>]*)>([\s\S]*?)<\/script>/g)) {
    if (/\bsrc=/.test(attrs)) continue;
    if (code.trim()) vm.runInContext(code, ctx);
  }
  return { ids, calls, ctx, get: code => vm.runInContext(code, ctx) };
}
const sleep = ms => new Promise(r => setTimeout(r, ms));
module.exports = { boot, sleep, html, assert };
