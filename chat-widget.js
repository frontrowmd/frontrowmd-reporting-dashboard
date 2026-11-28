// "Ask the data" — the dashboard chat bubble (2026-11-27).
// Served by the worker at /chat-widget.js (build.js inlines this file) and loaded
// by dashboard.html, bd-dashboard.html and sales-dashboard.html. Never by Growth
// or its share links: growth-dashboard.html has no tag, and this bails there too.
// Talks to POST /api/chat (Server-Sent Events) and /api/chat/feedback.
(function () {
  'use strict';
  if (typeof window === 'undefined' || window.FRChat) return;
  var path = (window.location && window.location.pathname) || '/';
  var BLOCKED = /^\/(growth|g\/)/.test(path);

  // ── page context: what the person is looking at ──────────────────────────
  var DASH_LABELS = { clinician: 'Clinician Dashboard', analyzer: 'Ad Analyzer', strategy: 'Monthly Review' };
  var BD_TABS = { tracker: 'Tracker', churn: 'Churn', recon: 'Reconciliation', waterfall: 'Billing' };
  function g(name) { try { return window[name] !== undefined ? window[name] : (new Function('return typeof ' + name + '!=="undefined"?' + name + ':undefined'))(); } catch (e) { return undefined; } }
  function pageCtx() {
    var o = { key: 'dash', label: 'Dashboard', window: null, windowLabel: null, from: null, to: null };
    try {
      if (path === '/bd') {
        var tb = g('_bdTab') || 'tracker';
        o.key = 'bd:' + tb; o.label = 'BD Tracker · ' + (BD_TABS[tb] || tb);
        var sf = g('_bdSumFrom'), stt = g('_bdSumTo');
        if (tb === 'tracker' && (sf || stt)) { o.window = 'custom'; o.from = sf || null; o.to = stt || null; o.windowLabel = 'Signed'; }
      } else if (path === '/sales') {
        var sap = g('AP') || 'preDemo', titles = g('TITLES') || {};
        o.key = 'sales:' + sap; o.label = 'Sales · ' + (titles[sap] || sap);
        var pw = g('_pdGlobalWin');
        // Sales keeps its own preset codes; send the dates it resolved them to.
        if (sap === 'postDemo' && pw && pw !== 'all') { o.window = 'custom'; o.from = g('_pdGlobalFrom') || null; o.to = g('_pdGlobalTo') || null;
          o.windowLabel = { last7: 'Last 7 Days', mtd: 'Month to Date', lm: 'Last Month', ytd: 'Year to Date' }[pw] || 'Custom range'; }
      } else {
        var ap = g('AP') || 'exec', T = g('T') || {}, S = g('S') || {};
        var st = S[ap] || null;
        var lab = T[ap] || DASH_LABELS[ap] || ap;
        if (ap === 'exec' && st && st.mode === 'clinician') lab += ' (Clinician mode)';
        o.key = 'dash:' + ap + (ap === 'exec' && st && st.mode === 'clinician' ? ':clinician' : '');
        o.label = lab;
        if (st && st.win && st.win !== 'all') { o.window = st.win; if (st.win === 'custom') { o.from = st.cFrom || null; o.to = st.cTo || null; } }
      }
    } catch (e) { /* context is best effort */ }
    return o;
  }
  var SUGGEST = {
    brand: ['How many brands signed this week vs last week?', 'Qualified Demos by month this year', 'Which ad creatives are performing best this month, and why?', 'Analyze our ad budget and recommend what to do next month'],
    clinician: ['How many clinician sign ups this month vs last month?', 'Break down this month’s clinician funnel by MD/DO vs non-MD/DO', 'Which clinician campaigns have the lowest cost per conversion this month?', 'Where is the clinician funnel losing the most people, and why?'],
    ads: ['Which creatives have the best CPQD this month?', 'Which ads are showing fatigue right now?', 'Compare spend and CPQD by channel, month to date', 'Analyze our ad budget and recommend what to do next month'],
    bd: ['How many brands signed this week vs last week?', 'What’s our current MRR and how many active brands?', 'How many pilots signed this month?', 'Signed brands by month this year'],
    sales: ['How many Qualified Demos this week vs last week?', 'How many brands signed this month?', 'Qualified Demos by month this year', 'How many pilots signed this month?']
  };
  function suggestFor(key) {
    if (/^bd:/.test(key)) return SUGGEST.bd;
    if (/^sales:/.test(key)) return SUGGEST.sales;
    if (/clinician/.test(key)) return SUGGEST.clinician;
    if (/analyzer|demoQuality|csession/.test(key)) return SUGGEST.ads;
    return SUGGEST.brand;
  }

  // ── markdown (escape first, then a small subset) ─────────────────────────
  function esc(s) { return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&#39;'); }
  function inl(s) {
    s = esc(s);
    s = s.replace(/`([^`]+)`/g, '<code>$1</code>');
    s = s.replace(/\*\*([^*]+)\*\*/g, '<strong>$1</strong>');
    s = s.replace(/(^|[^*\w])\*([^*\n]+?)\*(?![\w*])/g, '$1<em>$2</em>');
    s = s.replace(/(^|[^_\w])_([^_\n]+?)_(?![\w_])/g, '$1<em>$2</em>');
    return s;
  }
  var RE_TROW = /^\s*\|/, RE_TSEP = /^\s*\|?\s*:?-{2,}:?\s*(\|\s*:?-{2,}:?\s*)*\|?\s*$/, RE_H = /^(#{1,4})\s+(.*)$/, RE_UL = /^\s*[-*•]\s+/, RE_OL = /^\s*\d+[.)]\s+/, RE_SRC = /^\s*(\*\*)?sources?:/i;
  // Splits a table row on unescaped pipes; "\|" is a literal pipe in a cell
  // (campaign names like "FrontrowMD | Brand").
  function cells(r) {
    var s = r.trim(), out = [], cur = '';
    if (s.charAt(0) === '|') s = s.slice(1);
    if (s.slice(-1) === '|' && s.slice(-2) !== '\\|') s = s.slice(0, -1);
    for (var i = 0; i < s.length; i++) {
      var ch = s.charAt(i);
      if (ch === '\\' && s.charAt(i + 1) === '|') { cur += '|'; i++; }
      else if (ch === '|') { out.push(cur.trim()); cur = ''; }
      else cur += ch;
    }
    out.push(cur.trim());
    return out;
  }
  function isNum(c) { return /^[-+−]?[$€£]?\(?[\d,.]+\)?%?[kKmM]?$|^—$|^n\/a$/i.test(c.replace(/\*\*/g, '')); }
  function md(text) {
    var lines = String(text || '').replace(/\r/g, '').split('\n'), out = [], i = 0;
    var special = function (l) { return RE_TROW.test(l) || RE_H.test(l) || RE_UL.test(l) || RE_OL.test(l) || RE_SRC.test(l); };
    while (i < lines.length) {
      var l = lines[i];
      if (!l.trim()) { i++; continue; }
      if (RE_TROW.test(l) && i + 1 < lines.length && RE_TSEP.test(lines[i + 1])) {
        var head = cells(l); i += 2; var body = [];
        while (i < lines.length && RE_TROW.test(lines[i])) { body.push(cells(lines[i])); i++; }
        // A column is numeric (right-aligned, header included) when every
        // filled cell in it is a number.
        var numCol = head.map(function (_, ci) { var any = false; return body.every(function (r) { var c = (r[ci] || '').trim(); if (!c) return true; any = true; return isNum(c); }) && any; });
        var cls = function (ci) { return numCol[ci] ? ' class="n"' : ''; };
        out.push('<div class="frc-tw"><table><thead><tr>' + head.map(function (h, ci) { return '<th' + cls(ci) + '>' + inl(h) + '</th>'; }).join('') + '</tr></thead><tbody>'
          + body.map(function (r) { return '<tr>' + r.map(function (c, ci) { return '<td' + cls(ci) + '>' + inl(c) + '</td>'; }).join('') + '</tr>'; }).join('') + '</tbody></table></div>');
        continue;
      }
      var h = RE_H.exec(l);
      if (h) { out.push('<div class="frc-h">' + inl(h[2]) + '</div>'); i++; continue; }
      if (RE_SRC.test(l)) { out.push('<div class="frc-src">' + inl(l.trim().replace(/\*\*/g, '')) + '</div>'); i++; continue; }
      if (RE_UL.test(l) || RE_OL.test(l)) {
        var ol = RE_OL.test(l), re = ol ? RE_OL : RE_UL, it = [];
        while (i < lines.length && re.test(lines[i])) { it.push(lines[i].replace(re, '')); i++; }
        out.push((ol ? '<ol>' : '<ul>') + it.map(function (x) { return '<li>' + inl(x) + '</li>'; }).join('') + (ol ? '</ol>' : '</ul>'));
        continue;
      }
      var para = [];
      while (i < lines.length && lines[i].trim() && (para.length === 0 || !special(lines[i]))) { para.push(lines[i]); i++; }
      out.push('<p>' + para.map(inl).join('<br>') + '</p>');
    }
    return out.join('');
  }

  // ── state ────────────────────────────────────────────────────────────────
  var KEY = 'frchat_v1';
  var st = { messages: [], items: [], deep: false, busy: false, open: false, remaining: null, ctrl: null };
  function load() {
    try { var j = JSON.parse(window.sessionStorage.getItem(KEY) || 'null'); if (j && Array.isArray(j.items)) { st.messages = j.messages || []; st.items = j.items; st.deep = !!j.deep; st.remaining = j.remaining == null ? null : j.remaining; } } catch (e) { /* none */ }
  }
  function save() {
    try { window.sessionStorage.setItem(KEY, JSON.stringify({ messages: st.messages, items: st.items, deep: st.deep, remaining: st.remaining })); }
    catch (e) { try { window.sessionStorage.setItem(KEY, JSON.stringify({ messages: [], items: st.items, deep: st.deep })); } catch (_) { /* full */ } }
  }

  // ── SSE ──────────────────────────────────────────────────────────────────
  function readStream(body, onEvent) {
    var reader = body.getReader(), dec = new TextDecoder(), buf = '';
    function pump() {
      return reader.read().then(function (r) {
        if (r.value) buf += dec.decode(r.value, { stream: true });
        var i;
        while ((i = buf.indexOf('\n\n')) >= 0) {
          var chunk = buf.slice(0, i); buf = buf.slice(i + 2);
          var data = chunk.split('\n').filter(function (x) { return x.indexOf('data:') === 0; }).map(function (x) { return x.slice(5).replace(/^ /, ''); }).join('\n');
          if (data) { try { onEvent(JSON.parse(data)); } catch (e) { /* skip a bad line */ } }
        }
        if (!r.done) return pump();
      });
    }
    return pump();
  }
  function applyEvent(a, ev) {
    if (ev.t === 'start') a.tier = ev.tier;
    else if (ev.t === 'status') {
      var s = null; for (var k = 0; k < a.status.length; k++) if (a.status[k].id === ev.id) s = a.status[k];
      if (!s) { s = { id: ev.id }; a.status.push(s); }
      s.m = ev.m; s.done = !!ev.done; s.err = !!ev.err;
    } else if (ev.t === 'text') a.text += ev.d;
    else if (ev.t === 'trunc') a.text = a.text.slice(0, ev.n);
    else if (ev.t === 'done') { a.id = ev.id; a.model = ev.model; a.tier = ev.tier || a.tier; a.done = true; return { messages: ev.messages, remaining: ev.remaining }; }
    else if (ev.t === 'error') a.error = ev.m;
    return null;
  }

  // ── network ──────────────────────────────────────────────────────────────
  function ask(q) {
    q = String(q || '').trim();
    if (!q || st.busy) return;
    var a = { role: 'a', text: '', status: [], tier: null };
    st.items.push({ role: 'user', text: q }); st.items.push(a);
    st.busy = true; render(); scrollEnd(true);
    var ctrl = (typeof AbortController !== 'undefined') ? new AbortController() : null; st.ctrl = ctrl;
    var page = pageCtx();
    var finish = function () { st.busy = false; st.ctrl = null; if (!a.done && !a.error && !a.stopped) a.error = 'The answer was cut off. Try again.'; save(); render(); scrollEnd(); };
    fetch('/api/chat', { method: 'POST', headers: { 'Content-Type': 'application/json' }, credentials: 'same-origin', redirect: 'manual', signal: ctrl ? ctrl.signal : undefined,
      body: JSON.stringify({ question: q, messages: st.messages, page: { key: page.key, label: page.label, window: page.window, windowLabel: page.windowLabel, from: page.from, to: page.to }, deep: st.deep }) })
      .then(function (r) {
        if (!r.ok) {
          return r.json().catch(function () { return {}; }).then(function (j) {
            // An expired Cloudflare Access session answers with a redirect to its login.
            var signedOut = r.status === 401 || r.status === 0 || r.type === 'opaqueredirect';
            a.error = signedOut ? 'Your sign-in has expired. Reload the page and try again.' : (j.error || ('The request failed (' + r.status + ').'));
            if (j.code === 'too_long' || j.code === 'bad_history') a.offerNew = true;
            if (j.code === 'limit') st.remaining = 0;
          });
        }
        var pending = 0;
        return readStream(r.body, function (ev) {
          var o = applyEvent(a, ev);
          if (o) { st.messages = o.messages || st.messages; if (o.remaining != null) st.remaining = o.remaining; }
          if (!pending) { pending = 1; (window.requestAnimationFrame || setTimeout)(function () { pending = 0; renderItem(a); scrollEnd(); }); }
        });
      })
      .catch(function (e) { if (e && e.name === 'AbortError') a.stopped = true; else a.error = 'The connection dropped. Try again.'; })
      .then(finish);
  }
  function feedback(a, v) {
    if (!a.id || a.fb === v) return;
    a.fb = v; save(); renderItem(a);
    fetch('/api/chat/feedback', { method: 'POST', headers: { 'Content-Type': 'application/json' }, credentials: 'same-origin', body: JSON.stringify({ id: a.id, v: v }) }).catch(function () {});
  }
  function newChat() {
    if (st.ctrl) { try { st.ctrl.abort(); } catch (e) {} }
    st.messages = []; st.items = []; st.busy = false; save(); render();
    if (els.input) els.input.focus();
  }

  // ── DOM ──────────────────────────────────────────────────────────────────
  var CSS = ''
    + '.frc-btn{position:fixed;right:20px;bottom:20px;width:52px;height:52px;border-radius:50%;background:var(--navy,#172C45);border:2px solid var(--teal,#72A4BF);color:#BFDCEB;display:flex;align-items:center;justify-content:center;cursor:pointer;z-index:190;box-shadow:0 4px 14px rgba(2,15,24,.28);padding:0;transition:transform .15s}'
    + '.frc-btn:hover{transform:scale(1.06)}.frc-btn:focus-visible{outline:3px solid #BFDCEB;outline-offset:2px}.frc-btn svg{width:24px;height:24px}'
    + '.frc-panel{position:fixed;right:20px;bottom:84px;width:400px;height:min(640px,calc(100vh - 110px));background:#fff;border:1px solid var(--border,#D4E6EF);border-radius:14px;box-shadow:0 12px 40px rgba(2,15,24,.22);z-index:190;display:none;flex-direction:column;overflow:hidden;font-family:var(--font-b,Inter,system-ui,sans-serif);color:#172C45;font-size:13px;line-height:1.5}'
    + '.frc-panel.open{display:flex}'
    + '.frc-hd{background:var(--navy,#172C45);border-left:4px solid var(--teal,#72A4BF);padding:11px 12px 11px 14px;display:flex;align-items:center;gap:10px;flex:none}'
    + '.frc-hd .t{color:#fff;font-weight:600;font-size:14px}.frc-hd .c{color:#B9C8DE;font-size:11px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}'
    + '.frc-hd .tt{flex:1;min-width:0}.frc-ib{background:none;border:0;color:#B9C8DE;cursor:pointer;padding:4px;border-radius:6px;display:flex;align-items:center;gap:4px;font-size:11px;font-family:inherit}.frc-ib:hover{color:#fff;background:rgba(255,255,255,.08)}.frc-ib svg{width:16px;height:16px}'
    + '.frc-log{flex:1;overflow-y:auto;padding:14px;display:flex;flex-direction:column;gap:12px}'
    + '.frc-u{align-self:flex-end;background:#E9F3F8;color:#0F2C3F;padding:8px 12px;border-radius:12px 12px 2px 12px;max-width:85%;white-space:pre-wrap;word-wrap:break-word}'
    + '.frc-a{max-width:100%;word-wrap:break-word}.frc-a p{margin:0 0 8px}.frc-a p:last-child{margin-bottom:0}.frc-a ul,.frc-a ol{margin:0 0 8px;padding-left:18px}.frc-a li{margin:2px 0}'
    + '.frc-a code{background:#F1F5F8;padding:1px 4px;border-radius:4px;font-size:12px}.frc-h{font-weight:700;margin:10px 0 4px;font-size:13px}'
    + '.frc-tw{overflow-x:auto;margin:4px 0 10px}.frc-a table{border-collapse:collapse;width:100%;font-size:12px}.frc-a th{text-align:left;color:#64748B;font-weight:600;border-bottom:1px solid var(--border,#D4E6EF);padding:4px 8px 4px 0;white-space:nowrap}'
    + '.frc-a td{padding:4px 8px 4px 0;border-bottom:1px solid #EEF3F6;vertical-align:top}.frc-a td.n,.frc-a th.n{text-align:right;font-variant-numeric:tabular-nums;white-space:nowrap}'
    + '.frc-src{font-size:11px;color:#64748B;margin-top:8px;padding-top:6px;border-top:1px dashed #E2EAF0}'
    + '.frc-st{display:flex;flex-direction:column;gap:3px;margin-bottom:8px}.frc-st div{display:flex;align-items:center;gap:6px;color:#64748B;font-size:11px}.frc-st .x{color:#B45309}'
    + '.frc-sp{width:10px;height:10px;border:2px solid #CFE0EA;border-top-color:var(--teal,#72A4BF);border-radius:50%;animation:frcspin .8s linear infinite;flex:none}@keyframes frcspin{to{transform:rotate(360deg)}}'
    + '.frc-ok{color:#10B981;font-weight:700;width:10px;text-align:center;flex:none}'
    + '.frc-ft{display:flex;align-items:center;gap:10px;margin-top:8px;font-size:11px;color:#94A3B8}.frc-badge{background:#F1F5F8;color:#475569;padding:2px 8px;border-radius:50px}.frc-badge.d{background:#EEF0FB;color:#3C3489}'
    + '.frc-ft button{background:none;border:0;cursor:pointer;color:#94A3B8;padding:2px;display:flex}.frc-ft button:hover{color:#172C45}.frc-ft button.on{color:var(--teal,#4F8AA8)}.frc-ft svg{width:14px;height:14px}'
    + '.frc-err{color:#B91C1C;font-size:12px}.frc-err button{margin-left:6px;background:none;border:1px solid #FCA5A5;color:#B91C1C;border-radius:6px;padding:1px 8px;cursor:pointer;font-size:11px}'
    + '.frc-note{color:#94A3B8;font-size:11px;font-style:italic}'
    + '.frc-empty{color:#475569}.frc-empty p{margin:0 0 10px}.frc-chips{display:flex;flex-direction:column;gap:6px;align-items:flex-start}'
    + '.frc-chip{background:#fff;border:1px solid var(--border,#D4E6EF);border-radius:50px;padding:6px 12px;font-size:12px;color:#1D4053;cursor:pointer;text-align:left;font-family:inherit}.frc-chip:hover{border-color:var(--teal,#72A4BF);background:#F5FAFC}'
    + '.frc-in{border-top:1px solid var(--border,#D4E6EF);padding:10px 12px;flex:none}'
    + '.frc-in textarea{width:100%;box-sizing:border-box;border:1px solid #CBD9E3;border-radius:10px;padding:8px 10px;font:inherit;font-size:13px;resize:none;max-height:120px;min-height:38px;color:#172C45;outline:none}.frc-in textarea:focus{border-color:var(--teal,#72A4BF);box-shadow:0 0 0 3px rgba(114,164,191,.18)}'
    + '.frc-row{display:flex;align-items:center;justify-content:space-between;margin-top:8px;gap:8px}'
    + '.frc-deep{background:none;border:1px solid #D4E2EA;border-radius:50px;padding:3px 10px;font-size:11px;color:#475569;cursor:pointer;font-family:inherit}.frc-deep.on{background:#EEF0FB;border-color:#AFA9EC;color:#3C3489}'
    + '.frc-left{font-size:11px;color:#94A3B8;flex:1;text-align:right}'
    + '.frc-send{background:var(--navy,#172C45);color:#fff;border:0;border-radius:8px;padding:6px 12px;font-size:12px;cursor:pointer;font-family:inherit;display:flex;align-items:center;gap:4px}.frc-send:hover{background:#1D4053}.frc-send.stop{background:#64748B}'
    + '@media (max-width:640px){.frc-panel{right:0;left:0;bottom:0;top:0;width:auto;height:auto;border-radius:0}.frc-btn{right:14px;bottom:14px;width:48px;height:48px}.frc-panel.open~.frc-btn{display:none}}'
    + '.cs-copied{right:84px!important}'
    + '@media print{.frc-btn,.frc-panel{display:none!important}}';
  var I = {
    chat: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M21 12a8 8 0 0 1-11.6 7.1L4 20l1-4.4A8 8 0 1 1 21 12z"/><path d="M8.5 10.5h7M8.5 13.5h4.5"/></svg>',
    x: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" aria-hidden="true"><path d="M6 6l12 12M18 6L6 18"/></svg>',
    plus: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" aria-hidden="true"><path d="M12 5v14M5 12h14"/></svg>',
    copy: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><rect x="9" y="9" width="11" height="11" rx="2"/><path d="M5 15V6a2 2 0 0 1 2-2h8"/></svg>',
    up: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M7 11v9H4v-9zM7 11l4-7a2 2 0 0 1 3 2l-1 4h5a2 2 0 0 1 2 2.3l-1.2 6A2 2 0 0 1 16.8 20H7"/></svg>',
    down: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M17 13V4h3v9zM17 13l-4 7a2 2 0 0 1-3-2l1-4H6a2 2 0 0 1-2-2.3l1.2-6A2 2 0 0 1 7.2 4H17"/></svg>',
    send: '<svg viewBox="0 0 24 24" width="13" height="13" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M12 19V5M5 12l7-7 7 7"/></svg>'
  };
  var els = {};
  function build() {
    var style = document.createElement('style'); style.id = 'frc-css'; style.textContent = CSS; document.head.appendChild(style);
    var panel = document.createElement('div'); panel.className = 'frc-panel'; panel.id = 'frc-panel';
    panel.setAttribute('role', 'dialog'); panel.setAttribute('aria-label', 'Ask the data');
    panel.innerHTML = '<div class="frc-hd"><div class="tt"><div class="t">Ask the data</div><div class="c" id="frc-ctx"></div></div>'
      + '<button class="frc-ib" id="frc-new" title="New chat" aria-label="New chat">' + I.plus + '</button>'
      + '<button class="frc-ib" id="frc-close" title="Close (Esc)" aria-label="Close">' + I.x + '</button></div>'
      + '<div class="frc-log" id="frc-log" aria-live="polite"></div>'
      + '<div class="frc-in"><textarea id="frc-input" rows="1" placeholder="Ask about this data…" aria-label="Your question"></textarea>'
      + '<div class="frc-row"><button class="frc-deep" id="frc-deep" title="Auto sends why / analyze / recommend questions to the deeper model. On sends every question there (slower).">Deep analysis: Auto</button>'
      + '<span class="frc-left" id="frc-left"></span><button class="frc-send" id="frc-send">Send ' + I.send + '</button></div></div>';
    var btn = document.createElement('button'); btn.className = 'frc-btn'; btn.id = 'frc-btn'; btn.title = 'Ask the data ( / )'; btn.setAttribute('aria-label', 'Ask the data'); btn.innerHTML = I.chat;
    document.body.appendChild(panel); document.body.appendChild(btn);
    els = { panel: panel, btn: btn, log: panel.querySelector('#frc-log'), input: panel.querySelector('#frc-input'), ctx: panel.querySelector('#frc-ctx'),
      deep: panel.querySelector('#frc-deep'), send: panel.querySelector('#frc-send'), left: panel.querySelector('#frc-left') };
    btn.addEventListener('click', function () { toggle(); });
    panel.querySelector('#frc-close').addEventListener('click', function () { toggle(false); });
    panel.querySelector('#frc-new').addEventListener('click', newChat);
    els.deep.addEventListener('click', function () { st.deep = !st.deep; save(); renderChrome(); });
    els.send.addEventListener('click', function () { if (st.busy) { if (st.ctrl) st.ctrl.abort(); return; } var v = els.input.value; els.input.value = ''; grow(); ask(v); });
    els.input.addEventListener('keydown', function (e) { if (e.key === 'Enter' && !e.shiftKey && !e.isComposing) { e.preventDefault(); if (!st.busy) { var v = els.input.value; els.input.value = ''; grow(); ask(v); } } });
    els.input.addEventListener('input', grow);
    els.log.addEventListener('click', onLogClick);
    document.addEventListener('keydown', function (e) {
      // Escape closes the chat only when it is the chat's (not a page dialog's or editor's).
      if (e.key === 'Escape' && st.open && !e.defaultPrevented && (els.panel.contains(e.target) || e.target === document.body || e.target === els.btn)) { toggle(false); return; }
      if (e.key === '/' && !st.open && !e.metaKey && !e.ctrlKey && !e.altKey) {
        var t = e.target, tag = t && t.tagName;
        if (tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT' || (t && t.isContentEditable)) return;
        e.preventDefault(); toggle(true);
      }
    });
  }
  function grow() { var t = els.input; t.style.height = 'auto'; t.style.height = Math.min(120, t.scrollHeight) + 'px'; }
  function toggle(on) {
    st.open = on === undefined ? !st.open : !!on;
    els.panel.classList.toggle('open', st.open);
    els.btn.setAttribute('aria-expanded', st.open ? 'true' : 'false');
    if (st.open) { renderChrome(); render(); scrollEnd(true); setTimeout(function () { els.input.focus(); }, 30); }
  }
  function renderChrome() {
    var p = pageCtx();
    els.ctx.textContent = p.label + (p.window ? ' · ' + winLabel(p) : '');
    els.deep.textContent = 'Deep analysis: ' + (st.deep ? 'On' : 'Auto');
    els.deep.classList.toggle('on', st.deep);
    els.left.textContent = st.remaining == null ? '' : (st.remaining <= 0 ? 'No questions left today' : st.remaining + (st.remaining === 1 ? ' question' : ' questions') + ' left today');
    els.send.innerHTML = st.busy ? 'Stop' : ('Send ' + I.send);
    els.send.classList.toggle('stop', st.busy);
  }
  var WL = { '7d': 'Last 7 Days', mtd: 'Month to Date', lastMonth: 'Last Month', ytd: 'Year to Date' };
  function winLabel(p) {
    if (p.window !== 'custom') return WL[p.window] || p.window;
    var range = p.from && p.to ? p.from + ' to ' + p.to : p.to ? 'on or before ' + p.to : p.from ? 'from ' + p.from : '';
    return p.windowLabel && p.windowLabel !== 'Signed' ? p.windowLabel + (range && p.windowLabel === 'Custom range' ? ' (' + range + ')' : '') : (range || 'Custom');
  }
  function itemHTML(it, idx) {
    if (it.role === 'user') return '<div class="frc-u">' + esc(it.text) + '</div>';
    var h = '<div class="frc-a" data-i="' + idx + '">';
    if (it.status && it.status.length) {
      h += '<div class="frc-st">' + it.status.map(function (s) {
        return '<div' + (s.err ? ' class="x"' : '') + '>' + (s.done ? '<span class="frc-ok">' + (s.err ? '!' : '✓') + '</span>' : '<span class="frc-sp"></span>') + esc(s.m || '') + '</div>';
      }).join('') + '</div>';
    }
    var working = st.busy && idx === st.items.length - 1;
    if (!it.text && working && !(it.status || []).some(function (s) { return !s.done; })) {
      h += '<div class="frc-st"><div><span class="frc-sp"></span>' + (it.tier === 'deep' ? 'Deep analysis — this takes about a minute' : 'Working on it…') + '</div></div>';
    } else if (it.tier === 'deep' && working && !it.text) h += '<div class="frc-note">Deep analysis — this takes about a minute.</div>';
    if (it.text) h += md(it.text);
    if (it.stopped) h += '<div class="frc-note">Stopped.</div>';
    if (it.error) h += '<div class="frc-err">' + esc(it.error) + (it.offerNew ? '<button data-act="new">New chat</button>' : '') + '</div>';
    if (it.done) {
      h += '<div class="frc-ft"><span class="frc-badge' + (it.tier === 'deep' ? ' d' : '') + '">' + (it.tier === 'deep' ? 'Deep analysis' : 'Quick answer') + '</span>'
        + '<button data-act="copy" title="Copy" aria-label="Copy answer">' + I.copy + '</button>'
        + '<button data-act="up" class="' + (it.fb === 'up' ? 'on' : '') + '" title="Helpful" aria-label="Helpful">' + I.up + '</button>'
        + '<button data-act="down" class="' + (it.fb === 'down' ? 'on' : '') + '" title="Not helpful" aria-label="Not helpful">' + I.down + '</button>'
        + (it.copied ? '<span>Copied</span>' : '') + '</div>';
    }
    return h + '</div>';
  }
  function render() {
    if (!els.log) return;
    renderChrome();
    if (!st.items.length) {
      var p = pageCtx();
      els.log.innerHTML = '<div class="frc-empty"><p>Ask about the numbers on these dashboards. Answers use the same figures as the pages, and each one says where its numbers came from.</p><div class="frc-chips">'
        + suggestFor(p.key).map(function (q) { return '<button class="frc-chip" data-q="' + esc(q) + '">' + esc(q) + '</button>'; }).join('') + '</div></div>';
      return;
    }
    els.log.innerHTML = st.items.map(itemHTML).join('');
  }
  function renderItem(a) {
    var idx = st.items.indexOf(a);
    var node = els.log && els.log.querySelector('.frc-a[data-i="' + idx + '"]');
    if (!node) { render(); return; }
    var tmp = document.createElement('div'); tmp.innerHTML = itemHTML(a, idx);
    node.parentNode.replaceChild(tmp.firstChild, node);
    renderChrome();
  }
  function scrollEnd(force) {
    var l = els.log; if (!l) return;
    if (force || l.scrollHeight - l.scrollTop - l.clientHeight < 140) l.scrollTop = l.scrollHeight;
  }
  function onLogClick(e) {
    var chip = e.target.closest && e.target.closest('.frc-chip');
    if (chip) { ask(chip.getAttribute('data-q')); return; }
    var b = e.target.closest && e.target.closest('button[data-act]');
    if (!b) return;
    var act = b.getAttribute('data-act');
    if (act === 'new') { newChat(); return; }
    var holder = b.closest('.frc-a'); var a = holder && st.items[+holder.getAttribute('data-i')];
    if (!a) return;
    if (act === 'copy') {
      var done = function () { a.copied = true; renderItem(a); setTimeout(function () { a.copied = false; renderItem(a); }, 1500); };
      try { navigator.clipboard.writeText(a.text).then(done, function () {}); } catch (_) { /* no clipboard */ }
    } else if (act === 'up' || act === 'down') feedback(a, act);
  }

  window.FRChat = { _md: md, _esc: esc, _pageCtx: pageCtx, _suggestFor: suggestFor, _applyEvent: applyEvent, open: function () { if (els.panel) toggle(true); }, ask: function (q) { if (els.panel) { toggle(true); ask(q); } } };
  if (BLOCKED) return;
  var boot = function () { load(); build(); render(); };
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', boot); else boot();
})();
