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
  // Starter questions per page / tab, each answerable with the chat's tools.
  var S_BRAND = ['How many brands signed this week vs last week?', 'Regular Demos this month vs last month', 'Which ad creatives are performing best this month, and why?', 'Analyze our ad budget and recommend what to do next month'];
  var S_CLN = ['How many clinician sign ups this month vs last month?', 'Break down this month\u2019s clinician funnel by MD/DO vs non-MD/DO', 'What did we spend on clinician ads this month?', 'Where is the clinician funnel losing the most people, and why?'];
  var S_ADS = ['Which creatives have the best cost per demo this month?', 'Which ads are showing fatigue right now?', 'Compare spend and CPQD by channel, month to date', 'Analyze our ad budget and recommend what to do next month'];
  var S_BD = ['Which brands signed this week?', 'What\u2019s our current MRR and how many active brands?', 'Signed brands by month this year', 'How many pilots signed this month?'];
  var SUGGEST = {
    'dash:exec': S_BRAND,
    'dash:exec:clinician': S_CLN,
    'dash:dashboard': ['Regular Demos this month vs last month', 'Which channel has the best CPQD this month?', 'How many brands signed this month, and what\u2019s their MRR?', 'Analyze our ad budget and recommend what to do next month'],
    'dash:pilot': ['How many pilots are live right now?', 'Which pilots signed this month?', 'What MRR have won pilots brought in?', 'How many pilots are still collecting data?'],
    'dash:skincare': ['Which brands signed this month?', 'How many active brands do we have, and what\u2019s our MRR?', 'Signed brands by month this year'],
    'dash:irfan': ['Regular Demos this week vs last week', 'Which Meta creatives have the lowest cost per demo this month?', 'How has ad spend changed month to month this year?'],
    'dash:signup': ['Signed brands vs Regular Demos by month this year', 'How many brands signed this month vs last month?', 'Regular Demos by month this year'],
    'dash:calculator': ['What\u2019s our current ARR and ACV?', 'How many active brands do we have, and what\u2019s our MRR?', 'What MRR did this month\u2019s signed brands add?'],
    'dash:strategy': ['How did last month compare with the month before?', 'Ad spend by month this year', 'Analyze last month\u2019s performance and what to change next month'],
    'dash:demoQuality': ['Regular Demos and no-shows this month vs last month', 'What share of demos held this month were qualified?', 'Which channel brought the most qualified demos this month?'],
    'dash:webinarPerf': ['How many webinars happened this month vs last month?', 'Regular Demos and webinars happened this month', 'How many webinars happened each month this year?'],
    'dash:clinician': S_CLN,
    'dash:analyzer': S_ADS,
    'dash:csession': S_ADS,
    'dash:content': S_ADS,
    'dash:outbound': S_BRAND,
    'bd:tracker': S_BD,
    'bd:churn': ['Which brands that signed this year have since churned?', 'How many brands that signed last month have churned?', 'What\u2019s our current MRR and how many active brands?'],
    'bd:recon': ['How many active brands are there today?', 'Which brands signed this week?', 'What\u2019s our current MRR and ARR?'],
    'bd:waterfall': ['What\u2019s our current MRR and ARR?', 'How many brands are billing vs on trial?', 'Which pilots signed this month?'],
    'sales:preDemo': ['How many Regular Demos this week vs last week?', 'How many no-shows this month?', 'Regular Demos by month this year'],
    'sales:postDemo': ['Which brands signed this week?', 'How many brands signed this week vs last week?', 'Regular Demos this month vs last month']
  };
  function suggestFor(key) {
    if (SUGGEST[key]) return SUGGEST[key];
    if (/^bd:/.test(key)) return S_BD;
    if (/^sales:/.test(key)) return SUGGEST['sales:postDemo'];
    if (/clinician/.test(key)) return S_CLN;
    return S_BRAND;
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
      if (RE_SRC.test(l)) { var sl = l.trim().replace(/\*\*/g, ''); out.push('<div class="frc-src" title="' + esc(sl) + '">' + inl(sl) + '</div>'); i++; continue; }
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
  var st = { messages: [], items: [], convId: null, deep: false, wide: false, view: 'chat', chats: null, busy: false, open: false, remaining: null, ctrl: null };
  function load() {
    try { var j = JSON.parse(window.sessionStorage.getItem(KEY) || 'null'); if (j && Array.isArray(j.items)) { st.messages = j.messages || []; st.items = j.items; st.convId = j.convId || null; st.deep = !!j.deep; st.wide = !!j.wide; st.remaining = j.remaining == null ? null : j.remaining; } } catch (e) { /* none */ }
  }
  function save() {
    try { window.sessionStorage.setItem(KEY, JSON.stringify({ messages: st.messages, items: st.items, convId: st.convId, deep: st.deep, wide: st.wide, remaining: st.remaining })); }
    catch (e) { try { window.sessionStorage.setItem(KEY, JSON.stringify({ messages: [], items: st.items, convId: st.convId, deep: st.deep })); } catch (_) { /* full */ } }
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

  // ── saved chats (server, per person, 90 days) ────────────────────────────
  function post(path, body) {
    return fetch(path, { method: 'POST', headers: { 'Content-Type': 'application/json' }, credentials: 'same-origin', redirect: 'manual', body: JSON.stringify(body || {}) })
      .then(function (r) { return r.json().catch(function () { return {}; }).then(function (j) { if (!r.ok) throw new Error(j.error || ('Request failed (' + r.status + ')')); return j; }); });
  }
  function newConvId() { return 'c' + Date.now().toString(36) + Math.random().toString(36).slice(2, 8); }
  function saveRemote() {
    if (!st.convId || !st.messages.length) return;
    var first = null; for (var i = 0; i < st.items.length; i++) if (st.items[i].role === 'user') { first = st.items[i].text; break; }
    var items = st.items.map(function (it) { var c = {}; for (var k in it) if (k !== 'copied') c[k] = it[k]; return c; });
    post('/api/chat/save', { id: st.convId, title: first || 'Chat', page: pageCtx().label, items: items, messages: st.messages })
      .then(function () { st.chats = null; }, function () { /* the chat still lives in this tab */ });
  }
  function openHistory() {
    if (st.busy) return;
    st.view = 'history'; render();
    post('/api/chat/list', {}).then(function (j) {
      st.chats = j.chats || [];
      // A just-saved chat can take a minute to show in the store's list.
      if (st.convId && st.messages.length && !st.chats.some(function (c) { return c.id === st.convId; })) {
        var q = st.items.filter(function (x) { return x.role === 'user'; });
        st.chats.unshift({ id: st.convId, title: (q[0] && q[0].text) || 'Chat', page: pageCtx().label, updated: new Date().toISOString(), n: q.length });
      }
      if (st.view === 'history') render(); },
      function (e) { st.chats = { error: e.message }; if (st.view === 'history') render(); });
  }
  function openChat(id) {
    post('/api/chat/get', { id: id }).then(function (c) {
      st.convId = c.id; st.items = c.items || []; st.messages = c.messages || []; st.view = 'chat'; save(); render(); scrollEnd(true);
    }, function (e) { st.chats = { error: e.message }; render(); });
  }
  function deleteChat(id) {
    if (Array.isArray(st.chats)) st.chats = st.chats.filter(function (c) { return c.id !== id; });
    if (st.convId === id) { st.convId = null; st.messages = []; st.items = []; save(); }
    render();
    post('/api/chat/delete', { id: id }).catch(function () {});
  }
  function ago(iso) {
    var t = Date.parse(iso); if (isNaN(t)) return '';
    var m = Math.round((Date.now() - t) / 60000);
    if (m < 1) return 'just now'; if (m < 60) return m + ' min ago';
    var h = Math.round(m / 60); if (h < 24) return h + (h === 1 ? ' hour ago' : ' hours ago');
    var d = Math.round(h / 24); if (d < 7) return d + (d === 1 ? ' day ago' : ' days ago');
    return new Date(t).toLocaleDateString(undefined, { month: 'short', day: 'numeric' });
  }

  // ── network ──────────────────────────────────────────────────────────────
  function ask(q) {
    q = String(q || '').trim();
    if (!q || st.busy) return;
    if (!st.convId) st.convId = newConvId();
    st.view = 'chat';
    var a = { role: 'a', text: '', status: [], tier: null };
    st.items.push({ role: 'user', text: q }); st.items.push(a);
    st.busy = true; render(); scrollEnd(true);
    var ctrl = (typeof AbortController !== 'undefined') ? new AbortController() : null; st.ctrl = ctrl;
    var page = pageCtx();
    var finish = function () { st.busy = false; st.ctrl = null; if (!a.done && !a.error && !a.stopped) a.error = 'The answer was cut off. Try again.'; save(); render(); scrollEnd(); if (a.done) saveRemote(); };
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
    a.fb = v; save(); renderItem(a); saveRemote();
    fetch('/api/chat/feedback', { method: 'POST', headers: { 'Content-Type': 'application/json' }, credentials: 'same-origin', body: JSON.stringify({ id: a.id, v: v }) }).catch(function () {});
  }
  function newChat() {
    if (st.ctrl) { try { st.ctrl.abort(); } catch (e) {} }
    st.messages = []; st.items = []; st.convId = null; st.view = 'chat'; st.busy = false; save(); render();
    if (els.input) els.input.focus();
  }

  // ── DOM ──────────────────────────────────────────────────────────────────
  var CSS = ''
    // The bubble: a slowly turning teal gradient ring around a lit navy core. It
    // pulses until the chat has been opened once (.fresh); motion stops for
    // people who ask for reduced motion.
    + '.frc-btn{position:fixed;right:20px;bottom:20px;width:60px;height:60px;border-radius:50%;background:transparent;border:0;color:#E3F3FA;display:flex;align-items:center;justify-content:center;cursor:pointer;z-index:190;box-shadow:0 6px 18px rgba(2,15,24,.32);padding:0;transition:transform .2s ease,box-shadow .2s ease}'
    + '.frc-btn::before{content:"";position:absolute;inset:0;border-radius:50%;z-index:0;background:conic-gradient(from 0deg,#5EC8E5,#E4F7FD,#2EC4B6,#72A4BF,#1D4053,#5EC8E5);animation:frcring 7s linear infinite}'
    + '.frc-btn::after{content:"";position:absolute;inset:4px;border-radius:50%;z-index:1;background:radial-gradient(circle at 32% 26%,#3A6E8C 0%,#1D4053 38%,#172C45 66%,#0B1A2B 100%)}'
    + '.frc-btn svg{position:relative;z-index:2;width:28px;height:28px;filter:drop-shadow(0 1px 2px rgba(0,0,0,.35));transition:transform .2s ease}'
    + '.frc-btn:hover{transform:translateY(-2px) scale(1.05);box-shadow:0 10px 24px rgba(2,15,24,.38),0 0 18px rgba(114,164,191,.45)}.frc-btn:hover::before{animation-duration:2.2s}.frc-btn:hover svg{transform:rotate(-8deg) scale(1.06)}'
    + '.frc-btn:active{transform:scale(.97)}.frc-btn:focus-visible{outline:3px solid #BFDCEB;outline-offset:3px}'
    + '.frc-btn.fresh{animation:frcpulse 2.6s ease-out infinite}'
    + '@keyframes frcring{to{transform:rotate(360deg)}}'
    + '@keyframes frcpulse{0%{box-shadow:0 6px 18px rgba(2,15,24,.32),0 0 0 0 rgba(114,164,191,.55)}70%{box-shadow:0 6px 18px rgba(2,15,24,.32),0 0 0 14px rgba(114,164,191,0)}100%{box-shadow:0 6px 18px rgba(2,15,24,.32),0 0 0 0 rgba(114,164,191,0)}}'
    + '@media (prefers-reduced-motion:reduce){.frc-btn,.frc-btn::before,.frc-btn.fresh,.frc-tip{animation:none!important}.frc-btn,.frc-btn svg{transition:none}}'
    + '.frc-panel{position:fixed;right:20px;bottom:92px;width:460px;height:min(660px,calc(100vh - 110px));background:#fff;border:1px solid var(--border,#D4E6EF);border-radius:14px;box-shadow:0 12px 40px rgba(2,15,24,.22);z-index:190;display:none;flex-direction:column;overflow:hidden;font-family:var(--font-b,Inter,system-ui,sans-serif);color:#172C45;font-size:12.5px;line-height:1.5}'
    + '.frc-panel.wide{top:12px;bottom:12px;right:12px;width:min(680px,calc(100vw - 24px));height:auto}'
    + '.frc-panel.open.wide~.frc-btn{display:none}'
    + '.frc-panel.open{display:flex}'
    + '.frc-hd{background:var(--navy,#172C45);border-left:4px solid var(--teal,#72A4BF);padding:11px 12px 11px 14px;display:flex;align-items:center;gap:10px;flex:none}'
    + '.frc-hd .t{color:#fff;font-weight:600;font-size:14px}.frc-hd .c{color:#B9C8DE;font-size:11px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}'
    + '.frc-hd .tt{flex:1;min-width:0}.frc-ib{background:none;border:0;color:#B9C8DE;cursor:pointer;padding:4px;border-radius:6px;display:flex;align-items:center;gap:4px;font-size:11px;font-family:inherit}.frc-ib:hover{color:#fff;background:rgba(255,255,255,.08)}.frc-ib svg{width:16px;height:16px}'
    + '.frc-log{flex:1;overflow-y:auto;padding:14px;display:flex;flex-direction:column;gap:12px}'
    + '.frc-u{align-self:flex-end;background:#E9F3F8;color:#0F2C3F;padding:8px 12px;border-radius:12px 12px 2px 12px;max-width:85%;white-space:pre-wrap;word-wrap:break-word}'
    + '.frc-a{max-width:100%;word-wrap:break-word}.frc-a p{margin:0 0 8px}.frc-a p:last-child{margin-bottom:0}.frc-a ul,.frc-a ol{margin:0 0 8px;padding-left:18px}.frc-a li{margin:2px 0}'
    + '.frc-a code{background:#F1F5F8;padding:1px 4px;border-radius:4px;font-size:12px}.frc-h{font-weight:700;margin:10px 0 4px;font-size:13px}'
    + '.frc-tw{overflow-x:auto;margin:4px 0 10px}.frc-a table{border-collapse:collapse;width:100%;font-size:11.5px}.frc-a th{text-align:left;color:#64748B;font-weight:600;border-bottom:1px solid var(--border,#D4E6EF);padding:3px 10px 3px 0;white-space:nowrap}.frc-a th:last-child,.frc-a td:last-child{padding-right:0}'
    + '.frc-a td{padding:3px 10px 3px 0;border-bottom:1px solid #EEF3F6;vertical-align:top}.frc-a td:first-child{white-space:nowrap}.frc-a td.n,.frc-a th.n{text-align:right;font-variant-numeric:tabular-nums;white-space:nowrap}'
    + '.frc-src{font-size:11px;color:#64748B;margin-top:8px;padding-top:6px;border-top:1px dashed #E2EAF0;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;cursor:help}.frc-src.open{white-space:normal}'
    + '.frc-st{display:flex;flex-direction:column;gap:3px;margin-bottom:8px}.frc-st div{display:flex;align-items:center;gap:6px;color:#64748B;font-size:11px}.frc-st .x{color:#B45309}'
    + '.frc-sp{width:10px;height:10px;border:2px solid #CFE0EA;border-top-color:var(--teal,#72A4BF);border-radius:50%;animation:frcspin .8s linear infinite;flex:none}@keyframes frcspin{to{transform:rotate(360deg)}}'
    + '.frc-ok{color:#10B981;font-weight:700;width:10px;text-align:center;flex:none}'
    + '.frc-ft{display:flex;align-items:center;gap:10px;margin-top:8px;font-size:11px;color:#94A3B8}.frc-badge{background:#F1F5F8;color:#475569;padding:2px 8px;border-radius:50px}.frc-badge.d{background:#EEF0FB;color:#3C3489}'
    + '.frc-ft button{background:none;border:0;cursor:pointer;color:#94A3B8;padding:2px;display:flex}.frc-ft button:hover{color:#172C45}.frc-ft button.ok{color:#10B981}.frc-ft button.on{color:var(--teal,#4F8AA8)}.frc-ft svg{width:14px;height:14px}'
    + '.frc-err{color:#B91C1C;font-size:12px}.frc-err button{margin-left:6px;background:none;border:1px solid #FCA5A5;color:#B91C1C;border-radius:6px;padding:1px 8px;cursor:pointer;font-size:11px}'
    + '.frc-fu{display:flex;flex-wrap:wrap;gap:6px;margin-top:8px}.frc-fu .frc-chip{padding:4px 10px;font-size:11.5px}'
    + '.frc-tip{position:fixed;right:90px;bottom:35px;background:var(--navy,#172C45);color:#fff;font-family:var(--font-b,Inter,system-ui,sans-serif);font-size:12px;font-weight:600;padding:7px 12px;border-radius:50px;z-index:190;box-shadow:0 4px 14px rgba(2,15,24,.22);cursor:pointer;display:flex;align-items:center;gap:8px;animation:frcin .4s ease-out}'
    + '.frc-tip kbd{background:rgba(255,255,255,.14);border-radius:4px;padding:0 5px;font:inherit;font-size:11px;font-weight:500}.frc-tip button{background:none;border:0;color:#B9C8DE;cursor:pointer;padding:0 0 0 2px;font-size:14px;line-height:1}'
    + '@keyframes frcin{from{opacity:0;transform:translateX(8px)}to{opacity:1;transform:none}}'
    + '.frc-hl{display:flex;flex-direction:column;gap:2px}.frc-hl .h{font-weight:600;font-size:12px;color:#475569;margin:0 0 6px}'
    + '.frc-row2{display:flex;align-items:center;gap:8px;padding:8px 10px;border-radius:8px;cursor:pointer;border:1px solid transparent}.frc-row2:hover{background:#F5FAFC;border-color:#E2EEF4}.frc-row2.cur{background:#EEF6FA}'
    + '.frc-row2 .tx{flex:1;min-width:0}.frc-row2 .ti{white-space:nowrap;overflow:hidden;text-overflow:ellipsis;color:#172C45}.frc-row2 .mt{font-size:11px;color:#94A3B8;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}'
    + '.frc-row2 .del{background:none;border:0;color:#CBD5E1;cursor:pointer;font-size:15px;line-height:1;padding:2px 4px;border-radius:4px}.frc-row2 .del:hover{color:#B91C1C;background:#FEF2F2}'
    + '.frc-ib.on{color:#fff;background:rgba(255,255,255,.12)}'
    + '.frc-note{color:#94A3B8;font-size:11px;font-style:italic}'
    + '.frc-empty{color:#475569}.frc-empty p{margin:0 0 10px}.frc-chips{display:flex;flex-direction:column;gap:6px;align-items:flex-start}'
    + '.frc-chip{background:#fff;border:1px solid var(--border,#D4E6EF);border-radius:50px;padding:6px 12px;font-size:12px;color:#1D4053;cursor:pointer;text-align:left;font-family:inherit}.frc-chip:hover{border-color:var(--teal,#72A4BF);background:#F5FAFC}'
    + '.frc-in{border-top:1px solid var(--border,#D4E6EF);padding:10px 12px;flex:none}'
    + '.frc-in textarea{width:100%;box-sizing:border-box;border:1px solid #CBD9E3;border-radius:10px;padding:8px 10px;font:inherit;font-size:13px;resize:none;max-height:120px;min-height:38px;color:#172C45;outline:none}.frc-in textarea:focus{border-color:var(--teal,#72A4BF);box-shadow:0 0 0 3px rgba(114,164,191,.18)}'
    + '.frc-row{display:flex;align-items:center;justify-content:space-between;margin-top:8px;gap:8px}'
    + '.frc-deep{background:none;border:1px solid #D4E2EA;border-radius:50px;padding:3px 10px;font-size:11px;color:#475569;cursor:pointer;font-family:inherit}.frc-deep.on{background:#EEF0FB;border-color:#AFA9EC;color:#3C3489}'
    + '.frc-left{font-size:11px;color:#94A3B8;flex:1;text-align:right}'
    + '.frc-send{background:var(--navy,#172C45);color:#fff;border:0;border-radius:8px;padding:6px 12px;font-size:12px;cursor:pointer;font-family:inherit;display:flex;align-items:center;gap:4px}.frc-send:hover{background:#1D4053}.frc-send.stop{background:#64748B}'
    + '@media (max-width:640px){.frc-tip{display:none!important}.frc-panel,.frc-panel.wide{right:0;left:0;bottom:0;top:0;width:auto;height:auto;border-radius:0}.frc-btn{right:14px;bottom:14px;width:54px;height:54px}.frc-panel.open~.frc-btn{display:none}}'
    + '.cs-copied{right:92px!important}'
    + '@media print{.frc-btn,.frc-panel{display:none!important}}';
  var I = {
    chat: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M21 12a8 8 0 0 1-11.6 7.1L4 20l1-4.4A8 8 0 1 1 21 12z"/><path d="M8.5 10.5h7M8.5 13.5h4.5"/></svg>',
    x: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" aria-hidden="true"><path d="M6 6l12 12M18 6L6 18"/></svg>',
    plus: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" aria-hidden="true"><path d="M12 5v14M5 12h14"/></svg>',
    copy: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><rect x="9" y="9" width="11" height="11" rx="2"/><path d="M5 15V6a2 2 0 0 1 2-2h8"/></svg>',
    up: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M7 11v9H4v-9zM7 11l4-7a2 2 0 0 1 3 2l-1 4h5a2 2 0 0 1 2 2.3l-1.2 6A2 2 0 0 1 16.8 20H7"/></svg>',
    down: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M17 13V4h3v9zM17 13l-4 7a2 2 0 0 1-3-2l1-4H6a2 2 0 0 1-2-2.3l1.2-6A2 2 0 0 1 7.2 4H17"/></svg>',
    hist: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M3 12a9 9 0 1 0 3-6.7L3 8"/><path d="M3 3v5h5M12 7v5l3 2"/></svg>',
    expand: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M15 3h6v6M9 21H3v-6M21 3l-7 7M3 21l7-7"/></svg>',
    shrink: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M4 14h6v6M20 10h-6V4M14 10l7-7M3 21l7-7"/></svg>',
    check: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.4" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M5 12l5 5L20 7"/></svg>',
    send: '<svg viewBox="0 0 24 24" width="13" height="13" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M12 19V5M5 12l7-7 7 7"/></svg>'
  };
  var els = {};
  function build() {
    var style = document.createElement('style'); style.id = 'frc-css'; style.textContent = CSS; document.head.appendChild(style);
    var panel = document.createElement('div'); panel.className = 'frc-panel'; panel.id = 'frc-panel';
    panel.setAttribute('role', 'dialog'); panel.setAttribute('aria-label', 'Ask the data');
    panel.innerHTML = '<div class="frc-hd"><div class="tt"><div class="t">Ask the data</div><div class="c" id="frc-ctx"></div></div>'
      + '<button class="frc-ib" id="frc-hist" title="Previous chats" aria-label="Previous chats">' + I.hist + '</button>'
      + '<button class="frc-ib" id="frc-wide" title="Expand" aria-label="Expand">' + I.expand + '</button>'
      + '<button class="frc-ib" id="frc-new" title="New chat" aria-label="New chat">' + I.plus + '</button>'
      + '<button class="frc-ib" id="frc-close" title="Close (Esc)" aria-label="Close">' + I.x + '</button></div>'
      + '<div class="frc-log" id="frc-log" aria-live="polite"></div>'
      + '<div class="frc-in"><textarea id="frc-input" rows="1" placeholder="Ask about this data…" aria-label="Your question"></textarea>'
      + '<div class="frc-row"><button class="frc-deep" id="frc-deep" title="Auto sends why / analyze / recommend questions to the deeper model. On sends every question there (slower).">Deep analysis: Auto</button>'
      + '<span class="frc-left" id="frc-left"></span><button class="frc-send" id="frc-send">Send ' + I.send + '</button></div></div>';
    var btn = document.createElement('button'); btn.className = 'frc-btn'; btn.id = 'frc-btn'; btn.title = 'Ask the data ( / )'; btn.setAttribute('aria-label', 'Ask the data'); btn.innerHTML = I.chat;
    document.body.appendChild(panel); document.body.appendChild(btn);
    els = { panel: panel, btn: btn, log: panel.querySelector('#frc-log'), input: panel.querySelector('#frc-input'), ctx: panel.querySelector('#frc-ctx'),
      deep: panel.querySelector('#frc-deep'), send: panel.querySelector('#frc-send'), left: panel.querySelector('#frc-left'), wide: panel.querySelector('#frc-wide') };
    els.wide.addEventListener('click', function () { st.wide = !st.wide; save(); renderChrome(); scrollEnd(true); });
    tip();
    btn.addEventListener('click', function () { toggle(); });
    panel.querySelector('#frc-close').addEventListener('click', function () { toggle(false); });
    panel.querySelector('#frc-new').addEventListener('click', newChat);
    els.hist = panel.querySelector('#frc-hist');
    els.hist.addEventListener('click', function () { if (st.view === 'history') { st.view = 'chat'; render(); } else openHistory(); });
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
  // First visit: a label beside the bubble until the chat has been opened once.
  var SEEN = 'frchat_seen';
  function seen() { try { return !!window.localStorage.getItem(SEEN); } catch (e) { return true; } }
  function markSeen() { try { window.localStorage.setItem(SEEN, '1'); } catch (e) { /* private window */ } if (els.tip) { els.tip.remove(); els.tip = null; } if (els.btn) els.btn.classList.remove('fresh'); }
  function tip() {
    if (seen()) return;
    els.btn.classList.add('fresh');
    var t = document.createElement('div'); t.className = 'frc-tip'; t.id = 'frc-tip';
    t.innerHTML = 'Ask the data <kbd>/</kbd><button aria-label="Dismiss" title="Dismiss">\u00d7</button>';
    t.addEventListener('click', function (e) { if (e.target.tagName === 'BUTTON') { markSeen(); return; } toggle(true); });
    document.body.appendChild(t); els.tip = t;
  }
  function grow() { var t = els.input; t.style.height = 'auto'; t.style.height = Math.min(120, t.scrollHeight) + 'px'; }
  function toggle(on) {
    st.open = on === undefined ? !st.open : !!on;
    els.panel.classList.toggle('open', st.open);
    els.btn.setAttribute('aria-expanded', st.open ? 'true' : 'false');
    if (st.open) { markSeen(); renderChrome(); render(); scrollEnd(true); setTimeout(function () { els.input.focus(); }, 30); }
  }
  function renderChrome() {
    var p = pageCtx();
    els.ctx.textContent = p.label + (p.window ? ' · ' + winLabel(p) : '');
    els.panel.classList.toggle('wide', st.wide);
    if (els.hist) els.hist.classList.toggle('on', st.view === 'history');
    els.wide.innerHTML = st.wide ? I.shrink : I.expand;
    els.wide.title = st.wide ? 'Shrink' : 'Expand'; els.wide.setAttribute('aria-label', els.wide.title);
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
  // The model ends with "Follow-ups: a | b | c"; shown as chips, not text.
  var RE_FU = /(^|\n)\s*\**follow-?ups?:?\**\s*([^\n]*)\s*$/i;
  function splitFollowUps(text) {
    var m = RE_FU.exec(text || '');
    if (!m) return { body: text || '', fu: [] };
    var fu = m[2].split('|').map(function (x) { return x.replace(/^[\s\-*\u2022"]+|[\s"]+$/g, ''); }).filter(function (x) { return x && x.length <= 90; }).slice(0, 3);
    return { body: (text || '').slice(0, m.index), fu: fu };
  }
  // Backstop for the prompt rule: an opening paragraph that only spells out the
  // periods ("This week: Monday 2026-11-23 to 2026-11-27; last week: ...") is
  // dropped. Only when it holds a date, mentions week/month/today, and has no
  // other number in it, so a real first sentence is never removed.
  var RE_DATE = /\b\d{4}-\d{2}-\d{2}\b|\b\d{1,2}-\d{1,2}\b/g;
  function stripDateLead(body) {
    var m = /^\s*([^\n]+(?:\n(?!\s*\n)[^\n]+)*)\s*(?:\n\s*\n|$)/.exec(body || '');
    if (!m) return body;
    var p = m[1];
    if (p.length > 260 || !RE_DATE.test(p)) { RE_DATE.lastIndex = 0; return body; }
    RE_DATE.lastIndex = 0;
    var rest = p.replace(RE_DATE, '');
    if (/\d/.test(rest) || !/\b(week|month|today)\b/i.test(rest)) return body;
    var after = body.slice(m[0].length);
    return after.trim() ? after : body;
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
    var sp = splitFollowUps(it.text);
    sp.body = stripDateLead(sp.body);
    if (sp.body) h += md(sp.body);
    if (it.stopped) h += '<div class="frc-note">Stopped.</div>';
    if (it.error) h += '<div class="frc-err">' + esc(it.error) + (it.offerNew ? '<button data-act="new">New chat</button>' : '') + '</div>';
    if (it.done) {
      h += '<div class="frc-ft"><span class="frc-badge' + (it.tier === 'deep' ? ' d' : '') + '">' + (it.tier === 'deep' ? 'Deep analysis' : 'Quick answer') + '</span>'
        + '<button data-act="copy" class="' + (it.copied ? 'ok' : '') + '" title="' + (it.copied ? 'Copied' : 'Copy') + '" aria-label="Copy answer">' + (it.copied ? I.check : I.copy) + '</button>'
        + '<button data-act="up" class="' + (it.fb === 'up' ? 'on' : '') + '" title="Helpful" aria-label="Helpful">' + I.up + '</button>'
        + '<button data-act="down" class="' + (it.fb === 'down' ? 'on' : '') + '" title="Not helpful" aria-label="Not helpful">' + I.down + '</button>'
        + '</div>';
      if (sp.fu.length && idx === st.items.length - 1 && !st.busy)
        h += '<div class="frc-fu">' + sp.fu.map(function (q) { return '<button class="frc-chip" data-q="' + esc(q) + '">' + esc(q) + '</button>'; }).join('') + '</div>';
    }
    return h + '</div>';
  }
  function render() {
    if (!els.log) return;
    renderChrome();
    if (st.view === 'history') {
      var h = '<div class="frc-hl"><div class="h">Previous chats</div>';
      if (st.chats == null) h += '<div class="frc-st"><div><span class="frc-sp"></span>Loading\u2026</div></div>';
      else if (!Array.isArray(st.chats)) h += '<div class="frc-err">' + esc(st.chats.error || 'Could not load your chats.') + '</div>';
      else if (!st.chats.length) h += '<div class="frc-note">No saved chats yet. Chats are saved after each answer and kept for 90 days.</div>';
      else h += st.chats.map(function (c) {
        return '<div class="frc-row2' + (c.id === st.convId ? ' cur' : '') + '" data-open="' + esc(c.id) + '"><div class="tx"><div class="ti">' + esc(c.title) + '</div>'
          + '<div class="mt">' + esc([c.page, ago(c.updated), c.n ? c.n + (c.n === 1 ? ' question' : ' questions') : ''].filter(Boolean).join(' \u00b7 ')) + '</div></div>'
          + '<button class="del" data-del="' + esc(c.id) + '" title="Delete" aria-label="Delete chat">\u00d7</button></div>';
      }).join('');
      els.log.innerHTML = h + '</div>';
      return;
    }
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
    var del = e.target.closest && e.target.closest('[data-del]');
    if (del) { e.stopPropagation(); if (window.confirm('Delete this chat? This can\u2019t be undone.')) deleteChat(del.getAttribute('data-del')); return; }
    var row = e.target.closest && e.target.closest('[data-open]');
    if (row) { openChat(row.getAttribute('data-open')); return; }
    var src = e.target.closest && e.target.closest('.frc-src');
    if (src) { src.classList.toggle('open'); return; }
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
      try { navigator.clipboard.writeText(stripDateLead(splitFollowUps(a.text).body).trim()).then(done, function () {}); } catch (_) { /* no clipboard */ }
    } else if (act === 'up' || act === 'down') feedback(a, act);
  }

  window.FRChat = { _md: md, _esc: esc, _splitFollowUps: splitFollowUps, _stripDateLead: stripDateLead, _pageCtx: pageCtx, _suggestFor: suggestFor, _applyEvent: applyEvent, open: function () { if (els.panel) toggle(true); }, ask: function (q) { if (els.panel) { toggle(true); ask(q); } } };
  if (BLOCKED) return;
  var boot = function () { load(); build(); render(); };
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', boot); else boot();
})();
