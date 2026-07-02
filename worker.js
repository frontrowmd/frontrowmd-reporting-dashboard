// ============================================================================
// FrontrowMD Dashboard API — Cloudflare Worker (v3)
// ============================================================================
// Matches FrontrowMD-Metrics-Reference-Guide.md as source of truth.
// POST /api/data — returns structured JSON for all dashboard sections.
// ============================================================================

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const DASH_CHANNELS = ['meta', 'google', 'linkedin', 'tiktok', 'chatgpt', 'youtube'];

const CHANNEL_LABELS = { meta: 'Meta', google: 'Google', linkedin: 'LinkedIn', tiktok: 'TikTok', chatgpt: 'ChatGPT', youtube: 'YouTube' };

// Channels with no Windsor.ai (or equivalent) connector wired up yet — we
// still want them visible in the Channel Performance table so the budget
// allocation is transparent, but spend/demos/qual/won/ARR are all "—" until
// the integration ships. Dashboard reads `pendingConnector: true` on the
// channel payload and renders non-Budget cells as em-dashes; Total row
// skips them. (Field name avoids collision with the analyzer payload's
// existing `pending` demo-status count.)
const PENDING_CHANNELS = new Set(['chatgpt']);

const BUDGET_BY_MONTH = {
  '2026-01': { meta: 45000,  linkedin: 30000, google: 5000,  tiktok: 5000,  youtube: 5000 },
  '2026-02': { meta: 70000,  linkedin: 30000, google: 5000,  tiktok: 10000, youtube: 5000 },
  '2026-03': { meta: 90000,  linkedin: 15000, google: 10000, tiktok: 30000, youtube: 0 },
  '2026-04': { meta: 116667, linkedin: 34000, google: 16000, tiktok: 26667, youtube: 0 },
  '2026-05': { meta: 99900,  linkedin: 30900, google: 15500, tiktok: 24500, youtube: 0 },
  // chatgpt joined DASH_CHANNELS as a pending (no-connector-yet) channel
  // starting June 2026. Row appears in the Channel Performance table with
  // only the Budget cell populated; all metric cells render as em-dashes
  // until the data integration ships.
  '2026-06': { meta: 105000, linkedin: 35000, google: 25000, tiktok: 25000, youtube: 0, chatgpt: 10000 },
};
const BUDGET_FALLBACK = BUDGET_BY_MONTH['2026-06'];
function getBudgetsForMonth(dateStr) {
  if (!dateStr) return BUDGET_FALLBACK;
  return BUDGET_BY_MONTH[dateStr.slice(0, 7)] || BUDGET_FALLBACK;
}

// ── Meta "Leads (Form)" → Demos override ────────────────────────────────────
// Some Meta campaigns run the Lead Generation objective and report their
// primary result as on-Facebook instant-form leads ("Leads (Form)") instead
// of the usual submit-application conversion. Those campaigns would otherwise
// show ~0 Demos despite driving real lead volume. For campaigns in this
// allowlist we ADD the lead-form count to Demos everywhere Meta demos are
// computed from Windsor (Channel Performance / blended CPD, the analyzer
// Campaigns + Ad Sets + Audiences tables). Matched by case-insensitive
// substring of the campaign name — extend the list as more lead-gen
// campaigns launch.
const META_LEAD_AS_DEMO_CAMPAIGNS = [
  's02 - mof - warm - retargeting - video viewers - lead generation - 052726',
];
function isMetaLeadAsDemoCampaign(campaignName) {
  const n = (campaignName || '').toLowerCase();
  return META_LEAD_AS_DEMO_CAMPAIGNS.some(s => n.includes(s));
}
// "Leads (Form)" count for a Meta row = on-Facebook grouped leads
// (actions_onsite_conversion_lead_grouped), falling back to total leads
// (actions_lead) when the grouped field is absent.
function metaLeadFormCount(row) {
  const grouped = Math.round(parseFloat(row.actions_onsite_conversion_lead_grouped) || 0);
  if (grouped > 0) return grouped;
  return Math.round(parseFloat(row.actions_lead) || 0);
}
// Extra demos a Meta row contributes via Leads (Form). 0 unless the row's
// campaign is in the allowlist.
function metaLeadDemos(campaignName, row) {
  if (!isMetaLeadAsDemoCampaign(campaignName)) return 0;
  return metaLeadFormCount(row);
}
// Windsor fields needed to read Leads (Form). Appended to Meta/Facebook fetches.
const META_LEAD_FIELDS = ',actions_onsite_conversion_lead_grouped,actions_lead';

// ── CAPI custom-conversion labeling ─────────────────────────────────────────
// Only these campaigns run CAPI custom conversions; the subtext under Demos in
// the ad tables names which one. "Combined Ad Sets - Blended Conversion" uses a
// single blended Demo+Webinar conversion for all its ad sets. "Skincare & Beauty
// Brands" splits it per ad set (its "…- Demo" ad set → Demo, "…- Webinar" ad set
// → Webinar). Any other campaign returns '' (no CAPI conversion → no subtext).
// Match by case-insensitive substring of the campaign / ad-set name; extend as
// more CAPI campaigns launch. Returns 'Demo' | 'Webinar' | 'Demo + Webinar' | ''.
function capiConvLabel(campaignName, adsetName) {
  const c = (campaignName || '').toLowerCase();
  const a = (adsetName || '').toLowerCase();
  if (c.includes('combined ad sets') || c.includes('blended conversion')) return 'Demo + Webinar';
  if (c.includes('skincare & beauty brands') || c.includes('skincare & clean beauty')) {
    if (a.includes('webinar')) return 'Webinar';
    if (a.includes('demo')) return 'Demo';
    return 'Demo + Webinar';  // campaign row / unknown ad set → spans both
  }
  return '';
}

// Demo status categorization — uses new dual-field model with legacy fallback.
// Per demo-status-migration skill: prefer demo_attendance_status + demo_qualification_outcome,
// fall back to legacy demo_given__status for any unmigrated deals.
// Returns one of: qualified | pruned | noShow | rescheduled | pending | pendingEval | tooEarly | tooSmall | blank
// (tooEarly/tooSmall preserved for backward compat with dashboards that read those keys; always 0 for new data)
function categorizeDemoStatus(attendance, qualOutcome, legacyStatus) {
  const att = (attendance || '').trim();
  const qo = (qualOutcome || '').trim();

  // Prefer new fields when either is populated
  if (att || qo) {
    if (att === 'Demo Given (originally scheduled)' || att === 'Demo Given (rescheduled)') {
      if (qo === 'Qualified') return 'qualified';
      if (qo === 'Disqualified') return 'pruned';
      return 'pendingEval';                                     // demo given, qual not yet recorded
    }
    if (att === 'No Show') return 'noShow';
    if (att === 'Cancelled before demo') return 'pruned';        // pre-demo cancellation = pruned (old: "Disqualified, Meeting Cancelled")
    if (att === 'Rescheduled — pending') return 'rescheduled';
    if (att === 'Scheduled — pending') return 'pending';
    // Unknown attendance — fall through to legacy
  }

  // Legacy fallback for unmigrated deals
  const ls = (legacyStatus || '').trim();
  if (!ls) return 'blank';
  switch (ls) {
    case 'Demo Given':
    case 'Demo Given at Rescheduled time':
    case 'Demo Given, Qualified Company, too early':            // now Qualified per migration
    case 'Demo Given / Qualified / Too Small':                   // now Qualified per migration
      return 'qualified';
    case 'Not Qualified after the demo': return 'pruned';
    case 'Disqualified, Meeting Cancelled': return 'pruned';
    case 'No Show': return 'rescheduled';                        // mislabeled in old data — UI showed "Rescheduled"
    case 'No Showed': return 'noShow';
    default: return 'blank';
  }
}

// Helper: did the demo actually happen? (uses new fields; legacy fallback)
function demoDidHappen(attendance, legacyStatus) {
  const att = (attendance || '').trim();
  if (att === 'Demo Given (originally scheduled)' || att === 'Demo Given (rescheduled)') return true;
  if (att) return false;                                         // any other new-field value: did NOT happen
  // Legacy fallback
  const ls = (legacyStatus || '').trim();
  return ls === 'Demo Given' || ls === 'Demo Given at Rescheduled time'
      || ls === 'Demo Given, Qualified Company, too early'
      || ls === 'Demo Given / Qualified / Too Small'
      || ls === 'Not Qualified after the demo';
}

// Helper: is this a qualified opportunity? (demo happened AND qual = Qualified)
function isQualifiedOpp(attendance, qualOutcome, legacyStatus) {
  if (!demoDidHappen(attendance, legacyStatus)) return false;
  const qo = (qualOutcome || '').trim();
  if (qo) return qo === 'Qualified';                             // new field authoritative
  // Legacy fallback: under new model, "too early"/"too small" are now Qualified
  const ls = (legacyStatus || '').trim();
  return ls === 'Demo Given' || ls === 'Demo Given at Rescheduled time'
      || ls === 'Demo Given, Qualified Company, too early'
      || ls === 'Demo Given / Qualified / Too Small';
}

// Legacy status arrays — kept for any remaining call sites; new code uses demoDidHappen() / isQualifiedOpp()
const DEMO_HAPPENED = [
  'Demo Given', 'Demo Given at Rescheduled time',
  'Demo Given, Qualified Company, too early', 'Demo Given / Qualified / Too Small',
  'Not Qualified after the demo',
];
const QUALIFIED_STATUSES = ['Demo Given', 'Demo Given at Rescheduled time',
  'Demo Given, Qualified Company, too early', 'Demo Given / Qualified / Too Small'];

const FUNNEL_ORDER = ['qualified', 'rescheduled', 'pendingEval', 'tooEarly', 'tooSmall', 'pruned', 'noShow', 'pending', 'blank'];
const FUNNEL_LABELS = { qualified:'Qual. Demo Given', tooEarly:'Too Early', tooSmall:'Too Small', pruned:'Pruned', noShow:'No Show', rescheduled:'Rescheduled', pendingEval:'Pending Eval', pending:'Pending', blank:'Blanks' };
const FUNNEL_COLORS = { qualified:'#172C45', tooEarly:'#7C3AED', tooSmall:'#B794F4', pruned:'#F59E0B', noShow:'#EF4444', rescheduled:'#10B981', pendingEval:'#0891B2', pending:'#64748B', blank:'#9CA3AF' };

const AD_EPOCH = '2024-01-01';
const WINDSOR_EPOCH = '2025-11-01'; // Windsor ad data starts here; HubSpot can go back further

// ---------------------------------------------------------------------------
// Date Helpers (ET via offset — Workers run UTC)
// ---------------------------------------------------------------------------
function isEDT(date) {
  const y = date.getUTCFullYear();
  const mar1 = new Date(Date.UTC(y, 2, 1));
  const marSun2 = new Date(Date.UTC(y, 2, 8 + (7 - mar1.getUTCDay()) % 7));
  const dstStart = new Date(Date.UTC(y, 2, marSun2.getUTCDate(), 7, 0, 0));
  const nov1 = new Date(Date.UTC(y, 10, 1));
  const novSun1 = new Date(Date.UTC(y, 10, 1 + (7 - nov1.getUTCDay()) % 7));
  const dstEnd = new Date(Date.UTC(y, 10, novSun1.getUTCDate(), 6, 0, 0));
  return date >= dstStart && date < dstEnd;
}
function etOff(d) { return isEDT(d) ? -4 : -5; }

function todayET() {
  const n = new Date(), o = etOff(n);
  const e = new Date(n.getTime() + o * 3600000);
  return new Date(Date.UTC(e.getUTCFullYear(), e.getUTCMonth(), e.getUTCDate()));
}
function yesterdayET() { const t = todayET(); t.setUTCDate(t.getUTCDate()-1); return t; }

function fmt(d) {
  return `${d.getUTCFullYear()}-${String(d.getUTCMonth()+1).padStart(2,'0')}-${String(d.getUTCDate()).padStart(2,'0')}`;
}

function toMsET(dateStr, endOfDay = false) {
  const [y,m,d] = dateStr.split('-').map(Number);
  const ud = new Date(Date.UTC(y, m-1, d));
  const o = etOff(ud);
  if (endOfDay) return Date.UTC(y, m-1, d, 23, 59, 59, 999) - (o * 3600000);
  return Date.UTC(y, m-1, d, 0, 0, 0, 0) - (o * 3600000);
}

// For HubSpot DATE properties (like date_demo_booked) which are stored at midnight UTC
// Do NOT apply ET offset — the stored value is always midnight UTC
function toMsUTC(dateStr, endOfDay = false) {
  const [y,m,d] = dateStr.split('-').map(Number);
  if (endOfDay) return Date.UTC(y, m-1, d, 23, 59, 59, 999);
  return Date.UTC(y, m-1, d, 0, 0, 0, 0);
}

function dateMs(str) {
  if (!str) return NaN;
  if (/^\d+$/.test(str)) return parseInt(str);
  const [y,m,d] = str.split('-').map(Number);
  if (!y||!m||!d) return NaN;
  // DATE properties are stored at midnight UTC — do NOT apply ET offset
  return Date.UTC(y, m-1, d, 0, 0, 0, 0);
}
function isoMs(str) { return str ? new Date(str).getTime() : NaN; }

function daysInMonth(y, m) { return new Date(Date.UTC(y, m+1, 0)).getUTCDate(); }

// ---------------------------------------------------------------------------
// Time Windows (guide: Section "Time Windows")
// ---------------------------------------------------------------------------
function computeWindows(windowType, customFrom, customTo, vsFrom, vsTo) {
  const yd = todayET(); // Include today's data
  let current, prior, priorMonth;

  switch (windowType) {
    case '7d': {
      const f = new Date(yd); f.setUTCDate(f.getUTCDate()-6);
      current = { from: fmt(f), to: fmt(yd), label: 'Last 7 Days' };
      const pT = new Date(f); pT.setUTCDate(pT.getUTCDate()-1);
      const pF = new Date(pT); pF.setUTCDate(pF.getUTCDate()-6);
      prior = { from: fmt(pF), to: fmt(pT) };
      break;
    }
    case 'mtd': {
      const f = new Date(Date.UTC(yd.getUTCFullYear(), yd.getUTCMonth(), 1));
      current = { from: fmt(f), to: fmt(yd), label: 'Month to Date' };
      const pF = new Date(Date.UTC(yd.getUTCFullYear(), yd.getUTCMonth()-1, 1));
      const pD = Math.min(yd.getUTCDate(), daysInMonth(pF.getUTCFullYear(), pF.getUTCMonth()));
      const pT = new Date(Date.UTC(pF.getUTCFullYear(), pF.getUTCMonth(), pD));
      prior = { from: fmt(pF), to: fmt(pT) };
      break;
    }
    case 'lastMonth': {
      const f = new Date(Date.UTC(yd.getUTCFullYear(), yd.getUTCMonth()-1, 1));
      const t = new Date(Date.UTC(yd.getUTCFullYear(), yd.getUTCMonth(), 0));
      current = { from: fmt(f), to: fmt(t), label: 'Last Month' };
      const pF = new Date(Date.UTC(f.getUTCFullYear(), f.getUTCMonth()-1, 1));
      const pT = new Date(Date.UTC(f.getUTCFullYear(), f.getUTCMonth(), 0));
      prior = { from: fmt(pF), to: fmt(pT) };
      break;
    }
    case 'ytd': {
      const f = new Date(Date.UTC(yd.getUTCFullYear(), 0, 1));
      current = { from: fmt(f), to: fmt(yd), label: 'Year to Date' };
      prior = null;
      break;
    }
    case 'allTime': {
      current = { from: AD_EPOCH, to: fmt(yd), label: 'All Time' };
      prior = null;
      break;
    }
    case 'custom': {
      current = { from: customFrom, to: customTo, label: 'Custom Range' };
      if (vsFrom && vsTo) {
        // User-supplied comparison range
        prior = { from: vsFrom, to: vsTo };
      } else {
        // Default: same-length window immediately before customFrom
        const fD = new Date(customFrom+'T00:00:00Z'), tD = new Date(customTo+'T00:00:00Z');
        const span = Math.round((tD-fD)/86400000);
        const pT = new Date(fD); pT.setUTCDate(pT.getUTCDate()-1);
        const pF = new Date(pT); pF.setUTCDate(pF.getUTCDate()-span);
        prior = { from: fmt(pF), to: fmt(pT) };
      }
      break;
    }
    default: throw new Error(`Unknown window: ${windowType}`);
  }

  // Prior month: for MTD/YTD = full prior month; for 7d = same 7 days one month ago
  if (windowType === 'mtd' || windowType === 'ytd') {
    const pmF = new Date(Date.UTC(yd.getUTCFullYear(), yd.getUTCMonth()-1, 1));
    const pmT = new Date(Date.UTC(yd.getUTCFullYear(), yd.getUTCMonth(), 0));
    priorMonth = { from: fmt(pmF), to: fmt(pmT) };
  } else if (windowType === '7d') {
    // Same 7-day window one month ago
    const curF = new Date(yd); curF.setUTCDate(curF.getUTCDate()-6);
    const smF = new Date(Date.UTC(curF.getUTCFullYear(), curF.getUTCMonth()-1, curF.getUTCDate()));
    const smT = new Date(smF); smT.setUTCDate(smT.getUTCDate()+6);
    priorMonth = { from: fmt(smF), to: fmt(smT) };
  } else {
    priorMonth = null;
  }
  return { current, prior, priorMonth };
}

// ---------------------------------------------------------------------------
// UTM → Channel (guide Section 6)
// ---------------------------------------------------------------------------
function mapUtmToChannel(src, med) {
  const s = (src||'').toLowerCase().trim(), m = (med||'').toLowerCase().trim();
  if (['fb','ig','facebook','instagram','meta'].includes(s)) return 'meta';
  if (s === 'google' && (m === 'cpc' || m === 'paid')) return 'google';
  if (s === 'linkedin') return 'linkedin';
  if (['tiktok','tik_tok','tt','tiktok_ads'].includes(s)) return 'tiktok';
  if (s === 'youtube') return 'youtube';
  return null;
}

// ---------------------------------------------------------------------------
// Windsor Fetching
// ---------------------------------------------------------------------------
async function windsorFetch(apiKey, from, to, fields, extra = '') {
  const url = `https://connectors.windsor.ai/all?api_key=${apiKey}&date_from=${from}&date_to=${to}&fields=${fields}&page_size=5000${extra}`;
  for (let i = 0; i < 3; i++) {
    try {
      const r = await fetch(url);
      if (!r.ok) throw new Error(`Windsor ${r.status}`);
      const j = await r.json();
      return j.data || [];
    } catch(e) { if (i < 2) await sleep(1000*(i+1)); else { console.error('Windsor fail:', e); return []; } }
  }
  return [];
}

// Main ad data (guide Section 1)
async function fetchWindsorAds(apiKey, from, to) {
  // META_LEAD_FIELDS lets processAdSpend add "Leads (Form)" to Demos for
  // allowlisted Meta lead-gen campaigns (see metaLeadDemos).
  const fields = 'date,datasource,campaign_name,spend,clicks,impressions,ctr,conversions,externalwebsiteconversions,conversions_submit_application_total,all_conversions' + META_LEAD_FIELDS;
  return windsorFetch(apiKey, from, to, fields);
}

// LinkedIn demo override (guide Section 1 — separate conversion_name fetch)
async function fetchLinkedInDemos(apiKey, from, to) {
  const fields = 'date,datasource,conversion_name,externalwebsiteconversions';
  const rows = await windsorFetch(apiKey, from, to, fields);
  let filtered = 0;
  for (const r of rows) {
    if (!/linkedin/.test((r.datasource||'').toLowerCase())) continue;
    if ((r.conversion_name||'').toLowerCase().includes('demo request')) {
      filtered += r.externalwebsiteconversions || 0;
    }
  }
  return filtered;
}

// GA4 (guide Section 2)
async function fetchGA4(apiKey, from, to) {
  const fields = 'datasource,users,sessions,conversions_click_schedule_demo_button,conversions_hubspot_meeting_booked';
  return windsorFetch(apiKey, from, to, fields, '&connectors=googleanalytics4');
}

// GA4 page-level views via Windsor.ai. Used to count pageviews on the
// Disqualification page (/not-supported) for the % Disqualified Routing card.
// Uses pagepath (Windsor's GA4 field name) and screenpageviews (GA4's
// standard pageview metric).
// Returns { views, rowsSeen, matched, source, error }.
async function fetchGA4PageViews(apiKey, from, to, pathFilter) {
  const fields = 'date,pagepath,pagetitle,screenpageviews';
  try {
    const rows = await windsorFetch(apiKey, from, to, fields, '&connectors=googleanalytics4');
    let total = 0, matched = 0;
    const want = (pathFilter || '').toLowerCase();
    for (const r of rows) {
      const rawPath = (r.pagepath || r.page_path || r.pagePath || '').toLowerCase();
      // Strip query string for matching (/not-supported?utm=foo → /not-supported)
      const path = rawPath.split('?')[0].split('#')[0];
      if (!want || path === want || path === want + '/' || path.endsWith(want)) {
        const v = parseInt(r.screenpageviews ?? r.pageviews ?? r.views ?? 0, 10) || 0;
        total += v;
        matched++;
      }
    }
    return { views: total, rowsSeen: rows.length, matched, source: 'ga4_via_windsor', error: null };
  } catch(e) {
    console.warn('GA4 page views fetch failed:', e.message);
    return { views: null, rowsSeen: 0, matched: 0, source: 'ga4_via_windsor', error: e.message };
  }
}

// GA4 daily series for the % Disqualified Routing card:
//   • /not-supported pageviews  → numerator   ("Clicked Continue but DQ'd")
//   • cal_routing_submitted event → denominator ("Clicked Schedule a Demo and Continue")
// Both keyed by date over [from,to] via two Windsor GA4 calls. Returns
// { viewsByDate, eventByDate } where each is { 'YYYY-MM-DD': number }. The
// caller buckets these into the page's current/prior/priorMonth windows so the
// card can show vs-prior / vs-LM deltas from a single widest-window fetch.
async function fetchGA4RoutingSeries(apiKey, from, to, pagePath, eventName) {
  const want = (pagePath || '').toLowerCase();
  const viewsByDate = {}, eventByDate = {};
  try {
    const pv = await windsorFetch(apiKey, from, to, 'date,pagepath,screenpageviews', '&connectors=googleanalytics4');
    for (const r of pv) {
      const path = (r.pagepath || r.page_path || '').toLowerCase().split('?')[0].split('#')[0];
      if (!want || path === want || path === want + '/' || path.endsWith(want)) {
        const d = r.date; if (!d) continue;
        viewsByDate[d] = (viewsByDate[d] || 0) + (parseInt(r.screenpageviews ?? r.pageviews ?? 0, 10) || 0);
      }
    }
  } catch (e) { /* leave empty — card degrades to — */ }
  try {
    const ev = await windsorFetch(apiKey, from, to, 'date,event_name,event_count', '&connectors=googleanalytics4');
    for (const r of ev) {
      if ((r.event_name || '') !== eventName) continue;
      const d = r.date; if (!d) continue;
      eventByDate[d] = (eventByDate[d] || 0) + (parseInt(r.event_count) || 0);
    }
  } catch (e) { /* leave empty */ }
  return { viewsByDate, eventByDate };
}
function _sumDateRange(byDate, from, to) {
  let t = 0; for (const d in byDate) { if (d >= from && d <= to) t += byDate[d]; } return t;
}

// ---------------------------------------------------------------------------
// HubSpot Fetching
// ---------------------------------------------------------------------------
async function hsSearch(token, objectType, filterGroups, properties, limit = 200, sorts = [{ propertyName: 'hs_createdate', direction: 'ASCENDING' }], maxPages = 10) {
  const url = `https://api.hubapi.com/crm/v3/objects/${objectType}/search`;
  const all = [];
  let after = null;
  let pages = 0;
  while (true) {
    let lastErr;
    let resp;
    const body = { filterGroups, properties, limit, sorts };
    if (after) body.after = after;
    for (let attempt = 0; attempt < 4; attempt++) {
      try {
        resp = await fetch(url, {
          method: 'POST',
          headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
          body: JSON.stringify(body),
        });
        if (resp.status === 429) {
          // Rate limited — back off and retry
          await sleep(1000 * (attempt + 1));
          continue;
        }
        if (!resp.ok) { console.error(`HS ${objectType} ${resp.status}:`, await resp.text()); return all; }
        break; // success
      } catch(e) { lastErr = e; await sleep(500 * (attempt + 1)); }
    }
    if (!resp || !resp.ok) { console.error('HS search exhausted retries', lastErr); break; }
    const d = await resp.json();
    all.push(...(d.results || []));
    pages++;
    if (d.paging?.next?.after) after = d.paging.next.after; else break;
    if (all.length >= 10000 || pages >= maxPages) break;
  }
  return all;
}

// Wrap a WINDOW-INDEPENDENT fetch (same result regardless of the page's date
// selector) with a short-lived KV cache. KV binding reads/writes do NOT count
// against the per-invocation subrequest limit, so a cache hit replaces N
// paginated HubSpot subrequests with zero — directly relieving the "Too many
// subrequests by single Worker invocation" ceiling that the /api/data handler
// kept hitting as the deal database grew. Falls back to a live fetch whenever
// KV is unavailable, the entry is missing/expired, or parsing fails, so it can
// never serve stale-forever or break the response.
async function kvCachedFetch(env, key, ttlSec, fetchFn, fallback) {
  // staleData remembers the last cached payload even if expired, so a later
  // live-fetch failure degrades to stale data instead of crashing the handler.
  let staleData = (fallback !== undefined ? fallback : null);
  if (env && env.CONTENT_STORE) {
    try {
      const raw = await env.CONTENT_STORE.get(key);
      if (raw) {
        const parsed = JSON.parse(raw);
        if (parsed && typeof parsed.t === 'number') {
          staleData = parsed.data;
          if ((Date.now() - parsed.t) < ttlSec * 1000) return parsed.data;
        }
      }
    } catch (e) { /* fall through to live fetch */ }
  }
  let data;
  try {
    data = await fetchFn();
  } catch (e) {
    // Live fetch failed (e.g. subrequest budget). Serve stale/fallback rather
    // than bubbling up — a degraded card beats a dead dashboard.
    console.error('kvCachedFetch live fetch failed for ' + key + ':', e && e.message);
    return staleData;
  }
  if (env && env.CONTENT_STORE) {
    try {
      const blob = JSON.stringify({ t: Date.now(), data });
      // KV value hard limit is 25 MB — skip caching anything near it rather
      // than letting the put reject.
      if (blob.length < 20 * 1024 * 1024) await env.CONTENT_STORE.put(key, blob);
    } catch (e) { /* non-fatal — caching is best-effort */ }
  }
  return data;
}

// Like kvCachedFetch, but for WINDOW-STABLE list fetches (prior / last-month
// periods are PAST and don't change) that must NEVER cache an empty result —
// an empty array almost always means a failed/rate-limited fetch, not "no
// data", and caching it would blank out vs-prior / vs-LM deltas for the TTL.
// Caches only non-empty arrays; on an empty/failed live fetch, serves the last
// good cached value (even if past the freshness window — past data is stable).
async function kvCachedFetchNE(env, key, ttlSec, fetchFn) {
  let stale = null;
  if (env && env.CONTENT_STORE) {
    try {
      const raw = await env.CONTENT_STORE.get(key);
      if (raw) {
        const p = JSON.parse(raw);
        if (p && Array.isArray(p.data) && p.data.length) {
          stale = p.data;
          if (typeof p.t === 'number' && (Date.now() - p.t) < ttlSec * 1000) return p.data;
        }
      }
    } catch (e) { /* fall through to live fetch */ }
  }
  let data;
  try { data = await fetchFn(); } catch (e) { return stale || []; }
  if (Array.isArray(data) && data.length) {
    if (env && env.CONTENT_STORE) {
      try { const blob = JSON.stringify({ t: Date.now(), data }); if (blob.length < 20*1024*1024) await env.CONTENT_STORE.put(key, blob, { expirationTtl: Math.max(ttlSec*6, 3600) }); } catch (e) { /* best-effort */ }
    }
    return data;
  }
  // Live fetch came back empty (likely a transient failure) — prefer the last
  // good cached value over an empty set so deltas stay populated.
  return (stale && stale.length) ? stale : (data || []);
}

// Demos Booked = contacts created in window with date_demo_booked set (guide Section 3)
// Windsor date clamp — don't query before ad data exists
function wFrom(dateStr) { return dateStr < WINDSOR_EPOCH ? WINDSOR_EPOCH : dateStr; }

// Filter deals: exclude Paused, Churned, and Never Implemented - Churned
// Keeps: Paying, Signed, and empty/unset (new pipeline deals not yet categorized)
function filterActiveBrands(deals) {
  return deals.filter(d => {
    const bs = (d.properties?.brand_status || '').toLowerCase();
    if (!bs) return true; // keep deals with no brand_status (new pipeline deals)
    return bs === 'paying' || bs === 'signed';
  });
}

// Look up the "Disqualification Form" by name (case-insensitive substring match),
// then return BOTH:
//   • count — submissions in [from, to]  (count of people who filled out the form)
//   • views — form page views in [from, to] (count of times the form was loaded
//     on a page — pulled from HubSpot's v2 analytics endpoint)
// Used by Irfan KPI #5 (% Disqualified Routing). The numerator on the card is
// views — that's the right metric for "how many people were routed to the
// disqualification page", since a view happens whether or not the visitor
// completes the form.
async function fetchDisqualificationFormSubmissions(token, from, to) {
  // Step 1: list forms and find the DQ form by name
  const formsRes = await fetch('https://api.hubapi.com/marketing/v3/forms?limit=200', {
    headers: { Authorization: `Bearer ${token}` },
  });
  if (!formsRes.ok) {
    const txt = await formsRes.text();
    throw new Error(`Forms list failed: ${formsRes.status} ${txt.slice(0,120)}`);
  }
  const formsData = await formsRes.json();
  const form = (formsData.results || []).find(f =>
    (f.name || '').toLowerCase().includes('disqualification')
  );
  if (!form) {
    return { count: 0, views: null, formFound: false };
  }
  // Step 2a: walk submissions filtered by submittedAt in [from, to]
  const fromMs = new Date(from + 'T00:00:00Z').getTime();
  const toMs = new Date(to + 'T23:59:59.999Z').getTime();
  let count = 0, totalSeen = 0, after = null, pages = 0;
  while (pages < 50) {
    let url = `https://api.hubapi.com/form-integrations/v1/submissions/forms/${form.id}?limit=50`;
    if (after) url += `&after=${after}`;
    const res = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
    if (!res.ok) {
      console.error(`DQ form submissions ${res.status}: ${(await res.text()).slice(0,120)}`);
      break;
    }
    const data = await res.json();
    const results = data.results || [];
    totalSeen += results.length;
    let pastWindow = false;
    for (const s of results) {
      const ms = s.submittedAt || 0;
      if (ms >= fromMs && ms <= toMs) count++;
      // Submissions come back newest-first by default; once we cross before fromMs we can stop
      if (ms < fromMs) pastWindow = true;
    }
    if (pastWindow) break;
    if (!data.paging?.next?.after) break;
    after = data.paging.next.after;
    pages++;
  }
  // Step 2b: fetch form view counts via HubSpot Analytics v2.
  // Endpoint: /analytics/v2/reports/forms/total?start=YYYYMMDD&end=YYYYMMDD&f=<formId>
  // Date params are start/end (NOT d1/d2 — those are drilldown filter slots).
  // Response shape with the f= filter:
  //   { offset, total, totals: { formViews, submissions, submissionsPerFormView, ... },
  //     breakdowns: [{ breakdown: <formId>, formViews, submissions, ... }] }
  // The metric we want is `formViews` (matches the number in HubSpot's Forms
  // Performance UI). conversion-rate is `submissionsPerFormView`.
  // Requires the private app to have the business-intelligence (analytics.read) scope.
  let views = null, viewError = null, conversionRate = null, interactions = null, visibles = null;
  try {
    const d1 = from.replace(/-/g, '');
    const d2 = to.replace(/-/g, '');
    const viewsUrl = `https://api.hubapi.com/analytics/v2/reports/forms/total?start=${d1}&end=${d2}&f=${encodeURIComponent(form.id)}`;
    const viewsRes = await fetch(viewsUrl, { headers: { Authorization: `Bearer ${token}` } });
    if (viewsRes.ok) {
      const viewsData = await viewsRes.json();
      // Prefer totals (single-form filter returns the form's metrics here),
      // fall back to the breakdowns row matching our form ID.
      const t = viewsData?.totals;
      if (t && typeof t.formViews === 'number') {
        views = t.formViews;
        if (typeof t.submissionsPerFormView === 'number') conversionRate = t.submissionsPerFormView;
        if (typeof t.interactions === 'number') interactions = t.interactions;
        if (typeof t.visibles === 'number') visibles = t.visibles;
      } else if (Array.isArray(viewsData?.breakdowns)) {
        const myRow = viewsData.breakdowns.find(b => b?.breakdown === form.id);
        if (myRow) {
          if (typeof myRow.formViews === 'number') views = myRow.formViews;
          if (typeof myRow.submissionsPerFormView === 'number') conversionRate = myRow.submissionsPerFormView;
          if (typeof myRow.interactions === 'number') interactions = myRow.interactions;
          if (typeof myRow.visibles === 'number') visibles = myRow.visibles;
        } else {
          viewError = `Form ${form.id} not present in HubSpot breakdowns (got ${viewsData.breakdowns.length} rows)`;
        }
      }
      if (views == null && !viewError) {
        const snippet = JSON.stringify(viewsData).slice(0, 400);
        viewError = `Unrecognized response shape: ${snippet}`;
      }
      console.log(`DQ form: ${views} views, ${count} submissions, ${conversionRate!=null?(conversionRate*100).toFixed(2)+'%':'?'} conv (${form.name}, ${from}..${to})`);
    } else {
      const txt = await viewsRes.text();
      viewError = viewsRes.status === 403
        ? 'HubSpot 403: token missing analytics scope (Settings → Integrations → Private Apps → Scopes → Analytics)'
        : `HTTP ${viewsRes.status}: ${txt.slice(0,200)}`;
      console.warn(`DQ form views ${viewsRes.status}: ${txt.slice(0,200)}`);
    }
  } catch(e) {
    viewError = `Fetch threw: ${e.message}`;
    console.warn('DQ form views fetch threw:', e.message);
  }
  return { count, views, viewError, conversionRate, interactions, visibles, totalSeen, pages, formFound: true, formId: form.id, formName: form.name };
}

async function fetchScheduledContacts(token, from, to) {
  const fMs = String(toMsET(from));
  const tMs = String(toMsET(to, true));
  // Two ORed filter groups:
  //   1. Contacts who booked a meeting (date_demo_booked SET) in the window.
  //      This is the canonical "demos booked" population.
  //   2. Contacts in the user's "Webinar Registrations" segment
  //      (hs_object_source_detail_1 = "Livestorm Webinars") created in the
  //      window. These typically don't carry date_demo_booked but should
  //      still surface as the "Webinar" pie slice. processScheduledContacts
  //      bucket-maps them into Very Small and skips them from byDay /
  //      byDayScale / total per the dashboard spec.
  return hsSearch(token, 'contacts', [
    {
      filters: [
        { propertyName: 'createdate', operator: 'GTE', value: fMs },
        { propertyName: 'createdate', operator: 'LTE', value: tMs },
        { propertyName: 'date_demo_booked', operator: 'HAS_PROPERTY' },
      ],
    },
    {
      filters: [
        { propertyName: 'createdate', operator: 'GTE', value: fMs },
        { propertyName: 'createdate', operator: 'LTE', value: tMs },
        { propertyName: 'hs_object_source_detail_1', operator: 'EQ', value: 'Livestorm Webinars' },
      ],
    },
  ], ['createdate', 'date_demo_booked', 'email', 'website', 'company', 'average_monthly_web_traffic', 'hs_object_source_detail_1']);
}

// Contacts for Demo Quality — matched to deals by company name
async function fetchContactsForDQ(token, from, to) {
  return hsSearch(token, 'contacts', [{
    filters: [
      { propertyName: 'date_demo_booked', operator: 'GTE', value: String(toMsUTC(from)) },
      { propertyName: 'date_demo_booked', operator: 'LTE', value: String(toMsUTC(to, true)) },
    ],
  }, {
    // Webinar registrants created in the window (have webinar_date) so the Demo
    // Quality table's webinar rows can match their contact (Webinar Date / Webinar columns).
    filters: [
      { propertyName: 'createdate', operator: 'GTE', value: String(toMsET(from)) },
      { propertyName: 'createdate', operator: 'LTE', value: String(toMsET(to, true)) },
      { propertyName: 'webinar_date', operator: 'HAS_PROPERTY' },
    ],
  }], ['date_demo_booked', 'firstname', 'lastname', 'email', 'website', 'company', 'role_at_company',
       'hs_sales_email_last_opened', 'hs_sales_email_last_clicked', 'hs_sales_email_last_replied',
       'notes_last_contacted', 'hs_sequences_is_enrolled', 'hs_latest_sequence_enrolled',
       'hs_latest_sequence_enrolled_date',
       'average_monthly_web_traffic', 'sl_last_demo_name', 'sl_last_demo_completion_percent',
       'webinar_date', 'webinar_has_attended']);
}

// Webinar-stage deals (Webinar Registered / Webinar Attended) created in the
// window. They have no demo date so they're NOT caught by fetchPipelineDeals;
// surfaced as extra rows on the Demo Quality TABLE only (kept out of demo metrics).
async function fetchWebinarStageDeals(token, from, to) {
  return hsSearch(token, 'deals', [{
    filters: [
      { propertyName: 'dealstage', operator: 'IN', values: ['3741686470', '3741686471'] },
      { propertyName: 'createdate', operator: 'GTE', value: String(toMsET(from)) },
      { propertyName: 'createdate', operator: 'LTE', value: String(toMsET(to, true)) },
    ],
  }], ['dealname','dealstage','date_demo_booked','demo_attendance_status','demo_qualification_outcome','rescheduled_meeting_date','disqualification_reason','createdate','hs_createdate','hubspot_owner_id','utm_source','utm_medium','utm_campaign','utm_content','brand_status','average_monthly_web_traffic__cloned_'],
  200, [{ propertyName: 'createdate', direction: 'DESCENDING' }], 10);
}

// Pipeline deals (guide Section 4 — multiple filterGroups OR)
async function fetchPipelineDeals(token, from, to, opts = {}) {
  // date_demo_booked is a DATE property stored at midnight UTC — use UTC boundaries
  const fMsDate = String(toMsUTC(from)), tMsDate = String(toMsUTC(to, true));
  // hs_createdate is a datetime property — use ET boundaries
  const fMsET = String(toMsET(from)), tMsET2 = String(toMsET(to, true));
  // Pagination & sort can be overridden by callers fetching wide windows where
  // hsSearch's default (200 × 10 pages = 2000) is at risk of capping — e.g.
  // the 4-month Sign-Up Rate cohort fetch. Default sort (hs_createdate ASC)
  // truncates from the newest end if the cap is hit, which silently drops the
  // most recent months. Cohort callers should pass DESC sort + bumped maxPages.
  const limit = opts.limit ?? 200;
  const sorts = opts.sorts ?? [{ propertyName: 'hs_createdate', direction: 'ASCENDING' }];
  const maxPages = opts.maxPages ?? 10;
  const deals = await hsSearch(token, 'deals', [
    { filters: [
      { propertyName: 'date_demo_booked', operator: 'GTE', value: fMsDate },
      { propertyName: 'date_demo_booked', operator: 'LTE', value: tMsDate },
    ]},
    { filters: [
      // Rescheduled deals: include if rescheduled_meeting_date falls in window
      // (catches deals originally booked outside the window but rescheduled into it).
      // rescheduled_meeting_date is a DATE property — use UTC boundaries (same as date_demo_booked).
      { propertyName: 'rescheduled_meeting_date', operator: 'GTE', value: fMsDate },
      { propertyName: 'rescheduled_meeting_date', operator: 'LTE', value: tMsDate },
    ]},
    { filters: [
      // New fields: catch deals marked as missed-but-no-date in the create window
      { propertyName: 'demo_attendance_status', operator: 'IN', values: ['No Show', 'Cancelled before demo'] },
      { propertyName: 'hs_createdate', operator: 'GTE', value: fMsET },
      { propertyName: 'hs_createdate', operator: 'LTE', value: tMsET2 },
    ]},
  ], ['dealname','date_demo_booked','demo_given_date','demo_given__status','demo_attendance_status','demo_qualification_outcome','rescheduled_meeting_date','disqualification_reason','dealstage','amount','closedate','createdate','hs_createdate','hubspot_owner_id','utm_source','utm_medium','utm_campaign','utm_content','brand_status','average_monthly_web_traffic__cloned_'], limit, sorts, maxPages);
  return [...new Map(deals.map(d => [d.id, d])).values()]; // dedup
}

// Pipeline-agnostic "won" deals by closedate. Uses HubSpot's hs_is_closed_won
// computed property so custom-pipeline won stages (e.g. an Onboarding pipeline)
// are picked up alongside the default 'closedwon' stage. Used by Irfan MTD/Last14.
async function fetchAnyWonDealsByCloseDate(token, from, to) {
  return hsSearch(token, 'deals', [{
    filters: [
      { propertyName: 'hs_is_closed_won', operator: 'EQ', value: 'true' },
      { propertyName: 'closedate', operator: 'GTE', value: String(toMsET(from)) },
      { propertyName: 'closedate', operator: 'LTE', value: String(toMsET(to, true)) },
    ],
  }], ['amount','closedate','hs_createdate','hs_date_entered_closedwon','utm_source','utm_medium','utm_campaign','utm_content','hubspot_owner_id','brand_status','average_monthly_web_traffic__cloned_','average_monthly_web_traffic','dealstage','dealname']);
}

// Pipeline-agnostic "won" deals by hs_date_entered_closedwon (the timestamp HubSpot
// stamps when a deal enters a closed-won stage). Catches deals whose user-editable
// `closedate` falls outside the window but which entered won inside it.
async function fetchAnyWonDealsByEnteredWon(token, from, to) {
  return hsSearch(token, 'deals', [{
    filters: [
      { propertyName: 'hs_is_closed_won', operator: 'EQ', value: 'true' },
      { propertyName: 'hs_date_entered_closedwon', operator: 'GTE', value: String(toMsET(from)) },
      { propertyName: 'hs_date_entered_closedwon', operator: 'LTE', value: String(toMsET(to, true)) },
    ],
  }], ['amount','closedate','hs_createdate','hs_date_entered_closedwon','utm_source','utm_medium','utm_campaign','utm_content','hubspot_owner_id','brand_status','average_monthly_web_traffic__cloned_','average_monthly_web_traffic','dealstage','dealname']);
}

// Deals by date_demo_booked (single filter, no OR groups). Used by the
// Sign-Up Rate cohort builder. fetchPipelineDeals's three OR groups create
// duplicate hits that inflate the raw count against hsSearch's 10k hard cap,
// silently dropping deals on wide (4-month) cohort windows.
async function fetchCohortDealsByBookedDate(token, from, to) {
  return hsSearch(token, 'deals', [{
    filters: [
      { propertyName: 'date_demo_booked', operator: 'GTE', value: String(toMsUTC(from)) },
      { propertyName: 'date_demo_booked', operator: 'LTE', value: String(toMsUTC(to, true)) },
    ],
  }], ['dealname','date_demo_booked','demo_attendance_status','demo_qualification_outcome','dealstage','amount','closedate','hs_createdate','hubspot_owner_id','brand_status'],
  200, [{ propertyName: 'date_demo_booked', direction: 'DESCENDING' }], 50);
}

// Sign-Up Rate cohort fetch: ONE query per cohort month, SEQUENTIAL.
// A single 4-month wide query hits HubSpot's 10k hard limit on busy
// pipelines and silently drops one end of the window. Running 4 parallel
// queries can hit rate limits or CF subrequest budget at the wrong moment
// and individual months fail silently — so we go sequential. Total cost
// for ~250 demos/month is 4 × 1-2 page requests = 4-8 HTTP calls, fast
// enough not to matter.
// Returns { union: Deal[], perMonth: { [label]: count } } so the caller
// can surface the per-cohort fetched count to the dashboard.
async function fetchCohortDealsPerMonth(token, cohortMonths) {
  // FULLY SEQUENTIAL cohort fetch via fetchPipelineDeals. Every previous
  // attempt at parallel (or chunked parallel) has eventually overlapped
  // with HubSpot's 100req/10s rate-limit budget and silently dropped
  // older-month queries. Sequential with small gaps is the bulletproof
  // path:
  //
  //   • fetchPipelineDeals is the proven helper used by cPipe/pPipe/
  //     pmPipe; its 3-OR-filterGroup query shape paginates reliably
  //     past the 200-deal page cap that single-filter queries hit.
  //   • One month at a time means no overlapping page fetches against
  //     HubSpot's rate counter.
  //   • Each month gets up to 3 attempts with growing backoff (0/600/
  //     1200ms) if it returns empty or errors. Empty-after-retry means
  //     either the month is genuinely empty OR HubSpot triple-dropped
  //     it — surfaced to the dashboard via perMonthStatus.
  //   • 250ms gap between months further decays the rate counter.
  //
  // Total worst-case wall-clock: ~4 months × (1.5s avg fetch + retries)
  // ≈ 12-18s. CF Workers Paid has no fixed wall-clock cap for HTTP
  // handlers — only the 30s CPU-time limit, and time spent awaiting
  // fetches doesn't count toward that.

  const map = new Map();
  const perMonth = {};
  const perMonthStatus = {};
  const MAX_ATTEMPTS = 3;
  const BACKOFFS = [0, 600, 1200]; // ms before each attempt

  console.log(`SignUp cohort fetch (sequential via fetchPipelineDeals, ${cohortMonths.length} months)`);
  for (const cm of cohortMonths) {
    let arr = [];
    let status = 'empty';
    let lastErr = null;
    for (let attempt = 0; attempt < MAX_ATTEMPTS; attempt++) {
      if (BACKOFFS[attempt] > 0) await sleep(BACKOFFS[attempt]);
      try {
        // maxPages capped at 5 (1000-deal headroom per cohort, well above
        // any realistic single-month size). Lower cap keeps the SignUp
        // cohort fetch's own subrequest cost predictable so we don't blow
        // the CF Workers per-request budget on a single month.
        const candidate = await fetchPipelineDeals(token, cm.from, cm.to, { maxPages: 5 });
        if (candidate && candidate.length > 0) {
          arr = candidate;
          status = attempt === 0 ? 'ok' : 'ok-retry';
          break;
        }
        // Empty result — try again unless this was the last attempt
        status = 'empty';
      } catch(e) {
        status = 'failed';
        lastErr = e;
        console.warn(`SignUp ${cm.label} attempt ${attempt+1} threw:`, e.message);
      }
    }
    if (status === 'empty' && lastErr) status = 'failed-retry';
    perMonth[cm.label] = arr.length;
    perMonthStatus[cm.label] = status;
    for (const d of arr) map.set(d.id, d);
    console.log(`SignUp ${cm.label} (${cm.from}..${cm.to}): ${arr.length} deals [${status}]`);
    // Small gap before the next month's fetches start — lets HubSpot's
    // rate counter decay so we don't accumulate pressure.
    await sleep(250);
  }
  console.log(`SignUp cohort union: ${map.size} unique across ${Object.keys(perMonth).length} months`);
  return { union: [...map.values()], perMonth, perMonthStatus };
}

// Closed-won deals by closedate (guide Section 5).
// Filters on the default-pipeline 'closedwon' stage. We deliberately do NOT
// use the pipeline-agnostic hs_is_closed_won = true here: that matches every
// won deal across ALL pipelines (including the entire active customer base in
// the Onboarding pipeline), which paginates into many more pages per call.
// Since this helper runs 3-4× per /api/data invocation, the broad filter blew
// past Cloudflare's per-invocation subrequest budget and broke every page.
// The one-deal discrepancy vs. the Irfan Dashboard (which uses the broader
// filter) is handled separately/lightly — see note in processDataRequest.
async function fetchClosedWonDeals(token, from, to) {
  return hsSearch(token, 'deals', [{
    filters: [
      { propertyName: 'dealstage', operator: 'EQ', value: 'closedwon' },
      { propertyName: 'closedate', operator: 'GTE', value: String(toMsET(from)) },
      { propertyName: 'closedate', operator: 'LTE', value: String(toMsET(to, true)) },
    ],
  }], ['amount','closedate','hs_createdate','utm_source','utm_medium','utm_campaign','utm_content','hubspot_owner_id','brand_status','average_monthly_web_traffic__cloned_','average_monthly_web_traffic']);
}

// Deals by hs_createdate — for Demo Quality table (shows deals created/booked in the window)
async function fetchDealsByCreateDate(token, from, to) {
  const fMs = String(toMsET(from)), tMs = String(toMsET(to, true));
  const deals = await hsSearch(token, 'deals', [
    { filters: [
      { propertyName: 'hs_createdate', operator: 'GTE', value: fMs },
      { propertyName: 'hs_createdate', operator: 'LTE', value: tMs },
      { propertyName: 'date_demo_booked', operator: 'HAS_PROPERTY' },
    ]},
  ], ['dealname','date_demo_booked','demo_given_date','demo_given__status','demo_attendance_status','demo_qualification_outcome','dealstage','amount','closedate','hs_createdate','hubspot_owner_id','utm_source','utm_medium','utm_campaign','utm_content','brand_status'], 100);
  return [...new Map(deals.map(d => [d.id, d])).values()];
}

async function fetchOwners(token) {
  const map = {};
  let after = '';
  for (let p = 0; p < 10; p++) {
    const r = await fetch(`https://api.hubapi.com/crm/v3/owners?limit=100${after?'&after='+after:''}`, { headers: { 'Authorization': `Bearer ${token}` } });
    if (!r.ok) break;
    const d = await r.json();
    for (const o of (d.results||[])) map[o.id] = [o.firstName, o.lastName].filter(Boolean).join(' ') || o.email || o.id;
    if (d.paging?.next?.after) after = d.paging.next.after; else break;
  }
  return map;
}

// Batch fetch contact website URLs for an array of deals
// ---------------------------------------------------------------------------
// Processing — Ad Spend (guide Section 1)
// ---------------------------------------------------------------------------
function processAdSpend(rows, linkedInDemoOverride) {
  const ch = {};
  for (const c of DASH_CHANNELS) ch[c] = { spend:0, clicks:0, impressions:0, windsorDemos:0 };
  let tSpend=0, tClicks=0, tImpr=0, tDemos=0;
  const spendByDay = {};  // date (YYYY-MM-DD) → total spend across all channels — powers True CPD per week

  for (const row of rows) {
    const ds = (row.datasource||'').toLowerCase();
    const camp = (row.campaign_name||'').toLowerCase();
    if (/googleanalytics/.test(ds)) continue;

    const isYT = /\byt\b|youtube/i.test(camp);
    let key;
    if (/facebook|meta|fb|ig|instagram/.test(ds)) key = 'meta';
    else if (/linkedin/.test(ds)) key = 'linkedin';
    else if (/tiktok/.test(ds)) key = 'tiktok';
    else if (/google/.test(ds)) key = isYT ? 'youtube' : 'google';
    else continue;

    const spend = parseFloat(row.spend)||0;
    const clicks = parseInt(row.clicks)||0;
    const impr = parseInt(row.impressions)||0;

    let demos = 0;
    if (key === 'meta') {
      // Allowlisted lead-gen campaigns count Leads (Form) toward Demos.
      demos = (parseInt(row.conversions_submit_application_total)||0) + metaLeadDemos(row.campaign_name, row);
    } else if (key === 'linkedin') {
      demos = parseInt(row.externalwebsiteconversions)||0; // overridden below
    } else if (key === 'tiktok') {
      demos = parseInt(row.conversions)||0;
    } else {
      demos = Math.ceil(parseFloat(row.conversions)||0);
    }

    ch[key].spend += spend; ch[key].clicks += clicks; ch[key].impressions += impr; ch[key].windsorDemos += demos;
    tSpend += spend; tClicks += clicks; tImpr += impr; tDemos += demos;
    if (row.date) spendByDay[row.date] = (spendByDay[row.date]||0) + spend;
  }

  // LinkedIn demo override (guide Section 1)
  if (linkedInDemoOverride != null) {
    tDemos = tDemos - ch.linkedin.windsorDemos + linkedInDemoOverride;
    ch.linkedin.windsorDemos = linkedInDemoOverride;
  }

  // CTR = clicks/impressions * 100 for all channels
  for (const c of DASH_CHANNELS) {
    ch[c].ctr = ch[c].impressions > 0 ? (ch[c].clicks / ch[c].impressions) * 100 : 0;
  }

  return { total: { spend:tSpend, clicks:tClicks, impressions:tImpr, windsorDemos:tDemos }, channels: ch, spendByDay };
}

// ---------------------------------------------------------------------------
// Processing — GA4 (guide Section 2)
// ---------------------------------------------------------------------------
function processGA4(rows) {
  let users=0, sessions=0, demoClicks=0, ga4Booked=0;
  for (const r of rows) {
    if (r.datasource && r.datasource !== 'googleanalytics4') continue;
    users += parseInt(r.users)||0;
    sessions += parseInt(r.sessions)||0;
    demoClicks += parseInt(r.conversions_click_schedule_demo_button)||0;
    ga4Booked += parseInt(r.conversions_hubspot_meeting_booked)||0;
  }
  return { users, sessions, demoClicks, ga4Booked };
}

// ---------------------------------------------------------------------------
// Processing — Scheduled Contacts (guide Section 3)
// ---------------------------------------------------------------------------
function processScheduledContacts(contacts) {
  const byDay = {};
  const byDayScale = {};         // per-day count excluding pre-launch
  const byDayScale10KPlus = {};  // per-day count excluding pre-launch AND 0-10K (Irfan-only stricter view)
  const byWebTraffic = {};       // count by raw web-traffic tier value (Livestorm → "Very Small"/Webinar) — webinar count for CPD cards
  const byWebTrafficTrue = {};   // count by the contact's ACTUAL web-traffic tier (webinar regs counted in their real tier, NOT lumped into "Webinar") — drives the Demos+Webinars pie
  let lowTrafficCount = 0;       // contacts in the pre-launch tier
  let low10KCount = 0;           // contacts in pre-launch OR 0-10K tier (denominator for the Irfan stricter chart)
  let dailyTotal = 0;            // total excluding Very Small (= Webinar tier)
  // Weekday-only totals — match the day-of-week filter the dashboard's
  // "Weekday Avg" tile uses. Without these, the dashboard had to divide the
  // full-week total by the weekday-only day count, inflating prior/LM avgs
  // any time a contact happened to book on a Sat/Sun.
  let weekdayTotal = 0;                  // excludes Very Small + weekends
  let weekdayTotalScale = 0;             // excludes Very Small + Pre-launch + weekends
  let weekdayTotalScale10KPlus = 0;      // excludes Very Small + Pre-launch + 0-10K + weekends
  let lowTrafficCountWeekday = 0;        // Pre-launch contacts that landed on a weekday
  let low10KCountWeekday = 0;            // Pre-launch + 0-10K contacts on a weekday
  // Dedupe: fetchScheduledContacts ORs two filter groups (meeting bookers +
  // Livestorm Webinar registrants), so a contact could appear twice if they
  // booked AND registered for a webinar. Key by id and skip dupes.
  const seenIds = new Set();
  // Set of normalized company names + email domains flagged as Pre-launch.
  // Used downstream by processPipelineDeals to compute the scale-tier QDG count.
  const lowTrafficCompanies = new Set();
  for (const c of contacts) {
    const cd = c.properties?.createdate;
    if (!cd) continue;
    if (c.id && seenIds.has(c.id)) continue;
    if (c.id) seenIds.add(c.id);
    const wtRaw = c.properties?.average_monthly_web_traffic || '';
    const wt = wtRaw.toLowerCase();
    // "Webinar" tier = either (a) raw web-traffic value is "Very Small" OR
    // (b) the contact came in via the Livestorm Webinars source. Either
    // signal maps the contact into the Webinar slice on the pie. Per spec,
    // Webinar contacts are EXCLUDED from byDay / byDayScale / total — they're
    // a separate funnel and shouldn't pollute per-day/avg metrics.
    const isLivestormWebinar = (c.properties?.hs_object_source_detail_1 || '') === 'Livestorm Webinars';
    const isVerySmall = wt === 'very small' || isLivestormWebinar;
    const tierKey = isLivestormWebinar ? 'Very Small' : (wtRaw || '(none)');
    byWebTraffic[tierKey] = (byWebTraffic[tierKey]||0) + 1;
    // True-tier bucket for the pie: use the contact's real web-traffic tier so
    // pre-launch / 0-10K brands that funnel through webinars land in their own
    // slice instead of the catch-all "Webinar" (Very Small) slice.
    const trueTier = wtRaw || '(none)';
    byWebTrafficTrue[trueTier] = (byWebTrafficTrue[trueTier]||0) + 1;
    if (isVerySmall) continue;
    const d = new Date(cd);
    const o = etOff(d);
    const et = new Date(d.getTime() + o * 3600000);
    const ds = fmt(et);
    // ET-local day of week. `et` has its UTC fields shifted into ET, so
    // getUTCDay() returns the ET-local DOW. 0=Sun, 6=Sat.
    const etDow = et.getUTCDay();
    const isWeekday = etDow !== 0 && etDow !== 6;
    byDay[ds] = (byDay[ds]||0) + 1;
    dailyTotal++;
    if (isWeekday) weekdayTotal++;
    const isLow = wt.includes('pre-launch');
    // The 0-10K tier label is "0-10K monthly web visitors"; lowercase check
    // catches that exact string as well as any future formatting variants.
    const is0to10K = wt.includes('0-10k');
    if (!isLow) {
      byDayScale[ds] = (byDayScale[ds]||0) + 1;
      if (isWeekday) weekdayTotalScale++;
    } else {
      lowTrafficCount++;
      if (isWeekday) lowTrafficCountWeekday++;
      const company = (c.properties?.company || '').trim().toLowerCase();
      if (company) lowTrafficCompanies.add(company);
      const email = (c.properties?.email || '').toLowerCase();
      if (email.includes('@')) {
        const domain = email.split('@')[1];
        const personalDomains = ['gmail.com','yahoo.com','hotmail.com','aol.com','outlook.com','icloud.com','protonmail.com'];
        if (domain && !personalDomains.includes(domain)) {
          lowTrafficCompanies.add('_domain_' + domain);
        }
      }
    }
    // Stricter daily series for the Irfan Dashboard — excludes pre-launch
    // AND 0-10K (the user wants both tiers off the daily chart since they're
    // not in the scale-tier funnel).
    if (isLow || is0to10K) {
      low10KCount++;
      if (isWeekday) low10KCountWeekday++;
    } else {
      byDayScale10KPlus[ds] = (byDayScale10KPlus[ds]||0) + 1;
      if (isWeekday) weekdayTotalScale10KPlus++;
    }
  }
  return {
    total: dailyTotal, byDay, byDayScale, byDayScale10KPlus, byWebTraffic, byWebTrafficTrue,
    lowTrafficCount, low10KCount, lowTrafficCompanies,
    // Weekday-only mirrors of total / low-tier counts, exposed so prior &
    // last-month deltas in the Weekday Avg tile are apples-to-apples (only
    // weekday demos in the numerator, weekday-day-count in the denominator).
    weekdayTotal, weekdayTotalScale, weekdayTotalScale10KPlus,
    lowTrafficCountWeekday, low10KCountWeekday,
  };
}

// ---------------------------------------------------------------------------
// Processing — Pipeline Deals (guide Section 4)
// ---------------------------------------------------------------------------
function processPipelineDeals(deals, winFromUTC, winToUTC, winFromET, winToET, lowTrafficCompanies) {
  const lowSet = lowTrafficCompanies || new Set();
  const windowDeals = deals.filter(d => {
    // Effective date: rescheduled_meeting_date if set, else date_demo_booked.
    // This matches the pattern used by buildMarketingFunnel (L1357) and ensures deals
    // rescheduled INTO the window are counted (e.g. originally booked outside the window
    // but rescheduled in), and deals rescheduled OUT of the window are excluded.
    const ddb = d.properties?.date_demo_booked;
    const rmd = d.properties?.rescheduled_meeting_date;
    const eff = rmd || ddb;
    const effMs = dateMs(eff);
    const cMs = d.properties?.hs_createdate ? parseInt(d.properties.hs_createdate) : NaN;
    const att = (d.properties?.demo_attendance_status||'').trim();
    const ls = (d.properties?.demo_given__status||'').trim();
    const isMissed = att === 'No Show' || att === 'Cancelled before demo'
                  || (!att && (ls === 'No Show' || ls === 'No Showed'));   // legacy fallback
    if (!eff && isMissed)
      return !isNaN(cMs) && cMs >= winFromET && cMs <= winToET;
    return !isNaN(effMs) && effMs >= winFromUTC && effMs <= winToUTC;
  });

  const total = windowDeals.length;
  // Demos to Occur (extended): count of all deals with date_demo_booked in the
  // BROADER fetched window (e.g., full calendar month for MTD). For non-extended
  // windows (last7/lastMonth/custom), this equals total.
  // We re-filter `deals` (the full input) to count any deal with a valid date_demo_booked
  // or rescheduled_meeting_date — irrespective of whether it falls in the narrow window.
  let totalExtended = 0;
  for (const d of deals) {
    const ddb = d.properties?.date_demo_booked;
    const rmd = d.properties?.rescheduled_meeting_date;
    const eff = rmd || ddb;
    const effMs = dateMs(eff);
    if (!isNaN(effMs)) totalExtended++;
  }
  let demosHappened=0, tooEarlyCount=0, tooSmallCount=0, notQualCount=0;
  // New raw counters (from demo_attendance_status / demo_qualification_outcome)
  let demoGivenOrigCount=0, demoGivenReschedCount=0;
  // Scale-tier QDG: Demo Given (orig + resched) where matched contact's web traffic is NOT Pre-launch.
  // Used by Total True CPQD KPI on the Executive Overview page.
  let demoGivenScaleCount=0, noShowScaleCount=0;
  // Stricter scale-tier counter — excludes BOTH pre-launch AND 0-10K AND
  // Very Small (= "small brands"). Uses the deal-level cloned web-traffic
  // property directly so it doesn't depend on contact-side company-name
  // matching. Drives the new True CPD / True CPQD calcs (excl. small brands).
  let demoGivenStrictCount=0;
  // Strict (small-brand-excluded) No Show + Cancelled counts — the canonical
  // % No-Show / % Pruned denominators+numerators exclude Pre-launch + 0-10K +
  // Very Small, matching True CPD/CPQD.
  let noShowStrictCount=0, cancelledStrictCount=0;
  let qualifiedRawCount=0, disqualifiedRawCount=0, notYetEvalCount=0;
  let qualifiedRawScaleCount=0;  // Qualified count excluding Pre-launch brands (denominator for Total True CPQD)
  const SMALL_BRAND_TIERS = new Set(['Pre-launch / just launching','0-10K monthly web visitors','Very Small']);
  let staleScheduledCount=0;  // Scheduled — pending with date_demo_booked in the past
  let cancelledBeforeDemoCount=0;  // Prune Rate numerator: demo_attendance_status = 'Cancelled before demo'
  // Tight-cohort counts for Irfan KPI #3 "% Pruned": only deals where BOTH
  // hs_createdate AND date_demo_booked land in the current window — i.e., the
  // customer clicked Book Demo in the window AND the demo was supposed to
  // happen in the window. This is the cohort Irfan actually cares about for
  // "what % were pruned up front by the team?" — it excludes cross-month
  // bookings that would dilute the rate.
  let cancelledBeforeDemoCountTight=0, pruneDenomTightSettled=0;
  const todayMidnight = new Date(); todayMidnight.setHours(0,0,0,0);
  const todayMs = todayMidnight.getTime();
  const byCat = {}; for (const c of FUNNEL_ORDER) byCat[c] = 0;
  const byRep = {}, byChannel = {};
  for (const c of DASH_CHANNELS) byChannel[c] = { qualified:0, total:0 };
  byChannel['unattributed'] = { qualified:0, total:0 };
  const byDay = {};

  // Stage attribution (built from same windowDeals — guarantees matching totals)
  const STAGES = ['TOF','MOF','BOF','Brand','Referral','Other Paid','Organic / Direct / Unattributed'];
  const byStage = {};
  for (const s of STAGES) byStage[s] = {};
  for (const s of STAGES) { for (const c2 of FUNNEL_ORDER) byStage[s][c2] = 0; byStage[s].total = 0; }

  for (const deal of windowDeals) {
    const p = deal.properties||{};
    const att = (p.demo_attendance_status||'').trim();
    const qo = (p.demo_qualification_outcome||'').trim();
    const rawSt = (p.demo_given__status||'').trim();
    const cat = categorizeDemoStatus(att, qo, rawSt);
    const ownerId = p.hubspot_owner_id || 'unassigned';

    byCat[cat]++;
    if (demoDidHappen(att, rawSt)) demosHappened++;
    // tooEarly/tooSmall counts kept for backward compat (always 0 with new field model since both → Qualified)
    if (rawSt === 'Demo Given, Qualified Company, too early') tooEarlyCount++;
    if (rawSt === 'Demo Given / Qualified / Too Small') tooSmallCount++;
    if (rawSt === 'Not Qualified after the demo' || (att && att.startsWith('Demo Given') && qo === 'Disqualified')) notQualCount++;

    // Raw attendance counts (new field model)
    if (att === 'Demo Given (originally scheduled)') demoGivenOrigCount++;
    else if (att === 'Demo Given (rescheduled)') demoGivenReschedCount++;

    // Pre-launch detection — used by both demo-given and qualified scale counts.
    // Match by dealname.toLowerCase() against the low-traffic company set (joined to scheduled
    // contact's `company` field). If we can't find the dealname, we count it as scale-tier (assume not low-traffic).
    const dn = (p.dealname || '').trim().toLowerCase();
    let isLow = false;
    if (dn && lowSet.has(dn)) isLow = true;
    if (!isLow && dn) {
      // Substring match on company names (handles "Brand X" vs "Brand X Inc")
      for (const lk of lowSet) {
        if (lk.startsWith('_domain_')) continue;  // domain keys are matched separately if we had email
        if (lk.length > 3 && (dn.includes(lk) || lk.includes(dn))) { isLow = true; break; }
      }
    }

    // Scale-tier QDG: Demo Given (orig + resched) excluding pre-launch brands
    if (att === 'Demo Given (originally scheduled)' || att === 'Demo Given (rescheduled)') {
      if (!isLow) demoGivenScaleCount++;
      // Stricter version: also exclude 0-10K + Very Small via the deal's own
      // web-traffic tier. The lowSet path above can miss 0-10K because the
      // contact-side `lowTrafficCompanies` set is pre-launch-only.
      const dealWT = p.average_monthly_web_traffic__cloned_ || '';
      if (!isLow && !SMALL_BRAND_TIERS.has(dealWT)) demoGivenStrictCount++;
    }
    // isSmall = Pre-launch (isLow) OR the deal's cloned tier is 0-10K / Very
    // Small. Drives the canonical % No-Show / % Pruned (small brands excluded).
    const _dealWTsmall = isLow || SMALL_BRAND_TIERS.has(p.average_monthly_web_traffic__cloned_ || '');
    // Scale-tier No Show: No Show excluding pre-launch brands (used to compute Demos Held excl pre-launch)
    if (att === 'No Show') {
      if (!isLow) noShowScaleCount++;
      if (!_dealWTsmall) noShowStrictCount++;   // canonical (excl. small brands)
    }
    // Stale scheduled: Scheduled — pending with EFFECTIVE demo date in the past
    // (data hygiene). Effective = rescheduled_meeting_date when set, else
    // date_demo_booked. A deal rescheduled INTO the future stops counting as
    // stale; one rescheduled into the past starts counting.
    if (att === 'Scheduled — pending') {
      const _staleEff = p.rescheduled_meeting_date || p.date_demo_booked;
      const ddbMs = dateMs(_staleEff);
      if (!isNaN(ddbMs) && ddbMs < todayMs) staleScheduledCount++;
    }

    // Prune Rate numerator: demos cancelled before they happened
    if (att === 'Cancelled before demo') {
      cancelledBeforeDemoCount++;
      if (!_dealWTsmall) cancelledStrictCount++;   // canonical (excl. small brands)
    }

    // Tight-cohort counters — only deals where BOTH date_demo_booked and
    // hs_createdate are in the current window. ddb already passed the
    // windowDeals filter via the effective-date check, but we still need
    // to confirm the original date_demo_booked (not rescheduled_meeting_date)
    // is in range, plus hs_createdate in range.
    //
    // hs_createdate from HubSpot can come back as either a numeric ms string
    // ("1746083592000") OR an ISO datetime ("2026-04-25T10:33:12.000Z"). The
    // previous parseInt path silently returned 2026 (the year) for the ISO
    // form, which failed the ms comparison and zeroed the tight count.
    const _ddbMsT = dateMs(p.date_demo_booked);
    const _hcdRaw = p.hs_createdate;
    let _hcdMsT = NaN;
    if (_hcdRaw) {
      _hcdMsT = /^\d+$/.test(_hcdRaw) ? parseInt(_hcdRaw) : new Date(_hcdRaw).getTime();
    }
    const _isTight = !isNaN(_ddbMsT) && _ddbMsT >= winFromUTC && _ddbMsT <= winToUTC
                   && !isNaN(_hcdMsT) && _hcdMsT >= winFromET && _hcdMsT <= winToET;
    if (_isTight) {
      if (att === 'Demo Given (originally scheduled)' || att === 'Demo Given (rescheduled)' ||
          att === 'No Show' || att === 'Cancelled before demo') {
        pruneDenomTightSettled++;
      }
      if (att === 'Cancelled before demo') cancelledBeforeDemoCountTight++;
    }

    // Raw qualification outcome counts
    if (qo === 'Qualified') {
      qualifiedRawCount++;
      if (!isLow) qualifiedRawScaleCount++;  // Total True CPQD denominator
    }
    else if (qo === 'Disqualified') disqualifiedRawCount++;
    else if (qo === 'Not yet evaluated') notYetEvalCount++;

    // Per-rep
    if (!byRep[ownerId]) { byRep[ownerId] = { total:0 }; for (const c of FUNNEL_ORDER) byRep[ownerId][c] = 0; }
    byRep[ownerId].total++; byRep[ownerId][cat]++;

    // Per-channel via UTM
    const ch = mapUtmToChannel(p.utm_source, p.utm_medium) || 'unattributed';
    if (byChannel[ch]) { byChannel[ch][cat]++; byChannel[ch].total++; }

    // Stage attribution
    const stage = detectFunnelStage(p.utm_source, p.utm_medium, p.utm_campaign);
    byStage[stage][cat]++;
    byStage[stage].total++;

    // Daily chart — bucket by EFFECTIVE demo date so rescheduled demos plot
    // on the day they actually happen, not the original booked day.
    const _dailyEff = p.rescheduled_meeting_date || p.date_demo_booked;
    const ds = _dailyEff ? _dailyEff.substring(0,10) : null;
    if (ds) { if (!byDay[ds]) byDay[ds] = { deals:0, qualified:0 }; byDay[ds].deals++; if (cat==='qualified') byDay[ds].qualified++; }
  }

  // Guide formulas — REDEFINED per new dual-field model
  const demoShowRate = total > 0 ? (demosHappened / total) * 100 : 0;
  const qualifiedCount = byCat.qualified;
  const rescheduledCount = byCat.rescheduled || 0;
  const blanksCountLegacy = byCat.blank;                       // kept for legacy callers
  const noShowCount = byCat.noShow || 0;
  const tooEarlyByCat = byCat.tooEarly || 0;                   // 0 with new field model
  const tooSmallByCat = byCat.tooSmall || 0;                   // 0 with new field model
  const prunedByCat = byCat.pruned || 0;
  const pendingEvalCount = byCat.pendingEval || 0;
  const pendingCount = byCat.pending || 0;

  // Qualification Rate = Qualified ÷ (Qualified + Disqualified)
  // (Was briefly switched to Qualified ÷ Demos Held — reverted per user spec
  // so the rate isn't depressed by demos whose qual outcome is still
  // "Not yet evaluated".)
  const qualRateDenom = qualifiedRawCount + disqualifiedRawCount;
  const qualRateDenomLegacy = qualRateDenom; // kept as alias for downstream readers
  const disqualificationRate = qualRateDenom > 0 ? (disqualifiedRawCount / qualRateDenom) * 100 : 0;
  const qualificationRate = qualRateDenom > 0 ? (qualifiedRawCount / qualRateDenom) * 100 : 0;

  // CANONICAL No Show Rate / Prune Rate — small brands (Pre-launch + 0-10K +
  // Very Small) EXCLUDED, matching True CPD/CPQD. Sourced from
  // demo_attendance_status over settled outcomes only.
  //   held(strict)   = Demo Given orig+resched, excl. small brands
  //   noShow(strict) = No Show, excl. small brands
  //   cancel(strict) = Cancelled before demo, excl. small brands
  const noShowDenom = demoGivenStrictCount + noShowStrictCount;            // held + no-show (excl. small)
  const noShowRate = noShowDenom > 0 ? (noShowStrictCount / noShowDenom) * 100 : 0;
  const pruneDenom = demoGivenStrictCount + noShowStrictCount + cancelledStrictCount;  // + cancelled
  const pruneRate = pruneDenom > 0 ? (cancelledStrictCount / pruneDenom) * 100 : 0;
  // All-tier variants (kept for any legacy reader / diagnostics).
  const noShowDenomAll = demoGivenOrigCount + demoGivenReschedCount + noShowCount;
  const noShowRateAll = noShowDenomAll > 0 ? (noShowCount / noShowDenomAll) * 100 : 0;
  const pruneDenomAll = demoGivenOrigCount + demoGivenReschedCount + noShowCount + cancelledBeforeDemoCount;
  const pruneRateAll = pruneDenomAll > 0 ? (cancelledBeforeDemoCount / pruneDenomAll) * 100 : 0;
  // Scale-tier No Show Rate (pre-launch-only exclusion) — legacy.
  const noShowDenomScale = demoGivenScaleCount + noShowScaleCount;
  const noShowRateScale = noShowDenomScale > 0 ? (noShowScaleCount / noShowDenomScale) * 100 : 0;
  // Tight Prune Rate — same formula but on the tight cohort (hs_createdate AND
  // date_demo_booked both in window). Used by Irfan KPI #3 "% Pruned".
  const pruneRateTight = pruneDenomTightSettled > 0 ? (cancelledBeforeDemoCountTight / pruneDenomTightSettled) * 100 : 0;

  // Blanks (data hygiene) = Scheduled — pending deals where date_demo_booked is in the past
  const blanksCount = staleScheduledCount;

  return {
    total, qualifiedCount, blanksCount, blanksCountLegacy, demosHappened, tooEarlyCount, tooSmallCount, notQualCount,
    rescheduledCount, noShowCount, tooEarlyByCat, tooSmallByCat, prunedByCat,
    pendingEvalCount, pendingCount, qualRateDenom,
    // New raw counts surfaced for dashboards
    totalExtended, demoGivenOrigCount, demoGivenReschedCount, demoGivenScaleCount, demoGivenStrictCount, noShowScaleCount,
    // Strict (small-brand-excluded) numerators for the canonical rates.
    noShowStrictCount, cancelledStrictCount,
    qualifiedRawCount, disqualifiedRawCount, notYetEvalCount, qualifiedRawScaleCount,
    staleScheduledCount, cancelledBeforeDemoCount, pruneDenom,
    cancelledBeforeDemoCountTight, pruneDenomTightSettled, pruneRateTight,
    demoShowRate, qualificationRate, disqualificationRate, noShowRate, noShowDenom, noShowRateScale, noShowDenomScale, pruneRate,
    byCategory: byCat, byRep, byChannel, byDay, byStage,
    stageOrder: STAGES,
  };
}

// ---------------------------------------------------------------------------
// Processing — Closed Won (guide Section 5)
// ---------------------------------------------------------------------------
function processClosedWonDeals(deals) {
  let mrr=0, count=0;
  const byRep = {}, byChannel = {}, byStage = {};
  const cycleDays = [];

  for (const deal of deals) {
    const p = deal.properties||{};
    const amt = parseFloat(p.amount)||0;
    mrr += amt; count++;

    const rep = p.hubspot_owner_id || 'unassigned';
    if (!byRep[rep]) byRep[rep] = { mrr:0, count:0 };
    byRep[rep].mrr += amt; byRep[rep].count++;

    const ch = mapUtmToChannel(p.utm_source, p.utm_medium);
    if (ch) { if (!byChannel[ch]) byChannel[ch] = { mrr:0, count:0 }; byChannel[ch].mrr += amt; byChannel[ch].count++; }

    // Stage attribution for closed won
    const stage = detectFunnelStage(p.utm_source, p.utm_medium, p.utm_campaign);
    if (!byStage[stage]) byStage[stage] = { count:0, mrr:0 };
    byStage[stage].count++; byStage[stage].mrr += amt;

    const closeMs = isoMs(p.closedate), createMs = isoMs(p.hs_createdate);
    if (!isNaN(closeMs) && !isNaN(createMs) && closeMs > createMs) cycleDays.push((closeMs-createMs)/(1000*60*60*24));
  }

  const avgCycleDays = cycleDays.length > 0 ? cycleDays.reduce((a,b)=>a+b,0)/cycleDays.length : null;
  return { mrr, count, byRep, byChannel, byStage, avgCycleDays };
}

// ---------------------------------------------------------------------------
// Multi-Touch Attribution — Funnel Stage × Channel matrix
// ---------------------------------------------------------------------------
function detectFunnelStage(utmSource, utmMedium, utmCampaign) {
  const src = (utmSource || '').toLowerCase().trim();
  const med = (utmMedium || '').toLowerCase().trim();
  const camp = (utmCampaign || '').toLowerCase().trim();
  // No UTM = organic / direct traffic
  if (!src && !camp) return 'Organic / Direct / Unattributed';
  // Referral detection
  if (med === 'referral' || src === 'referral' || med === 'partner') return 'Referral';
  // Campaign-based stage detection
  if (/\bs01\b|tof\b/.test(camp)) return 'TOF';
  if (/\bs02\b|mof\b/.test(camp)) return 'MOF';
  if (/\bs03\b|bof\b|retarget/i.test(camp)) return 'BOF';
  if (/brand/i.test(camp)) return 'Brand';
  // Has UTM but no stage match
  return 'Other Paid';
}

// ---------------------------------------------------------------------------
// Processing — Campaigns
// ---------------------------------------------------------------------------
function rowToDashCh(row) {
  const ds = (row.datasource||'').toLowerCase(), camp = (row.campaign_name||'').toLowerCase();
  if (/googleanalytics/.test(ds)) return null;
  if (/facebook|meta|fb|ig|instagram/.test(ds)) return 'meta';
  if (/linkedin/.test(ds)) return 'linkedin';
  if (/tiktok/.test(ds)) return 'tiktok';
  if (/google/.test(ds)) return /\byt\b|youtube/i.test(camp) ? 'youtube' : 'google';
  return null;
}

function processCampaigns(rows) {
  const map = {};
  for (const row of rows) {
    const ch = rowToDashCh(row);
    if (!ch) continue;
    const name = row.campaign_name || '(no campaign name)';
    const key = `${ch}::${name}`;
    if (!map[key]) map[key] = { channel:ch, name, spend:0, clicks:0, impressions:0 };
    map[key].spend += parseFloat(row.spend)||0;
    map[key].clicks += parseInt(row.clicks)||0;
    map[key].impressions += parseInt(row.impressions)||0;
  }
  return Object.values(map).map(c => ({
    ...c, ctr: c.impressions > 0 ? (c.clicks/c.impressions)*100 : 0,
    channelLabel: CHANNEL_LABELS[c.channel] || c.channel,
  })).sort((a,b) => b.spend - a.spend);
}

// ---------------------------------------------------------------------------
// Sign-Up Rate / Cohorts — exact logic per user spec
// ---------------------------------------------------------------------------
// ALL_DEMO_HAPPENED: statuses where a demo actually happened
const ALL_DEMO_HAPPENED = [
  'Demo Given', 'Demo Given at Rescheduled time',
  'Demo Given, Qualified Company, too early', 'Demo Given / Qualified / Too Small',
  'Not Qualified after the demo',
];

// ── Canonical % Pruned / % No-Show ─────────────────────────────────────────
// One definition used everywhere (Irfan, Detailed, Demo Quality, Sign-Up):
// sourced from demo_attendance_status, over SETTLED outcomes only.
//   held      = Demo Given (originally scheduled) + Demo Given (rescheduled)
//   noShow    = No Show
//   cancelled = Cancelled before demo
//   % No-Show = noShow    ÷ (held + noShow)
//   % Pruned  = cancelled ÷ (held + noShow + cancelled)
// Pending/future demos are excluded from both denominators so the rates
// aren't deflated by demos that haven't reached an outcome yet.
function attBump(o, att) {
  if (att === 'Demo Given (originally scheduled)' || att === 'Demo Given (rescheduled)') o.attHeld++;
  else if (att === 'No Show') o.attNoShow++;
  else if (att === 'Cancelled before demo') o.attCancelled++;
}
// "Small brands" = Pre-launch + 0-10K + Very Small. Lowercase-substring match
// so it works on both the cloned tier string and the live property. The
// canonical % Pruned / % No-Show exclude these (matching True CPD/CPQD).
function isSmallBrandWT(wt) {
  const s = (wt || '').toLowerCase();
  return s.indexOf('pre-launch') >= 0 || s.indexOf('0-10k') >= 0 || s.indexOf('very small') >= 0;
}
function attRates(held, noShow, cancelled) {
  const noShowDenom = held + noShow;
  const pruneDenom = held + noShow + cancelled;
  return {
    pctNoShow: noShowDenom > 0 ? (noShow / noShowDenom) * 100 : 0,
    pctPruned: pruneDenom > 0 ? (cancelled / pruneDenom) * 100 : 0,
    noShowNum: noShow, noShowDenom, prunedNum: cancelled, pruneDenom,
  };
}

function buildSignUpCohorts(allDeals, cohortMonths, ownerMap) {
  // Sign-Up Rate cohorts use the SAME stage-based definition as the Irfan
  // Dashboard's Special #1 "Signed Deals from Last Month" card. For each
  // cohort month (date_demo_booked within month):
  //   demosHeld  = won + appt + demoHappened + dm + cs
  //   signed     = won
  //   pctSigned  = signed ÷ demosHeld
  //   pctPending = (appt + demoHappened + dm + cs) ÷ demosHeld
  //   pctPruned  = notAFit ÷ allBooked
  //   pctNoShow  = noShow ÷ allBooked
  //   avgDaysToClose = mean(closedate − date_demo_booked) across signed deals
  // Per-rep buckets include cntCS / cntDM so the rep table can show
  // "# Contract Sent" and "# Decision Maker" columns explicitly.
  const STAGE_APPT = 'appointmentscheduled';
  const STAGE_DEMO_HAPPENED = '1084214349';
  const STAGE_DM = 'decisionmakerboughtin';
  const STAGE_CS = 'contractsent';
  const STAGE_WON = 'closedwon';
  const STAGE_NO_SHOW = '3453957850';
  const STAGE_NOT_A_FIT = '1062974581';

  // Pre-build empty month buckets
  const emptyBucket = (name) => ({
    name: name || null,
    allBooked: 0,
    cntWon: 0, cntAppt: 0, cntDemoHappened: 0, cntDM: 0, cntCS: 0,
    cntNoShow: 0, cntNotAFit: 0,
    // Attendance-based counts for the canonical % Pruned / % No-Show (settled-
    // outcome denominator), separate from the stage counts above which still
    // drive Signed / Pending columns.
    attHeld: 0, attNoShow: 0, attCancelled: 0,
    signedMrrSum: 0,
    // Pre-launch brands are filtered out from cohort metrics (matches Special #1
    // card behavior — pre-launch demos aren't qualified for sign-up tracking).
    prelaunchExcluded: 0,
    // Avg Days — three variants:
    //   daysToDemo         = date_demo_booked − hs_createdate (any deal, "To Demo")
    //   daysToClose        = closedate − date_demo_booked   (Closed Won only, "Close From Demo")
    //   daysFromCreated    = closedate − hs_createdate      (Closed Won only, "Close From Created")
    // daysToCloseSum is the back-compat name for "From Booked".
    daysToCloseSum: 0, daysToCloseN: 0,
    daysFromCreatedSum: 0, daysFromCreatedN: 0,
    daysToDemoSum: 0, daysToDemoN: 0,
  });
  const buckets = {};
  for (const cm of cohortMonths) {
    // signedByWebTraffic — bucket Closed Won deals by web-traffic tier for the
    // pie chart on each cohort. Cohort-level only (not tracked per rep).
    // dealShadows — minimal per-deal data so the client can recompute cohort
    // metrics when the user filters by Demo Date (instant in-cohort filter).
    buckets[cm.label] = { period: cm, ...emptyBucket(), byRep: {}, signedByWebTraffic: {}, dealShadows: [] };
  }
  // Robust parse for hs_createdate (numeric ms string OR ISO datetime variants)
  const _parseDt = (v) => { if (!v) return NaN; return /^\d+$/.test(v) ? parseInt(v) : new Date(v).getTime(); };

  // Month-key lookup: "2026-03" → cohortMonth label. Routing by the YYYY-MM
  // prefix of date_demo_booked is sufficient — no need for a separate ms range
  // check (the prefix IS the month constraint).
  const monthToLabel = {};
  for (const cm of cohortMonths) monthToLabel[cm.from.slice(0, 7)] = cm.label;

  // Floor a ms timestamp to UTC-midnight so partial-day deltas don't skew
  // the Avg Days to Close metric.
  const _floor = (ms) => { const x = new Date(ms); return Date.UTC(x.getUTCFullYear(), x.getUTCMonth(), x.getUTCDate()); };
  // Today's UTC midnight — used to exclude FUTURE-dated demos from the
  // "held" count. Without this, the current month's Demos Held is inflated
  // by upcoming demos still in stage = appointmentscheduled (they haven't
  // happened yet, but the formula otherwise treats appt-stage as "held").
  // Past months don't have this problem — all dates are <= today.
  const _today = new Date();
  const _todayMs = Date.UTC(_today.getUTCFullYear(), _today.getUTCMonth(), _today.getUTCDate(), 23, 59, 59, 999);

  for (const deal of allDeals) {
    const p = deal.properties || {};
    const ddb = p.date_demo_booked;
    if (!ddb) continue;

    // Route by YYYY-MM prefix — works whether HubSpot returns "2026-04-15"
    // or "2026-04-15T00:00:00.000Z" (DATE props are usually the former, but
    // datetime-shape values do occur in the wild).
    const monthKey = String(ddb).substring(0, 7);
    const label = monthToLabel[monthKey];
    if (!label) continue;

    // ms parse for Avg Days to Close. dateMs handles "YYYY-MM-DD" exactly;
    // isoMs (Date.parse-based) is the fallback for ISO datetime variants.
    let ddbMs = dateMs(ddb);
    if (isNaN(ddbMs)) ddbMs = isoMs(ddb);

    const b = buckets[label];
    const stage = (p.dealstage || '').trim();
    const att = (p.demo_attendance_status || '').trim();
    const amt = parseFloat(p.amount) || 0;
    const oid = p.hubspot_owner_id || 'unassigned';

    if (!b.byRep[oid]) {
      b.byRep[oid] = emptyBucket(ownerMap[oid] || (oid === 'unassigned' ? 'Unassigned' : oid));
    }

    // Exclude pre-launch brands from the cohort (same as Special #1 card).
    // Pre-launch deals don't qualify for sign-up rate tracking — they're
    // prospects, not yet-launched brands ready to certify. Prefer the cloned
    // snapshot (signed-time value) over the live property.
    const wt = (p.average_monthly_web_traffic__cloned_ || p.average_monthly_web_traffic || '').toLowerCase();
    if (wt.indexOf('pre-launch') >= 0) {
      b.prelaunchExcluded++;
      b.byRep[oid].prelaunchExcluded++;
      continue;
    }

    b.allBooked++;
    b.byRep[oid].allBooked++;
    // Attendance-based settled-outcome counts (canonical % Pruned / % No-Show).
    // Small brands (0-10K / Very Small; pre-launch already excluded above) are
    // left out of these so the rates match the rest of the dashboard.
    if (!isSmallBrandWT(wt)) { attBump(b, att); attBump(b.byRep[oid], att); }

    // "To Demo" = date_demo_booked − hs_createdate, across ALL booked deals
    // in the cohort (not just signed). Mirrors the BD Tracker's interpretation.
    {
      const hcdMs = _parseDt(p.hs_createdate);
      if (!isNaN(hcdMs) && !isNaN(ddbMs)) {
        const daysD = (_floor(ddbMs) - _floor(hcdMs)) / 86400000;
        if (daysD >= 0) {
          b.daysToDemoSum += daysD; b.daysToDemoN++;
          b.byRep[oid].daysToDemoSum += daysD; b.byRep[oid].daysToDemoN++;
        }
      }
    }

    // Stash a minimal shadow of this deal on the cohort bucket so the
    // client can filter by Demo Date (date_demo_booked) within the cohort
    // and recompute metrics instantly. Keys are short to keep payload size
    // reasonable (~70 bytes/deal × ~500 deals/cohort ≈ 35KB per cohort).
    b.dealShadows.push({
      d: ddb,                                  // date_demo_booked (YYYY-MM-DD)
      s: stage,                                // dealstage
      at: att,                                 // demo_attendance_status (canonical prune/no-show)
      a: amt,                                  // amount
      cd: p.closedate || '',                   // closedate
      hd: p.hs_createdate || '',               // hs_createdate
      o: oid,                                  // hubspot_owner_id
      w: p.average_monthly_web_traffic__cloned_ || p.average_monthly_web_traffic || '',
    });

    if (stage === STAGE_WON) {
      b.cntWon++; b.byRep[oid].cntWon++;
      b.signedMrrSum += amt; b.byRep[oid].signedMrrSum += amt;
      const cdMs = isoMs(p.closedate);
      if (!isNaN(cdMs)) {
        // From date_demo_booked
        const days = (_floor(cdMs) - _floor(ddbMs)) / 86400000;
        if (days >= 0) {
          b.daysToCloseSum += days; b.daysToCloseN++;
          b.byRep[oid].daysToCloseSum += days; b.byRep[oid].daysToCloseN++;
        }
        // From hs_createdate (the "real" lead-to-close cycle time)
        const hcdMs = _parseDt(p.hs_createdate);
        if (!isNaN(hcdMs)) {
          const daysC = (_floor(cdMs) - _floor(hcdMs)) / 86400000;
          if (daysC >= 0) {
            b.daysFromCreatedSum += daysC; b.daysFromCreatedN++;
            b.byRep[oid].daysFromCreatedSum += daysC; b.byRep[oid].daysFromCreatedN++;
          }
        }
      }
      // Web-traffic tier bucket for the per-cohort pie chart.
      // Prefer the cloned snapshot (signed-time value); fall back to live.
      const wtRaw = p.average_monthly_web_traffic__cloned_ || p.average_monthly_web_traffic || '';
      const tierKey = wtRaw || '(none)';
      b.signedByWebTraffic[tierKey] = (b.signedByWebTraffic[tierKey]||0) + 1;
    } else if (stage === STAGE_APPT) {
      // STAGE_APPT (Appointment Scheduled) is the only ambiguous stage —
      // for past months the demo presumably happened (stage just wasn't
      // updated), for the current month the demo may still be upcoming.
      // Only count toward held if date_demo_booked is on or before today.
      if (!isNaN(ddbMs) && ddbMs <= _todayMs) {
        b.cntAppt++; b.byRep[oid].cntAppt++;
      }
    }
    else if (stage === STAGE_DEMO_HAPPENED)    { b.cntDemoHappened++;  b.byRep[oid].cntDemoHappened++; }
    else if (stage === STAGE_DM)               { b.cntDM++;            b.byRep[oid].cntDM++; }
    else if (stage === STAGE_CS)               { b.cntCS++;            b.byRep[oid].cntCS++; }
    else if (stage === STAGE_NO_SHOW)          { b.cntNoShow++;        b.byRep[oid].cntNoShow++; }
    else if (stage === STAGE_NOT_A_FIT)        { b.cntNotAFit++;       b.byRep[oid].cntNotAFit++; }
  }

  // Derive metric block for a bucket (works for both global cohort + per-rep)
  function deriveMetrics(b) {
    const demosHeld = b.cntWon + b.cntAppt + b.cntDemoHappened + b.cntDM + b.cntCS;
    const cntPending = b.cntAppt + b.cntDemoHappened + b.cntDM + b.cntCS;
    // Canonical % Pruned / % No-Show — attendance ÷ settled outcomes.
    const _ar = attRates(b.attHeld, b.attNoShow, b.attCancelled);
    return {
      allBooked: b.allBooked,
      prelaunchExcluded: b.prelaunchExcluded || 0,
      signed: b.cntWon,
      demosHeld,
      pctSigned:  demosHeld    > 0 ? (b.cntWon     / demosHeld)    * 100 : 0,
      pctPending: demosHeld    > 0 ? (cntPending   / demosHeld)    * 100 : 0,
      pctPruned:  _ar.pctPruned,
      pctNoShow:  _ar.pctNoShow,
      // Numerator/denominator for the canonical rates (for calc subtext).
      prunedNum: _ar.prunedNum, pruneDenom: _ar.pruneDenom,
      noShowNum: _ar.noShowNum, noShowDenom: _ar.noShowDenom,
      attHeld: b.attHeld, attNoShow: b.attNoShow, attCancelled: b.attCancelled,
      stageCounts: {
        won: b.cntWon, appt: b.cntAppt, demoHappened: b.cntDemoHappened,
        dm: b.cntDM, cs: b.cntCS, noShow: b.cntNoShow, notAFit: b.cntNotAFit,
      },
      mrr: b.signedMrrSum,
      newArr: b.signedMrrSum * 12,
      acv: b.cntWon > 0 ? b.signedMrrSum / b.cntWon : 0,
      // From date_demo_booked → closedate ("Close From Demo")
      avgDaysToClose: b.daysToCloseN > 0 ? b.daysToCloseSum / b.daysToCloseN : null,
      avgDaysToCloseN: b.daysToCloseN,
      avgDaysFromBooked: b.daysToCloseN > 0 ? b.daysToCloseSum / b.daysToCloseN : null,
      avgDaysFromBookedN: b.daysToCloseN,
      // From hs_createdate → closedate ("Close From Created")
      avgDaysFromCreated: b.daysFromCreatedN > 0 ? b.daysFromCreatedSum / b.daysFromCreatedN : null,
      avgDaysFromCreatedN: b.daysFromCreatedN,
      // hs_createdate → date_demo_booked ("To Demo"), all deals
      avgDaysToDemo: b.daysToDemoN > 0 ? b.daysToDemoSum / b.daysToDemoN : null,
      avgDaysToDemoN: b.daysToDemoN,
    };
  }

  // Build final cohort objects — current month first, then descending
  const cohorts = [];
  for (const cm of cohortMonths) {
    const b = buckets[cm.label];
    const m = deriveMetrics(b);

    const repData = {};
    for (const [oid, r] of Object.entries(b.byRep)) {
      repData[oid] = { name: r.name, ...deriveMetrics(r) };
    }

    cohorts.push({ period: cm, ...m, signedByWebTraffic: b.signedByWebTraffic||{}, byRep: repData, dealShadows: b.dealShadows||[] });
  }

  return { cohorts };
}

// Dedicated all-time fetches — no date filter, simpler queries = reliable pagination
async function fetchAllClosedWon(token) {
  // Sort by closedate DESC so the newest closures always make it in if we ever hit the page cap.
  // maxPages bumped from 3 to 20 (600 → 4000 deal headroom; verified all-time total is ~599 today).
  // This matches the fix applied to fetchAllQualifiedDeals — both share the same pagination pattern.
  return hsSearch(token, 'deals', [{
    filters: [{ propertyName: 'dealstage', operator: 'EQ', value: 'closedwon' }],
  }], ['amount','closedate','hs_createdate','hubspot_owner_id','brand_status'], 200,
  [{ propertyName: 'closedate', direction: 'DESCENDING' }], 20);
}

async function fetchAllQualifiedDeals(token) {
  // KPI-aligned filter — matches the "Qualified Demos" definition used by the Total CPQD KPI card
  // on the Executive Overview: count of deals where demo_attendance_status is one of the two
  // "Demo Given" variants. The KPI does NOT filter by demo_qualification_outcome (a demo that
  // happened still counts as a Qualified Demo even if later marked Disqualified or Not yet evaluated).
  // See worker.js L598-600 (demoGivenOrigCount/demoGivenReschedCount) and L1052 (cpqdTotal uses qualifiedRawCount).
  //
  // Legacy fallback (deals where demo_attendance_status was never set but legacy demo_given__status is
  // a qualified value) is intentionally OMITTED — verified zero such deals exist in this portal.
  // If/when legacy deals reappear, add a second hsSearch call and merge client-side.
  //
  // Sort by date_demo_booked DESC so newest demos always make it in if we ever hit the page cap.
  // maxPages: 20 → 4000 deal headroom. Verified all-time total is ~1,138.
  return hsSearch(token, 'deals', [{
    filters: [
      { propertyName: 'demo_attendance_status', operator: 'IN', values: ['Demo Given (originally scheduled)', 'Demo Given (rescheduled)'] },
    ],
  }], ['demo_given__status','demo_attendance_status','demo_qualification_outcome','dealstage','hubspot_owner_id','date_demo_booked','rescheduled_meeting_date'], 200,
  [{ propertyName: 'date_demo_booked', direction: 'DESCENDING' }], 20);
}

function buildQuarterlyHistory(closedWonDeals, fromDate, toDate) {
  const fromMs = fromDate ? toMsET(fromDate) : 0;
  const toMs = toDate ? toMsET(toDate, true) : Date.now();
  // Current quarter cap
  const now = new Date();
  const curY = now.getFullYear(), curQ = Math.floor(now.getMonth() / 3) + 1;
  const curSortKey = curY * 10 + curQ;
  const byQ = {};
  for (const deal of closedWonDeals) {
    const p = deal.properties || {};
    const closeMs = isoMs(p.closedate);
    if (isNaN(closeMs)) continue;
    // Filter to match KPI card date range
    if (closeMs < fromMs || closeMs > toMs) continue;
    const d = new Date(closeMs);
    const y = d.getFullYear(), m = d.getMonth();
    const q = Math.floor(m / 3) + 1;
    const sortKey = y * 10 + q;
    // Skip future quarters beyond current
    if (sortKey > curSortKey) continue;
    const key = `Q${q} ${y}`;
    if (!byQ[key]) byQ[key] = { quarter: key, sortKey, count: 0, mrr: 0 };
    byQ[key].count++;
    byQ[key].mrr += parseFloat(p.amount) || 0;
  }
  const quarters = Object.values(byQ).sort((a, b) => a.sortKey - b.sortKey);
  let cumCount = 0, cumMrr = 0;
  for (const q of quarters) {
    cumCount += q.count;
    cumMrr += q.mrr;
    q.cumCount = cumCount;
    q.cumMrr = Math.round(cumMrr * 100) / 100;
    delete q.sortKey;
  }
  return quarters;
}

function buildAllTimeRepStats(closedWonDeals, pipelineDeals, ownerMap) {
  const reps = {};
  const ensureRep = (oid) => {
    if (!reps[oid]) reps[oid] = { name: ownerMap[oid] || (oid === 'unassigned' ? 'Unassigned' : oid), won: 0, mrr: 0, cycleDays: [], qualified: 0, pending: 0 };
    return reps[oid];
  };

  // Pass 1: Closed Won deals — Won count, ARR, Avg Cycle (no demo status filter)
  for (const deal of closedWonDeals) {
    const p = deal.properties || {};
    const oid = p.hubspot_owner_id || 'unassigned';
    const r = ensureRep(oid);
    r.won++;
    r.mrr += parseFloat(p.amount) || 0;
    const closeMs = isoMs(p.closedate), createMs = isoMs(p.hs_createdate);
    if (!isNaN(closeMs) && !isNaN(createMs) && closeMs > createMs) r.cycleDays.push((closeMs - createMs) / (1000 * 60 * 60 * 24));
  }

  // Pass 2: Qualified demo deals (pre-filtered to Demo Given statuses) — denominator + Pending
  for (const deal of pipelineDeals) {
    const p = deal.properties || {};
    const stage = (p.dealstage || '').toLowerCase();
    const oid = p.hubspot_owner_id || 'unassigned';
    const r = ensureRep(oid);
    r.qualified++;
    if (stage !== 'closedwon' && stage !== 'closedlost') r.pending++;
  }

  const result = [];
  for (const [oid, r] of Object.entries(reps)) {
    result.push({
      ownerId: oid, name: r.name,
      won: r.won, mrr: r.mrr, qualified: r.qualified, pending: r.pending,
      signUpRate: r.qualified > 0 ? (r.won / r.qualified) * 100 : 0,
      avgCycleDays: r.cycleDays.length > 0 ? r.cycleDays.reduce((a, v) => a + v, 0) / r.cycleDays.length : null,
    });
  }
  result.sort((a, b) => b.won - a.won);
  return result;
}

// ---------------------------------------------------------------------------
// Build Tile Helper
// ---------------------------------------------------------------------------
function buildTile(value, priorValue, pmValue, definition) {
  const t = { value, definition };
  if (priorValue != null) {
    const nd = value - priorValue;
    const pd = priorValue !== 0 ? ((value-priorValue)/priorValue)*100 : null;
    t.sameTimePrior = { value: priorValue, nominalDelta: r2(nd), percentDelta: pd != null ? r2(pd) : null };
  }
  if (pmValue != null) {
    const nd = value - pmValue;
    const pd = pmValue !== 0 ? ((value-pmValue)/pmValue)*100 : null;
    t.lastMonth = { value: pmValue, nominalDelta: r2(nd), percentDelta: pd != null ? r2(pd) : null };
  }
  return t;
}

function buildFunnelData(byCat, total) {
  const r = {};
  for (const c of FUNNEL_ORDER) { const ct = byCat?.[c]||0; r[c] = { count: ct, pct: total > 0 ? r2((ct/total)*100) : 0 }; }
  r.total = total||0;
  return r;
}

// ---------------------------------------------------------------------------
// Build Response
// ---------------------------------------------------------------------------
function buildResponse(current, prior, priorMonth, isAllTime, ownerMap, windowType) {
  const c = current, p = prior||{}, pm = priorMonth||{};

  // ── Executive Summary ──
  const totalSpend = c.adSpend.total.spend;
  const totalWD = c.adSpend.total.windsorDemos;
  const totalScheduled = c.scheduled.total || 0;
  const totalQual = c.pipeline.qualifiedCount || 0; // legacy — used elsewhere
  // CPQD denominator: Demos Held = Demo Given (orig) + Demo Given (resched) — No Show excluded
  // True CPQD denominator: Demos Held excluding "small brands" (Pre-launch + 0-10K + Very Small)
  const demosHeldCount = (c.pipeline.demoGivenOrigCount||0) + (c.pipeline.demoGivenReschedCount||0);
  const demosHeldScaleCount = (c.pipeline.demoGivenScaleCount||0);
  // Stricter held count — excludes pre-launch + 0-10K + Very Small via the
  // deal-level web traffic property. Falls back to demoGivenScaleCount if
  // the stricter counter isn't populated yet (pre-deploy compat).
  const demosHeldStrictCount = (c.pipeline.demoGivenStrictCount!=null ? c.pipeline.demoGivenStrictCount : demosHeldScaleCount);
  const totalQualified = demosHeldCount;       // CPQD denominator → Demos Held
  const totalQualifiedScale = demosHeldStrictCount; // True CPQD denominator → Demos Held excl. small brands
  const cpdTotal = totalScheduled > 0 ? totalSpend/totalScheduled : null;
  const cpqdTotal = totalQualified > 0 ? totalSpend/totalQualified : null;
  const trueCpqdTotal = totalQualifiedScale > 0 ? totalSpend/totalQualifiedScale : null;

  const pTotalS = p.adSpend?.total?.spend||0;
  const pTotalWD = p.adSpend?.total?.windsorDemos||0;
  const pTotalSch = prior ? (p.scheduled?.total||0) : 0;
  const pTotalQ = prior ? (p.pipeline?.qualifiedCount||0) : 0;
  // CPQD/True CPQD prior period denominators use Demos Held counts (orig + resched, no-show excluded)
  const pTotalQual = prior ? ((p.pipeline?.demoGivenOrigCount||0) + (p.pipeline?.demoGivenReschedCount||0)) : 0;
  const pTotalQualScale = prior ? (p.pipeline?.demoGivenStrictCount!=null ? p.pipeline.demoGivenStrictCount : (p.pipeline?.demoGivenScaleCount||0)) : 0;
  const pmTotalS = pm.adSpend?.total?.spend||0;
  const pmTotalWD = pm.adSpend?.total?.windsorDemos||0;
  const pmTotalSch = priorMonth ? (pm.scheduled?.total||0) : 0;
  const pmTotalQ = priorMonth ? (pm.pipeline?.qualifiedCount||0) : 0;
  const pmTotalQual = priorMonth ? ((pm.pipeline?.demoGivenOrigCount||0) + (pm.pipeline?.demoGivenReschedCount||0)) : 0;
  const pmTotalQualScale = priorMonth ? (pm.pipeline?.demoGivenStrictCount!=null ? pm.pipeline.demoGivenStrictCount : (pm.pipeline?.demoGivenScaleCount||0)) : 0;

  const executiveSummary = {
    totalDemosScheduled: buildTile(c.scheduled.total, p.scheduled?.total??null, pm.scheduled?.total??null, 'Contacts created in period with date_demo_booked set'),
    totalCpd: buildTile(cpdTotal, prior&&pTotalSch>0?pTotalS/pTotalSch:null, priorMonth&&pmTotalSch>0?pmTotalS/pmTotalSch:null, 'Total Ad Spend ÷ New Demos Scheduled'),
    qualificationRate: buildTile(c.pipeline.qualificationRate, p.pipeline?.qualificationRate??null, pm.pipeline?.qualificationRate??null, 'Qualified ÷ (Qualified + Disqualified)'),
    disqualificationRate: buildTile(c.pipeline.disqualificationRate, p.pipeline?.disqualificationRate??null, pm.pipeline?.disqualificationRate??null, 'Disqualified ÷ (Qualified + Disqualified)'),
    totalCpqd: buildTile(cpqdTotal, prior&&pTotalQual>0?pTotalS/pTotalQual:null, priorMonth&&pmTotalQual>0?pmTotalS/pmTotalQual:null, 'Total Ad Spend ÷ Demos Held (Demo Given orig + resched)'),
    totalTrueCpqd: buildTile(trueCpqdTotal, prior&&pTotalQualScale>0?pTotalS/pTotalQualScale:null, priorMonth&&pmTotalQualScale>0?pmTotalS/pmTotalQualScale:null, 'Total Ad Spend ÷ Demos Held excl. small brands (Pre-launch + 0-10K)'),
    totalQualifiedDemos: buildTile(c.pipeline.qualifiedRawCount||0, p.pipeline?.qualifiedRawCount??null, pm.pipeline?.qualifiedRawCount??null, 'Deals with demo_qualification_outcome = Qualified'),
    pruneRate: buildTile(c.pipeline.pruneRate||0, p.pipeline?.pruneRate??null, pm.pipeline?.pruneRate??null, 'Cancelled before demo ÷ (held + no-show + cancelled), excl. small brands (Pre-launch + 0-10K + Very Small)'),
    _meta: { totalSpend, totalWD, totalScheduled, totalQual, totalQualified, totalQualifiedScale, demosHeldCount, demosHeldScaleCount, demosHeldStrictCount, closedWonCount: c.closedWon.count, closedWonMRR: c.closedWon.mrr, pClosedWonCount: p.closedWon?.count??null, pClosedWonMRR: p.closedWon?.mrr??null, pmClosedWonCount: pm.closedWon?.count??null, pmClosedWonMRR: pm.closedWon?.mrr??null, pTotalSpend: pTotalS, pmTotalSpend: pmTotalS, pDemosHeldCount: prior?((p.pipeline?.demoGivenOrigCount||0)+(p.pipeline?.demoGivenReschedCount||0)):null, pmDemosHeldCount: priorMonth?((pm.pipeline?.demoGivenOrigCount||0)+(pm.pipeline?.demoGivenReschedCount||0)):null, pDemosHeldScaleCount: prior?(p.pipeline?.demoGivenScaleCount||0):null, pmDemosHeldScaleCount: priorMonth?(pm.pipeline?.demoGivenScaleCount||0):null },
  };
  // Low-traffic demo metrics
  const lowTraffic = c.scheduled.lowTrafficCount || 0;
  const pLowTraffic = p.scheduled?.lowTrafficCount ?? null;
  const pmLowTraffic = pm.scheduled?.lowTrafficCount ?? null;
  const lowTrafficPct = totalScheduled > 0 ? (lowTraffic / totalScheduled) * 100 : 0;
  const pLowTrafficPct = pLowTraffic != null && (p.scheduled?.total||0) > 0 ? (pLowTraffic / p.scheduled.total) * 100 : null;
  const pmLowTrafficPct = pmLowTraffic != null && (pm.scheduled?.total||0) > 0 ? (pmLowTraffic / pm.scheduled.total) * 100 : null;
  executiveSummary.lowTrafficPct = buildTile(lowTrafficPct, pLowTrafficPct, pmLowTrafficPct, '% of demos from Pre-launch web traffic brands');
  // priors added so the Irfan Daily Activity chart can compute scale-tier
  // (excl. pre-launch) totals for vs-prior / vs-LM deltas.
  executiveSummary.lowTrafficPct._meta = {
    lowTraffic, totalScheduled,
    lowTrafficPrior: pLowTraffic,
    lowTrafficLastMonth: pmLowTraffic,
    // pre-launch + 0-10K combined, current/prior/LM — used by the Irfan
    // Dashboard's "Demos Booked per Day" delta math to subtract BOTH
    // excluded tiers from the prior/LM totals.
    low10K: c.scheduled.low10KCount || 0,
    low10KPrior: p.scheduled?.low10KCount ?? null,
    low10KLastMonth: pm.scheduled?.low10KCount ?? null,
  };
  // True CPD denominator now excludes "small brands" — Pre-launch AND 0-10K
  // AND Very Small. Falls back to pre-launch-only exclusion if the worker
  // hasn't yet computed low10KCount (shouldn't happen post-deploy).
  const _low10K = c.scheduled.low10KCount != null ? c.scheduled.low10KCount : lowTraffic;
  const _pLow10K = p.scheduled?.low10KCount != null ? p.scheduled.low10KCount : pLowTraffic;
  const _pmLow10K = pm.scheduled?.low10KCount != null ? pm.scheduled.low10KCount : pmLowTraffic;
  const qualifiedDemos = totalScheduled - _low10K;
  const trueCpd = qualifiedDemos > 0 ? totalSpend / qualifiedDemos : null;
  const pQualDemos = _pLow10K != null ? (p.scheduled?.total||0) - _pLow10K : null;
  const pTrueCpd = pQualDemos != null && pQualDemos > 0 && pTotalS > 0 ? pTotalS / pQualDemos : null;
  const pmQualDemos = _pmLow10K != null ? (pm.scheduled?.total||0) - _pmLow10K : null;
  const pmTrueCpd = pmQualDemos != null && pmQualDemos > 0 && pmTotalS > 0 ? pmTotalS / pmQualDemos : null;
  executiveSummary.trueCpd = buildTile(trueCpd, pTrueCpd, pmTrueCpd, 'Total Ad Spend ÷ Demos excl. small brands (Pre-launch + 0-10K)');

  // ── Web Performance (guide Section 2 + Section 10 — CVR uses HubSpot demos / users) ──
  const cvr = c.ga4.users > 0 ? (c.scheduled.total / c.ga4.users) * 100 : 0;
  const pCvr = prior && p.ga4?.users > 0 ? (p.scheduled?.total / p.ga4.users) * 100 : null;
  const pmCvr = priorMonth && pm.ga4?.users > 0 ? (pm.scheduled?.total / pm.ga4.users) * 100 : null;

  const webPerformance = {
    visitors: buildTile(c.ga4.users, p.ga4?.users??null, pm.ga4?.users??null, 'Unique users (GA4)'),
    cvr: buildTile(cvr, pCvr, pmCvr, 'Demos Booked (HubSpot) ÷ Website Visitors × 100'),
    _meta: { demosBooked: c.scheduled.total, users: c.ga4.users, demoClicks: c.ga4.demoClicks||0, demoClicksPrior: p.ga4?.demoClicks ?? null, demoClicksLastMonth: pm.ga4?.demoClicks ?? null },
  };

  // ── Ad Spend ──
  const budgets = getBudgetsForMonth(c.period.from);
  const totalBudget = Object.values(budgets).reduce((s,v)=>s+v, 0);
  const adSpend = {
    total: buildTile(c.adSpend.total.spend, p.adSpend?.total?.spend??null, pm.adSpend?.total?.spend??null, 'Sum of all channel spend'),
    channels: {}, budgets, totalBudget,
  };
  adSpend.total.windsorDemos = c.adSpend.total.windsorDemos;
  adSpend.total.priorWindsorDemos = p.adSpend?.total?.windsorDemos ?? null;
  for (const ch of DASH_CHANNELS) {
    adSpend.channels[ch] = {
      label: CHANNEL_LABELS[ch], spend: c.adSpend.channels[ch]?.spend||0,
      windsorDemos: c.adSpend.channels[ch]?.windsorDemos||0,
      ctr: c.adSpend.channels[ch]?.ctr||0,
      budget: budgets[ch]||0,
      pendingConnector: PENDING_CHANNELS.has(ch),
      // Prior-period stats (vs P delta)
      priorSpend: p.adSpend?.channels?.[ch]?.spend??null,
      priorWindsorDemos: p.adSpend?.channels?.[ch]?.windsorDemos??null,
      priorQualified: p.pipeline?.byChannel?.[ch]?.qualified??null,
      priorClosedWon: p.closedWon?.byChannel?.[ch]?.count??null,
      priorClosedWonMRR: p.closedWon?.byChannel?.[ch]?.mrr??null,
      // Prior-month stats (vs LM delta) — previously only spend was exposed,
      // which caused Demos / CPD / Qual / CPQD LM deltas in the Channel
      // Performance table to silently render as "—".
      priorMonthSpend: pm.adSpend?.channels?.[ch]?.spend??null,
      priorMonthWindsorDemos: pm.adSpend?.channels?.[ch]?.windsorDemos??null,
      priorMonthQualified: pm.pipeline?.byChannel?.[ch]?.qualified??null,
      priorMonthClosedWon: pm.closedWon?.byChannel?.[ch]?.count??null,
      priorMonthClosedWonMRR: pm.closedWon?.byChannel?.[ch]?.mrr??null,
      // Current-period outcomes
      qualified: c.pipeline.byChannel[ch]?.qualified||0,
      closedWon: c.closedWon.byChannel[ch]?.count||0,
      closedWonMRR: c.closedWon.byChannel[ch]?.mrr||0,
    };
  }

  // ── Demo Tracking ──
  const demoTracking = {
    totalScheduled: buildTile(c.scheduled.total, p.scheduled?.total??null, pm.scheduled?.total??null, 'Contacts created in period with date_demo_booked set'),
    demosToOccur: buildTile(c.pipeline.totalExtended, p.pipeline?.totalExtended??null, pm.pipeline?.totalExtended??null, 'Total deals scheduled in the time window (incl. future-dated for MTD/YTD)'),
    demosHappened: buildTile(c.pipeline.demosHappened, p.pipeline?.demosHappened??null, pm.pipeline?.demosHappened??null, 'Deals where demo actually happened'),
    qualifiedOccurred: buildTile(c.pipeline.qualifiedCount, p.pipeline?.qualifiedCount??null, pm.pipeline?.qualifiedCount??null, 'Deals where demo was given AND outcome = Qualified'),
    demoShowRate: buildTile(c.pipeline.demoShowRate, p.pipeline?.demoShowRate??null, pm.pipeline?.demoShowRate??null, 'Demos Happened ÷ Demos to Occur × 100'),
    noShowRate: buildTile(c.pipeline.noShowRate, p.pipeline?.noShowRate??null, pm.pipeline?.noShowRate??null, 'No Show ÷ (held + No Show), excl. small brands (Pre-launch + 0-10K + Very Small)'),
    noShowRateScale: buildTile(c.pipeline.noShowRateScale, p.pipeline?.noShowRateScale??null, pm.pipeline?.noShowRateScale??null, 'No Show ÷ (Demo Given orig + Demo Given resched + No Show), excluding pre-launch'),
    _qualMeta: {
      qualCount: c.pipeline.qualifiedCount||0, reschCount: c.pipeline.rescheduledCount||0,
      tooEarlyCount: c.pipeline.tooEarlyByCat||0, tooSmallCount: c.pipeline.tooSmallByCat||0, prunedCount: c.pipeline.prunedByCat||0,
      noShowCount: c.pipeline.noShowCount||0, qualRateDenom: c.pipeline.qualRateDenom||0,
      blanksCount: c.pipeline.blanksCount||0,
      // New raw counts for redesigned KPI subtext
      demoGivenOrigCount: c.pipeline.demoGivenOrigCount||0,
      demoGivenReschedCount: c.pipeline.demoGivenReschedCount||0,
      qualifiedRawCount: c.pipeline.qualifiedRawCount||0,
      disqualifiedRawCount: c.pipeline.disqualifiedRawCount||0,
      notYetEvalCount: c.pipeline.notYetEvalCount||0,
      noShowDenom: c.pipeline.noShowDenom||0,
      noShowDenomPrior: p.pipeline?.noShowDenom ?? null,
      noShowDenomLastMonth: pm.pipeline?.noShowDenom ?? null,
      noShowCountPrior: p.pipeline?.noShowCount ?? null,
      noShowCountLastMonth: pm.pipeline?.noShowCount ?? null,
      // Scale-tier (non-pre-launch) counters for Irfan Dashboard subtext
      noShowScaleCount: c.pipeline.noShowScaleCount||0,
      noShowDenomScale: c.pipeline.noShowDenomScale||0,
      demoGivenScaleCount: c.pipeline.demoGivenScaleCount||0,
      demoGivenScaleCountPrior: p.pipeline?.demoGivenScaleCount ?? null,
      demoGivenScaleCountLastMonth: pm.pipeline?.demoGivenScaleCount ?? null,
      // Qualified demos EXCLUDING pre-launch — used by the Detailed Dashboard's
      // redefined "Qualified demos" card and "Qualification rate" card.
      qualifiedRawScaleCount: c.pipeline.qualifiedRawScaleCount||0,
      qualifiedRawScaleCountPrior: p.pipeline?.qualifiedRawScaleCount ?? null,
      qualifiedRawScaleCountLastMonth: pm.pipeline?.qualifiedRawScaleCount ?? null,
      staleScheduledCount: c.pipeline.staleScheduledCount||0,
      cancelledBeforeDemoCount: c.pipeline.cancelledBeforeDemoCount||0,
      cancelledBeforeDemoCountPrior: p.pipeline?.cancelledBeforeDemoCount ?? null,
      cancelledBeforeDemoCountLastMonth: pm.pipeline?.cancelledBeforeDemoCount ?? null,
      pruneDenom: c.pipeline.pruneDenom||0,
      pruneDenomPrior: p.pipeline?.pruneDenom ?? null,
      pruneDenomLastMonth: pm.pipeline?.pruneDenom ?? null,
      // Strict (small-brand-excluded) numerators + held count — the canonical
      // % Pruned / % No-Show subtext reads these so it matches the rate, and
      // demoGivenStrictCount is the "held" term in the settled-outcome denom.
      noShowStrictCount: c.pipeline.noShowStrictCount||0,
      cancelledStrictCount: c.pipeline.cancelledStrictCount||0,
      demoGivenStrictCount: c.pipeline.demoGivenStrictCount||0,
      demoGivenStrictCountPrior: p.pipeline?.demoGivenStrictCount ?? null,
      demoGivenStrictCountLastMonth: pm.pipeline?.demoGivenStrictCount ?? null,
      // Tight prune-rate cohort (denominator = deals where BOTH date_demo_booked
      // AND hs_createdate land in the window). Used by Irfan KPI #3.
      cancelledBeforeDemoCountTight: c.pipeline.cancelledBeforeDemoCountTight||0,
      cancelledBeforeDemoCountTightPrior: p.pipeline?.cancelledBeforeDemoCountTight ?? null,
      cancelledBeforeDemoCountTightLastMonth: pm.pipeline?.cancelledBeforeDemoCountTight ?? null,
      pruneDenomTight: c.pipeline.pruneDenomTightSettled||0,
      pruneDenomTightPrior: p.pipeline?.pruneDenomTightSettled ?? null,
      pruneDenomTightLastMonth: pm.pipeline?.pruneDenomTightSettled ?? null,
      pruneRateTight: c.pipeline.pruneRateTight||0,
      pruneRateTightPrior: p.pipeline?.pruneRateTight ?? null,
      pruneRateTightLastMonth: pm.pipeline?.pruneRateTight ?? null,
    },
    blanks: buildTile(c.pipeline.blanksCount, p.pipeline?.blanksCount??null, pm.pipeline?.blanksCount??null, 'Scheduled — pending deals with date_demo_booked in the past'),
    demosPaidPct: buildTile(
      c.scheduled.total > 0 ? (c.adSpend.total.windsorDemos / c.scheduled.total)*100 : 0,
      prior && p.scheduled?.total > 0 ? (p.adSpend?.total?.windsorDemos / p.scheduled.total)*100 : null,
      priorMonth && pm.scheduled?.total > 0 ? (pm.adSpend?.total?.windsorDemos / pm.scheduled.total)*100 : null,
      'Ad Demos ÷ HubSpot Demos Booked × 100'
    ),
    dailyChart: c.pipeline.byDay,
    scheduledByDay: c.scheduled.byDay,
    scheduledByDayScale: c.scheduled.byDayScale,
    // Stricter daily series — excludes pre-launch AND 0-10K. Used by the
    // Irfan Dashboard "Demos Booked per Day" chart.
    scheduledByDayScale10KPlus: c.scheduled.byDayScale10KPlus,
    // Daily total ad spend (all channels) — powers the Irfan "True CPD per Week"
    // chart (weekly spend ÷ weekly demos excl. small brands).
    spendByDay: c.adSpend.spendByDay || {},
    scheduledByWebTraffic: c.scheduled.byWebTraffic,
    scheduledByWebTrafficPrior: prior ? p.scheduled?.byWebTraffic || null : null,
    scheduledByWebTrafficLastMonth: priorMonth ? pm.scheduled?.byWebTraffic || null : null,
    // True-tier breakdown (webinar regs counted by their real web-traffic tier) — drives the pie.
    scheduledByWebTrafficTrue: c.scheduled.byWebTrafficTrue,
    scheduledByWebTrafficTruePrior: prior ? p.scheduled?.byWebTrafficTrue || null : null,
    scheduledByWebTrafficTrueLastMonth: priorMonth ? pm.scheduled?.byWebTrafficTrue || null : null,
    // Weekday-only mirrors of total / scale / scale10KPlus, for current
    // + prior + last-month. The dashboard's "Weekday Avg" tile divides
    // these by the count of weekdays in the corresponding window so the
    // numerator and denominator stay weekday-only on both sides of the
    // delta. Without these, the prior avg was (full-week demos ÷
    // weekday-count) which inflated whenever a weekend booking landed in
    // the prior window.
    scheduledWeekdayTotal: c.scheduled.weekdayTotal,
    scheduledWeekdayTotalPrior: prior ? (p.scheduled?.weekdayTotal ?? null) : null,
    scheduledWeekdayTotalLastMonth: priorMonth ? (pm.scheduled?.weekdayTotal ?? null) : null,
    scheduledWeekdayTotalScale: c.scheduled.weekdayTotalScale,
    scheduledWeekdayTotalScalePrior: prior ? (p.scheduled?.weekdayTotalScale ?? null) : null,
    scheduledWeekdayTotalScaleLastMonth: priorMonth ? (pm.scheduled?.weekdayTotalScale ?? null) : null,
    scheduledWeekdayTotalScale10KPlus: c.scheduled.weekdayTotalScale10KPlus,
    scheduledWeekdayTotalScale10KPlusPrior: prior ? (p.scheduled?.weekdayTotalScale10KPlus ?? null) : null,
    scheduledWeekdayTotalScale10KPlusLastMonth: priorMonth ? (pm.scheduled?.weekdayTotalScale10KPlus ?? null) : null,
  };
  demoTracking.demosPaidPct._meta = { windsorDemos: c.adSpend.total.windsorDemos, demosBooked: c.scheduled.total };

  // ── Cost Per Demo (demos) ──
  const costPerDemo = { total: buildTile(
    c.adSpend.total.windsorDemos > 0 ? c.adSpend.total.spend/c.adSpend.total.windsorDemos : null,
    p.adSpend?.total?.windsorDemos > 0 ? p.adSpend.total.spend/p.adSpend.total.windsorDemos : null,
    pm.adSpend?.total?.windsorDemos > 0 ? pm.adSpend.total.spend/pm.adSpend.total.windsorDemos : null,
    'Total Ad Spend ÷ Total Demos'), channels: {} };
  for (const ch of DASH_CHANNELS) {
    const s = c.adSpend.channels[ch]?.spend||0, d = c.adSpend.channels[ch]?.windsorDemos||0;
    const ps = p.adSpend?.channels?.[ch]?.spend||0, pd = p.adSpend?.channels?.[ch]?.windsorDemos||0;
    const pms = pm.adSpend?.channels?.[ch]?.spend||0, pmd = pm.adSpend?.channels?.[ch]?.windsorDemos||0;
    costPerDemo.channels[ch] = buildTile(d>0?s/d:null, pd>0?ps/pd:null, pmd>0?pms/pmd:null, `${CHANNEL_LABELS[ch]} Spend ÷ ${CHANNEL_LABELS[ch]} Demos`);
    costPerDemo.channels[ch].label = CHANNEL_LABELS[ch];
    costPerDemo.channels[ch]._meta = { spend: s, demos: d };
  }
  costPerDemo.total._meta = { spend: c.adSpend.total.spend, demos: c.adSpend.total.windsorDemos };

  // ── Quality Funnel ──
  const qualityFunnel = {
    overall: buildFunnelData(c.pipeline.byCategory, c.pipeline.total),
    priorOverall: prior ? buildFunnelData(p.pipeline?.byCategory, p.pipeline?.total) : null,
    byRep: {}, categories: FUNNEL_ORDER, labels: FUNNEL_LABELS, colors: FUNNEL_COLORS,
  };
  for (const [rid, rd] of Object.entries(c.pipeline.byRep)) {
    qualityFunnel.byRep[rid] = { name: ownerMap[rid]||rid, ...buildFunnelData(rd, rd.total) };
  }

  // ── CPQD (UTM-attributed qualified demos — guide Section 6) ──
  const totalUtmQual = DASH_CHANNELS.reduce((s,ch) => s + (c.pipeline.byChannel[ch]?.qualified||0), 0);
  const pTotalUtmQual = prior ? DASH_CHANNELS.reduce((s,ch) => s + (p.pipeline?.byChannel?.[ch]?.qualified||0), 0) : 0;
  const pmTotalUtmQual = priorMonth ? DASH_CHANNELS.reduce((s,ch) => s + (pm.pipeline?.byChannel?.[ch]?.qualified||0), 0) : 0;

  const costPerQualifiedDemo = {
    total: buildTile(totalUtmQual>0?c.adSpend.total.spend/totalUtmQual:null, prior&&pTotalUtmQual>0?(p.adSpend?.total?.spend||0)/pTotalUtmQual:null, priorMonth&&pmTotalUtmQual>0?(pm.adSpend?.total?.spend||0)/pmTotalUtmQual:null, 'Total Ad Spend ÷ Total UTM-Attributed Qualified Demos'),
    channels: {},
  };
  costPerQualifiedDemo.total._meta = { spend: c.adSpend.total.spend, qualifiedDemos: totalUtmQual };
  for (const ch of DASH_CHANNELS) {
    const s = c.adSpend.channels[ch]?.spend||0, q = c.pipeline.byChannel[ch]?.qualified||0;
    const ps = p.adSpend?.channels?.[ch]?.spend||0, pq = p.pipeline?.byChannel?.[ch]?.qualified||0;
    const pms = pm.adSpend?.channels?.[ch]?.spend||0, pmq = pm.pipeline?.byChannel?.[ch]?.qualified||0;
    costPerQualifiedDemo.channels[ch] = buildTile(q>0?s/q:null, pq>0?ps/pq:null, pmq>0?pms/pmq:null, `${CHANNEL_LABELS[ch]} Spend ÷ Qualified Demos from ${CHANNEL_LABELS[ch]}`);
    costPerQualifiedDemo.channels[ch].label = CHANNEL_LABELS[ch];
    costPerQualifiedDemo.channels[ch]._meta = { spend: s, qualifiedDemos: q };
  }

  // ── MRR / ARR (guide Section 5) ──
  const mrrArr = {
    mrr: buildTile(c.closedWon.mrr, p.closedWon?.mrr??null, pm.closedWon?.mrr??null, 'Sum of amount on closed-won deals (by closedate)'),
    dealCount: buildTile(c.closedWon.count, p.closedWon?.count??null, pm.closedWon?.count??null, 'Deals with dealstage = closedwon (by closedate)'),
    avgCycleDays: c.closedWon.avgCycleDays,
    closedWonByRep: c.closedWon.byRep,
    closedWonByChannel: c.closedWon.byChannel,
  };

  // ── Sign-Up Rate ──
  const signUpRate = c.signUpRate || null;

  // ── Campaigns ──
  const campaignPerformance = c.campaigns || [];

  // Data quality check
  const dq = {
    windsorAds: c.adSpend.total.spend > 0 || c.adSpend.total.windsorDemos > 0,
    ga4: c.ga4.users > 0,
    hubspotContacts: c.scheduled.total > 0,
    hubspotDeals: c.pipeline.total > 0,
    hubspotClosedWon: true, // 0 closed won is valid
    priorData: prior ? (p.adSpend?.total?.spend > 0 || p.scheduled?.total > 0) : null,
    priorMonthData: priorMonth ? (pm.adSpend?.total?.spend > 0 || pm.scheduled?.total > 0) : null,
  };
  dq.complete = dq.windsorAds && dq.ga4 && dq.hubspotContacts && dq.hubspotDeals;

  return {
    period: c.period, priorPeriod: prior ? p.period : null, priorMonthPeriod: priorMonth ? pm.period : null, isAllTime,
    executiveSummary, webPerformance, adSpend, demoTracking, costPerDemo,
    qualityFunnel, costPerQualifiedDemo, mrrArr, signUpRate, campaignPerformance,
    ownerMap,
    quarterlyHistory: c.quarterlyHistory || null,
    marketingFunnel: c.marketingFunnel || null,
    multiTouchAttribution: (() => {
      const stages = c.pipeline.stageOrder || [];
      const byStage = c.pipeline.byStage || {};
      const cwByStage = c.closedWon.byStage || {};
      const total = c.pipeline.total || 0;
      const summary = stages.map(s => {
        const sd = byStage[s] || { total:0 };
        const cws = cwByStage[s] || { count:0, mrr:0 };
        // Use actual FUNNEL_ORDER category keys (qualified, tooEarly, pruned, noShow, rescheduled, blank)
        const row = { stage: s, total: sd.total, pct: total > 0 ? (sd.total / total) * 100 : 0, won: cws.count, mrr: cws.mrr };
        for (const cat of FUNNEL_ORDER) row[cat] = sd[cat] || 0;
        return row;
      }).filter(s => s.total > 0);
      return { summary, stages, totalDeals: total, funnelOrder: FUNNEL_ORDER, funnelLabels: FUNNEL_LABELS };
    })(),
    meta: { generatedAt: new Date().toISOString(), funnelDataAvailableFrom: '2026-02-01', adDataAvailableFrom: WINDSOR_EPOCH, dataQuality: dq, windowType },
  };
}

// ---------------------------------------------------------------------------
// Build Period Data
// ---------------------------------------------------------------------------
function buildPeriodData(period, windsorRows, linkedInDemos, ga4Rows, scheduledContacts, pipelineDeals, closedWonDeals) {
  const winFromUTC = toMsUTC(period.from), winToUTC = toMsUTC(period.to, true);
  const winFromET = toMsET(period.from), winToET = toMsET(period.to, true);
  const scheduled = processScheduledContacts(scheduledContacts);
  const pipeline = processPipelineDeals(pipelineDeals, winFromUTC, winToUTC, winFromET, winToET, scheduled.lowTrafficCompanies);
  // Strip the Set from scheduled — it doesn't serialize and isn't needed downstream.
  // Include the new stricter daily series (byDayScale10KPlus) + low10KCount
  // so the Irfan Dashboard's "Demos Booked per Day" exclusion + delta math
  // can see them. Without this, c.scheduled.byDayScale10KPlus is undefined
  // in buildResponse and the dashboard falls back to the old pre-launch-only
  // series.
  const scheduledOut = {
    total: scheduled.total, byDay: scheduled.byDay,
    byDayScale: scheduled.byDayScale, byDayScale10KPlus: scheduled.byDayScale10KPlus,
    byWebTraffic: scheduled.byWebTraffic, byWebTrafficTrue: scheduled.byWebTrafficTrue,
    lowTrafficCount: scheduled.lowTrafficCount, low10KCount: scheduled.low10KCount,
    // Weekday-only mirrors — buildResponse forwards these as
    // scheduledWeekdayTotal{,Prior,LastMonth} so the Weekday Avg deltas
    // compare like-for-like.
    weekdayTotal: scheduled.weekdayTotal,
    weekdayTotalScale: scheduled.weekdayTotalScale,
    weekdayTotalScale10KPlus: scheduled.weekdayTotalScale10KPlus,
    lowTrafficCountWeekday: scheduled.lowTrafficCountWeekday,
    low10KCountWeekday: scheduled.low10KCountWeekday,
  };
  return {
    period,
    adSpend: processAdSpend(windsorRows, linkedInDemos),
    ga4: processGA4(ga4Rows),
    scheduled: scheduledOut,
    pipeline,
    closedWon: processClosedWonDeals(closedWonDeals),
    campaigns: processCampaigns(windsorRows),
  };
}

// ---------------------------------------------------------------------------
// Main Processing
// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// Marketing Funnel — Monthly historical table (Jan 2026 onwards)
// ---------------------------------------------------------------------------
async function fetchMonthlyAdSpend(apiKey) {
  const from = '2026-01-01', to = fmt(yesterdayET());
  const configs = {
    meta:     { connector:'facebook', fields:'date,spend', demoField:null },
    linkedin: { connector:'linkedin', fields:'date,spend', demoField:null },
    tiktok:   { connector:'tiktok',   fields:'date,spend', demoField:null },
    google:   { connector:'google_ads', fields:'date,spend', demoField:null },
  };
  const promises = [], keys = [];
  for (const [ch, cfg] of Object.entries(configs)) {
    promises.push(azWindsorFetch(apiKey, cfg.connector, from, to, cfg.fields).catch(() => []));
    keys.push(ch);
  }
  const results = await Promise.all(promises);
  // Bucket by month — total AND per-channel
  const byMonth = {};
  const byChannelMonth = {};
  for (let i = 0; i < keys.length; i++) {
    const ch = keys[i], rows = results[i];
    for (const row of rows) {
      if (!row.date) continue;
      const monthKey = row.date.substring(0, 7); // "2026-01"
      const sp = parseFloat(row.spend) || 0;
      if (!byMonth[monthKey]) byMonth[monthKey] = 0;
      byMonth[monthKey] += sp;
      if (!byChannelMonth[monthKey]) byChannelMonth[monthKey] = {};
      if (!byChannelMonth[monthKey][ch]) byChannelMonth[monthKey][ch] = 0;
      byChannelMonth[monthKey][ch] += sp;
    }
  }
  return { byMonth, byChannelMonth };
}

function buildMarketingFunnel(monthlySpendData, allQualified, allClosedWon) {
  const monthlySpend = monthlySpendData.byMonth || monthlySpendData;
  const channelSpend = monthlySpendData.byChannelMonth || {};
  const startYear = 2026, startMonth = 0; // Jan 2026
  const now = new Date();
  const months = [];

  // Generate month list from Jan 2026 to current month
  for (let y = startYear, m = startMonth; ; ) {
    if (y > now.getFullYear() || (y === now.getFullYear() && m > now.getMonth())) break;
    const key = `${y}-${String(m + 1).padStart(2, '0')}`;
    const label = new Date(Date.UTC(y, m, 1)).toLocaleDateString('en-US', { month: 'short', year: 'numeric', timeZone: 'UTC' });
    months.push({ key, label });
    m++;
    if (m > 11) { m = 0; y++; }
  }

  // Bucket qualified deals by month of EFFECTIVE date (rescheduled if set, else date_demo_booked)
  // — same semantic as Demo Quality / Pre-Demo / Post-Demo tables.
  const qualByMonth = {};
  for (const deal of allQualified) {
    const p = deal.properties || {};
    const eff = p.rescheduled_meeting_date || p.date_demo_booked;
    if (!eff) continue;
    const key = eff.substring(0, 7);
    qualByMonth[key] = (qualByMonth[key] || 0) + 1;
  }

  // Bucket closed-won deals by month of closedate
  const cwByMonth = {};
  const mrrByMonth = {};
  for (const deal of allClosedWon) {
    const p = deal.properties || {};
    if (!p.closedate) continue;
    const cd = new Date(p.closedate);
    const key = `${cd.getFullYear()}-${String(cd.getMonth() + 1).padStart(2, '0')}`;
    cwByMonth[key] = (cwByMonth[key] || 0) + 1;
    mrrByMonth[key] = (mrrByMonth[key] || 0) + (parseFloat(p.amount) || 0);
  }

  // Build rows per month
  const data = months.map(m => {
    const spend = monthlySpend[m.key] || 0;
    const qual = qualByMonth[m.key] || 0;
    const cpqd = qual > 0 ? spend / qual : null;
    const cw = cwByMonth[m.key] || 0;
    const mrr = mrrByMonth[m.key] || 0;
    const cac = cw > 0 ? spend / cw : null;
    const acv = cw > 0 ? mrr / cw : null;
    const arr = mrr * 12;
    return { key: m.key, label: m.label, spend, qualified: qual, cpqd, closedWon: cw, cac, acv, arr, mrr, channels: channelSpend[m.key] || {} };
  });

  // Totals
  const tSpend = data.reduce((s, d) => s + d.spend, 0);
  const tQual = data.reduce((s, d) => s + d.qualified, 0);
  const tCW = data.reduce((s, d) => s + d.closedWon, 0);
  const tMRR = data.reduce((s, d) => s + d.mrr, 0);
  const totals = {
    spend: tSpend, qualified: tQual, cpqd: tQual > 0 ? tSpend / tQual : null,
    closedWon: tCW, cac: tCW > 0 ? tSpend / tCW : null,
    acv: tCW > 0 ? tMRR / tCW : null, arr: tMRR * 12, mrr: tMRR,
  };

  return { months: data, totals };
}

// ── Sign-Up Rate dedicated endpoint ────────────────────────────────────
// The full /api/data handler burns a lot of HubSpot subrequest budget on
// Phase 2 work (cPipe / pmPipe / dqContacts / companyInfoMap batches)
// before getting to the SignUp cohort fetch. By the time fetchCohortDeals
// PerMonth ran, older-month queries (Feb / March) hit the rate-limit wall
// and silently returned [].
//
// This dedicated endpoint has the SAME hsSearch behavior but with a fresh,
// dedicated subrequest budget — there's nothing else competing for it. We
// only do the per-month cohort fetch + owners, then build the cohorts.
// Irfan Special #1 cohort builder — extracted to top-level so both /api/data
// (legacy path) and /api/special1-cohorts can use it. Pure function: filters
// a deal array by date_demo_booked in [fromStr, toStr], excludes pre-launch
// brands, and returns the Closed Won / Demos Held / Pending / Pruned / No
// Show breakdown plus Avg Days to Close stats.
function buildSpecial1Cohort(dealsArr, fromStr, toStr) {
  const STAGE_APPT = 'appointmentscheduled';
  const STAGE_DEMO_HAPPENED = '1084214349';
  const STAGE_DM = 'decisionmakerboughtin';
  const STAGE_CS = 'contractsent';
  const STAGE_WON = 'closedwon';
  const STAGE_NO_SHOW = '3453957850';
  const STAGE_NOT_A_FIT = '1062974581';
  const fromMs = new Date(fromStr+'T00:00:00Z').getTime();
  const toD = new Date(toStr+'T00:00:00Z');
  const toMs = Date.UTC(toD.getUTCFullYear(), toD.getUTCMonth(), toD.getUTCDate(), 23, 59, 59, 999);
  const _today = new Date();
  const _todayMs = Date.UTC(_today.getUTCFullYear(), _today.getUTCMonth(), _today.getUTCDate(), 23, 59, 59, 999);
  const _floor = (ms) => { const x = new Date(ms); return Date.UTC(x.getUTCFullYear(), x.getUTCMonth(), x.getUTCDate()); };
  const _parseDt = (v) => { if (!v) return NaN; return /^\d+$/.test(v) ? parseInt(v) : new Date(v).getTime(); };
  let allBooked = 0, cntWon = 0, cntAppt = 0, cntDemoHappened = 0, cntDM = 0, cntCS = 0;
  let cntNoShow = 0, cntNotAFit = 0, signedMrrSum = 0;
  // Attendance-based counts for canonical % Pruned / % No-Show.
  const _att = { attHeld: 0, attNoShow: 0, attCancelled: 0 };
  let daysFromBookedSum = 0, daysFromBookedN = 0;
  let daysFromCreatedSum = 0, daysFromCreatedN = 0;
  let daysToDemoSum = 0, daysToDemoN = 0;   // hs_createdate → date_demo_booked, all deals
  let prelaunchExcluded = 0;
  for (const d of dealsArr) {
    const p = d.properties || {};
    const ddbMs = dateMs(p.date_demo_booked);
    if (isNaN(ddbMs) || ddbMs < fromMs || ddbMs > toMs) continue;
    const wt = (p.average_monthly_web_traffic__cloned_ || p.average_monthly_web_traffic || '').toLowerCase();
    if (wt.indexOf('pre-launch') >= 0) { prelaunchExcluded++; continue; }
    allBooked++;
    // Canonical prune/no-show exclude small brands (0-10K / Very Small).
    if (!isSmallBrandWT(wt)) attBump(_att, (p.demo_attendance_status || '').trim());
    // "To Demo" — hs_createdate → date_demo_booked, computed across all
    // non-prelaunch booked deals (not gated on STAGE_WON).
    {
      const hcdMs = _parseDt(p.hs_createdate);
      if (!isNaN(hcdMs)) {
        const daysD = (_floor(ddbMs) - _floor(hcdMs)) / 86400000;
        if (daysD >= 0) { daysToDemoSum += daysD; daysToDemoN++; }
      }
    }
    const stage = (p.dealstage||'').trim();
    if (stage === STAGE_WON) {
      cntWon++; signedMrrSum += parseFloat(p.amount)||0;
      const cdMs = isoMs(p.closedate);
      if (!isNaN(cdMs)) {
        const daysB = (_floor(cdMs) - _floor(ddbMs)) / 86400000;
        if (daysB >= 0) { daysFromBookedSum += daysB; daysFromBookedN++; }
        const hcdMs = _parseDt(p.hs_createdate);
        if (!isNaN(hcdMs)) {
          const daysC = (_floor(cdMs) - _floor(hcdMs)) / 86400000;
          if (daysC >= 0) { daysFromCreatedSum += daysC; daysFromCreatedN++; }
        }
      }
    }
    else if (stage === STAGE_APPT) { if (ddbMs <= _todayMs) cntAppt++; }
    else if (stage === STAGE_DEMO_HAPPENED) cntDemoHappened++;
    else if (stage === STAGE_DM) cntDM++;
    else if (stage === STAGE_CS) cntCS++;
    else if (stage === STAGE_NO_SHOW) cntNoShow++;
    else if (stage === STAGE_NOT_A_FIT) cntNotAFit++;
  }
  const demosHeld = cntWon + cntAppt + cntDemoHappened + cntDM + cntCS;
  const cntPending = cntAppt + cntDemoHappened + cntDM + cntCS;
  // Canonical % Pruned / % No-Show — attendance ÷ settled outcomes.
  const _ar = attRates(_att.attHeld, _att.attNoShow, _att.attCancelled);
  return {
    heldScale: demosHeld, signed: cntWon,
    pctSigned: demosHeld > 0 ? (cntWon / demosHeld) * 100 : 0,
    allBooked,
    pctPending: demosHeld > 0 ? (cntPending / demosHeld) * 100 : 0,
    pctPruned:  _ar.pctPruned,
    pctNoShow:  _ar.pctNoShow,
    prunedNum: _ar.prunedNum, pruneDenom: _ar.pruneDenom,
    noShowNum: _ar.noShowNum, noShowDenom: _ar.noShowDenom,
    attHeld: _att.attHeld, attNoShow: _att.attNoShow, attCancelled: _att.attCancelled,
    stageCounts: { won: cntWon, appt: cntAppt, demoHappened: cntDemoHappened, dm: cntDM, cs: cntCS, noShow: cntNoShow, notAFit: cntNotAFit },
    newArr: signedMrrSum * 12,
    acv: cntWon > 0 ? signedMrrSum / cntWon : 0,
    avgDaysFromBooked: daysFromBookedN > 0 ? daysFromBookedSum / daysFromBookedN : null,
    avgDaysFromBookedN: daysFromBookedN,
    avgDaysFromCreated: daysFromCreatedN > 0 ? daysFromCreatedSum / daysFromCreatedN : null,
    avgDaysFromCreatedN: daysFromCreatedN,
    avgDaysToClose: daysFromBookedN > 0 ? daysFromBookedSum / daysFromBookedN : null,
    avgDaysToCloseN: daysFromBookedN,
    // hs_createdate → date_demo_booked ("To Demo")
    avgDaysToDemo: daysToDemoN > 0 ? daysToDemoSum / daysToDemoN : null,
    avgDaysToDemoN: daysToDemoN,
    fromDate: fromStr, toDate: toStr,
    prelaunchExcluded,
  };
}

// Dedicated /api/special1-cohorts handler. Always fetches deals for the
// Last Month + MTD windows directly (independent of any page selector) and
// returns both cohorts. Mirrors the signup-cohorts pattern: fresh fetch
// budget, no cross-contamination with /api/data's other work.
async function processSpecial1Request(env) {
  const hsToken = env.HUBSPOT_TOKEN;
  const _now = new Date();
  // Last Month (prior calendar month — full)
  const _pcmFrom = new Date(Date.UTC(_now.getUTCFullYear(), _now.getUTCMonth()-1, 1));
  const _pcmTo   = new Date(Date.UTC(_now.getUTCFullYear(), _now.getUTCMonth(), 0));
  const _pcmFromStr = fmt(_pcmFrom), _pcmToStr = fmt(_pcmTo);
  // MTD (current month so far)
  const _mtdFrom = new Date(Date.UTC(_now.getUTCFullYear(), _now.getUTCMonth(), 1));
  const _mtdFromStr = fmt(_mtdFrom), _mtdToStr = fmt(_now);
  // One fetch covers both: Last Month start → today.
  const deals = await fetchPipelineDeals(hsToken, _pcmFromStr, _mtdToStr);
  console.log(`Special1 dedicated: ${deals.length} deals fetched for ${_pcmFromStr}..${_mtdToStr}`);
  const priorMonthHeldSigned = buildSpecial1Cohort(deals, _pcmFromStr, _pcmToStr);
  const mtdHeldSigned        = buildSpecial1Cohort(deals, _mtdFromStr, _mtdToStr);
  return {
    priorMonthHeldSigned,
    mtdHeldSigned,
    _meta: { generatedAt: new Date().toISOString(), dealsFetched: deals.length, window: { pcm: [_pcmFromStr, _pcmToStr], mtd: [_mtdFromStr, _mtdToStr] } },
  };
}

async function processSignupRequest(env) {
  const hsToken = env.HUBSPOT_TOKEN;
  // Same cohort-month definition as the main handler — current calendar
  // month plus the three prior months (newest first).
  const yd = todayET();
  const cohortMonths = [];
  for (let i = 0; i <= 3; i++) {
    const s = new Date(Date.UTC(yd.getUTCFullYear(), yd.getUTCMonth()-i, 1));
    const e = i === 0 ? yd : new Date(Date.UTC(yd.getUTCFullYear(), yd.getUTCMonth()-i+1, 0));
    // i===0 is the current calendar month; its `to` is today (not month-end).
    // Flag it so the dashboard can default its demo-date filter to "up to today".
    cohortMonths.push({ from: fmt(s), to: fmt(e), label: s.toLocaleDateString('en-US',{month:'long',year:'numeric',timeZone:'UTC'}), isCurrent: i === 0 });
  }
  // Owners + cohort fetches in parallel — both are cheap and independent.
  const [ownerMap, cohortRes] = await Promise.all([
    fetchOwners(hsToken),
    fetchCohortDealsPerMonth(hsToken, cohortMonths),
  ]);
  const signUpRate = buildSignUpCohorts(cohortRes.union, cohortMonths, ownerMap);
  // Attach per-cohort fetched count + status for the dashboard diagnostic.
  if (signUpRate?.cohorts) {
    for (const c of signUpRate.cohorts) {
      c._fetchedCount = cohortRes.perMonth[c.period.label] ?? null;
      c._fetchStatus = cohortRes.perMonthStatus[c.period.label] ?? null;
    }
  }
  return { signUpRate, _meta: { generatedAt: new Date().toISOString() } };
}

// Dedicated, UNCACHED Revenue Outcome endpoint. Computes current / prior /
// last-month closed-won MRR + count LIVE in its own invocation (fresh
// subrequest budget) — so the Revenue Outcome card's vs-prior / vs-LM deltas
// are always accurate, independent of the heavy (and cached) /api/data fetch
// chain whose late prior fetches get starved by the per-invocation subrequest
// budget. Three small same-filter closed-won fetches; nothing cached.
async function processRevenueOutcome(env, windowType, customFrom, customTo, vsFrom, vsTo) {
  const hsToken = env.HUBSPOT_TOKEN;
  const { current, prior, priorMonth } = computeWindows(windowType, customFrom, customTo, vsFrom, vsTo);
  const [cCW, pCW, pmCW] = await Promise.all([
    fetchClosedWonDeals(hsToken, current.from, current.to),
    prior ? fetchClosedWonDeals(hsToken, prior.from, prior.to) : Promise.resolve(null),
    priorMonth ? fetchClosedWonDeals(hsToken, priorMonth.from, priorMonth.to) : Promise.resolve(null),
  ]);
  const tot = (deals) => { if (!deals) return null; const a = filterActiveBrands(deals); let m = 0; for (const d of a) m += parseFloat(d.properties && d.properties.amount) || 0; return { mrr: m, count: a.length }; };
  return {
    current: tot(cCW), prior: tot(pCW), priorMonth: tot(pmCW),
    period: current, priorPeriod: prior, priorMonthPeriod: priorMonth,
    generatedAt: new Date().toISOString(),
  };
}

async function processRequest(windowType, customFrom, customTo, env, vsFrom, vsTo) {
  const apiKey = env.WINDSOR_API_KEY, hsToken = env.HUBSPOT_TOKEN;
  const { current, prior, priorMonth } = computeWindows(windowType, customFrom, customTo, vsFrom, vsTo);
  const isAllTime = windowType === 'allTime';
  const yd = todayET();

  // Sign-up cohort months: current month first, then 3 months back (descending)
  const cohortMonths = [];
  for (let i = 0; i <= 3; i++) {
    const s = new Date(Date.UTC(yd.getUTCFullYear(), yd.getUTCMonth()-i, 1));
    const e = i === 0 ? yd : new Date(Date.UTC(yd.getUTCFullYear(), yd.getUTCMonth()-i+1, 0));
    cohortMonths.push({ from: fmt(s), to: fmt(e), label: s.toLocaleDateString('en-US',{month:'long',year:'numeric',timeZone:'UTC'}) });
  }
  // Full cohort window: oldest start → today
  const cohortStart = cohortMonths[cohortMonths.length - 1].from;
  const cohortEnd = fmt(yd);

  // ── Phase 1: Fire all Windsor calls in parallel (no rate limit issue) ──
  // Clamp ad platform dates to WINDSOR_EPOCH (ad data doesn't exist before Nov 2025). GA4 uses full range.
  const windsorPromises = [fetchWindsorAds(apiKey, wFrom(current.from), current.to), fetchLinkedInDemos(apiKey, wFrom(current.from), current.to), fetchGA4(apiKey, current.from, current.to), fetchMonthlyAdSpend(apiKey)];
  if (prior) windsorPromises.push(fetchWindsorAds(apiKey, wFrom(prior.from), prior.to), fetchLinkedInDemos(apiKey, wFrom(prior.from), prior.to), fetchGA4(apiKey, prior.from, prior.to));
  if (priorMonth) windsorPromises.push(fetchWindsorAds(apiKey, wFrom(priorMonth.from), priorMonth.to), fetchLinkedInDemos(apiKey, wFrom(priorMonth.from), priorMonth.to), fetchGA4(apiKey, priorMonth.from, priorMonth.to));
  const windsorResults = await Promise.all(windsorPromises);
  let wIdx = 0;
  const cW = windsorResults[wIdx++], cLI = windsorResults[wIdx++], cG = windsorResults[wIdx++], monthlyAdSpend = windsorResults[wIdx++];
  let pW, pLI, pG; if (prior) { pW = windsorResults[wIdx++]; pLI = windsorResults[wIdx++]; pG = windsorResults[wIdx++]; }
  let pmW, pmLI, pmG; if (priorMonth) { pmW = windsorResults[wIdx++]; pmLI = windsorResults[wIdx++]; pmG = windsorResults[wIdx++]; }

  // Irfan KPI #5 — Disqualification Form submissions + form views.
  // HubSpot's /analytics/v2/reports/forms/total?start=&end=&f=<id> returns
  // the same formViews/submissions/conversion shown in HubSpot's Forms
  // Performance UI — use that as the authoritative source. GA4 pageviews
  // on /not-supported run in parallel as a fallback if HubSpot fails.
  const DQ_PAGE_PATH = '/not-supported';
  let _irfanDqForm = null, _irfanDqFormErr = null;
  try {
    const [dqForm, ga4Views] = await Promise.all([
      fetchDisqualificationFormSubmissions(hsToken, current.from, current.to),
      fetchGA4PageViews(apiKey, current.from, current.to, DQ_PAGE_PATH),
    ]);
    _irfanDqForm = dqForm;
    if (typeof _irfanDqForm.views === 'number') {
      _irfanDqForm.viewsSource = 'hubspot_form_analytics';
    } else if (ga4Views && typeof ga4Views.views === 'number') {
      // HubSpot failed; fall back to GA4.
      _irfanDqForm.views = ga4Views.views;
      _irfanDqForm.viewsSource = 'ga4_via_windsor_fallback';
      _irfanDqForm.ga4Matched = ga4Views.matched;
      _irfanDqForm.ga4PagePath = DQ_PAGE_PATH;
    } else {
      _irfanDqForm.viewsSource = null;
      if (ga4Views?.error) _irfanDqForm.ga4Error = ga4Views.error;
    }
    console.log(`Irfan DQ form: ${_irfanDqForm.count} submissions, ${_irfanDqForm.views} views (source=${_irfanDqForm.viewsSource||'none'})`);
  } catch(e) {
    _irfanDqFormErr = e.message;
    console.error('Irfan DQ form fetch failed:', e);
  }

  // % Disqualified Routing card series — numerator = /not-supported pageviews
  // ("Clicked Continue but DQ'd"), denominator = cal_routing_submitted events
  // ("Clicked Schedule a Demo and Continue"). Fetched ONCE over the widest of
  // the three windows, then bucketed per window so the card shows vs-prior /
  // vs-LM deltas. cal_routing_submitted only began firing 2026-05-26, so
  // windows entirely before that return 0 (rate → — for those periods).
  let _calRouting = null;
  try {
    const _rFrom = [current.from, prior && prior.from, priorMonth && priorMonth.from].filter(Boolean).sort()[0];
    const _series = await fetchGA4RoutingSeries(apiKey, _rFrom, current.to, DQ_PAGE_PATH, 'cal_routing_submitted');
    const _mk = (w) => w ? { views: _sumDateRange(_series.viewsByDate, w.from, w.to), routed: _sumDateRange(_series.eventByDate, w.from, w.to) } : null;
    _calRouting = { current: _mk(current), prior: _mk(prior), priorMonth: _mk(priorMonth) };
    console.log(`cal_routing: cur=${JSON.stringify(_calRouting.current)} prior=${JSON.stringify(_calRouting.prior)} pm=${JSON.stringify(_calRouting.priorMonth)}`);
  } catch(e) { console.error('cal_routing series failed:', e && e.message); }

  // Irfan Special #2 — MTD signed-deals fetch. Done EARLY (here, not later
  // inside the irfan response block) so it gets subrequest budget before the
  // heavy company/contact batches in Phase 2.
  //   • byClose: hs_is_closed_won=true + closedate in MTD window — pipeline-agnostic
  //   • byEntered: hs_is_closed_won=true + hs_date_entered_closedwon in MTD window —
  //     fallback for default-pipeline deals whose closedate was manually set
  //     outside the MTD window. SKIPPED when current page window is MTD (cCW
  //     + byClose already give full coverage) to conserve subrequests.
  const _irfanMtdToday = new Date();
  const _irfanMtdFrom  = new Date(Date.UTC(_irfanMtdToday.getUTCFullYear(), _irfanMtdToday.getUTCMonth(), 1));
  const _irfanMtdFromStr = fmt(_irfanMtdFrom), _irfanMtdToStr = fmt(_irfanMtdToday);
  const _irfanMtdSkipByEntered = (windowType === 'mtd');
  let _irfanMtdByClose = [], _irfanMtdByEntered = [];
  let _irfanMtdByCloseErr = null, _irfanMtdByEnteredErr = null;
  try {
    const fetches = [
      fetchAnyWonDealsByCloseDate(hsToken, _irfanMtdFromStr, _irfanMtdToStr).catch(e => { _irfanMtdByCloseErr = e.message; return []; }),
    ];
    if (!_irfanMtdSkipByEntered) {
      fetches.push(
        fetchAnyWonDealsByEnteredWon(hsToken, _irfanMtdFromStr, _irfanMtdToStr).catch(e => { _irfanMtdByEnteredErr = e.message; return []; })
      );
    }
    const results = await Promise.all(fetches);
    _irfanMtdByClose = results[0] || [];
    _irfanMtdByEntered = (_irfanMtdSkipByEntered ? [] : (results[1] || []));
    console.log(`Irfan MTD early-fetch: byClose=${_irfanMtdByClose.length} byEntered=${_irfanMtdByEntered.length}${_irfanMtdSkipByEntered?' (skipped, window=mtd)':''} window=${_irfanMtdFromStr}..${_irfanMtdToStr}`);
  } catch(e) {
    console.error('Irfan MTD early-fetch failed:', e);
  }

  // ── Phase 2: Run HubSpot calls sequentially (avoids 429 rate limits) ──
  const cSch = await fetchScheduledContacts(hsToken, current.from, current.to);
  // For Demo Quality: extend pipeline fetch through end of month (processPipelineDeals re-filters to MTD)
  let pipeEndDate = current.to;
  if (windowType === 'mtd' || windowType === 'ytd' || windowType === 'allTime') {
    pipeEndDate = fmt(new Date(Date.UTC(yd.getUTCFullYear(), yd.getUTCMonth() + 1, 0)));
  }
  const cPipe = await fetchPipelineDeals(hsToken, current.from, pipeEndDate);
  console.log(`cPipe: ${cPipe.length} deals (${current.from} to ${pipeEndDate})`);
  // Webinar-stage deals (no demo date) — extra Demo Quality table rows only.
  const cWebinar = await fetchWebinarStageDeals(hsToken, current.from, pipeEndDate);
  console.log(`cWebinar: ${cWebinar.length} webinar-stage deals`);
  const cCW = await fetchClosedWonDeals(hsToken, current.from, current.to);

  // Fetch priorMonth FIRST so we can derive prior from it in-memory when prior
  // is a strict subset (saves 3 HubSpot calls = up to ~15 subrequests).
  let pmSch, pmPipe, pmCW;
  if (priorMonth) {
    // Cache last-month fetches — these windows are PAST and stable, so caching
    // keeps vs-prior / vs-LM deltas reliable even when live HubSpot calls get
    // rate-limited/starved (which was zeroing them out), and cuts load.
    const _pmK = priorMonth.from + '_' + priorMonth.to;
    pmSch = await kvCachedFetchNE(env, 'pm_sch_' + _pmK, 3600, () => fetchScheduledContacts(hsToken, priorMonth.from, priorMonth.to));
    pmPipe = await kvCachedFetchNE(env, 'pm_pipe_' + _pmK, 3600, () => fetchPipelineDeals(hsToken, priorMonth.from, priorMonth.to));
    pmCW = await kvCachedFetchNE(env, 'pm_cw_' + _pmK, 3600, () => fetchClosedWonDeals(hsToken, priorMonth.from, priorMonth.to));
  }

  // For MTD: prior = April 1..(today-of-month) and priorMonth = April 1..30.
  // prior is a strict subset of priorMonth, so we can filter in-memory.
  // For 7d / lastMonth / custom: prior and priorMonth are disjoint windows, fetch normally.
  let pSch, pPipe, pCW;
  const _priorIsSubsetOfPriorMonth = prior && priorMonth
    && prior.from === priorMonth.from
    && prior.to <= priorMonth.to;
  if (prior && _priorIsSubsetOfPriorMonth && pmSch && pmPipe && pmCW) {
    // Derive prior arrays by filtering pm* sets to prior window.
    const _pFromMs = toMsET(prior.from);
    const _pToMs = toMsET(prior.to, true);
    const _pFromMsUTC = toMsUTC(prior.from);
    const _pToMsUTC = toMsUTC(prior.to, true);
    // pSch derivation: filter pmSch (priorMonth's contacts, already
    // fetched with createdate-in-priorMonth) down to those whose createdate
    // also falls in the prior window.
    //
    // CRITICAL: filter by `createdate`, NOT `date_demo_booked`. Two reasons:
    //   1. fetchScheduledContacts itself filters by createdate, so a fresh
    //      prior fetch returns "contacts created in prior window".
    //   2. processScheduledContacts buckets each contact by ET-local
    //      createdate when populating byDay/dailyTotal.
    // The old date_demo_booked filter produced a population mismatch — e.g.
    // a contact created May 20 (outside MTD-prior May 1..5) but with
    // date_demo_booked May 3 would be included, while a contact created
    // May 3 with date_demo_booked May 20 would be excluded. The resulting
    // pSch counts didn't match a fresh same-window fetch, which is exactly
    // the "MTD shows +30 but manual shows +12" symptom users reported.
    // Use ET-anchored bounds (_pFromMs/_pToMs) since createdate is a
    // datetime property and the source fetch uses toMsET().
    pSch = pmSch.filter(c => {
      const crMs = isoMs(c.properties?.createdate);
      return !isNaN(crMs) && crMs >= _pFromMs && crMs <= _pToMs;
    });
    // Match fetchPipelineDeals's three OR filter groups:
    //   (1) date_demo_booked in window (UTC)
    //   (2) rescheduled_meeting_date in window (UTC)
    //   (3) demo_attendance_status IN [No Show, Cancelled before demo] AND hs_createdate in window (ET)
    const _missedStatuses = new Set(['No Show', 'Cancelled before demo']);
    pPipe = pmPipe.filter(d => {
      const p = d.properties || {};
      const dbMs = dateMs(p.date_demo_booked);
      const rmMs = dateMs(p.rescheduled_meeting_date);
      const crMs = isoMs(p.hs_createdate);
      return (!isNaN(dbMs) && dbMs >= _pFromMsUTC && dbMs <= _pToMsUTC)
          || (!isNaN(rmMs) && rmMs >= _pFromMsUTC && rmMs <= _pToMsUTC)
          || (_missedStatuses.has(p.demo_attendance_status) && !isNaN(crMs) && crMs >= _pFromMs && crMs <= _pToMs);
    });
    pCW = pmCW.filter(d => {
      const ms = isoMs(d.properties?.closedate);
      return !isNaN(ms) && ms >= _pFromMs && ms <= _pToMs;
    });
    console.log(`prior derived from priorMonth in-memory (saved 3 fetches): pSch=${pSch.length} pPipe=${pPipe.length} pCW=${pCW.length}`);
  } else if (prior) {
    // Disjoint prior window (7d / lastMonth / custom) — also a stable past
    // period, so cache it (non-empty only) for reliable deltas.
    const _pK = prior.from + '_' + prior.to;
    pSch = await kvCachedFetchNE(env, 'p_sch_' + _pK, 3600, () => fetchScheduledContacts(hsToken, prior.from, prior.to));
    pPipe = await kvCachedFetchNE(env, 'p_pipe_' + _pK, 3600, () => fetchPipelineDeals(hsToken, prior.from, prior.to));
    pCW = await kvCachedFetchNE(env, 'p_cw_' + _pK, 3600, () => fetchClosedWonDeals(hsToken, prior.from, prior.to));
  }

  // ── DQ enrichment (contacts + companies) — moved AFTER the prior/priorMonth
  // fetches so those stable, delta-critical calls claim subrequest budget first.
  // The companies loop below can fire 50-100+ paginated searches; running it
  // last means a budget/rate-limit wall degrades DQ enrichment (graceful) rather
  // than blanking vs-prior / vs-LM deltas (which it was doing).
  // Fetch contacts for Demo Quality — website + engagement data matched by company name
  const dqContacts = await fetchContactsForDQ(hsToken, current.from, pipeEndDate);
  console.log(`dqContacts: ${dqContacts.length} contacts for DQ matching`);
  const contactInfoMap = {}; // keyed by normalized company name
  const dqUniqueDomains = new Set();
  for (const c of dqContacts) {
    const p = c.properties || {};
    const company = (p.company || '').trim().toLowerCase();
    const email = (p.email || '');
    const domain = email.includes('@') ? email.split('@')[1].toLowerCase() : '';
    const info = {
      contactId: c.id,
      name: ((p.firstname||'') + ' ' + (p.lastname||'')).trim(),
      website: p.website || '',
      aboutContact: p.role_at_company || '',
      lastOpen: p.hs_sales_email_last_opened || null,
      lastClick: p.hs_sales_email_last_clicked || null,
      lastReply: p.hs_sales_email_last_replied || null,
      lastContacted: p.notes_last_contacted || null,
      hasOpen: !!p.hs_sales_email_last_opened,
      hasClick: !!p.hs_sales_email_last_clicked,
      hasReply: !!p.hs_sales_email_last_replied,
      inSequence: p.hs_sequences_is_enrolled === 'true',
      sequenceName: p.hs_latest_sequence_enrolled || '',
      webTraffic: p.average_monthly_web_traffic || '',
      slName: p.sl_last_demo_name || '',
      slPct: p.sl_last_demo_completion_percent || '',
      webinarDate: p.webinar_date || '',
      webinarAttended: (p.webinar_has_attended || '').toString().toLowerCase() === 'true',
    };
    if (company) contactInfoMap[company] = info;
    const personalDomains = ['gmail.com','yahoo.com','hotmail.com','aol.com','outlook.com','icloud.com','protonmail.com'];
    if (domain && !personalDomains.includes(domain)) {
      contactInfoMap['_domain_' + domain] = info;
      dqUniqueDomains.add(domain);
    }
  }
  // Build companyInfoMap (industry/timezone/size/web traffic) — keyed by domain + _name_<companyName>
  const companyInfoMap = {};
  const dqDomainArr = [...dqUniqueDomains];
  for (let ci = 0; ci < dqDomainArr.length; ci += 50) {
    const batch = dqDomainArr.slice(ci, ci + 50);
    const companyResults = await hsSearch(hsToken, 'companies', [{
      filters: [{ propertyName: 'domain', operator: 'IN', values: batch }],
    }], ['domain','name','industry','company_time_zone','company_size_bucket','revenue_tier',
         'monthly_visitor_tier','demo_prep_briefing','main_products','target_customer_description','description']);
    for (const co of companyResults) {
      const cp = co.properties || {};
      const d = (cp.domain || '').toLowerCase();
      if (d) companyInfoMap[d] = {
        industry: cp.industry || '', timezone: cp.company_time_zone || '',
        size: cp.company_size_bucket || '', revenue: cp.revenue_tier || '',
        webTraffic: cp.monthly_visitor_tier || '', demoPrep: cp.demo_prep_briefing || '',
        mainProducts: cp.main_products || '', targetCustomer: cp.target_customer_description || '',
        description: cp.description || '',
      };
      const nm = (cp.name || '').trim().toLowerCase();
      if (nm) companyInfoMap['_name_' + nm] = companyInfoMap[d];
    }
  }
  console.log(`Executive DQ: ${Object.keys(companyInfoMap).length} companies matched`);

  // Owners change rarely — cache 30 min so this isn't re-paginated every load.
  const ownerMap = await kvCachedFetch(env, 'owners_cache_v1', 1800, () => fetchOwners(hsToken), {});

  // Sign-Up Rate cohorts are now served by a dedicated /api/signup-cohorts
  // endpoint (processSignupRequest) that has its own fresh HubSpot
  // subrequest budget. Removing the fetch from /api/data frees ~12-20
  // subrequests for the rest of the handler — exactly the budget that
  // older SignUp months used to silently lose.
  //
  // cohortDeals kept as an empty fallback so the Irfan PCM union code
  // (_addAll(cohortDeals)) is a harmless no-op. April PCM data still
  // comes from pmPipe; cohortDeals was only adding cross-month
  // redundancy.
  const cohortDeals = [];

  // All-time deals for rep summary and marketing funnel.
  //
  // Two different filter definitions in play:
  //  • Rep sign-up stats use isQualifiedOpp (demo happened AND qual_outcome = Qualified) — historical.
  //  • Marketing Funnel uses the KPI-aligned definition: demo_attendance_status IN
  //    [Demo Given (originally scheduled), Demo Given (rescheduled)] — matches Total CPQD KPI denom.
  //
  // For allTime window we filter cPipe in-memory (it already covers the full date range).
  // For other windows we issue dedicated queries — fetchAllQualifiedDeals already uses the KPI definition;
  // for rep stats we apply the stricter isQualifiedOpp filter on the result client-side here.
  let allTimeClosedWon, allTimeQualifiedForFunnel, allTimeQualifiedForReps;
  if (isAllTime) {
    allTimeClosedWon = filterActiveBrands(cCW);
    const activePipe = filterActiveBrands(cPipe);
    allTimeQualifiedForFunnel = activePipe.filter(d => {
      const p = d.properties||{};
      return demoDidHappen(p.demo_attendance_status, p.demo_given__status);
    });
    allTimeQualifiedForReps = activePipe.filter(d => {
      const p = d.properties||{};
      return isQualifiedOpp(p.demo_attendance_status, p.demo_qualification_outcome, p.demo_given__status);
    });
    console.log(`allTime: reusing cCW (${allTimeClosedWon.length}), funnel-qualified (${allTimeQualifiedForFunnel.length}), rep-qualified (${allTimeQualifiedForReps.length})`);
  } else {
    // These two scan the ENTIRE deal history (no date bound) and grow with the
    // database — together the single biggest chunk of the handler's subrequest
    // budget on non-AllTime windows. They're window-independent (same result
    // for MTD / Last Month / etc.), so cache 15 min. This is the primary fix
    // for the "Too many subrequests" failures the dashboard started hitting.
    const [allQualKpi, _cw] = await Promise.all([
      kvCachedFetch(env, 'alltime_qualified_v1', 900, () => fetchAllQualifiedDeals(hsToken), []),
      kvCachedFetch(env, 'alltime_closedwon_v1', 900, () => fetchAllClosedWon(hsToken), []),
    ]);
    allTimeClosedWon = _cw;
    allTimeQualifiedForFunnel = allQualKpi;
    // Apply the stricter rep-stats filter on the KPI result set (no extra HubSpot subrequest).
    allTimeQualifiedForReps = allQualKpi.filter(d => {
      const p = d.properties||{};
      return isQualifiedOpp(p.demo_attendance_status, p.demo_qualification_outcome, p.demo_given__status);
    });
    console.log(`allTime fetched: cw (${allTimeClosedWon.length}), funnel-qualified (${allTimeQualifiedForFunnel.length}), rep-qualified (${allTimeQualifiedForReps.length})`);
  }

  // ── Build period data (filter out Paused/Churned deals via deal-level brand_status on ALL windows) ──
  const currentData = buildPeriodData(current, cW, cLI, cG, cSch, filterActiveBrands(cPipe), filterActiveBrands(cCW));
  const priorData = prior ? buildPeriodData(prior, pW, pLI, pG, pSch, filterActiveBrands(pPipe), filterActiveBrands(pCW)) : null;
  const priorMonthData = priorMonth ? buildPeriodData(priorMonth, pmW, pmLI, pmG, pmSch, filterActiveBrands(pmPipe), filterActiveBrands(pmCW)) : null;
  // The Revenue Outcome (MRR/ARR) vs-prior & vs-LM deltas read prior/last-month
  // closed-won MRR + count. The dedicated prior fetches run late in the handler
  // and can return empty/partial under the subrequest budget (hsSearch swallows
  // errors and returns []), which made those deltas unreliable / stale even on
  // Refresh. Recompute prior + last-month closed-won MRR + count UNCONDITIONALLY
  // from the KV-cached all-time closed-won set (same dealstage=closedwon filter,
  // active-brands only), sliced to each window by closedate. Past windows are
  // settled so the ≤15-min-cached set is exact for them. Only mrr + count are
  // overridden — byRep / byChannel (Channel Performance attribution) are left as
  // the dedicated fetch produced them. Current stays on its own fresh fetch.
  if (allTimeClosedWon && allTimeClosedWon.length) {
    const _activeAll = filterActiveBrands(allTimeClosedWon);
    const _cwWin = (from, to) => { const lo=toMsET(from), hi=toMsET(to,true); return _activeAll.filter(dd=>{ const ms=isoMs(dd.properties&&dd.properties.closedate); return !isNaN(ms)&&ms>=lo&&ms<=hi; }); };
    const _cwSum = (arr) => arr.reduce((s,dd)=>s+(parseFloat(dd.properties&&dd.properties.amount)||0),0);
    if (priorData && prior) { const a=_cwWin(prior.from, prior.to); priorData.closedWon.mrr=_cwSum(a); priorData.closedWon.count=a.length; }
    if (priorMonthData && priorMonth) { const a=_cwWin(priorMonth.from, priorMonth.to); priorMonthData.closedWon.mrr=_cwSum(a); priorMonthData.closedWon.count=a.length; }
  }

  // Webinar tier ("Very Small") — sourced inline via fetchScheduledContacts
  // which now ORs in the Livestorm Webinars segment. processScheduled-
  // Contacts buckets those contacts into byWebTraffic['Very Small'] (and
  // excludes them from byDay/byDayScale/total per spec). No separate
  // override needed. Surface the final count on _webinarDebug for the
  // Network-tab inspection.
  currentData._webinarDebug = {
    source: 'fetchScheduledContacts OR hs_object_source_detail_1=Livestorm Webinars',
    finalCur: (currentData.scheduled?.byWebTraffic || {})['Very Small'] || 0,
    finalPrior: (priorData?.scheduled?.byWebTraffic || {})['Very Small'] || 0,
    finalPriorMonth: (priorMonthData?.scheduled?.byWebTraffic || {})['Very Small'] || 0,
  };

  // signUpRate is no longer computed in /api/data — the dashboard fetches
  // it from the dedicated /api/signup-cohorts endpoint instead. Skipping
  // the build saves a CPU loop and (more importantly) avoids using the
  // per-request HubSpot subrequest budget on a path no current consumer
  // reads. quarterlyHistory remains here since it's used by Detailed
  // Dashboard's All Time view.
  currentData.quarterlyHistory = isAllTime ? buildQuarterlyHistory(filterActiveBrands(cCW), current.from, current.to) : null;

  // ── Marketing Funnel (monthly historical table) ──
  // Uses KPI-aligned definition: count of deals with demo_attendance_status IN [Demo Given orig, Demo Given resched].
  currentData.marketingFunnel = buildMarketingFunnel(monthlyAdSpend, allTimeQualifiedForFunnel, filterActiveBrands(allTimeClosedWon));

  // ── Demo Quality page: use cPipe directly (already fetched through end of month above) ──
  const _mapDQDeal = (d) => {
    const p = d.properties || {};
    return {
      id: String(d.id),
      dealname: p.dealname || '',
      date_demo_booked: p.date_demo_booked || '',
      demo_given__status: p.demo_given__status || '',
      demo_attendance_status: p.demo_attendance_status || '',
      demo_qualification_outcome: p.demo_qualification_outcome || '',
      rescheduled_meeting_date: p.rescheduled_meeting_date || '',
      disqualification_reason: p.disqualification_reason || '',
      dealstage: p.dealstage || '',
      createdate: p.createdate || '',
      hs_createdate: p.hs_createdate || '',
      hubspot_owner_id: p.hubspot_owner_id || '',
      utm_source: p.utm_source || '',
      utm_medium: p.utm_medium || '',
      utm_campaign: p.utm_campaign || '',
      utm_content: p.utm_content || '',
      // Cloned web-traffic tier — lets the client-side prune/no-show recomputes
      // exclude small brands consistently with the worker (same source field).
      wtCloned: p.average_monthly_web_traffic__cloned_ || '',
      website: '',
    };
  };
  const demoQualityDeals = filterActiveBrands(cPipe).map(_mapDQDeal);
  console.log(`demoQualityDeals: ${demoQualityDeals.length}`);

  // Webinar-stage deals (Webinar Registered / Attended, no demo date) — extra
  // rows for the Demo Quality TABLE only (kept out of demo metrics). Deduped
  // against the demo deals by id.
  const _dqIds = new Set(demoQualityDeals.map(x => x.id));
  const webinarDeals = filterActiveBrands(cWebinar || []).map(_mapDQDeal).filter(x => !_dqIds.has(x.id));
  console.log(`webinarDeals: ${webinarDeals.length}`);

  const resp = buildResponse(currentData, priorData, priorMonthData, isAllTime, ownerMap, windowType);
  resp.demoQualityDeals = demoQualityDeals;
  resp.webinarDeals = webinarDeals;
  resp.contactInfoMap = contactInfoMap;
  resp.companyInfoMap = companyInfoMap;

  // ── Irfan Dashboard — Special Time-Bound Tiles ──
  // Always use fixed windows (last 14 days, prior calendar month), independent of
  // the page time selector. Where possible, reuse already-fetched data instead of
  // making extra HubSpot subrequests (Cloudflare Workers cap at 50/request).
  resp.irfan = {};

  // Irfan KPI #5 — Disqualification Form submissions for the current window.
  // Captured earlier in Phase 2; surface the result/error to the dashboard here.
  if (_irfanDqFormErr) {
    resp.irfan.dqFormError = _irfanDqFormErr;
  } else if (_irfanDqForm) {
    resp.irfan.dqForm = _irfanDqForm;
  }
  // Per-window cal_routing_submitted + /not-supported views for the
  // % Disqualified Routing card (denominator + deltas).
  if (_calRouting) resp.irfan.calRouting = _calRouting;

  // Tile #1 — Signed Deals (toggle: Last Month / MTD).
  // Moved to dedicated /api/special1-cohorts endpoint (processSpecial1Request).
  // The dashboard fetches that separately so the data stays time-bound to its
  // own toggle, regardless of the page-level window selector. No work needed
  // here, saving subrequests for the rest of /api/data.

  // Tile #2 — Closed-won deals in the last 14 days, aggregated by web-traffic tier
  // at signing time (deal property average_monthly_web_traffic__cloned_).
  // Strategy: combine already-fetched closed-won sets (cCW/pCW/pmCW) + always
  // do a fresh dedicated fetch, then take the union. This way reuse helps when
  // subrequest budget is tight, and the fresh fetch is the source of truth
  // for anything outside the dashboard's natural windows.
  try {
    const _today = new Date();
    const _from14 = new Date(_today); _from14.setUTCDate(_from14.getUTCDate()-14);
    const _fromStr14 = fmt(_from14), _toStr14 = fmt(_today);
    // Use midnight UTC bounds so the entire start day (14 days ago) is included
    // regardless of what hour the worker happens to run at.
    const _needFromMs = Date.UTC(_from14.getUTCFullYear(), _from14.getUTCMonth(), _from14.getUTCDate(), 0, 0, 0, 0);
    const _needToMs = Date.UTC(_today.getUTCFullYear(), _today.getUTCMonth(), _today.getUTCDate(), 23, 59, 59, 999);
    // Step 1: collect from already-fetched closed-won sets
    const _combined = new Map();
    const _addAll = (arr) => { if (!arr) return; for (const x of arr) _combined.set(x.id, x); };
    _addAll(cCW); _addAll(pCW); _addAll(pmCW);
    const cachedSourceCounts = { cCW: cCW?.length||0, pCW: pCW?.length||0, pmCW: pmCW?.length||0 };
    // Step 2: also do a dedicated fetch for the 14-day range (handles windows
    // like lastMonth where the existing fetches don't reach today)
    let freshFetched = [];
    try {
      freshFetched = await fetchClosedWonDeals(hsToken, _fromStr14, _toStr14) || [];
    } catch(fe) {
      console.warn('Irfan last-14 fresh fetch failed (will fall back to cached):', fe.message);
    }
    for (const x of freshFetched) _combined.set(x.id, x);
    // Step 3: filter the union by closedate
    const irfanCW14 = [];
    for (const dl of _combined.values()) {
      const cd = dl.properties?.closedate;
      if (!cd) continue;
      const cdMs = isoMs(cd);
      if (!isNaN(cdMs) && cdMs >= _needFromMs && cdMs <= _needToMs) irfanCW14.push(dl);
    }
    const signedLast14ByWebTraffic = {};
    let totalSignedLast14 = 0, totalSignedLast14MRR = 0;
    for (const dl of irfanCW14) {
      const pp = dl.properties || {};
      const wtRaw = pp.average_monthly_web_traffic__cloned_ || pp.average_monthly_web_traffic || '';
      const key = wtRaw || '(none)';
      signedLast14ByWebTraffic[key] = (signedLast14ByWebTraffic[key]||0) + 1;
      totalSignedLast14++;
      totalSignedLast14MRR += parseFloat(pp.amount)||0;
    }
    resp.irfan.signedLast14ByWebTraffic = signedLast14ByWebTraffic;
    resp.irfan.totalSignedLast14 = totalSignedLast14;
    resp.irfan.totalSignedLast14MRR = totalSignedLast14MRR;
    resp.irfan.last14FromDate = _fromStr14;
    resp.irfan.last14ToDate = _toStr14;
    // Diagnostics exposed for in-page debugging
    resp.irfan.last14Debug = {
      cached: cachedSourceCounts,
      fresh: freshFetched.length,
      unionAfterDedupe: _combined.size,
      filtered: irfanCW14.length,
    };
    console.log(`Irfan last-14: cached={cCW:${cachedSourceCounts.cCW},pCW:${cachedSourceCounts.pCW},pmCW:${cachedSourceCounts.pmCW}} fresh=${freshFetched.length} union=${_combined.size} filtered=${irfanCW14.length} signed=${totalSignedLast14} (${_fromStr14} to ${_toStr14}), MRR ${totalSignedLast14MRR}, tiers ${Object.keys(signedLast14ByWebTraffic).join('|')}`);

    // Same shape, but for the CURRENT CALENDAR MONTH (Month-to-Date toggle).
    // Uses the EARLY-FETCHED MTD results (_irfanMtdByClose + _irfanMtdByEntered)
    // captured before Phase 2's heavy company/contact batches so they had full
    // subrequest budget. Unioned with the last-14 cohort as final safety net.
    const _mtdFromStr = _irfanMtdFromStr, _mtdToStr = _irfanMtdToStr;
    const _mtdFromMs = _irfanMtdFrom.getTime();
    const _mtdToMs = Date.UTC(_irfanMtdToday.getUTCFullYear(), _irfanMtdToday.getUTCMonth(), _irfanMtdToday.getUTCDate(), 23, 59, 59, 999);
    // Three-way union:
    //   (1) deals where closedate is in MTD window  → _irfanMtdByClose
    //   (2) deals where hs_date_entered_closedwon is in MTD window → _irfanMtdByEntered
    //   (3) deals from the last-14 union whose closedate is in MTD window  → fallback
    const _mtdMap = new Map();
    for (const x of (_irfanMtdByClose||[]))   _mtdMap.set(x.id, x);
    const _mtdAfterClose = _mtdMap.size;
    for (const x of (_irfanMtdByEntered||[])) _mtdMap.set(x.id, x);
    const _mtdAfterEntered = _mtdMap.size;
    for (const x of _combined.values()) {
      const cd = x.properties?.closedate;
      if (!cd) continue;
      const cdMs = isoMs(cd);
      if (!isNaN(cdMs) && cdMs >= _mtdFromMs && cdMs <= _mtdToMs) _mtdMap.set(x.id, x);
    }
    const signedMtdByWebTraffic = {};
    let totalSignedMtd = 0, totalSignedMtdMRR = 0;
    for (const dl of _mtdMap.values()) {
      const pp = dl.properties || {};
      const wtRaw = pp.average_monthly_web_traffic__cloned_ || pp.average_monthly_web_traffic || '';
      const key = wtRaw || '(none)';
      signedMtdByWebTraffic[key] = (signedMtdByWebTraffic[key]||0) + 1;
      totalSignedMtd++;
      totalSignedMtdMRR += parseFloat(pp.amount)||0;
    }
    resp.irfan.signedMtdByWebTraffic = signedMtdByWebTraffic;
    resp.irfan.totalSignedMtd = totalSignedMtd;
    resp.irfan.totalSignedMtdMRR = totalSignedMtdMRR;
    resp.irfan.mtdFromDate = _mtdFromStr;
    resp.irfan.mtdToDate = _mtdToStr;
    resp.irfan.mtdDebug = {
      byClose: (_irfanMtdByClose||[]).length,
      byEntered: (_irfanMtdByEntered||[]).length,
      enteredAdds: _mtdAfterEntered - _mtdAfterClose,
      unionAdds: _mtdMap.size - _mtdAfterEntered,
      total: _mtdMap.size,
      byCloseError: _irfanMtdByCloseErr,
      byEnteredError: _irfanMtdByEnteredErr,
    };
    console.log(`Irfan MTD (dual-filter early-fetch): byClose=${(_irfanMtdByClose||[]).length} byEntered=${(_irfanMtdByEntered||[]).length} enteredAdds=${_mtdAfterEntered - _mtdAfterClose} unionAdds=${_mtdMap.size - _mtdAfterEntered} total=${_mtdMap.size} signed=${totalSignedMtd} (${_mtdFromStr} to ${_mtdToStr}), MRR ${totalSignedMtdMRR}, tiers ${Object.keys(signedMtdByWebTraffic).join('|')}`);
  } catch(e) {
    console.error('Irfan last-14-days fetch failed:', e);
    resp.irfan.last14Error = e.message;
  }

  return resp;
}

// ---------------------------------------------------------------------------
// Utility
// ---------------------------------------------------------------------------
function r2(n) { return Math.round(n*100)/100; }
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
function effM(cr) { return cr > 0 ? (1 - Math.pow(1 - cr, 12)) / cr : 12; }

// ---------------------------------------------------------------------------
// Analyzer — Per-Connector Windsor + UTM Attribution Join
// ---------------------------------------------------------------------------
const AZ_CONNECTORS = {
  meta:     { connector:'facebook',   demoField:'conversions_submit_application_total', hasFreq:true, thumbField:'thumbnail_url' },
  google:   { connector:'google_ads', demoField:'conversions', hasFreq:false, thumbField:null },
  youtube:  { connector:'google_ads', demoField:'conversions', hasFreq:false, thumbField:null },
  linkedin: { connector:'linkedin',   demoField:'externalwebsiteconversions', hasFreq:false, thumbField:'creative_thumbnail' },
  tiktok:   { connector:'tiktok',     demoField:'conversions', hasFreq:true, thumbField:null },
};

const AZ_PAGE_SIZE = 5000;
async function azWindsorFetch(apiKey, connector, from, to, fields) {
  const url = `https://connectors.windsor.ai/${connector}?api_key=${apiKey}&date_from=${from}&date_to=${to}&fields=${fields}&page_size=${AZ_PAGE_SIZE}`;
  for (let i = 0; i < 3; i++) {
    try {
      const r = await fetch(url);
      if (!r.ok) throw new Error(`Windsor ${connector} ${r.status}`);
      const j = await r.json(); return j.data || [];
    } catch(e) { if (i < 2) await sleep(1000*(i+1)); else { console.error(`Az Windsor ${connector}:`, e.message); return []; } }
  }
  return [];
}

// Windsor's connectors API has NO offset/page pagination — one request is capped
// at page_size rows (5000). For high-cardinality fetches (Meta creatives come
// back at date × ad × placement × ad-set grain — ~600 rows/DAY) a multi-week
// window blows past 5000 and Windsor silently drops the overflow. It returns
// earliest dates first, so recently-launched creatives disappear from the tables
// entirely (the reported "missing creative" bug). Fetch in date sub-ranges and,
// whenever a range comes back AT the cap (= truncated), bisect it and retry — so
// coverage self-adjusts to however many rows/day the account actually has.
async function azWindsorFetchAll(apiKey, connector, from, to, fields, chunkDays = 30) {
  const addDays = (ds, n) => { const d = new Date(ds + 'T00:00:00Z'); d.setUTCDate(d.getUTCDate() + n); return fmt(d); };
  const daysBetween = (a, b) => Math.round((new Date(b + 'T00:00:00Z') - new Date(a + 'T00:00:00Z')) / 86400000);
  const out = [];
  const stack = [];
  let s = from, seed = 0;
  while (s <= to && seed++ < 800) { let e = addDays(s, chunkDays - 1); if (e > to) e = to; stack.push([s, e]); s = addDays(e, 1); }
  let calls = 0;
  while (stack.length && calls++ < 800) {
    const [a, b] = stack.pop();
    const rows = await azWindsorFetch(apiKey, connector, a, b, fields);
    if (rows.length >= AZ_PAGE_SIZE && a !== b) {
      const mid = addDays(a, Math.floor(daysBetween(a, b) / 2));
      stack.push([a, mid], [addDays(mid, 1), b]);  // truncated → split & retry
    } else {
      out.push(...rows);
    }
  }
  return out;
}

// Normalize campaign status across connectors
// Meta: ACTIVE, PAUSED | Google: ENABLED | LinkedIn: ACTIVE, PAUSED, COMPLETED | TikTok: CAMPAIGN_STATUS_ENABLE, CAMPAIGN_STATUS_DISABLE
function normStatus(raw) {
  const s = (raw||'').toUpperCase().trim();
  if (['ACTIVE','ENABLED','CAMPAIGN_STATUS_ENABLE'].includes(s)) return 'ACTIVE';
  // ADSET_PAUSED / AD_PAUSED = the entity's own status is on, but a parent
  // (ad set / campaign) is paused → not delivering → treat as PAUSED so it
  // doesn't leak through as raw badge text.
  if (['PAUSED','CAMPAIGN_PAUSED','ADSET_PAUSED','AD_PAUSED','CAMPAIGN_STATUS_DISABLE'].includes(s)) return 'PAUSED';
  if (s === 'COMPLETED') return 'COMPLETED';
  if (s === 'REMOVED' || s === 'DELETED' || s === 'ARCHIVED') return 'REMOVED';
  return s || null;
}

// Roll a daily row's status into an entity (campaign / ad set / creative).
// Rule: latest date wins, but ACTIVE wins ties on that date. Windsor returns
// effective_status at the child grain (one row per ad set/ad with its CURRENT
// status), so a campaign with a mix of active + paused children produces both
// ACTIVE and PAUSED rows on the same date. Picking the last-processed row
// arbitrarily flagged delivering campaigns as Paused. Preferring ACTIVE among
// the latest-date rows matches Meta's Delivery column: a parent is "Active"
// if any child is active. obj must carry a `_statusDate` (init '') + `status`.
function rollupStatus(obj, rawUpper, dateStr) {
  if (!rawUpper) return;
  const st = normStatus(rawUpper);
  const d = dateStr || '';
  if (d > (obj._statusDate || '')) { obj.status = st; obj._statusDate = d; }
  else if (d === obj._statusDate && st === 'ACTIVE') { obj.status = 'ACTIVE'; }
}

async function fetchAzCampaigns(apiKey, from, to) {
  const base = 'date,campaign_name,spend,clicks,impressions';
  // LinkedIn per-connector endpoint uses 'campaign' not 'campaign_name'
  const [fbRows, gaRows, liCampRows, liDemoTotal, ttRows] = await Promise.all([
    azWindsorFetchAll(apiKey, 'facebook', from, to, base+',conversions_submit_application_total,frequency,effective_status'+META_LEAD_FIELDS),
    azWindsorFetchAll(apiKey, 'google_ads', from, to, base+',conversions,campaign_status'),
    azWindsorFetchAll(apiKey, 'linkedin', from, to, 'date,campaign,spend,clicks,impressions,campaign_status'),
    fetchLinkedInDemos(apiKey, from, to),
    azWindsorFetchAll(apiKey, 'tiktok', from, to, base+',conversions,frequency,campaign_status'),
  ]);

  function aggRows(rows, ch, cfg, filterFn, demoFilterFn) {
    const camps = {};
    for (const row of rows) {
      if (filterFn && !filterFn(row)) continue;
      const name = row.campaign_name || '(no name)';
      if (!camps[name]) camps[name] = { name, spend:0, clicks:0, impressions:0, demos:0, freqVals:[], dates:[], status:null, _statusDate:'' };
      camps[name].spend += parseFloat(row.spend)||0;
      camps[name].clicks += parseInt(row.clicks)||0;
      camps[name].impressions += parseInt(row.impressions)||0;
      if (row.date) camps[name].dates.push(row.date);
      // Track status (effective_status for Meta, campaign_status for others) —
      // pick the value from the row with the LATEST date so the displayed
      // status always reflects the most recent known state. Last-row-wins
      // produced inconsistent results across windows (e.g. a campaign paused
      // mid-month would show ACTIVE on MTD because an earlier row's status
      // overwrote the final paused row depending on Windsor's row order).
      // Latest date wins, ACTIVE wins ties (see rollupStatus) — so a campaign
      // with both active and paused ad sets reads ACTIVE like Meta's Delivery.
      rollupStatus(camps[name], (row.effective_status || row.campaign_status || '').toUpperCase(), row.date);
      // Only count demos if demoFilterFn passes (or no filter)
      if (!demoFilterFn || demoFilterFn(row)) {
        const rawD = parseFloat(row[cfg.demoField])||0;
        const dConv = (ch==='google'||ch==='youtube') ? Math.ceil(rawD) : Math.round(rawD);
        camps[name].demos += dConv; camps[name].demoConv = (camps[name].demoConv||0) + dConv;
        // Allowlisted Meta lead-gen campaigns: add Leads (Form) to Demos (CAPI Webinar).
        if (ch === 'meta') { const wl = metaLeadDemos(name, row); camps[name].demos += wl; camps[name].webinarLead = (camps[name].webinarLead||0) + wl; }
      }
      if (cfg.hasFreq && row.frequency != null && row.frequency !== '') camps[name].freqVals.push(parseFloat(row.frequency));
    }
    let tS=0,tC=0,tI=0,tD=0,fAll=[];
    for (const c of Object.values(camps)) {
      tS+=c.spend; tC+=c.clicks; tI+=c.impressions; tD+=c.demos;
      c.ctr = c.impressions>0?(c.clicks/c.impressions)*100:0;
      c.cpd = c.demos>0?c.spend/c.demos:null;
      c.frequency = c.freqVals.length?c.freqVals.reduce((a,b)=>a+b,0)/c.freqVals.length:null;
      if (c.freqVals.length) fAll.push(...c.freqVals);
      delete c.freqVals;
      delete c._statusDate;
      // Compute date range
      if (c.dates.length) {
        c.dates.sort();
        c.firstDate = c.dates[0];
        c.lastDate = c.dates[c.dates.length-1];
        const fd = new Date(c.firstDate+'T12:00:00Z'), ld = new Date(c.lastDate+'T12:00:00Z');
        c.activeDays = Math.round((ld-fd)/86400000)+1;
      } else { c.firstDate=null; c.lastDate=null; c.activeDays=0; }
      delete c.dates;
      c.capiConv = capiConvLabel(c.name, null);  // campaign-level (spans its ad sets)
    }
    return { campaigns:camps, totals:{ spend:tS, clicks:tC, impressions:tI, demos:tD, ctr:tI>0?(tC/tI)*100:0, cpd:tD>0?tS/tD:null, frequency:fAll.length?fAll.reduce((a,b)=>a+b,0)/fAll.length:null }};
  }

  const liDemoFilter = r => (r.conversion_name||'').toLowerCase().includes('demo request');

  // LinkedIn: build from spend rows, then overlay demo counts from demo rows
  function aggLinkedIn(rows, totalDemos) {
    const camps = {};
    // Per-connector endpoint returns 'campaign' not 'campaign_name', no 'datasource' field
    for (const row of rows) {
      const name = row.campaign || row.campaign_name || '(no name)';
      if (!camps[name]) camps[name] = { name, spend:0, clicks:0, impressions:0, demos:0, freqVals:[], dates:[], status:null, _statusDate:'' };
      camps[name].spend += parseFloat(row.spend)||0;
      camps[name].clicks += parseInt(row.clicks)||0;
      camps[name].impressions += parseInt(row.impressions)||0;
      if (row.date) camps[name].dates.push(row.date);
      // Latest date wins, ACTIVE wins ties (see rollupStatus).
      rollupStatus(camps[name], (row.campaign_status || '').toUpperCase(), row.date);
    }
    let tS=0,tC=0,tI=0;
    for (const c of Object.values(camps)) {
      tS+=c.spend; tC+=c.clicks; tI+=c.impressions;
      c.ctr = c.impressions>0?(c.clicks/c.impressions)*100:0;
      c.frequency = null; delete c.freqVals;
      delete c._statusDate;
      if (c.dates.length) {
        c.dates.sort(); c.firstDate=c.dates[0]; c.lastDate=c.dates[c.dates.length-1];
        const fd=new Date(c.firstDate+'T12:00:00Z'),ld=new Date(c.lastDate+'T12:00:00Z');
        c.activeDays=Math.round((ld-fd)/86400000)+1;
      } else { c.firstDate=null; c.lastDate=null; c.activeDays=0; }
      delete c.dates;
    }
    const tD = totalDemos || 0;
    for (const c of Object.values(camps)) c.cpd = null;
    return { campaigns:camps, totals:{ spend:tS, clicks:tC, impressions:tI, demos:tD, ctr:tI>0?(tC/tI)*100:0, cpd:tD>0?tS/tD:null, frequency:null }};
  }

  return {
    meta:     aggRows(fbRows, 'meta', AZ_CONNECTORS.meta),
    google:   aggRows(gaRows, 'google', AZ_CONNECTORS.google, r => !/\byt\b|youtube/i.test(r.campaign_name||'')),
    youtube:  aggRows(gaRows, 'youtube', AZ_CONNECTORS.youtube, r => /\byt\b|youtube/i.test(r.campaign_name||'')),
    linkedin: aggLinkedIn(liCampRows, liDemoTotal),
    tiktok:   aggRows(ttRows, 'tiktok', AZ_CONNECTORS.tiktok),
  };
}

// Attempt creative-level fetch (may timeout — graceful fallback)
async function fetchAzCreatives(apiKey, from, to) {
  const base = 'date,campaign_name,ad_name,spend,clicks,impressions';
  const results = {};

  // Build fetch promises for all channels in parallel (NO thumbnail fields - causes size overflow)
  // For Meta we also pull ad_status so the dashboard can distinguish ACTIVE
  // creatives from PAUSED/COMPLETED ones (rather than guessing from spend>0,
  // which mis-labels active creatives that had no impressions in the window).
  const promises = [];
  const channels = [];
  for (const ch of DASH_CHANNELS) {
    const cfg = AZ_CONNECTORS[ch]; if (!cfg) continue;
    if (ch === 'linkedin') {
      promises.push(azWindsorFetch(apiKey, 'linkedin', from, to, 'date,campaign,ad_name,spend,clicks,impressions').catch(e => { console.error(`Creative fetch linkedin:`, e.message); return []; }));
    } else {
      const extra = cfg.demoField === 'conversions_submit_application_total' ? ',conversions_submit_application_total' : ',conversions';
      const freq = cfg.hasFreq ? ',frequency' : '';
      const video = ch === 'meta' ? ',video_p25_watched_actions' : '';
      const placement = ch === 'meta' ? ',publisher_platform,platform_position' : '';
      // effective_status (not ad_status) so an ad whose parent ad set / campaign
      // is OFF reports ADSET_PAUSED / CAMPAIGN_PAUSED (→ PAUSED) instead of its
      // own ACTIVE toggle. ad_status alone made paused ad sets look Active
      // because the ads inside were still toggled on. Keep ad_status as fallback.
      const status = ch === 'meta' ? ',ad_status,effective_status' : '';
      // ad_created_time (Meta) → the creative's launch date, window-independent.
      // Powers the "Status Date" column (Active → launch; Paused → last run).
      const created = ch === 'meta' ? ',ad_created_time' : '';
      // adset_name (Meta) / ad_group_name (Google, TikTok) lets us group
      // creatives under their parent ad set for the Ad Sets table dropdown.
      const adset = ch === 'meta' ? ',adset_name' : ((ch === 'google' || ch === 'tiktok') ? ',ad_group_name' : '');
      // Lead-form fields (Meta) so allowlisted lead-gen campaigns count
      // Leads (Form) toward Demos at the CREATIVE level too — Windsor returns
      // these per ad_name, so each creative gets its own lead count.
      const lead = ch === 'meta' ? META_LEAD_FIELDS : '';
      promises.push(azWindsorFetchAll(apiKey, cfg.connector, from, to, base + extra + freq + video + placement + status + created + adset + lead, 7).catch(e => { console.error(`Creative fetch ${ch}:`, e.message); return []; }));
    }
    channels.push(ch);
  }

  // Separate lightweight thumbnail fetches (ad_name + thumb only, much smaller payload)
  const thumbPromises = [];
  const thumbChannels = [];
  for (const ch of DASH_CHANNELS) {
    const cfg = AZ_CONNECTORS[ch]; if (!cfg || !cfg.thumbField) continue;
    if (ch === 'linkedin') {
      thumbPromises.push(azWindsorFetch(apiKey, 'linkedin', from, to, 'ad_name,'+cfg.thumbField).catch(() => []));
    } else {
      thumbPromises.push(azWindsorFetch(apiKey, cfg.connector, from, to, 'ad_name,'+cfg.thumbField).catch(() => []));
    }
    thumbChannels.push(ch);
  }

  const [allResults, allThumbs] = await Promise.all([Promise.all(promises), Promise.all(thumbPromises)]);

  // Build thumbnail lookup maps
  const thumbMaps = {};
  for (let t = 0; t < thumbChannels.length; t++) {
    const ch = thumbChannels[t];
    const cfg = AZ_CONNECTORS[ch];
    const tm = {};
    for (const row of (allThumbs[t]||[])) {
      const name = row.ad_name;
      if (name && !tm[name] && row[cfg.thumbField]) tm[name] = row[cfg.thumbField];
    }
    thumbMaps[ch] = tm;
  }

  for (let i = 0; i < channels.length; i++) {
    const ch = channels[i];
    const cfg = AZ_CONNECTORS[ch];
    const rows = allResults[i];
    if (!rows || !rows.length) { results[ch] = null; continue; }
    const map = {};
    const campCreMap = {}; // Per-campaign creative breakdown
    const adsetCreMap = {}; // Per-ad-set creative breakdown (Ad Sets dropdown)
    const placementMap = {}; // Placement aggregation (Meta only)
    const tm = thumbMaps[ch] || {};
    for (const row of rows) {
      const campName = row.campaign_name || row.campaign || '';
      const isYT = /\byt\b|youtube/i.test(campName);
      const rCh = /google/.test(cfg.connector) ? (isYT ? 'youtube' : 'google') : ch;
      if (rCh !== ch) continue;
      const name = row.ad_name || '(no creative)';
      // Placement tracking (Meta only — other connectors don't return these fields)
      if (row.publisher_platform || row.platform_position) {
        const plat = row.publisher_platform || 'unknown';
        const pos = row.platform_position || 'unknown';
        const pKey = plat + '|' + pos;
        if (!placementMap[pKey]) placementMap[pKey] = { platform: plat, position: pos, spend:0, clicks:0, impressions:0, demos:0 };
        placementMap[pKey].spend += parseFloat(row.spend)||0;
        placementMap[pKey].clicks += parseInt(row.clicks)||0;
        placementMap[pKey].impressions += parseInt(row.impressions)||0;
        const pDemo = parseFloat(row[cfg.demoField])||0;
        placementMap[pKey].demos += Math.round(pDemo);
        // Per-creative placement breakdown
        if (!map[name]) map[name] = { name, spend:0, clicks:0, impressions:0, demos:0, freqVals:[], thumbnail: tm[name] || null, campaignName: campName, videoP25:0, _dates:[], _placements:{} };
        if (!map[name]._placements[pKey]) map[name]._placements[pKey] = { platform: plat, position: pos, spend:0, clicks:0, impressions:0, demos:0 };
        map[name]._placements[pKey].spend += parseFloat(row.spend)||0;
        map[name]._placements[pKey].clicks += parseInt(row.clicks)||0;
        map[name]._placements[pKey].impressions += parseInt(row.impressions)||0;
        map[name]._placements[pKey].demos += Math.round(pDemo);
      }
      // Flat aggregation
      if (!map[name]) map[name] = { name, spend:0, clicks:0, impressions:0, demos:0, freqVals:[], thumbnail: tm[name] || null, campaignName: campName, videoP25:0, _dates:[], _placements:{}, status:null, _statusDate:'' };
      // Roll up ad_status: latest date wins, ACTIVE wins ties (see rollupStatus).
      // _rowStatus/_rowDate are reused by the per-campaign + per-ad-set maps below.
      const _rowStatus = (row.effective_status || row.ad_status || row.adStatus || '').toString().toUpperCase();
      const _rowDate = row.date || '';
      const _rowCreated = (row.ad_created_time || '').slice(0, 10);  // YYYY-MM-DD launch date, constant per ad
      rollupStatus(map[name], _rowStatus, _rowDate);
      if (_rowCreated && !map[name].createdDate) map[name].createdDate = _rowCreated;
      // A flat creative can run in both a Demo and a Webinar ad set — accumulate
      // whichever CAPI conversions its ad sets use, then combine at finalize.
      { const _cl = capiConvLabel(campName, row.adset_name); if (_cl === 'Demo' || _cl === 'Demo + Webinar') map[name]._capiDemo = 1; if (_cl === 'Webinar' || _cl === 'Demo + Webinar') map[name]._capiWeb = 1; }
      map[name].spend += parseFloat(row.spend)||0;
      if (!map[name]._campSpend) map[name]._campSpend = {};
      if (!map[name]._campSpend[campName]) map[name]._campSpend[campName] = 0;
      map[name]._campSpend[campName] += parseFloat(row.spend)||0;
      map[name].clicks += parseInt(row.clicks)||0;
      map[name].impressions += parseInt(row.impressions)||0;
      if (row.date) map[name]._dates.push(row.date);
      // Parse video_p25 (Meta only — nested array with action_type/value)
      if (row.video_p25_watched_actions) {
        const vArr = Array.isArray(row.video_p25_watched_actions) ? row.video_p25_watched_actions : [];
        for (const v of vArr) { if (v && v.value) map[name].videoP25 += parseInt(v.value)||0; }
      }
      if (ch !== 'linkedin') {
        const rawD = parseFloat(row[cfg.demoField])||0;
        const dConv = (ch==='google'||ch==='youtube') ? Math.ceil(rawD) : Math.round(rawD);
        map[name].demos += dConv; map[name].demoConv = (map[name].demoConv||0) + dConv;
        // Allowlisted Meta lead-gen campaigns: add Leads (Form) to Demos (CAPI Webinar).
        if (ch === 'meta') { const wl = metaLeadDemos(campName, row); map[name].demos += wl; map[name].webinarLead = (map[name].webinarLead||0) + wl; }
      }
      if (cfg.hasFreq && row.frequency != null && row.frequency !== '') map[name].freqVals.push(parseFloat(row.frequency));
      // Per-campaign creative aggregation
      const campKey = campName.toLowerCase().trim();
      if (!campCreMap[campKey]) campCreMap[campKey] = {};
      if (!campCreMap[campKey][name]) campCreMap[campKey][name] = { name, spend:0, clicks:0, impressions:0, demos:0, freqVals:[], thumbnail: tm[name] || null, videoP25:0, _dates:[], _campName: campName, status:null, _statusDate:'' };
      // Same status rollup as the flat map (latest date wins, ACTIVE wins ties).
      rollupStatus(campCreMap[campKey][name], _rowStatus, _rowDate);
      if (_rowCreated && !campCreMap[campKey][name].createdDate) campCreMap[campKey][name].createdDate = _rowCreated;
      campCreMap[campKey][name].spend += parseFloat(row.spend)||0;
      campCreMap[campKey][name].clicks += parseInt(row.clicks)||0;
      campCreMap[campKey][name].impressions += parseInt(row.impressions)||0;
      if (row.date) campCreMap[campKey][name]._dates.push(row.date);
      if (row.video_p25_watched_actions) {
        const vArr2 = Array.isArray(row.video_p25_watched_actions) ? row.video_p25_watched_actions : [];
        for (const v of vArr2) { if (v && v.value) campCreMap[campKey][name].videoP25 += parseInt(v.value)||0; }
      }
      if (ch !== 'linkedin') {
        const rawD2 = parseFloat(row[cfg.demoField])||0;
        const dConv2 = (ch==='google'||ch==='youtube') ? Math.ceil(rawD2) : Math.round(rawD2);
        campCreMap[campKey][name].demos += dConv2; campCreMap[campKey][name].demoConv = (campCreMap[campKey][name].demoConv||0) + dConv2;
        if (ch === 'meta') { const wl2 = metaLeadDemos(campName, row); campCreMap[campKey][name].demos += wl2; campCreMap[campKey][name].webinarLead = (campCreMap[campKey][name].webinarLead||0) + wl2; }
      }
      if (cfg.hasFreq && row.frequency != null && row.frequency !== '') campCreMap[campKey][name].freqVals.push(parseFloat(row.frequency));
      // Per-ad-set creative aggregation — mirrors campCreMap but keyed by
      // adset_name (Meta) / ad_group_name (Google, TikTok). Powers the Ad Sets
      // table's nested-creative dropdown. Skipped when the row has no ad-set
      // name (e.g. LinkedIn, which has no ad-set concept).
      const adsetName = (row.adset_name || row.ad_group_name || '').trim();
      if (adsetName) {
        const adsetKey = adsetName.toLowerCase();
        if (!adsetCreMap[adsetKey]) adsetCreMap[adsetKey] = {};
        if (!adsetCreMap[adsetKey][name]) adsetCreMap[adsetKey][name] = { name, spend:0, clicks:0, impressions:0, demos:0, freqVals:[], thumbnail: tm[name] || null, videoP25:0, _dates:[], _adsetName: adsetName, campaignName: campName, status:null, _statusDate:'' };
        rollupStatus(adsetCreMap[adsetKey][name], _rowStatus, _rowDate);
        if (_rowCreated && !adsetCreMap[adsetKey][name].createdDate) adsetCreMap[adsetKey][name].createdDate = _rowCreated;
        adsetCreMap[adsetKey][name].spend += parseFloat(row.spend)||0;
        adsetCreMap[adsetKey][name].clicks += parseInt(row.clicks)||0;
        adsetCreMap[adsetKey][name].impressions += parseInt(row.impressions)||0;
        if (row.date) adsetCreMap[adsetKey][name]._dates.push(row.date);
        if (row.video_p25_watched_actions) {
          const vArr3 = Array.isArray(row.video_p25_watched_actions) ? row.video_p25_watched_actions : [];
          for (const v of vArr3) { if (v && v.value) adsetCreMap[adsetKey][name].videoP25 += parseInt(v.value)||0; }
        }
        if (ch !== 'linkedin') {
          const rawD3 = parseFloat(row[cfg.demoField])||0;
          const dConv3 = (ch==='google'||ch==='youtube') ? Math.ceil(rawD3) : Math.round(rawD3);
          adsetCreMap[adsetKey][name].demos += dConv3; adsetCreMap[adsetKey][name].demoConv = (adsetCreMap[adsetKey][name].demoConv||0) + dConv3;
          if (ch === 'meta') { const wl3 = metaLeadDemos(campName, row); adsetCreMap[adsetKey][name].demos += wl3; adsetCreMap[adsetKey][name].webinarLead = (adsetCreMap[adsetKey][name].webinarLead||0) + wl3; }
        }
        if (cfg.hasFreq && row.frequency != null && row.frequency !== '') adsetCreMap[adsetKey][name].freqVals.push(parseFloat(row.frequency));
      }
    }
    for (const c of Object.values(map)) {
      c.ctr = c.impressions > 0 ? (c.clicks/c.impressions)*100 : 0;
      c.cpd = c.demos > 0 ? c.spend/c.demos : null;
      c.frequency = c.freqVals.length ? c.freqVals.reduce((a,b)=>a+b,0)/c.freqVals.length : null;
      delete c.freqVals;
      delete c._statusDate;
      // Compute activeDays
      if (c._dates && c._dates.length) {
        c._dates.sort(); const fd=new Date(c._dates[0]+'T12:00:00Z'),ld=new Date(c._dates[c._dates.length-1]+'T12:00:00Z');
        c.activeDays=Math.round((ld-fd)/86400000)+1;
        c.firstDate=c._dates[0]; c.lastDate=c._dates[c._dates.length-1];
      } else c.activeDays=0;
      delete c._dates;
      if (c._campSpend) {
        let maxS=0, maxC='';
        for (const [cn,s] of Object.entries(c._campSpend)) { if (s>maxS) { maxS=s; maxC=cn; } }
        c.campaignName = maxC;
        delete c._campSpend;
      }
      // Finalize per-creative placements
      if (c._placements && Object.keys(c._placements).length) {
        c.placements = Object.values(c._placements).map(p => {
          p.ctr = p.impressions > 0 ? (p.clicks/p.impressions)*100 : 0;
          p.cpd = p.demos > 0 ? p.spend/p.demos : null;
          return p;
        }).sort((a,b)=>b.spend-a.spend);
      }
      delete c._placements;
      c.capiConv = (c._capiDemo && c._capiWeb) ? 'Demo + Webinar' : (c._capiWeb ? 'Webinar' : (c._capiDemo ? 'Demo' : ''));
      delete c._capiDemo; delete c._capiWeb;
    }
    // Finalize per-campaign creatives
    const campCreFinal = {};
    for (const [ck, cres] of Object.entries(campCreMap)) {
      campCreFinal[ck] = Object.values(cres).map(c => {
        c.ctr = c.impressions > 0 ? (c.clicks/c.impressions)*100 : 0;
        c.cpd = c.demos > 0 ? c.spend/c.demos : null;
        c.frequency = c.freqVals.length ? c.freqVals.reduce((a,b)=>a+b,0)/c.freqVals.length : null;
        delete c.freqVals;
        delete c._statusDate;
        if (c._dates && c._dates.length) {
          c._dates.sort(); const fd2=new Date(c._dates[0]+'T12:00:00Z'),ld2=new Date(c._dates[c._dates.length-1]+'T12:00:00Z');
          c.activeDays=Math.round((ld2-fd2)/86400000)+1;
          c.firstDate=c._dates[0]; c.lastDate=c._dates[c._dates.length-1];
        } else c.activeDays=0;
        delete c._dates;
        c.campaignName = c._campName || ck; delete c._campName;
        c.capiConv = capiConvLabel(c.campaignName, null);  // campaign-level fallback (no single ad set)
        return c;
      }).sort((a,b)=>b.spend-a.spend);
    }
    // Finalize per-ad-set creatives (same shape as campCreFinal, keyed by
    // lowercased ad-set name). Used by the Ad Sets table dropdown.
    const adsetCreFinal = {};
    for (const [ak, cres] of Object.entries(adsetCreMap)) {
      adsetCreFinal[ak] = Object.values(cres).map(c => {
        c.ctr = c.impressions > 0 ? (c.clicks/c.impressions)*100 : 0;
        c.cpd = c.demos > 0 ? c.spend/c.demos : null;
        c.frequency = c.freqVals.length ? c.freqVals.reduce((a,b)=>a+b,0)/c.freqVals.length : null;
        delete c.freqVals;
        delete c._statusDate;
        if (c._dates && c._dates.length) {
          c._dates.sort(); const fd3=new Date(c._dates[0]+'T12:00:00Z'),ld3=new Date(c._dates[c._dates.length-1]+'T12:00:00Z');
          c.activeDays=Math.round((ld3-fd3)/86400000)+1;
          c.firstDate=c._dates[0]; c.lastDate=c._dates[c._dates.length-1];
        } else c.activeDays=0;
        delete c._dates;
        c.adsetName = c._adsetName || ak; delete c._adsetName;
        c.capiConv = capiConvLabel(c.campaignName, c.adsetName);  // ad-set-specific
        return c;
      }).sort((a,b)=>b.spend-a.spend);
    }
    // Finalize overall placement summary
    const placementSummary = Object.values(placementMap).map(p => {
      p.ctr = p.impressions > 0 ? (p.clicks/p.impressions)*100 : 0;
      p.cpd = p.demos > 0 ? p.spend/p.demos : null;
      return p;
    }).sort((a,b)=>b.spend-a.spend);
    results[ch] = { flat: Object.values(map).sort((a,b)=>b.spend-a.spend), byCampaign: campCreFinal, byAdset: adsetCreFinal, placements: placementSummary };
  }
  return results;
}

// ── Creative Fatigue Detection ──
// Fetches 14-day daily CTR per creative per connector.
// Flags creatives where last-7-day avg CTR dropped >15% vs first-7-day avg CTR
// AND impressions haven't dropped >50% (excludes paused creatives).
async function fetchCreativeFatigue(apiKey) {
  const to = new Date(); to.setDate(to.getDate()-1);
  const from = new Date(to); from.setDate(from.getDate()-13); // 14 days
  const fromStr = fmt(from), toStr = fmt(to);
  const midDate = new Date(from); midDate.setDate(midDate.getDate()+7);
  const midMs = midDate.getTime();

  const fatigueMap = {}; // { 'meta::creative_lower': { fatigued, decline, firstCtr, lastCtr } }

  const channelFields = {
    meta:     { connector:'facebook', fields:'date,ad_name,impressions,clicks' },
    linkedin: { connector:'linkedin', fields:'date,ad_name,impressions,clicks' },
    tiktok:   { connector:'tiktok',   fields:'date,ad_name,impressions,clicks' },
    google:   { connector:'google_ads', fields:'date,ad_name,impressions,clicks' },
  };

  const promises = [], chKeys = [];
  for (const [ch, cfg] of Object.entries(channelFields)) {
    promises.push(azWindsorFetch(apiKey, cfg.connector, fromStr, toStr, cfg.fields).catch(e => { console.error(`Fatigue fetch ${ch}:`, e.message); return []; }));
    chKeys.push(ch);
  }

  const results = await Promise.all(promises);
  for (let i=0; i<chKeys.length; i++) {
    const ch = chKeys[i], rows = results[i];
    // Group by creative name
    const byCreative = {};
    for (const row of rows) {
      const name = (row.ad_name||'').trim();
      if (!name) continue;
      if (!byCreative[name]) byCreative[name] = [];
      const d = new Date(row.date+'T12:00:00');
      byCreative[name].push({ date: d.getTime(), impressions: parseInt(row.impressions)||0, clicks: parseInt(row.clicks)||0 });
    }

    for (const [name, days] of Object.entries(byCreative)) {
      if (days.length < 6) continue; // Need minimum data points
      const first = days.filter(d => d.date < midMs);
      const last = days.filter(d => d.date >= midMs);
      if (!first.length || !last.length) continue;

      const firstImpr = first.reduce((s,d)=>s+d.impressions,0);
      const firstClicks = first.reduce((s,d)=>s+d.clicks,0);
      const lastImpr = last.reduce((s,d)=>s+d.impressions,0);
      const lastClicks = last.reduce((s,d)=>s+d.clicks,0);

      if (firstImpr < 200 || lastImpr < 200) continue; // Need meaningful volume
      const firstCtr = firstClicks / firstImpr;
      const lastCtr = lastClicks / lastImpr;
      if (firstCtr <= 0) continue;

      const decline = (firstCtr - lastCtr) / firstCtr; // positive = declining
      const imprDrop = (firstImpr - lastImpr) / firstImpr;

      // Flag: CTR dropped >15%, impressions didn't drop >50% (not paused)
      const fatigued = decline > 0.15 && imprDrop < 0.5;
      if (fatigued) {
        const key = ch + '::' + name.toLowerCase();
        fatigueMap[key] = { fatigued:true, decline:Math.round(decline*100), firstCtr:Math.round(firstCtr*10000)/100, lastCtr:Math.round(lastCtr*10000)/100 };
      }
    }
  }
  return fatigueMap;
}

// ── Audience / Ad Group level fetch ──
async function fetchAzAudiences(apiKey, from, to) {
  const results = {};
  const configs = {
    meta:   { connector:'facebook', fields:'date,adset_name,campaign_name,spend,clicks,impressions,conversions_submit_application_total,adset_status,adset_effective_status'+META_LEAD_FIELDS, nameField:'adset_name', demoField:'conversions_submit_application_total', statusField:'adset_effective_status', statusFallback:'adset_status' },
    google: { connector:'google_ads', fields:'date,ad_group_name,campaign_name,spend,clicks,impressions,conversions', nameField:'ad_group_name', demoField:'conversions' },
    tiktok: { connector:'tiktok', fields:'date,ad_group_name,campaign_name,spend,clicks,impressions,conversions', nameField:'ad_group_name', demoField:'conversions' },
  };

  const promises = [], chKeys = [];
  for (const [ch, cfg] of Object.entries(configs)) {
    promises.push(azWindsorFetchAll(apiKey, cfg.connector, from, to, cfg.fields).catch(e => { console.error(`Audience fetch ${ch}:`, e.message); return []; }));
    chKeys.push(ch);
  }

  const rawResults = await Promise.all(promises);
  for (let i = 0; i < chKeys.length; i++) {
    const ch = chKeys[i], rows = rawResults[i], cfg = configs[ch];
    const map = {};
    for (const row of rows) {
      const name = (row[cfg.nameField] || '').trim();
      if (!name) continue;
      const campName = row.campaign_name || '';
      // Google: filter out YouTube campaigns
      if (ch === 'google' && /\byt\b|youtube/i.test(campName)) continue;
      if (!map[name]) map[name] = { name, campaign: '', spend: 0, clicks: 0, impressions: 0, demos: 0, status: null, _statusDate: '' };
      // Authoritative ad-set status (effective_status → PAUSED when the ad set /
      // its campaign is OFF). This is the source of truth for the row's status,
      // instead of guessing "active if any creative is active" (which read each
      // ad's own toggle and so showed paused ad sets as Active).
      if (cfg.statusField) rollupStatus(map[name], (row[cfg.statusField] || row[cfg.statusFallback] || '').toUpperCase(), row.date);
      map[name].spend += parseFloat(row.spend) || 0;
      map[name].clicks += parseInt(row.clicks) || 0;
      map[name].impressions += parseInt(row.impressions) || 0;
      let d = parseFloat(row[cfg.demoField]) || 0;
      if (ch === 'google') d = Math.ceil(d); else d = Math.round(d);
      map[name].demos += d; map[name].demoConv = (map[name].demoConv||0) + d;
      // Allowlisted Meta lead-gen campaigns: add Leads (Form) to ad-set Demos (CAPI Webinar).
      if (ch === 'meta') { const wl = metaLeadDemos(campName, row); map[name].demos += wl; map[name].webinarLead = (map[name].webinarLead||0) + wl; }
      if (!map[name].campaign && campName) map[name].campaign = campName;
    }
    const list = Object.values(map).map(a => {
      a.ctr = a.impressions > 0 ? (a.clicks / a.impressions) * 100 : 0;
      a.cpd = a.demos > 0 ? a.spend / a.demos : null;
      a.cpc = a.clicks > 0 ? a.spend / a.clicks : null;
      a.capiConv = capiConvLabel(a.campaign, a.name);  // ad-set-specific
      delete a._statusDate;
      return a;
    }).sort((a, b) => b.spend - a.spend);
    results[ch] = list;
  }
  // LinkedIn has no ad_group concept
  results.linkedin = null;
  results.youtube = null;
  return results;
}

function buildAzAttribution(deals, closedWonDeals) {
  const byChannel = {}, byCampaign = {}, byCreative = {};
  for (const ch of DASH_CHANNELS) byChannel[ch] = { qualified:0, pruned:0, noShow:0, rescheduled:0, pendingEval:0, pending:0, blank:0, total:0 };

  for (const deal of deals) {
    const p = deal.properties||{};
    const ch = mapUtmToChannel(p.utm_source, p.utm_medium);
    if (!ch) continue;
    const cat = categorizeDemoStatus(p.demo_attendance_status, p.demo_qualification_outcome, p.demo_given__status);
    byChannel[ch].total++; byChannel[ch][cat]++;
    const camp = (p.utm_campaign||'').trim();
    if (camp) {
      const key = `${ch}::${camp.toLowerCase()}`;
      if (!byCampaign[key]) byCampaign[key] = { channel:ch, campaign:camp.toLowerCase(), qualified:0, pruned:0, noShow:0, rescheduled:0, pendingEval:0, pending:0, blank:0, total:0 };
      byCampaign[key].total++; byCampaign[key][cat]++;
    }
    const creative = (p.utm_content||'').trim();
    if (creative) {
      const key = `${ch}::${creative.toLowerCase()}`;
      if (!byCreative[key]) byCreative[key] = { channel:ch, creative:creative.toLowerCase(), qualified:0, pruned:0, noShow:0, rescheduled:0, pendingEval:0, pending:0, blank:0, total:0 };
      byCreative[key].total++; byCreative[key][cat]++;
    }
  }

  const closedByChannel = {};
  for (const ch of DASH_CHANNELS) closedByChannel[ch] = { count:0, mrr:0 };
  for (const deal of closedWonDeals) {
    const p = deal.properties||{};
    const ch = mapUtmToChannel(p.utm_source, p.utm_medium);
    if (!ch) continue;
    closedByChannel[ch].count++; closedByChannel[ch].mrr += parseFloat(p.amount)||0;
  }
  return { byChannel, byCampaign, byCreative, closedByChannel };
}

function buildAzResponse(period, prior, priorMonth, windsor, creatives, priorW, pmW, attr, priorA, pmA, budgets, isAllTime, windowType, fatigue, audiences) {
  let tS=0,tD=0,tQ=0,tCW=0,tMRR=0;
  let ptS=0,ptD=0,ptQ=0,ptCW=0,ptMRR=0;
  let pmtS=0,pmtD=0,pmtQ=0,pmtCW=0,pmtMRR=0;
  const channels = {};

  for (const ch of DASH_CHANNELS) {
    const w = windsor[ch]?.totals||{spend:0,clicks:0,impressions:0,demos:0,ctr:0,frequency:null};
    const a = attr.byChannel[ch]||{};
    const cw = attr.closedByChannel[ch]||{count:0,mrr:0};
    const q = a.qualified||0, unattr = Math.max(0, w.demos-(a.total||0));
    tS+=w.spend; tD+=w.demos; tQ+=q; tCW+=cw.count; tMRR+=cw.mrr;

    const pw = priorW?.[ch]?.totals||null;
    const pa = priorA?.byChannel?.[ch]||null;
    const pcw = priorA?.closedByChannel?.[ch]||null;
    if (pw) { ptS+=pw.spend; ptD+=pw.demos; ptQ+=(pa?.qualified||0); ptCW+=(pcw?.count||0); ptMRR+=(pcw?.mrr||0); }

    const pmw = pmW?.[ch]?.totals||null;
    const pma = pmA?.byChannel?.[ch]||null;
    const pmcw = pmA?.closedByChannel?.[ch]||null;
    if (pmw) { pmtS+=pmw.spend; pmtD+=pmw.demos; pmtQ+=(pma?.qualified||0); pmtCW+=(pmcw?.count||0); pmtMRR+=(pmcw?.mrr||0); }

    // Join campaign attribution to Windsor campaign data
    const campAttr = {};
    for (const [key,ca] of Object.entries(attr.byCampaign)) {
      if (key.startsWith(ch+'::')) campAttr[key.slice(ch.length+2)] = ca;
    }
    const mergedCamps = [];
    for (const c of Object.values(windsor[ch]?.campaigns||{})) {
      const campKey = c.name.toLowerCase();
      const ca = campAttr[campKey]||{};
      const attrTotal = ca.total||0;
      // LinkedIn: Windsor can't split demos per campaign, use HubSpot UTM attribution instead
      const demos = ch === 'linkedin' ? attrTotal : c.demos;
      const cpd = demos > 0 ? c.spend / demos : null;
      mergedCamps.push({ ...c, demos, cpd, qualified:ca.qualified||0, pruned:ca.pruned||0, pendingEval:ca.pendingEval||0, rescheduled:ca.rescheduled||0, noShow:ca.noShow||0, pending:ca.pending||0, blank:ca.blank||0, attributedTotal:attrTotal });
    }
    mergedCamps.sort((a,b)=>b.demos-a.demos);

    // Creatives (may be null if timed out) — merge with utm_content attribution
    const rawCreativeData = creatives?.[ch] || null;
    const rawCreatives = rawCreativeData?.flat || rawCreativeData; // support both new {flat,byCampaign} and old array format
    const creativeByCampaign = rawCreativeData?.byCampaign || null;
    const creativeByAdset = rawCreativeData?.byAdset || null;
    let mergedCreatives = null;
    if (Array.isArray(rawCreatives) && rawCreatives.length) {
      const creativeAttr = {};
      for (const [key,ca] of Object.entries(attr.byCreative||{})) {
        if (key.startsWith(ch+'::')) creativeAttr[key.slice(ch.length+2)] = ca;
      }
      mergedCreatives = rawCreatives.map(c => {
        const ca = creativeAttr[c.name.toLowerCase()]||{};
        const attrTotal = ca.total||0;
        const demos = ch === 'linkedin' ? attrTotal : c.demos;
        const cpd = demos > 0 ? c.spend / demos : null;
        const fKey = ch+'::'+c.name.toLowerCase();
        const ft = fatigue?.[fKey] || null;
        return { ...c, demos, cpd, qualified:ca.qualified||0, pruned:ca.pruned||0, pendingEval:ca.pendingEval||0, rescheduled:ca.rescheduled||0, noShow:ca.noShow||0, pending:ca.pending||0, blank:ca.blank||0, attributedTotal:attrTotal, fatigued:!!ft, fatigueDecline:ft?.decline||0, fatigueFirstCtr:ft?.firstCtr||0, fatigueLastCtr:ft?.lastCtr||0 };
      });
    }

    // Demo quality funnel for this channel
    const chTotal = a.total||0;
    const dqFunnel = {};
    for (const cat of FUNNEL_ORDER) dqFunnel[cat] = { count:a[cat]||0, pct:chTotal>0?r2(((a[cat]||0)/chTotal)*100):0 };
    dqFunnel.total = chTotal;
    // Prior funnel
    let priorDqFunnel = null;
    if (pa) { const pt = pa.total||0; priorDqFunnel = {}; for (const cat of FUNNEL_ORDER) priorDqFunnel[cat] = { count:pa[cat]||0, pct:pt>0?r2(((pa[cat]||0)/pt)*100):0 }; priorDqFunnel.total = pt; }
    let pmDqFunnel = null;
    if (pma) { const pt = pma.total||0; pmDqFunnel = {}; for (const cat of FUNNEL_ORDER) pmDqFunnel[cat] = { count:pma[cat]||0, pct:pt>0?r2(((pma[cat]||0)/pt)*100):0 }; pmDqFunnel.total = pt; }

    channels[ch] = {
      label: CHANNEL_LABELS[ch],
      spend:w.spend, demos:w.demos, ctr:w.ctr, impressions:w.impressions, clicks:w.clicks,
      frequency:w.frequency, cpd:w.cpd, hasFrequency:AZ_CONNECTORS[ch]?.hasFreq||false,
      qualified:q, pruned:a.pruned||0, pendingEval:a.pendingEval||0,
      rescheduled:a.rescheduled||0, noShow:a.noShow||0,
      pending:a.pending||0, blank:a.blank||0,
      unattributed:unattr, attributedTotal:a.total||0,
      closedWon:cw.count, closedWonMRR:cw.mrr,
      cpqd:q>0?w.spend/q:null, qualifiedPct:w.demos>0?(q/w.demos)*100:0,
      budget:budgets[ch]||0,
      pendingConnector:PENDING_CHANNELS.has(ch),
      // Prior
      priorSpend:pw?.spend??null, priorDemos:pw?.demos??null, priorCtr:pw?.ctr??null,
      priorImpressions:pw?.impressions??null, priorClicks:pw?.clicks??null,
      priorQualified:pa?.qualified??null, priorClosedWon:pcw?.count??null, priorClosedWonMRR:pcw?.mrr??null,
      // Prior month
      pmSpend:pmw?.spend??null, pmDemos:pmw?.demos??null, pmCtr:pmw?.ctr??null,
      pmImpressions:pmw?.impressions??null, pmClicks:pmw?.clicks??null,
      pmQualified:pma?.qualified??null, pmClosedWon:pmcw?.count??null, pmClosedWonMRR:pmcw?.mrr??null,
      retargetPct: (() => { let rd=0; for (const camp of mergedCamps) { if (/\bS0[23]\b|lower\s*funnel|retargeting/i.test(camp.name)) rd+=camp.demos; } return w.demos>0?(rd/w.demos)*100:0; })(),
      retargetDemos: (() => { let rd=0; for (const camp of mergedCamps) { if (/\bS0[23]\b|lower\s*funnel|retargeting/i.test(camp.name)) rd+=camp.demos; } return rd; })(),
      priorRetargetPct: (() => { if (!pw) return null; const pc=Object.values(priorW?.[ch]?.campaigns||{}); let rd=0; for (const camp of pc) { if (/\bS0[23]\b|lower\s*funnel|retargeting/i.test(camp.name)) rd+=camp.demos; } return pw.demos>0?(rd/pw.demos)*100:null; })(),
      pmRetargetPct: (() => { if (!pmw) return null; const pc=Object.values(pmW?.[ch]?.campaigns||{}); let rd=0; for (const camp of pc) { if (/\bS0[23]\b|lower\s*funnel|retargeting/i.test(camp.name)) rd+=camp.demos; } return pmw.demos>0?(rd/pmw.demos)*100:null; })(),
      campaigns: mergedCamps,
      creatives: mergedCreatives,
      creativeByCampaign: creativeByCampaign,
      creativeByAdset: creativeByAdset,
      fatigueMap: Object.fromEntries(Object.entries(fatigue||{}).filter(([k])=>k.startsWith(ch+'::'))),
      audiences: audiences?.[ch] || null,
      placements: rawCreativeData?.placements || null,
      demoQuality: dqFunnel, priorDemoQuality: priorDqFunnel, pmDemoQuality: pmDqFunnel,
    };
  }

  const totalAttr = Object.values(attr.byChannel).reduce((s,a)=>s+(a.total||0),0);
  const overview = {
    closedWon:tCW, adSpend:tS, adDemos:tD, cpd:tD>0?tS/tD:null,
    qualified:tQ, cpqd:tQ>0?tS/tQ:null, qualifiedPct:tD>0?(tQ/tD)*100:0, closedWonMRR:tMRR,
    priorClosedWon:priorA?ptCW:null, priorAdSpend:priorW?ptS:null,
    priorAdDemos:priorW?ptD:null, priorQualified:priorA?ptQ:null, priorClosedWonMRR:priorA?ptMRR:null,
    pmClosedWon:pmA?pmtCW:null, pmAdSpend:pmW?pmtS:null,
    pmAdDemos:pmW?pmtD:null, pmQualified:pmA?pmtQ:null, pmClosedWonMRR:pmA?pmtMRR:null,
  };

  return {
    period, priorPeriod:prior||null, priorMonthPeriod:priorMonth||null, isAllTime,
    overview, channels,
    funnelColors: FUNNEL_COLORS, funnelLabels: FUNNEL_LABELS, funnelOrder: FUNNEL_ORDER,
    matchRate:{ attributed:totalAttr, windsorDemos:tD, pct:tD>0?r2((totalAttr/tD)*100):0 },
    meta:{ generatedAt:new Date().toISOString(), windowType, from:period&&period.from, to:period&&period.to },
  };
}

async function processAzRequest(windowType, customFrom, customTo, env, vsFrom, vsTo) {
  // Guard: 'custom' window requires from + to. Without them, downstream
  // toMsUTC(null) / toMsET(null) throw "Cannot read properties of null
  // (reading 'split')" and the whole analyzer endpoint 500s. Return an
  // explicit error so the dashboard surfaces a clear message instead.
  if (windowType === 'custom' && (!customFrom || !customTo)) {
    throw new Error('Custom window requires from + to date params');
  }
  const apiKey = env.WINDSOR_API_KEY, hsToken = env.HUBSPOT_TOKEN;
  const { current, prior, priorMonth } = computeWindows(windowType, customFrom, customTo, vsFrom, vsTo);
  const isAllTime = windowType === 'allTime';

  // Current period: Windsor campaigns + creatives + HubSpot (clamp Windsor dates)
  const windsorP = fetchAzCampaigns(apiKey, wFrom(current.from), current.to);
  const creativesP = fetchAzCreatives(apiKey, wFrom(current.from), current.to);
  const audiencesP = fetchAzAudiences(apiKey, wFrom(current.from), current.to).catch(e => { console.error('Audience fetch failed:', e.message); return {}; });
  const fatigueP = fetchCreativeFatigue(apiKey).catch(e => { console.error('Fatigue fetch failed:', e.message); return {}; });
  const fatigueWithTimeout = Promise.race([fatigueP, new Promise(r => setTimeout(() => r({}), 8000))]);
  const pipeP = fetchPipelineDeals(hsToken, current.from, current.to);
  const cwP = fetchClosedWonDeals(hsToken, current.from, current.to);
  const [windsor, creatives, audiences, fatigue, pipe, cw] = await Promise.all([windsorP, creativesP, audiencesP, fatigueWithTimeout, pipeP, cwP]);

  // Prior period
  let priorW=null, priorA=null;
  if (prior && !isAllTime) {
    const [pw, pp, pc] = await Promise.all([
      fetchAzCampaigns(apiKey, wFrom(prior.from), prior.to),
      fetchPipelineDeals(hsToken, prior.from, prior.to),
      fetchClosedWonDeals(hsToken, prior.from, prior.to),
    ]);
    priorW = pw; priorA = buildAzAttribution(filterActiveBrands(pp), filterActiveBrands(pc));
  }

  // Prior month
  let pmW=null, pmA=null;
  if (priorMonth && !isAllTime) {
    const [pw, pp, pc] = await Promise.all([
      fetchAzCampaigns(apiKey, wFrom(priorMonth.from), priorMonth.to),
      fetchPipelineDeals(hsToken, priorMonth.from, priorMonth.to),
      fetchClosedWonDeals(hsToken, priorMonth.from, priorMonth.to),
    ]);
    pmW = pw; pmA = buildAzAttribution(filterActiveBrands(pp), filterActiveBrands(pc));
  }

  const attr = buildAzAttribution(filterActiveBrands(pipe), filterActiveBrands(cw));
  const budgets = getBudgetsForMonth(current.from);

  return buildAzResponse(current, prior, priorMonth, windsor, creatives, priorW, pmW, attr, priorA, pmA, budgets, isAllTime, windowType, fatigue, audiences);
}

// ---------------------------------------------------------------------------
// Webinar Performance Page
// ---------------------------------------------------------------------------
// Compares two parallel funnels:
//   • Webinar funnel (sub-10K brands): Registered → Attended → Closed Won
//   • Demo funnel (10K+ brands):       Booked → Held → Qualified → Closed Won
// Shares a single click + spend pool at the top (one form-fill router; can't
// pre-segment clicks). Spend is ALLOCATED proportionally by form-fills
// (registrations + bookings) so per-funnel CPAs are meaningful.
const WEBINAR_PERF_REPS = ['Ashley', 'Eisa', 'Elias', 'James'];
const WEBINAR_PERF_DAILY_CAPACITY = 10;
const WEBINAR_PERF_SUB10K_TIERS = new Set(['Pre-launch / just launching', '0-10K monthly web visitors', 'Very Small']);
const WEBINAR_BASELINE_KV_KEY = 'webinar_attended_to_cw_baseline_pct';
const WEBINAR_BASELINE_DEFAULT = 17;

// Contacts with `webinar_date` in window — i.e. people who attended (or
// were scheduled to attend) a webinar that HAPPENED in the selected
// window. Anchoring on webinar_date instead of createdate is what users
// mean by "webinar performance for this period": the funnel for the
// webinars that ran in this window, regardless of when each registrant
// originally signed up.
//
// (Old behavior anchored on createdate, which meant a contact created
// in May for a June 25 webinar landed in May's stats — surfacing as
// "stats not updating" because attendance/no-show/CW couldn't yet be
// known when the contact was first counted.)
//
// webinar_date is a HubSpot DATE property stored at midnight UTC, so
// use toMsUTC bounds (no ET offset). Matches the convention already
// used by fetchDealsByDemoDateWindow for date_demo_booked.
//
// Two OR'd filter groups so we don't undercount vs HubSpot:
//   A) webinar_date in window — captures registrants for webinars THAT
//      RAN in the window, regardless of when each contact was created
//   B) createdate in window AND webinar_date HAS_PROPERTY — captures
//      contacts newly added this window who registered for SOME
//      webinar (even if the webinar itself is outside the window —
//      e.g. someone signed up this week for a webinar two weeks out)
// Dedupe by id since a contact who matches both (created in window
// AND webinar in window) would otherwise be double-counted.
//
// Pagination: explicit maxPages=50 (hsSearch's internal 10k hard cap)
// and webinar_date DESC sort so any cap would drop the OLDEST in
// window rather than the newest. ⚠ logs if even the 10k cap is hit.
async function fetchWebinarRegistrants(token, from, to) {
  const fUTC = String(toMsUTC(from));
  const tUTC = String(toMsUTC(to, true));
  const fET = String(toMsET(from));
  const tET = String(toMsET(to, true));
  const rows = await hsSearch(token, 'contacts', [
    { filters: [
      { propertyName: 'webinar_date', operator: 'GTE', value: fUTC },
      { propertyName: 'webinar_date', operator: 'LTE', value: tUTC },
    ]},
    { filters: [
      { propertyName: 'createdate', operator: 'GTE', value: fET },
      { propertyName: 'createdate', operator: 'LTE', value: tET },
      { propertyName: 'webinar_date', operator: 'HAS_PROPERTY' },
    ]},
  ], ['createdate','webinar_date','webinar_has_attended','email','average_monthly_web_traffic'],
     200,
     [{ propertyName: 'webinar_date', direction: 'DESCENDING' }],
     50);
  // Dedupe — HubSpot returns the union of OR-groups but a contact
  // matching both groups appears once in the response, so the dedupe is
  // a safety net rather than load-bearing. Still cheap to do.
  const deduped = [...new Map(rows.map(c => [c.id, c])).values()];
  // Per-group classification for diagnostic logging: how many were
  // matched by Group A only, Group B only, or both. Helps narrow down
  // any future "lower than HubSpot" discrepancy without redeploying.
  const fUTCn = Number(fUTC), tUTCn = Number(tUTC);
  const fETn  = Number(fET),  tETn  = Number(tET);
  let aOnly = 0, bOnly = 0, both = 0;
  for (const c of deduped) {
    const wd = c.properties?.webinar_date;
    const cd = c.properties?.createdate;
    const wdMs = wd ? (isoMs(wd) || dateMs(wd)) : NaN;
    const cdMs = cd ? isoMs(cd) : NaN;
    const inA = !isNaN(wdMs) && wdMs >= fUTCn && wdMs <= tUTCn;
    const inB = !isNaN(cdMs) && cdMs >= fETn  && cdMs <= tETn && !isNaN(wdMs);
    if (inA && inB) both++; else if (inA) aOnly++; else if (inB) bOnly++;
  }
  const capped = deduped.length >= 10000 ? ' ⚠ CAPPED at 10k' : '';
  console.log(`fetchWebinarRegistrants(${from}..${to}): ${deduped.length} contacts (A-only=${aOnly} B-only=${bOnly} both=${both})${capped}`);
  return deduped;
}

// Contacts created in window with date_demo_booked HAS_PROPERTY. Used for
// the demo-funnel "Booked" count so it matches the Irfan Dashboard's
// contact-based calculation exactly (rather than the deal-based count
// which can drift when a deal has multiple contacts or the cloned
// web-traffic property is stale).
async function fetchDemoBookerContacts(token, from, to) {
  const fMs = String(toMsET(from)), tMs = String(toMsET(to, true));
  return hsSearch(token, 'contacts', [{
    filters: [
      { propertyName: 'createdate', operator: 'GTE', value: fMs },
      { propertyName: 'createdate', operator: 'LTE', value: tMs },
      { propertyName: 'date_demo_booked', operator: 'HAS_PROPERTY' },
    ],
  }], ['createdate', 'date_demo_booked', 'average_monthly_web_traffic'], 200);
}

// Deals whose deal createdate is in window AND have a date_demo_booked. Used
// for the demo funnel's downstream stages (Held / No-show / Closed Won)
// since those statuses live on the DEAL, not the contact. The "Booked"
// count itself is now contact-based via fetchDemoBookerContacts.
async function fetchDemoFunnelDeals(token, from, to) {
  const fMs = String(toMsET(from)), tMs = String(toMsET(to, true));
  const deals = await hsSearch(token, 'deals', [{
    filters: [
      { propertyName: 'hs_createdate', operator: 'GTE', value: fMs },
      { propertyName: 'hs_createdate', operator: 'LTE', value: tMs },
      { propertyName: 'date_demo_booked', operator: 'HAS_PROPERTY' },
    ],
  }], ['hs_createdate','createdate','date_demo_booked','rescheduled_meeting_date','demo_attendance_status','demo_qualification_outcome','dealstage','average_monthly_web_traffic__cloned_','hubspot_owner_id'], 200);
  return [...new Map(deals.map(d => [d.id, d])).values()];
}

// For a cohort of attendee contact IDs, count how many have at least one
// associated deal that reached dealstage = closedwon (any time). Uses two
// batch reads — associations then deal stages — so the budget stays low
// even with hundreds of attendees.
async function countAttendeesClosedWon(token, contactIds) {
  if (!contactIds || !contactIds.length) return 0;
  const inputs = contactIds.map(id => ({ id: String(id) }));
  let assocData;
  try {
    const r = await fetch('https://api.hubapi.com/crm/v4/associations/contacts/deals/batch/read', {
      method: 'POST',
      headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({ inputs }),
    });
    if (!r.ok) { console.error('countAttendeesClosedWon assoc', r.status, await r.text()); return 0; }
    assocData = await r.json();
  } catch(e) { console.error('countAttendeesClosedWon assoc threw', e.message); return 0; }
  const contactDeals = {};
  const allDealIds = new Set();
  for (const row of (assocData.results || [])) {
    const cid = row.from?.id; if (!cid) continue;
    contactDeals[cid] = (row.to || []).map(t => String(t.toObjectId)).filter(Boolean);
    for (const did of contactDeals[cid]) allDealIds.add(did);
  }
  if (!allDealIds.size) return 0;
  let dealData;
  try {
    const r = await fetch('https://api.hubapi.com/crm/v3/objects/deals/batch/read', {
      method: 'POST',
      headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({ inputs: Array.from(allDealIds).map(id => ({ id })), properties: ['dealstage'] }),
    });
    if (!r.ok) { console.error('countAttendeesClosedWon deals', r.status, await r.text()); return 0; }
    dealData = await r.json();
  } catch(e) { console.error('countAttendeesClosedWon deals threw', e.message); return 0; }
  const cwDealIds = new Set();
  for (const d of (dealData.results || [])) {
    if ((d.properties?.dealstage || '') === 'closedwon') cwDealIds.add(String(d.id));
  }
  let n = 0;
  for (const cid in contactDeals) {
    if (contactDeals[cid].some(did => cwDealIds.has(did))) n++;
  }
  return n;
}

// Deals filtered by EFFECTIVE demo date (date_demo_booked OR
// rescheduled_meeting_date) within the given window. Used for both the
// Today rep capacity card and the per-window rep capacity card.
async function fetchDealsByDemoDateWindow(token, fromStr, toStr) {
  const fMs = String(toMsUTC(fromStr));
  const tMs = String(toMsUTC(toStr, true));
  const deals = await hsSearch(token, 'deals', [
    { filters: [
      { propertyName: 'date_demo_booked', operator: 'GTE', value: fMs },
      { propertyName: 'date_demo_booked', operator: 'LTE', value: tMs },
    ]},
    { filters: [
      { propertyName: 'rescheduled_meeting_date', operator: 'GTE', value: fMs },
      { propertyName: 'rescheduled_meeting_date', operator: 'LTE', value: tMs },
    ]},
  ], ['date_demo_booked','rescheduled_meeting_date','demo_attendance_status','hubspot_owner_id'], 200);
  return [...new Map(deals.map(d => [d.id, d])).values()];
}
async function fetchTodayDeals(token) {
  const todayStr = fmt(todayET());
  const deals = await fetchDealsByDemoDateWindow(token, todayStr, todayStr);
  return { deals, todayStr };
}
// Count of weekdays (Mon–Fri) in an inclusive [from, to] window. Used as
// the per-rep capacity denominator for window-scoped rep utilization
// (utilization = booked ÷ (10 × business_days_in_window)).
function countBusinessDays(fromStr, toStr) {
  if (!fromStr || !toStr) return 0;
  const f = new Date(fromStr + 'T00:00:00Z');
  const t = new Date(toStr + 'T00:00:00Z');
  if (isNaN(f) || isNaN(t)) return 0;
  let n = 0;
  for (let d = new Date(f); d <= t; d.setUTCDate(d.getUTCDate()+1)) {
    const dow = d.getUTCDay();
    if (dow !== 0 && dow !== 6) n++;
  }
  return n;
}

// Baseline value (Talar's "attended → closed won" benchmark) — stored in KV
// so the page's inline edit can persist across sessions.
async function readWebinarBaseline(env) {
  if (!env.CONTENT_STORE) return WEBINAR_BASELINE_DEFAULT;
  try {
    const raw = await env.CONTENT_STORE.get(WEBINAR_BASELINE_KV_KEY);
    if (!raw) return WEBINAR_BASELINE_DEFAULT;
    const parsed = JSON.parse(raw);
    const v = typeof parsed === 'number' ? parsed : parsed?.value;
    return (typeof v === 'number' && isFinite(v)) ? v : WEBINAR_BASELINE_DEFAULT;
  } catch(e) { return WEBINAR_BASELINE_DEFAULT; }
}
async function saveWebinarBaseline(env, valuePct) {
  if (!env.CONTENT_STORE) return { ok: false, error: 'KV not configured' };
  const n = parseFloat(valuePct);
  if (!isFinite(n) || n < 0 || n > 100) return { ok: false, error: 'Value must be 0-100' };
  await env.CONTENT_STORE.put(WEBINAR_BASELINE_KV_KEY, JSON.stringify({ value: n, savedAt: new Date().toISOString() }));
  return { ok: true, value: n };
}

async function processWebinarPerfRequest(windowType, customFrom, customTo, env, vsFrom, vsTo) {
  if (windowType === 'custom' && (!customFrom || !customTo)) {
    throw new Error('Custom window requires from + to date params');
  }
  const hsToken = env.HUBSPOT_TOKEN;
  const apiKey = env.WINDSOR_API_KEY;
  const { current, prior, priorMonth } = computeWindows(windowType, customFrom, customTo, vsFrom, vsTo);
  const todayMs = todayET().getTime();

  // Per-period metric builder — fetches webinar contacts + demo booker
  // contacts + demo deals (for downstream stages) + Windsor totals in
  // parallel, then derives funnel counts.
  async function periodMetrics(period) {
    if (!period) return null;
    const [registrants, demoBookers, demoDeals, windsor] = await Promise.all([
      fetchWebinarRegistrants(hsToken, period.from, period.to),
      fetchDemoBookerContacts(hsToken, period.from, period.to),
      fetchDemoFunnelDeals(hsToken, period.from, period.to),
      fetchAzCampaigns(apiKey, wFrom(period.from), period.to),
    ]);
    // ── Webinar funnel ──
    const registered = registrants.length;
    const attendees = registrants.filter(c => (c.properties?.webinar_has_attended || '').toString().toLowerCase() === 'true');
    const attended = attendees.length;
    let noShow = 0;
    for (const c of registrants) {
      const wasAtt = (c.properties?.webinar_has_attended || '').toString().toLowerCase() === 'true';
      if (wasAtt) continue;
      const wd = c.properties?.webinar_date;
      if (!wd) continue;
      const wdMs = new Date((String(wd)).split('T')[0] + 'T00:00:00Z').getTime();
      if (!isNaN(wdMs) && wdMs < todayMs) noShow++;
    }
    const attendeeIds = attendees.map(c => c.id).filter(Boolean);
    const webinarCW = await countAttendeesClosedWon(hsToken, attendeeIds);
    console.log(`Webinar funnel ${period.from}..${period.to}: registered=${registered} attended=${attended} noShow=${noShow} closedWon=${webinarCW}`);
    // ── Demo funnel (10K+ only) ──
    // "Booked" is CONTACT-based so it matches the Irfan Dashboard's
    // "Demos Booked per Day" exactly: contacts created in window with
    // date_demo_booked set AND web traffic NOT in small-brands set.
    // Downstream stages (Held / No-show / Closed Won) stay DEAL-based
    // since those statuses live on the deal record. Funnel ratios mix
    // contacts → deals as a deliberate trade-off.
    const demoBooked = demoBookers.filter(c => {
      const v = c.properties?.average_monthly_web_traffic;
      return !WEBINAR_PERF_SUB10K_TIERS.has(v || '');
    }).length;
    const scaleDeals = demoDeals.filter(d => {
      const v = d.properties?.average_monthly_web_traffic__cloned_;
      return !WEBINAR_PERF_SUB10K_TIERS.has(v || '');
    });
    let demoHeld = 0, demoNoShow = 0, demoQualified = 0, demoCW = 0;
    // Lead time accumulator (over current window — reused if periodMetrics
    // is called for current).
    let leadTimeSum = 0, leadTimeN = 0;
    for (const d of scaleDeals) {
      const p = d.properties || {};
      const att = (p.demo_attendance_status || '').trim();
      const qo = (p.demo_qualification_outcome || '').trim();
      if (att === 'Demo Given (originally scheduled)' || att === 'Demo Given (rescheduled)') demoHeld++;
      if (att === 'No Show' || att === 'Cancelled before demo') demoNoShow++;
      if (qo === 'Qualified') demoQualified++;
      if ((p.dealstage || '') === 'closedwon') demoCW++;
      const eff = p.rescheduled_meeting_date || p.date_demo_booked;
      const cd = p.hs_createdate || p.createdate;
      if (eff && cd) {
        const t1 = new Date(cd).getTime(), t2 = new Date(eff).getTime();
        if (!isNaN(t1) && !isNaN(t2) && t2 >= t1) { leadTimeSum += (t2 - t1) / 86400000; leadTimeN++; }
      }
    }
    // ── Shared clicks + spend ──
    let clicks = 0, spend = 0;
    for (const ch of DASH_CHANNELS) {
      const t = windsor[ch]?.totals;
      if (!t) continue;
      clicks += t.clicks || 0;
      spend  += t.spend  || 0;
    }
    // ── Spend allocation by form-fills ──
    const totalFills = registered + demoBooked;
    const wShare = totalFills > 0 ? (registered / totalFills) : 0;
    const dShare = totalFills > 0 ? (demoBooked / totalFills) : 0;
    return {
      clicks, spend,
      webinar: { registered, attended, noShow, closedWon: webinarCW, allocatedSpend: spend * wShare, spendSharePct: wShare * 100 },
      demo:    { booked: demoBooked, held: demoHeld, noShow: demoNoShow, qualified: demoQualified, closedWon: demoCW, allocatedSpend: spend * dShare, spendSharePct: dShare * 100 },
      leadTimeSum, leadTimeN,
    };
  }

  const [curM, priorM, pmM] = await Promise.all([
    periodMetrics(current),
    periodMetrics(prior),
    periodMetrics(priorMonth),
  ]);

  // ── Rep capacity (today, real-time, ignores page selector) ──
  // We also fetch deals by EFFECTIVE demo date over the page window for the
  // second "selected window" capacity card.
  const [ownerMap, todayBundle, windowDealsForCapacity] = await Promise.all([
    fetchOwners(hsToken),
    fetchTodayDeals(hsToken),
    fetchDealsByDemoDateWindow(hsToken, current.from, current.to),
  ]);
  // owner id → first name for the 4 watched reps
  const watchedById = {};
  for (const [oid, fullName] of Object.entries(ownerMap)) {
    const first = (fullName || '').trim().split(' ')[0];
    if (WEBINAR_PERF_REPS.indexOf(first) >= 0) watchedById[oid] = first;
  }
  const repCap = {};
  for (const name of WEBINAR_PERF_REPS) repCap[name] = { rep: name, booked: 0, held: 0 };
  for (const d of todayBundle.deals) {
    const oid = d.properties?.hubspot_owner_id;
    const repName = watchedById[oid];
    if (!repName) continue;
    repCap[repName].booked++;
    const att = (d.properties?.demo_attendance_status || '').trim();
    if (att === 'Demo Given (originally scheduled)' || att === 'Demo Given (rescheduled)') repCap[repName].held++;
  }
  const repCapacity = {
    today: todayBundle.todayStr,
    capacityPerRep: WEBINAR_PERF_DAILY_CAPACITY,
    perRep: WEBINAR_PERF_REPS.map(n => ({
      rep: n,
      booked: repCap[n].booked,
      held: repCap[n].held,
      // Utilization = booked ÷ 10-per-day capacity (capped 100 for display).
      // Dashboard renders a 2-color bar: held portion + booked-not-held
      // portion + empty capacity remainder.
      utilizationPct: Math.min(100, (repCap[n].booked / WEBINAR_PERF_DAILY_CAPACITY) * 100),
      heldPct: Math.min(100, (repCap[n].held / WEBINAR_PERF_DAILY_CAPACITY) * 100),
    })),
    avgLeadTimeDays: curM && curM.leadTimeN > 0 ? curM.leadTimeSum / curM.leadTimeN : null,
    leadTimeSampleSize: curM ? curM.leadTimeN : 0,
  };

  // ── Rep capacity for the page-selected window ──
  // Same per-rep / aggregate shape as the Today card, but counts demos whose
  // EFFECTIVE date (rescheduled_meeting_date || date_demo_booked) falls
  // within the page window. Per-rep utilization denominator scales with the
  // weekday count in the window (10/day × business_days), so percentages
  // are comparable across MTD / Last 7 Days / etc.
  const repCapWin = {};
  for (const name of WEBINAR_PERF_REPS) repCapWin[name] = { rep: name, booked: 0, held: 0 };
  for (const d of windowDealsForCapacity) {
    const oid = d.properties?.hubspot_owner_id;
    const repName = watchedById[oid];
    if (!repName) continue;
    repCapWin[repName].booked++;
    const att = (d.properties?.demo_attendance_status || '').trim();
    if (att === 'Demo Given (originally scheduled)' || att === 'Demo Given (rescheduled)') repCapWin[repName].held++;
  }
  const windowBusinessDays = countBusinessDays(current.from, current.to);
  const windowCapacityPerRep = WEBINAR_PERF_DAILY_CAPACITY * windowBusinessDays;
  const repCapacityWindow = {
    from: current.from,
    to: current.to,
    label: current.label,
    businessDays: windowBusinessDays,
    capacityPerRep: windowCapacityPerRep,
    dailyCapacityPerRep: WEBINAR_PERF_DAILY_CAPACITY,
    perRep: WEBINAR_PERF_REPS.map(n => ({
      rep: n,
      booked: repCapWin[n].booked,
      held: repCapWin[n].held,
      utilizationPct: windowCapacityPerRep > 0
        ? Math.min(100, (repCapWin[n].booked / windowCapacityPerRep) * 100)
        : 0,
      heldPct: windowCapacityPerRep > 0
        ? Math.min(100, (repCapWin[n].held / windowCapacityPerRep) * 100)
        : 0,
    })),
    avgLeadTimeDays: curM && curM.leadTimeN > 0 ? curM.leadTimeSum / curM.leadTimeN : null,
    leadTimeSampleSize: curM ? curM.leadTimeN : 0,
  };

  // ── Baseline ──
  const baselineValue = await readWebinarBaseline(env);

  // Strip lead-time accumulators from response (not needed client-side)
  function stripLT(p) { if (!p) return null; const { leadTimeSum, leadTimeN, ...rest } = p; return rest; }

  return {
    period: current,
    priorPeriod: prior,
    priorMonthPeriod: priorMonth,
    current: stripLT(curM),
    prior: stripLT(priorM),
    priorMonth: stripLT(pmM),
    repCapacity,
    repCapacityWindow,
    baseline: { attendedToClosedWonPct: baselineValue },
    meta: { generatedAt: new Date().toISOString(), windowType },
  };
}

// ---------------------------------------------------------------------------
// Worker Entry Point
// ---------------------------------------------------------------------------
// Content Studio static data — served by /api/content endpoint
const CONTENT_DATA = {
  posts: [
    {id:1,week:1,day:"Mon",date:"Mar 24",platform:"LinkedIn",format:"Text Post",pillar:"Trust Gap",hook:"46% of health shoppers verify product claims on ChatGPT before buying. Your PDP was not written for this.",time:"9:00 AM",score:9,script:"HOOK: 46% of health shoppers now verify product claims on ChatGPT before buying. Your PDP was not written for this.\n\nBODY: When a shopper sees an unfamiliar ingredient, they do not email support. They open ChatGPT. And ChatGPT gives a vague, unverified answer — or worse, a reason not to buy.\n\nThe brands winning right now answer the question before the shopper leaves.\n\nCTA: What is your number 1 unanswered shopper question? Drop it below.\n\n#DTCHealth #HealthMarketing #ClinicianAI",manifesto:"Stat-led hook. No product push until CTA. Engagement question. ✅"},
    {id:2,week:1,day:"Tue",date:"Mar 25",platform:"Instagram",format:"Reel 60s",pillar:"ClinicianAI Demo",hook:"Your wellness shoppers are fact-checking you on ChatGPT. Watch what they find.",time:"10:00 AM",score:10,script:"HOOK (0-3s): Your wellness shoppers are fact-checking you on ChatGPT right now.\n[TEXT: Are you passing the test?]\n\nBODY (3-45s): Screen recording — ChatGPT gives vague disclaimer. Then ClinicianAI gives specific, cited answer in 8 seconds.\n[CUT every 2-3s: face then screen]\n\nCTA (45-60s): Comment AUDIT — free PDP review.\n[SHOW: Clinicians Choice badge]",manifesto:"Audience called out. Face and screen cuts. Badge at CTA. DM trigger. ✅"},
    {id:3,week:1,day:"Wed",date:"Mar 26",platform:"LinkedIn",format:"Carousel",pillar:"DTC Intel",hook:"3 reasons wellness marketers are losing at the moment of decision.",time:"9:00 AM",score:8,script:"SLIDE 1: 3 reasons wellness marketers are losing at the moment of decision\nSLIDE 2: 1. Shoppers leave to Google ingredients. 50% never come back.\nSLIDE 3: 2. ChatGPT gives confident but unverified health answers.\nSLIDE 4: 3. Generic disclaimers signal you do not trust your own product.\nSLIDE 5: The fix: clinician-verified answers on your PDP — already live on leading wellness brands.\nCTA: Save this for your next product review.",manifesto:"Addresses wellness marketers. A-B-C logic. References live product. ✅"},
    {id:4,week:1,day:"Thu",date:"Mar 27",platform:"TikTok",format:"Talking Head 30s",pillar:"Trust Gap",hook:"3 questions your wellness shoppers are Googling about your product right now.",time:"11:00 AM",score:7,script:"HOOK (0-3s): 3 questions your wellness shoppers are Googling about your product right now.\n\nBODY: Can I take this with my blood pressure meds? Is this safe while breastfeeding? Will this interact with my thyroid medication?\n\nMost brands respond with a disclaimer, a ticket, or nothing.\n\nClinicianAI answers all three. On your product page. In 8 seconds.\n\nCTA: See it in action — link in bio.\n[SHOW: Clinicians Choice badge]",manifesto:"Audience in hook. Fast cuts. Badge at end. 30s. References live product. ✅"},
    {id:5,week:1,day:"Fri",date:"Mar 28",platform:"LinkedIn",format:"Repost + Commentary",pillar:"DTC Intel",hook:"ZOE just showed what happens when science becomes your marketing strategy.",time:"9:00 AM",score:6,script:"RESHARE: ZOE latest science-backed content\n\nCOMMENTARY: ZOE averages 132K Instagram plays. No ad spend. Just science.\n\nClinicianAI does this at the product level — clinician-reviewed answers, on your PDP, already live on brands like Moonbrew.",manifesto:"Tool: competitor case study. References existing customers. ✅"},
    {id:6,week:2,day:"Mon",date:"Mar 31",platform:"LinkedIn",format:"Results Post",pillar:"ClinicianAI Demo",hook:"We have been live for a while now. Here is what the data actually shows.",time:"9:00 AM",score:10,script:"HOOK: We have been live for a while now. Here is what the data actually shows.\n\nBrands using ClinicianAI are seeing:\n- 50% higher purchase likelihood from shoppers who interact with it\n- 90% of answers upvoted by shoppers (trust rate)\n- Major drop in support tickets about ingredient safety\n\nThe most surprising finding: it is not just about answering questions faster. It is about discovering questions brands never knew shoppers were asking.\n\nCTA: Want to see what questions YOUR shoppers are asking? Book a demo — link in comments.",manifesto:"Post-launch proof format. No launch language. Data-led. ✅"},
    {id:7,week:2,day:"Tue",date:"Apr 1",platform:"Instagram",format:"Reel 60s",pillar:"Clinician Authority",hook:"One wellness brand added clinician attribution to 12 PDPs. Here is what happened to conversion.",time:"10:00 AM",score:9,script:"HOOK (0-3s): One wellness brand added clinician attribution to their PDPs. Here is what happened.\n[TEXT: +50% purchase likelihood]\n\nBODY: Before: generic disclaimer. After: ClinicianAI — specific, sourced, 8 seconds.\n[DATA GRAPH: Purchase likelihood +50%]\n\nCTA (50-60s): Comment PROOF — I will send you the full case study.\n[SHOW: Clinicians Choice badge]",manifesto:"Tool: case study. Badge at CTA. Caption withholds answer. ✅"},
    {id:8,week:2,day:"Wed",date:"Apr 2",platform:"LinkedIn",format:"Text Post",pillar:"DTC Intel",hook:"Unilever shifted 50% of ad spend to influencers. Here is why wellness marketers need a different answer.",time:"9:00 AM",score:8,script:"73% of shoppers do not trust online reviews (McKinsey).\n\nThe brands gaining ground are not buying attention — they are buying credibility.\n\nClinician attribution is the most underpenetrated conversion lever in DTC health right now.\n\nBrands already using it are seeing 50% higher purchase likelihood.\n\nAgree or disagree?",manifesto:"Tool: 3rd party research (McKinsey). A-B-C logic. ✅"},
    {id:9,week:3,day:"Mon",date:"Apr 7",platform:"LinkedIn",format:"Partnership Announcement",pillar:"Clinician Authority",hook:"We are now live on another wellness brand. Here is what happened in the first 30 days.",time:"9:00 AM",score:10,script:"HOOK: We are now live on [Brand X] — and here is what happened in the first 30 days.\n\nShopper questions answered: [X]\nTrust rate (upvoted answers): 90%\nPurchase likelihood lift: 50%\n\nThe questions shoppers were asking that [Brand X] had never seen before:\n- [example question 1]\n- [example question 2]\n\nThis is what clinician intelligence at scale looks like.\n\nCTA: Want to see it for your brand? DM us or book a demo below.",manifesto:"Post-launch client announcement. Data-led. No launch language. ✅"},
    {id:10,week:3,day:"Wed",date:"Apr 9",platform:"LinkedIn",format:"Text Post",pillar:"DTC Intel",hook:"We analyzed 19 DTC health brand social accounts. Here is what separates the top 10%.",time:"9:00 AM",score:9,script:"OpenEvidence: 291 avg likes. Every post announces credibility.\nHuberman Lab: 1M+ plays. Every post is a science brief.\n\nThe pattern: top performers publish science before they publish CTAs.\n\nFrontrowMD has the science. We have the clinicians. We are already live.\n\nTime to publish.\n\nWhat DTC health content do you actually save and share?",manifesto:"Tool: original research. References live product. ✅"},
    {id:11,week:3,day:"Thu",date:"Apr 10",platform:"TikTok",format:"Talking Head 45s",pillar:"Trust Gap",hook:"Consult your clinician is the most damaging phrase on your wellness product page.",time:"11:00 AM",score:9,script:"HOOK (0-3s): THIS is the most damaging phrase on your wellness product page.\n[TEXT: Consult your clinician]\n\nWhen a wellness shopper asks Can I take this with my medication? and you say consult your clinician — you have told them you do not trust your own product.\n\n50% never come back.\n\nCTA: Comment ANSWER — I will show you what that looks like on a live PDP.\n[SHOW: Clinicians Choice badge]",manifesto:"Emphasis hook. Badge shown. References live solution. ✅"},
    {id:12,week:4,day:"Mon",date:"Apr 14",platform:"LinkedIn",format:"Benchmark Post",pillar:"DTC Intel",hook:"We analyzed 50 wellness brand product pages. The results are not great.",time:"9:00 AM",score:10,script:"HOOK: We analyzed 50 wellness brand product pages. The results are not great.\n\n% that answered a drug interaction question: [X]%\n% that gave clinician-verified answer: [X]%\n\nThe brands in the top quartile answer the question before the shopper leaves.\n\nClinicianAI is already live on brands doing exactly this.\n\nFull report dropping [date]. Comment REPORT to be first.",manifesto:"Tool: original research. References live product. DM trigger. ✅"},
    {id:13,week:4,day:"Wed",date:"Apr 16",platform:"Instagram",format:"Reel 60s",pillar:"ClinicianAI Demo",hook:"Watch ClinicianAI answer your hardest wellness shopper question in 8 seconds.",time:"10:00 AM",score:10,script:"HOOK (0-3s): Watch ClinicianAI answer your hardest wellness shopper question live.\n[TEXT: 8 seconds. No disclaimers.]\n\n[Screen recording] Can I take this if I am on Lexapro? — flags 5-HTP, explains serotonin syndrome risk, cites research — 7.8 seconds.\n\nThis is already live on wellness brands. Your competitors may already have it.\n\nCTA: Comment LIVE — free demo on your actual product page. 5 spots this week.\n[SHOW: Clinicians Choice badge]",manifesto:"Tool: live product demo. Badge shown. Creates urgency. ✅"},
    {id:14,week:4,day:"Fri",date:"Apr 18",platform:"LinkedIn",format:"CTA Post",pillar:"ClinicianAI Demo",hook:"We are offering 10 free PDP audits this month. Here is what you get.",time:"9:00 AM",score:8,script:"HOOK: We are offering 10 free PDP audits this month.\n\nHere is what you get:\n- Top 3 questions your wellness shoppers are Googling\n- How your current PDP answers (or does not answer) those questions\n- A side-by-side showing what a clinician-verified version looks like\n\nNo pitch. Just data.\n\nDM us AUDIT or comment below. 10 slots.\n\n#DTCHealth #WellnessMarketing",manifesto:"DM trigger. Benefit-led. No launch language. ✅"},
  ],
  irfan: [
    {id:"i1",date:"Mar 24",time:"9:00 AM",score:10,angle:"Macro Stat Hook",hook:"The global wellness economy just hit $6.8T. Here is what that means for wellness marketers right now.",why:"Fitt Insider format. Stat leads to operator implication.",post:"The global wellness economy just hit $6.8T.\n\nHere is what that means for wellness marketers right now.\n\nMost of that value is not captured by brands with the best formulas.\n\nIt is captured by the brands shoppers trust.\n\nAnd trust in health is shifting fast:\n- 73% of shoppers do not trust online reviews anymore\n- 46% now verify product claims on AI before buying\n- Brands without clinical credibility are losing at the moment of decision\n\nThe window to own clinician-backed positioning in your category is open.\n\nIt will not be open forever.\n\nWhat is your brand doing to close the trust gap?\n\n#DTCHealth #HealthMarketing #WellnessEconomy",manifesto:"No recommend. Addresses wellness marketers. A-B-C logic. Data cited. ✅"},
    {id:"i2",date:"Mar 26",time:"9:00 AM",score:9,angle:"Trend Implication",hook:"GLP-1 drugs just changed DTC health forever. Most wellness marketers do not realize it yet.",why:"40M+ Americans on GLP-1s. Every one has supplement questions.",post:"GLP-1 drugs just changed DTC health forever.\n\nMost wellness marketers do not realize it yet.\n\n40M+ Americans are now on GLP-1s like Ozempic or Mounjaro.\n\nEvery one of them is asking:\nWill this interact with my medication?\nIs this safe at my new weight?\nCan I still take this while on semaglutide?\n\nYour product page was not written for this shopper.\n\nThe brands that answer these questions with clinical backing will capture a category about to explode.\n\n#GLP1 #DTCHealth #HealthMarketing #ClinicianAI",manifesto:"Wellness marketers in hook. Logical chain. No doctor. ✅"},
    {id:"i3",date:"Mar 31",time:"9:00 AM",score:10,angle:"Category Report",hook:"Longevity just became a $100B consumer category. Here is who is winning and why.",why:"Fastest-growing wellness category. Connects to DTC credibility.",post:"Longevity just became a $100B consumer category.\n\nHere is who is winning and why it matters for every wellness brand.\n\nThe brands capturing this market are not winning on formulas.\n\nThey are winning on trust infrastructure:\n- Thorne: clinical-grade certifications, clinician partnerships\n- Momentous: NSF-certified, Huberman endorsement\n- AG1: published clinical trials, independent research\n\nNone of them lead with ingredients. They lead with proof.\n\nWhat is your credibility strategy for the longevity buyer?\n\n#Longevity #DTCHealth #WellnessTrends",manifesto:"Tool: competitor case studies. Clinician not doctor. ✅"},
    {id:"i4",date:"Apr 2",time:"9:00 AM",score:9,angle:"Industry Problem Frame",hook:"The supplement industry has a $300B healthwashing problem. Here is how smart wellness marketers are pulling ahead.",why:"Fitt Insider format. Stat lead, operator implication.",post:"The supplement industry has a $300B healthwashing problem.\n\nHere is how smart wellness marketers are pulling ahead.\n\nHealthwashing has made shoppers deeply skeptical of everyone.\n\nThe result:\n- Higher CAC as trust declines\n- Longer conversion cycles\n- More abandoned carts at the ingredient question\n\nOpenEvidence, ZOE, and Thorne all solved this the same way: clinical credibility built into the product.\n\nClinical credibility is the new moat in health commerce.\n\nWhat is yours?\n\n#HealthMarketing #Supplements #DTCBrands",manifesto:"Wellness marketers in hook. Tool: competitor cases. No doctor recommend. ✅"},
    {id:"i5",date:"Apr 7",time:"9:00 AM",score:9,angle:"Third-Party Data Drop",hook:"Adobe just published data that should concern every wellness marketer. Here is what it says.",why:"Highest-performing format. Third-party source plus operator implication.",post:"Adobe just published data that should concern every wellness marketer.\n\n46% of wellness shoppers now use AI to verify product claims before buying.\n\nNearly half your shoppers are leaving your product page mid-consideration to fact-check you on ChatGPT.\n\nChatGPT gives them a generic answer. Sometimes wrong. Sometimes scary.\n\nAnd then they do not come back.\n\nThe brands solving this are putting clinician-verified answers on their product pages now.\n\nSource: Adobe Digital Economy Index 2025\n\n#DTCHealth #HealthMarketing #ClinicianAI",manifesto:"Wellness marketer in hook. Tool: 3rd party research (Adobe). Source cited. ✅"},
    {id:"i6",date:"Apr 9",time:"9:00 AM",score:8,angle:"Original Research Post",hook:"I asked 5 top wellness brands one question. Here is what I found.",why:"Original analysis from Irfan is highest-signal content.",post:"I asked the top 5 DTC wellness brands one question:\n\nIs this safe if I am on blood pressure medication?\n\nBrand 1: These statements have not been evaluated by the FDA.\nBrand 2: No response field on PDP\nBrand 3: FAQ with no specific answer\nBrand 4: Support ticket. Response time: 3 days.\nBrand 5: Clinician-verified answer. Specific. Cited. 8 seconds.\n\nBrand 5 was a ClinicianAI client.\n\n#DTCHealth #HealthMarketing #ConversionRate",manifesto:"Tool: original research. Clinician-verified not doctor approved. ✅"},
    {id:"i7",date:"Apr 14",time:"9:00 AM",score:10,angle:"Category Trend Forecast",hook:"Mental health supplements are about to be the fastest-growing DTC category in 2026. Here is the data.",why:"Fitt Insider strongest format. Calling the trend early equals thought leadership.",post:"Mental health supplements are about to be the fastest-growing DTC category in 2026.\n\nHere is the data:\n- Anxiety supplement market: $2.1B globally, growing 8.4% YoY\n- Ashwagandha for anxiety search volume up 340% since 2021\n- GLP-1 users increasingly asking about mood and cognition supplements\n\nThe brands that will win this category answer the hard questions:\nCan I take this with my SSRI?\nIs this safe with my psychiatric medication?\n\nThe window to establish clinical credibility in mental health supplements is open right now.\n\n#MentalHealth #DTCHealth #Supplements",manifesto:"Tool: market data. Logical chain. No doctor recommend. ✅"},
    {id:"i8",date:"Apr 16",time:"9:00 AM",score:8,angle:"Intel Synthesis",hook:"Fitt Insider tracks 7,000+ wellness companies. Here is the signal I keep seeing.",why:"Positions Irfan as the insider operator with an extra insight layer.",post:"Fitt Insider tracks 7,000+ wellness companies.\n\nHere is the signal I keep seeing and what it means for your brand.\n\nThe companies growing fastest share one thing: they have built clinical credibility into their product, not just their marketing.\n\nThorne: NSF certification and clinician referral program\nZOE: Published microbiome research and clinician endorsement\nMomentous: Huberman and NSF-certified formulations\n\nThe brands at the bottom are still leading with clean ingredients, no fillers. That is table stakes now.\n\nWhat is your clinical credibility layer?\n\n#WellnessTrends #DTCHealth",manifesto:"Tool: competitor pattern analysis. Clinician not doctor. ✅"},
  ],
  benchmarks: [
    {account:"Huberman Lab",platform:"Instagram",avgLikes:"21,698",avgPlays:"1,011,059",notes:"Science + DM triggers",warn:false},
    {account:"Peter Attia MD",platform:"Instagram",avgLikes:"16,622",avgPlays:"703,876",notes:"Long-form clips, longevity",warn:false},
    {account:"Dr. Gabrielle Lyon",platform:"Instagram",avgLikes:"3,056",avgPlays:"147,454",notes:"Muscle-centric medicine",warn:false},
    {account:"ZOE",platform:"Instagram",avgLikes:"2,150",avgPlays:"132,114",notes:"Gut science, DM triggers",warn:false},
    {account:"OpenEvidence",platform:"LinkedIn",avgLikes:"291",avgPlays:"—",notes:"Partnership announcements",warn:false},
    {account:"HubSpot",platform:"LinkedIn",avgLikes:"422",avgPlays:"—",notes:"Humor + product demos",warn:false},
    {account:"Fitt Insider",platform:"Instagram",avgLikes:"948",avgPlays:"3,426",notes:"Industry trend reporting",warn:false},
    {account:"Irfan Alam",platform:"LinkedIn",avgLikes:"100",avgPlays:"—",notes:"DTC health/wellness ops",warn:false},
    {account:"FrontrowMD",platform:"LinkedIn",avgLikes:"27",avgPlays:"—",notes:"10-15x below peers — needs attention",warn:true},
  ],
  rules: [
    {section:"Hook Rules",icon:"🎣",items:["One sentence to stop scrolling","Question answered in the middle or end — never give it away immediately","Emphasis hook: THIS is the reason xyz","Must include wellness marketers or your wellness shoppers","Include abbreviated text hook on screen"]},
    {section:"Script Rules",icon:"📝",items:["30 seconds total — strict","Do not give away the resolution immediately","Stay focused on one topic only","Viewer must feel rewarded — would they forward this to their boss?","Do not be salesy about FrontrowMD until the very end","Every sentence connects logically (A then B then C)","Use at least one tool: research, stories, demos, testimonials, or competitor footage"]},
    {section:"Caption Rules",icon:"📸",items:["Keep it short — do not give away the answer","Frame as: here is what you will learn, but you have to watch","Can reuse the video hook as the caption hook"]},
    {section:"Video Visuals",icon:"🎬",items:["Authentic face/human-first — never stock photos","Keep ample space above head for icons and overlays","Cuts every few seconds","Motion in hook increases stop rate","Data: use graphs not raw numbers, show source on screen","At FrontrowMD pitch: show Clinicians Choice badge","Dress to match audience: fitness, workout, or work-from-home attire"]},
    {section:"Static Ad Rules",icon:"🖼️",items:["Clinicians Choice badge: large, at top or middle — NEVER bottom","No light font on light BG, no dark font on dark BG","Stick to brand colors: navy, steel blue, sky blue","Headline: large font, can be multi-line","Body copy: max 2 lines stacked","Text-light — 2 to 3 second decision window","ClinicianAI = search bar + blue button — NEVER full product UI"]},
    {section:"Language Rules",icon:"🚫",items:["NEVER say recommend (as in doctor recommend)","NEVER say doctor store","NEVER say medical advice","Use clinician not doctor","Clinicians submit reviews — NOT write reviews"]},
  ],
  ideas: [
    {id:1,title:"Clinicians are not marketers — and that is the point",hook:"Why FrontrowMD captures clinician feedback that is unbiased and unpaid for",why:"Audience misconception: people think we pay clinicians to market for us. This clears it up and becomes a trust-builder.",pillar:"Clinician Authority",platform:"LinkedIn",status:"approved",votes:5,submittedBy:"Irfan",comments:["This addresses the objection head on","Could pair with a real clinician testimonial — very powerful"]},
    {id:2,title:"What ChatGPT gets wrong about your ingredients",hook:"We ran the same 10 questions through ChatGPT and ClinicianAI. The differences were alarming.",why:"Direct comparison content always performs. Shows the gap without us just saying it.",pillar:"Trust Gap",platform:"TikTok",status:"new",votes:3,submittedBy:"Aurea",comments:["Good for TikTok, fast cuts between the two answers"]},
    {id:3,title:"The 90% trust rate explained",hook:"9 out of 10 shoppers upvoted ClinicianAI answers. Here is what that actually means.",why:"We have this stat but have not turned it into a dedicated piece of content yet.",pillar:"ClinicianAI Demo",platform:"Instagram",status:"new",votes:2,submittedBy:"Aurea",comments:[]},
  ],
  adComps: [
    {name:"Bazaarvoice",domain:"bazaarvoice.com",category:"User-Generated Content / Reviews"},
    {name:"Yotpo",domain:"yotpo.com",category:"eCommerce Marketing"},
    {name:"Okendo",domain:"okendo.io",category:"Customer Reviews & Loyalty"},
    {name:"Stamped.io",domain:"stamped.io",category:"Reviews & Loyalty for DTC"},
  ],
  brandContext: "FrontrowMD is a B2B SaaS platform for DTC health and wellness brands. It gives DTC health brands access to a network of 1,700+ vetted, uncompensated clinicians who organically share brand products on their personalized patient discount pages and write honest reviews. THREE PRODUCTS: 1. Clinicians Choice Badge 2. Clinician Reviews 3. ClinicianAI. KEY PROOF POINTS: 400+ brands, 1700+ clinicians, clinicians are NEVER compensated, Rootine: 2x CVR boost, YORO: 3x CVR, Profi: 74% CVR. TARGET: DTC health brand founders, CMOs, ecommerce directors. FORBIDDEN: Never say clinicians recommend.",
};


// AI proxy for Content Studio
async function proxyAnthropicAI(body, env) {
  const apiKey = env.ANTHROPIC_API_KEY;
  if (!apiKey) return { error: true, message: 'ANTHROPIC_API_KEY not configured in Cloudflare secrets' };
  for (let attempt = 0; attempt < 4; attempt++) {
    try {
      const r = await fetch('https://api.anthropic.com/v1/messages', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'x-api-key': apiKey, 'anthropic-version': '2023-06-01' },
        body: JSON.stringify(body),
      });
      if (r.status === 429 || r.status === 529) {
        const wait = Math.min(2000 * Math.pow(2, attempt), 8000);
        console.log(`Anthropic ${r.status} on attempt ${attempt+1}, retrying in ${wait}ms...`);
        await sleep(wait);
        continue;
      }
      const data = await r.json();
      if (!r.ok) return { error: true, message: 'Anthropic API ' + r.status + ': ' + (data.error?.message || JSON.stringify(data)) };
      return data;
    } catch(e) {
      if (attempt === 3) return { error: true, message: 'Proxy fetch failed: ' + e.message };
      await sleep(2000 * (attempt + 1));
    }
  }
  return { error: true, message: 'Anthropic API overloaded after 4 retries. Try again in a few minutes.' };
}

// ---------------------------------------------------------------------------
// Sales Dashboard — Data Fetching
// ---------------------------------------------------------------------------
const COMPANY_PROPS = ['name','domain','industry','company_time_zone','company_size_bucket','revenue_tier','monthly_visitor_tier','demo_prep_briefing','main_products','target_customer_description','description'];
const SALES_CONTACT_PROPS = ['firstname','lastname','date_demo_booked','email','website','company','role_at_company',
  'hs_sales_email_last_opened','hs_sales_email_last_clicked','hs_sales_email_last_replied',
  'notes_last_contacted','hs_sequences_is_enrolled','hs_latest_sequence_enrolled',
  'average_monthly_web_traffic','sl_last_demo_name','sl_last_demo_completion_percent'];

// BD Dashboard — fetch all closed-won deals with BD properties
const BD_DEAL_PROPS = [
  'dealname','dealstage','brand_status','amount','closedate','createdate',
  'price_per_review','package_type','setup_fee','months_until_upgrade',
  'fee_after_upgrade','clinician_ai_adopted_','clinician_ai_price_per_product',
  'hubspot_owner_id',
  // Added in v19: expanded BD column set
  'days_to_close','original_amount','average_monthly_web_traffic__cloned_',
  'of_products','month_minimum','of_free_reviews','of_total_reviews',
  'of_clinicianai','of_clinician_analysis','activation_fee','notes',
  // Added for "From Demo" column / KPI on BD Tracker. rescheduled_meeting_date
  // is needed for the "rescheduled overrides original" effective-date logic
  // (see bdFromDemo, the Date Demo Booked column highlight, and the date
  // filter "Demo Date" mode in bd-dashboard.html).
  'date_demo_booked','rescheduled_meeting_date',
  // Billing details group (new in v22)
  'invoice_date','approval_date','internal_recurly_link',
  'recurly_account_management_url','recurly_billing_intake_url',
  // Churn / Pause details group (new in v23). pausechurn_date is the only
  // column visible by default; the rest live behind the group-collapse
  // chevron in the BD Tracker table.
  'pausechurn_date','never_implemented_churn','churn_reason',
  'detailed_reason_for_churn','paused_billing_length_months',
  'temporary_pause_reason','paused_billing__billing_restart_date',
];

async function fetchBDData(env) {
  const hsToken = env.HUBSPOT_TOKEN;
  // hsSearch defaults to maxPages=10 → 2000 deals. We bump to 50 (10k cap, matches hsSearch's hard cap)
  // since BD pulls every late-funnel/won/churned deal across all time.
  const deals = await hsSearch(hsToken, 'deals', [
    { filters: [{ propertyName: 'dealstage', operator: 'IN', values: ['appointmentscheduled','1084214349','decisionmakerboughtin','contractsent','closedwon','closedlost','3453957850','3453925110','1062974581','3517067985'] }] },
  ], BD_DEAL_PROPS, 200, [{ propertyName: 'hs_createdate', direction: 'ASCENDING' }], 50);
  console.log(`BD: ${deals.length} deals across all stages`);

  const ownerMap = {};
  try {
    const ownersRes = await fetch('https://api.hubapi.com/crm/v3/owners?limit=100', {
      headers: { Authorization: `Bearer ${hsToken}` },
    });
    const ownersData = await ownersRes.json();
    for (const o of (ownersData.results || [])) {
      ownerMap[o.id] = ((o.firstName || '') + ' ' + (o.lastName || '')).trim() || o.email || o.id;
    }
  } catch(e) { console.error('BD owner fetch error:', e); }

  const dealIds = deals.map(d => d.id);
  // Load cached associations from KV so we don't re-fetch deals that we've
  // already mapped to a company. Same pattern as the companies cache.
  // Cache value per deal:
  //   "12345"   → company ID string (stable; never re-checked)
  //   {t: ts}   → tried at time ts and found no company; re-check after TTL
  //               (so deals that have a company added later in HubSpot
  //                eventually pick it up without a full cache wipe)
  //   null      → LEGACY no-company (pre-TTL). Treated as expired → re-fetch.
  //   missing   → never tried → fetch this load
  let assocCache = {};
  if (env.CONTENT_STORE) {
    try {
      const raw = await env.CONTENT_STORE.get('bd_assoc_cache_v1');
      if (raw) assocCache = JSON.parse(raw);
    } catch(e) { console.warn('BD assoc cache load failed:', e.message); }
  }
  const ASSOC_NULL_TTL_MS = 24 * 60 * 60 * 1000; // 24 hours
  const nowMs = Date.now();
  // Re-fetch null entries either (a) legacy (no timestamp) or (b) older
  // than TTL. String entries (= known company ID) stay valid forever.
  function _assocStillValid(c) {
    if (typeof c === 'string') return true;
    if (c === null) return false; // legacy
    if (c && typeof c === 'object' && typeof c.t === 'number') {
      return (nowMs - c.t) < ASSOC_NULL_TTL_MS;
    }
    return false;
  }
  const companyAssociations = {};
  for (const id of dealIds) {
    const cached = assocCache[id];
    if (typeof cached === 'string') companyAssociations[id] = cached;
    // Object/null/expired → leave unset; will be re-fetched below
  }
  const cachedAssocCount = Object.keys(companyAssociations).length;
  const missingDealIds = dealIds.filter(id => !_assocStillValid(assocCache[id]));
  // Cap per-load association fetching to stay within Cloudflare's
  // subrequest budget (deal search + owner + assoc + company-cache-read
  // already use ~25-30 subrequests). 12 batches = 1200 deals per load.
  // Subsequent loads will catch up on remaining missing deals.
  const MAX_ASSOC_BATCHES = 12;
  const idsToFetch = missingDealIds.slice(0, MAX_ASSOC_BATCHES * 100);
  let assocMatched = 0, assocBatches = 0, assocFailures = 0, assocNoCompany = 0;
  for (let i = 0; i < idsToFetch.length; i += 100) {
    const batch = idsToFetch.slice(i, i + 100);
    assocBatches++;
    try {
      const assocRes = await fetch('https://api.hubapi.com/crm/v4/associations/deals/companies/batch/read', {
        method: 'POST',
        headers: { Authorization: `Bearer ${hsToken}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ inputs: batch.map(id => ({ id: String(id) })) }),
      });
      if (!assocRes.ok) {
        const txt = await assocRes.text();
        console.error(`BD assoc batch ${i} HTTP ${assocRes.status}: ${txt.slice(0,200)}`);
        assocFailures++;
        continue;
      }
      const assocData = await assocRes.json();
      const returnedIds = new Set();
      for (const r of (assocData.results || [])) {
        const dealId = r.from?.id;
        if (!dealId) continue;
        returnedIds.add(dealId);
        const to = r.to || [];
        if (!to.length) {
          // No associated company at this time → store timestamped no-company
          // marker so we'll re-check after TTL (HubSpot user may add a company
          // later, e.g. for newly-created deals or fixed data).
          assocCache[dealId] = { t: nowMs };
          assocNoCompany++;
          continue;
        }
        // Prefer the PRIMARY company association (typeId 5 / label "Primary")
        let primaryId = null, firstId = null;
        for (const t of to) {
          const cId = t.toObjectId ?? t.id ?? t.companyId ?? null;
          if (cId == null) continue;
          if (firstId == null) firstId = cId;
          const types = t.associationTypes || t.types || [];
          if (types.some(at => at.typeId === 5 || /primary/i.test(at.label || ''))) {
            primaryId = cId; break;
          }
        }
        const companyId = primaryId ?? firstId;
        if (companyId != null) {
          const cidStr = String(companyId);
          companyAssociations[dealId] = cidStr;
          assocCache[dealId] = cidStr;
          assocMatched++;
        } else {
          assocCache[dealId] = { t: nowMs };
          assocNoCompany++;
        }
      }
      // Any deal in this batch NOT in returnedIds means HubSpot's batch read
      // didn't include it (typically: archived/deleted deal, or no
      // associations at all). Mark with timestamp so it re-checks after TTL.
      for (const id of batch) {
        if (!returnedIds.has(id) && !_assocStillValid(assocCache[id])) {
          assocCache[id] = { t: nowMs };
          assocNoCompany++;
        }
      }
    } catch(e) { console.error('BD assoc batch error:', e); assocFailures++; }
  }
  // Persist updated association cache
  if (env.CONTENT_STORE && idsToFetch.length > 0) {
    try {
      await env.CONTENT_STORE.put('bd_assoc_cache_v1', JSON.stringify(assocCache));
    } catch(e) { console.warn('BD assoc cache save failed:', e.message); }
  }
  const assocDeferred = missingDealIds.length - idsToFetch.length;
  console.log(`BD associations: ${cachedAssocCount} cache hits · ${assocMatched} newly matched · ${assocNoCompany} no-company · ${assocDeferred} deferred to next load (${assocBatches} batches, ${assocFailures} failures, ${dealIds.length} total deals)`);

  // Company names are loaded ONLY from KV cache in this endpoint — no live
  // HubSpot fetches here. Deal search + associations already eat most of
  // the per-invocation subrequest budget, so company fetches go to a
  // dedicated /api/bd/lookup-companies endpoint with its own fresh budget.
  // The client calls that endpoint after the initial render to fill in
  // missing names; the worker maintains the cache so each load is cheaper.
  const uniqueCompanyIds = [...new Set(Object.values(companyAssociations))];
  let companyMap = {};
  if (env.CONTENT_STORE) {
    try {
      const raw = await env.CONTENT_STORE.get('bd_company_cache_v5');
      if (raw) companyMap = JSON.parse(raw);
    } catch(e) { console.warn('BD cache load failed:', e.message); }
  }
  const cachedHits = uniqueCompanyIds.filter(id => companyMap[id]).length;
  const missingCompanyIds = uniqueCompanyIds.filter(id => !companyMap[id]);
  console.log(`BD companies (cache-only): ${cachedHits} cache hits · ${missingCompanyIds.length} missing → client will lookup via /api/bd/lookup-companies (total unique: ${uniqueCompanyIds.length})`);

  const mappedDeals = deals.map(d => {
    const p = d.properties || {};
    const companyId = companyAssociations[d.id];
    const company = companyId ? companyMap[companyId] : null;
    return {
      id: d.id, dealname: p.dealname||'', dealstage: p.dealstage||'',
      brand_status: p.brand_status||'', amount: parseFloat(p.amount)||0,
      closedate: p.closedate||'', createdate: p.createdate||'',
      price_per_review: p.price_per_review||'', package_type: p.package_type||'',
      setup_fee: p.setup_fee||'', months_until_upgrade: p.months_until_upgrade||'',
      fee_after_upgrade: p.fee_after_upgrade||'',
      clinician_ai_adopted_: p.clinician_ai_adopted_||'',
      clinician_ai_price_per_product: p.clinician_ai_price_per_product||'',
      hubspot_owner_id: p.hubspot_owner_id||'',
      // v19: expanded BD column fields
      days_to_close: p.days_to_close||'',
      original_amount: parseFloat(p.original_amount)||0,
      average_monthly_web_traffic__cloned_: p.average_monthly_web_traffic__cloned_||'',
      of_products: p.of_products||'',
      month_minimum: parseFloat(p.month_minimum)||0,
      of_free_reviews: p.of_free_reviews||'',
      of_total_reviews: p.of_total_reviews||'',
      of_clinicianai: p.of_clinicianai||'',
      of_clinician_analysis: p.of_clinician_analysis||'',
      activation_fee: parseFloat(p.activation_fee)||0,
      notes: p.notes||'',
      date_demo_booked: p.date_demo_booked||'',
      rescheduled_meeting_date: p.rescheduled_meeting_date||'',
      // Billing details group (new in v22) — sourced from custom HubSpot
      // deal properties. Each is rendered as its own column on the BD
      // Tracker's "Billing Details" group.
      invoice_date: p.invoice_date||'',
      approval_date: p.approval_date||'',
      internal_recurly_link: p.internal_recurly_link||'',
      recurly_account_management_url: p.recurly_account_management_url||'',
      recurly_billing_intake_url: p.recurly_billing_intake_url||'',
      // Churn / Pause group — surfaced on the BD Tracker behind a
      // collapse chevron (only pausechurn_date is visible by default).
      pausechurn_date: p.pausechurn_date||'',
      never_implemented_churn: p.never_implemented_churn||'',
      churn_reason: p.churn_reason||'',
      detailed_reason_for_churn: p.detailed_reason_for_churn||'',
      paused_billing_length_months: p.paused_billing_length_months||'',
      temporary_pause_reason: p.temporary_pause_reason||'',
      paused_billing__billing_restart_date: p.paused_billing__billing_restart_date||'',
      // Always include the associated companyId (from associations); the
      // companyName comes from the cache if available, else empty (client
      // will fill it in via /api/bd/lookup-companies).
      companyName: company?.name||'',
      companyId: companyId ? String(companyId) : '',
      // ops_owner from the COMPANY record (resolved client-side via
      // ownerMap). Surfaces as the "Ops Rep" column on the deal table.
      ops_owner_id: company?.ops_owner||'',
      // se_rep_id from the COMPANY record's hubspot_owner_id (the company
      // owner — typically the SE assigned to that account).
      se_rep_id: company?.se_owner||'',
      // account_manager from the COMPANY record (resolved client-side via
      // ownerMap, falling back to the raw value). Surfaces as "Acct Mgr".
      account_manager: company?.account_manager||'',
    };
  });

  return { deals: mappedDeals, ownerMap, missingCompanyIds, meta: { generatedAt: new Date().toISOString(), dealCount: deals.length } };
}

// Dedicated company-lookup endpoint. Each invocation gets its own fresh
// subrequest budget — separate from /api/bd's deal + association work —
// which is the only way to handle portals with 2500+ unique BD companies
// without blowing Cloudflare's per-invocation subrequest limit.
async function lookupBDCompanies(env, companyIds) {
  const hsToken = env.HUBSPOT_TOKEN;
  if (!Array.isArray(companyIds) || companyIds.length === 0) return { companies: {}, deferred: 0, fetched: 0, cacheHits: 0 };
  // Load cache
  let companyMap = {};
  if (env.CONTENT_STORE) {
    try {
      const raw = await env.CONTENT_STORE.get('bd_company_cache_v5');
      if (raw) companyMap = JSON.parse(raw);
    } catch(e) { console.warn('BD lookup cache load failed:', e.message); }
  }
  // Filter to IDs not in cache
  const cacheHits = companyIds.filter(id => companyMap[id]).length;
  const missingIds = companyIds.filter(id => !companyMap[id]);
  // Cap to 30 batches (3000 companies) per request — plenty of headroom on
  // any plan, leaves room for the KV write at the end.
  const MAX_BATCHES = 30;
  const idsToFetch = missingIds.slice(0, MAX_BATCHES * 100);
  let fetched = 0;
  let attempted = new Set();
  for (let i = 0; i < idsToFetch.length; i += 100) {
    const batch = idsToFetch.slice(i, i + 100);
    batch.forEach(id => attempted.add(id));
    try {
      // archived=true so we also pull metadata for archived/deleted companies
      // — without this HubSpot silently drops them from `results`, leaving
      // those deals' Company column permanently blank.
      const coRes = await fetch('https://api.hubapi.com/crm/v3/objects/companies/batch/read?archived=true', {
        method: 'POST',
        headers: { Authorization: `Bearer ${hsToken}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ inputs: batch.map(id => ({ id })), properties: ['name','domain','website','ops_owner','hubspot_owner_id','account_manager'] }),
      });
      if (!coRes.ok) {
        const txt = await coRes.text();
        console.error(`BD lookup batch ${i} HTTP ${coRes.status}: ${txt.slice(0,200)}`);
        continue;
      }
      const coData = await coRes.json();
      for (const co of (coData.results || [])) {
        const name = co.properties?.name || co.properties?.domain || co.properties?.website || '';
        const ops_owner = co.properties?.ops_owner || '';
        // SE Rep = the company record's hubspot_owner_id
        const se_owner = co.properties?.hubspot_owner_id || '';
        const account_manager = co.properties?.account_manager || '';
        companyMap[co.id] = { name, id: co.id, archived: !!co.archived, ops_owner, se_owner, account_manager };
        if (name) fetched++;
      }
    } catch(e) { console.error('BD lookup batch error:', e); }
  }
  // For IDs we ATTEMPTED but HubSpot didn't return (e.g., hard-deleted,
  // missing scope), stash a placeholder so we don't keep retrying them on
  // every page load. They'll show as "—" in the table but won't re-fetch.
  let unresolvedCount = 0;
  for (const id of attempted) {
    if (!companyMap[id]) {
      companyMap[id] = { name: '', id: String(id), _unresolved: true };
      unresolvedCount++;
    }
  }
  // Persist updated cache (always when we attempted anything, so the
  // unresolved-placeholder write also lands)
  if (env.CONTENT_STORE && attempted.size > 0) {
    try {
      await env.CONTENT_STORE.put('bd_company_cache_v5', JSON.stringify(companyMap));
    } catch(e) { console.warn('BD lookup cache save failed:', e.message); }
  }
  // Build response: include every requested ID (resolved OR unresolved placeholder)
  const companies = {};
  for (const id of companyIds) {
    if (companyMap[id]) companies[id] = companyMap[id];
  }
  const deferred = missingIds.length - idsToFetch.length;
  console.log(`BD lookup: ${companyIds.length} requested · ${cacheHits} cache hits · ${fetched} newly fetched · ${unresolvedCount} unresolved (cached as placeholder) · ${deferred} deferred`);
  return { companies, deferred, fetched, cacheHits, unresolved: unresolvedCount };
}

async function fetchSalesData(mode, env) {
  const hsToken = env.HUBSPOT_TOKEN;
  const today = new Date();
  const todayStr = `${today.getUTCFullYear()}-${String(today.getUTCMonth()+1).padStart(2,'0')}-${String(today.getUTCDate()).padStart(2,'0')}`;
  const ownerMap = await fetchOwners(hsToken);

  if (mode === 'preDemoData') {
    // Fetch upcoming deals: date_demo_booked >= today OR rescheduled_meeting_date >= today
    // (the second filter group catches deals originally booked in the past but rescheduled into the future)
    const todayMs = String(new Date(todayStr + 'T00:00:00').getTime());
    const farFutureMs = String(new Date('2027-01-01T00:00:00').getTime());
    const dealsRaw = await hsSearch(hsToken, 'deals', [
      { filters: [
        { propertyName: 'date_demo_booked', operator: 'GTE', value: todayMs },
        { propertyName: 'date_demo_booked', operator: 'LTE', value: farFutureMs },
      ]},
      { filters: [
        { propertyName: 'rescheduled_meeting_date', operator: 'GTE', value: todayMs },
        { propertyName: 'rescheduled_meeting_date', operator: 'LTE', value: farFutureMs },
      ]},
    ], ['date_demo_booked','dealname','dealstage','demo_given__status','hubspot_owner_id','amount',
         'utm_source','utm_medium','utm_campaign','utm_content','createdate','hs_createdate','brand_status',
         'demo_attendance_status','demo_qualification_outcome','rescheduled_meeting_date']);
    const deals = [...new Map(dealsRaw.map(d => [d.id, d])).values()]; // dedup across filter groups
    console.log(`Sales preDemoData: ${deals.length} upcoming deals`);

    // Fetch contacts broadly — go back 90 days to catch rescheduled demos where
    // contact date_demo_booked doesn't match deal date_demo_booked
    const ninetyDaysAgo = new Date(today);
    ninetyDaysAgo.setDate(ninetyDaysAgo.getDate() - 90);
    const ninetyDaysMs = String(ninetyDaysAgo.getTime());
    const contacts = await hsSearch(hsToken, 'contacts', [{
      filters: [
        { propertyName: 'date_demo_booked', operator: 'GTE', value: ninetyDaysMs },
        { propertyName: 'date_demo_booked', operator: 'LTE', value: farFutureMs },
      ],
    }], SALES_CONTACT_PROPS);
    console.log(`Sales preDemoData: ${contacts.length} contacts`);

    // Build contact info map (keyed by company name + email domain)
    const contactInfoMap = {};
    const uniqueDomains = new Set();
    for (const c of contacts) {
      const p = c.properties || {};
      const company = (p.company || '').trim().toLowerCase();
      const email = (p.email || '');
      const domain = email.includes('@') ? email.split('@')[1].toLowerCase() : '';
      const info = {
        contactId: c.id,
        name: ((p.firstname||'') + ' ' + (p.lastname||'')).trim(),
        website: p.website || '', aboutContact: p.role_at_company || '',
        lastOpen: p.hs_sales_email_last_opened || null,
        lastClick: p.hs_sales_email_last_clicked || null,
        lastReply: p.hs_sales_email_last_replied || null,
        hasOpen: !!p.hs_sales_email_last_opened, hasClick: !!p.hs_sales_email_last_clicked,
        hasReply: !!p.hs_sales_email_last_replied,
        inSequence: p.hs_sequences_is_enrolled === 'true',
        sequenceName: p.hs_latest_sequence_enrolled || '',
        webTraffic: p.average_monthly_web_traffic || '',
        slName: p.sl_last_demo_name || '',
        slPct: p.sl_last_demo_completion_percent || '',
      };
      if (company) contactInfoMap[company] = info;
      const personalDomains = ['gmail.com','yahoo.com','hotmail.com','aol.com','outlook.com','icloud.com','protonmail.com'];
      if (domain && !personalDomains.includes(domain)) {
        contactInfoMap['_domain_' + domain] = info;
        uniqueDomains.add(domain);
      }
    }

    // Fetch company data by domain (batch)
    const companyInfoMap = {};
    const domainArr = [...uniqueDomains];
    for (let i = 0; i < domainArr.length; i += 50) {
      const batch = domainArr.slice(i, i + 50);
      try {
        const companies = await hsSearch(hsToken, 'companies', [{
          filters: [{ propertyName: 'domain', operator: 'IN', values: batch }],
        }], COMPANY_PROPS);
        for (const co of companies) {
          const cp = co.properties || {};
          const d = (cp.domain || '').toLowerCase();
          const n = (cp.name || '').toLowerCase();
          const info = {
            name: cp.name||'', industry: cp.industry||'', timezone: cp.company_time_zone||'',
            size: cp.company_size_bucket||'', revenue: cp.revenue_tier||'',
            traffic: cp.monthly_visitor_tier||'', demoPrep: cp.demo_prep_briefing||'',
            mainProducts: cp.main_products||'', targetCustomer: cp.target_customer_description||'',
            description: cp.description||'',
          };
          if (d) companyInfoMap[d] = info;
          if (n) companyInfoMap['_name_' + n] = info;
        }
      } catch(e) { console.error('Company batch fetch error:', e.message); }
    }
    console.log(`Sales preDemoData: ${Object.keys(companyInfoMap).length} companies matched`);

    // Map deals with properties
    const mappedDeals = deals.map(d => {
      const p = d.properties || {};
      return {
        id: d.id, dealname: p.dealname||'', dealstage: p.dealstage||'',
        demo_given__status: p.demo_given__status||'', date_demo_booked: p.date_demo_booked||'',
        hs_createdate: p.hs_createdate||'', createdate: p.createdate||'', hubspot_owner_id: p.hubspot_owner_id||'',
        utm_source: p.utm_source||'', utm_medium: p.utm_medium||'',
        utm_campaign: p.utm_campaign||'', utm_content: p.utm_content||'',
        amount: p.amount||'', brand_status: p.brand_status||'',
        demo_attendance_status: p.demo_attendance_status||'',
        demo_qualification_outcome: p.demo_qualification_outcome||'',
        rescheduled_meeting_date: p.rescheduled_meeting_date||'',
      };
    });

    return {
      mode: 'preDemoData', deals: mappedDeals, contactInfoMap, companyInfoMap,
      ownerMap, today: todayStr,
      meta: { generatedAt: new Date().toISOString(), dealCount: deals.length },
    };
  }

  if (mode === 'postDemoData') {
    // Fetch past deals: date_demo_booked OR rescheduled_meeting_date is in (today-90d, today)
    // (the second filter group catches deals originally booked outside the 90d window
    // but rescheduled INTO it — e.g. originally booked Jan 1, rescheduled to last week)
    const todayMs = String(new Date(todayStr + 'T00:00:00').getTime());
    const ninetyAgo = new Date(today);
    ninetyAgo.setDate(ninetyAgo.getDate() - 90);
    const ninetyAgoMs = String(ninetyAgo.getTime());
    const dealsRaw = await hsSearch(hsToken, 'deals', [
      { filters: [
        { propertyName: 'date_demo_booked', operator: 'GTE', value: ninetyAgoMs },
        { propertyName: 'date_demo_booked', operator: 'LT', value: todayMs },
      ]},
      { filters: [
        { propertyName: 'rescheduled_meeting_date', operator: 'GTE', value: ninetyAgoMs },
        { propertyName: 'rescheduled_meeting_date', operator: 'LT', value: todayMs },
      ]},
      { filters: [
        { propertyName: 'demo_attendance_status', operator: 'IN', values: ['No Show', 'Cancelled before demo'] },
        { propertyName: 'hs_createdate', operator: 'GTE', value: ninetyAgoMs },
        { propertyName: 'hs_createdate', operator: 'LT', value: todayMs },
      ]},
    ], ['date_demo_booked','dealname','dealstage','demo_given__status','hubspot_owner_id','amount',
         'utm_source','utm_medium','utm_campaign','utm_content','createdate','hs_createdate','brand_status',
         'disqualification_reason','demo_given_date','closedate',
         'demo_attendance_status','demo_qualification_outcome','rescheduled_meeting_date']);
    const deals = [...new Map(dealsRaw.map(d => [d.id, d])).values()]; // dedup across filter groups
    console.log(`Sales postDemoData: ${deals.length} past deals`);

    // Contacts: same 90-day window
    const contacts = await hsSearch(hsToken, 'contacts', [{
      filters: [
        { propertyName: 'date_demo_booked', operator: 'GTE', value: ninetyAgoMs },
        { propertyName: 'date_demo_booked', operator: 'LT', value: String(new Date('2027-01-01T00:00:00').getTime()) },
      ],
    }], SALES_CONTACT_PROPS);
    console.log(`Sales postDemoData: ${contacts.length} contacts`);

    const contactInfoMap = {};
    const uniqueDomains = new Set();
    for (const c of contacts) {
      const p = c.properties || {};
      const company = (p.company || '').trim().toLowerCase();
      const email = (p.email || '');
      const domain = email.includes('@') ? email.split('@')[1].toLowerCase() : '';
      const info = {
        contactId: c.id,
        name: ((p.firstname||'') + ' ' + (p.lastname||'')).trim(),
        website: p.website || '', aboutContact: p.role_at_company || '',
        lastOpen: p.hs_sales_email_last_opened || null,
        lastClick: p.hs_sales_email_last_clicked || null,
        lastReply: p.hs_sales_email_last_replied || null,
        hasOpen: !!p.hs_sales_email_last_opened, hasClick: !!p.hs_sales_email_last_clicked,
        hasReply: !!p.hs_sales_email_last_replied,
        inSequence: p.hs_sequences_is_enrolled === 'true',
        sequenceName: p.hs_latest_sequence_enrolled || '',
        webTraffic: p.average_monthly_web_traffic || '',
        slName: p.sl_last_demo_name || '',
        slPct: p.sl_last_demo_completion_percent || '',
      };
      if (company) contactInfoMap[company] = info;
      const personalDomains = ['gmail.com','yahoo.com','hotmail.com','aol.com','outlook.com','icloud.com','protonmail.com'];
      if (domain && !personalDomains.includes(domain)) {
        contactInfoMap['_domain_' + domain] = info;
        uniqueDomains.add(domain);
      }
    }

    // Company data
    const companyInfoMap = {};
    const domainArr = [...uniqueDomains];
    for (let i = 0; i < domainArr.length; i += 50) {
      const batch = domainArr.slice(i, i + 50);
      const companyResults = await hsSearch(hsToken, 'companies', [{
        filters: [{ propertyName: 'domain', operator: 'IN', values: batch }],
      }], ['domain','name','industry','company_time_zone','company_size_bucket','revenue_tier',
           'monthly_visitor_tier','demo_prep_briefing','main_products','target_customer_description','description']);
      for (const co of companyResults) {
        const cp = co.properties || {};
        const d = (cp.domain || '').toLowerCase();
        if (d) companyInfoMap[d] = {
          industry: cp.industry||'', timezone: cp.company_time_zone||'',
          size: cp.company_size_bucket||'', revenue: cp.revenue_tier||'',
          traffic: cp.monthly_visitor_tier||'', demoPrep: cp.demo_prep_briefing||'',
          mainProducts: cp.main_products||'', targetCustomer: cp.target_customer_description||'',
          description: cp.description||'',
        };
        const nm = (cp.name || '').trim().toLowerCase();
        if (nm) companyInfoMap['_name_' + nm] = companyInfoMap[d];
      }
    }

    const mappedDeals = deals.map(d => {
      const p = d.properties || {};
      return {
        id: d.id, dealname: p.dealname||'', dealstage: p.dealstage||'',
        demo_given__status: p.demo_given__status||'', date_demo_booked: p.date_demo_booked||'',
        hs_createdate: p.hs_createdate||'', createdate: p.createdate||'', hubspot_owner_id: p.hubspot_owner_id||'',
        utm_source: p.utm_source||'', utm_medium: p.utm_medium||'',
        amount: p.amount||'', brand_status: p.brand_status||'',
        disqualification_reason: p.disqualification_reason||'',
        demo_given_date: p.demo_given_date||'', closedate: p.closedate||'',
        demo_attendance_status: p.demo_attendance_status||'',
        demo_qualification_outcome: p.demo_qualification_outcome||'',
        rescheduled_meeting_date: p.rescheduled_meeting_date||'',
      };
    });

    return {
      mode: 'postDemoData', deals: mappedDeals, contactInfoMap, companyInfoMap,
      ownerMap, today: todayStr,
      meta: { generatedAt: new Date().toISOString(), dealCount: deals.length },
    };
  }

  return { error: 'Unknown mode: ' + mode };
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    // CORS preflight
    if (request.method === 'OPTIONS') return new Response(null, { headers: { 'Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST, OPTIONS','Access-Control-Allow-Headers':'Content-Type','Access-Control-Max-Age':'86400' } });

    // GET → serve the dashboard HTML
    if (request.method === 'GET' && (url.pathname === '/' || url.pathname === '')) {
      return new Response(DASHBOARD_HTML, { headers: { 'Content-Type':'text/html;charset=UTF-8','Cache-Control':'no-cache' } });
    }

    // GET /sales → serve the sales dashboard HTML
    if (request.method === 'GET' && url.pathname === '/sales') {
      return new Response(SALES_HTML, { headers: { 'Content-Type':'text/html;charset=UTF-8','Cache-Control':'no-cache' } });
    }

    // GET /bd → serve the BD dashboard HTML
    if (request.method === 'GET' && url.pathname === '/bd') {
      return new Response(BD_HTML, { headers: { 'Content-Type':'text/html;charset=UTF-8','Cache-Control':'no-cache' } });
    }

    // POST /api/data → Dashboard API
    if (request.method === 'POST' && url.pathname === '/api/data') {
      let body;
      try { body = await request.json(); } catch { return jr({ error: 'Invalid JSON' }, 400); }
      if (body.password !== env.TEAM_PASSWORD) return jr({ error: 'Unauthorized' }, 401);
      // Short (60s) response cache. Rapid refreshes / tab-switches otherwise
      // re-run the full HubSpot fetch chain every time; under repeated use that
      // rate-limits HubSpot and makes the worker slow/unresponsive, which froze
      // the dashboard (the fetch never returns) and returned partial data. A
      // recent payload is served instead. The Refresh button sends fresh:true to
      // bypass; only successful (non-error) payloads are cached.
      const _win = body.window || '7d';
      const _ck = 'apidata_resp_v6_' + _win + '_' + (body.from||'') + '_' + (body.to||'') + '_' + (body.vsFrom||'') + '_' + (body.vsTo||'');
      if (!body.fresh && env.CONTENT_STORE) {
        try {
          const raw = await env.CONTENT_STORE.get(_ck);
          if (raw) { const p = JSON.parse(raw); if (p && typeof p.t === 'number' && (Date.now() - p.t) < 60000 && p.data) return jr(p.data); }
        } catch(e) { /* fall through to live compute */ }
      }
      try {
        const result = await processRequest(_win, body.from||null, body.to||null, env, body.vsFrom||null, body.vsTo||null);
        // Only cache COMPLETE responses. A transient partial fetch (subrequest
        // budget / HubSpot rate limit) yields blank current metrics (True CPD)
        // and/or missing prior data; caching that would persist blank cards and
        // dead vs-prior / vs-LM deltas across every page for the full TTL.
        // Healthy = current True CPD present AND, when a prior window exists,
        // its prior comparison present. Degraded responses are still returned
        // (so the user sees something) but NOT cached, so the next load retries.
        const _tc = result && result.executiveSummary && result.executiveSummary.trueCpd;
        const _healthy = result && !result.error && _tc && _tc.value != null && (!result.priorPeriod || _tc.sameTimePrior != null);
        if (env.CONTENT_STORE && _healthy) {
          try { const blob = JSON.stringify({ t: Date.now(), data: result }); if (blob.length < 20*1024*1024) await env.CONTENT_STORE.put(_ck, blob, { expirationTtl: 120 }); } catch(e) { /* best-effort */ }
        }
        return jr(result);
      } catch(err) { console.error('Error:', err); return jr({ error: 'Internal error', detail: (err && (err.message || String(err))) || 'unknown', stack: (err && err.stack ? String(err.stack).split('\n').slice(0,4).join(' | ') : null) }, 500); }
    }

    // POST /api/revenue-outcome → dedicated, UNCACHED Revenue Outcome endpoint.
    // Its own fresh subrequest budget computes current/prior/last-month
    // closed-won MRR + count live, so the card's vs-prior / vs-LM deltas are
    // always accurate (not starved by /api/data's big fetch chain, not cached).
    if (request.method === 'POST' && url.pathname === '/api/revenue-outcome') {
      let body;
      try { body = await request.json(); } catch { return jr({ error: 'Invalid JSON' }, 400); }
      if (body.password !== env.TEAM_PASSWORD) return jr({ error: 'Unauthorized' }, 401);
      try {
        const result = await processRevenueOutcome(env, body.window||'mtd', body.from||null, body.to||null, body.vsFrom||null, body.vsTo||null);
        return jr(result);
      } catch(err) { console.error('revenue-outcome error:', err); return jr({ error: 'Internal error', detail: (err && (err.message || String(err))) || 'unknown' }, 500); }
    }

    // POST /api/signup-cohorts → dedicated SignUp Rate endpoint.
    // Runs JUST the per-month cohort fetch + owners, builds cohorts. No
    // Phase 2 / Windsor / Irfan PCM work. Lets older months (Feb, March)
    // get a fresh subrequest budget so they actually return data.
    if (request.method === 'POST' && url.pathname === '/api/signup-cohorts') {
      let body;
      try { body = await request.json(); } catch { return jr({ error: 'Invalid JSON' }, 400); }
      if (body.password !== env.TEAM_PASSWORD) return jr({ error: 'Unauthorized' }, 401);
      try {
        const result = await processSignupRequest(env);
        return jr(result);
      } catch(err) { console.error('SignUp endpoint error:', err); return jr({ error: 'Internal error', detail: err.message }, 500); }
    }

    // POST /api/special1-cohorts → dedicated Irfan Special #1 endpoint.
    // Always builds Last Month + MTD cohorts from a dedicated deal fetch,
    // independent of the page time selector. Dashboard caches the result
    // and doesn't refetch when the user changes page windows — so the
    // Special #1 card's numbers stay fixed regardless of page state.
    if (request.method === 'POST' && url.pathname === '/api/special1-cohorts') {
      let body;
      try { body = await request.json(); } catch { return jr({ error: 'Invalid JSON' }, 400); }
      if (body.password !== env.TEAM_PASSWORD) return jr({ error: 'Unauthorized' }, 401);
      try {
        const result = await processSpecial1Request(env);
        return jr(result);
      } catch(err) { console.error('Special1 endpoint error:', err); return jr({ error: 'Internal error', detail: err.message }, 500); }
    }

    // POST /api/webinar-performance → Webinar vs Demo funnel comparison page
    if (request.method === 'POST' && url.pathname === '/api/webinar-performance') {
      let body;
      try { body = await request.json(); } catch { return jr({ error: 'Invalid JSON' }, 400); }
      if (body.password !== env.TEAM_PASSWORD) return jr({ error: 'Unauthorized' }, 401);
      try {
        const result = await processWebinarPerfRequest(body.window||'mtd', body.from||null, body.to||null, env, body.vsFrom||null, body.vsTo||null);
        return jr(result);
      } catch(err) { console.error('Webinar perf error:', err); return jr({ error: 'Internal error', detail: err.message }, 500); }
    }

    // POST /api/baseline → load editable webinar-funnel baselines from KV
    if (request.method === 'POST' && url.pathname === '/api/baseline') {
      let body;
      try { body = await request.json(); } catch { return jr({ error: 'Invalid JSON' }, 400); }
      if (body.password !== env.TEAM_PASSWORD) return jr({ error: 'Unauthorized' }, 401);
      try {
        const v = await readWebinarBaseline(env);
        return jr({ ok: true, attendedToClosedWonPct: v });
      } catch(err) { return jr({ error: 'Internal error', detail: err.message }, 500); }
    }
    // POST /api/baseline/save → persist a new baseline value to KV
    if (request.method === 'POST' && url.pathname === '/api/baseline/save') {
      let body;
      try { body = await request.json(); } catch { return jr({ error: 'Invalid JSON' }, 400); }
      if (body.password !== env.TEAM_PASSWORD) return jr({ error: 'Unauthorized' }, 401);
      try {
        const res = await saveWebinarBaseline(env, body.attendedToClosedWonPct);
        return jr(res);
      } catch(err) { return jr({ error: 'Internal error', detail: err.message }, 500); }
    }

    // POST /api/analyzer → Paid Channel Analyzer API
    if (request.method === 'POST' && url.pathname === '/api/analyzer') {
      let body;
      try { body = await request.json(); } catch { return jr({ error: 'Invalid JSON' }, 400); }
      if (body.password !== env.TEAM_PASSWORD) return jr({ error: 'Unauthorized' }, 401);
      try {
        const result = await processAzRequest(body.window||'mtd', body.from||null, body.to||null, env, body.vsFrom||null, body.vsTo||null);
        return jr(result);
      } catch(err) { console.error('Analyzer error:', err); return jr({ error: 'Internal error', detail: err.message }, 500); }
    }

    // POST /api/content → Content Studio static data
    if (request.method === 'POST' && url.pathname === '/api/content') {
      let body;
      try { body = await request.json(); } catch { return jr({ error: 'Invalid JSON' }, 400); }
      if (body.password !== env.TEAM_PASSWORD) return jr({ error: 'Unauthorized' }, 401);
      // Merge saved state if KV is available
      let saved = null;
      if (env.CONTENT_STORE) {
        try {
          const raw = await env.CONTENT_STORE.get('content_state');
          if (raw) saved = JSON.parse(raw);
        } catch(e) { /* KV read failed, use defaults */ }
      }
      const response = { ...CONTENT_DATA };
      if (saved) response._saved = saved;
      return jr(response);
    }

    // POST /api/content/ai → Anthropic API proxy for Content Studio AI features
    if (request.method === 'POST' && url.pathname === '/api/content/ai') {
      let body;
      try { body = await request.json(); } catch { return jr({ error: 'Invalid JSON' }, 400); }
      if (body.password !== env.TEAM_PASSWORD) return jr({ error: 'Unauthorized' }, 401);
      try {
        const result = await proxyAnthropicAI(body.aiBody, env);
        return jr(result);
      } catch(err) { return jr({ error: 'AI proxy error', detail: err.message }, 500); }
    }

    // POST /api/pq → Read prequalification results from KV (synced across devices)
    // Returns the global PQ map: { [dealId]: { pq: 'Y'|'N'|'S'|'?', reason: '...' } }
    if (request.method === 'POST' && url.pathname === '/api/pq') {
      let body;
      try { body = await request.json(); } catch { return jr({ error: 'Invalid JSON' }, 400); }
      if (body.password !== env.TEAM_PASSWORD) return jr({ error: 'Unauthorized' }, 401);
      if (!env.CONTENT_STORE) return jr({ pqMap: {}, _kvAvailable: false });
      try {
        const raw = await env.CONTENT_STORE.get('pq_results');
        const pqMap = raw ? JSON.parse(raw) : {};
        return jr({ pqMap, _kvAvailable: true });
      } catch(err) { return jr({ pqMap: {}, _kvAvailable: true, _readError: err.message }); }
    }

    // POST /api/pq/save → Save prequalification results to KV (synced across devices)
    // Body: { password, pqMap: {[dealId]: {pq, reason}}, mode: 'merge'|'replace' }
    // - 'merge' (default): merges body.pqMap into stored map, body wins on conflict
    // - 'replace': overwrites stored map entirely (used by Clear All)
    // POST /api/csession/actions → Creative Session per-creative action log.
    // Body shape:
    //   { password, load: true }                          → returns { ok, actions }
    //   { password, actions: { '<name>': {action,ts,...} } }  → merges into KV
    //   { password, actions: { '<name>': null } }         → removes that entry
    // Stored at KV key 'csession_actions' as a flat { creativeName -> entry } map.
    if (request.method === 'POST' && url.pathname === '/api/csession/actions') {
      let body;
      try { body = await request.json(); } catch { return jr({ error: 'Invalid JSON' }, 400); }
      if (body.password !== env.TEAM_PASSWORD) return jr({ error: 'Unauthorized' }, 401);
      if (!env.CONTENT_STORE) return jr({ ok: false, error: 'KV not configured' }, 200);
      try {
        let existing = {};
        try {
          const raw = await env.CONTENT_STORE.get('csession_actions');
          if (raw) existing = JSON.parse(raw);
        } catch(e) { /* treat as empty */ }
        // Load-only request
        if (body.load) return jr({ ok: true, actions: existing });
        // Save: merge incoming, allow null to delete
        const incoming = body.actions || {};
        const final = { ...existing };
        for (const k of Object.keys(incoming)) {
          if (incoming[k] == null) { delete final[k]; }
          else { final[k] = incoming[k]; }
        }
        await env.CONTENT_STORE.put('csession_actions', JSON.stringify(final));
        return jr({ ok: true, count: Object.keys(final).length, savedAt: new Date().toISOString(), actions: final });
      } catch(err) { return jr({ ok: false, error: 'Save error', detail: err.message }, 500); }
    }

    if (request.method === 'POST' && url.pathname === '/api/pq/save') {
      let body;
      try { body = await request.json(); } catch { return jr({ error: 'Invalid JSON' }, 400); }
      if (body.password !== env.TEAM_PASSWORD) return jr({ error: 'Unauthorized' }, 401);
      if (!env.CONTENT_STORE) {
        return jr({ ok: false, error: 'KV not configured. Run: npx wrangler kv namespace create CONTENT_STORE' }, 200);
      }
      try {
        const incoming = body.pqMap || {};
        const mode = body.mode === 'replace' ? 'replace' : 'merge';
        let final = incoming;
        if (mode === 'merge') {
          let existing = {};
          try {
            const raw = await env.CONTENT_STORE.get('pq_results');
            if (raw) existing = JSON.parse(raw);
          } catch(e) { /* read failed, treat as empty */ }
          final = { ...existing, ...incoming };
        }
        await env.CONTENT_STORE.put('pq_results', JSON.stringify(final));
        return jr({ ok: true, count: Object.keys(final).length, savedAt: new Date().toISOString() });
      } catch(err) { return jr({ ok: false, error: 'Save error', detail: err.message }, 500); }
    }

    // POST /api/content/save → Save content modifications to KV
    if (request.method === 'POST' && url.pathname === '/api/content/save') {
      let body;
      try { body = await request.json(); } catch { return jr({ error: 'Invalid JSON' }, 400); }
      if (body.password !== env.TEAM_PASSWORD) return jr({ error: 'Unauthorized' }, 401);
      try {
        if (env.CONTENT_STORE) {
          await env.CONTENT_STORE.put('content_state', JSON.stringify({
            posts: body.posts || [],
            irfan: body.irfan || [],
            ideas: body.ideas || [],
            done: body.done || {},
            savedAt: new Date().toISOString(),
          }));
          return jr({ ok: true, savedAt: new Date().toISOString() });
        } else {
          return jr({ ok: false, error: 'KV not configured. Run: npx wrangler kv namespace create CONTENT_STORE' }, 200);
        }
      } catch(err) { return jr({ error: 'Save error', detail: err.message }, 500); }
    }

    // POST /api/websites → Lazy website URL lookup (separate invocation, own subrequest budget)
    // Takes deal IDs, returns {dealId: domain} map via v3 company associations
    if (request.method === 'POST' && url.pathname === '/api/websites') {
      let body;
      try { body = await request.json(); } catch { return jr({ error: 'Invalid JSON' }, 400); }
      if (body.password !== env.TEAM_PASSWORD) return jr({ error: 'Unauthorized' }, 401);
      const dealIds = (body.dealIds || []).slice(0, 10); // max 10 per batch
      if (!dealIds.length) return jr({ websites: {} });
      const hsToken = env.HUBSPOT_TOKEN;
      if (!hsToken) return jr({ websites: {} });
      try {
        const companyAssoc = {};
        // Sequential association lookups (1 at a time to avoid 429s and subrequest limits)
        for (let i = 0; i < dealIds.length; i++) {
          const did = dealIds[i];
          for (let attempt = 0; attempt < 3; attempt++) {
            try {
              const r = await fetch(`https://api.hubapi.com/crm/v3/objects/deals/${did}/associations/companies`, {
                headers: { 'Authorization': `Bearer ${hsToken}` }
              });
              if (r.status === 429) { await sleep(500 * (attempt + 1)); continue; }
              if (!r.ok) break;
              const data = await r.json();
              const cid = data.results?.[0]?.id || data.results?.[0]?.toObjectId;
              if (cid) companyAssoc[String(did)] = String(cid);
              break;
            } catch(e) { break; }
          }
        }
        // Batch read companies
        const uniqueIds = [...new Set(Object.values(companyAssoc))];
        const websites = {};
        if (uniqueIds.length) {
          const r = await fetch('https://api.hubapi.com/crm/v3/objects/companies/batch/read', {
            method: 'POST',
            headers: { 'Authorization': `Bearer ${hsToken}`, 'Content-Type': 'application/json' },
            body: JSON.stringify({ inputs: uniqueIds.map(id => ({ id })), properties: ['domain', 'website'] }),
          });
          if (r.ok) {
            const d = await r.json();
            const companyDomains = {};
            for (const c of (d.results || [])) {
              companyDomains[String(c.id)] = c.properties?.domain || c.properties?.website || '';
            }
            for (const [did, cid] of Object.entries(companyAssoc)) {
              if (companyDomains[cid]) websites[did] = companyDomains[cid];
            }
          }
        }
        console.log(`/api/websites: ${dealIds.length} in → ${Object.keys(companyAssoc).length} assoc → ${Object.keys(websites).length} domains`);
        return jr({ websites });
      } catch(err) { console.error('Website API error:', err.message); return jr({ websites: {} }); }
    }

    // POST /api/engagement → HubSpot contact email engagement data for deals
    if (request.method === 'POST' && url.pathname === '/api/engagement') {
      let body;
      try { body = await request.json(); } catch { return jr({ error: 'Invalid JSON' }, 400); }
      if (body.password !== env.TEAM_PASSWORD) return jr({ error: 'Unauthorized' }, 401);
      const dealIds = (body.dealIds || []).slice(0, 10);
      if (!dealIds.length) return jr({ engagement: {} });
      const hsToken = env.HUBSPOT_TOKEN;
      if (!hsToken) return jr({ engagement: {} });
      try {
        // Step 1: Sequential association lookups (deal → contact)
        const dealContactMap = {};
        for (let i = 0; i < dealIds.length; i++) {
          const did = dealIds[i];
          for (let attempt = 0; attempt < 3; attempt++) {
            try {
              const r = await fetch(`https://api.hubapi.com/crm/v3/objects/deals/${did}/associations/contacts`, {
                headers: { Authorization: `Bearer ${hsToken}` },
              });
              if (r.status === 429) { await sleep(500 * (attempt + 1)); continue; }
              if (!r.ok) break;
              const d = await r.json();
              const cid = d.results?.[0]?.id || d.results?.[0]?.toObjectId;
              if (cid) dealContactMap[String(did)] = String(cid);
              break;
            } catch { break; }
          }
        }
        // Step 2: Batch-read contacts with engagement properties
        const contactIds = [...new Set(Object.values(dealContactMap))];
        const engagementProps = [
          'hs_sales_email_last_opened','hs_sales_email_last_clicked','hs_sales_email_last_replied',
          'notes_last_contacted',
          'hs_sequences_is_enrolled','hs_latest_sequence_enrolled','hs_latest_sequence_enrolled_date'
        ];
        const contactData = {};
        if (contactIds.length) {
          const batchBody = { inputs: contactIds.map(id => ({ id })), properties: engagementProps };
          const batchRes = await fetch('https://api.hubapi.com/crm/v3/objects/contacts/batch/read', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${hsToken}` },
            body: JSON.stringify(batchBody),
          });
          if (batchRes.ok) {
            const bd = await batchRes.json();
            for (const c of (bd.results || [])) {
              const p = c.properties || {};
              contactData[c.id] = {
                lastOpen: p.hs_sales_email_last_opened || null,
                lastClick: p.hs_sales_email_last_clicked || null,
                lastReply: p.hs_sales_email_last_replied || null,
                lastContacted: p.notes_last_contacted || null,
                hasOpen: !!p.hs_sales_email_last_opened,
                hasClick: !!p.hs_sales_email_last_clicked,
                hasReply: !!p.hs_sales_email_last_replied,
                inSequence: p.hs_sequences_is_enrolled === 'true',
                sequenceName: p.hs_latest_sequence_enrolled || '',
                seqEnrolledDate: p.hs_latest_sequence_enrolled_date || null,
              };
            }
          }
        }
        // Step 3: Map back to deal IDs
        const engagement = {};
        for (const [did, cid] of Object.entries(dealContactMap)) {
          engagement[did] = contactData[cid] || null;
        }
        console.log(`/api/engagement: ${dealIds.length} deals → ${contactIds.length} contacts → ${Object.keys(engagement).length} with data`);
        return jr({ engagement });
      } catch(err) { console.error('Engagement error:', err.message); return jr({ engagement: {}, error: err.message }); }
    }

    // POST /api/outbound → Apollo sequence data
    if (request.method === 'POST' && url.pathname === '/api/outbound') {
      console.log('/api/outbound: handler reached');
      let body;
      try { body = await request.json(); } catch { return jr({ error: 'Invalid JSON' }, 400); }
      if (body.password !== env.TEAM_PASSWORD) { console.log('/api/outbound: bad password'); return jr({ error: 'Unauthorized' }, 401); }
      const apolloKey = env.APOLLO_API_KEY;
      if (!apolloKey) { console.log('/api/outbound: no APOLLO_API_KEY'); return jr({ error: 'APOLLO_API_KEY not configured. Run: npx wrangler secret put APOLLO_API_KEY' }); }
      console.log('/api/outbound: key present, length=' + apolloKey.length);
      try {
        // Fetch all sequences (this has built-in stats — main data source)
        const seqRes = await fetch('https://api.apollo.io/api/v1/emailer_campaigns/search', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'x-api-key': apolloKey },
          body: JSON.stringify({ per_page: '50' }),
        });
        if (!seqRes.ok) {
          const errTxt = await seqRes.text().catch(()=>'');
          console.error(`Apollo seq failed: ${seqRes.status} ${errTxt.substring(0,300)}`);
          return jr({ error: `Apollo API error: ${seqRes.status}`, detail: errTxt.substring(0,200) });
        }
        const seqData = await seqRes.json();
        const sequences = (seqData.emailer_campaigns || []).map(s => ({
          id: s.id, name: s.name, active: s.active, num_steps: s.num_steps,
          scheduled: s.unique_scheduled||0, delivered: s.unique_delivered||0,
          opened: s.unique_opened_unfiltered||s.unique_opened||0,
          clicked: s.unique_clicked_unfiltered||s.unique_clicked||0,
          replied: s.unique_replied||0, bounced: s.unique_bounced||0,
          unsubscribed: s.unique_unsubscribed||0, demoed: s.unique_demoed||0,
          spam_blocked: s.unique_spam_blocked||0,
          // Use Apollo's pre-calculated rates (unfiltered = includes bots, matches Apollo UI)
          open_rate: (s.open_rate_unfiltered||s.open_rate||0)*100,
          click_rate: (s.click_rate_unfiltered||s.click_rate||0)*100,
          reply_rate: (s.reply_rate||0)*100,
          bounce_rate: (s.bounce_rate||0)*100,
          demo_rate: (s.demo_rate||0)*100,
          last_used: s.last_used_at||'', created: s.created_at||'',
        }));
        console.log(`/api/outbound: ${sequences.length} sequences (${sequences.filter(s=>s.active).length} active)`);

        // Fetch HubSpot deals with utm_medium = 'abm' for demo count
        let abmDemos = 0;
        const hsToken = env.HUBSPOT_TOKEN;
        if (hsToken) {
          try {
            const abmRes = await fetch('https://api.hubapi.com/crm/v3/objects/deals/search', {
              method: 'POST',
              headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${hsToken}` },
              body: JSON.stringify({
                filterGroups: [{ filters: [
                  { propertyName: 'utm_medium', operator: 'EQ', value: 'abm' },
                  { propertyName: 'date_demo_booked', operator: 'HAS_PROPERTY' },
                ]}],
                properties: ['utm_medium'],
                limit: 1,
              }),
            });
            if (abmRes.ok) {
              const abmData = await abmRes.json();
              abmDemos = abmData.total || 0;
            }
            console.log(`/api/outbound: ABM demos = ${abmDemos}`);
          } catch(e) { console.log('ABM demo count error (non-fatal):', e.message); }
        }

        // Try analytics trend (optional — skip if fails)
        let trend = [];
        try {
          const trendRes = await fetch('https://api.apollo.io/api/v1/analytics/report/sync', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'x-api-key': apolloKey },
            body: JSON.stringify({
              metrics: ['num_emails_sent','num_emails_opened','num_emails_replied','num_emails_clicked'],
              date_range: { modality: 'last_3_months' },
              group_by: ['smart_datetime_week'],
            }),
          });
          if (trendRes.ok) {
            const td = await trendRes.json();
            trend = (td.data || td.results || td.rows || []).map(r => {
              const dims = r.dimension_values || r.dimensions || [];
              return { week: dims[0]||r.group||'', sent: r.num_emails_sent||r.metrics?.[0]||0, opened: r.num_emails_opened||r.metrics?.[1]||0, replied: r.num_emails_replied||r.metrics?.[2]||0, clicked: r.num_emails_clicked||r.metrics?.[3]||0 };
            });
          } else { console.log('Apollo trend skipped:', trendRes.status); }
        } catch(e) { console.log('Apollo trend error (non-fatal):', e.message); }

        // Try step-level data per active sequence (optional)
        const activeSeqs = sequences.filter(s => s.active);
        let stepData = {};
        for (const seq of activeSeqs.slice(0, 3)) {
          try {
            const stepRes = await fetch('https://api.apollo.io/api/v1/analytics/report/sync', {
              method: 'POST',
              headers: { 'Content-Type': 'application/json', 'x-api-key': apolloKey },
              body: JSON.stringify({
                metrics: ['num_emails_sent','num_emails_opened','num_emails_replied','num_emails_clicked'],
                date_range: { modality: 'all_time' },
                filters: { emailer_campaign_ids: [seq.id] },
                group_by: ['emailer_touch_id'],
              }),
            });
            if (stepRes.ok) {
              const sd = await stepRes.json();
              stepData[seq.id] = (sd.data || sd.results || sd.rows || []).map(r => {
                const dims = r.dimension_values || r.dimensions || [];
                return { step: dims[0]||r.group||'', sent: r.num_emails_sent||0, opened: r.num_emails_opened||0, replied: r.num_emails_replied||0, clicked: r.num_emails_clicked||0 };
              });
            }
          } catch(e) { /* step data optional */ }
        }
        return jr({ sequences, trend, stepData, abmDemos, generatedAt: new Date().toISOString() });
      } catch(err) { console.error('Outbound error:', err.message); return jr({ error: 'Outbound error', detail: err.message }, 500); }
    }

    // POST /api/content/adlibrary → Meta Ad Library API (real competitor ads)
    if (request.method === 'POST' && url.pathname === '/api/content/adlibrary') {
      let body;
      try { body = await request.json(); } catch { return jr({ error: 'Invalid JSON' }, 400); }
      if (body.password !== env.TEAM_PASSWORD) return jr({ error: 'Unauthorized' }, 401);
      if (!env.META_AD_LIBRARY_TOKEN) return jr({ error: 'META_AD_LIBRARY_TOKEN not configured. Run: npx wrangler secret put META_AD_LIBRARY_TOKEN' }, 200);
      try {
        const searchTerm = body.search_term || '';
        const country = body.country || 'US';
        const limit = body.limit || 5;
        const fields = [
          'id', 'ad_creative_bodies', 'ad_creative_link_titles',
          'ad_creative_link_descriptions', 'ad_creative_link_captions',
          'ad_snapshot_url', 'ad_delivery_start_time', 'ad_delivery_stop_time',
          'page_name', 'publisher_platforms', 'impressions', 'spend',
          'estimated_audience_size', 'languages'
        ].join(',');

        const graphUrl = `https://graph.facebook.com/v25.0/ads_archive?` +
          `search_terms=${encodeURIComponent(searchTerm)}` +
          `&ad_reached_countries=['${country}']` +
          `&ad_type=ALL` +
          `&ad_active_status=ACTIVE` +
          `&fields=${fields}` +
          `&limit=${limit}` +
          `&access_token=${env.META_AD_LIBRARY_TOKEN}`;

        const res = await fetch(graphUrl);
        const data = await res.json();

        if (data.error) {
          return jr({ error: 'Meta API error', detail: data.error.message || JSON.stringify(data.error).substring(0, 300), code: data.error.code }, 200);
        }

        // Transform to clean format
        const ads = (data.data || []).map(ad => ({
          id: ad.id,
          page_name: ad.page_name || searchTerm,
          headline: (ad.ad_creative_link_titles || [])[0] || '',
          body: (ad.ad_creative_bodies || [])[0] || '',
          description: (ad.ad_creative_link_descriptions || [])[0] || '',
          caption: (ad.ad_creative_link_captions || [])[0] || '',
          snapshot_url: ad.ad_snapshot_url || '',
          started: ad.ad_delivery_start_time || '',
          stopped: ad.ad_delivery_stop_time || null,
          platforms: ad.publisher_platforms || [],
          impressions: ad.impressions || null,
          spend: ad.spend || null,
          audience_size: ad.estimated_audience_size || null,
          library_url: `https://www.facebook.com/ads/library/?id=${ad.id}`,
        }));

        return jr({ ok: true, search_term: searchTerm, country, count: ads.length, ads, has_more: !!data.paging?.next });
      } catch(err) { return jr({ error: 'Ad Library fetch error', detail: err.message }, 500); }
    }

    // POST /api/content/imagegen → Gemini image generation
    if (request.method === 'POST' && url.pathname === '/api/content/imagegen') {
      let body;
      try { body = await request.json(); } catch { return jr({ error: 'Invalid JSON' }, 400); }
      if (body.password !== env.TEAM_PASSWORD) return jr({ error: 'Unauthorized' }, 401);
      if (!env.GEMINI_API_KEY) return jr({ error: 'GEMINI_API_KEY not configured. Run: npx wrangler secret put GEMINI_API_KEY' }, 200);
      try {
        const prompt = body.prompt || 'A professional marketing ad creative';
        const aspectRatio = body.aspectRatio || '1:1';

        // Gemini 3.1 Flash Image Preview (Nano Banana 2) — matches Google's exact REST format
        const geminiRes = await fetch(
          `https://generativelanguage.googleapis.com/v1beta/models/gemini-3.1-flash-image-preview:generateContent?key=${env.GEMINI_API_KEY}`,
          {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              contents: [{ role: 'user', parts: [{ text: prompt }] }],
              generationConfig: {
                responseModalities: ['IMAGE', 'TEXT'],
                thinkingConfig: { thinkingLevel: 'MINIMAL' },
                imageConfig: { aspectRatio: aspectRatio }
              }
            })
          }
        );

        if (geminiRes.ok) {
          const data = await geminiRes.json();
          const parts = data.candidates?.[0]?.content?.parts || [];
          const imgPart = parts.find(p => p.inlineData);
          if (imgPart) {
            return jr({ ok: true, image: imgPart.inlineData.data, mimeType: imgPart.inlineData.mimeType || 'image/png', model: 'gemini-3.1-flash-image-preview' });
          }
          // Model returned text but no image
          const textPart = parts.find(p => p.text);
          return jr({ error: 'No image generated', detail: textPart?.text || 'Model returned no image data' }, 200);
        }

        const errText = await geminiRes.text().catch(() => 'Unknown error');
        return jr({ error: 'Image generation failed', detail: errText.substring(0, 400) }, 200);
      } catch(err) { return jr({ error: 'Image generation error', detail: err.message }, 500); }
    }

    // POST /api/content/social → Live organic social data from Windsor
    if (request.method === 'POST' && url.pathname === '/api/content/social') {
      let body;
      try { body = await request.json(); } catch { return jr({ error: 'Invalid JSON' }, 400); }
      if (body.password !== env.TEAM_PASSWORD) return jr({ error: 'Unauthorized' }, 401);
      try {
        const apiKey = env.WINDSOR_API_KEY;
        const period = body.period || 'last_30d';
        const baseUrl = 'https://connectors.windsor.ai';
        // Support explicit date range for prior period comparison
        const dateParam = body.dateFrom && body.dateTo
          ? `date_from=${body.dateFrom}&date_to=${body.dateTo}`
          : `date_preset=${period}`;

        // Fetch all 3 organic channels in parallel
        const [liDaily, liPosts, igDaily, igPosts, ttDaily, ttPosts] = await Promise.all([
          fetch(`${baseUrl}/linkedin_organic?api_key=${apiKey}&${dateParam}&fields=date,organization_follower_count,account_analytics_impression_count,account_analytics_click_count,account_analytics_like_count,account_analytics_comment_count,account_analytics_share_count,account_analytics_engagement&page_size=5000`).then(r=>r.json()).then(d=>d.data||[]).catch(()=>[]),
          fetch(`${baseUrl}/linkedin_organic?api_key=${apiKey}&${dateParam}&fields=share_text,share_impression_count,share_clicks_count,share_like_count,share_comment_count,share_published_time,share_url&page_size=5000`).then(r=>r.json()).then(d=>d.data||[]).catch(()=>[]),
          fetch(`${baseUrl}/instagram?api_key=${apiKey}&${dateParam}&fields=date,followers_count,reach,likes,comments,shares,saves&page_size=5000`).then(r=>r.json()).then(d=>d.data||[]).catch(()=>[]),
          fetch(`${baseUrl}/instagram?api_key=${apiKey}&${dateParam}&fields=media_caption,media_like_count,media_comments_count,media_shares,media_saved,media_reach,media_plays,media_type,media_permalink,media_thumbnail_url&page_size=5000`).then(r=>r.json()).then(d=>d.data||[]).catch(()=>[]),
          fetch(`${baseUrl}/tiktok_organic?api_key=${apiKey}&${dateParam}&fields=date,total_followers_count,followers_count,likes,comments,shares,profile_views,video_views&page_size=5000`).then(r=>r.json()).then(d=>d.data||[]).catch(()=>[]),
          fetch(`${baseUrl}/tiktok_organic?api_key=${apiKey}&${dateParam}&fields=video_caption,video_likes,video_comments,video_shares,video_views_count,video_share_url,video_thumbnail_url,video_create_datetime&page_size=5000`).then(r=>r.json()).then(d=>d.data||[]).catch(()=>[]),
        ]);

        // Aggregate LinkedIn daily
        let liImps=0,liClicks=0,liLikes=0,liComments=0,liShares=0,liFollowers=0;
        for (const r of liDaily) {
          liImps+=parseFloat(r.account_analytics_impression_count)||0;
          liClicks+=parseFloat(r.account_analytics_click_count)||0;
          liLikes+=parseFloat(r.account_analytics_like_count)||0;
          liComments+=parseFloat(r.account_analytics_comment_count)||0;
          liShares+=parseFloat(r.account_analytics_share_count)||0;
          const fc=parseInt(r.organization_follower_count);if(fc>liFollowers)liFollowers=fc;
        }

        // Aggregate Instagram daily
        let igReach=0,igLikes=0,igComments=0,igShares=0,igSaves=0,igFollowers=0;
        for (const r of igDaily) {
          igReach+=parseFloat(r.reach)||0;
          igLikes+=parseFloat(r.likes)||0;
          igComments+=parseFloat(r.comments)||0;
          igShares+=parseFloat(r.shares)||0;
          igSaves+=parseFloat(r.saves)||0;
          const fc=parseInt(r.followers_count);if(fc>igFollowers)igFollowers=fc;
        }

        // Aggregate TikTok daily
        let ttViews=0,ttLikes=0,ttComments=0,ttShares=0,ttFollowers=0,ttNewFollowers=0;
        for (const r of ttDaily) {
          ttViews+=parseFloat(r.video_views)||0;
          ttLikes+=parseFloat(r.likes)||0;
          ttComments+=parseFloat(r.comments)||0;
          ttShares+=parseFloat(r.shares)||0;
          const tfc=parseInt(r.total_followers_count);if(tfc>ttFollowers)ttFollowers=tfc;
          ttNewFollowers+=parseInt(r.followers_count)||0;
        }

        // Top posts by engagement
        const liTopPosts = liPosts.filter(p=>p.share_text).sort((a,b)=>((b.share_like_count||0)+(b.share_comment_count||0)+(b.share_clicks_count||0))-((a.share_like_count||0)+(a.share_comment_count||0)+(a.share_clicks_count||0))).slice(0,5).map(p=>({
          text:(p.share_text||'').substring(0,120),impressions:p.share_impression_count||0,clicks:p.share_clicks_count||0,likes:p.share_like_count||0,comments:p.share_comment_count||0,date:p.share_published_time,url:p.share_url
        }));
        const igTopPosts = igPosts.filter(p=>p.media_caption).sort((a,b)=>((b.media_like_count||0)+(b.media_comments_count||0)+(b.media_shares||0))-((a.media_like_count||0)+(a.media_comments_count||0)+(a.media_shares||0))).slice(0,5).map(p=>({
          text:(p.media_caption||'').substring(0,120),likes:p.media_like_count||0,comments:p.media_comments_count||0,shares:p.media_shares||0,saves:p.media_saved||0,reach:p.media_reach||0,type:p.media_type,url:p.media_permalink,thumb:p.media_thumbnail_url
        }));
        const ttTopPosts = ttPosts.filter(p=>p.video_caption).sort((a,b)=>((b.video_likes||0)+(b.video_comments||0)+(b.video_shares||0))-((a.video_likes||0)+(a.video_comments||0)+(a.video_shares||0))).slice(0,5).map(p=>({
          text:(p.video_caption||'').substring(0,120),likes:p.video_likes||0,comments:p.video_comments||0,shares:p.video_shares||0,views:p.video_views_count||0,date:p.video_create_datetime,url:p.video_share_url,thumb:p.video_thumbnail_url
        }));

        return jr({
          period,
          linkedin:{followers:liFollowers,impressions:liImps,clicks:liClicks,likes:liLikes,comments:liComments,shares:liShares,ctr:liImps>0?(liClicks/liImps*100):0,days:liDaily.length,topPosts:liTopPosts,daily:liDaily.map(r=>({date:r.date,impressions:parseFloat(r.account_analytics_impression_count)||0,clicks:parseFloat(r.account_analytics_click_count)||0,likes:parseFloat(r.account_analytics_like_count)||0}))},
          instagram:{followers:igFollowers,reach:igReach,likes:igLikes,comments:igComments,shares:igShares,saves:igSaves,days:igDaily.length,topPosts:igTopPosts,daily:igDaily.map(r=>({date:r.date,reach:parseFloat(r.reach)||0,likes:parseFloat(r.likes)||0}))},
          tiktok:{followers:ttFollowers,views:ttViews,likes:ttLikes,comments:ttComments,shares:ttShares,days:ttDaily.length,topPosts:ttTopPosts,daily:ttDaily.map(r=>({date:r.date,views:parseFloat(r.video_views)||0,likes:parseFloat(r.likes)||0}))},
        });
      } catch(err) { return jr({ error: 'Social data error', detail: err.message }, 500); }
    }

    // POST /api/sales → Sales Dashboard API
    if (request.method === 'POST' && url.pathname === '/api/sales') {
      let body;
      try { body = await request.json(); } catch { return jr({ error: 'Invalid JSON' }, 400); }
      if (body.password !== env.TEAM_PASSWORD) return jr({ error: 'Unauthorized' }, 401);
      try {
        const result = await fetchSalesData(body.mode || 'preDemoData', env);
        return jr(result);
      } catch(err) { console.error('Sales API error:', err); return jr({ error: 'Internal error', detail: err.message }, 500); }
    }

    // POST /api/bd → BD Dashboard API
    if (request.method === 'POST' && url.pathname === '/api/bd') {
      let body;
      try { body = await request.json(); } catch { return jr({ error: 'Invalid JSON' }, 400); }
      if (body.password !== env.TEAM_PASSWORD) return jr({ error: 'Unauthorized' }, 401);
      try {
        const result = await fetchBDData(env);
        return jr(result);
      } catch(err) { console.error('BD API error:', err); return jr({ error: 'Internal error', detail: err.message }, 500); }
    }

    // POST /api/bd/lookup-companies → fetch company names for a list of IDs.
    // Dedicated endpoint so it gets its own fresh subrequest budget,
    // separate from /api/bd which spends most of its budget on deal
    // pagination + association batches. Results are written to KV
    // (key 'bd_company_cache_v5') so /api/bd picks them up on next load.
    if (request.method === 'POST' && url.pathname === '/api/bd/lookup-companies') {
      let body;
      try { body = await request.json(); } catch { return jr({ error: 'Invalid JSON' }, 400); }
      if (body.password !== env.TEAM_PASSWORD) return jr({ error: 'Unauthorized' }, 401);
      try {
        const result = await lookupBDCompanies(env, body.companyIds || []);
        return jr({ ok: true, ...result });
      } catch(err) { console.error('BD lookup error:', err); return jr({ ok: false, error: 'Internal error', detail: err.message }, 500); }
    }

    return jr({ error: 'Not found' }, 404);
  },
};

// __DASHBOARD_HTML_PLACEHOLDER__ — build.js replaces this line
const _DASHBOARD_B64 = '';
const DASHBOARD_HTML = (() => { try { const b = atob(_DASHBOARD_B64); const bytes = new Uint8Array(b.length); for(let i=0;i<b.length;i++) bytes[i]=b.charCodeAt(i); return new TextDecoder().decode(bytes); } catch(e) { return '<html><body>Dashboard failed to load: '+e.message+'</body></html>'; } })();

const _SALES_B64 = '';
const SALES_HTML = (() => { try { const b = atob(_SALES_B64); const bytes = new Uint8Array(b.length); for(let i=0;i<b.length;i++) bytes[i]=b.charCodeAt(i); return new TextDecoder().decode(bytes); } catch(e) { return '<html><body>Sales Dashboard failed to load: '+e.message+'</body></html>'; } })();

const _BD_B64 = '';
const BD_HTML = (() => { try { const b = atob(_BD_B64); const bytes = new Uint8Array(b.length); for(let i=0;i<b.length;i++) bytes[i]=b.charCodeAt(i); return new TextDecoder().decode(bytes); } catch(e) { return '<html><body>BD Dashboard failed to load: '+e.message+'</body></html>'; } })();

function jr(data, status=200) {
  return new Response(JSON.stringify(data), { status, headers: { 'Content-Type':'application/json','Access-Control-Allow-Origin':'*' } });
}
