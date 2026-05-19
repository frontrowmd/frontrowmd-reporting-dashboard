// ============================================================================
// FrontrowMD Dashboard API — Cloudflare Worker (v3)
// ============================================================================
// Matches FrontrowMD-Metrics-Reference-Guide.md as source of truth.
// POST /api/data — returns structured JSON for all dashboard sections.
// ============================================================================

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const DASH_CHANNELS = ['meta', 'google', 'linkedin', 'tiktok', 'youtube'];

const CHANNEL_LABELS = { meta: 'Meta', google: 'Google', linkedin: 'LinkedIn', tiktok: 'TikTok', youtube: 'YouTube' };

const BUDGET_BY_MONTH = {
  '2026-01': { meta: 45000, linkedin: 30000, google: 5000, tiktok: 5000, youtube: 5000 },
  '2026-02': { meta: 70000, linkedin: 30000, google: 5000, tiktok: 10000, youtube: 5000 },
  '2026-03': { meta: 90000, linkedin: 15000, google: 10000, tiktok: 30000, youtube: 0 },
  '2026-04': { meta: 116667, linkedin: 34000, google: 16000, tiktok: 26667, youtube: 0 },
  '2026-05': { meta: 99900, linkedin: 30900, google: 15500, tiktok: 24500, youtube: 0 },
};
const BUDGET_FALLBACK = BUDGET_BY_MONTH['2026-05'];
function getBudgetsForMonth(dateStr) {
  if (!dateStr) return BUDGET_FALLBACK;
  return BUDGET_BY_MONTH[dateStr.slice(0, 7)] || BUDGET_FALLBACK;
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
  const fields = 'date,datasource,campaign_name,spend,clicks,impressions,ctr,conversions,externalwebsiteconversions,conversions_submit_application_total,all_conversions';
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

// Look up the "Disqualification Form" by name (case-insensitive substring match)
// and count submissions in [from, to]. Used by Irfan KPI #5 (% Unqualified Brand Fit).
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
    return { count: 0, formFound: false };
  }
  // Step 2: walk submissions filtered by submittedAt in [from, to]
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
  return { count, totalSeen, pages, formFound: true, formId: form.id, formName: form.name };
}

async function fetchScheduledContacts(token, from, to) {
  return hsSearch(token, 'contacts', [{
    filters: [
      { propertyName: 'createdate', operator: 'GTE', value: String(toMsET(from)) },
      { propertyName: 'createdate', operator: 'LTE', value: String(toMsET(to, true)) },
      { propertyName: 'date_demo_booked', operator: 'HAS_PROPERTY' },
    ],
  }], ['createdate', 'date_demo_booked', 'email', 'website', 'company', 'average_monthly_web_traffic']);
}

// Contacts for Demo Quality — matched to deals by company name
async function fetchContactsForDQ(token, from, to) {
  return hsSearch(token, 'contacts', [{
    filters: [
      { propertyName: 'date_demo_booked', operator: 'GTE', value: String(toMsUTC(from)) },
      { propertyName: 'date_demo_booked', operator: 'LTE', value: String(toMsUTC(to, true)) },
    ],
  }], ['date_demo_booked', 'firstname', 'lastname', 'email', 'website', 'company', 'role_at_company',
       'hs_sales_email_last_opened', 'hs_sales_email_last_clicked', 'hs_sales_email_last_replied',
       'notes_last_contacted', 'hs_sequences_is_enrolled', 'hs_latest_sequence_enrolled',
       'hs_latest_sequence_enrolled_date',
       'average_monthly_web_traffic', 'sl_last_demo_name', 'sl_last_demo_completion_percent']);
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
        const candidate = await fetchPipelineDeals(token, cm.from, cm.to, { maxPages: 20 });
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

// Closed-won deals by closedate (guide Section 5)
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
      demos = parseInt(row.conversions_submit_application_total)||0;
    } else if (key === 'linkedin') {
      demos = parseInt(row.externalwebsiteconversions)||0; // overridden below
    } else if (key === 'tiktok') {
      demos = parseInt(row.conversions)||0;
    } else {
      demos = Math.ceil(parseFloat(row.conversions)||0);
    }

    ch[key].spend += spend; ch[key].clicks += clicks; ch[key].impressions += impr; ch[key].windsorDemos += demos;
    tSpend += spend; tClicks += clicks; tImpr += impr; tDemos += demos;
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

  return { total: { spend:tSpend, clicks:tClicks, impressions:tImpr, windsorDemos:tDemos }, channels: ch };
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
  const byDayScale = {};    // per-day count excluding pre-launch (Irfan Dashboard daily chart)
  const byWebTraffic = {};  // count by raw web-traffic tier value (dashboard maps to display labels)
  let lowTrafficCount = 0;
  // Set of normalized company names + email domains flagged as Pre-launch.
  // Used downstream by processPipelineDeals to compute the scale-tier QDG count.
  const lowTrafficCompanies = new Set();
  for (const c of contacts) {
    const cd = c.properties?.createdate;
    if (!cd) continue;
    const d = new Date(cd);
    const o = etOff(d);
    const et = new Date(d.getTime() + o * 3600000);
    const ds = fmt(et);
    byDay[ds] = (byDay[ds]||0) + 1;
    const wtRaw = c.properties?.average_monthly_web_traffic || '';
    const wt = wtRaw.toLowerCase();
    const isLow = wt.includes('pre-launch');
    const tierKey = wtRaw || '(none)';
    byWebTraffic[tierKey] = (byWebTraffic[tierKey]||0) + 1;
    if (!isLow) {
      byDayScale[ds] = (byDayScale[ds]||0) + 1;
    } else {
      lowTrafficCount++;
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
  }
  return { total: contacts.length, byDay, byDayScale, byWebTraffic, lowTrafficCount, lowTrafficCompanies };
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
  let qualifiedRawCount=0, disqualifiedRawCount=0, notYetEvalCount=0;
  let qualifiedRawScaleCount=0;  // Qualified count excluding Pre-launch brands (denominator for Total True CPQD)
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
    }
    // Scale-tier No Show: No Show excluding pre-launch brands (used to compute Demos Held excl pre-launch)
    if (att === 'No Show') {
      if (!isLow) noShowScaleCount++;
    }
    // Stale scheduled: Scheduled — pending with date_demo_booked in the past (data hygiene)
    if (att === 'Scheduled — pending') {
      const ddbMs = dateMs(p.date_demo_booked);
      if (!isNaN(ddbMs) && ddbMs < todayMs) staleScheduledCount++;
    }

    // Prune Rate numerator: demos cancelled before they happened
    if (att === 'Cancelled before demo') cancelledBeforeDemoCount++;

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

    // Daily chart
    const ds = p.date_demo_booked ? p.date_demo_booked.substring(0,10) : null;
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

  // Qualification Rate = Qualified / (Qualified + Disqualified) — new field model
  // qualRateDenom now uses Demos Held (orig + resched + no-show) per dashboard spec.
  // Old denom (qualified + disqualified) preserved as legacy for any code still expecting it.
  const qualRateDenomLegacy = qualifiedRawCount + disqualifiedRawCount;
  const disqualificationRate = qualRateDenomLegacy > 0 ? (disqualifiedRawCount / qualRateDenomLegacy) * 100 : 0;
  // qualRateDenom = Demos Held = orig + resched (no-show excluded — they didn't actually happen)
  const _heldForQual = demoGivenOrigCount + demoGivenReschedCount;
  const qualRateDenom = _heldForQual;
  const qualificationRate = qualRateDenom > 0 ? (qualifiedRawCount / qualRateDenom) * 100 : 0;

  // No Show Rate = No Show / (Demo Given Orig + Demo Given Resched + No Show) — new field model
  const noShowDenom = demoGivenOrigCount + demoGivenReschedCount + noShowCount;
  const noShowRate = noShowDenom > 0 ? (noShowCount / noShowDenom) * 100 : 0;
  // Scale-tier No Show Rate — same formula but restricted to non-pre-launch demos.
  // Counters tracked at lines 638/642 already exclude pre-launch via the isLow check.
  const noShowDenomScale = demoGivenScaleCount + noShowScaleCount;
  const noShowRateScale = noShowDenomScale > 0 ? (noShowScaleCount / noShowDenomScale) * 100 : 0;

  // Prune Rate = Cancelled before demo / (Demo Given Orig + Demo Given Resched + No Show + Cancelled before demo)
  // Denominator: demos with any settled outcome (excludes still-pending so pending demos don't deflate the rate).
  const pruneDenom = demoGivenOrigCount + demoGivenReschedCount + noShowCount + cancelledBeforeDemoCount;
  const pruneRate = pruneDenom > 0 ? (cancelledBeforeDemoCount / pruneDenom) * 100 : 0;
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
    totalExtended, demoGivenOrigCount, demoGivenReschedCount, demoGivenScaleCount, noShowScaleCount,
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
    signedMrrSum: 0,
    // Avg Days to Close — two variants per spec: from date_demo_booked + from hs_createdate.
    daysToCloseSum: 0, daysToCloseN: 0,           // from date_demo_booked (back-compat name)
    daysFromCreatedSum: 0, daysFromCreatedN: 0,   // from hs_createdate
  });
  const buckets = {};
  for (const cm of cohortMonths) {
    // signedByWebTraffic — bucket Closed Won deals by web-traffic tier for the
    // pie chart on each cohort. Cohort-level only (not tracked per rep).
    buckets[cm.label] = { period: cm, ...emptyBucket(), byRep: {}, signedByWebTraffic: {} };
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
    const amt = parseFloat(p.amount) || 0;
    const oid = p.hubspot_owner_id || 'unassigned';

    if (!b.byRep[oid]) {
      b.byRep[oid] = emptyBucket(ownerMap[oid] || (oid === 'unassigned' ? 'Unassigned' : oid));
    }

    b.allBooked++;
    b.byRep[oid].allBooked++;

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
    return {
      allBooked: b.allBooked,
      signed: b.cntWon,
      demosHeld,
      pctSigned:  demosHeld    > 0 ? (b.cntWon     / demosHeld)    * 100 : 0,
      pctPending: demosHeld    > 0 ? (cntPending   / demosHeld)    * 100 : 0,
      pctPruned:  b.allBooked  > 0 ? (b.cntNotAFit / b.allBooked)  * 100 : 0,
      pctNoShow:  b.allBooked  > 0 ? (b.cntNoShow  / b.allBooked)  * 100 : 0,
      stageCounts: {
        won: b.cntWon, appt: b.cntAppt, demoHappened: b.cntDemoHappened,
        dm: b.cntDM, cs: b.cntCS, noShow: b.cntNoShow, notAFit: b.cntNotAFit,
      },
      mrr: b.signedMrrSum,
      newArr: b.signedMrrSum * 12,
      acv: b.cntWon > 0 ? b.signedMrrSum / b.cntWon : 0,
      // From date_demo_booked
      avgDaysToClose: b.daysToCloseN > 0 ? b.daysToCloseSum / b.daysToCloseN : null,
      avgDaysToCloseN: b.daysToCloseN,
      avgDaysFromBooked: b.daysToCloseN > 0 ? b.daysToCloseSum / b.daysToCloseN : null,
      avgDaysFromBookedN: b.daysToCloseN,
      // From hs_createdate
      avgDaysFromCreated: b.daysFromCreatedN > 0 ? b.daysFromCreatedSum / b.daysFromCreatedN : null,
      avgDaysFromCreatedN: b.daysFromCreatedN,
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

    cohorts.push({ period: cm, ...m, signedByWebTraffic: b.signedByWebTraffic||{}, byRep: repData });
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
  // True CPQD denominator: Demos Held excluding Pre-launch brands
  const demosHeldCount = (c.pipeline.demoGivenOrigCount||0) + (c.pipeline.demoGivenReschedCount||0);
  const demosHeldScaleCount = (c.pipeline.demoGivenScaleCount||0);
  const totalQualified = demosHeldCount;       // CPQD denominator → Demos Held
  const totalQualifiedScale = demosHeldScaleCount; // True CPQD denominator → Demos Held excl. pre-launch
  const cpdTotal = totalScheduled > 0 ? totalSpend/totalScheduled : null;
  const cpqdTotal = totalQualified > 0 ? totalSpend/totalQualified : null;
  const trueCpqdTotal = totalQualifiedScale > 0 ? totalSpend/totalQualifiedScale : null;

  const pTotalS = p.adSpend?.total?.spend||0;
  const pTotalWD = p.adSpend?.total?.windsorDemos||0;
  const pTotalSch = prior ? (p.scheduled?.total||0) : 0;
  const pTotalQ = prior ? (p.pipeline?.qualifiedCount||0) : 0;
  // CPQD/True CPQD prior period denominators use Demos Held counts (orig + resched, no-show excluded)
  const pTotalQual = prior ? ((p.pipeline?.demoGivenOrigCount||0) + (p.pipeline?.demoGivenReschedCount||0)) : 0;
  const pTotalQualScale = prior ? (p.pipeline?.demoGivenScaleCount||0) : 0;
  const pmTotalS = pm.adSpend?.total?.spend||0;
  const pmTotalWD = pm.adSpend?.total?.windsorDemos||0;
  const pmTotalSch = priorMonth ? (pm.scheduled?.total||0) : 0;
  const pmTotalQ = priorMonth ? (pm.pipeline?.qualifiedCount||0) : 0;
  const pmTotalQual = priorMonth ? ((pm.pipeline?.demoGivenOrigCount||0) + (pm.pipeline?.demoGivenReschedCount||0)) : 0;
  const pmTotalQualScale = priorMonth ? (pm.pipeline?.demoGivenScaleCount||0) : 0;

  const executiveSummary = {
    totalDemosScheduled: buildTile(c.scheduled.total, p.scheduled?.total??null, pm.scheduled?.total??null, 'Contacts created in period with date_demo_booked set'),
    totalCpd: buildTile(cpdTotal, prior&&pTotalSch>0?pTotalS/pTotalSch:null, priorMonth&&pmTotalSch>0?pmTotalS/pmTotalSch:null, 'Total Ad Spend ÷ New Demos Scheduled'),
    qualificationRate: buildTile(c.pipeline.qualificationRate, p.pipeline?.qualificationRate??null, pm.pipeline?.qualificationRate??null, 'Qualified ÷ (Qualified + Disqualified)'),
    disqualificationRate: buildTile(c.pipeline.disqualificationRate, p.pipeline?.disqualificationRate??null, pm.pipeline?.disqualificationRate??null, 'Disqualified ÷ (Qualified + Disqualified)'),
    totalCpqd: buildTile(cpqdTotal, prior&&pTotalQual>0?pTotalS/pTotalQual:null, priorMonth&&pmTotalQual>0?pmTotalS/pmTotalQual:null, 'Total Ad Spend ÷ Demos Held (Demo Given orig + resched)'),
    totalTrueCpqd: buildTile(trueCpqdTotal, prior&&pTotalQualScale>0?pTotalS/pTotalQualScale:null, priorMonth&&pmTotalQualScale>0?pmTotalS/pmTotalQualScale:null, 'Total Ad Spend ÷ Demos Held excluding Pre-launch'),
    totalQualifiedDemos: buildTile(c.pipeline.qualifiedRawCount||0, p.pipeline?.qualifiedRawCount??null, pm.pipeline?.qualifiedRawCount??null, 'Deals with demo_qualification_outcome = Qualified'),
    pruneRate: buildTile(c.pipeline.pruneRate||0, p.pipeline?.pruneRate??null, pm.pipeline?.pruneRate??null, 'Cancelled before demo ÷ (Demo Given orig + Demo Given resched + No Show + Cancelled before demo)'),
    _meta: { totalSpend, totalWD, totalScheduled, totalQual, totalQualified, totalQualifiedScale, demosHeldCount, demosHeldScaleCount, closedWonCount: c.closedWon.count, closedWonMRR: c.closedWon.mrr, pClosedWonCount: p.closedWon?.count??null, pClosedWonMRR: p.closedWon?.mrr??null, pmClosedWonCount: pm.closedWon?.count??null, pmClosedWonMRR: pm.closedWon?.mrr??null, pTotalSpend: pTotalS, pmTotalSpend: pmTotalS, pDemosHeldCount: prior?((p.pipeline?.demoGivenOrigCount||0)+(p.pipeline?.demoGivenReschedCount||0)):null, pmDemosHeldCount: priorMonth?((pm.pipeline?.demoGivenOrigCount||0)+(pm.pipeline?.demoGivenReschedCount||0)):null, pDemosHeldScaleCount: prior?(p.pipeline?.demoGivenScaleCount||0):null, pmDemosHeldScaleCount: priorMonth?(pm.pipeline?.demoGivenScaleCount||0):null },
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
  };
  const qualifiedDemos = totalScheduled - lowTraffic;
  const trueCpd = qualifiedDemos > 0 ? totalSpend / qualifiedDemos : null;
  const pQualDemos = pLowTraffic != null ? (p.scheduled?.total||0) - pLowTraffic : null;
  const pTrueCpd = pQualDemos != null && pQualDemos > 0 && pTotalS > 0 ? pTotalS / pQualDemos : null;
  const pmQualDemos = pmLowTraffic != null ? (pm.scheduled?.total||0) - pmLowTraffic : null;
  const pmTrueCpd = pmQualDemos != null && pmQualDemos > 0 && pmTotalS > 0 ? pmTotalS / pmQualDemos : null;
  executiveSummary.trueCpd = buildTile(trueCpd, pTrueCpd, pmTrueCpd, 'Total Ad Spend ÷ Demos (excluding Pre-launch)');

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
    noShowRate: buildTile(c.pipeline.noShowRate, p.pipeline?.noShowRate??null, pm.pipeline?.noShowRate??null, 'No Show ÷ (Demo Given orig + Demo Given resched + No Show)'),
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
    scheduledByWebTraffic: c.scheduled.byWebTraffic,
    scheduledByWebTrafficPrior: prior ? p.scheduled?.byWebTraffic || null : null,
    scheduledByWebTrafficLastMonth: priorMonth ? pm.scheduled?.byWebTraffic || null : null,
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
  // Strip the Set from scheduled — it doesn't serialize and isn't needed downstream
  const scheduledOut = { total: scheduled.total, byDay: scheduled.byDay, byDayScale: scheduled.byDayScale, byWebTraffic: scheduled.byWebTraffic, lowTrafficCount: scheduled.lowTrafficCount };
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

  // Irfan KPI #5 — Disqualification Form submissions for the current window.
  // Done here (after Windsor, before HubSpot's heavy Phase 2 fetches) so it
  // gets subrequest budget. Result stored in a closed-over variable.
  let _irfanDqForm = null, _irfanDqFormErr = null;
  try {
    _irfanDqForm = await fetchDisqualificationFormSubmissions(hsToken, current.from, current.to);
    console.log(`Irfan DQ form: ${_irfanDqForm.count} submissions in ${current.from}..${current.to} (form: ${_irfanDqForm.formName||'not found'})`);
  } catch(e) {
    _irfanDqFormErr = e.message;
    console.error('Irfan DQ form fetch failed:', e);
  }

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
  const cCW = await fetchClosedWonDeals(hsToken, current.from, current.to);

  // Fetch priorMonth FIRST so we can derive prior from it in-memory when prior
  // is a strict subset (saves 3 HubSpot calls = up to ~15 subrequests).
  let pmSch, pmPipe, pmCW;
  if (priorMonth) {
    pmSch = await fetchScheduledContacts(hsToken, priorMonth.from, priorMonth.to);
    pmPipe = await fetchPipelineDeals(hsToken, priorMonth.from, priorMonth.to);
    pmCW = await fetchClosedWonDeals(hsToken, priorMonth.from, priorMonth.to);
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
    pSch = pmSch.filter(c => {
      const ms = dateMs(c.properties?.date_demo_booked);
      return !isNaN(ms) && ms >= _pFromMsUTC && ms <= _pToMsUTC;
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
    pSch = await fetchScheduledContacts(hsToken, prior.from, prior.to);
    pPipe = await fetchPipelineDeals(hsToken, prior.from, prior.to);
    pCW = await fetchClosedWonDeals(hsToken, prior.from, prior.to);
  }

  const ownerMap = await fetchOwners(hsToken);

  // Sign-Up Rate cohort fetch — sequential per-month. A single 4-month
  // wide query hits HubSpot's 10k hard ceiling on busy pipelines and
  // silently drops one end of the window. Per-month queries each get
  // their own 10k budget. Sequential avoids the parallel-fetch
  // intermittent-failure issue we saw with the first attempt.
  // Done HERE (not earlier in the handler) so we don't risk crowding
  // out the rest of the work on the per-request CF budget — the
  // 4 sequential fetches add ~1-3s wall-clock but each one is small.
  const _cohortRes = await fetchCohortDealsPerMonth(hsToken, cohortMonths);
  const cohortDeals = _cohortRes.union;
  const cohortFetchedPerMonth = _cohortRes.perMonth;
  const cohortFetchStatus = _cohortRes.perMonthStatus || {};

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
    const [allQualKpi, _cw] = await Promise.all([
      fetchAllQualifiedDeals(hsToken),
      fetchAllClosedWon(hsToken),
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

  // ── Sign-Up Rate cohorts ──
  // Union cohortDeals (per-month early fetch) with the narrower cPipe/pPipe/
  // pmPipe sources. Each narrow source is a separate fetch over its own short
  // window and is nowhere near a pagination cap, so it acts as defense-in-
  // depth for the months it covers — same trick the Irfan PCM uses.
  const _signupUnion = new Map();
  const _signupAdd = (arr) => { if (!arr) return; for (const x of arr) _signupUnion.set(x.id, x); };
  _signupAdd(cohortDeals); _signupAdd(cPipe); _signupAdd(pPipe); _signupAdd(pmPipe);
  console.log(`SignUp union: cohortDeals=${cohortDeals.length}, cPipe=${cPipe?.length||0}, pPipe=${pPipe?.length||0}, pmPipe=${pmPipe?.length||0} → union=${_signupUnion.size} unique`);
  currentData.signUpRate = buildSignUpCohorts([..._signupUnion.values()], cohortMonths, ownerMap);
  // Attach per-cohort fetched count for the dashboard diagnostic strip.
  // This tells us exactly what HubSpot returned for each month before any
  // bucketing — if a number looks off, this is where we can start.
  if (currentData.signUpRate?.cohorts) {
    for (const c of currentData.signUpRate.cohorts) {
      c._fetchedCount = cohortFetchedPerMonth[c.period.label] ?? null;
      c._fetchStatus = cohortFetchStatus[c.period.label] ?? null;
    }
  }
  currentData.signUpRate.allTimeByRep = buildAllTimeRepStats(allTimeClosedWon, allTimeQualifiedForReps, ownerMap);
  currentData.quarterlyHistory = isAllTime ? buildQuarterlyHistory(filterActiveBrands(cCW), current.from, current.to) : null;

  // ── Marketing Funnel (monthly historical table) ──
  // Uses KPI-aligned definition: count of deals with demo_attendance_status IN [Demo Given orig, Demo Given resched].
  currentData.marketingFunnel = buildMarketingFunnel(monthlyAdSpend, allTimeQualifiedForFunnel, filterActiveBrands(allTimeClosedWon));

  // ── Demo Quality page: use cPipe directly (already fetched through end of month above) ──
  const dqDealsRaw = filterActiveBrands(cPipe);
  
  const demoQualityDeals = dqDealsRaw.map(d => {
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
      website: '',
    };
  });
  console.log(`demoQualityDeals: ${demoQualityDeals.length}`);

  const resp = buildResponse(currentData, priorData, priorMonthData, isAllTime, ownerMap, windowType);
  resp.demoQualityDeals = demoQualityDeals;
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

  // Tile #1 — Signed Deals (toggle: Last Month / MTD).
  //   "Qualified Demos Held" denominator = deals with date_demo_booked in the
  //     selected month AND dealstage in [closedwon, appointmentscheduled,
  //     1084214349 (Demo Happened), decisionmakerboughtin, contractsent].
  //     For STAGE_APPT, we require date_demo_booked <= today (the current
  //     month would otherwise be inflated by future-dated bookings).
  //   "Closed Won" numerator = denominator subset with dealstage='closedwon'.
  //   Three breakdown ratios over deals booked in the month (excl. pre-launch):
  //     % Pending = (appt-past + Demo Happened + decisionmaker + contractsent) / Qual Demos Held
  //     % Pruned  = Not a Fit / all booked
  //     % No Show = No Show / all booked
  //   PRE-LAUNCH (Pre-launch / just launching) brands are excluded entirely.
  // Union of pmPipe + cPipe + cohortDeals + pPipe avoids any single fetch's
  // pagination cap.
  function _buildSpecial1Cohort(dealsArr, fromStr, toStr) {
    const STAGE_APPT = 'appointmentscheduled';
    const STAGE_DEMO_HAPPENED = '1084214349';
    const STAGE_DM = 'decisionmakerboughtin';
    const STAGE_CS = 'contractsent';
    const STAGE_WON = 'closedwon';
    const STAGE_NO_SHOW = '3453957850';
    const STAGE_NOT_A_FIT = '1062974581';
    const fromD = new Date(fromStr+'T00:00:00Z');
    const toD = new Date(toStr+'T00:00:00Z');
    const fromMs = fromD.getTime();
    const toMs = Date.UTC(toD.getUTCFullYear(), toD.getUTCMonth(), toD.getUTCDate(), 23, 59, 59, 999);
    // Today's UTC midnight EOD — gate for STAGE_APPT (don't count future-dated)
    const _today = new Date();
    const _todayMs = Date.UTC(_today.getUTCFullYear(), _today.getUTCMonth(), _today.getUTCDate(), 23, 59, 59, 999);
    const _floor = (ms) => { const x = new Date(ms); return Date.UTC(x.getUTCFullYear(), x.getUTCMonth(), x.getUTCDate()); };
    // Robust parse for hs_createdate (numeric ms string OR ISO datetime)
    const _parseDt = (v) => { if (!v) return NaN; return /^\d+$/.test(v) ? parseInt(v) : new Date(v).getTime(); };

    let allBooked = 0;
    let cntWon = 0, cntAppt = 0, cntDemoHappened = 0, cntDM = 0, cntCS = 0;
    let cntNoShow = 0, cntNotAFit = 0;
    let signedMrrSum = 0;
    let daysFromBookedSum = 0, daysFromBookedN = 0;
    let daysFromCreatedSum = 0, daysFromCreatedN = 0;
    let prelaunchExcluded = 0;

    for (const d of dealsArr) {
      const p = d.properties || {};
      const ddbMs = dateMs(p.date_demo_booked);
      if (isNaN(ddbMs) || ddbMs < fromMs || ddbMs > toMs) continue;
      // Exclude pre-launch brands. Deal-level field is
      // average_monthly_web_traffic__cloned_ (snapshot at signing time);
      // fall back to the live field for deals without the cloned snapshot.
      const wt = (p.average_monthly_web_traffic__cloned_ || p.average_monthly_web_traffic || '').toLowerCase();
      if (wt.indexOf('pre-launch') >= 0) { prelaunchExcluded++; continue; }
      allBooked++;
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
    return {
      // heldScale kept as field name for backward compat — it's "Qualified Demos Held"
      heldScale: demosHeld, signed: cntWon,
      pctSigned: demosHeld > 0 ? (cntWon / demosHeld) * 100 : 0,
      allBooked,
      pctPending: demosHeld > 0 ? (cntPending / demosHeld) * 100 : 0,
      pctPruned:  allBooked > 0 ? (cntNotAFit / allBooked) * 100 : 0,
      pctNoShow:  allBooked > 0 ? (cntNoShow / allBooked) * 100 : 0,
      stageCounts: { won: cntWon, appt: cntAppt, demoHappened: cntDemoHappened, dm: cntDM, cs: cntCS, noShow: cntNoShow, notAFit: cntNotAFit },
      newArr: signedMrrSum * 12,
      acv: cntWon > 0 ? signedMrrSum / cntWon : 0,
      // Two Avg Days variants per Irfan request
      avgDaysFromBooked: daysFromBookedN > 0 ? daysFromBookedSum / daysFromBookedN : null,
      avgDaysFromBookedN: daysFromBookedN,
      avgDaysFromCreated: daysFromCreatedN > 0 ? daysFromCreatedSum / daysFromCreatedN : null,
      avgDaysFromCreatedN: daysFromCreatedN,
      // back-compat alias (older clients reading avgDaysToClose)
      avgDaysToClose: daysFromBookedN > 0 ? daysFromBookedSum / daysFromBookedN : null,
      avgDaysToCloseN: daysFromBookedN,
      fromDate: fromStr, toDate: toStr,
      prelaunchExcluded,
    };
  }

  try {
    const _now = new Date();
    // Last Month window (prior calendar month — full)
    const _pcmFrom = new Date(Date.UTC(_now.getUTCFullYear(), _now.getUTCMonth()-1, 1));
    const _pcmTo = new Date(Date.UTC(_now.getUTCFullYear(), _now.getUTCMonth(), 0));
    const _pcmFromStr = fmt(_pcmFrom), _pcmToStr = fmt(_pcmTo);
    // MTD window (current calendar month so far)
    const _mtdFrom = new Date(Date.UTC(_now.getUTCFullYear(), _now.getUTCMonth(), 1));
    const _mtdFromStr = fmt(_mtdFrom), _mtdToStr = fmt(_now);
    // Build the deal union once, share across both cohorts
    const _pcmUnion = new Map();
    const _addAll = (arr) => { if (!arr) return; for (const x of arr) _pcmUnion.set(x.id, x); };
    _addAll(pmPipe); _addAll(cPipe); _addAll(cohortDeals); _addAll(pPipe);
    const _srcCounts = { pmPipe: pmPipe?.length||0, cPipe: cPipe?.length||0, cohortDeals: cohortDeals?.length||0, pPipe: pPipe?.length||0, union: _pcmUnion.size };
    const _dealsArr = [..._pcmUnion.values()];
    const pcmCohort = _buildSpecial1Cohort(_dealsArr, _pcmFromStr, _pcmToStr);
    const mtdCohort = _buildSpecial1Cohort(_dealsArr, _mtdFromStr, _mtdToStr);
    pcmCohort.sourceCounts = _srcCounts;
    mtdCohort.sourceCounts = _srcCounts;
    resp.irfan.priorMonthHeldSigned = pcmCohort;
    resp.irfan.mtdHeldSigned = mtdCohort;
    console.log(`Irfan Special#1 LastMonth: all-booked=${pcmCohort.allBooked} demos-held=${pcmCohort.heldScale} won=${pcmCohort.signed} prelaunchExcluded=${pcmCohort.prelaunchExcluded}`);
    console.log(`Irfan Special#1 MTD: all-booked=${mtdCohort.allBooked} demos-held=${mtdCohort.heldScale} won=${mtdCohort.signed} prelaunchExcluded=${mtdCohort.prelaunchExcluded}`);
  } catch(e) {
    console.error('Irfan Special#1 processing failed:', e);
    resp.irfan.priorMonthError = e.message;
  }

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

async function azWindsorFetch(apiKey, connector, from, to, fields) {
  const url = `https://connectors.windsor.ai/${connector}?api_key=${apiKey}&date_from=${from}&date_to=${to}&fields=${fields}&page_size=5000`;
  for (let i = 0; i < 3; i++) {
    try {
      const r = await fetch(url);
      if (!r.ok) throw new Error(`Windsor ${connector} ${r.status}`);
      const j = await r.json(); return j.data || [];
    } catch(e) { if (i < 2) await sleep(1000*(i+1)); else { console.error(`Az Windsor ${connector}:`, e.message); return []; } }
  }
  return [];
}

// Normalize campaign status across connectors
// Meta: ACTIVE, PAUSED | Google: ENABLED | LinkedIn: ACTIVE, PAUSED, COMPLETED | TikTok: CAMPAIGN_STATUS_ENABLE, CAMPAIGN_STATUS_DISABLE
function normStatus(raw) {
  const s = (raw||'').toUpperCase().trim();
  if (['ACTIVE','ENABLED','CAMPAIGN_STATUS_ENABLE'].includes(s)) return 'ACTIVE';
  if (['PAUSED','CAMPAIGN_PAUSED','CAMPAIGN_STATUS_DISABLE'].includes(s)) return 'PAUSED';
  if (s === 'COMPLETED') return 'COMPLETED';
  if (s === 'REMOVED' || s === 'DELETED' || s === 'ARCHIVED') return 'REMOVED';
  return s || null;
}

async function fetchAzCampaigns(apiKey, from, to) {
  const base = 'date,campaign_name,spend,clicks,impressions';
  // LinkedIn per-connector endpoint uses 'campaign' not 'campaign_name'
  const [fbRows, gaRows, liCampRows, liDemoTotal, ttRows] = await Promise.all([
    azWindsorFetch(apiKey, 'facebook', from, to, base+',conversions_submit_application_total,frequency,effective_status'),
    azWindsorFetch(apiKey, 'google_ads', from, to, base+',conversions,campaign_status'),
    azWindsorFetch(apiKey, 'linkedin', from, to, 'date,campaign,spend,clicks,impressions,campaign_status'),
    fetchLinkedInDemos(apiKey, from, to),
    azWindsorFetch(apiKey, 'tiktok', from, to, base+',conversions,frequency,campaign_status'),
  ]);

  function aggRows(rows, ch, cfg, filterFn, demoFilterFn) {
    const camps = {};
    for (const row of rows) {
      if (filterFn && !filterFn(row)) continue;
      const name = row.campaign_name || '(no name)';
      if (!camps[name]) camps[name] = { name, spend:0, clicks:0, impressions:0, demos:0, freqVals:[], dates:[], status:null };
      camps[name].spend += parseFloat(row.spend)||0;
      camps[name].clicks += parseInt(row.clicks)||0;
      camps[name].impressions += parseInt(row.impressions)||0;
      if (row.date) camps[name].dates.push(row.date);
      // Track status (effective_status for Meta, campaign_status for others) — normalize across connectors
      const rawSt = (row.effective_status || row.campaign_status || '').toUpperCase();
      if (rawSt) camps[name].status = normStatus(rawSt);
      // Only count demos if demoFilterFn passes (or no filter)
      if (!demoFilterFn || demoFilterFn(row)) {
        const rawD = parseFloat(row[cfg.demoField])||0;
        camps[name].demos += (ch==='google'||ch==='youtube') ? Math.ceil(rawD) : Math.round(rawD);
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
      // Compute date range
      if (c.dates.length) {
        c.dates.sort();
        c.firstDate = c.dates[0];
        c.lastDate = c.dates[c.dates.length-1];
        const fd = new Date(c.firstDate+'T12:00:00Z'), ld = new Date(c.lastDate+'T12:00:00Z');
        c.activeDays = Math.round((ld-fd)/86400000)+1;
      } else { c.firstDate=null; c.lastDate=null; c.activeDays=0; }
      delete c.dates;
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
      if (!camps[name]) camps[name] = { name, spend:0, clicks:0, impressions:0, demos:0, freqVals:[], dates:[], status:null };
      camps[name].spend += parseFloat(row.spend)||0;
      camps[name].clicks += parseInt(row.clicks)||0;
      camps[name].impressions += parseInt(row.impressions)||0;
      if (row.date) camps[name].dates.push(row.date);
      const st = row.campaign_status;
      if (st) camps[name].status = normStatus(st);
    }
    let tS=0,tC=0,tI=0;
    for (const c of Object.values(camps)) {
      tS+=c.spend; tC+=c.clicks; tI+=c.impressions;
      c.ctr = c.impressions>0?(c.clicks/c.impressions)*100:0;
      c.frequency = null; delete c.freqVals;
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
      promises.push(azWindsorFetch(apiKey, cfg.connector, from, to, base + extra + freq + video + placement).catch(e => { console.error(`Creative fetch ${ch}:`, e.message); return []; }));
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
      if (!map[name]) map[name] = { name, spend:0, clicks:0, impressions:0, demos:0, freqVals:[], thumbnail: tm[name] || null, campaignName: campName, videoP25:0, _dates:[], _placements:{} };
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
        map[name].demos += (ch==='google'||ch==='youtube') ? Math.ceil(rawD) : Math.round(rawD);
      }
      if (cfg.hasFreq && row.frequency != null && row.frequency !== '') map[name].freqVals.push(parseFloat(row.frequency));
      // Per-campaign creative aggregation
      const campKey = campName.toLowerCase().trim();
      if (!campCreMap[campKey]) campCreMap[campKey] = {};
      if (!campCreMap[campKey][name]) campCreMap[campKey][name] = { name, spend:0, clicks:0, impressions:0, demos:0, freqVals:[], thumbnail: tm[name] || null, videoP25:0, _dates:[], _campName: campName };
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
        campCreMap[campKey][name].demos += (ch==='google'||ch==='youtube') ? Math.ceil(rawD2) : Math.round(rawD2);
      }
      if (cfg.hasFreq && row.frequency != null && row.frequency !== '') campCreMap[campKey][name].freqVals.push(parseFloat(row.frequency));
    }
    for (const c of Object.values(map)) {
      c.ctr = c.impressions > 0 ? (c.clicks/c.impressions)*100 : 0;
      c.cpd = c.demos > 0 ? c.spend/c.demos : null;
      c.frequency = c.freqVals.length ? c.freqVals.reduce((a,b)=>a+b,0)/c.freqVals.length : null;
      delete c.freqVals;
      // Compute activeDays
      if (c._dates && c._dates.length) {
        c._dates.sort(); const fd=new Date(c._dates[0]+'T12:00:00Z'),ld=new Date(c._dates[c._dates.length-1]+'T12:00:00Z');
        c.activeDays=Math.round((ld-fd)/86400000)+1;
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
    }
    // Finalize per-campaign creatives
    const campCreFinal = {};
    for (const [ck, cres] of Object.entries(campCreMap)) {
      campCreFinal[ck] = Object.values(cres).map(c => {
        c.ctr = c.impressions > 0 ? (c.clicks/c.impressions)*100 : 0;
        c.cpd = c.demos > 0 ? c.spend/c.demos : null;
        c.frequency = c.freqVals.length ? c.freqVals.reduce((a,b)=>a+b,0)/c.freqVals.length : null;
        delete c.freqVals;
        if (c._dates && c._dates.length) {
          c._dates.sort(); const fd2=new Date(c._dates[0]+'T12:00:00Z'),ld2=new Date(c._dates[c._dates.length-1]+'T12:00:00Z');
          c.activeDays=Math.round((ld2-fd2)/86400000)+1;
        } else c.activeDays=0;
        delete c._dates;
        c.campaignName = c._campName || ck; delete c._campName;
        return c;
      }).sort((a,b)=>b.spend-a.spend);
    }
    // Finalize overall placement summary
    const placementSummary = Object.values(placementMap).map(p => {
      p.ctr = p.impressions > 0 ? (p.clicks/p.impressions)*100 : 0;
      p.cpd = p.demos > 0 ? p.spend/p.demos : null;
      return p;
    }).sort((a,b)=>b.spend-a.spend);
    results[ch] = { flat: Object.values(map).sort((a,b)=>b.spend-a.spend), byCampaign: campCreFinal, placements: placementSummary };
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
    meta:   { connector:'facebook', fields:'date,adset_name,campaign_name,spend,clicks,impressions,conversions_submit_application_total', nameField:'adset_name', demoField:'conversions_submit_application_total' },
    google: { connector:'google_ads', fields:'date,ad_group_name,campaign_name,spend,clicks,impressions,conversions', nameField:'ad_group_name', demoField:'conversions' },
    tiktok: { connector:'tiktok', fields:'date,ad_group_name,campaign_name,spend,clicks,impressions,conversions', nameField:'ad_group_name', demoField:'conversions' },
  };

  const promises = [], chKeys = [];
  for (const [ch, cfg] of Object.entries(configs)) {
    promises.push(azWindsorFetch(apiKey, cfg.connector, from, to, cfg.fields).catch(e => { console.error(`Audience fetch ${ch}:`, e.message); return []; }));
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
      if (!map[name]) map[name] = { name, campaign: '', spend: 0, clicks: 0, impressions: 0, demos: 0 };
      map[name].spend += parseFloat(row.spend) || 0;
      map[name].clicks += parseInt(row.clicks) || 0;
      map[name].impressions += parseInt(row.impressions) || 0;
      let d = parseFloat(row[cfg.demoField]) || 0;
      if (ch === 'google') d = Math.ceil(d); else d = Math.round(d);
      map[name].demos += d;
      if (!map[name].campaign && campName) map[name].campaign = campName;
    }
    const list = Object.values(map).map(a => {
      a.ctr = a.impressions > 0 ? (a.clicks / a.impressions) * 100 : 0;
      a.cpd = a.demos > 0 ? a.spend / a.demos : null;
      a.cpc = a.clicks > 0 ? a.spend / a.clicks : null;
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
    meta:{ generatedAt:new Date().toISOString(), windowType },
  };
}

async function processAzRequest(windowType, customFrom, customTo, env, vsFrom, vsTo) {
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
  const companyAssociations = {};
  for (let i = 0; i < dealIds.length; i += 100) {
    const batch = dealIds.slice(i, i + 100);
    try {
      const assocRes = await fetch('https://api.hubapi.com/crm/v4/associations/deals/companies/batch/read', {
        method: 'POST',
        headers: { Authorization: `Bearer ${hsToken}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ inputs: batch.map(id => ({ id: String(id) })) }),
      });
      const assocData = await assocRes.json();
      for (const r of (assocData.results || [])) {
        const dealId = r.from?.id;
        const companyId = r.to?.[0]?.toObjectId;
        if (dealId && companyId) companyAssociations[dealId] = String(companyId);
      }
    } catch(e) { console.error('BD assoc batch error:', e); }
  }

  const uniqueCompanyIds = [...new Set(Object.values(companyAssociations))];
  const companyMap = {};
  for (let i = 0; i < uniqueCompanyIds.length; i += 100) {
    const batch = uniqueCompanyIds.slice(i, i + 100);
    try {
      const coRes = await fetch('https://api.hubapi.com/crm/v3/objects/companies/batch/read', {
        method: 'POST',
        headers: { Authorization: `Bearer ${hsToken}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ inputs: batch.map(id => ({ id })), properties: ['name'] }),
      });
      const coData = await coRes.json();
      for (const co of (coData.results || [])) {
        companyMap[co.id] = { name: co.properties?.name || '', id: co.id };
      }
    } catch(e) { console.error('BD company batch error:', e); }
  }

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
      companyName: company?.name||'', companyId: company?.id||'',
    };
  });

  return { deals: mappedDeals, ownerMap, meta: { generatedAt: new Date().toISOString(), dealCount: deals.length } };
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
      try {
        const result = await processRequest(body.window||'7d', body.from||null, body.to||null, env, body.vsFrom||null, body.vsTo||null);
        return jr(result);
      } catch(err) { console.error('Error:', err); return jr({ error: 'Internal error', detail: err.message }, 500); }
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
