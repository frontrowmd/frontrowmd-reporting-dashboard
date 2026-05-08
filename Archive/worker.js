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
};
const BUDGET_FALLBACK = BUDGET_BY_MONTH['2026-03'];
function getBudgetsForMonth(dateStr) {
  if (!dateStr) return BUDGET_FALLBACK;
  return BUDGET_BY_MONTH[dateStr.slice(0, 7)] || BUDGET_FALLBACK;
}

// Exact demo_given__status values → categories (from guide Section 4)
function categorizeDemoStatus(rawStatus) {
  if (!rawStatus || rawStatus.trim() === '') return 'blank';
  const s = rawStatus.trim();
  switch (s) {
    case 'Demo Given':
    case 'Demo Given at Rescheduled time':
      return 'qualified';
    case 'Demo Given, Qualified Company, too early':
      return 'tooEarly';
    case 'Not Qualified after the demo':
      return 'pruned';       // "Not Qualified" in guide
    case 'Disqualified, Meeting Cancelled':
      return 'pruned';       // "Disqualified Before Demo" in guide — CEO combines both
    case 'No Show':
      return 'rescheduled';  // guide: "Rescheduled"
    case 'No Showed':
      return 'noShow';       // guide: "Canceled"
    default:
      return 'blank';
  }
}

// Statuses where demo actually happened (guide Section 4)
const DEMO_HAPPENED = [
  'Demo Given', 'Demo Given at Rescheduled time',
  'Demo Given, Qualified Company, too early', 'Not Qualified after the demo',
];
const QUALIFIED_STATUSES = ['Demo Given', 'Demo Given at Rescheduled time'];

const FUNNEL_ORDER = ['qualified', 'tooEarly', 'pruned', 'noShow', 'rescheduled', 'blank'];
const FUNNEL_LABELS = { qualified:'Qual. Demo Given', tooEarly:'Too Early', pruned:'Pruned', noShow:'No Show', rescheduled:'Rescheduled', blank:'Blanks' };
const FUNNEL_COLORS = { qualified:'#172C45', tooEarly:'#7C3AED', pruned:'#F59E0B', noShow:'#EF4444', rescheduled:'#10B981', blank:'#9CA3AF' };

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

function dateMs(str) {
  if (!str) return NaN;
  if (/^\d+$/.test(str)) return parseInt(str);
  const [y,m,d] = str.split('-').map(Number);
  if (!y||!m||!d) return NaN;
  const ud = new Date(Date.UTC(y, m-1, d));
  return Date.UTC(y, m-1, d, 0, 0, 0, 0) - (etOff(ud) * 3600000);
}
function isoMs(str) { return str ? new Date(str).getTime() : NaN; }

function daysInMonth(y, m) { return new Date(Date.UTC(y, m+1, 0)).getUTCDate(); }

// ---------------------------------------------------------------------------
// Time Windows (guide: Section "Time Windows")
// ---------------------------------------------------------------------------
function computeWindows(windowType, customFrom, customTo) {
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
      const fD = new Date(customFrom+'T00:00:00Z'), tD = new Date(customTo+'T00:00:00Z');
      const span = Math.round((tD-fD)/86400000);
      const pT = new Date(fD); pT.setUTCDate(pT.getUTCDate()-1);
      const pF = new Date(pT); pF.setUTCDate(pF.getUTCDate()-span);
      prior = { from: fmt(pF), to: fmt(pT) };
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
async function hsSearch(token, objectType, filterGroups, properties, limit = 200) {
  const url = `https://api.hubapi.com/crm/v3/objects/${objectType}/search`;
  const all = [];
  let after = 0;
  while (true) {
    let lastErr;
    let resp;
    for (let attempt = 0; attempt < 4; attempt++) {
      try {
        resp = await fetch(url, {
          method: 'POST',
          headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
          body: JSON.stringify({ filterGroups, properties, limit, after, sorts: [{ propertyName: 'hs_createdate', direction: 'ASCENDING' }] }),
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
    if (d.paging?.next?.after) after = d.paging.next.after; else break;
    if (all.length >= 10000) break;
  }
  return all;
}

// Demos Booked = contacts created in window with date_demo_booked set (guide Section 3)
// Windsor date clamp — don't query before ad data exists
function wFrom(dateStr) { return dateStr < WINDSOR_EPOCH ? WINDSOR_EPOCH : dateStr; }

// Fetch deal IDs to exclude (associated company brand_status = Churned or Paused)
async function fetchExcludedDealIds(token) {
  const companies = await hsSearch(token, 'companies', [{
    filters: [{ propertyName: 'brand_status', operator: 'IN', values: ['Churned', 'Paused'] }],
  }], ['brand_status']);
  if (!companies.length) return new Set();
  // Batch-fetch associated deal IDs
  const inputs = companies.map(c => ({ id: c.id }));
  const excludedIds = new Set();
  // HubSpot v4 batch associations — max 100 per call
  for (let i = 0; i < inputs.length; i += 100) {
    const batch = inputs.slice(i, i + 100);
    try {
      const r = await fetch('https://api.hubapi.com/crm/v4/associations/companies/deals/batch/read', {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ inputs: batch }),
      });
      if (r.ok) {
        const d = await r.json();
        for (const res of (d.results || [])) {
          for (const to of (res.to || [])) {
            excludedIds.add(String(to.toObjectId));
          }
        }
      }
    } catch(e) { console.error('Assoc batch error:', e); }
  }
  return excludedIds;
}

async function fetchScheduledContacts(token, from, to) {
  return hsSearch(token, 'contacts', [{
    filters: [
      { propertyName: 'createdate', operator: 'GTE', value: String(toMsET(from)) },
      { propertyName: 'createdate', operator: 'LTE', value: String(toMsET(to, true)) },
      { propertyName: 'date_demo_booked', operator: 'HAS_PROPERTY' },
    ],
  }], ['createdate', 'date_demo_booked']);
}

// Pipeline deals (guide Section 4 — two filterGroups OR)
async function fetchPipelineDeals(token, from, to) {
  const fMs = String(toMsET(from)), tMs = String(toMsET(to, true));
  const deals = await hsSearch(token, 'deals', [
    { filters: [
      { propertyName: 'date_demo_booked', operator: 'GTE', value: fMs },
      { propertyName: 'date_demo_booked', operator: 'LTE', value: tMs },
    ]},
    { filters: [
      { propertyName: 'demo_given__status', operator: 'IN', values: ['No Show', 'No Showed'] },
      { propertyName: 'hs_createdate', operator: 'GTE', value: fMs },
      { propertyName: 'hs_createdate', operator: 'LTE', value: tMs },
    ]},
  ], ['date_demo_booked','demo_given_date','demo_given__status','dealstage','amount','closedate','hs_createdate','hubspot_owner_id','utm_source','utm_medium','utm_campaign','utm_content']);
  return [...new Map(deals.map(d => [d.id, d])).values()]; // dedup
}

// Closed-won deals by closedate (guide Section 5)
async function fetchClosedWonDeals(token, from, to) {
  return hsSearch(token, 'deals', [{
    filters: [
      { propertyName: 'dealstage', operator: 'EQ', value: 'closedwon' },
      { propertyName: 'closedate', operator: 'GTE', value: String(toMsET(from)) },
      { propertyName: 'closedate', operator: 'LTE', value: String(toMsET(to, true)) },
    ],
  }], ['amount','closedate','hs_createdate','utm_source','utm_medium','utm_campaign','utm_content','hubspot_owner_id']);
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
  for (const c of contacts) {
    const cd = c.properties?.createdate;
    if (!cd) continue;
    const d = new Date(cd);
    const o = etOff(d);
    const et = new Date(d.getTime() + o * 3600000);
    const ds = fmt(et);
    byDay[ds] = (byDay[ds]||0) + 1;
  }
  return { total: contacts.length, byDay };
}

// ---------------------------------------------------------------------------
// Processing — Pipeline Deals (guide Section 4)
// ---------------------------------------------------------------------------
function processPipelineDeals(deals, winFrom, winTo) {
  const windowDeals = deals.filter(d => {
    const bMs = dateMs(d.properties?.date_demo_booked);
    const cMs = d.properties?.hs_createdate ? parseInt(d.properties.hs_createdate) : NaN;
    const st = (d.properties?.demo_given__status||'').trim();
    if (!d.properties?.date_demo_booked && (st==='No Show'||st==='No Showed'))
      return !isNaN(cMs) && cMs >= winFrom && cMs <= winTo;
    return !isNaN(bMs) && bMs >= winFrom && bMs <= winTo;
  });

  const total = windowDeals.length;
  let demosHappened=0, tooEarlyCount=0, notQualCount=0;
  const byCat = {}; for (const c of FUNNEL_ORDER) byCat[c] = 0;
  const byRep = {}, byChannel = {};
  for (const c of DASH_CHANNELS) byChannel[c] = { qualified:0, total:0 };
  byChannel['unattributed'] = { qualified:0, total:0 };
  const byDay = {};

  for (const deal of windowDeals) {
    const p = deal.properties||{};
    const rawSt = (p.demo_given__status||'').trim();
    const cat = categorizeDemoStatus(rawSt);
    const ownerId = p.hubspot_owner_id || 'unassigned';

    byCat[cat]++;
    if (DEMO_HAPPENED.includes(rawSt)) demosHappened++;
    if (rawSt === 'Demo Given, Qualified Company, too early') tooEarlyCount++;
    if (rawSt === 'Not Qualified after the demo') notQualCount++;

    // Per-rep
    if (!byRep[ownerId]) { byRep[ownerId] = { total:0 }; for (const c of FUNNEL_ORDER) byRep[ownerId][c] = 0; }
    byRep[ownerId].total++; byRep[ownerId][cat]++;

    // Per-channel via UTM
    const ch = mapUtmToChannel(p.utm_source, p.utm_medium) || 'unattributed';
    if (byChannel[ch]) { byChannel[ch][cat]++; byChannel[ch].total++; }

    // Daily chart
    const ds = p.date_demo_booked ? p.date_demo_booked.substring(0,10) : null;
    if (ds) { if (!byDay[ds]) byDay[ds] = { deals:0, qualified:0 }; byDay[ds].deals++; if (cat==='qualified') byDay[ds].qualified++; }
  }

  // Guide formulas (Section 4)
  const demoShowRate = total > 0 ? (demosHappened / total) * 100 : 0;
  const qualDemoGivenPct = total > 0 ? ((demosHappened - tooEarlyCount - notQualCount) / total) * 100 : 0;
  const qualifiedCount = byCat.qualified;
  const blanksCount = byCat.blank;
  const noShowCount = (byCat.noShow||0) + (byCat.rescheduled||0);
  const noShowDenom = qualifiedCount + tooEarlyCount + noShowCount + blanksCount;
  const noShowRate = noShowDenom > 0 ? (noShowCount / noShowDenom) * 100 : 0;

  return {
    total, qualifiedCount, blanksCount, demosHappened, tooEarlyCount, notQualCount,
    demoShowRate, qualDemoGivenPct, noShowRate, noShowCount,
    byCategory: byCat, byRep, byChannel, byDay,
  };
}

// ---------------------------------------------------------------------------
// Processing — Closed Won (guide Section 5)
// ---------------------------------------------------------------------------
function processClosedWonDeals(deals) {
  let mrr=0, count=0;
  const byRep = {}, byChannel = {};
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

    const closeMs = isoMs(p.closedate), createMs = isoMs(p.hs_createdate);
    if (!isNaN(closeMs) && !isNaN(createMs) && closeMs > createMs) cycleDays.push((closeMs-createMs)/(1000*60*60*24));
  }

  const avgCycleDays = cycleDays.length > 0 ? cycleDays.reduce((a,b)=>a+b,0)/cycleDays.length : null;
  return { mrr, count, byRep, byChannel, avgCycleDays };
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
  'Demo Given, Qualified Company, too early', 'Not Qualified after the demo',
];

function buildSignUpCohorts(allDeals, cohortMonths, ownerMap) {
  const eM = effM(0.025); // fixed 2.5% churn for sign-up rate

  // Pre-build empty month buckets
  const buckets = {};
  for (const cm of cohortMonths) {
    buckets[cm.label] = {
      period: cm,
      demosGiven: 0, closedWon: 0, closedLost: 0, stillOpen: 0,
      tooEarly: 0, notQualified: 0, mrr: 0, cycleDays: [],
      byRep: {},
    };
  }

  // Build month key lookup: "2026-03" → cohortMonth label
  const monthToLabel = {};
  for (const cm of cohortMonths) monthToLabel[cm.from.slice(0, 7)] = cm.label;

  for (const deal of allDeals) {
    const p = deal.properties || {};
    const ddb = p.date_demo_booked;
    if (!ddb) continue; // skip deals without date_demo_booked

    const status = (p.demo_given__status || '').trim();
    if (!ALL_DEMO_HAPPENED.includes(status)) continue; // skip non-happened statuses

    // Route to correct month bucket
    const monthKey = ddb.substring(0, 7); // "2026-03"
    const label = monthToLabel[monthKey];
    if (!label) continue; // outside our cohort window
    const b = buckets[label];

    const stage = (p.dealstage || '').toLowerCase();
    const amt = parseFloat(p.amount) || 0;
    const oid = p.hubspot_owner_id || 'unassigned';

    if (!b.byRep[oid]) b.byRep[oid] = { name: ownerMap[oid] || (oid === 'unassigned' ? 'Unassigned' : oid), demosGiven: 0, closedWon: 0, closedLost: 0, stillOpen: 0, tooEarly: 0, notQualified: 0, mrr: 0 };

    // Route Too Early and Not Qualified to their own counters (NOT demosGiven)
    if (status === 'Demo Given, Qualified Company, too early') {
      b.tooEarly++; b.byRep[oid].tooEarly++;
      continue;
    }
    if (status === 'Not Qualified after the demo') {
      b.notQualified++; b.byRep[oid].notQualified++;
      continue;
    }

    // Qualified demo — counts as demosGiven
    b.demosGiven++; b.byRep[oid].demosGiven++;
    if (stage === 'closedwon') {
      b.closedWon++; b.mrr += amt;
      b.byRep[oid].closedWon++; b.byRep[oid].mrr += amt;
      const closeMs = isoMs(p.closedate), createMs = isoMs(p.hs_createdate);
      if (!isNaN(closeMs) && !isNaN(createMs) && closeMs > createMs) b.cycleDays.push((closeMs - createMs) / (1000 * 60 * 60 * 24));
    } else if (stage === 'closedlost') {
      b.closedLost++; b.byRep[oid].closedLost++;
    } else {
      b.stillOpen++; b.byRep[oid].stillOpen++;
    }
  }

  // Build final cohort objects — current month first, then descending
  const cohorts = [];
  for (const cm of cohortMonths) {
    const b = buckets[cm.label];
    const closeRate = b.demosGiven > 0 ? (b.closedWon / b.demosGiven) * 100 : 0;
    const avgCycleDays = b.cycleDays.length > 0 ? b.cycleDays.reduce((a, v) => a + v, 0) / b.cycleDays.length : null;
    const arr = b.mrr * eM;

    // Per-rep: compute sign-up rate = closedWon / demosGiven
    const repData = {};
    for (const [oid, r] of Object.entries(b.byRep)) {
      repData[oid] = {
        ...r, signUpRate: r.demosGiven > 0 ? (r.closedWon / r.demosGiven) * 100 : 0,
        arr: r.mrr * eM,
      };
    }

    cohorts.push({
      period: cm,
      demosGiven: b.demosGiven, closedWon: b.closedWon, closedLost: b.closedLost,
      stillOpen: b.stillOpen, tooEarly: b.tooEarly, notQualified: b.notQualified,
      closeRate, avgCycleDays, mrr: b.mrr, arr,
      dataGaps: b.stillOpen,
      byRep: repData,
    });
  }

  return { cohorts };
}

// Dedicated all-time fetches — no date filter, simpler queries = reliable pagination
async function fetchAllClosedWon(token) {
  return hsSearch(token, 'deals', [{
    filters: [{ propertyName: 'dealstage', operator: 'EQ', value: 'closedwon' }],
  }], ['amount','closedate','hs_createdate','hubspot_owner_id']);
}

async function fetchAllQualifiedDeals(token) {
  return hsSearch(token, 'deals', [{
    filters: [{ propertyName: 'demo_given__status', operator: 'IN', values: ['Demo Given', 'Demo Given at Rescheduled time'] }],
  }], ['demo_given__status','dealstage','hubspot_owner_id']);
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
  const totalQual = DASH_CHANNELS.reduce((s,ch) => s + (c.pipeline.byChannel[ch]?.qualified||0), 0);
  const cpdTotal = totalWD > 0 ? totalSpend/totalWD : null;
  const cpqdTotal = totalQual > 0 ? totalSpend/totalQual : null;

  const pTotalS = p.adSpend?.total?.spend||0;
  const pTotalWD = p.adSpend?.total?.windsorDemos||0;
  const pTotalQ = prior ? DASH_CHANNELS.reduce((s,ch) => s + (p.pipeline?.byChannel?.[ch]?.qualified||0), 0) : 0;
  const pmTotalS = pm.adSpend?.total?.spend||0;
  const pmTotalWD = pm.adSpend?.total?.windsorDemos||0;
  const pmTotalQ = priorMonth ? DASH_CHANNELS.reduce((s,ch) => s + (pm.pipeline?.byChannel?.[ch]?.qualified||0), 0) : 0;

  const executiveSummary = {
    totalDemosScheduled: buildTile(c.scheduled.total, p.scheduled?.total??null, pm.scheduled?.total??null, 'Contacts created in period with date_demo_booked set'),
    totalCpd: buildTile(cpdTotal, prior&&pTotalWD>0?pTotalS/pTotalWD:null, priorMonth&&pmTotalWD>0?pmTotalS/pmTotalWD:null, 'Total Ad Spend ÷ Total Demos'),
    qualDemoGivenPct: buildTile(c.pipeline.qualDemoGivenPct, p.pipeline?.qualDemoGivenPct??null, pm.pipeline?.qualDemoGivenPct??null, '(Demos Happened − Too Early − Not Qual) ÷ Demos to Occur × 100'),
    totalCpqd: buildTile(cpqdTotal, prior&&pTotalQ>0?pTotalS/pTotalQ:null, priorMonth&&pmTotalQ>0?pmTotalS/pmTotalQ:null, 'Total Ad Spend ÷ Total Qualified Demos'),
    _meta: { totalSpend, totalWD, totalQual },
  };

  // ── Web Performance (guide Section 2 + Section 10 — CVR uses HubSpot demos / users) ──
  const cvr = c.ga4.users > 0 ? (c.scheduled.total / c.ga4.users) * 100 : 0;
  const pCvr = prior && p.ga4?.users > 0 ? (p.scheduled?.total / p.ga4.users) * 100 : null;
  const pmCvr = priorMonth && pm.ga4?.users > 0 ? (pm.scheduled?.total / pm.ga4.users) * 100 : null;

  const webPerformance = {
    visitors: buildTile(c.ga4.users, p.ga4?.users??null, pm.ga4?.users??null, 'Unique users (GA4)'),
    cvr: buildTile(cvr, pCvr, pmCvr, 'Demos Booked (HubSpot) ÷ Website Visitors × 100'),
    _meta: { demosBooked: c.scheduled.total, users: c.ga4.users },
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
      priorSpend: p.adSpend?.channels?.[ch]?.spend??null,
      priorWindsorDemos: p.adSpend?.channels?.[ch]?.windsorDemos??null,
      priorMonthSpend: pm.adSpend?.channels?.[ch]?.spend??null,
      qualified: c.pipeline.byChannel[ch]?.qualified||0,
      priorQualified: p.pipeline?.byChannel?.[ch]?.qualified??null,
      closedWon: c.closedWon.byChannel[ch]?.count||0,
      closedWonMRR: c.closedWon.byChannel[ch]?.mrr||0,
      priorClosedWon: p.closedWon?.byChannel?.[ch]?.count??null,
      priorClosedWonMRR: p.closedWon?.byChannel?.[ch]?.mrr??null,
    };
  }

  // ── Demo Tracking ──
  const demoTracking = {
    totalScheduled: buildTile(c.scheduled.total, p.scheduled?.total??null, pm.scheduled?.total??null, 'Contacts created in period with date_demo_booked set'),
    demosToOccur: buildTile(c.pipeline.total, p.pipeline?.total??null, pm.pipeline?.total??null, 'Total deals with date_demo_booked in window'),
    demosHappened: buildTile(c.pipeline.demosHappened, p.pipeline?.demosHappened??null, pm.pipeline?.demosHappened??null, 'Deals where demo actually happened'),
    qualifiedOccurred: buildTile(c.pipeline.qualifiedCount, p.pipeline?.qualifiedCount??null, pm.pipeline?.qualifiedCount??null, 'Deals with Demo Given or Demo Given at Rescheduled time'),
    demoShowRate: buildTile(c.pipeline.demoShowRate, p.pipeline?.demoShowRate??null, pm.pipeline?.demoShowRate??null, 'Demos Happened ÷ Demos to Occur × 100'),
    noShowRate: buildTile(c.pipeline.noShowRate, p.pipeline?.noShowRate??null, pm.pipeline?.noShowRate??null, 'No Shows ÷ (Qual + Too Early + No Shows + Blanks)'),
    blanks: buildTile(c.pipeline.blanksCount, p.pipeline?.blanksCount??null, pm.pipeline?.blanksCount??null, 'Deals with empty demo_given__status'),
    demosPaidPct: buildTile(
      c.scheduled.total > 0 ? (c.adSpend.total.windsorDemos / c.scheduled.total)*100 : 0,
      prior && p.scheduled?.total > 0 ? (p.adSpend?.total?.windsorDemos / p.scheduled.total)*100 : null,
      priorMonth && pm.scheduled?.total > 0 ? (pm.adSpend?.total?.windsorDemos / pm.scheduled.total)*100 : null,
      'Ad Demos ÷ HubSpot Demos Booked × 100'
    ),
    dailyChart: c.pipeline.byDay,
    scheduledByDay: c.scheduled.byDay,
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
    meta: { generatedAt: new Date().toISOString(), funnelDataAvailableFrom: '2026-02-01', adDataAvailableFrom: WINDSOR_EPOCH, dataQuality: dq, windowType },
  };
}

// ---------------------------------------------------------------------------
// Build Period Data
// ---------------------------------------------------------------------------
function buildPeriodData(period, windsorRows, linkedInDemos, ga4Rows, scheduledContacts, pipelineDeals, closedWonDeals) {
  const winFrom = toMsET(period.from), winTo = toMsET(period.to, true);
  return {
    period,
    adSpend: processAdSpend(windsorRows, linkedInDemos),
    ga4: processGA4(ga4Rows),
    scheduled: processScheduledContacts(scheduledContacts),
    pipeline: processPipelineDeals(pipelineDeals, winFrom, winTo),
    closedWon: processClosedWonDeals(closedWonDeals),
    campaigns: processCampaigns(windsorRows),
  };
}

// ---------------------------------------------------------------------------
// Main Processing
// ---------------------------------------------------------------------------
async function processRequest(windowType, customFrom, customTo, env) {
  const apiKey = env.WINDSOR_API_KEY, hsToken = env.HUBSPOT_TOKEN;
  const { current, prior, priorMonth } = computeWindows(windowType, customFrom, customTo);
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
  const windsorPromises = [fetchWindsorAds(apiKey, wFrom(current.from), current.to), fetchLinkedInDemos(apiKey, wFrom(current.from), current.to), fetchGA4(apiKey, current.from, current.to)];
  if (prior) windsorPromises.push(fetchWindsorAds(apiKey, wFrom(prior.from), prior.to), fetchLinkedInDemos(apiKey, wFrom(prior.from), prior.to), fetchGA4(apiKey, prior.from, prior.to));
  if (priorMonth) windsorPromises.push(fetchWindsorAds(apiKey, wFrom(priorMonth.from), priorMonth.to), fetchLinkedInDemos(apiKey, wFrom(priorMonth.from), priorMonth.to), fetchGA4(apiKey, priorMonth.from, priorMonth.to));
  const windsorResults = await Promise.all(windsorPromises);
  let wIdx = 0;
  const cW = windsorResults[wIdx++], cLI = windsorResults[wIdx++], cG = windsorResults[wIdx++];
  let pW, pLI, pG; if (prior) { pW = windsorResults[wIdx++]; pLI = windsorResults[wIdx++]; pG = windsorResults[wIdx++]; }
  let pmW, pmLI, pmG; if (priorMonth) { pmW = windsorResults[wIdx++]; pmLI = windsorResults[wIdx++]; pmG = windsorResults[wIdx++]; }

  // ── Phase 2: Run HubSpot calls sequentially (avoids 429 rate limits) ──
  const cSch = await fetchScheduledContacts(hsToken, current.from, current.to);
  const cPipe = await fetchPipelineDeals(hsToken, current.from, current.to);
  const cCW = await fetchClosedWonDeals(hsToken, current.from, current.to);

  let pSch, pPipe, pCW;
  if (prior) {
    pSch = await fetchScheduledContacts(hsToken, prior.from, prior.to);
    pPipe = await fetchPipelineDeals(hsToken, prior.from, prior.to);
    pCW = await fetchClosedWonDeals(hsToken, prior.from, prior.to);
  }

  let pmSch, pmPipe, pmCW;
  if (priorMonth) {
    pmSch = await fetchScheduledContacts(hsToken, priorMonth.from, priorMonth.to);
    pmPipe = await fetchPipelineDeals(hsToken, priorMonth.from, priorMonth.to);
    pmCW = await fetchClosedWonDeals(hsToken, priorMonth.from, priorMonth.to);
  }

  const ownerMap = await fetchOwners(hsToken);
  // Always fetch excluded deal IDs (needed for All Time by Rep on Sign-Up Rate page)
  const excludedDealIds = await fetchExcludedDealIds(hsToken);

  // Single sign-up cohort fetch: full 4-month window
  const cohortDeals = await fetchPipelineDeals(hsToken, cohortStart, cohortEnd);

  // All-time deals for rep summary (no date filter — fetches all, reliable pagination)
  const [allTimeClosedWon, allTimeQualified] = await Promise.all([
    fetchAllClosedWon(hsToken),
    fetchAllQualifiedDeals(hsToken),
  ]);

  // ── Build period data (filter out churned/paused company deals on All Time tab only) ──
  const filterDeals = (deals) => excludedDealIds.size > 0 ? deals.filter(d => !excludedDealIds.has(d.id)) : deals;
  const filterForAllTime = isAllTime ? filterDeals : (deals) => deals;
  const currentData = buildPeriodData(current, cW, cLI, cG, cSch, filterForAllTime(cPipe), filterForAllTime(cCW));
  const priorData = prior ? buildPeriodData(prior, pW, pLI, pG, pSch, filterForAllTime(pPipe), filterForAllTime(pCW)) : null;
  const priorMonthData = priorMonth ? buildPeriodData(priorMonth, pmW, pmLI, pmG, pmSch, filterForAllTime(pmPipe), filterForAllTime(pmCW)) : null;

  // ── Sign-Up Rate cohorts (single query, bucketed by month) ──
  currentData.signUpRate = buildSignUpCohorts(cohortDeals, cohortMonths, ownerMap);
  currentData.signUpRate.allTimeByRep = buildAllTimeRepStats(allTimeClosedWon, allTimeQualified, ownerMap);

  return buildResponse(currentData, priorData, priorMonthData, isAllTime, ownerMap, windowType);
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
      promises.push(azWindsorFetch(apiKey, cfg.connector, from, to, base + extra + freq).catch(e => { console.error(`Creative fetch ${ch}:`, e.message); return []; }));
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
    const tm = thumbMaps[ch] || {};
    for (const row of rows) {
      const campName = row.campaign_name || row.campaign || '';
      const isYT = /\byt\b|youtube/i.test(campName);
      const rCh = /google/.test(cfg.connector) ? (isYT ? 'youtube' : 'google') : ch;
      if (rCh !== ch) continue;
      const name = row.ad_name || '(no creative)';
      if (!map[name]) map[name] = { name, spend:0, clicks:0, impressions:0, demos:0, freqVals:[], thumbnail: tm[name] || null };
      map[name].spend += parseFloat(row.spend)||0;
      map[name].clicks += parseInt(row.clicks)||0;
      map[name].impressions += parseInt(row.impressions)||0;
      if (ch !== 'linkedin') {
        const rawD = parseFloat(row[cfg.demoField])||0;
        map[name].demos += (ch==='google'||ch==='youtube') ? Math.ceil(rawD) : Math.round(rawD);
      }
      if (cfg.hasFreq && row.frequency != null && row.frequency !== '') map[name].freqVals.push(parseFloat(row.frequency));
    }
    for (const c of Object.values(map)) {
      c.ctr = c.impressions > 0 ? (c.clicks/c.impressions)*100 : 0;
      c.cpd = c.demos > 0 ? c.spend/c.demos : null;
      c.frequency = c.freqVals.length ? c.freqVals.reduce((a,b)=>a+b,0)/c.freqVals.length : null;
      delete c.freqVals;
    }
    results[ch] = Object.values(map).sort((a,b)=>b.spend-a.spend);
  }
  return results;
}


function buildAzAttribution(deals, closedWonDeals) {
  const byChannel = {}, byCampaign = {}, byCreative = {};
  for (const ch of DASH_CHANNELS) byChannel[ch] = { qualified:0, tooEarly:0, notQualAfter:0, disqualifiedBefore:0, rescheduled:0, canceled:0, noShow:0, blank:0, total:0 };

  for (const deal of deals) {
    const p = deal.properties||{};
    const ch = mapUtmToChannel(p.utm_source, p.utm_medium);
    if (!ch) continue;
    const cat = categorizeDemoStatus(p.demo_given__status);
    byChannel[ch].total++; byChannel[ch][cat]++;
    const camp = (p.utm_campaign||'').trim();
    if (camp) {
      const key = `${ch}::${camp.toLowerCase()}`;
      if (!byCampaign[key]) byCampaign[key] = { channel:ch, campaign:camp.toLowerCase(), qualified:0, tooEarly:0, notQualAfter:0, disqualifiedBefore:0, rescheduled:0, canceled:0, noShow:0, blank:0, total:0 };
      byCampaign[key].total++; byCampaign[key][cat]++;
    }
    const creative = (p.utm_content||'').trim();
    if (creative) {
      const key = `${ch}::${creative.toLowerCase()}`;
      if (!byCreative[key]) byCreative[key] = { channel:ch, creative:creative.toLowerCase(), qualified:0, tooEarly:0, notQualAfter:0, disqualifiedBefore:0, rescheduled:0, canceled:0, noShow:0, blank:0, total:0 };
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

function buildAzResponse(period, prior, priorMonth, windsor, creatives, priorW, pmW, attr, priorA, pmA, budgets, isAllTime, windowType) {
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
      mergedCamps.push({ ...c, demos, cpd, qualified:ca.qualified||0, tooEarly:ca.tooEarly||0, notQualAfter:ca.notQualAfter||0, disqualifiedBefore:ca.disqualifiedBefore||0, rescheduled:ca.rescheduled||0, canceled:ca.canceled||0, noShow:ca.noShow||0, blank:ca.blank||0, attributedTotal:attrTotal });
    }
    mergedCamps.sort((a,b)=>b.demos-a.demos);

    // Creatives (may be null if timed out) — merge with utm_content attribution
    const rawCreatives = creatives?.[ch] || null;
    let mergedCreatives = null;
    if (rawCreatives) {
      const creativeAttr = {};
      for (const [key,ca] of Object.entries(attr.byCreative||{})) {
        if (key.startsWith(ch+'::')) creativeAttr[key.slice(ch.length+2)] = ca;
      }
      mergedCreatives = rawCreatives.map(c => {
        const ca = creativeAttr[c.name.toLowerCase()]||{};
        const attrTotal = ca.total||0;
        const demos = ch === 'linkedin' ? attrTotal : c.demos;
        const cpd = demos > 0 ? c.spend / demos : null;
        return { ...c, demos, cpd, qualified:ca.qualified||0, tooEarly:ca.tooEarly||0, notQualAfter:ca.notQualAfter||0, disqualifiedBefore:ca.disqualifiedBefore||0, rescheduled:ca.rescheduled||0, canceled:ca.canceled||0, noShow:ca.noShow||0, blank:ca.blank||0, attributedTotal:attrTotal };
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
      qualified:q, tooEarly:a.tooEarly||0, notQualAfter:a.notQualAfter||0,
      disqualifiedBefore:a.disqualifiedBefore||0, rescheduled:a.rescheduled||0,
      canceled:a.canceled||0, noShow:a.noShow||0, blank:a.blank||0,
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

async function processAzRequest(windowType, customFrom, customTo, env) {
  const apiKey = env.WINDSOR_API_KEY, hsToken = env.HUBSPOT_TOKEN;
  const { current, prior, priorMonth } = computeWindows(windowType, customFrom, customTo);
  const isAllTime = windowType === 'allTime';

  // Current period: Windsor campaigns + creatives + HubSpot (clamp Windsor dates)
  const windsorP = fetchAzCampaigns(apiKey, wFrom(current.from), current.to);
  const creativesP = fetchAzCreatives(apiKey, wFrom(current.from), current.to);
  const pipeP = fetchPipelineDeals(hsToken, current.from, current.to);
  const cwP = fetchClosedWonDeals(hsToken, current.from, current.to);
  const exclP = isAllTime ? fetchExcludedDealIds(hsToken) : Promise.resolve(new Set());
  const [windsor, creatives, pipe, cw, excludedDealIds] = await Promise.all([windsorP, creativesP, pipeP, cwP, exclP]);
  const filterDeals = (deals) => excludedDealIds.size > 0 ? deals.filter(d => !excludedDealIds.has(d.id)) : deals;

  // Prior period
  let priorW=null, priorA=null;
  if (prior && !isAllTime) {
    const [pw, pp, pc] = await Promise.all([
      fetchAzCampaigns(apiKey, wFrom(prior.from), prior.to),
      fetchPipelineDeals(hsToken, prior.from, prior.to),
      fetchClosedWonDeals(hsToken, prior.from, prior.to),
    ]);
    priorW = pw; priorA = buildAzAttribution(filterDeals(pp), filterDeals(pc));
  }

  // Prior month
  let pmW=null, pmA=null;
  if (priorMonth && !isAllTime) {
    const [pw, pp, pc] = await Promise.all([
      fetchAzCampaigns(apiKey, wFrom(priorMonth.from), priorMonth.to),
      fetchPipelineDeals(hsToken, priorMonth.from, priorMonth.to),
      fetchClosedWonDeals(hsToken, priorMonth.from, priorMonth.to),
    ]);
    pmW = pw; pmA = buildAzAttribution(filterDeals(pp), filterDeals(pc));
  }

  const attr = buildAzAttribution(filterDeals(pipe), filterDeals(cw));
  const budgets = getBudgetsForMonth(current.from);

  return buildAzResponse(current, prior, priorMonth, windsor, creatives, priorW, pmW, attr, priorA, pmA, budgets, isAllTime, windowType);
}

// ---------------------------------------------------------------------------
// Worker Entry Point
// ---------------------------------------------------------------------------
export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    // CORS preflight
    if (request.method === 'OPTIONS') return new Response(null, { headers: { 'Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST, OPTIONS','Access-Control-Allow-Headers':'Content-Type','Access-Control-Max-Age':'86400' } });

    // GET → serve the dashboard HTML
    if (request.method === 'GET' && (url.pathname === '/' || url.pathname === '')) {
      return new Response(DASHBOARD_HTML, { headers: { 'Content-Type':'text/html;charset=UTF-8','Cache-Control':'no-cache' } });
    }

    // POST /api/data → Dashboard API
    if (request.method === 'POST' && url.pathname === '/api/data') {
      let body;
      try { body = await request.json(); } catch { return jr({ error: 'Invalid JSON' }, 400); }
      if (body.password !== env.TEAM_PASSWORD) return jr({ error: 'Unauthorized' }, 401);
      try {
        const result = await processRequest(body.window||'7d', body.from||null, body.to||null, env);
        return jr(result);
      } catch(err) { console.error('Error:', err); return jr({ error: 'Internal error', detail: err.message }, 500); }
    }

    // POST /api/analyzer → Paid Channel Analyzer API
    if (request.method === 'POST' && url.pathname === '/api/analyzer') {
      let body;
      try { body = await request.json(); } catch { return jr({ error: 'Invalid JSON' }, 400); }
      if (body.password !== env.TEAM_PASSWORD) return jr({ error: 'Unauthorized' }, 401);
      try {
        const result = await processAzRequest(body.window||'mtd', body.from||null, body.to||null, env);
        return jr(result);
      } catch(err) { console.error('Analyzer error:', err); return jr({ error: 'Internal error', detail: err.message }, 500); }
    }

    return jr({ error: 'Not found' }, 404);
  },
};

// __DASHBOARD_HTML_PLACEHOLDER__ — build.js replaces this line
const _DASHBOARD_B64 = '';
const DASHBOARD_HTML = (() => { try { const b = atob(_DASHBOARD_B64); const bytes = new Uint8Array(b.length); for(let i=0;i<b.length;i++) bytes[i]=b.charCodeAt(i); return new TextDecoder().decode(bytes); } catch(e) { return '<html><body>Dashboard failed to load: '+e.message+'</body></html>'; } })();

function jr(data, status=200) {
  return new Response(JSON.stringify(data), { status, headers: { 'Content-Type':'application/json','Access-Control-Allow-Origin':'*' } });
}
