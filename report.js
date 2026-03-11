require('dotenv').config();
const fetch = (...args) => import('node-fetch').then(({default: f}) => f(...args));
process.env.TZ = 'America/New_York';
const fs = require('fs');

const WINDSOR_KEY    = process.env.WINDSOR_API_KEY;
const HUBSPOT_TOKEN  = process.env.HUBSPOT_TOKEN;

// ── Delivery config (set in .env) ─────────────────────────────────────────────
const SLACK_WEBHOOK    = process.env.SLACK_WEBHOOK;
const EMAIL_FROM       = process.env.EMAIL_FROM;
const EMAIL_PASS       = process.env.EMAIL_PASS;
const EMAIL_TO         = process.env.EMAIL_TO ? process.env.EMAIL_TO.split(',').map(e => e.trim()) : [];
const GITHUB_TOKEN     = process.env.GITHUB_TOKEN;
const GITHUB_OWNER     = process.env.GITHUB_OWNER;   // your GitHub username
const GITHUB_REPO      = process.env.GITHUB_REPO;    // e.g. frontrowmd-dashboard

// ── Date helpers ──────────────────────────────────────────────────────────────
function toDateStr(d) { return d.toISOString().split('T')[0]; }
function getWindows() {
  const now      = new Date();
  const yest     = new Date(now); yest.setDate(now.getDate() - 1);
  const weekAgo  = new Date(now); weekAgo.setDate(now.getDate() - 7);
  const mtdStart = new Date(now.getFullYear(), now.getMonth(), 1);
  const ytdStart = new Date(now.getFullYear(), 0, 1);

  // Previous periods
  const dayBefore    = new Date(yest);    dayBefore.setDate(yest.getDate() - 1);
  const prev7Start   = new Date(weekAgo); prev7Start.setDate(weekAgo.getDate() - 7);
  const prev7End     = new Date(weekAgo); prev7End.setDate(weekAgo.getDate() - 1);
  const prevMtdStart = new Date(now.getFullYear(), now.getMonth() - 1, 1);
  const prevMtdEnd   = new Date(now.getFullYear(), now.getMonth(), 0);

  // Last month = full previous calendar month; its prior = the month before that
  const lastMonthStart = new Date(now.getFullYear(), now.getMonth() - 1, 1);
  const lastMonthEnd   = new Date(now.getFullYear(), now.getMonth(), 0);
  const prevLMStart    = new Date(now.getFullYear(), now.getMonth() - 2, 1);
  const prevLMEnd      = new Date(now.getFullYear(), now.getMonth() - 1, 0);

  // Windsor data is capped at yesterday — labels reflect actual data window
  const fmt = d => d.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
  const yestStr    = fmt(yest);
  const weekAgoStr = fmt(weekAgo);
  const mtdStartStr = fmt(mtdStart);
  const ytdStartStr = fmt(ytdStart);
  const lmLabel     = lastMonthStart.toLocaleDateString('en-US', { month: 'long', year: 'numeric' });

  return {
    current: {
      yesterday:  { from: toDateStr(yest),           to: toDateStr(yest), label: `Yesterday (${yestStr})` },
      rolling7:   { from: toDateStr(weekAgo),        to: toDateStr(now),  label: `Last 7 Days (${weekAgoStr}–${yestStr})` },
      mtd:        { from: toDateStr(mtdStart),       to: toDateStr(now),  label: `Month to Date (${mtdStartStr}–${yestStr})` },
      lastmonth:  { from: toDateStr(lastMonthStart), to: toDateStr(lastMonthEnd), label: `Last Month (${lmLabel})` },
      ytd:        { from: toDateStr(ytdStart),       to: toDateStr(now),  label: `Year to Date (${ytdStartStr}–${yestStr})` },
    },
    previous: {
      yesterday:  { from: toDateStr(dayBefore),    to: toDateStr(dayBefore),  label: 'Day Before' },
      rolling7:   { from: toDateStr(prev7Start),   to: toDateStr(prev7End),   label: `Prior 7 Days (${fmt(prev7Start)}–${fmt(prev7End)})` },
      mtd:        { from: toDateStr(prevMtdStart), to: toDateStr(prevMtdEnd), label: `Prior Month (${fmt(prevMtdStart)}–${fmt(prevMtdEnd)})` },
      lastmonth:  { from: toDateStr(prevLMStart),  to: toDateStr(prevLMEnd),  label: `${prevLMStart.toLocaleDateString('en-US', { month: 'long', year: 'numeric' })}` },
      ytd:        { from: toDateStr(ytdStart),     to: toDateStr(ytdStart),   label: 'N/A' },
    },
  };
}

// ── Windsor helpers ───────────────────────────────────────────────────────────
const sleep = ms => new Promise(r => setTimeout(r, ms));

async function windsorFetch(dateFrom, dateTo, fields, extra = '', attempt = 1) {
  const url = `https://connectors.windsor.ai/all?api_key=${WINDSOR_KEY}&date_from=${dateFrom}&date_to=${dateTo}&fields=${fields}&page_size=5000${extra}`;
  const res = await fetch(url);
  const text = await res.text();
  let json;
  try { json = JSON.parse(text); }
  catch (e) {
    if (attempt <= 3) {
      console.warn(`  ⚠️  Windsor returned non-JSON [${dateFrom}] attempt ${attempt}/3 — retrying in ${attempt * 2}s`);
      await sleep(attempt * 2000);
      return windsorFetch(dateFrom, dateTo, fields, extra, attempt + 1);
    }
    console.error(`  ❌  Windsor bad response [${dateFrom}→${dateTo}]: ${text.slice(0, 200)}`);
    return [];
  }
  const data = json.data || [];
  if (data.length >= 5000) console.warn(`  ⚠️  Windsor hit 5000 row limit [${dateFrom}→${dateTo}]`);
  return data;
}

// ── Windsor: per-channel demo counts ─────────────────────────────────────────
async function fetchWindsorDemos(dateFrom, dateTo) {
  // Windsor doesn't return data for today (incomplete day) — cap at yesterday
  const yest = new Date(); yest.setDate(yest.getDate() - 1);
  const yesterdayStr = yest.toISOString().slice(0, 10);
  if (dateTo > yesterdayStr) dateTo = yesterdayStr;
  if (dateFrom > dateTo) return { meta: {spend:0,clicks:0,impressions:0,demos:0,ctr:[]}, linkedin:{spend:0,clicks:0,impressions:0,demos:0}, tiktok:{spend:0,clicks:0,impressions:0,demos:0}, google:{spend:0,clicks:0,impressions:0,demos:0,_raw:0}, youtube:{spend:0,clicks:0,impressions:0,demos:0,_raw:0} };

  const fields = [
    'date','datasource','campaign_name','spend','clicks','impressions','ctr',
    'conversions','externalwebsiteconversions','conversions_submit_application_total',
    'all_conversions'
  ].join(',');

  // Windsor truncates rows for multi-day ranges — fetch day-by-day in small batches
  const days = [];
  for (let d = new Date(dateFrom); d <= new Date(dateTo); d.setDate(d.getDate() + 1)) {
    days.push(d.toISOString().slice(0, 10));
  }
  const allDayRows = [];
  for (let i = 0; i < days.length; i += 5) {
    const batch = days.slice(i, i + 5);
    const batchResults = await Promise.all(batch.map(day => windsorFetch(day, day, fields)));
    allDayRows.push(...batchResults.flat());
    if (i + 5 < days.length) await sleep(300);
  }
  const data = allDayRows;

  // Diagnostic: show all datasources Windsor returned for this window + their total spend
  const dsSummary = {};
  for (const row of data) {
    const ds = row.datasource || '(none)';
    dsSummary[ds] = (dsSummary[ds] || 0) + (row.spend || 0);
  }
  console.log(`  🔍 Windsor datasources [${dateFrom}→${dateTo}] (${data.length} rows):`, Object.entries(dsSummary).map(([k,v]) => `${k}=${v.toFixed(2)}`).join('  |  ') || '(no rows)');

  const result = {
    meta:     { spend: 0, clicks: 0, impressions: 0, demos: 0, ctr: [] },
    linkedin: { spend: 0, clicks: 0, impressions: 0, demos: 0 },
    tiktok:   { spend: 0, clicks: 0, impressions: 0, demos: 0 },
    google:   { spend: 0, clicks: 0, impressions: 0, demos: 0, _raw: 0 },
    youtube:  { spend: 0, clicks: 0, impressions: 0, demos: 0, _raw: 0 },
  };

  for (const row of data) {
    const src      = (row.datasource || '').toLowerCase();
    const campaign = (row.campaign_name || '').toLowerCase();
    // YouTube: campaign name contains | YT | or youtube
    const isYT = /\byt\b|youtube/i.test(campaign);

    // Meta
    if (/facebook|meta|fb|ig|instagram/.test(src)) {
      result.meta.spend       += row.spend || 0;
      result.meta.clicks      += row.clicks || 0;
      result.meta.impressions += row.impressions || 0;
      result.meta.demos       += row.conversions_submit_application_total || 0;
      if (row.ctr != null) result.meta.ctr.push(row.ctr);
    }
    // LinkedIn (demos overridden below via separate conversion_name fetch)
    else if (/linkedin/.test(src)) {
      result.linkedin.spend       += row.spend || 0;
      result.linkedin.clicks      += row.clicks || 0;
      result.linkedin.impressions += row.impressions || 0;
      result.linkedin.demos       += row.externalwebsiteconversions || 0; // overridden below
    }
    // TikTok
    else if (/tiktok/.test(src)) {
      if (process.env.DEBUG_TT === '1') console.log(`  [TikTok row] date=${row.date} camp=${row.campaign_name} spend=${row.spend} conv=${row.conversions}`);
      result.tiktok.spend       += row.spend || 0;
      result.tiktok.clicks      += row.clicks || 0;
      result.tiktok.impressions += row.impressions || 0;
      result.tiktok.demos += row.conversions || 0;
    }
    // Google Ads (non-analytics, non-YouTube) or YouTube
    else if (/google/.test(src) && !/googleanalytics/.test(src)) {
      const bucket = isYT ? result.youtube : result.google;
      if (process.env.DEBUG_YT === '1') {
        console.log(`  [Google row] bucket=${isYT?'YT':'Search'} src="${src}" camp="${row.campaign_name}" spend=${row.spend}`);
      } else if (!row.campaign_name && (row.spend || 0) > 0) {
        console.log(`  ⚠️  [Google row no campaign_name] src="${src}" spend=${row.spend}`);
      }
      bucket.spend       += row.spend || 0;
      bucket.clicks      += row.clicks || 0;
      bucket.impressions += row.impressions || 0;
      bucket._raw        += row.conversions || 0;
    }
    // Catch-all: log any unmatched rows with spend so we can spot missing datasources
    else if ((row.spend || 0) > 0) {
      console.log(`  ⚠️  [UNMATCHED row] src="${src}" camp="${row.campaign_name}" spend=${row.spend}`);
    }
  }

  // Apply ceil after all rows accumulated
  result.google.demos  = Math.ceil(result.google._raw);
  result.youtube.demos = Math.ceil(result.youtube._raw);

  // ── LinkedIn demos: separate fetch with conversion_name filter ──
  // The main fetch uses externalwebsiteconversions which includes pipeline events
  // (HubSpot - Opportunity, HubSpot - SQL, Demo Scheduled). Fetching with
  // conversion_name lets us filter to only actual demo booking events.
  const liFields = 'date,datasource,conversion_name,externalwebsiteconversions';
  const liRows = [];
  for (let i = 0; i < days.length; i += 5) {
    const batch = days.slice(i, i + 5);
    const batchResults = await Promise.all(batch.map(day => windsorFetch(day, day, liFields)));
    liRows.push(...batchResults.flat());
    if (i + 5 < days.length) await sleep(300);
  }
  let liDemosFiltered = 0;
  for (const row of liRows) {
    if (!/linkedin/.test((row.datasource || '').toLowerCase())) continue;
    const convName = (row.conversion_name || '').toLowerCase();
    if (convName.includes('demo request')) {
      liDemosFiltered += row.externalwebsiteconversions || 0;
    }
  }
  console.log(`  🔍 LinkedIn demos [${dateFrom}→${dateTo}]: raw=${result.linkedin.demos} → filtered=${liDemosFiltered} (conversion_name contains "demo request")`);
  result.linkedin.demos = liDemosFiltered;

  result.meta.ctrAvg = result.meta.ctr.length > 0
    ? result.meta.ctr.reduce((a, b) => a + b, 0) / result.meta.ctr.length
    : null;

  console.log(`  INFO spend split — Google Search: $${result.google.spend.toFixed(2)}  YouTube: $${result.youtube.spend.toFixed(2)}  (${dateFrom} → ${dateTo})`);

  return result;
}

// ── Windsor: GA4 ─────────────────────────────────────────────────────────────
async function fetchGA4(dateFrom, dateTo) {
  const yest = new Date(); yest.setDate(yest.getDate() - 1);
  const yesterdayStr = yest.toISOString().slice(0, 10);
  if (dateTo > yesterdayStr) dateTo = yesterdayStr;
  if (dateFrom > dateTo) return { users: 0, demoButtonClicks: 0, meetingBooked: 0 };
  const fields = 'datasource,users,sessions,conversions_click_schedule_demo_button,conversions_hubspot_meeting_booked';
  const data   = await windsorFetch(dateFrom, dateTo, fields, '&connectors=googleanalytics4');
  return data
    .filter(r => r.datasource === 'googleanalytics4')
    .reduce((acc, row) => {
      acc.users            += row.users || 0;
      acc.demoButtonClicks += row.conversions_click_schedule_demo_button || 0;
      acc.meetingBooked    += row.conversions_hubspot_meeting_booked || 0;
      return acc;
    }, { users: 0, demoButtonClicks: 0, meetingBooked: 0 });
}

// ── Windsor: GA4 source breakdown ────────────────────────────────────────────
// Returns array of { label, meetingBooked, demoButtonClicks, users }
// sorted by meetingBooked desc. Uses sessionDefaultChannelGrouping dimension.
async function fetchGA4Sources(dateFrom, dateTo) {
  const yest = new Date(); yest.setDate(yest.getDate() - 1);
  const yesterdayStr = yest.toISOString().slice(0, 10);
  if (dateTo > yesterdayStr) dateTo = yesterdayStr;
  if (dateFrom > dateTo) return [];
  const fields = 'datasource,sessionDefaultChannelGrouping,session_source,conversions_hubspot_meeting_booked,conversions_click_schedule_demo_button,users';
  let rows;
  try {
    rows = await windsorFetch(dateFrom, dateTo, fields, '&connectors=googleanalytics4');
  } catch(e) {
    console.warn('  WARN fetchGA4Sources failed:', e.message);
    return [];
  }
  const map = {};
  for (const r of rows) {
    if (r.datasource !== 'googleanalytics4') continue;
    const raw   = (r.sessionDefaultChannelGrouping || r.session_source || 'Unknown').trim();
    const label = (!raw || raw === '(none)') ? 'Direct' : raw.replace(/\b\w/g, l => l.toUpperCase());
    if (!map[label]) map[label] = { label, meetingBooked: 0, demoButtonClicks: 0, users: 0 };
    map[label].meetingBooked    += r.conversions_hubspot_meeting_booked     || 0;
    map[label].demoButtonClicks += r.conversions_click_schedule_demo_button || 0;
    map[label].users            += r.users || 0;
  }
  return Object.values(map).sort((a, b) => b.meetingBooked - a.meetingBooked);
}

// ── HubSpot helpers ───────────────────────────────────────────────────────────
async function hsSearch(objectType, body) {
  const sleep = ms => new Promise(r => setTimeout(r, ms));
  let all = [], after;
  while (true) {
    const payload = { ...body, limit: 100, ...(after ? { after } : {}) };
    let json, attempt = 0;
    // Retry loop: up to 5 attempts with exponential backoff on rate limit errors
    while (true) {
      let res;
      try {
        res = await fetch(`https://api.hubapi.com/crm/v3/objects/${objectType}/search`, {
          method: 'POST',
          headers: { 'Authorization': `Bearer ${HUBSPOT_TOKEN}`, 'Content-Type': 'application/json' },
          body: JSON.stringify(payload)
        });
      } catch (networkErr) {
        console.error(`  ❌  HubSpot network error [${objectType}] attempt ${attempt}:`, networkErr.message);
        if (attempt < 4) { await sleep([1000,2000,4000,8000,15000][attempt]); attempt++; continue; }
        break;
      }
      json = await res.json();
      const isRateLimit = res.status === 429
        || (json.message && /secondly|rate.limit|too many/i.test(json.message));
      if (isRateLimit && attempt < 5) {
        const wait = [1000, 2000, 4000, 8000, 15000][attempt];
        console.warn(`  ⏳  HubSpot rate limit [${objectType}] — retrying in ${wait/1000}s (attempt ${attempt+1}/5)...`);
        await sleep(wait);
        attempt++;
        continue;
      }
      break;
    }
    if (json.status === 'error' || json.message) {
      console.error(`  ❌  HubSpot error [${objectType}]:`, json.message || json.status, '| filter:', JSON.stringify(payload.filterGroups?.[0]?.filters?.map(f => f.propertyName)));
      break;
    }
    if (!json.results || json.results.length === 0) break;
    all = all.concat(json.results);
    // Pace requests to stay under HubSpot's secondly limit (4 req/sec for search)
    if (json.paging?.next?.after) { after = json.paging.next.after; await sleep(400); } else { break; }
  }
  return all;
}

function toMs(dateStr, endOfDay = false) {
  // Use Eastern Time boundaries (process.env.TZ = 'America/New_York')
  // No 'Z' suffix = JS parses in local TZ (which is ET due to TZ env var)
  return String(new Date(dateStr + (endOfDay ? 'T23:59:59.999' : 'T00:00:00.000')).getTime());
}

// ── UTM → Channel mapping ─────────────────────────────────────────────────────
function mapChannel(utmSource, utmMedium) {
  const src = (utmSource || '').toLowerCase().trim();
  const med = (utmMedium || '').toLowerCase().trim();
  if (['fb', 'ig', 'facebook', 'instagram', 'meta'].includes(src)) return 'meta';
  if (src === 'google' && (med === 'cpc' || med === 'paid')) return 'google';
  if (src === 'linkedin') return 'linkedin';
  if (['tiktok', 'tik_tok', 'tt', 'tiktok_ads'].includes(src)) return 'tiktok';
  if (src === 'youtube') return 'youtube';
  return null; // unattributed
}

// ── HubSpot: fetch all data for the widest window, slice per sub-window ────────
// Date dimensions:
// ── HubSpot Owners lookup ─────────────────────────────────────────────────────
async function fetchHubSpotOwners() {
  const owners = {};
  let after = '';
  for (let page = 0; page < 10; page++) {
    const url = `https://api.hubapi.com/crm/v3/owners?limit=100${after ? '&after=' + after : ''}`;
    const res = await fetch(url, { headers: { Authorization: `Bearer ${HUBSPOT_TOKEN}` } });
    if (!res.ok) { console.warn('  WARN owners fetch:', res.status); break; }
    const data = await res.json();
    for (const o of (data.results || [])) {
      const name = [o.firstName, o.lastName].filter(Boolean).join(' ') || o.email || o.id;
      owners[o.id] = name;
    }
    if (data.paging?.next?.after) { after = data.paging.next.after; } else break;
  }
  console.log(`  INFO owners fetched: ${Object.keys(owners).length}`);
  return owners;
}

//   demosBooked  = contacts with date_demo_booked in window
//   demosToOccur = deals with date_demo_booked in window
//   demosHappened, dealsWon, dq breakdown = deals with date_demo_booked in window (status fields)
async function fetchAllHubSpotData(windows) {
  // Compute widest date range across ALL windows (not just mtd)
  // This is critical when called with prevWindows where yesterday/rolling7 may be
  // in a different month than mtd (e.g., prev-yesterday is Feb 25 but prev-mtd is Jan).
  let earliest = null, latest = null;
  for (const win of Object.values(windows)) {
    if (!earliest || win.from < earliest) earliest = win.from;
    if (!latest   || win.to   > latest)   latest   = win.to;
  }
  const gteMs = toMs(earliest);
  const lteMs = toMs(latest, true);
  const sleep = ms => new Promise(r => setTimeout(r, ms));

  // ── 1. Fetch contacts with date_demo_booked in range ────────────────────────
  // date_demo_booked is the date the demo booking was made.
  // Query and slice both use date_demo_booked.
  const allBookedContacts = await hsSearch('contacts', {
    filterGroups: [{ filters: [
      { propertyName: 'date_demo_booked', operator: 'GTE', value: gteMs },
      { propertyName: 'date_demo_booked', operator: 'LTE', value: lteMs },
    ]}],
    properties: ['date_demo_booked']
  });
  console.log('  INFO allBookedContacts: ' + allBookedContacts.length + ' | range: ' + earliest + ' to ' + latest);
  await sleep(1500);

  // ── 3. Fetch all deals with date_demo_booked in range ────────────────────────
  // date_demo_booked is a DATE-type property: must use YYYY-MM-DD strings, not ms.
  // Two filterGroups (OR): deals with date_demo_booked in range,
  // OR No Show/No Showed deals by hs_createdate (those often lack date_demo_booked)
  const allDeals = await hsSearch('deals', {
    filterGroups: [
      { filters: [
        { propertyName: 'date_demo_booked', operator: 'GTE', value: gteMs },
        { propertyName: 'date_demo_booked', operator: 'LTE', value: lteMs },
      ]},
      { filters: [
        { propertyName: 'demo_given__status', operator: 'IN', values: ['No Show', 'No Showed'] },
        { propertyName: 'hs_createdate',      operator: 'GTE', value: String(gteMs) },
        { propertyName: 'hs_createdate',      operator: 'LTE', value: String(lteMs) },
      ]},
    ],
    properties: ['date_demo_booked', 'demo_given_date', 'demo_given__status', 'dealstage', 'amount', 'closedate', 'hs_createdate', 'hubspot_owner_id', 'utm_source', 'utm_medium']
  });
  // Dedup in case a deal matched both filterGroups (has date_demo_booked AND is No Show)
  const allDealsMap = new Map(allDeals.map(d => [d.id, d]));
  const allDealsDeduped = [...allDealsMap.values()];
  console.log('  INFO allDeals: ' + allDealsDeduped.length + ' (raw: ' + allDeals.length + ') | range: ' + earliest + ' to ' + latest);
  await sleep(1500);

  // ── 4. Fetch closed won deals by closedate for MRR ────────────────────────
  const allClosedWon = await hsSearch('deals', {
    filterGroups: [{ filters: [
      { propertyName: 'dealstage', operator: 'EQ',  value: 'closedwon' },
      { propertyName: 'closedate', operator: 'GTE', value: gteMs },
      { propertyName: 'closedate', operator: 'LTE', value: lteMs },
    ]}],
    properties: ['amount', 'closedate', 'hs_createdate', 'utm_source', 'utm_medium']
  });

  // ── Fetch owner names ─────────────────────────────────────────────────────
  const ownerNames = await fetchHubSpotOwners();

  // ── Slice per window ──────────────────────────────────────────────────────
  const result = {};
  for (const [key, win] of Object.entries(windows)) {
    const winFrom = new Date(win.from + 'T00:00:00.000').getTime();
    const winTo   = new Date(win.to   + 'T23:59:59.999').getTime();

    function inWin(ms) { return ms >= winFrom && ms <= winTo; }
    function dateMs(str) {
      if (!str) return NaN;
      if (/^\d+$/.test(str)) return parseInt(str);
      return new Date(str + 'T00:00:00.000').getTime();
    }
    function isoMs(str)  { return str ? new Date(str).getTime() : NaN; }

    // Demos Booked: contacts with date_demo_booked falling in window
    const contactsBooked = allBookedContacts.filter(c =>
      inWin(dateMs(c.properties?.date_demo_booked))
    );

    // All pipeline metrics: deals filtered by date_demo_booked
    // For No Show / No Showed deals that lack date_demo_booked, fall back to hs_createdate
    const deals = allDealsDeduped.filter(d => {
      const bookedMs  = dateMs(d.properties?.date_demo_booked);
      const createdMs = d.properties?.hs_createdate ? parseInt(d.properties.hs_createdate) : null;
      const status    = (d.properties?.demo_given__status || '').trim();
      const noBookedDate = !d.properties?.date_demo_booked;
      if (noBookedDate && (status === 'No Show' || status === 'No Showed')) {
        return createdMs !== null && inWin(createdMs);
      }
      return inWin(bookedMs);
    });

    // Count each metric from that same deal set
    let demosToOccur = deals.length;
    let demosHappened = 0, dealsWon = 0;
    let notQualAfterDemo = 0, disqualifiedBeforeDemo = 0, tooEarly = 0;
    let rescheduled = 0, canceled = 0, blankStatus = 0;
    let demoGivenCount = 0, notQualAfterDemoCount = 0;

    for (const deal of deals) {
      const rawStatus = (deal.properties?.demo_given__status || '').trim();
      const stage     = (deal.properties?.dealstage || '').toLowerCase();

      if (stage === 'closedwon') dealsWon++;

      // Exact matches against real HubSpot stored values
      if (rawStatus === 'Demo Given' || rawStatus === 'Demo Given at Rescheduled time') {
        // Demo Given (both scheduled and rescheduled count as demo happened)
        demosHappened++;
        demoGivenCount++;
      } else if (rawStatus === 'Demo Given, Qualified Company, too early') {
        // Too Early
        tooEarly++;
        demosHappened++;
        demoGivenCount++;
      } else if (rawStatus === 'Disqualified, Meeting Cancelled') {
        // Disqualified Before Demo
        disqualifiedBeforeDemo++;
      } else if (rawStatus === 'Not Qualified after the demo') {
        // Not Qualified After Demo
        notQualAfterDemo++;
        notQualAfterDemoCount++;
        demosHappened++;
        demoGivenCount++;
      } else if (rawStatus === 'No Show') {
        // Rescheduled (HubSpot label: "No Show / Rescheduled meeting")
        rescheduled++;
      } else if (rawStatus === 'No Showed') {
        // Canceled (HubSpot label: "No Show / Cancelled")
        canceled++;
      } else {
        // Blank or unrecognized
        blankStatus++;
      }
    }


    // MRR + closedDeals from closed won by closedate (separate window)
    const closedWon = allClosedWon.filter(d => inWin(isoMs(d.properties?.closedate)));
    const closedDeals = closedWon.length;

    const denom = demoGivenCount - notQualAfterDemoCount;
    const pctDemosWon = denom > 0 ? ((closedDeals / denom) * 100).toFixed(1) + '%' : 'N/A';

    // Avg deal cycle time: closedate minus createdate for closed-won deals
    const cycleDays = closedWon
      .map(d => {
        const close = isoMs(d.properties?.closedate);
        const create = isoMs(d.properties?.hs_createdate);
        if (isNaN(close) || isNaN(create) || close <= create) return null;
        return (close - create) / (1000 * 60 * 60 * 24);
      })
      .filter(v => v !== null);
    const avgDealCycleDays = cycleDays.length > 0
      ? Math.round(cycleDays.reduce((s, v) => s + v, 0) / cycleDays.length)
      : null;

    // ── Owner breakdown: group deals by hubspot_owner_id ──────────────────
    const ownerMap = {};
    for (const deal of deals) {
      const ownerId = deal.properties?.hubspot_owner_id || 'unassigned';
      const name = ownerId === 'unassigned' ? 'Unassigned' : (ownerNames[ownerId] || `Owner ${ownerId}`);
      if (!ownerMap[ownerId]) {
        ownerMap[ownerId] = { name, demoGiven: 0, tooEarly: 0, notQual: 0, disqBefore: 0, rescheduled: 0, canceled: 0, blank: 0, closedWon: 0 };
      }
      const o = ownerMap[ownerId];
      const rawStatus = (deal.properties?.demo_given__status || '').trim();
      const stage = (deal.properties?.dealstage || '').toLowerCase();
      if (rawStatus === 'Demo Given' || rawStatus === 'Demo Given at Rescheduled time') {
        o.demoGiven++;
        if (stage === 'closedwon') o.closedWon++;
      } else if (rawStatus === 'Demo Given, Qualified Company, too early') { o.tooEarly++;
      } else if (rawStatus === 'Not Qualified after the demo') { o.notQual++;
      } else if (rawStatus === 'Disqualified, Meeting Cancelled') { o.disqBefore++;
      } else if (rawStatus === 'No Show') { o.rescheduled++;
      } else if (rawStatus === 'No Showed') { o.canceled++;
      } else { o.blank++; }
    }
    const ownerBreakdown = Object.values(ownerMap)
      .sort((a, b) => (b.demoGiven + b.tooEarly + b.notQual + b.disqBefore + b.rescheduled + b.canceled + b.blank) - (a.demoGiven + a.tooEarly + a.notQual + a.disqBefore + a.rescheduled + a.canceled + a.blank));

    // ── Day-of-week breakdown ───────────────────────────────────────────────
    const DOW_NAMES = ['Sun','Mon','Tue','Wed','Thu','Fri','Sat'];
    const dowMap = DOW_NAMES.map(d => ({ day: d, demoGiven: 0, tooEarly: 0, notQual: 0, disqBefore: 0, rescheduled: 0, canceled: 0, blank: 0 }));
    for (const deal of deals) {
      const bookedStr = deal.properties?.date_demo_booked;
      if (!bookedStr) continue;
      const dayIdx = new Date(bookedStr + 'T12:00:00').getDay(); // 0=Sun
      const rawStatus = (deal.properties?.demo_given__status || '').trim();
      if (rawStatus === 'Demo Given' || rawStatus === 'Demo Given at Rescheduled time') { dowMap[dayIdx].demoGiven++;
      } else if (rawStatus === 'Demo Given, Qualified Company, too early') { dowMap[dayIdx].tooEarly++;
      } else if (rawStatus === 'Not Qualified after the demo') { dowMap[dayIdx].notQual++;
      } else if (rawStatus === 'Disqualified, Meeting Cancelled') { dowMap[dayIdx].disqBefore++;
      } else if (rawStatus === 'No Show') { dowMap[dayIdx].rescheduled++;
      } else if (rawStatus === 'No Showed') { dowMap[dayIdx].canceled++;
      } else { dowMap[dayIdx].blank++; }
    }

    // ── Channel attribution: group deals by UTM source → channel ────────────
    const chAttr = {};
    const CH_KEYS = ['meta','linkedin','google','tiktok','youtube'];
    CH_KEYS.forEach(k => { chAttr[k] = { qualified: 0, tooEarly: 0, notQual: 0, disqBefore: 0, noShow: 0, canceled: 0, blank: 0 }; });
    for (const deal of deals) {
      const ch = mapChannel(deal.properties?.utm_source, deal.properties?.utm_medium);
      if (!ch || !chAttr[ch]) continue;
      const rawStatus = (deal.properties?.demo_given__status || '').trim();
      if (rawStatus === 'Demo Given' || rawStatus === 'Demo Given at Rescheduled time') { chAttr[ch].qualified++;
      } else if (rawStatus === 'Demo Given, Qualified Company, too early') { chAttr[ch].tooEarly++;
      } else if (rawStatus === 'Not Qualified after the demo') { chAttr[ch].notQual++;
      } else if (rawStatus === 'Disqualified, Meeting Cancelled') { chAttr[ch].disqBefore++;
      } else if (rawStatus === 'No Show') { chAttr[ch].noShow++;
      } else if (rawStatus === 'No Showed') { chAttr[ch].canceled++;
      } else { chAttr[ch].blank++; }
    }

    // ── Channel closed won / MRR from closedWon deals ───────────────────────
    const chWon = {};
    CH_KEYS.forEach(k => { chWon[k] = { closedWon: 0, mrr: 0 }; });
    for (const deal of closedWon) {
      const ch = mapChannel(deal.properties?.utm_source, deal.properties?.utm_medium);
      if (!ch || !chWon[ch]) continue;
      chWon[ch].closedWon++;
      chWon[ch].mrr += parseFloat(deal.properties?.amount) || 0;
    }

    // Merge into channelAttribution
    const channelAttribution = {};
    CH_KEYS.forEach(k => {
      channelAttribution[k] = { ...chAttr[k], ...chWon[k] };
    });

    result[key] = {
      demosBooked:           contactsBooked.length,
      demosToOccur,
      demosHappened,
      dealsWon,
      pctDemosWon,
      notQualAfterDemo,
      disqualifiedBeforeDemo,
      tooEarly,
      rescheduled,
      canceled,
      blankStatus,
      closedDeals,
      avgDealCycleDays,
      ownerBreakdown,
      channelAttribution,
      dowBreakdown: dowMap,
      newMRR: closedWon.reduce((s, d) => s + (parseFloat(d.properties?.amount) || 0), 0),
    };
  }

  return result;
}


// ── Demo Cohort Analysis ──────────────────────────────────────────────────────
// Fetches deals by date_demo_booked for last 3 completed months + current month.
// Groups into monthly cohorts to track: how many qualified demos happened in month X,
// and what has happened to those deals since (closed, still open, etc.)
async function fetchDemoCohorts() {
  const now = new Date();
  // Go back 3 completed months + current month
  const cohortStart = new Date(now.getFullYear(), now.getMonth() - 3, 1);
  const cohortEnd   = new Date(now.getFullYear(), now.getMonth() + 1, 0);

  const gteMs = toMs(toDateStr(cohortStart));
  const lteMs = toMs(toDateStr(cohortEnd), true);

  console.log(`  Cohort query: date_demo_booked ${toDateStr(cohortStart)} → ${toDateStr(cohortEnd)}`);

  const deals = await hsSearch('deals', {
    filterGroups: [{ filters: [
      { propertyName: 'date_demo_booked', operator: 'GTE', value: gteMs },
      { propertyName: 'date_demo_booked', operator: 'LTE', value: lteMs },
    ]}],
    properties: ['date_demo_booked', 'demo_given__status', 'dealstage', 'closedate', 'hs_createdate', 'amount']
  });

  console.log(`  Cohort deals fetched: ${deals.length}`);

  // Build month buckets
  const buckets = {};
  for (let m = new Date(cohortStart); m <= now; m = new Date(m.getFullYear(), m.getMonth() + 1, 1)) {
    const ym = toDateStr(m).slice(0, 7);
    buckets[ym] = {
      month: ym,
      label: m.toLocaleDateString('en-US', { month: 'long', year: 'numeric' }),
      demosGiven: 0,
      closedWon: 0,
      closedLost: 0,
      stillOpen: 0,
      notQualified: 0,
      tooEarly: 0,
      mrr: 0,
      cycleDays: [],
    };
  }

  // Qualified demo statuses only — excludes Too Early and Not Qualified
  const QUALIFIED_DEMO = ['Demo Given', 'Demo Given at Rescheduled time'];
  // All statuses where a demo actually happened (for bucket assignment)
  const ALL_DEMO_HAPPENED = ['Demo Given', 'Demo Given at Rescheduled time', 'Demo Given, Qualified Company, too early', 'Not Qualified after the demo'];

  for (const deal of deals) {
    const bookedDate = deal.properties?.date_demo_booked;
    if (!bookedDate) continue;

    const status = (deal.properties?.demo_given__status || '').trim();
    if (!ALL_DEMO_HAPPENED.includes(status)) continue;

    const ym = bookedDate.slice(0, 7);
    if (!buckets[ym]) continue;

    const b = buckets[ym];
    const stage = (deal.properties?.dealstage || '').toLowerCase();

    // Too Early and Not Qualified go into their own columns only
    if (status === 'Demo Given, Qualified Company, too early') {
      b.tooEarly++;
      continue;
    }
    if (status === 'Not Qualified after the demo') {
      b.notQualified++;
      continue;
    }

    // Qualified demos only from here
    b.demosGiven++;

    if (stage === 'closedwon') {
      b.closedWon++;
      b.mrr += parseFloat(deal.properties?.amount) || 0;

      const closeMs  = deal.properties?.closedate    ? new Date(deal.properties.closedate).getTime()    : NaN;
      const createMs = deal.properties?.hs_createdate ? new Date(deal.properties.hs_createdate).getTime() : NaN;
      if (!isNaN(closeMs) && !isNaN(createMs) && closeMs > createMs) {
        b.cycleDays.push((closeMs - createMs) / (1000 * 60 * 60 * 24));
      }
    } else if (stage === 'closedlost') {
      b.closedLost++;
    } else {
      b.stillOpen++;
    }
  }

  const cohorts = Object.values(buckets).map(b => ({
    month: b.month,
    label: b.label,
    demosGiven: b.demosGiven,
    closedWon: b.closedWon,
    closedLost: b.closedLost,
    stillOpen: b.stillOpen,
    notQualified: b.notQualified,
    tooEarly: b.tooEarly,
    closeRate: b.demosGiven > 0 ? ((b.closedWon / b.demosGiven) * 100).toFixed(1) : null,
    avgCycleDays: b.cycleDays.length > 0 ? Math.round(b.cycleDays.reduce((s, v) => s + v, 0) / b.cycleDays.length) : null,
    mrr: b.mrr,
  }));

  console.log(`  Cohorts built: ${cohorts.map(c => `${c.label}: ${c.demosGiven} qual demos, ${c.closedWon} won`).join(' | ')}`);
  return cohorts;
}

// ── Formatting ────────────────────────────────────────────────────────────────
function fmt$(n)   { return '$' + Number(n).toFixed(2).replace(/\B(?=(\d{3})+(?!\d))/g, ','); }
function fmtN(n)   { return Number(n).toLocaleString(); }
function fmtP(a,b) { return b > 0 ? ((a / b) * 100).toFixed(1) + '%' : 'N/A'; }
function fmtPct(n) { return (n * 100).toFixed(2) + '%'; }
function cpd(spend, demos) { return demos > 0 ? fmt$(spend / demos) : 'N/A'; }

// ── Executive Summary ─────────────────────────────────────────────────────────
function buildExecSummary(label, channels, ga4, hs) {
  const { demosBooked, newMRR } = hs;

  // All Paid excl. LinkedIn AND excl. YouTube (YouTube is brand/awareness, not direct response)
  const totalSpendExLI = channels.meta.spend + channels.tiktok.spend + channels.google.spend;
  const totalDemosExLI = channels.meta.demos + channels.tiktok.demos + channels.google.demos;

  const { users, demoButtonClicks, meetingBooked } = ga4;

  const lines = [];
  lines.push(`\n${'★'.repeat(60)}`);
  lines.push(`  ⚡  EXECUTIVE SUMMARY — ${label.toUpperCase()}`);
  lines.push(`${'★'.repeat(60)}`);

  lines.push('\n── WEBSITE PERFORMANCE ──────────────────────────────────');
  lines.push(`  Website Visitors           ${String(fmtN(Math.round(users))).padStart(10)}`);
  lines.push(`  Demo Button Clicks         ${String(fmtN(Math.round(demoButtonClicks))).padStart(10)}${demoButtonClicks === 0 ? '  ⚠️  (Windsor GA4 key events not configured)' : ''}`);
  lines.push(`  % Who Click Schedule Demo  ${String(fmtP(demoButtonClicks, users)).padStart(10)}`);
  lines.push(`  % of Clicks Who Book Demo  ${String(fmtP(meetingBooked, demoButtonClicks)).padStart(10)}`);
  lines.push(`  Website CVR                ${String(fmtP(meetingBooked, users)).padStart(10)}`);

  lines.push('\n── DEMOS BOOKED (HubSpot) ───────────────────────────────');
  lines.push(`  Total Demos Booked         ${String(demosBooked).padStart(10)}`);

  lines.push('\n── META PERFORMANCE ─────────────────────────────────────');
  lines.push(`  Meta CTR                   ${channels.meta.ctrAvg !== null ? String(fmtPct(channels.meta.ctrAvg)).padStart(10) : '       N/A'}`);

  lines.push('\n── COST PER DEMO BY CHANNEL ─────────────────────────────');
  lines.push(`  All Paid (excl. LI + YT)   ${String(cpd(totalSpendExLI, totalDemosExLI)).padStart(10)}`);
  lines.push(`  Meta                       ${String(cpd(channels.meta.spend, channels.meta.demos)).padStart(10)}${channels.meta.demos === 0 ? '  ⚠️  (conversions_submit_application_total not in Windsor)' : ''}`);
  lines.push(`  Google Ads                 ${String(cpd(channels.google.spend, channels.google.demos)).padStart(10)}`);
  lines.push(`  TikTok                     ${String(cpd(channels.tiktok.spend, channels.tiktok.demos)).padStart(10)}`);
  lines.push(`  YouTube                    ${String(cpd(channels.youtube.spend, channels.youtube.demos)).padStart(10)}`);
  lines.push(`  LinkedIn                   ${String(cpd(channels.linkedin.spend, channels.linkedin.demos)).padStart(10)}${channels.linkedin.demos === 0 ? '  ⚠️  (externalwebsiteconversions not in Windsor)' : ''}`);

  lines.push('\n── REVENUE ──────────────────────────────────────────────');
  lines.push(`  New MRR (Closed Won)       ${String(fmt$(newMRR)).padStart(10)}`);

  return lines.join('\n');
}

// ── Slack Executive Summary (with +/- vs prior period) ───────────────────────
function buildSlackSummary(label, channels, ga4, hs, prevChannels, prevGa4, prevHs, prevLabel, dateFrom, dateTo) {

  // Format date range for header
  const fmtDate = s => {
    if (!s) return '';
    const d = new Date(s + 'T00:00:00Z');
    return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', timeZone: 'UTC' });
  };
  const dateRange = dateFrom && dateTo
    ? dateFrom === dateTo
      ? fmtDate(dateFrom)
      : `${fmtDate(dateFrom)} – ${fmtDate(dateTo)}`
    : '';

  // Helpers
  const n  = v => (v == null || isNaN(v)) ? 0 : v;
  const pct = (a, b) => b ? ((a - b) / Math.abs(b) * 100) : null;
  const arrow = v => v == null ? '' : v >= 0 ? '▲' : '▼';

  function delta(curr, prev, fmt) {
    const d = pct(n(curr), n(prev));
    if (d == null) return '';
    const sign  = d >= 0 ? '+' : '';
    const emoji = d >= 0 ? '📈' : '📉';
    return ` ${emoji} ${sign}${d.toFixed(0)}% vs ${prevLabel}`;
  }

  // For CPD lower is better — flip the emoji
  function deltaCPD(curr, prev) {
    if (!prev || !curr || prev === '—' || curr === '—') return '';
    const cNum = parseFloat(String(curr).replace(/[$,]/g, ''));
    const pNum = parseFloat(String(prev).replace(/[$,]/g, ''));
    if (!pNum) return '';
    const d = pct(cNum, pNum);
    const sign  = d >= 0 ? '+' : '';
    const emoji = d >= 0 ? '📉' : '📈'; // higher CPD = bad
    return ` ${emoji} ${sign}${d.toFixed(0)}% vs ${prevLabel}`;
  }

  const totalSpendExLI     = n(channels.meta.spend)  + n(channels.tiktok.spend)  + n(channels.google.spend);
  const totalDemosExLI     = n(channels.meta.demos)  + n(channels.tiktok.demos)  + n(channels.google.demos);
  const pTotalSpendExLI    = n(prevChannels.meta.spend) + n(prevChannels.tiktok.spend) + n(prevChannels.google.spend);
  const pTotalDemosExLI    = n(prevChannels.meta.demos) + n(prevChannels.tiktok.demos) + n(prevChannels.google.demos);

  const { users, demoButtonClicks, meetingBooked } = ga4;
  const { users: pUsers, demoButtonClicks: pDBC, meetingBooked: pMB } = prevGa4;
  const { demosBooked, newMRR } = hs;
  const { demosBooked: pDB, newMRR: pMRR } = prevHs;

  const cpdVal  = (s, d) => d > 0 ? `$${Math.round(s / d).toLocaleString()}` : '—';

  const lines = [];
  lines.push(`*⚡ EXECUTIVE SUMMARY — ${label.toUpperCase()}${dateRange ? `  (${dateRange})` : ''}*`);
  lines.push('```');

  lines.push('WEBSITE PERFORMANCE');
  lines.push(`  Visitors            ${fmtN(Math.round(n(users))).padStart(8)}${delta(users, pUsers)}`);
  lines.push(`  Demo Button Clicks  ${fmtN(Math.round(n(demoButtonClicks))).padStart(8)}${delta(demoButtonClicks, pDBC)}`);
  lines.push(`  % Click → Schedule  ${fmtP(demoButtonClicks, users).padStart(8)}${delta(demoButtonClicks/n(users), pDBC/n(pUsers))}`);
  lines.push(`  % Click → Books     ${fmtP(meetingBooked, demoButtonClicks).padStart(8)}${delta(meetingBooked/n(demoButtonClicks), pMB/n(pDBC))}`);
  lines.push(`  Website CVR         ${fmtP(meetingBooked, users).padStart(8)}${delta(meetingBooked/n(users), pMB/n(pUsers))}`);

  lines.push('');
  lines.push('DEMOS BOOKED');
  lines.push(`  Total               ${String(n(demosBooked)).padStart(8)}${delta(demosBooked, pDB)}`);

  lines.push('');
  lines.push('META');
  const metaCTR  = channels.meta.ctrAvg  != null ? fmtPct(channels.meta.ctrAvg)  : '—';
  const pMetaCTR = prevChannels.meta.ctrAvg != null ? prevChannels.meta.ctrAvg : null;
  lines.push(`  CTR                 ${metaCTR.padStart(8)}${delta(channels.meta.ctrAvg, pMetaCTR)}`);

  lines.push('');
  lines.push('COST PER DEMO');
  lines.push(`  All Paid (ex LI+YT) ${cpdVal(totalSpendExLI, totalDemosExLI).padStart(8)}${deltaCPD(cpdVal(totalSpendExLI, totalDemosExLI), cpdVal(pTotalSpendExLI, pTotalDemosExLI))}`);
  lines.push(`  Meta                ${cpdVal(channels.meta.spend, channels.meta.demos).padStart(8)}${deltaCPD(cpdVal(channels.meta.spend, channels.meta.demos), cpdVal(prevChannels.meta.spend, prevChannels.meta.demos))}`);
  lines.push(`  Google Ads          ${cpdVal(channels.google.spend, channels.google.demos).padStart(8)}${deltaCPD(cpdVal(channels.google.spend, channels.google.demos), cpdVal(prevChannels.google.spend, prevChannels.google.demos))}`);
  lines.push(`  TikTok              ${cpdVal(channels.tiktok.spend, channels.tiktok.demos).padStart(8)}${deltaCPD(cpdVal(channels.tiktok.spend, channels.tiktok.demos), cpdVal(prevChannels.tiktok.spend, prevChannels.tiktok.demos))}`);
  lines.push(`  YouTube             ${cpdVal(channels.youtube.spend, channels.youtube.demos).padStart(8)}${deltaCPD(cpdVal(channels.youtube.spend, channels.youtube.demos), cpdVal(prevChannels.youtube.spend, prevChannels.youtube.demos))}`);
  lines.push(`  LinkedIn            ${cpdVal(channels.linkedin.spend, channels.linkedin.demos).padStart(8)}${deltaCPD(cpdVal(channels.linkedin.spend, channels.linkedin.demos), cpdVal(prevChannels.linkedin.spend, prevChannels.linkedin.demos))}`);

  lines.push('');
  lines.push('REVENUE');
  lines.push(`  New MRR             ${fmt$(n(newMRR)).padStart(8)}${delta(newMRR, pMRR)}`);

  lines.push('```');
  return lines.join('\n');
}

// ── Detailed Section ──────────────────────────────────────────────────────────
function buildSection(label, channels, hs) {
  const { demosBooked, demosToOccur, demosHappened, dealsWon, pctDemosWon,
          notQualAfterDemo, disqualifiedBeforeDemo, tooEarly, rescheduled, canceled, blankStatus, newMRR } = hs;

  const paidChannels = [
    { lbl: '📘 Meta / Facebook', ...channels.meta },
    { lbl: '💼 LinkedIn',        ...channels.linkedin },
    { lbl: '🔍 Google Ads',      ...channels.google },
    { lbl: '🎵 TikTok',          ...channels.tiktok },
    { lbl: '▶️  YouTube',         ...channels.youtube },
  ];
  const totalSpend = paidChannels.reduce((s, c) => s + c.spend, 0);
  const drChannels = paidChannels.filter(c => c.lbl !== '💼 LinkedIn' && c.lbl !== '▶️  YouTube');
  const drSpend    = drChannels.reduce((s, c) => s + c.spend, 0);
  const drDemos    = drChannels.reduce((s, c) => s + c.demos, 0);

  const lines = [];
  lines.push('\n' + '═'.repeat(60));
  lines.push(`  📊  DETAILED BREAKDOWN — ${label.toUpperCase()}`);
  lines.push('═'.repeat(60));

  lines.push('\n── AD SPEND BY CHANNEL ──────────────────────────────────');
  for (const c of paidChannels.sort((a, b) => b.spend - a.spend)) {
    lines.push(`  ${c.lbl.padEnd(24)} ${fmt$(c.spend).padStart(10)}  (${fmtP(c.spend, totalSpend)})`);
    lines.push(`  ${''.padEnd(24)} Clicks: ${fmtN(c.clicks)}  |  Impressions: ${fmtN(c.impressions)}  |  Demos: ${Math.round(c.demos)}`);
  }
  lines.push(`  ${'TOTAL'.padEnd(24)} ${fmt$(totalSpend).padStart(10)}`);

  lines.push('\n── PAID MEDIA EFFICIENCY (excl. LinkedIn + YouTube) ─────');
  lines.push(`  Total Demos (paid)     ${String(Math.round(drDemos)).padStart(8)}`);
  lines.push(`  Cost Per Demo          ${cpd(drSpend, drDemos).padStart(8)}`);

  lines.push('\n── DEMO PIPELINE ────────────────────────────────────────');
  lines.push(`  Demos Booked           ${String(demosBooked).padStart(8)}  (contacts by date_demo_booked)`);
  lines.push(`  Demos to Occur         ${String(demosToOccur).padStart(8)}  (deals by date_demo_booked)`);
  lines.push(`  Demo Given             ${String(demosHappened).padStart(8)}  (Demo Given + Demo Given at Rescheduled time + Too Early + Not Qual After)`);
  lines.push(`  Deals Won              ${String(dealsWon).padStart(8)}  (deals by date_demo_booked, closedwon)`);
  lines.push(`  % Demos Won            ${String(pctDemosWon).padStart(8)}`);

  lines.push('\n── DISQUALIFICATION ─────────────────────────────────────');
  lines.push(`  Not Qual. After Demo   ${String(notQualAfterDemo).padStart(8)}`);
  lines.push(`  Disq. Before Demo      ${String(disqualifiedBeforeDemo).padStart(8)}`);
  lines.push(`  Too Early              ${String(tooEarly).padStart(8)}`);
  lines.push(`  Rescheduled            ${String(rescheduled).padStart(8)}`);
  lines.push(`  Canceled               ${String(canceled).padStart(8)}`);
  lines.push(`  Blank Status           ${String(blankStatus).padStart(8)}`);

  lines.push('\n── REVENUE ──────────────────────────────────────────────');
  lines.push(`  New MRR (Closed Won)   ${fmt$(newMRR).padStart(8)}`);

  return lines.join('\n');
}

// ── Dashboard ─────────────────────────────────────────────────────────────────
function buildDashboard(windowedChannels, hubspotData, prevWindowedChannels, prevHubspotData, winKeys, windows, prevWindows, txtFilename, ga4SourcesByWindow, ga4PrevSourcesByWindow, demoCohorts) {
  const generatedAt = new Date().toLocaleString('en-US', {
    timeZone: 'America/Los_Angeles', dateStyle: 'medium', timeStyle: 'short'
  });

  function buildWin(channels, ga4, hs, ga4Sources, ga4PrevSources) {
    const { demosBooked, demosToOccur, demosHappened, dealsWon, pctDemosWon,
            notQualAfterDemo, disqualifiedBeforeDemo, tooEarly, rescheduled, canceled, blankStatus, closedDeals, avgDealCycleDays, ownerBreakdown, channelAttribution, dowBreakdown, newMRR } = hs;
    const pipeline = { demosToOccur, demosHappened, dealsWon, pctDemosWon,
                       notQualAfterDemo, disqualifiedBeforeDemo, tooEarly,
                       rescheduled, canceled, blankStatus, closedDeals, avgDealCycleDays, ownerBreakdown: ownerBreakdown || [], dowBreakdown: dowBreakdown || [] };
    const metaCTR  = channels.meta.ctrAvg != null
      ? (channels.meta.ctrAvg * 100).toFixed(2) + '%' : null;
    return { channels, ga4, demosBooked, pipeline, channelAttribution: channelAttribution || {}, newMRR, metaCTR,
             ga4Sources: ga4Sources || [],
             ga4PrevSources: ga4PrevSources || [] };
  }

  const dashWindows = {};
  for (let i = 0; i < winKeys.length; i++) {
    const key = winKeys[i];
    const [channels, ga4]         = windowedChannels[i];
    const [prevChannels, prevGa4] = prevWindowedChannels[i];
    const ga4Src  = (ga4SourcesByWindow     || {})[key] || [];
    const ga4PSrc = (ga4PrevSourcesByWindow || {})[key] || [];
    dashWindows[key] = {
      ...buildWin(channels, ga4, hubspotData[key], ga4Src, ga4PSrc),
      label: windows[key].label,
      from: windows[key].from,
      to: windows[key].to,
      prev: buildWin(prevChannels, prevGa4, prevHubspotData[key], ga4PSrc, []),
      prevLabel: prevWindows[key].label,
      noPrev: key === 'ytd',
    };
  }

  const data = JSON.stringify({ generatedAt, filename: txtFilename, windows: dashWindows, demoCohorts: demoCohorts || [] })
    .replace(/<\/script>/gi, '<\/script>');
  let template = fs.readFileSync(__dirname + '/dashboard_template.html', 'utf8');

  // Inject custom range config: Cloudflare Worker URL and GitHub Pages base
  const workerUrl = process.env.WORKER_URL || '';
  const pagesBase = (GITHUB_OWNER && GITHUB_REPO)
    ? `https://${GITHUB_OWNER}.github.io/${GITHUB_REPO}`
    : '';
  template = template
    .replace("'__WORKER_URL__'", JSON.stringify(workerUrl))
    .replace("'__PAGES_BASE__'", JSON.stringify(pagesBase));

  return template.replace('"__DASHBOARD_DATA__"', data);
}

// ── Intelligence Engine ───────────────────────────────────────────────────────
// Analyzes all available data to surface alerts, weaknesses, opportunities, wins.
function buildIntelligence(allCurrentData, allPrevData, windows) {
  const findings = {
    alerts:        [],
    weaknesses:    [],
    opportunities: [],
    wins:          [],
  };

  function pct(a, b)         { return b > 0 ? (a / b) * 100 : null; }
  function chg(curr, prev)   { return prev > 0 ? ((curr - prev) / prev) * 100 : null; }
  function fmtD(n)           { return '$' + Number(n).toFixed(0).replace(/\B(?=(\d{3})+(?!\d))/g, ','); }
  function fmtPct(n, dec=1)  { return n !== null ? n.toFixed(dec) + '%' : 'N/A'; }
  function arrow(n)          { return n > 0 ? '\u2191' : '\u2193'; }
  function absDelta(n)       { return Math.abs(n).toFixed(1); }

  const win  = allCurrentData.mtd;
  const prev = allPrevData.mtd;
  const winLabel = windows.current.mtd.label;

  const { channels, ga4, hs }             = win;
  const { channels: pCh, ga4: pGa4, hs: pHs } = prev;

  // ── 1. CHANNEL CPD ANALYSIS ────────────────────────────────────────────────
  const chData = [
    { name: 'Meta',       spend: channels.meta.spend,     demos: channels.meta.demos,     pSpend: pCh.meta.spend,     pDemos: pCh.meta.demos },
    { name: 'TikTok',     spend: channels.tiktok.spend,   demos: channels.tiktok.demos,   pSpend: pCh.tiktok.spend,   pDemos: pCh.tiktok.demos },
    { name: 'Google Ads', spend: channels.google.spend,   demos: channels.google.demos,   pSpend: pCh.google.spend,   pDemos: pCh.google.demos },
    { name: 'LinkedIn',   spend: channels.linkedin.spend, demos: channels.linkedin.demos, pSpend: pCh.linkedin.spend, pDemos: pCh.linkedin.demos },
  ];

  const withCPD = chData.map(c => ({
    ...c,
    cpd:  c.demos  > 0 ? c.spend  / c.demos  : null,
    pCpd: c.pDemos > 0 ? c.pSpend / c.pDemos : null,
  }));

  // Best vs worst CPD gap (direct response only)
  const drChannels = withCPD.filter(c => c.name !== 'LinkedIn' && c.cpd !== null);
  if (drChannels.length >= 2) {
    const best  = drChannels.reduce((a, b) => a.cpd < b.cpd ? a : b);
    const worst = drChannels.reduce((a, b) => a.cpd > b.cpd ? a : b);
    const gap   = ((worst.cpd - best.cpd) / best.cpd) * 100;
    if (gap > 40) {
      const additionalDemos = Math.round(worst.spend * 0.2 / best.cpd);
      findings.opportunities.push(
        `CPD EFFICIENCY GAP — ${best.name} (${fmtD(best.cpd)}/demo) is ${gap.toFixed(0)}% cheaper than ${worst.name} (${fmtD(worst.cpd)}/demo).\n` +
        `    \u2192 Shifting 20% of ${worst.name} budget (${fmtD(worst.spend * 0.2)}) to ${best.name} would yield ~${additionalDemos} additional demos at current efficiency.`
      );
    }
  }

  // CPD trend per channel
  for (const c of withCPD) {
    if (c.name === 'LinkedIn') continue;
    if (c.cpd === null || c.pCpd === null) continue;
    const delta = chg(c.cpd, c.pCpd);
    if (delta !== null && delta > 25) {
      findings.weaknesses.push(
        `CPD SPIKE — ${c.name}: ${fmtD(c.pCpd)} \u2192 ${fmtD(c.cpd)}/demo (${arrow(delta)}${absDelta(delta)}% vs prior month).\n` +
        `    \u2192 Investigate creative fatigue, audience saturation, or bid strategy drift.`
      );
    }
    if (delta !== null && delta < -20) {
      findings.wins.push(
        `CPD IMPROVEMENT — ${c.name}: ${fmtD(c.pCpd)} \u2192 ${fmtD(c.cpd)}/demo (${arrow(delta)}${absDelta(delta)}%). Media efficiency gaining.`
      );
    }
    if (c.cpd > 500) {
      findings.alerts.push(
        `HIGH CPD ALERT — ${c.name} at ${fmtD(c.cpd)}/demo. Above $500 threshold. Review targeting and creative mix immediately.`
      );
    }
  }

  // LinkedIn CPD (separate — uses externalwebsiteconversions)
  const li = withCPD.find(c => c.name === 'LinkedIn');
  if (li && li.cpd !== null) {
    if (li.cpd > 600) {
      findings.alerts.push(
        `LINKEDIN CPD CRITICAL — ${fmtD(li.cpd)}/demo. Spending ${fmtD(li.spend)} for ${Math.round(li.demos)} demos.\n` +
        `    \u2192 Audit audience targeting, bid strategy, and creative format. Consider document ads and thought-leadership formats.`
      );
    } else if (li.cpd > 400) {
      findings.weaknesses.push(
        `LINKEDIN CPD HIGH — ${fmtD(li.cpd)}/demo. B2B SaaS benchmark is $200-350.\n` +
        `    \u2192 Test audience layering: job title + company size + seniority. Exclude small companies (<10 employees).`
      );
    }
    if (li.pCpd !== null) {
      const liDelta = chg(li.cpd, li.pCpd);
      if (liDelta !== null && liDelta > 20) {
        findings.weaknesses.push(
          `LINKEDIN EFFICIENCY DECLINING — CPD ${arrow(liDelta)}${absDelta(liDelta)}% (${fmtD(li.pCpd)} \u2192 ${fmtD(li.cpd)}).\n` +
          `    \u2192 Check for audience overlap across campaigns and creative frequency. Consider rotating audiences.`
        );
      }
    }
  }

  // ── 2. SPEND CONCENTRATION RISK ───────────────────────────────────────────
  const totalSpend = chData.reduce((s, c) => s + c.spend, 0);
  for (const c of chData) {
    const shareP = pct(c.spend, totalSpend);
    if (shareP !== null && shareP > 55) {
      findings.weaknesses.push(
        `CHANNEL CONCENTRATION — ${c.name} is ${fmtPct(shareP)} of total budget (${fmtD(c.spend)} of ${fmtD(totalSpend)}).\n` +
        `    \u2192 Single-channel dependency. A policy change, CPM spike, or account flag could collapse pipeline overnight. Diversify.`
      );
    }
  }

  // ── 3. FUNNEL ANALYSIS ─────────────────────────────────────────────────────
  const { users, demoButtonClicks, meetingBooked } = ga4;
  const clickRate  = pct(demoButtonClicks, users);
  const bookRate   = pct(meetingBooked, demoButtonClicks);
  const websiteCVR = pct(meetingBooked, users);

  if (clickRate !== null) {
    if (clickRate < 3.0) {
      findings.weaknesses.push(
        `LOW VISITOR-TO-CTA RATE — ${fmtPct(clickRate)} of ${Number(Math.round(users)).toLocaleString()} visitors click the demo button.\n` +
        `    \u2192 Above-the-fold headline, CTA placement, and page load speed need testing. Industry target: 5%+.`
      );
    } else if (clickRate > 6.0) {
      findings.wins.push(
        `STRONG CTA CLICK RATE — ${fmtPct(clickRate)} visitor-to-CTA rate. Page messaging is resonating with traffic quality.`
      );
    }
  }

  if (bookRate !== null) {
    if (bookRate < 5.0) {
      findings.alerts.push(
        `CRITICAL BOOKING DROP-OFF — Only ${fmtPct(bookRate)} of demo button clicks convert to a booked meeting.\n` +
        `    \u2192 Scheduling friction is destroying pipeline. Test: embed Calendly inline vs redirect, reduce form fields, show next 3 available slots immediately.`
      );
    } else if (bookRate < 15.0) {
      findings.weaknesses.push(
        `BOOKING FRICTION — ${fmtPct(bookRate)} click-to-book rate. ~${Math.round(demoButtonClicks * (1 - bookRate / 100))} clicks not converting.\n` +
        `    \u2192 A/B test the scheduling page: urgency copy, social proof above fold, one fewer required field.`
      );
    } else if (bookRate > 25.0) {
      findings.wins.push(
        `HIGH BOOKING CONVERSION — ${fmtPct(bookRate)} of CTA clicks convert to booked demos. Scheduling experience is working.`
      );
    }
  }

  const pWebsiteCVR = pct(pGa4.meetingBooked, pGa4.users);
  if (websiteCVR !== null && pWebsiteCVR !== null) {
    const cvrDelta = chg(websiteCVR, pWebsiteCVR);
    if (cvrDelta !== null && cvrDelta < -15) {
      findings.alerts.push(
        `WEBSITE CVR DECLINING — ${fmtPct(pWebsiteCVR, 2)} \u2192 ${fmtPct(websiteCVR, 2)} (${arrow(cvrDelta)}${absDelta(cvrDelta)}% vs prior period).\n` +
        `    \u2192 Could be: traffic quality degradation (new audiences), landing page regression, or scheduling tool issue. Segment by source.`
      );
    } else if (cvrDelta !== null && cvrDelta > 15) {
      findings.wins.push(
        `CVR IMPROVING — Website CVR up ${arrow(cvrDelta)}${absDelta(cvrDelta)}% (${fmtPct(pWebsiteCVR, 2)} \u2192 ${fmtPct(websiteCVR, 2)}). Landing page or traffic quality improving.`
      );
    }
  }

  // ── 4. PIPELINE HEALTH ─────────────────────────────────────────────────────
  const { demosBooked, demosToOccur, demosHappened, dealsWon, closedDeals,
          notQualAfterDemo, disqualifiedBeforeDemo, tooEarly, rescheduled, canceled, blankStatus } = hs;
  const { demosToOccur: pDemosToOccur, demosHappened: pDemosHappened,
          dealsWon: pDealsWon, closedDeals: pClosedDeals, disqualifiedBeforeDemo: pDisqBefore,
          notQualAfterDemo: pNotQualAfter } = pHs;

  // Show rate (booked \u2192 happened)
  const showRate = pct(demosHappened, demosToOccur);
  if (showRate !== null) {
    if (showRate < 60) {
      findings.alerts.push(
        `CRITICAL SHOW RATE — Only ${fmtPct(showRate)} of booked demos happen (${demosHappened}/${demosToOccur}). ${demosToOccur - demosHappened} no-shows/cancels.\n` +
        `    \u2192 Immediately add: 24hr + 1hr pre-demo SMS reminder, confirmation email with prep materials, AE personal outreach same day as booking.`
      );
    } else if (showRate < 78) {
      findings.weaknesses.push(
        `SHOW RATE BELOW TARGET — ${fmtPct(showRate)} of demos happen (${demosHappened}/${demosToOccur}). Target: 80%+.\n` +
        `    \u2192 Review reminder sequence timing. Add a 2hr pre-demo "getting ready" email with 3 questions to answer before the call.`
      );
    } else {
      findings.wins.push(
        `SOLID SHOW RATE — ${fmtPct(showRate)} of booked demos happen (${demosHappened}/${demosToOccur}). Reminder sequence is working.`
      );
    }
  }

  // Pre-demo disqualification
  const disqBeforeRate  = pct(disqualifiedBeforeDemo, demosToOccur);
  const pDisqBeforeRate = pct(pDisqBefore, pDemosToOccur);
  if (disqBeforeRate !== null) {
    if (disqBeforeRate > 25) {
      findings.alerts.push(
        `HIGH PRE-DEMO DISQ — ${fmtPct(disqBeforeRate)} of pipeline cancelled before demo (${disqualifiedBeforeDemo}/${demosToOccur}).\n` +
        `    \u2192 Add ICP filters to booking form: "How long has your brand been selling?" + "Monthly ad spend?" Disqualify pre-launch brands at scheduling stage, not after.`
      );
    } else if (disqBeforeRate > 12) {
      findings.weaknesses.push(
        `PRE-DEMO DISQ ELEVATED — ${disqualifiedBeforeDemo} demos cancelled pre-call (${fmtPct(disqBeforeRate)} of pipeline).\n` +
        `    \u2192 Add 1-2 qualifying questions to Calendly booking page. Review recent disq reasons for patterns.`
      );
    }
  }
  if (disqBeforeRate !== null && pDisqBeforeRate !== null) {
    const disqDelta = chg(disqBeforeRate, pDisqBeforeRate);
    if (disqDelta !== null && disqDelta > 30) {
      findings.weaknesses.push(
        `PRE-DEMO DISQ WORSENING — ${fmtPct(pDisqBeforeRate)} \u2192 ${fmtPct(disqBeforeRate)} (${arrow(disqDelta)}${absDelta(disqDelta)}% change).\n` +
        `    \u2192 Targeting may be drifting toward unqualified audiences. Cross-reference recent campaign launches with disq spike timing.`
      );
    }
  }

  // Post-demo disqualification
  const disqAfterRate = pct(notQualAfterDemo, demosHappened);
  if (disqAfterRate !== null && disqAfterRate > 25) {
    findings.weaknesses.push(
      `HIGH POST-DEMO DISQ — ${fmtPct(disqAfterRate)} of demos result in "Not Qualified After Demo" (${notQualAfterDemo}/${demosHappened}).\n` +
      `    \u2192 Discovery questions aren't surfacing disqualifiers early enough. Add pre-demo questionnaire. Review top disq reasons with sales team weekly.`
    );
  }

  // "Too Early" pipeline — nurture opportunity
  const tooEarlyRate = pct(tooEarly, demosHappened);
  if (tooEarly > 0) {
    if (tooEarlyRate !== null && tooEarlyRate > 15) {
      const estimatedFutureWins = Math.round(tooEarly * (closedDeals / Math.max(demosHappened, 1)));
      findings.opportunities.push(
        `LARGE NURTURE PIPELINE — ${tooEarlyRate.toFixed(0)}% of demos (${tooEarly}) marked "Too Early to Close."\n` +
        `    \u2192 At current close rates, ${estimatedFutureWins} of these could close. Add to 30/60/90-day email sequence immediately. This is warm, paid-for pipeline being abandoned.`
      );
    } else {
      findings.opportunities.push(
        `NURTURE PIPELINE — ${tooEarly} deals marked "Too Early." Ensure all have follow-up tasks set at 30 and 60 days. Don't let warm pipeline go cold.`
      );
    }
  }

  // Close rate
  const closeRate  = pct(closedDeals, demosHappened);
  const pCloseRate = pct(pClosedDeals, pDemosHappened);
  if (closeRate !== null) {
    if (closeRate < 10) {
      findings.weaknesses.push(
        `LOW CLOSE RATE — ${fmtPct(closeRate)} demo-to-close (${closedDeals}/${demosHappened}).\n` +
        `    \u2192 Review: Are proposals sent same-day? Is there a clear urgency/deadline? Analyze objection patterns in lost deals. Consider adding a follow-up call 48hrs post-demo.`
      );
    } else if (closeRate > 20) {
      findings.wins.push(
        `STRONG CLOSE RATE — ${fmtPct(closeRate)} of demos close (${closedDeals}/${demosHappened}). Sales team is converting well.`
      );
    }
  }
  if (closeRate !== null && pCloseRate !== null) {
    const closeDelta = chg(closeRate, pCloseRate);
    if (closeDelta !== null && closeDelta < -20) {
      findings.weaknesses.push(
        `CLOSE RATE DECLINING — ${fmtPct(pCloseRate)} \u2192 ${fmtPct(closeRate)} (${arrow(closeDelta)}${absDelta(closeDelta)}% vs prior month).\n` +
        `    \u2192 Check: pricing pushback, proposal quality, deal velocity. Are reps following up fast enough post-demo?`
      );
    } else if (closeDelta !== null && closeDelta > 20) {
      findings.wins.push(
        `CLOSE RATE IMPROVING — ${fmtPct(pCloseRate)} \u2192 ${fmtPct(closeRate)} (${arrow(closeDelta)}${absDelta(closeDelta)}% vs prior month). Sales execution improving.`
      );
    }
  }

  // ── 5. REVENUE VELOCITY ────────────────────────────────────────────────────
  const { newMRR }    = hs;
  const { newMRR: pNewMRR } = pHs;
  const mrrPerDemo  = demosHappened > 0 ? newMRR  / demosHappened  : null;
  const pMrrPerDemo = pDemosHappened > 0 ? pNewMRR / pDemosHappened : null;

  if (mrrPerDemo !== null && pMrrPerDemo !== null) {
    const mrrDelta = chg(mrrPerDemo, pMrrPerDemo);
    if (mrrDelta !== null && mrrDelta < -20) {
      findings.weaknesses.push(
        `DEAL SIZE SHRINKING — MRR per demo: ${fmtD(pMrrPerDemo)} \u2192 ${fmtD(mrrPerDemo)} (${arrow(mrrDelta)}${absDelta(mrrDelta)}%).\n` +
        `    \u2192 Smaller brands closing. Review new traffic sources and whether targeting changes are pulling in smaller companies.`
      );
    } else if (mrrDelta !== null && mrrDelta > 20) {
      findings.wins.push(
        `DEAL SIZE GROWING — MRR per demo: ${fmtD(pMrrPerDemo)} \u2192 ${fmtD(mrrPerDemo)} (${arrow(mrrDelta)}${absDelta(mrrDelta)}%). Larger accounts closing.`
      );
    }
  }

  if (newMRR > 0 && pNewMRR > 0) {
    const mrrGrowth = chg(newMRR, pNewMRR);
    if (mrrGrowth !== null && mrrGrowth > 20) {
      findings.wins.push(
        `STRONG MRR GROWTH — ${fmtD(newMRR)} vs ${fmtD(pNewMRR)} prior period (${arrow(mrrGrowth)}${absDelta(mrrGrowth)}%). Revenue momentum strong.`
      );
    } else if (mrrGrowth !== null && mrrGrowth < -15) {
      findings.alerts.push(
        `MRR DECLINING — ${fmtD(newMRR)} vs ${fmtD(pNewMRR)} prior month (${arrow(mrrGrowth)}${absDelta(mrrGrowth)}% drop).\n` +
        `    \u2192 Diagnose: pipeline volume, close rate, or deal size — which is declining? Each has a different fix.`
      );
    }
  }

  // ── 6. META CREATIVE HEALTH ────────────────────────────────────────────────
  const metaCTR  = channels.meta.ctrAvg;
  const pMetaCTR = pCh.meta.ctrAvg;
  if (metaCTR !== null) {
    if (metaCTR < 0.008) {
      findings.weaknesses.push(
        `META CTR LOW — ${(metaCTR * 100).toFixed(2)}% average CTR. Signals creative fatigue or audience mismatch.\n` +
        `    \u2192 Refresh top 3 highest-spend creatives. Test UGC-style vs polished production. New hook angles: pain-point lead vs aspirational outcome.`
      );
    } else if (metaCTR > 0.018) {
      findings.wins.push(
        `STRONG META CTR — ${(metaCTR * 100).toFixed(2)}%. Creative is resonating. Protect and scale top performers before fatigue sets in.`
      );
    }
    if (pMetaCTR !== null) {
      const ctrDelta = chg(metaCTR, pMetaCTR);
      if (ctrDelta !== null && ctrDelta < -20) {
        findings.weaknesses.push(
          `META CTR DECLINING — ${(pMetaCTR * 100).toFixed(2)}% \u2192 ${(metaCTR * 100).toFixed(2)}% (${arrow(ctrDelta)}${absDelta(ctrDelta)}%).\n` +
          `    \u2192 Audience has seen your ads. Rotate creative immediately. TikTok-native style content is converting for wellness brands right now.`
        );
      }
    }
  }

  // ── 7. PIPELINE VOLUME TREND ───────────────────────────────────────────────
  if (demosToOccur > 0 && pDemosToOccur > 0) {
    const volumeDelta = chg(demosToOccur, pDemosToOccur);
    if (volumeDelta !== null && volumeDelta > 20) {
      findings.wins.push(
        `PIPELINE GROWING — ${demosToOccur} demos this period vs ${pDemosToOccur} prior (${arrow(volumeDelta)}${absDelta(volumeDelta)}%). Demand is accelerating.`
      );
    } else if (volumeDelta !== null && volumeDelta < -15) {
      findings.alerts.push(
        `PIPELINE CONTRACTING — ${demosToOccur} demos vs ${pDemosToOccur} prior period (${arrow(volumeDelta)}${absDelta(volumeDelta)}% drop).\n` +
        `    \u2192 Investigate CPD trend and budget allocation. Is total spend holding? If yes, efficiency dropped. If no, budget was cut.`
      );
    }
  }

  // ── 8. YOUTUBE SPEND EFFICIENCY ────────────────────────────────────────────
  const ytShare = pct(channels.youtube.spend, totalSpend);
  if (ytShare !== null && ytShare > 8 && channels.youtube.demos === 0) {
    findings.weaknesses.push(
      `YOUTUBE: UNTRACKED ROI — ${fmtD(channels.youtube.spend)} (${ytShare.toFixed(0)}% of budget) with 0 attributed demos.\n` +
      `    \u2192 YouTube is brand/awareness only in Windsor. If brand lift isn't being actively measured, consider reallocating to direct-response channels with trackable CPD.`
    );
  }

  // ── 9. OVERALL CPD TREND ───────────────────────────────────────────────────
  const drSpend  = channels.meta.spend + channels.tiktok.spend + channels.google.spend;
  const drDemos  = channels.meta.demos + channels.tiktok.demos + channels.google.demos;
  const pDrSpend = pCh.meta.spend + pCh.tiktok.spend + pCh.google.spend;
  const pDrDemos = pCh.meta.demos + pCh.tiktok.demos + pCh.google.demos;
  const overallCPD  = drDemos  > 0 ? drSpend  / drDemos  : null;
  const pOverallCPD = pDrDemos > 0 ? pDrSpend / pDrDemos : null;

  if (overallCPD !== null && pOverallCPD !== null) {
    const cpdDelta = chg(overallCPD, pOverallCPD);
    if (cpdDelta !== null && cpdDelta > 20) {
      findings.weaknesses.push(
        `OVERALL CPD RISING — ${fmtD(pOverallCPD)} \u2192 ${fmtD(overallCPD)} (${arrow(cpdDelta)}${absDelta(cpdDelta)}% worse).\n` +
        `    \u2192 Across-the-board efficiency loss. Likely causes: creative fatigue, seasonal CPM increase, or audience exhaustion. Check top-spend creatives for frequency.`
      );
    } else if (cpdDelta !== null && cpdDelta < -15) {
      findings.wins.push(
        `OVERALL CPD IMPROVING — ${fmtD(pOverallCPD)} \u2192 ${fmtD(overallCPD)} (${arrow(cpdDelta)}${absDelta(cpdDelta)}% better). Media efficiency is gaining across the board.`
      );
    }
  }

  // ── FORMAT OUTPUT ──────────────────────────────────────────────────────────
  const lines = [];
  lines.push('\n' + '═'.repeat(60));
  lines.push('  \uD83E\uDDE0  INTELLIGENCE REPORT — ANALYSIS & INSIGHTS');
  lines.push('  (Based on: ' + winLabel + ' vs Prior Month)');
  lines.push('═'.repeat(60));

  if (findings.alerts.length > 0) {
    lines.push('\n\uD83D\uDEA8  ALERTS — Requires Immediate Attention');
    lines.push('─'.repeat(60));
    findings.alerts.forEach((f, i) => {
      lines.push(`  [${i + 1}] ${f}`);
      if (i < findings.alerts.length - 1) lines.push('');
    });
  }

  if (findings.weaknesses.length > 0) {
    lines.push('\n\u26A0\uFE0F   WEAKNESSES — Underperformance to Address');
    lines.push('─'.repeat(60));
    findings.weaknesses.forEach((f, i) => {
      lines.push(`  [${i + 1}] ${f}`);
      if (i < findings.weaknesses.length - 1) lines.push('');
    });
  }

  if (findings.opportunities.length > 0) {
    lines.push('\n\uD83C\uDFAF  OPPORTUNITIES — Actionable Upside');
    lines.push('─'.repeat(60));
    findings.opportunities.forEach((f, i) => {
      lines.push(`  [${i + 1}] ${f}`);
      if (i < findings.opportunities.length - 1) lines.push('');
    });
  }

  if (findings.wins.length > 0) {
    lines.push('\n\u2705  WINS — What\'s Working');
    lines.push('─'.repeat(60));
    findings.wins.forEach((f, i) => {
      lines.push(`  [${i + 1}] ${f}`);
      if (i < findings.wins.length - 1) lines.push('');
    });
  }

  const total = findings.alerts.length + findings.weaknesses.length + findings.opportunities.length + findings.wins.length;
  if (total === 0) {
    lines.push('  No significant signals detected. All metrics within normal ranges.');
  }

  return lines.join('\n');
}

// ── Report wrapper ────────────────────────────────────────────────────────────
function buildReport(execSections, detailSections, intelligenceSection) {
  const now = new Date().toLocaleString('en-US', {
    timeZone: 'America/Los_Angeles', dateStyle: 'full', timeStyle: 'short'
  });
  const lines = [
    '\n',
    '╔════════════════════════════════════════════════════════════╗',
    '║         FRONTROWMD MARKETING PERFORMANCE REPORT           ║',
    `║  ${now.substring(0, 57).padEnd(57)} ║`,
    '╚════════════════════════════════════════════════════════════╝',
    ...execSections,
    `\n${'─'.repeat(60)}`,
    '  DETAILED BREAKDOWNS',
    `${'─'.repeat(60)}`,
    ...detailSections,
    intelligenceSection || '',
    `\n${'─'.repeat(60)}`,
    '  ✅  Report complete. Data: Windsor.ai + HubSpot CRM',
    `${'─'.repeat(60)}\n`,
  ];
  return lines.join('\n');
}

// ── Main ──────────────────────────────────────────────────────────────────────
async function main() {
  console.log('\n⏳  Fetching data from Windsor.ai and HubSpot...\n');

  const { current: windows, previous: prevWindows } = getWindows();
  for (const [k, v] of Object.entries(windows)) {
    console.log(`  ${k}: ${v.from} → ${v.to}  |  prev: ${prevWindows[k].from} → ${prevWindows[k].to}`);
  }
  console.log('');

  const winKeys = Object.keys(windows);

  // Fetch HubSpot sequentially to avoid secondly rate limits, Windsor in parallel
  const hubspotData     = await fetchAllHubSpotData(windows);
  const prevHubspotData = await fetchAllHubSpotData(prevWindows);
  const demoCohorts     = await fetchDemoCohorts();

  // Windsor + GA4 can all fire in parallel (no rate limit issues)
  const allWindsor = await Promise.all([
    ...winKeys.map(k => Promise.all([fetchWindsorDemos(windows[k].from, windows[k].to), fetchGA4(windows[k].from, windows[k].to)])),
    ...winKeys.map(k => Promise.all([fetchWindsorDemos(prevWindows[k].from, prevWindows[k].to), fetchGA4(prevWindows[k].from, prevWindows[k].to)])),
  ]);

  // Split allWindsor: first N = current, next N = previous
  const n = winKeys.length;
  const windowedChannels     = allWindsor.slice(0, n);
  const prevWindowedChannels = allWindsor.slice(n, n * 2);

  // GA4 source breakdown (per-window, current + previous, non-blocking)
  const ga4SourcesByWindow = {};
  const ga4PrevSourcesByWindow = {};
  await Promise.all([
    ...winKeys.map(async k => {
      ga4SourcesByWindow[k]     = await fetchGA4Sources(windows[k].from,     windows[k].to);
      ga4PrevSourcesByWindow[k] = await fetchGA4Sources(prevWindows[k].from, prevWindows[k].to);
    }),
  ]);

  const execSections   = [];
  const detailSections = [];
  const slackSections  = [];

  for (let i = 0; i < winKeys.length; i++) {
    const key      = winKeys[i];
    const win      = windows[key];
    const [channels, ga4]         = windowedChannels[i];
    const [prevChannels, prevGa4] = prevWindowedChannels[i];
    execSections.push(buildExecSummary(win.label, channels, ga4, hubspotData[key]));
    detailSections.push(buildSection(win.label, channels, hubspotData[key]));
    slackSections.push(buildSlackSummary(
      win.label, channels, ga4, hubspotData[key],
      prevChannels, prevGa4, prevHubspotData[key],
      prevWindows[key].label,
      win.from, win.to
    ));
  }

  // Intelligence analysis (MTD vs prior month)
  const mtdIdx = winKeys.indexOf('mtd');
  const intlCurr = { mtd: { channels: windowedChannels[mtdIdx][0], ga4: windowedChannels[mtdIdx][1], hs: hubspotData.mtd } };
  const intlPrev = { mtd: { channels: prevWindowedChannels[mtdIdx][0], ga4: prevWindowedChannels[mtdIdx][1], hs: prevHubspotData.mtd } };
  const intelligenceSection = buildIntelligence(intlCurr, intlPrev, { current: windows, previous: prevWindows });

  const report = buildReport(execSections, detailSections, intelligenceSection);
  console.log(report);

  const ts        = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 16);
  const filename  = `frontrowmd-report-${ts}.txt`;
  const dashname  = `frontrowmd-dashboard-${ts}.html`;
  fs.writeFileSync(filename, report);
  console.log(`📄  Report saved to: ${filename}`);

  const dashboard = buildDashboard(windowedChannels, hubspotData, prevWindowedChannels, prevHubspotData, winKeys, windows, prevWindows, filename, ga4SourcesByWindow, ga4PrevSourcesByWindow, demoCohorts);
  fs.writeFileSync(dashname, dashboard);
  console.log(`📊  Dashboard saved to: ${dashname}\n`);

  return { report, dashname, slackSections };
}

// ── GitHub Pages deploy ────────────────────────────────────────────────────────
// Commits the dashboard HTML (+ logo if present) to the gh-pages branch via
// the GitHub Contents API. No git CLI required — pure HTTP calls.
// Returns the live GitHub Pages URL for the dashboard file.
async function deployToGitHub(dashPath) {
  if (!GITHUB_TOKEN || !GITHUB_OWNER || !GITHUB_REPO) {
    console.warn('⚠️  GITHUB_TOKEN / GITHUB_OWNER / GITHUB_REPO not set — skipping deploy');
    return null;
  }

  const path = require('path');
  const dashName = path.basename(dashPath);
  const base = `https://api.github.com/repos/${GITHUB_OWNER}/${GITHUB_REPO}/contents`;
  const headers = {
    'Authorization': `Bearer ${GITHUB_TOKEN}`,
    'Accept':        'application/vnd.github+json',
    'Content-Type':  'application/json',
    'X-GitHub-Api-Version': '2022-11-28',
  };

  // Helper — upsert a file to gh-pages branch (create or update)
  async function upsertFile(filename, fileBytes, message) {
    const encoded = fileBytes.toString('base64');

    // Check if file already exists so we can pass its SHA for update
    const checkRes = await fetch(`${base}/${filename}?ref=gh-pages`, { headers });
    let sha;
    if (checkRes.ok) {
      const existing = await checkRes.json();
      sha = existing.sha;
    }

    const body = { message, content: encoded, branch: 'gh-pages' };
    if (sha) body.sha = sha;

    const putRes = await fetch(`${base}/${filename}`, {
      method:  'PUT',
      headers,
      body:    JSON.stringify(body),
    });
    if (!putRes.ok) throw new Error(`GitHub upsert failed (${filename}): ${putRes.status} ${await putRes.text()}`);
  }

  const today = new Date().toISOString().slice(0, 10);

  // Commit the dashboard HTML
  const dashBytes = fs.readFileSync(dashPath);
  await upsertFile(dashName, dashBytes, `Dashboard ${today}`);
  console.log(`  ↑ committed ${dashName}`);

  // Commit the logo if it lives in the project folder
  const logoPath = path.join(__dirname, 'White_Graphic_Logo.png');
  if (fs.existsSync(logoPath)) {
    const logoBytes = fs.readFileSync(logoPath);
    await upsertFile('White_Graphic_Logo.png', logoBytes, `Logo ${today}`);
    console.log(`  ↑ committed White_Graphic_Logo.png`);
  }

  // Commit the favicon if it lives in the project folder
  const faviconPath = path.join(__dirname, 'FrontrowMD_Favicon.png');
  if (fs.existsSync(faviconPath)) {
    const faviconBytes = fs.readFileSync(faviconPath);
    await upsertFile('FrontrowMD_Favicon.png', faviconBytes, `Favicon ${today}`);
    console.log(`  ↑ committed FrontrowMD_Favicon.png`);
  }

  // Commit the navy blue logo (used on password page + footer)
  const navyLogoPath = path.join(__dirname, 'FrontrowMD_Navy_Blue_Logo.png');
  if (fs.existsSync(navyLogoPath)) {
    const navyLogoBytes = fs.readFileSync(navyLogoPath);
    await upsertFile('FrontrowMD_Navy_Blue_Logo.png', navyLogoBytes, `Navy logo ${today}`);
    console.log(`  ↑ committed FrontrowMD_Navy_Blue_Logo.png`);
  }

  const liveUrl = `https://${GITHUB_OWNER}.github.io/${GITHUB_REPO}/${dashName}`;
  console.log(`✅  Deployed to GitHub Pages: ${liveUrl}`);
  return liveUrl;
}

// ── Slack delivery ─────────────────────────────────────────────────────────────
async function postToSlack(text) {
  if (!SLACK_WEBHOOK) { console.warn('⚠️  SLACK_WEBHOOK not set — skipping Slack'); return; }
  const res = await fetch(SLACK_WEBHOOK, {
    method:  'POST',
    headers: { 'Content-Type': 'application/json' },
    body:    JSON.stringify({ text }),
  });
  if (!res.ok) throw new Error(`Slack post failed: ${res.status} ${await res.text()}`);
  console.log('✅  Slack posted');
}

// ── Email delivery ─────────────────────────────────────────────────────────────
async function sendEmail(subject, dashPath, dashUrl) {
  if (!EMAIL_FROM || !EMAIL_PASS || EMAIL_TO.length === 0) {
    console.warn('⚠️  Email credentials not set — skipping email'); return;
  }
  const nodemailer = require('nodemailer');
  const transporter = nodemailer.createTransport({
    service: 'gmail',
    auth: { user: EMAIL_FROM, pass: EMAIL_PASS },
  });

  // Build the email body — button if we have a URL, fallback to attachment if not
  let htmlBody, attachments;
  if (dashUrl) {
    htmlBody = `
      <div style="font-family:'Helvetica Neue',Arial,sans-serif;max-width:480px;margin:0 auto;padding:32px 24px">
        <p style="font-size:13px;letter-spacing:0.08em;text-transform:uppercase;color:#72A4BF;margin:0 0 8px">FrontrowMD Marketing</p>
        <h1 style="font-size:22px;font-weight:700;color:#172C45;margin:0 0 16px;line-height:1.3">${subject}</h1>
        <p style="font-size:14px;color:#1D4053;line-height:1.6;margin:0 0 28px">
          Your daily marketing dashboard is ready. Click below to open the full interactive report in your browser — no download required.
        </p>
        <a href="${dashUrl}"
           style="display:inline-block;background:#172C45;color:#ffffff;font-size:14px;font-weight:700;
                  text-decoration:none;padding:14px 28px;border-radius:50px">
          View Dashboard →
        </a>
        <p style="font-size:11px;color:#8a9aaa;margin:24px 0 0;line-height:1.6">
          Or copy this link:<br>
          <a href="${dashUrl}" style="color:#72A4BF">${dashUrl}</a>
        </p>
        <hr style="border:none;border-top:1px solid #D4E6EF;margin:32px 0 16px">
        <p style="font-size:11px;color:#8a9aaa;margin:0">
          Sent automatically by FrontrowMD Marketing Operations
        </p>
      </div>`;
    attachments = [];
  } else {
    // GitHub not configured — fall back to attaching the HTML file directly
    htmlBody = `
      <div style="font-family:'Helvetica Neue',Arial,sans-serif;max-width:480px;margin:0 auto;padding:32px 24px">
        <p style="font-size:13px;letter-spacing:0.08em;text-transform:uppercase;color:#72A4BF;margin:0 0 8px">FrontrowMD Marketing</p>
        <h1 style="font-size:22px;font-weight:700;color:#172C45;margin:0 0 16px">${subject}</h1>
        <p style="font-size:14px;color:#1D4053;line-height:1.6;margin:0 0 0">
          Your dashboard is attached. Open the <strong>.html file</strong> in any browser (Chrome, Safari, Edge).
        </p>
      </div>`;
    attachments = [{ filename: require('path').basename(dashPath), path: dashPath }];
  }

  await transporter.sendMail({
    from:   `FrontrowMD Marketing <${EMAIL_FROM}>`,
    to:     EMAIL_TO.join(', '),
    subject,
    html:   htmlBody,
    attachments,
  });
  console.log(`✅  Email sent → ${EMAIL_TO.join(', ')}`);
}

// ── Custom Range API ───────────────────────────────────────────────────────────
// Fetches Windsor + HubSpot for an arbitrary date range and returns a
// dashboard-compatible window object (same shape as buildWin() output).
// The "previous" window is the same-length period immediately before.
async function fetchCustomWindow(from, to) {
  // Calculate previous period (same length, immediately before)
  const fromD  = new Date(from + 'T00:00:00');
  const toD    = new Date(to   + 'T23:59:59');
  const dayMs  = 24 * 60 * 60 * 1000;
  const rangeMs = toD - fromD + dayMs;              // inclusive length in ms
  const pToD   = new Date(fromD.getTime() - dayMs);
  const pFromD = new Date(pToD.getTime() - rangeMs + dayMs);
  const pFrom  = toDateStr(pFromD);
  const pTo    = toDateStr(pToD);

  const label     = from === to ? from : `${from} to ${to}`;
  const prevLabel = `${pFrom} to ${pTo}`;

  console.log(`  custom window: ${from} → ${to}  |  prev: ${pFrom} → ${pTo}`);

  // Fetch Windsor in parallel, HubSpot sequentially to avoid rate limits
  const [[channels, ga4], [prevChannels, prevGa4]] = await Promise.all([
    Promise.all([fetchWindsorDemos(from, to), fetchGA4(from, to)]),
    Promise.all([fetchWindsorDemos(pFrom, pTo), fetchGA4(pFrom, pTo)]),
  ]);
  const hsRaw     = await fetchAllHubSpotData({ mtd: { from, to, label }, prev: { from: pFrom, to: pTo, label: prevLabel } });
  const prevHsRaw = await fetchAllHubSpotData({ mtd: { from: pFrom, to: pTo, label: prevLabel }, prev: { from: pFrom, to: pTo, label: prevLabel } });

  const hs     = hsRaw.mtd;
  const prevHs = prevHsRaw.mtd;

  // Fetch GA4 source breakdown for custom window
  let ga4Src = [], ga4PrevSrc = [];
  try {
    [ga4Src, ga4PrevSrc] = await Promise.all([
      fetchGA4Sources(from, to),
      fetchGA4Sources(pFrom, pTo),
    ]);
  } catch(e) { console.warn('  WARN custom GA4 sources:', e.message); }

  function buildWin(ch, g4, h, ga4Sources, ga4PrevSources) {
    const { demosBooked, demosToOccur, demosHappened, dealsWon, pctDemosWon,
            notQualAfterDemo, disqualifiedBeforeDemo, tooEarly, rescheduled, canceled, blankStatus, closedDeals, avgDealCycleDays, ownerBreakdown, channelAttribution, dowBreakdown, newMRR } = h;
    const pipeline = { demosToOccur, demosHappened, dealsWon, pctDemosWon,
                       notQualAfterDemo, disqualifiedBeforeDemo, tooEarly,
                       rescheduled, canceled, blankStatus, closedDeals, avgDealCycleDays, ownerBreakdown: ownerBreakdown || [], dowBreakdown: dowBreakdown || [] };
    const metaCTR  = ch.meta.ctrAvg != null
      ? (ch.meta.ctrAvg * 100).toFixed(2) + '%' : null;
    return { channels: ch, ga4: g4, demosBooked, pipeline, channelAttribution: channelAttribution || {}, newMRR, metaCTR,
             ga4Sources: ga4Sources || [],
             ga4PrevSources: ga4PrevSources || [] };
  }

  const current = buildWin(channels, ga4, hs, ga4Src, ga4PrevSrc);
  const previous = buildWin(prevChannels, prevGa4, prevHs, ga4PrevSrc, []);

  return {
    ...current,
    prev: previous,
    prevLabel,
    from,
    to,
    dateFrom: from,
    dateTo: to,
    label,
  };
}

// ── Local API Server ───────────────────────────────────────────────────────────
// Start with:  node report.js --serve
// Or auto-starts alongside the scheduled report run.
//
// Endpoints:
//   GET /custom?from=YYYY-MM-DD&to=YYYY-MM-DD
//       → 200 { ok: true, window: <dashboardWindowObject> }
//       → 400 { ok: false, error: '...' }
//       → 500 { ok: false, error: '...' }
//
//   GET /health
//       → 200 { ok: true, time: '...' }
//
// CORS: allows all origins (dashboard is served from GitHub Pages)
const SERVER_PORT = parseInt(process.env.API_PORT || '3001', 10);

function startServer() {
  const http = require('http');

  const server = http.createServer(async (req, res) => {
    // CORS — allow the GitHub Pages dashboard (and localhost) to call this
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
    res.setHeader('Content-Type', 'application/json');

    if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return; }

    const url  = new URL(req.url, `http://localhost:${SERVER_PORT}`);
    const path = url.pathname;

    // ── GET /health ──────────────────────────────────────────────
    if (path === '/health') {
      res.writeHead(200);
      res.end(JSON.stringify({ ok: true, time: new Date().toISOString() }));
      return;
    }

    // ── GET /custom ──────────────────────────────────────────────
    if (path === '/custom') {
      const from = url.searchParams.get('from');
      const to   = url.searchParams.get('to');

      if (!from || !to) {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false, error: 'Missing ?from=YYYY-MM-DD&to=YYYY-MM-DD' }));
        return;
      }
      if (!/^\d{4}-\d{2}-\d{2}$/.test(from) || !/^\d{4}-\d{2}-\d{2}$/.test(to)) {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false, error: 'Dates must be YYYY-MM-DD format' }));
        return;
      }
      if (from > to) {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false, error: 'from must be before or equal to to' }));
        return;
      }

      const today = toDateStr(new Date());
      if (to > today) {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false, error: `to date cannot be in the future (today is ${today})` }));
        return;
      }

      console.log(`\n→ API /custom  ${from} → ${to}`);
      try {
        const windowData = await fetchCustomWindow(from, to);
        res.writeHead(200);
        res.end(JSON.stringify({ ok: true, window: windowData }));
        console.log(`← API /custom  OK`);
      } catch (err) {
        console.error(`← API /custom  ERROR:`, err.message);
        res.writeHead(500);
        res.end(JSON.stringify({ ok: false, error: err.message }));
      }
      return;
    }

    // 404
    res.writeHead(404);
    res.end(JSON.stringify({ ok: false, error: 'Not found' }));
  });

  server.listen(SERVER_PORT, '127.0.0.1', () => {
    console.log(`\n🚀  Custom range API running at http://localhost:${SERVER_PORT}`);
    console.log(`    GET /custom?from=2026-02-01&to=2026-02-07`);
    console.log(`    GET /health\n`);
  });

  server.on('error', err => {
    if (err.code === 'EADDRINUSE') {
      console.error(`❌  Port ${SERVER_PORT} already in use. Set API_PORT= in .env to change it.`);
    } else {
      console.error('❌  Server error:', err.message);
    }
    process.exit(1);
  });

  return server;
}

// ── Entry point ────────────────────────────────────────────────────────────────
const args = process.argv.slice(2);

// Catch anything that escapes the main try/catch
process.on('unhandledRejection', (reason, promise) => {
  console.error('\n💥  UNHANDLED REJECTION:');
  console.error(reason);
  process.exit(1);
});
process.on('uncaughtException', (err) => {
  console.error('\n💥  UNCAUGHT EXCEPTION:');
  console.error(err);
  process.exit(1);
});

if (args.includes('--serve')) {
  // Server-only mode: node report.js --serve
  // No report run — just start the API and keep it alive for dashboard queries
  console.log('🖥️  Server-only mode. No report will be generated.');
  startServer();

} else if (args.find(a => a.startsWith('--custom-from'))) {
  // Custom range mode: node report.js --custom-from=YYYY-MM-DD --custom-to=YYYY-MM-DD
  // Used by GitHub Actions when triggered from the dashboard Custom Range tab.
  // Generates a custom.html dashboard and deploys it to GitHub Pages.
  const fromArg = args.find(a => a.startsWith('--custom-from'));
  const toArg   = args.find(a => a.startsWith('--custom-to'));
  const customFrom = fromArg ? fromArg.split('=')[1] : null;
  const customTo   = toArg   ? toArg.split('=')[1]   : null;

  if (!customFrom || !customTo || !/^\d{4}-\d{2}-\d{2}$/.test(customFrom) || !/^\d{4}-\d{2}-\d{2}$/.test(customTo)) {
    console.error('❌  Usage: node report.js --custom-from=YYYY-MM-DD --custom-to=YYYY-MM-DD');
    process.exit(1);
  }

  (async () => {
    try {
      console.log(`\n🔧  Custom range mode: ${customFrom} → ${customTo}`);
      const windowData = await fetchCustomWindow(customFrom, customTo);

      // Build a dashboard HTML with this single custom window
      const generatedAt = new Date().toLocaleString('en-US', {
        timeZone: 'America/Los_Angeles', dateStyle: 'medium', timeStyle: 'short'
      });
      const customDashWindows = {
        custom: {
          ...windowData,
          label: windowData.label,
          prev: windowData.prev,
          prevLabel: windowData.prevLabel,
        }
      };
      const customCohorts = await fetchDemoCohorts();
      const data = JSON.stringify({ generatedAt, filename: `custom-${customFrom}-to-${customTo}`, windows: customDashWindows, demoCohorts: customCohorts })
        .replace(/<\/script>/gi, '<\\/script>');
      let template = fs.readFileSync(__dirname + '/dashboard_template.html', 'utf8');

      // Inject custom range config
      const workerUrl = process.env.WORKER_URL || '';
      const pagesBase = (GITHUB_OWNER && GITHUB_REPO)
        ? `https://${GITHUB_OWNER}.github.io/${GITHUB_REPO}`
        : '';
      template = template
        .replace("'__WORKER_URL__'", JSON.stringify(workerUrl))
        .replace("'__PAGES_BASE__'", JSON.stringify(pagesBase));

      const customHTML = template.replace('"__DASHBOARD_DATA__"', data);

      // Write to disk
      const customFilename = 'custom.html';
      fs.writeFileSync(customFilename, customHTML);
      console.log(`  ✅ Wrote ${customFilename}`);

      // Deploy to GitHub Pages
      const customUrl = await deployToGitHub(customFilename);
      if (customUrl) {
        console.log(`  📊 Custom dashboard live at: ${customUrl}`);
      }

      // Send Slack notification
      const label = `${customFrom} to ${customTo}`;
      const prevLabel = windowData.prevLabel || 'prior period';
      const urlLine = customUrl ? `\n📊 *Dashboard:* ${customUrl}\n` : '';
      await postToSlack(`*FrontrowMD Custom Report — ${label}*\n_Compared to: ${prevLabel}_${urlLine}`);

      console.log(`\n✅  Custom range report complete.`);
      process.exit(0);
    } catch (err) {
      console.error('\n❌  Fatal error in custom range mode:');
      console.error(err);
      process.exit(1);
    }
  })();

} else {
  // Normal mode: run the report AND start the server in the background
  // so the dashboard can immediately query custom ranges after opening
  startServer();

  main()
  .then(async ({ report, dashname, slackSections }) => {
    const today = new Date().toLocaleDateString('en-US', {
      weekday: 'long', month: 'long', day: 'numeric', year: 'numeric'
    });

    // Deploy to GitHub Pages first so we have the URL for both Slack + email
    const dashUrl = await deployToGitHub(dashname);

    // Slack — title + dashboard link only
    const urlLine   = dashUrl ? `\n📊 Full dashboard: ${dashUrl}` : '';
    await postToSlack(
      `*FrontrowMD Marketing Dashboard — ${today}*${urlLine}`
    );

    // Email — branded button with live URL (or attachment fallback)
    await sendEmail(`FrontrowMD Marketing Dashboard — ${today}`, dashname, dashUrl);

    console.log(`\n✅  Report complete. Server staying alive on port ${SERVER_PORT} for custom range queries.`);
    console.log(`    Close with Ctrl+C when done.\n`);
    // Server keeps the process alive — no process.exit() here
  })
  .catch(err => {
    console.error('\n❌  Fatal error in main():');
    console.error(err);
    process.exit(1);
  });
}
