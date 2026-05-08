/**
 * FrontrowMD — Weekly Pipeline Report (Full Funnel)
 * Runs: Every Friday at 4:00 PM PT via GitHub Actions
 * Delivers: Slack channel
 *
 * Top of Funnel: Windsor.ai per-channel spend, clicks, CTR, CPC, demos, CPA
 * Mid Funnel: HubSpot demos by source (UTM), DQ rate, CPQD
 * Bottom of Funnel: Closed won, MRR, ARR, deal cycle
 * Per-Rep: Demos, qual rate, deals won, ARR, pruned
 * Budget Reallocation: Shift recommendation based on CPQD
 * Trend Flags: Channels >15% worse on CPQD vs 4-week avg
 */

process.env.TZ = 'America/New_York';
require('dotenv').config();
const fetch = (...a) => import('node-fetch').then(m => m.default(...a));

// ═══════════════════════════════════════════════════════════════
// CONFIG
// ═══════════════════════════════════════════════════════════════

const WINDSOR_KEY = process.env.WINDSOR_API_KEY;
const HS_TOKEN = process.env.HUBSPOT_TOKEN;
const SLACK_WEBHOOK = process.env.SLACK_WEBHOOK;
const DASHBOARD_URL = process.env.DASHBOARD_URL || '';

const CHANNELS = {
  meta:     { connector: 'facebook', demoField: 'conversions_submit_application_total', label: 'Meta',
              fields: 'date,campaign_name,spend,clicks,impressions,conversions_submit_application_total' },
  linkedin: { connector: 'linkedin', demoField: 'externalwebsiteconversions', label: 'LinkedIn',
              fields: 'date,campaign,spend,clicks,impressions,externalwebsiteconversions', campField: 'campaign' },
  tiktok:   { connector: 'tiktok', demoField: 'conversions', label: 'TikTok',
              fields: 'date,campaign_name,spend,clicks,impressions,conversions' },
  google:   { connector: 'google_ads', demoField: 'conversions', label: 'Google Ads',
              fields: 'date,campaign_name,spend,clicks,impressions,conversions' },
};

// ═══════════════════════════════════════════════════════════════
// DATE HELPERS
// ═══════════════════════════════════════════════════════════════

function fmt(d) {
  return `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}`;
}

function toMs(dateStr, endOfDay) {
  return new Date(dateStr + (endOfDay ? 'T23:59:59.999' : 'T00:00:00.000')).getTime();
}

function getThisWeek() {
  const now = new Date();
  const day = now.getDay();
  const mon = new Date(now); mon.setDate(now.getDate() - (day === 0 ? 6 : day - 1));
  const today = new Date(now); // Include today (Friday)
  return { from: fmt(mon), to: fmt(today), label: `${fmt(mon)} to ${fmt(today)}` };
}

function getLastWeek() {
  const tw = getThisWeek();
  const mon = new Date(tw.from + 'T12:00:00');
  mon.setDate(mon.getDate() - 7);
  const fri = new Date(mon); fri.setDate(mon.getDate() + 4);
  return { from: fmt(mon), to: fmt(fri), label: `${fmt(mon)} to ${fmt(fri)}` };
}

function get4WeekAvg() {
  // 4 weeks ending last week's Friday
  const tw = getThisWeek();
  const lwFri = new Date(tw.from + 'T12:00:00'); lwFri.setDate(lwFri.getDate() - 3); // last Friday
  const start = new Date(lwFri); start.setDate(start.getDate() - 27); // 28 days back
  return { from: fmt(start), to: fmt(lwFri), label: `${fmt(start)} to ${fmt(lwFri)}`, weeks: 4 };
}

function getMTD() {
  const now = new Date();
  const first = new Date(now.getFullYear(), now.getMonth(), 1);
  const today = new Date(now); // Include today
  return { from: fmt(first), to: fmt(today), label: `${fmt(first)} to ${fmt(today)}` };
}

function delay(ms) { return new Promise(r => setTimeout(r, ms)); }

// ═══════════════════════════════════════════════════════════════
// WINDSOR.AI FETCH
// ═══════════════════════════════════════════════════════════════

async function windsorFetch(connector, from, to, fields) {
  const url = `https://connectors.windsor.ai/${connector}?api_key=${WINDSOR_KEY}&date_from=${from}&date_to=${to}&fields=${fields}&page_size=5000`;
  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      const r = await fetch(url);
      if (!r.ok) throw new Error(`Windsor ${connector}: ${r.status}`);
      const d = await r.json();
      return d.data || [];
    } catch (e) {
      if (attempt === 2) { console.error(`Windsor fetch failed: ${e.message}`); return []; }
      await new Promise(r => setTimeout(r, 2000 * (attempt + 1)));
    }
  }
  return [];
}

async function fetchAdData(from, to) {
  const results = {};
  for (const [ch, cfg] of Object.entries(CHANNELS)) {
    const rows = await windsorFetch(cfg.connector, from, to, cfg.fields);
    let spend = 0, clicks = 0, impressions = 0, demos = 0;
    for (const row of rows) {
      const campName = row[cfg.campField || 'campaign_name'] || row.campaign_name || row.campaign || '';
      if (ch === 'google' && /\byt\b|youtube/i.test(campName)) continue;
      spend += parseFloat(row.spend) || 0;
      clicks += parseInt(row.clicks) || 0;
      impressions += parseInt(row.impressions) || 0;
      let d = parseFloat(row[cfg.demoField]) || 0;
      if (ch === 'google') d = Math.ceil(d); else d = Math.round(d);
      demos += d;
    }
    const ctr = impressions > 0 ? (clicks / impressions) * 100 : 0;
    const cpc = clicks > 0 ? spend / clicks : null;
    const cpa = demos > 0 ? spend / demos : null;
    results[ch] = { spend, clicks, impressions, demos, ctr, cpc, cpa };
  }
  return results;
}

// ═══════════════════════════════════════════════════════════════
// HUBSPOT SEARCH
// ═══════════════════════════════════════════════════════════════

async function hsSearch(objectType, body) {
  const results = [];
  let after = undefined;
  for (let page = 0; page < 20; page++) {
    const req = { ...body, limit: 100 };
    if (after) req.after = after;
    let r;
    for (let attempt = 0; attempt < 3; attempt++) {
      r = await fetch(`https://api.hubapi.com/crm/v3/objects/${objectType}/search`, {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${HS_TOKEN}`, 'Content-Type': 'application/json' },
        body: JSON.stringify(req),
      });
      if (r.status === 429) {
        console.warn(`  HubSpot 429, retrying in ${(attempt+1)*2}s...`);
        await new Promise(res => setTimeout(res, (attempt + 1) * 2000));
        continue;
      }
      break;
    }
    if (!r.ok) { console.error(`HubSpot ${objectType}: ${r.status}`); break; }
    const d = await r.json();
    results.push(...(d.results || []));
    if (!d.paging?.next?.after) break;
    after = d.paging.next.after;
  }
  return results;
}

async function fetchDemosBooked(from, to) {
  const startMs = toMs(from), endMs = toMs(to, true);
  console.log(`    Demos Booked: ${from} to ${to} | ms: ${startMs}–${endMs}`);
  const contacts = await hsSearch('contacts', {
    filterGroups: [{ filters: [
      { propertyName: 'createdate', operator: 'GTE', value: String(startMs) },
      { propertyName: 'createdate', operator: 'LTE', value: String(endMs) },
      { propertyName: 'date_demo_booked', operator: 'HAS_PROPERTY' },
    ]}],
    properties: ['createdate', 'date_demo_booked'],
  });
  console.log(`    → ${contacts.length}`);
  return contacts.length;
}

async function fetchPipelineDeals(from, to) {
  const gteMs = toMs(from), lteMs = toMs(to, true);
  return await hsSearch('deals', {
    filterGroups: [
      { filters: [
        { propertyName: 'date_demo_booked', operator: 'GTE', value: String(gteMs) },
        { propertyName: 'date_demo_booked', operator: 'LTE', value: String(lteMs) },
      ]},
      { filters: [
        { propertyName: 'demo_given__status', operator: 'IN', values: ['No Show', 'No Showed'] },
        { propertyName: 'hs_createdate', operator: 'GTE', value: String(gteMs) },
        { propertyName: 'hs_createdate', operator: 'LTE', value: String(lteMs) },
      ]},
    ],
    properties: ['date_demo_booked', 'demo_given__status', 'dealstage', 'amount',
                 'closedate', 'hs_createdate', 'hubspot_owner_id', 'utm_source', 'utm_medium', 'hs_analytics_source'],
  });
}

async function fetchClosedWon(from, to) {
  const gteMs = toMs(from), lteMs = toMs(to, true);
  return await hsSearch('deals', {
    filterGroups: [{ filters: [
      { propertyName: 'dealstage', operator: 'EQ', value: 'closedwon' },
      { propertyName: 'closedate', operator: 'GTE', value: String(gteMs) },
      { propertyName: 'closedate', operator: 'LTE', value: String(lteMs) },
    ]}],
    properties: ['amount', 'closedate', 'hs_createdate', 'hubspot_owner_id', 'utm_source', 'utm_medium', 'hs_analytics_source'],
  });
}

async function fetchOwners() {
  const r = await fetch('https://api.hubapi.com/crm/v3/owners?limit=100', {
    headers: { 'Authorization': `Bearer ${HS_TOKEN}` },
  });
  if (!r.ok) return {};
  const d = await r.json();
  const map = {};
  for (const o of (d.results || [])) map[o.id] = `${o.firstName || ''} ${o.lastName || ''}`.trim() || o.email || o.id;
  return map;
}

// ═══════════════════════════════════════════════════════════════
// ANALYSIS
// ═══════════════════════════════════════════════════════════════

function mapUtmToChannel(utm_source, utm_medium) {
  const src = (utm_source || '').toLowerCase().trim();
  const med = (utm_medium || '').toLowerCase().trim();
  if (['fb', 'ig', 'facebook', 'instagram', 'meta'].includes(src)) return 'meta';
  if (src === 'google' && (med === 'cpc' || med === 'paid')) return 'google';
  if (src === 'linkedin') return 'linkedin';
  if (['tiktok', 'tik_tok', 'tt', 'tiktok_ads'].includes(src)) return 'tiktok';
  return null;
}

function categorizeDeal(status) {
  const s = (status || '').trim();
  if (s === 'Demo Given' || s === 'Demo Given at Rescheduled time') return 'qualified';
  if (s === 'Demo Given, Qualified Company, too early') return 'tooEarly';
  if (s === 'Not Qualified after the demo') return 'notQualified';
  if (s === 'Disqualified, Meeting Cancelled') return 'disqualifiedBefore';
  if (s === 'No Show') return 'rescheduled';
  if (s === 'No Showed') return 'canceled';
  return 'blank';
}

function analyzePipeline(deals) {
  const stats = { total:0, happened:0, qualified:0, tooEarly:0, notQualified:0, disqualifiedBefore:0, rescheduled:0, canceled:0, blank:0 };
  const byOwner = {}, byChannel = {};
  for (const ch of Object.keys(CHANNELS)) byChannel[ch] = { total:0, happened:0, qualified:0, tooEarly:0, notQualified:0, disqualifiedBefore:0 };

  for (const deal of deals) {
    const p = deal.properties || {};
    const cat = categorizeDeal(p.demo_given__status);
    stats.total++; stats[cat]++;
    if (['qualified', 'tooEarly', 'notQualified'].includes(cat)) stats.happened++;

    const ownerId = p.hubspot_owner_id || 'unassigned';
    if (!byOwner[ownerId]) byOwner[ownerId] = { total:0, happened:0, qualified:0, tooEarly:0, notQualified:0, disqualifiedBefore:0, rescheduled:0, canceled:0, blank:0 };
    byOwner[ownerId].total++; byOwner[ownerId][cat]++;
    if (['qualified', 'tooEarly', 'notQualified'].includes(cat)) byOwner[ownerId].happened++;

    // UTM channel attribution
    const ch = mapUtmToChannel(p.utm_source, p.utm_medium);
    if (ch && byChannel[ch]) {
      byChannel[ch].total++; byChannel[ch][cat] = (byChannel[ch][cat]||0) + 1;
      if (['qualified', 'tooEarly', 'notQualified'].includes(cat)) byChannel[ch].happened++;
    }
  }

  stats.showRate = stats.total > 0 ? (stats.happened / stats.total) * 100 : 0;
  stats.qualRate = stats.happened > 0 ? (stats.qualified / stats.happened) * 100 : 0;
  return { stats, byOwner, byChannel };
}

function analyzeClosedWon(deals) {
  let count = 0, mrr = 0, cycleDays = [];
  const byOwner = {}, byChannel = {};
  for (const deal of deals) {
    const p = deal.properties || {};
    count++;
    const amt = parseFloat(p.amount) || 0;
    mrr += amt;
    if (p.closedate && p.hs_createdate) {
      const days = (new Date(p.closedate).getTime() - new Date(p.hs_createdate).getTime()) / 86400000;
      if (days > 0 && days < 365) cycleDays.push(days);
    }
    const ownerId = p.hubspot_owner_id || 'unassigned';
    if (!byOwner[ownerId]) byOwner[ownerId] = { count:0, mrr:0 };
    byOwner[ownerId].count++; byOwner[ownerId].mrr += amt;

    const ch = mapUtmToChannel(p.utm_source, p.utm_medium);
    if (ch) {
      if (!byChannel[ch]) byChannel[ch] = { count:0, mrr:0 };
      byChannel[ch].count++; byChannel[ch].mrr += amt;
    }
  }
  const avgCycle = cycleDays.length > 0 ? cycleDays.reduce((a, b) => a + b, 0) / cycleDays.length : null;
  return { count, mrr, arr: mrr * 12, avgCycle, byOwner, byChannel };
}

// ═══════════════════════════════════════════════════════════════
// FIRST/LAST TOUCH ATTRIBUTION
// ═══════════════════════════════════════════════════════════════

function mapFirstTouch(source) {
  const s = (source || '').toLowerCase().trim();
  if (s.includes('paid_social') || s.includes('paid social')) return 'Paid Social';
  if (s.includes('paid_search') || s.includes('paid search')) return 'Paid Search';
  if (s.includes('organic_search') || s.includes('organic search')) return 'Organic Search';
  if (s.includes('social') || s.includes('social_media')) return 'Organic Social';
  if (s.includes('email')) return 'Email';
  if (s.includes('referral') || s.includes('referrals')) return 'Referral';
  if (s.includes('direct')) return 'Direct';
  if (s.includes('other')) return 'Other';
  if (!s) return 'Unknown';
  return s.charAt(0).toUpperCase() + s.slice(1);
}

function analyzeAttribution(deals) {
  const firstTouch = {}, lastTouch = {};
  let total = 0, mismatched = 0;

  for (const deal of deals) {
    const p = deal.properties || {};
    total++;
    const ft = mapFirstTouch(p.hs_analytics_source);
    const lt = mapUtmToChannel(p.utm_source, p.utm_medium);
    const ltLabel = lt ? CHANNELS[lt]?.label || lt : 'Unattributed';

    if (!firstTouch[ft]) firstTouch[ft] = 0;
    firstTouch[ft]++;
    if (!lastTouch[ltLabel]) lastTouch[ltLabel] = 0;
    lastTouch[ltLabel]++;

    if (ft !== ltLabel && lt) mismatched++;
  }

  return { firstTouch, lastTouch, total, mismatched };
}

// ═══════════════════════════════════════════════════════════════
// SLACK MESSAGE
// ═══════════════════════════════════════════════════════════════

function buildSlackMessage(tw, lw, mtd, twCW, lwCW, mtdCW, twBooked, lwBooked, mtdBooked, owners, twAds, lwAds, avg4Ads, twAttrib) {
  const f$ = n => '$' + Math.round(n).toLocaleString();
  const fP = n => n.toFixed(1) + '%';
  const delta = (curr, prev) => {
    if (prev === 0 || prev == null) return '';
    const pct = ((curr - prev) / prev) * 100;
    return pct > 5 ? ` \u2191${Math.abs(pct).toFixed(0)}%` : pct < -5 ? ` \u2193${Math.abs(pct).toFixed(0)}%` : ' \u2192';
  };

  const thisWeek = getThisWeek();
  let msg = `*\uD83D\uDCC8 Weekly Pipeline Report \u2014 ${thisWeek.label}*\n`;
  msg += '\u2501'.repeat(40) + '\n';

  // Verdict
  if (twCW.count > 0 && tw.stats.qualRate > 50) {
    msg += `\uD83D\uDFE2 *Strong week* \u2014 ${twCW.count} deal${twCW.count > 1 ? 's' : ''} closed (${f$(twCW.mrr)} MRR), ${fP(tw.stats.qualRate)} qual rate\n`;
  } else if (tw.stats.showRate < 50 || tw.stats.qualRate < 30) {
    msg += `\uD83D\uDD34 *Pipeline needs attention* \u2014 ${fP(tw.stats.showRate)} show rate, ${fP(tw.stats.qualRate)} qual rate\n`;
  } else {
    msg += `\uD83D\uDFE1 *Steady week* \u2014 ${tw.stats.happened} demos happened, ${fP(tw.stats.qualRate)} qual rate\n`;
  }

  // ── 1. CHANNEL PERFORMANCE (unified TOF + MOF) ──
  msg += `\n*1. Channel Performance (This Week vs Last Week)*\n`;
  msg += `\`${'Channel'.padEnd(11)} ${'Spend'.padStart(8)} ${'\u0394'.padStart(5)} ${'Demos'.padStart(5)} ${'CPD'.padStart(6)} ${'Qual'.padStart(4)} ${'CPQD'.padStart(7)} ${'Show%'.padStart(6)}\`\n`;

  let twTotalSpend = 0, twTotalDemos = 0, lwTotalSpend = 0, lwTotalDemos = 0;
  let twTotalQual = 0, twTotalSched = 0, twTotalShown = 0;
  for (const [ch, cfg] of Object.entries(CHANNELS)) {
    const t = twAds[ch] || { spend:0, clicks:0, impressions:0, demos:0, ctr:0, cpc:null, cpa:null };
    const l = lwAds[ch] || { spend:0, clicks:0, impressions:0, demos:0, ctr:0, cpc:null, cpa:null };
    const pipe = tw.byChannel[ch] || { total:0, happened:0, qualified:0 };
    twTotalSpend += t.spend; twTotalDemos += t.demos;
    lwTotalSpend += l.spend; lwTotalDemos += l.demos;
    twTotalQual += pipe.qualified; twTotalSched += pipe.total; twTotalShown += pipe.happened;

    const spD = delta(t.spend, l.spend);
    const cpd = t.demos > 0 ? f$(t.spend / t.demos) : '\u2014';
    const cpqd = pipe.qualified > 0 ? f$(t.spend / pipe.qualified) : '\u2014';
    const showRate = pipe.total > 0 ? fP((pipe.happened / pipe.total) * 100) : '\u2014';
    const showEmoji = pipe.total > 0 ? (pipe.happened / pipe.total >= 0.6 ? '\ud83d\udfe2' : pipe.happened / pipe.total >= 0.4 ? '\ud83d\udfe1' : '\ud83d\udd34') : '';

    msg += `  \`${cfg.label.padEnd(10)} ${f$(t.spend).padStart(8)} ${spD.padStart(5)} ${String(t.demos).padStart(5)} ${cpd.padStart(6)} ${String(pipe.qualified).padStart(4)} ${cpqd.padStart(7)} ${showRate.padStart(6)}\`${showEmoji ? ' ' + showEmoji : ''}\n`;
  }
  // Totals
  const twBlendedCPD = twTotalDemos > 0 ? twTotalSpend / twTotalDemos : null;
  const lwBlendedCPD = lwTotalDemos > 0 ? lwTotalSpend / lwTotalDemos : null;
  const twBlendedCPQD = twTotalQual > 0 ? twTotalSpend / twTotalQual : null;
  const twOverallShow = twTotalSched > 0 ? fP((twTotalShown / twTotalSched) * 100) : '\u2014';
  msg += `\`${'─'.repeat(60)}\`\n`;
  msg += `  \`${'TOTAL'.padEnd(10)} ${f$(twTotalSpend).padStart(8)} ${delta(twTotalSpend, lwTotalSpend).padStart(5)} ${String(twTotalDemos).padStart(5)} ${(twBlendedCPD ? f$(twBlendedCPD) : '\u2014').padStart(6)} ${String(twTotalQual).padStart(4)} ${(twBlendedCPQD ? f$(twBlendedCPQD) : '\u2014').padStart(7)} ${twOverallShow.padStart(6)}\`\n`;
  msg += `_Spend & Demos from Windsor. Qual & Show Rate from HubSpot UTM attribution._\n`;

  // ── 2. PIPELINE — Funnel KPIs (This Week vs Last Week) ──
  msg += `\n*2. Pipeline (This Week vs Last Week)*\n`;
  msg += `\`${'Metric'.padEnd(18)} ${'This Wk'.padStart(8)} ${'Last Wk'.padStart(8)} ${'\u0394'.padStart(6)}\`\n`;
  const kpiRows = [
    ['Demos Booked', twBooked, lwBooked],
    ['Demos to Occur', tw.stats.total, lw.stats.total],
    ['Demos Happened', tw.stats.happened, lw.stats.happened],
    ['Show Rate', tw.stats.showRate, lw.stats.showRate, true],
    ['Qualified', tw.stats.qualified, lw.stats.qualified],
    ['Qual Rate', tw.stats.qualRate, lw.stats.qualRate, true],
    ['Too Early', tw.stats.tooEarly, lw.stats.tooEarly],
    ['Not Qualified', tw.stats.notQualified, lw.stats.notQualified],
    ['Pruned', tw.stats.disqualifiedBefore, lw.stats.disqualifiedBefore],
    ['Closed Won', twCW.count, lwCW.count],
    ['New MRR', twCW.mrr, lwCW.mrr, false, true],
    ['New ARR', twCW.arr, lwCW.arr, false, true],
  ];
  for (const [label, curr, prev, isPct, isDollar] of kpiRows) {
    const currStr = isPct ? fP(curr) : isDollar ? f$(curr) : String(curr);
    const prevStr = isPct ? fP(prev) : isDollar ? f$(prev) : String(prev);
    msg += `\`${label.padEnd(18)} ${currStr.padStart(8)} ${prevStr.padStart(8)} ${delta(curr, prev).padStart(6)}\`\n`;
  }
  if (twCW.avgCycle != null) {
    const twC = twCW.avgCycle.toFixed(0) + 'd', lwC = lwCW.avgCycle != null ? lwCW.avgCycle.toFixed(0) + 'd' : '\u2014';
    msg += `\`${'Avg Deal Cycle'.padEnd(18)} ${twC.padStart(8)} ${lwC.padStart(8)} ${(lwCW.avgCycle != null ? delta(twCW.avgCycle, lwCW.avgCycle) : '').padStart(6)}\`\n`;
  }
  // Blended LTV:CAC and Payback
  const CHURN_RATE = 0.025;
  const LIFETIME_MO = Math.round(1 / CHURN_RATE);
  if (twCW.count > 0) {
    let twTotalAdSpend = 0;
    for (const d of Object.values(twAds)) twTotalAdSpend += d.spend;
    const twCAC = twTotalAdSpend / twCW.count;
    const twAvgMRR = twCW.mrr / twCW.count;
    const twLTV = twAvgMRR * LIFETIME_MO;
    const twRatio = twCAC > 0 ? (twLTV / twCAC).toFixed(1) + ':1' : '\u2014';
    const twPayback = twAvgMRR > 0 ? (twCAC / twAvgMRR).toFixed(1) + 'mo' : '\u2014';
    let lwRatio = '\u2014', lwPayback = '\u2014';
    if (lwCW.count > 0) {
      let lwTotalAdSpend = 0;
      for (const d of Object.values(lwAds)) lwTotalAdSpend += d.spend;
      const lwCAC2 = lwTotalAdSpend / lwCW.count;
      const lwAvgMRR2 = lwCW.mrr / lwCW.count;
      const lwLTV2 = lwAvgMRR2 * LIFETIME_MO;
      lwRatio = lwCAC2 > 0 ? (lwLTV2 / lwCAC2).toFixed(1) + ':1' : '\u2014';
      lwPayback = lwAvgMRR2 > 0 ? (lwCAC2 / lwAvgMRR2).toFixed(1) + 'mo' : '\u2014';
    }
    msg += `\`${'LTV:CAC'.padEnd(18)} ${twRatio.padStart(8)} ${lwRatio.padStart(8)}${' '.repeat(6)}\`\n`;
    msg += `\`${'Payback'.padEnd(18)} ${twPayback.padStart(8)} ${lwPayback.padStart(8)}${' '.repeat(6)}\`\n`;
  }

  // ── Unit Economics:CAC & PAYBACK BY CHANNEL ──
  const CHURN_MO = 0.025; // 2.5% monthly churn
  const AVG_LIFETIME = Math.round(1 / CHURN_MO); // 40 months
  const chsWithDeals = Object.entries(CHANNELS).filter(([ch]) => {
    const cw = twCW.byChannel[ch];
    return cw && cw.count > 0;
  });
  if (chsWithDeals.length) {
    msg += `\n*\ud83d\udcca Unit Economics by Channel (This Week, ${AVG_LIFETIME}mo lifetime @ ${(CHURN_MO*100).toFixed(1)}% churn)*\n`;
    msg += `\`${'Channel'.padEnd(11)} ${'CAC'.padStart(7)} ${'ACV'.padStart(7)} ${'LTV'.padStart(8)} ${'LTV:CAC'.padStart(8)} ${'Payback'.padStart(8)}\`\n`;
    let tCAC = 0, tLTV = 0, tDeals = 0, tSpend = 0, tMRR = 0;
    for (const [ch, cfg] of chsWithDeals) {
      const cw = twCW.byChannel[ch];
      const ad = twAds[ch] || { spend: 0 };
      const cac = ad.spend / cw.count;
      const avgMRR = cw.mrr / cw.count;
      const acv = avgMRR * 12;
      const ltv = avgMRR * AVG_LIFETIME;
      const ratio = cac > 0 ? (ltv / cac).toFixed(1) + ':1' : '\u2014';
      const payback = avgMRR > 0 ? (cac / avgMRR).toFixed(1) + 'mo' : '\u2014';
      const ratioNum = cac > 0 ? ltv / cac : 0;
      const emoji = ratioNum >= 5 ? '\ud83d\udfe2' : ratioNum >= 3 ? '\ud83d\udfe1' : '\ud83d\udd34';
      msg += `  \`${cfg.label.padEnd(10)} ${f$(cac).padStart(7)} ${f$(acv).padStart(7)} ${f$(ltv).padStart(8)} ${ratio.padStart(8)} ${payback.padStart(8)}\`${emoji ? ' ' + emoji : ''}\n`;
      tSpend += ad.spend; tMRR += cw.mrr; tDeals += cw.count;
    }
    // Blended totals
    if (tDeals > 0) {
      const bCAC = tSpend / tDeals, bMRR = tMRR / tDeals, bACV = bMRR * 12, bLTV = bMRR * AVG_LIFETIME;
      const bRatio = bCAC > 0 ? (bLTV / bCAC).toFixed(1) + ':1' : '\u2014';
      const bPayback = bMRR > 0 ? (bCAC / bMRR).toFixed(1) + 'mo' : '\u2014';
      msg += `\`${'\u2500'.repeat(58)}\`\n`;
      msg += `  \`${'BLENDED'.padEnd(10)} ${f$(bCAC).padStart(7)} ${f$(bACV).padStart(7)} ${f$(bLTV).padStart(8)} ${bRatio.padStart(8)} ${bPayback.padStart(8)}\`\n`;
    }
    msg += `_\ud83d\udfe2 LTV:CAC \u2265 5:1 (excellent) \u00b7 \ud83d\udfe1 3\u20135:1 (healthy) \u00b7 \ud83d\udd34 <3:1 (watch)_\n`;
    msg += `_\u26a0\ufe0f LTV assumes ${(CHURN_MO*100).toFixed(1)}% monthly churn (${AVG_LIFETIME}mo avg lifetime) \u2014 update when actual churn data is available._\n`;
  }

  // ── 4. REP PERFORMANCE ──
  const repIds = Object.keys(tw.byOwner).filter(id => id !== 'unassigned');
  if (repIds.length) {
    msg += `\n*3. Rep Performance (This Week)*\n`;
    msg += `\`${'Rep'.padEnd(10)} ${'Demos'.padStart(5)} ${'Show%'.padStart(5)} ${'Qual'.padStart(4)} ${'Q%'.padStart(4)} ${'Early'.padStart(5)} ${'NQ'.padStart(3)} ${'Prun'.padStart(4)} ${'Won'.padStart(3)} ${'ARR'.padStart(8)}\`\n`;

    const repRows = repIds.map(id => {
      const o = tw.byOwner[id];
      const cw = twCW.byOwner[id] || { count:0, mrr:0 };
      const name = (owners[id] || 'Unknown').split(' ')[0];
      return { name, ...o, won: cw.count, wonARR: cw.mrr * 12 };
    }).sort((a, b) => b.qualified - a.qualified || b.won - a.won);

    for (const r of repRows) {
      const showP = r.total > 0 ? ((r.happened / r.total) * 100).toFixed(0) + '%' : '\u2014';
      const qualP = r.happened > 0 ? ((r.qualified / r.happened) * 100).toFixed(0) + '%' : '\u2014';
      msg += `\`${r.name.slice(0,10).padEnd(10)} ${String(r.total).padStart(5)} ${showP.padStart(5)} ${String(r.qualified).padStart(4)} ${qualP.padStart(4)} ${String(r.tooEarly).padStart(5)} ${String(r.notQualified).padStart(3)} ${String(r.disqualifiedBefore).padStart(4)} ${String(r.won).padStart(3)} ${f$(r.wonARR).padStart(8)}\`\n`;
    }
    msg += `_Q% = Qualified \u00f7 Demos Happened (excl. no-shows, cancels, pruned)_\n`;
  }

  // ── 5. MONTH TO DATE ──
  msg += `\n*4. Month to Date (${getMTD().label})*\n`;
  msg += `\`${'Metric'.padEnd(18)} ${'Value'.padStart(10)}\`\n`;
  msg += `\`${'Demos Booked'.padEnd(18)} ${String(mtdBooked).padStart(10)}\`\n`;
  msg += `\`${'Demos to Occur'.padEnd(18)} ${String(mtd.stats.total).padStart(10)}\`\n`;
  msg += `\`${'Demos Happened'.padEnd(18)} ${String(mtd.stats.happened).padStart(10)}\`\n`;
  msg += `\`${'Show Rate'.padEnd(18)} ${fP(mtd.stats.showRate).padStart(10)}\`\n`;
  msg += `\`${'Qualified'.padEnd(18)} ${String(mtd.stats.qualified).padStart(10)}\`\n`;
  msg += `\`${'Qual Rate'.padEnd(18)} ${fP(mtd.stats.qualRate).padStart(10)}\`\n`;
  msg += `\`${'Closed Won'.padEnd(18)} ${String(mtdCW.count).padStart(10)}\`\n`;
  msg += `\`${'MRR'.padEnd(18)} ${f$(mtdCW.mrr).padStart(10)}\`\n`;
  msg += `\`${'ARR'.padEnd(18)} ${f$(mtdCW.arr).padStart(10)}\`\n`;

  // ── 6. BUDGET REALLOCATION ──
  const chCpqds = [];
  for (const [ch, cfg] of Object.entries(CHANNELS)) {
    const pipe = tw.byChannel[ch] || {};
    const ad = twAds[ch] || {};
    if (ad.spend > 100 && (pipe.qualified || 0) > 0) {
      chCpqds.push({ ch, label: cfg.label, spend: ad.spend, qualified: pipe.qualified, cpqd: ad.spend / pipe.qualified });
    }
  }
  if (chCpqds.length >= 2) {
    chCpqds.sort((a, b) => a.cpqd - b.cpqd);
    const best = chCpqds[0], worst = chCpqds[chCpqds.length - 1];
    if (worst.cpqd > best.cpqd * 1.5) {
      const shift = Math.min(worst.spend * 0.2, 2000);
      const addDemos = Math.round(shift / best.cpqd);
      const loseDemos = Math.round(shift / worst.cpqd);
      const netGain = addDemos - loseDemos;
      if (netGain > 0) {
        msg += `\n*5. \uD83D\uDCB0 Budget Reallocation*\n`;
        msg += `Shifting *${f$(shift)}* from *${worst.label}* (${f$(worst.cpqd)}/qualified demo) to *${best.label}* (${f$(best.cpqd)}/qualified demo) could yield *~${netGain} additional qualified demos* at current rates.\n`;
      }
    }
  }

  // ── 7. TREND FLAGS ──
  const flags = [];
  if (avg4Ads) {
    for (const [ch, cfg] of Object.entries(CHANNELS)) {
      const twAd = twAds[ch] || {};
      const avgAd = avg4Ads[ch] || {};
      const twPipe = tw.byChannel[ch] || {};
      const twQ = twPipe.qualified || 0;
      const twCPQD = twQ > 0 ? twAd.spend / twQ : null;
      // 4-week avg CPQD — approximate: total spend / (total qualified / 4 weeks) per week
      const avgSpendWk = avgAd.spend / 4;
      // We don't have 4-week qualified counts from HubSpot (would need separate fetch)
      // Use CPA as proxy for trend comparison
      const twCPA = twAd.cpa;
      const avgCPA = avgAd.demos > 0 ? avgAd.spend / avgAd.demos : null;
      if (twCPA && avgCPA) {
        const avgCPAWk = avgCPA; // 4-week blended CPA (already averaged by total spend/total demos)
        const cpaDrift = (twCPA - avgCPAWk) / avgCPAWk;
        if (cpaDrift > 0.15) {
          flags.push(`\uD83D\uDD34 *${cfg.label}* CPA trending ${(cpaDrift * 100).toFixed(0)}% worse than 4-week avg (${f$(twCPA)} vs ${f$(avgCPAWk)})`);
        }
      }
    }
  }
  if (flags.length) {
    msg += `\n*6. \u26A0\uFE0F Trend Flags*\n`;
    for (const f of flags) msg += f + '\n';
  }

  // ── FIRST/LAST TOUCH ATTRIBUTION ──
  if (twAttrib && twAttrib.total > 0) {
    msg += `\n*\uD83D\uDD00 Attribution: First Touch vs Last Touch (This Week, ${twAttrib.total} demos)*\n`;
    const allSources = new Set([...Object.keys(twAttrib.firstTouch), ...Object.keys(twAttrib.lastTouch)]);
    const sorted = [...allSources].sort((a, b) => (twAttrib.firstTouch[b] || 0) - (twAttrib.firstTouch[a] || 0));
    msg += `\`${'Source'.padEnd(16)} ${'First'.padStart(5)} ${'Last'.padStart(5)} ${'Shift'.padStart(7)}\`\n`;
    for (const src of sorted.slice(0, 8)) {
      const ft = twAttrib.firstTouch[src] || 0;
      const lt = twAttrib.lastTouch[src] || 0;
      const shift = lt - ft;
      const shiftStr = shift > 0 ? '+' + shift : shift < 0 ? String(shift) : '\u2192';
      msg += `\`${src.slice(0,16).padEnd(16)} ${String(ft).padStart(5)} ${String(lt).padStart(5)} ${shiftStr.padStart(7)}\`\n`;
    }
    const mismatchPct = twAttrib.total > 0 ? (twAttrib.mismatched / twAttrib.total * 100).toFixed(0) : 0;
    msg += `_${mismatchPct}% of demos had different first vs last touch. Positive shift = channel converts others' traffic._\n`;
  }

  // ── INSIGHTS ──
  msg += `\n*\uD83D\uDCA1 Insights*\n`;
  let iN = 0;
  if (tw.stats.showRate < 60 && tw.stats.total >= 5 && iN < 3) { iN++; msg += `${iN}. *Show rate at ${fP(tw.stats.showRate)}* \u2014 below 60%. Review no-show follow-up.\n`; }
  if (tw.stats.qualRate < 40 && tw.stats.happened >= 3 && iN < 3) { iN++; msg += `${iN}. *Qual rate at ${fP(tw.stats.qualRate)}* \u2014 below 40%. Check targeting quality.\n`; }
  if (tw.stats.tooEarly > tw.stats.qualified && tw.stats.tooEarly >= 2 && iN < 3) { iN++; msg += `${iN}. *${tw.stats.tooEarly} "Too Early" demos* exceeds qualified count.\n`; }
  if (twCW.count > lwCW.count && twCW.count > 0 && iN < 3) { iN++; msg += `${iN}. *${twCW.count} deal${twCW.count > 1 ? 's' : ''} closed* (${f$(twCW.mrr)} MRR) \u2014 up from ${lwCW.count} last week.\n`; }
  if (twCW.count < lwCW.count && lwCW.count > 0 && iN < 3) { iN++; msg += `${iN}. *Closed deals down* \u2014 ${twCW.count} vs ${lwCW.count} last week.\n`; }
  if (twBooked > lwBooked * 1.2 && twBooked >= 5 && iN < 3) { iN++; msg += `${iN}. *Bookings up* \u2014 ${twBooked} vs ${lwBooked} last week.\n`; }
  if (tw.stats.disqualifiedBefore > 3 && iN < 3) { iN++; msg += `${iN}. *${tw.stats.disqualifiedBefore} demos pruned* \u2014 review lead quality.\n`; }
  if (iN === 0) msg += `1. Pipeline within normal range.\n`;

  if (DASHBOARD_URL) msg += `\n<${DASHBOARD_URL}|\uD83D\uDCC8 Open Full Dashboard>`;
  return msg;
}

// ═══════════════════════════════════════════════════════════════
// SLACK POST
// ═══════════════════════════════════════════════════════════════

async function postToSlack(text) {
  if (!SLACK_WEBHOOK) { console.log('No SLACK_WEBHOOK:\n', text); return; }
  const r = await fetch(SLACK_WEBHOOK, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ text }) });
  if (!r.ok) console.error(`Slack: ${r.status}`);
  else console.log('\u2705 Posted to Slack');
}

// ═══════════════════════════════════════════════════════════════
// MAIN — Sequential HubSpot, parallel Windsor
// ═══════════════════════════════════════════════════════════════

async function main() {
  console.log('\uD83D\uDCC8 Weekly Pipeline Report \u2014 Starting...');

  const thisWeek = getThisWeek();
  const lastWeek = getLastWeek();
  const avg4 = get4WeekAvg();
  const mtdWin = getMTD();

  console.log(`  This Week: ${thisWeek.label}`);
  console.log(`  Last Week: ${lastWeek.label}`);
  console.log(`  4-Week Avg: ${avg4.label}`);
  console.log(`  MTD: ${mtdWin.label}`);

  // Windsor: parallel (separate API, no rate limit conflict)
  console.log('  Fetching Windsor ad data...');
  const [twAds, lwAds, avg4Ads] = await Promise.all([
    fetchAdData(thisWeek.from, thisWeek.to),
    fetchAdData(lastWeek.from, lastWeek.to),
    fetchAdData(avg4.from, avg4.to),
  ]);

  // HubSpot: sequential to avoid 429
  const owners = await fetchOwners();

  console.log('  Fetching this week HubSpot...');
  const twDeals = await fetchPipelineDeals(thisWeek.from, thisWeek.to); await delay(300);
  const twCWDeals = await fetchClosedWon(thisWeek.from, thisWeek.to); await delay(300);
  const twBooked = await fetchDemosBooked(thisWeek.from, thisWeek.to); await delay(300);

  console.log('  Fetching last week HubSpot...');
  const lwDeals = await fetchPipelineDeals(lastWeek.from, lastWeek.to); await delay(300);
  const lwCWDeals = await fetchClosedWon(lastWeek.from, lastWeek.to); await delay(300);
  const lwBooked = await fetchDemosBooked(lastWeek.from, lastWeek.to); await delay(300);

  console.log('  Fetching MTD HubSpot...');
  const mtdDeals = await fetchPipelineDeals(mtdWin.from, mtdWin.to); await delay(300);
  const mtdCWDeals = await fetchClosedWon(mtdWin.from, mtdWin.to); await delay(300);
  const mtdBooked = await fetchDemosBooked(mtdWin.from, mtdWin.to);

  console.log('  Analyzing...');
  const tw = analyzePipeline(twDeals);
  const lw = analyzePipeline(lwDeals);
  const mtd = analyzePipeline(mtdDeals);
  const twCW = analyzeClosedWon(twCWDeals);
  const lwCW = analyzeClosedWon(lwCWDeals);
  const mtdCW = analyzeClosedWon(mtdCWDeals);

  console.log('  Analyzing first/last touch attribution...');
  const twAttrib = analyzeAttribution(twDeals);

  const msg = buildSlackMessage(tw, lw, mtd, twCW, lwCW, mtdCW, twBooked, lwBooked, mtdBooked, owners, twAds, lwAds, avg4Ads, twAttrib);
  console.log('\n' + msg + '\n');
  await postToSlack(msg);

  console.log('\u2705 Weekly Pipeline Report complete');
}

main().catch(e => { console.error('\u274C Fatal error:', e); process.exit(1); });
