/**
 * FrontrowMD — Morning Intelligence Brief
 * Replaces: Daily monitoring, budget pacing, anomaly detection, tracking QA
 * Runs: Every weekday 7:00 AM PT via GitHub Actions
 * Delivers: Slack #paid-media channel
 *
 * Uses same Windsor.ai + HubSpot patterns as report.js
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
              fields: 'date,campaign_name,ad_name,spend,clicks,impressions,ctr,conversions_submit_application_total' },
  linkedin: { connector: 'linkedin', demoField: null, label: 'LinkedIn',
              fields: 'date,campaign,spend,clicks,impressions', campField: 'campaign' },
  tiktok:   { connector: 'tiktok',   demoField: 'conversions', label: 'TikTok',
              fields: 'date,campaign_name,ad_name,spend,clicks,impressions,ctr,conversions' },
  google:   { connector: 'google_ads', demoField: 'conversions', label: 'Google Ads',
              fields: 'date,campaign_name,ad_name,spend,clicks,impressions,ctr,conversions' },
};

const BUDGETS = {
  meta: 116667, linkedin: 34000, tiktok: 26667, google: 16000,
};

const ANOMALY_WARN = 0.20;  // 20% deviation = warning
const ANOMALY_CRIT = 0.40;  // 40% deviation = critical

// ═══════════════════════════════════════════════════════════════
// DATE HELPERS
// ═══════════════════════════════════════════════════════════════

function fmt(d) {
  return `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}`;
}

function getYesterday() {
  const d = new Date(); d.setDate(d.getDate() - 1); return d;
}

function getDaysAgo(n) {
  const d = new Date(); d.setDate(d.getDate() - n); return d;
}

function toMs(dateStr, endOfDay) {
  return new Date(dateStr + (endOfDay ? 'T23:59:59.999' : 'T00:00:00.000')).getTime();
}

// ═══════════════════════════════════════════════════════════════
// WINDSOR.AI FETCH
// ═══════════════════════════════════════════════════════════════

async function windsorFetch(connector, from, to, fields) {
  const url = `https://connectors.windsor.ai/${connector}?api_key=${WINDSOR_KEY}&date_from=${from}&date_to=${to}&fields=${fields}&page_size=5000`;
  for (let attempt = 0; attempt < 2; attempt++) {
    try {
      const r = await fetch(url, { timeout: 30000 });
      if (!r.ok) throw new Error(`Windsor ${connector}: ${r.status}`);
      const d = await r.json();
      const rows = d.data || [];
      console.log(`    Windsor ${connector} (${from}→${to}, fields=${fields.split(',').length}): ${rows.length} rows`);
      return rows;
    } catch (e) {
      if (attempt === 1) { console.error(`Windsor ${connector} failed (timeout or error): ${e.message}`); return []; }
      await new Promise(r => setTimeout(r, 2000));
    }
  }
  return [];
}

async function fetchChannelData(from, to) {
  const results = {};

  for (const [ch, cfg] of Object.entries(CHANNELS)) {
    const rows = await windsorFetch(cfg.connector, from, to, cfg.fields);
    let spend = 0, clicks = 0, impressions = 0, demos = 0;
    const campaigns = {};

    for (const row of rows) {
      const campName = row[cfg.campField || 'campaign_name'] || row.campaign_name || row.campaign || '';
      // Google: filter out YouTube campaigns
      if (ch === 'google' && /\byt\b|youtube/i.test(campName)) continue;

      const rSpend = parseFloat(row.spend) || 0;
      const rClicks = parseInt(row.clicks) || 0;
      const rImpr = parseInt(row.impressions) || 0;
      let rDemos = 0;
      if (cfg.demoField) {
        rDemos = parseFloat(row[cfg.demoField]) || 0;
        if (ch === 'google') rDemos = Math.ceil(rDemos);
        else rDemos = Math.round(rDemos);
      }

      spend += rSpend;
      clicks += rClicks;
      impressions += rImpr;
      demos += rDemos;

      // Track per-campaign for zero-conversion detection
      if (!campaigns[campName]) campaigns[campName] = { spend: 0, impressions: 0, demos: 0, creatives: {} };
      campaigns[campName].spend += rSpend;
      campaigns[campName].impressions += rImpr;
      campaigns[campName].demos += rDemos;

      // Track per-creative within campaign (non-LinkedIn — LinkedIn creatives fetched separately)
      if (ch !== 'linkedin') {
        const adName = row.ad_name || '';
        if (adName) {
          if (!campaigns[campName].creatives[adName]) campaigns[campName].creatives[adName] = { spend: 0, impressions: 0, demos: 0, clicks: 0 };
          campaigns[campName].creatives[adName].spend += rSpend;
          campaigns[campName].creatives[adName].impressions += rImpr;
          campaigns[campName].creatives[adName].demos += rDemos;
          campaigns[campName].creatives[adName].clicks += rClicks;
        }
      }
    }

    // LinkedIn: separate fetch for demos (externalwebsiteconversions) and creatives
    if (ch === 'linkedin') {
      // Demo fetch — separate call to avoid field conflicts
      const liDemoRows = await windsorFetch('linkedin', from, to, 'date,campaign,externalwebsiteconversions');
      for (const row of liDemoRows) {
        const campName = row.campaign || '';
        const rDemos = Math.round(parseFloat(row.externalwebsiteconversions) || 0);
        demos += rDemos;
        if (campaigns[campName]) campaigns[campName].demos += rDemos;
      }

      // Creative fetch — separate call with ad_name (no demo field)
      const liCreativeRows = await windsorFetch('linkedin', from, to, 'date,campaign,ad_name,spend,clicks,impressions');
      for (const row of liCreativeRows) {
        const campName = row.campaign || '';
        const adName = row.ad_name || '';
        if (adName && campaigns[campName]) {
          if (!campaigns[campName].creatives[adName]) campaigns[campName].creatives[adName] = { spend: 0, impressions: 0, demos: 0, clicks: 0 };
          campaigns[campName].creatives[adName].spend += parseFloat(row.spend) || 0;
          campaigns[campName].creatives[adName].impressions += parseInt(row.impressions) || 0;
          campaigns[campName].creatives[adName].clicks += parseInt(row.clicks) || 0;
        }
      }
    }

    const ctr = impressions > 0 ? (clicks / impressions) * 100 : 0;
    const cpd = demos > 0 ? spend / demos : null;

    results[ch] = { spend, clicks, impressions, demos, ctr, cpd, campaigns };
  }

  return results;
}

// ═══════════════════════════════════════════════════════════════
// HUBSPOT FETCH
// ═══════════════════════════════════════════════════════════════

async function hsSearch(objectType, filterGroups, properties) {
  const results = [];
  let after = undefined;
  for (let page = 0; page < 20; page++) {
    const body = { filterGroups, properties, limit: 100 };
    if (after) body.after = after;
    const r = await fetch(`https://api.hubapi.com/crm/v3/objects/${objectType}/search`, {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${HS_TOKEN}`, 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
      timeout: 30000,
    });
    if (!r.ok) { console.error(`HubSpot ${objectType}: ${r.status}`); break; }
    const d = await r.json();
    results.push(...(d.results || []));
    if (!d.paging?.next?.after) break;
    after = d.paging.next.after;
  }
  return results;
}

async function fetchDemosBooked(from, to) {
  const startMs = toMs(from);
  const endMs = toMs(to, true);
  const contacts = await hsSearch('contacts', [{ filters: [
    { propertyName: 'createdate', operator: 'GTE', value: String(startMs) },
    { propertyName: 'createdate', operator: 'LTE', value: String(endMs) },
    { propertyName: 'date_demo_booked', operator: 'HAS_PROPERTY' },
  ]}], ['createdate', 'date_demo_booked']);
  return contacts.length;
}

// ═══════════════════════════════════════════════════════════════
// ANOMALY DETECTION
// ═══════════════════════════════════════════════════════════════

function detectAnomalies(yesterday, rolling7) {
  const alerts = [];

  for (const [ch, yd] of Object.entries(yesterday)) {
    const r7 = rolling7[ch];
    if (!r7) continue;

    // Spend anomaly (with weekend awareness)
    const avgSpend = r7.spend / 7;
    if (avgSpend > 0) {
      const spendDev = Math.abs(yd.spend - avgSpend) / avgSpend;
      const isWeekend = [0, 6].includes(getYesterday().getDay());
      // Raise thresholds on weekends (30% drop is normal)
      const warnThresh = isWeekend ? ANOMALY_CRIT : ANOMALY_WARN;
      const critThresh = isWeekend ? 0.60 : ANOMALY_CRIT;
      if (spendDev > critThresh) alerts.push({ level: '🔴', ch: CHANNELS[ch].label, metric: 'Spend', msg: `$${yd.spend.toFixed(0)} vs $${avgSpend.toFixed(0)} avg (${(spendDev*100).toFixed(0)}% off)${isWeekend ? ' (weekend)' : ''}` });
      else if (spendDev > warnThresh) alerts.push({ level: '🟡', ch: CHANNELS[ch].label, metric: 'Spend', msg: `$${yd.spend.toFixed(0)} vs $${avgSpend.toFixed(0)} avg (${(spendDev*100).toFixed(0)}% off)${isWeekend ? ' (weekend)' : ''}` });
    }

    // Demo anomaly
    const avgDemos = r7.demos / 7;
    if (false) { // replaced by hypothesis-enhanced version above
    }

    // CPD anomaly (higher CPD = worse)
    const avgCPD = r7.demos > 0 ? r7.spend / r7.demos : null;
    if (avgCPD && yd.cpd) {
      const cpdDev = (yd.cpd - avgCPD) / avgCPD;
      if (cpdDev > ANOMALY_WARN) {
        const level = cpdDev > ANOMALY_CRIT ? '🔴' : '🟡';
        // Generate hypothesis for WHY
        const hypotheses = [];
        const avgCTR = r7.impressions > 0 ? (r7.clicks / r7.impressions) * 100 : 0;
        const ydCTR = yd.impressions > 0 ? (yd.clicks / yd.impressions) * 100 : 0;
        if (avgCTR > 0 && ydCTR < avgCTR * 0.75) hypotheses.push('CTR dropped ' + ((1 - ydCTR/avgCTR)*100).toFixed(0) + '% → likely creative fatigue');
        const avgDailyImpr = r7.impressions / 7;
        if (avgDailyImpr > 0 && yd.impressions > avgDailyImpr * 1.3) hypotheses.push('impressions up ' + (((yd.impressions/avgDailyImpr)-1)*100).toFixed(0) + '% → possible audience expansion into lower-quality segments');
        if (avgDailyImpr > 0 && yd.impressions < avgDailyImpr * 0.7) hypotheses.push('impressions dropped ' + ((1-yd.impressions/avgDailyImpr)*100).toFixed(0) + '% → delivery issue or budget cap');
        const isWeekend = [0, 6].includes(getYesterday().getDay());
        if (isWeekend) hypotheses.push('weekend delivery pattern (typically lower quality)');
        // Check for new campaigns (campaigns in yesterday not in rolling 7)
        const ydCamps = Object.keys(yd.campaigns || {});
        const r7Camps = Object.keys(r7.campaigns || {});
        const newCamps = ydCamps.filter(c => !r7Camps.includes(c));
        if (newCamps.length) hypotheses.push(newCamps.length + ' new campaign(s) launched: "' + newCamps[0].slice(0,30) + '"' + (newCamps.length > 1 ? ' +' + (newCamps.length-1) + ' more' : ''));

        let msg = `$${yd.cpd.toFixed(0)} vs $${avgCPD.toFixed(0)} avg (${(cpdDev*100).toFixed(0)}% higher)`;
        if (hypotheses.length) msg += '\n      _Why? ' + hypotheses.join(' · ') + '_';
        alerts.push({ level, ch: CHANNELS[ch].label, metric: 'CPD', msg });
      }
    }

    // Demo anomaly — with hypothesis and weekend awareness
    const avgDemos2 = r7.demos / 7;
    if (avgDemos2 > 0.5) {
      const demoDev2 = (avgDemos2 - yd.demos) / avgDemos2;
      const isWeekendD = [0, 6].includes(getYesterday().getDay());
      // Raise thresholds on weekends (40% demo drop is normal)
      const demoWarn = isWeekendD ? ANOMALY_CRIT : ANOMALY_WARN;
      const demoCrit = isWeekendD ? 0.60 : ANOMALY_CRIT;
      if (demoDev2 > demoWarn) {
        const level = demoDev2 > demoCrit ? '🔴' : '🟡';
        const hypotheses = [];
        if (isWeekendD) hypotheses.push('weekend — demo volume typically 30-50% lower');
        const avgDailySpend = r7.spend / 7;
        if (avgDailySpend > 0 && yd.spend < avgDailySpend * 0.7) hypotheses.push('spend also dropped ' + ((1-yd.spend/avgDailySpend)*100).toFixed(0) + '% — delivery may be constrained');
        const avgCTR = r7.impressions > 0 ? (r7.clicks / r7.impressions) * 100 : 0;
        const ydCTR = yd.impressions > 0 ? (yd.clicks / yd.impressions) * 100 : 0;
        if (avgCTR > 0 && ydCTR < avgCTR * 0.8) hypotheses.push('CTR also declined — creative fatigue or audience saturation');
        let msg = `${yd.demos} vs ${avgDemos2.toFixed(1)} avg (${(demoDev2*100).toFixed(0)}% below)${isWeekendD ? ' (weekend)' : ''}`;
        if (hypotheses.length) msg += '\n      _Why? ' + hypotheses.join(' · ') + '_';
        alerts.push({ level, ch: CHANNELS[ch].label, metric: 'Demos', msg });
      }
    }
    // Zero-conversion tracking QA — with creative-level drill-down
    for (const [campName, camp] of Object.entries(yd.campaigns || {})) {
      if (camp.spend > 50 && camp.impressions > 100 && camp.demos === 0) {
        let msg = `"${campName.slice(0,40)}" — $${camp.spend.toFixed(0)} spent, ${camp.impressions} impr, 0 demos`;
        // Drill into creatives within this campaign
        const crNames = Object.keys(camp.creatives || {});
        if (crNames.length > 0) {
          const topSpenders = crNames
            .map(n => ({ name: n, ...camp.creatives[n] }))
            .filter(c => c.spend > 10)
            .sort((a, b) => b.spend - a.spend)
            .slice(0, 3);
          if (topSpenders.length) {
            msg += '\n      _Creatives: ' + topSpenders.map(c => '"' + c.name.slice(0,30) + '" $' + c.spend.toFixed(0)).join(' · ') + '_';
          }
        }
        alerts.push({ level: '🔴', ch: CHANNELS[ch].label, metric: 'Tracking', msg });
      }
    }
  }

  return alerts;
}

// ═══════════════════════════════════════════════════════════════
// BUDGET PACING
// ═══════════════════════════════════════════════════════════════

function calculatePacing(mtdData) {
  const now = new Date();
  const dayOfMonth = now.getDate() - 1; // completed days
  const daysInMonth = new Date(now.getFullYear(), now.getMonth() + 1, 0).getDate();
  const expectedPct = (dayOfMonth / daysInMonth) * 100;
  const pacing = [];

  for (const [ch, data] of Object.entries(mtdData)) {
    const budget = BUDGETS[ch] || 0;
    if (!budget) continue;
    const actualPct = (data.spend / budget) * 100;
    const diff = actualPct - expectedPct;
    const projected = dayOfMonth > 0 ? (data.spend / dayOfMonth) * daysInMonth : 0;
    const status = Math.abs(diff) <= 7 ? '✅' : diff > 7 ? '⚠️ Over' : '⚠️ Under';

    pacing.push({
      ch: CHANNELS[ch].label,
      budget,
      spent: data.spend,
      actualPct,
      expectedPct,
      diff,
      projected,
      status,
    });
  }

  return { pacing, expectedPct, dayOfMonth, daysInMonth };
}

// ═══════════════════════════════════════════════════════════════
// ITEM 9: META FREQUENCY FETCH
// ═══════════════════════════════════════════════════════════════

async function fetchMetaFrequency(from, to) {
  const rows = await windsorFetch('facebook', from, to, 'date,campaign_name,frequency,spend,impressions');
  const campaigns = {};
  for (const row of rows) {
    const name = row.campaign_name || '';
    const freq = parseFloat(row.frequency) || 0;
    const spend = parseFloat(row.spend) || 0;
    if (!name || freq <= 0) continue;
    if (!campaigns[name]) campaigns[name] = { totalFreqWeighted: 0, totalImpr: 0, spend: 0 };
    const impr = parseInt(row.impressions) || 0;
    campaigns[name].totalFreqWeighted += freq * impr;
    campaigns[name].totalImpr += impr;
    campaigns[name].spend += spend;
  }
  // Compute weighted avg frequency per campaign
  const result = {};
  for (const [name, d] of Object.entries(campaigns)) {
    result[name] = { frequency: d.totalImpr > 0 ? d.totalFreqWeighted / d.totalImpr : 0, spend: d.spend };
  }
  return result;
}

// ═══════════════════════════════════════════════════════════════
// ITEM 7: CAMPAIGN CHANGE DETECTION
// ═══════════════════════════════════════════════════════════════

function detectCampaignChanges(ydData, priorDayData) {
  const changes = { launched: [], paused: [] };

  for (const [ch, yd] of Object.entries(ydData)) {
    const prior = priorDayData[ch];
    if (!prior) continue;
    const ydCamps = Object.keys(yd.campaigns || {});
    const priorCamps = Object.keys(prior.campaigns || {});
    const label = CHANNELS[ch]?.label || ch;

    // New campaigns = in yesterday but not in prior day (with meaningful activity)
    for (const c of ydCamps) {
      if (!priorCamps.includes(c) && (yd.campaigns[c].spend > 20 || yd.campaigns[c].impressions > 500)) {
        changes.launched.push({ ch: label, name: c, spend: yd.campaigns[c].spend, demos: yd.campaigns[c].demos });
      }
    }
    // Went silent = in prior day but not in yesterday (with meaningful prior spend)
    for (const c of priorCamps) {
      if (!ydCamps.includes(c) && prior.campaigns[c].spend > 50) {
        changes.paused.push({ ch: label, name: c, priorSpend: prior.campaigns[c].spend });
      }
    }
  }
  return changes;
}

// ═══════════════════════════════════════════════════════════════
// ITEM 8: UTM HYGIENE AUDIT
// ═══════════════════════════════════════════════════════════════

function auditUTMHygiene(ydData) {
  const issues = [];
  // Expected patterns: campaign names should contain S01, S02, S03 (or retarget/brand)
  const stagePattern = /\b(s01|s02|s03|retarget|brand|remarketing)\b/i;

  for (const [ch, data] of Object.entries(ydData)) {
    const label = CHANNELS[ch]?.label || ch;
    for (const [campName, camp] of Object.entries(data.campaigns || {})) {
      if (camp.spend < 20) continue; // skip low-spend
      // Check for missing stage tag
      if (!stagePattern.test(campName)) {
        issues.push({ ch: label, name: campName, issue: 'Missing stage tag (S01/S02/S03)', spend: camp.spend });
      }
    }
  }
  return issues;
}

// ═══════════════════════════════════════════════════════════════
// CREATIVE NAMING CONVENTION AUDIT
// ═══════════════════════════════════════════════════════════════

function auditCreativeNaming(ydData) {
  // Expected pattern: {Format}_{Talent}_{Concept}_{Setting}_{Variant}
  // Detect known subjects/products to identify "tagged" creatives
  const knownPatterns = [/\baurea\b/i, /\birfan\b/i, /\btalar\b/i, /\bforrest\b/i, /clinician\s*ai/i, /retargeting/i];
  const formats = ['vid','img','car','ugc','video','image','carousel','static'];
  const structuredPattern = new RegExp('^(' + formats.join('|') + ')_', 'i'); // Starts with Format_

  let total = 0, tagged = 0, untaggedSpend = 0;
  const examples = [];

  for (const [ch, data] of Object.entries(ydData)) {
    for (const [campName, camp] of Object.entries(data.campaigns || {})) {
      for (const [adName, cr] of Object.entries(camp.creatives || {})) {
        if (!adName || cr.spend < 5) continue;
        total++;
        const hasKnown = knownPatterns.some(p => p.test(adName));
        const hasStructure = structuredPattern.test(adName);
        if (hasKnown || hasStructure) {
          tagged++;
        } else {
          untaggedSpend += cr.spend;
          examples.push({ name: adName, spend: cr.spend, channel: ch });
        }
      }
    }
  }
  return { total, tagged, untagged: total - tagged, untaggedSpend, examples };
}

// ═══════════════════════════════════════════════════════════════
// ITEM 9: FREQUENCY CAP CHECK
// ═══════════════════════════════════════════════════════════════

function checkFrequencyCaps(freqData) {
  const alerts = [];
  const FREQ_WARN = 6;
  const FREQ_CRIT = 10;

  for (const [campName, data] of Object.entries(freqData)) {
    if (data.frequency >= FREQ_CRIT) {
      alerts.push({ level: '🔴', name: campName, freq: data.frequency, spend: data.spend, msg: `Frequency ${data.frequency.toFixed(1)} — audience seeing ads ${Math.round(data.frequency)} times. Wasted spend + brand damage risk.` });
    } else if (data.frequency >= FREQ_WARN) {
      alerts.push({ level: '🟡', name: campName, freq: data.frequency, spend: data.spend, msg: `Frequency ${data.frequency.toFixed(1)} — approaching saturation. Consider audience expansion or creative refresh.` });
    }
  }
  // Sort by frequency descending
  alerts.sort((a, b) => b.freq - a.freq);
  return alerts;
}

// ═══════════════════════════════════════════════════════════════
// SLACK MESSAGE BUILDER
// ═══════════════════════════════════════════════════════════════

function buildSlackMessage(yesterday, ydDemos, rolling7, mtdData, alerts, pacing, changes, utmIssues, freqAlerts, namingIssues) {
  const ydDate = fmt(getYesterday());
  const f$ = n => '$' + Math.round(n).toLocaleString();
  const fN = n => Math.round(n).toLocaleString();

  // Totals
  let totalSpend = 0, totalDemos = 0;
  for (const d of Object.values(yesterday)) { totalSpend += d.spend; totalDemos += d.demos; }
  const blendedCPD = totalDemos > 0 ? totalSpend / totalDemos : null;

  // Rolling 7 totals
  let r7Spend = 0, r7Demos = 0;
  for (const d of Object.values(rolling7)) { r7Spend += d.spend; r7Demos += d.demos; }
  const r7CPD = r7Demos > 0 ? r7Spend / r7Demos : null;

  const pctDiff = (curr, avg) => {
    if (!avg || avg === 0) return { arrow: '', pct: 0 };
    const pct = ((curr - avg) / avg) * 100;
    const arrow = pct > 5 ? '\u2191' + Math.abs(pct).toFixed(0) + '%' : pct < -5 ? '\u2193' + Math.abs(pct).toFixed(0) + '%' : '\u2192';
    return { arrow, pct };
  };

  // \u2500\u2500 Header \u2500\u2500
  let msg = '*\ud83d\udcca Morning Intelligence Brief \u2014 ' + ydDate + '*\n';
  msg += '\u2501'.repeat(34) + '\n';
  msg += '\ud83d\udccb *HubSpot Demos Booked:*  ' + ydDemos + '\n';
  msg += '\ud83d\udcb0 *Total Ad Spend:*  ' + f$(totalSpend) + '\n';
  msg += '\ud83d\udcca *Total Ad Demos:*  ' + fN(totalDemos) + '\n';
  msg += '\ud83c\udfaf *Blended CPD:*  ' + (blendedCPD ? f$(blendedCPD) : 'N/A') + '\n';

  // \u2500\u2500 One-line verdict \u2500\u2500
  const trackingAlerts = alerts.filter(a => a.metric === 'Tracking');
  const demoAlerts = alerts.filter(a => a.metric === 'Demos');
  const cpdAlerts = alerts.filter(a => a.metric === 'CPD');
  const spendAlerts = alerts.filter(a => a.metric === 'Spend');
  const critAlerts = alerts.filter(a => a.level === '\ud83d\udd34');
  const warnAlerts = alerts.filter(a => a.level === '\ud83d\udfe1');

  if (critAlerts.length >= 3) {
    const chSet = [...new Set(critAlerts.map(a => a.ch))].join(' + ');
    msg += '\n\ud83d\udd34 *' + chSet + ' need attention* \u2014 ' + critAlerts.length + ' critical alerts\n';
  } else if (trackingAlerts.length >= 5) {
    msg += '\n\ud83d\udd34 *' + trackingAlerts.length + ' campaigns with zero tracking* \u2014 check pixel setup\n';
  } else if (alerts.length > 0) {
    msg += '\n\ud83d\udfe1 *' + alerts.length + ' alert' + (alerts.length > 1 ? 's' : '') + ' detected* \u2014 see details below\n';
  } else {
    msg += '\n\ud83d\udfe2 *Normal day* \u2014 all metrics within range\n';
  }

  // \u2500\u2500 Channel Performance Table (with 7d avg columns) \u2500\u2500
  msg += '\n*Channel Performance (Yesterday vs 7-Day Avg)*\n';
  msg += '`' + 'Channel'.padEnd(11) + ' ' + 'Spend'.padStart(7) + ' ' + '7d Avg'.padStart(7) + ' ' + '\u0394'.padStart(5) + '  ' + 'Demos'.padStart(5) + ' ' + '7d Avg'.padStart(6) + ' ' + '\u0394'.padStart(5) + '  ' + 'CPD'.padStart(6) + ' ' + '7d CPD'.padStart(7) + ' ' + '\u0394'.padStart(5) + '`\n';

  const chOrder = ['meta', 'linkedin', 'tiktok', 'google'];
  for (const ch of chOrder) {
    const cfg = CHANNELS[ch];
    const yd = yesterday[ch] || { spend: 0, clicks: 0, impressions: 0, demos: 0, ctr: 0, cpd: null };
    const r7 = rolling7[ch] || { spend: 0, demos: 0 };
    const avgSpend = r7.spend / 7;
    const avgDemos = r7.demos / 7;
    const avgCPD = r7.demos > 0 ? r7.spend / r7.demos : null;
    const spD = pctDiff(yd.spend, avgSpend);
    const dmD = pctDiff(yd.demos, avgDemos);
    const cpdD = yd.cpd && avgCPD ? pctDiff(yd.cpd, avgCPD) : { arrow: '', pct: 0 };

    msg += '`' + cfg.label.padEnd(11) + ' ' + f$(yd.spend).padStart(7) + ' ' + f$(avgSpend).padStart(7) + ' ' + spD.arrow.padStart(5) + '  ' + String(yd.demos).padStart(5) + ' ' + avgDemos.toFixed(1).padStart(6) + ' ' + dmD.arrow.padStart(5) + '  ' + (yd.cpd ? f$(yd.cpd) : '\u2014').padStart(6) + ' ' + (avgCPD ? f$(avgCPD) : '\u2014').padStart(7) + ' ' + cpdD.arrow.padStart(5) + '`\n';
  }

  // Totals row
  const r7AvgSpend = r7Spend / 7;
  const r7AvgDemos = r7Demos / 7;
  const spTD = pctDiff(totalSpend, r7AvgSpend);
  const dmTD = pctDiff(totalDemos, r7AvgDemos);
  const cpdTD = blendedCPD && r7CPD ? pctDiff(blendedCPD, r7CPD) : { arrow: '', pct: 0 };
  msg += '`' + '\u2500'.repeat(75) + '`\n';
  msg += '`' + 'TOTAL'.padEnd(11) + ' ' + f$(totalSpend).padStart(7) + ' ' + f$(r7AvgSpend).padStart(7) + ' ' + spTD.arrow.padStart(5) + '  ' + String(totalDemos).padStart(5) + ' ' + r7AvgDemos.toFixed(1).padStart(6) + ' ' + dmTD.arrow.padStart(5) + '  ' + (blendedCPD ? f$(blendedCPD) : '\u2014').padStart(6) + ' ' + (r7CPD ? f$(r7CPD) : '\u2014').padStart(7) + ' ' + cpdTD.arrow.padStart(5) + '`\n';

  // \u2500\u2500 Budget Pacing (no emoji for on-pace, no projected) \u2500\u2500
  if (pacing.pacing.length) {
    msg += '\n*Budget Pacing \u2014 ' + pacing.expectedPct.toFixed(0) + '% of month elapsed (' + pacing.dayOfMonth + '/' + pacing.daysInMonth + ' days)*\n';
    msg += '`' + 'Channel'.padEnd(12) + ' ' + 'Budget'.padStart(9) + ' ' + 'Spent'.padStart(9) + ' ' + 'Consumed'.padStart(9) + ' ' + 'Projected'.padStart(10) + ' ' + 'vs Pace'.padStart(8) + '`\n';
    for (const p of pacing.pacing) {
      const status = p.diff > 7 ? '\ud83d\udfe1' : p.diff < -7 ? '\ud83d\udd34' : '';
      const diffStr = (p.diff > 0 ? '+' : '') + p.diff.toFixed(0) + '%';
      const projStr = p.projected > 0 ? f$(p.projected) : '\u2014';
      const projEmoji = p.projected > p.budget * 1.1 ? ' \u26a0\ufe0f' : p.projected < p.budget * 0.85 ? ' \u2193' : '';
      msg += '  `' + p.ch.padEnd(11) + ' ' + f$(p.budget).padStart(9) + ' ' + f$(p.spent).padStart(9) + ' ' + (p.actualPct.toFixed(0) + '%').padStart(9) + ' ' + projStr.padStart(10) + projEmoji + ' ' + diffStr.padStart(8) + '`' + (status ? ' ' + status : '') + '\n';
    }
    // Total projected
    const totalBudget = pacing.pacing.reduce((s, p) => s + p.budget, 0);
    const totalSpentMTD = pacing.pacing.reduce((s, p) => s + p.spent, 0);
    const totalProjected = pacing.pacing.reduce((s, p) => s + p.projected, 0);
    const totalConsumed = totalBudget > 0 ? (totalSpentMTD / totalBudget * 100).toFixed(0) + '%' : '\u2014';
    msg += '`' + '\u2500'.repeat(72) + '`\n';
    msg += '  `' + 'TOTAL'.padEnd(11) + ' ' + f$(totalBudget).padStart(9) + ' ' + f$(totalSpentMTD).padStart(9) + ' ' + totalConsumed.padStart(9) + ' ' + f$(totalProjected).padStart(10) + ' `\n';
  }

  // \u2500\u2500 Alerts (grouped, capped, actions first) \u2500\u2500
  if (alerts.length) {
    // Build recommendations FIRST
    msg += '\n*\ud83d\udca1 Recommended Actions*\n';
    let recNum = 0;
    if (trackingAlerts.length && recNum < 3) {
      recNum++;
      const trackByChannel = {};
      trackingAlerts.forEach(a => { if (!trackByChannel[a.ch]) trackByChannel[a.ch] = { count: 0, spend: 0 }; trackByChannel[a.ch].count++; trackByChannel[a.ch].spend += parseFloat(a.msg.match(/\$([\d,]+)/)?.[1]?.replace(',','') || 0); });
      const trackSummary = Object.entries(trackByChannel).map(([ch, d]) => ch + ': ' + d.count + ' campaigns (' + f$(d.spend) + ')').join(', ');
      msg += recNum + '. *Investigate tracking* \u2014 ' + trackingAlerts.length + ' campaign(s) with $0 demos. ' + trackSummary + '\n';
    }
    if (demoAlerts.length && recNum < 3) {
      recNum++;
      const w = demoAlerts[0];
      msg += recNum + '. *Review ' + w.ch + ' demo volume* \u2014 ' + w.msg + '. Consider creative refresh or audience expansion.\n';
    }
    if (cpdAlerts.length && recNum < 3) {
      recNum++;
      const w = cpdAlerts[0];
      msg += recNum + '. *Optimize ' + w.ch + ' efficiency* \u2014 ' + w.msg + '. Review underperforming campaigns for pause/reallocate.\n';
    }
    if (spendAlerts.length && recNum < 3) {
      recNum++;
      const w = spendAlerts[0];
      msg += recNum + '. *Monitor ' + w.ch + ' spend* \u2014 ' + w.msg + '. Check for paused campaigns or delivery issues.\n';
    }
    if (recNum === 0) {
      msg += '1. Monitor current trends \u2014 anomalies detected but no urgent action needed.\n';
    }

    // Then grouped alerts
    msg += '\n*\u26a0\ufe0f Alerts (' + alerts.length + ')*\n';
    // Group tracking alerts by channel
    const nonTracking = alerts.filter(a => a.metric !== 'Tracking');
    const trackByChannel = {};
    trackingAlerts.forEach(a => {
      if (!trackByChannel[a.ch]) trackByChannel[a.ch] = { count: 0, spend: 0 };
      trackByChannel[a.ch].count++;
      const spendMatch = a.msg.match(/\$([\d,]+)\s+spent/);
      trackByChannel[a.ch].spend += parseFloat((spendMatch?.[1] || '0').replace(',',''));
    });
    // Show grouped tracking alerts
    for (const [ch, d] of Object.entries(trackByChannel)) {
      msg += '\ud83d\udd34 *' + ch + '* \u2014 ' + d.count + ' campaign' + (d.count > 1 ? 's' : '') + ' spending with 0 demos (' + f$(d.spend) + ' total)\n';
    }
    // Show non-tracking alerts (capped at 5)
    const shown = nonTracking.slice(0, 5);
    for (const a of shown) {
      msg += a.level + ' *' + a.ch + '* \u2014 ' + a.metric + ': ' + a.msg + '\n';
    }
    if (nonTracking.length > 5) {
      msg += '_...and ' + (nonTracking.length - 5) + ' more (see dashboard)_\n';
    }
  } else {
    msg += '\n\ud83d\udfe2 *No alerts* \u2014 all metrics within normal range.\n';
  }

  // ── ITEM 7: What Changed (campaign launches/pauses) ──
  if (changes && (changes.launched.length || changes.paused.length)) {
    msg += '\n\n*\ud83d\udd04 What Changed*\n';
    if (changes.launched.length) {
      msg += '_New campaigns detected:_\n';
      for (const c of changes.launched.slice(0, 5)) {
        msg += '\ud83d\udfe2 `' + c.ch + '` *' + c.name.slice(0, 40) + '* \u2014 ' + f$(c.spend) + ' spend, ' + c.demos + ' demos\n';
      }
      if (changes.launched.length > 5) msg += '_...+' + (changes.launched.length - 5) + ' more_\n';
    }
    if (changes.paused.length) {
      msg += '_Campaigns went dark:_\n';
      for (const c of changes.paused.slice(0, 5)) {
        msg += '\ud83d\udd34 `' + c.ch + '` *' + c.name.slice(0, 40) + '* \u2014 was spending ' + f$(c.priorSpend) + '/day\n';
      }
      if (changes.paused.length > 5) msg += '_...+' + (changes.paused.length - 5) + ' more_\n';
    }
  }

  // ── ITEM 9: Frequency Cap Monitoring ──
  if (freqAlerts && freqAlerts.length) {
    msg += '\n*\ud83d\udd01 Meta Frequency Alerts (' + freqAlerts.length + ')*\n';
    for (const fa of freqAlerts.slice(0, 5)) {
      msg += fa.level + ' "' + fa.name.slice(0, 35) + '" \u2014 ' + fa.msg + ' (' + f$(fa.spend) + ')\n';
    }
    if (freqAlerts.length > 5) msg += '_...+' + (freqAlerts.length - 5) + ' more_\n';
  }

  // ── Creative Naming Convention ──
  if (namingIssues && namingIssues.total > 0 && namingIssues.untagged > 0) {
    const pct = Math.round((namingIssues.tagged / namingIssues.total) * 100);
    msg += '\n*\ud83c\udff7\ufe0f Creative Naming: ' + pct + '% compliant* \u2014 ' + namingIssues.untagged + ' of ' + namingIssues.total + ' creatives (' + f$(namingIssues.untaggedSpend) + ') untagged.\n';
    msg += '_Adopt: Format_Talent_Concept_Setting_Variant (e.g. Vid_Aurea_TrustHook_Studio_v1)_\n';
  }

  if (DASHBOARD_URL) {
    msg += '\n<' + DASHBOARD_URL + '|\ud83d\udcca Open Full Dashboard>';
  }

  return msg;
}

// ═══════════════════════════════════════════════════════════════
// SLACK POST
// ═══════════════════════════════════════════════════════════════

async function postToSlack(text) {
  if (!SLACK_WEBHOOK) { console.log('No SLACK_WEBHOOK — printing to console:\n', text); return; }
  const r = await fetch(SLACK_WEBHOOK, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ text }),
    timeout: 15000,
  });
  if (!r.ok) console.error(`Slack post failed: ${r.status}`);
  else console.log('✅ Posted to Slack');
}

// ═══════════════════════════════════════════════════════════════
// MAIN
// ═══════════════════════════════════════════════════════════════

async function main() {
  console.log('🌅 Morning Intelligence Brief — Starting...');
  const yd = fmt(getYesterday());
  const r7From = fmt(getDaysAgo(8));
  const r7To = fmt(getDaysAgo(2));
  const mtdFrom = `${new Date().getFullYear()}-${String(new Date().getMonth()+1).padStart(2,'0')}-01`;

  console.log(`  Yesterday: ${yd}`);
  console.log(`  Rolling 7: ${r7From} to ${r7To}`);
  console.log(`  MTD: ${mtdFrom} to ${yd}`);

  // Fetch data
  console.log('  Fetching Windsor data...');
  const dayBefore = fmt(getDaysAgo(2));
  const [ydData, r7Data, mtdData, priorDayData] = await Promise.all([
    fetchChannelData(yd, yd),
    fetchChannelData(r7From, r7To),
    fetchChannelData(mtdFrom, yd),
    fetchChannelData(dayBefore, dayBefore),
  ]);

  // Item 9: Fetch Meta frequency data
  console.log('  Fetching Meta frequency data...');
  const freqData = await fetchMetaFrequency(yd, yd);

  console.log('  Fetching HubSpot demos...');
  const ydDemos = await fetchDemosBooked(yd, yd);

  // Analyze
  console.log('  Running anomaly detection...');
  const alerts = detectAnomalies(ydData, r7Data);

  // Item 7: Detect campaign changes
  console.log('  Detecting campaign changes...');
  const changes = detectCampaignChanges(ydData, priorDayData);

  // Item 8: UTM hygiene audit
  console.log('  Running UTM hygiene audit...');
  const utmIssues = auditUTMHygiene(ydData);

  // Item 9: Frequency cap check
  console.log('  Checking frequency caps...');
  const freqAlerts = checkFrequencyCaps(freqData);

  console.log('  Calculating budget pacing...');
  const pacing = calculatePacing(mtdData);

  // Creative naming convention audit
  console.log('  Auditing creative naming convention...');
  const namingIssues = auditCreativeNaming(ydData);

  // Build & send
  const msg = buildSlackMessage(ydData, ydDemos, r7Data, mtdData, alerts, pacing, changes, utmIssues, freqAlerts, namingIssues);
  console.log('\n' + msg + '\n');
  await postToSlack(msg);

  console.log('✅ Morning Brief complete');
}

main().catch(e => { console.error('❌ Fatal error:', e); process.exit(1); });
