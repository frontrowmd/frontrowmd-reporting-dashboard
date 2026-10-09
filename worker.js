// ============================================================================
// FrontrowMD Dashboard API — Cloudflare Worker (v3)
// ============================================================================
// Matches FrontrowMD-Metrics-Reference-Guide.md as source of truth.
// POST /api/data — returns structured JSON for all dashboard sections.
// ============================================================================

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const DASH_CHANNELS = ['meta', 'clinician', 'linkedin', 'google', 'reddit', 'chatgpt', 'tiktok'];

// "Clinician" is not a separate ad platform — it's the Meta campaigns whose name
// contains "Clinician Targeting", split out into their own channel. Those rows
// are removed from Meta entirely (spend, stats, budget) so nothing is counted
// twice.
function isClinicianCampaign(campaignName) {
  return /clinician\s*targeting/i.test(campaignName || '');
}
// Channels deliberately EXCLUDED from every roll-up KPI (total ad spend, CPD,
// CPQD, CAC, ROAS, budget pacing) on both dashboards. They still render as their
// own row in Channel Performance and get their own ad-table tabs — they just
// don't move the headline numbers.
// Windsor's connectors for these channels expose NO conversion metric at all —
// not a zero, an absent field (verified 2026-08-17: requesting `conversions` on
// openai_ads returns "Unexpected field(s)"). Reporting 0 would assert something
// false, so the client renders these as "—" instead.
const CHANNELS_NO_CONV = new Set(['reddit', 'chatgpt']);

// Stopgap while Windsor request 70111764 is open: conversions read by hand from
// the ad platform's own reporting for the channels above. Update `asOf` and the
// range whenever the figures are re-read — the UI shows both, so a stale entry is
// visible rather than silent. Delete a channel's entry once its connector reports
// the metric and the number goes back to coming from the API.
// Keyed by channel, then a LIST of periods — a channel gets one entry per range
// that was read by hand. It used to be a single object, which could only ever
// describe one range: asserting "September was zero" would have meant either
// overwriting August's real figures or stretching August's range over a month it
// never covered. Neither is true, hence the list.
const MANUAL_CONVERSIONS = {
  chatgpt: [
  {
    asOf: '2026-08-24',
    from: '2026-08-04', to: '2026-08-24',   // the range these figures cover
    // Read from OpenAI Ads' own Campaigns tab. Totals 10.
    byCampaign: {
      'Health Brand Marketing Strategy': 4,
      'Clinician Reviews / ClinicianAI': 3,
      'Clinician Backing': 3,
      'Chatgpt - Broader - Conversions': 0,
    },
    // Ads tab. Note "Health Brand Marketing Strategy" exists as a campaign, an
    // ad group AND an ad, with DIFFERENT counts at each level (4 / 4 / 0) —
    // which is why these are three separate maps rather than one keyed by name.
    // Two ads share the name "Suggested ad for Clinician Backing"; the creative
    // aggregation merges same-named rows, and both are 0, so the total holds.
    // Totals 10.
    byAd: {
      'HealthBrandMarketing Strategy Ad 2': 3,
      'Leverage clinicians to support your Shoppers': 3,
      'Clinician Reviews': 2,
      'Suggested ad for Health Brand Marketing Strategy': 1,
      'Clinician AI Ad': 1,
      'Conversion Ad 1': 0,
      'Suggested ad for Clinician Backing': 0,
      'Suggested ad for Clinician Reviews / ClinicianAI': 0,
      'Health Brand Marketing Strategy': 0,
    },
    // Ad-group tab. Names differ from the campaign names, and one campaign
    // splits across two groups (Clinician Reviews / ClinicianAI = Clinician
    // Reviews 2 + Clinician AI 1), so this cannot be derived from byCampaign.
    // Also totals 10, which is the cross-check that the two reads agree.
    byAdGroup: {
      'Health Brand Marketing Strategy': 4,
      'Clinician Backing for Health Brands': 3,
      'Clinician Reviews': 2,
      'Clinician AI': 1,
      'Broader Health and Wellness Marketers': 0,
    },
  },
  {
    // September 2026: ZERO conversions, confirmed by Alex 2026-09-17.
    // This is an assertion, not an absence. Without it the channel falls back to
    // "\u2014" (we have no figure), which reads as a gap in the data rather than
    // as the real answer. zeroAll forces every campaign / ad set / ad group row
    // to 0 instead of relying on them happening to be 0 already — so if
    // openai_ads ever starts returning a conversions field mid-month, this keeps
    // asserting what was actually read rather than silently mixing the two.
    asOf: '2026-09-17',
    from: '2026-09-01', to: '2026-09-30',
    zeroAll: true,
    byCampaign: {}, byAd: {}, byAdGroup: {},
  },
  ],
};
// Only surfaces the figures when the selected window overlaps the range they were
// read for. Outside it there is genuinely nothing counted, so the channel falls
// back to "—" rather than presenting these numbers as data for another period.
// Stamps the hand-entered per-campaign figures onto an aggRows() result, so the
// Campaigns and Ad Sets tables show the same numbers as the channel row. Rows the
// manual list doesn't name keep their (zero) value — a campaign that started after
// the figures were read should read 0, not inherit someone else's count.
function _applyManualConv(agg, ch, from, to) {
  const m = manualConv(ch, from, to);
  if (!m || !agg) return agg;
  let total = 0;
  const stamp = (rows) => {
    for (const r of (Array.isArray(rows) ? rows : Object.values(rows || {}))) {
      if (m.zeroAll) { r.demos = 0; continue; }
      if (Object.prototype.hasOwnProperty.call(m.byCampaign, r.name)) r.demos = m.byCampaign[r.name];
    }
  };
  stamp(agg.campaigns); stamp(agg.adsets);
  for (const r of Object.values(agg.campaigns || {})) total += r.demos || 0;
  if (agg.totals) {
    agg.totals.demos = total;
    agg.totals.cpd = total > 0 ? agg.totals.spend / total : null;
  }
  agg.manualConv = { asOf: m.asOf, from: m.from, to: m.to };
  return agg;
}

// Audiences rows are keyed by the connector's nameField — `ad_group` for
// openai_ads — so the hand-read ad-group conversions are stamped here rather
// than in _applyManualConv, which only sees campaign/ad-set aggregates.
function _applyManualConvAudiences(rows, ch, from, to) {
  const m = manualConv(ch, from, to);
  if (!m || !Array.isArray(rows)) return rows;
  if (m.zeroAll) { for (const r of rows) { if (r) r.demos = 0; } return rows; }
  if (!m.byAdGroup) return rows;
  for (const r of rows) {
    if (r && Object.prototype.hasOwnProperty.call(m.byAdGroup, r.name)) r.demos = m.byAdGroup[r.name];
  }
  return rows;
}

function manualConv(ch, from, to) {
  const periods = MANUAL_CONVERSIONS[ch];
  if (!Array.isArray(periods) || !periods.length) return null;
  // Without a window there is no way to tell which period applies, and the
  // overlap test below would exclude nothing — silently merging every hand-read
  // month into one total. That is exactly how the August figures leaked into
  // September. A missing window is "unknown", which renders as an em dash.
  if (!from || !to) return null;
  // Every period the window touches, not just the first: a YTD or custom range
  // can span several hand-read months, and returning one of them would report a
  // single month's figures as the whole window's.
  const hit = periods.filter(m => !((to && to < m.from) || (from && from > m.to)));
  if (!hit.length) return null;
  const merge = (key) => {
    let any = false; const out = {};
    for (const m of hit) {
      if (!m[key]) continue;
      any = true;
      for (const [k, v] of Object.entries(m[key])) out[k] = (out[k] || 0) + v;
    }
    return any ? out : null;
  };
  const byCampaign = merge('byCampaign') || {};
  const total = Object.values(byCampaign).reduce((a, b) => a + b, 0);
  return {
    total,
    // Latest read wins for the stamp; the range spans everything merged.
    asOf: hit.map(m => m.asOf).sort().pop(),
    from: hit.map(m => m.from).sort()[0],
    to:   hit.map(m => m.to).sort().pop(),
    byCampaign,
    byAdGroup: merge('byAdGroup'),
    byAd: merge('byAd'),
    // Only when EVERY merged period is a zero assertion. A window spanning a
    // zero month and a non-zero one must not force the non-zero rows to 0.
    zeroAll: hit.every(m => !!m.zeroAll),
  };
}
const KPI_EXCLUDED_CHANNELS = new Set(['clinician']);
// The Clinician Targeting campaign has optimized for TWO different custom
// conversions over its life, so reading only one loses an entire era:
//   * Jul-Aug 2026: "CAPI -- Clinicians"        (id 1403040218341205)
//   * Sep 2026 on:  "CAPI -- Virtual Clinician" (id 2367172530757383)
// The campaign was renamed to "...Official Test - CAPI" and repointed around
// Sept 1 2026; all four of its ad sets now target the Virtual Clinician id
// (verified against adset_promoted_object via the Windsor API, and against the
// custom-conversion definition table). The two columns are MUTUALLY EXCLUSIVE
// per row -- an August row populates only the old one, a September row only the
// new one -- so a plain SUM is correct AND history-preserving. A straight swap
// would zero out every August CPD / CPS / CAC and the Weekly CPD chart.
// Add the next id here (and to META_CUSTOM_CONVERSIONS) if it is repointed again.
const CLINICIAN_CONV_FIELDS = ['actions_capi_clinicians', 'actions_capi_virtual_clinician'];
// The CURRENT conversion, for anything that needs one name rather than the pair.
const CLINICIAN_CONV_FIELD = 'actions_capi_virtual_clinician';
// Conversions a CLINICIAN row reported. Clinician ad sets do NOT all optimize
// for the same thing -- as of Oct 2026 they split across the custom conversion
// "CAPI -- Virtual Clinician" and the RAW pixel event "Clinician", and Meta
// reports Results per ad set accordingly. Counting one fixed pair of columns
// therefore both over- and under-counted: it added conversions an ad set wasn't
// optimizing for, and missed the "Clinician" event entirely (no custom
// conversion wraps it, so Windsor exposes no column -- verified 2026-10-07).
// So: follow THIS row's own target, and let a campaign total be the sum of its
// ad sets' targets. The two-era pair survives only as the fallback for rows
// fetched below ad-set grain, which carry no adset_promoted_object.
function clinicianConvCount(row) {
  const t = metaTargetCount(row);
  if (t >= 0) return t;
  let n = 0;
  for (const f of CLINICIAN_CONV_FIELDS) n += parseFloat(row[f]) || 0;
  return Math.round(n);
}
// A channel's demoField is either ONE Windsor column or a LIST of columns to sum
// (Clinician spans two custom conversions). One reader handles both shapes so
// every aggregation path counts the same way.
function demoRawOf(row, demoField) {
  if (!demoField) return 0;
  if (Array.isArray(demoField)) {
    let n = 0;
    for (const f of demoField) n += parseFloat(row[f]) || 0;
    return n;
  }
  return parseFloat(row[demoField]) || 0;
}
// Meta and Clinician share the facebook connector, so both need the Meta-only
// field set on every fetch.
function isMetaConnectorCh(ch) { return ch === 'meta' || ch === 'clinician'; }
// Demos one row contributes to a channel. Meta AND Clinician both follow the ad
// set's own optimization target (a campaign can span several conversion types,
// and Meta reports Results per ad set); every other channel reads its single
// configured column. One reader so the Campaigns, Ad Sets and Creatives tables
// can never drift apart on what a conversion is.
function chDemoCount(ch, cfg, row) {
  if (ch === 'meta') return metaDemos(row);
  if (ch === 'clinician') return clinicianConvCount(row);
  const d = demoRawOf(row, (cfg && cfg.demoField) || null);
  return (ch === 'google' || ch === 'youtube') ? Math.ceil(d) : Math.round(d);
}

const CHANNEL_LABELS = { meta: 'Meta', clinician: 'Clinician', google: 'Google', linkedin: 'LinkedIn', tiktok: 'TikTok', chatgpt: 'ChatGPT', reddit: 'Reddit', youtube: 'YouTube' };

// Channels with no Windsor.ai (or equivalent) connector wired up yet — we
// still want them visible in the Channel Performance table so the budget
// allocation is transparent, but spend/demos/qual/won/ARR are all "—" until
// the integration ships. Dashboard reads `pendingConnector: true` on the
// channel payload and renders non-Budget cells as em-dashes; Total row
// skips them. (Field name avoids collision with the analyzer payload's
// existing `pending` demo-status count.)
// Reddit + OpenAI Ads connectors went live 2026-08, so they now report real
// spend and are no longer stubs. Note neither exposes a conversion metric via
// the shared /all endpoint, so their Demos/CPD/CPQD read 0 — that's a source
// limitation, not a missing connector (see processAdSpend).
const PENDING_CHANNELS = new Set([]);

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
  '2026-07': { meta: 43080, linkedin: 13260, google: 7215, tiktok: 1445, youtube: 0, chatgpt: 0 },
  // August plan — Meta scaled up, LinkedIn/Google keep-alive, and new Reddit +
  // ChatGPT (OpenAI) tests. TikTok/YouTube off. Total = $42,500.
  '2026-08': { meta: 25500, clinician: 20000, linkedin: 5000, google: 2000, reddit: 5000, chatgpt: 5000, tiktok: 0, youtube: 0 },
  // September plan, set 2026-09-03. These FIVE channels are the entire month and
  // total exactly $50,000: Meta $20,080, LinkedIn $4,920, Clinician $10,000,
  // Google $10,000, Reddit $5,000. Every other channel gets $0 — that is an
  // explicit instruction, not an oversight, so do NOT carry a prior month's
  // number forward into chatgpt/tiktok/youtube here. ChatGPT ran $5,000 in
  // August and is switched off for September.
  '2026-09': { meta: 20080, clinician: 10000, linkedin: 4920, google: 10000, reddit: 5000, chatgpt: 0, tiktok: 0, youtube: 0 },
  // October plan, set 2026-10-02. THREE funded lines totalling $21,000:
  // Meta Brand $12,000, Meta Clinician $6,000, Google $3,000. LinkedIn, ChatGPT
  // and Reddit are explicitly $0 for the month — that is the instruction, not an
  // omission, so do NOT carry September's numbers forward into them.
  // Note the roll-up total differs from the $21,000 plan: `clinician` is in
  // KPI_EXCLUDED_CHANNELS (worker.js:176), so every KPI budget — including the
  // Ad Spend pacing card — sees $15,000 (Meta + Google).
  '2026-10': { meta: 12000, clinician: 6000, linkedin: 0, google: 3000, reddit: 0, chatgpt: 0, tiktok: 0, youtube: 0 },
};
// Pinned to a literal month, this silently paced every unlisted month against a
// plan that kept getting older — August's $42,500 would have stood in for
// November. Taking the newest key means a missing month inherits the most recent
// plan instead, which is the least wrong default. It is still a fallback: the
// log says when one is used, so a gap is visible rather than plausible.
const BUDGET_FALLBACK = BUDGET_BY_MONTH[Object.keys(BUDGET_BY_MONTH).sort().pop()];
function getBudgetsForMonth(dateStr) {
  if (!dateStr) return BUDGET_FALLBACK;
  const key = dateStr.slice(0, 7);
  const hit = BUDGET_BY_MONTH[key];
  if (hit) return hit;
  console.warn(`budget: no plan for ${key} — falling back to ${Object.keys(BUDGET_BY_MONTH).sort().pop()}. Add the month to BUDGET_BY_MONTH.`);
  return BUDGET_FALLBACK;
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

// ── Demos source labeling (subtext under Demos in the ad tables) ─────────────
// Returns the FULL subtext label (incl. any prefix) for a campaign / ad set, or
// '' for none. CAPI custom conversions run only on the TEST campaigns:
//   • "Combined Ad Sets - Blended Conversion" → one blended Demo+Webinar
//     conversion for all its ad sets.
//   • "Skincare & Beauty Brands" (the TEST one) → per ad set: "…- Demo" ad set →
//     Demo, "…- Webinar" ad set → Webinar. The 'test' gate excludes the
//     production "…- 020626" Skincare campaign, which does NOT use CAPI.
// Separately, the S02 MOF lead-gen retargeting campaign reports Meta Lead Forms
// (not a CAPI conversion) → "Lead Forms". Extend as new campaigns launch.
function capiConvLabel(campaignName, adsetName) {
  const c = (campaignName || '').toLowerCase();
  const a = (adsetName || '').toLowerCase();
  if (c.includes('test')) {
    if (c.includes('combined ad sets') || c.includes('blended conversion')) return 'CAPI · Demo + Webinar';
    if (c.includes('skincare & beauty brands') || c.includes('skincare & clean beauty')) {
      if (a.includes('webinar')) return 'CAPI · Webinar';
      if (a.includes('demo')) return 'CAPI · Demo';
      return 'CAPI · Demo + Webinar';  // campaign row / unknown ad set → spans both
    }
  }
  if (isMetaLeadAsDemoCampaign(campaignName)) return 'Lead Forms';
  return '';
}

// Windsor scalar fields for the CAPI custom conversions (the demo/webinar
// "Results" for campaigns that optimize for a CAPI custom conversion instead of
// the submit-application pixel event). Appended to Meta fetches.
// ── Meta optimization target (real "conversion result name") ────────────────
// Windsor exposes the account's custom conversions two ways, and BOTH are
// needed because neither is complete on its own:
//   • one flat NUMERIC column per custom conversion (actions_capi_*) — safe to
//     add to a spend query, no row multiplication;
//   • the configured target id inside adset_promoted_object (ad-set grain).
// The human-readable NAME lives only in the Windsor field LABEL — there is no
// dimension that returns it alongside spend (custom_conversion_action_name is a
// dimension that fans rows out ~36x and inflates spend), so the id→name map is
// mirrored here. Verified verbatim against get_fields on the facebook connector.
// NOTE: several names exist under multiple ids in Meta (e.g. "CAPI -- Demo +
// Webinar" has 4) and only the id below carries an actions_* column. A
// duplicate id therefore has no column of its own; META_CC_ALIAS maps it onto
// the one that does, so keep this list ONE ENTRY PER COLUMN and put duplicates
// there instead.
const META_CUSTOM_CONVERSIONS = [
  { field: 'actions_capi_clinicians',                 id: '1403040218341205',  label: 'CAPI -- Clinicians' },
  { field: 'actions_capi_virtual_clinician',          id: '2367172530757383',  label: 'CAPI -- Virtual Clinician' },
  { field: 'actions_capi_demo_webinar_ebook_disqual', id: '27583605861331174', label: 'CAPI -- Demo + Webinar + Ebook + Disqual' },
  { field: 'actions_capi_disqual',                    id: '2138656430023236',  label: 'CAPI -- Disqual' },
  { field: 'actions_capi_ebook',                      id: '990268934047832',   label: 'CAPI -- Ebook' },
  { field: 'actions_capi_demo_webinar',               id: '1489552102408235',  label: 'CAPI -- Demo + Webinar' },
  { field: 'actions_capi_webinar',                    id: '986373240899325',   label: 'CAPI -- Webinar' },
  { field: 'actions_capi_demo',                       id: '910686358708623',   label: 'CAPI -- Demo' },
  { field: 'actions_combined_demos_webinars',         id: '1028010476340460',  label: 'Combined -- Demos + Webinars' },
  { field: 'actions_fake_test',                       id: '2352065058650788',  label: 'Fake test' },
];
// Standard (non-custom) pixel events → the Windsor column that counts them.
// Only events we actually fetch belong here; an event with no column must stay
// absent so metaTargetField falls back rather than naming a field that is always
// undefined (which would silently read as zero conversions).
const META_STD_EVENT_FIELD = {
  SCHEDULE: 'conversions_schedule_total',
  SUBMIT_APPLICATION: 'conversions_submit_application_total',
};
// RAW custom pixel event → the Windsor column that happens to count the same
// thing. Only needed as a FALLBACK for rows fetched without the `conversions`
// array; where a custom conversion wraps the event, its column and the event
// agree (verified Oct 1-7 2026: virtualclinicians 16/3 == actions_capi_virtual_clinician
// 16/3, clinicians 3/1 == actions_capi_clinicians 3/1). Keys are lowercased.
// The "Clinician" event is deliberately absent — no custom conversion wraps it,
// so no column exists and only the conversions array can count it.
const META_EVENT_FIELD = {
  virtualclinicians: 'actions_capi_virtual_clinician',
  clinicians: 'actions_capi_clinicians',
};
const META_CC_BY_ID = {};        // custom_conversion_id → display name
const META_CC_FIELD_BY_ID = {};  // custom_conversion_id → Windsor count column
for (const _cc of META_CUSTOM_CONVERSIONS) { META_CC_BY_ID[_cc.id] = _cc.label; META_CC_FIELD_BY_ID[_cc.id] = _cc.field; }
// Meta lets the SAME custom conversion name exist under several ids, and only
// one of them carries an actions_* column in Windsor. An ad set optimizing for
// a duplicate id therefore resolved to no name and no column, which sent it to
// the submit_application fallback — a different event entirely.
//
// Verified against Meta's own Results for Oct 5-9 2026. The five
// "CAPI Demo + Webinar" ad sets (all on id 1612252243966047) reported
// 3 / 2 / 2 / 0 / 0, and actions_capi_demo_webinar returns exactly
// 3 / 2 / 2 / 0 / 0 for them, while submit_application_total returned
// 1 / 0 / 2 / 0 / 0 — which is what the dashboard had been showing.
//
// Aliases are kept OUT of META_CUSTOM_CONVERSIONS so META_CAPI_FIELDS does not
// request the same column twice.
const META_CC_ALIAS = {
  '1612252243966047': '1489552102408235',   // CAPI -- Demo + Webinar (duplicate id)
};
// Canonical custom-conversion id for a promoted object, or '' when it has none.
function _metaCcId(o) {
  const raw = (o && o.custom_conversion_id != null) ? String(o.custom_conversion_id) : '';
  if (!raw) return '';
  return META_CC_ALIAS[raw] || raw;
}

// All nine custom-conversion count columns. These are flat NUMERIC metrics —
// verified to add NO rows to a spend query (unlike the custom_conversion_action_name
// dimension, which fans rows out ~36x). Appended to Meta fetches.
const META_CAPI_FIELDS = ',' + META_CUSTOM_CONVERSIONS.map(c => c.field).join(',');
// Ad-set field carrying the configured optimization target. Ad-set grain, so
// only append it to fetches that are already at ad-set/ad grain, or to
// PAGINATED fetches (azWindsorFetchAll), which bisect any range that comes back
// at the row cap and so can absorb the extra grain. That now includes
// fetchWindsorAds, which moved off the unpaginated /all endpoint to a
// per-connector azWindsorFetchAll — an older note here still said never.
const META_PROMOTED_OBJ_FIELD = ',adset_promoted_object';
// Windsor's per-action-type breakdown, returned as a NESTED ARRAY inside each
// row (not extra rows — verified: same row count and spend with and without).
// It is the ONLY source for ad sets that optimize for a raw custom pixel event,
// since Windsor exposes a column only where a custom conversion wraps the event.
// Rides along with META_PROMOTED_OBJ_FIELD — the two are useless apart, because
// the promoted object names the target and this counts it.
const META_CONV_ARRAY_FIELD = ',conversions';

// Optimization target for a Meta ad set, parsed from adset_promoted_object
// ("the object this ad set is optimizing for" — a JSON-encoded STRING).
// custom_conversion_id is present only when custom_event_type === 'OTHER' (i.e.
// a custom conversion); otherwise the standard pixel event IS the target
// (e.g. SUBMIT_APPLICATION → "Submit Application"). '' when unknown.
function metaOptTarget(promotedObjectRaw) {
  const o = _metaParsePromoted(promotedObjectRaw);
  if (!o) return '';
  const ccId = _metaCcId(o);
  if (ccId && META_CC_BY_ID[ccId]) return META_CC_BY_ID[ccId];
  const evt = String(o.custom_event_type || '').trim();
  // An id we cannot name used to stop here and print the raw number. Fall
  // through to the event type first: a custom conversion wrapping a STANDARD
  // event (e.g. id 1454873686662315, custom_event_type SCHEDULE) is perfectly
  // nameable, and "Schedule" beats "Custom conversion 1454873686662315".
  if (evt && evt.toUpperCase() !== 'OTHER') {
    return evt.split('_').map(w => w ? w.charAt(0) + w.slice(1).toLowerCase() : '').join(' ');
  }
  // 'OTHER' with no custom conversion = a RAW custom pixel event, named only by
  // custom_event_str (e.g. 'virtualclinicians', 'Clinician'). Returning ''
  // here left every such ad set with a blank Optimization Target, which is what
  // hid the clinician split.
  if (evt.toUpperCase() === 'OTHER') {
    const s = String(o.custom_event_str || '').trim();
    if (s) return s;
  }
  return ccId ? ('Custom conversion ' + ccId) : '';
}
function _metaParsePromoted(raw) {
  if (!raw) return null;
  let o;
  try { o = JSON.parse(raw); } catch (e) { return null; }
  return (o && typeof o === 'object') ? o : null;
}
// Windsor count column holding THIS row's optimized conversion, or '' when the
// row doesn't optimize for a custom conversion (or adset_promoted_object wasn't
// fetched). Lets demo counting follow the campaign's real target.
function metaTargetField(row) {
  const o = _metaParsePromoted(row.adset_promoted_object);
  if (!o) return '';
  const id = _metaCcId(o);
  if (id && META_CC_FIELD_BY_ID[id]) return META_CC_FIELD_BY_ID[id];
  // An UNMAPPED id used to return '' right here, SKIPPING the standard-event
  // lookup below, so the row fell through to submit_application even when its
  // own event type named a column we fetch. That is how id 1454873686662315
  // (custom_event_type SCHEDULE) would be counted as Submit Applications.
  //
  // STANDARD pixel events used to return '' here too, which sent every one of
  // them to the submit_application fallback regardless of what the ad set
  // actually optimizes for. Verified against the live account 2026-09-17: the
  // "CAPI schedule" campaigns optimize for SCHEDULE, and Meta's own Results
  // column reported 10 / 1 / 1 Website Schedules where this dashboard reported
  // 17 / 1 / 0 Submit Applications — a different event, so CPD on those
  // campaigns was understated (Skincare read $59 against a true $101).
  const evt = String(o.custom_event_type || '').trim().toUpperCase();
  return META_STD_EVENT_FIELD[evt] || '';
}
// The RAW custom pixel event an ad set optimizes for ('' unless the target is a
// custom EVENT rather than a custom CONVERSION or a standard event).
function metaTargetEvent(row) {
  const o = _metaParsePromoted(row.adset_promoted_object);
  if (!o) return '';
  // A custom conversion we can COUNT is counted by its column, not from the
  // array. An unmapped one falls through, so an ad set on an unknown id that
  // still names a raw event in custom_event_str can be counted from the array.
  const id = _metaCcId(o);
  if (id && META_CC_FIELD_BY_ID[id]) return '';
  if (String(o.custom_event_type || '').trim().toUpperCase() !== 'OTHER') return '';
  return String(o.custom_event_str || '').trim();
}
// Count of one named pixel event, read from Windsor's `conversions` nested array
// (action_type 'offsite_conversion.fb_pixel_custom.<event>'). -1 when the array
// wasn't fetched, so callers can tell "not available" from a real zero.
const META_PIXEL_EVENT_PREFIX = 'offsite_conversion.fb_pixel_custom.';
function metaEventCount(row, eventName) {
  const arr = row && row.conversions;
  if (!eventName || !Array.isArray(arr)) return -1;
  const want = META_PIXEL_EVENT_PREFIX + eventName;
  const wantLc = want.toLowerCase();
  let exact = -1, loose = -1;
  for (const a of arr) {
    const t = String((a && a.action_type) || '');
    if (t === want) exact = (exact < 0 ? 0 : exact) + (parseFloat(a.value) || 0);
    else if (t.toLowerCase() === wantLc) loose = (loose < 0 ? 0 : loose) + (parseFloat(a.value) || 0);
  }
  const n = exact >= 0 ? exact : loose;
  return n < 0 ? -1 : Math.round(n);
}
// Conversions THIS row reported for the target its ad set actually optimizes
// for, or -1 when the target can't be determined (no adset_promoted_object) or
// can't be counted. One resolver for every Meta-connector aggregation path, so
// a campaign total is always the sum of its ad sets' own Results — which is
// exactly how Meta reports a campaign spanning several conversion types.
function metaTargetCount(row) {
  const ev = metaTargetEvent(row);
  if (ev) {
    const n = metaEventCount(row, ev);
    if (n >= 0) return n;
    const f = META_EVENT_FIELD[ev.toLowerCase()];
    if (f) return Math.round(parseFloat(row[f]) || 0);
    return -1;
  }
  const tf = metaTargetField(row);
  if (tf) return Math.round(parseFloat(row[tf]) || 0);
  return -1;
}
// Demos a Meta row contributes via its configured CAPI custom conversion, or -1
// when the row's campaign doesn't use CAPI (caller falls back to
// submit_application). Mirrors capiConvLabel: the TEST "Combined Ad Sets"
// campaign uses ONE blended demo+webinar conversion; the TEST "Skincare" campaign
// splits it per ad set (Demo→capi_demo, Webinar→capi_webinar, campaign row→both
// summed). Only the TEST campaigns run CAPI (they have no submit_application), so
// counting this is purely additive — every other campaign returns -1 (unchanged).
function metaCapiDemoCount(row) {
  // PREFERRED: count the conversion this row actually optimizes for, read from
  // adset_promoted_object. Works for every campaign, not just the two the old
  // name heuristic below knew about — and, via metaTargetCount, also for ad sets
  // optimizing for a raw custom pixel event, which have no count column at all.
  const t = metaTargetCount(row);
  if (t >= 0) return t;
  // FALLBACK: legacy campaign-name heuristic, for rows from fetches that don't
  // carry adset_promoted_object (e.g. fetchWindsorAds). Unchanged behavior.
  const c = (row.campaign_name || '').toLowerCase();
  if (!c.includes('test')) return -1;
  const a = (row.adset_name || row.ad_group_name || '').toLowerCase();
  const D = Math.round(parseFloat(row.actions_capi_demo) || 0);
  const W = Math.round(parseFloat(row.actions_capi_webinar) || 0);
  const DW = Math.round(parseFloat(row.actions_capi_demo_webinar) || 0);
  if (c.includes('combined ad sets') || c.includes('blended conversion')) return DW;
  if (c.includes('skincare & beauty brands') || c.includes('skincare & clean beauty')) {
    if (a.includes('webinar')) return W;
    if (a.includes('demo')) return D;
    return D + W;  // campaign row / unknown ad set → its two ad-set conversions summed
  }
  return -1;
}
// Total demos for a Meta row: the CAPI conversion when the campaign uses one,
// else the submit-application conversion; plus Leads(Form) on allowlisted
// lead-gen campaigns. One source of truth for Meta demo counting.
function metaDemos(row) {
  const capi = metaCapiDemoCount(row);
  let base;
  if (capi >= 0) {
    base = capi;
  } else {
    // Reached only when metaTargetCount could NOT resolve a target — i.e. the row
    // carries no adset_promoted_object (fetches below ad-set grain) or its target
    // has no column and no conversions array. Falls back to submit_application,
    // exactly the old behaviour, so nothing regresses where the target is unknown.
    base = Math.round(parseFloat(row.conversions_submit_application_total) || 0);
  }
  return base + metaLeadDemos(row.campaign_name, row);
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
// Date Helpers (US PACIFIC via offset — Workers run UTC)
// ---------------------------------------------------------------------------
// Every date boundary on every page routes through ptOff(), so switching the
// dashboard's timezone is this one function. The *ET-named* helpers below
// (etOff / todayET / toMsET) are kept as-is because ~40 call sites use them;
// they now return PACIFIC values. Prefer the PT names in new code.
//
// DST: Pacific shares the US transition DATES with Eastern (2nd Sunday March,
// 1st Sunday November) but flips at 2am LOCAL, i.e. 10:00 UTC entering PDT and
// 9:00 UTC leaving it — 3h later than the Eastern instants this used before.
function isPDT(date) {
  const y = date.getUTCFullYear();
  const mar1 = new Date(Date.UTC(y, 2, 1));
  const marSun2 = new Date(Date.UTC(y, 2, 8 + (7 - mar1.getUTCDay()) % 7));
  const dstStart = new Date(Date.UTC(y, 2, marSun2.getUTCDate(), 10, 0, 0));
  const nov1 = new Date(Date.UTC(y, 10, 1));
  const novSun1 = new Date(Date.UTC(y, 10, 1 + (7 - nov1.getUTCDay()) % 7));
  const dstEnd = new Date(Date.UTC(y, 10, novSun1.getUTCDate(), 9, 0, 0));
  return date >= dstStart && date < dstEnd;
}
function ptOff(d) { return isPDT(d) ? -7 : -8; }   // PDT / PST
function etOff(d) { return ptOff(d); }             // legacy name, now Pacific

function todayET() {
  const n = new Date(), o = etOff(n);
  const e = new Date(n.getTime() + o * 3600000);
  return new Date(Date.UTC(e.getUTCFullYear(), e.getUTCMonth(), e.getUTCDate()));
}
function yesterdayET() { const t = todayET(); t.setUTCDate(t.getUTCDate()-1); return t; }

function fmt(d) {
  return `${d.getUTCFullYear()}-${String(d.getUTCMonth()+1).padStart(2,'0')}-${String(d.getUTCDate()).padStart(2,'0')}`;
}

// Pacific day key (YYYY-MM-DD) for a HubSpot date or datetime value. A bare
// YYYY-MM-DD is already local and passes through untouched; anything carrying a
// time component is shifted into Pacific first, so an evening close does not
// land on the following day. Same rule as the local dayKey() inside
// buildClinicianDaily — hoisted here so the closed-won daily series uses it too.
function ptDayKeyOf(v) {
  if (!v) return null;
  const str = String(v);
  if (/^\d{4}-\d{2}-\d{2}$/.test(str)) return str;
  const t = /^\d+$/.test(str) ? parseInt(str, 10) : new Date(str).getTime();
  if (isNaN(t)) return null;
  return fmt(new Date(t + ptOff(new Date(t)) * 3600000));
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
  if (['reddit','reddit_ads'].includes(s)) return 'reddit';
  if (['openai','openai_ads','chatgpt','chat_gpt'].includes(s)) return 'chatgpt';
  // Bare referral domains: paid ONLY with a paid medium, like google above.
  if (['chatgpt.com','openai.com'].includes(s) && (m === 'cpc' || m === 'paid')) return 'chatgpt';
  if (s === 'youtube') return 'youtube';
  return null;
}

// ---------------------------------------------------------------------------
// Windsor Fetching
// ---------------------------------------------------------------------------
// /api/data response-cache version. Bump on any payload-shape change so a new
// deploy can't serve the previous build's cached payload. v7: Reddit + OpenAI
// channels added, PENDING_CHANNELS emptied.
const API_CACHE_VER = 'v39';   // v39: `signed` / `signedPct*` dropped from the trend series.
                               // v38: trend/trendRange are actually FORWARDED by buildResponse.
                               //      v37 built them onto currentData but never shipped them, so a
                               //      cached v37 payload has the fields missing, not stale.
                               // v37: Brand Dashboard Trend Analysis — adds a fixed-lookback
                               //      `trend` + `trendRange` to the /api/data payload.
                               // v36: clinician conversions follow each AD SET's own optimization
                               //      target instead of one fixed column pair — campaign totals are
                               //      now the sum of their ad sets, and the "Clinician" raw pixel
                               //      event (previously uncountable) is included.
                               // v35: trend series is page-filter independent, adds a month grain.
                               // v30: last-click acquisition split (paid / organic / untagged) on the regular
                               //      scheduled cohort, + chatgpt.com added to the paid mapper.
                               // v29: all Volume + Quality figures exclude pre-launch (deal tier, not isLow).
                               // v28: Demos supposed to happen excludes pre-launch.
                               // v27: October ad budgets.
                               // v26: Special1 held count is attendance-based + webinars.
                               // v25: totalCpqd divides by the canonical Qualified Demo count.
                               // v24: funnel webinar fetch fixed (was silently empty).
                               // v23: Quality is demo-only + webinar no-show; funnel CPQD redefined.
                               // v22: webinar-date deals folded into Demos to occur / supposed to happen.
                               // v21: Qualification rate moved onto the Qualified Demo
                               // definition, so Webinars Happened lift it too.
                               // v20: Qualified Demo redefined (held demos excl. pre-launch,
                               // plus webinars that reached Webinar Happened) — bumped so a
                               // cached response cannot keep serving the old counts.
                               // v19: dqForm/calRouting removed from the payload.

const WINDSOR_PAGE_SIZE = 5000;
async function windsorFetch(apiKey, from, to, fields, extra = '') {
  const url = `https://connectors.windsor.ai/all?api_key=${apiKey}&date_from=${from}&date_to=${to}&fields=${fields}&page_size=${WINDSOR_PAGE_SIZE}${extra}`;
  for (let i = 0; i < 3; i++) {
    // This path (the /all connector, used by the GA4 fetches) was never counted,
    // so SUBREQ under-reported by ~5 and its 3 retries burned real budget while
    // the log showed 0. That gap is what made the ceiling look like it was 36.
    if (SUBREQ.exhausted) return [];
    try {
      SUBREQ.n++; SUBREQ.windsor++;
      const r = await fetch(url);
      if (!r.ok) { azDrain(r); throw new Error(`Windsor ${r.status}`); }
      const j = await r.json();
      return j.data || [];
    } catch(e) {
      if (noteSubreqError(e)) return [];
      if (i < 2) await sleep(1000*(i+1)); else { console.error('Windsor fail:', e); return []; }
    }
  }
  return [];
}

// Paginated variant of windsorFetch for the all-connectors endpoint. Windsor
// has no offset pagination — it SILENTLY truncates at page_size — so we chunk
// by date and bisect any chunk that comes back at the cap (same strategy as
// azWindsorFetchAll). Without this, a high-cardinality or long-window fetch
// quietly loses rows and undercounts spend.
// chunkDays defaults to the WHOLE range (one request) on purpose. fetchWindsorAds
// runs 3x per /api/data call (current + prior + prior-month), and Cloudflare caps
// subrequests per invocation — pre-chunking by 30 days multiplied those 3 calls
// into dozens and blew the budget, which made ad spend come back empty across the
// dashboard. Starting with a single request keeps the normal case at 1 subrequest;
// the bisect-on-truncation path below still protects windows that actually exceed
// the row cap, so we get the safety without the cost.
async function windsorFetchAll(apiKey, from, to, fields, extra = '', chunkDays = 3650) {
  const addDays = (ds, n) => { const d = new Date(ds + 'T00:00:00Z'); d.setUTCDate(d.getUTCDate() + n); return fmt(d); };
  const daysBetween = (a, b) => Math.round((new Date(b + 'T00:00:00Z') - new Date(a + 'T00:00:00Z')) / 86400000);
  const out = [];
  const stack = [];
  let s = from, seed = 0;
  while (s <= to && seed++ < 800) { let e = addDays(s, chunkDays - 1); if (e > to) e = to; stack.push([s, e]); s = addDays(e, 1); }
  let calls = 0;
  while (stack.length && calls++ < 800) {
    const [a, b] = stack.pop();
    const rows = await windsorFetch(apiKey, a, b, fields, extra);
    if (rows.length >= WINDSOR_PAGE_SIZE && a !== b) {
      const mid = addDays(a, Math.floor(daysBetween(a, b) / 2));
      stack.push([a, mid], [addDays(mid, 1), b]);  // truncated → split & retry
    } else {
      // A single day at the cap can't be split any further, so this is the one
      // case where rows are genuinely lost. Never let that pass silently —
      // spend/demos would be undercounted with no visible signal.
      if (rows.length >= WINDSOR_PAGE_SIZE) {
        console.error(`windsorFetchAll: ${a} hit the ${WINDSOR_PAGE_SIZE}-row cap and cannot be split further — data for this day is TRUNCATED (fields: ${fields.slice(0, 120)})`);
      }
      out.push(...rows);
    }
  }
  return out;
}

// Main ad data (guide Section 1)
// Per-connector ad fetch. Each connector is queried on its OWN endpoint with
// only the fields it actually has, then tagged with a `datasource` so
// processAdSpend routes it exactly as before.
//
// WHY NOT /all: that endpoint spans every connected connector with ONE shared
// field list. Connecting Reddit + OpenAI Ads broke it — openai_ads has no
// campaign_name, and neither new connector has
// conversions_submit_application_total / externalwebsiteconversions — so the
// combined query failed, windsorFetch returned [], and every spend figure read
// as a legitimate $0. Scoping /all with a multi-connector param was also
// unproven. This uses azWindsorFetch, the same per-connector call the ad tables
// already rely on, so each connector only ever sees fields it supports and one
// failing connector can never blank the others.
const WINDSOR_AD_SOURCES = [
  // adset_promoted_object names the campaign's REAL optimization target, which is
  // what metaDemos() counts. Verified fan-out-free: it adds no rows to a spend
  // query, unlike custom_conversion_action_name (which multiplied spend ~36x).
  { connector:'facebook',   fields:'date,campaign_name,spend,clicks,impressions,ctr,conversions_submit_application_total,conversions_schedule_total' + META_LEAD_FIELDS + META_CAPI_FIELDS + META_PROMOTED_OBJ_FIELD + META_CONV_ARRAY_FIELD },
  { connector:'google_ads', fields:'date,campaign_name,spend,clicks,impressions,ctr,conversions' },
  { connector:'linkedin',   fields:'date,campaign,spend,clicks,impressions,ctr,externalwebsiteconversions' },
  { connector:'tiktok',     fields:'date,campaign_name,spend,clicks,impressions,ctr,conversions' },
  { connector:'reddit',     fields:'date,campaign_name,spend,clicks,impressions,ctr' },
  { connector:'openai_ads', fields:'date,campaign,spend,clicks,impressions,ctr' },
];
// azWindsorFetchAll, NOT azWindsorFetch: Windsor has no pagination and caps a
// single response at AZ_PAGE_SIZE (5000) rows, returning EARLIEST dates first
// and silently dropping the overflow. At date x campaign grain a month fits
// comfortably, but a Year to Date window (~240 days x every campaign across 6
// connectors) blows past the cap — so YTD showed only early-year ad spend on the
// Detailed, Irfan AND Clinician dashboards, with no error anywhere. FetchAll
// splits the window into date sub-ranges and bisects any range that comes back
// at the cap, so coverage self-adjusts to the account's actual rows/day. Short
// windows still resolve in one request per connector, so MTD is unchanged.
async function fetchWindsorAds(apiKey, from, to) {
  const per = await Promise.all(WINDSOR_AD_SOURCES.map(src =>
    // chunkDays = the WHOLE range on purpose: seed ONE request per connector and
    // let azWindsorFetchAll bisect only the ranges that actually come back at the
    // 5000-row cap. Pre-chunking by a fixed 30/31 days looks equivalent but is
    // not — this runs up to 3x per /api/data call (current + prior + prior-month)
    // across 6 connectors, so a fixed chunk multiplies into dozens of subrequests
    // and exhausts Cloudflare's per-invocation budget, which starves the fetches
    // that run after it (GA4 came back empty, tripping the "some data sources may
    // not have fully loaded" banner). Same reasoning as windsorFetchAll's default.
    azWindsorFetchAll(apiKey, src.connector, from, to, src.fields, 3650)
      .then(rows => (rows || []).map(r => Object.assign({}, r, {
        datasource: src.connector,                          // drives channel routing
        campaign_name: r.campaign_name || r.campaign || '',  // LinkedIn/OpenAI use `campaign`
      })))
      .catch(e => { console.error(`Windsor ad fetch ${src.connector}:`, e.message); return []; })
  ));
  return per.flat();
}


// LinkedIn demo override (guide Section 1 — separate conversion_name fetch)
// LinkedIn counts every conversion action in externalwebsiteconversions, so the
// raw figure mixes demos with webinar signups. We want demos only, matched by the
// conversion action's name.
//
// The action was renamed to LinkedIn's "CAPI -- *" convention, so the old
// 'demo request' match stopped hitting anything and silently zeroed the channel —
// the count is an override, so matching nothing reads as "0 demos", not "unknown".
// Current actions on the account: CAPI -- Demo, CAPI -- Webinar, HubSpot - WebinarSQL.
const LINKEDIN_DEMO_CONVERSIONS = [/capi\s*--\s*demo\b/i, /demo\s*request/i];
// LinkedIn keeps attributing conversions to retired campaign GROUPS long after they
// stop spending: B2Linked's campaigns are paused with no spend in the window yet
// still logged 10 CAPI -- Demo in Aug 1-16, against 2 for the live group. Counting
// them made the channel read 12 where LinkedIn's own UI shows 2 for live campaigns.
// Matched on campaign_group_name — the LinkedIn UI's "Campaign name" column.
const LINKEDIN_EXCLUDED_GROUPS = [/^\s*b2linked\s*$/i];
const isRetiredLinkedInGroup = (g) => LINKEDIN_EXCLUDED_GROUPS.some(re => re.test(g || ''));
async function fetchLinkedInDemos(apiKey, from, to) {
  // Per-connector endpoint, NOT the mixed /all one: conversion_name and
  // externalwebsiteconversions don't exist on every connected connector, and a
  // single unknown field fails the whole /all query — the same failure that
  // emptied ad spend when Reddit and OpenAI Ads were connected.
  // Whole-range seed + bisect-on-cap, same as fetchWindsorAds: one subrequest in
  // the normal case, extra ones only when a range is genuinely truncated.
  const rows = await azWindsorFetchAll(apiKey, 'linkedin', from, to, 'date,campaign_group_name,conversion_name,externalwebsiteconversions', 3650);
  // No rows means the fetch failed or the window is empty. Return null so the
  // caller leaves LinkedIn's own total alone instead of overriding it to zero.
  if (!rows.length) return null;
  let n = 0;
  for (const r of rows) {
    if (isRetiredLinkedInGroup(r.campaign_group_name)) continue;
    if (LINKEDIN_DEMO_CONVERSIONS.some(re => re.test(r.conversion_name || ''))) {
      n += parseInt(r.externalwebsiteconversions) || 0;
    }
  }
  return n;
}

// Top-of-funnel stages for the Clinician Funnel. All four are window-scoped, like
// the rest of that funnel now is — mixing a windowed ad metric with a history CRM
// count would make the conversion rate between them meaningless.
//
// Field choices are measured, not inferred (Windsor, 30 days to 2026-10-05):
//   link_clicks IS a distinct metric from clicks  — 8,601 vs 13,060.
//   conversions_click_join_for_free DOES NOT EXIST: the conversions_<event>
//     pattern only covers GA4 KEY events and this is not one. event_name +
//     event_count do work, and the real names are click_join_for_free_button
//     (3,721) and click_join_for_free_button_alt_no_sample (64) — so the match
//     has to be a CONTAINS, not an equality.
//   page_path matches EXACTLY /clinicians and /for-clinicians; a contains-match
//     on "clinician" also sweeps /clinicians-choice, /clinician-reviews and nine
//     blog posts, roughly doubling the stage.
const CLN_CTA_EVENT_MATCH = 'click_join_for_free';
const CLN_FUNNEL_PAGES = ['/clinicians', '/for-clinicians'];
async function fetchClinicianTopFunnel(apiKey, from, to) {
  const out = { impressions: null, linkClicks: null, clicks: null, pageViews: null, ctaClicks: null };
  try {
    const rows = await azWindsorFetchAll(apiKey, 'facebook', from, to,
      'date,campaign_name,impressions,clicks,link_clicks', 30);
    let im = 0, lc = 0, ck = 0;
    for (const r of rows) {
      if (!isClinicianCampaign(r.campaign_name)) continue;
      im += parseInt(r.impressions) || 0;
      lc += parseInt(r.link_clicks) || 0;   // null on zero-spend campaigns
      ck += parseInt(r.clicks) || 0;
    }
    out.impressions = im; out.linkClicks = lc; out.clicks = ck;
  } catch (e) { console.error('Clinician top-funnel (meta):', e && e.message); }
  // Two separate GA4 calls on purpose: page_path lives in the Page/Screen table
  // and event_name in the Event table, so asking for both at once cross-joins them.
  try {
    const rows = await windsorFetch(apiKey, from, to, 'page_path,screen_page_views', '&connectors=googleanalytics4');
    let n = 0;
    for (const r of rows) {
      if (CLN_FUNNEL_PAGES.indexOf((r.page_path || '').trim()) >= 0) n += parseInt(r.screen_page_views) || 0;
    }
    // Still fetched and shipped, but the funnel no longer has a Page Views
    // stage. Nothing else reads topFunnel.pageViews today — if it stays unused,
    // this GA4 call can go and save a subrequest.
    out.pageViews = n;
  } catch (e) { console.error('Clinician top-funnel (ga4 pages):', e && e.message); }
  try {
    const rows = await windsorFetch(apiKey, from, to, 'event_name,event_count', '&connectors=googleanalytics4');
    let n = 0;
    for (const r of rows) {
      if ((r.event_name || '').indexOf(CLN_CTA_EVENT_MATCH) >= 0) n += parseInt(r.event_count) || 0;
    }
    out.ctaClicks = n;
  } catch (e) { console.error('Clinician top-funnel (ga4 events):', e && e.message); }
  return out;
}


// Kick Off Call No-Show Rate = 1 - (calls given / calls BOOKED in the window).
//
// Per Alex: the denominator is every deal whose clinician_meeting_booked_date
// falls in the window, and the numerator is those recorded as Demo Given. So
// anything that is not a Demo Given counts against the rate — No Show,
// Cancelled Before Demo, still Scheduled, Rescheduled, and a blank attendance
// status alike. That is deliberately BROADER than the previous definition,
// which divided no-shows by no-shows-plus-held and excluded the other four
// states from the denominator entirely.
//
// Two consequences worth knowing, both inherent to the definition:
//   * A call booked inside the window but scheduled to happen AFTER it has not
//     happened yet, so it is counted as not-given and pushes the rate up. The
//     card reports how many of the denominator are still upcoming.
//   * The window is on the BOOKED date, not the deal's create date, so the
//     deals considered are drawn from the full history rather than the page
//     cohort — a deal created in June and booked in September belongs to
//     September.
const CLN_ATT_HELD = ['Demo Given \u2014 Originally Scheduled', 'Demo Given \u2014 Rescheduled'];
const CLN_ATT_PENDING = ['Scheduled', 'Rescheduled'];
function buildClinicianVirtualNoShow(deals, from, to) {
  let booked = 0, given = 0, noShow = 0, pending = 0, cancelled = 0, blank = 0;
  for (const d of (deals || [])) {
    const p = d.properties || {};
    const bk = _clnDayKey(p.clinician_meeting_booked_date);
    if (!bk) continue;
    if (from && bk < from) continue;
    if (to && bk > to) continue;
    booked++;
    const att = (p.clinician_attendance_status || '').trim();
    if (CLN_ATT_HELD.indexOf(att) >= 0) given++;
    else if (att === 'No Show') noShow++;
    else if (CLN_ATT_PENDING.indexOf(att) >= 0) pending++;
    else if (att === 'Cancelled Before Demo') cancelled++;
    else blank++;
  }
  return {
    booked, given, noShow, pending, cancelled, blank,
    held: given,                       // kept: the client labels this "given"
    rate: booked > 0 ? (1 - given / booked) * 100 : null,
  };
}

// GA4 (guide Section 2)
async function fetchGA4(apiKey, from, to) {
  const fields = 'datasource,users,sessions,conversions_click_schedule_demo_button,conversions_hubspot_meeting_booked';
  return windsorFetch(apiKey, from, to, fields, '&connectors=googleanalytics4');
}



// ---------------------------------------------------------------------------
// HubSpot Fetching
// ---------------------------------------------------------------------------
// ─── Subrequest budget ──────────────────────────────────────────────────────
// Cloudflare caps subrequests PER INVOCATION and every fetch in the request
// shares that one pool. A Year to Date load blew it: Windsor alone issues
// ~450 calls (campaigns run for current + prior + priorMonth = 3 windows, and
// Meta creatives chunk at 7 days => 35 chunks/channel across 243 days), which
// starved the HubSpot deal fetches queued alongside them in the same
// Promise.all. The symptom was silent: scheduledContacts came back 1600 while
// pipelineDeals / closedWon / churnedByClose all came back 0, so Key Metrics
// and Signed Brands rendered as zeros instead of erroring.
//
// Two guarantees here:
//  1. HubSpot is never starved by Windsor. Windsor gets a hard call ceiling and
//     degrades the TAIL of very wide ad tables; the deal fetches always run.
//  2. Once the pool IS exhausted, every retry and every bisect is guaranteed to
//     fail too. Before the breaker, one YTD load logged 296 doomed Windsor
//     retries with 1-2s sleeps between them — ~15 minutes of dead waiting
//     against a 180s frontend timeout. Now the first exhaustion stops the rest.
// softFail counts give-ups that leave a GAP in the data without tripping the
// pool breaker: a fetch that burned its retries, or a count that came back null.
// Without it a degraded payload looks healthy and gets cached.
const SUBREQ = { n: 0, windsor: 0, hubspot: 0, kv: 0, exhausted: false, windsorDropped: 0, softFail: 0, creCapped: 0,
  // Attempts RESERVED, incremented synchronously the moment a chunk is admitted.
  // SUBREQ.windsor only counts attempts already STARTED, and ~20 chunkers admit
  // their waves synchronously before any of those increments land, so budgeting
  // against it let every chunker admit as though it were the only one. Reserving
  // up-front is what makes the ceiling hold across concurrent chunkers.
  windsorPlanned: 0 };
const WINDSOR_CALL_BUDGET = 700;   // leaves the rest of the pool for HubSpot.
// Raised from 420 after a measured YTD run hit it and dropped chunks on four
// connectors (google_ads, reddit, openai_ads, tiktok). HubSpot peaked at ~102
// subrequests on that same run, so 700 + ~102 + kv still clears the 1000 ceiling.
// Chunks inside one azWindsorFetchAll are independent, so they are fetched in
// bounded-concurrency waves rather than one at a time. A year-wide creative
// window is ~40 chunks per channel (chunkDays=7); serially that is 60s+ per
// channel and the dashboard aborts at 75s. Kept modest because the ~6 channels
// already run their chunkers in parallel, so the real outbound concurrency is
// this times the channel count.
const AZ_CHUNK_CONCURRENCY = 3;
// azWindsorFetch retries up to 3 times and counts EVERY attempt, so one admitted
// chunk can cost 3 subrequests. The budget must reserve the worst case or a
// retry storm walks straight past Cloudflare's 1000-per-invocation ceiling and
// the whole request 503s.
const AZ_MAX_ATTEMPTS = 3;
// Reserved per admitted chunk. Reserving the worst case (3) stopped the
// chunker at a third of the budget and dropped real data; SUBREQ_HARD_MAX is
// what actually prevents the platform-limit 503, so reserve the typical cost.
const AZ_RESERVE_PER_CHUNK = 1;
// Windows whose fetch cost is structural rather than incidental. A Year to Date
// Meta creative fetch alone is ~107k rows over ~54 Windsor calls (~38s measured),
// so these are warmed by the cron below and served from cache; recomputing one
// inside a browser request will always be marginal against its abort.
// MTD is on this list even though it is a SHORT window: short does not mean
// cheap here. The cost is driven by campaign x ad-set x creative cardinality and
// the attribution match, not by the number of days, and MTD measured past the
// dashboard's 75s abort — so every load failed, and because the abort killed the
// request before it could finish, the cache could never populate from a user
// either. Being on this list gets it a cron warm AND the long freshness TTL
// below, which is what breaks that loop. MTD is first so it is warmed before the
// two expensive windows can eat the budget.
// Windows the cron warms, and which therefore get the long cache TTL. allTime
// was dropped 2026-10-08 with the pill that selected it: the cron was paying
// for the slowest precompute of the three on every tick for a window nothing
// could ask for. The API still ACCEPTS allTime (see the handler's OK list) —
// it just is not warmed, so a direct call pays for it live.
const AZ_PRECOMPUTED_WINDOWS = ['mtd', 'ytd'];
// Keys with a background refresh in flight, so concurrent readers do not all
// kick off the same rebuild. Module scope, so it is per-isolate rather than
// global — a best-effort stampede guard, not a lock.
const AZ_REFRESHING = new Set();
// Soft failures the CRON will still cache through. A scheduled warm that throws
// away a 6-minute rebuild over one timed-out sub-fetch leaves the dashboard with
// nothing at all, which is strictly worse than a payload with a small gap. The
// request path stays strict.
const AZ_CRON_SOFTFAIL_TOLERANCE = 5;
// The AD DATA's own version, deliberately NOT API_CACHE_VER. The two cache
// different things: API_CACHE_VER tracks the /api/data and /api/clinician
// payload shapes, which change whenever a dashboard card is added, while this
// tracks the ad-data shape, which almost never changes.
//
// They used to share one constant, which meant any unrelated dashboard change
// invalidated the CRON-WARMED Year to Date and All Time entries — the ones that
// take minutes to rebuild and cannot be computed on the request path. The
// symptom was the Ads section failing to load for up to 20 minutes after every
// deploy that touched an unrelated payload, which reads exactly like the
// timeout bug those precomputes were built to fix.
//
// Bump this ONLY when the ad-data shape changes. A dashboard payload change
// should not cost a cold precompute.
// a2: ad rows now carry the `conversions` array, and Meta/Clinician demos are
// counted from each ad set's own target — a cached a1 payload holds the old
// per-campaign conversion and CPD figures, so it must not be served.
const AZ_DATA_VER = 'a2';
// Cloudflare caps CONCURRENT in-flight fetches, and a Response whose body is
// never read never completes — so it holds its slot indefinitely. Past the cap
// the runtime cancels the OLDEST in-flight response to break the deadlock,
// which is the "A stalled HTTP response was canceled to prevent deadlock"
// warning the live log is full of. Every error path that threw straight off a
// Response leaked a body — three times over with retries — and Windsor answers
// 400 for LinkedIn on every single run, so the leak was continuous and the
// victims were other connectors' healthy requests queued behind it.
//
// Call this before throwing or returning off a Response you will not read.
function azDrain(r) {
  try { if (r && r.body && !r.bodyUsed) r.body.cancel(); } catch (_) { /* already gone */ }
  return r;
}
function azCacheKey(win, from, to, vsFrom, vsTo) {
  return 'azdata_resp_' + AZ_DATA_VER + '_' + win + '_' + (from||'') + '_' + (to||'')
       + '_' + (vsFrom||'') + '_' + (vsTo||'');
}
// These aggregate months of history, so a half-hour-old view is accurate enough;
// the short TTL exists for windows that move intraday.
function azCacheTtlMs(win) { return AZ_PRECOMPUTED_WINDOWS.includes(win) ? 1800000 : 120000; }
// How long a cache entry is KEPT, which is deliberately much longer than how
// long it is considered FRESH. Freshness decides whether to rebuild; retention
// decides whether there is anything to fall back on when the rebuild fails.
// Entries used to be dropped 5 minutes after going stale, so two consecutive
// cron failures — or any deploy that changed the key — left the request path
// with nothing but a rebuild it cannot finish inside the browser's abort.
// Day-old ad data is a far better answer than an error.
const AZ_CACHE_RETAIN_SEC = 86400;
// Last-resort backstop across EVERY connector, checked per attempt. The
// per-connector budget is a plan; this is the thing that cannot be overshot.
const SUBREQ_HARD_MAX = 900;
// Creative grain is the densest fetch, but only on Meta. Start wide and let
// azWindsorFetchAll's existing cap-bisection narrow only the connectors that
// actually need it, instead of charging every connector 40 calls for a year.
const AZ_CREATIVE_CHUNK_DAYS = 30;
function subreqReset() {
  SUBREQ.n = 0; SUBREQ.windsor = 0; SUBREQ.hubspot = 0; SUBREQ.kv = 0;
  SUBREQ.exhausted = false; SUBREQ.windsorDropped = 0; SUBREQ.softFail = 0; SUBREQ.windsorPlanned = 0;
  SUBREQ.creCapped = 0;
}
function isSubreqLimit(e) {
  return !!e && /Too many subrequests/i.test(e.message || String(e));
}
// Call on every caught error so one exhaustion trips the breaker for everyone.
function noteSubreqError(e) {
  if (isSubreqLimit(e) && !SUBREQ.exhausted) {
    SUBREQ.exhausted = true;
    console.error(`SUBREQ EXHAUSTED after ~${SUBREQ.n} subrequests (windsor ${SUBREQ.windsor}, hubspot ${SUBREQ.hubspot}) — failing fast from here instead of retrying`);
  }
  return SUBREQ.exhausted;
}
function subreqSummary() {
  return `subrequests ~${SUBREQ.n} (windsor ${SUBREQ.windsor}/${SUBREQ.windsorPlanned} planned, hubspot ${SUBREQ.hubspot}, kv ${SUBREQ.kv})`
    + (SUBREQ.softFail ? `, soft failures: ${SUBREQ.softFail}` : '')
    + (SUBREQ.creCapped ? `, creative fetches capped: ${SUBREQ.creCapped}` : '')
    + (SUBREQ.windsorDropped ? `, windsor chunks dropped at budget: ${SUBREQ.windsorDropped}` : '')
    + (SUBREQ.exhausted ? ' — POOL EXHAUSTED' : '');
}

// Count-only search. HubSpot returns `total` on every search response, so a
// count costs ONE subrequest with limit:1 — hsSearch drops that field and would
// page through every matching record to length-count them instead. Returns null
// (not 0) on failure, so a failed fetch reads as "unknown" and suppresses a
// delta rather than asserting zero.
async function hsCount(token, objectType, filterGroups) {
  const url = `https://api.hubapi.com/crm/v3/objects/${objectType}/search`;
  // HubSpot's SEARCH endpoint is rate limited far more tightly than the rest of
  // the API (a few requests a second per token). Without a retry a throttled
  // count came back null and rendered as an em dash — indistinguishable from
  // "this campaign genuinely has none", which is exactly how the Inquiries
  // column ended up populating for some campaigns and not others. hsSearch has
  // always backed off on 429; this had not.
  for (let attempt = 0; attempt < 3; attempt++) {
    if (SUBREQ.exhausted) { console.warn(`hsCount ${objectType}: skipped, subrequest pool exhausted`); return null; }
    try {
      SUBREQ.n++; SUBREQ.hubspot++;
      const r = await fetch(url, {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ filterGroups, properties: ['hs_object_id'], limit: 1 }),
      });
      if (r.status === 429) { await sleep(250 * (attempt + 1)); continue; }
      if (!r.ok) { azDrain(r); console.error(`hsCount ${objectType} ${r.status}`); return null; }
      const d = await r.json();
      return typeof d.total === 'number' ? d.total : null;
    } catch (e) {
      if (noteSubreqError(e)) return null;
      if (attempt === 2) { console.error('hsCount failed:', e && e.message); return null; }
      await sleep(250 * (attempt + 1));
    }
  }
  SUBREQ.softFail++;
  console.error(`hsCount ${objectType}: still rate limited after retries`);
  return null;
}

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
      if (SUBREQ.exhausted) { lastErr = new Error('subrequest pool exhausted'); break; }
      try {
        SUBREQ.n++; SUBREQ.hubspot++;
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
        if (!resp.ok) { console.error(`HS ${objectType} ${resp.status} after ${pages} page(s) — returning PARTIAL ${all.length} results:`, await resp.text()); return all; }
        break; // success
      } catch(e) { lastErr = e; if (noteSubreqError(e)) break; await sleep(500 * (attempt + 1)); }
    }
    if (!resp || !resp.ok) { console.error(`HS ${objectType} search failed after ${pages} page(s) — returning PARTIAL ${all.length}:`, (lastErr && lastErr.message) || lastErr); break; }
    const d = await resp.json();
    all.push(...(d.results || []));
    pages++;
    if (d.paging?.next?.after) after = d.paging.next.after; else break;
    if (all.length >= 10000 || pages >= maxPages) {
      // Truncation is silent otherwise, and with an ASCENDING sort it is the
      // NEWEST records that get dropped — i.e. exactly the recent edits someone
      // would report as "this field is out of date".
      const _dir = (sorts && sorts[0] && sorts[0].direction) || 'ASCENDING';
      console.warn(`hsSearch ${objectType} TRUNCATED at ${all.length} results / ${pages} pages (cap ${maxPages}, hard ceiling 10000) — sorted ${_dir}, so the ${_dir === 'ASCENDING' ? 'NEWEST' : 'OLDEST'} records are missing from this payload`);
      break;
    }
  }
  return all;
}

// Wrap a WINDOW-INDEPENDENT fetch (same result regardless of the page's date
// selector) with a short-lived KV cache.
//
// CORRECTION — this comment used to claim "KV binding reads/writes do NOT count
// against the per-invocation subrequest limit". That is FALSE, and it was the
// premise this entire caching layer was designed on. Every KV get and put costs
// one subrequest from the same pool as a fetch(). A cache hit therefore replaces
// N paginated subrequests with ONE, not with zero — still a large win for N>1,
// but it means adding "free" cache keys makes the ceiling WORSE, and a bundle of
// M values belongs under ONE key rather than M. Falls back to a live fetch whenever
// KV is unavailable, the entry is missing/expired, or parsing fails, so it can
// never serve stale-forever or break the response.
function kvOp() { SUBREQ.n++; SUBREQ.kv = (SUBREQ.kv || 0) + 1; }
async function kvCachedFetch(env, key, ttlSec, fetchFn, fallback) {
  // staleData remembers the last cached payload even if expired, so a later
  // live-fetch failure degrades to stale data instead of crashing the handler.
  let staleData = (fallback !== undefined ? fallback : null);
  if (env && env.CONTENT_STORE) {
    try {
      kvOp();
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
      kvOp();
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
      kvOp();
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
      kvOp();
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


// hsSearch defaults to 10 pages x 200 = 2000 rows. That comfortably covers a
// month, but a Year to Date (or All Time) window blows straight past it and the
// overflow is dropped silently — and because the default sort is ASCENDING it is
// the MOST RECENT records that vanish, so YTD looked like it was missing its
// latest months. Scale the page budget with the window length: a month-or-less
// window keeps exactly today's 10 pages (no extra latency on the common path),
// YTD gets the full 50 (= 10,000, HubSpot's own hard search ceiling).
function pagesForWindow(from, to, base = 10, max = 25) {
  if (!from || !to) return base;
  const a = new Date(String(from) + 'T00:00:00Z'), b = new Date(String(to) + 'T00:00:00Z');
  if (isNaN(a) || isNaN(b)) return base;
  const days = Math.max(1, Math.round((b - a) / 86400000) + 1);
  return Math.min(max, Math.max(base, Math.ceil(days / 31) * base));
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
  ], ['createdate', 'date_demo_booked', 'email', 'website', 'company', 'average_monthly_web_traffic', 'hs_object_source_detail_1',
      // Acquisition: powers the Detailed Dashboard's "Demo Acquisition" donut.
      'utm_source', 'utm_medium', 'utm_campaign'],
     200, [{ propertyName: 'createdate', direction: 'ASCENDING' }], pagesForWindow(from, to));
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
  const maxPages = opts.maxPages ?? pagesForWindow(from, to);
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
  }], ['amount','closedate','hs_createdate','hs_v2_date_entered_closedwon','utm_source','utm_medium','utm_campaign','utm_content','hubspot_owner_id','brand_status','average_monthly_web_traffic__cloned_','average_monthly_web_traffic','dealstage','dealname']);
}

// Pipeline-agnostic "won" deals by hs_v2_date_entered_closedwon (the timestamp HubSpot
// stamps when a deal enters a closed-won stage). Catches deals whose user-editable
// `closedate` falls outside the window but which entered won inside it.
async function fetchAnyWonDealsByEnteredWon(token, from, to) {
  return hsSearch(token, 'deals', [{
    filters: [
      { propertyName: 'hs_is_closed_won', operator: 'EQ', value: 'true' },
      { propertyName: 'hs_v2_date_entered_closedwon', operator: 'GTE', value: String(toMsET(from)) },
      { propertyName: 'hs_v2_date_entered_closedwon', operator: 'LTE', value: String(toMsET(to, true)) },
    ],
  }], ['amount','closedate','hs_createdate','hs_v2_date_entered_closedwon','utm_source','utm_medium','utm_campaign','utm_content','hubspot_owner_id','brand_status','average_monthly_web_traffic__cloned_','average_monthly_web_traffic','dealstage','dealname']);
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
  }], ['amount','closedate','hs_createdate','utm_source','utm_medium','utm_campaign','utm_content','hubspot_owner_id','brand_status','average_monthly_web_traffic__cloned_','average_monthly_web_traffic'],
     200, [{ propertyName: 'hs_createdate', direction: 'ASCENDING' }], pagesForWindow(from, to));
}

// Deals that reached a Churned stage (Churned / Churned Onboarding) but still
// carry a Close Date in the window. Added to the Marketing Funnel "Signed
// Brands" count so brands that signed in-period and later churned are still
// counted (Close Date basis, per request — churned deals often have closedate
// cleared, so this only recovers the ones that retained it).
async function fetchChurnedByCloseDate(token, from, to) {
  return hsSearch(token, 'deals', [{
    filters: [
      { propertyName: 'dealstage', operator: 'IN', values: ['3453925110','3517067985'] },
      { propertyName: 'closedate', operator: 'GTE', value: String(toMsET(from)) },
      { propertyName: 'closedate', operator: 'LTE', value: String(toMsET(to, true)) },
    ],
  }], ['amount','closedate','brand_status','dealstage'],
     200, [{ propertyName: 'hs_createdate', direction: 'ASCENDING' }], pagesForWindow(from, to));
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
    if (!r.ok) { azDrain(r); break; }
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
// Which channel a Windsor ad row belongs to, or '' when the row is not an ad row
// at all (GA4 rows ride the same array). ONE mapper, so every aggregation over
// these rows — processAdSpend's channel table and totals, and the Brand trend
// series — agrees on what a channel is and therefore on what KPI_EXCLUDED_CHANNELS
// takes out. A second copy of this if-chain is how the trend's Ad Spend would
// quietly stop matching the Ad Spend tile above it.
function windsorRowChannel(row) {
  const ds = ((row && row.datasource) || '').toLowerCase();
  if (!ds || /googleanalytics/.test(ds)) return '';
  const camp = (row.campaign_name || '');
  // Clinician Targeting campaigns are their own channel, not Meta.
  if (/facebook|meta|fb|ig|instagram/.test(ds)) return isClinicianCampaign(camp) ? 'clinician' : 'meta';
  if (/linkedin/.test(ds)) return 'linkedin';
  if (/tiktok/.test(ds)) return 'tiktok';
  if (/reddit/.test(ds)) return 'reddit';
  // Windsor's datasource for OpenAI Ads is literally "openai_ads" — match on
  // the substring, not an exact "openai"/"chatgpt".
  if (/openai|chatgpt/.test(ds)) return 'chatgpt';
  if (/google/.test(ds)) return /\byt\b|youtube/i.test(camp) ? 'youtube' : 'google';
  return '';
}
function processAdSpend(rows, linkedInDemoOverride) {
  const ch = {};
  for (const c of DASH_CHANNELS) ch[c] = { spend:0, clicks:0, impressions:0, windsorDemos:0 };
  let tSpend=0, tClicks=0, tImpr=0, tDemos=0;
  const spendByDay = {};  // date (YYYY-MM-DD) → total spend across all channels — powers True CPD per week

  for (const row of rows) {
    const key = windsorRowChannel(row);
    if (!key) continue;

    const spend = parseFloat(row.spend)||0;
    const clicks = parseInt(row.clicks)||0;
    const impr = parseInt(row.impressions)||0;

    let demos = 0;
    if (key === 'meta') {
      // Same counter as the ad tables: the campaign's real optimization target
      // (adset_promoted_object -> custom conversion), falling back to
      // submit_application, plus allowlisted lead-gen campaigns' Leads (Form).
      //
      // Previously this counted submit_application ONLY, because the old mixed
      // /all endpoint couldn't safely carry adset_promoted_object. fetchWindsorAds
      // now queries each connector separately, so it can — and every campaign
      // optimizing for a different custom conversion is no longer dropped, which
      // is what made Channel Performance undercount Meta against the campaigns
      // table.
      demos = metaDemos(row);
    } else if (key === 'linkedin') {
      demos = parseInt(row.externalwebsiteconversions)||0; // overridden below
    } else if (key === 'tiktok') {
      demos = parseInt(row.conversions)||0;
    } else if (key === 'clinician') {
      // Its optimized conversion — not submit_application.
      demos = clinicianConvCount(row);
    } else if (key === 'reddit' || key === 'chatgpt') {
      // Neither connector exposes a conversion metric on the /all endpoint:
      // Reddit splits conversions into per-action fields (conversion_*_clicks)
      // that aren't shared across connectors, and OpenAI Ads has none at all.
      // Spend / clicks / impressions are real; conversions stay 0 by design.
      demos = 0;
    } else {
      demos = Math.ceil(parseFloat(row.conversions)||0);
    }

    // The mapper above can emit 'youtube' (a Google Ads campaign whose name
    // matches /\byt\b|youtube/i), but ch is seeded from DASH_CHANNELS, which has
    // no youtube entry — so ch['youtube'] was undefined and this line threw
    // "Cannot read properties of undefined (reading 'spend')", 500-ing the whole
    // request. It only bites a window that actually contains a YouTube campaign,
    // which is why Year to Date died while Month to Date was fine. Create the
    // bucket on demand so no key the mapper produces can take the request down.
    if (!ch[key]) ch[key] = { spend:0, clicks:0, impressions:0, windsorDemos:0 };
    ch[key].spend += spend; ch[key].clicks += clicks; ch[key].impressions += impr; ch[key].windsorDemos += demos;
    // KPI-excluded channels keep their own per-channel row but contribute
    // nothing to the totals behind CPD / CPQD / CAC / ROAS / spend-by-day.
    if (KPI_EXCLUDED_CHANNELS.has(key)) continue;
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
// Largest-first list with "Not set" pinned last, capped at `max` entries with the
// remainder rolled into one labelled bucket so the total still reconciles.
// Mirrors the local `capped`/`toList` pair in buildClinicianComposition.
function cappedCounts(o, max) {
  const l = Object.entries(o)
    .map(([label, count]) => ({ label, count }))
    .sort((a, b) => (a.label === 'Not set') - (b.label === 'Not set') || b.count - a.count);
  if (l.length <= max) return l;
  const head = l.slice(0, max - 1);
  const tail = l.slice(max - 1);
  // The rolled-up bucket CARRIES its rows, so the client can expand it in place
  // without another request. Same shape buildClinicianComposition's capped()
  // returns — _clinicianPie renders `children` on whichever bucket has them, so
  // Demo Acquisition gets the expandable Other the Clinician UTM card has.
  head.push({ label: `Other (${tail.length} more)`, count: tail.reduce((n, x) => n + x.count, 0), children: tail });
  return head;
}
function processScheduledContacts(contacts) {
  const byDay = {};
  const byDayScale = {};         // per-day count excluding pre-launch
  const byDayScale10KPlus = {};  // per-day count excluding pre-launch AND 0-10K (Irfan-only stricter view)
  const byDayAll = {};           // per-day count INCLUDING webinars (Very Small) — matches "CPD · All"
  const byWebTraffic = {};       // count by raw web-traffic tier value (Livestorm → "Very Small"/Webinar) — webinar count for CPD cards
  // Regular vs Pre-Launch. Both count demos AND webinars; the split is purely on
  // the brand's REAL web-traffic tier, which is known for a webinar registrant too
  // (wtRaw) — so a pre-launch brand that came in through a webinar lands in
  // Pre-Launch rather than being lumped into the Webinar tier. Complementary by
  // construction: byDayRegular + byDayPre === byDayAll, day for day.
  const byDayRegular = {};
  const byDayPre = {};
  let regularTotal = 0, preTotal = 0;
  let weekdayRegular = 0, weekdayPre = 0;   // weekday-only mirrors, for the Weekday Avg tile
  // Last-click acquisition split of the SAME regular cohort, three-way so the
  // unknown is visible rather than silently folded into "organic":
  //   paid    -- the booking contact carries a UTM mapUtmToChannel recognises
  //   organic -- no paid UTM, but a self-describing one is present ('organic', 'referral')
  //   untagged-- no utm_source and no utm_medium at all; origin genuinely unknown
  // Counted in the else branch below, which IS the definition of regularTotal, so
  // the three can never drift from the Regular Demos Scheduled card.
  let regularPaid = 0, regularOrganicTagged = 0, regularUntagged = 0;
  const byDayRegularPaid = {};
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
  let weekdayTotalAll = 0;               // INCLUDES Very Small (webinars); excludes weekends
  let lowTrafficCountWeekday = 0;        // Pre-launch contacts that landed on a weekday
  let low10KCountWeekday = 0;            // Pre-launch + 0-10K contacts on a weekday
  // Dedupe: fetchScheduledContacts ORs two filter groups (meeting bookers +
  // Livestorm Webinar registrants), so a contact could appear twice if they
  // booked AND registered for a webinar. Key by id and skip dupes.
  const seenIds = new Set();
  // Acquisition mix. Counted over every deduped contact — the same population
  // byWebTrafficTrue uses — so "Demo Acquisition" and "Demo Composition" agree
  // on their denominator. Webinar registrants are NOT skipped here for the same
  // reason they aren't skipped from byWebTrafficTrue.
  const byUtmS = {}, byUtmM = {}, byUtmC = {};
  // Same three dimensions, split by the brand's tier, so the Demo Acquisition
  // card can scope its mix the way every other card on the page does. Each
  // segment is capped on its OWN distribution in the return block -- capping
  // the combined map and then splitting it would put the wrong rows in "Other".
  const byUtmSReg = {}, byUtmMReg = {}, byUtmCReg = {};
  const byUtmSPre = {}, byUtmMPre = {}, byUtmCPre = {};
  let utmTotalRegular = 0, utmTotalPre = 0;
  let utmTotal = 0;
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
    // Same "Not set" fallback the clinician acquisition mix uses, so a blank and
    // a missing property read the same way on both dashboards.
    const _utm = (k) => ((c.properties?.[k] || '').trim()) || 'Not set';
    const _us = _utm('utm_source'), _um = _utm('utm_medium'), _uc = _utm('utm_campaign');
    byUtmS[_us] = (byUtmS[_us] || 0) + 1;
    byUtmM[_um] = (byUtmM[_um] || 0) + 1;
    byUtmC[_uc] = (byUtmC[_uc] || 0) + 1;
    // Tier split, on the same pre-launch test as byDayRegular / byDayPre below.
    { const _pre = wt.includes('pre-launch');
      const _S = _pre ? byUtmSPre : byUtmSReg, _M = _pre ? byUtmMPre : byUtmMReg, _C = _pre ? byUtmCPre : byUtmCReg;
      _S[_us] = (_S[_us] || 0) + 1; _M[_um] = (_M[_um] || 0) + 1; _C[_uc] = (_C[_uc] || 0) + 1;
      if (_pre) utmTotalPre++; else utmTotalRegular++; }
    // One increment per contact, so each dimension's rows sum to exactly this —
    // _clinicianPie divides by the caller's total, and a rows array that didn't
    // sum to it would report percentages that don't reach 100%.
    utmTotal++;
    // Webinar-inclusive daily series (bucketed by createdate in ET) — feeds the
    // "CPD per Week" chart so it aggregates to the Efficiency "CPD · All" card.
    { const _ad = new Date(cd); const _aet = new Date(_ad.getTime() + etOff(_ad) * 3600000); const _ak = fmt(_aet);
      byDayAll[_ak] = (byDayAll[_ak]||0) + 1;
      const _adow = _aet.getUTCDay(); if (_adow !== 0 && _adow !== 6) weekdayTotalAll++;
      // Split on the TRUE tier, before the webinar skip below — a webinar
      // registrant still carries its brand's real traffic tier.
      const _isWeekdayAll = (_adow !== 0 && _adow !== 6);
      if (wt.includes('pre-launch')) { byDayPre[_ak] = (byDayPre[_ak]||0) + 1; preTotal++; if (_isWeekdayAll) weekdayPre++; }
      else { byDayRegular[_ak] = (byDayRegular[_ak]||0) + 1; regularTotal++; if (_isWeekdayAll) weekdayRegular++;
        // RAW properties, not the _us/_um pair below -- those are 'Not set'-normalised,
        // which would make every untagged contact look tagged.
        const _rs = (c.properties?.utm_source || '').trim(), _rm = (c.properties?.utm_medium || '').trim();
        if (mapUtmToChannel(_rs, _rm)) { regularPaid++; byDayRegularPaid[_ak] = (byDayRegularPaid[_ak]||0) + 1; }
        else if (_rs || _rm) regularOrganicTagged++;
        else regularUntagged++; } }
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
    total: dailyTotal, byDay, byDayScale, byDayScale10KPlus, byDayAll, byDayRegular, byDayPre,
    regularTotal, preTotal, weekdayRegular, weekdayPre, byWebTraffic, byWebTrafficTrue,
    regularPaid, regularOrganicTagged, regularUntagged, byDayRegularPaid,
    // Acquisition mix, capped at 8 slices like the clinician equivalent.
    utmSource: cappedCounts(byUtmS, 8), utmMedium: cappedCounts(byUtmM, 8), utmCampaign: cappedCounts(byUtmC, 8),
    utmSourceRegular: cappedCounts(byUtmSReg, 8), utmMediumRegular: cappedCounts(byUtmMReg, 8),
    utmCampaignRegular: cappedCounts(byUtmCReg, 8),
    utmSourcePre: cappedCounts(byUtmSPre, 8), utmMediumPre: cappedCounts(byUtmMPre, 8),
    utmCampaignPre: cappedCounts(byUtmCPre, 8),
    utmTotalRegular, utmTotalPre,
    utmTotal,
    lowTrafficCount, low10KCount, lowTrafficCompanies,
    // Weekday-only mirrors of total / low-tier counts, exposed so prior &
    // last-month deltas in the Weekday Avg tile are apples-to-apples (only
    // weekday demos in the numerator, weekday-day-count in the denominator).
    weekdayTotal, weekdayTotalScale, weekdayTotalScale10KPlus, weekdayTotalAll,
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
  // Small-brand web-traffic tiers (Pre-launch + 0-10K + Very Small) — used for
  // every "excl. small brands" variant in this function.
  const SMALL_BRAND_TIERS = new Set(['Pre-launch / just launching','0-10K monthly web visitors','Very Small']);
  let totalExtended = 0, totalExtendedStrict = 0;
  // Ids of the deals each metric counted. Used to dedupe the webinar-date deals
  // folded in by applyWebinarDateDeals — without this, a deal with both a demo
  // booked in the window and a webinar-date contact would be counted twice.
  const extendedDealIds = new Set();
  const supposedDealIds = new Set();
  for (const d of deals) {
    const ddb = d.properties?.date_demo_booked;
    const rmd = d.properties?.rescheduled_meeting_date;
    const eff = rmd || ddb;
    const effMs = dateMs(eff);
    // Pre-launch brands are excluded from every Volume and Quality figure.
    if ((d.properties?.average_monthly_web_traffic__cloned_ || '') === PRE_LAUNCH_TIER) continue;
    if (!isNaN(effMs)) {
      totalExtended++;
      if (d.id) extendedDealIds.add(String(d.id));
      // "excl. small brands" variant — keyed off the deal-level cloned tier.
      if (!SMALL_BRAND_TIERS.has(d.properties?.average_monthly_web_traffic__cloned_ || '')) totalExtendedStrict++;
    }
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
  // Held / no-show / cancelled with PRE-LAUNCH ONLY removed. Distinct from the
  // *Strict counters below, which also drop 0-10K and Very Small.
  let heldExPre=0, noShowExPre=0, cancelledExPre=0;
  // Qualification RATE keeps its own outcome-based counts. Redefining a Qualified
  // Demo as "held" (below) would otherwise fold disqualified-after-demo into the
  // rate's numerator while leaving it in the denominator, which can only push the
  // rate toward 100% and would stop measuring anything.
  let qualOutcomeQualified=0;
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
    const _isPreLaunchDeal = (p.average_monthly_web_traffic__cloned_ || '') === PRE_LAUNCH_TIER;
    if (!_isPreLaunchDeal) {
      if (att === 'Demo Given (originally scheduled)' || att === 'Demo Given (rescheduled)') heldExPre++;
      else if (att === 'No Show') noShowExPre++;
      else if (att === 'Cancelled before demo') cancelledExPre++;
    }
    if (att === 'Demo Given (originally scheduled)') demoGivenOrigCount++;
    else if (att === 'Demo Given (rescheduled)') demoGivenReschedCount++;
    if (deal.id && (att === 'Demo Given (originally scheduled)' || att === 'Demo Given (rescheduled)'
        || cat === 'noShow' || att === 'Cancelled before demo')) supposedDealIds.add(String(deal.id));

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

    // A QUALIFIED DEMO is a demo that was HELD, excluding pre-launch brands
    // (Alex, 2026-10-01). The qualification OUTCOME no longer gates it: a demo
    // that happened and was then disqualified still happened, as does one not yet
    // evaluated. What still does not count is anything never held — no-shows and
    // cancelled-before-demo.
    //
    // Pre-launch is excluded by the definition itself, so the headline count and
    // the old "scale" count are now the same number; both are kept because
    // several consumers read one or the other.
    if (demoDidHappen(att, rawSt) && !_isPreLaunchDeal) {
      qualifiedRawCount++;
      qualifiedRawScaleCount++;
    }
    // Outcome tallies, for the rate only — same population as the count above,
    // or the Qualification rate compares two different sets of demos.
    if (!_isPreLaunchDeal) {
      if (qo === 'Qualified') qualOutcomeQualified++;
      else if (qo === 'Disqualified') disqualifiedRawCount++;
      else if (qo === 'Not yet evaluated') notYetEvalCount++;
    }

    // Per-rep
    if (!byRep[ownerId]) { byRep[ownerId] = { total:0 }; for (const c of FUNNEL_ORDER) byRep[ownerId][c] = 0; }
    // QUALIFIED DEMO, new definition: the demo was HELD, excluding pre-launch.
    // Tallied separately from `cat` because the categoriser's buckets must keep
    // PARTITIONING the population — a held-then-disqualified demo is both a
    // Qualified Demo and 'pruned', so folding the new rule into `cat` would make
    // the funnel breakdown sum to more than its own total.
    const isQualDemo = demoDidHappen(att, rawSt) && !_isPreLaunchDeal;
    byRep[ownerId].total++; byRep[ownerId][cat]++;
    byRep[ownerId].qualDemo = (byRep[ownerId].qualDemo||0) + (isQualDemo?1:0);

    // Per-channel via UTM
    const ch = mapUtmToChannel(p.utm_source, p.utm_medium) || 'unattributed';
    if (byChannel[ch]) { byChannel[ch][cat]++; byChannel[ch].total++;
      byChannel[ch].qualDemo = (byChannel[ch].qualDemo||0) + (isQualDemo?1:0); }

    // Stage attribution
    const stage = detectFunnelStage(p.utm_source, p.utm_medium, p.utm_campaign);
    byStage[stage][cat]++;
    byStage[stage].qualDemo = (byStage[stage].qualDemo||0) + (isQualDemo?1:0);
    byStage[stage].total++;

    // Daily chart — bucket by EFFECTIVE demo date so rescheduled demos plot
    // on the day they actually happen, not the original booked day.
    const _dailyEff = p.rescheduled_meeting_date || p.date_demo_booked;
    const ds = _dailyEff ? _dailyEff.substring(0,10) : null;
    if (ds) { if (!byDay[ds]) byDay[ds] = { deals:0, qualified:0 }; byDay[ds].deals++; if (isQualDemo) byDay[ds].qualified++; }
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
  // Rate stays on the outcome fields: of the demos actually evaluated, what share
  // were judged qualified. Independent of the held-based count above.
  const qualRateDenom = qualOutcomeQualified + disqualifiedRawCount;
  const qualRateDenomLegacy = qualRateDenom; // kept as alias for downstream readers
  const disqualificationRate = qualRateDenom > 0 ? (disqualifiedRawCount / qualRateDenom) * 100 : 0;
  const qualificationRate = qualRateDenom > 0 ? (qualOutcomeQualified / qualRateDenom) * 100 : 0;

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
    totalExtended, totalExtendedStrict, demoGivenOrigCount, demoGivenReschedCount, demoGivenScaleCount, demoGivenStrictCount, noShowScaleCount,
    // Strict (small-brand-excluded) numerators for the canonical rates.
    noShowStrictCount, cancelledStrictCount,
    qualifiedRawCount, disqualifiedRawCount, notYetEvalCount, qualifiedRawScaleCount,
    heldExPre, noShowExPre, cancelledExPre,
    extendedDealIds, supposedDealIds,
    qualOutcomeQualified,
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
  // Deals closed per Pacific day — the CAC denominator for the Weekly Cost
  // chart. These deals are ALREADY window-filtered on closedate upstream
  // (fetchClosedWonDeals), so bucketing by that same field keeps the chart's
  // weeks consistent with the window that selected the rows.
  // closedate is blank on a minority of won deals; those cannot be placed on a
  // timeline at all, so they are counted separately rather than dropped
  // silently — a CAC week is wrong in a way nobody can see if its denominator
  // quietly lost rows.
  const byDay = {};
  let noCloseDate = 0;

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

    const _cwDay = ptDayKeyOf(p.closedate);
    if (_cwDay) byDay[_cwDay] = (byDay[_cwDay]||0) + 1; else noCloseDate++;
  }

  const avgCycleDays = cycleDays.length > 0 ? cycleDays.reduce((a,b)=>a+b,0)/cycleDays.length : null;
  return { mrr, count, byRep, byChannel, byStage, avgCycleDays, byDay, noCloseDate };
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
function attBump(o, att, isWon) {
  // isWon (optional): a Closed Won deal definitionally had its demo happen, so
  // it counts as "held" even if the demo_attendance_status stamp is missing.
  // This guarantees signed ⊆ held (so % Signed + % Pending = 100%). Callers
  // that omit isWon keep the pure attendance behavior.
  if (isWon || att === 'Demo Given (originally scheduled)' || att === 'Demo Given (rescheduled)') o.attHeld++;
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

// ── Clinician Funnel ────────────────────────────────────────────────────────
// Pipeline "Clinician" (2486191840). Order verified against HubSpot; note the
// non-obvious sequence — Form Submitted comes AFTER Meeting Happened/Signed Up.
const CLINICIAN_PIPELINE_ID = '2486191840';
// The Clinician Funnel and the All Clinicians table are HISTORY views: they
// always reach back to this date regardless of the page's date filter, because
// a funnel scoped to one month can't show a pipeline that takes months to walk.
// Everything else on the page (Daily Activity, Composition, Ads, Weekly CPD)
// stays on the page window — spend is window-scoped, so its denominators must be.
const CLINICIAN_HISTORY_FROM = '2026-07-01';
// Order here IS the funnel order the dashboard renders. Stage ids verified
// against the live HubSpot `dealstage` property on 2026-08-24.
// NOTE: HubSpot's enumeration happens to list Clinician Inquiry AFTER Signed Up;
// it sits BEFORE it here because that is the actual funnel order.
const CLINICIAN_STAGES = [
  // Order here IS the combined ("Both") funnel's display order. The two flows'
  // ENTRY stages are kept adjacent at the top -- Meeting Scheduled opens the demo
  // path, Clinician Inquiry opens the virtual path -- so the combined view reads
  // as "everyone enters here" rather than dropping the virtual entry point in
  // between two demo-only stages. Safe to reorder: every consumer looks these up
  // by id (see the note below).
  { id: '4128510704', label: 'Meeting Scheduled' },
  { id: '4201261817', label: 'Clinician Inquiry' },
  { id: '4128510705', label: 'Meeting Happened' },
  { id: '4128510709', label: 'Signed Up' },
  { id: '4133870316', label: 'Form Submitted' },
  { id: '4133870317', label: 'Clinician Approved' },
  { id: '4135230193', label: 'Fully Onboarded' },
  { id: '4133870318', label: '1st Sample Shipped' },
  { id: '4164547317', label: '1st Review Submitted' },
  { id: '4164547318', label: '2nd Review Submitted' },
  { id: '4164547319', label: '3rd+ Review Submitted' },   // matches HubSpot's own stage name
];
// Referenced by id, never by array index. The previous code reached in as
// CLINICIAN_STAGES[0] / [5], which meant inserting a stage anywhere before
// Fully Onboarded silently repointed the velocity + CAC metrics at the wrong
// stage — the old comment above the list existed only to work around that.
const CLINICIAN_STAGE_MEETING_SCHEDULED = '4128510704';
const CLINICIAN_STAGE_FULLY_ONBOARDED   = '4135230193';
// Every stage referenced by NAME anywhere below. Rates used to index cnt[] by
// position (cnt[7]/cnt[6] for the review rate, cnt[3] for close rate,
// reached[5] for velocity), which silently repointed at the wrong stages the
// moment Clinician Inquiry was inserted mid-list. Look them up by id instead.
const CLS = {
  MEETING_SCHEDULED: '4128510704', MEETING_HAPPENED: '4128510705',
  CLINICIAN_INQUIRY: '4201261817', SIGNED_UP:        '4128510709',
  FORM_SUBMITTED:    '4133870316', CLINICIAN_APPROVED:'4133870317',
  FULLY_ONBOARDED:   '4135230193', SAMPLE_1:         '4133870318',
  REVIEW_1:          '4164547317', REVIEW_2:         '4164547318',
  REVIEW_3:          '4164547319',
};

// The Clinician Funnel's nine stages, in the order the dashboard shows them.
// Four are pre-CRM (ad + analytics) and five are HubSpot stages, so this is a
// deliberate sequence rather than a subset of CLINICIAN_FLOWS — neither existing
// flow matches it. The ad/analytics stages have no "here now": nobody sits in an
// impression. Window-scoped throughout, which is what lets the conversion rate
// from CTA Click to Inquiries mean anything.
const CLN_FUNNEL9 = [
  { key: 'impressions', label: 'Impressions',      sub: 'Meta \u00b7 Clinician campaigns' },
  { key: 'linkClicks',  label: 'Link Clicks',      sub: 'Meta \u00b7 Clinician campaigns' },
  { key: 'ctaClicks',   label: 'CTA Click',        sub: 'click_join_for_free events' },
  // acq: read from the Acquisition populations, not a dealstage. Key Metrics is
  // the authority for these two, and it counts deduped CONTACTS; a deal-based
  // stage count is a different number and the two were visibly disagreeing.
  { key: 'inquiry',     label: 'Inquiries',        sub: 'clinician contacts created', acq: 'contacts' },
  { key: 'onboarded',   label: 'Fully Onboarded',  sub: 'kickoff call booked',    acq: 'submission' },
  { key: 'sample',      label: 'Sample Shipped',   sub: '1st Sample Shipped', id: CLS.SAMPLE_1 },
  // 1st + 2nd + 3rd+ combined. Ever-reached is nested (you cannot reach 2nd
  // without 1st) so reached is the max, but "here now" is the SUM — a clinician
  // sitting at 3rd+ is not also sitting at 1st.
  { key: 'review',      label: 'Review Submitted', sub: '1st + 2nd + 3rd+',
    ids: [CLS.REVIEW_1, CLS.REVIEW_2, CLS.REVIEW_3] },
];
// Two workflows share this one HubSpot pipeline, each walking a different
// subset. A deal's own flow decides which stages it can legitimately be
// credited with — see the `reached` logic below.
const CLINICIAN_FLOWS = {
  demo: {
    label: 'Clinician Demo Flow',
    stages: [CLS.MEETING_SCHEDULED, CLS.MEETING_HAPPENED, CLS.SIGNED_UP, CLS.FORM_SUBMITTED,
             CLS.CLINICIAN_APPROVED, CLS.FULLY_ONBOARDED, CLS.SAMPLE_1,
             CLS.REVIEW_1, CLS.REVIEW_2, CLS.REVIEW_3],
    exits: ['4128854721', '4128510710'],   // No Show, Not a Fit
  },
  virtual: {
    label: 'Virtual Clinician Onboarding',
    // Confirmed by Alex 2026-09-08: the virtual path runs through Clinician
    // Approved, NOT Form Submitted. Consequence worth knowing: a virtual deal is
    // no longer credited with Form Submitted by monotonic inference, so it only
    // counts toward Form Submitted if it carries a real stage-entry timestamp for
    // it. That is what the Cost Per Signup denominator reads.
    stages: [CLS.CLINICIAN_INQUIRY, CLS.CLINICIAN_APPROVED, CLS.FULLY_ONBOARDED, CLS.SAMPLE_1,
             CLS.REVIEW_1, CLS.REVIEW_2, CLS.REVIEW_3],
    exits: ['4128510710'],                 // Not a Fit only — no meeting to no-show
  },
  // ASSUMED SEQUENCE — still unconfirmed. It NO LONGER mirrors Virtual: Virtual
  // was corrected to run through Clinician Approved (2026-09-08) and this one was
  // deliberately left alone, because correcting one unconfirmed path from a
  // different path's correction is a guess, not a fix. So Field still carries
  // Form Submitted. Confirm the real Field Rep path and swap the array below.
  field: {
    label: 'Field Rep Signup',
    stages: [CLS.CLINICIAN_INQUIRY, CLS.FORM_SUBMITTED, CLS.FULLY_ONBOARDED, CLS.SAMPLE_1,
             CLS.REVIEW_1, CLS.REVIEW_2, CLS.REVIEW_3],
    exits: ['4128510710'],
  },
};
// HubSpot `clinician_workflow` option value → flow key. Option values verified
// against the live property on 2026-08-24 (value === label for all three).
const CLINICIAN_WORKFLOW_MAP = {
  'virtual signup':  'virtual',
  'onboarding demo': 'demo',
  'field rep':       'field',
};
// Short label for the All Clinicians table pill.
const CLINICIAN_FLOW_SHORT = { demo: 'Demo', virtual: 'Virtual', field: 'Field' };
// The `clinician_workflow` deal property is authoritative. Deals created before
// it existed have it blank, so those fall back to the old inference: ever
// entered Clinician Inquiry → Virtual, else Demo. An unrecognised value also
// falls through rather than inventing a fourth flow.
function clinicianFlowOf(p) {
  const raw = (p.clinician_workflow || '').trim().toLowerCase();
  const mapped = CLINICIAN_WORKFLOW_MAP[raw];
  if (mapped) return mapped;
  return clnEnteredAt(p, CLS.CLINICIAN_INQUIRY) ? 'virtual' : 'demo';
}
const CLINICIAN_STAGE_NO_SHOW = '4128854721';
const CLINICIAN_STAGE_NOT_A_FIT = '4128510710';
const CLINICIAN_ALL_STAGE_IDS = CLINICIAN_STAGES.map(x => x.id).concat([CLINICIAN_STAGE_NO_SHOW, CLINICIAN_STAGE_NOT_A_FIT]);
// Stage-entry timestamps give a true "ever reached this stage" signal (and the
// stamps velocity needs) rather than a current-stage snapshot.
//
// THE PROPERTY IS `hs_v2_date_entered_<stageId>`, NOT `hs_date_entered_<stageId>`.
// This file used the un-prefixed name everywhere and it DOES NOT EXIST in this
// portal — verified against the live HubSpot property API, which returns it in
// propertiesNotFound. Every read silently evaluated to undefined, so:
//   • the funnel's "ever reached" fell back entirely on the positional backfill
//   • velocity (tMS / tFO) was permanently NaN
//   • anything reading a stage-entry date counted zero of everything
// Three clinician stages have NO entry property at all: Meeting Scheduled (the
// pipeline's first stage), No Show and Not a Fit. A deal is created straight
// into Meeting Scheduled, so createdate is the honest stand-in there; the two
// exit stages are genuinely undated and must not be faked.
const CLN_NO_ENTERED_PROP = new Set([CLINICIAN_STAGE_MEETING_SCHEDULED, CLINICIAN_STAGE_NO_SHOW, CLINICIAN_STAGE_NOT_A_FIT]);
function clnEnteredProp(stageId) {
  return CLN_NO_ENTERED_PROP.has(stageId) ? null : 'hs_v2_date_entered_' + stageId;
}
// REAL STAMPS ONLY. This used to fall back to createdate for Meeting Scheduled.
// Every deal has a createdate, so that fallback made the "off-path stages count
// only when HubSpot stamped them" loop credit EVERY Virtual and Field deal with
// Meeting Scheduled — a stage those flows never touch. It inflated the Meeting
// Scheduled reached-count by the entire non-demo population, and that count is
// the denominator of Show Rate, so Show Rate was reading too low.
function clnEnteredAt(p, stageId) {
  const prop = clnEnteredProp(stageId);
  return prop ? (p[prop] || null) : null;
}
// A deal is CREATED into the first stage of its OWN flow, so createdate is
// legitimate evidence for that one stage — but only along the path the deal
// actually walks. Kept separate from clnEnteredAt so it can never be mistaken
// for a universal stamp and leak onto another flow's stages.
function clnPathStartAt(p, seqIds) {
  return clnEnteredAt(p, seqIds[0]) || p.createdate || null;
}
const CLINICIAN_ENTERED_PROPS = CLINICIAN_ALL_STAGE_IDS.map(clnEnteredProp).filter(Boolean);
const CLINICIAN_DEAL_PROPS = [
  'dealstage', 'pipeline', 'clinician_meeting_date', 'clinician_meeting_booked_date',
  'clinician_attendance_status', 'clinician_qualification_outcome', 'createdate',
  'dealname', 'hubspot_owner_id',
  'utm_source', 'utm_medium', 'utm_campaign',
  // Which of the three workflows this deal belongs to. Authoritative — replaces
  // inferring it from whether the deal ever entered Clinician Inquiry.
  'clinician_workflow',
].concat(CLINICIAN_ENTERED_PROPS);
const CLINICIAN_CONTACT_PROPS = [
  'firstname','lastname','email','phone','clinician_specialty','clinician_type',
  'clinician_referral_source','npi_number','photo_verification_status','createdate',
  'message','proof_link','sample_preferences_notes','address','samples_tracking_number',
  'which_samples_shipped','clinician_license_credential_files',
  'utm_source','utm_medium','utm_campaign',
  'medical_degree_status','samples_arrival_date','number_of_samples_shipped',
  // Shipping address for Ready For Samples. The populated fields are `city` plus
  // the CUSTOM `contact_state` and `zip_code` — HubSpot's standard `state` and
  // `zip` are empty on every clinician checked, so do not swap these for them.
  'city','contact_state','zip_code',
  // "# Reviews Submitted" — a NUMBER on the contact (verified against the live
  // HubSpot property), so a cohort total is a SUM of this, not a row count.
  // It carries no date, so it can only ever be a running per-clinician total —
  // it cannot be attributed to a time period. Anything period-scoped must come
  // from the dated hs_v2_date_entered_<reviewStage> timestamps instead.
  'reviews_submitted',
];

// A "won" clinician is one who reached FULLY ONBOARDED — there is no closed-won
// stage in the clinician pipeline and no revenue on the deal, so the brand-side
// definition (dealstage = closedwon, attributed by UTM) counts nothing at all
// for this channel. Confirmed by Alex 2026-09-17.
//
// Counted by STAGE-ENTRY timestamp, not current stage: a clinician who onboarded
// in the window and has since moved on to 1st Sample Shipped still onboarded in
// the window. hs_v2_date_entered_<id> is a datetime, so it filters on epoch ms.
async function fetchClinicianOnboardedCount(token, from, to) {
  if (!from || !to) return null;
  return hsCount(token, 'deals', [{
    filters: [
      { propertyName: 'pipeline', operator: 'EQ', value: CLINICIAN_PIPELINE_ID },
      { propertyName: 'hs_v2_date_entered_' + CLS.FULLY_ONBOARDED, operator: 'GTE', value: String(toMsET(from)) },
      { propertyName: 'hs_v2_date_entered_' + CLS.FULLY_ONBOARDED, operator: 'LTE', value: String(toMsET(to, true)) },
    ],
  }]);
}
// Webinars that actually happened, for the Qualified Demo count.
//
// These deals carry NO date_demo_booked — they are webinar registrations, not
// demo bookings — so fetchPipelineDeals never returns them and the demo loop
// cannot see them. Hence a separate count rather than a flag inside that loop.
//
// Counted by STAGE-ENTRY stamp, not current stage: a webinar that happened and
// then progressed to Closed Won has left the stage but still happened. "Moved to
// Webinar Happened" is exactly what the entry stamp records.
const WEBINAR_HAPPENED_STAGE = '3741686471';
// Deal-side label for the pre-launch tier. Reading the deal's own property is
// more reliable than the contact company-name matching used elsewhere, which
// only sees pre-launch contacts created inside the same window.
const PRE_LAUNCH_TIER = 'Pre-launch / just launching';
async function fetchWebinarHeldCount(token, from, to) {
  if (!token || !from || !to) return null;
  return hsCount(token, 'deals', [{
    filters: [
      { propertyName: 'hs_v2_date_entered_' + WEBINAR_HAPPENED_STAGE, operator: 'GTE', value: String(toMsET(from)) },
      { propertyName: 'hs_v2_date_entered_' + WEBINAR_HAPPENED_STAGE, operator: 'LTE', value: String(toMsET(to, true)) },
    ],
  }]);
}
// Deals whose ASSOCIATED CONTACT has a Webinar Date inside the window.
//
// There is no deal-side webinar date property in this portal (verified: six
// semantic property searches against `deals` return zero webinar fields, while
// the same search on `contacts` ranks webinar_date second), and HubSpot CRM
// search cannot filter one object by an associated object's property. So this
// has to resolve contacts -> deals through the association API, the same route
// countAttendeesClosedWon takes.
//
// webinar_date is stored at midnight UTC, so the bounds use toMsUTC — NOT the ET
// offset used for createdate. fetchWebinarRegistrants sets that precedent.
// Only the "webinar ran in this window" group is used here: its second OR-group
// (created in window + has ANY webinar_date) would pull in people registered for
// a webinar outside the window, which is not what "Webinar Date in window" means.
async function fetchWebinarDateDealIds(token, from, to) {
  if (!token || !from || !to) return null;
  const contacts = await hsSearch(token, 'contacts', [
    { filters: [
      { propertyName: 'webinar_date', operator: 'GTE', value: String(toMsUTC(from)) },
      { propertyName: 'webinar_date', operator: 'LTE', value: String(toMsUTC(to, true)) },
    ]},
  ], ['webinar_date','webinar_has_attended','average_monthly_web_traffic'], 200, [{ propertyName: 'webinar_date', direction: 'DESCENDING' }], 50);
  // Attendance for the Webinar No-Show Rate. Free — these are the same contacts
  // the deal lookup already pages through. Anything other than an explicit
  // 'true' counts as a no-show, so a blank (never reconciled) reads as no-show
  // rather than silently shrinking the denominator.
  const seen = new Map();
  for (const c of (contacts || [])) {
    if (!c || !c.id) continue;
    // Pre-launch is excluded from every Volume and Quality figure. Matched on
    // the contact's own tier, lowercased, because this field is free-text on
    // contacts and the exact casing of the option has varied.
    if (String(c.properties?.average_monthly_web_traffic || '').toLowerCase().includes('pre-launch')) continue;
    seen.set(String(c.id), c);
  }
  let registrants = 0, attended = 0;
  for (const c of seen.values()) {
    registrants++;
    if (String(c.properties?.webinar_has_attended || '').toLowerCase() === 'true') attended++;
  }
  const ids = [...seen.keys()];
  if (!ids.length) return { dealIds: new Set(), contacts: 0, failed: 0, registrants: 0, attended: 0 };
  // Batch reads cap at 100 inputs. Chunk and accumulate partial results rather
  // than collapsing to zero on one bad batch — the failure mode that silently
  // zeroed webinar Closed Won before.
  const CHUNK = 100;
  const dealIds = new Set();
  let failed = 0;
  for (let i = 0; i < ids.length; i += CHUNK) {
    const batch = ids.slice(i, i + CHUNK);
    if (SUBREQ.exhausted) { failed += batch.length; continue; }
    try {
      SUBREQ.n++; SUBREQ.hubspot++;
      const r = await fetch('https://api.hubapi.com/crm/v4/associations/contacts/deals/batch/read', {
        method: 'POST',
        headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ inputs: batch.map(id => ({ id })) }),
      });
      if (!r.ok) {
        console.error('fetchWebinarDateDealIds assoc', r.status, (await r.text()).slice(0, 200));
        failed += batch.length; continue;
      }
      const j = await r.json();
      for (const row of (j.results || [])) {
        for (const t of (row.to || [])) {
          const did = String(t.toObjectId || '');
          if (did) dealIds.add(did);
        }
      }
    } catch (e) {
      console.error('fetchWebinarDateDealIds assoc threw', e && e.message);
      failed += batch.length;
    }
  }
  // The deal metrics these feed only ever see filterActiveBrands(...) — Paused,
  // Churned and Never Implemented deals are excluded from the pipeline before
  // processPipelineDeals runs. Associations carry no such filter, so read
  // brand_status and apply the SAME rule, or this adds back deals the metric
  // deliberately drops and the two halves stop describing one population.
  const active = new Set();
  let dealsFailed = 0;
  const resolved = [...dealIds];
  for (let i = 0; i < resolved.length; i += CHUNK) {
    const batch = resolved.slice(i, i + CHUNK);
    if (SUBREQ.exhausted) { dealsFailed += batch.length; continue; }
    try {
      SUBREQ.n++; SUBREQ.hubspot++;
      const r = await fetch('https://api.hubapi.com/crm/v3/objects/deals/batch/read', {
        method: 'POST',
        headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ inputs: batch.map(id => ({ id })), properties: ['brand_status','average_monthly_web_traffic__cloned_'] }),
      });
      if (!r.ok) {
        console.error('fetchWebinarDateDealIds deals', r.status, (await r.text()).slice(0, 200));
        dealsFailed += batch.length; continue;
      }
      const j = await r.json();
      for (const dd of filterActiveBrands(j.results || [])) {
        // Pre-launch is out of the Qualified Demo definition, so it is out of
        // the population those demos come from as well.
        if ((dd.properties?.average_monthly_web_traffic__cloned_ || '') === PRE_LAUNCH_TIER) continue;
        active.add(String(dd.id));
      }
    } catch (e) {
      console.error('fetchWebinarDateDealIds deals threw', e && e.message);
      dealsFailed += batch.length;
    }
  }
  if (failed || dealsFailed) {
    console.warn(`fetchWebinarDateDealIds: PARTIAL — ${failed}/${ids.length} contact lookups and `
      + `${dealsFailed}/${resolved.length} deal lookups failed; ${active.size} active of ${dealIds.size} resolved`);
  }
  return { dealIds: active, contacts: ids.length, resolved: dealIds.size, failed: failed + dealsFailed, registrants, attended };
}

// Folds webinar-date deals into "Demos to occur" and "Demos supposed to happen".
// Both additions are DEDUPED against the deals the metric already counts, so a
// deal that has a demo booked in the window AND an associated webinar-date
// contact is counted once, not twice.
function applyWebinarDateDeals(periodData, res) {
  if (!periodData || !periodData.pipeline || !res || !res.dealIds) return;
  const pl = periodData.pipeline;
  const ext = pl.extendedDealIds instanceof Set ? pl.extendedDealIds : new Set();
  const sup = pl.supposedDealIds instanceof Set ? pl.supposedDealIds : new Set();
  let extraExt = 0, extraSup = 0;
  for (const id of res.dealIds) {
    if (!ext.has(id)) extraExt++;
    if (!sup.has(id)) extraSup++;
  }
  pl.webinarDateDealCount = res.dealIds.size;
  pl.webinarDateContacts = res.contacts || 0;
  pl.webinarRegistrants = res.registrants || 0;
  pl.webinarAttended = res.attended || 0;
  pl.webinarNoShow = Math.max(0, (res.registrants || 0) - (res.attended || 0));
  pl.webinarDateExtraExtended = extraExt;
  pl.webinarDateExtraSupposed = extraSup;
  pl.totalExtended = (pl.totalExtended || 0) + extraExt;
}

// Folds the webinar count into a period's Qualified Demo totals. Kept additive
// and separate so webinarHeldCount stays legible on its own — a Qualified Demo
// figure that silently mixes two populations is impossible to reconcile against
// either source otherwise.
function applyWebinarHeld(periodData, n) {
  if (!periodData || !periodData.pipeline) return;
  const pl = periodData.pipeline;
  if (pl.qualifiedDemoOnlyCount == null) pl.qualifiedDemoOnlyCount = pl.qualifiedRawCount || 0;
  if (n != null) {
    pl.webinarHeldCount = n;
    pl.qualifiedRawCount = (pl.qualifiedRawCount || 0) + n;
    pl.qualifiedRawScaleCount = (pl.qualifiedRawScaleCount || 0) + n;
  }
  // Qualification rate follows the Qualified Demo definition, so a Webinar
  // Happened lifts the rate the same way a held demo does. It ran on the
  // outcome fields before (qualOutcomeQualified), which ignored webinars
  // entirely and so never moved when the webinar count did. Recomputed HERE,
  // after the fold, because the webinar total isn't known inside
  // processPipelineDeals. Runs even when n is null so a failed webinar fetch
  // still leaves the rate on the current definition rather than the old one.
  const den = (pl.qualifiedRawCount || 0) + (pl.disqualifiedRawCount || 0);
  pl.qualRateDenom = den;
  pl.qualificationRate = den > 0 ? ((pl.qualifiedRawCount || 0) / den) * 100 : 0;
  pl.disqualificationRate = den > 0 ? ((pl.disqualifiedRawCount || 0) / den) * 100 : 0;
}

// Clinician inquiries attributed to each ad campaign, for the Clinician Ad
// Tables' Inquiries / CPI columns. Counts CONTACTS whose utm_campaign matches the
// campaign name and who were created in the window — the same attribution the
// Acquisition card's UTM Campaign pie uses, so the two agree.
//
// One count per campaign rather than paging every clinician contact and bucketing
// client-side: each is a single subrequest that reads `total`, and the clinician
// channel carries a handful of campaigns. Capped regardless so a portal with a
// long campaign list cannot eat the subrequest budget.
async function fetchClinicianInquiriesByCampaign(token, names, from, to) {
  const out = {};
  if (!token || !Array.isArray(names) || !names.length || !from || !to) return out;
  const fMs = String(toMsET(from)), tMs = String(toMsET(to, true));
  const wanted = names.filter(n => n && n !== '(no name)').slice(0, 15);
  // Two at a time, not all at once. Firing one search per campaign in parallel
  // is the quickest way to trip HubSpot's search rate limit, and a throttled
  // count is indistinguishable from a real zero once it reaches the table.
  for (let i = 0; i < wanted.length; i += 2) {
    const batch = wanted.slice(i, i + 2);
    const counts = await Promise.all(batch.map(n => hsCount(token, 'contacts', [{
      filters: [
        { propertyName: 'utm_campaign', operator: 'EQ', value: n },
        { propertyName: 'createdate', operator: 'GTE', value: fMs },
        { propertyName: 'createdate', operator: 'LTE', value: tMs },
      ],
    }])));
    batch.forEach((n, k) => { if (counts[k] != null) out[n] = counts[k]; });
  }
  return out;
}

// Current + prior + prior-month in one go, so the Won column keeps its deltas.
// Three cheap count calls; each degrades to null independently.
async function fetchClinicianOnboardedCounts(token, current, prior, priorMonth) {
  // Sequential for the same reason as the campaign counts: three concurrent
  // searches is enough to get one of them throttled, and a throttled count
  // silently suppresses the Won column's delta.
  const cur = await fetchClinicianOnboardedCount(token, current && current.from, current && current.to);
  const pri = prior ? await fetchClinicianOnboardedCount(token, prior.from, prior.to) : null;
  const pm = priorMonth ? await fetchClinicianOnboardedCount(token, priorMonth.from, priorMonth.to) : null;
  return { cur, prior: pri, pm };
}
// Overwrites the clinician channel's won counts on a built response. Shared by
// both endpoints so Channel Performance and the Ad KPI CAC card cannot drift
// apart. Leaves MRR/ARR alone: an onboarded clinician carries no revenue, and
// inventing one would be worse than the blank the column already shows.
function applyClinicianWon(channels, counts) {
  const c = channels && channels.clinician;
  if (!c || !counts) return;
  if (counts.cur != null) c.closedWon = counts.cur;
  if (counts.prior != null) c.priorClosedWon = counts.prior;
  // The two endpoints spell the prior-month key differently (pmClosedWon in the
  // analyzer, priorMonthClosedWon in the Detailed Dashboard payload). Setting
  // both is cheaper than making this helper care which caller it has.
  if (counts.pm != null) { c.pmClosedWon = counts.pm; c.priorMonthClosedWon = counts.pm; }
  c.wonBasis = 'Reached Fully Onboarded in this window';
}

// Deals in the Clinician pipeline created within the window. Cohorting on
// createdate (not clinician_meeting_date) means a deal is counted from the
// moment it enters the pipeline, so early-stage deals with no meeting booked
// yet are included -- the meeting-date basis silently dropped them.
// createdate is a datetime, so it filters on epoch ms (toMsET), not YYYY-MM-DD.
async function fetchClinicianDeals(token, from, to) {
  const fMs = String(toMsET(from));
  const tMs = String(toMsET(to, true));
  return hsSearch(token, 'deals', [{
    filters: [
      { propertyName: 'pipeline', operator: 'EQ', value: CLINICIAN_PIPELINE_ID },
      { propertyName: 'createdate', operator: 'GTE', value: fMs },
      { propertyName: 'createdate', operator: 'LTE', value: tMs },
    ],
  }], CLINICIAN_DEAL_PROPS, 200, [{ propertyName: 'createdate', direction: 'DESCENDING' }], 20);
}

// deal → associated contact properties, via two batch reads (associations then
// contacts) so the subrequest cost stays flat regardless of cohort size.
async function fetchClinicianContacts(token, dealIds) {
  const out = {};
  if (!dealIds || !dealIds.length) return out;
  const chunk = (arr, n) => { const o = []; for (let i = 0; i < arr.length; i += n) o.push(arr.slice(i, i + n)); return o; };
  const dealToContact = {};
  const contactIds = new Set();
  for (const batch of chunk(dealIds, 100)) {
    try {
      const r = await fetch('https://api.hubapi.com/crm/v4/associations/deals/contacts/batch/read', {
        method: 'POST',
        headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ inputs: batch.map(id => ({ id: String(id) })) }),
      });
      if (!r.ok) { azDrain(r); console.error('clinician assoc', r.status); continue; }
      const j = await r.json();
      for (const row of (j.results || [])) {
        const did = row.from && row.from.id; if (!did) continue;
        // Keep EVERY associated contact, not just the first. A clinician with a
        // duplicate contact record (two HubSpot contacts for one person, both
        // associated to the same deal) was silently resolved to whichever came
        // back first — and if that was the stale duplicate, every field read off
        // the contact was blank. Real case: Saswatha Anireddy had
        // saswatha@hotmail.com (6 reviews) and saswatha.anireddy1@gmail.com
        // (none) on one deal; the empty one won, so Total Reviews Submitted read
        // 103 from 21 clinicians instead of 109 from 22.
        const ids = (row.to || []).map(x => x && x.toObjectId).filter(Boolean).map(String);
        if (ids.length) { dealToContact[did] = ids; ids.forEach(id => contactIds.add(id)); }
      }
    } catch (e) { console.error('clinician assoc threw', e.message); }
  }
  const byContact = {};
  for (const batch of chunk(Array.from(contactIds), 100)) {
    try {
      const r = await fetch('https://api.hubapi.com/crm/v3/objects/contacts/batch/read', {
        method: 'POST',
        headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ properties: CLINICIAN_CONTACT_PROPS, inputs: batch.map(id => ({ id })) }),
      });
      if (!r.ok) { azDrain(r); console.error('clinician contacts', r.status); continue; }
      const j = await r.json();
      for (const c of (j.results || [])) byContact[String(c.id)] = Object.assign({ __cid: String(c.id) }, c.properties || {});
    } catch (e) { console.error('clinician contacts threw', e.message); }
  }
  // Merge a deal's contacts into one record. Duplicates are partial in different
  // places, so take the first non-empty value per field; reviews_submitted takes
  // the MAX rather than a sum, because two records for the SAME person are not
  // two people's reviews. __cid stays the first id so the per-clinician dedupe
  // downstream still resolves two deals for one person to one clinician.
  const NUMERIC_MAX = ['reviews_submitted'];
  for (const did in dealToContact) {
    const recs = dealToContact[did].map(id => byContact[id]).filter(Boolean);
    if (!recs.length) continue;
    if (recs.length === 1) { out[did] = recs[0]; continue; }
    const merged = Object.assign({}, recs[0]);
    merged.__cid = recs[0].__cid;
    merged.__mergedFrom = recs.map(r => r.__cid);
    for (const r of recs.slice(1)) {
      for (const k in r) {
        if (k === '__cid' || k === '__mergedFrom') continue;
        if (NUMERIC_MAX.indexOf(k) >= 0) {
          const a = parseFloat(merged[k]), b = parseFloat(r[k]);
          const best = Math.max(isNaN(a) ? -Infinity : a, isNaN(b) ? -Infinity : b);
          if (best > -Infinity) merged[k] = String(best);
          continue;
        }
        if (merged[k] == null || merged[k] === '') merged[k] = r[k];
      }
    }
    console.log(`clinician contacts: deal ${did} has ${recs.length} associated contacts (${merged.__mergedFrom.join(', ')}) — merged`);
    out[did] = merged;
  }
  return out;
}

// Clinician CONTACTS created per day, bucketed on the contact's createdate and
// deduped by contact id. Sourced from the contacts associated with the cohort's
// deals, so scope still follows the funnel's deal cohort.
// Deliberately uses the RAW deal list — the future-meeting exclusion applies to
// funnel outcomes, but a meeting booked today for next week is still a booking
// today, so filtering it here would undercount activity.
function buildClinicianDaily(deals, contactsByDeal, from, to) {
  const byDay = {};
  // Bucket by PACIFIC calendar day, matching every other date boundary.
  // Previously a loose /^\d{4}-\d{2}-\d{2}/ (no anchor at the end) matched
  // HubSpot's full ISO timestamp and sliced off the UTC date, so a contact
  // created 7pm PDT (= 02:00Z next day) was counted on the FOLLOWING day.
  // Only a bare YYYY-MM-DD value is taken as-is; anything with a time
  // component is shifted into Pacific first.
  const dayKey = (v) => {
    if (!v) return null;
    const str = String(v);
    if (/^\d{4}-\d{2}-\d{2}$/.test(str)) return str;          // date-only: already local
    const t = /^\d+$/.test(str) ? parseInt(str) : new Date(str).getTime();
    if (isNaN(t)) return null;
    const d = new Date(t + ptOff(new Date(t)) * 3600000);
    return `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, '0')}-${String(d.getUTCDate()).padStart(2, '0')}`;
  };
  // Seed every day in the window so gaps render as zero rather than collapsing.
  let cur = from;
  let guard = 0;
  while (cur <= to && guard++ < 800) {
    byDay[cur] = 0;
    const d = new Date(cur + 'T00:00:00Z'); d.setUTCDate(d.getUTCDate() + 1);
    cur = `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, '0')}-${String(d.getUTCDate()).padStart(2, '0')}`;
  }
  // Counts DEALS, bucketed on the DEAL's own createdate.
  // Deliberately NOT deduped by contact, and NOT gated on having an associated
  // contact. The funnel counts deals (one funnel row = one deal), and CPS / CAC
  // read their denominators straight off it, so a contact-deduped Daily Activity
  // made CPD the only cost card on the page measured in a different unit.
  // Two consequences of bucketing on the DEAL date rather than the contact's:
  //   * a deal whose contact was created in an earlier month is no longer thrown
  //     out as `outside` -- it belongs to the window its DEAL was created in;
  //   * a deal with no associated contact is now counted instead of dropped.
  // `contacts` still reports the unique-person count so it stays visible.
  let total = 0, noDate = 0, noContact = 0, outside = 0;
  const seen = new Set();
  for (const deal of deals) {
    const p = deal.properties || {};
    const cp = contactsByDeal[deal.id];
    // Tracked for the footnote, no longer a reason to skip the deal.
    if (!cp) noContact++;
    else seen.add(cp.__cid || ('deal:' + deal.id));
    const k = dayKey(p.createdate);
    if (!k) { noDate++; continue; }
    if (k < from || k > to) { outside++; continue; }
    byDay[k] = (byDay[k] || 0) + 1;
    total++;
  }
  // Weekday average ignores Sat/Sun, matching the Detailed Dashboard's tile.
  let wdSum = 0, wdN = 0;
  for (const k in byDay) {
    const dow = new Date(k + 'T12:00:00Z').getUTCDay();
    if (dow === 0 || dow === 6) continue;
    wdSum += byDay[k]; wdN++;
  }
  // Every day in the window — the tile is "Daily Avg", so weekends count.
  const _dayMs = 86400000;
  const _spanDays = (from && to)
    ? Math.max(1, Math.round((Date.parse(to + 'T00:00:00Z') - Date.parse(from + 'T00:00:00Z')) / _dayMs) + 1)
    : Object.keys(byDay).length;
  return { byDay, total, noDate, noContact, outside, contacts: seen.size,
    weekdayAvg: wdN ? wdSum / wdN : null, weekdayDays: wdN,
    dailyAvg: _spanDays > 0 ? total / _spanDays : null, days: _spanDays };
}

// Clinician composition: unique CONTACTS broken down by type and specialty.
// Deduped by contact id for the same reason Daily Activity is — one clinician
// with two deals is one person, and counting deals would skew the mix toward
// whoever has the most pipeline activity.
// Ad metrics for the Clinician channel only. Spend comes from the same Windsor
// rows the rest of the dashboard uses, filtered to Meta rows whose campaign name
// marks them as Clinician Targeting — the identical predicate that splits the
// channel elsewhere, so this can't drift from Channel Performance.
// funnelAll and cliniciansAdded are RETAINED but no longer drive the two cost
// cards (2026-09-24). Both denominators used to be whole-population counts —
// every clinician deal created, and everyone who ever reached Fully Onboarded —
// while the numerator was only the clinician CAMPAIGNS' spend. That divided paid
// spend by an all-source denominator and understated both costs by however much
// arrived organically. They are still passed so the card can show what share of
// the population the paid campaigns actually account for.
function buildClinicianAds(windsorRows, budgets, funnelAll, cliniciansAdded, rows, from, to) {
  let spend = 0, conversions = 0;
  const byDay = {};
  for (const r of (windsorRows || [])) {
    const ds = (r.datasource || '').toLowerCase();
    if (!/facebook|meta|fb|ig|instagram/.test(ds)) continue;
    if (!isClinicianCampaign(r.campaign_name)) continue;
    // This takes from/to and, until now, used them only for budget pacing — the
    // spend total summed every row it was handed. That was safe only because
    // the caller fetched exactly the page window. The Trend Analysis section
    // needs a much wider fetch, so the window has to be enforced HERE or the
    // page's Ad Spend silently becomes three months of spend.
    const rk = String(r.date || '').slice(0, 10);
    if (from && rk && rk < from) continue;
    if (to && rk && rk > to) continue;
    const v = parseFloat(r.spend) || 0;
    spend += v;
    // Conversions the clinician campaigns actually reported, summed across BOTH
    // CAPI eras (see CLINICIAN_CONV_FIELDS) — the same number the Conversions
    // column totals in the Clinician Ad Tables.
    conversions += clinicianConvCount(r);
    if (rk) byDay[rk] = (byDay[rk] || 0) + v;
  }
  // Inquiries ATTRIBUTED to those campaigns: clinicians in this window whose
  // utm_campaign is one of them. Same matcher that decides which Windsor spend
  // counts as clinician spend, so numerator and denominator describe one set.
  let inquiriesAttr = 0;
  for (const row of (rows || [])) {
    if (isClinicianCampaign(row && row.utmCampaign)) inquiriesAttr++;
  }
  const budget = (budgets && budgets.clinician) || 0;
  // True CPD and Cost Per Signup were retired 2026-09-08. Their denominators went
  // with them: qualGiven (qualified AND demo actually given) and signups
  // (_reachedOf FORM_SUBMITTED). Dropping CPS also retires a metric that had gone
  // structurally wrong -- Form Submitted left the virtual path, so the clinician
  // campaign's own signups had stopped landing in its denominator.
  const _reachedOf = (stageId) => {
    const row = (funnelAll || []).find(x => x && x.id === stageId);
    return (row && row.reachedCount) || 0;
  };
  const fo = _reachedOf(CLS.FULLY_ONBOARDED);
  const div = (n, d) => (d > 0 ? n / d : null);
  // Cumulative spend as a % of budget, one point per day — the sparkline series.
  let paceSeries = null;
  if (budget > 0 && from && to) {
    paceSeries = [];
    let run = 0, cur = from, guard = 0;
    while (cur <= to && guard++ < 400) {
      run += byDay[cur] || 0;
      paceSeries.push(Math.round((run / budget) * 1000) / 10);
      const d = new Date(cur + 'T00:00:00Z'); d.setUTCDate(d.getUTCDate() + 1);
      cur = `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, '0')}-${String(d.getUTCDate()).padStart(2, '0')}`;
    }
  }
  return {
    spend, budget, paceSeries,
    pacePct: budget > 0 ? (spend / budget) * 100 : null,
    // Cost Per Inquiry: clinician ad spend ÷ clinician DEALS created. Renamed from
    // CPD because that is what the denominator has always been -- a created deal is
    // an inquiry, not a demo -- so the old name described a number it never computed.
    // Cost Per Inquiry: clinician ad spend ÷ inquiries ATTRIBUTED to the clinician
    // campaigns. cpiDenomAll carries the whole-window clinician count so the card
    // can show the attributed share rather than implying every clinician was paid.
    cpi: div(spend, inquiriesAttr), cpiDenom: inquiriesAttr, cpiDenomAll: cliniciansAdded,
    // Cost per Full Submission: clinician ad spend ÷ conversions those campaigns
    // reported. Fully Onboarded is kept alongside as the old basis, since it is a
    // pipeline milestone rather than an ad-reported conversion and the two will
    // not agree.
    cac: div(spend, conversions), cacDenom: conversions, cacDenomOnboarded: fo,
  };
}

// Trend Analysis series. One pass over the window's Meta rows and its contacts,
// emitted at BOTH grains so the card's Weekly/Daily toggle is a client-side
// switch rather than a refetch.
//
// Every denominator here is the one the Key Metrics row uses, deliberately:
//   inquiries = unique clinician CONTACTS created that day (deduped per person)
//   fullApp   = those of them with a kickoff call booked
// Both are attributed to the CONTACT's create date, not to the booking date, so
// summing either daily series across the window reproduces the Key Metrics total
// exactly. Bucketing fullApp on clinician_meeting_booked_date reads more
// naturally and does NOT tie out — a contact created inside the window can book
// outside it, and vice versa.
//
// CTR and both CVRs ride the same Meta rows that already fund `spend`, so the
// whole section still costs no extra subrequest.
// How far back the Trend Analysis section reaches, INDEPENDENT of the page's
// date filter. The widest view it offers is 3 months, so the series is built
// once over that span and the client slices the last 8 weeks / 30 days / 3
// months out of it without another request.
const CLN_TREND_DAYS = 100;
function clnTrendWindow(today) {
  const d = new Date(today + 'T00:00:00Z');
  d.setUTCDate(d.getUTCDate() - (CLN_TREND_DAYS - 1));
  return { from: fmt(d), to: today };
}
// ── Trend Analysis (Brand Dashboard) ───────────────────────────────────────
// The Clinician section's shape over the Brand page's own populations:
// Regular Demos (every demo + webinar from a brand that is NOT pre-launch) and
// Paid Demos (the subset whose booking contact carries a paid last-click UTM).
//
// It shares clnTrendWindow, so BOTH sections have one lookback constant and
// cannot drift to different spans.
//
// Why its own inputs rather than the page's: processScheduledContacts and
// processAdSpend count every row handed to them, so widening the page fetches
// to cover 100 days would silently inflate every headline figure on the page.
// The caller therefore fetches a trend-window set separately and KV-caches the
// built series — the window ends at the same day on every request, so one build
// per TTL serves every user and every page window.
//
// Day keys: the two CRM sides normalise to a PACIFIC calendar day; the ad side
// uses the platform's own `row.date` verbatim, because that is the only date
// Windsor reports and re-zoning it would invent precision. The two conventions
// can disagree within a few hours of a day boundary. Same asymmetry
// buildClinicianTrend carries — named here rather than left to be rediscovered.
function buildBrandTrend(schContacts, windsorRows, from, to) {
  const B = {};
  const bump = (dayKey, field, n) => {
    if (!dayKey) return null;
    if (from && dayKey < from) return null;
    if (to && dayKey > to) return null;
    if (!B[dayKey]) B[dayKey] = { spend: 0, impressions: 0, clicks: 0, regular: 0, paid: 0 };
    B[dayKey][field] += (n == null ? 1 : n);
    return B[dayKey];
  };

  // Spend / clicks / impressions across every channel the page's KPIs count.
  // windsorRowChannel + KPI_EXCLUDED_CHANNELS is exactly what processAdSpend
  // applies to produce the Ad Spend tile, so this series sums to that tile over
  // the same span instead of merely resembling it.
  for (const r of (windsorRows || [])) {
    const chKey = windsorRowChannel(r);
    if (!chKey || KPI_EXCLUDED_CHANNELS.has(chKey)) continue;
    const k = String(r.date || '').slice(0, 10);
    if (!k) continue;
    bump(k, 'spend', parseFloat(r.spend) || 0);
    bump(k, 'impressions', parseInt(r.impressions) || 0);
    bump(k, 'clicks', parseInt(r.clicks) || 0);
  }

  // Regular / Paid demos. Mirrors processScheduledContacts field for field —
  // same dedupe on contact id, same createdate anchor (NOT date_demo_booked),
  // same pre-launch substring gate, and the paid test on RAW utm properties
  // rather than the 'Not set'-normalised pair, which would make every untagged
  // contact look tagged. Any drift here shows up as the trend disagreeing with
  // the Key Metrics tiles it sits under.
  const seenC = new Set();
  for (const c of (schContacts || [])) {
    const cp = (c && c.properties) || {};
    if (!cp.createdate) continue;
    if (c.id) { if (seenC.has(c.id)) continue; seenC.add(c.id); }
    const wt = (cp.average_monthly_web_traffic || '').toLowerCase();
    if (wt.includes('pre-launch')) continue;           // pre-launch is not "regular"
    const k = ptDayKeyOf(cp.createdate);
    if (!bump(k, 'regular')) continue;
    const rs = (cp.utm_source || '').trim(), rm = (cp.utm_medium || '').trim();
    if (mapUtmToChannel(rs, rm)) bump(k, 'paid');
  }

  const div = (n, dd) => (dd > 0 ? n / dd : null);
  // A COST with no spend is undefined, not zero — a real 0 plots on the floor of
  // the cost axis and reads as the most efficient period in the window.
  const cost = (sp, dd) => (sp > 0 && dd > 0 ? sp / dd : null);
  // Every calendar day, so a dead period still emits a point: the chart places
  // points by ARRAY INDEX, so a dropped period is spliced over rather than drawn
  // as a gap.
  const allDays = [];
  if (from && to) {
    const d1 = new Date(from + 'T00:00:00Z'), d2 = new Date(to + 'T00:00:00Z');
    if (!isNaN(d1) && !isNaN(d2)) {
      for (const t = new Date(d1); t <= d2 && allDays.length < 2000; t.setUTCDate(t.getUTCDate() + 1)) allDays.push(fmt(t));
    }
  }
  const monOf = (ds) => {
    if (!ds) return null;
    const q = ds.split('-');
    const d = new Date(Date.UTC(+q[0], +q[1] - 1, +q[2]));
    if (isNaN(d)) return null;
    const dow = d.getUTCDay();
    d.setUTCDate(d.getUTCDate() - (dow === 0 ? 6 : dow - 1));   // ISO: week starts Monday
    return fmt(d);
  };
  const roll = (keyOf) => {
    const g = {};
    const touch = (k) => { if (k && !g[k]) g[k] = { spend: 0, impressions: 0, clicks: 0, regular: 0, paid: 0 }; };
    for (const day of allDays) touch(keyOf(day));
    for (const day of Object.keys(B)) {
      const k = keyOf(day); if (!k) continue;
      touch(k);
      const b = B[day], o = g[k];
      o.spend += b.spend; o.impressions += b.impressions; o.clicks += b.clicks;
      o.regular += b.regular; o.paid += b.paid;
    }
    return Object.keys(g).sort().map(k => {
      const w = g[k];
      return {
        wk: k,
        spend: Math.round(w.spend * 100) / 100,
        impressions: w.impressions, clicks: w.clicks,
        regular: w.regular, paid: w.paid,
        // Both populations precomputed so the client can show either without a
        // second request. Rates are percentages computed on the period's OWN
        // totals, never averaged from daily rates — a 10-impression day must not
        // weigh the same as a 10,000-impression one.
        cpdReg: cost(w.spend, w.regular), cpdPaid: cost(w.spend, w.paid),
        cvrReg: div(w.regular * 100, w.clicks), cvrPaid: div(w.paid * 100, w.clicks),
        ctr: div(w.clicks * 100, w.impressions),
      };
    });
  };
  const monthOf = (ds) => (ds ? ds.slice(0, 7) + '-01' : null);
  return { week: roll(monOf), day: roll(x => x), month: roll(monthOf) };
}

function buildClinicianTrend(deals, contactsByDeal, windsorRows, from, to) {
  const monOf = (ds) => {
    if (!ds) return null;
    const p2 = ds.split('-');
    const d = new Date(Date.UTC(+p2[0], +p2[1] - 1, +p2[2]));
    if (isNaN(d)) return null;
    const dow = d.getUTCDay();
    d.setUTCDate(d.getUTCDate() - (dow === 0 ? 6 : dow - 1));   // ISO: week starts Monday
    return fmt(d);
  };
  // Bucket by DAY first, then roll up. fetchWindsorAds is handed a widened
  // `from` (wFrom) for the trailing-window metrics, so days outside the selected
  // window arrive here and are dropped at the source rather than stretching the
  // axis backwards by one point.
  const B = {};
  // Returns the bucket so the caller can tally the per-type maps onto the same
  // day without repeating the window test.
  const bump = (dayKey, field, n) => {
    if (!dayKey) return null;
    if (from && dayKey < from) return null;
    if (to && dayKey > to) return null;
    if (!B[dayKey]) B[dayKey] = { spend: 0, impressions: 0, clicks: 0, inquiries: 0, fullApp: 0, byType: {}, bySubType: {} };
    B[dayKey][field] += (n == null ? 1 : n);
    return B[dayKey];
  };

  // Spend / impressions / clicks — the same clinician-campaign filter
  // buildClinicianAds and fetchClinicianTopFunnel use, so CTR here and CTR in
  // Key Metrics are the same measurement at two grains.
  for (const r of (windsorRows || [])) {
    const ds = (r.datasource || '').toLowerCase();
    if (!/facebook|meta|fb|ig|instagram/.test(ds)) continue;
    if (!isClinicianCampaign(r.campaign_name)) continue;
    const k = String(r.date || '').slice(0, 10);
    if (!k) continue;
    bump(k, 'spend', parseFloat(r.spend) || 0);
    bump(k, 'impressions', parseInt(r.impressions) || 0);
    bump(k, 'clicks', parseInt(r.clicks) || 0);
  }

  // Contacts, deduped per person exactly as buildClinicianComposition does it —
  // same key, same date field, same fallback — so the two cannot drift.
  const seen = new Set();
  for (const d of (deals || [])) {
    const cp = contactsByDeal[d.id];
    if (!cp) continue;
    const cid = cp.__cid || ('deal:' + d.id);
    if (seen.has(cid)) continue;
    seen.add(cid);
    const k = _clnDayKey(cp.createdate);
    if (!k) continue;
    const bk = bump(k, 'inquiries');
    if (!bk) continue;
    // Clinician type, on the same 'Not set' fallback buildClinicianComposition
    // uses — the On-Target card's mix has to reconcile with the Acquisition pie
    // beside it, and a different blank label would split one category in two.
    const ty = (cp.clinician_type || '').trim() || 'Not set';
    bk.byType[ty] = (bk.byType[ty] || 0) + 1;
    if (((d.properties || {}).clinician_meeting_booked_date || '').trim()) {
      bump(k, 'fullApp');
      bk.bySubType[ty] = (bk.bySubType[ty] || 0) + 1;
    }
  }

  const div = (n, dd) => (dd > 0 ? n / dd : null);
  // A COST per outcome is undefined when nothing was spent — not zero. `div`
  // guards only the denominator, so a period with organic inquiries and no
  // clinician ad spend used to yield a real 0, which the chart cannot tell from
  // a measured value: it plots on the floor of the cost axis and reads as the
  // most efficient period in the window. Null is the honest answer, and it is
  // what makes the renderer break the line there.
  const cost = (sp, dd) => (sp > 0 && dd > 0 ? sp / dd : null);
  // Every day in the window, so a period with no spend AND no contacts still
  // produces a point. The chart positions points by ARRAY INDEX, so a dropped
  // period is not drawn as a gap — its neighbours become adjacent and the line
  // splices straight across it, which is exactly what the null-break logic
  // exists to prevent. Capped so an unbounded window cannot build a runaway
  // array; past the cap the series degrades to present-periods-only.
  const allDays = [];
  if (from && to) {
    const d1 = new Date(from + 'T00:00:00Z'), d2 = new Date(to + 'T00:00:00Z');
    if (!isNaN(d1) && !isNaN(d2)) {
      for (const t = new Date(d1); t <= d2 && allDays.length < 2000; t.setUTCDate(t.getUTCDate() + 1)) {
        allDays.push(fmt(t));
      }
    }
  }
  const roll = (keyOf) => {
    const g = {};
    const touch = (k) => {
      if (k && !g[k]) g[k] = { spend: 0, impressions: 0, clicks: 0, inquiries: 0, fullApp: 0, byType: {}, bySubType: {} };
    };
    const addMap = (dst, srcMap) => {
      for (const t of Object.keys(srcMap || {})) dst[t] = (dst[t] || 0) + srcMap[t];
    };
    for (const day of allDays) touch(keyOf(day));
    for (const day of Object.keys(B)) {
      const k = keyOf(day);
      if (!k) continue;
      touch(k);
      const b = B[day], o = g[k];
      o.spend += b.spend; o.impressions += b.impressions; o.clicks += b.clicks;
      o.inquiries += b.inquiries; o.fullApp += b.fullApp;
      addMap(o.byType, b.byType); addMap(o.bySubType, b.bySubType);
    }
    return Object.keys(g).sort().map(k => {
      const w = g[k];
      return {
        wk: k,
        spend: Math.round(w.spend * 100) / 100,
        impressions: w.impressions, clicks: w.clicks,
        inquiries: w.inquiries, fullApp: w.fullApp,
        // Clinician-type mix for BOTH populations, so the On-Target card can
        // follow the Acquisition section's Inquiry / Full Submission toggle
        // without a second request.
        byType: w.byType, bySubType: w.bySubType,
        cpi: cost(w.spend, w.inquiries),
        cpfa: cost(w.spend, w.fullApp),
        // Rates as PERCENTAGES, computed on the period's own totals rather than
        // averaged from the daily rates — a 10-impression day must not weigh the
        // same as a 10,000-impression one.
        cvrInq: div(w.inquiries * 100, w.clicks),
        cvrFull: div(w.fullApp * 100, w.clicks),
        ctr: div(w.clicks * 100, w.impressions),
      };
    });
  };
  const monthOf = (ds) => (ds ? ds.slice(0, 7) + '-01' : null);
  return { week: roll(monOf), day: roll(x => x), month: roll(monthOf) };
}

// Did the clinician submit the credentials form BEFORE their demo?
//
// Anchored on `clinician_meeting_date`, NOT the Meeting Happened stage stamp —
// that stamp is blank on ~19 of every 20 recent deals because reps move straight
// to Form Submitted and skip the stage, so it is unusable as an anchor.
//
// Both sides are reduced to a PACIFIC calendar day before comparing. That
// matters: the Form Submitted stamp is a UTC timestamp, so a submission at
// 2026-09-01T00:21Z is really Aug 31 5:21pm Pacific — the naive UTC comparison
// against a Sep 1 meeting says "Same Day" where Pacific correctly says "Before".
// Pacific is what every other date boundary in this file uses.
//
// Known limitation, accepted deliberately: `clinician_meeting_date` is mutable
// and holds the CURRENT scheduled date. A rescheduled demo therefore reads
// "Before" even though the clinician did not submit early. Per Alex there is no
// separate Rescheduled pill, so those land in Before.
//
// `meetingStarts` (optional) is the list of hs_meeting_start_time values on the
// deal's associated meetings. When one of them falls on the same calendar day as
// the demo, the comparison switches from day-level to TIMESTAMP-level and Same
// Day resolves to a real answer. A form submitted DURING the call counts as
// After — the question is whether it arrived before the demo, and 20:52 into a
// 20:30 meeting did not. Same Day therefore survives only as the fallback for
// deals with no meeting record at all.
function clinicianFormVsDemo(p, meetingStarts) {
  const fRaw = clnEnteredAt(p, CLS.FORM_SUBMITTED);
  const f = _clnDayKey(fRaw);
  const m = _clnDayKey(p.clinician_meeting_date);
  if (!f || !m) return '';           // never reached Form Submitted, or no demo date
  if (f < m) return 'Before';
  if (f > m) return 'After';
  const toMs = (v) => { if (!v) return NaN; return /^\d+$/.test(String(v)) ? parseInt(v) : new Date(v).getTime(); };
  const onDay = (meetingStarts || []).filter(x => _clnDayKey(x) === m).map(toMs).filter(x => !isNaN(x));
  if (onDay.length) {
    const fMs = toMs(fRaw);
    // Earliest meeting that day is the demo it was measured against.
    if (!isNaN(fMs)) return fMs < Math.min.apply(null, onDay) ? 'Before' : 'After';
  }
  return 'Same Day';
}

// hs_meeting_start_time for the deals whose Form-vs-Demo is ambiguous at day
// level. Deliberately fetched for ONLY those deals: it is 2 subrequests per 100
// ambiguous deals against a 50-per-invocation ceiling, and the unambiguous ones
// gain nothing from a timestamp.
async function fetchClinicianMeetingStarts(token, dealIds) {
  const out = {};
  if (!dealIds || !dealIds.length) return out;
  const chunk = (arr, n) => { const o = []; for (let i = 0; i < arr.length; i += n) o.push(arr.slice(i, i + n)); return o; };
  const dealToMeetings = {};
  const meetingIds = new Set();
  for (const batch of chunk(dealIds, 100)) {
    try {
      const r = await fetch('https://api.hubapi.com/crm/v4/associations/deals/meetings/batch/read', {
        method: 'POST',
        headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ inputs: batch.map(id => ({ id: String(id) })) }),
      });
      if (!r.ok) { azDrain(r); console.error('clinician meeting assoc', r.status); continue; }
      const j = await r.json();
      for (const row of (j.results || [])) {
        const did = row.from && row.from.id; if (!did) continue;
        const ids = (row.to || []).map(x => x && x.toObjectId).filter(Boolean).map(String);
        if (ids.length) { dealToMeetings[did] = ids; ids.forEach(id => meetingIds.add(id)); }
      }
    } catch (e) { console.error('clinician meeting assoc threw', e.message); }
  }
  const startById = {};
  for (const batch of chunk(Array.from(meetingIds), 100)) {
    try {
      const r = await fetch('https://api.hubapi.com/crm/v3/objects/meetings/batch/read', {
        method: 'POST',
        headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ properties: ['hs_meeting_start_time'], inputs: batch.map(id => ({ id })) }),
      });
      if (!r.ok) { azDrain(r); console.error('clinician meetings', r.status); continue; }
      const j = await r.json();
      for (const mtg of (j.results || [])) {
        const st = (mtg.properties || {}).hs_meeting_start_time;
        if (st) startById[String(mtg.id)] = st;
      }
    } catch (e) { console.error('clinician meetings threw', e.message); }
  }
  for (const did in dealToMeetings) {
    const starts = dealToMeetings[did].map(id => startById[id]).filter(Boolean);
    if (starts.length) out[did] = starts;
  }
  console.log(`clinician meeting starts: resolved ${Object.keys(out).length} of ${dealIds.length} ambiguous deals`);
  return out;
}

// Pacific calendar day for a HubSpot timestamp (same rule as Daily Activity).
function _clnDayKey(v) {
  if (!v) return null;
  const str = String(v);
  if (/^\d{4}-\d{2}-\d{2}$/.test(str)) return str;
  const t = /^\d+$/.test(str) ? parseInt(str) : new Date(str).getTime();
  if (isNaN(t)) return null;
  const d = new Date(t + ptOff(new Date(t)) * 3600000);
  return `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, '0')}-${String(d.getUTCDate()).padStart(2, '0')}`;
}
// owner id → name for the Deal Rep column. Paged; failures degrade to ids.
async function fetchClinicianOwners(token) {
  const map = {};
  try {
    let after = '';
    for (let i = 0; i < 5; i++) {
      const url = 'https://api.hubapi.com/crm/v3/owners?limit=100' + (after ? '&after=' + after : '');
      const r = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
      if (!r.ok) { azDrain(r); break; }
      const j = await r.json();
      for (const o of (j.results || [])) {
        const nm = [o.firstName, o.lastName].filter(Boolean).join(' ').trim() || o.email || String(o.id);
        map[String(o.id)] = nm;
      }
      after = j.paging && j.paging.next && j.paging.next.after;
      if (!after) break;
    }
  } catch (e) { console.error('clinician owners:', e.message); }
  return map;
}

// Flat row per deal for the All Clinicians table. Uses the RAW cohort (no
// future-meeting exclusion) — a data table should list everything in the window.
function buildClinicianTable(deals, contactsByDeal, ownerMap, meetingStarts) {
  const stageLabel = {};
  for (const st of CLINICIAN_STAGES) stageLabel[st.id] = st.label;
  stageLabel[CLINICIAN_STAGE_NO_SHOW] = 'No Show';
  stageLabel[CLINICIAN_STAGE_NOT_A_FIT] = 'Not a Fit';
  return deals.map(d => {
    const p = d.properties || {}, c = contactsByDeal[d.id] || {};
    const stage = (p.dealstage || '').trim();
    return {
      dealId: String(d.id),
      contactId: c.__cid || null,
      dealName: p.dealname || '(no name)',
      contactName: [c.firstname, c.lastname].filter(Boolean).join(' ').trim() || c.email || '',
      stage, stageLabel: stageLabel[stage] || stage || '\u2014',
      // Workflow Type column. `workflow` is the raw HubSpot value (blank on
      // older deals); workflowShort is the pill label the table renders.
      workflow: (p.clinician_workflow || '').trim(),
      workflowShort: CLINICIAN_FLOW_SHORT[clinicianFlowOf(p)] || '',
      // Call Scheduled. One definition, two tables. 'na' for the Field Rep
      // workflow: those clinicians are signed up in person, so there is no call
      // to book and an absent date is not a gap. Semantic token, not a glyph —
      // the tables decide how to draw it.
      callScheduled: (CLINICIAN_FLOW_SHORT[clinicianFlowOf(p)] === 'Field')
        ? 'na'
        : ((p.clinician_meeting_booked_date || '').trim() ? 'yes' : 'no'),
      rep: ownerMap[String(p.hubspot_owner_id || '')] || '',
      formVsDemo: clinicianFormVsDemo(p, (meetingStarts || {})[d.id]),
      createdate: p.createdate || '',
      bookedDate: p.clinician_meeting_booked_date || '',
      meetingDate: p.clinician_meeting_date || '',
      attendance: (p.clinician_attendance_status || '').trim(),
      qualification: (p.clinician_qualification_outcome || '').trim(),
      specialty: c.clinician_specialty || '', clinicianType: c.clinician_type || '',
      npi: c.npi_number || '', message: c.message || '',
      referral: c.clinician_referral_source || '',
      photoStatus: c.photo_verification_status || '', credStatus: c.medical_degree_status || '',
      address: c.address || '', city: c.city || '',
      state: c.contact_state || '', zip: c.zip_code || '',
      proofLink: c.proof_link || '',
      samplePrefs: c.sample_preferences_notes || '',
      samplesArrival: c.samples_arrival_date || '', samplesTracking: c.samples_tracking_number || '',
      samplesCount: c.number_of_samples_shipped || '', whichSamples: c.which_samples_shipped || '',
      // Blank stays blank: "never recorded" and "recorded as 0" are different
      // states, and the renderer turns '' into an em-dash like every other
      // contact-sourced column.
      reviewsSubmitted: (c.reviews_submitted == null || c.reviews_submitted === '') ? '' : c.reviews_submitted,
      utmSource: c.utm_source || p.utm_source || '', utmMedium: c.utm_medium || p.utm_medium || '',
      utmCampaign: c.utm_campaign || p.utm_campaign || '',
    };
  });
}

function buildClinicianComposition(deals, contactsByDeal, from, to) {
  const seen = new Set();
  const byType = {}, bySpec = {};
  const byRef = {}, byUtmS = {}, byUtmM = {}, byUtmC = {};
  // Same four tallies over the SUBSET that has a kickoff call booked. The
  // Acquisition card toggles between them: "Inquiry" is everyone who arrived,
  // "Full Submission" only those who got as far as scheduling a call. Kept as a
  // parallel tally rather than a second pass so both come out of one loop.
  const subRef = {}, subUtmS = {}, subUtmM = {}, subUtmC = {}, subType = {};
  let subContacts = 0;
  const byCred = {}, byPhoto = {};
  let verifContacts = 0;   // contacts CREATED in the window (verification scope)
  let contacts = 0;
  for (const deal of deals) {
    const cp = contactsByDeal[deal.id];
    if (!cp) continue;
    const cid = cp.__cid || ('deal:' + deal.id);
    if (seen.has(cid)) continue;
    seen.add(cid);
    // ONE scope for the whole section: the contact's own create date must fall in
    // the window. Previously only Verification applied this, so Composition and
    // Acquisition were built from a wider population and their totals disagreed.
    const ck = _clnDayKey(cp.createdate);
    if (from && to && (!ck || ck < from || ck > to)) continue;
    contacts++;
    const t = (cp.clinician_type || '').trim() || 'Not set';
    const sp = (cp.clinician_specialty || '').trim() || 'Not set';
    byType[t] = (byType[t] || 0) + 1;
    bySpec[sp] = (bySpec[sp] || 0) + 1;
    // Acquisition. UTMs may sit on either object, so take the contact's value and
    // fall back to the deal's rather than assuming one of them.
    const dp = deal.properties || {};
    const pick = (k) => ((cp[k] || dp[k] || '').trim()) || 'Not set';
    const rs = (cp.clinician_referral_source || '').trim() || 'Not set';
    byRef[rs] = (byRef[rs] || 0) + 1;
    const us = pick('utm_source'), um = pick('utm_medium'), uc = pick('utm_campaign');
    byUtmS[us] = (byUtmS[us] || 0) + 1;
    byUtmM[um] = (byUtmM[um] || 0) + 1;
    byUtmC[uc] = (byUtmC[uc] || 0) + 1;
    // "Full Submission" = a kickoff call is on the books. clinician_meeting_booked_date
    // is the booking field (NOT clinician_meeting_date, which is the meeting day);
    // see the note on CLINICIAN_DEAL_PROPS for why those two are easy to confuse.
    if ((dp.clinician_meeting_booked_date || '').trim()) {
      subContacts++;
      subRef[rs] = (subRef[rs] || 0) + 1;
      subType[t] = (subType[t] || 0) + 1;
      subUtmS[us] = (subUtmS[us] || 0) + 1;
      subUtmM[um] = (subUtmM[um] || 0) + 1;
      subUtmC[uc] = (subUtmC[uc] || 0) + 1;
    }
    // Verification: medical_degree_status is labelled "Credentials Status" in HubSpot.
    // Verification only applies once the meeting has actually happened — before
    // that there is nothing to verify, so Meeting Scheduled deals would pad the
    // mix with a meaningless "Pending"/"Not set". Both exit stages (No Show,
    // Not a Fit) are excluded too, so this counts only clinicians still live in
    // the pipeline at Meeting Happened or beyond.
    const vStage = (dp.dealstage || '').trim();
    // By ID, not by array index: `vIdx >= 1` silently repointed at a different
    // stage whenever one was inserted earlier in CLINICIAN_STAGES.
    const _vFrom = CLINICIAN_STAGES.findIndex(x => x.id === CLS.MEETING_HAPPENED);
    const vIdx = CLINICIAN_STAGES.findIndex(x => x.id === vStage);
    if (vIdx >= 0 && _vFrom >= 0 && vIdx >= _vFrom) {
      verifContacts++;
      const cr = (cp.medical_degree_status || '').trim() || 'Not set';
      const ph = (cp.photo_verification_status || '').trim() || 'Not set';
      byCred[cr] = (byCred[cr] || 0) + 1;
      byPhoto[ph] = (byPhoto[ph] || 0) + 1;
    }
  }
  // Largest first, but "Not set" is pinned last so it never leads the legend.
  const toList = (o) => Object.entries(o)
    .map(([label, count]) => ({ label, count }))
    .sort((a, b) => (a.label === 'Not set') - (b.label === 'Not set') || b.count - a.count);
  // Cap slice count so a high-cardinality dimension (utm_campaign especially)
  // stays readable; the remainder is rolled into one labelled bucket rather than
  // dropped, so the total still reconciles.
  const STATUS_ORDER = ['Approved', 'Pending', 'Not Approved'];
  const statusList = (o) => {
    const keys = Object.keys(o);
    const rank = (k) => { const i = STATUS_ORDER.indexOf(k); return i >= 0 ? i : (k === 'Not set' ? 99 : 50); };
    return keys.sort((a, b) => rank(a) - rank(b) || o[b] - o[a]).map(label => ({ label, count: o[label] }));
  };
  // The rolled-up remainder CARRIES its rows. Previously the tail was summed and
  // thrown away, so "Other (8 more)" was a dead end — the eight were unknowable
  // from the payload. Keeping them on the bucket lets the client expand it
  // without a second request, and costs only the rows that already existed.
  const capped = (o, max) => {
    const l = toList(o);
    if (l.length <= max) return l;
    const head = l.slice(0, max - 1);
    const tail = l.slice(max - 1);
    const rest = tail.reduce((n, x) => n + x.count, 0);
    head.push({ label: `Other (${tail.length} more)`, count: rest, children: tail });
    return head;
  };
  return {
    contacts, verifContacts,
    // Capped at 9 = CLINICIAN_PIE_COLORS.length, so the client's i%9 can never
    // wrap. clinician_specialty has 12 options in HubSpot; the overflow rolls
    // into the expandable Other bucket rather than reusing colour 0.
    type: capped(byType, 9), specialty: capped(bySpec, 9),
    referral: capped(byRef, 9),
    // Fixed review order rather than by size, so the two charts stay comparable
    // and a status doesn't jump position as counts change.
    credentials: statusList(byCred), photoVerification: statusList(byPhoto),
    utmSource: capped(byUtmS, 8), utmMedium: capped(byUtmM, 8), utmCampaign: capped(byUtmC, 8),
    // Acquisition card, "Full Submission" mode: the same breakdowns over
    // clinicians who have a kickoff call booked.
    submission: {
      contacts: subContacts,
      referral: capped(subRef, 9),
      type: capped(subType, 9),
      utmSource: capped(subUtmS, 8), utmMedium: capped(subUtmM, 8), utmCampaign: capped(subUtmC, 8),
    },
  };
}

function buildClinicianFunnel(deals, contactsByDeal, meetingStarts) {
  const _ms = (v) => { if (!v) return NaN; return /^\d+$/.test(String(v)) ? parseInt(v) : new Date(v).getTime(); };
  // A deal "reached" a stage if HubSpot stamped its entered-date; fall back to
  // the current stage's position so deals predating stage tracking still count.
  // Meetings still in the future haven't had their chance to happen yet, so
  // counting them drags Show Rate down and inflates the Meeting Scheduled bar.
  // Excluded here (deals with NO meeting date are kept — they're unscheduled,
  // not future-dated). Count is reported so the drop is visible, not silent.
  const _today = new Date();
  const _todayEnd = Date.UTC(_today.getUTCFullYear(), _today.getUTCMonth(), _today.getUTCDate(), 23, 59, 59, 999);
  let excludedFuture = 0;
  const rows = deals.filter(d => {
    const md = (d.properties || {}).clinician_meeting_date;
    if (!md) return true;
    const t = /^\d+$/.test(String(md)) ? parseInt(md) : new Date(md).getTime();
    if (!isNaN(t) && t > _todayEnd) { excludedFuture++; return false; }
    return true;
  }).map(d => {
    const p = d.properties || {};
    const cur = (p.dealstage || '').trim();
    // "Ever reached" per stage id.
    //
    // These stages are STRICTLY SEQUENTIAL — a clinician cannot submit a 2nd
    // review without having submitted a 1st, cannot submit a 1st without a
    // sample shipped, and cannot be shipped a sample without being fully
    // onboarded. Nobody skips. So "reached" is the monotonic closure of the
    // furthest point on the deal's own flow path we can demonstrate: take the
    // highest index evidenced by EITHER the current stage OR any stage HubSpot
    // stamped, then credit everything at or below it.
    //
    // The old version closed over the CURRENT stage only, which left two holes:
    //   • a deal now sitting in an exit stage (No Show / Not a Fit) has no
    //     position on the path at all, so it got no backfill and counted only
    //     for whatever stamps happened to exist
    //   • a deal stamped at 3rd+ Review but missing its 2nd Review stamp (stage
    //     jumps from imports and automation do this) counted for 3rd+ and not
    //     2nd — a funnel that widens as it descends
    //
    // The closure runs over the deal's OWN flow sequence, never the full stage
    // list: a Virtual Onboarding deal at Fully Onboarded must not be credited
    // with Meeting Scheduled / Meeting Happened / Signed Up / Clinician
    // Approved, which are not on its path. Off-path stages count only when
    // HubSpot actually stamped them, and never drag the closure with them.
    const flow = clinicianFlowOf(p);
    const seqIds = CLINICIAN_FLOWS[flow].stages;
    const posInSeq = seqIds.indexOf(cur);
    // Seed at 0: the deal was created into its own flow's first stage, so it has
    // entered at least that. (Meeting Scheduled, Clinician Inquiry and the two
    // exit stages carry no hs_v2_date_entered_* property at all, so there is no
    // stamp to find for the demo path's opener.)
    // Seed at 0 only when the current stage is one we can actually place: on the
    // deal's own path, one of its exits, or blank. An UNRECOGNISED stage id (one
    // added in HubSpot after this list was verified) is not evidence of anything,
    // and seeding it would silently assert a stage the deal may never have walked
    // — which matters most for the Field Rep flow, whose sequence is still ASSUMED.
    const _curKnown = !cur || posInSeq >= 0
      || (CLINICIAN_FLOWS[flow].exits || []).indexOf(cur) >= 0
      || CLINICIAN_ALL_STAGE_IDS.indexOf(cur) >= 0;
    let maxIdx = _curKnown ? Math.max(posInSeq, 0) : posInSeq;
    seqIds.forEach((id, i) => { if (i > maxIdx && clnEnteredAt(p, id)) maxIdx = i; });
    const reachedIds = new Set();
    seqIds.forEach((id, i) => { if (i <= maxIdx) reachedIds.add(id); });
    for (const st of CLINICIAN_STAGES) {
      if (!seqIds.includes(st.id) && clnEnteredAt(p, st.id)) reachedIds.add(st.id);
    }
    // A deal is standing in its current stage, so it has by definition reached
    // it. Without this the renderer's implicit invariant (reachedCount >= count)
    // breaks for an OFF-PATH current stage: a Virtual or Field deal sitting in
    // Meeting Scheduled is counted by curOf but never by cntOf — Meeting
    // Scheduled is off its flow AND has no stamp property to find — so the bar
    // rendered "0 reached · 3 here now" as an empty 2%-wide sliver. Exits are
    // excluded: they are counted by current stage in their own list, and are
    // not funnel stages.
    if (cur && CLINICIAN_STAGES.some(st => st.id === cur)) reachedIds.add(cur);
    const cp = contactsByDeal[d.id] || {};
    return {
      cur, flow, reachedIds,
      // Contact identity + review count travel with the row so seg() can total
      // reviews for whatever flow/type slice the card is showing. HubSpot returns
      // number properties as STRINGS ('3', not 3), so this must be parsed — a
      // bare += would concatenate them.
      cid: cp.__cid || ('deal:' + d.id),
      hasContact: !!cp.__cid,
      reviews: (function(){ const n = parseFloat(cp.reviews_submitted); return isNaN(n) ? 0 : n; })(),
      fvd: clinicianFormVsDemo(p, (meetingStarts || {})[d.id]),
      // Whether a call is actually on the books. Same property the All Clinicians
      // "Call Scheduled" column reads, so the funnel stat and that column can
      // never disagree about who has booked.
      hasBookedCall: !!(p.clinician_meeting_booked_date || '').trim(),
      att: (p.clinician_attendance_status || '').trim(),
      qual: (p.clinician_qualification_outcome || '').trim(),
      type: (cp.clinician_type || '').trim(),
      // Velocity endpoints. hs_v2_date_entered_* is HubSpot's stage-entry stamp;
      // fall back to the meeting date / createdate for deals that predate stage
      // tracking, otherwise velocity silently reports nothing.
      tMS: (function(){ const a=_ms(clnPathStartAt(p, seqIds)); if(!isNaN(a))return a;
                        const b=_ms(p.clinician_meeting_date); if(!isNaN(b))return b;
                        return _ms(p.createdate); })(),
      tFO: _ms(clnEnteredAt(p, CLINICIAN_STAGE_FULLY_ONBOARDED)),
      // True when the start time is TRUSTWORTHY: a real stamp, or a first stage
      // that has no stamp property at all (where createdate IS the canonical
      // evidence). Using clnPathStartAt here made this always true — createdate
      // is set on every deal — which silently collapsed velocityStamped into
      // hasEnteredFO and stopped it flagging deals with a missing start.
      hasEnteredMS: !!clnEnteredAt(p, seqIds[0]) || !clnEnteredProp(seqIds[0]),
      hasEnteredFO: !!clnEnteredAt(p, CLINICIAN_STAGE_FULLY_ONBOARDED),
    };
  });

  // flowKey: 'both' (every deal, full stage list) | 'demo' | 'virtual'.
  // For a single flow the row set is narrowed to that flow AND the stage list
  // becomes that flow's own sequence, so its conversion rates are computed over
  // a path deals actually walk. Every count is looked up by stage id — never by
  // array position — so inserting a stage can't silently repoint a rate.
  const seg = (pred, flowKey) => {
    flowKey = flowKey || 'both';
    const stageList = flowKey === 'both'
      ? CLINICIAN_STAGES.slice()
      : CLINICIAN_FLOWS[flowKey].stages.map(id => CLINICIAN_STAGES.find(x => x.id === id)).filter(Boolean);
    const rs = rows.filter(r => pred(r) && (flowKey === 'both' || r.flow === flowKey));
    // Total Reviews Submitted for this slice. Deduped by contact id — one
    // clinician holding two deals is one person, and fetchClinicianContacts maps
    // both deals to the SAME contact record, so an undeduped sum would double
    // their reviews. Same idiom as buildClinicianDaily / buildClinicianComposition.
    // NOTE: reviews_submitted is a LIFETIME running total on the contact with no
    // timestamp, so this is a lifetime figure for the cohort — it is NOT scoped
    // to the selected window and cannot be, from this property.
    let reviewsTotal = 0, reviewsContacts = 0, reviewsNoContact = 0;
    {
      const seenCid = new Set();
      for (const r of rs) {
        if (seenCid.has(r.cid)) continue;
        seenCid.add(r.cid);
        if (!r.hasContact) { reviewsNoContact++; continue; }
        if (r.reviews > 0) { reviewsTotal += r.reviews; reviewsContacts++; }
      }
    }
    const has = (id) => stageList.some(st => st.id === id);
    // Funnel bars show deals CURRENTLY in each stage (per Alex).
    const curOf = (id) => rs.filter(r => r.cur === id).length;
    // Rates stay on "ever reached" — a current-stage denominator would exclude
    // everyone who already progressed, making show/close rates read far too low.
    const cntOf = (id) => rs.filter(r => r.reachedIds.has(id)).length;
    // No Show excludes anyone whose attendance was Rescheduled (they were
    // re-booked, not a no-show).
    const isCancelled = (r) => /cancelled before demo/i.test(r.att);
    // Every meeting-based rate must draw its numerator and denominator from the
    // SAME population. In the combined ('both') view `rs` spans all three flows,
    // but ONLY the demo flow walks meeting stages — so counting a numerator
    // across every flow over a demo-only denominator produced readings like
    // "Close Rate 467.6% · 159 / 34". Restrict these to the rows that actually
    // walk meetings; the funnel bars and the exits list keep the full `rs`.
    const rsMeet = rs.filter(r => {
      const f = CLINICIAN_FLOWS[r.flow];
      return !!f && f.stages.indexOf(CLS.MEETING_HAPPENED) >= 0;
    });
    const cntIn = (rows, id) => rows.filter(r => r.reachedIds.has(id)).length;
    const noShow = rsMeet.filter(r => r.cur === CLINICIAN_STAGE_NO_SHOW && !/rescheduled/i.test(r.att)).length;
    const notAFit = rsMeet.filter(r => r.cur === CLINICIAN_STAGE_NOT_A_FIT).length;
    // Disqualifications exclude anyone cancelled before the demo — they were
    // pruned, never assessed, so counting them as disqualified overstates it.
    const notAFitQual = rsMeet.filter(r => r.cur === CLINICIAN_STAGE_NOT_A_FIT && !isCancelled(r)).length;
    const cancelled = rsMeet.filter(isCancelled).length;
    const pct = (n, d) => (d > 0 ? (n / d) * 100 : null);
    const mh = cntIn(rsMeet, CLS.MEETING_HAPPENED);
    const msReached = cntIn(rsMeet, CLS.MEETING_SCHEDULED);
    const fsMeet = cntIn(rsMeet, CLS.FORM_SUBMITTED);
    // Form-before-demo. Denominator is the rows where the question is ANSWERABLE
    // (a form stamp and a demo date both exist), not every row — otherwise the
    // rate is diluted by deals that never submitted or have no demo booked, and
    // it would read as a share of the whole cohort rather than of the comparable
    // ones. Same rsMeet population as Show Rate, which it sits under.
    const fvdAnswerable = rsMeet.filter(r => r.fvd === 'Before' || r.fvd === 'Same Day' || r.fvd === 'After').length;
    const fvdBefore = rsMeet.filter(r => r.fvd === 'Before').length;
    // Full mix for the Verification Funnel's pie. Same rsMeet rows as the stat
    // above — DEAL-based and demo-flow — so the two figures now reconcile instead
    // of quoting different denominators for the same metric. Fixed pipeline
    // order, and empty slices dropped.
    const _fvdTally = {};
    for (const r of rsMeet) { const k = r.fvd || 'Not set'; _fvdTally[k] = (_fvdTally[k] || 0) + 1; }
    // 'Not set' is deliberately NOT a slice. Those deals have no form stamp or no
    // demo date, so they carry no before/after answer — showing them as a wedge
    // made a third of the chart mean "we can't tell". Dropping it makes the pie
    // exactly the comparable population, so its total now equals the Form
    // Submitted Before Demo denominator and its Before slice equals that
    // numerator. The excluded count is still tallied above if it is ever needed.
    const fvdDist = ['Before', 'Same Day', 'After']
      .map(label => ({ label, count: _fvdTally[label] || 0 }))
      .filter(x => x.count > 0);
    // Two different "supposed to occur" sets, per spec:
    //   no-show  → reached Meeting Happened + No Show
    //   prune    → reached Meeting Happened + No Show + Not a Fit
    const supposedNS = mh + noShow;
    const supposedPrune = mh + noShow + notAFit;
    const vel = rs.filter(r => !isNaN(r.tMS) && !isNaN(r.tFO) && r.tFO >= r.tMS).map(r => (r.tFO - r.tMS) / 86400000);
    // Meeting-based rates are meaningless for a flow with no meeting stages, so
    // they report null rather than a number computed on a zero denominator.
    const hasMeeting = has(CLS.MEETING_HAPPENED);
    // "Stuck in Clinician Approved": of everyone who EVER reached Clinician
    // Approved, how many are STILL sitting there instead of moving on. curOf is a
    // current-stage snapshot and cntOf is ever-reached, so this is a stall rate,
    // not a conversion rate -- it goes DOWN as the backlog clears, and a high
    // number means work is piling up at approval.
    // Gated on the flow actually containing the stage: it is in both the demo and
    // virtual paths, so this reports for either, and null for a flow without it.
    // "No Call Scheduled Yet": deals where a call IS expected but no
    // clinician_meeting_booked_date exists yet.
    // Field Rep signups are excluded from BOTH sides. No call is booked for that
    // workflow, so counting them as "not yet scheduled" would be counting an
    // absence that is by design -- the same reason the All Clinicians column
    // renders them "Field Signup" instead of a red X. This matters most in the
    // combined view, where field rows would otherwise inflate the rate.
    // Expect this to read near 0% for the demo flow: those deals enter AT Meeting
    // Scheduled, so a booked date is implicit. It is the VIRTUAL flow where the
    // call is a separate step and this number is the real signal.
    // VIRTUAL-FLOW ONLY (per Alex, 2026-09-08). Both of these describe problems
    // specific to the virtual path -- a call that must be booked as its own step,
    // and the approval queue ahead of onboarding -- so the POPULATION is scoped
    // here rather than the render being gated. Consequences of doing it this way:
    //   * on the Demo pill there are no virtual rows, so both report null and
    //     render as an em-dash, which reads correctly as "not applicable";
    //   * in the combined view they still report the virtual number, and the
    //     client labels them "Virtual flow only" so that is not mistaken for the
    //     whole population.
    // Scoping to virtual also subsumes the old Field exclusion: a field row can
    // never be virtual, so it cannot inflate either denominator.
    const vRows = rs.filter(r => r.flow === 'virtual');
    const vCur = (id) => vRows.filter(r => r.cur === id).length;
    const vCnt = (id) => vRows.filter(r => r.reachedIds.has(id)).length;
    const noCallYet = vRows.filter(r => !r.hasBookedCall).length;
    const caReached = vCnt(CLS.CLINICIAN_APPROVED);
    const caStuck = vCur(CLS.CLINICIAN_APPROVED);
    const hasNoShowExit = flowKey === 'both' || CLINICIAN_FLOWS[flowKey].exits.indexOf(CLINICIAN_STAGE_NO_SHOW) >= 0;
    const exitDefs = flowKey === 'both'
      ? [{ label: 'No Show', id: CLINICIAN_STAGE_NO_SHOW }, { label: 'Not a Fit', id: CLINICIAN_STAGE_NOT_A_FIT }]
      : CLINICIAN_FLOWS[flowKey].exits.map(id => ({ id, label: id === CLINICIAN_STAGE_NO_SHOW ? 'No Show' : 'Not a Fit' }));
    return {
      total: rs.length,
      flow: flowKey,
      flowLabel: flowKey === 'both' ? 'Both Flows' : CLINICIAN_FLOWS[flowKey].label,
      funnel: stageList.map((st, i) => {
        const nextId = i < stageList.length - 1 ? stageList[i + 1].id : null;
        return {
          id: st.id, label: st.label, count: curOf(st.id), reachedCount: cntOf(st.id),
          // Conversion uses EVER-REACHED counts so it reads as a true funnel; the
          // bar itself still shows current stage.
          //
          // In the combined ('both') view the rendered list interleaves THREE
          // different paths, so adjacent entries are often not consecutive for
          // any actual deal — Clinician Inquiry (virtual-only) sits directly
          // above Signed Up (demo-only), and dividing one population by the
          // other produced pills reading 200%. A conversion is only meaningful
          // among deals whose own flow walks BOTH stages, so that is the
          // population it is computed over. For a single-flow view every row
          // qualifies and this is identical to the old behaviour; in the
          // combined view a pair no flow walks together has no denominator and
          // honestly reports nothing rather than a fabricated percentage.
          ...(function(){
            if (!nextId) return { convToNext: null, convNum: null, convDenom: cntOf(st.id) };
            const elig = rs.filter(r => {
              const f = CLINICIAN_FLOWS[r.flow];
              return !!f && f.stages.indexOf(st.id) >= 0 && f.stages.indexOf(nextId) >= 0;
            });
            const den = cntIn(elig, st.id), num = cntIn(elig, nextId);
            return { convToNext: pct(num, den), convNum: num, convDenom: den };
          })(),
        };
      }),
      exits: exitDefs.map(e => ({ label: e.label, count: rs.filter(r => r.cur === e.id).length })),
      stats: {
        // Of clinicians who received samples, how many submitted their 1st review.
        reviewRate: pct(cntOf(CLS.REVIEW_1), cntOf(CLS.SAMPLE_1)),
        reviewNum: cntOf(CLS.REVIEW_1), reviewDenom: cntOf(CLS.SAMPLE_1),
        // Sum of the CONTACT property reviews_submitted across this slice.
        // Deliberately NOT the same thing as reviewNum, which counts DEALS that
        // ever reached the 1st Review Submitted stage — one clinician can submit
        // several reviews, so this normally reads higher. The UI names the source
        // so the two adjacent numbers don't look like a bug.
        reviewsSubmittedTotal: reviewsTotal,
        reviewsSubmittedContacts: reviewsContacts,
        reviewsSubmittedNoContact: reviewsNoContact,
        // pct() returns null on a zero denominator, so a segment with no virtual
        // rows (the Demo pill) yields null without needing a separate gate.
        noCallRate: pct(noCallYet, vRows.length),
        noCallNum: noCallYet, noCallDenom: vRows.length,
        stuckApprovedRate: pct(caStuck, caReached),
        stuckApprovedNum: caStuck, stuckApprovedDenom: caReached,
        showRate: hasMeeting ? pct(mh, msReached) : null,
        formBeforeDemoRate: hasMeeting ? pct(fvdBefore, fvdAnswerable) : null,
        formVsDemoDist: hasMeeting ? fvdDist : null,
        formBeforeDemoNum: hasMeeting ? fvdBefore : null,
        formBeforeDemoDenom: hasMeeting ? fvdAnswerable : null,
        closeRate: hasMeeting ? pct(fsMeet, mh) : null,
        noShowRate: hasNoShowExit ? pct(noShow, supposedNS) : null,
        disqualRate: hasMeeting ? pct(notAFitQual, mh) : null,
        pruneRate: hasMeeting ? pct(cancelled, supposedPrune) : null,
        velocityDays: vel.length ? vel.reduce((a, b) => a + b, 0) / vel.length : null,
        velocityN: vel.length,
        // Why velocity may be blank: how many deals ever reached Fully Onboarded
        // vs how many actually carry the stage timestamps it needs.
        velocityFOReached: rs.filter(r => r.reachedIds.has(CLS.FULLY_ONBOARDED)).length,
        velocityStamped: rs.filter(r => r.hasEnteredMS && r.hasEnteredFO).length,
        noShowNum: hasNoShowExit ? noShow : null, noShowDenom: hasNoShowExit ? supposedNS : null,
        disqualNum: hasMeeting ? notAFitQual : null, disqualDenom: hasMeeting ? mh : null,
        pruneNum: hasMeeting ? cancelled : null, pruneDenom: hasMeeting ? supposedPrune : null,
        notAFitAll: notAFit,
        showNum: hasMeeting ? mh : null, showDenom: hasMeeting ? msReached : null,
        closeNum: hasMeeting ? fsMeet : null, closeDenom: hasMeeting ? mh : null,
      },
    };
  };
  // Accepts the legacy combined value AND the split ones. The onboarding form
  // offers MD and DO separately, and clinician_type is being widened to match it
  // (2026-09-11), so a signup writing 'MD' must still land in the MD/DO segment.
  // Historical records hold 'MD/DO', so all three have to match.
  const isMD = (t) => /^(md|do|md\/do)$/i.test((t || '').trim());
  // Top level stays the combined view so existing consumers are unchanged;
  // byFlow carries the same three type segments computed per workflow, which is
  // what the funnel card's flow toggle reads.
  const flowSegs = (fk) => ({
    all: seg(() => true, fk),
    mddo: seg(r => isMD(r.type), fk),
    nonmddo: seg(r => !isMD(r.type), fk),
  });
  const flowCounts = {};
  for (const k of Object.keys(CLINICIAN_FLOWS)) flowCounts[k] = rows.filter(r => r.flow === k).length;
  // Onboarding conversion by ENTRY PATH — the one comparison the flow pills
  // cannot make, because each pill scopes every stat to a single flow:
  //   old (Demo)    enters at Meeting Scheduled
  //   new (Virtual) enters at Clinician Inquiry
  // Each rate divides Fully Onboarded by that flow's OWN entry stage, both
  // ever-reached, so the two are like-for-like even though the flows walk stage
  // sequences of different lengths (10 stages vs 7).
  //
  // Field Rep is reported SEPARATELY rather than folded into "came through
  // Clinician Inquiry". It does enter at that same stage, but it is a different
  // acquisition motion — a rep signing someone up in person, not inbound — so
  // merging it would move the new-flow rate for reasons unrelated to the inbound
  // funnel. Its own stage sequence is still the ASSUMED one (see CLINICIAN_FLOWS),
  // which is a second reason not to blend it into a headline number.
  const _fcPct = (n, d) => (d > 0 ? (n / d) * 100 : null);
  const flowConv = (k) => {
    const f = CLINICIAN_FLOWS[k];
    if (!f) return null;
    const entryId = f.stages[0];
    const inFlow = rows.filter(r => r.flow === k);
    const den = inFlow.filter(r => r.reachedIds.has(entryId)).length;
    const num = inFlow.filter(r => r.reachedIds.has(CLS.FULLY_ONBOARDED)).length;
    const entry = CLINICIAN_STAGES.find(x => x.id === entryId);
    return {
      key: k, label: f.label, short: CLINICIAN_FLOW_SHORT[k] || k,
      entryLabel: (entry && entry.label) || '',
      rate: _fcPct(num, den), num, den,
      // Exposed so a gap between "deals in this flow" and "deals that reached its
      // entry stage" is visible rather than quietly shrinking the denominator.
      deals: inFlow.length,
    };
  };
  const _fcOld = flowConv('demo'), _fcNew = flowConv('virtual');
  // Field is intentionally NOT in the old-vs-new PAIR (removed 2026-09-08). It has
  // its own segment under byFlow and its own pill on the funnel card; it just does
  // not belong in a two-way read. flowConv stays generic so it can be added back
  // with one key.
  //
  // The roll-up is the two INBOUND paths only — Demo + Virtual. Field Rep was
  // dropped from it on 2026-09-15 (Alex), for the same reason it is not in the
  // old-vs-new pair: a rep signing someone up in person is a different motion, and
  // its stage sequence is still the ASSUMED one, so folding it into a headline
  // rate moved that rate for reasons unrelated to the inbound funnel this strip
  // compares. Field still has its own pill and its own byFlow segment.
  //
  // Summing each flow's own numerator and denominator is what makes the two
  // comparable — they enter at different stages, so there is no single entry stage
  // to divide by.
  //
  // INVARIANT, now that it is a true roll-up of exactly the two rates beside it:
  // this rate must land between them (inclusive). If it ever falls outside that
  // range, the parts and the whole disagree and something upstream is wrong.
  const FC_ROLLUP_FLOWS = ['demo', 'virtual'];
  const _fcAll = (function () {
    let num = 0, den = 0, deals = 0;
    for (const k of FC_ROLLUP_FLOWS) {
      const c = flowConv(k);
      if (!c) continue;
      num += c.num; den += c.den; deals += c.deals;
    }
    return {
      key: 'all', label: 'Demo + Virtual', short: 'Demo + Virtual',
      entryLabel: "each flow's own entry stage",
      rate: _fcPct(num, den), num, den, deals,
    };
  })();
  const flowCompare = {
    all: _fcAll, old: _fcOld, new: _fcNew,
    deltaPp: (_fcOld && _fcNew && _fcOld.rate != null && _fcNew.rate != null)
      ? _fcNew.rate - _fcOld.rate : null,
  };
  console.log(`Clinician flows: ${Object.keys(flowCounts).map(k => k + '=' + flowCounts[k]).join(' ')}`);
  return {
    all: seg(() => true, 'both'),
    mddo: seg(r => isMD(r.type), 'both'),
    nonmddo: seg(r => !isMD(r.type), 'both'),
    byFlow: Object.fromEntries(Object.keys(CLINICIAN_FLOWS).map(k => [k, flowSegs(k)])),
    // Flow-invariant: computed from every row, not the selected segment.
    flowCompare,
    flowMeta: Object.fromEntries(Object.keys(CLINICIAN_FLOWS).map(k => [k, {
      label: CLINICIAN_FLOWS[k].label,
      short: CLINICIAN_FLOW_SHORT[k] || k,
      count: flowCounts[k],
      stageOrder: CLINICIAN_FLOWS[k].stages.map(id => (CLINICIAN_STAGES.find(x => x.id === id) || {}).label),
    }])),
    rows: rows.map(r => ({ qual: r.qual, att: r.att, type: r.type, flow: r.flow })),
    stageOrder: CLINICIAN_STAGES.map(x => x.label),
    contactsMatched: Object.keys(contactsByDeal).length,
    excludedFuture,
  };
}

function buildSignUpCohorts(allDeals, cohortMonths, ownerMap) {
  // Sign-Up Rate cohorts use a single ATTENDANCE basis for all four rates. For
  // each cohort month (date_demo_booked within month):
  //   held       = demo_attendance_status ∈ {Demo Given orig, Demo Given resched}
  //                OR dealstage = Closed Won (a won deal's demo happened)
  //   signed     = won                       (⊆ held)
  //   pctSigned  = signed ÷ held
  //   pctPending = (held − signed) ÷ held    (held demos not yet signed)
  //   pctPruned  = cancelled ÷ (held + noShow + cancelled)   (attendance)
  //   pctNoShow  = noShow ÷ (held + noShow)                  (attendance)
  //   avgDaysToClose = mean(closedate − date_demo_booked) across signed deals
  // Tier scope (all tiers vs excl. small brands) is applied by the client
  // toggle, which filters the deal shadows before recomputing — so no
  // small-brand gate lives in this default (all-tiers) build.
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

    // All traffic tiers are included in the base — matches the Detailed
    // Dashboard's all-traffic "Qualified Demos (Held)". The client's
    // "Excluding Small Brands" toggle removes Pre-launch / 0-10K / Very Small
    // on demand; the worker no longer drops pre-launch here.
    const wt = (p.average_monthly_web_traffic__cloned_ || p.average_monthly_web_traffic || '').toLowerCase();

    b.allBooked++;
    b.byRep[oid].allBooked++;
    // Attendance-based counts drive ALL four Sign-Up rates now (held / signed /
    // pending / pruned / no-show share one basis). No small-brand gate here —
    // the client's All Deals / Excluding Small Brands toggle filters tiers on
    // demand, so the default (server) cohort includes every tier.
    attBump(b, att, stage === STAGE_WON);
    attBump(b.byRep[oid], att, stage === STAGE_WON);

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
    // Attendance basis: "held" = demos that actually happened (demo_attendance_status
    // = Demo Given orig/resched, or Closed Won which implies a demo). Signed,
    // Pending, Pruned and No-Show all key off this single basis.
    //   demosHeld  = attHeld
    //   signed     = cntWon           (⊆ attHeld, since Won counts as held)
    //   pending    = attHeld − cntWon (held demos not yet signed)
    const demosHeld = b.attHeld;
    const cntPending = Math.max(0, b.attHeld - b.cntWon);
    // % Pruned / % No-Show — attendance ÷ settled outcomes (same held basis).
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
  }], ['demo_given__status','demo_attendance_status','demo_qualification_outcome','dealstage','hubspot_owner_id','date_demo_booked','rescheduled_meeting_date','average_monthly_web_traffic__cloned_'], 200,
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
  const totalQualified = c.pipeline.qualifiedRawCount || 0;   // CPQD denominator → canonical Qualified Demos
  const totalQualifiedScale = demosHeldStrictCount; // True CPQD denominator → Demos Held excl. small brands
  const cpdTotal = totalScheduled > 0 ? totalSpend/totalScheduled : null;
  const cpqdTotal = totalQualified > 0 ? totalSpend/totalQualified : null;
  const trueCpqdTotal = totalQualifiedScale > 0 ? totalSpend/totalQualifiedScale : null;

  const pTotalS = p.adSpend?.total?.spend||0;
  const pTotalWD = p.adSpend?.total?.windsorDemos||0;
  const pTotalSch = prior ? (p.scheduled?.total||0) : 0;
  const pTotalQ = prior ? (p.pipeline?.qualifiedCount||0) : 0;
  // Prior denominators must match the current one or the deltas compare two
  // different metrics. True CPQD keeps its own small-brand-excluded basis below.
  const pTotalQual = prior ? (p.pipeline?.qualifiedRawCount||0) : 0;
  const pTotalQualScale = prior ? (p.pipeline?.demoGivenStrictCount!=null ? p.pipeline.demoGivenStrictCount : (p.pipeline?.demoGivenScaleCount||0)) : 0;
  const pmTotalS = pm.adSpend?.total?.spend||0;
  const pmTotalWD = pm.adSpend?.total?.windsorDemos||0;
  const pmTotalSch = priorMonth ? (pm.scheduled?.total||0) : 0;
  const pmTotalQ = priorMonth ? (pm.pipeline?.qualifiedCount||0) : 0;
  const pmTotalQual = priorMonth ? (pm.pipeline?.qualifiedRawCount||0) : 0;
  const pmTotalQualScale = priorMonth ? (pm.pipeline?.demoGivenStrictCount!=null ? pm.pipeline.demoGivenStrictCount : (pm.pipeline?.demoGivenScaleCount||0)) : 0;

  const executiveSummary = {
    totalDemosScheduled: buildTile(c.scheduled.total, p.scheduled?.total??null, pm.scheduled?.total??null, 'Contacts created in period with date_demo_booked set'),
    totalCpd: buildTile(cpdTotal, prior&&pTotalSch>0?pTotalS/pTotalSch:null, priorMonth&&pmTotalSch>0?pmTotalS/pmTotalSch:null, 'Total Ad Spend ÷ New Demos Scheduled'),
    qualificationRate: buildTile(c.pipeline.qualificationRate, p.pipeline?.qualificationRate??null, pm.pipeline?.qualificationRate??null, 'Qualified ÷ (Qualified + Disqualified)'),
    disqualificationRate: buildTile(c.pipeline.disqualificationRate, p.pipeline?.disqualificationRate??null, pm.pipeline?.disqualificationRate??null, 'Disqualified ÷ (Qualified + Disqualified)'),
    totalCpqd: buildTile(cpqdTotal, prior&&pTotalQual>0?pTotalS/pTotalQual:null, priorMonth&&pmTotalQual>0?pmTotalS/pmTotalQual:null, 'Total Ad Spend ÷ Qualified Demos (held excl. pre-launch + Webinar Happened)'),
    totalTrueCpqd: buildTile(trueCpqdTotal, prior&&pTotalQualScale>0?pTotalS/pTotalQualScale:null, priorMonth&&pmTotalQualScale>0?pmTotalS/pmTotalQualScale:null, 'Total Ad Spend ÷ Demos Held excl. small brands (Pre-launch + 0-10K)'),
    totalQualifiedDemos: buildTile(c.pipeline.qualifiedRawCount||0, p.pipeline?.qualifiedRawCount??null, pm.pipeline?.qualifiedRawCount??null, 'Demos actually held (excl. pre-launch) plus webinars that reached Webinar Happened'),
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
  // Budget total also skips KPI-excluded channels, so pacing compares like with
  // like against the KPI spend total above.
  const totalBudget = Object.entries(budgets).reduce((s,[k,v]) => s + (KPI_EXCLUDED_CHANNELS.has(k) ? 0 : v), 0);
  const adSpend = {
    total: buildTile(c.adSpend.total.spend, p.adSpend?.total?.spend??null, pm.adSpend?.total?.spend??null, 'Sum of all channel spend'),
    channels: {}, budgets, totalBudget,
  };
  adSpend.total.windsorDemos = c.adSpend.total.windsorDemos;
  adSpend.total.priorWindsorDemos = p.adSpend?.total?.windsorDemos ?? null;
  const _manualCh = {};   // channel -> hand-entered conversion figures covering this window
  for (const ch of DASH_CHANNELS) {
    // c.period, NOT c.window — buildPeriodData returns `period` and has no
    // `window` key, so this read was undefined on every request and the window
    // guard in manualConv never fired. The August figures were therefore applied
    // to every window, including months they do not cover.
    const _mc = CHANNELS_NO_CONV.has(ch) ? manualConv(ch, c.period && c.period.from, c.period && c.period.to) : null;
    if (_mc) _manualCh[ch] = _mc;
    adSpend.channels[ch] = {
      label: CHANNEL_LABELS[ch], spend: c.adSpend.channels[ch]?.spend||0,
      windsorDemos: _mc ? _mc.total : (c.adSpend.channels[ch]?.windsorDemos||0),
      ctr: c.adSpend.channels[ch]?.ctr||0,
      budget: budgets[ch]||0,
      pendingConnector: PENDING_CHANNELS.has(ch),
      // Renders as its own row but must not feed any roll-up KPI (spend total,
      // budget total, CPD/CPQD/CAC/ROAS, pacing). Client honours this too.
      kpiExcluded: KPI_EXCLUDED_CHANNELS.has(ch),
      // No conversion field exists upstream — show "—", not 0. See CHANNELS_NO_CONV.
      // Unless a hand-entered figure covers this window, in which case use it and
      // tell the client so it can date-stamp the number.
      convNA: CHANNELS_NO_CONV.has(ch) && !_manualCh[ch],
      convManual: _manualCh[ch] ? { asOf: _manualCh[ch].asOf, from: _manualCh[ch].from, to: _manualCh[ch].to } : null,
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
      // The new definition (held, excl. pre-launch), not the categoriser bucket.
      qualified: c.pipeline.byChannel[ch]?.qualDemo||0,
      closedWon: c.closedWon.byChannel[ch]?.count||0,
      closedWonMRR: c.closedWon.byChannel[ch]?.mrr||0,
    };
  }

  // ── Demo Tracking ──
  const demoTracking = {
    totalScheduled: buildTile(c.scheduled.total, p.scheduled?.total??null, pm.scheduled?.total??null, 'Contacts created in period with date_demo_booked set'),
    demosToOccur: buildTile(c.pipeline.totalExtended, p.pipeline?.totalExtended??null, pm.pipeline?.totalExtended??null, 'Total deals scheduled in the time window (incl. future-dated for MTD/YTD)'),
    demosToOccurStrict: buildTile(c.pipeline.totalExtendedStrict, p.pipeline?.totalExtendedStrict??null, pm.pipeline?.totalExtendedStrict??null, 'Deals scheduled in the window, excluding small brands (Pre-launch + 0-10K + Very Small)'),
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
      disqualifiedRawCountPrior: p.pipeline?.disqualifiedRawCount ?? null,
      // Demos supposed to happen, pre-launch excluded.
      heldExPre: c.pipeline.heldExPre||0,
      heldExPrePrior: p.pipeline?.heldExPre ?? null,
      heldExPreLastMonth: pm.pipeline?.heldExPre ?? null,
      noShowExPre: c.pipeline.noShowExPre||0,
      noShowExPrePrior: p.pipeline?.noShowExPre ?? null,
      noShowExPreLastMonth: pm.pipeline?.noShowExPre ?? null,
      cancelledExPre: c.pipeline.cancelledExPre||0,
      cancelledExPrePrior: p.pipeline?.cancelledExPre ?? null,
      cancelledExPreLastMonth: pm.pipeline?.cancelledExPre ?? null,
      disqualifiedRawCountLastMonth: pm.pipeline?.disqualifiedRawCount ?? null,
      // THE canonical Qualified Demo figure — held demos excluding pre-launch,
      // plus webinars that reached Webinar Happened. Every section that shows
      // "Qualified Demos" reads these three, so Marketing Funnel, Demo Insights,
      // Efficiency and the Conversion Funnel cannot disagree. They previously
      // each read demosHeldCount (held, ALL traffic, webinars excluded), which is
      // why redefining the count alone moved nothing on screen.
      qualDemoCount: c.pipeline.qualifiedRawCount||0,
      qualDemoCountPrior: p.pipeline?.qualifiedRawCount ?? null,
      qualDemoCountLastMonth: pm.pipeline?.qualifiedRawCount ?? null,
      // Outcome-only tally, for the Qualification Rate's calc text. Kept apart so
      // the rate's displayed "X ÷ Y" matches the rate it sits under.
      qualOutcomeQualified: c.pipeline.qualOutcomeQualified||0,
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
      // The webinar half of the Qualified Demo count, kept visible on its own so
      // the figure can be reconciled against each source.
      webinarHeldCount: c.pipeline.webinarHeldCount||0,
      // Webinar-date deals folded into Demos to occur / supposed to happen.
      // "Extra" = the ones not already counted by that metric.
      webinarDateDealCount: c.pipeline.webinarDateDealCount||0,
      // Quality panel: demo-only figures, webinars deliberately excluded.
      qualDemoOnlyCount: c.pipeline.qualifiedDemoOnlyCount||0,
      qualDemoOnlyCountPrior: p.pipeline?.qualifiedDemoOnlyCount ?? null,
      qualDemoOnlyCountLastMonth: pm.pipeline?.qualifiedDemoOnlyCount ?? null,
      // Webinar No-Show Rate, from the contact property webinar_has_attended.
      webinarRegistrants: c.pipeline.webinarRegistrants||0,
      webinarAttended: c.pipeline.webinarAttended||0,
      webinarNoShow: c.pipeline.webinarNoShow||0,
      webinarNoShowRate: (c.pipeline.webinarRegistrants||0) > 0
        ? ((c.pipeline.webinarNoShow||0) / c.pipeline.webinarRegistrants) * 100 : null,
      webinarNoShowRatePrior: (p.pipeline?.webinarRegistrants||0) > 0
        ? ((p.pipeline.webinarNoShow||0) / p.pipeline.webinarRegistrants) * 100 : null,
      webinarNoShowRateLastMonth: (pm.pipeline?.webinarRegistrants||0) > 0
        ? ((pm.pipeline.webinarNoShow||0) / pm.pipeline.webinarRegistrants) * 100 : null,
      webinarDateExtraSupposed: c.pipeline.webinarDateExtraSupposed||0,
      webinarDateExtraSupposedPrior: p.pipeline?.webinarDateExtraSupposed ?? null,
      webinarDateExtraSupposedLastMonth: pm.pipeline?.webinarDateExtraSupposed ?? null,
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
    scheduledByDayAll: c.scheduled.byDayAll,
    scheduledByDayScale: c.scheduled.byDayScale,
    // Stricter daily series — excludes pre-launch AND 0-10K. Used by the
    // Irfan Dashboard "Demos Booked per Day" chart.
    scheduledByDayScale10KPlus: c.scheduled.byDayScale10KPlus,
    // Regular / Pre-Launch daily series + their window totals. Both include
    // webinars; they differ only on the brand's real web-traffic tier.
    scheduledByDayRegular: c.scheduled.byDayRegular,
    scheduledByDayPre: c.scheduled.byDayPre,
    scheduledRegularTotal: c.scheduled.regularTotal,
    scheduledPreTotal: c.scheduled.preTotal,
    scheduledRegularTotalPrior: prior ? (p.scheduled?.regularTotal ?? null) : null,
    scheduledPreTotalPrior: prior ? (p.scheduled?.preTotal ?? null) : null,
    scheduledRegularPaid: c.scheduled.regularPaid,
    scheduledRegularOrganicTagged: c.scheduled.regularOrganicTagged,
    scheduledRegularUntagged: c.scheduled.regularUntagged,
    scheduledByDayRegularPaid: c.scheduled.byDayRegularPaid,
    scheduledRegularPaidPrior: prior ? (p.scheduled?.regularPaid ?? null) : null,
    scheduledRegularPaidLastMonth: priorMonth ? (pm.scheduled?.regularPaid ?? null) : null,
    scheduledRegularTotalLastMonth: priorMonth ? (pm.scheduled?.regularTotal ?? null) : null,
    scheduledPreTotalLastMonth: priorMonth ? (pm.scheduled?.preTotal ?? null) : null,
    // Weekday-only mirrors, so the Weekday Avg tile divides a weekday numerator
    // by a weekday denominator on both sides of every comparison.
    scheduledWeekdayRegular: c.scheduled.weekdayRegular,
    scheduledWeekdayPre: c.scheduled.weekdayPre,
    scheduledWeekdayRegularPrior: prior ? (p.scheduled?.weekdayRegular ?? null) : null,
    scheduledWeekdayPrePrior: prior ? (p.scheduled?.weekdayPre ?? null) : null,
    scheduledWeekdayRegularLastMonth: priorMonth ? (pm.scheduled?.weekdayRegular ?? null) : null,
    scheduledWeekdayPreLastMonth: priorMonth ? (pm.scheduled?.weekdayPre ?? null) : null,
    // Daily total ad spend (all channels) — powers the Irfan "True CPD per Week"
    // chart (weekly spend ÷ weekly demos excl. small brands).
    spendByDay: c.adSpend.spendByDay || {},
    // Deals closed-won per day — the CAC line on that same Weekly Cost chart.
    // closedWonNoDate is the count that carries no closedate and therefore sits
    // in no week; it is surfaced so the chart can say so instead of implying a
    // complete denominator.
    closedWonByDay: c.closedWon.byDay || {},
    closedWonNoDate: c.closedWon.noCloseDate || 0,
    scheduledByWebTraffic: c.scheduled.byWebTraffic,
    scheduledByWebTrafficPrior: prior ? p.scheduled?.byWebTraffic || null : null,
    scheduledByWebTrafficLastMonth: priorMonth ? pm.scheduled?.byWebTraffic || null : null,
    // Acquisition mix over the same population as scheduledByWebTrafficTrue —
    // drives the Detailed Dashboard's "Demo Acquisition" donut.
    scheduledUtmSource: c.scheduled.utmSource || [],
    scheduledUtmMedium: c.scheduled.utmMedium || [],
    scheduledUtmCampaign: c.scheduled.utmCampaign || [],
    scheduledUtmTotal: c.scheduled.utmTotal || 0,
    scheduledUtmSourceRegular: c.scheduled.utmSourceRegular || [],
    scheduledUtmMediumRegular: c.scheduled.utmMediumRegular || [],
    scheduledUtmCampaignRegular: c.scheduled.utmCampaignRegular || [],
    scheduledUtmTotalRegular: c.scheduled.utmTotalRegular || 0,
    scheduledUtmSourcePre: c.scheduled.utmSourcePre || [],
    scheduledUtmMediumPre: c.scheduled.utmMediumPre || [],
    scheduledUtmCampaignPre: c.scheduled.utmCampaignPre || [],
    scheduledUtmTotalPre: c.scheduled.utmTotalPre || 0,
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
    scheduledWeekdayTotalAll: c.scheduled.weekdayTotalAll,
    scheduledWeekdayTotalAllPrior: prior ? (p.scheduled?.weekdayTotalAll ?? null) : null,
    scheduledWeekdayTotalAllLastMonth: priorMonth ? (pm.scheduled?.weekdayTotalAll ?? null) : null,
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
    signedChurnedByCloseCount: c.signedChurnedByCloseCount || 0,
    // Brand Trend Analysis. buildResponse forwards fields ONE BY ONE rather
    // than spreading `current`, so anything attached to currentData later in
    // processRequest has to be named here too — omitting it is silent: the
    // handler builds the series, logs it, and the client renders "No trend
    // data available yet" with no error anywhere.
    trend: c.trend || null,
    trendRange: c.trendRange || null,
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
    byDayScale: scheduled.byDayScale, byDayScale10KPlus: scheduled.byDayScale10KPlus, byDayAll: scheduled.byDayAll,
    byWebTraffic: scheduled.byWebTraffic, byWebTrafficTrue: scheduled.byWebTrafficTrue,
    byDayRegular: scheduled.byDayRegular, byDayPre: scheduled.byDayPre,
    regularTotal: scheduled.regularTotal, preTotal: scheduled.preTotal,
    regularPaid: scheduled.regularPaid, regularOrganicTagged: scheduled.regularOrganicTagged,
    regularUntagged: scheduled.regularUntagged, byDayRegularPaid: scheduled.byDayRegularPaid,
    weekdayRegular: scheduled.weekdayRegular, weekdayPre: scheduled.weekdayPre,
    // Acquisition mix — buildResponse forwards these as scheduledUtm*. This
    // object is an explicit allow-list (it exists to drop the non-serializable
    // lowTrafficCompanies Set), so anything new added to
    // processScheduledContacts' return has to be listed here too or it silently
    // arrives undefined in buildResponse.
    utmSource: scheduled.utmSource, utmMedium: scheduled.utmMedium,
    utmSourceRegular: scheduled.utmSourceRegular, utmMediumRegular: scheduled.utmMediumRegular,
    utmCampaignRegular: scheduled.utmCampaignRegular,
    utmSourcePre: scheduled.utmSourcePre, utmMediumPre: scheduled.utmMediumPre,
    utmCampaignPre: scheduled.utmCampaignPre,
    utmTotalRegular: scheduled.utmTotalRegular, utmTotalPre: scheduled.utmTotalPre,
    utmCampaign: scheduled.utmCampaign, utmTotal: scheduled.utmTotal,
    lowTrafficCount: scheduled.lowTrafficCount, low10KCount: scheduled.low10KCount,
    // Weekday-only mirrors — buildResponse forwards these as
    // scheduledWeekdayTotal{,Prior,LastMonth} so the Weekday Avg deltas
    // compare like-for-like.
    weekdayTotal: scheduled.weekdayTotal,
    weekdayTotalScale: scheduled.weekdayTotalScale,
    weekdayTotalScale10KPlus: scheduled.weekdayTotalScale10KPlus,
    weekdayTotalAll: scheduled.weekdayTotalAll,
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

// Deals that reached Webinar Happened, for the Marketing Funnel's per-month
// Qualified Demos. The funnel used to count held demos only (all traffic, no
// webinars), so its CPQD divided by a different population than the Efficiency
// card — the two could never agree.
async function fetchWebinarHappenedAllTime(token) {
  const STAMP = 'hs_v2_date_entered_' + WEBINAR_HAPPENED_STAGE;
  const rows = await hsSearch(token, 'deals', [{
    filters: [{ propertyName: STAMP, operator: 'HAS_PROPERTY' }],
  }], [STAMP, 'average_monthly_web_traffic__cloned_', 'brand_status', 'createdate'], 200,
  [{ propertyName: 'createdate', direction: 'DESCENDING' }], 20);
  // Say the count out loud. An empty result here is indistinguishable on screen
  // from "no webinars happened", and that is precisely how this read zero.
  console.log(`funnel webinars: ${(rows || []).length} deals with a ${STAMP} stamp`);
  return rows;
}

function buildMarketingFunnel(monthlySpendData, allQualified, allClosedWon, allWebinarHappened) {
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
  // Qualified Demos, on the SAME definition the Efficiency card uses: demos held
  // excluding pre-launch, plus deals that reached Webinar Happened. Previously
  // this counted held demos at ALL traffic tiers and no webinars at all, so the
  // funnel's CPQD divided spend by a population no other card reported.
  const qualByMonth = {};
  for (const deal of allQualified) {
    const p = deal.properties || {};
    const eff = p.rescheduled_meeting_date || p.date_demo_booked;
    if (!eff) continue;
    if ((p.average_monthly_web_traffic__cloned_ || '') === PRE_LAUNCH_TIER) continue;
    const key = eff.substring(0, 7);
    qualByMonth[key] = (qualByMonth[key] || 0) + 1;
  }
  const STAMP_MF = 'hs_v2_date_entered_' + WEBINAR_HAPPENED_STAGE;
  for (const deal of (allWebinarHappened || [])) {
    const p = deal.properties || {};
    const st = p[STAMP_MF];
    if (!st) continue;
    if ((p.average_monthly_web_traffic__cloned_ || '') === PRE_LAUNCH_TIER) continue;
    const key = String(st).length > 7 && String(st).includes('-')
      ? String(st).substring(0, 7)
      : new Date(Number(st) || st).toISOString().substring(0, 7);
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
// Buckets won deals by web-traffic tier for one window. Deduped by deal id
// because the two fetches (by closedate, by entered-won stamp) overlap heavily.
function buildSignedTierCohort(rows, fromStr, toStr) {
  const fromMs = toMsET(fromStr), toMs = toMsET(toStr, true);
  const byId = new Map();
  for (const r of (rows || [])) if (r && r.id) byId.set(String(r.id), r);
  const byWebTraffic = {};
  let total = 0, mrr = 0;
  for (const d of byId.values()) {
    const p = d.properties || {};
    const cd = p.closedate;
    if (!cd) continue;
    const ms = isoMs(cd);
    if (isNaN(ms) || ms < fromMs || ms > toMs) continue;
    const key = p.average_monthly_web_traffic__cloned_ || p.average_monthly_web_traffic || '(none)';
    byWebTraffic[key] = (byWebTraffic[key] || 0) + 1;
    total++;
    mrr += (parseFloat(p.amount) || 0);
  }
  return { byWebTraffic, total, mrr, fromDate: fromStr, toDate: toStr };
}

function buildSpecial1Cohort(dealsArr, fromStr, toStr, webinarHeld) {
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
  // Demos actually HELD, on the same attendance test as the rest of the
  // dashboard. The dealstage sum below cannot stand in for it: a held demo
  // that later moved to Closed Lost counts as held everywhere else and is
  // invisible to a current-stage tally.
  let cntHeld = 0;
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
    if (demoDidHappen(p.demo_attendance_status, p.demo_given__status)) cntHeld++;
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
  // Attendance-based, plus the webinar fold — the canonical Qualified Demo
  // rule (demos held excl. pre-launch + deals that reached Webinar Happened).
  // Webinar deals carry no date_demo_booked, so they cannot arrive through the
  // pipeline fetch above; an additive count is the only route, exactly as
  // applyWebinarHeld does for the page figure.
  const demosHeld = cntHeld + (webinarHeld || 0);
  const cntPending = cntAppt + cntDemoHappened + cntDM + cntCS;
  // Canonical % Pruned / % No-Show — attendance ÷ settled outcomes.
  const _ar = attRates(_att.attHeld, _att.attNoShow, _att.attCancelled);
  return {
    heldScale: demosHeld, signed: cntWon,
    heldDemosOnly: cntHeld, webinarHeld: webinarHeld || 0,
    stageSumLegacy: cntWon + cntAppt + cntDemoHappened + cntDM + cntCS,
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
  // Webinars that reached Webinar Happened in each window. They carry no
  // date_demo_booked so they cannot arrive via fetchPipelineDeals; counted
  // separately and folded in, exactly as applyWebinarHeld does for the page.
  let _s1WbPcm = 0, _s1WbMtd = 0;
  try {
    const [a, b] = await Promise.all([
      fetchWebinarHeldCount(hsToken, _pcmFromStr, _pcmToStr),
      fetchWebinarHeldCount(hsToken, _mtdFromStr, _mtdToStr),
    ]);
    _s1WbPcm = a || 0; _s1WbMtd = b || 0;
  } catch (e) { console.error('Special1 webinar counts failed:', e && e.message); }

  // Signed brands by traffic tier for the SAME two windows, so one section
  // toggle can drive both tiles. Each window fetched two ways (closedate and
  // the entered-won stamp) and deduped, matching how the page builds its own.
  let signedLastMonth = null, signedMtd = null, signedTierError = null;
  try {
    const [wPcmClose, wPcmEnter, wMtdClose, wMtdEnter] = await Promise.all([
      fetchAnyWonDealsByCloseDate(hsToken, _pcmFromStr, _pcmToStr),
      fetchAnyWonDealsByEnteredWon(hsToken, _pcmFromStr, _pcmToStr),
      fetchAnyWonDealsByCloseDate(hsToken, _mtdFromStr, _mtdToStr),
      fetchAnyWonDealsByEnteredWon(hsToken, _mtdFromStr, _mtdToStr),
    ]);
    signedLastMonth = buildSignedTierCohort(
      filterActiveBrands([...(wPcmClose||[]), ...(wPcmEnter||[])]), _pcmFromStr, _pcmToStr);
    signedMtd = buildSignedTierCohort(
      filterActiveBrands([...(wMtdClose||[]), ...(wMtdEnter||[])]), _mtdFromStr, _mtdToStr);
    console.log(`Special1 signed tiers: lastMonth=${signedLastMonth.total} mtd=${signedMtd.total}`);
  } catch (e) {
    signedTierError = (e && e.message) || 'unknown error';
    console.error('Special1 signed-tier cohort failed:', signedTierError);
  }

  const priorMonthHeldSigned = buildSpecial1Cohort(deals, _pcmFromStr, _pcmToStr, _s1WbPcm);
  const mtdHeldSigned        = buildSpecial1Cohort(deals, _mtdFromStr, _mtdToStr, _s1WbMtd);
  return {
    priorMonthHeldSigned,
    mtdHeldSigned,
    // Signed-by-tier for the same two windows, so the section toggle drives
    // both tiles from one fetch family rather than one tile silently showing a
    // 14-day cohort while the pill says Last Month.
    signedLastMonth,
    signedMtd,
    signedTierError,
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
  //
  // Subrequest accounting (the ceiling is 50 for the WHOLE invocation, shared
  // with every HubSpot page — see the SUBREQ block). Two of these groups are
  // window-independent or immutable, so they are cached behind a SINGLE KV key
  // each. One key, not one per fetch: a KV get costs a subrequest too, so N keys
  // replacing N fetches would save nothing.
  //
  //   fetchMonthlyAdSpend  4 Windsor fetches -> 1 KV get   (-3)
  //   priorMonth trio      8 Windsor fetches -> 1 KV get   (-7)
  //
  // That is 10 of the ~35 a Year to Date load had to shed, and it costs nothing:
  // fetchMonthlyAdSpend's range is hardcoded 2026-01-01..yesterday (identical on
  // every window, every request, all day), and priorMonth is a closed calendar
  // month that can never change again.
  const _pmKey = priorMonth ? `pm_windsor_${priorMonth.from}_${priorMonth.to}_v1` : null;
  const windsorPromises = [
    fetchWindsorAds(apiKey, wFrom(current.from), current.to),
    fetchLinkedInDemos(apiKey, wFrom(current.from), current.to),
    fetchGA4(apiKey, current.from, current.to),
    // Date-stamped key: rolls at midnight ET on its own, so it can never go stale.
    kvCachedFetch(env, `monthly_adspend_${fmt(yesterdayET())}_v1`, 43200,
      async () => {
        const r = await fetchMonthlyAdSpend(apiKey);
        // Never cache an empty result — a failed fetch would blank the Marketing
        // Funnel for the whole TTL, which is worse than paying the 4 fetches.
        if (!r || !r.byMonth || !Object.keys(r.byMonth).length) throw new Error('monthlyAdSpend empty — not caching');
        return r;
      }, null),
  ];
  if (prior) windsorPromises.push(fetchWindsorAds(apiKey, wFrom(prior.from), prior.to), fetchLinkedInDemos(apiKey, wFrom(prior.from), prior.to), fetchGA4(apiKey, prior.from, prior.to));
  if (priorMonth) windsorPromises.push(kvCachedFetch(env, _pmKey, 86400,
    async () => {
      const [a, l, g] = await Promise.all([
        fetchWindsorAds(apiKey, wFrom(priorMonth.from), priorMonth.to),
        fetchLinkedInDemos(apiKey, wFrom(priorMonth.from), priorMonth.to),
        fetchGA4(apiKey, priorMonth.from, priorMonth.to),
      ]);
      if (!Array.isArray(a) || !a.length) throw new Error('priorMonth ads empty — not caching');
      return { a, l, g };
    }, null));
  const windsorResults = await Promise.all(windsorPromises);
  let wIdx = 0;
  const cW = windsorResults[wIdx++], cLI = windsorResults[wIdx++], cG = windsorResults[wIdx++];
  const _mas = windsorResults[wIdx++];
  const monthlyAdSpend = _mas || { byMonth: {}, byChannelMonth: {} };
  let pW, pLI, pG; if (prior) { pW = windsorResults[wIdx++]; pLI = windsorResults[wIdx++]; pG = windsorResults[wIdx++]; }
  let pmW, pmLI, pmG;
  if (priorMonth) {
    const _pm = windsorResults[wIdx++] || {};
    // Preserve the pre-cache contract EXACTLY. Note the asymmetry:
    // fetchWindsorAds/fetchGA4 return arrays, but fetchLinkedInDemos returns a
    // NUMBER or null (worker.js fetchLinkedInDemos) and downstream code tests
    // `cLI == null`. Defaulting that one to [] would turn a real 0 into a truthy
    // empty array and read as "LinkedIn fetch succeeded with no demos" when it
    // had actually failed.
    pmW = Array.isArray(_pm.a) ? _pm.a : [];
    pmLI = (typeof _pm.l === 'number') ? _pm.l : null;
    pmG = Array.isArray(_pm.g) ? _pm.g : [];
  }

  // ── Fetch diagnostics ──────────────────────────────────────────────────────
  // Surfaced on the payload (meta.fetchDiag) and rendered by the dashboard, so a
  // silently-empty upstream fetch is visible instead of just showing $0. Windsor
  // helpers swallow their own errors and return [], which is why an outage looks
  // like a legitimate zero — this makes the difference observable.
  const _diagSpend = (rows) => {
    if (!Array.isArray(rows)) return null;
    let s = 0, ds = {};
    for (const r of rows) {
      s += parseFloat(r.spend) || 0;
      const k = (r.datasource || '?').toLowerCase();
      ds[k] = (ds[k] || 0) + (parseFloat(r.spend) || 0);
    }
    return { rows: rows.length, spend: Math.round(s * 100) / 100, byDatasource: ds };
  };
  const fetchDiag = {
    windsorAds:        _diagSpend(cW),
    windsorAdsPrior:   prior ? _diagSpend(pW) : null,
    windsorAdsLastMon: priorMonth ? _diagSpend(pmW) : null,
    linkedInDemos:     (cLI == null ? 'FAILED/EMPTY' : cLI),
    ga4Rows:           Array.isArray(cG) ? cG.length : (cG ? 'object' : 'FAILED/EMPTY'),
    monthlyAdSpendRows: Array.isArray(monthlyAdSpend) ? monthlyAdSpend.length : (monthlyAdSpend ? 'object' : 'FAILED/EMPTY'),
    window: { from: wFrom(current.from), to: current.to },
    subrequests: { total: SUBREQ.n, windsor: SUBREQ.windsor, hubspot: SUBREQ.hubspot, kv: SUBREQ.kv,
                   exhausted: SUBREQ.exhausted, windsorChunksDropped: SUBREQ.windsorDropped },
  };
  // Name the fetches that came back with nothing — the actionable bit.
  fetchDiag.failed = [
    (!cW || !cW.length) && 'windsorAds(current)',
    prior && (!pW || !pW.length) && 'windsorAds(prior)',
    priorMonth && (!pmW || !pmW.length) && 'windsorAds(lastMonth)',
    (!cG || (Array.isArray(cG) && !cG.length)) && 'ga4',
    (!monthlyAdSpend || (Array.isArray(monthlyAdSpend) && !monthlyAdSpend.length)) && 'monthlyAdSpend',
  ].filter(Boolean);
  if (fetchDiag.failed.length) console.error('fetchDiag: empty fetches →', fetchDiag.failed.join(', '), JSON.stringify(fetchDiag.windsorAds));

  // The Disqualification-Form / % Disqualified Routing block used to live here.
  // Deleted: `dqForm`, `dqFormError` and `calRouting` had ZERO consumers — the
  // card was replaced by Ad Budget Pacing (dashboard.html) and the payload
  // fields were still being computed and shipped for nobody. It cost 11
  // subrequests on a Year to Date load (1 forms list + ~6 submission pages + 1
  // analytics-v2 + 1 GA4 pageviews + 2 GA4 routing) out of a 50-subrequest
  // per-invocation ceiling, all of it spent BEFORE the pipeline-deal fetch —
  // which is why Key Metrics and Signed Brands came back as zeros.

  // Irfan Special #2 — MTD signed-deals fetch. Done EARLY (here, not later
  // inside the irfan response block) so it gets subrequest budget before the
  // heavy company/contact batches in Phase 2.
  //   • byClose: hs_is_closed_won=true + closedate in MTD window — pipeline-agnostic
  //   • byEntered: hs_is_closed_won=true + hs_v2_date_entered_closedwon in MTD window —
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
  // Churned deals that still carry a Close Date in the window — added to the
  // Marketing Funnel "Signed Brands" count (Close Date basis, per request).
  const cChurnedByClose = await fetchChurnedByCloseDate(hsToken, current.from, current.to);
  // fetchDiag is assembled in Phase 1, BEFORE any of these run, so an empty
  // HubSpot fetch never appeared in `failed` — the banner said "some data
  // sources may not have fully loaded" while listing only healthy ad numbers,
  // with nothing naming the actual culprit. Record the Phase 2 counts too.
  fetchDiag.hubspot = {
    scheduledContacts: Array.isArray(cSch) ? cSch.length : 'FAILED',
    pipelineDeals:     Array.isArray(cPipe) ? cPipe.length : 'FAILED',
    closedWon:         Array.isArray(cCW) ? cCW.length : 'FAILED',
    churnedByClose:    Array.isArray(cChurnedByClose) ? cChurnedByClose.length : 'FAILED',
  };
  // closedWon / churned can legitimately be 0 in a window; the two that drive
  // dataQuality (and therefore the banner) cannot.
  if (!Array.isArray(cSch) || !cSch.length) fetchDiag.failed.push('hubspot.scheduledContacts');
  if (!Array.isArray(cPipe) || !cPipe.length) fetchDiag.failed.push('hubspot.pipelineDeals');
  console.log(`fetchDiag.hubspot: ${JSON.stringify(fetchDiag.hubspot)}`);
  console.log(`cChurnedByClose: ${cChurnedByClose.length} churned deals with closedate in window`);

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
  // This runs ONE hsSearch per 50 domains and the domain count scales with the
  // cohort, so a Year to Date window turned it into dozens of subrequests. It was
  // the main contributor to "Too many subrequests by single Worker invocation",
  // which made every fetch queued after it return nothing — pipelineDeals,
  // closedWon and churnedByClose all came back 0 while contacts (fetched first)
  // were fine. This map only DECORATES Demo Quality rows with industry/timezone,
  // so capping the tail is far better than losing the whole response.
  const DQ_MAX_DOMAIN_BATCHES = 8;   // 400 domains
  const _dqLimit = Math.min(dqDomainArr.length, DQ_MAX_DOMAIN_BATCHES * 50);
  if (dqDomainArr.length > _dqLimit) {
    console.warn(`companyInfoMap: enriching ${_dqLimit} of ${dqDomainArr.length} domains — capped to protect the subrequest budget`);
  }
  for (let ci = 0; ci < _dqLimit; ci += 50) {
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
      kvCachedFetch(env, 'alltime_qualified_v2', 900, () => fetchAllQualifiedDeals(hsToken), []),
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
  // Signed Brands (Marketing Funnel) counts closed-won brands + churned deals
  // that still carry a Close Date in the window (per request).
  currentData.signedChurnedByCloseCount = cChurnedByClose.length;

  // ── Marketing Funnel (monthly historical table) ──
  // Uses KPI-aligned definition: count of deals with demo_attendance_status IN [Demo Given orig, Demo Given resched].
  // Webinar Happened deals for the funnel's Qualified Demos. KV-cached like the
  // other all-time funnel inputs: identical on every request within the TTL.
  let _mfWebinars = [];
  try {
    _mfWebinars = await kvCachedFetch(env, 'alltime_webinar_happened_v1', 900,
      () => fetchWebinarHappenedAllTime(hsToken), []);
  } catch (e) { console.error('funnel webinar fetch failed:', e && e.message); }
  const _mfWebActive = filterActiveBrands(_mfWebinars || []);
  console.log(`funnel inputs: ${(allTimeQualifiedForFunnel||[]).length} held, ${_mfWebActive.length} webinars (of ${(_mfWebinars||[]).length} fetched)`);
  currentData.marketingFunnel = buildMarketingFunnel(monthlyAdSpend, allTimeQualifiedForFunnel, filterActiveBrands(allTimeClosedWon), _mfWebActive);

  // ── Trend Analysis (Brand Dashboard) ──────────────────────────────────────
  // A fixed trailing window, deliberately NOT the page's date filter — the same
  // contract the Clinician section ships under, and the reason both share
  // clnTrendWindow rather than each carrying a lookback constant.
  //
  // KV-cached as a whole. The window ends at the same day on every request and
  // the series is a daily rollup, so one build serves every user AND every page
  // window: switching the page filter does not recompute it. That is what makes
  // the two extra HubSpot searches affordable on a path whose HubSpot calls are
  // deliberately sequential to dodge 429s.
  //
  // The Signed stat was removed from the card, and with it the closed-won slice
  // and the churned-by-close fetch that fed it — one HubSpot search per cache
  // build that nothing read. The series is now contacts + Windsor only.
  {
    const _btWin = clnTrendWindow(fmt(yd));
    currentData.trendRange = _btWin;
    try {
      currentData.trend = await kvCachedFetch(env, `brand_trend_${_btWin.from}_${_btWin.to}_v2`, 1800,
        async () => {
          const _wP = fetchWindsorAds(apiKey, wFrom(_btWin.from), _btWin.to);
          const _sch = await fetchScheduledContacts(hsToken, _btWin.from, _btWin.to);
          const _w = await _wP;
          // Never cache a series with no ad rows at all — that is a failed
          // Windsor fetch, and caching it would flatten the chart for the TTL.
          if (!Array.isArray(_w) || !_w.length) throw new Error('brand trend: no Windsor rows — not caching');
          const t = buildBrandTrend(_sch, _w, _btWin.from, _btWin.to);
          console.log(`brand trend: ${_sch.length} contacts, ${_w.length} ad rows -> ${t.day.length}d/${t.week.length}w/${t.month.length}m`);
          return t;
        }, null);
    } catch (e) { console.error('brand trend failed:', e && e.message); currentData.trend = null; }
  }

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

  // A Qualified Demo is a held demo (excl. pre-launch) OR a webinar that reached
  // Webinar Happened. The demo half is counted in processPipelineDeals; the
  // webinar half has to be fetched, because those deals never enter that loop.
  let _wbCur = null, _wbPri = null, _wbPm = null;
  try {
    [_wbCur, _wbPri, _wbPm] = await Promise.all([
      fetchWebinarHeldCount(hsToken, current.from, current.to),
      prior ? fetchWebinarHeldCount(hsToken, prior.from, prior.to) : Promise.resolve(null),
      priorMonth ? fetchWebinarHeldCount(hsToken, priorMonth.from, priorMonth.to) : Promise.resolve(null),
    ]);
    console.log(`webinars held: cur=${_wbCur} prior=${_wbPri} pm=${_wbPm}`);
  } catch (e) { console.error('webinar held count failed:', e && e.message); }
  // Folded OUTSIDE the try: applyWebinarHeld also puts the qualification rate on
  // the current definition, and that has to hold even when the webinar fetch
  // threw — otherwise a transient 429 silently reverts the rate to the old one.
  applyWebinarHeld(currentData, _wbCur);
  applyWebinarHeld(priorData, _wbPri);
  applyWebinarHeld(priorMonthData, _wbPm);

  // Deals reached through a contact whose Webinar Date falls in the window, for
  // "Demos to occur" and "Demos supposed to happen". Costs a contacts search
  // plus one association batch per 100 contacts, per period — affordable on the
  // paid plan's 1000-subrequest budget.
  let _wdCur = null, _wdPri = null, _wdPm = null;
  try {
    [_wdCur, _wdPri, _wdPm] = await Promise.all([
      fetchWebinarDateDealIds(hsToken, current.from, current.to),
      prior ? fetchWebinarDateDealIds(hsToken, prior.from, prior.to) : Promise.resolve(null),
      priorMonth ? fetchWebinarDateDealIds(hsToken, priorMonth.from, priorMonth.to) : Promise.resolve(null),
    ]);
    console.log(`webinar-date deals: cur=${_wdCur?.dealIds?.size ?? 'n/a'} (from ${_wdCur?.contacts ?? 0} contacts)`
      + ` prior=${_wdPri?.dealIds?.size ?? 'n/a'} pm=${_wdPm?.dealIds?.size ?? 'n/a'}`);
  } catch (e) { console.error('webinar-date deal lookup failed:', e && e.message); }
  applyWebinarDateDeals(currentData, _wdCur);
  applyWebinarDateDeals(priorData, _wdPri);
  applyWebinarDateDeals(priorMonthData, _wdPm);

  const resp = buildResponse(currentData, priorData, priorMonthData, isAllTime, ownerMap, windowType);
  // Clinician channel "won" is Fully Onboarded, not closed-won — see
  // fetchClinicianOnboardedCount. Applied after the build so nothing else in the
  // response shape has to know about it.
  try {
    const _clnWon = await fetchClinicianOnboardedCounts(hsToken, current, prior, priorMonth);
    applyClinicianWon(resp.adSpend && resp.adSpend.channels, _clnWon);
  } catch (e) { console.error('clinician won count failed:', e && e.message); }
  // Recount at the end: fetchDiag is built mid-flight, so its snapshot misses
  // everything issued after it. This is the number to read in the tail log.
  fetchDiag.subrequests = { total: SUBREQ.n, windsor: SUBREQ.windsor, hubspot: SUBREQ.hubspot, kv: SUBREQ.kv,
                            exhausted: SUBREQ.exhausted, windsorChunksDropped: SUBREQ.windsorDropped };
  console.log(`SUBREQ TOTAL for ${wFrom(current.from)}..${current.to}: ${subreqSummary()}`);
  if (resp.meta) resp.meta.fetchDiag = fetchDiag;   // upstream fetch visibility
  resp.demoQualityDeals = demoQualityDeals;
  resp.webinarDeals = webinarDeals;
  resp.contactInfoMap = contactInfoMap;
  resp.companyInfoMap = companyInfoMap;

  // ── Irfan Dashboard — Special Time-Bound Tiles ──
  // Always use fixed windows (last 14 days, prior calendar month), independent of
  // the page time selector. Where possible, reuse already-fetched data instead of
  // making extra HubSpot subrequests (Cloudflare Workers cap at 50/request).
  resp.irfan = {};

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
    //   (2) deals where hs_v2_date_entered_closedwon is in MTD window → _irfanMtdByEntered
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
  // demoField null: neither connector reports a demo-equivalent conversion.
  // Reddit only exposes per-action counters (conversion_page_visit_clicks etc.
  // — page visits, not demos, so counting them as Conversions would mislead);
  // OpenAI Ads has no conversion metric at all. Spend/clicks/impressions are
  // real. Matches the 0 these channels show in Channel Performance.
  // Same Meta connector/fields; the campaign-name filter is what separates them.
  clinician:{ connector:'facebook',   demoField:CLINICIAN_CONV_FIELDS, hasFreq:true, thumbField:'thumbnail_url' },
  reddit:   { connector:'reddit',     demoField:null, hasFreq:false, thumbField:null },
  chatgpt:  { connector:'openai_ads', demoField:null, hasFreq:false, thumbField:null },
};

const AZ_PAGE_SIZE = 5000;
// OpenAI Ads rejects OUTRIGHT any request whose date_to is today or later —
// it fails the whole request rather than returning rows up to yesterday, which
// is what every other connector does. azWindsorFetch swallows the error and
// returns [], so a live window (MTD, WTD, today) silently emptied the entire
// ChatGPT channel: spend, campaigns, ad groups, all zero. Cap the end date for
// the connectors that behave this way; the rest keep asking for today.
const WINDSOR_NO_TODAY = new Set(['openai_ads']);
function windsorCapTo(connector, to) {
  if (!WINDSOR_NO_TODAY.has(connector)) return to;
  const y = fmt(yesterdayET());
  return to > y ? y : to;
}

// _chunked=true is passed by azWindsorFetchAll, which handles a capped response
// by bisecting the range; only an UNCHUNKED caller hitting the cap is a silent
// data-loss bug worth warning about.
async function azWindsorFetch(apiKey, connector, from, to, fields, _chunked) {
  to = windsorCapTo(connector, to);
  if (to < from) return [];   // window was entirely today — nothing to ask for
  const url = `https://connectors.windsor.ai/${connector}?api_key=${apiKey}&date_from=${from}&date_to=${to}&fields=${fields}&page_size=${AZ_PAGE_SIZE}`;
  for (let i = 0; i < AZ_MAX_ATTEMPTS; i++) {
    if (SUBREQ.exhausted) return [];
    // Backstop: stop BEFORE the platform limit rather than letting it throw, which
    // surfaces as a bare 503 with no JSON body and no diagnosis.
    if (SUBREQ.n >= SUBREQ_HARD_MAX) {
      SUBREQ.softFail++;
      if (!SUBREQ.exhausted) { SUBREQ.exhausted = true; console.error(`SUBREQ HARD CAP ${SUBREQ_HARD_MAX} reached — ${subreqSummary()}`); }
      return [];
    }
    try {
      SUBREQ.n++; SUBREQ.windsor++;
      const r = await fetch(url);
      if (!r.ok) { azDrain(r); throw new Error(`Windsor ${connector} ${r.status}`); }
      const j = await r.json();
      const rows = j.data || [];
      if (!_chunked && rows.length >= AZ_PAGE_SIZE) {
        console.warn(`Az Windsor ${connector} HIT THE ${AZ_PAGE_SIZE}-ROW CAP for ${from}..${to} — Windsor returns earliest dates first and drops the rest, so the most recent data in this window is MISSING. This caller should use azWindsorFetchAll.`);
      }
      return rows;
    } catch(e) {
      // A retry after the pool is gone can only fail again — and the 1-2s sleep
      // between attempts is what turned this into a multi-minute hang.
      if (noteSubreqError(e)) return [];
      if (i < 2) await sleep(1000*(i+1));
      else { SUBREQ.softFail++; console.error(`Az Windsor ${connector}:`, e.message); return []; }
    }
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
async function azWindsorFetchAll(apiKey, connector, from, to, fields, chunkDays = 30, onChunk = null) {
  // Cap here too, not just in azWindsorFetch: otherwise the chunker builds
  // sub-ranges that sit entirely past the cap and burns calls returning [].
  to = windsorCapTo(connector, to);
  if (to < from) return [];
  const _azAllT0 = Date.now();
  let _kept = 0;
  const addDays = (ds, n) => { const d = new Date(ds + 'T00:00:00Z'); d.setUTCDate(d.getUTCDate() + n); return fmt(d); };
  const daysBetween = (a, b) => Math.round((new Date(b + 'T00:00:00Z') - new Date(a + 'T00:00:00Z')) / 86400000);
  const out = [];
  const stack = [];
  let s = from, seed = 0;
  while (s <= to && seed++ < 800) { let e = addDays(s, chunkDays - 1); if (e > to) e = to; stack.push([s, e]); s = addDays(e, 1); }
  let calls = 0;
  while (stack.length && calls < 800) {
    // Windsor's share is capped so the HubSpot deal fetches sharing this
    // invocation's pool always have room. Dropping the tail of an ad table on a
    // year-wide window is a far better failure than zeroing every deal metric.
    if (SUBREQ.exhausted || SUBREQ.windsorPlanned + AZ_RESERVE_PER_CHUNK > WINDSOR_CALL_BUDGET) {
      SUBREQ.windsorDropped += stack.length;
      console.warn(`azWindsorFetchAll ${connector}: stopping with ${stack.length} chunk(s) unfetched — ${subreqSummary()}`);
      break;
    }
    // Take a wave off the stack and fetch it concurrently. The budget is checked
    // while filling the wave as well as before it, so a wave can never push
    // SUBREQ.windsor past WINDSOR_CALL_BUDGET even though its calls overlap.
    const wave = [];
    while (wave.length < AZ_CHUNK_CONCURRENCY && stack.length && calls < 800
           && SUBREQ.windsorPlanned + AZ_RESERVE_PER_CHUNK <= WINDSOR_CALL_BUDGET) {
      wave.push(stack.pop()); calls++;
      SUBREQ.windsorPlanned += AZ_RESERVE_PER_CHUNK;   // reserve BEFORE awaiting
    }
    if (!wave.length) break;
    // Handle each chunk AS IT ARRIVES, not after the whole wave. Collecting the
    // wave first held concurrency x connectors x up-to-AZ_PAGE_SIZE rows at once
    // — with 7 connectors fetching concurrently that peak is larger than the
    // whole-window row set this folding exists to avoid, and it is what pushed
    // the Worker past its 128MB limit once chunks stopped being serial.
    await Promise.all(wave.map(([a, b]) =>
      azWindsorFetch(apiKey, connector, a, b, fields, true)
        .then(rows => {
          // Bisection is unchanged: a chunk that came back AT the row cap was
          // truncated by Windsor, so it is split and retried on a later wave.
          if (rows.length >= AZ_PAGE_SIZE && a !== b) {
            const mid = addDays(a, Math.floor(daysBetween(a, b) / 2));
            stack.push([a, mid], [addDays(mid, 1), b]);  // truncated → split & retry
          } else if (onChunk) {
            // Fold and drop, before the rest of the wave has even landed.
            _kept += rows.length; onChunk(rows);
          } else {
            out.push(...rows);
          }
        })
        .catch(() => {})));
  }
  const _el = Date.now() - _azAllT0;
  // Only the slow ones, or this drowns the useful lines on a short window.
  if (_el > 3000) console.log(`azChunk ${connector} ${from}..${to} d=${chunkDays}: ${calls} call(s), ${onChunk ? _kept + ' row(s) folded' : out.length + ' row(s)'}, ${_el}ms`);
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
  // Reddit uses campaign_name; OpenAI Ads only has `campaign` (no _name).
  // Both are .catch-guarded so a failure on these newer connectors can't reject
  // Promise.all and take the established channels down with them.
  // Campaign rows fold per chunk and are dropped, like creatives. A YTD facebook
  // campaign fetch is ~19k rows; holding every connector's at once contributed to
  // exceeding the Worker's 128MB limit. aggRows is a function declaration, so it
  // is hoisted and usable from these closures.
  const cm = { meta:{}, clinician:{}, google:{}, youtube:{}, tiktok:{}, reddit:{}, chatgpt:{} };
  const foldFb = rows => {
    aggRowsFold(rows, 'meta', AZ_CONNECTORS.meta, r => !isClinicianCampaign(r.campaign_name), undefined, cm.meta);
    aggRowsFold(rows, 'clinician', AZ_CONNECTORS.clinician, r => isClinicianCampaign(r.campaign_name), undefined, cm.clinician);
  };
  const foldGa = rows => {
    aggRowsFold(rows, 'google', AZ_CONNECTORS.google, r => !/\byt\b|youtube/i.test(r.campaign_name||''), undefined, cm.google);
    aggRowsFold(rows, 'youtube', AZ_CONNECTORS.youtube, r => /\byt\b|youtube/i.test(r.campaign_name||''), undefined, cm.youtube);
  };
  const [fbRows, gaRows, liCampRows, liDemoTotal, ttRows, rdRows, oaRows] = await Promise.all([
    // adset_promoted_object drops this to ad-set grain; aggRows re-aggregates by
    // campaign_name so spend/demo totals are unchanged, and azWindsorFetchAll
    // paginates so the extra rows can't be truncated.
    azWindsorFetchAll(apiKey, 'facebook', from, to, base+',conversions_submit_application_total,conversions_schedule_total,frequency,effective_status'+META_LEAD_FIELDS+META_CAPI_FIELDS+META_PROMOTED_OBJ_FIELD+META_CONV_ARRAY_FIELD, 30, foldFb),
    azWindsorFetchAll(apiKey, 'google_ads', from, to, base+',conversions,campaign_status', 30, foldGa),
    azWindsorFetchAll(apiKey, 'linkedin', from, to, 'date,campaign,campaign_group_name,spend,clicks,impressions,campaign_status'),
    fetchLinkedInDemos(apiKey, from, to),
    azWindsorFetchAll(apiKey, 'tiktok', from, to, base+',conversions,frequency,campaign_status', 30, rows => aggRowsFold(rows, 'tiktok', AZ_CONNECTORS.tiktok, undefined, undefined, cm.tiktok)),
    azWindsorFetchAll(apiKey, 'reddit', from, to, base+',campaign_status', 30, rows => aggRowsFold(rows, 'reddit', AZ_CONNECTORS.reddit, undefined, undefined, cm.reddit)).catch(e => { console.error('Reddit campaign fetch:', e.message); return []; }),
    azWindsorFetchAll(apiKey, 'openai_ads', from, to, 'date,campaign,spend,clicks,impressions,campaign_status', 30, rows => aggRowsFold(rows, 'chatgpt', AZ_CONNECTORS.chatgpt, undefined, undefined, cm.chatgpt)).catch(e => { console.error('OpenAI campaign fetch:', e.message); return []; }),
  ]);

  // Split so the row loop can run per CHUNK while the post-processing — which
  // DELETES c.freqVals and c.dates — runs exactly once at the end. Folding the
  // whole thing deleted those arrays after the first chunk, and the next chunk
  // then pushed to undefined.
  function aggRowsFold(rows, ch, cfg, filterFn, demoFilterFn, camps) {
    for (const row of rows) {
      if (filterFn && !filterFn(row)) continue;
      // OpenAI Ads returns `campaign`, every other connector `campaign_name`.
      const name = row.campaign_name || row.campaign || '(no name)';
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
        camps[name].demos += chDemoCount(ch, cfg, row);
      }
      if (cfg.hasFreq && row.frequency != null && row.frequency !== '') camps[name].freqVals.push(parseFloat(row.frequency));
    }
  }
  function aggRowsFinish(camps) {
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
  function aggRows(rows, ch, cfg, filterFn, demoFilterFn) {
    const camps = {};
    aggRowsFold(rows, ch, cfg, filterFn, demoFilterFn, camps);
    return aggRowsFinish(camps);
  }


  const liDemoFilter = r => (r.conversion_name||'').toLowerCase().includes('demo request');

  // LinkedIn hierarchy differs from Meta: Campaign Group (≈ Meta Campaign) →
  // Campaign (≈ Meta ad set) → Creative. Windsor's `campaign` field is the
  // mid-level Campaign; `campaign_group_name` is the top-level group. So the
  // Campaigns table is built from GROUPS and the Ad Sets table from the
  // individual campaigns. Per-connector rows use `campaign`, no `datasource`.
  function aggLinkedIn(rows, totalDemos) {
    const _mk = (name) => ({ name, campaign:'', spend:0, clicks:0, impressions:0, demos:0, dates:[], status:null, _statusDate:'' });
    const groups = {}, sets = {};
    for (const row of rows) {
      const setName = row.campaign || row.campaign_name || '(no name)';
      const grpName = row.campaign_group_name || setName; // fall back to campaign when group missing
      if (!sets[setName]) { sets[setName] = _mk(setName); sets[setName].campaign = grpName; }
      if (!groups[grpName]) groups[grpName] = _mk(grpName);
      for (const b of [sets[setName], groups[grpName]]) {
        b.spend += parseFloat(row.spend)||0;
        b.clicks += parseInt(row.clicks)||0;
        b.impressions += parseInt(row.impressions)||0;
        if (row.date) b.dates.push(row.date);
        // Latest date wins, ACTIVE wins ties (see rollupStatus).
        rollupStatus(b, (row.campaign_status || '').toUpperCase(), row.date);
      }
    }
    const _finalize = (obj) => {
      let tS=0,tC=0,tI=0;
      for (const c of Object.values(obj)) {
        tS+=c.spend; tC+=c.clicks; tI+=c.impressions;
        c.ctr = c.impressions>0?(c.clicks/c.impressions)*100:0;
        c.frequency = null; c.cpd = null;
        delete c._statusDate;
        if (c.dates.length) {
          c.dates.sort(); c.firstDate=c.dates[0]; c.lastDate=c.dates[c.dates.length-1];
          const fd=new Date(c.firstDate+'T12:00:00Z'),ld=new Date(c.lastDate+'T12:00:00Z');
          c.activeDays=Math.round((ld-fd)/86400000)+1;
        } else { c.firstDate=null; c.lastDate=null; c.activeDays=0; }
        delete c.dates;
      }
      return { tS, tC, tI };
    };
    const g = _finalize(groups); _finalize(sets);
    const tD = totalDemos || 0;
    return {
      campaigns: groups,                                     // Campaigns table = campaign groups
      adsets: Object.values(sets).sort((a,b)=>b.spend-a.spend), // Ad Sets table = individual campaigns
      totals:{ spend:g.tS, clicks:g.tC, impressions:g.tI, demos:tD, ctr:g.tI>0?(g.tC/g.tI)*100:0, cpd:tD>0?g.tS/tD:null, frequency:null }
    };
  }

  return {
    // One facebook fetch, split two ways — Meta excludes Clinician Targeting and
    // Clinician takes only those, so no campaign is counted in both.
    // Already folded per chunk above; fbRows/gaRows/ttRows/rdRows/oaRows are empty.
    meta:     aggRowsFinish(cm.meta),
    clinician: aggRowsFinish(cm.clinician),
    google:   aggRowsFinish(cm.google),
    youtube:  aggRowsFinish(cm.youtube),
    // LinkedIn stays accumulated: aggLinkedIn is a different shape and the
    // connector returns ~12k rows for a year, well inside budget.
    linkedin: aggLinkedIn(liCampRows.filter(r => !isRetiredLinkedInGroup(r.campaign_group_name)), liDemoTotal),
    tiktok:   aggRowsFinish(cm.tiktok),
    reddit:   aggRowsFinish(cm.reddit),
    chatgpt:  _applyManualConv(aggRowsFinish(cm.chatgpt), 'chatgpt', from, to),
  };
}

// Attempt creative-level fetch (may timeout — graceful fallback)
// The per-row creative fold, lifted out of fetchAzCreatives unchanged so it can
// run per CHUNK as rows arrive. Mutates the four aggregate maps and keeps no
// reference to `rows`, so each chunk is collectable as soon as it is folded.
function _foldCreRows(ch, cfg, rows, map, campCreMap, adsetCreMap, placementMap, tm) {
  for (const row of rows) {
      const campName = row.campaign_name || row.campaign || '';
      const isYT = /\byt\b|youtube/i.test(campName);
      const rCh = /google/.test(cfg.connector) ? (isYT ? 'youtube' : 'google') : ch;
      if (rCh !== ch) continue;
      // Meta and Clinician share the facebook connector, so split its creatives
      // by campaign name — each creative lands in exactly one of the two.
      if (ch === 'clinician' && !isClinicianCampaign(campName)) continue;
      if (ch === 'meta' && isClinicianCampaign(campName)) continue;
      const name = row.ad_name || row.ad || '(no creative)';
      // Placement tracking (Meta only — other connectors don't return these fields)
      if (row.publisher_platform || row.platform_position) {
        const plat = row.publisher_platform || 'unknown';
        const pos = row.platform_position || 'unknown';
        const pKey = plat + '|' + pos;
        if (!placementMap[pKey]) placementMap[pKey] = { platform: plat, position: pos, spend:0, clicks:0, impressions:0, demos:0 };
        placementMap[pKey].spend += parseFloat(row.spend)||0;
        placementMap[pKey].clicks += parseInt(row.clicks)||0;
        placementMap[pKey].impressions += parseInt(row.impressions)||0;
        const pDemo = chDemoCount(ch, cfg, row);
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
      { const _cl = capiConvLabel(campName, row.adset_name); if (_cl.indexOf('Demo') >= 0) map[name]._capiDemo = 1; if (_cl.indexOf('Webinar') >= 0) map[name]._capiWeb = 1; if (_cl === 'Lead Forms') map[name]._capiLead = 1; }
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
        map[name].demos += chDemoCount(ch, cfg, row);
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
        campCreMap[campKey][name].demos += chDemoCount(ch, cfg, row);
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
          adsetCreMap[adsetKey][name].demos += chDemoCount(ch, cfg, row);
        }
        if (cfg.hasFreq && row.frequency != null && row.frequency !== '') adsetCreMap[adsetKey][name].freqVals.push(parseFloat(row.frequency));
      }
    }
}

async function fetchAzCreatives(apiKey, from, to) {
  const base = 'date,campaign_name,ad_name,spend,clicks,impressions';
  // Past this span the placement dimension costs more than it is worth — see the
  // `placement` field below.
  const _creSpanDays = Math.round((new Date(to + 'T00:00:00Z') - new Date(from + 'T00:00:00Z')) / 86400000) + 1;
  const _creWideWindow = _creSpanDays > 120;
  if (_creWideWindow) console.log(`fetchAzCreatives: ${_creSpanDays}-day window — placement breakdown omitted to keep the fetch affordable`);
  const results = {};
  // ONE pathological connector must not take the whole phase past the browser's
  // abort. LinkedIn at creative grain is the known offender — the comment below
  // records it eating ~240s of a 379s YTD creatives phase — and a request that
  // never returns is also a request that never caches, so the next load is in
  // exactly the same position.
  //
  // Capping is safe because these fetches FOLD ON ARRIVAL through onChunk: rows
  // already returned are kept, so a capped channel degrades to PARTIAL creative
  // data rather than none. It counts as a soft failure, which the cache gate
  // tolerates but reports, so a channel that is persistently timing out shows up
  // in the logs instead of silently going thin.
  // 30s, not 45: this has to leave room inside the response deadline for the
  // two comparison periods, or a slow creatives phase silently costs every
  // delta on the page. A healthy run finishes here in well under 30s anyway —
  // the cap only bites on the pathological connector.
  const AZ_CRE_DEADLINE_MS = 30000;
  const _creCapped = (ch, pr) => Promise.race([
    pr,
    new Promise(r => setTimeout(() => {
      // Counted SEPARATELY from softFail, and this is not a nicety. softFail is
      // the cache gate: six channels capping at once pushed it to 7 against a
      // tolerance of 5, so the cron computed the whole payload and then refused
      // to cache it — "cron warm ytd: NOT caching — soft failures: 7". The cap,
      // added to bound a slow fetch, was what stopped anything ever being
      // warmed. A cap is a bounded, reported shortfall on ONE channel; a soft
      // failure is a fetch that gave up. Conflating them inverted the fix.
      SUBREQ.creCapped++;
      console.warn(`Creative fetch ${ch}: CAPPED at ${AZ_CRE_DEADLINE_MS}ms — keeping the rows that arrived`);
      r([]);
    }, AZ_CRE_DEADLINE_MS)),
  ]);

  // Build fetch promises for all channels in parallel (NO thumbnail fields - causes size overflow)
  // For Meta we also pull ad_status so the dashboard can distinguish ACTIVE
  // creatives from PAUSED/COMPLETED ones (rather than guessing from spend>0,
  // which mis-labels active creatives that had no impressions in the window).
  const promises = [];
  const channels = [];
  // Rows are folded per chunk and dropped — see _foldCre. A YTD Meta creative
  // window is 73k+ rows (creative x day x placement) and holding every channel's
  // rows at once exceeded the Worker's 128MB limit.
  const foldState = {};
  const _foldCre = (ch, cfg, rows) => {
    const st = foldState[ch] || (foldState[ch] = { map:{}, campCreMap:{}, adsetCreMap:{}, placementMap:{}, n:0 });
    const map = st.map, campCreMap = st.campCreMap, adsetCreMap = st.adsetCreMap, placementMap = st.placementMap;
    const tm = {};   // thumbnails are attached in the post-pass, once fetched
    st.n += rows.length;
    _foldCreRows(ch, cfg, rows, map, campCreMap, adsetCreMap, placementMap, tm);
  };
  // One fetch per CONNECTOR, folded into every channel that uses it. meta and
  // clinician both sit on facebook (and google/youtube on google_ads), and the
  // fold already splits rows by campaign name, so a second identical query bought
  // nothing and cost 36 subrequests plus ~34s of wall clock.
  const _seenConn = {};
  for (const ch of DASH_CHANNELS) {
    const cfg = AZ_CONNECTORS[ch]; if (!cfg) continue;
    if (cfg.connector && _seenConn[cfg.connector]) {
      // Already fetching this connector — attach this channel to that fold.
      _seenConn[cfg.connector].push(ch); channels.push(ch); continue;
    }
    if (cfg.connector) _seenConn[cfg.connector] = [ch];
    if (ch === 'linkedin') {
      // Was an UNCHUNKED azWindsorFetch over the whole window with 3 retries, no
      // budget guard and — because it is not azWindsorFetchAll — no azChunk log
      // line, so it was invisible. On a YTD run it accounted for ~240s of the
      // 379s creatives phase while the slowest visible chunker was 135s.
      promises.push(_creCapped('linkedin', azWindsorFetchAll(apiKey, 'linkedin', from, to, 'date,campaign,ad_name,spend,clicks,impressions', AZ_CREATIVE_CHUNK_DAYS, rows => { for (const _c of _seenConn[cfg.connector]) _foldCre(_c, AZ_CONNECTORS[_c], rows); }).catch(e => { console.error(`Creative fetch linkedin:`, e.message); return []; })));
    } else if (ch === 'chatgpt') {
      // Field ids confirmed from Windsor's openai_ads schema: ad / ad_group /
      // campaign / ad_status. Requesting ad_name, campaign_name or conversions
      // fails the entire request.
      promises.push(_creCapped('chatgpt', azWindsorFetchAll(apiKey, cfg.connector, from, to, 'date,campaign,ad_group,ad,ad_status,spend,clicks,impressions', AZ_CREATIVE_CHUNK_DAYS, rows => { for (const _c of _seenConn[cfg.connector]) _foldCre(_c, AZ_CONNECTORS[_c], rows); }).catch(e => { console.error(`Creative fetch chatgpt:`, e.message); return []; })));
    } else {
      // Meta-connector channels need BOTH standard-event columns plus the
      // promoted object, or metaDemos cannot tell which event to count.
      const extra = isMetaConnectorCh(ch)
        ? ',conversions_submit_application_total,conversions_schedule_total' + META_PROMOTED_OBJ_FIELD + META_CONV_ARRAY_FIELD
        : (cfg.demoField === 'conversions_submit_application_total' ? ',conversions_submit_application_total' : ',conversions');
      const freq = cfg.hasFreq ? ',frequency' : '';
      const video = isMetaConnectorCh(ch) ? ',video_p25_watched_actions' : '';
      // Placement multiplies rows by creative x day x PLACEMENT. On a long window
      // that is the difference between ~16k rows and ~115k, and it is the only
      // reason a Year to Date creative fetch took 6 minutes. The placement
      // breakdown is dropped for those windows; every client consumer guards an
      // empty array (dashboard.html:15005, :15062, :15104).
      const placement = (isMetaConnectorCh(ch) && !_creWideWindow) ? ',publisher_platform,platform_position' : '';
      // effective_status (not ad_status) so an ad whose parent ad set / campaign
      // is OFF reports ADSET_PAUSED / CAMPAIGN_PAUSED (→ PAUSED) instead of its
      // own ACTIVE toggle. ad_status alone made paused ad sets look Active
      // because the ads inside were still toggled on. Keep ad_status as fallback.
      const status = isMetaConnectorCh(ch) ? ',ad_status,effective_status' : '';
      // ad_created_time (Meta) → the creative's launch date, window-independent.
      // Powers the "Status Date" column (Active → launch; Paused → last run).
      const created = isMetaConnectorCh(ch) ? ',ad_created_time' : '';
      // adset_name (Meta) / ad_group_name (Google, TikTok) lets us group
      // creatives under their parent ad set for the Ad Sets table dropdown.
      // adset_promoted_object rides along on Meta (already ad grain, so no extra
      // rows) to let metaDemos count each creative's real optimized conversion.
      const adset = isMetaConnectorCh(ch) ? (',adset_name' + META_PROMOTED_OBJ_FIELD) : ((ch === 'google' || ch === 'tiktok') ? ',ad_group_name' : '');
      // Lead-form fields (Meta) so allowlisted lead-gen campaigns count
      // Leads (Form) toward Demos at the CREATIVE level too — Windsor returns
      // these per ad_name, so each creative gets its own lead count.
      const lead = isMetaConnectorCh(ch) ? META_LEAD_FIELDS : '';
      // CAPI custom-conversion counts (Demo / Webinar / Demo+Webinar) — the real
      // "Results" for campaigns that optimize for a CAPI conversion.
      const capi = isMetaConnectorCh(ch) ? META_CAPI_FIELDS : '';
      promises.push(_creCapped(ch, azWindsorFetchAll(apiKey, cfg.connector, from, to, base + extra + freq + video + placement + status + created + adset + lead + capi, AZ_CREATIVE_CHUNK_DAYS, rows => { for (const _c of _seenConn[cfg.connector]) _foldCre(_c, AZ_CONNECTORS[_c], rows); }).catch(e => { console.error(`Creative fetch ${ch}:`, e.message); return []; })));
    }
    channels.push(ch);
  }

  // Thumbnails are decoration, and they were the silent 40s on a YTD load: no
  // chunking, no timeout, the full window, and azWindsorFetch retries 3x with
  // 1-2s sleeps between attempts. Bound BOTH the window and the wait — a missing
  // thumbnail costs nothing, a stalled response costs the whole section.
  const _thumbFrom = (() => {
    const d = new Date(to + 'T00:00:00Z'); d.setUTCDate(d.getUTCDate() - 89);
    const cut = fmt(d); return cut > from ? cut : from;
  })();
  const _thumbCapped = (pr) => Promise.race([
    pr.catch(() => []),
    new Promise(r => setTimeout(() => r([]), 8000)),
  ]);
  // Separate lightweight thumbnail fetches (ad_name + thumb only, much smaller payload)
  const thumbPromises = [];
  const thumbChannels = [];
  for (const ch of DASH_CHANNELS) {
    const cfg = AZ_CONNECTORS[ch]; if (!cfg || !cfg.thumbField) continue;
    if (ch === 'linkedin') {
      thumbPromises.push(_thumbCapped(azWindsorFetch(apiKey, 'linkedin', _thumbFrom, to, 'ad_name,'+cfg.thumbField)));
    } else {
      thumbPromises.push(_thumbCapped(azWindsorFetch(apiKey, cfg.connector, _thumbFrom, to, 'ad_name,'+cfg.thumbField)));
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
    const st = foldState[ch];
    if (!st || !st.n) { results[ch] = null; continue; }
    const map = st.map;
    const campCreMap = st.campCreMap;   // Per-campaign creative breakdown
    const adsetCreMap = st.adsetCreMap; // Per-ad-set creative breakdown (Ad Sets dropdown)
    const placementMap = st.placementMap; // Placement aggregation (Meta only)
    // Folding ran before the thumbnail fetch resolved, so attach by name now.
    const tm = thumbMaps[ch] || {};
    for (const _m of [map, campCreMap, adsetCreMap]) {
      for (const v of Object.values(_m)) {
        if (v && v.name !== undefined) { if (!v.thumbnail) v.thumbnail = tm[v.name] || null; }
        else if (v) for (const c2 of Object.values(v)) { if (c2 && !c2.thumbnail) c2.thumbnail = tm[c2.name] || null; }
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
      c.capiConv = (c._capiDemo && c._capiWeb) ? 'CAPI · Demo + Webinar' : (c._capiWeb ? 'CAPI · Webinar' : (c._capiDemo ? 'CAPI · Demo' : (c._capiLead ? 'Lead Forms' : '')));
      delete c._capiDemo; delete c._capiWeb; delete c._capiLead;
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
// The per-row audience fold, lifted out of fetchAzAudiences unchanged so it can
// run per CHUNK. Mutates `map` and keeps no reference to `rows`.
function _foldAudRows(ch, cfg, rows, map) {
  for (const row of rows) {
      const name = (row[cfg.nameField] || '').trim();
      if (!name) continue;
      const campName = row.campaign_name || row.campaign || '';  // OpenAI Ads uses `campaign`
      // Clinician Targeting ad sets belong to the Clinician channel only, and
      // are correspondingly excluded from Meta.
      if (cfg.onlyClinician && !isClinicianCampaign(campName)) continue;
      if (ch === 'meta' && isClinicianCampaign(campName)) continue;
      // Google: filter out YouTube campaigns
      if (ch === 'google' && /\byt\b|youtube/i.test(campName)) continue;
      if (!map[name]) map[name] = { name, campaign: '', spend: 0, clicks: 0, impressions: 0, demos: 0, status: null, _statusDate: '', optTarget: '' };
      // Real optimization target from the platform (both facebook-connector
      // channels). Same for every row of an ad set, so first non-empty wins.
      // Gating this to 'meta' left the Clinician Ad Sets table with a blank
      // Optimization Target column — which is precisely what hid the fact that
      // clinician ad sets optimize for several different conversions.
      if (isMetaConnectorCh(ch) && !map[name].optTarget) {
        const _ot = metaOptTarget(row.adset_promoted_object);
        if (_ot) map[name].optTarget = _ot;
      }
      // Authoritative ad-set status (effective_status → PAUSED when the ad set /
      // its campaign is OFF). This is the source of truth for the row's status,
      // instead of guessing "active if any creative is active" (which read each
      // ad's own toggle and so showed paused ad sets as Active).
      if (cfg.statusField) rollupStatus(map[name], (row[cfg.statusField] || row[cfg.statusFallback] || '').toUpperCase(), row.date);
      map[name].spend += parseFloat(row.spend) || 0;
      map[name].clicks += parseInt(row.clicks) || 0;
      map[name].impressions += parseInt(row.impressions) || 0;
      map[name].demos += chDemoCount(ch, cfg, row);
      if (!map[name].campaign && campName) map[name].campaign = campName;
    }
}

async function fetchAzAudiences(apiKey, from, to) {
  const results = {};
  const configs = {
    // adset_promoted_object carries the real optimization target. It's an
    // Ad-table field but this query is already ad-set grain, so it adds no rows
    // (verified: same 13 rows / same spend total as without it).
    meta:   { connector:'facebook', fields:'date,adset_name,campaign_name,spend,clicks,impressions,conversions_submit_application_total,conversions_schedule_total,adset_status,adset_effective_status,adset_promoted_object'+META_LEAD_FIELDS+META_CAPI_FIELDS+META_CONV_ARRAY_FIELD, nameField:'adset_name', demoField:'conversions_submit_application_total', statusField:'adset_effective_status', statusFallback:'adset_status' },
    google: { connector:'google_ads', fields:'date,ad_group_name,campaign_name,spend,clicks,impressions,conversions', nameField:'ad_group_name', demoField:'conversions' },
    tiktok: { connector:'tiktok', fields:'date,ad_group_name,campaign_name,spend,clicks,impressions,conversions', nameField:'ad_group_name', demoField:'conversions' },
    // Reddit groups by ad_group_name; OpenAI Ads by ad_group (and `campaign`,
    // not campaign_name). Neither reports a demo-equivalent conversion.
    clinician: { connector:'facebook', fields:'date,adset_name,campaign_name,spend,clicks,impressions,conversions_submit_application_total,conversions_schedule_total,adset_status,adset_effective_status,adset_promoted_object'+META_LEAD_FIELDS+META_CAPI_FIELDS+META_CONV_ARRAY_FIELD, nameField:'adset_name', demoField:CLINICIAN_CONV_FIELDS, statusField:'adset_effective_status', statusFallback:'adset_status', onlyClinician:true },
    reddit: { connector:'reddit', fields:'date,ad_group_name,campaign_name,spend,clicks,impressions', nameField:'ad_group_name', demoField:null },
    chatgpt: { connector:'openai_ads', fields:'date,ad_group,campaign,spend,clicks,impressions', nameField:'ad_group', demoField:null },
  };

  // Audience rows fold per chunk too — a YTD window is ~30k rows across the
  // connectors and holding them all contributed to the 128MB overrun.
  const audMaps = {}, audN = {};
  const promises = [], chKeys = [];
  for (const [ch, cfg] of Object.entries(configs)) {
    audMaps[ch] = {}; audN[ch] = 0;
    promises.push(azWindsorFetchAll(apiKey, cfg.connector, from, to, cfg.fields, 30,
      rows => { audN[ch] += rows.length; _foldAudRows(ch, cfg, rows, audMaps[ch]); })
      .catch(e => { console.error(`Audience fetch ${ch}:`, e.message); return []; }));
    chKeys.push(ch);
  }

  await Promise.all(promises);
  for (let i = 0; i < chKeys.length; i++) {
    const ch = chKeys[i], cfg = configs[ch];
    const map = audMaps[ch];
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
    // Qualified Demo = the demo was HELD. Pre-launch is NOT excluded here: this
    // attribution runs over ad-attributed deals that carry no web-traffic tier,
    // so the ad tables' Qual column counts held demos only and will read slightly
    // higher than the dashboard's pre-launch-excluded figure. Flagged rather than
    // silently approximated.
    const _qd = demoDidHappen(p.demo_attendance_status, p.demo_given__status);
    byChannel[ch].total++; byChannel[ch][cat]++;
    byChannel[ch].qualDemo = (byChannel[ch].qualDemo||0) + (_qd?1:0);
    const camp = (p.utm_campaign||'').trim();
    if (camp) {
      const key = `${ch}::${camp.toLowerCase()}`;
      if (!byCampaign[key]) byCampaign[key] = { channel:ch, campaign:camp.toLowerCase(), qualified:0, pruned:0, noShow:0, rescheduled:0, pendingEval:0, pending:0, blank:0, total:0 };
      byCampaign[key].total++; byCampaign[key][cat]++;
      byCampaign[key].qualDemo = (byCampaign[key].qualDemo||0) + (_qd?1:0);
    }
    const creative = (p.utm_content||'').trim();
    if (creative) {
      const key = `${ch}::${creative.toLowerCase()}`;
      if (!byCreative[key]) byCreative[key] = { channel:ch, creative:creative.toLowerCase(), qualified:0, pruned:0, noShow:0, rescheduled:0, pendingEval:0, pending:0, blank:0, total:0 };
      byCreative[key].total++; byCreative[key][cat]++;
      byCreative[key].qualDemo = (byCreative[key].qualDemo||0) + (_qd?1:0);
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
    const q = (a.qualDemo!=null?a.qualDemo:(a.qualified||0)), unattr = Math.max(0, w.demos-(a.total||0));
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
    // LinkedIn: build ad-set (Campaign-level) rows for the Ad Sets table, joining
    // HubSpot UTM demo attribution per ad set (utm_campaign matches the LinkedIn
    // Campaign, not the group). Roll those demos up per campaign group so the
    // Campaigns table (grouped by campaign_group_name) shows demo totals too.
    let liAdsets = null; const liGroupDemos = {};
    if (ch === 'linkedin') {
      liAdsets = (windsor.linkedin?.adsets || []).map(a => {
        const ca = campAttr[(a.name || '').toLowerCase()] || {};
        const demos = ca.total || 0;
        if (a.campaign) liGroupDemos[a.campaign] = (liGroupDemos[a.campaign] || 0) + demos;
        return { ...a, demos, cpd: demos > 0 ? a.spend / demos : null, qualified:ca.qualified||0, pruned:ca.pruned||0, pendingEval:ca.pendingEval||0, rescheduled:ca.rescheduled||0, noShow:ca.noShow||0, pending:ca.pending||0, blank:ca.blank||0, attributedTotal:demos };
      });
    }
    // Campaign-level optimization target, rolled up from its ad sets (the target
    // is configured per ad set). One distinct target → that name; several → the
    // distinct names joined, so a mixed campaign isn't mislabeled as one thing.
    const campOptTargets = {};
    for (const a of (audiences?.[ch] || [])) {
      if (!a || !a.optTarget || !a.campaign) continue;
      const k = a.campaign.toLowerCase();
      if (!campOptTargets[k]) campOptTargets[k] = [];
      if (campOptTargets[k].indexOf(a.optTarget) < 0) campOptTargets[k].push(a.optTarget);
    }
    const mergedCamps = [];
    for (const c of Object.values(windsor[ch]?.campaigns||{})) {
      const campKey = c.name.toLowerCase();
      const ca = campAttr[campKey]||{};
      const attrTotal = ca.total||0;
      // LinkedIn: Windsor can't split demos per campaign. Campaign groups sum the
      // UTM-attributed demos of their child ad sets; other channels use Windsor.
      const demos = ch === 'linkedin' ? (liGroupDemos[c.name] || 0) : c.demos;
      const cpd = demos > 0 ? c.spend / demos : null;
      const optTarget = (campOptTargets[campKey] || []).join(' · ');
      mergedCamps.push({ ...c, demos, cpd, optTarget, qualified:ca.qualified||0, pruned:ca.pruned||0, pendingEval:ca.pendingEval||0, rescheduled:ca.rescheduled||0, noShow:ca.noShow||0, pending:ca.pending||0, blank:ca.blank||0, attributedTotal:attrTotal });
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
      // Hand-read per-ad conversions for connectors that report none (chatgpt).
      const _mAd = manualConv(ch, period && period.from, period && period.to);
      mergedCreatives = rawCreatives.map(c => {
        const ca = creativeAttr[c.name.toLowerCase()]||{};
        const attrTotal = ca.total||0;
        let demos = ch === 'linkedin' ? attrTotal : c.demos;
        if (_mAd && _mAd.zeroAll) demos = 0;
        else if (_mAd && _mAd.byAd && Object.prototype.hasOwnProperty.call(_mAd.byAd, c.name)) demos = _mAd.byAd[c.name];
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
      kpiExcluded:KPI_EXCLUDED_CHANNELS.has(ch),
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
      audiences: ch === 'linkedin' ? liAdsets : _applyManualConvAudiences(audiences?.[ch] || null, ch, period && period.from, period && period.to),
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
  // Per-phase timing. YTD was hitting the client's 75s abort and guessing which
  // fetch owned the time costs a round trip per guess; each promise now stamps
  // its own elapsed ms so the critical path is visible in `wrangler tail`.
  const _azT0 = Date.now();
  const _stamp = (name, pr) => pr.then(v => { console.log(`azPhase ${name}: ${Date.now() - _azT0}ms`); return v; });
  const [windsor, creatives, audiences, fatigue, pipe, cw] = await Promise.all([
    _stamp('campaigns', windsorP), _stamp('creatives', creativesP), _stamp('audiences', audiencesP),
    _stamp('fatigue', fatigueWithTimeout), _stamp('pipelineDeals', pipeP), _stamp('closedWon', cwP)]);
  console.log(`azPhase CURRENT-TOTAL (${windowType} ${current.from}..${current.to}): ${Date.now() - _azT0}ms — ${subreqSummary()}`);

  // Clinician Inquiries / CPI, fetched HERE rather than after the response is
  // built. These used to run last, behind the prior and prior-month HubSpot
  // pagination, so on a busy window the subrequest pool was spent before they
  // ran — hsCount returns null when that happens and every cell rendered as a
  // dash with nothing to say why. Campaign names come from the Windsor aggregate
  // (keyed by name), which is already in hand at this point.
  const _capped = (p, ms, fallback) => Promise.race([p, new Promise(r => setTimeout(() => r(fallback), ms))]);
  let _clnInq = {}, _clnInqNames = 0;
  try {
    _clnInqNames = Object.keys((windsor && windsor.clinician && windsor.clinician.campaigns) || {}).length;
    if (_clnInqNames) {
      _clnInq = await _capped(fetchClinicianInquiriesByCampaign(
        hsToken, Object.keys(windsor.clinician.campaigns), current.from, current.to), 7000, {});
    }
    console.log(`clinician inquiries: ${Object.keys(_clnInq).length}/${_clnInqNames} campaigns resolved — ${subreqSummary()}`);
  } catch (e) { console.error('clinician inquiries by campaign failed:', e && e.message); }

  // ── Response deadline ──────────────────────────────────────────────────────
  // The endpoint must ALWAYS answer. A request that runs past the browser's
  // abort returns nothing AND caches nothing, so the next load repeats it — the
  // loop behind "no response after 75s". Past this point the comparison periods
  // are the only work left, and they exist purely to put deltas on the cards:
  // the ad tables, the channel rows and every current-period figure are already
  // built. So when the clock runs out they are SKIPPED rather than waited for.
  //
  // Dropping them is a known-good shape, not a new one: All Time already runs
  // with priorW/pmW null, so every consumer downstream handles it.
  const AZ_RESP_DEADLINE_MS = 55000;
  const _azLeft = () => AZ_RESP_DEADLINE_MS - (Date.now() - _azT0);
  // A comparison period is three fetches; below this there is no point starting.
  const AZ_CMP_MIN_MS = 9000;
  let _azSkipped = [];

  // Prior period
  let priorW=null, priorA=null;
  if (prior && !isAllTime && _azLeft() < AZ_CMP_MIN_MS) {
    _azSkipped.push('prior');
    console.warn(`azPhase prior: SKIPPED at ${Date.now()-_azT0}ms — ${Math.round(_azLeft())}ms left of the ${AZ_RESP_DEADLINE_MS}ms budget`);
  } else if (prior && !isAllTime) {
    const [pw, pp, pc] = await Promise.all([
      fetchAzCampaigns(apiKey, wFrom(prior.from), prior.to),
      fetchPipelineDeals(hsToken, prior.from, prior.to),
      fetchClosedWonDeals(hsToken, prior.from, prior.to),
    ]);
    priorW = pw; priorA = buildAzAttribution(filterActiveBrands(pp), filterActiveBrands(pc));
  }

  // Prior month
  let pmW=null, pmA=null;
  if (priorMonth && !isAllTime && _azLeft() < AZ_CMP_MIN_MS) {
    _azSkipped.push('priorMonth');
    console.warn(`azPhase priorMonth: SKIPPED at ${Date.now()-_azT0}ms — ${Math.round(_azLeft())}ms left of the ${AZ_RESP_DEADLINE_MS}ms budget`);
  } else if (priorMonth && !isAllTime) {
    const [pw, pp, pc] = await Promise.all([
      fetchAzCampaigns(apiKey, wFrom(priorMonth.from), priorMonth.to),
      fetchPipelineDeals(hsToken, priorMonth.from, priorMonth.to),
      fetchClosedWonDeals(hsToken, priorMonth.from, priorMonth.to),
    ]);
    pmW = pw; pmA = buildAzAttribution(filterActiveBrands(pp), filterActiveBrands(pc));
  }

  const attr = buildAzAttribution(filterActiveBrands(pipe), filterActiveBrands(cw));
  const budgets = getBudgetsForMonth(current.from);

  if (_azSkipped.length) console.warn(`azPhase ALL-FETCHES (${windowType}): comparison period(s) skipped for the deadline: ${_azSkipped.join(', ')} — deltas will be blank`);
  console.log(`azPhase ALL-FETCHES (${windowType}): ${Date.now() - _azT0}ms — ${subreqSummary()}`);
  const azResp = buildAzResponse(current, prior, priorMonth, windsor, creatives, priorW, pmW, attr, priorA, pmA, budgets, isAllTime, windowType, fatigue, audiences);
  // Same clinician "won" override the Detailed Dashboard applies, so the Ad KPI
  // CAC card and Channel Performance agree.
  try {
    const _clnWon = await _capped(
      fetchClinicianOnboardedCounts(hsToken, current, prior, priorMonth),
      5000, { cur: null, prior: null, pm: null });
    applyClinicianWon(azResp.channels, _clnWon);
  } catch (e) { console.error('analyzer clinician won count failed:', e && e.message); }
  // Stamp the counts fetched above onto the campaign rows, so the table reads
  // them the same way it reads spend. A campaign the lookup could not resolve is
  // left ABSENT rather than zeroed — a dash and a 0 mean different things.
  // inquiriesUnavailable distinguishes "the lookup could not run at all" (budget
  // exhausted, HubSpot error) from "this campaign genuinely has no inquiries",
  // so the column can say which instead of showing the same dash for both.
  try {
    const cch = azResp.channels && azResp.channels.clinician;
    if (cch && Array.isArray(cch.campaigns) && cch.campaigns.length) {
      cch.inquiriesByCampaign = _clnInq;
      cch.inquiriesUnavailable = (_clnInqNames > 0 && Object.keys(_clnInq).length === 0);
      for (const c of cch.campaigns) {
        const v = _clnInq[c.name];
        if (v != null) { c.inquiries = v; c.cpi = v > 0 ? (c.spend || 0) / v : null; }
      }
    }
  } catch (e) { console.error('clinician inquiries stamp failed:', e && e.message); }
  return azResp;
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
  // HubSpot batch reads cap at 100 inputs per request. This used to send every
  // attendee (and then every associated deal) in ONE request and `return 0` on
  // any failure — so past ~100 attendees the call failed and the webinar
  // funnel's Closed Won silently read 0. Chunk both calls, and on a partial
  // failure keep what did come back and log the shortfall instead of collapsing
  // the whole number to zero.
  const CHUNK = 100;
  const chunk = (arr) => { const out = []; for (let i = 0; i < arr.length; i += CHUNK) out.push(arr.slice(i, i + CHUNK)); return out; };
  const contactDeals = {};
  const allDealIds = new Set();
  let assocFailed = 0;
  for (const batch of chunk(contactIds.map(String))) {
    try {
      const r = await fetch('https://api.hubapi.com/crm/v4/associations/contacts/deals/batch/read', {
        method: 'POST',
        headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ inputs: batch.map(id => ({ id })) }),
      });
      if (!r.ok) { console.error('countAttendeesClosedWon assoc', r.status, (await r.text()).slice(0, 200)); assocFailed += batch.length; continue; }
      const assocData = await r.json();
      for (const row of (assocData.results || [])) {
        const cid = row.from?.id; if (!cid) continue;
        contactDeals[cid] = (row.to || []).map(t => String(t.toObjectId)).filter(Boolean);
        for (const did of contactDeals[cid]) allDealIds.add(did);
      }
    } catch(e) { console.error('countAttendeesClosedWon assoc threw', e.message); assocFailed += batch.length; }
  }
  if (!allDealIds.size) {
    if (assocFailed) console.warn(`countAttendeesClosedWon: no associations resolved and ${assocFailed}/${contactIds.length} attendees failed to look up — Closed Won is UNDERSTATED`);
    return 0;
  }
  const cwDealIds = new Set();
  let dealsFailed = 0;
  for (const batch of chunk(Array.from(allDealIds))) {
    try {
      const r = await fetch('https://api.hubapi.com/crm/v3/objects/deals/batch/read', {
        method: 'POST',
        headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ inputs: batch.map(id => ({ id })), properties: ['dealstage'] }),
      });
      if (!r.ok) { console.error('countAttendeesClosedWon deals', r.status, (await r.text()).slice(0, 200)); dealsFailed += batch.length; continue; }
      const dealData = await r.json();
      for (const d of (dealData.results || [])) {
        if ((d.properties?.dealstage || '') === 'closedwon') cwDealIds.add(String(d.id));
      }
    } catch(e) { console.error('countAttendeesClosedWon deals threw', e.message); dealsFailed += batch.length; }
  }
  let n = 0;
  for (const cid in contactDeals) {
    if (contactDeals[cid].some(did => cwDealIds.has(did))) n++;
  }
  if (assocFailed || dealsFailed) {
    console.warn(`countAttendeesClosedWon: PARTIAL — ${assocFailed} attendee lookups and ${dealsFailed} deal lookups failed; ${n} closed-won counted from ${contactIds.length} attendees`);
  } else {
    console.log(`countAttendeesClosedWon: ${n} closed-won across ${contactIds.length} attendees (${allDealIds.size} associated deals, ${chunk(contactIds).length}+${chunk(Array.from(allDealIds)).length} batches)`);
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
    kvOp();
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
  kvOp();
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
  // Churn analysis (Churn tab). ab_test is Yes/No/Unknown — "Unknown" is a
  // STORED value, distinct from blank, so the two must not be collapsed.
  // web_traffic__measured is a NUMBER; the reported one is a band enum, so the
  // two are not directly comparable without binning one of them.
  'ab_test','web_traffic__measured','monthly_web_traffic__reported_at_time_of_churn',
  // Which products a brand actually turned on. A MULTI-SELECT enum (Badge,
  // Banner, Reviews, CAI, CAS), so HubSpot returns it semicolon-delimited and
  // a brand can hold several. Powers the Churn tab's implemented-products
  // breakdown, which needs it on ACTIVE deals too — the denominator of a
  // per-product churn rate is every brand that implemented it, not just the
  // ones that left.
  'implemented_products',
  // Timestamp the deal first entered closed-won — the true signing date, which
  // PERSISTS after a brand churns (closedate is often blank on churned deals).
  // Used by the BD "Snapshot" chart to place churned brands in the months they
  // were actually active.
  'hs_v2_date_entered_closedwon',
];

// Company owner fields (Acct Mgr / SE Rep / Ops Rep) live on the associated
// company record and DO change (rep reassignments), but the company cache is
// otherwise permanent. Entries are re-fetched once older than this TTL, and
// immediately when the client passes fresh:true (the Refresh button) — so
// owner changes propagate on refresh instead of sticking to the cached value.
const BD_COMPANY_TTL_MS = 4 * 60 * 60 * 1000; // 4h
function _bdCompanyStale(entry, fresh, now){
  if (!entry) return true;              // not cached → fetch
  // fresh is checked BEFORE _unresolved: an explicit refresh is the user saying
  // "re-check this", and a placeholder written during a transient failure is
  // precisely what needs retrying. Bounded by MAX_BATCHES (3000) per wave.
  if (fresh) return true;               // explicit refresh → refetch owners
  if (entry._unresolved) return false;  // known-unresolvable → don't keep retrying
  // Cached before company_segment was requested. Refetched once, in place,
  // rather than by bumping the cache key — a bump discards every resolved NAME
  // too, blanking the Company column on every row until the cache refills.
  if (entry.company_segment === undefined) return true;
  return !entry.t || (now - entry.t) >= BD_COMPANY_TTL_MS;
}
async function fetchBDData(env, fresh) {
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
      kvOp();
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
  // ── Re-verify already-resolved associations on an explicit refresh ────────
  // A resolved entry is a bare company-id string and _assocStillValid() calls
  // those valid forever, so a deal re-pointed at a different company in HubSpot
  // kept its ORIGINAL company on this page indefinitely — the Refresh button
  // re-pulled deals and company owner fields but never the association itself.
  // Refresh now spends whatever budget the missing deals didn't use on
  // re-checking resolved ones, walking a cursor through the set so repeated
  // clicks sweep everything instead of re-checking the same first 1200.
  // Nothing is cleared up front: the batch loop only overwrites on a positive
  // result, and its not-returned branch is already guarded by
  // _assocStillValid(), so a HubSpot hiccup can't blank a company that is
  // currently showing.
  let reverifyFrom = 0, reverifyCount = 0;
  if (fresh && idsToFetch.length < MAX_ASSOC_BATCHES * 100) {
    const resolvedIds = dealIds.filter(id => typeof assocCache[id] === 'string');
    if (resolvedIds.length) {
      if (env.CONTENT_STORE) {
        try {
          kvOp();
          const rawCur = await env.CONTENT_STORE.get('bd_assoc_reverify_cursor');
          const n = rawCur == null ? 0 : parseInt(rawCur, 10);
          if (Number.isFinite(n) && n >= 0) reverifyFrom = n % resolvedIds.length;
        } catch(e) { /* cursor is an optimisation — start from 0 */ }
      }
      const room = (MAX_ASSOC_BATCHES * 100) - idsToFetch.length;
      // Wrap around the end so the tail of the list isn't starved.
      const slice = resolvedIds.slice(reverifyFrom, reverifyFrom + room);
      if (slice.length < room) slice.push(...resolvedIds.slice(0, room - slice.length));
      idsToFetch.push(...slice);
      reverifyCount = slice.length;
      if (env.CONTENT_STORE) {
        try {
          kvOp();
          await env.CONTENT_STORE.put('bd_assoc_reverify_cursor',
            String((reverifyFrom + reverifyCount) % resolvedIds.length));
        } catch(e) { /* best-effort */ }
      }
    }
  }
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
      kvOp();
      await env.CONTENT_STORE.put('bd_assoc_cache_v1', JSON.stringify(assocCache));
    } catch(e) { console.warn('BD assoc cache save failed:', e.message); }
  }
  const assocDeferred = Math.max(0, missingDealIds.length - (idsToFetch.length - reverifyCount));
  if (reverifyCount) console.log(`BD assoc re-verify: ${reverifyCount} resolved associations re-checked from cursor ${reverifyFrom} (fresh=true)`);
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
      kvOp();
      const raw = await env.CONTENT_STORE.get('bd_company_cache_v5');
      if (raw) companyMap = JSON.parse(raw);
    } catch(e) { console.warn('BD cache load failed:', e.message); }
  }
  const _now = Date.now();
  const cachedHits = uniqueCompanyIds.filter(id => companyMap[id]).length;
  // Include cache entries whose owner fields are stale (or every resolved entry
  // when fresh=true) so the client re-looks them up and Acct Mgr / SE Rep /
  // Ops Rep changes surface on refresh.
  const missingCompanyIds = uniqueCompanyIds.filter(id => _bdCompanyStale(companyMap[id], fresh, _now));
  console.log(`BD companies (cache-only): ${cachedHits} cache hits · ${missingCompanyIds.length} missing → client will lookup via /api/bd/lookup-companies (total unique: ${uniqueCompanyIds.length})`);

  const mappedDeals = deals.map(d => {
    const p = d.properties || {};
    const companyId = companyAssociations[d.id];
    const company = companyId ? companyMap[companyId] : null;
    return {
      id: d.id, dealname: p.dealname||'', dealstage: p.dealstage||'',
      brand_status: p.brand_status||'', amount: parseFloat(p.amount)||0,
      closedate: p.closedate||'', createdate: p.createdate||'', entered_closedwon: p.hs_v2_date_entered_closedwon||'',
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
      ab_test: p.ab_test||'',
      // Kept as the raw delimited string; the client splits it, so the
      // delimiter handling lives in one place next to the grouping rules.
      implemented_products: p.implemented_products||'',
      monthly_web_traffic__reported_at_time_of_churn: p.monthly_web_traffic__reported_at_time_of_churn||'',
      // Numeric, so parsed like amount rather than passed through as a string.
      // NOTE a stored 0 and "not measured" are indistinguishable after this, and
      // 24 churned deals do store a literal 0 — almost all of them
      // churn_reason='Never Implemented', i.e. brands that never launched and
      // were never measured. The client must treat 0 as MISSING, or every
      // traffic cut is dragged toward zero by brands that were never live.
      web_traffic__measured: parseFloat(p.web_traffic__measured)||0,
      // Always include the associated companyId (from associations); the
      // companyName comes from the cache if available, else empty (client
      // will fill it in via /api/bd/lookup-companies).
      companyName: company?.name||'', companySegment: company?.company_segment||'',
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
async function lookupBDCompanies(env, companyIds, fresh) {
  const hsToken = env.HUBSPOT_TOKEN;
  if (!Array.isArray(companyIds) || companyIds.length === 0) return { companies: {}, deferred: 0, fetched: 0, cacheHits: 0 };
  // Load cache
  let companyMap = {};
  if (env.CONTENT_STORE) {
    try {
      kvOp();
      const raw = await env.CONTENT_STORE.get('bd_company_cache_v5');
      if (raw) companyMap = JSON.parse(raw);
    } catch(e) { console.warn('BD lookup cache load failed:', e.message); }
  }
  // Filter to IDs not in cache
  const _now = Date.now();
  const cacheHits = companyIds.filter(id => companyMap[id]).length;
  // Re-fetch absent, stale, or (on fresh=true) all requested entries.
  const missingIds = companyIds.filter(id => _bdCompanyStale(companyMap[id], fresh, _now));
  // Cap to 30 batches (3000 companies) per request — plenty of headroom on
  // any plan, leaves room for the KV write at the end.
  const MAX_BATCHES = 30;
  const idsToFetch = missingIds.slice(0, MAX_BATCHES * 100);
  let fetched = 0;
  let attempted = new Set();
  for (let i = 0; i < idsToFetch.length; i += 100) {
    const batch = idsToFetch.slice(i, i + 100);
    try {
      // archived=true so we also pull metadata for archived/deleted companies
      // — without this HubSpot silently drops them from `results`, leaving
      // those deals' Company column permanently blank.
      const coRes = await fetch('https://api.hubapi.com/crm/v3/objects/companies/batch/read?archived=true', {
        method: 'POST',
        headers: { Authorization: `Bearer ${hsToken}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ inputs: batch.map(id => ({ id })), properties: ['name','domain','website','ops_owner','hubspot_owner_id','account_manager','company_segment'] }),
      });
      if (!coRes.ok) {
        const txt = await coRes.text();
        console.error(`BD lookup batch ${i} HTTP ${coRes.status}: ${txt.slice(0,200)}`);
        continue;
      }
      const coData = await coRes.json();
      // Mark attempted only now: a batch that 429'd, 5xx'd or threw must NOT
      // leave its ids looking "tried and unresolvable", or the placeholder loop
      // below freezes all 100 of them permanently.
      batch.forEach(id => attempted.add(id));
      for (const co of (coData.results || [])) {
        const name = co.properties?.name || co.properties?.domain || co.properties?.website || '';
        const ops_owner = co.properties?.ops_owner || '';
        // SE Rep = the company record's hubspot_owner_id
        const se_owner = co.properties?.hubspot_owner_id || '';
        const account_manager = co.properties?.account_manager || '';
        // Company Segment (VIP / Strategic / Enterprise / Mid-Market) lives on the
        // COMPANY, not the deal, so the BD table reads it through this cache
        // rather than paying an association walk per deal.
        const company_segment = co.properties?.company_segment || '';
        companyMap[co.id] = { name, id: co.id, archived: !!co.archived, ops_owner, se_owner, account_manager, company_segment, t: Date.now() };
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
      kvOp();
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
  // Cron. The browser aborts at a fixed timeout and cannot wait for a Year to
  // Date rebuild, so compute those windows here and leave them in KV for the
  // dashboard to pick up. Nothing user-facing depends on this succeeding — a
  // miss just falls through to the live path.
  async scheduled(event, env, ctx) {
    ctx.waitUntil((async () => {
      for (const win of AZ_PRECOMPUTED_WINDOWS) {
        subreqReset();   // each window gets its own budget
        const t0 = Date.now();
        try {
          const result = await processAzRequest(win, null, null, env, null, null);
          // Same health gate as the request path: never warm the cache with a
          // degraded payload, or every reader inherits the gap for the full TTL.
          // Dropped chunks and an exhausted pool mean whole date ranges are
          // missing, which is never cacheable. A couple of soft failures are a
          // small gap, and worth keeping over serving nothing.
          const ok = !SUBREQ.exhausted && !SUBREQ.windsorDropped
                     && SUBREQ.softFail <= AZ_CRON_SOFTFAIL_TOLERANCE;
          if (!ok) { console.warn(`cron warm ${win}: NOT caching — ${subreqSummary()}`); continue; }
          if (SUBREQ.softFail) console.warn(`cron warm ${win}: caching WITH ${SUBREQ.softFail} soft failure(s) — ${subreqSummary()}`);
          if (env.CONTENT_STORE) {
            const blob = JSON.stringify({ t: Date.now(), data: result });
            if (blob.length < 20*1024*1024) {
              kvOp();
              await env.CONTENT_STORE.put(azCacheKey(win, null, null, null, null), blob,
                { expirationTtl: AZ_CACHE_RETAIN_SEC });
              console.log(`cron warm ${win}: cached in ${Date.now()-t0}ms — ${subreqSummary()}`);
            }
          }
        } catch (e) { console.error(`cron warm ${win} failed after ${Date.now()-t0}ms:`, e && e.message); }
      }
    })());
  },

  async fetch(request, env, ctx) {
    subreqReset();   // the pool is per-invocation, so the counters are too
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
      // BUMP API_CACHE_VER whenever the payload SHAPE changes (new channels,
      // renamed fields, changed metric definitions). The key is otherwise
      // deploy-independent, so a new build would keep serving the previous
      // build's cached payload until the TTL lapsed — which reads as "I
      // deployed/refreshed but the data didn't change".
      const _ck = 'apidata_resp_' + API_CACHE_VER + '_' + _win + '_' + (body.from||'') + '_' + (body.to||'') + '_' + (body.vsFrom||'') + '_' + (body.vsTo||'');
      if (!body.fresh && env.CONTENT_STORE) {
        try {
          kvOp();
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
        // The trueCpd test alone was NOT enough, and it let the exact failure the
        // user was reporting get cached and re-served for the full TTL:
        // trueCpd = spend / scheduled, and on a Year to Date load BOTH of those
        // fetch fine — it is the DEAL fetches that get starved. With prior=null
        // on ytd the second clause is vacuously true, so a payload with
        // pipelineDeals 0 / closedWon 0 sailed through as "healthy", got cached,
        // and every refresh re-served the zeros instead of retrying. Requiring an
        // unexhausted subrequest pool is the direct test.
        const _sq = result && result.meta && result.meta.fetchDiag && result.meta.fetchDiag.subrequests;
        const _hs = result && result.meta && result.meta.fetchDiag && result.meta.fetchDiag.hubspot;
        const _poolOk = !_sq || _sq.exhausted !== true;
        const _dealsOk = !_hs || (_hs.pipelineDeals > 0 || _hs.scheduledContacts === 0);
        const _healthy = result && !result.error && _tc && _tc.value != null
          && (!result.priorPeriod || _tc.sameTimePrior != null)
          && _poolOk && _dealsOk;
        if (!_healthy) console.warn(`NOT caching this payload — poolOk=${_poolOk} dealsOk=${_dealsOk} ${subreqSummary()}`);
        if (env.CONTENT_STORE && _healthy) {
          kvOp();
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
    // POST /api/clinician → Clinician Funnel. Cohorts on clinician_meeting_date
    // (the meeting day) within the selected window, and joins each deal to its
    // associated contact so the funnel can be segmented by Clinician Type.
    // POST /api/clinician-funnel → the Clinician Funnel card's own date filter.
    // Its own invocation and its own subrequest pool, so narrowing the funnel
    // never disturbs (or is starved by) the main page fetch.
    if (request.method === 'POST' && url.pathname === '/api/clinician-funnel') {
      let body;
      try { body = await request.json(); } catch { return jr({ error: 'Invalid JSON' }, 400); }
      if (body.password !== env.TEAM_PASSWORD) return jr({ error: 'Unauthorized' }, 401);
      try {
        const _d = (v) => (/^\d{4}-\d{2}-\d{2}$/.test(String(v || '')) ? String(v) : null);
        const from = _d(body.from) || CLINICIAN_HISTORY_FROM;
        const to = _d(body.to) || fmt(todayET());
        if (to < from) return jr({ error: 'Invalid range', detail: 'to is before from' }, 400);
        const token = env.HUBSPOT_TOKEN;
        const deals = await fetchClinicianDeals(token, from, to);
        const contactsByDeal = await fetchClinicianContacts(token, deals.map(d => d.id));
        // Same same-day disambiguation as /api/clinician, or this endpoint would
        // report a different Form-vs-Demo split than the main page for one range.
        const _amb = deals.filter(d => clinicianFormVsDemo(d.properties || {}) === 'Same Day').map(d => d.id);
        const funnel = buildClinicianFunnel(deals, contactsByDeal, await fetchClinicianMeetingStarts(token, _amb));
        // Top-of-funnel + spend for THIS range, so all nine stages describe one span.
        const topFunnel = await fetchClinicianTopFunnel(env.WINDSOR_API_KEY, from, to);
        let clnSpend = null;
        try {
          const wr = await fetchWindsorAds(env.WINDSOR_API_KEY, wFrom(from), to);
          const a = buildClinicianAds(wr, getBudgetsForMonth(from), funnel.all && funnel.all.funnel, 0, funnel.rows || [], from, to);
          clnSpend = (a && typeof a.spend === 'number') ? a.spend : null;
        } catch (e) { console.error('Clinician funnel spend:', e && e.message); }
        return jr({ range: { from, to }, topFunnel, clnSpend, ...funnel,
          meta: { generatedAt: new Date().toISOString(), dealsFetched: deals.length, cohortField: 'createdate' } });
      } catch(err) { console.error('Clinician funnel endpoint error:', err); return jr({ error: 'Internal error', detail: err.message }, 500); }
    }

    if (request.method === 'POST' && url.pathname === '/api/clinician') {
      let body;
      try { body = await request.json(); } catch { return jr({ error: 'Invalid JSON' }, 400); }
      if (body.password !== env.TEAM_PASSWORD) return jr({ error: 'Unauthorized' }, 401);
      try {
        const token = env.HUBSPOT_TOKEN;
        const { current: cur, prior, priorMonth } = computeWindows(body.window || 'mtd', body.from || null, body.to || null, body.vsFrom || null, body.vsTo || null);
        // ONE fetch spanning both scopes: the page window AND the history range.
        // Splitting them into two searches would double the HubSpot page cost
        // against a 50-subrequest ceiling for data we can just slice in memory.
        const _today = fmt(todayET());
        // Widen to cover the comparison windows too. The deals fetch is ONE search
        // over a superset that is then sliced in memory, so prior/last-month deal
        // counts cost no extra HubSpot page — only their ad spend needs new calls.
        // Trend Analysis reaches back a fixed 100 days whatever the page filter
        // says, so the deal fetch has to cover it too — otherwise the section is
        // empty on any window shorter than its own.
        const trendWin = clnTrendWindow(_today);
        let histFrom = CLINICIAN_HISTORY_FROM < cur.from ? CLINICIAN_HISTORY_FROM : cur.from;
        for (const _w of [prior, priorMonth, trendWin]) if (_w && _w.from < histFrom) histFrom = _w.from;
        const histTo = cur.to > _today ? cur.to : _today;
        const deals = await fetchClinicianDeals(token, histFrom, histTo);
        const contactsByDeal = await fetchClinicianContacts(token, deals.map(d => d.id));
        // Slice by the deal's createdate — the same cohort field fetchClinicianDeals
        // filters on, so these subsets are exactly what a narrower fetch would return.
        const _inWin = (d, f, t) => { const k = _clnDayKey((d.properties || {}).createdate); return !!k && k >= f && k <= t; };
        const dealsPage = deals.filter(d => _inWin(d, cur.from, cur.to));
        const dealsHist = deals.filter(d => _inWin(d, CLINICIAN_HISTORY_FROM, histTo));
        // Only the deals whose Form-vs-Demo is a same-day tie need a meeting time.
        const _ambiguous = dealsHist.filter(d => clinicianFormVsDemo(d.properties || {}) === 'Same Day').map(d => d.id);
        const meetingStarts = await fetchClinicianMeetingStarts(token, _ambiguous);
        // Funnel + table are history-scoped; everything else follows the page.
        const funnel = buildClinicianFunnel(dealsHist, contactsByDeal, meetingStarts);
        const funnelPage = buildClinicianFunnel(dealsPage, contactsByDeal, meetingStarts);
        const daily = buildClinicianDaily(dealsPage, contactsByDeal, cur.from, cur.to);
        const composition = buildClinicianComposition(dealsPage, contactsByDeal, cur.from, cur.to);
        // The Verification Funnel is a HISTORY view like the Funnel and the All
        // Clinicians table, so it gets its own pass over dealsHist. `composition`
        // itself stays PAGE-scoped because Clinician Composition and Acquisition
        // read the same object — repointing it would silently move those two
        // sections' windows as well.
        const _compHist = buildClinicianComposition(dealsHist, contactsByDeal, CLINICIAN_HISTORY_FROM, histTo);
        const verification = {
          verifContacts: _compHist.verifContacts,
          credentials: _compHist.credentials,
          photoVerification: _compHist.photoVerification,
          // The Form-vs-Demo mix now comes off the funnel's rsMeet rows
          // (deal-based, demo-flow) so it reconciles with the Before-Demo stat.
          range: { from: CLINICIAN_HISTORY_FROM, to: histTo },
        };
        // KPI comparison windows. Only the three numbers the Key Metrics row needs:
        // ad spend, deals that reached Fully Onboarded, and clinicians added. The
        // deal-side pair is sliced from `deals`, already in hand; only the Windsor
        // spend is a new fetch, and the comparison windows are months, not years.
        const _clnKpiFor = async (w) => {
          if (!w) return null;
          const dw = deals.filter(d => _inWin(d, w.from, w.to));
          const fp = buildClinicianFunnel(dw, contactsByDeal, meetingStarts);
          const dl = buildClinicianDaily(dw, contactsByDeal, w.from, w.to);
          // Acquisition's two populations: unique contacts created in the window,
          // and those of them with a kickoff call booked. CONTACT-based and
          // deduped per person — deliberately NOT the funnel's deal-based stages.
          const cp = buildClinicianComposition(dw, contactsByDeal, w.from, w.to);
          const fullSub = (cp.submission && cp.submission.contacts) || 0;
          let spend = null;
          try {
            const wr = await fetchWindsorAds(env.WINDSOR_API_KEY, wFrom(w.from), w.to);
            const a = buildClinicianAds(wr, getBudgetsForMonth(w.from), fp.all && fp.all.funnel, dl.total || 0, fp.rows || [], w.from, w.to);
            spend = a && typeof a.spend === 'number' ? a.spend : null;
          } catch (e) { console.error('Clinician KPI spend ' + w.from + '..' + w.to + ':', e && e.message); }
          return { spend, fullSub, inquiries: cp.contacts || 0 };
        };
        const [kpiPrior, kpiPriorMonth] = await Promise.all([_clnKpiFor(prior), _clnKpiFor(priorMonth)]);
        const topFunnel = await fetchClinicianTopFunnel(env.WINDSOR_API_KEY, cur.from, cur.to);
        // `deals`, not `dealsPage`: the window is on the BOOKED date, so a deal
        // created before the window but booked inside it belongs to it.
        const virtualNoShow = buildClinicianVirtualNoShow(deals, cur.from, cur.to);
        const ownerMap = await fetchClinicianOwners(token);
        const table = buildClinicianTable(dealsHist, contactsByDeal, ownerMap, meetingStarts);
        // Percent of the window elapsed, so pacing can be compared against spend.
        const _wEnd = Math.min(Date.now(), toMsET(cur.to, true));
        const _wStart = toMsET(cur.from);
        const elapsedPct = _wEnd > _wStart ? Math.min(100, ((_wEnd - _wStart) / (toMsET(cur.to, true) - _wStart)) * 100) : 100;
        let ads = null;
        try {
          // ONE fetch covering both the page window and the (fixed, wider) trend
          // window. buildClinicianAds now filters to the window it is handed, so
          // the page's own figures are unaffected by the wider pull.
          const _adsFrom = trendWin.from < cur.from ? trendWin.from : cur.from;
          const _adsTo = trendWin.to > cur.to ? trendWin.to : cur.to;
          const wRows = await fetchWindsorAds(env.WINDSOR_API_KEY, wFrom(_adsFrom), _adsTo);
          // PAGE-scoped funnel, deliberately: spend is the page window's spend, so
          // CPD / CPS / CAC must divide by that window's clinicians. Handing it the
          // history funnel would divide one month of spend by three months of
          // signups and silently report costs a third of their real value.
          ads = buildClinicianAds(wRows, getBudgetsForMonth(cur.from), funnelPage.all && funnelPage.all.funnel, daily.total || 0, funnelPage.rows || [], cur.from, cur.to);
          ads.elapsedPct = elapsedPct;
          // Same wRows — the Trend Analysis series costs no extra subrequest.
          // Contacts come from the PAGE deals over the page window, the same pair
          // buildClinicianComposition gets, so the series sums to Key Metrics.
          // `deals` and the TREND window, not dealsPage and the page window: this
          // section is independent of the page's date filter by design.
          ads.trend = buildClinicianTrend(deals, contactsByDeal, wRows, trendWin.from, trendWin.to);
          ads.trendRange = trendWin;
        } catch (e) { console.error('Clinician ads fetch:', e.message); ads = { error: e.message, elapsedPct }; }
        return jr({
          period: cur,
          // Comparison windows for the Key Metrics row. null when the selected
          // window has no counterpart (computeWindows gives ytd/allTime no prior).
          kpiPrior, kpiPriorMonth,
          // Nine-stage funnel + the Virtual No Show Rate that replaced the
          // No Call Scheduled Yet stat. funnel9 is attached after `ads` resolves
          // so cost per stage divides by the same window's spend.
          virtualNoShow,
          topFunnel,
          funnelStages: CLN_FUNNEL9,
          clnSpend: (ads && !ads.error && typeof ads.spend === 'number') ? ads.spend : null,
          priorPeriod: prior || null, priorMonthPeriod: priorMonth || null,
          // PAGE-scoped: the funnel section's own date filter defaults to the page
          // window, so shipping the history funnel here made the section ignore it
          // until Apply was pressed. The All Clinicians table keeps its own
          // history-scoped `table` and `historyRange`.
          ...funnelPage,
          // The range the funnel and table actually cover, so the UI can label
          // them honestly instead of implying they follow the page filter.
          historyRange: { from: CLINICIAN_HISTORY_FROM, to: histTo },
          daily,
          composition,
          verification,
          table,
          ads,
          meta: {
            generatedAt: new Date().toISOString(),
            dealsFetched: deals.length,
            dealsInPageWindow: dealsPage.length,
            dealsInHistory: dealsHist.length,
            cohortField: 'createdate',
            historyFrom: CLINICIAN_HISTORY_FROM,
            pipeline: CLINICIAN_PIPELINE_ID,
          },
        });
      } catch(err) { console.error('Clinician endpoint error:', err); return jr({ error: 'Internal error', detail: err.message }, 500); }
    }

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
      // This endpoint had NO cache, so every load recomputed a fetch chain that
      // measured 70s+ on a Year to Date window — past the dashboard's 75s abort.
      // Ad data barely moves intraday, so a short TTL turns every load after the
      // first into an instant one. Keyed on the window + custom dates, and on
      // API_CACHE_VER so a payload-shape change cannot be served from the
      // previous build's cache.
      const _azWin = body.window || 'mtd';
      const _azCk = azCacheKey(_azWin, body.from, body.to, body.vsFrom, body.vsTo);
      // An expired-but-present entry is held as a FALLBACK rather than
      // discarded. A Year to Date rebuild cannot finish inside the browser's
      // abort, so when one fails the choice is between day-old ad data and no
      // ad data at all — and ad data barely moves intraday.
      let _azStale = null;
      if (!body.fresh && env.CONTENT_STORE) {
        try {
          kvOp();
          const raw = await env.CONTENT_STORE.get(_azCk);
          if (raw) {
            const pcache = JSON.parse(raw);
            if (pcache && typeof pcache.t === 'number' && pcache.data) {
              const age = Date.now() - pcache.t;
              if (age < azCacheTtlMs(_azWin)) {
                console.log(`analyzer cache HIT (${_azWin}, age ${Math.round(age/1000)}s)`);
                return jr(pcache.data);
              }
              _azStale = pcache;
            }
          }
        } catch(e) { /* fall through to live compute */ }
      }
      // Serving stale is a REPORTED outcome, never a silent one: the payload
      // says so and the dashboard labels it, so nobody reads day-old spend as
      // current without being told.
      const _azServeStale = (why) => {
        const ageSec = Math.round((Date.now() - _azStale.t) / 1000);
        console.warn(`analyzer serving STALE (${_azWin}, age ${ageSec}s) — ${why}`);
        return jr(Object.assign({}, _azStale.data, { _stale: true, _staleAgeSec: ageSec }));
      };
      // EXPENSIVE WINDOWS NEVER REBUILD ON THE REQUEST PATH when a previous
      // payload exists. These measured past the browser's abort, so computing
      // synchronously does not produce a slow answer — it produces no answer,
      // and the abort kills the run before it can cache, so the next request is
      // in exactly the same position. Returning last-good immediately and
      // refreshing behind the response breaks that loop: the reader gets data
      // now, and the cache is fresh for the next reader.
      if (_azStale && AZ_PRECOMPUTED_WINDOWS.includes(_azWin)) {
        if (ctx && ctx.waitUntil && !AZ_REFRESHING.has(_azCk)) {
          AZ_REFRESHING.add(_azCk);
          ctx.waitUntil((async () => {
            try {
              subreqReset();
              const fresh = await processAzRequest(_azWin, body.from||null, body.to||null, env, body.vsFrom||null, body.vsTo||null);
              // Same health gate as the cron: never warm the cache with a
              // payload that is missing rows, or every reader inherits the gap.
              const ok = !SUBREQ.exhausted && !SUBREQ.windsorDropped && SUBREQ.softFail <= AZ_CRON_SOFTFAIL_TOLERANCE;
              if (!ok) { console.warn(`bg refresh ${_azWin}: NOT caching — ${subreqSummary()}`); return; }
              if (env.CONTENT_STORE) {
                const blob = JSON.stringify({ t: Date.now(), data: fresh });
                if (blob.length < 20*1024*1024) {
                  kvOp();
                  await env.CONTENT_STORE.put(_azCk, blob, { expirationTtl: AZ_CACHE_RETAIN_SEC });
                  console.log(`bg refresh ${_azWin}: cached — ${subreqSummary()}`);
                }
              }
            } catch (e) { console.error(`bg refresh ${_azWin} failed:`, e && e.message); }
            finally { AZ_REFRESHING.delete(_azCk); }
          })());
        }
        return _azServeStale('rebuilding behind the response');
      }
      try {
        const result = await processAzRequest(body.window||'mtd', body.from||null, body.to||null, env, body.vsFrom||null, body.vsTo||null);
        // Never cache a payload that is MISSING DATE RANGES: an exhausted pool
        // or dropped Windsor chunks mean whole stretches of the window are
        // absent, and caching that serves the gap for the full TTL.
        //
        // A SOFT failure is different, and this used to treat them the same.
        // One connector returning an error is a small, known hole — and when it
        // is PERSISTENT (Windsor has been answering 400 for LinkedIn on every
        // run) strictness means the request path can never cache anything at
        // all. Every load then recomputes a chain that measured past the
        // browser's abort, so nothing is cached, so the next load recomputes:
        // the exact loop behind "no response after 75s". The cron already
        // tolerates a few soft failures for this reason; the request path now
        // uses the same tolerance rather than a stricter one that cannot be met.
        //
        // NOTE: SUBREQ is module-global and subreqReset() runs per request, so a
        // concurrent request can zero these mid-flight and make a degraded run look
        // clean. A per-request counter is the real fix; until then the short TTL
        // bounds how long any missed degradation can persist.
        const _azOk = !SUBREQ.exhausted && !SUBREQ.windsorDropped
                      && SUBREQ.softFail <= AZ_CRON_SOFTFAIL_TOLERANCE;
        if (!_azOk) console.warn(`NOT caching analyzer payload (${_azWin}) — ${subreqSummary()}`);
        else if (SUBREQ.softFail) console.warn(`caching analyzer payload (${_azWin}) WITH ${SUBREQ.softFail} soft failure(s) — ${subreqSummary()}`);
        // A degraded run can come back MISSING WHOLE CHANNELS, and the dashboard
        // renders that as "No <channel> data available for this window" — which
        // reads as "there was no spend", not as "the fetch fell short". When the
        // last good payload covers a channel this one lost, it is the more
        // truthful answer even though it is older.
        if (_azStale) {
          const _chOf = (x) => Object.keys((x && x.channels) || {});
          const _lost = _chOf(_azStale.data).filter(c => _chOf(result).indexOf(c) < 0);
          if (_lost.length) return _azServeStale(`live run lost channel(s): ${_lost.join(', ')}`);
        }
        if (env.CONTENT_STORE && _azOk) {
          kvOp();
          try {
            const blob = JSON.stringify({ t: Date.now(), data: result });
            if (blob.length < 20*1024*1024) await env.CONTENT_STORE.put(_azCk, blob, { expirationTtl: AZ_CACHE_RETAIN_SEC });
          } catch(e) { /* best-effort */ }
        }
        return jr(result);
      } catch(err) {
        console.error('Analyzer error:', err);
        // Last good payload beats an error banner. Only when there is none does
        // this report the failure.
        if (_azStale) return _azServeStale(err && err.message);
        return jr({ error: 'Internal error', detail: err.message }, 500);
      }
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
          kvOp();
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
        kvOp();
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
          kvOp();
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
        kvOp();
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
            kvOp();
            const raw = await env.CONTENT_STORE.get('pq_results');
            if (raw) existing = JSON.parse(raw);
          } catch(e) { /* read failed, treat as empty */ }
          final = { ...existing, ...incoming };
        }
        kvOp();
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
          kvOp();
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
              if (!r.ok) { azDrain(r); break; }
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
              if (!r.ok) { azDrain(r); break; }
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
        const result = await fetchBDData(env, body.fresh === true);
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
        const result = await lookupBDCompanies(env, body.companyIds || [], body.fresh === true);
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
