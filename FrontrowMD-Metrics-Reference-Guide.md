# FrontrowMD Dashboard — Complete Metrics Reference Guide

## How to Use This Guide

This document explains how every metric on the FrontrowMD marketing dashboard is calculated, including the exact API queries, data sources, field names, and formulas. Use it to recreate any metric in a new dashboard, script, or reporting tool.

---

## Critical Setup Requirements

### Timezone Handling

```javascript
process.env.TZ = 'America/New_York'; // MUST be first line after imports
```

All date boundaries use Eastern Time. **Never** append `Z` to date strings — the `Z` suffix forces UTC parsing and causes evening bookings to land on the wrong day.

```javascript
// ✅ CORRECT — Eastern Time boundaries
new Date('2026-03-09T00:00:00.000').getTime()

// ❌ WRONG — UTC boundaries (misses evening ET events)
new Date('2026-03-09T00:00:00.000Z').getTime()
```

### Date Helper

```javascript
function toDateStr(d) {
  return `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}`;
}
```

Never use `toISOString().slice(0,10)` — it returns UTC dates regardless of the TZ setting.

---

## Data Sources

| Source | Base URL | Auth | What It Provides |
|--------|----------|------|------------------|
| **Windsor.ai** | `https://connectors.windsor.ai/all` | `api_key` query param | Ad spend, clicks, impressions, demos (pixel events), GA4 website data |
| **HubSpot CRM** | `https://api.hubapi.com/crm/v3/objects/` | `Bearer` token header | Demo bookings, pipeline stages, deal outcomes, UTM attribution, MRR |
| **HubSpot Owners** | `https://api.hubapi.com/crm/v3/owners` | `Bearer` token header | Deal owner names for breakdown tables |

---

## Time Windows

| Window | Current Period | Prior Period (for delta comparison) |
|--------|---------------|-------------------------------------|
| **Last 7 Days** | 7 days ending yesterday | Prior 7 days |
| **Month to Date** | 1st of month → yesterday | Same # of days in prior month (e.g., Mar 1-11 vs Feb 1-11) |
| **Last Month** | Full prior calendar month | Month before that |
| **Year to Date** | Jan 1 → yesterday | No prior period (deltas suppressed) |

---

## Section 1: Windsor.ai — Ad Platform Metrics

### How to Fetch

```javascript
const url = `https://connectors.windsor.ai/all?api_key=${KEY}&date_from=${from}&date_to=${to}&fields=${fields}&page_size=5000`;
```

**Important:** Windsor data is capped at yesterday — today's data is always incomplete.

**Important:** For TikTok, Windsor collapses multi-day rows. Always fetch day-by-day in batches of 5.

### Fields Used

**Main ad data fetch:**
`date, datasource, campaign_name, spend, clicks, impressions, ctr, conversions, externalwebsiteconversions, conversions_submit_application_total, all_conversions`

**GA4 fetch (add `&connectors=googleanalytics4`):**
`datasource, users, sessions, conversions_click_schedule_demo_button, conversions_hubspot_meeting_booked`

**GA4 source breakdown:**
`datasource, sessionDefaultChannelGrouping, session_source, conversions_hubspot_meeting_booked, conversions_click_schedule_demo_button, users`

### Channel → Datasource Mapping

| Channel | Windsor `datasource` value | Demo Field |
|---------|---------------------------|------------|
| Meta | `facebook` (also matches meta, fb, ig, instagram) | `conversions_submit_application_total` |
| LinkedIn | `linkedin` | `externalwebsiteconversions` (filtered by `conversion_name` containing "demo request") |
| TikTok | `tiktok` | `conversions` |
| Google Ads | `google_ads` (excluding YouTube campaigns) | `conversions` (ceiling applied) |
| YouTube | `google_ads` (campaigns containing "yt" or "youtube") | `conversions` (ceiling applied) |

### Per-Channel Metrics from Windsor

| Metric | Source | Formula |
|--------|--------|---------|
| **Spend** | `row.spend` | Sum per channel |
| **Clicks** | `row.clicks` | Sum per channel |
| **Impressions** | `row.impressions` | Sum per channel |
| **Ad Demos** (Windsor-tracked) | See demo field column above | Sum per channel |
| **CTR** (Meta only) | `row.ctr` | Average across all rows with non-null CTR |

### LinkedIn Demo Count Override

The main Windsor fetch returns all `externalwebsiteconversions` for LinkedIn, which includes pipeline events (SQL, Opportunity). A separate fetch with `conversion_name` filters to only "demo request" conversions:

```javascript
const liFields = 'date,datasource,conversion_name,externalwebsiteconversions';
// Filter: only rows where conversion_name includes 'demo request'
```

### Aggregated Metrics (All Channels)

| Metric | Formula |
|--------|---------|
| **Total Ad Spend** | `meta.spend + linkedin.spend + google.spend + tiktok.spend + youtube.spend` |
| **Total Ad Demos** | `meta.demos + linkedin.demos + google.demos + tiktok.demos + youtube.demos` |
| **Blended CPD** | `Total Ad Spend ÷ Total Ad Demos` |

---

## Section 2: GA4 via Windsor — Website Analytics

### Metrics

| Metric | Windsor Field | Notes |
|--------|--------------|-------|
| **Website Visitors** | `users` | Filter: `datasource === 'googleanalytics4'` |
| **Demo Button Clicks** | `conversions_click_schedule_demo_button` | GA4 custom event |
| **Demos Booked (GA4)** | `conversions_hubspot_meeting_booked` | GA4 conversion event (NOT used for KPI — HubSpot is source of truth) |

### Website Funnel Conversion Rates

| Rate | Formula |
|------|---------|
| **Click Rate** (visitor → click) | `Demo Button Clicks ÷ Website Visitors × 100` |
| **Book Rate** (click → booked) | `Demos Booked ÷ Demo Button Clicks × 100` |
| **Website CVR** | `Demos Booked ÷ Website Visitors × 100` |

**Note:** The "Demos Booked" in the website funnel uses the HubSpot-sourced `demosBooked` count (see Section 3), not the GA4 conversion event.

---

## Section 3: HubSpot — Demo Bookings (Demos Booked KPI)

### What "Demos Booked" Means

"Demos Booked" counts how many demos were **scheduled** during the time period — i.e., contacts who were **created** during the window and have a demo appointment set.

### The Query

**Object:** `contacts`

**Filters (AND):**
1. `createdate >= startMs` (millisecond timestamp)
2. `createdate <= endMs` (millisecond timestamp)
3. `date_demo_booked HAS_PROPERTY`

**Properties requested:** `createdate`, `date_demo_booked`

```javascript
const results = await hsSearch('contacts', {
  filterGroups: [{ filters: [
    { propertyName: 'createdate', operator: 'GTE', value: String(startMs) },
    { propertyName: 'createdate', operator: 'LTE', value: String(endMs) },
    { propertyName: 'date_demo_booked', operator: 'HAS_PROPERTY' },
  ]}],
  properties: ['createdate', 'date_demo_booked']
});
```

### Understanding the Two Date Fields

| Field | Meaning | Example |
|-------|---------|---------|
| `createdate` | When the prospect **booked** the demo (clicked schedule) | Monday March 9 |
| `date_demo_booked` | When the demo **meeting happens** (appointment date) | Friday March 13 |

**Demos Booked** uses `createdate` — it answers "how many people booked a demo today?"

### Demos Booked Per Day

Group scheduled contacts by the calendar date of their `createdate`:

```javascript
const cd = new Date(contact.properties.createdate);
const dayStr = `${cd.getFullYear()}-${String(cd.getMonth()+1).padStart(2,'0')}-${String(cd.getDate()).padStart(2,'0')}`;
```

### Derived Metric: Demos % Paid Source

```
Demos % Paid Source = (Total Ad Demos from Windsor) ÷ (Demos Booked from HubSpot) × 100
```

This shows what percentage of booked demos are attributable to paid advertising.

---

## Section 4: HubSpot — Deal Pipeline Metrics

### The Query

**Object:** `deals`

**Filter Groups (OR — two groups):**

Group 1: Deals with `date_demo_booked` in the time window
Group 2: No Show/No Showed deals by `hs_createdate` (these often lack `date_demo_booked`)

```javascript
const deals = await hsSearch('deals', {
  filterGroups: [
    { filters: [
      { propertyName: 'date_demo_booked', operator: 'GTE', value: gteMs },
      { propertyName: 'date_demo_booked', operator: 'LTE', value: lteMs },
    ]},
    { filters: [
      { propertyName: 'demo_given__status', operator: 'IN', values: ['No Show', 'No Showed'] },
      { propertyName: 'hs_createdate', operator: 'GTE', value: String(gteMs) },
      { propertyName: 'hs_createdate', operator: 'LTE', value: String(lteMs) },
    ]},
  ],
  properties: ['date_demo_booked', 'demo_given_date', 'demo_given__status',
               'dealstage', 'amount', 'closedate', 'hs_createdate',
               'hubspot_owner_id', 'utm_source', 'utm_medium']
});
```

### `demo_given__status` Values → Metric Mapping

| Exact HubSpot Value | Dashboard Category | Counts As |
|---------------------|-------------------|-----------|
| `Demo Given` | Qualified Demo Given | demosHappened ✓, demoGivenCount ✓ |
| `Demo Given at Rescheduled time` | Qualified Demo Given | demosHappened ✓, demoGivenCount ✓ |
| `Demo Given, Qualified Company, too early` | Too Early | demosHappened ✓, tooEarly ✓ |
| `Not Qualified after the demo` | Not Qualified | demosHappened ✓, notQualAfterDemo ✓ |
| `Disqualified, Meeting Cancelled` | Disqualified Before Demo | disqualifiedBeforeDemo ✓ |
| `No Show` | Rescheduled | rescheduled ✓ |
| `No Showed` | Canceled | canceled ✓ |
| (empty/other) | Blank Status | blankStatus ✓ |

### Pipeline Metrics

| Metric | Formula |
|--------|---------|
| **Demos to Occur** | Total count of deals in window |
| **Demos Happened** | Deals where status = Demo Given, Demo Given at Rescheduled, Too Early, or Not Qual After |
| **Demo Show Rate** | `Demos Happened ÷ Demos to Occur × 100` |
| **Qual. Demo Given %** | `(Demos Happened − Too Early − Not Qual After) ÷ Demos to Occur × 100` |

---

## Section 5: HubSpot — Closed Won / MRR

### The Query (Separate from Pipeline)

Closed won deals are queried by **`closedate`** (not `date_demo_booked`), so a deal demoed in January but closed in March appears in March's MRR.

```javascript
const closedWon = await hsSearch('deals', {
  filterGroups: [{ filters: [
    { propertyName: 'dealstage', operator: 'EQ', value: 'closedwon' },
    { propertyName: 'closedate', operator: 'GTE', value: gteMs },
    { propertyName: 'closedate', operator: 'LTE', value: lteMs },
  ]}],
  properties: ['amount', 'closedate', 'hs_createdate', 'utm_source', 'utm_medium', 'hubspot_owner_id']
});
```

### Metrics

| Metric | Formula |
|--------|---------|
| **Closed Deals** | `closedWon.length` |
| **New MRR** | `Sum of closedWon deal.properties.amount` |
| **ARR** | `New MRR × 12` |
| **ROAS** | `New MRR ÷ Total Ad Spend` |
| **Avg Deal Cycle** | Average of `(closedate − hs_createdate)` in days, across closed-won deals |

---

## Section 6: UTM Channel Attribution

### How It Works

Each HubSpot deal has UTM properties from the original ad click. These map the deal back to a paid channel.

### UTM → Channel Mapping

```javascript
function mapChannel(utm_source, utm_medium) {
  const src = (utm_source || '').toLowerCase().trim();
  const med = (utm_medium || '').toLowerCase().trim();
  if (['fb', 'ig', 'facebook', 'instagram', 'meta'].includes(src)) return 'meta';
  if (src === 'google' && (med === 'cpc' || med === 'paid')) return 'google';
  if (src === 'linkedin') return 'linkedin';
  if (['tiktok', 'tik_tok', 'tt', 'tiktok_ads'].includes(src)) return 'tiktok';
  if (src === 'youtube') return 'youtube';
  return null; // unattributed
}
```

### Per-Channel Attribution Metrics (from deals)

For each channel, count deals by `demo_given__status`:

| Metric | Formula |
|--------|---------|
| **Qualified** (per channel) | Deals with status = `Demo Given` or `Demo Given at Rescheduled time` mapped to that channel |
| **Not Attributed** (per channel) | `Ad Demos (Windsor) − Qualified (HubSpot UTM)` |
| **CPQD** (per channel) | `Channel Spend ÷ Channel Qualified Demos` |
| **Closed Won** (per channel) | Closed-won deals mapped to that channel via UTM |
| **MRR** (per channel) | Sum of `amount` for closed-won deals mapped to that channel |
| **ROAS** (per channel) | `Channel MRR ÷ Channel Spend` |

### Total CPQD (Stat Card)

Uses UTM-attributed qualified demos across all channels:
```
Total CPQD = Total Ad Spend ÷ Total UTM-Attributed Qualified Demos
```

---

## Section 7: Demo Quality by Deal Owner

### Owner Name Lookup

```javascript
// GET https://api.hubapi.com/crm/v3/owners?limit=100
// Returns { id, firstName, lastName, email }
// Build map: { ownerId: "First Last" }
```

### Per-Owner Breakdown

Group all deals in the window by `hubspot_owner_id`, then classify each by `demo_given__status` using the same mapping as Section 4.

| Per-Owner Metric | Formula |
|-----------------|---------|
| **Qual %** | `demoGiven ÷ (demoGiven + tooEarly + notQual + disqBefore + rescheduled + canceled + blank) × 100` |

### Closed Won by Owner

Separate grouping from the closed-won query (by `closedate`), mapped by `hubspot_owner_id`. Shows deals that actually closed in this period per rep.

---

## Section 8: Demo Quality by Day of Week

Groups deals by the day of week of their `date_demo_booked`:

```javascript
const dayIdx = new Date(deal.properties.date_demo_booked + 'T12:00:00').getDay(); // 0=Sun
```

Classifies each deal using the same `demo_given__status` mapping. Dashboard shows Monday–Friday only (weekends excluded).

---

## Section 9: Demo Cohort Performance

### The Query

Fetches deals with `date_demo_booked` in the last 3 completed months + current month.

```javascript
const cohortStart = new Date(now.getFullYear(), now.getMonth() - 3, 1);
const cohortEnd = new Date(now.getFullYear(), now.getMonth() + 1, 0);
```

### How Cohorts Are Built

Each deal is bucketed by the month of its `date_demo_booked`. Within each monthly bucket:

| Status Classification | Cohort Column |
|----------------------|---------------|
| `Demo Given` or `Demo Given at Rescheduled time` | `demosGiven` (qualified) → then check `dealstage` for closedwon/closedlost/still open |
| `Demo Given, Qualified Company, too early` | `tooEarly` (separate column, NOT counted in demosGiven) |
| `Not Qualified after the demo` | `notQualified` (separate column, NOT counted in demosGiven) |

### Cohort Metrics

| Metric | Formula |
|--------|---------|
| **Close Rate** | `closedWon ÷ demosGiven × 100` |
| **Avg Cycle** | Average `(closedate − hs_createdate)` in days for closed-won deals in that cohort |
| **Still Open** | Qualified demos where `dealstage` is not closedwon or closedlost |
| **MRR** | Sum of `amount` for closed-won deals in that cohort |

---

## Section 10: Website Funnel (Conversion Funnel)

Uses a mix of GA4 data and HubSpot pipeline data:

| Funnel Step | Data Source | Field |
|-------------|------------|-------|
| Website Visitors | GA4 via Windsor | `users` |
| Clicked Demo Button | GA4 via Windsor | `conversions_click_schedule_demo_button` |
| Demos Booked | HubSpot contacts | `scheduledInWindow.length` (createdate-based) |
| Demos to Occur | HubSpot deals | `deals.length` (date_demo_booked-based) |
| Demo Happened | HubSpot deals | `demosHappened` count |
| Closed Won | HubSpot deals | `closedDeals` count (by closedate) |

---

## Section 11: Budget Pacing

### Configuration (Hardcoded in Template)

```javascript
const BUDGET_BY_MONTH = {
  '2026-01': { meta: 45000, linkedin: 30000, google: 5000, tiktok: 5000, youtube: 5000 },
  '2026-02': { meta: 70000, linkedin: 30000, google: 5000, tiktok: 10000, youtube: 5000 },
  '2026-03': { meta: 90000, linkedin: 15000, google: 10000, tiktok: 30000, youtube: 0 },
};
```

### Pacing Calculation

```javascript
const dayOfMonth = today.getDate() - 1; // completed days
const pacingPct = (dayOfMonth / daysInMonth) * 100;
// Each channel bar: actual spend vs monthly budget, with pacing marker
```

Only shown on MTD and Last Month tabs.

---

## Section 12: Intelligence Engine

Runs on MTD data vs prior month. Analyzes 9 dimensions and categorizes findings as Alerts, Weaknesses, Opportunities, or Wins:

1. **Channel CPD Efficiency Gaps** — Compares CPD across channels, identifies cheapest vs most expensive
2. **Spend Concentration Risk** — Flags if any single channel exceeds 60% of total budget
3. **Funnel Leakage** — Analyzes visitor → demo conversion drop-off
4. **Disqualification Rates** — Flags elevated pre-demo cancellations
5. **Revenue Velocity** — Tracks pipeline-to-close speed
6. **Meta Creative Health** — Benchmarks Meta CTR (warn below 1.5%, alert below 1%)
7. **Pipeline Volume Trend** — Compares current vs prior period demo volume
8. **YouTube Spend Efficiency** — Flags YouTube spend with zero demos
9. **Overall CPD Trend** — Compares blended CPD vs prior period

---

## HubSpot Property Reference

### Contact Properties Used

| Property | Type | Description |
|----------|------|-------------|
| `createdate` | datetime | When the contact record was created |
| `date_demo_booked` | DATE | The appointment date of the demo meeting |

### Deal Properties Used

| Property | Type | Description |
|----------|------|-------------|
| `date_demo_booked` | DATE | Demo appointment date (stored as midnight UTC ms) |
| `demo_given__status` | enumeration | Demo outcome status (see exact values in Section 4) |
| `dealstage` | enumeration | Pipeline stage (`closedwon`, `closedlost`, etc.) |
| `amount` | number | Deal value (MRR) |
| `closedate` | datetime | When the deal was closed |
| `hs_createdate` | datetime | When the deal was created |
| `hubspot_owner_id` | number | Owner/rep assigned to the deal |
| `utm_source` | string | UTM source from original ad click |
| `utm_medium` | string | UTM medium from original ad click |

### Deal Stage Values

- `closedwon` (lowercase)
- `closedlost` (lowercase)

---

## Replication Checklist

To recreate these metrics in a new script:

1. Set `process.env.TZ = 'America/New_York'` before any date operations
2. Compute date boundaries WITHOUT the `Z` suffix
3. Fetch Windsor data day-by-day for TikTok compatibility
4. Fetch HubSpot contacts by `createdate` + `date_demo_booked HAS_PROPERTY` for Demos Booked
5. Fetch HubSpot deals by `date_demo_booked` (with fallback to `hs_createdate` for No Show deals)
6. Fetch HubSpot closed-won deals by `closedate` separately for MRR/revenue metrics
7. Use the exact `demo_given__status` string values (case-sensitive, spaces matter)
8. Map UTM parameters using the `mapChannel()` function for per-channel attribution
9. Fetch HubSpot owners API for deal owner name resolution
