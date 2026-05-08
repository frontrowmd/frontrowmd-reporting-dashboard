# FrontrowMD Dashboard API — Setup Guide

## What This Is

A Cloudflare Worker that serves as the API backend for the live marketing dashboard. It holds all API keys server-side and proxies Windsor.ai + HubSpot data to the browser-based dashboard.

**Endpoint:** `POST /api/data`

## Prerequisites

- Cloudflare account (free plan works for testing; Workers Paid plan recommended for production — $5/month for 30s execution time)
- [Wrangler CLI](https://developers.cloudflare.com/workers/wrangler/install-and-update/) installed: `npm install -g wrangler`

## Deploy

```bash
# 1. Log in to Cloudflare
wrangler login

# 2. Set secrets (you'll be prompted to paste each value)
wrangler secret put TEAM_PASSWORD
wrangler secret put WINDSOR_API_KEY
wrangler secret put HUBSPOT_TOKEN

# 3. Deploy
wrangler deploy
```

After deploy, Wrangler will output the Worker URL, e.g.:
`https://frontrowmd-dashboard-api.<your-subdomain>.workers.dev`

## Test with curl

```bash
curl -X POST https://frontrowmd-dashboard-api.<your-subdomain>.workers.dev/api/data \
  -H "Content-Type: application/json" \
  -d '{"password":"your-team-password","window":"7d"}'
```

## Request Format

```json
{
  "password": "team-password",
  "window": "7d",          // "7d" | "mtd" | "lastMonth" | "custom" | "allTime"
  "from": "2026-02-01",    // only needed for "custom"
  "to": "2026-02-28"       // only needed for "custom"
}
```

## Response Structure

The Worker returns a single JSON object with all dashboard sections pre-computed:

```
{
  period: { from, to, label },
  priorPeriod: { from, to } | null,
  priorMonthPeriod: { from, to } | null,
  isAllTime: boolean,
  executiveSummary: { totalDemosScheduled, cpdMetaTikTok, qualifiedShowRate, cpqdMetaTikTok },
  webPerformance: { visitors, cvr },
  adSpend: { total, channels, budgets, totalBudget },
  demoTracking: { totalScheduled, qualifiedOccurred, blanks, dailyChart },
  costPerDemo: { total, channels },
  qualityFunnel: { overall, priorOverall, byRep, categories, labels, colors },
  costPerQualifiedDemo: { total, channels },
  mrrArr: { mrr, arr, dealCount },
  signUpRate: { lastMonth: {...}, twoMonthsAgo: {...} },
  creativePerformance: [...],
  campaignPerformance: [...],
  ownerMap: { ownerId: "Name", ... },
  meta: { generatedAt, funnelDataAvailableFrom, adDataAvailableFrom }
}
```

Each "tile" object follows this format:
```
{
  value: 42,
  definition: "Human-readable calculation definition",
  sameTimePrior: { value: 38, nominalDelta: 4, percentDelta: 10.53 },
  lastMonth: { value: 156 }
}
```

## Secrets Reference

| Secret | Source |
|--------|--------|
| TEAM_PASSWORD | Your chosen dashboard access password |
| WINDSOR_API_KEY | Windsor.ai → Settings → API |
| HUBSPOT_TOKEN | HubSpot → Settings → Integrations → Private Apps |

## Updating

Edit `worker.js`, then run `wrangler deploy` again. Changes go live in seconds.

## Updating Monthly Budgets

Edit the `BUDGET_BY_MONTH` object near the top of `worker.js`. Add new month entries as needed.

## Updating Quality Funnel Categories

Edit the `FUNNEL_CATEGORIES` mapping in `worker.js` if HubSpot `demo_given__status` values change.
