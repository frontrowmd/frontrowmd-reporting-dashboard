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
wrangler secret put WINDSOR_API_KEY
wrangler secret put HUBSPOT_TOKEN

# 3. Deploy
wrangler deploy
```

After deploy, Wrangler will output the Worker URL, e.g.:
`https://frontrowmd-dashboard-api.<your-subdomain>.workers.dev`

## Sign-in (Cloudflare Access)

There is no team password (removed 2026-11-10). The workers.dev hostname sits
behind Cloudflare Zero Trust → Access (team domain `frontrowmd.cloudflareaccess.com`,
Google login, Allow = emails ending in `@thefrontrowhealth.com`), with a Bypass
app on path `api/slack/events` — Slack's request signature is that route's auth.

The worker re-checks the Access token itself (`accessIdentity` in worker.js)
because Access does not cover version preview URLs, and it refuses data requests
that do not come from the dashboard's own pages (cross-site request forgery guard).
So data routes only answer a signed-in browser on the production hostname:
`curl`, `wrangler dev` and opening the HTML from `file://` all get 401. A new
Access app (e.g. a custom domain) has its own AUD tag — add it as the
`ACCESS_AUD` var (comma-separated).

## Request Format

```json
{
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
| WINDSOR_API_KEY | Windsor.ai → Settings → API |
| HUBSPOT_TOKEN | HubSpot → Settings → Integrations → Private Apps |

## Updating

Edit `worker.js`, then run `wrangler deploy` again. Changes go live in seconds.

## Updating Monthly Budgets

Edit the `BUDGET_BY_MONTH` object near the top of `worker.js`. Add new month entries as needed.

## Updating Quality Funnel Categories

Edit the `FUNNEL_CATEGORIES` mapping in `worker.js` if HubSpot `demo_given__status` values change.
