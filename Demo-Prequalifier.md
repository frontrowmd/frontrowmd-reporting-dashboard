---
name: demo-prequalification
description: "Prequalify inbound demo leads for FrontrowMD by evaluating whether a brand is a good fit for the Clinician Certified badge program. Use this skill whenever the user asks to qualify, prequalify, score, vet, triage, or evaluate demo leads, inbound prospects, or booked meetings — especially when a CSV or list of companies/URLs is provided. Also trigger when the user mentions 'qualified Y/N', 'good fit', 'CC badge', or wants to determine if a brand should get a demo. This skill encodes FrontrowMD's specific qualification criteria and scoring logic."
---

# Demo Prequalification for FrontrowMD

## Purpose

Evaluate whether inbound demo leads are a good fit for FrontrowMD's Clinician Certified (CC) badge program. Each lead is scored **Y** (qualified), **N** (not qualified), or **?** (insufficient information) based on the criteria below.

## Context: What FrontrowMD Does

FrontrowMD connects wellness brands with a clinician network. Clinicians submit research-based reviews and ingredient analyses for consumer health products. The CC badge appears on a brand's product page to signal clinician endorsement to shoppers. The sub-product ClinicianAI helps power this workflow.

---

## Qualification Criteria

### Must-Haves (both required for Y)

1. **DTC ecommerce with add-to-cart on their own website.** The brand must sell products directly to consumers through their own site (not exclusively through Amazon, retail stores, or marketplace platforms). An "Add to Cart" or "Buy Now" button on a product page is the signal.

2. **A health claim of some kind.** This can be broad — it includes:
   - Direct health claims ("supports gut health", "boosts immunity", "reduces inflammation")
   - Ingredient-absence claims that imply health ("does not contain X, which is normally found in this kind of product, and X is harmful")
   - Functional claims ("improves focus", "supports sleep", "aids recovery")
   - Nutritional positioning ("organic", "superfood", "clinically tested")

### Ideal-But-Not-Required (strengthen a Y, weaken a borderline)

3. **Research plausibility.** You can imagine medical research existing to support the health claim. Without this, clinicians won't have a basis for submitting research-based reviews or ingredient analyses. Most supplements, functional foods, and evidence-based wellness products pass this test.

4. **Healthspan / longevity alignment.** The product's health claim helps people live healthier or longer lives. This is subjective, but the test is: does this product contribute to physical health, disease prevention, nutritional wellness, or recovery? Products that are purely cosmetic, purely recreational, or purely about appearance (without a health mechanism) are weaker fits.

5. **CC badge intuitive sense.** If you imagine the Clinician Certified badge appearing on that product page, would a shopper instantly understand why a clinician would endorse this? The badge should feel natural and credible in context. Products where you'd have to stretch to explain the clinician connection are weaker fits.

---

## Scoring Logic

### Score Y (Qualified) when:
- Both must-haves are clearly met, OR
- Must-haves are very likely met based on available signals (brand name, domain, email domain, industry, company name), even without visiting the site
- **Lean Y generously.** If a brand is plausibly a DTC wellness product with any health angle, score Y. The demo call itself will further qualify them. False positives are cheap (a 30-min call); false negatives lose potential revenue.

### Score N (Not Qualified) when:
- The company is clearly NOT a product brand (services, agencies, clinics, gyms, real estate, legal, etc.)
- The product has zero health angle (fitness equipment only, bedding, travel accessories, adaptive clothing, etc.)
- The lead appears to be spam (adsense tags in URL, disposable email patterns, nonsensical data)
- The company is B2B-only with no consumer-facing product

### Score ? (Insufficient Info) when:
- There is genuinely no information available — no URL, no company name, no email domain clue, no industry
- This should be rare. Before scoring ?, exhaust all signals:
  - Check the **email domain** — a company email like `name@brandname.com` reveals the brand
  - Check the **company name field** — even without a URL, a company name is searchable
  - Check the **industry field** — industry classification alone can lean toward Y or N
  - Check **UTM parameters** — referral sources (e.g., "client_referrals") or campaign names can provide context
  - A gmail/yahoo/outlook email with no company name and no URL is the typical ? scenario

---

## Signal Interpretation Guide

Use these heuristics when a website visit isn't possible or practical:

### Strong Y signals
- Health/wellness/supplement keywords in domain: "health", "wellness", "bio", "vita", "nutra", "organic", "pure"
- Industry field: "Health, Wellness and Fitness", "Pharmaceuticals", "Food Production", "Food & Beverages", "Alternative Medicine", "Biotechnology", "Consumer Goods" (when combined with health-sounding brand)
- Email domain matches a known supplement/wellness brand
- Referral from an existing FrontrowMD client (UTM source = client name, UTM campaign = "client_referrals")
- Doctor/clinician founder signals: "dr" in email or company name

### Strong N signals
- Industry field: "Real Estate", "Legal Services", "Marketing and Advertising", "Information Technology and Services" (without health product)
- Service-only businesses: clinics, gyms, training services, consulting, agencies
- B2B platforms or white-label services
- Product has no health mechanism at all: pure fashion, home goods, travel gear, office supplies

### Borderline cases — lean Y
- **Skincare / beauty:** Lean Y. Most skincare brands make ingredient-based health claims (anti-inflammatory, non-toxic, dermatologist-tested). Clean beauty especially fits.
- **Devices (not consumables):** Lean Y. Red light therapy, PEMF, derma rollers, oral care devices, posture correctors — all have health claims and research.
- **Functional beverages:** Lean Y. Kombucha, adaptogenic drinks, collagen beverages, superfood lattes all have health claims.
- **Hair health / regrowth:** Lean Y. Biotin, collagen, DHT blockers etc. have research and health claims.
- **Weight management:** Lean Y. Weight loss is a health outcome with extensive research.
- **Hangover recovery:** Lean Y. Liver support, electrolyte replenishment, and recovery are health claims.
- **MLM/network marketing brands:** Lean Y. They still have DTC products and health claims. The business model is the sales team's concern, not a prequalification filter.
- **Pet health/CBD:** Lean Y if the product has health claims, even for animals.

### Borderline cases — lean N or ?
- **Alcohol brands with "functional" positioning:** ? — The CC badge on alcohol is a hard sell for clinician endorsement, even with adaptogens or botanicals added.
- **Pure cosmetics with no health mechanism:** If the only claim is "you'll look prettier" with no ingredient science, it's borderline. But if there's ANY ingredient-level claim (antioxidants, non-toxic, reduces redness), lean Y.
- **Sex toys / intimate products:** N unless there's a clear health mechanism (e.g., pelvic floor, post-surgical recovery).

---

## Process for Batch Evaluation

When given a CSV or list of leads:

1. **Parse the data.** Identify columns for: name, company, website URL, email, industry, and the target qualification column.

2. **Triage by available information:**
   - Leads WITH a URL and/or company name → evaluate against criteria
   - Leads with ONLY an email domain → use the domain as the brand signal
   - Leads with NO info (gmail + no company + no URL) → score ?

3. **For leads with URLs:** Search the web for `[domain] [product category keywords]` to determine:
   - Does the site have an add-to-cart / shop page?
   - What health claims does the brand make?
   - What kind of product is it?

4. **For leads without URLs but with company names:** Search `[company name] [industry] products` to find their site and evaluate.

5. **Write the qualification into the target column** (Y / N / ?) and optionally add a brief rationale.

6. **Present summary stats** to the user: count of Y, N, ? and highlight the strongest leads.

---

## Examples

| Lead | URL | Score | Reasoning |
|------|-----|-------|-----------|
| Troomy | troomy.com | Y | DTC mushroom gummies, clear health claims (focus, immunity), research-backed ingredients, CC badge natural fit |
| Metagenics | metagenicscanada.com | Y | Major professional-grade supplement brand, clinical research, already works with practitioners |
| TRX Training | trxtraining.com | N | Fitness equipment only, no consumable health product, no health claims on products |
| Bold Legal | boldlegal.com | N | Legal services, not a product brand at all |
| [gmail only, no company] | — | ? | No information to evaluate. Need to ask in outreach. |
| Rockstar Blends | rockstarblends.com | Y | DTC botanical oils, ECS/inflammation health claims, ingredient research exists |
| Ettitude | ettitude.com | N | Sustainable bedding brand, no health claims |
| KYPRIS Beauty | kyprisbeauty.com | Y | Clean beauty with ingredient-science positioning, skincare health claims |
