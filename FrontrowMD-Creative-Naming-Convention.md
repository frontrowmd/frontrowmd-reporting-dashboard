# FrontrowMD Creative Naming Convention

## Why This Matters

Your dashboard's Placement & Talent analysis can only report on what the ad name encodes. Currently, ~80% of your Meta spend goes to names like "Video Ad - 8" — which tells us format (video) and nothing else. This means talent performance, concept testing, and variant tracking are invisible.

Adopting a structured naming convention unlocks automated answers to:
- **Which talent converts best?** (Irfan vs Aurea vs Talar)
- **Which concept drives the most demos?** (Trust Hook vs 2AM Shoppers vs Rubiks Cube)
- **Which format wins by stage?** (UGC vs polished, video vs static vs carousel)
- **Which variant of a winning concept should scale?** (v1 vs v2 vs v3)

---

## Recommended Convention

```
{Channel}_{Stage}_{Format}_{Talent}_{Concept}_{Variant}
```

### Segment Definitions

| Segment | Required | Values | Notes |
|---|---|---|---|
| **Channel** | Yes | `Meta`, `LI`, `TT`, `Google` | Platform where the ad runs |
| **Stage** | Yes | `S01` (TOF), `S02` (MOF), `S03` (BOF/Retargeting) | Funnel stage — matches existing campaign naming |
| **Format** | Yes | `Vid`, `Img`, `Car`, `UGC`, `Search` | Creative format |
| **Talent** | Yes | `Irfan`, `Aurea`, `Talar`, `Forrest`, `NoTalent` | Who appears in the creative. Use `NoTalent` for graphics/text-only |
| **Concept** | Yes | 2-3 word PascalCase | Hook or narrative theme (e.g., `TrustHook`, `2AMShoppers`) |
| **Variant** | Yes | `v1`, `v2`, `v3` or `A`, `B` | Version/variant for testing |

### Rules

- Use underscores `_` as the delimiter (not hyphens, spaces, or mixed)
- Keep each segment concise — max 15 characters
- PascalCase for Concept (no spaces, no special characters)
- Always include all 6 segments, even if a field is "default" (e.g., `NoTalent`)
- Never change a live ad's name mid-flight — apply to new creatives going forward

---

## Migration Examples

| Current Name | New Name |
|---|---|
| `Video Ad - 8` | `Meta_S01_Vid_Irfan_TrustHook_v1` |
| `Aurea Video - 1` | `Meta_S01_UGC_Aurea_ClinicianReview_v1` |
| `Clinician AI Video Ad - 1 (60 Seconds)` | `Meta_S01_Vid_NoTalent_ClinicianAI60s_v1` |
| `Retargeting Video Ad - Forrest` | `Meta_S03_Vid_Forrest_Retargeting_v1` |
| `Retargeting Video Ad - LofiGreen` | `Meta_S03_Vid_NoTalent_LofiGreen_v1` |
| `2 AM Shoppers` | `Meta_S01_Vid_Irfan_2AMShoppers_v1` |
| `Rubiks Cube` | `Meta_S01_Vid_NoTalent_RubiksCube_v1` |
| `Image Ad - 2` | `Meta_S01_Img_NoTalent_ProductShot_v2` |
| `Image Ad - 3 - Gruns` | `Meta_S01_Img_NoTalent_Gruns_v3` |
| `Image Ad - 4 - Cheers` | `Meta_S01_Img_NoTalent_Cheers_v4` |
| `Carousel Ad - 1` | `Meta_S01_Car_NoTalent_FeatureWalk_v1` |
| `PaidAd - Video - Clincian Suite - Website Image` | `Meta_S02_Vid_NoTalent_ClinicianSuite_v1` |

---

## What the Dashboard Parses Automatically

Once you adopt this convention, the dashboard's `detectSubject()` function will automatically extract:

| Segment | Parser Logic | Analysis Unlocked |
|---|---|---|
| **Talent** | 4th underscore-delimited segment | "Aurea drives 3x more demos than Irfan at 50% lower CPD" |
| **Format** | 3rd segment (`Vid`/`Img`/`Car`/`UGC`) | "UGC outperforms polished video on Meta by 40% CTR" |
| **Stage** | 2nd segment (`S01`/`S02`/`S03`) | "S01 creatives have highest CTR but S03 has lowest CPD" |
| **Concept** | 5th segment | "TrustHook concept drove 18 demos across 4 variants" |
| **Variant** | 6th segment | "v2 of 2AMShoppers outperforms v1 by 22% CTR" |

Combined with the existing placement data (`publisher_platform` + `platform_position` from Windsor), you get a full matrix:

> **"Aurea's UGC videos perform best in IG Stories ($142 CPD) while Irfan's polished videos win in IG Feed ($189 CPD). Shift Aurea's budget toward Stories and Irfan's toward Feed."**

That's the kind of insight that's currently invisible.

---

## Implementation Plan

1. **Week 1:** Apply new naming to all NEW creatives launched this week. Don't rename existing live ads.
2. **Week 2:** As old creatives get paused (from LOSER/fatigue flags), their replacements use the new convention.
3. **Week 3:** Dashboard parser updated to extract all 6 segments. Full talent × placement × concept analysis live.
4. **Week 4:** First full Monday Creative Session with structured naming data. Expect ~60% coverage (new ads only).
5. **Week 6+:** ~90% coverage as old ads cycle out.

---

## LinkedIn / TikTok / Google Naming

The same convention applies across all channels — just swap the Channel prefix:

- `LI_S01_Vid_Irfan_TrustHook_v1`
- `TT_S01_UGC_Aurea_ClinicianReview_v1`
- `Google_S02_Search_NoTalent_BrandTerm_v1`

Note: LinkedIn ad-level data is limited by UGC permissions. Campaign-level naming (`LI_S01_BrandAwareness`) is more actionable for LinkedIn.
