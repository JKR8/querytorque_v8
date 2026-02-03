# DAX Performance Optimization Case Study: MV CR Intensity
**Date:** 08 December 2025

## Purpose
This document explains the architectural and DAX-pattern changes that enabled a step-function performance improvement in the *Portfolio_Asset_Matrix MV CR Intensity* calculation. It is designed as internal training material.

## Context in one sentence
The original implementation spread logic across a long measure chain with repeated table scans and Formula Engine-heavy patterns; the optimized version collapses the computation into a single, grain-first pipeline.

## Original design (high-level anatomy)
- Top-level measure forces Portfolio filter, then delegates to a Scope switch.
- Each scope route branches into Benchmark vs non-Benchmark logic.
- Benchmark pathway uses GROUPBY/SUMX to roll up Sub_Sector results.
- Multiple measures repeatedly scan GS Asset, Daily Position, and ESG Trucost Climate tables.
- Ownership and market cap logic is complex and re-evaluated across dependent measures.

### Typical red-flag patterns observed
- Repeated `SELECTEDVALUE` calls inside branching logic.
- Nested `SUMX` over large tables across multiple dependent measures.
- `GROUPBY` + `SUMX` inside conditional Benchmark logic.
- Multiple measures computing similar numerators/denominators with slight scope variations.

## Optimized design (high-level anatomy)
- Caches scope and market-cap selection once.
- Builds OwnershipByAsset at ISIN grain with early Portfolio filtering.
- Builds CarbonRevByAsset at ISIN grain, computing Base/Up/Down once per ISIN.
- Joins the two compact tables once.
- Computes Numerator and Denominator as weighted sums, then divides.

## The principles to teach
### 1. Collapse measure forests into a single orchestrator
Replace long dependency chains with one measure that defines the full execution plan using VARs.

### 2. Cache slicer state once
Store SELECTEDVALUE/ISINSCOPE outputs in variables to avoid repeated evaluation inside iterators.

### 3. Choose and lock the correct grain early
Build intermediate tables at the smallest necessary grain (e.g., ISIN) before computing totals.

### 4. Aggregate early, reuse often
Compute Carbon/Revenue/Ownership once per grain and reuse them; avoid repeated SUMX over large facts.

### 5. Prefer ratio of sums for intensity metrics
Compute Σ(weight × numerator) / Σ(weight × denominator) instead of summing ratios across levels.

### 6. Avoid GROUPBY + SUMX in conditional branches
These patterns often force heavy Formula Engine iteration; replace with simpler grain-first logic.

### 7. Filter early and narrowly
Use KEEPFILTERS/explicit filters inside CALCULATETABLE so later steps operate on smaller sets.

### 8. Precompute stable business rules
Move complex ownership or mapping logic into columns/Power Query when it’s not truly dynamic.

### 9. Use small intermediate tables and join once
Materialize compact tables and combine with NATURALINNERJOIN/LOOKUPVALUE patterns where appropriate.

## Before vs After mental model
**Before:** *Many measures × many scans × many context transitions*  → unpredictable performance.

**After:** *Two compact ISIN-level tables → one join → ratio of weighted sums*  → stable, fast execution.

## Practical checklist for similar measures
- Can I compute this at a smaller grain first (asset/ISIN/security)?
- Am I scanning the same big table more than once across dependants?
- Is there a `GROUPBY/SUMX` pattern that can be replaced by a grain-first approach?
- Can I convert a sum-of-ratios pattern into a ratio-of-sums with weights?
- Should any part of this logic move to Power Query or calculated columns?
- Have I cached slicer selections into VARs?
- Have I applied the most restrictive filters as early as possible?