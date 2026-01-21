# Data Flow Pipeline

This document describes the data flow through a series of MapReduce jobs that process predicates and compute mutual information (MI) metrics for rule discovery.

---

## Job 1: Extract Predicates

**Input:** Raw biarcs data

**Output:**
- **Key:** `<p, Slot, w>`
- **Value:** `c(<p, Slot, w>)` - count of the predicate-slot-word tuple

---

## Job 2: Compute Totals

**Input:** `<p, Slot, w>` → `c(<p, Slot, w>)`

**Output:** Four aggregated count types:
- **Key:** `<p, Slot, *>` | **Value:** `c(<p, Slot, *>)` - count across all words
- **Key:** `<*, Slot, w>` | **Value:** `c(<*, Slot, w>)` - count across all predicates
- **Key:** `<*, Slot, *>` | **Value:** `c(<*, Slot, *>)` - total count

---

## Job 3: Join Predicate-Slot-Word with Predicate-Slot-Total

**Inputs (Merge):**
- From Job 2: `<p, Slot, *>` → `c(<p, Slot, *>)`
- From Job 1: `<p, Slot, w>` → `c(<p, Slot, w>)`

**Output:**
- **Key:** `<p, Slot, w>`
- **Value:** `c(<p, Slot, w>), c(<p, Slot, *)` - combined counts

**Mapper Details:**
- Mapper processing `<p, Slot, w>, c(<p, Slot, w>)` sends to reducer: `<p, Slot, *>` → `W \t w \t c(<p, Slot, *>)`
- Mapper processing `<p, Slot, *>, c(<p, Slot, *>)` sends to reducer: `<p, Slot, *>` → `T \t c(<p, Slot, *>)`

---

## Job 4: Compute Mutual Information (MI)

**Inputs:**
- `<p, Slot, w>` → `c(<p, Slot, w>), c(<p, Slot, *>)`
- `<p, Slot, *>` → `c(<p, Slot, *>)`
- `<*, Slot, w>` → `c(<*, Slot, w>)`
- `<*, Slot, *>` → `c(<*, Slot, *>)` *(Note: should not be committed)*

**Output:**
- **Key:** `<p, Slot, w>`
- **Value:** `MI = log(c(p,Slot,w) × c(*,Slot,*) / (c(p,Slot,*) × c(*,Slot,w)))`

---

## Job 5: Compute Denominator

**Input:** `<p, Slot, w>` → `MI`

**Condition:** Only processes predicates in the Test Set

**Output:**
- **Key:** `<p, *, *>`
- **Value:** `Denominator = Σ MI(p, s, w)` - sum of MI values for all slots and words

---

## Job 6: Intersection Contribution

**Input:** `<p, Slot, w>` → `MI`

**Condition:** For predicates p₁, p₂ ∈ test set

**Mapper Details:**
- Mapper gets `<p, Slot, w>, MI` and sends to reducer: `<*, Slot, w>` → `p, MI`

**Output:**
- **Key:** `<p₁, p₂>`
- **Value:** `contrib = Σ(MI(p₁,s,w) + MI(p₂,s,w))` where `w ∈ <p₁,s,*> ∩ <p₂,s,*>`

---

## Job 7: Final Similarity

**Inputs (Multiple):**
- From Job 6: `<p₁, p₂>` → `contrib`
- From Job 4: `<p, Slot, w>` → `MI`
- From Job 5 (cached in reducer): `p` → `denom`
- Cache files (for labels): positive-preds.txt, negative-preds.txt

**Processing:**
1. Mapper receives both Job 6 and Job 4 outputs, marks them for distinction
2. Reducer:
   - Loads denominator map from Job 5 output (stored in memory)
   - Loads test pair information from cache files (includes original predicates and labels)
   - For each predicate pair, retrieves their denominators: $d₁ = \text{denom}(p₁)$, $d₂ = \text{denom}(p₂)$
   - Calculates similarity: $\text{similarity} = \frac{\text{contrib}}{\text{denom}_1 + \text{denom}_2}$

**Output:**
- **Key:** `<p₁, p₂>` (canonical predicate pair)
- **Value:** `similarity \t label` where label ∈ {0, 1}
  - Label = 1 if pair is in positive test set
  - Label = 0 if pair is in negative test set

---

## Notation

- `c(...)` - Count function
- `MI(...)` - Mutual Information value
- `T` - Total marker in mapper output
- `W` - Word marker in mapper output
