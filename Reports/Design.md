## Design

This section documents the system components, their I/O, and MapReduce characteristics. Numbers and sizes are estimated based on the pipeline structure and should be updated after running on the target datasets.

### Component A: Step 1 - Extract Predicates and Totals

**Purpose:** Parse raw biarcs into predicate-slot-word counts and totals required for MI.

**Input:**
- Raw biarcs text lines.

**Mapper:**
- **Key:** `<p, Slot, w>`
- **Value:** `1` (count signal)
- **Emits (derived totals):**
  - `<p, Slot, *>` → `1`
  - `<*, Slot, w>` → `1`
  - `<*, Slot, *>` → `1`

**Reducer:**
- **Key:** same as mapper key
- **Value:** aggregated count `c(...)`

**Key-Value characteristics (estimates):**
- **Key size:** `p` + `Slot` + `w` tokens (string-like); varies with predicate and word length.
- **Value size:** small integer count.
- **Number of K/V pairs:** proportional to number of biarcs lines; totals add a small constant factor.

**Memory usage (estimate):**
- Reducer holds counts per key; memory grows with unique keys assigned to reducer.

---

### Component B: Step 2 - Compute Mutual Information (MI)

**Purpose:** Compute MI for each `<p, Slot, w>` using totals from Step 1.

**Inputs:**
- `<p, Slot, w>` → `c(<p, Slot, w>)`
- `<p, Slot, *>` → `c(<p, Slot, *>)`
- `<*, Slot, w>` → `c(<*, Slot, w>)`
- `<*, Slot, *>` → `c(<*, Slot, *>)`

**Mapper:**
- Tags records to join totals with `<p, Slot, w>`.

**Reducer:**
- Joins counts and computes:
  - `MI = log(c(p,Slot,w) × c(*,Slot,*) / (c(p,Slot,*) × c(*,Slot,w)))`

**Key-Value characteristics (estimates):**
- **Key:** `<p, Slot, w>`
- **Value:** `MI` float
- **Value size:** float/double.

**Memory usage (estimate):**
- Reducer caches totals needed per key group; bounded by keys routed to reducer.

---

### Component C: Step 3 - Compute Denominator

**Purpose:** Sum MI values per predicate in the test set.

**Input:**
- `<p, Slot, w>` → `MI`

**Mapper:**
- **Key:** `<p, *, *>`
- **Value:** `MI`

**Reducer:**
- **Key:** `<p, *, *>`
- **Value:** `Σ MI(p, s, w)`

**Key-Value characteristics (estimates):**
- **Key size:** predicate string.
- **Value size:** float/double.

**Memory usage (estimate):**
- Reducer aggregates MI values per predicate; minimal memory per predicate.

---

### Component D: Step 4 - Intersection Contribution

**Purpose:** Compute contribution for predicate pairs based on shared `(Slot, w)` items.

**Input:**
- `<p, Slot, w>` → `MI`

**Mapper:**
- **Key:** `<*, Slot, w>`
- **Value:** `p, MI`

**Reducer:**
- Forms predicate pairs for the same `<Slot, w>`.
- **Key:** `<p1, p2>`
- **Value:** partial contribution.

**Key-Value characteristics (estimates):**
- **Key size:** two predicate strings.
- **Value size:** float/double.
- **Number of K/V pairs:** depends on co-occurrence counts; can be large for frequent words.

**Memory usage (estimate):**
- Reducer buffers all predicates sharing a `<Slot, w>` to form pairs; can be high for popular words.

---

### Component E: Step 5 - Final Similarity

**Purpose:** Produce final similarity and label for each predicate pair.

**Inputs:**
- `<p1, p2>` → `contrib`
- `<p, Slot, w>` → `MI`
- `p` → `denom`
- Cache files: `positive-preds.txt`, `negative-preds.txt`

**Mapper:**
- Tags sources and forwards to reducers.

**Reducer:**
- Joins `contrib` with denominators:
  - `similarity = contrib / (denom(p1) + denom(p2))`
- Attaches label.

**Key-Value characteristics (estimates):**
- **Key:** `<p1, p2>`
- **Value:** `similarity \t label`
- **Value size:** float + small int label.

**Memory usage (estimate):**
- Reducer holds denominator map (test set predicates); memory grows with test set size.

