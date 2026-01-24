## Analysis

This section reports the evaluation results computed from `10-output/score` (small) and `100-output/score` (large).

### F1-Measure (Small and Large Inputs)

**Goal:** Compute precision, recall, and F1 for each input size.

**Results (best F1 threshold per size):**
- Small input:
  - Threshold: `t = 0.0`
  - Precision: `0.9616279069767442`
  - Recall: `1.0`
  - F1: `0.980438648488441`
  - Confusion: `TP=2481, FP=99, TN=0, FN=0`
- Large input:
  - Threshold: `t = 0.0`
  - Precision: `0.9616279069767442`
  - Recall: `1.0`
  - F1: `0.980438648488441`
  - Confusion: `TP=2481, FP=99, TN=0, FN=0`

**Why F1 is identical for both inputs:**
At threshold `t = 0.0`, all items are predicted as positive (since all similarity scores are ≥ 0.0). This means:
- The confusion matrix depends **only on the ground truth label distribution**, not on the similarity scores themselves
- Both test sets have the same label distribution: 2481 positives and 99 negatives
- Therefore: `TP = 2481` (all positives predicted correctly), `FP = 99` (all negatives predicted incorrectly), `TN = 0`, `FN = 0`
- Since the confusion matrices are identical, precision, recall, and F1 are identical

**Score distribution:**
- **Small input:** 2563 scores = 0.0 (99.3%), only 17 scores > 0.0
- **Large input:** 1313 scores = 0.0 (50.9%), 1267 scores > 0.0 (49.1%), including 2 scores between 0.0 and 0.01
- The large input has much better score separation, with nearly half of scores being positive, while the small input has almost all scores at exactly 0.0

**Note:** While the F1 metrics are identical at `t=0.0`, the similarity scores themselves ARE different between small and large inputs. This difference becomes apparent at other thresholds, as shown in the Precision-Recall curves where the large input shows better score separation and more gradual precision/recall trade-offs.

---

### Precision-Recall Curve (Small and Large Inputs)

**Goal:** Plot PR curves for both input sizes.


**Generated plots:**
- Small input: `Reports/Analysis/pr_small.svg`
- Large input: `Reports/Analysis/pr_large.svg`

**Small input (sample PR points):**
- `(t=0.2598168476, P=1.0, R=0.0008061266)`
- `(t=0.2076454596, P=1.0, R=0.0016122531)`
- `(t=0.2010747254, P=1.0, R=0.0024183797)`
- `(t=0.1879160062, P=1.0, R=0.0032245062)`
- `(t=0.1446377565, P=1.0, R=0.0040306328)`
- `(t=0.1276202170, P=1.0, R=0.0048367594)`
- `(t=0.0908794904, P=1.0, R=0.0056428859)`
- `(t=0.0735036171, P=1.0, R=0.0064490125)`
- `(t=0.0680174843, P=1.0, R=0.0068520758)`
- `(t=0.0, P=0.9616279069, R=1.0)`

**Large input (sample PR points):**
- `(t=0.4776048331, P=1.0, R=0.0008061266)`
- `(t=0.2292267679, P=0.9642857143, R=0.0544135429)`
- `(t=0.1760809062, P=0.9572953737, R=0.1084240226)`
- `(t=0.1441758268, P=0.9594272076, R=0.1620314389)`
- `(t=0.1228319573, P=0.9661319073, R=0.2184602983)`
- `(t=0.1039244992, P=0.9617021277, R=0.2732769045)`
- `(t=0.0850884365, P=0.9610389610, R=0.3280935107)`
- `(t=0.0665583011, P=0.9614213198, R=0.3817009270)`
- `(t=0.0466652660, P=0.9645075421, R=0.4381297864)`
- `(t=0.0, P=0.9616279069, R=1.0)`

---

### Error Analysis

**Goal:** Inspect 5 examples each for TP, FP, TN, FN.

**Thresholds used for error analysis:**
- Small: `t = 0.0` (best F1, but yields no TN/FN).
- Large: `t = 0.0085838696` (chosen to provide all four categories).

**Small input examples (t=0.0):**
- True Positives (score small | score large):
  - `X accompani Y` vs `X accompani by Y` | `0.1879160062` | `0.1815334133`
  - `X accompani by Y` vs `X accompani Y` | `0.1879160062` | `0.1815334133`
  - `X accompani Y` vs `X associ with Y` | `0.1276202170` | `0.1239164294`
  - `X associ with Y` vs `X accompani Y` | `0.1276202170` | `0.1239164294`
  - `X accompani with Y` vs `X accompani by Y` | `0.2010747254` | `0.1887838144`
- False Positives (score small | score large):
  - `X produc by Y` vs `X destroi Y` | `0.0` | `0.1020041345`
  - `X destroi Y` vs `X produc by Y` | `0.0` | `0.1020041345`
  - `X differenti from Y` vs `X resembl x` | `0.0` | `0.0`
  - `X treat with Y` vs `X produc Y` | `0.0` | `0.1177616465`
  - `X produc Y` vs `X treat with Y` | `0.0` | `0.1177616465`
- True Negatives: none at `t=0.0` (all items are predicted positive).
- False Negatives: none at `t=0.0` (all items are predicted positive).

**Large input examples (t=0.0085838696):**
- True Positives (score small | score large):
  - `X accompani Y` vs `X accompani by Y` | `0.1879160062` | `0.1815334133`
  - `X accompani by Y` vs `X accompani Y` | `0.1879160062` | `0.1815334133`
  - `X accompani Y` vs `X associ with Y` | `0.1276202170` | `0.1239164294`
  - `X associ with Y` vs `X accompani Y` | `0.1276202170` | `0.1239164294`
  - `X accompani Y` vs `X caus Y` | `0.0` | `0.1039244992`
- False Positives (score small | score large):
  - `X distinguish from Y` vs `X associ with Y` | `0.0` | `0.2134533075`
  - `X associ with Y` vs `X distinguish from Y` | `0.0` | `0.2134533075`
  - `X avoid in Y` vs `X us in Y` | `0.0` | `0.0963978288`
  - `X us in Y` vs `X avoid in Y` | `0.0` | `0.0963978288`
  - `X distinguish from Y` vs `X character by Y` | `0.0` | `0.2200268060`
- True Negatives (score small | score large):
  - `X differenti from Y` vs `X resembl x` | `0.0` | `0.0`
  - `X differ from Y` vs `X includ x` | `0.0` | `0.0`
  - `X deriv from Y` vs `X be against Y` | `0.0` | `0.0`
  - `X be against Y` vs `X deriv from Y` | `0.0` | `0.0`
  - `X character by Y` vs `X distinguish from x` | `0.0` | `0.0`
- False Negatives (score small | score large):
  - `X us for Y` vs `X correct with x` | `0.0` | `0.0`
  - `X treat by Y` vs `X erad x` | `0.0` | `0.0`
  - `X prevent Y` vs `X control with x` | `0.0` | `0.0`
  - `X associ with Y` vs `X occur with x` | `0.0` | `0.0`
  - `X us for Y` vs `X be with x` | `0.0` | `0.0`

**Comparison:**
- Compare similarity values for each example between small and large inputs.
- Note shifts in similarity and label flips.

**Observations:**
- Small input has heavy score mass at `0.0`, making it impossible to produce TN/FN and FP simultaneously with a single threshold.
- Large input shows more separation, allowing a non-zero threshold to produce all four categories.
- Many FP/TP examples are near-synonyms or close paraphrases (e.g., `accompani` vs `associ`).
- FN examples tend to have zero similarity in both sizes, suggesting sparse evidence for some positive pairs.