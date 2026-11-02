# The Reversibility Canvas — Decision Studio

**Calibrating Entrepreneurial Evidence to the Cost of Being Wrong**

## What changed

This is a **decision-first working prototype**, not a company search engine. Its unit is an entrepreneurial commitment with a critical assumption. The optional company field provides context for linked evidence; it is not used to infer risk from the company name.

The sequence stays faithful to the original symposium proposal:

1. Decision
2. Critical Assumption
3. Reversibility Profile
4. Evidence Threshold
5. Exit Trigger

The separate research assessment contains five constructs: Evidence–Claim Fit, Commercial Viability, Reversibility Exposure, Strategic Optionality, and Exit Executability.

## Run

Unzip and open `index.html` locally in a modern browser. No installation, internet connection, or login is required. You can fill the steps, review the full A3-printable Canvas, compare two commitments under the same assumption, inspect candidate model weights, and export a JSON decision record.

`node tests.js` runs deterministic core-rule tests if Node.js is installed.

## How to test the central claim

Click **Load fictional comparison**. It populates one hypothetical venture with the assumption that retailers will reorder within 60 days, a 24-month exclusive distribution agreement, and an alternative six-week non-exclusive pilot. The same assumption and evidence remain constant; the long contract receives a higher provisional evidence threshold than the pilot. No company facts are claimed.

## Methodological limits

- Reversibility classifications and evidence-level mappings are **explicit uncalibrated design heuristics**, not learned risk probabilities or validated evidence requirements.
- A restrictive contract and a small pilot can legitimately require different evidentiary efforts. Ex ante thresholds are provisional design rules to be tested experimentally.
- All seven profile components must be assessed. **Unknown is never zero.**
- Legal or unacceptable third-party harm gates cannot be compensated by a favourable composite index.
- Candidate weights (25/25/20/15/15) remain illustrative. Empirical predictive contributions, normative decision weights, and decision utility require distinct forms of validation.
- Source fields are user-supplied and not independently verified in this standalone version. A real UK evidence database, company identity matching, audited source extracts, AI extraction and company search are **not integrated**.
- Do not present this demo as a validated recommendation system or use it as legal, regulatory or investment advice.

## Intended architecture

Decision and critical assumption → source-linked evidence from authorised UK company/market/contract data → reversibility and hard-gate checks → proportional evidence requirements → executable exit conditions → an auditable report. The 10-domain UK data library is a supporting resource, not the main user journey.

## Deployment note

The provided files are a standalone alternative prototype; they do not alter an existing deployed website. To replace or edit that website, use the website's editable source or deployment workflow.

Palette: Olive & Oat · #444D36 · #8A9673 · #CED4B7 · #EBDEBA · #F6F2E8.
