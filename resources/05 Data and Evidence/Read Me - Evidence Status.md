# Evidence Pack — read before republishing

## Verified from the user's audit files, not from live database inspection

- Stage 2 report dated 2 November 2026: 5,851,547 distinct UK company IDs; 810,721 OCDS release/compiled records; 234,570 strictly ID-matched supplier associations across 48,244 companies; 29,253 financial file names indexed.
- Stage 2 processed 63 local files (5.33GB), including overlapping company source files; `company_rows_read` is not a count of distinct firms.
- Additional audit enumerated 15 ZIPs totalling about 2.94GB compressed, inspected ZIP member metadata/headers; it did not parse all records, perform Stage 3 linkage, or calibrate the model.
- The project-reported DOI `10.5281/zenodo.23873243` was provided by the researcher. The attempted live DOI inspection was not successful in the current tool environment. Do not claim its item-level size, licensing, or complete contents were independently checked here.

## Reproducibility rules

1. Publish a machine-readable source manifest with each package's original source URL, retrieval date, SHA256, version, licence, jurisdiction and transformation history.
2. Source license governs reuse. A single Zenodo-wide CC BY licence does not create a new right to relicense incompatible third-party data. Where needed, publish metadata and scripts, not restricted raw contents.
3. Identify committed actions separately from firm records and procurement association rows.
4. Record when every fact was first available. Predictors at `t0` may not use later-filed facts.
5. Document deduplication and matching failures and public-vs-private coverage gaps.
6. Use explicit statuses Observed, Derived, Unobserved/Not assessable and Needs review.

## File scope

This pack contains source **catalogues, citations, audit summaries and code specifications**, not the multi-GB dataset itself. No new raw UK company records were downloaded or privately accessed while creating this source pack.
