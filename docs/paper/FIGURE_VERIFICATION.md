# Figure verification

## Current composition figure

Figure 1 was deterministically delivered with 9/9 showcase checks, zero errors
and zero warnings. `figures/receipts.json` contains the current specification and
HTML hashes; `figures/exports.json` binds the SVG exports. Automated browser
inspection failed with Chromium DevTools EPIPE. Current perceptual review of
Figure 1 is pending; the delivery receipt is not a browser-acceptance result.

All five figures are typed **architecture** views: they show composition and
control relationships without asserting a measured execution timeline. Their
authored sources, standalone HTML viewers, and print SVGs are adjacent in `figures/`.

## Deterministic delivery

Archify 2.17.0-dev.1 validated and delivered every figure with **9/9 showcase
checks, zero composition errors and zero warnings**. Geometry corrections were
limited to diagnosed endpoint/label issues: two rounds for Figure 1, one round
for Figures 2–5. No authored specification was changed after its final successful
validation/delivery.

`figures/receipts.json` records exact specification and artifact SHA-256 hashes,
byte counts, and validation counts. `figures/exports.json` binds each exported SVG
to its HTML source. All five SVGs parse as XML and embed styles/font resources;
they contain no executable scripts.

## Browser evidence: failed/incomplete

The packaged browser inspection was attempted for all five delivered HTML files.
The local Chromium harness failed with evaluation timeouts or process exits.
Consequently **automated browser evidence is failed**, not passed or skipped.
No complete four-viewport containment, theme-switching, focus, or export-interaction
acceptance is claimed. The deterministic delivery receipts remain valid.

Raw local browser diagnostics contain machine-specific paths and were excluded
from this public package; the portable receipts record the outcome without them.

## Separate perceptual review

The recorded static SVG review for Figures 2–5 passed at 1440 × 1000 in the light
theme. Figure 1's current artifact has not received a new perceptual acceptance;
its receipt reports `pending`. Earlier captures are not acceptance of changed
bytes. Comprehensive HTML viewer acceptance remains incomplete.

## Rechecking

Use the commands in [README.md](README.md). Re-run `visual-check` in a suitable
browser environment before claiming complete viewer acceptance. If the source
specifications change, revalidate, redeliver, regenerate the exports, and update
the hashes and visual-review evidence rather than reusing old receipts.
