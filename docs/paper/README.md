# CCM architecture paper

Start with **[CCM: Composite Cognitive Model](CCM.md)**, the author-approved
manuscript, version 1.0 (22 October 2026), based on the supplied architecture brief
and the inspected CCM implementation.

- [Editorial and technical review record](REVIEW.md)
- [Claim-to-code evidence appendix](CLAIMS.md)
- [Bibliography](references.bib)
- [Packaged source manifest](source-manifest.json)
- [Figure verification record](FIGURE_VERIFICATION.md)

## Figures

| Figure | Standalone interactive HTML | Paper SVG |
|---|---|---|
| Composition architecture | [Open](figures/01-composition.html) | [SVG](figures/01-composition.svg) |
| MoA agent lifecycle | [Open](figures/02-agents.html) | [SVG](figures/02-agents.svg) |
| MoW workflow graph | [Open](figures/03-workflows.html) | [SVG](figures/03-workflows.svg) |
| MoM model resolution | [Open](figures/04-models.html) | [SVG](figures/04-models.svg) |
| Response/improvement boundaries | [Open](figures/05-improvement.html) | [SVG](figures/05-improvement.svg) |

GitHub renders the Markdown/SVGs; download an HTML file and open it locally for
interactive viewing. The paper contains architecture graphs and evidence tables;
it deliberately does not manufacture performance plots without benchmark data.

## Reproduction

Diagram specifications are adjacent `*.architecture.json` files. With the Archify
tool available, validate and deliver each specification using:

```sh
node "$ARCHIFY_HOME/bin/archify.mjs" validate architecture \
  docs/paper/figures/01-composition.architecture.json --quality showcase --json
node "$ARCHIFY_HOME/bin/archify.mjs" deliver architecture \
  docs/paper/figures/01-composition.architecture.json \
  docs/paper/figures/01-composition.html --quality showcase --json
node "$ARCHIFY_HOME/bin/archify.mjs" visual-check \
  docs/paper/figures/01-composition.html --json
```

Repeat for figures 02–05. `$ARCHIFY_HOME` is the local tool installation, not a
repository dependency. After successful deterministic delivery:

```sh
python3 docs/paper/export_figures.py
```

The exporter extracts delivered geometry and embeds its styles/fonts into static
light-theme SVGs. It does not modify the trusted HTML. `figures/exports.json` binds
the SVGs to source HTML hashes. Browser inspection and perceptual review remain
separate from XML parsing and deterministic diagram checks.

## Attribution and publication status

Author: **TÖRE ÇAĞRI UYAR**. Repository license: [MIT](../../LICENSE); copyright
© 2026 TÖRE ÇAĞRI UYAR. The manuscript is author-approved as version 1.0 (22 October
2026) and includes an explicit AI-use disclosure. It is not peer-reviewed, published,
accepted by a journal, or assigned a DOI. The exported viewers include Archify runtime
assets and bundled font resources; their notices are retained in
[third-party](third-party/). No target venue, institutional affiliation, or benchmark
results are claimed. Local operational logs and secrets are
excluded from this package.
