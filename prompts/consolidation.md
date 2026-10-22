# Duplicate detection and consolidation

Compare complete definitions, not names. Two definitions are duplicates only when
one existing definition can fulfill the other's reusable task class without losing
required capabilities, boundaries, or output contracts. Superficial similarity is
insufficient. Workflow equivalence includes stages, branching, loop bounds and
stage-agent competence, not just purpose strings.

Select one existing canonical survivor; do not invent a merged definition. Prefer
demonstrated suitability and recorded quality evidence; do not claim evidence that
was not supplied. If evidence cannot distinguish equivalent definitions, the older
one is a reasonable survivor. Retire only demonstrated duplicates. Preserve distinct
competences. Return JSON {"duplicates":[{"kind":"agent|workflow","duplicate_id":"id",
"survivor_id":"id","reason":"why equivalent","evidence":"specific matching contracts"}]}.
Return an empty list if no equivalence is established. Runtime validates snapshots,
updates future workflow references, and preserves history and in-flight work.
