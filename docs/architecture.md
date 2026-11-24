# Project architecture

```mermaid
flowchart TD
    A[(Delta control table)] --> B[Lakeflow Job: SQL lookup]
    B --> C[For each active pipeline]
    C --> D[Generic Auto Loader PySpark notebook]
    E[CSV / JSON in Unity Catalog volume] --> D
    D --> F[(Bronze Delta tables)]
    F --> G[Rule-set validation and deduplication]
    G --> H[(Silver Delta tables)]
    G --> Q[(Invalid-row quarantine tables)]
    H --> I[Gold aggregate notebook]
    I --> J[(Gold region totals)]
    I --> K[(Orphan customer references)]
```

## Limitations
- Silver and Gold snapshots are overwritten for simplicity; large production workloads should use transactional incremental changes.
- `dropDuplicates` is used only for identical seeded duplicates. Conflicting duplicate records need deterministic precedence.
- The rule-set dispatch is intentionally explicit and supports customer and transaction schemas. New schema types require corresponding rules.
- Config metadata must be restricted to trusted writers; secrets do not belong in control tables.
- No SLA, encryption policy, DR/HA, high-scale benchmarks, or production reliability claims are demonstrated here.
