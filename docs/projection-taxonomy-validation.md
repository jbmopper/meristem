# Projection Taxonomy Validation

R6 validation source: `.meristem/feed-watch-pending.jsonl`, a JSONL archive of
feed watcher snapshots from June 2026 coordination work.

Classifier:

- `work_item.event_appended` with `payload.inner_kind` prefix `coordination.`:
  `decision`
- other `work_item.event_appended` inner kinds: `progress`
- all other event kinds: static classes from `internal/feed/taxonomy.go`

Results over all feed item appearances:

| Metric | Count |
| --- | ---: |
| Feed item appearances | 185 |
| `work_item.event_appended` appearances | 125 |
| Progress appearances | 35 |
| Decision appearances | 90 |
| Progress share of `work_item.event_appended` appearances | 28.00% |

Results over unique event ids:

| Metric | Count |
| --- | ---: |
| Unique feed items | 106 |
| Unique `work_item.event_appended` items | 74 |
| Unique progress items | 20 |
| Unique decision items | 54 |
| Progress share of unique `work_item.event_appended` items | 27.03% |

Conclusion: this archive is mostly coordination chatter, not progress chatter,
under the R6 taxonomy. The `>90% progress` acceptance wording is not satisfied
by this sample. The taxonomy implementation keeps the spec rule that
`coordination.*` inner events classify as `decision`; this artifact records the
sample mismatch rather than weakening that classifier.
