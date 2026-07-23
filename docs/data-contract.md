# Data contract

All timestamps are UTC ISO-8601 values. Natural identifiers are strings and never
reassigned.

| Dataset | Grain / key | Required fields | Invalid handling |
|---|---|---|---|
| users | one customer / `customer_id` | plan, contract, region, signup date | fail master-data load |
| content | one title / `content_id` | title, category, duration | fail master-data load |
| watch_events | one playback event / `event_id` | customer, content, session, timestamps, positive minutes | quarantine |
| search_events | one query / `search_id` | customer, query, timestamp | quarantine |

Duplicate event IDs use latest `ingest_ts`. A clicked search must reference a known
content ID. Watch time may not exceed 125% of the catalog duration. Compatible additive
fields may be accepted after updating this contract; missing, renamed, or incompatible
required fields fail before gold publication.
