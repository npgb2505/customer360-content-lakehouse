# Data model

```mermaid
erDiagram
  USERS ||--o{ WATCH_EVENTS : watches
  CONTENT ||--o{ WATCH_EVENTS : viewed
  USERS ||--o{ SEARCH_EVENTS : searches
  CONTENT ||--o{ SEARCH_EVENTS : clicked_result
  USERS ||--|| CUSTOMER_360 : summarized_as
  CONTENT ||--o{ CONTENT_KPIS : summarized_as
```

`customer_360` is one current snapshot row per customer. `content_kpis` is one row per
date and content title. `search_trends` is one row per date, normalized query, and
result category. `monthly_search_trends` is one row per month and normalized query.
