# Power BI delivery

Run the demo, then use **Get data > Text/CSV** in Power BI Desktop to import:

- `exports/customer_360.csv`
- `exports/content_kpis.csv`
- `exports/search_trends.csv`
- `exports/monthly_search_trends.csv`

Recommended relationships are not required because each export is already at its
dashboard grain. Use `event_date`/`month` as the shared reporting period.

## Dashboard 1: Content performance

Cards: total views, unique viewers, total watch hours, completion rate.

```DAX
Total Views = SUM(content_kpis[views])
Unique Viewers = SUM(content_kpis[unique_viewers])
Watch Hours = DIVIDE(SUM(content_kpis[watch_minutes]), 60)
Completion Rate = AVERAGE(content_kpis[completion_rate])
```

Visuals: category share, top titles, daily watch-hours trend, and completion-rate
scatter plot.

## Dashboard 2: Customer and search intelligence

```DAX
Active Customers = CALCULATE(COUNTROWS(customer_360), customer_360[view_events] > 0)
Searches = SUM(search_trends[searches])
Search Clicks = SUM(search_trends[clicks])
Search CTR = DIVIDE([Search Clicks], [Searches])
```

Visuals: plan/contract segmentation, engagement segment, favorite category, regional
distribution, top queries, zero-result proxies, and month-over-month search shifts.
