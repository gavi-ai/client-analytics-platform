Client Runbook — Analytics Platform Operations Guide
This guide is written for non-technical team members. It answers the three questions you're most likely to have:
Did the pipeline run today?
Why did I get a Slack alert?
How do I add a new data source?
***1. How to check if the pipeline ran successfully
Check Slack first. Every morning after 6:00 AM UTC, you'll receive one of two messages in #pipeline-alerts:
Green message (success):
> ✅ Pipeline Complete | Date: 2026-05-30 | Rows loaded: stripe: 1,842 | hubspot: 4,210 | ... | Dashboard refreshed.
This means all data is current. The Streamlit dashboard will reflect today's data.
Red message (failure):
> ❌ Pipeline Failed | DAG: master_client_pipeline | Task: load_to_bigquery | View logs →
This means something went wrong. See Section 2.
No message by 8:00 AM UTC?
Contact your data engineer. The pipeline may have failed silently (rare, but possible).
***2. What to do when you get a failure alert
Most failures fall into four categories:
API credentials expired
Symptom: Slack shows task: extract_all_apis failed.
Fix: One of your API keys has expired or been revoked. Common culprits:
Stripe: regenerate in Stripe Dashboard → Developers → API Keys
HubSpot: regenerate in HubSpot → Settings → Integrations → Private Apps
Shopify: check if the custom app is still installed
Send the new key to your data engineer — never paste it in Slack. Use a password manager or send via the agreed secure channel.
BigQuery quota exceeded
Symptom: Slack shows task: load_to_bigquery failed.
Fix: This is rare on the free tier. If it happens, the pipeline will retry automatically twice. If all retries fail, contact your data engineer.
dbt schema test failed
Symptom: Slack shows task: run_dbt failed.
What this means: One of your upstream APIs changed its data format, and a data quality check caught it before bad data reached your dashboard. This is the system working correctly — the dashboard may show yesterday's data until the issue is resolved.
Fix: Forward the Slack alert to your data engineer. They'll identify which API changed and update the connector.
Network timeout
Symptom: Slack shows any task failed with "timeout" in the error.
Fix: Usually self-resolving — the pipeline retries automatically. If you see the same task fail 3 days in a row, escalate to your data engineer.
***3. Dashboard is showing old data
If the pipeline succeeded (green Slack message) but the dashboard looks stale:
Hard-refresh your browser (Ctrl+Shift+R / Cmd+Shift+R).
The dashboard cache refreshes every 5 minutes — wait 5 minutes and reload.
If data is still missing after 30 minutes, contact your data engineer.
***4. What data is in the dashboard
| Metric | Source | Update frequency |
|---|---|---|
| Revenue, MRR | Stripe | Daily (previous day's data) |
| Customer counts, lifecycle stages | HubSpot | Daily |
| Order counts | Shopify | Daily |
| Channel attribution | Google Analytics | Daily |
| Email campaign metrics | Mailchimp | Daily |
Note: All metrics reflect data through the end of the previous day. Real-time revenue is not shown (this is by design — daily granularity is sufficient for the reporting use case).
***5. Common questions
Q: Can I change the date range shown in the dashboard?
A: Yes — use the date filter in the left sidebar of the Streamlit dashboard.
Q: Can I export the data to Excel?
A: Yes — every chart has a download button (top right of each chart). You can also query BigQuery directly using Google Sheets' BigQuery connector.
Q: Why does yesterday's data sometimes look slightly different from what I saw this morning?
A: Late-arriving data. Some APIs (especially Stripe for certain payment methods) finalise transactions 24–48 hours after the event. The pipeline processes any late data automatically on the next run.
Q: Can we add a new metric to the dashboard?
A: Yes. Share the business question with your data engineer and they'll build the SQL model and add it. Typical turnaround: 1–2 days.
***6. Emergency contacts
| Situation | Who to contact |
|---|---|
| Pipeline down > 2 days | Data engineer (garvnet358@gmail.com) |
| API credentials | Relevant platform admin |
| Dashboard down | Data engineer |
| Data looks wrong | Data engineer — include a screenshot and the date range |
***Last updated: May 2026 | Maintained by Garvpreet Singh