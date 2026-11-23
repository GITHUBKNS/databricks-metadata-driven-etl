-- Replace YOUR_CATALOG with your catalog name before saving in Databricks SQL.
SELECT pipeline_id
FROM YOUR_CATALOG.md_framework.pipeline_config
WHERE active = true
ORDER BY pipeline_id;
