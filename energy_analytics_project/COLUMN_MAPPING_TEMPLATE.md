# Column mapping template

After running notebook 00, fill in the exact source column names:

| Logical field | Energy CSV column | Peak CSV column | Shortage CSV column |
|---|---|---|---|
| Date |  |  |  |
| State |  |  |  |
| Metric value | Energy met (MU) | Peak met (MW) | Peak unmet (MW) |

Then update `COLUMN_MAP` in `databricks/02_silver_transform.py`.

Check that all date columns represent the same calendar date and state names use consistent labels.
