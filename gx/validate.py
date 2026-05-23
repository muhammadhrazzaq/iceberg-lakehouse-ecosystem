# gx/validate.py
import great_expectations as gx
from pyiceberg.catalog import load_catalog
import pandas as pd
import sys
import os

# ── Load data from Iceberg ────────────────────────────────
print("Loading data from Iceberg...")
catalog = load_catalog("nexus", **{
    "type":      "rest",
    "uri":       "http://localhost:8181/catalog",
    "warehouse": "nexus",
    "token":     "dummy",
})
df = catalog.load_table("github.events").scan().to_arrow().to_pandas()
print(f"  ✓ Loaded {len(df)} rows")

# ── GX context ────────────────────────────────────────────
context = gx.get_context()

# ── Add datasource ────────────────────────────────────────
datasource = context.sources.add_or_update_pandas("iceberg_source")
asset      = datasource.add_dataframe_asset("github_events")
batch_req  = asset.build_batch_request(dataframe=df)

# ── Add expectation suite ─────────────────────────────────
suite_name = "github_events_suite"
suite = context.add_or_update_expectation_suite(suite_name)

validator = context.get_validator(
    batch_request=batch_req,
    expectation_suite_name=suite_name,
)

# ── Completeness ──────────────────────────────────────────
print("Adding expectations...")
validator.expect_column_values_to_not_be_null("event_id")
validator.expect_column_values_to_not_be_null("event_type")
validator.expect_column_values_to_not_be_null("repo_name")
validator.expect_column_values_to_not_be_null("actor_login")
validator.expect_column_values_to_not_be_null("created_at")

# ── Uniqueness ────────────────────────────────────────────
validator.expect_column_values_to_be_unique("event_id")

# ── Validity ──────────────────────────────────────────────
validator.expect_column_values_to_be_in_set(
    "event_type",
    ["PushEvent", "PullRequestEvent", "IssuesEvent", "WatchEvent", "ForkEvent"]
)

validator.expect_column_values_to_match_regex(
    "repo_name",
    r"^[a-zA-Z0-9._-]+/[a-zA-Z0-9._-]+$",
)

validator.expect_column_values_to_be_between(
    "stars",
    min_value=0,
    max_value=1_000_000,
    mostly=0.99,
)

# ── Volume ────────────────────────────────────────────────
validator.expect_table_row_count_to_be_between(
    min_value=1,
    max_value=100_000_000,
)

# ── Schema ────────────────────────────────────────────────
validator.expect_table_columns_to_match_set(
    {"event_id", "event_type", "repo_name", "actor_login", "created_at", "stars"}
)

# ── Save suite ────────────────────────────────────────────
validator.save_expectation_suite()
print("  ✓ Expectation suite saved")

# ── Add checkpoint ────────────────────────────────────────
checkpoint = context.add_or_update_checkpoint(
    name="iceberg_checkpoint",
    validations=[{
        "batch_request": batch_req,
        "expectation_suite_name": suite_name,
    }],
)

# ── Run validation ────────────────────────────────────────
print("Running validation...")
result = checkpoint.run()

# ── Print results ─────────────────────────────────────────
print()
if result.success:
    print("✅ All expectations passed")
else:
    print("❌ Validation FAILED")

total  = 0
passed = 0
failed = 0

for run_result in result.run_results.values():
    for er in run_result["validation_result"]["results"]:
        total += 1
        col = er["expectation_config"]["kwargs"].get("column", "table")
        exp = er["expectation_config"]["expectation_type"]
        if er["success"]:
            passed += 1
            print(f"  ✅ {col:20} {exp}")
        else:
            failed += 1
            print(f"  ❌ {col:20} {exp}")
            print(f"     → {er['result']}")

print()
print(f"Results: {passed}/{total} passed, {failed} failed")

# ── Build data docs ───────────────────────────────────────
context.build_data_docs()
print()
print("Data docs built — open: gx/uncommitted/data_docs/local_site/index.html")

# ── Exit with error if failed ────────────────────────────
if not result.success:
    sys.exit(1)