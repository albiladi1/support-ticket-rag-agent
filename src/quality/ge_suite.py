"""
Deliverable 5 (part of): Great Expectations quality gate.

Validates the Silver Delta table before it is allowed to flow into Gold /
the RAG index. On failure, raises so the Airflow DAG task fails loudly
instead of silently indexing bad data.

Uses the great_expectations==0.18.19 pandas-validator API.
"""
import great_expectations as gx
import pandas as pd


def validate_silver(silver_pdf: pd.DataFrame) -> bool:
    context = gx.get_context()
    validator = context.sources.pandas_default.read_dataframe(silver_pdf)

    validator.expect_column_values_to_not_be_null(column="ticket_id")
    validator.expect_column_values_to_be_unique(column="ticket_id")
    validator.expect_column_values_to_not_be_null(column="body")
    validator.expect_column_values_to_be_in_set(
        column="category", value_set=["account", "billing", "technical", "other"]
    )

    results = validator.validate()
    all_passed = results.success

    passed_count = sum(1 for r in results.results if r.success)
    total_count = len(results.results)
    print(f"[great_expectations] {'PASSED' if all_passed else 'FAILED'} "
          f"({passed_count}/{total_count} expectations met)")

    if not all_passed:
        raise ValueError("Silver zone failed data quality validation — pipeline halted.")

    return all_passed