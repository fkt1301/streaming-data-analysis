"""
Data quality rules applied to each micro-batch before it's written to
Postgres. Records failing any rule are routed to the DLQ instead of
silently passing through or crashing the whole streaming job.
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, array, array_except, lit, when

EMAIL_REGEX = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"


def add_validation_errors(df: DataFrame) -> DataFrame:
    """
    Adds a `validation_errors` array column: empty array if the row is
    clean, otherwise one string per failed rule. Doesn't drop anything --
    splitting valid/invalid is a separate, explicit step.
    """
    checks = [
        (col("id").isNull() | (col("id") == ""), "missing_id"),
        (col("email").isNull() | (~col("email").rlike(EMAIL_REGEX)), "invalid_email"),
        (col("username").isNull() | (col("username") == ""), "missing_username"),
        (col("country").isNull() | (col("country") == ""), "missing_country"),
        (col("registered_date") > current_timestamp(), "registered_date_in_future"),
    ]

    error_array = array(
        *[when(condition, lit(label)) for condition, label in checks]
    )
    # array() with `when(...)` that doesn't match produces NULLs; strip them out.
    return df.withColumn(
        "validation_errors", array_except(error_array, array(lit(None).cast("string")))
    )


def split_valid_invalid(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    validated = add_validation_errors(df)
    valid = validated.filter(col("validation_errors").getItem(0).isNull()).drop(
        "validation_errors"
    )
    invalid = validated.filter(col("validation_errors").getItem(0).isNotNull())
    return valid, invalid
