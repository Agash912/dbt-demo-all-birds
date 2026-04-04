import snowflake.snowpark.functions as F


def model(dbt, session):
    """
    Adds a customer_segment column based on spend and order frequency.
    Reads from the SQL intermediate model and applies rule-based segmentation.

    This runs natively on Snowflake via Snowpark, so no data leaves the warehouse.
    Python models slot into the DAG the same way SQL models do. Downstream SQL models
    can ref() this just like any other model.

    Segments:
        champion = high spend and frequent buyer
        loyal    = moderate-to-high spend
        new      = only one order so far
        casual   = everyone else

    This could be done in SQL with CASE WHEN. We use Python here to show the
    capability for teams that need more complex logic later (ML scoring,
    statistical thresholds, library-based clustering).
    """

    customer_orders = dbt.ref("int_customer_order_summary")

    segmented = customer_orders.with_column(
        "CUSTOMER_SEGMENT",
        F.when(
            (F.col("LIFETIME_SPEND") > 100) & (F.col("ORDER_COUNT") > 3),
            F.lit("champion"),
        )
        .when(F.col("LIFETIME_SPEND") > 50, F.lit("loyal"))
        .when(F.col("ORDER_COUNT") == 1, F.lit("new"))
        .otherwise(F.lit("casual")),
    )

    return segmented
