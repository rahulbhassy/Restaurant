from typing import List, Optional
from pyspark.sql import *
from pyspark.sql.functions import create_map, lit, coalesce, when, round, datediff
from pyspark.sql.functions import col, unix_timestamp , avg , count , year , to_date , weekofyear
from pyspark.sql import DataFrame, SparkSession
from Shared.FileIO import DataLakeIO
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    ArrayType
)

from Shared.DataLoader import DataLoader
from Shared.DataWriter import DataWriter
from pyspark.sql.types import StructType
from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import from_json, explode, col
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, to_date, hour, weekofyear, month, year, date_format,
    when, sum as spark_sum, avg as spark_avg, max as spark_max, min as spark_min,
    countDistinct, size, count, lit, coalesce
)
from Shared.FileIO import WindowHelper
from pyspark.sql.functions import (
    to_date, hour, weekofyear, month, year, dayofweek,
    when, lit, col , row_number
)
class CustomerHarmonizer:
    def __init__(self,loadtype: str,runtype: str = 'full'):
        self.loadtype = loadtype
        self.runtype = runtype

    def harmonize(self,
        spark: SparkSession,
        dataframes: dict,
        currentio: Optional[DataLakeIO]
    ) -> DataFrame:
        fact_sales_df = dataframes.get('fact_sales')
        dim_customer_df = dataframes.get('dim_customer')
        item_df = dataframes.get('fact_sales_items')
        ingested_at = current_timestamp()

        item_agg = item_df.groupBy("customer_id","order_id").agg(
            spark_sum("quantity").alias("items_in_order")
        )

        # Join fact_sales with dim_customer to get enriched customer data
        customer_enriched_df = fact_sales_df.join(
            dim_customer_df,
            fact_sales_df.customer_id == dim_customer_df.customer_id,
            how='left'
        ).join(
            item_agg,
            fact_sales_df.order_id == item_agg.order_id,
            how='left'
        ).select(
            fact_sales_df['*'],
            dim_customer_df.customer_id,
            dim_customer_df.customer_mobile,
            item_agg.items_in_order
        ).drop(
            fact_sales_df.customer_id,
            fact_sales_df.ingested_at
        )


        customer_agg = customer_enriched_df.groupBy("customer_id",).agg(
            count("order_id").alias("total_orders"),
            round(spark_avg("total_order_price"),2).alias("average_order_value"),
            spark_max("total_order_price").alias("max_order_value"),
            spark_min("total_order_price").alias("min_order_value"),
            spark_sum("total_order_price").alias("total_spent"),
            spark_min("order_ts").alias("first_order_date"),
            spark_max("order_ts").alias("last_order_date"),
            datediff(spark_max("order_ts"), spark_min("order_ts")).alias("days_as_customer"),
            datediff(current_timestamp(), spark_max("order_ts")).alias("days_since_last_order"),
            spark_sum("items_in_order").alias("total_items_ordered")
        ).withColumn(
            "is_repeat_customer",
            (col("total_orders") > 1)
        ).withColumn(
            "is_active_customer",
            when(col("days_since_last_order") <= 90, lit(True)).otherwise(lit(False))
        ).withColumn(
            "is_high_value_customer",
            when(col("total_spent") >= 5000, lit(True)).otherwise(lit(False))
        ).withColumn(
            "customer_segment",
            when(col("total_spent") >= 10000, "Gold")
            .when(col("total_spent") >= 3000, "Silver")
            .when(col("total_spent") >= 500, "Bronze")
            .otherwise("New")
        ).withColumn(
            "customer_frequency_band",
            when(col("total_orders") >= 50, "High")
            .when(col("total_orders") >= 10, "Medium")
            .when(col("total_orders") >= 1, "Low")
            .otherwise("None")
        ).withColumn(
            "items_per_order",
            round(col("total_items_ordered") / col("total_orders"),2)
        )

        return customer_agg.withColumn(
            "ingested_at",
            ingested_at
        )

class CustomerPreferenceHarmonizer:
    def __init__(self,loadtype: str,runtype: str = 'full'):
        self.loadtype = loadtype
        self.runtype = runtype

    def harmonize(self,
        spark: SparkSession,
        dataframes: dict,
        currentio: Optional[DataLakeIO]
    ) -> DataFrame:
        # Placeholder for actual harmonization logic
        fact_sales_df = dataframes.get('fact_sales')
        fact_sales_items_df = dataframes.get('fact_sales_items')
        dim_customer_df = dataframes.get('dim_customer')
        dim_item_df = dataframes.get('dim_item')
        dim_outlet_df = dataframes.get('dim_outlet')

        ingested_at = current_timestamp()

        window = WindowHelper()
        w = window.create_window_spec(partition_by_cols=["customer_id"])

        items_agg = (
            fact_sales_items_df
            .groupBy("customer_id", "item_id")
            .agg(
                spark_sum("quantity").alias("items_in_order"),
                spark_sum("line_total").alias("order_items_line_total"),
                spark_sum(size(col("toppings"))).alias("toppings_count_in_order")
            )
            .withColumn("rn_q", row_number().over(w.orderBy(col("items_in_order").desc())))
            .withColumn("rn_lt", row_number().over(w.orderBy(col("order_items_line_total").desc())))
            .withColumn("rn_tc", row_number().over(w.orderBy(col("toppings_count_in_order").desc())))
            .withColumn(
                "preference_priority",
                when(col("rn_q") == 1, 1)
                .when(col("rn_lt") == 1, 2)
                .when(col("rn_tc") == 1, 3)
                .otherwise(99)
            )
            .withColumn(
                "final_rank",
                row_number().over(
                    w.orderBy(
                        col("preference_priority"),
                        col("items_in_order").desc(),
                        col("order_items_line_total").desc(),
                        col("toppings_count_in_order").desc()
                    )
                )
            ).filter(col("final_rank") == 1)
            .join(
                dim_item_df,
                on="item_id",
                how="left"
            )
            .select(
                "customer_id",
                "item_id",
                dim_item_df.item_name.alias('favorite_item_name')

            )
        )


        outlet_agg = (
            fact_sales_df.groupBy("customer_id", "outlet_id").agg(
                count("order_id").alias("orders_at_outlet")
            ).withColumn(
                "rnk",
                row_number().over(
                    w.orderBy(col("orders_at_outlet").desc())
                )
            ).filter(
                col("rnk") == 1
            ).join(
                dim_outlet_df,
                on="outlet_id",
                how="left"
            ).select(
                "customer_id",
                "outlet_id",
                dim_outlet_df.outlet_name.alias("favorite_outlet_name"),
                "orders_at_outlet"
            )
        )

        payment_agg = (
            fact_sales_df.groupBy("customer_id", "payment_mode").agg(
                count("order_id").alias("orders_by_payment_method")
            ).withColumn(
                "rnk",
                row_number().over(
                    w.orderBy(col("orders_by_payment_method").desc())
                )
            ).filter(
                col("rnk") == 1
            ).select(
                "customer_id",
                col("payment_mode").alias('preferred_payment_mode'),
                "orders_by_payment_method"
            )
        )

        from pyspark.sql.functions import date_format

        # create day name and hour
        sales_time = fact_sales_df.withColumn("order_day", date_format(col("order_ts"), "E")) \
            .withColumn("order_hour", hour(col("order_ts")))

        # favorite day
        day_counts = sales_time.groupBy("customer_id", "order_day").agg(count("order_id").alias("day_count"))
        w_day = w.orderBy(col("day_count").desc())
        favorite_day = day_counts.withColumn("rn", row_number().over(w_day)).filter(col("rn") == 1).select(
            "customer_id", col("order_day").alias("favorite_order_day"))

        # favorite hour
        hour_counts = sales_time.groupBy("customer_id", "order_hour").agg(count("order_id").alias("hour_count"))
        w_hour = w.orderBy(col("hour_count").desc())
        favorite_hour = hour_counts.withColumn("rn", row_number().over(w_hour)).filter(col("rn") == 1).select(
            "customer_id", col("order_hour").alias("favorite_order_hour"))

        # weekend ratio -> use dayofweek (Spark: Sunday=1, Saturday=7)
        sales_time_num = fact_sales_df.withColumn("dow_num", dayofweek(col("order_ts"))) \
            .withColumn("is_weekend", when(col("dow_num").isin(1, 7), 1).otherwise(0))

        weekend_ratio = (
            sales_time_num.groupBy("customer_id")
            .agg(spark_sum("is_weekend").alias("weekend_orders"),
                 count("order_id").alias("total_orders_for_weekend_ratio"))
            .withColumn("weekend_order_ratio", when(col("total_orders_for_weekend_ratio") > 0,
                                                    col("weekend_orders") / col(
                                                        "total_orders_for_weekend_ratio")).otherwise(lit(0.0)))
            .select("customer_id", round(col("weekend_order_ratio"),2).alias("weekend_order_ratio"))
        )

        order_type_counts = (
            fact_sales_df
            .groupBy("customer_id", "order_type")
            .agg(count("order_id").alias("ot_count"))
        )
        w_ot = w.orderBy(col("ot_count").desc())
        preferred_order_type = order_type_counts.withColumn("rn", row_number().over(w_ot)).filter(
            col("rn") == 1).select("customer_id", col("order_type").alias("preferred_order_type"))

        return dim_customer_df.join(
            items_agg,
            on="customer_id",
            how="left"
        ).join(
            outlet_agg,
            on="customer_id",
            how="left"
        ).join(
            payment_agg,
            on="customer_id",
            how="left"
        ).join(
            favorite_day,
            on="customer_id",
            how="left"
        ).join(
            favorite_hour,
            on="customer_id",
            how="left"
        ).join(
            weekend_ratio,
            on="customer_id",
            how="left"
        ).join(
            preferred_order_type,
            on="customer_id",
            how="left"
        ).withColumn(
            "ingested_at",
            ingested_at
        )


class ChefPerformanceHarmonizer:
    def __init__(self,loadtype: str,runtype: str = 'full'):
        self.loadtype = loadtype
        self.runtype = runtype

    def harmonize(self,
                  spark: SparkSession,
                  dataframes: dict,
                  currentio: Optional[DataLakeIO]
                  ):
        fact_kitchen_df = dataframes.get('fact_kitchen')
        dim_chef = dataframes.get('dim_chef')
        dim_outlet = dataframes.get('dim_outlet')
        fact_sales_df = dataframes.get('fact_sales')

        kitchen_agg = (
            fact_kitchen_df
            .withColumn("cooking_start", to_timestamp(col("cooking_start")))
            .withColumn("cooking_end", to_timestamp(col("cooking_end")))
            .withColumn("cooking_time_sec",
                        (col("cooking_end").cast("long") - col("cooking_start").cast("long")))
            .withColumn("cooking_date", col("cooking_start").cast("date"))
            .withColumn("cooking_hour", hour(col("cooking_start")))
        )

        # total orders cooked
        total_orders_agg = (
            kitchen_agg.groupBy("chef_id")
            .agg(count("order_id").alias("total_orders_cooked"))
        )

        # total *distinct* working days
        active_days = (
            kitchen_agg.groupBy("chef_id")
            .agg(countDistinct("cooking_date").alias("total_days"))
        )

        # average orders per day
        orders_per_day = (
            total_orders_agg.join(active_days, "chef_id")
            .withColumn(
                "avg_orders_per_day",
                col("total_orders_cooked") / col("total_days")
            )
        )


        cooking_stats = (
            kitchen_agg.groupBy("chef_id")
            .agg(
                avg("cooking_time_sec").alias("avg_cooking_time_sec"),
                spark_min("cooking_time_sec").alias("min_cooking_time_sec"),
                spark_max("cooking_time_sec").alias("max_cooking_time_sec")
            )
        )


        # Peak hours = 12–1 PM (12,13) & 7–9 PM (19,20,21)
        peak_hours = [12, 13, 19, 20, 21]

        peak_order_count = (
            kitchen_agg.withColumn(
                "is_peak_hour",
                when(col("cooking_hour").isin(peak_hours), 1).otherwise(0)
            )
            .groupBy("chef_id")
            .agg(
                count(when(col("is_peak_hour") == 1, True)).alias("peak_hour_orders")
            )
        )
        combined_sales_chef = (
            fact_sales_df.select("order_id", "total_order_price")
            .join(
                fact_kitchen_df.select("order_id", "chef_id"),
                on=["order_id"],
                how="inner"
            )
        )
        sales_agg = (
            combined_sales_chef.groupBy("chef_id").agg(
                spark_sum("total_order_price").alias("total_sales_value_cooked_by_chef")
            )
        )
        perf = (
            orders_per_day
            .join(cooking_stats, "chef_id", "left")
            .join(peak_order_count, "chef_id", "left")
            .join(sales_agg, "chef_id", "left")
            .fillna({"peak_hour_orders": 0})
        )

        perf = (
            perf.join(dim_chef.select("chef_id", "chef_name", "outlet_id"), "chef_id", "left")
            .join(dim_outlet.select("outlet_id", "outlet_name"), "outlet_id", "left")
        )

        perf = (
            perf.withColumn("ingested_at", current_timestamp())
        )

        # Final selection (clean ordering)
        final_cols = [
            "chef_id", "chef_name", "outlet_id", "outlet_name",
            "total_orders_cooked", "avg_orders_per_day",
            "avg_cooking_time_sec", "min_cooking_time_sec", "max_cooking_time_sec",
            "peak_hour_orders","ingested_at" , "total_sales_value_cooked_by_chef"
        ]

        return perf.select(*final_cols)

class Harmonizer:
    _harmonizer_map = {
        "customer": CustomerHarmonizer,
        "customer_preference": CustomerPreferenceHarmonizer,
        "chef_performance": ChefPerformanceHarmonizer
    }

    def __init__(self,table,loadtype: str,runtype: str = 'full'):
        self.table = table
        self.loadtype = loadtype
        self.runtype = runtype
        self.harmonizer_class = self._harmonizer_map.get(table)

        if not self.harmonizer_class:
            raise ValueError(f"No harmonizer found for source: {table}")
        self.harmonizer_instance = self.harmonizer_class(
            loadtype=self.loadtype,
            runtype=self.runtype
        )

    def harmonize(self,spark: SparkSession ,dataframes: dict, currentio: Optional[DataLakeIO]) -> DataFrame:
        """Instance method to harmonize data using the selected harmonizer"""
        return self.harmonizer_instance.harmonize(
            spark=spark,
            dataframes=dataframes,
            currentio=currentio
        )