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



class OutletPerformanceHarmonizer:
    def __init__(self,loadtype: str,runtype: str = 'full'):
        self.loadtype = loadtype
        self.runtype = runtype

    def harmonize(self,
                  spark: SparkSession,
                  dataframes: dict,
                  currentio: Optional[DataLakeIO]) -> DataFrame:
        PEAK_HOURS = [12, 13, 19, 20, 21]


        fact_sales_df = dataframes.get('fact_sales')
        fact_kitchen_df = dataframes.get('fact_kitchen')
        dim_outlet_df = dataframes.get('dim_outlet')

        combined_kitchen_sales_df = fact_sales_df.select(
            "order_id",
            "order_ts",
            "total_order_price",
            "order_type",
            "payment_mode",
            "high_value_flag",
            "total_order_price_anomaly_flag"
        ).join(
            fact_kitchen_df.select(
                "order_id",
                "outlet_id",
                "cooking_start",
                "cooking_end"
            ),
            on="order_id",
            how="inner"
        ).withColumn(
            "cooking_duration_mins",
            (unix_timestamp(col("cooking_end")) - unix_timestamp(col("cooking_start"))) / 60
        ).withColumn(
            "order_date",
            to_date('order_ts')
        ).withColumn(
            "hour_of_day",
            hour(col("order_ts"))
        ).withColumn(
            "is_peak_hour",
            when(col("hour_of_day").isin(PEAK_HOURS), True).otherwise(False)
        )

        total_agg = (
            combined_kitchen_sales_df.groupBy('outlet_id').agg(
                spark_sum("total_order_price").alias("total_sales"),
                count("order_id").alias("total_orders"),
                round(spark_avg("total_order_price"),2).alias("avg_order_value"),
                count(when(col("high_value_flag") == True, True)).alias("high_value_orders"),
                count(when(col("total_order_price_anomaly_flag") == True, True)).alias("anomalous_orders"),
                count(when(col('order_type') == 'Dine-In', True)).alias("dine_in_orders"),
                count(when(col('order_type') == 'Takeaway', True)).alias("takeaway_orders"),
                count(when(col('order_type') == 'Delivery', True)).alias("delivery_orders"),
                count(when(col('payment_mode') == 'UPI', True)).alias("upi_payments"),
                count(when(col('payment_mode') == 'Card', True)).alias("card_payments"),
                count(when(col('payment_mode') == 'Cash', True)).alias("cash_payments"),
                count(when(col('payment_mode') == 'Wallet', True)).alias("wallet_payments"),
                round(spark_avg("cooking_duration_mins"),2).alias("avg_cooking_duration_mins")
            ).withColumn(
                "high_value_order_pct",
                round((col("high_value_orders") / col("total_orders")) * 100, 2)
            ).withColumn(
                "anomalous_order_pct",
                round((col("anomalous_orders") / col("total_orders")) * 100, 2)
            ).withColumn(
                "dine_in_order_pct",
                round((col("dine_in_orders") / col("total_orders")) * 100, 2)
            ).withColumn(
                "takeaway_order_pct",
                round((col("takeaway_orders") / col("total_orders")) * 100, 2)
            ).withColumn(
                "delivery_order_pct",
                round((col("delivery_orders") / col("total_orders")) * 100, 2)
            ).withColumn(
                "upi_payment_pct",
                round((col("upi_payments") / col("total_orders")) * 100, 2)
            ).withColumn(
                    "card_payment_pct",
                    round((col("card_payments") / col("total_orders")) * 100, 2)
            ).withColumn(
                "cash_payment_pct",
                round((col("cash_payments") / col("total_orders")) * 100, 2)
            ).withColumn(
                "wallet_payment_pct",
                round((col("wallet_payments") / col("total_orders")) * 100, 2)
            )
        )


        daily_agg = (
            combined_kitchen_sales_df.groupBy('outlet_id','order_date').agg(
                spark_sum("total_order_price").alias("avg_daily_sales"),
                count("order_id").alias("daily_order_count")
            )
        )
        daily_counts = (
            daily_agg.groupBy('outlet_id').agg(
                round(spark_avg("avg_daily_sales"),2).alias("avg_daily_sales"),
                round(spark_avg("daily_order_count"),2).alias("avg_daily_order_count")
            )
        )

        peak_hour_agg = (
            combined_kitchen_sales_df
            .filter(col("is_peak_hour") == 1)
            .groupBy("outlet_id")
            .agg(
                spark_sum("total_order_price").alias("peak_hour_sales"),
                count("order_id").alias("peak_hour_order_count")
            )
        )

        slot_agg = (
            combined_kitchen_sales_df.groupBy("outlet_id").agg(
                spark_sum(
                    when((col("hour_of_day") >= 8) & (col("hour_of_day") < 12), col("total_order_price")).otherwise(0)
                ).alias("morning_sales"),
                spark_sum(
                    when((col("hour_of_day") >= 12) & (col("hour_of_day") < 16), col("total_order_price")).otherwise(0)
                ).alias("afternoon_sales"),
                spark_sum(
                    when((col("hour_of_day") >= 16) & (col("hour_of_day") < 20), col("total_order_price")).otherwise(0)
                ).alias("evening_sales"),
                spark_sum(
                    when((col("hour_of_day") >= 20) & (col("hour_of_day") < 24), col("total_order_price")).otherwise(0)
                ).alias("night_sales"),
                count(
                    when((col("hour_of_day") >= 8) & (col("hour_of_day") < 12), True)
                ).alias("morning_orders"),
                count(
                    when((col("hour_of_day") >= 12) & (col("hour_of_day") < 16), True)
                ).alias("afternoon_orders"),
                count(
                    when((col("hour_of_day") >= 16) & (col("hour_of_day") < 20), True)
                ).alias("evening_orders"),
                count(
                    when((col("hour_of_day") >= 20) & (col("hour_of_day") < 24), True)
                ).alias("night_orders")
            )
        )
        return dim_outlet_df.join(
            total_agg,
            on="outlet_id",
            how="left"
        ).join(
            daily_counts,
            on="outlet_id",
            how="left"
        ).join(
            peak_hour_agg,
            on="outlet_id",
            how="left"
        ).join(
            slot_agg,
            on="outlet_id",
            how="left"
        ).withColumn(
            "ingested_at",
            current_timestamp()
        )


class Harmonizer:
    _harmonizer_map = {
        "outlet_performance": OutletPerformanceHarmonizer
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