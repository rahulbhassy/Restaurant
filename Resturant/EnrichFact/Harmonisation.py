from typing import List, Optional
from pyspark.sql import *
from pyspark.sql.functions import create_map, lit, coalesce , when ,round
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
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    to_date, hour, weekofyear, month, year, dayofweek,
    when, lit, col
)


class FactItems:
    def __init__(self,loadtype: str,spark: SparkSession,runtype: str = 'prod'):
        self.loadtype = loadtype
        self.runtype = runtype
        self.spark = spark
        self.srcio = DataLakeIO(
            process="read",
            table='fact_sales',
            state='current',
            layer='raw',
            loadtype=self.loadtype,
            runtype=self.runtype
        )
        self.tgtio = DataLakeIO(
            process="read",
            table='fact_sales_items',
            state='current',
            layer='raw',
            loadtype=self.loadtype,
            runtype=self.runtype
        )

    def _schema(self):
        order_menu_detail_schema = ArrayType(StructType([
            StructField("item_id", StringType(), True),
            StructField("item_name", StringType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("toppings", ArrayType(StringType()), True),
            StructField("unit_price", DoubleType(), True),
            StructField("line_total", DoubleType(), True)
        ]))
        return order_menu_detail_schema

    def _loader(self):
        loader = DataLoader(
            path=self.srcio.filepath(),
            filetype='delta',
            loadtype=self.loadtype
        )
        return loader.LoadData(self.spark)

    def _writer(self,df: DataFrame):
        writer = DataWriter(
            loadtype=self.loadtype,
            path=self.tgtio.filepath(),
            format='delta',
            spark=self.spark
        )
        writer.WriteData(df=df)

    def create_fact_items(self):
        df = self._loader()
        order_menu_detail_schema = self._schema()
        df_parsed = df.withColumn(
            "menu_items",
            from_json(col("order_menu_details"), order_menu_detail_schema)
        )
        df_exploded = df_parsed.withColumn("menu_item", explode("menu_items"))
        df_flat = df_exploded.select(
            "order_id",
            "order_ts",
            "outlet_id",
            "customer_id",
            col("menu_item.item_id").alias("item_id"),
            col("menu_item.item_name").alias("item_name"),
            col("menu_item.quantity").alias("quantity"),
            col("menu_item.unit_price").alias("unit_price"),
            col("menu_item.line_total").alias("line_total"),
            col("menu_item.toppings").alias("toppings")
        )
        df_flat = df_flat.withColumn("ingested_at", current_timestamp())
        self._writer(df_flat)
        print("Fact Items created and written successfully.")

class FactSalesEnricher:
    def __init__(self, runtype: str , loadtype: str):
        """
        :param spark: SparkSession
        :param cook_sla_seconds: SLA threshold for cooking time in seconds (default 600s = 10min)
        """

        self.cook_sla_seconds = 600
        self.runtype = runtype
        self.loadtype = loadtype

    def _time_dims(self, df: DataFrame, ts_col: str = "order_ts") -> DataFrame:
        df = df.withColumn(ts_col, to_timestamp(col(ts_col)))
        return (
            df
            .withColumn("order_date", to_date(col(ts_col)))
            .withColumn("order_hour", hour(col(ts_col)))
            .withColumn("order_week", weekofyear(col(ts_col)))
            .withColumn("order_month", month(col(ts_col)))
            .withColumn("order_year", year(col(ts_col)))

            # Spark 3.x safe replacement for date_format(..., "E")
            .withColumn("day_of_week_num", dayofweek(col(ts_col)))
            .withColumn(
                "day_of_week",
                when(col("day_of_week_num") == 1, "Sunday")
                .when(col("day_of_week_num") == 2, "Monday")
                .when(col("day_of_week_num") == 3, "Tuesday")
                .when(col("day_of_week_num") == 4, "Wednesday")
                .when(col("day_of_week_num") == 5, "Thursday")
                .when(col("day_of_week_num") == 6, "Friday")
                .otherwise("Saturday")
            )

            # Weekend flag: Sunday (1) & Saturday (7)
            .withColumn(
                "is_weekend",
                when(col("day_of_week_num").isin(1, 7), lit(True))
                .otherwise(lit(False))
            )

            # Peak hour
            .withColumn(
                "is_peak_hour",
                when(col("order_hour").isin(12, 13, 19, 20), lit(True))
                .otherwise(lit(False))
            )
        )

    def _aggregate_items(self, df_items: DataFrame) -> DataFrame:
        """
        Aggregates fact_sales_items to order-level metrics:
          total_items, distinct_items_count, total_toppings_count,
          avg_item_price, max_item_price, min_item_price, items_with_customizations
        """
        # safe columns: quantity, unit_price, line_total, toppings
        agg = (
            df_items
            .groupBy("order_id")
            .agg(
                spark_sum(col("quantity")).alias("total_items"),
                countDistinct(col("item_id")).alias("distinct_items_count"),
                spark_sum(size(col("toppings"))).alias("total_toppings_count"),
                (spark_sum(col("line_total")) / spark_sum(col("quantity"))).alias("avg_item_price_per_unit"), # fallback
                spark_max(col("unit_price")).alias("max_item_price"),
                spark_min(col("unit_price")).alias("min_item_price"),
                count( when( size(col("toppings")) > 0 , True) ).alias("items_with_customizations")
            )
        )

        # derive is_customized_order boolean
        agg = agg.withColumn("is_customized_order", when(col("items_with_customizations") > 0, lit(True)).otherwise(lit(False)))

        # defensive null fills
        fill_cols = {
            "total_items": 0,
            "distinct_items_count": 0,
            "total_toppings_count": 0,
            "avg_item_price_per_unit": 0.0,
            "max_item_price": 0.0,
            "min_item_price": 0.0,
            "items_with_customizations": 0,
            "is_customized_order": False
        }
        for c, v in fill_cols.items():
            agg = agg.withColumn(c, coalesce(col(c), lit(v)))

        return agg

    def _customer_metrics(self, df_sales: DataFrame) -> DataFrame:
        """
        Compute customer-level rolling metrics using window functions:
          total_orders_by_customer (lifetime),
          lifetime_value (sum total_order_price),
          days_since_last_order (per order)
        Note: This uses df_sales itself (historical). If you have a full history
        table, pass that instead.
        """
        # lifetime sums / counts per customer (global/lifetime)
        cust_agg = (
            df_sales
            .groupBy("customer_id")
            .agg(
                count(col("order_id")).alias("total_orders_by_customer"),
                spark_sum(col("total_order_price")).alias("customer_lifetime_value"),
                spark_max(col("order_ts")).alias("last_order_ts")   # latest timestamp
            )
        )

        # days since last order per order: join later using last_order_ts (we compute recency relative to last_order_ts before current order)
        return cust_agg

    def _kitchen_flags(self, df_kitchen: DataFrame) -> DataFrame:
        """
        Optionally compute order-level kitchen flags: cook_duration_seconds and was_delayed_order
        Expects df_kitchen to have cooking_start and cooking_end as timestamps.
        """
        if df_kitchen is None:
            return None

        df = df_kitchen.withColumn("cooking_start", to_timestamp(col("cooking_start"))) \
                       .withColumn("cooking_end", to_timestamp(col("cooking_end")))

        df = df.withColumn("cook_duration_seconds", (col("cooking_end").cast("long") - col("cooking_start").cast("long")))
        df = df.withColumn("was_delayed_order", when(col("cook_duration_seconds") > self.cook_sla_seconds, lit(True)).otherwise(lit(False)))

        # pick max cook_duration per order (if multiple chefs)
        agg = (
            df.groupBy("order_id")
              .agg(
                  spark_max(col("cook_duration_seconds")).alias("cook_duration_seconds"),
                  (spark_sum( when(col("was_delayed_order"), 1).otherwise(0) ) > 0).alias("was_delayed_order")
              )
        )
        return agg

    def harmonize(self,
               spark: SparkSession,
               dataframes: dict,
               currentio: Optional[DataLakeIO]
               ) -> DataFrame:
        """
        Main entry:
          df_sales        -> fact_sales (order header)
          df_items        -> fact_sales_items (item-level exploded)
          dim_customer    -> dim_customer
          dim_outlet      -> dim_outlet
          df_kitchen      -> fact_kitchen (optional)
        Returns fact_sales_enriched DataFrame
        """
        df_sales = dataframes['fact_sales']
        df_items = dataframes['fact_sales_items']
        dim_customer = dataframes['dim_customer']
        dim_outlet = dataframes['dim_outlet']
        df_kitchen = dataframes.get('fact_kitchen', None)  # optional

        # 1) ensure time dims on sales
        df_sales = self._time_dims(df_sales, ts_col="order_ts")

        # 2) aggregate item metrics to order-level
        items_agg = self._aggregate_items(df_items)

        # 3) optional kitchen flags
        kitchen_agg = self._kitchen_flags(df_kitchen) if df_kitchen is not None else None

        # 4) customer lifetime metrics (per customer)
        cust_agg = self._customer_metrics(df_sales)

        # 5) Build enriched order-level DF by left-joining aggregates and dims
        enriched = (
            df_sales
            .join(items_agg, on="order_id", how="left")
            .join(dim_customer.select("customer_id", "customer_mobile"), on="customer_id", how="left")
            .join(dim_outlet.select("outlet_id", "outlet_name"), on="outlet_id", how="left")
            .join(cust_agg, on="customer_id", how="left")
        )

        if kitchen_agg is not None:
            enriched = enriched.join(kitchen_agg, on="order_id", how="left")
        else:
            # add placeholder columns to keep schema stable
            enriched = enriched.withColumn("cook_duration_seconds", coalesce(col("cook_duration_seconds"), lit(None))) \
                               .withColumn("was_delayed_order", coalesce(col("was_delayed_order"), lit(False)))

        # 6) Derived fields, flags & bands
        enriched = (
            enriched
            .withColumn("total_items", coalesce(col("total_items"), lit(0)))
            .withColumn("distinct_items_count", coalesce(col("distinct_items_count"), lit(0)))
            .withColumn("total_toppings_count", coalesce(col("total_toppings_count"), lit(0)))
            .withColumn("avg_item_price", when(col("total_items") > 0, col("total_order_price") / col("total_items")).otherwise(lit(0.0)))
            .withColumn("is_multi_item_order", when(col("total_items") > 1, lit(True)).otherwise(lit(False)))
            .withColumn("is_high_value_order", when(col("total_order_price") > 500, lit(True)).otherwise(lit(False)))
            .withColumn("is_digital_payment", when(col("payment_mode").isin("UPI", "Card", "Wallet"), lit(True)).otherwise(lit(False)))
            .withColumn("order_value_band", when(col("total_order_price") < 200, lit("Low"))
                                         .when((col("total_order_price") >= 200) & (col("total_order_price") < 500), lit("Medium"))
                                         .otherwise(lit("High")))
        )

        # 7) customer recency & repeat / first order flags:
        # total_orders_by_customer and customer_lifetime_value are from cust_agg
        enriched = (
            enriched
            .withColumn("is_repeat_customer", when(col("total_orders_by_customer") > 1, lit(True)).otherwise(lit(False)))
        )

        # days_since_last_order: difference between this order_ts and last_order_ts
        enriched = enriched.withColumn("days_since_last_order",
                                       when(col("last_order_ts").isNotNull(),
                                            (to_date(col("order_ts")).cast("long") - to_date(col("last_order_ts")).cast("long"))
                                           ).otherwise(lit(None))
                                      )

        # 8) Final selection / order of columns (recommended)
        final_cols = [
            "order_id", "order_ts", "order_date", "order_hour", "order_week", "order_month", "order_year", "day_of_week", "is_weekend", "is_peak_hour",
            "customer_id", "customer_mobile", "is_repeat_customer", "total_orders_by_customer", "customer_lifetime_value", "days_since_last_order",
            "outlet_id", "outlet_name",
            "total_order_price", "total_items", "distinct_items_count", "avg_item_price", "max_item_price", "min_item_price",
            "total_toppings_count", "items_with_customizations", "is_customized_order",
            "payment_mode", "is_digital_payment", "order_type", "order_value_band", "is_multi_item_order", "is_high_value_order",
            "cook_duration_seconds", "was_delayed_order"
        ]

        # keep only existing columns (safe)
        final_cols = [c for c in final_cols if c in enriched.columns]

        fact_sales_enriched = enriched.select(*final_cols)
        fact_sales_enriched = fact_sales_enriched.withColumn("ingested_at", current_timestamp())
        return fact_sales_enriched

class FactKitchenEnricher:

    def __init__(self, runtype: str, loadtype: str):
        """
        Minimal enrichment for fact_kitchen:
        - cooking time
        - SLA delay flag
        - join dims
        """
        self.runtype = runtype
        self.loadtype = loadtype
        self.sla_seconds = 600

    def _time_dims(self, df: DataFrame, ts_col: str):

        df = df.withColumn(ts_col, to_timestamp(col(ts_col)))

        return (
            df
            .withColumn("date", to_date(col(ts_col)))
            .withColumn("hour", hour(col(ts_col)))
            .withColumn("week", weekofyear(col(ts_col)))
            .withColumn("month", month(col(ts_col)))
            .withColumn("year", year(col(ts_col)))
            .withColumn("day_of_week_num", dayofweek(col(ts_col)))
        )

    def harmonize(
        self,
        spark: SparkSession,
        dataframes: dict,
        currentio: Optional[DataLakeIO]
    ) -> DataFrame:

        df_kitchen = dataframes['fact_kitchen']
        dim_chef = dataframes['dim_chef']
        dim_outlet = dataframes['dim_outlet']
        df = (
            df_kitchen
            .withColumn("cooking_start", to_timestamp(col("cooking_start")))
            .withColumn("cooking_end", to_timestamp(col("cooking_end")))
        )

        # -----------------------------------------
        # 2. Compute cooking duration in seconds
        # -----------------------------------------
        df = df.withColumn(
            "cook_duration_seconds",
            col("cooking_end").cast("long") - col("cooking_start").cast("long")
        )

        # -----------------------------------------
        # 3. SLA flag (delayed if duration > threshold)
        # -----------------------------------------
        df = df.withColumn(
            "was_delayed_order",
            when(col("cook_duration_seconds") > self.sla_seconds, lit(True))
            .otherwise(lit(False))
        )

        # -----------------------------------------
        # 4. Add Time Dimensions
        # -----------------------------------------
        df = self._time_dims(df, "cooking_start")

        # -----------------------------------------
        # 5. Join Chefs + Outlets
        # -----------------------------------------
        df = (
            df
            .join(dim_chef.select("chef_id", "chef_name"), on="chef_id", how="left")
            .join(dim_outlet.select("outlet_id", "outlet_name"), on="outlet_id", how="left")
        )

        df = df.withColumn("ingested_at", current_timestamp())
        return df.select(
            "order_id",
            "outlet_id", "outlet_name",
            "chef_id", "chef_name",
            "cooking_start", "cooking_end",
            "cook_duration_seconds", "was_delayed_order",
            "date", "hour", "week", "month", "year", "day_of_week_num",
            "ingested_at"
        )
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, to_date, weekofyear, month, year, dayofweek,
    when, lit, coalesce
)


class FactStockEnricher:
    def __init__(self, runtype: str, loadtype: str):
        """
        - normalize timestamps & quantities
        - compute inventory_status (low/medium/high)
        - compute needs_restock flag
        - join dim_stock_item & dim_outlet
        """
        self.runtype = runtype
        self.loadtype = loadtype
        self.low_threshold = 10.0
        self.high_threshold = 100.0

    def _time_dims(self, df: DataFrame, ts_col: str):
        """Minimal Spark-3 compatible time dims"""
        df = df.withColumn(ts_col, to_timestamp(col(ts_col)))
        return (
            df
            .withColumn("stock_date", to_date(col(ts_col)))
            .withColumn("stock_week", weekofyear(col(ts_col)))
            .withColumn("stock_month", month(col(ts_col)))
            .withColumn("stock_year", year(col(ts_col)))
            .withColumn("day_of_week_num", dayofweek(col(ts_col)))
        )

    def harmonize(
        self,
        spark: SparkSession,
        dataframes: dict,
        currentio: Optional[DataLakeIO]
    ) -> DataFrame:
        """
        Inputs:
          df_stock       -> fact_stock (expects columns: outlet_id, stock_item, available_quantity, unit_of_measure, stock_date)
          dim_stock_item -> dim_stock_item (stock_id / stock_item / unit_of_measure)
          dim_outlet     -> dim_outlet (outlet_id / outlet_name)

        Returns:
          Minimal enriched fact_stock DataFrame
        """
        df_stock = dataframes['fact_stock']
        dim_stock_item = dataframes['dim_stock_item']
        dim_outlet = dataframes['dim_outlet']

        # 1) normalize timestamp and numeric quantity
        df = df_stock.withColumn("stock_date_raw", col("stock_date")) \
                     .withColumn("stock_date", to_timestamp(col("stock_date"))) \
                     .withColumn("available_quantity", coalesce(col("available_quantity").cast("double"), lit(0.0)))

        # 2) add simple inventory status & restock flag
        df = df.withColumn(
            "inventory_status",
            when(col("available_quantity") <= self.low_threshold, lit("low"))
            .when(col("available_quantity") >= self.high_threshold, lit("high"))
            .otherwise(lit("medium"))
        ).withColumn(
            "needs_restock",
            when(col("available_quantity") <= self.low_threshold, lit(True)).otherwise(lit(False))
        )

        # 3) add time dimensions (Spark 3 safe)
        df = self._time_dims(df, "stock_date")

        # 4) join with dims for friendly names
        df = (
            df
            .join(dim_stock_item.select(col("stock_item").alias("dim_stock_item"), "unit_of_measure").withColumnRenamed("unit_of_measure","dim_unit_of_measure"),
                  df["stock_item"] == col("dim_stock_item"),
                  how="left")
            .drop("dim_stock_item")
            .withColumn("unit_of_measure", coalesce(col("unit_of_measure"), col("dim_unit_of_measure")))
            .drop("dim_unit_of_measure")
            .join(dim_outlet.select("outlet_id", "outlet_name"), on="outlet_id", how="left")
        )

        # 5) final minimal column selection
        final_cols = [
            "outlet_id", "outlet_name",
            "stock_item", "unit_of_measure",
            "available_quantity",
            "inventory_status", "needs_restock",
            "stock_date", "stock_date_raw",
            "stock_week", "stock_month", "stock_year", "day_of_week_num"
        ]

        # keep only existing columns (safe)
        final_cols = [c for c in final_cols if c in df.columns]
        df = df.select(*final_cols)
        df = df.withColumn("ingested_at", current_timestamp())
        return df

class Harmonizer:
    _harmonizer_map = {
        'fact_sales_enriched': FactSalesEnricher,
        'fact_kitchen_enriched': FactKitchenEnricher,
        'fact_stock_enriched': FactStockEnricher
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