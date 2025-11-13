from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, ArrayType, TimestampType
)

# ============================================================
# DIMENSION SCHEMAS
# ============================================================

dim_customer_schema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("customer_mobile", StringType(), True)
])

dim_item_schema = StructType([
    StructField("item_id", StringType(), False),
    StructField("item_name", StringType(), True),
    StructField("unit_price", DoubleType(), True)
])

dim_outlet_schema = StructType([
    StructField("outlet_id", StringType(), False),
    StructField("outlet_name", StringType(), True)
])

dim_chef_schema = StructType([
    StructField("chef_id", StringType(), False),
    StructField("chef_name", StringType(), True),
    StructField("outlet_id", StringType(), True)
])

dim_stock_item_schema = StructType([
    StructField("stock_id", StringType(), False),
    StructField("stock_item", StringType(), True),
    StructField("unit_of_measure", StringType(), True)
])

# ============================================================
# FACT SCHEMAS
# ============================================================

# Nested schema for order menu details inside fact_sales
order_menu_detail_schema = StructType([
    StructField("item_id", StringType(), True),
    StructField("item_name", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("toppings", ArrayType(StringType()), True),
    StructField("unit_price", DoubleType(), True),
    StructField("line_total", DoubleType(), True)
])

fact_sales_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("outlet_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_ts", StringType(), True),
    StructField("order_menu_details", ArrayType(order_menu_detail_schema), True),
    StructField("total_order_price", DoubleType(), True),
    StructField("payment_mode", StringType(), True),
    StructField("order_type", StringType(), True),
    StructField("high_value_flag", BooleanType(), True)
])

fact_kitchen_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("outlet_id", StringType(), True),
    StructField("chef_id", StringType(), True),
    StructField("cooking_start", StringType(), True),
    StructField("cooking_end", StringType(), True)
])

fact_stock_schema = StructType([
    StructField("outlet_id", StringType(), True),
    StructField("stock_item", StringType(), True),
    StructField("available_quantity", DoubleType(), True),
    StructField("unit_of_measure", StringType(), True),
    StructField("stock_date", StringType(), True)
])

# ============================================================
# SCHEMA DICTIONARY (LOOKUP)
# ============================================================

schema_dict = {
    "dim_outlet": dim_outlet_schema,
    "dim_customer": dim_customer_schema,
    "dim_item": dim_item_schema,
    "dim_stock_item": dim_stock_item_schema,
    "dim_chef": dim_chef_schema,
    "fact_sales": fact_sales_schema,
    "fact_kitchen": fact_kitchen_schema,
    "fact_stock": fact_stock_schema
}