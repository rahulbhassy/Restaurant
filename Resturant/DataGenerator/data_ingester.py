from Shared.pyspark_env import setVEnv
from Shared.sparkconfig import create_spark_session_jdbc
from Shared.DataWriter import DataWriter
from Shared.DataLoader import DataLoader
from schema import schema_dict, writer_mode

from pyspark.sql.functions import to_json , col
setVEnv()
spark = create_spark_session_jdbc()

table = 'fact_sales'
loadtype = 'delta'
table_load_order = [
    "dim_outlet",      # Parent of chef, fact_sales, fact_kitchen, fact_stock
    "dim_customer",    # Referenced by fact_sales
    "dim_item",        # (used inside JSON / optional dimension for analytics)
    "dim_stock_item",  # Referenced by inventory data (if extended)
    "dim_chef",        # Depends on dim_outlet
    "fact_sales",      # Depends on dim_outlet, dim_customer
    "fact_kitchen",    # Depends on fact_sales, dim_outlet, dim_chef
    "fact_stock"       # Depends on dim_outlet
]
for table in table_load_order:
    dataloader = DataLoader(
        path=f'C:/Users/HP/uber_project/Resturant/DataGenerator/kfc_complete_data/{table}.json',
        schema=schema_dict.get(table),
        filetype='json',
        loadtype=loadtype
    )
    df = dataloader.LoadData(spark)
    if table == 'fact_sales':
        df = df.withColumn("order_menu_details", to_json(col("order_menu_details")))
    jdbcwriter = DataWriter(
        loadtype='delta',
        spark=spark,
        format='jdbc',
        table=table,
        mode=writer_mode.get(table)
    )
    jdbcwriter.WriteData(df=df)
