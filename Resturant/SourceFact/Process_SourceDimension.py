from Shared.pyspark_env import setVEnv
from Shared.sparkconfig import create_spark_session_jdbc
from Shared.FileIO import DataLakeIO
from Shared.DataWriter import DataWriter
from Shared.FileIO import DeltaLakeOps
from Resturant.SourceFact.utilities import TableLoader
from Resturant.SourceFact.DataCleaner import DataCleaner
from Resturant.SourceFact.config import table_config , delta_column_dict
from pyspark.sql.functions import current_timestamp
from concurrent.futures import ThreadPoolExecutor, as_completed
from Shared.pyspark_env import stop_spark

setVEnv()
spark = create_spark_session_jdbc()
dim_tables = [
    "dim_customer",
    "dim_item",
    "dim_outlet",
    "dim_chef",
    "dim_stock_item"
]
loadtype = 'full'

def process_table(table: str):
    ingestion_time = current_timestamp()
    start_delta = None
    end_delta = None
    delta_column = delta_column_dict.get(table)

    currentio = DataLakeIO(
        process="write",
        table=table,
        state='current',
        layer='raw',
        loadtype=loadtype
    )
    dataops = DeltaLakeOps(
        table=table,
        path=currentio.filepath()
    )
    if loadtype == 'delta':
        end_delta = ingestion_time
        start_delta = dataops.get_last_loaded_timestamp(spark)


    loader = TableLoader(
        table_name=table,
        delta_column=delta_column,
        start_delta=start_delta,
        end_delta=end_delta
    )
    df = loader.load_data(spark)
    cleaner = DataCleaner(
        spark,
        mandatory_cols=table_config[table]["mandatory_cols"],
        cast_config=table_config[table]["cast_config"],
        allowed_values=table_config[table]["allowed_values"],
        duplicate_keys=table_config[table]["duplicate_keys"],
        anomaly_rules=table_config[table]["anomaly_rules"],
        ingestion_time=ingestion_time
    )

    df = cleaner.clean(df)

    writer = DataWriter(
        loadtype=loadtype,
        spark=spark,
        format='delta',
        path=currentio.filepath(),
    )

    writer.WriteData(df)

    # Adjust max_workers to the number of tables or your system’s capacity
with ThreadPoolExecutor(max_workers=3) as executor:
    # submit all jobs
    future_to_table = {
        executor.submit(process_table, tbl): tbl
        for tbl in dim_tables
    }

    for fut in as_completed(future_to_table):
        tbl  = future_to_table[fut]
        try:
            result = fut.result()
            print(f"[{tbl}] completed successfully")
        except Exception as e:
            print(f"[{tbl}] failed: {e}")

stop_spark(spark)