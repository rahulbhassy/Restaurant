from Shared.pyspark_env import setVEnv
from Shared.sparkconfig import create_spark_session_jdbc
from Shared.FileIO import DataLakeIO
from Shared.DataWriter import DataWriter
from Shared.FileIO import DeltaLakeOps
from Resturant.SourceFact.utilities import TableLoader
from Resturant.SourceFact.DataCleaner import DataCleaner
from Resturant.SourceFact.config import table_config , delta_column_dict
from pyspark.sql.functions import current_timestamp
from Shared.pyspark_env import stop_spark

setVEnv()
spark = create_spark_session_jdbc()
table = 'fact_kitchen'
loadtype = 'full'
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

stop_spark(spark)
