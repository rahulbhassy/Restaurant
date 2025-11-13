from Shared.pyspark_env import setVEnv
from Shared.sparkconfig import create_spark_session_jdbc
from Shared.FileIO import SourceObjectAssignment
from Shared.DataWriter import DataWriter
from Resturant.PowerBIRefresh.Utilities import refreshtables,layer,schema

setVEnv()
loadtype = 'full'
runtype = 'prod'
configname = ['all']
spark = create_spark_session_jdbc()
tables = refreshtables if configname[0] == 'all' else configname
tableassignments = SourceObjectAssignment(
    sourcetables=tables,
    loadtype=loadtype,
    runtype=runtype
)
tablereaders = tableassignments.assign_Readers(
    io_map=tableassignments.assign_DataLakeIO(layer=layer)
)
dataframes = tableassignments.getData(
    spark=spark,
    readers=tablereaders
)

for table_name, df in dataframes.items():
    jdbcwriter = DataWriter(
        loadtype=loadtype,
        spark=spark,
        format='jdbc',
        table=table_name,
        schema=schema.get(table_name)
    )
    jdbcwriter.WriteData(df=df)

spark.stop()