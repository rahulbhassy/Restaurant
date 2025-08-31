from EnrichPeople.Config import config,layer , keys, updateitems
from Shared.sparkconfig import create_spark_session_large
from Shared.FileIO import SourceObjectAssignment , DataLakeIO , MergeIO
from Shared.DataWriter import DataWriter
from EnrichPeople.Harmonization import Harmonizer
from Shared.pyspark_env import setVEnv , stop_spark
from Shared.FileIO import SparkTableViewer


setVEnv()
table = "driverpreference"
loadtype = 'full'
runtype = 'prod'
initial_load = 'yes'

spark = create_spark_session_large()
sourcetables = config[table]

sourceobjectassignments = SourceObjectAssignment(
    sourcetables=sourcetables,
    loadtype=loadtype,
    runtype=runtype
)
sourcereaders = sourceobjectassignments.assign_Readers(
    io_map=sourceobjectassignments.assign_DataLakeIO(layer=layer)
)
dataframes = sourceobjectassignments.getData(
    spark=spark,
    readers=sourcereaders
)
harmonizer = Harmonizer(
    table=table,
    loadtype=loadtype,
    runtype=runtype
)
currentio = DataLakeIO(
    process='write',
    table=table,
    state='current',
    layer=layer.get(table),
    loadtype=loadtype,
    runtype=runtype
)
destination_data = harmonizer.harmonize(
    spark=spark,
    dataframes=dataframes,
    currentio=currentio
)

datawriter = DataWriter(
    loadtype=loadtype,
    path=currentio.filepath(),
    spark=spark,
    format='delta'
)

if initial_load == 'yes':
    datawriter.WriteData(df=destination_data)
else:
    datawriter.WriteParquet(df=destination_data)
    mergeconfig = MergeIO(
        table=table,
        currentio= currentio,
        key_columns= keys.get(table)
    )
    mergeconfig.merge(spark=spark,updated_df=destination_data)
stop_spark(spark=spark)
