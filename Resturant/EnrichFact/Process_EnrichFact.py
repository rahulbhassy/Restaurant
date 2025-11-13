from Shared.pyspark_env import setVEnv
from Shared.sparkconfig import create_spark_session_large
from Shared.DataWriter import DataWriter
from Shared.FileIO import DataLakeIO , SourceObjectAssignment
from Resturant.EnrichFact.Config import config, layer
from Resturant.EnrichFact.Harmonisation import FactItems , Harmonizer
from Shared.pyspark_env import stop_spark


setVEnv()
table = 'fact_kitchen_enriched'
loadtype = 'full'
runtype = 'prod'
spark = create_spark_session_large()
sourcetables = config[table]

if table == 'fact_sales_enriched':
    fact_item = FactItems(
        loadtype=loadtype,
        spark=spark,
        runtype=runtype
    )
    fact_item.create_fact_items()

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
    format='delta',
    spark=spark
)

datawriter.WriteData(df=destination_data)
stop_spark(spark)