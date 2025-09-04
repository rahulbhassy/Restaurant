from Shared.pyspark_env import setVEnv , stop_spark
from Shared.sparkconfig import create_spark_session
from Shared.FileIO import SourceObjectAssignment
from Shared.FileIO import DataLakeIO
from Balancing.config import CHECKS, SCHEMA , layer
from Balancing.utilities import Balancing
from Shared.DataWriter import DataWriter

setVEnv()
spark = create_spark_session()
runtype = 'prod'
loadtype = 'full'
tables = ['all']
tables = CHECKS.keys() if tables[0] == 'all' else tables
final = spark.createDataFrame([], SCHEMA)
results = []
for table_name in tables:
    sourcetable_assignment = SourceObjectAssignment(
        sourcetables=CHECKS[table_name]['tables'],
        loadtype=loadtype,
        runtype=runtype
    )
    io_map = {}
    if layer.get(table_name) == 'raw':
        io_map = sourcetable_assignment.assign_DataLakeIO(layer=layer,process='load')
    else:
        io_map = sourcetable_assignment.assign_DataLakeIO(layer=layer)

    targetio = DataLakeIO(
        process='read',
        table=table_name if table_name != 'uberfaresenrich' else 'uberfares',
        state='current',
        layer=layer.get(table_name),
        loadtype=loadtype,
        runtype=runtype
    )
    target_mapping = {table_name: targetio.filepath()}
    targetquery = CHECKS[table_name]['targetquery'].format(**target_mapping)

    source_mapping = {t: io_map[t].filepath() for t in CHECKS[table_name]['tables']}
    sourcequery = CHECKS[table_name]['sourcequery'].format(**source_mapping)

    balancing = Balancing(
        table=table_name,
        sourcequery=sourcequery,
        targetquery=targetquery,
    )

    df,result = balancing.getResult(spark=spark)
    final = final.unionByName(df)
    results.append(result)

balancingio = DataLakeIO(
    process='write',
    table='balancingresults',
    state='current',
    layer='system',
    loadtype=loadtype,
    runtype=runtype
)

writer = DataWriter(
    loadtype='delta',
    path=balancingio.filepath(),
    format='delta',
    spark=spark
)

writer.WriteData(df=final)
final.show()
if 'Fail' in results:
    raise Exception("Balancing checks failed. Please review the logs for details.")

stop_spark(spark=spark)


