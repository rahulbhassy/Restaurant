from Shared.pyspark_env import setVEnv
from Shared.sparkconfig import create_spark_session_large
from Shared.DataWriter import DataWriter
from Shared.FileIO import DataLakeIO , SourceObjectAssignment
from Resturant.EnrichPeople.Config import config, layer
from Resturant.EnrichPeople.Harmonisation import Harmonizer
from Shared.pyspark_env import stop_spark
from Shared.Logger import Logger
import argparse
import sys
import datetime

def main(table, loadtype,runtype='prod'):
    logging = Logger(notebook_name='Process_EnrichPeople')
    logger = logging.setup_logger()

    # Log critical environment information
    logger.info(f"Starting Restaurant Data Processing")
    logger.info(f"Parameters:, sourceobject={table}, loadtype={loadtype}")

    try:
        setVEnv()
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
            format='delta',
            spark=spark
        )

        datawriter.WriteData(df=destination_data)
        stop_spark(spark=spark)
        logger.info(f"Processing completed at {datetime.datetime.now()}")
        return 0

    except Exception as e:
        logger.exception(f"Critical error: {str(e)}")
        return 1

if __name__ == "__main__":
    # Force immediate output flushing
    sys.stdout.reconfigure(line_buffering=True)
    sys.stderr.reconfigure(line_buffering=True)

    parser = argparse.ArgumentParser()
    parser.add_argument("--table", required=True)
    parser.add_argument("--loadtype", required=True)
    parser.add_argument("--runtype", required=False)

    args = parser.parse_args()
    exit_code = main(args.table, args.loadtype,args.runtype)
    sys.exit(exit_code)