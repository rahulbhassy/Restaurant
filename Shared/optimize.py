from typing import Optional

from Shared.FileIO import DeltaLakeOps , SourceObjectAssignment
from Shared.config import RAWLAYER,RAWTABLES,SYSTEMLAYER,SYSTEMTABLES,ENRICHTABLES,ENRICHLAYER,SPATIALLAYER,SPATIALTABLES
from Shared.pyspark_env import setVEnv,stop_spark
from Shared.sparkconfig import create_spark_session_large,create_spark_session_sedona
from Shared.Logger import Logger
import argparse
import sys
import datetime

def main(tabletype, loadtype,table: Optional[str]= None,runtype='prod',altertable=False):
    logging = Logger(notebook_name='Process_Optimize')
    logger = logging.setup_logger()

    # Log critical environment information
    logger.info(f"Starting Optimize")

    try:
        setVEnv()
        # Define configuration mapping
        config = {
            'raw': (RAWTABLES, RAWLAYER, 'large'),
            'system': (SYSTEMTABLES, SYSTEMLAYER, 'large'),
            'spatial': (SPATIALTABLES, SPATIALLAYER, 'sedona'),
            'enrich': (ENRICHTABLES, ENRICHLAYER, 'large')
        }

        # Get configuration based on tabletype (default to 'enrich' if not found)
        tables, layer, session_type = config.get(
            tabletype,
            (ENRICHTABLES, ENRICHLAYER, 'large')
        )
        if table:
            tables = [table]

        # Create appropriate Spark session
        spark = create_spark_session_sedona() if session_type == 'sedona' else create_spark_session_large()
        spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        assignment = SourceObjectAssignment(
            loadtype=loadtype,
            runtype=runtype,
            sourcetables=tables
        )
        io_map = assignment.assign_DataLakeIO(layer=layer)
        for table, io in io_map.items():
            deltalakeops = DeltaLakeOps(
                path=io.filepath(),
                table=table
            )
            if altertable:
                deltalakeops.altertable(spark=spark)
            deltalakeops.vacuum(spark=spark,retention=120)
            deltalakeops.optimise(spark=spark)

        logger.info(f"Processing completed at {datetime.datetime.now()}")
        stop_spark(spark=spark)
        return 0

    except Exception as e:
        logger.exception(f"Critical error: {str(e)}")
        return 1

if __name__ == "__main__":
    # Force immediate output flushing
    sys.stdout.reconfigure(line_buffering=True)
    sys.stderr.reconfigure(line_buffering=True)

    parser = argparse.ArgumentParser()
    parser.add_argument("--tabletype", required=True)
    parser.add_argument("--loadtype", required=True)
    parser.add_argument("--runtype", required=False)
    parser.add_argument("--altertable", required=False)

    args = parser.parse_args()
    exit_code = main(args.sourceobject, args.loadtype,args.runtype,args.altertable)
    sys.exit(exit_code)