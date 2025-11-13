from typing import List
from Shared.pyspark_env import setVEnv , stop_spark
from Shared.sparkconfig import create_spark_session_jdbc
from Shared.FileIO import SourceObjectAssignment
from Shared.DataWriter import DataWriter
from Resturant.PowerBIRefresh.Utilities import refreshtables,layer,schema
from Shared.Logger import Logger
import argparse
import sys
import datetime

def main(configname: List[str],loadtype: str, runtype: str = 'prod'):

    logging = Logger(notebook_name='Process_PowerBIRefresh')
    logger = logging.setup_logger()
    # Log critical environment information
    logger.info(f"Starting PowerBIRefresh")

    try:
        setVEnv()
        spark = create_spark_session_jdbc()
        tables = refreshtables if configname[0] == 'all' else configname
        logger.info(" Spark session initialized successfully")
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

        logger.info("Loaded data...")

        for table_name, df in dataframes.items():
            jdbcwriter = DataWriter(
                loadtype=loadtype,
                spark=spark,
                format='jdbc',
                table=table_name,
                schema=schema.get(table_name)
            )
            jdbcwriter.WriteData(df=df)

        stop_spark(spark=spark)
        logger.info(f"PowerBI Refresh Completed at {datetime.datetime.now()}")
        return 0

    except Exception as e:
        logger.exception(f"Critical error: {str(e)}")
        return 1

if __name__ == "__main__":
    # Force immediate output flushing
    sys.stdout.reconfigure(line_buffering=True)
    sys.stderr.reconfigure(line_buffering=True)

    parser = argparse.ArgumentParser()
    parser.add_argument("--configname", required=True)
    parser.add_argument("--loadtype", required=True)
    parser.add_argument("--runtype", required=False)

    args = parser.parse_args()
    exit_code = main(args.configname,args.loadtype,args.runtype)
    sys.exit(exit_code)