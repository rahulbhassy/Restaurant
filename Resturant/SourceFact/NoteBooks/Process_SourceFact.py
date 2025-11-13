from Shared.pyspark_env import setVEnv
from Shared.sparkconfig import create_spark_session_jdbc
from Shared.FileIO import DataLakeIO
from Shared.DataWriter import DataWriter
from Shared.FileIO import DeltaLakeOps , MergeIO
from Resturant.SourceFact.utilities import TableLoader
from Resturant.SourceFact.DataCleaner import DataCleaner
from Resturant.SourceFact.config import table_config , delta_column_dict , merge_keys
from pyspark.sql.functions import current_timestamp
from Shared.pyspark_env import stop_spark
from Shared.Logger import Logger
import argparse
import sys
from datetime import datetime

def main(table, loadtype,runtype='prod'):
    logging = Logger(notebook_name='Process_SourceFact')
    logger = logging.setup_logger()

    # Log critical environment information
    logger.info(f"Starting Restaurant Data Processing")
    logger.info(f"Parameters:, sourceobject={table}, loadtype={loadtype}")

    try:
        setVEnv()
        spark = create_spark_session_jdbc()
        ingestion_time = current_timestamp()
        start_delta = None
        end_delta = None
        delta_column = delta_column_dict.get(table)

        currentio = DataLakeIO(
            process="write",
            table=table,
            state='current',
            layer='raw',
            loadtype=loadtype,
            runtype=runtype
        )
        dataops = DeltaLakeOps(
            table=table,
            path=currentio.filepath()
        )
        delta_tables = ['fact_sales', 'fact_kitchen']
        if loadtype == 'delta' and table in delta_tables:
            end_delta = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            start_delta = dataops.get_last_loaded_timestamp(spark)
        else:
            loadtype = 'full'


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

        if loadtype == 'full':
            writer = DataWriter(
                loadtype=loadtype,
                spark=spark,
                format='delta',
                path=currentio.filepath(),
            )
            writer.WriteData(df)
        else:
            mergeio = MergeIO(
                table=table,
                currentio=currentio,
                key_columns=merge_keys.get(table)
            )
            mergeio.merge(spark=spark, updated_df=df)
        stop_spark(spark=spark)
        logger.info(f"Processing completed at {datetime.now()}")
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