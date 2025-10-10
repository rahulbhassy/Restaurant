from Shared.pyspark_env import setEnv
from Shared.sparkconfig import create_spark_session
from Schema import sourceschema
from Shared.DataLoader import DataLoader
from Shared.DataWriter import DataWriter
from Shared.FileIO import DataLakeIO
from DataCleaner import DataCleaner
from concurrent.futures import ThreadPoolExecutor, as_completed

setEnv()
spark = create_spark_session()
satellite_tables = ["customerdetails", "driverdetails", "vehicledetails"]
loadtype = 'delta'

def process_table(sourceobject: str):
    """Load, clean and write one satellite table."""
    schema = sourceschema(sourcedefinition=sourceobject)

    loadio = DataLakeIO(
        process='load',
        table=sourceobject,
        loadtype=loadtype
    )
    dataloader = DataLoader(
        path=loadio.filepath(),
        schema=schema,
        filetype=loadio.file_ext(),
        loadtype=loadtype
    )
    source_data = dataloader.LoadData(spark)

    datacleaner = DataCleaner(
        sourceobject=sourceobject,
        spark=spark,
        loadtype=loadtype
    )
    destination_data = datacleaner.clean(data=source_data)

    currentio = DataLakeIO(
        process="write",
        table=sourceobject,
        state='current',
        loadtype=loadtype,
        layer='raw'
    )
    datawriter = DataWriter(
        loadtype=loadtype,
        path=currentio.filepath(),
        format="delta",
        spark=spark
    )
    datawriter.WriteData(
        df=destination_data
    )

    # Adjust max_workers to the number of tables or your system’s capacity
with ThreadPoolExecutor(max_workers=len(satellite_tables)) as executor:
    # submit all jobs
    future_to_table = {
        executor.submit(process_table, tbl): tbl
        for tbl in satellite_tables
    }
    # optionally, track progress
    for fut in as_completed(future_to_table):
        tbl  = future_to_table[fut]
        try:
            result = fut.result()
            print(f"[{tbl}] completed successfully")
        except Exception as e:
            print(f"[{tbl}] failed: {e}")

spark.stop()