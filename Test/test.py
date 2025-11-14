from functools import reduce

from Shared.pyspark_env import setVEnv
from Shared.sparkconfig import create_spark_session
from SourceWeather.Initialiser import Init
from SourceWeather.Schema import weather_schema
from SourceWeather.APILoader import WeatherAPI
from Shared.FileIO import DataLakeIO
from Shared.DataWriter import DataWriter
from Shared.DataLoader import DataLoader
from Shared.FileIO import SparkTableViewer,DeltaLakeOps , SourceObjectAssignment
from pyspark.sql.functions import avg, col, lit , round
from Balancing.config import SCHEMA
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    ArrayType
)


setVEnv()
table = 'outlet_performance'
spark = create_spark_session()
loadtype = 'full'


currentio = DataLakeIO(
    process="write",
    table=table,
    state='current',
    layer='enrich',
    loadtype=loadtype
)
loader = DataLoader(
    path=currentio.filepath(),
    filetype='delta',
    loadtype=loadtype
)
df = loader.LoadData(spark)


viewer = SparkTableViewer(df)
viewer.display()


