from functools import reduce

from Shared.pyspark_env import setVEnv
from Shared.sparkconfig import create_spark_session
from SourceWeather.Initialiser import Init
from SourceWeather.Schema import weather_schema
from SourceWeather.APILoader import WeatherAPI
from Shared.FileIO import DataLakeIO
from Shared.DataWriter import DataWriter
from Shared.DataLoader import DataLoader
from Shared.FileIO import SparkTableViewer
from pyspark.sql.functions import avg, col, lit , round
from Balancing.config import SCHEMA

setVEnv()
table = 'driverdetails'
spark = create_spark_session()
loadtype = 'full'

balancingio = DataLakeIO(
    process='read',
    table=table,
    state='current',
    layer='raw',
    loadtype=loadtype
)

reader = DataLoader(
    loadtype=loadtype,
    path=balancingio.filepath(),
    filetype='delta'

)
df = reader.LoadData(spark)
df = df.filter(
    df.driver_id == '2381'
)
df.show()



