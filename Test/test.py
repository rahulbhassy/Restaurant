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

setVEnv()
table = ['uberfares','tripdetails']
loadtype = 'full'

assign = SourceObjectAssignment(
    loadtype=loadtype,
    runtype='prod',
    sourcetables=table
)
dlassign = assign.assign_DataLakeIO(layer={'uberfares':'raw','tripdetails':'raw'})
readers = assign.assign_Readers(io_map=dlassign)
dataframes = assign.getData(spark=spark,readers=readers)
uberfares = dataframes['uberfares'].select('trip_id')
tripdetails = dataframes['tripdetails'].select('trip_id')
trip_ids = uberfares.join(
    tripdetails,
    on='trip_id',
    how='leftanti'
)
print(trip_ids.count())



