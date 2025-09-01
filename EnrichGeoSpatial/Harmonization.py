from pyspark.sql import DataFrame, SparkSession
from Shared.FileIO import DataLakeIO
from Shared.DataLoader import DataLoader
from pyspark.sql.functions import expr, col, lit , round , broadcast

_KEY_COLUMN = "trip_id"


class PreHarmonizer:
    def __init__(self, sourcedata: DataFrame, currentio: DataLakeIO, loadtype: str):
        self.sourcedata = sourcedata
        self.currentio = currentio
        self.loadtype = loadtype

    def preharmonize(self, spark: SparkSession):
        print("Starting preharmonization")

        if self.loadtype == 'delta':
            print("Loading current delta data")
            reader = DataLoader(
                path=self.currentio.filepath(),
                filetype='delta',
                loadtype='full'
            )
            currentdata = reader.LoadData(spark=spark)
            print("currentdata count: ", currentdata.count())
            print("sourcedata count before preharmonizing: ", self.sourcedata.count())
            self.sourcedata = self.sourcedata.join(
                currentdata,
                on=_KEY_COLUMN,
                how='left_anti'
            )
            print("sourcedata count after preharmonizing: ", self.sourcedata.count())
            print("Filtered out existing trip IDs")

        print("Adding point geometries")
        self.sourcedata = self.sourcedata.withColumn(
            "pickup_point",
            expr("ST_Point(CAST(pickup_longitude AS DOUBLE), CAST(pickup_latitude AS DOUBLE))")
        ).withColumn(
            "dropoff_point",
            expr("ST_Point(CAST(dropoff_longitude AS DOUBLE), CAST(dropoff_latitude AS DOUBLE))")
        )
        print("Preharmonization complete")
        return self.sourcedata


class Harmonizer:
    def __init__(self, sourcedata: DataFrame, boroughs_df: DataFrame, tripio: DataLakeIO):
        self.sourcedata = sourcedata
        self.boroughs_df = boroughs_df
        self.tripio = tripio
        self.pickup_condition = expr("ST_Contains(p.geom, u.pickup_point)")
        self.dropoff_condition = expr("ST_Contains(d.geom, u.dropoff_point)")

    def populate_boroughs(
            self,
            uberdata: DataFrame,
            tripdata: DataFrame,
            pb_null: bool = True,
            db_null: bool = True
    ) -> DataFrame:
        print("Populating missing boroughs")

        if not (pb_null or db_null):
            print("No boroughs to populate")
            return uberdata

        to_drop = []
        if pb_null:
            to_drop.append("pickup_borough")
        if db_null:
            to_drop.append("dropoff_borough")
        uberdata = uberdata.drop(*to_drop)
        print("Dropped old borough columns")

        joined = uberdata.alias("u") \
            .join(tripdata.alias("t"), on="trip_id", how="inner")
        print("Joined with trip data")

        select_exprs = [col("u.*")]
        if pb_null:
            select_exprs.append(col("t.pickup_location").alias("pickup_borough"))
        if db_null:
            select_exprs.append(col("t.dropoff_location").alias("dropoff_borough"))

        return joined.select(*select_exprs)

    def harmonize(self, spark: SparkSession):
        print("Starting harmonization")
        # create one broadcasted DataFrame and cache it so Spark does not rebuild it twice
        b_df = broadcast(self.boroughs_df).cache()

        # materialize the cache (forces a single read/serialize on driver)
        _ = b_df.count()
        print("Materialized boroughs_df (broadcast/cache)")
        print("Performing spatial joins")
        enriched_uber = (
            self.sourcedata.alias("u")
            .join(
                b_df.alias("p"),
                self.pickup_condition,
                "left"
            )
            .join(
                b_df.alias("d"),
                self.dropoff_condition,
                "left"
            )
            .select(
                col("u.date"),
                col("u.trip_id"),
                col("u.dropoff_datetime"),
                col("u.pickup_datetime"),
                col("u.fare_amount"),
                col("u.passenger_count"),
                col("u.pickup_hour"),
                col("u.pickup_day"),
                col("u.pickup_month"),
                round(col("u.avg_temp"), 2).alias("temperature"),
                round(col("u.precipitation"), 2).alias("precipitation"),
                round(col("u.avg_humidity"), 2).alias("humidity"),
                round(col("u.avg_wind_speed"), 2).alias("wind_speed"),
                round(col("u.avg_snow_fall"), 2).alias("snow_fall"),
                col('u.distance_km'),
                col("p.borough").alias("pickup_borough"),
                col("d.borough").alias("dropoff_borough")
            )
        )
        b_df.unpersist()
#hi
        print("Splitting data by borough completeness")
        full_enriched_uber = enriched_uber.filter(
            (enriched_uber.pickup_borough.isNotNull()) &
            (enriched_uber.dropoff_borough.isNotNull())
        ).withColumn(
            'pickupboroughsource', lit('features')
        ).withColumn(
            'dropoffboroughsource', lit('features')
        )

        print("Full enriched Uber data count: ", full_enriched_uber.count())

        pb_null_enriched_uber = enriched_uber.filter(
            (enriched_uber.pickup_borough.isNull()) &
            (enriched_uber.dropoff_borough.isNotNull())
        ).withColumn(
            'pickupboroughsource', lit('tripdetails')
        ).withColumn(
            'dropoffboroughsource', lit('features')
        )

        db_null_enriched_uber = enriched_uber.filter(
            (enriched_uber.pickup_borough.isNotNull()) &
            (enriched_uber.dropoff_borough.isNull())
        ).withColumn(
            'pickupboroughsource', lit('features')
        ).withColumn(
            'dropoffboroughsource', lit('tripdetails')
        )

        b_null_enriched_uber = enriched_uber.filter(
            (enriched_uber.pickup_borough.isNull()) &
            (enriched_uber.dropoff_borough.isNull())
        ).withColumn(
            'pickupboroughsource', lit('tripdetails')
        ).withColumn(
            'dropoffboroughsource', lit('tripdetails')
        )

        print("Loading trip details data")
        reader = DataLoader(
            path=self.tripio.filepath(),
            filetype='delta',
            loadtype='full'
        )
        tripdata = reader.LoadData(spark=spark)

        print("Combining datasets")
        combined = full_enriched_uber.unionByName(
            self.populate_boroughs(
                uberdata=pb_null_enriched_uber,
                tripdata=tripdata,
                pb_null=True,
                db_null=False
            )
        ).unionByName(
            self.populate_boroughs(
                uberdata=db_null_enriched_uber,
                tripdata=tripdata,
                pb_null=False,
                db_null=True
            )
        ).unionByName(
            self.populate_boroughs(
                uberdata=b_null_enriched_uber,
                tripdata=tripdata
            )
        )

        print("Harmonization complete")
        return combined