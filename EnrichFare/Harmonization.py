from typing import List, Optional
from pyspark.sql import *
from pyspark.sql.functions import create_map, lit, coalesce , when ,round
from pyspark.sql.functions import col, unix_timestamp , avg , count , year , to_date , weekofyear
from pyspark.sql import DataFrame, SparkSession
from Shared.FileIO import DataLakeIO
from Shared.DataLoader import DataLoader
from Shared.DataWriter import DataWriter
from pyspark.sql.types import StructType
from pyspark.sql.functions import col
from functools import reduce

class PreHarmonizer:
    def __init__(self,sourcedata: DataFrame,currentdata: DataFrame, loadtype: str):
        self.sourcedata = sourcedata
        self.loadtype = loadtype
        self.currentdata = currentdata

    def preharmonize(self,keycolumn: List[str]):

        self.sourcedata = self.sourcedata.join(
            self.currentdata,
            on=keycolumn,
            how='left_anti'
        )
        return self.sourcedata

class FareHarmonizer:

    _month_map = {
        1: "January", 2: "February", 3: "March", 4: "April",
        5: "May", 6: "June", 7: "July", 8: "August",
        9: "September", 10: "October", 11: "November", 12: "December"
    }
    _day_map = {
        1: "Sunday", 2: "Monday", 3: "Tuesday", 4: "Wednesday",
        5: "Thursday", 6: "Friday", 7: "Saturday"
    }

    rain_bins = [0.1, 2, 10]  # mm
    rain_labels = ["No Rain", "Slight Rain", "Medium Rain", "Heavy Rain"]

    snow_bins = [0.1, 1, 5]  # mm
    snow_labels = ["No Snow", "Slight Snow", "Medium Snow", "Heavy Snow"]

    wind_bins = [5, 20, 40]  # km/h
    wind_labels = ["Calm", "Slight Wind", "Windy", "Heavy Wind"]
    temp_bins = [0, 5, 15, 25, 35, 40]  # boundaries
    temp_labels = [
        "Freezing",  # <= 0°C
        "Very Cold",  # 0 < t <= 5
        "Cool",  # 5 < t <= 15
        "Mild",  # 15 < t <= 25
        "Warm",  # 25 < t <= 35
        "Hot",  # 35 < t <= 40
        "Extreme Heat"  # > 40
    ]

    def __init__(self,loadtype: str,runtype: str = 'full'):
        self.loadtype = loadtype
        self.runtype = runtype
        self.mapping_monthexpr = create_map([lit(x) for pair in self._month_map.items() for x in pair])
        self.mapping_dayexpr = create_map([lit(x) for pair in self._day_map.items() for x in pair])


    def categorize_metric(self,col_name, bins, labels, null_label="Unknown"):
        """
        bins: list of numeric boundaries (ascending). e.g. [0.1, 2, 10]
        labels: list of label names, len(labels) == len(bins)+1
        produces a column expression with chained WHENs
        """
        expr = when(col(col_name).isNull(),lit(null_label))
        # first bucket: <= bins[0]
        expr = expr.when(col(col_name) <= bins[0], lit(labels[0]))
        # middle buckets
        for i in range(1, len(bins)):
            expr = expr.when((col(col_name) > bins[i - 1]) & (col(col_name) <= bins[i]),lit(labels[i]))
        # final otherwise
        expr = expr.otherwise(lit(labels[-1]))
        return expr

    def harmonize(self, spark: SparkSession,dataframes: dict,currentio: Optional[DataLakeIO]) -> DataFrame:
        # Implement fare harmonization logic here
        # Pre-harmonization step
        uberdf = dataframes.get('uber')
        tripdf = dataframes.get('tripdetails')
        if self.loadtype == 'delta':
            reader = DataLoader(
                path=currentio.filepath(),
                filetype=currentio.file_ext(),
                loadtype= self.loadtype
            )
            currentdata = reader.LoadData(spark=spark)
            uberph = PreHarmonizer(
                sourcedata=uberdf,
                currentdata=currentdata,
                loadtype=self.loadtype
            )
            tripph = PreHarmonizer(
                sourcedata=tripdf,
                currentdata=currentdata,
                loadtype=self.loadtype
            )
            uberdf = uberph.preharmonize(keycolumn=['trip_id'])
            tripdf = tripph.preharmonize(keycolumn=['trip_id'])

        destinationdata = (
            uberdf.alias('u').join(
                tripdf.alias('t'),
                on='trip_id',
                how='inner'
            ).withColumn(
                "trip_duration_min",
                (unix_timestamp(col("u.dropoff_datetime")) - unix_timestamp(col("u.pickup_datetime"))) / 60.0
            ).withColumn(
                "avg_speed_kmph",
                when(col("trip_duration_min") > 0, (col("distance_km") * 60) / col("trip_duration_min"))
                .otherwise(None)
            ).withColumn(
                "fare_per_km",
                when(
                    col("distance_km") > 0,
                    col("fare_amount") / col("distance_km")
                ).otherwise(None)
            ).withColumn(
                "fare_per_min",
                when(
                    col("trip_duration_min") > 0,
                    col("fare_amount") / col("trip_duration_min")
                ).otherwise(None)
            ).withColumn(
                "tip_pct",
                when(
                    (col("fare_amount") + col("tip_amount")) == 0, None
                ).otherwise((col("tip_amount") / (col("fare_amount") + 1e-9))*100)
            ).withColumn(
                "pickup_month",
                self.mapping_monthexpr.getItem(col("pickup_month"))
            ).withColumn(
                "pickup_day",
                self.mapping_dayexpr.getItem(col("pickup_day"))
            ).withColumn(
        "pickup_period",
                 when(col("pickup_hour").isNull(), lit("Unknown"))
                .when((col("pickup_hour") >= 0) & (col("pickup_hour") <= 3), lit("After Midnight"))
                .when((col("pickup_hour") > 3) & (col("pickup_hour") <= 5), lit("Early Morning"))
                .when((col("pickup_hour") > 5) & (col("pickup_hour") <= 9), lit("Morning Rush"))
                .when((col("pickup_hour") > 9) & (col("pickup_hour") <= 15), lit("Midday"))
                .when((col("pickup_hour") > 15) & (col("pickup_hour") <= 19), lit("Evening Rush"))
                .when((col("pickup_hour") > 19) & (col("pickup_hour") <= 21), lit("Night"))
                .when((col("pickup_hour") > 21) & (col("pickup_hour") <= 24), lit("Late Night"))
                .otherwise(lit("InvalidHour"))  # covers 0, 25, negative, etc.
            ).withColumn(
                "total_fareamount",
                col("u.fare_amount") + col("t.tip_amount")
            ).withColumn(
                "rain_intensity",
                self.categorize_metric("precipitation", self.rain_bins, self.rain_labels)
            ).withColumn(
                "snow_intensity",
                self.categorize_metric("snow_fall", self.snow_bins, self.snow_labels)
            ).withColumn(
                "wind_intensity",
                self.categorize_metric("wind_speed", self.wind_bins, self.wind_labels)
            ).withColumn(
                "temperature_intensity",
                self.categorize_metric("temperature", self.temp_bins, self.temp_labels)
            ).withColumn(
                "is_weather_extreme",
                when(
                    (col("rain_intensity").isin("Heavy Rain","Medium Rain")) |
                    (col("snow_intensity").isin("Heavy Snow","Medium Snow")) |
                    (col("wind_intensity") == "Heavy Wind") |
                    (col("temperature_intensity").isin("Extreme Heat", "Freezing","Very Cold","Hot")),
                    lit(True)
                ).otherwise(lit(False))
            ).select(
                col("u.trip_id"),
                col("u.date"),
                col("u.pickup_datetime"),
                col("u.dropoff_datetime"),
                col("u.distance_km"),
                "trip_duration_min",
                round(col("avg_speed_kmph"),2).alias("avg_speed_kmph"),
                col("u.fare_amount"),
                col("t.tip_amount"),
                round(col("fare_per_km"), 2).alias("fare_per_km"),
                round(col("fare_per_min"), 2).alias("fare_per_min"),
                round(col("tip_pct"), 2).alias("tip_pct"),
                round(col("total_fareamount"), 2).alias("total_fareamount"),
                "pickup_month",
                "pickup_day",
                "pickup_period",
                "rain_intensity",
                "snow_intensity",
                "wind_intensity",
                "temperature_intensity",
                "is_weather_extreme",
                col("u.pickup_borough"),
                col("u.dropoff_borough"),
                col("u.pickupboroughsource"),
                col("u.dropoffboroughsource")
            )
        )
        return destinationdata

class WeatherImpactHarmonizer:
    def __init__(self,loadtype: str,runtype: str = 'dev'):
        self.loadtype = loadtype
        self.runtype = runtype

    def harmonize(self, spark: SparkSession, dataframes: dict , currentio: Optional[DataLakeIO]) -> DataFrame:
        # Implement weather impact harmonization logic here
        fares = dataframes.get('fares')
        trip = dataframes.get('tripdetails').select('trip_id','trip_status','driver_rating')
        expected = fares.filter(
            col('is_weather_extreme') == False
        ).groupBy(
            'pickup_borough', 'dropoff_borough','pickup_period'
        ).agg(
            round(avg('fare_amount'), 2).alias('expected_fare_amount'),
            round(avg('trip_duration_min'), 2).alias('expected_trip_duration_min')
        )
        weatherimpact = fares.alias('f').join(
            trip.alias('t'),
            on='trip_id',
            how='inner'
        ).join(
            expected.alias('e'),
            on=['pickup_borough', 'dropoff_borough','pickup_period'],
            how='left'
        ).withColumn(
            'fare_amount_diff',
            when(
                (col('f.is_weather_extreme') == True) & (col('f.fare_amount') > col('e.expected_fare_amount')),
                col('f.fare_amount') - col('e.expected_fare_amount')
            ).otherwise(lit(0))
        ).withColumn(
            'tip_duration_diff',
            when(
                (col('f.is_weather_extreme') == True) & (col('f.trip_duration_min') > col('e.expected_trip_duration_min')),
                col('f.trip_duration_min') - col('e.expected_trip_duration_min')
            ).otherwise(lit(0))
        ).withColumn(
            'fare_amount_pct_diff',
            when(
                col('e.expected_fare_amount') > 0,
                (col('fare_amount_diff') / col('e.expected_fare_amount')) * 100
            ).otherwise(lit(0))
        ).withColumn(
            'trip_duration_pct_diff',
            when(
                col('e.expected_trip_duration_min') > 0,
                (col('tip_duration_diff') / col('e.expected_trip_duration_min')) * 100
            ).otherwise(lit(0))
        ).withColumn(
            "low_rating_due_to_weather",
             when(
                 (col("t.driver_rating") < 3) & (col("f.is_weather_extreme") == True), True
             ).otherwise(False)
        ).withColumn(
            "cancellation_due_to_weather",
            when(
                (col("trip_status") == "cancelled") & (col("f.is_weather_extreme") == True),
                True
            ).otherwise(False)
        ).select(
            col("f.trip_id"),
            col("f.date"),
            col("f.pickup_datetime"),
            col("f.dropoff_datetime"),
            col("f.trip_duration_min"),
            col("f.fare_amount"),
            col("e.expected_fare_amount").alias("expected_fare_amount"),
            col("e.expected_trip_duration_min").alias("expected_trip_duration_min"),
            round(col("fare_amount_diff"), 2).alias("fare_amount_diff"),
            round(col("tip_duration_diff"), 2).alias("tip_duration_diff"),
            round(col("fare_amount_pct_diff"), 2).alias("fare_amount_pct_diff"),
            round(col("trip_duration_pct_diff"), 2).alias("trip_duration_pct_diff"),
            col("f.pickup_period"),
            col("f.rain_intensity"),
            col("f.snow_intensity"),
            col("f.wind_intensity"),
            col("f.temperature_intensity"),
            col("f.is_weather_extreme"),
            col("t.driver_rating"),
            col("low_rating_due_to_weather"),
            col("t.trip_status"),
            col("cancellation_due_to_weather"),
            col("f.pickup_borough"),
            col("f.dropoff_borough")
        )

        return weatherimpact

class TimeSeriesHarmonizer:
    def __init__(self,loadtype: str,runtype: str = 'dev'):
        self.loadtype = 'full'
        self.runtype = runtype

    @staticmethod
    def _round_avg_columns(df: DataFrame) -> DataFrame:
        """Rounds all columns containing 'avg' in their name to 2 decimal places."""
        # Identify columns with 'avg' in their name
        avg_cols = [col_name for col_name in df.columns if 'avg' in col_name]
        # Apply rounding only to identified columns
        if avg_cols:
            rounding_exprs = [
                round(col(col_name), 2).alias(col_name)
                if col_name in avg_cols
                else col(col_name)
                for col_name in df.columns
            ]
            return df.select(*rounding_exprs)
        return df

    def SaveInterimTables(self,interimtables: dict,spark: SparkSession):
        for table_name, df in interimtables.items():
            jdbcwriter = DataWriter(
                loadtype=self.loadtype,
                spark=spark,
                format='jdbc',
                table=table_name,
                schema='fares'
            )
            jdbcwriter.WriteData(df=df)

    def intermediateTables(self,uberfares: DataFrame , fares: DataFrame):
        intermediate = {}
        fares = fares.join(
            uberfares,
            on='trip_id',
            how='inner'
        ).withColumn(
            'pickup_year',
            year(to_date(col("date"), "yyyy-MM-dd"))
        ).withColumn(
            'pickup_week',
            weekofyear(to_date(col("date"), "yyyy-MM-dd"))
        )

        hourly_aggregation = fares.groupBy(
            'pickup_year','pickup_hour'
        ).agg(
            avg('fare_amount').alias('avg_fare_amount_hour'),
            avg('trip_duration_min').alias('avg_trip_duration_min_hour'),
            avg('fare_per_km').alias('avg_fare_per_km_hour'),
            avg('fare_per_min').alias('avg_fare_per_min_hour'),
            count('trip_id').alias('trip_count_hour')
        ).withColumn(
            'pickup_hour',
            when(col('pickup_hour').isNull(), lit('Unknown'))
            .otherwise(col('pickup_hour'))
        ).withColumn(
            'pickup_year',
            when(col('pickup_year').isNull(), lit('Unknown'))
            .otherwise(col('pickup_year'))
        )

        daily_aggregation = fares.groupBy(
            'pickup_year','pickup_day'
        ).agg(
            avg('fare_amount').alias('avg_fare_amount_day'),
            avg('trip_duration_min').alias('avg_trip_duration_min_day'),
            avg('fare_per_km').alias('avg_fare_per_km_day'),
            avg('fare_per_min').alias('avg_fare_per_min_day'),
            count('trip_id').alias('trip_count_day')
        ).withColumn(
            'pickup_day',
            when(col('pickup_day').isNull(), lit('Unknown'))
            .otherwise(col('pickup_day'))
        ).withColumn(
            'pickup_year',
            when(col('pickup_year').isNull(), lit('Unknown'))
            .otherwise(col('pickup_year'))
        )

        monthly_aggregation = fares.groupBy(
            'pickup_year','pickup_month'
        ).agg(
            avg('fare_amount').alias('avg_fare_amount_month'),
            avg('trip_duration_min').alias('avg_trip_duration_min_month'),
            avg('fare_per_km').alias('avg_fare_per_km_month'),
            avg('fare_per_min').alias('avg_fare_per_min_month'),
            count('trip_id').alias('trip_count_month')
        ).withColumn(
            'pickup_month',
            when(col('pickup_month').isNull(), lit('Unknown'))
            .otherwise(col('pickup_month'))
        ).withColumn(
            'pickup_year',
            when(col('pickup_year').isNull(), lit('Unknown'))
            .otherwise(col('pickup_year'))
        )

        peak_hour = fares.filter(
            col('pickup_period').isin('Morning Rush', 'Evening Rush')
        ).groupBy(
            'pickup_year','pickup_period'
        ).agg(
            count('trip_id').alias('peak_hour_trip_count'),
            avg('fare_amount').alias('peak_hour_avg_fare_amount'),
            avg('fare_per_km').alias('avg_fare_per_km_peak_hour'),
            avg('fare_per_min').alias('avg_fare_per_peak_hour'),
            avg('trip_duration_min').alias('peak_hour_avg_trip_duration_min')
        ).withColumn(
            'pickup_period',
            when(col('pickup_period').isNull(), lit('Unknown'))
            .otherwise(col('pickup_period'))
        ).withColumn(
            'pickup_year',
            when(col('pickup_year').isNull(), lit('Unknown'))
            .otherwise(col('pickup_year'))
        )

        non_peak_hour = fares.filter(
            ~col('pickup_period').isin('Morning Rush', 'Evening Rush')
        ).groupBy(
            'pickup_year','pickup_period'
        ).agg(
            count('trip_id').alias('non_peak_hour_trip_count'),
            avg('fare_amount').alias('non_peak_hour_avg_fare_amount'),
            avg('fare_per_km').alias('avg_fare_per_km_non_peak_hour'),
            avg('fare_per_min').alias('avg_fare_per_min_non_peak_hour'),
            avg('trip_duration_min').alias('non_peak_hour_avg_trip_duration_min')
        ).withColumn(
            'pickup_period',
            when(col('pickup_period').isNull(), lit('Unknown'))
            .otherwise(col('pickup_period'))
        ).withColumn(
            'pickup_year',
            when(col('pickup_year').isNull(), lit('Unknown'))
            .otherwise(col('pickup_year'))
        )

        weekend_fares = fares.filter(
            col('pickup_day').isin('Saturday', 'Sunday')
        ).groupBy(
            'pickup_year','pickup_day'
        ).agg(
            avg('fare_amount').alias('avg_weekend_fare_amount'),
            avg('trip_duration_min').alias('avg_weekend_trip_duration_min'),
            avg('fare_per_km').alias('avg_fare_per_km_weekend'),
            avg('fare_per_min').alias('avg_fare_per_min_hour_weekend'),
            count('trip_id').alias('weekend_trip_count')
        ).withColumn(
            'pickup_year',
            when(col('pickup_year').isNull(), lit('Unknown'))
            .otherwise(col('pickup_year'))
        )

        weekday_fares = fares.filter(
            ~col('pickup_day').isin('Saturday', 'Sunday')
        ).groupBy(
            'pickup_year','pickup_day'
        ).agg(
            avg('fare_amount').alias('avg_weekday_fare_amount'),
            avg('trip_duration_min').alias('avg_weekday_trip_duration_min'),
            avg('fare_per_km').alias('avg_fare_per_km_weekday'),
            avg('fare_per_min').alias('avg_fare_per_min_weekday'),
            count('trip_id').alias('weekday_trip_count')
        ).withColumn(
            'pickup_year',
            when(col('pickup_year').isNull(), lit('Unknown'))
            .otherwise(col('pickup_year'))
        )


        yearly_fares = fares.groupBy(
            'pickup_year'
        ).agg(
            avg('fare_amount').alias('avg_fare_amount_year'),
            avg('trip_duration_min').alias('avg_trip_duration_min_year'),
            avg('fare_per_km').alias('avg_fare_per_km_year'),
            avg('fare_per_min').alias('avg_fare_per_min_year'),
            count('trip_id').alias('trip_count_year')
        ).withColumn(
            'pickup_year',
            when(col('pickup_year').isNull(), lit('Unknown'))
            .otherwise(col('pickup_year'))
        )

        weekly_fares = fares.groupBy(
            'pickup_year','pickup_week'
        ).agg(
            avg('fare_amount').alias('avg_fare_amount_week'),
            avg('trip_duration_min').alias('avg_trip_duration_min_week'),
            avg('fare_per_km').alias('avg_fare_per_km_weekly'),
            avg('fare_per_min').alias('avg_fare_per_min_weekly'),
            count('trip_id').alias('trip_count_week')
        ).withColumn(
            'pickup_week',
            when(col('pickup_week').isNull(), lit('Unknown'))
            .otherwise(col('pickup_week'))
        ).withColumn(
            'pickup_year',
            when(col('pickup_year').isNull(), lit('Unknown'))
            .otherwise(col('pickup_year'))
        )

        intermediate['weekday_fares'] = weekday_fares
        intermediate['yearly_fares'] = yearly_fares
        intermediate['peak_hour'] = peak_hour
        intermediate['monthly_aggregation'] = monthly_aggregation
        intermediate['daily_aggregation'] = daily_aggregation
        intermediate['hourly_aggregation'] = hourly_aggregation
        intermediate['weekend_fares'] = weekend_fares
        intermediate['weekly_fares'] = weekly_fares
        intermediate['non_peak_hour'] = non_peak_hour

        for table in intermediate:
            intermediate[table] = self._round_avg_columns(intermediate[table])

        return intermediate

    def harmonize(self, spark: SparkSession, dataframes: dict, currentio: Optional[DataLakeIO]) -> DataFrame:
        fares = dataframes.get('fares')
        uberfares = dataframes.get('uberfares').select('trip_id','pickup_hour')
        interim_tables = self.intermediateTables(
            uberfares=uberfares,
            fares=fares
        )
        self.SaveInterimTables(
            interimtables=interim_tables,
            spark=spark
        )
        # Directly union all interim tables without schema standardization
        all_dataframes = list(interim_tables.values())

        # Add a "time_period" column to each DataFrame for identification
        for i, df in enumerate(all_dataframes):
            # Identify the time period based on the original key
            original_key = list(interim_tables.keys())[i]
            if 'hourly' in original_key:
                period = 'hourly'
            elif 'daily' in original_key:
                period = 'daily'
            elif 'monthly' in original_key:
                period = 'monthly'
            elif 'weekly' in original_key:
                period = 'weekly'
            elif 'yearly' in original_key:
                period = 'yearly'
            elif 'peak' in original_key:
                period = 'peak'
            elif 'non_peak' in original_key:
                period = 'non_peak'
            elif 'weekend' in original_key:
                period = 'weekend'
            elif 'weekday' in original_key:
                period = 'weekday'
            else:
                period = 'unknown'

            all_dataframes[i] = df.withColumn("time_period", lit(period))

        # Union all DataFrames
        if not all_dataframes:
            return spark.createDataFrame([], StructType())  # Return empty DataFrame if no data

        timeseries_data = reduce(
            lambda x, y: x.unionByName(y, allowMissingColumns=True),
            all_dataframes
        )

        return timeseries_data




class Harmonizer:
    _harmonizer_map = {
        "fares": FareHarmonizer,
        "weatherimpact": WeatherImpactHarmonizer,
        "timeseries": TimeSeriesHarmonizer
    }

    def __init__(self,table,loadtype: str,runtype: str = 'full'):
        self.table = table
        self.loadtype = loadtype
        self.runtype = runtype
        self.harmonizer_class = self._harmonizer_map.get(table)

        if not self.harmonizer_class:
            raise ValueError(f"No harmonizer found for source: {table}")
        self.harmonizer_instance = self.harmonizer_class(
            loadtype=self.loadtype,
            runtype=self.runtype
        )

    def harmonize(self,spark: SparkSession ,dataframes: dict, currentio: Optional[DataLakeIO]) -> DataFrame:
        """Instance method to harmonize data using the selected harmonizer"""
        return self.harmonizer_instance.harmonize(
            spark=spark,
            dataframes=dataframes,
            currentio=currentio
        )


