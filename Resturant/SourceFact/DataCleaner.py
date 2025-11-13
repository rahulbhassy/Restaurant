from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *


class DataCleaner:

    def __init__(self, spark,
                 ingestion_time,
                 mandatory_cols=None,
                 cast_config=None,
                 allowed_values=None,
                 duplicate_keys=None,
                 anomaly_rules=None
                 ):
        """
        Parameters
        ----------
        mandatory_cols : list
            Columns that cannot be null or empty.

        cast_config : dict
            Dictionary of column_name -> datatype (e.g., "order_ts": "timestamp")

        allowed_values : dict
            Dictionary of column_name -> allowed list (e.g., "payment_mode": ["Cash", "UPI"])

        duplicate_keys : list
            Columns on which duplicates should be removed

        anomaly_rules : dict
            Dictionary of rules like {"total_order_price": 5000}
        """

        self.spark = spark
        self.ingestion_time = ingestion_time
        self.mandatory_cols = mandatory_cols or []
        self.cast_config = cast_config or {}
        self.allowed_values = allowed_values or {}
        self.duplicate_keys = duplicate_keys or []
        self.anomaly_rules = anomaly_rules or {}

    # ------------------------------
    # Clean string columns (trim)
    # ------------------------------
    def clean_strings(self, df: DataFrame) -> DataFrame:
        for field in df.schema.fields:
            if isinstance(field.dataType, StringType):
                df = df.withColumn(field.name, trim(col(field.name)))
        return df

    # ------------------------------
    # Cast columns using config
    # ------------------------------
    def cast_columns(self, df: DataFrame) -> DataFrame:
        for col_name, dtype in self.cast_config.items():
            if dtype == "timestamp":
                df = df.withColumn(col_name, to_timestamp(col(col_name)))
            elif dtype == "double":
                df = df.withColumn(col_name, col(col_name).cast(DoubleType()))
            elif dtype == "int":
                df = df.withColumn(col_name, col(col_name).cast(IntegerType()))
            elif dtype == "string":
                df = df.withColumn(col_name, col(col_name).cast(StringType()))
        return df

    # ------------------------------
    # Null Checks
    # ------------------------------
    def null_check(self, df: DataFrame) -> DataFrame:
        for col_name in self.mandatory_cols:
            df = df.filter(col(col_name).isNotNull() & (col(col_name) != ""))
        return df

    # ------------------------------
    # Drop duplicates
    # ------------------------------
    def drop_duplicates(self, df: DataFrame) -> DataFrame:
        if self.duplicate_keys:
            return df.dropDuplicates(self.duplicate_keys)
        return df

    # ------------------------------
    # Validate categorical values
    # ------------------------------
    def validate_values(self, df: DataFrame) -> DataFrame:
        for col_name, valid_list in self.allowed_values.items():
            df = df.filter(col(col_name).isin(valid_list))
        return df

    # ------------------------------
    # Anomaly Detection
    # ------------------------------
    def detect_anomalies(self, df: DataFrame) -> DataFrame:
        for col_name, threshold in self.anomaly_rules.items():
            df = df.withColumn(
                f"{col_name}_anomaly_flag",
                when(col(col_name) > threshold, lit(True)).otherwise(lit(False))
            )
        return df

    # ------------------------------
    # Add ingestion timestamp
    # ------------------------------
    def add_ingestion_metadata(self, df: DataFrame) -> DataFrame:
        return df.withColumn("ingested_at", self.ingestion_time)

    # ------------------------------
    # MAIN PIPELINE
    # ------------------------------
    def clean(self, df: DataFrame) -> DataFrame:
        df = self.clean_strings(df)
        df = self.cast_columns(df)
        df = self.null_check(df)
        df = self.drop_duplicates(df)
        df = self.validate_values(df)
        df = self.detect_anomalies(df)
        df = self.add_ingestion_metadata(df)
        return df
