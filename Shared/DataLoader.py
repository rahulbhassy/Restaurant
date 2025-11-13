from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from Shared.connection import JDBC_URL, JDBC_PROPERTIES
import time


class DataLoader:
    """
    DataLoader for CSV, Delta, Parquet, JDBC, and GeoJSON formats with optional schema support.

    filetype options:
      - 'csv'
      - 'delta'
      - 'parquet'
      - 'jdbc'
      - 'geojson'
    """

    def __init__(self, filetype: str,path: str = None,query: str = None, loadtype: str = None, schema: StructType = None):
        self.path = path
        self.schema = schema
        self.filetype = filetype.lower()
        self.loadtype = loadtype
        self.query = query

        print("\n" + "=" * 80)
        print("DATA LOADER INITIALIZED")
        print("=" * 80)
        print(f" Path:       {self.path}")
        print(f" Format:     {self.filetype.upper()}")
        print(f" Schema:     {'Provided' if self.schema else 'Inferred'}")
        print(f" Load Type:  {self.loadtype if self.loadtype else 'Default'}")
        print("=" * 80)

    def LoadData(self, spark: SparkSession):
        """
        Loads data from the path depending on filetype.

        :param spark: SparkSession instance
        :return: DataFrame
        """
        start_time = time.time()
        print(f"\n Loading {self.filetype.upper()} data from: {self.path}")

        try:
            # CSV
            if self.filetype == 'csv':
                print(" Using CSV loader with options: header=True")
                reader = spark.read.option("header", True)
                if self.schema:
                    print(" Applying custom schema")
                    reader = reader.schema(self.schema)
                df = reader.csv(self.path)

            # Delta or Parquet
            elif self.filetype in ('delta', 'parquet'):
                print(f" Using {self.filetype.upper()} loader")
                df = spark.read.format(self.filetype).load(self.path)

            # JDBC
            elif self.filetype == 'jdbc':
                print("🗄️  Using JDBC loader")
                print(f"   URL: {JDBC_URL}")
                print(f"   User: {JDBC_PROPERTIES['user']}")

                # ----------------------------------------------------------
                # Support two modes:
                #   1️⃣ Table name only (self.path = "fact_sales")
                #   2️⃣ Query (self.query = "SELECT * FROM fact_sales WHERE order_date = '2025-11-12'")
                # ----------------------------------------------------------

                # Default: use dbtable (full table read)
                jdbc_options = {
                    "url": JDBC_URL,
                    "user": JDBC_PROPERTIES['user'],
                    "password": JDBC_PROPERTIES['password'],
                    "driver": JDBC_PROPERTIES['driver']
                }

                if hasattr(self, "query") and self.query:
                    # Use a query instead of full table
                    print(f"   Executing SQL Query:\n   {self.query}")
                    # Wrap the query in parentheses and alias as a subquery
                    jdbc_options["dbtable"] = f"({self.query}) AS tmp"
                elif self.path:
                    print(f"   Table: {self.path}")
                    jdbc_options["dbtable"] = self.path
                else:
                    raise ValueError(
                        "❌ Either 'self.path' (table name) or 'self.query' must be provided for JDBC loader.")

                # Load DataFrame
                df = (
                    spark.read
                    .format("jdbc")
                    .options(**jdbc_options)
                    .load()
                )

            # GeoJSON
            elif self.filetype == 'geojson':
                print(" Using GeoJSON loader with multiline=True")
                reader = spark.read.option("multiline", "true")
                if self.schema:
                    print("🔧 Applying custom schema")
                    reader = reader.schema(self.schema)
                df = reader.json(self.path)
            elif self.filetype == 'json':
                print(" Using JSON loader with multiline=True")
                reader = spark.read.option("multiline", "true")
                if self.schema:
                    print("🔧 Applying custom schema")
                    reader = reader.schema(self.schema)
                df = reader.json(self.path)
            else:
                raise ValueError(f"Unsupported filetype '{self.filetype}'")

            # Post-load analysis
            load_time = time.time() - start_time
            print(f"\n Successfully loaded data in {load_time:.2f} seconds")
            return df

        except Exception as e:
            load_time = time.time() - start_time
            print("\n" + "=" * 80)
            print(f" ERROR LOADING DATA (after {load_time:.2f}s)")
            print("=" * 80)
            print(f"Error Type:    {type(e).__name__}")
            print(f"Error Message: {str(e)[:500]}")
            print("=" * 80)
            raise  # Re-raise exception after logging