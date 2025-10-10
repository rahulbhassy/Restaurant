
import json
from pathlib import Path
from dash import Dash, dash_table, html
import dash_bootstrap_components as dbc
from dash_bootstrap_components import Card, CardBody
from pyspark.sql import DataFrame
from shapely.geometry import Point, Polygon, LineString
from pyspark.sql import SparkSession
from Shared.DataLoader import DataLoader
from delta.tables import DeltaTable

# -- CONFIG --
import os
from datetime import datetime
from typing import List, Optional

DATALAKE_PREFIX = r"C:\Users\HP\uber_project\Data"


class DataLakeIO:
    _RAW_TABLES = frozenset({
        "uberfares", "tripdetails", "driverdetails",
        "customerdetails", "vehicledetails", "features" , "weatherdetails"
    })
    _INPUT_SUFFIX = ".geojson"

    # centralized layer‐to‐path mapping
    _LAYER_MAP = {
        'raw': {
            'features': ['Raw', 'boroughs'],
            '__default__': ['Raw']
        },
        'input': {
            '__default__': ['Input', 'Borough']
        },
        'enrich': {
            'uber': ['Enrich', 'Enriched', 'spatial'],
            'uberfares|enrichweather': ['Enrich', 'Enriched_Weather_uberData'],
            '__default__': ['Enrich', 'Enriched']
        },
        'system': {
            '__default__': ['System','BalancingResults']
        }
    }

    def __init__(
            self,
            process: str,
            table: str,
            loadtype: str,
            state: Optional[str] = 'current',
            runtype: Optional[str] = 'prod',
            layer: Optional[str] = None,
    ):
        self.process = process.lower()
        if layer:
            self.layer = layer.lower()
        else:
            self.layer = layer
        self.table = table.lower()
        self.state = state.lower()
        self.runtype = runtype.lower()
        self.loadtype = loadtype

    def _get_layer_parts(self) -> List[str]:
        cfg = self._LAYER_MAP.get(self.layer, {})
        # special keys
        if self.layer == 'raw' and self.table == 'features':
            return cfg['features']
        if self.layer == 'enrich' and self.table == 'uber':
            return cfg['uber']
        if self.layer == 'enrich' and self.table == 'uberfares' and self.process == 'enrichweather':
            return cfg['uberfares|enrichweather']
        # fallback
        return cfg.get('__default__', [])

    def file_ext(self) -> str:
        if self.process == 'load':
            if self.table in self._RAW_TABLES:
                return 'csv'
            if self.table.endswith(self._INPUT_SUFFIX):
                return 'geojson'
        if self.process in ('read', 'write','enrichweather'):
            return 'delta' if self.state == 'current' else 'parquet'

    def _build_path(self, parts: List[str]) -> str:
        if self.runtype == 'dev':
            return os.path.join(DATALAKE_PREFIX.rstrip(os.sep), 'Sandbox', *parts)
        return os.path.join(DATALAKE_PREFIX.rstrip(os.sep), *parts)

    def filepath(self, date: Optional[str] = None) -> str:
        ext = self.file_ext()

        if self.table.endswith(self._INPUT_SUFFIX):
            parts = self._get_layer_parts() + [self.table]
            return self._build_path(parts)

        if self.process == 'load':
            if self.table in self._RAW_TABLES:
                # YYYY‑MM‑DD
                folder = date or datetime.now().strftime('%Y-%m-%d')
                if self.loadtype == 'full':
                    return self._build_path(['DataSource', '*', f"{self.table}.csv"])
                # delta or default
                return self._build_path(['DataSource', folder, f"{self.table}.csv"])

            raise ValueError(f"Can't load table '{self.table}' in process 'load'")

        if self.process in ('read', 'write','enrichweather'):
            parts = self._get_layer_parts()
            if self.state == 'current':
                parts = parts + [self.table, self.state, f"{self.table}.{ext}"]
            elif self.state == 'delta':
                folder = date or datetime.now().strftime('%Y-%m-%d')
                parts = parts + [self.table, self.state,folder, f"{self.table}.{ext}"]
            return self._build_path(parts)

        raise ValueError(f"Unknown process '{self.process}'")

class IntermediateIO:
    _TABLES = frozenset([
        "uberfares", "tripdetails", "driverdetails", "weatherimpact", "balancingresults" , "BalancingResults",
        "customerdetails", "vehicledetails", "uber","features", "weatherdetails" , "fares" , "timeseries",
        "customerprofile","driverprofile","customerpreference","driverpreference","driverperformance"
    ])

    def __init__(self, fullpath: str, date: str = None):
        self.fullpath = Path(fullpath)
        self.date = date
        # derive sourceobject once
        self.sourceobject = self._derive_sourceobject()

    def _derive_sourceobject(self) -> str:
        """
        Peel off the DATALAKE_PREFIX and pick the first TABLE name we hit.
        """
        dlprefix = Path(DATALAKE_PREFIX)
        rel = self.fullpath.relative_to(dlprefix)
        for part in rel.parts:
            if part in self._TABLES:
                return part
        raise ValueError(f"No known table found in {self.fullpath}")

    def _get_intermediate_path(self) -> Path:
        """
        Builds <DATALAKE_PREFIX>/.../<sourceobject> (with trailing slash).
        """
        dlprefix = Path(DATALAKE_PREFIX)
        rel = self.fullpath.relative_to(dlprefix)
        ipath = dlprefix
        for part in rel.parts:
            ipath = ipath / part
            if part == self.sourceobject:
                break
        return ipath  # Note: no trailing slash here; Path handles it

    def get_deltapath(self) -> str:
        """
        Returns a string path:
          <intermediatepath>/delta/YYYY-MM-DD/<sourceobject>.parquet
        """
        ipath        = self._get_intermediate_path()
        today_folder = datetime.now().strftime('%Y-%m-%d')
        if self.date:
            today_folder = self.date
        delta_path   = ipath / "delta" / today_folder / f"{self.sourceobject}.parquet"
        return str(delta_path)

class GeoJsonIO:
    def __init__(self, input_filename: str, path: str, validator_func=None):
        self.input_filename = input_filename
        self.validator_func = validator_func
        self.path = path

    def load(self):
        """Load GeoJSON data from file."""
        with open(self.path, 'r') as file:
            return json.load(file)

    def validate_and_fix_geojson(self, validator_func=None):
        """
        Loads, validates, and writes back a fixed GeoJSON file.

        :param input_filename: name of the original GeoJSON file
        :param output_filename: name of the fixed output GeoJSON file
        :param validator_func: a callable that accepts GeoJSON dict and returns a fixed version
        """
        # Use provided validator_func or fall back to instance variable
        validator = validator_func or self.validator_func
        if validator is None:
            raise ValueError("A `validator_func` must be provided to validate and fix the GeoJSON.")

        # Load, validate, and write fixed GeoJSON
        geojson_input = self.load()
        fixed_geojson = validator(geojson_input)

        output_filename = f"fixed_{self.input_filename}"
        datalake_io = DataLakeIO(
            process='write',
            table=output_filename,
            loadtype='full',
            layer='input'
        )
        fixed_geojson_path = datalake_io.filepath()

        with open(fixed_geojson_path, 'w') as file:
            json.dump(fixed_geojson, file, indent=4)

        print("GeoJSON validation and fixing completed successfully.")
        return output_filename

class MergeIO:
    def __init__(self,table: str, currentio: DataLakeIO,key_columns: List[str],updateitems: Optional[List[str]] =None):
        self.table = table
        self.currentio = currentio
        self.updateitems = updateitems
        self.key_columns = key_columns

    def merge(self,spark:SparkSession,updated_df: DataFrame):
        """
        Merges the updated DataFrame into the current Delta table.

        :param updated_df: DataFrame with new or updated records.
        :param key_columns: List of columns to use as keys for the merge operation.
        """
        print(f"Starting merge operation for table: {self.table}")
        deltaTable = DeltaTable.forPath(spark, self.currentio.filepath())

        update_expr = {}
        if self.updateitems is None:
            update_expr = {col: f"s.{col}" for col in updated_df.columns}
        else:
            update_expr = {col: f"s.{col}" for col in self.updateitems}

        # Build insert dict dynamically (insert all columns from source DF)
        insert_expr = {col: f"s.{col}" for col in updated_df.columns}
        merge_condition = " AND ".join([f"t.{k} = s.{k}" for k in self.key_columns])
        (deltaTable.alias("t")
         .merge(
            updated_df.alias("s"),
            merge_condition
        )
         .whenMatchedUpdate(set=update_expr)  # parameterized updates
         .whenNotMatchedInsert(values=insert_expr)  # insert all cols
         .execute()
         )
        print(f"Merge operation completed successfully for table: {self.table}")

class DeltaLakeOps:
    def __init__(self,path: str,table: str):
        self.path = path
        self.table = table

    def getHistory(self,spark: SparkSession,count: bool = False):
        historyDF = spark.sql(f"DESCRIBE HISTORY delta.`{self.path}`")
        historyDF.show()
        if count:
            # 2. Collect just the version numbers
            versions = [row.version for row in historyDF.select("version").collect()]

            # 3. For each version, issue a COUNT(*) query
            results = []
            for v in versions:
                cnt = spark.sql(f"""
                    SELECT {v} AS version,
                           COUNT(*) AS record_count
                      FROM delta.`{self.path}` VERSION AS OF {v}
                """).collect()[0]
                results.append((cnt.version, cnt.record_count))

            # 4. Display all at once
            for version, count in results:
                print(f"Version {version:>2} → {count} rows")

    def restore(self,spark: SparkSession, version: int):
        """
        Restores the Delta table to a specific version.

        :param version: The version number to restore to.
        """
        spark.sql(f"""
            RESTORE delta.`{self.path}`
            TO VERSION AS OF {version};
        """)
        print(f"Restored Delta table at {self.path} to version {version}.")

    def vacuum(self,spark: SparkSession,retention: int):
        spark.sql(f"""
            VACUUM delta.`{self.path}`
            RETAIN {retention} HOURS
        """)
        print(f"Vacuumed {self.table}")

    def optimise(self,spark: SparkSession):
        spark.sql(f"""
            OPTIMIZE delta.`{self.path}`
        """)
        print(f"Optimized {self.table}")

    def altertable(self,spark: SparkSession):
        spark.sql(f"""
            ALTER TABLE delta.`{self.path}` SET TBLPROPERTIES (
                'delta.deletedFileRetentionDuration' = '120 hours',
                'delta.logRetentionDuration' = '120 hours'
            );
        """)
        print(f"Table {self.table} altered.")


import math
from dash import Dash, dash_table, html, dcc, Input, Output, State
import dash_bootstrap_components as dbc
from pyspark.sql import DataFrame
from pyspark.sql.functions import monotonically_increasing_id, row_number, col
from pyspark.sql.window import Window
from shapely.geometry import Point, Polygon, LineString
from pyspark.sql import SparkSession

class SparkTableViewer:
    """
    Dash-based Spark DataFrame viewer with server-side pagination & virtualization for faster browser loads.
    Handles optional 'spatial' geometry serialization.
    """
    def __init__(self, df: DataFrame, table_name: str = '', page_size: int = 20):
        if not hasattr(df, 'columns'):
            raise ValueError("Expected a PySpark DataFrame.")

        self.df = df
        self.columns = df.columns
        self.page_size = page_size
        self.table_name = table_name.lower()

    def _serialize_value(self, val):
        try:
            if hasattr(val, 'toText'):
                return val.toText()
            if isinstance(val, (Point, Polygon, LineString)):
                return val.wkt
            return val
        except Exception:
            return str(val)

    def _get_page(self, page_current, page_size):
        # Add an index column for pagination
        w = Window().orderBy(monotonically_increasing_id())
        indexed = self.df.withColumn(
            "__row_num",
            row_number().over(w) - 1
        )
        start = page_current * page_size
        end = start + page_size
        # Use col() to reference column, not attribute
        page_df = indexed.filter(
            (col("__row_num") >= start) & (col("__row_num") < end)
        ).drop("__row_num")
        rows = page_df.collect()

        # Serialize rows
        result = []
        for row in rows:
            d = {}
            for col_name, val in zip(self.columns, row):
                if self.table_name == 'spatial':
                    d[col_name] = self._serialize_value(val)
                else:
                    d[col_name] = val
            result.append(d)
        return result

    def display(self, host='127.0.0.1', port=8050, debug=False):
        app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

        table = dash_table.DataTable(
            id='spark-table',
            columns=[{"name": c, "id": c} for c in self.columns],
            page_current=0,
            page_size=self.page_size,
            page_action='custom',
            virtualization=True,
            style_table={
                'overflowX': 'auto',
                'overflowY': 'auto',
                'maxHeight': 'calc(100vh - 200px)',
                'width': '100%',
            },
            style_header={
                'backgroundColor': '#004085', 'color': 'white',
                'fontWeight': 'bold', 'textAlign': 'center',
                'position': 'sticky', 'top': 0, 'zIndex': 1,
            },
            style_cell={
                'textAlign': 'left', 'padding': '5px',
                'minWidth': '100px', 'whiteSpace': 'normal',
            },
            style_data_conditional=[{'if': {'row_index': 'odd'}, 'backgroundColor': '#f8f9fa'}]
        )

        card = dbc.Card([
            html.H4("Spark DataFrame Viewer", className="card-title p-2 text-white bg-primary"),
            dbc.CardBody(table)
        ], className="m-4 shadow-sm")

        app.layout = html.Div([card], className="bg-light vh-100")

        @app.callback(
            Output('spark-table', 'data'),
            Input('spark-table', 'page_current'),
            Input('spark-table', 'page_size')
        )
        def update_table(page_current, page_size):
            return self._get_page(page_current, page_size)

        app.run(host=host, port=port, debug=debug)

class SourceObjectAssignment:
    def __init__(self,loadtype: str, sourcetables: List[str],runtype: str = 'prod'):
        self.loadtype = loadtype
        self.runtype = runtype
        self.sourcetables = sourcetables

    def assign_DataLakeIO(self,layer: dict,process: Optional[str] = 'read'):
        """Assigns DataLakeIO objects to each source table based on the load type and runtime type"""
        io_map = {}
        for table in self.sourcetables:
            io_map[table] = DataLakeIO(
                process="enrichweather" if layer.get(table) == 'enrichweather' else process,
                table=table,
                loadtype=self.loadtype,
                state='current',
                layer='enrich' if layer.get(table) == 'enrichweather' else layer.get(table),
                runtype=self.runtype
            )
        return io_map

    def assign_Readers(self,io_map: dict):
        """Creates DataLoader instances for each source table"""
        readers = {}
        for table, io in io_map.items():
            readers[table] = DataLoader(
                path=io.filepath(),
                filetype=io.file_ext(),
                loadtype=self.loadtype,
            )
        return readers

    def getData(self, spark: SparkSession, readers: dict):
        """Loads data from all source tables into a dictionary of DataFrames"""
        dataframes = {}
        for table, reader in readers.items():
            dataframes[table] = reader.LoadData(spark=spark)
        return dataframes