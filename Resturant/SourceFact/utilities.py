from pyspark.sql import SparkSession
from Shared.DataLoader import DataLoader

class TableLoader:
    def __init__(self, table_name: str,delta_column: str=None,start_delta: str=None, end_delta: str=None):
        self.table_name = table_name
        self.start_delta = start_delta
        self.end_delta = end_delta
        self.delta_column = delta_column

    def base_query(self) -> str:
        return f"SELECT * FROM {self.table_name}"

    def delta_query(self) -> str:
        if self.delta_column is not None:
            if self.end_delta is not None and self.start_delta is not None:
                return (f"{self.base_query()} WHERE {self.delta_column} > '{self.start_delta}' "
                        f"AND {self.delta_column} <= '{self.end_delta}'")
            elif self.end_delta is not None:
                return f"{self.base_query()} WHERE {self.delta_column} <= '{self.end_delta}'"
            elif self.end_delta is None and self.start_delta is None:
                return self.base_query()
        else:
            return self.base_query()

    def load_data(self,spark: SparkSession):
        query = self.delta_query()
        loader = DataLoader(filetype="jdbc", query=query)
        return loader.LoadData(spark)





