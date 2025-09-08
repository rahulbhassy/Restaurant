from typing import Optional

from prefect import flow, task
from prefect_dask.task_runners import DaskTaskRunner
from Shared import optimize

@task(name="Optimizer", tags=["Optimize storage", "etl"])
def optimise_deltalake(tabletype: str, load_type: str,table: Optional[str]=None,runtype: str = 'dev',altertable: bool = False):
    """Task to process Uber fares data"""
    optimize.main(
        tabletype=tabletype,
        loadtype=load_type,
        runtype=runtype,
        table=table,
        altertable=altertable
    )

@flow(
    name="Optimizer_Pipeline",
    task_runner=DaskTaskRunner(),  # Remove for sequential execution
    description="ETL pipeline for Uber data processing",
    version="1.0"
)
def optimize_flow(tabletype: str,load_type: str,table: Optional[str]=None,runtype: str = 'dev',altertable: bool = False):
    optimise_deltalake(
        tabletype=tabletype,
        load_type=load_type,
        runtype=runtype,
        table=table,
        altertable=altertable
    )
if __name__ == "__main__":

    optimize_flow(
        tabletype='raw',
        load_type='full',
        runtype='prod',
        altertable=False
    )