from prefect import flow, task
from prefect_dask.task_runners import DaskTaskRunner
from prefect import get_run_logger
from EnrichFare.NoteBooks import Process_FareTablesRefresh
from Balancing.NoteBooks import Process_Balancing
from PowerBIRefresh_Pipeline import powerbirefresh_flow
from Optimize_Pipeline import optimize_flow

@task(name="Enrich_Fare_Table", tags=["enrich", "fare"])
def enrich_fare_tables_task(table: str, loadtype: str, runtype: str = 'prod'):
    """Task to enrich fare tables"""
    logger = get_run_logger()
    logger.info("Enriching fare tables")
    Process_FareTablesRefresh.main(
        table=table,
        loadtype=loadtype,
        runtype=runtype
    )

@task(name="Enrich_WeatherImpact_TableRefresh", tags=["enrich", "weatherimpact", "refresh"])
def enrich_weatherimpact_table_task(table: str, loadtype: str, runtype: str = 'prod'):
    """Task to refresh WeatherImpact table"""
    logger = get_run_logger()
    logger.info("Refreshing WeatherImpact table")
    Process_FareTablesRefresh.main(
        table=table,
        loadtype=loadtype,
        runtype=runtype
    )

@task(name="Enrich_TimeSeries_TableRefresh", tags=["enrich", "timeseries","refresh"])
def enrich_timeseries_table_task(table: str, loadtype: str, runtype: str = 'prod'):
    """Task to refresh TimeSeries table"""
    logger = get_run_logger()
    logger.info("Refreshing TimeSeries table")
    Process_FareTablesRefresh.main(
        table=table,
        loadtype=loadtype,
        runtype=runtype
    )

@task(name="Load_Balancing_EnrichGRP2", tags=["balancing", "etl"])
def load_balancing_enrichgrp2_task(load_type: str,runtype: str = 'prod'):
    """Task to process balancing results"""
    Process_Balancing.main(
        runtype=runtype,
        loadtype=load_type,
        tables=['fares','weatherimpact']
    )

@flow(
    name="Enrich_Uber_GRP2_Processing_Pipeline",
    task_runner=DaskTaskRunner(),  # Remove for sequential execution
    description="ETL pipeline for Uber data processing",
    version="1.0"
)
def enrich_grp2_processing_flow(load_type: str, runtype: str = 'prod'):
    """Orchestrates Uber data processing workflow"""
    logger = get_run_logger()
    logger.info(f"Starting pipeline with load_type: {load_type}")

    enrich_fare_tables_task(
        table="fares",
        loadtype=load_type,
        runtype=runtype
    )

    enrich_weatherimpact_table_task(
        table="weatherimpact",
        loadtype='full',
        runtype=runtype,
        wait_for=[enrich_fare_tables_task]
    )

    enrich_timeseries_table_task(
        table="timeseries",
        loadtype='full',
        runtype=runtype,
        wait_for=[enrich_fare_tables_task]
    )

    downstream_dependencies = [
        enrich_fare_tables_task,
        enrich_weatherimpact_table_task,
        enrich_timeseries_table_task
    ]
    tables = ['fares','weatherimpact','timeseries']
    for table in tables:
        optimize_flow(
            tabletype='enrich',
            load_type='full',
            runtype=runtype,
            table=table,
            altertable=False,
            wait_for=downstream_dependencies
        )
    downstream_dependencies.append(optimize_flow)

    load_balancing_enrichgrp2_task(
        load_type=load_type,
        runtype=runtype,
        wait_for=downstream_dependencies
    )
    downstream_dependencies.append(load_balancing_enrichgrp2_task)

    logger.info("Starting PowerBI Refresh")
    powerbirefresh_flow(
        configname=tables,
        loadtype='full',
        runtype=runtype,
        wait_for=downstream_dependencies
    )

if __name__ == "__main__":
    # Example execution
    enrich_grp2_processing_flow(
        load_type="delta",
        runtype="prod"
    )