from typing import List
from prefect import flow, task
from prefect_dask.task_runners import DaskTaskRunner
from prefect import get_run_logger
from EnrichPeople.NoteBooks import Process_PeopleTables_Refresh
from PowerBIRefresh_Pipeline import powerbirefresh_flow
from Balancing.NoteBooks import Process_Balancing

@task(name="Enrich_DriverProfile_Table", tags=["enrich", "people", "driverprofile"])
def enrich_profile_table_task(table: str, loadtype: str, runtype: str = 'prod',initial_load: str = 'no'):
    """Task to enrich Uber data with people information """
    logger = get_run_logger()
    logger.info("Processing Driver Profile")
    Process_PeopleTables_Refresh.main(
        table=table,
        loadtype=loadtype,
        runtype=runtype,
        initial_load=initial_load
    )

@task(name="Enrich_DriverPreference_Table", tags=["enrich", "people", "driverpreference"])
def enrich_preference_table_task(table: str, loadtype: str, runtype: str = 'prod',initial_load: str = 'no'):
    """Task to enrich Uber data with people information"""
    logger = get_run_logger()
    logger.info("Processing Driver Preference")
    Process_PeopleTables_Refresh.main(
        table=table,
        loadtype=loadtype,
        runtype=runtype,
        initial_load=initial_load
    )

@task(name="Enrich_DriverSalary_Table", tags=["enrich", "people", "driversalary"])
def enrich_salary_table_task(table: str, loadtype: str, runtype: str = 'prod',initial_load: str = 'no'):
    """Task to enrich Uber data with people information"""
    logger = get_run_logger()
    logger.info("Processing Driver Preference")
    Process_PeopleTables_Refresh.main(
        table=table,
        loadtype=loadtype,
        runtype=runtype,
        initial_load=initial_load
    )

@task(name="Load_Balancing_EnrichGRP2", tags=["balancing", "etl"])
def load_balancing_enrichgrp4_task(load_type: str,tables: List[str],runtype: str = 'prod'):
    """Task to process balancing results"""
    Process_Balancing.main(
        runtype=runtype,
        loadtype=load_type,
        tables=tables
    )


@flow(
    name="Enrich_Uber_GRP4_Processing_Pipeline",
    task_runner=DaskTaskRunner(),  # Remove for sequential execution
    description="ETL pipeline for Uber data processing",
    version="1.0"
)
def enrich_grp4_processing_flow(load_type: str, runtype: str = 'prod',initial_load: str = 'no'):
    """Orchestrates Uber data processing workflow"""
    logger = get_run_logger()
    logger.info(f"Starting pipeline with load_type: {load_type}")

    enrich_profile_table_task(
        table='driverprofile',
        loadtype=load_type,
        runtype=runtype,
        initial_load=initial_load
    )
    enrich_salary_table_task(
        table='driversalary',
        loadtype=load_type,
        runtype=runtype,
        initial_load=initial_load
    )
    downstream_dependencies = [enrich_profile_table_task]
    enrich_preference_table_task(
        table='driverpreference',
        loadtype=load_type,
        runtype=runtype,
        initial_load=initial_load,
        wait_for=downstream_dependencies
    )
    downstream_dependencies.append(enrich_salary_table_task)
    downstream_dependencies.append(enrich_preference_table_task)

    load_balancing_enrichgrp4_task(
        load_type='full',
        tables=['driverprofile','driverpreference','driversalary'],
        runtype=runtype,
        wait_for=downstream_dependencies
    )
    downstream_dependencies.append(load_balancing_enrichgrp4_task)

    logger.info("Starting PowerBI Refresh")

    powerbirefresh_flow(
        configname=['driverprofile','driverpreference','driversalary'],
        loadtype=load_type,
        runtype=runtype,
        wait_for=downstream_dependencies
    )

if __name__ == "__main__":
    # Example execution
    enrich_grp4_processing_flow(
        load_type="full",
        runtype="prod",
        initial_load='yes'
    )
