from typing import List
from prefect import flow, task
from prefect_dask.task_runners import DaskTaskRunner
from prefect import get_run_logger
from EnrichPeople.NoteBooks import Process_PeopleTables_Refresh
from Balancing.NoteBooks import Process_Balancing
from PowerBIRefresh_Pipeline import powerbirefresh_flow
from Optimize_Pipeline import optimize_flow

@task(name="Enrich_People_Tables", tags=["enrich", "people", "customerprofile"])
def enrich_profile_tables_task(table: str, loadtype: str, runtype: str = 'prod',initial_load: str = 'no'):
    """Task to enrich Uber data with people information"""
    logger = get_run_logger()
    logger.info("Processing Customer Profile")
    Process_PeopleTables_Refresh.main(
        table=table,
        loadtype=loadtype,
        runtype=runtype,
        initial_load=initial_load
    )

@task(name="Enrich_People_Tables", tags=["enrich", "people", "customerpreference"])
def enrich_preference_tables_task(table: str, loadtype: str, runtype: str = 'prod',initial_load: str = 'no'):
    """Task to enrich Uber data with people information"""
    logger = get_run_logger()
    logger.info("Processing Customer Preference")
    Process_PeopleTables_Refresh.main(
        table=table,
        loadtype=loadtype,
        runtype=runtype,
        initial_load=initial_load
    )

@task(name="Load_Balancing_EnrichGRP3", tags=["balancing", "etl"])
def load_balancing_enrichgrp3_task(load_type: str,tables: List[str],runtype: str = 'prod'):
    """Task to process balancing results"""
    Process_Balancing.main(
        runtype=runtype,
        loadtype=load_type,
        tables=tables
    )

@flow(
    name="Enrich_Uber_GRP3_Processing_Pipeline",
    task_runner=DaskTaskRunner(),  # Remove for sequential execution
    description="ETL pipeline for Uber data processing",
    version="1.0"
)
def enrich_grp3_processing_flow(load_type: str, runtype: str = 'prod',initial_load: str = 'no',optimize: bool = False):
    """Orchestrates Uber data processing workflow"""
    logger = get_run_logger()
    logger.info(f"Starting pipeline with load_type: {load_type}")


    enrich_profile_tables_task(
        table="customerprofile",
        loadtype=load_type,
        runtype=runtype,
        initial_load=initial_load
    )
    downstream_dependencies = [enrich_profile_tables_task]

    enrich_preference_tables_task(
        table="customerpreference",
        loadtype=load_type,
        runtype=runtype,
        initial_load=initial_load,
        wait_for=downstream_dependencies
    )
    downstream_dependencies.append(enrich_preference_tables_task)
    tables = ['customerprofile','customerpreference']

    if optimize:
        for table in tables:
            optimize_flow(
                tabletype='enrich',
                load_type=load_type,
                runtype=runtype,
                table=table,
                altertable=True,
                wait_for=downstream_dependencies
            )
    downstream_dependencies.append(optimize_flow)

    load_balancing_enrichgrp3_task(
        load_type='full',
        tables=tables,
        runtype=runtype,
        wait_for=downstream_dependencies
    )
    downstream_dependencies.append(load_balancing_enrichgrp3_task)

    logger.info("Starting PowerBI Refresh")

    powerbirefresh_flow(
        configname=tables,
        loadtype='full',
        runtype=runtype,
        wait_for=downstream_dependencies
    )

if __name__ == "__main__":
    # Example execution
    enrich_grp3_processing_flow(
        load_type="full",
        runtype="prod",
        initial_load='yes',
        optimize=False
    )