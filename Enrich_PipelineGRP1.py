from asyncio import wait_for

from prefect import flow, task
from prefect_dask.task_runners import DaskTaskRunner
from prefect import get_run_logger
from EnrichGeoSpatial.NoteBooks import Process_GeospatialTablesRefresh
from Balancing.NoteBooks import Process_Balancing
from Optimize_Pipeline import optimize_flow


@task(name="Enrich_Geospatial_Tables", tags=["enrich", "geospatial"])
def enrich_geospatial_uber_task(uber: str,borough: str,trip : str, loadtype: str, runtype: str = 'prod'):
    """Task to enrich Uber data with geospatial information"""
    logger = get_run_logger()
    logger.info("Enriching Uber data with geospatial information")
    Process_GeospatialTablesRefresh.main(
        uber=uber,
        borough=borough,
        trip=trip,
        loadtype=loadtype,
        runtype=runtype
    )

@task(name="Load_Balancing_EnrichGRP1", tags=["balancing", "etl"])
def load_balancing_enrichgrp1_task(load_type: str,runtype: str = 'prod'):
    """Task to process balancing results"""
    Process_Balancing.main(
        runtype=runtype,
        loadtype=load_type,
        tables=['uber','uberfaresenrich']
    )

@flow(
    name="Enrich_Uber_GRP1_Processing_Pipeline",
    task_runner=DaskTaskRunner(),  # Remove for sequential execution
    description="ETL pipeline for Uber data processing",
    version="1.0"
)
def enrich_grp1_processing_flow(load_type: str,runtype: str = 'prod'):
    """Orchestrates Uber data processing workflow"""
    logger = get_run_logger()
    logger.info(f"Starting pipeline with load_type: {load_type}")


    enrich_geospatial_uber_task(
        uber="uberfares",
        borough="features",
        trip="tripdetails",
        loadtype=load_type,
        runtype=runtype,
    )
    downstream_dependencies = [enrich_geospatial_uber_task]
    optimize_flow(
        tabletype='spatial',
        load_type='full',
        runtype=runtype,
        table='uber',
        altertable=False,
        wait_for=downstream_dependencies
    )
    downstream_dependencies.append(optimize_flow)
    load_balancing_enrichgrp1_task(
        load_type='full',
        runtype=runtype,
        wait_for=downstream_dependencies
    )

if __name__ == "__main__":
    # Example execution
    enrich_grp1_processing_flow(
        load_type="delta",
        runtype="prod"
    )
