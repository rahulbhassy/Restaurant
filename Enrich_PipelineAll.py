
from prefect import flow, task
from prefect_dask.task_runners import DaskTaskRunner
from prefect import get_run_logger
from Enrich_PipelineGRP1 import enrich_grp1_processing_flow
from Enrich_PipelineGRP2 import enrich_grp2_processing_flow
from Enrich_PipelineGRP3 import enrich_grp3_processing_flow
from Enrich_PipelineGRP4 import enrich_grp4_processing_flow


@flow(
    name="Enrich_Uber_Processing_Pipeline",
    task_runner=DaskTaskRunner(),  # Remove for sequential execution
    description="ETL pipeline for Uber data processing",
    version="1.0"
)
def enrich_processing_flow(load_type: str,runtype: str = 'prod'):
    """Orchestrates the entire Uber data processing workflow"""
    logger = get_run_logger()
    logger.info(f"Starting master pipeline with load_type: {load_type}")

    # Execute enrichment flow for Group 1
    logger.info("Starting enrichment flow for Group 1")
    enrich_grp1_processing_flow(
        load_type=load_type,
        runtype=runtype,
    )
    # Execute enrichment flow for Group 2
    logger.info("Starting enrichment flow for Group 2")
    enrich_grp2_processing_flow(
        load_type=load_type,
        runtype=runtype,
        wait_for=[enrich_grp1_processing_flow]
    )
    downstream_dependencies = [enrich_grp1_processing_flow,enrich_grp2_processing_flow]
    logger.info("Starting enrichment flow for Group 3")
    enrich_grp3_processing_flow(
        load_type='full',
        runtype=runtype,
        initial_load='yes',
        wait_for=downstream_dependencies

    )
    enrich_grp4_processing_flow(
        load_type='full',
        runtype=runtype,
        initial_load='yes',
        wait_for=downstream_dependencies
    )


if __name__ == "__main__":
    # Example usage
    enrich_processing_flow(
        load_type="delta",  # or "full" based on your requirement
        runtype='prod'  # or 'dev' based on your environment
    )
