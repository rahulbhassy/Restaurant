from prefect import flow, task
from Resturant.EnrichOutlet.NoteBooks import Process_EnrichOutlet
from prefect import get_run_logger
from prefect_dask.task_runners import DaskTaskRunner

@task(name="Load_EnrichedOutlet", tags=["enrichedOutlet", "etl"])
def load_enriched_outlet_task(source_object: str, load_type: str,runtype: str = 'prod'):
    """Task to process enriched outlet data"""
    Process_EnrichOutlet.main(
        table=source_object,
        loadtype=load_type,
        runtype=runtype
    )

@flow(
    name="Restaurant_EnrichedOutlet_Pipeline",
    task_runner = DaskTaskRunner(),
    description="ETL pipeline for processing restaurant enriched outlet data"
)
def restaurant_enriched_outlet_pipeline(
    load_type: str,
    runtype: str = 'prod'
):
    """Main workflow to orchestrate the ETL process for restaurant enriched outlet data"""
    logger = get_run_logger()
    logger.info("Starting Restaurant Enriched Outlet Data ETL Pipeline")

    downstream_dependencies = []
    load_enriched_outlet_task(
        source_object='outlet_performance',
        load_type=load_type,
        runtype=runtype
    )
    downstream_dependencies.append(load_enriched_outlet_task)

if __name__ == "__main__":
    restaurant_enriched_outlet_pipeline(
        load_type='full',
        runtype='prod'
    )