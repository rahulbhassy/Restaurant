from prefect import flow, task
from Resturant.EnrichFact.NoteBooks import Process_EnrichFact
from prefect import get_run_logger
from prefect_dask.task_runners import DaskTaskRunner

@task(name="Load_EnrichedFacts_sales", tags=["enrichedFacts", "etl","sales"])
def load_enriched_sales_facts_task(source_object: str, load_type: str,runtype: str = 'prod'):
    """Task to process enriched sales fact data"""
    Process_EnrichFact.main(
        table=source_object,
        loadtype=load_type,
        runtype=runtype
    )

@task(name="Load_EnrichedFacts_kitchen", tags=["enrichedFacts", "etl","kitchen"])
def load_enriched_kitchen_facts_task(source_object: str, load_type: str,runtype: str = 'prod'):
    """Task to process enriched kitchen fact data"""
    Process_EnrichFact.main(
        table=source_object,
        loadtype=load_type,
        runtype=runtype
    )
@task(name="Load_EnrichedFacts_stock", tags=["enrichedFacts", "etl","stock"])
def load_enriched_stock_facts_task(source_object: str, load_type: str,runtype: str = 'prod'):
    """Task to process enriched stock fact data"""
    Process_EnrichFact.main(
        table=source_object,
        loadtype=load_type,
        runtype=runtype
    )

@flow(
    name="Restaurant_EnrichedFact_Pipeline",
    task_runner = DaskTaskRunner(),
    description="ETL pipeline for processing restaurant enriched fact data"
)
def restaurant_enriched_fact_pipeline(
    load_type: str,
    runtype: str = 'prod'
):
    """Main workflow to orchestrate the ETL process for restaurant enriched fact data"""
    logger = get_run_logger()
    logger.info("Starting Restaurant Enriched Fact Data ETL Pipeline")

    downstream_dependencies = []
    load_enriched_sales_facts_task(
        source_object='fact_sales_enriched',
        load_type=load_type,
        runtype=runtype
    )
    load_enriched_kitchen_facts_task(
        source_object='fact_kitchen_enriched',
        load_type=load_type,
        runtype=runtype
    )
    load_enriched_stock_facts_task(
        source_object='fact_stock_enriched',
        load_type=load_type,
        runtype=runtype
    )
    downstream_dependencies.append(load_enriched_sales_facts_task)
    downstream_dependencies.append(load_enriched_kitchen_facts_task)
    downstream_dependencies.append(load_enriched_stock_facts_task)

if __name__ == "__main__":
    restaurant_enriched_fact_pipeline(
        load_type='full',
        runtype='prod'
    )