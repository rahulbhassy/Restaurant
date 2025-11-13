from prefect import flow, task
from Resturant.EnrichPeople.NoteBooks import Process_EnrichPeople
from prefect import get_run_logger
from prefect_dask.task_runners import DaskTaskRunner

@task(name="Load_EnrichPeople_Customer", tags=["enrichedPeople", "etl","customer"])
def load_enriched_customer_people_task(source_object: str, load_type: str,runtype: str = 'prod'):
    """Task to process enriched customer people data"""
    Process_EnrichPeople.main(
        table=source_object,
        loadtype=load_type,
        runtype=runtype
    )

@task(name="Load_EnrichPeople_CustomerPreference", tags=["enrichedPeople", "etl","customer_preference"])
def load_enriched_customer_preference_people_task(source_object: str, load_type: str,runtype: str = 'prod'):
    """Task to process enriched customer preference people data"""
    Process_EnrichPeople.main(
        table=source_object,
        loadtype=load_type,
        runtype=runtype
    )

@task(name="Load_EnrichPeople_Chef", tags=["enrichedPeople", "etl","Chef_performance"])
def load_enriched_chef_people_task(source_object: str, load_type: str,runtype: str = 'prod'):
    """Task to process enriched chef people data"""
    Process_EnrichPeople.main(
        table=source_object,
        loadtype=load_type,
        runtype=runtype
    )

@flow(
    name="Restaurant_EnrichPeople_Pipeline",
    task_runner = DaskTaskRunner(),
    description="ETL pipeline for processing restaurant enriched people data"
)
def restaurant_enrich_people_pipeline(
    load_type: str,
    runtype: str = 'prod'
):
    """Main workflow to orchestrate the ETL process for restaurant enriched people data"""
    logger = get_run_logger()
    logger.info("Starting Restaurant Enriched People Data ETL Pipeline")

    downstream_dependencies = []
    load_enriched_customer_people_task(
        source_object='customer',
        load_type=load_type,
        runtype=runtype
    )
    load_enriched_customer_preference_people_task(
        source_object="customer_preference",
        load_type=load_type,
        runtype=runtype
    )
    load_enriched_chef_people_task(
        source_object='chef_performance',
        load_type=load_type,
        runtype=runtype
    )
    downstream_dependencies.append(load_enriched_customer_people_task)
    downstream_dependencies.append(load_enriched_customer_preference_people_task)
    downstream_dependencies.append(load_enriched_chef_people_task)

if __name__ == "__main__":
    restaurant_enrich_people_pipeline(
        load_type='full',
        runtype='prod'
    )