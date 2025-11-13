
from prefect import flow, task
from Resturant.SourceFact.NoteBooks import Process_SourceFact,Process_SourceDimension
from prefect import get_run_logger
from prefect_dask.task_runners import DaskTaskRunner

@task(name="Load_SourceDimensions", tags=["sourceDimensions", "etl"])
def load_source_dimensions_task(load_type: str,runtype: str = 'prod'):
    """Task to process source dimension data"""
    Process_SourceDimension.main(
        loadtype=load_type,
        runtype=runtype
    )

@task(name="Load_SourceFacts_Sales", tags=["sourceFacts", "etl", "fact_sales"])
def load_sales_facts_task(source_object: str, load_type: str,runtype: str = 'prod'):
    """Task to process source fact data"""
    Process_SourceFact.main(
        table=source_object,
        loadtype=load_type,
        runtype=runtype
    )

@task(name="Load_SourceFacts_Kitchen", tags=["sourceFacts", "etl", "fact_kitchen"])
def load_kitchen_facts_task(source_object: str, load_type: str,runtype: str = 'prod'):
    """Task to process kitchen fact data"""
    Process_SourceFact.main(
        table=source_object,
        loadtype=load_type,
        runtype=runtype
    )

@task(name="Load_SourceFacts_Stock", tags=["sourceFacts", "etl", "fact_stock"])
def load_stock_facts_task(source_object: str, load_type: str,runtype: str = 'prod'):
    """Task to process stock fact data"""
    Process_SourceFact.main(
        table=source_object,
        loadtype=load_type,
        runtype=runtype
    )

@flow(
    name="Restaurant_Raw_Data_Pipeline",
    task_runner = DaskTaskRunner(),
    description="ETL pipeline for processing restaurant raw data into source dimensions and facts"
)
def restaurant_raw_data_pipeline(
    load_type: str,
    runtype: str = 'prod'
):
    """Main workflow to orchestrate the ETL process for restaurant raw data"""
    logger = get_run_logger()
    logger.info("Starting Restaurant Raw Data ETL Pipeline")

    load_source_dimensions_task(
        load_type=load_type,
        runtype=runtype
    )
    downstream_dependencies = [load_source_dimensions_task]
    load_sales_facts_task(
        source_object='fact_sales',
        load_type=load_type,
        runtype=runtype,
        wait_for=downstream_dependencies
    )

    load_kitchen_facts_task(
        source_object='fact_kitchen',
        load_type=load_type,
        runtype=runtype,
        wait_for=downstream_dependencies
    )

    load_stock_facts_task(
        source_object='fact_stock',
        load_type=load_type,
        runtype=runtype,
        wait_for=downstream_dependencies
    )
    downstream_dependencies.append(load_sales_facts_task)
    downstream_dependencies.append(load_kitchen_facts_task)
    downstream_dependencies.append(load_stock_facts_task)

    logger.info("Completed Restaurant Raw Data ETL Pipeline")

if __name__ == "__main__":
    restaurant_raw_data_pipeline(
        load_type="full",
        runtype="prod"
    )