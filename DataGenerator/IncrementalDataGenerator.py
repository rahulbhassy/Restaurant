# generate_data.py
import random
from faker import Faker
from datetime import datetime
import os
from pyspark.sql import DataFrame
import argparse
import sys
import datetime
from Shared.pyspark_env import setVEnv, stop_spark
# Import all required classes from the previous code
from DataGenerator.DGFunctions import (
    GetData, DataGenerator, Combiner, TripGenerator, DataSaver
)

def main(runtype: str = 'prod'):
    print("Starting data generation process...")
    Faker.seed(42)
    fake = Faker()
      # For reproducible results

    # 1. Load existing data
    print("\n1. Loading existing data...")
    customer_getter = GetData(table="customerdetails",runtype=runtype)
    existing_customers = customer_getter.read_existing_data()

    driver_getter = GetData(table="driverdetails",runtype=runtype)
    existing_drivers = driver_getter.read_existing_data()

    vehicle_getter = GetData(table="vehicledetails",runtype=runtype)
    existing_vehicles = vehicle_getter.read_existing_data()

    # 2. Get max IDs for generation
    print("\n2. Calculating max IDs...")
    max_customer_id = customer_getter.get_max_ids(existing_customers) or 0
    max_driver_id = driver_getter.get_max_ids(existing_drivers) or 0
    print(f"Max Customer ID: {max_customer_id}, Max Driver ID: {max_driver_id}")

    # 3. Generate new entities
    print("\n3. Generating new entities...")
    generator = DataGenerator(fake, max_customer_id, max_driver_id)

    # Generate new customers and drivers first
    new_customers = generator.generate_new_customers(100)
    new_drivers = generator.generate_new_drivers(1)

    # Get all driver IDs (existing + new)
    new_driver_ids = [d['driver_id'] for d in new_drivers]

    # Generate vehicles with driver assignments
    new_vehicles = generator.generate_new_vehicles(
        driver_ids=new_driver_ids  # Pass driver IDs for assignment
    )

    new_data = {
        'customers': new_customers,
        'drivers': new_drivers,
        'vehicles': new_vehicles
    }

    # 4. Combine with existing data
    print("\n4. Combining with existing data...")
    combiner = Combiner(
        existing_customers_df=existing_customers,
        existing_drivers_df=existing_drivers,
        existing_vehicles_df=existing_vehicles
    ).create_combined_pools(
        new_customers=new_data['customers'],
        new_drivers=new_data['drivers'],
        new_vehicles=new_data['vehicles']
    )

    # 5. Generate trip details
    print("\n5. Generating trip details...")
    # Get existing trip IDs
    trip_getter = GetData(table="uberfares",runtype=runtype)
    new_trip_ids = trip_getter._get_trip_ids()

    # Generate new trip IDs (1000 trips)

    # Generate trip details
    trip_generator = TripGenerator(combiner, fake)
    trip_details = trip_generator.generate_trip_details(new_trip_ids)

    # 6. Save all data
    print("\n6. Saving data to CSV files...")
    data_saver = DataSaver(runtype=runtype)
    data_saver.save_all(
        new_customers=new_data['customers'],
        new_drivers=new_data['drivers'],
        new_vehicles=new_data['vehicles'],
        trip_details=trip_details
    )

    # 7. Generate summary report
    print("\n7. Generating summary report...")
    data_saver.generate_summary_report()

    print("\nData generation completed successfully!")
    stop_spark()


if __name__ == "__main__":
    sys.stdout.reconfigure(line_buffering=True)
    sys.stderr.reconfigure(line_buffering=True)

    parser = argparse.ArgumentParser()
    parser.add_argument("--runtype", required=True)

    args = parser.parse_args()
    exit_code = main(args.runtype)
    sys.exit(exit_code)