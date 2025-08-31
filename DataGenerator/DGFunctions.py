import csv
import random
from faker import Faker
from datetime import datetime, timedelta , date
import os
from typing import List, Any
from Shared.pyspark_env import stop_spark
from pyspark.sql import DataFrame
from pyspark.sql.functions import max, col
from Shared.sparkconfig import create_spark_session
from Shared.FileIO import DataLakeIO
from Shared.DataLoader import DataLoader


_BASE_PATH = 'C:/Users/HP/uber_project/Data/DataSource'

'''    self.today_folder = datetime.now().strftime('%Y-%m-%d')
    self.output_path = os.path.join(self._BASE_PATH, self.today_folder)

    os.makedirs(self.output_path, exist_ok=True)
    print(f"Output folder created: {self.output_path}")'''

class GetData:

    _KEY_COLUMNS = {
        "driverdetails": "driver_id",
        "customerdetails": "customer_id",
        "vehicledetails": "vehicle_no",
    }
    def __init__(self,table,runtype: str = 'prod'):
        self.table = table
        self.spark = create_spark_session()
        self.runtype = runtype

    def _get_trip_ids(self):
        deltafileio = DataLakeIO(
            process='read',
            table=self.table,
            state='delta',
            layer='raw',
            loadtype='delta',
            runtype=self.runtype
        )
        deltapath = deltafileio.filepath()
        print(f"Input folder created for getting New Trip Ids: {deltapath}")
        dataloader = DataLoader(
            path=deltapath,
            filetype='parquet'
        )
        rows = dataloader.LoadData(self.spark).select("trip_id").collect()

        # Extract the value from each Row
        trip_ids: List[Any] = [row.trip_id for row in rows]
        print(f"Trip ids loaded with count : {len(trip_ids)}")
        return trip_ids

    def read_existing_data(self):
        currentio = DataLakeIO(
            process='read',
            table=self.table,
            state='current',
            layer='raw',
            loadtype='full',
            runtype= self.runtype
        )
        dataloader = DataLoader(
            path=currentio.filepath(),
            filetype='delta'
        )

        existingdata = dataloader.LoadData(self.spark)
        print(f"Loaded Existing Data for {self.table} with count: {existingdata.count()}")
        return existingdata

    def get_max_ids(self,df=DataFrame):
        numeric_max_df = df.agg(max(col(self._KEY_COLUMNS[self.table]).cast("long")).alias("max_id"))
        return numeric_max_df.collect()[0]["max_id"]




class DataGenerator:
    def __init__(self, fake, max_customer_id, max_driver_id):
        self.fake = fake
        self.max_customer_id = max_customer_id
        self.max_driver_id = max_driver_id

    def generate_new_customers(self, num_new_customers=50):
        new_customers = []
        customer_types = ['Regular', 'Premium', 'Corporate', 'Student']
        membership_statuses = ['Active', 'Active', 'Active', 'Active','Active', 'Active', 'Active', 'Inactive', 'Suspended']

        for i in range(num_new_customers):
            customer_id = self.max_customer_id + i + 1
            start_date = date(2009, 1, 1)
            end_date = date(2012, 12, 31)
            registration_date = self.fake.date_between(start_date=start_date, end_date=end_date)

            new_customers.append({
                'customer_id': customer_id,
                'customer_name': self.fake.name(),
                'phone_no': self.fake.numerify(text='+1##########'),
                'email': self.fake.email(),
                'address': self.fake.address().replace('\n', ', '),
                'date_of_birth': self.fake.date_of_birth(minimum_age=16, maximum_age=80),
                'registration_date': registration_date,
                'customer_type': random.choice(customer_types),
                'membership_status': random.choice(membership_statuses),
                'rating': round(random.uniform(3.0, 5.0), 1),
                'preferred_payment': random.choice(['Credit Card', 'Debit Card', 'Digital Wallet', 'Cash'])
            })

        print(
            f"Generated {len(new_customers)} new customers (IDs: {self.max_customer_id + 1} to {self.max_customer_id + num_new_customers})")
        return new_customers

    def generate_new_drivers(self, num_new_drivers=12):
        new_drivers = []
        experience_levels = ['Trainee', 'Junior', 'Senior', 'Expert', 'Master']
        driver_statuses = ['Active', 'Active', 'Active', 'Active', 'Active','Active', 'Active', 'Active', 'Active', 'Active', 'On Leave', 'Inactive']
        start_date = date(2009, 1, 1)
        end_date = date(2012, 12, 31)
        for i in range(num_new_drivers):
            driver_id = self.max_driver_id + i + 1
            hire_date = self.fake.date_between(start_date=start_date, end_date=end_date)

            new_drivers.append({
                'driver_id': driver_id,
                'driver_name': self.fake.name(),
                'license_no': self.fake.unique.bothify(text='??######').upper(),
                'phone_no': self.fake.numerify(text='+1##########'),
                'email': self.fake.email(),
                'address': self.fake.address().replace('\n', ', '),
                'date_of_birth': self.fake.date_of_birth(minimum_age=21, maximum_age=65),
                'hire_date': hire_date,
                'experience_level': random.choice(experience_levels),
                'rating': round(random.uniform(3.5, 5.0), 1),
                'status': random.choice(driver_statuses),
                'emergency_contact': self.fake.phone_number()
            })

        print(
            f"Generated {len(new_drivers)} new drivers (IDs: {self.max_driver_id + 1} to {self.max_driver_id + num_new_drivers})")
        return new_drivers

    def generate_new_vehicles(self,driver_ids):
        new_vehicles = []
        vehicle_models = [
            'Toyota Camry', 'Honda Civic', 'Ford F-150', 'Chevrolet Silverado',
            'Tesla Model 3', 'Nissan Altima', 'Hyundai Elantra', 'BMW 3 Series',
            'Audi A4', 'Mercedes-Benz C-Class', 'Volkswagen Jetta', 'Subaru Outback',
            'Mazda CX-5', 'Jeep Wrangler', 'GMC Sierra', 'Ram 1500',
            'Tesla Model S', 'Lexus ES', 'Acura TLX', 'Infiniti Q50',
            # New York taxi‐specific models
            'Toyota Prius', 'Nissan NV200', 'Ford Transit Connect', 'Chevrolet Impala',
            'Honda Accord', 'Toyota Sienna', 'Chrysler Pacifica', 'Ford Escape',
            'Kia Optima', 'Hyundai Sonata', 'Lincoln Town Car', 'Cadillac XTS',
            'Mercedes-Benz Sprinter'
        ]
        vehicle_types = ['Sedan', 'SUV', 'Hatchback', 'Pickup Truck', 'Electric', 'Luxury', 'Minivan', 'Van']
        fuel_types = ['Gasoline', 'Electric', 'Hybrid', 'Diesel']
        vehicle_statuses = ['Active','Active', 'Active', 'Active', 'Active', 'Active', 'Maintenance', 'Out of Service']
        # Ensure we have enough unique vehicle models
        start_date = date(2009, 1, 1)
        end_date = date(2015, 12, 31)
        start_m_date = date(2012,1,1)
        for driver_id in driver_ids:
            model = random.choice(vehicle_models)
            year = random.randint(2009, 2015)

            # Fuel logic
            if 'Tesla' in model:
                fuel_type = 'Electric'
            elif 'Prius' in model or random.random() < 0.2:
                fuel_type = 'Hybrid'
            else:
                # fallback to any fuel type
                fuel_type = random.choice(fuel_types)
            # Generate vehicle details
            new_vehicles.append({
                'vehicle_no': self.fake.unique.license_plate(),
                'driver_id': driver_id,
                'model': model,
                'year': year,
                'color': self.fake.color_name(),
                'vehicle_type': random.choice(vehicle_types),
                'fuel_type': fuel_type,
                'seating_capacity': random.choice([4, 5, 6, 7, 8]),
                'mileage': random.randint(15000, 200000),
                'insurance_expiry': self.fake.date_between(start_date=start_date, end_date=end_date),
                'last_maintenance': self.fake.date_between(start_date=start_m_date, end_date=end_date),
                'status': random.choice(vehicle_statuses),
                'registration_state': self.fake.state_abbr()
            })

        print(f"Generated {len(new_vehicles)} vehicles for {len(driver_ids)} drivers")
        return new_vehicles


class Combiner:
    def __init__(self, existing_customers_df, existing_drivers_df, existing_vehicles_df):
        """
        Initialize Combiner with existing data from Delta tables.

        :param existing_customers_df: Spark DataFrame of existing customers
        :param existing_drivers_df: Spark DataFrame of existing drivers
        :param existing_vehicles_df: Spark DataFrame of existing vehicles
        """
        self.existing_customers_df = existing_customers_df
        self.existing_drivers_df = existing_drivers_df
        self.existing_vehicles_df = existing_vehicles_df

        # Will be populated in create_combined_pools()
        self.all_customers = []
        self.all_drivers = []
        self.all_vehicles = []
        self.active_customers = []
        self.active_drivers = []
        self.active_vehicles = []

    def create_combined_pools(self, new_customers, new_drivers, new_vehicles):
        """Combine existing and new data for trip assignment"""
        # Convert existing customer data to dictionaries
        existing_customers = self.existing_customers_df.select(
            "customer_id", "customer_name", "membership_status"
        ).collect()

        # Combine existing + new customers
        self.all_customers = [
                                 {
                                     'customer_id': row.customer_id,
                                     'customer_name': row.customer_name,
                                     'membership_status': row.membership_status
                                 }
                                 for row in existing_customers
                             ] + [
                                 {
                                     'customer_id': c['customer_id'],
                                     'customer_name': c['customer_name'],
                                     'membership_status': c['membership_status']
                                 }
                                 for c in new_customers
                             ]

        # Convert existing driver data to dictionaries
        existing_drivers = self.existing_drivers_df.select(
            "driver_id", "driver_name", "status"
        ).collect()

        # Combine existing + new drivers
        self.all_drivers = [
                               {
                                   'driver_id': row.driver_id,
                                   'driver_name': row.driver_name,
                                   'status': row.status
                               }
                               for row in existing_drivers
                           ] + [
                               {
                                   'driver_id': d['driver_id'],
                                   'driver_name': d['driver_name'],
                                   'status': d['status']
                               }
                               for d in new_drivers
                           ]

        # Convert existing vehicle data to dictionaries
        existing_vehicles = self.existing_vehicles_df.select(
            "vehicle_no", "status"
        ).collect()

        # Combine existing + new vehicles
        self.all_vehicles = [
                                {
                                    'vehicle_no': row.vehicle_no,
                                    'status': row.status
                                }
                                for row in existing_vehicles
                            ] + [
                                {
                                    'vehicle_no': v['vehicle_no'],
                                    'status': v['status']
                                }
                                for v in new_vehicles
                            ]

        # Filter active entities
        self.active_customers = [c for c in self.all_customers if
                                 c['membership_status'] == 'Active'] or self.all_customers
        self.active_drivers = [d for d in self.all_drivers if d['status'] == 'Active'] or self.all_drivers
        self.active_vehicles = [v for v in self.all_vehicles if v['status'] == 'Active'] or self.all_vehicles

        print(
            f"Combined pools - Customers: {len(self.all_customers)}, Drivers: {len(self.all_drivers)}, Vehicles: {len(self.all_vehicles)}")
        print(
            f"Active pools - Customers: {len(self.active_customers)}, Drivers: {len(self.active_drivers)}, Vehicles: {len(self.active_vehicles)}")

        return self


import random

import random


class TripGenerator:
    def __init__(self, combiner, fake):
        """
        Initialize TripGenerator with combined entity pools and Faker instance.

        :param combiner: Combiner instance with active entity pools
        :param fake: Faker instance for generating realistic data
        """
        self.combiner = combiner
        self.fake = fake
        self.customer_usage = {}
        self.driver_usage = {}
        self.vehicle_usage = {}

        # Define location weights (major cities get higher weights)
        self.location_weights = {
            # Tier 1: NYC Boroughs (Highest weight)
            "Manhattan": 35, "Brooklyn": 35, "Queens": 30, "Bronx": 30, "Staten Island": 25,
            # Tier 2: Major Northeast Cities
            "Boston": 20, "Philadelphia": 20, "Pittsburgh": 18, "Toronto": 20, "Ottawa": 18,
            "Montreal": 18, "Providence": 18, "Newark": 18, "Jersey City": 18, "Bridgeport": 15,
            "New Haven": 15, "Hartford": 15, "Stamford": 15,
            # Tier 3: Other Notable Cities
            "Cambridge": 12, "Lowell": 10, "Brockton": 10, "Mississauga": 12, "Brampton": 12,
            "Hamilton": 12, "London": 10, "Markham": 12, "Vaughan": 12, "Kingston": 10,
            "Windsor": 10, "Paterson": 12, "Elizabeth": 12, "Edison Township": 10, "Clifton": 10,
            "Allentown": 10, "Erie": 8, "Scranton": 8, "Lancaster": 8, "York": 8,
            "Warwick": 10, "Cranston": 10, "Waterbury": 8, "Norwalk": 10, "Danbury": 8,
            "Worcester": 10, "Springfield": 8, "New Bedford": 8, "Lynn": 8, "Quincy": 8,
            "Fall River": 8, "Portland": 8, "Burlington": 8, "Manchester": 8,
            # Tier 4: Rural/Other areas (Default weight=1)
        }

        # Define trip intentions with realistic weights
        self.trip_intentions = [
            "Commute",  # 25%
            "Business",  # 15%
            "Leisure",  # 12%
            "Errand",  # 10%
            "Airport Transfer",  # 8%
            "Social/Visiting",  # 7%
            "Dining",  # 5%
            "Shopping",  # 5%
            "Medical/Healthcare",  # 4%
            "School/Education",  # 3%
            "Event/Entertainment",  # 3%
            "Tourism/Sightseeing",  # 2%
            "Hotel Transfer",  # 1%
            "Ride Pooling/Shared",  # 1%
            "Religious",  # 0.5%
            "Exercise/Outdoor",  # 0.5%
            "Delivery",  # 0.5%
            "Relocation/Move",  # 0.5%
            "Other"  # 1%
        ]
        self.intention_weights = [
            250,  # Commute
            150,  # Business
            120,  # Leisure
            100,  # Errand
            80,  # Airport Transfer
            70,  # Social/Visiting
            50,  # Dining
            50,  # Shopping
            40,  # Medical/Healthcare
            30,  # School/Education
            30,  # Event/Entertainment
            20,  # Tourism/Sightseeing
            10,  # Hotel Transfer
            10,  # Ride Pooling/Shared
            5,  # Religious
            5,  # Exercise/Outdoor
            5,  # Delivery
            5,  # Relocation/Move
            10  # Other
        ]

    def generate_trip_details(self, trip_ids):
        """Generate trip details using combined customer/driver/vehicle pools"""
        trip_details = []
        trip_statuses = ['Completed', 'Completed', 'Completed', 'Completed', 'Completed',
                         'Completed', 'Completed', 'Completed', 'Cancelled', 'No Show']
        payment_methods = ['Credit Card', 'Debit Card', 'Cash', 'Digital Wallet', 'Corporate Account']

        # Full list of possible locations
        locations = [
            # NYC Boroughs
            "Staten Island", "Queens", "Brooklyn", "Manhattan", "Bronx",

            # Surrounding NY areas
            "Nassau County (Long Island outside NYC)", "Suffolk County (Eastern Long Island)",
            "Westchester County (outside NYC)", "Rockland County (outside NYC)",
            "Orange County (Hudson Valley)", "Putnam County (Hudson Valley)",
            "Dutchess County (Hudson Valley)", "Ulster County (Catskills region)",
            "Sullivan County (Catskills)", "Delaware County (rural Catskills)",
            "Greene County (Catskills)", "Columbia County (rural)",
            "Rensselaer County (outside Albany)", "Washington County (rural northeast)",
            "Essex County (Adirondacks)", "Franklin County (Adirondacks)",
            "Clinton County (north border)", "St. Lawrence County (rural north)",
            "Jefferson County (Thousand Islands)", "Lewis County (rural)",
            "Hamilton County (Adirondacks)", "Fulton County (rural)",
            "Montgomery County (rural)", "Herkimer County (rural)",
            "Oneida County (outside Utica)", "Sussex County (rural northwest)",

            # New Jersey
            "Warren County (Delaware Water Gap area)", "Hunterdon County (rural townships)",
            "Salem County (rural south)", "Cumberland County (rural areas)",
            "Cape May County (rural areas outside resorts)", "Ocean County (Pine Barrens)",
            "Burlington County (rural townships)", "Atlantic County (rural inland)",
            "Morris County (rural areas)",

            # Pennsylvania
            "Lackawanna County (rural areas)", "Berks County (outside Reading)",
            "Chester County (rural townships)", "Westmoreland County (outside Pittsburgh metro)",
            "Bradford County (northern rural)", "Somerset County (rural mountains)",
            "Tioga County (forest areas)", "Potter County (rural)",
            "Wayne County (rural northeast)", "Cameron County (sparsely populated)",

            # New England
            "Essex County (rural northeast)", "Orleans County (rural north)",
            "Caledonia County (rural)", "Lamoille County (rural)",
            "Washington County (rural areas)", "Orange County (rural)",
            "Windsor County (rural)", "Windham County (rural south)",
            "Bennington County (rural southwest)", "Rutland County (rural areas)",
            "Berkshire County (western rural)", "Franklin County (rural northwest)",
            "Hampshire County (rural areas)", "Hampden County (rural areas)",
            "Worcester County (rural areas)", "Barnstable County (Cape Cod rural)",
            "Dukes County (Martha's Vineyard)", "Nantucket County",
            "Essex County (rural areas)", "Plymouth County (rural areas)",
            "Litchfield County (rural northwest)", "Windham County (rural northeast)",
            "Tolland County (rural areas)", "New London County (rural areas)",
            "Middlesex County (rural townships)", "Fairfield County (rural areas outside cities)",
            "Hartford County (rural areas)", "New Haven County (rural areas)",
            "Eastern CT rural areas", "Northwestern CT hills",

            # Canada
            "Muskoka District (rural)", "Haliburton County (rural)",
            "Renfrew County (rural)", "Lanark County (rural)",
            "Hastings County (rural)", "Lennox and Addington (rural)",
            "Prince Edward County (rural)", "Northumberland County (rural)",
            "Peterborough County (rural)", "Kawartha Lakes (rural)",
            "Ville-Marie", "Le Plateau-Mont-Royal", "Outremont",
            "Côte-des-Neiges–Notre-Dame-de-Grâce", "Verdun", "Rosemont–La Petite-Patrie",
            "La Cité-Limoilou", "Sainte-Foy–Sillery–Cap-Rouge", "Charlesbourg",
            "Beauport", "Abitibi-Témiscamingue (rural)", "Bas-Saint-Laurent (rural)",
            "Centre-du-Québec (rural)", "Chaudière-Appalaches (rural)", "Côte-Nord (rural)",
            "Estrie (rural)", "Gaspésie–Îles-de-la-Madeleine (rural)", "Lanaudière (rural)",
            "Laurentides (rural)", "Mauricie (rural)",

            # Major Cities (already in weights dictionary)
            "Newark", "Jersey City", "Paterson", "Elizabeth", "Edison Township",
            "Woodbridge Township", "Lakewood Township", "Toms River Township",
            "Hamilton Township", "Clifton", "Philadelphia", "Pittsburgh", "Allentown",
            "Erie", "Reading Borough", "Scranton", "Bethlehem", "Lancaster", "Harrisburg",
            "York", "Toronto", "Ottawa", "Mississauga", "Brampton", "Hamilton", "London",
            "Markham", "Vaughan", "Kingston", "Windsor", "Providence", "Warwick", "Cranston",
            "Pawtucket", "East Providence", "Woonsocket", "Newport", "Central Falls",
            "Cumberland", "Coventry", "Bridgeport", "New Haven", "Hartford", "Stamford",
            "Waterbury", "Norwalk", "Danbury", "New Britain", "West Haven", "Greenwich",
            "Washington County (rural south)", "Kent County (rural areas)",
            "Bristol County (rural areas)", "Newport County (rural areas)",
            "Providence County (rural areas)", "Block Island", "Exeter (rural town)",
            "Hopkinton (rural)", "Richmond (rural)", "Foster (rural)", "Boston", "Worcester",
            "Springfield", "Cambridge", "Lowell", "Brockton", "New Bedford", "Lynn", "Quincy",
            "Fall River", "Portland", "Burlington", "Manchester"
        ]

        # Create weight list matching locations
        weight_list = [self.location_weights.get(loc, 1) for loc in locations]

        for trip_id in trip_ids:
            # Select random active entities
            customer = random.choice(self.combiner.active_customers)
            driver = random.choice(self.combiner.active_drivers)
            vehicle = random.choice(self.combiner.active_vehicles)

            # Update usage statistics
            self.customer_usage[customer['customer_id']] = self.customer_usage.get(customer['customer_id'], 0) + 1
            self.driver_usage[driver['driver_id']] = self.driver_usage.get(driver['driver_id'], 0) + 1
            self.vehicle_usage[vehicle['vehicle_no']] = self.vehicle_usage.get(vehicle['vehicle_no'], 0) + 1

            # Generate realistic trip data
            distance = round(random.uniform(0.5, 50.0), 2)
            base_fare = round(distance * random.uniform(1.5, 3.0), 2)
            tip_amount = round(base_fare * random.uniform(0, 0.25), 2)

            # Select locations with weighted probability
            pickup = random.choices(locations, weights=weight_list, k=1)[0]
            dropoff = random.choices(locations, weights=weight_list, k=1)[0]

            # Ensure pickup and dropoff aren't identical
            while pickup == dropoff:
                dropoff = random.choices(locations, weights=weight_list, k=1)[0]

            # Select trip intention with weighted probability
            intention = random.choices(
                self.trip_intentions,
                weights=self.intention_weights,
                k=1
            )[0]

            # Adjust intention probabilities based on locations
            if "Airport" in pickup or "Airport" in dropoff:
                # Increase probability for airport transfers
                intention = random.choices(
                    ["Airport Transfer", intention],
                    weights=[70, 30],
                    k=1
                )[0]
            elif "Hotel" in pickup or "Hotel" in dropoff:
                # Increase probability for hotel transfers
                intention = random.choices(
                    ["Hotel Transfer", intention],
                    weights=[60, 40],
                    k=1
                )[0]
            elif "Tourism" in pickup or "Tourism" in dropoff or "Sightseeing" in pickup or "Sightseeing" in dropoff:
                # Increase probability for tourism
                intention = random.choices(
                    ["Tourism/Sightseeing", intention],
                    weights=[65, 35],
                    k=1
                )[0]
            elif "Medical" in pickup or "Medical" in dropoff or "Healthcare" in pickup or "Healthcare" in dropoff:
                # Increase probability for medical trips
                intention = random.choices(
                    ["Medical/Healthcare", intention],
                    weights=[75, 25],
                    k=1
                )[0]
            elif "School" in pickup or "School" in dropoff or "University" in pickup or "University" in dropoff:
                # Increase probability for school trips
                intention = random.choices(
                    ["School/Education", intention],
                    weights=[80, 20],
                    k=1
                )[0]

            trip_details.append({
                'trip_id': trip_id,
                'driver_id': driver['driver_id'],
                'customer_id': customer['customer_id'],
                'pickup_location': pickup,
                'dropoff_location': dropoff,
                'tip_amount': tip_amount,
                'payment_method': random.choice(payment_methods),
                'trip_status': random.choice(trip_statuses),
                'customer_rating': random.choice([None,2.5,2,1.5,1, 3,3,3.5,4, 4.5,4.5,4.5,4.5,4.5,4.5,4.5,4.5, 4, 5, 5, 5, 5,5 ,5 ,5 ,5, 5, 5]),
                'driver_rating': random.choice([None,2.5,2,1.5,1, 3,3,3.5,4, 4.5,4.5,4.5,4.5,4.5,4.5,4.5,4.5, 4, 5, 5, 5, 5,5 ,5 ,5 ,5, 5, 5]),
                'trip_intention': intention
            })

        print(f"Generated {len(trip_details)} trip records")
        print(f"Unique customers used: {len(self.customer_usage)}")
        print(f"Unique drivers used: {len(self.driver_usage)}")
        print(f"Unique vehicles used: {len(self.vehicle_usage)}")

        return trip_details


import os
import csv
from datetime import datetime
from tabulate import tabulate

class DataSaver:
    # Base path for all generated data
    _BASE_PATH = 'C:/Users/HP/uber_project/Data/DataSource'

    # Fixed filenames for daily data
    FILENAMES = {
        'customers': 'customerdetails.csv',
        'drivers': 'driverdetails.csv',
        'vehicles': 'vehicledetails.csv',
        'trips': 'tripdetails.csv'
    }

    def __init__(self,runtype: str = 'prod'):
        """
        Initialize DataSaver with dated output directory and fixed filenames
        """
        self.runtype = runtype
        self.date_str = datetime.now().strftime("%Y-%m-%d")
        if self.runtype == 'dev':
            self.output_path = os.path.join(self._BASE_PATH,'Sandbox', self.date_str)
        else:
            self.output_path = os.path.join(self._BASE_PATH, self.date_str)
        self.report_data = []
        self.runtype = runtype

        os.makedirs(self.output_path, exist_ok=True)
        print(f"Data will be saved to: {self.output_path}")

    def write_to_csv(self, filename, data, fieldnames):
        """Write data to CSV file with error handling"""
        try:
            full_path = os.path.join(self.output_path, filename)
            with open(full_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(data)

            self.report_data.append({
                'file': filename,
                'records': len(data),
                'status': 'Success',
                'message': f"Created with {len(data)} records"
            })
            return True
        except Exception as e:
            self.report_data.append({
                'file': filename,
                'records': 0,
                'status': 'Failed',
                'message': str(e)
            })
            return False

    def save_all(self,
                 new_vehicles=None,
                 new_customers=None,
                 new_drivers=None,
                 trip_details=None):
        """
        Save vehicles (mandatory) and optionally customers, drivers, trips.
        Returns True if all attempted writes succeed.
        """
        results = []

        # field mappings
        field_mappings = {
            'customers': [
                'customer_id', 'customer_name', 'phone_no', 'email', 'address',
                'date_of_birth', 'registration_date', 'customer_type',
                'membership_status', 'rating', 'total_trips', 'preferred_payment'
            ],
            'drivers': [
                'driver_id', 'driver_name', 'license_no', 'phone_no', 'email',
                'address', 'date_of_birth', 'hire_date', 'experience_level',
                'rating', 'status', 'emergency_contact'
            ],
            'vehicles': [
                'vehicle_no', 'driver_id', 'model', 'year', 'color', 'vehicle_type',
                'fuel_type', 'seating_capacity', 'mileage', 'insurance_expiry',
                'last_maintenance', 'status', 'registration_state'
            ],
            'trips': [
                'trip_id', 'driver_id', 'customer_id', 'pickup_location',
                'dropoff_location', 'tip_amount','payment_method', 'trip_status',
                'customer_rating','driver_rating','trip_intention'
            ]
        }

        if new_vehicles:
            results.append(
                self.write_to_csv(
                    self.FILENAMES['vehicles'],
                    new_vehicles,
                    field_mappings['vehicles']
                )
            )

        # optional writes
        if new_customers:
            results.append(
                self.write_to_csv(
                    self.FILENAMES['customers'],
                    new_customers,
                    field_mappings['customers']
                )
            )

        if new_drivers:
            results.append(
                self.write_to_csv(
                    self.FILENAMES['drivers'],
                    new_drivers,
                    field_mappings['drivers']
                )
            )

        if trip_details:
            results.append(
                self.write_to_csv(
                    self.FILENAMES['trips'],
                    trip_details,
                    field_mappings['trips']
                )
            )

        return all(results)

    def generate_summary_report(self):
        """Generate and display a summary report of all saved data"""
        if not self.report_data:
            print("No data generated to report")
            return ""

        headers = ["File", "Records", "Status", "Details"]

        # totals
        total_records = sum(item['records'] for item in self.report_data)
        self.report_data.append({
            'file': 'TOTAL',
            'records': total_records,
            'status': '',
            'message': f"{len(self.report_data) - 1} files generated"
        })

        table_data = [
            [item['file'], item['records'], item['status'], item['message']]
            for item in self.report_data
        ]
        report = tabulate(table_data,
                          headers=headers,
                          tablefmt="fancy_grid",
                          numalign="right",
                          stralign="left")

        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        header = f"DATA GENERATION REPORT ({timestamp})"
        separator = "=" * len(header)
        full_report = f"\n{separator}\n{header}\n{separator}\n\n{report}\n"

        print(full_report)
        report_filename = os.path.join(self.output_path, "generation_report.txt")
        with open(report_filename, 'w',encoding='utf-8') as f:
            f.write(full_report)

        print(f"Report saved to: {report_filename}")
        return full_report
