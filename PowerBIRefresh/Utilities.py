

refreshtables = [
    "fares",
    "weatherimpact",
    "timeseries",
    "customerdetails",
    "driverdetails",
    "vehicledetails",
    "customerpreference",
    "customerprofile",
    "driverprofile",
    "driverpreference",
    "driversalary"
]

schema = {
    "fares" : "fares",
    "weatherimpact" : "fares",
    "timeseries" : "fares",
    "customerdetails" : "raw",
    "driverdetails" : "raw",
    "vehicledetails" : "raw",
    "customerpreference" : "people",
    "customerprofile" : "people",
    "driverpreference": "people",
    "driverprofile": "people",
    "driversalary" : "people"
}

layer = {
    "fares" : "enrich",
    "weatherimpact" : "enrich",
    "timeseries" : "enrich",
    "customerdetails" : "raw",
    "driverdetails" : "raw",
    "vehicledetails" : "raw",
    "customerpreference" : "enrich",
    "customerprofile" : "enrich",
    "driverpreference": "enrich",
    "driverprofile": "enrich",
    "driversalary": "enrich"
}