

refreshtables = [
    "fares",
    "weatherimpact",
    "timeseries",
    "customerdetails",
    "driverdetails",
    "vehicledetails",
    "customerpreference",
    "customerprofile"
]

schema = {
    "fares" : "fares",
    "weatherimpact" : "fares",
    "timeseries" : "fares",
    "customerdetails" : "raw",
    "driverdetails" : "raw",
    "vehicledetails" : "raw",
    "customerpreference" : "people",
    "customerprofile" : "people"

}

layer = {
    "fares" : "enrich",
    "weatherimpact" : "enrich",
    "timeseries" : "enrich",
    "customerdetails" : "raw",
    "driverdetails" : "raw",
    "vehicledetails" : "raw",
    "customerpreference" : "enrich",
    "customerprofile" : "enrich"
}