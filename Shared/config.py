RAWTABLES = [
    "uberfares",
    "tripdetails",
    "driverdetails",
    "customerdetails",
    "vehicledetails",
]

SYSTEMTABLES = ["balancingresults"]

ENRICHTABLES = [
    "fares",
    "timeseries",
    "customerprofile",
    "driverprofile",
    "customerpreference",
    "driverpreference",
    "driverperformance",
    "weatherimpact",
    "uberfares"
]
SPATIALTABLES = {
    "uber"
}
SPATIALLAYER = {
    "uber": "enrich"
}
RAWLAYER = {
    "uberfares" : "raw",
    "tripdetails" : "raw",
    "driverdetails" : "raw",
    "customerdetails" : "raw",
    "vehicledetails" : "raw",

}

SYSTEMLAYER = {
    "balancingresults": "system",
    "uberfares": "enrichweather"
}

ENRICHLAYER = {
    "uberfares" : "enrich",
    "fares": "enrich",
    "timeseries": "enrich",
    "customerprofile": "enrich",
    "driverprofile": "enrich",
    "customerpreference": "enrich",
    "driverpreference": "enrich",
    "driverperformance": "enrich",
    "weatherimpact": "enrich"
}
