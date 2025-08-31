config = {
    "customerprofile" : ["customerdetails","fares","tripdetails"],
    "customerpreference" : ["customerprofile","fares","tripdetails","uberfares"],
    "driverprofile" : ["driverdetails","fares","tripdetails","vehicledetails"],
    "driverpreference" : ["driverprofile","fares","tripdetails","uberfares"],
    "driversalary" : ["driverdetails","fares","tripdetails"]
}
layer = {
    "customerdetails" : "raw",
    "driverdetails" : "raw",
    "fares" : "enrich",
    "tripdetails" : "raw",
    "customerprofile" : "enrich",
    "driverprofile" : "enrich",
    "customerpreference" : "enrich",
    "driverpreference" : "enrich",
    "uberfares" : "raw",
    "vehicledetails": "raw",
    "driversalary" : "enrich"
}

keys = {
    "customerprofile" : ["customer_id"],
    "driverprofile" : ["driver_id"],
    "customerpreference" : ["customer_id"],
    "driverpreference" : ["driver_id"],
    "driversalary" : ["salary_key"]
}

updateitems = {
    "customerprofile" : [
    "total_fareamount",
    "total_tip_amount",
    "avg_tip_pct",
    "total_distance_km",
    "total_trip_duration_min",
    "total_trip_count",
    "avg_customer_rating",
    "Cancelled",
    "Completed",
    "No_Show",
    "cnt_cash_payment",
    "cnt_corporate_account_payment",
    "cnt_credit_card_payment",
    "cnt_debit_card_payment",
    "cnt_digital_wallet_payment",
    "total_cash_amount",
    "total_corporate_account_amount",
    "total_credit_card_amount",
    "total_debit_card_amount",
    "total_digital_wallet_amount",
    "age"
    ]

}