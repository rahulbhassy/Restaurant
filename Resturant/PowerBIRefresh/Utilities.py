

refreshtables = [
    "fact_sales_enriched",
    "fact_kitchen_enriched",
    "fact_stock_enriched",
    "chef_performance",
    "customer",
    "customer_preference",
    "outlet_performance"
]

schema = {
    "fact_sales_enriched" : "enrich",
    "fact_kitchen_enriched" : "enrich",
    "fact_stock_enriched" : "enrich",
    "chef_performance" : "people",
    "customer" : "people",
    "customer_preference" : "people",
    "outlet_performance" : "enrich"
}

layer = {
    "fact_sales_enriched" : "enrich",
    "fact_kitchen_enriched" : "enrich",
    "fact_stock_enriched" : "enrich",
    "chef_performance" : "enrich",
    "customer" : "enrich",
    "customer_preference" : "enrich",
    "outlet_performance" : "enrich"
}