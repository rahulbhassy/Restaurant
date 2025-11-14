
config = {

    "customer_preference": [
        "fact_sales",
        "fact_sales_items",
        "dim_customer",
        "dim_item",
        "dim_outlet"
    ],

    "customer": [
        "fact_sales",
        "dim_customer",
        "fact_sales_items"
    ],

    "chef_performance": [
        "fact_kitchen",
        "dim_chef",
        "dim_outlet",
        "fact_sales"     
    ]
}
layer = {

    "fact_sales_enriched": "enrich",
    "fact_kitchen_enriched": "enrich",
    "fact_stock_enriched": "enrich",

    "customer_preference": "enrich",
    "customer": "enrich",
    "chef_performance": "enrich",


    "fact_sales": "raw",
    "fact_sales_items": "raw",
    "fact_kitchen": "raw",
    "fact_stock": "raw",

    "dim_customer": "raw",
    "dim_outlet": "raw",
    "dim_item": "raw",
    "dim_chef": "raw",
    "dim_stock_item": "raw"
}
