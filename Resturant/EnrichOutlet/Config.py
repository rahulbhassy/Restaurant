config = {
    "outlet_performance":[
        "fact_sales",
        "dim_outlet",
        "fact_kitchen"
    ]
}
layer = {
    "fact_sales": "raw",
    "fact_sales_items": "raw",
    "fact_kitchen": "raw",
    "fact_stock": "raw",
    "dim_customer": "raw",
    "dim_outlet": "raw",
    "dim_item": "raw",
    "dim_chef": "raw",
    "dim_stock_item": "raw",
    "outlet_performance": "enrich"
}