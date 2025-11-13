
config = {
    "fact_sales_enriched": [
        "fact_sales",
        "fact_sales_items",
        "dim_customer",
        "dim_outlet",
        "fact_kitchen"  # optional
    ],

    "fact_kitchen_enriched": [
        "fact_kitchen",
        "dim_chef",
        "dim_outlet"
    ],

    "fact_stock_enriched": [
        "fact_stock",
        "dim_stock_item",
        "dim_outlet"
    ]
}
layer = {
    "fact_sales_enriched": "enrich",
    "fact_kitchen_enriched": "enrich",
    "fact_stock_enriched": "enrich",
    "fact_sales": "raw",
    "fact_sales_items": "raw",
    "dim_customer": "raw",
    "dim_outlet": "raw",
    "fact_kitchen": "raw",
    "dim_chef": "raw",
    "fact_stock": "raw",
    "dim_stock_item": "raw"
}