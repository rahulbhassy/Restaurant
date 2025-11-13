table_config = {

    "dim_customer": {
        "mandatory_cols": ["customer_id"],
        "cast_config": {},
        "allowed_values": {},
        "duplicate_keys": ["customer_id"],
        "anomaly_rules": {}
    },

    "dim_item": {
        "mandatory_cols": ["item_id"],
        "cast_config": {"unit_price": "double"},
        "allowed_values": {},
        "duplicate_keys": ["item_id"],
        "anomaly_rules": {"unit_price": 10000}
    },

    "dim_outlet": {
        "mandatory_cols": ["outlet_id"],
        "cast_config": {},
        "allowed_values": {},
        "duplicate_keys": ["outlet_id"],
        "anomaly_rules": {}
    },

    "dim_chef": {
        "mandatory_cols": ["chef_id"],
        "cast_config": {},
        "allowed_values": {},
        "duplicate_keys": ["chef_id"],
        "anomaly_rules": {}
    },

    "dim_stock_item": {
        "mandatory_cols": ["stock_id"],
        "cast_config": {},
        "allowed_values": {"unit_of_measure": ["kg", "g", "litre", "piece"]},
        "duplicate_keys": ["stock_id"],
        "anomaly_rules": {}
    },

    "fact_sales": {
        "mandatory_cols": ["order_id"],
        "cast_config": {
            "order_ts": "timestamp",
            "total_order_price": "double"
        },
        "allowed_values": {
            "payment_mode": ["Cash", "UPI", "Card", "Wallet"],
            "order_type": ["Dine-In", "Takeaway", "Delivery"]
        },
        "duplicate_keys": ["order_id"],
        "anomaly_rules": {"total_order_price": 5000}
    },

    "fact_kitchen": {
        "mandatory_cols": ["order_id"],
        "cast_config": {
            "cooking_start": "timestamp",
            "cooking_end": "timestamp"
        },
        "allowed_values": {},
        "duplicate_keys": ["order_id"],
        "anomaly_rules": {}
    },

    "fact_stock": {
        "mandatory_cols": ["outlet_id", "stock_item"],
        "cast_config": {
            "stock_date": "timestamp",
            "available_quantity": "double"
        },
        "allowed_values": {"unit_of_measure": ["kg", "g", "litre", "piece"]},
        "duplicate_keys": ["outlet_id", "stock_item", "stock_date"],
        "anomaly_rules": {"available_quantity": -1}  # negative qty flagged
    }
}
delta_column_dict = {
    # DIM tables (no timestamp columns)
    "dim_customer": None,
    "dim_item": None,
    "dim_outlet": None,
    "dim_chef": None,
    "dim_stock_item": None,

    # FACT tables
    "fact_sales": "order_ts",
    "fact_kitchen": "cooking_end",    # better for incremental
    "fact_stock": "stock_date"
}
