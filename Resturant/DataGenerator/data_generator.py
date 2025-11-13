import random
import json
import datetime
import uuid
import os

# ===============================
# USER PARAMETERS
# ===============================
OUTPUT_DIR = "kfc_complete_data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

start_date_str = "2025-11-01"
num_days = 3
orders_per_outlet_per_day = 300
outlets = [
    {"outlet_id": "OUT001", "outlet_name": "KFC Koramangala"},
    {"outlet_id": "OUT002", "outlet_name": "KFC Indiranagar"},
    {"outlet_id": "OUT003", "outlet_name": "KFC Whitefield"},
    {"outlet_id": "OUT004", "outlet_name": "KFC MG Road"},
    {"outlet_id": "OUT005", "outlet_name": "KFC Electronic City"}
]

# ===============================
# BASE DIMENSIONS
# ===============================
ITEMS = [
    {"item_id": "ITEM001", "item_name": "Zinger Burger", "unit_price": 180},
    {"item_id": "ITEM002", "item_name": "Hot & Crispy Chicken", "unit_price": 220},
    {"item_id": "ITEM003", "item_name": "Chicken Popcorn", "unit_price": 150},
    {"item_id": "ITEM004", "item_name": "Chicken Wings", "unit_price": 200},
    {"item_id": "ITEM005", "item_name": "Smoky Grilled Chicken", "unit_price": 250},
    {"item_id": "ITEM006", "item_name": "Rice Bowlz", "unit_price": 180},
    {"item_id": "ITEM007", "item_name": "French Fries", "unit_price": 120},
    {"item_id": "ITEM008", "item_name": "Pepsi", "unit_price": 60},
    {"item_id": "ITEM009", "item_name": "Krushers", "unit_price": 120},
    {"item_id": "ITEM010", "item_name": "Veg Zinger Burger", "unit_price": 150}
]

TOPPINGS = ["Extra Cheese", "Spicy Sauce", "Mayonnaise", "No Onion", "Double Patty"]

STOCK_ITEMS = [
    {"stock_id": "ST001", "stock_item": "Chicken Breast", "unit_of_measure": "kg"},
    {"stock_id": "ST002", "stock_item": "Burger Buns", "unit_of_measure": "pcs"},
    {"stock_id": "ST003", "stock_item": "Cooking Oil", "unit_of_measure": "liters"},
    {"stock_id": "ST004", "stock_item": "Cheese Slices", "unit_of_measure": "pcs"},
    {"stock_id": "ST005", "stock_item": "Cola Bottles", "unit_of_measure": "pcs"},
    {"stock_id": "ST006", "stock_item": "Potato Fries", "unit_of_measure": "kg"},
    {"stock_id": "ST007", "stock_item": "Spices Mix", "unit_of_measure": "g"},
    {"stock_id": "ST008", "stock_item": "Lettuce", "unit_of_measure": "kg"},
    {"stock_id": "ST009", "stock_item": "Packaging Boxes", "unit_of_measure": "pcs"}
]

# Generate customers
def generate_customers(n=800):
    customers = []
    for i in range(n):
        mobile = f"+91{random.randint(7000000000, 9999999999)}"
        customers.append({
            "customer_id": f"CUST{i+1:04d}",
            "customer_mobile": mobile
        })
    return customers

# Generate chefs
def generate_chefs(outlets):
    chefs = []
    for o in outlets:
        num_chefs = random.randint(3, 6)
        for i in range(1, num_chefs + 1):
            chefs.append({
                "chef_id": f"CHEF_{o['outlet_id']}_{i}",
                "chef_name": f"Chef_{i}_{o['outlet_name'].split()[-1]}",
                "outlet_id": o["outlet_id"]
            })
    return chefs

dim_customer = generate_customers()
dim_item = ITEMS
dim_outlet = outlets
dim_stock_item = STOCK_ITEMS
dim_chef = generate_chefs(outlets)

# ===============================
# FACT TABLE GENERATION
# ===============================
start_date = datetime.datetime.strptime(start_date_str, "%Y-%m-%d")

fact_sales = []
fact_kitchen = []
fact_stock = []

for outlet in outlets:
    outlet_chefs = [c for c in dim_chef if c["outlet_id"] == outlet["outlet_id"]]

    # Generate stock details
    for s in STOCK_ITEMS:
        fact_stock.append({
            "outlet_id": outlet["outlet_id"],
            "stock_item": s["stock_item"],
            "available_quantity": round(random.uniform(20, 200), 2),
            "unit_of_measure": s["unit_of_measure"],
            "stock_date": start_date.strftime("%Y-%m-%d")
        })

    # Generate orders for each day
    for day_offset in range(num_days):
        current_date = start_date + datetime.timedelta(days=day_offset)
        for _ in range(orders_per_outlet_per_day):
            order_id = str(uuid.uuid4())
            customer = random.choice(dim_customer)
            items = random.sample(dim_item, random.randint(1, 5))
            chef = random.choice(outlet_chefs)
            order_details = []
            total_price = 0

            for it in items:
                qty = random.randint(1, 3)
                toppings = random.sample(TOPPINGS, random.randint(0, 2))
                line_total = qty * it["unit_price"]
                total_price += line_total
                order_details.append({
                    "item_id": it["item_id"],
                    "item_name": it["item_name"],
                    "quantity": qty,
                    "toppings": toppings,
                    "unit_price": it["unit_price"],
                    "line_total": line_total
                })

            # Random times
            rand_min = random.randint(0, 1439)
            order_ts = current_date + datetime.timedelta(minutes=rand_min)
            cooking_start = order_ts + datetime.timedelta(minutes=random.randint(1, 5))
            cooking_end = cooking_start + datetime.timedelta(minutes=random.randint(5, 15))

            # ========== FACT SALES ==========
            fact_sales.append({
                "order_id": order_id,
                "outlet_id": outlet["outlet_id"],
                "customer_id": customer["customer_id"],
                "order_ts": order_ts.isoformat(),
                "order_menu_details": order_details,
                "total_order_price": total_price,
                "payment_mode": random.choice(["UPI", "Card", "Cash", "Wallet"]),
                "order_type": random.choice(["Dine-In", "Takeaway", "Delivery"]),
                "high_value_flag": total_price > 1000
            })

            # ========== FACT KITCHEN ==========
            fact_kitchen.append({
                "order_id": order_id,
                "outlet_id": outlet["outlet_id"],
                "chef_id": chef["chef_id"],
                "cooking_start": cooking_start.isoformat(),
                "cooking_end": cooking_end.isoformat()
            })

# ===============================
# SAVE OUTPUTS
# ===============================
def save_json(name, data):
    with open(f"{OUTPUT_DIR}/{name}.json", "w") as f:
        json.dump(data, f, indent=2)

save_json("dim_customer", dim_customer)
save_json("dim_item", dim_item)
save_json("dim_outlet", dim_outlet)
save_json("dim_chef", dim_chef)
save_json("dim_stock_item", dim_stock_item)
save_json("fact_sales", fact_sales)
save_json("fact_kitchen", fact_kitchen)
save_json("fact_stock", fact_stock)

print("✅ KFC data generation complete!")
print(f"📅 Date range: {start_date_str} → {(start_date + datetime.timedelta(days=num_days-1)).strftime('%Y-%m-%d')}")
print(f"📦 Output files: {OUTPUT_DIR}/")
print(f"💰 fact_sales={len(fact_sales)}, 👨‍🍳 fact_kitchen={len(fact_kitchen)}, 📦 fact_stock={len(fact_stock)}")
