from faker import Faker
import pandas as pd
from sqlalchemy import create_engine
import random
from datetime import datetime

# Initialize Faker
fake = Faker()

# Database connection setup
DATABASE_URI = 'mysql+pymysql://root:12345@localhost/warehouse_db'
engine = create_engine(DATABASE_URI)

# 1. Generate synthetic data for dim_store
def generate_dim_store(num_records=10):
    stores = []
    for _ in range(num_records):
        stores.append({
            'store_id': fake.unique.random_int(min=1, max=1000),
            'store_name': fake.company(),
            'store_location': fake.city()
        })
    return pd.DataFrame(stores)

# Load dim_store data into the database
def load_store_data():
    df_stores = generate_dim_store()
    df_stores.to_sql('dim_store', con=engine, if_exists='append', index=False)
    print("dim_store loaded successfully.")

load_store_data()
# Generate synthetic data for fact_inventory
def generate_fact_inventory(num_records=100):
    # Fetch products and stores from the database
    dim_product = pd.read_sql('SELECT product_id FROM dim_product', con=engine)
    dim_store = pd.read_sql('SELECT store_id FROM dim_store', con=engine)

    inventory_data = []
    for _ in range(num_records):
        product = random.choice(dim_product['product_id'].values)
        store = random.choice(dim_store['store_id'].values)
        inventory_data.append({
            'inventory_id': fake.unique.random_int(min=1, max=10000),
            'product_id': product,
            'store_id': store,
            'stock_level': random.randint(1, 500),  # Random stock level between 1 and 500
            'last_updated': fake.date_this_year()  # Random date within the current year
        })
    return pd.DataFrame(inventory_data)

# Load fact_inventory data into the database
def load_inventory_data():
    df_inventory = generate_fact_inventory()
    df_inventory.to_sql('fact_inventory', con=engine, if_exists='append', index=False)
    print("fact_inventory loaded successfully.")

load_inventory_data()
