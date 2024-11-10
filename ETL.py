
from faker import Faker
import pandas as pd
from sqlalchemy import create_engine
import random
from datetime import datetime, timedelta

# Initialize Faker
fake = Faker()

# Create connection to MySQL
DATABASE_URI = 'mysql+pymysql://root:12345@localhost/warehouse_db'
engine = create_engine(DATABASE_URI)

# Helper function to generate random dates within a range
def generate_dates(start_date, end_date, num_records):
    date_list = []
    current_date = start_date
    while current_date <= end_date:
        date_list.append(current_date)
        current_date += timedelta(days=1)
    return random.sample(date_list, num_records)
fake.unique.clear()
# 1. Generate synthetic data for dim_customer
def generate_dim_customer(num_records=100):
    customers = []
    for _ in range(num_records):
        customers.append({
            'customer_id': fake.unique.random_int(min=1, max=10000),
            'customer_name': fake.name(),
            'customer_address': fake.address(),
            'customer_email': fake.email(),
            'customer_phone': fake.phone_number()
        })
    return pd.DataFrame(customers)

fake.unique.clear()
# 2. Generate synthetic data for dim_product
def generate_dim_product(num_records=50):
    products = []
    for _ in range(num_records):
        products.append({
            'product_id': fake.unique.random_int(min=1, max=1000),
            'product_name': fake.word(),
            'product_category': fake.word(),
            'product_subcategory': fake.word(),
            'product_price': round(random.uniform(10, 500), 2),
            'product_description': fake.sentence()
        })
    return pd.DataFrame(products)

fake.unique.clear()
# 3. Generate synthetic data for dim_store
def generate_dim_store(num_records=10):
    stores = []
    for _ in range(num_records):
        stores.append({
            'store_id': fake.unique.random_int(min=1, max=100),
            'store_name': fake.company(),
            'store_location': fake.city()
        })
    return pd.DataFrame(stores)

fake.unique.clear()
# 4. Generate synthetic data for dim_time
def generate_dim_time(start_date='2020-01-01', end_date='2022-12-31'):
    dates = pd.date_range(start=start_date, end=end_date)
    time_dim = []
    for date in dates:
        time_dim.append({
            'time_id': int(date.strftime('%Y%m%d')),
            'Date': date.date(),
            'Year': date.year,
            'Quarter': (date.month - 1) // 3 + 1,
            'Month': date.month,
            'Day': date.day,
            'DayOfWeek': date.weekday() + 1,
            'DayName': date.strftime('%A'),
            'IsWeekend': date.weekday() >= 5
        })
    return pd.DataFrame(time_dim)

# 5. Generate synthetic data for fact_sales
def generate_fact_sales(num_records=500, customer_ids=None, product_ids=None, store_ids=None, time_ids=None):
    sales = []
    for _ in range(num_records):
        sales.append({
            'sale_id': fake.unique.random_int(min=1, max=10000),
            'product_id': random.choice(product_ids),
            'store_id': random.choice(store_ids),
            'customer_id': random.choice(customer_ids),
            'time_id': random.choice(time_ids),
            'quantity_sold': random.randint(1, 10),
            'total_sale_amount': round(random.uniform(10, 500), 2)
        })
    return pd.DataFrame(sales)

fake.unique.clear()
# 6. Generate synthetic data for fact_inventory
def generate_fact_inventory(num_records=100, product_ids=None, store_ids=None):
    inventory = []
    for _ in range(num_records):
        inventory.append({
            'inventory_id': fake.unique.random_int(min=1, max=10000),
            'product_id': random.choice(product_ids),
            'store_id': random.choice(store_ids),
            'stock_level': random.randint(0, 500),
            'last_updated': fake.date_this_year()
        })
    return pd.DataFrame(inventory)

fake.unique.clear()
# Load data into MySQL
def load_to_mysql(df, table_name):
    df.to_sql(table_name, con=engine, if_exists='append', index=False)
    print(f"{table_name} loaded successfully.")

# Generate and load data
def main():
    # Generate synthetic data
    df_customers = generate_dim_customer()
    df_products = generate_dim_product()
    df_stores = generate_dim_store()
    df_time = generate_dim_time()
    
    # Load dimensions into MySQL
    load_to_mysql(df_customers, 'dim_customer')
    load_to_mysql(df_products, 'dim_product')
    load_to_mysql(df_stores, 'dim_store')
    load_to_mysql(df_time, 'dim_time')
    
    # Get IDs for foreign key relationships
    customer_ids = df_customers['customer_id'].tolist()
    product_ids = df_products['product_id'].tolist()
    store_ids = df_stores['store_id'].tolist()
    time_ids = df_time['time_id'].tolist()
    
    # Generate and load fact tables
    df_sales = generate_fact_sales(customer_ids=customer_ids, product_ids=product_ids, store_ids=store_ids, time_ids=time_ids)
    df_inventory = generate_fact_inventory(product_ids=product_ids, store_ids=store_ids)
    
    load_to_mysql(df_sales, 'fact_sales')
    load_to_mysql(df_inventory, 'fact_inventory')

if __name__ == "__main__":
    main()
