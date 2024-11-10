import os
import pandas as pd
import random
from faker import Faker
from sqlalchemy import create_engine
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SuperstoreETL:
    def __init__(self, db_uri: str):
        self.fake = Faker()
        self.engine = create_engine(db_uri, pool_size=10, max_overflow=20)

    def extract_csvs(self, directory_path: str) -> pd.DataFrame:
        """Combine all CSV files in the specified directory into a single DataFrame."""
        all_files = [os.path.join(directory_path, f) for f in os.listdir(directory_path) if f.endswith('.csv')]
        df_list = [pd.read_csv(file) for file in all_files]
        combined_df = pd.concat(df_list, ignore_index=True)
        return combined_df

    def prepare_dim_customer(self, df: pd.DataFrame) -> pd.DataFrame:
        """Extract and prepare the customer dimension."""
        customer_df = df[['Customer ID', 'Customer Name']].drop_duplicates()
        customer_df['customer_address'] = [self.fake.address() for _ in range(len(customer_df))]
        customer_df['customer_email'] = [self.fake.email() for _ in range(len(customer_df))]
        customer_df['customer_phone'] = [self.fake.phone_number() for _ in range(len(customer_df))]
        return customer_df.rename(columns={
            'Customer ID': 'customer_id',
            'Customer Name': 'customer_name'
        })

    def prepare_dim_product(self, df: pd.DataFrame) -> pd.DataFrame:
        """Extract and prepare the product dimension."""
        product_df = df[['Product ID', 'Product Name', 'Category', 'Sub-Category']].drop_duplicates()
        product_df['product_price'] = [round(random.uniform(10, 500), 2) for _ in range(len(product_df))]
        product_df['product_description'] = [self.fake.sentence() for _ in range(len(product_df))]
        product_df = product_df.rename(columns={
            'Product ID': 'product_id',
            'Product Name': 'product_name',
            'Category': 'product_category',
            'Sub-Category': 'product_subcategory'
        })
        return product_df.drop_duplicates(subset=['product_id'])

    def prepare_dim_store(self, n_stores: int = 10) -> pd.DataFrame:
        """Generate synthetic store data."""
        store_data = {
            'store_id': [i for i in range(1, n_stores + 1)],
            'store_name': [self.fake.company() for _ in range(n_stores)],
            'store_location': [self.fake.city() for _ in range(n_stores)]
        }
        return pd.DataFrame(store_data)

    def prepare_dim_time(self, df: pd.DataFrame) -> pd.DataFrame:
        """Extract and prepare the time dimension."""
        dates = pd.to_datetime(df['Order Date']).drop_duplicates().reset_index(drop=True)
        time_df = pd.DataFrame({
            'Date': dates,
            'Year': dates.dt.year,
            'Quarter': dates.dt.quarter,
            'Month': dates.dt.month,
            'Day': dates.dt.day,
            'DayOfWeek': dates.dt.dayofweek + 1,
            'DayName': dates.dt.strftime('%A'),
            'IsWeekend': dates.dt.dayofweek >= 5
        })
        time_df['time_id'] = time_df['Date'].dt.strftime('%Y%m%d').astype(int)
        return time_df

    def prepare_fact_sales(self, df: pd.DataFrame, time_df: pd.DataFrame) -> pd.DataFrame:
        """Extract and prepare the fact sales data."""
        df['Order Date'] = pd.to_datetime(df['Order Date'])
        df = df.merge(time_df[['Date', 'time_id']], left_on='Order Date', right_on='Date', how='left')
        fact_sales_df = df.rename(columns={
            'Order ID': 'sale_id',
            'Product ID': 'product_id',
            'Customer ID': 'customer_id',
            'Quantity': 'quantity_sold',
            'Sales': 'total_sale_amount'
        })
        fact_sales_df['store_id'] = random.randint(1, 10)
        return fact_sales_df[['sale_id', 'product_id', 'store_id', 'customer_id', 'time_id', 'quantity_sold', 'total_sale_amount']]

    def load_data(self, df: pd.DataFrame, table_name: str):
        """Load data into the MySQL database."""
        if df.empty:
            logger.warning(f"No data to load for {table_name}")
            return
        try:
            if table_name == 'dim_product':
                df = df.drop_duplicates(subset=['product_id'])
            elif table_name == 'dim_customer':
                df = df.drop_duplicates(subset=['customer_id'])
            elif table_name == 'fact_sales':
                df = df.drop_duplicates(subset=['sale_id'])
            
            df.to_sql(table_name, self.engine, if_exists='append', index=False, method='multi', chunksize=1000)
            logger.info(f"Loaded {len(df)} records into {table_name}.")
        except Exception as e:
            logger.error(f"Error loading {table_name}: {e}")

    def process(self, directory_path: str):
        """Run the entire ETL process for all CSV files in the directory."""
        df = self.extract_csvs(directory_path)

        # Prepare dimensions
        customer_dim = self.prepare_dim_customer(df)
        product_dim = self.prepare_dim_product(df)
        store_dim = self.prepare_dim_store()
        time_dim = self.prepare_dim_time(df)

        # Prepare fact tables
        fact_sales = self.prepare_fact_sales(df, time_dim)

        # Load data into the database
        self.load_data(customer_dim, 'dim_customer')
        self.load_data(product_dim, 'dim_product')
        self.load_data(store_dim, 'dim_store')
        self.load_data(time_dim, 'dim_time')
        self.load_data(fact_sales, 'fact_sales')

def main():
    db_uri = 'mysql+pymysql://root:12345@localhost/warehouse_db'
    etl = SuperstoreETL(db_uri)
    directory_path = "data/ETL1"
    etl.process(directory_path)

if __name__ == "__main__":
    main()
