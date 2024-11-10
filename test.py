from pyspark.sql import SparkSession
from pyspark.sql import Row

# Paths and database configuration
JDBC_DRIVER_PATH = "C:\\Program Files\\Java\\mysql-connector-j-9.1.0\\mysql-connector-j-9.1.0.jar"
DATABASE_URI = "jdbc:mysql://localhost:3306/warehouse_db?useSSL=false"
jdbc_properties = {
    "user": "root",
    "password": "12345",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Initialize Spark session with the JDBC driver
spark = SparkSession.builder \
    .appName("MySQL ETL") \
    .config("spark.jars", JDBC_DRIVER_PATH) \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

# Step 3: Create sample data directly in the script
print("\nCreating sample data...")
sample_data = [
    ("101", "John Doe", "123 Elm Street", "johndoe@example.com", "123-456-7890"),
    ("102", "Jane Smith", "456 Oak Avenue", "janesmith@example.com", "987-654-3210"),
    ("103", "Alice Johnson", "789 Pine Road", "alicej@example.com", "555-987-1234")
]

columns = ["customer_id", "customer_name", "customer_address", "customer_email", "customer_phone"]

# Create DataFrame
df = spark.createDataFrame(sample_data, columns)

# Cache the DataFrame to force materialization
df.cache()

# Try showing the data
print("\nShowing manually created DataFrame:")
df.collect()  # Force evaluation
df.show()

# Display the sample data
print("\nSample data created:")
df.printSchema()
print(type(df))
# Step 4: Write DataFrame to MySQL database
try:
    print("\nWriting data to MySQL...")
    df.write.jdbc(
        url=DATABASE_URI,
        table='dim_customer',
        mode='append',
        properties=jdbc_properties
    )
    print("Data successfully written to MySQL!")
except Exception as e:
    print("Error writing data to MySQL:", e)

# Step 5: Read data back from MySQL and display
try:
    print("\nReading data back from MySQL...")
    df_read = spark.read.jdbc(
        url=DATABASE_URI,
        table='dim_customer',
        properties=jdbc_properties
    )
    print("\nData read from MySQL:")
    df_read.show(truncate=False)
    print(type(df_read))
    df_read.printSchema()
except Exception as e:
    print("Error reading data from MySQL:", e)

# Stop the Spark session
spark.stop()
