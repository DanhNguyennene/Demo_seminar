const express = require('express');
const mysql = require('mysql2');
const cors = require('cors');

const app = express();
app.use(cors()); // Enable CORS to allow cross-origin requests

// Create a connection to your MySQL database
const db = mysql.createConnection({
  host: 'localhost',
  user: 'root',       // Your MySQL username
  password: '12345',  // Your MySQL password
  database: 'warehouse_db' // Your database name
});

// Connect to the database
db.connect((err) => {
  if (err) {
    console.error('Error connecting to the database:', err);
    process.exit(1);
  }
  console.log('Connected to MySQL database');
});

// 1. Fetch all fact sales
app.get('/api/fact_sales', (req, res) => {
  const query = 'SELECT * FROM fact_sales';
  db.query(query, (err, results) => {
    if (err) {
      console.error('Error fetching fact_sales:', err);
      res.status(500).send('Internal Server Error');
      return;
    }
    res.json(results);
  });
});

// 2. Fetch sales by product
app.get('/api/sales_by_product', (req, res) => {
  const query = `
    SELECT dp.product_name, SUM(fs.total_sale_amount) AS total_sales
    FROM fact_sales fs
    JOIN dim_product dp ON fs.product_id = dp.product_id
    GROUP BY dp.product_name
    ORDER BY total_sales DESC
  `;
  db.query(query, (err, results) => {
    if (err) {
      console.error('Error fetching sales by product:', err);
      res.status(500).send('Internal Server Error');
      return;
    }
    res.json(results);
  });
});

// 3. Fetch sales by customer
app.get('/api/sales_by_customer', (req, res) => {
  const query = `
    SELECT dc.customer_name, SUM(fs.total_sale_amount) AS total_sales
    FROM fact_sales fs
    JOIN dim_customer dc ON fs.customer_id = dc.customer_id
    GROUP BY dc.customer_name
    ORDER BY total_sales DESC
  `;
  db.query(query, (err, results) => {
    if (err) {
      console.error('Error fetching sales by customer:', err);
      res.status(500).send('Internal Server Error');
      return;
    }
    res.json(results);
  });
});

// 4. Fetch sales by year
app.get('/api/sales_by_year', (req, res) => {
  const query = `
    SELECT dt.Year, SUM(fs.total_sale_amount) AS total_sales
    FROM fact_sales fs
    JOIN dim_time dt ON fs.time_id = dt.time_id
    GROUP BY dt.Year
    ORDER BY dt.Year
  `;
  db.query(query, (err, results) => {
    if (err) {
      console.error('Error fetching sales by year:', err);
      res.status(500).send('Internal Server Error');
      return;
    }
    res.json(results);
  });
});

// 5. Fetch sales by month for a specific year
app.get('/api/sales_by_month/:year', (req, res) => {
  const { year } = req.params;
  const query = `
    SELECT dt.Month, SUM(fs.total_sale_amount) AS total_sales
    FROM fact_sales fs
    JOIN dim_time dt ON fs.time_id = dt.time_id
    WHERE dt.Year = ?
    GROUP BY dt.Month
    ORDER BY dt.Month
  `;
  db.query(query, [year], (err, results) => {
    if (err) {
      console.error('Error fetching sales by month:', err);
      res.status(500).send('Internal Server Error');
      return;
    }
    res.json(results);
  });
});

// 6. Fetch sales by product and store
app.get('/api/sales_by_product_store', (req, res) => {
  const query = `
    SELECT dp.product_name, ds.store_name, SUM(fs.total_sale_amount) AS total_sales
    FROM fact_sales fs
    JOIN dim_product dp ON fs.product_id = dp.product_id
    JOIN dim_store ds ON fs.store_id = ds.store_id
    GROUP BY dp.product_name, ds.store_name
    ORDER BY total_sales DESC
  `;
  db.query(query, (err, results) => {
    if (err) {
      console.error('Error fetching sales by product and store:', err);
      res.status(500).send('Internal Server Error');
      return;
    }
    res.json(results);
  });
});

// 7. Fetch top N products by sales
app.get('/api/top_products', (req, res) => {
  const limit = parseInt(req.query.limit) || 10;
  const query = `
    SELECT dp.product_name, SUM(fs.total_sale_amount) AS total_sales
    FROM fact_sales fs
    JOIN dim_product dp ON fs.product_id = dp.product_id
    GROUP BY dp.product_name
    ORDER BY total_sales DESC
    LIMIT ?
  `;
  db.query(query, [limit], (err, results) => {
    if (err) {
      console.error('Error fetching top products:', err);
      res.status(500).send('Internal Server Error');
      return;
    }
    res.json(results);
  });
});
app.get('/api/aggregated_sales', (req, res) => {
  const query = `
    SELECT 
      dp.product_category AS category,
      SUM(fs.total_sale_amount) AS totalSales
    FROM fact_sales fs
    JOIN dim_product dp ON fs.product_id = dp.product_id
    GROUP BY dp.product_category
    ORDER BY totalSales DESC;
  `;

  db.query(query, (err, results) => {
    if (err) {
      console.error('Error fetching aggregated sales:', err);
      res.status(500).send('Internal Server Error');
      return;
    }
    // Check if results are empty
    if (results.length === 0) {
      res.status(404).json({ message: 'No data found' });
      return;
    }
    res.json(results);
  });
});

// Start the server
const PORT = 5000;
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
