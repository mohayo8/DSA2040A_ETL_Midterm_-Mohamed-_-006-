# DSA2040A_ETL_Midterm_-Mohamed-_-006-

# 📊 ETL Midterm Project – DSA 2040A (Data Warehousing & Mining)

**Student Name**: MOHAMED 
**Student ID (Last 3 Digits)**: 006

---

## 🧠 Project Overview

This ETL mini-project demonstrates the practical implementation of the **Extract → Transform → Load (ETL)** process. The goal is to prepare and load sales order data from raw CSV files into structured database formats while ensuring cleanliness, consistency, and usability for analysis. The full pipeline is implemented in Python using **pandas** and **SQLite**, and is version-controlled via GitHub with clear documentation and structure.

The project simulates a real-world data engineering task and is submitted through a well-organized GitHub repository for evaluation.

---

## 🔍 Dataset Description

The dataset includes sales order information with the following fields:

- `order_id`: Unique identifier for each order
- `customer_name`: Name of the customer
- `product`: Product purchased
- `quantity`: Number of items purchased
- `unit_price`: Price per item
- `order_date`: Date of order placement (dd/mm/yyyy format)
- `region`: Geographic region of the customer

Both `raw_data.csv` (full dataset) and `incremental_data.csv` are processed through the ETL pipeline.

---

## ⚙️ ETL Pipeline Breakdown

### 1️⃣ Extract Phase — `etl._extract.ipynb`

This notebook handles the initial data loading and inspection.

- **Loaded** both `raw_data.csv` and `incremental_data.csv` using `pandas.read_csv()`
- **Previewed** the datasets with `.head()` and `.info()` to understand their structure
- **Identified** potential issues:
  - Missing values in numeric columns
  - Duplicate records
  - Date format as strings (dd/mm/yyyy)
- **Saved** clean copies of both files into the `data/` directory for reproducibility

> 🔍 This step ensured we began the pipeline with clean, well-documented input.

---

### 2️⃣ Transform Phase — `etl_transform.ipynb`

In this notebook, I applied meaningful transformations to prepare the data for analysis and loading.

#### 🔧 Transformations Applied:

1. **Removed Duplicate Rows**  
   Ensures no repeated transactions distort aggregations or insights.

2. **Handled Missing Values**  
   For numeric columns like `quantity` or `unit_price`, filled missing values using the **median**, preserving data consistency while avoiding skewing with outliers.

3. **Created `total_price` Column**  
   Enriched the dataset by adding `total_price = quantity * unit_price`, a common metric in sales analytics.

4. **Converted `order_date` to Datetime Format**  
   Standardized the `order_date` column using `pd.to_datetime(..., dayfirst=True)` to allow for time-based filtering, grouping, and trend analysis.

> 🔄 The transformed datasets were saved as:
- `transformed/transformed_full.csv`
- `transformed/transformed_incremental.csv`

---

### 3️⃣ Load Phase — `etl_load.ipynb`

The final stage loads the transformed data into structured database files for efficient querying and long-term storage.

- **Loaded `transformed_full.csv`** into `loaded/full_data.db` under the table `full_data`
- **Loaded `transformed_incremental.csv`** into a **separate database** `loaded/incremental_data.db` under the table `incremental_data`

Both databases were verified by running SQL queries to preview their contents.

> 🔗 This decoupled structure allows for scalable, modular loading and easier incremental updates.

---

## 🛠️ Tools & Technologies Used

| Tool         | Purpose                          |
|--------------|----------------------------------|
| **Python 3.x** | Core language for scripting     |
| **Pandas**     | Data loading, transformation    |
| **Jupyter**    | Notebook-based development      |
| **SQLite3**    | Lightweight relational storage  |
| **Git & GitHub** | Version control, documentation |

---

## ▶️ How to Run This Project

1. Clone or download the repository.
2. Ensure Python and Jupyter are installed.
3. Run each notebook in this order:
   - `etl_extract.ipynb`
   - `etl_transform.ipynb`
   - `etl_load.ipynb`
4. Check the output folders:
   - `data/`: raw extracted data
   - `transformed/`: cleaned & enriched CSVs
   - `loaded/`: SQLite `.db` files



