# Real-Time Earthquake Data Engineering Pipeline using Databricks
## Project Overview
This project demonstrates an end-to-end Data Engineering pipeline built on Databricks to ingest, process, and analyze real-time earthquake data from the USGS Earthquake API.
The pipeline follows modern Data Engineering practices by implementing data ingestion, streaming transformations, Change Data Capture (CDC), and a Medallion Architecture (Bronze and Silver layers). The processed data is then used to create an interactive Power BI dashboard for earthquake analysis.

## Project Architecture
USGS Earthquake API
        │
        ▼
Bronze Layer (Raw JSON)
        │
        ▼
Delta Live Tables (Streaming)
        │
        ▼
Silver Layer (Clean & Validated Data)
        │
        ▼
Power BI Dashboard

## Technologies Used
- Databricks - PySpark  - Spark Structured Streaming  - Delta Live Tables (DLT)  - Delta Lake  - Unity Catalog  - Databricks Asset Bundles  - SQL  - REST API
- Power BI  - Git & GitHub

## Data Pipeline
### Step 1: Data Ingestion

- Connect to the USGS Earthquake API.
- Fetch earthquake data in JSON format.
- Save raw data into a Databricks Volume (Bronze Laye

### Step 2: Data Transformation
- Read JSON files using Spark Structured Streaming.
- Parse nested JSON data.
- Flatten the dataset.
- Convert timestamps.
- Cast numeric columns.
- Add metadata such as load timestamps.

### Step 3: Delta Live Tables

- Create a streaming view.
- Build the Silver table.
- Apply CDC using `dlt.apply_changes()`.
- Store data as SCD Type 1.

### Step 4: Data Visualization
The processed Silver layer is connected to Power BI to build an interactive dashboard showing:
- Earthquake Magnitude
- Location
- Time
- Tsunami Alerts
- Earthquake Distribution
- Earthquake Trends

## 📊 Dashboard
The Power BI dashboard provides interactive insights into earthquake events and helps users explore earthquake activity through charts, maps, and summary metrics.

👨‍💻 Author
**Samrat Thapa**
Computer Engineering Graduate
Aspiring Data Engineer
LinkedIn: https://linkedin.com/in/samrat-thapa
GitHub: https://github.com/Samrat0294
