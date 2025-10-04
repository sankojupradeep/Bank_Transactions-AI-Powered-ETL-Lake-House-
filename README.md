# 🏦 Banking Data Lakehouse Pipeline

## 📌 Project Overview  
This project implements a **Banking Data Lakehouse Pipeline** using **Apache Spark, Delta Lake, and Apache Airflow**.  
The goal is to design an **end-to-end ETL pipeline** that ingests raw banking transactions, applies transformations, and produces **fact and dimension tables** for analytical dashboards.

The architecture follows a **multi-layered lakehouse approach**:
- **Raw Layer** → Generates synthetic transactional data  
- **Bronze Layer** → Ingests raw CSV data into Delta tables  
- **Silver Layer** → Cleans, standardizes, and enriches data  
- **Gold Layer** → Builds fact & dimension tables for BI and reporting  

---

## ⚙️ Tools & Technologies  
- **Python 3.11** – Data processing & scripting  
- **Apache Spark 4.0.1** – Distributed data processing engine  
- **Delta Lake** – ACID-compliant storage format  
- **Apache Airflow** – Workflow orchestration & scheduling  
- **AWS S3** – Cloud data lake storage  
- **Streamlit + Plotly** – Interactive dashboards  
- **Docker / WSL2 (Ubuntu)** – Development environment  




## 📂 Project Structure  
```bash
Financial_Transaction Project/
│
├── data/                  # Raw data generator (raw_data.py)
├── Bronze_layer/          # Ingestion scripts (csv_to_delta.py)
├── Silver_layer/          # Data transformations (transformations.py)
├── Gold_layer/            # Dimension & Fact table scripts (dimension.py, fact.py)
├── LLM/                   # Streamlit dashboard & visualizations
├── airflow_dags/          # Airflow DAG (banking_pipeline_orchestration.py)
└── README.md              # Documentation

