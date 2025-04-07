# 🗽 NYC Restaurant Inspection Data Pipeline

This project is a pure Python data engineering pipeline that processes and analyzes New York City restaurant inspection data. The pipeline performs the following major steps:

- **Data Loading**
- **Data Cleaning**
- **Data Validation**
- **Data Transformation / Enrichment**
- **Data Storage to SQL Server**
- **SQL-Based Data Cleaning**
- **Automated Stored Procedure Execution**
- (Upcoming: Scheduling, Visualization, Reporting)

---

## 🏗️ Project Architecture

```
nyc_data_project/
│
├── config/                   # Config files
│   └── config.yaml
│
├── data/                     # Raw CSV input files
│
├── logs/                     # Pipeline logs
│   └── pipeline.log
│
├── output/                   # Output CSVs (cleaned, transformed)
│   ├── DOHMH_New_York_City_Restaurant_Inspection_C.csv
│   └── DOHMH_New_York_City_Restaurant_Inspection_T.csv
│
├── src/                      # Source code
│   ├── config_loader.py
│   ├── data_cleaner.py
│   ├── data_loader.py
│   ├── data_transformer.py
│   ├── data_validator.py
│   ├── logger.py
│   ├── pipeline_runner.py
│   └── save_data.py
│
│── sql/
│   ├── 01_create_raw_table.sql
│   ├── 02_create_clean_table.sql
│   ├── 03_cleaning_procedure.sql
│ 
│
├── .gitignore
├── requirements.txt
├── main.py
└── README.md


```

---

## 🧪 How to Run the Pipeline

1. **Install dependencies**  
```bash
pip install -r requirements.txt
```

2. **Update config.yaml**  
Make sure `csv_path` points to your raw CSV and `output_path` for saving files.

3. **Run the pipeline**  
```bash
python src/pipeline_runner.py
```

4. **(Optional)**: View cleaned data  
```bash
python view_cleaned_data.py
```

---

## ✅ Completed Modules

- [x] Data Loader
- [x] Data Cleaner
- [x] Data Validator
- [x] Data Transformer
- [x] Logging System (auto truncates on each run)
- [x] SQL Server Integration
- [x] Stored Procedure Execution from Python
- [x] Git + GitHub setup

---

## 🔜 Upcoming

- Data Visualization Dashboard
- Scheduled Automation with Airflow or Cron
- Unit Tests

---

## 📒 Notes

- All logs are saved in logs/pipeline.log and refreshed each time the pipeline is run.
- The database cleaning logic lives inside the SQL Server stored procedure

---

## 📌 Author

**Sree Madhuchandra Gamidi**  
GitHub: [@Madhugamidi14](https://github.com/Madhugamidi14)
