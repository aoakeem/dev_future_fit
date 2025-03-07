# Career Data Pipeline

## Overview
Career ETL pipeline that transforms nested JSON data about professionals into a dimensional model using pandas and DuckDB.

## Data Model
- **dim_professional**: Professional details (name, email, experience, role)
- **dim_certification**: List of Certifications 
- **dim_education**: List of Education 
- **fact_professional_job**: Work experience history
- **fact_professional_skill**: Skills and proficiency levels
- **fact_professional_certification**: Certifications earned by Professionals
- **fact_professional_education**: Education history of Professionals


## Implementation
The solution has two main components:

### 1. ETL Pipeline (`etl_professionals.py`)
- Extracts data from nested JSON
- Transforms into structured dataframes
- Loads into DuckDB tables
- Supports both full refresh and incremental loading

### 2. Airflow DAG (`career_data_dag.py`)
- Daily scheduled pipeline
- Automatic incremental detection

## Usage
```bash
# Run ETL pipeline directly
python etl_professionals.py

# Deploy Airflow DAG
cp career_data_dag.py $AIRFLOW_HOME/dags/
```

## Airflow set up requirements 
 - Airflow version: 2.10.1
 - Python version: 3.11

## Production Considerations
- Configure environment-specific parameters (e.g., file paths, database connections) via environment variables or configuration files.
- Implement  error handling and logging
- Set up Alerts for Monitoring when job passes or fails (Alerts can be integrated to team slack channel)
- Automate tests and continuous integration (CI) to validate pipeline changes.

