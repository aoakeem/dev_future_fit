import json
import pandas as pd
import duckdb
import os
from datetime import datetime

def run_etl_pipeline(input_file, output_db, incremental=False):
    
    with open(input_file, 'r') as f:
        data = json.load(f)
    
    professionals, jobs, skills, certifications, education = [], [], [], [], []
    
    for prof in data.get('professionals', []):
        prof_id = prof.get('professional_id')
        professionals.append({
            'professional_id': prof_id,
            'years_experience': prof.get('years_experience'),
            'current_industry': prof.get('current_industry'),
            'current_role': prof.get('current_role'),
            'education_level': prof.get('education_level')
        })
        
        for job in prof.get('jobs', []):
            jobs.append({
                'job_id': job.get('job_id'),
                'professional_id': prof_id,
                'company': job.get('company'),
                'role': job.get('role'),
                'start_date': job.get('start_date'),
                'end_date': job.get('end_date'),
                'is_current': job.get('end_date') is None
            })
        
        for skill in prof.get('skills', []):
            skills.append({
                'professional_id': prof_id,
                'skill_name': skill.get('skill_name'),
                'proficiency_level': skill.get('proficiency_level')
            })
        
        for cert in prof.get('certifications', []):
            certifications.append({
                'professional_id': prof_id,
                'certification_id': cert.get('certification_id'),
                'certification_name': cert.get('certification_name'),
                'issuing_organization': cert.get('issuing_organization'),
                'date_earned': cert.get('date_earned')
            })
        
        for edu in prof.get('education', []):
            education.append({
                'professional_id': prof_id,
                'education_id': edu.get('education_id', ''),
                'degree': edu.get('degree'),
                'institution': edu.get('institution'),
                'field_of_study': edu.get('field_of_study'),
                'graduation_date': edu.get('graduation_date')
            })
    
    dfs = {
        'professionals': pd.DataFrame(professionals),
        'jobs': pd.DataFrame(jobs),
        'skills': pd.DataFrame(skills),
        'certifications': pd.DataFrame(certifications),
        'education': pd.DataFrame(education)
    }
    # Ensure education dataframe has the required columns
    if 'education_id' not in dfs['education'].columns:
        dfs['education']['education_id'] = ''
    if 'degree' not in dfs['education'].columns:
        dfs['education']['degree'] = ''
    
    # data validation    

    # Check for missing IDs
    missing_prof_ids = dfs['professionals']['professional_id'].isnull().sum()
    if missing_prof_ids > 0:
        print(f"Warning: {missing_prof_ids} professionals missing IDs")
    
    # Check for date inconsistencies in jobs
    dfs['jobs']['start_date'] = pd.to_datetime(dfs['jobs']['start_date'], errors='coerce')
    dfs['jobs']['end_date'] = pd.to_datetime(dfs['jobs']['end_date'], errors='coerce')
    invalid_dates = ((~dfs['jobs']['start_date'].isnull()) & 
                     (~dfs['jobs']['end_date'].isnull()) & 
                     (dfs['jobs']['end_date'] < dfs['jobs']['start_date'])).sum()
    if invalid_dates > 0:
        print(f"Warning: {invalid_dates} jobs with end date before start date")
    
    # Load data into DuckDB
    try:
        conn = duckdb.connect(output_db)
        conn.register("professionals", dfs["professionals"])
        conn.register("jobs", dfs["jobs"])
        conn.register("skills", dfs["skills"])
        conn.register("certifications", dfs["certifications"])
        conn.register("education", dfs["education"])
        create_tables(conn)
        
        if incremental:
            incremental_load(conn, dfs)
        else:
            full_refresh(conn, dfs)
        
        print("\nLoaded Tables:")
        for table in conn.execute("SHOW TABLES").fetchall():
            print(f"- {table[0]}: {conn.execute(f'SELECT COUNT(*) FROM {table[0]}').fetchone()[0]} rows")
        
        conn.close()
        print(f"\nETL completed successfully: {output_db}")
        return True
    except Exception as e:
        print(f"Error: {str(e)}")
        return False

def create_tables(conn):
    """Create tables if they don't exist"""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS dim_professional (
            professional_id VARCHAR PRIMARY KEY,
            years_experience INTEGER,
            current_industry VARCHAR,
            current_role VARCHAR,
            education_level VARCHAR
        )
    """)
    
    conn.execute("""
        CREATE TABLE IF NOT EXISTS fact_professional_job (
            job_id VARCHAR PRIMARY KEY,
            professional_id VARCHAR,
            company VARCHAR,
            role VARCHAR,
            start_date DATE,
            end_date DATE,
            is_current BOOLEAN
        )
    """)
    
    conn.execute("""
        CREATE TABLE IF NOT EXISTS fact_professional_skill (
            id INTEGER,
            professional_id VARCHAR,
            skill_name VARCHAR,
            proficiency_level VARCHAR
        )
    """)
    
    conn.execute("""
        CREATE TABLE IF NOT EXISTS dim_certification (
            certification_id VARCHAR PRIMARY KEY,
            certification_name VARCHAR,
            issuing_organization VARCHAR
        )
    """)
    
    conn.execute("""
        CREATE TABLE IF NOT EXISTS dim_education (
            education_id VARCHAR PRIMARY KEY,
            degree VARCHAR,
            institution VARCHAR,
            field_of_study VARCHAR
        )
    """)
    
    conn.execute("""
        CREATE TABLE IF NOT EXISTS fact_professional_certification (
            id INTEGER,
            professional_id VARCHAR,
            certification_id VARCHAR,
            date_earned DATE
        )
    """)
    
    conn.execute("""
        CREATE TABLE IF NOT EXISTS fact_professional_education (
            id INTEGER,
            professional_id VARCHAR,
            education_id VARCHAR,
            graduation_date DATE
        )
    """)

def full_refresh(conn, dfs):
    print("\nPerforming full refresh...")
    
    try:
        conn.execute("DELETE FROM fact_professional_education")
        conn.execute("DELETE FROM fact_professional_certification")
        conn.execute("DELETE FROM fact_professional_skill")
        conn.execute("DELETE FROM fact_professional_job")
        conn.execute("DELETE FROM dim_professional")
        
        if not dfs['professionals'].empty:
            conn.execute("INSERT INTO dim_professional SELECT * FROM professionals")
        
        if not dfs['jobs'].empty:
            conn.execute("INSERT INTO fact_professional_job SELECT * FROM jobs")
        
        if not dfs['skills'].empty:
            conn.execute("INSERT INTO fact_professional_skill SELECT ROW_NUMBER() OVER (ORDER BY professional_id) as id, professional_id, skill_name, proficiency_level FROM skills")
        
        if not dfs['certifications'].empty:
            conn.execute("DELETE FROM dim_certification")
            conn.execute("INSERT INTO dim_certification(certification_id, certification_name, issuing_organization) SELECT DISTINCT certification_id, certification_name, issuing_organization FROM certifications")
            conn.execute("INSERT INTO fact_professional_certification SELECT ROW_NUMBER() OVER (ORDER BY c.professional_id) as id, c.professional_id, d.certification_id, CAST(c.date_earned AS DATE) FROM certifications c JOIN dim_certification d ON c.certification_id = d.certification_id")
        
        if not dfs['education'].empty:
            conn.execute("DELETE FROM dim_education")
            conn.execute("INSERT INTO dim_education(education_id, degree, institution, field_of_study) SELECT DISTINCT education_id, degree, institution, field_of_study FROM education")
            conn.execute("INSERT INTO fact_professional_education SELECT ROW_NUMBER() OVER (ORDER BY e.professional_id) as id, e.professional_id, d.education_id, CAST(e.graduation_date AS DATE) FROM education e JOIN dim_education d ON e.education_id = d.education_id")
        
        return True
    except Exception as e:
        print(f"Error in full refresh: {str(e)}")
        return False

def incremental_load(conn, dfs):
    print("\nPerforming incremental load...")
    
    try:
        if not dfs['professionals'].empty:
            prof_ids = dfs['professionals']['professional_id'].tolist()
            if prof_ids:
                conn.execute(f"DELETE FROM dim_professional WHERE professional_id IN {tuple(prof_ids)}")
                conn.execute("INSERT INTO dim_professional SELECT * FROM professionals")
        
        if not dfs['jobs'].empty:
            conn.execute("""
                INSERT INTO fact_professional_job 
                SELECT * FROM jobs j 
                WHERE j.start_date > (
                    SELECT COALESCE(MAX(start_date), '1900-01-01') 
                    FROM fact_professional_job f 
                    WHERE f.professional_id = j.professional_id
                )
            """)
        
        if not dfs['skills'].empty:
            prof_ids = dfs['skills']['professional_id'].unique().tolist()
            if prof_ids:
                conn.execute(f"DELETE FROM fact_professional_skill WHERE professional_id IN {tuple(prof_ids)}")
                conn.execute("INSERT INTO fact_professional_skill SELECT ROW_NUMBER() OVER (ORDER BY professional_id) as id, professional_id, skill_name, proficiency_level FROM skills")
        
        if not dfs['certifications'].empty:
            conn.execute("""INSERT INTO dim_certification(certification_id, certification_name, issuing_organization) 
                         SELECT DISTINCT certification_id, certification_name, issuing_organization FROM certifications
                          EXCEPT SELECT certification_id, certification_name, issuing_organization FROM dim_certification""")
            
            conn.execute("""INSERT INTO fact_professional_certification 
                         SELECT ROW_NUMBER() OVER (ORDER BY c.professional_id) as id, c.professional_id, d.certification_id, CAST(c.date_earned AS DATE) 
                         FROM certifications c 
                         JOIN dim_certification d ON c.certification_id = d.certification_id 
                         WHERE CAST(c.date_earned AS DATE) > (SELECT COALESCE(MAX(CAST(date_earned AS DATE)), DATE '1900-01-01') 
                         FROM fact_professional_certification f 
                         WHERE f.professional_id = c.professional_id AND f.certification_id = d.certification_id)""")
        
        if not dfs['education'].empty:
            conn.execute("""INSERT INTO dim_education(education_id, degree, institution, field_of_study) 
                         SELECT DISTINCT education_id, degree, institution, field_of_study FROM education EXCEPT 
                         SELECT education_id, degree, institution, field_of_study FROM dim_education""")
            
            conn.execute("""INSERT INTO fact_professional_education 
                         SELECT ROW_NUMBER() OVER (ORDER BY e.professional_id) as id, e.professional_id, d.education_id, CAST(e.graduation_date AS DATE) 
                         FROM education e 
                         JOIN dim_education d ON e.education_id = d.education_id 
                         WHERE CAST(e.graduation_date AS DATE) > (SELECT COALESCE(MAX(CAST(graduation_date AS DATE)), DATE '1900-01-01') 
                         FROM fact_professional_education f 
                         WHERE f.professional_id = e.professional_id AND f.education_id = d.education_id)""")
        return True
    except Exception as e:
        print(f"Error in incremental load: {str(e)}")
        return False

if __name__ == "__main__":
    input_file = "professionals_nested.json"
    output_db = "professionals_dimensional.db"
    
    # Check if this is first run or incremental
    is_incremental = os.path.exists(output_db)
    
    success = run_etl_pipeline(input_file, output_db, incremental=is_incremental)
    
    if not success:
        print("ETL process failed.") 