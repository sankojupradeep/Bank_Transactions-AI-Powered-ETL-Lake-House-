from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Default args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 2),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': lambda context: print(f"Task {context['task_instance'].task_id} failed! Check logs."),
}

dag = DAG(
    'banking_pipeline_orchestration',
    default_args=default_args,
    description='Full ETL pipeline: raw → bronze → silver → gold',
    schedule_interval='0 0 * * *',
    catchup=False,
    tags=['banking', 'etl', 'delta']
)

PROJECT_ROOT = '/mnt/d/Financial_Transaction'
JAVA_HOME = '/usr/lib/jvm/java-11-openjdk-amd64'  # Matches Dockerfile (Java 11)

# Task 1: Append raw data
append_raw_task = BashOperator(
    task_id='append_raw_data',
    bash_command=f'cd {PROJECT_ROOT}/data && python3 raw_data.py',
    dag=dag
)

# Task 2: Bronze ingestion
bronze_task = BashOperator(
    task_id='bronze_ingestion',
    bash_command=f"""
    export JAVA_HOME={JAVA_HOME}
    export PATH=$JAVA_HOME/bin:$PATH
    cd {PROJECT_ROOT}/Bronze_layer && python3 csv_to_delta.py
    """,
    dag=dag
)

# Task 3: Silver transformation
silver_task = BashOperator(
    task_id='silver_transformation',
    bash_command=f"""
    export JAVA_HOME={JAVA_HOME}
    export PATH=$JAVA_HOME/bin:$PATH
    cd {PROJECT_ROOT}/Silver_layer && python3 transformations.py
    """,
    dag=dag
)

# Task 4: Gold dimensions
gold_dims_task = BashOperator(
    task_id='gold_dimensions',
    bash_command=f"""
    export JAVA_HOME={JAVA_HOME}
    export PATH=$JAVA_HOME/bin:$PATH
    cd {PROJECT_ROOT}/Gold_layer && python3 dimension.py
    """,
    dag=dag
)

# Task 5: Gold facts
gold_facts_task = BashOperator(
    task_id='gold_facts',
    bash_command=f"""
    export JAVA_HOME={JAVA_HOME}
    export PATH=$JAVA_HOME/bin:$PATH
    cd {PROJECT_ROOT}/Gold_layer && python3 fact.py
    """,
    dag=dag
)

# Task dependencies
append_raw_task >> bronze_task >> silver_task >> gold_dims_task >> gold_facts_task