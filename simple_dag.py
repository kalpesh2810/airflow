from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Task functions
def print_hello():
    print("Hello, Airflow!")

def print_goodbye():
    print("Goodbye, Airflow!")

def print_task_execution(task_number):
    print(f"Executing Task {task_number}")

def print_custom_message(task_name):
    print(f"Task {task_name} says hello!")

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 30),  # The start date for the DAG
    'retries': 1,
}

# Define the DAG
dag = DAG(
    'simple_dag',  
    default_args=default_args,
    description='A simple hello world DAG',
    schedule_interval='@daily', #run daily
    schedule_interval='*/2 * * * *',  # Run every 2 minutes
    catchup=False,  
)

# Define tasks
hello_task = PythonOperator(
    task_id='print_hello_task',  
    python_callable=print_hello,  
    dag=dag,  
)

goodbye_task = PythonOperator(
    task_id='print_goodbye_task',
    python_callable=print_goodbye,
    dag=dag,
)

# Create dynamic tasks
for i in range(1, 4):
    dynamic_task = PythonOperator(
        task_id=f'print_task_{i}',
        python_callable=lambda task_number=i: print_task_execution(task_number),
        dag=dag,
    )
    # Sequential execution (one after the other)
    hello_task >> dynamic_task >> goodbye_task

# Add custom tasks
custom_task_1 = PythonOperator(
    task_id='custom_task_1',
    python_callable=lambda: print_custom_message("Custom Task 1"),
    dag=dag,
)

custom_task_2 = PythonOperator(
    task_id='custom_task_2',
    python_callable=lambda: print_custom_message("Custom Task 2"),
    dag=dag,
)

custom_task_3 = PythonOperator(
    task_id='custom_task_3',
    python_callable=lambda: print_custom_message("Custom Task 3"),
    dag=dag,
)

custom_task_4 = PythonOperator(
    task_id='custom_task_4',
    python_callable=lambda: print_custom_message("Custom Task 4"),
    dag=dag,
)

# Sequential execution of tasks
hello_task >> custom_task_1 >> custom_task_2 >> custom_task_3 >> custom_task_4 >> goodbye_task
