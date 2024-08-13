from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

def dynamic_task_creation():
    # Example function to create tasks dynamically
    # You can replace this with logic to determine tasks
    return [
        {'task_id': 'task_1', 'message': 'Task 1 executed'},
        {'task_id': 'task_2', 'message': 'Task 2 executed'},
        {'task_id': 'task_3', 'message': 'Task 3 executed'}
    ]

def print_message(message):
    print(message)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dynamic_task_example',
    default_args=default_args,
    description='An example DAG for dynamic task creation',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 8, 1),
    catchup=False,
)

# Initial dummy task
start = DummyOperator(
    task_id='start',
    dag=dag,
)

# Create tasks dynamically
tasks = dynamic_task_creation()
previous_task = start

for task in tasks:
    new_task = PythonOperator(
        task_id=task['task_id'],
        python_callable=print_message,
        op_args=[task['message']],
        dag=dag,
    )
    previous_task >> new_task
    previous_task = new_task

end = DummyOperator(
    task_id='end',
    dag=dag,
)

previous_task >> end
