import json
from datetime import datetime
from airflow.decorators import dag, task, task_group

default_args = {
    'start_date': datetime(2021, 1, 1)
}

import random
import string


def generate_random_string(length):
    return ''.join(random.choices(string.ascii_letters, k=length))


def generate_random_array(max_length, string_length):
    array_length = random.randint(1, max_length)
    return [generate_random_string(string_length) for _ in range(array_length)]


@dag(
    dag_id='xcom_taskflow_dag',
    schedule_interval='@daily',
    default_args=default_args,
    params={'param1': '20', 'param2': 'default_value2'},
    catchup=False)
def taskflow():
    @task()
    def get():
        return generate_random_array(10, 4)

    @task_group(group_id='group')
    def group(data):
        @task(task_id=f'subtask')
        def unitask(t):
            return {"result": t}

        tasks_result = unitask.expand(t=data)
        return tasks_result

    group(get())


dag = taskflow()
