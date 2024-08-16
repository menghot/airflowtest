import datetime
import pendulum

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


def task_failure_alert(context):
    print(f"Task has failed, task_instance_key_str: {context['task_instance_key_str']}")


def dag_success_alert(context):
    print(f"----dag_success_alert-------------DAG has succeeded, run_id: {context['run_id']}")


def my_dag_success_alert(context):
    print(f"-----my_dag_success_alert------My DAG has succeeded, run_id: {context['run_id']}")


with DAG(
        dag_id="example_callback",
        schedule=None,
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        dagrun_timeout=datetime.timedelta(minutes=60),
        catchup=False,
        on_success_callback=my_dag_success_alert,
        on_failure_callback=task_failure_alert,
        tags=["example"],
):
    task1 = EmptyOperator(task_id="task1")
    task2 = EmptyOperator(task_id="task2")
    task3 = EmptyOperator(task_id="task3", on_success_callback=[dag_success_alert])


    @task()
    def echo_end():
        print("123. ")
        configs = []
        for i in range(3):
            configs.append({"bash_command": "echo " + f"new_add_task_{i}"})
        return configs


    a = echo_end.override(task_id="a")()
    b = echo_end.override(task_id="b")()
    c = echo_end.override(task_id="c")()

    bash_echo = BashOperator.partial(task_id="bash_echo",
                                     on_success_callback=dag_success_alert,
                                     on_failure_callback=task_failure_alert).expand_kwargs(c)

    task1 >> task2 >> task3 >> a >> b >> c >> bash_echo >> echo_end.override(task_id="d")()
