import pendulum

from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator

VALUE = "my_city"

with DAG(dag_id="dynamic_task_mapping",
         start_date=pendulum.datetime(2022, 10, 1, tz="UTC"),
         schedule_interval="@daily",
         catchup=False,
         params={'param1': '10', 'param2': 'default_value2'},
         ) as dag:

    start, end = [EmptyOperator(task_id=tid, trigger_rule="all_done") for tid in ["start", "end"]]

    @task
    def create_data(val):
        xx = []
        for x in range(int("{params.param1}")):
            xx.append({"name": "Santanu", "city": val})
        d1 = {"name": "Santanu", "city": val}
        d2 = {"name": "Ghosh", "city": val}
        return xx

    @task
    def print_data(data):
        print(data)

    create_data = create_data(val=VALUE)

    start >> create_data >> print_data.expand(data=create_data) >> end
