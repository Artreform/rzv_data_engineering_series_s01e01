
import json

import pendulum

from airflow.decorators import dag, task, task_group
from airflow.operators.empty import EmptyOperator

@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["learning", "example"],
)
def tutorial_taskflow_api():
    """
    ### TaskFlow API Tutorial Documentation
    This is a simple data pipeline example which demonstrates the use of
    the TaskFlow API using three simple tasks for Extract, Transform, and Load.
    Documentation that goes along with the Airflow TaskFlow API tutorial is
    located
    [here](https://airflow.apache.org/docs/apache-airflow/stable/tutorial_taskflow_api.html)
    """
    @task()
    def extract():
        """
        #### Extract task
        A simple Extract task to get data ready for the rest of the data
        pipeline. In this case, getting data is simulated by reading from a
        hardcoded JSON string.
        """
        data_string = '{"1001": 301.27, "1002": 433.21, "1003": 502.22}'

        order_data_dict = json.loads(data_string)
        return order_data_dict
    @task(multiple_outputs=True)
    def transform(order_data_dict: dict, **context):
        """
        #### Transform task
        A simple Transform task which takes in the collection of order data and
        computes the total order value.
        """
        total_order_value = 0
        print("task run exec date is")
        print(context["dag_run"].execution_date)

        for value in order_data_dict.values():
            total_order_value += value

        return {"total_order_value": total_order_value}
    



    @task()
    def load(total_order_value: float):
        """
        #### Load task
        A simple Load task which takes in the result of the Transform task and
        instead of saving it to end user review, just prints it out.
        """

        print(f"Total order value is: {total_order_value:.2f}")
    




    for item in ['sales', 'customers']:
        @task_group(group_id=item)
        def tg1():
            t1 = EmptyOperator(task_id='task_1')
            t2 = EmptyOperator(task_id='task_2')


        tg1()

    tg1_task = tg1()



    order_data = extract()
    order_summary = transform(order_data)
    load(order_summary["total_order_value"])
    tg1_task
tutorial_taskflow_api()
