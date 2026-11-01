from airflow.sdk import TaskGroup, chain
from blueprint import BaseModel, Blueprint, Field
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator


def extract_data_function():
    import random
    return {"my_source_number": random.randint(1, 100)}


def multiply_by_x_function(x: int, input_data: dict) -> dict:
    result = input_data["my_source_number"] * x
    return {"my_result": result}


class MyMathETLConfig(BaseModel):
    my_number: int = Field(
        default=2,
        description="Number to multiply the source by",
    )
    my_name: str = Field(
        default="Rémy",
        description="Name to print",
    )


class MyMathETLBlueprint(Blueprint[MyMathETLConfig]):
    
    def render(self, config: MyMathETLConfig) -> TaskGroup:
        with TaskGroup(group_id=self.step_id) as group:
            _extract = PythonOperator(
                task_id="extract",
                python_callable=extract_data_function,
            )
            _multiply = PythonOperator(
                task_id="multiply",
                python_callable=multiply_by_x_function,
                op_kwargs={"x": config.my_number, "input_data": _extract.output},
            )
            _print_result = BashOperator(
                task_id="print_result",
                bash_command=(
                    f"echo 'Hello {config.my_name}! The result is "
                    "{{ task_instance.xcom_pull(task_ids='my_math_etl.multiply') }}'"
                ),
            )

            chain(_extract, _multiply, _print_result)
            # Chain basically tells the order of execution. Once _extract is successful then run _multiply
        return group