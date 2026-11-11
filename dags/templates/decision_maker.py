from blueprint import Blueprint
from pydantic import BaseModel
from airflow.providers.standard.operators.python import BranchPythonOperator


class DecisionConfig(BaseModel):
    number: int
    even_task_id: str
    odd_task_id: str


def decide_branch(number: int, even_task_id: str, odd_task_id: str):
    print(f"Checking number: {number}")
    if number % 2 == 0:
        return even_task_id
    return odd_task_id


class DecisionMaker(Blueprint[DecisionConfig]):

    def render(self, config: DecisionConfig):
        return BranchPythonOperator(
            task_id=self.step_id,
            python_callable=decide_branch,
            op_kwargs={
                "number": config.number,
                "even_task_id": config.even_task_id,
                "odd_task_id": config.odd_task_id,
            },
        )
