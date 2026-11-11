from blueprint import Blueprint, BaseModel
from airflow.providers.standard.operators.python import PythonOperator


class ProcessOddConfig(BaseModel):
    pass


def process_odd():
    print("Number is odd. Executing Task C.")


class ProcessOdd(Blueprint[ProcessOddConfig]):

    def render(self, config: ProcessOddConfig):
        return PythonOperator(
            task_id=self.step_id,
            python_callable=process_odd
        )