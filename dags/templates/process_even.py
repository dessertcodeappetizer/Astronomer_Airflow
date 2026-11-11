from blueprint import Blueprint, BaseModel
from airflow.providers.standard.operators.python import PythonOperator


class ProcessEvenConfig(BaseModel):
    pass


def process_even():
    print("Number is even. Executing Task B.")


class ProcessEven(Blueprint[ProcessEvenConfig]):

    def render(self, config: ProcessEvenConfig):
        return PythonOperator(
            task_id=self.step_id,
            python_callable=process_even
        )