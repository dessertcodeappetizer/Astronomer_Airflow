from typing import Literal
import requests
from airflow.sdk import TaskGroup, chain
from airflow.exceptions import AirflowException
from airflow.providers.standard.operators.python import PythonOperator
from blueprint import Blueprint, Field, BaseModel


# 1. Configuration received from YAML
class APICallConfig(BaseModel):
    url: str = Field(description="The API endpoint URL.")
    method: Literal["GET", "POST", "PUT", "PATCH", "DELETE"] = Field(default="GET", description="HTTP method.")
    headers: dict[str, str] = Field(default_factory=dict, description="Optional request headers.")
    params: dict[str, str] = Field(default_factory=dict, description="Optional query parameters.")
    data: dict[str, str] = Field(default_factory=dict, description="Optional JSON request body.")
    timeout: int = Field(default=30, gt=0, description="Request timeout in seconds.")


# 2. Validate API response
def validate_response(response_data: dict, description: str) -> dict:
    if not isinstance(response_data, dict):
        raise AirflowException(f"Response for {description} is not a valid JSON object.")
    else:
        if response_data.get("success") != True:
            raise AirflowException(f"API response for {description} indicates failure: {response_data}")
    return response_data


# 3. Execute API request (worker-side)
def execute_api_call(config: dict) -> dict:
    try:
        response = requests.request(
            method=config["method"],
            url=config["url"],
            headers=config["headers"],
            params=config["params"],
            verify=False,
            json=config["data"] or None,
            timeout=config["timeout"],
        )
        response.raise_for_status()
        try:
            response_data = response.json()
            print(response_data)
        except ValueError as exc:
            raise AirflowException("API returned an invalid or empty JSON response.") from exc
        return validate_response(response_data, config["url"])

    except requests.exceptions.RequestException as exc:
        raise AirflowException(
            f"API request failed: {exc}"
        ) from exc


# 4. Blueprint class (scheduler-side rendering)
class APICall(Blueprint[APICallConfig]):

    # def render(self, config: APICallConfig) -> TaskGroup:
    def render(self, config: APICallConfig):
        # with TaskGroup(group_id=self.step_id) as group:
        api_call = PythonOperator(
            task_id="api_call",
            python_callable=execute_api_call,
            op_kwargs={"config": config.model_dump()},
            show_return_value_in_logs=True,
        )
        validate_task = PythonOperator(
            task_id="validate_response",
            python_callable=validate_response,
            op_kwargs={"response_data": api_call.output, "description": self.step_id}
        )
        # chain(api_call, validate_task)
        api_call >> validate_task
        return validate_task
        # return group
