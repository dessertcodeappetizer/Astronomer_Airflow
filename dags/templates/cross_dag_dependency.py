from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from blueprint import BaseModel, ConfigDict, Field, Blueprint


class TriggerDagRunOperatorConfig(BaseModel):
    """Configuration for TriggerDagRunOperator."""

    dag_id: str = Field(description="The dag_id of the DAG to trigger.")
    conf: dict[str, str] = Field(ConfigDict(default_factory=dict), description="Configuration for the triggered DAG.")
    wait_for_completion: bool = Field(False, description="Whether to wait for the triggered DAG to complete.")
    poke_interval: int = Field(default=60, ge=5, description="Interval in seconds to check the status of the triggered DAG.")
    allowed_states: list[str] = Field(ConfigDict(default_factory=lambda: ['success']), description="List of allowed states for the triggered DAG.")
    failed_states: list[str] = Field(ConfigDict(default_factory=lambda: ['failed', 'skipped']), description="List of failed states for the triggered DAG.")
    fail_when_dag_is_paused: bool = Field(default=False, description="Whether to fail the task if the triggered DAG is paused.")
    reset_dag_run: bool = Field(default=False, description="Whether to reset the triggered DAG run if it already exists.")
    

class TriggerDag(Blueprint[TriggerDagRunOperatorConfig]):
    """Blueprint for TriggerDagRunOperator."""

    def render(self, config: TriggerDagRunOperatorConfig) -> TriggerDagRunOperator:
        """Render the TriggerDagRunOperator with the provided configuration."""
        return TriggerDagRunOperator(
            task_id=f"trigger_{config.dag_id}",
            trigger_dag_id=config.dag_id,
            trigger_run_id="from_{{ dag_run.run_id }}__{{ run_id }}",
            conf=config.conf,
            wait_for_completion=config.wait_for_completion,
            poke_interval=config.poke_interval,
            allowed_states=config.allowed_states,
            failed_states=config.failed_states,
            fail_when_dag_is_paused=config.fail_when_dag_is_paused,
            reset_dag_run=config.reset_dag_run,
            deferrable=True,
            logical_date=None
        )


# task_id:	                 Unique task ID inside the current DAG.
# trigger_dag_id:	         Identifies which DAG should be triggered.
# trigger_run_id:	         Sets the run ID for the newly triggered DAG run.
# conf:	                     Passes configuration data to the triggered DAG through dag_run.conf.
# wait_for_completion:	     Waits for the triggered DAG to finish.
# poke_interval:	         Number of seconds between status checks.
# allowed_states:	         States considered successful by the trigger operator.
# failed_states:	         States considered failures by the trigger operator.
# fail_when_dag_is_paused:	 Determines whether triggering a paused DAG should fail the task.
# reset_dag_run:	         Controls whether an existing DAG run is reset when the same run is triggered again.
# deferrable:	             Allows the operator to defer while waiting, freeing its worker slot.
# logical_date:	             Sets the logical date for the triggered DAG run. Here it is None.