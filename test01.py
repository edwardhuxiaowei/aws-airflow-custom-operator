from __future__ import annotations

from airflow.models import Variable
import boto3
import datetime
import functools
import hashlib
import logging
import time
import traceback
from datetime import timedelta
from typing import Any, Callable, Iterable

from airflow import settings
from airflow.configuration import conf
from airflow.exceptions import (
    AirflowException,
    AirflowFailException,
    AirflowRescheduleException,
    AirflowSensorTimeout,
    AirflowSkipException,
    AirflowTaskTimeout,
)
from airflow.models.baseoperator import BaseOperator
from airflow.models.skipmixin import SkipMixin
from airflow.models.taskreschedule import TaskReschedule
from airflow.ti_deps.deps.ready_to_reschedule import ReadyToRescheduleDep
from airflow.utils import timezone
from airflow.utils.context import Context
from airflow.providers.amazon.aws.hooks.emr import EmrHook

variables_basic = Variable.get(key='variables_basic', deserialize_json=True)
airflow_s3_bucket = variables_basic.get("airflow_s3_bucket")
job_flow_id = variables_basic["cluster_id"]

_MYSQL_TIMESTAMP_MAX = datetime.datetime(2038, 1, 19, 3, 14, 7, tzinfo=timezone.utc)


@functools.lru_cache(maxsize=None)
def _is_metadatabase_mysql() -> bool:
    if settings.engine is None:
        raise AirflowException("Must initialize ORM first")
    return settings.engine.url.get_backend_name() == "mysql"


class PokeReturnValue:
    """
    Optional return value for poke methods.

    Sensors can optionally return an instance of the PokeReturnValue class in the poke method.
    If an XCom value is supplied when the sensor is done, then the XCom value will be
    pushed through the operator return value.
    :param is_done: Set to true to indicate the sensor can stop poking.
    :param xcom_value: An optional XCOM value to be returned by the operator.
    """

    def __init__(self, is_done: bool, xcom_value: Any | None = None) -> None:
        self.xcom_value = xcom_value
        self.is_done = is_done

    def __bool__(self) -> bool:
        return self.is_done


class MySensorOperator(BaseOperator, SkipMixin):
    valid_modes: Iterable[str] = ["poke", "reschedule"]

    deps = BaseOperator.deps | {ReadyToRescheduleDep()}

    def __init__(
            self,
            *,
            aws_conn_id: str = "aws_default",
            script_sql: str = None,
            script_path: str = None,
            poke_interval: float = 60,
            timeout: float = conf.getfloat("sensors", "default_timeout"),
            soft_fail: bool = False,
            mode: str = "poke",
            exponential_backoff: bool = False,
            max_wait: timedelta | float | None = None,
            silent_fail: bool = False,
            target_states: Iterable[str] | None = None,
            failed_states: Iterable[str] | None = None,
            **kwargs,

    ) -> None:
        super().__init__(**kwargs)
        self.aws_conn_id = aws_conn_id
        self.script_sql = script_sql
        self.script_path = script_path
        self.poke_interval = poke_interval
        self.soft_fail = soft_fail
        self.timeout = timeout
        self.mode = mode
        self.exponential_backoff = exponential_backoff
        self.max_wait = self._coerce_max_wait(max_wait)
        self.silent_fail = silent_fail
        self._validate_input_values()
        self.target_states = target_states or ["COMPLETED"]
        self.failed_states = failed_states or ["CANCELLED", "FAILED", "INTERRUPTED"]
        self.hook: EmrHook | None = None

    @staticmethod
    def _coerce_max_wait(max_wait: float | timedelta | None) -> timedelta | None:
        if max_wait is None or isinstance(max_wait, timedelta):
            return max_wait
        if isinstance(max_wait, (int, float)) and max_wait >= 0:
            return timedelta(seconds=max_wait)
        raise AirflowException("Operator arg `max_wait` must be timedelta object or a non-negative number")

    def _validate_input_values(self) -> None:
        if not isinstance(self.poke_interval, (int, float)) or self.poke_interval < 0:
            raise AirflowException("The poke_interval must be a non-negative number")
        if not isinstance(self.timeout, (int, float)) or self.timeout < 0:
            raise AirflowException("The timeout must be a non-negative number")
        if self.mode not in self.valid_modes:
            raise AirflowException(
                f"The mode must be one of {self.valid_modes},'{self.dag.dag_id if self.has_dag() else ''} "
                f".{self.task_id}'; received '{self.mode}'."
            )

        # Quick check for poke_interval isn't immediately over MySQL's TIMESTAMP limit.
        # This check is only rudimentary to catch trivial user errors, e.g. mistakenly
        # set the value to milliseconds instead of seconds. There's another check when
        # we actually try to reschedule to ensure database coherence.
        if self.reschedule and _is_metadatabase_mysql():
            if timezone.utcnow() + datetime.timedelta(seconds=self.poke_interval) > _MYSQL_TIMESTAMP_MAX:
                raise AirflowException(
                    f"Cannot set poke_interval to {self.poke_interval} seconds in reschedule "
                    f"mode since it will take reschedule time over MySQL's TIMESTAMP limit."
                )

    def get_hook(self) -> EmrHook:
        """Get EmrHook"""
        if self.hook:
            return self.hook

        self.hook = EmrHook(aws_conn_id=self.aws_conn_id)
        return self.hook

    def poke(self, step_id):

        emr_client = self.get_hook().get_conn()
        self.log.info("Poking step %s on cluster %s", step_id, job_flow_id)
        response = emr_client.describe_step(ClusterId=job_flow_id, StepId=step_id)

        if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
            self.log.info("Bad HTTP response: %s", response)
            return False

        state = self.state_from_response(response)
        self.log.info("Job flow currently %s", state)

        if state in self.target_states:
            return True

        if state in self.failed_states:
            raise AirflowException(f"EMR job failed: {self.failure_message_from_response(response)}")

        return False

    def get_step_ids(self) -> list[str]:

        s3 = boto3.resource('s3')
        obj = s3.Object(airflow_s3_bucket, self.script_path)
        obj.put(Body=self.script_sql)

        emr_hook = self.get_hook()

        hive_step_args = [
            'hive-script',
            '--run-hive-script',
            '--args',
            '-f',
            "s3://" + airflow_s3_bucket + '/' + self.script_path,
        ]

        steps = [
            {
                'Name': "Script Step",
                'ActionOnFailure': 'CONTINUE',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': hive_step_args,
                },
            }
        ]

        return emr_hook.add_job_flow_steps(
            job_flow_id=job_flow_id,
            steps=steps,
        )

    @staticmethod
    def state_from_response(response: dict[str, Any]) -> str:
        """
        Get state from response dictionary.

        :param response: response from AWS API
        :return: execution state of the cluster step
        """
        return response["Step"]["Status"]["State"]

    @staticmethod
    def failure_message_from_response(response: dict[str, Any]) -> str | None:
        """
        Get failure message from response dictionary.

        :param response: response from AWS API
        :return: failure message
        """
        fail_details = response["Step"]["Status"].get("FailureDetails")
        if fail_details:
            return (
                f"for reason {fail_details.get('Reason')} "
                f"with message {fail_details.get('Message')} and log file {fail_details.get('LogFile')}"
            )
        return None

    def execute(self, context: Context) -> Any:

        started_at: datetime.datetime | float

        if self.reschedule:

            first_try_number = context["ti"].max_tries - self.retries + 1
            task_reschedules = TaskReschedule.find_for_task_instance(
                context["ti"], try_number=first_try_number
            )
            if not task_reschedules:
                start_date = timezone.utcnow()
            else:
                start_date = task_reschedules[0].start_date
            started_at = start_date

            def run_duration() -> float:
                # If we are in reschedule mode, then we have to compute diff
                # based on the time in a DB, so can't use time.monotonic
                return (timezone.utcnow() - start_date).total_seconds()

        else:
            started_at = start_monotonic = time.monotonic()

            def run_duration() -> float:
                return time.monotonic() - start_monotonic

        try_number = 1
        log_dag_id = self.dag.dag_id if self.has_dag() else ""

        step_id = self.get_step_ids()[0]
        xcom_value = None
        while True:
            try:
                poke_return = self.poke(step_id)
            except (
                    AirflowSensorTimeout,
                    AirflowTaskTimeout,
                    AirflowSkipException,
                    AirflowFailException,
            ) as e:
                raise e
            except Exception as e:
                if self.silent_fail:
                    logging.error("Sensor poke failed: \n %s", traceback.format_exc())
                    poke_return = False
                else:
                    raise e

            if poke_return:
                if isinstance(poke_return, PokeReturnValue):
                    xcom_value = poke_return.xcom_value
                break

            if run_duration() > self.timeout:
                # If sensor is in soft fail mode but times out raise AirflowSkipException.
                message = (
                    f"Sensor has timed out; run duration of {run_duration()} seconds exceeds "
                    f"the specified timeout of {self.timeout}."
                )

                if self.soft_fail:
                    raise AirflowSkipException(message)
                else:
                    raise AirflowSensorTimeout(message)
            if self.reschedule:
                next_poke_interval = self._get_next_poke_interval(started_at, run_duration, try_number)
                reschedule_date = timezone.utcnow() + timedelta(seconds=next_poke_interval)
                if _is_metadatabase_mysql() and reschedule_date > _MYSQL_TIMESTAMP_MAX:
                    raise AirflowSensorTimeout(
                        f"Cannot reschedule DAG {log_dag_id} to {reschedule_date.isoformat()} "
                        f"since it is over MySQL's TIMESTAMP storage limit."
                    )
                raise AirflowRescheduleException(reschedule_date)
            else:
                time.sleep(self._get_next_poke_interval(started_at, run_duration, try_number))
                try_number += 1
        self.log.info("Success criteria met. Exiting.")
        return xcom_value

    def _get_next_poke_interval(
            self,
            started_at: datetime.datetime | float,
            run_duration: Callable[[], float],
            try_number: int,
    ) -> float:
        """Using the similar logic which is used for exponential backoff retry delay for operators."""
        if not self.exponential_backoff:
            return self.poke_interval

        # The value of min_backoff should always be greater than or equal to 1.
        min_backoff = max(int(self.poke_interval * (2 ** (try_number - 2))), 1)

        run_hash = int(
            hashlib.sha1(f"{self.dag_id}#{self.task_id}#{started_at}#{try_number}".encode()).hexdigest(),
            16,
        )
        modded_hash = min_backoff + run_hash % min_backoff

        delay_backoff_in_seconds = min(modded_hash, timedelta.max.total_seconds() - 1)
        new_interval = min(self.timeout - int(run_duration()), delay_backoff_in_seconds)

        if self.max_wait:
            new_interval = min(self.max_wait.total_seconds(), new_interval)

        self.log.info("new %s interval is %s", self.mode, new_interval)
        return new_interval



    @property
    def reschedule(self):
        """Define mode rescheduled sensors."""
        return self.mode == "reschedule"

    @classmethod
    def get_serialized_fields(cls):
        return super().get_serialized_fields() | {"reschedule"}
