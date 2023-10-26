'''
此版本为了把 EmrAddStepsOperator 和 EmrStepSensor 合并在一起，修改code把
EmrStepSensor 和 EmrAddStepsOperator 都实现到继承相同的 BaseOperator，
实现 EmrAddStepsOperator 中 execute 以及 EmrStepSensor 中的 poke逻辑，
其中主要注意把 EmrAddStepsOperator 中 execute 返回的 step_ids 传递给
EmrStepSensor 中的 poke 方法即可，具体实现代码如下
'''

from __future__ import annotations

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
    AirflowSensorTimeout,
    AirflowSkipException,
    AirflowTaskTimeout,
)
from airflow.models.baseoperator import BaseOperator
from airflow.utils import timezone
from airflow.utils.context import Context
from airflow.providers.amazon.aws.hooks.emr import EmrHook

_MYSQL_TIMESTAMP_MAX = datetime.datetime(2038, 1, 19, 3, 14, 7, tzinfo=timezone.utc)


@functools.lru_cache(maxsize=None)
def _is_metadatabase_mysql() -> bool:
    if settings.engine is None:
        raise AirflowException("Must initialize ORM first")
    return settings.engine.url.get_backend_name() == "mysql"


class PokeReturnValue:

    def __init__(self, is_done: bool, xcom_value: Any | None = None) -> None:
        self.xcom_value = xcom_value
        self.is_done = is_done

    def __bool__(self) -> bool:
        return self.is_done


class EmrAddStepsWithSensorOperator(BaseOperator):

    def __init__(
            self,
            *,
            aws_conn_id: str = "aws_default",
            steps: list[dict] | str | None = None,
            job_flow_id: str | None = None,
            poke_interval: float = 60,
            timeout: float = conf.getfloat("sensors", "default_timeout"),
            soft_fail: bool = False,
            mode: str = "poke",
            exponential_backoff: bool = False,
            silent_fail: bool = False,
            target_states: Iterable[str] | None = None,
            failed_states: Iterable[str] | None = None,
            **kwargs,

    ) -> None:
        super().__init__(**kwargs)
        self.aws_conn_id = aws_conn_id
        self.steps = steps or []
        self.job_flow_id = job_flow_id
        self.poke_interval = poke_interval
        self.soft_fail = soft_fail
        self.timeout = timeout
        self.mode = mode
        self.exponential_backoff = exponential_backoff
        self.silent_fail = silent_fail
        self.target_states = target_states or ["COMPLETED"]
        self.failed_states = failed_states or ["CANCELLED", "FAILED", "INTERRUPTED"]
        self.hook: EmrHook | None = None
        self.step_id = None

    def get_hook(self) -> EmrHook:
        """Get EmrHook"""
        if self.hook:
            return self.hook

        self.hook = EmrHook(aws_conn_id=self.aws_conn_id)
        return self.hook

    def poke(self):

        '''
        第一种通过初始化 step_id 变量传递给 poke
         直接通过 self.step_id 拿到

        第二种通过 xcom_pull 拿到
        step_id = context['ti'].xcom_pull(task_ids=self.task_id, key='step_id')

        '''

        emr_client = self.get_hook().get_conn()
        self.log.info("Poking step %s on cluster %s", self.step_id, job_flow_id)
        response = emr_client.describe_step(ClusterId=job_flow_id, StepId=self.step_id)

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

        emr_hook = self.get_hook()

        return emr_hook.add_job_flow_steps(
            job_flow_id=self.job_flow_id,
            steps=self.steps,
        )

    @staticmethod
    def state_from_response(response: dict[str, Any]) -> str:

        return response["Step"]["Status"]["State"]

    @staticmethod
    def failure_message_from_response(response: dict[str, Any]) -> str | None:

        fail_details = response["Step"]["Status"].get("FailureDetails")
        if fail_details:
            return (
                f"for reason {fail_details.get('Reason')} "
                f"with message {fail_details.get('Message')} and log file {fail_details.get('LogFile')}"
            )
        return None


def execute(self, context: Context):
    started_at: datetime.datetime | float

    started_at = start_monotonic = time.monotonic()

    def run_duration() -> float:
        return time.monotonic() - start_monotonic

    '''
    第一种通过初始化 step_id 变量传递给 poke
    step_ids = self.get_step_ids()
    for step_id in step_ids:
        self.step_id = step_id
        
    第二种通过xcom_push 传参 
    step_ids = self.get_step_ids()
    for step_id in step_ids:
        context['ti'].xcom_push(key='step_id', value=step_id)
    '''
    step_ids = self.get_step_ids(self)
    for step_id in step_ids:
        self.step_id = step_id

        try_number = 1
        while True:
            try:
                poke_return = self.poke()
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
                    self.log.info(xcom_value)
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
            time.sleep(self._get_next_poke_interval(started_at, run_duration, try_number))
            try_number += 1
        self.log.info("Success criteria met. Exiting.")


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
