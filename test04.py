from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.sensors.base import BaseSensorOperator
from airflow.providers.amazon.aws.hooks.emr import EmrHook
from airflow.sensors.base import poke_mode_only

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
from airflow.executors.executor_loader import ExecutorLoader
from airflow.models.baseoperator import BaseOperator
from airflow.models.skipmixin import SkipMixin
from airflow.models.taskreschedule import TaskReschedule
from airflow.ti_deps.deps.ready_to_reschedule import ReadyToRescheduleDep
from airflow.utils import timezone
from airflow.utils.context import Context


from airflow.models import Variable

import boto3

variables_basic = Variable.get(key='variables_basic', deserialize_json=True)
airflow_s3_bucket = variables_basic.get("airflow_s3_bucket")

job_flow_id = variables_basic["cluster_id"]




class EmrBaseSensor(BaseSensorOperator):
    """
    Contains general sensor behavior for EMR.

    Subclasses should implement following methods:
        - ``get_emr_response()``
        - ``state_from_response()``
        - ``failure_message_from_response()``

    Subclasses should set ``target_states`` and ``failed_states`` fields.

    :param aws_conn_id: aws connection to use
    """

    ui_color = "#66c3ff"

    def __init__(self, *, aws_conn_id: str = "aws_default", **kwargs):
        super().__init__(**kwargs)
        self.aws_conn_id = aws_conn_id
        self.target_states: Iterable[str] = []  # will be set in subclasses
        self.failed_states: Iterable[str] = []  # will be set in subclasses
        self.hook: EmrHook | None = None

    def get_hook(self) -> EmrHook:
        """Get EmrHook"""
        if self.hook:
            return self.hook

        self.hook = EmrHook(aws_conn_id=self.aws_conn_id)
        return self.hook

    def poke(self, context: Context):
        response = self.get_emr_response(context=context)

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

    def get_emr_response(self, context: Context) -> dict[str, Any]:
        """
        Make an API call with boto3 and get response.

        :return: response
        """
        raise NotImplementedError("Please implement get_emr_response() in subclass")

    @staticmethod
    def state_from_response(response: dict[str, Any]) -> str:
        """
        Get state from boto3 response.

        :param response: response from AWS API
        :return: state
        """
        raise NotImplementedError("Please implement state_from_response() in subclass")

    @staticmethod
    def failure_message_from_response(response: dict[str, Any]) -> str | None:
        """
        Get state from boto3 response.

        :param response: response from AWS API
        :return: failure message
        """
        raise NotImplementedError("Please implement failure_message_from_response() in subclass")



@poke_mode_only
class EmrStepSensor(EmrBaseSensor):
    """
    Asks for the state of the step until it reaches any of the target states.
    If it fails the sensor errors, failing the task.

    With the default target states, sensor waits step to be completed.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:EmrStepSensor`

    :param job_flow_id: job_flow_id which contains the step check the state of
    :param step_id: step to check the state of
    :param target_states: the target states, sensor waits until
        step reaches any of these states
    :param failed_states: the failure states, sensor fails when
        step reaches any of these states
    """

    def __init__(
            self,
            *,
            job_flow_id: str,
            step_id: str,
            target_states: Iterable[str] | None = None,
            failed_states: Iterable[str] | None = None,
            **kwargs,
    ):
        super().__init__(**kwargs)
        self.job_flow_id = job_flow_id
        self.step_id = step_id
        self.target_states = target_states or ["COMPLETED"]
        self.failed_states = failed_states or ["CANCELLED", "FAILED", "INTERRUPTED"]

    def get_emr_response(self, context: Context) -> dict[str, Any]:
        """
        Make an API call with boto3 and get details about the cluster step.

        .. seealso::
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/emr.html#EMR.Client.describe_step

        :return: response
        """
        emr_client = self.get_hook().get_conn()

        self.log.info("Poking step %s on cluster %s", self.step_id, self.job_flow_id)
        return emr_client.describe_step(ClusterId=self.job_flow_id, StepId=self.step_id)

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



class MyEmrOperator(EmrBaseSensor):
    """
    Custom operator that combines EmrAddStepsOperator and EmrStepSensor.
    """

    @apply_defaults
    def __init__(
            self,
            script_sql: str = None,
            script_path: str = None,
            aws_conn_id: str = "aws_default",
            **kwargs,
    ):
        super().__init__(**kwargs)
        self.aws_conn_id = aws_conn_id
        self.script_sql = script_sql
        self.script_path = script_path


    def execute(self, context):
        
        s3 = boto3.resource('s3')
        obj = s3.Object(airflow_s3_bucket, self.script_path)
        obj.put(Body=self.script_sql)

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
                    'Args': hive_step_args
                },
            }
        ]

        """    
        add_steps_operator = EmrAddStepsOperator(
            task_id="add_steps_task",
            job_flow_id=job_flow_id,
            steps=steps
        )
        step_ids = add_steps_operator.execute(context)
        """

        emr_hook = EmrHook(aws_conn_id=self.aws_conn_id)

        step_ids = emr_hook.add_job_flow_steps(
            job_flow_id=job_flow_id,
            steps=steps
        )

        emr_client = emr_hook.get_conn()

        def get_emr_response(self, context: Context) -> dict[str, Any]:
            s3 = boto3.resource('s3')
        obj = s3.Object(airflow_s3_bucket, self.script_path)
        obj.put(Body=self.script_sql)

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
                    'Args': hive_step_args
                },
            }
        ]

        """    
        add_steps_operator = EmrAddStepsOperator(
            task_id="add_steps_task",
            job_flow_id=job_flow_id,
            steps=steps
        )
        step_ids = add_steps_operator.execute(context)
        """

        emr_hook = EmrHook(aws_conn_id=self.aws_conn_id)

        step_ids = emr_hook.add_job_flow_steps(
            job_flow_id=job_flow_id,
            steps=steps
        )


class MyEmrSensor(EmrBaseSensor):


    @apply_defaults
    def __init__(
            self,
            script_sql: str = None,
            script_path: str = None,
            aws_conn_id: str = "aws_default",
            **kwargs,
    ):
        super().__init__(**kwargs)
        self.aws_conn_id = aws_conn_id
        self.script_sql = script_sql
        self.script_path = script_path


        def poke(self, context):

            return True


    @apply_defaults
    def __init__(
            self,
            script_sql: str = None,
            script_path: str = None,
            aws_conn_id: str = "aws_default",
            **kwargs,
    ):
        super().__init__(**kwargs)
        self.aws_conn_id = aws_conn_id
        self.script_sql = script_sql
        self.script_path = script_path


    def execute(self, context):

        s3 = boto3.resource('s3')
        obj = s3.Object(airflow_s3_bucket, self.script_path)
        obj.put(Body=self.script_sql)

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
                    'Args': hive_step_args
                },
            }
        ]

        """    
        add_steps_operator = EmrAddStepsOperator(
            task_id="add_steps_task",
            job_flow_id=job_flow_id,
            steps=steps
        )
        step_ids = add_steps_operator.execute(context)
        """

        emr_hook = EmrHook(aws_conn_id=self.aws_conn_id)

        step_ids = emr_hook.add_job_flow_steps(
            job_flow_id=job_flow_id,
            steps=steps
        )

        emr_client = emr_hook.get_conn()

        def get_emr_response(self, context: Context) -> dict[str, Any]:
            s3 = boto3.resource('s3')
        obj = s3.Object(airflow_s3_bucket, self.script_path)
        obj.put(Body=self.script_sql)

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
                    'Args': hive_step_args
                },
            }
        ]

        """    
        add_steps_operator = EmrAddStepsOperator(
            task_id="add_steps_task",
            job_flow_id=job_flow_id,
            steps=steps
        )
        step_ids = add_steps_operator.execute(context)
        """

        emr_hook = EmrHook(aws_conn_id=self.aws_conn_id)

        step_ids = emr_hook.add_job_flow_steps(
            job_flow_id=job_flow_id,
            steps=steps
        )


            


"""
        for step_id in step_ids:
            
            step_sensor = EmrStepSensor(
                task_id=f"step_sensor_{step_id}",
                job_flow_id=job_flow_id,
                step_id=step_id,
            )

        step_sensor.execute(context)
"""