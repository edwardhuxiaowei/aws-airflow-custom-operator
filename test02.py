
from __future__ import annotations

import ast
from typing import TYPE_CHECKING,Sequence

from airflow.models import Variable
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.sensors.base import BaseSensorOperator
from airflow.providers.amazon.aws.hooks.emr import EmrHook

from airflow.providers.amazon.aws.links.emr import EmrClusterLink
from airflow.utils.helpers import exactly_one
import boto3
from typing import TYPE_CHECKING, Any, Iterable, Sequence


if TYPE_CHECKING:
    from airflow.utils.context import Context


variables_basic = Variable.get(key='variables_basic', deserialize_json=True)
airflow_s3_bucket = variables_basic.get("airflow_s3_bucket")

job_flow_id=variables_basic["cluster_id"]


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
        def failure_message_from_response(response: dict[str, Any]) -> str | None:
            """
            Get state from boto3 response.
    
            :param response: response from AWS API
            :return: failure message
            """
            raise NotImplementedError("Please implement failure_message_from_response() in subclass")



class EmrAddStepsOperator(EmrBaseSensor):

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

    def get_emr_response(self, context: Context) -> dict[str, Any]:

        self.log.info("Adding steps to %s", job_flow_id)

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

        step_id = self.get_hook().add_job_flow_steps(
            job_flow_id=job_flow_id,
            steps=steps,
        )

        emr_client = self.get_hook().get_conn()

        self.log.info("Poking step %s on cluster %s", step_id, job_flow_id)
        return emr_client.describe_step(ClusterId=job_flow_id, StepId=step_id)

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




    


