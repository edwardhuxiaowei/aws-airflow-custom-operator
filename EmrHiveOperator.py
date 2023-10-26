import logging
from airflow.models.baseoperator import BaseOperator
from airflow.models import Variable
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.operators.python import PythonOperator
from datetime import timedelta
import boto3
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.decorators import apply_defaults




LOGGER = logging.getLogger(__name__)
variables_basic = Variable.get(key='variables_basic', deserialize_json=True)
airflow_s3_bucket = variables_basic.get("airflow_s3_bucket")


class EmrHiveOperator(BaseOperator):

    @apply_defaults
    def __init__(self, name: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.name = name

    def execute(self, context):
        message = f"Hello {self.name}"
        print(message)
        return message




# ##############  Tasks  ############## #
def create_emr_scripts(script_sql, script_path):
    s3 = boto3.resource('s3')
    object1 = s3.Object(airflow_s3_bucket, script_path)
    object1.put(Body=script_sql)


def do_create_emr_scripts(task_id, script_sql, script_path, **kwargs):

    return PythonOperator (
        task_id= f'{task_id}_create_emr_scripts',
        provide_context=True,
        python_callable=create_emr_scripts,
        op_kwargs={"script_sql": script_sql,
                   "script_path": script_path},
        **kwargs
    )




