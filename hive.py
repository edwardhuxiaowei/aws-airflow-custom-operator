from airflow.models import Variable
from airflow.models import BaseOperator
import boto3
from airflow.utils.decorators import apply_defaults
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor

variables_basic = Variable.get(key='variables_basic', deserialize_json=True)
airflow_s3_bucket = variables_basic.get("airflow_s3_bucket")

job_flow_id = variables_basic["cluster_id"]


class EmrHiveOperator(BaseOperator):

    @apply_defaults
    def __init__(
            self,
            script_sql=None,
            script_path=None,
            aws_conn_id='aws_default',
            *args,
            **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.script_sql = script_sql
        self.script_path = script_path
        self.aws_conn_id = aws_conn_id

    def execute(self, context):

        s3 = boto3.resource('s3')
        object = s3.Object(airflow_s3_bucket, self.script_path)
        object.put(Body=self.script_sql)

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


