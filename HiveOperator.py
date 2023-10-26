import logging
from airflow.models.baseoperator import BaseOperator
from airflow.models import Variable
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.operators.python import PythonOperator
import boto3
from airflow.utils.decorators import apply_defaults


LOGGER = logging.getLogger(__name__)
variables_basic = Variable.get(key='variables_basic', deserialize_json=True)
airflow_s3_bucket = variables_basic.get("airflow_s3_bucket")


class HiveOperator(BaseOperator):

    job_flow_id=variables_basic["cluster_id"]

    @apply_defaults
    def __init__(self, 
                 script_sql=None,
                 script_path=None,
                 *args,
                 **kwargs):
        super().__init__( *args, **kwargs)
        self.script_sql = script_sql
        self.script_path = script_path

    def execute(self, context):

        run_create_emr_scripts = PythonOperator(
            task_id=f"{self.task_id}_create_emr_scripts",
            python_callable=do_create_emr_scripts,
            op_kwargs={
                'task_id': self.task_id,
                'script_sql': self.script_sql,
                'script_path': self.script_path,
                **self.kwargs
            }
        )

        start_job_task = PythonOperator(
            task_id=f"{self.task_id}_run_script",
            python_callable=do_run_script,
            op_kwargs={
                'task_id': self.task_id,
                'job_flow_id': self.job_flow_id,
                'script_path': self.script_path,
                **self.kwargs
            }
        )

        start_job_checker = PythonOperator(
            task_id=f"{self.task_id}_check_task",
            python_callable=check_task,
            op_kwargs={
                'task_id_to_check': self.task_id,
                'job_flow_id': self.job_flow_id,
                **self.kwargs
            }
        )

        self.dag.add_task(run_create_emr_scripts)
        self.dag.add_task(start_job_task)
        self.dag.add_task(start_job_checker)

        run_create_emr_scripts.set_upstream(self)
        start_job_task.set_upstream(run_create_emr_scripts)
        start_job_checker.set_upstream(start_job_task)





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

def do_run_script(task_id, job_flow_id, script_path,
                  **kwargs):


    hive_step_args = [
        'hive-script',
        '--run-hive-script',
        '--args',
        '-f',
        "s3://" + airflow_s3_bucket + '/' + script_path,
        ]

    step = [
        {
            'Name': "Script Step",
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': hive_step_args,
            },
        }
    ]

    return EmrAddStepsOperator(
        task_id=task_id,
        job_flow_id=job_flow_id,
        steps=step,
        **kwargs,
    )


def check_task(task_id_to_check, job_flow_id, **kwargs):

    step_id = "{{task_instance.xcom_pull('" + task_id_to_check + "', key='return_value')[0]}}"

    return EmrStepSensor(
        task_id=f"{task_id_to_check}_checker",
        job_flow_id=job_flow_id,
        step_id=step_id,
        **kwargs,
    )
