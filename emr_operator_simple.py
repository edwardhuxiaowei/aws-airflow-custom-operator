'''
此版本为了比较简单，也是继承BaseOperator，但
实现execute方法后得到step_ids后直接调用
EmrStepSensor operator
也能实现自定义Operator 把 EmrAddStepsOperator 和
EmrStepSensor 组合到一起
'''


from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.providers.amazon.aws.hooks.emr import EmrHook


class EmrAddStepsWithSensorOperator(BaseOperator):

    @apply_defaults
    def __init__(
            self,
            steps: list[dict] | str | None = None,
            job_flow_id: str | None = None,
            aws_conn_id: str = "aws_default",
            **kwargs,
    ):
        super().__init__(**kwargs)
        self.aws_conn_id = aws_conn_id
        self.steps = steps or []
        self.job_flow_id = job_flow_id


    def execute(self, context):

        emr_hook = EmrHook(aws_conn_id=self.aws_conn_id)

        step_ids = emr_hook.add_job_flow_steps(
            job_flow_id=self.job_flow_id,
            steps=self.steps
        )

        for step_id in step_ids:
            
            step_sensor = EmrStepSensor(
                task_id=f"step_sensor_{step_id}",
                job_flow_id=self.job_flow_id,
                step_id=step_id,
            )
            step_sensor.execute(context)
