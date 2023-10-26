# aws-airflow-custom-operator

Airflow Operator（aws emr）:

Custom Operator 实现 EmrAddStepsOperator 和 EmrStepSensor 封装成一个 Operator,

其中 EmrAddStepsOperator 继承 BaseOperator，实现 execute 方法， execute函数主要
用 EmrHook 中 add_job_flow_steps 把任务提交EMR上 并返回 step_ids,

EmrStepSensor ->  EmrBaseSensor -> BaseSensorOperator -> 
其中 EmrStepSensor 继承 EmrBaseSensor, 重写 get_emr_response 方法用 emr client 中
describe_step 方法检测当前提交任务(step_id)状态;
而 EmrBaseSensor 继承 BaseSensorOperator , 主要实现 poke 逻辑;
而 BaseSensorOperator 继承 BaseOperator, execute 方法中对 poke 循环判断，并对超时做了处理

