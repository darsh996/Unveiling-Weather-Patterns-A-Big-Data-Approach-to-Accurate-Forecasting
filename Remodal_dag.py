from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import datetime

Remodal_dag = DAG(
    dag_id="Remodal_dag",
    start_date=datetime.datetime(2024, 2, 17),
    schedule="5 2 * * *"
)

hello = BashOperator(task_id="hello", dag=Remodal_dag, bash_command='echo "weather forecast" ')

modalTraining = BashOperator(task_id="modelTraining",dag=Remodal_dag,bash_command="spark-submit ~/Documents/newdata/extendedTraining.py")

hello >> modalTraining

