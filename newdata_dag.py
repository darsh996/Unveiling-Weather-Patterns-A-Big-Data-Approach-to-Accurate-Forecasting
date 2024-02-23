from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import datetime

newdata_dag = DAG(
    dag_id="newdata_dag",
    start_date=datetime.datetime(2024, 2, 18),
    schedule="@daily"
)

hello = BashOperator(task_id="hello", dag=newdata_dag, bash_command='echo "weather forecast" ')

API = BashOperator(task_id="API", dag=newdata_dag, bash_command="python3 ~/Documents/newdata/hourly_Data_Through_API.py")

temperatureData = BashOperator(task_id="temperatureData", dag=newdata_dag, bash_command="spark-submit  ~/Documents/newdata/dataPreprocessingTemperature.py")
humidData = BashOperator(task_id="humidData", dag=newdata_dag, bash_command="spark-submit ~/Documents/newdata/dataPreprocessingHumid.py")
windData = BashOperator(task_id="windData", dag=newdata_dag, bash_command="spark-submit ~/Documents/newdata/dataPreprocessingWind.py")

cityData = BashOperator(task_id="cityData", dag=newdata_dag, bash_command="spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.2 ~/Documents/newdata/mongodb/join_Temp_Humid_Wind.py")

clearTempData = BashOperator(task_id="clearTempData", dag=newdata_dag, bash_command="bash ~/Documents/newdata/cleartemp.sh ")

renameCityData = BashOperator(task_id="renameCity", dag=newdata_dag, bash_command="bash ~/Documents/rename.sh ~/Documents/newdata/city")

hello >>API >>[temperatureData,humidData,windData] >> cityData >> clearTempData >> renameCityData
