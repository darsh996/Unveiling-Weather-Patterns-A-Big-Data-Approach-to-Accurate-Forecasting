from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import datetime

first_dag = DAG(
    dag_id="first_dag",
    start_date=datetime.datetime(2024, 2, 18),
    schedule="@daily"
)

hello = BashOperator(task_id="hello", dag=first_dag, bash_command='echo "weather forecast" ')

API = BashOperator(task_id="API", dag=first_dag, bash_command="python3 ~/Documents/hourly_Data_Through_API.py")

tempData = BashOperator(task_id="tempData", dag=first_dag, bash_command="spark-submit --master spark://ankit-vivobook:4040 ~/Documents/dataPreprocessingTemperature.py")
humidData = BashOperator(task_id="humidData", dag=first_dag, bash_command="spark-submit --master spark://ankit-vivobook:4040 ~/Documents/dataPreprocessingHumid.py")
windData = BashOperator(task_id="windData", dag=first_dag, bash_command="spark-submit --master spark://ankit-vivobook:4040 ~/Documents/dataPreprocessingWind.py")

cityData = BashOperator(task_id="cityData", dag=first_dag, bash_command="spark-submit --master spark://ankit-vivobook:4040 ~/Documents/join_Temp_Humid_Wind.py")

modaltraining = BashOperator(task_id="modelTraining",dag=first_dag,bash_command="spark-submit --master spark://ankit-vivobook:4040 ~/Documents/missing.py $city",env={"city":'{{city}}'})
def python_function():
    city='Adilabad'
    print(city)


inputCity = PythonOperator(task_id="InputCity", dag=first_dag, python_callable=python_function)
#hello >> API

hello >> [tempData,humidData,windData] >> cityData >> inputCity
