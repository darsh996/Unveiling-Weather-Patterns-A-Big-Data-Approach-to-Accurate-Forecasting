**Project Report On**

**Unveiling-Weather-Patterns-A-Big-Data-Approach-to-Accurate-Forecasting**

![Screenshot 2024-02-23 at 9 29 43 AM](https://github.com/darsh996/Unveiling-Weather-Patterns-A-Big-Data-Approach-to-Accurate-Forecasting/assets/97582053/38abb5a4-781b-4ff2-9a5f-5682ad6ed150)

Submitted in partial fulfillment for the award of
Post Graduate Diploma in Big Data Analytics (PGDBDA)
From Know IT(Pune)

Guided by:

Trupti Joshi Ma’am & Prasad Deshmukh Sir

Submitted By:

Ankit Kharwar<br>
Akhil Goplani<br>
Darshan Satone<br>
Manish Kumar<br>

**Introduction**

This project presents a comprehensive approach to weather forecasting by harnessing the power of big data technologies and advanced analytical methods. Focusing on 200+ Indian cities over 20 years, we employ a sophisticated blend of time series analysis, machine learning, and Apache Spark to delve into the intricacies of weather patterns. The project begins with a robust Extract Transform & Load (ETL) process, ensuring efficient data handling and scalability. Subsequently, we explore time series analysis and machine learning techniques to unveil hidden patterns within the temporal sequences of weather data. Python serves as the primary language, offering flexibility for data manipulation, and analysis, and Tableau for visualization. By uniting machine learning models with big data technologies, our goal is to provide accurate weather predictions for selective city in our dataset.

**Dataset Collection and Features**

We successfully collected a substantial dataset from NASA's prediction of worldwide energy resources through their data access viewer (https://power.larc.nasa.gov/data-access-viewer/).
Additionally, we express our gratitude to NASA for providing this valuable dataset, acknowledging their significant role in advancing scientific understanding and fostering collaborative efforts for the greater good.


**Architecture**

![image](https://github.com/darsh996/Unveiling-Weather-Patterns-A-Big-Data-Approach-to-Accurate-Forecasting/assets/97582053/fab185c4-e503-43c1-a25c-73730b5d9ca0)

**Algorithms**

**AutoRegressive Integrated Moving Average (ARIMA) :**

![image](https://github.com/darsh996/Unveiling-Weather-Patterns-A-Big-Data-Approach-to-Accurate-Forecasting/assets/97582053/f2bf0c53-f8d1-460c-b861-aaff0200752e)

The Root Mean Square Error (RMSE) is a crucial metric for evaluating the accuracy of a forecasting model, including the ARIMA model for weather temperature prediction. An RMSE of 8.1337 indicates that, on average, the model's predictions are approximately 8.1337 units away from the actual observed values.
In the context of weather temperature prediction, an RMSE of 8.1337 implies that the model's forecasts may deviate from the true temperatures by around 8 degrees Celsius. While this might be acceptable for some applications, it is essential to aim for lower RMSE values to improve the model's accuracy.

**Seasonal Autoregressive Integrated Moving Average (SARIMAX):**

![image](https://github.com/darsh996/Unveiling-Weather-Patterns-A-Big-Data-Approach-to-Accurate-Forecasting/assets/97582053/942138cc-063e-41d6-b3c4-556407cf8a84)

Achieving an RMSE of 0.6172 demonstrates the model's ability to effectively capture both seasonal patterns and the influence of external factors on temperature. This improvement in accuracy can significantly benefit various applications, such as energy management, agriculture, or transportation planning, by providing more precise and reliable weather forecast.

**Working Model:**

![Screenshot 2024-02-23 at 9 26 23 AM](https://github.com/darsh996/Unveiling-Weather-Patterns-A-Big-Data-Approach-to-Accurate-Forecasting/assets/97582053/b3b88815-e65d-46e9-905b-aae6024cfb1f)

**References**

Apache Spark. [https://spark.apache.org/]

MongoDB. [https://www.mongodb.com/]

Kafka. [https://kafka.apache.org/]

Python. [https://www.python.org/]

scikit-learn. [https://scikit-learn.org/]

The data has been taken from NASA prediction of worldwide Energy Resources https://power.larc.nasa.gov/data-access-viewer/
