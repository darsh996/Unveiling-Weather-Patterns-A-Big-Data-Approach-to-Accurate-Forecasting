# In[ ]:Importing required packages

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from statsmodels.tsa.statespace.sarimax import SARIMAX
from statsmodels.graphics.tsaplots import plot_acf, plot_pacf
import pickle
from sklearn.metrics import mean_squared_error

# In[ ]: Defining city
city="Delhi"

# In[ ]: loading data
data=pd.read_csv(f'/kaggle/input/{city}-temp/{city}.csv')

# In[ ]: dropping unrequired features
data=data.drop(['T2MDEW', 'T2MWET', 'PS', 'PSC', 'WS2M', 'QV2M', 'RH2M', 'PRECTOTCORR'], axis=1)

# In[ ]: convert the date column to datetime format
data['Date'] = pd.to_datetime(data['Date'], format='%Y%m%d%H')
data.head()

# In[ ]: changing index to date
data.index = data['Date']
data.head()

# In[ ]: soring on the basis of date
data=data.sort_index()

# In[ ]: Renaming columns
data.rename(columns={'T2M':'TEMP'}, inplace=True)

# In[ ]: extended training data
train_data=data[-24:]

# In[ ]: Loading the saved SARIMA model from the file
with open(f'/kaggle/input/Trained Models SARIMA/{city}_model.pkl', 'rb') as f:
    city_result = pickle.load(f)
    
# In[ ]: extending the training data
city_result=city_result.extend(train_data['temp'])

# In[ ]: saving the extended training data
with open(f"/kaggle/working/{city}_model.pkl", 'wb') as f:
    pickle.dump(city_result, f)