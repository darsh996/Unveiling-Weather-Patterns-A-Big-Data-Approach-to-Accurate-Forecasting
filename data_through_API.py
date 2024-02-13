#!/usr/bin/env python
# coding: utf-8

# In[1]:


# Import the relevant modules
import requests
import json


# In[2]:


# define base URL
base_site = "https://power.larc.nasa.gov/api/temporal/daily/point"
#curl -X GET ? -H "accept: application/json"
#"start=20000101&end=20101231&latitude=25.5248&longitude=85.2507&community=ag&parameters=T2M%2CPS%2CWS10M&format=json&header=true&time-standard=lst&site-elevation=10"
# In[3]:


# Make a request
r = requests.get(base_site, params = { "start=20000101&end=20101231&latitude=25.5248&longitude=85.2507&community=ag&parameters=T2M%2CPS%2CWS10M&format=json&header=true&time-standard=lst&site-elevation=10" })
r.status_code


# In[4]:


# Store the response
info = r.json()

# Inspect the response
print(json.dumps(info, indent=4))


# In[ ]:




