
#Parameters: "T2M,T2MDEW,T2MWET,PS,WS2M,QV2M,RH2M,PRECTOTCORR,WD2M,WS10M,WD10M",

import os
import requests
import json
import csv
from datetime import datetime ,timedelta


# Create a folder to store the output files
output_folder = '/home/hadoop/Documents/newdata/'
os.makedirs(output_folder, exist_ok=True)
print("1")

# create file if request return error
left_file_path = os.path.join('/home/hadoop/Documents/newdata/left.csv')

# Define base URL
base_site = "https://power.larc.nasa.gov/api/temporal/hourly/point"

# today date 
#current_date = datetime.now().strftime('%Y%m%d')
current_date = datetime.now()
current_date = current_date - timedelta(days=3)

# Calculate the previous date
previous_date = current_date - timedelta(days=1)



current_date = current_date.strftime('%Y%m%d')
previous_date = previous_date.strftime('%Y%m%d')
#previous_date = "20231227"

# Set the time to 00:00:00
#previous_date_midnight = previous_date.replace(hour=0, minute=0, second=0, microsecond=0)
#current_date_midnight = current_date.replace(hour=0,minute=0, second=0, microsecond=0)

# set interval for the previous day data
# interval =  {"start":"20231231","end":current_date}
interval =  {"start":previous_date,"end":current_date}

parameters = {"temperature":"T2M,T2MDEW,T2MWET,PS,PSC","humid":"WS2M,QV2M,RH2M,PRECTOTCORR","wind":"WD2M,WS10M,WD10M"}

for key,parameter in parameters.items():
    print(interval)
    start = interval['start']
    end = interval['end']
    # Read latitude and longitude values from the CSV file
    with open('/home/hadoop/Documents/Indian_Cities_Database.csv', 'r') as csvfile:
        coordinates = csv.DictReader(csvfile)
        for row in coordinates:
            city = row['City']
            latitude = row['latitude']
            longitude = row['longitude']

            # Make a request for each latitude and longitude combination
            try:
                r = requests.get(base_site, params={
                    "start": start,
                    "end": end,
                    "latitude": latitude,
                    "longitude": longitude,
                    "community": "ag",
                    "parameters": parameter,
                    "format": "json",
                    "header": "true",
                    "time-standard": "lst",
                    "site-elevation": "10"
                })

                # Check if the request was successful
                if r.status_code == 200:
                    # Store the response
                    info = r.json()

                    # Specify the path for the output file
                    output_file_path = os.path.join(output_folder, f"{key}/{city}_{latitude}_{longitude}_{start}_{end}.json")

                    # Write the response to the output file
                    with open(output_file_path, "w") as output_file:
                        json.dump(info, output_file, indent=4)

                    print("Output saved to:", output_file_path)
                else:
                    print("Error:", r.status_code)
                    # Append latitude and longitude to left.csv file
                    with open(left_file_path, 'a', newline='') as left_csvfile:
                        left_writer = csv.writer(left_csvfile)
                        left_writer.writerow([city, latitude, longitude,start, end])

            except Exception as e:
                print("Exception occurred:", str(e))
                # Append latitude and longitude to left.csv file
                with open(left_file_path, 'a', newline='') as left_csvfile:
                    left_writer = csv.writer(left_csvfile)
                    left_writer.writerow([city, latitude, longitude, start, end])
